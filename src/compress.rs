use crate::offload::{
    handler::{setup_worker_thread, Completion, FlushJob, ReadJob},
    OffloadTargetLogic,
};
use libublk::{
    ctrl::UblkCtrl,
    io::{UblkDev, UblkIOCtx, UblkQueue},
};
use rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, DBCompressionType, Options, SliceTransform, WriteBatch,
    WriteOptions, DB,
};
use serde::{Deserialize, Serialize};
use serde_json;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;

#[derive(clap::Args, Debug, Clone)]
pub(crate) struct CompressAddArgs {
    #[command(flatten)]
    pub(crate) gen_arg: super::args::GenAddArgs,

    /// Path to the RocksDB directory
    #[clap(long)]
    pub dir: PathBuf,

    /// Size of the device
    #[clap(long)]
    pub size: Option<String>,

    /// Compression algorithm to use [none|snappy|zlib|lz4|zstd]
    #[clap(long, default_value = "lz4")]
    pub compression: String,
}

fn default_compression() -> String {
    "lz4".to_string()
}

#[derive(Serialize, Deserialize)]
struct CompressJson {
    size: u64,
    dir: PathBuf,
    #[serde(default = "default_compression")]
    compression: String,
    logical_block_size: u32,
    physical_block_size: u32,
}

fn handle_read(
    db: &DB,
    start_sector: u64,
    nr_sectors: u32,
    buf: &mut [u8],
    lbs: u32,
) -> Result<i32, i32> {
    let sectors_per_block = (lbs >> 9) as u64;
    let nr_blocks = nr_sectors as usize / sectors_per_block as usize;

    let keys: Vec<_> = (0..nr_blocks)
        .map(|i| (start_sector + i as u64 * sectors_per_block).to_be_bytes())
        .collect();
    let results = db.multi_get(&keys);

    for (i, result) in results.iter().enumerate() {
        let block_buf = &mut buf[(i * lbs as usize)..((i + 1) * lbs as usize)];
        match result {
            Ok(Some(value)) => {
                block_buf.copy_from_slice(value);
            }
            Ok(None) => {
                for byte in block_buf.iter_mut() {
                    *byte = 0;
                }
            }
            Err(e) => {
                log::error!("rocksdb multi_get error: {}", e);
                return Err(-libc::EIO);
            }
        }
    }
    Ok((nr_sectors << 9) as i32)
}

fn handle_write(
    db: &DB,
    start_sector: u64,
    nr_sectors: u32,
    buf: &[u8],
    lbs: u32,
) -> Result<i32, i32> {
    let mut batch = WriteBatch::default();
    let sectors_per_block = (lbs >> 9) as u64;
    let nr_blocks = nr_sectors as usize / sectors_per_block as usize;

    for i in 0..nr_blocks {
        let key = (start_sector + i as u64 * sectors_per_block).to_be_bytes();
        let block_buf = &buf[(i * lbs as usize)..((i + 1) * lbs as usize)];
        batch.put(&key, block_buf);
    }
    let mut write_options = WriteOptions::new();
    write_options.set_sync(false);
    write_options.disable_wal(true);
    if let Err(e) = db.write_opt(batch, &write_options) {
        log::error!("rocksdb write batch error: {}", e);
        return Err(-libc::EIO);
    }
    Ok((nr_sectors << 9) as i32)
}

fn handle_discard(
    db: &DB,
    cf: &ColumnFamily,
    start_sector: u64,
    nr_sectors: u32,
    _lbs: u32,
) -> Result<i32, i32> {
    let from = start_sector.to_be_bytes();
    let to = (start_sector + nr_sectors as u64).to_be_bytes();
    if let Err(e) = db.delete_range_cf(cf, &from, &to) {
        log::error!("rocksdb discard (delete_range_cf) error: {}", e);
        return Err(-libc::EIO);
    }
    Ok(0)
}

struct CompressTarget {
    db: Arc<DB>,
    lbs: u32,
    read_only: bool,
}

impl OffloadTargetLogic for CompressTarget {
    fn setup_read_worker(
        &self,
        efd: i32,
    ) -> (Sender<ReadJob>, Receiver<Completion>) {
        let db = self.db.clone();
        let lbs = self.lbs;
        setup_worker_thread(efd, move |job: ReadJob| {
            let buf_slice = unsafe {
                std::slice::from_raw_parts_mut(
                    job.buf_addr as *mut u8,
                    (job.nr_sectors << 9) as usize,
                )
            };
            let res = handle_read(&db, job.start_sector, job.nr_sectors, buf_slice, lbs);
            Completion {
                tag: job.tag,
                result: res,
            }
        })
    }

    fn setup_flush_worker(
        &self,
        efd: i32,
    ) -> (Sender<FlushJob>, Receiver<Completion>) {
        let db = self.db.clone();
        setup_worker_thread(efd, move |job: FlushJob| {
            let res = if let Err(e) = db.flush() {
                log::error!("Background rocksdb flush error: {}", e);
                Err(-libc::EIO)
            } else {
                Ok(0)
            };
            Completion {
                tag: job.tag,
                result: res,
            }
        })
    }

    fn handle_io(
        &self,
        _q: &UblkQueue,
        _tag: u16,
        iod: &libublk::sys::ublksrv_io_desc,
        buf: &mut [u8],
    ) -> Result<i32, i32> {
        let op = iod.op_flags & 0xff;
        match op {
            libublk::sys::UBLK_IO_OP_WRITE => {
                if self.read_only {
                    Err(-libc::EACCES)
                } else {
                    handle_write(&self.db, iod.start_sector, iod.nr_sectors, buf, self.lbs)
                }
            }
            libublk::sys::UBLK_IO_OP_DISCARD => {
                if self.read_only {
                    Err(-libc::EACCES)
                } else {
                    let cf = self.db.cf_handle("default").unwrap();
                    handle_discard(&self.db, cf, iod.start_sector, iod.nr_sectors, self.lbs)
                }
            }
            _ => Err(-libc::EINVAL),
        }
    }
}

fn q_sync_fn(qid: u16, dev: &UblkDev, db: &Arc<DB>) {
    let mut bufs = dev.alloc_queue_io_bufs();
    let q = UblkQueue::new(qid, dev)
        .unwrap()
        .regiser_io_bufs(Some(&bufs))
        .submit_fetch_commands(Some(&bufs));

    let target = CompressTarget {
        db: db.clone(),
        lbs: 1u32 << dev.tgt.params.basic.logical_bs_shift,
        read_only: (dev.tgt.params.basic.attrs & libublk::sys::UBLK_ATTR_READ_ONLY) != 0,
    };
    let handler = crate::offload::handler::QueueHandler::new(&q, &target, &mut bufs, dev);

    let io_handler = |_q: &UblkQueue, tag: u16, io_ctx: &UblkIOCtx| {
        handler.handle_io(tag, io_ctx);
    };

    q.wait_and_handle_io(io_handler);
}

fn parse_compression_type(s: &str) -> anyhow::Result<DBCompressionType> {
    match s {
        "none" => Ok(DBCompressionType::None),
        "snappy" => Ok(DBCompressionType::Snappy),
        "zlib" => Ok(DBCompressionType::Zlib),
        "lz4" => Ok(DBCompressionType::Lz4),
        "zstd" => Ok(DBCompressionType::Zstd),
        _ => Err(anyhow::anyhow!("Invalid compression type")),
    }
}

fn parse_and_load_config(
    ctrl: &UblkCtrl,
    opt: &Option<CompressAddArgs>,
) -> anyhow::Result<(PathBuf, u64, String, u32, u32, Option<CompressAddArgs>)> {
    if let Some(o) = opt {
        let dir = o.gen_arg.build_abs_path(o.dir.clone());
        let json_path = dir.join("ublk_compress.json");

        let (size, compression, lbs, pbs) = if json_path.exists() {
            let file = File::open(&json_path)?;
            let config: CompressJson = serde_json::from_reader(file)?;
            (
                config.size,
                config.compression,
                config.logical_block_size,
                config.physical_block_size,
            )
        } else {
            let size_str = o
                .size
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("--size is required for new device"))?;
            let size = parse_size::parse_size(size_str)?;
            let compression = o.compression.clone();
            let lbs = o.gen_arg.logical_block_size.unwrap_or(512);
            let pbs = o.gen_arg.physical_block_size.unwrap_or(4096);

            let config = CompressJson {
                size,
                dir: dir.clone(),
                compression: compression.clone(),
                logical_block_size: lbs,
                physical_block_size: pbs,
            };
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .open(&json_path)?;
            file.write_all(serde_json::to_string_pretty(&config)?.as_bytes())?;
            (size, compression, lbs, pbs)
        };
        Ok((dir, size, compression, lbs, pbs, Some(o.clone())))
    } else {
        let val = ctrl
            .get_target_data_from_json()
            .ok_or_else(|| anyhow::anyhow!("no json data for recovery"))?;
        let dir_str = val["compress"]["dir"].as_str().unwrap().to_string();
        let dir = PathBuf::from(dir_str);
        let json_path = dir.join("ublk_compress.json");
        let file = File::open(json_path)?;
        let config: CompressJson = serde_json::from_reader(file)?;
        Ok((
            dir,
            config.size,
            config.compression,
            config.logical_block_size,
            config.physical_block_size,
            None,
        ))
    }
}

fn setup_database(
    db_path: &PathBuf,
    compression: &str,
    lbs: u32,
    read_only: bool,
) -> anyhow::Result<DB> {
    let mut db_opts = Options::default();
    db_opts.create_if_missing(true);
    db_opts.set_use_fsync(false);
    db_opts.set_use_direct_io_for_flush_and_compaction(true);
    let compression_type = parse_compression_type(&compression)?;
    db_opts.set_compression_type(compression_type);
    db_opts.set_bottommost_compression_type(compression_type);
    db_opts.set_write_buffer_size(64 * 1024 * 1024);
    db_opts.set_max_write_buffer_number(4);
    db_opts.set_max_background_jobs(4);

    let mut block_based_opts = rocksdb::BlockBasedOptions::default();
    /* 32 logical block for one rocksdb block */
    block_based_opts.set_block_size((32 * lbs).try_into().unwrap());
    let cache = rocksdb::Cache::new_lru_cache(256 * 1024 * 1024);
    block_based_opts.set_block_cache(&cache);
    block_based_opts.set_bloom_filter(10.0, false);
    db_opts.set_block_based_table_factory(&block_based_opts);

    db_opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(8));
    db_opts.set_memtable_prefix_bloom_ratio(0.1);
    db_opts.set_optimize_filters_for_hits(true);

    let db = if read_only {
        let cfs = vec!["default"];
        DB::open_cf_as_secondary(&db_opts, db_path, db_path, &cfs)?
    } else {
        let cf_descriptor = ColumnFamilyDescriptor::new("default", db_opts.clone());
        DB::open_cf_descriptors(&db_opts, db_path, vec![cf_descriptor])?
    };
    Ok(db)
}

pub(crate) fn ublk_add_compress(
    ctrl: UblkCtrl,
    opt: Option<CompressAddArgs>,
    comm_arc: &Arc<crate::DevIdComm>,
) -> anyhow::Result<i32> {
    let (db_path, size, compression, lbs, pbs, args_opt) = parse_and_load_config(&ctrl, &opt)?;
    let read_only = args_opt.as_ref().map_or(false, |o| o.gen_arg.read_only);
    let db = setup_database(&db_path, &compression, lbs, read_only)?;
    let db_arc = Arc::new(db);
    let db_for_handler = db_arc.clone();

    let tgt_init = |dev: &mut UblkDev| {
        let tgt = &mut dev.tgt;
        tgt.dev_size = size;
        //todo: figure out correct block size
        tgt.params = libublk::sys::ublk_params {
            types: libublk::sys::UBLK_PARAM_TYPE_BASIC | libublk::sys::UBLK_PARAM_TYPE_DISCARD,
            basic: libublk::sys::ublk_param_basic {
                attrs: libublk::sys::UBLK_ATTR_VOLATILE_CACHE,
                logical_bs_shift: lbs.trailing_zeros() as u8,
                physical_bs_shift: pbs.trailing_zeros() as u8,
                io_opt_shift: pbs.trailing_zeros() as u8,
                io_min_shift: lbs.trailing_zeros() as u8,
                max_sectors: dev.dev_info.max_io_buf_bytes >> 9,
                dev_sectors: tgt.dev_size >> 9,
                ..Default::default()
            },
            discard: libublk::sys::ublk_param_discard {
                max_discard_sectors: u32::MAX >> 9,
                max_discard_segments: 1,
                discard_granularity: 4096,
                ..Default::default()
            },
            ..Default::default()
        };
        if let Some(ref args) = args_opt {
            args.gen_arg.apply_read_only(dev);
            let val = serde_json::json!({"compress": { "dir": &db_path }});
            dev.set_target_json(val);
        }
        Ok(())
    };

    let q_handler = move |qid, dev: &_| q_sync_fn(qid, dev, &db_for_handler);

    let comm = comm_arc.clone();
    let run_result = ctrl.run_target(tgt_init, q_handler, move |dev: &UblkCtrl| {
        comm.send_dev_id(dev.dev_info().dev_id).unwrap();
    });

    if !read_only {
        log::info!("ublk device stopped, flushing RocksDB before exit...");
        db_arc.flush()?;
        log::info!("Final RocksDB flush successful.");
    }

    run_result.unwrap();

    Ok(0)
}
