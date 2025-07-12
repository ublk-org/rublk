use crate::offload::{
    handler::{setup_worker_thread, Completion, OffloadHandler, OffloadJob, QueueHandler},
    OffloadTargetLogic, OffloadType,
};
use libublk::{
    ctrl::UblkCtrl,
    io::{UblkDev, UblkIOCtx, UblkQueue},
    UblkIORes,
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

fn handle_flush(db: &DB) -> Result<i32, i32> {
    if let Err(e) = db.flush() {
        log::error!("Background rocksdb flush error: {}", e);
        Err(-libc::EIO)
    } else {
        Ok(0)
    }
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

fn handle_offload_fn(
    db: &Arc<DB>,
    lbs: u32,
    job: OffloadJob,
) -> Completion {
    let res = match job.op as u32 {
        libublk::sys::UBLK_IO_OP_READ => {
            let buf_slice = unsafe {
                std::slice::from_raw_parts_mut(
                    job.buf_addr as *mut u8,
                    (job.nr_sectors << 9) as usize,
                )
            };
            handle_read(db, job.start_sector, job.nr_sectors, buf_slice, lbs)
        }
        libublk::sys::UBLK_IO_OP_FLUSH => handle_flush(db),
        _ => Err(-libc::EINVAL),
    };
    Completion {
        tag: job.tag,
        result: res,
        buf_addr: job.buf_addr,
    }
}

impl<'a> CompressTarget {
    fn setup_one_handler(&self, handler: &mut QueueHandler<'a, Self>, op_type: OffloadType) {
        let idx = op_type as usize;
        let efd = nix::sys::eventfd::eventfd(0, nix::sys::eventfd::EfdFlags::EFD_CLOEXEC).unwrap();
        let db = self.db.clone();
        let lbs = self.lbs;
        let (tx, rx) = setup_worker_thread(efd, move |job: OffloadJob| {
            handle_offload_fn(&db, lbs, job)
        });

        handler.offload_handlers[idx] = Some(OffloadHandler::new(
            handler.q,
            idx as u32,
            tx,
            rx,
            efd,
        ));
    }
}

impl<'a> OffloadTargetLogic<'a> for CompressTarget {
    fn setup_offload_handlers(&self, handler: &mut QueueHandler<'a, Self>) {
        self.setup_one_handler(handler, OffloadType::Read);
        self.setup_one_handler(handler, OffloadType::Flush);
    }

    fn handle_io(
        &self,
        handler: &mut QueueHandler<'a, Self>,
        tag: u16,
        io_ctx: &UblkIOCtx,
        buf: Option<&mut [u8]>,
    ) -> Result<i32, i32> {
        if io_ctx.is_tgt_io() && io_ctx.get_tag() as u16 == crate::offload::handler::POLL_TAG {
            let handler_idx = UblkIOCtx::user_data_to_op(io_ctx.user_data()) as usize;
            if let Some(Some(h)) = handler.offload_handlers.get_mut(handler_idx) {
                h.handle_completion(handler_idx as u32);
            }
            return Ok(0);
        }

        let q = handler.q;
        let iod = q.get_iod(tag);
        let op = (iod.op_flags & 0xff) as u16;
        let buf = buf.unwrap();

        let res = match op as u32 {
            libublk::sys::UBLK_IO_OP_READ => {
                if let Some(Some(h)) = handler.offload_handlers.get(OffloadType::Read as usize) {
                    h.send_job(op, tag, iod, buf);
                }
                return Ok(0);
            }
            libublk::sys::UBLK_IO_OP_FLUSH => {
                if let Some(Some(h)) = handler.offload_handlers.get(OffloadType::Flush as usize) {
                    h.send_job(op, tag, iod, buf);
                }
                return Ok(0);
            }
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
        };

        let result = match res {
            Ok(r) => UblkIORes::Result(r),
            Err(e) => UblkIORes::Result(e),
        };
        q.complete_io_cmd(tag, buf.as_mut_ptr(), Ok(result));
        Ok(0)
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
    let mut handler = crate::offload::handler::QueueHandler::new(&q, &target);
    let max_io_buf_bytes = dev.dev_info.max_io_buf_bytes as usize;

    let io_handler = |_q: &UblkQueue, tag: u16, io_ctx: &UblkIOCtx| {
        if io_ctx.is_tgt_io() && tag == crate::offload::handler::POLL_TAG {
            handler.handle_event(tag, io_ctx, None);
        } else {
            let iod = q.get_iod(tag);
            let buf_len =
                std::cmp::min((iod.nr_sectors << 9) as usize, max_io_buf_bytes);
            let buf = &mut bufs[tag as usize].as_mut()[..buf_len];
            handler.handle_event(tag, io_ctx, Some(buf));
        }
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
