use libublk::{
    ctrl::UblkCtrl,
    helpers::IoBuf,
    io::{UblkDev, UblkIOCtx, UblkQueue},
    UblkIORes,
};
use nix::sys::eventfd::{eventfd, EfdFlags};
use rocksdb::{
    ColumnFamily, DBCompressionType, Options, SliceTransform, WriteBatch, WriteOptions, DB,
};
use serde::{Deserialize, Serialize};
use serde_json;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;

#[derive(clap::Args, Debug)]
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

struct ReadJob {
    tag: u16,
    start_sector: u64,
    nr_sectors: u32,
    buf_addr: u64,
}

struct ReadCompletion {
    tag: u16,
    result: Result<i32, i32>,
}

struct FlushJob {
    tag: u16,
}

struct FlushCompletion {
    tag: u16,
    result: Result<i32, i32>,
}

const FLUSH_POLL_TAG: u16 = u16::MAX;
const READ_POLL_TAG: u16 = u16::MAX - 1;

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

fn submit_poll_sqe(q: &UblkQueue, efd: i32, tag: u16, op: u32) {
    let user_data = UblkIOCtx::build_user_data(tag, op, 0, true);
    let sqe = io_uring::opcode::PollAdd::new(io_uring::types::Fd(efd), libc::POLLIN as _)
        .build()
        .user_data(user_data);
    q.ublk_submit_sqe_sync(sqe).unwrap();
}

fn handle_flush_completion(
    q: &UblkQueue,
    efd: i32,
    bufs_ptr: *mut IoBuf<u8>,
    completion_rx: &Receiver<FlushCompletion>,
) {
    let mut buf = [0u8; 8];
    nix::unistd::read(efd, &mut buf).unwrap();

    while let Ok(completion) = completion_rx.try_recv() {
        let result = match completion.result {
            Ok(r) => UblkIORes::Result(r),
            Err(e) => UblkIORes::Result(e),
        };
        let tag = completion.tag;
        let buf_addr = unsafe {
            let io_buf: &mut IoBuf<u8> = &mut *bufs_ptr.add(tag as usize);
            io_buf.as_mut().as_mut_ptr()
        };
        q.complete_io_cmd(tag, buf_addr, Ok(result));
    }
    submit_poll_sqe(q, efd, FLUSH_POLL_TAG, libublk::sys::UBLK_IO_OP_FLUSH);
}

fn handle_read_completion(
    q: &UblkQueue,
    completion_rx: &Receiver<ReadCompletion>,
    read_efd: i32,
    bufs_ptr: *mut IoBuf<u8>,
) {
    let mut efd_buf = [0u8; 8];
    nix::unistd::read(read_efd, &mut efd_buf).unwrap();
    while let Ok(completion) = completion_rx.try_recv() {
        let result = match completion.result {
            Ok(r) => UblkIORes::Result(r),
            Err(e) => UblkIORes::Result(e),
        };
        let tag = completion.tag;
        let buf_addr = unsafe {
            let io_buf: &mut IoBuf<u8> = &mut *bufs_ptr.add(tag as usize);
            io_buf.as_mut().as_mut_ptr()
        };
        q.complete_io_cmd(tag, buf_addr, Ok(result));
    }
    submit_poll_sqe(q, read_efd, READ_POLL_TAG, libublk::sys::UBLK_IO_OP_READ);
}

fn q_sync_fn(qid: u16, dev: &UblkDev, db: &Arc<DB>) {
    let mut bufs = dev.alloc_queue_io_bufs();
    let bufs_ptr = bufs.as_mut_ptr();
    let flush_efd = eventfd(0, EfdFlags::EFD_CLOEXEC).unwrap();
    let read_efd = eventfd(0, EfdFlags::EFD_CLOEXEC).unwrap();
    let max_buf_len = dev.dev_info.max_io_buf_bytes as usize;
    let lbs = 1u32 << dev.tgt.params.basic.logical_bs_shift;
    let read_only = (dev.tgt.params.basic.attrs & libublk::sys::UBLK_ATTR_READ_ONLY) != 0;

    let (read_job_tx, read_job_rx): (Sender<ReadJob>, Receiver<ReadJob>) = channel();
    let (read_completion_tx, read_completion_rx): (
        Sender<ReadCompletion>,
        Receiver<ReadCompletion>,
    ) = channel();

    let (flush_job_tx, flush_job_rx): (Sender<FlushJob>, Receiver<FlushJob>) = channel();
    let (flush_completion_tx, flush_completion_rx): (
        Sender<FlushCompletion>,
        Receiver<FlushCompletion>,
    ) = channel();

    let db_clone = db.clone();
    std::thread::spawn(move || {
        for job in read_job_rx {
            let buf_slice = unsafe {
                std::slice::from_raw_parts_mut(
                    job.buf_addr as *mut u8,
                    (job.nr_sectors << 9) as usize,
                )
            };
            let res = handle_read(&db_clone, job.start_sector, job.nr_sectors, buf_slice, lbs);
            read_completion_tx
                .send(ReadCompletion {
                    tag: job.tag,
                    result: res,
                })
                .unwrap();
            nix::unistd::write(read_efd, &1u64.to_le_bytes()).unwrap();
        }
    });

    let db_clone2 = db.clone();
    std::thread::spawn(move || {
        for job in flush_job_rx {
            let res = if let Err(e) = db_clone2.flush() {
                log::error!("Background rocksdb flush error: {}", e);
                Err(-libc::EIO)
            } else {
                Ok(0)
            };
            flush_completion_tx
                .send(FlushCompletion {
                    tag: job.tag,
                    result: res,
                })
                .unwrap();
            nix::unistd::write(flush_efd, &1u64.to_le_bytes()).unwrap();
        }
    });

    let q = UblkQueue::new(qid, dev)
        .unwrap()
        .regiser_io_bufs(Some(&bufs))
        .submit_fetch_commands(Some(&bufs));
    submit_poll_sqe(&q, read_efd, READ_POLL_TAG, libublk::sys::UBLK_IO_OP_READ);
    submit_poll_sqe(
        &q,
        flush_efd,
        FLUSH_POLL_TAG,
        libublk::sys::UBLK_IO_OP_FLUSH,
    );

    let io_handler = {
        let db_clone = db.clone();

        move |q: &UblkQueue, tag: u16, io_ctx: &UblkIOCtx| {
            if io_ctx.is_tgt_io() {
                match UblkIOCtx::user_data_to_op(io_ctx.user_data()) {
                    libublk::sys::UBLK_IO_OP_FLUSH => {
                        handle_flush_completion(q, flush_efd, bufs_ptr, &flush_completion_rx);
                        return;
                    }
                    libublk::sys::UBLK_IO_OP_READ => {
                        handle_read_completion(q, &read_completion_rx, read_efd, bufs_ptr);
                        return;
                    }
                    _ => {}
                }
            }

            let iod = q.get_iod(tag);
            let op = iod.op_flags & 0xff;

            let buf = unsafe {
                let buf_len = std::cmp::min((iod.nr_sectors << 9) as usize, max_buf_len);
                let io_buf: &mut IoBuf<u8> = &mut *bufs_ptr.add(tag as usize);
                &mut io_buf.as_mut()[..buf_len]
            };

            let res = match op {
                libublk::sys::UBLK_IO_OP_READ => {
                    read_job_tx
                        .send(ReadJob {
                            tag,
                            start_sector: iod.start_sector,
                            nr_sectors: iod.nr_sectors,
                            buf_addr: buf.as_mut_ptr() as u64,
                        })
                        .unwrap();
                    return;
                }
                libublk::sys::UBLK_IO_OP_WRITE => {
                    if read_only {
                        Err(-libc::EACCES)
                    } else {
                        handle_write(&db_clone, iod.start_sector, iod.nr_sectors, buf, lbs)
                    }
                }
                libublk::sys::UBLK_IO_OP_DISCARD => {
                    if read_only {
                        Err(-libc::EACCES)
                    } else {
                        let cf = db_clone.cf_handle("default").unwrap();
                        handle_discard(&db_clone, cf, iod.start_sector, iod.nr_sectors, lbs)
                    }
                }
                libublk::sys::UBLK_IO_OP_FLUSH => {
                    flush_job_tx.send(FlushJob { tag }).unwrap();
                    return;
                }
                _ => Err(-libc::EINVAL),
            };

            let result = match res {
                Ok(r) => UblkIORes::Result(r),
                Err(e) => UblkIORes::Result(e),
            };
            q.complete_io_cmd(tag, buf.as_mut_ptr(), Ok(result));
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

pub(crate) fn ublk_add_compress(
    ctrl: UblkCtrl,
    opt: Option<CompressAddArgs>,
    comm_arc: &Arc<crate::DevIdComm>,
) -> anyhow::Result<i32> {
    let (db_path, size, compression, lbs, pbs, args_opt) = if let Some(o) = opt {
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
        (dir, size, compression, lbs, pbs, Some(o))
    } else {
        let val = ctrl
            .get_target_data_from_json()
            .ok_or_else(|| anyhow::anyhow!("no json data for recovery"))?;
        let dir_str = val["compress"]["dir"].as_str().unwrap().to_string();
        let dir = PathBuf::from(dir_str);
        let json_path = dir.join("ublk_compress.json");
        let file = File::open(json_path)?;
        let config: CompressJson = serde_json::from_reader(file)?;
        (
            dir,
            config.size,
            config.compression,
            config.logical_block_size,
            config.physical_block_size,
            None,
        )
    };

    let mut db_opts = Options::default();
    db_opts.create_if_missing(true);
    db_opts.set_use_fsync(false);
    db_opts.set_use_direct_io_for_flush_and_compaction(true);
    db_opts.set_compression_type(parse_compression_type(&compression)?);
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

    let cfs = vec!["default"];
    let read_only = args_opt.as_ref().map_or(false, |o| o.gen_arg.read_only);
    let db = if read_only {
        DB::open_cf_as_secondary(&db_opts, &db_path, &db_path, &cfs).unwrap()
    } else {
        DB::open_cf(&db_opts, &db_path, &cfs).unwrap()
    };
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
