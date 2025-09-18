//use io_uring::{squeue, IoUring};
use io_uring::{squeue, IoUring};
use libublk::{
    ctrl::UblkCtrl,
    helpers::IoBuf,
    io::{with_queue_ring, with_queue_ring_mut, BufDesc, UblkDev, UblkQueue},
    uring_async::ublk_wake_task,
    UblkError,
};
use nix::sys::eventfd::{EfdFlags, EventFd};
use rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, DBCompressionType, Options, SliceTransform, WriteBatch,
    WriteOptions, DB,
};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::fd::FromRawFd;
use std::path::PathBuf;
use std::rc::Rc;
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

struct CompressQueue<'a> {
    q: &'a UblkQueue<'a>,
    eventfd: EventFd,
    eventfd_reading: RefCell<bool>,
}

impl<'a> CompressQueue<'a> {
    fn new(q: &'a UblkQueue<'a>) -> Result<Self, std::io::Error> {
        let eventfd = EventFd::from_value_and_flags(0, EfdFlags::EFD_CLOEXEC)?;
        Ok(CompressQueue {
            q,
            eventfd,
            eventfd_reading: RefCell::new(false),
        })
    }
}

fn __handle_read(
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

async fn handle_read(
    cq: &CompressQueue<'_>,
    db: Arc<DB>,
    start_sector: u64,
    nr_sectors: u32,
    buf: &mut [u8],
    lbs: u32,
) -> Result<i32, i32> {
    let buf_ptr = buf.as_ptr() as u64;
    let buf_len = buf.len();
    let db_clone = db.clone();

    // Offload to thread pool
    let res = smol::unblock(move || {
        // Reconstruct the slice safely since the buffer outlive the closure
        let buf_slice = unsafe { std::slice::from_raw_parts_mut(buf_ptr as *mut u8, buf_len) };
        let res = __handle_read(&db_clone, start_sector, nr_sectors, buf_slice, lbs);
        log::info!(" read offload done");
        res
    })
    .await;

    log::info!("handle read done");
    nix::unistd::write(&cq.eventfd, &1u64.to_le_bytes()).unwrap();
    res
}

fn __handle_flush(db: &DB) -> Result<i32, i32> {
    if let Err(e) = db.flush() {
        log::error!("Background rocksdb flush error: {}", e);
        Err(-libc::EIO)
    } else {
        Ok(0)
    }
}

async fn handle_flush(cq: &CompressQueue<'_>, db: Arc<DB>) -> Result<i32, i32> {
    let db_clone = db.clone();

    // Offload to thread pool
    let res = smol::unblock(move || __handle_flush(&db_clone)).await;
    // Notify via eventfd
    nix::unistd::write(&cq.eventfd, &1u64.to_le_bytes()).unwrap();
    res
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
        batch.put(key, block_buf);
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

async fn handle_compress_io_cmd_async(
    cq: &CompressQueue<'_>,
    tag: u16,
    buf: &mut IoBuf<u8>,
    db: &Arc<DB>,
    lbs: u32,
    read_only: bool,
) -> Result<i32, i32> {
    let iod = cq.q.get_iod(tag);
    let op = iod.op_flags & 0xff;

    match op as u32 {
        libublk::sys::UBLK_IO_OP_READ => {
            let buf_len = std::cmp::min((iod.nr_sectors << 9) as usize, buf.len());
            let buf_slice = &mut buf.as_mut_slice()[..buf_len];

            log::info!("before read tid {}", unsafe { libc::gettid() });
            let res = handle_read(
                cq,
                db.clone(),
                iod.start_sector,
                iod.nr_sectors,
                buf_slice,
                lbs,
            )
            .await;
            log::info!("after read tid {} res {:?}", unsafe { libc::gettid() }, res);

            res
        }
        libublk::sys::UBLK_IO_OP_WRITE => {
            if read_only {
                Err(-libc::EACCES)
            } else {
                let buf_len = std::cmp::min((iod.nr_sectors << 9) as usize, buf.len());
                let buf_slice = &buf.as_slice()[..buf_len];
                handle_write(db, iod.start_sector, iod.nr_sectors, buf_slice, lbs)
            }
        }
        libublk::sys::UBLK_IO_OP_FLUSH => handle_flush(cq, db.clone()).await,
        libublk::sys::UBLK_IO_OP_DISCARD => {
            if read_only {
                Err(-libc::EACCES)
            } else {
                let cf = db.cf_handle("default").unwrap();
                handle_discard(db, cf, iod.start_sector, iod.nr_sectors, lbs)
            }
        }
        _ => Err(-libc::EINVAL),
    }
}

async fn handle_queue_tag_async_compress(
    cq: &CompressQueue<'_>,
    tag: u16,
    db: Arc<DB>,
    lbs: u32,
    read_only: bool,
) -> Result<(), UblkError> {
    let mut buf = IoBuf::<u8>::new(cq.q.dev.dev_info.max_io_buf_bytes as usize);

    // Submit initial prep command
    cq.q.submit_io_prep_cmd(tag, BufDesc::Slice(buf.as_slice()), 0, Some(&buf))
        .await?;
    loop {
        let res = handle_compress_io_cmd_async(&cq, tag, &mut buf, &db, lbs, read_only).await;
        let buf_desc = BufDesc::Slice(buf.as_slice());
        match res {
            Ok(result) => {
                cq.q.submit_io_commit_cmd(tag, buf_desc, result).await?;
            }
            Err(error) => {
                cq.q.submit_io_commit_cmd(tag, buf_desc, error).await?;
            }
        }
    }
}

async fn handle_eventfd(cq: &CompressQueue<'_>) -> Result<(), UblkError> {
    let mut buf = [0u8; 8];
    loop {
        if cq.q.is_stopping() {
            break;
        }

        *cq.eventfd_reading.borrow_mut() = true;
        let eventfd = cq.eventfd.as_raw_fd();
        let sqe =
            io_uring::opcode::Read::new(io_uring::types::Fd(eventfd), buf.as_mut_ptr(), 8).build();
        log::info!("before eventfd reading");
        cq.q.ublk_submit_sqe(sqe).await;
        *cq.eventfd_reading.borrow_mut() = false;
        log::info!("after eventfd reading");
    }
    Ok(())
}

async fn reap_events(
    q: &UblkQueue<'_>,
    async_uring: &smol::Async<std::fs::File>,
) -> Result<(), UblkError> {
    with_queue_ring_mut(q, |r| r.submit())?;
    log::info!("before readable tid {}", unsafe { libc::gettid() });
    async_uring
        .readable()
        .await
        .map_err(|_| UblkError::OtherError(-libc::EIO))?;
    log::info!("after readable {}", unsafe { libc::gettid() });
    q.flush_and_wake_io_tasks(|data, cqe, _| ublk_wake_task(data, cqe), 0)?;
    Ok(())
}

async fn handle_uring_events(
    exe: &smol::LocalExecutor<'_>,
    cq: &CompressQueue<'_>,
) -> Result<(), UblkError> {
    let q = cq.q;
    // Register io_uring FD with smol's async reactor
    let uring_fd = with_queue_ring(q, |ring| ring.as_raw_fd());
    let file = unsafe { File::from_raw_fd(uring_fd) };
    let async_uring = smol::Async::new(file).map_err(|_e| UblkError::OtherError(-libc::EINVAL))?;

    smol::future::yield_now().await;
    loop {
        if q.is_stopping() {
            break;
        }
        reap_events(q, &async_uring).await?;
        while exe.try_tick() {}
    }
    // Only do these operations if eventfd reading is active
    if *cq.eventfd_reading.borrow() {
        log::info!("write to eventfd {}", unsafe { libc::gettid() });
        nix::unistd::write(&cq.eventfd, &1u64.to_le_bytes()).unwrap();
        libublk::io::with_queue_ring_mut(cq.q, |r: &mut IoUring<squeue::Entry>| {
            let c = match r.submit_and_wait(1) {
                Err(_) => None,
                _ => r.completion().next(),
            };
            match c {
                Some(cqe) => {
                    let user_data = cqe.user_data();
                    log::info!("wait task user_data {:x}", user_data);
                    libublk::uring_async::ublk_wake_task(user_data, &cqe);
                }
                _ => {}
            }
        })
    }
    Ok(())
}

fn q_async_fn(qid: u16, dev: &UblkDev, db: Arc<DB>) -> Result<(), UblkError> {
    let depth = dev.dev_info.queue_depth;
    let q = UblkQueue::new(qid, dev)?;
    let cq_rc = Rc::new(CompressQueue::new(&q).map_err(|_e| UblkError::OtherError(-libc::EINVAL))?);
    let exe_rc = Rc::new(smol::LocalExecutor::new());
    let exe = exe_rc.clone();
    let mut f_vec = Vec::new();
    let lbs = 1u32 << dev.tgt.params.basic.logical_bs_shift;
    let read_only = (dev.tgt.params.basic.attrs & libublk::sys::UBLK_ATTR_READ_ONLY) != 0;

    for tag in 0..depth {
        let cq = cq_rc.clone();
        let db = db.clone();
        f_vec.push(exe.spawn(async move {
            match handle_queue_tag_async_compress(&cq, tag, db, lbs, read_only).await {
                Err(UblkError::QueueIsDown) | Ok(_) => {}
                Err(e) => log::error!(
                    "handle_queue_tag_async_compress failed for tag {}: {}",
                    tag,
                    e
                ),
            }
        }));
    }

    let cq = cq_rc.clone();
    f_vec.push(exe.spawn(async move {
        if let Err(e) = handle_eventfd(&cq).await {
            log::error!("handle_eventfd failed: {}", e);
        }
    }));

    let cq = cq_rc.clone();
    let exe2 = exe_rc.clone();
    f_vec.push(exe.spawn(async move {
        if let Err(e) = handle_uring_events(&exe2, &cq).await {
            log::error!("handle_uring_events failed: {}", e);
        }
    }));

    smol::block_on(exe_rc.run(async { futures::future::join_all(f_vec).await }));
    Ok(())
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
    let compression_type = parse_compression_type(compression)?;
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
    let read_only = args_opt.as_ref().is_some_and(|o| o.gen_arg.read_only);
    let flags = ctrl.dev_info().flags;

    if flags & libublk::sys::UBLK_F_UNPRIVILEGED_DEV as u64 != 0 {
        return Err(anyhow::anyhow!("compress doesn't support unprivileged"));
    }

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

    let q_handler = move |qid, dev: &_| {
        let result = q_async_fn(qid, dev, db_for_handler.clone());
        if let Err(e) = result {
            log::error!("Queue handler failed for queue {}: {}", qid, e);
        }
    };

    let comm = comm_arc.clone();
    match ctrl.run_target(tgt_init, q_handler, move |dev: &UblkCtrl| {
        if let Err(e) = comm.send_dev_id(dev.dev_info().dev_id) {
            log::error!("Failed to send device ID: {}", e);
        }
    }) {
        Ok(_) => {}
        Err(e) => log::error!("Failed to run target: {}", e),
    }

    if !read_only {
        log::info!("ublk device stopped, flushing RocksDB before exit...");
        db_arc.flush()?;
        log::info!("Final RocksDB flush successful.");
    }

    Ok(0)
}
