use crate::notifier::Notifier;
use libublk::{
    ctrl::UblkCtrl,
    helpers::IoBuf,
    io::{BufDesc, UblkDev, UblkQueue},
    UblkError, UblkRuntime,
};
use rocksdb::{
    ColumnFamily, ColumnFamilyDescriptor, DBCompressionType, Options, SliceTransform, WriteBatch,
    WriteOptions, DB,
};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::Write;
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

struct CompressQueue {
    q: Rc<UblkQueue>,
    notifier: Arc<Notifier>,
    worker: DbWorker,
}

impl CompressQueue {
    fn new(q: Rc<UblkQueue>, db: Arc<DB>) -> Result<Self, std::io::Error> {
        let notifier = Arc::new(Notifier::new()?);
        let worker = DbWorker::start(db, notifier.clone());
        Ok(CompressQueue {
            q,
            notifier,
            worker,
        })
    }
}

/// One RocksDB request offloaded to the queue's [`DbWorker`].
enum DbWork {
    /// Read `nr_sectors` from `start_sector` into the caller's buffer.
    ///
    /// The raw pointer is valid because the submitting tag task owns the
    /// buffer and blocks on the reply before touching it again -- the
    /// same contract the previous spawn_blocking offload relied on.
    Read {
        start_sector: u64,
        nr_sectors: u32,
        buf_ptr: u64,
        buf_len: usize,
        lbs: u32,
    },
    Flush,
}

struct DbRequest {
    work: DbWork,
    tx: tokio::sync::oneshot::Sender<Result<i32, i32>>,
}

type DbQueue = std::sync::Mutex<(std::collections::VecDeque<DbRequest>, bool)>;

/// Dedicated blocking-side worker of one queue: it drains whole batches
/// of requests, runs them against RocksDB, publishes every result, and
/// fires one eventfd kick for the batch -- so the queue thread, parked
/// in io_uring_enter, pays one wakeup per batch instead of one per
/// request.
struct DbWorker {
    shared: Arc<(DbQueue, std::sync::Condvar)>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl DbWorker {
    fn start(db: Arc<DB>, notifier: Arc<Notifier>) -> Self {
        let shared: Arc<(DbQueue, std::sync::Condvar)> = Arc::new((
            std::sync::Mutex::new((std::collections::VecDeque::new(), false)),
            std::sync::Condvar::new(),
        ));
        let worker_shared = shared.clone();
        let handle = std::thread::spawn(move || {
            let (lock, cvar) = &*worker_shared;
            let mut batch = Vec::new();
            loop {
                {
                    let mut guard = lock.lock().unwrap();
                    while guard.0.is_empty() && !guard.1 {
                        guard = cvar.wait(guard).unwrap();
                    }
                    if guard.0.is_empty() && guard.1 {
                        return;
                    }
                    batch.extend(guard.0.drain(..));
                }
                for req in batch.drain(..) {
                    let res = match req.work {
                        DbWork::Read {
                            start_sector,
                            nr_sectors,
                            buf_ptr,
                            buf_len,
                            lbs,
                        } => {
                            // SAFETY: the submitting task owns the buffer and
                            // waits for this reply before reusing it.
                            let buf = unsafe {
                                std::slice::from_raw_parts_mut(buf_ptr as *mut u8, buf_len)
                            };
                            __handle_read(&db, start_sector, nr_sectors, buf, lbs)
                        }
                        DbWork::Flush => __handle_flush(&db),
                    };
                    let _ = req.tx.send(res);
                }
                // One wakeup for the whole batch, after every result is
                // published, so the eventfd CQE always finds the
                // receivers ready.
                notifier.notify().unwrap_or_else(|e| {
                    log::error!("Failed to notify blocking work done: {}", e);
                });
            }
        });
        DbWorker {
            shared,
            handle: Some(handle),
        }
    }

    async fn submit(&self, work: DbWork) -> Result<i32, i32> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        {
            let (lock, cvar) = &*self.shared;
            lock.lock().unwrap().0.push_back(DbRequest { work, tx });
            cvar.notify_one();
        }
        rx.await.expect("db worker dropped a request")
    }
}

impl Drop for DbWorker {
    fn drop(&mut self) {
        let (lock, cvar) = &*self.shared;
        lock.lock().unwrap().1 = true;
        cvar.notify_one();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
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
    cq: &CompressQueue,
    start_sector: u64,
    nr_sectors: u32,
    buf: &mut [u8],
    lbs: u32,
) -> Result<i32, i32> {
    cq.worker
        .submit(DbWork::Read {
            start_sector,
            nr_sectors,
            buf_ptr: buf.as_ptr() as u64,
            buf_len: buf.len(),
            lbs,
        })
        .await
}

fn __handle_flush(db: &DB) -> Result<i32, i32> {
    if let Err(e) = db.flush() {
        log::error!("Background rocksdb flush error: {}", e);
        Err(-libc::EIO)
    } else {
        Ok(0)
    }
}

async fn handle_flush(cq: &CompressQueue) -> Result<i32, i32> {
    cq.worker.submit(DbWork::Flush).await
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
    cq: &CompressQueue,
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

            handle_read(cq, iod.start_sector, iod.nr_sectors, buf_slice, lbs).await
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
        libublk::sys::UBLK_IO_OP_FLUSH => handle_flush(cq).await,
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
    cq: &CompressQueue,
    tag: u16,
    db: Arc<DB>,
    lbs: u32,
    read_only: bool,
) -> Result<(), UblkError> {
    let mut buf = IoBuf::<u8>::new(cq.q.dev().dev_info.max_io_buf_bytes as usize);

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

async fn handle_eventfd(cq: &CompressQueue) -> Result<(), UblkError> {
    loop {
        if cq.q.is_stopping() {
            break;
        }

        cq.notifier.event_read().await?;
        log::debug!("after eventfd reading");
    }
    Ok(())
}

fn q_async_fn(qid: u16, dev: &Arc<UblkDev>, db: Arc<DB>) -> Result<(), UblkError> {
    let depth = dev.dev_info.queue_depth;
    let lbs = 1u32 << dev.tgt.params.basic.logical_bs_shift;
    let read_only = (dev.tgt.params.basic.attrs & libublk::sys::UBLK_ATTR_READ_ONLY) != 0;
    let dev = dev.clone();

    let rt = UblkRuntime::new()?;
    rt.block_on(async move {
        let q = Rc::new(UblkQueue::new(qid, &dev)?);
        let cq_rc = Rc::new(
            CompressQueue::new(q, db.clone()).map_err(|_e| UblkError::OtherError(-libc::EINVAL))?,
        );
        let mut f_vec = Vec::new();

        for tag in 0..depth {
            let cq = cq_rc.clone();
            let db = db.clone();
            f_vec.push(libublk::tokio::task::spawn_local(async move {
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
        let event_task = libublk::tokio::task::spawn_local(async move {
            if let Err(e) = handle_eventfd(&cq).await {
                log::error!("handle_eventfd failed: {}", e);
            }
        });

        for f in f_vec {
            let _ = f.await;
        }

        // Queue is down: kick the eventfd task so it observes is_stopping()
        cq_rc.notifier.notify().unwrap_or_else(|e| {
            log::error!("Failed to notify eventfd task on shutdown: {}", e);
        });
        let _ = event_task.await;
        Ok(())
    })
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
