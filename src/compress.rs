use libublk::{
    ctrl::UblkCtrl,
    helpers::IoBuf,
    io::{UblkDev, UblkIOCtx, UblkQueue},
    UblkIORes,
};
use rocksdb::{BlockBasedOptions, Cache, Options, SliceTransform, WriteBatch, WriteOptions, DB, DBCompressionType};
use serde::{Deserialize, Serialize};
use serde_json;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
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
}

fn handle_read(db: &DB, start_sector: u64, nr_sectors: u32, buf: &mut [u8]) -> Result<i32, i32> {
    let keys: Vec<_> = (0..nr_sectors)
        .map(|i| (start_sector + i as u64).to_be_bytes())
        .collect();
    let results = db.multi_get(&keys);

    for (i, result) in results.iter().enumerate() {
        let sector_buf = &mut buf[(i << 9) as usize..((i + 1) << 9) as usize];
        match result {
            Ok(Some(value)) => {
                sector_buf.copy_from_slice(value);
            }
            Ok(None) => {
                // Not found, fill with zeros
                for byte in sector_buf.iter_mut() {
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

fn handle_write(db: &DB, start_sector: u64, nr_sectors: u32, buf: &[u8]) -> Result<i32, i32> {
    let mut batch = WriteBatch::default();
    for i in 0..nr_sectors {
        let key = (start_sector + i as u64).to_be_bytes();
        let sector_buf = &buf[(i << 9) as usize..((i + 1) << 9) as usize];
        batch.put(&key, sector_buf);
    }
    let mut write_options = WriteOptions::new();
    write_options.disable_wal(true);
    if let Err(e) = db.write_opt(batch, &write_options) {
        log::error!("rocksdb write batch error: {}", e);
        return Err(-libc::EIO);
    }
    Ok((nr_sectors << 9) as i32)
}

fn handle_flush(db: &DB) -> Result<i32, i32> {
    if let Err(e) = db.flush() {
        log::error!("rocksdb flush error: {}", e);
        Err(-libc::EIO)
    } else {
        Ok(0)
    }
}

fn q_sync_fn(qid: u16, dev: &UblkDev, db: &Arc<DB>) {
    let mut bufs = dev.alloc_queue_io_bufs();
    let bufs_ptr = bufs.as_mut_ptr();

    let io_handler = move |q: &UblkQueue, tag: u16, _io: &UblkIOCtx| {
        let iod = q.get_iod(tag);
        let op = iod.op_flags & 0xff;
        
        let buf = unsafe {
            let io_buf: &mut IoBuf<u8> = &mut *bufs_ptr.add(tag as usize);
            &mut io_buf.as_mut()[..(iod.nr_sectors << 9) as usize]
        };

        let res = match op {
            libublk::sys::UBLK_IO_OP_READ => handle_read(db, iod.start_sector, iod.nr_sectors, buf),
            libublk::sys::UBLK_IO_OP_WRITE => handle_write(db, iod.start_sector, iod.nr_sectors, buf),
            libublk::sys::UBLK_IO_OP_FLUSH => handle_flush(db),
            _ => Err(-libc::EINVAL),
        };

        let result = match res {
            Ok(r) => UblkIORes::Result(r),
            Err(e) => UblkIORes::Result(e),
        };
        q.complete_io_cmd(tag, buf.as_mut_ptr(), Ok(result));
    };

    let queue = UblkQueue::new(qid, dev).unwrap();
    
    queue
        .regiser_io_bufs(Some(&bufs))
        .submit_fetch_commands(Some(&bufs))
        .wait_and_handle_io(io_handler);
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
    let (dir, size, compression, args_opt) = if let Some(o) = opt {
        let dir = o.gen_arg.build_abs_path(o.dir.clone());
        let json_path = dir.join("ublk_compress.json");

        let (size, compression) = if json_path.exists() {
            let file = File::open(&json_path)?;
            let config: CompressJson = serde_json::from_reader(file)?;
            (config.size, config.compression)
        } else {
            let size_str = o.size.as_ref().ok_or_else(|| anyhow::anyhow!("--size is required for new device"))?;
            let size = parse_size::parse_size(size_str)?;
            let compression = o.compression.clone();
            let config = CompressJson { size, dir: dir.clone(), compression: compression.clone() };
            let mut file = OpenOptions::new().create(true).write(true).open(&json_path)?;
            file.write_all(serde_json::to_string_pretty(&config)?.as_bytes())?;
            (size, compression)
        };
        (dir, size, compression, Some(o))
    } else {
        let val = ctrl.get_target_data_from_json().ok_or_else(|| anyhow::anyhow!("no json data for recovery"))?;
        let dir_str = val["compress"]["dir"].as_str().unwrap().to_string();
        let dir = PathBuf::from(dir_str);
        let json_path = dir.join("ublk_compress.json");
        let file = File::open(json_path)?;
        let config: CompressJson = serde_json::from_reader(file)?;
        (dir, config.size, config.compression, None)
    };

    let db_path = dir.join("ublk_compress");

    let mut db_opts = Options::default();
    db_opts.create_if_missing(true);
    db_opts.set_use_fsync(false);
    db_opts.set_use_direct_io_for_flush_and_compaction(true);
    db_opts.set_compression_type(parse_compression_type(&compression)?);
    db_opts.set_write_buffer_size(64 * 1024 * 1024);
    db_opts.set_max_write_buffer_number(4);
    db_opts.set_max_background_jobs(4);

    let cache = Cache::new_lru_cache(256 * 1024 * 1024);
    let mut block_based_opts = BlockBasedOptions::default();
    block_based_opts.set_block_size(16 * 1024);
    block_based_opts.set_block_cache(&cache);
    block_based_opts.set_bloom_filter(10.0, false);
    db_opts.set_block_based_table_factory(&block_based_opts);

    db_opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(8));
    db_opts.set_memtable_prefix_bloom_ratio(0.1);
    db_opts.set_optimize_filters_for_hits(true);

    let db = DB::open(&db_opts, &db_path).unwrap();
    let db_arc = Arc::new(db);

    let tgt_init = |dev: &mut UblkDev| {
        dev.set_default_params(size);
        if let Some(ref args) = args_opt {
            args.gen_arg.apply_block_size(dev);
            args.gen_arg.apply_read_only(dev);
            let val = serde_json::json!({"compress": { "dir": &dir }});
            dev.set_target_json(val);
        }
        dev.tgt.params.basic.attrs |= libublk::sys::UBLK_ATTR_VOLATILE_CACHE;
        Ok(())
    };

    let q_handler = move |qid, dev: &_| {
        q_sync_fn(qid, dev, &db_arc.clone())
    };

    let comm = comm_arc.clone();
    ctrl.run_target(tgt_init, q_handler, move |dev: &UblkCtrl| {
        comm.send_dev_id(dev.dev_info().dev_id).unwrap();
    })
    .unwrap();

    Ok(0)
}