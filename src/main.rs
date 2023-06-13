use anyhow::Result as AnyRes;
use clap::{Args, Parser, Subcommand};
use libublk::{UblkCtrl, UblkDev, UblkIO, UblkQueue};
use log::{error, trace};
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

struct NoneOps {}
struct NoneQueueOps {}

impl libublk::UblkTgtOps for NoneOps {
    fn init_tgt(&self, dev: &UblkDev, _tgt_data: serde_json::Value) -> AnyRes<serde_json::Value> {
        trace!("none: init_tgt {}", dev.dev_info.dev_id);
        let info = dev.dev_info;
        let dev_size = 250_u64 << 30;

        let mut tgt = dev.tgt.borrow_mut();

        tgt.dev_size = dev_size;
        tgt.params = libublk::ublk_params {
            types: libublk::UBLK_PARAM_TYPE_BASIC,
            basic: libublk::ublk_param_basic {
                logical_bs_shift: 9,
                physical_bs_shift: 12,
                io_opt_shift: 12,
                io_min_shift: 9,
                max_sectors: info.max_io_buf_bytes >> 9,
                dev_sectors: dev_size >> 9,
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(serde_json::json!({}))
    }
    fn deinit_tgt(&self, dev: &UblkDev) {
        trace!("none: deinit_tgt {}", dev.dev_info.dev_id);
    }
}

impl libublk::UblkQueueOps for NoneQueueOps {
    fn queue_io(&self, q: &UblkQueue, io: &mut UblkIO, tag: u32) -> AnyRes<i32> {
        let iod = q.get_iod(tag);
        let bytes = unsafe { (*iod).nr_sectors << 9 } as i32;

        q.complete_io(io, tag as u16, bytes);
        Ok(0)
    }
    fn tgt_io_done(&self, _q: &UblkQueue, _tag: u32, _res: i32, _user_data: u64) {}
}

#[derive(Parser)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Adds ublk target
    Add(AddArgs),
    /// Deletes ublk target
    Del(DelArgs),
    /// Lists ublk targets
    List(UblkArgs),
    /// Recover ublk targets
    Recover(UblkArgs),
}

#[derive(Args)]
struct AddArgs {
    ///For ublk-loop only
    #[clap(long, short = 'f')]
    file: Option<PathBuf>,

    ///Config file for creating ublk(json format)
    #[clap(long)]
    config: Option<PathBuf>,

    #[clap(long, short = 'n', default_value_t=-1)]
    number: i32,

    #[clap(long, short = 't', default_value = "none")]
    r#type: String,

    #[clap(long, short = 'q', default_value_t = 1)]
    queue: u32,

    #[clap(long, short = 'd', default_value_t = 128)]
    depth: u32,
}

#[derive(Args)]
struct DelArgs {
    #[clap(long, short = 'n', default_value_t = -1)]
    number: i32,

    #[clap(long, short = 'a', default_value_t = false)]
    all: bool,
}

#[derive(Args)]
struct UblkArgs {
    #[clap(long, short = 'n', default_value_t = -1)]
    number: i32,
}

fn ublk_queue_fn(
    dev: &UblkDev,
    q_id: u16,
    qdata: libublk::UblkQueueAffinity,
    tid: Arc<(Mutex<i32>, Condvar)>,
) {
    let cq_depth = dev.dev_info.queue_depth as u32;
    let sq_depth = cq_depth;
    let q = UblkQueue::new(Box::new(NoneQueueOps {}), q_id, dev, sq_depth, cq_depth, 0).unwrap();

    let (lock, cvar) = &*tid;
    unsafe {
        let mut guard = lock.lock().unwrap();
        *guard = libc::gettid();
        cvar.notify_one();
    }
    unsafe {
        libc::pthread_setaffinity_np(
            libc::pthread_self(),
            qdata.buf_len(),
            qdata.addr() as *const libc::cpu_set_t,
        );
    }

    q.submit_fetch_commands();
    loop {
        if q.process_io() < 0 {
            break;
        }
    }
}

fn ublk_daemon_work(opt: &AddArgs) -> AnyRes<i32> {
    let mut ctrl = UblkCtrl::new(opt.number, opt.queue, opt.depth, 512_u32 * 1024, 0, true)?;
    let ublk_dev = Arc::new(UblkDev::new(
        Box::new(NoneOps {}),
        &mut ctrl,
        &opt.r#type,
        0,
        serde_json::json!({}),
    )?);

    let nr_queues = ublk_dev.dev_info.nr_hw_queues;
    let mut qdata = Vec::new();
    let mut threads = Vec::new();
    let mut tids = Vec::<Arc<(Mutex<i32>, Condvar)>>::with_capacity(nr_queues as usize);

    ctrl.get_info().unwrap();

    for q in 0..nr_queues {
        let mut data = libublk::UblkQueueAffinity::new();
        let tid = Arc::new((Mutex::new(0_i32), Condvar::new()));

        ctrl.get_queue_affinity(q as u32, &mut data)?;

        let _dev = Arc::clone(&ublk_dev);
        let _q = q.clone();
        let _data = data.clone();
        let _tid = Arc::clone(&tid);

        threads.push(thread::spawn(move || {
            ublk_queue_fn(&_dev, _q, _data, _tid);
        }));
        qdata.push((data, 0));
        tids.push(tid);
    }

    for q in 0..nr_queues {
        let (lock, cvar) = &*tids[q as usize];

        let mut guard = lock.lock().unwrap();
        while *guard == 0 {
            guard = cvar.wait(guard).unwrap();
        }
        qdata[q as usize].1 = *guard;
    }

    let params = ublk_dev.tgt.borrow();
    ctrl.set_params(&params.params).unwrap();
    ctrl.start(unsafe { libc::getpid() as i32 }).unwrap();

    //Now we are up, and build & export json
    ctrl.build_json(&ublk_dev, qdata);
    ctrl.flush_json()?;

    trace!("ctrl {} start {:?}", ctrl.dev_info.dev_id, ctrl.dev_info);

    ctrl.dump();

    for qh in threads {
        qh.join().unwrap_or_else(|_| {
            error!("dev-{} join queue thread failed", ublk_dev.dev_info.dev_id)
        });
    }

    ctrl.stop().unwrap();

    Ok(0)
}

fn ublk_add(opt: &AddArgs) -> AnyRes<i32> {
    let daemonize = daemonize::Daemonize::new()
        .stdout(daemonize::Stdio::keep())
        .stderr(daemonize::Stdio::keep());

    match daemonize.start() {
        Ok(_) => ublk_daemon_work(opt),
        Err(e) => Err(anyhow::anyhow!(e)),
    }
}

fn ublk_recover(opt: &UblkArgs) -> AnyRes<i32> {
    trace!("ublk recover {}", opt.number);
    Ok(0)
}

fn __ublk_del(id: i32) -> AnyRes<i32> {
    let mut ctrl = UblkCtrl::new(id, 0, 0, 0, 0, false)?;

    ctrl.stop()?;
    ctrl.del()?;

    Ok(0)
}

fn ublk_del(opt: &DelArgs) -> AnyRes<i32> {
    trace!("ublk del {} {}", opt.number, opt.all);

    if !opt.all {
        __ublk_del(opt.number)?;

        return Ok(0);
    }

    if let Ok(entries) = std::fs::read_dir(UblkCtrl::run_dir()) {
        for entry in entries {
            if let Ok(entry) = entry {
                let f = entry.path();
                if f.is_file() {
                    if let Some(file_stem) = f.file_stem() {
                        if let Some(stem) = file_stem.to_str() {
                            if let Ok(num) = stem.parse::<i32>() {
                                __ublk_del(num)?;
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(0)
}

fn __ublk_list(id: i32) {
    let mut ctrl = UblkCtrl::new(id, 0, 0, 0, 0, false).unwrap();

    ctrl.get_info().unwrap();
    ctrl.dump();
}

fn ublk_list(opt: &UblkArgs) -> AnyRes<i32> {
    if opt.number > 0 {
        __ublk_list(opt.number);
        return Ok(0);
    }

    if let Ok(entries) = std::fs::read_dir(UblkCtrl::run_dir()) {
        for entry in entries {
            if let Ok(entry) = entry {
                let f = entry.path();
                if f.is_file() {
                    if let Some(file_stem) = f.file_stem() {
                        if let Some(stem) = file_stem.to_str() {
                            if let Ok(num) = stem.parse::<i32>() {
                                __ublk_list(num);
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(0)
}

fn main() -> AnyRes<()> {
    let cli = Cli::parse();

    env_logger::builder()
        .format_target(false)
        .format_timestamp(None)
        .init();

    match &cli.command {
        Commands::Add(opt) => ublk_add(opt)?,
        Commands::Del(opt) => ublk_del(opt)?,
        Commands::List(opt) => ublk_list(opt)?,
        Commands::Recover(opt) => ublk_recover(opt)?,
    };
    Ok(())
}
