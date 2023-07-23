use anyhow::Result as AnyRes;
use clap::{Parser, Subcommand};
use libublk::{ctrl::UblkCtrl, io::UblkQueueImpl};
use log::trace;

#[macro_use]
extern crate nix;

mod args;
mod r#loop;
mod null;

#[derive(Parser)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Adds ublk target
    Add(args::AddArgs),
    /// Deletes ublk target
    Del(args::DelArgs),
    /// Lists ublk targets
    List(args::UblkArgs),
    /// Recover ublk targets
    Recover(args::UblkArgs),
}

fn ublk_daemon_work(opt: args::AddArgs) -> AnyRes<i32> {
    let file = match opt.file {
        Some(p) => p.display().to_string(),
        _ => "".to_string(),
    };
    let id = opt.number;
    let queues = opt.queue;
    let depth = opt.depth;
    let dio = opt.direct_io;
    let tgt_type2 = opt.r#type.clone();
    let tgt_type3 = opt.r#type.clone();

    libublk::ublk_tgt_worker(
        id,
        queues,
        depth,
        512_u32 * 1024,
        0,
        true,
        move |_| match tgt_type2.as_str() {
            "loop" => Box::new(r#loop::LoopTgt {
                back_file: std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(file.clone())
                    .unwrap(),
                direct_io: if dio { 1 } else { 0 },
                back_file_path: file.clone(),
            }),
            _ => Box::new(null::NullTgt {}),
        },
        move |_| match tgt_type3.clone().as_str() {
            "loop" => Box::new(r#loop::LoopQueue {}) as Box<dyn UblkQueueImpl>,
            _ => Box::new(null::NullQueue {}) as Box<dyn UblkQueueImpl>,
        },
        |dev_id| {
            let mut ctrl = UblkCtrl::new(dev_id, 0, 0, 0, 0, false).unwrap();
            ctrl.dump();
        },
    )
    .unwrap()
    .join()
    .unwrap();

    Ok(0)
}

fn ublk_add(opt: args::AddArgs) -> AnyRes<i32> {
    let daemonize = daemonize::Daemonize::new()
        .stdout(daemonize::Stdio::keep())
        .stderr(daemonize::Stdio::keep());

    match daemonize.start() {
        Ok(_) => ublk_daemon_work(opt),
        Err(e) => Err(anyhow::anyhow!(e)),
    }
}

fn ublk_recover(opt: args::UblkArgs) -> AnyRes<i32> {
    trace!("ublk recover {}", opt.number);
    Ok(0)
}

fn __ublk_del(id: i32) -> AnyRes<i32> {
    let mut ctrl = UblkCtrl::new(id, 0, 0, 0, 0, false)?;

    ctrl.stop()?;
    ctrl.del_dev()?;

    Ok(0)
}

fn ublk_del(opt: args::DelArgs) -> AnyRes<i32> {
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

    if let Ok(_) = ctrl.get_info() {
        ctrl.dump();
    }
}

fn ublk_list(opt: args::UblkArgs) -> AnyRes<i32> {
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

    match cli.command {
        Commands::Add(opt) => ublk_add(opt)?,
        Commands::Del(opt) => ublk_del(opt)?,
        Commands::List(opt) => ublk_list(opt)?,
        Commands::Recover(opt) => ublk_recover(opt)?,
    };
    Ok(())
}
