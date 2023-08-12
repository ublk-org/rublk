use anyhow::Result as AnyRes;
use clap::{Parser, Subcommand};
use libublk::ctrl::UblkCtrl;
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
    let tgt_type = opt.r#type.clone();

    match tgt_type.as_str() {
        "loop" => r#loop::ublk_add_loop(opt),
        "null" => null::ublk_add_null(opt),
        _ => panic!("wrong target type"),
    }

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
