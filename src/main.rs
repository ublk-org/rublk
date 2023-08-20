use args::{AddCommands, Commands};
use clap::Parser;
use libublk::{ctrl::UblkCtrl, UblkError};
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

fn ublk_add(opt: args::AddCommands) -> Result<i32, UblkError> {
    let daemonize = daemonize::Daemonize::new()
        .stdout(daemonize::Stdio::keep())
        .stderr(daemonize::Stdio::keep());

    match daemonize.start() {
        Ok(_) => match opt {
            AddCommands::Loop(opt) => r#loop::ublk_add_loop(opt),
            AddCommands::Null(opt) => null::ublk_add_null(opt),
        },
        Err(_) => Err(UblkError::OtherError(-libc::EINVAL)),
    }
}

fn ublk_recover(opt: args::UblkArgs) -> Result<i32, UblkError> {
    trace!("ublk recover {}", opt.number);
    Ok(0)
}

fn __ublk_del(id: i32) -> Result<i32, UblkError> {
    let mut ctrl = UblkCtrl::new(id, 0, 0, 0, 0, false)?;

    ctrl.stop()?;
    ctrl.del_dev()?;

    Ok(0)
}

fn ublk_del(opt: args::DelArgs) -> Result<i32, UblkError> {
    trace!("ublk del {} {}", opt.number, opt.all);

    if !opt.all {
        __ublk_del(opt.number)?;

        return Ok(0);
    }

    if let Ok(entries) = std::fs::read_dir(UblkCtrl::run_dir()) {
        for entry in entries.flatten() {
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

    Ok(0)
}

fn __ublk_list(id: i32) {
    let mut ctrl = UblkCtrl::new(id, 0, 0, 0, 0, false).unwrap();

    if ctrl.get_info().is_ok() {
        ctrl.dump();
    }
}

fn ublk_list(opt: args::UblkArgs) -> Result<i32, UblkError> {
    if opt.number > 0 {
        __ublk_list(opt.number);
        return Ok(0);
    }

    if let Ok(entries) = std::fs::read_dir(UblkCtrl::run_dir()) {
        for entry in entries.flatten() {
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
    Ok(0)
}

fn main() {
    let cli = Cli::parse();

    env_logger::builder()
        .format_target(false)
        .format_timestamp(None)
        .init();

    match cli.command {
        Commands::Add(opt) => ublk_add(opt).unwrap(),
        Commands::Del(opt) => ublk_del(opt).unwrap(),
        Commands::List(opt) => ublk_list(opt).unwrap(),
        Commands::Recover(opt) => ublk_recover(opt).unwrap(),
    };
}
