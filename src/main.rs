use args::{AddCommands, Commands};
use clap::Parser;
use libublk::{ctrl::UblkCtrl, UblkError, UBLK_DEV_F_ADD_DEV, UBLK_DEV_F_RECOVER_DEV};
use log::trace;

#[macro_use]
extern crate nix;

mod args;
mod r#loop;
mod null;
mod zoned;

#[derive(Parser)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

fn ublk_parse_add_args(opt: &args::AddCommands) -> (&'static str, &args::GenAddArgs) {
    match opt {
        AddCommands::Loop(_opt) => ("loop", &_opt.gen_arg),
        AddCommands::Null(_opt) => ("null", &_opt.gen_arg),
        AddCommands::Zoned(_opt) => ("zoned", &_opt.gen_arg),
    }
}

fn ublk_add(opt: args::AddCommands) -> Result<i32, UblkError> {
    let daemonize = daemonize::Daemonize::new()
        .stdout(daemonize::Stdio::keep())
        .stderr(daemonize::Stdio::keep());

    let (tgt_type, gen_arg) = ublk_parse_add_args(&opt);
    let sess = gen_arg.new_ublk_sesson(tgt_type, UBLK_DEV_F_ADD_DEV);

    match daemonize.start() {
        Ok(_) => match opt {
            AddCommands::Loop(opt) => r#loop::ublk_add_loop(sess, -1, Some(opt)),
            AddCommands::Null(opt) => null::ublk_add_null(sess, -1, Some(opt)),
            AddCommands::Zoned(opt) => zoned::ublk_add_zoned(sess, -1, Some(opt)),
        },
        Err(_) => Err(UblkError::OtherError(-libc::EINVAL)),
    }
}

fn ublk_recover_work(opt: args::UblkArgs) -> Result<i32, UblkError> {
    if opt.number < 0 {
        return Err(UblkError::OtherError(-libc::EINVAL));
    }

    let mut ctrl = UblkCtrl::new_simple(opt.number, 0)?;

    if (ctrl.dev_info.flags & (libublk::sys::UBLK_F_USER_RECOVERY as u64)) == 0 {
        return Err(UblkError::OtherError(-libc::EOPNOTSUPP));
    }

    if ctrl.dev_info.state != libublk::sys::UBLK_S_DEV_QUIESCED as u16 {
        return Err(UblkError::OtherError(-libc::EBUSY));
    }

    ctrl.start_user_recover()?;

    let tgt_type = ctrl.get_target_type_from_json().unwrap();
    let sess = libublk::UblkSessionBuilder::default()
        .name(tgt_type.clone())
        .depth(ctrl.dev_info.queue_depth)
        .nr_queues(ctrl.dev_info.nr_hw_queues)
        .id(ctrl.dev_info.dev_id as i32)
        .ctrl_flags(libublk::sys::UBLK_F_USER_RECOVERY)
        .dev_flags(UBLK_DEV_F_RECOVER_DEV)
        .build()
        .unwrap();

    match tgt_type.as_str() {
        "loop" => r#loop::ublk_add_loop(sess, opt.number, None),
        "null" => null::ublk_add_null(sess, opt.number, None),
        "zoned" => zoned::ublk_add_zoned(sess, opt.number, None),
        &_ => todo!(),
    }
}

fn ublk_recover(opt: args::UblkArgs) -> Result<i32, UblkError> {
    let daemonize = daemonize::Daemonize::new()
        .stdout(daemonize::Stdio::keep())
        .stderr(daemonize::Stdio::keep());

    match daemonize.start() {
        Ok(_) => ublk_recover_work(opt),
        Err(_) => Err(UblkError::OtherError(-libc::EINVAL)),
    }
}

const NR_FEATURES: usize = 9;
const FEATURES_TABLE: [&'static str; NR_FEATURES] = [
    "ZERO_COPY",
    "COMP_IN_TASK",
    "NEED_GET_DATA",
    "USER_RECOVERY",
    "USER_RECOVERY_REISSUE",
    "UNPRIVILEGED_DEV",
    "CMD_IOCTL_ENCODE",
    "USER_COPY",
    "ZONED",
];

fn ublk_features(_opt: args::UblkFeaturesArgs) -> Result<i32, UblkError> {
    let mut ctrl = UblkCtrl::new_simple(-1, 0)?;

    match ctrl.get_features() {
        Ok(f) => {
            println!("\t{:<22} {:#12x}", "UBLK FEATURES", f);
            for i in 0..64 {
                if ((1_u64 << i) & f) == 0 {
                    continue;
                }

                let feat = if i < NR_FEATURES {
                    FEATURES_TABLE[i]
                } else {
                    "unknown"
                };
                println!("\t{:<22} {:#12x}", feat, 1_u64 << i);
            }
        }
        Err(_) => eprintln!("not support GET_FEATURES, require linux v6.5"),
    }
    Ok(0)
}

fn __ublk_del(id: i32) -> Result<i32, UblkError> {
    let mut ctrl = UblkCtrl::new_simple(id, 0)?;

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
    let mut ctrl = UblkCtrl::new_simple(id, 0).unwrap();

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
        Commands::Features(opt) => ublk_features(opt).unwrap(),
    };
}
