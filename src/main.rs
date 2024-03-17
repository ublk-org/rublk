use args::{AddCommands, Commands};
use clap::Parser;
use ilog::IntLog;
use libublk::{ctrl::UblkCtrl, UblkError, UblkFlags};
use shared_memory::*;
use std::os::linux::fs::MetadataExt;
use std::os::unix::fs::FileTypeExt;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::atomic::{fence, Ordering};

pub(crate) mod target_flags {
    pub const TGT_QUIET: u64 = 0b00000001;
}

#[macro_use]
extern crate nix;

mod args;
mod r#loop;
mod null;
mod qcow2;
mod zoned;

#[derive(Parser)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

// Generate ioctl function
const BLK_IOCTL_TYPE: u8 = 0x12; // Defined in linux/fs.h
const BLKGETSIZE64_NR: u8 = 114;
const BLKSSZGET_NR: u8 = 104;
const BLKPBSZGET_NR: u8 = 123;

ioctl_read!(ioctl_blkgetsize64, BLK_IOCTL_TYPE, BLKGETSIZE64_NR, u64);
ioctl_read_bad!(
    ioctl_blksszget,
    request_code_none!(BLK_IOCTL_TYPE, BLKSSZGET_NR),
    i32
);
ioctl_read_bad!(
    ioctl_blkpbszget,
    request_code_none!(BLK_IOCTL_TYPE, BLKPBSZGET_NR),
    u32
);

pub(crate) fn ublk_file_size(f: &std::fs::File) -> anyhow::Result<(u64, u8, u8)> {
    if let Ok(meta) = f.metadata() {
        if meta.file_type().is_block_device() {
            let fd = f.as_raw_fd();
            let mut cap = 0_u64;
            let mut ssz = 0_i32;
            let mut pbsz = 0_u32;

            unsafe {
                let cap_ptr = &mut cap as *mut u64;
                let ssz_ptr = &mut ssz as *mut i32;
                let pbsz_ptr = &mut pbsz as *mut u32;

                ioctl_blkgetsize64(fd, cap_ptr).unwrap();
                ioctl_blksszget(fd, ssz_ptr).unwrap();
                ioctl_blkpbszget(fd, pbsz_ptr).unwrap();
            }

            Ok((cap, ssz.log2() as u8, pbsz.log2() as u8))
        } else if meta.file_type().is_file() {
            let m = f.metadata().unwrap();
            Ok((
                m.len(),
                m.st_blksize().log2() as u8,
                m.st_blksize().log2() as u8,
            ))
        } else {
            Err(anyhow::anyhow!("unsupported file"))
        }
    } else {
        Err(anyhow::anyhow!("no file meta got"))
    }
}

/// Wait until control device state is updated to `state`
fn ublk_state_wait_until(ctrl: &mut UblkCtrl, state: u32, timeout: u32) -> Result<i32, UblkError> {
    let mut count = 0;
    let unit = 100_u32;
    loop {
        std::thread::sleep(std::time::Duration::from_millis(unit as u64));

        ctrl.read_dev_info()?;
        if ctrl.dev_info().state == state as u16 {
            return Ok(0);
        }
        count += unit;
        if count >= timeout {
            return Err(UblkError::OtherError(-libc::ETIME));
        }
    }
}

/// Write device ID into shared memory, so that parent process can
/// know this ID info
///
/// The 1st 4 char is : 'U' 'B' 'L' 'K', then follows the 4
/// ID chars which is encoded by hex.
fn rublk_write_id_into_shm(shm_id: &String, id: u32) {
    log::info!("shm_id {} id {}", shm_id, id);
    match ShmemConf::new().os_id(shm_id).size(4096).open() {
        Ok(mut shmem) => {
            let s: &mut [u8] = unsafe { shmem.as_slice_mut() };

            let id_str = format!("{:04x}", id);
            let mut i = 4;
            for c in id_str.as_bytes() {
                s[i] = *c;
                i += 1;
            }

            // order the two WRITEs
            fence(Ordering::Release);

            s[0] = b'U';
            s[1] = b'B';
            s[2] = b'L';
            s[3] = b'K';
        }
        Err(e) => println!("write id open failed {} {}", shm_id, e),
    }
}

pub(crate) fn rublk_prep_dump_dev(shm_id: Option<String>, fg: bool, ctrl: &UblkCtrl) {
    if !fg {
        if let Some(shm) = shm_id {
            crate::rublk_write_id_into_shm(&shm, ctrl.dev_info().dev_id);
        }
    } else {
        ctrl.dump();
    }
}

fn rublk_read_id_from_shm(shm_id: &String) -> Result<i32, UblkError> {
    if let Ok(shmem) = ShmemConf::new().os_id(shm_id).size(4096).open() {
        let s: &[u8] = unsafe { shmem.as_slice() };

        if s[0] != b'U' || s[1] != b'B' || s[2] != b'L' || s[3] != b'K' {
            return Err(UblkError::OtherError(-libc::EAGAIN));
        }

        // order the two READs
        fence(Ordering::Acquire);

        let ss = String::from_utf8(s[4..8].to_vec()).unwrap();
        if let Ok(i) = i32::from_str_radix(&ss, 16) {
            Ok(i)
        } else {
            Err(UblkError::OtherError(-libc::EINVAL))
        }
    } else {
        Err(UblkError::OtherError(-libc::EAGAIN))
    }
}

fn rublk_wait_and_dump(shm_id: &String) -> Result<i32, UblkError> {
    let mut count = 0;
    loop {
        if count >= 500 {
            eprintln!("create ublk device failed");
            return Err(UblkError::OtherError(-libc::EINVAL));
        }
        match rublk_read_id_from_shm(shm_id) {
            Ok(id) => {
                let ctrl = UblkCtrl::new_simple(id)?;

                if (ctrl.dev_info().ublksrv_flags & target_flags::TGT_QUIET) == 0 {
                    ctrl.dump();
                }

                return Ok(0);
            }
            Err(UblkError::OtherError(code)) => {
                if code == -libc::EAGAIN {
                    count += 1;
                    std::thread::sleep(std::time::Duration::from_millis(10));
                } else {
                    return Err(UblkError::OtherError(code));
                }
            }
            _ => return Err(UblkError::OtherError(-libc::EINVAL)),
        };
    }
}

fn ublk_parse_add_args(opt: &args::AddCommands) -> (&'static str, &args::GenAddArgs) {
    match opt {
        AddCommands::Loop(_opt) => ("loop", &_opt.gen_arg),
        AddCommands::Null(_opt) => ("null", &_opt.gen_arg),
        AddCommands::Zoned(_opt) => ("zoned", &_opt.gen_arg),
        AddCommands::Qcow2(_opt) => ("qcow2", &_opt.gen_arg),
    }
}

fn ublk_add_worker(opt: args::AddCommands) -> Result<i32, UblkError> {
    let (tgt_type, gen_arg) = ublk_parse_add_args(&opt);
    let ctrl = gen_arg.new_ublk_ctrl(tgt_type, UblkFlags::UBLK_DEV_F_ADD_DEV)?;

    match opt {
        AddCommands::Loop(opt) => r#loop::ublk_add_loop(ctrl, Some(opt)),
        AddCommands::Null(opt) => null::ublk_add_null(ctrl, Some(opt)),
        AddCommands::Zoned(opt) => zoned::ublk_add_zoned(ctrl, Some(opt)),
        AddCommands::Qcow2(opt) => qcow2::ublk_add_qcow2(ctrl, Some(opt)),
    }
}

fn ublk_add(opt: args::AddCommands) -> Result<i32, UblkError> {
    let (_, gen_arg) = ublk_parse_add_args(&opt);
    gen_arg.save_start_dir();

    if gen_arg.foreground {
        ublk_add_worker(opt)
    } else {
        let daemonize = daemonize::Daemonize::new()
            .stdout(daemonize::Stdio::keep())
            .stderr(daemonize::Stdio::keep());

        gen_arg.generate_shm_id();

        let shm_id = gen_arg.get_shm_id();
        match ShmemConf::new().os_id(&shm_id).size(4096).create() {
            Ok(_shm) => match daemonize.execute() {
                daemonize::Outcome::Child(Ok(_)) => ublk_add_worker(opt),
                daemonize::Outcome::Parent(Ok(_)) => rublk_wait_and_dump(&shm_id),
                _ => Err(UblkError::OtherError(-libc::EINVAL)),
            },
            Err(_) => Err(UblkError::OtherError(-libc::EINVAL)),
        }
    }
}

fn ublk_recover_work(opt: args::UblkArgs) -> Result<i32, UblkError> {
    if opt.number < 0 {
        return Err(UblkError::OtherError(-libc::EINVAL));
    }

    let ctrl = UblkCtrl::new_simple(opt.number)?;

    if (ctrl.dev_info().flags & (libublk::sys::UBLK_F_USER_RECOVERY as u64)) == 0 {
        return Err(UblkError::OtherError(-libc::EOPNOTSUPP));
    }

    if ctrl.dev_info().state != libublk::sys::UBLK_S_DEV_QUIESCED as u16 {
        return Err(UblkError::OtherError(-libc::EBUSY));
    }

    ctrl.start_user_recover()?;

    let tgt_type = ctrl.get_target_type_from_json().unwrap();
    let ctrl = libublk::ctrl::UblkCtrlBuilder::default()
        .name(&tgt_type.clone())
        .depth(ctrl.dev_info().queue_depth)
        .nr_queues(ctrl.dev_info().nr_hw_queues)
        .id(ctrl.dev_info().dev_id as i32)
        .ctrl_flags(libublk::sys::UBLK_F_USER_RECOVERY.into())
        .dev_flags(UblkFlags::UBLK_DEV_F_RECOVER_DEV)
        .build()
        .unwrap();

    match tgt_type.as_str() {
        "loop" => r#loop::ublk_add_loop(ctrl, None),
        "null" => null::ublk_add_null(ctrl, None),
        "zoned" => zoned::ublk_add_zoned(ctrl, None),
        &_ => todo!(),
    }
}

fn ublk_recover(opt: args::UblkArgs) -> Result<i32, UblkError> {
    let daemonize = daemonize::Daemonize::new()
        .stdout(daemonize::Stdio::keep())
        .stderr(daemonize::Stdio::keep());

    let id = opt.number;
    if id < 0 {
        return Err(UblkError::OtherError(-libc::EINVAL));
    }

    match daemonize.execute() {
        daemonize::Outcome::Child(Ok(_)) => ublk_recover_work(opt),
        daemonize::Outcome::Parent(Ok(_)) => {
            let mut ctrl = UblkCtrl::new_simple(id)?;
            ublk_state_wait_until(&mut ctrl, libublk::sys::UBLK_S_DEV_LIVE, 5000)?;

            if (ctrl.dev_info().ublksrv_flags & target_flags::TGT_QUIET) == 0 {
                ctrl.dump();
            }
            Ok(0)
        }
        _ => Err(UblkError::OtherError(-libc::EINVAL)),
    }
}

const NR_FEATURES: usize = 9;
const FEATURES_TABLE: [&str; NR_FEATURES] = [
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

#[allow(clippy::needless_range_loop)]
fn ublk_features(_opt: args::UblkFeaturesArgs) -> Result<i32, UblkError> {
    match UblkCtrl::get_features() {
        Some(f) => {
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
        None => eprintln!("not support GET_FEATURES, require linux v6.5"),
    }
    Ok(0)
}

fn __ublk_del(id: i32) -> Result<i32, UblkError> {
    let ctrl = UblkCtrl::new_simple(id)?;

    let _ = ctrl.kill_dev();
    let _ = ctrl.del_dev();

    let run_path = ctrl.run_path();
    let json_path = std::path::Path::new(&run_path);
    assert!(!json_path.exists());

    Ok(0)
}

fn ublk_del(opt: args::DelArgs) -> Result<i32, UblkError> {
    log::trace!("ublk del {} {}", opt.number, opt.all);

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

fn __ublk_list(id: i32) -> Result<i32, UblkError> {
    match UblkCtrl::new_simple(id) {
        Ok(ctrl) => {
            if ctrl.read_dev_info().is_ok() {
                ctrl.dump();
            }
            Ok(0)
        }
        _ => Err(UblkError::OtherError(-libc::ENODEV)),
    }
}

fn ublk_list(opt: args::UblkArgs) -> Result<i32, UblkError> {
    if opt.number > 0 {
        let _ = __ublk_list(opt.number);
        return Ok(0);
    }

    if let Ok(entries) = std::fs::read_dir(UblkCtrl::run_dir()) {
        for entry in entries.flatten() {
            let f = entry.path();
            if f.is_file() {
                if let Some(file_stem) = f.file_stem() {
                    if let Some(stem) = file_stem.to_str() {
                        if let Ok(num) = stem.parse::<i32>() {
                            let _ = __ublk_list(num);
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

    if !Path::new("/dev/ublk-control").exists() {
        eprintln!("Please run `modprobe ublk_drv` first");
        std::process::exit(1);
    }

    match cli.command {
        Commands::Add(opt) => ublk_add(opt).unwrap(),
        Commands::Del(opt) => ublk_del(opt).unwrap(),
        Commands::List(opt) => ublk_list(opt).unwrap(),
        Commands::Recover(opt) => ublk_recover(opt).unwrap(),
        Commands::Features(opt) => ublk_features(opt).unwrap(),
    };
}
