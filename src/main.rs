use args::{AddCommands, Commands};
use clap::Parser;
use ilog::IntLog;
use libublk::{ctrl::UblkCtrl, UblkFlags};
use std::os::linux::fs::MetadataExt;
use std::os::unix::fs::FileTypeExt;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::Arc;

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

pub(crate) struct DevIdComm {
    efd: i32,
    dump: bool,
}

impl DevIdComm {
    pub fn new(dump: bool) -> anyhow::Result<DevIdComm> {
        let fd = nix::sys::eventfd::eventfd(0, nix::sys::eventfd::EfdFlags::empty())?;

        Ok(DevIdComm { efd: fd, dump })
    }

    fn write_failure(&self) -> anyhow::Result<i32> {
        let id = i64::MAX;
        let bytes = id.to_le_bytes();

        match nix::unistd::write(self.efd, &bytes) {
            Ok(_) => Ok(0),
            _ => Err(anyhow::anyhow!("fail to write failure to eventfd")),
        }
    }
    fn write_dev_id(&self, dev_id: u32) -> anyhow::Result<i32> {
        // Can't write 0 to eventfd file, otherwise the read() side may
        // not be waken up
        let id = (dev_id + 1) as i64;
        let bytes = id.to_le_bytes();

        match nix::unistd::write(self.efd, &bytes) {
            Ok(_) => Ok(0),
            _ => Err(anyhow::anyhow!("fail to write dev_id to eventfd")),
        }
    }

    fn read_dev_id(&self) -> anyhow::Result<i32> {
        let mut buffer = [0; 8];

        let bytes_read = nix::unistd::read(self.efd, &mut buffer)?;
        if bytes_read == 0 {
            return Err(anyhow::anyhow!("fail to read dev_id from eventfd"));
        }
        let ret = i64::from_le_bytes(buffer);
        if ret == i64::MAX {
            return Err(anyhow::anyhow!("Fail to start ublk daemon"));
        }

        return Ok((ret - 1) as i32);
    }

    pub(crate) fn send_dev_id(&self, id: u32) -> anyhow::Result<()> {
        if self.dump {
            UblkCtrl::new_simple(id as i32).unwrap().dump();
        } else {
            self.write_dev_id(id).expect("Fail to write efd");
        }
        Ok(())
    }

    pub(crate) fn recieve_dev_id(&self) -> anyhow::Result<i32> {
        let id = self.read_dev_id()?;
        Ok(id)
    }
}

fn ublk_dump_dev(comm: &Arc<DevIdComm>) -> anyhow::Result<i32> {
    match comm.recieve_dev_id() {
        Ok(id) => {
            UblkCtrl::new_simple(id).unwrap().dump();
            Ok(0)
        }
        _ => Err(anyhow::anyhow!("not recieved device id")),
    }
}

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
fn ublk_state_wait_until(ctrl: &mut UblkCtrl, state: u32, timeout: u32) -> anyhow::Result<i32> {
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
            return Err(anyhow::anyhow!("timeout error"));
        }
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

fn ublk_add_worker(opt: args::AddCommands, comm: &Arc<DevIdComm>) -> anyhow::Result<i32> {
    let (tgt_type, gen_arg) = ublk_parse_add_args(&opt);
    let ctrl = gen_arg.new_ublk_ctrl(tgt_type, UblkFlags::UBLK_DEV_F_ADD_DEV)?;

    match opt {
        AddCommands::Loop(opt) => r#loop::ublk_add_loop(ctrl, Some(opt), comm),
        AddCommands::Null(opt) => null::ublk_add_null(ctrl, Some(opt), comm),
        AddCommands::Zoned(opt) => zoned::ublk_add_zoned(ctrl, Some(opt), comm),
        AddCommands::Qcow2(opt) => qcow2::ublk_add_qcow2(ctrl, Some(opt), comm),
    }
}

fn ublk_add(opt: args::AddCommands) -> anyhow::Result<i32> {
    let (_, gen_arg) = ublk_parse_add_args(&opt);
    let comm = Arc::new(DevIdComm::new(gen_arg.foreground).expect("Create eventfd failed"));
    gen_arg.save_start_dir();

    if gen_arg.foreground {
        ublk_add_worker(opt, &comm)
    } else {
        let daemonize = daemonize::Daemonize::new()
            .stdout(daemonize::Stdio::keep())
            .stderr(daemonize::Stdio::keep());

        match daemonize.execute() {
            daemonize::Outcome::Child(Ok(_)) => match ublk_add_worker(opt, &comm) {
                Ok(res) => Ok(res),
                Err(r) => {
                    comm.write_failure().expect("fail to send failure");
                    Err(r)
                }
            },
            daemonize::Outcome::Parent(Ok(_)) => ublk_dump_dev(&comm),
            _ => Err(anyhow::anyhow!("daemonize execute failure")),
        }
    }
}

fn ublk_recover_work(opt: args::UblkArgs) -> anyhow::Result<i32> {
    if opt.number < 0 {
        return Err(anyhow::anyhow!("invalid device number"));
    }

    let ctrl = UblkCtrl::new_simple(opt.number)?;

    if (ctrl.dev_info().flags & (libublk::sys::UBLK_F_USER_RECOVERY as u64)) == 0 {
        return Err(anyhow::anyhow!("not set user recovery flag"));
    }

    if ctrl.dev_info().state != libublk::sys::UBLK_S_DEV_QUIESCED as u16 {
        return Err(anyhow::anyhow!("device isn't quiesced"));
    }

    let comm = Arc::new(DevIdComm::new(false).expect("Create eventfd failed"));
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
        "loop" => r#loop::ublk_add_loop(ctrl, None, &comm),
        "null" => null::ublk_add_null(ctrl, None, &comm),
        "zoned" => zoned::ublk_add_zoned(ctrl, None, &comm),
        "qcow2" => qcow2::ublk_add_qcow2(ctrl, None, &comm),
        &_ => todo!(),
    }
}

fn ublk_recover(opt: args::UblkArgs) -> anyhow::Result<i32> {
    let daemonize = daemonize::Daemonize::new()
        .stdout(daemonize::Stdio::keep())
        .stderr(daemonize::Stdio::keep());

    let id = opt.number;
    if id < 0 {
        return Err(anyhow::anyhow!("invalid device id"));
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
        _ => Err(anyhow::anyhow!("daemonize execute failed")),
    }
}

const FEATURES_TABLE: &[&str] = &[
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
fn ublk_features(_opt: args::UblkFeaturesArgs) -> anyhow::Result<i32> {
    match UblkCtrl::get_features() {
        Some(f) => {
            println!("\t{:<22} {:#12x}", "UBLK FEATURES", f);
            for i in 0..64 {
                if ((1_u64 << i) & f) == 0 {
                    continue;
                }

                let feat = if i < FEATURES_TABLE.len() {
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

fn __ublk_del(id: i32, async_del: bool) -> anyhow::Result<i32> {
    let ctrl = UblkCtrl::new_simple(id)?;

    ctrl.kill_dev()?;
    match async_del {
        false => ctrl.del_dev()?,
        true => ctrl.del_dev_async()?,
    };

    let run_path = ctrl.run_path();
    let json_path = std::path::Path::new(&run_path);
    assert!(!json_path.exists());

    Ok(0)
}

fn ublk_del(opt: args::DelArgs) -> anyhow::Result<i32> {
    log::trace!("ublk del {} {}", opt.number, opt.all);

    if !opt.all {
        __ublk_del(opt.number, opt.r#async)?;

        return Ok(0);
    }

    if let Ok(entries) = std::fs::read_dir(UblkCtrl::run_dir()) {
        for entry in entries.flatten() {
            let f = entry.path();
            if f.is_file() {
                if let Some(file_stem) = f.file_stem() {
                    if let Some(stem) = file_stem.to_str() {
                        if let Ok(num) = stem.parse::<i32>() {
                            __ublk_del(num, opt.r#async)?;
                        }
                    }
                }
            }
        }
    }

    Ok(0)
}

fn __ublk_list(id: i32) -> anyhow::Result<i32> {
    match UblkCtrl::new_simple(id) {
        Ok(ctrl) => {
            if ctrl.read_dev_info().is_ok() {
                ctrl.dump();
            }
            Ok(0)
        }
        _ => Err(anyhow::anyhow!("nodev failure")),
    }
}

fn ublk_list(opt: args::UblkArgs) -> anyhow::Result<i32> {
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
