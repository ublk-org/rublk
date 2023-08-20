use anyhow::Result as AnyRes;
use ilog::IntLog;
use io_uring::{opcode, squeue, types};
use libublk::ctrl::UblkCtrl;
use libublk::io::{UblkDev, UblkIOCtx, UblkQueueCtx};
use libublk::{UblkError, UblkSession};
use log::trace;
use serde::Serialize;
use std::os::unix::fs::FileTypeExt;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;

#[derive(clap::Args, Debug)]
pub struct LoopArgs {
    #[command(flatten)]
    pub gen_arg: super::args::GenAddArgs,

    ///backing file of ublk target(loop)
    #[clap(long, short = 'f')]
    pub file: Option<PathBuf>,

    ///if direct io is applied for backing file of ublk target(loop)
    #[clap(long, default_value_t = true)]
    pub direct_io: bool,
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

#[derive(Debug, Serialize)]
struct LoJson {
    back_file_path: String,
    direct_io: i32,
}

pub struct LoopTgt {
    pub back_file_path: String,
    pub back_file: std::fs::File,
    pub direct_io: i32,
}

fn lo_file_size(f: &std::fs::File) -> AnyRes<(u64, u8, u8)> {
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
            Ok((f.metadata().unwrap().len(), 9, 12))
        } else {
            Err(anyhow::anyhow!("unsupported file"))
        }
    } else {
        Err(anyhow::anyhow!("no file meta got"))
    }
}

fn loop_queue_tgt_io(
    io: &mut UblkIOCtx,
    tag: u32,
    iod: &libublk::sys::ublksrv_io_desc,
) -> Result<i32, UblkError> {
    let off = (iod.start_sector << 9) as u64;
    let bytes = (iod.nr_sectors << 9) as u32;
    let op = iod.op_flags & 0xff;
    let data = UblkIOCtx::build_user_data(tag as u16, op, 0, true);
    let buf_addr = io.io_buf_addr();
    let r = io.get_ring();

    if op == libublk::sys::UBLK_IO_OP_WRITE_ZEROES || op == libublk::sys::UBLK_IO_OP_DISCARD {
        return Err(UblkError::OtherError(-libc::EINVAL));
    }

    match op {
        libublk::sys::UBLK_IO_OP_FLUSH => {
            let sqe = &opcode::SyncFileRange::new(types::Fixed(1), bytes)
                .offset(off)
                .build()
                .flags(squeue::Flags::FIXED_FILE)
                .user_data(data);
            unsafe {
                r.submission().push(sqe).expect("submission fail");
            }
        }
        libublk::sys::UBLK_IO_OP_READ => {
            let sqe = &opcode::Read::new(types::Fixed(1), buf_addr, bytes)
                .offset(off)
                .build()
                .flags(squeue::Flags::FIXED_FILE)
                .user_data(data);
            unsafe {
                r.submission().push(sqe).expect("submission fail");
            }
        }
        libublk::sys::UBLK_IO_OP_WRITE => {
            let sqe = &opcode::Write::new(types::Fixed(1), buf_addr, bytes)
                .offset(off)
                .build()
                .flags(squeue::Flags::FIXED_FILE)
                .user_data(data);
            unsafe {
                r.submission().push(sqe).expect("submission fail");
            }
        }
        _ => return Err(UblkError::OtherError(-libc::EINVAL)),
    }

    Ok(1)
}

fn _lo_handle_io(ctx: &UblkQueueCtx, i: &mut UblkIOCtx) -> Result<i32, UblkError> {
    let tag = i.get_tag();

    // our IO on backing file is done
    if i.is_tgt_io() {
        let user_data = i.user_data();
        let res = i.result();
        let cqe_tag = UblkIOCtx::user_data_to_tag(user_data);

        assert!(cqe_tag == tag);

        if res != -(libc::EAGAIN) {
            i.complete_io(res);

            return Ok(0);
        }
    }

    // either start to handle or retry
    let _iod = ctx.get_iod(tag);
    let iod = unsafe { &*_iod };

    loop_queue_tgt_io(i, tag, iod)
}

fn lo_init_tgt(dev: &mut UblkDev, lo: &LoopTgt) -> Result<serde_json::Value, UblkError> {
    trace!("loop: init_tgt {}", dev.dev_info.dev_id);
    let info = dev.dev_info;

    if lo.direct_io != 0 {
        unsafe {
            libc::fcntl(lo.back_file.as_raw_fd(), libc::F_SETFL, libc::O_DIRECT);
        }
    }

    let tgt = &mut dev.tgt;
    let nr_fds = tgt.nr_fds;
    tgt.fds[nr_fds as usize] = lo.back_file.as_raw_fd();
    tgt.nr_fds = nr_fds + 1;

    let sz = lo_file_size(&lo.back_file).unwrap();

    tgt.dev_size = sz.0;
    //todo: figure out correct block size
    tgt.params = libublk::sys::ublk_params {
        types: libublk::sys::UBLK_PARAM_TYPE_BASIC,
        basic: libublk::sys::ublk_param_basic {
            logical_bs_shift: sz.1,
            physical_bs_shift: sz.2,
            io_opt_shift: 12,
            io_min_shift: 9,
            max_sectors: info.max_io_buf_bytes >> 9,
            dev_sectors: tgt.dev_size >> 9,
            ..Default::default()
        },
        ..Default::default()
    };

    Ok(
        serde_json::json!({"loop": LoJson { back_file_path: lo.back_file_path.clone(), direct_io: 1 } }),
    )
}

pub fn ublk_add_loop(sess: UblkSession, opt: LoopArgs) -> Result<i32, UblkError> {
    let file = match opt.file {
        Some(p) => p.display().to_string(),
        _ => "".to_string(),
    };
    let lo = LoopTgt {
        back_file: std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.clone())
            .unwrap(),
        direct_io: i32::from(opt.direct_io),
        back_file_path: file,
    };

    let tgt_init = |dev: &mut UblkDev| lo_init_tgt(dev, &lo);
    let wh = {
        let (mut ctrl, dev) = sess.create_devices(tgt_init).unwrap();
        let lo_handle_io = move |ctx: &UblkQueueCtx,
                                 io: &mut UblkIOCtx|
              -> Result<i32, UblkError> { _lo_handle_io(ctx, io) };

        sess.run(&mut ctrl, &dev, lo_handle_io, |dev_id| {
            let mut d_ctrl = UblkCtrl::new(dev_id, 0, 0, 0, 0, false).unwrap();
            d_ctrl.dump();
        })
        .unwrap()
    };
    wh.join().unwrap();
    Ok(0)
}
