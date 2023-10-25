use anyhow::Result as AnyRes;
use ilog::IntLog;
use io_uring::{opcode, squeue, types};
use libublk::ctrl::UblkCtrl;
use libublk::io::{UblkDev, UblkIOCtx, UblkQueue};
use libublk::{exe::Executor, exe::UringOpFuture, UblkError, UblkSession};
use log::trace;
use serde::{Deserialize, Serialize};
use std::os::unix::fs::FileTypeExt;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::rc::Rc;

#[derive(clap::Args, Debug)]
pub struct LoopArgs {
    #[command(flatten)]
    pub gen_arg: super::args::GenAddArgs,

    /// backing file of ublk target
    #[clap(long, short = 'f')]
    pub file: PathBuf,

    /// direct io is applied for backing file of ublk target
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

#[derive(Debug, Serialize, Deserialize)]
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

#[inline]
fn __lo_prep_submit_io_cmd(iod: &libublk::sys::ublksrv_io_desc) -> i32 {
    let op = iod.op_flags & 0xff;

    match op {
        libublk::sys::UBLK_IO_OP_FLUSH
        | libublk::sys::UBLK_IO_OP_READ
        | libublk::sys::UBLK_IO_OP_WRITE => return 0,
        _ => return -libc::EINVAL,
    };
}

#[inline]
fn __lo_submit_io_cmd(q: &UblkQueue<'_>, tag: u16, iod: &libublk::sys::ublksrv_io_desc, data: u64) {
    let op = iod.op_flags & 0xff;
    // either start to handle or retry
    let off = (iod.start_sector << 9) as u64;
    let bytes = (iod.nr_sectors << 9) as u32;
    let buf_addr = q.get_io_buf_addr(tag);

    match op {
        libublk::sys::UBLK_IO_OP_FLUSH => {
            let sqe = &opcode::SyncFileRange::new(types::Fixed(1), bytes)
                .offset(off)
                .build()
                .flags(squeue::Flags::FIXED_FILE)
                .user_data(data);
            unsafe {
                q.q_ring
                    .borrow_mut()
                    .submission()
                    .push(sqe)
                    .expect("submission fail");
            }
        }
        libublk::sys::UBLK_IO_OP_READ => {
            let sqe = &opcode::Read::new(types::Fixed(1), buf_addr, bytes)
                .offset(off)
                .build()
                .flags(squeue::Flags::FIXED_FILE)
                .user_data(data);
            unsafe {
                q.q_ring
                    .borrow_mut()
                    .submission()
                    .push(sqe)
                    .expect("submission fail");
            }
        }
        libublk::sys::UBLK_IO_OP_WRITE => {
            let sqe = &opcode::Write::new(types::Fixed(1), buf_addr, bytes)
                .offset(off)
                .build()
                .flags(squeue::Flags::FIXED_FILE)
                .user_data(data);
            unsafe {
                q.q_ring
                    .borrow_mut()
                    .submission()
                    .push(sqe)
                    .expect("submission fail");
            }
        }
        _ => {}
    };
}

async fn lo_handle_io_cmd_async(q: &UblkQueue<'_>, tag: u16) -> i32 {
    let iod = q.get_iod(tag);
    let op = iod.op_flags & 0xff;
    let user_data = UblkIOCtx::build_user_data_async(tag as u16, op, 0);
    let res = __lo_prep_submit_io_cmd(iod);
    if res < 0 {
        return res;
    }

    for _ in 0..4 {
        __lo_submit_io_cmd(q, tag, iod, user_data);
        let res = UringOpFuture { user_data }.await;
        if res != -(libc::EAGAIN) {
            return res;
        }
    }

    return -libc::EAGAIN;
}

fn lo_init_tgt(dev: &mut UblkDev, lo: &LoopTgt) -> Result<i32, UblkError> {
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

    let val = serde_json::json!({"loop": LoJson { back_file_path: lo.back_file_path.clone(), direct_io: 1 } });
    dev.set_target_json(val);

    Ok(0)
}

fn to_absolute_path(p: PathBuf, parent: Option<PathBuf>) -> PathBuf {
    if p.is_absolute() {
        p
    } else {
        match parent {
            None => p,
            Some(n) => n.join(p),
        }
    }
}

pub fn ublk_add_loop(
    sess: UblkSession,
    id: i32,
    opt: Option<LoopArgs>,
    parent: Option<PathBuf>,
) -> Result<i32, UblkError> {
    let (file, dio) = match opt {
        Some(o) => (to_absolute_path(o.file, parent), o.direct_io),
        None => {
            let ctrl = UblkCtrl::new_simple(id, 0)?;
            match ctrl.get_target_data_from_json() {
                Some(val) => {
                    let lo = &val["loop"];
                    let tgt_data: Result<LoJson, _> = serde_json::from_value(lo.clone());

                    match tgt_data {
                        Ok(t) => (PathBuf::from(t.back_file_path.as_str()), t.direct_io != 0),
                        Err(_) => return Err(UblkError::OtherError(-libc::EINVAL)),
                    }
                }
                None => return Err(UblkError::OtherError(-libc::EINVAL)),
            }
        }
    };

    let file_path = format!("{}", file.as_path().display());
    let lo = LoopTgt {
        back_file: std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file)
            .unwrap(),
        direct_io: i32::from(dio),
        back_file_path: file_path,
    };

    let tgt_init = |dev: &mut UblkDev| lo_init_tgt(dev, &lo);
    let (mut ctrl, dev) = sess.create_devices(tgt_init).unwrap();

    //todo: USER_COPY should be the default option
    if (ctrl.dev_info.flags & (libublk::sys::UBLK_F_USER_COPY as u64)) != 0 {
        return Err(UblkError::OtherError(-libc::EINVAL));
    }

    let depth = dev.dev_info.queue_depth;
    let q_handler = move |qid: u16, dev: &UblkDev| {
        let q_rc = Rc::new(UblkQueue::new(qid as u16, &dev, false).unwrap());
        let exe = Executor::new(dev.get_nr_ios());

        for tag in 0..depth as u16 {
            let q = q_rc.clone();

            exe.spawn(tag as u16, async move {
                let buf_addr = q.get_io_buf_addr(tag) as u64;
                let mut cmd_op = libublk::sys::UBLK_IO_FETCH_REQ;
                let mut res = 0;
                loop {
                    let cmd_res = q.submit_io_cmd(tag, cmd_op, buf_addr, res).await;
                    if cmd_res == libublk::sys::UBLK_IO_RES_ABORT {
                        break;
                    }

                    res = lo_handle_io_cmd_async(&q, tag).await;
                    cmd_op = libublk::sys::UBLK_IO_COMMIT_AND_FETCH_REQ;
                }
            });
        }
        q_rc.wait_and_wake_io_tasks(&exe);
    };

    sess.run_target(&mut ctrl, &dev, q_handler, |dev_id| {
        let mut d_ctrl = UblkCtrl::new_simple(dev_id, 0).unwrap();
        d_ctrl.dump();
    })
    .unwrap();
    Ok(0)
}
