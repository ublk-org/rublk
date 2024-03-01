use io_uring::{opcode, squeue, types};
use libublk::ctrl::UblkCtrl;
use libublk::io::{UblkDev, UblkQueue};
use libublk::uring_async::ublk_wait_and_handle_ios;
use libublk::{helpers::IoBuf, UblkError};
use log::trace;
use serde::{Deserialize, Serialize};
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

    /// buffered io is applied for backing file of ublk target, default is direct IO
    #[clap(long, default_value_t = false)]
    pub buffered_io: bool,

    /// use smol executor
    #[clap(long, default_value_t = false)]
    pub smol: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct LoJson {
    back_file_path: String,
    direct_io: i32,
    smol: bool,
}

pub struct LoopTgt {
    pub back_file_path: String,
    pub back_file: std::fs::File,
    pub direct_io: i32,
    pub smol: bool,
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
fn __lo_make_io_sqe(op: u32, off: u64, bytes: u32, buf_addr: *mut u8) -> io_uring::squeue::Entry {
    match op {
        libublk::sys::UBLK_IO_OP_FLUSH => opcode::SyncFileRange::new(types::Fixed(1), bytes)
            .offset(off)
            .build()
            .flags(squeue::Flags::FIXED_FILE),
        libublk::sys::UBLK_IO_OP_READ => opcode::Read::new(types::Fixed(1), buf_addr, bytes)
            .offset(off)
            .build()
            .flags(squeue::Flags::FIXED_FILE),
        libublk::sys::UBLK_IO_OP_WRITE => opcode::Write::new(types::Fixed(1), buf_addr, bytes)
            .offset(off)
            .build()
            .flags(squeue::Flags::FIXED_FILE),
        _ => panic!(),
    }
}

#[inline]
async fn lo_handle_io_cmd_async(q: &UblkQueue<'_>, tag: u16, buf_addr: *mut u8) -> i32 {
    let iod = q.get_iod(tag);
    let res = __lo_prep_submit_io_cmd(iod);
    if res < 0 {
        return res;
    }

    for _ in 0..4 {
        let op = iod.op_flags & 0xff;
        // either start to handle or retry
        let off = (iod.start_sector << 9) as u64;
        let bytes = (iod.nr_sectors << 9) as u32;

        let sqe = __lo_make_io_sqe(op, off, bytes, buf_addr);
        let res = q.ublk_submit_sqe(sqe).await;
        if res != -(libc::EAGAIN) {
            return res;
        }
    }

    return -libc::EAGAIN;
}

fn lo_init_tgt(dev: &mut UblkDev, lo: &LoopTgt, opt: Option<LoopArgs>) -> Result<(), UblkError> {
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

    let sz = crate::ublk_file_size(&lo.back_file).unwrap();

    tgt.dev_size = sz.0;
    //todo: figure out correct block size
    tgt.params = libublk::sys::ublk_params {
        types: libublk::sys::UBLK_PARAM_TYPE_BASIC,
        basic: libublk::sys::ublk_param_basic {
            logical_bs_shift: sz.1,
            physical_bs_shift: sz.2,
            io_opt_shift: sz.2,
            io_min_shift: sz.1,
            max_sectors: info.max_io_buf_bytes >> 9,
            dev_sectors: tgt.dev_size >> 9,
            ..Default::default()
        },
        ..Default::default()
    };

    if let Some(o) = opt {
        o.gen_arg.apply_block_size(dev);
        o.gen_arg.apply_read_only(dev);
    }

    let val = serde_json::json!({"loop": LoJson { back_file_path: lo.back_file_path.clone(), direct_io: lo.direct_io, smol:lo.smol, } });
    dev.set_target_json(val);

    Ok(())
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

fn q_a_fn(qid: u16, dev: &UblkDev) {
    let depth = dev.dev_info.queue_depth;
    let q_rc = Rc::new(UblkQueue::new(qid as u16, &dev).unwrap());
    let exe = smol::LocalExecutor::new();
    let mut f_vec = Vec::new();

    for tag in 0..depth {
        let q = q_rc.clone();

        f_vec.push(exe.spawn(async move {
            let buf = IoBuf::<u8>::new(q.dev.dev_info.max_io_buf_bytes as usize);
            let buf_addr = buf.as_mut_ptr();
            let mut cmd_op = libublk::sys::UBLK_U_IO_FETCH_REQ;
            let mut res = 0;

            q.register_io_buf(tag, &buf);
            loop {
                let cmd_res = q.submit_io_cmd(tag, cmd_op, buf_addr, res).await;
                if cmd_res == libublk::sys::UBLK_IO_RES_ABORT {
                    break;
                }

                res = lo_handle_io_cmd_async(&q, tag, buf_addr).await;
                cmd_op = libublk::sys::UBLK_U_IO_COMMIT_AND_FETCH_REQ;
            }
        }));
    }
    ublk_wait_and_handle_ios(&exe, &q_rc);
    smol::block_on(async { futures::future::join_all(f_vec).await });
}

pub fn ublk_add_loop(ctrl: UblkCtrl, _id: i32, opt: Option<LoopArgs>) -> Result<i32, UblkError> {
    let (file, dio, ro, smol) = match opt {
        Some(ref o) => {
            let parent = o.gen_arg.get_start_dir();

            (
                to_absolute_path(o.file.clone(), parent),
                !o.buffered_io,
                o.gen_arg.read_only,
                o.smol,
            )
        }
        None => {
            let mut p: libublk::sys::ublk_params = Default::default();
            ctrl.get_params(&mut p)?;

            match ctrl.get_target_data_from_json() {
                Some(val) => {
                    let lo = &val["loop"];
                    let tgt_data: Result<LoJson, _> = serde_json::from_value(lo.clone());

                    match tgt_data {
                        Ok(t) => (
                            PathBuf::from(t.back_file_path.as_str()),
                            t.direct_io != 0,
                            (p.basic.attrs & libublk::sys::UBLK_ATTR_READ_ONLY) != 0,
                            t.smol,
                        ),
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
            .write(!ro)
            .open(&file)
            .unwrap(),
        direct_io: i32::from(dio),
        back_file_path: file_path,
        smol,
    };

    let _shm = {
        if let Some(ref o) = opt {
            Some(o.gen_arg.get_shm_id())
        } else {
            None
        }
    };

    //todo: USER_COPY should be the default option
    if (ctrl.dev_info().flags & (libublk::sys::UBLK_F_USER_COPY as u64)) != 0 {
        return Err(UblkError::OtherError(-libc::EINVAL));
    }

    ctrl.run_target(
        |dev: &mut UblkDev| lo_init_tgt(dev, &lo, opt),
        move |qid, dev: &_| q_a_fn(qid, dev),
        |ctrl: &UblkCtrl| {
            if let Some(shm) = _shm {
                crate::rublk_write_id_into_shm(&shm, ctrl.dev_info().dev_id);
            }
        },
    )
    .unwrap();
    Ok(0)
}
