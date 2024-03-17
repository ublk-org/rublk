use io_uring::{opcode, squeue, types};
use libublk::io::{UblkDev, UblkIOCtx, UblkQueue};
use libublk::uring_async::ublk_wait_and_handle_ios;
use libublk::{ctrl::UblkCtrl, helpers::IoBuf, UblkError, UblkIORes};
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

    /// use async_await
    #[clap(long, short = 'a', default_value_t = false)]
    pub async_await: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct LoJson {
    back_file_path: String,
    direct_io: i32,
    async_await: bool,
}

pub(crate) struct LoopTgt {
    pub back_file_path: String,
    pub back_file: std::fs::File,
    pub direct_io: i32,
    pub async_await: bool,
}

#[inline]
fn __lo_prep_submit_io_cmd(iod: &libublk::sys::ublksrv_io_desc) -> i32 {
    let op = iod.op_flags & 0xff;

    match op {
        libublk::sys::UBLK_IO_OP_FLUSH
        | libublk::sys::UBLK_IO_OP_READ
        | libublk::sys::UBLK_IO_OP_WRITE => 0,
        _ => -libc::EINVAL,
    }
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
        let off = iod.start_sector << 9;
        let bytes = iod.nr_sectors << 9;

        let sqe = __lo_make_io_sqe(op, off, bytes, buf_addr);
        let res = q.ublk_submit_sqe(sqe).await;
        if res != -(libc::EAGAIN) {
            return res;
        }
    }

    -libc::EAGAIN
}

fn lo_init_tgt(
    dev: &mut UblkDev,
    lo: &LoopTgt,
    opt: Option<LoopArgs>,
    dio: bool,
) -> Result<(), UblkError> {
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
    let attrs = if dio {
        0
    } else {
        libublk::sys::UBLK_ATTR_VOLATILE_CACHE
    };

    tgt.dev_size = sz.0;
    //todo: figure out correct block size
    tgt.params = libublk::sys::ublk_params {
        types: libublk::sys::UBLK_PARAM_TYPE_BASIC,
        basic: libublk::sys::ublk_param_basic {
            attrs,
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

    let val = serde_json::json!({"loop": LoJson { back_file_path: lo.back_file_path.clone(), direct_io: lo.direct_io, async_await:lo.async_await, } });
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

fn lo_handle_io_cmd_sync(q: &UblkQueue<'_>, tag: u16, i: &UblkIOCtx, buf_addr: *mut u8) {
    let iod = q.get_iod(tag);
    let op = iod.op_flags & 0xff;
    let data = UblkIOCtx::build_user_data(tag, op, 0, true);
    if i.is_tgt_io() {
        let user_data = i.user_data();
        let res = i.result();
        let cqe_tag = UblkIOCtx::user_data_to_tag(user_data);

        assert!(cqe_tag == tag as u32);

        if res != -(libc::EAGAIN) {
            q.complete_io_cmd(tag, buf_addr, Ok(UblkIORes::Result(res)));
            return;
        }
    }

    let res = __lo_prep_submit_io_cmd(iod);
    if res < 0 {
        q.complete_io_cmd(tag, buf_addr, Ok(UblkIORes::Result(res)));
    } else {
        let op = iod.op_flags & 0xff;
        // either start to handle or retry
        let off = iod.start_sector << 9;
        let bytes = iod.nr_sectors << 9;
        let sqe = __lo_make_io_sqe(op, off, bytes, buf_addr).user_data(data);
        q.ublk_submit_sqe_sync(sqe).unwrap();
    }
}

fn q_fn(qid: u16, dev: &UblkDev) {
    let bufs_rc = Rc::new(dev.alloc_queue_io_bufs());
    let bufs = bufs_rc.clone();
    let lo_io_handler = move |q: &UblkQueue, tag: u16, io: &UblkIOCtx| {
        let bufs = bufs_rc.clone();

        lo_handle_io_cmd_sync(q, tag, io, bufs[tag as usize].as_mut_ptr());
    };

    UblkQueue::new(qid, dev)
        .unwrap()
        .regiser_io_bufs(Some(&bufs))
        .submit_fetch_commands(Some(&bufs))
        .wait_and_handle_io(lo_io_handler);
}

fn q_a_fn(qid: u16, dev: &UblkDev) {
    let depth = dev.dev_info.queue_depth;
    let q_rc = Rc::new(UblkQueue::new(qid, dev).unwrap());
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

pub(crate) fn ublk_add_loop(ctrl: UblkCtrl, opt: Option<LoopArgs>) -> Result<i32, UblkError> {
    let (file, dio, ro, aa, _shm, fg) = match opt {
        Some(ref o) => {
            let parent = o.gen_arg.get_start_dir();

            (
                to_absolute_path(o.file.clone(), parent),
                !o.buffered_io,
                o.gen_arg.read_only,
                o.async_await,
                Some(o.gen_arg.get_shm_id()),
                o.gen_arg.foreground,
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
                            t.async_await,
                            None,
                            false,
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
        async_await: aa,
    };

    //todo: USER_COPY should be the default option
    if (ctrl.dev_info().flags & (libublk::sys::UBLK_F_USER_COPY as u64)) != 0 {
        return Err(UblkError::OtherError(-libc::EINVAL));
    }

    ctrl.run_target(
        |dev: &mut UblkDev| lo_init_tgt(dev, &lo, opt, dio),
        move |qid, dev: &_| if aa { q_a_fn(qid, dev) } else { q_fn(qid, dev) },
        move |ctrl: &UblkCtrl| crate::rublk_prep_dump_dev(_shm, fg, ctrl),
    )
    .unwrap();
    Ok(0)
}
