use io_uring::{opcode, squeue, types};
use libublk::io::{UblkDev, UblkIOCtx, UblkQueue};
use libublk::uring_async::ublk_wait_and_handle_ios;
use libublk::{ctrl::UblkCtrl, helpers::IoBuf, UblkError, UblkIORes};
use log::trace;
use serde::{Deserialize, Serialize};
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;

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

    /// disable discard
    #[clap(long, default_value_t = false)]
    pub no_discard: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct LoJson {
    back_file_path: String,
    direct_io: i32,
    async_await: bool,
    no_discard: bool,
}

struct LoopTgt {
    pub json: LoJson,
    pub back_file: std::fs::File,
}

impl LoopTgt {
    pub fn new(json: LoJson, back_file: std::fs::File) -> Self {
        Self { json, back_file }
    }
}

#[inline]
fn __lo_prep_submit_io_cmd(iod: &libublk::sys::ublksrv_io_desc) -> i32 {
    let op = iod.op_flags & 0xff;

    match op {
        libublk::sys::UBLK_IO_OP_FLUSH
        | libublk::sys::UBLK_IO_OP_READ
        | libublk::sys::UBLK_IO_OP_WRITE
        | libublk::sys::UBLK_IO_OP_DISCARD
        | libublk::sys::UBLK_IO_OP_WRITE_ZEROES => 0,
        _ => -libc::EINVAL,
    }
}

fn lo_fallocate_mode(ublk_op: u32, flags: u32) -> i32 {
    /* follow logic of linux kernel loop */
    libc::FALLOC_FL_KEEP_SIZE
        | if ublk_op == libublk::sys::UBLK_IO_OP_DISCARD {
            libc::FALLOC_FL_PUNCH_HOLE
        } else if ublk_op == libublk::sys::UBLK_IO_OP_WRITE_ZEROES {
            if (flags & libublk::sys::UBLK_IO_F_NOUNMAP) != 0 {
                libc::FALLOC_FL_ZERO_RANGE
            } else {
                libc::FALLOC_FL_PUNCH_HOLE
            }
        } else {
            libc::FALLOC_FL_ZERO_RANGE
        }
}

fn lo_make_discard_sqe(op: u32, flags: u32, off: u64, bytes: u32) -> io_uring::squeue::Entry {
    let mode = lo_fallocate_mode(op, flags);

    opcode::Fallocate::new(types::Fixed(1), bytes as u64)
        .offset(off)
        .mode(mode)
        .build()
        .flags(squeue::Flags::FIXED_FILE)
}

#[inline]
fn __lo_make_io_sqe_zc(
    iod: &libublk::sys::ublksrv_io_desc,
    buf_index: u16,
) -> io_uring::squeue::Entry {
    let op = iod.op_flags & 0xff;
    let off = iod.start_sector << 9;
    let bytes = iod.nr_sectors << 9;

    match op {
        libublk::sys::UBLK_IO_OP_FLUSH => opcode::SyncFileRange::new(types::Fixed(1), bytes)
            .offset(off)
            .build()
            .flags(squeue::Flags::FIXED_FILE),
        libublk::sys::UBLK_IO_OP_READ => {
            opcode::ReadFixed::new(types::Fixed(1), std::ptr::null_mut(), bytes, buf_index)
                .offset(off)
                .build()
                .flags(squeue::Flags::FIXED_FILE)
        }
        libublk::sys::UBLK_IO_OP_WRITE => {
            opcode::WriteFixed::new(types::Fixed(1), std::ptr::null(), bytes, buf_index)
                .offset(off)
                .build()
                .flags(squeue::Flags::FIXED_FILE)
        }
        libublk::sys::UBLK_IO_OP_DISCARD | libublk::sys::UBLK_IO_OP_WRITE_ZEROES => {
            lo_make_discard_sqe(op, iod.op_flags >> 8, off, bytes)
        }
        _ => panic!(),
    }
}

#[inline]
fn __lo_make_io_sqe(
    iod: &libublk::sys::ublksrv_io_desc,
    buf_addr: *mut u8,
) -> io_uring::squeue::Entry {
    let op = iod.op_flags & 0xff;
    let off = iod.start_sector << 9;
    let bytes = iod.nr_sectors << 9;

    match op {
        libublk::sys::UBLK_IO_OP_FLUSH => opcode::Fsync::new(types::Fixed(1)).build(),
        libublk::sys::UBLK_IO_OP_READ => opcode::Read::new(types::Fixed(1), buf_addr, bytes)
            .offset(off)
            .build(),
        libublk::sys::UBLK_IO_OP_WRITE => opcode::Write::new(types::Fixed(1), buf_addr, bytes)
            .offset(off)
            .build(),
        libublk::sys::UBLK_IO_OP_DISCARD | libublk::sys::UBLK_IO_OP_WRITE_ZEROES => {
            lo_make_discard_sqe(op, iod.op_flags >> 8, off, bytes)
        }
        _ => panic!(),
    }
}

#[inline]
async fn lo_handle_io_cmd_async(q: &UblkQueue<'_>, tag: u16, buf_addr: *mut u8, zc: bool) -> i32 {
    let iod = q.get_iod(tag);
    let res = __lo_prep_submit_io_cmd(iod);
    if res < 0 {
        return res;
    }

    for _ in 0..4 {
        let sqe = if zc {
            __lo_make_io_sqe_zc(iod, tag)
        } else {
            __lo_make_io_sqe(iod, buf_addr)
        };
        let res = q.ublk_submit_sqe(sqe).await;
        if res != -(libc::EAGAIN) {
            return res;
        }
    }

    -libc::EAGAIN
}

fn lo_init_tgt(dev: &mut UblkDev, lo: &LoopTgt, opt: Option<LoopArgs>) -> Result<(), UblkError> {
    trace!("loop: init_tgt {}", dev.dev_info.dev_id);
    let info = dev.dev_info;

    if lo.json.direct_io != 0 {
        unsafe {
            libc::fcntl(lo.back_file.as_raw_fd(), libc::F_SETFL, libc::O_DIRECT);
        }
    }

    let tgt = &mut dev.tgt;
    let nr_fds = tgt.nr_fds;
    tgt.fds[nr_fds as usize] = lo.back_file.as_raw_fd();
    tgt.nr_fds = nr_fds + 1;

    let sz = crate::ublk_file_size(&lo.back_file).unwrap();
    let attrs = libublk::sys::UBLK_ATTR_VOLATILE_CACHE;

    tgt.dev_size = sz.0;
    //todo: figure out correct block size
    tgt.params = libublk::sys::ublk_params {
        types: libublk::sys::UBLK_PARAM_TYPE_BASIC
            | if !lo.json.no_discard {
                libublk::sys::UBLK_PARAM_TYPE_DISCARD
            } else {
                0
            },
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

        discard: libublk::sys::ublk_param_discard {
            discard_granularity: (sz.2 as u32) << 9,
            max_discard_sectors: 1 << 30 >> 9,
            max_write_zeroes_sectors: 1 << 30 >> 9,
            max_discard_segments: 1,
            ..Default::default()
        },
        ..Default::default()
    };

    if let Some(o) = opt {
        o.gen_arg.apply_block_size(dev);
        o.gen_arg.apply_read_only(dev);
    }

    let val = serde_json::json!({"loop": &lo.json });
    dev.set_target_json(val);

    Ok(())
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
        let sqe = __lo_make_io_sqe(iod, buf_addr).user_data(data);
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

async fn handle_queue_tag_async_zc(q: Rc<UblkQueue<'_>>, tag: u16) {
    let mut cmd_op = libublk::sys::UBLK_U_IO_FETCH_REQ;
    let mut res = 0;
    let auto_buf_reg = libublk::sys::ublk_auto_buf_reg {
        index: tag,
        flags: libublk::sys::UBLK_AUTO_BUF_REG_FALLBACK as u8,
        ..Default::default()
    };

    loop {
        let cmd_res = q
            .submit_io_cmd_with_auto_buf_reg(tag, cmd_op, &auto_buf_reg, res)
            .await;
        if cmd_res == libublk::sys::UBLK_IO_RES_ABORT {
            break;
        }

        res = lo_handle_io_cmd_async(&q, tag, std::ptr::null_mut(), true).await;
        cmd_op = libublk::sys::UBLK_U_IO_COMMIT_AND_FETCH_REQ;
    }
}

async fn handle_queue_tag_async(q: Rc<UblkQueue<'_>>, tag: u16) {
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

        res = lo_handle_io_cmd_async(&q, tag, buf_addr, false).await;
        cmd_op = libublk::sys::UBLK_U_IO_COMMIT_AND_FETCH_REQ;
    }
}

fn q_a_fn(qid: u16, dev: &UblkDev) {
    let depth = dev.dev_info.queue_depth;
    let q_rc = Rc::new(UblkQueue::new(qid, dev).unwrap());
    let exe = smol::LocalExecutor::new();
    let mut f_vec = Vec::new();

    for tag in 0..depth {
        let q = q_rc.clone();

        if q.support_auto_buf_zc() {
            f_vec.push(exe.spawn(handle_queue_tag_async_zc(q, tag)));
        } else {
            f_vec.push(exe.spawn(handle_queue_tag_async(q, tag)));
        }
    }
    ublk_wait_and_handle_ios(&exe, &q_rc);
    smol::block_on(async { futures::future::join_all(f_vec).await });
}

pub(crate) fn ublk_add_loop(
    ctrl: UblkCtrl,
    opt: Option<LoopArgs>,
    comm_rc: &Arc<crate::DevIdComm>,
) -> anyhow::Result<i32> {
    let (file, dio, ro, aa, no_discard) = match opt {
        Some(ref o) => (
            o.gen_arg.build_abs_path(o.file.clone()),
            !o.buffered_io,
            o.gen_arg.read_only,
            o.async_await,
            o.no_discard,
        ),
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
                            t.no_discard,
                        ),
                        Err(_) => return Err(anyhow::anyhow!("wrong json data")),
                    }
                }
                None => return Err(anyhow::anyhow!("not get json data")),
            }
        }
    };

    let file_path = format!("{}", file.as_path().display());
    let json = LoJson {
        back_file_path: file_path,
        direct_io: i32::from(dio),
        async_await: aa,
        no_discard,
    };
    let lo = LoopTgt::new(
        json,
        std::fs::OpenOptions::new()
            .read(true)
            .write(!ro)
            .open(&file)
            .unwrap(),
    );

    if (ctrl.dev_info().flags & (libublk::sys::UBLK_F_USER_COPY as u64)) != 0 {
        return Err(anyhow::anyhow!("loop doesn't support user copy"));
    }

    if ((ctrl.dev_info().flags & (libublk::sys::UBLK_F_AUTO_BUF_REG as u64)) != 0) && !aa {
        return Err(anyhow::anyhow!("loop zero copy requires --async-wait"));
    }

    let comm = comm_rc.clone();
    ctrl.run_target(
        |dev: &mut UblkDev| lo_init_tgt(dev, &lo, opt),
        move |qid, dev: &_| {
            if lo.json.async_await {
                q_a_fn(qid, dev)
            } else {
                q_fn(qid, dev)
            }
        },
        move |ctrl: &UblkCtrl| comm.send_dev_id(ctrl.dev_info().dev_id).unwrap(),
    )
    .unwrap();
    Ok(0)
}
