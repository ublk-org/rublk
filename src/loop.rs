use anyhow::Context;
use io_uring::{opcode, squeue, types};
use libublk::io::{BufDesc, BufDescList, UblkDev, UblkIOCtx, UblkQueue};
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

fn __lo_make_io_sqe_zc(
    iod: &libublk::sys::ublksrv_io_desc,
    buf_index: u16,
) -> io_uring::squeue::Entry {
    let op = iod.op_flags & 0xff;
    let off = iod.start_sector << 9;
    let bytes = iod.nr_sectors << 9;

    match op {
        libublk::sys::UBLK_IO_OP_FLUSH => opcode::Fsync::new(types::Fixed(1))
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

fn __lo_make_io_sqe(
    iod: &libublk::sys::ublksrv_io_desc,
    buf: Option<&[u8]>,
) -> io_uring::squeue::Entry {
    let op = iod.op_flags & 0xff;
    let off = iod.start_sector << 9;
    let bytes = iod.nr_sectors << 9;
    let buf_addr = buf.map_or(std::ptr::null(), |b| b.as_ptr()) as *mut u8;

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

async fn lo_handle_io_cmd_async(q: &UblkQueue<'_>, tag: u16, buf: Option<&[u8]>) -> i32 {
    let iod = q.get_iod(tag);
    let res = __lo_prep_submit_io_cmd(iod);
    if res < 0 {
        return res;
    }

    let zc = buf.is_none();
    for _ in 0..4 {
        let sqe = if zc {
            __lo_make_io_sqe_zc(iod, tag)
        } else {
            __lo_make_io_sqe(iod, buf)
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

#[inline]
fn __lo_handle_io_cmd_sync(
    q: &UblkQueue<'_>,
    tag: u16,
    i: &UblkIOCtx,
    buf_desc: BufDesc,
    buf: Option<&[u8]>,
) {
    let iod = q.get_iod(tag);
    let op = iod.op_flags & 0xff;
    let data = UblkIOCtx::build_user_data(tag, op, 0, true);
    if i.is_tgt_io() {
        let user_data = i.user_data();
        let res = i.result();
        let cqe_tag = UblkIOCtx::user_data_to_tag(user_data);

        assert!(cqe_tag == tag as u32);

        if res != -(libc::EAGAIN) {
            q.complete_io_cmd_unified(tag, buf_desc, Ok(UblkIORes::Result(res)))
                .unwrap();
            return;
        }
    }

    let res = __lo_prep_submit_io_cmd(iod);
    if res < 0 {
        q.complete_io_cmd_unified(tag, buf_desc, Ok(UblkIORes::Result(res)))
            .unwrap();
    } else {
        let zc = buf.is_none();
        let sqe = if zc {
            __lo_make_io_sqe_zc(iod, tag).user_data(data)
        } else {
            __lo_make_io_sqe(iod, buf).user_data(data)
        };
        q.ublk_submit_sqe_sync(sqe).unwrap();
    }
}

fn lo_handle_io_cmd_sync(q: &UblkQueue<'_>, tag: u16, i: &UblkIOCtx, buf: Option<&[u8]>) {
    let buf_desc = match buf {
        Some(slice) => BufDesc::Slice(slice),
        None => BufDesc::Slice(&[]),
    };
    __lo_handle_io_cmd_sync(q, tag, i, buf_desc, buf);
}

fn q_sync_fn_zc(qid: u16, dev: &UblkDev) {
    let auto_buf_reg_list_rc = Rc::new(
        (0..dev.dev_info.queue_depth)
            .map(|tag| libublk::sys::ublk_auto_buf_reg {
                index: tag,
                flags: libublk::sys::UBLK_AUTO_BUF_REG_FALLBACK as u8,
                ..Default::default()
            })
            .collect::<Vec<_>>(),
    );

    let auto_buf_reg_list = auto_buf_reg_list_rc.clone();
    let lo_io_handler = move |q: &UblkQueue, tag: u16, io: &UblkIOCtx| {
        let buf_desc = BufDesc::AutoReg(auto_buf_reg_list[tag as usize]);
        __lo_handle_io_cmd_sync(q, tag, io, buf_desc, None);
    };

    UblkQueue::new(qid, dev)
        .unwrap()
        .submit_fetch_commands_unified(BufDescList::AutoRegs(&auto_buf_reg_list_rc))
        .unwrap()
        .wait_and_handle_io(lo_io_handler);
}

fn q_sync_fn_buf(qid: u16, dev: &UblkDev) {
    let bufs_rc = Rc::new(dev.alloc_queue_io_bufs());
    let bufs = bufs_rc.clone();
    let lo_io_handler = move |q: &UblkQueue, tag: u16, io: &UblkIOCtx| {
        let bufs = bufs.clone();
        let buf_slice = &*bufs[tag as usize];
        lo_handle_io_cmd_sync(q, tag, io, Some(buf_slice));
    };

    UblkQueue::new(qid, dev)
        .unwrap()
        .regiser_io_bufs(Some(&bufs_rc))
        .submit_fetch_commands_unified(BufDescList::Slices(Some(&bufs_rc)))
        .unwrap()
        .wait_and_handle_io(lo_io_handler);
}

fn q_sync_fn(qid: u16, dev: &UblkDev) {
    let flags = dev.dev_info.flags;
    if (flags & libublk::sys::UBLK_F_AUTO_BUF_REG as u64) != 0 {
        q_sync_fn_zc(qid, dev);
    } else {
        q_sync_fn_buf(qid, dev);
    }
}

#[inline]
async fn __handle_queue_tag_async(q: Rc<UblkQueue<'_>>, tag: u16, buf: Option<&IoBuf<u8>>) {
    let mut cmd_op = libublk::sys::UBLK_U_IO_FETCH_REQ;
    let mut res = 0;
    let auto_buf_reg = libublk::sys::ublk_auto_buf_reg {
        index: tag,
        flags: libublk::sys::UBLK_AUTO_BUF_REG_FALLBACK as u8,
        ..Default::default()
    };
    let (buf_desc, buf_ref) = match buf {
        Some(io_buf) => {
            q.register_io_buf(tag, &io_buf);
            (BufDesc::Slice(io_buf.as_slice()), Some(io_buf.as_slice()))
        }
        _ => (BufDesc::AutoReg(auto_buf_reg), None),
    };

    loop {
        let cmd_res = q
            .submit_io_cmd_unified(tag, cmd_op, buf_desc.clone(), res)
            .unwrap()
            .await;
        if cmd_res == libublk::sys::UBLK_IO_RES_ABORT {
            break;
        }

        res = lo_handle_io_cmd_async(&q, tag, buf_ref).await;
        cmd_op = libublk::sys::UBLK_U_IO_COMMIT_AND_FETCH_REQ;
    }
}

async fn handle_queue_tag_async(q: Rc<UblkQueue<'_>>, tag: u16) {
    if q.support_auto_buf_zc() {
        __handle_queue_tag_async(q, tag, None).await
    } else {
        let buf = IoBuf::<u8>::new(q.dev.dev_info.max_io_buf_bytes as usize);

        __handle_queue_tag_async(q, tag, Some(&buf)).await
    };
}

fn q_a_fn(qid: u16, dev: &UblkDev) {
    let depth = dev.dev_info.queue_depth;
    let q_rc = Rc::new(UblkQueue::new(qid, dev).unwrap());
    let exe = smol::LocalExecutor::new();
    let mut f_vec = Vec::new();

    for tag in 0..depth {
        let q = q_rc.clone();

        f_vec.push(exe.spawn(handle_queue_tag_async(q, tag)));
    }
    ublk_wait_and_handle_ios(&exe, &q_rc);
    smol::block_on(async { futures::future::join_all(f_vec).await });
}

pub(crate) fn ublk_add_loop(
    ctrl: UblkCtrl,
    opt: Option<LoopArgs>,
    comm_rc: &Arc<crate::DevIdComm>,
) -> anyhow::Result<i32> {
    let (file, dio, ro, aa, no_discard) = if let Some(ref o) = opt {
        (
            o.gen_arg.build_abs_path(o.file.clone()),
            !o.buffered_io,
            o.gen_arg.read_only,
            o.async_await,
            o.no_discard,
        )
    } else {
        let mut p: libublk::sys::ublk_params = Default::default();
        ctrl.get_params(&mut p)?;

        let val = ctrl
            .get_target_data_from_json()
            .ok_or_else(|| anyhow::anyhow!("not get json data"))?;
        let lo = &val["loop"];
        let tgt_data: LoJson =
            serde_json::from_value(lo.clone()).map_err(|e| anyhow::anyhow!(e))?;

        (
            PathBuf::from(tgt_data.back_file_path.as_str()),
            tgt_data.direct_io != 0,
            (p.basic.attrs & libublk::sys::UBLK_ATTR_READ_ONLY) != 0,
            tgt_data.async_await,
            tgt_data.no_discard,
        )
    };

    let file_path = format!("{}", file.as_path().display());
    let json = LoJson {
        back_file_path: file_path,
        direct_io: dio.into(),
        async_await: aa,
        no_discard,
    };
    let lo = LoopTgt::new(
        json,
        std::fs::OpenOptions::new()
            .read(true)
            .write(!ro)
            .open(&file)
            .with_context(|| format!("failed to open backing file {:?}", file))?,
    );

    if (ctrl.dev_info().flags & (libublk::sys::UBLK_F_USER_COPY as u64)) != 0 {
        return Err(anyhow::anyhow!("loop doesn't support user copy"));
    }

    let comm = comm_rc.clone();
    ctrl.run_target(
        |dev: &mut UblkDev| lo_init_tgt(dev, &lo, opt),
        move |qid, dev: &_| {
            if lo.json.async_await {
                q_a_fn(qid, dev)
            } else {
                q_sync_fn(qid, dev)
            }
        },
        move |ctrl: &UblkCtrl| comm.send_dev_id(ctrl.dev_info().dev_id).unwrap(),
    )
    .unwrap();
    Ok(0)
}
