use libublk::{
    ctrl::UblkCtrl,
    helpers::IoBuf,
    io::{BufDesc, BufDescList, UblkDev, UblkIOCtx, UblkQueue},
    ShmemBufs, UblkError,
};
use std::rc::Rc;
use std::sync::Arc;

#[derive(clap::Args, Debug)]
pub(crate) struct NullAddArgs {
    #[command(flatten)]
    pub(crate) gen_arg: super::args::GenAddArgs,

    /// use async/.await
    #[clap(long, short = 'a', default_value_t = false)]
    async_await: bool,
}

#[inline]
fn get_io_cmd_result(q: &UblkQueue, tag: u16, shmem: &ShmemBufs) -> i32 {
    let iod = q.get_iod(tag);
    let op = iod.op_flags & 0xff;

    match op {
        libublk::sys::UBLK_IO_OP_READ | libublk::sys::UBLK_IO_OP_WRITE => {
            // null moves no data, but a request the driver matched to a
            // shared buffer must still name one this device registered
            if iod.op_flags & libublk::sys::UBLK_IO_F_SHMEM_ZC != 0 && shmem.resolve(iod).is_none()
            {
                return -libc::EINVAL;
            }
            (iod.nr_sectors << 9) as i32
        }
        _ => 0,
    }
}

#[inline]
fn handle_io_cmd(
    q: &UblkQueue,
    tag: u16,
    buf: Option<&[u8]>,
    shmem: &ShmemBufs,
) -> Result<(), UblkError> {
    let bytes = get_io_cmd_result(q, tag, shmem);
    let buf_desc = match buf {
        Some(slice) => BufDesc::Slice(slice),
        None => BufDesc::Slice(&[]),
    };

    q.complete_io_cmd_unified(tag, buf_desc, bytes)
}

fn q_sync_zc_fn(qid: u16, dev: &Arc<UblkDev>, shmem: Arc<ShmemBufs>) -> Result<(), UblkError> {
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
    let io_handler = move |q: &UblkQueue, tag: u16, _io: &UblkIOCtx| {
        let bytes = get_io_cmd_result(q, tag, &shmem);
        let buf_desc = BufDesc::AutoReg(auto_buf_reg_list[tag as usize]);
        if let Err(e) = q.complete_io_cmd_unified(tag, buf_desc, bytes) {
            log::error!("complete_io_cmd_unified failed {}/{}: {}", qid, tag, e);
        }
    };

    UblkQueue::new(qid, dev)?
        .submit_fetch_commands_unified(BufDescList::AutoRegs(&auto_buf_reg_list_rc))?
        .wait_and_handle_io(io_handler);
    Ok(())
}

fn q_sync_fn(
    qid: u16,
    dev: &Arc<UblkDev>,
    user_copy: bool,
    shmem: Arc<ShmemBufs>,
) -> Result<(), UblkError> {
    let bufs_rc = Rc::new(dev.alloc_queue_io_bufs());
    let bufs = bufs_rc.clone();

    // logic for io handling
    let io_handler = move |q: &UblkQueue, tag: u16, _io: &UblkIOCtx| {
        let buf = if user_copy {
            None
        } else {
            Some(&*bufs[tag as usize])
        };
        if let Err(e) = handle_io_cmd(q, tag, buf, &shmem) {
            log::error!("handle_io_cmd failed {}/{}: {}", qid, tag, e);
        }
    };

    UblkQueue::new(qid, dev)?
        .submit_fetch_commands_unified(BufDescList::Slices(if user_copy {
            None
        } else {
            Some(&bufs_rc)
        }))?
        .wait_and_handle_io(io_handler);
    Ok(())
}

#[inline]
async fn __handle_queue_tag_async_null(
    q: Rc<UblkQueue>,
    tag: u16,
    buf: Option<&IoBuf<u8>>,
    user_copy: bool,
    shmem: &ShmemBufs,
) -> Result<(), UblkError> {
    let auto_buf_reg = libublk::sys::ublk_auto_buf_reg {
        index: tag,
        flags: libublk::sys::UBLK_AUTO_BUF_REG_FALLBACK as u8,
        ..Default::default()
    };

    let buf_desc = match buf {
        Some(io_buf) => BufDesc::Slice(io_buf.as_slice()),
        None if user_copy => BufDesc::Slice(&[]),
        _ => BufDesc::AutoReg(auto_buf_reg),
    };

    // Submit initial prep command
    q.submit_io_prep_cmd(tag, buf_desc.clone(), 0, buf).await?;
    loop {
        let res = get_io_cmd_result(&q, tag, shmem);
        q.submit_io_commit_cmd(tag, buf_desc.clone(), res).await?;
    }
}

async fn handle_queue_tag_async_null(
    q: Rc<UblkQueue>,
    tag: u16,
    user_copy: bool,
    shmem: Arc<ShmemBufs>,
) -> Result<(), UblkError> {
    if q.support_auto_buf_zc() {
        __handle_queue_tag_async_null(q, tag, None, user_copy, &shmem).await
    } else {
        let buf = if user_copy {
            None
        } else {
            Some(IoBuf::<u8>::new(q.dev().dev_info.max_io_buf_bytes as usize))
        };
        __handle_queue_tag_async_null(q, tag, buf.as_ref(), user_copy, &shmem).await
    }
}

fn q_async_fn(
    qid: u16,
    dev: &Arc<UblkDev>,
    user_copy: bool,
    shmem: Arc<ShmemBufs>,
) -> Result<(), UblkError> {
    crate::Rt::run_io_tasks(dev, qid, move |q, tag| {
        handle_queue_tag_async_null(q, tag, user_copy, shmem.clone())
    })
}

pub(crate) fn ublk_add_null(
    ctrl: UblkCtrl,
    opt: Option<NullAddArgs>,
    comm_arc: &Arc<crate::DevIdComm>,
) -> anyhow::Result<i32> {
    let size = 250_u64 << 30;
    let flags = ctrl.dev_info().flags;
    let user_copy = (flags & libublk::sys::UBLK_F_USER_COPY as u64) != 0;

    if flags & libublk::sys::UBLK_F_UNPRIVILEGED_DEV as u64 != 0 {
        return Err(anyhow::anyhow!("null doesn't support unprivileged"));
    }

    let aa = if let Some(ref o) = opt {
        o.async_await
    } else {
        false
    };

    let shmem = crate::shmem::Shmem::new(&ctrl, opt.as_ref().map(|o| &o.gen_arg))?;
    let shmem_bufs = shmem.bufs.clone();

    let tgt_init = |dev: &mut UblkDev| {
        dev.set_default_params(size);
        let p = &mut dev.tgt.params;

        p.types |= libublk::sys::UBLK_PARAM_TYPE_DISCARD;
        p.discard.max_discard_sectors = 2 << 30 >> 9;
        p.discard.discard_granularity = 1 << p.basic.physical_bs_shift;
        p.discard.max_discard_segments = 1;

        if let Some(ref o) = opt {
            o.gen_arg.apply_block_size(dev);
            o.gen_arg.apply_read_only(dev);
        }
        shmem.save_json(dev);
        Ok(())
    };

    let q_handler = move |qid, dev: &Arc<UblkDev>| {
        let shmem = shmem_bufs.clone();
        let result = if aa {
            q_async_fn(qid, dev, user_copy, shmem)
        } else {
            if (flags & libublk::sys::UBLK_F_AUTO_BUF_REG as u64) != 0 {
                q_sync_zc_fn(qid, dev, shmem)
            } else {
                q_sync_fn(qid, dev, user_copy, shmem)
            }
        };
        if let Err(e) = result {
            log::error!("Queue handler failed for queue {}: {}", qid, e);
        }
    };

    let comm = comm_arc.clone();
    match ctrl.run_target(tgt_init, q_handler, move |dev: &UblkCtrl| {
        if let Err(e) = comm.send_dev_id(dev.dev_info().dev_id) {
            log::error!("Failed to send device ID: {}", e);
        }
    }) {
        Ok(_) => {}
        Err(e) => log::error!("Failed to run target: {}", e),
    }

    Ok(0)
}
