use libublk::{
    ctrl::UblkCtrl,
    helpers::IoBuf,
    io::{UblkDev, UblkIOCtx, UblkQueue},
    uring_async::ublk_wait_and_handle_ios,
    UblkError, UblkIORes,
};
use std::rc::Rc;

#[derive(clap::Args, Debug)]
pub(crate) struct NullAddArgs {
    #[command(flatten)]
    pub(crate) gen_arg: super::args::GenAddArgs,

    /// use async/.await
    #[clap(long, short = 'a', default_value_t = false)]
    async_await: bool,
}

#[inline]
fn get_io_cmd_result(q: &UblkQueue, tag: u16) -> i32 {
    let iod = q.get_iod(tag);

    (iod.nr_sectors << 9) as i32
}

#[inline]
fn handle_io_cmd(q: &UblkQueue, tag: u16, buf_addr: *mut u8) {
    let bytes = get_io_cmd_result(q, tag);

    q.complete_io_cmd(tag, buf_addr, Ok(UblkIORes::Result(bytes)));
}

fn q_sync_fn(qid: u16, dev: &UblkDev, user_copy: bool) {
    let bufs_rc = Rc::new(dev.alloc_queue_io_bufs());
    let bufs = bufs_rc.clone();

    // logic for io handling
    let io_handler = move |q: &UblkQueue, tag: u16, _io: &UblkIOCtx| {
        let buf_addr = if user_copy {
            std::ptr::null_mut()
        } else {
            bufs[tag as usize].as_mut_ptr()
        };
        handle_io_cmd(q, tag, buf_addr);
    };

    UblkQueue::new(qid, dev)
        .unwrap()
        .regiser_io_bufs(if user_copy { None } else { Some(&bufs_rc) })
        .submit_fetch_commands(if user_copy { None } else { Some(&bufs_rc) })
        .wait_and_handle_io(io_handler);
}

fn q_async_fn(qid: u16, dev: &UblkDev, user_copy: bool) {
    let q_rc = Rc::new(UblkQueue::new(qid, dev).unwrap());
    let exe = smol::LocalExecutor::new();
    let mut f_vec = Vec::new();

    for tag in 0..dev.dev_info.queue_depth {
        let q = q_rc.clone();

        f_vec.push(exe.spawn(async move {
            let mut cmd_op = libublk::sys::UBLK_U_IO_FETCH_REQ;
            let mut res = 0;
            let (_buf, buf_addr) = if user_copy {
                (None, std::ptr::null_mut())
            } else {
                let buf = IoBuf::<u8>::new(q.dev.dev_info.max_io_buf_bytes as usize);

                q.register_io_buf(tag, &buf);
                let addr = buf.as_mut_ptr();
                (Some(buf), addr)
            };

            loop {
                let cmd_res = q.submit_io_cmd(tag, cmd_op, buf_addr, res).await;
                if cmd_res == libublk::sys::UBLK_IO_RES_ABORT {
                    break;
                }

                res = get_io_cmd_result(&q, tag);
                cmd_op = libublk::sys::UBLK_U_IO_COMMIT_AND_FETCH_REQ;
            }
        }));
    }
    ublk_wait_and_handle_ios(&exe, &q_rc);
    smol::block_on(async { futures::future::join_all(f_vec).await });
}

pub(crate) fn ublk_add_null(ctrl: UblkCtrl, opt: Option<NullAddArgs>) -> Result<i32, UblkError> {
    let size = 250_u64 << 30;
    let user_copy = (ctrl.dev_info().flags & libublk::sys::UBLK_F_USER_COPY as u64) != 0;

    let tgt_init = |dev: &mut UblkDev| {
        dev.set_default_params(size);
        if let Some(ref o) = opt {
            o.gen_arg.apply_block_size(dev);
            o.gen_arg.apply_read_only(dev);
        }
        Ok(())
    };

    let (_shm, aa, fg) = {
        if let Some(ref o) = opt {
            (
                Some(o.gen_arg.get_shm_id()),
                o.async_await,
                o.gen_arg.foreground,
            )
        } else {
            (None, false, false)
        }
    };

    let q_handler = move |qid, dev: &_| {
        if aa {
            q_async_fn(qid, dev, user_copy)
        } else {
            q_sync_fn(qid, dev, user_copy)
        }
    };
    ctrl.run_target(tgt_init, q_handler, move |dev: &UblkCtrl| {
        crate::rublk_prep_dump_dev(_shm, fg, dev);
    })
    .unwrap();

    Ok(0)
}
