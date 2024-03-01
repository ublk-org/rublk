use libublk::ctrl::UblkCtrl;
use libublk::io::{UblkDev, UblkIOCtx, UblkQueue};
use libublk::{UblkError, UblkIORes};
use std::rc::Rc;

#[derive(clap::Args, Debug)]
pub struct NullAddArgs {
    #[command(flatten)]
    pub gen_arg: super::args::GenAddArgs,
}

#[inline]
fn get_io_cmd_result(q: &UblkQueue, tag: u16) -> i32 {
    let iod = q.get_iod(tag);
    let bytes = (iod.nr_sectors << 9) as i32;

    bytes
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

pub fn ublk_add_null(ctrl: UblkCtrl, _id: i32, opt: Option<NullAddArgs>) -> Result<i32, UblkError> {
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
    let _shm = {
        if let Some(ref o) = opt {
            Some(o.gen_arg.get_shm_id())
        } else {
            None
        }
    };

    let q_handler = move |qid, dev: &_| q_sync_fn(qid, dev, user_copy);
    ctrl.run_target(tgt_init, q_handler, |dev: &UblkCtrl| {
        if let Some(shm) = _shm {
            crate::rublk_write_id_into_shm(&shm, dev.dev_info().dev_id);
        }
    })
    .unwrap();

    Ok(0)
}
