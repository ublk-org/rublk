use libublk::ctrl::UblkCtrl;
use libublk::io::{UblkDev, UblkIOCtx, UblkQueue};
use libublk::{UblkError, UblkIORes, UblkSession};

#[derive(clap::Args, Debug)]
pub struct NullAddArgs {
    #[command(flatten)]
    pub gen_arg: super::args::GenAddArgs,
}

pub fn ublk_add_null(
    sess: UblkSession,
    id: i32,
    _opt: Option<NullAddArgs>,
) -> Result<i32, UblkError> {
    let size = match _opt {
        Some(_) => 250_u64 << 30,
        None => {
            let ctrl = UblkCtrl::new_simple(id, 0)?;
            match ctrl.get_target_from_json() {
                Ok(tgt) => tgt.dev_size,
                _ => return Err(UblkError::OtherError(-libc::EINVAL)),
            }
        }
    };

    let tgt_init = |dev: &mut UblkDev| {
        dev.set_default_params(size);
        Ok(serde_json::json!({}))
    };
    let wh = {
        let (mut ctrl, dev) = sess.create_devices(tgt_init).unwrap();
        let handle_io = move |q: &UblkQueue, tag: u16, _io: &UblkIOCtx| {
            let iod = q.get_iod(tag);
            let res = Ok(UblkIORes::Result(unsafe { (*iod).nr_sectors << 9 } as i32));
            q.complete_io_cmd(tag, res);
        };

        sess.run(&mut ctrl, &dev, handle_io, |dev_id| {
            let mut d_ctrl = UblkCtrl::new_simple(dev_id, 0).unwrap();
            d_ctrl.dump();
        })
        .unwrap()
    };
    wh.join().unwrap();

    Ok(0)
}
