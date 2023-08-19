use libublk::ctrl::UblkCtrl;
use libublk::io::{UblkDev, UblkIOCtx, UblkQueueCtx};
use libublk::UblkError;

pub fn ublk_add_null(opt: super::args::DefAddArgs) {
    let sess = libublk::UblkSessionBuilder::default()
        .name("null")
        .depth(opt.depth)
        .nr_queues(opt.queue)
        .id(opt.number)
        .build()
        .unwrap();
    let tgt_init = |dev: &mut UblkDev| {
        dev.set_default_params(250_u64 << 30);
        Ok(serde_json::json!({}))
    };
    let wh = {
        let (mut ctrl, dev) = sess.create_devices(tgt_init).unwrap();
        let handle_io = move |ctx: &UblkQueueCtx, io: &mut UblkIOCtx| -> Result<i32, UblkError> {
            let iod = ctx.get_iod(io.get_tag());
            io.complete_io(unsafe { (*iod).nr_sectors << 9 } as i32);
            Ok(0)
        };

        sess.run(&mut ctrl, &dev, handle_io, |dev_id| {
            let mut d_ctrl = UblkCtrl::new(dev_id, 0, 0, 0, 0, false).unwrap();
            d_ctrl.dump();
        })
        .unwrap()
    };
    wh.join().unwrap();
}
