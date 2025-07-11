pub mod handler;

use crate::offload::handler::QueueHandler;
use libublk::io::UblkIOCtx;

pub trait OffloadTargetLogic<'a> {
    fn setup_offload_handlers(&self, handler: &mut QueueHandler<'a, Self>);

    fn handle_io(
        &self,
        handler: &mut QueueHandler<'a, Self>,
        tag: u16,
        io_ctx: &UblkIOCtx,
    ) -> Result<i32, i32>;
}
