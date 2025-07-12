pub mod handler;

use crate::offload::handler::QueueHandler;
use libublk::io::UblkIOCtx;

#[derive(Clone, Copy)]
pub enum OffloadType {
    Read = 0,
    Flush = 1,
}

impl From<OffloadType> for usize {
    fn from(val: OffloadType) -> Self {
        val as usize
    }
}

pub trait OffloadTargetLogic<'a> {
    fn setup_offload_handlers(&self, handler: &mut QueueHandler<'a, Self>);

    fn handle_io(
        &self,
        handler: &mut QueueHandler<'a, Self>,
        tag: u16,
        io_ctx: &UblkIOCtx,
        buf: Option<&mut [u8]>,
    ) -> Result<i32, i32>;
}