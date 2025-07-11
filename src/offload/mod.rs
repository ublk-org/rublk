pub mod handler;

use libublk::io::UblkQueue;
use std::sync::mpsc::{Receiver, Sender};

pub trait OffloadTargetLogic {
    fn setup_read_worker(
        &self,
        efd: i32,
    ) -> (Sender<handler::OffloadJob>, Receiver<handler::Completion>);

    fn setup_flush_worker(
        &self,
        efd: i32,
    ) -> (Sender<handler::OffloadJob>, Receiver<handler::Completion>);

    fn handle_io(
        &self,
        q: &UblkQueue,
        tag: u16,
        iod: &libublk::sys::ublksrv_io_desc,
        buf: &mut [u8],
    ) -> Result<i32, i32>;
}
