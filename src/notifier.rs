use libublk::{io::UblkQueue, UblkError};
use nix::sys::eventfd::{EfdFlags, EventFd};
use std::cell::RefCell;
use std::os::fd::AsRawFd;

pub(crate) struct Notifier {
    eventfd: EventFd,
    counter: RefCell<u32>,
}

impl Notifier {
    pub fn new() -> Result<Self, std::io::Error> {
        let eventfd = EventFd::from_value_and_flags(0, EfdFlags::EFD_CLOEXEC)?;
        Ok(Notifier {
            eventfd,
            counter: RefCell::new(0),
        })
    }

    pub fn notify(&self) -> anyhow::Result<()> {
        let old_value = self.counter.replace_with(|&mut old| old + 1);
        if old_value == 0 {
            nix::unistd::write(&self.eventfd, &1u64.to_le_bytes())?;
        }
        Ok(())
    }

    pub async fn event_read(&self, q: &UblkQueue<'_>) -> Result<(), UblkError> {
        let mut buf = [0u8; 8];
        let eventfd = self.eventfd.as_raw_fd();
        let sqe =
            io_uring::opcode::Read::new(io_uring::types::Fd(eventfd), buf.as_mut_ptr(), 8).build();
        log::debug!("before eventfd reading");
        let res = q.ublk_submit_sqe(sqe).await;

        self.counter.replace(0);

        if res < 8 {
            Err(UblkError::OtherError(-libc::EIO))
        } else {
            Ok(())
        }
    }
}
