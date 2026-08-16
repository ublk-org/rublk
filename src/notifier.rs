use libublk::{ops, UblkError};
use nix::sys::eventfd::{EfdFlags, EventFd};
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicU32, Ordering};

/// Coalescing eventfd notifier, safe to signal from any thread.
///
/// Its job is to make a CQE arrive on the queue ring whenever another
/// thread completes offloaded work: the queue thread parks inside
/// `io_uring_enter`, so a plain task wakeup cannot reach it.
pub(crate) struct Notifier {
    eventfd: EventFd,
    counter: AtomicU32,
}

impl Notifier {
    pub fn new() -> Result<Self, std::io::Error> {
        let eventfd = EventFd::from_value_and_flags(0, EfdFlags::EFD_CLOEXEC)?;
        Ok(Notifier {
            eventfd,
            counter: AtomicU32::new(0),
        })
    }

    pub fn notify(&self) -> anyhow::Result<()> {
        if self.counter.fetch_add(1, Ordering::AcqRel) == 0 {
            nix::unistd::write(&self.eventfd, &1u64.to_le_bytes())?;
        }
        Ok(())
    }

    pub async fn event_read(&self) -> Result<(), UblkError> {
        let mut buf = [0u8; 8];
        let eventfd = self.eventfd.as_raw_fd();

        log::debug!("before eventfd reading");
        // SAFETY: `buf` outlives the await below
        let res =
            unsafe { ops::read_at_raw(ops::TgtFd::Raw(eventfd), buf.as_mut_ptr(), 8, 0) }?.await;

        // Notifications arriving before this swap are covered by the wakeup
        // being handled right now; any later notify() sees 0 and writes the
        // eventfd again, so no wakeup is ever lost.
        self.counter.swap(0, Ordering::AcqRel);

        if res < 8 {
            Err(UblkError::OtherError(-libc::EIO))
        } else {
            Ok(())
        }
    }
}
