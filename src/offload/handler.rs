use libublk::{
    io::{UblkIOCtx, UblkQueue},
    UblkIORes,
};
use std::sync::mpsc::{channel, Receiver, Sender};

pub(crate) const POLL_TAG: u16 = u16::MAX;
pub(crate) const NR_OFFLOAD_HANDLERS: usize = 8;

#[derive(Debug, Default)]
pub(crate) struct OffloadJob {
    pub op: u16,
    pub tag: u16,
    pub start_sector: u64,
    pub nr_sectors: u32,
    pub buf_addr: u64,
}

#[derive(Debug)]
pub(crate) struct Completion {
    pub tag: u16,
    pub result: Result<i32, i32>,
    pub buf_addr: u64,
}

pub(crate) struct OffloadHandler {
    efd: i32,
    job_tx: Sender<OffloadJob>,
    completion_rx: Receiver<Completion>,
}

impl OffloadHandler {
    pub(crate) fn new<F>(
        worker_fn: F,
    ) -> Self
    where
        F: Fn(OffloadJob) -> Completion + Send + 'static,
    {
        let efd = nix::sys::eventfd::eventfd(0, nix::sys::eventfd::EfdFlags::EFD_CLOEXEC).unwrap();
        let (job_tx, completion_rx) = setup_worker_thread(efd, worker_fn);

        let handler = Self {
            efd,
            job_tx,
            completion_rx,
        };
        handler
    }

    pub(crate) fn submit_poll_sqe(&self, q: &UblkQueue, handler_idx: u32) {
        let user_data = libublk::io::UblkIOCtx::build_user_data(POLL_TAG, handler_idx, 0, true);
        let sqe = io_uring::opcode::PollAdd::new(io_uring::types::Fd(self.efd), libc::POLLIN as _)
            .build()
            .user_data(user_data);
        q.ublk_submit_sqe_sync(sqe).unwrap();
    }

    pub(crate) fn handle_completion(&mut self, q: &UblkQueue, handler_idx: u32) {
        let mut buf = [0u8; 8];
        nix::unistd::read(self.efd, &mut buf).unwrap();

        while let Ok(completion) = self.completion_rx.try_recv() {
            let result = match completion.result {
                Ok(r) => UblkIORes::Result(r),
                Err(e) => UblkIORes::Result(e),
            };
            let tag = completion.tag;
            q.complete_io_cmd(tag, completion.buf_addr as *mut u8, Ok(result));
        }
        self.submit_poll_sqe(q, handler_idx);
    }

    pub(crate) fn send_job(&self, op: u16, tag: u16, iod: &libublk::sys::ublksrv_io_desc, buf: &mut [u8]) {
        self.job_tx
            .send(OffloadJob {
                op,
                tag,
                start_sector: iod.start_sector,
                nr_sectors: iod.nr_sectors,
                buf_addr: buf.as_mut_ptr() as u64,
            })
            .unwrap();
    }
}

/// The `QueueHandler` serves as a generic and reusable component that orchestrates I/O handling
/// for a `UblkQueue`. Its primary purposes are:
///
/// 1.  **Abstraction**: It abstracts the complexities of I/O offloading. It manages a pool of
///     `OffloadHandler`s, each running in a separate worker thread, to process tasks
///     asynchronously. This keeps the main I/O processing loop non-blocking.
/// 2.  **Decoupling**: It decouples the generic queue management logic from the specific
///     application logic of an I/O target (like the `CompressTarget` which uses RocksDB). This is
///     achieved by using the `OffloadTargetLogic` trait.
/// 3.  **Centralization**: It centralizes the logic for handling events from the underlying
///     `UblkQueue`, interpreting them, and dispatching them to the appropriate handler—either a
///     synchronous operation or an asynchronous offload handler.
///
/// In essence, `QueueHandler` acts as the "brain" for a `ublk` queue, using the
/// `OffloadTargetLogic` trait as a pluggable "strategy" to define how different I/O operations
/// should actually be handled.
///
/// ### Is it really necessary?
///
/// Yes, this abstraction is highly beneficial and arguably necessary for a clean and maintainable
/// design. Here’s why:
///
/// 1.  **Separation of Concerns**: Without `QueueHandler`, the logic currently in `CompressTarget`
///     (specific to RocksDB) and the logic for managing worker threads, `eventfd`, and channels
///     would be entangled within the main queue processing loop (`q_sync_fn`). This would create a
///     monolithic and hard-to-maintain function.
/// 2.  **Reusability**: The `QueueHandler` and `OffloadHandler` provide a reusable framework for
///     creating new `ublk` targets. To implement a new target with its own offloading
///     requirements, you would only need to provide a new implementation of the
///     `OffloadTargetLogic` trait. The core machinery for managing the queue and worker threads
///     remains unchanged.
/// 3.  **Extensibility and Scalability**: This design, which employs the Strategy design pattern,
///     makes the system extensible. New features or even entirely new storage backends can be
///     added with minimal changes to the existing core logic. It also scales well by allowing
///     different targets to define their own set of offload handlers tailored to their specific
///     needs.
///
/// In conclusion, while it would be possible to implement the compression target without
/// `QueueHandler`, the result would be less modular, harder to test, and more difficult to extend.
/// The `QueueHandler` provides a necessary abstraction that leads to a cleaner, more robust, and
/// more reusable codebase.
pub(crate) struct QueueHandler<'a, T: super::OffloadTargetLogic<'a> + ?Sized> {
    pub q: &'a UblkQueue<'a>,
    target_logic: &'a T,
    pub offload_handlers: Vec<Option<OffloadHandler>>,
}

impl<'a, T: super::OffloadTargetLogic<'a>> QueueHandler<'a, T> {
    pub(crate) fn new(
        q: &'a UblkQueue<'a>,
        target_logic: &'a T,
    ) -> Self {
        let mut handlers = Vec::with_capacity(NR_OFFLOAD_HANDLERS);
        for _ in 0..NR_OFFLOAD_HANDLERS {
            handlers.push(None);
        }

        let mut s = Self {
            q,
            target_logic,
            offload_handlers: handlers,
        };
        target_logic.setup_offload_handlers(&mut s);

        for (idx, h) in s.offload_handlers.iter().enumerate() {
            if h.is_some() {
                h.as_ref().unwrap().submit_poll_sqe(s.q, idx as u32);
            }
        }
        s
    }

    pub(crate) fn handle_event(&mut self, tag: u16, io_ctx: &UblkIOCtx, buf: Option<&mut [u8]>) {
        self.target_logic.handle_io(self, tag, io_ctx, buf).unwrap();
    }
}

fn setup_worker_thread<F>(efd: i32, handler: F) -> (Sender<OffloadJob>, Receiver<Completion>)
where
    F: Fn(OffloadJob) -> Completion + Send + 'static,
{
    let (job_tx, job_rx) = channel();
    let (completion_tx, completion_rx) = channel();

    std::thread::spawn(move || {
        for job in job_rx {
            let completion = handler(job);
            if completion_tx.send(completion).is_err() {
                break;
            }
            nix::unistd::write(efd, &1u64.to_le_bytes()).unwrap();
        }
    });

    (job_tx, completion_rx)
}
