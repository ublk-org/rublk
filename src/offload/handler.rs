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

pub(crate) struct OffloadHandler<'a> {
    q: &'a UblkQueue<'a>,
    efd: i32,
    job_tx: Sender<OffloadJob>,
    completion_rx: Receiver<Completion>,
}

impl<'a> OffloadHandler<'a> {
    pub(crate) fn new(
        q: &'a UblkQueue<'a>,
        handler_idx: u32,
        job_tx: Sender<OffloadJob>,
        completion_rx: Receiver<Completion>,
        efd: i32,
    ) -> Self {
        let handler = Self {
            q,
            efd,
            job_tx,
            completion_rx,
        };
        handler.submit_poll_sqe(handler_idx);
        handler
    }

    fn submit_poll_sqe(&self, handler_idx: u32) {
        let user_data = libublk::io::UblkIOCtx::build_user_data(POLL_TAG, handler_idx, 0, true);
        let sqe = io_uring::opcode::PollAdd::new(io_uring::types::Fd(self.efd), libc::POLLIN as _)
            .build()
            .user_data(user_data);
        self.q.ublk_submit_sqe_sync(sqe).unwrap();
    }

    pub(crate) fn handle_completion(&mut self, handler_idx: u32) {
        let mut buf = [0u8; 8];
        nix::unistd::read(self.efd, &mut buf).unwrap();

        while let Ok(completion) = self.completion_rx.try_recv() {
            let result = match completion.result {
                Ok(r) => UblkIORes::Result(r),
                Err(e) => UblkIORes::Result(e),
            };
            let tag = completion.tag;
            self.q
                .complete_io_cmd(tag, completion.buf_addr as *mut u8, Ok(result));
        }
        self.submit_poll_sqe(handler_idx);
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

pub(crate) struct QueueHandler<'a, T: super::OffloadTargetLogic<'a> + ?Sized> {
    pub q: &'a UblkQueue<'a>,
    target_logic: &'a T,
    pub offload_handlers: Vec<Option<OffloadHandler<'a>>>,
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
        s
    }

    pub(crate) fn handle_event(&mut self, tag: u16, io_ctx: &UblkIOCtx, buf: Option<&mut [u8]>) {
        self.target_logic.handle_io(self, tag, io_ctx, buf).unwrap();
    }
}

pub(crate) fn setup_worker_thread<F>(efd: i32, handler: F) -> (Sender<OffloadJob>, Receiver<Completion>)
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
