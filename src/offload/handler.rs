use libublk::{
    helpers::IoBuf,
    io::{UblkDev, UblkIOCtx, UblkQueue},
    UblkIORes,
};
use std::sync::mpsc::{channel, Receiver, Sender};

pub const POLL_TAG: u16 = u16::MAX;
pub const NR_OFFLOAD_HANDLERS: usize = 8;

#[derive(Debug, Default)]
pub struct OffloadJob {
    pub op: u16,
    pub tag: u16,
    pub start_sector: u64,
    pub nr_sectors: u32,
    pub buf_addr: u64,
}

#[derive(Debug)]
pub struct Completion {
    pub tag: u16,
    pub result: Result<i32, i32>,
}

pub struct OffloadHandler<'a> {
    q: &'a UblkQueue<'a>,
    bufs_ptr: *mut IoBuf<u8>,
    efd: i32,
    job_tx: Sender<OffloadJob>,
    completion_rx: Receiver<Completion>,
}

impl<'a> OffloadHandler<'a> {
    pub fn new(
        q: &'a UblkQueue<'a>,
        bufs_ptr: *mut IoBuf<u8>,
        handler_idx: u32,
        job_tx: Sender<OffloadJob>,
        completion_rx: Receiver<Completion>,
        efd: i32,
    ) -> Self {
        let handler = Self {
            q,
            bufs_ptr,
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

    pub fn handle_completion(&mut self, handler_idx: u32) {
        let mut buf = [0u8; 8];
        nix::unistd::read(self.efd, &mut buf).unwrap();

        while let Ok(completion) = self.completion_rx.try_recv() {
            let result = match completion.result {
                Ok(r) => UblkIORes::Result(r),
                Err(e) => UblkIORes::Result(e),
            };
            let tag = completion.tag;
            let buf_addr = unsafe { (*self.bufs_ptr.add(tag as usize)).as_mut().as_mut_ptr() };
            self.q.complete_io_cmd(tag, buf_addr, Ok(result));
        }
        self.submit_poll_sqe(handler_idx);
    }

    pub fn send_job(&self, op: u16, tag: u16, iod: &libublk::sys::ublksrv_io_desc, buf: &mut [u8]) {
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

pub struct QueueHandler<'a, T: super::OffloadTargetLogic<'a> + ?Sized> {
    pub q: &'a UblkQueue<'a>,
    target_logic: &'a T,
    pub bufs_ptr: *mut IoBuf<u8>,
    pub max_buf_len: usize,
    pub offload_handlers: Vec<Option<OffloadHandler<'a>>>,
}

impl<'a, T: super::OffloadTargetLogic<'a>> QueueHandler<'a, T> {
    pub fn new(
        q: &'a UblkQueue<'a>,
        target_logic: &'a T,
        bufs: &'a mut [IoBuf<u8>],
        dev: &'a UblkDev,
    ) -> Self {
        let bufs_ptr = bufs.as_mut_ptr();
        let mut handlers = Vec::with_capacity(NR_OFFLOAD_HANDLERS);
        for _ in 0..NR_OFFLOAD_HANDLERS {
            handlers.push(None);
        }

        let mut s = Self {
            q,
            target_logic,
            bufs_ptr,
            max_buf_len: dev.dev_info.max_io_buf_bytes as usize,
            offload_handlers: handlers,
        };
        target_logic.setup_offload_handlers(&mut s);
        s
    }

    pub fn handle_event(&mut self, tag: u16, io_ctx: &UblkIOCtx) {
        self.target_logic.handle_io(self, tag, io_ctx).unwrap();
    }
}

pub fn setup_worker_thread<F>(efd: i32, handler: F) -> (Sender<OffloadJob>, Receiver<Completion>)
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