use libublk::{
    helpers::IoBuf,
    io::{UblkDev, UblkQueue},
    UblkIORes,
};
use nix::sys::eventfd::{EfdFlags};
use std::sync::mpsc::{channel, Receiver, Sender};

pub const POLL_TAG: u16 = u16::MAX;

#[derive(Debug)]
pub struct ReadJob {
    pub tag: u16,
    pub start_sector: u64,
    pub nr_sectors: u32,
    pub buf_addr: u64,
}

#[derive(Debug)]
pub struct FlushJob {
    pub tag: u16,
}

#[derive(Debug)]
pub struct Completion {
    pub tag: u16,
    pub result: Result<i32, i32>,
}

pub struct OffloadHandler<'a, Job> {
    q: &'a UblkQueue<'a>,
    bufs_ptr: *mut IoBuf<u8>,
    efd: i32,
    job_tx: Sender<Job>,
    completion_rx: Receiver<Completion>,
    poll_op: u32,
}

impl<'a, Job> OffloadHandler<'a, Job> {
    pub fn new(
        q: &'a UblkQueue<'a>,
        bufs_ptr: *mut IoBuf<u8>,
        efd: i32,
        job_tx: Sender<Job>,
        completion_rx: Receiver<Completion>,
        poll_op: u32,
    ) -> Self {
        let handler = Self {
            q,
            bufs_ptr,
            efd,
            job_tx,
            completion_rx,
            poll_op,
        };
        handler.submit_poll_sqe();
        handler
    }

    fn submit_poll_sqe(&self) {
        let user_data = libublk::io::UblkIOCtx::build_user_data(POLL_TAG, self.poll_op, 0, true);
        let sqe = io_uring::opcode::PollAdd::new(io_uring::types::Fd(self.efd), libc::POLLIN as _)
            .build()
            .user_data(user_data);
        self.q.ublk_submit_sqe_sync(sqe).unwrap();
    }

    pub fn handle_completion(&self) {
        let mut buf = [0u8; 8];
        nix::unistd::read(self.efd, &mut buf).unwrap();

        while let Ok(completion) = self.completion_rx.try_recv() {
            let result = match completion.result {
                Ok(r) => UblkIORes::Result(r),
                Err(e) => UblkIORes::Result(e),
            };
            let tag = completion.tag;
            let buf_addr = unsafe {
                let io_buf: &mut IoBuf<u8> = &mut *self.bufs_ptr.add(tag as usize);
                io_buf.as_mut().as_mut_ptr()
            };
            self.q.complete_io_cmd(tag, buf_addr, Ok(result));
        }
        self.submit_poll_sqe();
    }
}

pub fn send_read_job(handler: &OffloadHandler<ReadJob>, tag: u16, iod: &libublk::sys::ublksrv_io_desc, buf: &mut [u8]) {
    handler.job_tx
        .send(ReadJob {
            tag,
            start_sector: iod.start_sector,
            nr_sectors: iod.nr_sectors,
            buf_addr: buf.as_mut_ptr() as u64,
        })
        .unwrap();
}

pub fn send_flush_job(handler: &OffloadHandler<FlushJob>, tag: u16) {
    handler.job_tx.send(FlushJob { tag }).unwrap();
}

pub struct QueueHandler<'a, T: super::OffloadTargetLogic> {
    q: &'a UblkQueue<'a>,
    target_logic: &'a T,
    bufs_ptr: *mut IoBuf<u8>,
    max_buf_len: usize,
    read_handler: OffloadHandler<'a, ReadJob>,
    flush_handler: OffloadHandler<'a, FlushJob>,
}

impl<'a, T: super::OffloadTargetLogic> QueueHandler<'a, T> {
    pub fn new(q: &'a UblkQueue<'a>, target_logic: &'a T, bufs: &'a mut [IoBuf<u8>], dev: &'a UblkDev) -> Self {
        let flush_efd = nix::sys::eventfd::eventfd(0, EfdFlags::EFD_CLOEXEC).unwrap();
        let read_efd = nix::sys::eventfd::eventfd(0, EfdFlags::EFD_CLOEXEC).unwrap();
        let bufs_ptr = bufs.as_mut_ptr();

        let (read_job_tx, read_completion_rx) = target_logic.setup_read_worker(read_efd);
        let (flush_job_tx, flush_completion_rx) = target_logic.setup_flush_worker(flush_efd);

        let read_handler = OffloadHandler::new(
            q,
            bufs_ptr,
            read_efd,
            read_job_tx,
            read_completion_rx,
            libublk::sys::UBLK_IO_OP_READ,
        );

        let flush_handler = OffloadHandler::new(
            q,
            bufs_ptr,
            flush_efd,
            flush_job_tx,
            flush_completion_rx,
            libublk::sys::UBLK_IO_OP_FLUSH,
        );

        Self {
            q,
            target_logic,
            bufs_ptr,
            max_buf_len: dev.dev_info.max_io_buf_bytes as usize,
            read_handler,
            flush_handler,
        }
    }

    pub fn handle_io(&self, tag: u16, io_ctx: &libublk::io::UblkIOCtx) {
        if io_ctx.is_tgt_io() {
            match libublk::io::UblkIOCtx::user_data_to_op(io_ctx.user_data()) {
                libublk::sys::UBLK_IO_OP_FLUSH => {
                    self.flush_handler.handle_completion();
                    return;
                }
                libublk::sys::UBLK_IO_OP_READ => {
                    self.read_handler.handle_completion();
                    return;
                }
                _ => {}
            }
        }

        let iod = self.q.get_iod(tag);
        let op = iod.op_flags & 0xff;

        let buf = unsafe {
            let buf_len = std::cmp::min((iod.nr_sectors << 9) as usize, self.max_buf_len);
            let io_buf: &mut IoBuf<u8> = &mut *self.bufs_ptr.add(tag as usize);
            &mut io_buf.as_mut()[..buf_len]
        };

        let res = match op {
            libublk::sys::UBLK_IO_OP_READ => {
                send_read_job(&self.read_handler, tag, iod, buf);
                return;
            }
            libublk::sys::UBLK_IO_OP_FLUSH => {
                send_flush_job(&self.flush_handler, tag);
                return;
            }
            _ => self.target_logic.handle_io(self.q, tag, iod, buf),
        };

        let result = match res {
            Ok(r) => UblkIORes::Result(r),
            Err(e) => UblkIORes::Result(e),
        };
        self.q.complete_io_cmd(tag, buf.as_mut_ptr(), Ok(result));
    }
}

pub fn setup_worker_thread<Job, C, F>(efd: i32, handler: F) -> (Sender<Job>, Receiver<C>)
where
    Job: Send + 'static,
    C: Send + 'static,
    F: Fn(Job) -> C + Send + 'static,
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
