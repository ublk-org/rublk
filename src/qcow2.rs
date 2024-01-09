use crate::uring::{
    ublk_get_uring_io_seq, ublk_run_task, ublk_submit_io_cmd, ublk_submit_sqe, ublk_wake_task,
};
#[rustversion::before(1.75)]
use async_trait::async_trait;
use io_uring::{opcode, types};
use libublk::ctrl::{UblkCtrl, UblkQueueAffinity};
use libublk::io::{UblkDev, UblkQueue};
use libublk::{UblkError, UblkSession};
use qcow2_rs::dev::{Qcow2Dev, Qcow2DevParams, Qcow2IoOps};
use qcow2_rs::error::Qcow2Result;
use qcow2_rs::utils::qcow2_alloc_dev_sync;
use serde::{Deserialize, Serialize};
use std::cell::{RefCell, UnsafeCell};
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::mpsc;

#[derive(clap::Args, Debug)]
pub struct Qcow2Args {
    #[command(flatten)]
    pub gen_arg: super::args::GenAddArgs,

    /// backing file of ublk target
    #[clap(long, short = 'f')]
    pub file: PathBuf,

    /// buffered io is applied for backing file of ublk target, default is direct IO
    #[clap(long, default_value_t = false)]
    pub buffered_io: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct Qcow2Json {
    back_file_path: String,
    direct_io: i32,
}

pub struct Qcow2Tgt<T> {
    pub back_file_path: String,
    pub direct_io: i32,

    queue_is_down: RefCell<bool>,
    qdev: Qcow2Dev<T>,
}

std::thread_local! {
    static MY_THREAD_QUEUE: UnsafeCell<*const ()> = UnsafeCell::new(std::ptr::null());
}

#[inline]
fn set_thread_local_queue(q: *const ()) {
    MY_THREAD_QUEUE.with(|cell| unsafe {
        *cell.get() = q;
    });
}
#[inline]
fn get_thread_local_queue() -> *const UblkQueue<'static> {
    MY_THREAD_QUEUE.with(|cell| unsafe {
        let a = *cell.get();
        a as *const UblkQueue
    })
}

#[derive(Debug)]
pub struct UblkQcow2Io {
    _file: std::fs::File,
    fd: i32,
}

qcow2_rs::qcow2_setup_dev_fn_sync!(UblkQcow2Io, ulbk_qcow2_setup_dev);

#[allow(dead_code)]
impl UblkQcow2Io {
    pub fn new(path: &PathBuf, ro: bool, dio: bool) -> UblkQcow2Io {
        log::trace!(
            "qcow2: setup ublk qcow2 IO path {:?} readonly {} direct io {}",
            path,
            ro,
            dio
        );
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(!ro)
            .open(path.clone())
            .unwrap();
        let fd = file.as_raw_fd();

        if dio {
            unsafe {
                libc::fcntl(file.as_raw_fd(), libc::F_SETFL, libc::O_DIRECT);
            }
        }
        UblkQcow2Io { _file: file, fd }
    }
}

#[rustversion::attr(before(1.75), async_trait(?Send))]
impl Qcow2IoOps for UblkQcow2Io {
    async fn read_to(&self, offset: u64, buf: &mut [u8]) -> Qcow2Result<usize> {
        let seq = ublk_get_uring_io_seq();
        let qp = get_thread_local_queue();
        let q = unsafe { &*qp };
        let fd = types::Fd(self.fd);
        //let user_data = UblkIOCtx::build_user_data_async(tag as u16, op, seq);
        let user_data = seq | (1 << 63);

        log::debug!(
            "qcow2_read: {} offset {:x} len {} key {:x}",
            seq,
            offset,
            buf.len(),
            user_data
        );
        loop {
            let sqe = &opcode::Read::new(fd, buf.as_mut_ptr(), buf.len() as u32)
                .offset(offset)
                .build()
                .user_data(user_data);

            let res = ublk_submit_sqe(q, sqe, user_data).await;
            if res >= 0 {
                return Ok(res as usize);
            } else if res == -libc::EAGAIN {
                continue;
            } else {
                return Err("uring read failed".into());
            }
        }
    }

    async fn write_from(&self, offset: u64, buf: &[u8]) -> Qcow2Result<()> {
        let seq = ublk_get_uring_io_seq();
        let qp = get_thread_local_queue();
        let q = unsafe { &*qp };
        let fd = types::Fd(self.fd);
        //let user_data = UblkIOCtx::build_user_data_async(tag as u16, op, seq);
        let user_data = seq | (1 << 63);

        log::debug!(
            "qcow2_write: {} offset {:x} len {} key {:x}",
            seq,
            offset,
            buf.len(),
            user_data
        );
        loop {
            let sqe = &opcode::Write::new(fd, buf.as_ptr(), buf.len() as u32)
                .offset(offset)
                .build()
                .user_data(user_data);
            let res = ublk_submit_sqe(q, sqe, user_data).await;
            if res >= 0 {
                return Ok(());
            } else if res == -libc::EAGAIN {
                log::debug!("qcow2_write: -EAGAIN");
                continue;
            } else {
                return Err("uring write failed".into());
            }
        }
    }

    async fn discard_range(&self, offset: u64, len: usize, _flags: i32) -> Qcow2Result<()> {
        let seq = ublk_get_uring_io_seq();
        let qp = get_thread_local_queue();
        let q = unsafe { &*qp };
        let fd = types::Fd(self.fd);
        let user_data = seq | (1 << 63);

        log::debug!(
            "qcow2 discard: {} offset {:x} len {} key {:x}",
            seq,
            offset,
            len,
            user_data
        );
        loop {
            let sqe = &opcode::Fallocate::new(fd, len as u64)
                .offset(offset)
                .build()
                .user_data(user_data);
            let res = ublk_submit_sqe(q, sqe, user_data).await;
            if res >= 0 {
                return Ok(());
            } else if res == -libc::EAGAIN {
                log::debug!("qcow2_discard: -EAGAIN");
                continue;
            } else {
                return Err("uring discard failed".into());
            }
        }
    }

    async fn fsync(&self, offset: u64, len: usize, _flags: i32) -> Qcow2Result<()> {
        let seq = ublk_get_uring_io_seq();
        let qp = get_thread_local_queue();
        let q = unsafe { &*qp };
        let fd = types::Fd(self.fd);
        let user_data = seq | (1 << 63);

        log::debug!(
            "qcow2 fsync: {} offset {:x} len {} key {:x}",
            seq,
            offset,
            len,
            user_data
        );
        loop {
            let sqe = &opcode::SyncFileRange::new(fd, len as u32)
                .offset(offset)
                .build()
                .user_data(user_data);
            let res = ublk_submit_sqe(q, sqe, user_data).await;
            if res >= 0 {
                return Ok(());
            } else if res == -libc::EAGAIN {
                log::debug!("qcow2_discard: -EAGAIN");
                continue;
            } else {
                return Err("uring discard failed".into());
            }
        }
    }
}

async fn qcow2_handle_io_cmd_async<T: Qcow2IoOps>(
    q: &UblkQueue<'_>,
    qdev: &Qcow2Dev<T>,
    tag: u16,
    buf: &mut [u8],
) -> i32 {
    let iod = q.get_iod(tag);
    let op = iod.op_flags & 0xff;
    let off = (iod.start_sector << 9) as u64;
    let bytes = (iod.nr_sectors << 9) as usize;

    log::debug!("ublk_io: {} op {} offset {:x} len {}", tag, op, off, bytes);
    let res = match op {
        libublk::sys::UBLK_IO_OP_FLUSH => {
            qdev.fsync_range(0, qdev.info.virtual_size() as usize)
                .await
                .unwrap();
            qdev.flush_meta().await.unwrap();
            0
        }
        libublk::sys::UBLK_IO_OP_READ => {
            let res = qdev.read_at(&mut buf[..bytes], off).await.unwrap();
            res as i32
        }
        libublk::sys::UBLK_IO_OP_WRITE => {
            qdev.write_at(&buf[..bytes], off).await.unwrap();
            bytes as i32
        }
        _ => -libc::EINVAL,
    };

    res
}

fn qcow2_init_tgt<T: Qcow2IoOps>(
    dev: &mut UblkDev,
    qcow2: &Qcow2Tgt<T>,
    opt: Option<Qcow2Args>,
    size: u64,
) -> Result<i32, UblkError> {
    log::info!("qcow2: init_tgt {}", dev.dev_info.dev_id);
    let info = dev.dev_info;

    let depth = info.queue_depth;
    let tgt = &mut dev.tgt;
    tgt.extra_ios = 1;
    tgt.sq_depth = depth * 4;
    tgt.cq_depth = depth * 4;

    let file = std::fs::OpenOptions::new()
        .read(true)
        .open(&qcow2.back_file_path)
        .unwrap();
    let sz = crate::ublk_file_size(&file).unwrap();

    tgt.dev_size = size;
    //todo: figure out correct block size
    tgt.params = libublk::sys::ublk_params {
        types: libublk::sys::UBLK_PARAM_TYPE_BASIC,
        basic: libublk::sys::ublk_param_basic {
            attrs: libublk::sys::UBLK_ATTR_VOLATILE_CACHE,
            logical_bs_shift: sz.1,
            physical_bs_shift: sz.2,
            io_opt_shift: sz.2,
            io_min_shift: sz.1,
            max_sectors: info.max_io_buf_bytes >> 9,
            dev_sectors: tgt.dev_size >> 9,
            ..Default::default()
        },
        ..Default::default()
    };

    if let Some(o) = opt {
        o.gen_arg.apply_read_only(dev);
        o.gen_arg.apply_block_size(dev);
    }

    let val = serde_json::json!({"qcow2": Qcow2Json { back_file_path: qcow2.back_file_path.clone(), direct_io: qcow2.direct_io } });
    dev.set_target_json(val);

    Ok(0)
}

fn to_absolute_path(p: PathBuf, parent: Option<PathBuf>) -> PathBuf {
    if p.is_absolute() {
        p
    } else {
        match parent {
            None => p,
            Some(n) => n.join(p),
        }
    }
}

async fn ublk_qcow2_io_fn<T: Qcow2IoOps>(tgt: &Qcow2Tgt<T>, q: &UblkQueue<'_>, tag: u16) {
    let qdev_q = &tgt.qdev;
    let buf_addr = q.get_io_buf_addr(tag);
    let buf_len = q.dev.dev_info.max_io_buf_bytes as usize;
    let mut buf = unsafe { Vec::from_raw_parts(buf_addr, buf_len, buf_len) };
    let mut cmd_op = libublk::sys::UBLK_IO_FETCH_REQ;
    let mut res = 0;

    log::debug!("qcow2: io task {} stated", tag);
    loop {
        let cmd_res = ublk_submit_io_cmd(&q, tag, cmd_op, buf_addr, res).await;
        if cmd_res == libublk::sys::UBLK_IO_RES_ABORT {
            break;
        }

        res = qcow2_handle_io_cmd_async(&q, &qdev_q, tag, &mut buf).await;
        cmd_op = libublk::sys::UBLK_IO_COMMIT_AND_FETCH_REQ;
    }
    std::mem::forget(buf);
}

pub fn ublk_add_qcow2(
    sess: UblkSession,
    id: i32,
    opt: Option<Qcow2Args>,
) -> Result<i32, UblkError> {
    let (file, dio) = match opt {
        Some(ref o) => {
            let parent = o.gen_arg.get_start_dir();

            (to_absolute_path(o.file.clone(), parent), !o.buffered_io)
        }
        None => {
            let ctrl = UblkCtrl::new_simple(id, 0)?;
            match ctrl.get_target_data_from_json() {
                Some(val) => {
                    let lo = &val["qcow2"];
                    let tgt_data: Result<Qcow2Json, _> = serde_json::from_value(lo.clone());

                    match tgt_data {
                        Ok(t) => (PathBuf::from(t.back_file_path.as_str()), t.direct_io != 0),
                        Err(_) => return Err(UblkError::OtherError(-libc::EINVAL)),
                    }
                }
                None => return Err(UblkError::OtherError(-libc::EINVAL)),
            }
        }
    };

    let file_path = format!("{}", file.as_path().display());
    log::info!("qcow2: add: path {}", &file_path);
    let p = qcow2_rs::qcow2_default_params!(false, dio);
    let qdev = ulbk_qcow2_setup_dev(&file, &p).unwrap();
    let dev_size = qdev.info.virtual_size();
    let lo = Qcow2Tgt {
        direct_io: i32::from(dio),
        back_file_path: file_path,
        qdev,
        queue_is_down: RefCell::new(false),
    };

    let _shm = {
        if let Some(ref o) = opt {
            Some(o.gen_arg.get_shm_id())
        } else {
            None
        }
    };

    let tgt_init = |dev: &mut UblkDev| qcow2_init_tgt(dev, &lo, opt, dev_size);
    let (mut ctrl, dev) = sess.create_devices(tgt_init).unwrap();

    //todo: USER_COPY should be the default option
    if (ctrl.dev_info.flags & (libublk::sys::UBLK_F_USER_COPY as u64)) != 0 {
        return Err(UblkError::OtherError(-libc::EINVAL));
    }

    let mut affinity = UblkQueueAffinity::new();
    ctrl.get_queue_affinity(0, &mut affinity).unwrap();

    let (tx, rx) = mpsc::channel();
    let this_dev = dev.clone();
    let _tx = tx.clone();
    let depth = dev.dev_info.queue_depth;
    let qh = std::thread::spawn(move || {
        let q_rc = Rc::new(UblkQueue::new(0, &this_dev).unwrap());
        let tgt_rc = Rc::new(&lo);
        let q = q_rc.clone();
        let qp = &*q as *const UblkQueue;
        set_thread_local_queue(qp as *const ());

        unsafe {
            libc::pthread_setaffinity_np(
                libc::pthread_self(),
                affinity.buf_len(),
                affinity.addr() as *const libc::cpu_set_t,
            );
        }
        _tx.send(unsafe { libc::gettid() }).unwrap();

        let mut f_vec = Vec::new();
        let exe_rc = Rc::new(smol::LocalExecutor::new());
        let exe = exe_rc.clone();

        //prepare for handling IO
        let tgt = tgt_rc.clone();
        let task = exe.spawn(async move { tgt.qdev.qcow2_prep_io().await.unwrap() });
        ublk_run_task(&q_rc, &exe, &task, 1);

        for tag in 0..depth as u16 {
            let q = q_rc.clone();
            let tgt = tgt_rc.clone();

            f_vec.push(exe.spawn(async move {
                let t = &tgt;
                let qp = &q;
                ublk_qcow2_io_fn(t, qp, tag).await;
            }));
        }

        let q = q_rc.clone();
        let tgt = tgt_rc.clone();
        let flush_task = exe.spawn(async move {
            let t = &tgt;
            while *(t.queue_is_down.borrow()) == false {
                if tgt.qdev.need_flush_meta() {
                    tgt.qdev.flush_meta().await.unwrap();
                }
                smol::Timer::after(std::time::Duration::from_millis(50)).await;
            }
        });
        //start all io tasks
        while exe_rc.try_tick() {}

        loop {
            log::debug!("submit sqes & waiting for cqe completion");
            match q.flush_and_wake_io_tasks(|data, cqe, _| ublk_wake_task(data, cqe), 1) {
                Err(_) => break,
                _ => {}
            }

            // run io tasks
            while exe_rc.try_tick() {}
        }

        *(tgt_rc.queue_is_down.borrow_mut()) = true;
        ublk_run_task(&q_rc, &exe, &flush_task, 0);

        // flushing meta final time
        let tgt = tgt_rc.clone();
        let task = exe.spawn(async move {
            tgt.qdev.flush_meta().await.unwrap();
            // check if everything is fine
            tgt_rc.qdev.check().await.unwrap();
        });
        ublk_run_task(&q_rc, &exe, &task, 1);

        log::info!("qcow2: wait on io tasks");
        smol::block_on(async { futures::future::join_all(f_vec).await });
    });

    let dev_id = dev.dev_info.dev_id as i32;
    let tid = rx.recv().unwrap();
    if ctrl.configure_queue(&dev, 0, tid).is_err() {
        println!("qcow2: configure queue failed for {}-{}", dev_id, 0);
    }

    ctrl.start_dev(&dev)?;
    if let Some(shm) = _shm {
        crate::rublk_write_id_into_shm(&shm, dev_id as i32);
    }

    qh.join()
        .unwrap_or_else(|_| eprintln!("dev-{} join queue thread failed", dev_id));
    log::info!("queue thread is done");

    let _ = ctrl.stop_dev(&dev);

    Ok(0)
}