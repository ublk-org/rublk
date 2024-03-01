#[rustversion::before(1.75)]
use async_trait::async_trait;
use io_uring::{opcode, types};
use libublk::ctrl::UblkCtrl;
use libublk::helpers::IoBuf;
use libublk::io::{UblkDev, UblkQueue};
use libublk::uring_async::{ublk_run_ctrl_task, ublk_run_io_task, ublk_wait_and_handle_ios};
use libublk::UblkError;
use qcow2_rs::dev::{Qcow2Dev, Qcow2DevParams};
use qcow2_rs::error::Qcow2Result;
use qcow2_rs::ops::*;
use qcow2_rs::utils::qcow2_alloc_dev_sync;
use serde::{Deserialize, Serialize};
use std::cell::{RefCell, UnsafeCell};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::rc::Rc;

#[derive(clap::Args, Debug)]
pub(crate) struct Qcow2Args {
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

struct Qcow2Tgt<T> {
    back_file_path: String,
    direct_io: i32,

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
struct UblkQcow2Io {
    _file: std::fs::File,
    fd: i32,
}

qcow2_rs::qcow2_setup_dev_fn_sync!(UblkQcow2Io, ulbk_qcow2_setup_dev);

#[allow(dead_code)]
impl UblkQcow2Io {
    fn new(path: &Path, ro: bool, dio: bool) -> UblkQcow2Io {
        log::trace!(
            "qcow2: setup ublk qcow2 IO path {:?} readonly {} direct io {}",
            path,
            ro,
            dio
        );
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(!ro)
            .open(path)
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
        let qp = get_thread_local_queue();
        let q = unsafe { &*qp };
        let fd = types::Fd(self.fd);
        //let user_data = UblkIOCtx::build_user_data_async(tag as u16, op, seq);

        log::debug!("qcow2_read: offset {:x} len {}", offset, buf.len(),);
        loop {
            let sqe = opcode::Read::new(fd, buf.as_mut_ptr(), buf.len() as u32)
                .offset(offset)
                .build();

            let res = q.ublk_submit_sqe(sqe).await;
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
        let qp = get_thread_local_queue();
        let q = unsafe { &*qp };
        let fd = types::Fd(self.fd);
        //let user_data = UblkIOCtx::build_user_data_async(tag as u16, op, seq);

        log::debug!("qcow2_write: offset {:x} len {}", offset, buf.len(),);
        loop {
            let sqe = opcode::Write::new(fd, buf.as_ptr(), buf.len() as u32)
                .offset(offset)
                .build();
            let res = q.ublk_submit_sqe(sqe).await;
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

    async fn fallocate(&self, offset: u64, len: usize, flags: u32) -> Qcow2Result<()> {
        let qp = get_thread_local_queue();
        let q = unsafe { &*qp };
        let fd = types::Fd(self.fd);
        let mode = if (flags & Qcow2OpsFlags::FALLOCATE_ZERO_RAGE) != 0 {
            0x10 //ZERO_RANGE include/uapi/linux/falloc.h
        } else {
            0
        };

        log::debug!("qcow2 discard: offset {:x} len {}", offset, len);
        loop {
            let sqe = opcode::Fallocate::new(fd, len as u64)
                .offset(offset)
                .mode(mode)
                .build();
            let res = q.ublk_submit_sqe(sqe).await;
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

    async fn fsync(&self, offset: u64, len: usize, _flags: u32) -> Qcow2Result<()> {
        let qp = get_thread_local_queue();
        let q = unsafe { &*qp };
        let fd = types::Fd(self.fd);

        log::debug!("qcow2 fsync: offset {:x} len {}", offset, len,);
        loop {
            let sqe = opcode::SyncFileRange::new(fd, len as u32)
                .offset(offset)
                .build();
            let res = q.ublk_submit_sqe(sqe).await;
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
) -> Result<(), UblkError> {
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

    Ok(())
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
    let mut buf = IoBuf::<u8>::new(q.dev.dev_info.max_io_buf_bytes as usize);
    let buf_addr = buf.as_mut_ptr();
    let mut cmd_op = libublk::sys::UBLK_U_IO_FETCH_REQ;
    let mut res = 0;

    log::debug!("qcow2: io task {} stated", tag);
    q.register_io_buf(tag, &buf);
    loop {
        let cmd_res = q.submit_io_cmd(tag, cmd_op, buf_addr, res).await;
        if cmd_res == libublk::sys::UBLK_IO_RES_ABORT {
            break;
        }

        res = qcow2_handle_io_cmd_async(&q, &qdev_q, tag, &mut buf).await;
        cmd_op = libublk::sys::UBLK_U_IO_COMMIT_AND_FETCH_REQ;
    }
}

/// Start device in async IO task, in which both control and io rings
/// are driven in current context
fn start_dev_fn(
    exe: &smol::LocalExecutor,
    ctrl_rc: &Rc<UblkCtrl>,
    dev_arc: &Rc<UblkDev>,
    q: &UblkQueue,
) -> Result<i32, UblkError> {
    let ctrl_clone = ctrl_rc.clone();
    let dev_clone = dev_arc.clone();

    // Start device in one dedicated io task
    let task = exe.spawn(async move {
        let r = ctrl_clone.configure_queue(&dev_clone, 0, unsafe { libc::gettid() });
        if r.is_err() {
            r
        } else {
            ctrl_clone.start_dev_async(&dev_clone).await
        }
    });
    ublk_run_ctrl_task(exe, q, &task)?;
    smol::block_on(task)
}

fn ublk_qcow2_shutdown<'a, T: Qcow2IoOps + 'a>(
    exe: &smol::LocalExecutor<'a>,
    tgt_rc: &Rc<Qcow2Tgt<T>>,
    q: &UblkQueue,
) -> Result<(), UblkError> {
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
    *(tgt_rc.queue_is_down.borrow_mut()) = true;
    ublk_run_io_task(&exe, &flush_task, q, 0)?;

    // flushing meta final time
    let tgt = tgt_rc.clone();
    let task = exe.spawn(async move {
        tgt.qdev.flush_meta().await.unwrap();
    });
    ublk_run_io_task(&exe, &task, q, 1)?;

    Ok(())
}

pub(crate) fn ublk_add_qcow2(
    ctrl_in: UblkCtrl,
    _id: i32,
    opt: Option<Qcow2Args>,
) -> Result<i32, UblkError> {
    let ctrl = Rc::new(ctrl_in);

    if (ctrl.dev_info().flags & (libublk::sys::UBLK_F_USER_COPY as u64)) != 0 {
        eprintln!("qcow2 doesn't support USER_COPY yet");
        return Err(UblkError::InvalidVal);
    }

    if ctrl.dev_info().nr_hw_queues != 1 {
        eprintln!("qcow2 doesn't support MQ yet");
        return Err(UblkError::InvalidVal);
    }

    let (file, dio) = match opt {
        Some(ref o) => {
            let parent = o.gen_arg.get_start_dir();

            (to_absolute_path(o.file.clone(), parent), !o.buffered_io)
        }
        None => match ctrl.get_target_data_from_json() {
            Some(val) => {
                let lo = &val["qcow2"];
                let tgt_data: Result<Qcow2Json, _> = serde_json::from_value(lo.clone());

                match tgt_data {
                    Ok(t) => (PathBuf::from(t.back_file_path.as_str()), t.direct_io != 0),
                    Err(_) => return Err(UblkError::InvalidVal),
                }
            }
            None => return Err(UblkError::InvalidVal),
        },
    };

    let file_path = format!("{}", file.as_path().display());
    log::info!("qcow2: add: path {}", &file_path);

    let p = qcow2_rs::qcow2_default_params!(false, dio);
    let qdev = ulbk_qcow2_setup_dev(file.as_path(), &p).unwrap();
    let dev_size = qdev.info.virtual_size();
    let tgt_rc = Rc::new(Qcow2Tgt {
        direct_io: i32::from(dio),
        back_file_path: file_path,
        qdev,
        queue_is_down: RefCell::new(false),
    });

    let _shm = {
        if let Some(ref o) = opt {
            Some(o.gen_arg.get_shm_id())
        } else {
            None
        }
    };

    let tgt_clone = tgt_rc.clone();
    let tgt_init = move |dev: &mut UblkDev| qcow2_init_tgt(dev, &tgt_clone, opt, dev_size);
    let dev_rc = Rc::new(UblkDev::new(ctrl.get_name(), tgt_init, &ctrl).unwrap());

    let this_dev = dev_rc.clone();
    let q_rc = Rc::new(UblkQueue::new(0, &this_dev).unwrap());
    let q = q_rc.clone();
    let qp = &*q as *const UblkQueue;
    set_thread_local_queue(qp as *const ());

    // Executor has to be created finally
    let exe = smol::LocalExecutor::new();

    // Prepare qcow2 for handling IO
    let tgt = tgt_rc.clone();
    let task = exe.spawn(async move { tgt.qdev.qcow2_prep_io().await.unwrap() });
    ublk_run_io_task(&exe, &task, &q_rc, 1)?;

    // Spawn io tasks
    let mut f_vec = Vec::new();
    for tag in 0..ctrl.dev_info().queue_depth as u16 {
        let q = q_rc.clone();
        let tgt = tgt_rc.clone();

        f_vec.push(exe.spawn(async move {
            let t = &tgt;
            let qp = &q;
            ublk_qcow2_io_fn(t, qp, tag).await;
        }));
    }

    // Start device
    start_dev_fn(&exe, &ctrl, &dev_rc, &q)?;
    log::info!("qcow2: device started");
    if let Some(shm) = _shm {
        let dev_id = ctrl.dev_info().dev_id;
        crate::rublk_write_id_into_shm(&shm, dev_id);
    }

    // Drive IO tasks for moving on
    ublk_wait_and_handle_ios(&exe, &q);
    smol::block_on(async { futures::future::join_all(f_vec).await });
    log::info!("qcow2: queue is down");

    // Shutdown qcow2 device
    ublk_qcow2_shutdown(&exe, &tgt_rc, &q_rc)?;

    Ok(0)
}
