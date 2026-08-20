#[rustversion::before(1.75)]
use async_trait::async_trait;
use libublk::ctrl::UblkCtrl;
use libublk::helpers::IoBuf;
use libublk::io::{BufDesc, UblkDev, UblkQueue};
use libublk::ops::{self, TgtFd};
use libublk::UblkError;
use qcow2_rs::dev::{Qcow2Dev, Qcow2DevParams};
use qcow2_rs::error::Qcow2Result;
use qcow2_rs::ops::*;
use qcow2_rs::utils::qcow2_alloc_dev_sync;
use serde::{Deserialize, Serialize};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;

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

    qdev: Qcow2Dev<T>,
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
        log::info!(
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
        log::trace!("qcow2_read: offset {:x} len {}", offset, buf.len(),);
        loop {
            // SAFETY: `buf` outlives the await below
            let res = match unsafe {
                ops::read_at_raw(
                    TgtFd::Raw(self.fd),
                    buf.as_mut_ptr(),
                    buf.len() as u32,
                    offset,
                )
            } {
                Ok(op) => op.await,
                Err(e) => e.errno(),
            };
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
        log::trace!("qcow2_write: offset {:x} len {}", offset, buf.len(),);
        loop {
            // SAFETY: `buf` outlives the await below
            let res = match unsafe {
                ops::write_at_raw(TgtFd::Raw(self.fd), buf.as_ptr(), buf.len() as u32, offset)
            } {
                Ok(op) => op.await,
                Err(e) => e.errno(),
            };
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
        let mode = if (flags & Qcow2OpsFlags::FALLOCATE_ZERO_RAGE) != 0 {
            0x10 //ZERO_RANGE include/uapi/linux/falloc.h
        } else {
            0
        };

        log::trace!("qcow2 discard: offset {:x} len {}", offset, len);
        loop {
            let res = match ops::fallocate(TgtFd::Raw(self.fd), mode, offset, len as u64) {
                Ok(op) => op.await,
                Err(e) => e.errno(),
            };
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
        log::trace!("qcow2 fsync: offset {:x} len {}", offset, len,);
        loop {
            let res = match ops::sync_file_range(TgtFd::Raw(self.fd), offset, len as u32, 0) {
                Ok(op) => op.await,
                Err(e) => e.errno(),
            };
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
    q: &UblkQueue,
    qdev: &Qcow2Dev<T>,
    tag: u16,
    buf: &mut [u8],
) -> i32 {
    let iod = q.get_iod(tag);
    let op = iod.op_flags & 0xff;
    let off = iod.start_sector << 9;
    let bytes = (iod.nr_sectors << 9) as usize;

    log::trace!("ublk_io: {} op {} offset {:x} len {}", tag, op, off, bytes);
    match op {
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
    }
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

async fn ublk_qcow2_io_fn<T: Qcow2IoOps>(
    tgt: &Qcow2Tgt<T>,
    q: &UblkQueue,
    tag: u16,
) -> Result<(), UblkError> {
    let qdev_q = &tgt.qdev;
    let mut buf = IoBuf::<u8>::new(q.dev().dev_info.max_io_buf_bytes as usize);
    let _buf_addr = buf.as_mut_ptr();

    log::debug!("qcow2: io task {} stated", tag);

    // Submit initial prep command
    q.submit_io_prep_cmd(tag, BufDesc::Slice(buf.as_slice()), 0, Some(&buf))
        .await?;
    loop {
        let res = qcow2_handle_io_cmd_async(q, qdev_q, tag, &mut buf).await;
        q.submit_io_commit_cmd(tag, BufDesc::Slice(buf.as_slice()), res)
            .await?;
    }
}

pub(crate) fn ublk_add_qcow2(
    ctrl_in: UblkCtrl,
    opt: Option<Qcow2Args>,
    comm_arc: &Arc<crate::DevIdComm>,
) -> anyhow::Result<i32> {
    let dev_id = ctrl_in.dev_info().dev_id;
    let ctrl = Rc::new(ctrl_in);

    if (ctrl.dev_info().flags & (libublk::sys::UBLK_F_USER_COPY as u64)) != 0 {
        return Err(anyhow::anyhow!("qcow2 doesn't support USER_COPY yet"));
    }

    if ctrl.dev_info().nr_hw_queues != 1 {
        return Err(anyhow::anyhow!("qcow2 doesn't support MQ yet"));
    }

    let (file, dio) = match opt {
        Some(ref o) => (o.gen_arg.build_abs_path(o.file.clone()), !o.buffered_io),
        None => match ctrl.get_target_data_from_json() {
            Some(val) => {
                let lo = &val["qcow2"];
                let tgt_data: Result<Qcow2Json, _> = serde_json::from_value(lo.clone());

                match tgt_data {
                    Ok(t) => (PathBuf::from(t.back_file_path.as_str()), t.direct_io != 0),
                    Err(_) => return Err(anyhow::anyhow!("invalid json data")),
                }
            }
            None => return Err(anyhow::anyhow!("no json data")),
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
    });

    let tgt_clone = tgt_rc.clone();
    let tgt_init = move |dev: &mut UblkDev| qcow2_init_tgt(dev, &tgt_clone, opt, dev_size);
    let dev_rc = Arc::new(UblkDev::new(ctrl.get_name(), tgt_init, &ctrl).unwrap());

    let depth = ctrl.dev_info().queue_depth;
    let comm = comm_arc.clone();
    let rt = crate::Rt::new()?;
    rt.block_on(async move {
        let q_rc = Rc::new(UblkQueue::new(0, &dev_rc)?);

        // Spawn one io task per tag; they progress whenever this task awaits
        let mut f_vec = Vec::new();
        for tag in 0..depth {
            let q = q_rc.clone();
            let tgt = tgt_rc.clone();

            f_vec.push(libublk::spawn_local(async move {
                match ublk_qcow2_io_fn(&tgt, &q, tag).await {
                    Err(UblkError::QueueIsDown) | Ok(_) => {}
                    Err(e) => log::error!("ublk_qcow2_io_fn failed for tag {}: {}", tag, e),
                }
            }));
        }

        // Prepare qcow2 for handling IO
        tgt_rc.qdev.qcow2_prep_io().await.unwrap();

        //setup single cpu affinity
        if dev_rc
            .flags
            .intersects(libublk::UblkFlags::UBLK_DEV_F_SINGLE_CPU_AFFINITY)
        {
            ctrl.set_queue_single_affinity(0, None)?;
        }

        ctrl.configure_queue(&dev_rc, 0, unsafe { libc::gettid() })?;
        ctrl.start_dev_async(&dev_rc).await?;
        log::info!("qcow2: device started");

        // Tell parent we are up
        comm.send_dev_id(dev_id).unwrap();

        // Flush qcow2 meta with a 50ms delay, matching the old executor's
        // delayed flush task
        let flush_tgt = tgt_rc.clone();
        let flush_task = libublk::spawn_local(async move {
            loop {
                match libublk::ops::sleep(std::time::Duration::from_millis(50)) {
                    Ok(op) => {
                        if op.await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
                if flush_tgt.qdev.need_flush_meta() {
                    flush_tgt.qdev.flush_meta().await.unwrap();
                }
            }
        });

        for f in f_vec {
            let _ = f.await;
        }
        log::info!("qcow2: queue is down");
        flush_task.cancel();

        // flushing meta final time
        tgt_rc.qdev.flush_meta().await.unwrap();
        Ok::<i32, UblkError>(0)
    })?;

    Ok(0)
}
