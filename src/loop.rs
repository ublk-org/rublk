use anyhow::Result as AnyRes;
use core::any::Any;
use ilog::IntLog;
use io_uring::{opcode, squeue, types};
use libublk::{ublksrv_io_desc, UblkDev, UblkQueue};
use log::trace;
use serde::{Deserialize, Serialize};
use std::os::unix::fs::FileTypeExt;
use std::os::unix::io::AsRawFd;

// Generate ioctl function
const BLK_IOCTL_TYPE: u8 = 0x12; // Defined in linux/fs.h
const BLKGETSIZE64_NR: u8 = 114;
const BLKSSZGET_NR: u8 = 104;
const BLKPBSZGET_NR: u8 = 123;

ioctl_read!(ioctl_blkgetsize64, BLK_IOCTL_TYPE, BLKGETSIZE64_NR, u64);
ioctl_read_bad!(
    ioctl_blksszget,
    request_code_none!(BLK_IOCTL_TYPE, BLKSSZGET_NR),
    i32
);
ioctl_read_bad!(
    ioctl_blkpbszget,
    request_code_none!(BLK_IOCTL_TYPE, BLKPBSZGET_NR),
    u32
);

#[derive(Debug, Serialize)]
struct LoJson {
    back_file_path: String,
    direct_io: i32,
}

pub struct LoopTgt {
    pub back_file_path: String,
    pub back_file: std::fs::File,
    pub direct_io: i32,
}

pub struct LoopQueue {}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct LoopArgs {
    back_file: String,
    direct_io: i32,
}

fn lo_file_size(f: &std::fs::File) -> AnyRes<(u64, u8, u8)> {
    if let Ok(meta) = f.metadata() {
        if meta.file_type().is_block_device() {
            let fd = f.as_raw_fd();
            let mut cap = 0_u64;
            let mut ssz = 0_i32;
            let mut pbsz = 0_u32;

            unsafe {
                let cap_ptr = &mut cap as *mut u64;
                let ssz_ptr = &mut ssz as *mut i32;
                let pbsz_ptr = &mut pbsz as *mut u32;

                ioctl_blkgetsize64(fd, cap_ptr).unwrap();
                ioctl_blksszget(fd, ssz_ptr).unwrap();
                ioctl_blkpbszget(fd, pbsz_ptr).unwrap();
            }

            Ok((cap, ssz.log2() as u8, pbsz.log2() as u8))
        } else if meta.file_type().is_file() {
            Ok((f.metadata().unwrap().len(), 9, 12))
        } else {
            Err(anyhow::anyhow!("unsupported file"))
        }
    } else {
        Err(anyhow::anyhow!("no file meta got"))
    }
}

impl libublk::UblkTgtImpl for LoopTgt {
    fn init_tgt(&self, dev: &UblkDev) -> AnyRes<serde_json::Value> {
        trace!("loop: init_tgt {}", dev.dev_info.dev_id);
        let info = dev.dev_info;

        if self.direct_io != 0 {
            unsafe {
                libc::fcntl(self.back_file.as_raw_fd(), libc::F_SETFL, libc::O_DIRECT);
            }
        }

        let mut td = dev.tdata.borrow_mut();
        let nr_fds = td.nr_fds;
        td.fds[nr_fds as usize] = self.back_file.as_raw_fd();
        td.nr_fds = nr_fds + 1;

        let mut tgt = dev.tgt.borrow_mut();
        let sz = lo_file_size(&self.back_file).unwrap();

        tgt.dev_size = sz.0;
        //todo: figure out correct block size
        tgt.params = libublk::ublk_params {
            types: libublk::UBLK_PARAM_TYPE_BASIC,
            basic: libublk::ublk_param_basic {
                logical_bs_shift: sz.1,
                physical_bs_shift: sz.2,
                io_opt_shift: 12,
                io_min_shift: 9,
                max_sectors: info.max_io_buf_bytes >> 9,
                dev_sectors: tgt.dev_size >> 9,
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(
            serde_json::json!({"loop": LoJson { back_file_path: self.back_file_path.clone(), direct_io: 1 } }),
        )
    }
    fn deinit_tgt(&self, dev: &UblkDev) {
        trace!("loop: deinit_tgt {}", dev.dev_info.dev_id);
    }
    fn tgt_type(&self) -> &'static str {
        "loop"
    }
    #[inline(always)]
    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn loop_queue_tgt_io(q: &mut UblkQueue, tag: u32, iod: &ublksrv_io_desc) -> AnyRes<i32> {
    let off = (iod.start_sector << 9) as u64;
    let bytes = (iod.nr_sectors << 9) as u32;
    let op = iod.op_flags & 0xff;
    let data = libublk::build_user_data(tag as u16, op, 0, true);
    let buf_addr = q.get_buf_addr(tag);

    if op == libublk::UBLK_IO_OP_WRITE_ZEROES || op == libublk::UBLK_IO_OP_DISCARD {
        return Err(anyhow::anyhow!("unexpected discard"));
    }

    match op {
        libublk::UBLK_IO_OP_FLUSH => {
            let sqe = &opcode::SyncFileRange::new(types::Fixed(1), bytes)
                .offset(off)
                .build()
                .flags(squeue::Flags::FIXED_FILE)
                .user_data(data);
            unsafe {
                q.q_ring.submission().push(sqe).expect("submission fail");
            }
        }
        libublk::UBLK_IO_OP_READ => {
            let sqe = &opcode::Read::new(types::Fixed(1), buf_addr, bytes)
                .offset(off)
                .build()
                .flags(squeue::Flags::FIXED_FILE)
                .user_data(data);
            unsafe {
                q.q_ring.submission().push(sqe).expect("submission fail");
            }
        }
        libublk::UBLK_IO_OP_WRITE => {
            let sqe = &opcode::Write::new(types::Fixed(1), buf_addr, bytes)
                .offset(off)
                .build()
                .flags(squeue::Flags::FIXED_FILE)
                .user_data(data);
            unsafe {
                q.q_ring.submission().push(sqe).expect("submission fail");
            }
        }
        _ => return Err(anyhow::anyhow!("unexpected op")),
    }

    Ok(1)
}

impl libublk::UblkQueueImpl for LoopQueue {
    fn queue_io(&self, q: &mut UblkQueue, tag: u32) -> AnyRes<i32> {
        let _iod = q.get_iod(tag);
        let iod = unsafe { &*_iod };

        loop_queue_tgt_io(q, tag, iod)
    }

    fn tgt_io_done(&self, q: &mut UblkQueue, tag: u32, res: i32, user_data: u64) {
        let cqe_tag = libublk::user_data_to_tag(user_data);

        assert!(cqe_tag == tag);

        if res != -(libc::EAGAIN) {
            q.complete_io(tag as u16, res);
        } else {
            let _iod = q.get_iod(tag);
            let iod = unsafe { &*_iod };

            loop_queue_tgt_io(q, tag, iod).unwrap();
        }
    }
}
