use crate::args::AddArgs;
use anyhow::Result as AnyRes;
use libublk::{ublk_tgt_priv_data, UblkDev, UblkIO, UblkQueue};
use log::trace;
use serde::{Deserialize, Serialize};
use std::os::unix::fs::FileTypeExt;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::io::AsRawFd;

// Generate ioctl function
const BLKGETSIZE64_CODE: u8 = 0x12; // Defined in linux/fs.h
const BLKGETSIZE64_SEQ: u8 = 114;
ioctl_read!(ioctl_blkgetsize64, BLKGETSIZE64_CODE, BLKGETSIZE64_SEQ, u64);

#[derive(Debug)]
pub struct LoPriData {
    file: std::fs::File,
}

pub struct LoopOps {}
pub struct LoopQueueOps {}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct LoopArgs {
    back_file: String,
    direct_io: i32,
}

pub fn loop_build_args_json(a: &AddArgs) -> (usize, serde_json::Value) {
    let lo_args = LoopArgs {
        back_file: (&a)
            .file
            .as_ref()
            .map(|path_buf| path_buf.to_string_lossy().into_owned())
            .unwrap_or(String::new()),
        direct_io: a.direct_io as i32,
    };

    (
        core::mem::size_of::<LoPriData>(),
        serde_json::json!({"loop": lo_args,}),
    )
}

fn lo_file_size(f: &mut std::fs::File) -> AnyRes<u64> {
    if let Ok(meta) = f.metadata() {
        if meta.file_type().is_block_device() {
            let fd = f.as_raw_fd();
            let mut cap = 0u64;
            let cap_ptr = &mut cap as *mut u64;

            unsafe {
                ioctl_blkgetsize64(fd, cap_ptr).unwrap();
            }

            Ok(cap)
        } else if meta.file_type().is_file() {
            Ok(f.metadata().unwrap().len())
        } else {
            Err(anyhow::anyhow!("unsupported file"))
        }
    } else {
        Err(anyhow::anyhow!("no file meta got"))
    }
}

impl libublk::UblkTgtOps for LoopOps {
    fn init_tgt(&self, dev: &UblkDev, _tdj: serde_json::Value) -> AnyRes<serde_json::Value> {
        trace!("loop: init_tgt {}", dev.dev_info.dev_id);
        let info = dev.dev_info;

        let mut td = dev.tdata.borrow_mut();
        let tdj = _tdj.get("loop").unwrap().clone();
        let lo_arg = serde_json::from_value::<LoopArgs>(tdj).unwrap();

        let mut _pd = ublk_tgt_priv_data::<LoPriData>(&td).unwrap();
        let mut pd = unsafe { &mut *_pd };
        pd.file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_DIRECT)
            .open(&lo_arg.back_file)?;

        let nr_fds = td.nr_fds;
        td.fds[nr_fds as usize] = pd.file.as_raw_fd();
        td.nr_fds = nr_fds + 1;

        let mut tgt = dev.tgt.borrow_mut();
        tgt.dev_size = lo_file_size(&mut pd.file).unwrap();
        tgt.params = libublk::ublk_params {
            types: libublk::UBLK_PARAM_TYPE_BASIC,
            basic: libublk::ublk_param_basic {
                logical_bs_shift: 9,
                physical_bs_shift: 12,
                io_opt_shift: 12,
                io_min_shift: 9,
                max_sectors: info.max_io_buf_bytes >> 9,
                dev_sectors: tgt.dev_size >> 9,
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(serde_json::json!({"loop": lo_arg,}))
    }
    fn deinit_tgt(&self, dev: &UblkDev) {
        trace!("none: deinit_tgt {}", dev.dev_info.dev_id);
    }
}

impl libublk::UblkQueueOps for LoopQueueOps {
    fn queue_io(&self, q: &UblkQueue, io: &mut UblkIO, tag: u32) -> AnyRes<i32> {
        let iod = q.get_iod(tag);
        let bytes = unsafe { (*iod).nr_sectors << 9 } as i32;

        q.complete_io(io, tag as u16, bytes);
        Ok(0)
    }
    fn tgt_io_done(&self, _q: &UblkQueue, _tag: u32, _res: i32, _user_data: u64) {}
}
