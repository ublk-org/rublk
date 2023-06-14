use anyhow::Result as AnyRes;
use libublk::{UblkDev, UblkIO, UblkQueue};
use log::trace;
use serde::{Deserialize, Serialize};

use crate::args::AddArgs;

pub struct LoopOps {}
pub struct LoopQueueOps {}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct LoopArgs {
    back_file: String,
    direct_io: i32,
}

pub fn loop_build_args_json(a: &AddArgs) -> serde_json::Value {
    let lo_args = LoopArgs {
        back_file: (&a)
            .file
            .as_ref()
            .map(|path_buf| path_buf.to_string_lossy().into_owned())
            .unwrap_or(String::new()),
        direct_io: a.direct_io as i32,
    };

    serde_json::json!({"loop": lo_args,})
}

impl libublk::UblkTgtOps for LoopOps {
    fn init_tgt(&self, dev: &UblkDev, _tdj: serde_json::Value) -> AnyRes<serde_json::Value> {
        trace!("loop: init_tgt {}", dev.dev_info.dev_id);
        let info = dev.dev_info;
        let dev_size = 250_u64 << 30;

        let mut tgt = dev.tgt.borrow_mut();
        tgt.dev_size = dev_size;
        tgt.params = libublk::ublk_params {
            types: libublk::UBLK_PARAM_TYPE_BASIC,
            basic: libublk::ublk_param_basic {
                logical_bs_shift: 9,
                physical_bs_shift: 12,
                io_opt_shift: 12,
                io_min_shift: 9,
                max_sectors: info.max_io_buf_bytes >> 9,
                dev_sectors: dev_size >> 9,
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(serde_json::json!({}))
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
