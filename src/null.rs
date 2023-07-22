use core::any::Any;
use libublk::{UblkDev, UblkError, UblkQueue};
use log::trace;

pub struct NullTgt {}
pub struct NullQueue {}

impl libublk::UblkTgtImpl for NullTgt {
    fn init_tgt(&self, dev: &UblkDev) -> Result<serde_json::Value, UblkError> {
        trace!("none: init_tgt {}", dev.dev_info.dev_id);
        let info = dev.dev_info;
        let dev_size = 250_u64 << 30;

        let mut tgt = dev.tgt.borrow_mut();

        tgt.dev_size = dev_size;
        tgt.params = libublk::sys::ublk_params {
            types: libublk::sys::UBLK_PARAM_TYPE_BASIC,
            basic: libublk::sys::ublk_param_basic {
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
    fn tgt_type(&self) -> &'static str {
        "null"
    }
    #[inline(always)]
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl libublk::UblkQueueImpl for NullQueue {
    fn queue_io(&self, q: &mut UblkQueue, tag: u32) -> Result<i32, UblkError> {
        let iod = q.get_iod(tag);
        let bytes = unsafe { (*iod).nr_sectors << 9 } as i32;

        q.complete_io(tag as u16, bytes);
        Ok(0)
    }
}
