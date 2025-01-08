#![allow(non_snake_case)]

use libublk::helpers::IoBuf;
use libublk::sys::ublksrv_io_desc;
use libublk::sys::{
    BLK_ZONE_COND_CLOSED, BLK_ZONE_COND_EMPTY, BLK_ZONE_COND_EXP_OPEN, BLK_ZONE_COND_FULL,
    BLK_ZONE_COND_IMP_OPEN, BLK_ZONE_COND_NOT_WP, BLK_ZONE_COND_OFFLINE, BLK_ZONE_COND_READONLY,
};
use libublk::sys::{BLK_ZONE_TYPE_CONVENTIONAL, BLK_ZONE_TYPE_SEQWRITE_REQ};
use libublk::sys::{
    UBLK_IO_OP_FLUSH, UBLK_IO_OP_READ, UBLK_IO_OP_REPORT_ZONES, UBLK_IO_OP_WRITE,
    UBLK_IO_OP_ZONE_APPEND, UBLK_IO_OP_ZONE_CLOSE, UBLK_IO_OP_ZONE_FINISH, UBLK_IO_OP_ZONE_OPEN,
    UBLK_IO_OP_ZONE_RESET, UBLK_IO_OP_ZONE_RESET_ALL,
};
use libublk::uring_async::ublk_wait_and_handle_ios;
use libublk::{ctrl::UblkCtrl, io::UblkDev, io::UblkIOCtx, io::UblkQueue};

use log::trace;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::RwLock;

#[derive(Debug, Default)]
struct Zone {
    r#type: libublk::sys::blk_zone_type,
    cond: libublk::sys::blk_zone_cond,
    start: u64,
    len: u32,
    capacity: u32,

    wp: u64,
}

#[derive(Debug, Default)]
struct TgtData {
    nr_zones_imp_open: u32,
    nr_zones_exp_open: u32,
    nr_zones_closed: u32,
    imp_close_zone_no: u32,
}

#[derive(Debug)]
struct ZonedTgt {
    size: u64,
    start: u64,

    zone_size: u64,
    _zone_capacity: u64,
    zone_nr_conv: u32,
    zone_max_open: u32,
    zone_max_active: u32,
    nr_zones: u32,

    zones: Vec<RwLock<Zone>>,
    data: RwLock<TgtData>,
    _buf: IoBuf<u8>,
}

impl ZonedTgt {
    #[allow(clippy::uninit_vec)]
    fn new(size: u64, zone_size: u64, conv_zones: u32) -> ZonedTgt {
        let _buf = IoBuf::<u8>::new(size as usize);
        let buf_addr = _buf.as_mut_ptr() as u64;
        let zone_cap = zone_size >> 9;
        let nr_zones = size / zone_size;

        let zones: Vec<RwLock<Zone>> = (0..nr_zones as usize)
            .map(|i| {
                RwLock::new(Zone {
                    capacity: zone_cap as u32,
                    len: (zone_size >> 9) as u32,
                    r#type: if i < conv_zones.try_into().unwrap() {
                        BLK_ZONE_TYPE_CONVENTIONAL
                    } else {
                        BLK_ZONE_TYPE_SEQWRITE_REQ
                    },
                    cond: if i < conv_zones.try_into().unwrap() {
                        BLK_ZONE_COND_NOT_WP
                    } else {
                        BLK_ZONE_COND_EMPTY
                    },
                    start: (i as u64) * (zone_size >> 9),
                    wp: (i as u64) * (zone_size >> 9),
                })
            })
            .collect();

        ZonedTgt {
            size,
            start: buf_addr,
            zone_size,
            _zone_capacity: zone_size,
            zone_max_open: 0,
            zone_max_active: 0,
            nr_zones: nr_zones as u32,
            data: RwLock::new(TgtData {
                ..Default::default()
            }),
            zone_nr_conv: conv_zones,
            zones,
            _buf,
        }
    }

    #[inline(always)]
    fn get_zone_no(&self, sector: u64) -> u32 {
        let zsects = (self.zone_size >> 9) as u32;

        (sector / (zsects as u64)) as u32
    }

    #[inline(always)]
    fn get_zone(&self, sector: u64) -> std::sync::RwLockReadGuard<'_, Zone> {
        let zno = self.get_zone_no(sector) as usize;
        self.zones[zno].read().unwrap()
    }

    #[inline(always)]
    fn get_zone_mut(&self, sector: u64) -> std::sync::RwLockWriteGuard<'_, Zone> {
        let zno = self.get_zone_no(sector) as usize;
        self.zones[zno].write().unwrap()
    }

    #[inline(always)]
    fn get_zone_meta(&self, zno: u32) -> (libublk::sys::blk_zone_cond, u64, u32, u64) {
        let z = self.zones[zno as usize].read().unwrap();

        (z.cond, z.start, z.len, z.wp)
    }

    #[inline(always)]
    fn get_imp_close_zone_no(&self) -> u32 {
        let data = self.data.read().unwrap();

        data.imp_close_zone_no
    }

    #[inline(always)]
    fn set_imp_close_zone_no(&self, zno: u32) {
        let mut data = self.data.write().unwrap();
        data.imp_close_zone_no = zno;
    }

    #[inline(always)]
    fn get_open_zones(&self) -> (u32, u32) {
        let data = self.data.read().unwrap();

        (data.nr_zones_exp_open, data.nr_zones_imp_open)
    }

    fn __close_zone(&self, z: &mut std::sync::RwLockWriteGuard<'_, Zone>) -> i32 {
        match z.cond {
            BLK_ZONE_COND_CLOSED => return 0,
            BLK_ZONE_COND_IMP_OPEN => {
                let mut data = self.data.write().unwrap();
                data.nr_zones_imp_open -= 1;
            }
            BLK_ZONE_COND_EXP_OPEN => {
                let mut data = self.data.write().unwrap();
                data.nr_zones_exp_open -= 1;
            }
            //case BLK_ZONE_COND_EMPTY:
            //case BLK_ZONE_COND_FULL:
            _ => return -libc::EIO,
        }

        if z.wp == z.start {
            z.cond = BLK_ZONE_COND_EMPTY;
        } else {
            z.cond = BLK_ZONE_COND_CLOSED;

            let mut data = self.data.write().unwrap();
            data.nr_zones_closed += 1;
        }

        0
    }

    fn close_imp_open_zone(&self) {
        let mut zno = self.get_imp_close_zone_no();
        if zno >= self.nr_zones {
            zno = self.zone_nr_conv;
        }

        let total = self.zone_nr_conv + self.nr_zones;

        for _ in self.zone_nr_conv..total {
            let _zno = zno as usize;
            let mut z = self.zones[_zno].write().unwrap();

            zno += 1;

            if zno >= self.nr_zones {
                zno = self.zone_nr_conv;
            }

            if z.cond == BLK_ZONE_COND_IMP_OPEN {
                self.__close_zone(&mut z);
                self.set_imp_close_zone_no(zno);
                return;
            }
        }
    }

    fn check_active(&self) -> i32 {
        if self.zone_max_active == 0 {
            return 0;
        }

        let data = self.data.read().unwrap();
        if data.nr_zones_exp_open + data.nr_zones_imp_open + data.nr_zones_closed
            < self.zone_max_active
        {
            return 0;
        }

        -libc::EBUSY
    }

    fn check_open(&self) -> i32 {
        if self.zone_max_open == 0 {
            return 0;
        }

        let (exp_open, imp_open) = self.get_open_zones();

        if exp_open + imp_open < self.zone_max_open {
            return 0;
        }

        if imp_open > 0 && self.check_active() == 0 {
            self.close_imp_open_zone();
            return 0;
        }

        -libc::EBUSY
    }

    fn check_zone_resources(&self, cond: libublk::sys::blk_zone_cond) -> i32 {
        match cond {
            BLK_ZONE_COND_EMPTY => {
                let ret = self.check_active();
                if ret != 0 {
                    ret
                } else {
                    self.check_open()
                }
            }
            BLK_ZONE_COND_CLOSED => self.check_open(),
            _ => -libc::EIO,
        }
    }

    fn zone_reset(&self, sector: u64) -> i32 {
        let mut z = self.get_zone_mut(sector);

        if z.r#type == BLK_ZONE_TYPE_CONVENTIONAL {
            return -libc::EPIPE;
        }

        match z.cond {
            BLK_ZONE_COND_EMPTY | BLK_ZONE_COND_READONLY | BLK_ZONE_COND_OFFLINE => return 0,
            BLK_ZONE_COND_IMP_OPEN => {
                let mut data = self.data.write().unwrap();
                data.nr_zones_imp_open -= 1;
            }
            BLK_ZONE_COND_EXP_OPEN => {
                let mut data = self.data.write().unwrap();
                data.nr_zones_exp_open -= 1;
            }
            BLK_ZONE_COND_CLOSED => {
                let mut data = self.data.write().unwrap();
                data.nr_zones_closed -= 1;
            }
            BLK_ZONE_COND_FULL => {}
            _ => return -libc::EINVAL,
        }

        let start = z.start;
        z.cond = BLK_ZONE_COND_EMPTY;
        z.wp = start;

        self.discard_zone(self.get_zone_no(sector));

        0
    }

    fn zone_open(&self, sector: u64) -> i32 {
        let mut z = self.get_zone_mut(sector);

        if z.r#type == BLK_ZONE_TYPE_CONVENTIONAL {
            return -libc::EPIPE;
        }

        //fixme: add zone check
        match z.cond {
            BLK_ZONE_COND_EXP_OPEN => {}
            BLK_ZONE_COND_EMPTY => {
                let ret = self.check_zone_resources(BLK_ZONE_COND_EMPTY);
                if ret != 0 {
                    return ret;
                }
            }
            BLK_ZONE_COND_IMP_OPEN => {
                let mut data = self.data.write().unwrap();
                data.nr_zones_imp_open -= 1;
            }
            BLK_ZONE_COND_CLOSED => {
                let ret = self.check_zone_resources(BLK_ZONE_COND_CLOSED);
                if ret != 0 {
                    return ret;
                }
                let mut data = self.data.write().unwrap();
                data.nr_zones_closed -= 1;
            }
            _ => return -libc::EINVAL,
        }
        z.cond = BLK_ZONE_COND_EXP_OPEN;
        let mut data = self.data.write().unwrap();
        data.nr_zones_exp_open += 1;
        0
    }

    fn zone_close(&self, sector: u64) -> i32 {
        let mut z = self.get_zone_mut(sector);

        if z.r#type == BLK_ZONE_TYPE_CONVENTIONAL {
            return -libc::EPIPE;
        }

        self.__close_zone(&mut z)
    }

    fn zone_finish(&self, sector: u64) -> i32 {
        let mut ret = 0;
        let mut z = self.get_zone_mut(sector);

        if z.r#type == BLK_ZONE_TYPE_CONVENTIONAL {
            return -libc::EPIPE;
        }

        match z.cond {
            // finish operation on full is not an error
            BLK_ZONE_COND_FULL => return ret,
            BLK_ZONE_COND_EMPTY => {
                ret = self.check_zone_resources(BLK_ZONE_COND_EMPTY);
                if ret != 0 {
                    return ret;
                }
            }
            BLK_ZONE_COND_IMP_OPEN => {
                let mut data = self.data.write().unwrap();
                data.nr_zones_imp_open -= 1;
            }

            BLK_ZONE_COND_EXP_OPEN => {
                let mut data = self.data.write().unwrap();
                data.nr_zones_exp_open -= 1;
            }
            BLK_ZONE_COND_CLOSED => {
                ret = self.check_zone_resources(BLK_ZONE_COND_CLOSED);
                if ret != 0 {
                    return ret;
                }
                let mut data = self.data.write().unwrap();
                data.nr_zones_closed -= 1;
            }
            _ => ret = libc::EIO,
        }

        z.cond = BLK_ZONE_COND_FULL;
        z.wp = z.start + z.len as u64;

        ret
    }

    fn discard_zone(&self, zon: u32) {
        unsafe {
            let off = zon as u64 * self.zone_size;

            libc::madvise(
                (self.start + off) as *mut libc::c_void,
                0,
                self.zone_size as i32,
            );
        }
    }
}

fn handle_report_zones(tgt: &ZonedTgt, q: &UblkQueue, tag: u16, iod: &ublksrv_io_desc) -> isize {
    let zsects = (tgt.zone_size >> 9) as u32;
    let zones = iod.nr_sectors; //union
    let zno = iod.start_sector / (zsects as u64);
    let blkz_sz = core::mem::size_of::<libublk::sys::blk_zone>() as u32;

    // USER_COPY is enabled, so we have to allocate buffer for report_zones
    let buf = IoBuf::<u8>::new((blkz_sz * zones) as usize);
    let buf_addr = buf.as_mut_ptr();
    let mut off = buf_addr as u64;

    trace!(
        "start_sec {} nr_sectors {} zones {}-{} zone_size {} buf {:x}/{:x}",
        iod.start_sector,
        iod.nr_sectors,
        zno,
        zones,
        tgt.zone_size,
        buf_addr as u64,
        off
    );
    for i in zno..(zno + zones as u64) {
        let zone = unsafe { &mut *(off as *mut libublk::sys::blk_zone) };
        let z = tgt.zones[i as usize].read().unwrap();

        zone.capacity = z.capacity as u64;
        zone.len = z.len as u64;
        zone.start = z.start;
        zone.wp = z.wp;
        zone.type_ = z.r#type as u8;
        zone.cond = z.cond as u8;

        off += blkz_sz as u64;
    }

    let dsize = off - buf_addr as u64;
    unsafe {
        let offset = UblkIOCtx::ublk_user_copy_pos(q.get_qid(), tag, 0);

        libc::pwrite(
            q.dev.tgt.fds[0],
            buf_addr as *const libc::c_void,
            dsize.try_into().unwrap(),
            offset.try_into().unwrap(),
        )
    }
}

fn handle_mgmt(tgt: &ZonedTgt, _q: &UblkQueue, _tag: u16, iod: &ublksrv_io_desc) -> i32 {
    if (iod.op_flags & 0xff) == UBLK_IO_OP_ZONE_RESET_ALL {
        let mut off = 0;
        while off < tgt.size {
            tgt.zone_reset(off >> 9);
            off += tgt.zone_size;
        }
        return 0;
    }

    {
        let z = tgt.get_zone(iod.start_sector);

        if z.cond == BLK_ZONE_COND_READONLY || z.cond == BLK_ZONE_COND_OFFLINE {
            return -libc::EPIPE;
        }
    }

    match iod.op_flags & 0xff {
        UBLK_IO_OP_ZONE_RESET => tgt.zone_reset(iod.start_sector),
        UBLK_IO_OP_ZONE_OPEN => tgt.zone_open(iod.start_sector),
        UBLK_IO_OP_ZONE_CLOSE => tgt.zone_close(iod.start_sector),
        UBLK_IO_OP_ZONE_FINISH => tgt.zone_finish(iod.start_sector),
        _ => -libc::EINVAL,
    }
}

async fn handle_read(tgt: &ZonedTgt, q: &UblkQueue<'_>, tag: u16, iod: &ublksrv_io_desc) -> i32 {
    let zno = tgt.get_zone_no(iod.start_sector);
    let z = tgt.get_zone_meta(zno);
    let cond = z.0;

    if cond == BLK_ZONE_COND_OFFLINE {
        return -libc::EIO;
    }

    let bytes = (iod.nr_sectors << 9) as usize;
    let off = iod.start_sector << 9;

    trace!(
        "read lba {:06x}-{:04}), zone: no {:4} cond {:4} start/wp {:06x}/{:06x}",
        iod.start_sector,
        iod.nr_sectors,
        zno,
        z.0,
        z.1,
        z.2,
    );

    unsafe {
        let offset = UblkIOCtx::ublk_user_copy_pos(q.get_qid(), tag, 0);
        let mut addr = (tgt.start + off) as *mut libc::c_void;
        let fd = q.dev.tgt.fds[0];

        // make sure data is zeroed for reset zone
        if cond == BLK_ZONE_COND_EMPTY {
            let buf = IoBuf::<u8>::new(bytes);
            addr = buf.as_mut_ptr() as *mut libc::c_void;

            libc::memset(addr, 0, bytes);
            libc::pwrite(fd, addr, bytes, offset.try_into().unwrap()) as i32
        } else {
            //write data to /dev/ublkcN from our ram directly
            libc::pwrite(fd, addr, bytes, offset.try_into().unwrap()) as i32
        }
    }
}

async fn handle_plain_write(
    tgt: &ZonedTgt,
    q: &UblkQueue<'_>,
    tag: u16,
    start_sector: u64,
    nr_sectors: u32,
) -> i32 {
    let off = start_sector << 9;
    let bytes = (nr_sectors << 9) as usize;
    let offset = UblkIOCtx::ublk_user_copy_pos(q.get_qid(), tag, 0);

    unsafe {
        //read data from /dev/ublkcN to our ram directly
        libc::pread(
            q.dev.tgt.fds[0],
            (tgt.start + off) as *mut libc::c_void,
            bytes,
            offset.try_into().unwrap(),
        ) as i32
    }
}

fn handle_flush(
    _tgt: &ZonedTgt,
    _q: &UblkQueue<'_>,
    _tag: u16,
    _iod: &libublk::sys::ublksrv_io_desc,
) -> i32 {
    return 0;
}

async fn handle_write(
    tgt: &ZonedTgt,
    q: &UblkQueue<'_>,
    tag: u16,
    iod: &libublk::sys::ublksrv_io_desc,
    append: bool,
) -> (u64, i32) {
    let zno = tgt.get_zone_no(iod.start_sector);

    if zno < tgt.zone_nr_conv {
        if append {
            return (u64::MAX, -libc::EIO);
        }

        return (
            u64::MAX,
            handle_plain_write(tgt, q, tag, iod.start_sector, iod.nr_sectors).await,
        );
    }

    let (wp, zone_end, sector) = {
        let mut z = tgt.get_zone_mut(iod.start_sector);
        let mut ret = -libc::EIO;

        let cond = z.cond;
        if cond == BLK_ZONE_COND_FULL
            || cond == BLK_ZONE_COND_READONLY
            || cond == BLK_ZONE_COND_OFFLINE
        {
            return (u64::MAX, ret);
        }

        let sector = z.wp;
        if !append && iod.start_sector != sector {
            return (u64::MAX, ret);
        }

        let zone_end = z.start + z.capacity as u64;
        if z.wp + iod.nr_sectors as u64 > zone_end {
            return (u64::MAX, ret);
        }

        if cond == BLK_ZONE_COND_CLOSED || cond == BLK_ZONE_COND_EMPTY {
            ret = tgt.check_zone_resources(cond);
            if ret != 0 {
                return (u64::MAX, ret);
            }

            let mut data = tgt.data.write().unwrap();
            if cond == BLK_ZONE_COND_CLOSED {
                data.nr_zones_closed -= 1;
                data.nr_zones_imp_open += 1;
            } else if cond == BLK_ZONE_COND_EMPTY {
                data.nr_zones_imp_open += 1;
            }

            if cond != BLK_ZONE_COND_EXP_OPEN {
                z.cond = BLK_ZONE_COND_IMP_OPEN;
            }
        }
        z.wp += iod.nr_sectors as u64;

        (z.wp, zone_end, sector)
    };

    let ret = handle_plain_write(tgt, q, tag, sector, iod.nr_sectors).await;

    if wp == zone_end {
        let mut z = tgt.get_zone_mut(iod.start_sector);

        if z.cond != BLK_ZONE_COND_FULL {
            if z.cond == BLK_ZONE_COND_EXP_OPEN {
                let mut data = tgt.data.write().unwrap();

                data.nr_zones_exp_open -= 1;
            } else if z.cond == BLK_ZONE_COND_IMP_OPEN {
                let mut data = tgt.data.write().unwrap();

                data.nr_zones_imp_open -= 1;
            }
            z.cond = BLK_ZONE_COND_FULL;
        }
    }
    trace!(
        "write done(append {:1} lba {:06x}-{:04}), zone: no {:4} wp {:06x} end {:06x} sector {:06x} sects {:03x}",
        append,
        iod.start_sector,
        iod.nr_sectors,
        tgt.get_zone_no(iod.start_sector),
        wp,
        zone_end, sector, ret >> 9,
    );

    (sector, ret)
}

async fn zoned_handle_io(tgt: &ZonedTgt, q: &UblkQueue<'_>, tag: u16) -> (i32, u64) {
    let iod = q.get_iod(tag);
    let mut sector: u64 = 0;
    let bytes;
    let op = iod.op_flags & 0xff;

    trace!(
        "=>tag {:3} ublk op {:2}, lba {:6x}-{:3} zno {}",
        tag,
        op,
        iod.start_sector,
        iod.nr_sectors,
        tgt.get_zone_no(iod.start_sector)
    );

    match op {
        UBLK_IO_OP_FLUSH => bytes = handle_flush(tgt, q, tag, iod),
        UBLK_IO_OP_READ => {
            bytes = handle_read(tgt, q, tag, iod).await;
        }
        UBLK_IO_OP_WRITE => {
            (_, bytes) = handle_write(tgt, q, tag, iod, false).await;
        }
        UBLK_IO_OP_ZONE_APPEND => {
            (sector, bytes) = handle_write(tgt, q, tag, iod, true).await;
        }
        UBLK_IO_OP_REPORT_ZONES => {
            bytes = handle_report_zones(tgt, q, tag, iod) as i32;
        }
        UBLK_IO_OP_ZONE_RESET
        | UBLK_IO_OP_ZONE_RESET_ALL
        | UBLK_IO_OP_ZONE_OPEN
        | UBLK_IO_OP_ZONE_CLOSE
        | UBLK_IO_OP_ZONE_FINISH => bytes = handle_mgmt(tgt, q, tag, iod),
        _ => {
            return (-libc::EINVAL, 0);
        }
    }

    trace!(
        "<=tag {:3} ublk op {:2}, lba {:6x}-{:3} zno {} done {:4}",
        tag,
        op,
        iod.start_sector,
        iod.nr_sectors,
        tgt.get_zone_no(iod.start_sector),
        bytes >> 9
    );

    (bytes, sector)
}

#[derive(clap::Args, Debug)]
pub(crate) struct ZonedAddArgs {
    #[command(flatten)]
    pub gen_arg: super::args::GenAddArgs,

    ///backing file of Zoned; So far, only None is allowed, and
    ///support ramdisk only
    #[clap(long, default_value = None)]
    path: Option<PathBuf>,

    /// How many megabytes(MB) of the whole zoned device
    #[clap(long, default_value_t = 1024)]
    size: u32,

    ///zone size, unit is megabytes(MB)
    #[clap(long, default_value_t = 256)]
    zone_size: u32,

    /// How many conventioanl zones starting from sector 0
    #[clap(long, default_value_t = 2)]
    conv_zones: u32,
}

pub(crate) fn ublk_add_zoned(
    ctrl: UblkCtrl,
    opt: Option<ZonedAddArgs>,
    comm_arc: &Arc<crate::DevIdComm>,
) -> anyhow::Result<i32> {
    //It doesn't make sense to support recovery for zoned_ramdisk
    let (size, zone_size, conv_zones) = match opt {
        Some(ref o) => {
            if o.gen_arg.user_recovery {
                return Err(anyhow::anyhow!("zoned(ramdisk) can't support recovery\n"));
            }

            if o.path.is_some() {
                return Err(anyhow::anyhow!("only support ramdisk now\n"));
            } else {
                (
                    ((o.size as u64) << 20),
                    ((o.zone_size as u64) << 20),
                    o.conv_zones,
                )
            }
        }
        None => return Err(anyhow::anyhow!("invalid parameter")),
    };

    if size < zone_size {
        return Err(anyhow::anyhow!("size is less than zone size\n"));
    }

    if u64::from((conv_zones as u64) * zone_size) >= size {
        return Err(anyhow::anyhow!("Too many conventioanl zones"));
    }

    let tgt_init = |dev: &mut UblkDev| {
        dev.set_default_params(size);
        dev.tgt.params.types |= libublk::sys::UBLK_PARAM_TYPE_ZONED;
        dev.tgt.params.basic.chunk_sectors = (zone_size >> 9) as u32;
        dev.tgt.params.zoned = libublk::sys::ublk_param_zoned {
            max_open_zones: 0,
            max_active_zones: 0,
            max_zone_append_sectors: (zone_size >> 9) as u32,
            ..Default::default()
        };

        if let Some(ref o) = opt {
            o.gen_arg.apply_block_size(dev);
            o.gen_arg.apply_read_only(dev);
        }

        Ok(())
    };

    let zoned_tgt = Arc::new(ZonedTgt::new(size, zone_size, conv_zones));
    let depth = ctrl.dev_info().queue_depth;

    match ctrl.get_driver_features() {
        Some(f) => {
            if (f & (libublk::sys::UBLK_F_ZONED as u64)) == 0 {
                return Err(anyhow::anyhow!("zoned isn't supported until v6.6 kernel"));
            }
        }
        _ => return Err(anyhow::anyhow!("zoned isn't supported until v6.6 kernel")),
    }

    let q_handler = move |qid: u16, dev: &UblkDev| {
        let q_rc = Rc::new(UblkQueue::new(qid, dev).unwrap());
        let exe = smol::LocalExecutor::new();
        let mut f_vec = Vec::new();

        //// `q_handler` closure implements Clone()
        let ztgt_q = Rc::new(&zoned_tgt);

        for tag in 0..depth {
            let q = q_rc.clone();
            let ztgt_io = ztgt_q.clone();

            f_vec.push(exe.spawn(async move {
                let mut lba = 0_u64;
                let mut cmd_op = libublk::sys::UBLK_U_IO_FETCH_REQ;
                let mut res = 0;
                loop {
                    let cmd_res = q.submit_io_cmd(tag, cmd_op, lba as *mut u8, res).await;
                    if cmd_res == libublk::sys::UBLK_IO_RES_ABORT {
                        break;
                    }

                    (res, lba) = zoned_handle_io(&ztgt_io, &q, tag).await;
                    cmd_op = libublk::sys::UBLK_U_IO_COMMIT_AND_FETCH_REQ;
                }
            }));
        }
        ublk_wait_and_handle_ios(&exe, &q_rc);
        smol::block_on(async { futures::future::join_all(f_vec).await });
    };

    let comm = comm_arc.clone();
    ctrl.run_target(tgt_init, q_handler, move |ctrl: &_| {
        comm.send_dev_id(ctrl.dev_info().dev_id).unwrap();
    })
    .unwrap();

    Ok(0)
}
