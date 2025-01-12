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
struct ZoneState {
    cond: libublk::sys::blk_zone_cond,
    wp: u64,
}

#[derive(Debug, Default)]
struct Zone {
    r#type: libublk::sys::blk_zone_type,
    start: u64,
    len: u32,
    capacity: u32,
    stat: RwLock<ZoneState>,
}

impl Zone {
    #[inline(always)]
    fn get_stat_mut(&self) -> std::sync::RwLockWriteGuard<'_, ZoneState> {
        self.stat.write().unwrap()
    }

    #[inline(always)]
    fn get_stat(&self) -> std::sync::RwLockReadGuard<'_, ZoneState> {
        self.stat.read().unwrap()
    }
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
    _size: u64,
    start: u64,

    zone_size: u64,
    _zone_capacity: u64,
    zone_nr_conv: u32,
    zone_max_open: u32,
    zone_max_active: u32,
    nr_zones: u32,

    zones: Vec<Zone>,
    data: RwLock<TgtData>,
    _buf: IoBuf<u8>,
}

impl ZonedTgt {
    #[allow(clippy::uninit_vec)]
    fn new(size: u64, zone_size: u64, conv_zones: usize) -> ZonedTgt {
        let _buf = IoBuf::<u8>::new(size as usize);
        let buf_addr = _buf.as_mut_ptr() as u64;
        let zone_cap = zone_size >> 9;
        let nr_zones = size / zone_size;

        let zones: Vec<Zone> = (0..nr_zones as usize)
            .map(|i| Zone {
                capacity: zone_cap as u32,
                len: (zone_size >> 9) as u32,
                r#type: if i < conv_zones {
                    BLK_ZONE_TYPE_CONVENTIONAL
                } else {
                    BLK_ZONE_TYPE_SEQWRITE_REQ
                },
                start: (i as u64) * (zone_size >> 9),
                stat: RwLock::new(ZoneState {
                    wp: (i as u64) * (zone_size >> 9),
                    cond: if i < conv_zones {
                        BLK_ZONE_COND_NOT_WP
                    } else {
                        BLK_ZONE_COND_EMPTY
                    },
                }),
            })
            .collect();

        ZonedTgt {
            _size: size,
            start: buf_addr,
            zone_size,
            _zone_capacity: zone_size,
            zone_max_open: 0,
            zone_max_active: 0,
            nr_zones: nr_zones as u32,
            data: RwLock::new(TgtData {
                ..Default::default()
            }),
            zone_nr_conv: conv_zones.try_into().unwrap(),
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
    fn get_zone(&self, sector: u64) -> &Zone {
        let zno = self.get_zone_no(sector);

        &self.zones[zno as usize]
    }

    #[inline(always)]
    fn get_zone_cond(&self, sector: u64) -> libublk::sys::blk_zone_cond {
        let z = self.get_zone(sector);
        let zs = z.get_stat();

        zs.cond
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

    fn close_imp_open_zone(&self) -> anyhow::Result<()> {
        let mut zno = self.get_imp_close_zone_no();
        if zno >= self.nr_zones {
            zno = self.zone_nr_conv;
        }

        for _ in self.zone_nr_conv..self.nr_zones {
            let z = &self.zones[zno as usize];

            zno += 1;
            if zno >= self.nr_zones {
                zno = self.zone_nr_conv;
            }

            // the current zone may be locked already, so have
            // to use try_lock
            match z.stat.try_write() {
                Ok(mut zs) => {
                    if zs.cond == BLK_ZONE_COND_IMP_OPEN {
                        self.__close_zone(&z, &mut zs)?;
                        self.set_imp_close_zone_no(zno);
                        return Ok(());
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn check_active(&self) -> anyhow::Result<i32> {
        if self.zone_max_active == 0 {
            return Ok(0);
        }

        let data = self.data.read().unwrap();
        if data.nr_zones_exp_open + data.nr_zones_imp_open + data.nr_zones_closed
            < self.zone_max_active
        {
            return Ok(0);
        }

        Err(anyhow::anyhow!("check active busy"))
    }

    fn check_open(&self) -> anyhow::Result<i32> {
        if self.zone_max_open == 0 {
            return Ok(0);
        }

        let (exp_open, imp_open) = self.get_open_zones();

        if exp_open + imp_open < self.zone_max_open {
            return Ok(0);
        }

        if imp_open > 0 {
            self.check_active()?;
            self.close_imp_open_zone()?;
            return Ok(0);
        }

        Err(anyhow::anyhow!("check open busy"))
    }

    fn check_zone_resources(&self, cond: libublk::sys::blk_zone_cond) -> anyhow::Result<i32> {
        match cond {
            BLK_ZONE_COND_EMPTY => {
                self.check_active()?;
                self.check_open()
            }
            BLK_ZONE_COND_CLOSED => self.check_open(),
            _ => Err(anyhow::anyhow!("check zone resource, wrong cond {}", cond)),
        }
    }

    fn zone_reset(&self, sector: u64) -> anyhow::Result<i32> {
        let z = self.get_zone(sector);
        let mut zs = z.get_stat_mut();

        if z.r#type == BLK_ZONE_TYPE_CONVENTIONAL {
            return Err(anyhow::anyhow!("reset conventional zone"));
        }

        match zs.cond {
            BLK_ZONE_COND_EMPTY | BLK_ZONE_COND_READONLY | BLK_ZONE_COND_OFFLINE => return Ok(0),
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
            _ => return Err(anyhow::anyhow!("bad zone cond {} in zone_reset", zs.cond)),
        }

        let start = z.start;
        zs.cond = BLK_ZONE_COND_EMPTY;
        zs.wp = start;

        self.discard_zone(self.get_zone_no(sector));

        Ok(0)
    }

    fn zone_open(&self, sector: u64) -> anyhow::Result<i32> {
        let z = self.get_zone(sector);
        let mut zs = z.get_stat_mut();

        if z.r#type == BLK_ZONE_TYPE_CONVENTIONAL {
            return Err(anyhow::anyhow!("open conventional zone"));
        }

        //fixme: add zone check
        match zs.cond {
            BLK_ZONE_COND_EXP_OPEN => return Ok(0), //already open
            BLK_ZONE_COND_EMPTY => {
                self.check_zone_resources(BLK_ZONE_COND_EMPTY)?;
                let mut data = self.data.write().unwrap();
                data.nr_zones_exp_open += 1;
            }
            BLK_ZONE_COND_IMP_OPEN => {
                let mut data = self.data.write().unwrap();
                data.nr_zones_imp_open -= 1;
                data.nr_zones_exp_open += 1;
            }
            BLK_ZONE_COND_CLOSED => {
                self.check_zone_resources(BLK_ZONE_COND_CLOSED)?;
                let mut data = self.data.write().unwrap();
                data.nr_zones_closed -= 1;
                data.nr_zones_exp_open += 1;
            }
            _ => return Err(anyhow::anyhow!("bad zone cond {} in zone_open", zs.cond)),
        };

        zs.cond = BLK_ZONE_COND_EXP_OPEN;
        Ok(0)
    }

    fn __close_zone(
        &self,
        z: &Zone,
        zs: &mut std::sync::RwLockWriteGuard<'_, ZoneState>,
    ) -> anyhow::Result<i32> {
        match zs.cond {
            BLK_ZONE_COND_CLOSED => return Ok(0),
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
            _ => return Err(anyhow::anyhow!("bad zone cond {} in __close_zone", zs.cond)),
        }

        if zs.wp == z.start {
            zs.cond = BLK_ZONE_COND_EMPTY;
        } else {
            zs.cond = BLK_ZONE_COND_CLOSED;

            let mut data = self.data.write().unwrap();
            data.nr_zones_closed += 1;
        }

        Ok(0)
    }

    fn zone_close(&self, sector: u64) -> anyhow::Result<i32> {
        let z = self.get_zone(sector);

        if z.r#type == BLK_ZONE_TYPE_CONVENTIONAL {
            return Err(anyhow::anyhow!("close conventional zone"));
        }

        let mut zs = z.get_stat_mut();
        self.__close_zone(&z, &mut zs)
    }

    fn zone_finish(&self, sector: u64) -> anyhow::Result<i32> {
        let z = self.get_zone(sector);
        let mut zs = z.get_stat_mut();

        if z.r#type == BLK_ZONE_TYPE_CONVENTIONAL {
            return Err(anyhow::anyhow!("finish conventional zone"));
        }

        match zs.cond {
            // finish operation on full is not an error
            BLK_ZONE_COND_FULL => {}
            BLK_ZONE_COND_EMPTY => {
                self.check_zone_resources(BLK_ZONE_COND_EMPTY)?;
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
                self.check_zone_resources(BLK_ZONE_COND_CLOSED)?;
                let mut data = self.data.write().unwrap();
                data.nr_zones_closed -= 1;
            }
            _ => return Err(anyhow::anyhow!("bad zone cond {}", zs.cond)),
        };

        zs.cond = BLK_ZONE_COND_FULL;
        zs.wp = z.start + z.len as u64;

        Ok(0)
    }

    fn discard_zone(&self, zon: u32) {
        unsafe {
            let off = zon as u64 * self.zone_size;

            libc::madvise(
                (self.start + off) as *mut libc::c_void,
                self.zone_size.try_into().unwrap(),
                libc::MADV_DONTNEED,
            );
        }
    }
}

fn handle_report_zones(
    tgt: &ZonedTgt,
    q: &UblkQueue,
    tag: u16,
    iod: &ublksrv_io_desc,
) -> anyhow::Result<i32> {
    let zsects = (tgt.zone_size >> 9) as u32;
    let zones = iod.nr_sectors; //union
    let zno = iod.start_sector / (zsects as u64);
    let blkz_sz = core::mem::size_of::<libublk::sys::blk_zone>() as u32;

    // USER_COPY is enabled, so we have to allocate buffer for report_zones
    let buf = IoBuf::<u8>::new((blkz_sz * zones) as usize);
    let buf_addr = buf.as_mut_ptr();
    let mut off = buf_addr as u64;

    for i in zno..(zno + zones as u64) {
        let zone = unsafe { &mut *(off as *mut libublk::sys::blk_zone) };
        let z = &tgt.zones[i as usize];
        let zs = z.get_stat();

        zone.capacity = z.capacity as u64;
        zone.len = z.len as u64;
        zone.start = z.start;
        zone.wp = zs.wp;
        zone.type_ = z.r#type as u8;
        zone.cond = zs.cond as u8;

        off += blkz_sz as u64;
    }

    let dsize = off - buf_addr as u64;
    let res = unsafe {
        let offset = UblkIOCtx::ublk_user_copy_pos(q.get_qid(), tag, 0);

        libc::pwrite(
            q.dev.tgt.fds[0],
            buf_addr as *const libc::c_void,
            dsize.try_into().unwrap(),
            offset.try_into().unwrap(),
        )
    };
    if res < 0 {
        Err(anyhow::anyhow!("pwrite on ublkc failure {}", res))
    } else {
        Ok(res as i32)
    }
}

fn handle_mgmt(
    tgt: &ZonedTgt,
    _q: &UblkQueue,
    _tag: u16,
    iod: &ublksrv_io_desc,
) -> anyhow::Result<i32> {
    if (iod.op_flags & 0xff) == UBLK_IO_OP_ZONE_RESET_ALL {
        for zno in tgt.zone_nr_conv..tgt.nr_zones {
            tgt.zone_reset((zno as u64) * (tgt.zone_size >> 9))?;
        }

        return Ok(0);
    }

    let cond = tgt.get_zone_cond(iod.start_sector);
    if cond == BLK_ZONE_COND_READONLY || cond == BLK_ZONE_COND_OFFLINE {
        return Err(anyhow::anyhow!("mgmt op on readonly or offline zone"));
    }

    match iod.op_flags & 0xff {
        UBLK_IO_OP_ZONE_RESET => tgt.zone_reset(iod.start_sector),
        UBLK_IO_OP_ZONE_OPEN => tgt.zone_open(iod.start_sector),
        UBLK_IO_OP_ZONE_CLOSE => tgt.zone_close(iod.start_sector),
        UBLK_IO_OP_ZONE_FINISH => tgt.zone_finish(iod.start_sector),
        _ => return Err(anyhow::anyhow!("unknow mgmt op")),
    }
}

async fn handle_read(
    tgt: &ZonedTgt,
    q: &UblkQueue<'_>,
    tag: u16,
    iod: &ublksrv_io_desc,
) -> anyhow::Result<i32> {
    let cond = tgt.get_zone_cond(iod.start_sector);
    if cond == BLK_ZONE_COND_OFFLINE {
        return Err(anyhow::anyhow!("read from offline zone"));
    }

    let bytes = (iod.nr_sectors << 9) as usize;
    let off = iod.start_sector << 9;

    let res = unsafe {
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
    };

    if res < 0 {
        Err(anyhow::anyhow!("handle_read failure {}", res))
    } else {
        Ok(res)
    }
}

async fn handle_plain_write(
    tgt: &ZonedTgt,
    q: &UblkQueue<'_>,
    tag: u16,
    start_sector: u64,
    nr_sectors: u32,
) -> anyhow::Result<i32> {
    let off = start_sector << 9;
    let bytes = (nr_sectors << 9) as usize;
    let offset = UblkIOCtx::ublk_user_copy_pos(q.get_qid(), tag, 0);

    let res = unsafe {
        //read data from /dev/ublkcN to our ram directly
        libc::pread(
            q.dev.tgt.fds[0],
            (tgt.start + off) as *mut libc::c_void,
            bytes,
            offset.try_into().unwrap(),
        ) as i32
    };

    if res < 0 {
        Err(anyhow::anyhow!("wrong pread on ublkc {}", res))
    } else {
        Ok(res)
    }
}

fn handle_flush(
    _tgt: &ZonedTgt,
    _q: &UblkQueue<'_>,
    _tag: u16,
    _iod: &libublk::sys::ublksrv_io_desc,
) -> anyhow::Result<i32> {
    Ok(0)
}

async fn handle_write(
    tgt: &ZonedTgt,
    q: &UblkQueue<'_>,
    tag: u16,
    iod: &libublk::sys::ublksrv_io_desc,
    append: bool,
) -> anyhow::Result<(u64, i32)> {
    let zno = tgt.get_zone_no(iod.start_sector);
    let z = tgt.get_zone(iod.start_sector);

    if zno < tgt.zone_nr_conv {
        if append {
            return Err(anyhow::anyhow!("write append on conventional zone"));
        }

        let res = handle_plain_write(tgt, q, tag, iod.start_sector, iod.nr_sectors).await?;
        return Ok((u64::MAX, res));
    }

    let zone_end = z.start + z.capacity as u64;
    let (wp, sector) = {
        let mut zs = z.get_stat_mut();
        let cond = zs.cond;

        if cond == BLK_ZONE_COND_FULL
            || cond == BLK_ZONE_COND_READONLY
            || cond == BLK_ZONE_COND_OFFLINE
        {
            return Err(anyhow::anyhow!("write on full/ro/offline zone"));
        }

        let sector = zs.wp;
        if !append && iod.start_sector != sector {
            return Err(anyhow::anyhow!("not write on write pointer"));
        }

        if zs.wp + iod.nr_sectors as u64 > zone_end {
            return Err(anyhow::anyhow!("write out of zone"));
        }

        if cond == BLK_ZONE_COND_CLOSED || cond == BLK_ZONE_COND_EMPTY {
            tgt.check_zone_resources(cond)?;

            let mut data = tgt.data.write().unwrap();
            if cond == BLK_ZONE_COND_CLOSED {
                data.nr_zones_closed -= 1;
                data.nr_zones_imp_open += 1;
            } else if cond == BLK_ZONE_COND_EMPTY {
                data.nr_zones_imp_open += 1;
            }

            if cond != BLK_ZONE_COND_EXP_OPEN {
                zs.cond = BLK_ZONE_COND_IMP_OPEN;
            }
        }
        zs.wp += iod.nr_sectors as u64;

        (zs.wp, sector)
    };

    let res = handle_plain_write(tgt, q, tag, sector, iod.nr_sectors).await?;
    if wp == zone_end {
        let mut zs = z.get_stat_mut();

        if zs.cond != BLK_ZONE_COND_FULL {
            if zs.cond == BLK_ZONE_COND_EXP_OPEN {
                let mut data = tgt.data.write().unwrap();

                data.nr_zones_exp_open -= 1;
            } else if zs.cond == BLK_ZONE_COND_IMP_OPEN {
                let mut data = tgt.data.write().unwrap();

                data.nr_zones_imp_open -= 1;
            }
            zs.cond = BLK_ZONE_COND_FULL;
        }
    }
    Ok((sector, res))
}

async fn zoned_handle_io(
    tgt: &ZonedTgt,
    q: &UblkQueue<'_>,
    tag: u16,
) -> anyhow::Result<(i32, u64)> {
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
        UBLK_IO_OP_FLUSH => bytes = handle_flush(tgt, q, tag, iod)?,
        UBLK_IO_OP_READ => bytes = handle_read(tgt, q, tag, iod).await?,
        UBLK_IO_OP_WRITE => (_, bytes) = handle_write(tgt, q, tag, iod, false).await?,
        UBLK_IO_OP_ZONE_APPEND => (sector, bytes) = handle_write(tgt, q, tag, iod, true).await?,
        UBLK_IO_OP_REPORT_ZONES => bytes = handle_report_zones(tgt, q, tag, iod)?,
        UBLK_IO_OP_ZONE_RESET
        | UBLK_IO_OP_ZONE_RESET_ALL
        | UBLK_IO_OP_ZONE_OPEN
        | UBLK_IO_OP_ZONE_CLOSE
        | UBLK_IO_OP_ZONE_FINISH => bytes = handle_mgmt(tgt, q, tag, iod)?,
        _ => return Err(anyhow::anyhow!("unknown ublk op")),
    }

    trace!(
        "<=tag {:3} ublk op {:2}, lba {:6x}-{:3} zno {} done {:4} sector {}",
        tag,
        op,
        iod.start_sector,
        iod.nr_sectors,
        tgt.get_zone_no(iod.start_sector),
        bytes >> 9,
        sector
    );

    Ok((bytes, sector))
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
                    o.conv_zones as usize,
                )
            }
        }
        None => return Err(anyhow::anyhow!("invalid parameter")),
    };

    if size < zone_size || (size % zone_size) != 0 {
        return Err(anyhow::anyhow!("size or zone_size is invalid\n"));
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

                    match zoned_handle_io(&ztgt_io, &q, tag).await {
                        Ok((r, l)) => {
                            cmd_op = libublk::sys::UBLK_U_IO_COMMIT_AND_FETCH_REQ;
                            res = r;
                            lba = l;
                        }
                        Err(e) => {
                            eprintln!("handle io failre {:?}", e);
                            break;
                        }
                    }
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
