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
use libublk::wait_and_handle_io_events;
use libublk::{ctrl::UblkCtrl, io::BufDesc, io::UblkDev, io::UblkIOCtx, io::UblkQueue, UblkError};

use anyhow::bail;
use bitflags::bitflags;
use io_uring::{opcode, types};
use libc::{c_void, memset, pread, pwrite};
use log::trace;
use nix::sys::eventfd::{EfdFlags, EventFd};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, RwLock, RwLockReadGuard as RLGuard, RwLockWriteGuard as WLGuard};

bitflags! {
    #[derive(Default)]
    struct ZoneFlags: u32 {
        const SEQ_ERROR = 0b00000001;
    }
}

#[derive(Debug, thiserror::Error)]
enum ZIOError<'a> {
    #[error("zone_max_open limit is reached in {0}")]
    MaxOpenOverflow(&'a str),

    #[error("zone_max_active limit is reached in {0}")]
    MaxActiveOverflow(&'a str),

    #[error("update seq zone failed")]
    UpdateSeq(String),
}

#[derive(Default)]
struct ZoneState {
    flags: ZoneFlags,
    cond: libublk::sys::blk_zone_cond,
    wp: u64,
}

impl ZoneState {
    #[inline(always)]
    fn set_seq_err(&mut self, msg: String) -> anyhow::Error {
        self.flags.insert(ZoneFlags::SEQ_ERROR);
        ZIOError::UpdateSeq(msg).into()
    }

    #[inline(always)]
    fn is_seq_err(&self) -> bool {
        self.flags.intersects(ZoneFlags::SEQ_ERROR)
    }

    #[inline(always)]
    fn clear_seq_err(&mut self) {
        self.flags.remove(ZoneFlags::SEQ_ERROR);
    }
}

#[derive(Default)]
struct Zone {
    r#type: libublk::sys::blk_zone_type,
    start: u64,
    len: u32,
    capacity: u32,
    stat: RwLock<ZoneState>,
}

impl Zone {
    #[inline(always)]
    fn get_stat_mut(&self) -> WLGuard<'_, ZoneState> {
        self.stat.write().unwrap()
    }

    #[inline(always)]
    fn get_stat(&self) -> RLGuard<'_, ZoneState> {
        self.stat.read().unwrap()
    }

    #[inline(always)]
    fn get_cond(&self) -> libublk::sys::blk_zone_cond {
        self.get_stat().cond
    }
}

#[derive(Debug, Default)]
struct TgtData {
    nr_zones_imp_open: u32,
    nr_zones_exp_open: u32,
    nr_zones_closed: u32,
    imp_close_zone_no: u32,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Default, Clone)]
struct TgtCfg {
    size: u64,
    zone_size: u64,
    pre_alloc_size: u64,
    zone_capacity: u64,
    zone_nr_conv: u32,
    zone_max_open: u32,
    zone_max_active: u32,
    base: Option<PathBuf>,
}

impl TgtCfg {
    const DEF_ZONE_PRE_ALLOC_MB: u32 = 256;
    #[allow(clippy::too_many_arguments)]
    fn new(
        s: u32,
        zs: u32,
        zc: u32,
        conv: u32,
        m_open: u32,
        m_act: u32,
        b: Option<PathBuf>,
        p: u32,
    ) -> Self {
        TgtCfg {
            size: (s as u64) << 20,
            zone_size: (zs as u64) << 20,
            zone_capacity: (zc as u64) << 20,
            zone_nr_conv: conv,
            zone_max_open: m_open,
            zone_max_active: m_act,
            base: b,
            pre_alloc_size: (p as u64) << 20,
        }
    }
    /// Check parameters from command line or `superblock.json`
    fn validate(&self) -> anyhow::Result<()> {
        if self.zone_size == 0
            || !self.zone_size.is_power_of_two()
            || self.size < self.zone_size
            || (self.size % self.zone_size) != 0
            || (self.zone_size % 512) != 0
        {
            return Err(anyhow::anyhow!("size or zone_size is invalid\n"));
        }

        if self.zone_capacity == 0
            || self.zone_capacity > self.zone_size
            || (self.zone_capacity % (1 << 20)) != 0
        {
            bail!(
                "zone_capacity {} is invalid: it has to be a multiple of 1MiB and not bigger than zone_size {}",
                self.zone_capacity,
                self.zone_size
            );
        }

        if ((self.zone_nr_conv as u64) * self.zone_size) >= self.size {
            return Err(anyhow::anyhow!("Too many conventioanl zones"));
        }

        // It is fine to be bigger than zone size, and preallocation is
        // always capped by zone capacity.
        if !super::args::is_power2_of(self.pre_alloc_size, 4096) {
            bail!(
                "pre_alloc_size {} is invalid: it has to be power of 2 and not less than 4096",
                self.pre_alloc_size
            );
        }
        Ok(())
    }

    /// Max open & active zones can't be enforced if they aren't less than
    /// sequential zones, and max open zones can't be bigger than max
    /// active zones. Adjust them as null_blk does, otherwise the limits are
    /// rejected by kernel.
    ///
    /// Has to be called after `validate()`.
    fn adjust_zone_limits(&mut self) {
        let nr_seq = (self.size / self.zone_size) as u32 - self.zone_nr_conv;

        if self.zone_max_active >= nr_seq {
            self.zone_max_active = 0;
        }
        if self.zone_max_active != 0 && self.zone_max_open > self.zone_max_active {
            self.zone_max_open = self.zone_max_active;
        } else if self.zone_max_open >= nr_seq {
            self.zone_max_open = 0;
        }
    }

    /// Parse a size argument (default unit MiB) into whole MiB; sizes
    /// that aren't a multiple of 1MiB would otherwise be truncated.
    fn parse_mib(name: &str, val: &str) -> anyhow::Result<u32> {
        let cfg = parse_size::Config::new().with_default_factor(1_048_576);
        let bytes = cfg.parse_size(val)?;

        if bytes % (1 << 20) != 0 {
            bail!("{} {} is not a multiple of 1MiB", name, val);
        }
        u32::try_from(bytes >> 20).map_err(|_| anyhow::anyhow!("{} {} is too big", name, val))
    }

    fn from_argument(a: &ZonedAddArgs) -> anyhow::Result<Self> {
        let p = match a.path.clone() {
            Some(p) => {
                if p.is_absolute() {
                    Some(p)
                } else {
                    Some(a.gen_arg.build_abs_path(p))
                }
            }
            _ => None,
        };
        let s = Self::parse_mib("size", &a.size)?;
        let zs = Self::parse_mib("zone_size", &a.zone_size)?;
        // zone capacity is same with zone size if it isn't specified
        let zc = match a.zone_capacity {
            Some(ref c) => Self::parse_mib("zone_capacity", c)?,
            None => zs,
        };
        let za = Self::parse_mib("pre_alloc_size", &a.pre_alloc_size)?;
        let ma_zones = a.max_active_zones;
        let tcfg = Self::new(s, zs, zc, a.conv_zones, a.max_open_zones, ma_zones, p, za);

        Ok(tcfg)
    }

    fn get_pre_alloc_size(zo: Option<&ZonedAddArgs>) -> anyhow::Result<u32> {
        let za = match zo {
            Some(a) => Self::parse_mib("pre_alloc_size", &a.pre_alloc_size)?,
            _ => Self::DEF_ZONE_PRE_ALLOC_MB,
        };
        Ok(za)
    }

    fn save_file_json(&self, p: &PathBuf) -> anyhow::Result<()> {
        let json_string = serde_json::to_string_pretty(self)?;
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(p)?;
        f.write_all(json_string.as_bytes())?;
        let _ = f.flush();
        Ok(())
    }

    fn is_parent_of(parent: &PathBuf, child: &PathBuf) -> bool {
        match child.parent() {
            Some(parent_dir) => parent_dir == parent,
            None => false,
        }
    }

    fn from_file_json(p: &PathBuf, zo: Option<&ZonedAddArgs>) -> anyhow::Result<Self> {
        let f = OpenOptions::new().read(true).open(p)?;
        let reader = BufReader::new(f);
        let mut cfg: Self = serde_json::from_reader(reader)?;

        let exp = match cfg.base {
            Some(ref base) => Self::is_parent_of(base, p),
            None => false,
        };

        if !exp {
            bail!("base isn't match with --path {}", p.display());
        }

        // pre-alloc-size can be overrided from command line
        cfg.pre_alloc_size = (Self::get_pre_alloc_size(zo)? as u64) << 20;
        Ok(cfg)
    }
    fn from_file(p: &PathBuf, zo: Option<&ZonedAddArgs>) -> anyhow::Result<Self> {
        let first_line = BufReader::new(File::open(p)?).lines().next();
        if let Some(Ok(l)) = first_line {
            let n: Vec<u32> = l
                .split_whitespace()
                .map(|s| s.parse().expect("fail"))
                .collect();
            let base = p.parent().expect("no parent?").to_path_buf();
            let za = Self::get_pre_alloc_size(zo)?;
            let c = TgtCfg::new(n[0], n[1], n[1], n[2], n[3], n[4], Some(base), za);
            return Ok(c);
        }
        anyhow::bail!("faile to create TgtCfg from file")
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ZoneJson {
    path: PathBuf,
}

struct ZonedTgt {
    start: u64,
    nr_zones: u32,
    zones: Vec<Zone>,
    data: RwLock<TgtData>,
    _buf: Option<IoBuf<u8>>,
    zfiles: Vec<File>,
    cfg: TgtCfg,
    // lock of the base directory, which is released when it is closed
    _base_lock: Option<File>,
    // queue of the flush thread, each flush sends the eventfd to be
    // written when it is done
    flush_tx: Option<Sender<Arc<EventFd>>>,
}

/// Lock the base directory, which is owned by one zoned device only. The
/// lock is released when the returned file is closed.
fn lock_base_dir(dir: &std::path::Path) -> anyhow::Result<File> {
    let h = OpenOptions::new().read(true).open(dir)?;
    let res = unsafe { libc::flock(h.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if res != 0 {
        anyhow::bail!(
            "fail to lock {}: {}",
            dir.display(),
            std::io::Error::last_os_error()
        );
    }
    Ok(h)
}

impl ZonedTgt {
    /// All flushes of this device are handled by one thread, so syncfs()
    /// isn't called from the queue threads.
    ///
    /// The thread takes all the flushes which are waiting, calls syncfs()
    /// once for them, then writes the eventfd of each one: 1 for success,
    /// 1 + errno for failure. One flush which comes when syncfs() is
    /// running has to wait for the next round, since the running one may
    /// not cover the writes completed before this flush.
    fn flush_thread_fn(zf: File, rx: Receiver<Arc<EventFd>>) {
        // syncfs() used to be called from the queue thread, which is one
        // IO flusher, and the same is needed here: memory reclaim in this
        // thread must not wait for IO of this ublk device.
        UblkCtrl::init_queue_thread();

        while let Ok(first) = rx.recv() {
            let mut efds = vec![first];
            while let Ok(efd) = rx.try_recv() {
                efds.push(efd);
            }

            let val: u64 = if unsafe { libc::syncfs(zf.as_raw_fd()) } < 0 {
                1 + std::io::Error::last_os_error()
                    .raw_os_error()
                    .unwrap_or(libc::EIO) as u64
            } else {
                1
            };

            for efd in efds {
                // can't fail: the counter is zero and the value is small
                let _ = nix::unistd::write(&*efd, &val.to_ne_bytes());
            }
        }
    }

    /// Flush is handled from the queue thread if this thread can't be
    /// started.
    fn start_flush_thread(&mut self) {
        // Not one dup of zfiles[0]: syncfs() reports one writeback error
        // once for each open file, and the flush handled from the queue
        // thread uses zfiles[0].
        let zf = match self.back_file_path(0).and_then(|p| Ok(File::open(p)?)) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("zoned: no flush thread, fail to open zone file: {}", e);
                return;
            }
        };
        let (tx, rx) = channel();

        match std::thread::Builder::new()
            .name("zoned_flush".to_string())
            .spawn(move || Self::flush_thread_fn(zf, rx))
        {
            Ok(_) => self.flush_tx = Some(tx),
            Err(e) => eprintln!("zoned: fail to start flush thread: {}", e),
        }
    }

    /// Queue one flush to the flush thread, and return the eventfd which
    /// is written when this flush is done.
    fn queue_flush(&self) -> Option<Arc<EventFd>> {
        let tx = self.flush_tx.as_ref()?;
        let efd = Arc::new(EventFd::from_value_and_flags(0, EfdFlags::EFD_CLOEXEC).ok()?);

        tx.send(efd.clone()).ok()?;
        Some(efd)
    }

    fn install_zone_files(&mut self, new_dev: bool) -> anyhow::Result<()> {
        for i in 0..self.nr_zones as usize {
            let path = self.back_file_path(i.try_into().unwrap())?;
            let f = OpenOptions::new()
                .create(new_dev)
                .read(true)
                .write(true)
                .open(path)?;
            unsafe {
                libc::fcntl(f.as_raw_fd(), libc::F_SETFL, libc::O_DIRECT);
            }
            self.zfiles.push(f);
        }
        Ok(())
    }

    fn setup_zone_backing_file(&mut self, zno: u32) -> anyhow::Result<()> {
        if zno < self.cfg.zone_nr_conv.try_into().unwrap() {
            self.back_file_truncate(zno, self.cfg.zone_size)
        } else {
            self.back_file_truncate(zno, 0)
        }
    }

    fn __update_seq_zone(&self, z: &Zone, zs: &mut WLGuard<'_, ZoneState>) -> anyhow::Result<()> {
        let zno = self.get_zone_no(z.start);
        let secs = match self.back_file_size(zno) {
            Ok(s) => s >> 9,
            Err(e) => {
                bail!(zs.set_seq_err(format!("read zone {zno} file size failed {e:?}")));
            }
        };
        if secs > (z.capacity as u64) {
            anyhow::bail!(ZIOError::UpdateSeq("file size 2big".to_string()));
        }

        let old_cond = zs.cond;
        if secs == 0 {
            zs.cond = BLK_ZONE_COND_EMPTY;
            zs.wp = z.start;
        } else if secs == (z.capacity as u64) {
            zs.cond = BLK_ZONE_COND_FULL;
            zs.wp = z.start + z.len as u64;
        } else {
            zs.cond = BLK_ZONE_COND_CLOSED;
            zs.wp = z.start + secs;
        }
        self.account_cond_change(old_cond, zs.cond);
        zs.clear_seq_err();
        Ok(())
    }

    /// Update open & closed zones counters after zone condition is changed
    /// from `old` to `new`, both have to be called with the zone locked.
    fn account_cond_change(
        &self,
        old: libublk::sys::blk_zone_cond,
        new: libublk::sys::blk_zone_cond,
    ) {
        if old == new {
            return;
        }

        let mut data = self.data.write().unwrap();
        match old {
            BLK_ZONE_COND_IMP_OPEN => data.nr_zones_imp_open -= 1,
            BLK_ZONE_COND_EXP_OPEN => data.nr_zones_exp_open -= 1,
            BLK_ZONE_COND_CLOSED => data.nr_zones_closed -= 1,
            _ => {}
        }
        match new {
            BLK_ZONE_COND_IMP_OPEN => data.nr_zones_imp_open += 1,
            BLK_ZONE_COND_EXP_OPEN => data.nr_zones_exp_open += 1,
            BLK_ZONE_COND_CLOSED => data.nr_zones_closed += 1,
            _ => {}
        }
    }

    fn update_seq_zone(&self, zno: u32) -> anyhow::Result<()> {
        let z = &self.zones[zno as usize];
        let mut zs = z.get_stat_mut();

        self.__update_seq_zone(z, &mut zs)
    }

    #[allow(clippy::uninit_vec)]
    fn new(cfg: TgtCfg, new_dev: bool, base_lock: Option<File>) -> anyhow::Result<ZonedTgt> {
        let ram_backed = cfg.base.is_none();
        let _buf = if ram_backed {
            Some(IoBuf::<u8>::new(cfg.size as usize))
        } else {
            None
        };
        let buf_addr = match _buf {
            None => 0,
            Some(ref val) => val.as_mut_ptr() as u64,
        };
        let zone_cap = cfg.zone_capacity >> 9;
        let nr_zones = cfg.size / cfg.zone_size;

        let zones: Vec<Zone> = (0..nr_zones as usize)
            .map(|i| Zone {
                // only sequential zone can have smaller capacity than its size
                capacity: if i < cfg.zone_nr_conv as usize {
                    (cfg.zone_size >> 9) as u32
                } else {
                    zone_cap as u32
                },
                len: (cfg.zone_size >> 9) as u32,
                r#type: if i < cfg.zone_nr_conv as usize {
                    BLK_ZONE_TYPE_CONVENTIONAL
                } else {
                    BLK_ZONE_TYPE_SEQWRITE_REQ
                },
                start: (i as u64) * (cfg.zone_size >> 9),
                stat: RwLock::new(ZoneState {
                    wp: (i as u64) * (cfg.zone_size >> 9),
                    cond: if i < cfg.zone_nr_conv as usize {
                        BLK_ZONE_COND_NOT_WP
                    } else {
                        BLK_ZONE_COND_EMPTY
                    },
                    flags: ZoneFlags::empty(),
                }),
            })
            .collect();

        let mut dev = ZonedTgt {
            start: buf_addr,
            nr_zones: nr_zones as u32,
            data: RwLock::new(TgtData {
                ..Default::default()
            }),
            zones,
            _buf,
            zfiles: Vec::new(),
            cfg,
            _base_lock: base_lock,
            flush_tx: None,
        };

        if !ram_backed {
            // the base directory is locked already if `superblock.json` is
            // read or created for adding device
            if dev._base_lock.is_none() {
                if let Some(ref dir) = dev.cfg.base {
                    dev._base_lock = Some(lock_base_dir(dir)?);
                }
            }
            dev.install_zone_files(new_dev)?;
            for zno in 0..dev.nr_zones as usize {
                if new_dev {
                    dev.setup_zone_backing_file(zno as u32)?;
                }
                // closed zones are counted when updating zone
                if zno >= dev.cfg.zone_nr_conv.try_into().unwrap() {
                    dev.update_seq_zone(zno as u32)?;
                }
            }
            dev.start_flush_thread();
        }

        Ok(dev)
    }

    #[inline(always)]
    fn ram_backed(&self) -> bool {
        self.cfg.base.is_none()
    }

    #[inline(always)]
    fn __get_zone_fd(&self, zno: usize) -> i32 {
        self.zfiles[zno].as_raw_fd()
    }

    #[inline(always)]
    fn get_zone_fd(&self, sector: u64) -> i32 {
        let zno = self.get_zone_no(sector) as usize;
        self.__get_zone_fd(zno)
    }

    #[inline(always)]
    fn get_zone_no(&self, sector: u64) -> u32 {
        let zsects = (self.cfg.zone_size >> 9) as u32;
        (sector / (zsects as u64)) as u32
    }

    #[inline(always)]
    fn get_zone(&self, sector: u64) -> &Zone {
        &self.zones[self.get_zone_no(sector) as usize]
    }

    #[inline(always)]
    fn get_zone_cond(&self, sector: u64) -> libublk::sys::blk_zone_cond {
        self.get_zone(sector).get_cond()
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

    fn back_file_path(&self, zno: u32) -> anyhow::Result<PathBuf> {
        match self.cfg.base {
            Some(ref p) => {
                let mut path = p.clone();
                let t = if zno < self.cfg.zone_nr_conv {
                    "conv"
                } else {
                    "seq"
                };
                path = path.join(format!("{t}-{zno:06}"));
                Ok(path)
            }
            None => Err(anyhow::anyhow!("base dir doesn't exist")),
        }
    }

    fn back_file_size(&self, zno: u32) -> anyhow::Result<u64> {
        let fd = self.__get_zone_fd(zno as usize);
        let mut file_stat: libc::stat = unsafe { std::mem::zeroed() };
        let result = unsafe { libc::fstat(fd, &mut file_stat) };

        if result < 0 {
            Err(anyhow::anyhow!("fstat() failed"))
        } else {
            Ok(file_stat.st_size as u64)
        }
    }

    fn back_file_truncate(&self, zno: u32, size: u64) -> anyhow::Result<()> {
        let fd = self.__get_zone_fd(zno as usize);
        let result = unsafe { libc::ftruncate(fd, size as libc::off_t) };
        if result < 0 {
            Err(anyhow::anyhow!("fruncate() failed size {}", size))
        } else {
            Ok(())
        }
    }

    async fn back_file_preallocate(&self, q: &UblkQueue<'_>, sector: u64, nr_sects: u32) {
        let zfd = self.get_zone_fd(sector);
        let zsize = self.cfg.zone_size;
        let za_size = self.cfg.pre_alloc_size;
        let start = (sector << 9) & (zsize - 1);
        let end = start + ((nr_sects << 9) as u64);
        let za_start = super::args::round_up(start, za_size);
        // nothing can be written behind zone capacity
        let za_end = super::args::round_up(end, za_size).min(self.cfg.zone_capacity);

        if za_start >= za_end {
            return;
        }

        let sz = za_end - za_start;
        let sqe = opcode::Fallocate::new(types::Fd(zfd), sz)
            .offset(za_start)
            .mode(libc::FALLOC_FL_KEEP_SIZE)
            .build();
        let _ = q.ublk_submit_sqe(sqe).await;
    }

    fn close_imp_open_zone(&self) -> anyhow::Result<()> {
        let mut zno = self.get_imp_close_zone_no();
        if zno >= self.nr_zones {
            zno = self.cfg.zone_nr_conv;
        }

        for _ in self.cfg.zone_nr_conv..self.nr_zones {
            let z = &self.zones[zno as usize];

            zno += 1;
            if zno >= self.nr_zones {
                zno = self.cfg.zone_nr_conv;
            }

            // the current zone may be locked already, so have
            // to use try_lock
            if let Ok(mut zs) = z.stat.try_write() {
                if zs.cond == BLK_ZONE_COND_IMP_OPEN {
                    self.__close_zone(z, &mut zs)?;
                    self.set_imp_close_zone_no(zno);
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    fn check_active(&self) -> anyhow::Result<i32> {
        if self.cfg.zone_max_active == 0 {
            return Ok(0);
        }

        let data = self.data.read().unwrap();
        if data.nr_zones_exp_open + data.nr_zones_imp_open + data.nr_zones_closed
            < self.cfg.zone_max_active
        {
            return Ok(0);
        }
        anyhow::bail!(ZIOError::MaxActiveOverflow("check active busy"));
    }

    fn check_open(&self) -> anyhow::Result<i32> {
        if self.cfg.zone_max_open == 0 {
            return Ok(0);
        }

        let (exp_open, imp_open) = self.get_open_zones();
        if exp_open + imp_open < self.cfg.zone_max_open {
            return Ok(0);
        }

        if imp_open > 0 {
            self.check_active()?;
            self.close_imp_open_zone()?;
            return Ok(0);
        }
        anyhow::bail!(ZIOError::MaxOpenOverflow("check open busy"));
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

        let old_cond = zs.cond;
        match old_cond {
            BLK_ZONE_COND_EMPTY | BLK_ZONE_COND_READONLY | BLK_ZONE_COND_OFFLINE => return Ok(0),
            BLK_ZONE_COND_IMP_OPEN
            | BLK_ZONE_COND_EXP_OPEN
            | BLK_ZONE_COND_CLOSED
            | BLK_ZONE_COND_FULL => {}
            _ => return Err(anyhow::anyhow!("bad zone cond {} in zone_reset", zs.cond)),
        }

        // Zone condition isn't changed if discarding zone fails, so update
        // the counters after it succeeds.
        self.discard_zone(self.get_zone_no(sector), &mut zs)?;
        self.account_cond_change(old_cond, BLK_ZONE_COND_EMPTY);

        let start = z.start;
        zs.cond = BLK_ZONE_COND_EMPTY;
        zs.wp = start;
        zs.clear_seq_err();

        Ok(0)
    }

    fn zone_open(&self, sector: u64) -> anyhow::Result<i32> {
        let z = self.get_zone(sector);
        let mut zs = z.get_stat_mut();

        if z.r#type == BLK_ZONE_TYPE_CONVENTIONAL {
            return Err(anyhow::anyhow!("open conventional zone"));
        }

        if zs.is_seq_err() {
            self.__update_seq_zone(z, &mut zs)?;
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

    fn __close_zone(&self, z: &Zone, zs: &mut WLGuard<'_, ZoneState>) -> anyhow::Result<i32> {
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

        if zs.is_seq_err() {
            self.__update_seq_zone(z, &mut zs)?;
        }
        self.__close_zone(z, &mut zs)
    }

    fn zone_finish(&self, sector: u64) -> anyhow::Result<i32> {
        let z = self.get_zone(sector);
        let mut zs = z.get_stat_mut();

        if z.r#type == BLK_ZONE_TYPE_CONVENTIONAL {
            return Err(anyhow::anyhow!("finish conventional zone"));
        }

        let old_cond = zs.cond;
        match old_cond {
            // finish operation on full is not an error
            BLK_ZONE_COND_FULL | BLK_ZONE_COND_IMP_OPEN | BLK_ZONE_COND_EXP_OPEN => {}
            BLK_ZONE_COND_EMPTY | BLK_ZONE_COND_CLOSED => {
                self.check_zone_resources(old_cond)?;
            }
            _ => return Err(anyhow::anyhow!("bad zone cond {}", zs.cond)),
        };

        if !self.ram_backed() {
            let zno = self.get_zone_no(sector);
            // The zone file can't be bigger than zone capacity, and write
            // pointer is figured out from the file size when the device is
            // setup again.
            if let Err(e) = self.back_file_truncate(zno, self.cfg.zone_capacity) {
                bail!(zs.set_seq_err(format!("truncate zone {zno} file, err {e:?}")));
            }
        }
        // Zone condition isn't changed if truncating zone file fails, so
        // update the counters after it succeeds.
        self.account_cond_change(old_cond, BLK_ZONE_COND_FULL);
        zs.cond = BLK_ZONE_COND_FULL;
        zs.wp = z.start + z.len as u64;
        zs.clear_seq_err();

        Ok(0)
    }

    fn discard_zone(&self, zno: u32, zs: &mut WLGuard<'_, ZoneState>) -> anyhow::Result<i32> {
        if self.ram_backed() {
            unsafe {
                let off = zno as u64 * self.cfg.zone_size;

                libc::madvise(
                    (self.start + off) as *mut libc::c_void,
                    self.cfg.zone_size.try_into().unwrap(),
                    libc::MADV_DONTNEED,
                );
            }
        } else if let Err(e) = self.back_file_truncate(zno, 0) {
            bail!(zs.set_seq_err(format!("truncated zone {zno:?} file failed {e:?}")));
        }
        Ok(0)
    }
}

fn handle_report_zones(
    tgt: &ZonedTgt,
    q: &UblkQueue,
    tag: u16,
    iod: &ublksrv_io_desc,
) -> anyhow::Result<i32> {
    let zsects = (tgt.cfg.zone_size >> 9) as u32;
    let zones = iod.nr_sectors; //union
    let zno = iod.start_sector / (zsects as u64);
    let blkz_sz = core::mem::size_of::<libublk::sys::blk_zone>() as u32;

    // USER_COPY is enabled, so we have to allocate buffer for report_zones
    // and it has to be zeroed, since not all fields of `blk_zone` are filled
    let mut buf = IoBuf::<u8>::new((blkz_sz * zones) as usize);
    buf.zero_buf();
    let buf_addr = buf.as_mut_ptr();
    let mut off = buf_addr as u64;

    for i in zno..(zno + zones as u64) {
        let zone = unsafe { &mut *(off as *mut libublk::sys::blk_zone) };
        let z = &tgt.zones[i as usize];
        let mut zs = z.get_stat_mut();

        if zs.is_seq_err() {
            tgt.__update_seq_zone(z, &mut zs)?;
        }

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
        for zno in tgt.cfg.zone_nr_conv..tgt.nr_zones {
            tgt.zone_reset((zno as u64) * (tgt.cfg.zone_size >> 9))?;
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
        _ => Err(anyhow::anyhow!("unknow mgmt op")),
    }
}

fn handle_read_copy(
    fd: i32,
    bytes: usize,
    offset: u64,
    addr: *mut c_void,
    zero: bool,
) -> anyhow::Result<i32> {
    let res = unsafe {
        // make sure data is zeroed for reset zone
        if zero {
            let buf = IoBuf::<u8>::new(bytes);
            let mut _addr = buf.as_mut_ptr() as *mut c_void;

            memset(_addr, 0, bytes);
            pwrite(fd, _addr, bytes, offset.try_into().unwrap()) as i32
        } else {
            //write data to /dev/ublkcN from our ram directly
            pwrite(fd, addr, bytes, offset.try_into().unwrap()) as i32
        }
    };
    if res < 0 {
        Err(anyhow::anyhow!("wrong pwrite to ublkc {}", res))
    } else {
        Ok(res)
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
    let offset = UblkIOCtx::ublk_user_copy_pos(q.get_qid(), tag, 0);
    let fd = q.dev.tgt.fds[0];

    if !tgt.ram_backed() {
        let zfd = tgt.get_zone_fd(iod.start_sector);
        let buf = IoBuf::<u8>::new(bytes);
        let buf_addr = buf.as_mut_ptr() as *mut c_void;
        let zf_offset = (iod.start_sector << 9) & (tgt.cfg.zone_size - 1);

        //read to temp buffer from backed-file
        //
        // The backing file ends at the write pointer of this zone, so the
        // read is short if it covers anything behind. Read again after a
        // short read: 0 tells that the end of file is really reached, and
        // any other reason of the short read shows up as one failure.
        let mut done = 0_usize;
        while done < bytes {
            let sqe = opcode::Read::new(
                types::Fd(zfd),
                unsafe { (buf_addr as *mut u8).add(done) },
                (bytes - done) as u32,
            )
            .offset(zf_offset + done as u64)
            .build();
            let res = q.ublk_submit_sqe(sqe).await;
            if res < 0 {
                return Err(anyhow::anyhow!("io uring read failure {}", res));
            }
            if res == 0 {
                break;
            }
            done += res as usize;
        }
        // what is behind the end of file is read as zero
        if done < bytes {
            unsafe { memset(buf_addr.add(done), 0, bytes - done) };
        }
        //write data from temp buffer to ublkc request
        handle_read_copy(fd, bytes, offset, buf_addr, cond == BLK_ZONE_COND_EMPTY)
    } else {
        let off = iod.start_sector << 9;
        let addr = (tgt.start + off) as *mut c_void;

        //write data from ram-backed zoned buffer to ublkc request
        handle_read_copy(fd, bytes, offset, addr, cond == BLK_ZONE_COND_EMPTY)
    }
}

async fn handle_plain_write(
    tgt: &ZonedTgt,
    q: &UblkQueue<'_>,
    tag: u16,
    start_sector: u64,
    nr_sectors: u32,
) -> anyhow::Result<i32> {
    let bytes = (nr_sectors << 9) as usize;
    let offset = UblkIOCtx::ublk_user_copy_pos(q.get_qid(), tag, 0);
    let fd = q.dev.tgt.fds[0];

    let res = if !tgt.ram_backed() {
        let zfd = tgt.get_zone_fd(start_sector);
        let buf = IoBuf::<u8>::new(bytes);
        let buf_addr = buf.as_mut_ptr() as *mut c_void;
        let zf_offset = (start_sector << 9) & (tgt.cfg.zone_size - 1);

        // read data from ublk driver first
        let res = unsafe { pread(fd, buf_addr, bytes, offset.try_into().unwrap()) };
        if res < 0 {
            return Ok(res as i32);
        }

        // handle the write
        let sqe = opcode::Write::new(types::Fd(zfd), buf_addr as *const u8, bytes as u32)
            .offset(zf_offset)
            .build();
        q.ublk_submit_sqe(sqe).await
    } else {
        let off = start_sector << 9;
        let addr = (tgt.start + off) as *mut c_void;

        unsafe {
            //read data from /dev/ublkcN to the zoned ramdisk
            pread(fd, addr, bytes, offset.try_into().unwrap()) as i32
        }
    };

    Ok(res as i32)
}

async fn handle_flush(
    tgt: &ZonedTgt,
    q: &UblkQueue<'_>,
    _tag: u16,
    _iod: &libublk::sys::ublksrv_io_desc,
) -> anyhow::Result<i32> {
    if tgt.ram_backed() {
        return Ok(0);
    }

    // syncfs() may take long, and no other IO of this queue is handled
    // if it is called from the queue thread. So hand it to the flush
    // thread, and wait via io_uring for the eventfd written by it.
    let efd = match tgt.queue_flush() {
        Some(efd) => efd,
        None => {
            // no flush thread, or the flush can't be queued
            let res = unsafe { libc::syncfs(tgt.__get_zone_fd(0)) };

            return if res < 0 {
                Err(anyhow::anyhow!("fail to syncfs {}", res))
            } else {
                Ok(0)
            };
        }
    };

    let mut buf = [0u8; 8];
    let sqe = opcode::Read::new(types::Fd(efd.as_raw_fd()), buf.as_mut_ptr(), 8).build();
    let res = q.ublk_submit_sqe(sqe).await;

    if res != 8 {
        return Err(anyhow::anyhow!("fail to read flush eventfd {}", res));
    }
    match u64::from_ne_bytes(buf) {
        1 => Ok(0),
        val => Err(anyhow::anyhow!("fail to syncfs, errno {}", val - 1)),
    }
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

    if zno < tgt.cfg.zone_nr_conv {
        if append {
            return Err(anyhow::anyhow!("write append on conventional zone"));
        }

        let res = handle_plain_write(tgt, q, tag, iod.start_sector, iod.nr_sectors).await?;
        return Ok((u64::MAX, res));
    }

    let zone_end = z.start + z.capacity as u64;
    let (wp, sector) = {
        let mut zs = z.get_stat_mut();

        // Zone condition and write pointer have to be updated first, before
        // any of them is checked.
        if zs.is_seq_err() {
            tgt.__update_seq_zone(z, &mut zs)?;
        }
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

    // preallocate space for this zone
    if !tgt.ram_backed() {
        tgt.back_file_preallocate(q, sector, iod.nr_sectors).await;
    }

    let res = handle_plain_write(tgt, q, tag, sector, iod.nr_sectors).await?;
    if wp == zone_end {
        let mut zs = z.get_stat_mut();

        // The zone isn't locked when this write is in progress, so it may
        // have been closed, either explicitly or implicitly for opening
        // another zone, and it may have been finished or reset too.
        //
        // So account from its current condition, and leave the zone alone
        // if its write pointer has been moved.
        if zs.wp == zone_end && zs.cond != BLK_ZONE_COND_FULL {
            tgt.account_cond_change(zs.cond, BLK_ZONE_COND_FULL);
            zs.cond = BLK_ZONE_COND_FULL;
        }
    }

    if res < 0 {
        let mut zs = z.get_stat_mut();
        bail!(zs.set_seq_err(format!("append fail: {zno} {res}")));
    }

    Ok((sector, res))
}

async fn handle_queue_tag_async_zoned(
    q: Rc<UblkQueue<'_>>,
    ztgt_io: Rc<&Arc<ZonedTgt>>,
    tag: u16,
) -> Result<(), UblkError> {
    let mut lba = 0_u64;
    let mut res = 0;

    // Submit initial prep command
    q.submit_io_prep_cmd(tag, BufDesc::ZonedAppendLba(lba), res, None)
        .await?;

    loop {
        match zoned_handle_io(&ztgt_io, &q, tag).await {
            Ok((r, l)) => {
                res = r;
                lba = l;
            }
            Err(e) => {
                lba = 0;
                res = match e.downcast::<ZIOError>() {
                    Ok(ZIOError::MaxOpenOverflow(_)) => -libc::ETOOMANYREFS,
                    Ok(ZIOError::MaxActiveOverflow(_)) => -libc::EOVERFLOW,
                    Ok(ZIOError::UpdateSeq(_)) => -libc::EIO,
                    // Any other failure is still a failure of this io
                    // command only, which has to be completed.
                    Err(e) => {
                        eprintln!("zoned: io command of tag {} failed: {}", tag, e);
                        -libc::EIO
                    }
                };
            }
        }

        // Submit commit command and get result
        q.submit_io_commit_cmd(tag, BufDesc::ZonedAppendLba(lba), res)
            .await?;
    }
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
        UBLK_IO_OP_FLUSH => bytes = handle_flush(tgt, q, tag, iod).await?,
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

    #[clap(long,
        default_value = None,
        help = "work directory of file-backed Zoned, if `superblock` file exists,\n\
        parse parameters from this file and setup zoned; otherwise, create\n\
        it and store current parameters to this file.\n\
        Default: None(ram backed zoned)"
        )]
    path: Option<PathBuf>,

    #[clap(
        long,
        default_value = "1024MiB",
        help = "Size of the whole zoned device, default unit is MiB, with common\n\
        suffixes supported ([B|KiB|KB|MiB|MB|GiB|GB|TiB|TB])"
    )]
    size: String,

    #[clap(
        long,
        default_value = "256MiB",
        help = "zone size, default unit is MiB, with common suffixes supported\n\
        ([B|KiB|MiB|GiB]), has to be a power of 2 and at least 1MiB"
    )]
    zone_size: String,

    #[clap(
        long,
        help = "zone capacity of sequential zones, the writable part from the\n\
        start of each zone, default unit is MiB, with common suffixes supported\n\
        ([B|KiB|MiB|GiB]), has to be a multiple of 1MiB and can't be bigger\n\
        than zone size. Default: same as zone size"
    )]
    zone_capacity: Option<String>,

    #[clap(
        long,
        default_value = "256MiB",
        help = "how many bytes preallocated before appending data to zone, default\n\
        unit is MiB, with common suffixes supported([B|KiB|MiB|GiB]), has to\n\
        be power of 2, and preallocation is capped by zone capacity"
    )]
    pre_alloc_size: String,

    /// How many conventioanl zones starting from sector 0
    #[clap(long, default_value_t = 2)]
    conv_zones: u32,

    /// Max open zones allowed, 0 means no limit. It is capped by max
    /// active zones, and disabled if it isn't less than sequential zones
    #[clap(long, default_value_t = 0)]
    max_open_zones: u32,

    /// Max active zones allowed, 0 means no limit. It is disabled if it
    /// isn't less than sequential zones
    #[clap(long, default_value_t = 0)]
    max_active_zones: u32,
}

/// Return zoned parameters, if the device is one new one, and the locked
/// base directory for file-backed zoned.
fn parse_zone_params(zo: &ZonedAddArgs) -> anyhow::Result<(TgtCfg, bool, Option<File>)> {
    if zo.path.is_none() {
        if zo.gen_arg.user_recovery {
            return Err(anyhow::anyhow!("zoned(ramdisk) can't support recovery\n"));
        }

        Ok((TgtCfg::from_argument(zo)?, true, None))
    } else {
        let path = zo.path.clone().ok_or(PathBuf::new()).expect("path failure");
        let path = zo.gen_arg.build_abs_path(path);

        if !path.exists() {
            return Err(anyhow::anyhow!("base dir doesn't exist"));
        }
        // `superblock.json` can be created, so the directory has to be
        // locked first, otherwise the one of another live device may be
        // overwritten.
        let lock = Some(lock_base_dir(&path)?);
        let zf = path.join("superblock");
        let zfj = path.join("superblock.json");
        if zfj.exists() {
            Ok((TgtCfg::from_file_json(&zfj, Some(zo))?, false, lock))
        } else if zf.exists() {
            let c = TgtCfg::from_file(&zf, Some(zo))?;
            c.save_file_json(&zfj)?;
            std::fs::remove_file(zf)?;
            Ok((c, false, lock))
        } else {
            let mut c = TgtCfg::from_argument(zo)?;
            // don't leave bad parameters in `superblock.json`, which can't
            // be overridden from command line any more
            c.validate()?;
            c.adjust_zone_limits();
            c.save_file_json(&zfj)?;
            Ok((c, true, lock))
        }
    }
}

pub(crate) fn ublk_add_zoned(
    ctrl: UblkCtrl,
    opt: Option<ZonedAddArgs>,
    comm_arc: &Arc<crate::DevIdComm>,
) -> anyhow::Result<i32> {
    //It doesn't make sense to support recovery for zoned_ramdisk
    let (mut cfg, is_new, base_lock) = match opt {
        Some(ref o) => parse_zone_params(o)?,
        None => match ctrl.get_target_data_from_json() {
            Some(val) => {
                let zoned = &val["zoned"];
                let tgt_data: Result<ZoneJson, _> = serde_json::from_value(zoned.clone());

                match tgt_data {
                    Ok(t) => {
                        let p = t.path.join("superblock.json");
                        (TgtCfg::from_file_json(&p, None)?, false, None)
                    }
                    Err(_) => return Err(anyhow::anyhow!("wrong json data")),
                }
            }
            None => return Err(anyhow::anyhow!("invalid parameter")),
        },
    };

    cfg.validate()?;
    // `superblock.json` may be created before the limits are adjusted
    cfg.adjust_zone_limits();

    let cfg_i = cfg.clone();
    let tgt_init = |dev: &mut UblkDev| {
        dev.set_default_params(cfg_i.size);
        dev.tgt.params.types |= libublk::sys::UBLK_PARAM_TYPE_ZONED;
        dev.tgt.params.basic.chunk_sectors = (cfg_i.zone_size >> 9) as u32;
        dev.tgt.params.zoned = libublk::sys::ublk_param_zoned {
            max_open_zones: cfg_i.zone_max_open,
            max_active_zones: cfg_i.zone_max_active,
            max_zone_append_sectors: (cfg_i.zone_capacity >> 9) as u32,
            ..Default::default()
        };

        if let Some(ref o) = opt {
            o.gen_arg.apply_block_size(dev);
            o.gen_arg.apply_read_only(dev);
        }

        if let Some(c) = cfg_i.base {
            let val = serde_json::json!({"zoned": ZoneJson { path: c}});
            dev.set_target_json(val);
        }
        Ok(())
    };

    let zoned_tgt = Arc::new(ZonedTgt::new(cfg, is_new, base_lock)?);
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
        let exe_rc = Rc::new(smol::LocalExecutor::new());
        let exe = exe_rc.clone();
        let mut f_vec = Vec::new();

        //// `q_handler` closure implements Clone()
        let ztgt_q = Rc::new(&zoned_tgt);

        for tag in 0..depth {
            let q = q_rc.clone();
            let ztgt_io = ztgt_q.clone();

            f_vec.push(exe.spawn(async move {
                match handle_queue_tag_async_zoned(q, ztgt_io, tag).await {
                    Err(UblkError::QueueIsDown) | Ok(_) => {}
                    Err(e) => {
                        log::error!("handle_queue_tag_async_zoned failed for tag {}: {}", tag, e)
                    }
                }
            }));
        }

        smol::block_on(exe_rc.run(async move {
            let run_ops = || while exe.try_tick() {};
            let done = || f_vec.iter().all(|task| task.is_finished());

            if let Err(e) = wait_and_handle_io_events(&q_rc, Some(20), run_ops, done).await {
                log::error!("zoned queue wait_and_handle_io_events failed: {}", e);
            }
        }));
    };

    let comm = comm_arc.clone();
    // Return the failure to caller, so that the parent of daemon can be
    // notified, instead of waiting for the device forever.
    ctrl.run_target(tgt_init, q_handler, move |ctrl: &_| {
        comm.send_dev_id(ctrl.dev_info().dev_id).unwrap();
    })
    .map_err(|e| anyhow::anyhow!("run zoned target failed: {:?}", e))?;

    Ok(0)
}
