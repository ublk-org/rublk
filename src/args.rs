use crate::target_flags::*;
use clap::{Args, Subcommand};
use ilog::IntLog;
use libublk::{ctrl::UblkCtrl, io::UblkDev, UblkFlags};
use std::cell::RefCell;
use std::path::PathBuf;

#[derive(Args, Debug, Clone)]
pub(crate) struct GenAddArgs {
    /// device id, -1 means ublk driver assigns ID for us
    #[clap(long, short = 'n', default_value_t=-1)]
    pub number: i32,

    /// nr_hw_queues
    #[clap(long, short = 'q', default_value_t = 1)]
    pub queue: u32,

    /// queue depth
    #[clap(long, short = 'd', default_value_t = 128)]
    pub depth: u32,

    #[clap(
        long,
        short = 'b',
        default_value = "524288",
        help = "io buffer size, has to be aligned with PAGE_SIZE\n\
        and common suffixes supported ([B|KiB|MiB|GiB])"
    )]
    pub io_buf_size: String,

    /// enable user recovery
    #[clap(long, short = 'r', default_value_t = false)]
    pub user_recovery: bool,

    /// enable user copy
    #[clap(long, short = 'u', default_value_t = false)]
    pub user_copy: bool,

    /// enable unprivileged
    #[clap(long, short = 'p', default_value_t = false)]
    pub unprivileged: bool,

    /// quiet, don't dump device info
    #[clap(long, default_value_t = false)]
    pub quiet: bool,

    /// logical block size
    #[clap(long)]
    pub logical_block_size: Option<u32>,

    /// physical block size
    #[clap(long)]
    pub physical_block_size: Option<u32>,

    /// read only
    #[clap(long, short = 'o', default_value_t = false)]
    pub read_only: bool,

    /// start the device in foreground
    #[clap(long, default_value_t = false)]
    pub foreground: bool,

    #[clap(skip)]
    start_dir: RefCell<Option<PathBuf>>,
}

impl GenAddArgs {
    pub fn apply_block_size(&self, dev: &mut UblkDev) {
        if let Some(bs) = self.logical_block_size {
            dev.tgt.params.basic.logical_bs_shift = bs.log2() as u8;
        }
        if let Some(bs) = self.physical_block_size {
            dev.tgt.params.basic.physical_bs_shift = bs.log2() as u8;
        }
    }

    pub fn apply_read_only(&self, dev: &mut UblkDev) {
        if self.read_only {
            dev.tgt.params.basic.attrs |= libublk::sys::UBLK_ATTR_READ_ONLY;
        } else {
            dev.tgt.params.basic.attrs &= !libublk::sys::UBLK_ATTR_READ_ONLY;
        }
    }
}

use std::convert::TryFrom;

pub fn round_up<T>(x: T, y: T) -> T
where
    T: Copy + Into<u64> + TryFrom<u64>, // support u32, u64 and try conversion back
{
    let x: u64 = x.into();
    let y: u64 = y.into();
    assert!(y & (y - 1) == 0, "y isn't power_of_2");
    let result = (x + y - 1) & !(y - 1);

    T::try_from(result).unwrap_or_else(|_| {
        panic!("Overflow occurred during conversion from u64 to the original type");
    })
}

pub fn is_power2_of(input: u64, base: u64) -> bool {
    assert!((base & (base - 1)) == 0);

    let quotient = input / base;
    quotient > 0 && (quotient & (quotient - 1)) == 0
}

impl GenAddArgs {
    /// Return shared memory os id
    pub fn get_start_dir(&self) -> Option<PathBuf> {
        let dir = self.start_dir.borrow();

        (*dir).clone()
    }

    pub fn build_abs_path(&self, p: PathBuf) -> PathBuf {
        let parent = self.get_start_dir();

        if p.is_absolute() {
            p
        } else {
            match parent {
                None => p,
                Some(n) => n.join(p),
            }
        }
    }
    pub fn save_start_dir(&self) {
        let start_dir = match std::env::current_dir() {
            Ok(p) => Some(p),
            Err(_) => None,
        };

        let mut dir = self.start_dir.borrow_mut();
        *dir = start_dir;
    }

    pub fn new_ublk_ctrl(
        &self,
        name: &'static str,
        dev_flags: UblkFlags,
    ) -> anyhow::Result<UblkCtrl> {
        let mut ctrl_flags = if self.user_recovery {
            libublk::sys::UBLK_F_USER_RECOVERY
        } else {
            0
        };

        if name == "zoned" {
            ctrl_flags |= libublk::sys::UBLK_F_USER_COPY | libublk::sys::UBLK_F_ZONED;
        }

        if self.user_copy {
            ctrl_flags |= libublk::sys::UBLK_F_USER_COPY;
        }

        if self.unprivileged {
            ctrl_flags |= libublk::sys::UBLK_F_UNPRIVILEGED_DEV;
        }

        let mut gen_flags: u64 = 0;
        if self.quiet {
            gen_flags |= TGT_QUIET;
        }

        match self.logical_block_size {
            None | Some(512) | Some(1024) | Some(2048) | Some(4096) => {}
            _ => {
                anyhow::bail!("invalid logical block size");
            }
        }

        if let Some(pbs) = self.physical_block_size {
            if !is_power2_of(pbs as u64, 512) {
                anyhow::bail!("invalid physical block size");
            }

            if let Some(lbs) = self.logical_block_size {
                if lbs > pbs {
                    anyhow::bail!("invalid block size");
                }
            }
        }

        let buf_size = parse_size::parse_size(self.io_buf_size.clone())?;
        if !is_power2_of(buf_size, 4096) || buf_size > u32::MAX.into() {
            anyhow::bail!("invalid io buf size {}", buf_size);
        }

        Ok(libublk::ctrl::UblkCtrlBuilder::default()
            .name(name)
            .depth(self.depth.try_into().unwrap())
            .nr_queues(self.queue.try_into().unwrap())
            .id(self.number)
            .ctrl_flags(ctrl_flags.into())
            .ctrl_target_flags(gen_flags)
            .dev_flags(dev_flags)
            .io_buf_bytes(buf_size as u32)
            .build()
            .unwrap())
    }
}

#[derive(Args)]
pub(crate) struct DelArgs {
    /// device id, -1 means ublk driver assigns ID for us
    #[clap(long, short = 'n', default_value_t = -1)]
    pub number: i32,

    /// remove all ublk devices
    #[clap(long, short = 'a', default_value_t = false)]
    pub all: bool,

    /// remove device in async way. The same device id may not be
    /// reused after returning from async deletion
    #[clap(long, default_value_t = false)]
    pub r#async: bool,
}

#[derive(Args)]
pub(crate) struct UblkArgs {
    /// device id, -1 means ublk driver assigns ID for us
    #[clap(long, short = 'n', default_value_t = -1)]
    pub number: i32,
}

#[derive(Args)]
pub(crate) struct UblkFeaturesArgs {}

#[derive(Subcommand)]
pub(crate) enum AddCommands {
    /// Add loop target
    Loop(super::r#loop::LoopArgs),

    /// Add null target
    Null(super::null::NullAddArgs),

    /// Add zoned target, supported since linux kernel v6.6
    Zoned(super::zoned::ZonedAddArgs),

    /// Add qcow2 target
    Qcow2(super::qcow2::Qcow2Args),

    /// Add compress target
    Compress(super::compress::CompressAddArgs),
}

#[derive(Subcommand)]
pub(crate) enum Commands {
    /// Adds ublk target
    #[clap(subcommand)]
    Add(AddCommands),
    /// Deletes ublk target
    Del(DelArgs),
    /// Lists ublk targets
    List(UblkArgs),
    /// Recover ublk targets
    Recover(UblkArgs),
    /// Get supported features from ublk driver, supported since v6.5
    Features(UblkFeaturesArgs),
}
