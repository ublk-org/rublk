use crate::target_flags::*;
use clap::{Args, Subcommand};
use ilog::IntLog;
use libublk::{ctrl::UblkCtrl, io::UblkDev, UblkFlags};
use rand::Rng;
use std::cell::RefCell;
use std::io::{Error, ErrorKind};

#[derive(Args, Debug)]
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

    /// io buffer size, has to be aligned with PAGE_SIZE
    #[clap(long, short = 'b', default_value_t = 524288)]
    pub io_buf_size: u32,

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
    shm_id: RefCell<String>,

    #[clap(skip)]
    start_dir: RefCell<Option<std::path::PathBuf>>,
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

fn is_power2_of(input: u32, base: u32) -> bool {
    assert!((base & (base - 1)) == 0);

    let quotient = input / base;
    quotient > 0 && (quotient & (quotient - 1)) == 0
}

impl GenAddArgs {
    /// Return shared memory os id
    pub fn get_start_dir(&self) -> Option<std::path::PathBuf> {
        let dir = self.start_dir.borrow();

        (*dir).clone()
    }
    /// Return shared memory os id
    pub fn get_shm_id(&self) -> String {
        let shm_id = self.shm_id.borrow();

        (*shm_id).clone()
    }

    /// Generate shared memory os id
    ///
    /// The 1st 4 hex is from process id, and the other 4 hex
    /// is from random generator
    pub fn generate_shm_id(&self) {
        let mut rng = rand::thread_rng();
        let mut shm = self.shm_id.borrow_mut();

        *shm = format!("{:04x}{:04x}", std::process::id(), rng.gen::<i32>());
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
    ) -> Result<UblkCtrl, std::io::Error> {
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
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "invalid logical block size",
                ))
            }
        }

        if let Some(pbs) = self.physical_block_size {
            if !is_power2_of(pbs, 512) {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "invalid physical block size",
                ));
            }

            if let Some(lbs) = self.logical_block_size {
                if lbs > pbs {
                    return Err(Error::new(ErrorKind::InvalidInput, "invalid block size"));
                }
            }
        }

        if !is_power2_of(self.io_buf_size, 4096) {
            return Err(Error::new(ErrorKind::InvalidInput, "invalid io buf size"));
        }

        Ok(libublk::ctrl::UblkCtrlBuilder::default()
            .name(name)
            .depth(self.depth.try_into().unwrap())
            .nr_queues(self.queue.try_into().unwrap())
            .id(self.number)
            .ctrl_flags(ctrl_flags.into())
            .ctrl_target_flags(gen_flags)
            .dev_flags(dev_flags)
            .io_buf_bytes(self.io_buf_size)
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
