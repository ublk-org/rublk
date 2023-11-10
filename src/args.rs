use crate::target_flags::*;
use clap::{Args, Subcommand};
use ilog::IntLog;
use libublk::io::UblkDev;
use rand::Rng;
use std::cell::RefCell;
use std::io::{Error, ErrorKind};

#[derive(Args, Debug)]
pub struct GenAddArgs {
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
    #[clap(long, default_value_t = 512)]
    pub logical_block_size: u32,

    /// physical block size
    #[clap(long, default_value_t = 4096)]
    pub physical_block_size: u32,

    #[clap(skip)]
    shm_id: RefCell<String>,
}

impl GenAddArgs {
    #[allow(dead_code)]
    pub fn apply_block_size(&self, dev: &mut UblkDev) {
        dev.tgt.params.basic.logical_bs_shift = self.logical_block_size.log2() as u8;
        dev.tgt.params.basic.physical_bs_shift = self.physical_block_size.log2() as u8;
    }
}

fn is_power2_of(input: u32, base: u32) -> bool {
    assert!((base & (base - 1)) == 0);

    let quotient = input / base;
    quotient > 0 && (quotient & (quotient - 1)) == 0
}

impl GenAddArgs {
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

    pub fn new_ublk_sesson(
        &self,
        name: &'static str,
        dev_flags: u32,
    ) -> Result<libublk::UblkSession, std::io::Error> {
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

        let mut dflags = dev_flags;
        if (ctrl_flags & libublk::sys::UBLK_F_USER_COPY) != 0 {
            dflags |= libublk::dev_flags::UBLK_DEV_F_DONT_ALLOC_BUF;
        }

        if self.logical_block_size > self.physical_block_size {
            return Err(Error::new(ErrorKind::InvalidInput, "invalid block size"));
        }

        match self.logical_block_size {
            512 | 1024 | 2048 | 4096 => {}
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "invalid logical block size",
                ))
            }
        }

        if !is_power2_of(self.physical_block_size, 512) {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "invalid physical block size",
            ));
        }

        if !is_power2_of(self.io_buf_size, 4096) {
            return Err(Error::new(ErrorKind::InvalidInput, "invalid io buf size"));
        }

        Ok(libublk::UblkSessionBuilder::default()
            .name(name)
            .depth(self.depth)
            .nr_queues(self.queue)
            .id(self.number)
            .ctrl_flags(ctrl_flags)
            .ctrl_target_flags(gen_flags)
            .dev_flags(dflags)
            .io_buf_bytes(self.io_buf_size)
            .build()
            .unwrap())
    }
}

#[derive(Args)]
pub struct DelArgs {
    /// device id, -1 means ublk driver assigns ID for us
    #[clap(long, short = 'n', default_value_t = -1)]
    pub number: i32,

    /// remove all ublk devices
    #[clap(long, short = 'a', default_value_t = false)]
    pub all: bool,
}

#[derive(Args)]
pub struct UblkArgs {
    /// device id, -1 means ublk driver assigns ID for us
    #[clap(long, short = 'n', default_value_t = -1)]
    pub number: i32,
}

#[derive(Args)]
pub struct UblkFeaturesArgs {}

#[derive(Subcommand)]
pub enum AddCommands {
    /// Add loop target
    Loop(super::r#loop::LoopArgs),
    /// Add null target
    Null(super::null::NullAddArgs),

    /// Add Zoned target, supported since v6.6
    Zoned(super::zoned::ZonedAddArgs),
}

#[derive(Subcommand)]
pub enum Commands {
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
