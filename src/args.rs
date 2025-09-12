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

    /// Use auto_buf_reg for supporting zero copy
    #[clap(long, short = 'z', default_value_t = false)]
    pub zero_copy: bool,

    /// Use multi-cpus affinity instead of single CPU affinity (default is single CPU)
    #[clap(long, default_value_t = false)]
    pub multi_cpus_affinity: bool,

    /// Enable memory locking for I/O buffers (sets UBLK_DEV_F_MLOCK_IO_BUFFER)
    #[clap(long, default_value_t = false)]
    pub mlock: bool,

    /// Used to resolve relative paths for backing files.
    /// `RefCell` is used to allow deferred initialization of this field
    /// from an immutable `GenAddArgs` reference, which is necessary
    /// because the current directory is saved after argument parsing.
    #[clap(skip)]
    start_dir: RefCell<Option<PathBuf>>,
}

/// Rounds `x` up to the nearest multiple of `y`.
/// `y` must be a power of two.
pub fn round_up<T>(x: T, y: T) -> T
where
    T: Copy + Into<u64> + std::convert::TryFrom<u64>, // support u32, u64 and try conversion back
{
    let x: u64 = x.into();
    let y: u64 = y.into();
    assert!(y.is_power_of_two(), "y isn\'t power_of_2");
    let result = (x + y - 1) & !(y - 1);

    T::try_from(result).unwrap_or_else(|_| {
        panic!("Overflow occurred during conversion from u64 to the original type");
    })
}

/// Checks if `input` is a multiple of `base`, and that the result of
/// `input / base` is a power of two.
/// `base` must be a power of two.
pub fn is_power2_of(input: u64, base: u64) -> bool {
    assert!(base.is_power_of_two());

    input % base == 0 && (input / base).is_power_of_two()
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
        let start_dir = std::env::current_dir().ok();

        let mut dir = self.start_dir.borrow_mut();
        *dir = start_dir;
    }

    /// Validates that the target is compatible with mlock flag
    pub fn validate_mlock_compatibility(&self, target_name: &str) -> anyhow::Result<()> {
        if self.mlock {
            match target_name {
                "compress" | "qcow2" | "zoned" => {
                    anyhow::bail!("{} target is not compatible with --mlock flag", target_name);
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub fn new_ublk_ctrl(
        &self,
        name: &'static str,
        dev_flags: UblkFlags,
    ) -> anyhow::Result<UblkCtrl> {
        let mut ctrl_flags = 0;
        if self.user_recovery {
            ctrl_flags |= libublk::sys::UBLK_F_USER_RECOVERY;
        }

        if name == "zoned" {
            ctrl_flags |= libublk::sys::UBLK_F_USER_COPY | libublk::sys::UBLK_F_ZONED;
        }

        if self.zero_copy {
            if name != "loop" && name != "null" {
                anyhow::bail!("Target {} doesn't support zero copy", name);
            }
            ctrl_flags |=
                libublk::sys::UBLK_F_SUPPORT_ZERO_COPY | libublk::sys::UBLK_F_AUTO_BUF_REG;
        }

        if self.user_copy {
            ctrl_flags |= libublk::sys::UBLK_F_USER_COPY;
        }

        // Validate --mlock flag
        if self.mlock {
            if self.user_copy {
                anyhow::bail!("--mlock is not allowed with --user-copy (-u) because user needs extra buffer, which is a deadlock risk");
            }
        }

        if self.unprivileged {
            ctrl_flags |= libublk::sys::UBLK_F_UNPRIVILEGED_DEV;
        }

        let mut gen_flags: u64 = 0;
        if self.quiet {
            gen_flags |= TGT_QUIET;
        }

        match self.logical_block_size {
            None | Some(512) | Some(1024) | Some(2048) | Some(4096) => {} // No-op
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

        // Apply single CPU affinity by default unless multi_cpus_affinity is enabled
        let mut final_dev_flags = if self.multi_cpus_affinity {
            dev_flags
        } else {
            dev_flags | UblkFlags::UBLK_DEV_F_SINGLE_CPU_AFFINITY
        };

        // Set UBLK_DEV_F_MLOCK_IO_BUFFER when --mlock is used without --zero-copy
        if self.mlock && !self.zero_copy {
            final_dev_flags |= UblkFlags::UBLK_DEV_F_MLOCK_IO_BUFFER;
        }

        // Store UblkFlags in high 32 bits of target_flags for recovery
        // Exclude UBLK_DEV_F_ADD_DEV since it's only relevant during creation
        let persistent_dev_flags = final_dev_flags & !UblkFlags::UBLK_DEV_F_ADD_DEV;
        let combined_target_flags = gen_flags | ((persistent_dev_flags.bits() as u64) << 32);

        Ok(libublk::ctrl::UblkCtrlBuilder::default()
            .name(name)
            .depth(self.depth.try_into()?)
            .nr_queues(self.queue.try_into()?)
            .id(self.number)
            .ctrl_flags(ctrl_flags.into())
            .ctrl_target_flags(combined_target_flags)
            .dev_flags(final_dev_flags)
            .io_buf_bytes(buf_size as u32)
            .build()?)
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
    #[cfg(feature = "compress")]
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
