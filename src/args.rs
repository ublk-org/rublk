use clap::{Args, Subcommand};

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
}

impl GenAddArgs {
    pub fn new_ublk_sesson(&self, name: &'static str, dev_flags: u32) -> libublk::UblkSession {
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

        let mut dflags = dev_flags;
        if (ctrl_flags & libublk::sys::UBLK_F_USER_COPY) != 0 {
            dflags |= libublk::dev_flags::UBLK_DEV_F_DONT_ALLOC_BUF;
        }

        libublk::UblkSessionBuilder::default()
            .name(name)
            .depth(self.depth)
            .nr_queues(self.queue)
            .id(self.number)
            .ctrl_flags(ctrl_flags)
            .dev_flags(dflags)
            .io_buf_bytes(self.io_buf_size)
            .build()
            .unwrap()
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
