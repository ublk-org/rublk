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

    /// enable user recovery
    #[clap(long, short = 'r', default_value_t = false)]
    pub user_recovery: bool,
}

impl GenAddArgs {
    pub fn new_ublk_sesson(&self, name: &'static str, for_recovery: bool) -> libublk::UblkSession {
        libublk::UblkSessionBuilder::default()
            .name(name)
            .depth(self.depth)
            .nr_queues(self.queue)
            .id(self.number)
            .ctrl_flags(if self.user_recovery {
                libublk::sys::UBLK_F_USER_RECOVERY
            } else {
                0
            })
            .for_add(!for_recovery)
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

#[derive(Subcommand)]
pub enum AddCommands {
    /// Add loop target
    Loop(super::r#loop::LoopArgs),
    /// Add null target
    Null(super::null::NullAddArgs),
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
}
