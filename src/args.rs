use clap::{Args, Subcommand};

#[derive(Args, Debug)]
pub struct GenAddArgs {
    #[clap(long, short = 'n', default_value_t=-1)]
    pub number: i32,

    #[clap(long, short = 'q', default_value_t = 1)]
    pub queue: u32,

    #[clap(long, short = 'd', default_value_t = 128)]
    pub depth: u32,
}

impl GenAddArgs {
    pub fn new_ublk_session(&self, name: &'static str) -> libublk::UblkSession {
        libublk::UblkSessionBuilder::default()
            .name(name)
            .depth(self.depth)
            .nr_queues(self.queue)
            .id(self.number)
            .build()
            .unwrap()
    }
}

#[derive(Args)]
pub struct DelArgs {
    #[clap(long, short = 'n', default_value_t = -1)]
    pub number: i32,

    #[clap(long, short = 'a', default_value_t = false)]
    pub all: bool,
}

#[derive(Args)]
pub struct UblkArgs {
    #[clap(long, short = 'n', default_value_t = -1)]
    pub number: i32,
}

#[derive(Subcommand)]
pub enum AddCommands {
    Loop(super::r#loop::LoopArgs),
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
