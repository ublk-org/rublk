use super::r#loop::LoopArgs;
use clap::{Args, Subcommand};

#[derive(Args, Debug)]
pub struct DefAddArgs {
    #[clap(long, short = 'n', default_value_t=-1)]
    pub number: i32,

    #[clap(long, short = 'q', default_value_t = 1)]
    pub queue: u32,

    #[clap(long, short = 'd', default_value_t = 128)]
    pub depth: u32,
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
    Loop(LoopArgs),
    Null(DefAddArgs),
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
