use clap::Args;
use std::path::PathBuf;

#[derive(Args)]
pub struct AddArgs {
    ///backing file of ublk target(loop)
    #[clap(long, short = 'f')]
    pub file: Option<PathBuf>,

    ///if direct io is applied for backing file of ublk target(loop)
    #[clap(long, default_value_t = true)]
    pub direct_io: bool,

    ///Config file for creating ublk(json format)
    #[clap(long)]
    pub config: Option<PathBuf>,

    #[clap(long, short = 'n', default_value_t=-1)]
    pub number: i32,

    #[clap(long, short = 't', default_value = "none")]
    pub r#type: String,

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
