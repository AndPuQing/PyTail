use clap::Parser;
use std::path::PathBuf;

#[derive(Debug, Clone, Parser)]
#[command(
    name = "pytail",
    version,
    about = "Incremental PyPI Simple API caching mirror"
)]
pub struct AppConfig {
    #[arg(long, default_value = "127.0.0.1:3141")]
    pub bind: String,

    #[arg(long, default_value = "https://pypi.org")]
    pub upstream_base_url: String,

    #[arg(
        long = "torch-url",
        default_value = "https://download.pytorch.org/whl/"
    )]
    pub pytorch_wheels_upstream_base_url: String,

    #[arg(long, default_value = ".cache/pytail")]
    pub cache_dir: PathBuf,

    #[arg(long, default_value_t = 900)]
    pub project_cache_ttl_secs: u64,

    #[arg(long, default_value_t = 15)]
    pub request_timeout_secs: u64,

    #[arg(long, default_value_t = false)]
    pub verbose: bool,
}
