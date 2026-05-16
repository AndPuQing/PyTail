use clap::Parser;
use std::path::PathBuf;

#[derive(Debug, Clone, Parser)]
#[command(
    name = "devpi-rs",
    version,
    about = "Lazy PyPI Simple API caching proxy"
)]
pub struct AppConfig {
    #[arg(long, default_value = "127.0.0.1:3141")]
    pub bind: String,

    #[arg(long, default_value = "https://pypi.org")]
    pub upstream_base_url: String,

    #[arg(long, default_value = ".cache/devpi-rs")]
    pub cache_dir: PathBuf,

    #[arg(long, default_value_t = 900)]
    pub project_cache_ttl_secs: u64,

    #[arg(long, default_value_t = 15)]
    pub request_timeout_secs: u64,
}
