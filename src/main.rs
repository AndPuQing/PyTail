use clap::Parser;
use pytail::config::AppConfig;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    let config = AppConfig::parse();
    init_logging(config.verbose);

    if let Err(err) = pytail::server::run(config).await {
        tracing::error!(error = %err, "server stopped with error");
        std::process::exit(1);
    }
}

fn init_logging(verbose: bool) {
    let default_filter = if verbose {
        "pytail=debug"
    } else {
        "pytail=info"
    };
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .init();
}
