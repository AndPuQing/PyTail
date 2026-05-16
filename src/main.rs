use clap::Parser;
use devpi_rs::config::AppConfig;

#[tokio::main]
async fn main() {
    let config = AppConfig::parse();
    if let Err(err) = devpi_rs::server::run(config).await {
        eprintln!("server error: {err}");
        std::process::exit(1);
    }
}
