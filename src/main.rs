use clap::Parser;
use pytail::config::AppConfig;

#[tokio::main]
async fn main() {
    let config = AppConfig::parse();
    if let Err(err) = pytail::server::run(config).await {
        eprintln!("server error: {err}");
        std::process::exit(1);
    }
}
