use devpi_rs::config::RuntimeCommand;

#[tokio::main]
async fn main() {
    match RuntimeCommand::from_env() {
        Ok(RuntimeCommand::Serve(config)) => {
            if let Err(err) = devpi_rs::server::serve(config).await {
                eprintln!("server error: {err}");
                std::process::exit(1);
            }
        }
        Ok(RuntimeCommand::Export(config)) => {
            if let Err(err) =
                devpi_rs::snapshot::export_package_dir(&config.package_dir, &config.path)
            {
                eprintln!("export error: {err}");
                std::process::exit(1);
            }
        }
        Ok(RuntimeCommand::Import(config)) => {
            if let Err(err) =
                devpi_rs::snapshot::import_package_dir(&config.path, &config.package_dir)
            {
                eprintln!("import error: {err}");
                std::process::exit(1);
            }
        }
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(2);
        }
    }
}
