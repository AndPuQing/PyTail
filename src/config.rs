use clap::{Args, Parser, Subcommand};
use std::collections::HashSet;
use std::env;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceConfig {
    pub name: String,
    pub simple_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppConfig {
    pub listen: String,
    pub cache_dir: PathBuf,
    pub package_dir: PathBuf,
    pub sources: Vec<SourceConfig>,
    pub outside_url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeCommand {
    Serve(AppConfig),
    Export(SnapshotConfig),
    Import(SnapshotConfig),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotConfig {
    pub package_dir: PathBuf,
    pub path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError {
    message: String,
}

impl ConfigError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ConfigError {}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:3141".to_string(),
            cache_dir: PathBuf::from(".devpi-rs/cache"),
            package_dir: PathBuf::from(".devpi-rs/packages"),
            sources: vec![SourceConfig {
                name: "pypi".to_string(),
                simple_url: "https://pypi.org/simple/".to_string(),
            }],
            outside_url: None,
        }
    }
}

impl AppConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_args(env::args_os())
    }

    pub fn from_args<I, S>(args: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = S>,
        S: Into<std::ffi::OsString> + Clone,
    {
        let cli = Cli::try_parse_from(args).map_err(|err| ConfigError::new(err.to_string()))?;
        Self::from_cli(cli)
    }

    fn from_cli(cli: Cli) -> Result<Self, ConfigError> {
        match cli.command {
            Some(Command::Serve(args)) => Self::from_serve_args(args),
            Some(Command::Export(_)) | Some(Command::Import(_)) => {
                Err(ConfigError::new("command does not start a server"))
            }
            None => Self::from_serve_args(ServeArgs::default()),
        }
    }

    fn from_serve_args(serve: ServeArgs) -> Result<Self, ConfigError> {
        let mut config = if let Some(path) = serve.config {
            parse_config_file(&path)?
        } else {
            AppConfig::default()
        };

        if let Some(listen) = serve.listen {
            config.listen = listen;
        }
        if let Some(cache_dir) = serve.cache_dir {
            config.cache_dir = cache_dir;
        }
        if let Some(package_dir) = serve.package_dir {
            config.package_dir = package_dir;
        }

        if !serve.sources.is_empty() {
            config.sources = serve
                .sources
                .iter()
                .map(|raw| parse_source(raw))
                .collect::<Result<Vec<_>, _>>()?;
        }
        if let Some(outside_url) = serve.outside_url {
            config.outside_url = Some(normalize_outside_url(&outside_url)?);
        }

        validate_config(config)
    }
}

impl RuntimeCommand {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_args(env::args_os())
    }

    pub fn from_args<I, S>(args: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = S>,
        S: Into<std::ffi::OsString> + Clone,
    {
        let cli = Cli::try_parse_from(args).map_err(|err| ConfigError::new(err.to_string()))?;
        match cli.command {
            Some(Command::Export(args)) => Ok(RuntimeCommand::Export(args.into_config())),
            Some(Command::Import(args)) => Ok(RuntimeCommand::Import(args.into_config())),
            Some(Command::Serve(args)) => {
                AppConfig::from_serve_args(args).map(RuntimeCommand::Serve)
            }
            None => AppConfig::from_serve_args(ServeArgs::default()).map(RuntimeCommand::Serve),
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "devpi-rs",
    version,
    about = "Rust Python package index with multi-source support"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    Serve(ServeArgs),
    Export(SnapshotArgs),
    Import(SnapshotArgs),
}

#[derive(Args, Debug, Default)]
struct ServeArgs {
    #[arg(long)]
    listen: Option<String>,

    #[arg(long = "cache-dir")]
    cache_dir: Option<PathBuf>,

    #[arg(long = "package-dir")]
    package_dir: Option<PathBuf>,

    #[arg(long = "source", value_name = "name=url")]
    sources: Vec<String>,

    #[arg(long)]
    config: Option<PathBuf>,

    #[arg(long = "outside-url")]
    outside_url: Option<String>,
}

#[derive(Args, Debug)]
struct SnapshotArgs {
    #[arg(long = "package-dir", default_value = ".devpi-rs/packages")]
    package_dir: PathBuf,

    path: PathBuf,
}

impl SnapshotArgs {
    fn into_config(self) -> SnapshotConfig {
        SnapshotConfig {
            package_dir: self.package_dir,
            path: self.path,
        }
    }
}

fn validate_config(config: AppConfig) -> Result<AppConfig, ConfigError> {
    if let Some(outside_url) = &config.outside_url
        && !outside_url.starts_with("http://")
        && !outside_url.starts_with("https://")
    {
        return Err(ConfigError::new("outside_url must use http:// or https://"));
    }
    if config.sources.is_empty() {
        return Err(ConfigError::new("at least one source is required"));
    }
    let mut source_names = HashSet::new();
    for source in &config.sources {
        if source.name.trim().is_empty() {
            return Err(ConfigError::new("source name cannot be empty"));
        }
        if !source_names.insert(source.name.as_str()) {
            return Err(ConfigError::new(format!(
                "duplicate source name {}",
                source.name
            )));
        }
        if !source.simple_url.starts_with("http://") && !source.simple_url.starts_with("https://") {
            return Err(ConfigError::new(format!(
                "source {} must use http:// or https://",
                source.name
            )));
        }
    }
    Ok(config)
}

fn parse_config_file(path: &Path) -> Result<AppConfig, ConfigError> {
    let text = fs::read_to_string(path)
        .map_err(|err| ConfigError::new(format!("failed to read {}: {err}", path.display())))?;
    let mut config = AppConfig {
        sources: Vec::new(),
        ..AppConfig::default()
    };

    for (index, line) in text.lines().enumerate() {
        let line_no = index + 1;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((key, value)) = trimmed.split_once('=') else {
            return Err(ConfigError::new(format!(
                "{}:{line_no}: expected key=value",
                path.display()
            )));
        };
        let key = key.trim();
        let value = value.trim().trim_matches('"');
        match key {
            "listen" => config.listen = value.to_string(),
            "cache_dir" => config.cache_dir = PathBuf::from(value),
            "package_dir" => config.package_dir = PathBuf::from(value),
            "outside_url" => config.outside_url = Some(normalize_outside_url(value)?),
            "source" => config.sources.push(parse_source(value)?),
            _ if key.starts_with("source.") => {
                let name = key.trim_start_matches("source.").trim();
                config.sources.push(SourceConfig {
                    name: name.to_string(),
                    simple_url: normalize_base_url(value),
                });
            }
            _ => {
                return Err(ConfigError::new(format!(
                    "{}:{line_no}: unknown config key {key}",
                    path.display()
                )));
            }
        }
    }
    validate_config(config)
}

fn parse_source(raw: &str) -> Result<SourceConfig, ConfigError> {
    let Some((name, url)) = raw.split_once('=') else {
        return Err(ConfigError::new(format!(
            "invalid source {raw}; expected name=url"
        )));
    };
    Ok(SourceConfig {
        name: name.trim().to_string(),
        simple_url: normalize_base_url(url.trim()),
    })
}

fn normalize_base_url(value: &str) -> String {
    let mut value = value.trim().to_string();
    if !value.ends_with('/') {
        value.push('/');
    }
    value
}

fn normalize_outside_url(value: &str) -> Result<String, ConfigError> {
    let value = value.trim().trim_end_matches('/').to_string();
    if value.is_empty() {
        return Err(ConfigError::new("outside_url cannot be empty"));
    }
    if !value.starts_with("http://") && !value.starts_with("https://") {
        return Err(ConfigError::new("outside_url must use http:// or https://"));
    }
    Ok(value)
}

pub fn usage() -> &'static str {
    "usage: devpi-rs serve [--listen 127.0.0.1:3141] [--cache-dir PATH] [--package-dir PATH] [--outside-url URL] [--source name=https://host/simple/]... | devpi-rs export [--package-dir PATH] PATH | devpi-rs import [--package-dir PATH] PATH"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_sources_replace_default() {
        let config = AppConfig::from_args([
            "devpi-rs",
            "serve",
            "--source",
            "corp=http://localhost/simple",
            "--source",
            "pypi=https://pypi.org/simple/",
        ])
        .unwrap();

        assert_eq!(
            config.sources,
            vec![
                SourceConfig {
                    name: "corp".to_string(),
                    simple_url: "http://localhost/simple/".to_string()
                },
                SourceConfig {
                    name: "pypi".to_string(),
                    simple_url: "https://pypi.org/simple/".to_string()
                }
            ]
        );
    }

    #[test]
    fn rejects_duplicate_source_names() {
        let err = AppConfig::from_args([
            "devpi-rs",
            "serve",
            "--source",
            "pypi=https://pypi.org/simple/",
            "--source",
            "pypi=http://mirror/simple/",
        ])
        .unwrap_err();

        assert!(err.to_string().contains("duplicate source name pypi"));
    }

    #[test]
    fn reads_config_file_sources_in_order() {
        let path = std::env::temp_dir().join(format!("devpi-rs-config-{}.ini", std::process::id()));
        fs::write(
            &path,
            r#"
listen = 127.0.0.1:4000
cache_dir = /tmp/devpi-rs-cache
package_dir = /tmp/devpi-rs-packages
outside_url = https://outside.example/
source.corp = http://localhost:8080/simple
source.pypi = https://pypi.org/simple/
"#,
        )
        .unwrap();

        let config = AppConfig::from_args([
            "devpi-rs".to_string(),
            "serve".to_string(),
            "--config".to_string(),
            path.display().to_string(),
        ])
        .unwrap();

        assert_eq!(config.listen, "127.0.0.1:4000");
        assert_eq!(config.cache_dir, PathBuf::from("/tmp/devpi-rs-cache"));
        assert_eq!(config.package_dir, PathBuf::from("/tmp/devpi-rs-packages"));
        assert_eq!(
            config.outside_url,
            Some("https://outside.example".to_string())
        );
        assert_eq!(
            config.sources,
            vec![
                SourceConfig {
                    name: "corp".to_string(),
                    simple_url: "http://localhost:8080/simple/".to_string()
                },
                SourceConfig {
                    name: "pypi".to_string(),
                    simple_url: "https://pypi.org/simple/".to_string()
                }
            ]
        );
    }

    #[test]
    fn parses_outside_url_from_cli() {
        let config = AppConfig::from_args([
            "devpi-rs",
            "serve",
            "--outside-url",
            "http://outside.example/",
        ])
        .unwrap();

        assert_eq!(
            config.outside_url,
            Some("http://outside.example".to_string())
        );
    }

    #[test]
    fn parses_export_and_import_commands() {
        let export = RuntimeCommand::from_args([
            "devpi-rs",
            "export",
            "--package-dir",
            "/tmp/packages",
            "/tmp/export",
        ])
        .unwrap();
        assert_eq!(
            export,
            RuntimeCommand::Export(SnapshotConfig {
                package_dir: PathBuf::from("/tmp/packages"),
                path: PathBuf::from("/tmp/export"),
            })
        );

        let import = RuntimeCommand::from_args(["devpi-rs", "import", "/tmp/export"]).unwrap();
        assert_eq!(
            import,
            RuntimeCommand::Import(SnapshotConfig {
                package_dir: PathBuf::from(".devpi-rs/packages"),
                path: PathBuf::from("/tmp/export"),
            })
        );
    }
}
