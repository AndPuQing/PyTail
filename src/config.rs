use clap::Parser;
use std::path::PathBuf;

const KIB: u64 = 1024;
const MIB: u64 = KIB * 1024;
const GIB: u64 = MIB * 1024;
const TIB: u64 = GIB * 1024;

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

    #[arg(long, default_value = "0", value_parser = parse_size_bytes)]
    pub cache_max_size: u64,

    #[arg(long, default_value_t = 900)]
    pub project_cache_ttl_secs: u64,

    #[arg(long, default_value_t = 15)]
    pub request_timeout_secs: u64,

    #[arg(long, default_value_t = 60)]
    pub stats_interval_secs: u64,

    #[arg(long, default_value_t = false)]
    pub verbose: bool,
}

fn parse_size_bytes(value: &str) -> Result<u64, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("size cannot be empty".to_string());
    }

    let split_at = value
        .find(|ch: char| !(ch.is_ascii_digit() || ch == '_'))
        .unwrap_or(value.len());
    let digits = value[..split_at].replace('_', "");
    if digits.is_empty() {
        return Err(format!("invalid size {value:?}"));
    }
    let number = digits
        .parse::<u64>()
        .map_err(|_| format!("invalid size {value:?}"))?;
    let unit = value[split_at..].trim().to_ascii_lowercase();
    let multiplier = match unit.as_str() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => KIB,
        "m" | "mb" | "mib" => MIB,
        "g" | "gb" | "gib" => GIB,
        "t" | "tb" | "tib" => TIB,
        _ => return Err(format!("unsupported size unit {unit:?}")),
    };
    number
        .checked_mul(multiplier)
        .ok_or_else(|| format!("size {value:?} is too large"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cache_max_size_units() {
        assert_eq!(parse_size_bytes("0").unwrap(), 0);
        assert_eq!(parse_size_bytes("512").unwrap(), 512);
        assert_eq!(parse_size_bytes("2KiB").unwrap(), 2048);
        assert_eq!(parse_size_bytes("3mb").unwrap(), 3 * MIB);
        assert_eq!(parse_size_bytes("4G").unwrap(), 4 * GIB);
    }

    #[test]
    fn rejects_invalid_cache_max_size() {
        assert!(parse_size_bytes("").is_err());
        assert!(parse_size_bytes("MiB").is_err());
        assert!(parse_size_bytes("1XB").is_err());
    }
}
