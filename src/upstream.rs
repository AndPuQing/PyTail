use crate::config::SourceConfig;
use crate::simple::{SourcePage, source_project_url};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub trait Fetcher: Send + Sync {
    fn fetch(&self, url: &str) -> io::Result<String>;

    fn fetch_page(&self, url: &str) -> io::Result<FetchedPage> {
        self.fetch(url).map(FetchedPage::body)
    }

    fn fetch_bytes(&self, url: &str) -> io::Result<Vec<u8>> {
        self.fetch(url).map(String::into_bytes)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedPage {
    pub body: String,
    pub pypi_last_serial: Option<u64>,
}

impl FetchedPage {
    fn body(body: String) -> Self {
        Self {
            body,
            pypi_last_serial: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CurlFetcher {
    timeout: Duration,
}

impl Default for CurlFetcher {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(15),
        }
    }
}

impl Fetcher for CurlFetcher {
    fn fetch(&self, url: &str) -> io::Result<String> {
        String::from_utf8(self.fetch_bytes(url)?).map_err(|err| {
            io::Error::new(io::ErrorKind::InvalidData, format!("invalid utf-8: {err}"))
        })
    }

    fn fetch_page(&self, url: &str) -> io::Result<FetchedPage> {
        let header_path = temp_header_path();
        let output = Command::new("curl")
            .arg("-fsSL")
            .arg("--max-time")
            .arg(self.timeout.as_secs().to_string())
            .arg("-D")
            .arg(&header_path)
            .arg(url)
            .output();
        let headers = fs::read_to_string(&header_path).unwrap_or_default();
        let _ = fs::remove_file(&header_path);
        let output = output?;

        if output.status.success() {
            let body = String::from_utf8(output.stdout).map_err(|err| {
                io::Error::new(io::ErrorKind::InvalidData, format!("invalid utf-8: {err}"))
            })?;
            Ok(FetchedPage {
                body,
                pypi_last_serial: parse_pypi_last_serial(&headers),
            })
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(io::Error::other(format!(
                "curl failed for {url}: {}",
                stderr.trim()
            )))
        }
    }

    fn fetch_bytes(&self, url: &str) -> io::Result<Vec<u8>> {
        let output = Command::new("curl")
            .arg("-fsSL")
            .arg("--max-time")
            .arg(self.timeout.as_secs().to_string())
            .arg(url)
            .output()?;

        if output.status.success() {
            Ok(output.stdout)
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(io::Error::other(format!(
                "curl failed for {url}: {}",
                stderr.trim()
            )))
        }
    }
}

fn temp_header_path() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    std::env::temp_dir().join(format!(
        "devpi-rs-curl-headers-{}-{nanos}",
        std::process::id()
    ))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchReport {
    pub source: String,
    pub url: String,
    pub fetched_at_unix_secs: u64,
    pub from_cache: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamResult {
    pub pages: Vec<SourcePage>,
    pub reports: Vec<FetchReport>,
}

#[derive(Debug, Clone)]
pub struct CachedFile {
    pub bytes: Vec<u8>,
    pub modified: Option<SystemTime>,
}

#[derive(Debug, Clone)]
pub struct MultiSourceIndex {
    sources: Vec<SourceConfig>,
    cache_dir: PathBuf,
}

impl MultiSourceIndex {
    pub fn new(sources: Vec<SourceConfig>, cache_dir: PathBuf) -> Self {
        Self { sources, cache_dir }
    }

    pub fn source_names(&self) -> Vec<String> {
        self.sources
            .iter()
            .map(|source| source.name.clone())
            .collect()
    }

    pub fn project_pages<F: Fetcher + ?Sized>(
        &self,
        project: &str,
        fetcher: &F,
    ) -> io::Result<UpstreamResult> {
        self.project_pages_for_sources(project, &[], fetcher)
    }

    pub fn project_pages_for_sources<F: Fetcher + ?Sized>(
        &self,
        project: &str,
        source_names: &[String],
        fetcher: &F,
    ) -> io::Result<UpstreamResult> {
        let mut result = UpstreamResult {
            pages: Vec::new(),
            reports: Vec::new(),
        };

        for source in self.sources_for(source_names)? {
            let url = source_project_url(&source.simple_url, project);
            self.fetch_source(&mut result, source, &url, project, None, fetcher);
        }

        Ok(result)
    }

    pub fn project_pages_for_url<F: Fetcher + ?Sized>(
        &self,
        source_name: &str,
        simple_url: &str,
        project: &str,
        fetcher: &F,
    ) -> UpstreamResult {
        self.project_pages_for_url_with_cache_expiry(
            source_name,
            simple_url,
            project,
            None,
            fetcher,
        )
    }

    pub fn project_pages_for_url_with_cache_expiry<F: Fetcher + ?Sized>(
        &self,
        source_name: &str,
        simple_url: &str,
        project: &str,
        cache_expiry: Option<Duration>,
        fetcher: &F,
    ) -> UpstreamResult {
        let mut result = UpstreamResult {
            pages: Vec::new(),
            reports: Vec::new(),
        };
        let source = SourceConfig {
            name: source_name.to_string(),
            simple_url: simple_url.to_string(),
        };
        let url = source_project_url(&source.simple_url, project);
        self.fetch_source(&mut result, &source, &url, project, cache_expiry, fetcher);
        result
    }

    pub fn root_pages<F: Fetcher + ?Sized>(&self, fetcher: &F) -> io::Result<UpstreamResult> {
        self.root_pages_for_sources(&[], fetcher)
    }

    pub fn root_pages_for_sources<F: Fetcher + ?Sized>(
        &self,
        source_names: &[String],
        fetcher: &F,
    ) -> io::Result<UpstreamResult> {
        let mut result = UpstreamResult {
            pages: Vec::new(),
            reports: Vec::new(),
        };

        for source in self.sources_for(source_names)? {
            let url = source.simple_url.clone();
            self.fetch_source(&mut result, source, &url, "__root__", None, fetcher);
        }

        Ok(result)
    }

    pub fn root_pages_for_url<F: Fetcher + ?Sized>(
        &self,
        source_name: &str,
        simple_url: &str,
        fetcher: &F,
    ) -> UpstreamResult {
        self.root_pages_for_url_with_cache_expiry(source_name, simple_url, None, fetcher)
    }

    pub fn root_pages_for_url_with_cache_expiry<F: Fetcher + ?Sized>(
        &self,
        source_name: &str,
        simple_url: &str,
        cache_expiry: Option<Duration>,
        fetcher: &F,
    ) -> UpstreamResult {
        let mut result = UpstreamResult {
            pages: Vec::new(),
            reports: Vec::new(),
        };
        let source = SourceConfig {
            name: source_name.to_string(),
            simple_url: simple_url.to_string(),
        };
        self.fetch_source(
            &mut result,
            &source,
            simple_url,
            "__root__",
            cache_expiry,
            fetcher,
        );
        result
    }

    pub fn clear_project_cache_for_sources(
        &self,
        project: &str,
        source_names: &[String],
    ) -> io::Result<usize> {
        let mut removed = 0;
        for source in self.sources_for(source_names)? {
            match fs::remove_file(cache_path(&self.cache_dir, &source.name, project)) {
                Ok(()) => {
                    let _ =
                        fs::remove_file(cache_serial_path(&self.cache_dir, &source.name, project));
                    removed += 1;
                }
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(err),
            }
        }
        Ok(removed)
    }

    pub fn clear_project_cache_for_source(
        &self,
        source_name: &str,
        project: &str,
    ) -> io::Result<bool> {
        match fs::remove_file(cache_path(&self.cache_dir, source_name, project)) {
            Ok(()) => {
                let _ = fs::remove_file(cache_serial_path(&self.cache_dir, source_name, project));
                Ok(true)
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(err) => Err(err),
        }
    }

    pub fn cached_or_fetch_file<F: Fetcher + ?Sized>(
        &self,
        cache_key: &str,
        url: &str,
        fetcher: &F,
    ) -> io::Result<Vec<u8>> {
        self.cached_or_fetch_file_entry(cache_key, url, fetcher)
            .map(|file| file.bytes)
    }

    pub fn cached_or_fetch_file_entry<F: Fetcher + ?Sized>(
        &self,
        cache_key: &str,
        url: &str,
        fetcher: &F,
    ) -> io::Result<CachedFile> {
        match read_file_cache(&self.cache_dir, cache_key) {
            Ok(file) => Ok(file),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                let bytes = fetcher.fetch_bytes(url)?;
                write_file_cache(&self.cache_dir, cache_key, &bytes)?;
                read_file_cache(&self.cache_dir, cache_key)
            }
            Err(err) => Err(err),
        }
    }

    pub fn cached_file(&self, cache_key: &str) -> io::Result<Vec<u8>> {
        read_file_cache(&self.cache_dir, cache_key).map(|file| file.bytes)
    }

    pub fn cached_file_entry(&self, cache_key: &str) -> io::Result<CachedFile> {
        read_file_cache(&self.cache_dir, cache_key)
    }

    fn sources_for<'a>(&'a self, source_names: &'a [String]) -> io::Result<Vec<&'a SourceConfig>> {
        if source_names.is_empty() {
            return Ok(self.sources.iter().collect());
        }
        source_names
            .iter()
            .map(|name| {
                self.sources
                    .iter()
                    .find(|source| source.name == *name)
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("unknown upstream source {name:?}"),
                        )
                    })
            })
            .collect()
    }

    fn fetch_source<F: Fetcher + ?Sized>(
        &self,
        result: &mut UpstreamResult,
        source: &SourceConfig,
        url: &str,
        key: &str,
        cache_expiry: Option<Duration>,
        fetcher: &F,
    ) {
        let fetched_at_unix_secs = current_unix_secs();
        if let Some(expiry) = cache_expiry
            && !expiry.is_zero()
            && let Ok(Some(page)) =
                read_cache_entry_if_fresh(&self.cache_dir, &source.name, key, expiry)
        {
            result.pages.push(SourcePage {
                source: source.name.clone(),
                body: page.body,
                page_url: Some(url.to_string()),
                pypi_last_serial: page.pypi_last_serial,
            });
            result.reports.push(FetchReport {
                source: source.name.clone(),
                url: url.to_string(),
                fetched_at_unix_secs,
                from_cache: true,
                error: None,
            });
            return;
        }

        match fetcher.fetch_page(url) {
            Ok(page) => {
                let _ = write_cache_entry(
                    &self.cache_dir,
                    &source.name,
                    key,
                    &page.body,
                    page.pypi_last_serial,
                );
                result.pages.push(SourcePage {
                    source: source.name.clone(),
                    body: page.body,
                    page_url: Some(url.to_string()),
                    pypi_last_serial: page.pypi_last_serial,
                });
                result.reports.push(FetchReport {
                    source: source.name.clone(),
                    url: url.to_string(),
                    fetched_at_unix_secs,
                    from_cache: false,
                    error: None,
                });
            }
            Err(err) => {
                let cache = read_cache_entry(&self.cache_dir, &source.name, key);
                let from_cache = cache.is_ok();
                if let Ok(page) = cache {
                    result.pages.push(SourcePage {
                        source: source.name.clone(),
                        body: page.body,
                        page_url: Some(url.to_string()),
                        pypi_last_serial: page.pypi_last_serial,
                    });
                }
                result.reports.push(FetchReport {
                    source: source.name.clone(),
                    url: url.to_string(),
                    fetched_at_unix_secs,
                    from_cache,
                    error: Some(err.to_string()),
                });
            }
        }
    }
}

fn parse_pypi_last_serial(headers: &str) -> Option<u64> {
    let mut serial = None;
    for line in headers.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.trim().eq_ignore_ascii_case("X-PYPI-LAST-SERIAL") {
            serial = value.trim().parse().ok();
        }
    }
    serial
}

#[cfg(test)]
fn write_cache(cache_dir: &Path, source: &str, key: &str, body: &str) -> io::Result<()> {
    write_cache_entry(cache_dir, source, key, body, None)
}

fn write_cache_entry(
    cache_dir: &Path,
    source: &str,
    key: &str,
    body: &str,
    pypi_last_serial: Option<u64>,
) -> io::Result<()> {
    fs::create_dir_all(cache_dir)?;
    fs::write(cache_path(cache_dir, source, key), body)?;
    match pypi_last_serial {
        Some(serial) => fs::write(
            cache_serial_path(cache_dir, source, key),
            serial.to_string(),
        ),
        None => match fs::remove_file(cache_serial_path(cache_dir, source, key)) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err),
        },
    }
}

#[cfg(test)]
fn read_cache(cache_dir: &Path, source: &str, key: &str) -> io::Result<String> {
    read_cache_entry(cache_dir, source, key).map(|page| page.body)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CachedPage {
    body: String,
    pypi_last_serial: Option<u64>,
}

fn read_cache_entry(cache_dir: &Path, source: &str, key: &str) -> io::Result<CachedPage> {
    Ok(CachedPage {
        body: fs::read_to_string(cache_path(cache_dir, source, key))?,
        pypi_last_serial: read_cache_serial(cache_dir, source, key)?,
    })
}

fn read_cache_entry_if_fresh(
    cache_dir: &Path,
    source: &str,
    key: &str,
    max_age: Duration,
) -> io::Result<Option<CachedPage>> {
    let path = cache_path(cache_dir, source, key);
    let modified = fs::metadata(&path)?.modified()?;
    let age = SystemTime::now()
        .duration_since(modified)
        .unwrap_or_default();
    if age <= max_age {
        read_cache_entry(cache_dir, source, key).map(Some)
    } else {
        Ok(None)
    }
}

fn read_cache_serial(cache_dir: &Path, source: &str, key: &str) -> io::Result<Option<u64>> {
    match fs::read_to_string(cache_serial_path(cache_dir, source, key)) {
        Ok(value) => Ok(value.trim().parse().ok()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

fn write_file_cache(cache_dir: &Path, key: &str, bytes: &[u8]) -> io::Result<()> {
    let path = file_cache_path(cache_dir, key);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, bytes)
}

fn read_file_cache(cache_dir: &Path, key: &str) -> io::Result<CachedFile> {
    let path = file_cache_path(cache_dir, key);
    let bytes = fs::read(&path)?;
    let modified = fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .ok();
    Ok(CachedFile { bytes, modified })
}

fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

fn cache_path(cache_dir: &Path, source: &str, key: &str) -> PathBuf {
    cache_dir.join(format!("{}__{}.html", safe_name(source), safe_name(key)))
}

fn cache_serial_path(cache_dir: &Path, source: &str, key: &str) -> PathBuf {
    cache_dir.join(format!("{}__{}.serial", safe_name(source), safe_name(key)))
}

fn file_cache_path(cache_dir: &Path, key: &str) -> PathBuf {
    cache_dir.join("files").join(safe_name(key))
}

fn safe_name(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    struct MockFetcher {
        responses: Mutex<HashMap<String, io::Result<String>>>,
    }

    impl Fetcher for MockFetcher {
        fn fetch(&self, url: &str) -> io::Result<String> {
            self.responses
                .lock()
                .unwrap()
                .remove(url)
                .unwrap_or_else(|| Err(io::Error::new(io::ErrorKind::NotFound, url.to_string())))
        }
    }

    struct PageMockFetcher {
        responses: Mutex<HashMap<String, io::Result<FetchedPage>>>,
    }

    impl Fetcher for PageMockFetcher {
        fn fetch(&self, url: &str) -> io::Result<String> {
            self.fetch_page(url).map(|page| page.body)
        }

        fn fetch_page(&self, url: &str) -> io::Result<FetchedPage> {
            self.responses
                .lock()
                .unwrap()
                .remove(url)
                .unwrap_or_else(|| Err(io::Error::new(io::ErrorKind::NotFound, url.to_string())))
        }
    }

    #[test]
    fn parses_valid_pypi_last_serial_header_only() {
        assert_eq!(
            parse_pypi_last_serial("HTTP/1.1 200 OK\r\nX-PYPI-LAST-SERIAL: 10000\r\n"),
            Some(10000)
        );
        assert_eq!(
            parse_pypi_last_serial("HTTP/1.1 200 OK\r\nX-PYPI-LAST-SERIAL: foo\r\n"),
            None
        );
        assert_eq!(parse_pypi_last_serial("HTTP/1.1 200 OK\r\n"), None);
    }

    #[test]
    fn fetches_each_source_for_project() {
        let index = MultiSourceIndex::new(
            vec![
                SourceConfig {
                    name: "corp".to_string(),
                    simple_url: "http://corp/simple/".to_string(),
                },
                SourceConfig {
                    name: "pypi".to_string(),
                    simple_url: "https://pypi.org/simple/".to_string(),
                },
            ],
            std::env::temp_dir().join("devpi-rs-test-fetches"),
        );
        let fetcher = MockFetcher {
            responses: Mutex::new(HashMap::from([
                (
                    "http://corp/simple/demo-pkg/".to_string(),
                    Ok("corp".to_string()),
                ),
                (
                    "https://pypi.org/simple/demo-pkg/".to_string(),
                    Ok("pypi".to_string()),
                ),
            ])),
        };

        let result = index.project_pages("Demo_Pkg", &fetcher).unwrap();

        assert_eq!(result.pages.len(), 2);
        assert_eq!(result.pages[0].source, "corp");
        assert_eq!(result.pages[1].source, "pypi");
        assert!(result.reports.iter().all(|report| report.error.is_none()));
    }

    #[test]
    fn stores_pypi_last_serial_from_fetched_page() {
        let index = MultiSourceIndex::new(
            vec![SourceConfig {
                name: "pypi".to_string(),
                simple_url: "https://pypi.org/simple/".to_string(),
            }],
            std::env::temp_dir().join("devpi-rs-test-pypi-last"),
        );
        let fetcher = PageMockFetcher {
            responses: Mutex::new(HashMap::from([(
                "https://pypi.org/simple/demo/".to_string(),
                Ok(FetchedPage {
                    body: "pypi".to_string(),
                    pypi_last_serial: Some(10000),
                }),
            )])),
        };

        let result = index.project_pages("demo", &fetcher).unwrap();

        assert_eq!(result.pages[0].pypi_last_serial, Some(10000));
    }

    #[test]
    fn keeps_pypi_last_serial_when_using_fresh_page_cache() {
        let cache_dir = std::env::temp_dir().join(format!(
            "devpi-rs-test-pypi-last-cache-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&cache_dir);
        write_cache_entry(&cache_dir, "root/pypi", "demo", "cached", Some(10000)).unwrap();
        let index = MultiSourceIndex::new(Vec::new(), cache_dir);
        let fetcher = MockFetcher {
            responses: Mutex::new(HashMap::new()),
        };

        let result = index.project_pages_for_url_with_cache_expiry(
            "root/pypi",
            "https://mirror.example/simple/",
            "demo",
            Some(Duration::from_secs(600)),
            &fetcher,
        );

        assert_eq!(result.pages[0].body, "cached");
        assert_eq!(result.pages[0].pypi_last_serial, Some(10000));
        assert!(result.reports[0].from_cache);
        assert!(result.reports[0].error.is_none());
    }

    #[test]
    fn fetches_configured_source_subset_in_requested_order() {
        let index = MultiSourceIndex::new(
            vec![
                SourceConfig {
                    name: "corp".to_string(),
                    simple_url: "http://corp/simple/".to_string(),
                },
                SourceConfig {
                    name: "pypi".to_string(),
                    simple_url: "https://pypi.org/simple/".to_string(),
                },
            ],
            std::env::temp_dir().join("devpi-rs-test-source-subset"),
        );
        let fetcher = MockFetcher {
            responses: Mutex::new(HashMap::from([
                (
                    "http://corp/simple/demo/".to_string(),
                    Ok("corp".to_string()),
                ),
                (
                    "https://pypi.org/simple/demo/".to_string(),
                    Ok("pypi".to_string()),
                ),
            ])),
        };

        let result = index
            .project_pages_for_sources("demo", &["pypi".to_string()], &fetcher)
            .unwrap();

        assert_eq!(result.pages.len(), 1);
        assert_eq!(result.pages[0].source, "pypi");
        assert_eq!(result.pages[0].body, "pypi");
    }

    #[test]
    fn rejects_unknown_configured_source() {
        let index = MultiSourceIndex::new(
            vec![SourceConfig {
                name: "pypi".to_string(),
                simple_url: "https://pypi.org/simple/".to_string(),
            }],
            std::env::temp_dir().join("devpi-rs-test-unknown-source"),
        );
        let fetcher = MockFetcher {
            responses: Mutex::new(HashMap::new()),
        };

        let err = index
            .project_pages_for_sources("demo", &["corp".to_string()], &fetcher)
            .unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("unknown upstream source"));
    }

    #[test]
    fn uses_stale_cache_when_source_fails() {
        let cache_dir =
            std::env::temp_dir().join(format!("devpi-rs-test-cache-{}", std::process::id()));
        let source = SourceConfig {
            name: "corp".to_string(),
            simple_url: "http://corp/simple/".to_string(),
        };
        write_cache_entry(&cache_dir, "corp", "demo", "cached", Some(42)).unwrap();
        let index = MultiSourceIndex::new(vec![source], cache_dir);
        let fetcher = MockFetcher {
            responses: Mutex::new(HashMap::new()),
        };

        let result = index.project_pages("demo", &fetcher).unwrap();

        assert_eq!(result.pages[0].body, "cached");
        assert_eq!(result.pages[0].pypi_last_serial, Some(42));
        assert!(result.reports[0].from_cache);
        assert!(result.reports[0].error.is_some());
    }

    #[test]
    fn clears_project_cache_for_configured_sources() {
        let cache_dir =
            std::env::temp_dir().join(format!("devpi-rs-test-cache-clear-{}", std::process::id()));
        let _ = fs::remove_dir_all(&cache_dir);
        let index = MultiSourceIndex::new(
            vec![
                SourceConfig {
                    name: "corp".to_string(),
                    simple_url: "http://corp/simple/".to_string(),
                },
                SourceConfig {
                    name: "pypi".to_string(),
                    simple_url: "https://pypi.org/simple/".to_string(),
                },
            ],
            cache_dir.clone(),
        );
        write_cache_entry(&cache_dir, "corp", "demo", "corp", Some(10)).unwrap();
        write_cache(&cache_dir, "pypi", "demo", "pypi").unwrap();

        let removed = index
            .clear_project_cache_for_sources("demo", &["corp".to_string()])
            .unwrap();

        assert_eq!(removed, 1);
        assert!(read_cache(&cache_dir, "corp", "demo").is_err());
        assert!(
            read_cache_serial(&cache_dir, "corp", "demo")
                .unwrap()
                .is_none()
        );
        assert_eq!(read_cache(&cache_dir, "pypi", "demo").unwrap(), "pypi");
    }

    #[test]
    fn uses_fresh_cache_before_configured_expiry() {
        let cache_dir =
            std::env::temp_dir().join(format!("devpi-rs-test-cache-expiry-{}", std::process::id()));
        let _ = fs::remove_dir_all(&cache_dir);
        write_cache(&cache_dir, "root/pypi", "demo", "cached").unwrap();
        let index = MultiSourceIndex::new(Vec::new(), cache_dir);
        let fetcher = MockFetcher {
            responses: Mutex::new(HashMap::new()),
        };

        let result = index.project_pages_for_url_with_cache_expiry(
            "root/pypi",
            "https://mirror.example/simple/",
            "demo",
            Some(Duration::from_secs(600)),
            &fetcher,
        );

        assert_eq!(result.pages[0].body, "cached");
        assert!(result.reports[0].from_cache);
        assert!(result.reports[0].error.is_none());
    }

    #[test]
    fn zero_cache_expiry_fetches_even_when_cache_exists() {
        let cache_dir = std::env::temp_dir().join(format!(
            "devpi-rs-test-cache-expiry-zero-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&cache_dir);
        write_cache(&cache_dir, "root/pypi", "demo", "cached").unwrap();
        let index = MultiSourceIndex::new(Vec::new(), cache_dir);
        let fetcher = MockFetcher {
            responses: Mutex::new(HashMap::from([(
                "https://mirror.example/simple/demo/".to_string(),
                Ok("fresh".to_string()),
            )])),
        };

        let result = index.project_pages_for_url_with_cache_expiry(
            "root/pypi",
            "https://mirror.example/simple/",
            "demo",
            Some(Duration::from_secs(0)),
            &fetcher,
        );

        assert_eq!(result.pages[0].body, "fresh");
        assert!(!result.reports[0].from_cache);
        assert!(result.reports[0].error.is_none());
    }

    #[test]
    fn caches_fetched_files() {
        let cache_dir =
            std::env::temp_dir().join(format!("devpi-rs-file-cache-{}", std::process::id()));
        let _ = fs::remove_dir_all(&cache_dir);
        let index = MultiSourceIndex::new(Vec::new(), cache_dir);
        let fetcher = MockFetcher {
            responses: Mutex::new(HashMap::from([(
                "https://files.example/demo.whl".to_string(),
                Ok("wheel-bytes".to_string()),
            )])),
        };

        let first = index
            .cached_or_fetch_file("demo-key", "https://files.example/demo.whl", &fetcher)
            .unwrap();
        let second = index
            .cached_or_fetch_file("demo-key", "https://files.example/demo.whl", &fetcher)
            .unwrap();

        assert_eq!(first, b"wheel-bytes");
        assert_eq!(second, b"wheel-bytes");
    }
}
