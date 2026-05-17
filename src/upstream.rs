use crate::simple::normalize_project_name;
use bytes::Bytes;
use futures_util::Stream;
use reqwest::header::{ACCEPT, CONTENT_RANGE, CONTENT_TYPE, ETAG, IF_NONE_MATCH, RANGE};
use reqwest::{Response as ReqwestResponse, StatusCode};
use std::io;
use std::time::Duration;
use tracing::debug;
use url::Url;

const SIMPLE_ACCEPT: &str = concat!(
    "application/vnd.pypi.simple.v1+json, ",
    "application/vnd.pypi.simple.v1+html;q=0.9, ",
    "text/html;q=0.8"
);
const PYPI_LAST_SERIAL: &str = "x-pypi-last-serial";

#[derive(Debug, Clone)]
pub struct UpstreamClient {
    base_url: Url,
    base_is_simple_root: bool,
    client: reqwest::Client,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedProjectPage {
    pub project_url: String,
    pub body: String,
    pub format: ProjectPageFormat,
    pub etag: Option<String>,
    pub serial: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectPageFormat {
    Html,
    Json,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectFetch {
    Fresh(FetchedProjectPage),
    NotModified,
    NotFound,
}

impl UpstreamClient {
    pub fn new(base_url: &str, timeout_secs: u64) -> io::Result<Self> {
        let mut base_url = Url::parse(base_url).map_err(invalid_input)?;
        if !base_url.path().ends_with('/') {
            let path = format!("{}/", base_url.path());
            base_url.set_path(&path);
        }
        let base_is_simple_root = base_url
            .path_segments()
            .and_then(|mut segments| {
                segments
                    .by_ref()
                    .rfind(|segment| !segment.is_empty())
                    .map(|segment| segment.eq_ignore_ascii_case("simple"))
            })
            .unwrap_or(false);
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(timeout_secs))
            .read_timeout(Duration::from_secs(timeout_secs))
            .user_agent(format!("pytail/{}", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(io_other)?;
        Ok(Self {
            base_url,
            base_is_simple_root,
            client,
        })
    }

    pub fn new_project_root(base_url: &str, timeout_secs: u64) -> io::Result<Self> {
        let mut client = Self::new(base_url, timeout_secs)?;
        client.base_is_simple_root = true;
        Ok(client)
    }

    pub fn child_project_root(&self, path: &str, timeout_secs: u64) -> io::Result<Self> {
        let base_url = self
            .base_url
            .join(&format!("{}/", path.trim_matches('/')))
            .map_err(invalid_input)?;
        Self::new_project_root(base_url.as_str(), timeout_secs)
    }

    pub async fn fetch_project(
        &self,
        project: &str,
        etag: Option<&str>,
    ) -> io::Result<ProjectFetch> {
        let url = self.project_url(project)?;
        let mut request = self.client.get(url.clone()).header(ACCEPT, SIMPLE_ACCEPT);
        if let Some(etag) = etag {
            request = request.header(IF_NONE_MATCH, etag);
        }
        debug!(%url, has_etag = etag.is_some(), "fetching upstream project page");
        let response = request.send().await.map_err(io_other)?;
        debug!(%url, status = %response.status(), "upstream project page response");
        match response.status() {
            StatusCode::OK => {
                let etag = response
                    .headers()
                    .get(ETAG)
                    .and_then(|value| value.to_str().ok())
                    .map(ToOwned::to_owned);
                let final_url = response.url().to_string();
                let serial = response
                    .headers()
                    .get(PYPI_LAST_SERIAL)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<u64>().ok());
                let format = response
                    .headers()
                    .get(CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok())
                    .map(project_page_format)
                    .unwrap_or(ProjectPageFormat::Html);
                let body = response.text().await.map_err(io_other)?;
                Ok(ProjectFetch::Fresh(FetchedProjectPage {
                    project_url: final_url,
                    body,
                    format,
                    etag,
                    serial,
                }))
            }
            StatusCode::NOT_MODIFIED => Ok(ProjectFetch::NotModified),
            StatusCode::NOT_FOUND => Ok(ProjectFetch::NotFound),
            status => Err(io::Error::other(format!(
                "upstream project request failed with status {status}"
            ))),
        }
    }

    pub async fn open_file(&self, url: &str) -> io::Result<UpstreamFile> {
        self.open_file_range(url, None).await
    }

    pub async fn open_file_range(&self, url: &str, start: Option<u64>) -> io::Result<UpstreamFile> {
        let url = Url::parse(url).map_err(invalid_input)?;
        debug!(%url, ?start, "opening upstream file");
        let mut request = self.client.get(url);
        if let Some(start) = start {
            request = request.header(RANGE, format!("bytes={start}-"));
        }
        let response = request.send().await.map_err(io_other)?;
        debug!(url = %response.url(), status = %response.status(), "upstream file response");
        if response.status() == StatusCode::NOT_FOUND {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "upstream file not found",
            ));
        }
        if start.is_some()
            && response.status() != StatusCode::PARTIAL_CONTENT
            && response.status() != StatusCode::OK
        {
            return Err(io::Error::other(format!(
                "upstream file range request failed with status {}",
                response.status()
            )));
        }
        if !response.status().is_success() {
            return Err(io::Error::other(format!(
                "upstream file request failed with status {}",
                response.status()
            )));
        }
        let range = if response.status() == StatusCode::PARTIAL_CONTENT {
            response
                .headers()
                .get(CONTENT_RANGE)
                .and_then(|value| value.to_str().ok())
                .and_then(parse_content_range)
        } else {
            None
        };
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned);
        let content_length = response.content_length();
        Ok(UpstreamFile {
            response,
            content_type,
            content_length,
            range,
        })
    }

    pub(crate) fn project_url(&self, project: &str) -> io::Result<Url> {
        let path = if self.base_is_simple_root {
            format!("{}/", normalize_project_name(project))
        } else {
            format!("simple/{}/", normalize_project_name(project))
        };
        self.base_url.join(&path).map_err(invalid_input)
    }
}

fn project_page_format(content_type: &str) -> ProjectPageFormat {
    if content_type
        .to_ascii_lowercase()
        .contains("application/vnd.pypi.simple.v1+json")
        || content_type
            .to_ascii_lowercase()
            .contains("application/json")
    {
        ProjectPageFormat::Json
    } else {
        ProjectPageFormat::Html
    }
}

pub struct UpstreamFile {
    response: ReqwestResponse,
    pub content_type: Option<String>,
    pub content_length: Option<u64>,
    pub range: Option<ContentRange>,
}

impl UpstreamFile {
    pub fn into_stream(
        self,
    ) -> impl Stream<Item = Result<Bytes, reqwest::Error>> + Send + Sync + 'static {
        self.response.bytes_stream()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentRange {
    pub start: u64,
    pub end: u64,
    pub total: Option<u64>,
}

fn parse_content_range(value: &str) -> Option<ContentRange> {
    let value = value.strip_prefix("bytes ")?;
    let (range, total) = value.split_once('/')?;
    let (start, end) = range.split_once('-')?;
    Some(ContentRange {
        start: start.parse().ok()?,
        end: end.parse().ok()?,
        total: if total == "*" {
            None
        } else {
            Some(total.parse().ok()?)
        },
    })
}

fn invalid_input(err: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, err.to_string())
}

fn io_other(err: impl std::fmt::Display) -> io::Error {
    io::Error::other(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_url_accepts_origin_base_url() {
        let client = UpstreamClient::new("https://pypi.org", 5).unwrap();
        assert_eq!(
            client.project_url("Requests").unwrap().as_str(),
            "https://pypi.org/simple/requests/"
        );
    }

    #[test]
    fn project_url_accepts_simple_root_base_url() {
        let client = UpstreamClient::new("https://mirror.example/simple", 5).unwrap();
        assert_eq!(
            client.project_url("Requests").unwrap().as_str(),
            "https://mirror.example/simple/requests/"
        );
    }

    #[test]
    fn project_root_base_url_places_project_directly_under_base_url() {
        let client =
            UpstreamClient::new_project_root("https://download.pytorch.org/whl/cu126", 5).unwrap();
        assert_eq!(
            client.project_url("Torch").unwrap().as_str(),
            "https://download.pytorch.org/whl/cu126/torch/"
        );
    }
}
