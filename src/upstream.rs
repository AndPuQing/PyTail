use crate::simple::normalize_project_name;
use bytes::Bytes;
use futures_util::Stream;
use reqwest::header::{ACCEPT, CONTENT_RANGE, ETAG, IF_NONE_MATCH, RANGE};
use reqwest::{Response as ReqwestResponse, StatusCode};
use std::io;
use std::time::Duration;
use url::Url;

const SIMPLE_HTML_ACCEPT: &str = "application/vnd.pypi.simple.v1+html, text/html;q=0.9";
const PYPI_LAST_SERIAL: &str = "x-pypi-last-serial";

#[derive(Debug, Clone)]
pub struct UpstreamClient {
    base_url: Url,
    client: reqwest::Client,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedProjectPage {
    pub project_url: String,
    pub body: String,
    pub etag: Option<String>,
    pub serial: Option<u64>,
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
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(timeout_secs))
            .read_timeout(Duration::from_secs(timeout_secs))
            .user_agent(format!("devpi-rs/{}", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(io_other)?;
        Ok(Self { base_url, client })
    }

    pub async fn fetch_project(
        &self,
        project: &str,
        etag: Option<&str>,
    ) -> io::Result<ProjectFetch> {
        let url = self.project_url(project)?;
        let mut request = self
            .client
            .get(url.clone())
            .header(ACCEPT, SIMPLE_HTML_ACCEPT);
        if let Some(etag) = etag {
            request = request.header(IF_NONE_MATCH, etag);
        }
        let response = request.send().await.map_err(io_other)?;
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
                let body = response.text().await.map_err(io_other)?;
                Ok(ProjectFetch::Fresh(FetchedProjectPage {
                    project_url: final_url,
                    body,
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
        let mut request = self.client.get(url);
        if let Some(start) = start {
            request = request.header(RANGE, format!("bytes={start}-"));
        }
        let response = request.send().await.map_err(io_other)?;
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

    fn project_url(&self, project: &str) -> io::Result<Url> {
        self.base_url
            .join(&format!("simple/{}/", normalize_project_name(project)))
            .map_err(invalid_input)
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
