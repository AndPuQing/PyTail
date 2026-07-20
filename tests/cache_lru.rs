use axum::Router;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::header::{CONTENT_TYPE, ETAG};
use axum::http::{HeaderValue, Response, StatusCode};
use axum::routing::get;
use bytes::Bytes;
use pytail::cache::{BlobStatus, CacheStore};
use pytail::config::AppConfig;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

const PROJECT: &str = "demo";

#[tokio::test]
async fn disk_cache_limit_evicts_least_recently_used_file() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let upstream = spawn_upstream().await?;
    let proxy_listener = TcpListener::bind("127.0.0.1:0").await?;
    let proxy_addr = proxy_listener.local_addr()?;
    let cache_dir = temp.path().join("pytail-cache");
    let proxy_task = tokio::spawn(pytail::server::serve_listener(
        AppConfig {
            bind: proxy_addr.to_string(),
            upstream_base_url: upstream.base_url.clone(),
            pytorch_wheels_upstream_base_url: upstream.base_url.clone(),
            pytorch_wheels_flat_index: false,
            cache_dir: cache_dir.clone(),
            cache_max_size: 12,
            project_cache_ttl_secs: 3600,
            request_timeout_secs: 5,
            stats_interval_secs: 0,
            verbose: false,
        },
        proxy_listener,
    ));

    let client = reqwest::Client::builder().no_proxy().build()?;
    let files = fetch_project_files(&client, proxy_addr).await?;
    let cache = CacheStore::new(cache_dir.clone());
    download(&client, &files["a.whl"]).await?;
    wait_until_cached(&cache, &upstream, "a.whl").await?;
    download(&client, &files["b.whl"]).await?;
    wait_until_cached(&cache, &upstream, "b.whl").await?;
    assert_eq!(upstream.file_requests("a.whl").await, 1);
    assert_eq!(upstream.file_requests("b.whl").await, 1);

    download(&client, &files["a.whl"]).await?;
    assert_eq!(
        upstream.file_requests("a.whl").await,
        1,
        "recent access to a.whl should be served from cache"
    );
    cache
        .touch_blob_access("sha256", upstream.hash("a.whl"), current_unix_secs() + 60)
        .await?;

    download(&client, &files["c.whl"]).await?;
    assert_eq!(upstream.file_requests("c.whl").await, 1);
    wait_until_cached(&cache, &upstream, "c.whl").await?;
    wait_until_evicted(&cache, &upstream, "b.whl").await?;
    assert_cached(&cache, &upstream, "a.whl").await?;

    download(&client, &files["a.whl"]).await?;
    assert_eq!(
        upstream.file_requests("a.whl").await,
        1,
        "recently used a.whl should survive cache size enforcement"
    );

    download(&client, &files["b.whl"]).await?;
    assert_eq!(
        upstream.file_requests("b.whl").await,
        2,
        "least recently used b.whl should be evicted and fetched again"
    );

    proxy_task.abort();
    Ok(())
}

async fn fetch_project_files(
    client: &reqwest::Client,
    proxy_addr: std::net::SocketAddr,
) -> Result<HashMap<String, String>, Box<dyn Error>> {
    let html = client
        .get(format!("http://{proxy_addr}/simple/{PROJECT}/"))
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let mut files = HashMap::new();
    for filename in ["a.whl", "b.whl", "c.whl"] {
        let marker = "href=\"";
        let filename_at = html
            .find(filename)
            .ok_or_else(|| format!("project page did not contain {filename}"))?;
        let href_start = html[..filename_at]
            .rfind(&marker)
            .ok_or_else(|| format!("project page link for {filename} did not include href"))?
            + marker.len();
        let href_end = html[href_start..]
            .find('"')
            .ok_or_else(|| format!("project page link for {filename} had unterminated href"))?
            + href_start;
        let href = &html[href_start..href_end];
        files.insert(filename.to_string(), format!("http://{proxy_addr}{href}"));
    }
    Ok(files)
}

async fn download(client: &reqwest::Client, url: &str) -> Result<Bytes, Box<dyn Error>> {
    Ok(client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?)
}

async fn wait_until_cached(
    cache: &CacheStore,
    upstream: &TestUpstream,
    filename: &str,
) -> Result<(), Box<dyn Error>> {
    wait_until_blob_status(cache, upstream, filename, |status| {
        matches!(status, BlobStatus::Ready(_))
    })
    .await
}

async fn wait_until_evicted(
    cache: &CacheStore,
    upstream: &TestUpstream,
    filename: &str,
) -> Result<(), Box<dyn Error>> {
    wait_until_blob_status(cache, upstream, filename, |status| {
        matches!(status, BlobStatus::Missing)
    })
    .await
}

async fn wait_until_blob_status(
    cache: &CacheStore,
    upstream: &TestUpstream,
    filename: &str,
    matches_status: impl Fn(&BlobStatus) -> bool,
) -> Result<(), Box<dyn Error>> {
    for _ in 0..100 {
        let status = cache.blob_status("sha256", upstream.hash(filename)).await?;
        if matches_status(&status) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Err(format!("timed out waiting for {filename} cache status").into())
}

fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

struct TestUpstream {
    base_url: String,
    state: Arc<TestUpstreamState>,
}

impl TestUpstream {
    async fn file_requests(&self, filename: &str) -> usize {
        self.state
            .file_requests
            .lock()
            .await
            .get(filename)
            .map(|requests| requests.load(Ordering::SeqCst))
            .unwrap_or(0)
    }

    fn hash(&self, filename: &str) -> &str {
        &self.state.hashes[filename]
    }
}

async fn assert_cached(
    cache: &CacheStore,
    upstream: &TestUpstream,
    filename: &str,
) -> Result<(), Box<dyn Error>> {
    assert!(matches!(
        cache.blob_status("sha256", upstream.hash(filename)).await?,
        BlobStatus::Ready(_)
    ));
    Ok(())
}

struct TestUpstreamState {
    files: HashMap<String, Bytes>,
    hashes: HashMap<String, String>,
    file_requests: Mutex<HashMap<String, Arc<AtomicUsize>>>,
}

async fn spawn_upstream() -> Result<TestUpstream, Box<dyn Error>> {
    let files = HashMap::from([
        ("a.whl".to_string(), Bytes::from_static(b"aaaaaa")),
        ("b.whl".to_string(), Bytes::from_static(b"bbbbbb")),
        ("c.whl".to_string(), Bytes::from_static(b"cccccc")),
    ]);
    let hashes = files
        .iter()
        .map(|(filename, bytes)| (filename.clone(), format!("{:x}", Sha256::digest(bytes))))
        .collect();
    let state = Arc::new(TestUpstreamState {
        files,
        hashes,
        file_requests: Mutex::new(HashMap::new()),
    });
    let app = Router::new()
        .route("/simple/{project}/", get(upstream_project))
        .route("/packages/{filename}", get(upstream_file))
        .with_state(state.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Ok(TestUpstream {
        base_url: format!("http://{addr}/"),
        state,
    })
}

async fn upstream_project(
    State(state): State<Arc<TestUpstreamState>>,
    Path(project): Path<String>,
) -> Response<Body> {
    if project != PROJECT {
        let mut response = Response::new(Body::from("not found"));
        *response.status_mut() = StatusCode::NOT_FOUND;
        return response;
    }

    let mut body = String::from("<!doctype html><html><body>\n");
    for filename in ["a.whl", "b.whl", "c.whl"] {
        body.push_str(&format!(
            r#"<a href="/packages/{filename}#sha256={}">{filename}</a>"#,
            state.hashes[filename]
        ));
        body.push('\n');
    }
    body.push_str("</body></html>\n");

    let mut response = Response::new(Body::from(body));
    response
        .headers_mut()
        .insert(ETAG, HeaderValue::from_static("\"cache-lru\""));
    response
}

async fn upstream_file(
    State(state): State<Arc<TestUpstreamState>>,
    Path(filename): Path<String>,
) -> Response<Body> {
    let Some(bytes) = state.files.get(&filename) else {
        let mut response = Response::new(Body::from("not found"));
        *response.status_mut() = StatusCode::NOT_FOUND;
        return response;
    };
    let requests = {
        let mut file_requests = state.file_requests.lock().await;
        file_requests
            .entry(filename)
            .or_insert_with(|| Arc::new(AtomicUsize::new(0)))
            .clone()
    };
    requests.fetch_add(1, Ordering::SeqCst);

    let mut response = Response::new(Body::from(bytes.clone()));
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    response
}
