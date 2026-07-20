use axum::Router;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::header::{ACCEPT, CONTENT_TYPE};
use axum::http::{HeaderValue, Response, StatusCode};
use axum::routing::get;
use bytes::Bytes;
use pytail::config::AppConfig;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::tempdir;
use tokio::net::TcpListener;

const TORCH_WHEEL: &str = "torch-2.11.0+cu128-cp313-cp313-manylinux_2_28_x86_64.whl";
const TORCHVISION_WHEEL: &str = "torchvision-0.26.0+cu128-cp313-cp313-manylinux_2_28_x86_64.whl";

#[tokio::test]
async fn flat_torch_index_becomes_project_simple_api() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let upstream = spawn_flat_upstream().await?;
    let proxy_listener = TcpListener::bind("127.0.0.1:0").await?;
    let proxy_addr = proxy_listener.local_addr()?;
    let proxy_task = tokio::spawn(pytail::server::serve_listener(
        AppConfig {
            bind: proxy_addr.to_string(),
            upstream_base_url: upstream.base_url.clone(),
            pytorch_wheels_upstream_base_url: upstream.base_url.clone(),
            pytorch_wheels_flat_index: true,
            cache_dir: temp.path().join("pytail-cache"),
            cache_max_size: 0,
            project_cache_ttl_secs: 3600,
            request_timeout_secs: 5,
            stats_interval_secs: 0,
            verbose: false,
        },
        proxy_listener,
    ));
    let client = reqwest::Client::builder().no_proxy().build()?;

    let torch = fetch_project(&client, proxy_addr, "torch").await?;
    assert_eq!(filenames(&torch), vec![TORCH_WHEEL]);

    let torchvision = fetch_project(&client, proxy_addr, "torchvision").await?;
    assert_eq!(filenames(&torchvision), vec![TORCHVISION_WHEEL]);

    let missing = client
        .get(format!(
            "http://{proxy_addr}/pytorch-wheels/cu128/not-on-the-mirror/"
        ))
        .send()
        .await?;
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        upstream.state.index_requests.load(Ordering::SeqCst),
        1,
        "the flat channel page should be shared by all project lookups"
    );

    let file_url = torch["files"][0]["url"].as_str().unwrap();
    let file = client
        .get(format!("http://{proxy_addr}{file_url}"))
        .send()
        .await?;
    assert_eq!(file.status(), StatusCode::OK);
    assert_eq!(file.bytes().await?, upstream.state.wheel);
    assert_eq!(upstream.state.file_requests.load(Ordering::SeqCst), 1);

    proxy_task.abort();
    Ok(())
}

async fn fetch_project(
    client: &reqwest::Client,
    proxy_addr: std::net::SocketAddr,
    project: &str,
) -> Result<Value, Box<dyn Error>> {
    let response = client
        .get(format!(
            "http://{proxy_addr}/pytorch-wheels/cu128/{project}/"
        ))
        .header(ACCEPT, "application/vnd.pypi.simple.v1+json")
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    Ok(serde_json::from_str(&response.text().await?)?)
}

fn filenames(project: &Value) -> Vec<&str> {
    project["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|file| file["filename"].as_str().unwrap())
        .collect()
}

struct FlatUpstream {
    base_url: String,
    state: Arc<FlatUpstreamState>,
}

struct FlatUpstreamState {
    wheel: Bytes,
    sha256: String,
    index_requests: AtomicUsize,
    file_requests: AtomicUsize,
}

async fn spawn_flat_upstream() -> Result<FlatUpstream, Box<dyn Error>> {
    let wheel = Bytes::from_static(b"flat-wheel-bytes");
    let state = Arc::new(FlatUpstreamState {
        sha256: format!("{:x}", Sha256::digest(&wheel)),
        wheel,
        index_requests: AtomicUsize::new(0),
        file_requests: AtomicUsize::new(0),
    });
    let app = Router::new()
        .route("/cu128/", get(flat_index))
        .route("/cu128/{filename}", get(flat_file))
        .with_state(state.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Ok(FlatUpstream {
        base_url: format!("http://{addr}/"),
        state,
    })
}

async fn flat_index(State(state): State<Arc<FlatUpstreamState>>) -> String {
    state.index_requests.fetch_add(1, Ordering::SeqCst);
    format!(
        r#"<html><body>
        <a href="{TORCH_WHEEL}#sha256={hash}">{TORCH_WHEEL}</a>
        <a href="{TORCHVISION_WHEEL}">{TORCHVISION_WHEEL}</a>
        <a href="/assets/site.css">site.css</a>
        </body></html>"#,
        hash = state.sha256,
    )
}

async fn flat_file(
    State(state): State<Arc<FlatUpstreamState>>,
    Path(filename): Path<String>,
) -> Response<Body> {
    if filename != TORCH_WHEEL && filename != TORCHVISION_WHEEL {
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap();
    }
    state.file_requests.fetch_add(1, Ordering::SeqCst);
    Response::builder()
        .status(StatusCode::OK)
        .header(
            CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        )
        .body(Body::from(state.wheel.clone()))
        .unwrap()
}
