use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::header::{CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, RANGE};
use axum::http::{HeaderMap, HeaderValue, Response, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::serve::ListenerExt;
use bytes::Bytes;
use clap::{Parser, ValueEnum};
use devpi_rs::config::AppConfig;
use futures_util::{StreamExt, stream};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio::sync::Barrier;

#[derive(Debug, Parser)]
#[command(
    name = "fanout_bench",
    about = "Local benchmark for PyPI wheel fan-out streaming"
)]
struct Args {
    #[arg(long, default_value_t = 8)]
    clients: usize,

    #[arg(long, default_value_t = 32)]
    wheel_mib: usize,

    #[arg(long, default_value_t = 64)]
    chunk_kib: usize,

    #[arg(long, default_value_t = 5)]
    slow_chunk_delay_ms: u64,

    #[arg(long, default_value_t = 10)]
    slow_client_read_delay_ms: u64,

    #[arg(long, default_value_t = 15)]
    request_timeout_secs: u64,

    #[arg(long, value_enum, default_value_t = Mode::Both)]
    mode: Mode,

    #[arg(long)]
    cache_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Mode {
    Fast,
    Slow,
    Late,
    SlowClient,
    Paths,
    Both,
    All,
}

#[derive(Debug)]
struct ClientMetric {
    id: usize,
    status: Option<StatusCode>,
    content_length: Option<u64>,
    bytes: u64,
    chunks: u64,
    first_byte: Duration,
    full_bytes: Option<Duration>,
    total: Duration,
    error: Option<String>,
}

#[derive(Clone)]
struct UpstreamState {
    wheel: Arc<Vec<u8>>,
    sha256: String,
    chunk_bytes: usize,
    chunk_delay: Duration,
    file_requests: Arc<AtomicUsize>,
    chunks_sent: Arc<AtomicUsize>,
}

#[derive(Debug, Clone, Copy)]
enum ClientStart {
    Simultaneous,
    LateJoin,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    match args.mode {
        Mode::Fast => {
            run_scenario(
                "fast",
                &args,
                Duration::ZERO,
                ClientStart::Simultaneous,
                None,
            )
            .await?
        }
        Mode::Slow => {
            run_scenario(
                "slow",
                &args,
                Duration::from_millis(args.slow_chunk_delay_ms),
                ClientStart::Simultaneous,
                None,
            )
            .await?
        }
        Mode::Late => {
            run_scenario(
                "late",
                &args,
                Duration::from_millis(args.slow_chunk_delay_ms.max(1)),
                ClientStart::LateJoin,
                None,
            )
            .await?
        }
        Mode::SlowClient => {
            run_scenario(
                "slow-client",
                &args,
                Duration::from_millis(args.slow_chunk_delay_ms.max(1)),
                ClientStart::Simultaneous,
                Some(Duration::from_millis(args.slow_client_read_delay_ms)),
            )
            .await?
        }
        Mode::Paths => run_path_scenarios(&args).await?,
        Mode::Both => {
            run_scenario(
                "fast",
                &args,
                Duration::ZERO,
                ClientStart::Simultaneous,
                None,
            )
            .await?;
            run_scenario(
                "slow",
                &args,
                Duration::from_millis(args.slow_chunk_delay_ms),
                ClientStart::Simultaneous,
                None,
            )
            .await?;
        }
        Mode::All => {
            run_scenario(
                "fast",
                &args,
                Duration::ZERO,
                ClientStart::Simultaneous,
                None,
            )
            .await?;
            run_scenario(
                "slow",
                &args,
                Duration::from_millis(args.slow_chunk_delay_ms),
                ClientStart::Simultaneous,
                None,
            )
            .await?;
            run_scenario(
                "late",
                &args,
                Duration::from_millis(args.slow_chunk_delay_ms.max(1)),
                ClientStart::LateJoin,
                None,
            )
            .await?;
            run_scenario(
                "slow-client",
                &args,
                Duration::from_millis(args.slow_chunk_delay_ms.max(1)),
                ClientStart::Simultaneous,
                Some(Duration::from_millis(args.slow_client_read_delay_ms)),
            )
            .await?;
            run_path_scenarios(&args).await?;
        }
    }
    Ok(())
}

async fn run_path_scenarios(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let wheel = Arc::new(make_wheel(args.wheel_mib * 1024 * 1024));
    let sha256 = format!("{:x}", Sha256::digest(wheel.as_slice()));
    let upstream = spawn_upstream(UpstreamState {
        wheel,
        sha256,
        chunk_bytes: args.chunk_kib * 1024,
        chunk_delay: Duration::ZERO,
        file_requests: Arc::new(AtomicUsize::new(0)),
        chunks_sent: Arc::new(AtomicUsize::new(0)),
    })
    .await?;

    let cache_dir = args
        .cache_dir
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join("devpi-rs-path-bench"));
    let cache_dir = cache_dir.join(unique_suffix());
    tokio::fs::create_dir_all(&cache_dir).await?;

    let proxy_listener = TcpListener::bind("127.0.0.1:0").await?;
    let proxy_addr = proxy_listener.local_addr()?;
    let proxy_task = tokio::spawn(devpi_rs::server::serve_listener(
        AppConfig {
            bind: proxy_addr.to_string(),
            upstream_base_url: upstream.base_url.clone(),
            cache_dir,
            project_cache_ttl_secs: 3600,
            request_timeout_secs: args.request_timeout_secs,
        },
        proxy_listener,
    ));

    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(args.clients.max(1))
        .build()?;

    let before_requests = upstream.file_requests.load(Ordering::SeqCst);
    let before_chunks = upstream.chunks_sent.load(Ordering::SeqCst);
    let started = Instant::now();
    let direct = download_client(
        0,
        client.clone(),
        format!("{}packages/demo-1.0.whl", upstream.base_url),
        None,
    )
    .await;
    print_report(
        "path-direct-upstream-stream",
        args,
        Duration::ZERO,
        upstream.file_requests.load(Ordering::SeqCst) - before_requests,
        upstream.chunks_sent.load(Ordering::SeqCst) - before_chunks,
        started.elapsed(),
        &[direct],
    );

    let before_requests = upstream.file_requests.load(Ordering::SeqCst);
    let before_chunks = upstream.chunks_sent.load(Ordering::SeqCst);
    let started = Instant::now();
    let direct_static = download_client(
        0,
        client.clone(),
        format!("{}packages-static/demo-1.0.whl", upstream.base_url),
        None,
    )
    .await;
    print_report(
        "path-direct-static",
        args,
        Duration::ZERO,
        upstream.file_requests.load(Ordering::SeqCst) - before_requests,
        upstream.chunks_sent.load(Ordering::SeqCst) - before_chunks,
        started.elapsed(),
        &[direct_static],
    );

    let simple_url = format!("http://{proxy_addr}/simple/demo/");
    let html = client.get(simple_url).send().await?.text().await?;
    let file_path = html
        .split('"')
        .find(|part| part.starts_with("/root/pypi/+f/"))
        .and_then(|part| part.split('#').next())
        .ok_or("proxy simple page did not contain a cached file link")?;
    let file_url = format!("http://{proxy_addr}{file_path}");

    let before_requests = upstream.file_requests.load(Ordering::SeqCst);
    let before_chunks = upstream.chunks_sent.load(Ordering::SeqCst);
    let started = Instant::now();
    let cold = download_client(0, client.clone(), file_url.clone(), None).await;
    print_report(
        "path-proxy-cold",
        args,
        Duration::ZERO,
        upstream.file_requests.load(Ordering::SeqCst) - before_requests,
        upstream.chunks_sent.load(Ordering::SeqCst) - before_chunks,
        started.elapsed(),
        &[cold],
    );

    let before_requests = upstream.file_requests.load(Ordering::SeqCst);
    let before_chunks = upstream.chunks_sent.load(Ordering::SeqCst);
    let started = Instant::now();
    let cached = download_client(0, client.clone(), file_url.clone(), None).await;
    print_report(
        "path-proxy-cached-single",
        args,
        Duration::ZERO,
        upstream.file_requests.load(Ordering::SeqCst) - before_requests,
        upstream.chunks_sent.load(Ordering::SeqCst) - before_chunks,
        started.elapsed(),
        &[cached],
    );

    let before_requests = upstream.file_requests.load(Ordering::SeqCst);
    let before_chunks = upstream.chunks_sent.load(Ordering::SeqCst);
    let started = Instant::now();
    let barrier = Arc::new(Barrier::new(args.clients + 1));
    let mut handles = Vec::with_capacity(args.clients);
    for id in 0..args.clients {
        let client = client.clone();
        let file_url = file_url.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            download_client(id, client, file_url, None).await
        }));
    }
    barrier.wait().await;
    let mut metrics = Vec::with_capacity(args.clients);
    for handle in handles {
        metrics.push(handle.await?);
    }
    metrics.sort_by_key(|metric| metric.id);
    print_report(
        "path-proxy-cached-fanout",
        args,
        Duration::ZERO,
        upstream.file_requests.load(Ordering::SeqCst) - before_requests,
        upstream.chunks_sent.load(Ordering::SeqCst) - before_chunks,
        started.elapsed(),
        &metrics,
    );

    proxy_task.abort();
    upstream.task.abort();
    Ok(())
}

async fn run_scenario(
    name: &str,
    args: &Args,
    chunk_delay: Duration,
    client_start: ClientStart,
    slow_client_delay: Option<Duration>,
) -> Result<(), Box<dyn std::error::Error>> {
    let wheel = Arc::new(make_wheel(args.wheel_mib * 1024 * 1024));
    let sha256 = format!("{:x}", Sha256::digest(wheel.as_slice()));
    let upstream = spawn_upstream(UpstreamState {
        wheel,
        sha256,
        chunk_bytes: args.chunk_kib * 1024,
        chunk_delay,
        file_requests: Arc::new(AtomicUsize::new(0)),
        chunks_sent: Arc::new(AtomicUsize::new(0)),
    })
    .await?;

    let cache_dir = args
        .cache_dir
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join(format!("devpi-rs-fanout-bench-{name}")));
    let cache_dir = cache_dir.join(unique_suffix());
    tokio::fs::create_dir_all(&cache_dir).await?;

    let proxy_listener = TcpListener::bind("127.0.0.1:0").await?;
    let proxy_addr = proxy_listener.local_addr()?;
    let proxy_task = tokio::spawn(devpi_rs::server::serve_listener(
        AppConfig {
            bind: proxy_addr.to_string(),
            upstream_base_url: upstream.base_url.clone(),
            cache_dir: cache_dir.clone(),
            project_cache_ttl_secs: 3600,
            request_timeout_secs: args.request_timeout_secs,
        },
        proxy_listener,
    ));

    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(args.clients.max(1))
        .build()?;
    let simple_url = format!("http://{proxy_addr}/simple/demo/");
    let html = client.get(simple_url).send().await?.text().await?;
    let file_path = html
        .split('"')
        .find(|part| part.starts_with("/root/pypi/+f/"))
        .and_then(|part| part.split('#').next())
        .ok_or("proxy simple page did not contain a cached file link")?;
    let file_url = format!("http://{proxy_addr}{file_path}");

    let start = Instant::now();
    let mut handles = Vec::with_capacity(args.clients);
    match client_start {
        ClientStart::Simultaneous => {
            let barrier = Arc::new(Barrier::new(args.clients + 1));
            for id in 0..args.clients {
                let client = client.clone();
                let file_url = file_url.clone();
                let barrier = barrier.clone();
                let read_delay = if id == 0 { slow_client_delay } else { None };
                handles.push(tokio::spawn(async move {
                    barrier.wait().await;
                    download_client(id, client, file_url, read_delay).await
                }));
            }
            barrier.wait().await;
        }
        ClientStart::LateJoin => {
            let leader_client = client.clone();
            let leader_url = file_url.clone();
            handles.push(tokio::spawn(async move {
                download_client(0, leader_client, leader_url, None).await
            }));
            let total_chunks = (args.wheel_mib * 1024).div_ceil(args.chunk_kib);
            let join_after_chunks = (total_chunks / 2).max(1);
            while upstream.chunks_sent.load(Ordering::SeqCst) < join_after_chunks {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            for id in 1..args.clients {
                let client = client.clone();
                let file_url = file_url.clone();
                handles.push(tokio::spawn(async move {
                    download_client(id, client, file_url, None).await
                }));
            }
        }
    }

    let mut metrics = Vec::with_capacity(args.clients);
    for handle in handles {
        metrics.push(handle.await?);
    }
    let wall = start.elapsed();

    proxy_task.abort();
    upstream.task.abort();

    metrics.sort_by_key(|metric| metric.id);
    print_report(
        name,
        args,
        chunk_delay,
        upstream.file_requests.load(Ordering::SeqCst),
        upstream.chunks_sent.load(Ordering::SeqCst),
        wall,
        &metrics,
    );
    Ok(())
}

async fn download_client(
    id: usize,
    client: reqwest::Client,
    file_url: String,
    read_delay: Option<Duration>,
) -> ClientMetric {
    let started = Instant::now();
    let response = match client.get(file_url).send().await {
        Ok(response) => response,
        Err(err) => {
            return ClientMetric {
                id,
                status: None,
                content_length: None,
                bytes: 0,
                chunks: 0,
                first_byte: started.elapsed(),
                full_bytes: None,
                total: started.elapsed(),
                error: Some(err.to_string()),
            };
        }
    };
    let status = response.status();
    let content_length = response.content_length();
    let mut stream = response.bytes_stream();
    let mut first_byte = None;
    let mut full_bytes = None;
    let mut bytes = 0_u64;
    let mut chunks = 0_u64;
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(err) => {
                return ClientMetric {
                    id,
                    status: Some(status),
                    content_length,
                    bytes,
                    chunks,
                    first_byte: first_byte.unwrap_or_else(|| started.elapsed()),
                    full_bytes,
                    total: started.elapsed(),
                    error: Some(err.to_string()),
                };
            }
        };
        if first_byte.is_none() {
            first_byte = Some(started.elapsed());
        }
        chunks += 1;
        bytes += chunk.len() as u64;
        if full_bytes.is_none()
            && let Some(content_length) = content_length
            && bytes >= content_length
        {
            full_bytes = Some(started.elapsed());
        }
        if let Some(read_delay) = read_delay
            && !read_delay.is_zero()
        {
            tokio::time::sleep(read_delay).await;
        }
    }
    ClientMetric {
        id,
        status: Some(status),
        content_length,
        bytes,
        chunks,
        first_byte: first_byte.unwrap_or_else(|| started.elapsed()),
        full_bytes,
        total: started.elapsed(),
        error: None,
    }
}

fn print_report(
    name: &str,
    args: &Args,
    chunk_delay: Duration,
    upstream_requests: usize,
    upstream_chunks: usize,
    wall: Duration,
    metrics: &[ClientMetric],
) {
    let total_client_bytes = metrics.iter().map(|metric| metric.bytes).sum::<u64>();
    let max_total = metrics
        .iter()
        .map(|metric| metric.total)
        .max()
        .unwrap_or_default();
    let throughput_mib_s = total_client_bytes as f64 / 1024.0 / 1024.0 / max_total.as_secs_f64();
    println!();
    println!("scenario: {name}");
    println!(
        "config: clients={} wheel_mib={} chunk_kib={} upstream_chunk_delay_ms={}",
        args.clients,
        args.wheel_mib,
        args.chunk_kib,
        chunk_delay.as_millis()
    );
    println!("upstream_file_requests: {upstream_requests}");
    println!("upstream_chunks_sent: {upstream_chunks}");
    println!("wall_time_ms: {:.2}", wall.as_secs_f64() * 1000.0);
    println!("aggregate_client_bytes: {total_client_bytes}");
    println!("effective_client_throughput_mib_s: {throughput_mib_s:.2}");
    println!("clients:");
    for metric in metrics {
        println!(
            "  id={} status={} content_length={} bytes={} chunks={} first_byte_ms={:.2} full_bytes_ms={} total_ms={:.2} error={}",
            metric.id,
            metric
                .status
                .map(|status| status.as_u16().to_string())
                .unwrap_or_else(|| "-".to_string()),
            metric
                .content_length
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            metric.bytes,
            metric.chunks,
            metric.first_byte.as_secs_f64() * 1000.0,
            metric
                .full_bytes
                .map(|value| format!("{:.2}", value.as_secs_f64() * 1000.0))
                .unwrap_or_else(|| "-".to_string()),
            metric.total.as_secs_f64() * 1000.0,
            metric.error.as_deref().unwrap_or("-")
        );
    }
}

struct UpstreamServer {
    base_url: String,
    file_requests: Arc<AtomicUsize>,
    chunks_sent: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

async fn spawn_upstream(state: UpstreamState) -> std::io::Result<UpstreamServer> {
    let file_requests = state.file_requests.clone();
    let chunks_sent = state.chunks_sent.clone();
    let app = Router::new()
        .route("/simple/demo/", get(simple_page))
        .route("/packages/demo-1.0.whl", get(package_file))
        .route("/packages-static/demo-1.0.whl", get(package_file_static))
        .with_state(Arc::new(state));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let task = tokio::spawn(async move {
        axum::serve(
            listener.tap_io(|stream| {
                let _ = stream.set_nodelay(true);
            }),
            app,
        )
        .await
        .map_err(std::io::Error::other)
    });
    Ok(UpstreamServer {
        base_url: format!("http://{addr}/"),
        file_requests,
        chunks_sent,
        task,
    })
}

async fn simple_page(State(state): State<Arc<UpstreamState>>) -> impl IntoResponse {
    let body = format!(
        r#"<!doctype html><html><body><a href="../../packages/demo-1.0.whl#sha256={}">demo-1.0.whl</a></body></html>"#,
        state.sha256
    );
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/html; charset=utf-8")
        .body(Body::from(body))
        .unwrap()
}

async fn package_file_static(State(state): State<Arc<UpstreamState>>) -> impl IntoResponse {
    state.file_requests.fetch_add(1, Ordering::SeqCst);
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/octet-stream")
        .header(CONTENT_LENGTH, state.wheel.len().to_string())
        .body(Body::from(Bytes::copy_from_slice(state.wheel.as_slice())))
        .unwrap()
}

async fn package_file(
    State(state): State<Arc<UpstreamState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    state.file_requests.fetch_add(1, Ordering::SeqCst);
    let start = headers
        .get(RANGE)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_range_start)
        .unwrap_or(0);
    if start as usize >= state.wheel.len() {
        return Response::builder()
            .status(StatusCode::RANGE_NOT_SATISFIABLE)
            .body(Body::empty())
            .unwrap();
    }

    let total_len = state.wheel.len() as u64;
    let status = if start > 0 {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };
    let chunk_bytes = state.chunk_bytes;
    let chunk_delay = state.chunk_delay;
    let wheel = state.wheel.clone();
    let chunks_sent = state.chunks_sent.clone();
    let body_stream = stream::unfold(start as usize, move |offset| {
        let wheel = wheel.clone();
        let chunks_sent = chunks_sent.clone();
        async move {
            if offset >= wheel.len() {
                return None;
            }
            if !chunk_delay.is_zero() {
                tokio::time::sleep(chunk_delay).await;
            }
            let end = (offset + chunk_bytes).min(wheel.len());
            chunks_sent.fetch_add(1, Ordering::SeqCst);
            Some((
                Ok::<Bytes, std::io::Error>(Bytes::copy_from_slice(&wheel[offset..end])),
                end,
            ))
        }
    });

    let mut response = Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/octet-stream")
        .header(CONTENT_LENGTH, (total_len - start).to_string());
    if start > 0 {
        response = response.header(
            CONTENT_RANGE,
            HeaderValue::from_str(&format!("bytes {start}-{}/{}", total_len - 1, total_len))
                .unwrap(),
        );
    }
    response.body(Body::from_stream(body_stream)).unwrap()
}

fn parse_range_start(value: &str) -> Option<u64> {
    let value = value.strip_prefix("bytes=")?;
    let (start, _) = value.split_once('-')?;
    start.parse().ok()
}

fn make_wheel(bytes: usize) -> Vec<u8> {
    let mut wheel = Vec::with_capacity(bytes);
    while wheel.len() < bytes {
        let n = wheel.len();
        wheel.extend_from_slice(format!("fake-wheel-block-{n:016x}\n").as_bytes());
    }
    wheel.truncate(bytes);
    wheel
}

fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{}-{nanos}", std::process::id())
}
