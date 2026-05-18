use axum::Router;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::header::{ACCEPT, CONTENT_TYPE, ETAG, IF_NONE_MATCH};
use axum::http::{HeaderValue, Response, StatusCode};
use axum::routing::get;
use clap::{Parser, ValueEnum};
use pytail::config::AppConfig;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;

#[derive(Debug, Parser)]
#[command(
    name = "hot_path_bench",
    about = "Local benchmark for hot /simple/{project}/ cache hits"
)]
struct Args {
    #[arg(long, default_value_t = 32)]
    clients: usize,

    #[arg(long, default_value_t = 10_000)]
    requests: usize,

    #[arg(long, default_value_t = 250)]
    links: usize,

    #[arg(long, value_enum, default_value_t = Mode::Html)]
    mode: Mode,

    #[arg(long, default_value_t = 15)]
    request_timeout_secs: u64,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Mode {
    Html,
    Json,
    NotModified,
}

#[derive(Clone)]
struct UpstreamState {
    project_body: Arc<String>,
    project_requests: Arc<AtomicUsize>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let upstream = spawn_upstream(args.links).await?;
    let proxy_listener = TcpListener::bind("127.0.0.1:0").await?;
    let proxy_addr = proxy_listener.local_addr()?;
    let cache_dir = std::env::temp_dir().join(format!("pytail-hot-path-{}", unique_suffix()));
    tokio::fs::create_dir_all(&cache_dir).await?;
    let proxy_task = tokio::spawn(pytail::server::serve_listener(
        AppConfig {
            bind: proxy_addr.to_string(),
            upstream_base_url: upstream.base_url.clone(),
            pytorch_wheels_upstream_base_url: upstream.base_url.clone(),
            cache_dir,
            project_cache_ttl_secs: 3600,
            request_timeout_secs: args.request_timeout_secs,
            stats_interval_secs: 0,
            verbose: false,
        },
        proxy_listener,
    ));

    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(args.clients.max(1))
        .timeout(Duration::from_secs(args.request_timeout_secs))
        .build()?;
    let url = format!("http://{proxy_addr}/simple/demo/");
    let (accept, etag) = prime_project(&client, &url, args.mode).await?;

    let before_upstream_requests = upstream.state.project_requests.load(Ordering::SeqCst);
    let next_request = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    let mut workers = Vec::with_capacity(args.clients);

    for _ in 0..args.clients {
        let client = client.clone();
        let url = url.clone();
        let next_request = next_request.clone();
        let accept = accept.clone();
        let etag = etag.clone();
        let total_requests = args.requests;
        workers.push(tokio::spawn(async move {
            let mut latencies = Vec::new();
            loop {
                let request_id = next_request.fetch_add(1, Ordering::Relaxed);
                if request_id >= total_requests {
                    return Ok::<Vec<u128>, String>(latencies);
                }

                let mut request = client.get(&url);
                if let Some(accept) = &accept {
                    request = request.header(ACCEPT, accept);
                }
                if let Some(etag) = &etag {
                    request = request.header(IF_NONE_MATCH, etag);
                }

                let started = Instant::now();
                let response = request.send().await.map_err(|err| err.to_string())?;
                let expected = if etag.is_some() {
                    StatusCode::NOT_MODIFIED
                } else {
                    StatusCode::OK
                };
                if response.status() != expected {
                    return Err(format!(
                        "unexpected response status: expected {expected}, got {}",
                        response.status()
                    ));
                }
                let _ = response.bytes().await.map_err(|err| err.to_string())?;
                latencies.push(started.elapsed().as_micros());
            }
        }));
    }

    let mut latencies = Vec::with_capacity(args.requests);
    for worker in workers {
        latencies.extend(worker.await??);
    }
    let elapsed = started.elapsed();
    latencies.sort_unstable();
    let upstream_project_requests =
        upstream.state.project_requests.load(Ordering::SeqCst) - before_upstream_requests;

    println!("scenario: hot-project-{:?}", args.mode);
    println!(
        "config: clients={} requests={} links={}",
        args.clients, args.requests, args.links
    );
    println!("upstream_project_requests: {upstream_project_requests}");
    println!("wall_time_ms: {:.2}", elapsed.as_secs_f64() * 1000.0);
    println!(
        "requests_per_sec: {:.2}",
        args.requests as f64 / elapsed.as_secs_f64()
    );
    println!("latency_p50_us: {}", percentile(&latencies, 50.0));
    println!("latency_p95_us: {}", percentile(&latencies, 95.0));
    println!("latency_p99_us: {}", percentile(&latencies, 99.0));

    proxy_task.abort();
    Ok(())
}

async fn prime_project(
    client: &reqwest::Client,
    url: &str,
    mode: Mode,
) -> Result<(Option<String>, Option<String>), Box<dyn std::error::Error>> {
    let accept = match mode {
        Mode::Html => None,
        Mode::Json | Mode::NotModified => Some("application/vnd.pypi.simple.v1+json".to_string()),
    };
    let mut request = client.get(url);
    if let Some(accept) = &accept {
        request = request.header(ACCEPT, accept);
    }
    let response = request.send().await?;
    if response.status() != StatusCode::OK {
        return Err(format!("prime request failed with {}", response.status()).into());
    }
    let etag = response
        .headers()
        .get(ETAG)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned);
    let _ = response.bytes().await?;
    let etag = match mode {
        Mode::NotModified => etag,
        Mode::Html | Mode::Json => None,
    };
    Ok((accept, etag))
}

fn percentile(values: &[u128], percentile: f64) -> u128 {
    if values.is_empty() {
        return 0;
    }
    let rank = ((percentile / 100.0) * (values.len().saturating_sub(1)) as f64).round() as usize;
    values[rank.min(values.len() - 1)]
}

struct Upstream {
    base_url: String,
    state: UpstreamState,
}

async fn spawn_upstream(links: usize) -> Result<Upstream, Box<dyn std::error::Error>> {
    let state = UpstreamState {
        project_body: Arc::new(render_upstream_project(links)),
        project_requests: Arc::new(AtomicUsize::new(0)),
    };
    let app = Router::new()
        .route("/simple/{project}/", get(upstream_project))
        .with_state(state.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Ok(Upstream {
        base_url: format!("http://{addr}/"),
        state,
    })
}

async fn upstream_project(
    State(state): State<UpstreamState>,
    Path(project): Path<String>,
) -> Response<Body> {
    state.project_requests.fetch_add(1, Ordering::SeqCst);
    if project != "demo" {
        let mut response = Response::new(Body::from("not found"));
        *response.status_mut() = StatusCode::NOT_FOUND;
        return response;
    }
    let mut response = Response::new(Body::from(state.project_body.as_str().to_owned()));
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    response
}

fn render_upstream_project(links: usize) -> String {
    let mut html = String::from("<!doctype html><html><body>\n");
    for index in 0..links {
        let hash = format!("{index:064x}");
        html.push_str("<a href=\"../../packages/demo-");
        html.push_str(&index.to_string());
        html.push_str(".whl#sha256=");
        html.push_str(&hash);
        html.push_str("\" data-requires-python=\"&gt;=3.8\">demo-");
        html.push_str(&index.to_string());
        html.push_str(".whl</a>\n");
    }
    html.push_str("</body></html>\n");
    html
}

fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{nanos}-{}", std::process::id())
}
