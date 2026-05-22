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

    #[arg(long, default_value_t = 1)]
    projects: usize,

    #[arg(long, default_value_t = 250)]
    links: usize,

    #[arg(long, default_value_t = 5)]
    min_links: usize,

    #[arg(long, default_value = "127.0.0.1")]
    bind_host: String,

    #[arg(long, value_enum, default_value_t = Mode::Html)]
    mode: Mode,

    #[arg(long, value_enum, default_value_t = Workload::Fixed)]
    workload: Workload,

    #[arg(long, value_enum, default_value_t = Distribution::RoundRobin)]
    distribution: Distribution,

    #[arg(long, default_value_t = 100)]
    warm_project_percent: usize,

    #[arg(long, default_value_t = 15)]
    request_timeout_secs: u64,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Mode {
    Html,
    Json,
    NotModified,
    Mixed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Workload {
    Fixed,
    Realistic,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Distribution {
    RoundRobin,
    Hotspot,
}

#[derive(Clone)]
struct UpstreamState {
    link_profile: LinkProfile,
    project_requests: Arc<AtomicUsize>,
}

#[derive(Debug, Clone, Copy)]
struct LinkProfile {
    workload: Workload,
    links: usize,
    min_links: usize,
}

#[derive(Debug, Clone)]
struct ProjectSpec {
    name: String,
    link_count: usize,
}

#[derive(Debug, Clone, Default)]
struct PrimeResult {
    json_etag: Option<String>,
}

#[derive(Debug, Clone, Copy)]
enum RequestKind {
    Html,
    Json,
    NotModified,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let link_profile = LinkProfile {
        workload: args.workload,
        links: args.links,
        min_links: args.min_links,
    };
    let upstream = spawn_upstream(link_profile).await?;
    let proxy_listener = TcpListener::bind((args.bind_host.as_str(), 0)).await?;
    let proxy_addr = proxy_listener.local_addr()?;
    let cache_dir = std::env::temp_dir().join(format!("pytail-hot-path-{}", unique_suffix()));
    tokio::fs::create_dir_all(&cache_dir).await?;
    let proxy_task = tokio::spawn(pytail::server::serve_listener(
        AppConfig {
            bind: proxy_addr.to_string(),
            upstream_base_url: upstream.base_url.clone(),
            pytorch_wheels_upstream_base_url: upstream.base_url.clone(),
            cache_dir,
            cache_max_size: 0,
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
        .no_proxy()
        .build()?;
    let project_count = args.projects.max(1);
    let projects = build_projects(project_count, link_profile);
    let urls = projects
        .iter()
        .map(|project| format!("http://{proxy_addr}/simple/{}/", project.name))
        .collect::<Vec<_>>();
    let warm_project_count = warm_project_count(project_count, args.warm_project_percent);
    let mut prime_results = vec![PrimeResult::default(); urls.len()];
    for project_index in 0..warm_project_count {
        prime_results[project_index] = prime_project(&client, &urls[project_index]).await?;
    }

    let before_upstream_requests = upstream.state.project_requests.load(Ordering::SeqCst);
    let next_request = Arc::new(AtomicUsize::new(0));
    let urls = Arc::new(urls);
    let projects = Arc::new(projects);
    let prime_results = Arc::new(prime_results);
    let started = Instant::now();
    let mut workers = Vec::with_capacity(args.clients);

    for _ in 0..args.clients {
        let client = client.clone();
        let urls = urls.clone();
        let projects = projects.clone();
        let prime_results = prime_results.clone();
        let next_request = next_request.clone();
        let total_requests = args.requests;
        let distribution = args.distribution;
        let mode = args.mode;
        workers.push(tokio::spawn(async move {
            let mut latencies = Vec::new();
            loop {
                let request_id = next_request.fetch_add(1, Ordering::Relaxed);
                if request_id >= total_requests {
                    return Ok::<Vec<u128>, String>(latencies);
                }

                let project_index = select_project(request_id, projects.len(), distribution);
                let request_kind =
                    select_request_kind(request_id, mode, &prime_results[project_index]);
                let mut request = client.get(&urls[project_index]);
                match request_kind {
                    RequestKind::Html => {}
                    RequestKind::Json | RequestKind::NotModified => {
                        request = request.header(ACCEPT, "application/vnd.pypi.simple.v1+json");
                    }
                }
                if let RequestKind::NotModified = request_kind
                    && let Some(etag) = &prime_results[project_index].json_etag
                {
                    request = request.header(IF_NONE_MATCH, etag);
                }

                let started = Instant::now();
                let response = request.send().await.map_err(|err| err.to_string())?;
                let expected = if matches!(request_kind, RequestKind::NotModified)
                    && prime_results[project_index].json_etag.is_some()
                {
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
        "config: clients={} requests={} projects={} warm_projects={} links={} min_links={} workload={:?} distribution={:?} bind_host={}",
        args.clients,
        args.requests,
        project_count,
        warm_project_count,
        args.links,
        args.min_links,
        args.workload,
        args.distribution,
        args.bind_host
    );
    println!(
        "project_links: min={} p50={} p95={} max={}",
        project_link_percentile(&projects, 0.0),
        project_link_percentile(&projects, 50.0),
        project_link_percentile(&projects, 95.0),
        project_link_percentile(&projects, 100.0)
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
) -> Result<PrimeResult, Box<dyn std::error::Error>> {
    let html_response = client.get(url).send().await?;
    if html_response.status() != StatusCode::OK {
        return Err(format!("html prime request failed with {}", html_response.status()).into());
    }
    let _ = html_response.bytes().await?;

    let response = client
        .get(url)
        .header(ACCEPT, "application/vnd.pypi.simple.v1+json")
        .send()
        .await?;
    if response.status() != StatusCode::OK {
        return Err(format!("json prime request failed with {}", response.status()).into());
    }
    let json_etag = response
        .headers()
        .get(ETAG)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned);
    let _ = response.bytes().await?;
    Ok(PrimeResult { json_etag })
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

async fn spawn_upstream(link_profile: LinkProfile) -> Result<Upstream, Box<dyn std::error::Error>> {
    let state = UpstreamState {
        link_profile,
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
    let links = link_count_for_project(&project, state.link_profile);
    let mut response = Response::new(Body::from(render_upstream_project(&project, links)));
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    response
}

fn render_upstream_project(project: &str, links: usize) -> String {
    let mut html = String::from("<!doctype html><html><body>\n");
    for index in 0..links {
        let hash = format!("{index:064x}");
        html.push_str("<a href=\"../../packages/");
        html.push_str(project);
        html.push('-');
        html.push_str(&index.to_string());
        html.push_str(".whl#sha256=");
        html.push_str(&hash);
        html.push_str("\" data-requires-python=\"&gt;=3.8\">");
        html.push_str(project);
        html.push('-');
        html.push_str(&index.to_string());
        html.push_str(".whl</a>\n");
    }
    html.push_str("</body></html>\n");
    html
}

fn project_name(project_id: usize) -> String {
    if let Some(name) = COMMON_PROJECTS.get(project_id) {
        return (*name).to_string();
    }
    let prefix = COMMON_PREFIXES[project_id % COMMON_PREFIXES.len()];
    format!("{prefix}-{project_id}")
}

fn build_projects(project_count: usize, link_profile: LinkProfile) -> Vec<ProjectSpec> {
    (0..project_count)
        .map(|project_id| {
            let name = if link_profile.workload == Workload::Fixed {
                fixed_project_name(project_id)
            } else {
                project_name(project_id)
            };
            let link_count = link_count_for_project(&name, link_profile);
            ProjectSpec { name, link_count }
        })
        .collect()
}

fn fixed_project_name(project_id: usize) -> String {
    if project_id == 0 {
        return "demo".to_string();
    }
    format!("demo-{project_id}")
}

fn link_count_for_project(project: &str, profile: LinkProfile) -> usize {
    if profile.workload == Workload::Fixed {
        return profile.links.max(1);
    }
    let min_links = profile.min_links.min(profile.links).max(1);
    let max_links = profile.links.max(min_links);
    let spread = max_links - min_links + 1;
    min_links + (stable_hash(project) as usize % spread)
}

fn warm_project_count(project_count: usize, warm_project_percent: usize) -> usize {
    let percent = warm_project_percent.min(100);
    ((project_count * percent) + 99) / 100
}

fn select_project(request_id: usize, project_count: usize, distribution: Distribution) -> usize {
    match distribution {
        Distribution::RoundRobin => request_id % project_count,
        Distribution::Hotspot => {
            let hot_count = (project_count / 5).max(1);
            let mixed = stable_mix(request_id as u64);
            if request_id % 10 < 8 || hot_count == project_count {
                (mixed as usize) % hot_count
            } else {
                hot_count + ((mixed as usize) % (project_count - hot_count))
            }
        }
    }
}

fn select_request_kind(request_id: usize, mode: Mode, prime: &PrimeResult) -> RequestKind {
    match mode {
        Mode::Html => RequestKind::Html,
        Mode::Json => RequestKind::Json,
        Mode::NotModified => {
            if prime.json_etag.is_some() {
                RequestKind::NotModified
            } else {
                RequestKind::Json
            }
        }
        Mode::Mixed => {
            let bucket = stable_mix(request_id as u64) % 100;
            if bucket < 65 {
                RequestKind::Html
            } else if bucket < 90 {
                RequestKind::Json
            } else if prime.json_etag.is_some() {
                RequestKind::NotModified
            } else {
                RequestKind::Json
            }
        }
    }
}

fn project_link_percentile(projects: &[ProjectSpec], percentile: f64) -> usize {
    let mut counts = projects
        .iter()
        .map(|project| project.link_count)
        .collect::<Vec<_>>();
    counts.sort_unstable();
    let rank = ((percentile / 100.0) * (counts.len().saturating_sub(1)) as f64).round() as usize;
    counts[rank.min(counts.len() - 1)]
}

fn stable_hash(value: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn stable_mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58476d1ce4e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}

fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{nanos}-{}", std::process::id())
}

const COMMON_PROJECTS: &[&str] = &[
    "pip",
    "setuptools",
    "wheel",
    "requests",
    "urllib3",
    "certifi",
    "charset-normalizer",
    "idna",
    "packaging",
    "typing-extensions",
    "six",
    "numpy",
    "pandas",
    "python-dateutil",
    "pytz",
    "pyyaml",
    "click",
    "jinja2",
    "markupsafe",
    "attrs",
    "platformdirs",
    "filelock",
    "pydantic",
    "pydantic-core",
    "fastapi",
    "starlette",
    "uvicorn",
    "anyio",
    "httpx",
    "httpcore",
    "h11",
    "sniffio",
    "pytest",
    "pluggy",
    "iniconfig",
    "tomli",
    "cryptography",
    "cffi",
    "pycparser",
    "rich",
    "pygments",
    "markdown-it-py",
    "mdurl",
    "sqlalchemy",
    "greenlet",
    "alembic",
    "django",
    "asgiref",
    "flask",
    "werkzeug",
    "itsdangerous",
    "scipy",
    "matplotlib",
    "pillow",
    "contourpy",
    "kiwisolver",
    "fonttools",
    "cycler",
    "torch",
    "torchvision",
    "torchaudio",
    "transformers",
    "tokenizers",
    "huggingface-hub",
    "safetensors",
];

const COMMON_PREFIXES: &[&str] = &[
    "pytest",
    "django",
    "fastapi",
    "pydantic",
    "types",
    "opentelemetry",
    "google",
    "azure",
    "apache",
    "jupyter",
    "sphinx",
    "mkdocs",
];
