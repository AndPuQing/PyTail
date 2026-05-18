use crate::cache::{
    BlobInfo, BlobStatus, BlobWrite, CacheStore, CachedLink, ProjectCache, ProjectRecord,
    RootHistorySample, current_unix_secs, fallback_blob_identity, hash_blob_identity,
};
use crate::config::AppConfig;
use crate::range::parse_byte_range;
use crate::simple::{
    RootStats, json_media_type, normalize_project_name, parse_project_json_links,
    parse_project_links, render_project_html_with_file_base, render_project_json_with_file_base,
    render_root_html, render_root_json, wants_json,
};
use crate::upstream::{ProjectFetch, ProjectPageFormat, UpstreamClient};
use axum::Router;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::header::{
    ACCEPT, ACCEPT_RANGES, CONTENT_RANGE, CONTENT_TYPE, ETAG, IF_NONE_MATCH, LOCATION, RANGE,
    WARNING,
};
use axum::http::{HeaderMap, HeaderValue, Response, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::serve::ListenerExt;
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use futures_util::{Stream, StreamExt, TryStreamExt};
use mime_guess::from_path;
use moka::sync::Cache as ConcurrentCache;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io;
use std::io::SeekFrom;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Notify, OwnedMutexGuard, broadcast, mpsc};
use tokio_util::io::ReaderStream;
use tower_http::compression::{CompressionLayer, CompressionLevel};
use tracing::{debug, error, info, warn};
use url::Url;

#[derive(Clone)]
struct AppState {
    cache: CacheStore,
    upstream: UpstreamClient,
    pytorch_wheels_upstream: UpstreamClient,
    request_timeout_secs: u64,
    project_cache_ttl_secs: u64,
    locks: RequestLocks,
    metrics: CacheMetrics,
    active_blobs: ActiveBlobRegistry,
    hot_projects: HotProjectCache,
    hot_links: HotLinkCache,
    hot_blobs: HotBlobCache,
}

type ActiveBlobRegistry = Arc<DashMap<String, Arc<ActiveBlob>>>;
type HotProjectCache = ConcurrentCache<String, Arc<HotProject>>;
type HotLinkCache = ConcurrentCache<String, CachedLink>;
type HotBlobCache = ConcurrentCache<String, BlobInfo>;
type RequestLockMap = Arc<DashMap<String, Arc<Mutex<()>>>>;
type Counter = Arc<AtomicU64>;

const REQUEST_LOCK_CLEANUP_THRESHOLD: usize = 1024;
const HOT_PROJECT_CACHE_MAX_ENTRIES: usize = 1024;
const HOT_LINK_CACHE_MAX_ENTRIES: usize = 16 * 1024;
const HOT_BLOB_CACHE_MAX_ENTRIES: usize = 8 * 1024;
const ROOT_STATS_HISTORY_MAX_AGE_SECS: u64 = 24 * 60 * 60;
const FILE_STREAM_BUFFER_SIZE: usize = 256 * 1024;
const PYPI_FILE_BASE_PATH: &str = "/root/pypi/+f";
const PYTORCH_WHEELS_FILE_BASE_PATH: &str = "/pytorch-wheels/+f";

#[derive(Clone)]
struct HotProject {
    cache: ProjectCache,
    rendered: RenderedProject,
}

#[derive(Clone)]
struct RenderedProject {
    html_body: Bytes,
    html_etag: String,
    json_body: Bytes,
    json_etag: String,
}

struct ActiveBlob {
    temp_path: std::path::PathBuf,
    final_path: std::path::PathBuf,
    content_type: String,
    content_length: AtomicU64,
    initial_bytes: u64,
    bytes_written: AtomicU64,
    finished: AtomicBool,
    notify: Notify,
    chunk_tx: broadcast::Sender<BlobChunk>,
    failure: std::sync::Mutex<Option<String>>,
}

#[derive(Clone)]
struct BlobChunk {
    offset: u64,
    bytes: Bytes,
}

#[derive(Clone, Default)]
struct CacheMetrics {
    project_hits: Counter,
    project_misses: Counter,
    blob_hits: Counter,
    blob_misses: Counter,
    blob_coalesced: Counter,
}

#[derive(Clone, Copy)]
struct CacheMetricsSnapshot {
    project_hits: u64,
    project_misses: u64,
    blob_hits: u64,
    blob_misses: u64,
    blob_coalesced: u64,
}

impl CacheMetrics {
    fn incr_project_hit(&self) {
        self.project_hits.fetch_add(1, Ordering::Relaxed);
    }

    fn incr_project_miss(&self) {
        self.project_misses.fetch_add(1, Ordering::Relaxed);
    }

    fn incr_blob_hit(&self) {
        self.blob_hits.fetch_add(1, Ordering::Relaxed);
    }

    fn incr_blob_miss(&self) {
        self.blob_misses.fetch_add(1, Ordering::Relaxed);
    }

    fn incr_blob_coalesced(&self) {
        self.blob_coalesced.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> CacheMetricsSnapshot {
        CacheMetricsSnapshot {
            project_hits: self.project_hits.load(Ordering::Relaxed),
            project_misses: self.project_misses.load(Ordering::Relaxed),
            blob_hits: self.blob_hits.load(Ordering::Relaxed),
            blob_misses: self.blob_misses.load(Ordering::Relaxed),
            blob_coalesced: self.blob_coalesced.load(Ordering::Relaxed),
        }
    }
}

impl CacheMetricsSnapshot {
    fn delta_since(self, previous: Self) -> Self {
        Self {
            project_hits: self.project_hits.saturating_sub(previous.project_hits),
            project_misses: self.project_misses.saturating_sub(previous.project_misses),
            blob_hits: self.blob_hits.saturating_sub(previous.blob_hits),
            blob_misses: self.blob_misses.saturating_sub(previous.blob_misses),
            blob_coalesced: self.blob_coalesced.saturating_sub(previous.blob_coalesced),
        }
    }
}

#[derive(Clone, Default)]
struct RequestLocks {
    projects: RequestLockMap,
    blobs: RequestLockMap,
}

impl RequestLocks {
    async fn project_guard(&self, project: &str) -> OwnedMutexGuard<()> {
        self.guard(&self.projects, project).await
    }

    async fn blob_guard(&self, blob_kind: &str, blob_id: &str) -> OwnedMutexGuard<()> {
        self.guard(&self.blobs, &format!("{blob_kind}:{blob_id}"))
            .await
    }

    async fn guard(&self, map: &RequestLockMap, key: &str) -> OwnedMutexGuard<()> {
        if map.len() >= REQUEST_LOCK_CLEANUP_THRESHOLD {
            map.retain(|_, lock| Arc::strong_count(lock) > 1);
        }
        let lock = map
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        lock.lock_owned().await
    }
}

pub async fn run(config: AppConfig) -> io::Result<()> {
    let listener = TcpListener::bind(&config.bind).await?;
    serve_listener(config, listener).await
}

pub async fn serve_listener(config: AppConfig, listener: TcpListener) -> io::Result<()> {
    let local_addr = listener.local_addr()?;
    info!(
        bind = %local_addr,
        upstream = %config.upstream_base_url,
        pytorch_wheels_upstream = %config.pytorch_wheels_upstream_base_url,
        cache_dir = %config.cache_dir.display(),
        project_cache_ttl_secs = config.project_cache_ttl_secs,
        request_timeout_secs = config.request_timeout_secs,
        stats_interval_secs = config.stats_interval_secs,
        "starting pytail server"
    );
    let cache = CacheStore::new(config.cache_dir.clone());
    cache.initialize().await?;
    let state = AppState {
        cache,
        upstream: UpstreamClient::new(&config.upstream_base_url, config.request_timeout_secs)?,
        pytorch_wheels_upstream: UpstreamClient::new_project_root(
            &config.pytorch_wheels_upstream_base_url,
            config.request_timeout_secs,
        )?,
        request_timeout_secs: config.request_timeout_secs,
        project_cache_ttl_secs: config.project_cache_ttl_secs,
        locks: RequestLocks::default(),
        metrics: CacheMetrics::default(),
        active_blobs: ActiveBlobRegistry::default(),
        hot_projects: ConcurrentCache::new(HOT_PROJECT_CACHE_MAX_ENTRIES as u64),
        hot_links: ConcurrentCache::new(HOT_LINK_CACHE_MAX_ENTRIES as u64),
        hot_blobs: ConcurrentCache::new(HOT_BLOB_CACHE_MAX_ENTRIES as u64),
    };
    if config.stats_interval_secs > 0 {
        spawn_cache_stats_logger(state.metrics.clone(), config.stats_interval_secs);
        spawn_root_stats_sampler(
            state.cache.clone(),
            state.metrics.clone(),
            config.stats_interval_secs,
        );
    }
    let app = app(state);
    axum::serve(
        listener.tap_io(|stream| {
            let _ = stream.set_nodelay(true);
        }),
        app,
    )
    .await
    .map_err(io::Error::other)
}

fn spawn_cache_stats_logger(metrics: CacheMetrics, interval_secs: u64) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
        interval.tick().await;
        let mut previous = metrics.snapshot();
        loop {
            interval.tick().await;
            let current = metrics.snapshot();
            let window = current.delta_since(previous);
            previous = current;
            log_cache_stats("window", window);
            log_cache_stats("total", current);
        }
    });
}

fn log_cache_stats(scope: &str, snapshot: CacheMetricsSnapshot) {
    let project_total = snapshot.project_hits + snapshot.project_misses;
    let blob_total = snapshot.blob_hits + snapshot.blob_misses;
    if project_total == 0 && blob_total == 0 && snapshot.blob_coalesced == 0 {
        return;
    }
    info!(
        scope,
        project_hits = snapshot.project_hits,
        project_misses = snapshot.project_misses,
        project_hit_rate = format_args!("{:.2}%", hit_rate(snapshot.project_hits, project_total)),
        blob_hits = snapshot.blob_hits,
        blob_misses = snapshot.blob_misses,
        blob_hit_rate = format_args!("{:.2}%", hit_rate(snapshot.blob_hits, blob_total)),
        blob_coalesced = snapshot.blob_coalesced,
        "cache stats"
    );
}

fn spawn_root_stats_sampler(cache: CacheStore, metrics: CacheMetrics, interval_secs: u64) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
        loop {
            interval.tick().await;
            let Ok(cache_summary) = cache.cache_summary().await else {
                warn!("failed to sample cache summary for UI trends");
                continue;
            };
            let metrics = metrics.snapshot();
            if let Err(err) = record_root_stats_sample(
                &cache,
                RootHistorySample {
                    sampled_at: current_unix_secs(),
                    cached_size_bytes: cache_summary.cached_size_bytes,
                    package_count: cache_summary.project_count,
                    hit_rate_percent: combined_hit_rate(&metrics),
                },
            )
            .await
            {
                warn!(error = %err, "failed to record root stats sample");
            }
        }
    });
}

fn hit_rate(hits: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        hits as f64 * 100.0 / total as f64
    }
}

fn combined_hit_rate(snapshot: &CacheMetricsSnapshot) -> f64 {
    let hits = snapshot.project_hits + snapshot.blob_hits;
    let misses = snapshot.project_misses + snapshot.blob_misses;
    hit_rate(hits, hits + misses)
}

async fn record_root_stats_sample(cache: &CacheStore, sample: RootHistorySample) -> io::Result<()> {
    cache.record_root_stats_sample(sample).await?;
    let cutoff = sample
        .sampled_at
        .saturating_sub(ROOT_STATS_HISTORY_MAX_AGE_SECS);
    cache.prune_root_stats_samples_before(cutoff).await
}

fn app(state: AppState) -> Router {
    Router::new()
        .route("/", get(root_redirect))
        .route("/simple", get(simple_root))
        .route("/simple/", get(simple_root))
        .route("/simple/{project}", get(simple_project))
        .route("/simple/{project}/", get(simple_project))
        .route("/pytorch-wheels/{project}", get(pytorch_wheels_project))
        .route("/pytorch-wheels/{project}/", get(pytorch_wheels_project))
        .route(
            "/pytorch-wheels/{channel}/{project}",
            get(pytorch_wheels_channel_project),
        )
        .route(
            "/pytorch-wheels/{channel}/{project}/",
            get(pytorch_wheels_channel_project),
        )
        .route(
            "/root/pypi/{plusf}/{route_a}/{route_b}/{filename}",
            get(package_file),
        )
        .route(
            "/pytorch-wheels/{plusf}/{route_a}/{route_b}/{filename}",
            get(pytorch_wheels_package_file),
        )
        .route(
            "/pytorch-wheels/{channel}/{plusf}/{route_a}/{route_b}/{filename}",
            get(pytorch_wheels_channel_package_file),
        )
        .layer(
            CompressionLayer::new()
                .quality(CompressionLevel::Fastest)
                .compress_when(should_compress_simple_response),
        )
        .with_state(Arc::new(state))
}

fn should_compress_simple_response(
    status: StatusCode,
    _version: axum::http::Version,
    headers: &HeaderMap,
    _extensions: &axum::http::Extensions,
) -> bool {
    if status != StatusCode::OK {
        return false;
    }
    let Some(content_length) = headers
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
    else {
        return false;
    };
    if content_length < 64 * 1024 {
        return false;
    }
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|content_type| {
            content_type.starts_with("text/html")
                || content_type.starts_with(json_media_type())
                || content_type.starts_with("application/json")
        })
}

async fn root_redirect() -> impl IntoResponse {
    (
        StatusCode::TEMPORARY_REDIRECT,
        [(LOCATION, HeaderValue::from_static("/simple/"))],
    )
}

async fn simple_root(
    State(state): State<Arc<AppState>>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<Response<Body>, StatusCode> {
    if let Some(project) = query.get("q").map(|value| value.trim())
        && !project.is_empty()
    {
        let normalized = normalize_project_name(project);
        let mut response = Response::new(Body::empty());
        *response.status_mut() = StatusCode::SEE_OTHER;
        response
            .headers_mut()
            .insert(LOCATION, header(&format!("/simple/{normalized}/")));
        return Ok(response);
    }

    debug!("serving simple root");
    let format_json = wants_json(header_value(&headers, ACCEPT));
    let body = if format_json {
        let projects = state
            .cache
            .list_projects()
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        render_root_json(&projects)
    } else {
        let projects = state
            .cache
            .list_project_summaries()
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let cache_summary = state
            .cache
            .cache_summary()
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let metrics = state.metrics.snapshot();
        let sampled_at = current_unix_secs();
        record_root_stats_sample(
            &state.cache,
            RootHistorySample {
                sampled_at,
                cached_size_bytes: cache_summary.cached_size_bytes,
                package_count: cache_summary.project_count,
                hit_rate_percent: combined_hit_rate(&metrics),
            },
        )
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let history = state
            .cache
            .root_stats_history_since(sampled_at.saturating_sub(ROOT_STATS_HISTORY_MAX_AGE_SECS))
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        render_root_html(
            &projects,
            RootStats {
                cached_size_bytes: cache_summary.cached_size_bytes,
                cached_file_count: cache_summary.ready_blob_count,
                package_count: cache_summary.project_count,
                project_hits: metrics.project_hits,
                project_misses: metrics.project_misses,
                blob_hits: metrics.blob_hits,
                blob_misses: metrics.blob_misses,
                history,
            },
        )
    };
    Ok(render_simple_response(
        &headers,
        if format_json {
            json_media_type()
        } else {
            "text/html; charset=utf-8"
        },
        body,
        false,
    ))
}

async fn simple_project(
    State(state): State<Arc<AppState>>,
    Path(project): Path<String>,
    headers: HeaderMap,
) -> Response<Body> {
    let normalized = normalize_project_name(&project);
    debug!(project = %normalized, "serving project page");
    match ensure_project(&state, &normalized).await {
        Ok(ProjectState::Ready(hot_project, stale)) => {
            project_response(&headers, normalized, hot_project, stale)
        }
        Ok(ProjectState::Missing) => {
            info!(project = %normalized, "project not found upstream");
            text_response(StatusCode::NOT_FOUND, "project not found\n")
        }
        Err(status) => {
            warn!(project = %normalized, %status, "project request failed");
            text_response(status, "upstream unavailable\n")
        }
    }
}

async fn pytorch_wheels_project(
    State(state): State<Arc<AppState>>,
    Path(project): Path<String>,
    headers: HeaderMap,
) -> Response<Body> {
    let normalized = normalize_project_name(&project);
    let cache_project = pytorch_wheels_cache_project(None, &normalized);
    debug!(project = %normalized, "serving pytorch wheels project page");
    match ensure_project_with(
        &state,
        &state.pytorch_wheels_upstream,
        &cache_project,
        &normalized,
        PYTORCH_WHEELS_FILE_BASE_PATH,
    )
    .await
    {
        Ok(ProjectState::Ready(hot_project, stale)) => {
            project_response(&headers, normalized, hot_project, stale)
        }
        Ok(ProjectState::Missing) => {
            info!(project = %normalized, "pytorch wheels project not found upstream");
            text_response(StatusCode::NOT_FOUND, "project not found\n")
        }
        Err(status) => {
            warn!(project = %normalized, %status, "pytorch wheels project request failed");
            text_response(status, "upstream unavailable\n")
        }
    }
}

async fn pytorch_wheels_channel_project(
    State(state): State<Arc<AppState>>,
    Path((channel, project)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response<Body> {
    let channel = normalize_project_name(&channel);
    let normalized = normalize_project_name(&project);
    let cache_project = pytorch_wheels_cache_project(Some(&channel), &normalized);
    let file_base_path = pytorch_wheels_channel_file_base_path(&channel);
    let upstream = match state
        .pytorch_wheels_upstream
        .child_project_root(&channel, state.request_timeout_secs)
    {
        Ok(upstream) => upstream,
        Err(_) => {
            return text_response(StatusCode::INTERNAL_SERVER_ERROR, "upstream unavailable\n");
        }
    };
    debug!(channel, project = %normalized, "serving pytorch wheels channel project page");
    match ensure_project_with(
        &state,
        &upstream,
        &cache_project,
        &normalized,
        &file_base_path,
    )
    .await
    {
        Ok(ProjectState::Ready(hot_project, stale)) => {
            project_response(&headers, normalized, hot_project, stale)
        }
        Ok(ProjectState::Missing) => {
            info!(channel, project = %normalized, "pytorch wheels project not found upstream");
            text_response(StatusCode::NOT_FOUND, "project not found\n")
        }
        Err(status) => {
            warn!(channel, project = %normalized, %status, "pytorch wheels project request failed");
            text_response(status, "upstream unavailable\n")
        }
    }
}

fn project_response(
    headers: &HeaderMap,
    project: String,
    hot_project: Arc<HotProject>,
    stale: bool,
) -> Response<Body> {
    let format_json = wants_json(header_value(headers, ACCEPT));
    let (content_type, body, etag) = if format_json {
        (
            json_media_type(),
            hot_project.rendered.json_body.clone(),
            hot_project.rendered.json_etag.clone(),
        )
    } else {
        (
            "text/html; charset=utf-8",
            hot_project.rendered.html_body.clone(),
            hot_project.rendered.html_etag.clone(),
        )
    };
    debug!(
        project,
        stale,
        format = if format_json { "json" } else { "html" },
        "project page ready"
    );
    render_cached_simple_response(headers, content_type, body, etag, stale)
}

async fn package_file(
    State(state): State<Arc<AppState>>,
    Path((plusf, route_a, route_b, filename)): Path<(String, String, String, String)>,
    headers: HeaderMap,
) -> Response<Body> {
    package_file_from_parts(state, plusf, route_a, route_b, filename, headers).await
}

async fn pytorch_wheels_package_file(
    State(state): State<Arc<AppState>>,
    Path((plusf, route_a, route_b, filename)): Path<(String, String, String, String)>,
    headers: HeaderMap,
) -> Response<Body> {
    package_file_from_parts(state, plusf, route_a, route_b, filename, headers).await
}

async fn pytorch_wheels_channel_package_file(
    State(state): State<Arc<AppState>>,
    Path((_channel, plusf, route_a, route_b, filename)): Path<(
        String,
        String,
        String,
        String,
        String,
    )>,
    headers: HeaderMap,
) -> Response<Body> {
    package_file_from_parts(state, plusf, route_a, route_b, filename, headers).await
}

async fn package_file_from_parts(
    state: Arc<AppState>,
    plusf: String,
    route_a: String,
    route_b: String,
    filename: String,
    headers: HeaderMap,
) -> Response<Body> {
    if plusf != "+f" {
        debug!(
            plusf,
            route_a, route_b, filename, "rejecting unknown file route"
        );
        return text_response(StatusCode::NOT_FOUND, "file unavailable\n");
    }
    debug!(route_a, route_b, filename, "serving package file");
    let response = if let Some(base_filename) = filename.strip_suffix(".metadata") {
        ensure_metadata_blob(&state, &route_a, &route_b, base_filename, &filename).await
    } else {
        ensure_blob(&state, &route_a, &route_b, &filename).await
    };
    match response {
        Ok(BlobResponse::Ready(blob)) => cached_file_response(&state.cache, &blob, &headers).await,
        Ok(BlobResponse::Streaming(response)) => response,
        Err(status) => text_response(status, "file unavailable\n"),
    }
}

enum ProjectState {
    Ready(Arc<HotProject>, bool),
    Missing,
}

async fn ensure_project(state: &AppState, project: &str) -> Result<ProjectState, StatusCode> {
    ensure_project_with(
        state,
        &state.upstream,
        project,
        project,
        PYPI_FILE_BASE_PATH,
    )
    .await
}

async fn ensure_project_with(
    state: &AppState,
    upstream: &UpstreamClient,
    cache_project: &str,
    display_project: &str,
    file_base_path: &str,
) -> Result<ProjectState, StatusCode> {
    let project = cache_project;
    if let Some(hot_project) = state.hot_projects.get(project)
        && state.cache.project_is_fresh(&hot_project.cache)
    {
        debug!(project, "project cache hit in memory");
        state.metrics.incr_project_hit();
        return Ok(ProjectState::Ready(hot_project, false));
    }

    if let Some(cached) = state
        .cache
        .load_project(project)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        && state.cache.project_is_fresh(&cached)
    {
        debug!(project, "project cache hit on disk");
        state.metrics.incr_project_hit();
        let hot_project = build_hot_project(display_project, file_base_path, cached);
        cache_hot_project(state, project, hot_project.clone()).await;
        return Ok(ProjectState::Ready(hot_project, false));
    }

    let _guard = state.locks.project_guard(project).await;
    let cached = state
        .cache
        .load_project(project)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if let Some(project_cache) = &cached
        && state.cache.project_is_fresh(project_cache)
    {
        debug!(project, "project cache filled while waiting for lock");
        state.metrics.incr_project_hit();
        let hot_project = build_hot_project(display_project, file_base_path, project_cache.clone());
        cache_hot_project(state, project, hot_project.clone()).await;
        return Ok(ProjectState::Ready(hot_project, false));
    }

    let etag = cached
        .as_ref()
        .and_then(|project_cache| project_cache.upstream_etag.as_deref());
    state.metrics.incr_project_miss();
    let result = match upstream.fetch_project(display_project, etag).await {
        Ok(ProjectFetch::Fresh(page)) => {
            info!(project, "fetched fresh project page from upstream");
            let page_url = Url::parse(&page.project_url).map_err(|_| StatusCode::BAD_GATEWAY)?;
            let parsed_links = match page.format {
                ProjectPageFormat::Json => parse_project_json_links(&page.body, &page_url)
                    .unwrap_or_else(|_| parse_project_links(&page.body, &page_url)),
                ProjectPageFormat::Html => parse_project_links(&page.body, &page_url),
            };
            let links = parsed_links
                .into_iter()
                .map(|link| {
                    let (blob_kind, blob_id) = match (&link.hash_name, &link.hash_value) {
                        (Some(hash_name), Some(hash_value)) => {
                            hash_blob_identity(hash_name, hash_value, &link.upstream_url)
                        }
                        _ => fallback_blob_identity(&link.upstream_url),
                    };
                    CachedLink {
                        filename: link.filename,
                        upstream_url: link.upstream_url,
                        blob_kind,
                        blob_id,
                        cached_size_bytes: None,
                        requires_python: link.requires_python,
                        yanked: link.yanked,
                        gpg_sig: link.gpg_sig,
                        dist_info_metadata: link.dist_info_metadata,
                        core_metadata: link.core_metadata,
                        hash_name: link.hash_name,
                        hash_value: link.hash_value,
                    }
                })
                .collect::<Vec<_>>();
            info!(
                project,
                links = links.len(),
                "parsed upstream project links"
            );
            let now = current_unix_secs();
            let record = ProjectRecord {
                project: project.to_string(),
                fetched_at: now,
                expires_at: now + state.project_cache_ttl_secs,
                upstream_etag: page.etag,
                upstream_serial: page.serial,
                upstream_project_url: page.project_url,
                raw_body: page.body,
                links,
            };
            let cached = state
                .cache
                .store_project(&record)
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            let hot_project = build_hot_project(display_project, file_base_path, cached);
            cache_hot_project(state, project, hot_project.clone()).await;
            Ok(ProjectState::Ready(hot_project, false))
        }
        Ok(ProjectFetch::NotModified) => {
            debug!(project, "upstream project page not modified");
            if let Some(mut cached_value) = cached.clone() {
                let now = current_unix_secs();
                cached_value.fetched_at = now;
                cached_value.expires_at = now + state.project_cache_ttl_secs;
                state
                    .cache
                    .touch_project(
                        &cached_value.project,
                        cached_value.fetched_at,
                        cached_value.expires_at,
                    )
                    .await
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                let hot_project = build_hot_project(display_project, file_base_path, cached_value);
                cache_hot_project(state, project, hot_project.clone()).await;
                Ok(ProjectState::Ready(hot_project, false))
            } else {
                Err(StatusCode::BAD_GATEWAY)
            }
        }
        Ok(ProjectFetch::NotFound) => Ok(ProjectState::Missing),
        Err(err) => {
            warn!(project, error = %err, "failed to fetch project page from upstream");
            Err(StatusCode::BAD_GATEWAY)
        }
    };
    match result {
        Ok(value) => Ok(value),
        Err(status) => {
            if let Some(cached) = cached {
                warn!(project, "serving stale project page after refresh failure");
                let hot_project = build_hot_project(display_project, file_base_path, cached);
                cache_hot_project(state, project, hot_project.clone()).await;
                Ok(ProjectState::Ready(hot_project, true))
            } else {
                Err(status)
            }
        }
    }
}

fn build_hot_project(project: &str, file_base_path: &str, cache: ProjectCache) -> Arc<HotProject> {
    let html_body = render_project_html_with_file_base(project, &cache.links, file_base_path);
    let json_body = render_project_json_with_file_base(project, &cache.links, file_base_path);
    Arc::new(HotProject {
        cache,
        rendered: RenderedProject {
            html_etag: response_etag(html_body.as_bytes()),
            json_etag: response_etag(json_body.as_bytes()),
            html_body: Bytes::from(html_body),
            json_body: Bytes::from(json_body),
        },
    })
}

async fn cache_hot_project(state: &AppState, project: &str, hot_project: Arc<HotProject>) {
    for link in &hot_project.cache.links {
        state
            .hot_links
            .insert(route_key_for_link(link), link.clone());
    }
    state.hot_projects.insert(project.to_string(), hot_project);
}

enum BlobResponse {
    Ready(BlobInfo),
    Streaming(Response<Body>),
}

fn blob_key(blob_kind: &str, blob_id: &str) -> String {
    format!("{blob_kind}:{blob_id}")
}

fn route_key(route_a: &str, route_b: &str, filename: &str) -> String {
    format!("{route_a}/{route_b}/{filename}")
}

fn route_key_for_link(link: &CachedLink) -> String {
    if link.blob_kind == "sha256" && link.blob_id.len() >= 16 {
        return route_key(&link.blob_id[..3], &link.blob_id[3..16], &link.filename);
    }
    route_key("_url", &link.blob_id, &link.filename)
}

fn pytorch_wheels_cache_project(channel: Option<&str>, project: &str) -> String {
    match channel {
        Some(channel) => format!("pytorch-wheels-{channel}-{project}"),
        None => format!("pytorch-wheels-{project}"),
    }
}

fn pytorch_wheels_channel_file_base_path(channel: &str) -> String {
    format!("/pytorch-wheels/{channel}/+f")
}

async fn ensure_blob(
    state: &AppState,
    route_a: &str,
    route_b: &str,
    filename: &str,
) -> Result<BlobResponse, StatusCode> {
    let key = route_key(route_a, route_b, filename);
    let link = if let Some(link) = state.hot_links.get(&key) {
        link
    } else {
        let Some(link) = state
            .cache
            .find_link_by_blob(route_a, route_b, filename)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        else {
            return Err(StatusCode::NOT_FOUND);
        };
        state.hot_links.insert(key, link.clone());
        link
    };

    ensure_link_blob(state, link, filename).await
}

async fn ensure_metadata_blob(
    state: &AppState,
    route_a: &str,
    route_b: &str,
    base_filename: &str,
    metadata_filename: &str,
) -> Result<BlobResponse, StatusCode> {
    let base_key = route_key(route_a, route_b, base_filename);
    let link = if let Some(link) = state.hot_links.get(&base_key) {
        link
    } else {
        let Some(link) = state
            .cache
            .find_link_by_blob(route_a, route_b, base_filename)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        else {
            return Err(StatusCode::NOT_FOUND);
        };
        state.hot_links.insert(base_key, link.clone());
        link
    };
    let metadata_value = link
        .dist_info_metadata
        .as_deref()
        .or(link.core_metadata.as_deref());
    if metadata_value.is_none() {
        return Err(StatusCode::NOT_FOUND);
    }
    let upstream_url = format!("{}.metadata", link.upstream_url);
    let (hash_name, hash_value) = metadata_value
        .and_then(metadata_hash)
        .map_or((None, None), |(name, value)| (Some(name), Some(value)));
    let (blob_kind, blob_id) = match (&hash_name, &hash_value) {
        (Some(hash_name), Some(hash_value)) => {
            hash_blob_identity(hash_name, hash_value, &upstream_url)
        }
        _ => fallback_blob_identity(&upstream_url),
    };
    let metadata_link = CachedLink {
        filename: metadata_filename.to_string(),
        upstream_url,
        blob_kind,
        blob_id,
        cached_size_bytes: None,
        requires_python: None,
        yanked: None,
        gpg_sig: None,
        dist_info_metadata: None,
        core_metadata: None,
        hash_name,
        hash_value,
    };
    {
        state.hot_links.insert(
            route_key(route_a, route_b, metadata_filename),
            metadata_link.clone(),
        );
    }
    ensure_link_blob(state, metadata_link, metadata_filename).await
}

fn metadata_hash(value: &str) -> Option<(String, String)> {
    let (name, hash) = value.split_once('=')?;
    if name.is_empty() || hash.is_empty() {
        return None;
    }
    Some((name.to_string(), hash.to_string()))
}

fn is_sha256_hash_name(hash_name: Option<&str>) -> bool {
    hash_name.is_some_and(|hash_name| hash_name.eq_ignore_ascii_case("sha256"))
}

async fn ensure_link_blob(
    state: &AppState,
    link: CachedLink,
    filename: &str,
) -> Result<BlobResponse, StatusCode> {
    let key = blob_key(&link.blob_kind, &link.blob_id);
    if let Some(blob) = state.hot_blobs.get(&key) {
        debug!(filename, blob_kind = %link.blob_kind, blob_id = %link.blob_id, "blob cache hit in memory");
        state.metrics.incr_blob_hit();
        return Ok(BlobResponse::Ready(blob));
    }

    if let BlobStatus::Ready(blob) = state
        .cache
        .blob_status(&link.blob_kind, &link.blob_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    {
        debug!(filename, blob_kind = %link.blob_kind, blob_id = %link.blob_id, "blob cache hit on disk");
        state.metrics.incr_blob_hit();
        state.hot_blobs.insert(key, blob.clone());
        return Ok(BlobResponse::Ready(blob));
    }

    let storage_relpath = state
        .cache
        .blob_storage_relpath(&link.blob_kind, &link.blob_id, filename)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    if let Some(active) = state.active_blobs.get(&key).map(|entry| entry.clone()) {
        debug!(filename, blob_kind = %link.blob_kind, blob_id = %link.blob_id, "joining active blob download");
        state.metrics.incr_blob_coalesced();
        return active_blob_response(active).await;
    }

    let guard = state.locks.blob_guard(&link.blob_kind, &link.blob_id).await;
    if let BlobStatus::Ready(blob) = state
        .cache
        .blob_status(&link.blob_kind, &link.blob_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    {
        debug!(filename, blob_kind = %link.blob_kind, blob_id = %link.blob_id, "blob cache filled while waiting for lock");
        state.metrics.incr_blob_hit();
        state.hot_blobs.insert(key, blob.clone());
        return Ok(BlobResponse::Ready(blob));
    }
    if let Some(active) = state.active_blobs.get(&key).map(|entry| entry.clone()) {
        debug!(filename, blob_kind = %link.blob_kind, blob_id = %link.blob_id, "joining active blob download after lock");
        state.metrics.incr_blob_coalesced();
        return active_blob_response(active).await;
    }
    state.metrics.incr_blob_miss();
    let _inserted = state
        .cache
        .mark_blob_pending(
            &link.blob_kind,
            &link.blob_id,
            filename,
            &link.upstream_url,
            &storage_relpath,
        )
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let initial_content_type = from_path(filename).first_or_octet_stream().to_string();
    let can_resume = is_sha256_hash_name(link.hash_name.as_deref());
    let resume_from = if can_resume {
        state
            .cache
            .blob_temp_len(&storage_relpath)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    } else {
        0
    };
    info!(
        filename,
        blob_kind = %link.blob_kind,
        blob_id = %link.blob_id,
        resume_from,
        "starting blob download"
    );
    let active = Arc::new(ActiveBlob {
        temp_path: state.cache.blob_temp_path(&storage_relpath),
        final_path: state.cache.blob_path(&storage_relpath),
        content_type: initial_content_type.clone(),
        content_length: AtomicU64::new(0),
        initial_bytes: resume_from,
        bytes_written: AtomicU64::new(resume_from),
        finished: AtomicBool::new(false),
        notify: Notify::new(),
        chunk_tx: broadcast::channel(BLOB_BROADCAST_CHANNEL_CAPACITY).0,
        failure: std::sync::Mutex::new(None),
    });
    state.active_blobs.insert(key.clone(), active.clone());
    drop(guard);

    let leader_rx = active.chunk_tx.subscribe();
    tokio::spawn(download_blob_task(DownloadJob {
        cache: state.cache.clone(),
        upstream: state.upstream.clone(),
        active_blobs: state.active_blobs.clone(),
        hot_blobs: state.hot_blobs.clone(),
        active_key: key.clone(),
        active: active.clone(),
        link,
        storage_relpath,
        initial_content_type,
        resume_from,
    }));

    active_blob_response_with_rx(active, leader_rx).await
}

async fn cached_file_response(
    cache: &CacheStore,
    blob: &BlobInfo,
    headers: &HeaderMap,
) -> Response<Body> {
    match File::open(cache.blob_path(&blob.storage_relpath)).await {
        Ok(mut file) => {
            let range = match header_value(headers, RANGE) {
                Some(value) => match parse_byte_range(value, blob.size_bytes) {
                    Ok(range) => range,
                    Err(_) => return range_not_satisfiable_response(blob.size_bytes),
                },
                None => None,
            };
            if let Some(range) = range {
                if file.seek(SeekFrom::Start(range.start)).await.is_err() {
                    return text_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "cache file missing\n",
                    );
                }
                let length = range.end - range.start + 1;
                let stream =
                    ReaderStream::with_capacity(file.take(length), FILE_STREAM_BUFFER_SIZE)
                        .map_err(io::Error::other);
                let mut response = Response::new(Body::from_stream(stream));
                *response.status_mut() = StatusCode::PARTIAL_CONTENT;
                response
                    .headers_mut()
                    .insert(CONTENT_TYPE, header(&blob.content_type));
                response.headers_mut().insert(
                    CONTENT_RANGE,
                    header(&format!(
                        "bytes {}-{}/{}",
                        range.start, range.end, blob.size_bytes
                    )),
                );
                response.headers_mut().insert(
                    axum::http::header::CONTENT_LENGTH,
                    header(&length.to_string()),
                );
                response
                    .headers_mut()
                    .insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
                return response;
            }

            let stream = ReaderStream::with_capacity(file, FILE_STREAM_BUFFER_SIZE)
                .map_err(io::Error::other);
            let mut response = Response::new(Body::from_stream(stream));
            *response.status_mut() = StatusCode::OK;
            response
                .headers_mut()
                .insert(CONTENT_TYPE, header(&blob.content_type));
            response.headers_mut().insert(
                axum::http::header::CONTENT_LENGTH,
                header(&blob.size_bytes.to_string()),
            );
            response
                .headers_mut()
                .insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
            response
        }
        Err(_) => text_response(StatusCode::INTERNAL_SERVER_ERROR, "cache file missing\n"),
    }
}

fn range_not_satisfiable_response(size: u64) -> Response<Body> {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::RANGE_NOT_SATISFIABLE;
    response
        .headers_mut()
        .insert(CONTENT_RANGE, header(&format!("bytes */{size}")));
    response
        .headers_mut()
        .insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    response
}

async fn active_blob_response(active: Arc<ActiveBlob>) -> Result<BlobResponse, StatusCode> {
    let rx = active.chunk_tx.subscribe();
    active_blob_response_with_rx(active, rx).await
}

async fn active_blob_response_with_rx(
    active: Arc<ActiveBlob>,
    rx: broadcast::Receiver<BlobChunk>,
) -> Result<BlobResponse, StatusCode> {
    let content_length = wait_for_active_response_start(&active).await?;
    let stream: BoxByteStream = Box::pin(tail_file_stream(active.clone(), rx));
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = StatusCode::OK;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, header(&active.content_type));
    if let Some(content_length) = content_length
        && let Ok(value) = HeaderValue::from_str(&content_length.to_string())
    {
        response
            .headers_mut()
            .insert(axum::http::header::CONTENT_LENGTH, value);
    }
    Ok(BlobResponse::Streaming(response))
}

async fn wait_for_active_response_start(active: &ActiveBlob) -> Result<Option<u64>, StatusCode> {
    loop {
        let notified = active.notify.notified();
        if let Some(message) = active_failure_message(active) {
            return Err(active_failure_status(&message));
        }
        let content_length = active.content_length.load(Ordering::SeqCst);
        if content_length > 0 {
            return Ok(Some(content_length));
        }
        let bytes_written = active.bytes_written.load(Ordering::SeqCst);
        if bytes_written > active.initial_bytes {
            return Ok(None);
        }
        if active.finished.load(Ordering::SeqCst) {
            if let Some(message) = active_failure_message(active) {
                return Err(active_failure_status(&message));
            }
            return Ok(Some(bytes_written));
        }
        notified.await;
    }
}

fn active_failure_status(message: &str) -> StatusCode {
    if message.contains("not found") {
        StatusCode::NOT_FOUND
    } else {
        StatusCode::BAD_GATEWAY
    }
}

type BoxByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + 'static>>;
const CLIENT_CHUNK_BYTES: usize = 256 * 1024;
const DISK_FLUSH_BYTES: usize = 4 * 1024 * 1024;
const BLOB_BROADCAST_CHANNEL_CAPACITY: usize = 1024;
const BLOB_WRITE_CHANNEL_CAPACITY: usize = 8;

async fn open_active_blob_file(active: &ActiveBlob) -> io::Result<File> {
    match File::open(&active.temp_path).await {
        Ok(file) => Ok(file),
        Err(err) if err.kind() == io::ErrorKind::NotFound => File::open(&active.final_path).await,
        Err(err) => Err(err),
    }
}

struct DownloadJob {
    cache: CacheStore,
    upstream: UpstreamClient,
    active_blobs: ActiveBlobRegistry,
    hot_blobs: HotBlobCache,
    active_key: String,
    active: Arc<ActiveBlob>,
    link: CachedLink,
    storage_relpath: String,
    initial_content_type: String,
    resume_from: u64,
}

fn tail_file_stream(
    active: Arc<ActiveBlob>,
    rx: broadcast::Receiver<BlobChunk>,
) -> impl Stream<Item = Result<Bytes, io::Error>> + Send + 'static {
    futures_util::stream::unfold(
        (active, rx, None::<File>, 0_u64),
        |(active, mut rx, mut file, mut offset)| async move {
            loop {
                if let Some(message) = active_failure_message(&active) {
                    return Some((Err(io::Error::other(message)), (active, rx, file, offset)));
                }

                match rx.try_recv() {
                    Ok(chunk) => match chunk_for_offset(chunk, offset) {
                        ChunkRead::Ready(bytes) => {
                            let (bytes, next_offset) =
                                coalesce_broadcast_chunks(&mut rx, bytes, offset);
                            offset = next_offset;
                            return Some((Ok(bytes), (active, rx, file, offset)));
                        }
                        ChunkRead::AlreadyRead => continue,
                        ChunkRead::Gap => {}
                    },
                    Err(broadcast::error::TryRecvError::Lagged(_)) => {}
                    Err(broadcast::error::TryRecvError::Empty) => {}
                    Err(broadcast::error::TryRecvError::Closed) => {
                        if active.finished.load(Ordering::SeqCst) {
                            if let Some(message) = active_failure_message(&active) {
                                return Some((
                                    Err(io::Error::other(message)),
                                    (active, rx, file, offset),
                                ));
                            }
                            return None;
                        }
                    }
                }

                if file.is_none() {
                    match open_active_blob_file(&active).await {
                        Ok(opened) => file = Some(opened),
                        Err(err)
                            if err.kind() == io::ErrorKind::NotFound
                                && !active.finished.load(Ordering::SeqCst) =>
                        {
                            wait_for_active_progress(&active, offset).await;
                            continue;
                        }
                        Err(err) => {
                            if let Some(message) = active_failure_message(&active) {
                                return Some((
                                    Err(io::Error::other(message)),
                                    (active, rx, file, offset),
                                ));
                            }
                            return Some((Err(err), (active, rx, file, offset)));
                        }
                    }
                }

                let available = active.bytes_written.load(Ordering::SeqCst);
                if offset < available {
                    let mut buffer = vec![0_u8; (available - offset).min(1024 * 1024) as usize];
                    let opened = file.as_mut().expect("tail file is open");
                    match opened.read(&mut buffer).await {
                        Ok(0) if !active.finished.load(Ordering::SeqCst) => {
                            wait_for_active_progress(&active, offset).await;
                            continue;
                        }
                        Ok(0) => {
                            if let Some(message) = active_failure_message(&active) {
                                return Some((
                                    Err(io::Error::other(message)),
                                    (active, rx, file, offset),
                                ));
                            }
                            return None;
                        }
                        Ok(read) => {
                            buffer.truncate(read);
                            offset += read as u64;
                            return Some((Ok(Bytes::from(buffer)), (active, rx, file, offset)));
                        }
                        Err(err) => return Some((Err(err), (active, rx, file, offset))),
                    }
                }

                let content_length = active.content_length.load(Ordering::SeqCst);
                if content_length > 0 && offset >= content_length {
                    if active.finished.load(Ordering::SeqCst) {
                        if let Some(message) = active_failure_message(&active) {
                            return Some((
                                Err(io::Error::other(message)),
                                (active, rx, file, offset),
                            ));
                        }
                        return None;
                    }
                    active.notify.notified().await;
                    continue;
                }
                if active.finished.load(Ordering::SeqCst) {
                    if let Some(message) = active_failure_message(&active) {
                        return Some((Err(io::Error::other(message)), (active, rx, file, offset)));
                    }
                    return None;
                }
                tokio::select! {
                    received = rx.recv() => {
                        match received {
                            Ok(chunk) => match chunk_for_offset(chunk, offset) {
                                ChunkRead::Ready(bytes) => {
                                    let (bytes, next_offset) =
                                        coalesce_broadcast_chunks(&mut rx, bytes, offset);
                                    offset = next_offset;
                                    return Some((Ok(bytes), (active, rx, file, offset)));
                                }
                                ChunkRead::AlreadyRead | ChunkRead::Gap => continue,
                            },
                            Err(broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(broadcast::error::RecvError::Closed) => {
                                if active.finished.load(Ordering::SeqCst) {
                                    if let Some(message) = active_failure_message(&active) {
                                        return Some((
                                            Err(io::Error::other(message)),
                                            (active, rx, file, offset),
                                        ));
                                    }
                                    return None;
                                }
                            }
                        }
                    }
                    _ = active.notify.notified() => {
                        if active.finished.load(Ordering::SeqCst) {
                            if let Some(message) = active_failure_message(&active) {
                                return Some((
                                    Err(io::Error::other(message)),
                                    (active, rx, file, offset),
                                ));
                            }
                            return None;
                        }
                    }
                }
            }
        },
    )
}

fn active_failure_message(active: &ActiveBlob) -> Option<String> {
    active.failure.lock().ok().and_then(|value| value.clone())
}

enum ChunkRead {
    Ready(Bytes),
    AlreadyRead,
    Gap,
}

fn chunk_for_offset(chunk: BlobChunk, offset: u64) -> ChunkRead {
    let chunk_end = chunk.offset + chunk.bytes.len() as u64;
    if chunk_end <= offset {
        return ChunkRead::AlreadyRead;
    }
    if chunk.offset > offset {
        return ChunkRead::Gap;
    }
    let start = (offset - chunk.offset) as usize;
    ChunkRead::Ready(chunk.bytes.slice(start..))
}

fn coalesce_broadcast_chunks(
    rx: &mut broadcast::Receiver<BlobChunk>,
    first: Bytes,
    offset: u64,
) -> (Bytes, u64) {
    let mut next_offset = offset + first.len() as u64;
    if first.len() >= CLIENT_CHUNK_BYTES {
        return (first, next_offset);
    }

    let mut out = BytesMut::with_capacity(CLIENT_CHUNK_BYTES);
    out.extend_from_slice(&first);
    while out.len() < CLIENT_CHUNK_BYTES {
        let chunk = match rx.try_recv() {
            Ok(chunk) => chunk,
            Err(broadcast::error::TryRecvError::Lagged(_)) => break,
            Err(broadcast::error::TryRecvError::Empty) => break,
            Err(broadcast::error::TryRecvError::Closed) => break,
        };
        match chunk_for_offset(chunk, next_offset) {
            ChunkRead::Ready(bytes) => {
                next_offset += bytes.len() as u64;
                out.extend_from_slice(&bytes);
            }
            ChunkRead::AlreadyRead => continue,
            ChunkRead::Gap => break,
        }
    }
    (out.freeze(), next_offset)
}

async fn wait_for_active_progress(active: &ActiveBlob, offset: u64) {
    let notified = active.notify.notified();
    if active.bytes_written.load(Ordering::SeqCst) <= offset
        && !active.finished.load(Ordering::SeqCst)
        && active
            .failure
            .lock()
            .ok()
            .and_then(|value| value.clone())
            .is_none()
    {
        notified.await;
    }
}

async fn download_blob_task(job: DownloadJob) {
    if let Err(err) = download_blob_task_inner(&job).await {
        let message = err.to_string();
        error!(
            filename = %job.link.filename,
            blob_kind = %job.link.blob_kind,
            blob_id = %job.link.blob_id,
            error = %message,
            "blob download failed"
        );
        if let Ok(mut failure) = job.active.failure.lock() {
            *failure = Some(message);
        }
        job.active.finished.store(true, Ordering::SeqCst);
        job.active.notify.notify_waiters();
        job.active_blobs.remove(&job.active_key);
    }
}

async fn download_blob_task_inner(job: &DownloadJob) -> io::Result<()> {
    job.cache.prepare_blob_parent(&job.storage_relpath).await?;
    let mut resume_from = job.resume_from;
    let temp_path = job.cache.blob_temp_path(&job.storage_relpath);
    let verify_sha256 = is_sha256_hash_name(job.link.hash_name.as_deref());
    let mut hasher = verify_sha256.then(Sha256::new);
    if resume_from > 0
        && let Some(hasher) = &mut hasher
    {
        debug!(
            filename = %job.link.filename,
            resume_from,
            "hashing existing blob prefix before resume"
        );
        hash_existing_prefix(&temp_path, hasher).await?;
    }

    let mut upstream_file = job
        .upstream
        .open_file_range(
            &job.link.upstream_url,
            if resume_from > 0 {
                Some(resume_from)
            } else {
                None
            },
        )
        .await?;

    if resume_from > 0 && upstream_file.range.map(|range| range.start) != Some(resume_from) {
        warn!(
            filename = %job.link.filename,
            resume_from,
            "upstream did not honor range request, restarting blob download"
        );
        let _ = tokio::fs::remove_file(&temp_path).await;
        resume_from = 0;
        hasher = verify_sha256.then(Sha256::new);
        job.active.bytes_written.store(0, Ordering::SeqCst);
        job.active.notify.notify_waiters();
        upstream_file = job.upstream.open_file(&job.link.upstream_url).await?;
    }

    let content_type = upstream_file
        .content_type
        .clone()
        .unwrap_or_else(|| job.initial_content_type.clone());
    let content_length = if resume_from > 0 {
        upstream_file
            .range
            .and_then(|range| range.total)
            .or_else(|| {
                upstream_file
                    .content_length
                    .map(|length| resume_from + length)
            })
    } else {
        upstream_file.content_length
    };
    if let Some(content_length) = content_length {
        debug!(
            filename = %job.link.filename,
            content_length,
            "upstream blob content length"
        );
        job.active
            .content_length
            .store(content_length, Ordering::SeqCst);
        job.active.notify.notify_waiters();
    }

    let (write_tx, write_rx) = mpsc::channel(BLOB_WRITE_CHANNEL_CAPACITY);
    let writer_active = job.active.clone();
    let writer_temp_path = temp_path.clone();
    let writer = tokio::spawn(async move {
        write_blob_chunks_task(writer_temp_path, resume_from, writer_active, write_rx).await
    });

    let mut bytes_received = resume_from;
    let mut stream = upstream_file.into_stream().map_err(io::Error::other);
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if let Some(hasher) = &mut hasher {
            hasher.update(&chunk);
        }
        let chunk_offset = bytes_received;
        bytes_received += chunk.len() as u64;
        let _ = job.active.chunk_tx.send(BlobChunk {
            offset: chunk_offset,
            bytes: chunk.clone(),
        });
        job.active.notify.notify_waiters();

        if write_tx.send(chunk).await.is_err() {
            return Err(io::Error::other("blob writer task stopped"));
        }
    }
    drop(write_tx);

    if let (Some(hasher), Some(expected)) = (hasher, &job.link.hash_value) {
        let actual_hash = format!("{:x}", hasher.finalize());
        if actual_hash != *expected {
            let _ = writer.await.map_err(join_error)?;
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "downloaded file sha256 did not match link hash",
            ));
        }
    } else if verify_sha256 && job.link.hash_value.is_none() {
        let _ = writer.await.map_err(join_error)?;
        let _ = tokio::fs::remove_file(&temp_path).await;
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "sha256 link did not include an expected hash",
        ));
    }

    let bytes_written = writer.await.map_err(join_error)??;
    if bytes_written != bytes_received {
        let _ = tokio::fs::remove_file(&temp_path).await;
        return Err(io::Error::new(
            io::ErrorKind::WriteZero,
            "cached file size did not match upstream bytes received",
        ));
    }

    job.cache
        .commit_blob_file(&temp_path, &job.storage_relpath)
        .await?;
    let blob = job
        .cache
        .mark_blob_ready(&BlobWrite {
            blob_kind: job.link.blob_kind.clone(),
            blob_id: job.link.blob_id.clone(),
            storage_relpath: job.storage_relpath.clone(),
            content_type,
            fetched_at: current_unix_secs(),
            size_bytes: bytes_received,
            filename: job.link.filename.clone(),
            upstream_url: job.link.upstream_url.clone(),
        })
        .await?;
    job.hot_blobs
        .insert(blob_key(&blob.blob_kind, &blob.blob_id), blob);
    info!(
        filename = %job.link.filename,
        blob_kind = %job.link.blob_kind,
        blob_id = %job.link.blob_id,
        size_bytes = bytes_received,
        "blob download completed"
    );
    job.active_blobs.remove(&job.active_key);
    job.active.finished.store(true, Ordering::SeqCst);
    job.active.notify.notify_waiters();
    Ok(())
}

async fn write_blob_chunks_task(
    temp_path: std::path::PathBuf,
    resume_from: u64,
    active: Arc<ActiveBlob>,
    mut rx: mpsc::Receiver<Bytes>,
) -> io::Result<u64> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(resume_from > 0)
        .write(true)
        .truncate(resume_from == 0)
        .open(&temp_path)
        .await?;
    let mut bytes_written = resume_from;
    let mut pending = Vec::with_capacity(DISK_FLUSH_BYTES);

    while let Some(chunk) = rx.recv().await {
        pending.extend_from_slice(&chunk);
        if bytes_written > resume_from && pending.len() < DISK_FLUSH_BYTES {
            continue;
        }
        flush_blob_pending(&mut file, &mut pending, &mut bytes_written, &active).await?;
    }

    if !pending.is_empty() {
        flush_blob_pending(&mut file, &mut pending, &mut bytes_written, &active).await?;
    }
    file.flush().await?;
    file.sync_all().await?;
    Ok(bytes_written)
}

async fn flush_blob_pending(
    file: &mut File,
    pending: &mut Vec<u8>,
    bytes_written: &mut u64,
    active: &ActiveBlob,
) -> io::Result<()> {
    file.write_all(pending).await?;
    *bytes_written += pending.len() as u64;
    active.bytes_written.store(*bytes_written, Ordering::SeqCst);
    active.notify.notify_waiters();
    pending.clear();
    Ok(())
}

async fn hash_existing_prefix(path: &std::path::Path, hasher: &mut Sha256) -> io::Result<()> {
    let mut file = File::open(path).await?;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            return Ok(());
        }
        hasher.update(&buffer[..read]);
    }
}

fn render_simple_response(
    headers: &HeaderMap,
    content_type: &str,
    body: String,
    stale: bool,
) -> Response<Body> {
    let etag = response_etag(body.as_bytes());
    render_cached_simple_response(headers, content_type, Bytes::from(body), etag, stale)
}

fn render_cached_simple_response(
    headers: &HeaderMap,
    content_type: &str,
    body: Bytes,
    etag: String,
    stale: bool,
) -> Response<Body> {
    if let Some(if_none_match) = header_value(headers, IF_NONE_MATCH)
        && if_none_match.trim() == etag
    {
        let mut response = Response::new(Body::empty());
        *response.status_mut() = StatusCode::NOT_MODIFIED;
        response.headers_mut().insert(ETAG, header(&etag));
        if stale {
            response
                .headers_mut()
                .insert(WARNING, HeaderValue::from_static("110 - response is stale"));
        }
        return response;
    }
    let content_length = body.len();
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = StatusCode::OK;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, header(content_type));
    response.headers_mut().insert(
        axum::http::header::CONTENT_LENGTH,
        header(&content_length.to_string()),
    );
    response.headers_mut().insert(ETAG, header(&etag));
    if stale {
        response
            .headers_mut()
            .insert(WARNING, HeaderValue::from_static("110 - response is stale"));
    }
    response
}

fn text_response(status: StatusCode, body: &str) -> Response<Body> {
    let mut response = Response::new(Body::from(body.to_string()));
    *response.status_mut() = status;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

fn response_etag(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut value = String::from("\"");
    for byte in digest {
        value.push(hex_digit(byte >> 4));
        value.push(hex_digit(byte & 0x0f));
    }
    value.push('"');
    value
}

fn hex_digit(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'a' + (value - 10)) as char,
        _ => '0',
    }
}

fn header(value: &str) -> HeaderValue {
    HeaderValue::from_str(value).unwrap_or_else(|_| HeaderValue::from_static(""))
}

fn header_value(headers: &HeaderMap, name: axum::http::header::HeaderName) -> Option<&str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

fn join_error(err: tokio::task::JoinError) -> io::Error {
    io::Error::other(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hot_cache::BoundedLruCache;
    use axum::body::to_bytes;
    use axum::extract::State;
    use axum::http::{Request, header::RANGE};
    use axum::routing::get;
    use futures_util::stream;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tempfile::tempdir;
    use tokio::sync::mpsc;
    use tower::ServiceExt;

    fn test_link(index: usize) -> CachedLink {
        let blob_id = format!("{index:064x}");
        CachedLink {
            filename: format!("demo-{index}.whl"),
            upstream_url: format!("https://files.example/demo-{index}.whl"),
            blob_kind: "sha256".to_string(),
            blob_id: blob_id.clone(),
            cached_size_bytes: None,
            requires_python: None,
            yanked: None,
            gpg_sig: None,
            dist_info_metadata: None,
            core_metadata: None,
            hash_name: Some("sha256".to_string()),
            hash_value: Some(blob_id),
        }
    }

    fn test_project_record(project: &str) -> ProjectRecord {
        let now = current_unix_secs();
        ProjectRecord {
            project: project.to_string(),
            fetched_at: now,
            expires_at: now + 3600,
            upstream_etag: Some(format!("\"etag-{project}\"")),
            upstream_serial: Some(1),
            upstream_project_url: format!("https://example.invalid/simple/{project}/"),
            raw_body: "<html></html>".to_string(),
            links: vec![test_link(0)],
        }
    }

    fn test_hot_project(project: &str) -> Arc<HotProject> {
        let record = test_project_record(project);
        build_hot_project(
            project,
            PYPI_FILE_BASE_PATH,
            ProjectCache {
                project: record.project,
                fetched_at: record.fetched_at,
                expires_at: record.expires_at,
                upstream_etag: record.upstream_etag,
                upstream_serial: record.upstream_serial,
                upstream_project_url: record.upstream_project_url,
                raw_body: record.raw_body,
                links: record.links,
            },
        )
    }

    fn test_blob(index: usize) -> BlobInfo {
        BlobInfo {
            blob_kind: "sha256".to_string(),
            blob_id: format!("{index:064x}"),
            storage_relpath: format!("sha256/demo-{index}.whl"),
            content_type: "application/octet-stream".to_string(),
            fetched_at: current_unix_secs(),
            size_bytes: index as u64,
            filename: format!("demo-{index}.whl"),
            upstream_url: format!("https://files.example/demo-{index}.whl"),
            state: "ready".to_string(),
        }
    }

    #[test]
    fn sha256_hash_name_matching_is_case_insensitive() {
        assert!(is_sha256_hash_name(Some("sha256")));
        assert!(is_sha256_hash_name(Some("SHA256")));
        assert!(is_sha256_hash_name(Some("Sha256")));
        assert!(!is_sha256_hash_name(Some("sha512")));
        assert!(!is_sha256_hash_name(None));
    }

    fn test_state(cache: CacheStore, upstream_base_url: &str) -> AppState {
        AppState {
            cache,
            upstream: UpstreamClient::new(upstream_base_url, 5).unwrap(),
            pytorch_wheels_upstream: UpstreamClient::new_project_root(
                "https://download.pytorch.org/whl/",
                5,
            )
            .unwrap(),
            request_timeout_secs: 5,
            project_cache_ttl_secs: 3600,
            locks: RequestLocks::default(),
            metrics: CacheMetrics::default(),
            active_blobs: ActiveBlobRegistry::default(),
            hot_projects: ConcurrentCache::new(HOT_PROJECT_CACHE_MAX_ENTRIES as u64),
            hot_links: ConcurrentCache::new(HOT_LINK_CACHE_MAX_ENTRIES as u64),
            hot_blobs: ConcurrentCache::new(HOT_BLOB_CACHE_MAX_ENTRIES as u64),
        }
    }

    #[tokio::test]
    async fn request_locks_prune_stale_project_keys() {
        let locks = RequestLocks::default();
        for index in 0..REQUEST_LOCK_CLEANUP_THRESHOLD {
            let guard = locks.project_guard(&format!("stale-{index}")).await;
            drop(guard);
        }

        let fresh_guard = locks.project_guard("fresh").await;
        assert_eq!(locks.projects.len(), 1);
        assert!(locks.projects.contains_key("fresh"));
        drop(fresh_guard);
    }

    #[tokio::test]
    async fn request_locks_keep_active_project_keys_when_pruning() {
        let locks = RequestLocks::default();
        let active_guard = locks.project_guard("active").await;
        for index in 0..(REQUEST_LOCK_CLEANUP_THRESHOLD - 1) {
            let guard = locks.project_guard(&format!("stale-{index}")).await;
            drop(guard);
        }

        let fresh_guard = locks.project_guard("fresh").await;
        assert_eq!(locks.projects.len(), 2);
        assert!(locks.projects.contains_key("active"));
        assert!(locks.projects.contains_key("fresh"));
        drop(fresh_guard);
        drop(active_guard);
    }

    #[test]
    fn bounded_hot_cache_uses_lru_eviction() {
        let attempted_projects = HOT_PROJECT_CACHE_MAX_ENTRIES + 25;
        let mut projects = BoundedLruCache::new(HOT_PROJECT_CACHE_MAX_ENTRIES);
        for index in 0..attempted_projects {
            projects.insert(
                format!("project-{index}"),
                test_hot_project(&format!("project-{index}")),
            );
        }
        assert_eq!(projects.len(), HOT_PROJECT_CACHE_MAX_ENTRIES);
        assert!(projects.len() < attempted_projects);

        let attempted_links = HOT_LINK_CACHE_MAX_ENTRIES + 25;
        let mut links = BoundedLruCache::new(HOT_LINK_CACHE_MAX_ENTRIES);
        for index in 0..attempted_links {
            links.insert(format!("route/{index}/demo-{index}.whl"), test_link(index));
        }
        assert_eq!(links.len(), HOT_LINK_CACHE_MAX_ENTRIES);
        assert!(links.len() < attempted_links);

        let attempted_blobs = HOT_BLOB_CACHE_MAX_ENTRIES + 25;
        let mut blobs = BoundedLruCache::new(HOT_BLOB_CACHE_MAX_ENTRIES);
        for index in 0..attempted_blobs {
            blobs.insert(format!("sha256:{index}"), test_blob(index));
        }
        assert_eq!(blobs.len(), HOT_BLOB_CACHE_MAX_ENTRIES);
        assert!(blobs.len() < attempted_blobs);

        assert!(!projects.contains_key(&"project-0".to_string()));
        assert!(projects.contains_key(&format!("project-{}", attempted_projects - 1)));
    }

    #[test]
    fn pytorch_wheels_endpoint_uses_namespaced_cache_and_local_file_prefix() {
        let channel = "cu126";
        let project = "torch";
        let cache_project = pytorch_wheels_cache_project(Some(channel), project);
        assert_eq!(cache_project, "pytorch-wheels-cu126-torch");

        let file_base_path = pytorch_wheels_channel_file_base_path(channel);
        let hot_project = build_hot_project(
            project,
            &file_base_path,
            ProjectCache {
                project: cache_project,
                fetched_at: current_unix_secs(),
                expires_at: current_unix_secs() + 3600,
                upstream_etag: None,
                upstream_serial: None,
                upstream_project_url: "https://download.pytorch.org/whl/cu126/torch/".to_string(),
                raw_body: "<html></html>".to_string(),
                links: vec![test_link(0)],
            },
        );

        assert!(
            std::str::from_utf8(&hot_project.rendered.html_body)
                .unwrap()
                .contains("/pytorch-wheels/cu126/+f/000/0000000000000/demo-0.whl#sha256=")
        );
        assert!(
            std::str::from_utf8(&hot_project.rendered.json_body)
                .unwrap()
                .contains("\"url\":\"/pytorch-wheels/cu126/+f/000/0000000000000/demo-0.whl\"")
        );
    }

    #[test]
    fn pytorch_wheels_upstream_places_channel_projects_under_whl_channel() {
        let upstream =
            UpstreamClient::new_project_root("https://download.pytorch.org/whl/", 5).unwrap();
        let upstream = upstream.child_project_root("cu126", 5).unwrap();
        assert_eq!(
            upstream.project_url("Torch").unwrap().as_str(),
            "https://download.pytorch.org/whl/cu126/torch/"
        );
    }

    #[test]
    fn pytorch_wheels_upstream_base_is_configurable() {
        let upstream =
            UpstreamClient::new_project_root("https://mirror.example/pytorch/whl/", 5).unwrap();
        assert_eq!(
            upstream.project_url("Torch").unwrap().as_str(),
            "https://mirror.example/pytorch/whl/torch/"
        );
        let upstream = upstream.child_project_root("cu126", 5).unwrap();
        assert_eq!(
            upstream.project_url("Torch").unwrap().as_str(),
            "https://mirror.example/pytorch/whl/cu126/torch/"
        );
    }

    #[tokio::test]
    async fn download_pipeline_write_channel_is_bounded() {
        let (tx, mut rx) = mpsc::channel::<Bytes>(BLOB_WRITE_CHANNEL_CAPACITY);
        assert_eq!(tx.max_capacity(), BLOB_WRITE_CHANNEL_CAPACITY);

        for _ in 0..BLOB_WRITE_CHANNEL_CAPACITY {
            tx.send(Bytes::from_static(b"x")).await.unwrap();
        }
        assert_eq!(tx.capacity(), 0);

        let blocked = tokio::time::timeout(
            Duration::from_millis(25),
            tx.send(Bytes::from_static(b"blocked")),
        )
        .await;
        assert!(blocked.is_err());

        assert!(rx.recv().await.is_some());
        tokio::time::timeout(
            Duration::from_millis(25),
            tx.send(Bytes::from_static(b"unblocked")),
        )
        .await
        .unwrap()
        .unwrap();
    }

    fn test_active_blob(temp: &tempfile::TempDir) -> Arc<ActiveBlob> {
        Arc::new(ActiveBlob {
            temp_path: temp.path().join("blob.tmp"),
            final_path: temp.path().join("blob"),
            content_type: "application/octet-stream".to_string(),
            content_length: AtomicU64::new(0),
            initial_bytes: 0,
            bytes_written: AtomicU64::new(0),
            finished: AtomicBool::new(false),
            notify: Notify::new(),
            chunk_tx: broadcast::channel(BLOB_BROADCAST_CHANNEL_CAPACITY).0,
            failure: std::sync::Mutex::new(None),
        })
    }

    fn fail_active_blob(active: &ActiveBlob, message: &str) {
        *active.failure.lock().unwrap() = Some(message.to_string());
        active.finished.store(true, Ordering::SeqCst);
        active.notify.notify_waiters();
    }

    #[tokio::test]
    async fn active_blob_stream_reports_failure_while_waiting_for_file_progress() {
        let temp = tempdir().unwrap();
        let active = test_active_blob(&temp);
        let rx = active.chunk_tx.subscribe();
        let mut stream = Box::pin(tail_file_stream(active.clone(), rx));
        let next_chunk = tokio::spawn(async move { stream.next().await });

        tokio::time::sleep(Duration::from_millis(10)).await;
        fail_active_blob(&active, "download failed");

        let item = tokio::time::timeout(Duration::from_secs(1), next_chunk)
            .await
            .expect("stream did not wake after failure")
            .unwrap()
            .expect("stream ended without reporting failure");
        let err = item.expect_err("stream item should be an error");
        assert_eq!(err.to_string(), "download failed");
    }

    #[tokio::test]
    async fn active_blob_stream_reports_failure_while_waiting_for_chunk_or_notify() {
        let temp = tempdir().unwrap();
        let active = test_active_blob(&temp);
        tokio::fs::write(&active.temp_path, b"").await.unwrap();
        let rx = active.chunk_tx.subscribe();
        let mut stream = Box::pin(tail_file_stream(active.clone(), rx));
        let next_chunk = tokio::spawn(async move { stream.next().await });

        tokio::time::sleep(Duration::from_millis(10)).await;
        fail_active_blob(&active, "download failed");

        let item = tokio::time::timeout(Duration::from_secs(1), next_chunk)
            .await
            .expect("stream did not wake after failure")
            .unwrap()
            .expect("stream ended without reporting failure");
        let err = item.expect_err("stream item should be an error");
        assert_eq!(err.to_string(), "download failed");
    }

    #[tokio::test]
    async fn active_blob_response_returns_error_before_streaming_starts() {
        let temp = tempdir().unwrap();
        let active = test_active_blob(&temp);
        let response = tokio::spawn({
            let active = active.clone();
            async move { active_blob_response(active).await }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        fail_active_blob(&active, "upstream file request timed out");

        let result = tokio::time::timeout(Duration::from_secs(1), response)
            .await
            .expect("response did not wake after failure")
            .unwrap();
        let Err(status) = result else {
            panic!("failure before first byte should return a status");
        };
        assert_eq!(status, StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn evicted_hot_project_falls_back_to_sqlite() {
        let temp = tempdir().unwrap();
        let state = test_state(
            CacheStore::new(temp.path().to_path_buf()),
            "https://example.invalid/simple/",
        );
        state.cache.initialize().await.unwrap();
        let record = test_project_record("demo");
        state.cache.store_project(&record).await.unwrap();
        cache_hot_project(&state, "demo", test_hot_project("demo")).await;
        assert!(state.hot_projects.get("demo").is_some());

        state.hot_projects.invalidate("demo");
        let project_state = ensure_project(&state, "demo").await.unwrap();

        let ProjectState::Ready(hot_project, stale) = project_state else {
            panic!("expected project to load from sqlite");
        };
        assert!(!stale);
        assert_eq!(hot_project.cache.project, "demo");
        assert!(state.hot_projects.get("demo").is_some());
    }

    #[tokio::test]
    async fn caches_project_and_rewrites_file_links() {
        let upstream = spawn_upstream().await;
        let temp = tempdir().unwrap();
        let state = test_state(
            CacheStore::new(temp.path().to_path_buf()),
            &upstream.base_url,
        );
        state.cache.initialize().await.unwrap();
        let app = app(state);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/simple/demo/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        assert!(html.contains("/root/pypi/+f/9ce/b18f15662bb87/demo-1.0.whl"));

        let root = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/simple/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(root.into_body(), usize::MAX).await.unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        assert!(html.contains("demo"));
    }

    #[tokio::test]
    async fn search_query_redirects_to_normalized_project_page() {
        let temp = tempdir().unwrap();
        let state = test_state(
            CacheStore::new(temp.path().to_path_buf()),
            "https://example.invalid/simple/",
        );
        state.cache.initialize().await.unwrap();
        let app = app(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/simple/?q=My_Pkg.demo")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            response.headers().get(LOCATION).unwrap(),
            "/simple/my-pkg-demo/"
        );
    }

    #[tokio::test]
    async fn returns_json_simple_api_when_requested() {
        let upstream = spawn_upstream().await;
        let temp = tempdir().unwrap();
        let state = test_state(
            CacheStore::new(temp.path().to_path_buf()),
            &upstream.base_url,
        );
        state.cache.initialize().await.unwrap();
        let app = app(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/simple/demo/")
                    .header(ACCEPT, json_media_type())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["name"], "demo");
        assert_eq!(json["files"][0]["filename"], "demo-1.0.whl");
        assert_eq!(
            json["files"][0]["url"],
            "/root/pypi/+f/9ce/b18f15662bb87/demo-1.0.whl"
        );
    }

    #[tokio::test]
    async fn caches_file_downloads() {
        let upstream = spawn_upstream().await;
        let temp = tempdir().unwrap();
        let state = test_state(
            CacheStore::new(temp.path().to_path_buf()),
            &upstream.base_url,
        );
        state.cache.initialize().await.unwrap();
        let app = app(state);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/simple/demo/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        let path = html
            .split('"')
            .find(|part| part.starts_with("/root/pypi/+f/"))
            .unwrap()
            .split('#')
            .next()
            .unwrap()
            .to_string();

        let response = app
            .clone()
            .oneshot(Request::builder().uri(&path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body.as_ref(), b"wheel-bytes");
    }

    #[tokio::test]
    async fn serves_dist_info_metadata_files() {
        let upstream = spawn_upstream().await;
        let temp = tempdir().unwrap();
        let state = test_state(
            CacheStore::new(temp.path().to_path_buf()),
            &upstream.base_url,
        );
        state.cache.initialize().await.unwrap();
        let app = app(state);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/simple/demo/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        let path = html
            .split('"')
            .find(|part| part.starts_with("/root/pypi/+f/"))
            .unwrap()
            .split('#')
            .next()
            .unwrap()
            .to_string();
        let metadata_path = format!("{path}.metadata");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(&metadata_path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body.as_ref(), b"metadata-bytes");
        assert_eq!(upstream.file_requests(), 0);
        assert_eq!(upstream.metadata_requests(), 1);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(&metadata_path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body.as_ref(), b"metadata-bytes");
        assert_eq!(upstream.metadata_requests(), 1);
    }

    #[tokio::test]
    async fn serves_byte_ranges_for_cached_file_downloads() {
        let upstream = spawn_upstream().await;
        let temp = tempdir().unwrap();
        let state = test_state(
            CacheStore::new(temp.path().to_path_buf()),
            &upstream.base_url,
        );
        state.cache.initialize().await.unwrap();
        let app = app(state);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/simple/demo/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        let path = html
            .split('"')
            .find(|part| part.starts_with("/root/pypi/+f/"))
            .unwrap()
            .split('#')
            .next()
            .unwrap()
            .to_string();

        let response = app
            .clone()
            .oneshot(Request::builder().uri(&path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body.as_ref(), b"wheel-bytes");
        assert_eq!(upstream.file_requests(), 1);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(&path)
                    .header(RANGE, "bytes=0-4")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response.headers().get(CONTENT_RANGE).unwrap(),
            "bytes 0-4/11"
        );
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_LENGTH)
                .unwrap(),
            "5"
        );
        assert_eq!(response.headers().get(ACCEPT_RANGES).unwrap(), "bytes");
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "application/octet-stream"
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body.as_ref(), b"wheel");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(&path)
                    .header(RANGE, "bytes=6-")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response.headers().get(CONTENT_RANGE).unwrap(),
            "bytes 6-10/11"
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body.as_ref(), b"bytes");

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(&path)
                    .header(RANGE, "bytes=-5")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            response.headers().get(CONTENT_RANGE).unwrap(),
            "bytes 6-10/11"
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body.as_ref(), b"bytes");
        assert_eq!(upstream.file_requests(), 1);
    }

    #[tokio::test]
    async fn rejects_unsatisfiable_cached_file_ranges() {
        let upstream = spawn_upstream().await;
        let temp = tempdir().unwrap();
        let state = test_state(
            CacheStore::new(temp.path().to_path_buf()),
            &upstream.base_url,
        );
        state.cache.initialize().await.unwrap();
        let app = app(state);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/simple/demo/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        let path = html
            .split('"')
            .find(|part| part.starts_with("/root/pypi/+f/"))
            .unwrap()
            .split('#')
            .next()
            .unwrap()
            .to_string();

        let response = app
            .clone()
            .oneshot(Request::builder().uri(&path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let _ = to_bytes(response.into_body(), usize::MAX).await.unwrap();

        for range in ["bytes=11-", "bytes=5-3", "bytes=-0", "bytes=0-1,2-3"] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(&path)
                        .header(RANGE, range)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
            assert_eq!(response.headers().get(CONTENT_RANGE).unwrap(), "bytes */11");
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            assert!(body.is_empty());
        }
        assert_eq!(upstream.file_requests(), 1);
    }

    #[tokio::test]
    async fn concurrent_file_downloads_share_single_upstream_fetch() {
        let upstream = spawn_slow_upstream().await;
        let temp = tempdir().unwrap();
        let state = test_state(
            CacheStore::new(temp.path().to_path_buf()),
            &upstream.base_url,
        );
        state.cache.initialize().await.unwrap();
        let app = app(state);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/simple/demo/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        let path = html
            .split('"')
            .find(|part| part.starts_with("/root/pypi/+f/"))
            .unwrap()
            .split('#')
            .next()
            .unwrap()
            .to_string();

        let mut handles = Vec::new();
        for _ in 0..8 {
            let app = app.clone();
            let path = path.clone();
            handles.push(tokio::spawn(async move {
                let response = app
                    .oneshot(Request::builder().uri(&path).body(Body::empty()).unwrap())
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::OK);
                to_bytes(response.into_body(), usize::MAX).await.unwrap()
            }));
        }

        for handle in handles {
            let body = handle.await.unwrap();
            assert_eq!(body.as_ref(), b"wheel-bytes");
        }
        assert_eq!(upstream.file_requests(), 1);
    }

    #[tokio::test]
    async fn concurrent_file_downloads_fan_out_before_blob_is_ready() {
        let upstream = spawn_slow_upstream().await;
        let temp = tempdir().unwrap();
        let state = test_state(
            CacheStore::new(temp.path().to_path_buf()),
            &upstream.base_url,
        );
        state.cache.initialize().await.unwrap();
        let app = app(state);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/simple/demo/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        let path = html
            .split('"')
            .find(|part| part.starts_with("/root/pypi/+f/"))
            .unwrap()
            .split('#')
            .next()
            .unwrap()
            .to_string();

        let (first_chunk_tx, mut first_chunk_rx) = mpsc::channel(8);
        let mut handles = Vec::new();
        for _ in 0..8 {
            let app = app.clone();
            let path = path.clone();
            let first_chunk_tx = first_chunk_tx.clone();
            handles.push(tokio::spawn(async move {
                let response = app
                    .oneshot(Request::builder().uri(&path).body(Body::empty()).unwrap())
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::OK);
                assert_eq!(
                    response
                        .headers()
                        .get(axum::http::header::CONTENT_LENGTH)
                        .unwrap(),
                    "11"
                );
                let mut stream = response.into_body().into_data_stream();
                let first = stream.next().await.unwrap().unwrap();
                first_chunk_tx.send(first.clone()).await.unwrap();
                let mut body = first.to_vec();
                while let Some(chunk) = stream.next().await {
                    body.extend_from_slice(&chunk.unwrap());
                }
                body
            }));
        }
        drop(first_chunk_tx);

        for _ in 0..8 {
            let first = tokio::time::timeout(Duration::from_millis(150), first_chunk_rx.recv())
                .await
                .expect("client did not receive fan-out data before upstream completed")
                .expect("first chunk channel closed");
            assert_eq!(first.as_ref(), b"wheel-");
        }
        assert_eq!(upstream.file_requests(), 1);
        assert_eq!(upstream.chunks_sent(), 1);

        for handle in handles {
            let body = handle.await.unwrap();
            assert_eq!(body.as_slice(), b"wheel-bytes");
        }
    }

    #[tokio::test]
    async fn concurrent_followers_get_response_before_leader_finishes() {
        let upstream = spawn_upstream().await;
        let temp = tempdir().unwrap();
        let state = test_state(
            CacheStore::new(temp.path().to_path_buf()),
            &upstream.base_url,
        );
        state.cache.initialize().await.unwrap();
        let app = app(state);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/simple/demo/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        let path = html
            .split('"')
            .find(|part| part.starts_with("/root/pypi/+f/"))
            .unwrap()
            .split('#')
            .next()
            .unwrap()
            .to_string();

        let leader = app
            .clone()
            .oneshot(Request::builder().uri(&path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(leader.status(), StatusCode::OK);

        let follower = tokio::time::timeout(
            Duration::from_millis(100),
            app.clone()
                .oneshot(Request::builder().uri(&path).body(Body::empty()).unwrap()),
        )
        .await
        .expect("follower should not wait for leader body to finish")
        .unwrap();
        assert_eq!(follower.status(), StatusCode::OK);
        drop(follower);
        drop(leader);
    }

    #[tokio::test]
    async fn download_task_continues_when_leader_body_is_not_consumed() {
        let data = Bytes::from(vec![
            b'x';
            BLOB_BROADCAST_CHANNEL_CAPACITY
                + BLOB_WRITE_CHANNEL_CAPACITY
                + 16
        ]);
        let upstream = spawn_chunked_upstream(data.clone(), 1, Duration::ZERO).await;
        let temp = tempdir().unwrap();
        let state = test_state(
            CacheStore::new(temp.path().to_path_buf()),
            &upstream.base_url,
        );
        state.cache.initialize().await.unwrap();
        let app = app(state);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/simple/demo/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        let path = html
            .split('"')
            .find(|part| part.starts_with("/root/pypi/+f/"))
            .unwrap()
            .split('#')
            .next()
            .unwrap()
            .to_string();

        let leader = app
            .clone()
            .oneshot(Request::builder().uri(&path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(leader.status(), StatusCode::OK);

        tokio::time::timeout(Duration::from_millis(700), async {
            while upstream.chunks_sent()
                < BLOB_BROADCAST_CHANNEL_CAPACITY + BLOB_WRITE_CHANNEL_CAPACITY + 16
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("background download task should pass the bounded buffers when leader body is not polled");

        let follower = app
            .clone()
            .oneshot(Request::builder().uri(&path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(follower.status(), StatusCode::OK);
        let body = to_bytes(follower.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body, data);
        assert_eq!(upstream.file_requests(), 1);
        drop(leader);
    }

    #[tokio::test]
    async fn late_joining_follower_tails_large_tmp_without_broadcast_lag() {
        let mut data = Vec::new();
        for n in 0..512_u32 {
            data.extend_from_slice(&n.to_le_bytes());
            data.resize((n as usize + 1) * 1024, (n % 251) as u8);
        }
        let data = Bytes::from(data);
        let upstream = spawn_chunked_upstream(data.clone(), 1024, Duration::from_millis(1)).await;
        let temp = tempdir().unwrap();
        let state = test_state(
            CacheStore::new(temp.path().to_path_buf()),
            &upstream.base_url,
        );
        state.cache.initialize().await.unwrap();
        let app = app(state);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/simple/demo/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        let path = html
            .split('"')
            .find(|part| part.starts_with("/root/pypi/+f/"))
            .unwrap()
            .split('#')
            .next()
            .unwrap()
            .to_string();

        let leader_app = app.clone();
        let leader_path = path.clone();
        let leader = tokio::spawn(async move {
            let response = leader_app
                .oneshot(
                    Request::builder()
                        .uri(&leader_path)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            to_bytes(response.into_body(), usize::MAX).await.unwrap()
        });

        tokio::time::timeout(Duration::from_secs(2), async {
            while upstream.chunks_sent() < 300 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("upstream should have written a large .tmp prefix before follower joins");

        let follower = app
            .clone()
            .oneshot(Request::builder().uri(&path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(follower.status(), StatusCode::OK);
        let follower_body = to_bytes(follower.into_body(), usize::MAX).await.unwrap();
        assert_eq!(follower_body, data);
        let leader_body = leader.await.unwrap();
        assert_eq!(leader_body, data);
        assert_eq!(upstream.file_requests(), 1);
    }

    #[tokio::test]
    async fn resumes_partial_file_downloads_with_range() {
        let upstream = spawn_upstream().await;
        let temp = tempdir().unwrap();
        let cache = CacheStore::new(temp.path().to_path_buf());
        let state = test_state(cache.clone(), &upstream.base_url);
        state.cache.initialize().await.unwrap();
        let app = app(state);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/simple/demo/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        let path = html
            .split('"')
            .find(|part| part.starts_with("/root/pypi/+f/"))
            .unwrap()
            .split('#')
            .next()
            .unwrap()
            .to_string();

        let blob_id = "9ceb18f15662bb87e54af2f5953c0484d2ef76f5444d87913360b9ef87d7296d";
        let relpath = cache
            .blob_storage_relpath("sha256", blob_id, "demo-1.0.whl")
            .unwrap();
        cache.prepare_blob_parent(&relpath).await.unwrap();
        tokio::fs::write(cache.blob_temp_path(&relpath), b"wheel-")
            .await
            .unwrap();

        let response = app
            .clone()
            .oneshot(Request::builder().uri(&path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_LENGTH)
                .unwrap(),
            "11"
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body.as_ref(), b"wheel-bytes");
    }

    #[tokio::test]
    async fn serves_stale_project_when_upstream_is_broken() {
        let upstream = spawn_upstream().await;
        let temp = tempdir().unwrap();
        let mut state = test_state(
            CacheStore::new(temp.path().to_path_buf()),
            &upstream.base_url,
        );
        state.project_cache_ttl_secs = 0;
        state.cache.initialize().await.unwrap();
        let app = app(state);

        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/simple/demo/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        *upstream.state.broken.lock().await = true;

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/simple/demo/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(WARNING).unwrap(),
            "110 - response is stale"
        );
    }

    async fn spawn_upstream() -> TestUpstream {
        spawn_upstream_with_slow_file(false).await
    }

    async fn spawn_slow_upstream() -> TestUpstream {
        spawn_upstream_with_slow_file(true).await
    }

    async fn spawn_upstream_with_slow_file(slow_file: bool) -> TestUpstream {
        let data = Bytes::from_static(b"wheel-bytes");
        let chunk_bytes = if slow_file { 6 } else { data.len() };
        let chunk_delay = if slow_file {
            Duration::from_millis(250)
        } else {
            Duration::ZERO
        };
        spawn_chunked_upstream(data, chunk_bytes, chunk_delay).await
    }

    async fn spawn_chunked_upstream(
        data: Bytes,
        chunk_bytes: usize,
        chunk_delay: Duration,
    ) -> TestUpstream {
        let metadata = Bytes::from_static(b"metadata-bytes");
        let state = Arc::new(TestUpstreamState {
            broken: Mutex::new(false),
            file_requests: AtomicUsize::new(0),
            metadata_requests: AtomicUsize::new(0),
            chunks_sent: AtomicUsize::new(0),
            sha256: format!("{:x}", Sha256::digest(data.as_ref())),
            metadata_sha256: format!("{:x}", Sha256::digest(metadata.as_ref())),
            data,
            metadata,
            chunk_bytes,
            chunk_delay,
        });
        let app = Router::new()
            .route("/simple/demo/", get(upstream_demo))
            .route("/packages/demo-1.0.whl", get(upstream_file))
            .route("/packages/demo-1.0.whl.metadata", get(upstream_metadata))
            .with_state(state.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        TestUpstream {
            base_url: format!("http://{addr}/"),
            state,
        }
    }

    async fn upstream_demo(
        State(state): State<Arc<TestUpstreamState>>,
        headers: HeaderMap,
    ) -> Response<Body> {
        if *state.broken.lock().await {
            let mut response = Response::new(Body::from("boom"));
            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            return response;
        }
        if wants_json(header_value(&headers, ACCEPT)) {
            let body = serde_json::json!({
                "meta": {"api-version": "1.0"},
                "name": "demo",
                "files": [
                    {
                        "filename": "demo-1.0.whl",
                        "url": "/packages/demo-1.0.whl",
                        "hashes": {"sha256": state.sha256},
                        "requires-python": ">=3.10",
                        "dist-info-metadata": {"sha256": state.metadata_sha256},
                    }
                ],
            })
            .to_string();
            let mut response = Response::new(Body::from(body));
            response
                .headers_mut()
                .insert(CONTENT_TYPE, HeaderValue::from_static(json_media_type()));
            response
                .headers_mut()
                .insert(ETAG, HeaderValue::from_static("\"etag-demo\""));
            response
                .headers_mut()
                .insert("x-pypi-last-serial", HeaderValue::from_static("17"));
            return response;
        }
        let body = r#"
            <!DOCTYPE html>
            <html><body>
              <a href="/packages/demo-1.0.whl#sha256=__SHA256__"
                 data-requires-python="&gt;=3.10"
                 data-dist-info-metadata="sha256=__METADATA_SHA256__">demo-1.0.whl</a>
            </body></html>
        "#
        .replace("__SHA256__", &state.sha256)
        .replace("__METADATA_SHA256__", &state.metadata_sha256);
        let mut response = Response::new(Body::from(body));
        response
            .headers_mut()
            .insert(ETAG, HeaderValue::from_static("\"etag-demo\""));
        response
            .headers_mut()
            .insert("x-pypi-last-serial", HeaderValue::from_static("17"));
        response
    }

    async fn upstream_file(
        State(state): State<Arc<TestUpstreamState>>,
        headers: HeaderMap,
    ) -> impl IntoResponse {
        state.file_requests.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(25)).await;
        let data = state.data.clone();
        if let Some(start) = headers
            .get(RANGE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("bytes="))
            .and_then(|value| value.strip_suffix('-'))
            .and_then(|value| value.parse::<usize>().ok())
        {
            let mut response = Response::new(Body::from(data.slice(start..)));
            *response.status_mut() = StatusCode::PARTIAL_CONTENT;
            response.headers_mut().insert(
                CONTENT_TYPE,
                HeaderValue::from_static("application/octet-stream"),
            );
            response.headers_mut().insert(
                axum::http::header::CONTENT_RANGE,
                HeaderValue::from_str(&format!("bytes {start}-{}/{}", data.len() - 1, data.len()))
                    .unwrap(),
            );
            response.headers_mut().insert(
                axum::http::header::CONTENT_LENGTH,
                header(&(data.len() - start).to_string()),
            );
            return response;
        }
        let mut response = Response::new(Body::from_stream(stream::unfold(0, {
            let state = state.clone();
            move |offset| {
                let state = state.clone();
                async move {
                    if offset >= state.data.len() {
                        return None;
                    }
                    if offset > 0 && !state.chunk_delay.is_zero() {
                        tokio::time::sleep(state.chunk_delay).await;
                    }
                    let end = (offset + state.chunk_bytes).min(state.data.len());
                    state.chunks_sent.fetch_add(1, Ordering::SeqCst);
                    Some((Ok::<_, io::Error>(state.data.slice(offset..end)), end))
                }
            }
        })));
        response.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        response.headers_mut().insert(
            axum::http::header::CONTENT_LENGTH,
            header(&data.len().to_string()),
        );
        response
    }

    async fn upstream_metadata(State(state): State<Arc<TestUpstreamState>>) -> impl IntoResponse {
        state.metadata_requests.fetch_add(1, Ordering::SeqCst);
        let mut response = Response::new(Body::from(state.metadata.clone()));
        response.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        response.headers_mut().insert(
            axum::http::header::CONTENT_LENGTH,
            header(&state.metadata.len().to_string()),
        );
        response
    }

    struct TestUpstream {
        base_url: String,
        state: Arc<TestUpstreamState>,
    }

    impl TestUpstream {
        fn file_requests(&self) -> usize {
            self.state.file_requests.load(Ordering::SeqCst)
        }

        fn metadata_requests(&self) -> usize {
            self.state.metadata_requests.load(Ordering::SeqCst)
        }

        fn chunks_sent(&self) -> usize {
            self.state.chunks_sent.load(Ordering::SeqCst)
        }
    }

    struct TestUpstreamState {
        broken: Mutex<bool>,
        file_requests: AtomicUsize,
        metadata_requests: AtomicUsize,
        chunks_sent: AtomicUsize,
        sha256: String,
        metadata_sha256: String,
        data: Bytes,
        metadata: Bytes,
        chunk_bytes: usize,
        chunk_delay: Duration,
    }
}
