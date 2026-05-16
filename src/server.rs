use crate::cache::{
    BlobInfo, BlobStatus, BlobWrite, CacheStore, CachedLink, ProjectCache, ProjectRecord,
    current_unix_secs, fallback_blob_identity, hash_blob_identity,
};
use crate::config::AppConfig;
use crate::simple::{
    json_media_type, normalize_project_name, parse_project_links, render_project_html,
    render_project_json, render_root_html, render_root_json, wants_json,
};
use crate::upstream::{ProjectFetch, UpstreamClient};
use axum::Router;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::header::{ACCEPT, CONTENT_TYPE, ETAG, IF_NONE_MATCH, LOCATION, WARNING};
use axum::http::{HeaderMap, HeaderValue, Response, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use bytes::Bytes;
use futures_util::{Stream, StreamExt, TryStreamExt};
use mime_guess::from_path;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Notify, OwnedMutexGuard, RwLock, broadcast};
use tokio_util::io::ReaderStream;
use url::Url;

#[derive(Clone)]
struct AppState {
    cache: CacheStore,
    upstream: UpstreamClient,
    project_cache_ttl_secs: u64,
    locks: RequestLocks,
    active_blobs: ActiveBlobRegistry,
    hot_projects: HotProjectCache,
}

type ActiveBlobRegistry = Arc<Mutex<HashMap<String, Arc<ActiveBlob>>>>;
type HotProjectCache = Arc<RwLock<HashMap<String, ProjectCache>>>;

struct ActiveBlob {
    temp_path: std::path::PathBuf,
    final_path: std::path::PathBuf,
    content_type: String,
    content_length: AtomicU64,
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
struct RequestLocks {
    projects: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    blobs: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
}

impl RequestLocks {
    async fn project_guard(&self, project: &str) -> OwnedMutexGuard<()> {
        self.guard(&self.projects, project).await
    }

    async fn blob_guard(&self, blob_kind: &str, blob_id: &str) -> OwnedMutexGuard<()> {
        self.guard(&self.blobs, &format!("{blob_kind}:{blob_id}"))
            .await
    }

    async fn guard(
        &self,
        map: &Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
        key: &str,
    ) -> OwnedMutexGuard<()> {
        let lock = {
            let mut map = map.lock().await;
            map.entry(key.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        lock.lock_owned().await
    }
}

pub async fn run(config: AppConfig) -> io::Result<()> {
    let listener = TcpListener::bind(&config.bind).await?;
    serve_listener(config, listener).await
}

pub async fn serve_listener(config: AppConfig, listener: TcpListener) -> io::Result<()> {
    let cache = CacheStore::new(config.cache_dir.clone());
    cache.initialize().await?;
    let state = AppState {
        cache,
        upstream: UpstreamClient::new(&config.upstream_base_url, config.request_timeout_secs)?,
        project_cache_ttl_secs: config.project_cache_ttl_secs,
        locks: RequestLocks::default(),
        active_blobs: ActiveBlobRegistry::default(),
        hot_projects: HotProjectCache::default(),
    };
    let app = app(state);
    axum::serve(listener, app).await.map_err(io::Error::other)
}

fn app(state: AppState) -> Router {
    Router::new()
        .route("/", get(root_redirect))
        .route("/simple", get(simple_root))
        .route("/simple/", get(simple_root))
        .route("/simple/{project}", get(simple_project))
        .route("/simple/{project}/", get(simple_project))
        .route(
            "/root/pypi/{plusf}/{route_a}/{route_b}/{filename}",
            get(package_file),
        )
        .with_state(Arc::new(state))
}

async fn root_redirect() -> impl IntoResponse {
    (
        StatusCode::TEMPORARY_REDIRECT,
        [(LOCATION, HeaderValue::from_static("/simple/"))],
    )
}

async fn simple_root(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Response<Body>, StatusCode> {
    let projects = state
        .cache
        .list_projects()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let format_json = wants_json(header_value(&headers, ACCEPT));
    let body = if format_json {
        render_root_json(&projects)
    } else {
        render_root_html(&projects)
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
    match ensure_project(&state, &normalized).await {
        Ok(ProjectState::Ready(cache, stale)) => {
            let format_json = wants_json(header_value(&headers, ACCEPT));
            let body = if format_json {
                render_project_json(&normalized, &cache.links)
            } else {
                render_project_html(&normalized, &cache.links)
            };
            render_simple_response(
                &headers,
                if format_json {
                    json_media_type()
                } else {
                    "text/html; charset=utf-8"
                },
                body,
                stale,
            )
        }
        Ok(ProjectState::Missing) => text_response(StatusCode::NOT_FOUND, "project not found\n"),
        Err(status) => text_response(status, "upstream unavailable\n"),
    }
}

async fn package_file(
    State(state): State<Arc<AppState>>,
    Path((plusf, route_a, route_b, filename)): Path<(String, String, String, String)>,
) -> Response<Body> {
    if plusf != "+f" {
        return text_response(StatusCode::NOT_FOUND, "file unavailable\n");
    }
    match ensure_blob(&state, &route_a, &route_b, &filename).await {
        Ok(BlobResponse::Ready(blob)) => cached_file_response(&state.cache, &blob).await,
        Ok(BlobResponse::Streaming(response)) => response,
        Err(status) => text_response(status, "file unavailable\n"),
    }
}

enum ProjectState {
    Ready(ProjectCache, bool),
    Missing,
}

async fn ensure_project(state: &AppState, project: &str) -> Result<ProjectState, StatusCode> {
    if let Some(cached) = state.hot_projects.read().await.get(project).cloned()
        && state.cache.project_is_fresh(&cached)
    {
        return Ok(ProjectState::Ready(cached, false));
    }

    if let Some(cached) = state
        .cache
        .load_project(project)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        && state.cache.project_is_fresh(&cached)
    {
        state
            .hot_projects
            .write()
            .await
            .insert(project.to_string(), cached.clone());
        return Ok(ProjectState::Ready(cached, false));
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
        state
            .hot_projects
            .write()
            .await
            .insert(project.to_string(), project_cache.clone());
        return Ok(ProjectState::Ready(project_cache.clone(), false));
    }

    let etag = cached
        .as_ref()
        .and_then(|project_cache| project_cache.upstream_etag.as_deref());
    let result = match state.upstream.fetch_project(project, etag).await {
        Ok(ProjectFetch::Fresh(page)) => {
            let page_url = Url::parse(&page.project_url).map_err(|_| StatusCode::BAD_GATEWAY)?;
            let parsed_links = parse_project_links(&page.body, &page_url);
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
            state
                .hot_projects
                .write()
                .await
                .insert(project.to_string(), cached.clone());
            Ok(ProjectState::Ready(cached, false))
        }
        Ok(ProjectFetch::NotModified) => {
            if let Some(mut cached_value) = cached.clone() {
                let now = current_unix_secs();
                cached_value.fetched_at = now;
                cached_value.expires_at = now + state.project_cache_ttl_secs;
                let record = ProjectRecord {
                    project: cached_value.project.clone(),
                    fetched_at: cached_value.fetched_at,
                    expires_at: cached_value.expires_at,
                    upstream_etag: cached_value.upstream_etag.clone(),
                    upstream_serial: cached_value.upstream_serial,
                    upstream_project_url: cached_value.upstream_project_url.clone(),
                    raw_body: cached_value.raw_body.clone(),
                    links: cached_value.links.clone(),
                };
                let cached = state
                    .cache
                    .store_project(&record)
                    .await
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                state
                    .hot_projects
                    .write()
                    .await
                    .insert(project.to_string(), cached.clone());
                Ok(ProjectState::Ready(cached, false))
            } else {
                Err(StatusCode::BAD_GATEWAY)
            }
        }
        Ok(ProjectFetch::NotFound) => Ok(ProjectState::Missing),
        Err(_) => Err(StatusCode::BAD_GATEWAY),
    };
    match result {
        Ok(value) => Ok(value),
        Err(status) => {
            if let Some(cached) = cached {
                state
                    .hot_projects
                    .write()
                    .await
                    .insert(project.to_string(), cached.clone());
                Ok(ProjectState::Ready(cached, true))
            } else {
                Err(status)
            }
        }
    }
}

enum BlobResponse {
    Ready(BlobInfo),
    Streaming(Response<Body>),
}

fn blob_key(blob_kind: &str, blob_id: &str) -> String {
    format!("{blob_kind}:{blob_id}")
}

async fn ensure_blob(
    state: &AppState,
    route_a: &str,
    route_b: &str,
    filename: &str,
) -> Result<BlobResponse, StatusCode> {
    let Some(link) = state
        .cache
        .find_link_by_blob(route_a, route_b, filename)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };

    if let BlobStatus::Ready(blob) = state
        .cache
        .blob_status(&link.blob_kind, &link.blob_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    {
        return Ok(BlobResponse::Ready(blob));
    }

    let storage_relpath = state
        .cache
        .blob_storage_relpath(&link.blob_kind, &link.blob_id, filename)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let key = blob_key(&link.blob_kind, &link.blob_id);
    if let Some(active) = state.active_blobs.lock().await.get(&key).cloned() {
        return active_blob_response(active).await;
    }

    let guard = state.locks.blob_guard(&link.blob_kind, &link.blob_id).await;
    if let BlobStatus::Ready(blob) = state
        .cache
        .blob_status(&link.blob_kind, &link.blob_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    {
        return Ok(BlobResponse::Ready(blob));
    }
    if let Some(active) = state.active_blobs.lock().await.get(&key).cloned() {
        return active_blob_response(active).await;
    }
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
    let can_resume = link.hash_name.as_deref() == Some("sha256");
    let resume_from = if can_resume {
        state
            .cache
            .blob_temp_len(&storage_relpath)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    } else {
        0
    };
    let active = Arc::new(ActiveBlob {
        temp_path: state.cache.blob_temp_path(&storage_relpath),
        final_path: state.cache.blob_path(&storage_relpath),
        content_type: initial_content_type.clone(),
        content_length: AtomicU64::new(0),
        bytes_written: AtomicU64::new(resume_from),
        finished: AtomicBool::new(false),
        notify: Notify::new(),
        chunk_tx: broadcast::channel(1024).0,
        failure: std::sync::Mutex::new(None),
    });
    state
        .active_blobs
        .lock()
        .await
        .insert(key.clone(), active.clone());
    drop(guard);

    tokio::spawn(download_blob_task(DownloadJob {
        cache: state.cache.clone(),
        upstream: state.upstream.clone(),
        active_blobs: state.active_blobs.clone(),
        active_key: key.clone(),
        active: active.clone(),
        link,
        storage_relpath,
        initial_content_type,
        resume_from,
    }));

    active_blob_response(active).await
}

async fn cached_file_response(cache: &CacheStore, blob: &BlobInfo) -> Response<Body> {
    match File::open(cache.blob_path(&blob.storage_relpath)).await {
        Ok(file) => {
            let stream = ReaderStream::new(file).map_err(io::Error::other);
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
        }
        Err(_) => text_response(StatusCode::INTERNAL_SERVER_ERROR, "cache file missing\n"),
    }
}

async fn active_blob_response(active: Arc<ActiveBlob>) -> Result<BlobResponse, StatusCode> {
    let stream: BoxByteStream = Box::pin(tail_file_stream(
        active.clone(),
        active.chunk_tx.subscribe(),
    ));
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = StatusCode::OK;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, header(&active.content_type));
    let content_length = wait_for_active_content_length(&active).await;
    if let Some(content_length) = content_length
        && let Ok(value) = HeaderValue::from_str(&content_length.to_string())
    {
        response
            .headers_mut()
            .insert(axum::http::header::CONTENT_LENGTH, value);
    }
    Ok(BlobResponse::Streaming(response))
}

async fn wait_for_active_content_length(active: &ActiveBlob) -> Option<u64> {
    for _ in 0..100 {
        let content_length = active.content_length.load(Ordering::SeqCst);
        if content_length > 0 {
            return Some(content_length);
        }
        if active.finished.load(Ordering::SeqCst) {
            return None;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    None
}

type BoxByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + 'static>>;

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
                if let Some(message) = active.failure.lock().ok().and_then(|value| value.clone()) {
                    return Some((Err(io::Error::other(message)), (active, rx, file, offset)));
                }

                match rx.try_recv() {
                    Ok(chunk) => match chunk_for_offset(chunk, offset) {
                        ChunkRead::Ready(bytes) => {
                            offset += bytes.len() as u64;
                            return Some((Ok(bytes), (active, rx, file, offset)));
                        }
                        ChunkRead::AlreadyRead => continue,
                        ChunkRead::Gap => {}
                    },
                    Err(broadcast::error::TryRecvError::Lagged(_)) => {}
                    Err(broadcast::error::TryRecvError::Empty) => {}
                    Err(broadcast::error::TryRecvError::Closed) => {
                        if active.finished.load(Ordering::SeqCst) {
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
                        Err(err) => return Some((Err(err), (active, rx, file, offset))),
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
                        Ok(0) => return None,
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
                    return None;
                }
                if active.finished.load(Ordering::SeqCst) {
                    return None;
                }
                tokio::select! {
                    received = rx.recv() => {
                        match received {
                            Ok(chunk) => match chunk_for_offset(chunk, offset) {
                                ChunkRead::Ready(bytes) => {
                                    offset += bytes.len() as u64;
                                    return Some((Ok(bytes), (active, rx, file, offset)));
                                }
                                ChunkRead::AlreadyRead | ChunkRead::Gap => continue,
                            },
                            Err(broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(broadcast::error::RecvError::Closed) => {
                                if active.finished.load(Ordering::SeqCst) {
                                    return None;
                                }
                            }
                        }
                    }
                    _ = active.notify.notified() => {
                        if active.finished.load(Ordering::SeqCst) {
                            return None;
                        }
                    }
                }
            }
        },
    )
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
        if let Ok(mut failure) = job.active.failure.lock() {
            *failure = Some(message);
        }
        job.active.finished.store(true, Ordering::SeqCst);
        job.active.notify.notify_waiters();
        job.active_blobs.lock().await.remove(&job.active_key);
    }
}

async fn download_blob_task_inner(job: &DownloadJob) -> io::Result<()> {
    job.cache.prepare_blob_parent(&job.storage_relpath).await?;
    let mut resume_from = job.resume_from;
    let temp_path = job.cache.blob_temp_path(&job.storage_relpath);
    let mut hasher = Sha256::new();
    if resume_from > 0 {
        hash_existing_prefix(&temp_path, &mut hasher).await?;
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
        let _ = tokio::fs::remove_file(&temp_path).await;
        resume_from = 0;
        hasher = Sha256::new();
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
        job.active
            .content_length
            .store(content_length, Ordering::SeqCst);
        job.active.notify.notify_waiters();
    }

    let mut file = OpenOptions::new()
        .create(true)
        .append(resume_from > 0)
        .write(true)
        .truncate(resume_from == 0)
        .open(&temp_path)
        .await?;
    let mut bytes_written = resume_from;
    let mut stream = upstream_file.into_stream().map_err(io::Error::other);
    let mut pending = Vec::with_capacity(256 * 1024);
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        hasher.update(&chunk);
        pending.extend_from_slice(&chunk);
        if bytes_written > resume_from && pending.len() < 256 * 1024 {
            continue;
        }
        file.write_all(&pending).await?;
        let chunk_offset = bytes_written;
        bytes_written += pending.len() as u64;
        job.active
            .bytes_written
            .store(bytes_written, Ordering::SeqCst);
        let _ = job.active.chunk_tx.send(BlobChunk {
            offset: chunk_offset,
            bytes: Bytes::copy_from_slice(&pending),
        });
        job.active.notify.notify_waiters();
        pending.clear();
    }
    if !pending.is_empty() {
        file.write_all(&pending).await?;
        let chunk_offset = bytes_written;
        bytes_written += pending.len() as u64;
        job.active
            .bytes_written
            .store(bytes_written, Ordering::SeqCst);
        let _ = job.active.chunk_tx.send(BlobChunk {
            offset: chunk_offset,
            bytes: Bytes::copy_from_slice(&pending),
        });
        job.active.notify.notify_waiters();
    }

    file.flush().await?;
    file.sync_all().await?;
    let actual_hash = format!("{:x}", hasher.finalize());
    if job.link.hash_name.as_deref() == Some("sha256")
        && let Some(expected) = &job.link.hash_value
        && actual_hash != *expected
    {
        let _ = tokio::fs::remove_file(&temp_path).await;
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "downloaded file sha256 did not match link hash",
        ));
    }

    job.cache
        .commit_blob_file(&temp_path, &job.storage_relpath)
        .await?;
    job.cache
        .mark_blob_ready(&BlobWrite {
            blob_kind: job.link.blob_kind.clone(),
            blob_id: job.link.blob_id.clone(),
            storage_relpath: job.storage_relpath.clone(),
            content_type,
            fetched_at: current_unix_secs(),
            size_bytes: bytes_written,
            filename: job.link.filename.clone(),
            upstream_url: job.link.upstream_url.clone(),
        })
        .await?;
    job.active.finished.store(true, Ordering::SeqCst);
    job.active.notify.notify_waiters();
    job.active_blobs.lock().await.remove(&job.active_key);
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
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = StatusCode::OK;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, header(content_type));
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

fn header_value<'a>(
    headers: &'a HeaderMap,
    name: axum::http::header::HeaderName,
) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

#[cfg(test)]
mod tests {
    use super::*;
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

    #[tokio::test]
    async fn caches_project_and_rewrites_file_links() {
        let upstream = spawn_upstream().await;
        let temp = tempdir().unwrap();
        let state = AppState {
            cache: CacheStore::new(temp.path().to_path_buf()),
            upstream: UpstreamClient::new(&upstream.base_url, 5).unwrap(),
            project_cache_ttl_secs: 3600,
            locks: RequestLocks::default(),
            active_blobs: ActiveBlobRegistry::default(),
            hot_projects: HotProjectCache::default(),
        };
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
    async fn returns_json_simple_api_when_requested() {
        let upstream = spawn_upstream().await;
        let temp = tempdir().unwrap();
        let state = AppState {
            cache: CacheStore::new(temp.path().to_path_buf()),
            upstream: UpstreamClient::new(&upstream.base_url, 5).unwrap(),
            project_cache_ttl_secs: 3600,
            locks: RequestLocks::default(),
            active_blobs: ActiveBlobRegistry::default(),
            hot_projects: HotProjectCache::default(),
        };
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
        let state = AppState {
            cache: CacheStore::new(temp.path().to_path_buf()),
            upstream: UpstreamClient::new(&upstream.base_url, 5).unwrap(),
            project_cache_ttl_secs: 3600,
            locks: RequestLocks::default(),
            active_blobs: ActiveBlobRegistry::default(),
            hot_projects: HotProjectCache::default(),
        };
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
    async fn concurrent_file_downloads_share_single_upstream_fetch() {
        let upstream = spawn_upstream().await;
        let temp = tempdir().unwrap();
        let state = AppState {
            cache: CacheStore::new(temp.path().to_path_buf()),
            upstream: UpstreamClient::new(&upstream.base_url, 5).unwrap(),
            project_cache_ttl_secs: 3600,
            locks: RequestLocks::default(),
            active_blobs: ActiveBlobRegistry::default(),
            hot_projects: HotProjectCache::default(),
        };
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
        let state = AppState {
            cache: CacheStore::new(temp.path().to_path_buf()),
            upstream: UpstreamClient::new(&upstream.base_url, 5).unwrap(),
            project_cache_ttl_secs: 3600,
            locks: RequestLocks::default(),
            active_blobs: ActiveBlobRegistry::default(),
            hot_projects: HotProjectCache::default(),
        };
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
        let state = AppState {
            cache: CacheStore::new(temp.path().to_path_buf()),
            upstream: UpstreamClient::new(&upstream.base_url, 5).unwrap(),
            project_cache_ttl_secs: 3600,
            locks: RequestLocks::default(),
            active_blobs: ActiveBlobRegistry::default(),
            hot_projects: HotProjectCache::default(),
        };
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
        let upstream = spawn_slow_upstream().await;
        let temp = tempdir().unwrap();
        let state = AppState {
            cache: CacheStore::new(temp.path().to_path_buf()),
            upstream: UpstreamClient::new(&upstream.base_url, 5).unwrap(),
            project_cache_ttl_secs: 3600,
            locks: RequestLocks::default(),
            active_blobs: ActiveBlobRegistry::default(),
            hot_projects: HotProjectCache::default(),
        };
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
            while upstream.chunks_sent() < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("background download task should finish even if leader body is not polled");

        let follower = app
            .clone()
            .oneshot(Request::builder().uri(&path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(follower.status(), StatusCode::OK);
        let body = to_bytes(follower.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body.as_ref(), b"wheel-bytes");
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
        let state = AppState {
            cache: CacheStore::new(temp.path().to_path_buf()),
            upstream: UpstreamClient::new(&upstream.base_url, 5).unwrap(),
            project_cache_ttl_secs: 3600,
            locks: RequestLocks::default(),
            active_blobs: ActiveBlobRegistry::default(),
            hot_projects: HotProjectCache::default(),
        };
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
        let state = AppState {
            cache: cache.clone(),
            upstream: UpstreamClient::new(&upstream.base_url, 5).unwrap(),
            project_cache_ttl_secs: 3600,
            locks: RequestLocks::default(),
            active_blobs: ActiveBlobRegistry::default(),
            hot_projects: HotProjectCache::default(),
        };
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
        let state = AppState {
            cache: CacheStore::new(temp.path().to_path_buf()),
            upstream: UpstreamClient::new(&upstream.base_url, 5).unwrap(),
            project_cache_ttl_secs: 0,
            locks: RequestLocks::default(),
            active_blobs: ActiveBlobRegistry::default(),
            hot_projects: HotProjectCache::default(),
        };
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
        let state = Arc::new(TestUpstreamState {
            broken: Mutex::new(false),
            file_requests: AtomicUsize::new(0),
            chunks_sent: AtomicUsize::new(0),
            sha256: format!("{:x}", Sha256::digest(data.as_ref())),
            data,
            chunk_bytes,
            chunk_delay,
        });
        let app = Router::new()
            .route("/simple/demo/", get(upstream_demo))
            .route("/packages/demo-1.0.whl", get(upstream_file))
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

    async fn upstream_demo(State(state): State<Arc<TestUpstreamState>>) -> Response<Body> {
        if *state.broken.lock().await {
            let mut response = Response::new(Body::from("boom"));
            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            return response;
        }
        let body = r#"
            <!DOCTYPE html>
            <html><body>
              <a href="/packages/demo-1.0.whl#sha256=__SHA256__"
                 data-requires-python="&gt;=3.10">demo-1.0.whl</a>
            </body></html>
        "#
        .replace("__SHA256__", &state.sha256);
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

    struct TestUpstream {
        base_url: String,
        state: Arc<TestUpstreamState>,
    }

    impl TestUpstream {
        fn file_requests(&self) -> usize {
            self.state.file_requests.load(Ordering::SeqCst)
        }

        fn chunks_sent(&self) -> usize {
            self.state.chunks_sent.load(Ordering::SeqCst)
        }
    }

    struct TestUpstreamState {
        broken: Mutex<bool>,
        file_requests: AtomicUsize,
        chunks_sent: AtomicUsize,
        sha256: String,
        data: Bytes,
        chunk_bytes: usize,
        chunk_delay: Duration,
    }
}
