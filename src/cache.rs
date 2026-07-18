use crate::simple::normalize_project_name;
use bytes::Bytes;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio::sync::Mutex;
use tracing::info;

const BLOB_STATE_PENDING: &str = "pending";
const BLOB_STATE_READY: &str = "ready";
const CACHE_SCHEMA_VERSION: u32 = 2;
const PROJECT_RESPONSE_COMPRESSION_LEVEL: i32 = 1;
const MAX_PROJECT_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

thread_local! {
    static SQLITE_CONNECTIONS: RefCell<HashMap<PathBuf, Connection>> = RefCell::new(HashMap::new());
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CachedLink {
    pub filename: String,
    pub upstream_url: String,
    pub blob_kind: String,
    pub blob_id: String,
    pub cached_size_bytes: Option<u64>,
    pub requires_python: Option<String>,
    pub yanked: Option<String>,
    pub gpg_sig: Option<bool>,
    pub dist_info_metadata: Option<String>,
    pub core_metadata: Option<String>,
    pub hash_name: Option<String>,
    pub hash_value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectSummary {
    pub project: String,
    pub display_name: String,
    pub page_url: String,
    pub file_count: u64,
    pub cached_file_count: u64,
    pub cached_size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheSummary {
    pub project_count: u64,
    pub ready_blob_count: u64,
    pub cached_size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RootDashboardSnapshot {
    pub projects: Vec<ProjectSummary>,
    pub cache_summary: CacheSummary,
    pub history: Vec<RootHistorySample>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RootHistorySample {
    pub sampled_at: u64,
    pub cached_size_bytes: u64,
    pub package_count: u64,
    pub hit_rate_percent: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProjectCache {
    pub project: String,
    pub fetched_at: u64,
    pub expires_at: u64,
    pub upstream_etag: Option<String>,
    pub upstream_serial: Option<u64>,
    pub upstream_project_url: String,
    pub links: Vec<CachedLink>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobInfo {
    pub blob_kind: String,
    pub blob_id: String,
    pub storage_relpath: String,
    pub content_type: String,
    pub fetched_at: u64,
    pub size_bytes: u64,
    pub filename: String,
    pub upstream_url: String,
    pub state: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobReadyResult {
    pub blob: BlobInfo,
    pub projects: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvictedBlob {
    pub blob_kind: String,
    pub blob_id: String,
    pub storage_relpath: String,
    pub size_bytes: u64,
    pub projects: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlobStatus {
    Ready(BlobInfo),
    Pending,
    Missing,
}

#[derive(Debug, Clone)]
pub struct CacheStore {
    root: PathBuf,
    db_path: PathBuf,
    db_write_lock: Arc<Mutex<()>>,
}

#[derive(Debug, Clone)]
pub struct ProjectRecord {
    pub project: String,
    pub fetched_at: u64,
    pub expires_at: u64,
    pub upstream_etag: Option<String>,
    pub upstream_serial: Option<u64>,
    pub upstream_project_url: String,
    pub links: Vec<CachedLink>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectResponseFormat {
    Html,
    Json,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedProjectResponses {
    pub project: String,
    pub fetched_at: u64,
    pub expires_at: u64,
    pub upstream_etag: Option<String>,
    pub upstream_serial: Option<u64>,
    pub upstream_project_url: String,
    pub html_body: Bytes,
    pub html_etag: String,
    pub json_body: Bytes,
    pub json_etag: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectResponseRecord {
    pub project: String,
    pub html_body: Bytes,
    pub html_etag: String,
    pub json_body: Bytes,
    pub json_etag: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectResponseValidator {
    pub expires_at: u64,
    pub etag: String,
}

#[derive(Debug, Clone)]
struct CompressedProjectResponseRecord {
    project: String,
    html_body: Vec<u8>,
    html_size: usize,
    html_etag: String,
    json_body: Vec<u8>,
    json_size: usize,
    json_etag: String,
}

#[derive(Debug, Clone)]
pub struct BlobWrite {
    pub blob_kind: String,
    pub blob_id: String,
    pub storage_relpath: String,
    pub content_type: String,
    pub fetched_at: u64,
    pub size_bytes: u64,
    pub filename: String,
    pub upstream_url: String,
}

impl CacheStore {
    pub fn new(root: PathBuf) -> Self {
        let db_path = root.join("index.sqlite3");
        Self {
            root,
            db_path,
            db_write_lock: Arc::new(Mutex::new(())),
        }
    }

    pub async fn initialize(&self) -> io::Result<()> {
        fs::create_dir_all(self.files_root()).await?;
        let _write_guard = self.db_write_lock.lock().await;
        let db_path = self.db_path.clone();
        tokio::task::spawn_blocking(move || init_db(&db_path))
            .await
            .map_err(join_error)?
    }

    pub async fn list_projects(&self) -> io::Result<Vec<String>> {
        let db_path = self.db_path.clone();
        tokio::task::spawn_blocking(move || with_db(&db_path, list_projects_db))
            .await
            .map_err(join_error)?
    }

    pub async fn list_project_summaries(&self) -> io::Result<Vec<ProjectSummary>> {
        let db_path = self.db_path.clone();
        tokio::task::spawn_blocking(move || with_db(&db_path, list_project_summaries_db))
            .await
            .map_err(join_error)?
    }

    pub async fn cache_summary(&self) -> io::Result<CacheSummary> {
        let db_path = self.db_path.clone();
        tokio::task::spawn_blocking(move || with_db(&db_path, cache_summary_db))
            .await
            .map_err(join_error)?
    }

    pub async fn root_dashboard_snapshot_since(
        &self,
        history_since: u64,
    ) -> io::Result<RootDashboardSnapshot> {
        let db_path = self.db_path.clone();
        tokio::task::spawn_blocking(move || {
            with_db(&db_path, |conn| {
                Ok(RootDashboardSnapshot {
                    projects: list_project_summaries_db(conn)?,
                    cache_summary: cache_summary_db(conn)?,
                    history: root_stats_history_since_db(conn, history_since)?,
                })
            })
        })
        .await
        .map_err(join_error)?
    }

    pub async fn record_root_stats_sample(&self, sample: RootHistorySample) -> io::Result<()> {
        let _write_guard = self.db_write_lock.lock().await;
        let db_path = self.db_path.clone();
        tokio::task::spawn_blocking(move || {
            with_db(&db_path, |conn| {
                conn.execute(
                    "INSERT INTO root_stats_samples (
                    sampled_at, cached_size_bytes, package_count, hit_rate_percent
                 ) VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(sampled_at) DO UPDATE SET
                    cached_size_bytes = excluded.cached_size_bytes,
                    package_count = excluded.package_count,
                    hit_rate_percent = excluded.hit_rate_percent",
                    params![
                        sample.sampled_at,
                        sample.cached_size_bytes,
                        sample.package_count,
                        sample.hit_rate_percent,
                    ],
                )
                .map_err(sqlite_error)?;
                Ok(())
            })
        })
        .await
        .map_err(join_error)?
    }

    pub async fn root_stats_history_since(&self, since: u64) -> io::Result<Vec<RootHistorySample>> {
        let db_path = self.db_path.clone();
        tokio::task::spawn_blocking(move || {
            with_db(&db_path, |conn| root_stats_history_since_db(conn, since))
        })
        .await
        .map_err(join_error)?
    }

    pub async fn prune_root_stats_samples_before(&self, before: u64) -> io::Result<()> {
        let _write_guard = self.db_write_lock.lock().await;
        let db_path = self.db_path.clone();
        tokio::task::spawn_blocking(move || {
            with_db(&db_path, |conn| {
                conn.execute(
                    "DELETE FROM root_stats_samples WHERE sampled_at < ?1",
                    params![before],
                )
                .map_err(sqlite_error)?;
                Ok(())
            })
        })
        .await
        .map_err(join_error)?
    }

    pub async fn load_project(&self, project: &str) -> io::Result<Option<ProjectCache>> {
        let db_path = self.db_path.clone();
        let project = normalize_project_name(project);
        tokio::task::spawn_blocking(move || load_project_db(&db_path, &project))
            .await
            .map_err(join_error)?
    }

    pub async fn store_project(&self, record: &ProjectRecord) -> io::Result<ProjectCache> {
        let _write_guard = self.db_write_lock.lock().await;
        let db_path = self.db_path.clone();
        let record = record.clone();
        tokio::task::spawn_blocking(move || store_project_db(&db_path, &record))
            .await
            .map_err(join_error)?
    }

    pub async fn load_project_responses(
        &self,
        project: &str,
    ) -> io::Result<Option<CachedProjectResponses>> {
        let db_path = self.db_path.clone();
        let project = normalize_project_name(project);
        tokio::task::spawn_blocking(move || load_project_responses_db(&db_path, &project))
            .await
            .map_err(join_error)?
    }

    pub async fn load_project_response_validator(
        &self,
        project: &str,
        format: ProjectResponseFormat,
    ) -> io::Result<Option<ProjectResponseValidator>> {
        let db_path = self.db_path.clone();
        let project = normalize_project_name(project);
        tokio::task::spawn_blocking(move || {
            load_project_response_validator_db(&db_path, &project, format)
        })
        .await
        .map_err(join_error)?
    }

    pub async fn store_project_responses(&self, record: &ProjectResponseRecord) -> io::Result<()> {
        let record = record.clone();
        let record = tokio::task::spawn_blocking(move || compress_project_responses(&record))
            .await
            .map_err(join_error)??;
        let _write_guard = self.db_write_lock.lock().await;
        let db_path = self.db_path.clone();
        tokio::task::spawn_blocking(move || store_project_responses_db(&db_path, &record))
            .await
            .map_err(join_error)?
    }

    pub async fn touch_project(
        &self,
        project: &str,
        fetched_at: u64,
        expires_at: u64,
    ) -> io::Result<()> {
        let _write_guard = self.db_write_lock.lock().await;
        let db_path = self.db_path.clone();
        let project = normalize_project_name(project);
        tokio::task::spawn_blocking(move || {
            with_db(&db_path, |conn| {
                conn.execute(
                    "UPDATE projects SET fetched_at = ?2, expires_at = ?3 WHERE project = ?1",
                    params![project, fetched_at, expires_at],
                )
                .map_err(sqlite_error)?;
                Ok(())
            })
        })
        .await
        .map_err(join_error)?
    }

    pub async fn blob_status(&self, blob_kind: &str, blob_id: &str) -> io::Result<BlobStatus> {
        let db_path = self.db_path.clone();
        let blob_kind = blob_kind.to_string();
        let blob_id = blob_id.to_string();
        tokio::task::spawn_blocking(move || blob_status_db(&db_path, &blob_kind, &blob_id))
            .await
            .map_err(join_error)?
    }

    pub async fn find_link_by_blob(
        &self,
        route_a: &str,
        route_b: &str,
        filename: &str,
    ) -> io::Result<Option<CachedLink>> {
        let db_path = self.db_path.clone();
        let route_a = route_a.to_string();
        let route_b = route_b.to_string();
        let filename = filename.to_string();
        tokio::task::spawn_blocking(move || {
            with_db(&db_path, |conn| {
                if route_a == "_url" {
                    return query_link_by_blob(
                        conn,
                        "SELECT filename, upstream_url, blob_kind, blob_id, requires_python, yanked, gpg_sig,
                            dist_info_metadata, core_metadata, hash_name, hash_value
                     FROM project_links
                     WHERE blob_kind = 'url' AND blob_id = ?1 AND filename = ?2
                     LIMIT 1",
                        params![route_b, filename],
                    );
                }

                let prefix = format!("{route_a}{route_b}");
                let Some(prefix_end) = next_lexicographic_prefix(&prefix) else {
                    return Ok(None);
                };
                query_link_by_blob(
                    conn,
                    "SELECT filename, upstream_url, blob_kind, blob_id, requires_python, yanked, gpg_sig,
                        dist_info_metadata, core_metadata, hash_name, hash_value
                 FROM project_links
                 WHERE blob_kind = 'sha256'
                   AND blob_id >= ?1
                   AND blob_id < ?2
                   AND filename = ?3
                 LIMIT 1",
                    params![prefix, prefix_end, filename],
                )
            })
        })
        .await
        .map_err(join_error)?
    }

    pub async fn touch_blob_access(
        &self,
        blob_kind: &str,
        blob_id: &str,
        accessed_at: u64,
    ) -> io::Result<()> {
        let _write_guard = self.db_write_lock.lock().await;
        let db_path = self.db_path.clone();
        let blob_kind = blob_kind.to_string();
        let blob_id = blob_id.to_string();
        tokio::task::spawn_blocking(move || {
            with_db(&db_path, |conn| {
                conn.execute(
                    "UPDATE blobs
                     SET last_accessed_at = ?3
                     WHERE blob_kind = ?1
                       AND blob_id = ?2
                       AND state = 'ready'
                       AND last_accessed_at < ?3",
                    params![blob_kind, blob_id, accessed_at],
                )
                .map_err(sqlite_error)?;
                Ok(())
            })
        })
        .await
        .map_err(join_error)?
    }

    pub async fn mark_blob_pending(
        &self,
        blob_kind: &str,
        blob_id: &str,
        filename: &str,
        upstream_url: &str,
        storage_relpath: &str,
    ) -> io::Result<bool> {
        let _write_guard = self.db_write_lock.lock().await;
        let db_path = self.db_path.clone();
        let blob_kind = blob_kind.to_string();
        let blob_id = blob_id.to_string();
        let filename = filename.to_string();
        let upstream_url = upstream_url.to_string();
        let storage_relpath = storage_relpath.to_string();
        tokio::task::spawn_blocking(move || {
            with_db(&db_path, |conn| {
                let inserted = conn
                    .execute(
                        "INSERT OR IGNORE INTO blobs (
                    blob_kind, blob_id, storage_relpath, content_type, fetched_at,
                    last_accessed_at, size_bytes, filename, upstream_url, state
                 ) VALUES (?1, ?2, ?3, '', 0, 0, 0, ?4, ?5, ?6)",
                        params![
                            blob_kind,
                            blob_id,
                            storage_relpath,
                            filename,
                            upstream_url,
                            BLOB_STATE_PENDING
                        ],
                    )
                    .map_err(sqlite_error)?;
                Ok(inserted == 1)
            })
        })
        .await
        .map_err(join_error)?
    }

    pub async fn store_blob_file(&self, storage_relpath: &str, bytes: &[u8]) -> io::Result<()> {
        let path = self.root.join(storage_relpath);
        write_atomic(&path, bytes).await
    }

    pub fn blob_path(&self, storage_relpath: &str) -> PathBuf {
        self.root.join(storage_relpath)
    }

    pub fn blob_temp_path(&self, storage_relpath: &str) -> PathBuf {
        self.root.join(storage_relpath).with_extension("tmp")
    }

    pub async fn blob_temp_len(&self, storage_relpath: &str) -> io::Result<u64> {
        match fs::metadata(self.blob_temp_path(storage_relpath)).await {
            Ok(metadata) => Ok(metadata.len()),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(0),
            Err(err) => Err(err),
        }
    }

    pub async fn prepare_blob_parent(&self, storage_relpath: &str) -> io::Result<()> {
        let path = self.blob_path(storage_relpath);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        Ok(())
    }

    pub async fn commit_blob_file(
        &self,
        temp_path: &Path,
        storage_relpath: &str,
    ) -> io::Result<()> {
        fs::rename(temp_path, self.blob_path(storage_relpath)).await
    }

    pub async fn load_blob_bytes(&self, storage_relpath: &str) -> io::Result<Option<Vec<u8>>> {
        let path = self.root.join(storage_relpath);
        match fs::read(path).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub async fn mark_blob_ready(&self, blob: &BlobWrite) -> io::Result<BlobReadyResult> {
        let _write_guard = self.db_write_lock.lock().await;
        let db_path = self.db_path.clone();
        let blob = blob.clone();
        tokio::task::spawn_blocking(move || {
            with_db(&db_path, |conn| {
                let tx = conn.transaction().map_err(sqlite_error)?;
                tx.execute(
                    "UPDATE blobs
                 SET storage_relpath = ?3,
                     content_type = ?4,
                     fetched_at = ?5,
                     last_accessed_at = ?5,
                     size_bytes = ?6,
                     filename = ?7,
                     upstream_url = ?8,
                     state = ?9
                 WHERE blob_kind = ?1 AND blob_id = ?2",
                    params![
                        blob.blob_kind,
                        blob.blob_id,
                        blob.storage_relpath,
                        blob.content_type,
                        blob.fetched_at,
                        blob.size_bytes,
                        blob.filename,
                        blob.upstream_url,
                        BLOB_STATE_READY,
                    ],
                )
                .map_err(sqlite_error)?;
                let projects = {
                    let mut stmt = tx
                        .prepare(
                            "SELECT DISTINCT project
                             FROM project_links
                             WHERE blob_kind = ?1 AND blob_id = ?2",
                        )
                        .map_err(sqlite_error)?;
                    let rows = stmt
                        .query_map(params![blob.blob_kind, blob.blob_id], |row| {
                            row.get::<_, String>(0)
                        })
                        .map_err(sqlite_error)?;
                    let mut projects = Vec::new();
                    for row in rows {
                        projects.push(row.map_err(sqlite_error)?);
                    }
                    projects
                };
                for project in &projects {
                    tx.execute(
                        "DELETE FROM project_responses WHERE project = ?1",
                        params![project],
                    )
                    .map_err(sqlite_error)?;
                    update_project_stats_tx(&tx, project)?;
                }
                tx.commit().map_err(sqlite_error)?;
                Ok(BlobReadyResult {
                    blob: BlobInfo {
                        blob_kind: blob.blob_kind,
                        blob_id: blob.blob_id,
                        storage_relpath: blob.storage_relpath,
                        content_type: blob.content_type,
                        fetched_at: blob.fetched_at,
                        size_bytes: blob.size_bytes,
                        filename: blob.filename,
                        upstream_url: blob.upstream_url,
                        state: BLOB_STATE_READY.to_string(),
                    },
                    projects,
                })
            })
        })
        .await
        .map_err(join_error)?
    }

    pub async fn enforce_cache_size(&self, max_size_bytes: u64) -> io::Result<Vec<EvictedBlob>> {
        if max_size_bytes == 0 {
            return Ok(Vec::new());
        }

        let evicted = {
            let _write_guard = self.db_write_lock.lock().await;
            let db_path = self.db_path.clone();
            tokio::task::spawn_blocking(move || {
                with_db(&db_path, |conn| {
                    prune_cache_to_size_db(conn, max_size_bytes)
                })
            })
            .await
            .map_err(join_error)??
        };

        for blob in &evicted {
            match fs::remove_file(self.blob_path(&blob.storage_relpath)).await {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(err),
            }
            prune_empty_parent_dirs(&self.root, &blob.storage_relpath).await?;
        }

        Ok(evicted)
    }

    pub fn project_is_fresh(&self, project: &ProjectCache) -> bool {
        project.expires_at > current_unix_secs()
    }

    pub fn blob_storage_relpath(
        &self,
        blob_kind: &str,
        blob_id: &str,
        filename: &str,
    ) -> io::Result<String> {
        let safe_filename = sanitize_filename(filename)?;
        if blob_kind == "sha256" {
            if blob_id.len() < 16 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("sha256 blob id too short: {blob_id}"),
                ));
            }
            return Ok(format!(
                "+files/root/pypi/+f/{}/{}/{}",
                &blob_id[..3],
                &blob_id[3..16],
                safe_filename
            ));
        }
        Ok(format!(
            "+files/root/pypi/+f/_url/{}/{}",
            blob_id, safe_filename
        ))
    }

    fn files_root(&self) -> PathBuf {
        self.root.join("+files")
    }
}

pub fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn project_summary_link(project: &str, upstream_project_url: &str) -> (String, String) {
    let Some(rest) = project.strip_prefix("pytorch-wheels-") else {
        return (project.to_string(), format!("{project}/"));
    };
    let upstream_project = upstream_project_url
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .filter(|value| !value.is_empty())
        .unwrap_or(rest);
    if rest == upstream_project {
        return (
            upstream_project.to_string(),
            format!("/pytorch-wheels/{upstream_project}/"),
        );
    }
    if let Some(channel) = rest.strip_suffix(&format!("-{upstream_project}"))
        && !channel.is_empty()
    {
        return (
            upstream_project.to_string(),
            format!("/pytorch-wheels/{channel}/{upstream_project}/"),
        );
    }
    (project.to_string(), format!("{project}/"))
}

fn list_projects_db(conn: &mut Connection) -> io::Result<Vec<String>> {
    let mut stmt = conn
        .prepare_cached("SELECT project FROM projects ORDER BY project")
        .map_err(sqlite_error)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(sqlite_error)?;
    let mut projects = Vec::new();
    for row in rows {
        projects.push(row.map_err(sqlite_error)?);
    }
    Ok(projects)
}

fn list_project_summaries_db(conn: &mut Connection) -> io::Result<Vec<ProjectSummary>> {
    let mut stmt = conn
        .prepare_cached(
            "SELECT
                 p.project,
                 p.upstream_project_url,
                 COALESCE(ps.file_count, 0) AS file_count,
                 COALESCE(ps.cached_file_count, 0) AS cached_file_count,
                 COALESCE(ps.cached_size_bytes, 0) AS cached_size_bytes
             FROM projects p
             LEFT JOIN project_stats ps ON ps.project = p.project
             ORDER BY p.project",
        )
        .map_err(sqlite_error)?;
    let rows = stmt
        .query_map([], |row| {
            let project = row.get::<_, String>(0)?;
            let upstream_project_url = row.get::<_, String>(1)?;
            let (display_name, page_url) = project_summary_link(&project, &upstream_project_url);
            Ok(ProjectSummary {
                project,
                display_name,
                page_url,
                file_count: row.get(2)?,
                cached_file_count: row.get(3)?,
                cached_size_bytes: row.get(4)?,
            })
        })
        .map_err(sqlite_error)?;
    let mut projects = Vec::new();
    for row in rows {
        projects.push(row.map_err(sqlite_error)?);
    }
    Ok(projects)
}

fn cache_summary_db(conn: &mut Connection) -> io::Result<CacheSummary> {
    let mut stmt = conn
        .prepare_cached(
            "SELECT
                (SELECT COUNT(*) FROM projects),
                COUNT(*),
                COALESCE(SUM(size_bytes), 0)
             FROM blobs
             WHERE state = 'ready'",
        )
        .map_err(sqlite_error)?;
    stmt.query_row([], |row| {
        Ok(CacheSummary {
            project_count: row.get(0)?,
            ready_blob_count: row.get(1)?,
            cached_size_bytes: row.get(2)?,
        })
    })
    .map_err(sqlite_error)
}

fn root_stats_history_since_db(
    conn: &mut Connection,
    since: u64,
) -> io::Result<Vec<RootHistorySample>> {
    let mut stmt = conn
        .prepare_cached(
            "SELECT sampled_at, cached_size_bytes, package_count, hit_rate_percent
             FROM root_stats_samples
             WHERE sampled_at >= ?1
             ORDER BY sampled_at",
        )
        .map_err(sqlite_error)?;
    let rows = stmt
        .query_map(params![since], |row| {
            Ok(RootHistorySample {
                sampled_at: row.get(0)?,
                cached_size_bytes: row.get(1)?,
                package_count: row.get(2)?,
                hit_rate_percent: row.get(3)?,
            })
        })
        .map_err(sqlite_error)?;
    let mut samples = Vec::new();
    for row in rows {
        samples.push(row.map_err(sqlite_error)?);
    }
    Ok(samples)
}

pub fn fallback_blob_identity(upstream_url: &str) -> (String, String) {
    ("url".to_string(), sha256_hex(upstream_url.as_bytes()))
}

pub fn hash_blob_identity(
    hash_name: &str,
    hash_value: &str,
    upstream_url: &str,
) -> (String, String) {
    if hash_name.eq_ignore_ascii_case("sha256") && !hash_value.is_empty() {
        ("sha256".to_string(), hash_value.to_string())
    } else {
        fallback_blob_identity(upstream_url)
    }
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest = sha2::Sha256::digest(bytes);
    hex_encode(digest.as_slice())
}

fn init_db(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut conn = open_db(path)?;
    conn.execute_batch(
        "
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;
        PRAGMA busy_timeout=5000;
        PRAGMA temp_store=MEMORY;

        CREATE TABLE IF NOT EXISTS projects (
            project TEXT PRIMARY KEY,
            fetched_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL,
            upstream_etag TEXT,
            upstream_serial INTEGER,
            upstream_project_url TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS project_links (
            project TEXT NOT NULL,
            position INTEGER NOT NULL,
            filename TEXT NOT NULL,
            upstream_url TEXT NOT NULL,
            blob_kind TEXT NOT NULL,
            blob_id TEXT NOT NULL,
            requires_python TEXT,
            yanked TEXT,
            gpg_sig INTEGER,
            dist_info_metadata TEXT,
            core_metadata TEXT,
            hash_name TEXT,
            hash_value TEXT,
            PRIMARY KEY (project, position)
        );

        CREATE TABLE IF NOT EXISTS blobs (
            blob_kind TEXT NOT NULL,
            blob_id TEXT NOT NULL,
            storage_relpath TEXT NOT NULL,
            content_type TEXT NOT NULL,
            fetched_at INTEGER NOT NULL,
            last_accessed_at INTEGER NOT NULL,
            size_bytes INTEGER NOT NULL,
            filename TEXT NOT NULL,
            upstream_url TEXT NOT NULL,
            state TEXT NOT NULL,
            PRIMARY KEY (blob_kind, blob_id),
            UNIQUE (storage_relpath)
        );

        CREATE INDEX IF NOT EXISTS idx_blobs_state_size
            ON blobs (state, size_bytes);

        CREATE INDEX IF NOT EXISTS idx_blobs_state_fetched_at
            ON blobs (state, fetched_at);

        CREATE TABLE IF NOT EXISTS project_responses (
            project TEXT PRIMARY KEY,
            html_body BLOB NOT NULL,
            html_size INTEGER NOT NULL,
            html_etag TEXT NOT NULL,
            json_body BLOB NOT NULL,
            json_size INTEGER NOT NULL,
            json_etag TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS project_stats (
            project TEXT PRIMARY KEY,
            file_count INTEGER NOT NULL,
            cached_file_count INTEGER NOT NULL,
            cached_size_bytes INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS root_stats_samples (
            sampled_at INTEGER PRIMARY KEY,
            cached_size_bytes INTEGER NOT NULL,
            package_count INTEGER NOT NULL,
            hit_rate_percent REAL NOT NULL
        );
        ",
    )
    .map_err(sqlite_error)?;
    let project_metadata_changed = migrate_project_metadata_schema(&mut conn)?;
    let project_response_schema_changed = migrate_project_response_schema(&mut conn)?;
    migrate_blob_access_times(&conn)?;
    backfill_project_stats(&conn)?;
    finish_project_metadata_migration(
        &conn,
        path,
        project_metadata_changed || project_response_schema_changed,
    )?;
    Ok(())
}

fn migrate_project_response_schema(conn: &mut Connection) -> io::Result<bool> {
    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .map_err(sqlite_error)?;
    if table_has_column(&tx, "project_responses", "html_size")? {
        tx.commit().map_err(sqlite_error)?;
        return Ok(false);
    }
    tx.execute_batch(
        "
        DROP TABLE project_responses;
        CREATE TABLE project_responses (
            project TEXT PRIMARY KEY,
            html_body BLOB NOT NULL,
            html_size INTEGER NOT NULL,
            html_etag TEXT NOT NULL,
            json_body BLOB NOT NULL,
            json_size INTEGER NOT NULL,
            json_etag TEXT NOT NULL
        );
        ",
    )
    .map_err(sqlite_error)?;
    tx.commit().map_err(sqlite_error)?;
    Ok(true)
}

fn migrate_project_metadata_schema(conn: &mut Connection) -> io::Result<bool> {
    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .map_err(sqlite_error)?;
    let migrate_projects = table_has_column(&tx, "projects", "raw_body")?;
    let migrate_project_links = table_has_unique_index_columns(
        &tx,
        "project_links",
        &["project", "filename", "upstream_url"],
    )?;
    let drop_redundant_order_index = index_exists(&tx, "idx_project_links_project_order")?;
    let changed = migrate_projects || migrate_project_links || drop_redundant_order_index;

    if migrate_projects {
        tx.execute_batch(
            "
            DROP TABLE IF EXISTS projects_migrated;
            CREATE TABLE projects_migrated (
                project TEXT PRIMARY KEY,
                fetched_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                upstream_etag TEXT,
                upstream_serial INTEGER,
                upstream_project_url TEXT NOT NULL
            );
            INSERT INTO projects_migrated (
                project, fetched_at, expires_at, upstream_etag, upstream_serial,
                upstream_project_url
            )
            SELECT
                project, fetched_at, expires_at, upstream_etag, upstream_serial,
                upstream_project_url
            FROM projects;
            DROP TABLE projects;
            ALTER TABLE projects_migrated RENAME TO projects;
            ",
        )
        .map_err(sqlite_error)?;
    }

    if migrate_project_links {
        tx.execute_batch(
            "
            DROP TABLE IF EXISTS project_links_migrated;
            CREATE TABLE project_links_migrated (
                project TEXT NOT NULL,
                position INTEGER NOT NULL,
                filename TEXT NOT NULL,
                upstream_url TEXT NOT NULL,
                blob_kind TEXT NOT NULL,
                blob_id TEXT NOT NULL,
                requires_python TEXT,
                yanked TEXT,
                gpg_sig INTEGER,
                dist_info_metadata TEXT,
                core_metadata TEXT,
                hash_name TEXT,
                hash_value TEXT,
                PRIMARY KEY (project, position)
            );
            INSERT INTO project_links_migrated (
                project, position, filename, upstream_url, blob_kind, blob_id,
                requires_python, yanked, gpg_sig, dist_info_metadata, core_metadata,
                hash_name, hash_value
            )
            SELECT
                project, position, filename, upstream_url, blob_kind, blob_id,
                requires_python, yanked, gpg_sig, dist_info_metadata, core_metadata,
                hash_name, hash_value
            FROM project_links
            ORDER BY project, position;
            DROP TABLE project_links;
            ALTER TABLE project_links_migrated RENAME TO project_links;
            ",
        )
        .map_err(sqlite_error)?;
    } else if drop_redundant_order_index {
        tx.execute("DROP INDEX idx_project_links_project_order", [])
            .map_err(sqlite_error)?;
    }

    tx.execute(
        "CREATE INDEX IF NOT EXISTS idx_project_links_blob_lookup
         ON project_links (blob_kind, blob_id, filename)",
        [],
    )
    .map_err(sqlite_error)?;
    tx.commit().map_err(sqlite_error)?;
    Ok(changed)
}

fn finish_project_metadata_migration(
    conn: &Connection,
    path: &Path,
    schema_changed: bool,
) -> io::Result<()> {
    let schema_version = conn
        .query_row("PRAGMA user_version", [], |row| row.get::<_, u32>(0))
        .map_err(sqlite_error)?;
    let freelist_pages = conn
        .query_row("PRAGMA freelist_count", [], |row| row.get::<_, u64>(0))
        .map_err(sqlite_error)?;
    let should_compact =
        schema_changed || (schema_version < CACHE_SCHEMA_VERSION && freelist_pages > 0);

    if should_compact {
        let before_bytes = std::fs::metadata(path).map_or(0, |metadata| metadata.len());
        let started = Instant::now();
        info!(
            database = %path.display(),
            before_bytes,
            freelist_pages,
            "compacting migrated cache database"
        );
        conn.execute_batch("VACUUM; PRAGMA wal_checkpoint(TRUNCATE);")
            .map_err(sqlite_error)?;
        let after_bytes = std::fs::metadata(path).map_or(0, |metadata| metadata.len());
        info!(
            database = %path.display(),
            before_bytes,
            after_bytes,
            reclaimed_bytes = before_bytes.saturating_sub(after_bytes),
            elapsed_ms = started.elapsed().as_millis(),
            "compacted migrated cache database"
        );
    }

    if schema_version < CACHE_SCHEMA_VERSION {
        conn.execute_batch(&format!("PRAGMA user_version = {CACHE_SCHEMA_VERSION};"))
            .map_err(sqlite_error)?;
    }
    Ok(())
}

fn migrate_blob_access_times(conn: &Connection) -> io::Result<()> {
    if !table_has_column(conn, "blobs", "last_accessed_at")? {
        conn.execute(
            "ALTER TABLE blobs
             ADD COLUMN last_accessed_at INTEGER NOT NULL DEFAULT 0",
            [],
        )
        .map_err(sqlite_error)?;
    }
    conn.execute(
        "UPDATE blobs
         SET last_accessed_at = fetched_at
         WHERE state = 'ready' AND last_accessed_at = 0",
        [],
    )
    .map_err(sqlite_error)?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_blobs_state_last_accessed_at
         ON blobs (state, last_accessed_at)",
        [],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

fn table_has_column(conn: &Connection, table: &str, column: &str) -> io::Result<bool> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info({table})"))
        .map_err(sqlite_error)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(sqlite_error)?;
    for row in rows {
        if row.map_err(sqlite_error)? == column {
            return Ok(true);
        }
    }
    Ok(false)
}

fn table_has_unique_index_columns(
    conn: &Connection,
    table: &str,
    expected_columns: &[&str],
) -> io::Result<bool> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA index_list({table})"))
        .map_err(sqlite_error)?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, bool>(2)?))
        })
        .map_err(sqlite_error)?;
    let mut unique_indexes = Vec::new();
    for row in rows {
        let (name, unique) = row.map_err(sqlite_error)?;
        if unique {
            unique_indexes.push(name);
        }
    }
    drop(stmt);

    for index in unique_indexes {
        let quoted_index = index.replace('"', "\"\"");
        let mut stmt = conn
            .prepare(&format!("PRAGMA index_info(\"{quoted_index}\")"))
            .map_err(sqlite_error)?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(2))
            .map_err(sqlite_error)?;
        let mut columns = Vec::new();
        for row in rows {
            columns.push(row.map_err(sqlite_error)?);
        }
        if columns
            .iter()
            .map(String::as_str)
            .eq(expected_columns.iter().copied())
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn index_exists(conn: &Connection, index: &str) -> io::Result<bool> {
    conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM sqlite_schema WHERE type = 'index' AND name = ?1
         )",
        params![index],
        |row| row.get(0),
    )
    .map_err(sqlite_error)
}

fn prune_cache_to_size_db(
    conn: &mut Connection,
    max_size_bytes: u64,
) -> io::Result<Vec<EvictedBlob>> {
    let total_size = cache_summary_db(conn)?.cached_size_bytes;
    if total_size <= max_size_bytes {
        return Ok(Vec::new());
    }

    let candidates = {
        let mut stmt = conn
            .prepare_cached(
                "SELECT blob_kind, blob_id, storage_relpath, size_bytes
                 FROM blobs
                 WHERE state = 'ready'
                 ORDER BY last_accessed_at ASC, fetched_at ASC, blob_kind ASC, blob_id ASC",
            )
            .map_err(sqlite_error)?;
        let rows = stmt
            .query_map([], |row| {
                Ok(EvictedBlob {
                    blob_kind: row.get(0)?,
                    blob_id: row.get(1)?,
                    storage_relpath: row.get(2)?,
                    size_bytes: row.get(3)?,
                    projects: Vec::new(),
                })
            })
            .map_err(sqlite_error)?;
        let mut candidates = Vec::new();
        for row in rows {
            candidates.push(row.map_err(sqlite_error)?);
        }
        candidates
    };

    let tx = conn.transaction().map_err(sqlite_error)?;
    let mut remaining_size = total_size;
    let mut evicted = Vec::new();
    let mut changed_projects = HashSet::new();

    for mut candidate in candidates {
        if remaining_size <= max_size_bytes {
            break;
        }

        candidate.projects = {
            let mut stmt = tx
                .prepare(
                    "SELECT DISTINCT project
                     FROM project_links
                     WHERE blob_kind = ?1 AND blob_id = ?2",
                )
                .map_err(sqlite_error)?;
            let rows = stmt
                .query_map(params![candidate.blob_kind, candidate.blob_id], |row| {
                    row.get::<_, String>(0)
                })
                .map_err(sqlite_error)?;
            let mut projects = Vec::new();
            for row in rows {
                projects.push(row.map_err(sqlite_error)?);
            }
            projects
        };

        tx.execute(
            "DELETE FROM blobs WHERE blob_kind = ?1 AND blob_id = ?2",
            params![candidate.blob_kind, candidate.blob_id],
        )
        .map_err(sqlite_error)?;
        remaining_size = remaining_size.saturating_sub(candidate.size_bytes);
        changed_projects.extend(candidate.projects.iter().cloned());
        evicted.push(candidate);
    }

    for project in changed_projects {
        tx.execute(
            "DELETE FROM project_responses WHERE project = ?1",
            params![project],
        )
        .map_err(sqlite_error)?;
        update_project_stats_tx(&tx, &project)?;
    }
    tx.commit().map_err(sqlite_error)?;
    Ok(evicted)
}

fn open_db(path: &Path) -> io::Result<Connection> {
    let conn = Connection::open(path).map_err(sqlite_error)?;
    conn.busy_timeout(std::time::Duration::from_secs(5))
        .map_err(sqlite_error)?;
    conn.execute_batch(
        "
        PRAGMA synchronous=NORMAL;
        PRAGMA busy_timeout=5000;
        PRAGMA temp_store=MEMORY;
        ",
    )
    .map_err(sqlite_error)?;
    Ok(conn)
}

fn with_db<T>(path: &Path, op: impl FnOnce(&mut Connection) -> io::Result<T>) -> io::Result<T> {
    SQLITE_CONNECTIONS.with(|connections| {
        let mut connections = connections.borrow_mut();
        let key = path.to_path_buf();
        if !connections.contains_key(&key) {
            connections.insert(key.clone(), open_db(path)?);
        }
        let conn = connections
            .get_mut(&key)
            .expect("thread-local sqlite connection was just inserted");
        op(conn)
    })
}

fn backfill_project_stats(conn: &Connection) -> io::Result<()> {
    conn.execute_batch(
        "
        INSERT INTO project_stats (
            project, file_count, cached_file_count, cached_size_bytes
        )
        SELECT
            p.project,
            COALESCE(pfc.file_count, 0) AS file_count,
            COALESCE(rpt.cached_file_count, 0) AS cached_file_count,
            COALESCE(rpt.cached_size_bytes, 0) AS cached_size_bytes
        FROM projects p
        LEFT JOIN (
            SELECT project, COUNT(position) AS file_count
            FROM project_links
            GROUP BY project
        ) pfc ON pfc.project = p.project
        LEFT JOIN (
            SELECT
                project,
                COUNT(*) AS cached_file_count,
                COALESCE(SUM(size_bytes), 0) AS cached_size_bytes
            FROM (
                SELECT
                    pl.project,
                    b.blob_kind,
                    b.blob_id,
                    MAX(b.size_bytes) AS size_bytes
                FROM project_links pl
                JOIN blobs b
                  ON b.blob_kind = pl.blob_kind
                 AND b.blob_id = pl.blob_id
                 AND b.state = 'ready'
                GROUP BY pl.project, b.blob_kind, b.blob_id
            )
            GROUP BY project
        ) rpt ON rpt.project = p.project
        ON CONFLICT(project) DO NOTHING;
        ",
    )
    .map_err(sqlite_error)
}

fn update_project_stats_tx(tx: &rusqlite::Transaction<'_>, project: &str) -> io::Result<()> {
    tx.execute(
        "INSERT INTO project_stats (
            project, file_count, cached_file_count, cached_size_bytes
         )
         SELECT
            ?1,
            (SELECT COUNT(position) FROM project_links WHERE project = ?1),
            COALESCE((
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT pl.blob_kind, pl.blob_id
                    FROM project_links pl
                    JOIN blobs b
                      ON b.blob_kind = pl.blob_kind
                     AND b.blob_id = pl.blob_id
                     AND b.state = 'ready'
                    WHERE pl.project = ?1
                )
            ), 0),
            COALESCE((
                SELECT SUM(size_bytes)
                FROM (
                    SELECT DISTINCT b.blob_kind, b.blob_id, b.size_bytes
                    FROM project_links pl
                    JOIN blobs b
                      ON b.blob_kind = pl.blob_kind
                     AND b.blob_id = pl.blob_id
                     AND b.state = 'ready'
                    WHERE pl.project = ?1
                )
            ), 0)
         ON CONFLICT(project) DO UPDATE SET
            file_count = excluded.file_count,
            cached_file_count = excluded.cached_file_count,
            cached_size_bytes = excluded.cached_size_bytes",
        params![project],
    )
    .map_err(sqlite_error)?;
    Ok(())
}

fn load_project_responses_db(
    path: &Path,
    project: &str,
) -> io::Result<Option<CachedProjectResponses>> {
    with_db(path, |conn| {
        let mut stmt = conn
            .prepare_cached(
                "SELECT
                    p.fetched_at, p.expires_at, p.upstream_etag, p.upstream_serial,
                    p.upstream_project_url, r.html_body, r.html_size, r.html_etag,
                    r.json_body, r.json_size, r.json_etag
                 FROM projects p
                 JOIN project_responses r ON r.project = p.project
                 WHERE p.project = ?1",
            )
            .map_err(sqlite_error)?;
        let row = stmt
            .query_row(params![project], |row| {
                Ok((
                    row.get::<_, u64>(0)?,
                    row.get::<_, u64>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<u64>>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Vec<u8>>(5)?,
                    row.get::<_, u64>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, Vec<u8>>(8)?,
                    row.get::<_, u64>(9)?,
                    row.get::<_, String>(10)?,
                ))
            })
            .optional()
            .map_err(sqlite_error)?;
        let Some((
            fetched_at,
            expires_at,
            upstream_etag,
            upstream_serial,
            upstream_project_url,
            html_body,
            html_size,
            html_etag,
            json_body,
            json_size,
            json_etag,
        )) = row
        else {
            return Ok(None);
        };
        Ok(Some(CachedProjectResponses {
            project: project.to_string(),
            fetched_at,
            expires_at,
            upstream_etag,
            upstream_serial,
            upstream_project_url,
            html_body: Bytes::from(decompress_project_response(&html_body, html_size)?),
            html_etag,
            json_body: Bytes::from(decompress_project_response(&json_body, json_size)?),
            json_etag,
        }))
    })
}

fn load_project_response_validator_db(
    path: &Path,
    project: &str,
    format: ProjectResponseFormat,
) -> io::Result<Option<ProjectResponseValidator>> {
    with_db(path, |conn| {
        let sql = match format {
            ProjectResponseFormat::Html => {
                "SELECT p.expires_at, r.html_etag
                 FROM projects p
                 JOIN project_responses r ON r.project = p.project
                 WHERE p.project = ?1"
            }
            ProjectResponseFormat::Json => {
                "SELECT p.expires_at, r.json_etag
                 FROM projects p
                 JOIN project_responses r ON r.project = p.project
                 WHERE p.project = ?1"
            }
        };
        let mut stmt = conn.prepare_cached(sql).map_err(sqlite_error)?;
        stmt.query_row(params![project], |row| {
            Ok(ProjectResponseValidator {
                expires_at: row.get(0)?,
                etag: row.get(1)?,
            })
        })
        .optional()
        .map_err(sqlite_error)
    })
}

fn compress_project_responses(
    record: &ProjectResponseRecord,
) -> io::Result<CompressedProjectResponseRecord> {
    for (format, body) in [("HTML", &record.html_body), ("JSON", &record.json_body)] {
        if body.len() > MAX_PROJECT_RESPONSE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("materialized project {format} exceeds size limit"),
            ));
        }
    }
    Ok(CompressedProjectResponseRecord {
        project: record.project.clone(),
        html_body: zstd::bulk::compress(
            record.html_body.as_ref(),
            PROJECT_RESPONSE_COMPRESSION_LEVEL,
        )
        .map_err(io::Error::other)?,
        html_size: record.html_body.len(),
        html_etag: record.html_etag.clone(),
        json_body: zstd::bulk::compress(
            record.json_body.as_ref(),
            PROJECT_RESPONSE_COMPRESSION_LEVEL,
        )
        .map_err(io::Error::other)?,
        json_size: record.json_body.len(),
        json_etag: record.json_etag.clone(),
    })
}

fn decompress_project_response(body: &[u8], size: u64) -> io::Result<Vec<u8>> {
    let size = usize::try_from(size)
        .ok()
        .filter(|size| *size <= MAX_PROJECT_RESPONSE_BYTES)
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid project response size")
        })?;
    let decompressed = zstd::bulk::decompress(body, size).map_err(io::Error::other)?;
    if decompressed.len() != size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "materialized project response size mismatch",
        ));
    }
    Ok(decompressed)
}

fn store_project_responses_db(
    path: &Path,
    record: &CompressedProjectResponseRecord,
) -> io::Result<()> {
    with_db(path, |conn| {
        conn.execute(
            "INSERT INTO project_responses (
                project, html_body, html_size, html_etag, json_body, json_size, json_etag
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(project) DO UPDATE SET
                html_body = excluded.html_body,
                html_size = excluded.html_size,
                html_etag = excluded.html_etag,
                json_body = excluded.json_body,
                json_size = excluded.json_size,
                json_etag = excluded.json_etag",
            params![
                record.project,
                record.html_body,
                record.html_size,
                record.html_etag,
                record.json_body,
                record.json_size,
                record.json_etag,
            ],
        )
        .map_err(sqlite_error)?;
        Ok(())
    })
}

fn load_project_db(path: &Path, project: &str) -> io::Result<Option<ProjectCache>> {
    with_db(path, |conn| {
        let mut project_stmt = conn
            .prepare_cached(
            "SELECT fetched_at, expires_at, upstream_etag, upstream_serial, upstream_project_url
             FROM projects WHERE project = ?1",
            )
            .map_err(sqlite_error)?;
        let row = project_stmt
            .query_row(params![project], |row| {
                Ok((
                    row.get::<_, u64>(0)?,
                    row.get::<_, u64>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<u64>>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })
            .optional()
            .map_err(sqlite_error)?;
        let Some((fetched_at, expires_at, upstream_etag, upstream_serial, upstream_project_url)) =
            row
        else {
            return Ok(None);
        };

        let mut stmt = conn
            .prepare_cached(
                "SELECT pl.filename, pl.upstream_url, pl.blob_kind, pl.blob_id,
                    (
                        SELECT b.size_bytes
                        FROM blobs b
                        WHERE b.blob_kind = pl.blob_kind
                          AND b.blob_id = pl.blob_id
                          AND b.state = 'ready'
                    ) AS cached_size_bytes,
                    pl.requires_python, pl.yanked, pl.gpg_sig,
                    pl.dist_info_metadata, pl.core_metadata, pl.hash_name, pl.hash_value
             FROM project_links pl
             WHERE pl.project = ?1
             ORDER BY pl.position",
            )
            .map_err(sqlite_error)?;
        let rows = stmt
            .query_map(params![project], |row| {
                Ok(CachedLink {
                    filename: row.get(0)?,
                    upstream_url: row.get(1)?,
                    blob_kind: row.get(2)?,
                    blob_id: row.get(3)?,
                    cached_size_bytes: row.get(4)?,
                    requires_python: row.get(5)?,
                    yanked: row.get(6)?,
                    gpg_sig: row.get::<_, Option<i64>>(7)?.map(|value| value != 0),
                    dist_info_metadata: row.get(8)?,
                    core_metadata: row.get(9)?,
                    hash_name: row.get(10)?,
                    hash_value: row.get(11)?,
                })
            })
            .map_err(sqlite_error)?;
        let mut links = Vec::new();
        for row in rows {
            links.push(row.map_err(sqlite_error)?);
        }
        Ok(Some(ProjectCache {
            project: project.to_string(),
            fetched_at,
            expires_at,
            upstream_etag,
            upstream_serial,
            upstream_project_url,
            links,
        }))
    })
}

fn store_project_db(path: &Path, record: &ProjectRecord) -> io::Result<ProjectCache> {
    with_db(path, |conn| {
        let tx = conn.transaction().map_err(sqlite_error)?;
        tx.execute(
            "INSERT INTO projects (
            project, fetched_at, expires_at, upstream_etag, upstream_serial, upstream_project_url
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(project) DO UPDATE SET
            fetched_at = excluded.fetched_at,
            expires_at = excluded.expires_at,
            upstream_etag = excluded.upstream_etag,
            upstream_serial = excluded.upstream_serial,
            upstream_project_url = excluded.upstream_project_url",
            params![
                record.project,
                record.fetched_at,
                record.expires_at,
                record.upstream_etag,
                record.upstream_serial,
                record.upstream_project_url,
            ],
        )
        .map_err(sqlite_error)?;
        tx.execute(
            "DELETE FROM project_responses WHERE project = ?1",
            params![record.project],
        )
        .map_err(sqlite_error)?;
        tx.execute(
            "DELETE FROM project_links WHERE project = ?1",
            params![record.project],
        )
        .map_err(sqlite_error)?;
        let stored_links = {
            let mut insert_link = tx
                .prepare(
                "INSERT INTO project_links (
                project, position, filename, upstream_url, blob_kind, blob_id,
                requires_python, yanked, gpg_sig, dist_info_metadata, core_metadata, hash_name, hash_value
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            )
            .map_err(sqlite_error)?;
            let mut seen = HashSet::with_capacity(record.links.len());
            let mut stored_links = Vec::with_capacity(record.links.len());
            for link in &record.links {
                if !seen.insert((link.filename.as_str(), link.upstream_url.as_str())) {
                    continue;
                }
                let position = stored_links.len();
                insert_link
                    .execute(params![
                        record.project,
                        position as i64,
                        link.filename,
                        link.upstream_url,
                        link.blob_kind,
                        link.blob_id,
                        link.requires_python,
                        link.yanked,
                        link.gpg_sig.map(|value| if value { 1 } else { 0 }),
                        link.dist_info_metadata,
                        link.core_metadata,
                        link.hash_name,
                        link.hash_value,
                    ])
                    .map_err(sqlite_error)?;
                stored_links.push(link.clone());
            }
            stored_links
        };
        update_project_stats_tx(&tx, &record.project)?;
        tx.commit().map_err(sqlite_error)?;
        Ok(ProjectCache {
            project: record.project.clone(),
            fetched_at: record.fetched_at,
            expires_at: record.expires_at,
            upstream_etag: record.upstream_etag.clone(),
            upstream_serial: record.upstream_serial,
            upstream_project_url: record.upstream_project_url.clone(),
            links: stored_links,
        })
    })
}

fn blob_status_db(path: &Path, blob_kind: &str, blob_id: &str) -> io::Result<BlobStatus> {
    with_db(path, |conn| {
        let mut stmt = conn
            .prepare_cached(
                "SELECT storage_relpath, content_type, fetched_at, size_bytes, filename, upstream_url, state
             FROM blobs
             WHERE blob_kind = ?1 AND blob_id = ?2",
            )
            .map_err(sqlite_error)?;
        let row = stmt
            .query_row(params![blob_kind, blob_id], |row| {
                Ok(BlobInfo {
                    blob_kind: blob_kind.to_string(),
                    blob_id: blob_id.to_string(),
                    storage_relpath: row.get(0)?,
                    content_type: row.get(1)?,
                    fetched_at: row.get(2)?,
                    size_bytes: row.get(3)?,
                    filename: row.get(4)?,
                    upstream_url: row.get(5)?,
                    state: row.get(6)?,
                })
            })
            .optional()
            .map_err(sqlite_error)?;
        let Some(blob) = row else {
            return Ok(BlobStatus::Missing);
        };
        if blob.state == BLOB_STATE_READY {
            Ok(BlobStatus::Ready(blob))
        } else if blob.state == BLOB_STATE_PENDING {
            Ok(BlobStatus::Pending)
        } else {
            Ok(BlobStatus::Missing)
        }
    })
}

fn query_link_by_blob<P>(
    conn: &mut Connection,
    sql: &str,
    params: P,
) -> io::Result<Option<CachedLink>>
where
    P: rusqlite::Params,
{
    let mut stmt = conn.prepare_cached(sql).map_err(sqlite_error)?;
    stmt.query_row(params, |row| {
        Ok(CachedLink {
            filename: row.get(0)?,
            upstream_url: row.get(1)?,
            blob_kind: row.get(2)?,
            blob_id: row.get(3)?,
            cached_size_bytes: None,
            requires_python: row.get(4)?,
            yanked: row.get(5)?,
            gpg_sig: row.get::<_, Option<i64>>(6)?.map(|value| value != 0),
            dist_info_metadata: row.get(7)?,
            core_metadata: row.get(8)?,
            hash_name: row.get(9)?,
            hash_value: row.get(10)?,
        })
    })
    .optional()
    .map_err(sqlite_error)
}

fn next_lexicographic_prefix(prefix: &str) -> Option<String> {
    let mut bytes = prefix.as_bytes().to_vec();
    for index in (0..bytes.len()).rev() {
        if bytes[index] != u8::MAX {
            bytes[index] += 1;
            bytes.truncate(index + 1);
            return String::from_utf8(bytes).ok();
        }
    }
    None
}

fn sanitize_filename(filename: &str) -> io::Result<String> {
    if filename.is_empty() || filename.contains('/') || filename.contains('\\') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid filename {filename}"),
        ));
    }
    Ok(filename.to_string())
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut value = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        value.push(hex_digit(byte >> 4));
        value.push(hex_digit(byte & 0x0f));
    }
    value
}

fn hex_digit(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'a' + (value - 10)) as char,
        _ => '0',
    }
}

fn sqlite_error(err: rusqlite::Error) -> io::Error {
    io::Error::other(err.to_string())
}

fn join_error(err: tokio::task::JoinError) -> io::Error {
    io::Error::other(err.to_string())
}

async fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    let temp_path = path.with_extension("tmp");
    fs::write(&temp_path, bytes).await?;
    fs::rename(temp_path, path).await
}

async fn prune_empty_parent_dirs(root: &Path, relpath: &str) -> io::Result<()> {
    let files_root = root.join("+files");
    let mut current = root.join(relpath).parent().map(Path::to_path_buf);
    while let Some(dir) = current {
        if dir == files_root || !dir.starts_with(&files_root) {
            break;
        }
        match fs::remove_dir(&dir).await {
            Ok(()) => current = dir.parent().map(Path::to_path_buf),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                current = dir.parent().map(Path::to_path_buf);
            }
            Err(err) if err.kind() == io::ErrorKind::DirectoryNotEmpty => break,
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn stores_and_loads_project_from_sqlite() {
        let temp = tempdir().unwrap();
        let cache = CacheStore::new(temp.path().to_path_buf());
        cache.initialize().await.unwrap();
        let mut record = ProjectRecord {
            project: "requests".to_string(),
            fetched_at: 1,
            expires_at: 10,
            upstream_etag: Some("abc".to_string()),
            upstream_serial: Some(42),
            upstream_project_url: "https://example/simple/requests/".to_string(),
            links: vec![CachedLink {
                filename: "requests-1.0.whl".to_string(),
                upstream_url: "https://files.example/requests-1.0.whl".to_string(),
                blob_kind: "sha256".to_string(),
                blob_id: "deadbeef".to_string(),
                cached_size_bytes: None,
                requires_python: None,
                yanked: None,
                gpg_sig: None,
                dist_info_metadata: None,
                core_metadata: None,
                hash_name: Some("sha256".to_string()),
                hash_value: Some("deadbeef".to_string()),
            }],
        };
        let mut duplicate = record.links[0].clone();
        duplicate.blob_id = "ignored-duplicate".to_string();
        duplicate.hash_value = Some("ignored-duplicate".to_string());
        record.links.push(duplicate);

        let stored = cache.store_project(&record).await.unwrap();
        assert_eq!(stored.links.len(), 1);
        let loaded = cache.load_project("Requests").await.unwrap().unwrap();
        assert_eq!(loaded.project, "requests");
        assert_eq!(loaded.upstream_serial, Some(42));
        assert_eq!(loaded.links.len(), 1);
        assert_eq!(loaded.links[0].blob_id, "deadbeef");
        assert_eq!(loaded.links[0].cached_size_bytes, None);
        cache
            .store_project_responses(&ProjectResponseRecord {
                project: "requests".to_string(),
                html_body: Bytes::from_static(b"<html>requests</html>"),
                html_etag: "html-etag".to_string(),
                json_body: Bytes::from_static(br#"{"name":"requests"}"#),
                json_etag: "json-etag".to_string(),
            })
            .await
            .unwrap();

        cache
            .mark_blob_pending(
                "sha256",
                "deadbeef",
                "requests-1.0.whl",
                "https://files.example/requests-1.0.whl",
                "+files/root/pypi/+f/dea/dbeef/requests-1.0.whl",
            )
            .await
            .unwrap();
        let ready = cache
            .mark_blob_ready(&BlobWrite {
                blob_kind: "sha256".to_string(),
                blob_id: "deadbeef".to_string(),
                storage_relpath: "+files/root/pypi/+f/dea/dbeef/requests-1.0.whl".to_string(),
                content_type: "application/octet-stream".to_string(),
                fetched_at: 2,
                size_bytes: 1234,
                filename: "requests-1.0.whl".to_string(),
                upstream_url: "https://files.example/requests-1.0.whl".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(ready.projects, ["requests"]);
        assert!(
            cache
                .load_project_responses("requests")
                .await
                .unwrap()
                .is_none()
        );

        let loaded = cache.load_project("requests").await.unwrap().unwrap();
        assert_eq!(loaded.links[0].cached_size_bytes, Some(1234));
        let summaries = cache.list_project_summaries().await.unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].project, "requests");
        assert_eq!(summaries[0].display_name, "requests");
        assert_eq!(summaries[0].page_url, "requests/");
        assert_eq!(summaries[0].file_count, 1);
        assert_eq!(summaries[0].cached_file_count, 1);
        assert_eq!(summaries[0].cached_size_bytes, 1234);

        let summary = cache.cache_summary().await.unwrap();
        assert_eq!(summary.project_count, 1);
        assert_eq!(summary.ready_blob_count, 1);
        assert_eq!(summary.cached_size_bytes, 1234);

        let snapshot = cache.root_dashboard_snapshot_since(0).await.unwrap();
        assert_eq!(snapshot.projects, summaries);
        assert_eq!(snapshot.cache_summary, summary);
    }

    #[tokio::test]
    async fn stores_validates_and_invalidates_materialized_project_responses() {
        let temp = tempdir().unwrap();
        let cache = CacheStore::new(temp.path().to_path_buf());
        cache.initialize().await.unwrap();
        let record = ProjectRecord {
            project: "demo".to_string(),
            fetched_at: 10,
            expires_at: 20,
            upstream_etag: Some("upstream-etag".to_string()),
            upstream_serial: Some(42),
            upstream_project_url: "https://example/simple/demo/".to_string(),
            links: Vec::new(),
        };
        cache.store_project(&record).await.unwrap();
        cache
            .store_project_responses(&ProjectResponseRecord {
                project: "demo".to_string(),
                html_body: Bytes::from_static(b"<html>demo</html>"),
                html_etag: "html-etag".to_string(),
                json_body: Bytes::from_static(br#"{"name":"demo"}"#),
                json_etag: "json-etag".to_string(),
            })
            .await
            .unwrap();

        let responses = cache.load_project_responses("Demo").await.unwrap().unwrap();
        assert_eq!(responses.expires_at, 20);
        assert_eq!(responses.upstream_serial, Some(42));
        assert_eq!(responses.html_body, "<html>demo</html>");
        assert_eq!(responses.json_body, r#"{"name":"demo"}"#);
        assert_eq!(
            cache
                .load_project_response_validator("demo", ProjectResponseFormat::Html)
                .await
                .unwrap()
                .unwrap(),
            ProjectResponseValidator {
                expires_at: 20,
                etag: "html-etag".to_string(),
            }
        );
        assert_eq!(
            cache
                .load_project_response_validator("demo", ProjectResponseFormat::Json)
                .await
                .unwrap()
                .unwrap()
                .etag,
            "json-etag"
        );

        cache.touch_project("demo", 15, 30).await.unwrap();
        assert_eq!(
            cache
                .load_project_response_validator("demo", ProjectResponseFormat::Json)
                .await
                .unwrap()
                .unwrap()
                .expires_at,
            30
        );

        cache.store_project(&record).await.unwrap();
        assert!(
            cache
                .load_project_responses("demo")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn initialize_compacts_legacy_project_metadata_schema() {
        let temp = tempdir().unwrap();
        let db_path = temp.path().join("index.sqlite3");
        {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch(
                "
                CREATE TABLE projects (
                    project TEXT PRIMARY KEY,
                    fetched_at INTEGER NOT NULL,
                    expires_at INTEGER NOT NULL,
                    upstream_etag TEXT,
                    upstream_serial INTEGER,
                    upstream_project_url TEXT NOT NULL,
                    raw_body TEXT NOT NULL
                );
                CREATE TABLE project_links (
                    project TEXT NOT NULL,
                    position INTEGER NOT NULL,
                    filename TEXT NOT NULL,
                    upstream_url TEXT NOT NULL,
                    blob_kind TEXT NOT NULL,
                    blob_id TEXT NOT NULL,
                    requires_python TEXT,
                    yanked TEXT,
                    gpg_sig INTEGER,
                    dist_info_metadata TEXT,
                    core_metadata TEXT,
                    hash_name TEXT,
                    hash_value TEXT,
                    PRIMARY KEY (project, position),
                    UNIQUE (project, filename, upstream_url)
                );
                CREATE INDEX idx_project_links_blob_lookup
                    ON project_links (blob_kind, blob_id, filename);
                CREATE INDEX idx_project_links_project_order
                    ON project_links (project, position);
                ",
            )
            .unwrap();
            conn.execute(
                "INSERT INTO projects (
                    project, fetched_at, expires_at, upstream_etag, upstream_serial,
                    upstream_project_url, raw_body
                 ) VALUES ('demo', 1, 10, 'etag', 42,
                    'https://example/simple/demo/', ?1)",
                params!["x".repeat(2 * 1024 * 1024)],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO project_links (
                    project, position, filename, upstream_url, blob_kind, blob_id,
                    requires_python, yanked, gpg_sig, dist_info_metadata, core_metadata,
                    hash_name, hash_value
                 ) VALUES (
                    'demo', 0, 'demo.whl', 'https://files.example/demo.whl',
                    'sha256', 'deadbeef', NULL, NULL, NULL, NULL, NULL,
                    'sha256', 'deadbeef'
                 )",
                [],
            )
            .unwrap();
        }
        let before_bytes = std::fs::metadata(&db_path).unwrap().len();

        let cache = CacheStore::new(temp.path().to_path_buf());
        cache.initialize().await.unwrap();
        let after_bytes = std::fs::metadata(&db_path).unwrap().len();

        let loaded = cache.load_project("demo").await.unwrap().unwrap();
        assert_eq!(loaded.upstream_serial, Some(42));
        assert_eq!(loaded.links.len(), 1);
        assert_eq!(loaded.links[0].filename, "demo.whl");
        with_db(&cache.db_path, |conn| {
            assert!(!table_has_column(conn, "projects", "raw_body")?);
            assert!(!table_has_unique_index_columns(
                conn,
                "project_links",
                &["project", "filename", "upstream_url"],
            )?);
            assert!(!index_exists(conn, "idx_project_links_project_order")?);
            assert!(index_exists(conn, "idx_project_links_blob_lookup")?);
            let schema_version = conn
                .query_row("PRAGMA user_version", [], |row| row.get::<_, u32>(0))
                .map_err(sqlite_error)?;
            let freelist_pages = conn
                .query_row("PRAGMA freelist_count", [], |row| row.get::<_, u64>(0))
                .map_err(sqlite_error)?;
            let integrity = conn
                .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                .map_err(sqlite_error)?;
            assert_eq!(schema_version, CACHE_SCHEMA_VERSION);
            assert_eq!(freelist_pages, 0);
            assert_eq!(integrity, "ok");
            Ok(())
        })
        .unwrap();
        assert!(after_bytes < before_bytes / 2);

        cache.initialize().await.unwrap();
        assert_eq!(std::fs::metadata(&db_path).unwrap().len(), after_bytes);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn serializes_concurrent_database_writes() {
        let temp = tempdir().unwrap();
        let cache = CacheStore::new(temp.path().to_path_buf());
        cache.initialize().await.unwrap();
        let mut tasks = Vec::new();

        for project_index in 0..8 {
            let cache = cache.clone();
            tasks.push(tokio::spawn(async move {
                let project = format!("project-{project_index}");
                let links = (0..1_000)
                    .map(|link_index| {
                        let blob_id = format!("{project_index:02x}{link_index:062x}");
                        CachedLink {
                            filename: format!("{project}-{link_index}.whl"),
                            upstream_url: format!(
                                "https://files.example/{project}/{link_index}.whl"
                            ),
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
                    })
                    .collect();
                cache
                    .store_project(&ProjectRecord {
                        project: project.clone(),
                        fetched_at: 1,
                        expires_at: 10,
                        upstream_etag: None,
                        upstream_serial: None,
                        upstream_project_url: format!("https://example/simple/{project}/"),
                        links,
                    })
                    .await
                    .map(|_| ())
            }));
        }

        for task in tasks {
            task.await.unwrap().unwrap();
        }
        assert_eq!(cache.list_projects().await.unwrap().len(), 8);
        assert_eq!(
            cache
                .load_project("project-0")
                .await
                .unwrap()
                .unwrap()
                .links
                .len(),
            1_000
        );
    }

    #[tokio::test]
    async fn project_summaries_link_pytorch_wheel_cache_entries_to_wheel_routes() {
        let temp = tempdir().unwrap();
        let cache = CacheStore::new(temp.path().to_path_buf());
        cache.initialize().await.unwrap();
        cache
            .store_project(&ProjectRecord {
                project: "pytorch-wheels-cu126-setuptools".to_string(),
                fetched_at: 1,
                expires_at: 10,
                upstream_etag: None,
                upstream_serial: None,
                upstream_project_url: "https://download.pytorch.org/whl/cu126/setuptools/"
                    .to_string(),
                links: Vec::new(),
            })
            .await
            .unwrap();

        let summaries = cache.list_project_summaries().await.unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].project, "pytorch-wheels-cu126-setuptools");
        assert_eq!(summaries[0].display_name, "setuptools");
        assert_eq!(summaries[0].page_url, "/pytorch-wheels/cu126/setuptools/");
    }

    #[tokio::test]
    async fn stores_and_loads_root_stats_history() {
        let temp = tempdir().unwrap();
        let cache = CacheStore::new(temp.path().to_path_buf());
        cache.initialize().await.unwrap();

        cache
            .record_root_stats_sample(RootHistorySample {
                sampled_at: 100,
                cached_size_bytes: 1024,
                package_count: 1,
                hit_rate_percent: 50.0,
            })
            .await
            .unwrap();
        cache
            .record_root_stats_sample(RootHistorySample {
                sampled_at: 200,
                cached_size_bytes: 2048,
                package_count: 2,
                hit_rate_percent: 75.0,
            })
            .await
            .unwrap();

        let history = cache.root_stats_history_since(150).await.unwrap();
        assert_eq!(
            history,
            vec![RootHistorySample {
                sampled_at: 200,
                cached_size_bytes: 2048,
                package_count: 2,
                hit_rate_percent: 75.0,
            }]
        );

        cache.prune_root_stats_samples_before(200).await.unwrap();
        let history = cache.root_stats_history_since(0).await.unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].sampled_at, 200);
    }

    #[tokio::test]
    async fn stores_blob_metadata_in_sqlite() {
        let temp = tempdir().unwrap();
        let cache = CacheStore::new(temp.path().to_path_buf());
        cache.initialize().await.unwrap();
        let blob_id = "9ceb18f15662bb87e54af2f5953c0484d2ef76f5444d87913360b9ef87d7296d";
        let relpath = cache
            .blob_storage_relpath("sha256", blob_id, "demo.whl")
            .unwrap();
        assert!(
            cache
                .mark_blob_pending(
                    "sha256",
                    blob_id,
                    "demo.whl",
                    "https://example/demo.whl",
                    &relpath
                )
                .await
                .unwrap()
        );
        cache.store_blob_file(&relpath, b"wheel").await.unwrap();
        let blob = cache
            .mark_blob_ready(&BlobWrite {
                blob_kind: "sha256".to_string(),
                blob_id: blob_id.to_string(),
                storage_relpath: relpath.clone(),
                content_type: "application/octet-stream".to_string(),
                fetched_at: 1,
                size_bytes: 5,
                filename: "demo.whl".to_string(),
                upstream_url: "https://example/demo.whl".to_string(),
            })
            .await
            .unwrap()
            .blob;
        assert_eq!(blob.storage_relpath, relpath);
        let bytes = cache.load_blob_bytes(&relpath).await.unwrap().unwrap();
        assert_eq!(bytes, b"wheel");
    }

    #[tokio::test]
    async fn cache_size_enforcement_evicts_least_recently_used_blob() {
        let temp = tempdir().unwrap();
        let cache = CacheStore::new(temp.path().to_path_buf());
        cache.initialize().await.unwrap();
        let first_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let second_id = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let first_relpath = cache
            .blob_storage_relpath("sha256", first_id, "first.whl")
            .unwrap();
        let second_relpath = cache
            .blob_storage_relpath("sha256", second_id, "second.whl")
            .unwrap();

        cache
            .store_project(&ProjectRecord {
                project: "demo".to_string(),
                fetched_at: 1,
                expires_at: 10,
                upstream_etag: None,
                upstream_serial: None,
                upstream_project_url: "https://example/simple/demo/".to_string(),
                links: vec![
                    CachedLink {
                        filename: "first.whl".to_string(),
                        upstream_url: "https://files.example/first.whl".to_string(),
                        blob_kind: "sha256".to_string(),
                        blob_id: first_id.to_string(),
                        cached_size_bytes: None,
                        requires_python: None,
                        yanked: None,
                        gpg_sig: None,
                        dist_info_metadata: None,
                        core_metadata: None,
                        hash_name: Some("sha256".to_string()),
                        hash_value: Some(first_id.to_string()),
                    },
                    CachedLink {
                        filename: "second.whl".to_string(),
                        upstream_url: "https://files.example/second.whl".to_string(),
                        blob_kind: "sha256".to_string(),
                        blob_id: second_id.to_string(),
                        cached_size_bytes: None,
                        requires_python: None,
                        yanked: None,
                        gpg_sig: None,
                        dist_info_metadata: None,
                        core_metadata: None,
                        hash_name: Some("sha256".to_string()),
                        hash_value: Some(second_id.to_string()),
                    },
                ],
            })
            .await
            .unwrap();

        for (blob_id, relpath, filename, fetched_at) in [
            (first_id, first_relpath.as_str(), "first.whl", 1),
            (second_id, second_relpath.as_str(), "second.whl", 2),
        ] {
            cache
                .mark_blob_pending(
                    "sha256",
                    blob_id,
                    filename,
                    &format!("https://files.example/{filename}"),
                    relpath,
                )
                .await
                .unwrap();
            cache.store_blob_file(relpath, b"123456").await.unwrap();
            cache
                .mark_blob_ready(&BlobWrite {
                    blob_kind: "sha256".to_string(),
                    blob_id: blob_id.to_string(),
                    storage_relpath: relpath.to_string(),
                    content_type: "application/octet-stream".to_string(),
                    fetched_at,
                    size_bytes: 6,
                    filename: filename.to_string(),
                    upstream_url: format!("https://files.example/{filename}"),
                })
                .await
                .unwrap();
        }
        cache
            .store_project_responses(&ProjectResponseRecord {
                project: "demo".to_string(),
                html_body: Bytes::from_static(b"<html>demo</html>"),
                html_etag: "html-etag".to_string(),
                json_body: Bytes::from_static(br#"{"name":"demo"}"#),
                json_etag: "json-etag".to_string(),
            })
            .await
            .unwrap();
        assert!(
            cache
                .load_project_responses("demo")
                .await
                .unwrap()
                .is_some()
        );

        cache
            .touch_blob_access("sha256", first_id, 10)
            .await
            .unwrap();
        let evicted = cache.enforce_cache_size(6).await.unwrap();

        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0].blob_id, second_id);
        assert!(
            cache
                .load_project_responses("demo")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            cache
                .load_blob_bytes(&first_relpath)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            cache
                .load_blob_bytes(&second_relpath)
                .await
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            cache.blob_status("sha256", first_id).await.unwrap(),
            BlobStatus::Ready(_)
        ));
        assert_eq!(
            cache.blob_status("sha256", second_id).await.unwrap(),
            BlobStatus::Missing
        );

        let summary = cache.cache_summary().await.unwrap();
        assert_eq!(summary.ready_blob_count, 1);
        assert_eq!(summary.cached_size_bytes, 6);
        let project = cache.list_project_summaries().await.unwrap().remove(0);
        assert_eq!(project.cached_file_count, 1);
        assert_eq!(project.cached_size_bytes, 6);
    }

    #[tokio::test]
    async fn initialize_migrates_blob_access_times_for_existing_cache() {
        let temp = tempdir().unwrap();
        let db_path = temp.path().join("index.sqlite3");
        {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch(
                "
                CREATE TABLE blobs (
                    blob_kind TEXT NOT NULL,
                    blob_id TEXT NOT NULL,
                    storage_relpath TEXT NOT NULL,
                    content_type TEXT NOT NULL,
                    fetched_at INTEGER NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    filename TEXT NOT NULL,
                    upstream_url TEXT NOT NULL,
                    state TEXT NOT NULL,
                    PRIMARY KEY (blob_kind, blob_id),
                    UNIQUE (storage_relpath)
                );
                INSERT INTO blobs (
                    blob_kind, blob_id, storage_relpath, content_type, fetched_at,
                    size_bytes, filename, upstream_url, state
                ) VALUES (
                    'sha256', 'old', '+files/root/pypi/+f/old/demo.whl',
                    'application/octet-stream', 7, 5, 'demo.whl',
                    'https://files.example/demo.whl', 'ready'
                );
                ",
            )
            .unwrap();
        }

        let cache = CacheStore::new(temp.path().to_path_buf());
        cache.initialize().await.unwrap();
        cache.touch_blob_access("sha256", "old", 9).await.unwrap();

        let last_accessed_at = with_db(&cache.db_path, |conn| {
            conn.query_row(
                "SELECT last_accessed_at FROM blobs WHERE blob_kind = 'sha256' AND blob_id = 'old'",
                [],
                |row| row.get::<_, u64>(0),
            )
            .map_err(sqlite_error)
        })
        .unwrap();
        assert_eq!(last_accessed_at, 9);
    }
}
