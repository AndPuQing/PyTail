use crate::simple::normalize_project_name;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs;

const BLOB_STATE_PENDING: &str = "pending";
const BLOB_STATE_READY: &str = "ready";

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
    pub raw_body: String,
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
pub enum BlobStatus {
    Ready(BlobInfo),
    Pending,
    Missing,
}

#[derive(Debug, Clone)]
pub struct CacheStore {
    root: PathBuf,
    db_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct ProjectRecord {
    pub project: String,
    pub fetched_at: u64,
    pub expires_at: u64,
    pub upstream_etag: Option<String>,
    pub upstream_serial: Option<u64>,
    pub upstream_project_url: String,
    pub raw_body: String,
    pub links: Vec<CachedLink>,
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
        Self { root, db_path }
    }

    pub async fn initialize(&self) -> io::Result<()> {
        fs::create_dir_all(self.files_root()).await?;
        let db_path = self.db_path.clone();
        tokio::task::spawn_blocking(move || init_db(&db_path))
            .await
            .map_err(join_error)?
    }

    pub async fn list_projects(&self) -> io::Result<Vec<String>> {
        let db_path = self.db_path.clone();
        tokio::task::spawn_blocking(move || {
            let conn = open_db(&db_path)?;
            let mut stmt = conn
                .prepare("SELECT project FROM projects ORDER BY project")
                .map_err(sqlite_error)?;
            let rows = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(sqlite_error)?;
            let mut projects = Vec::new();
            for row in rows {
                projects.push(row.map_err(sqlite_error)?);
            }
            Ok(projects)
        })
        .await
        .map_err(join_error)?
    }

    pub async fn list_project_summaries(&self) -> io::Result<Vec<ProjectSummary>> {
        let db_path = self.db_path.clone();
        tokio::task::spawn_blocking(move || {
            let conn = open_db(&db_path)?;
            let mut stmt = conn
                .prepare(
                    "SELECT
                        p.project,
                        COUNT(pl.position) AS file_count,
                        COALESCE((
                            SELECT COUNT(*)
                            FROM (
                                SELECT DISTINCT pl2.blob_kind, pl2.blob_id
                                FROM project_links pl2
                                JOIN blobs b2
                                  ON b2.blob_kind = pl2.blob_kind
                                 AND b2.blob_id = pl2.blob_id
                                 AND b2.state = 'ready'
                                WHERE pl2.project = p.project
                            )
                        ), 0) AS cached_file_count,
                        COALESCE((
                            SELECT SUM(size_bytes)
                            FROM (
                                SELECT DISTINCT b3.blob_kind, b3.blob_id, b3.size_bytes
                                FROM project_links pl3
                                JOIN blobs b3
                                  ON b3.blob_kind = pl3.blob_kind
                                 AND b3.blob_id = pl3.blob_id
                                 AND b3.state = 'ready'
                                WHERE pl3.project = p.project
                            )
                        ), 0) AS cached_size_bytes
                     FROM projects p
                     LEFT JOIN project_links pl ON pl.project = p.project
                     GROUP BY p.project
                     ORDER BY p.project",
                )
                .map_err(sqlite_error)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok(ProjectSummary {
                        project: row.get(0)?,
                        file_count: row.get(1)?,
                        cached_file_count: row.get(2)?,
                        cached_size_bytes: row.get(3)?,
                    })
                })
                .map_err(sqlite_error)?;
            let mut projects = Vec::new();
            for row in rows {
                projects.push(row.map_err(sqlite_error)?);
            }
            Ok(projects)
        })
        .await
        .map_err(join_error)?
    }

    pub async fn cache_summary(&self) -> io::Result<CacheSummary> {
        let db_path = self.db_path.clone();
        tokio::task::spawn_blocking(move || {
            let conn = open_db(&db_path)?;
            conn.query_row(
                "SELECT
                    (SELECT COUNT(*) FROM projects),
                    COUNT(*),
                    COALESCE(SUM(size_bytes), 0)
                 FROM blobs
                 WHERE state = 'ready'",
                [],
                |row| {
                    Ok(CacheSummary {
                        project_count: row.get(0)?,
                        ready_blob_count: row.get(1)?,
                        cached_size_bytes: row.get(2)?,
                    })
                },
            )
            .map_err(sqlite_error)
        })
        .await
        .map_err(join_error)?
    }

    pub async fn record_root_stats_sample(&self, sample: RootHistorySample) -> io::Result<()> {
        let db_path = self.db_path.clone();
        tokio::task::spawn_blocking(move || {
            let conn = open_db(&db_path)?;
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
        .await
        .map_err(join_error)?
    }

    pub async fn root_stats_history_since(&self, since: u64) -> io::Result<Vec<RootHistorySample>> {
        let db_path = self.db_path.clone();
        tokio::task::spawn_blocking(move || {
            let conn = open_db(&db_path)?;
            let mut stmt = conn
                .prepare(
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
        })
        .await
        .map_err(join_error)?
    }

    pub async fn prune_root_stats_samples_before(&self, before: u64) -> io::Result<()> {
        let db_path = self.db_path.clone();
        tokio::task::spawn_blocking(move || {
            let conn = open_db(&db_path)?;
            conn.execute(
                "DELETE FROM root_stats_samples WHERE sampled_at < ?1",
                params![before],
            )
            .map_err(sqlite_error)?;
            Ok(())
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
        let db_path = self.db_path.clone();
        let record = record.clone();
        tokio::task::spawn_blocking(move || store_project_db(&db_path, &record))
            .await
            .map_err(join_error)?
    }

    pub async fn touch_project(
        &self,
        project: &str,
        fetched_at: u64,
        expires_at: u64,
    ) -> io::Result<()> {
        let db_path = self.db_path.clone();
        let project = normalize_project_name(project);
        tokio::task::spawn_blocking(move || {
            let conn = open_db(&db_path)?;
            conn.execute(
                "UPDATE projects SET fetched_at = ?2, expires_at = ?3 WHERE project = ?1",
                params![project, fetched_at, expires_at],
            )
            .map_err(sqlite_error)?;
            Ok(())
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
            let conn = open_db(&db_path)?;
            if route_a == "_url" {
                return query_link_by_blob(
                    &conn,
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
                &conn,
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
        let db_path = self.db_path.clone();
        let blob_kind = blob_kind.to_string();
        let blob_id = blob_id.to_string();
        let filename = filename.to_string();
        let upstream_url = upstream_url.to_string();
        let storage_relpath = storage_relpath.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = open_db(&db_path)?;
            let inserted = conn
                .execute(
                    "INSERT OR IGNORE INTO blobs (
                    blob_kind, blob_id, storage_relpath, content_type, fetched_at, size_bytes,
                    filename, upstream_url, state
                 ) VALUES (?1, ?2, ?3, '', 0, 0, ?4, ?5, ?6)",
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

    pub async fn mark_blob_ready(&self, blob: &BlobWrite) -> io::Result<BlobInfo> {
        let db_path = self.db_path.clone();
        let blob = blob.clone();
        tokio::task::spawn_blocking(move || {
            let conn = open_db(&db_path)?;
            conn.execute(
                "UPDATE blobs
                 SET storage_relpath = ?3,
                     content_type = ?4,
                     fetched_at = ?5,
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
            Ok(BlobInfo {
                blob_kind: blob.blob_kind,
                blob_id: blob.blob_id,
                storage_relpath: blob.storage_relpath,
                content_type: blob.content_type,
                fetched_at: blob.fetched_at,
                size_bytes: blob.size_bytes,
                filename: blob.filename,
                upstream_url: blob.upstream_url,
                state: BLOB_STATE_READY.to_string(),
            })
        })
        .await
        .map_err(join_error)?
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
    let conn = open_db(path)?;
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
            upstream_project_url TEXT NOT NULL,
            raw_body TEXT NOT NULL
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
            PRIMARY KEY (project, position),
            UNIQUE (project, filename, upstream_url)
        );

        CREATE TABLE IF NOT EXISTS blobs (
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

        CREATE INDEX IF NOT EXISTS idx_project_links_blob_lookup
            ON project_links (blob_kind, blob_id, filename);

        CREATE INDEX IF NOT EXISTS idx_project_links_project_order
            ON project_links (project, position);

        CREATE TABLE IF NOT EXISTS root_stats_samples (
            sampled_at INTEGER PRIMARY KEY,
            cached_size_bytes INTEGER NOT NULL,
            package_count INTEGER NOT NULL,
            hit_rate_percent REAL NOT NULL
        );
        ",
    )
    .map_err(sqlite_error)?;
    Ok(())
}

fn open_db(path: &Path) -> io::Result<Connection> {
    let conn = Connection::open(path).map_err(sqlite_error)?;
    conn.busy_timeout(std::time::Duration::from_secs(5))
        .map_err(sqlite_error)?;
    conn.execute_batch(
        "
        PRAGMA synchronous=NORMAL;
        PRAGMA temp_store=MEMORY;
        ",
    )
    .map_err(sqlite_error)?;
    Ok(conn)
}

fn load_project_db(path: &Path, project: &str) -> io::Result<Option<ProjectCache>> {
    let conn = open_db(path)?;
    let row = conn
        .query_row(
            "SELECT fetched_at, expires_at, upstream_etag, upstream_serial, upstream_project_url, raw_body
             FROM projects WHERE project = ?1",
            params![project],
            |row| {
                Ok((
                    row.get::<_, u64>(0)?,
                    row.get::<_, u64>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<u64>>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                ))
            },
        )
        .optional()
        .map_err(sqlite_error)?;
    let Some((
        fetched_at,
        expires_at,
        upstream_etag,
        upstream_serial,
        upstream_project_url,
        raw_body,
    )) = row
    else {
        return Ok(None);
    };

    let mut stmt = conn
        .prepare(
            "SELECT pl.filename, pl.upstream_url, pl.blob_kind, pl.blob_id,
                    (
                        SELECT b.size_bytes
                        FROM blobs b
                        WHERE b.blob_kind = pl.blob_kind
                          AND b.blob_id = pl.blob_id
                          AND b.state = 'ready'
                        LIMIT 1
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
        raw_body,
        links,
    }))
}

fn store_project_db(path: &Path, record: &ProjectRecord) -> io::Result<ProjectCache> {
    let mut conn = open_db(path)?;
    let tx = conn.transaction().map_err(sqlite_error)?;
    tx.execute(
        "INSERT INTO projects (
            project, fetched_at, expires_at, upstream_etag, upstream_serial, upstream_project_url, raw_body
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
         ON CONFLICT(project) DO UPDATE SET
            fetched_at = excluded.fetched_at,
            expires_at = excluded.expires_at,
            upstream_etag = excluded.upstream_etag,
            upstream_serial = excluded.upstream_serial,
            upstream_project_url = excluded.upstream_project_url,
            raw_body = excluded.raw_body",
        params![
            record.project,
            record.fetched_at,
            record.expires_at,
            record.upstream_etag,
            record.upstream_serial,
            record.upstream_project_url,
            record.raw_body,
        ],
    )
    .map_err(sqlite_error)?;
    tx.execute(
        "DELETE FROM project_links WHERE project = ?1",
        params![record.project],
    )
    .map_err(sqlite_error)?;
    {
        let mut insert_link = tx
            .prepare(
                "INSERT INTO project_links (
                project, position, filename, upstream_url, blob_kind, blob_id,
                requires_python, yanked, gpg_sig, dist_info_metadata, core_metadata, hash_name, hash_value
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            )
            .map_err(sqlite_error)?;
        for (position, link) in record.links.iter().enumerate() {
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
        }
    }
    tx.commit().map_err(sqlite_error)?;
    Ok(ProjectCache {
        project: record.project.clone(),
        fetched_at: record.fetched_at,
        expires_at: record.expires_at,
        upstream_etag: record.upstream_etag.clone(),
        upstream_serial: record.upstream_serial,
        upstream_project_url: record.upstream_project_url.clone(),
        raw_body: record.raw_body.clone(),
        links: record.links.clone(),
    })
}

fn blob_status_db(path: &Path, blob_kind: &str, blob_id: &str) -> io::Result<BlobStatus> {
    let conn = open_db(path)?;
    let row = conn
        .query_row(
            "SELECT storage_relpath, content_type, fetched_at, size_bytes, filename, upstream_url, state
             FROM blobs
             WHERE blob_kind = ?1 AND blob_id = ?2",
            params![blob_kind, blob_id],
            |row| {
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
            },
        )
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
}

fn query_link_by_blob<P>(conn: &Connection, sql: &str, params: P) -> io::Result<Option<CachedLink>>
where
    P: rusqlite::Params,
{
    conn.query_row(sql, params, |row| {
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn stores_and_loads_project_from_sqlite() {
        let temp = tempdir().unwrap();
        let cache = CacheStore::new(temp.path().to_path_buf());
        cache.initialize().await.unwrap();
        let record = ProjectRecord {
            project: "requests".to_string(),
            fetched_at: 1,
            expires_at: 10,
            upstream_etag: Some("abc".to_string()),
            upstream_serial: Some(42),
            upstream_project_url: "https://example/simple/requests/".to_string(),
            raw_body: "<html></html>".to_string(),
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

        cache.store_project(&record).await.unwrap();
        let loaded = cache.load_project("Requests").await.unwrap().unwrap();
        assert_eq!(loaded.project, "requests");
        assert_eq!(loaded.upstream_serial, Some(42));
        assert_eq!(loaded.links.len(), 1);
        assert_eq!(loaded.links[0].blob_id, "deadbeef");
        assert_eq!(loaded.links[0].cached_size_bytes, None);

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
        cache
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

        let loaded = cache.load_project("requests").await.unwrap().unwrap();
        assert_eq!(loaded.links[0].cached_size_bytes, Some(1234));
        let summaries = cache.list_project_summaries().await.unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].project, "requests");
        assert_eq!(summaries[0].file_count, 1);
        assert_eq!(summaries[0].cached_file_count, 1);
        assert_eq!(summaries[0].cached_size_bytes, 1234);

        let summary = cache.cache_summary().await.unwrap();
        assert_eq!(summary.project_count, 1);
        assert_eq!(summary.ready_blob_count, 1);
        assert_eq!(summary.cached_size_bytes, 1234);
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
            .unwrap();
        assert_eq!(blob.storage_relpath, relpath);
        let bytes = cache.load_blob_bytes(&relpath).await.unwrap().unwrap();
        assert_eq!(bytes, b"wheel");
    }
}
