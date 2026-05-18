use clap::Parser;
use pytail::cache::CacheStore;
use rusqlite::{Connection, params};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Parser)]
#[command(
    name = "root_summary_bench",
    about = "Local benchmark for root project summary SQL"
)]
struct Args {
    #[arg(long, default_value_t = 2_000)]
    projects: usize,

    #[arg(long, default_value_t = 10)]
    links_per_project: usize,

    #[arg(long, default_value_t = 2)]
    cached_every: usize,

    #[arg(long, default_value_t = 20)]
    iterations: usize,

    #[arg(long)]
    cache_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let cache_dir = args.cache_dir.clone().unwrap_or_else(|| {
        std::env::temp_dir().join(format!("pytail-root-summary-{}", unique_suffix()))
    });
    tokio::fs::create_dir_all(&cache_dir).await?;
    let cache = CacheStore::new(cache_dir.clone());
    cache.initialize().await?;

    let db_path = cache_dir.join("index.sqlite3");
    seed_db(
        &db_path,
        args.projects,
        args.links_per_project,
        args.cached_every.max(1),
    )?;

    let expected_rows = args.projects;
    let old = bench_query(&db_path, OLD_SUMMARY_SQL, args.iterations, true)?;
    let new = bench_query(&db_path, NEW_SUMMARY_SQL, args.iterations, true)?;
    let api = bench_cache_api(&cache, args.iterations).await?;
    let dashboard = bench_dashboard_api(&cache, args.iterations).await?;

    println!("scenario: root-summary");
    println!(
        "config: projects={} links_per_project={} cached_every={} iterations={}",
        args.projects, args.links_per_project, args.cached_every, args.iterations
    );
    println!("expected_rows: {expected_rows}");
    println!(
        "old_correlated_sql_ms: {:.2} rows={}",
        old.elapsed_ms, old.rows
    );
    println!("new_cte_sql_ms: {:.2} rows={}", new.elapsed_ms, new.rows);
    println!("cache_api_ms: {:.2} rows={}", api.elapsed_ms, api.rows);
    println!(
        "dashboard_api_ms: {:.2} rows={}",
        dashboard.elapsed_ms, dashboard.rows
    );
    println!(
        "sql_speedup: {:.2}x",
        old.elapsed_ms.max(0.001) / new.elapsed_ms.max(0.001)
    );

    Ok(())
}

#[derive(Debug)]
struct BenchResult {
    elapsed_ms: f64,
    rows: usize,
}

fn bench_query(
    db_path: &Path,
    sql: &str,
    iterations: usize,
    reuse_connection: bool,
) -> Result<BenchResult, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let mut rows = 0;
    if reuse_connection {
        let conn = Connection::open(db_path)?;
        for _ in 0..iterations {
            rows = run_summary_query(&conn, sql)?;
        }
    } else {
        for _ in 0..iterations {
            let conn = Connection::open(db_path)?;
            rows = run_summary_query(&conn, sql)?;
        }
    }
    Ok(BenchResult {
        elapsed_ms: started.elapsed().as_secs_f64() * 1000.0,
        rows,
    })
}

async fn bench_cache_api(
    cache: &CacheStore,
    iterations: usize,
) -> Result<BenchResult, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let mut rows = 0;
    for _ in 0..iterations {
        rows = cache.list_project_summaries().await?.len();
    }
    Ok(BenchResult {
        elapsed_ms: started.elapsed().as_secs_f64() * 1000.0,
        rows,
    })
}

async fn bench_dashboard_api(
    cache: &CacheStore,
    iterations: usize,
) -> Result<BenchResult, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let mut rows = 0;
    for _ in 0..iterations {
        rows = cache.root_dashboard_snapshot_since(0).await?.projects.len();
    }
    Ok(BenchResult {
        elapsed_ms: started.elapsed().as_secs_f64() * 1000.0,
        rows,
    })
}

fn run_summary_query(conn: &Connection, sql: &str) -> Result<usize, rusqlite::Error> {
    let mut stmt = conn.prepare(sql)?;
    let mut rows = stmt.query([])?;
    let mut count = 0;
    while let Some(row) = rows.next()? {
        let _: String = row.get(0)?;
        let _: String = row.get(1)?;
        let _: u64 = row.get(2)?;
        let _: u64 = row.get(3)?;
        let _: u64 = row.get(4)?;
        count += 1;
    }
    Ok(count)
}

fn seed_db(
    db_path: &Path,
    projects: usize,
    links_per_project: usize,
    cached_every: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = Connection::open(db_path)?;
    let tx = conn.transaction()?;
    for project_index in 0..projects {
        let project = format!("pkg-{project_index:05}");
        tx.execute(
            "INSERT INTO projects (
                project, fetched_at, expires_at, upstream_etag, upstream_serial,
                upstream_project_url, raw_body
             ) VALUES (?1, 1, 3601, NULL, NULL, ?2, '')",
            params![project, format!("https://example.test/simple/{project}/")],
        )?;
        for link_index in 0..links_per_project {
            let filename = format!("{project}-{link_index}.whl");
            let blob_id = format!("{project_index:016x}{link_index:048x}");
            tx.execute(
                "INSERT INTO project_links (
                    project, position, filename, upstream_url, blob_kind, blob_id,
                    requires_python, yanked, gpg_sig, dist_info_metadata, core_metadata,
                    hash_name, hash_value
                 ) VALUES (?1, ?2, ?3, ?4, 'sha256', ?5, NULL, NULL, NULL, NULL, NULL, 'sha256', ?5)",
                params![
                    project,
                    link_index as i64,
                    filename,
                    format!("https://files.example.test/{filename}"),
                    blob_id,
                ],
            )?;
            if link_index % cached_every == 0 {
                tx.execute(
                    "INSERT INTO blobs (
                        blob_kind, blob_id, storage_relpath, content_type, fetched_at,
                        size_bytes, filename, upstream_url, state
                     ) VALUES ('sha256', ?1, ?2, 'application/octet-stream', 1, ?3, ?4, ?5, 'ready')",
                    params![
                        blob_id,
                        format!("+files/root/pypi/+f/{}/{}/{}", &blob_id[..3], &blob_id[3..16], filename),
                        1024_u64 + link_index as u64,
                        filename,
                        format!("https://files.example.test/{project}-{link_index}.whl"),
                    ],
                )?;
            }
        }
    }
    tx.commit()?;
    Ok(())
}

fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{nanos}-{}", std::process::id())
}

const OLD_SUMMARY_SQL: &str = "
SELECT
    p.project,
    p.upstream_project_url,
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
 GROUP BY p.project, p.upstream_project_url
 ORDER BY p.project";

const NEW_SUMMARY_SQL: &str = "
WITH
    project_file_counts AS (
        SELECT project, COUNT(position) AS file_count
        FROM project_links
        GROUP BY project
    ),
    ready_project_blobs AS (
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
    ),
    ready_project_totals AS (
        SELECT
            project,
            COUNT(*) AS cached_file_count,
            COALESCE(SUM(size_bytes), 0) AS cached_size_bytes
        FROM ready_project_blobs
        GROUP BY project
    )
 SELECT
     p.project,
     p.upstream_project_url,
     COALESCE(pfc.file_count, 0) AS file_count,
     COALESCE(rpt.cached_file_count, 0) AS cached_file_count,
     COALESCE(rpt.cached_size_bytes, 0) AS cached_size_bytes
 FROM projects p
 LEFT JOIN project_file_counts pfc ON pfc.project = p.project
 LEFT JOIN ready_project_totals rpt ON rpt.project = p.project
 ORDER BY p.project";
