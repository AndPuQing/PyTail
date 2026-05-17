use axum::Router;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::header::{CONTENT_TYPE, ETAG};
use axum::http::{HeaderValue, Response, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use bytes::Bytes;
use pytail::config::AppConfig;
use sha2::{Digest, Sha256};
use std::error::Error;
use std::path::Path as FsPath;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::tempdir;
use tokio::net::TcpListener;

const PACKAGE_NAME: &str = "demo-pkg";
const MODULE_NAME: &str = "demo_pkg";
const VERSION: &str = "1.0.0";
const WHEEL_FILENAME: &str = "demo_pkg-1.0.0-py3-none-any.whl";

#[tokio::test]
async fn pip_installs_package_through_local_simple_api() -> Result<(), Box<dyn Error>> {
    let temp = tempdir()?;
    let wheel_path = temp.path().join(WHEEL_FILENAME);
    let python = python_executable()?;

    create_demo_wheel(&python, &wheel_path)?;
    let wheel = tokio::fs::read(&wheel_path).await?;
    let sha256 = format!("{:x}", Sha256::digest(&wheel));

    let upstream = spawn_upstream(Bytes::from(wheel), sha256).await?;
    let proxy_listener = TcpListener::bind("127.0.0.1:0").await?;
    let proxy_addr = proxy_listener.local_addr()?;
    let cache_dir = temp.path().join("pytail-cache");
    let proxy_task = tokio::spawn(pytail::server::serve_listener(
        AppConfig {
            bind: proxy_addr.to_string(),
            upstream_base_url: upstream.base_url.clone(),
            pytorch_wheels_upstream_base_url: upstream.base_url.clone(),
            cache_dir,
            project_cache_ttl_secs: 3600,
            request_timeout_secs: 5,
            stats_interval_secs: 0,
            verbose: false,
        },
        proxy_listener,
    ));

    let target_dir = temp.path().join("install-target");
    let index_url = format!("http://{proxy_addr}/simple/");
    let install = {
        let python = python.clone();
        let target_dir = target_dir.clone();
        tokio::task::spawn_blocking(move || {
            Command::new(&python)
                .args([
                    "-m",
                    "pip",
                    "install",
                    "--disable-pip-version-check",
                    "--no-deps",
                    "--target",
                ])
                .arg(&target_dir)
                .args([
                    "--index-url",
                    &index_url,
                    &format!("{PACKAGE_NAME}=={VERSION}"),
                ])
                .output()
        })
        .await??
    };

    proxy_task.abort();

    assert!(
        install.status.success(),
        "pip install failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&install.stdout),
        String::from_utf8_lossy(&install.stderr)
    );
    assert!(
        target_dir.join(MODULE_NAME).join("__init__.py").exists(),
        "pip did not install the package module"
    );
    assert_eq!(upstream.state.file_requests.load(Ordering::SeqCst), 1);

    let import = Command::new(&python)
        .arg("-c")
        .arg("import demo_pkg; assert demo_pkg.VALUE == 'installed-through-pytail'")
        .env("PYTHONPATH", &target_dir)
        .output()?;
    assert!(
        import.status.success(),
        "installed package import failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&import.stdout),
        String::from_utf8_lossy(&import.stderr)
    );

    Ok(())
}

fn python_executable() -> Result<String, Box<dyn Error>> {
    if let Ok(python) = std::env::var("PYTHON") {
        ensure_pip(&python)?;
        return Ok(python);
    }
    for candidate in ["python3", "python"] {
        if ensure_pip(candidate).is_ok() {
            return Ok(candidate.to_string());
        }
    }
    Err("Python with pip is required for the pip smoke test".into())
}

fn ensure_pip(python: &str) -> Result<(), Box<dyn Error>> {
    let status = Command::new(python)
        .args(["-m", "pip", "--version"])
        .status()?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("{python} does not have pip").into())
    }
}

fn create_demo_wheel(python: &str, wheel_path: &FsPath) -> Result<(), Box<dyn Error>> {
    let script = r#"
import sys
import zipfile

wheel_path = sys.argv[1]
dist_info = "demo_pkg-1.0.0.dist-info"
files = {
    "demo_pkg/__init__.py": "VALUE = 'installed-through-pytail'\n",
    f"{dist_info}/METADATA": "\n".join([
        "Metadata-Version: 2.1",
        "Name: demo-pkg",
        "Version: 1.0.0",
        "Requires-Python: >=3.8",
        "",
    ]),
    f"{dist_info}/WHEEL": "\n".join([
        "Wheel-Version: 1.0",
        "Generator: pytail-smoke",
        "Root-Is-Purelib: true",
        "Tag: py3-none-any",
        "",
    ]),
}
record = "\n".join([f"{name},," for name in files] + [f"{dist_info}/RECORD,,", ""])
files[f"{dist_info}/RECORD"] = record

with zipfile.ZipFile(wheel_path, "w", compression=zipfile.ZIP_DEFLATED) as wheel:
    for name, content in files.items():
        wheel.writestr(name, content)
"#;
    let output = Command::new(python)
        .args(["-c", script])
        .arg(wheel_path)
        .output()?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "failed to create test wheel\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .into())
    }
}

struct TestUpstream {
    base_url: String,
    state: Arc<TestUpstreamState>,
}

struct TestUpstreamState {
    wheel: Bytes,
    sha256: String,
    file_requests: AtomicUsize,
}

async fn spawn_upstream(wheel: Bytes, sha256: String) -> Result<TestUpstream, Box<dyn Error>> {
    let state = Arc::new(TestUpstreamState {
        wheel,
        sha256,
        file_requests: AtomicUsize::new(0),
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
    if project != PACKAGE_NAME {
        let mut response = Response::new(Body::from("not found"));
        *response.status_mut() = StatusCode::NOT_FOUND;
        return response;
    }
    let body = format!(
        r#"<!doctype html>
<html><body>
  <a href="/packages/{WHEEL_FILENAME}#sha256={}" data-requires-python="&gt;=3.8">{WHEEL_FILENAME}</a>
</body></html>
"#,
        state.sha256
    );
    let mut response = Response::new(Body::from(body));
    response
        .headers_mut()
        .insert(ETAG, HeaderValue::from_static("\"pip-smoke\""));
    response
}

async fn upstream_file(
    State(state): State<Arc<TestUpstreamState>>,
    Path(filename): Path<String>,
) -> impl IntoResponse {
    if filename != WHEEL_FILENAME {
        return (StatusCode::NOT_FOUND, Body::from("not found"));
    }
    state.file_requests.fetch_add(1, Ordering::SeqCst);
    let mut response = Response::new(Body::from(state.wheel.clone()));
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    (StatusCode::OK, response.into_body())
}
