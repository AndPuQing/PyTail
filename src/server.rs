use crate::config::AppConfig;
use crate::local::{FileLogEntry, FileMetadata, LocalFile, LocalReadFile, LocalStore, sha256_hex};
use crate::registry::{
    IndexConfig, IndexInput, LOGIN_EXPIRATION_SECS, Registry, UserInput, default_stage_config,
};
use crate::simple::{
    SourcePage, merge_project_json, merge_project_page, merge_root_json, merge_root_page,
    normalize_project_name, rewrite_project_page_hrefs,
};
use crate::upstream::{CurlFetcher, FetchReport, Fetcher, MultiSourceIndex, UpstreamResult};
use axum::Router;
use axum::body::{Bytes, to_bytes};
use axum::extract::{Extension, FromRequest, Multipart, Path, Query, Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use flate2::read::{DeflateDecoder, GzDecoder};
use httpdate::fmt_http_date;
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio::task;

const SIMPLE_JSON_CONTENT_TYPE: &str = "application/vnd.pypi.simple.v1+json";
const DEVPI_API_VERSION: &str = "2";
const DEVPI_SERVER_VERSION: &str = concat!("devpi-rs/", env!("CARGO_PKG_VERSION"));

struct ExternalMultipartFile<'a> {
    field: &'a str,
    filename: &'a str,
    bytes: &'a [u8],
}

struct ExternalPostResponse {
    status: u16,
    body: String,
}

trait ExternalPoster: Send + Sync {
    fn post_multipart<'a>(
        &self,
        url: &str,
        auth: Option<(&str, &str)>,
        fields: &[(String, String)],
        file: Option<ExternalMultipartFile<'a>>,
    ) -> io::Result<ExternalPostResponse>;
}

struct TcpExternalPoster;

impl ExternalPoster for TcpExternalPoster {
    fn post_multipart<'a>(
        &self,
        url: &str,
        auth: Option<(&str, &str)>,
        fields: &[(String, String)],
        file: Option<ExternalMultipartFile<'a>>,
    ) -> io::Result<ExternalPostResponse> {
        external_post_multipart(url, auth, fields, file)
    }
}

#[derive(Clone)]
struct AppState {
    index: Arc<MultiSourceIndex>,
    local: Arc<LocalStore>,
    registry: Arc<Registry>,
    serial: Arc<SerialStore>,
    fetcher: Arc<dyn Fetcher>,
    external_poster: Arc<dyn ExternalPoster>,
    upstream_reports: Arc<Mutex<Vec<FetchReport>>>,
}

#[derive(Clone)]
struct OutsideUrl(Option<String>);

#[derive(Debug)]
struct SerialStore {
    path: PathBuf,
    changelog_path: PathBuf,
    value: Mutex<u64>,
}

impl SerialStore {
    fn new(path: PathBuf) -> Self {
        let value = fs::read_to_string(&path)
            .ok()
            .and_then(|text| text.trim().parse::<u64>().ok())
            .unwrap_or(0);
        Self {
            changelog_path: path.with_file_name(".devpi-rs-changelog.jsonl"),
            path,
            value: Mutex::new(value),
        }
    }

    fn current(&self) -> u64 {
        *self.value.lock().unwrap()
    }

    fn bump_with_event(&self, event: Value) -> io::Result<u64> {
        let mut value = self.value.lock().unwrap();
        *value = value.saturating_add(1);
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&self.path, format!("{}\n", *value))?;
        self.append_changelog(*value, event)?;
        Ok(*value)
    }

    fn append_changelog(&self, serial: u64, event: Value) -> io::Result<()> {
        let entry = match event {
            Value::Object(mut object) => {
                object.insert("serial".to_string(), json!(serial));
                Value::Object(object)
            }
            value => json!({
                "serial": serial,
                "event": value,
            }),
        };
        let mut options = fs::OpenOptions::new();
        options.create(true).append(true);
        use std::io::Write as _;
        let mut file = options.open(&self.changelog_path)?;
        writeln!(file, "{entry}")?;
        Ok(())
    }

    fn changelog_since(&self, serial: u64, streaming: bool) -> io::Result<Vec<Value>> {
        if !self.changelog_path.exists() {
            return Ok(Vec::new());
        }
        let text = fs::read_to_string(&self.changelog_path)?;
        let mut entries = Vec::new();
        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            let value: Value = serde_json::from_str(line).map_err(|err| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid changelog entry: {err}"),
                )
            })?;
            let entry_serial = value["serial"].as_u64().unwrap_or_default();
            let include = if streaming {
                entry_serial >= serial
            } else {
                entry_serial == serial
            };
            if include {
                entries.push(value);
            }
        }
        Ok(entries)
    }
}

struct SaveFileRequest {
    user: String,
    index: String,
    project: String,
    filename: String,
    body: Bytes,
    metadata: BTreeMap<String, String>,
    auth_user: Option<String>,
}

struct StoreToxResultRequest {
    user: String,
    index: String,
    project: String,
    filename: String,
    body: Bytes,
    auth_user: Option<String>,
    outside_url: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct LoginRequest {
    user: Option<String>,
    password: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct PushRequest {
    name: Option<String>,
    version: Option<String>,
    targetindex: Option<String>,
    posturl: Option<String>,
    username: Option<String>,
    password: Option<String>,
    #[serde(default)]
    register_project: bool,
    #[serde(default)]
    no_docs: bool,
    #[serde(default)]
    only_docs: bool,
}

struct SelectedPushFile {
    filename: String,
    bytes: Vec<u8>,
    metadata: Option<BTreeMap<String, String>>,
    log: Vec<FileLogEntry>,
}

pub async fn serve(config: AppConfig) -> io::Result<()> {
    let listener = TcpListener::bind(&config.listen).await?;
    let app = router(config.clone());

    eprintln!("devpi-rs listening on http://{}", config.listen);
    eprintln!("local packages -> {}", config.package_dir.display());
    eprintln!("sources:");
    for source in &config.sources {
        eprintln!("  {} -> {}", source.name, source.simple_url);
    }

    axum::serve(listener, app)
        .await
        .map_err(|err| io::Error::other(format!("server error: {err}")))
}

fn router(config: AppConfig) -> Router {
    let package_dir = config.package_dir.clone();
    let outside_url = OutsideUrl(config.outside_url.clone());
    let state = AppState {
        index: Arc::new(MultiSourceIndex::new(
            config.sources.clone(),
            config.cache_dir.clone(),
        )),
        local: Arc::new(LocalStore::new(package_dir.clone())),
        registry: Arc::new(Registry::new(package_dir.clone())),
        serial: Arc::new(SerialStore::new(package_dir.join(".devpi-rs-serial"))),
        fetcher: Arc::new(CurlFetcher::default()),
        external_poster: Arc::new(TcpExternalPoster),
        upstream_reports: Arc::new(Mutex::new(Vec::new())),
    };

    router_from_state_with_outside_url(state, outside_url)
}

#[cfg(test)]
fn router_from_state(state: AppState) -> Router {
    router_from_state_with_outside_url(state, OutsideUrl(None))
}

fn router_from_state_with_outside_url(state: AppState, outside_url: OutsideUrl) -> Router {
    Router::new()
        .route("/", get(root))
        .route("/+status", get(status))
        .route("/+status/", get(status))
        .route("/+changelog/{serial}", get(changelog))
        .route("/+changelog/{serial}/", get(changelog))
        .route("/+files/{*relpath}", get(files_download))
        .route("/+authcheck", get(authcheck).post(authcheck))
        .route("/+authcheck/", get(authcheck).post(authcheck))
        .route("/+login", post(login))
        .route("/+login/", post(login))
        .route("/+api", get(root_api))
        .route("/+api/", get(root_api))
        .route(
            "/{user}",
            get(user_get)
                .put(user_put)
                .patch(user_put)
                .delete(user_delete),
        )
        .route(
            "/{user}/",
            get(user_get)
                .put(user_put)
                .patch(user_put)
                .delete(user_delete),
        )
        .route("/{user}/+api", get(user_api))
        .route("/{user}/+api/", get(user_api))
        .route(
            "/{user}/{index}",
            get(index_get)
                .post(index_post)
                .put(index_put)
                .patch(index_put)
                .delete(index_delete)
                .fallback(index_method_fallback),
        )
        .route(
            "/{user}/{index}/",
            get(stage_index)
                .post(stage_post)
                .put(index_put)
                .patch(index_put)
                .delete(index_delete)
                .fallback(index_method_fallback),
        )
        .route("/{user}/{index}/+api", get(stage_api))
        .route("/{user}/{index}/+api/", get(stage_api))
        .route(
            "/{user}/{index}/{project}",
            get(stage_project).put(project_put).delete(delete_project),
        )
        .route(
            "/{user}/{index}/{project}/",
            get(stage_project).put(project_put).delete(delete_project),
        )
        .route(
            "/{user}/{index}/{project}/{version}",
            get(stage_version).delete(delete_version),
        )
        .route(
            "/{user}/{index}/{project}/{version}/",
            get(stage_version).delete(delete_version),
        )
        .route(
            "/{user}/{index}/{project}/{version}/{filename}",
            get(version_file_download).put(version_file_upload),
        )
        .route(
            "/{user}/{index}/{project}/{version}/{filename}/",
            get(version_file_download).put(version_file_upload),
        )
        .route("/simple", get(legacy_simple_root_no_slash))
        .route("/simple/", get(legacy_simple_root))
        .route("/simple/{project}", get(legacy_simple_project_no_slash))
        .route("/simple/{project}/", get(legacy_simple_project))
        .route("/{user}/{index}/+simple", get(stage_simple_root_no_slash))
        .route("/{user}/{index}/+simple/", get(stage_simple_root))
        .route(
            "/{user}/{index}/+simple/{project}",
            get(stage_simple_project_no_slash),
        )
        .route(
            "/{user}/{index}/+simple/{project}/",
            get(stage_simple_project),
        )
        .route(
            "/{user}/{index}/+simple/{project}/refresh",
            post(stage_simple_project_refresh),
        )
        .route(
            "/{user}/{index}/+simple/{project}/refresh/",
            post(stage_simple_project_refresh),
        )
        .route(
            "/files/{project}/{filename}",
            get(legacy_file_download)
                .put(legacy_file_upload)
                .delete(legacy_file_delete),
        )
        .route(
            "/files/{project}/{filename}/",
            get(legacy_file_download)
                .put(legacy_file_upload)
                .delete(legacy_file_delete),
        )
        .route(
            "/{user}/{index}/+f/{project}/{filename}",
            get(stage_file_download)
                .put(stage_file_upload)
                .post(stage_tox_result_upload)
                .delete(stage_file_delete),
        )
        .route(
            "/{user}/{index}/+f/{project}/{filename}/",
            get(stage_file_download)
                .put(stage_file_upload)
                .post(stage_tox_result_upload)
                .delete(stage_file_delete),
        )
        .route("/{user}/{index}/+e/{*relpath}", get(mirror_file_download))
        .layer(Extension(outside_url))
        .with_state(state)
}

async fn root(State(state): State<AppState>) -> Response {
    render_root(state).await
}

async fn status(State(state): State<AppState>) -> Response {
    let serial = state.serial.current();
    let indexes = match status_indexes(&state) {
        Ok(indexes) => indexes,
        Err(response) => return *response,
    };
    let upstream_reports = match status_upstream_reports(&state) {
        Ok(reports) => reports,
        Err(response) => return *response,
    };
    let sources = state.index.source_names();
    let response = json_value_response(
        StatusCode::OK,
        json!({
            "status": 200,
            "type": "status",
            "result": {
                "role": "MASTER",
                "server": "devpi-rs",
                "api_version": DEVPI_API_VERSION,
                "server_version": DEVPI_SERVER_VERSION,
                "serial": serial,
                "event-serial": serial,
                "event-serial-timestamp": null,
                "event-serial-in-sync-at": null,
                "sources": sources,
                "indexes": indexes,
                "upstream_reports": upstream_reports,
                "metrics": [],
                "polling_replicas": {},
                "replication-errors": {},
            },
        }),
    );
    with_current_serial(&state, response)
}

async fn changelog(State(state): State<AppState>, Path(serial): Path<String>) -> Response {
    let (serial, streaming) = if let Some(serial) = serial.strip_suffix('-') {
        (serial, true)
    } else {
        (serial.as_str(), false)
    };
    let Ok(serial) = serial.parse::<u64>() else {
        return json_message_response(StatusCode::BAD_REQUEST, "invalid changelog serial");
    };
    match state.serial.changelog_since(serial, streaming) {
        Ok(entries) => with_current_serial(
            &state,
            json_value_response(
                StatusCode::OK,
                json!({
                    "status": 200,
                    "type": "changelog",
                    "result": entries,
                }),
            ),
        ),
        Err(err) => {
            TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n")).into_response()
        }
    }
}

async fn files_download(
    State(state): State<AppState>,
    Path(relpath): Path<String>,
    headers: HeaderMap,
) -> Response {
    let auth_user = match authenticated_user(&state, &headers) {
        Ok(user) => user,
        Err(response) => return *response,
    };
    let Some(relpath) = parse_files_relpath(&relpath) else {
        return TextResponse(StatusCode::NOT_FOUND, "file not found\n".to_string()).into_response();
    };
    match relpath {
        FilesRelpath::Project {
            user,
            index,
            project,
            filename,
        } => {
            read_file(
                state,
                user,
                index,
                project,
                filename,
                FileReadOptions {
                    auth_user,
                    conditional: conditional_request(&headers),
                    ..FileReadOptions::default()
                },
            )
            .await
        }
        FilesRelpath::Hash {
            user,
            index,
            hash_prefix,
            hash_rest,
            filename,
        } => {
            read_hashed_file(
                state,
                user,
                index,
                hash_prefix,
                hash_rest,
                filename,
                FileReadOptions {
                    auth_user,
                    conditional: conditional_request(&headers),
                    ..FileReadOptions::default()
                },
            )
            .await
        }
    }
}

async fn authcheck(State(state): State<AppState>, headers: HeaderMap) -> Response {
    check_original_uri_auth(state, headers).await
}

async fn login(State(state): State<AppState>, body: Bytes) -> Response {
    authenticate_login(state, body).await
}

async fn user_get(State(state): State<AppState>, Path(user): Path<String>) -> Response {
    render_user(state, user).await
}

async fn user_put(
    State(state): State<AppState>,
    Path(user): Path<String>,
    headers: HeaderMap,
    method: Method,
    body: Bytes,
) -> Response {
    let auth_user = match authenticated_user(&state, &headers) {
        Ok(user) => user,
        Err(response) => return *response,
    };
    put_user_config(state, user, body, auth_user, method == Method::PUT).await
}

async fn user_delete(
    State(state): State<AppState>,
    Path(user): Path<String>,
    headers: HeaderMap,
) -> Response {
    let auth_user = match authenticated_user(&state, &headers) {
        Ok(user) => user,
        Err(response) => return *response,
    };
    remove_user(state, user, auth_user).await
}

async fn index_get(
    State(state): State<AppState>,
    Path((user, index)): Path<(String, String)>,
    Query(query): Query<BTreeMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    if simple_format(&headers) == SimpleFormat::Json {
        let auth_user = match authenticated_user(&state, &headers) {
            Ok(user) => user,
            Err(response) => return *response,
        };
        return render_simple_root_with_format(state, user, index, SimpleFormat::Json, auth_user)
            .await;
    }
    render_index_config(state, user, index, !query.contains_key("no_projects")).await
}

async fn index_put(
    State(state): State<AppState>,
    Path((user, index)): Path<(String, String)>,
    Query(query): Query<BTreeMap<String, String>>,
    headers: HeaderMap,
    method: Method,
    body: Bytes,
) -> Response {
    let auth_user = match authenticated_user(&state, &headers) {
        Ok(user) => user,
        Err(response) => return *response,
    };
    put_index_config(
        state,
        user,
        index,
        body,
        auth_user,
        method == Method::PUT,
        query.contains_key("error_on_noop"),
    )
    .await
}

async fn index_post(
    State(state): State<AppState>,
    Path((user, index)): Path<(String, String)>,
    request: Request,
) -> Response {
    handle_stage_post(state, user, index, request).await
}

async fn index_method_fallback(
    State(state): State<AppState>,
    Path((user, index)): Path<(String, String)>,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if method.as_str() == "PUSH" {
        let auth_user = match authenticated_user(&state, &headers) {
            Ok(user) => user,
            Err(response) => return *response,
        };
        return push_release(state, user, index, body, auth_user).await;
    }
    TextResponse(
        StatusCode::METHOD_NOT_ALLOWED,
        "method not allowed\n".to_string(),
    )
    .into_response()
}

async fn index_delete(
    State(state): State<AppState>,
    Path((user, index)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let auth_user = match authenticated_user(&state, &headers) {
        Ok(user) => user,
        Err(response) => return *response,
    };
    remove_index_config(state, user, index, auth_user).await
}

async fn stage_index(
    State(state): State<AppState>,
    Path((user, index)): Path<(String, String)>,
    Query(query): Query<BTreeMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    if simple_format(&headers) == SimpleFormat::Json {
        let auth_user = match authenticated_user(&state, &headers) {
            Ok(user) => user,
            Err(response) => return *response,
        };
        return render_simple_root_with_format(state, user, index, SimpleFormat::Json, auth_user)
            .await;
    }
    render_stage_index_with_projects(state, user, index, !query.contains_key("no_projects")).await
}

async fn stage_post(
    State(state): State<AppState>,
    Path((user, index)): Path<(String, String)>,
    request: Request,
) -> Response {
    handle_stage_post(state, user, index, request).await
}

async fn handle_stage_post(
    state: AppState,
    user: String,
    index: String,
    request: Request,
) -> Response {
    let headers = request.headers().clone();
    let auth_user = match authenticated_user(&state, &headers) {
        Ok(user) => user,
        Err(response) => return *response,
    };
    if is_multipart_request(&headers) {
        let multipart = match Multipart::from_request(request, &state).await {
            Ok(multipart) => multipart,
            Err(err) => {
                return TextResponse(StatusCode::BAD_REQUEST, format!("bad multipart: {err}\n"))
                    .into_response();
            }
        };
        handle_stage_submit(state, user, index, auth_user, multipart).await
    } else {
        let body = match to_bytes(request.into_body(), usize::MAX).await {
            Ok(body) => body,
            Err(err) => {
                return TextResponse(StatusCode::BAD_REQUEST, format!("bad body: {err}\n"))
                    .into_response();
            }
        };
        if is_urlencoded_request(&headers) {
            handle_stage_form_submit(state, user, index, auth_user, body).await
        } else {
            push_release(state, user, index, body, auth_user).await
        }
    }
}

async fn root_api(
    State(state): State<AppState>,
    Extension(outside_url): Extension<OutsideUrl>,
    headers: HeaderMap,
) -> Response {
    render_api(state, None, headers, outside_url.0).await
}

async fn user_api(
    State(state): State<AppState>,
    Extension(outside_url): Extension<OutsideUrl>,
    Path(user): Path<String>,
    headers: HeaderMap,
) -> Response {
    render_api(state, Some((user, None)), headers, outside_url.0).await
}

async fn stage_api(
    State(state): State<AppState>,
    Extension(outside_url): Extension<OutsideUrl>,
    Path((user, index)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    if !valid_stage_segment(&user) || !valid_stage_segment(&index) {
        return TextResponse(StatusCode::BAD_REQUEST, "invalid stage\n".to_string())
            .into_response();
    }
    render_api(state, Some((user, Some(index))), headers, outside_url.0).await
}

async fn stage_project(
    State(state): State<AppState>,
    Path((user, index, project)): Path<(String, String, String)>,
    Query(query): Query<BTreeMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let auth_user = match authenticated_user(&state, &headers) {
        Ok(user) => user,
        Err(response) => return *response,
    };
    render_stage_project(
        state,
        user,
        index,
        project,
        !query.contains_key("ignore_bases"),
        auth_user,
    )
    .await
}

async fn project_put(
    State(state): State<AppState>,
    Path((user, index, project)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Response {
    let auth_user = match authenticated_user(&state, &headers) {
        Ok(user) => user,
        Err(response) => return *response,
    };
    register_project(state, user, index, project, auth_user).await
}

async fn delete_project(
    State(state): State<AppState>,
    Path((user, index, project)): Path<(String, String, String)>,
    Query(query): Query<BTreeMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let auth_user = match authenticated_user(&state, &headers) {
        Ok(user) => user,
        Err(response) => return *response,
    };
    remove_project(
        state,
        user,
        index,
        project,
        auth_user,
        query.contains_key("force"),
    )
    .await
}

async fn delete_version(
    State(state): State<AppState>,
    Path((user, index, project, version)): Path<(String, String, String, String)>,
    Query(query): Query<BTreeMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let auth_user = match authenticated_user(&state, &headers) {
        Ok(user) => user,
        Err(response) => return *response,
    };
    remove_version(
        state,
        user,
        index,
        project,
        version,
        auth_user,
        query.contains_key("force"),
    )
    .await
}

async fn legacy_simple_root(State(state): State<AppState>, headers: HeaderMap) -> Response {
    render_legacy_simple_root_with_format_conditional(
        state,
        simple_format(&headers),
        conditional_request(&headers),
    )
    .await
}

async fn legacy_simple_root_no_slash(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    if should_redirect_simple_html(&headers) {
        return redirect_response("/simple/");
    }
    render_legacy_simple_root_with_format_conditional(
        state,
        simple_format(&headers),
        conditional_request(&headers),
    )
    .await
}

async fn legacy_simple_project(
    State(state): State<AppState>,
    Path(project): Path<String>,
    headers: HeaderMap,
) -> Response {
    render_legacy_simple_project_with_format_conditional(
        state,
        project,
        simple_format(&headers),
        conditional_request(&headers),
    )
    .await
}

async fn legacy_simple_project_no_slash(
    State(state): State<AppState>,
    Path(project): Path<String>,
    headers: HeaderMap,
) -> Response {
    if should_redirect_simple_html(&headers) {
        return redirect_response(&format!("/simple/{}/", normalize_project_name(&project)));
    }
    render_legacy_simple_project_with_format_conditional(
        state,
        project,
        simple_format(&headers),
        conditional_request(&headers),
    )
    .await
}

async fn stage_simple_root(
    State(state): State<AppState>,
    Path((user, index)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let format = simple_format(&headers);
    let auth_user = match authenticated_user(&state, &headers) {
        Ok(user) => user,
        Err(response) => return *response,
    };
    render_simple_root_with_format_conditional(
        state,
        user,
        index,
        format,
        auth_user,
        conditional_request(&headers),
    )
    .await
}

async fn stage_simple_root_no_slash(
    State(state): State<AppState>,
    Path((user, index)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    if should_redirect_simple_html(&headers) {
        return redirect_response(&format!("/{user}/{index}/+simple/"));
    }
    let format = simple_format(&headers);
    let auth_user = match authenticated_user(&state, &headers) {
        Ok(user) => user,
        Err(response) => return *response,
    };
    render_simple_root_with_format_conditional(
        state,
        user,
        index,
        format,
        auth_user,
        conditional_request(&headers),
    )
    .await
}

async fn stage_simple_project(
    State(state): State<AppState>,
    Path((user, index, project)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Response {
    let format = simple_format(&headers);
    let auth_user = match authenticated_user(&state, &headers) {
        Ok(user) => user,
        Err(response) => return *response,
    };
    render_simple_project_with_format_conditional(
        state,
        user,
        index,
        project,
        format,
        auth_user,
        SimpleRenderOptions {
            include_refresh_form: should_embed_simple_refresh_form(&headers),
            conditional: conditional_request(&headers),
        },
    )
    .await
}

async fn stage_simple_project_no_slash(
    State(state): State<AppState>,
    Path((user, index, project)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Response {
    if should_redirect_simple_html(&headers) {
        return redirect_response(&format!(
            "/{user}/{index}/+simple/{}/",
            normalize_project_name(&project)
        ));
    }
    let format = simple_format(&headers);
    let auth_user = match authenticated_user(&state, &headers) {
        Ok(user) => user,
        Err(response) => return *response,
    };
    render_simple_project_with_format_conditional(
        state,
        user,
        index,
        project,
        format,
        auth_user,
        SimpleRenderOptions {
            include_refresh_form: should_embed_simple_refresh_form(&headers),
            conditional: conditional_request(&headers),
        },
    )
    .await
}

async fn stage_simple_project_refresh(
    State(state): State<AppState>,
    Path((user, index, project)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Response {
    let auth_user = match authenticated_user(&state, &headers) {
        Ok(user) => user,
        Err(response) => return *response,
    };
    refresh_simple_project(state, user, index, project, auth_user).await
}

async fn legacy_file_upload(
    State(state): State<AppState>,
    Path((project, filename)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    save_file(
        state,
        SaveFileRequest {
            user: "root".to_string(),
            index: "pypi".to_string(),
            project,
            filename,
            body,
            metadata: BTreeMap::new(),
            auth_user: None,
        },
    )
    .await
}

async fn legacy_file_delete(
    State(state): State<AppState>,
    Path((project, filename)): Path<(String, String)>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    delete_file(
        state,
        "root".to_string(),
        "pypi".to_string(),
        project,
        filename,
        None,
        query.contains_key("force"),
    )
    .await
}

async fn stage_file_upload(
    State(state): State<AppState>,
    Path((user, index, project, filename)): Path<(String, String, String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let auth_user = match authenticated_user(&state, &headers) {
        Ok(user) => user,
        Err(response) => return *response,
    };
    save_file(
        state,
        SaveFileRequest {
            user,
            index,
            project,
            filename,
            body,
            metadata: BTreeMap::new(),
            auth_user,
        },
    )
    .await
}

async fn stage_file_delete(
    State(state): State<AppState>,
    Path((user, index, project, filename)): Path<(String, String, String, String)>,
    Query(query): Query<BTreeMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let auth_user = match authenticated_user(&state, &headers) {
        Ok(user) => user,
        Err(response) => return *response,
    };
    delete_file(
        state,
        user,
        index,
        project,
        filename,
        auth_user,
        query.contains_key("force"),
    )
    .await
}

async fn stage_tox_result_upload(
    State(state): State<AppState>,
    Path((user, index, project, filename)): Path<(String, String, String, String)>,
    Extension(outside_url): Extension<OutsideUrl>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let auth_user = match authenticated_user(&state, &headers) {
        Ok(user) => user,
        Err(response) => return *response,
    };
    let outside_url = effective_outside_url(&headers, outside_url.0.as_deref());
    store_tox_result(
        state,
        StoreToxResultRequest {
            user,
            index,
            project,
            filename,
            body,
            auth_user,
            outside_url,
        },
    )
    .await
}

async fn legacy_file_download(
    State(state): State<AppState>,
    Path((project, filename)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    read_file(
        state,
        "root".to_string(),
        "pypi".to_string(),
        project,
        filename,
        FileReadOptions {
            json_preferred: wants_json_response(&headers),
            conditional: conditional_request(&headers),
            ..FileReadOptions::default()
        },
    )
    .await
}

async fn stage_file_download(
    State(state): State<AppState>,
    Path((user, index, project, filename)): Path<(String, String, String, String)>,
    headers: HeaderMap,
) -> Response {
    let auth_user = match authenticated_user(&state, &headers) {
        Ok(user) => user,
        Err(response) => return *response,
    };
    read_file(
        state,
        user,
        index,
        project,
        filename,
        FileReadOptions {
            json_preferred: wants_json_response(&headers),
            auth_user,
            conditional: conditional_request(&headers),
        },
    )
    .await
}

async fn mirror_file_download(
    State(state): State<AppState>,
    Path((user, index, relpath)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Response {
    let auth_user = match authenticated_user(&state, &headers) {
        Ok(user) => user,
        Err(response) => return *response,
    };
    read_mirror_file(
        state,
        user,
        index,
        relpath,
        FileReadOptions {
            auth_user,
            conditional: conditional_request(&headers),
            ..FileReadOptions::default()
        },
    )
    .await
}

async fn version_file_upload(
    State(state): State<AppState>,
    Path((user, index, project, version, filename)): Path<(String, String, String, String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !filename.contains(&version) {
        return TextResponse(
            StatusCode::BAD_REQUEST,
            format!("filename {filename:?} does not contain version {version:?}\n"),
        )
        .into_response();
    }
    let auth_user = match authenticated_user(&state, &headers) {
        Ok(user) => user,
        Err(response) => return *response,
    };
    let metadata = BTreeMap::from([("version".to_string(), version)]);
    save_file(
        state,
        SaveFileRequest {
            user,
            index,
            project,
            filename,
            body,
            metadata,
            auth_user,
        },
    )
    .await
}

async fn version_file_download(
    State(state): State<AppState>,
    Path((user, index, project, version, filename)): Path<(String, String, String, String, String)>,
    headers: HeaderMap,
) -> Response {
    if !filename.contains(&version) {
        return TextResponse(
            StatusCode::BAD_REQUEST,
            format!("filename {filename:?} does not contain version {version:?}\n"),
        )
        .into_response();
    }
    let auth_user = match authenticated_user(&state, &headers) {
        Ok(user) => user,
        Err(response) => return *response,
    };
    read_file(
        state,
        user,
        index,
        project,
        filename,
        FileReadOptions {
            json_preferred: wants_json_response(&headers),
            auth_user,
            conditional: conditional_request(&headers),
        },
    )
    .await
}

async fn stage_version(
    State(state): State<AppState>,
    Path((user, index, project, version)): Path<(String, String, String, String)>,
    Query(query): Query<BTreeMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let auth_user = match authenticated_user(&state, &headers) {
        Ok(user) => user,
        Err(response) => return *response,
    };
    render_version_metadata(
        state,
        user,
        index,
        project,
        version,
        !query.contains_key("ignore_bases"),
        auth_user,
    )
    .await
}

async fn render_user(state: AppState, user: String) -> Response {
    blocking(move || match state.registry.public_user(&user) {
        Ok(Some(config)) => {
            let response = json_value_response(
                StatusCode::OK,
                json!({"status": 200, "type": "userconfig", "result": config}),
            );
            with_current_serial(&state, response)
        }
        Ok(None) => {
            TextResponse(StatusCode::NOT_FOUND, "user not found\n".to_string()).into_response()
        }
        Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
            TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response()
        }
        Err(err) => {
            TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n")).into_response()
        }
    })
    .await
}

async fn put_user_config(
    state: AppState,
    user: String,
    body: Bytes,
    auth_user: Option<String>,
    create_only: bool,
) -> Response {
    blocking(move || {
        let input = match parse_json_or_default::<UserInput>(&body) {
            Ok(input) => input,
            Err(err) => {
                return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
            }
        };
        let creating = match state.registry.user(&user) {
            Ok(Some(_)) => {
                if create_only {
                    return json_message_response(StatusCode::CONFLICT, "user already exists");
                }
                if let Err(response) = ensure_user_modify_allowed(&user, auth_user.as_deref()) {
                    return *response;
                }
                false
            }
            Ok(None) => {
                if !create_only {
                    return TextResponse(StatusCode::NOT_FOUND, "user not found\n".to_string())
                        .into_response();
                }
                true
            }
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
            }
            Err(err) => {
                return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                    .into_response();
            }
        };
        if creating && input.password.is_none() {
            return json_message_response(StatusCode::BAD_REQUEST, "password needs to be set");
        }
        let password_update = (!creating).then(|| input.password.clone()).flatten();
        match state.registry.put_user(&user, input) {
            Ok((created, config)) => {
                let status = if created {
                    StatusCode::CREATED
                } else {
                    StatusCode::OK
                };
                if let Some(password) = password_update {
                    let token = match state.registry.issue_proxy_token(&user, &password) {
                        Ok(Some(token)) => token,
                        Ok(None) => {
                            return json_message_response(
                                StatusCode::UNAUTHORIZED,
                                format!("user {user:?} could not be authenticated"),
                            );
                        }
                        Err(err) => {
                            return TextResponse(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                format!("{err}\n"),
                            )
                            .into_response();
                        }
                    };
                    let response = json_value_response(
                        StatusCode::OK,
                        json!({
                            "status": 200,
                            "message": "user updated, new proxy auth",
                            "type": "userpassword",
                            "result": {
                                "password": token,
                                "expiration": LOGIN_EXPIRATION_SECS
                            }
                        }),
                    );
                    return bump_serial_response_with_event(
                        &state,
                        response,
                        json!({"event": "user_modify", "user": user}),
                    );
                }
                let response = json_value_response(
                    status,
                    json!({"status": status.as_u16(), "type": "userconfig", "result": crate::registry::PublicUserConfig::from(config)}),
                );
                let event = if created {
                    json!({"event": "user_create", "user": user})
                } else {
                    json!({"event": "user_modify", "user": user})
                };
                bump_serial_response_with_event(&state, response, event)
            }
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response()
            }
            Err(err) => {
                TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n")).into_response()
            }
        }
    })
    .await
}

async fn remove_user(state: AppState, user: String, auth_user: Option<String>) -> Response {
    blocking(move || {
        match state.registry.user(&user) {
            Ok(Some(_)) => {
                if let Err(response) = ensure_user_modify_allowed(&user, auth_user.as_deref()) {
                    return *response;
                }
            }
            Ok(None) => {
                return TextResponse(StatusCode::NOT_FOUND, "user not found\n".to_string())
                    .into_response();
            }
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
            }
            Err(err) => {
                return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                    .into_response();
            }
        }
        if let Err(err) = state.local.tombstone_user_files(&user) {
            return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                .into_response();
        }
        match state.registry.delete_user(&user) {
            Ok(true) => {
                let response = json_value_response(
                    StatusCode::OK,
                    json!({"status": 200, "message": format!("user '{user}' deleted")}),
                );
                bump_serial_response_with_event(
                    &state,
                    response,
                    json!({"event": "user_delete", "user": user}),
                )
            }
            Ok(false) => {
                TextResponse(StatusCode::NOT_FOUND, "user not found\n".to_string()).into_response()
            }
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response()
            }
            Err(err) if err.kind() == io::ErrorKind::PermissionDenied => {
                TextResponse(StatusCode::FORBIDDEN, format!("{err}\n")).into_response()
            }
            Err(err) => {
                TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n")).into_response()
            }
        }
    })
    .await
}

async fn render_index_config(
    state: AppState,
    user: String,
    index: String,
    include_projects: bool,
) -> Response {
    blocking(move || match state.registry.index(&user, &index) {
        Ok(Some(config)) => {
            let resolved_sources = if config.sources.is_empty() {
                state.index.source_names()
            } else {
                config.sources.clone()
            };
            let mut result = serde_json::to_value(config).unwrap_or_else(|_| json!({}));
            result["resolved_sources"] = json!(resolved_sources);
            if include_projects {
                match state.local.projects_in(&user, &index) {
                    Ok(projects) => result["projects"] = json!(projects),
                    Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                        return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n"))
                            .into_response();
                    }
                    Err(err) => {
                        return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                            .into_response();
                    }
                }
            }
            let response = json_value_response(
                StatusCode::OK,
                json!({"status": 200, "type": "indexconfig", "result": result}),
            );
            with_current_serial(&state, response)
        }
        Ok(None) => {
            TextResponse(StatusCode::NOT_FOUND, "index not found\n".to_string()).into_response()
        }
        Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
            TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response()
        }
        Err(err) => {
            TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n")).into_response()
        }
    })
    .await
}

async fn put_index_config(
    state: AppState,
    user: String,
    index: String,
    body: Bytes,
    auth_user: Option<String>,
    create_only: bool,
    error_on_noop: bool,
) -> Response {
    blocking(move || {
        let value = match parse_json_value_or_default(&body) {
            Ok(value) => value,
            Err(err) => {
                return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
            }
        };
        let existing_config = match state.registry.index(&user, &index) {
            Ok(Some(config)) => {
                if create_only {
                    return json_message_response(
                        StatusCode::CONFLICT,
                        format!("index {user}/{index} already exists"),
                    );
                }
                if let Err(response) =
                    ensure_index_modify_allowed(&user, &index, auth_user.as_deref())
                {
                    return *response;
                }
                Some(config)
            }
            Ok(None) => {
                if !create_only {
                    return TextResponse(StatusCode::NOT_FOUND, "index not found\n".to_string())
                        .into_response();
                }
                if let Err(response) = ensure_index_create_allowed(&user, auth_user.as_deref()) {
                    return *response;
                }
                None
            }
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
            }
            Err(err) => {
                return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                    .into_response();
            }
        };
        let input = match index_input_from_value(value, existing_config.as_ref()) {
            Ok(input) => input,
            Err(err) => {
                return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
            }
        };
        if let Err(response) = ensure_known_sources(&state, input.sources.as_deref()) {
            return *response;
        }
        match state.registry.put_index(&user, &index, input) {
            Ok((created, config)) => {
                if error_on_noop
                    && !created
                    && existing_config
                        .as_ref()
                        .is_some_and(|existing| existing == &config)
                {
                    return json_message_response(
                        StatusCode::BAD_REQUEST,
                        "The requested modifications resulted in no changes",
                    );
                }
                let status = if created {
                    StatusCode::CREATED
                } else {
                    StatusCode::OK
                };
                let response = json_value_response(
                    status,
                    json!({"status": status.as_u16(), "type": "indexconfig", "result": config}),
                );
                let stage = format!("{user}/{index}");
                let event = if created {
                    json!({"event": "index_create", "stage": stage})
                } else {
                    json!({"event": "index_modify", "stage": stage})
                };
                bump_serial_response_with_event(&state, response, event)
            }
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response()
            }
            Err(err) => {
                TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n")).into_response()
            }
        }
    })
    .await
}

async fn remove_index_config(
    state: AppState,
    user: String,
    index: String,
    auth_user: Option<String>,
) -> Response {
    blocking(move || {
        match state.registry.index(&user, &index) {
            Ok(Some(_)) => {
                if let Err(response) =
                    ensure_index_modify_allowed(&user, &index, auth_user.as_deref())
                {
                    return *response;
                }
            }
            Ok(None) => {
                return TextResponse(StatusCode::NOT_FOUND, "index not found\n".to_string())
                    .into_response();
            }
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
            }
            Err(err) => {
                return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                    .into_response();
            }
        }
        if let Err(err) = state.local.tombstone_stage_files(&user, &index) {
            return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                .into_response();
        }
        match state.registry.delete_index(&user, &index) {
            Ok(true) => {
                let response = json_value_response(
                    StatusCode::CREATED,
                    json!({"status": 201, "message": format!("index {user}/{index} deleted")}),
                );
                bump_serial_response_with_event(
                    &state,
                    response,
                    json!({"event": "index_delete", "stage": format!("{user}/{index}")}),
                )
            }
            Ok(false) => {
                TextResponse(StatusCode::NOT_FOUND, "index not found\n".to_string()).into_response()
            }
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response()
            }
            Err(err) if err.kind() == io::ErrorKind::PermissionDenied => {
                TextResponse(StatusCode::FORBIDDEN, format!("{err}\n")).into_response()
            }
            Err(err) => {
                TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n")).into_response()
            }
        }
    })
    .await
}

async fn render_root(state: AppState) -> Response {
    blocking(move || match state.registry.public_users() {
        Ok(users) => {
            let response = json_value_response(
                StatusCode::OK,
                json!({"status": 200, "type": "list:userconfig", "result": users}),
            );
            with_current_serial(&state, response)
        }
        Err(err) => {
            TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n")).into_response()
        }
    })
    .await
}

async fn render_api(
    state: AppState,
    route: Option<(String, Option<String>)>,
    headers: HeaderMap,
    outside_url: Option<String>,
) -> Response {
    blocking(move || {
        let authstatus = match api_authstatus(&state, &headers) {
            Ok(authstatus) => authstatus,
            Err(response) => return *response,
        };
        let outside_url = effective_outside_url(&headers, outside_url.as_deref());
        let mut result = json!({
            "login": api_url(outside_url.as_deref(), "/+login"),
            "authstatus": authstatus,
            "features": [
                "server-keyvalue-parsing",
                "push-no-docs",
                "push-only-docs",
                "push-register-project",
                "multi-source"
            ],
        });

        if let Some((user, index)) = route {
            if !valid_stage_segment(&user) {
                return TextResponse(StatusCode::BAD_REQUEST, "invalid user\n".to_string())
                    .into_response();
            }
            match state.registry.user(&user) {
                Ok(Some(_)) => {}
                Ok(None) => {
                    return TextResponse(StatusCode::NOT_FOUND, "user not found\n".to_string())
                        .into_response();
                }
                Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                    return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n"))
                        .into_response();
                }
                Err(err) => {
                    return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                        .into_response();
                }
            }
            if let Some(index) = index {
                if !valid_stage_segment(&index) {
                    return TextResponse(StatusCode::BAD_REQUEST, "invalid stage\n".to_string())
                        .into_response();
                }
                let ixconfig = match state.registry.index(&user, &index) {
                    Ok(Some(config)) => config,
                    Ok(None) => {
                        return TextResponse(
                            StatusCode::NOT_FOUND,
                            "index not found\n".to_string(),
                        )
                        .into_response();
                    }
                    Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                        return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n"))
                            .into_response();
                    }
                    Err(err) => {
                        return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                            .into_response();
                    }
                };
                let index_url = match outside_url.as_deref() {
                    Some(base) => api_url(Some(base), &format!("/{user}/{index}")),
                    None => format!("/{user}/{index}/"),
                };
                result["simpleindex"] = json!(api_url(
                    outside_url.as_deref(),
                    &format!("/{user}/{index}/+simple/")
                ));
                result["index"] = json!(index_url);
                if ixconfig.index_type != "mirror" {
                    result["pypisubmit"] = json!(api_url(
                        outside_url.as_deref(),
                        &format!("/{user}/{index}/")
                    ));
                }
            }
        }

        let response = json_value_response(
            StatusCode::OK,
            json!({"status": 200, "type": "apiconfig", "result": result}),
        );
        with_current_serial(&state, response)
    })
    .await
}

fn effective_outside_url(headers: &HeaderMap, configured: Option<&str>) -> Option<String> {
    if let Some(value) = headers
        .get("x-outside-url")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Some(value.to_string());
    }
    if let Some(value) = configured.map(str::trim).filter(|value| !value.is_empty()) {
        return Some(value.to_string());
    }
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("http");
    Some(format!("{scheme}://{host}"))
}

fn api_url(outside_url: Option<&str>, path: &str) -> String {
    match outside_url {
        Some(base) => format!("{}{}", base.trim_end_matches('/'), path),
        None => path.to_string(),
    }
}

async fn authenticate_login(state: AppState, body: Bytes) -> Response {
    blocking(move || {
        let input = match parse_json_or_default::<LoginRequest>(&body) {
            Ok(input) => input,
            Err(err) => {
                return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
            }
        };
        let Some(user) = input.user else {
            return TextResponse(
                StatusCode::BAD_REQUEST,
                "Bad request: no user/password specified\n".to_string(),
            )
            .into_response();
        };
        let Some(password) = input.password else {
            return TextResponse(
                StatusCode::BAD_REQUEST,
                "Bad request: no user/password specified\n".to_string(),
            )
            .into_response();
        };
        match state.registry.issue_proxy_token(&user, &password) {
            Ok(Some(token)) => json_value_response(
                StatusCode::OK,
                json!({
                    "status": 200,
                    "message": "login successful",
                    "type": "proxyauth",
                    "result": {
                        "password": token,
                        "expiration": LOGIN_EXPIRATION_SECS
                    }
                }),
            ),
            Ok(None) => json_value_response(
                StatusCode::UNAUTHORIZED,
                json!({
                    "status": 401,
                    "message": format!("user {user:?} could not be authenticated")
                }),
            ),
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => json_value_response(
                StatusCode::UNAUTHORIZED,
                json!({
                    "status": 401,
                    "message": format!("user {user:?} could not be authenticated")
                }),
            ),
            Err(err) => {
                TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n")).into_response()
            }
        }
    })
    .await
}

async fn check_original_uri_auth(state: AppState, headers: HeaderMap) -> Response {
    blocking(move || {
        let Some(original_uri) = original_uri_path(&headers) else {
            return TextResponse(StatusCode::OK, String::new()).into_response();
        };
        match authcheck_target(&original_uri) {
            AuthcheckTarget::AlwaysOk | AuthcheckTarget::Known => {
                TextResponse(StatusCode::OK, String::new()).into_response()
            }
            AuthcheckTarget::Unknown => {
                TextResponse(StatusCode::FORBIDDEN, "unknown route\n".to_string()).into_response()
            }
            AuthcheckTarget::PackageRead { user, index } => {
                match state.registry.index(&user, &index) {
                    Ok(Some(_)) => {}
                    Ok(None) => {
                        return TextResponse(
                            StatusCode::FORBIDDEN,
                            "index not found\n".to_string(),
                        )
                        .into_response();
                    }
                    Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                        return TextResponse(StatusCode::FORBIDDEN, format!("{err}\n"))
                            .into_response();
                    }
                    Err(err) => {
                        return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                            .into_response();
                    }
                }
                let auth_user = match authenticated_user(&state, &headers) {
                    Ok(auth_user) => auth_user,
                    Err(response) => return *response,
                };
                match pkg_read_allowed(&state, &user, &index, auth_user.as_deref()) {
                    Ok(true) => TextResponse(StatusCode::OK, String::new()).into_response(),
                    Ok(false) => TextResponse(
                        StatusCode::FORBIDDEN,
                        "package read forbidden\n".to_string(),
                    )
                    .into_response(),
                    Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                        TextResponse(StatusCode::FORBIDDEN, format!("{err}\n")).into_response()
                    }
                    Err(err) => TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                        .into_response(),
                }
            }
        }
    })
    .await
}

async fn save_file(state: AppState, request: SaveFileRequest) -> Response {
    blocking(move || {
        let SaveFileRequest {
            user,
            index,
            project,
            filename,
            body,
            mut metadata,
            auth_user,
        } = request;
        if let Err(response) = ensure_index_exists(&state, &user, &index) {
            return *response;
        }
        let ixconfig = match index_config_for_stage(&state, &user, &index) {
            Ok(config) => config,
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
            }
            Err(err) => {
                return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                    .into_response();
            }
        };
        if let Err(response) =
            ensure_acl_upload_allowed(&state, &user, &index, auth_user.as_deref())
        {
            return *response;
        }

        let overwrote_existing = match state
            .local
            .file_exists_in(&user, &index, &project, &filename)
        {
            Ok(true) if !ixconfig.volatile => {
                let existing = match state.local.read_in(&user, &index, &project, &filename) {
                    Ok(existing) => existing,
                    Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                        return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n"))
                            .into_response();
                    }
                    Err(err) => {
                        return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                            .into_response();
                    }
                };
                if existing.as_slice() == body.as_ref() {
                    return json_value_response(
                        StatusCode::OK,
                        json!({
                            "status": 200,
                            "user": user,
                            "index": index,
                            "project": normalize_project_name(&project),
                            "filename": filename,
                            "bytes": existing.len(),
                            "identical": true,
                        }),
                    );
                }
                return TextResponse(
                    StatusCode::CONFLICT,
                    format!("{filename} already exists in non-volatile index\n"),
                )
                .into_response();
            }
            Ok(exists) => exists,
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
            }
            Err(err) => {
                return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                    .into_response();
            }
        };

        let previous_file_metadata = if overwrote_existing {
            match state
                .local
                .project_file_metadata_in(&user, &index, &project)
            {
                Ok(metadata) => metadata.get(&filename).cloned(),
                Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                    return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n"))
                        .into_response();
                }
                Err(err) => {
                    return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                        .into_response();
                }
            }
        } else {
            None
        };

        match state
            .local
            .save_in(&user, &index, &project, &filename, &body)
        {
            Ok(stored) => {
                if let Some(extracted) = extracted_file_metadata(&stored.filename, &body) {
                    for (key, value) in extracted {
                        metadata.entry(key).or_insert(value);
                    }
                }
                if !is_doczip_metadata(&stored.filename, &metadata) {
                    metadata.entry("requires_python".to_string()).or_default();
                }
                let log = upload_log_entries(
                    auth_user.as_deref(),
                    &stored.user,
                    &stored.index,
                    previous_file_metadata.as_ref(),
                    overwrote_existing,
                );
                if let Err(err) = state.local.save_file_metadata_with_log_in(
                    &stored.user,
                    &stored.index,
                    &stored.project,
                    &stored.filename,
                    metadata,
                    log,
                ) {
                    return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                        .into_response();
                }
                let response = json_value_response(
                    StatusCode::CREATED,
                    json!({
                        "status": 201,
                        "user": stored.user,
                        "index": stored.index,
                        "project": stored.project,
                        "filename": stored.filename,
                        "bytes": stored.bytes,
                    }),
                );
                bump_serial_response_with_event(
                    &state,
                    response,
                    json!({
                        "event": "file_upload",
                        "stage": format!("{}/{}", stored.user, stored.index),
                        "project": stored.project,
                        "filename": stored.filename,
                    }),
                )
            }
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response()
            }
            Err(err) => {
                TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n")).into_response()
            }
        }
    })
    .await
}

async fn handle_stage_submit(
    state: AppState,
    user: String,
    index: String,
    auth_user: Option<String>,
    mut multipart: Multipart,
) -> Response {
    if !valid_stage_segment(&user) || !valid_stage_segment(&index) {
        return TextResponse(StatusCode::BAD_REQUEST, "invalid stage\n".to_string())
            .into_response();
    }

    let mut action = None;
    let mut name = None;
    let mut version = None;
    let mut content_filename = None;
    let mut content = None;
    let mut metadata = BTreeMap::new();

    while let Some(field) = match multipart.next_field().await {
        Ok(field) => field,
        Err(err) => {
            return TextResponse(StatusCode::BAD_REQUEST, format!("bad multipart: {err}\n"))
                .into_response();
        }
    } {
        let field_name = field.name().unwrap_or_default().to_string();
        match field_name.as_str() {
            ":action" => match field.text().await {
                Ok(value) => action = Some(value),
                Err(err) => {
                    return TextResponse(StatusCode::BAD_REQUEST, format!("bad action: {err}\n"))
                        .into_response();
                }
            },
            "name" => match field.text().await {
                Ok(value) => name = Some(value),
                Err(err) => {
                    return TextResponse(StatusCode::BAD_REQUEST, format!("bad name: {err}\n"))
                        .into_response();
                }
            },
            "version" => match field.text().await {
                Ok(value) => version = Some(value),
                Err(err) => {
                    return TextResponse(StatusCode::BAD_REQUEST, format!("bad version: {err}\n"))
                        .into_response();
                }
            },
            "content" => {
                content_filename = field.file_name().map(ToString::to_string);
                match field.bytes().await {
                    Ok(bytes) => content = Some(bytes),
                    Err(err) => {
                        return TextResponse(
                            StatusCode::BAD_REQUEST,
                            format!("bad content: {err}\n"),
                        )
                        .into_response();
                    }
                }
            }
            _ => match field.text().await {
                Ok(value) => {
                    insert_submit_metadata(&mut metadata, field_name, value);
                }
                Err(err) => {
                    return TextResponse(
                        StatusCode::BAD_REQUEST,
                        format!("bad metadata field: {err}\n"),
                    )
                    .into_response();
                }
            },
        }
    }

    let Some(action) = action else {
        return TextResponse(
            StatusCode::BAD_REQUEST,
            ":action field not found\n".to_string(),
        )
        .into_response();
    };
    if action == "submit" {
        let Some(name) = name else {
            return TextResponse(
                StatusCode::BAD_REQUEST,
                "name field not found\n".to_string(),
            )
            .into_response();
        };
        let Some(version) = version else {
            return TextResponse(
                StatusCode::BAD_REQUEST,
                "version field not found\n".to_string(),
            )
            .into_response();
        };
        metadata.insert("name".to_string(), name.clone());
        metadata.insert("version".to_string(), version.clone());
        return register_version_metadata(state, user, index, name, version, metadata, auth_user)
            .await;
    }
    if action != "file_upload" && action != "doc_upload" {
        return TextResponse(
            StatusCode::BAD_REQUEST,
            format!("action {action:?} not supported\n"),
        )
        .into_response();
    }

    let Some(name) = name else {
        return TextResponse(
            StatusCode::BAD_REQUEST,
            "name field not found\n".to_string(),
        )
        .into_response();
    };
    let Some(version) = version else {
        return TextResponse(
            StatusCode::BAD_REQUEST,
            "version field not found\n".to_string(),
        )
        .into_response();
    };
    let Some(content) = content else {
        return TextResponse(
            StatusCode::BAD_REQUEST,
            "content file field not found\n".to_string(),
        )
        .into_response();
    };

    let filename = if action == "doc_upload" {
        format!("{}-{version}.doc.zip", normalize_project_name(&name))
    } else {
        let Some(filename) = content_filename else {
            return TextResponse(
                StatusCode::BAD_REQUEST,
                "content file field not found\n".to_string(),
            )
            .into_response();
        };
        filename
    };

    if action == "file_upload" {
        if !is_valid_release_filename(&filename) {
            return TextResponse(
                StatusCode::BAD_REQUEST,
                format!("filename {filename:?} is not a valid release file\n"),
            )
            .into_response();
        }
        if !version.is_empty() && !filename_version_matches(&name, &filename, &version) {
            return TextResponse(
                StatusCode::BAD_REQUEST,
                format!("filename {filename:?} does not contain version {version:?}\n"),
            )
            .into_response();
        }
    }

    metadata.insert("name".to_string(), name.clone());
    metadata.insert("version".to_string(), version);
    if action == "doc_upload" {
        metadata.insert("filetype".to_string(), "doczip".to_string());
    }
    save_file(
        state,
        SaveFileRequest {
            user,
            index,
            project: name,
            filename,
            body: content,
            metadata,
            auth_user,
        },
    )
    .await
}

async fn handle_stage_form_submit(
    state: AppState,
    user: String,
    index: String,
    auth_user: Option<String>,
    body: Bytes,
) -> Response {
    if !valid_stage_segment(&user) || !valid_stage_segment(&index) {
        return TextResponse(StatusCode::BAD_REQUEST, "invalid stage\n".to_string())
            .into_response();
    }
    let fields = match parse_urlencoded_fields(&body) {
        Ok(fields) => fields,
        Err(err) => {
            return TextResponse(StatusCode::BAD_REQUEST, format!("bad form body: {err}\n"))
                .into_response();
        }
    };
    let mut action = None;
    let mut name = None;
    let mut version = None;
    let mut metadata = BTreeMap::new();

    for (field_name, value) in fields {
        match field_name.as_str() {
            ":action" => action = Some(value),
            "name" => name = Some(value),
            "version" => version = Some(value),
            _ => insert_submit_metadata(&mut metadata, field_name, value),
        }
    }

    let Some(action) = action else {
        return TextResponse(
            StatusCode::BAD_REQUEST,
            ":action field not found\n".to_string(),
        )
        .into_response();
    };
    if action != "submit" {
        return TextResponse(
            StatusCode::BAD_REQUEST,
            format!("action {action:?} not supported for urlencoded submit\n"),
        )
        .into_response();
    }
    let Some(name) = name else {
        return TextResponse(
            StatusCode::BAD_REQUEST,
            "name field not found\n".to_string(),
        )
        .into_response();
    };
    let Some(version) = version else {
        return TextResponse(
            StatusCode::BAD_REQUEST,
            "version field not found\n".to_string(),
        )
        .into_response();
    };
    metadata.insert("name".to_string(), name.clone());
    metadata.insert("version".to_string(), version.clone());
    register_version_metadata(state, user, index, name, version, metadata, auth_user).await
}

fn normalize_submit_metadata_value(value: String) -> String {
    if value == "UNKNOWN" {
        String::new()
    } else {
        value
    }
}

fn insert_submit_metadata(metadata: &mut BTreeMap<String, String>, key: String, value: String) {
    let value = normalize_submit_metadata_value(value);
    if is_multi_value_metadata_field(&key) {
        metadata
            .entry(key)
            .and_modify(|existing| {
                if !existing.is_empty() && !value.is_empty() {
                    existing.push('\n');
                }
                existing.push_str(&value);
            })
            .or_insert(value);
    } else {
        metadata.insert(key, value);
    }
}

async fn register_version_metadata(
    state: AppState,
    user: String,
    index: String,
    project: String,
    version: String,
    metadata: BTreeMap<String, String>,
    auth_user: Option<String>,
) -> Response {
    blocking(move || {
        if let Err(response) = ensure_index_exists(&state, &user, &index) {
            return *response;
        }
        if let Err(response) =
            ensure_acl_upload_allowed(&state, &user, &index, auth_user.as_deref())
        {
            return *response;
        }
        match state
            .local
            .save_version_metadata_in(&user, &index, &project, &version, metadata)
        {
            Ok(()) => {
                let normalized = normalize_project_name(&project);
                let response = json_value_response(
                    StatusCode::OK,
                    json!({
                        "status": 200,
                        "user": user,
                        "index": index,
                        "project": normalized,
                        "version": version,
                        "registered": true,
                    }),
                );
                bump_serial_response_with_event(
                    &state,
                    response,
                    json!({
                        "event": "version_register",
                        "stage": format!("{user}/{index}"),
                        "project": normalized,
                        "version": version,
                    }),
                )
            }
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response()
            }
            Err(err) => {
                TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n")).into_response()
            }
        }
    })
    .await
}

async fn push_release(
    state: AppState,
    user: String,
    index: String,
    body: Bytes,
    auth_user: Option<String>,
) -> Response {
    blocking(move || {
        if let Err(response) = ensure_index_exists(&state, &user, &index) {
            return *response;
        }
        let input = match parse_json_or_default::<PushRequest>(&body) {
            Ok(input) => input,
            Err(err) => {
                return json_message_response(StatusCode::BAD_REQUEST, format!("{err}"));
            }
        };
        if input.no_docs && input.only_docs {
            return json_message_response(
                StatusCode::BAD_REQUEST,
                "can't use 'no_docs' and 'only_docs' together",
            );
        }
        let Some(name) = input
            .name
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(ToString::to_string)
        else {
            return json_message_response(StatusCode::BAD_REQUEST, "name is required");
        };
        let Some(version) = input
            .version
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(ToString::to_string)
        else {
            return json_message_response(StatusCode::BAD_REQUEST, "version is required");
        };
        let doc_filters = (input.no_docs, input.only_docs);
        if input
            .targetindex
            .as_ref()
            .is_none_or(|value| value.trim().is_empty())
        {
            if input
                .posturl
                .as_ref()
                .is_none_or(|value| value.trim().is_empty())
            {
                return json_message_response(
                    StatusCode::BAD_REQUEST,
                    "targetindex or posturl is required",
                );
            }
            return push_release_external(
                &state,
                &user,
                &index,
                &name,
                &version,
                &input,
                auth_user.as_deref(),
            );
        }
        let Some(targetindex) = input.targetindex.filter(|value| !value.trim().is_empty()) else {
            return json_message_response(
                StatusCode::BAD_REQUEST,
                "targetindex or posturl is required",
            );
        };
        let Some((target_user, target_index)) = targetindex.trim_matches('/').split_once('/')
        else {
            return json_message_response(
                StatusCode::BAD_REQUEST,
                "targetindex not in format user/index",
            );
        };
        if target_index.contains('/')
            || !valid_stage_segment(target_user)
            || !valid_stage_segment(target_index)
        {
            return json_message_response(StatusCode::BAD_REQUEST, "invalid targetindex");
        }

        if let Err(response) = ensure_index_exists(&state, target_user, target_index) {
            return *response;
        }
        let target_config = match index_config_for_stage(&state, target_user, target_index) {
            Ok(config) => config,
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
            }
            Err(err) => {
                return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                    .into_response();
            }
        };
        if target_config.index_type != "stage" {
            return json_message_response(
                StatusCode::BAD_REQUEST,
                format!("targetindex {target_user}/{target_index} is not a stage"),
            );
        }
        if let Err(response) =
            ensure_acl_upload_allowed(&state, target_user, target_index, auth_user.as_deref())
        {
            return *response;
        }
        if let Err(response) = ensure_pkg_read_allowed(&state, &user, &index, auth_user.as_deref())
        {
            return *response;
        }

        let normalized = normalize_project_name(&name);
        let source_files = match state.local.files_in(&user, &index, &normalized) {
            Ok(files) => files,
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
            }
            Err(err) => {
                return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                    .into_response();
            }
        };
        let file_metadata = match state
            .local
            .project_file_metadata_in(&user, &index, &normalized)
        {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
            }
            Err(err) => {
                return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                    .into_response();
            }
        };
        let version_metadata =
            match state
                .local
                .project_version_metadata_in(&user, &index, &normalized)
            {
                Ok(metadata) => metadata,
                Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                    return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n"))
                        .into_response();
                }
                Err(err) => {
                    return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                        .into_response();
                }
            };
        let tox_results = match state
            .local
            .project_tox_results_in(&user, &index, &normalized)
        {
            Ok(results) => results,
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
            }
            Err(err) => {
                return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                    .into_response();
            }
        };

        let mut selected = Vec::new();
        for file in source_files {
            if infer_version(&normalized, &file.filename) != version {
                continue;
            }
            let is_doc = is_doczip_file(&file.filename, file_metadata.get(&file.filename));
            if (input.no_docs && is_doc) || (input.only_docs && !is_doc) {
                continue;
            }
            let bytes = match state
                .local
                .read_in(&user, &index, &normalized, &file.filename)
            {
                Ok(bytes) => bytes,
                Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                    return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n"))
                        .into_response();
                }
                Err(err) => {
                    return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                        .into_response();
                }
            };
            let filename = file.filename;
            let metadata = file_metadata.get(&filename);
            selected.push(SelectedPushFile {
                metadata: metadata.map(|metadata| metadata.fields.clone()),
                log: metadata
                    .map(|metadata| metadata.log.clone())
                    .unwrap_or_default(),
                filename,
                bytes,
            });
        }
        if selected.is_empty() {
            let source_config = match index_config_for_stage(&state, &user, &index) {
                Ok(config) => config,
                Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                    return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n"))
                        .into_response();
                }
                Err(err) => {
                    return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                        .into_response();
                }
            };
            if source_config.index_type == "mirror" {
                match select_mirror_files_for_push(
                    &state,
                    &user,
                    &index,
                    &source_config,
                    &normalized,
                    &version,
                    doc_filters,
                ) {
                    Ok(files) => selected = files,
                    Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                        return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n"))
                            .into_response();
                    }
                    Err(err) => {
                        return TextResponse(
                            StatusCode::BAD_GATEWAY,
                            format!("upstream error: {err}\n"),
                        )
                        .into_response();
                    }
                }
            }
        }
        if selected.is_empty() {
            return json_message_response(
                StatusCode::NOT_FOUND,
                format!("no release/files found for {name} {version}"),
            );
        }

        if !target_config.volatile {
            for selected_file in &selected {
                let filename = &selected_file.filename;
                let bytes = &selected_file.bytes;
                match state
                    .local
                    .file_exists_in(target_user, target_index, &normalized, filename)
                {
                    Ok(true) => {
                        let existing = match state.local.read_in(
                            target_user,
                            target_index,
                            &normalized,
                            filename,
                        ) {
                            Ok(existing) => existing,
                            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                                return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n"))
                                    .into_response();
                            }
                            Err(err) => {
                                return TextResponse(
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    format!("{err}\n"),
                                )
                                .into_response();
                            }
                        };
                        if existing != *bytes {
                            return json_message_response(
                                StatusCode::CONFLICT,
                                format!("{filename} already exists in non-volatile index"),
                            );
                        }
                    }
                    Ok(false) => {}
                    Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                        return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n"))
                            .into_response();
                    }
                    Err(err) => {
                        return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                            .into_response();
                    }
                }
            }
        }

        let mut pushed_version_metadata = version_metadata
            .get(&version)
            .map(|metadata| metadata.fields.clone())
            .unwrap_or_default();
        pushed_version_metadata
            .entry("name".to_string())
            .or_insert_with(|| name.clone());
        pushed_version_metadata
            .entry("version".to_string())
            .or_insert_with(|| version.clone());
        if let Err(err) = state.local.save_version_metadata_in(
            target_user,
            target_index,
            &normalized,
            &version,
            pushed_version_metadata,
        ) {
            return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                .into_response();
        }

        let mut actionlog = vec![json!([
            200,
            "register",
            name,
            version,
            "->",
            format!("{target_user}/{target_index}")
        ])];
        let mut target_tox_results =
            match state
                .local
                .project_tox_results_in(target_user, target_index, &normalized)
            {
                Ok(results) => results,
                Err(err) => {
                    return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                        .into_response();
                }
            };
        for selected_file in selected {
            let filename = selected_file.filename;
            let bytes = selected_file.bytes;
            let already_identical = state
                .local
                .read_in(target_user, target_index, &normalized, &filename)
                .map(|existing| existing == bytes)
                .unwrap_or(false);
            if !already_identical || target_config.volatile {
                match state
                    .local
                    .save_in(target_user, target_index, &normalized, &filename, &bytes)
                {
                    Ok(_) => {}
                    Err(err) => {
                        return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                            .into_response();
                    }
                }
            }
            let log = pushed_file_log(
                &selected_file.log,
                auth_user.as_deref(),
                &user,
                &index,
                target_user,
                target_index,
            );
            match state.local.save_file_metadata_with_log_in(
                target_user,
                target_index,
                &normalized,
                &filename,
                selected_file.metadata.unwrap_or_default(),
                log,
            ) {
                Ok(()) => {}
                Err(err) => {
                    return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                        .into_response();
                }
            }
            actionlog.push(json!([
                if already_identical { 200 } else { 201 },
                "upload",
                format!("{user}/{index}/{normalized}/{filename}"),
                "->",
                format!("{target_user}/{target_index}/{normalized}/{filename}")
            ]));
            if let (true, Some(results)) = (
                !target_tox_results.contains_key(&filename),
                tox_results.get(&filename),
            ) {
                for result in results {
                    if let Err(err) = state.local.store_tox_result_in(
                        target_user,
                        target_index,
                        &normalized,
                        &filename,
                        result.clone(),
                    ) {
                        return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                            .into_response();
                    }
                }
                target_tox_results.insert(filename.clone(), results.clone());
                actionlog.push(json!([200, "toxresult", filename, results.len()]));
            }
        }

        let response = json_value_response(
            StatusCode::OK,
            json!({
                "status": 200,
                "type": "actionlog",
                "result": actionlog,
            }),
        );
        bump_serial_response_with_event(
            &state,
            response,
            json!({
                "event": "release_push",
                "source_stage": format!("{user}/{index}"),
                "target_stage": format!("{target_user}/{target_index}"),
                "project": normalized,
                "version": version,
            }),
        )
    })
    .await
}

fn push_release_external(
    state: &AppState,
    user: &str,
    index: &str,
    name: &str,
    version: &str,
    input: &PushRequest,
    auth_user: Option<&str>,
) -> Response {
    if let Err(response) = ensure_pkg_read_allowed(state, user, index, auth_user) {
        return *response;
    }
    let Some(posturl) = input
        .posturl
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    else {
        return json_message_response(StatusCode::BAD_REQUEST, "posturl is required");
    };
    let Some(username) = input.username.as_deref() else {
        return json_message_response(StatusCode::BAD_REQUEST, "username is required");
    };
    let Some(password) = input.password.as_deref() else {
        return json_message_response(StatusCode::BAD_REQUEST, "password is required");
    };
    let normalized = normalize_project_name(name);
    let (selected, mut version_metadata) =
        match select_local_files_for_external_push(state, user, index, &normalized, version, input)
        {
            Ok(result) => result,
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
            }
            Err(err) => {
                return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                    .into_response();
            }
        };
    if selected.is_empty() {
        return json_message_response(
            StatusCode::NOT_FOUND,
            format!("no release/files found for {name} {version}"),
        );
    }

    version_metadata
        .entry("name".to_string())
        .or_insert_with(|| name.to_string());
    version_metadata
        .entry("version".to_string())
        .or_insert_with(|| version.to_string());
    version_metadata
        .entry("metadata_version".to_string())
        .or_default();

    let auth = Some((username, password));
    let mut actionlog = Vec::new();
    if input.register_project && !input.only_docs {
        let fields = external_metadata_fields(":action", "submit", &version_metadata);
        let outcome = state
            .external_poster
            .post_multipart(posturl, auth, &fields, None);
        let entry = external_action_entry(outcome, "register", [name, version]);
        let failed = external_action_failed(&entry, true);
        actionlog.push(entry);
        if failed {
            return external_actionlog_response(state, actionlog, true);
        }
    }

    let mut failed = false;
    for selected_file in selected {
        let filename = selected_file.filename;
        let file_metadata = selected_file.metadata.unwrap_or_default();
        let is_doc = is_doczip_metadata(&filename, &file_metadata);
        let action = if is_doc { "docfile" } else { "upload" };
        let form_action = if is_doc { "doc_upload" } else { "file_upload" };
        let mut metadata = version_metadata.clone();
        for (key, value) in file_metadata {
            metadata.insert(key, value);
        }
        let fields = external_metadata_fields(":action", form_action, &metadata);
        let file = ExternalMultipartFile {
            field: "content",
            filename: &filename,
            bytes: &selected_file.bytes,
        };
        let outcome = state
            .external_poster
            .post_multipart(posturl, auth, &fields, Some(file));
        let path = if is_doc {
            normalized.clone()
        } else {
            format!("{user}/{index}/+f/{normalized}/{filename}")
        };
        let entry = external_action_entry(outcome, action, [&path]);
        failed |= external_action_failed(&entry, false);
        actionlog.push(entry);
        if failed {
            break;
        }
    }

    external_actionlog_response(state, actionlog, failed)
}

fn select_local_files_for_external_push(
    state: &AppState,
    user: &str,
    index: &str,
    normalized: &str,
    version: &str,
    input: &PushRequest,
) -> io::Result<(Vec<SelectedPushFile>, BTreeMap<String, String>)> {
    let source_files = state.local.files_in(user, index, normalized)?;
    let file_metadata = state
        .local
        .project_file_metadata_in(user, index, normalized)?;
    let version_metadata = state
        .local
        .project_version_metadata_in(user, index, normalized)?;
    let mut selected = Vec::new();
    for file in source_files {
        if infer_version(normalized, &file.filename) != version {
            continue;
        }
        let is_doc = is_doczip_file(&file.filename, file_metadata.get(&file.filename));
        if (input.no_docs && is_doc) || (input.only_docs && !is_doc) {
            continue;
        }
        let filename = file.filename;
        let bytes = state.local.read_in(user, index, normalized, &filename)?;
        let metadata = file_metadata.get(&filename);
        selected.push(SelectedPushFile {
            metadata: metadata.map(|metadata| metadata.fields.clone()),
            log: metadata
                .map(|metadata| metadata.log.clone())
                .unwrap_or_default(),
            filename,
            bytes,
        });
    }
    Ok((
        selected,
        version_metadata
            .get(version)
            .map(|metadata| metadata.fields.clone())
            .unwrap_or_default(),
    ))
}

fn external_metadata_fields(
    action_key: &str,
    action: &str,
    metadata: &BTreeMap<String, String>,
) -> Vec<(String, String)> {
    let mut fields = vec![(action_key.to_string(), action.to_string())];
    fields.extend(
        metadata
            .iter()
            .map(|(key, value)| (key.clone(), value.clone())),
    );
    fields
}

fn external_action_entry<const N: usize>(
    outcome: io::Result<ExternalPostResponse>,
    action: &str,
    details: [&str; N],
) -> Value {
    match outcome {
        Ok(response) => {
            let mut parts = vec![json!(response.status), json!(action)];
            parts.extend(details.into_iter().map(|detail| json!(detail)));
            parts.push(json!(response.body));
            Value::Array(parts)
        }
        Err(err) => json!([-1, external_exception_action(action), err.to_string()]),
    }
}

fn external_exception_action(action: &str) -> &'static str {
    match action {
        "register" => "exception on register:",
        "upload" => "exception on release upload:",
        "docfile" => "exception on docfile upload:",
        _ => "exception on push:",
    }
}

fn external_action_failed(entry: &Value, register: bool) -> bool {
    let status = entry
        .as_array()
        .and_then(|parts| parts.first())
        .and_then(Value::as_i64)
        .unwrap_or(-1);
    status < 0 || status >= 400 && !(register && status == 410)
}

fn external_actionlog_response(state: &AppState, actionlog: Vec<Value>, failed: bool) -> Response {
    let status = if failed {
        StatusCode::BAD_GATEWAY
    } else {
        StatusCode::OK
    };
    let response = json_value_response(
        status,
        json!({
            "status": status.as_u16(),
            "type": "actionlog",
            "result": actionlog,
        }),
    );
    with_current_serial(state, response)
}

fn external_post_multipart(
    url: &str,
    auth: Option<(&str, &str)>,
    fields: &[(String, String)],
    file: Option<ExternalMultipartFile<'_>>,
) -> io::Result<ExternalPostResponse> {
    let target = parse_http_post_url(url)?;
    let boundary = "devpi-rs-boundary";
    let body = multipart_body(boundary, fields, file);
    let mut stream = TcpStream::connect((&*target.host, target.port))?;
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    stream.set_write_timeout(Some(Duration::from_secs(30)))?;
    let auth_header = auth
        .map(|(username, password)| {
            format!(
                "Authorization: Basic {}\r\n",
                encode_base64(format!("{username}:{password}").as_bytes())
            )
        })
        .unwrap_or_default();
    let request = format!(
        "POST {} HTTP/1.1\r\nHost: {}\r\nUser-Agent: devpi-rs/{}\r\n{}Connection: close\r\nContent-Type: multipart/form-data; boundary={}\r\nContent-Length: {}\r\n\r\n",
        target.path,
        target.host_header(),
        env!("CARGO_PKG_VERSION"),
        auth_header,
        boundary,
        body.len()
    );
    stream.write_all(request.as_bytes())?;
    stream.write_all(&body)?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response)?;
    parse_http_response(&response)
}

struct HttpPostTarget {
    host: String,
    port: u16,
    path: String,
}

impl HttpPostTarget {
    fn host_header(&self) -> String {
        if self.port == 80 {
            self.host.clone()
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }
}

fn parse_http_post_url(url: &str) -> io::Result<HttpPostTarget> {
    let Some(rest) = url.strip_prefix("http://") else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "only http:// posturl is supported",
        ));
    };
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    if authority.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "posturl host is required",
        ));
    }
    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) if !host.is_empty() && port.chars().all(|ch| ch.is_ascii_digit()) => {
            let port = port.parse::<u16>().map_err(|err| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("invalid posturl port: {err}"),
                )
            })?;
            (host.to_string(), port)
        }
        _ => (authority.to_string(), 80),
    };
    Ok(HttpPostTarget {
        host,
        port,
        path: format!("/{}", path),
    })
}

fn multipart_body(
    boundary: &str,
    fields: &[(String, String)],
    file: Option<ExternalMultipartFile<'_>>,
) -> Vec<u8> {
    let mut body = Vec::new();
    for (name, value) in fields {
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            format!(
                "Content-Disposition: form-data; name=\"{}\"\r\n\r\n",
                multipart_escape(name)
            )
            .as_bytes(),
        );
        body.extend_from_slice(value.as_bytes());
        body.extend_from_slice(b"\r\n");
    }
    if let Some(file) = file {
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            format!(
                "Content-Disposition: form-data; name=\"{}\"; filename=\"{}\"\r\nContent-Type: application/octet-stream\r\n\r\n",
                multipart_escape(file.field),
                multipart_escape(file.filename)
            )
            .as_bytes(),
        );
        body.extend_from_slice(file.bytes);
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    body
}

fn multipart_escape(value: &str) -> String {
    value.replace(['\\', '"', '\r', '\n'], "_")
}

fn parse_http_response(bytes: &[u8]) -> io::Result<ExternalPostResponse> {
    let header_end = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid HTTP response"))?;
    let header_text = String::from_utf8_lossy(&bytes[..header_end]);
    let status = header_text
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse::<u16>().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid HTTP status"))?;
    let body = String::from_utf8_lossy(&bytes[header_end + 4..]).to_string();
    Ok(ExternalPostResponse { status, body })
}

fn select_mirror_files_for_push(
    state: &AppState,
    user: &str,
    index: &str,
    config: &IndexConfig,
    project: &str,
    version: &str,
    doc_filters: (bool, bool),
) -> io::Result<Vec<SelectedPushFile>> {
    let result = project_pages_for_index_config(state, user, index, config, project)?;
    record_upstream_reports(state, result.reports.clone());
    let project_json = merge_project_json(project, &result.pages);
    let value: Value = serde_json::from_str(&project_json).map_err(io::Error::other)?;
    let mut selected = Vec::new();
    let Some(files) = value["files"].as_array() else {
        return Ok(selected);
    };

    for file in files {
        let Some(filename) = file["filename"].as_str() else {
            continue;
        };
        if infer_version(project, filename) != version {
            continue;
        }
        let is_doc = filename.ends_with(".doc.zip") || filename.ends_with(".doc.tgz");
        if (doc_filters.0 && is_doc) || (doc_filters.1 && !is_doc) {
            continue;
        }
        let Some(url) = file["url"].as_str() else {
            continue;
        };
        if !mirror_url_allowed(config, url) {
            continue;
        }
        let bytes = state.index.cached_or_fetch_file(
            &encode_mirror_url(url),
            url,
            state.fetcher.as_ref(),
        )?;
        selected.push(SelectedPushFile {
            filename: filename.to_string(),
            bytes,
            metadata: mirror_file_metadata_from_json(file),
            log: Vec::new(),
        });
    }

    Ok(selected)
}

fn mirror_file_metadata_from_json(file: &Value) -> Option<BTreeMap<String, String>> {
    let mut metadata = BTreeMap::new();
    if let Some(value) = file["requires-python"].as_str() {
        metadata.insert("requires_python".to_string(), value.to_string());
    }
    if let Some(value) = metadata_attr_from_json(&file["yanked"]) {
        metadata.insert("yanked".to_string(), value);
    }
    if let Some(value) = metadata_attr_from_json(&file["core-metadata"]) {
        metadata.insert("core_metadata".to_string(), value);
    }
    if let Some(value) = metadata_attr_from_json(&file["dist-info-metadata"]) {
        metadata.insert("dist-info-metadata".to_string(), value);
    }
    if let Some(value) = metadata_attr_from_json(&file["gpg-sig"]) {
        metadata.insert("gpg_sig".to_string(), value);
    }
    (!metadata.is_empty()).then_some(metadata)
}

fn metadata_attr_from_json(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Bool(value) => Some(value.to_string()),
        Value::Object(values) => values
            .iter()
            .next()
            .and_then(|(key, value)| value.as_str().map(|value| format!("{key}={value}"))),
        _ => None,
    }
}

async fn render_version_metadata(
    state: AppState,
    user: String,
    index: String,
    project: String,
    version: String,
    include_bases: bool,
    auth_user: Option<String>,
) -> Response {
    blocking(move || {
        if let Err(response) = ensure_index_exists(&state, &user, &index) {
            return *response;
        }
        if let Err(response) = ensure_pkg_read_allowed(&state, &user, &index, auth_user.as_deref())
        {
            return *response;
        }
        let normalized = normalize_project_name(&project);
        let metadata = match find_version_metadata(
            &state,
            (&user, &index),
            &normalized,
            &version,
            include_bases,
            auth_user.as_deref(),
            &mut HashSet::new(),
        ) {
            Ok(Some(metadata)) => metadata,
            Ok(None) => {
                match local_version_links_response(
                    &state,
                    &user,
                    &index,
                    &normalized,
                    &version,
                    include_bases,
                    auth_user.as_deref(),
                ) {
                    Ok(Some(response)) => return response,
                    Ok(None) => {}
                    Err(response) => return *response,
                }
                match missing_version_upstream_response(
                    &state,
                    &user,
                    &index,
                    &normalized,
                    &version,
                    auth_user.as_deref(),
                ) {
                    Ok(Some(response)) => return response,
                    Ok(None) => {}
                    Err(response) => return *response,
                }
                return TextResponse(StatusCode::NOT_FOUND, "version not found\n".to_string())
                    .into_response();
            }
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
            }
            Err(err) => {
                return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                    .into_response();
            }
        };
        let files = match project_files_for_stage(
            &state,
            &user,
            &index,
            &normalized,
            include_bases,
            auth_user.as_deref(),
            &mut HashSet::new(),
        ) {
            Ok(files) => files,
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
            }
            Err(err) => {
                return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                    .into_response();
            }
        };
        let tox_results = match project_tox_results_for_stage(
            &state,
            &user,
            &index,
            &normalized,
            include_bases,
            auth_user.as_deref(),
            &mut HashSet::new(),
        ) {
            Ok(tox_results) => tox_results,
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
            }
            Err(err) => {
                return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                    .into_response();
            }
        };
        let file_metadata = match project_file_metadata_for_stage(
            &state,
            &user,
            &index,
            &normalized,
            include_bases,
            auth_user.as_deref(),
            &mut HashSet::new(),
        ) {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
            }
            Err(err) => {
                return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                    .into_response();
            }
        };
        let mut result = metadata_fields_json(&metadata.fields);
        if let Some(result) = result.as_object_mut() {
            result
                .entry("name".to_string())
                .or_insert_with(|| json!(normalized.clone()));
            result
                .entry("version".to_string())
                .or_insert_with(|| json!(version.clone()));
            result.insert(
                "+links".to_string(),
                json!(version_file_links_json(
                    &user,
                    &index,
                    &normalized,
                    &version,
                    &files,
                    &file_metadata,
                    &tox_results
                )),
            );
        }
        let response = json_value_response(
            StatusCode::OK,
            json!({
                "status": 200,
                "type": "versiondata",
                "result": result,
            }),
        );
        with_current_serial(&state, response)
    })
    .await
}

fn local_version_links_response(
    state: &AppState,
    user: &str,
    index: &str,
    project: &str,
    version: &str,
    include_bases: bool,
    auth_user: Option<&str>,
) -> Result<Option<Response>, Box<Response>> {
    let files = project_files_for_stage(
        state,
        user,
        index,
        project,
        include_bases,
        auth_user,
        &mut HashSet::new(),
    )
    .map_err(version_links_error_response)?;
    let tox_results = project_tox_results_for_stage(
        state,
        user,
        index,
        project,
        include_bases,
        auth_user,
        &mut HashSet::new(),
    )
    .map_err(version_links_error_response)?;
    let file_metadata = project_file_metadata_for_stage(
        state,
        user,
        index,
        project,
        include_bases,
        auth_user,
        &mut HashSet::new(),
    )
    .map_err(version_links_error_response)?;
    let links = version_file_links_json(
        user,
        index,
        project,
        version,
        &files,
        &file_metadata,
        &tox_results,
    );
    if links.is_empty() {
        return Ok(None);
    }
    let response = json_value_response(
        StatusCode::OK,
        json!({
            "status": 200,
            "type": "versiondata",
            "result": {
                "name": project,
                "version": version,
                "+links": links,
            },
        }),
    );
    Ok(Some(with_current_serial(state, response)))
}

fn version_links_error_response(err: io::Error) -> Box<Response> {
    if err.kind() == io::ErrorKind::InvalidInput {
        Box::new(TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response())
    } else {
        Box::new(
            TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n")).into_response(),
        )
    }
}

fn find_version_metadata(
    state: &AppState,
    stage: (&str, &str),
    project: &str,
    version: &str,
    include_bases: bool,
    auth_user: Option<&str>,
    visited: &mut HashSet<String>,
) -> io::Result<Option<FileMetadata>> {
    let (user, index) = stage;
    if !visited.insert(format!("{user}/{index}")) {
        return Ok(None);
    }
    let versions = state
        .local
        .project_version_metadata_in(user, index, project)?;
    if let Some(metadata) = versions.get(version) {
        return Ok(Some(metadata.clone()));
    }
    if include_bases {
        for (base_user, base_index) in configured_bases(state, user, index)? {
            if !pkg_read_allowed(state, &base_user, &base_index, auth_user)? {
                continue;
            }
            if let Some(metadata) = find_version_metadata(
                state,
                (&base_user, &base_index),
                project,
                version,
                include_bases,
                auth_user,
                visited,
            )? {
                return Ok(Some(metadata));
            }
        }
    }
    Ok(None)
}

fn missing_version_upstream_response(
    state: &AppState,
    user: &str,
    index: &str,
    project: &str,
    version: &str,
    auth_user: Option<&str>,
) -> Result<Option<Response>, Box<Response>> {
    let include_upstream =
        match should_include_upstream_for_project(state, user, index, project, auth_user) {
            Ok(include_upstream) => include_upstream,
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                return Err(Box::new(
                    TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response(),
                ));
            }
            Err(err) => {
                return Err(Box::new(
                    TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                        .into_response(),
                ));
            }
        };
    if !include_upstream {
        return Ok(None);
    }

    let config = match index_config_for_stage(state, user, index) {
        Ok(config) => config,
        Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
            return Err(Box::new(
                TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response(),
            ));
        }
        Err(err) => {
            return Err(Box::new(
                TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n")).into_response(),
            ));
        }
    };
    let result = match project_pages_for_index_config(state, user, index, &config, project) {
        Ok(result) => result,
        Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
            return Err(Box::new(
                TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response(),
            ));
        }
        Err(err) => {
            return Err(Box::new(upstream_error_response(&err.to_string())));
        }
    };
    record_upstream_reports(state, result.reports.clone());
    let uncached_error_response = upstream_uncached_error_response(&result.reports);
    let stale_cache_used = upstream_stale_cache_used(&result.reports);
    let pages = match mirror_project_pages_for_stage(state, user, index, result.pages) {
        Ok(pages) => pages,
        Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
            return Err(Box::new(
                TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response(),
            ));
        }
        Err(err) => {
            return Err(Box::new(
                TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n")).into_response(),
            ));
        }
    };
    if pages.is_empty() {
        return Ok(uncached_error_response);
    }

    if !simple_pages_contain_version(project, version, &pages)? {
        return Ok(None);
    }
    let mut response = json_value_response(
        StatusCode::OK,
        json!({
            "status": 200,
            "type": "versiondata",
            "result": {
                "name": project,
                "version": version,
            },
        }),
    );
    if stale_cache_used {
        mark_stale_cache_response(&mut response);
    }
    Ok(Some(with_current_serial(state, response)))
}

fn simple_pages_contain_version(
    project: &str,
    version: &str,
    pages: &[SourcePage],
) -> Result<bool, Box<Response>> {
    let project_json = merge_project_json(project, pages);
    let value: Value = serde_json::from_str(&project_json).map_err(|err| {
        Box::new(
            TextResponse(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("invalid upstream project json: {err}\n"),
            )
            .into_response(),
        )
    })?;
    let Some(files) = value["files"].as_array() else {
        return Ok(false);
    };
    Ok(files.iter().any(|file| {
        file["filename"]
            .as_str()
            .is_some_and(|filename| infer_version(project, filename) == version)
    }))
}

async fn read_file(
    state: AppState,
    user: String,
    index: String,
    project: String,
    filename: String,
    options: FileReadOptions,
) -> Response {
    blocking(move || {
        if let Err(response) = ensure_index_exists(&state, &user, &index) {
            if matches!(
                state
                    .local
                    .file_deleted_in(&user, &index, &project, &filename),
                Ok(true)
            ) {
                return TextResponse(StatusCode::GONE, "file not found\n".to_string())
                    .into_response();
            }
            return *response;
        }
        if let Err(response) =
            ensure_pkg_read_allowed(&state, &user, &index, options.auth_user.as_deref())
        {
            return *response;
        }
        if let Some((release_filename, result_index)) = tox_result_path(&filename) {
            return match tox_result_from_stage_or_bases(
                &state,
                (&user, &index),
                &project,
                release_filename,
                result_index,
                options.auth_user.as_deref(),
                &mut HashSet::new(),
            ) {
                Ok(Some(result)) => {
                    with_current_serial(&state, json_value_response(StatusCode::OK, result))
                }
                Ok(None) => {
                    TextResponse(StatusCode::NOT_FOUND, "tox result not found\n".to_string())
                        .into_response()
                }
                Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                    TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response()
                }
                Err(err) => TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                    .into_response(),
            };
        }

        if let Some(release_filename) = metadata_path(&filename) {
            return core_metadata_response(
                &state,
                &user,
                &index,
                &project,
                release_filename,
                options.auth_user.as_deref(),
            );
        }

        if options.json_preferred {
            return release_file_meta_response(
                &state,
                &user,
                &index,
                &project,
                &filename,
                options.auth_user.as_deref(),
            );
        }

        match read_file_from_stage_or_bases(
            &state,
            &user,
            &index,
            &project,
            &filename,
            options.auth_user.as_deref(),
            &mut HashSet::new(),
        ) {
            Ok(StageFileRead::Found(_resolved_user, _resolved_index, file)) => with_current_serial(
                &state,
                PackageFileResponse::new(filename, file)
                    .with_conditional(options.conditional)
                    .into_response(),
            ),
            Ok(StageFileRead::Gone) => {
                TextResponse(StatusCode::GONE, "file not found\n".to_string()).into_response()
            }
            Ok(StageFileRead::Missing) => {
                TextResponse(StatusCode::NOT_FOUND, "file not found\n".to_string()).into_response()
            }
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response()
            }
            Err(err) => {
                TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n")).into_response()
            }
        }
    })
    .await
}

async fn read_hashed_file(
    state: AppState,
    user: String,
    index: String,
    hash_prefix: String,
    hash_rest: String,
    filename: String,
    options: FileReadOptions,
) -> Response {
    blocking(move || {
        if let Err(response) = ensure_index_exists(&state, &user, &index) {
            return *response;
        }
        if let Err(response) =
            ensure_pkg_read_allowed(&state, &user, &index, options.auth_user.as_deref())
        {
            return *response;
        }
        match state
            .local
            .read_hash_path_file_in(&user, &index, &hash_prefix, &hash_rest, &filename)
        {
            Ok(Some(file)) => with_current_serial(
                &state,
                PackageFileResponse::new(filename, file)
                    .with_conditional(options.conditional)
                    .into_response(),
            ),
            Ok(None) => {
                TextResponse(StatusCode::NOT_FOUND, "file not found\n".to_string()).into_response()
            }
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response()
            }
            Err(err) => {
                TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n")).into_response()
            }
        }
    })
    .await
}

async fn read_mirror_file(
    state: AppState,
    user: String,
    index: String,
    relpath: String,
    options: FileReadOptions,
) -> Response {
    blocking(move || {
        if let Err(response) = ensure_index_exists(&state, &user, &index) {
            return *response;
        }
        if let Err(response) =
            ensure_pkg_read_allowed(&state, &user, &index, options.auth_user.as_deref())
        {
            return *response;
        }
        let config = match index_config_for_stage(&state, &user, &index) {
            Ok(config) => config,
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
            }
            Err(err) => {
                return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                    .into_response();
            }
        };
        if config.index_type != "mirror" {
            return TextResponse(StatusCode::NOT_FOUND, "mirror file not found\n".to_string())
                .into_response();
        }
        let Some((encoded_url, filename)) = mirror_relpath_parts(&relpath) else {
            return TextResponse(
                StatusCode::BAD_REQUEST,
                "invalid mirror file path\n".to_string(),
            )
            .into_response();
        };
        let Some(url) = decode_mirror_url(encoded_url) else {
            return TextResponse(
                StatusCode::BAD_REQUEST,
                "invalid mirror file URL\n".to_string(),
            )
            .into_response();
        };
        if !mirror_url_allowed(&config, &url) {
            return TextResponse(
                StatusCode::FORBIDDEN,
                "mirror file URL forbidden\n".to_string(),
            )
            .into_response();
        }
        let (release_filename, is_metadata) = filename
            .strip_suffix(".metadata")
            .map_or((filename, false), |filename| (filename, true));
        if is_metadata && !config.mirror_provides_core_metadata {
            return TextResponse(
                StatusCode::NOT_FOUND,
                "mirror_provides_core_metadata disabled\n".to_string(),
            )
            .into_response();
        }
        if !mirror_filename_matches_url(release_filename, &url) {
            return TextResponse(
                StatusCode::BAD_REQUEST,
                "mirror file name does not match URL\n".to_string(),
            )
            .into_response();
        }
        let fetch_url = if is_metadata {
            mirror_metadata_url(&url)
        } else {
            url.clone()
        };
        let cache_key = if is_metadata {
            format!("{encoded_url}.metadata")
        } else {
            encoded_url.to_string()
        };

        if config.mirror_use_external_urls {
            return match state.index.cached_file_entry(&cache_key) {
                Ok(file) => {
                    PackageFileResponse::from_parts(filename.to_string(), file.bytes, file.modified)
                        .with_conditional(options.conditional)
                        .into_response()
                }
                Err(err) if err.kind() == io::ErrorKind::NotFound => {
                    redirect_response_with_status(StatusCode::FOUND, &fetch_url)
                }
                Err(err) => TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                    .into_response(),
            };
        }

        match state
            .index
            .cached_or_fetch_file_entry(&cache_key, &fetch_url, state.fetcher.as_ref())
        {
            Ok(file) => {
                PackageFileResponse::from_parts(filename.to_string(), file.bytes, file.modified)
                    .with_conditional(options.conditional)
                    .into_response()
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                TextResponse(StatusCode::NOT_FOUND, "mirror file not found\n".to_string())
                    .into_response()
            }
            Err(err) => TextResponse(StatusCode::BAD_GATEWAY, format!("upstream error: {err}\n"))
                .into_response(),
        }
    })
    .await
}

enum StageFileRead {
    Found(String, String, LocalReadFile),
    Gone,
    Missing,
}

fn read_file_from_stage_or_bases(
    state: &AppState,
    user: &str,
    index: &str,
    project: &str,
    filename: &str,
    auth_user: Option<&str>,
    visited: &mut HashSet<String>,
) -> io::Result<StageFileRead> {
    if !visited.insert(format!("{user}/{index}")) {
        return Ok(StageFileRead::Missing);
    }
    match state.local.read_file_in(user, index, project, filename) {
        Ok(file) => {
            return Ok(StageFileRead::Found(
                user.to_string(),
                index.to_string(),
                file,
            ));
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            if state
                .local
                .file_deleted_in(user, index, project, filename)?
            {
                return Ok(StageFileRead::Gone);
            }
        }
        Err(err) => return Err(err),
    }
    for (base_user, base_index) in configured_bases(state, user, index)? {
        if !pkg_read_allowed(state, &base_user, &base_index, auth_user)? {
            continue;
        }
        match read_file_from_stage_or_bases(
            state,
            &base_user,
            &base_index,
            project,
            filename,
            auth_user,
            visited,
        )? {
            StageFileRead::Found(user, index, file) => {
                return Ok(StageFileRead::Found(user, index, file));
            }
            StageFileRead::Gone => return Ok(StageFileRead::Gone),
            StageFileRead::Missing => {}
        }
    }
    Ok(StageFileRead::Missing)
}

fn tox_result_from_stage_or_bases(
    state: &AppState,
    stage: (&str, &str),
    project: &str,
    filename: &str,
    result_index: usize,
    auth_user: Option<&str>,
    visited: &mut HashSet<String>,
) -> io::Result<Option<Value>> {
    let (user, index) = stage;
    if !visited.insert(format!("{user}/{index}")) {
        return Ok(None);
    }
    match state
        .local
        .tox_result_in(user, index, project, filename, result_index)
    {
        Ok(result) => return Ok(Some(result)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    for (base_user, base_index) in configured_bases(state, user, index)? {
        if !pkg_read_allowed(state, &base_user, &base_index, auth_user)? {
            continue;
        }
        if let Some(result) = tox_result_from_stage_or_bases(
            state,
            (&base_user, &base_index),
            project,
            filename,
            result_index,
            auth_user,
            visited,
        )? {
            return Ok(Some(result));
        }
    }
    Ok(None)
}

fn core_metadata_response(
    state: &AppState,
    user: &str,
    index: &str,
    project: &str,
    filename: &str,
    auth_user: Option<&str>,
) -> Response {
    let normalized = normalize_project_name(project);
    let (resolved_user, resolved_index, _file) = match read_file_from_stage_or_bases(
        state,
        user,
        index,
        &normalized,
        filename,
        auth_user,
        &mut HashSet::new(),
    ) {
        Ok(StageFileRead::Found(resolved_user, resolved_index, file)) => {
            (resolved_user, resolved_index, file)
        }
        Ok(StageFileRead::Gone) => {
            return TextResponse(StatusCode::GONE, "file not found\n".to_string()).into_response();
        }
        Ok(StageFileRead::Missing) => {
            return TextResponse(StatusCode::NOT_FOUND, "file not found\n".to_string())
                .into_response();
        }
        Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
            return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
        }
        Err(err) => {
            return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                .into_response();
        }
    };
    let metadata =
        match state
            .local
            .project_file_metadata_in(&resolved_user, &resolved_index, &normalized)
        {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
            }
            Err(err) => {
                return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                    .into_response();
            }
        };
    let Some(metadata) = metadata.get(filename) else {
        return TextResponse(StatusCode::NOT_FOUND, "metadata not found\n".to_string())
            .into_response();
    };
    let text = core_metadata_text(&normalized, filename, &metadata.fields);
    with_current_serial(
        state,
        with_content_type(
            StatusCode::OK,
            "text/plain; charset=utf-8",
            text.into_bytes(),
        ),
    )
}

fn release_file_meta_response(
    state: &AppState,
    user: &str,
    index: &str,
    project: &str,
    filename: &str,
    auth_user: Option<&str>,
) -> Response {
    let normalized = normalize_project_name(project);
    let (resolved_user, resolved_index, file) = match read_file_from_stage_or_bases(
        state,
        user,
        index,
        &normalized,
        filename,
        auth_user,
        &mut HashSet::new(),
    ) {
        Ok(StageFileRead::Found(resolved_user, resolved_index, file)) => {
            (resolved_user, resolved_index, file)
        }
        Ok(StageFileRead::Gone) => {
            return TextResponse(StatusCode::GONE, "file not found\n".to_string()).into_response();
        }
        Ok(StageFileRead::Missing) => {
            return TextResponse(StatusCode::NOT_FOUND, "file not found\n".to_string())
                .into_response();
        }
        Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
            return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
        }
        Err(err) => {
            return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                .into_response();
        }
    };
    let metadata =
        match state
            .local
            .project_file_metadata_in(&resolved_user, &resolved_index, &normalized)
        {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
            }
            Err(err) => {
                return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                    .into_response();
            }
        };

    let mut result = serde_json::Map::new();
    if let Some(metadata) = metadata.get(filename) {
        for (key, value) in &metadata.fields {
            result.insert(key.clone(), json!(value));
        }
    }
    result.insert("filename".to_string(), json!(filename));
    result.insert(
        "url".to_string(),
        json!(format!(
            "/{resolved_user}/{resolved_index}/+f/{normalized}/{filename}"
        )),
    );
    result.insert("bytes".to_string(), json!(file.bytes.len()));
    let sha256 = sha256_hex(&file.bytes);
    result.insert("hash_spec".to_string(), json!(format!("sha256={sha256}")));
    result.insert("hashes".to_string(), json!({ "sha256": sha256 }));
    result
        .entry("version".to_string())
        .or_insert_with(|| json!(infer_version(&normalized, filename)));

    let response = json_value_response(
        StatusCode::OK,
        json!({
            "status": 200,
            "type": "releasefilemeta",
            "result": Value::Object(result),
        }),
    );
    with_current_serial(state, response)
}

async fn remove_project(
    state: AppState,
    user: String,
    index: String,
    project: String,
    auth_user: Option<String>,
    force: bool,
) -> Response {
    blocking(move || {
        if let Err(response) = ensure_index_exists(&state, &user, &index) {
            return *response;
        }
        match index_config_for_stage(&state, &user, &index) {
            Ok(config) if !config.volatile && !force => {
                return TextResponse(
                    StatusCode::FORBIDDEN,
                    format!(
                        "project {:?} is on non-volatile index {}/{}\n",
                        normalize_project_name(&project),
                        user,
                        index
                    ),
                )
                .into_response();
            }
            Ok(_) => {}
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
            }
            Err(err) => {
                return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                    .into_response();
            }
        }

        if let Err(response) =
            ensure_acl_upload_allowed(&state, &user, &index, auth_user.as_deref())
        {
            return *response;
        }

        match state.local.delete_project_in(&user, &index, &project) {
            Ok(true) => {
                let normalized = normalize_project_name(&project);
                let response = json_value_response(
                    StatusCode::OK,
                    json!({
                        "status": 200,
                        "user": user,
                        "index": index,
                        "project": normalized,
                        "deleted": true,
                    }),
                );
                bump_serial_response_with_event(
                    &state,
                    response,
                    json!({
                        "event": "project_delete",
                        "stage": format!("{user}/{index}"),
                        "project": normalized,
                    }),
                )
            }
            Ok(false) => TextResponse(StatusCode::NOT_FOUND, "project not found\n".to_string())
                .into_response(),
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response()
            }
            Err(err) => {
                TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n")).into_response()
            }
        }
    })
    .await
}

async fn remove_version(
    state: AppState,
    user: String,
    index: String,
    project: String,
    version: String,
    auth_user: Option<String>,
    force: bool,
) -> Response {
    blocking(move || {
        if let Err(response) = ensure_index_exists(&state, &user, &index) {
            return *response;
        }
        match index_config_for_stage(&state, &user, &index) {
            Ok(config) if !config.volatile && !force => {
                return TextResponse(
                    StatusCode::FORBIDDEN,
                    "cannot delete version on non-volatile index\n".to_string(),
                )
                .into_response();
            }
            Ok(_) => {}
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
            }
            Err(err) => {
                return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                    .into_response();
            }
        }

        if let Err(response) =
            ensure_acl_upload_allowed(&state, &user, &index, auth_user.as_deref())
        {
            return *response;
        }

        match state
            .local
            .delete_version_in(&user, &index, &project, &version)
        {
            Ok(count) if count > 0 => {
                let normalized = normalize_project_name(&project);
                let response = json_value_response(
                    StatusCode::OK,
                    json!({
                        "status": 200,
                        "user": user,
                        "index": index,
                        "project": normalized,
                        "version": version,
                        "deleted": count,
                    }),
                );
                bump_serial_response_with_event(
                    &state,
                    response,
                    json!({
                        "event": "version_delete",
                        "stage": format!("{user}/{index}"),
                        "project": normalized,
                        "version": version,
                        "files": count,
                    }),
                )
            }
            Ok(_) => TextResponse(StatusCode::NOT_FOUND, "version not found\n".to_string())
                .into_response(),
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response()
            }
            Err(err) => {
                TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n")).into_response()
            }
        }
    })
    .await
}

async fn register_project(
    state: AppState,
    user: String,
    index: String,
    project: String,
    auth_user: Option<String>,
) -> Response {
    blocking(
        move || {
            if let Err(response) = ensure_index_exists(&state, &user, &index) {
                return *response;
            }
            if let Err(response) =
                ensure_acl_upload_allowed(&state, &user, &index, auth_user.as_deref())
            {
                return *response;
            }

            match state.local.create_project_in(&user, &index, &project) {
                Ok(created) => {
                    let status = if created {
                        StatusCode::CREATED
                    } else {
                        StatusCode::OK
                    };
                    let normalized = normalize_project_name(&project);
                    let response = json_value_response(
                        status,
                        json!({
                            "status": status.as_u16(),
                            "message": format!("project '{}' {}", normalized, if created { "created" } else { "already exists" })
                        }),
                    );
                    if created {
                        bump_serial_response_with_event(
                            &state,
                            response,
                            json!({
                                "event": "project_create",
                                "stage": format!("{user}/{index}"),
                                "project": normalized,
                            }),
                        )
                    } else {
                        with_current_serial(&state, response)
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                    TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response()
                }
                Err(err) => {
                    TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                        .into_response()
                }
            }
        },
    )
    .await
}

async fn delete_file(
    state: AppState,
    user: String,
    index: String,
    project: String,
    filename: String,
    auth_user: Option<String>,
    force: bool,
) -> Response {
    blocking(move || {
        if let Err(response) = ensure_index_exists(&state, &user, &index) {
            return *response;
        }
        if let Some((release_filename, result_index)) = tox_result_path(&filename) {
            let release_filename = release_filename.to_string();
            if let Err(response) =
                ensure_acl_upload_allowed(&state, &user, &index, auth_user.as_deref())
            {
                return *response;
            }
            return match state.local.delete_tox_result_in(
                &user,
                &index,
                &project,
                &release_filename,
                result_index,
            ) {
                Ok(true) => {
                    let normalized = normalize_project_name(&project);
                    let response = json_value_response(
                        StatusCode::OK,
                        json!({
                            "status": 200,
                            "user": user,
                            "index": index,
                            "project": normalized,
                            "filename": release_filename,
                            "result_index": result_index,
                            "deleted": true,
                        }),
                    );
                    bump_serial_response_with_event(
                        &state,
                        response,
                        json!({
                            "event": "toxresult_delete",
                            "stage": format!("{user}/{index}"),
                            "project": normalized,
                            "filename": release_filename,
                            "result_index": result_index,
                        }),
                    )
                }
                Ok(false) => TextResponse(StatusCode::GONE, "tox result not found\n".to_string())
                    .into_response(),
                Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                    TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response()
                }
                Err(err) => TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                    .into_response(),
            };
        }

        match index_config_for_stage(&state, &user, &index) {
            Ok(config)
                if !config.volatile
                    && !force
                    && !is_open_root_pypi_mirror(&user, &index, &config) =>
            {
                return TextResponse(
                    StatusCode::FORBIDDEN,
                    format!("{filename} is on non-volatile index {user}/{index}\n"),
                )
                .into_response();
            }
            Ok(_) => {}
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
            }
            Err(err) => {
                return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                    .into_response();
            }
        }

        if let Err(response) =
            ensure_acl_upload_allowed(&state, &user, &index, auth_user.as_deref())
        {
            return *response;
        }

        match state
            .local
            .delete_file_in(&user, &index, &project, &filename)
        {
            Ok(true) => {
                let normalized = normalize_project_name(&project);
                let response = json_value_response(
                    StatusCode::OK,
                    json!({
                        "status": 200,
                        "user": user,
                        "index": index,
                        "project": normalized,
                        "filename": filename,
                        "deleted": true,
                    }),
                );
                bump_serial_response_with_event(
                    &state,
                    response,
                    json!({
                        "event": "file_delete",
                        "stage": format!("{user}/{index}"),
                        "project": normalized,
                        "filename": filename,
                    }),
                )
            }
            Ok(false) => {
                match state
                    .local
                    .file_deleted_in(&user, &index, &project, &filename)
                {
                    Ok(true) => TextResponse(StatusCode::GONE, "file not found\n".to_string())
                        .into_response(),
                    Ok(false) => {
                        TextResponse(StatusCode::NOT_FOUND, "file not found\n".to_string())
                            .into_response()
                    }
                    Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                        TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response()
                    }
                    Err(err) => TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                        .into_response(),
                }
            }
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response()
            }
            Err(err) => {
                TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n")).into_response()
            }
        }
    })
    .await
}

async fn store_tox_result(state: AppState, request: StoreToxResultRequest) -> Response {
    blocking(move || {
        let StoreToxResultRequest {
            user,
            index,
            project,
            filename,
            body,
            auth_user,
            outside_url,
        } = request;
        if let Err(response) = ensure_index_exists(&state, &user, &index) {
            return *response;
        }
        if let Err(response) =
            ensure_toxresult_upload_allowed(&state, &user, &index, auth_user.as_deref())
        {
            return *response;
        }
        let result = match serde_json::from_slice::<Value>(&body) {
            Ok(value) => value,
            Err(err) => {
                return TextResponse(
                    StatusCode::BAD_REQUEST,
                    format!("invalid tox result json: {err}\n"),
                )
                .into_response();
            }
        };
        match state
            .local
            .store_tox_result_in(&user, &index, &project, &filename, result)
        {
            Ok(result_index) => {
                let normalized = normalize_project_name(&project);
                let response = json_value_response(
                    StatusCode::OK,
                    json!({
                        "status": 200,
                        "type": "toxresultpath",
                        "result": api_url(
                            outside_url.as_deref(),
                            &format!("/{user}/{index}/+f/{normalized}/{filename}.toxresult-{result_index}"),
                        )
                    }),
                );
                bump_serial_response_with_event(
                    &state,
                    response,
                    json!({
                        "event": "toxresult_upload",
                        "stage": format!("{user}/{index}"),
                        "project": normalized,
                        "filename": filename,
                        "result_index": result_index,
                    }),
                )
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                TextResponse(StatusCode::NOT_FOUND, "file not found\n".to_string()).into_response()
            }
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response()
            }
            Err(err) => {
                TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n")).into_response()
            }
        }
    })
    .await
}

async fn render_legacy_simple_root_with_format_conditional(
    state: AppState,
    format: SimpleFormat,
    conditional: ConditionalRequest,
) -> Response {
    blocking(move || render_legacy_simple_root_sync_conditional(&state, format, &conditional)).await
}

#[cfg(test)]
async fn render_legacy_simple_project_with_format(
    state: AppState,
    project: String,
    format: SimpleFormat,
) -> Response {
    blocking(move || render_legacy_simple_project_sync(&state, &project, format)).await
}

async fn render_legacy_simple_project_with_format_conditional(
    state: AppState,
    project: String,
    format: SimpleFormat,
    conditional: ConditionalRequest,
) -> Response {
    blocking(move || {
        render_legacy_simple_project_sync_conditional(&state, &project, format, &conditional)
    })
    .await
}

async fn render_simple_root_with_format(
    state: AppState,
    user: String,
    index: String,
    format: SimpleFormat,
    auth_user: Option<String>,
) -> Response {
    blocking(move || render_simple_root_sync(&state, &user, &index, format, auth_user.as_deref()))
        .await
}

async fn render_simple_root_with_format_conditional(
    state: AppState,
    user: String,
    index: String,
    format: SimpleFormat,
    auth_user: Option<String>,
    conditional: ConditionalRequest,
) -> Response {
    blocking(move || {
        render_simple_root_sync_conditional(
            &state,
            &user,
            &index,
            format,
            auth_user.as_deref(),
            &conditional,
        )
    })
    .await
}

#[cfg(test)]
async fn render_simple_project_with_format(
    state: AppState,
    user: String,
    index: String,
    project: String,
    format: SimpleFormat,
    auth_user: Option<String>,
    include_refresh_form: bool,
) -> Response {
    blocking(move || {
        render_simple_project_sync(
            &state,
            &user,
            &index,
            &project,
            format,
            auth_user.as_deref(),
            include_refresh_form,
        )
    })
    .await
}

async fn render_simple_project_with_format_conditional(
    state: AppState,
    user: String,
    index: String,
    project: String,
    format: SimpleFormat,
    auth_user: Option<String>,
    options: SimpleRenderOptions,
) -> Response {
    blocking(move || {
        render_simple_project_sync_conditional(
            &state,
            &user,
            &index,
            &project,
            format,
            auth_user.as_deref(),
            &options,
        )
    })
    .await
}

async fn refresh_simple_project(
    state: AppState,
    user: String,
    index: String,
    project: String,
    auth_user: Option<String>,
) -> Response {
    blocking(move || {
        refresh_simple_project_sync(&state, &user, &index, &project, auth_user.as_deref())
    })
    .await
}

async fn render_stage_index_with_projects(
    state: AppState,
    user: String,
    index: String,
    include_projects: bool,
) -> Response {
    blocking(move || render_stage_index_sync(&state, &user, &index, include_projects)).await
}

async fn render_stage_project(
    state: AppState,
    user: String,
    index: String,
    project: String,
    include_bases: bool,
    auth_user: Option<String>,
) -> Response {
    blocking(move || {
        render_stage_project_sync(
            &state,
            &user,
            &index,
            &project,
            include_bases,
            auth_user.as_deref(),
        )
    })
    .await
}

async fn blocking<F>(f: F) -> Response
where
    F: FnOnce() -> Response + Send + 'static,
{
    task::spawn_blocking(f).await.unwrap_or_else(|err| {
        TextResponse(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("route task failed: {err}\n"),
        )
        .into_response()
    })
}

fn render_legacy_simple_root_sync_conditional(
    state: &AppState,
    format: SimpleFormat,
    conditional: &ConditionalRequest,
) -> Response {
    match state.index.root_pages(state.fetcher.as_ref()) {
        Ok(result) => {
            record_upstream_reports(state, result.reports.clone());
            let mut pages = Vec::new();
            match state.local.root_page() {
                Ok(Some(page)) => pages.push(page),
                Ok(None) => {}
                Err(err) => {
                    return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                        .into_response();
                }
            }
            pages.extend(result.pages);
            if pages.is_empty()
                && let Some(response) = upstream_uncached_error_response(&result.reports)
            {
                return response;
            }
            simple_page_response_with_cache_policy(
                state,
                format,
                merge_root_page(&pages),
                merge_root_json(&pages),
                pypi_last_serial(&pages),
                upstream_stale_cache_used(&result.reports),
                conditional,
            )
        }
        Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
            TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response()
        }
        Err(err) => TextResponse(StatusCode::BAD_GATEWAY, format!("upstream error: {err}\n"))
            .into_response(),
    }
}

#[cfg(test)]
fn render_legacy_simple_project_sync(
    state: &AppState,
    project: &str,
    format: SimpleFormat,
) -> Response {
    render_legacy_simple_project_sync_conditional(
        state,
        project,
        format,
        &ConditionalRequest::default(),
    )
}

fn render_legacy_simple_project_sync_conditional(
    state: &AppState,
    project: &str,
    format: SimpleFormat,
    conditional: &ConditionalRequest,
) -> Response {
    let normalized = normalize_project_name(project);
    match state
        .index
        .project_pages(&normalized, state.fetcher.as_ref())
    {
        Ok(result) => {
            record_upstream_reports(state, result.reports.clone());
            let mut pages = Vec::new();
            match state.local.project_page(&normalized) {
                Ok(Some(page)) => pages.push(page),
                Ok(None) => {}
                Err(err) => {
                    return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                        .into_response();
                }
            }
            pages.extend(result.pages);
            if pages.is_empty() {
                if let Some(response) = upstream_uncached_error_response(&result.reports) {
                    return response;
                }
                simple_project_not_found_response(format, &normalized)
            } else {
                simple_page_response_with_cache_policy(
                    state,
                    format,
                    merge_project_page(&normalized, &pages),
                    merge_project_json(&normalized, &pages),
                    pypi_last_serial(&pages),
                    upstream_stale_cache_used(&result.reports),
                    conditional,
                )
            }
        }
        Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
            TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response()
        }
        Err(err) => TextResponse(StatusCode::BAD_GATEWAY, format!("upstream error: {err}\n"))
            .into_response(),
    }
}

fn render_simple_root_sync(
    state: &AppState,
    user: &str,
    index: &str,
    format: SimpleFormat,
    auth_user: Option<&str>,
) -> Response {
    render_simple_root_sync_conditional(
        state,
        user,
        index,
        format,
        auth_user,
        &ConditionalRequest::default(),
    )
}

fn render_simple_root_sync_conditional(
    state: &AppState,
    user: &str,
    index: &str,
    format: SimpleFormat,
    auth_user: Option<&str>,
    conditional: &ConditionalRequest,
) -> Response {
    if !valid_stage_segment(user) || !valid_stage_segment(index) {
        return TextResponse(StatusCode::BAD_REQUEST, "invalid stage\n".to_string())
            .into_response();
    }
    if let Err(response) = ensure_index_exists(state, user, index) {
        return *response;
    }
    if let Err(response) = ensure_pkg_read_allowed(state, user, index, auth_user) {
        return *response;
    }

    let config = match index_config_for_stage(state, user, index) {
        Ok(config) => config,
        Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
            return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
        }
        Err(err) => {
            return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                .into_response();
        }
    };

    let mut pages = match local_root_pages_for_stage(state, user, index, auth_user) {
        Ok(local_pages) => local_pages,
        Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
            return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
        }
        Err(err) => {
            return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                .into_response();
        }
    };

    if config.index_type == "mirror" && config.mirror_no_project_list {
        return simple_page_response_with_current_serial(
            state,
            format,
            merge_root_page(&pages),
            merge_root_json(&pages),
            pypi_last_serial(&pages),
            conditional,
        );
    }

    match root_pages_for_index_config(state, user, index, &config) {
        Ok(result) => {
            record_upstream_reports(state, result.reports.clone());
            pages.extend(result.pages);
            if pages.is_empty()
                && let Some(response) = upstream_uncached_error_response(&result.reports)
            {
                return response;
            }
            simple_page_response_with_cache_policy(
                state,
                format,
                merge_root_page(&pages),
                merge_root_json(&pages),
                pypi_last_serial(&pages),
                upstream_stale_cache_used(&result.reports),
                conditional,
            )
        }
        Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
            TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response()
        }
        Err(err) => TextResponse(StatusCode::BAD_GATEWAY, format!("upstream error: {err}\n"))
            .into_response(),
    }
}

#[cfg(test)]
fn render_simple_project_sync(
    state: &AppState,
    user: &str,
    index: &str,
    project: &str,
    format: SimpleFormat,
    auth_user: Option<&str>,
    include_refresh_form: bool,
) -> Response {
    render_simple_project_sync_conditional(
        state,
        user,
        index,
        project,
        format,
        auth_user,
        &SimpleRenderOptions {
            include_refresh_form,
            conditional: ConditionalRequest::default(),
        },
    )
}

fn render_simple_project_sync_conditional(
    state: &AppState,
    user: &str,
    index: &str,
    project: &str,
    format: SimpleFormat,
    auth_user: Option<&str>,
    options: &SimpleRenderOptions,
) -> Response {
    if !valid_stage_segment(user) || !valid_stage_segment(index) {
        return TextResponse(StatusCode::BAD_REQUEST, "invalid stage\n".to_string())
            .into_response();
    }
    if let Err(response) = ensure_index_exists(state, user, index) {
        return *response;
    }
    if let Err(response) = ensure_pkg_read_allowed(state, user, index, auth_user) {
        return *response;
    }

    let normalized = normalize_project_name(project);
    if normalized.is_empty() {
        return TextResponse(StatusCode::NOT_FOUND, "not found\n".to_string()).into_response();
    }

    let mut pages = match local_project_pages_for_stage(state, user, index, &normalized, auth_user)
    {
        Ok(local_pages) => local_pages,
        Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
            return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
        }
        Err(err) => {
            return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                .into_response();
        }
    };
    let include_upstream =
        match should_include_upstream_for_project(state, user, index, &normalized, auth_user) {
            Ok(include_upstream) => include_upstream,
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
            }
            Err(err) => {
                return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                    .into_response();
            }
        };

    let mut stale_cache_used = false;
    let mut upstream_uncached_error = None;
    if include_upstream {
        let config = match index_config_for_stage(state, user, index) {
            Ok(config) => config,
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
            }
            Err(err) => {
                return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                    .into_response();
            }
        };

        match project_pages_for_index_config(state, user, index, &config, &normalized) {
            Ok(result) => {
                stale_cache_used = upstream_stale_cache_used(&result.reports);
                upstream_uncached_error = upstream_uncached_error_message(&result.reports);
                record_upstream_reports(state, result.reports.clone());
                match mirror_project_pages_for_stage_relative(state, user, index, result.pages) {
                    Ok(upstream_pages) => pages.extend(upstream_pages),
                    Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                        return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n"))
                            .into_response();
                    }
                    Err(err) => {
                        return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                            .into_response();
                    }
                }
            }
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
            }
            Err(err) => {
                return TextResponse(StatusCode::BAD_GATEWAY, format!("upstream error: {err}\n"))
                    .into_response();
            }
        }
    }

    let mut response = if pages.is_empty() {
        if let Some(error) = upstream_uncached_error {
            return upstream_error_response(&error);
        }
        simple_project_not_found_response(format, &normalized)
    } else {
        let html = if format == SimpleFormat::Html {
            let mut html = merge_project_page(&normalized, &pages);
            if !include_upstream {
                html = html_with_mirror_whitelist_notice(html);
            }
            if options.include_refresh_form {
                html = html_with_refresh_form(html, user, index, &normalized);
            }
            html
        } else {
            merge_project_page(&normalized, &pages)
        };
        simple_page_response_with_current_serial(
            state,
            format,
            html,
            merge_project_json(&normalized, &pages),
            pypi_last_serial(&pages),
            &options.conditional,
        )
    };
    if stale_cache_used {
        mark_stale_cache_response(&mut response);
    }
    response
}

fn simple_page_response_with_cache_policy(
    state: &AppState,
    format: SimpleFormat,
    html: String,
    json: String,
    pypi_last_serial: Option<u64>,
    stale_cache_used: bool,
    conditional: &ConditionalRequest,
) -> Response {
    let mut response = simple_page_response_with_current_serial(
        state,
        format,
        html,
        json,
        pypi_last_serial,
        conditional,
    );
    if stale_cache_used {
        mark_stale_cache_response(&mut response);
    }
    response
}

fn upstream_stale_cache_used(reports: &[FetchReport]) -> bool {
    reports
        .iter()
        .any(|report| report.from_cache && report.error.is_some())
}

fn upstream_uncached_error_message(reports: &[FetchReport]) -> Option<String> {
    reports
        .iter()
        .find(|report| !report.from_cache && report.error.is_some())
        .and_then(|report| report.error.clone())
}

fn upstream_uncached_error_response(reports: &[FetchReport]) -> Option<Response> {
    upstream_uncached_error_message(reports).map(|error| upstream_error_response(&error))
}

fn upstream_error_response(error: &str) -> Response {
    TextResponse(
        StatusCode::BAD_GATEWAY,
        format!("upstream error: {error}\n"),
    )
    .into_response()
}

fn mark_stale_cache_response(response: &mut Response) {
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("max-age=0"));
    response.headers_mut().insert(
        header::EXPIRES,
        HeaderValue::from_str(&fmt_http_date(SystemTime::UNIX_EPOCH))
            .unwrap_or_else(|_| HeaderValue::from_static("Thu, 01 Jan 1970 00:00:00 GMT")),
    );
    response
        .headers_mut()
        .insert(header::PRAGMA, HeaderValue::from_static("no-cache"));
}

fn refresh_simple_project_sync(
    state: &AppState,
    user: &str,
    index: &str,
    project: &str,
    auth_user: Option<&str>,
) -> Response {
    if !valid_stage_segment(user) || !valid_stage_segment(index) {
        return TextResponse(StatusCode::BAD_REQUEST, "invalid stage\n".to_string())
            .into_response();
    }
    if let Err(response) = ensure_index_exists(state, user, index) {
        return *response;
    }
    if let Err(response) = ensure_pkg_read_allowed(state, user, index, auth_user) {
        return *response;
    }

    let normalized = normalize_project_name(project);
    if normalized.is_empty() {
        return TextResponse(StatusCode::NOT_FOUND, "not found\n".to_string()).into_response();
    }

    let config = match index_config_for_stage(state, user, index) {
        Ok(config) => config,
        Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
            return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
        }
        Err(err) => {
            return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                .into_response();
        }
    };

    if config.index_type == "mirror" && config.mirror_url.is_some() {
        if let Err(err) = state
            .index
            .clear_project_cache_for_source(&format!("{user}/{index}"), &normalized)
        {
            return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                .into_response();
        }
    } else if let Err(err) = state
        .index
        .clear_project_cache_for_sources(&normalized, &config.sources)
    {
        return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n")).into_response();
    }

    match project_pages_for_index_config(state, user, index, &config, &normalized) {
        Ok(result) => record_upstream_reports(state, result.reports),
        Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
            return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
        }
        Err(err) => {
            return TextResponse(StatusCode::BAD_GATEWAY, format!("upstream error: {err}\n"))
                .into_response();
        }
    }

    let mut response = TextResponse(StatusCode::SEE_OTHER, String::new()).into_response();
    response.headers_mut().insert(
        header::LOCATION,
        HeaderValue::from_str(&format!("/{user}/{index}/+simple/{normalized}/"))
            .unwrap_or_else(|_| HeaderValue::from_static("/")),
    );
    response
}

fn local_root_pages_for_stage(
    state: &AppState,
    user: &str,
    index: &str,
    auth_user: Option<&str>,
) -> io::Result<Vec<crate::simple::SourcePage>> {
    let mut pages = Vec::new();
    collect_local_root_pages(
        state,
        user,
        index,
        auth_user,
        &mut HashSet::new(),
        &mut pages,
    )?;
    Ok(pages)
}

fn local_project_pages_for_stage(
    state: &AppState,
    user: &str,
    index: &str,
    project: &str,
    auth_user: Option<&str>,
) -> io::Result<Vec<crate::simple::SourcePage>> {
    let mut pages = Vec::new();
    collect_local_project_pages(
        state,
        user,
        index,
        project,
        auth_user,
        &mut HashSet::new(),
        &mut pages,
    )?;
    Ok(pages)
}

fn should_include_upstream_for_project(
    state: &AppState,
    user: &str,
    index: &str,
    project: &str,
    auth_user: Option<&str>,
) -> io::Result<bool> {
    if mirror_whitelist_allows_project_for_stage(state, user, index, project)? {
        return Ok(true);
    }
    if !stage_has_mirror_base(state, user, index, &mut HashSet::new())? {
        return Ok(true);
    }
    Ok(!local_project_exists_for_stage(
        state,
        user,
        index,
        project,
        auth_user,
        &mut HashSet::new(),
    )?)
}

fn mirror_whitelist_allows_project_for_stage(
    state: &AppState,
    user: &str,
    index: &str,
    project: &str,
) -> io::Result<bool> {
    let config = index_config_for_stage(state, user, index)?;
    let inheritance = config.mirror_whitelist_inheritance.as_str();
    let whitelist =
        inherited_mirror_whitelist(state, user, index, inheritance, &mut HashSet::new())?
            .unwrap_or_default();
    Ok(whitelist
        .iter()
        .any(|entry| entry == "*" || entry == project || normalize_project_name(entry) == project))
}

fn inherited_mirror_whitelist(
    state: &AppState,
    user: &str,
    index: &str,
    inheritance: &str,
    visited: &mut HashSet<String>,
) -> io::Result<Option<HashSet<String>>> {
    if !visited.insert(format!("{user}/{index}")) {
        return Ok(None);
    }
    let config = index_config_for_stage(state, user, index)?;
    if config.index_type == "mirror" {
        return Ok(None);
    }
    let mut whitelist = config.mirror_whitelist.into_iter().collect::<HashSet<_>>();
    for (base_user, base_index) in configured_bases(state, user, index)? {
        let Some(base_whitelist) =
            inherited_mirror_whitelist(state, &base_user, &base_index, inheritance, visited)?
        else {
            continue;
        };
        if inheritance == "union" {
            whitelist.extend(base_whitelist);
        } else {
            whitelist = whitelist
                .intersection(&base_whitelist)
                .cloned()
                .collect::<HashSet<_>>();
        }
    }
    Ok(Some(whitelist))
}

fn stage_has_mirror_base(
    state: &AppState,
    user: &str,
    index: &str,
    visited: &mut HashSet<String>,
) -> io::Result<bool> {
    if !visited.insert(format!("{user}/{index}")) {
        return Ok(false);
    }
    for (base_user, base_index) in configured_bases(state, user, index)? {
        let config = index_config_for_stage(state, &base_user, &base_index)?;
        if config.index_type == "mirror" {
            return Ok(true);
        }
        if stage_has_mirror_base(state, &base_user, &base_index, visited)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn local_project_exists_for_stage(
    state: &AppState,
    user: &str,
    index: &str,
    project: &str,
    auth_user: Option<&str>,
    visited: &mut HashSet<String>,
) -> io::Result<bool> {
    if !visited.insert(format!("{user}/{index}")) {
        return Ok(false);
    }
    if state.local.project_exists_in(user, index, project)? {
        return Ok(true);
    }
    for (base_user, base_index) in configured_bases(state, user, index)? {
        if !pkg_read_allowed(state, &base_user, &base_index, auth_user)? {
            continue;
        }
        let config = index_config_for_stage(state, &base_user, &base_index)?;
        if config.index_type == "mirror" {
            continue;
        }
        if local_project_exists_for_stage(
            state,
            &base_user,
            &base_index,
            project,
            auth_user,
            visited,
        )? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn project_versions_for_stage(
    state: &AppState,
    user: &str,
    index: &str,
    project: &str,
    include_bases: bool,
    auth_user: Option<&str>,
    visited: &mut HashSet<String>,
) -> io::Result<BTreeMap<String, FileMetadata>> {
    if !visited.insert(format!("{user}/{index}")) {
        return Ok(BTreeMap::new());
    }
    let mut versions = state
        .local
        .project_version_metadata_in(user, index, project)?;
    if include_bases {
        for (base_user, base_index) in configured_bases(state, user, index)? {
            if !pkg_read_allowed(state, &base_user, &base_index, auth_user)? {
                continue;
            }
            for (version, metadata) in project_versions_for_stage(
                state,
                &base_user,
                &base_index,
                project,
                include_bases,
                auth_user,
                visited,
            )? {
                versions.entry(version).or_insert(metadata);
            }
        }
    }
    Ok(versions)
}

fn project_files_for_stage(
    state: &AppState,
    user: &str,
    index: &str,
    project: &str,
    include_bases: bool,
    auth_user: Option<&str>,
    visited: &mut HashSet<String>,
) -> io::Result<Vec<LocalFile>> {
    if !visited.insert(format!("{user}/{index}")) {
        return Ok(Vec::new());
    }
    let mut files = state.local.files_in(user, index, project)?;
    let mut seen = files
        .iter()
        .map(|file| file.filename.clone())
        .collect::<HashSet<_>>();
    if include_bases {
        for (base_user, base_index) in configured_bases(state, user, index)? {
            if !pkg_read_allowed(state, &base_user, &base_index, auth_user)? {
                continue;
            }
            for file in project_files_for_stage(
                state,
                &base_user,
                &base_index,
                project,
                include_bases,
                auth_user,
                visited,
            )? {
                if seen.insert(file.filename.clone()) {
                    files.push(file);
                }
            }
        }
    }
    Ok(files)
}

fn project_file_metadata_for_stage(
    state: &AppState,
    user: &str,
    index: &str,
    project: &str,
    include_bases: bool,
    auth_user: Option<&str>,
    visited: &mut HashSet<String>,
) -> io::Result<BTreeMap<String, FileMetadata>> {
    if !visited.insert(format!("{user}/{index}")) {
        return Ok(BTreeMap::new());
    }
    let mut metadata = state.local.project_file_metadata_in(user, index, project)?;
    if include_bases {
        for (base_user, base_index) in configured_bases(state, user, index)? {
            if !pkg_read_allowed(state, &base_user, &base_index, auth_user)? {
                continue;
            }
            for (filename, file_metadata) in project_file_metadata_for_stage(
                state,
                &base_user,
                &base_index,
                project,
                include_bases,
                auth_user,
                visited,
            )? {
                metadata.entry(filename).or_insert(file_metadata);
            }
        }
    }
    Ok(metadata)
}

fn project_tox_results_for_stage(
    state: &AppState,
    user: &str,
    index: &str,
    project: &str,
    include_bases: bool,
    auth_user: Option<&str>,
    visited: &mut HashSet<String>,
) -> io::Result<BTreeMap<String, Vec<Value>>> {
    if !visited.insert(format!("{user}/{index}")) {
        return Ok(BTreeMap::new());
    }
    let mut tox_results = state.local.project_tox_results_in(user, index, project)?;
    if include_bases {
        for (base_user, base_index) in configured_bases(state, user, index)? {
            if !pkg_read_allowed(state, &base_user, &base_index, auth_user)? {
                continue;
            }
            for (filename, results) in project_tox_results_for_stage(
                state,
                &base_user,
                &base_index,
                project,
                include_bases,
                auth_user,
                visited,
            )? {
                tox_results.entry(filename).or_insert(results);
            }
        }
    }
    Ok(tox_results)
}

fn versions_with_file_versions(
    project: &str,
    files: &[LocalFile],
    mut versions: BTreeMap<String, FileMetadata>,
) -> BTreeMap<String, FileMetadata> {
    for file in files {
        let version = infer_version(project, &file.filename);
        if !version.is_empty() {
            versions.entry(version).or_default();
        }
    }
    versions
}

fn root_pages_for_index_config(
    state: &AppState,
    user: &str,
    index: &str,
    config: &IndexConfig,
) -> io::Result<UpstreamResult> {
    if config.index_type == "mirror"
        && let Some(mirror_url) = &config.mirror_url
    {
        return Ok(state.index.root_pages_for_url_with_cache_expiry(
            &format!("{user}/{index}"),
            mirror_url,
            mirror_cache_expiry_duration(config),
            state.fetcher.as_ref(),
        ));
    }
    state
        .index
        .root_pages_for_sources(&config.sources, state.fetcher.as_ref())
}

fn project_pages_for_index_config(
    state: &AppState,
    user: &str,
    index: &str,
    config: &IndexConfig,
    project: &str,
) -> io::Result<UpstreamResult> {
    if config.index_type == "mirror"
        && let Some(mirror_url) = &config.mirror_url
    {
        return Ok(state.index.project_pages_for_url_with_cache_expiry(
            &format!("{user}/{index}"),
            mirror_url,
            project,
            mirror_cache_expiry_duration(config),
            state.fetcher.as_ref(),
        ));
    }
    state
        .index
        .project_pages_for_sources(project, &config.sources, state.fetcher.as_ref())
}

fn mirror_cache_expiry_duration(config: &IndexConfig) -> Option<Duration> {
    config
        .mirror_cache_expiry
        .and_then(|seconds| u64::try_from(seconds).ok())
        .map(Duration::from_secs)
}

fn mirror_project_pages_for_stage(
    state: &AppState,
    user: &str,
    index: &str,
    pages: Vec<crate::simple::SourcePage>,
) -> io::Result<Vec<crate::simple::SourcePage>> {
    let config = index_config_for_stage(state, user, index)?;
    if config.index_type != "mirror" || config.mirror_use_external_urls {
        return Ok(pages);
    }
    Ok(pages
        .into_iter()
        .map(|page| {
            rewrite_project_page_hrefs(&page, |href| mirror_absolute_href(user, index, href))
        })
        .collect())
}

fn mirror_project_pages_for_stage_relative(
    state: &AppState,
    user: &str,
    index: &str,
    pages: Vec<crate::simple::SourcePage>,
) -> io::Result<Vec<crate::simple::SourcePage>> {
    let config = index_config_for_stage(state, user, index)?;
    if config.index_type != "mirror" || config.mirror_use_external_urls {
        return Ok(pages);
    }
    Ok(pages
        .into_iter()
        .map(|page| rewrite_project_page_hrefs(&page, mirror_relative_href))
        .collect())
}

fn collect_local_root_pages(
    state: &AppState,
    user: &str,
    index: &str,
    auth_user: Option<&str>,
    visited: &mut HashSet<String>,
    pages: &mut Vec<crate::simple::SourcePage>,
) -> io::Result<()> {
    if !visited.insert(format!("{user}/{index}")) {
        return Ok(());
    }
    if let Some(page) = state.local.root_page_in(user, index)? {
        pages.push(page);
    }
    for (base_user, base_index) in configured_bases(state, user, index)? {
        if !pkg_read_allowed(state, &base_user, &base_index, auth_user)? {
            continue;
        }
        collect_local_root_pages(state, &base_user, &base_index, auth_user, visited, pages)?;
    }
    Ok(())
}

fn collect_local_project_pages(
    state: &AppState,
    user: &str,
    index: &str,
    project: &str,
    auth_user: Option<&str>,
    visited: &mut HashSet<String>,
    pages: &mut Vec<crate::simple::SourcePage>,
) -> io::Result<()> {
    if !visited.insert(format!("{user}/{index}")) {
        return Ok(());
    }
    if let Some(page) = state.local.project_page_in(user, index, project)? {
        pages.push(page);
    }
    for (base_user, base_index) in configured_bases(state, user, index)? {
        if !pkg_read_allowed(state, &base_user, &base_index, auth_user)? {
            continue;
        }
        collect_local_project_pages(
            state,
            &base_user,
            &base_index,
            project,
            auth_user,
            visited,
            pages,
        )?;
    }
    Ok(())
}

fn configured_bases(
    state: &AppState,
    user: &str,
    index: &str,
) -> io::Result<Vec<(String, String)>> {
    let config = index_config_for_stage(state, user, index)?;
    config
        .bases
        .iter()
        .map(|base| {
            let Some((base_user, base_index)) = base.trim_matches('/').split_once('/') else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "base must use user/index format",
                ));
            };
            if !valid_stage_segment(base_user) || !valid_stage_segment(base_index) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid base stage",
                ));
            }
            Ok((base_user.to_string(), base_index.to_string()))
        })
        .collect()
}

fn index_config_for_stage(state: &AppState, user: &str, index: &str) -> io::Result<IndexConfig> {
    Ok(state
        .registry
        .index(user, index)?
        .unwrap_or_else(|| default_stage_config(user)))
}

fn ensure_index_exists(state: &AppState, user: &str, index: &str) -> Result<(), Box<Response>> {
    match state.registry.index(user, index) {
        Ok(Some(_)) => Ok(()),
        Ok(None) => Err(Box::new(
            TextResponse(StatusCode::NOT_FOUND, "index not found\n".to_string()).into_response(),
        )),
        Err(err) if err.kind() == io::ErrorKind::InvalidInput => Err(Box::new(
            TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response(),
        )),
        Err(err) => Err(Box::new(
            TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n")).into_response(),
        )),
    }
}

fn authenticated_user(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<Option<String>, Box<Response>> {
    if let Some(value) = headers.get("x-devpi-auth") {
        let Ok(value) = value.to_str() else {
            return Err(Box::new(unauthorized_response("invalid x-devpi-auth")));
        };
        let (username, password) = decode_credentials(value.trim(), "invalid x-devpi-auth")?;
        return verify_credentials(state, &username, &password);
    }

    let Some(value) = headers.get(header::AUTHORIZATION) else {
        return Ok(None);
    };
    let Ok(value) = value.to_str() else {
        return Err(Box::new(unauthorized_response(
            "invalid authorization header",
        )));
    };
    let Some((scheme, encoded)) = value.split_once(char::is_whitespace) else {
        return Err(Box::new(unauthorized_response(
            "unsupported authorization scheme",
        )));
    };
    if !scheme.eq_ignore_ascii_case("Basic") {
        return Err(Box::new(unauthorized_response(
            "unsupported authorization scheme",
        )));
    }
    let (username, password) = decode_credentials(encoded.trim(), "invalid basic auth")?;
    verify_credentials(state, &username, &password)
}

fn api_authstatus(state: &AppState, headers: &HeaderMap) -> Result<Value, Box<Response>> {
    match authenticated_user(state, headers)? {
        Some(user) => Ok(json!(["ok", user, []])),
        None => Ok(json!(["noauth", "", []])),
    }
}

fn status_indexes(state: &AppState) -> Result<Vec<Value>, Box<Response>> {
    let global_sources = state.index.source_names();
    match state.registry.users() {
        Ok(users) => Ok(users
            .into_iter()
            .flat_map(|(user, config)| {
                let global_sources = global_sources.clone();
                config.indexes.into_iter().map(move |(index, ixconfig)| {
                    let resolved_sources = if ixconfig.sources.is_empty() {
                        global_sources.clone()
                    } else {
                        ixconfig.sources.clone()
                    };
                    json!({
                        "name": format!("{user}/{index}"),
                        "type": ixconfig.index_type,
                        "bases": ixconfig.bases,
                        "sources": ixconfig.sources,
                        "resolved_sources": resolved_sources,
                    })
                })
            })
            .collect()),
        Err(err) => Err(Box::new(
            TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n")).into_response(),
        )),
    }
}

fn status_upstream_reports(state: &AppState) -> Result<Vec<Value>, Box<Response>> {
    match state.upstream_reports.lock() {
        Ok(reports) => Ok(reports
            .iter()
            .map(|report| {
                json!({
                    "source": report.source,
                    "url": report.url,
                    "fetched_at_unix_secs": report.fetched_at_unix_secs,
                    "from_cache": report.from_cache,
                    "error": report.error,
                })
            })
            .collect()),
        Err(err) => Err(Box::new(
            TextResponse(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("upstream report lock poisoned: {err}\n"),
            )
            .into_response(),
        )),
    }
}

fn record_upstream_reports(state: &AppState, reports: Vec<FetchReport>) {
    if let Ok(mut upstream_reports) = state.upstream_reports.lock() {
        *upstream_reports = reports;
    }
}

enum AuthcheckTarget {
    AlwaysOk,
    Known,
    PackageRead { user: String, index: String },
    Unknown,
}

fn original_uri_path(headers: &HeaderMap) -> Option<String> {
    let value = headers
        .get("x-original-uri")
        .or_else(|| headers.get("x-original-url"))?;
    let value = value.to_str().ok()?.trim();
    if value.is_empty() {
        return Some("/".to_string());
    }
    Some(path_from_original_uri(value))
}

fn path_from_original_uri(value: &str) -> String {
    let without_fragment = value.split_once('#').map_or(value, |(path, _)| path);
    let without_query = without_fragment
        .split_once('?')
        .map_or(without_fragment, |(path, _)| path);
    if let Some((_, after_scheme)) = without_query.split_once("://") {
        return after_scheme
            .find('/')
            .map(|pos| after_scheme[pos..].to_string())
            .unwrap_or_else(|| "/".to_string());
    }
    if without_query.starts_with('/') {
        without_query.to_string()
    } else {
        format!("/{without_query}")
    }
}

fn mirror_absolute_href(user: &str, index: &str, href: &str) -> String {
    mirror_prefixed_href(&format!("/{user}/{index}/+e"), href)
}

fn mirror_relative_href(href: &str) -> String {
    mirror_prefixed_href("../../+e", href)
}

fn mirror_prefixed_href(prefix: &str, href: &str) -> String {
    let (url, fragment) = href
        .split_once('#')
        .map_or((href, ""), |(url, fragment)| (url, fragment));
    let filename = mirror_filename_from_url(url).unwrap_or("download");
    let encoded_url = encode_mirror_url(url);
    let mut local = format!("{prefix}/{encoded_url}/{filename}");
    if !fragment.is_empty() {
        local.push('#');
        local.push_str(fragment);
    }
    local
}

fn mirror_relpath_parts(relpath: &str) -> Option<(&str, &str)> {
    let relpath = relpath.trim_end_matches('/');
    let (encoded_url, filename) = relpath.split_once('/')?;
    if encoded_url.is_empty() || filename.is_empty() || filename.contains('/') {
        return None;
    }
    Some((encoded_url, filename))
}

fn mirror_filename_matches_url(filename: &str, url: &str) -> bool {
    mirror_filename_from_url(url) == Some(filename)
}

fn mirror_metadata_url(url: &str) -> String {
    if let Some((base, query)) = url.split_once('?') {
        format!("{base}.metadata?{query}")
    } else {
        format!("{url}.metadata")
    }
}

fn mirror_url_allowed(config: &IndexConfig, url: &str) -> bool {
    let Some(mirror_url) = &config.mirror_url else {
        return true;
    };
    let Some((mirror_origin, _)) = split_url_origin_path(mirror_url) else {
        return false;
    };
    let Some((url_origin, _)) = split_url_origin_path(url) else {
        return false;
    };
    mirror_origin.eq_ignore_ascii_case(url_origin)
}

fn split_url_origin_path(url: &str) -> Option<(&str, &str)> {
    let scheme_end = url.find("://")? + 3;
    let path_start = url[scheme_end..]
        .find('/')
        .map(|pos| scheme_end + pos)
        .unwrap_or(url.len());
    Some((&url[..path_start], &url[path_start..]))
}

fn mirror_filename_from_url(url: &str) -> Option<&str> {
    let without_query = url.split_once('?').map_or(url, |(url, _)| url);
    without_query
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .filter(|filename| !filename.is_empty())
}

fn encode_mirror_url(url: &str) -> String {
    let mut encoded = String::from("u");
    for byte in url.as_bytes() {
        encoded.push(hex_digit(byte >> 4));
        encoded.push(hex_digit(byte & 0x0f));
    }
    encoded
}

fn decode_mirror_url(value: &str) -> Option<String> {
    let hex = value.strip_prefix('u')?;
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for chunk in hex.as_bytes().chunks_exact(2) {
        let high = hex_value(chunk[0])?;
        let low = hex_value(chunk[1])?;
        bytes.push((high << 4) | low);
    }
    let url = String::from_utf8(bytes).ok()?;
    (url.starts_with("http://") || url.starts_with("https://")).then_some(url)
}

fn hex_digit(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'a' + value - 10) as char,
        _ => '0',
    }
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn authcheck_target(path: &str) -> AuthcheckTarget {
    let segments: Vec<&str> = path
        .trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    match segments.as_slice() {
        [] => AuthcheckTarget::Known,
        ["+api" | "+login" | "+status" | "+authcheck"] => AuthcheckTarget::AlwaysOk,
        ["+changelog", _] => AuthcheckTarget::Known,
        [segment] if segment.starts_with('+') => AuthcheckTarget::Unknown,
        [_] => AuthcheckTarget::Known,
        ["simple", _] => AuthcheckTarget::Known,
        ["files", _, _] => AuthcheckTarget::Known,
        [_, "+api"] => AuthcheckTarget::AlwaysOk,
        [_, segment] if segment.starts_with('+') => AuthcheckTarget::Unknown,
        [_, _] => AuthcheckTarget::Known,
        [user, index, "+api"] => {
            if valid_stage_segment(user) && valid_stage_segment(index) {
                AuthcheckTarget::AlwaysOk
            } else {
                AuthcheckTarget::Unknown
            }
        }
        [user, index, "+f" | "+e", ..] => {
            if valid_stage_segment(user) && valid_stage_segment(index) {
                AuthcheckTarget::PackageRead {
                    user: (*user).to_string(),
                    index: (*index).to_string(),
                }
            } else {
                AuthcheckTarget::Unknown
            }
        }
        [_, _, "+simple", ..] => AuthcheckTarget::Known,
        [_, _, segment, ..] if segment.starts_with('+') => AuthcheckTarget::Unknown,
        [_, _, _, ..] => AuthcheckTarget::Known,
    }
}

fn ensure_known_sources(state: &AppState, sources: Option<&[String]>) -> Result<(), Box<Response>> {
    let Some(sources) = sources else {
        return Ok(());
    };
    let known = state.index.source_names();
    for source in sources {
        let source = source.trim();
        if !known.iter().any(|known| known == source) {
            return Err(Box::new(
                TextResponse(
                    StatusCode::BAD_REQUEST,
                    format!("unknown upstream source {source:?}\n"),
                )
                .into_response(),
            ));
        }
    }
    Ok(())
}

fn verify_credentials(
    state: &AppState,
    username: &str,
    password: &str,
) -> Result<Option<String>, Box<Response>> {
    match state.registry.verify_password(username, password) {
        Ok(true) => Ok(Some(username.to_string())),
        Ok(false) => Err(Box::new(unauthorized_response(
            "invalid username or password",
        ))),
        Err(err) if err.kind() == io::ErrorKind::InvalidInput => Err(Box::new(
            unauthorized_response("invalid username or password"),
        )),
        Err(err) => Err(Box::new(
            TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n")).into_response(),
        )),
    }
}

fn decode_credentials(
    encoded: &str,
    message: &'static str,
) -> Result<(String, String), Box<Response>> {
    let decoded = decode_base64(encoded)
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .ok_or_else(|| Box::new(unauthorized_response(message)))?;
    let Some((username, password)) = decoded.split_once(':') else {
        return Err(Box::new(unauthorized_response(message)));
    };
    Ok((username.to_string(), password.to_string()))
}

fn ensure_index_modify_allowed(
    user: &str,
    index: &str,
    auth_user: Option<&str>,
) -> Result<(), Box<Response>> {
    let Some(auth_user) = auth_user else {
        return Err(Box::new(unauthorized_response("authentication required")));
    };
    if auth_user == "root" || auth_user == user {
        Ok(())
    } else {
        Err(Box::new(
            TextResponse(
                StatusCode::FORBIDDEN,
                format!("user {auth_user:?} cannot modify index {user}/{index}\n"),
            )
            .into_response(),
        ))
    }
}

fn ensure_index_create_allowed(user: &str, auth_user: Option<&str>) -> Result<(), Box<Response>> {
    let Some(auth_user) = auth_user else {
        return Ok(());
    };
    if auth_user == "root" || auth_user == user {
        Ok(())
    } else {
        Err(Box::new(
            TextResponse(
                StatusCode::FORBIDDEN,
                format!("user {auth_user:?} cannot create index for {user}\n"),
            )
            .into_response(),
        ))
    }
}

fn ensure_user_modify_allowed(user: &str, auth_user: Option<&str>) -> Result<(), Box<Response>> {
    let Some(auth_user) = auth_user else {
        return Err(Box::new(unauthorized_response("authentication required")));
    };
    if auth_user == "root" || auth_user == user {
        Ok(())
    } else {
        Err(Box::new(
            TextResponse(
                StatusCode::FORBIDDEN,
                format!("user {auth_user:?} cannot modify user {user}\n"),
            )
            .into_response(),
        ))
    }
}

fn ensure_acl_upload_allowed(
    state: &AppState,
    user: &str,
    index: &str,
    auth_user: Option<&str>,
) -> Result<(), Box<Response>> {
    let config = match state.registry.index(user, index) {
        Ok(Some(config)) => config,
        Ok(None) => return Ok(()),
        Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
            return Err(Box::new(
                TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response(),
            ));
        }
        Err(err) => {
            return Err(Box::new(
                TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n")).into_response(),
            ));
        }
    };
    if config.acl_upload.iter().any(|entry| entry == ":ANONYMOUS:")
        || is_open_root_pypi_mirror(user, index, &config)
    {
        return Ok(());
    }
    let Some(auth_user) = auth_user else {
        return Err(Box::new(unauthorized_response("authentication required")));
    };
    if acl_allows_authenticated_user(&config.acl_upload, auth_user) {
        Ok(())
    } else {
        Err(Box::new(
            TextResponse(
                StatusCode::FORBIDDEN,
                format!("user {auth_user:?} cannot upload to {user}/{index}\n"),
            )
            .into_response(),
        ))
    }
}

fn is_open_root_pypi_mirror(user: &str, index: &str, config: &IndexConfig) -> bool {
    user == "root"
        && index == "pypi"
        && config.index_type == "mirror"
        && config.acl_upload.is_empty()
}

fn ensure_toxresult_upload_allowed(
    state: &AppState,
    user: &str,
    index: &str,
    auth_user: Option<&str>,
) -> Result<(), Box<Response>> {
    let config = match state.registry.index(user, index) {
        Ok(Some(config)) => config,
        Ok(None) => return Ok(()),
        Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
            return Err(Box::new(
                TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response(),
            ));
        }
        Err(err) => {
            return Err(Box::new(
                TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n")).into_response(),
            ));
        }
    };
    if config
        .acl_toxresult_upload
        .iter()
        .any(|entry| entry == ":ANONYMOUS:")
    {
        return Ok(());
    }
    let Some(auth_user) = auth_user else {
        return Err(Box::new(unauthorized_response("authentication required")));
    };
    if acl_allows_authenticated_user(&config.acl_toxresult_upload, auth_user) {
        Ok(())
    } else {
        Err(Box::new(
            TextResponse(
                StatusCode::FORBIDDEN,
                format!("user {auth_user:?} cannot upload tox results to {user}/{index}\n"),
            )
            .into_response(),
        ))
    }
}

fn ensure_pkg_read_allowed(
    state: &AppState,
    user: &str,
    index: &str,
    auth_user: Option<&str>,
) -> Result<(), Box<Response>> {
    match pkg_read_allowed(state, user, index, auth_user) {
        Ok(true) => Ok(()),
        Ok(false) => Err(Box::new(
            TextResponse(
                StatusCode::FORBIDDEN,
                "package read forbidden\n".to_string(),
            )
            .into_response(),
        )),
        Err(err) if err.kind() == io::ErrorKind::InvalidInput => Err(Box::new(
            TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response(),
        )),
        Err(err) => Err(Box::new(
            TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n")).into_response(),
        )),
    }
}

fn pkg_read_allowed(
    state: &AppState,
    user: &str,
    index: &str,
    auth_user: Option<&str>,
) -> io::Result<bool> {
    let config = index_config_for_stage(state, user, index)?;
    if config
        .acl_pkg_read
        .iter()
        .any(|entry| entry == ":ANONYMOUS:")
    {
        return Ok(true);
    }
    let Some(auth_user) = auth_user else {
        return Ok(false);
    };
    Ok(auth_user == "root" || acl_allows_authenticated_user(&config.acl_pkg_read, auth_user))
}

fn acl_allows_authenticated_user(acl: &[String], auth_user: &str) -> bool {
    acl.iter()
        .any(|entry| entry == ":AUTHENTICATED:" || entry == auth_user)
}

fn unauthorized_response(message: &str) -> Response {
    let mut response =
        TextResponse(StatusCode::UNAUTHORIZED, format!("{message}\n")).into_response();
    response.headers_mut().insert(
        header::WWW_AUTHENTICATE,
        HeaderValue::from_static("Basic realm=\"devpi-rs\""),
    );
    response
}

fn decode_base64(value: &str) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    let mut chunk = [0_u8; 4];
    let mut chunk_len = 0;

    for byte in value.bytes().filter(|byte| !byte.is_ascii_whitespace()) {
        let decoded = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            b'=' => 64,
            _ => return None,
        };
        chunk[chunk_len] = decoded;
        chunk_len += 1;
        if chunk_len == 4 {
            if chunk[0] == 64 || chunk[1] == 64 {
                return None;
            }
            out.push((chunk[0] << 2) | (chunk[1] >> 4));
            if chunk[2] != 64 {
                out.push((chunk[1] << 4) | (chunk[2] >> 2));
            }
            if chunk[3] != 64 {
                out.push((chunk[2] << 6) | chunk[3]);
            }
            chunk_len = 0;
        }
    }

    if chunk_len == 0 { Some(out) } else { None }
}

fn encode_base64(value: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in value.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        out.push(TABLE[(b0 >> 2) as usize] as char);
        out.push(TABLE[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(b2 & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

fn render_stage_index_sync(
    state: &AppState,
    user: &str,
    index: &str,
    include_projects: bool,
) -> Response {
    if !valid_stage_segment(user) || !valid_stage_segment(index) {
        return TextResponse(StatusCode::BAD_REQUEST, "invalid stage\n".to_string())
            .into_response();
    }

    match state.local.projects_in(user, index) {
        Ok(projects) => {
            let ixconfig = match state.registry.index(user, index) {
                Ok(Some(config)) => config,
                Ok(None) => {
                    return TextResponse(StatusCode::NOT_FOUND, "index not found\n".to_string())
                        .into_response();
                }
                Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                    return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n"))
                        .into_response();
                }
                Err(err) => {
                    return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                        .into_response();
                }
            };
            let source_names = state.index.source_names();
            let response = JsonText(
                StatusCode::OK,
                stage_index_json(
                    user,
                    index,
                    include_projects.then_some(projects.as_slice()),
                    &ixconfig,
                    &source_names,
                ),
            )
            .into_response();
            with_current_serial(state, response)
        }
        Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
            TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response()
        }
        Err(err) => {
            TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n")).into_response()
        }
    }
}

fn render_stage_project_sync(
    state: &AppState,
    user: &str,
    index: &str,
    project: &str,
    include_bases: bool,
    auth_user: Option<&str>,
) -> Response {
    if !valid_stage_segment(user) || !valid_stage_segment(index) {
        return TextResponse(StatusCode::BAD_REQUEST, "invalid stage\n".to_string())
            .into_response();
    }
    if let Err(response) = ensure_index_exists(state, user, index) {
        return *response;
    }
    if let Err(response) = ensure_pkg_read_allowed(state, user, index, auth_user) {
        return *response;
    }

    let normalized = normalize_project_name(project);
    if normalized.is_empty() {
        return TextResponse(StatusCode::NOT_FOUND, "not found\n".to_string()).into_response();
    }

    let metadata = match project_file_metadata_for_stage(
        state,
        user,
        index,
        &normalized,
        include_bases,
        auth_user,
        &mut HashSet::new(),
    ) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
            return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
        }
        Err(err) => {
            return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                .into_response();
        }
    };
    let tox_results = match project_tox_results_for_stage(
        state,
        user,
        index,
        &normalized,
        include_bases,
        auth_user,
        &mut HashSet::new(),
    ) {
        Ok(tox_results) => tox_results,
        Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
            return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
        }
        Err(err) => {
            return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                .into_response();
        }
    };
    let versions = match project_versions_for_stage(
        state,
        user,
        index,
        &normalized,
        include_bases,
        auth_user,
        &mut HashSet::new(),
    ) {
        Ok(versions) => versions,
        Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
            return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
        }
        Err(err) => {
            return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                .into_response();
        }
    };

    let files = match project_files_for_stage(
        state,
        user,
        index,
        &normalized,
        include_bases,
        auth_user,
        &mut HashSet::new(),
    ) {
        Ok(files) => files,
        Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
            return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
        }
        Err(err) => {
            return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                .into_response();
        }
    };
    let versions = versions_with_file_versions(&normalized, &files, versions);

    if files.is_empty() {
        match state.local.project_exists_in(user, index, &normalized) {
            Ok(false) if versions.is_empty() => {
                match missing_stage_project_upstream_response(
                    state,
                    user,
                    index,
                    &normalized,
                    auth_user,
                ) {
                    Ok(Some(response)) => return response,
                    Ok(None) => {}
                    Err(response) => return *response,
                }
                return TextResponse(StatusCode::NOT_FOUND, "project not found\n".to_string())
                    .into_response();
            }
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                return TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response();
            }
            Err(err) => {
                return TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                    .into_response();
            }
            Ok(_) => {}
        }
    }

    let response = JsonText(
        StatusCode::OK,
        stage_project_json(
            user,
            index,
            &normalized,
            &files,
            &metadata,
            &tox_results,
            &versions,
        ),
    )
    .into_response();
    with_current_serial(state, response)
}

fn missing_stage_project_upstream_response(
    state: &AppState,
    user: &str,
    index: &str,
    project: &str,
    auth_user: Option<&str>,
) -> Result<Option<Response>, Box<Response>> {
    let include_upstream =
        match should_include_upstream_for_project(state, user, index, project, auth_user) {
            Ok(include_upstream) => include_upstream,
            Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
                return Err(Box::new(
                    TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response(),
                ));
            }
            Err(err) => {
                return Err(Box::new(
                    TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n"))
                        .into_response(),
                ));
            }
        };
    if !include_upstream {
        return Ok(None);
    }

    let config = match index_config_for_stage(state, user, index) {
        Ok(config) => config,
        Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
            return Err(Box::new(
                TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response(),
            ));
        }
        Err(err) => {
            return Err(Box::new(
                TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n")).into_response(),
            ));
        }
    };

    let result = match project_pages_for_index_config(state, user, index, &config, project) {
        Ok(result) => result,
        Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
            return Err(Box::new(
                TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response(),
            ));
        }
        Err(err) => {
            return Err(Box::new(upstream_error_response(&err.to_string())));
        }
    };
    record_upstream_reports(state, result.reports.clone());
    let uncached_error_response = upstream_uncached_error_response(&result.reports);
    let pages = match mirror_project_pages_for_stage(state, user, index, result.pages) {
        Ok(pages) => pages,
        Err(err) if err.kind() == io::ErrorKind::InvalidInput => {
            return Err(Box::new(
                TextResponse(StatusCode::BAD_REQUEST, format!("{err}\n")).into_response(),
            ));
        }
        Err(err) => {
            return Err(Box::new(
                TextResponse(StatusCode::INTERNAL_SERVER_ERROR, format!("{err}\n")).into_response(),
            ));
        }
    };
    if pages.is_empty() {
        return Ok(uncached_error_response);
    }

    let response = stage_project_response_from_simple_pages(state, user, index, project, &pages)?;
    Ok(Some(response))
}

fn stage_project_response_from_simple_pages(
    state: &AppState,
    user: &str,
    index: &str,
    project: &str,
    pages: &[SourcePage],
) -> Result<Response, Box<Response>> {
    let project_json = merge_project_json(project, pages);
    let value: Value = serde_json::from_str(&project_json).map_err(|err| {
        Box::new(
            TextResponse(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("invalid upstream project json: {err}\n"),
            )
            .into_response(),
        )
    })?;
    let mut files = Vec::new();
    if let Some(upstream_files) = value["files"].as_array() {
        for file in upstream_files {
            let Some(filename) = file["filename"].as_str() else {
                continue;
            };
            let Some(url) = file["url"].as_str() else {
                continue;
            };
            let mut entry = json!({
                "filename": filename,
                "url": url,
                "version": infer_version(project, filename),
            });
            if let Some(hashes) = file.get("hashes") {
                entry["hashes"] = hashes.clone();
            }
            if let Some(requires_python) = file.get("requires-python") {
                entry["requires_python"] = requires_python.clone();
            }
            if let Some(yanked) = file.get("yanked") {
                entry["yanked"] = yanked.clone();
            }
            files.push(entry);
        }
    }
    let response = json_value_response(
        StatusCode::OK,
        json!({
            "status": 200,
            "type": "projectconfig",
            "result": {
                "user": user,
                "index": index,
                "project": project,
                "files": files,
                "versions": {},
            },
        }),
    );
    Ok(with_current_serial(state, response))
}

struct HtmlResponse(StatusCode, String);

impl IntoResponse for HtmlResponse {
    fn into_response(self) -> Response {
        with_content_type(self.0, "text/html; charset=utf-8", self.1.into_bytes())
    }
}

struct JsonText(StatusCode, String);

impl IntoResponse for JsonText {
    fn into_response(self) -> Response {
        with_content_type(
            self.0,
            "application/json; charset=utf-8",
            self.1.into_bytes(),
        )
    }
}

fn json_value_response(status: StatusCode, value: Value) -> Response {
    JsonText(status, format!("{value}\n")).into_response()
}

fn json_message_response(status: StatusCode, message: impl Into<String>) -> Response {
    json_value_response(
        status,
        json!({
            "status": status.as_u16(),
            "message": message.into(),
        }),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SimpleFormat {
    Html,
    Json,
}

#[derive(Debug, Clone, Default)]
struct ConditionalRequest {
    if_none_match: Option<String>,
    if_modified_since: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct SimpleRenderOptions {
    include_refresh_form: bool,
    conditional: ConditionalRequest,
}

#[derive(Debug, Clone, Default)]
struct FileReadOptions {
    json_preferred: bool,
    auth_user: Option<String>,
    conditional: ConditionalRequest,
}

fn conditional_request(headers: &HeaderMap) -> ConditionalRequest {
    ConditionalRequest {
        if_none_match: headers
            .get(header::IF_NONE_MATCH)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string),
        if_modified_since: headers
            .get(header::IF_MODIFIED_SINCE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string),
    }
}

fn simple_format(headers: &HeaderMap) -> SimpleFormat {
    if headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .map(wants_simple_json)
        .unwrap_or(false)
    {
        SimpleFormat::Json
    } else {
        SimpleFormat::Html
    }
}

fn should_redirect_simple_html(headers: &HeaderMap) -> bool {
    simple_format(headers) == SimpleFormat::Html && !is_installer_request(headers)
}

fn should_embed_simple_refresh_form(headers: &HeaderMap) -> bool {
    simple_format(headers) == SimpleFormat::Html && !is_installer_request(headers)
}

fn is_installer_request(headers: &HeaderMap) -> bool {
    headers
        .get(header::USER_AGENT)
        .and_then(|value| value.to_str().ok())
        .map(|user_agent| {
            let user_agent = user_agent.to_ascii_lowercase();
            user_agent.contains("pip/")
                || user_agent.contains("setuptools")
                || user_agent.contains("python-urllib")
                || user_agent.contains("pex/")
                || user_agent.contains("uv/")
        })
        .unwrap_or(false)
}

fn wants_json_response(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .map(|accept| {
            accept
                .split(',')
                .filter_map(|item| item.split(';').next())
                .map(str::trim)
                .any(|media_type| {
                    media_type.eq_ignore_ascii_case("application/json")
                        || media_type.ends_with("+json")
                })
        })
        .unwrap_or(false)
}

fn is_multipart_request(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|media_type| {
            media_type
                .trim()
                .eq_ignore_ascii_case("multipart/form-data")
        })
}

fn is_urlencoded_request(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|media_type| {
            media_type
                .trim()
                .eq_ignore_ascii_case("application/x-www-form-urlencoded")
        })
}

fn parse_urlencoded_fields(body: &[u8]) -> io::Result<Vec<(String, String)>> {
    let text = std::str::from_utf8(body).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("form body is not utf-8: {err}"),
        )
    })?;
    let mut fields = Vec::new();
    for pair in text.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        fields.push((decode_form_component(key)?, decode_form_component(value)?));
    }
    Ok(fields)
}

fn decode_form_component(value: &str) -> io::Result<String> {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                out.push(b' ');
                index += 1;
            }
            b'%' => {
                let high = bytes.get(index + 1).copied();
                let low = bytes.get(index + 2).copied();
                let Some(high) = high.and_then(hex_value) else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "invalid percent escape",
                    ));
                };
                let Some(low) = low.and_then(hex_value) else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "invalid percent escape",
                    ));
                };
                out.push((high << 4) | low);
                index += 3;
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8(out).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("decoded form component is not utf-8: {err}"),
        )
    })
}

fn wants_simple_json(accept: &str) -> bool {
    accept
        .split(',')
        .filter_map(|item| item.split(';').next())
        .any(|media_type| {
            media_type
                .trim()
                .eq_ignore_ascii_case(SIMPLE_JSON_CONTENT_TYPE)
        })
}

fn simple_page_response_conditional(
    format: SimpleFormat,
    html: String,
    json: String,
    conditional: &ConditionalRequest,
) -> Response {
    let (content_type, body) = match format {
        SimpleFormat::Html => ("text/html; charset=utf-8", html.into_bytes()),
        SimpleFormat::Json => (SIMPLE_JSON_CONTENT_TYPE, json.into_bytes()),
    };
    let etag = format!("\"{}\"", sha256_hex(&body));
    let mut response = if conditional_etag_matches(conditional, &etag) {
        with_content_type(StatusCode::NOT_MODIFIED, content_type, Vec::new())
    } else {
        with_content_type(StatusCode::OK, content_type, body)
    };
    if let Ok(value) = HeaderValue::from_str(&etag) {
        response.headers_mut().insert(header::ETAG, value);
    }
    response
        .headers_mut()
        .insert(header::VARY, HeaderValue::from_static("Accept, User-Agent"));
    response
}

fn conditional_etag_matches(conditional: &ConditionalRequest, etag: &str) -> bool {
    conditional.if_none_match.as_deref().is_some_and(|value| {
        value
            .split(',')
            .map(str::trim)
            .any(|candidate| etag_candidate_matches(candidate, etag))
    })
}

fn etag_candidate_matches(candidate: &str, etag: &str) -> bool {
    candidate == "*"
        || candidate == etag
        || candidate
            .strip_prefix("W/")
            .or_else(|| candidate.strip_prefix("w/"))
            .is_some_and(|weak| weak == etag)
}

fn conditional_modified_since_matches(
    conditional: &ConditionalRequest,
    modified: SystemTime,
) -> bool {
    conditional
        .if_modified_since
        .as_deref()
        .map(str::trim)
        .is_some_and(|value| {
            fmt_http_date(modified) == value
                || httpdate::parse_http_date(value).is_ok_and(|since| modified <= since)
        })
}

fn simple_project_not_found_response(format: SimpleFormat, project: &str) -> Response {
    let mut response = match format {
        SimpleFormat::Html => HtmlResponse(
            StatusCode::NOT_FOUND,
            format!(
                "<!doctype html>\n<html><body><h1>No links for {}</h1></body></html>\n",
                escape_html_attr(project)
            ),
        )
        .into_response(),
        SimpleFormat::Json => with_content_type(
            StatusCode::NOT_FOUND,
            SIMPLE_JSON_CONTENT_TYPE,
            format!(
                "{}\n",
                json!({
                    "meta": {"api-version": "1.0"},
                    "name": project,
                    "files": [],
                })
            )
            .into_bytes(),
        ),
    };
    response
        .headers_mut()
        .insert(header::VARY, HeaderValue::from_static("Accept, User-Agent"));
    response
}

fn simple_page_response_with_current_serial(
    state: &AppState,
    format: SimpleFormat,
    html: String,
    json: String,
    pypi_last_serial: Option<u64>,
    conditional: &ConditionalRequest,
) -> Response {
    let mut response = with_current_serial(
        state,
        simple_page_response_conditional(format, html, json, conditional),
    );
    if let Some(serial) = pypi_last_serial {
        insert_header_u64(&mut response, "X-PYPI-LAST-SERIAL", serial);
    }
    response
}

fn pypi_last_serial(pages: &[SourcePage]) -> Option<u64> {
    pages.iter().filter_map(|page| page.pypi_last_serial).max()
}

fn redirect_response(location: &str) -> Response {
    redirect_response_with_status(StatusCode::FOUND, location)
}

fn redirect_response_with_status(status: StatusCode, location: &str) -> Response {
    let mut response = TextResponse(StatusCode::FOUND, String::new()).into_response();
    *response.status_mut() = status;
    let location =
        HeaderValue::from_str(location).unwrap_or_else(|_| HeaderValue::from_static("/"));
    response.headers_mut().insert(header::LOCATION, location);
    response
}

fn html_with_refresh_form(mut html: String, user: &str, index: &str, project: &str) -> String {
    let action = format!(
        "/{}/{}/+simple/{}/refresh",
        escape_html_attr(user),
        escape_html_attr(index),
        escape_html_attr(project)
    );
    let form = format!(
        "<form action=\"{action}\" method=\"post\"><input name=\"refresh\" type=\"submit\" value=\"Refresh mirror links\"></form>\n"
    );
    if let Some(pos) = html.find("</h1>\n") {
        html.insert_str(pos + "</h1>\n".len(), &form);
    } else if let Some(pos) = html.rfind("</body>") {
        html.insert_str(pos, &form);
    } else {
        html.push_str(&form);
    }
    html
}

fn html_with_mirror_whitelist_notice(mut html: String) -> String {
    let notice = "<p><strong>INFO:</strong> Because this project is not in the <code>mirror_whitelist</code>, upstream mirror links are omitted.</p>\n";
    if let Some(pos) = html.find("</h1>\n") {
        html.insert_str(pos + "</h1>\n".len(), notice);
    } else if let Some(pos) = html.rfind("</body>") {
        html.insert_str(pos, notice);
    } else {
        html.push_str(notice);
    }
    html
}

fn escape_html_attr(value: &str) -> String {
    let mut escaped = String::new();
    for ch in value.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '"' => escaped.push_str("&quot;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

struct TextResponse(StatusCode, String);

impl IntoResponse for TextResponse {
    fn into_response(self) -> Response {
        with_content_type(self.0, "text/plain; charset=utf-8", self.1.into_bytes())
    }
}

struct PackageFileResponse {
    filename: String,
    bytes: Vec<u8>,
    modified: Option<SystemTime>,
    conditional: ConditionalRequest,
}

impl PackageFileResponse {
    fn new(filename: String, file: LocalReadFile) -> Self {
        Self::from_parts(filename, file.bytes, file.modified)
    }

    fn from_parts(filename: String, bytes: Vec<u8>, modified: Option<SystemTime>) -> Self {
        Self {
            filename,
            bytes,
            modified,
            conditional: ConditionalRequest::default(),
        }
    }

    fn with_conditional(mut self, conditional: ConditionalRequest) -> Self {
        self.conditional = conditional;
        self
    }
}

impl IntoResponse for PackageFileResponse {
    fn into_response(self) -> Response {
        let content_length = self.bytes.len().to_string();
        let content_type = package_content_type(&self.filename);
        let etag = format!("\"{}\"", sha256_hex(&self.bytes));
        let not_modified = if self.conditional.if_none_match.is_some() {
            conditional_etag_matches(&self.conditional, &etag)
        } else {
            self.modified.is_some_and(|modified| {
                conditional_modified_since_matches(&self.conditional, modified)
            })
        };
        let mut response = if not_modified {
            with_content_type(StatusCode::NOT_MODIFIED, content_type, Vec::new())
        } else {
            let mut response = with_content_type(StatusCode::OK, content_type, self.bytes);
            response.headers_mut().insert(
                header::CONTENT_LENGTH,
                HeaderValue::from_str(&content_length)
                    .unwrap_or_else(|_| HeaderValue::from_static("0")),
            );
            response
        };
        if let Ok(value) = HeaderValue::from_str(&etag) {
            response.headers_mut().insert(header::ETAG, value);
        }
        if let Some(modified) = self.modified
            && let Ok(value) = HeaderValue::from_str(&fmt_http_date(modified))
        {
            response.headers_mut().insert(header::LAST_MODIFIED, value);
        }
        response.headers_mut().insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static("max-age=365000000, immutable, public"),
        );
        if let Ok(value) = HeaderValue::from_str(&format!(
            "attachment; filename=\"{}\"",
            header_filename(&self.filename)
        )) {
            response
                .headers_mut()
                .insert(header::CONTENT_DISPOSITION, value);
        }
        response
    }
}

fn header_filename(filename: &str) -> String {
    filename.replace(['\\', '"'], "_")
}

fn package_content_type(filename: &str) -> &'static str {
    let lower = filename.to_ascii_lowercase();
    if lower.ends_with(".zip") {
        "application/zip"
    } else if lower.ends_with(".tar")
        || lower.ends_with(".tar.gz")
        || lower.ends_with(".tgz")
        || lower.ends_with(".tar.bz2")
        || lower.ends_with(".tbz2")
        || lower.ends_with(".tar.xz")
        || lower.ends_with(".txz")
    {
        "application/x-tar"
    } else if lower.ends_with(".rpm") {
        "application/x-rpm"
    } else {
        "application/octet-stream"
    }
}

fn with_content_type(status: StatusCode, content_type: &'static str, body: Vec<u8>) -> Response {
    let mut response = (status, body).into_response();
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    response.headers_mut().insert(
        "X-DEVPI-API-VERSION",
        HeaderValue::from_static(DEVPI_API_VERSION),
    );
    response.headers_mut().insert(
        "X-DEVPI-SERVER-VERSION",
        HeaderValue::from_static(DEVPI_SERVER_VERSION),
    );
    response
        .headers_mut()
        .insert("X-DEVPI-SERIAL", HeaderValue::from_static("0"));
    response
}

fn with_current_serial(state: &AppState, mut response: Response) -> Response {
    insert_serial_header(&mut response, state.serial.current());
    response
}

fn bump_serial_response_with_event(state: &AppState, response: Response, event: Value) -> Response {
    let status = response.status();
    if status.is_success() && status != StatusCode::NOT_MODIFIED {
        match state.serial.bump_with_event(event) {
            Ok(serial) => return with_serial_header(response, serial),
            Err(err) => {
                return TextResponse(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("could not update serial: {err}\n"),
                )
                .into_response();
            }
        }
    }
    with_current_serial(state, response)
}

fn with_serial_header(mut response: Response, serial: u64) -> Response {
    insert_serial_header(&mut response, serial);
    response
}

fn insert_serial_header(response: &mut Response, serial: u64) {
    insert_header_u64(response, "X-DEVPI-SERIAL", serial);
}

fn insert_header_u64(response: &mut Response, name: &'static str, value: u64) {
    let value =
        HeaderValue::from_str(&value.to_string()).unwrap_or_else(|_| HeaderValue::from_static("0"));
    response.headers_mut().insert(name, value);
}

enum FilesRelpath {
    Project {
        user: String,
        index: String,
        project: String,
        filename: String,
    },
    Hash {
        user: String,
        index: String,
        hash_prefix: String,
        hash_rest: String,
        filename: String,
    },
}

fn parse_files_relpath(relpath: &str) -> Option<FilesRelpath> {
    let parts = relpath
        .split('/')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    match parts.as_slice() {
        [user, index, "+f", project, filename] => Some(FilesRelpath::Project {
            user: (*user).to_string(),
            index: (*index).to_string(),
            project: (*project).to_string(),
            filename: (*filename).to_string(),
        }),
        [user, index, "+f", hash_prefix, hash_rest, filename] => Some(FilesRelpath::Hash {
            user: (*user).to_string(),
            index: (*index).to_string(),
            hash_prefix: (*hash_prefix).to_string(),
            hash_rest: (*hash_rest).to_string(),
            filename: (*filename).to_string(),
        }),
        _ => None,
    }
}

fn valid_stage_segment(value: &str) -> bool {
    !value.is_empty()
        && !value.contains('+')
        && !value.contains('/')
        && !value.contains('\\')
        && value != "."
        && value != ".."
}

fn parse_json_or_default<T>(body: &[u8]) -> io::Result<T>
where
    T: DeserializeOwned + Default,
{
    if body.trim_ascii().is_empty() {
        return Ok(T::default());
    }
    serde_json::from_slice(body).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid json body: {err}"),
        )
    })
}

fn parse_json_value_or_default(body: &[u8]) -> io::Result<Value> {
    if body.trim_ascii().is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(body).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid json body: {err}"),
        )
    })
}

fn index_input_from_value(value: Value, existing: Option<&IndexConfig>) -> io::Result<IndexInput> {
    match value {
        Value::Object(_) => serde_json::from_value(value).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid index config body: {err}"),
            )
        }),
        Value::Array(_) => {
            let Some(existing) = existing else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "list patch requires an existing index",
                ));
            };
            let commands = serde_json::from_value(value).map_err(|err| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("invalid index patch body: {err}"),
                )
            })?;
            index_input_from_list_patch(commands, existing)
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "index config body must be an object or list patch",
        )),
    }
}

fn index_input_from_list_patch(
    commands: Vec<String>,
    existing: &IndexConfig,
) -> io::Result<IndexInput> {
    let mut input = IndexInput::default();
    let mut bases = existing.bases.clone();
    let mut mirror_whitelist = existing.mirror_whitelist.clone();
    let mut acl_upload = existing.acl_upload.clone();
    let mut acl_pkg_read = existing.acl_pkg_read.clone();
    let mut acl_toxresult_upload = existing.acl_toxresult_upload.clone();
    let mut touched_bases = false;
    let mut touched_mirror_whitelist = false;
    let mut touched_acl_upload = false;
    let mut touched_acl_pkg_read = false;
    let mut touched_acl_toxresult_upload = false;

    for command in commands {
        let (key, op, value) = parse_list_patch_command(&command)?;
        if op == "=" {
            match key {
                "bases" => {
                    bases = list_patch_assignment_values(key, value);
                    touched_bases = true;
                }
                "mirror_whitelist" => {
                    mirror_whitelist = list_patch_assignment_values(key, value);
                    touched_mirror_whitelist = true;
                }
                "acl_upload" => {
                    acl_upload = list_patch_assignment_values(key, value);
                    touched_acl_upload = true;
                }
                "acl_pkg_read" => {
                    acl_pkg_read = list_patch_assignment_values(key, value);
                    touched_acl_pkg_read = true;
                }
                "acl_toxresult_upload" => {
                    acl_toxresult_upload = list_patch_assignment_values(key, value);
                    touched_acl_toxresult_upload = true;
                }
                _ => apply_list_patch_assignment(&mut input, key, value)?,
            }
            continue;
        }
        if op == "-=" && value.is_empty() {
            match key {
                "bases" => {
                    bases.clear();
                    touched_bases = true;
                }
                "mirror_whitelist" => {
                    mirror_whitelist.clear();
                    touched_mirror_whitelist = true;
                }
                "acl_upload" => {
                    acl_upload.clear();
                    touched_acl_upload = true;
                }
                "acl_pkg_read" => {
                    acl_pkg_read.clear();
                    touched_acl_pkg_read = true;
                }
                "acl_toxresult_upload" => {
                    acl_toxresult_upload.clear();
                    touched_acl_toxresult_upload = true;
                }
                _ => {
                    clear_pending_index_input_field(&mut input, key);
                    if !input.clear_fields.iter().any(|field| field == key) {
                        input.clear_fields.push(key.to_string());
                    }
                }
            }
            continue;
        }
        let value = list_patch_value(key, value);
        let (list, touched) = match key {
            "bases" => (&mut bases, &mut touched_bases),
            "mirror_whitelist" => (&mut mirror_whitelist, &mut touched_mirror_whitelist),
            "acl_upload" => (&mut acl_upload, &mut touched_acl_upload),
            "acl_pkg_read" => (&mut acl_pkg_read, &mut touched_acl_pkg_read),
            "acl_toxresult_upload" => {
                (&mut acl_toxresult_upload, &mut touched_acl_toxresult_upload)
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unsupported list patch setting {key:?}"),
                ));
            }
        };
        *touched = true;
        if op == "+=" {
            if !list.iter().any(|entry| entry == &value) {
                list.push(value);
            }
        } else if let Some(pos) = list.iter().position(|entry| entry == &value) {
            list.remove(pos);
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("The {key:?} setting doesn't have value {value:?}"),
            ));
        }
    }

    if touched_bases {
        input.bases = Some(bases);
    }
    if touched_mirror_whitelist {
        input.mirror_whitelist = Some(mirror_whitelist);
    }
    if touched_acl_upload {
        input.acl_upload = Some(acl_upload);
    }
    if touched_acl_pkg_read {
        input.acl_pkg_read = Some(acl_pkg_read);
    }
    if touched_acl_toxresult_upload {
        input.acl_toxresult_upload = Some(acl_toxresult_upload);
    }
    Ok(input)
}

fn parse_list_patch_command(command: &str) -> io::Result<(&str, &str, &str)> {
    if let Some((key, value)) = command.split_once("+=") {
        return Ok((key, "+=", value));
    }
    if let Some((key, value)) = command.split_once("-=") {
        return Ok((key, "-=", value));
    }
    if let Some((key, value)) = command.split_once('=') {
        return Ok((key, "=", value));
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("unsupported index patch command {command:?}"),
    ))
}

fn list_patch_value(key: &str, value: &str) -> String {
    match key {
        "bases" => value.trim_matches('/').to_string(),
        "mirror_whitelist" if value == "*" => value.to_string(),
        "mirror_whitelist" => normalize_project_name(value),
        "acl_upload" | "acl_pkg_read" | "acl_toxresult_upload" => {
            let upper = value.to_ascii_uppercase();
            if upper == ":ANONYMOUS:" || upper == ":AUTHENTICATED:" {
                upper
            } else {
                value.to_string()
            }
        }
        _ => value.to_string(),
    }
}

fn list_patch_assignment_values(key: &str, value: &str) -> Vec<String> {
    if value.is_empty() {
        Vec::new()
    } else {
        vec![list_patch_value(key, value)]
    }
}

fn apply_list_patch_assignment(input: &mut IndexInput, key: &str, value: &str) -> io::Result<()> {
    input.clear_fields.retain(|field| field != key);
    match key {
        "type" => input.index_type = Some(value.to_string()),
        "volatile" => input.volatile = Some(parse_patch_bool(key, value)?),
        "mirror_whitelist_inheritance" => {
            input.mirror_whitelist_inheritance = Some(value.to_string())
        }
        "title" => input.title = Some(value.to_string()),
        "description" => input.description = Some(value.to_string()),
        "mirror_url" => input.mirror_url = Some(value.to_string()),
        "mirror_web_url_fmt" => input.mirror_web_url_fmt = Some(value.to_string()),
        "mirror_cache_expiry" => input.mirror_cache_expiry = Some(parse_patch_i64(key, value)?),
        "mirror_ignore_serial_header" => {
            input.mirror_ignore_serial_header = Some(parse_patch_bool(key, value)?)
        }
        "mirror_no_project_list" => {
            input.mirror_no_project_list = Some(parse_patch_bool(key, value)?)
        }
        "mirror_provides_core_metadata" => {
            input.mirror_provides_core_metadata = Some(parse_patch_bool(key, value)?)
        }
        "mirror_use_external_urls" => {
            input.mirror_use_external_urls = Some(parse_patch_bool(key, value)?)
        }
        _ => {
            input
                .extra
                .insert(key.to_string(), parse_extra_patch_value(value));
        }
    }
    Ok(())
}

fn parse_extra_patch_value(value: &str) -> Value {
    match value.to_ascii_lowercase().as_str() {
        "true" => Value::Bool(true),
        "false" => Value::Bool(false),
        _ => value
            .parse::<i64>()
            .map(|number| json!(number))
            .unwrap_or_else(|_| Value::String(value.to_string())),
    }
}

fn clear_pending_index_input_field(input: &mut IndexInput, key: &str) {
    match key {
        "type" => input.index_type = None,
        "volatile" => input.volatile = None,
        "mirror_whitelist_inheritance" => input.mirror_whitelist_inheritance = None,
        "title" => input.title = None,
        "description" => input.description = None,
        "custom_data" => input.custom_data = None,
        "mirror_url" => input.mirror_url = None,
        "mirror_web_url_fmt" => input.mirror_web_url_fmt = None,
        "mirror_cache_expiry" => input.mirror_cache_expiry = None,
        "mirror_ignore_serial_header" => input.mirror_ignore_serial_header = None,
        "mirror_no_project_list" => input.mirror_no_project_list = None,
        "mirror_provides_core_metadata" => input.mirror_provides_core_metadata = None,
        "mirror_use_external_urls" => input.mirror_use_external_urls = None,
        _ => {
            input.extra.remove(key);
        }
    }
}

fn parse_patch_bool(key: &str, value: &str) -> io::Result<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{key} must be true or false"),
        )),
    }
}

fn parse_patch_i64(key: &str, value: &str) -> io::Result<i64> {
    value.parse::<i64>().map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{key} must be an integer: {err}"),
        )
    })
}

fn stage_index_json(
    user: &str,
    index: &str,
    projects: Option<&[String]>,
    ixconfig: &IndexConfig,
    source_names: &[String],
) -> String {
    let name = format!("{user}/{index}");
    let index_url = format!("/{user}/{index}/");
    let simple_url = format!("/{user}/{index}/+simple/");
    let mut ixconfig = ixconfig.clone();
    if ixconfig.acl_upload.is_empty() && ixconfig.index_type == "stage" {
        ixconfig.acl_upload.push(user.to_string());
    }
    let resolved_sources = if ixconfig.sources.is_empty() {
        source_names.to_vec()
    } else {
        ixconfig.sources.clone()
    };
    let mut value = json!({
        "status": 200,
        "type": "stage",
        "result": {
            "user": user,
            "index": index,
            "name": name,
            "simpleindex": simple_url,
            "ixconfig": {
                "type": ixconfig.index_type,
                "bases": ixconfig.bases,
                "volatile": ixconfig.volatile,
                "acl_upload": ixconfig.acl_upload,
                "acl_pkg_read": ixconfig.acl_pkg_read,
                "acl_toxresult_upload": ixconfig.acl_toxresult_upload,
                "mirror_whitelist": ixconfig.mirror_whitelist,
                "mirror_whitelist_inheritance": ixconfig.mirror_whitelist_inheritance,
                "mirror_use_external_urls": ixconfig.mirror_use_external_urls,
                "sources": ixconfig.sources,
                "resolved_sources": resolved_sources,
                "multi_source": true
            }
        }
    });
    if let Some(projects) = projects {
        value["result"]["projects"] = json!(projects);
    }
    if let Some(title) = &ixconfig.title {
        value["result"]["ixconfig"]["title"] = json!(title);
    }
    if let Some(description) = &ixconfig.description {
        value["result"]["ixconfig"]["description"] = json!(description);
    }
    if let Some(custom_data) = &ixconfig.custom_data {
        value["result"]["ixconfig"]["custom_data"] = custom_data.clone();
    }
    if let Some(mirror_url) = &ixconfig.mirror_url {
        value["result"]["ixconfig"]["mirror_url"] = json!(mirror_url);
    }
    if let Some(mirror_web_url_fmt) = &ixconfig.mirror_web_url_fmt {
        value["result"]["ixconfig"]["mirror_web_url_fmt"] = json!(mirror_web_url_fmt);
    }
    if let Some(mirror_cache_expiry) = ixconfig.mirror_cache_expiry {
        value["result"]["ixconfig"]["mirror_cache_expiry"] = json!(mirror_cache_expiry);
    }
    if ixconfig.mirror_ignore_serial_header {
        value["result"]["ixconfig"]["mirror_ignore_serial_header"] = json!(true);
    }
    if ixconfig.mirror_no_project_list {
        value["result"]["ixconfig"]["mirror_no_project_list"] = json!(true);
    }
    if ixconfig.mirror_provides_core_metadata {
        value["result"]["ixconfig"]["mirror_provides_core_metadata"] = json!(true);
    }
    if let Some(ixconfig_value) = value["result"]["ixconfig"].as_object_mut() {
        for (key, extra_value) in &ixconfig.extra {
            ixconfig_value
                .entry(key.clone())
                .or_insert_with(|| extra_value.clone());
        }
    }
    if ixconfig.index_type != "mirror" {
        value["result"]["pypisubmit"] = json!(index_url);
    }
    format!("{value}\n")
}

fn stage_project_json(
    user: &str,
    index: &str,
    project: &str,
    files: &[LocalFile],
    metadata: &BTreeMap<String, FileMetadata>,
    tox_results: &BTreeMap<String, Vec<Value>>,
    versions: &BTreeMap<String, FileMetadata>,
) -> String {
    let files_json = files
        .iter()
        .map(|file| {
            let url = format!("/{user}/{index}/+f/{project}/{}", file.filename);
            let version = infer_version(project, &file.filename);
            let metadata_json = metadata
                .get(&file.filename)
                .map(|metadata| {
                    format!(
                        ",\"metadata\":{}",
                        metadata_fields_json_string(&metadata.fields)
                    )
                })
                .unwrap_or_default();
            let tox_results_json = tox_results
                .get(&file.filename)
                .map(|results| {
                    format!(
                        ",\"toxresults\":{}",
                        serde_json::to_string(results).unwrap_or_else(|_| "[]".to_string())
                    )
                })
                .unwrap_or_default();
            format!(
                "{{\"filename\":\"{}\",\"url\":\"{}\",\"bytes\":{},\"version\":\"{}\"{}{}}}",
                json_escape(&file.filename),
                json_escape(&url),
                file.bytes,
                json_escape(&version),
                metadata_json,
                tox_results_json
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let versions_json = versions
        .iter()
        .map(|(version, metadata)| {
            format!(
                "\"{}\":{}",
                json_escape(version),
                metadata_fields_json_string(&metadata.fields)
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{{\"status\":200,\"type\":\"projectconfig\",\"result\":{{\"user\":\"{}\",\"index\":\"{}\",\"project\":\"{}\",\"files\":[{}],\"versions\":{{{}}}}}}}\n",
        json_escape(user),
        json_escape(index),
        json_escape(project),
        files_json,
        versions_json
    )
}

const MULTI_VALUE_METADATA_FIELDS: &[&str] = &[
    "classifiers",
    "platform",
    "requires",
    "provides",
    "obsoletes",
    "requires_dist",
    "provides_dist",
    "obsoletes_dist",
    "project_urls",
];

fn is_multi_value_metadata_field(key: &str) -> bool {
    MULTI_VALUE_METADATA_FIELDS.contains(&key)
}

fn metadata_fields_json(fields: &BTreeMap<String, String>) -> Value {
    let mut object = serde_json::Map::new();
    for (key, value) in fields {
        let value = if is_multi_value_metadata_field(key) {
            json!(metadata_list_values(value))
        } else {
            json!(value)
        };
        object.insert(key.clone(), value);
    }
    Value::Object(object)
}

fn metadata_fields_json_string(fields: &BTreeMap<String, String>) -> String {
    serde_json::to_string(&metadata_fields_json(fields)).unwrap_or_else(|_| "{}".to_string())
}

fn metadata_list_values(value: &str) -> Vec<String> {
    value
        .split('\n')
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn upload_log_entries(
    auth_user: Option<&str>,
    user: &str,
    index: &str,
    previous_metadata: Option<&FileMetadata>,
    overwrote_existing: bool,
) -> Vec<FileLogEntry> {
    let when = current_gmtime_tuple();
    let mut log = Vec::new();
    if overwrote_existing {
        let overwrite_count = previous_metadata
            .and_then(|metadata| {
                metadata
                    .log
                    .iter()
                    .filter(|entry| entry.what == "overwrite")
                    .filter_map(|entry| entry.count)
                    .max()
            })
            .unwrap_or(0)
            + 1;
        log.push(FileLogEntry {
            what: "overwrite".to_string(),
            who: None,
            when,
            dst: None,
            src: None,
            count: Some(overwrite_count),
        });
    }
    log.push(FileLogEntry {
        what: "upload".to_string(),
        who: auth_user.map(ToString::to_string),
        when,
        dst: Some(format!("{user}/{index}")),
        src: None,
        count: None,
    });
    log
}

fn pushed_file_log(
    source_log: &[FileLogEntry],
    auth_user: Option<&str>,
    source_user: &str,
    source_index: &str,
    target_user: &str,
    target_index: &str,
) -> Vec<FileLogEntry> {
    let mut log = source_log
        .iter()
        .filter(|entry| entry.what != "overwrite")
        .cloned()
        .collect::<Vec<_>>();
    if log.is_empty() {
        log.push(FileLogEntry {
            what: "upload".to_string(),
            who: None,
            when: current_gmtime_tuple(),
            dst: Some(format!("{source_user}/{source_index}")),
            src: None,
            count: None,
        });
    }
    log.push(FileLogEntry {
        what: "push".to_string(),
        who: auth_user.map(ToString::to_string),
        when: current_gmtime_tuple(),
        dst: Some(format!("{target_user}/{target_index}")),
        src: Some(format!("{source_user}/{source_index}")),
        count: None,
    });
    log
}

fn current_gmtime_tuple() -> [u64; 6] {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let seconds = duration.as_secs() as i64;
    let days = seconds.div_euclid(86_400);
    let seconds_of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = (seconds_of_day / 3_600) as u64;
    let minute = ((seconds_of_day % 3_600) / 60) as u64;
    let second = (seconds_of_day % 60) as u64;
    [year as u64, month as u64, day as u64, hour, minute, second]
}

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let days = days + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let doe = days - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    (y + (m <= 2) as i64, m, d)
}

fn version_file_links_json(
    user: &str,
    index: &str,
    project: &str,
    version: &str,
    files: &[LocalFile],
    file_metadata: &BTreeMap<String, FileMetadata>,
    tox_results: &BTreeMap<String, Vec<Value>>,
) -> Vec<Value> {
    let mut links = Vec::new();
    for file in files
        .iter()
        .filter(|file| infer_version(project, &file.filename) == version)
    {
        let href = format!("/{user}/{index}/+f/{project}/{}", file.filename);
        let rel = if is_doczip_filename(&file.filename) {
            "doczip"
        } else {
            "releasefile"
        };
        let log = file_metadata
            .get(&file.filename)
            .and_then(|metadata| (!metadata.log.is_empty()).then_some(json!(metadata.log)))
            .unwrap_or_else(|| {
                json!([{
                    "what": "upload",
                    "dst": format!("{user}/{index}"),
                    "who": null,
                    "when": null,
                }])
            });
        links.push(json!({
                "rel": rel,
                "href": href,
                "basename": file.filename,
                "log": log,
        }));
        if let Some(results) = tox_results.get(&file.filename) {
            for result_index in 0..results.len() {
                links.push(json!({
                    "rel": "toxresult",
                    "href": format!("/{user}/{index}/+f/{project}/{}.toxresult-{result_index}", file.filename),
                    "for_href": href,
                    "log": log.clone(),
                }));
            }
        }
    }
    links
}

const RELEASE_FILE_SUFFIXES: &[&str] = &[
    ".tar.gz", ".tar.bz2", ".tar.xz", ".tgz", ".zip", ".whl", ".egg", ".exe", ".msi", ".rpm",
    ".deb",
];

fn is_valid_release_filename(filename: &str) -> bool {
    release_file_stem(filename).is_some()
}

fn release_file_stem(filename: &str) -> Option<&str> {
    RELEASE_FILE_SUFFIXES
        .iter()
        .find_map(|suffix| filename.strip_suffix(suffix))
}

fn filename_version_matches(project: &str, filename: &str, version: &str) -> bool {
    let inferred = infer_version(project, filename);
    inferred == version || (filename.ends_with(".whl") && inferred == version.replace('+', "_"))
}

fn infer_version(project: &str, filename: &str) -> String {
    let stem = filename
        .strip_suffix(".doc.zip")
        .or_else(|| filename.strip_suffix(".doc.tgz"))
        .or_else(|| release_file_stem(filename))
        .unwrap_or(filename);
    let normalized_project = normalize_project_name(project);
    stem.match_indices('-')
        .filter_map(|(index, _)| {
            let prefix = &stem[..index];
            (normalize_project_name(prefix) == normalized_project).then_some(&stem[index + 1..])
        })
        .next_back()
        .and_then(|rest| rest.split('-').next())
        .unwrap_or_default()
        .to_string()
}

fn tox_result_path(filename: &str) -> Option<(&str, usize)> {
    let (release_filename, result_index) = filename.rsplit_once(".toxresult-")?;
    let result_index = result_index.parse::<usize>().ok()?;
    Some((release_filename, result_index))
}

fn metadata_path(filename: &str) -> Option<&str> {
    filename.strip_suffix(".metadata")
}

fn is_doczip_file(filename: &str, metadata: Option<&FileMetadata>) -> bool {
    metadata
        .and_then(|metadata| metadata.fields.get("filetype"))
        .is_some_and(|filetype| filetype == "doczip")
        || is_doczip_filename(filename)
}

fn is_doczip_metadata(filename: &str, metadata: &BTreeMap<String, String>) -> bool {
    metadata
        .get("filetype")
        .is_some_and(|filetype| filetype == "doczip")
        || is_doczip_filename(filename)
}

fn is_doczip_filename(filename: &str) -> bool {
    filename.ends_with(".doc.zip") || filename.ends_with(".doc.tgz")
}

fn core_metadata_text(project: &str, filename: &str, fields: &BTreeMap<String, String>) -> String {
    let mut out = String::new();
    let name = fields
        .get("name")
        .cloned()
        .unwrap_or_else(|| project.to_string());
    let version = fields
        .get("version")
        .cloned()
        .unwrap_or_else(|| infer_version(project, filename));
    out.push_str("Metadata-Version: 2.1\n");
    out.push_str("Name: ");
    out.push_str(&metadata_header_value(&name));
    out.push('\n');
    if !version.is_empty() {
        out.push_str("Version: ");
        out.push_str(&metadata_header_value(&version));
        out.push('\n');
    }
    for (key, value) in fields {
        if key == "name" || key == "version" {
            continue;
        }
        if is_simple_link_metadata_key(key) {
            continue;
        }
        out.push_str(&metadata_header_name(key));
        out.push_str(": ");
        out.push_str(&metadata_header_value(value));
        out.push('\n');
    }
    out
}

fn extracted_file_metadata(filename: &str, bytes: &[u8]) -> Option<BTreeMap<String, String>> {
    let (text, wheel_metadata) = if filename.ends_with(".whl") {
        (extract_wheel_metadata(bytes)?, true)
    } else if filename.ends_with(".tar.gz") || filename.ends_with(".tgz") {
        (extract_sdist_metadata(bytes)?, false)
    } else if filename.ends_with(".zip") && !filename.ends_with(".doc.zip") {
        (extract_zip_sdist_metadata(bytes)?, false)
    } else {
        return None;
    };
    let mut fields = parse_core_metadata_fields(&text);
    if fields.is_empty() {
        return None;
    }
    fields.insert("core_metadata".to_string(), "true".to_string());
    if wheel_metadata {
        fields.insert("dist_info_metadata".to_string(), "true".to_string());
    }
    Some(fields)
}

fn extract_sdist_metadata(bytes: &[u8]) -> Option<String> {
    let decoder = GzDecoder::new(bytes);
    let mut archive = tar::Archive::new(decoder);
    for entry in archive.entries().ok()? {
        let mut entry = entry.ok()?;
        let path = entry.path().ok()?;
        if path.file_name().is_some_and(|name| name == "PKG-INFO") {
            let mut text = String::new();
            entry.read_to_string(&mut text).ok()?;
            return Some(text);
        }
    }
    None
}

fn extract_wheel_metadata(bytes: &[u8]) -> Option<String> {
    extract_zip_metadata(bytes, |name| name.ends_with(".dist-info/METADATA"))
}

fn extract_zip_sdist_metadata(bytes: &[u8]) -> Option<String> {
    extract_zip_metadata(bytes, |name| {
        name.rsplit('/')
            .next()
            .is_some_and(|name| name == "PKG-INFO")
    })
}

fn extract_zip_metadata(bytes: &[u8], matches: impl Fn(&str) -> bool) -> Option<String> {
    let mut offset = 0;
    while offset + 30 <= bytes.len() {
        if read_le_u32(bytes, offset)? != 0x0403_4b50 {
            return None;
        }
        let flags = read_le_u16(bytes, offset + 6)?;
        let method = read_le_u16(bytes, offset + 8)?;
        let compressed_size = read_le_u32(bytes, offset + 18)? as usize;
        let uncompressed_size = read_le_u32(bytes, offset + 22)? as usize;
        let name_len = read_le_u16(bytes, offset + 26)? as usize;
        let extra_len = read_le_u16(bytes, offset + 28)? as usize;
        let name_start = offset + 30;
        let name_end = name_start.checked_add(name_len)?;
        let data_start = name_end.checked_add(extra_len)?;
        let data_end = data_start.checked_add(compressed_size)?;
        if data_end > bytes.len() {
            return None;
        }
        let name = std::str::from_utf8(bytes.get(name_start..name_end)?).ok()?;
        if matches(name) {
            if flags & 0x0008 != 0 {
                return None;
            }
            let data = decode_zip_entry(method, &bytes[data_start..data_end], uncompressed_size)?;
            return String::from_utf8(data).ok();
        }
        if flags & 0x0008 != 0 {
            return None;
        }
        offset = data_end;
    }
    None
}

fn decode_zip_entry(method: u16, data: &[u8], uncompressed_size: usize) -> Option<Vec<u8>> {
    match method {
        0 => Some(data.to_vec()),
        8 => {
            let mut decoder = DeflateDecoder::new(data);
            let mut decoded = Vec::with_capacity(uncompressed_size);
            decoder.read_to_end(&mut decoded).ok()?;
            if uncompressed_size != 0 && decoded.len() != uncompressed_size {
                return None;
            }
            Some(decoded)
        }
        _ => None,
    }
}

fn parse_core_metadata_fields(text: &str) -> BTreeMap<String, String> {
    let mut fields: BTreeMap<String, String> = BTreeMap::new();
    let mut current_key: Option<String> = None;
    for line in text.lines() {
        if line.is_empty() {
            break;
        }
        if line.starts_with([' ', '\t']) {
            if let Some(key) = current_key.as_ref()
                && let Some(value) = fields.get_mut(key)
            {
                value.push(' ');
                value.push_str(line.trim());
            }
            continue;
        }
        let Some((name, value)) = line.split_once(':') else {
            current_key = None;
            continue;
        };
        let key = metadata_field_key(name);
        if key == "metadata_version" || key.is_empty() {
            current_key = None;
            continue;
        }
        fields.insert(key.clone(), value.trim_start().to_string());
        current_key = Some(key);
    }
    fields
}

fn metadata_field_key(name: &str) -> String {
    name.trim()
        .chars()
        .map(|ch| {
            if ch == '-' {
                '_'
            } else {
                ch.to_ascii_lowercase()
            }
        })
        .collect()
}

fn is_simple_link_metadata_key(key: &str) -> bool {
    matches!(
        key,
        "dist_info_metadata"
            | "dist-info-metadata"
            | "data-dist-info-metadata"
            | "core_metadata"
            | "core-metadata"
            | "data-core-metadata"
            | "gpg_sig"
            | "gpg-sig"
            | "data-gpg-sig"
    )
}

fn read_le_u16(bytes: &[u8], offset: usize) -> Option<u16> {
    Some(u16::from_le_bytes(
        bytes.get(offset..offset + 2)?.try_into().ok()?,
    ))
}

fn read_le_u32(bytes: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_le_bytes(
        bytes.get(offset..offset + 4)?.try_into().ok()?,
    ))
}

fn metadata_header_name(key: &str) -> String {
    key.split(['_', '-'])
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => format!("{}{}", first.to_ascii_uppercase(), chars.as_str()),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join("-")
}

fn metadata_header_value(value: &str) -> String {
    value.replace(['\r', '\n'], " ")
}

fn json_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AppConfig, SourceConfig};
    use axum::body::to_bytes;
    use axum::http::{Method, Request};
    use std::collections::{HashMap, VecDeque};
    use std::io::Write;
    use std::sync::Mutex;
    use tower::ServiceExt;

    struct MockFetcher {
        responses: Mutex<HashMap<String, io::Result<String>>>,
    }

    impl Fetcher for MockFetcher {
        fn fetch(&self, url: &str) -> io::Result<String> {
            self.responses
                .lock()
                .unwrap()
                .remove(url)
                .unwrap_or_else(|| Err(io::Error::new(io::ErrorKind::NotFound, url.to_string())))
        }
    }

    struct SerialMockFetcher {
        responses: Mutex<HashMap<String, io::Result<crate::upstream::FetchedPage>>>,
    }

    impl Fetcher for SerialMockFetcher {
        fn fetch(&self, url: &str) -> io::Result<String> {
            self.fetch_page(url).map(|page| page.body)
        }

        fn fetch_page(&self, url: &str) -> io::Result<crate::upstream::FetchedPage> {
            self.responses
                .lock()
                .unwrap()
                .remove(url)
                .unwrap_or_else(|| Err(io::Error::new(io::ErrorKind::NotFound, url.to_string())))
        }
    }

    #[derive(Default)]
    struct MockExternalPoster {
        requests: Mutex<Vec<RecordedExternalRequest>>,
        responses: Mutex<VecDeque<io::Result<ExternalPostResponse>>>,
    }

    impl MockExternalPoster {
        fn with_responses(responses: Vec<ExternalPostResponse>) -> Self {
            Self::with_outcomes(responses.into_iter().map(Ok).collect())
        }

        fn with_outcomes(responses: Vec<io::Result<ExternalPostResponse>>) -> Self {
            Self {
                requests: Mutex::new(Vec::new()),
                responses: Mutex::new(VecDeque::from(responses)),
            }
        }
    }

    struct RecordedExternalRequest {
        url: String,
        auth: Option<(String, String)>,
        fields: Vec<(String, String)>,
        file: Option<(String, Vec<u8>)>,
    }

    impl ExternalPoster for MockExternalPoster {
        fn post_multipart<'a>(
            &self,
            url: &str,
            auth: Option<(&str, &str)>,
            fields: &[(String, String)],
            file: Option<ExternalMultipartFile<'a>>,
        ) -> io::Result<ExternalPostResponse> {
            self.requests.lock().unwrap().push(RecordedExternalRequest {
                url: url.to_string(),
                auth: auth.map(|(username, password)| (username.to_string(), password.to_string())),
                fields: fields.to_vec(),
                file: file.map(|file| (file.filename.to_string(), file.bytes.to_vec())),
            });
            if let Some(response) = self.responses.lock().unwrap().pop_front() {
                return response;
            }
            Ok(ExternalPostResponse {
                status: 200,
                body: "ok".to_string(),
            })
        }
    }

    fn test_state(name: &str) -> AppState {
        let dir = std::env::temp_dir().join(format!("devpi-rs-axum-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        AppState {
            index: Arc::new(MultiSourceIndex::new(
                Vec::<SourceConfig>::new(),
                std::env::temp_dir(),
            )),
            local: Arc::new(LocalStore::new(dir.clone())),
            registry: Arc::new(Registry::new(dir.clone())),
            serial: Arc::new(SerialStore::new(dir.join(".devpi-rs-serial"))),
            fetcher: Arc::new(CurlFetcher::default()),
            external_poster: Arc::new(TcpExternalPoster),
            upstream_reports: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn test_router_with_index(name: &str, user: &str, index: &str) -> (Router, PathBuf) {
        let dir = std::env::temp_dir().join(format!("devpi-rs-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let state = AppState {
            index: Arc::new(MultiSourceIndex::new(
                Vec::<SourceConfig>::new(),
                std::env::temp_dir(),
            )),
            local: Arc::new(LocalStore::new(dir.clone())),
            registry: Arc::new(Registry::new(dir.clone())),
            serial: Arc::new(SerialStore::new(dir.join(".devpi-rs-serial"))),
            fetcher: Arc::new(CurlFetcher::default()),
            external_poster: Arc::new(TcpExternalPoster),
            upstream_reports: Arc::new(Mutex::new(Vec::new())),
        };
        state
            .registry
            .put_index(
                user,
                index,
                IndexInput {
                    acl_upload: Some(vec![":ANONYMOUS:".to_string()]),
                    acl_toxresult_upload: Some(vec![":ANONYMOUS:".to_string()]),
                    ..IndexInput::default()
                },
            )
            .unwrap();
        (router_from_state(state), dir)
    }

    fn put_anonymous_index(state: &AppState, user: &str, index: &str) {
        state
            .registry
            .put_index(
                user,
                index,
                IndexInput {
                    acl_upload: Some(vec![":ANONYMOUS:".to_string()]),
                    acl_toxresult_upload: Some(vec![":ANONYMOUS:".to_string()]),
                    ..IndexInput::default()
                },
            )
            .unwrap();
    }

    async fn body_text(response: Response) -> String {
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    fn basic_auth(username: &str, password: &str) -> String {
        format!(
            "Basic {}",
            encode_base64(format!("{username}:{password}").as_bytes())
        )
    }

    fn assert_upload_log(log: &Value, who: Option<&str>, dst: &str) {
        let entries = log.as_array().unwrap();
        assert_eq!(entries.len(), 1);
        let entry = &entries[0];
        assert_eq!(entry["what"], "upload");
        assert_eq!(entry["dst"], dst);
        match who {
            Some(who) => assert_eq!(entry["who"], who),
            None => assert!(entry["who"].is_null()),
        }
        let when = entry["when"].as_array().unwrap();
        assert_eq!(when.len(), 6);
        assert!(when[0].as_u64().unwrap() >= 2024);
    }

    fn deflated_zip_entry(name: &str, content: &[u8]) -> Vec<u8> {
        let mut encoder =
            flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(content).unwrap();
        let compressed = encoder.finish().unwrap();
        zip_entry(name, content, 8, &compressed)
    }

    fn zip_entry(name: &str, content: &[u8], method: u16, compressed: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&0x0403_4b50u32.to_le_bytes());
        bytes.extend_from_slice(&20u16.to_le_bytes());
        bytes.extend_from_slice(&0u16.to_le_bytes());
        bytes.extend_from_slice(&method.to_le_bytes());
        bytes.extend_from_slice(&0u16.to_le_bytes());
        bytes.extend_from_slice(&0u16.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&(content.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&(name.len() as u16).to_le_bytes());
        bytes.extend_from_slice(&0u16.to_le_bytes());
        bytes.extend_from_slice(name.as_bytes());
        bytes.extend_from_slice(compressed);
        bytes
    }

    fn gzipped_tar_entry(name: &str, content: &[u8]) -> Vec<u8> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        {
            let mut builder = tar::Builder::new(&mut encoder);
            let mut header = tar::Header::new_gnu();
            header.set_size(content.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder.append_data(&mut header, name, content).unwrap();
            builder.finish().unwrap();
        }
        encoder.finish().unwrap()
    }

    #[tokio::test]
    async fn status_reports_configured_sources() {
        let package_dir =
            std::env::temp_dir().join(format!("devpi-rs-status-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: vec![
                SourceConfig {
                    name: "corp".to_string(),
                    simple_url: "http://corp/simple/".to_string(),
                },
                SourceConfig {
                    name: "pypi".to_string(),
                    simple_url: "https://pypi.org/simple/".to_string(),
                },
            ],
            outside_url: None,
        });
        let create_index = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev")
            .body(axum::body::Body::from(r#"{"sources":["corp"]}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_index).await.unwrap().status(),
            StatusCode::CREATED
        );
        let request = Request::builder()
            .method(Method::GET)
            .uri("/+status")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "1");
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["status"], 200);
        assert_eq!(body["type"], "status");
        assert_eq!(body["result"]["role"], "MASTER");
        assert_eq!(body["result"]["api_version"], DEVPI_API_VERSION);
        assert_eq!(body["result"]["server_version"], DEVPI_SERVER_VERSION);
        assert_eq!(body["result"]["serial"], 1);
        assert_eq!(body["result"]["event-serial"], 1);
        assert_eq!(body["result"]["replication-errors"], json!({}));
        assert_eq!(body["result"]["sources"], json!(["corp", "pypi"]));
        assert!(
            body["result"]["indexes"]
                .as_array()
                .unwrap()
                .iter()
                .any(|index| {
                    index["name"] == "alice/dev"
                        && index["sources"] == json!(["corp"])
                        && index["resolved_sources"] == json!(["corp"])
                })
        );
        assert!(
            body["result"]["indexes"]
                .as_array()
                .unwrap()
                .iter()
                .any(|index| {
                    index["name"] == "root/pypi"
                        && index["sources"] == json!([])
                        && index["resolved_sources"] == json!(["corp", "pypi"])
                })
        );

        let slash_request = Request::builder()
            .method(Method::GET)
            .uri("/+status/")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(slash_request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["type"], "status");
    }

    #[tokio::test]
    async fn changelog_routes_return_persisted_events() {
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir: std::env::temp_dir()
                .join(format!("devpi-rs-changelog-{}", std::process::id())),
            sources: Vec::new(),
            outside_url: None,
        });
        let create_user = Request::builder()
            .method(Method::PUT)
            .uri("/alice")
            .body(axum::body::Body::from(r#"{"password":"123"}"#))
            .unwrap();
        let response = app.clone().oneshot(create_user).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "1");

        let request = Request::builder()
            .method(Method::GET)
            .uri("/+changelog/1")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "1");
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["type"], "changelog");
        assert_eq!(
            body["result"],
            json!([{"serial":1,"event":"user_create","user":"alice"}])
        );

        let streaming = Request::builder()
            .method(Method::GET)
            .uri("/+changelog/1-")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(streaming).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["result"].as_array().unwrap().len(), 1);

        let invalid = Request::builder()
            .method(Method::GET)
            .uri("/+changelog/not-a-number")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.oneshot(invalid).await.unwrap().status(),
            StatusCode::BAD_REQUEST
        );
    }

    #[tokio::test]
    async fn package_mutations_record_structured_changelog_events() {
        let (app, _package_dir) = test_router_with_index("package-changelog", "alice", "dev");

        let create_project = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/demo")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(create_project).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "1");

        let existing_project = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/demo")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(existing_project).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "1");

        let upload = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::from("sdist"))
            .unwrap();
        let response = app.clone().oneshot(upload).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "2");

        let tox = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::from(r#"{"envname":"py312","retcode":0}"#))
            .unwrap();
        let response = app.clone().oneshot(tox).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "3");

        let delete_tox = Request::builder()
            .method(Method::DELETE)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz.toxresult-0")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(delete_tox).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "4");

        let delete_file = Request::builder()
            .method(Method::DELETE)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(delete_file).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "5");

        let upload_for_version_delete = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::from("sdist"))
            .unwrap();
        let response = app
            .clone()
            .oneshot(upload_for_version_delete)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "6");

        let delete_version = Request::builder()
            .method(Method::DELETE)
            .uri("/alice/dev/demo/1.0.0")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(delete_version).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "7");

        let upload_for_project_delete = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/+f/demo/demo-2.0.0.tar.gz")
            .body(axum::body::Body::from("sdist"))
            .unwrap();
        let response = app
            .clone()
            .oneshot(upload_for_project_delete)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "8");

        let delete_project = Request::builder()
            .method(Method::DELETE)
            .uri("/alice/dev/demo")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(delete_project).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "9");

        let changelog = Request::builder()
            .method(Method::GET)
            .uri("/+changelog/1-")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(changelog).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(
            body["result"],
            json!([
                {"serial":1,"event":"project_create","stage":"alice/dev","project":"demo"},
                {"serial":2,"event":"file_upload","stage":"alice/dev","project":"demo","filename":"demo-1.0.0.tar.gz"},
                {"serial":3,"event":"toxresult_upload","stage":"alice/dev","project":"demo","filename":"demo-1.0.0.tar.gz","result_index":0},
                {"serial":4,"event":"toxresult_delete","stage":"alice/dev","project":"demo","filename":"demo-1.0.0.tar.gz","result_index":0},
                {"serial":5,"event":"file_delete","stage":"alice/dev","project":"demo","filename":"demo-1.0.0.tar.gz"},
                {"serial":6,"event":"file_upload","stage":"alice/dev","project":"demo","filename":"demo-1.0.0.tar.gz"},
                {"serial":7,"event":"version_delete","stage":"alice/dev","project":"demo","version":"1.0.0","files":1},
                {"serial":8,"event":"file_upload","stage":"alice/dev","project":"demo","filename":"demo-2.0.0.tar.gz"},
                {"serial":9,"event":"project_delete","stage":"alice/dev","project":"demo"},
            ])
        );
    }

    #[tokio::test]
    async fn authcheck_allows_api_and_rejects_unknown_original_uri() {
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir: std::env::temp_dir()
                .join(format!("devpi-rs-authcheck-{}", std::process::id())),
            sources: Vec::new(),
            outside_url: None,
        });
        let api_request = Request::builder()
            .method(Method::GET)
            .uri("/+authcheck")
            .header("X-Original-URI", "http://localhost/+api")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(api_request).await.unwrap().status(),
            StatusCode::OK
        );

        let unknown_request = Request::builder()
            .method(Method::GET)
            .uri("/+authcheck")
            .header(
                "X-Original-URI",
                "http://localhost/user/index/+unavailable_route/",
            )
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.oneshot(unknown_request).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );
    }

    #[tokio::test]
    async fn authcheck_enforces_package_read_acl() {
        let package_dir =
            std::env::temp_dir().join(format!("devpi-rs-authcheck-acl-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: Vec::new(),
            outside_url: None,
        });
        let create_user = Request::builder()
            .method(Method::PUT)
            .uri("/alice")
            .body(axum::body::Body::from(r#"{"password":"123"}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_user).await.unwrap().status(),
            StatusCode::CREATED
        );
        let create_index = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev")
            .body(axum::body::Body::from(r#"{"acl_pkg_read":["alice"]}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_index).await.unwrap().status(),
            StatusCode::CREATED
        );

        let denied = Request::builder()
            .method(Method::GET)
            .uri("/+authcheck")
            .header(
                "X-Original-URI",
                "http://localhost/alice/dev/+f/example/example-1.0.tar.gz",
            )
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(denied).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );

        let allowed = Request::builder()
            .method(Method::POST)
            .uri("/+authcheck")
            .header(
                "X-Original-URI",
                "http://localhost/alice/dev/+f/example/example-1.0.tar.gz",
            )
            .header(header::AUTHORIZATION, basic_auth("alice", "123"))
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(app.oneshot(allowed).await.unwrap().status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn authcheck_returns_forbidden_for_unknown_index() {
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir: std::env::temp_dir().join(format!(
                "devpi-rs-authcheck-missing-index-{}",
                std::process::id()
            )),
            sources: Vec::new(),
            outside_url: None,
        });
        let denied = Request::builder()
            .method(Method::GET)
            .uri("/+authcheck")
            .header(
                "X-Original-URI",
                "http://localhost/alice/dev/+f/example/example-1.0.tar.gz",
            )
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(denied).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert!(body_text(response).await.contains("index not found"));
    }

    #[tokio::test]
    async fn status_reports_last_upstream_fetches() {
        let dir =
            std::env::temp_dir().join(format!("devpi-rs-status-upstream-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let state = AppState {
            index: Arc::new(MultiSourceIndex::new(
                vec![
                    SourceConfig {
                        name: "corp".to_string(),
                        simple_url: "http://corp/simple/".to_string(),
                    },
                    SourceConfig {
                        name: "pypi".to_string(),
                        simple_url: "https://pypi.org/simple/".to_string(),
                    },
                ],
                dir.join("cache"),
            )),
            local: Arc::new(LocalStore::new(dir.clone())),
            registry: Arc::new(Registry::new(dir.clone())),
            serial: Arc::new(SerialStore::new(dir.join(".devpi-rs-serial"))),
            fetcher: Arc::new(MockFetcher {
                responses: Mutex::new(HashMap::from([
                    (
                        "http://corp/simple/demo/".to_string(),
                        Err(io::Error::other("corp down")),
                    ),
                    (
                        "https://pypi.org/simple/demo/".to_string(),
                        Ok("<a href=\"demo-1.0.0.tar.gz\">demo-1.0.0.tar.gz</a>".to_string()),
                    ),
                ])),
            }),
            external_poster: Arc::new(TcpExternalPoster),
            upstream_reports: Arc::new(Mutex::new(Vec::new())),
        };

        let response = render_legacy_simple_project_with_format(
            state.clone(),
            "demo".to_string(),
            SimpleFormat::Html,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = status(State(state)).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        let reports = body["result"]["upstream_reports"].as_array().unwrap();
        assert_eq!(reports.len(), 2);
        assert!(reports.iter().any(|report| {
            report["source"] == "corp"
                && report["url"] == "http://corp/simple/demo/"
                && report["fetched_at_unix_secs"].as_u64().is_some()
                && report["error"].as_str().unwrap().contains("corp down")
        }));
        assert!(reports.iter().any(|report| {
            report["source"] == "pypi"
                && report["url"] == "https://pypi.org/simple/demo/"
                && report["fetched_at_unix_secs"].as_u64().is_some()
                && report["error"].is_null()
        }));
    }

    #[tokio::test]
    async fn index_config_rejects_unknown_sources() {
        let package_dir =
            std::env::temp_dir().join(format!("devpi-rs-known-sources-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: vec![SourceConfig {
                name: "corp".to_string(),
                simple_url: "http://corp/simple/".to_string(),
            }],
            outside_url: None,
        });
        let bad = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev")
            .body(axum::body::Body::from(r#"{"sources":["missing"]}"#))
            .unwrap();
        let response = app.clone().oneshot(bad).await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(
            body_text(response)
                .await
                .contains("unknown upstream source")
        );

        let good = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev")
            .body(axum::body::Body::from(r#"{"sources":["corp"]}"#))
            .unwrap();
        let response = app.oneshot(good).await.unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        assert!(body_text(response).await.contains("\"sources\":[\"corp\"]"));
    }

    #[tokio::test]
    async fn renders_stage_simple_project_with_stage_file_links() {
        let state = test_state("simple");
        state
            .registry
            .put_index("alice", "dev", IndexInput::default())
            .unwrap();
        state
            .local
            .save_in("alice", "dev", "demo", "demo-1.0.0.tar.gz", b"sdist")
            .unwrap();

        let response = render_simple_project_with_format(
            state,
            "alice".to_string(),
            "dev".to_string(),
            "demo".to_string(),
            SimpleFormat::Html,
            None,
            false,
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            body_text(response)
                .await
                .contains("../../+f/demo/demo-1.0.0.tar.gz")
        );
    }

    #[tokio::test]
    async fn simple_html_routes_return_not_modified_for_matching_etag() {
        let (app, _package_dir) = test_router_with_index("simple-html-etag", "alice", "dev");
        let upload = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::from("sdist"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(upload).await.unwrap().status(),
            StatusCode::CREATED
        );

        let project = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+simple/demo/")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(project).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/html; charset=utf-8"
        );
        let etag = response
            .headers()
            .get(header::ETAG)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert!(
            body_text(response)
                .await
                .contains("../../+f/demo/demo-1.0.0.tar.gz")
        );

        let cached_project = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+simple/demo/")
            .header(header::IF_NONE_MATCH, etag.as_str())
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(cached_project).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(response.headers().get(header::ETAG).unwrap(), etag.as_str());
        assert!(body_text(response).await.is_empty());
    }

    #[tokio::test]
    async fn simple_html_routes_redirect_to_trailing_slash() {
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir: std::env::temp_dir()
                .join(format!("devpi-rs-simple-redirect-{}", std::process::id())),
            sources: Vec::new(),
            outside_url: None,
        });
        let root = Request::builder()
            .method(Method::GET)
            .uri("/root/pypi/+simple")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(root).await.unwrap();

        assert_eq!(response.status(), StatusCode::FOUND);
        assert_eq!(
            response.headers().get(header::LOCATION).unwrap(),
            "/root/pypi/+simple/"
        );

        let project = Request::builder()
            .method(Method::GET)
            .uri("/root/pypi/+simple/Demo_Pkg")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(project).await.unwrap();

        assert_eq!(response.status(), StatusCode::FOUND);
        assert_eq!(
            response.headers().get(header::LOCATION).unwrap(),
            "/root/pypi/+simple/demo-pkg/"
        );
    }

    #[tokio::test]
    async fn simple_no_slash_routes_do_not_redirect_installers() {
        let package_dir =
            std::env::temp_dir().join(format!("devpi-rs-simple-installer-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: Vec::new(),
            outside_url: None,
        });
        let upload = Request::builder()
            .method(Method::PUT)
            .uri("/root/pypi/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::from("sdist"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(upload).await.unwrap().status(),
            StatusCode::CREATED
        );
        let request = Request::builder()
            .method(Method::GET)
            .uri("/root/pypi/+simple/demo")
            .header(header::USER_AGENT, "pip/25.0")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(body_text(response).await.contains("demo-1.0.0.tar.gz"));
    }

    #[tokio::test]
    async fn simple_project_html_includes_refresh_form_for_browsers() {
        let package_dir =
            std::env::temp_dir().join(format!("devpi-rs-simple-form-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: Vec::new(),
            outside_url: None,
        });
        let upload = Request::builder()
            .method(Method::PUT)
            .uri("/root/pypi/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::from("sdist"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(upload).await.unwrap().status(),
            StatusCode::CREATED
        );

        let browser = Request::builder()
            .method(Method::GET)
            .uri("/root/pypi/+simple/demo/")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(browser).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::VARY).unwrap(),
            "Accept, User-Agent"
        );
        let body = body_text(response).await;
        assert!(body.contains("<form action=\"/root/pypi/+simple/demo/refresh\""));
        assert!(body.contains("name=\"refresh\""));

        let installer = Request::builder()
            .method(Method::GET)
            .uri("/root/pypi/+simple/demo/")
            .header(header::USER_AGENT, "pip/25.0")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(installer).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_text(response).await;
        assert!(!body.contains("<form"));
        assert!(body.contains("demo-1.0.0.tar.gz"));
    }

    #[tokio::test]
    async fn missing_simple_project_returns_html_404() {
        let (app, _package_dir) = test_router_with_index("simple-missing", "alice", "dev");
        let request = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+simple/missing/")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/html; charset=utf-8"
        );
        assert_eq!(
            response.headers().get(header::VARY).unwrap(),
            "Accept, User-Agent"
        );
        assert!(body_text(response).await.contains("No links for missing"));
    }

    #[tokio::test]
    async fn missing_simple_project_returns_pep_691_json_404() {
        let (app, _package_dir) = test_router_with_index("simple-missing-json", "alice", "dev");
        let request = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+simple/missing/")
            .header(header::ACCEPT, SIMPLE_JSON_CONTENT_TYPE)
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            SIMPLE_JSON_CONTENT_TYPE
        );
        assert_eq!(
            response.headers().get(header::VARY).unwrap(),
            "Accept, User-Agent"
        );
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["meta"]["api-version"], "1.0");
        assert_eq!(body["name"], "missing");
        assert!(body["files"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn stage_simple_returns_not_found_for_unknown_index() {
        let state = test_state("stage-simple-index-404");

        let root = render_simple_root_with_format(
            state.clone(),
            "alice".to_string(),
            "dev".to_string(),
            SimpleFormat::Html,
            None,
        )
        .await;
        assert_eq!(root.status(), StatusCode::NOT_FOUND);
        assert!(body_text(root).await.contains("index not found"));

        let project = render_simple_project_with_format(
            state.clone(),
            "alice".to_string(),
            "dev".to_string(),
            "demo".to_string(),
            SimpleFormat::Html,
            None,
            false,
        )
        .await;
        assert_eq!(project.status(), StatusCode::NOT_FOUND);
        assert!(body_text(project).await.contains("index not found"));

        let refresh = refresh_simple_project(
            state,
            "alice".to_string(),
            "dev".to_string(),
            "demo".to_string(),
            None,
        )
        .await;
        assert_eq!(refresh.status(), StatusCode::NOT_FOUND);
        assert!(body_text(refresh).await.contains("index not found"));
    }

    #[tokio::test]
    async fn simple_routes_return_pep_691_json_for_accept_header() {
        let (app, _package_dir) = test_router_with_index("pep691", "alice", "dev");
        let upload = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::from("sdist"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(upload).await.unwrap().status(),
            StatusCode::CREATED
        );

        let project = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+simple/demo/")
            .header(header::ACCEPT, SIMPLE_JSON_CONTENT_TYPE)
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(project).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            SIMPLE_JSON_CONTENT_TYPE
        );
        assert_eq!(
            response.headers().get(header::VARY).unwrap(),
            "Accept, User-Agent"
        );
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "1");
        let project_etag = response
            .headers()
            .get(header::ETAG)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["meta"]["api-version"], "1.0");
        assert_eq!(body["name"], "demo");
        assert_eq!(body["files"][0]["filename"], "demo-1.0.0.tar.gz");
        assert_eq!(body["files"][0]["url"], "../../+f/demo/demo-1.0.0.tar.gz");
        assert_eq!(
            body["files"][0]["hashes"]["sha256"],
            "714772a9f82b2aeb4fa5f7092d00fe4ac4c9cdeb6800840b6ed39ea64c4d785a"
        );
        assert_eq!(body["files"][0]["requires-python"], "");
        let cached_project = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+simple/demo/")
            .header(header::ACCEPT, SIMPLE_JSON_CONTENT_TYPE)
            .header(header::IF_NONE_MATCH, project_etag.as_str())
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(cached_project).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(response.headers().get(header::ETAG).unwrap(), &project_etag);
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "1");
        assert!(body_text(response).await.is_empty());

        let project_no_slash = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+simple/demo")
            .header(header::ACCEPT, SIMPLE_JSON_CONTENT_TYPE)
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(project_no_slash).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["name"], "demo");

        let root = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+simple/")
            .header(
                header::ACCEPT,
                format!("{SIMPLE_JSON_CONTENT_TYPE}; q=1.0, text/html; q=0.1"),
            )
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(root).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::VARY).unwrap(),
            "Accept, User-Agent"
        );
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "1");
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            SIMPLE_JSON_CONTENT_TYPE
        );
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["meta"]["api-version"], "1.0");
        assert_eq!(body["projects"][0]["name"], "demo");

        let root_no_slash = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+simple")
            .header(header::ACCEPT, SIMPLE_JSON_CONTENT_TYPE)
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(root_no_slash).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["projects"][0]["name"], "demo");

        let legacy_upload = Request::builder()
            .method(Method::PUT)
            .uri("/files/legacy/legacy-1.0.0.tar.gz")
            .body(axum::body::Body::from("sdist"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(legacy_upload).await.unwrap().status(),
            StatusCode::CREATED
        );
        let legacy_project_no_slash = Request::builder()
            .method(Method::GET)
            .uri("/simple/legacy")
            .header(header::ACCEPT, SIMPLE_JSON_CONTENT_TYPE)
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(legacy_project_no_slash).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "2");
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["name"], "legacy");
        assert_eq!(body["files"][0]["url"], "/files/legacy/legacy-1.0.0.tar.gz");

        let legacy_root_no_slash = Request::builder()
            .method(Method::GET)
            .uri("/simple")
            .header(header::ACCEPT, SIMPLE_JSON_CONTENT_TYPE)
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(legacy_root_no_slash).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "2");
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert!(
            body["projects"]
                .as_array()
                .unwrap()
                .iter()
                .any(|project| project["name"] == "legacy")
        );
    }

    #[tokio::test]
    async fn stage_index_returns_pep_691_json_for_accept_header() {
        let (app, _package_dir) = test_router_with_index("index-pep691", "alice", "dev");
        let upload = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::from("sdist"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(upload).await.unwrap().status(),
            StatusCode::CREATED
        );
        let request = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev")
            .header(header::ACCEPT, SIMPLE_JSON_CONTENT_TYPE)
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            SIMPLE_JSON_CONTENT_TYPE
        );
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["meta"]["api-version"], "1.0");
        assert_eq!(body["projects"][0]["name"], "demo");

        let slash_request = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/")
            .header(header::ACCEPT, SIMPLE_JSON_CONTENT_TYPE)
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(slash_request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            SIMPLE_JSON_CONTENT_TYPE
        );
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["meta"]["api-version"], "1.0");
        assert_eq!(body["projects"][0]["name"], "demo");
    }

    #[tokio::test]
    async fn saves_stage_file_without_touching_default_stage() {
        let state = test_state("upload");
        state
            .registry
            .put_index(
                "alice",
                "dev",
                IndexInput {
                    acl_upload: Some(vec![":ANONYMOUS:".to_string()]),
                    ..IndexInput::default()
                },
            )
            .unwrap();

        let response = save_file(
            state.clone(),
            SaveFileRequest {
                user: "alice".to_string(),
                index: "dev".to_string(),
                project: "demo".to_string(),
                filename: "demo-1.0.0.tar.gz".to_string(),
                body: Bytes::from_static(b"sdist"),
                metadata: BTreeMap::new(),
                auth_user: None,
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::CREATED);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["status"], 201);
        assert_eq!(body["project"], "demo");
        assert_eq!(body["filename"], "demo-1.0.0.tar.gz");
        assert_eq!(
            state
                .local
                .read_in("alice", "dev", "demo", "demo-1.0.0.tar.gz")
                .unwrap(),
            b"sdist"
        );
        assert_eq!(
            state
                .local
                .read_in("root", "pypi", "demo", "demo-1.0.0.tar.gz")
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotFound
        );
    }

    #[tokio::test]
    async fn rejects_bad_stage_segment() {
        let state = test_state("bad-stage");

        let response = render_simple_project_with_format(
            state,
            "+bad".to_string(),
            "dev".to_string(),
            "demo".to_string(),
            SimpleFormat::Html,
            None,
            false,
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn rejects_stage_simple_with_unknown_upstream_source() {
        let state = test_state("unknown-source");
        state
            .registry
            .put_index(
                "alice",
                "dev",
                IndexInput {
                    bases: None,
                    volatile: None,
                    index_type: None,
                    acl_upload: None,
                    acl_pkg_read: None,
                    acl_toxresult_upload: None,
                    sources: Some(vec!["missing".to_string()]),
                    ..Default::default()
                },
            )
            .unwrap();

        let response = render_simple_root_with_format(
            state,
            "alice".to_string(),
            "dev".to_string(),
            SimpleFormat::Html,
            None,
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(
            body_text(response)
                .await
                .contains("unknown upstream source")
        );
    }

    #[tokio::test]
    async fn stage_simple_project_uses_configured_upstream_sources() {
        let dir = std::env::temp_dir().join(format!(
            "devpi-rs-stage-source-select-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let state = AppState {
            index: Arc::new(MultiSourceIndex::new(
                vec![
                    SourceConfig {
                        name: "corp".to_string(),
                        simple_url: "http://corp/simple/".to_string(),
                    },
                    SourceConfig {
                        name: "pypi".to_string(),
                        simple_url: "https://pypi.org/simple/".to_string(),
                    },
                ],
                std::env::temp_dir(),
            )),
            local: Arc::new(LocalStore::new(dir.clone())),
            registry: Arc::new(Registry::new(dir.clone())),
            serial: Arc::new(SerialStore::new(dir.join(".devpi-rs-serial"))),
            fetcher: Arc::new(MockFetcher {
                responses: Mutex::new(HashMap::from([(
                    "https://pypi.org/simple/demo/".to_string(),
                    Ok("<a href=\"demo-1.0.0.tar.gz\">demo-1.0.0.tar.gz</a>".to_string()),
                )])),
            }),
            external_poster: Arc::new(TcpExternalPoster),
            upstream_reports: Arc::new(Mutex::new(Vec::new())),
        };
        state
            .registry
            .put_index(
                "alice",
                "dev",
                IndexInput {
                    bases: None,
                    volatile: None,
                    index_type: None,
                    acl_upload: None,
                    acl_pkg_read: None,
                    acl_toxresult_upload: None,
                    sources: Some(vec!["pypi".to_string()]),
                    ..Default::default()
                },
            )
            .unwrap();

        let response = render_simple_project_with_format(
            state,
            "alice".to_string(),
            "dev".to_string(),
            "demo".to_string(),
            SimpleFormat::Html,
            None,
            false,
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = body_text(response).await;
        assert!(body.contains("data-source=\"pypi\""));
        assert!(!body.contains("data-source=\"corp\""));
    }

    #[tokio::test]
    async fn mirror_simple_project_propagates_valid_pypi_last_serial() {
        let dir = std::env::temp_dir().join(format!("devpi-rs-pypi-last-{}", std::process::id()));
        let cache_dir =
            std::env::temp_dir().join(format!("devpi-rs-pypi-last-cache-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&cache_dir);
        let state = AppState {
            index: Arc::new(MultiSourceIndex::new(Vec::new(), cache_dir)),
            local: Arc::new(LocalStore::new(dir.clone())),
            registry: Arc::new(Registry::new(dir.clone())),
            serial: Arc::new(SerialStore::new(dir.join(".devpi-rs-serial"))),
            fetcher: Arc::new(SerialMockFetcher {
                responses: Mutex::new(HashMap::from([(
                    "https://pypi.org/simple/demo/".to_string(),
                    Ok(crate::upstream::FetchedPage {
                        body: r#"<a href="demo-1.0.0.tar.gz">demo-1.0.0.tar.gz</a>"#.to_string(),
                        pypi_last_serial: Some(10000),
                    }),
                )])),
            }),
            external_poster: Arc::new(TcpExternalPoster),
            upstream_reports: Arc::new(Mutex::new(Vec::new())),
        };
        state
            .serial
            .bump_with_event(json!({"event": "seed"}))
            .unwrap();

        let response = render_simple_project_with_format(
            state,
            "root".to_string(),
            "pypi".to_string(),
            "demo".to_string(),
            SimpleFormat::Html,
            None,
            false,
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "1");
        assert_eq!(
            response.headers().get("X-PYPI-LAST-SERIAL").unwrap(),
            "10000"
        );
        assert!(body_text(response).await.contains("demo-1.0.0.tar.gz"));
    }

    #[tokio::test]
    async fn mirror_whitelist_controls_private_project_upstream_merging() {
        let dir =
            std::env::temp_dir().join(format!("devpi-rs-mirror-whitelist-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let state = AppState {
            index: Arc::new(MultiSourceIndex::new(
                vec![SourceConfig {
                    name: "pypi".to_string(),
                    simple_url: "https://pypi.org/simple/".to_string(),
                }],
                std::env::temp_dir(),
            )),
            local: Arc::new(LocalStore::new(dir.clone())),
            registry: Arc::new(Registry::new(dir.clone())),
            serial: Arc::new(SerialStore::new(dir.join(".devpi-rs-serial"))),
            fetcher: Arc::new(MockFetcher {
                responses: Mutex::new(HashMap::from([(
                    "https://pypi.org/simple/demo/".to_string(),
                    Ok("<a href=\"demo-2.0.0.tar.gz\">demo-2.0.0.tar.gz</a>".to_string()),
                )])),
            }),
            external_poster: Arc::new(TcpExternalPoster),
            upstream_reports: Arc::new(Mutex::new(Vec::new())),
        };
        state
            .registry
            .put_index(
                "alice",
                "dev",
                IndexInput {
                    bases: Some(vec!["root/pypi".to_string()]),
                    sources: Some(vec!["pypi".to_string()]),
                    ..Default::default()
                },
            )
            .unwrap();
        state
            .local
            .save_in("alice", "dev", "demo", "demo-1.0.0.tar.gz", b"sdist")
            .unwrap();

        let blocked = render_simple_project_with_format(
            state.clone(),
            "alice".to_string(),
            "dev".to_string(),
            "demo".to_string(),
            SimpleFormat::Html,
            None,
            false,
        )
        .await;
        assert_eq!(blocked.status(), StatusCode::OK);
        let body = body_text(blocked).await;
        assert!(body.contains("demo-1.0.0.tar.gz"));
        assert!(!body.contains("demo-2.0.0.tar.gz"));
        assert!(body.contains("mirror_whitelist"));

        state
            .registry
            .put_index(
                "alice",
                "dev",
                IndexInput {
                    mirror_whitelist: Some(vec!["demo".to_string()]),
                    ..Default::default()
                },
            )
            .unwrap();
        let whitelisted = render_simple_project_with_format(
            state,
            "alice".to_string(),
            "dev".to_string(),
            "demo".to_string(),
            SimpleFormat::Html,
            None,
            false,
        )
        .await;
        assert_eq!(whitelisted.status(), StatusCode::OK);
        let body = body_text(whitelisted).await;
        assert!(body.contains("demo-1.0.0.tar.gz"));
        assert!(body.contains("demo-2.0.0.tar.gz"));
        assert!(!body.contains("upstream mirror links are omitted"));
    }

    #[tokio::test]
    async fn mirror_whitelist_inheritance_controls_base_whitelist_merging() {
        let dir = std::env::temp_dir().join(format!(
            "devpi-rs-mirror-whitelist-inheritance-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let state = AppState {
            index: Arc::new(MultiSourceIndex::new(
                vec![SourceConfig {
                    name: "pypi".to_string(),
                    simple_url: "https://pypi.org/simple/".to_string(),
                }],
                std::env::temp_dir(),
            )),
            local: Arc::new(LocalStore::new(dir.clone())),
            registry: Arc::new(Registry::new(dir.clone())),
            serial: Arc::new(SerialStore::new(dir.join(".devpi-rs-serial"))),
            fetcher: Arc::new(MockFetcher {
                responses: Mutex::new(HashMap::from([(
                    "https://pypi.org/simple/demo/".to_string(),
                    Ok("<a href=\"demo-2.0.0.tar.gz\">demo-2.0.0.tar.gz</a>".to_string()),
                )])),
            }),
            external_poster: Arc::new(TcpExternalPoster),
            upstream_reports: Arc::new(Mutex::new(Vec::new())),
        };
        state
            .registry
            .put_index(
                "team",
                "prod",
                IndexInput {
                    bases: Some(vec!["root/pypi".to_string()]),
                    mirror_whitelist: Some(vec!["demo".to_string()]),
                    ..Default::default()
                },
            )
            .unwrap();
        state
            .registry
            .put_index(
                "alice",
                "dev",
                IndexInput {
                    bases: Some(vec!["team/prod".to_string()]),
                    sources: Some(vec!["pypi".to_string()]),
                    ..Default::default()
                },
            )
            .unwrap();
        state
            .local
            .save_in("alice", "dev", "demo", "demo-1.0.0.tar.gz", b"sdist")
            .unwrap();

        let blocked = render_simple_project_with_format(
            state.clone(),
            "alice".to_string(),
            "dev".to_string(),
            "demo".to_string(),
            SimpleFormat::Html,
            None,
            false,
        )
        .await;
        assert_eq!(blocked.status(), StatusCode::OK);
        let body = body_text(blocked).await;
        assert!(body.contains("demo-1.0.0.tar.gz"));
        assert!(!body.contains("demo-2.0.0.tar.gz"));
        assert!(body.contains("mirror_whitelist"));

        state
            .registry
            .put_index(
                "alice",
                "dev",
                IndexInput {
                    mirror_whitelist_inheritance: Some("union".to_string()),
                    ..Default::default()
                },
            )
            .unwrap();
        let allowed = render_simple_project_with_format(
            state,
            "alice".to_string(),
            "dev".to_string(),
            "demo".to_string(),
            SimpleFormat::Html,
            None,
            false,
        )
        .await;
        assert_eq!(allowed.status(), StatusCode::OK);
        let body = body_text(allowed).await;
        assert!(body.contains("demo-1.0.0.tar.gz"));
        assert!(body.contains("demo-2.0.0.tar.gz"));
        assert!(!body.contains("upstream mirror links are omitted"));
    }

    #[tokio::test]
    async fn simple_refresh_reloads_cached_upstream_project() {
        let dir =
            std::env::temp_dir().join(format!("devpi-rs-simple-refresh-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let fetcher = Arc::new(MockFetcher {
            responses: Mutex::new(HashMap::from([(
                "http://corp/simple/demo/".to_string(),
                Ok("<a href=\"demo-1.0.0.tar.gz\">demo-1.0.0.tar.gz</a>".to_string()),
            )])),
        });
        let state = AppState {
            index: Arc::new(MultiSourceIndex::new(
                vec![SourceConfig {
                    name: "corp".to_string(),
                    simple_url: "http://corp/simple/".to_string(),
                }],
                dir.join("cache"),
            )),
            local: Arc::new(LocalStore::new(dir.clone())),
            registry: Arc::new(Registry::new(dir.clone())),
            serial: Arc::new(SerialStore::new(dir.join(".devpi-rs-serial"))),
            fetcher: fetcher.clone(),
            external_poster: Arc::new(TcpExternalPoster),
            upstream_reports: Arc::new(Mutex::new(Vec::new())),
        };
        state
            .registry
            .put_index(
                "alice",
                "dev",
                IndexInput {
                    sources: Some(vec!["corp".to_string()]),
                    ..Default::default()
                },
            )
            .unwrap();

        let first = render_simple_project_with_format(
            state.clone(),
            "alice".to_string(),
            "dev".to_string(),
            "demo".to_string(),
            SimpleFormat::Html,
            None,
            false,
        )
        .await;
        assert_eq!(first.status(), StatusCode::OK);
        assert!(body_text(first).await.contains("demo-1.0.0.tar.gz"));

        fetcher.responses.lock().unwrap().insert(
            "http://corp/simple/demo/".to_string(),
            Ok("<a href=\"demo-2.0.0.tar.gz\">demo-2.0.0.tar.gz</a>".to_string()),
        );
        let app = Router::new()
            .route(
                "/{user}/{index}/+simple/{project}/refresh",
                post(stage_simple_project_refresh),
            )
            .with_state(state.clone());
        let refresh = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev/+simple/demo/refresh")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(refresh).await.unwrap();

        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            response.headers().get(header::LOCATION).unwrap(),
            "/alice/dev/+simple/demo/"
        );

        let second = render_simple_project_with_format(
            state,
            "alice".to_string(),
            "dev".to_string(),
            "demo".to_string(),
            SimpleFormat::Html,
            None,
            false,
        )
        .await;
        let body = body_text(second).await;
        assert!(body.contains("demo-2.0.0.tar.gz"));
        assert!(!body.contains("demo-1.0.0.tar.gz"));
    }

    #[tokio::test]
    async fn simple_project_marks_stale_cache_when_upstream_fails() {
        let dir = std::env::temp_dir().join(format!(
            "devpi-rs-stale-simple-cache-{}",
            std::process::id()
        ));
        let cache_dir = std::env::temp_dir().join(format!(
            "devpi-rs-stale-simple-cache-upstream-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&cache_dir);
        let state = AppState {
            index: Arc::new(MultiSourceIndex::new(
                vec![SourceConfig {
                    name: "pypi".to_string(),
                    simple_url: "https://mirror.example/simple/".to_string(),
                }],
                cache_dir,
            )),
            local: Arc::new(LocalStore::new(dir.clone())),
            registry: Arc::new(Registry::new(dir.clone())),
            serial: Arc::new(SerialStore::new(dir.join(".devpi-rs-serial"))),
            fetcher: Arc::new(MockFetcher {
                responses: Mutex::new(HashMap::from([(
                    "https://mirror.example/simple/demo/".to_string(),
                    Ok(r#"<a href="demo-1.0.0.zip">demo-1.0.0.zip</a>"#.to_string()),
                )])),
            }),
            external_poster: Arc::new(TcpExternalPoster),
            upstream_reports: Arc::new(Mutex::new(Vec::new())),
        };

        let first = render_legacy_simple_project_sync(&state, "demo", SimpleFormat::Html);
        assert_eq!(first.status(), StatusCode::OK);
        assert!(first.headers().get(header::CACHE_CONTROL).is_none());
        assert!(body_text(first).await.contains("demo-1.0.0.zip"));

        let stale = render_legacy_simple_project_sync(&state, "demo", SimpleFormat::Html);
        assert_eq!(stale.status(), StatusCode::OK);
        assert_eq!(
            stale.headers().get(header::CACHE_CONTROL).unwrap(),
            "max-age=0"
        );
        assert_eq!(stale.headers().get(header::PRAGMA).unwrap(), "no-cache");
        assert!(stale.headers().get(header::EXPIRES).is_some());
        assert!(body_text(stale).await.contains("demo-1.0.0.zip"));
    }

    #[tokio::test]
    async fn simple_project_bad_gateway_when_upstream_fails_without_cache() {
        let dir = std::env::temp_dir().join(format!(
            "devpi-rs-empty-simple-cache-{}",
            std::process::id()
        ));
        let cache_dir = std::env::temp_dir().join(format!(
            "devpi-rs-empty-simple-cache-upstream-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&cache_dir);
        let state = AppState {
            index: Arc::new(MultiSourceIndex::new(
                vec![SourceConfig {
                    name: "pypi".to_string(),
                    simple_url: "https://mirror.example/simple/".to_string(),
                }],
                cache_dir,
            )),
            local: Arc::new(LocalStore::new(dir.clone())),
            registry: Arc::new(Registry::new(dir.clone())),
            serial: Arc::new(SerialStore::new(dir.join(".devpi-rs-serial"))),
            fetcher: Arc::new(MockFetcher {
                responses: Mutex::new(HashMap::new()),
            }),
            external_poster: Arc::new(TcpExternalPoster),
            upstream_reports: Arc::new(Mutex::new(Vec::new())),
        };

        let missing = render_legacy_simple_project_sync(&state, "demo", SimpleFormat::Html);
        assert_eq!(missing.status(), StatusCode::BAD_GATEWAY);
        assert!(body_text(missing).await.contains("upstream error"));

        state.local.save("demo", "demo-1.0.0.zip", b"zip").unwrap();
        let local = render_legacy_simple_project_sync(&state, "demo", SimpleFormat::Html);
        assert_eq!(local.status(), StatusCode::OK);
        assert!(body_text(local).await.contains("demo-1.0.0.zip"));
    }

    #[tokio::test]
    async fn mirror_project_json_bad_gateway_when_upstream_fails_without_cache() {
        let dir = std::env::temp_dir().join(format!(
            "devpi-rs-mirror-json-empty-cache-{}",
            std::process::id()
        ));
        let cache_dir = std::env::temp_dir().join(format!(
            "devpi-rs-mirror-json-empty-cache-upstream-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&cache_dir);
        let state = AppState {
            index: Arc::new(MultiSourceIndex::new(Vec::new(), cache_dir)),
            local: Arc::new(LocalStore::new(dir.clone())),
            registry: Arc::new(Registry::new(dir.clone())),
            serial: Arc::new(SerialStore::new(dir.join(".devpi-rs-serial"))),
            fetcher: Arc::new(MockFetcher {
                responses: Mutex::new(HashMap::new()),
            }),
            external_poster: Arc::new(TcpExternalPoster),
            upstream_reports: Arc::new(Mutex::new(Vec::new())),
        };

        let response = render_stage_project_sync(&state, "root", "pypi", "demo", true, None);
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert!(body_text(response).await.contains("upstream error"));
    }

    #[tokio::test]
    async fn mirror_project_json_includes_upstream_simple_files() {
        let dir = std::env::temp_dir().join(format!(
            "devpi-rs-mirror-json-upstream-{}",
            std::process::id()
        ));
        let cache_dir = std::env::temp_dir().join(format!(
            "devpi-rs-mirror-json-upstream-cache-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&cache_dir);
        let state = AppState {
            index: Arc::new(MultiSourceIndex::new(Vec::new(), cache_dir)),
            local: Arc::new(LocalStore::new(dir.clone())),
            registry: Arc::new(Registry::new(dir.clone())),
            serial: Arc::new(SerialStore::new(dir.join(".devpi-rs-serial"))),
            fetcher: Arc::new(MockFetcher {
                responses: Mutex::new(HashMap::from([(
                    "https://pypi.org/simple/demo/".to_string(),
                    Ok(r#"<a href="https://pypi.org/packages/demo-1.0.0.zip#sha256=abc">demo-1.0.0.zip</a>"#.to_string()),
                )])),
            }),
            external_poster: Arc::new(TcpExternalPoster),
            upstream_reports: Arc::new(Mutex::new(Vec::new())),
        };

        let response = render_stage_project_sync(&state, "root", "pypi", "demo", true, None);
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["result"]["files"][0]["filename"], "demo-1.0.0.zip");
        assert_eq!(body["result"]["files"][0]["version"], "1.0.0");
        assert_eq!(body["result"]["files"][0]["hashes"]["sha256"], "abc");
        assert!(
            body["result"]["files"][0]["url"]
                .as_str()
                .unwrap()
                .starts_with("/root/pypi/+e/")
        );
    }

    #[tokio::test]
    async fn mirror_version_json_bad_gateway_when_upstream_fails_without_cache() {
        let dir = std::env::temp_dir().join(format!(
            "devpi-rs-mirror-version-empty-cache-{}",
            std::process::id()
        ));
        let cache_dir = std::env::temp_dir().join(format!(
            "devpi-rs-mirror-version-empty-cache-upstream-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&cache_dir);
        let state = AppState {
            index: Arc::new(MultiSourceIndex::new(Vec::new(), cache_dir)),
            local: Arc::new(LocalStore::new(dir.clone())),
            registry: Arc::new(Registry::new(dir.clone())),
            serial: Arc::new(SerialStore::new(dir.join(".devpi-rs-serial"))),
            fetcher: Arc::new(MockFetcher {
                responses: Mutex::new(HashMap::new()),
            }),
            external_poster: Arc::new(TcpExternalPoster),
            upstream_reports: Arc::new(Mutex::new(Vec::new())),
        };

        let response = render_version_metadata(
            state,
            "root".to_string(),
            "pypi".to_string(),
            "demo".to_string(),
            "2.6".to_string(),
            true,
            None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert!(body_text(response).await.contains("upstream error"));
    }

    #[tokio::test]
    async fn mirror_version_json_includes_upstream_simple_version() {
        let dir = std::env::temp_dir().join(format!(
            "devpi-rs-mirror-version-upstream-{}",
            std::process::id()
        ));
        let cache_dir = std::env::temp_dir().join(format!(
            "devpi-rs-mirror-version-upstream-cache-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&cache_dir);
        let state = AppState {
            index: Arc::new(MultiSourceIndex::new(Vec::new(), cache_dir)),
            local: Arc::new(LocalStore::new(dir.clone())),
            registry: Arc::new(Registry::new(dir.clone())),
            serial: Arc::new(SerialStore::new(dir.join(".devpi-rs-serial"))),
            fetcher: Arc::new(MockFetcher {
                responses: Mutex::new(HashMap::from([(
                    "https://pypi.org/simple/demo/".to_string(),
                    Ok(
                        r#"<a href="https://pypi.org/packages/demo-2.6.zip">demo-2.6.zip</a>"#
                            .to_string(),
                    ),
                )])),
            }),
            external_poster: Arc::new(TcpExternalPoster),
            upstream_reports: Arc::new(Mutex::new(Vec::new())),
        };

        let response = render_version_metadata(
            state,
            "root".to_string(),
            "pypi".to_string(),
            "demo".to_string(),
            "2.6".to_string(),
            true,
            None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["type"], "versiondata");
        assert_eq!(body["result"]["name"], "demo");
        assert_eq!(body["result"]["version"], "2.6");
    }

    #[tokio::test]
    async fn stage_simple_project_json_preserves_upstream_link_metadata() {
        let dir = std::env::temp_dir().join(format!(
            "devpi-rs-stage-json-link-metadata-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let state = AppState {
            index: Arc::new(MultiSourceIndex::new(
                vec![SourceConfig {
                    name: "pypi".to_string(),
                    simple_url: "https://pypi.org/simple/".to_string(),
                }],
                std::env::temp_dir(),
            )),
            local: Arc::new(LocalStore::new(dir.clone())),
            registry: Arc::new(Registry::new(dir.clone())),
            serial: Arc::new(SerialStore::new(dir.join(".devpi-rs-serial"))),
            fetcher: Arc::new(MockFetcher {
                responses: Mutex::new(HashMap::from([(
                    "https://pypi.org/simple/demo/".to_string(),
                    Ok(r#"<a href="demo-1.0.0-py3-none-any.whl#sha256=file123"
                          data-requires-python="&gt;=3.10"
                          data-yanked="bad wheel"
                          data-core-metadata="sha256=meta123"
                          data-gpg-sig="true">demo-1.0.0-py3-none-any.whl</a>"#
                        .to_string()),
                )])),
            }),
            external_poster: Arc::new(TcpExternalPoster),
            upstream_reports: Arc::new(Mutex::new(Vec::new())),
        };
        state
            .registry
            .put_index(
                "alice",
                "dev",
                IndexInput {
                    sources: Some(vec!["pypi".to_string()]),
                    ..Default::default()
                },
            )
            .unwrap();

        let response = render_simple_project_with_format(
            state,
            "alice".to_string(),
            "dev".to_string(),
            "demo".to_string(),
            SimpleFormat::Json,
            None,
            false,
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            SIMPLE_JSON_CONTENT_TYPE
        );
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        let file = &body["files"][0];
        assert_eq!(file["filename"], "demo-1.0.0-py3-none-any.whl");
        assert_eq!(file["hashes"]["sha256"], "file123");
        assert_eq!(file["requires-python"], ">=3.10");
        assert_eq!(file["yanked"], "bad wheel");
        assert_eq!(file["core-metadata"]["sha256"], "meta123");
        assert_eq!(file["gpg-sig"], true);
    }

    #[tokio::test]
    async fn mirror_simple_rewrites_upstream_links_to_local_e_routes() {
        let dir = std::env::temp_dir().join(format!(
            "devpi-rs-mirror-link-rewrite-{}",
            std::process::id()
        ));
        let cache_dir = std::env::temp_dir().join(format!(
            "devpi-rs-mirror-link-rewrite-cache-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&cache_dir);
        let state = AppState {
            index: Arc::new(MultiSourceIndex::new(
                vec![SourceConfig {
                    name: "pypi".to_string(),
                    simple_url: "https://pypi.org/simple/".to_string(),
                }],
                cache_dir,
            )),
            local: Arc::new(LocalStore::new(dir.clone())),
            registry: Arc::new(Registry::new(dir.clone())),
            serial: Arc::new(SerialStore::new(dir.join(".devpi-rs-serial"))),
            fetcher: Arc::new(MockFetcher {
                responses: Mutex::new(HashMap::from([(
                    "https://pypi.org/simple/demo/".to_string(),
                    Ok(r#"<a href="demo-1.0.0-py3-none-any.whl#sha256=file123"
                          data-requires-python="&gt;=3.10">demo-1.0.0-py3-none-any.whl</a>"#
                        .to_string()),
                )])),
            }),
            external_poster: Arc::new(TcpExternalPoster),
            upstream_reports: Arc::new(Mutex::new(Vec::new())),
        };

        let html_response = render_simple_project_with_format(
            state.clone(),
            "root".to_string(),
            "pypi".to_string(),
            "demo".to_string(),
            SimpleFormat::Html,
            None,
            false,
        )
        .await;

        assert_eq!(html_response.status(), StatusCode::OK);
        let html = body_text(html_response).await;
        assert!(html.contains(r#"href="../../+e/u"#));
        assert!(html.contains("demo-1.0.0-py3-none-any.whl#sha256=file123"));
        assert!(html.contains(r#"data-requires-python="&gt;=3.10""#));
        assert!(!html.contains(r#"href="https://pypi.org/simple/demo/demo-1.0.0"#));

        let json_response = render_simple_project_with_format(
            state,
            "root".to_string(),
            "pypi".to_string(),
            "demo".to_string(),
            SimpleFormat::Json,
            None,
            false,
        )
        .await;

        assert_eq!(json_response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(json_response).await).unwrap();
        let file = &body["files"][0];
        assert!(file["url"].as_str().unwrap().starts_with("../../+e/u"));
        assert_eq!(file["filename"], "demo-1.0.0-py3-none-any.whl");
        assert_eq!(file["hashes"]["sha256"], "file123");
    }

    #[tokio::test]
    async fn mirror_no_project_list_skips_upstream_root_listing_only() {
        let dir = std::env::temp_dir().join(format!(
            "devpi-rs-mirror-no-project-list-{}",
            std::process::id()
        ));
        let cache_dir = std::env::temp_dir().join(format!(
            "devpi-rs-mirror-no-project-list-cache-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&cache_dir);
        let state = AppState {
            index: Arc::new(MultiSourceIndex::new(
                vec![SourceConfig {
                    name: "pypi".to_string(),
                    simple_url: "https://pypi.org/simple/".to_string(),
                }],
                cache_dir,
            )),
            local: Arc::new(LocalStore::new(dir.clone())),
            registry: Arc::new(Registry::new(dir.clone())),
            serial: Arc::new(SerialStore::new(dir.join(".devpi-rs-serial"))),
            fetcher: Arc::new(MockFetcher {
                responses: Mutex::new(HashMap::from([
                    (
                        "https://pypi.org/simple/".to_string(),
                        Ok(r#"<a href="demo/">demo</a>"#.to_string()),
                    ),
                    (
                        "https://pypi.org/simple/demo/".to_string(),
                        Ok(r#"<a href="demo-1.0.0.tar.gz">demo-1.0.0.tar.gz</a>"#.to_string()),
                    ),
                ])),
            }),
            external_poster: Arc::new(TcpExternalPoster),
            upstream_reports: Arc::new(Mutex::new(Vec::new())),
        };
        state
            .registry
            .put_index(
                "root",
                "pypi",
                IndexInput {
                    mirror_no_project_list: Some(true),
                    ..Default::default()
                },
            )
            .unwrap();

        let root = render_simple_root_with_format(
            state.clone(),
            "root".to_string(),
            "pypi".to_string(),
            SimpleFormat::Html,
            None,
        )
        .await;
        assert_eq!(root.status(), StatusCode::OK);
        assert!(!body_text(root).await.contains("demo"));
        assert!(state.upstream_reports.lock().unwrap().is_empty());

        let project = render_simple_project_with_format(
            state,
            "root".to_string(),
            "pypi".to_string(),
            "demo".to_string(),
            SimpleFormat::Html,
            None,
            false,
        )
        .await;
        assert_eq!(project.status(), StatusCode::OK);
        assert!(body_text(project).await.contains("demo-1.0.0.tar.gz"));
    }

    #[tokio::test]
    async fn mirror_stage_uses_configured_mirror_url_for_upstream_fetches() {
        let dir = std::env::temp_dir().join(format!("devpi-rs-mirror-url-{}", std::process::id()));
        let cache_dir =
            std::env::temp_dir().join(format!("devpi-rs-mirror-url-cache-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&cache_dir);
        let state = AppState {
            index: Arc::new(MultiSourceIndex::new(
                vec![SourceConfig {
                    name: "pypi".to_string(),
                    simple_url: "https://pypi.org/simple/".to_string(),
                }],
                cache_dir,
            )),
            local: Arc::new(LocalStore::new(dir.clone())),
            registry: Arc::new(Registry::new(dir.clone())),
            serial: Arc::new(SerialStore::new(dir.join(".devpi-rs-serial"))),
            fetcher: Arc::new(MockFetcher {
                responses: Mutex::new(HashMap::from([
                    (
                        "https://mirror.example/simple/".to_string(),
                        Ok(r#"<a href="demo/">demo</a>"#.to_string()),
                    ),
                    (
                        "https://mirror.example/simple/demo/".to_string(),
                        Ok(r#"<a href="demo-1.0.0.tar.gz">demo-1.0.0.tar.gz</a>"#.to_string()),
                    ),
                ])),
            }),
            external_poster: Arc::new(TcpExternalPoster),
            upstream_reports: Arc::new(Mutex::new(Vec::new())),
        };
        state
            .registry
            .put_index(
                "root",
                "pypi",
                IndexInput {
                    mirror_url: Some("https://mirror.example/simple/".to_string()),
                    ..Default::default()
                },
            )
            .unwrap();

        let root = render_simple_root_with_format(
            state.clone(),
            "root".to_string(),
            "pypi".to_string(),
            SimpleFormat::Html,
            None,
        )
        .await;
        assert_eq!(root.status(), StatusCode::OK);
        assert!(body_text(root).await.contains("demo"));

        let project = render_simple_project_with_format(
            state.clone(),
            "root".to_string(),
            "pypi".to_string(),
            "demo".to_string(),
            SimpleFormat::Html,
            None,
            false,
        )
        .await;
        assert_eq!(project.status(), StatusCode::OK);
        assert!(body_text(project).await.contains("demo-1.0.0.tar.gz"));
        let reports = state.upstream_reports.lock().unwrap();
        assert!(
            reports
                .iter()
                .all(|report| report.url.starts_with("https://mirror.example/simple/"))
        );
    }

    #[tokio::test]
    async fn mirror_cache_expiry_uses_cached_project_until_refresh() {
        let dir = std::env::temp_dir().join(format!(
            "devpi-rs-mirror-cache-expiry-{}",
            std::process::id()
        ));
        let cache_dir = std::env::temp_dir().join(format!(
            "devpi-rs-mirror-cache-expiry-cache-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&cache_dir);
        let fetcher = Arc::new(MockFetcher {
            responses: Mutex::new(HashMap::from([(
                "https://mirror.example/simple/demo/".to_string(),
                Ok(r#"<a href="demo-1.0.0.tar.gz">demo-1.0.0.tar.gz</a>"#.to_string()),
            )])),
        });
        let state = AppState {
            index: Arc::new(MultiSourceIndex::new(Vec::new(), cache_dir)),
            local: Arc::new(LocalStore::new(dir.clone())),
            registry: Arc::new(Registry::new(dir.clone())),
            serial: Arc::new(SerialStore::new(dir.join(".devpi-rs-serial"))),
            fetcher: fetcher.clone(),
            external_poster: Arc::new(TcpExternalPoster),
            upstream_reports: Arc::new(Mutex::new(Vec::new())),
        };
        state
            .registry
            .put_index(
                "root",
                "pypi",
                IndexInput {
                    mirror_url: Some("https://mirror.example/simple/".to_string()),
                    mirror_cache_expiry: Some(600),
                    ..Default::default()
                },
            )
            .unwrap();

        let first = render_simple_project_with_format(
            state.clone(),
            "root".to_string(),
            "pypi".to_string(),
            "demo".to_string(),
            SimpleFormat::Html,
            None,
            false,
        )
        .await;
        assert_eq!(first.status(), StatusCode::OK);
        assert!(body_text(first).await.contains("demo-1.0.0.tar.gz"));

        fetcher.responses.lock().unwrap().insert(
            "https://mirror.example/simple/demo/".to_string(),
            Ok(r#"<a href="demo-2.0.0.tar.gz">demo-2.0.0.tar.gz</a>"#.to_string()),
        );
        let cached = render_simple_project_with_format(
            state.clone(),
            "root".to_string(),
            "pypi".to_string(),
            "demo".to_string(),
            SimpleFormat::Html,
            None,
            false,
        )
        .await;
        let cached_body = body_text(cached).await;
        assert!(cached_body.contains("demo-1.0.0.tar.gz"));
        assert!(!cached_body.contains("demo-2.0.0.tar.gz"));
        assert!(
            fetcher
                .responses
                .lock()
                .unwrap()
                .contains_key("https://mirror.example/simple/demo/")
        );

        let app = Router::new()
            .route(
                "/{user}/{index}/+simple/{project}/refresh",
                post(stage_simple_project_refresh),
            )
            .with_state(state.clone());
        let refresh = Request::builder()
            .method(Method::POST)
            .uri("/root/pypi/+simple/demo/refresh")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(refresh).await.unwrap();
        assert_eq!(response.status(), StatusCode::SEE_OTHER);

        let refreshed = render_simple_project_with_format(
            state,
            "root".to_string(),
            "pypi".to_string(),
            "demo".to_string(),
            SimpleFormat::Html,
            None,
            false,
        )
        .await;
        let refreshed_body = body_text(refreshed).await;
        assert!(refreshed_body.contains("demo-2.0.0.tar.gz"));
        assert!(!refreshed_body.contains("demo-1.0.0.tar.gz"));
    }

    #[tokio::test]
    async fn mirror_e_route_fetches_and_caches_remote_file() {
        let dir =
            std::env::temp_dir().join(format!("devpi-rs-mirror-file-cache-{}", std::process::id()));
        let cache_dir = std::env::temp_dir().join(format!(
            "devpi-rs-mirror-file-cache-upstream-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&cache_dir);
        let state = AppState {
            index: Arc::new(MultiSourceIndex::new(Vec::new(), cache_dir)),
            local: Arc::new(LocalStore::new(dir.clone())),
            registry: Arc::new(Registry::new(dir.clone())),
            serial: Arc::new(SerialStore::new(dir.join(".devpi-rs-serial"))),
            fetcher: Arc::new(MockFetcher {
                responses: Mutex::new(HashMap::from([(
                    "https://pypi.org/packages/demo-1.0.0.whl".to_string(),
                    Ok("wheel-bytes".to_string()),
                )])),
            }),
            external_poster: Arc::new(TcpExternalPoster),
            upstream_reports: Arc::new(Mutex::new(Vec::new())),
        };
        let encoded = encode_mirror_url("https://pypi.org/packages/demo-1.0.0.whl");
        let relpath = format!("{encoded}/demo-1.0.0.whl");

        let first = read_mirror_file(
            state.clone(),
            "root".to_string(),
            "pypi".to_string(),
            relpath.clone(),
            FileReadOptions::default(),
        )
        .await;
        assert_eq!(first.status(), StatusCode::OK);
        let etag = first
            .headers()
            .get(header::ETAG)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert_eq!(body_text(first).await, "wheel-bytes");

        let cached = read_mirror_file(
            state.clone(),
            "root".to_string(),
            "pypi".to_string(),
            relpath.clone(),
            FileReadOptions {
                conditional: ConditionalRequest {
                    if_none_match: Some(etag.clone()),
                    ..ConditionalRequest::default()
                },
                ..FileReadOptions::default()
            },
        )
        .await;
        assert_eq!(cached.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(cached.headers().get(header::ETAG).unwrap(), etag.as_str());
        assert!(body_text(cached).await.is_empty());

        let second = read_mirror_file(
            state,
            "root".to_string(),
            "pypi".to_string(),
            format!("{relpath}/"),
            FileReadOptions::default(),
        )
        .await;
        assert_eq!(second.status(), StatusCode::OK);
        assert_eq!(body_text(second).await, "wheel-bytes");
    }

    #[tokio::test]
    async fn mirror_e_route_fetches_and_caches_remote_metadata() {
        let dir = std::env::temp_dir().join(format!(
            "devpi-rs-mirror-metadata-cache-{}",
            std::process::id()
        ));
        let cache_dir = std::env::temp_dir().join(format!(
            "devpi-rs-mirror-metadata-cache-upstream-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&cache_dir);
        let state = AppState {
            index: Arc::new(MultiSourceIndex::new(Vec::new(), cache_dir)),
            local: Arc::new(LocalStore::new(dir.clone())),
            registry: Arc::new(Registry::new(dir.clone())),
            serial: Arc::new(SerialStore::new(dir.join(".devpi-rs-serial"))),
            fetcher: Arc::new(MockFetcher {
                responses: Mutex::new(HashMap::from([(
                    "https://pypi.org/packages/demo-1.0.0.whl.metadata".to_string(),
                    Ok("Metadata-Version: 2.1\nName: demo\n".to_string()),
                )])),
            }),
            external_poster: Arc::new(TcpExternalPoster),
            upstream_reports: Arc::new(Mutex::new(Vec::new())),
        };
        let encoded = encode_mirror_url("https://pypi.org/packages/demo-1.0.0.whl");
        let relpath = format!("{encoded}/demo-1.0.0.whl.metadata");

        let disabled = read_mirror_file(
            state.clone(),
            "root".to_string(),
            "pypi".to_string(),
            relpath.clone(),
            FileReadOptions::default(),
        )
        .await;
        assert_eq!(disabled.status(), StatusCode::NOT_FOUND);

        state
            .registry
            .put_index(
                "root",
                "pypi",
                IndexInput {
                    mirror_provides_core_metadata: Some(true),
                    ..Default::default()
                },
            )
            .unwrap();

        let first = read_mirror_file(
            state.clone(),
            "root".to_string(),
            "pypi".to_string(),
            relpath.clone(),
            FileReadOptions::default(),
        )
        .await;
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(
            body_text(first).await,
            "Metadata-Version: 2.1\nName: demo\n"
        );

        let second = read_mirror_file(
            state,
            "root".to_string(),
            "pypi".to_string(),
            relpath,
            FileReadOptions::default(),
        )
        .await;
        assert_eq!(second.status(), StatusCode::OK);
        assert_eq!(
            body_text(second).await,
            "Metadata-Version: 2.1\nName: demo\n"
        );
    }

    #[tokio::test]
    async fn mirror_e_route_redirects_external_urls_until_cached() {
        let dir = std::env::temp_dir().join(format!(
            "devpi-rs-mirror-external-url-{}",
            std::process::id()
        ));
        let cache_dir = std::env::temp_dir().join(format!(
            "devpi-rs-mirror-external-url-cache-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&cache_dir);
        let state = AppState {
            index: Arc::new(MultiSourceIndex::new(Vec::new(), cache_dir)),
            local: Arc::new(LocalStore::new(dir.clone())),
            registry: Arc::new(Registry::new(dir.clone())),
            serial: Arc::new(SerialStore::new(dir.join(".devpi-rs-serial"))),
            fetcher: Arc::new(MockFetcher {
                responses: Mutex::new(HashMap::from([(
                    "https://pypi.org/packages/demo-1.0.0.whl".to_string(),
                    Ok("wheel-bytes".to_string()),
                )])),
            }),
            external_poster: Arc::new(TcpExternalPoster),
            upstream_reports: Arc::new(Mutex::new(Vec::new())),
        };
        state
            .registry
            .put_index(
                "root",
                "pypi",
                IndexInput {
                    mirror_use_external_urls: Some(true),
                    mirror_provides_core_metadata: Some(true),
                    ..Default::default()
                },
            )
            .unwrap();
        let url = "https://pypi.org/packages/demo-1.0.0.whl";
        let encoded = encode_mirror_url(url);
        let relpath = format!("{encoded}/demo-1.0.0.whl");

        let redirect = read_mirror_file(
            state.clone(),
            "root".to_string(),
            "pypi".to_string(),
            relpath.clone(),
            FileReadOptions::default(),
        )
        .await;
        assert_eq!(redirect.status(), StatusCode::FOUND);
        assert_eq!(redirect.headers().get(header::LOCATION).unwrap(), url);

        let metadata_redirect = read_mirror_file(
            state.clone(),
            "root".to_string(),
            "pypi".to_string(),
            format!("{encoded}/demo-1.0.0.whl.metadata"),
            FileReadOptions::default(),
        )
        .await;
        assert_eq!(metadata_redirect.status(), StatusCode::FOUND);
        assert_eq!(
            metadata_redirect.headers().get(header::LOCATION).unwrap(),
            "https://pypi.org/packages/demo-1.0.0.whl.metadata"
        );

        state
            .registry
            .put_index(
                "root",
                "pypi",
                IndexInput {
                    mirror_use_external_urls: Some(false),
                    mirror_provides_core_metadata: Some(true),
                    ..Default::default()
                },
            )
            .unwrap();
        let fetched = read_mirror_file(
            state.clone(),
            "root".to_string(),
            "pypi".to_string(),
            relpath.clone(),
            FileReadOptions::default(),
        )
        .await;
        assert_eq!(fetched.status(), StatusCode::OK);
        assert_eq!(body_text(fetched).await, "wheel-bytes");

        state
            .registry
            .put_index(
                "root",
                "pypi",
                IndexInput {
                    mirror_use_external_urls: Some(true),
                    mirror_provides_core_metadata: Some(true),
                    ..Default::default()
                },
            )
            .unwrap();
        let cached = read_mirror_file(
            state,
            "root".to_string(),
            "pypi".to_string(),
            relpath,
            FileReadOptions::default(),
        )
        .await;
        assert_eq!(cached.status(), StatusCode::OK);
        assert_eq!(body_text(cached).await, "wheel-bytes");
    }

    #[tokio::test]
    async fn renders_stage_index_json_with_projects() {
        let dir = std::env::temp_dir().join(format!("devpi-rs-stage-index-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let state = AppState {
            index: Arc::new(MultiSourceIndex::new(
                vec![
                    SourceConfig {
                        name: "corp".to_string(),
                        simple_url: "http://corp/simple/".to_string(),
                    },
                    SourceConfig {
                        name: "pypi".to_string(),
                        simple_url: "https://pypi.org/simple/".to_string(),
                    },
                ],
                std::env::temp_dir(),
            )),
            local: Arc::new(LocalStore::new(dir.clone())),
            registry: Arc::new(Registry::new(dir.clone())),
            serial: Arc::new(SerialStore::new(dir.join(".devpi-rs-serial"))),
            fetcher: Arc::new(CurlFetcher::default()),
            external_poster: Arc::new(TcpExternalPoster),
            upstream_reports: Arc::new(Mutex::new(Vec::new())),
        };
        state
            .local
            .save_in("alice", "dev", "demo", "demo-1.0.0.tar.gz", b"sdist")
            .unwrap();
        state
            .registry
            .put_index("alice", "dev", IndexInput::default())
            .unwrap();

        let response =
            render_stage_index_with_projects(state, "alice".to_string(), "dev".to_string(), true)
                .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = body_text(response).await;
        assert!(body.contains("\"status\":200"));
        assert!(body.contains("\"type\":\"stage\""));
        assert!(body.contains("\"simpleindex\":\"/alice/dev/+simple/\""));
        assert!(body.contains("\"pypisubmit\":\"/alice/dev/\""));
        assert!(body.contains("\"projects\":[\"demo\"]"));
        assert!(body.contains("\"multi_source\":true"));
        assert!(body.contains("\"resolved_sources\":[\"corp\",\"pypi\"]"));
    }

    #[tokio::test]
    async fn stage_index_returns_not_found_for_unknown_index() {
        let state = test_state("stage-index-404");

        let response =
            render_stage_index_with_projects(state, "alice".to_string(), "dev".to_string(), true)
                .await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn mirror_stage_index_json_omits_pypisubmit() {
        let state = test_state("mirror-stage-index");

        let response =
            render_stage_index_with_projects(state, "root".to_string(), "pypi".to_string(), true)
                .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = body_text(response).await;
        assert!(body.contains("\"type\":\"mirror\""));
        assert!(body.contains("\"simpleindex\":\"/root/pypi/+simple/\""));
        assert!(!body.contains("\"pypisubmit\""));
    }

    #[tokio::test]
    async fn renders_stage_api_json() {
        let package_dir =
            std::env::temp_dir().join(format!("devpi-rs-stage-api-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: Vec::new(),
            outside_url: None,
        });
        let create_user = Request::builder()
            .method(Method::PUT)
            .uri("/alice")
            .body(axum::body::Body::from(r#"{"password":"123"}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_user).await.unwrap().status(),
            StatusCode::CREATED
        );
        let create_index = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev")
            .body(axum::body::Body::from("{}"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_index).await.unwrap().status(),
            StatusCode::CREATED
        );
        let request = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+api")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.clone().oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("X-DEVPI-API-VERSION").unwrap(),
            DEVPI_API_VERSION
        );
        assert_eq!(
            response.headers().get("X-DEVPI-SERVER-VERSION").unwrap(),
            DEVPI_SERVER_VERSION
        );
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "2");
        let body = body_text(response).await;
        assert!(body.contains("\"type\":\"apiconfig\""));
        assert!(body.contains("\"index\":\"/alice/dev/\""));
        assert!(body.contains("\"simpleindex\":\"/alice/dev/+simple/\""));
        assert!(body.contains("\"multi-source\""));
        assert!(body.contains("\"push-no-docs\""));
        assert!(body.contains("\"push-only-docs\""));
        assert!(body.contains("\"push-register-project\""));

        let slash_request = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+api/")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(slash_request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(body_text(response).await.contains("\"type\":\"apiconfig\""));
    }

    #[tokio::test]
    async fn stage_api_uses_configured_outside_url() {
        let package_dir =
            std::env::temp_dir().join(format!("devpi-rs-stage-api-outside-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: Vec::new(),
            outside_url: Some("http://outside.example".to_string()),
        });
        let request = Request::builder()
            .method(Method::GET)
            .uri("/root/pypi/+api")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        let result = &body["result"];
        assert_eq!(result["login"], "http://outside.example/+login");
        assert_eq!(result["index"], "http://outside.example/root/pypi");
        assert_eq!(
            result["simpleindex"],
            "http://outside.example/root/pypi/+simple/"
        );
        assert!(result.get("pypisubmit").is_none());
    }

    #[tokio::test]
    async fn stage_api_uses_x_outside_url_header_before_configured_url() {
        let package_dir = std::env::temp_dir().join(format!(
            "devpi-rs-stage-api-header-outside-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: Vec::new(),
            outside_url: Some("http://configured.example".to_string()),
        });
        let request = Request::builder()
            .method(Method::GET)
            .uri("/root/pypi/+api")
            .header("X-Outside-Url", "http://proxy.example/devpi")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        let result = &body["result"];
        assert_eq!(result["login"], "http://proxy.example/devpi/+login");
        assert_eq!(result["index"], "http://proxy.example/devpi/root/pypi");
        assert_eq!(
            result["simpleindex"],
            "http://proxy.example/devpi/root/pypi/+simple/"
        );
    }

    #[tokio::test]
    async fn stage_api_uses_trailing_slash_pypisubmit_with_outside_url() {
        let package_dir = std::env::temp_dir().join(format!(
            "devpi-rs-stage-api-pypisubmit-outside-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: Vec::new(),
            outside_url: Some("http://outside.example/devpi".to_string()),
        });
        let create_index = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev")
            .body(axum::body::Body::from(r#"{"acl_upload":[":ANONYMOUS:"]}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_index).await.unwrap().status(),
            StatusCode::CREATED
        );
        let request = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+api")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        let result = &body["result"];
        assert_eq!(result["index"], "http://outside.example/devpi/alice/dev");
        assert_eq!(
            result["pypisubmit"],
            "http://outside.example/devpi/alice/dev/"
        );
    }

    #[tokio::test]
    async fn stage_api_uses_host_header_when_no_outside_url_is_configured() {
        let package_dir = std::env::temp_dir().join(format!(
            "devpi-rs-stage-api-host-outside-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: Vec::new(),
            outside_url: None,
        });
        let request = Request::builder()
            .method(Method::GET)
            .uri("/root/pypi/+api")
            .header(header::HOST, "packages.example:3141")
            .header("X-Forwarded-Proto", "https")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        let result = &body["result"];
        assert_eq!(result["login"], "https://packages.example:3141/+login");
        assert_eq!(result["index"], "https://packages.example:3141/root/pypi");
    }

    #[tokio::test]
    async fn stage_api_returns_not_found_for_unknown_index() {
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir: std::env::temp_dir()
                .join(format!("devpi-rs-stage-api-404-{}", std::process::id())),
            sources: Vec::new(),
            outside_url: None,
        });
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/user/name/+api")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn renders_root_and_user_api_json() {
        let package_dir =
            std::env::temp_dir().join(format!("devpi-rs-root-api-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: Vec::new(),
            outside_url: None,
        });
        let root = Request::builder()
            .method(Method::GET)
            .uri("/+api")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(root).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = body_text(response).await;
        assert!(body.contains("\"type\":\"apiconfig\""));
        assert!(body.contains("\"login\":\"/+login\""));
        assert!(body.contains("\"authstatus\":[\"noauth\",\"\",[]]"));
        assert!(!body.contains("\"simpleindex\""));

        let root_slash = Request::builder()
            .method(Method::GET)
            .uri("/+api/")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(root_slash).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(body_text(response).await.contains("\"type\":\"apiconfig\""));

        let create_user = Request::builder()
            .method(Method::PUT)
            .uri("/alice")
            .body(axum::body::Body::from(r#"{"password":"123"}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_user).await.unwrap().status(),
            StatusCode::CREATED
        );
        let user = Request::builder()
            .method(Method::GET)
            .uri("/alice/+api")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(user).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = body_text(response).await;
        assert!(body.contains("\"type\":\"apiconfig\""));
        assert!(!body.contains("\"simpleindex\""));

        let user_slash = Request::builder()
            .method(Method::GET)
            .uri("/alice/+api/")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(user_slash).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(body_text(response).await.contains("\"type\":\"apiconfig\""));
    }

    #[tokio::test]
    async fn user_api_returns_not_found_for_unknown_user() {
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir: std::env::temp_dir()
                .join(format!("devpi-rs-user-api-404-{}", std::process::id())),
            sources: Vec::new(),
            outside_url: None,
        });
        let response = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/missing/+api")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn creates_and_reads_user_config() {
        let package_dir =
            std::env::temp_dir().join(format!("devpi-rs-user-config-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: Vec::new(),
            outside_url: None,
        });
        let missing_password = Request::builder()
            .method(Method::PUT)
            .uri("/bob")
            .body(axum::body::Body::from(r#"{"email":"bob@example.com"}"#))
            .unwrap();
        let response = app.clone().oneshot(missing_password).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(
            body_text(response)
                .await
                .contains("password needs to be set")
        );
        let put = Request::builder()
            .method(Method::PUT)
            .uri("/alice")
            .body(axum::body::Body::from(
                r#"{"password":"123","email":"alice@example.com"}"#,
            ))
            .unwrap();

        let response = app.clone().oneshot(put).await.unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let get = Request::builder()
            .method(Method::GET)
            .uri("/alice")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(get).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "1");
        let body = body_text(response).await;
        assert!(body.contains("\"type\":\"userconfig\""));
        assert!(body.contains("\"username\":\"alice\""));
        assert!(body.contains("\"email\":\"alice@example.com\""));
        assert!(!body.contains("password"));
    }

    #[tokio::test]
    async fn login_returns_proxy_auth_and_x_devpi_auth_can_upload() {
        let package_dir =
            std::env::temp_dir().join(format!("devpi-rs-login-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: Vec::new(),
            outside_url: None,
        });
        let create_user = Request::builder()
            .method(Method::PUT)
            .uri("/alice")
            .body(axum::body::Body::from(r#"{"password":"123"}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_user).await.unwrap().status(),
            StatusCode::CREATED
        );
        let create_index = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev")
            .body(axum::body::Body::from(r#"{"acl_upload":["alice"]}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_index).await.unwrap().status(),
            StatusCode::CREATED
        );

        let login = Request::builder()
            .method(Method::POST)
            .uri("/+login")
            .body(axum::body::Body::from(
                r#"{"user":"alice","password":"123"}"#,
            ))
            .unwrap();
        let response = app.clone().oneshot(login).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["type"], "proxyauth");
        assert_eq!(body["message"], "login successful");
        let token = body["result"]["password"].as_str().unwrap();
        assert!(token.starts_with("devpi-rs-token-v1:"));
        assert_ne!(token, "123");
        assert_eq!(body["result"]["expiration"], 36_000);

        let slash_login = Request::builder()
            .method(Method::POST)
            .uri("/+login/")
            .body(axum::body::Body::from(
                r#"{"user":"alice","password":"123"}"#,
            ))
            .unwrap();
        let response = app.clone().oneshot(slash_login).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["type"], "proxyauth");

        let x_devpi_auth = encode_base64(format!("alice:{token}").as_bytes());
        let upload = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .header("X-Devpi-Auth", x_devpi_auth)
            .body(axum::body::Body::from("sdist"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(upload).await.unwrap().status(),
            StatusCode::CREATED
        );

        let bad_x_devpi_auth = encode_base64(b"alice:wrong-token");
        let bad_upload = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/+f/demo/demo-2.0.0.tar.gz")
            .header("X-Devpi-Auth", bad_x_devpi_auth)
            .body(axum::body::Body::from("sdist"))
            .unwrap();
        let response = app.clone().oneshot(bad_upload).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.headers().get(header::WWW_AUTHENTICATE).unwrap(),
            "Basic realm=\"devpi-rs\""
        );

        let bad_login = Request::builder()
            .method(Method::POST)
            .uri("/+login")
            .body(axum::body::Body::from(
                r#"{"user":"alice","password":"wrong"}"#,
            ))
            .unwrap();
        assert_eq!(
            app.oneshot(bad_login).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn changing_password_returns_new_proxy_auth() {
        let package_dir =
            std::env::temp_dir().join(format!("devpi-rs-user-password-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: Vec::new(),
            outside_url: None,
        });
        let create_user = Request::builder()
            .method(Method::PUT)
            .uri("/alice")
            .body(axum::body::Body::from(r#"{"password":"old"}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_user).await.unwrap().status(),
            StatusCode::CREATED
        );

        let update = Request::builder()
            .method(Method::PATCH)
            .uri("/alice")
            .header(header::AUTHORIZATION, basic_auth("alice", "old"))
            .body(axum::body::Body::from(r#"{"password":"new"}"#))
            .unwrap();
        let response = app.clone().oneshot(update).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["type"], "userpassword");
        assert_eq!(body["message"], "user updated, new proxy auth");
        assert_eq!(body["result"]["expiration"], LOGIN_EXPIRATION_SECS);
        let token = body["result"]["password"].as_str().unwrap();
        assert!(token.starts_with("devpi-rs-token-v1:"));

        let old_login = Request::builder()
            .method(Method::POST)
            .uri("/+login")
            .body(axum::body::Body::from(
                r#"{"user":"alice","password":"old"}"#,
            ))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(old_login).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );

        let token_auth = Request::builder()
            .method(Method::GET)
            .uri("/+api")
            .header(
                "X-Devpi-Auth",
                encode_base64(format!("alice:{token}").as_bytes()),
            )
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(token_auth).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_text(response).await;
        assert!(body.contains("\"authstatus\":[\"ok\",\"alice\",[]]"));
    }

    #[tokio::test]
    async fn patches_user_and_index_config() {
        let package_dir =
            std::env::temp_dir().join(format!("devpi-rs-patch-config-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: Vec::new(),
            outside_url: None,
        });
        let create_user = Request::builder()
            .method(Method::PUT)
            .uri("/alice")
            .body(axum::body::Body::from(r#"{"password":"123"}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_user).await.unwrap().status(),
            StatusCode::CREATED
        );
        let anonymous_patch_user = Request::builder()
            .method(Method::PATCH)
            .uri("/alice/")
            .body(axum::body::Body::from(r#"{"email":"blocked@example.com"}"#))
            .unwrap();
        assert_eq!(
            app.clone()
                .oneshot(anonymous_patch_user)
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        let patch_user = Request::builder()
            .method(Method::PATCH)
            .uri("/alice/")
            .header(header::AUTHORIZATION, basic_auth("alice", "123"))
            .body(axum::body::Body::from(r#"{"email":"alice@example.com"}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(patch_user).await.unwrap().status(),
            StatusCode::OK
        );
        let create_index = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev")
            .body(axum::body::Body::from(r#"{"bases":["root/pypi"]}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_index).await.unwrap().status(),
            StatusCode::CREATED
        );
        let anonymous_patch = Request::builder()
            .method(Method::PATCH)
            .uri("/alice/dev")
            .body(axum::body::Body::from(r#"{"volatile":false}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(anonymous_patch).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );
        let patch_index = Request::builder()
            .method(Method::PATCH)
            .uri("/alice/dev")
            .header(header::AUTHORIZATION, basic_auth("alice", "123"))
            .body(axum::body::Body::from(
                r#"{"volatile":false,"acl_upload":["alice"]}"#,
            ))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(patch_index).await.unwrap().status(),
            StatusCode::OK
        );
        let list_patch_index = Request::builder()
            .method(Method::PATCH)
            .uri("/alice/dev")
            .header(header::AUTHORIZATION, basic_auth("alice", "123"))
            .body(axum::body::Body::from(
                r#"["volatile=False","mirror_cache_expiry=0","mirror_ignore_serial_header=True","mirror_no_project_list=True","mirror_provides_core_metadata=True","mirror_use_external_urls=True","bases+=/root/other/","bases-=root/other","mirror_whitelist+=Foo_Bar","mirror_whitelist+=baz","mirror_whitelist-=baz","acl_upload+=bob"]"#,
            ))
            .unwrap();
        assert_eq!(
            app.clone()
                .oneshot(list_patch_index)
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        let noop_list_patch_index = Request::builder()
            .method(Method::PATCH)
            .uri("/alice/dev?error_on_noop")
            .header(header::AUTHORIZATION, basic_auth("alice", "123"))
            .body(axum::body::Body::from(r#"["acl_upload+=bob"]"#))
            .unwrap();
        let response = app.clone().oneshot(noop_list_patch_index).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(
            body_text(response)
                .await
                .contains("The requested modifications resulted in no changes")
        );

        let get_user = Request::builder()
            .method(Method::GET)
            .uri("/alice/")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(get_user).await.unwrap();
        let body = body_text(response).await;
        assert!(body.contains("\"email\":\"alice@example.com\""));
        assert!(!body.contains("password"));

        let get_index = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(get_index).await.unwrap();
        let body = body_text(response).await;
        assert!(body.contains("\"bases\":[\"root/pypi\"]"));
        assert!(body.contains("\"volatile\":false"));
        assert!(body.contains("\"acl_upload\":[\"alice\",\"bob\"]"));
        assert!(body.contains("\"mirror_whitelist\":[\"foo-bar\"]"));
        assert!(body.contains("\"mirror_cache_expiry\":0"));
        assert!(body.contains("\"mirror_ignore_serial_header\":true"));
        assert!(body.contains("\"mirror_no_project_list\":true"));
        assert!(body.contains("\"mirror_provides_core_metadata\":true"));
        assert!(body.contains("\"mirror_use_external_urls\":true"));
    }

    #[tokio::test]
    async fn patch_preserves_and_deletes_unknown_index_config() {
        let package_dir = std::env::temp_dir().join(format!(
            "devpi-rs-patch-extra-config-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: Vec::new(),
            outside_url: None,
        });
        let create_user = Request::builder()
            .method(Method::PUT)
            .uri("/alice")
            .body(axum::body::Body::from(r#"{"password":"123"}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_user).await.unwrap().status(),
            StatusCode::CREATED
        );
        let create_index = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev")
            .header(header::AUTHORIZATION, basic_auth("alice", "123"))
            .body(axum::body::Body::from(
                r#"{"title":"Demo","ham":"baz","notify":true}"#,
            ))
            .unwrap();
        let response = app.clone().oneshot(create_index).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let list_patch = Request::builder()
            .method(Method::PATCH)
            .uri("/alice/dev")
            .header(header::AUTHORIZATION, basic_auth("alice", "123"))
            .body(axum::body::Body::from(
                r#"["acl_upload+=bob","notify=true","answer=42"]"#,
            ))
            .unwrap();
        let response = app.clone().oneshot(list_patch).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_text(response).await;
        assert!(body.contains("\"ham\":\"baz\""));
        assert!(body.contains("\"notify\":true"));
        assert!(body.contains("\"answer\":42"));

        let delete_patch = Request::builder()
            .method(Method::PATCH)
            .uri("/alice/dev")
            .header(header::AUTHORIZATION, basic_auth("alice", "123"))
            .body(axum::body::Body::from(r#"["ham-=","notify-=","title-="]"#))
            .unwrap();
        let response = app.clone().oneshot(delete_patch).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_text(response).await;
        assert!(!body.contains("\"ham\""));
        assert!(!body.contains("\"notify\""));
        assert!(!body.contains("\"title\""));
        assert!(body.contains("\"answer\":42"));
        assert!(body.contains("\"acl_upload\":[\"alice\",\"bob\"]"));
    }

    #[tokio::test]
    async fn put_existing_configs_conflicts_and_patch_missing_is_not_found() {
        let package_dir =
            std::env::temp_dir().join(format!("devpi-rs-put-patch-config-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: Vec::new(),
            outside_url: None,
        });

        let create_user = Request::builder()
            .method(Method::PUT)
            .uri("/alice")
            .body(axum::body::Body::from(r#"{"password":"old"}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_user).await.unwrap().status(),
            StatusCode::CREATED
        );

        let put_existing_user = Request::builder()
            .method(Method::PUT)
            .uri("/alice")
            .body(axum::body::Body::from(r#"{"password":"new"}"#))
            .unwrap();
        let response = app.clone().oneshot(put_existing_user).await.unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert!(body_text(response).await.contains("user already exists"));

        let new_password_login = Request::builder()
            .method(Method::POST)
            .uri("/+login")
            .body(axum::body::Body::from(
                r#"{"user":"alice","password":"new"}"#,
            ))
            .unwrap();
        assert_eq!(
            app.clone()
                .oneshot(new_password_login)
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );

        let patch_missing_user = Request::builder()
            .method(Method::PATCH)
            .uri("/missing")
            .body(axum::body::Body::from(r#"{"email":"missing@example.com"}"#))
            .unwrap();
        assert_eq!(
            app.clone()
                .oneshot(patch_missing_user)
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );

        let create_index = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_index).await.unwrap().status(),
            StatusCode::CREATED
        );

        let put_existing_index = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev")
            .body(axum::body::Body::from(r#"{"volatile":false}"#))
            .unwrap();
        let response = app.clone().oneshot(put_existing_index).await.unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert!(
            body_text(response)
                .await
                .contains("index alice/dev already exists")
        );

        let patch_missing_index = Request::builder()
            .method(Method::PATCH)
            .uri("/alice/missing")
            .header(header::AUTHORIZATION, basic_auth("alice", "old"))
            .body(axum::body::Body::from(r#"{"bases":[]}"#))
            .unwrap();
        assert_eq!(
            app.clone()
                .oneshot(patch_missing_index)
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );

        let delete_index = Request::builder()
            .method(Method::DELETE)
            .uri("/alice/dev")
            .header(header::AUTHORIZATION, basic_auth("alice", "old"))
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.oneshot(delete_index).await.unwrap().status(),
            StatusCode::CREATED
        );
    }

    #[tokio::test]
    async fn root_lists_users_and_indexes() {
        let package_dir =
            std::env::temp_dir().join(format!("devpi-rs-root-users-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: Vec::new(),
            outside_url: None,
        });
        let create_index = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev")
            .body(axum::body::Body::from(r#"{"bases":["root/pypi"]}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_index).await.unwrap().status(),
            StatusCode::CREATED
        );
        let get_root = Request::builder()
            .method(Method::GET)
            .uri("/")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(get_root).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "1");
        let body = body_text(response).await;
        assert!(body.contains("\"type\":\"list:userconfig\""));
        assert!(body.contains("\"alice\""));
        assert!(body.contains("\"root\""));
        assert!(body.contains("\"pypi\""));
    }

    #[tokio::test]
    async fn deleting_user_with_non_volatile_index_is_forbidden() {
        let package_dir =
            std::env::temp_dir().join(format!("devpi-rs-delete-user-prod-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: Vec::new(),
            outside_url: None,
        });
        let create_user = Request::builder()
            .method(Method::PUT)
            .uri("/alice")
            .body(axum::body::Body::from(r#"{"password":"123"}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_user).await.unwrap().status(),
            StatusCode::CREATED
        );
        let create_index = Request::builder()
            .method(Method::PUT)
            .uri("/alice/prod")
            .body(axum::body::Body::from(r#"{"volatile":false}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_index).await.unwrap().status(),
            StatusCode::CREATED
        );
        let delete_user = Request::builder()
            .method(Method::DELETE)
            .uri("/alice")
            .header(header::AUTHORIZATION, basic_auth("alice", "123"))
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(delete_user).await.unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn deleting_user_tombstones_release_files() {
        let package_dir = std::env::temp_dir().join(format!(
            "devpi-rs-delete-user-tombstones-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir: package_dir.clone(),
            sources: Vec::new(),
            outside_url: None,
        });
        let create_user = Request::builder()
            .method(Method::PUT)
            .uri("/alice")
            .body(axum::body::Body::from(r#"{"password":"123"}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_user).await.unwrap().status(),
            StatusCode::CREATED
        );
        let create_index = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev")
            .header(header::AUTHORIZATION, basic_auth("alice", "123"))
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_index).await.unwrap().status(),
            StatusCode::CREATED
        );
        std::fs::create_dir_all(package_dir.join("alice/dev/demo")).unwrap();
        std::fs::write(
            package_dir.join("alice/dev/demo/demo-1.0.0.tar.gz"),
            b"sdist",
        )
        .unwrap();

        let delete_user = Request::builder()
            .method(Method::DELETE)
            .uri("/alice")
            .header(header::AUTHORIZATION, basic_auth("alice", "123"))
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(delete_user).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(!package_dir.join("alice").exists());

        let deleted_file = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.oneshot(deleted_file).await.unwrap().status(),
            StatusCode::GONE
        );
    }

    #[tokio::test]
    async fn configured_index_enforces_acl_upload_basic_auth() {
        let package_dir =
            std::env::temp_dir().join(format!("devpi-rs-acl-upload-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: Vec::new(),
            outside_url: None,
        });
        for (user, password) in [("alice", "123"), ("bob", "123")] {
            let create_user = Request::builder()
                .method(Method::PUT)
                .uri(format!("/{user}"))
                .body(axum::body::Body::from(format!(
                    r#"{{"password":"{password}"}}"#
                )))
                .unwrap();
            assert_eq!(
                app.clone().oneshot(create_user).await.unwrap().status(),
                StatusCode::CREATED
            );
        }
        let create_index = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_index).await.unwrap().status(),
            StatusCode::CREATED
        );

        let unauthenticated = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/demo/1.0.0/demo-1.0.0.tar.gz")
            .body(axum::body::Body::from("one"))
            .unwrap();
        let response = app.clone().oneshot(unauthenticated).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.headers().get(header::WWW_AUTHENTICATE).unwrap(),
            "Basic realm=\"devpi-rs\""
        );

        let invalid_basic = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/demo/1.0.0/demo-1.0.0.tar.gz")
            .header(header::AUTHORIZATION, "Basic YWxpY2U6d3Jvbmc=")
            .body(axum::body::Body::from("one"))
            .unwrap();
        let response = app.clone().oneshot(invalid_basic).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.headers().get(header::WWW_AUTHENTICATE).unwrap(),
            "Basic realm=\"devpi-rs\""
        );

        let forbidden = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/demo/1.0.0/demo-1.0.0.tar.gz")
            .header(header::AUTHORIZATION, "Basic Ym9iOjEyMw==")
            .body(axum::body::Body::from("one"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(forbidden).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );

        let allowed = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/demo/1.0.0/demo-1.0.0.tar.gz")
            .header(header::AUTHORIZATION, "Basic YWxpY2U6MTIz")
            .body(axum::body::Body::from("one"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(allowed).await.unwrap().status(),
            StatusCode::CREATED
        );

        let lower_case_scheme = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/demo/1.0.0/demo-1.0.0-py3-none-any.whl")
            .header(header::AUTHORIZATION, "basic YWxpY2U6MTIz")
            .body(axum::body::Body::from("wheel"))
            .unwrap();
        assert_eq!(
            app.oneshot(lower_case_scheme).await.unwrap().status(),
            StatusCode::CREATED
        );
    }

    #[tokio::test]
    async fn acl_upload_authenticated_allows_any_authenticated_user() {
        let package_dir =
            std::env::temp_dir().join(format!("devpi-rs-acl-auth-upload-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: Vec::new(),
            outside_url: None,
        });
        for user in ["alice", "bob"] {
            let create_user = Request::builder()
                .method(Method::PUT)
                .uri(format!("/{user}"))
                .body(axum::body::Body::from(r#"{"password":"123"}"#))
                .unwrap();
            assert_eq!(
                app.clone().oneshot(create_user).await.unwrap().status(),
                StatusCode::CREATED
            );
        }
        let create_index = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev")
            .body(axum::body::Body::from(
                r#"{"acl_upload":[":authenticated:"]}"#,
            ))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_index).await.unwrap().status(),
            StatusCode::CREATED
        );

        let unauthenticated = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/demo/1.0.0/demo-1.0.0.tar.gz")
            .body(axum::body::Body::from("sdist"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(unauthenticated).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );

        let authenticated = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/demo/1.0.0/demo-1.0.0.tar.gz")
            .header(header::AUTHORIZATION, "Basic Ym9iOjEyMw==")
            .body(axum::body::Body::from("sdist"))
            .unwrap();
        assert_eq!(
            app.oneshot(authenticated).await.unwrap().status(),
            StatusCode::CREATED
        );
    }

    #[tokio::test]
    async fn configured_index_enforces_acl_pkg_read() {
        let package_dir =
            std::env::temp_dir().join(format!("devpi-rs-acl-pkg-read-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: Vec::new(),
            outside_url: None,
        });
        for (user, password) in [("alice", "alice-password"), ("bob", "bob-password")] {
            let create_user = Request::builder()
                .method(Method::PUT)
                .uri(format!("/{user}"))
                .body(axum::body::Body::from(format!(
                    r#"{{"password":"{password}"}}"#
                )))
                .unwrap();
            assert_eq!(
                app.clone().oneshot(create_user).await.unwrap().status(),
                StatusCode::CREATED
            );
        }
        let create_index = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev")
            .body(axum::body::Body::from(r#"{"acl_pkg_read":["alice"]}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_index).await.unwrap().status(),
            StatusCode::CREATED
        );
        let upload = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .header(header::AUTHORIZATION, basic_auth("alice", "alice-password"))
            .body(axum::body::Body::from("sdist"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(upload).await.unwrap().status(),
            StatusCode::CREATED
        );

        for uri in [
            "/alice/dev/+simple/demo/",
            "/alice/dev/+f/demo/demo-1.0.0.tar.gz",
            "/alice/dev/+f/demo/demo-1.0.0.tar.gz.metadata",
            "/alice/dev/demo",
            "/alice/dev/demo/1.0.0",
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(Method::GET)
                        .uri(uri)
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN, "{uri}");
            assert!(body_text(response).await.contains("package read forbidden"));
        }

        let bob = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+simple/demo/")
            .header(header::AUTHORIZATION, basic_auth("bob", "bob-password"))
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(bob).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );

        let alice = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+simple/demo/")
            .header(header::AUTHORIZATION, basic_auth("alice", "alice-password"))
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(alice).await.unwrap().status(),
            StatusCode::OK
        );

        let root = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .header(header::AUTHORIZATION, basic_auth("root", ""))
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(app.oneshot(root).await.unwrap().status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn creates_reads_and_deletes_index_config() {
        let package_dir =
            std::env::temp_dir().join(format!("devpi-rs-index-config-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir: package_dir.clone(),
            sources: vec![
                SourceConfig {
                    name: "corp".to_string(),
                    simple_url: "http://corp/simple/".to_string(),
                },
                SourceConfig {
                    name: "pypi".to_string(),
                    simple_url: "https://pypi.org/simple/".to_string(),
                },
            ],
            outside_url: None,
        });
        let create_user = Request::builder()
            .method(Method::PUT)
            .uri("/alice")
            .body(axum::body::Body::from(r#"{"password":"123"}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_user).await.unwrap().status(),
            StatusCode::CREATED
        );
        let put = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev")
            .body(axum::body::Body::from(r#"{"bases":["root/pypi"]}"#))
            .unwrap();

        let response = app.clone().oneshot(put).await.unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let body = body_text(response).await;
        assert!(body.contains("\"type\":\"indexconfig\""));
        assert!(body.contains("\"bases\":[\"root/pypi\"]"));

        std::fs::create_dir_all(package_dir.join("alice/dev/demo")).unwrap();
        std::fs::write(
            package_dir.join("alice/dev/demo/demo-1.0.0.tar.gz"),
            b"sdist",
        )
        .unwrap();
        let get = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(get).await.unwrap();
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "2");
        let body = body_text(response).await;
        assert!(body.contains("\"projects\":[\"demo\"]"));
        assert!(body.contains("\"resolved_sources\":[\"corp\",\"pypi\"]"));

        let get_no_projects = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev?no_projects")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(get_no_projects).await.unwrap();
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "2");
        let body = body_text(response).await;
        assert!(!body.contains("\"projects\""));

        let get_stage = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(get_stage).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "2");
        assert!(body_text(response).await.contains("\"type\":\"stage\""));

        let get_stage_no_projects = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/?no_projects")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(get_stage_no_projects).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_text(response).await;
        assert!(body.contains("\"type\":\"stage\""));
        assert!(!body.contains("\"projects\""));

        let delete = Request::builder()
            .method(Method::DELETE)
            .uri("/alice/dev")
            .header(header::AUTHORIZATION, basic_auth("alice", "123"))
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(delete).await.unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["status"], 201);
        assert!(!package_dir.join("alice/dev").exists());
        let deleted_file = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.oneshot(deleted_file).await.unwrap().status(),
            StatusCode::GONE
        );
    }

    #[tokio::test]
    async fn authenticated_index_creation_requires_owner_or_root() {
        let package_dir =
            std::env::temp_dir().join(format!("devpi-rs-index-create-auth-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: Vec::new(),
            outside_url: None,
        });
        for (user, password) in [("alice", "123"), ("bob", "456")] {
            let create_user = Request::builder()
                .method(Method::PUT)
                .uri(format!("/{user}"))
                .body(axum::body::Body::from(format!(
                    r#"{{"password":"{password}"}}"#
                )))
                .unwrap();
            assert_eq!(
                app.clone().oneshot(create_user).await.unwrap().status(),
                StatusCode::CREATED
            );
        }

        let bob_creates_alice_index = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev")
            .header(header::AUTHORIZATION, basic_auth("bob", "456"))
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.clone()
                .oneshot(bob_creates_alice_index)
                .await
                .unwrap()
                .status(),
            StatusCode::FORBIDDEN
        );

        let owner_creates_index = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev")
            .header(header::AUTHORIZATION, basic_auth("alice", "123"))
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.clone()
                .oneshot(owner_creates_index)
                .await
                .unwrap()
                .status(),
            StatusCode::CREATED
        );

        let root_creates_index = Request::builder()
            .method(Method::PUT)
            .uri("/alice/rooted")
            .header(header::AUTHORIZATION, basic_auth("root", ""))
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.clone()
                .oneshot(root_creates_index)
                .await
                .unwrap()
                .status(),
            StatusCode::CREATED
        );

        let anonymous_create_remains_open = Request::builder()
            .method(Method::PUT)
            .uri("/alice/anonymous")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.oneshot(anonymous_create_remains_open)
                .await
                .unwrap()
                .status(),
            StatusCode::CREATED
        );
    }

    #[tokio::test]
    async fn index_modify_and_delete_require_owner_or_root() {
        let package_dir =
            std::env::temp_dir().join(format!("devpi-rs-index-owner-acl-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: Vec::new(),
            outside_url: None,
        });
        for (user, password) in [("alice", "123"), ("bob", "456")] {
            let create_user = Request::builder()
                .method(Method::PUT)
                .uri(format!("/{user}"))
                .body(axum::body::Body::from(format!(
                    r#"{{"password":"{password}"}}"#
                )))
                .unwrap();
            assert_eq!(
                app.clone().oneshot(create_user).await.unwrap().status(),
                StatusCode::CREATED
            );
        }

        let create_index = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev")
            .body(axum::body::Body::from(r#"{"acl_upload":["bob"]}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_index).await.unwrap().status(),
            StatusCode::CREATED
        );
        let create_public_index = Request::builder()
            .method(Method::PUT)
            .uri("/alice/public")
            .body(axum::body::Body::from(r#"{"acl_upload":[":ANONYMOUS:"]}"#))
            .unwrap();
        assert_eq!(
            app.clone()
                .oneshot(create_public_index)
                .await
                .unwrap()
                .status(),
            StatusCode::CREATED
        );

        let anonymous_patch = Request::builder()
            .method(Method::PATCH)
            .uri("/alice/public")
            .body(axum::body::Body::from(r#"{"acl_upload":[":ANONYMOUS:"]}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(anonymous_patch).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );

        let bob_patch = Request::builder()
            .method(Method::PATCH)
            .uri("/alice/dev")
            .header(header::AUTHORIZATION, basic_auth("bob", "456"))
            .body(axum::body::Body::from(r#"{"acl_upload":["bob"]}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(bob_patch).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );

        let alice_patch = Request::builder()
            .method(Method::PATCH)
            .uri("/alice/dev")
            .header(header::AUTHORIZATION, basic_auth("alice", "123"))
            .body(axum::body::Body::from(r#"{"acl_upload":["bob"]}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(alice_patch).await.unwrap().status(),
            StatusCode::OK
        );

        let root_patch = Request::builder()
            .method(Method::PATCH)
            .uri("/alice/public")
            .header(header::AUTHORIZATION, basic_auth("root", ""))
            .body(axum::body::Body::from(r#"{"acl_upload":[":ANONYMOUS:"]}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(root_patch).await.unwrap().status(),
            StatusCode::OK
        );

        let bob_delete = Request::builder()
            .method(Method::DELETE)
            .uri("/alice/dev")
            .header(header::AUTHORIZATION, basic_auth("bob", "456"))
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(bob_delete).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );

        let alice_delete = Request::builder()
            .method(Method::DELETE)
            .uri("/alice/dev")
            .header(header::AUTHORIZATION, basic_auth("alice", "123"))
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.oneshot(alice_delete).await.unwrap().status(),
            StatusCode::CREATED
        );
    }

    #[tokio::test]
    async fn registers_project_without_uploading_files() {
        let package_dir =
            std::env::temp_dir().join(format!("devpi-rs-register-project-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: Vec::new(),
            outside_url: None,
        });
        let create_user = Request::builder()
            .method(Method::PUT)
            .uri("/alice")
            .body(axum::body::Body::from(r#"{"password":"123"}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_user).await.unwrap().status(),
            StatusCode::CREATED
        );
        let create_index = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev")
            .header(header::AUTHORIZATION, basic_auth("alice", "123"))
            .body(axum::body::Body::from(r#"{"acl_upload":[":ANONYMOUS:"]}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_index).await.unwrap().status(),
            StatusCode::CREATED
        );
        let put = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/Demo_Pkg")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.clone().oneshot(put).await.unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let get_project = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/demo-pkg")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(get_project).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(body_text(response).await.contains("\"files\":[]"));

        let put_slash = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/Other_Pkg/")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(put_slash).await.unwrap().status(),
            StatusCode::CREATED
        );
        let get_project_slash = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/other-pkg/")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(get_project_slash).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            body_text(response)
                .await
                .contains("\"project\":\"other-pkg\"")
        );

        let get_stage = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(get_stage).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = body_text(response).await;
        assert!(body.contains("\"demo-pkg\""));
        assert!(body.contains("\"other-pkg\""));
    }

    #[tokio::test]
    async fn non_volatile_index_rejects_file_overwrite() {
        let package_dir =
            std::env::temp_dir().join(format!("devpi-rs-non-volatile-put-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: Vec::new(),
            outside_url: None,
        });
        let create_user = Request::builder()
            .method(Method::PUT)
            .uri("/alice")
            .body(axum::body::Body::from(r#"{"password":"123"}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_user).await.unwrap().status(),
            StatusCode::CREATED
        );
        let create_index = Request::builder()
            .method(Method::PUT)
            .uri("/alice/prod")
            .body(axum::body::Body::from(r#"{"volatile":false}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_index).await.unwrap().status(),
            StatusCode::CREATED
        );
        let first_put = Request::builder()
            .method(Method::PUT)
            .uri("/alice/prod/demo/1.0.0/demo-1.0.0.tar.gz")
            .header(header::AUTHORIZATION, "Basic YWxpY2U6MTIz")
            .body(axum::body::Body::from("one"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(first_put).await.unwrap().status(),
            StatusCode::CREATED
        );
        let identical = Request::builder()
            .method(Method::PUT)
            .uri("/alice/prod/demo/1.0.0/demo-1.0.0.tar.gz")
            .header(header::AUTHORIZATION, "Basic YWxpY2U6MTIz")
            .body(axum::body::Body::from("one"))
            .unwrap();
        let response = app.clone().oneshot(identical).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["status"], 200);
        assert_eq!(body["identical"], true);

        let overwrite = Request::builder()
            .method(Method::PUT)
            .uri("/alice/prod/demo/1.0.0/demo-1.0.0.tar.gz")
            .header(header::AUTHORIZATION, "Basic YWxpY2U6MTIz")
            .body(axum::body::Body::from("two"))
            .unwrap();

        let response = app.oneshot(overwrite).await.unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn non_volatile_index_rejects_project_and_version_delete() {
        let package_dir = std::env::temp_dir().join(format!(
            "devpi-rs-non-volatile-delete-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: Vec::new(),
            outside_url: None,
        });
        let create_user = Request::builder()
            .method(Method::PUT)
            .uri("/alice")
            .body(axum::body::Body::from(r#"{"password":"123"}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_user).await.unwrap().status(),
            StatusCode::CREATED
        );
        let create_index = Request::builder()
            .method(Method::PUT)
            .uri("/alice/prod")
            .body(axum::body::Body::from(r#"{"volatile":false}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_index).await.unwrap().status(),
            StatusCode::CREATED
        );
        let put = Request::builder()
            .method(Method::PUT)
            .uri("/alice/prod/demo/1.0.0/demo-1.0.0.tar.gz")
            .header(header::AUTHORIZATION, "Basic YWxpY2U6MTIz")
            .body(axum::body::Body::from("one"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(put).await.unwrap().status(),
            StatusCode::CREATED
        );
        let put_file = Request::builder()
            .method(Method::PUT)
            .uri("/alice/prod/fileonly/1.0.0/fileonly-1.0.0.tar.gz")
            .header(header::AUTHORIZATION, "Basic YWxpY2U6MTIz")
            .body(axum::body::Body::from("two"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(put_file).await.unwrap().status(),
            StatusCode::CREATED
        );
        let delete_version = Request::builder()
            .method(Method::DELETE)
            .uri("/alice/prod/demo/1.0.0")
            .header(header::AUTHORIZATION, "Basic YWxpY2U6MTIz")
            .body(axum::body::Body::empty())
            .unwrap();
        let delete_project = Request::builder()
            .method(Method::DELETE)
            .uri("/alice/prod/demo")
            .header(header::AUTHORIZATION, "Basic YWxpY2U6MTIz")
            .body(axum::body::Body::empty())
            .unwrap();

        assert_eq!(
            app.clone().oneshot(delete_version).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            app.clone().oneshot(delete_project).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );

        let force_file = Request::builder()
            .method(Method::DELETE)
            .uri("/alice/prod/+f/fileonly/fileonly-1.0.0.tar.gz?force")
            .header(header::AUTHORIZATION, "Basic YWxpY2U6MTIz")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(force_file).await.unwrap().status(),
            StatusCode::OK
        );

        let force_version = Request::builder()
            .method(Method::DELETE)
            .uri("/alice/prod/demo/1.0.0?force")
            .header(header::AUTHORIZATION, "Basic YWxpY2U6MTIz")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(force_version).await.unwrap().status(),
            StatusCode::OK
        );
        let put_again = Request::builder()
            .method(Method::PUT)
            .uri("/alice/prod/demo/1.0.0/demo-1.0.0.tar.gz")
            .header(header::AUTHORIZATION, "Basic YWxpY2U6MTIz")
            .body(axum::body::Body::from("one"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(put_again).await.unwrap().status(),
            StatusCode::CREATED
        );
        let force_project = Request::builder()
            .method(Method::DELETE)
            .uri("/alice/prod/demo?force")
            .header(header::AUTHORIZATION, "Basic YWxpY2U6MTIz")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.oneshot(force_project).await.unwrap().status(),
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn stage_index_json_uses_persisted_index_config() {
        let state = test_state("stage-config");
        state
            .registry
            .put_index(
                "alice",
                "dev",
                IndexInput {
                    bases: Some(vec!["root/pypi".to_string()]),
                    volatile: Some(false),
                    index_type: None,
                    acl_upload: Some(vec!["alice".to_string(), "bob".to_string()]),
                    acl_pkg_read: None,
                    acl_toxresult_upload: None,
                    sources: Some(vec!["corp".to_string(), "pypi".to_string()]),
                    mirror_whitelist: Some(vec!["Demo_Project".to_string()]),
                    mirror_whitelist_inheritance: Some("union".to_string()),
                    title: Some("Alice Dev".to_string()),
                    description: Some("Private stage".to_string()),
                    custom_data: Some(json!({"team": "platform"})),
                    mirror_url: Some("https://example.com/simple/".to_string()),
                    mirror_web_url_fmt: Some("https://example.com/project/{name}/".to_string()),
                    mirror_cache_expiry: Some(900),
                    mirror_ignore_serial_header: Some(true),
                    mirror_no_project_list: Some(true),
                    mirror_provides_core_metadata: Some(true),
                    mirror_use_external_urls: Some(true),
                    extra: BTreeMap::from([("notify".to_string(), json!(true))]),
                    ..Default::default()
                },
            )
            .unwrap();

        let response =
            render_stage_index_with_projects(state, "alice".to_string(), "dev".to_string(), true)
                .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = body_text(response).await;
        assert!(body.contains("\"bases\":[\"root/pypi\"]"));
        assert!(body.contains("\"volatile\":false"));
        assert!(body.contains("\"acl_upload\":[\"alice\",\"bob\"]"));
        assert!(body.contains("\"acl_pkg_read\":[\":ANONYMOUS:\"]"));
        assert!(body.contains("\"sources\":[\"corp\",\"pypi\"]"));
        assert!(body.contains("\"mirror_whitelist\":[\"demo-project\"]"));
        assert!(body.contains("\"mirror_whitelist_inheritance\":\"union\""));
        assert!(body.contains("\"mirror_use_external_urls\":true"));
        assert!(body.contains("\"title\":\"Alice Dev\""));
        assert!(body.contains("\"description\":\"Private stage\""));
        assert!(body.contains("\"custom_data\":{\"team\":\"platform\"}"));
        assert!(body.contains("\"mirror_url\":\"https://example.com/simple/\""));
        assert!(body.contains("\"mirror_web_url_fmt\":\"https://example.com/project/{name}/\""));
        assert!(body.contains("\"mirror_cache_expiry\":900"));
        assert!(body.contains("\"mirror_ignore_serial_header\":true"));
        assert!(body.contains("\"mirror_no_project_list\":true"));
        assert!(body.contains("\"mirror_provides_core_metadata\":true"));
        assert!(body.contains("\"notify\":true"));
    }

    #[tokio::test]
    async fn stage_simple_project_includes_local_base_index_files() {
        let state = test_state("base-project");
        state
            .registry
            .put_index(
                "alice",
                "dev",
                IndexInput {
                    bases: Some(vec!["root/pypi".to_string()]),
                    volatile: None,
                    index_type: None,
                    acl_upload: None,
                    acl_pkg_read: None,
                    acl_toxresult_upload: None,
                    sources: None,
                    ..Default::default()
                },
            )
            .unwrap();
        state
            .local
            .save_in("root", "pypi", "basepkg", "basepkg-1.0.0.tar.gz", b"sdist")
            .unwrap();

        let response = render_simple_project_with_format(
            state.clone(),
            "alice".to_string(),
            "dev".to_string(),
            "basepkg".to_string(),
            SimpleFormat::Html,
            None,
            false,
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            body_text(response)
                .await
                .contains("../../+f/basepkg/basepkg-1.0.0.tar.gz")
        );

        let response = read_file(
            state.clone(),
            "alice".to_string(),
            "dev".to_string(),
            "basepkg".to_string(),
            "basepkg-1.0.0.tar.gz".to_string(),
            FileReadOptions::default(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&bytes[..], b"sdist");

        let response = read_file(
            state,
            "alice".to_string(),
            "dev".to_string(),
            "basepkg".to_string(),
            "basepkg-1.0.0.tar.gz".to_string(),
            FileReadOptions {
                json_preferred: true,
                ..FileReadOptions::default()
            },
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["type"], "releasefilemeta");
        assert_eq!(
            body["result"]["url"],
            "/root/pypi/+f/basepkg/basepkg-1.0.0.tar.gz"
        );
        assert_eq!(body["result"]["bytes"], 5);
        assert_eq!(
            body["result"]["hash_spec"],
            "sha256=714772a9f82b2aeb4fa5f7092d00fe4ac4c9cdeb6800840b6ed39ea64c4d785a"
        );
        assert_eq!(
            body["result"]["hashes"]["sha256"],
            "714772a9f82b2aeb4fa5f7092d00fe4ac4c9cdeb6800840b6ed39ea64c4d785a"
        );
    }

    #[tokio::test]
    async fn stage_reads_skip_unreadable_base_indexes() {
        let state = test_state("base-pkg-read");
        state
            .registry
            .put_index(
                "team",
                "prod",
                IndexInput {
                    bases: None,
                    volatile: None,
                    index_type: None,
                    acl_upload: None,
                    acl_pkg_read: Some(vec!["team".to_string()]),
                    acl_toxresult_upload: None,
                    sources: None,
                    ..Default::default()
                },
            )
            .unwrap();
        state
            .registry
            .put_index(
                "alice",
                "dev",
                IndexInput {
                    bases: Some(vec!["team/prod".to_string()]),
                    volatile: None,
                    index_type: None,
                    acl_upload: None,
                    acl_pkg_read: None,
                    acl_toxresult_upload: None,
                    sources: None,
                    ..Default::default()
                },
            )
            .unwrap();
        state
            .local
            .save_in("team", "prod", "basepkg", "basepkg-1.0.0.tar.gz", b"sdist")
            .unwrap();

        let simple = render_simple_project_with_format(
            state.clone(),
            "alice".to_string(),
            "dev".to_string(),
            "basepkg".to_string(),
            SimpleFormat::Html,
            None,
            false,
        )
        .await;
        assert_eq!(simple.status(), StatusCode::NOT_FOUND);

        let file = read_file(
            state.clone(),
            "alice".to_string(),
            "dev".to_string(),
            "basepkg".to_string(),
            "basepkg-1.0.0.tar.gz".to_string(),
            FileReadOptions::default(),
        )
        .await;
        assert_eq!(file.status(), StatusCode::NOT_FOUND);

        let simple = render_simple_project_with_format(
            state.clone(),
            "alice".to_string(),
            "dev".to_string(),
            "basepkg".to_string(),
            SimpleFormat::Html,
            Some("team".to_string()),
            false,
        )
        .await;
        assert_eq!(simple.status(), StatusCode::OK);
        assert!(
            body_text(simple)
                .await
                .contains("../../+f/basepkg/basepkg-1.0.0.tar.gz")
        );

        let file = read_file(
            state,
            "alice".to_string(),
            "dev".to_string(),
            "basepkg".to_string(),
            "basepkg-1.0.0.tar.gz".to_string(),
            FileReadOptions {
                auth_user: Some("team".to_string()),
                ..FileReadOptions::default()
            },
        )
        .await;
        assert_eq!(file.status(), StatusCode::OK);
        let bytes = to_bytes(file.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&bytes[..], b"sdist");
    }

    #[tokio::test]
    async fn stage_simple_root_includes_recursive_base_projects() {
        let state = test_state("base-root");
        state
            .registry
            .put_index(
                "alice",
                "dev",
                IndexInput {
                    bases: Some(vec!["team/prod".to_string()]),
                    volatile: None,
                    index_type: None,
                    acl_upload: None,
                    acl_pkg_read: None,
                    acl_toxresult_upload: None,
                    sources: None,
                    ..Default::default()
                },
            )
            .unwrap();
        state
            .registry
            .put_index(
                "team",
                "prod",
                IndexInput {
                    bases: Some(vec!["root/pypi".to_string()]),
                    volatile: None,
                    index_type: None,
                    acl_upload: None,
                    acl_pkg_read: None,
                    acl_toxresult_upload: None,
                    sources: None,
                    ..Default::default()
                },
            )
            .unwrap();
        state
            .local
            .save_in("root", "pypi", "basepkg", "basepkg-1.0.0.tar.gz", b"sdist")
            .unwrap();

        let response = render_simple_root_with_format(
            state,
            "alice".to_string(),
            "dev".to_string(),
            SimpleFormat::Html,
            None,
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = body_text(response).await;
        assert!(body.contains("basepkg"));
        assert!(body.contains(r#"href="basepkg/""#));
        assert!(!body.contains(r#"href="/simple/basepkg/""#));
    }

    #[tokio::test]
    async fn renders_stage_project_json_with_files() {
        let state = test_state("stage-project");
        state
            .registry
            .put_index("alice", "dev", IndexInput::default())
            .unwrap();
        state
            .local
            .save_in("alice", "dev", "demo", "demo-1.0.0.tar.gz", b"sdist")
            .unwrap();

        let response = render_stage_project(
            state,
            "alice".to_string(),
            "dev".to_string(),
            "demo".to_string(),
            true,
            None,
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body = body_text(response).await;
        assert!(body.contains("\"status\":200"));
        assert!(body.contains("\"type\":\"projectconfig\""));
        assert!(body.contains("\"project\":\"demo\""));
        assert!(body.contains("\"filename\":\"demo-1.0.0.tar.gz\""));
        assert!(body.contains("\"url\":\"/alice/dev/+f/demo/demo-1.0.0.tar.gz\""));
        assert!(body.contains("\"version\":\"1.0.0\""));
        assert!(body.contains("\"versions\":{\"1.0.0\":{}}"));
    }

    #[tokio::test]
    async fn stage_project_json_infers_version_from_normalized_filename_prefix() {
        let state = test_state("stage-project-normalized-version");
        state
            .registry
            .put_index("alice", "dev", IndexInput::default())
            .unwrap();
        state
            .local
            .save_in(
                "alice",
                "dev",
                "demo-pkg",
                "demo_pkg-1.0.0-py3-none-any.whl",
                b"wheel",
            )
            .unwrap();

        let response = render_stage_project(
            state,
            "alice".to_string(),
            "dev".to_string(),
            "demo-pkg".to_string(),
            true,
            None,
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(
            body["result"]["files"][0]["filename"],
            "demo_pkg-1.0.0-py3-none-any.whl"
        );
        assert_eq!(body["result"]["files"][0]["version"], "1.0.0");
    }

    #[tokio::test]
    async fn stage_project_returns_not_found_for_unknown_index() {
        let state = test_state("stage-project-index-404");

        let response = render_stage_project(
            state,
            "alice".to_string(),
            "dev".to_string(),
            "demo".to_string(),
            true,
            None,
        )
        .await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert!(body_text(response).await.contains("index not found"));
    }

    #[tokio::test]
    async fn stage_file_download_returns_not_found_for_unknown_index() {
        let state = test_state("stage-file-index-404");
        state
            .local
            .save_in("alice", "dev", "demo", "demo-1.0.0.tar.gz", b"sdist")
            .unwrap();

        let response = read_file(
            state,
            "alice".to_string(),
            "dev".to_string(),
            "demo".to_string(),
            "demo-1.0.0.tar.gz".to_string(),
            FileReadOptions::default(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert!(body_text(response).await.contains("index not found"));
    }

    #[tokio::test]
    async fn stage_file_upload_returns_not_found_for_unknown_index() {
        let state = test_state("stage-file-upload-index-404");

        let upload = save_file(
            state.clone(),
            SaveFileRequest {
                user: "alice".to_string(),
                index: "dev".to_string(),
                project: "demo".to_string(),
                filename: "demo-1.0.0.tar.gz".to_string(),
                body: Bytes::from_static(b"sdist"),
                metadata: BTreeMap::new(),
                auth_user: None,
            },
        )
        .await;
        assert_eq!(upload.status(), StatusCode::NOT_FOUND);
        assert!(body_text(upload).await.contains("index not found"));

        let register = register_version_metadata(
            state,
            "alice".to_string(),
            "dev".to_string(),
            "demo".to_string(),
            "1.0.0".to_string(),
            BTreeMap::new(),
            None,
        )
        .await;
        assert_eq!(register.status(), StatusCode::NOT_FOUND);
        assert!(body_text(register).await.contains("index not found"));
    }

    #[tokio::test]
    async fn stage_delete_mutations_return_not_found_for_unknown_index() {
        let state = test_state("stage-delete-index-404");

        let project = remove_project(
            state.clone(),
            "alice".to_string(),
            "dev".to_string(),
            "demo".to_string(),
            None,
            false,
        )
        .await;
        assert_eq!(project.status(), StatusCode::NOT_FOUND);
        assert!(body_text(project).await.contains("index not found"));

        let version = remove_version(
            state.clone(),
            "alice".to_string(),
            "dev".to_string(),
            "demo".to_string(),
            "1.0.0".to_string(),
            None,
            false,
        )
        .await;
        assert_eq!(version.status(), StatusCode::NOT_FOUND);
        assert!(body_text(version).await.contains("index not found"));

        let file = delete_file(
            state.clone(),
            "alice".to_string(),
            "dev".to_string(),
            "demo".to_string(),
            "demo-1.0.0.tar.gz".to_string(),
            None,
            false,
        )
        .await;
        assert_eq!(file.status(), StatusCode::NOT_FOUND);
        assert!(body_text(file).await.contains("index not found"));

        let tox = store_tox_result(
            state,
            StoreToxResultRequest {
                user: "alice".to_string(),
                index: "dev".to_string(),
                project: "demo".to_string(),
                filename: "demo-1.0.0.tar.gz".to_string(),
                body: Bytes::from_static(br#"{"envname":"py312","retcode":0}"#),
                auth_user: None,
                outside_url: None,
            },
        )
        .await;
        assert_eq!(tox.status(), StatusCode::NOT_FOUND);
        assert!(body_text(tox).await.contains("index not found"));
    }

    #[tokio::test]
    async fn accepts_multipart_file_upload_on_stage_index() {
        let (app, package_dir) = test_router_with_index("multipart", "alice", "dev");
        let boundary = "BOUNDARY";
        let body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\":action\"\r\n\r\nfile_upload\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"name\"\r\n\r\ndemo\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"version\"\r\n\r\n1.0.0\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"summary\"\r\n\r\nDemo package\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"content\"; filename=\"demo-1.0.0.tar.gz\"\r\nContent-Type: application/octet-stream\r\n\r\nsdist\r\n\
             --{boundary}--\r\n"
        );
        let request = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(body))
            .unwrap();

        let response = app.clone().oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let upload_serial = response.headers().get("X-DEVPI-SERIAL").unwrap().clone();
        assert_eq!(
            std::fs::read(package_dir.join("alice/dev/demo/demo-1.0.0.tar.gz")).unwrap(),
            b"sdist"
        );
        let project = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/demo")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(project).await.unwrap();
        assert_eq!(
            response.headers().get("X-DEVPI-SERIAL").unwrap(),
            &upload_serial
        );
        let body = body_text(response).await;
        assert!(body.contains("\"metadata\""));
        assert!(body.contains("\"summary\":\"Demo package\""));
    }

    #[tokio::test]
    async fn version_metadata_includes_release_file_links() {
        let (app, _package_dir) = test_router_with_index("version-links", "alice", "dev");
        let boundary = "BOUNDARY";
        let submit = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\":action\"\r\n\r\nsubmit\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"name\"\r\n\r\ndemo\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"version\"\r\n\r\n1.0.0\r\n\
             --{boundary}--\r\n"
        );
        let request = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(submit))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(request).await.unwrap().status(),
            StatusCode::OK
        );
        let upload = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::from("sdist"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(upload).await.unwrap().status(),
            StatusCode::CREATED
        );
        let tox = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::from(r#"{"envname":"py312","retcode":0}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(tox).await.unwrap().status(),
            StatusCode::OK
        );
        let version = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/demo/1.0.0")
            .header(header::ACCEPT, "application/json")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(version).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        let links = body["result"]["+links"].as_array().unwrap();
        assert_eq!(links.len(), 2);
        assert_eq!(
            links[0],
            json!({
                "rel": "releasefile",
                "href": "/alice/dev/+f/demo/demo-1.0.0.tar.gz",
                "basename": "demo-1.0.0.tar.gz",
                "log": links[0]["log"].clone(),
            })
        );
        assert_upload_log(&links[0]["log"], None, "alice/dev");
        assert_eq!(
            links[1],
            json!({
                "rel": "toxresult",
                "href": "/alice/dev/+f/demo/demo-1.0.0.tar.gz.toxresult-0",
                "for_href": "/alice/dev/+f/demo/demo-1.0.0.tar.gz",
                "log": links[1]["log"].clone(),
            })
        );
        assert_upload_log(&links[1]["log"], None, "alice/dev");
    }

    #[tokio::test]
    async fn version_metadata_records_authenticated_upload_user_in_log() {
        let state = test_state("version-auth-log");
        state
            .registry
            .put_user(
                "alice",
                UserInput {
                    email: None,
                    password: Some("123".to_string()),
                },
            )
            .unwrap();
        state
            .registry
            .put_index(
                "alice",
                "dev",
                IndexInput {
                    acl_upload: Some(vec!["alice".to_string()]),
                    ..IndexInput::default()
                },
            )
            .unwrap();
        let app = router_from_state(state);
        let upload = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .header(header::AUTHORIZATION, basic_auth("alice", "123"))
            .body(axum::body::Body::from("sdist"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(upload).await.unwrap().status(),
            StatusCode::CREATED
        );
        let version = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/demo/1.0.0")
            .header(header::ACCEPT, "application/json")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(version).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        let links = body["result"]["+links"].as_array().unwrap();
        assert_eq!(links.len(), 1);
        assert_upload_log(&links[0]["log"], Some("alice"), "alice/dev");
    }

    #[tokio::test]
    async fn version_metadata_records_overwrite_count_in_file_log() {
        let (app, _package_dir) = test_router_with_index("version-overwrite-log", "alice", "dev");
        for body in ["one", "two", "three"] {
            let upload = Request::builder()
                .method(Method::PUT)
                .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
                .body(axum::body::Body::from(body))
                .unwrap();
            assert_eq!(
                app.clone().oneshot(upload).await.unwrap().status(),
                StatusCode::CREATED
            );
        }
        let version = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/demo/1.0.0")
            .header(header::ACCEPT, "application/json")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(version).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        let links = body["result"]["+links"].as_array().unwrap();
        assert_eq!(links.len(), 1);
        let log = links[0]["log"].as_array().unwrap();
        assert_eq!(log.len(), 2);
        assert_eq!(log[0]["what"], "overwrite");
        assert!(log[0]["who"].is_null());
        assert_eq!(log[0]["count"], 2);
        assert_eq!(log[0]["when"].as_array().unwrap().len(), 6);
        assert_eq!(log[1]["what"], "upload");
        assert_eq!(log[1]["dst"], "alice/dev");
        assert!(log[1]["who"].is_null());
        assert_eq!(log[1]["when"].as_array().unwrap().len(), 6);
    }

    #[tokio::test]
    async fn version_metadata_exists_for_file_only_upload() {
        let (app, _package_dir) = test_router_with_index("version-file-only", "alice", "dev");
        let upload = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::from("sdist"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(upload).await.unwrap().status(),
            StatusCode::CREATED
        );
        let version = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/demo/1.0.0")
            .header(header::ACCEPT, "application/json")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(version).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["type"], "versiondata");
        assert_eq!(body["result"]["name"], "demo");
        assert_eq!(body["result"]["version"], "1.0.0");
        let links = body["result"]["+links"].as_array().unwrap();
        assert_eq!(links.len(), 1);
        assert_eq!(
            links[0],
            json!({
                "rel": "releasefile",
                "href": "/alice/dev/+f/demo/demo-1.0.0.tar.gz",
                "basename": "demo-1.0.0.tar.gz",
                "log": links[0]["log"].clone(),
            })
        );
        assert_upload_log(&links[0]["log"], None, "alice/dev");
    }

    #[tokio::test]
    async fn version_metadata_marks_doczip_links() {
        let (app, _package_dir) = test_router_with_index("version-doczip-links", "alice", "dev");
        let boundary = "BOUNDARY";
        let submit = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\":action\"\r\n\r\nsubmit\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"name\"\r\n\r\ndemo\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"version\"\r\n\r\n1.0.0\r\n\
             --{boundary}--\r\n"
        );
        let request = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(submit))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(request).await.unwrap().status(),
            StatusCode::OK
        );
        let doc = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\":action\"\r\n\r\ndoc_upload\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"name\"\r\n\r\ndemo\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"version\"\r\n\r\n1.0.0\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"content\"; filename=\"docs.zip\"\r\nContent-Type: application/zip\r\n\r\ndocs\r\n\
             --{boundary}--\r\n"
        );
        let request = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(doc))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(request).await.unwrap().status(),
            StatusCode::CREATED
        );
        let version = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/demo/1.0.0")
            .header(header::ACCEPT, "application/json")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(version).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        let links = body["result"]["+links"].as_array().unwrap();
        assert_eq!(links.len(), 1);
        assert_eq!(
            links[0],
            json!({
                "rel": "doczip",
                "href": "/alice/dev/+f/demo/demo-1.0.0.doc.zip",
                "basename": "demo-1.0.0.doc.zip",
                "log": links[0]["log"].clone(),
            })
        );
        assert_upload_log(&links[0]["log"], None, "alice/dev");
    }

    #[tokio::test]
    async fn rejects_invalid_multipart_release_filenames() {
        let (app, _package_dir) = test_router_with_index("multipart-invalid", "alice", "dev");
        let boundary = "BOUNDARY";
        let invalid_extension = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\":action\"\r\n\r\nfile_upload\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"name\"\r\n\r\ndemo\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"version\"\r\n\r\n1.0.0\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"content\"; filename=\"demo-1.0.0.qwe\"\r\nContent-Type: application/octet-stream\r\n\r\nsdist\r\n\
             --{boundary}--\r\n"
        );
        let request = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(invalid_extension))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(
            body_text(response)
                .await
                .contains("not a valid release file")
        );

        let version_mismatch = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\":action\"\r\n\r\nfile_upload\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"name\"\r\n\r\ndemo\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"version\"\r\n\r\n1.0.0\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"content\"; filename=\"demo-2.0.0.tar.gz\"\r\nContent-Type: application/octet-stream\r\n\r\nsdist\r\n\
             --{boundary}--\r\n"
        );
        let request = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(version_mismatch))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(
            body_text(response)
                .await
                .contains("does not contain version")
        );

        let stage = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(stage).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(body_text(response).await.contains("\"projects\":[]"));
    }

    #[tokio::test]
    async fn accepts_multipart_wheel_with_local_version_underscore() {
        let (app, package_dir) = test_router_with_index("multipart-local-wheel", "alice", "dev");
        let boundary = "BOUNDARY";
        let body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\":action\"\r\n\r\nfile_upload\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"name\"\r\n\r\npkg-hello\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"version\"\r\n\r\n1.0+gaadc053\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"content\"; filename=\"pkg-hello-1.0_gaadc053.whl\"\r\nContent-Type: application/octet-stream\r\n\r\nwheel\r\n\
             --{boundary}--\r\n"
        );
        let request = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(body))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(
            std::fs::read(package_dir.join("alice/dev/pkg-hello/pkg-hello-1.0_gaadc053.whl"))
                .unwrap(),
            b"wheel"
        );
    }

    #[tokio::test]
    async fn package_metadata_endpoint_returns_stored_file_metadata() {
        let (app, _package_dir) = test_router_with_index("core-metadata", "alice", "dev");
        let boundary = "BOUNDARY";
        let body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\":action\"\r\n\r\nfile_upload\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"name\"\r\n\r\ndemo\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"version\"\r\n\r\n1.0.0\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"summary\"\r\n\r\nDemo package\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"requires_python\"\r\n\r\n>=3.12\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"content\"; filename=\"demo-1.0.0.tar.gz\"\r\nContent-Type: application/octet-stream\r\n\r\nsdist\r\n\
             --{boundary}--\r\n"
        );
        let upload = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev/")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(body))
            .unwrap();
        let upload_response = app.clone().oneshot(upload).await.unwrap();
        assert_eq!(upload_response.status(), StatusCode::CREATED);
        let upload_serial = upload_response
            .headers()
            .get("X-DEVPI-SERIAL")
            .unwrap()
            .clone();

        let metadata = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz.metadata")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(metadata).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("X-DEVPI-SERIAL").unwrap(),
            &upload_serial
        );
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/plain; charset=utf-8"
        );
        let body = body_text(response).await;
        assert!(body.contains("Metadata-Version: 2.1\n"));
        assert!(body.contains("Name: demo\n"));
        assert!(body.contains("Version: 1.0.0\n"));
        assert!(body.contains("Summary: Demo package\n"));
        assert!(body.contains("Requires-Python: >=3.12\n"));

        let missing = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+f/demo/missing-1.0.0.tar.gz.metadata")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.oneshot(missing).await.unwrap().status(),
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn wheel_upload_extracts_core_metadata_from_deflated_archive() {
        let (app, _package_dir) = test_router_with_index("wheel-metadata", "alice", "dev");
        let wheel = deflated_zip_entry(
            "demo-1.0.0.dist-info/METADATA",
            b"Metadata-Version: 2.1\nName: demo\nVersion: 1.0.0\nSummary: From wheel\nRequires-Python: >=3.11\n\nbody\n",
        );
        let upload = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/+f/demo/demo-1.0.0-py3-none-any.whl")
            .body(axum::body::Body::from(wheel))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(upload).await.unwrap().status(),
            StatusCode::CREATED
        );

        let metadata = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+f/demo/demo-1.0.0-py3-none-any.whl.metadata")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(metadata).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_text(response).await;
        assert!(body.contains("Name: demo\n"));
        assert!(body.contains("Version: 1.0.0\n"));
        assert!(body.contains("Summary: From wheel\n"));
        assert!(body.contains("Requires-Python: >=3.11\n"));
        assert!(!body.contains("Dist-Info-Metadata"));
        assert!(!body.contains("Core-Metadata"));

        let simple = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+simple/demo/")
            .body(axum::body::Body::empty())
            .unwrap();
        let body = body_text(app.oneshot(simple).await.unwrap()).await;
        assert!(body.contains(r#"data-dist-info-metadata="true""#));
        assert!(body.contains(r#"data-core-metadata="true""#));
        assert!(body.contains(r#"data-requires-python="&gt;=3.11""#));
    }

    #[tokio::test]
    async fn sdist_upload_extracts_core_metadata_from_pkg_info() {
        let (app, _package_dir) = test_router_with_index("sdist-metadata", "alice", "dev");
        let sdist = gzipped_tar_entry(
            "demo-1.0.0/PKG-INFO",
            b"Metadata-Version: 2.1\nName: demo\nVersion: 1.0.0\nSummary: From sdist\nRequires-Python: >=3.10\n\nbody\n",
        );
        let upload = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::from(sdist))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(upload).await.unwrap().status(),
            StatusCode::CREATED
        );

        let metadata = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz.metadata")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(metadata).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_text(response).await;
        assert!(body.contains("Summary: From sdist\n"));
        assert!(body.contains("Requires-Python: >=3.10\n"));
        assert!(!body.contains("Dist-Info-Metadata"));
        assert!(!body.contains("Core-Metadata"));

        let simple = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+simple/demo/")
            .body(axum::body::Body::empty())
            .unwrap();
        let body = body_text(app.oneshot(simple).await.unwrap()).await;
        assert!(body.contains(r#"data-core-metadata="true""#));
        assert!(!body.contains(r#"data-dist-info-metadata="true""#));
        assert!(body.contains(r#"data-requires-python="&gt;=3.10""#));
    }

    #[tokio::test]
    async fn zip_sdist_upload_extracts_core_metadata_from_pkg_info() {
        let (app, _package_dir) = test_router_with_index("zip-sdist-metadata", "alice", "dev");
        let sdist = deflated_zip_entry(
            "demo-1.0.0/PKG-INFO",
            b"Metadata-Version: 2.1\nName: demo\nVersion: 1.0.0\nSummary: From zip sdist\nRequires-Python: >=3.9\n\nbody\n",
        );
        let upload = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/+f/demo/demo-1.0.0.zip")
            .body(axum::body::Body::from(sdist))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(upload).await.unwrap().status(),
            StatusCode::CREATED
        );

        let metadata = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+f/demo/demo-1.0.0.zip.metadata")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(metadata).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body_text(response).await;
        assert!(body.contains("Summary: From zip sdist\n"));
        assert!(body.contains("Requires-Python: >=3.9\n"));
        assert!(!body.contains("Dist-Info-Metadata"));
        assert!(!body.contains("Core-Metadata"));

        let simple = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+simple/demo/")
            .body(axum::body::Body::empty())
            .unwrap();
        let body = body_text(app.oneshot(simple).await.unwrap()).await;
        assert!(body.contains(r#"data-core-metadata="true""#));
        assert!(!body.contains(r#"data-dist-info-metadata="true""#));
        assert!(body.contains(r#"data-requires-python="&gt;=3.9""#));
    }

    #[tokio::test]
    async fn package_metadata_endpoint_inherits_base_file_metadata() {
        let package_dir = std::env::temp_dir().join(format!(
            "devpi-rs-core-metadata-base-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: Vec::new(),
            outside_url: None,
        });
        let create_index = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev")
            .body(axum::body::Body::from(r#"{"bases":["root/pypi"]}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_index).await.unwrap().status(),
            StatusCode::CREATED
        );

        let boundary = "BOUNDARY";
        let body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\":action\"\r\n\r\nfile_upload\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"name\"\r\n\r\ndemo\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"version\"\r\n\r\n1.0.0\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"summary\"\r\n\r\nBase package\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"content\"; filename=\"demo-1.0.0.tar.gz\"\r\nContent-Type: application/octet-stream\r\n\r\nsdist\r\n\
             --{boundary}--\r\n"
        );
        let upload = Request::builder()
            .method(Method::POST)
            .uri("/root/pypi/")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(body))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(upload).await.unwrap().status(),
            StatusCode::CREATED
        );

        let metadata = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz.metadata")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(metadata).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = body_text(response).await;
        assert!(body.contains("Name: demo\n"));
        assert!(body.contains("Version: 1.0.0\n"));
        assert!(body.contains("Summary: Base package\n"));
    }

    #[tokio::test]
    async fn accepts_multipart_submit_release_metadata() {
        let (app, _package_dir) = test_router_with_index("submit", "alice", "dev");
        let boundary = "BOUNDARY";
        let body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\":action\"\r\n\r\nsubmit\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"name\"\r\n\r\ndemo\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"version\"\r\n\r\n1.0.0\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"summary\"\r\n\r\nDemo release\r\n\
             --{boundary}--\r\n"
        );
        let request = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev/")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(body))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "1");
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["status"], 200);
        assert_eq!(body["registered"], true);
        let changelog = Request::builder()
            .method(Method::GET)
            .uri("/+changelog/1")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(changelog).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(
            body["result"],
            json!([{
                "serial": 1,
                "event": "version_register",
                "stage": "alice/dev",
                "project": "demo",
                "version": "1.0.0",
            }])
        );
        let project = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/demo")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(project).await.unwrap();
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "1");
        let body = body_text(response).await;
        assert!(body.contains("\"files\":[]"));
        assert!(body.contains("\"versions\""));
        assert!(body.contains("\"1.0.0\""));
        assert!(body.contains("\"summary\":\"Demo release\""));
        let version = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/demo/1.0.0")
            .header(header::ACCEPT, "application/json")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(version).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "1");
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["type"], "versiondata");
        assert_eq!(body["result"]["name"], "demo");
        assert_eq!(body["result"]["version"], "1.0.0");
        assert_eq!(body["result"]["summary"], "Demo release");
    }

    #[tokio::test]
    async fn accepts_urlencoded_submit_release_metadata() {
        let (app, _package_dir) = test_router_with_index("submit-urlencoded", "alice", "dev");
        let request = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(axum::body::Body::from(
                "%3Aaction=submit&name=demo&version=1.0.0&description=hello+world&classifiers=Intended+Audience+%3A%3A+Developers&classifiers=License+%3A%3A+MIT",
            ))
            .unwrap();

        let response = app.clone().oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "1");
        let version = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/demo/1.0.0")
            .header(header::ACCEPT, "application/json")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(version).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["result"]["description"], "hello world");
        assert_eq!(
            body["result"]["classifiers"],
            json!(["Intended Audience :: Developers", "License :: MIT"])
        );
    }

    #[tokio::test]
    async fn submit_release_metadata_normalizes_unknown_values() {
        let (app, _package_dir) = test_router_with_index("submit-unknown", "alice", "dev");
        let boundary = "BOUNDARY";
        let body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\":action\"\r\n\r\nsubmit\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"name\"\r\n\r\ndemo\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"version\"\r\n\r\n1.0.0\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"download_url\"\r\n\r\nUNKNOWN\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"platform\"\r\n\r\n\r\n\
             --{boundary}--\r\n"
        );
        let request = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev/")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(body))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(request).await.unwrap().status(),
            StatusCode::OK
        );
        let version = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/demo/1.0.0")
            .header(header::ACCEPT, "application/json")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(version).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["result"]["download_url"], "");
        assert_eq!(body["result"]["platform"], json!([]));
    }

    #[tokio::test]
    async fn submit_release_metadata_preserves_multivalue_fields() {
        let (app, _package_dir) = test_router_with_index("submit-multivalue", "alice", "dev");
        let boundary = "BOUNDARY";
        let body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\":action\"\r\n\r\nsubmit\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"name\"\r\n\r\ndemo\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"version\"\r\n\r\n1.0.0\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"classifiers\"\r\n\r\nIntended Audience :: Developers\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"classifiers\"\r\n\r\nLicense :: OSI Approved :: MIT License\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"platform\"\r\n\r\nunix\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"platform\"\r\n\r\nwin32\r\n\
             --{boundary}--\r\n"
        );
        let request = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev/")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(body))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(request).await.unwrap().status(),
            StatusCode::OK
        );
        let version = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/demo/1.0.0")
            .header(header::ACCEPT, "application/json")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(version).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(
            body["result"]["classifiers"],
            json!([
                "Intended Audience :: Developers",
                "License :: OSI Approved :: MIT License"
            ])
        );
        assert_eq!(body["result"]["platform"], json!(["unix", "win32"]));
    }

    #[tokio::test]
    async fn version_metadata_get_inherits_base_index_by_default() {
        let package_dir =
            std::env::temp_dir().join(format!("devpi-rs-version-bases-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: Vec::new(),
            outside_url: None,
        });
        let create_index = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev")
            .body(axum::body::Body::from(r#"{"bases":["root/pypi"]}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_index).await.unwrap().status(),
            StatusCode::CREATED
        );

        let boundary = "BOUNDARY";
        let body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\":action\"\r\n\r\nsubmit\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"name\"\r\n\r\ndemo\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"version\"\r\n\r\n1.0.0\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"summary\"\r\n\r\nBase release\r\n\
             --{boundary}--\r\n"
        );
        let submit = Request::builder()
            .method(Method::POST)
            .uri("/root/pypi/")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(body))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(submit).await.unwrap().status(),
            StatusCode::OK
        );

        let inherited = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/demo/1.0.0")
            .header(header::ACCEPT, "application/json")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(inherited).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["type"], "versiondata");
        assert_eq!(body["result"]["summary"], "Base release");

        let inherited_slash = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/demo/1.0.0/")
            .header(header::ACCEPT, "application/json")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(inherited_slash).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["type"], "versiondata");
        assert_eq!(body["result"]["summary"], "Base release");

        let project = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/demo")
            .header(header::ACCEPT, "application/json")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(project).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(
            body["result"]["versions"]["1.0.0"]["summary"],
            "Base release"
        );

        let perstage = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/demo/1.0.0?ignore_bases")
            .header(header::ACCEPT, "application/json")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(perstage).await.unwrap().status(),
            StatusCode::NOT_FOUND
        );

        let perstage_project = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/demo?ignore_bases")
            .header(header::ACCEPT, "application/json")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.oneshot(perstage_project).await.unwrap().status(),
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn stage_version_returns_not_found_for_unknown_index() {
        let state = test_state("stage-version-index-404");

        let response = render_version_metadata(
            state,
            "alice".to_string(),
            "dev".to_string(),
            "demo".to_string(),
            "1.0.0".to_string(),
            true,
            None,
        )
        .await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert!(body_text(response).await.contains("index not found"));
    }

    #[tokio::test]
    async fn project_json_inherits_base_files_metadata_and_tox_results() {
        let package_dir =
            std::env::temp_dir().join(format!("devpi-rs-project-bases-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: Vec::new(),
            outside_url: None,
        });
        let create_index = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev")
            .body(axum::body::Body::from(r#"{"bases":["root/pypi"]}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_index).await.unwrap().status(),
            StatusCode::CREATED
        );

        let boundary = "BOUNDARY";
        let body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\":action\"\r\n\r\nfile_upload\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"name\"\r\n\r\ndemo\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"version\"\r\n\r\n1.0.0\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"requires_python\"\r\n\r\n>=3.12\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"content\"; filename=\"demo-1.0.0.tar.gz\"\r\nContent-Type: application/octet-stream\r\n\r\nsdist\r\n\
             --{boundary}--\r\n"
        );
        let upload = Request::builder()
            .method(Method::POST)
            .uri("/root/pypi/")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(body))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(upload).await.unwrap().status(),
            StatusCode::CREATED
        );
        let tox = Request::builder()
            .method(Method::POST)
            .uri("/root/pypi/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::from(r#"{"envname":"py312","retcode":0}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(tox).await.unwrap().status(),
            StatusCode::OK
        );

        let inherited = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/demo")
            .header(header::ACCEPT, "application/json")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(inherited).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["result"]["files"][0]["filename"], "demo-1.0.0.tar.gz");
        assert_eq!(
            body["result"]["files"][0]["url"],
            "/alice/dev/+f/demo/demo-1.0.0.tar.gz"
        );
        assert_eq!(
            body["result"]["files"][0]["metadata"]["requires_python"],
            ">=3.12"
        );
        assert_eq!(
            body["result"]["files"][0]["toxresults"][0]["envname"],
            "py312"
        );

        let tox_result = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz.toxresult-0")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(tox_result).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "3");
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["envname"], "py312");
        assert_eq!(body["retcode"], 0);

        let perstage = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/demo?ignore_bases")
            .header(header::ACCEPT, "application/json")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.oneshot(perstage).await.unwrap().status(),
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn pushes_release_to_internal_stage() {
        let (app, package_dir) = test_router_with_index("internal-push", "alice", "dev");
        let target = Request::builder()
            .method(Method::PUT)
            .uri("/bob/prod")
            .body(axum::body::Body::from(r#"{"acl_upload":[":ANONYMOUS:"]}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(target).await.unwrap().status(),
            StatusCode::CREATED
        );

        let boundary = "BOUNDARY";
        let body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\":action\"\r\n\r\nfile_upload\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"name\"\r\n\r\ndemo\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"version\"\r\n\r\n1.0.0\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"summary\"\r\n\r\nDemo package\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"requires_python\"\r\n\r\n>=3.12\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"content\"; filename=\"demo-1.0.0.tar.gz\"\r\nContent-Type: application/octet-stream\r\n\r\nsdist\r\n\
             --{boundary}--\r\n"
        );
        let upload = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev/")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(body))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(upload).await.unwrap().status(),
            StatusCode::CREATED
        );
        let tox = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::from(r#"{"envname":"py312","retcode":0}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(tox).await.unwrap().status(),
            StatusCode::OK
        );

        let push = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev")
            .body(axum::body::Body::from(
                r#"{"name":"demo","version":"1.0.0","targetindex":"bob/prod"}"#,
            ))
            .unwrap();
        let response = app.clone().oneshot(push).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "4");
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["type"], "actionlog");
        assert!(body["result"].as_array().unwrap().iter().any(|entry| {
            entry.as_array().is_some_and(|parts| {
                parts.get(1) == Some(&json!("upload"))
                    && parts.get(3) == Some(&json!("->"))
                    && parts.get(4) == Some(&json!("bob/prod/demo/demo-1.0.0.tar.gz"))
            })
        }));
        let changelog = Request::builder()
            .method(Method::GET)
            .uri("/+changelog/4")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(changelog).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(
            body["result"],
            json!([{
                "serial": 4,
                "event": "release_push",
                "source_stage": "alice/dev",
                "target_stage": "bob/prod",
                "project": "demo",
                "version": "1.0.0",
            }])
        );

        let get_file = Request::builder()
            .method(Method::GET)
            .uri("/bob/prod/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(get_file).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&bytes[..], b"sdist");
        assert_eq!(
            std::fs::read(package_dir.join("bob/prod/demo/demo-1.0.0.tar.gz")).unwrap(),
            b"sdist"
        );

        let project = Request::builder()
            .method(Method::GET)
            .uri("/bob/prod/demo")
            .header(header::ACCEPT, "application/json")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(project).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(
            body["result"]["files"][0]["metadata"]["summary"],
            "Demo package"
        );
        assert_eq!(
            body["result"]["files"][0]["metadata"]["requires_python"],
            ">=3.12"
        );
        assert_eq!(
            body["result"]["files"][0]["toxresults"][0]["envname"],
            "py312"
        );

        let version = Request::builder()
            .method(Method::GET)
            .uri("/bob/prod/demo/1.0.0")
            .header(header::ACCEPT, "application/json")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(version).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["result"]["name"], "demo");
        assert_eq!(body["result"]["version"], "1.0.0");
        let links = body["result"]["+links"].as_array().unwrap();
        let release_link = links
            .iter()
            .find(|link| link["rel"] == "releasefile")
            .unwrap();
        let log = release_link["log"].as_array().unwrap();
        assert_eq!(log.len(), 2);
        assert_eq!(log[0]["what"], "upload");
        assert_eq!(log[0]["dst"], "alice/dev");
        assert!(log[0]["who"].is_null());
        assert_eq!(log[0]["when"].as_array().unwrap().len(), 6);
        assert_eq!(log[1]["what"], "push");
        assert_eq!(log[1]["src"], "alice/dev");
        assert_eq!(log[1]["dst"], "bob/prod");
        assert!(log[1]["who"].is_null());
        assert_eq!(log[1]["when"].as_array().unwrap().len(), 6);
        let tox_link = links
            .iter()
            .find(|link| link["rel"] == "toxresult")
            .unwrap();
        let tox_log = tox_link["log"].as_array().unwrap();
        assert_eq!(tox_log.len(), 2);
        assert_eq!(tox_log[0]["what"], "upload");
        assert_eq!(tox_log[0]["dst"], "alice/dev");
        assert_eq!(tox_log[1]["what"], "push");
        assert_eq!(tox_log[1]["src"], "alice/dev");
        assert_eq!(tox_log[1]["dst"], "bob/prod");
    }

    #[tokio::test]
    async fn pushes_release_from_mirror_stage_to_internal_stage() {
        let dir = std::env::temp_dir().join(format!("devpi-rs-mirror-push-{}", std::process::id()));
        let cache_dir =
            std::env::temp_dir().join(format!("devpi-rs-mirror-push-cache-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&cache_dir);
        let state = AppState {
            index: Arc::new(MultiSourceIndex::new(Vec::new(), cache_dir)),
            local: Arc::new(LocalStore::new(dir.clone())),
            registry: Arc::new(Registry::new(dir.clone())),
            serial: Arc::new(SerialStore::new(dir.join(".devpi-rs-serial"))),
            fetcher: Arc::new(MockFetcher {
                responses: Mutex::new(HashMap::from([
                    (
                        "https://pypi.org/simple/demo/".to_string(),
                        Ok(
                            r#"<a href="https://pypi.org/packages/demo-1.0.0.tar.gz#sha256=file123"
                              data-requires-python="&gt;=3.9"
                              data-yanked="bad build"
                              data-core-metadata="sha256=meta123"
                              data-gpg-sig="true">demo-1.0.0.tar.gz</a>"#
                                .to_string(),
                        ),
                    ),
                    (
                        "https://pypi.org/packages/demo-1.0.0.tar.gz".to_string(),
                        Ok("sdist".to_string()),
                    ),
                ])),
            }),
            external_poster: Arc::new(TcpExternalPoster),
            upstream_reports: Arc::new(Mutex::new(Vec::new())),
        };
        state
            .registry
            .put_index(
                "bob",
                "prod",
                IndexInput {
                    acl_upload: Some(vec![":ANONYMOUS:".to_string()]),
                    ..Default::default()
                },
            )
            .unwrap();

        let response = push_release(
            state.clone(),
            "root".to_string(),
            "pypi".to_string(),
            Bytes::from(r#"{"name":"demo","version":"1.0.0","targetindex":"bob/prod"}"#),
            None,
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["type"], "actionlog");
        assert!(body["result"].as_array().unwrap().iter().any(|entry| {
            entry.as_array().is_some_and(|parts| {
                parts.get(1) == Some(&json!("upload"))
                    && parts.get(4) == Some(&json!("bob/prod/demo/demo-1.0.0.tar.gz"))
            })
        }));
        assert_eq!(
            std::fs::read(dir.join("bob/prod/demo/demo-1.0.0.tar.gz")).unwrap(),
            b"sdist"
        );
        assert!(
            state
                .upstream_reports
                .lock()
                .unwrap()
                .iter()
                .any(|report| report.url == "https://pypi.org/simple/demo/")
        );
        let simple = render_simple_project_with_format(
            state,
            "bob".to_string(),
            "prod".to_string(),
            "demo".to_string(),
            SimpleFormat::Html,
            None,
            false,
        )
        .await;
        assert_eq!(simple.status(), StatusCode::OK);
        let body = body_text(simple).await;
        assert!(body.contains(r#"data-requires-python="&gt;=3.9""#));
        assert!(body.contains(r#"data-yanked="bad build""#));
        assert!(body.contains(r#"data-core-metadata="sha256=meta123""#));
        assert!(body.contains(r#"data-gpg-sig="true""#));
    }

    #[tokio::test]
    async fn pushes_release_to_external_http_posturl() {
        let dir =
            std::env::temp_dir().join(format!("devpi-rs-external-push-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let external_poster = Arc::new(MockExternalPoster::default());
        let state = AppState {
            index: Arc::new(MultiSourceIndex::new(Vec::new(), std::env::temp_dir())),
            local: Arc::new(LocalStore::new(dir.clone())),
            registry: Arc::new(Registry::new(dir.clone())),
            serial: Arc::new(SerialStore::new(dir.join(".devpi-rs-serial"))),
            fetcher: Arc::new(CurlFetcher::default()),
            external_poster: external_poster.clone(),
            upstream_reports: Arc::new(Mutex::new(Vec::new())),
        };
        put_anonymous_index(&state, "alice", "dev");
        state
            .local
            .save_in("alice", "dev", "demo", "demo-1.0.0.tar.gz", b"sdist")
            .unwrap();

        let response = push_release(
            state,
            "alice".to_string(),
            "dev".to_string(),
            Bytes::from(
                r#"{
                    "name":"demo",
                    "version":"1.0.0",
                    "posturl":"http://upload.example/legacy/",
                    "username":"user",
                    "password":"password",
                    "register_project":true
                }"#,
            ),
            None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "0");
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["type"], "actionlog");
        assert_eq!(body["result"][0][0], 200);
        assert_eq!(body["result"][0][1], "register");
        assert_eq!(body["result"][1][0], 200);
        assert_eq!(body["result"][1][1], "upload");
        assert_eq!(body["result"][1][2], "alice/dev/+f/demo/demo-1.0.0.tar.gz");

        let requests = external_poster.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].url, "http://upload.example/legacy/");
        assert_eq!(
            requests[0].auth,
            Some(("user".to_string(), "password".to_string()))
        );
        assert!(
            requests[0]
                .fields
                .contains(&(":action".to_string(), "submit".to_string()))
        );
        assert!(
            requests[1]
                .fields
                .contains(&(":action".to_string(), "file_upload".to_string()))
        );
        assert_eq!(
            requests[1].file,
            Some(("demo-1.0.0.tar.gz".to_string(), b"sdist".to_vec()))
        );
    }

    #[tokio::test]
    async fn external_push_treats_register_410_as_continue() {
        let dir =
            std::env::temp_dir().join(format!("devpi-rs-external-push-410-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let external_poster = Arc::new(MockExternalPoster::with_responses(vec![
            ExternalPostResponse {
                status: 410,
                body: "gone".to_string(),
            },
            ExternalPostResponse {
                status: 200,
                body: "ok".to_string(),
            },
        ]));
        let state = AppState {
            index: Arc::new(MultiSourceIndex::new(Vec::new(), std::env::temp_dir())),
            local: Arc::new(LocalStore::new(dir.clone())),
            registry: Arc::new(Registry::new(dir.clone())),
            serial: Arc::new(SerialStore::new(dir.join(".devpi-rs-serial"))),
            fetcher: Arc::new(CurlFetcher::default()),
            external_poster: external_poster.clone(),
            upstream_reports: Arc::new(Mutex::new(Vec::new())),
        };
        put_anonymous_index(&state, "alice", "dev");
        state
            .local
            .save_in("alice", "dev", "demo", "demo-1.0.0.tar.gz", b"sdist")
            .unwrap();

        let response = push_release(
            state,
            "alice".to_string(),
            "dev".to_string(),
            Bytes::from(
                r#"{
                    "name":"demo",
                    "version":"1.0.0",
                    "posturl":"http://upload.example/legacy/",
                    "username":"user",
                    "password":"password",
                    "register_project":true
                }"#,
            ),
            None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["result"][0][0], 410);
        assert_eq!(body["result"][0][1], "register");
        assert_eq!(body["result"][1][0], 200);
        assert_eq!(body["result"][1][1], "upload");
        assert_eq!(external_poster.requests.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn external_push_register_exception_stops_before_upload() {
        let dir = std::env::temp_dir().join(format!(
            "devpi-rs-external-push-register-error-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let external_poster = Arc::new(MockExternalPoster::with_outcomes(vec![Err(
            io::Error::other("network down"),
        )]));
        let state = AppState {
            index: Arc::new(MultiSourceIndex::new(Vec::new(), std::env::temp_dir())),
            local: Arc::new(LocalStore::new(dir.clone())),
            registry: Arc::new(Registry::new(dir.clone())),
            serial: Arc::new(SerialStore::new(dir.join(".devpi-rs-serial"))),
            fetcher: Arc::new(CurlFetcher::default()),
            external_poster: external_poster.clone(),
            upstream_reports: Arc::new(Mutex::new(Vec::new())),
        };
        put_anonymous_index(&state, "alice", "dev");
        state
            .local
            .save_in("alice", "dev", "demo", "demo-1.0.0.tar.gz", b"sdist")
            .unwrap();

        let response = push_release(
            state,
            "alice".to_string(),
            "dev".to_string(),
            Bytes::from(
                r#"{
                    "name":"demo",
                    "version":"1.0.0",
                    "posturl":"http://upload.example/legacy/",
                    "username":"user",
                    "password":"password",
                    "register_project":true
                }"#,
            ),
            None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["type"], "actionlog");
        assert_eq!(body["result"].as_array().unwrap().len(), 1);
        assert_eq!(body["result"][0][0], -1);
        assert_eq!(body["result"][0][1], "exception on register:");
        assert_eq!(external_poster.requests.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn external_push_release_upload_exception_is_reported() {
        let dir = std::env::temp_dir().join(format!(
            "devpi-rs-external-push-upload-error-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let external_poster = Arc::new(MockExternalPoster::with_outcomes(vec![
            Ok(ExternalPostResponse {
                status: 410,
                body: "gone".to_string(),
            }),
            Err(io::Error::other("upload failed")),
        ]));
        let state = AppState {
            index: Arc::new(MultiSourceIndex::new(Vec::new(), std::env::temp_dir())),
            local: Arc::new(LocalStore::new(dir.clone())),
            registry: Arc::new(Registry::new(dir.clone())),
            serial: Arc::new(SerialStore::new(dir.join(".devpi-rs-serial"))),
            fetcher: Arc::new(CurlFetcher::default()),
            external_poster: external_poster.clone(),
            upstream_reports: Arc::new(Mutex::new(Vec::new())),
        };
        put_anonymous_index(&state, "alice", "dev");
        state
            .local
            .save_in("alice", "dev", "demo", "demo-1.0.0.tar.gz", b"sdist")
            .unwrap();

        let response = push_release(
            state,
            "alice".to_string(),
            "dev".to_string(),
            Bytes::from(
                r#"{
                    "name":"demo",
                    "version":"1.0.0",
                    "posturl":"http://upload.example/legacy/",
                    "username":"user",
                    "password":"password",
                    "register_project":true
                }"#,
            ),
            None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["result"].as_array().unwrap().len(), 2);
        assert_eq!(body["result"][0][0], 410);
        assert_eq!(body["result"][0][1], "register");
        assert_eq!(body["result"][1][0], -1);
        assert_eq!(body["result"][1][1], "exception on release upload:");
        assert_eq!(external_poster.requests.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn external_push_honors_doc_filters() {
        let dir = std::env::temp_dir().join(format!(
            "devpi-rs-external-push-docs-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let external_poster = Arc::new(MockExternalPoster::default());
        let state = AppState {
            index: Arc::new(MultiSourceIndex::new(Vec::new(), std::env::temp_dir())),
            local: Arc::new(LocalStore::new(dir.clone())),
            registry: Arc::new(Registry::new(dir.clone())),
            serial: Arc::new(SerialStore::new(dir.join(".devpi-rs-serial"))),
            fetcher: Arc::new(CurlFetcher::default()),
            external_poster: external_poster.clone(),
            upstream_reports: Arc::new(Mutex::new(Vec::new())),
        };
        put_anonymous_index(&state, "alice", "dev");
        state
            .local
            .save_in("alice", "dev", "demo", "demo-1.0.0.tar.gz", b"sdist")
            .unwrap();
        state
            .local
            .save_in("alice", "dev", "demo", "demo-1.0.0.doc.zip", b"docs")
            .unwrap();

        let only_docs = push_release(
            state.clone(),
            "alice".to_string(),
            "dev".to_string(),
            Bytes::from(
                r#"{
                    "name":"demo",
                    "version":"1.0.0",
                    "posturl":"http://upload.example/legacy/",
                    "username":"user",
                    "password":"password",
                    "only_docs":true
                }"#,
            ),
            None,
        )
        .await;
        assert_eq!(only_docs.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(only_docs).await).unwrap();
        assert_eq!(body["result"].as_array().unwrap().len(), 1);
        assert_eq!(body["result"][0][1], "docfile");

        let no_docs = push_release(
            state,
            "alice".to_string(),
            "dev".to_string(),
            Bytes::from(
                r#"{
                    "name":"demo",
                    "version":"1.0.0",
                    "posturl":"http://upload.example/legacy/",
                    "username":"user",
                    "password":"password",
                    "no_docs":true
                }"#,
            ),
            None,
        )
        .await;
        assert_eq!(no_docs.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(no_docs).await).unwrap();
        assert_eq!(body["result"].as_array().unwrap().len(), 1);
        assert_eq!(body["result"][0][1], "upload");

        let requests = external_poster.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(
            requests[0]
                .fields
                .contains(&(":action".to_string(), "doc_upload".to_string()))
        );
        assert_eq!(
            requests[0].file,
            Some(("demo-1.0.0.doc.zip".to_string(), b"docs".to_vec()))
        );
        assert!(
            requests[1]
                .fields
                .contains(&(":action".to_string(), "file_upload".to_string()))
        );
        assert_eq!(
            requests[1].file,
            Some(("demo-1.0.0.tar.gz".to_string(), b"sdist".to_vec()))
        );
    }

    #[tokio::test]
    async fn external_push_sends_version_and_file_metadata() {
        let dir = std::env::temp_dir().join(format!(
            "devpi-rs-external-push-metadata-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let external_poster = Arc::new(MockExternalPoster::default());
        let state = AppState {
            index: Arc::new(MultiSourceIndex::new(Vec::new(), std::env::temp_dir())),
            local: Arc::new(LocalStore::new(dir.clone())),
            registry: Arc::new(Registry::new(dir.clone())),
            serial: Arc::new(SerialStore::new(dir.join(".devpi-rs-serial"))),
            fetcher: Arc::new(CurlFetcher::default()),
            external_poster: external_poster.clone(),
            upstream_reports: Arc::new(Mutex::new(Vec::new())),
        };
        put_anonymous_index(&state, "alice", "dev");
        state
            .local
            .save_in("alice", "dev", "demo", "demo-1.0.0.tar.gz", b"sdist")
            .unwrap();
        state
            .local
            .save_version_metadata_in(
                "alice",
                "dev",
                "demo",
                "1.0.0",
                BTreeMap::from([
                    ("name".to_string(), "demo".to_string()),
                    ("version".to_string(), "1.0.0".to_string()),
                    ("metadata_version".to_string(), "2.4".to_string()),
                    ("license_expression".to_string(), "MIT".to_string()),
                ]),
            )
            .unwrap();
        state
            .local
            .save_file_metadata_in(
                "alice",
                "dev",
                "demo",
                "demo-1.0.0.tar.gz",
                BTreeMap::from([
                    ("requires_python".to_string(), ">=3.12".to_string()),
                    ("yanked".to_string(), "bad build".to_string()),
                ]),
            )
            .unwrap();

        let response = push_release(
            state,
            "alice".to_string(),
            "dev".to_string(),
            Bytes::from(
                r#"{
                    "name":"demo",
                    "version":"1.0.0",
                    "posturl":"http://upload.example/legacy/",
                    "username":"user",
                    "password":"password",
                    "register_project":true
                }"#,
            ),
            None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);

        let requests = external_poster.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(
            requests[0]
                .fields
                .contains(&("metadata_version".to_string(), "2.4".to_string()))
        );
        assert!(
            requests[0]
                .fields
                .contains(&("license_expression".to_string(), "MIT".to_string()))
        );
        assert!(
            requests[1]
                .fields
                .contains(&("metadata_version".to_string(), "2.4".to_string()))
        );
        assert!(
            requests[1]
                .fields
                .contains(&("license_expression".to_string(), "MIT".to_string()))
        );
        assert!(
            requests[1]
                .fields
                .contains(&("requires_python".to_string(), ">=3.12".to_string()))
        );
        assert!(
            requests[1]
                .fields
                .contains(&("yanked".to_string(), "bad build".to_string()))
        );
    }

    #[tokio::test]
    async fn custom_push_method_uses_internal_push() {
        let (app, _package_dir) = test_router_with_index("custom-push", "alice", "dev");
        let target = Request::builder()
            .method(Method::PUT)
            .uri("/bob/prod")
            .body(axum::body::Body::from(r#"{"acl_upload":[":ANONYMOUS:"]}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(target).await.unwrap().status(),
            StatusCode::CREATED
        );
        let upload = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::from("sdist"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(upload).await.unwrap().status(),
            StatusCode::CREATED
        );

        let push = Request::builder()
            .method(Method::from_bytes(b"PUSH").unwrap())
            .uri("/alice/dev")
            .body(axum::body::Body::from(
                r#"{"name":"demo","version":"1.0.0","targetindex":"bob/prod"}"#,
            ))
            .unwrap();
        let response = app.clone().oneshot(push).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["type"], "actionlog");

        let get = Request::builder()
            .method(Method::GET)
            .uri("/bob/prod/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(app.oneshot(get).await.unwrap().status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn custom_push_method_accepts_trailing_slash() {
        let (app, _package_dir) = test_router_with_index("custom-push-slash", "alice", "dev");
        let target = Request::builder()
            .method(Method::PUT)
            .uri("/bob/prod")
            .body(axum::body::Body::from(r#"{"acl_upload":[":ANONYMOUS:"]}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(target).await.unwrap().status(),
            StatusCode::CREATED
        );
        let upload = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::from("sdist"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(upload).await.unwrap().status(),
            StatusCode::CREATED
        );

        let push = Request::builder()
            .method(Method::from_bytes(b"PUSH").unwrap())
            .uri("/alice/dev/")
            .body(axum::body::Body::from(
                r#"{"name":"demo","version":"1.0.0","targetindex":"bob/prod"}"#,
            ))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(push).await.unwrap().status(),
            StatusCode::OK
        );

        let get = Request::builder()
            .method(Method::GET)
            .uri("/bob/prod/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(app.oneshot(get).await.unwrap().status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn stage_post_json_with_trailing_slash_uses_internal_push() {
        let (app, _package_dir) = test_router_with_index("post-push-slash", "alice", "dev");
        let target = Request::builder()
            .method(Method::PUT)
            .uri("/bob/prod")
            .body(axum::body::Body::from(r#"{"acl_upload":[":ANONYMOUS:"]}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(target).await.unwrap().status(),
            StatusCode::CREATED
        );
        let upload = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::from("sdist"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(upload).await.unwrap().status(),
            StatusCode::CREATED
        );

        let push = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev/")
            .header(header::CONTENT_TYPE, "application/json")
            .body(axum::body::Body::from(
                r#"{"name":"demo","version":"1.0.0","targetindex":"bob/prod"}"#,
            ))
            .unwrap();
        let response = app.clone().oneshot(push).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["type"], "actionlog");

        let get = Request::builder()
            .method(Method::GET)
            .uri("/bob/prod/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(app.oneshot(get).await.unwrap().status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn internal_push_honors_doc_filters() {
        let (app, _package_dir) = test_router_with_index("push-doc-filters", "alice", "dev");
        for target in ["/bob/prod", "/bob/docs"] {
            let request = Request::builder()
                .method(Method::PUT)
                .uri(target)
                .body(axum::body::Body::from(r#"{"acl_upload":[":ANONYMOUS:"]}"#))
                .unwrap();
            assert_eq!(
                app.clone().oneshot(request).await.unwrap().status(),
                StatusCode::CREATED
            );
        }

        let boundary = "BOUNDARY";
        let release = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\":action\"\r\n\r\nfile_upload\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"name\"\r\n\r\ndemo\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"version\"\r\n\r\n1.0.0\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"content\"; filename=\"demo-1.0.0.tar.gz\"\r\nContent-Type: application/octet-stream\r\n\r\nsdist\r\n\
             --{boundary}--\r\n"
        );
        let upload_release = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev/")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(release))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(upload_release).await.unwrap().status(),
            StatusCode::CREATED
        );
        let docs = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\":action\"\r\n\r\ndoc_upload\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"name\"\r\n\r\ndemo\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"version\"\r\n\r\n1.0.0\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"content\"; filename=\"docs.zip\"\r\nContent-Type: application/zip\r\n\r\ndocs\r\n\
             --{boundary}--\r\n"
        );
        let upload_docs = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev/")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(docs))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(upload_docs).await.unwrap().status(),
            StatusCode::CREATED
        );

        let push_no_docs = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev")
            .body(axum::body::Body::from(
                r#"{"name":"demo","version":"1.0.0","targetindex":"bob/prod","no_docs":true}"#,
            ))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(push_no_docs).await.unwrap().status(),
            StatusCode::OK
        );
        let push_only_docs = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev")
            .body(axum::body::Body::from(
                r#"{"name":"demo","version":"1.0.0","targetindex":"bob/docs","only_docs":true}"#,
            ))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(push_only_docs).await.unwrap().status(),
            StatusCode::OK
        );

        for (path, expected) in [
            ("/bob/prod/+f/demo/demo-1.0.0.tar.gz", StatusCode::OK),
            (
                "/bob/prod/+f/demo/demo-1.0.0.doc.zip",
                StatusCode::NOT_FOUND,
            ),
            ("/bob/docs/+f/demo/demo-1.0.0.tar.gz", StatusCode::NOT_FOUND),
            ("/bob/docs/+f/demo/demo-1.0.0.doc.zip", StatusCode::OK),
        ] {
            let request = Request::builder()
                .method(Method::GET)
                .uri(path)
                .body(axum::body::Body::empty())
                .unwrap();
            assert_eq!(
                app.clone().oneshot(request).await.unwrap().status(),
                expected
            );
        }
    }

    #[tokio::test]
    async fn internal_push_enforces_target_acl_upload() {
        let (app, _package_dir) = test_router_with_index("push-acl", "alice", "dev");
        let create_user = Request::builder()
            .method(Method::PUT)
            .uri("/bob")
            .body(axum::body::Body::from(r#"{"password":"123"}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_user).await.unwrap().status(),
            StatusCode::CREATED
        );
        let create_target = Request::builder()
            .method(Method::PUT)
            .uri("/bob/prod")
            .body(axum::body::Body::from(r#"{"acl_upload":["bob"]}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_target).await.unwrap().status(),
            StatusCode::CREATED
        );
        let upload = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::from("sdist"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(upload).await.unwrap().status(),
            StatusCode::CREATED
        );

        let push = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev")
            .body(axum::body::Body::from(
                r#"{"name":"demo","version":"1.0.0","targetindex":"bob/prod"}"#,
            ))
            .unwrap();
        let response = app.clone().oneshot(push).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.headers().get(header::WWW_AUTHENTICATE).unwrap(),
            "Basic realm=\"devpi-rs\""
        );
        let missing = Request::builder()
            .method(Method::GET)
            .uri("/bob/prod/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(missing).await.unwrap().status(),
            StatusCode::NOT_FOUND
        );

        let push = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev")
            .header(header::AUTHORIZATION, "Basic Ym9iOjEyMw==")
            .body(axum::body::Body::from(
                r#"{"name":"demo","version":"1.0.0","targetindex":"bob/prod"}"#,
            ))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(push).await.unwrap().status(),
            StatusCode::OK
        );
        let get = Request::builder()
            .method(Method::GET)
            .uri("/bob/prod/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(app.oneshot(get).await.unwrap().status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn internal_push_respects_non_volatile_target_conflicts() {
        let (app, _package_dir) = test_router_with_index("push-nonvolatile", "alice", "dev");
        let create_target = Request::builder()
            .method(Method::PUT)
            .uri("/bob/prod")
            .body(axum::body::Body::from(
                r#"{"volatile":false,"acl_upload":[":ANONYMOUS:"]}"#,
            ))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_target).await.unwrap().status(),
            StatusCode::CREATED
        );
        for content in ["one", "two"] {
            let upload = Request::builder()
                .method(Method::PUT)
                .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
                .body(axum::body::Body::from(content))
                .unwrap();
            assert_eq!(
                app.clone().oneshot(upload).await.unwrap().status(),
                StatusCode::CREATED
            );
            let push = Request::builder()
                .method(Method::POST)
                .uri("/alice/dev")
                .body(axum::body::Body::from(
                    r#"{"name":"demo","version":"1.0.0","targetindex":"bob/prod"}"#,
                ))
                .unwrap();
            let response = app.clone().oneshot(push).await.unwrap();
            if content == "one" {
                assert_eq!(response.status(), StatusCode::OK);
            } else {
                assert_eq!(response.status(), StatusCode::CONFLICT);
                assert!(body_text(response).await.contains("non-volatile index"));
            }
        }
        let get = Request::builder()
            .method(Method::GET)
            .uri("/bob/prod/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(get).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&bytes[..], b"one");
    }

    #[tokio::test]
    async fn internal_push_returns_json_errors() {
        let (app, _package_dir) = test_router_with_index("push-json-errors", "alice", "dev");
        for (payload, expected_status, expected_message) in [
            (
                r#"{"name":"demo","version":"1.0.0","targetindex":"bob/prod","no_docs":true,"only_docs":true}"#,
                StatusCode::BAD_REQUEST,
                "can't use 'no_docs' and 'only_docs' together",
            ),
            (
                r#"{"name":"demo","version":"1.0.0"}"#,
                StatusCode::BAD_REQUEST,
                "targetindex or posturl is required",
            ),
            (
                r#"{"name":"demo","version":"1.0.0","targetindex":"bob/prod/extra"}"#,
                StatusCode::BAD_REQUEST,
                "invalid targetindex",
            ),
        ] {
            let push = Request::builder()
                .method(Method::POST)
                .uri("/alice/dev")
                .body(axum::body::Body::from(payload))
                .unwrap();
            let response = app.clone().oneshot(push).await.unwrap();
            assert_eq!(response.status(), expected_status);
            assert_eq!(
                response.headers().get(header::CONTENT_TYPE).unwrap(),
                "application/json; charset=utf-8"
            );
            let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
            assert_eq!(body["status"], expected_status.as_u16());
            assert_eq!(body["message"], expected_message);
        }

        let target = Request::builder()
            .method(Method::PUT)
            .uri("/bob/prod")
            .body(axum::body::Body::from(r#"{"acl_upload":[":ANONYMOUS:"]}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(target).await.unwrap().status(),
            StatusCode::CREATED
        );
        let push = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev")
            .body(axum::body::Body::from(
                r#"{"name":"demo","version":"1.0.0","targetindex":"bob/prod"}"#,
            ))
            .unwrap();
        let response = app.oneshot(push).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["status"], 404);
        assert_eq!(body["message"], "no release/files found for demo 1.0.0");
    }

    #[tokio::test]
    async fn internal_push_returns_not_found_for_unknown_source_index() {
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir: std::env::temp_dir().join(format!(
                "devpi-rs-push-source-index-404-{}",
                std::process::id()
            )),
            sources: Vec::new(),
            outside_url: None,
        });

        let push = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev")
            .body(axum::body::Body::from(
                r#"{"name":"demo","version":"1.0.0","targetindex":"bob/prod"}"#,
            ))
            .unwrap();
        let response = app.oneshot(push).await.unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert!(body_text(response).await.contains("index not found"));
    }

    #[tokio::test]
    async fn internal_push_returns_not_found_for_unknown_target_index() {
        let (app, _package_dir) = test_router_with_index("push-target-index-404", "alice", "dev");
        let upload = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::from("sdist"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(upload).await.unwrap().status(),
            StatusCode::CREATED
        );

        let push = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev")
            .body(axum::body::Body::from(
                r#"{"name":"demo","version":"1.0.0","targetindex":"bob/prod"}"#,
            ))
            .unwrap();
        let response = app.oneshot(push).await.unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert!(body_text(response).await.contains("index not found"));
    }

    #[tokio::test]
    async fn accepts_multipart_doc_upload_on_stage_index() {
        let (app, _package_dir) = test_router_with_index("doc-upload", "alice", "dev");
        let boundary = "BOUNDARY";
        let body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\":action\"\r\n\r\ndoc_upload\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"name\"\r\n\r\nDemo_Pkg\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"version\"\r\n\r\n1.0.0\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"content\"; filename=\"docs.zip\"\r\nContent-Type: application/zip\r\n\r\ndocs\r\n\
             --{boundary}--\r\n"
        );
        let request = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev/")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(body))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let body = body_text(response).await;
        assert!(body.contains("\"filename\":\"demo-pkg-1.0.0.doc.zip\""));
        let get = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+f/demo-pkg/demo-pkg-1.0.0.doc.zip")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(get).await.unwrap();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&bytes[..], b"docs");
    }

    #[tokio::test]
    async fn accepts_legacy_version_file_put_and_get() {
        let (app, package_dir) = test_router_with_index("version-put", "alice", "dev");
        let put = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/demo/1.0.0/demo-1.0.0.tar.gz")
            .body(axum::body::Body::from("sdist"))
            .unwrap();

        let response = app.clone().oneshot(put).await.unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let upload_serial = response.headers().get("X-DEVPI-SERIAL").unwrap().clone();
        let get = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/demo/1.0.0/demo-1.0.0.tar.gz")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(get).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("X-DEVPI-SERIAL").unwrap(),
            &upload_serial
        );
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/x-tar"
        );
        assert_eq!(response.headers().get(header::CONTENT_LENGTH).unwrap(), "5");
        assert_eq!(
            response.headers().get(header::CONTENT_DISPOSITION).unwrap(),
            "attachment; filename=\"demo-1.0.0.tar.gz\""
        );
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            "max-age=365000000, immutable, public"
        );
        let last_modified = response
            .headers()
            .get(header::LAST_MODIFIED)
            .unwrap()
            .clone();
        httpdate::parse_http_date(last_modified.to_str().unwrap()).unwrap();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&bytes[..], b"sdist");
        let head = Request::builder()
            .method(Method::HEAD)
            .uri("/alice/dev/demo/1.0.0/demo-1.0.0.tar.gz")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(head).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("X-DEVPI-SERIAL").unwrap(),
            &upload_serial
        );
        assert_eq!(response.headers().get(header::CONTENT_LENGTH).unwrap(), "5");
        assert_eq!(
            response.headers().get(header::LAST_MODIFIED).unwrap(),
            &last_modified
        );
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(bytes.is_empty());
        let get_meta = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/demo/1.0.0/demo-1.0.0.tar.gz")
            .header(header::ACCEPT, "application/json")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(get_meta).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("X-DEVPI-SERIAL").unwrap(),
            &upload_serial
        );
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["type"], "releasefilemeta");
        assert_eq!(body["result"]["filename"], "demo-1.0.0.tar.gz");
        assert_eq!(
            body["result"]["url"],
            "/alice/dev/+f/demo/demo-1.0.0.tar.gz"
        );
        assert_eq!(body["result"]["bytes"], 5);
        assert_eq!(body["result"]["version"], "1.0.0");
        assert_eq!(
            body["result"]["hash_spec"],
            "sha256=714772a9f82b2aeb4fa5f7092d00fe4ac4c9cdeb6800840b6ed39ea64c4d785a"
        );
        assert_eq!(
            body["result"]["hashes"]["sha256"],
            "714772a9f82b2aeb4fa5f7092d00fe4ac4c9cdeb6800840b6ed39ea64c4d785a"
        );
        assert_eq!(
            std::fs::read(package_dir.join("alice/dev/demo/demo-1.0.0.tar.gz")).unwrap(),
            b"sdist"
        );
    }

    #[tokio::test]
    async fn package_file_response_guesses_content_type_from_filename() {
        let (app, _package_dir) = test_router_with_index("file-content-type", "alice", "dev");

        for (filename, expected) in [
            ("demo-1.0.0.zip", "application/zip"),
            ("demo-1.0.0.doc.zip", "application/zip"),
            ("demo-1.0.0-py3-none-any.whl", "application/octet-stream"),
        ] {
            let put = Request::builder()
                .method(Method::PUT)
                .uri(format!("/alice/dev/+f/demo/{filename}"))
                .body(axum::body::Body::from("content"))
                .unwrap();
            assert_eq!(
                app.clone().oneshot(put).await.unwrap().status(),
                StatusCode::CREATED
            );

            let get = Request::builder()
                .method(Method::GET)
                .uri(format!("/alice/dev/+f/demo/{filename}"))
                .body(axum::body::Body::empty())
                .unwrap();
            let response = app.clone().oneshot(get).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.headers().get(header::CONTENT_TYPE).unwrap(),
                expected
            );
        }
    }

    #[tokio::test]
    async fn plus_files_route_reads_stage_file_relpath() {
        let (app, _package_dir) = test_router_with_index("plus-files", "alice", "dev");
        let put = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::from("sdist"))
            .unwrap();
        let response = app.clone().oneshot(put).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let upload_serial = response.headers().get("X-DEVPI-SERIAL").unwrap().clone();

        let get = Request::builder()
            .method(Method::GET)
            .uri("/+files/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(get).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("X-DEVPI-SERIAL").unwrap(),
            &upload_serial
        );
        let etag = response
            .headers()
            .get(header::ETAG)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let last_modified = response
            .headers()
            .get(header::LAST_MODIFIED)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        httpdate::parse_http_date(&last_modified).unwrap();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&bytes[..], b"sdist");

        let cached_get = Request::builder()
            .method(Method::GET)
            .uri("/+files/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .header(header::IF_NONE_MATCH, etag.as_str())
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(cached_get).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(response.headers().get(header::ETAG).unwrap(), etag.as_str());
        assert_eq!(
            response.headers().get("X-DEVPI-SERIAL").unwrap(),
            &upload_serial
        );
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(bytes.is_empty());

        let weak_cached_get = Request::builder()
            .method(Method::GET)
            .uri("/+files/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .header(header::IF_NONE_MATCH, format!("W/{etag}"))
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(weak_cached_get).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(response.headers().get(header::ETAG).unwrap(), etag.as_str());
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(bytes.is_empty());

        let cached_by_modified = Request::builder()
            .method(Method::GET)
            .uri("/+files/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .header(header::IF_MODIFIED_SINCE, last_modified.as_str())
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(cached_by_modified).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(response.headers().get(header::ETAG).unwrap(), etag.as_str());
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(bytes.is_empty());

        let hashdir_get = Request::builder()
            .method(Method::GET)
            .uri("/+files/alice/dev/+f/714/772a9f82b2aeb/demo-1.0.0.tar.gz")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(hashdir_get).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("X-DEVPI-SERIAL").unwrap(),
            &upload_serial
        );
        httpdate::parse_http_date(
            response
                .headers()
                .get(header::LAST_MODIFIED)
                .unwrap()
                .to_str()
                .unwrap(),
        )
        .unwrap();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&bytes[..], b"sdist");

        let wrong_hash = Request::builder()
            .method(Method::GET)
            .uri("/+files/alice/dev/+f/000/0000000000000/demo-1.0.0.tar.gz")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(wrong_hash).await.unwrap().status(),
            StatusCode::NOT_FOUND
        );

        let unknown_shape = Request::builder()
            .method(Method::GET)
            .uri("/+files/alice/dev/not-f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.oneshot(unknown_shape).await.unwrap().status(),
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn deletes_stage_file_and_sidecars() {
        let (app, package_dir) = test_router_with_index("delete-file", "alice", "dev");
        let boundary = "BOUNDARY";
        let body = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\":action\"\r\n\r\nfile_upload\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"name\"\r\n\r\ndemo\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"version\"\r\n\r\n1.0.0\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"summary\"\r\n\r\nDemo package\r\n\
             --{boundary}\r\nContent-Disposition: form-data; name=\"content\"; filename=\"demo-1.0.0.tar.gz\"\r\nContent-Type: application/octet-stream\r\n\r\nsdist\r\n\
             --{boundary}--\r\n"
        );
        let upload = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev/")
            .header(
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(axum::body::Body::from(body))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(upload).await.unwrap().status(),
            StatusCode::CREATED
        );
        let tox = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::from(r#"{"envname":"py312","retcode":0}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(tox).await.unwrap().status(),
            StatusCode::OK
        );
        let delete = Request::builder()
            .method(Method::DELETE)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(delete).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(body_text(response).await.contains("\"deleted\":true"));
        assert!(
            !package_dir
                .join("alice/dev/demo/demo-1.0.0.tar.gz")
                .exists()
        );
        let missing = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(missing).await.unwrap().status(),
            StatusCode::GONE
        );
        let delete_again = Request::builder()
            .method(Method::DELETE)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(delete_again).await.unwrap().status(),
            StatusCode::GONE
        );
        let reupload = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::from("new"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(reupload).await.unwrap().status(),
            StatusCode::CREATED
        );
        let restored = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.oneshot(restored).await.unwrap().status(),
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn deletes_legacy_file_on_default_stage() {
        let package_dir = std::env::temp_dir().join(format!(
            "devpi-rs-delete-legacy-file-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir: package_dir.clone(),
            sources: Vec::new(),
            outside_url: None,
        });
        let put = Request::builder()
            .method(Method::PUT)
            .uri("/files/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::from("sdist"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(put).await.unwrap().status(),
            StatusCode::CREATED
        );
        let delete = Request::builder()
            .method(Method::DELETE)
            .uri("/files/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(delete).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            !package_dir
                .join("root/pypi/demo/demo-1.0.0.tar.gz")
                .exists()
        );
        let missing = Request::builder()
            .method(Method::GET)
            .uri("/files/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.oneshot(missing).await.unwrap().status(),
            StatusCode::GONE
        );
    }

    #[tokio::test]
    async fn package_file_routes_accept_trailing_slash_aliases() {
        let (app, _package_dir) = test_router_with_index("file-slash", "alice", "dev");

        let version_put = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/demo/1.0.0/demo-1.0.0.tar.gz/")
            .body(axum::body::Body::from("sdist"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(version_put).await.unwrap().status(),
            StatusCode::CREATED
        );
        let version_get = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/demo/1.0.0/demo-1.0.0.tar.gz/")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(version_get).await.unwrap().status(),
            StatusCode::OK
        );

        let stage_put = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/+f/slashpkg/slashpkg-1.0.0.tar.gz/")
            .body(axum::body::Body::from("pkg"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(stage_put).await.unwrap().status(),
            StatusCode::CREATED
        );
        let stage_tox = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev/+f/slashpkg/slashpkg-1.0.0.tar.gz/")
            .body(axum::body::Body::from(r#"{"envname":"py312","retcode":0}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(stage_tox).await.unwrap().status(),
            StatusCode::OK
        );
        let stage_tox_get = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+f/slashpkg/slashpkg-1.0.0.tar.gz.toxresult-0/")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(stage_tox_get).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["envname"], "py312");
        let stage_delete = Request::builder()
            .method(Method::DELETE)
            .uri("/alice/dev/+f/slashpkg/slashpkg-1.0.0.tar.gz/")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(stage_delete).await.unwrap().status(),
            StatusCode::OK
        );

        let legacy_put = Request::builder()
            .method(Method::PUT)
            .uri("/files/legacy/legacy-1.0.0.tar.gz/")
            .body(axum::body::Body::from("legacy"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(legacy_put).await.unwrap().status(),
            StatusCode::CREATED
        );
        let legacy_get = Request::builder()
            .method(Method::GET)
            .uri("/files/legacy/legacy-1.0.0.tar.gz/")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(legacy_get).await.unwrap().status(),
            StatusCode::OK
        );
        let legacy_delete = Request::builder()
            .method(Method::DELETE)
            .uri("/files/legacy/legacy-1.0.0.tar.gz/")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.oneshot(legacy_delete).await.unwrap().status(),
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn accepts_tox_result_post_on_stage_file() {
        let (app, _package_dir) = test_router_with_index("toxresult", "alice", "dev");
        let put = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/demo/1.0.0/demo-1.0.0.tar.gz")
            .body(axum::body::Body::from("sdist"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(put).await.unwrap().status(),
            StatusCode::CREATED
        );
        let tox = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::from(r#"{"envname":"py312","retcode":0}"#))
            .unwrap();

        let response = app.clone().oneshot(tox).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["type"], "toxresultpath");
        let tox_result_path = body["result"].as_str().unwrap();

        let tox_result = Request::builder()
            .method(Method::GET)
            .uri(tox_result_path)
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(tox_result).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["envname"], "py312");
        assert_eq!(body["retcode"], 0);

        let project = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/demo")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(project).await.unwrap();
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "2");
        let body = body_text(response).await;
        assert!(body.contains("\"toxresults\""));
        assert!(body.contains("\"envname\":\"py312\""));

        let delete_tox = Request::builder()
            .method(Method::DELETE)
            .uri(tox_result_path)
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.clone().oneshot(delete_tox).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "3");

        let deleted_tox_result = Request::builder()
            .method(Method::GET)
            .uri(tox_result_path)
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.clone()
                .oneshot(deleted_tox_result)
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
        let delete_tox_again = Request::builder()
            .method(Method::DELETE)
            .uri(tox_result_path)
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.clone()
                .oneshot(delete_tox_again)
                .await
                .unwrap()
                .status(),
            StatusCode::GONE
        );

        let project = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/demo")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(project).await.unwrap();
        assert_eq!(response.headers().get("X-DEVPI-SERIAL").unwrap(), "3");
        let body = body_text(response).await;
        assert!(!body.contains("\"envname\":\"py312\""));
    }

    #[tokio::test]
    async fn tox_result_upload_uses_outside_url_for_result_path() {
        let package_dir =
            std::env::temp_dir().join(format!("devpi-rs-tox-outside-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: Vec::new(),
            outside_url: Some("http://configured.example".to_string()),
        });
        let create_index = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev")
            .body(axum::body::Body::from(
                r#"{"acl_upload":[":ANONYMOUS:"],"acl_toxresult_upload":[":ANONYMOUS:"]}"#,
            ))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_index).await.unwrap().status(),
            StatusCode::CREATED
        );
        let put = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::from("sdist"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(put).await.unwrap().status(),
            StatusCode::CREATED
        );
        let tox = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .header("X-Outside-Url", "http://proxy.example/devpi")
            .body(axum::body::Body::from(r#"{"envname":"py312","retcode":0}"#))
            .unwrap();

        let response = app.oneshot(tox).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(
            body["result"],
            "http://proxy.example/devpi/alice/dev/+f/demo/demo-1.0.0.tar.gz.toxresult-0"
        );
    }

    #[tokio::test]
    async fn configured_index_enforces_acl_toxresult_upload_basic_auth() {
        let package_dir =
            std::env::temp_dir().join(format!("devpi-rs-acl-toxresult-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: Vec::new(),
            outside_url: None,
        });
        let create_user = Request::builder()
            .method(Method::PUT)
            .uri("/alice")
            .body(axum::body::Body::from(r#"{"password":"123"}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_user).await.unwrap().status(),
            StatusCode::CREATED
        );
        let create_index = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev")
            .body(axum::body::Body::from(
                r#"{"acl_toxresult_upload":["alice"]}"#,
            ))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_index).await.unwrap().status(),
            StatusCode::CREATED
        );
        let put = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/demo/1.0.0/demo-1.0.0.tar.gz")
            .header(header::AUTHORIZATION, "Basic YWxpY2U6MTIz")
            .body(axum::body::Body::from("sdist"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(put).await.unwrap().status(),
            StatusCode::CREATED
        );

        let unauthenticated = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::from(r#"{"envname":"py312","retcode":0}"#))
            .unwrap();
        let response = app.clone().oneshot(unauthenticated).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.headers().get(header::WWW_AUTHENTICATE).unwrap(),
            "Basic realm=\"devpi-rs\""
        );

        let invalid_basic = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .header(header::AUTHORIZATION, "Basic YWxpY2U6d3Jvbmc=")
            .body(axum::body::Body::from(r#"{"envname":"py312","retcode":0}"#))
            .unwrap();
        let response = app.clone().oneshot(invalid_basic).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.headers().get(header::WWW_AUTHENTICATE).unwrap(),
            "Basic realm=\"devpi-rs\""
        );

        let authenticated = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .header(header::AUTHORIZATION, "Basic YWxpY2U6MTIz")
            .body(axum::body::Body::from(r#"{"envname":"py312","retcode":0}"#))
            .unwrap();
        assert_eq!(
            app.oneshot(authenticated).await.unwrap().status(),
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn acl_toxresult_upload_authenticated_allows_any_authenticated_user() {
        let package_dir = std::env::temp_dir().join(format!(
            "devpi-rs-acl-auth-toxresult-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&package_dir);
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir,
            sources: Vec::new(),
            outside_url: None,
        });
        for user in ["alice", "bob"] {
            let create_user = Request::builder()
                .method(Method::PUT)
                .uri(format!("/{user}"))
                .body(axum::body::Body::from(r#"{"password":"123"}"#))
                .unwrap();
            assert_eq!(
                app.clone().oneshot(create_user).await.unwrap().status(),
                StatusCode::CREATED
            );
        }
        let create_index = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev")
            .body(axum::body::Body::from(
                r#"{"acl_toxresult_upload":[":AUTHENTICATED:"]}"#,
            ))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(create_index).await.unwrap().status(),
            StatusCode::CREATED
        );
        let put = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/demo/1.0.0/demo-1.0.0.tar.gz")
            .header(header::AUTHORIZATION, "Basic YWxpY2U6MTIz")
            .body(axum::body::Body::from("sdist"))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(put).await.unwrap().status(),
            StatusCode::CREATED
        );

        let unauthenticated = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::from(r#"{"envname":"py312","retcode":0}"#))
            .unwrap();
        assert_eq!(
            app.clone().oneshot(unauthenticated).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );

        let authenticated = Request::builder()
            .method(Method::POST)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .header(header::AUTHORIZATION, "Basic Ym9iOjEyMw==")
            .body(axum::body::Body::from(r#"{"envname":"py312","retcode":0}"#))
            .unwrap();
        assert_eq!(
            app.oneshot(authenticated).await.unwrap().status(),
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn rejects_legacy_version_file_put_with_mismatched_version() {
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir: std::env::temp_dir()
                .join(format!("devpi-rs-version-mismatch-{}", std::process::id())),
            sources: Vec::new(),
            outside_url: None,
        });
        let request = Request::builder()
            .method(Method::PUT)
            .uri("/alice/dev/demo/2.0.0/demo-1.0.0.tar.gz")
            .body(axum::body::Body::from("sdist"))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn deletes_version_through_curl_style_route() {
        let (app, package_dir) = test_router_with_index("delete-version", "alice", "dev");
        std::fs::create_dir_all(package_dir.join("alice/dev/demo")).unwrap();
        std::fs::write(package_dir.join("alice/dev/demo/demo-1.0.0.tar.gz"), b"one").unwrap();
        std::fs::write(package_dir.join("alice/dev/demo/demo-2.0.0.tar.gz"), b"two").unwrap();
        let request = Request::builder()
            .method(Method::DELETE)
            .uri("/alice/dev/demo/1.0.0/")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.clone().oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["status"], 200);
        assert_eq!(body["deleted"], 1);
        assert!(
            !package_dir
                .join("alice/dev/demo/demo-1.0.0.tar.gz")
                .exists()
        );
        assert!(
            package_dir
                .join("alice/dev/demo/demo-2.0.0.tar.gz")
                .exists()
        );
        let deleted_file = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+f/demo/demo-1.0.0.tar.gz")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.clone().oneshot(deleted_file).await.unwrap().status(),
            StatusCode::GONE
        );
        let retained_file = Request::builder()
            .method(Method::GET)
            .uri("/alice/dev/+f/demo/demo-2.0.0.tar.gz")
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(
            app.oneshot(retained_file).await.unwrap().status(),
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn deletes_project_through_curl_style_route() {
        let (app, package_dir) = test_router_with_index("delete-project", "alice", "dev");
        std::fs::create_dir_all(package_dir.join("alice/dev/demo")).unwrap();
        std::fs::write(package_dir.join("alice/dev/demo/demo-1.0.0.tar.gz"), b"one").unwrap();
        let request = Request::builder()
            .method(Method::DELETE)
            .uri("/alice/dev/demo/")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_str(&body_text(response).await).unwrap();
        assert_eq!(body["status"], 200);
        assert_eq!(body["deleted"], true);
        assert!(!package_dir.join("alice/dev/demo").exists());
    }

    #[tokio::test]
    async fn returns_not_found_for_missing_delete() {
        let app = router(AppConfig {
            listen: "127.0.0.1:0".to_string(),
            cache_dir: std::env::temp_dir(),
            package_dir: std::env::temp_dir()
                .join(format!("devpi-rs-delete-missing-{}", std::process::id())),
            sources: Vec::new(),
            outside_url: None,
        });
        let request = Request::builder()
            .method(Method::DELETE)
            .uri("/alice/dev/demo/1.0.0")
            .body(axum::body::Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
