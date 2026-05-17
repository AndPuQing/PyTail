use crate::cache::{CachedLink, ProjectSummary};
use scraper::{Html, Selector};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use url::Url;

const SIMPLE_JSON_MEDIA_TYPE: &str = "application/vnd.pypi.simple.v1+json";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimpleFormat {
    Html,
    Json,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedLink {
    pub filename: String,
    pub upstream_url: String,
    pub requires_python: Option<String>,
    pub yanked: Option<String>,
    pub gpg_sig: Option<bool>,
    pub dist_info_metadata: Option<String>,
    pub core_metadata: Option<String>,
    pub hash_name: Option<String>,
    pub hash_value: Option<String>,
}

pub fn normalize_project_name(name: &str) -> String {
    let mut normalized = String::with_capacity(name.len());
    let mut last_dash = false;
    for ch in name.chars() {
        let mapped = match ch {
            'A'..='Z' => ch.to_ascii_lowercase(),
            'a'..='z' | '0'..='9' => ch,
            '-' | '_' | '.' => '-',
            _ => '-',
        };
        if mapped == '-' {
            if !last_dash {
                normalized.push(mapped);
            }
            last_dash = true;
        } else {
            normalized.push(mapped);
            last_dash = false;
        }
    }
    normalized.trim_matches('-').to_string()
}

pub fn wants_json(accept: Option<&str>) -> bool {
    accept.is_some_and(|accept| {
        accept.contains(SIMPLE_JSON_MEDIA_TYPE) || accept.contains("application/json")
    })
}

pub fn json_media_type() -> &'static str {
    SIMPLE_JSON_MEDIA_TYPE
}

pub fn parse_project_links(body: &str, page_url: &Url) -> Vec<ParsedLink> {
    let document = Html::parse_document(body);
    let selector = Selector::parse("a").expect("valid selector");
    let mut links = Vec::new();
    for element in document.select(&selector) {
        let Some(href) = element.value().attr("href") else {
            continue;
        };
        let Ok(resolved) = page_url.join(href) else {
            continue;
        };
        let filename = element.text().collect::<String>().trim().to_string();
        let filename = if filename.is_empty() {
            resolved
                .path_segments()
                .and_then(|mut segments| segments.next_back())
                .unwrap_or("artifact")
                .to_string()
        } else {
            filename
        };
        let (hash_name, hash_value) = split_hash_fragment(resolved.fragment());
        let mut upstream_url = resolved.clone();
        upstream_url.set_fragment(None);
        let value = element.value();
        links.push(ParsedLink {
            filename,
            upstream_url: upstream_url.to_string(),
            requires_python: value.attr("data-requires-python").map(ToOwned::to_owned),
            yanked: value.attr("data-yanked").map(ToOwned::to_owned),
            gpg_sig: value.attr("data-gpg-sig").map(parse_bool_attr),
            dist_info_metadata: value.attr("data-dist-info-metadata").map(ToOwned::to_owned),
            core_metadata: value.attr("data-core-metadata").map(ToOwned::to_owned),
            hash_name,
            hash_value,
        });
    }
    links
}

pub fn parse_project_json_links(body: &str, page_url: &Url) -> Result<Vec<ParsedLink>, String> {
    let page: SimpleProjectJson = serde_json::from_str(body).map_err(|err| err.to_string())?;
    let mut links = Vec::with_capacity(page.files.len());
    for file in page.files {
        let resolved = page_url.join(&file.url).map_err(|err| err.to_string())?;
        let filename = file.filename.unwrap_or_else(|| {
            resolved
                .path_segments()
                .and_then(|mut segments| segments.next_back())
                .unwrap_or("artifact")
                .to_string()
        });
        let (fragment_hash_name, fragment_hash_value) = split_hash_fragment(resolved.fragment());
        let (hash_name, hash_value) = file
            .hashes
            .as_ref()
            .and_then(first_hash)
            .unwrap_or((fragment_hash_name, fragment_hash_value));
        let mut upstream_url = resolved.clone();
        upstream_url.set_fragment(None);
        links.push(ParsedLink {
            filename,
            upstream_url: upstream_url.to_string(),
            requires_python: file.requires_python,
            yanked: file.yanked.and_then(yanked_from_json),
            gpg_sig: file.gpg_sig,
            dist_info_metadata: file.dist_info_metadata.and_then(metadata_from_json),
            core_metadata: file.core_metadata.and_then(metadata_from_json),
            hash_name,
            hash_value,
        });
    }
    Ok(links)
}

#[derive(Debug, Deserialize)]
struct SimpleProjectJson {
    #[serde(default)]
    files: Vec<SimpleProjectFileJson>,
}

#[derive(Debug, Deserialize)]
struct SimpleProjectFileJson {
    filename: Option<String>,
    url: String,
    #[serde(default)]
    hashes: Option<Map<String, Value>>,
    #[serde(rename = "requires-python")]
    requires_python: Option<String>,
    yanked: Option<Value>,
    #[serde(rename = "gpg-sig")]
    gpg_sig: Option<bool>,
    #[serde(rename = "dist-info-metadata")]
    dist_info_metadata: Option<Value>,
    #[serde(rename = "core-metadata")]
    core_metadata: Option<Value>,
}

fn first_hash(hashes: &Map<String, Value>) -> Option<(Option<String>, Option<String>)> {
    if let Some(value) = hashes.get("sha256").and_then(Value::as_str) {
        return Some((Some("sha256".to_string()), Some(value.to_string())));
    }
    hashes.iter().find_map(|(name, value)| {
        value
            .as_str()
            .map(|value| (Some(name.clone()), Some(value.to_string())))
    })
}

fn yanked_from_json(value: Value) -> Option<String> {
    match value {
        Value::Bool(true) => Some(String::new()),
        Value::Bool(false) | Value::Null => None,
        Value::String(value) => Some(value),
        _ => None,
    }
}

fn metadata_from_json(value: Value) -> Option<String> {
    match value {
        Value::Bool(true) => Some("true".to_string()),
        Value::Bool(false) | Value::Null => None,
        Value::String(value) => Some(value),
        Value::Object(mut object) => {
            if let Some(value) = object.remove("sha256").and_then(|value| match value {
                Value::String(value) => Some(value),
                _ => None,
            }) {
                return Some(format!("sha256={value}"));
            }
            object.into_iter().find_map(|(name, value)| match value {
                Value::String(value) => Some(format!("{name}={value}")),
                _ => None,
            })
        }
        _ => None,
    }
}

pub fn render_root_html(projects: &[ProjectSummary]) -> String {
    let mut html = String::from(
        "<!DOCTYPE html>\n<html>\n  <head>\n    <meta name=\"pypi:repository-version\" content=\"1.0\">\n    <title>devpi</title>\n",
    );
    push_ui_head(&mut html);
    html.push_str("  </head>\n  <body>\n");
    html.push_str("    <div class=\"page-shell\">\n");
    push_header(&mut html, &[("root", "/simple/")]);
    html.push_str(
        "      <main id=\"content\" class=\"content\">\n        <h1 class=\"page-title\">root/pypi</h1>\n        <p class=\"subtitle\">Cached packages in root/pypi</p>\n        <p class=\"notice\">Package sizes count files already downloaded into the local cache.</p>\n",
    );
    if projects.is_empty() {
        html.push_str("        <div class=\"empty-state\">No cached projects yet. Search for a package name to fetch it from upstream.</div>\n");
    } else {
        html.push_str("        <div class=\"project-grid\">\n");
        for project in projects {
            html.push_str("          <a class=\"project-link\" href=\"");
            html.push_str(&escape_html_attr(&project.project));
            html.push_str("/\">");
            html.push_str(&escape_html(&project.project));
            html.push_str("<span class=\"project-stats\">");
            html.push_str(&project.cached_file_count.to_string());
            html.push('/');
            html.push_str(&project.file_count.to_string());
            html.push_str(" files cached · ");
            html.push_str(&format_size(project.cached_size_bytes));
            html.push_str("</span></a>\n");
        }
        html.push_str("        </div>\n");
    }
    html.push_str("      </main>\n");
    push_footer(&mut html);
    html.push_str("    </div>\n  </body>\n</html>\n");
    html
}

pub fn render_root_json(projects: &[String]) -> String {
    let projects = projects
        .iter()
        .map(|project| {
            json!({
                "name": project,
                "url": format!("/simple/{project}/"),
            })
        })
        .collect::<Vec<_>>();
    json!({
        "meta": {"api-version": "1.0"},
        "projects": projects,
    })
    .to_string()
}

pub fn render_project_html(project: &str, links: &[CachedLink]) -> String {
    render_project_html_with_file_base(project, links, "/root/pypi/+f")
}

pub fn render_project_html_with_file_base(
    project: &str,
    links: &[CachedLink],
    file_base_path: &str,
) -> String {
    let mut html = String::from(
        "<!DOCTYPE html>\n<html>\n  <head>\n    <meta name=\"pypi:repository-version\" content=\"1.0\">\n    <title>",
    );
    html.push_str(&escape_html(project));
    html.push_str(" - root/pypi</title>\n");
    push_ui_head(&mut html);
    html.push_str("  </head>\n  <body>\n");
    html.push_str("    <div class=\"page-shell\">\n");
    push_header(&mut html, &[("root", "/simple/"), ("pypi", "/simple/")]);
    html.push_str(
        "      <main id=\"content\" class=\"content\">\n        <h1 class=\"page-title\">",
    );
    html.push_str(&escape_html(project));
    html.push_str("</h1>\n        <p class=\"subtitle\">root/pypi package files</p>\n        <nav class=\"toolbar\"><a href=\"/simple/\">Index</a><a href=\"");
    html.push_str(&escape_html_attr(&format!("/simple/{project}/")));
    html.push_str("\">Simple API</a></nav>\n        <div class=\"file-table-wrap\">\n          <table class=\"file-table\">\n            <thead>\n              <tr><th>File</th><th>Size</th><th>Requires</th><th>Hash</th><th>Metadata</th></tr>\n            </thead>\n            <tbody>\n");
    for link in links {
        html.push_str("              <tr>\n                <td><a class=\"file-name\" href=\"");
        html.push_str(&escape_html_attr(&local_file_url_with_base(
            project,
            link,
            file_base_path,
        )));
        if let (Some(name), Some(value)) = (&link.hash_name, &link.hash_value) {
            html.push('#');
            html.push_str(name);
            html.push('=');
            html.push_str(value);
        }
        html.push('"');
        if let Some(value) = &link.requires_python {
            html.push_str(" data-requires-python=\"");
            html.push_str(&escape_html_attr(value));
            html.push('"');
        }
        if let Some(value) = &link.yanked {
            html.push_str(" data-yanked");
            if !value.is_empty() {
                html.push_str("=\"");
                html.push_str(&escape_html_attr(value));
                html.push('"');
            }
        }
        if let Some(value) = link.gpg_sig {
            html.push_str(" data-gpg-sig=\"");
            html.push_str(if value { "true" } else { "false" });
            html.push('"');
        }
        if let Some(value) = &link.dist_info_metadata {
            html.push_str(" data-dist-info-metadata=\"");
            html.push_str(&escape_html_attr(value));
            html.push('"');
        }
        if let Some(value) = &link.core_metadata {
            html.push_str(" data-core-metadata=\"");
            html.push_str(&escape_html_attr(value));
            html.push('"');
        }
        html.push('>');
        html.push_str(&escape_html(&link.filename));
        html.push_str("</a>");
        if let Some(value) = &link.yanked {
            html.push_str(" <span class=\"badge-warning\" title=\"");
            html.push_str(&escape_html_attr(value));
            html.push_str("\">yanked</span>");
        }
        html.push_str("</td>\n                <td>");
        match link.cached_size_bytes {
            Some(size) => html.push_str(&format_size(size)),
            None => html.push_str("<span class=\"muted\">not cached</span>"),
        }
        html.push_str("</td>\n                <td>");
        if let Some(value) = &link.requires_python {
            html.push_str(&escape_html(value));
        } else {
            html.push_str("&nbsp;");
        }
        html.push_str("</td>\n                <td>");
        if let (Some(name), Some(value)) = (&link.hash_name, &link.hash_value) {
            html.push_str("<dl class=\"meta-list\"><dt>");
            html.push_str(&escape_html(name));
            html.push_str("</dt><dd><code>");
            html.push_str(&escape_html(value));
            html.push_str("</code></dd></dl>");
        } else {
            html.push_str("&nbsp;");
        }
        html.push_str("</td>\n                <td>");
        push_metadata_cell(&mut html, link);
        html.push_str("</td>\n              </tr>\n");
    }
    if links.is_empty() {
        html.push_str(
            "              <tr><td colspan=\"5\">No files are cached for this project yet.</td></tr>\n",
        );
    }
    html.push_str("            </tbody>\n          </table>\n        </div>\n      </main>\n");
    push_footer(&mut html);
    html.push_str("    </div>\n  </body>\n</html>\n");
    html
}

pub fn render_project_json(project: &str, links: &[CachedLink]) -> String {
    render_project_json_with_file_base(project, links, "/root/pypi/+f")
}

pub fn render_project_json_with_file_base(
    project: &str,
    links: &[CachedLink],
    file_base_path: &str,
) -> String {
    let files = links
        .iter()
        .map(|link| {
            let mut value = BTreeMap::<String, Value>::new();
            value.insert("filename".to_string(), Value::String(link.filename.clone()));
            value.insert(
                "url".to_string(),
                Value::String(local_file_url_with_base(project, link, file_base_path)),
            );
            let hashes = if let Some(hash_name) = &link.hash_name
                && let Some(hash_value) = &link.hash_value
            {
                json!({ hash_name: hash_value })
            } else {
                json!({})
            };
            value.insert("hashes".to_string(), hashes);
            if let Some(value_text) = &link.requires_python {
                value.insert(
                    "requires-python".to_string(),
                    Value::String(value_text.clone()),
                );
            }
            if let Some(yanked) = &link.yanked {
                value.insert("yanked".to_string(), yanked_json(yanked));
            }
            if let Some(gpg_sig) = link.gpg_sig {
                value.insert("gpg-sig".to_string(), Value::Bool(gpg_sig));
            }
            if let Some(value_text) = &link.dist_info_metadata {
                value.insert("dist-info-metadata".to_string(), metadata_json(value_text));
            }
            if let Some(value_text) = &link.core_metadata {
                value.insert("core-metadata".to_string(), metadata_json(value_text));
            }
            Value::Object(value.into_iter().collect())
        })
        .collect::<Vec<_>>();
    json!({
        "meta": {"api-version": "1.0"},
        "name": project,
        "files": files,
    })
    .to_string()
}

pub fn cached_links(project: &str, links: Vec<ParsedLink>) -> Vec<CachedLink> {
    let _ = normalize_project_name(project);
    links
        .into_iter()
        .map(|link| CachedLink {
            filename: link.filename,
            upstream_url: link.upstream_url,
            blob_kind: String::new(),
            blob_id: String::new(),
            cached_size_bytes: None,
            requires_python: link.requires_python,
            yanked: link.yanked,
            gpg_sig: link.gpg_sig,
            dist_info_metadata: link.dist_info_metadata,
            core_metadata: link.core_metadata,
            hash_name: link.hash_name,
            hash_value: link.hash_value,
        })
        .collect()
}

pub fn local_file_url(_project: &str, link: &CachedLink) -> String {
    local_file_url_with_base(_project, link, "/root/pypi/+f")
}

pub fn local_file_url_with_base(_project: &str, link: &CachedLink, file_base_path: &str) -> String {
    let file_base_path = file_base_path.trim_end_matches('/');
    if link.blob_kind == "sha256" && link.blob_id.len() >= 16 {
        return format!(
            "{}/{}/{}/{}",
            file_base_path,
            &link.blob_id[..3],
            &link.blob_id[3..16],
            link.filename
        );
    }
    format!("{file_base_path}/_url/{}/{}", link.blob_id, link.filename)
}

fn split_hash_fragment(fragment: Option<&str>) -> (Option<String>, Option<String>) {
    let Some(fragment) = fragment else {
        return (None, None);
    };
    let Some((name, value)) = fragment.split_once('=') else {
        return (None, None);
    };
    if name.is_empty() || value.is_empty() {
        return (None, None);
    }
    (Some(name.to_string()), Some(value.to_string()))
}

fn parse_bool_attr(value: &str) -> bool {
    value.eq_ignore_ascii_case("true")
}

fn yanked_json(value: &str) -> Value {
    if value.is_empty() {
        Value::Bool(true)
    } else {
        Value::String(value.to_string())
    }
}

fn metadata_json(value: &str) -> Value {
    if value.eq_ignore_ascii_case("true") {
        return Value::Bool(true);
    }
    if let Some((name, digest)) = value.split_once('=')
        && !name.is_empty()
        && !digest.is_empty()
    {
        return json!({ name: digest });
    }
    Value::String(value.to_string())
}

fn format_size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else if value >= 10.0 {
        format!("{value:.0} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn push_ui_head(html: &mut String) {
    html.push_str("    <style>");
    html.push_str(include_str!("generated/ui.css"));
    html.push_str("</style>\n    <script>");
    html.push_str(include_str!("generated/ui.js"));
    html.push_str("</script>\n");
}

fn push_header(html: &mut String, breadcrumbs: &[(&str, &str)]) {
    html.push_str("      <header class=\"topbar\">\n        <div class=\"search-row\">\n          <a class=\"brand\" href=\"/simple/\">devpi</a>\n          <form id=\"search\" class=\"search-form\" action=\"/simple/\" method=\"get\">\n            <input class=\"search-input\" type=\"text\" name=\"q\" placeholder=\"package name\" autocomplete=\"off\">\n            <input class=\"search-button\" type=\"submit\" value=\"Search\">\n          </form>\n        </div>\n        <nav class=\"breadcrumbs\">\n");
    for (label, href) in breadcrumbs {
        html.push_str("          <a href=\"");
        html.push_str(&escape_html_attr(href));
        html.push_str("\">");
        html.push_str(&escape_html(label));
        html.push_str("</a><span class=\"breadcrumb-separator\">/</span>\n");
    }
    html.push_str("        </nav>\n      </header>\n");
}

fn push_footer(html: &mut String) {
    html.push_str("      <footer class=\"footer\">pytail simple package index</footer>\n");
}

fn push_metadata_cell(html: &mut String, link: &CachedLink) {
    let mut wrote = false;
    if let Some(value) = link.gpg_sig {
        html.push_str("<dl class=\"meta-list\"><dt>gpg-sig</dt><dd>");
        html.push_str(if value { "true" } else { "false" });
        html.push_str("</dd></dl>");
        wrote = true;
    }
    if let Some(value) = &link.dist_info_metadata {
        html.push_str("<dl class=\"meta-list\"><dt>dist-info</dt><dd>");
        html.push_str(&escape_html(value));
        html.push_str("</dd></dl>");
        wrote = true;
    }
    if let Some(value) = &link.core_metadata {
        html.push_str("<dl class=\"meta-list\"><dt>core</dt><dd>");
        html.push_str(&escape_html(value));
        html.push_str("</dd></dl>");
        wrote = true;
    }
    if !wrote {
        html.push_str("&nbsp;");
    }
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn escape_html_attr(value: &str) -> String {
    escape_html(value).replace('"', "&quot;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_project_names_like_simple_api() {
        assert_eq!(normalize_project_name("Requests"), "requests");
        assert_eq!(normalize_project_name("my_pkg.demo"), "my-pkg-demo");
    }

    #[test]
    fn parses_project_links_with_hash_and_attributes() {
        let page_url = Url::parse("https://example.test/simple/demo/").unwrap();
        let body = r#"
            <html><body>
            <a href="../../packages/demo-1.0.whl#sha256=abcd"
               data-requires-python="&gt;=3.10"
               data-yanked
               data-gpg-sig="true">demo-1.0.whl</a>
            </body></html>
        "#;

        let links = parse_project_links(body, &page_url);
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].filename, "demo-1.0.whl");
        assert_eq!(links[0].hash_name.as_deref(), Some("sha256"));
        assert_eq!(links[0].hash_value.as_deref(), Some("abcd"));
        assert_eq!(links[0].requires_python.as_deref(), Some(">=3.10"));
        assert_eq!(links[0].yanked.as_deref(), Some(""));
        assert_eq!(links[0].gpg_sig, Some(true));
    }

    #[test]
    fn parses_project_json_links_with_metadata() {
        let page_url = Url::parse("https://example.test/simple/demo/").unwrap();
        let body = r#"
        {
          "meta": {"api-version": "1.0"},
          "name": "demo",
          "files": [
            {
              "filename": "demo-1.0.whl",
              "url": "../../packages/demo-1.0.whl",
              "hashes": {"sha256": "abcd"},
              "requires-python": ">=3.10",
              "yanked": "bad release",
              "gpg-sig": true,
              "dist-info-metadata": {"sha256": "metaabcd"}
            }
          ]
        }
        "#;

        let links = parse_project_json_links(body, &page_url).unwrap();
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].filename, "demo-1.0.whl");
        assert_eq!(
            links[0].upstream_url,
            "https://example.test/packages/demo-1.0.whl"
        );
        assert_eq!(links[0].hash_name.as_deref(), Some("sha256"));
        assert_eq!(links[0].hash_value.as_deref(), Some("abcd"));
        assert_eq!(links[0].requires_python.as_deref(), Some(">=3.10"));
        assert_eq!(links[0].yanked.as_deref(), Some("bad release"));
        assert_eq!(links[0].gpg_sig, Some(true));
        assert_eq!(
            links[0].dist_info_metadata.as_deref(),
            Some("sha256=metaabcd")
        );
    }

    #[test]
    fn renders_root_html_like_simple_index_listing() {
        let html = render_root_html(&[
            ProjectSummary {
                project: "demo".to_string(),
                file_count: 2,
                cached_file_count: 1,
                cached_size_bytes: 2048,
            },
            ProjectSummary {
                project: "my_pkg".to_string(),
                file_count: 0,
                cached_file_count: 0,
                cached_size_bytes: 0,
            },
        ]);

        assert!(html.contains("<title>devpi</title>"));
        assert!(html.contains("<style>"));
        assert!(html.contains("<script>"));
        assert!(html.contains("<form id=\"search\" class=\"search-form\""));
        assert!(html.contains("<h1 class=\"page-title\">root/pypi</h1>"));
        assert!(html.contains("<a class=\"project-link\" href=\"demo/\">demo"));
        assert!(html.contains("1/2 files cached"));
        assert!(html.contains("2.0 KB"));
        assert!(html.contains("<a class=\"project-link\" href=\"my_pkg/\">my_pkg"));
        assert!(!html.contains("href=\"/simple/demo/\""));
    }

    #[test]
    fn renders_project_html_as_devpi_style_file_table() {
        let links = vec![CachedLink {
            filename: "demo-1.0.whl".to_string(),
            upstream_url: "https://files.example/demo-1.0.whl".to_string(),
            blob_kind: "sha256".to_string(),
            blob_id: "abcd000000000000000000000000000000000000000000000000000000000000".to_string(),
            cached_size_bytes: Some(1536),
            requires_python: None,
            yanked: None,
            gpg_sig: None,
            dist_info_metadata: None,
            core_metadata: None,
            hash_name: Some("sha256".to_string()),
            hash_value: Some(
                "abcd000000000000000000000000000000000000000000000000000000000000".to_string(),
            ),
        }];

        let html = render_project_html("demo", &links);

        assert!(html.contains("<title>demo - root/pypi</title>"));
        assert!(html.contains("<h1 class=\"page-title\">demo</h1>"));
        assert!(html.contains("<table class=\"file-table\">"));
        assert!(html.contains("demo-1.0.whl</a>"));
        assert!(html.contains("1.5 KB"));
        assert!(html.contains("<dt>sha256</dt>"));
    }

    #[test]
    fn renders_project_json_with_empty_hashes_when_digest_is_unknown() {
        let links = vec![CachedLink {
            filename: "demo-1.0.whl".to_string(),
            upstream_url: "https://files.example/demo-1.0.whl".to_string(),
            blob_kind: "url".to_string(),
            blob_id: "demo-1.0.whl".to_string(),
            cached_size_bytes: None,
            requires_python: None,
            yanked: None,
            gpg_sig: None,
            dist_info_metadata: None,
            core_metadata: None,
            hash_name: None,
            hash_value: None,
        }];

        let body = render_project_json("demo", &links);
        let json: Value = serde_json::from_str(&body).unwrap();

        assert_eq!(json["files"][0]["hashes"], json!({}));
    }
}
