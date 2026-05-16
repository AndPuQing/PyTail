use crate::cache::CachedLink;
use scraper::{Html, Selector};
use serde_json::{Value, json};
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
                .and_then(|segments| segments.last())
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

pub fn render_root_html(projects: &[String]) -> String {
    let mut html = String::from(
        "<!DOCTYPE html><html><head><meta name=\"pypi:repository-version\" content=\"1.0\"></head><body>\n",
    );
    for project in projects {
        html.push_str("<a href=\"/simple/");
        html.push_str(project);
        html.push_str("/\">");
        html.push_str(&escape_html(project));
        html.push_str("</a>\n");
    }
    html.push_str("</body></html>\n");
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
    let mut html = String::from(
        "<!DOCTYPE html><html><head><meta name=\"pypi:repository-version\" content=\"1.0\"></head><body>\n",
    );
    for link in links {
        html.push_str("<a href=\"");
        html.push_str(&escape_html_attr(&local_file_url(project, link)));
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
        html.push_str("</a>\n");
    }
    html.push_str("</body></html>\n");
    html
}

pub fn render_project_json(project: &str, links: &[CachedLink]) -> String {
    let files = links
        .iter()
        .map(|link| {
            let mut value = BTreeMap::<String, Value>::new();
            value.insert("filename".to_string(), Value::String(link.filename.clone()));
            value.insert(
                "url".to_string(),
                Value::String(local_file_url(project, link)),
            );
            if let Some(hash_name) = &link.hash_name
                && let Some(hash_value) = &link.hash_value
            {
                value.insert("hashes".to_string(), json!({ hash_name: hash_value }));
            }
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
    if link.blob_kind == "sha256" && link.blob_id.len() >= 16 {
        return format!(
            "/root/pypi/+f/{}/{}/{}",
            &link.blob_id[..3],
            &link.blob_id[3..16],
            link.filename
        );
    }
    format!("/root/pypi/+f/_url/{}/{}", link.blob_id, link.filename)
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
}
