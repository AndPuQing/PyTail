use std::collections::{BTreeMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourcePage {
    pub source: String,
    pub body: String,
    pub page_url: Option<String>,
    pub pypi_last_serial: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Link {
    href: String,
    text: String,
    requires_python: Option<String>,
    yanked: Option<String>,
    dist_info_metadata: Option<String>,
    core_metadata: Option<String>,
    gpg_sig: Option<String>,
}

pub fn normalize_project_name(name: &str) -> String {
    let mut normalized = String::new();
    let mut previous_dash = false;

    for ch in name.chars() {
        if ch == '-' || ch == '_' || ch == '.' {
            if !previous_dash {
                normalized.push('-');
                previous_dash = true;
            }
        } else {
            for lower in ch.to_lowercase() {
                normalized.push(lower);
            }
            previous_dash = false;
        }
    }

    normalized.trim_matches('-').to_string()
}

pub fn source_project_url(base_url: &str, project: &str) -> String {
    format!(
        "{}{}/",
        ensure_trailing_slash(base_url),
        normalize_project_name(project)
    )
}

pub fn merge_project_page(project: &str, pages: &[SourcePage]) -> String {
    let normalized = normalize_project_name(project);
    let mut seen = HashSet::new();
    let mut links = Vec::new();

    for page in pages {
        for link in parse_links(&page.body) {
            let href = page
                .page_url
                .as_deref()
                .map(|page_url| resolve_href(page_url, &link.href))
                .unwrap_or_else(|| link.href.clone());
            let key = href.clone();
            if seen.insert(key) {
                links.push((
                    page.source.as_str(),
                    Link {
                        href,
                        text: link.text,
                        requires_python: link.requires_python,
                        yanked: link.yanked,
                        dist_info_metadata: link.dist_info_metadata,
                        core_metadata: link.core_metadata,
                        gpg_sig: link.gpg_sig,
                    },
                ));
            }
        }
    }

    let mut html = String::new();
    html.push_str("<!doctype html>\n<html><head><meta charset=\"utf-8\"><title>");
    html.push_str(&escape_html(&normalized));
    html.push_str("</title></head><body>\n<h1>Links for ");
    html.push_str(&escape_html(&normalized));
    html.push_str("</h1>\n");

    for (source, link) in links {
        html.push_str("<a data-source=\"");
        html.push_str(&escape_attr(source));
        html.push_str("\" href=\"");
        html.push_str(&escape_attr(&link.href));
        append_link_attrs(&mut html, &link);
        html.push_str("\">");
        html.push_str(&escape_html(&link.text));
        html.push_str("</a><br>\n");
    }

    html.push_str("</body></html>\n");
    html
}

pub fn rewrite_project_page_hrefs<F>(page: &SourcePage, mut map_href: F) -> SourcePage
where
    F: FnMut(&str) -> String,
{
    let mut body = String::new();
    body.push_str("<!doctype html>\n<html><body>\n");

    for link in parse_links(&page.body) {
        let href = page
            .page_url
            .as_deref()
            .map(|page_url| resolve_href(page_url, &link.href))
            .unwrap_or_else(|| link.href.clone());
        let href = map_href(&href);
        body.push_str("<a href=\"");
        body.push_str(&escape_attr(&href));
        append_link_attrs(&mut body, &link);
        body.push_str("\">");
        body.push_str(&escape_html(&link.text));
        body.push_str("</a><br>\n");
    }

    body.push_str("</body></html>\n");
    SourcePage {
        source: page.source.clone(),
        body,
        page_url: None,
        pypi_last_serial: page.pypi_last_serial,
    }
}

pub fn merge_root_page(pages: &[SourcePage]) -> String {
    let mut projects = BTreeMap::new();

    for page in pages {
        for link in parse_links(&page.body) {
            let label = if link.text.trim().is_empty() {
                link.href.trim_matches('/').to_string()
            } else {
                link.text
            };
            let normalized = normalize_project_name(&label);
            if !normalized.is_empty() {
                projects.entry(normalized).or_insert(page.source.clone());
            }
        }
    }

    let mut html = String::new();
    html.push_str(
        "<!doctype html>\n<html><head><meta charset=\"utf-8\"><title>simple</title></head><body>\n",
    );
    html.push_str("<h1>Simple index</h1>\n");
    for (project, source) in projects {
        html.push_str("<a data-source=\"");
        html.push_str(&escape_attr(&source));
        html.push_str("\" href=\"");
        html.push_str(&escape_attr(&project));
        html.push_str("/\">");
        html.push_str(&escape_html(&project));
        html.push_str("</a><br>\n");
    }
    html.push_str("</body></html>\n");
    html
}

pub fn merge_project_json(project: &str, pages: &[SourcePage]) -> String {
    let normalized = normalize_project_name(project);
    let mut seen = HashSet::new();
    let mut files = Vec::new();

    for page in pages {
        for link in parse_links(&page.body) {
            let href = page
                .page_url
                .as_deref()
                .map(|page_url| resolve_href(page_url, &link.href))
                .unwrap_or_else(|| link.href.clone());
            if seen.insert(href.clone()) {
                let (url, hashes) = split_hash_fragment(&href);
                let filename = link_filename(&link.text, url);
                let mut file = serde_json::json!({
                    "filename": filename,
                    "url": url,
                });
                if let Some((hash_name, hash_value)) = hashes {
                    file["hashes"] = serde_json::json!({hash_name: hash_value});
                }
                if let Some(requires_python) = link.requires_python {
                    file["requires-python"] = serde_json::json!(requires_python);
                }
                if let Some(yanked) = link.yanked {
                    file["yanked"] = if yanked.is_empty() {
                        serde_json::json!(true)
                    } else {
                        serde_json::json!(yanked)
                    };
                }
                if let Some(value) = link.core_metadata {
                    file["core-metadata"] = metadata_availability_json(&value);
                } else if let Some(value) = &link.dist_info_metadata {
                    file["core-metadata"] = metadata_availability_json(value);
                }
                if let Some(value) = link.dist_info_metadata {
                    file["dist-info-metadata"] = metadata_availability_json(&value);
                }
                if let Some(value) = link.gpg_sig {
                    file["gpg-sig"] = bool_attr_json(&value);
                }
                files.push(file);
            }
        }
    }

    format!(
        "{}\n",
        serde_json::json!({
            "meta": {"api-version": "1.0"},
            "name": normalized,
            "files": files,
        })
    )
}

pub fn merge_root_json(pages: &[SourcePage]) -> String {
    let mut projects = BTreeMap::new();

    for page in pages {
        for link in parse_links(&page.body) {
            let label = if link.text.trim().is_empty() {
                link.href.trim_matches('/').to_string()
            } else {
                link.text
            };
            let normalized = normalize_project_name(&label);
            if !normalized.is_empty() {
                projects.entry(normalized).or_insert(());
            }
        }
    }

    let projects = projects
        .into_keys()
        .map(|name| serde_json::json!({"name": name}))
        .collect::<Vec<_>>();
    format!(
        "{}\n",
        serde_json::json!({
            "meta": {"api-version": "1.0"},
            "projects": projects,
        })
    )
}

fn parse_links(html: &str) -> Vec<Link> {
    let mut links = Vec::new();
    let mut rest = html;

    while let Some(anchor_start) = rest.to_ascii_lowercase().find("<a") {
        rest = &rest[anchor_start + 2..];
        let Some(tag_end) = rest.find('>') else {
            break;
        };
        let tag = &rest[..tag_end];
        rest = &rest[tag_end + 1..];
        let Some(href) = find_attr(tag, "href") else {
            continue;
        };
        let lower_rest = rest.to_ascii_lowercase();
        let text = if let Some(anchor_end) = lower_rest.find("</a>") {
            let text = strip_tags(&rest[..anchor_end]);
            rest = &rest[anchor_end + 4..];
            text
        } else {
            String::new()
        };
        links.push(Link {
            href,
            text: decode_basic_entities(text.trim()),
            requires_python: find_attr(tag, "data-requires-python"),
            yanked: find_attr(tag, "data-yanked"),
            dist_info_metadata: find_attr(tag, "data-dist-info-metadata"),
            core_metadata: find_attr(tag, "data-core-metadata"),
            gpg_sig: find_attr(tag, "data-gpg-sig"),
        });
    }

    links
}

fn append_link_attrs(html: &mut String, link: &Link) {
    if let Some(value) = &link.requires_python {
        html.push_str("\" data-requires-python=\"");
        html.push_str(&escape_attr(value));
    }
    if let Some(value) = &link.yanked {
        html.push_str("\" data-yanked=\"");
        html.push_str(&escape_attr(value));
    }
    if let Some(value) = &link.dist_info_metadata {
        html.push_str("\" data-dist-info-metadata=\"");
        html.push_str(&escape_attr(value));
    }
    if let Some(value) = &link.core_metadata {
        html.push_str("\" data-core-metadata=\"");
        html.push_str(&escape_attr(value));
    }
    if let Some(value) = &link.gpg_sig {
        html.push_str("\" data-gpg-sig=\"");
        html.push_str(&escape_attr(value));
    }
}

fn metadata_availability_json(value: &str) -> serde_json::Value {
    if let Some((hash_name, hash_value)) = value.split_once('=')
        && !hash_name.is_empty()
        && !hash_value.is_empty()
    {
        return serde_json::json!({hash_name: hash_value});
    }
    bool_attr_json(value)
}

fn bool_attr_json(value: &str) -> serde_json::Value {
    if value.eq_ignore_ascii_case("false") {
        serde_json::json!(false)
    } else {
        serde_json::json!(true)
    }
}

fn find_attr(tag: &str, attr: &str) -> Option<String> {
    let lower = tag.to_ascii_lowercase();
    let mut offset = 0;

    while let Some(pos) = lower[offset..].find(attr) {
        let attr_start = offset + pos;
        let after_attr = attr_start + attr.len();
        let before_ok = attr_start == 0
            || tag[..attr_start]
                .chars()
                .next_back()
                .map(char::is_whitespace)
                .unwrap_or(false);
        let raw_after = &tag[after_attr..];
        let after_ok = raw_after
            .chars()
            .next()
            .map(|ch| ch.is_whitespace() || ch == '=')
            .unwrap_or(true);
        if !before_ok || !after_ok {
            offset = after_attr;
            continue;
        }
        let original_after = raw_after.trim_start();
        if !original_after.starts_with('=') {
            return Some(String::new());
        }
        let value = original_after[1..].trim_start();
        if let Some(stripped) = value.strip_prefix('"') {
            return stripped.split('"').next().map(decode_basic_entities);
        }
        if let Some(stripped) = value.strip_prefix('\'') {
            return stripped.split('\'').next().map(decode_basic_entities);
        }
        return value
            .split(char::is_whitespace)
            .next()
            .map(decode_basic_entities);
    }

    None
}

fn strip_tags(value: &str) -> String {
    let mut out = String::new();
    let mut in_tag = false;

    for ch in value.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(ch),
            _ => {}
        }
    }

    out
}

fn ensure_trailing_slash(value: &str) -> String {
    if value.ends_with('/') {
        value.to_string()
    } else {
        format!("{value}/")
    }
}

fn resolve_href(page_url: &str, href: &str) -> String {
    if href.contains("://") || href.starts_with("mailto:") {
        return href.to_string();
    }
    if href.starts_with('/') {
        if let Some(origin_end) = origin_end(page_url) {
            return format!("{}{}", &page_url[..origin_end], href);
        }
        return href.to_string();
    }
    if href.starts_with('#') || href.starts_with('?') {
        return format!("{page_url}{href}");
    }

    let base = if page_url.ends_with('/') {
        page_url.to_string()
    } else if let Some((prefix, _)) = page_url.rsplit_once('/') {
        format!("{prefix}/")
    } else {
        String::new()
    };
    collapse_dot_segments(&format!("{base}{href}"))
}

fn split_hash_fragment(url: &str) -> (&str, Option<(&str, &str)>) {
    let Some((url, fragment)) = url.split_once('#') else {
        return (url, None);
    };
    let Some((name, value)) = fragment.split_once('=') else {
        return (url, None);
    };
    if name.is_empty() || value.is_empty() {
        return (url, None);
    }
    (url, Some((name, value)))
}

fn link_filename<'a>(text: &'a str, url: &'a str) -> &'a str {
    let text = text.trim();
    if !text.is_empty() {
        return text;
    }
    url.split('?')
        .next()
        .unwrap_or(url)
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or_default()
}

fn origin_end(url: &str) -> Option<usize> {
    let scheme_end = url.find("://")? + 3;
    let path_start = url[scheme_end..]
        .find('/')
        .map(|pos| scheme_end + pos)
        .unwrap_or(url.len());
    Some(path_start)
}

fn collapse_dot_segments(url: &str) -> String {
    let Some(origin_end) = origin_end(url) else {
        return url.to_string();
    };
    let origin = &url[..origin_end];
    let path = &url[origin_end..];
    let mut parts = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            _ => parts.push(part),
        }
    }
    format!("{origin}/{}", parts.join("/"))
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn escape_attr(value: &str) -> String {
    escape_html(value).replace('"', "&quot;")
}

fn decode_basic_entities(value: &str) -> String {
    value
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_pep_503_names() {
        assert_eq!(normalize_project_name("My_Package.Name"), "my-package-name");
        assert_eq!(normalize_project_name("a---b___c...d"), "a-b-c-d");
    }

    #[test]
    fn merges_project_pages_in_source_order_and_dedupes() {
        let merged = merge_project_page(
            "Demo",
            &[
                SourcePage {
                    source: "corp".to_string(),
                    body: r#"<a href="https://corp/demo-1.whl">demo-1.whl</a>"#.to_string(),
                    page_url: Some("http://corp/simple/demo/".to_string()),
                    pypi_last_serial: None,
                },
                SourcePage {
                    source: "pypi".to_string(),
                    body: r#"
                        <a href="https://corp/demo-1.whl">duplicate</a>
                        <a href="https://files/demo-2.whl">demo-2.whl</a>
                    "#
                    .to_string(),
                    page_url: Some("https://pypi.org/simple/demo/".to_string()),
                    pypi_last_serial: None,
                },
            ],
        );

        let first = merged.find("https://corp/demo-1.whl").unwrap();
        let second = merged.find("https://files/demo-2.whl").unwrap();
        assert!(first < second);
        assert_eq!(merged.matches("https://corp/demo-1.whl").count(), 1);
        assert!(merged.contains(r#"data-source="corp""#));
        assert!(merged.contains(r#"data-source="pypi""#));
    }

    #[test]
    fn merges_root_pages_by_normalized_project() {
        let merged = merge_root_page(&[
            SourcePage {
                source: "corp".to_string(),
                body: r#"<a href="Demo_Pkg/">Demo_Pkg</a>"#.to_string(),
                page_url: Some("http://corp/simple/".to_string()),
                pypi_last_serial: None,
            },
            SourcePage {
                source: "pypi".to_string(),
                body: r#"<a href="demo-pkg/">demo-pkg</a>"#.to_string(),
                page_url: Some("https://pypi.org/simple/".to_string()),
                pypi_last_serial: None,
            },
        ]);

        assert_eq!(merged.matches(r#"href="demo-pkg/""#).count(), 1);
        assert!(merged.contains(r#"data-source="corp""#));
    }

    #[test]
    fn resolves_relative_file_links_against_upstream_page() {
        let merged = merge_project_page(
            "demo",
            &[SourcePage {
                source: "corp".to_string(),
                body: r#"
                    <a href="../../files/demo-1.tar.gz">demo-1.tar.gz</a>
                    <a href="/packages/demo-2.whl">demo-2.whl</a>
                "#
                .to_string(),
                page_url: Some("http://corp.local/simple/demo/".to_string()),
                pypi_last_serial: None,
            }],
        );

        assert!(merged.contains("http://corp.local/files/demo-1.tar.gz"));
        assert!(merged.contains("http://corp.local/packages/demo-2.whl"));
    }

    #[test]
    fn project_json_derives_empty_link_text_filename_from_url() {
        let pages = [SourcePage {
            source: "corp".to_string(),
            body: r#"<a href="../../files/demo-1.0.0.tar.gz?download=1#sha256=abc"></a>"#
                .to_string(),
            page_url: Some("http://corp.local/simple/demo/".to_string()),
            pypi_last_serial: None,
        }];

        let json: serde_json::Value =
            serde_json::from_str(&merge_project_json("demo", &pages)).expect("valid simple json");
        assert_eq!(json["files"][0]["filename"], "demo-1.0.0.tar.gz");
        assert_eq!(
            json["files"][0]["url"],
            "http://corp.local/files/demo-1.0.0.tar.gz?download=1"
        );
        assert_eq!(json["files"][0]["hashes"]["sha256"], "abc");
    }

    #[test]
    fn preserves_pep_503_link_metadata_in_html_and_json() {
        let pages = [SourcePage {
            source: "pypi".to_string(),
            body: r#"
                <a href="demo-1.whl#sha256=abc123"
                   data-requires-python="&gt;=3.9"
                   data-yanked="bad build"
                   data-dist-info-metadata="true"
                   data-core-metadata="sha256=meta123"
                   data-gpg-sig="true">demo-1.whl</a>
            "#
            .to_string(),
            page_url: Some("https://pypi.org/simple/demo/".to_string()),
            pypi_last_serial: None,
        }];

        let html = merge_project_page("demo", &pages);
        assert!(html.contains("demo-1.whl#sha256=abc123"));
        assert!(html.contains(r#"data-requires-python="&gt;=3.9""#));
        assert!(html.contains(r#"data-yanked="bad build""#));
        assert!(html.contains(r#"data-dist-info-metadata="true""#));
        assert!(html.contains(r#"data-core-metadata="sha256=meta123""#));
        assert!(html.contains(r#"data-gpg-sig="true""#));

        let json: serde_json::Value =
            serde_json::from_str(&merge_project_json("demo", &pages)).expect("valid simple json");
        let file = &json["files"][0];
        assert_eq!(file["url"], "https://pypi.org/simple/demo/demo-1.whl");
        assert_eq!(file["hashes"]["sha256"], "abc123");
        assert_eq!(file["requires-python"], ">=3.9");
        assert_eq!(file["yanked"], "bad build");
        assert_eq!(file["core-metadata"]["sha256"], "meta123");
        assert_eq!(file["dist-info-metadata"], true);
        assert_eq!(file["gpg-sig"], true);
    }

    #[test]
    fn rewrites_project_page_hrefs_after_resolving_relative_links() {
        let page = SourcePage {
            source: "pypi".to_string(),
            body: r#"
                <a href="demo-1.whl#sha256=abc123" data-requires-python="&gt;=3.9">demo-1.whl</a>
            "#
            .to_string(),
            page_url: Some("https://pypi.org/simple/demo/".to_string()),
            pypi_last_serial: None,
        };

        let rewritten = rewrite_project_page_hrefs(&page, |href| format!("/mirror/{href}"));
        assert!(
            rewritten
                .body
                .contains("/mirror/https://pypi.org/simple/demo/demo-1.whl#sha256=abc123")
        );
        assert!(
            rewritten
                .body
                .contains(r#"data-requires-python="&gt;=3.9""#)
        );
        assert_eq!(rewritten.page_url, None);
    }

    #[test]
    fn parses_minimized_boolean_link_attributes() {
        let pages = [SourcePage {
            source: "pypi".to_string(),
            body: r#"
                <a href="demo-1.whl" data-yanked data-core-metadata data-gpg-sig>demo-1.whl</a>
            "#
            .to_string(),
            page_url: Some("https://pypi.org/simple/demo/".to_string()),
            pypi_last_serial: None,
        }];

        let html = merge_project_page("demo", &pages);
        assert!(html.contains(r#"data-yanked="""#));
        assert!(html.contains(r#"data-core-metadata="""#));
        assert!(html.contains(r#"data-gpg-sig="""#));

        let json: serde_json::Value =
            serde_json::from_str(&merge_project_json("demo", &pages)).expect("valid simple json");
        let file = &json["files"][0];
        assert_eq!(file["yanked"], true);
        assert_eq!(file["core-metadata"], true);
        assert_eq!(file["gpg-sig"], true);
    }
}
