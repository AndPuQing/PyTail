# devpi-rs

`devpi-rs` is a Rust rewrite focused on the Python simple repository API.
The first implemented feature is multi-source package lookup: one local
server can aggregate several upstream `simple/` indexes and return a single
PEP 503-style HTML page or PEP 691 JSON response to pip-compatible clients.

The server uses `axum` on top of `tokio` for HTTP handling and `clap` for
command-line parsing.
Responses include devpi-style `X-DEVPI-API-VERSION`,
`X-DEVPI-SERVER-VERSION`, and `X-DEVPI-SERIAL` headers. The serial is
persisted locally and increments for successful local mutations. A minimal
local JSON changelog is persisted for inspection, but the full devpi replica
protocol is not implemented.

This is not yet a full replacement for Python devpi. The current
authentication and ACL support is intentionally basic; replication, plugins,
full permission parity, and the web UI are not implemented in this initial
Rust version.

## Run

```sh
cargo run -- serve \
  --listen 127.0.0.1:3141 \
  --cache-dir .devpi-rs/cache \
  --package-dir .devpi-rs/packages \
  --source corp=http://localhost:8080/simple/ \
  --source pypi=https://pypi.org/simple/
```

Then point pip at the local simple index:

```sh
pip install --index-url http://127.0.0.1:3141/root/pypi/+simple/ requests
```

Minimal local storage snapshots can be copied with:

```sh
cargo run -- export --package-dir .devpi-rs/packages .devpi-rs/export
cargo run -- import --package-dir .devpi-rs/packages .devpi-rs/export
```

The snapshot format is a directory copy of the Rust storage layout, not the
official devpi import/export format.

## Multi-source behavior

For `GET /root/pypi/+simple/<project>/`, devpi-rs queries every configured
source in the order provided, using the PEP 503 normalized project name. The
returned pages are merged into one response. Duplicate links are removed by
`href`, and the first source wins.

For `GET /root/pypi/+simple/`, devpi-rs merges project listings from every
configured source and de-duplicates them by normalized project name.

Simple API routes return PEP 503 HTML by default. Link metadata such as
`data-requires-python`, `data-yanked`, `data-gpg-sig`, and core metadata
markers or hashes is preserved when upstream pages are merged. If the request
`Accept` header includes
`application/vnd.pypi.simple.v1+json`, they return a PEP 691 JSON response with
the same merged package data, including hash fragments split into `hashes`.
Stage index routes `GET /<user>/<index>` and `GET /<user>/<index>/` also return
the PEP 691 project listing when that media type is requested.

Successful upstream responses are cached on disk. If a source later fails,
devpi-rs serves the stale cached response for that source when available.
Simple index responses include ETag validators and honor `If-None-Match` with
`304 Not Modified` responses for both PEP 503 HTML and PEP 691 JSON clients.

Locally uploaded packages are scoped by devpi stage (`user/index`) and merged
before upstream sources, so local files win when the same distribution link
appears in more than one place on that stage.

If an index config contains `bases`, local files from those base indexes are
merged after the current stage and before upstream sources. Bases use devpi's
`user/index` form and are followed recursively with cycle protection.
Package file downloads and `releasefilemeta` JSON also fall back through bases
when the requested file is not present on the current stage.

By default, each stage queries every configured upstream source. A stage can
limit and order upstream lookup with an index config `sources` list:

```sh
curl -H "Content-Type: application/json" \
  -X PATCH -d '{"sources":["corp","pypi"]}' \
  http://127.0.0.1:3141/alice/dev
```

Names in `sources` refer to the global `--source name=url` or `source.name`
configuration entries.

## Users and Indexes

Create a user and a devpi-style index configuration:

```sh
curl -H "Content-Type: application/json" \
  -X PUT -d '{"password":"123","email":"alice@example.com"}' \
  http://127.0.0.1:3141/alice

curl -H "Content-Type: application/json" \
  -X PUT -d '{"bases":["root/pypi"],"volatile":true}' \
  http://127.0.0.1:3141/alice/dev
```

Direct user creation requires a `password` field. User and index metadata is
persisted under `--package-dir` in `.devpi-rs-users.json`. Passwords are used
for HTTP Basic authentication on configured user and index mutations. Existing
users can be modified or deleted by that user or by `root`. Changing an
existing user's password returns a fresh devpi-style proxy auth token. New
passwords are stored as salted hashes; legacy plain-text password entries from
older devpi-rs data files are still accepted for compatibility.

`POST /+login` accepts devpi-style JSON login requests:

```sh
curl -X POST -H "Content-Type: application/json" \
  -d '{"user":"alice","password":"123"}' \
  http://127.0.0.1:3141/+login
```

The response has `type: "proxyauth"` and can be used by clients which send
`X-Devpi-Auth`. The returned proxy password is an expiring devpi-rs token
checked against a local secret under `--package-dir`.

Uploads, project registration, and project/version deletion on configured
indexes require a user in `acl_upload`, unless `acl_upload` contains
`:ANONYMOUS:` or `:AUTHENTICATED:`. Existing index configuration changes and
index deletion require the index owner or `root`. Package routes require an
explicitly configured index and return `index not found` for unknown stages.
ACL special entries are normalized like devpi, so `:anonymous:` and
`:authenticated:` are stored as `:ANONYMOUS:` and `:AUTHENTICATED:`.
When an index creation request is authenticated, it must be made by the target
user or by `root`; unauthenticated index creation is still open for early
prototype compatibility.

Index configs accept and round-trip the common devpi metadata keys `title`,
`description`, `custom_data`, `mirror_whitelist`,
`mirror_whitelist_inheritance`, `mirror_url`, `mirror_web_url_fmt`,
`mirror_cache_expiry`, `mirror_ignore_serial_header`, `mirror_no_project_list`,
`mirror_provides_core_metadata`, and `mirror_use_external_urls`.
`mirror_whitelist` values are normalized in the same basic form as
devpi-client input, so comma-separated strings are split, names are
lowercased, and underscores become dashes. The legacy
`pypi_whitelist` input is accepted for old-client compatibility but ignored.
When a stage inherits a mirror base and a private project exists locally,
simple project pages omit upstream mirror links unless `mirror_whitelist`
contains the project name or `*`; `mirror_whitelist_inheritance` controls
whether non-mirror base whitelists are combined by `intersection` or `union`.
Mirror stages with `mirror_no_project_list=true` skip upstream root project
listing fetches while still allowing direct project fetches.
Mirror stages with `mirror_url` fetch upstream simple pages from that URL
instead of the global source list. When `mirror_cache_expiry` is set, cached
mirror simple root and project pages are reused until the configured number of
seconds has elapsed; the simple project refresh route clears the project cache
and fetches it again.
PATCH requests also accept devpi-client style list operations such as
`["bases+=root/pypi", "mirror_whitelist+=demo", "mirror_whitelist-=old"]`
for list settings, scalar assignments such as `["volatile=False"]`, and
`key-=` deletion for optional or plugin-provided index settings. Unknown index
settings are preserved and round-tripped for basic plugin compatibility.
Use `?error_on_noop` on PATCH to reject modifications that leave the index
configuration unchanged.
Mirror stages rewrite upstream simple file links to local `+e` URLs by default
and cache downloaded mirror files; `mirror_use_external_urls=true` leaves
upstream file links external and redirects uncached `+e` requests to the
upstream URL. Mirror `+e` requests for `<filename>.metadata` fetch and cache
the corresponding upstream metadata sidecar when `mirror_provides_core_metadata`
is enabled. Cached mirror files include ETag validators and honor
`If-None-Match`. Full devpi mirror behavior is still incomplete.

For `volatile=false` indexes, first uploads are accepted but overwriting an
existing file and deleting projects, versions, or files is rejected unless the
delete request includes `?force`.

## Local Packages

Register a project before uploading files:

```sh
curl -X PUT http://127.0.0.1:3141/alice/dev/example
```

Upload a package file with the devpi-style stage file path
`PUT /<user>/<index>/+f/<project>/<filename>`:

```sh
curl -X PUT --data-binary @dist/example-1.0.0.tar.gz \
  http://127.0.0.1:3141/alice/dev/+f/example/example-1.0.0.tar.gz
```

The file is stored under `--package-dir/alice/dev`, exposed at the same
`/alice/dev/+f/...` URL, and listed in `GET /alice/dev/+simple/<project>/`.
Local simple links include a `sha256` hash fragment, which is also exposed in
PEP 691 JSON `hashes` and devpi-style `releasefilemeta` JSON.
Package downloads are served as `application/octet-stream` with content length,
attachment filename, immutable cache headers, `Last-Modified`, and SHA256
ETag validators. `If-None-Match` and `If-Modified-Since` requests return
`304 Not Modified` when the cached package file is unchanged.
`HEAD` requests return the same package headers without the file body.
Package file routes accept the same paths with or without a trailing slash.
When a package file is requested with `Accept: application/json`, devpi-rs
returns a devpi-style `releasefilemeta` JSON response instead of the file body.
For files uploaded with stored release metadata, `GET
/<user>/<index>/+f/<project>/<filename>.metadata` returns a small core metadata
text response built from those stored fields. Wheel uploads also extract
stored or deflated `*.dist-info/METADATA`, and `.tar.gz`/`.tgz` or `.zip`
sdist uploads extract `PKG-INFO` when present.

`POST /<user>/<index>` and `POST /<user>/<index>/` also accept
devpi/setup.py style multipart forms.
With `:action=submit`, `name`, and `version`, it registers release metadata
without files. With `:action=file_upload`, `name`, `version`, and a `content`
file field, it stores the release file. With `:action=doc_upload`, it stores
a single documentation zip as `<normalized-project>-<version>.doc.zip`.
Additional text fields are stored as per-version or per-file release metadata
and returned by
`GET /<user>/<index>/<project>`.
For uploaded files, `requires_python` defaults to an empty string and is
reflected in the stage simple index link attributes and PEP 691 JSON; `yanked`
metadata is also reflected when provided, as are core metadata and GPG signature
markers.

The legacy `PUT /files/<project>/<filename>` and `GET /files/<project>/<filename>`
aliases still work and map to the default `root/pypi` stage.

The devpi curl-style path `PUT /<user>/<index>/<project>/<version>/<filename>`
is also accepted when `<filename>` contains `<version>`.

Delete a release or a whole local project with the matching devpi curl-style
paths:

```sh
curl -X DELETE http://127.0.0.1:3141/alice/dev/example/1.0.0
curl -X DELETE http://127.0.0.1:3141/alice/dev/example
```

Upload a tox result JSON document for a release file:

```sh
curl -X POST -H "Content-Type: application/json" \
  -d '{"envname":"py312","retcode":0}' \
  http://127.0.0.1:3141/alice/dev/+f/example/example-1.0.0.tar.gz
```

Tox results are returned in `GET /<user>/<index>/<project>` alongside file
metadata. The `toxresultpath` returned by the upload response is readable with
`GET /<user>/<index>/+f/<project>/<filename>.toxresult-N` and removable with
`DELETE` on the same path. When `outside_url` or `X-Outside-Url` is active,
the returned `toxresultpath` uses that external base URL.
Version metadata responses at `GET /<user>/<index>/<project>/<version>` include
devpi-style `+links` entries for release files and documentation zips. Local
uploads persist a minimal per-file history log with `what`, `who`, `when`, and
`dst` fields, and tox result links expose the same history as their release
file. Internal stage-to-stage pushes preserve source upload history and append a
`push` entry with `src` and `dst`; richer overwrite and external push history
edge cases remain incomplete.

Push a local release from one stage to another with the devpi-style internal
push endpoint:

```sh
curl -X POST -H "Content-Type: application/json" \
  -d '{"name":"example","version":"1.0.0","targetindex":"bob/prod"}' \
  http://127.0.0.1:3141/alice/dev
```

This copies matching local release files, file metadata, version metadata, and
tox results to the target stage. When the source is a mirror stage without a
local cached release file, devpi-rs fetches matching files from the mirror
simple page, preserves supported simple-link metadata, and stores them in the
target stage.

External push has initial support for legacy multipart upload posts to an
`http://` `posturl` with `username`, `password`, and optional
`register_project=true`. It sends register and release file upload actions and
returns a devpi-style action log. HTTPS/TLS external push is not implemented
yet, so direct pushes to modern PyPI endpoints remain incomplete.
The `+api` feature list advertises `push-no-docs`, `push-only-docs`, and
`push-register-project` so devpi-client can send those supported push options.

## Config File

The same settings can be provided with `--config`:

```ini
listen = 127.0.0.1:3141
cache_dir = .devpi-rs/cache
package_dir = .devpi-rs/packages
outside_url = https://packages.example.com
source.corp = http://localhost:8080/simple/
source.pypi = https://pypi.org/simple/
```

`outside_url` is optional. When set, `+api` responses use it for client-facing
`login`, `index`, `simpleindex`, and `pypisubmit` URLs behind a reverse proxy.
Requests with `X-Outside-Url` override the configured value for devpi-style
proxy deployments; otherwise `+api` falls back to the request `Host` header
when present.

```sh
cargo run -- serve --config devpi-rs.ini
```

## Implemented Endpoints

- `GET /` list users and configured indexes
- `GET /+status` devpi-style status JSON with API/server version, configured source names, and index source selections
  plus effective per-index source order and the most recent timestamped upstream fetch report
  for each source touched by a simple request
- `GET /+changelog/<serial>` returns persisted local JSON changelog entries; `<serial>-` returns entries from that serial onward
- `GET/POST /+authcheck` validates `X-Original-URI` routes for reverse-proxy auth checks
- `POST /+login` devpi-style JSON login returning proxy auth
- `GET /+api` root devpi-style API location JSON
- `PUT /<user>` create user metadata, returning conflict when the user already exists
- `PATCH /<user>` update existing user metadata
- `GET /<user>` read user metadata and configured indexes
- `GET/PUT/PATCH/DELETE /<user>/` trailing-slash user aliases
- `GET /<user>/+api` user devpi-style API location JSON
- `DELETE /<user>` delete user metadata and package files
- `PUT /<user>/<index>` create index config, returning conflict when the index already exists
- `PATCH /<user>/<index>` update existing index config
- `GET /<user>/<index>` read index config with local projects and effective source order unless `?no_projects`
- `POST /<user>/<index>` internal stage-to-stage release push with JSON `name`, `version`, and `targetindex`, or multipart submit/upload
- `DELETE /<user>/<index>` delete a volatile index and package files
- `GET /<user>/<index>/` stage metadata JSON with local project list and effective source order
- `POST /<user>/<index>/` internal stage-to-stage release push or multipart submit/upload trailing-slash alias
- `GET /<user>/<index>/+api` devpi-style API location JSON
- `PUT /<user>/<index>/<project>` register a project without files
- `GET /<user>/<index>/<project>` project metadata JSON with local file list and inherited version metadata unless `?ignore_bases`
- `DELETE /<user>/<index>/<project>` delete local project files
- `GET /<user>/<index>/<project>/<version>` version metadata JSON, following bases unless `?ignore_bases`
- `DELETE /<user>/<index>/<project>/<version>` delete local files matching version
- `PUT /<user>/<index>/<project>/<version>/<filename>` curl-style package upload
- `GET /<user>/<index>/<project>/<version>/<filename>` curl-style package download
- `GET /root/pypi/+simple/` devpi-style merged simple index root
- `GET /root/pypi/+simple/<project>/` devpi-style merged project release links
- `POST /<user>/<index>/+simple/<project>/refresh` clears cached upstream project pages, fetches them again, and redirects back to the project simple page
- Browser HTML project simple pages include a devpi-style refresh form; installer and PEP 691 JSON responses omit it
- `PUT /<user>/<index>/+f/<project>/<filename>` stage-scoped local package upload
- `POST /<user>/<index>/+f/<project>/<filename>` upload tox result JSON
- `GET /<user>/<index>/+f/<project>/<filename>` stage-scoped local package download
- `GET/DELETE /<user>/<index>/+f/<project>/<filename>.toxresult-N` read or delete a stored tox result
- `GET /<user>/<index>/+f/<project>/<filename>.metadata` stored core metadata text, following bases
- `DELETE /<user>/<index>/+f/<project>/<filename>` delete a stage-scoped local package file
- `GET /+files/<user>/<index>/+f/<project>/<filename>` devpi-style local file relpath alias
- `GET /+files/<user>/<index>/+f/<hash3>/<hash13>/<filename>` devpi-style hashdir file relpath alias for local stage files
- `GET /simple/` merged simple index root
- `GET /simple/<project>/` merged project release links
- Simple index routes without the trailing slash redirect HTML browser requests to the canonical slash URL, while installer and PEP 691 JSON requests are served directly
- `PUT /files/<project>/<filename>` local package upload
- `GET /files/<project>/<filename>` local package download
- `DELETE /files/<project>/<filename>` local package delete

## Development

```sh
cargo fmt
cargo test
cargo clippy --all-targets -- -D warnings
```
