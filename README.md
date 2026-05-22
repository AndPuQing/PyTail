# PyTail

[![CI](https://img.shields.io/github/actions/workflow/status/AndPuQing/PyTail/ci.yml?branch=main&label=CI)](https://github.com/AndPuQing/PyTail/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/actions/workflow/status/AndPuQing/PyTail/release.yml?label=Release)](https://github.com/AndPuQing/PyTail/actions/workflows/release.yml)
[![PyPI](https://img.shields.io/pypi/v/pytail.svg)](https://pypi.org/project/pytail/)
[![Python](https://img.shields.io/pypi/pyversions/pytail.svg)](https://pypi.org/project/pytail/)

`pytail` is a minimal incremental PyPI caching mirror.

It is no longer a `devpi` clone. The new server only does four things:

- expose a valid Python Simple Repository API root at `/simple/`
- lazily fetch and cache `/simple/<project>/` from an upstream index
- rewrite file links to a devpi-style `root/pypi/+f/...` path
- serve cached project pages and cached files on later requests

## Plan

1. Implement the smallest Simple API surface that `pip` and `uv` actually need:
   root index, project index, normalized names, file links, and content negotiation
   for HTML vs JSON responses.
2. Keep the cache incremental:
   do not mirror the full upstream project list, only remember projects that have
   already been requested.
3. Use conditional project refresh:
   cache upstream project pages, preserve `ETag`, and refresh a project after a TTL
   with `If-None-Match`.
4. Keep package files immutable:
   once a file URL is requested, cache it under `+files/` and serve it from a
   stable devpi-style file route on later requests.
5. Store the index in SQLite:
   project pages, file metadata, and known-project state live in SQLite, while
   package bytes live on disk.
6. Handle concurrent requests safely inside one process:
   per-project and per-file locks avoid duplicate upstream fetches and duplicate
   downloads.
7. Explicitly drop old `devpi` goals:
   no users, no ACLs, no upload API, no index inheritance, no replication, no
   snapshot format, no mirror whitelist, no web UI.

## Spec Coverage

The implementation is intentionally narrow:

- PEP 503 HTML Simple API for `/simple/` and `/simple/<project>/`
- PEP 691 JSON Simple API responses when `Accept` asks for
  `application/vnd.pypi.simple.v1+json`
- preservation of `requires-python`, `yanked`, `gpg-sig`,
  `dist-info-metadata`, and `core-metadata` markers when they are present on the
  upstream project page
- lazy local root index containing only already-cached projects

The implementation does not currently fetch or synthesize a full global upstream
project list. That is deliberate: the root index is only a local catalogue of
known projects, while project pages are fetched on demand.

## Why This Is Enough

Package resolution for `pip` and `uv` depends primarily on per-project Simple API
pages. A full pre-fetched mirror root is not required for normal dependency
resolution as long as:

- `/simple/<normalized-project>/` is available and correct
- file links are reachable
- project metadata such as hashes and `requires-python` are preserved

## Run

```sh
cargo run -- \
  --bind 127.0.0.1:3141 \
  --upstream-base-url https://pypi.org \
  --torch-url https://download.pytorch.org/whl/ \
  --cache-dir .cache/pytail
```

Then point tools at it:

```sh
uv pip install --index-url http://127.0.0.1:3141/simple/ requests
pip install --index-url http://127.0.0.1:3141/simple/ requests
```

PyTorch wheel indexes are also exposed under `/pytorch-wheels/`, so a command
that uses:

```sh
pip install torch --index-url https://download.pytorch.org/whl/cu126
```

can use the local caching endpoint instead:

```sh
pip install torch --index-url http://127.0.0.1:3141/pytorch-wheels/cu126
```

The local path is mapped directly under the configured PyTorch wheels upstream:

```text
/pytorch-wheels/torch/        -> <torch-url>/torch/
/pytorch-wheels/cu126/torch/  -> <torch-url>/cu126/torch/
```

For packages that need both PyPI and PyTorch wheels, keep the indexes separate:

```sh
pip install torch \
  --index-url http://127.0.0.1:3141/pytorch-wheels/cu126 \
  --extra-index-url http://127.0.0.1:3141/simple/
```

## Configuration

- `--bind`: listen address, default `127.0.0.1:3141`
- `--upstream-base-url`: upstream index origin, default `https://pypi.org`
- `--torch-url`: PyTorch wheels upstream root, default
  `https://download.pytorch.org/whl/`
- `--cache-dir`: local cache directory, default `.cache/pytail`
- `--cache-max-size`: maximum on-disk file cache size, default `0` for
  unlimited; accepts byte values or units like `512MiB`, `2G`, and `10GB`.
  When the limit is exceeded after a download, least-recently-used cached files
  are evicted first.
- `--project-cache-ttl-secs`: refresh age for cached project pages, default `900`
- `--request-timeout-secs`: upstream HTTP timeout, default `15`
- `--stats-interval-secs`: cache hit-rate and memory stats log interval, default
  `60`; set to `0` to disable periodic stats
- `--verbose`: enable debug logging for pytail

## Package

Build a Python wheel with `maturin`:

```sh
maturin build --release
```

Publish to PyPI:

```sh
maturin publish --release
```

The wheel installs the `pytail` command as a native Rust binary.

## Cache Layout

```text
<cache-dir>/
  index.sqlite3
  +files/
    root/
      pypi/
        +f/
          f0a/
            f3fc39e7459b0/
              gradio_client-1.0.2-py3-none-any.whl
          _url/
            <url-digest>/
              <filename>
```

- SQLite stores project cache entries, parsed file rows, upstream `ETag`, and
  known project names
- `+files/root/pypi/+f/<first-3-sha256>/<next-13-sha256>/<filename>` stores
  hashed files in the same shape as devpi's filesystem layout
- `+files/root/pypi/+f/_url/<url-digest>/<filename>` is used only when an
  upstream file link does not provide a usable `sha256` hash
- PyTorch wheel endpoints reuse the same blob store and download pipeline; only
  their project cache keys and client-facing file URLs are namespaced

## Non-Goals

- full PyPI mirroring
- authentication or private indexes
- package upload
- merge of local and upstream package sources
- replica mode or background synchronization
