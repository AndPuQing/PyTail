# PyTail

[![CI](https://img.shields.io/github/actions/workflow/status/AndPuQing/PyTail/ci.yml?branch=main&label=CI)](https://github.com/AndPuQing/PyTail/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/actions/workflow/status/AndPuQing/PyTail/release.yml?label=Release)](https://github.com/AndPuQing/PyTail/actions/workflows/release.yml)
[![PyPI](https://img.shields.io/pypi/v/pytail.svg)](https://pypi.org/project/pytail/)
[![Python](https://img.shields.io/pypi/pyversions/pytail.svg)](https://pypi.org/project/pytail/)

`pytail` is a small PyPI caching proxy. It exposes a Python Simple Repository
API endpoint, fetches packages from upstream on demand, and keeps downloaded
files in a local cache.

It is useful when you want faster repeated installs, a simple shared package
cache, or a local endpoint for PyPI and PyTorch wheel indexes.

## Features

- PyPI-compatible `/simple/` endpoint for `pip` and `uv`
- lazy caching of project pages and package files
- optional cache size limit with least-recently-used cleanup
- PyTorch wheel index proxy under `/pytorch-wheels/`
- HTML and JSON Simple API responses

## Install

Install from PyPI:

```sh
pip install pytail
```

Or install from source:

```sh
cargo install --path .
```

## Run

After installation:

```sh
pytail \
  --bind 127.0.0.1:3141 \
  --cache-dir .cache/pytail
```

From a source checkout:

```sh
cargo run -- \
  --bind 127.0.0.1:3141 \
  --cache-dir .cache/pytail
```

Then use it as the package index:

```sh
uv pip install --index-url http://127.0.0.1:3141/simple/ requests
pip install --index-url http://127.0.0.1:3141/simple/ requests
```

Open the local index in a browser:

```text
http://127.0.0.1:3141/simple/
```

## PyTorch Wheels

PyTorch wheel indexes are available under `/pytorch-wheels/`.

For example, this upstream command:

```sh
pip install torch --index-url https://download.pytorch.org/whl/cu126
```

can use the local cache instead:

```sh
pip install torch --index-url http://127.0.0.1:3141/pytorch-wheels/cu126
```

If a package needs both PyTorch wheels and normal PyPI packages:

```sh
pip install torch \
  --index-url http://127.0.0.1:3141/pytorch-wheels/cu126 \
  --extra-index-url http://127.0.0.1:3141/simple/
```

Flat wheel mirrors, such as the Aliyun PyTorch mirror, can be converted into
the same project-based Simple API. Start PyTail with the mirror root and enable
flat-index mode:

```sh
pytail \
  --torch-url https://mirrors.aliyun.com/pytorch-wheels/ \
  --torch-flat-index
```

Clients continue to use the normal local project index. For example:

```sh
pip install torch \
  --index-url http://127.0.0.1:3141/pytorch-wheels/cu128
```

## Options

Common options:

- `--bind`: listen address, default `127.0.0.1:3141`
- `--cache-dir`: cache directory, default `.cache/pytail`
- `--cache-max-size`: max file cache size, default `0` for unlimited; examples:
  `512MiB`, `2G`, `10GB`
- `--upstream-base-url`: PyPI-compatible upstream, default `https://pypi.org`
- `--torch-url`: PyTorch wheel upstream, default
  `https://download.pytorch.org/whl/`
- `--torch-flat-index`: treat each PyTorch channel URL as a flat `--find-links`
  page and expose it as a project-based Simple API
- `--project-cache-ttl-secs`: project page refresh interval, default `900`
- `--request-timeout-secs`: upstream request timeout, default `15`
- `--stats-interval-secs`: stats log and trend sampling interval, default `3600`;
  set to `0` to disable
- `--verbose`: enable debug logs

## Build

Build the Rust binary:

```sh
cargo build --release
```

Build a Python wheel with `maturin`:

```sh
maturin build --release
```

The wheel installs the `pytail` command.
