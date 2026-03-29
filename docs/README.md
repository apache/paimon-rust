# Documentation

This directory contains the source files for the Apache Paimon Rust documentation site, built with [MkDocs](https://www.mkdocs.org/) and the [Material for MkDocs](https://squidfun.github.io/mkdocs-material/) theme.

## Prerequisites

- Python 3.8+
- pip3

## Setup

```bash
pip3 install mkdocs-material
```

## Development

Preview the docs locally with live reload:

```bash
cd docs
mkdocs serve
```

Then open [http://127.0.0.1:8000](http://127.0.0.1:8000) in your browser.

## Build

Generate the static site:

```bash
cd docs
mkdocs build
```

The output will be in the `docs/site/` directory.
