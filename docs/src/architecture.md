# Architecture

## Overview

Apache Paimon Rust is organized as a Cargo workspace with multiple crates, each responsible for a distinct layer of functionality.

## Crate Structure

### `crates/paimon` — Core Library

The core crate implements the Paimon table format, including:

- **Catalog** — REST Catalog client for discovering and managing databases and tables
- **Table** — Table abstraction for reading Paimon tables
- **Snapshot & Manifest** — Reading snapshot and manifest metadata
- **Schema** — Table schema management and evolution
- **File IO** — Abstraction layer for storage backends (local filesystem, S3)
- **File Format** — Parquet file reading and writing via Apache Arrow

### `crates/integrations/datafusion` — DataFusion Integration

Provides a `TableProvider` implementation that allows querying Paimon tables using [Apache DataFusion](https://datafusion.apache.org/)'s SQL engine.

## Data Model

Paimon organizes data in a layered structure:

```
Catalog
 └── Database
      └── Table
           ├── Schema
           └── Snapshot
                └── Manifest
                     └── Data Files (Parquet)
```

- **Catalog** manages databases and tables, accessed via REST API
- **Snapshot** represents a consistent view of a table at a point in time
- **Manifest** lists the data files that belong to a snapshot
- **Data Files** store the actual data in Parquet format
