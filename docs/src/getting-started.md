<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Getting Started

## Installation

Add `paimon` to your `Cargo.toml`:

```toml
[dependencies]
paimon = "0.0.0"
tokio = { version = "1", features = ["full"] }
```

By default, the `storage-fs` (local filesystem) and `storage-memory` (in-memory) backends are enabled. To use additional storage backends, enable the corresponding feature flags:

```toml
[dependencies]
paimon = { version = "0.0.0", features = ["storage-s3"] }
```

Available storage features:

| Feature          | Backend          |
|------------------|------------------|
| `storage-fs`     | Local filesystem |
| `storage-memory` | In-memory        |
| `storage-s3`     | Amazon S3        |
| `storage-oss`    | Alibaba Cloud OSS|
| `storage-all`    | All of the above |

## Catalog Management

Paimon supports multiple catalog types. The `CatalogFactory` provides a unified way to create catalogs based on configuration options.

### Create a Catalog

The `CatalogFactory` automatically determines the catalog type based on the `metastore` option:

```rust
use paimon::{CatalogFactory, CatalogOptions, Options};

// Local filesystem (no credentials needed)
let mut options = Options::new();
options.set(CatalogOptions::WAREHOUSE, "/path/to/warehouse");
let catalog = CatalogFactory::create(options).await?;

// Amazon S3
let mut options = Options::new();
options.set(CatalogOptions::WAREHOUSE, "s3://bucket/warehouse");
options.set("s3.access-key-id", "your-access-key-id");
options.set("s3.secret-access-key", "your-secret-access-key");
options.set("s3.region", "us-east-1");
let catalog = CatalogFactory::create(options).await?;

// Alibaba Cloud OSS
let mut options = Options::new();
options.set(CatalogOptions::WAREHOUSE, "oss://bucket/warehouse");
options.set("fs.oss.accessKeyId", "your-access-key-id");
options.set("fs.oss.accessKeySecret", "your-access-key-secret");
options.set("fs.oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
let catalog = CatalogFactory::create(options).await?;

// REST catalog
let mut options = Options::new();
options.set(CatalogOptions::METASTORE, "rest");
options.set(CatalogOptions::URI, "http://localhost:8080");
options.set(CatalogOptions::WAREHOUSE, "my_warehouse");
let catalog = CatalogFactory::create(options).await?;
```

Supported metastore types:

| Metastore Type | Description                      |
|----------------|----------------------------------|
| `filesystem`   | Local or remote filesystem (default) |
| `rest`         | REST catalog server              |

### Manage Databases

```rust
use paimon::Catalog; // import the trait
use std::collections::HashMap;

// Create a database
catalog.create_database("my_db", false, HashMap::new()).await?;

// List databases
let databases = catalog.list_databases().await?;

// Drop a database (cascade = true to drop all tables inside)
catalog.drop_database("my_db", false, true).await?;
```

### Manage Tables

```rust
use paimon::catalog::Identifier;
use paimon::spec::{DataType, IntType, VarCharType, Schema};

// Define a schema
let schema = Schema::builder()
    .column("id", DataType::Int(IntType::new()))
    .column("name", DataType::VarChar(VarCharType::string_type()))
    .build()?;

// Create a table
let identifier = Identifier::new("my_db", "my_table");
catalog.create_table(&identifier, schema, false).await?;

// List tables in a database
let tables = catalog.list_tables("my_db").await?;

// Get a table handle
let table = catalog.get_table(&identifier).await?;
```

## Reading a Table

Paimon Rust uses a scan-then-read pattern: first scan the table to produce splits, then read data from those splits as Arrow `RecordBatch` streams.

```rust
use futures::StreamExt;

// Get a table from the catalog
let table = catalog.get_table(&Identifier::new("my_db", "my_table")).await?;

// Create a read builder
let read_builder = table.new_read_builder();

// Step 1: Scan — produces a Plan containing DataSplits
let plan = {
    let scan = read_builder.new_scan();
    scan.plan().await?
};

// Step 2: Read — consumes splits and returns Arrow RecordBatches
let reader = read_builder.new_read()?;
let mut stream = reader.to_arrow(plan.splits())?;

while let Some(batch) = stream.next().await {
    let batch = batch?;
    println!("RecordBatch: {batch:#?}");
}
```

## Building from Source

```bash
git clone https://github.com/apache/paimon-rust.git
cd paimon-rust
cargo build
```

## Running Tests

```bash
# Unit tests
cargo test

# Integration tests (requires Docker)
make docker-up
cargo test -p integration_tests
make docker-down
```
