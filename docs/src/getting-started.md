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

`FileSystemCatalog` manages databases and tables stored on a local (or remote) filesystem.

### Create a Catalog

```rust
use paimon::FileSystemCatalog;

let catalog = FileSystemCatalog::new("/tmp/paimon-warehouse")?;
```

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

## DataFusion Integration

Query Paimon tables using SQL with [Apache DataFusion](https://datafusion.apache.org/). Add the integration crate:

```toml
[dependencies]
paimon = "0.0.0"
paimon-datafusion = "0.0.0"
datafusion = "52"
```

Register a Paimon table and run SQL queries:

```rust
use std::sync::Arc;
use datafusion::prelude::SessionContext;
use paimon_datafusion::PaimonTableProvider;

// Get a Paimon table from your catalog
let table = catalog.get_table(&identifier).await?;

// Register as a DataFusion table
let provider = PaimonTableProvider::try_new(table)?;
let ctx = SessionContext::new();
ctx.register_table("my_table", Arc::new(provider))?;

// Query with SQL
let df = ctx.sql("SELECT * FROM my_table").await?;
df.show().await?;
```

> **Note:** The DataFusion integration currently supports full table scans only. Column projection and predicate pushdown are not yet implemented.

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
