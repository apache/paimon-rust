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

# DataFusion Integration

[Apache DataFusion](https://datafusion.apache.org/) is a fast, extensible query engine for building data-centric systems in Rust. The `paimon-datafusion` crate provides a read-only integration that lets you query Paimon tables using SQL.

## Setup

```toml
[dependencies]
paimon = "0.1.0"
paimon-datafusion = "0.1.0"
datafusion = "53"
tokio = { version = "1", features = ["full"] }
```

## Registering Tables

Register an entire Paimon catalog so all databases and tables are accessible via `catalog.database.table` syntax:

```rust
use std::sync::Arc;
use datafusion::prelude::SessionContext;
use paimon_datafusion::PaimonCatalogProvider;

let ctx = SessionContext::new();
ctx.register_catalog("paimon", Arc::new(PaimonCatalogProvider::new(Arc::new(catalog))));

let df = ctx.sql("SELECT * FROM paimon.default.my_table").await?;
df.show().await?;
```

## Time Travel

Paimon supports time travel queries to read historical data. In DataFusion, this is done via the `VERSION AS OF` and `TIMESTAMP AS OF` clauses.

### By Snapshot ID

Read data from a specific snapshot by passing an integer:

```sql
SELECT * FROM paimon.default.my_table VERSION AS OF 1
```

This sets the `scan.version` option. At scan time, the value is resolved as a snapshot ID.

### By Tag Name

Read data from a named tag. Since `VERSION AS OF` in SQL only accepts numeric values, tag-based time travel is done via the `scan.version` table option:

```rust
let table = table.copy_with_options(HashMap::from([
    ("scan.version".to_string(), "my_tag".to_string()),
]));
```

At scan time, the version value is resolved by first checking if a tag with that name exists, then trying to parse it as a snapshot ID. If neither matches, an error is returned.

### By Timestamp

Read data as of a specific point in time by passing a timestamp string in `YYYY-MM-DD HH:MM:SS` format:

```sql
SELECT * FROM paimon.default.my_table TIMESTAMP AS OF '2024-01-01 00:00:00'
```

This finds the latest snapshot whose commit time is less than or equal to the given timestamp. The timestamp is interpreted in the local timezone.

### Enabling Time Travel Syntax

DataFusion requires the Databricks SQL dialect to parse `VERSION AS OF` and `TIMESTAMP AS OF`. You also need to register the `PaimonRelationPlanner`:

```rust
use std::sync::Arc;
use datafusion::prelude::{SessionConfig, SessionContext};
use paimon_datafusion::{PaimonCatalogProvider, PaimonRelationPlanner};

let config = SessionConfig::new()
    .set_str("datafusion.sql_parser.dialect", "Databricks");
let ctx = SessionContext::new_with_config(config);

ctx.register_catalog("paimon", Arc::new(PaimonCatalogProvider::new(Arc::new(catalog))));
ctx.register_relation_planner(Arc::new(PaimonRelationPlanner::new()))?;

// Now time travel queries work
let df = ctx.sql("SELECT * FROM paimon.default.my_table VERSION AS OF 1").await?;
```
