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

# SQL Integration

[Apache DataFusion](https://datafusion.apache.org/) is a fast, extensible query engine for building data-centric systems in Rust. The `paimon-datafusion` crate provides a full SQL integration that lets you create, query, and modify Paimon tables.

## Setup

```toml
[dependencies]
paimon = "0.1.0"
paimon-datafusion = "0.1.0"
datafusion = "53"
tokio = { version = "1", features = ["full"] }
```

## Registering Catalog

Register an entire Paimon catalog so all databases and tables are accessible via `paimon.database.table` syntax:

```rust
use std::sync::Arc;
use paimon::{CatalogOptions, FileSystemCatalog, Options};
use paimon_datafusion::SQLContext;

async fn example() -> Result<(), Box<dyn std::error::Error>> {
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, "file:///tmp/paimon-warehouse");
    let catalog = Arc::new(FileSystemCatalog::new(options)?);

    let mut ctx = SQLContext::new();
    ctx.register_catalog("paimon", catalog)?;
    let df = ctx.sql("SELECT * FROM paimon.default.my_table").await?;
    df.show().await?;
    Ok(())
}
```

`SQLContext::new` creates a session context with the Paimon relation planner pre-registered. Use `register_catalog` to add one or more Paimon catalogs. It also manages session-scoped dynamic options internally for `SET`/`RESET` support.

## Data Types

The following SQL data types are supported in CREATE TABLE and mapped to their corresponding Paimon types:

| SQL Type | Paimon Type | Notes |
|---|---|---|
| `BOOLEAN` | BooleanType | |
| `TINYINT` | TinyIntType | |
| `SMALLINT` | SmallIntType | |
| `INT` / `INTEGER` | IntType | |
| `BIGINT` | BigIntType | |
| `FLOAT` / `REAL` | FloatType | |
| `DOUBLE` / `DOUBLE PRECISION` | DoubleType | |
| `VARCHAR` / `TEXT` / `STRING` / `CHAR` | VarCharType | |
| `BINARY` / `VARBINARY` / `BYTEA` | VarBinaryType | |
| `BLOB` | BlobType | Binary large object |
| `DATE` | DateType | |
| `TIMESTAMP[(p)]` | TimestampType | Precision p: 0/3/6/9, default 3 |
| `TIMESTAMP WITH TIME ZONE` | LocalZonedTimestampType | |
| `DECIMAL(p, s)` | DecimalType | |
| `ARRAY<element>` | ArrayType | e.g. `ARRAY<INT>` |
| `MAP(key, value)` | MapType | e.g. `MAP(STRING, INT)` |
| `STRUCT<field TYPE, ...>` | RowType | e.g. `STRUCT<city STRING, zip INT>` |

## DDL

### CREATE SCHEMA / DROP SCHEMA

```sql
CREATE SCHEMA paimon.my_db;
DROP SCHEMA paimon.my_db CASCADE;
```

### CREATE TABLE

```sql
CREATE TABLE paimon.my_db.users (
    id INT NOT NULL,
    name STRING,
    age INT,
    PRIMARY KEY (id)
) WITH ('bucket' = '4');
```

`IF NOT EXISTS` is supported:

```sql
CREATE TABLE IF NOT EXISTS paimon.my_db.users (
    id INT NOT NULL
);
```

Unsupported syntax (will return an error):
- `CREATE EXTERNAL TABLE`
- `LOCATION`
- `CREATE TABLE AS SELECT`

### Partitioned Tables

Use `PARTITIONED BY` to specify partition columns. Partition columns must already be declared in the column definitions and must not include a type:

```sql
CREATE TABLE paimon.my_db.events (
    id INT NOT NULL,
    name STRING,
    dt STRING,
    PRIMARY KEY (id, dt)
) PARTITIONED BY (dt)
WITH ('bucket' = '2');
```

Invalid usage (will return an error):

```sql
-- Partition columns must not specify a type
CREATE TABLE paimon.my_db.events (
    id INT NOT NULL,
    dt STRING
) PARTITIONED BY (dt STRING);
```

### Complex Types

```sql
CREATE TABLE paimon.my_db.complex_types (
    id INT NOT NULL,
    tags ARRAY<STRING>,
    props MAP(STRING, INT),
    address STRUCT<city STRING, zip INT>,
    PRIMARY KEY (id)
);
```

### DROP TABLE

```sql
DROP TABLE paimon.my_db.users;
DROP TABLE IF EXISTS paimon.my_db.users;
```

### CREATE TEMPORARY TABLE

Create an in-memory temporary table from a query result. Temporary tables exist only for the lifetime of the `SQLContext` instance and are automatically cleaned up when the context is dropped.

```sql
-- Without column types (types inferred from the query)
CREATE TEMPORARY TABLE paimon.my_db.source AS SELECT * FROM (VALUES (1, 'alice'), (2, 'bob')) AS t(id, name);

-- With explicit column types (recommended when integer precision matters)
CREATE TEMPORARY TABLE paimon.my_db.source (id INT, name STRING) AS SELECT * FROM (VALUES (1, 'alice'), (2, 'bob')) AS t(id, name);
```

`IF NOT EXISTS` is supported — if the table already exists, the statement is silently ignored:

```sql
CREATE TEMPORARY TABLE IF NOT EXISTS paimon.my_db.source AS SELECT 1;
```

> **Note:** When using `VALUES` without explicit column types, DataFusion infers integer literals as `Int64`. If the temporary table will be used as a source in `MERGE INTO` against a Paimon table with `Int32` columns, specify the column types explicitly to avoid type mismatch errors.

### CREATE TEMPORARY VIEW

Create a temporary view from a query:

```sql
CREATE TEMPORARY VIEW paimon.my_db.active_users AS SELECT * FROM paimon.my_db.users WHERE id > 0;
```

`IF NOT EXISTS` is supported:

```sql
CREATE TEMPORARY VIEW IF NOT EXISTS paimon.my_db.active_users AS SELECT * FROM paimon.my_db.users WHERE id > 0;
```

### DROP TEMPORARY TABLE / DROP TEMPORARY VIEW

Remove a temporary table or view:

```sql
DROP TEMPORARY TABLE paimon.my_db.source;
DROP TEMPORARY TABLE IF EXISTS paimon.my_db.source;
DROP TEMPORARY VIEW paimon.my_db.active_users;
DROP TEMPORARY VIEW IF EXISTS paimon.my_db.active_users;
```

### ALTER TABLE

```sql
-- Add a column
ALTER TABLE paimon.my_db.users ADD COLUMN email STRING;

-- Drop a column
ALTER TABLE paimon.my_db.users DROP COLUMN age;

-- Rename a column
ALTER TABLE paimon.my_db.users RENAME COLUMN name TO username;

-- Rename a table
ALTER TABLE paimon.my_db.users RENAME TO members;

-- Set table properties
ALTER TABLE paimon.my_db.users SET TBLPROPERTIES('data-evolution.enabled' = 'true');
```

`IF EXISTS` is supported:

```sql
ALTER TABLE IF EXISTS paimon.my_db.users ADD COLUMN age INT;
```

## DML

### INSERT INTO

```sql
INSERT INTO paimon.my_db.users VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');
```

`INSERT INTO ... SELECT ...` is also supported:

```sql
INSERT INTO paimon.my_db.users SELECT * FROM source_table;
```

For primary-key tables, records with duplicate keys are deduplicated according to the merge engine (default: Deduplicate engine, where the last written value wins).

### INSERT OVERWRITE

For partitioned tables, `INSERT OVERWRITE` replaces only the affected partitions. For unpartitioned tables, it replaces the entire table:

```sql
-- Dynamic partition overwrite: overwrites only the dt='2024-01-01' partition
INSERT OVERWRITE paimon.my_db.events VALUES ('2024-01-01', 10, 'new_alice');
```

Hive-style static partition overwrite is also supported via the `PARTITION` clause. The source query provides only non-partition columns, and partition values are specified explicitly:

```sql
-- Static partition overwrite: explicitly specify the target partition
INSERT OVERWRITE paimon.my_db.events PARTITION (dt = '2024-01-01')
VALUES (10, 'new_alice'), (20, 'new_bob');

-- With a SELECT source
INSERT OVERWRITE paimon.my_db.events PARTITION (dt = '2024-01-01')
SELECT id, name FROM source_table;
```

For multi-level partitioned tables, you can specify a subset of partition columns. Unspecified partition columns are read from the source query (dynamic partition). All sub-partitions under the specified partition are replaced:

```sql
-- Only dt is static; all data under dt='2024-01-01' is replaced.
-- region comes from the source data.
INSERT OVERWRITE paimon.my_db.events PARTITION (dt = '2024-01-01')
VALUES ('us', 10, 'alice'), ('eu', 20, 'bob');
```

### UPDATE

For append-only tables (no primary key), updates are executed using Copy-on-Write:

```sql
UPDATE paimon.my_db.t SET name = 'a_new' WHERE id = 1;
```

For primary-key tables, `data-evolution.enabled` must be enabled to perform UPDATE.

### DELETE

For append-only tables, deletes are executed using Copy-on-Write:

```sql
DELETE FROM paimon.my_db.t WHERE name = 'b';
```

### MERGE INTO

Standard SQL MERGE INTO syntax is supported, allowing INSERT, UPDATE, and DELETE in a single statement:

```sql
MERGE INTO paimon.my_db.target
USING source ON target.a = source.a
WHEN MATCHED THEN UPDATE SET a = source.a, b = source.b, c = source.c
WHEN NOT MATCHED THEN INSERT (a, b, c) VALUES (source.a, source.b, source.c);
```

Delete matched rows only:

```sql
MERGE INTO paimon.my_db.target
USING source ON target.a = source.a
WHEN MATCHED THEN DELETE;
```

UPDATE + INSERT combination:

```sql
MERGE INTO paimon.my_db.target
USING source ON target.a = source.a
WHEN MATCHED THEN UPDATE SET b = source.b
WHEN NOT MATCHED THEN INSERT (a, b, c) VALUES (source.a, source.b, source.c);
```

The source can also be a subquery:

```sql
MERGE INTO paimon.my_db.target
USING (SELECT * FROM other_table WHERE active = true) AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET name = source.name;
```

For data-evolution tables, MERGE INTO uses the `_ROW_ID` virtual column for row-level tracking. For append-only tables, it uses Copy-on-Write file rewriting.

### TRUNCATE TABLE

Truncate an entire table or specific partitions:

```sql
-- Truncate the entire table
TRUNCATE TABLE paimon.my_db.users;

-- Truncate specific partitions
TRUNCATE TABLE paimon.my_db.events PARTITION (dt = '2024-01-01');
```

### DROP PARTITION

Drop specific partitions from a table using `ALTER TABLE ... DROP PARTITION`:

```sql
ALTER TABLE paimon.my_db.events DROP PARTITION (dt = '2024-01-01');
```

Multiple partition key-value pairs can be specified:

```sql
ALTER TABLE paimon.my_db.events DROP PARTITION (dt = '2024-01-01', region = 'us');
```

## Procedures

Use `CALL` to invoke built-in procedures. All procedures are under the `sys` namespace.

### create_tag

Create a named tag from a snapshot:

```sql
CALL sys.create_tag(table => 'paimon.my_db.my_table', tag => 'my_tag', snapshot_id => 1);
```

### create_tag_from_timestamp

Create a named tag from a timestamp (finds the latest snapshot at or before the given time):

```sql
CALL sys.create_tag_from_timestamp(table => 'paimon.my_db.my_table', tag => 'my_tag', timestamp => 1234567890000);
```

### delete_tag

Delete a named tag:

```sql
CALL sys.delete_tag(table => 'paimon.my_db.my_table', tag => 'my_tag');
```

### rollback_to

Rollback a table to a specific snapshot or tag:

```sql
-- Rollback to a snapshot
CALL sys.rollback_to(table => 'paimon.my_db.my_table', snapshot_id => 1);

-- Rollback to a tag
CALL sys.rollback_to(table => 'paimon.my_db.my_table', tag => 'my_tag');
```

### rollback_to_timestamp

Rollback a table to a specific timestamp:

```sql
CALL sys.rollback_to_timestamp(table => 'paimon.my_db.my_table', timestamp => 1234567890000);
```

## Queries

### Basic Queries

All DataFusion query capabilities are supported (JOINs, aggregations, subqueries, CTEs, etc.):

```sql
SELECT id, name FROM paimon.my_db.users WHERE id > 10 ORDER BY id LIMIT 100;
```

### Column Projection

Only the required columns are read, reducing I/O:

```sql
SELECT name FROM paimon.my_db.users;
```

### Filter Pushdown

The following filter predicates are pushed down to the Paimon storage layer:

- Comparison: `=`, `!=`, `<`, `<=`, `>`, `>=`
- Logical: `AND`, `OR`
- Null checks: `IS NULL`, `IS NOT NULL`
- Range: `IN`, `NOT IN`, `BETWEEN`

Filters on partition columns enable exact partition pruning, avoiding scans of irrelevant data.

### COUNT(*) Pushdown

When the following conditions are met, `COUNT(*)` retrieves exact row counts directly from split metadata without a full table scan:

- All splits have a known `merged_row_count`
- No LIMIT clause
- Filter predicates only involve partition columns (Exact level)

## Vector Search

Paimon supports approximate nearest neighbor (ANN) vector search via the Lumina vector index. The `vector_search` table-valued function is registered as a UDTF on the DataFusion session context.

### Registration

```rust
use paimon_datafusion::register_vector_search;

register_vector_search(&ctx, catalog.clone(), "default");
```

### Usage

```sql
SELECT * FROM vector_search('table_name', 'column_name', 'query_vector_json', limit)
```

| Argument | Type | Description |
|---|---|---|
| `table_name` | STRING | Table name, fully qualified (`catalog.db.table`) or short form |
| `column_name` | STRING | The vector column to search |
| `query_vector_json` | STRING | Query vector as a JSON array of floats |
| `limit` | INT | Maximum number of results (top-k) |

Example:

```sql
SELECT * FROM vector_search('paimon.my_db.items', 'embedding', '[1.0, 0.0, 0.0, 0.0]', 10);
```

The function performs ANN search across all Lumina vector index files for the target column, merges results, and returns the top-k rows ordered by relevance score. If no matching index is found, an empty result is returned.

### Supported Metrics

The distance metric is configured at index creation time via table options:

| Metric | Description |
|---|---|
| `inner_product` | Inner product (default) |
| `cosine` | Cosine similarity |
| `l2` | Euclidean (L2) distance |

### Vector Index Options

Vector index behavior is configured via table options prefixed with `lumina.`:

| Option | Description |
|---|---|
| `lumina.dimension` | Vector dimension |
| `lumina.metric` | Distance metric (`inner_product`, `cosine`, `l2`) |
| `lumina.index-type` | Index type (default: `diskann`) |

### Environment

The Lumina native library must be available at runtime. Set the `LUMINA_LIB_PATH` environment variable to the path of the shared library, or place it in the platform default location.

## Full-Text Search

Paimon supports full-text search via the Tantivy search engine. The `full_text_search` table-valued function is registered as a UDTF on the DataFusion session context.

> **Note:** Full-text search requires the `fulltext` feature flag to be enabled on both `paimon` and `paimon-datafusion` crates.

```toml
[dependencies]
paimon = { version = "0.1.0", features = ["fulltext"] }
paimon-datafusion = { version = "0.1.0", features = ["fulltext"] }
```

### Registration

```rust
use paimon_datafusion::register_full_text_search;

register_full_text_search(&ctx, catalog.clone(), "default");
```

### Usage

```sql
SELECT * FROM full_text_search('table_name', 'column_name', 'query_text', limit)
```

| Argument | Type | Description |
|---|---|---|
| `table_name` | STRING | Table name, fully qualified (`catalog.db.table`) or short form |
| `column_name` | STRING | The text column to search |
| `query_text` | STRING | Search query (Tantivy query syntax) |
| `limit` | INT | Maximum number of results (top-k) |

Example:

```sql
SELECT * FROM full_text_search('paimon.my_db.docs', 'content', 'paimon search', 10);
```

The function searches across all Tantivy full-text index files for the target column, merges results by relevance score, and returns the top-k matching rows. If no matching index is found, an empty result is returned.

## Time Travel

Paimon supports time travel queries to read historical data.

### By Snapshot ID

```sql
SELECT * FROM paimon.default.my_table VERSION AS OF 1;
```

### By Tag Name

Use a quoted tag name with `VERSION AS OF`:

```sql
SELECT * FROM paimon.default.my_table VERSION AS OF 'my_tag';
```

Resolution order: first checks if a tag with that name exists, then tries to parse it as a snapshot ID.

### By Timestamp

Read data as of a specific point in time. The format is `YYYY-MM-DD HH:MM:SS`:

```sql
SELECT * FROM paimon.default.my_table TIMESTAMP AS OF '2024-01-01 00:00:00';
```

This finds the latest snapshot whose commit time is less than or equal to the given timestamp. The timestamp is interpreted in the local timezone.

## Dynamic Options (SET / RESET)

Use `SET` to configure session-scoped Paimon dynamic options that apply to subsequent table loads:

```sql
-- Set an option
SET 'paimon.scan.version' = '1';

-- Reset an option
RESET 'paimon.scan.version';
```

Options prefixed with `paimon.` are handled by Paimon; all others are delegated to DataFusion. Dynamic options are applied at table load time via `table.copy_with_options()`.

Example — enable BLOB descriptor mode:

```sql
SET 'paimon.blob-as-descriptor' = 'true';
SELECT * FROM paimon.my_db.assets;
RESET 'paimon.blob-as-descriptor';
```

## Temporary Tables

You can register in-memory temporary tables under any catalog. Temporary tables exist only for the lifetime of the `SQLContext` instance and are automatically cleaned up when the context is dropped.

The table name accepts flexible references, similar to DataFusion:
- `"my_table"` — uses the current catalog and current database
- `"database.my_table"` — uses the current catalog with the specified database
- `"catalog.database.my_table"` — fully qualified

### register_temp_table

Register any `Arc<dyn TableProvider>` as a temporary table (including `MemTable`, `ViewTable`, custom providers, etc.):

```rust
use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;

let schema = Arc::new(Schema::new(vec![
    Field::new("id", ArrowDataType::Int32, false),
    Field::new("name", ArrowDataType::Utf8, true),
]));
let batch = RecordBatch::try_new(
    schema.clone(),
    vec![
        Arc::new(Int32Array::from(vec![1, 2, 3])),
        Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
    ],
)?;

// Register a MemTable as a temp table
let mem_table = Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch.clone()]])?);
ctx.register_temp_table("paimon.my_db.users", mem_table)?;
let df = ctx.sql("SELECT * FROM paimon.my_db.users WHERE id > 1").await?;
df.show().await?;

// Register a ViewTable as a temp table
use datafusion::datasource::ViewTable;
let view_table = Arc::new(ViewTable::new(logical_plan, Some(query_sql)));
ctx.register_temp_table("paimon.my_db.my_view", view_table)?;
```

### CREATE TEMPORARY TABLE

You can also create temporary tables directly from SQL. See the [DDL section](#create-temporary-table) for details.

```sql
CREATE TEMPORARY TABLE paimon.my_db.source (id INT, name STRING) AS SELECT * FROM (VALUES (1, 'alice'), (2, 'bob')) AS t(id, name);
```

### CREATE TEMPORARY VIEW

Create a temporary view directly from SQL. See the [DDL section](#create-temporary-view) for details.

```sql
CREATE TEMPORARY VIEW paimon.my_db.active_users AS SELECT * FROM paimon.my_db.users WHERE id > 0;
```

### Deregister

Use `deregister_temp_table` to remove a temporary table or view programmatically, or use the `DROP TEMPORARY TABLE` / `DROP TEMPORARY VIEW` SQL statements (see the [DDL section](#drop-temporary-table--drop-temporary-view)):

```rust
ctx.deregister_temp_table("paimon.my_db.users")?;
```

Multiple temporary tables can share the same database — the database is created automatically on first use:

```rust
let mem_a = Arc::new(MemTable::try_new(schema_a, vec![vec![batch_a]])?);
let mem_b = Arc::new(MemTable::try_new(schema_b, vec![vec![batch_b]])?);
ctx.register_temp_table("my_db.table_a", mem_a)?;
ctx.register_temp_table("my_db.table_b", mem_b)?;

// Join two temp tables
let df = ctx.sql("SELECT * FROM paimon.my_db.table_a JOIN paimon.my_db.table_b ON a.id = b.id").await?;
```

## System Tables

Access table metadata via the `$` syntax.

### $options

View all configuration options for a table:

```sql
SELECT key, value FROM paimon.default.my_table$options;
```

Returns two columns: `key` (STRING) and `value` (STRING).

### $schemas

View the schema history of a table:

```sql
SELECT * FROM paimon.default.my_table$schemas;
```

Columns:

| Column | Type | Description |
|---|---|---|
| `schema_id` | BIGINT | Schema ID |
| `fields` | STRING | Field definitions (JSON) |
| `partition_keys` | STRING | Partition keys (JSON) |
| `primary_keys` | STRING | Primary keys (JSON) |
| `options` | STRING | Table options (JSON) |
| `comment` | STRING | Comment |
| `update_time` | TIMESTAMP | Update time |

### $snapshots

View the snapshot history of a table:

```sql
SELECT * FROM paimon.default.my_table$snapshots;
```

Columns:

| Column | Type | Description |
|---|---|---|
| `snapshot_id` | BIGINT | Snapshot ID |
| `schema_id` | BIGINT | Schema ID |
| `commit_user` | STRING | Commit user |
| `commit_identifier` | BIGINT | Commit identifier |
| `commit_kind` | STRING | `APPEND` / `COMPACT` / `OVERWRITE` / `ANALYZE` |
| `commit_time` | TIMESTAMP | Commit time |
| `base_manifest_list` | STRING | Base manifest list file |
| `delta_manifest_list` | STRING | Delta manifest list file |
| `changelog_manifest_list` | STRING | Changelog manifest list file |
| `total_record_count` | BIGINT | Total record count |
| `delta_record_count` | BIGINT | Delta record count |
| `changelog_record_count` | BIGINT | Changelog record count |
| `watermark` | BIGINT | Watermark |
| `next_row_id` | BIGINT | Next row id |

### $tags

View all named tags of a table:

```sql
SELECT * FROM paimon.default.my_table$tags;
```

Columns:

| Column | Type | Description |
|---|---|---|
| `tag_name` | STRING | Tag name |
| `snapshot_id` | BIGINT | Snapshot ID |
| `schema_id` | BIGINT | Schema ID |
| `commit_time` | TIMESTAMP | Commit time |
| `record_count` | BIGINT | Record count |
| `create_time` | TIMESTAMP | Tag creation time |
| `time_retained` | STRING | Retention duration |

### $manifests

View manifest files of the latest snapshot:

```sql
SELECT * FROM paimon.default.my_table$manifests;
```

Columns:

| Column | Type | Description |
|---|---|---|
| `file_name` | STRING | Manifest file name |
| `file_size` | BIGINT | File size in bytes |
| `num_added_files` | BIGINT | Number of added data files |
| `num_deleted_files` | BIGINT | Number of deleted data files |
| `schema_id` | BIGINT | Schema ID |
| `min_partition_stats` | STRING | Minimum partition stats, formatted as a Java row cast string |
| `max_partition_stats` | STRING | Maximum partition stats, formatted as a Java row cast string |
| `min_row_id` | BIGINT | Minimum row id covered (when row tracking is enabled) |
| `max_row_id` | BIGINT | Maximum row id covered (when row tracking is enabled) |

### Branch References

System tables support branch syntax:

```sql
SELECT * FROM paimon.default.my_table$branch_main$options;
```

## Table Options

Set via `WITH ('key' = 'value')` at table creation time, or dynamically via `SET`.

### Bucket Configuration

| Option | Description |
|---|---|
| `'bucket' = 'N'` | Fixed N buckets (e.g. 1, 2, 4) |
| `'bucket' = '-1'` | Dynamic bucket mode (HASH index) |
| `'bucket' = '-2'` | Postpone bucket mode (deferred assignment) |
| `'bucket-key' = 'col'` | Explicit bucket key column |

### Merge Engine

| Option | Description |
|---|---|
| `'merge-engine' = 'deduplicate'` | Deduplicate engine (default for PK tables), last write wins |
| `'merge-engine' = 'first-row'` | Keeps the first written row |

### Other Options

| Option | Description |
|---|---|
| `'sequence.field' = 'col'` | Sequence field used to determine which record wins during deduplication |
| `'data-evolution.enabled' = 'true'` | Enable data evolution (partial-column writes, row-level UPDATE/MERGE) |
| `'deletion-vectors.enabled' = 'true'` | Enable deletion vectors |
| `'cross-partition-update.enabled' = 'true'` | Allow cross-partition updates |
| `'changelog-producer' = 'input'` | Changelog producer (PK tables with input mode reject writes) |

## Full Example

```rust
use std::sync::Arc;
use paimon::{CatalogOptions, FileSystemCatalog, Options};
use paimon_datafusion::SQLContext;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create catalog
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, "file:///tmp/paimon-warehouse");
    let catalog = Arc::new(FileSystemCatalog::new(options)?);

    // Create SQL context and register catalog
    let mut ctx = SQLContext::new();
    ctx.register_catalog("paimon", catalog)?;

    // Create database and table
    ctx.sql("CREATE SCHEMA paimon.my_db").await?;
    ctx.sql(
        "CREATE TABLE paimon.my_db.users (
            id INT NOT NULL,
            name STRING,
            PRIMARY KEY (id)
        ) WITH ('bucket' = '1')"
    ).await?;

    // Insert data
    ctx.sql("INSERT INTO paimon.my_db.users VALUES (1, 'alice'), (2, 'bob')")
        .await?.collect().await?;

    // Query
    let df = ctx.sql("SELECT * FROM paimon.my_db.users ORDER BY id").await?;
    df.show().await?;

    Ok(())
}
```
