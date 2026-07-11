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
paimon = "0.3.0"
paimon-datafusion = "0.3.0"
datafusion = "54.0.0"
tokio = { version = "1", features = ["full"] }
```

Mosaic support is always available and currently read-only. SQL queries can read existing `.mosaic` files, but Paimon Rust does not write Mosaic data files yet.

## SQL Support Scope

`paimon-datafusion` currently targets Apache DataFusion 54.x. The workspace pins `datafusion = "54.0.0"`.

SQL support has two layers:

- DataFusion provides the parser, query planner, optimizer, execution engine, expressions, scalar functions, aggregate functions, and window functions. SQL statements that `SQLContext` does not intercept are delegated to DataFusion. This includes the DataFusion SQL surface for `SELECT` queries, CTEs (including recursive CTEs), subqueries, joins including `LATERAL` joins, SQL lambda functions, grouping, `HAVING`, window clauses, `QUALIFY`, set operations, `ORDER BY`, `LIMIT`/`OFFSET`, `EXPLAIN`, information-schema commands such as `SHOW TABLES`, `DESCRIBE`, `COPY`, and ordinary `INSERT`.
- Paimon-specific table management and row-level writes are implemented by `SQLContext`. This includes Paimon `CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`, `CREATE TEMPORARY TABLE`, `CREATE TEMPORARY VIEW`, REST Catalog persistent `CREATE VIEW`, `DROP VIEW`, and `CREATE FUNCTION`, `DROP TEMPORARY TABLE` / `VIEW`, `INSERT OVERWRITE ... PARTITION`, `UPDATE`, `DELETE`, `MERGE INTO`, `TRUNCATE TABLE`, `ALTER TABLE ... DROP PARTITION`, `CALL sys.*`, Paimon time travel, and `SET` / `RESET 'paimon.*'`.

Not every DataFusion DDL/DML statement maps to a Paimon table operation. For Paimon catalogs, `CREATE EXTERNAL TABLE`, `LOCATION`, `CREATE MATERIALIZED VIEW`, and persistent `CREATE TABLE AS SELECT` are rejected or not implemented. Persistent `CREATE FUNCTION` is supported only for the REST Catalog SQL scalar form documented below. DataFusion `COPY` can export query results to files; it does not create or commit Paimon table files.

For the exact delegated SQL grammar, see the [DataFusion SQL Reference](https://datafusion.apache.org/user-guide/sql/index.html).

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
    ctx.register_catalog("paimon", catalog).await?;
    let df = ctx.sql("SELECT * FROM paimon.default.my_table").await?;
    df.show().await?;
    Ok(())
}
```

`SQLContext::new` creates a session context with the Paimon relation planner
pre-registered. Use `register_catalog(...).await` to add one or more Paimon
catalogs; registering a catalog also registers the built-in scalar function
`blob_view` (alias `sys.blob_view`) and the built-in table-valued functions
(`vector_search`, `hybrid_search`, and `full_text_search` when the `fulltext`
feature is enabled) against it. It also manages session-scoped dynamic options
internally for `SET`/`RESET` support.

### REST Catalog Views and SQL Functions

When the registered catalog is a Paimon REST Catalog, `SQLContext` can read, execute, create, and drop persistent views and can create SQL scalar functions.

Create a persistent view with this syntax:

```sql
CREATE VIEW [IF NOT EXISTS] view_name [(column_name, ...)] AS query;
```

Drop a persistent view with this syntax:

```sql
DROP VIEW [IF EXISTS] view_name;
```

For example:

```sql
CREATE VIEW paimon.reporting.daily_orders (order_date, order_count) AS
SELECT order_date, COUNT(*)
FROM orders
GROUP BY order_date;
```

The defining query is planned before the view is created. Its output types and
nullability become the stored REST view schema, with field IDs assigned from
zero. An optional column list changes only the output names and must contain
exactly one unique name per query column. `IF NOT EXISTS` is passed to the
catalog so the REST server handles concurrent creates atomically.

Unqualified relations and REST SQL functions in the defining query resolve in
the new view's owning catalog and database, not the session's current database.
The canonical query is stored as both the default query and the `datafusion`
dialect definition.

Persistent `CREATE VIEW` and `DROP VIEW` are currently implemented by REST
Catalog. `DROP VIEW` sends a direct delete request; `IF EXISTS` ignores only a
missing-view response. Bare, two-part, and three-part names are supported, but
only one target may be dropped per statement. Other catalog implementations may
return `Unsupported`. `CREATE OR REPLACE VIEW`, materialized/secure views, view
comments or options, vendor-specific create modifiers, persistent `ALTER VIEW`,
and `DROP VIEW` modifiers such as `CASCADE`, `RESTRICT`, or `PURGE` are not
supported.

Persistent views resolve through the normal DataFusion catalog path, so they
can be queried wherever a table can be used:

```sql
SELECT * FROM analytics_view;
SELECT * FROM paimon.reporting.daily_orders;
```

For a view, the `datafusion` entry in `schema.dialects` is preferred. If that
entry is absent, DataFusion uses the view's default `schema.query`. Unqualified
relations inside the stored query resolve against the view's owning catalog and
database, not the caller's current database. The REST-declared output fields
are authoritative: query results are matched by position, renamed to the
declared field names, and cast to the declared types. Recursive view
dependencies are rejected during planning.

REST SQL scalar functions support bare names in the current catalog/database
and fully qualified three-part names:

```sql
SELECT normalize_score(score) FROM scores;
SELECT paimon.reporting.normalize_score(score) FROM scores;
```

Create a persistent REST SQL scalar function with this syntax:

```sql
CREATE FUNCTION [IF NOT EXISTS] function_name(
    [parameter_name data_type, ...]
)
RETURNS data_type
[LANGUAGE SQL]
RETURN scalar_expression;
```

For example:

```sql
CREATE FUNCTION paimon.reporting.add_tax(amount DECIMAL(12, 2))
RETURNS DECIMAL(12, 2)
RETURN amount * DECIMAL '1.10';

SELECT add_tax(total) FROM orders;
```

Bare, two-part (`database.function`), and three-part
(`catalog.database.function`) names are accepted as creation targets. Unquoted
names are normalized and quoted names are preserved. Calls remain limited to
bare or three-part names; two-part function calls are not supported.

Parameters must be named and cannot have modes or defaults. Zero parameters
are allowed. Inputs and the single return value are stored as nullable fields;
parameter IDs start at zero and the return field has ID `0` and name `result`.
The canonical, unexpanded `RETURN` expression is stored in
`definitions.datafusion` with `type: "sql"`.

`LANGUAGE SQL` is optional and SQL is the default, matching Databricks SQL
function syntax. `IMMUTABLE` is not required; when omitted, determinism is
inferred from the planned expression. An explicit `IMMUTABLE` clause remains
accepted for compatibility.

Before the REST create request is sent, `SQLContext` expands dependencies using
the new function as a candidate, validates argument substitution and the
declared return cast, and builds both logical and physical DataFusion plans in
the function's owning catalog/database. This rejects undeclared identifiers,
recursive dependencies (including indirect recursion), non-deterministic REST
dependencies, subqueries/table access, aggregate or window functions,
Stable/Volatile DataFusion functions, and incompatible return types. The
function is stored as deterministic only after the planned expression passes
these checks.

`IF NOT EXISTS` still validates the proposed definition first. The REST server
then handles the create atomically; only an already-existing function error is
ignored.

A function is executable only when all of the following are true:

- `definitions.datafusion` exists and has `type: "sql"`;
- input parameters are declared and the call supplies the exact number of
  positional expression arguments;
- exactly one return parameter is declared;
- the function is deterministic;
- the SQL definition is a scalar expression that references inputs by their
  declared parameter names.

Nested REST SQL functions are supported. Bare function names inside a stored
definition resolve in that function's owning catalog/database. Recursive
function dependencies, missing DataFusion SQL definitions, undeclared
identifiers, named arguments, and incompatible return types fail during
planning. If no REST function exists for a bare name, normal DataFusion
built-in or registered-function resolution continues.

Function expansion is implemented by `SQLContext::sql`. Queries executed
directly through a raw DataFusion `SessionContext` do not expand REST SQL
functions. Two-part function names such as `database.function(...)`, lambda or
file definitions, aggregate/table/multi-return functions, and non-deterministic
functions are not supported. `CREATE OR REPLACE/ALTER/TEMPORARY FUNCTION`,
non-SQL bodies, `STABLE`/`VOLATILE`, null-input/parallel/security/SET clauses,
options/remote functions, and persistent `ALTER FUNCTION` / `DROP FUNCTION`
are also not supported. Catalog implementations other than REST Catalog may
return `Unsupported` for persistent function creation.

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
| `BINARY` / `VARBINARY` / `BYTEA` / `BYTES` | VarBinaryType | |
| `VARIANT` | VariantType | Semi-structured value encoded as value + metadata binary buffers |
| `BLOB` | BlobType | Binary large object |
| `DATE` | DateType | |
| `TIMESTAMP[(p)]` | TimestampType | Precision p: 0/3/6/9, default 3 |
| `TIMESTAMP WITH TIME ZONE` | LocalZonedTimestampType | |
| `DECIMAL(p, s)` | DecimalType | |
| `ARRAY<element>` | ArrayType | e.g. `ARRAY<INT>` |
| `MAP(key, value)` | MapType | e.g. `MAP(STRING, INT)` |
| `STRUCT<field TYPE, ...>` | RowType | e.g. `STRUCT<city STRING, zip INT>` |

For vector search tables created from SQL, use `ARRAY<FLOAT>` for embedding
columns. Existing Paimon tables may also expose logical `VECTOR<FLOAT,N>`
columns; DataFusion reads those as Arrow `FixedSizeList<Float32>`, and vindex
index creation uses `N` as the vector dimension. `SHOW CREATE TABLE` currently
does not round-trip `VECTOR` columns.

### Blob Columns

BLOB columns store large binary values using Paimon's dedicated BLOB layout.
Declare them as top-level columns and enable data evolution:

```sql
CREATE TABLE paimon.my_db.assets (
    id INT,
    picture BLOB
) WITH (
    'data-evolution.enabled' = 'true'
);
```

For Java-compatible DDL, DataFusion also supports the BLOB comment directives
used by Java Paimon. A binary column with one of these directive comments is
normalized to a Paimon BLOB column in the core schema layer:

```sql
CREATE TABLE paimon.my_db.assets (
    id INT,
    picture BYTES COMMENT '__BLOB_FIELD; original image',
    thumbnail BYTES COMMENT '__BLOB_DESCRIPTOR_FIELD; descriptor bytes',
    picture_ref BYTES COMMENT '__BLOB_VIEW_FIELD; upstream image reference'
) WITH (
    'data-evolution.enabled' = 'true'
);
```

The directive is stripped from the stored column comment; text after the first
semicolon is kept as the real comment. The directives also populate the matching
table options. A comment directive that starts with `__BLOB` but is not one of
the supported directives is rejected.

| Comment directive | Table option | Storage semantics |
| --- | --- | --- |
| `__BLOB_FIELD` | `blob-field` | Store BLOB bytes in dedicated `.blob` files |
| `__BLOB_DESCRIPTOR_FIELD` | `blob-descriptor-field` | Store serialized `BlobDescriptor` bytes inline |
| `__BLOB_VIEW_FIELD` | `blob-view-field` | Store serialized `BlobViewStruct` bytes inline |

The same directives are supported by `ALTER TABLE ... ADD COLUMN`.

### Blob View

Blob View stores an inline reference to a BLOB value in another table, using a
Java-compatible `BlobViewStruct` payload. It is useful when one table should
point at media or large binary content owned by an upstream table without
copying the bytes at write time.

Declare Blob View columns as top-level BLOB columns and list them in the
`blob-view-field` table option:

```sql
CREATE TABLE paimon.my_db.asset_refs (
    id INT,
    picture BLOB
) WITH (
    'data-evolution.enabled' = 'true',
    'row-tracking.enabled' = 'true',
    'blob-view-field' = 'picture'
);
```

Use `blob_view(table, field_name_or_id, row_id)` or `sys.blob_view(...)` to
create the reference. The table argument may be `table`, `database.table`, or
`catalog.database.table`; the stored reference contains the resolved
`database.table`, field id, and row id. In typical SQL, read `_ROW_ID` from a
row-tracking source table:

```sql
CREATE TABLE paimon.my_db.assets (
    id INT,
    picture BLOB
) WITH (
    'data-evolution.enabled' = 'true',
    'row-tracking.enabled' = 'true'
);

INSERT INTO paimon.my_db.asset_refs (id, picture)
SELECT
    id,
    sys.blob_view('my_db.assets', 'picture', "_ROW_ID")
FROM paimon.my_db.assets;
```

By default, RESTCatalog-backed reads resolve Blob View fields to the upstream
BLOB value by reusing the table's REST environment. Other catalog types
currently preserve the raw serialized `BlobViewStruct` bytes. Set the dynamic
option `paimon.blob-view.resolve.enabled` to `false` to preserve raw references
even for RESTCatalog-backed reads:

```sql
SET 'paimon.blob-view.resolve.enabled' = 'false';
SELECT id, picture FROM paimon.my_db.asset_refs;
RESET 'paimon.blob-view.resolve.enabled';
```

Like ordinary BLOB reads, `paimon.blob-as-descriptor = true` makes resolved Blob
View columns return serialized BLOB descriptors instead of loading the BLOB
bytes.

### Variant Usage

`VARIANT` stores semi-structured data using the same logical value + metadata binary shape as Paimon Java. Use it for JSON-like fields whose schema may differ row by row.

Create `VARIANT` columns like ordinary table columns:

```sql
CREATE TABLE paimon.my_db.user_events (
    user_id BIGINT NOT NULL,
    event_time TIMESTAMP,
    payload VARIANT,
    attributes VARIANT,
    dt STRING,
    PRIMARY KEY (user_id, dt)
) PARTITIONED BY (dt)
WITH ('bucket' = '4');
```

`VARIANT` columns can be nullable or `NOT NULL`:

```sql
CREATE TABLE paimon.my_db.variant_examples (
    id INT NOT NULL,
    payload VARIANT NOT NULL,
    optional_payload VARIANT
);
```

Do not use `VARIANT` as a partition column. Partition values must be scalar strings, numbers, dates, or timestamps that can be encoded as stable partition names.

Use `parse_json` when inserting JSON text into a `VARIANT` column:

```sql
INSERT INTO paimon.my_db.user_events VALUES
(
    1,
    TIMESTAMP '2024-01-01 10:00:00',
    parse_json('{"event":"login","device":{"os":"ios","version":17},"score":98.5}'),
    parse_json('{"city":"Beijing","tags":["new","mobile"],"vip":true}'),
    '2024-01-01'
);
```

`parse_json` rejects invalid JSON and duplicate object keys. Use `try_parse_json` when malformed JSON should become SQL `NULL` instead of failing the query:

```sql
INSERT INTO paimon.my_db.user_events
SELECT
    user_id,
    event_time,
    try_parse_json(raw_payload),
    try_parse_json(raw_attributes),
    dt
FROM staging_events;
```

`SQLContext::new` registers Spark-compatible scalar functions for common `VARIANT` workflows:

```sql
SELECT
    user_id,
    variant_get(payload, '$.event', 'string') AS event_name,
    variant_get(payload, '$.device.os', 'string') AS os,
    variant_get(payload, '$.score', 'double') AS score,
    variant_get(attributes, '$.tags[0]', 'string') AS first_tag
FROM paimon.my_db.user_events
WHERE variant_get(attributes, '$.vip', 'boolean') = true;
```

Supported functions:

| Function | Notes |
|---|---|
| `parse_json(json)` | Parses a JSON string into `VARIANT`; invalid JSON returns an error |
| `try_parse_json(json)` | Parses a JSON string into `VARIANT`; invalid JSON returns `NULL` |
| `variant_get(v, path[, type])` | Extracts a path; missing paths return `NULL`; invalid casts return an error |
| `try_variant_get(v, path[, type])` | Extracts a path; missing paths, invalid paths, and invalid casts return `NULL` |
| `is_variant_null(v)` | Returns true for JSON `null` inside `VARIANT`; SQL `NULL` returns false |

Path syntax supports the root path `$`, object access (`$.field`), quoted object access (`$["field"]` or `$['field']`), array indexes (`$[0]`), and nested combinations such as `$.items[0].price`.

The optional `type` argument is a string literal. Supported result types are `variant` (or omitted), `boolean`, `byte` / `tinyint`, `short` / `smallint`, `int` / `integer`, `long` / `bigint`, `float`, `double`, `decimal(p, s)`, and `string`.

When `type` is omitted or set to `variant`, `variant_get` returns a nested `VARIANT` value that can be passed to another `variant_get` call:

```sql
SELECT
    variant_get(
        variant_get(payload, '$.device'),
        '$.os',
        'string'
    ) AS os
FROM paimon.my_db.user_events;
```

Missing paths return SQL `NULL`. JSON `null` is represented as a non-SQL-null Variant value, so use `is_variant_null` when you need to distinguish it:

```sql
SELECT
    is_variant_null(parse_json('null')) AS json_null,
    is_variant_null(NULL) AS sql_null;
```

### Variant Shredding

Variant shredding stores selected fields from a `VARIANT` column as typed
physical fields in Parquet files while keeping the logical table schema as
`VARIANT`. Reads are automatic: when a projected `VARIANT` column is stored in
shredded physical form, Paimon Rust assembles it back into the normal
value + metadata representation before returning the batch.

Use a configured shredding schema when the hot fields are known in advance:

```sql
CREATE TABLE paimon.my_db.shredded_events (
    user_id BIGINT,
    payload VARIANT
) WITH (
    'file.format' = 'parquet',
    'variant.shreddingSchema' =
        '{"type":"ROW","fields":[{"name":"payload","type":{"type":"ROW","fields":[{"name":"event","type":"STRING"},{"name":"score","type":"DOUBLE"},{"name":"city","type":"STRING"}]}}]}'
);
```

The configured schema is a Paimon `ROW` type encoded as JSON. Field IDs may be
omitted; Paimon Rust assigns them by position. Each top-level field name must
match a `VARIANT` column to shred. The field's type describes the typed fields
to extract from that Variant value; values that do not match the typed field
still remain in the Variant payload so the logical value can be rebuilt on read.

Use inferred shredding when the hot fields should be discovered from the first
rows written by each data-file writer:

```sql
CREATE TABLE paimon.my_db.inferred_events (
    user_id BIGINT,
    payload VARIANT
) WITH (
    'file.format' = 'parquet',
    'variant.inferShreddingSchema' = 'true',
    'variant.shredding.maxInferBufferRow' = '4096',
    'variant.shredding.maxSchemaDepth' = '50',
    'variant.shredding.maxSchemaWidth' = '300',
    'variant.shredding.minFieldCardinalityRatio' = '0.1'
);
```

When both configured and inferred shredding are set, the configured schema takes
precedence. Shredding currently applies to Parquet data-file writes; ordinary
non-shredded `VARIANT` files continue to read normally.

Current limitations:

- `schema_of_variant`, `schema_of_variant_agg`, `to_variant_object`, `variant_explode`, and `variant_explode_outer` are not implemented yet.
- `variant_get` currently casts to scalar types and `VARIANT`. It does not yet cast directly to `ARRAY`, `MAP`, or `STRUCT`.
- Simple `variant_get` and `try_variant_get` expressions over a `VARIANT` column, a literal path, and a scalar literal type can be pushed into scans as Variant extraction fields for projections and filters. Predicate translation through `variant_get` is still not applied to Paimon/Parquet statistics; DataFusion evaluates those filters after reading the extracted field.

With a raw DataFusion `SessionContext`, register these scalar functions explicitly:

```rust
use paimon_datafusion::register_variant_functions;

register_variant_functions(&ctx);
```

## DDL

### CREATE DATABASE / CREATE SCHEMA / DROP SCHEMA

```sql
CREATE SCHEMA paimon.my_db;
CREATE DATABASE paimon.my_db;
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

### DROP VIEW

Drop one persistent view from a REST Catalog:

```sql
DROP VIEW active_users;
DROP VIEW IF EXISTS my_db.active_users;
DROP VIEW IF EXISTS paimon.my_db.active_users;
```

`IF EXISTS` ignores only a missing view; authorization, server, and network errors are still returned. Only one persistent view target is supported per statement. `CASCADE`, `RESTRICT`, `PURGE`, and other drop modifiers are rejected. Catalog implementations without persistent view support return `Unsupported`.

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

The table type determines which row-level DML operations are supported:

| Operation | Append-only table | Primary-key table | Data-evolution row-tracking table (no primary key) |
|---|---|---|---|
| `INSERT INTO` | Supported | Supported | Supported |
| `INSERT OVERWRITE` | Supported | Supported | Supported |
| `INSERT OVERWRITE ... PARTITION` | Supported for partitioned tables | Supported for partitioned tables | Supported for partitioned tables |
| `TRUNCATE TABLE` | Supported | Supported | Supported |
| `ALTER TABLE ... DROP PARTITION` | Supported for partitioned tables | Supported for partitioned tables | Supported for partitioned tables |
| `UPDATE` | Supported via Copy-on-Write | Not supported | Supported via row-id update |
| `DELETE` | Supported via Copy-on-Write | Not supported | Supported when deletion vectors are enabled |
| `MERGE INTO` | Supported via Copy-on-Write | Not supported | Supported for matched `UPDATE`, matched `DELETE` with deletion vectors, and not-matched `INSERT` |

A data-evolution row-tracking table must have both `'data-evolution.enabled' = 'true'` and `'row-tracking.enabled' = 'true'`, and must not have primary keys. `DELETE` and matched `DELETE` in `MERGE INTO` additionally require `'deletion-vectors.enabled' = 'true'`. Primary-key row-level `UPDATE`, `DELETE`, and `MERGE INTO` are not supported even when data evolution is enabled.

### INSERT INTO

```sql
INSERT INTO paimon.my_db.users VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');
```

`INSERT INTO ... SELECT ...` is also supported:

```sql
INSERT INTO paimon.my_db.users SELECT * FROM source_table;
```

For `VARIANT` columns, convert JSON text with `parse_json` or `try_parse_json`:

```sql
INSERT INTO paimon.my_db.user_events (user_id, event_time, payload, attributes, dt)
VALUES (
    1,
    TIMESTAMP '2024-01-01 10:00:00',
    parse_json('{"event":"login","device":{"os":"ios"}}'),
    try_parse_json('{"vip":true,"tags":["mobile"]}'),
    '2024-01-01'
);
```

For primary-key tables, records with duplicate keys are deduplicated according to the merge engine (default: Deduplicate engine, where the last written value wins).

### Mosaic Read Scope

The Mosaic reader supports scalar, temporal, array, and map columns. It uses row-group statistics for conservative pruning when they are present. This pruning is not row-level filter enforcement; DataFusion still applies SQL filters above the reader to produce exact query results.

Unsupported or limited Mosaic areas include writing `.mosaic` files, emitting manifest `value_stats` for Mosaic writes, Mosaic bloom filters, and Mosaic-specific performance tuning.

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

For data-evolution row-tracking tables without primary keys, updates are executed with row-id-based partial-column writes. Primary-key tables are not supported for `UPDATE`.

### DELETE

For append-only tables, deletes are executed using Copy-on-Write:

```sql
DELETE FROM paimon.my_db.t WHERE name = 'b';
```

For data-evolution row-tracking tables without primary keys, deletes are executed via deletion vectors and require `'deletion-vectors.enabled' = 'true'`.

`DELETE` is not supported on primary-key tables.

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

For append-only tables, `MERGE INTO` uses Copy-on-Write file rewriting and supports matched `UPDATE`, matched `DELETE`, and not-matched `INSERT`. For data-evolution row-tracking tables without primary keys, `MERGE INTO` uses the `_ROW_ID` virtual column for row-level tracking and supports matched `UPDATE`, matched `DELETE` when deletion vectors are enabled, and not-matched `INSERT`. Primary-key tables are not supported for `MERGE INTO`.

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

### create_global_index

Build and commit a global index for a table column:

```sql
CALL sys.create_global_index(
  table => 'paimon.my_db.my_table',
  index_column => 'id',
  index_type => 'btree'
);

CALL sys.create_global_index(
  table => 'paimon.my_db.my_table',
  index_column => 'tag',
  index_type => 'bitmap'
);
```

`index_type` defaults to `btree`. BTree and bitmap global indexes support
scalar columns and do not accept the `options` argument yet. Bitmap global
indexes use the same on-disk file format as Java Paimon's
`BitmapGlobalIndexFormat`.

The current global-index builders require a row-tracking data-evolution table
with global indexes enabled. They do not support primary-key tables or tables
with deletion vectors enabled:

```sql
CREATE TABLE paimon.my_db.items (
  id INT,
  embedding ARRAY<FLOAT>
) WITH (
  'bucket' = '1',
  'row-tracking.enabled' = 'true',
  'data-evolution.enabled' = 'true',
  'global-index.enabled' = 'true',
  'global-index.row-count-per-shard' = '100000'
);
```

For vector indexes backed by vindex, set `index_type` to one of `ivf-flat`,
`ivf-pq`, `ivf-hnsw-flat`, or `ivf-hnsw-sq`:

```sql
CALL sys.create_global_index(
  table => 'paimon.my_db.items',
  index_column => 'embedding',
  index_type => 'ivf-flat',
  options => 'ivf-flat.dimension=4,ivf-flat.nlist=256,ivf-flat.distance.metric=inner_product'
);
```

The `options` argument is a comma-separated `key=value` string. User options
override table options. Use keys prefixed by the selected index type, or set
field-level table options with `fields.<column>.<option>`:

```sql
CREATE TABLE paimon.my_db.image_items (
  id INT,
  embedding ARRAY<FLOAT>
) WITH (
  'bucket' = '1',
  'row-tracking.enabled' = 'true',
  'data-evolution.enabled' = 'true',
  'global-index.enabled' = 'true',
  'fields.embedding.dimension' = '768',
  'fields.embedding.distance.metric' = 'cosine',
  'fields.embedding.nlist' = '1024'
);
```

Supported vindex options:

| Option | Default | Applies To | Description |
|---|---:|---|---|
| `<index-type>.dimension` | `128` | all vindex types | Vector dimension for `ARRAY<FLOAT>` columns. Existing `VECTOR<FLOAT,N>` columns use `N` from the type. |
| `<index-type>.distance.metric` | `inner_product` | all vindex types | Distance metric: `inner_product`, `cosine`, or `l2`. |
| `<index-type>.nlist` | `256` | all vindex types | Number of IVF lists. |
| `<index-type>.pq.m` | `16` | `ivf-pq` | Number of product-quantization sub-vectors. The dimension must be divisible by this value. |
| `<index-type>.pq.use-opq` | `false` | `ivf-pq` | Whether to enable OPQ before PQ encoding. |
| `<index-type>.hnsw.m` | native default | HNSW vindex types | HNSW graph connectivity. |
| `<index-type>.hnsw.ef-construction` | native default | HNSW vindex types | HNSW construction beam width. |
| `<index-type>.hnsw.max-level` | native default | HNSW vindex types | Maximum HNSW graph level. |

Native vindex aliases are also accepted in the `options` string: `dimension`,
`metric`, `nlist`, `pq.m`, `use-opq`, and `hnsw.*`.

Inspect committed index files with the `$table_indexes` system table:

```sql
SELECT index_type, index_field_name, row_count, row_range_start, row_range_end
FROM paimon.my_db.items$table_indexes;
```

### drop_global_index

Drop a committed sorted global index:

```sql
CALL sys.drop_global_index(
  table => 'paimon.my_db.my_table',
  index_column => 'id',
  index_type => 'btree'
);
```

BTree and bitmap global indexes can be dropped through this procedure currently.

### create_lumina_index

Build and commit a Lumina global vector index for a table column:

```sql
CALL sys.create_lumina_index(table => 'paimon.my_db.my_table', index_column => 'embedding');
```

The optional `index_type` argument selects the Lumina index identifier. It defaults to
`lumina`. Valid values are `lumina` and the legacy-compatible `lumina-vector-ann`.

```sql
CALL sys.create_lumina_index(
  table => 'paimon.my_db.my_table',
  index_column => 'embedding',
  index_type => 'lumina'
);
```

Optional Lumina builder settings can be supplied as comma-separated `key=value` pairs:

```sql
CALL sys.create_lumina_index(
  table => 'paimon.my_db.my_table',
  index_column => 'embedding',
  options => 'lumina.index.dimension=128,lumina.encoding.type=pq'
);
```

## Queries

### Basic Queries

All DataFusion query capabilities are supported (JOINs, aggregations, subqueries, CTEs, etc.):

```sql
SELECT id, name FROM paimon.my_db.users WHERE id > 10 ORDER BY id LIMIT 100;
```

### Variant Queries

Use `variant_get` to extract fields from `VARIANT` columns. Provide a target type string when the query needs a scalar result:

```sql
SELECT
    user_id,
    variant_get(payload, '$.event', 'string') AS event_name,
    variant_get(payload, '$.device.os', 'string') AS device_os,
    variant_get(attributes, '$.vip', 'boolean') AS is_vip
FROM paimon.my_db.user_events
WHERE variant_get(payload, '$.event', 'string') = 'login';
```

Use `try_variant_get` when incompatible values should return `NULL`:

```sql
SELECT
    user_id,
    try_variant_get(payload, '$.score', 'double') AS score
FROM paimon.my_db.user_events;
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
- String predicates: positive `LIKE`, including no-wildcard, prefix, suffix, contains, and more complex patterns. `NOT LIKE` and `ILIKE` are evaluated by DataFusion as residual filters.

Filters on partition columns enable exact partition pruning, avoiding scans of irrelevant data.

### COUNT(*) Pushdown

When the following conditions are met, `COUNT(*)` retrieves exact row counts directly from split metadata without a full table scan:

- All splits have a known `merged_row_count`
- No LIMIT clause
- Filter predicates only involve partition columns (Exact level)

## Python Multimodal Helper Functions

When you use `pypaimon_rust.datafusion.SQLContext`, the Python binding registers a small set of scalar helper functions for BLOB-backed media and vector workflows. These helpers are Python-binding built-ins; they are not registered by the Rust `paimon_datafusion::SQLContext`.

Media helpers require the optional Python media dependencies:

```shell
pip install "pypaimon-rust[video]"
```

| Function | Return Type | Description |
|---|---|---|
| `media_info(blob)` | STRING | JSON metadata for image, video, or audio input |
| `media_thumbnail(blob)` | BINARY | PNG thumbnail, using a default 320x320 bounding box |
| `media_thumbnail(blob, max_width, max_height)` | BINARY | PNG thumbnail constrained to the given dimensions |
| `video_snapshot(blob)` | BINARY | PNG frame near timestamp 0ms |
| `video_snapshot(blob, timestamp_ms)` | BINARY | PNG frame near the given timestamp |
| `video_frame(blob, frame_index)` | BINARY | PNG frame by zero-based decoded frame index |
| `vector_from_json(json)` | `List<Float32>` | Converts a JSON float array string into an Arrow float vector |
| `vector_to_json(vector)` | STRING | Converts an Arrow float vector back to a JSON array string |

Invalid, NULL, unsupported, or undecodable media inputs return SQL `NULL`. Media functions read either inline bytes or BLOB descriptor bytes when the `SQLContext` has a registered Paimon catalog that can resolve the descriptor.

Example:

```sql
SELECT
    id,
    media_info(content) AS info_json,
    media_thumbnail(content, 160, 90) AS preview_png,
    video_frame(content, 10) AS frame_png
FROM paimon.my_db.assets;
```

Use `vector_from_json` to bridge JSON-encoded embeddings into lateral vector search queries:

```sql
WITH queries AS (
    SELECT id, vector_from_json(embedding_json) AS embedding
    FROM paimon.my_db.query_embeddings
)
SELECT q.id AS query_id, r.id AS result_id
FROM queries q
CROSS JOIN LATERAL vector_search(
    'paimon.my_db.items',
    'embedding',
    q.embedding,
    10
) AS r;
```

## Vector Search

Paimon supports approximate nearest neighbor (ANN) vector search through global
vector indexes. DataFusion can search vindex indexes created by
`CALL sys.create_global_index` and Lumina indexes created by
`CALL sys.create_lumina_index`. The `vector_search` table-valued function is
registered as a UDTF on the DataFusion session context.

### Registration

When you use a `SQLContext`, `vector_search` is registered automatically for every catalog you register — no extra setup is needed.

With a raw DataFusion `SessionContext`, register it explicitly:

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

The function performs ANN search across all matching vector index files for the
target column, merges results, and returns the top-k rows ordered by relevance
score. If no matching index is found, an empty result is returned.

### Refine / Rerank

Vector index search can optionally refine ANN results by reading the raw vectors
for a larger candidate set, recomputing exact scores, and reranking the final
top-k results. This is useful for quantized indexes, such as IVF-PQ, where ANN
scores are approximate.

Refine is disabled by default. Configure a positive refine factor as a table
property. The search first requests `limit * refine_factor` candidates from the
index, then reranks those candidates by exact raw-vector scores and keeps the
requested `limit` rows. A factor of `1` still performs exact reranking over the
original `limit` candidates, but does not over-fetch additional candidates.
Omit the option to keep the default ANN-only behavior.

Set the option when creating the table:

```sql
CREATE TABLE paimon.my_db.items (
  id INT,
  embedding ARRAY<FLOAT>
) WITH (
  'bucket' = '1',
  'row-tracking.enabled' = 'true',
  'data-evolution.enabled' = 'true',
  'global-index.enabled' = 'true',
  'fields.embedding.ivf.refine-factor' = '3'
);
```

Or enable it on an existing table:

```sql
ALTER TABLE paimon.my_db.items SET TBLPROPERTIES(
  'fields.embedding.ivf.refine-factor' = '3'
);
```

Then run `vector_search` as usual:

```sql
SELECT *
FROM vector_search('paimon.my_db.items', 'embedding', '[1.0, 0.0, 0.0, 0.0]', 10);
```

Supported refine option names are `refine_factor`, `refine-factor`,
`rerank_factor`, and `rerank-factor`. Option lookup checks field-specific and
index-specific keys before global keys. For example, for an `ivf-flat` index on
column `embedding`, these keys are accepted, from more specific to more general:

| Example Key | Scope |
|---|---|
| `fields.embedding.ivf-flat.refine-factor` | This column and index type |
| `fields.embedding.ivf_flat.refine-factor` | This column and normalized index type |
| `fields.embedding.ivf.refine-factor` | This column and all IVF vector indexes |
| `fields.embedding.refine-factor` | This column |
| `ivf-flat.refine-factor` | This index type |
| `ivf_flat.refine-factor` | This normalized index type |
| `ivf.refine-factor` | All IVF vector indexes |
| `refine-factor` | All vector searches on the table |

Larger refine factors may improve recall and ordering quality, but they also
increase index result merging, raw-vector reads, and exact scoring work. Use the
smallest factor that provides the desired recall.

### Lateral Joins

Use `CROSS JOIN LATERAL` when query vectors come from another relation. In this mode, the third `vector_search` argument is a column reference from the left side of the join instead of a JSON literal:

```sql
SELECT q.id AS query_id, r.id AS result_id
FROM paimon.my_db.queries q
CROSS JOIN LATERAL vector_search(
    'paimon.my_db.items',
    'embedding',
    q.embedding,
    10
) AS r
ORDER BY query_id, result_id;
```

The query-vector column must have Arrow type `List<Float32>` or `FixedSizeList<Float32>`. Null query-vector rows produce no joined results, and null elements inside a vector are rejected. The lateral form returns the left row joined with the top-k matching rows from the target Paimon table for that row's query vector.

### Supported Metrics

The distance metric is configured at index creation time via table options:

| Metric | Description |
|---|---|
| `inner_product` | Inner product (default) |
| `cosine` | Cosine similarity |
| `l2` | Euclidean (L2) distance |

### Vindex Index Options

For vindex-backed search, build the index with
`CALL sys.create_global_index` and an index type such as `ivf-flat` or
`ivf-pq`. See [create_global_index](#create_global_index) for the supported
index types, table requirements, and option keys.

### Lumina Index Options

Lumina index behavior is configured via table options prefixed with `lumina.`:

| Option | Description |
|---|---|
| `lumina.index.dimension` | Vector dimension |
| `lumina.distance.metric` | Distance metric (`inner_product`, `cosine`, `l2`) |
| `lumina.index.type` | Index type (default: `diskann`) |
| `lumina.encoding.type` | Encoding type (default: `pq`) |

### Lumina Environment

The Lumina native library must be available at runtime. Set the `LUMINA_LIB_PATH` environment variable to the path of the shared library, or place it in the platform default location.

## Hybrid Search

Paimon supports hybrid search by combining multiple search routes and ranking the merged results. The `hybrid_search` table-valued function is registered as a UDTF on the DataFusion session context.

Hybrid search does not require the `fulltext` feature when all routes are vector routes. Enable `fulltext` only when you include full-text routes.

### Registration

When you use a `SQLContext`, `hybrid_search` is registered automatically for every catalog you register — no extra setup is needed.

With a raw DataFusion `SessionContext`, register it explicitly:

```rust
use paimon_datafusion::register_hybrid_search;

register_hybrid_search(&ctx, catalog.clone(), "default");
```

### Usage

```sql
SELECT * FROM hybrid_search(
    'table_name',
    vector_routes,
    full_text_routes,
    limit,
    'ranker'
)
```

| Argument | Type | Description |
|---|---|---|
| `table_name` | STRING | Table name, fully qualified (`catalog.db.table`) or short form |
| `vector_routes` | ARRAY | Vector route definitions; use `array()` when no vector route is needed |
| `full_text_routes` | ARRAY | Full-text route definitions; use `array()` for vector-only hybrid search |
| `limit` | INT | Maximum number of merged results (top-k) |
| `ranker` | STRING | Optional ranker: `rrf` (default), `weighted_score`, or `mrr` |

Route definitions use Spark-compatible `array(named_struct(...))` syntax. A vector route accepts `field` (or `vector_column`), `query_vector`, optional `limit`, optional `weight`, and optional `options`:

```sql
SELECT *
FROM hybrid_search(
    'paimon.my_db.items',
    array(
        named_struct(
            'field', 'title_embedding',
            'query_vector', array(1.0, 0.0, 0.0, 0.0),
            'limit', 20,
            'weight', 1.0
        ),
        named_struct(
            'field', 'body_embedding',
            'query_vector', array(0.9, 0.1, 0.0, 0.0),
            'limit', 20,
            'weight', 0.7
        )
    ),
    array(),
    10,
    'rrf'
);
```

A full-text route accepts `column`, `query`, optional `limit`, and optional `weight`. Full-text routes require the `fulltext` feature:

```sql
SELECT *
FROM hybrid_search(
    'paimon.my_db.docs',
    array(
        named_struct(
            'field', 'embedding',
            'query_vector', array(1.0, 0.0, 0.0, 0.0),
            'limit', 20,
            'weight', 1.0
        )
    ),
    array(
        named_struct(
            'column', 'content',
            'query', 'paimon search',
            'limit', 20,
            'weight', 0.8
        )
    ),
    10,
    'weighted_score'
);
```

The function searches each route independently, merges route results with the selected ranker, and returns the top-k matching rows from the target table. The current DataFusion table function returns table rows only; it does not expose a metadata score column.

## Full-Text Search

Paimon supports full-text search via the Tantivy search engine. The `full_text_search` table-valued function is registered as a UDTF on the DataFusion session context.

> **Note:** Full-text search requires the `fulltext` feature flag to be enabled on both `paimon` and `paimon-datafusion` crates.

```toml
[dependencies]
paimon = { version = "0.3.0", features = ["fulltext"] }
paimon-datafusion = { version = "0.3.0", features = ["fulltext"] }
```

### Registration

When you use a `SQLContext`, `full_text_search` is registered automatically for every catalog you register (when the `fulltext` feature is enabled) — no extra setup is needed.

With a raw DataFusion `SessionContext`, register it explicitly:

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

Example — preserve Blob View references instead of resolving upstream BLOB
values on RESTCatalog-backed reads:

```sql
SET 'paimon.blob-view.resolve.enabled' = 'false';
SELECT * FROM paimon.my_db.asset_refs;
RESET 'paimon.blob-view.resolve.enabled';
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

Use `deregister_temp_table` to remove a temporary table or view programmatically, or use the `DROP TEMPORARY TABLE` / `DROP TEMPORARY VIEW` SQL statements (see the [DDL section](#drop-temporary-table-drop-temporary-view)):

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

### $partitions

View all partitions of a table with aggregated record counts and file sizes:

```sql
SELECT * FROM paimon.default.my_table$partitions;
```

Columns:

| Column | Type | Description |
|---|---|---|
| `partition` | STRING | Partition spec, formatted as `key1=val1/key2=val2` |
| `record_count` | BIGINT | Total record count across all data files in the partition |
| `file_size_in_bytes` | BIGINT | Total file size in bytes |
| `file_count` | BIGINT | Number of data files |
| `last_update_time` | TIMESTAMP | Latest data-file creation time |
| `created_at` | TIMESTAMP | Partition creation time (only available with metastore-tracked catalogs) |
| `created_by` | STRING | Snapshot id that created the partition (catalog-tracked only) |
| `updated_by` | STRING | Snapshot id that last updated the partition (catalog-tracked only) |
| `options` | STRING | Per-partition options as flat JSON (catalog-tracked only) |
| `total_buckets` | INT | Total bucket count for the partition (0 unless catalog-tracked) |
| `done` | BOOLEAN | Whether the partition is marked done (false unless catalog-tracked) |

### $table_indexes

View committed global index files, including BTree indexes, vector indexes, and
deletion-vector metadata:

```sql
SELECT * FROM paimon.default.my_table$table_indexes;
```

Columns:

| Column | Type | Description |
|---|---|---|
| `partition` | STRING | Partition spec for the indexed data, or `NULL` for unpartitioned tables |
| `bucket` | INT | Bucket id covered by the index file |
| `index_type` | STRING | Index type, such as `btree`, `bitmap`, `ivf-flat`, `lumina`, or `DELETION_VECTORS` |
| `file_name` | STRING | Index file name under the table index directory |
| `file_size` | BIGINT | Index file size in bytes |
| `row_count` | BIGINT | Number of rows covered by the index file |
| `dv_ranges` | ARRAY | Deletion-vector ranges, only populated for deletion-vector metadata |
| `row_range_start` | BIGINT | First row id covered by the index file |
| `row_range_end` | BIGINT | Last row id covered by the index file |
| `index_field_id` | INT | Field id of the indexed column |
| `index_field_name` | STRING | Name of the indexed column |

### $physical_files_size

Scan the table directory recursively and compute the total size of recognized physical files on disk, categorized by file type. This table is a diagnostic size summary; orphan cleanup needs file-level candidates and retention checks, not just aggregate size differences.

Files are classified by their table-relative path:
- `manifest/manifest-*`, `manifest/manifest-list-*`, and `manifest/index-manifest-*` → manifest
- `statistics/*` → manifest file counters for the current compatible output schema
- `index/*` → index
- `<partition>/bucket-*/*` and `<partition>/bucket-postpone/*` → data, using the table's partition depth
- unknown files are ignored by this summary

```sql
SELECT * FROM paimon.default.my_table$physical_files_size;
```

Columns:

| Column | Type | Description |
|---|---|---|
| `manifest_file_count` | BIGINT | Number of manifest files on disk |
| `manifest_file_size` | BIGINT | Total size of manifest files (bytes) |
| `data_file_count` | BIGINT | Number of recognized data files on disk |
| `data_file_size` | BIGINT | Total size of recognized data files (bytes) |
| `index_file_count` | BIGINT | Number of index files on disk |
| `index_file_size` | BIGINT | Total size of index files (bytes) |

### $referenced_files_size

Compute aggregated manifest/data/index file size summaries for all snapshots referenced by a table, including snapshots from the main branch, tags, and other branches. This is useful for understanding storage usage and for orphan file analysis.

Historical snapshots may be in the process of being cleaned up — if a manifest file has already been deleted, it is gracefully skipped (counted as 0 files/bytes).

```sql
SELECT * FROM paimon.default.my_table$referenced_files_size;
```

Columns:

| Column | Type | Description |
|---|---|---|
| `source` | STRING | Scope: `total` or `branch:<name>` |
| `manifest_file_count` | BIGINT | Number of manifest files |
| `manifest_file_size` | BIGINT | Total size of manifest files (bytes) |
| `data_file_count` | BIGINT | Number of data files |
| `data_file_size` | BIGINT | Total size of data files (bytes) |
| `index_file_count` | BIGINT | Number of index files |
| `index_file_size` | BIGINT | Total size of index files (bytes) |

The output contains one row per scope:
- `total` — sum across all branches and tags
- `branch:main` — main branch snapshots + tag snapshots
- `branch:<name>` — one row per other branch

To estimate possible orphan file size for recognized data files:

```sql
SELECT p.data_file_size - r.data_file_size AS orphan_data_size
FROM paimon.default.my_table$physical_files_size p,
     paimon.default.my_table$referenced_files_size r
WHERE r.source = 'total';
```

### Branch References

Read a table branch with Java-compatible `$branch_<name>` syntax:

```sql
SELECT * FROM paimon.default.my_table$branch_b1;
```

System tables support the same branch syntax:

```sql
SELECT * FROM paimon.default.my_table$branch_b1$options;
SELECT * FROM paimon.default.my_table$branch_b1$snapshots;
```

Branch references are read-only in DataFusion. `INSERT`, `UPDATE`, `DELETE`,
`MERGE INTO`, `TRUNCATE TABLE`, and `ALTER TABLE` against a branch reference are
rejected.

## Table Options

Set via `WITH ('key' = 'value')` at table creation time, or dynamically via `SET`.

### Bucket Configuration

| Option | Description |
|---|---|
| `'bucket' = 'N'` | Fixed N buckets (e.g. 1, 2, 4) |
| `'bucket' = '-1'` | Dynamic bucket mode (HASH index) |
| `'bucket' = '-2'` | Postpone bucket mode (deferred assignment) |
| `'bucket-key' = 'col'` | Explicit bucket key column |
| `'bucket-function.type' = 'default' \| 'mod' \| 'hive'` | Function used to map fixed bucket keys to bucket ids |

### Merge Engine

| Option | Description |
|---|---|
| `'merge-engine' = 'deduplicate'` | Deduplicate engine (default for PK tables), last write wins |
| `'merge-engine' = 'first-row'` | Keeps the first written row |
| `'merge-engine' = 'partial-update'` | Basic partial-update engine for PK tables |
| `'merge-engine' = 'aggregation'` | Basic aggregation engine for PK tables |

Rust currently supports `merge-engine=aggregation` in basic mode only. It works
with fixed buckets and ordinary dynamic buckets (`'bucket' = '-1'`) when the
primary key includes all partition columns. It supports per-field aggregate
functions such as `sum`, `min`, `max`, value functions, boolean functions, and
`listagg`, plus `fields.default-aggregate-function`.

Sequence fields are always merged with `last_value`. Defining
`fields.<sequence-field>.aggregate-function` is rejected, matching Java schema
validation.

This is not full Java feature parity. Aggregation tables do not support retract
rows (`DELETE` / `UPDATE_BEFORE`), deletion vectors, cross-partition dynamic
bucket writes, or advanced aggregation options such as `ignore-retract`,
`distinct`, `nested-key`, `count-limit`, and sequence groups.

### Global Index Options

Set these options when building global indexes with
`CALL sys.create_global_index`. The current DataFusion builders require
row-tracking and data evolution, and reject primary-key tables and tables with
deletion vectors enabled.

| Option | Default | Description |
|---|---:|---|
| `row-tracking.enabled` | `false` | Enables stable row ids required by global index files. |
| `data-evolution.enabled` | `false` | Enables row-id-aware table evolution and partial-column writes. |
| `global-index.enabled` | `false` | Enables global index metadata and global-index-aware reads. |
| `global-index.row-count-per-shard` | `100000` | Maximum row count per vector global-index shard. |
| `sorted-index.records-per-range` | `100000` | Maximum row count per BTree range. |
| `btree-index.fallback-scan-max-size` | `256mb` | Maximum total size of selected BTree global-index files for fallback scans used by range/between and suffix/contains/complex LIKE predicates; `0` disables BTree fallback index scans. |
| `bitmap-index.fallback-scan-max-size` | `256mb` | Maximum total size of selected bitmap global-index files for fallback scans used by range/between and suffix/contains/complex LIKE predicates; `0` disables bitmap fallback index scans. |
| `global-index.search-mode` | `fast` | Global index coverage mode for reads: `fast`, `full`, or `detail`. |

### Variant Shredding Options

Set these as table options when writing `VARIANT` columns to Parquet. The
logical table schema remains `VARIANT`; the options only affect the physical
file layout and automatic read-time assembly.

| Option | Default | Description |
|---|---:|---|
| `variant.shreddingSchema` | unset | Configured shredding schema as a Paimon `ROW` type JSON string. Top-level field names match `VARIANT` column names, and their nested types describe the typed fields to extract. |
| `parquet.variant.shreddingSchema` | unset | Parquet-scoped alias for `variant.shreddingSchema`. |
| `variant.inferShreddingSchema` | `false` | Enables per-writer schema inference for `VARIANT` columns when no configured shredding schema is set. |
| `parquet.variant.inferShreddingSchema` | `false` | Parquet-scoped alias for `variant.inferShreddingSchema`. |
| `variant.shredding.maxInferBufferRow` | `4096` | Number of initial rows buffered per data-file writer before inferring the shredding schema. If fewer rows are written, inference runs when the writer is flushed or closed. |
| `variant.shredding.maxSchemaDepth` | `50` | Maximum nested depth considered by inference. |
| `variant.shredding.maxSchemaWidth` | `300` | Maximum number of inferred typed fields across inferred Variant schemas. |
| `variant.shredding.minFieldCardinalityRatio` | `0.1` | Minimum ratio of sampled non-null Variant values that must contain a field before inference keeps it as a typed field. |

Configured shredding takes precedence over inferred shredding. If a table has no
`VARIANT` columns, or none of these options enable shredding, Paimon Rust writes
the normal physical format without wrapping the writer.

### Other Options

| Option | Description |
|---|---|
| `'sequence.field' = 'col'` | Sequence field used to determine which record wins during deduplication |
| `'row-tracking.enabled' = 'true'` | Enable stable row ids |
| `'data-evolution.enabled' = 'true'` | Enable data evolution (partial-column writes, row-level UPDATE/MERGE/DELETE) |
| `'global-index.enabled' = 'true'` | Enable global index metadata and reads |
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
    ctx.register_catalog("paimon", catalog).await?;

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
