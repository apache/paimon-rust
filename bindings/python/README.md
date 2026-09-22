<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# PyPaimon Rust

This project builds the Rust-powered core for [PyPaimon](https://paimon.apache.org/docs/master/pypaimon/overview/) while also providing DataFusion integration for querying Paimon tables.

## Usage

```python
import pyarrow as pa
from pypaimon_rust.datafusion import SQLContext

# Create a SQL context and register a Paimon catalog
ctx = SQLContext()
ctx.register_catalog("paimon", {"warehouse": "/tmp/paimon-warehouse"})

# Create a table and insert data
ctx.sql("CREATE SCHEMA paimon.my_db")
ctx.sql("CREATE TABLE paimon.my_db.users (id INT, name STRING, PRIMARY KEY (id))")
ctx.sql("INSERT INTO paimon.my_db.users VALUES (1, 'alice'), (2, 'bob')")

# Query data
batches = ctx.sql("SELECT id, name FROM paimon.my_db.users ORDER BY id")

# Inspect BLOB media or build thumbnails when installed with pypaimon-rust[video]
batches = ctx.sql(
    "SELECT id, media_info(content), media_thumbnail(content, 160, 90) "
    "FROM paimon.my_db.assets"
)

# Register a temporary table from a PyArrow RecordBatch
batch = pa.record_batch([[1, 2], ["alice", "bob"]], names=["id", "name"])
ctx.register_batch("paimon.default.my_temp", batch)
batches = ctx.sql("SELECT * FROM paimon.default.my_temp")

# Drop it via SQL when no longer needed
ctx.sql("DROP TEMPORARY TABLE paimon.default.my_temp")
```

For the full SQL reference, see the [SQL Integration docs](https://paimon.apache.org/docs/master/sql/).

### Native Read / Write

Beyond SQL, you can use the lower-level read and write APIs directly from Python.
Time travel is supported via the `options` dict on `new_read_builder`.

```python
import pyarrow as pa
from pypaimon_rust.datafusion import SQLContext, PaimonCatalog

WAREHOUSE = "/tmp/paimon-warehouse"

# --- DDL/DML via DataFusion SQLContext ---
ctx = SQLContext()
ctx.register_catalog("paimon", {"warehouse": WAREHOUSE})
ctx.sql("CREATE SCHEMA paimon.my_db")
ctx.sql("CREATE TABLE paimon.my_db.users (id INT, name STRING, PRIMARY KEY (id))")
ctx.sql("INSERT INTO paimon.my_db.users VALUES (1, 'alice'), (2, 'bob')")
catalog = PaimonCatalog({"warehouse": WAREHOUSE})
table = catalog.get_table("my_db.users")

# --- Read data ---
read_builder = table.new_read_builder().with_projection(["id", "name"]).with_limit(100)
scan = read_builder.new_scan()
plan = scan.plan()
batches = read_builder.new_read().read(plan.splits())

print(f"\nRead: {batches[0].num_rows} rows")
print(batches[0])

# --- Write data, from a PyArrow RecordBatch ---
batch = pa.record_batch(
    [[3, 4], ["charlie", "diana"]],
    schema=pa.schema([("id", pa.int32()), ("name", pa.utf8())]),
)
write_builder = table.new_write_builder()
writer = write_builder.new_write()
writer.write_arrow(batch)
commit_messages = writer.prepare_commit()
write_builder.new_commit().commit(commit_messages)

# --- Time travel: read a past version ---
# Supported options: scan.version, scan.timestamp-millis, scan.snapshot-id, or scan.tag-name
read_builder_tt = table.new_read_builder({"scan.snapshot-id": "1"})
scan_tt = read_builder_tt.new_scan()
plan_tt = scan_tt.plan()
batches_tt = read_builder_tt.new_read().read(plan_tt.splits())

print(f"\nRead: {batches_tt[0].num_rows} rows")
print(batches_tt[0])
```

### Native commit from serialized messages

Use `table.new_commit(commit_user)` to commit messages prepared by an external
Java/PyPaimon writer. Keep `commit_user` stable across retries of the same job.
The Java v14 body has no version header, table identity, or overwrite mode;
the caller must retain that context alongside the bytes.

```python
committer = table.new_commit("ingest-job")
messages = [
    committer.deserialize_commit_message(body, source_table_location, version=14)
    for body in serialized_messages
]
committer.commit(messages, commit_identifier=42)

# After an uncertain result, retry the same identifier and complete message set.
table.new_commit("ingest-job").filter_and_commit(messages, 42)
```

Identifiers must increase monotonically for each commit user. Omitting an
identifier uses batch commit semantics. `abort(messages)` deletes newly written
files; only use it for messages known not to have committed.

`overwrite(messages, static_partitions=None, *, commit_identifier=None)` supports
dynamic overwrite (`None`), whole-table overwrite (`{}`), and static/partial
partition specs. Specs use Python values matching the partition schema, for
example `{"region": "EU", "year": 2026}`; `None` or the configured default partition
name denotes null. `truncate_partitions(specs, commit_identifier=None)` and
`truncate_table(commit_identifier=None)` expose the Rust truncate operations.
Overwrite and truncate filter repeated identifiers when an explicit identifier
is supplied, preserving data written after the original operation.

For overwrite, prepare native writers with
`table.new_write_builder(commit_user="ingest-job", overwrite=True)`. When importing
fixed-bucket messages carrying `totalBuckets`, pass `overwrite=True` to restore
operation context absent from Java's wire format. Submit these through `overwrite`.
Compact increments remain unsupported by the Rust committer and are rejected.

### Tables resolved outside the Rust catalog

`Table.from_resolved_schema(location, schema_json, *, database="default",
table="table", branch="main", options=None)` accepts a Java-format TableSchema
JSON document. It preserves the supplied fields, field IDs and complete table
options, including removed options, without reloading a catalog schema.
`options` configures FileIO; `branch` selects the snapshot/schema/tag namespace.

Use `new_read_builder()` without extra options to keep that resolved schema.
Snapshot selectors in the schema's options still select the requested snapshot.
Passing options to `new_read_builder(options)` instead uses the normal schema
and snapshot time-travel resolution.

For names containing dots, use `catalog.get_table(("namespace.database", "table.with.dots"))`
to preserve the database and table components. Both string and tuple identifiers
support `$branch_<name>` on the table component and reject system-table suffixes.

For REST tables, first use `PaimonCatalog.get_table()`, then
`table.copy_with_resolved_schema(schema_json, branch=None)`. This replaces the
complete fields/options while retaining the table location, identity, FileIO
provider and REST environment. The optional branch selects its metadata namespace
without reading a branch schema file. Omit it to retain the original branch.
Cached time-travel resolution is discarded so the supplied options select the
snapshot, with the externally resolved fields preserved.

REST tables load the latest snapshot through the catalog, including empty
results and branch-scoped requests. Permission and service failures (including
HTTP 501) are propagated as in Java, and the
FileIO provider continues to refresh catalog credentials after schema replacement.

## Setup

Install [uv](https://docs.astral.sh/uv/getting-started/installation/):

```shell
pip install uv
```

Set up the development environment:

```shell
make install
```

## Build

```shell
make build
```

## Test

Python integration tests expect the shared Paimon test warehouse to be prepared
first from the repository root:

```shell
make docker-up
cd bindings/python
```

```shell
make test
```
