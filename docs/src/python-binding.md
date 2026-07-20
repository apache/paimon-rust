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

# Python Integration

The Python integration is a binding built on top of Apache Paimon Rust, allowing you to access Paimon tables from Python programs. It uses [PyArrow](https://arrow.apache.org/docs/python/) for zero-copy data transfer via the [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html).

## Prerequisites

- Python 3.10 or later
- Supported platforms: Linux (amd64, arm64), macOS (amd64, arm64), Windows (amd64)

## Installation

```bash
pip install pypaimon-rust pyarrow
```

The pre-built native library is embedded in the package and automatically loaded at runtime — no manual build step is needed. [PyArrow](https://arrow.apache.org/docs/python/) is a required peer dependency and must be installed separately.

## Creating a Catalog

Use `PaimonCatalog` with a dictionary of options to create a catalog. The catalog type is determined by the `metastore` option (default: `filesystem`).

```python
from pypaimon_rust.datafusion import PaimonCatalog

# Local filesystem
catalog = PaimonCatalog({"warehouse": "/path/to/warehouse"})

# List databases and tables
print(catalog.list_databases())
print(catalog.list_tables("default"))

# Get a table handle
table = catalog.get_table("default.my_table")
```

### Alibaba Cloud OSS

```python
catalog = PaimonCatalog({
    "warehouse": "oss://bucket/warehouse",
    "fs.oss.accessKeyId": "your-access-key-id",
    "fs.oss.accessKeySecret": "your-access-key-secret",
    "fs.oss.endpoint": "oss-cn-hangzhou.aliyuncs.com",
})
```

### REST Catalog

```python
catalog = PaimonCatalog({
    "metastore": "rest",
    "uri": "http://localhost:8080",
    "warehouse": "my_warehouse",
})
```

## SQL Context

`SQLContext` supports registering multiple Paimon catalogs and executing SQL queries with DataFusion.

```python
from pypaimon_rust.datafusion import SQLContext

ctx = SQLContext()
ctx.register_catalog("paimon", {"warehouse": "/path/to/warehouse"})

# DDL and DML
ctx.sql("CREATE SCHEMA paimon.my_db")
ctx.sql("CREATE TABLE paimon.my_db.t (id INT, name STRING)")
ctx.sql("INSERT INTO paimon.my_db.t VALUES (1, 'alice'), (2, 'bob')")

# Query returns a list of PyArrow RecordBatches
batches = ctx.sql("SELECT * FROM paimon.my_db.t")
for batch in batches:
    print(batch)
```

## Reading a Table

Paimon Python uses a **scan-then-read** pattern: first scan the table to produce splits, then read data from those splits as PyArrow RecordBatches.

```python
import pyarrow as pa
from pypaimon_rust.datafusion import PaimonCatalog

catalog = PaimonCatalog({"warehouse": "/path/to/warehouse"})
table = catalog.get_table("default.my_table")

# Create a read builder
rb = table.new_read_builder()

# Step 1: Scan — produces a Plan containing Splits
scan = rb.new_scan()
plan = scan.plan()
splits = plan.splits()

# Step 2: Read — consumes splits and returns PyArrow RecordBatches
read = rb.new_read()
batches = read.read(splits)

for batch in batches:
    print(batch)
```

Alternatively, read via SQL using `SQLContext`:

```python
from pypaimon_rust.datafusion import SQLContext

ctx = SQLContext()
ctx.register_catalog("paimon", {"warehouse": "/path/to/warehouse"})

batches = ctx.sql("SELECT id, name FROM paimon.default.my_table")
for batch in batches:
    print(batch)
```

## Writing to a Table

Paimon Python uses a **write-then-commit** pattern: write PyArrow RecordBatches to a writer, prepare commit messages, then commit.

```python
import pyarrow as pa
from pypaimon_rust.datafusion import PaimonCatalog

catalog = PaimonCatalog({"warehouse": "/path/to/warehouse"})
table = catalog.get_table("default.my_table")

# Build a batch matching the table schema
batch = pa.record_batch(
    [pa.array([1, 2, 3], pa.int32()), pa.array(["a", "b", "c"], pa.string())],
    names=["id", "name"],
)

# Create a write builder (shared commit_user for writer and committer)
wb = table.new_write_builder()

# Write batches
write = wb.new_write()
write.write_arrow(batch)

# Prepare commit messages
messages = write.prepare_commit()

# Commit
wb.new_commit().commit(messages)
```

Alternatively, write via SQL using `SQLContext`:

```python
from pypaimon_rust.datafusion import SQLContext

ctx = SQLContext()
ctx.register_catalog("paimon", {"warehouse": "/path/to/warehouse"})

ctx.sql("INSERT INTO paimon.default.my_table VALUES (1, 'alice'), (2, 'bob')")
```

!!! warning "Schema Validation"
    The input batch schema is strictly validated against the table schema: field count, order, names, and types must match exactly. A `ValueError` is raised on mismatch.

!!! note "Write Builder Consistency"
    The writer and committer must come from the same `WriteBuilder` — they share a `commit_user` for duplicate-commit detection. Passing messages from one builder's writer to another builder's committer will raise a `ValueError`.

## Column Projection

Use `with_projection` to select specific columns. Only the requested columns are read, reducing I/O.

```python
rb = table.new_read_builder()
rb.with_projection(["id", "name"])

# Continue with scan-then-read as above...
```

## Limit

Use `with_limit` to set a hint for the number of rows returned. A limit of `0` returns zero rows.

```python
rb = table.new_read_builder()
rb.with_limit(100)
```

!!! warning
    `with_limit` is a scan-planning hint, not an exact row cap. When all rows fall within a single split, the entire split is returned regardless of the limit value. Callers should apply application-level limiting if an exact upper bound is required.

## Case Sensitivity

Use `with_case_sensitive` to control whether column-name matching in projections and predicates is case-sensitive. Defaults to `True` (exact match). Set to `False` for case-insensitive matching (ASCII case-folding).

```python
rb = table.new_read_builder()
rb.with_case_sensitive(False)
```

!!! note
    `with_case_sensitive` must be called **before** `with_filter` to affect predicate construction. The predicate is built using the case-sensitivity setting at the time `with_filter` is invoked; changing it afterward has no effect on an already-constructed predicate.

## Filter Push-Down

Filter push-down prunes data at two levels:

1. **Scan planning** — skips partitions, buckets, and data files based on file-level statistics (min/max).
2. **Read-side** — applies exact residual filtering, ensuring only rows that match the predicate are returned.

!!! note
    Filter push-down at the scan-planning level is **best-effort**: it may conservatively include files that do not contain matching rows. The read-side applies exact residual filtering and will not return rows that fail the predicate.

### Predicate Format

Predicates use a lightweight dictionary format. Each leaf node specifies a `method`, `field`, and `literals`:

```python
# Comparison predicates
{"method": "equal", "field": "id", "literals": [1]}           # id = 1
{"method": "notEqual", "field": "name", "literals": ["bob"]}   # name != "bob"
{"method": "lessThan", "field": "id", "literals": [3]}         # id < 3
{"method": "lessOrEqual", "field": "id", "literals": [2]}      # id <= 2
{"method": "greaterThan", "field": "id", "literals": [1]}      # id > 1
{"method": "greaterOrEqual", "field": "id", "literals": [2]}   # id >= 2

# Null checks
{"method": "isNull", "field": "name"}                           # name IS NULL
{"method": "isNotNull", "field": "name"}                        # name IS NOT NULL

# IN / NOT IN
{"method": "in", "field": "id", "literals": [1, 2, 3]}          # id IN (1, 2, 3)
{"method": "notIn", "field": "name", "literals": ["x", "y"]}    # name NOT IN ("x", "y")

# String predicates
{"method": "startsWith", "field": "name", "literals": ["al"]}   # name LIKE 'al%'
{"method": "endsWith", "field": "name", "literals": ["ce"]}     # name LIKE '%ce'
{"method": "contains", "field": "name", "literals": ["ic"]}     # name LIKE '%ic%'
{"method": "like", "field": "name", "literals": ["a%b%c"]}      # name LIKE 'a%b%c'
```

### Applying Filters

Pass a predicate dict to `with_filter` on the `ReadBuilder`:

```python
rb = table.new_read_builder()
rb.with_filter({"method": "equal", "field": "id", "literals": [1]})

# Continue with scan-then-read...
```

### Compound Predicates

Combine predicates with `"and"` / `"or"` methods using a `"children"` list:

```python
# id >= 1 AND id <= 3
rb.with_filter({
    "method": "and",
    "children": [
        {"method": "greaterOrEqual", "field": "id", "literals": [1]},
        {"method": "lessOrEqual", "field": "id", "literals": [3]},
    ]
})

# (id = 1 OR id = 2) AND name = "alice"
rb.with_filter({
    "method": "and",
    "children": [
        {
            "method": "or",
            "children": [
                {"method": "equal", "field": "id", "literals": [1]},
                {"method": "equal", "field": "id", "literals": [2]},
            ]
        },
        {"method": "equal", "field": "name", "literals": ["alice"]},
    ]
})
```

### Supported Literal Types

Literal values are automatically converted from Python types based on the column's declared schema type:

| Python Type                    | Paimon Type          |
|--------------------------------|----------------------|
| `bool`                         | Bool                 |
| `int`                          | TinyInt / SmallInt / Int / BigInt |
| `int` / `float`                | Float / Double       |
| `str`                          | String               |
| `datetime.date`                | Date                 |
| `datetime.time` (naive)        | Time                 |
| `datetime.datetime` (naive)    | Timestamp            |
| `datetime.datetime` (aware)    | LocalZonedTimestamp  |
| `decimal.Decimal` / `int`      | Decimal              |

For temporal types, ensure the Python object matches the column type exactly:

```python
import datetime
from decimal import Decimal

# Date
{"method": "equal", "field": "dt", "literals": [datetime.date(2024, 1, 1)]}

# Decimal at scale 2 — int or Decimal accepted
{"method": "equal", "field": "amount", "literals": [Decimal("123.45")]}

# Naive datetime for TIMESTAMP (no timezone)
{"method": "equal", "field": "ts", "literals": [datetime.datetime(2024, 1, 1, 12, 0, 0)]}

# Aware datetime for TIMESTAMP WITH LOCAL TIME ZONE
from zoneinfo import ZoneInfo
{"method": "equal", "field": "ts_ltz", "literals": [
    datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
]}
```

## Time Travel

Use scan options on `new_read_builder` to read a table at a specific point in time:

```python
# By snapshot ID
rb = table.new_read_builder({"scan.snapshot-id": "1"})

# By timestamp (epoch millis)
rb = table.new_read_builder({"scan.timestamp-millis": "1700000000000"})

# By version
rb = table.new_read_builder({"scan.version": "3"})

# By tag name
rb = table.new_read_builder({"scan.tag-name": "release-1.0"})
```

!!! warning
    Only one time-travel selector may be set. Providing multiple selectors will raise a `ValueError`.

## Table Inspection

Inspect snapshots, tags, and partition statistics on a table:

```python
# Latest snapshot
snap = table.latest_snapshot()
if snap:
    print(f"Snapshot {snap.id()} at {snap.commit_time_ms()}")
    print(f"  commit kind: {snap.commit_kind()}")
    print(f"  total records: {snap.total_record_count()}")

# All snapshots (newest first)
for snap in table.list_snapshots():
    print(snap.id(), snap.commit_kind())

# Tags
for tag in table.list_tags():
    print(tag.name(), tag.snapshot_id())

# Partition stats
for stat in table.partition_stats():
    print(stat.partition(), stat.record_count(), stat.total_size_bytes())
```

## Python UDF

Register Python scalar UDFs into a `SQLContext`:

```python
from pypaimon_rust.datafusion import SQLContext, udf
import pyarrow as pa

ctx = SQLContext()
ctx.register_catalog("paimon", {"warehouse": "/tmp/paimon-warehouse"})

batch = pa.record_batch([[1, None, 3]], names=["id"])
ctx.register_batch("my_temp", batch)

def plus_ten(values):
    # values is a PyArrow Array (not a tuple)
    return pa.array(
        [None if value is None else value + 10 for value in values.to_pylist()],
        type=pa.int64(),
    )

ctx.register_udf(udf(plus_ten, [pa.int64()], pa.int64(), "volatile", "plus_ten"))

batches = ctx.sql(
    "SELECT plus_ten(id) AS id FROM paimon.default.my_temp ORDER BY id"
)
for batch in batches:
    print(batch)
```

## Complete Example

```python
import pyarrow as pa
from pypaimon_rust.datafusion import PaimonCatalog, SQLContext

# 1. Write data via SQL
ctx = SQLContext()
ctx.register_catalog("paimon", {"warehouse": "/tmp/paimon-warehouse"})
ctx.sql("CREATE SCHEMA paimon.wdb")
ctx.sql("CREATE TABLE paimon.wdb.t (id INT, name STRING)")

# 2. Write using the programmatic API
catalog = PaimonCatalog({"warehouse": "/tmp/paimon-warehouse"})
table = catalog.get_table("wdb.t")

batch = pa.record_batch(
    [pa.array([1, 2, 3], pa.int32()), pa.array(["alice", "bob", "carol"], pa.string())],
    names=["id", "name"],
)
wb = table.new_write_builder()
write = wb.new_write()
write.write_arrow(batch)
wb.new_commit().commit(write.prepare_commit())

# 3. Read with projection and filter
rb = table.new_read_builder()
rb.with_projection(["id", "name"])
rb.with_filter({"method": "greaterThan", "field": "id", "literals": [0]})

scan = rb.new_scan()
plan = scan.plan()
splits = plan.splits()

read = rb.new_read()
for batch in read.read(splits):
    tbl = pa.Table.from_batches([batch]).sort_by("id")
    print(tbl)

# 4. Read via SQL
for batch in ctx.sql("SELECT id, name FROM paimon.wdb.t ORDER BY id"):
    print(batch)
```
