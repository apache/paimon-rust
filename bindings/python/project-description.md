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

Install via PyPI:

```
pip install pypaimon-rust
```

If you want to use the native Python DataFusion `SessionContext`, install `datafusion` as well.

## Query Paimon Tables with DataFusion

The recommended way to query Paimon tables is through `SQLContext`, which supports
multi-catalog registration, DDL, DML, and all Paimon-specific SQL extensions:

```python
from pypaimon_rust.datafusion import SQLContext

ctx = SQLContext()
ctx.register_catalog("paimon", {
    "warehouse": "/path/to/warehouse",
})

# DDL
ctx.sql("CREATE SCHEMA paimon.my_db")
ctx.sql("CREATE TABLE paimon.my_db.users (id INT, name STRING, PRIMARY KEY (id))")

# DML
ctx.sql("INSERT INTO paimon.my_db.users VALUES (1, 'alice'), (2, 'bob')")

# Query tables via SQL (catalog.database.table)
batches = ctx.sql("SELECT * FROM paimon.my_db.users")
```

### Blob Source Streams and Commit Identity

The write and update APIs accept a `UriReaderFactory` for custom Blob reference
sources. Both stream protocols are public runtime imports and support structural
typing; implementations do not need to inherit from them.

```python
from io import BytesIO
from pypaimon_rust.datafusion import UriReader, UriReaderFactory

payloads = {"custom://example": b"content"}

class Reader:
    def new_input_stream(self, uri: str):
        return BytesIO(payloads[uri])

class Factory:
    def create(self, uri: str) -> UriReader:
        return Reader()

factory: UriReaderFactory = Factory()
builder = table.new_batch_write_builder().with_commit_user("my-job")
writer = builder.new_write().with_blob_uri_reader_factory(factory)
update = builder.new_update().with_blob_uri_reader_factory(factory)
```

Rust copies the requested Blob ranges and closes the source streams. Pass `None`
to clear the factory. Configure a writer before its first write. Batch and stream
updates, including incremental row-ID updaters, expose the same chainable setter.
`with_commit_user` is available on batch, stream, and postpone fixed bucket write
builders, and the identity applies to both writers and committers from that builder.

### Typed MERGE Actions

Batch and stream `merge_into` accept ordered `WhenMatched` and `WhenNotMatched`
objects. Assignments use `MergeAssignment.source_column`, `target_column`,
`literal`, or `function`. A function receives the selected clause's Arrow table
and returns an Array or ChunkedArray with the same number of rows.

Execution engines that already joined target and source data can reuse these
types through the batch execution factories:

```python
import pyarrow as pa
from pypaimon_rust.datafusion import MergeAssignment, MergeBatchExecution, WhenMatched

joined = pa.table({
    "t._ROW_ID": [10, 11],
    "t.value": pa.array([20, 30], type=pa.int32()),
    "s.value": pa.array([40, 10], type=pa.int32()),
})
matched = [
    WhenMatched.update(
        {"value": MergeAssignment.source_column("value")},
        condition='"s.value" > "t.value"',
    ),
    WhenMatched.delete(),
]
update_schema = pa.schema([
    pa.field("_ROW_ID", pa.int64(), nullable=False),
    pa.field("value", pa.int32()),
])
updates = MergeBatchExecution.for_updates(joined.schema, update_schema, matched)
deletes = MergeBatchExecution.for_deletes(joined.schema, matched)
assert updates.execute(joined).to_pylist() == [{"_ROW_ID": 10, "value": 40}]
assert deletes.execute(joined).to_pylist() == [{"_ROW_ID": 11}]
```

Both matched factories consume the complete ordered branch list. The delete
factory skips UPDATE assignments and callbacks while preserving their conditions.
For unmatched source batches, use `for_inserts(source_schema, target_schema,
when_not_matched)` with `WhenNotMatched` objects. The factories compile conditions
once and return an executor with `execute(Table | RecordBatch) -> Table`.
The calling engine owns joins, authorization and table writes.

### Hybrid Search Phases

Hybrid queries can execute routes separately and then fuse their results:

```python
hybrid = (table.new_hybrid_search_builder()
          .add_vector_route("embedding", [1.0, 0.0], 20)
          .with_limit(10))
routes = hybrid.route_builders()
results = [hybrid.to_route_result(route, route.search_builder().execute())
           for route in routes]
ranked = hybrid.rank(results)
```

`route_builders()` checks read authorization and captures one snapshot for all
routes, including an empty view. Each route retains its search builder: repeated
`search_builder()` calls return the same object, and configuring it changes later
`route.execute()` calls. Returned builders retain their snapshot after later
commits. `to_route_result()` accepts scored
global results and normalizes empty unscored selections; nonempty unscored or
physical primary-key selections cannot participate in global row-ID fusion.

For externally evaluated scores, `HybridSearchWeightedResult` and
`HybridSearchRanker.RRF`, `.WEIGHTED_SCORE`, or `.MRR` provide the same fusion
without a table or query:

```python
from pypaimon_rust.datafusion import (
    GlobalIndexResult, HybridSearchRanker, HybridSearchWeightedResult,
)

hits = GlobalIndexResult.from_scored_row_ids({7: 0.9, 8: 0.5})
ranked = HybridSearchRanker.MRR.rank([HybridSearchWeightedResult(hits, 2.0)], 1)
assert ranked.row_ids() == {7: 2.0}
```

Rankers use Java's float32 scores, weights and arithmetic. A shared captured read
context is retained only when every nonempty input belongs to the same table
view. Fusing external scores or different views returns scores without attaching
an unrelated table or snapshot.

### Multimodal SQL Helpers

`SQLContext` also registers Python scalar UDFs for common BLOB media and vector
workflows. Install the optional media dependencies when you need image or video
decoding:

```shell
pip install "pypaimon-rust[video]"
```

```python
batches = ctx.sql("""
    SELECT
        id,
        media_info(content) AS info_json,
        media_thumbnail(content, 160, 90) AS preview_png,
        video_snapshot(content, 1000) AS frame_png
    FROM paimon.my_db.assets
""")
```

For vector search, `vector_from_json` converts JSON-encoded embeddings into
Arrow `List<Float32>` values that can be used from a temporary table or subquery:

```python
batches = ctx.sql("""
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
    ) AS r
""")
```

### Temporary Tables

You can register temporary in-memory tables programmatically. Names support the same resolution rules as SQL: bare names use the current catalog and database, partially qualified names use the current catalog, and fully qualified names specify catalog.database.table.

Register a single PyArrow RecordBatch as a temporary table:

```python
import pyarrow as pa

batch = pa.record_batch([[1, 2], ["alice", "bob"]], names=["id", "name"])

ctx.register_batch("paimon.default.my_temp", batch)

batches = ctx.sql("SELECT * FROM paimon.default.my_temp")

# Drop it via SQL when no longer needed
ctx.sql("DROP TEMPORARY TABLE paimon.default.my_temp")
```

Alternatively, if you want to use the native Python DataFusion `SessionContext`,
install `datafusion` and register a `PaimonCatalog`:

```python
from datafusion import SessionContext
from pypaimon_rust.datafusion import PaimonCatalog

catalog = PaimonCatalog({
    "warehouse": "/path/to/warehouse",
})

ctx = SessionContext()
ctx.register_catalog_provider("paimon", catalog)

# Query tables via SQL (catalog.database.table)
df = ctx.sql("SELECT * FROM paimon.default.my_table LIMIT 10")
df.show()
```

### REST Catalog

```python
from datafusion import SessionContext
from pypaimon_rust.datafusion import PaimonCatalog

catalog = PaimonCatalog({
    "metastore": "rest",
    "uri": "http://localhost:8080",
    "warehouse": "my_warehouse",
})

ctx = SessionContext()
ctx.register_catalog_provider("paimon", catalog)
```

REST catalogs also support branch management through Rust core:

```python
identifier = ("db", "table")
catalog.create_branch(identifier, "empty-branch")
catalog.create_branch(identifier, "release-branch", from_tag="release-tag")
branches = catalog.list_branches(identifier)
release = catalog.get_table(("db", "table$branch_release-branch"))
catalog.fast_forward(identifier, "release-branch")
catalog.drop_branch(identifier, "empty-branch")
```

The server validates branch names and publishes metadata changes. Fast-forward
updates the identifier's current branch (main by default); it does not change
the source branch. These catalog methods currently require a REST catalog.
Other catalog implementations raise `NotImplementedError`.

Time travel queries are not supported in the Python binding at this time.
