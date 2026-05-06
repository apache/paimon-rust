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

Time travel queries are not supported in the Python binding at this time.
