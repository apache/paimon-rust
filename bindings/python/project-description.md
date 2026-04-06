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

`pypaimon-rust` provides a `PaimonCatalog` that can be registered into the native DataFusion `SessionContext`.
This keeps the standard DataFusion Python API available for regular queries.

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
