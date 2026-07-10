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

# Apache Paimon DataFusion Integration

[![crates.io](https://img.shields.io/crates/v/paimon-datafusion.svg)](https://crates.io/crates/paimon-datafusion)
[![docs.rs](https://img.shields.io/docsrs/paimon-datafusion.svg)](https://docs.rs/paimon-datafusion/latest/paimon_datafusion/)

This crate contains the integration of [Apache DataFusion](https://datafusion.apache.org/) and [Apache Paimon](https://paimon.apache.org/).

## REST Catalog views and SQL functions

`SQLContext` can read persistent views and SQL scalar functions that already exist in a Paimon
REST Catalog. No view or function DDL is added.

- A persistent view is resolved lazily like a table. The `datafusion` dialect is preferred and the
  default view query is used when that dialect is absent. Unqualified relations inside the view
  resolve in the view's owning catalog and database.
- A SQL function can be called as `function(args...)` in the current catalog/database or as
  `catalog.database.function(args...)`. Its `definitions.datafusion` value must be a scalar SQL
  expression, it must be deterministic, and it must declare its input parameters and exactly one
  return parameter.
- Only reads and execution are supported. Lambda/file functions, named arguments, multiple return
  values, non-deterministic functions, and calls made directly through a raw DataFusion
  `SessionContext` are not supported.

Use `SQLContext::sql` for function expansion:

```rust,ignore
let mut ctx = paimon_datafusion::SQLContext::new();
ctx.register_catalog("paimon", rest_catalog).await?;

let view = ctx.sql("SELECT * FROM analytics_view").await?;
let function = ctx.sql("SELECT normalize_score(score) FROM scores").await?;
```

See the [documentation](https://paimon.apache.org/docs/rust/datafusion/) for getting started guide and more details.
