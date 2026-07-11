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

`SQLContext` can read, execute, create, and drop persistent views and can create SQL scalar functions in a Paimon REST Catalog:

```sql
CREATE VIEW [IF NOT EXISTS] view_name [(column_name, ...)] AS query;
DROP VIEW [IF EXISTS] view_name;

CREATE FUNCTION [IF NOT EXISTS] function_name([parameter_name data_type, ...])
RETURNS data_type
[LANGUAGE SQL]
RETURN scalar_expression;
```

- A persistent view is resolved lazily like a table. The `datafusion` dialect is preferred and the
  default view query is used when that dialect is absent. Unqualified relations inside the view
  resolve in the view's owning catalog and database.
- `CREATE VIEW` infers stored field types and nullability from the defining query. An optional
  column list overrides names only and must match the query output. Unqualified relations and REST
  SQL functions are planned in the new view's owning catalog and database. `IF NOT EXISTS` is
  handled atomically by the catalog.
- `DROP VIEW` accepts bare, two-part, and three-part names and sends one direct REST delete request.
  `IF EXISTS` ignores only a missing view. Multiple targets and `CASCADE`, `RESTRICT`, `PURGE`, or
  other drop modifiers are not supported. Catalogs without persistent view support may return
  `Unsupported`.
- A SQL function can be called as `function(args...)` in the current catalog/database or as
  `catalog.database.function(args...)`. Its `definitions.datafusion` value must be a scalar SQL
  expression, it must be deterministic, and it must declare its input parameters and exactly one
  return parameter.
- `CREATE FUNCTION` requires named parameters, one return type, and a scalar `RETURN` expression.
  `LANGUAGE SQL` is optional and SQL is the default, matching Databricks syntax. Determinism is
  inferred and validated from the planned expression before sending the REST create request. Bare,
  two-part, and three-part creation targets are supported; calls remain limited to bare and
  three-part names.
- `CREATE OR REPLACE VIEW`, materialized/secure views, comments/options, persistent `ALTER VIEW`,
  `CREATE OR REPLACE/ALTER/TEMPORARY FUNCTION`, and persistent `ALTER FUNCTION` / `DROP FUNCTION`
  are not supported. Lambda/file, aggregate/table/multi-return, non-deterministic, Stable/Volatile,
  and non-SQL functions are also not supported.

Use `SQLContext::sql` for function expansion:

```rust,ignore
let mut ctx = paimon_datafusion::SQLContext::new();
ctx.register_catalog("paimon", rest_catalog).await?;

ctx.sql("CREATE VIEW daily_scores AS SELECT normalize_score(score) AS score FROM scores").await?;
ctx.sql("CREATE FUNCTION plus_one(x BIGINT) RETURNS BIGINT RETURN x + 1").await?;
let view = ctx.sql("SELECT * FROM analytics_view").await?;
let function = ctx.sql("SELECT plus_one(score) FROM scores").await?;
```

See the [documentation](https://paimon.apache.org/docs/rust/datafusion/) for getting started guide and more details.
