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

For DataFusion queries, use the native `SessionContext` and register a `PaimonCatalog`:

```python
from datafusion import SessionContext
from pypaimon_rust.datafusion import PaimonCatalog

catalog = PaimonCatalog({"warehouse": "/path/to/warehouse"})
ctx = SessionContext()
ctx.register_catalog_provider("paimon", catalog)

df = ctx.sql("SELECT * FROM paimon.default.my_table")
df.show()
```

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
```````
