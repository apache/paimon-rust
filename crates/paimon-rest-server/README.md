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

# paimon-rest-server

A Paimon REST catalog server backed by a real [`FileSystemCatalog`], for
**local end-to-end testing** of the REST catalog client. It is a dev/testing
tool and is **not published** to crates.io.

Unlike the in-memory mock used in `paimon`'s own unit tests, this server maps
the Paimon REST protocol onto a real `FileSystemCatalog`, so the client-side
`RESTCatalog` can be exercised against actual on-disk metadata:

- config + database/table metadata CRUD;
- append write + commit (the commit endpoint persists the posted snapshot via
  `SnapshotManager`) + read back;
- column-level `alter table`.

Because both the server and the client point at the **same** local warehouse,
the client writes data files directly while the server persists the snapshot
metadata it receives on the commit endpoint. The wire format mirrors Java
Paimon, so the same warehouse can be round-tripped with a Java reader/writer.

## Run the standalone server

```bash
REST_WAREHOUSE=/tmp/paimon-warehouse REST_HOST=127.0.0.1 REST_PORT=8080 \
  cargo run -p paimon-rest-server
```

Environment variables: `REST_WAREHOUSE` (default `/tmp/paimon-warehouse`),
`REST_HOST` (default `127.0.0.1`), `REST_PORT` (default `8080`),
`REST_PREFIX` (default empty).

## Use as a test fixture

```rust,no_run
# async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let server = paimon_rest_server::FsRestCatalogServer::start("/tmp/paimon-wh", "").await?;
// Point a RESTCatalog client at `server.url()`.
# Ok(()) }
```

See `tests/e2e.rs` for the full metadata / write-commit-read / alter-table
round trips.

## Endpoints

Served under the configured prefix (`/v1/...` by default):

| Method | Path | Operation |
| --- | --- | --- |
| GET | `/v1/config` | server config |
| GET / POST | `/databases` | list / create database |
| GET / POST / DELETE | `/databases/{db}` | get / alter (no-op) / drop database |
| GET / POST | `/databases/{db}/tables` | list / create table |
| GET / POST / DELETE | `/databases/{db}/tables/{table}` | get / alter / drop table |
| POST | `/tables/rename` | rename table |
| POST | `/databases/{db}/tables/{table}/commit` | commit a snapshot |
| GET | `/databases/{db}/tables/{table}/partitions` | list partitions |

The data-token endpoint returns `501`; it is never called when
`data-token.enabled=false` (the default).

[`FileSystemCatalog`]: https://docs.rs/paimon
