// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! A real Paimon REST catalog server backed by [`FileSystemCatalog`].
//!
//! Unlike the in-memory mock used by `paimon`'s own tests, this server maps the
//! Paimon REST protocol onto a real [`FileSystemCatalog`], so the client-side
//! [`RESTCatalog`](paimon::catalog::RESTCatalog) can be exercised end to end:
//! metadata CRUD, plus append write + commit + read back (the commit endpoint
//! persists the posted snapshot via [`SnapshotManager`]).
//!
//! Because both the server and the client point at the *same* local warehouse,
//! the client writes data files directly while the server only persists the
//! snapshot metadata it receives on the commit endpoint.
//!
//! # Example
//! ```no_run
//! # async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! let server = paimon_rest_server::FsRestCatalogServer::start("/tmp/paimon-wh", "").await?;
//! println!("listening on {}", server.url());
//! # Ok(()) }
//! ```

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    extract::{Extension, FromRequestParts, Json, MatchedPath, Query},
    http::request::Parts,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    serve, Router,
};
use serde::Deserialize;
use serde_json::json;

use paimon::api::{
    AlterDatabaseRequest, AlterTableRequest, AuditRESTResponse, ConfigResponse, CreateTableRequest,
    ErrorResponse, GetDatabaseResponse, GetTableResponse, ListDatabasesResponse,
    ListPartitionsResponse, ListTablesResponse, RESTUtil, RenameTableRequest, ResourcePaths,
};
use paimon::catalog::{list_partitions_from_file_system, Catalog, Identifier};
use paimon::common::{CatalogOptions, Options};
use paimon::spec::{Schema, Snapshot};
use paimon::table::SnapshotManager;
use paimon::{Error, FileSystemCatalog};

/// Convenience boxed error type for server construction (covers both
/// [`paimon::Error`] from catalog setup and [`std::io::Error`] from binding).
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Shared server state handed to every request handler.
struct AppState {
    catalog: FileSystemCatalog,
    config: ConfigResponse,
}

/// A running FileSystemCatalog-backed REST catalog server.
///
/// The server runs on a background Tokio task and is aborted on drop, so a
/// test can simply let it go out of scope when finished.
pub struct FsRestCatalogServer {
    addr: SocketAddr,
    handle: tokio::task::JoinHandle<()>,
}

impl FsRestCatalogServer {
    /// Start a server on an OS-assigned port (`127.0.0.1:0`).
    ///
    /// `warehouse` is the local warehouse root; `prefix` is the REST resource
    /// prefix (pass `""` for none, mirroring `/v1/...`).
    pub async fn start(warehouse: impl Into<String>, prefix: &str) -> Result<Self, BoxError> {
        Self::start_on(warehouse, prefix, "127.0.0.1", 0).await
    }

    /// Start a server bound to an explicit host and port.
    pub async fn start_on(
        warehouse: impl Into<String>,
        prefix: &str,
        host: &str,
        port: u16,
    ) -> Result<Self, BoxError> {
        let warehouse = warehouse.into();

        let mut options = Options::new();
        options.set(CatalogOptions::WAREHOUSE, warehouse.clone());
        let catalog = FileSystemCatalog::new(options)?;

        // Advertise the prefix (and warehouse) to clients via GET /v1/config.
        let mut defaults = HashMap::new();
        if !prefix.is_empty() {
            defaults.insert(CatalogOptions::PREFIX.to_string(), prefix.to_string());
        }
        defaults.insert(CatalogOptions::WAREHOUSE.to_string(), warehouse);
        let config = ConfigResponse::new(defaults);

        let state = Arc::new(AppState { catalog, config });
        let app = build_router(prefix, state);

        let listener = tokio::net::TcpListener::bind((host, port)).await?;
        let addr = listener.local_addr()?;
        let handle = tokio::spawn(async move {
            if let Err(e) = serve(listener, app.into_make_service()).await {
                eprintln!("paimon-rest-server error: {e}");
            }
        });

        Ok(Self { addr, handle })
    }

    /// The bound socket address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The base URL clients should connect to (e.g. `http://127.0.0.1:54321`).
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }
}

impl Drop for FsRestCatalogServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Build the axum router, wiring every endpoint under the given prefix.
fn build_router(prefix: &str, state: Arc<AppState>) -> Router {
    let paths = ResourcePaths::new(prefix);
    let base = paths.base_path();

    Router::new()
        // Config endpoint is always at the fixed /v1/config path.
        .route("/v1/config", get(get_config))
        .route(
            &format!("{base}/databases"),
            get(list_databases).post(create_database),
        )
        .route(
            &format!("{base}/databases/:db"),
            get(get_database).post(alter_database).delete(drop_database),
        )
        .route(
            &format!("{base}/databases/:db/tables"),
            get(list_tables).post(create_table),
        )
        .route(
            &format!("{base}/databases/:db/tables/:table"),
            get(get_table).post(alter_table).delete(drop_table),
        )
        .route(&format!("{base}/tables/rename"), post(rename_table))
        .route(
            &format!("{base}/databases/:db/tables/:table/commit"),
            post(commit),
        )
        .route(
            &format!("{base}/databases/:db/tables/:table/partitions"),
            get(list_partitions),
        )
        // Token endpoint is never hit when `data-token.enabled=false` (default),
        // but we serve a stub so a misconfigured client gets a clear 501.
        .route(
            &format!("{base}/databases/:db/tables/:table/token"),
            get(table_token_stub),
        )
        .layer(Extension(state))
}

// ============================================================================
// Error mapping: paimon::Error -> (HTTP status, ErrorResponse)
//
// The client reconstructs the original error solely from the HTTP status code
// (see `crates/paimon/src/api/rest_error.rs`), so the code below MUST line the
// status codes up with `RestError::from_error_response`.
// ============================================================================

fn error_response(e: Error) -> Response {
    let (status, resource_type, resource_name) = match &e {
        Error::DatabaseNotExist { database } => (
            StatusCode::NOT_FOUND,
            Some("database".to_string()),
            Some(database.clone()),
        ),
        Error::TableNotExist { full_name } => (
            StatusCode::NOT_FOUND,
            Some("table".to_string()),
            Some(full_name.clone()),
        ),
        Error::DatabaseAlreadyExist { database } => (
            StatusCode::CONFLICT,
            Some("database".to_string()),
            Some(database.clone()),
        ),
        Error::TableAlreadyExist { full_name } => (
            StatusCode::CONFLICT,
            Some("table".to_string()),
            Some(full_name.clone()),
        ),
        Error::DatabaseNotEmpty { database } => (
            StatusCode::CONFLICT,
            Some("database".to_string()),
            Some(database.clone()),
        ),
        Error::ColumnNotExist { full_name, column } => (
            StatusCode::BAD_REQUEST,
            Some(format!("column:{full_name}")),
            Some(column.clone()),
        ),
        Error::ColumnAlreadyExist { full_name, column } => (
            StatusCode::BAD_REQUEST,
            Some(format!("column:{full_name}")),
            Some(column.clone()),
        ),
        Error::IdentifierInvalid { .. } | Error::ConfigInvalid { .. } => {
            (StatusCode::BAD_REQUEST, None, None)
        }
        Error::Unsupported { .. } => (StatusCode::NOT_IMPLEMENTED, None, None),
        _ => (StatusCode::INTERNAL_SERVER_ERROR, None, None),
    };

    let body = ErrorResponse::new(
        resource_type,
        resource_name,
        Some(e.to_string()),
        Some(status.as_u16() as i32),
    );
    (status, Json(body)).into_response()
}

/// A 2xx response with an empty JSON body, for operations whose REST contract
/// returns nothing meaningful (the client deserializes these as `Value`).
fn ok_empty() -> Response {
    (StatusCode::OK, Json(json!({}))).into_response()
}

// ============================================================================
// Path parameter decoding
//
// The client (`ResourcePaths`) builds path segments with `RESTUtil::encode_string`
// (`application/x-www-form-urlencoded`), so e.g. a space becomes `+`. Axum's own
// `Path`/`RawPathParams` extractors percent-decode `%xx` but leave `+` untouched,
// which makes catalog names containing spaces unaddressable through `RESTCatalog`.
//
// We therefore decode the *raw* (still percent-encoded) URI segments with the
// same `RESTUtil` codec, mirroring Java's `RESTCatalogServer`, which calls
// `RESTUtil.decodeString` on the raw segments. Decoding the raw segment (rather
// than post-processing Axum's already percent-decoded value) is the only way to
// recover names correctly for all inputs — a literal `+` (encoded as `%2B`) and
// a real space (encoded as `+`) are indistinguishable once `%xx` is decoded.
// ============================================================================

/// Path parameters captured from the matched route, decoded with the REST codec.
struct RestPath(HashMap<String, String>);

impl RestPath {
    /// The decoded value of a captured parameter (empty string if absent).
    fn get(&self, key: &str) -> String {
        self.0.get(key).cloned().unwrap_or_default()
    }
}

#[async_trait::async_trait]
impl<S: Send + Sync> FromRequestParts<S> for RestPath {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // The route pattern (e.g. `/v1/databases/:db/tables/:table`) is recorded
        // by Axum in the request extensions once a route matches.
        let pattern = parts
            .extensions
            .get::<MatchedPath>()
            .map(|m| m.as_str().to_string());
        // `parts.uri.path()` is the original, still-percent-encoded request path.
        let raw_path = parts.uri.path().to_string();

        let mut params = HashMap::new();
        if let Some(pattern) = pattern {
            for (pat_seg, raw_seg) in pattern.split('/').zip(raw_path.split('/')) {
                if let Some(name) = pat_seg.strip_prefix(':') {
                    params.insert(name.to_string(), RESTUtil::decode_string(raw_seg));
                }
            }
        }
        Ok(RestPath(params))
    }
}

// ============================================================================
// Handlers
// ============================================================================

async fn get_config(
    Query(_params): Query<HashMap<String, String>>,
    Extension(state): Extension<Arc<AppState>>,
) -> Response {
    (StatusCode::OK, Json(state.config.clone())).into_response()
}

async fn list_databases(Extension(state): Extension<Arc<AppState>>) -> Response {
    match state.catalog.list_databases().await {
        Ok(mut dbs) => {
            dbs.sort();
            (StatusCode::OK, Json(ListDatabasesResponse::new(dbs, None))).into_response()
        }
        Err(e) => error_response(e),
    }
}

async fn create_database(
    Extension(state): Extension<Arc<AppState>>,
    Json(payload): Json<paimon::api::CreateDatabaseRequest>,
) -> Response {
    match state
        .catalog
        .create_database(&payload.name, false, payload.options)
        .await
    {
        Ok(()) => ok_empty(),
        Err(e) => error_response(e),
    }
}

async fn get_database(path: RestPath, Extension(state): Extension<Arc<AppState>>) -> Response {
    let db = path.get("db");
    match state.catalog.get_database(&db).await {
        Ok(database) => {
            let response = GetDatabaseResponse::new(
                Some(database.name.clone()),
                Some(database.name),
                None,
                database.options,
                empty_audit(),
            );
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => error_response(e),
    }
}

/// Alter database: `FileSystemCatalog` does not persist database properties,
/// and the `Catalog` trait has no `alter_database`, so no client path reaches
/// this. We only validate that the database exists and return OK; the request
/// is intentionally a no-op.
async fn alter_database(
    path: RestPath,
    Extension(state): Extension<Arc<AppState>>,
    Json(_request): Json<AlterDatabaseRequest>,
) -> Response {
    let db = path.get("db");
    match state.catalog.get_database(&db).await {
        Ok(_) => ok_empty(),
        Err(e) => error_response(e),
    }
}

async fn drop_database(path: RestPath, Extension(state): Extension<Arc<AppState>>) -> Response {
    let db = path.get("db");
    // The client (`RESTCatalog::drop_database`) already enforces the non-cascade
    // "database must be empty" check before issuing the DELETE, so the server
    // force-drops with cascade=true.
    match state.catalog.drop_database(&db, false, true).await {
        Ok(()) => ok_empty(),
        Err(e) => error_response(e),
    }
}

async fn list_tables(path: RestPath, Extension(state): Extension<Arc<AppState>>) -> Response {
    let db = path.get("db");
    match state.catalog.list_tables(&db).await {
        Ok(mut tables) => {
            tables.sort();
            (
                StatusCode::OK,
                Json(ListTablesResponse::new(Some(tables), None)),
            )
                .into_response()
        }
        Err(e) => error_response(e),
    }
}

async fn create_table(
    path: RestPath,
    Extension(state): Extension<Arc<AppState>>,
    Json(request): Json<CreateTableRequest>,
) -> Response {
    // Trust the path's database; take the table name from the request body.
    let identifier = Identifier::new(path.get("db"), request.identifier.object().to_string());
    match state
        .catalog
        .create_table(&identifier, request.schema, false)
        .await
    {
        Ok(()) => ok_empty(),
        Err(e) => error_response(e),
    }
}

async fn get_table(path: RestPath, Extension(state): Extension<Arc<AppState>>) -> Response {
    let table = path.get("table");
    let identifier = Identifier::new(path.get("db"), table.clone());
    let resolved = match state.catalog.get_table(&identifier).await {
        Ok(t) => t,
        Err(e) => return error_response(e),
    };

    let table_schema = resolved.schema();
    // Convert the stored `TableSchema` into the DDL `Schema` the response
    // carries. `Schema` is a field subset of `TableSchema` (both camelCase),
    // and serde ignores the extra keys, preserving field ids exactly.
    let schema: Schema =
        match serde_json::to_value(table_schema).and_then(serde_json::from_value::<Schema>) {
            Ok(s) => s,
            Err(e) => {
                return error_response(Error::DataInvalid {
                    message: format!("Failed to convert table schema: {e}"),
                    source: Some(Box::new(e)),
                })
            }
        };

    let response = GetTableResponse::new(
        // FileSystemCatalog has no UUID concept; the full name is a stable id
        // that satisfies the client's RESTEnv requirement.
        Some(identifier.full_name()),
        Some(table),
        Some(resolved.location().to_string()),
        Some(false),
        Some(table_schema.id()),
        Some(schema),
        empty_audit(),
    );
    (StatusCode::OK, Json(response)).into_response()
}

async fn drop_table(path: RestPath, Extension(state): Extension<Arc<AppState>>) -> Response {
    let identifier = Identifier::new(path.get("db"), path.get("table"));
    match state.catalog.drop_table(&identifier, false).await {
        Ok(()) => ok_empty(),
        Err(e) => error_response(e),
    }
}

async fn alter_table(
    path: RestPath,
    Extension(state): Extension<Arc<AppState>>,
    Json(request): Json<AlterTableRequest>,
) -> Response {
    let identifier = Identifier::new(path.get("db"), path.get("table"));
    match state
        .catalog
        .alter_table(&identifier, request.changes, false)
        .await
    {
        Ok(()) => ok_empty(),
        Err(e) => error_response(e),
    }
}

async fn rename_table(
    Extension(state): Extension<Arc<AppState>>,
    Json(request): Json<RenameTableRequest>,
) -> Response {
    match state
        .catalog
        .rename_table(&request.source, &request.destination, false)
        .await
    {
        Ok(()) => ok_empty(),
        Err(e) => error_response(e),
    }
}

/// Request body posted by the client's `RESTSnapshotCommit` (see
/// `crates/paimon/src/api/rest_api.rs::commit_snapshot`).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CommitRequest {
    #[serde(default)]
    table_uuid: Option<String>,
    snapshot: Snapshot,
    #[serde(default)]
    statistics: serde_json::Value,
}

async fn commit(
    path: RestPath,
    Extension(state): Extension<Arc<AppState>>,
    Json(request): Json<CommitRequest>,
) -> Response {
    let _ = (request.table_uuid, request.statistics);
    let identifier = Identifier::new(path.get("db"), path.get("table"));

    // Resolve the table's FileIO and on-disk location, then persist the posted
    // snapshot exactly like the filesystem catalog's own commit path does.
    let resolved = match state.catalog.get_table(&identifier).await {
        Ok(t) => t,
        Err(e) => return error_response(e),
    };
    let manager = SnapshotManager::new(resolved.file_io().clone(), resolved.location().to_string());
    match manager.commit_snapshot(&request.snapshot).await {
        Ok(success) => (StatusCode::OK, Json(json!({ "success": success }))).into_response(),
        Err(e) => error_response(e),
    }
}

/// List a table's partitions, computed from the latest snapshot on disk.
///
/// Mirrors `Catalog::list_partitions`: resolve the table, then derive partition
/// aggregates from the filesystem via [`list_partitions_from_file_system`]. The
/// pagination params (`maxResults`/`pageToken`) are accepted but ignored — the
/// whole set is returned in one page (`nextPageToken = null`).
async fn list_partitions(
    path: RestPath,
    Query(_params): Query<HashMap<String, String>>,
    Extension(state): Extension<Arc<AppState>>,
) -> Response {
    let identifier = Identifier::new(path.get("db"), path.get("table"));
    let resolved = match state.catalog.get_table(&identifier).await {
        Ok(t) => t,
        Err(e) => return error_response(e),
    };
    match list_partitions_from_file_system(&resolved).await {
        Ok(partitions) => (
            StatusCode::OK,
            Json(ListPartitionsResponse::new(Some(partitions), None)),
        )
            .into_response(),
        Err(e) => error_response(e),
    }
}

async fn table_token_stub() -> Response {
    let body = ErrorResponse::new(
        None,
        None,
        Some(
            "Data token is not supported by paimon-rest-server; \
             set data-token.enabled=false (the default)."
                .to_string(),
        ),
        Some(StatusCode::NOT_IMPLEMENTED.as_u16() as i32),
    );
    (StatusCode::NOT_IMPLEMENTED, Json(body)).into_response()
}

fn empty_audit() -> AuditRESTResponse {
    AuditRESTResponse::new(None, None, None, None, None)
}
