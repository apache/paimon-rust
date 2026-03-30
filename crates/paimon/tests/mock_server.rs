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

//! Mock REST server for testing.
//!
//! This module provides a mock HTTP server that simulates the Paimon REST API
//! for testing purposes.

use axum::{
    extract::{Extension, Json, Path, Query},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    serve, Router,
};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;

use paimon::api::{
    AlterDatabaseRequest, AuditRESTResponse, ConfigResponse, ErrorResponse, GetDatabaseResponse,
    GetTableResponse, ListDatabasesResponse, ListTablesResponse, RenameTableRequest, ResourcePaths,
};

#[derive(Clone, Debug, Default)]
struct MockState {
    databases: HashMap<String, GetDatabaseResponse>,
    tables: HashMap<String, GetTableResponse>,
    no_permission_databases: HashSet<String>,
    no_permission_tables: HashSet<String>,
    /// ECS metadata role name (for token loader testing)
    ecs_role_name: Option<String>,
    /// ECS metadata token (for token loader testing)
    ecs_token: Option<serde_json::Value>,
}

#[derive(Clone)]
pub struct RESTServer {
    warehouse: String,
    #[allow(dead_code)]
    data_path: String,
    config: ConfigResponse,
    inner: Arc<Mutex<MockState>>,
    resource_paths: ResourcePaths,
    addr: Option<SocketAddr>,
    server_handle: Option<Arc<JoinHandle<()>>>,
}

impl RESTServer {
    /// Create a new RESTServer with initial databases.
    pub fn new(
        warehouse: String,
        data_path: String,
        config: ConfigResponse,
        initial_dbs: Vec<String>,
    ) -> Self {
        let prefix = config.defaults.get("prefix").cloned().unwrap_or_default();

        // Create database set for initial databases
        let databases: HashMap<String, GetDatabaseResponse> = initial_dbs
            .into_iter()
            .map(|name| {
                let response = GetDatabaseResponse::new(
                    Some(name.clone()),
                    Some(name.clone()),
                    None,
                    HashMap::new(),
                    AuditRESTResponse::new(None, None, None, None, None),
                );
                (name, response)
            })
            .collect();

        RESTServer {
            data_path,
            config,
            warehouse,
            inner: Arc::new(Mutex::new(MockState {
                databases,
                ..Default::default()
            })),
            resource_paths: ResourcePaths::new(&prefix),
            addr: None,
            server_handle: None,
        }
    }

    // ==================== HTTP Handlers ====================

    /// Handle GET /v1/config - return config for RESTApi initialization.
    pub async fn get_config(
        Query(params): Query<HashMap<String, String>>,
        Extension(state): Extension<Arc<RESTServer>>,
    ) -> impl IntoResponse {
        // Check if warehouse parameter matches
        let warehouse_param = params.get("warehouse");
        if let Some(warehouse) = warehouse_param {
            if warehouse != &state.warehouse {
                let err = ErrorResponse::new(
                    None,
                    None,
                    Some(format!("Warehouse {warehouse} not found")),
                    Some(404),
                );
                return (StatusCode::NOT_FOUND, Json(err)).into_response();
            }
        }
        (StatusCode::OK, Json(state.config.clone())).into_response()
    }

    /// Handle GET /databases - list all databases.
    pub async fn list_databases(Extension(state): Extension<Arc<RESTServer>>) -> impl IntoResponse {
        let s = state.inner.lock().unwrap();
        let mut dbs: Vec<String> = s.databases.keys().cloned().collect();
        dbs.sort();
        let response = ListDatabasesResponse::new(dbs, None);
        (StatusCode::OK, Json(response))
    }
    /// Handle POST /databases - create a new database.
    pub async fn create_database(
        Extension(state): Extension<Arc<RESTServer>>,
        Json(payload): Json<serde_json::Value>,
    ) -> impl IntoResponse {
        let name = match payload.get("name").and_then(|n| n.as_str()) {
            Some(n) => n.to_string(),
            None => {
                let err =
                    ErrorResponse::new(None, None, Some("Missing name".to_string()), Some(400));
                return (StatusCode::BAD_REQUEST, Json(err)).into_response();
            }
        };

        let mut s = state.inner.lock().unwrap();
        if let std::collections::hash_map::Entry::Vacant(e) = s.databases.entry(name.clone()) {
            let response = GetDatabaseResponse::new(
                Some(name.clone()),
                Some(name.clone()),
                None,
                HashMap::new(),
                AuditRESTResponse::new(None, None, None, None, None),
            );
            e.insert(response);
            (StatusCode::OK, Json(serde_json::json!(""))).into_response()
        } else {
            let err = ErrorResponse::new(
                Some("database".to_string()),
                Some(name),
                Some("Already Exists".to_string()),
                Some(409),
            );
            (StatusCode::CONFLICT, Json(err)).into_response()
        }
    }
    /// Handle GET /databases/:name - get a specific database.
    pub async fn get_database(
        Path(name): Path<String>,
        Extension(state): Extension<Arc<RESTServer>>,
    ) -> impl IntoResponse {
        let s = state.inner.lock().unwrap();

        if s.no_permission_databases.contains(&name) {
            let err = ErrorResponse::new(
                Some("database".to_string()),
                Some(name.clone()),
                Some("No Permission".to_string()),
                Some(403),
            );
            return (StatusCode::FORBIDDEN, Json(err)).into_response();
        }

        if let Some(response) = s.databases.get(&name) {
            (StatusCode::OK, Json(response.clone())).into_response()
        } else {
            let err = ErrorResponse::new(
                Some("database".to_string()),
                Some(name.clone()),
                Some("Not Found".to_string()),
                Some(404),
            );
            (StatusCode::NOT_FOUND, Json(err)).into_response()
        }
    }

    /// Handle POST /databases/:name - alter database configuration.
    pub async fn alter_database(
        Path(name): Path<String>,
        Extension(state): Extension<Arc<RESTServer>>,
        Json(request): Json<AlterDatabaseRequest>,
    ) -> impl IntoResponse {
        let mut s = state.inner.lock().unwrap();

        if s.no_permission_databases.contains(&name) {
            let err = ErrorResponse::new(
                Some("database".to_string()),
                Some(name.clone()),
                Some("No Permission".to_string()),
                Some(403),
            );
            return (StatusCode::FORBIDDEN, Json(err)).into_response();
        }

        if let Some(response) = s.databases.get_mut(&name) {
            // Apply removals
            for key in &request.removals {
                response.options.remove(key);
            }
            // Apply updates
            response.options.extend(request.updates);
            (StatusCode::OK, Json(serde_json::json!(""))).into_response()
        } else {
            let err = ErrorResponse::new(
                Some("database".to_string()),
                Some(name.clone()),
                Some("Not Found".to_string()),
                Some(404),
            );
            (StatusCode::NOT_FOUND, Json(err)).into_response()
        }
    }

    /// Handle DELETE /databases/:name - drop a database.
    pub async fn drop_database(
        Path(name): Path<String>,
        Extension(state): Extension<Arc<RESTServer>>,
    ) -> impl IntoResponse {
        let mut s = state.inner.lock().unwrap();

        if s.no_permission_databases.contains(&name) {
            let err = ErrorResponse::new(
                Some("database".to_string()),
                Some(name.clone()),
                Some("No Permission".to_string()),
                Some(403),
            );
            return (StatusCode::FORBIDDEN, Json(err)).into_response();
        }

        if s.databases.remove(&name).is_some() {
            // Also remove all tables in this database
            let prefix = format!("{name}.");
            s.tables.retain(|key, _| !key.starts_with(&prefix));
            s.no_permission_tables
                .retain(|key| !key.starts_with(&prefix));
            (StatusCode::OK, Json(serde_json::json!(""))).into_response()
        } else {
            let err = ErrorResponse::new(
                Some("database".to_string()),
                Some(name.clone()),
                Some("Not Found".to_string()),
                Some(404),
            );
            (StatusCode::NOT_FOUND, Json(err)).into_response()
        }
    }

    /// Handle GET /databases/:db/tables - list all tables in a database.
    pub async fn list_tables(
        Path(db): Path<String>,
        Extension(state): Extension<Arc<RESTServer>>,
    ) -> impl IntoResponse {
        let s = state.inner.lock().unwrap();

        if s.no_permission_databases.contains(&db) {
            let err = ErrorResponse::new(
                Some("database".to_string()),
                Some(db.clone()),
                Some("No Permission".to_string()),
                Some(403),
            );
            return (StatusCode::FORBIDDEN, Json(err)).into_response();
        }

        if !s.databases.contains_key(&db) {
            let err = ErrorResponse::new(
                Some("database".to_string()),
                Some(db.clone()),
                Some("Not Found".to_string()),
                Some(404),
            );
            return (StatusCode::NOT_FOUND, Json(err)).into_response();
        }

        let prefix = format!("{db}.");
        let mut tables: Vec<String> = s
            .tables
            .keys()
            .filter_map(|key| {
                if key.starts_with(&prefix) {
                    Some(key[prefix.len()..].to_string())
                } else {
                    None
                }
            })
            .collect();
        tables.sort();

        let response = ListTablesResponse::new(Some(tables), None);
        (StatusCode::OK, Json(response)).into_response()
    }

    /// Handle POST /databases/:db/tables - create a new table.
    pub async fn create_table(
        Path(db): Path<String>,
        Extension(state): Extension<Arc<RESTServer>>,
        Json(payload): Json<serde_json::Value>,
    ) -> impl IntoResponse {
        // Extract table name from payload
        let table_name = payload
            .get("identifier")
            .and_then(|id| id.get("object"))
            .and_then(|o| o.as_str())
            .map(|s| s.to_string());

        let table_name = match table_name {
            Some(name) => name,
            None => {
                let err = ErrorResponse::new(
                    None,
                    None,
                    Some("Missing table name in identifier".to_string()),
                    Some(400),
                );
                return (StatusCode::BAD_REQUEST, Json(err)).into_response();
            }
        };

        let mut s = state.inner.lock().unwrap();

        // Check database exists
        if !s.databases.contains_key(&db) {
            let err = ErrorResponse::new(
                Some("database".to_string()),
                Some(db.clone()),
                Some("Not Found".to_string()),
                Some(404),
            );
            return (StatusCode::NOT_FOUND, Json(err)).into_response();
        }

        let key = format!("{db}.{table_name}");
        if s.tables.contains_key(&key) {
            let err = ErrorResponse::new(
                Some("table".to_string()),
                Some(table_name),
                Some("Already Exists".to_string()),
                Some(409),
            );
            return (StatusCode::CONFLICT, Json(err)).into_response();
        }

        // Create table response
        let response = GetTableResponse::new(
            Some(table_name.clone()),
            Some(table_name),
            None,
            Some(true),
            None,
            None,
            AuditRESTResponse::new(None, None, None, None, None),
        );
        s.tables.insert(key, response);
        (StatusCode::OK, Json(serde_json::json!(""))).into_response()
    }

    /// Handle GET /databases/:db/tables/:table - get a specific table.
    pub async fn get_table(
        Path((db, table)): Path<(String, String)>,
        Extension(state): Extension<Arc<RESTServer>>,
    ) -> impl IntoResponse {
        let s = state.inner.lock().unwrap();

        let key = format!("{db}.{table}");
        if s.no_permission_tables.contains(&key) {
            let err = ErrorResponse::new(
                Some("table".to_string()),
                Some(table.clone()),
                Some("No Permission".to_string()),
                Some(403),
            );
            return (StatusCode::FORBIDDEN, Json(err)).into_response();
        }

        if let Some(response) = s.tables.get(&key) {
            return (StatusCode::OK, Json(response.clone())).into_response();
        }

        if !s.databases.contains_key(&db) {
            let err = ErrorResponse::new(
                Some("database".to_string()),
                Some(db),
                Some("Not Found".to_string()),
                Some(404),
            );
            return (StatusCode::NOT_FOUND, Json(err)).into_response();
        }

        let err = ErrorResponse::new(
            Some("table".to_string()),
            Some(table),
            Some("Not Found".to_string()),
            Some(404),
        );
        (StatusCode::NOT_FOUND, Json(err)).into_response()
    }

    /// Handle DELETE /databases/:db/tables/:table - drop a table.
    pub async fn drop_table(
        Path((db, table)): Path<(String, String)>,
        Extension(state): Extension<Arc<RESTServer>>,
    ) -> impl IntoResponse {
        let mut s = state.inner.lock().unwrap();

        let key = format!("{db}.{table}");
        if s.no_permission_tables.contains(&key) {
            let err = ErrorResponse::new(
                Some("table".to_string()),
                Some(table.clone()),
                Some("No Permission".to_string()),
                Some(403),
            );
            return (StatusCode::FORBIDDEN, Json(err)).into_response();
        }

        if s.tables.remove(&key).is_some() {
            s.no_permission_tables.remove(&key);
            (StatusCode::OK, Json(serde_json::json!(""))).into_response()
        } else {
            let err = ErrorResponse::new(
                Some("table".to_string()),
                Some(table),
                Some("Not Found".to_string()),
                Some(404),
            );
            (StatusCode::NOT_FOUND, Json(err)).into_response()
        }
    }

    /// Handle POST /rename-table - rename a table.
    pub async fn rename_table(
        Extension(state): Extension<Arc<RESTServer>>,
        Json(request): Json<RenameTableRequest>,
    ) -> impl IntoResponse {
        let mut s = state.inner.lock().unwrap();

        let source_key = format!("{}.{}", request.source.database(), request.source.object());
        let dest_key = format!(
            "{}.{}",
            request.destination.database(),
            request.destination.object()
        );

        // Check source table permission
        if s.no_permission_tables.contains(&source_key) {
            let err = ErrorResponse::new(
                Some("table".to_string()),
                Some(request.source.object().to_string()),
                Some("No Permission".to_string()),
                Some(403),
            );
            return (StatusCode::FORBIDDEN, Json(err)).into_response();
        }

        // Check if source table exists
        if let Some(table_response) = s.tables.remove(&source_key) {
            // Check if destination already exists
            if s.tables.contains_key(&dest_key) {
                // Restore source table
                s.tables.insert(source_key, table_response);
                let err = ErrorResponse::new(
                    Some("table".to_string()),
                    Some(dest_key.clone()),
                    Some("Already Exists".to_string()),
                    Some(409),
                );
                return (StatusCode::CONFLICT, Json(err)).into_response();
            }

            // Update the table name in response and insert at new location
            let new_table_response = GetTableResponse::new(
                Some(request.destination.object().to_string()),
                Some(request.destination.object().to_string()),
                table_response.path,
                table_response.is_external,
                table_response.schema_id,
                table_response.schema,
                table_response.audit,
            );
            s.tables.insert(dest_key.clone(), new_table_response);

            // Update permission tracking if needed
            if s.no_permission_tables.remove(&source_key) {
                s.no_permission_tables.insert(dest_key.clone());
            }

            (StatusCode::OK, Json(serde_json::json!(""))).into_response()
        } else {
            let err = ErrorResponse::new(
                Some("table".to_string()),
                Some(source_key),
                Some("Not Found".to_string()),
                Some(404),
            );
            (StatusCode::NOT_FOUND, Json(err)).into_response()
        }
    }
    // ====================== Server Control ====================
    /// Add a database to the server state.
    #[allow(dead_code)]
    pub fn add_database(&self, name: &str) {
        let mut s = self.inner.lock().unwrap();
        s.databases.entry(name.to_string()).or_insert_with(|| {
            GetDatabaseResponse::new(
                Some(name.to_string()),
                Some(name.to_string()),
                None,
                HashMap::new(),
                AuditRESTResponse::new(None, None, None, None, None),
            )
        });
    }
    /// Add a no-permission database to the server state.
    #[allow(dead_code)]
    pub fn add_no_permission_database(&self, name: &str) {
        let mut s = self.inner.lock().unwrap();
        s.no_permission_databases.insert(name.to_string());
    }

    /// Add a table to the server state.
    #[allow(dead_code)]
    pub fn add_table(&self, database: &str, table: &str) {
        let mut s = self.inner.lock().unwrap();
        s.databases.entry(database.to_string()).or_insert_with(|| {
            // Auto-create database if not exists
            GetDatabaseResponse::new(
                Some(database.to_string()),
                Some(database.to_string()),
                None,
                HashMap::new(),
                AuditRESTResponse::new(None, None, None, None, None),
            )
        });

        let key = format!("{database}.{table}");
        s.tables.entry(key).or_insert_with(|| {
            GetTableResponse::new(
                Some(table.to_string()),
                Some(table.to_string()),
                None,
                Some(true),
                None,
                None,
                AuditRESTResponse::new(None, None, None, None, None),
            )
        });
    }

    /// Add a table with schema and path to the server state.
    ///
    /// This is needed for `RESTCatalog::get_table` which requires
    /// the response to contain `schema` and `path`.
    #[allow(dead_code)]
    pub fn add_table_with_schema(
        &self,
        database: &str,
        table: &str,
        schema: paimon::spec::Schema,
        path: &str,
    ) {
        let mut s = self.inner.lock().unwrap();
        s.databases.entry(database.to_string()).or_insert_with(|| {
            GetDatabaseResponse::new(
                Some(database.to_string()),
                Some(database.to_string()),
                None,
                HashMap::new(),
                AuditRESTResponse::new(None, None, None, None, None),
            )
        });

        let key = format!("{}.{}", database, table);
        s.tables.insert(
            key,
            GetTableResponse::new(
                Some(table.to_string()),
                Some(table.to_string()),
                Some(path.to_string()),
                Some(true),
                Some(0),
                Some(schema),
                AuditRESTResponse::new(None, None, None, None, None),
            ),
        );
    }

    /// Add a no-permission table to the server state.
    #[allow(dead_code)]
    pub fn add_no_permission_table(&self, database: &str, table: &str) {
        let mut s = self.inner.lock().unwrap();
        s.no_permission_tables.insert(format!("{database}.{table}"));
    }
    /// Get the server URL.
    pub fn url(&self) -> Option<String> {
        self.addr.map(|a| format!("http://{a}"))
    }
    /// Get the warehouse path.
    #[allow(dead_code)]
    pub fn warehouse(&self) -> &str {
        &self.warehouse
    }

    /// Get the resource paths.
    pub fn resource_paths(&self) -> &ResourcePaths {
        &self.resource_paths
    }
    /// Get the server address.
    #[allow(dead_code)]
    pub fn addr(&self) -> Option<SocketAddr> {
        self.addr
    }

    /// Set ECS metadata role name and token for token loader testing.
    #[allow(dead_code)]
    pub fn set_ecs_metadata(&self, role_name: &str, token: serde_json::Value) {
        let mut s = self.inner.lock().unwrap();
        s.ecs_role_name = Some(role_name.to_string());
        s.ecs_token = Some(token);
    }

    /// Handle GET /ram/security-credential/:role - ECS metadata endpoint.
    pub async fn get_ecs_metadata(
        Path(role): Path<String>,
        Extension(state): Extension<Arc<RESTServer>>,
    ) -> impl IntoResponse {
        let s = state.inner.lock().unwrap();

        // If role_name is set and matches, return the token
        if let Some(expected_role) = &s.ecs_role_name {
            if &role == expected_role {
                if let Some(token) = &s.ecs_token {
                    return (StatusCode::OK, Json(token.clone())).into_response();
                }
            }
        }

        (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "Role not found"})),
        )
            .into_response()
    }

    /// Handle GET /ram/security-credential/ - ECS metadata endpoint (list roles).
    pub async fn list_ecs_roles(Extension(state): Extension<Arc<RESTServer>>) -> impl IntoResponse {
        let s = state.inner.lock().unwrap();

        if let Some(role_name) = &s.ecs_role_name {
            (StatusCode::OK, role_name.clone()).into_response()
        } else {
            (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "No role configured"})),
            )
                .into_response()
        }
    }
}

impl Drop for RESTServer {
    fn drop(&mut self) {
        if let Some(handle) = &self.server_handle {
            handle.abort();
        }
    }
}

/// Start a mock REST server with configuration.
///
/// # Arguments
/// * `warehouse` - Warehouse path.
/// * `data_path` - Data path for storage.
/// * `config` - Configuration response containing defaults like prefix.
/// * `initial_dbs` - Initial databases to create.
///
/// # Returns
/// A RESTServer with address and control.
pub async fn start_mock_server(
    warehouse: String,
    data_path: String,
    config: ConfigResponse,
    initial_dbs: Vec<String>,
) -> RESTServer {
    let mut server = RESTServer::new(warehouse, data_path, config, initial_dbs);

    // Build routes based on prefix from config
    let prefix = server.resource_paths().base_path();
    let state = Arc::new(server.clone());
    let app = Router::new()
        // Config endpoint (for RESTApi initialization)
        .route("/v1/config", get(RESTServer::get_config))
        // Database routes
        .route(
            &format!("{prefix}/databases"),
            get(RESTServer::list_databases).post(RESTServer::create_database),
        )
        .route(
            &format!("{prefix}/databases/:name"),
            get(RESTServer::get_database)
                .post(RESTServer::alter_database)
                .delete(RESTServer::drop_database),
        )
        .route(
            &format!("{prefix}/databases/:db/tables"),
            get(RESTServer::list_tables).post(RESTServer::create_table),
        )
        .route(
            &format!("{prefix}/databases/:db/tables/:table"),
            get(RESTServer::get_table).delete(RESTServer::drop_table),
        )
        .route(
            &format!("{prefix}/tables/rename"),
            post(RESTServer::rename_table),
        )
        // ECS metadata endpoints (for token loader testing)
        .route(
            "/ram/security-credentials/",
            get(RESTServer::list_ecs_roles),
        )
        .route(
            "/ram/security-credentials/:role",
            get(RESTServer::get_ecs_metadata),
        )
        .layer(Extension(state));

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind failed");
    let addr = listener.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        if let Err(e) = serve(listener, app.into_make_service()).await {
            eprintln!("mock server error: {e}");
        }
    });

    server.addr = Some(addr);
    server.server_handle = Some(Arc::new(server_handle));
    server
}
