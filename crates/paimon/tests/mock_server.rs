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
    AlterDatabaseRequest, AlterTableRequest, AuditRESTResponse, ConfigResponse,
    CreateFunctionRequest, CreatePartitionsRequest, CreateViewRequest, DropPartitionsRequest,
    ErrorResponse, GetDatabaseResponse, GetFunctionResponse, GetTableResponse, GetViewResponse,
    ListDatabasesResponse, ListFunctionsResponse, ListPartitionsByFilterRequest,
    ListPartitionsByNamesRequest, ListPartitionsResponse, ListTablesResponse, ListViewsResponse,
    RenameTableRequest, ResourcePaths,
};
use paimon::catalog::{Function, Identifier};
use paimon::spec::Partition;

type PartitionPageResponse = (Vec<Partition>, Option<String>);
type PartitionSpecPageResponse = (Vec<HashMap<String, String>>, Option<String>);

#[derive(Clone, Debug, Default)]
struct MockState {
    databases: HashMap<String, GetDatabaseResponse>,
    tables: HashMap<String, GetTableResponse>,
    views: HashMap<String, GetViewResponse>,
    functions: HashMap<String, GetFunctionResponse>,
    partitions: HashMap<String, Vec<Partition>>,
    partition_page_responses: HashMap<String, Vec<PartitionPageResponse>>,
    partition_list_call_counts: HashMap<String, usize>,
    partition_list_name_patterns: HashMap<String, Vec<Option<String>>>,
    partition_list_by_names_calls: HashMap<String, Vec<Vec<HashMap<String, String>>>>,
    partition_list_by_filter_requests: HashMap<String, Vec<ListPartitionsByFilterRequest>>,
    list_partitions_by_names_error_status: Option<StatusCode>,
    list_partitions_by_filter_error_status: Option<StatusCode>,
    view_function_endpoints_unsupported: bool,
    drop_view_error_status: Option<StatusCode>,
    list_page_size: Option<usize>,
    no_permission_databases: HashSet<String>,
    no_permission_tables: HashSet<String>,
    create_partitions_calls: Vec<(String, String, CreatePartitionsRequest)>,
    drop_partitions_calls: Vec<(String, String, DropPartitionsRequest)>,
    create_partitions_error_status: Option<StatusCode>,
    list_partitions_error_status: Option<StatusCode>,
    /// ECS metadata role name (for token loader testing)
    ecs_role_name: Option<String>,
    /// ECS metadata token (for token loader testing)
    ecs_token: Option<serde_json::Value>,
}

/// Match a partition spec against a partition-name pattern the way a catalog would.
///
/// Only the pattern shape clients send is understood: `key=value` segments joined by `/`,
/// optionally ending in a `/%` wildcard. Values are compared unescaped, so a test that
/// needs escaping should assert the pattern the client sent instead of the rows returned.
fn partition_spec_matches_name_pattern(spec: &HashMap<String, String>, pattern: &str) -> bool {
    let prefix = pattern.strip_suffix("/%");
    let segments = prefix.unwrap_or(pattern).split('/').collect::<Vec<_>>();
    if prefix.is_none() && segments.len() != spec.len() {
        return false;
    }
    segments.iter().all(|segment| {
        segment
            .split_once('=')
            .is_some_and(|(key, value)| spec.get(key).map(String::as_str) == Some(value))
    })
}

fn partition_from_spec(spec: HashMap<String, String>) -> Partition {
    Partition {
        spec,
        record_count: Partition::UNKNOWN,
        file_size_in_bytes: Partition::UNKNOWN,
        file_count: Partition::UNKNOWN,
        last_file_creation_time: Partition::UNKNOWN,
        total_buckets: 0,
        done: false,
        created_at: None,
        created_by: None,
        updated_at: None,
        updated_by: None,
        options: None,
    }
}

fn paginate_names(
    names: Vec<String>,
    params: &HashMap<String, String>,
    page_size: Option<usize>,
) -> (Vec<String>, Option<String>) {
    let Some(page_size) = page_size else {
        return (names, None);
    };
    let offset = params
        .get("pageToken")
        .and_then(|token| token.parse::<usize>().ok())
        .unwrap_or(0)
        .min(names.len());
    let end = (offset + page_size).min(names.len());
    let next_page_token = (end < names.len()).then(|| end.to_string());
    (names[offset..end].to_vec(), next_page_token)
}

#[derive(Clone)]
pub struct RESTServer {
    warehouse: String,
    _data_path: String,
    config: ConfigResponse,
    inner: Arc<Mutex<MockState>>,
    resource_paths: ResourcePaths,
    addr: Option<SocketAddr>,
    server_handle: Option<Arc<JoinHandle<()>>>,
}

#[allow(dead_code)]
impl RESTServer {
    /// Create a new RESTServer with initial databases.
    pub fn new(
        warehouse: String,
        _data_path: String,
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
            _data_path,
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
            s.partitions.retain(|key, _| !key.starts_with(&prefix));
            s.partition_page_responses
                .retain(|key, _| !key.starts_with(&prefix));
            s.partition_list_call_counts
                .retain(|key, _| !key.starts_with(&prefix));
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

    /// Handle GET /databases/:db/views/:view - get a persistent view.
    pub async fn get_view(
        Path((db, view)): Path<(String, String)>,
        Extension(state): Extension<Arc<RESTServer>>,
    ) -> impl IntoResponse {
        let s = state.inner.lock().unwrap();
        if s.view_function_endpoints_unsupported {
            let err = ErrorResponse::new(
                Some("view".to_string()),
                Some(view),
                Some("Not Implemented".to_string()),
                Some(501),
            );
            return (StatusCode::NOT_IMPLEMENTED, Json(err)).into_response();
        }
        let key = format!("{db}.{view}");
        if let Some(response) = s.views.get(&key) {
            (StatusCode::OK, Json(response.clone())).into_response()
        } else {
            let err = ErrorResponse::new(
                Some("view".to_string()),
                Some(view),
                Some("Not Found".to_string()),
                Some(404),
            );
            (StatusCode::NOT_FOUND, Json(err)).into_response()
        }
    }

    /// Handle DELETE /databases/:db/views/:view - drop a persistent view.
    pub async fn drop_view(
        Path((db, view)): Path<(String, String)>,
        Extension(state): Extension<Arc<RESTServer>>,
    ) -> impl IntoResponse {
        let mut s = state.inner.lock().unwrap();
        if s.view_function_endpoints_unsupported {
            let err = ErrorResponse::new(
                Some("view".to_string()),
                Some(view),
                Some("Not Implemented".to_string()),
                Some(501),
            );
            return (StatusCode::NOT_IMPLEMENTED, Json(err)).into_response();
        }
        if let Some(status) = s.drop_view_error_status {
            let err = ErrorResponse::new(
                Some("view".to_string()),
                Some(view),
                status.canonical_reason().map(ToString::to_string),
                Some(status.as_u16() as i32),
            );
            return (status, Json(err)).into_response();
        }
        let key = format!("{db}.{view}");
        if s.views.remove(&key).is_some() {
            (StatusCode::OK, Json(serde_json::json!(""))).into_response()
        } else {
            let err = ErrorResponse::new(
                Some("view".to_string()),
                Some(view),
                Some("Not Found".to_string()),
                Some(404),
            );
            (StatusCode::NOT_FOUND, Json(err)).into_response()
        }
    }

    /// Handle GET /databases/:db/views - list persistent views.
    pub async fn list_views(
        Path(db): Path<String>,
        Query(params): Query<HashMap<String, String>>,
        Extension(state): Extension<Arc<RESTServer>>,
    ) -> impl IntoResponse {
        let s = state.inner.lock().unwrap();
        if s.view_function_endpoints_unsupported {
            let err = ErrorResponse::new(
                Some("view".to_string()),
                None,
                Some("Not Implemented".to_string()),
                Some(501),
            );
            return (StatusCode::NOT_IMPLEMENTED, Json(err)).into_response();
        }
        let prefix = format!("{db}.");
        let mut views: Vec<String> = s
            .views
            .keys()
            .filter_map(|key| key.strip_prefix(&prefix).map(ToString::to_string))
            .collect();
        views.sort();
        let (views, next_page_token) = paginate_names(views, &params, s.list_page_size);
        (
            StatusCode::OK,
            Json(ListViewsResponse::new(views, next_page_token)),
        )
            .into_response()
    }

    /// Handle POST /databases/:db/views - create a persistent view.
    pub async fn create_view(
        Path(db): Path<String>,
        Extension(state): Extension<Arc<RESTServer>>,
        Json(request): Json<CreateViewRequest>,
    ) -> impl IntoResponse {
        let mut s = state.inner.lock().unwrap();
        let view = request.identifier.object().to_string();
        if s.view_function_endpoints_unsupported {
            let err = ErrorResponse::new(
                Some("view".to_string()),
                Some(view),
                Some("Not Implemented".to_string()),
                Some(501),
            );
            return (StatusCode::NOT_IMPLEMENTED, Json(err)).into_response();
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
        let key = format!("{db}.{view}");
        if s.views.contains_key(&key) {
            let err = ErrorResponse::new(
                Some("view".to_string()),
                Some(view),
                Some("Already Exists".to_string()),
                Some(409),
            );
            return (StatusCode::CONFLICT, Json(err)).into_response();
        }
        let response = GetViewResponse::new(
            Some(view.clone()),
            Some(view),
            request.schema,
            AuditRESTResponse::new(None, None, None, None, None),
        );
        s.views.insert(key, response);
        (StatusCode::OK, Json(serde_json::json!(""))).into_response()
    }

    /// Handle GET /databases/:db/functions/:function - get a persistent function.
    pub async fn get_function(
        Path((db, function)): Path<(String, String)>,
        Extension(state): Extension<Arc<RESTServer>>,
    ) -> impl IntoResponse {
        let s = state.inner.lock().unwrap();
        if s.view_function_endpoints_unsupported {
            let err = ErrorResponse::new(
                Some("function".to_string()),
                Some(function),
                Some("Not Implemented".to_string()),
                Some(501),
            );
            return (StatusCode::NOT_IMPLEMENTED, Json(err)).into_response();
        }
        let key = format!("{db}.{function}");
        if let Some(response) = s.functions.get(&key) {
            (StatusCode::OK, Json(response.clone())).into_response()
        } else {
            let err = ErrorResponse::new(
                Some("function".to_string()),
                Some(function),
                Some("Not Found".to_string()),
                Some(404),
            );
            (StatusCode::NOT_FOUND, Json(err)).into_response()
        }
    }

    /// Handle GET /databases/:db/functions - list persistent functions.
    pub async fn list_functions(
        Path(db): Path<String>,
        Query(params): Query<HashMap<String, String>>,
        Extension(state): Extension<Arc<RESTServer>>,
    ) -> impl IntoResponse {
        let s = state.inner.lock().unwrap();
        if s.view_function_endpoints_unsupported {
            let err = ErrorResponse::new(
                Some("function".to_string()),
                None,
                Some("Not Implemented".to_string()),
                Some(501),
            );
            return (StatusCode::NOT_IMPLEMENTED, Json(err)).into_response();
        }
        let prefix = format!("{db}.");
        let mut functions: Vec<String> = s
            .functions
            .keys()
            .filter_map(|key| key.strip_prefix(&prefix).map(ToString::to_string))
            .collect();
        functions.sort();
        let (functions, next_page_token) = paginate_names(functions, &params, s.list_page_size);
        (
            StatusCode::OK,
            Json(ListFunctionsResponse::new(functions, next_page_token)),
        )
            .into_response()
    }

    /// Handle POST /databases/:db/functions - create a persistent function.
    pub async fn create_function(
        Path(db): Path<String>,
        Extension(state): Extension<Arc<RESTServer>>,
        Json(request): Json<CreateFunctionRequest>,
    ) -> impl IntoResponse {
        let mut s = state.inner.lock().unwrap();
        let function_name = request.name.clone();
        if s.view_function_endpoints_unsupported {
            let err = ErrorResponse::new(
                Some("function".to_string()),
                Some(function_name),
                Some("Not Implemented".to_string()),
                Some(501),
            );
            return (StatusCode::NOT_IMPLEMENTED, Json(err)).into_response();
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
        let key = format!("{db}.{function_name}");
        if s.functions.contains_key(&key) {
            let err = ErrorResponse::new(
                Some("function".to_string()),
                Some(function_name),
                Some("Already Exists".to_string()),
                Some(409),
            );
            return (StatusCode::CONFLICT, Json(err)).into_response();
        }
        let function = Function::new(
            Identifier::new(&db, &request.name),
            request.input_params,
            request.return_params,
            request.deterministic,
            request.definitions,
            request.comment,
            request.options,
        );
        s.functions.insert(
            key,
            GetFunctionResponse::from_function(
                &function,
                AuditRESTResponse::new(None, None, None, None, None),
            ),
        );
        (StatusCode::OK, Json(json!({"function": function_name}))).into_response()
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
            s.partitions.remove(&key);
            s.partition_page_responses.remove(&key);
            s.partition_list_call_counts.remove(&key);
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

    /// Handle POST /databases/:db/tables/:table - alter a table.
    ///
    /// The mock does not mutate the stored schema; it only validates that the
    /// table exists, which is enough to exercise the client's alter-table path
    /// (request serialization + 2xx handling).
    pub async fn alter_table(
        Path((db, table)): Path<(String, String)>,
        Extension(state): Extension<Arc<RESTServer>>,
        Json(_request): Json<AlterTableRequest>,
    ) -> impl IntoResponse {
        let s = state.inner.lock().unwrap();
        let key = format!("{db}.{table}");
        if s.no_permission_tables.contains(&key) {
            let err = ErrorResponse::new(
                Some("table".to_string()),
                Some(table),
                Some("No Permission".to_string()),
                Some(403),
            );
            return (StatusCode::FORBIDDEN, Json(err)).into_response();
        }
        if s.tables.contains_key(&key) {
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

    /// Handle POST /databases/:db/tables/:table/partitions - create partitions.
    pub async fn create_partitions(
        Path((db, table)): Path<(String, String)>,
        Extension(state): Extension<Arc<RESTServer>>,
        Json(request): Json<CreatePartitionsRequest>,
    ) -> impl IntoResponse {
        let mut inner = state.inner.lock().unwrap();
        inner
            .create_partitions_calls
            .push((db.clone(), table.clone(), request.clone()));
        if let Some(status) = inner.create_partitions_error_status {
            let message = if status == StatusCode::CONFLICT {
                "Some partitions already exist"
            } else {
                "Invalid partition request"
            };
            let error = ErrorResponse::new(
                Some("partition".to_string()),
                Some(table.clone()),
                Some(message.to_string()),
                Some(status.as_u16() as i32),
            );
            return (status, Json(error)).into_response();
        }

        let key = format!("{db}.{table}");
        if !inner.tables.contains_key(&key) {
            let error = ErrorResponse::new(
                Some("table".to_string()),
                Some(table),
                Some("Not Found".to_string()),
                Some(404),
            );
            return (StatusCode::NOT_FOUND, Json(error)).into_response();
        }

        let registered_partitions = inner.partitions.entry(key).or_default();
        let has_conflict = request
            .partition_specs
            .iter()
            .enumerate()
            .any(|(index, spec)| {
                registered_partitions
                    .iter()
                    .any(|partition| partition.spec == *spec)
                    || request.partition_specs[..index].contains(spec)
            });
        if has_conflict && !request.ignore_if_exists {
            let error = ErrorResponse::new(
                Some("partition".to_string()),
                Some(table),
                Some("Some partitions already exist".to_string()),
                Some(StatusCode::CONFLICT.as_u16() as i32),
            );
            return (StatusCode::CONFLICT, Json(error)).into_response();
        }

        for spec in request.partition_specs {
            if !registered_partitions
                .iter()
                .any(|partition| partition.spec == spec)
            {
                registered_partitions.push(partition_from_spec(spec));
            }
        }
        // As the catalog does: a negative field was never measured and leaves the stored value
        // alone; a replacing report overwrites what is stored, any other is added to it.
        let replace = request.replace_statistics.unwrap_or(false);
        for statistic in request.partition_statistics.unwrap_or_default() {
            let Some(partition) = registered_partitions
                .iter_mut()
                .find(|partition| partition.spec == statistic.spec)
            else {
                continue;
            };
            for (stored, reported) in [
                (&mut partition.record_count, statistic.record_count),
                (
                    &mut partition.file_size_in_bytes,
                    statistic.file_size_in_bytes,
                ),
                (&mut partition.file_count, statistic.file_count),
            ] {
                if reported >= 0 {
                    *stored = if replace {
                        reported
                    } else {
                        (*stored).max(0) + reported
                    };
                }
            }
            if statistic.last_file_creation_time >= 0 {
                partition.last_file_creation_time = if replace {
                    statistic.last_file_creation_time
                } else {
                    partition
                        .last_file_creation_time
                        .max(statistic.last_file_creation_time)
                };
            }
        }
        let response = json!({"success": true});
        (StatusCode::OK, Json(response)).into_response()
    }

    /// Handle GET /databases/:db/tables/:table/partitions - list partitions.
    pub async fn list_partitions(
        Path((db, table)): Path<(String, String)>,
        Query(params): Query<HashMap<String, String>>,
        Extension(state): Extension<Arc<RESTServer>>,
    ) -> impl IntoResponse {
        let mut inner = state.inner.lock().unwrap();
        let key = format!("{db}.{table}");
        let name_pattern = params.get("partitionNamePattern").cloned();
        inner
            .partition_list_name_patterns
            .entry(key.clone())
            .or_default()
            .push(name_pattern.clone());
        if !inner.tables.contains_key(&key) {
            let error = ErrorResponse::new(
                Some("table".to_string()),
                Some(table),
                Some("Not Found".to_string()),
                Some(404),
            );
            return (StatusCode::NOT_FOUND, Json(error)).into_response();
        }
        if let Some(status) = inner.list_partitions_error_status {
            let error = ErrorResponse::new(
                Some("partition".to_string()),
                Some(table),
                Some("Partition listing is not implemented".to_string()),
                Some(status.as_u16() as i32),
            );
            return (status, Json(error)).into_response();
        }
        if inner.partition_page_responses.contains_key(&key) {
            let request_index = {
                let calls = inner
                    .partition_list_call_counts
                    .entry(key.clone())
                    .or_default();
                let request_index = *calls;
                *calls += 1;
                request_index
            };
            let responses = &inner.partition_page_responses[&key];
            let expected_token = request_index
                .checked_sub(1)
                .and_then(|index| responses.get(index))
                .and_then(|(_, token)| token.clone());
            let actual_token = params.get("pageToken").cloned();
            if actual_token != expected_token || request_index >= responses.len() {
                let error = ErrorResponse::new(
                    Some("partition".to_string()),
                    Some(table),
                    Some(format!(
                        "Invalid page token: expected {expected_token:?}, got {actual_token:?}"
                    )),
                    Some(StatusCode::BAD_REQUEST.as_u16() as i32),
                );
                return (StatusCode::BAD_REQUEST, Json(error)).into_response();
            }
            let (partitions, next_page_token) = responses[request_index].clone();
            return (
                StatusCode::OK,
                Json(ListPartitionsResponse::new(
                    Some(partitions),
                    next_page_token,
                )),
            )
                .into_response();
        }
        let mut partitions = inner.partitions.get(&key).cloned().unwrap_or_default();
        if let Some(pattern) = name_pattern {
            partitions
                .retain(|partition| partition_spec_matches_name_pattern(&partition.spec, &pattern));
        }
        (
            StatusCode::OK,
            Json(ListPartitionsResponse::new(Some(partitions), None)),
        )
            .into_response()
    }

    /// Handle POST /databases/:db/tables/:table/partitions/drop - drop partitions.
    pub async fn drop_partitions(
        Path((db, table)): Path<(String, String)>,
        Extension(state): Extension<Arc<RESTServer>>,
        Json(request): Json<DropPartitionsRequest>,
    ) -> impl IntoResponse {
        let mut inner = state.inner.lock().unwrap();
        inner
            .drop_partitions_calls
            .push((db.clone(), table.clone(), request.clone()));

        let key = format!("{db}.{table}");
        if !inner.tables.contains_key(&key) {
            let error = ErrorResponse::new(
                Some("table".to_string()),
                Some(table),
                Some("Not Found".to_string()),
                Some(404),
            );
            return (StatusCode::NOT_FOUND, Json(error)).into_response();
        }

        let registered_partitions = inner.partitions.entry(key).or_default();
        let has_missing = request.partition_specs.iter().any(|spec| {
            !registered_partitions
                .iter()
                .any(|partition| partition.spec == *spec)
        });
        if has_missing && !request.ignore_if_not_exists {
            let error = ErrorResponse::new(
                Some("partition".to_string()),
                Some(table),
                Some("Some partitions do not exist".to_string()),
                Some(StatusCode::NOT_FOUND.as_u16() as i32),
            );
            return (StatusCode::NOT_FOUND, Json(error)).into_response();
        }

        registered_partitions
            .retain(|partition| !request.partition_specs.contains(&partition.spec));
        let response = json!({"success": true});
        (StatusCode::OK, Json(response)).into_response()
    }

    /// Handle POST /databases/:db/tables/:table/partitions/list-by-names - look up partitions.
    pub async fn list_partitions_by_names(
        Path((db, table)): Path<(String, String)>,
        Extension(state): Extension<Arc<RESTServer>>,
        Json(request): Json<ListPartitionsByNamesRequest>,
    ) -> impl IntoResponse {
        let mut inner = state.inner.lock().unwrap();
        let key = format!("{db}.{table}");
        inner
            .partition_list_by_names_calls
            .entry(key.clone())
            .or_default()
            .push(request.specs.clone());
        if !inner.tables.contains_key(&key) {
            let error = ErrorResponse::new(
                Some("table".to_string()),
                Some(table),
                Some("Not Found".to_string()),
                Some(404),
            );
            return (StatusCode::NOT_FOUND, Json(error)).into_response();
        }
        if let Some(status) = inner.list_partitions_by_names_error_status {
            let error = ErrorResponse::new(
                Some("partition".to_string()),
                Some(table),
                Some("Listing partitions by names is not implemented".to_string()),
                Some(status.as_u16() as i32),
            );
            return (status, Json(error)).into_response();
        }
        let partitions = inner
            .partitions
            .get(&key)
            .map(|partitions| {
                partitions
                    .iter()
                    .filter(|partition| request.specs.contains(&partition.spec))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        (
            StatusCode::OK,
            Json(ListPartitionsResponse::new(Some(partitions), None)),
        )
            .into_response()
    }

    /// Handle POST /databases/:db/tables/:table/partitions/list-by-filter - list partitions.
    ///
    /// Like a catalog that does not evaluate predicates yet, this applies the name pattern only
    /// and returns every other registered partition, which the endpoint contract allows.
    pub async fn list_partitions_by_filter(
        Path((db, table)): Path<(String, String)>,
        Extension(state): Extension<Arc<RESTServer>>,
        Json(request): Json<ListPartitionsByFilterRequest>,
    ) -> impl IntoResponse {
        let mut inner = state.inner.lock().unwrap();
        let key = format!("{db}.{table}");
        inner
            .partition_list_by_filter_requests
            .entry(key.clone())
            .or_default()
            .push(request.clone());
        if !inner.tables.contains_key(&key) {
            let error = ErrorResponse::new(
                Some("table".to_string()),
                Some(table),
                Some("Not Found".to_string()),
                Some(404),
            );
            return (StatusCode::NOT_FOUND, Json(error)).into_response();
        }
        if let Some(status) = inner.list_partitions_by_filter_error_status {
            let error = ErrorResponse::new(
                Some("partition".to_string()),
                Some(table),
                Some("Listing partitions by filter is not implemented".to_string()),
                Some(status.as_u16() as i32),
            );
            return (status, Json(error)).into_response();
        }
        let mut partitions = inner.partitions.get(&key).cloned().unwrap_or_default();
        if let Some(pattern) = &request.partition_name_pattern {
            partitions
                .retain(|partition| partition_spec_matches_name_pattern(&partition.spec, pattern));
        }
        (
            StatusCode::OK,
            Json(ListPartitionsResponse::new(Some(partitions), None)),
        )
            .into_response()
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
    pub fn add_no_permission_database(&self, name: &str) {
        let mut s = self.inner.lock().unwrap();
        s.no_permission_databases.insert(name.to_string());
    }

    /// Add a table to the server state.
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

    /// Add a persistent view to the server state.
    pub fn add_view(&self, database: &str, view: &str, schema: paimon::catalog::ViewSchema) {
        let mut s = self.inner.lock().unwrap();
        let key = format!("{database}.{view}");
        s.views.insert(
            key,
            GetViewResponse::new(
                Some(view.to_string()),
                Some(view.to_string()),
                schema,
                AuditRESTResponse::new(None, None, None, None, None),
            ),
        );
    }

    /// Add a persistent function to the server state.
    pub fn add_function(&self, function: Function) {
        let key = function.full_name();
        let response = GetFunctionResponse::from_function(
            &function,
            AuditRESTResponse::new(None, None, None, None, None),
        );
        self.inner.lock().unwrap().functions.insert(key, response);
    }

    /// Force list-view and list-function handlers to paginate at this size.
    pub fn set_list_page_size(&self, page_size: usize) {
        self.inner.lock().unwrap().list_page_size = Some(page_size.max(1));
    }

    /// Make persistent view and function endpoints return HTTP 501.
    pub fn set_view_function_endpoints_unsupported(&self) {
        self.inner
            .lock()
            .unwrap()
            .view_function_endpoints_unsupported = true;
    }

    /// Make the drop-view endpoint return the given status.
    pub fn set_drop_view_error_status(&self, status: Option<StatusCode>) {
        self.inner.lock().unwrap().drop_view_error_status = status;
    }

    /// Make the create-partitions endpoint return the given status.
    pub fn set_create_partitions_error_status(&self, status: Option<StatusCode>) {
        self.inner.lock().unwrap().create_partitions_error_status = status;
    }

    /// Make the list-partitions endpoint return the given status.
    pub fn set_list_partitions_error_status(&self, status: Option<StatusCode>) {
        self.inner.lock().unwrap().list_partitions_error_status = status;
    }

    /// Make the list-partitions-by-names endpoint return the given status.
    pub fn set_list_partitions_by_names_error_status(&self, status: Option<StatusCode>) {
        self.inner
            .lock()
            .unwrap()
            .list_partitions_by_names_error_status = status;
    }

    /// Make the list-partitions-by-filter endpoint return the given status.
    pub fn set_list_partitions_by_filter_error_status(&self, status: Option<StatusCode>) {
        self.inner
            .lock()
            .unwrap()
            .list_partitions_by_filter_error_status = status;
    }

    /// Return the specs of every list-by-names request the table received, in order.
    pub fn table_partition_list_by_names_calls(
        &self,
        database: &str,
        table: &str,
    ) -> Vec<Vec<HashMap<String, String>>> {
        self.inner
            .lock()
            .unwrap()
            .partition_list_by_names_calls
            .get(&format!("{database}.{table}"))
            .cloned()
            .unwrap_or_default()
    }

    /// Return every list-by-filter request the table received, in order.
    pub fn table_partition_list_by_filter_requests(
        &self,
        database: &str,
        table: &str,
    ) -> Vec<ListPartitionsByFilterRequest> {
        self.inner
            .lock()
            .unwrap()
            .partition_list_by_filter_requests
            .get(&format!("{database}.{table}"))
            .cloned()
            .unwrap_or_default()
    }

    /// Add a table with schema and path to the server state.
    ///
    /// This is needed for `RESTCatalog::get_table` which requires
    /// the response to contain `schema` and `path`.
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

        let key = format!("{database}.{table}");
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
    pub fn add_no_permission_table(&self, database: &str, table: &str) {
        let mut s = self.inner.lock().unwrap();
        s.no_permission_tables.insert(format!("{database}.{table}"));
    }

    /// Set the catalog-registered partitions for a table.
    pub fn set_table_partitions(
        &self,
        database: &str,
        table: &str,
        partition_specs: Vec<HashMap<String, String>>,
    ) {
        let partitions = partition_specs
            .into_iter()
            .map(partition_from_spec)
            .collect();
        let key = format!("{database}.{table}");
        let mut inner = self.inner.lock().unwrap();
        assert!(
            inner.tables.contains_key(&key),
            "table {key} does not exist"
        );
        inner.partitions.insert(key.clone(), partitions);
        inner.partition_page_responses.remove(&key);
        inner.partition_list_call_counts.remove(&key);
    }

    /// Return the partition specs registered for a table, in registration order.
    pub fn table_partition_specs(
        &self,
        database: &str,
        table: &str,
    ) -> Vec<HashMap<String, String>> {
        self.inner
            .lock()
            .unwrap()
            .partitions
            .get(&format!("{database}.{table}"))
            .map(|partitions| {
                partitions
                    .iter()
                    .map(|partition| partition.spec.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Return the partitions registered for a table, statistics included, in registration order.
    pub fn table_partitions(&self, database: &str, table: &str) -> Vec<Partition> {
        self.inner
            .lock()
            .unwrap()
            .partitions
            .get(&format!("{database}.{table}"))
            .cloned()
            .unwrap_or_default()
    }

    /// Set whether a stored table is external.
    pub fn set_table_external(&self, database: &str, table: &str, is_external: bool) {
        let key = format!("{database}.{table}");
        let mut state = self.inner.lock().unwrap();
        state
            .tables
            .get_mut(&key)
            .unwrap_or_else(|| panic!("table {key} does not exist"))
            .is_external = Some(is_external);
    }

    /// Attach catalog options, such as a custom `path`, to a registered partition.
    pub fn set_table_partition_options(
        &self,
        database: &str,
        table: &str,
        spec: &HashMap<String, String>,
        options: HashMap<String, String>,
    ) {
        let mut inner = self.inner.lock().unwrap();
        let partition = inner
            .partitions
            .get_mut(&format!("{database}.{table}"))
            .and_then(|partitions| {
                partitions
                    .iter_mut()
                    .find(|partition| &partition.spec == spec)
            })
            .unwrap_or_else(|| panic!("partition {spec:?} is not registered"));
        partition.options = Some(options);
    }

    /// Set explicit list-partitions pages and response tokens in request order.
    pub fn set_table_partition_page_responses(
        &self,
        database: &str,
        table: &str,
        responses: Vec<PartitionSpecPageResponse>,
    ) {
        let responses = responses
            .into_iter()
            .map(|(specs, token)| (specs.into_iter().map(partition_from_spec).collect(), token))
            .collect();
        let key = format!("{database}.{table}");
        let mut inner = self.inner.lock().unwrap();
        assert!(
            inner.tables.contains_key(&key),
            "table {key} does not exist"
        );
        inner
            .partition_page_responses
            .insert(key.clone(), responses);
        inner.partition_list_call_counts.remove(&key);
    }

    /// Return the `partitionNamePattern` of every partition-list request the table
    /// received, in order, with `None` where the client sent no pattern.
    pub fn table_partition_list_name_patterns(
        &self,
        database: &str,
        table: &str,
    ) -> Vec<Option<String>> {
        self.inner
            .lock()
            .unwrap()
            .partition_list_name_patterns
            .get(&format!("{database}.{table}"))
            .cloned()
            .unwrap_or_default()
    }

    /// Return how many paged partition-list requests the table received.
    pub fn table_partition_list_call_count(&self, database: &str, table: &str) -> usize {
        self.inner
            .lock()
            .unwrap()
            .partition_list_call_counts
            .get(&format!("{database}.{table}"))
            .copied()
            .unwrap_or_default()
    }

    /// Return all create-partitions calls received by the server.
    pub fn create_partitions_calls(&self) -> Vec<(String, String, CreatePartitionsRequest)> {
        self.inner.lock().unwrap().create_partitions_calls.clone()
    }

    /// Return all drop-partitions calls received by the server.
    pub fn drop_partitions_calls(&self) -> Vec<(String, String, DropPartitionsRequest)> {
        self.inner.lock().unwrap().drop_partitions_calls.clone()
    }

    /// Get the server URL.
    pub fn url(&self) -> Option<String> {
        self.addr.map(|a| format!("http://{a}"))
    }
    /// Get the warehouse path.
    pub fn warehouse(&self) -> &str {
        &self.warehouse
    }

    /// Get the resource paths.
    pub fn resource_paths(&self) -> &ResourcePaths {
        &self.resource_paths
    }
    /// Get the server address.
    pub fn addr(&self) -> Option<SocketAddr> {
        self.addr
    }

    /// Set ECS metadata role name and token for token loader testing.
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
            get(RESTServer::get_table)
                .post(RESTServer::alter_table)
                .delete(RESTServer::drop_table),
        )
        .route(
            &format!("{prefix}/databases/:db/tables/:table/partitions"),
            get(RESTServer::list_partitions).post(RESTServer::create_partitions),
        )
        .route(
            &format!("{prefix}/databases/:db/tables/:table/partitions/drop"),
            post(RESTServer::drop_partitions),
        )
        .route(
            &format!("{prefix}/databases/:db/tables/:table/partitions/list-by-names"),
            post(RESTServer::list_partitions_by_names),
        )
        .route(
            &format!("{prefix}/databases/:db/tables/:table/partitions/list-by-filter"),
            post(RESTServer::list_partitions_by_filter),
        )
        .route(
            &format!("{prefix}/databases/:db/views"),
            get(RESTServer::list_views).post(RESTServer::create_view),
        )
        .route(
            &format!("{prefix}/databases/:db/views/:view"),
            get(RESTServer::get_view).delete(RESTServer::drop_view),
        )
        .route(
            &format!("{prefix}/databases/:db/functions"),
            get(RESTServer::list_functions).post(RESTServer::create_function),
        )
        .route(
            &format!("{prefix}/databases/:db/functions/:function"),
            get(RESTServer::get_function),
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
