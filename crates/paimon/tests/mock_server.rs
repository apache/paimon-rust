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
    extract::{Extension, Json, Query},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Router,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;

use paimon::api::{ConfigResponse, ErrorResponse, ListDatabasesResponse, ResourcePaths};

#[derive(Clone, Debug, Default)]
struct MockState {
    databases: HashMap<String, ()>,
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
    /// Create a new RESTServer with initial databases (backward compatibility).
    pub fn new(
        warehouse: String,
        data_path: String,
        config: ConfigResponse,
        initial_dbs: Vec<String>,
    ) -> Self {
        let prefix = config.defaults.get("prefix").cloned().unwrap_or_default();

        // Create database set for initial databases
        let databases: HashMap<String, ()> =
            initial_dbs.into_iter().map(|name| (name, ())).collect();

        RESTServer {
            data_path,
            config,
            warehouse,
            inner: Arc::new(Mutex::new(MockState { databases })),
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
                    Some(format!("Warehouse {} not found", warehouse)),
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

    // ====================== Server Control ====================
    /// Get the warehouse path.
    #[allow(dead_code)]
    pub fn warehouse(&self) -> &str {
        &self.warehouse
    }

    /// Get the resource paths.
    pub fn resource_paths(&self) -> &ResourcePaths {
        &self.resource_paths
    }

    /// Add a database to the server state.
    pub fn add_database(&self, name: &str) {
        let mut s = self.inner.lock().unwrap();
        if !s.databases.contains_key(name) {
            s.databases.insert(name.to_string(), ());
        }
    }

    /// Get the server URL.
    pub fn url(&self) -> Option<String> {
        self.addr.map(|a| format!("http://{}", a))
    }

    /// Get the server address.
    #[allow(dead_code)]
    pub fn addr(&self) -> Option<SocketAddr> {
        self.addr
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
            &format!("{}/databases", prefix),
            get(RESTServer::list_databases),
        )
        .layer(Extension(state));

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind failed");
    let addr = listener.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app.into_make_service()).await {
            eprintln!("mock server error: {}", e);
        }
    });

    server.addr = Some(addr);
    server.server_handle = Some(Arc::new(server_handle));
    server
}
