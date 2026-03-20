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

//! Integration tests for REST API.
//!
//! These tests use a mock server to verify the REST API client behavior.

use std::collections::HashMap;

use paimon::api::rest_api::RESTApi;
use paimon::api::ConfigResponse;
use paimon::common::Options;

mod mock_server;
use mock_server::{start_mock_server, RESTServer};

/// Helper struct to hold test resources.
struct TestContext {
    server: RESTServer,
    api: RESTApi,
}

/// Helper function to set up a test environment with a custom prefix.
async fn setup_test_server(initial_dbs: Vec<&str>) -> TestContext {
    let prefix = "mock-test";
    // Create config with prefix
    let mut defaults = HashMap::new();
    defaults.insert("prefix".to_string(), prefix.to_string());
    let config = ConfigResponse::new(defaults);

    let initial: Vec<String> = initial_dbs.iter().map(|s| s.to_string()).collect();
    // Start server with config
    let server = start_mock_server(
        "test_warehouse".to_string(),
        "/tmp/test_warehouse".to_string(),
        config,
        initial,
    )
    .await;
    let token = "test_token";
    let url = server.url().expect("server url");
    let mut options = Options::new();
    options.set("uri", &url);
    options.set("warehouse", "test_warehouse");
    options.set("token.provider", "bear");
    options.set("token", token);

    let api = RESTApi::new(options, true)
        .await
        .expect("Failed to create RESTApi");

    TestContext { server, api }
}

// ==================== Database Tests ====================

#[tokio::test]
async fn test_list_databases() {
    let ctx = setup_test_server(vec!["default", "test_db1", "prod_db"]).await;

    let dbs = ctx.api.list_databases().await.unwrap();

    assert!(dbs.contains(&"default".to_string()));
    assert!(dbs.contains(&"test_db1".to_string()));
    assert!(dbs.contains(&"prod_db".to_string()));
}

#[tokio::test]
async fn test_list_databases_empty() {
    let ctx = setup_test_server(vec![]).await;

    let dbs = ctx.api.list_databases().await.unwrap();
    assert!(dbs.is_empty());
}

#[tokio::test]
async fn test_list_databases_add_after_creation() {
    let ctx = setup_test_server(vec!["default"]).await;

    // Add a new database after server creation
    ctx.server.add_database("new_db");

    let dbs = ctx.api.list_databases().await.unwrap();
    assert!(dbs.contains(&"default".to_string()));
    assert!(dbs.contains(&"new_db".to_string()));
}
