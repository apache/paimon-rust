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
//! Both the mock server and API client run asynchronously using tokio.

use std::collections::HashMap;

use paimon::api::auth::{DLFECSTokenLoader, DLFToken, DLFTokenLoader};
use paimon::api::rest_api::RESTApi;
use paimon::api::ConfigResponse;
use paimon::catalog::Identifier;
use paimon::common::Options;
use serde_json::json;

mod mock_server;
use mock_server::{start_mock_server, RESTServer};
/// Helper struct to hold test resources.
struct TestContext {
    server: RESTServer,
    api: RESTApi,
    url: String,
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
        "test_warehouse".to_string(),      // warehouse
        "/tmp/test_warehouse".to_string(), // data_path
        config,
        initial,
    )
    .await;
    let token = "test_token";
    let url = server.url().expect("Failed to get server URL");
    let mut options = Options::new();
    options.set("uri", &url);
    options.set("warehouse", "test_warehouse");
    options.set("token.provider", "bear");
    options.set("token", token);

    let api = RESTApi::new(options, true)
        .await
        .expect("Failed to create RESTApi");

    TestContext { server, api, url }
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
async fn test_create_database() {
    let ctx = setup_test_server(vec!["default"]).await;

    // Create new database
    let result = ctx.api.create_database("new_db", None).await;
    assert!(result.is_ok(), "failed to create database: {result:?}");

    // Verify creation
    let dbs = ctx.api.list_databases().await.unwrap();
    assert!(dbs.contains(&"new_db".to_string()));

    // Duplicate creation should fail
    let result = ctx.api.create_database("new_db", None).await;
    assert!(result.is_err(), "creating duplicate database should fail");
}

#[tokio::test]
async fn test_get_database() {
    let ctx = setup_test_server(vec!["default"]).await;

    let db_resp = ctx.api.get_database("default").await.unwrap();
    assert_eq!(db_resp.name, Some("default".to_string()));
}

#[tokio::test]
async fn test_error_responses_status_mapping() {
    let ctx = setup_test_server(vec!["default"]).await;

    // Add no-permission database
    ctx.server.add_no_permission_database("secret");

    // GET on no-permission database -> 403
    // Use the prefix from config (v1/mock-test)
    let url = format!("{}/v1/mock-test/databases/{}", ctx.url, "secret");
    let result = reqwest::get(&url).await;
    match result {
        Ok(resp) => {
            assert_eq!(resp.status(), 403);
            let j: serde_json::Value = resp.json().await.unwrap();
            assert_eq!(
                j.get("resourceType").and_then(|v| v.as_str()),
                Some("database")
            );
            assert_eq!(
                j.get("resourceName").and_then(|v| v.as_str()),
                Some("secret")
            );
            assert_eq!(j.get("code").and_then(|v| v.as_u64()), Some(403));
        }
        Err(e) => panic!("Expected 403 response, got error: {e:?}"),
    }

    // POST create existing database -> 409
    let body = json!({"name": "default", "properties": {}});
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/v1/mock-test/databases", ctx.url))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409);

    let j2: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        j2.get("resourceType").and_then(|v| v.as_str()),
        Some("database")
    );
    assert_eq!(
        j2.get("resourceName").and_then(|v| v.as_str()),
        Some("default")
    );
    assert_eq!(j2.get("code").and_then(|v| v.as_u64()), Some(409));
}

#[tokio::test]
async fn test_alter_database() {
    let ctx = setup_test_server(vec!["default"]).await;

    // Alter database with updates
    let mut updates = HashMap::new();
    updates.insert("key1".to_string(), "value1".to_string());
    updates.insert("key2".to_string(), "value2".to_string());

    let result = ctx.api.alter_database("default", vec![], updates).await;
    assert!(result.is_ok(), "failed to alter database: {result:?}");

    // Verify the updates by getting the database
    let db_resp = ctx.api.get_database("default").await.unwrap();
    assert_eq!(db_resp.options.get("key1"), Some(&"value1".to_string()));
    assert_eq!(db_resp.options.get("key2"), Some(&"value2".to_string()));

    // Alter database with removals
    let result = ctx
        .api
        .alter_database("default", vec!["key1".to_string()], HashMap::new())
        .await;
    assert!(result.is_ok(), "failed to remove key: {result:?}");

    let db_resp = ctx.api.get_database("default").await.unwrap();
    assert!(!db_resp.options.contains_key("key1"));
    assert_eq!(db_resp.options.get("key2"), Some(&"value2".to_string()));
}

#[tokio::test]
async fn test_alter_database_not_found() {
    let ctx = setup_test_server(vec!["default"]).await;

    let result = ctx
        .api
        .alter_database("non_existent", vec![], HashMap::new())
        .await;
    assert!(
        result.is_err(),
        "altering non-existent database should fail"
    );
}

#[tokio::test]
async fn test_drop_database() {
    let ctx = setup_test_server(vec!["default", "to_drop"]).await;

    // Verify database exists
    let dbs = ctx.api.list_databases().await.unwrap();
    assert!(dbs.contains(&"to_drop".to_string()));

    // Drop database
    let result = ctx.api.drop_database("to_drop").await;
    assert!(result.is_ok(), "failed to drop database: {result:?}");

    // Verify database is gone
    let dbs = ctx.api.list_databases().await.unwrap();
    assert!(!dbs.contains(&"to_drop".to_string()));

    // Dropping non-existent database should fail
    let result = ctx.api.drop_database("to_drop").await;
    assert!(
        result.is_err(),
        "dropping non-existent database should fail"
    );
}

#[tokio::test]
async fn test_drop_database_no_permission() {
    let ctx = setup_test_server(vec!["default"]).await;
    ctx.server.add_no_permission_database("secret");

    let result = ctx.api.drop_database("secret").await;
    assert!(
        result.is_err(),
        "dropping no-permission database should fail"
    );
}
// ==================== Table Tests ====================

#[tokio::test]
async fn test_list_tables_and_get_table() {
    let ctx = setup_test_server(vec!["default"]).await;

    // Add tables
    ctx.server.add_table("default", "table1");
    ctx.server.add_table("default", "table2");

    // List tables
    let tables = ctx.api.list_tables("default").await.unwrap();
    assert!(tables.contains(&"table1".to_string()));
    assert!(tables.contains(&"table2".to_string()));

    // Get table
    let table_resp = ctx
        .api
        .get_table(&Identifier::new("default", "table1"))
        .await
        .unwrap();
    assert_eq!(table_resp.id.unwrap_or_default(), "table1");
}

#[tokio::test]
async fn test_get_table_not_found() {
    let ctx = setup_test_server(vec!["default"]).await;

    let result = ctx
        .api
        .get_table(&Identifier::new("default", "non_existent_table"))
        .await;
    assert!(result.is_err(), "getting non-existent table should fail");
}

#[tokio::test]
async fn test_list_tables_empty_database() {
    let ctx = setup_test_server(vec!["default"]).await;

    let tables = ctx.api.list_tables("default").await.unwrap();
    assert!(
        tables.is_empty(),
        "expected empty tables list, got: {tables:?}"
    );
}

#[tokio::test]
async fn test_multiple_databases_with_tables() {
    let ctx = setup_test_server(vec!["db1", "db2"]).await;

    // Add tables to different databases
    ctx.server.add_table("db1", "table1_db1");
    ctx.server.add_table("db1", "table2_db1");
    ctx.server.add_table("db2", "table1_db2");

    // Verify db1 tables
    let tables_db1 = ctx.api.list_tables("db1").await.unwrap();
    assert_eq!(tables_db1.len(), 2);
    assert!(tables_db1.contains(&"table1_db1".to_string()));
    assert!(tables_db1.contains(&"table2_db1".to_string()));

    // Verify db2 tables
    let tables_db2 = ctx.api.list_tables("db2").await.unwrap();
    assert_eq!(tables_db2.len(), 1);
    assert!(tables_db2.contains(&"table1_db2".to_string()));
}

#[tokio::test]
async fn test_create_table() {
    let ctx = setup_test_server(vec!["default"]).await;

    // Create a simple schema using builder
    use paimon::spec::{DataType, Schema};
    let schema = Schema::builder()
        .column("id", DataType::BigInt(paimon::spec::BigIntType::new()))
        .column(
            "name",
            DataType::VarChar(paimon::spec::VarCharType::new(255).unwrap()),
        )
        .build()
        .expect("Failed to build schema");

    let result = ctx
        .api
        .create_table(&Identifier::new("default", "new_table"), schema)
        .await;
    assert!(result.is_ok(), "failed to create table: {result:?}");

    // Verify table exists
    let tables = ctx.api.list_tables("default").await.unwrap();
    assert!(tables.contains(&"new_table".to_string()));

    // Get the table
    let table_resp = ctx
        .api
        .get_table(&Identifier::new("default", "new_table"))
        .await
        .unwrap();
    assert_eq!(table_resp.name, Some("new_table".to_string()));
}

#[tokio::test]
async fn test_drop_table() {
    let ctx = setup_test_server(vec!["default"]).await;

    // Add a table
    ctx.server.add_table("default", "table_to_drop");

    // Verify table exists
    let tables = ctx.api.list_tables("default").await.unwrap();
    assert!(tables.contains(&"table_to_drop".to_string()));

    // Drop table
    let result = ctx
        .api
        .drop_table(&Identifier::new("default", "table_to_drop"))
        .await;
    assert!(result.is_ok(), "failed to drop table: {result:?}");

    // Verify table is gone
    let tables = ctx.api.list_tables("default").await.unwrap();
    assert!(!tables.contains(&"table_to_drop".to_string()));

    // Dropping non-existent table should fail
    let result = ctx
        .api
        .drop_table(&Identifier::new("default", "table_to_drop"))
        .await;
    assert!(result.is_err(), "dropping non-existent table should fail");
}

#[tokio::test]
async fn test_drop_table_no_permission() {
    let ctx = setup_test_server(vec!["default"]).await;
    ctx.server
        .add_no_permission_table("default", "secret_table");

    let result = ctx
        .api
        .drop_table(&Identifier::new("default", "secret_table"))
        .await;
    assert!(result.is_err(), "dropping no-permission table should fail");
}

// ==================== Rename Table Tests ====================

#[tokio::test]
async fn test_rename_table() {
    let ctx = setup_test_server(vec!["default"]).await;

    // Add a table
    ctx.server.add_table("default", "old_table");

    // Rename table
    let result = ctx
        .api
        .rename_table(
            &Identifier::new("default", "old_table"),
            &Identifier::new("default", "new_table"),
        )
        .await;
    assert!(result.is_ok(), "failed to rename table: {result:?}");

    // Verify old table is gone
    let tables = ctx.api.list_tables("default").await.unwrap();
    assert!(!tables.contains(&"old_table".to_string()));

    // Verify new table exists
    assert!(tables.contains(&"new_table".to_string()));

    // Get the renamed table
    let table_resp = ctx
        .api
        .get_table(&Identifier::new("default", "new_table"))
        .await
        .unwrap();
    assert_eq!(table_resp.name, Some("new_table".to_string()));
}

// ==================== Token Loader Tests ====================

#[tokio::test]
async fn test_ecs_loader_token() {
    let prefix = "mock-test";
    let mut defaults = HashMap::new();
    defaults.insert("prefix".to_string(), prefix.to_string());
    let config = ConfigResponse::new(defaults);

    let initial: Vec<String> = vec!["default".to_string()];
    let server = start_mock_server(
        "test_warehouse".to_string(),
        "/tmp/test_warehouse".to_string(),
        config,
        initial,
    )
    .await;

    let role_name = "test_role";
    let token_json = json!({
        "AccessKeyId": "AccessKeyId",
        "AccessKeySecret": "AccessKeySecret",
        "SecurityToken": "AQoDYXdzEJr...<remainder of security token>",
        "Expiration": "2023-12-01T12:00:00Z"
    });

    server.set_ecs_metadata(role_name, token_json.clone());

    let ecs_metadata_url = format!("{}/ram/security-credentials/", server.url().unwrap());

    // Test without role name
    let loader = DLFECSTokenLoader::new(&ecs_metadata_url, None);
    let load_token: DLFToken = loader.load_token().await.unwrap();

    assert_eq!(load_token.access_key_id, "AccessKeyId");
    assert_eq!(load_token.access_key_secret, "AccessKeySecret");
    assert_eq!(
        load_token.security_token,
        Some("AQoDYXdzEJr...<remainder of security token>".to_string())
    );
    assert_eq!(
        load_token.expiration,
        Some("2023-12-01T12:00:00Z".to_string())
    );

    // Test with role name
    let loader_with_role = DLFECSTokenLoader::new(&ecs_metadata_url, Some(role_name.to_string()));
    let token: DLFToken = loader_with_role.load_token().await.unwrap();

    assert_eq!(token.access_key_id, "AccessKeyId");
    assert_eq!(token.access_key_secret, "AccessKeySecret");
    assert_eq!(
        token.security_token,
        Some("AQoDYXdzEJr...<remainder of security token>".to_string())
    );
    assert_eq!(token.expiration, Some("2023-12-01T12:00:00Z".to_string()));
}
