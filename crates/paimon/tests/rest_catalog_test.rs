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

//! Integration tests for RESTCatalog.
//!
//! These tests use a mock server to verify the RESTCatalog behavior
//! through the Catalog trait interface.

use std::collections::HashMap;

use paimon::api::ConfigResponse;
use paimon::catalog::{Catalog, Identifier, RESTCatalog};
use paimon::common::Options;
use paimon::spec::{BigIntType, DataType, Schema, VarCharType};

mod mock_server;
use mock_server::{start_mock_server, RESTServer};

/// Helper struct to hold test resources.
struct TestContext {
    server: RESTServer,
    catalog: RESTCatalog,
}

/// Helper function to set up a test environment with RESTCatalog.
async fn setup_catalog(initial_dbs: Vec<&str>) -> TestContext {
    let prefix = "mock-test";
    let mut defaults = HashMap::new();
    defaults.insert("prefix".to_string(), prefix.to_string());
    let config = ConfigResponse::new(defaults);

    let initial: Vec<String> = initial_dbs.iter().map(|s| s.to_string()).collect();
    let server = start_mock_server(
        "test_warehouse".to_string(),
        "/tmp/test_warehouse".to_string(),
        config,
        initial,
    )
    .await;

    let url = server.url().expect("Failed to get server URL");
    let mut options = Options::new();
    options.set("uri", &url);
    options.set("warehouse", "test_warehouse");
    options.set("token.provider", "bear");
    options.set("token", "test_token");

    let catalog = RESTCatalog::new(options, true)
        .await
        .expect("Failed to create RESTCatalog");

    TestContext { server, catalog }
}

/// Helper to build a simple test schema.
fn test_schema() -> Schema {
    Schema::builder()
        .column("id", DataType::BigInt(BigIntType::new()))
        .column("name", DataType::VarChar(VarCharType::new(255).unwrap()))
        .build()
        .expect("Failed to build schema")
}

// ==================== Database Tests ====================

#[tokio::test]
async fn test_catalog_list_databases() {
    let ctx = setup_catalog(vec!["default", "test_db1", "prod_db"]).await;

    let dbs = ctx.catalog.list_databases().await.unwrap();

    assert!(dbs.contains(&"default".to_string()));
    assert!(dbs.contains(&"test_db1".to_string()));
    assert!(dbs.contains(&"prod_db".to_string()));
}

#[tokio::test]
async fn test_catalog_create_database() {
    let ctx = setup_catalog(vec!["default"]).await;

    // Create new database
    let result = ctx
        .catalog
        .create_database("new_db", false, HashMap::new())
        .await;
    assert!(result.is_ok(), "failed to create database: {:?}", result);

    // Verify creation
    let dbs = ctx.catalog.list_databases().await.unwrap();
    assert!(dbs.contains(&"new_db".to_string()));
}

#[tokio::test]
async fn test_catalog_create_database_already_exists() {
    let ctx = setup_catalog(vec!["default"]).await;

    // Duplicate creation with ignore_if_exists=false should fail
    let result = ctx
        .catalog
        .create_database("default", false, HashMap::new())
        .await;
    assert!(
        result.is_err(),
        "creating duplicate database should fail when ignore_if_exists=false"
    );
}

#[tokio::test]
async fn test_catalog_create_database_ignore_if_exists() {
    let ctx = setup_catalog(vec!["default"]).await;

    // Duplicate creation with ignore_if_exists=true should succeed
    let result = ctx
        .catalog
        .create_database("default", true, HashMap::new())
        .await;
    assert!(
        result.is_ok(),
        "creating duplicate database should succeed when ignore_if_exists=true"
    );
}

#[tokio::test]
async fn test_catalog_drop_database() {
    let ctx = setup_catalog(vec!["default", "to_drop"]).await;

    // Verify database exists
    let dbs = ctx.catalog.list_databases().await.unwrap();
    assert!(dbs.contains(&"to_drop".to_string()));

    // Drop database (cascade=true to skip empty check)
    let result = ctx.catalog.drop_database("to_drop", false, true).await;
    assert!(result.is_ok(), "failed to drop database: {:?}", result);

    // Verify database is gone
    let dbs = ctx.catalog.list_databases().await.unwrap();
    assert!(!dbs.contains(&"to_drop".to_string()));
}

#[tokio::test]
async fn test_catalog_drop_database_not_exists() {
    let ctx = setup_catalog(vec!["default"]).await;

    // Dropping non-existent database with ignore_if_not_exists=false should fail
    let result = ctx.catalog.drop_database("non_existent", false, true).await;
    assert!(
        result.is_err(),
        "dropping non-existent database should fail when ignore_if_not_exists=false"
    );
}

#[tokio::test]
async fn test_catalog_drop_database_ignore_if_not_exists() {
    let ctx = setup_catalog(vec!["default"]).await;

    // Dropping non-existent database with ignore_if_not_exists=true should succeed
    let result = ctx.catalog.drop_database("non_existent", true, true).await;
    assert!(
        result.is_ok(),
        "dropping non-existent database should succeed when ignore_if_not_exists=true"
    );
}

#[tokio::test]
async fn test_catalog_drop_database_not_empty_no_cascade() {
    let ctx = setup_catalog(vec!["default"]).await;

    // Add a table to the database
    ctx.server.add_table("default", "some_table");

    // Drop database with cascade=false should fail because it's not empty
    let result = ctx.catalog.drop_database("default", false, false).await;
    assert!(
        result.is_err(),
        "dropping non-empty database should fail when cascade=false"
    );
}

#[tokio::test]
async fn test_catalog_drop_database_not_empty_cascade() {
    let ctx = setup_catalog(vec!["default"]).await;

    // Add a table to the database
    ctx.server.add_table("default", "some_table");

    // Drop database with cascade=true should succeed
    let result = ctx.catalog.drop_database("default", false, true).await;
    assert!(
        result.is_ok(),
        "dropping non-empty database should succeed when cascade=true"
    );

    // Verify database is gone
    let dbs = ctx.catalog.list_databases().await.unwrap();
    assert!(!dbs.contains(&"default".to_string()));
}

// ==================== Table Tests ====================

#[tokio::test]
async fn test_catalog_list_tables() {
    let ctx = setup_catalog(vec!["default"]).await;

    // Add tables
    ctx.server.add_table("default", "table1");
    ctx.server.add_table("default", "table2");

    // List tables
    let tables = ctx.catalog.list_tables("default").await.unwrap();
    assert!(tables.contains(&"table1".to_string()));
    assert!(tables.contains(&"table2".to_string()));
}

#[tokio::test]
async fn test_catalog_list_tables_empty() {
    let ctx = setup_catalog(vec!["default"]).await;

    let tables = ctx.catalog.list_tables("default").await.unwrap();
    assert!(
        tables.is_empty(),
        "expected empty tables list, got: {:?}",
        tables
    );
}

#[tokio::test]
async fn test_catalog_get_table() {
    let ctx = setup_catalog(vec!["default"]).await;

    // Add a table with schema and path so get_table can build a Table object
    let schema = test_schema();
    ctx.server.add_table_with_schema(
        "default",
        "my_table",
        schema,
        "file:///tmp/test_warehouse/default.db/my_table",
    );

    let identifier = Identifier::new("default", "my_table");
    let table = ctx.catalog.get_table(&identifier).await;
    assert!(table.is_ok(), "failed to get table: {:?}", table);
}

#[tokio::test]
async fn test_catalog_get_table_not_found() {
    let ctx = setup_catalog(vec!["default"]).await;

    let identifier = Identifier::new("default", "non_existent");
    let result = ctx.catalog.get_table(&identifier).await;
    assert!(result.is_err(), "getting non-existent table should fail");
}

#[tokio::test]
async fn test_catalog_create_table() {
    let ctx = setup_catalog(vec!["default"]).await;

    let schema = test_schema();
    let identifier = Identifier::new("default", "new_table");

    let result = ctx.catalog.create_table(&identifier, schema, false).await;
    assert!(result.is_ok(), "failed to create table: {:?}", result);

    // Verify table exists
    let tables = ctx.catalog.list_tables("default").await.unwrap();
    assert!(tables.contains(&"new_table".to_string()));
}

#[tokio::test]
async fn test_catalog_create_table_already_exists() {
    let ctx = setup_catalog(vec!["default"]).await;

    // Add a table first
    ctx.server.add_table("default", "existing_table");

    let schema = test_schema();
    let identifier = Identifier::new("default", "existing_table");

    // Create with ignore_if_exists=false should fail
    let result = ctx.catalog.create_table(&identifier, schema, false).await;
    assert!(
        result.is_err(),
        "creating duplicate table should fail when ignore_if_exists=false"
    );
}

#[tokio::test]
async fn test_catalog_create_table_ignore_if_exists() {
    let ctx = setup_catalog(vec!["default"]).await;

    // Add a table first
    ctx.server.add_table("default", "existing_table");

    let schema = test_schema();
    let identifier = Identifier::new("default", "existing_table");

    // Create with ignore_if_exists=true should succeed
    let result = ctx.catalog.create_table(&identifier, schema, true).await;
    assert!(
        result.is_ok(),
        "creating duplicate table should succeed when ignore_if_exists=true"
    );
}

#[tokio::test]
async fn test_catalog_drop_table() {
    let ctx = setup_catalog(vec!["default"]).await;

    // Add a table
    ctx.server.add_table("default", "table_to_drop");

    let identifier = Identifier::new("default", "table_to_drop");

    // Drop table
    let result = ctx.catalog.drop_table(&identifier, false).await;
    assert!(result.is_ok(), "failed to drop table: {:?}", result);

    // Verify table is gone
    let tables = ctx.catalog.list_tables("default").await.unwrap();
    assert!(!tables.contains(&"table_to_drop".to_string()));
}

#[tokio::test]
async fn test_catalog_drop_table_not_found() {
    let ctx = setup_catalog(vec!["default"]).await;

    let identifier = Identifier::new("default", "non_existent");

    // Drop with ignore_if_not_exists=false should fail
    let result = ctx.catalog.drop_table(&identifier, false).await;
    assert!(
        result.is_err(),
        "dropping non-existent table should fail when ignore_if_not_exists=false"
    );
}

#[tokio::test]
async fn test_catalog_drop_table_ignore_if_not_exists() {
    let ctx = setup_catalog(vec!["default"]).await;

    let identifier = Identifier::new("default", "non_existent");

    // Drop with ignore_if_not_exists=true should succeed
    let result = ctx.catalog.drop_table(&identifier, true).await;
    assert!(
        result.is_ok(),
        "dropping non-existent table should succeed when ignore_if_not_exists=true"
    );
}

// ==================== Rename Table Tests ====================

#[tokio::test]
async fn test_catalog_rename_table() {
    let ctx = setup_catalog(vec!["default"]).await;

    // Add a table
    ctx.server.add_table("default", "old_table");

    let from = Identifier::new("default", "old_table");
    let to = Identifier::new("default", "new_table");

    // Rename table
    let result = ctx.catalog.rename_table(&from, &to, false).await;
    assert!(result.is_ok(), "failed to rename table: {:?}", result);

    // Verify old table is gone and new table exists
    let tables = ctx.catalog.list_tables("default").await.unwrap();
    assert!(!tables.contains(&"old_table".to_string()));
    assert!(tables.contains(&"new_table".to_string()));
}

#[tokio::test]
async fn test_catalog_rename_table_not_found() {
    let ctx = setup_catalog(vec!["default"]).await;

    let from = Identifier::new("default", "non_existent");
    let to = Identifier::new("default", "new_name");

    // Rename with ignore_if_not_exists=false should fail
    let result = ctx.catalog.rename_table(&from, &to, false).await;
    assert!(
        result.is_err(),
        "renaming non-existent table should fail when ignore_if_not_exists=false"
    );
}

#[tokio::test]
async fn test_catalog_rename_table_ignore_if_not_exists() {
    let ctx = setup_catalog(vec!["default"]).await;

    let from = Identifier::new("default", "non_existent");
    let to = Identifier::new("default", "new_name");

    // Rename with ignore_if_not_exists=true should succeed
    let result = ctx.catalog.rename_table(&from, &to, true).await;
    assert!(
        result.is_ok(),
        "renaming non-existent table should succeed when ignore_if_not_exists=true"
    );
}

// ==================== Alter Table Tests ====================

#[tokio::test]
async fn test_catalog_alter_table_unsupported() {
    let ctx = setup_catalog(vec!["default"]).await;

    let identifier = Identifier::new("default", "some_table");

    // alter_table should return Unsupported error
    let result = ctx.catalog.alter_table(&identifier, vec![], false).await;
    assert!(
        result.is_err(),
        "alter_table should return Unsupported error"
    );
}

// ==================== Multiple Databases Tests ====================

#[tokio::test]
async fn test_catalog_multiple_databases_with_tables() {
    let ctx = setup_catalog(vec!["db1", "db2"]).await;

    // Add tables to different databases
    ctx.server.add_table("db1", "table1_db1");
    ctx.server.add_table("db1", "table2_db1");
    ctx.server.add_table("db2", "table1_db2");

    // Verify db1 tables
    let tables_db1 = ctx.catalog.list_tables("db1").await.unwrap();
    assert_eq!(tables_db1.len(), 2);
    assert!(tables_db1.contains(&"table1_db1".to_string()));
    assert!(tables_db1.contains(&"table2_db1".to_string()));

    // Verify db2 tables
    let tables_db2 = ctx.catalog.list_tables("db2").await.unwrap();
    assert_eq!(tables_db2.len(), 1);
    assert!(tables_db2.contains(&"table1_db2".to_string()));
}
