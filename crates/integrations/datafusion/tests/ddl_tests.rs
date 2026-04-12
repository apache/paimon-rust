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

//! DDL integration tests for paimon-datafusion.

use std::sync::Arc;

use datafusion::catalog::CatalogProvider;
use datafusion::prelude::SessionContext;
use paimon::catalog::Identifier;
use paimon::spec::{ArrayType, DataType, IntType, MapType, VarCharType};
use paimon::{Catalog, CatalogOptions, FileSystemCatalog, Options};
use paimon_datafusion::{PaimonCatalogProvider, PaimonDdlHandler, PaimonRelationPlanner};
use tempfile::TempDir;

fn create_test_env() -> (TempDir, Arc<FileSystemCatalog>) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let warehouse = format!("file://{}", temp_dir.path().display());
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse);
    let catalog = FileSystemCatalog::new(options).expect("Failed to create catalog");
    (temp_dir, Arc::new(catalog))
}

fn create_handler(catalog: Arc<FileSystemCatalog>) -> PaimonDdlHandler {
    let ctx = SessionContext::new();
    ctx.register_catalog(
        "paimon",
        Arc::new(PaimonCatalogProvider::new(catalog.clone())),
    );
    ctx.register_relation_planner(Arc::new(PaimonRelationPlanner::new()))
        .expect("Failed to register relation planner");
    PaimonDdlHandler::new(ctx, catalog, "paimon")
}

// ======================= CREATE / DROP SCHEMA =======================

#[tokio::test]
async fn test_create_schema() {
    let (_tmp, catalog) = create_test_env();
    let handler = create_handler(catalog.clone());

    handler
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA should succeed");

    let databases = catalog.list_databases().await.unwrap();
    assert!(
        databases.contains(&"test_db".to_string()),
        "Database test_db should exist after CREATE SCHEMA"
    );
}

#[tokio::test]
async fn test_drop_schema() {
    let (_tmp, catalog) = create_test_env();
    let handler = create_handler(catalog.clone());

    catalog
        .create_database("drop_me", false, Default::default())
        .await
        .unwrap();

    handler
        .sql("DROP SCHEMA paimon.drop_me CASCADE")
        .await
        .expect("DROP SCHEMA should succeed");

    let databases = catalog.list_databases().await.unwrap();
    assert!(
        !databases.contains(&"drop_me".to_string()),
        "Database drop_me should not exist after DROP SCHEMA"
    );
}

#[tokio::test]
async fn test_schema_names_via_catalog_provider() {
    let (_tmp, catalog) = create_test_env();
    let provider = PaimonCatalogProvider::new(catalog.clone());

    catalog
        .create_database("db_a", false, Default::default())
        .await
        .unwrap();
    catalog
        .create_database("db_b", false, Default::default())
        .await
        .unwrap();

    let names = provider.schema_names();
    assert!(names.contains(&"db_a".to_string()));
    assert!(names.contains(&"db_b".to_string()));
}

// ======================= CREATE TABLE =======================

#[tokio::test]
async fn test_create_table() {
    let (_tmp, catalog) = create_test_env();
    let handler = create_handler(catalog.clone());

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();

    handler
        .sql(
            "CREATE TABLE paimon.mydb.users (
                id INT NOT NULL,
                name STRING,
                age INT,
                PRIMARY KEY (id)
            )",
        )
        .await
        .expect("CREATE TABLE should succeed");

    let tables = catalog.list_tables("mydb").await.unwrap();
    assert!(
        tables.contains(&"users".to_string()),
        "Table users should exist after CREATE TABLE"
    );

    // Verify schema
    let table = catalog
        .get_table(&Identifier::new("mydb", "users"))
        .await
        .unwrap();
    let schema = table.schema();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.primary_keys(), &["id"]);
}

#[tokio::test]
async fn test_create_table_with_partition() {
    let (_tmp, catalog) = create_test_env();
    let handler = create_handler(catalog.clone());

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();

    handler
        .sql(
            "CREATE TABLE paimon.mydb.events (
                id INT NOT NULL,
                name STRING,
                dt STRING,
                PRIMARY KEY (id, dt)
            ) PARTITIONED BY (dt STRING)
            WITH ('bucket' = '2')",
        )
        .await
        .expect("CREATE TABLE with partition should succeed");

    let table = catalog
        .get_table(&Identifier::new("mydb", "events"))
        .await
        .unwrap();
    let schema = table.schema();
    assert_eq!(schema.partition_keys(), &["dt"]);
    assert_eq!(schema.primary_keys(), &["id", "dt"]);
    assert_eq!(
        schema.options().get("bucket"),
        Some(&"2".to_string()),
        "Table option 'bucket' should be preserved"
    );
}

#[tokio::test]
async fn test_create_table_if_not_exists() {
    let (_tmp, catalog) = create_test_env();
    let handler = create_handler(catalog.clone());

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();

    let sql = "CREATE TABLE IF NOT EXISTS paimon.mydb.t1 (
        id INT NOT NULL
    )";

    // First create should succeed
    handler.sql(sql).await.expect("First CREATE should succeed");

    // Second create with IF NOT EXISTS should also succeed
    handler
        .sql(sql)
        .await
        .expect("Second CREATE with IF NOT EXISTS should succeed");
}

#[tokio::test]
async fn test_create_external_table_rejected() {
    let (_tmp, catalog) = create_test_env();
    let handler = create_handler(catalog.clone());

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();

    let result = handler
        .sql(
            "CREATE EXTERNAL TABLE paimon.mydb.bad (
                id INT NOT NULL
            ) STORED AS PARQUET
            LOCATION '/some/path'",
        )
        .await;

    assert!(result.is_err(), "CREATE EXTERNAL TABLE should be rejected");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("CREATE EXTERNAL TABLE is not supported"),
        "Error should mention CREATE EXTERNAL TABLE is not supported, got: {err_msg}"
    );
}

// ======================= CREATE TABLE with complex types =======================

#[tokio::test]
async fn test_create_table_with_array_and_map() {
    let (_tmp, catalog) = create_test_env();
    let handler = create_handler(catalog.clone());

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();

    handler
        .sql(
            "CREATE TABLE paimon.mydb.complex_types (
                id INT NOT NULL,
                tags ARRAY<STRING>,
                props MAP(STRING, INT),
                PRIMARY KEY (id)
            )",
        )
        .await
        .expect("CREATE TABLE with ARRAY and MAP should succeed");

    let table = catalog
        .get_table(&Identifier::new("mydb", "complex_types"))
        .await
        .unwrap();
    let schema = table.schema();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.primary_keys(), &["id"]);

    // Verify ARRAY<STRING> column
    let tags_field = &schema.fields()[1];
    assert_eq!(tags_field.name(), "tags");
    assert_eq!(
        *tags_field.data_type(),
        DataType::Array(ArrayType::new(
            DataType::VarChar(VarCharType::string_type())
        ))
    );

    // Verify MAP(STRING, INT) column
    let props_field = &schema.fields()[2];
    assert_eq!(props_field.name(), "props");
    assert_eq!(
        *props_field.data_type(),
        DataType::Map(MapType::new(
            DataType::VarChar(VarCharType::string_type())
                .copy_with_nullable(false)
                .unwrap(),
            DataType::Int(IntType::new()),
        ))
    );
}

#[tokio::test]
async fn test_create_table_with_row_type() {
    let (_tmp, catalog) = create_test_env();
    let handler = create_handler(catalog.clone());

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();

    handler
        .sql(
            "CREATE TABLE paimon.mydb.row_table (
                id INT NOT NULL,
                address STRUCT<city STRING, zip INT>,
                PRIMARY KEY (id)
            )",
        )
        .await
        .expect("CREATE TABLE with STRUCT should succeed");

    let table = catalog
        .get_table(&Identifier::new("mydb", "row_table"))
        .await
        .unwrap();
    let schema = table.schema();
    assert_eq!(schema.fields().len(), 2);

    // Verify STRUCT<city STRING, zip INT> column
    let address_field = &schema.fields()[1];
    assert_eq!(address_field.name(), "address");
    if let DataType::Row(row) = address_field.data_type() {
        assert_eq!(row.fields().len(), 2);
        assert_eq!(row.fields()[0].name(), "city");
        assert!(matches!(row.fields()[0].data_type(), DataType::VarChar(_)));
        assert_eq!(row.fields()[1].name(), "zip");
        assert!(matches!(row.fields()[1].data_type(), DataType::Int(_)));
    } else {
        panic!("expected Row type for address column");
    }
}

// ======================= DROP TABLE =======================

#[tokio::test]
async fn test_drop_table() {
    let (_tmp, catalog) = create_test_env();
    let handler = create_handler(catalog.clone());

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();

    // Create a table first
    let schema = paimon::spec::Schema::builder()
        .column(
            "id",
            paimon::spec::DataType::Int(paimon::spec::IntType::new()),
        )
        .build()
        .unwrap();
    catalog
        .create_table(&Identifier::new("mydb", "to_drop"), schema, false)
        .await
        .unwrap();

    assert!(catalog
        .list_tables("mydb")
        .await
        .unwrap()
        .contains(&"to_drop".to_string()));

    handler
        .sql("DROP TABLE paimon.mydb.to_drop")
        .await
        .expect("DROP TABLE should succeed");

    assert!(
        !catalog
            .list_tables("mydb")
            .await
            .unwrap()
            .contains(&"to_drop".to_string()),
        "Table should not exist after DROP TABLE"
    );
}

// ======================= ALTER TABLE =======================

#[tokio::test]
async fn test_alter_table_add_column() {
    let (_tmp, catalog) = create_test_env();
    let handler = create_handler(catalog.clone());

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();

    let schema = paimon::spec::Schema::builder()
        .column(
            "id",
            paimon::spec::DataType::Int(paimon::spec::IntType::new()),
        )
        .column(
            "name",
            paimon::spec::DataType::VarChar(paimon::spec::VarCharType::string_type()),
        )
        .build()
        .unwrap();
    catalog
        .create_table(&Identifier::new("mydb", "alter_test"), schema, false)
        .await
        .unwrap();

    // ALTER TABLE is not yet implemented in FileSystemCatalog, so we expect an error
    let result = handler
        .sql("ALTER TABLE paimon.mydb.alter_test ADD COLUMN age INT")
        .await;

    // FileSystemCatalog returns Unsupported for alter_table, which is expected
    assert!(
        result.is_err(),
        "ALTER TABLE should fail because FileSystemCatalog does not implement alter_table yet"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("not yet implemented") || err_msg.contains("Unsupported"),
        "Error should indicate alter_table is not implemented, got: {err_msg}"
    );
}

#[tokio::test]
async fn test_alter_table_rename() {
    let (_tmp, catalog) = create_test_env();
    let handler = create_handler(catalog.clone());

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();

    let schema = paimon::spec::Schema::builder()
        .column(
            "id",
            paimon::spec::DataType::Int(paimon::spec::IntType::new()),
        )
        .build()
        .unwrap();
    catalog
        .create_table(&Identifier::new("mydb", "old_name"), schema, false)
        .await
        .unwrap();

    handler
        .sql("ALTER TABLE mydb.old_name RENAME TO new_name")
        .await
        .expect("ALTER TABLE RENAME should succeed");

    let tables = catalog.list_tables("mydb").await.unwrap();
    assert!(
        !tables.contains(&"old_name".to_string()),
        "old_name should not exist after rename"
    );
    assert!(
        tables.contains(&"new_name".to_string()),
        "new_name should exist after rename"
    );
}

#[tokio::test]
async fn test_ddl_handler_delegates_select() {
    let (_tmp, catalog) = create_test_env();
    let handler = create_handler(catalog.clone());

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();

    let schema = paimon::spec::Schema::builder()
        .column(
            "id",
            paimon::spec::DataType::Int(paimon::spec::IntType::new()),
        )
        .build()
        .unwrap();
    catalog
        .create_table(&Identifier::new("mydb", "t1"), schema, false)
        .await
        .unwrap();

    // SELECT should be delegated to DataFusion
    let df = handler
        .sql("SELECT * FROM paimon.mydb.t1")
        .await
        .expect("SELECT should be delegated to DataFusion");

    let batches = df.collect().await.expect("SELECT should execute");
    // Empty table, but should succeed
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0, "Empty table should return 0 rows");
}
