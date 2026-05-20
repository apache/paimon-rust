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

//! SQL context integration tests for paimon-datafusion.

use std::sync::Arc;

use datafusion::catalog::CatalogProvider;
use datafusion::datasource::MemTable;
use paimon::catalog::Identifier;
use paimon::spec::{ArrayType, BlobType, DataType, IntType, MapType, VarCharType};
use paimon::{Catalog, CatalogOptions, FileSystemCatalog, Options};
use paimon_datafusion::{PaimonCatalogProvider, SQLContext};
use tempfile::TempDir;

fn create_test_env() -> (TempDir, Arc<FileSystemCatalog>) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let warehouse = format!("file://{}", temp_dir.path().display());
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse);
    let catalog = FileSystemCatalog::new(options).expect("Failed to create catalog");
    (temp_dir, Arc::new(catalog))
}

async fn create_sql_context(catalog: Arc<FileSystemCatalog>) -> SQLContext {
    let mut ctx = SQLContext::new();
    ctx.register_catalog("paimon", catalog).await.unwrap();
    ctx
}

#[tokio::test]
async fn test_show_tables_is_enabled() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog).await;

    sql_context
        .sql("SHOW TABLES")
        .await
        .expect("SHOW TABLES should be planned when information_schema is enabled")
        .collect()
        .await
        .expect("SHOW TABLES should execute");
}

// ======================= CREATE / DROP SCHEMA =======================

#[tokio::test]
async fn test_create_schema() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    sql_context
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
    let sql_context = create_sql_context(catalog.clone()).await;

    catalog
        .create_database("drop_me", false, Default::default())
        .await
        .unwrap();

    sql_context
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
    let sql_context = create_sql_context(catalog.clone()).await;

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();

    sql_context
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
async fn test_create_table_with_blob_type() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();

    sql_context
        .sql(
            "CREATE TABLE paimon.mydb.assets (
                id INT NOT NULL,
                payload BLOB,
                PRIMARY KEY (id)
            ) WITH ('data-evolution.enabled' = 'true')",
        )
        .await
        .expect("CREATE TABLE with BLOB should succeed");

    let table = catalog
        .get_table(&Identifier::new("mydb", "assets"))
        .await
        .unwrap();
    let schema = table.schema();
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.primary_keys(), &["id"]);
    assert_eq!(
        *schema.fields()[1].data_type(),
        DataType::Blob(BlobType::new())
    );
}

#[tokio::test]
async fn test_create_table_with_partition() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();

    sql_context
        .sql(
            "CREATE TABLE paimon.mydb.events (
                id INT NOT NULL,
                name STRING,
                dt STRING,
                PRIMARY KEY (id, dt)
            ) PARTITIONED BY (dt)
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
async fn test_create_table_partitioned_by_rejects_typed_columns() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();

    let err = sql_context
        .sql(
            "CREATE TABLE paimon.mydb.events (
                id INT NOT NULL,
                dt STRING
            ) PARTITIONED BY (dt STRING)",
        )
        .await
        .expect_err("PARTITIONED BY with typed columns should fail");

    let msg = err.to_string();
    assert!(
        msg.contains("should not specify a type"),
        "unexpected error: {msg}"
    );
}

#[tokio::test]
async fn test_create_table_if_not_exists() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();

    let sql = "CREATE TABLE IF NOT EXISTS paimon.mydb.t1 (
        id INT NOT NULL
    )";

    // First create should succeed
    sql_context
        .sql(sql)
        .await
        .expect("First CREATE should succeed");

    // Second create with IF NOT EXISTS should also succeed
    sql_context
        .sql(sql)
        .await
        .expect("Second CREATE with IF NOT EXISTS should succeed");
}

#[tokio::test]
async fn test_create_external_table_rejected() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();

    let result = sql_context
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
    let sql_context = create_sql_context(catalog.clone()).await;

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();

    sql_context
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
    let sql_context = create_sql_context(catalog.clone()).await;

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();

    sql_context
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
    let sql_context = create_sql_context(catalog.clone()).await;

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

    sql_context
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
    let sql_context = create_sql_context(catalog.clone()).await;

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
    let result = sql_context
        .sql("ALTER TABLE paimon.mydb.alter_test ADD COLUMN age INT")
        .await;

    // FileSystemCatalog does not support AddColumn schema change yet
    assert!(
        result.is_err(),
        "ALTER TABLE ADD COLUMN should fail because AddColumn is not yet supported"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("not yet implemented")
            || err_msg.contains("Unsupported")
            || err_msg.contains("not yet supported"),
        "Error should indicate alter_table is not implemented, got: {err_msg}"
    );
}

#[tokio::test]
async fn test_alter_table_rename() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

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

    sql_context
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
async fn test_ddl_context_delegates_select() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

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
    let df = sql_context
        .sql("SELECT * FROM paimon.mydb.t1")
        .await
        .expect("SELECT should be delegated to DataFusion");

    let batches = df.collect().await.expect("SELECT should execute");
    // Empty table, but should succeed
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0, "Empty table should return 0 rows");
}

// ======================= MULTI-CATALOG =======================

#[tokio::test]
async fn test_multi_catalog_register_and_query() {
    let (_tmp1, catalog1) = create_test_env();
    let (_tmp2, catalog2) = create_test_env();

    let mut ctx = SQLContext::new();
    ctx.register_catalog("cat1", catalog1).await.unwrap();
    ctx.register_catalog("cat2", catalog2).await.unwrap();

    ctx.sql("CREATE SCHEMA cat1.db1").await.unwrap();
    ctx.sql("CREATE SCHEMA cat2.db2").await.unwrap();

    ctx.sql("CREATE TABLE cat1.db1.t (id INT NOT NULL, name STRING, PRIMARY KEY (id))")
        .await
        .unwrap();
    ctx.sql("CREATE TABLE cat2.db2.t (id INT NOT NULL, value STRING, PRIMARY KEY (id))")
        .await
        .unwrap();

    ctx.sql("INSERT INTO cat1.db1.t VALUES (1, 'alice')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    ctx.sql("INSERT INTO cat2.db2.t VALUES (2, 'hello')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = ctx
        .sql("SELECT id, name FROM cat1.db1.t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);

    let batches = ctx
        .sql("SELECT id, value FROM cat2.db2.t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
}

#[tokio::test]
async fn test_set_current_catalog() {
    let (_tmp1, catalog1) = create_test_env();
    let (_tmp2, catalog2) = create_test_env();

    let mut ctx = SQLContext::new();
    ctx.register_catalog("cat1", catalog1).await.unwrap();
    ctx.register_catalog("cat2", catalog2).await.unwrap();

    ctx.sql("CREATE SCHEMA cat1.mydb").await.unwrap();
    ctx.sql("CREATE TABLE cat1.mydb.t (id INT NOT NULL, PRIMARY KEY (id))")
        .await
        .unwrap();

    ctx.set_current_catalog("cat1").await.unwrap();
    ctx.set_current_database("mydb").await.unwrap();

    // Unqualified query should resolve against cat1.mydb
    let df = ctx.sql("SELECT * FROM t").await;
    assert!(
        df.is_ok(),
        "Unqualified table should resolve via current catalog/database"
    );

    // Switching to unknown catalog should fail
    let err = ctx.set_current_catalog("nonexistent").await;
    assert!(err.is_err());
}

#[tokio::test]
async fn test_set_default_catalog_via_datafusion_config() {
    let (_tmp1, catalog1) = create_test_env();
    let (_tmp2, catalog2) = create_test_env();

    let mut ctx = SQLContext::new();
    ctx.register_catalog("cat1", catalog1).await.unwrap();
    ctx.register_catalog("cat2", catalog2).await.unwrap();

    // Create a table in cat2
    ctx.sql("CREATE SCHEMA cat2.mydb").await.unwrap();
    ctx.sql("CREATE TABLE cat2.mydb.t (id INT NOT NULL, name VARCHAR, PRIMARY KEY (id))")
        .await
        .unwrap();
    ctx.sql("INSERT INTO cat2.mydb.t VALUES (1, 'hello')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Switch default catalog via raw DataFusion SET instead of set_current_catalog()
    ctx.sql("SET datafusion.catalog.default_catalog = 'cat2'")
        .await
        .unwrap();
    ctx.set_current_database("mydb").await.unwrap();

    // Unqualified query should now resolve against cat2.mydb
    let batches = ctx
        .sql("SELECT id, name FROM t")
        .await
        .expect("Unqualified table should resolve via DataFusion default_catalog config")
        .collect()
        .await
        .unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);

    // DDL on unqualified name should also go to cat2.
    // CREATE TABLE in mydb should succeed because cat2.mydb exists.
    ctx.sql("CREATE TABLE mydb.t2 (id INT NOT NULL, PRIMARY KEY (id))")
        .await
        .expect("CREATE TABLE should resolve against cat2 after SET default_catalog");

    // Verify the table was created in cat2 by querying with fully qualified name
    let df = ctx.sql("SELECT * FROM cat2.mydb.t2").await;
    assert!(
        df.is_ok(),
        "Table t2 should exist in cat2.mydb after unqualified CREATE TABLE"
    );
}

#[tokio::test]
async fn test_first_registered_catalog_is_default() {
    let (_tmp, catalog) = create_test_env();
    let mut ctx = SQLContext::new();
    ctx.register_catalog("paimon", catalog).await.unwrap();

    ctx.sql("CREATE SCHEMA paimon.mydb").await.unwrap();
    ctx.sql("CREATE TABLE paimon.mydb.t (id INT NOT NULL, PRIMARY KEY (id))")
        .await
        .unwrap();

    ctx.set_current_database("mydb").await.unwrap();

    // Should resolve to paimon.mydb.t without calling set_current_catalog
    let df = ctx.sql("SELECT * FROM t").await;
    assert!(
        df.is_ok(),
        "First registered catalog should be the default for unqualified queries"
    );
}

#[tokio::test]
async fn test_one_part_table_name_uses_current_database() {
    let (_tmp, catalog) = create_test_env();
    let mut ctx = SQLContext::new();
    ctx.register_catalog("paimon", catalog.clone())
        .await
        .unwrap();

    catalog
        .create_database("mydb", false, Default::default())
        .await
        .unwrap();
    ctx.set_current_database("mydb").await.unwrap();

    // 1-part name: "users" should resolve to paimon.mydb.users
    ctx.sql(
        "CREATE TABLE users (
            id INT NOT NULL,
            name STRING,
            PRIMARY KEY (id)
        )",
    )
    .await
    .expect("CREATE TABLE with 1-part name should succeed");

    let tables = catalog.list_tables("mydb").await.unwrap();
    assert!(
        tables.contains(&"users".to_string()),
        "Table should be created in the current database"
    );

    // SELECT with 1-part name should also work
    let df = ctx.sql("SELECT * FROM users").await;
    assert!(
        df.is_ok(),
        "SELECT with 1-part name should resolve correctly"
    );
}

// ======================= TEMP TABLE =======================

use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};

#[tokio::test]
async fn test_register_temp_table_fully_qualified() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;

    let schema = Arc::new(Schema::new(vec![ArrowField::new(
        "id",
        ArrowDataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();

    // Fully qualified: catalog.database.table
    let mem_table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_temp_table("paimon.my_db.my_temp", Arc::new(mem_table))
        .unwrap();

    // Query the temp table via SQL
    let batches = ctx
        .sql("SELECT * FROM paimon.my_db.my_temp")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

#[tokio::test]
async fn test_register_temp_table_database_qualified() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;

    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("name", ArrowDataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec![
                Some("alice"),
                Some("bob"),
                Some("charlie"),
                Some("dave"),
            ])),
        ],
    )
    .unwrap();

    // Database-qualified: database.table (uses current catalog)
    let mem_table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_temp_table("my_db.users", Arc::new(mem_table))
        .unwrap();

    let batches = ctx
        .sql("SELECT id, name FROM paimon.my_db.users WHERE id > 2")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn test_register_temp_table_bare() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;

    // Create a database and set it as current database
    ctx.sql("CREATE DATABASE paimon.my_db").await.unwrap();
    ctx.set_current_database("my_db").await.unwrap();

    let schema = Arc::new(Schema::new(vec![ArrowField::new(
        "id",
        ArrowDataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();

    // Bare: just table name (uses current catalog + current database)
    let mem_table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_temp_table("my_temp", Arc::new(mem_table))
        .unwrap();

    // Query via paimon.my_db.my_temp
    let batches = ctx
        .sql("SELECT * FROM paimon.my_db.my_temp")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

#[tokio::test]
async fn test_register_temp_table_unknown_catalog() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;

    let schema = Arc::new(Schema::new(vec![ArrowField::new(
        "id",
        ArrowDataType::Int32,
        false,
    )]));

    let mem_table = MemTable::try_new(schema, vec![vec![]]).unwrap();
    let result = ctx.register_temp_table("nonexistent.my_db.t", Arc::new(mem_table));
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("Unknown catalog"));
}

#[tokio::test]
async fn test_deregister_temp_table() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;

    let schema = Arc::new(Schema::new(vec![ArrowField::new(
        "id",
        ArrowDataType::Int32,
        false,
    )]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap();

    let mem_table = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();
    ctx.register_temp_table("paimon.my_db.my_temp", Arc::new(mem_table))
        .unwrap();

    // Deregister with flexible name
    ctx.deregister_temp_table("paimon.my_db.my_temp").unwrap();

    // Query should fail
    let result = ctx.sql("SELECT * FROM paimon.my_db.my_temp").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_multiple_temp_tables_in_same_database() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;

    let schema1 = Arc::new(Schema::new(vec![ArrowField::new(
        "id",
        ArrowDataType::Int32,
        false,
    )]));
    let batch1 = RecordBatch::try_new(
        schema1.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2]))],
    )
    .unwrap();

    let schema2 = Arc::new(Schema::new(vec![ArrowField::new(
        "value",
        ArrowDataType::Int32,
        false,
    )]));
    let batch2 = RecordBatch::try_new(
        schema2.clone(),
        vec![Arc::new(Int32Array::from(vec![10, 20, 30]))],
    )
    .unwrap();

    let mem_table = MemTable::try_new(schema1, vec![vec![batch1]]).unwrap();
    ctx.register_temp_table("my_db.t1", Arc::new(mem_table))
        .unwrap();
    let mem_table = MemTable::try_new(schema2, vec![vec![batch2]]).unwrap();
    ctx.register_temp_table("my_db.t2", Arc::new(mem_table))
        .unwrap();

    // Both should be queryable
    let rows1 = ctx
        .sql("SELECT * FROM paimon.my_db.t1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum::<usize>();
    assert_eq!(rows1, 2);

    let rows2 = ctx
        .sql("SELECT * FROM paimon.my_db.t2")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum::<usize>();
    assert_eq!(rows2, 3);
}

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;

#[tokio::test]
async fn test_create_temporary_table_as_select() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;

    // Create a temporary table via SQL
    ctx.sql("CREATE TEMPORARY TABLE paimon.my_db.source AS SELECT * FROM (VALUES (1, 'alice'), (2, 'bob')) AS t(id, name)")
        .await
        .unwrap();

    // Query the temporary table
    let batches = ctx
        .sql("SELECT * FROM paimon.my_db.source ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
}

#[tokio::test]
async fn test_drop_temporary_table() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;

    // Create a temporary table
    ctx.sql("CREATE TEMPORARY TABLE paimon.my_db.source AS SELECT * FROM (VALUES (1, 'alice'), (2, 'bob')) AS t(id, name)")
        .await
        .unwrap();

    // Verify it exists
    let batches = ctx
        .sql("SELECT * FROM paimon.my_db.source ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);

    // Drop it
    ctx.sql("DROP TEMPORARY TABLE paimon.my_db.source")
        .await
        .unwrap();

    // Verify it no longer exists
    let result = ctx.sql("SELECT * FROM paimon.my_db.source").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_drop_temporary_table_if_exists() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;

    // DROP TEMPORARY TABLE on non-existent table with IF EXISTS should succeed
    ctx.sql("DROP TEMPORARY TABLE IF EXISTS paimon.my_db.nonexistent")
        .await
        .unwrap();

    // Without IF EXISTS, it should fail
    let result = ctx
        .sql("DROP TEMPORARY TABLE paimon.my_db.nonexistent")
        .await;
    assert!(result.is_err());
}

// ======================= TEMP VIEW =======================

#[tokio::test]
async fn test_create_temporary_view_fully_qualified() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;

    let schema = Arc::new(Schema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("name", ArrowDataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("alice"), Some("bob")])),
        ],
    )
    .unwrap();
    let mem_table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_temp_table("paimon.my_db.users", Arc::new(mem_table))
        .unwrap();

    ctx.sql("CREATE TEMPORARY VIEW paimon.my_db.my_view AS SELECT * FROM paimon.my_db.users WHERE id > 0")
        .await
        .unwrap();

    let batches = ctx
        .sql("SELECT * FROM paimon.my_db.my_view")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn test_create_temporary_view_database_qualified() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;

    let schema = Arc::new(Schema::new(vec![ArrowField::new(
        "value",
        ArrowDataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![10, 20, 30]))],
    )
    .unwrap();
    let mem_table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_temp_table("paimon.my_db.data", Arc::new(mem_table))
        .unwrap();

    ctx.sql("CREATE TEMPORARY VIEW my_db.summary AS SELECT value FROM paimon.my_db.data WHERE value > 5")
        .await
        .unwrap();

    let batches = ctx
        .sql("SELECT value FROM paimon.my_db.summary WHERE value > 15")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn test_create_temporary_view_bare() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;

    ctx.sql("CREATE DATABASE paimon.my_db").await.unwrap();
    ctx.set_current_database("my_db").await.unwrap();

    let schema = Arc::new(Schema::new(vec![ArrowField::new(
        "id",
        ArrowDataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![100, 200]))],
    )
    .unwrap();
    let mem_table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_temp_table("my_db.source", Arc::new(mem_table))
        .unwrap();

    ctx.sql("CREATE TEMPORARY VIEW my_view AS SELECT id FROM paimon.my_db.source")
        .await
        .unwrap();

    let batches = ctx
        .sql("SELECT * FROM paimon.my_db.my_view")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2);
}

#[tokio::test]
async fn test_drop_temporary_view() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;

    let schema = Arc::new(Schema::new(vec![ArrowField::new(
        "id",
        ArrowDataType::Int32,
        false,
    )]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap();
    let mem_table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_temp_table("paimon.my_db.source", Arc::new(mem_table))
        .unwrap();

    ctx.sql("CREATE TEMPORARY VIEW paimon.my_db.my_view AS SELECT * FROM paimon.my_db.source")
        .await
        .unwrap();

    // Drop via SQL
    ctx.sql("DROP TEMPORARY VIEW paimon.my_db.my_view")
        .await
        .unwrap();

    let result = ctx.sql("SELECT * FROM paimon.my_db.my_view").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_multiple_temporary_views_in_same_database() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;

    let schema1 = Arc::new(Schema::new(vec![ArrowField::new(
        "id",
        ArrowDataType::Int32,
        false,
    )]));
    let batch1 = RecordBatch::try_new(
        schema1.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2]))],
    )
    .unwrap();
    let mem_table = MemTable::try_new(schema1, vec![vec![batch1]]).unwrap();
    ctx.register_temp_table("paimon.my_db.t1", Arc::new(mem_table))
        .unwrap();

    let schema2 = Arc::new(Schema::new(vec![ArrowField::new(
        "name",
        ArrowDataType::Utf8,
        true,
    )]));
    let batch2 = RecordBatch::try_new(
        schema2.clone(),
        vec![Arc::new(StringArray::from(vec![
            Some("x"),
            Some("y"),
            Some("z"),
        ]))],
    )
    .unwrap();
    let mem_table = MemTable::try_new(schema2, vec![vec![batch2]]).unwrap();
    ctx.register_temp_table("paimon.my_db.t2", Arc::new(mem_table))
        .unwrap();

    ctx.sql("CREATE TEMPORARY VIEW my_db.v1 AS SELECT id FROM paimon.my_db.t1")
        .await
        .unwrap();
    ctx.sql("CREATE TEMPORARY VIEW my_db.v2 AS SELECT name FROM paimon.my_db.t2")
        .await
        .unwrap();

    let rows1 = ctx
        .sql("SELECT * FROM paimon.my_db.v1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum::<usize>();
    assert_eq!(rows1, 2);

    let rows2 = ctx
        .sql("SELECT * FROM paimon.my_db.v2")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum::<usize>();
    assert_eq!(rows2, 3);
}
