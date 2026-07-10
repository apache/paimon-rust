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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::{Array, Int64Array};
use datafusion::catalog::CatalogProvider;
use datafusion::datasource::MemTable;
use paimon::catalog::{list_partitions_from_file_system, Identifier};
use paimon::spec::{
    ArrayType, BinaryType, BlobType, CharType, DataType, FloatType, IntType,
    LocalZonedTimestampType, MapType, MultisetType, SchemaChange, TimeType, VarBinaryType,
    VarCharType, VectorType,
};
use paimon::table::{BranchManager, SnapshotManager, TagManager};
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

struct PartitionCatalog {
    inner: Arc<FileSystemCatalog>,
    fail_list_partitions: AtomicBool,
    partition_identifiers: Mutex<Vec<Identifier>>,
}

impl PartitionCatalog {
    fn new(inner: Arc<FileSystemCatalog>) -> Self {
        Self {
            inner,
            fail_list_partitions: AtomicBool::new(false),
            partition_identifiers: Mutex::new(Vec::new()),
        }
    }

    fn set_fail_list_partitions(&self, fail: bool) {
        self.fail_list_partitions.store(fail, Ordering::SeqCst);
    }

    fn take_partition_identifiers(&self) -> Vec<Identifier> {
        std::mem::take(&mut *self.partition_identifiers.lock().unwrap())
    }
}

#[async_trait]
impl Catalog for PartitionCatalog {
    async fn list_databases(&self) -> paimon::Result<Vec<String>> {
        self.inner.list_databases().await
    }

    async fn create_database(
        &self,
        name: &str,
        ignore_if_exists: bool,
        properties: std::collections::HashMap<String, String>,
    ) -> paimon::Result<()> {
        self.inner
            .create_database(name, ignore_if_exists, properties)
            .await
    }

    async fn get_database(&self, name: &str) -> paimon::Result<paimon::catalog::Database> {
        self.inner.get_database(name).await
    }

    async fn drop_database(
        &self,
        name: &str,
        ignore_if_not_exists: bool,
        cascade: bool,
    ) -> paimon::Result<()> {
        self.inner
            .drop_database(name, ignore_if_not_exists, cascade)
            .await
    }

    async fn get_table(&self, identifier: &Identifier) -> paimon::Result<paimon::table::Table> {
        self.inner.get_table(identifier).await
    }

    async fn list_tables(&self, database_name: &str) -> paimon::Result<Vec<String>> {
        self.inner.list_tables(database_name).await
    }

    async fn create_table(
        &self,
        identifier: &Identifier,
        creation: paimon::spec::Schema,
        ignore_if_exists: bool,
    ) -> paimon::Result<()> {
        self.inner
            .create_table(identifier, creation, ignore_if_exists)
            .await
    }

    async fn drop_table(
        &self,
        identifier: &Identifier,
        ignore_if_not_exists: bool,
    ) -> paimon::Result<()> {
        self.inner
            .drop_table(identifier, ignore_if_not_exists)
            .await
    }

    async fn rename_table(
        &self,
        from: &Identifier,
        to: &Identifier,
        ignore_if_not_exists: bool,
    ) -> paimon::Result<()> {
        self.inner
            .rename_table(from, to, ignore_if_not_exists)
            .await
    }

    async fn alter_table(
        &self,
        identifier: &Identifier,
        changes: Vec<SchemaChange>,
        ignore_if_not_exists: bool,
    ) -> paimon::Result<()> {
        self.inner
            .alter_table(identifier, changes, ignore_if_not_exists)
            .await
    }

    async fn list_partitions(
        &self,
        identifier: &Identifier,
    ) -> paimon::Result<Vec<paimon::spec::Partition>> {
        self.partition_identifiers
            .lock()
            .unwrap()
            .push(identifier.clone());

        let Some(branch) = identifier.branch_name()? else {
            return self.inner.list_partitions(identifier).await;
        };
        if self.fail_list_partitions.load(Ordering::SeqCst) {
            return Err(paimon::Error::Unsupported {
                message: "injected list_partitions failure".to_string(),
            });
        }

        let base = Identifier::new(identifier.database(), identifier.table_name()?);
        let table = self.inner.get_table(&base).await?;
        let table = table.copy_with_branch(&branch).await?;
        let mut partitions = list_partitions_from_file_system(&table).await?;
        for partition in &mut partitions {
            partition.created_by = Some("catalog".to_string());
        }
        Ok(partitions)
    }
}

async fn collect_ids(sql_context: &SQLContext, sql: &str) -> Vec<i32> {
    let batches = sql_context.sql(sql).await.unwrap().collect().await.unwrap();
    let mut ids = Vec::new();
    for batch in batches {
        let id_array = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("id column");
        for row in 0..batch.num_rows() {
            ids.push(id_array.value(row));
        }
    }
    ids.sort_unstable();
    ids
}

async fn collect_i64_column(sql_context: &SQLContext, sql: &str, column: &str) -> Vec<i64> {
    let batches = sql_context.sql(sql).await.unwrap().collect().await.unwrap();
    let mut values = Vec::new();
    for batch in batches {
        let array = batch
            .column_by_name(column)
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .expect(column);
        for row in 0..batch.num_rows() {
            values.push(array.value(row));
        }
    }
    values.sort_unstable();
    values
}

async fn collect_string_column(sql_context: &SQLContext, sql: &str, column: &str) -> Vec<String> {
    let batches = sql_context.sql(sql).await.unwrap().collect().await.unwrap();
    let mut values = Vec::new();
    for batch in batches {
        let array = batch
            .column_by_name(column)
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect(column);
        for row in 0..batch.num_rows() {
            if !array.is_null(row) {
                values.push(array.value(row).to_string());
            }
        }
    }
    values.sort_unstable();
    values
}

async fn assert_sql_error_contains(sql_context: &SQLContext, sql: &str, expected: &str) {
    let err = match sql_context.sql(sql).await {
        Ok(df) => df
            .collect()
            .await
            .expect_err("SQL should fail but succeeded")
            .to_string(),
        Err(err) => err.to_string(),
    };
    assert!(
        err.contains(expected),
        "expected error containing '{expected}', got: {err}"
    );
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

#[tokio::test]
async fn test_select_branch_table_reads_branch_snapshot() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    sql_context
        .sql("CREATE TABLE paimon.default.branch_orders (id INT, name STRING)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    sql_context
        .sql("INSERT INTO paimon.default.branch_orders VALUES (1, 'branch')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let identifier = Identifier::new("default", "branch_orders");
    let table = catalog.get_table(&identifier).await.unwrap();
    let snapshot_manager =
        SnapshotManager::new(table.file_io().clone(), table.location().to_string());
    let snapshot = snapshot_manager
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    let tag_manager = TagManager::new(table.file_io().clone(), table.location().to_string());
    tag_manager.create("branch_base", &snapshot).await.unwrap();
    let branch_manager = BranchManager::new(table.file_io().clone(), table.location().to_string());
    branch_manager
        .create_branch_from_tag("b1", "branch_base")
        .await
        .unwrap();

    sql_context
        .sql("INSERT INTO paimon.default.branch_orders VALUES (2, 'main')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(
        collect_ids(&sql_context, "SELECT id FROM paimon.default.branch_orders").await,
        vec![1, 2]
    );
    assert_eq!(
        collect_ids(
            &sql_context,
            "SELECT id FROM paimon.default.branch_orders$branch_b1"
        )
        .await,
        vec![1]
    );
    assert_eq!(
        collect_i64_column(
            &sql_context,
            "SELECT snapshot_id FROM paimon.default.branch_orders$snapshots",
            "snapshot_id"
        )
        .await,
        vec![1, 2]
    );
    assert_eq!(
        collect_i64_column(
            &sql_context,
            "SELECT snapshot_id FROM paimon.default.branch_orders$branch_b1$snapshots",
            "snapshot_id"
        )
        .await,
        vec![1]
    );
    assert_eq!(
        collect_i64_column(
            &sql_context,
            "SELECT record_count FROM paimon.default.branch_orders$files VERSION AS OF 'branch_base'",
            "record_count"
        )
        .await,
        vec![1]
    );
    assert_eq!(
        collect_i64_column(
            &sql_context,
            "SELECT record_count FROM paimon.default.branch_orders$branch_b1$files VERSION AS OF 'branch_base'",
            "record_count"
        )
        .await,
        vec![1]
    );
    assert!(!collect_string_column(
        &sql_context,
        "SELECT file_name FROM paimon.default.branch_orders$branch_b1$manifests",
        "file_name",
    )
    .await
    .is_empty());

    let branch_table = table.copy_with_branch("b1").await.unwrap();
    let write_builder = branch_table.new_write_builder();
    assert!(write_builder.new_write().is_err());
    assert!(write_builder.new_update(vec!["name".to_string()]).is_err());
    assert!(write_builder.new_delete().is_err());
    assert!(write_builder.try_new_commit().is_err());

    assert_sql_error_contains(
        &sql_context,
        "INSERT INTO paimon.default.branch_orders$branch_b1 VALUES (3, 'blocked')",
        "Writing to Paimon branch 'b1' is not supported",
    )
    .await;
    assert_sql_error_contains(
        &sql_context,
        "INSERT INTO paimon.default.branch_orders$branch_main VALUES (3, 'blocked')",
        "Writing to Paimon branch 'main' is not supported",
    )
    .await;
    assert_sql_error_contains(
        &sql_context,
        "UPDATE paimon.default.branch_orders$branch_b1 SET name = 'blocked' WHERE id = 1",
        "UPDATE on Paimon branch 'b1' is not supported",
    )
    .await;
    assert_sql_error_contains(
        &sql_context,
        "UPDATE paimon.default.branch_orders$branch_main SET name = 'blocked' WHERE id = 1",
        "UPDATE on Paimon branch 'main' is not supported",
    )
    .await;
    assert_sql_error_contains(
        &sql_context,
        "DELETE FROM paimon.default.branch_orders$branch_b1 WHERE id = 1",
        "DELETE on Paimon branch 'b1' is not supported",
    )
    .await;
    assert_sql_error_contains(
        &sql_context,
        "DELETE FROM paimon.default.branch_orders$branch_main WHERE id = 1",
        "DELETE on Paimon branch 'main' is not supported",
    )
    .await;
    assert_sql_error_contains(
        &sql_context,
        "MERGE INTO paimon.default.branch_orders$branch_main AS target \
         USING (SELECT 1 AS id, 'blocked' AS name) AS source \
         ON target.id = source.id \
         WHEN MATCHED THEN UPDATE SET name = source.name",
        "MERGE INTO on Paimon branch 'main' is not supported",
    )
    .await;
    assert_sql_error_contains(
        &sql_context,
        "TRUNCATE TABLE paimon.default.branch_orders$branch_main",
        "TRUNCATE TABLE on Paimon branch 'main' is not supported",
    )
    .await;
    assert_sql_error_contains(
        &sql_context,
        "ALTER TABLE paimon.default.branch_orders$branch_main ADD COLUMN blocked INT",
        "ALTER TABLE on Paimon branch 'main' is not supported",
    )
    .await;
    assert_sql_error_contains(
        &sql_context,
        "INSERT OVERWRITE paimon.default.branch_orders$branch_main \
         PARTITION (id = 1) SELECT 'blocked'",
        "INSERT OVERWRITE on Paimon branch 'main' is not supported",
    )
    .await;
}

#[tokio::test]
async fn test_branch_partitions_system_table_reads_branch_snapshot() {
    let (_tmp, file_catalog) = create_test_env();
    let catalog = Arc::new(PartitionCatalog::new(file_catalog.clone()));
    let mut sql_context = SQLContext::new();
    sql_context
        .register_catalog("paimon", catalog.clone())
        .await
        .unwrap();

    sql_context
        .sql(
            "CREATE TABLE paimon.default.branch_partition_orders \
             (id INT, name STRING) PARTITIONED BY (id)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    sql_context
        .sql("INSERT INTO paimon.default.branch_partition_orders VALUES (1, 'branch')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let identifier = Identifier::new("default", "branch_partition_orders");
    let table = file_catalog.get_table(&identifier).await.unwrap();
    let snapshot_manager =
        SnapshotManager::new(table.file_io().clone(), table.location().to_string());
    let snapshot = snapshot_manager
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    let tag_manager = TagManager::new(table.file_io().clone(), table.location().to_string());
    tag_manager
        .create("partition_branch_base", &snapshot)
        .await
        .unwrap();
    let branch_manager = BranchManager::new(table.file_io().clone(), table.location().to_string());
    branch_manager
        .create_branch_from_tag("b1", "partition_branch_base")
        .await
        .unwrap();

    sql_context
        .sql("INSERT INTO paimon.default.branch_partition_orders VALUES (2, 'main')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(
        collect_string_column(
            &sql_context,
            "SELECT \"partition\" FROM paimon.default.branch_partition_orders$partitions",
            "partition",
        )
        .await,
        vec!["id=1".to_string(), "id=2".to_string()]
    );
    catalog.take_partition_identifiers();

    assert_eq!(
        collect_string_column(
            &sql_context,
            "SELECT created_by FROM paimon.default.branch_partition_orders$branch_b1$partitions",
            "created_by",
        )
        .await,
        vec!["catalog".to_string()]
    );
    assert_eq!(
        catalog.take_partition_identifiers(),
        vec![Identifier::new(
            "default",
            "branch_partition_orders$branch_b1"
        )]
    );

    assert_eq!(
        collect_string_column(
            &sql_context,
            "SELECT \"partition\" FROM paimon.default.branch_partition_orders$branch_main$partitions",
            "partition",
        )
        .await,
        vec!["id=1".to_string(), "id=2".to_string()]
    );
    assert_eq!(
        catalog.take_partition_identifiers(),
        vec![Identifier::new("default", "branch_partition_orders")]
    );

    catalog.set_fail_list_partitions(true);
    assert_eq!(
        collect_string_column(
            &sql_context,
            "SELECT \"partition\" FROM paimon.default.branch_partition_orders$branch_b1$partitions",
            "partition",
        )
        .await,
        vec!["id=1".to_string()]
    );
    assert_eq!(
        catalog.take_partition_identifiers(),
        vec![Identifier::new(
            "default",
            "branch_partition_orders$branch_b1"
        )]
    );

    assert_eq!(
        collect_string_column(
            &sql_context,
            "SELECT \"partition\" FROM paimon.default.branch_partition_orders$partitions \
             VERSION AS OF 'partition_branch_base'",
            "partition",
        )
        .await,
        vec!["id=1".to_string()]
    );
    assert_eq!(
        collect_string_column(
            &sql_context,
            "SELECT \"partition\" FROM paimon.default.branch_partition_orders$branch_b1$partitions \
             VERSION AS OF 'partition_branch_base'",
            "partition",
        )
        .await,
        vec!["id=1".to_string()]
    );
    assert!(catalog.take_partition_identifiers().is_empty());
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

    sql_context
        .sql("ALTER TABLE paimon.mydb.alter_test ADD COLUMN age INT")
        .await
        .expect("ALTER TABLE ADD COLUMN should succeed");

    // The new column is appended to the table schema.
    let table = catalog
        .get_table(&Identifier::new("mydb", "alter_test"))
        .await
        .unwrap();
    let names: Vec<&str> = table.schema().fields().iter().map(|f| f.name()).collect();
    assert_eq!(names, vec!["id", "name", "age"]);
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
async fn test_not_filter_pushdown_keeps_sql_null_semantics() {
    let (_tmp, catalog) = create_test_env();
    let ctx = create_sql_context(catalog.clone()).await;

    catalog
        .create_database("my_db", false, Default::default())
        .await
        .unwrap();
    ctx.sql("CREATE TABLE paimon.my_db.t (id INT, name STRING)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    ctx.sql("INSERT INTO paimon.my_db.t VALUES (1, 'one'), (2, 'two'), (NULL, 'nil')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = ctx
        .sql("SELECT id FROM paimon.my_db.t WHERE NOT (id = 1) ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let values = batches
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column_by_name("id")
                .and_then(|column| {
                    column
                        .as_any()
                        .downcast_ref::<datafusion::arrow::array::Int32Array>()
                })
                .expect("id column");
            (0..ids.len()).map(|row| ids.value(row)).collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    assert_eq!(values, vec![2]);
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

// ======================= SHOW CREATE TABLE =======================

/// Collect the `definition` column from `SHOW CREATE TABLE` output as a String.
async fn collect_definition(sql_context: &SQLContext, table_ref: &str) -> String {
    let rows = sql_context
        .sql(&format!("SHOW CREATE TABLE {}", table_ref))
        .await
        .expect("SHOW CREATE TABLE should plan")
        .collect()
        .await
        .expect("SHOW CREATE TABLE should execute");
    assert_eq!(
        rows.len(),
        1,
        "SHOW CREATE TABLE should return exactly one row"
    );
    let row = &rows[0];
    assert_eq!(
        row.num_rows(),
        1,
        "SHOW CREATE TABLE should return exactly one row"
    );
    let val = row.column(3); // definition is the 4th column
    let def = val
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .expect("definition column should be a StringArray")
        .value(0);
    def.to_string()
}

#[tokio::test]
async fn test_show_create_table_simple() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog).await;

    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA should succeed");
    sql_context
        .sql("CREATE TABLE paimon.test_db.t (id INT, name VARCHAR(100))")
        .await
        .expect("CREATE TABLE should succeed");

    let definition = collect_definition(&sql_context, "paimon.test_db.t").await;
    assert!(
        definition.contains("CREATE TABLE \"test_db\".\"t\""),
        "definition should start with CREATE TABLE \"test_db\".\"t\", got: {definition}"
    );
    assert!(
        definition.contains("\"id\" INT"),
        "definition should contain `\"id\" INT`, got: {definition}"
    );
    assert!(
        definition.contains("\"name\" VARCHAR("),
        "definition should contain `\"name\" VARCHAR(...)`, got: {definition}"
    );
}

#[tokio::test]
async fn test_show_create_table_with_primary_key() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog).await;

    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA should succeed");
    sql_context
        .sql("CREATE TABLE paimon.test_db.t (id INT NOT NULL, name VARCHAR, PRIMARY KEY (id))")
        .await
        .expect("CREATE TABLE should succeed");

    let definition = collect_definition(&sql_context, "paimon.test_db.t").await;
    assert!(
        definition.contains("PRIMARY KEY (\"id\")"),
        "definition should contain PRIMARY KEY (\"id\"), got: {definition}"
    );
}

#[tokio::test]
async fn test_show_create_table_with_partition_and_options() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog).await;

    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA should succeed");
    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t (id INT, name VARCHAR, pt INT) \
             PARTITIONED BY (pt) WITH ('bucket' = '4', 'file.format' = 'parquet')",
        )
        .await
        .expect("CREATE TABLE should succeed");

    let definition = collect_definition(&sql_context, "paimon.test_db.t").await;
    assert!(
        definition.contains("PARTITIONED BY (\"pt\")"),
        "definition should contain PARTITIONED BY (\"pt\"), got: {definition}"
    );
    assert!(
        definition.contains("'bucket' = '4'"),
        "definition should contain bucket option, got: {definition}"
    );
    assert!(
        definition.contains("'file.format' = 'parquet'"),
        "definition should contain file.format option, got: {definition}"
    );
}

#[tokio::test]
async fn test_show_create_table_excludes_session_dynamic_options() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog).await;

    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA should succeed");
    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t (id INT, name VARCHAR, pt INT) \
             PARTITIONED BY (pt) WITH ('file.format' = 'parquet')",
        )
        .await
        .expect("CREATE TABLE should succeed");
    sql_context
        .sql("INSERT INTO paimon.test_db.t VALUES (1, 'one', 1)")
        .await
        .expect("INSERT should plan")
        .collect()
        .await
        .expect("INSERT should execute");
    sql_context
        .sql("CALL sys.create_tag(table => 'test_db.t', tag => 'before_age')")
        .await
        .expect("CREATE TAG should succeed");
    sql_context
        .sql("ALTER TABLE paimon.test_db.t ADD COLUMN age INT")
        .await
        .expect("ALTER TABLE should succeed");
    sql_context
        .sql("SET 'paimon.scan.version' = 'before_age'")
        .await
        .expect("SET scan.version should succeed");
    sql_context
        .sql("SET 'paimon.blob-as-descriptor' = 'true'")
        .await
        .expect("SET blob-as-descriptor should succeed");

    let definition = collect_definition(&sql_context, "paimon.test_db.t").await;
    assert!(
        definition.contains("'file.format' = 'parquet'"),
        "definition should keep persisted table options, got: {definition}"
    );
    assert!(
        definition.contains("\"age\" INT"),
        "definition should use current persisted schema, got: {definition}"
    );
    for dynamic_option in ["scan.version", "blob-as-descriptor"] {
        assert!(
            !definition.contains(dynamic_option),
            "definition should not contain session dynamic option {dynamic_option}, got: {definition}"
        );
    }
}

#[tokio::test]
async fn test_dynamic_scan_ignores_current_show_create_unsupported_type() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA should succeed");
    sql_context
        .sql("CREATE TABLE paimon.test_db.t (id INT)")
        .await
        .expect("CREATE TABLE should succeed");
    sql_context
        .sql("INSERT INTO paimon.test_db.t VALUES (1)")
        .await
        .expect("INSERT should plan")
        .collect()
        .await
        .expect("INSERT should execute");
    sql_context
        .sql("CALL sys.create_tag(table => 'test_db.t', tag => 'before_time')")
        .await
        .expect("CREATE TAG should succeed");

    let identifier = Identifier::new("test_db", "t");
    catalog
        .alter_table(
            &identifier,
            vec![SchemaChange::add_column(
                "unsupported_col".to_string(),
                DataType::Time(TimeType::new(3).unwrap()),
            )],
            false,
        )
        .await
        .expect("ALTER TABLE should add unsupported SHOW CREATE type");

    sql_context
        .sql("SET 'paimon.scan.version' = 'before_time'")
        .await
        .expect("SET scan.version should succeed");

    let rows = sql_context
        .sql("SELECT * FROM paimon.test_db.t")
        .await
        .expect("dynamic scan should plan with historical schema")
        .collect()
        .await
        .expect("dynamic scan should execute");
    assert_eq!(rows[0].schema().fields().len(), 1);
    assert_eq!(rows[0].schema().field(0).name(), "id");
    let row_count: usize = rows.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(row_count, 1);
}

#[tokio::test]
async fn test_show_create_table_various_types() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog).await;

    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA should succeed");
    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t (\
             a BOOLEAN, \
             b TINYINT, \
             c SMALLINT, \
             d BIGINT, \
             e DECIMAL(10, 2), \
             f DOUBLE, \
             g FLOAT, \
             h DATE, \
             i TIMESTAMP(3), \
             j BLOB) \
             WITH ('data-evolution.enabled' = 'true')",
        )
        .await
        .expect("CREATE TABLE should succeed");

    let definition = collect_definition(&sql_context, "paimon.test_db.t").await;
    for needle in [
        "\"a\" BOOLEAN",
        "\"b\" TINYINT",
        "\"c\" SMALLINT",
        "\"d\" BIGINT",
        "\"e\" DECIMAL(10, 2)",
        "\"f\" DOUBLE",
        "\"g\" FLOAT",
        "\"h\" DATE",
        "\"i\" TIMESTAMP(3)",
        "\"j\" BLOB",
    ] {
        assert!(
            definition.contains(needle),
            "definition should contain `{needle}`, got: {definition}"
        );
    }
}

/// Assert that two `TableSchema`s are equivalent for round-trip purposes:
/// same fields (id, name, type), same primary keys, same partition keys.
///
/// We do not compare `options` because the CREATE TABLE path may inject
/// catalog defaults (e.g. `bucket`) that the user did not specify; the
/// schema fields and key columns are what the DDL must preserve.
fn assert_schema_equivalent(left: &paimon::spec::TableSchema, right: &paimon::spec::TableSchema) {
    assert_eq!(
        left.fields().len(),
        right.fields().len(),
        "field count mismatch\nleft  (original): {:?}\nright (recreated): {:?}",
        left.fields(),
        right.fields()
    );
    for (lf, rf) in left.fields().iter().zip(right.fields().iter()) {
        assert_eq!(
            lf.id(),
            rf.id(),
            "field id mismatch for `{}`: {} vs {}",
            lf.name(),
            lf.id(),
            rf.id()
        );
        assert_eq!(
            lf.name(),
            rf.name(),
            "field name mismatch: `{}` vs `{}`",
            lf.name(),
            rf.name()
        );
        assert_eq!(
            lf.data_type(),
            rf.data_type(),
            "field type mismatch for `{}`: {:?} vs {:?}",
            lf.name(),
            lf.data_type(),
            rf.data_type()
        );
    }
    assert_eq!(
        left.primary_keys(),
        right.primary_keys(),
        "primary keys mismatch: {:?} vs {:?}",
        left.primary_keys(),
        right.primary_keys()
    );
    assert_eq!(
        left.partition_keys(),
        right.partition_keys(),
        "partition keys mismatch: {:?} vs {:?}",
        left.partition_keys(),
        right.partition_keys()
    );
}

/// Round-trip test: the DDL returned by `SHOW CREATE TABLE` must be executable
/// by paimon-rust's own `CREATE TABLE` parser and reproduce an equivalent
/// schema (fields, primary keys, partition keys).
///
/// This guards against regressions where the rendered DDL drifts away from
/// what the parser accepts (e.g. `ROW<name: type>` vs `STRUCT<name type>`,
/// `MAP<k: v>` vs `MAP(k, v)`, or dropped `NOT NULL`).
#[tokio::test]
async fn test_show_create_table_round_trip() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA should succeed");
    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t1 (\
             id INT NOT NULL, \
             name VARCHAR NOT NULL, \
             tags ARRAY<STRING>, \
             props MAP(INT, VARCHAR), \
             addr STRUCT<city VARCHAR, zip VARCHAR>, \
             meta STRUCT<kv MAP(STRING, STRING), tags ARRAY<INT>>, \
             PRIMARY KEY (id)) \
             PARTITIONED BY (name) \
             WITH ('bucket' = '2', 'file.format' = 'parquet')",
        )
        .await
        .expect("CREATE TABLE should succeed");

    let identifier = Identifier::new("test_db", "t1");
    let original = catalog.get_table(&identifier).await.expect("table exists");
    let original_schema = original.schema().clone();

    let definition = collect_definition(&sql_context, "paimon.test_db.t1").await;
    // The DDL is rendered as `CREATE TABLE test_db.t1 (...)` without the
    // catalog prefix; paimon is the default catalog so this resolves back
    // to the same catalog/database.
    assert!(
        definition.starts_with("CREATE TABLE \"test_db\".\"t1\""),
        "definition should start with `CREATE TABLE \"test_db\".\"t1\"`, got: {definition}"
    );

    catalog
        .drop_table(&identifier, false)
        .await
        .expect("drop should succeed");

    sql_context
        .sql(&definition)
        .await
        .expect("DDL should re-execute")
        .collect()
        .await
        .expect("DDL should execute");

    let recreated = catalog
        .get_table(&identifier)
        .await
        .expect("recreated table exists");
    assert_schema_equivalent(&original_schema, recreated.schema());
}

#[tokio::test]
async fn test_show_create_table_round_trip_with_quoted_identifiers_and_options() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA should succeed");

    let identifier = Identifier::new("test_db", "select");
    let schema = paimon::spec::Schema::builder()
        .column("group", DataType::Int(IntType::with_nullable(false)))
        .column("order", DataType::Int(IntType::with_nullable(false)))
        .column("a\"b,c", DataType::Int(IntType::new()))
        .column(
            "nested",
            DataType::Row(paimon::spec::RowType::new(vec![
                paimon::spec::DataField::new(
                    0,
                    "from".to_string(),
                    DataType::VarChar(VarCharType::new(VarCharType::MAX_LENGTH).unwrap()),
                ),
            ])),
        )
        .column(
            "ts_ltz",
            DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(3).unwrap()),
        )
        .column("fixed_char", DataType::Char(CharType::new(7).unwrap()))
        .column(
            "bounded_varchar",
            DataType::VarChar(VarCharType::new(42).unwrap()),
        )
        .column(
            "fixed_binary",
            DataType::Binary(BinaryType::new(8).unwrap()),
        )
        .column(
            "bounded_varbinary",
            DataType::VarBinary(VarBinaryType::try_new(true, 32).unwrap()),
        )
        .primary_key(vec!["group", "order"])
        .partition_keys(vec!["a\"b,c"])
        .option("comment", "Bob's table")
        .build()
        .expect("schema should build");
    catalog
        .create_table(&identifier, schema, false)
        .await
        .expect("table should be created");
    let original = catalog.get_table(&identifier).await.expect("table exists");
    let original_schema = original.schema().clone();

    let definition = collect_definition(&sql_context, "paimon.test_db.\"select\"").await;
    assert!(
        definition.starts_with("CREATE TABLE \"test_db\".\"select\""),
        "definition should quote table identifiers, got: {definition}"
    );
    assert!(
        definition.contains("\"order\" INT NOT NULL"),
        "definition should quote column identifiers, got: {definition}"
    );
    assert!(
        definition.contains("PRIMARY KEY (\"group\", \"order\")"),
        "definition should quote primary key identifiers, got: {definition}"
    );
    assert!(
        definition.contains("\"a\"\"b,c\" INT"),
        "definition should escape quoted column identifiers, got: {definition}"
    );
    assert!(
        definition.contains("PARTITIONED BY (\"a\"\"b,c\")"),
        "definition should escape quoted partition identifiers, got: {definition}"
    );
    assert!(
        definition.contains("STRUCT<\"from\" VARCHAR"),
        "definition should quote nested struct field identifiers, got: {definition}"
    );
    assert!(
        definition.contains("'comment' = 'Bob''s table'"),
        "definition should escape string literals, got: {definition}"
    );
    assert!(
        definition.contains("\"ts_ltz\" TIMESTAMP(3) WITH TIME ZONE"),
        "definition should render TIMESTAMP WITH TIME ZONE for LTZ, got: {definition}"
    );
    assert!(
        definition.contains("\"fixed_char\" CHAR(7)"),
        "definition should preserve CHAR length, got: {definition}"
    );
    assert!(
        definition.contains("\"bounded_varchar\" VARCHAR(42)"),
        "definition should preserve VARCHAR length, got: {definition}"
    );
    assert!(
        definition.contains("\"fixed_binary\" BINARY(8)"),
        "definition should preserve BINARY length, got: {definition}"
    );
    assert!(
        definition.contains("\"bounded_varbinary\" VARBINARY(32)"),
        "definition should preserve VARBINARY length, got: {definition}"
    );

    catalog
        .drop_table(&identifier, false)
        .await
        .expect("drop should succeed");

    sql_context
        .sql(&definition)
        .await
        .expect("DDL should re-execute")
        .collect()
        .await
        .expect("DDL should execute");

    let recreated = catalog
        .get_table(&identifier)
        .await
        .expect("recreated table exists");
    assert_schema_equivalent(&original_schema, recreated.schema());
}

#[tokio::test]
async fn test_show_create_table_rejects_non_round_trippable_types() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA should succeed");

    for (table_name, data_type, type_name) in [
        ("time_t", DataType::Time(TimeType::new(3).unwrap()), "TIME"),
        (
            "multiset_t",
            DataType::Multiset(MultisetType::new(DataType::Int(IntType::new()))),
            "MULTISET",
        ),
        (
            "vector_t",
            DataType::Vector(VectorType::new(4, DataType::Float(FloatType::new())).unwrap()),
            "VECTOR",
        ),
    ] {
        let identifier = Identifier::new("test_db", table_name);
        let schema = paimon::spec::Schema::builder()
            .column("unsupported_col", data_type)
            .build()
            .expect("schema should build");
        catalog
            .create_table(&identifier, schema, false)
            .await
            .expect("table should be created");

        sql_context
            .sql("SET 'paimon.blob-as-descriptor' = 'true'")
            .await
            .expect("SET blob-as-descriptor should succeed");

        let err = sql_context
            .sql(&format!("SHOW CREATE TABLE paimon.test_db.{table_name}"))
            .await
            .expect_err("SHOW CREATE TABLE should reject unsupported type");
        assert!(
            err.to_string().contains(type_name),
            "error should mention {type_name}, got: {err}"
        );
    }
}
