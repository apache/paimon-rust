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

//! End-to-end tests for the FileSystemCatalog-backed REST catalog server.
//!
//! Each test spins up a real [`FsRestCatalogServer`] over a temporary warehouse
//! and drives it through the client-side [`RESTCatalog`], covering metadata CRUD
//! and a full append write + commit + read-back round trip.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use futures::TryStreamExt;
use tempfile::TempDir;

use paimon::catalog::{Catalog, Identifier, RESTCatalog};
use paimon::common::{CatalogOptions, Options};
use paimon::spec::{BigIntType, DataType, IntType, Schema, SchemaChange, VarCharType};

use paimon_rest_server::FsRestCatalogServer;

/// Holds the temp warehouse and the running server so they outlive the catalog.
struct TestContext {
    _warehouse: TempDir,
    _server: FsRestCatalogServer,
    catalog: RESTCatalog,
}

async fn setup() -> TestContext {
    let warehouse = TempDir::new().expect("create temp warehouse");
    let warehouse_path = warehouse.path().to_str().unwrap().to_string();

    let server = FsRestCatalogServer::start(warehouse_path.clone(), "")
        .await
        .expect("start server");

    let mut options = Options::new();
    options.set(CatalogOptions::METASTORE, "rest");
    options.set(CatalogOptions::URI, server.url());
    options.set(CatalogOptions::WAREHOUSE, &warehouse_path);
    options.set(CatalogOptions::TOKEN_PROVIDER, "bear");
    options.set(CatalogOptions::TOKEN, "dummy-token");

    let catalog = RESTCatalog::new(options, true)
        .await
        .expect("create RESTCatalog");

    TestContext {
        _warehouse: warehouse,
        _server: server,
        catalog,
    }
}

fn append_only_schema() -> Schema {
    Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("name", DataType::VarChar(VarCharType::new(255).unwrap()))
        .option("bucket", "1")
        .option("bucket-key", "id")
        .build()
        .expect("build schema")
}

fn sample_batch() -> RecordBatch {
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, true),
        ArrowField::new("name", ArrowDataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
        ],
    )
    .expect("build batch")
}

fn partitioned_schema() -> Schema {
    Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("region", DataType::VarChar(VarCharType::new(255).unwrap()))
        .partition_keys(["region"])
        .option("bucket", "1")
        .option("bucket-key", "id")
        .build()
        .expect("build schema")
}

fn partitioned_batch() -> RecordBatch {
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, true),
        ArrowField::new("region", ArrowDataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["us", "eu", "us"])),
        ],
    )
    .expect("build batch")
}

// ==================== Database metadata ====================

#[tokio::test]
async fn test_database_crud() {
    let ctx = setup().await;
    let cat = &ctx.catalog;

    // Initially empty.
    assert!(cat.list_databases().await.unwrap().is_empty());

    // Create + list + get.
    cat.create_database("db1", false, HashMap::new())
        .await
        .unwrap();
    let dbs = cat.list_databases().await.unwrap();
    assert_eq!(dbs, vec!["db1".to_string()]);
    let db = cat.get_database("db1").await.unwrap();
    assert_eq!(db.name, "db1");

    // Duplicate without ignore_if_exists -> error; with ignore -> ok.
    assert!(cat
        .create_database("db1", false, HashMap::new())
        .await
        .is_err());
    cat.create_database("db1", true, HashMap::new())
        .await
        .unwrap();

    // Get missing -> error.
    assert!(cat.get_database("nope").await.is_err());

    // Drop + verify; ignore_if_not_exists semantics.
    cat.drop_database("db1", false, false).await.unwrap();
    assert!(cat.list_databases().await.unwrap().is_empty());
    assert!(cat.drop_database("db1", false, false).await.is_err());
    cat.drop_database("db1", true, false).await.unwrap();
}

// ==================== Table metadata ====================

#[tokio::test]
async fn test_table_crud_and_rename() {
    let ctx = setup().await;
    let cat = &ctx.catalog;
    cat.create_database("db", false, HashMap::new())
        .await
        .unwrap();

    let users = Identifier::new("db", "users");
    cat.create_table(&users, append_only_schema(), false)
        .await
        .unwrap();
    assert_eq!(
        cat.list_tables("db").await.unwrap(),
        vec!["users".to_string()]
    );

    // Duplicate create errors unless ignored.
    assert!(cat
        .create_table(&users, append_only_schema(), false)
        .await
        .is_err());
    cat.create_table(&users, append_only_schema(), true)
        .await
        .unwrap();

    // get_table returns a usable table with the right schema/location.
    let table = cat.get_table(&users).await.unwrap();
    assert_eq!(table.schema().fields().len(), 2);
    assert!(table.location().ends_with("db.db/users"));

    // Rename round trip.
    let renamed = Identifier::new("db", "users_renamed");
    cat.rename_table(&users, &renamed, false).await.unwrap();
    assert_eq!(
        cat.list_tables("db").await.unwrap(),
        vec!["users_renamed".to_string()]
    );
    assert!(cat.get_table(&users).await.is_err());
    cat.rename_table(&renamed, &users, false).await.unwrap();

    // Drop + missing semantics.
    cat.drop_table(&users, false).await.unwrap();
    assert!(cat.list_tables("db").await.unwrap().is_empty());
    assert!(cat.drop_table(&users, false).await.is_err());
    cat.drop_table(&users, true).await.unwrap();
}

#[tokio::test]
async fn test_get_table_missing() {
    let ctx = setup().await;
    let cat = &ctx.catalog;
    cat.create_database("db", false, HashMap::new())
        .await
        .unwrap();
    assert!(cat
        .get_table(&Identifier::new("db", "ghost"))
        .await
        .is_err());
}

// ==================== Full write + commit + read ====================

#[tokio::test]
async fn test_write_commit_read_roundtrip() {
    let ctx = setup().await;
    let cat = &ctx.catalog;

    cat.create_database("smoke_db", false, HashMap::new())
        .await
        .unwrap();
    let ident = Identifier::new("smoke_db", "users");
    cat.create_table(&ident, append_only_schema(), false)
        .await
        .unwrap();

    // Append write + commit through RESTSnapshotCommit (-> server commit endpoint).
    let table = cat.get_table(&ident).await.unwrap();
    let write_builder = table.new_write_builder();
    let mut writer = write_builder.new_write().unwrap();
    writer.write_arrow_batch(&sample_batch()).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert!(!messages.is_empty(), "expected at least one commit message");
    write_builder.new_commit().commit(messages).await.unwrap();

    // Read back.
    let table = cat.get_table(&ident).await.unwrap();
    let read_builder = table.new_read_builder();
    let plan = read_builder.new_scan().plan().await.unwrap();
    let read = read_builder.new_read().unwrap();
    let mut stream = read.to_arrow(plan.splits()).unwrap();

    let mut total = 0usize;
    while let Some(batch) = stream.try_next().await.unwrap() {
        total += batch.num_rows();
    }
    assert_eq!(total, 3, "expected 3 rows read back, got {total}");
}

// ==================== alter table over REST ====================

#[tokio::test]
async fn test_alter_table_columns() {
    let ctx = setup().await;
    let cat = &ctx.catalog;

    cat.create_database("db", false, HashMap::new())
        .await
        .unwrap();
    let ident = Identifier::new("db", "events");
    cat.create_table(&ident, append_only_schema(), false)
        .await
        .unwrap();

    // Apply a batch of column changes through the REST alter_table path.
    cat.alter_table(
        &ident,
        vec![
            SchemaChange::add_column("age".to_string(), DataType::Int(IntType::new())),
            SchemaChange::rename_column("name".to_string(), "full_name".to_string()),
            SchemaChange::update_column_comment("id".to_string(), "the id".to_string()),
            SchemaChange::update_column_type(
                "age".to_string(),
                DataType::BigInt(BigIntType::new()),
            ),
        ],
        false,
    )
    .await
    .unwrap();

    // The server persisted a new schema version; get_table reflects it.
    let table = cat.get_table(&ident).await.unwrap();
    let schema = table.schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name()).collect();
    assert_eq!(names, vec!["id", "full_name", "age"]);

    let id_field = schema.fields().iter().find(|f| f.name() == "id").unwrap();
    assert_eq!(id_field.description(), Some("the id"));
    let age_field = schema.fields().iter().find(|f| f.name() == "age").unwrap();
    assert!(matches!(age_field.data_type(), DataType::BigInt(_)));

    // alter on a missing table: ignored vs. error.
    let missing = Identifier::new("db", "nope");
    cat.alter_table(
        &missing,
        vec![SchemaChange::update_column_comment(
            "id".to_string(),
            "x".to_string(),
        )],
        true,
    )
    .await
    .unwrap();
    assert!(cat
        .alter_table(
            &missing,
            vec![SchemaChange::update_column_comment(
                "id".to_string(),
                "x".to_string(),
            )],
            false,
        )
        .await
        .is_err());
}

// ==================== list partitions over REST ====================

#[tokio::test]
async fn test_list_partitions() {
    let ctx = setup().await;
    let cat = &ctx.catalog;

    cat.create_database("db", false, HashMap::new())
        .await
        .unwrap();
    let ident = Identifier::new("db", "events");
    cat.create_table(&ident, partitioned_schema(), false)
        .await
        .unwrap();

    // Write rows spanning two partitions (region=us has 2 rows, region=eu 1),
    // then commit through the server's commit endpoint.
    let table = cat.get_table(&ident).await.unwrap();
    let write_builder = table.new_write_builder();
    let mut writer = write_builder.new_write().unwrap();
    writer
        .write_arrow_batch(&partitioned_batch())
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert!(!messages.is_empty(), "expected at least one commit message");
    write_builder.new_commit().commit(messages).await.unwrap();

    // The partitions endpoint must serve the two partitions (no 404 fallback);
    // the client receives them directly from the server.
    let partitions = cat.list_partitions(&ident).await.unwrap();
    assert_eq!(partitions.len(), 2, "expected two partitions");

    let mut regions: Vec<String> = partitions
        .iter()
        .map(|p| p.spec.get("region").cloned().unwrap_or_default())
        .collect();
    regions.sort();
    assert_eq!(regions, vec!["eu".to_string(), "us".to_string()]);

    let total: i64 = partitions.iter().map(|p| p.record_count).sum();
    assert_eq!(total, 3, "partition record counts should sum to all rows");

    let us = partitions
        .iter()
        .find(|p| p.spec.get("region").map(String::as_str) == Some("us"))
        .expect("us partition present");
    assert_eq!(us.record_count, 2, "region=us holds two rows");
}

// ==================== Path codec round trip ====================

/// Database/table names with characters the REST path codec encodes specially
/// must survive the client encode -> server decode round trip.
///
/// The client builds path segments with `RESTUtil::encode_string`
/// (`application/x-www-form-urlencoded`), so a space becomes `+` and a literal
/// `+` becomes `%2B`. The server must decode them back to the exact original
/// names; otherwise these databases/tables are unaddressable through
/// `RESTCatalog`. A literal `+` additionally proves the server decodes the raw
/// segment (not Axum's already percent-decoded value), since `+` and a space
/// would otherwise be indistinguishable.
#[tokio::test]
async fn test_special_char_names() {
    let ctx = setup().await;
    let cat = &ctx.catalog;

    // Two databases: one with a space, one with a literal `+`.
    let space_db = "sales db";
    let plus_db = "a+b";
    cat.create_database(space_db, false, HashMap::new())
        .await
        .unwrap();
    cat.create_database(plus_db, false, HashMap::new())
        .await
        .unwrap();

    let mut dbs = cat.list_databases().await.unwrap();
    dbs.sort();
    assert_eq!(dbs, vec![plus_db.to_string(), space_db.to_string()]);

    // get_database must address each one by its exact name.
    assert_eq!(cat.get_database(space_db).await.unwrap().name, space_db);
    assert_eq!(cat.get_database(plus_db).await.unwrap().name, plus_db);

    // A table whose name also contains a space, under the space database.
    let ident = Identifier::new(space_db, "my table");
    cat.create_table(&ident, append_only_schema(), false)
        .await
        .unwrap();
    assert_eq!(
        cat.list_tables(space_db).await.unwrap(),
        vec!["my table".to_string()]
    );
    let table = cat.get_table(&ident).await.unwrap();
    assert_eq!(table.schema().fields().len(), 2);

    // Drop the table and both databases by their exact names.
    cat.drop_table(&ident, false).await.unwrap();
    cat.drop_database(space_db, false, false).await.unwrap();
    cat.drop_database(plus_db, false, false).await.unwrap();
    assert!(cat.list_databases().await.unwrap().is_empty());
}
