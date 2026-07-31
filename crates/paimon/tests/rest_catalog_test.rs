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
use std::sync::Arc;

use arrow_array::{Array, BinaryArray, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use axum::http::StatusCode;
use futures::TryStreamExt;
use paimon::api::{AuthTableQueryResponse, ConfigResponse};
use paimon::catalog::{Catalog, Function, FunctionDefinition, Identifier, RESTCatalog, ViewSchema};
use paimon::common::Options;
use paimon::spec::{
    BigIntType, BlobType, BlobViewStruct, DataField, DataType, Datum, IntType, PredicateBuilder,
    Schema, SchemaChange, VarCharType,
};
use paimon::{CatalogOptions, FileSystemCatalog, Table};

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

fn blob_schema(options: &[(&str, &str)]) -> Schema {
    let mut builder = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("name", DataType::VarChar(VarCharType::new(255).unwrap()))
        .column("picture", DataType::Blob(BlobType::new()))
        .option("data-evolution.enabled", "true")
        .option("row-tracking.enabled", "true");
    for (key, value) in options {
        builder = builder.option(*key, *value);
    }
    builder.build().expect("Failed to build blob schema")
}

fn blob_batch(ids: Vec<i32>, names: Vec<&str>, pictures: Vec<Vec<u8>>) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("name", ArrowDataType::Utf8, true),
        ArrowField::new("picture", ArrowDataType::Binary, true),
    ]));
    let picture_refs = pictures
        .iter()
        .map(|bytes| Some(bytes.as_slice()))
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(BinaryArray::from(picture_refs)),
        ],
    )
    .unwrap()
}

async fn write_batch(table: &Table, batch: RecordBatch, commit_user: &str) {
    let write_builder = table
        .new_write_builder()
        .with_commit_user(commit_user)
        .expect("valid commit user");
    let mut write = write_builder.new_write().expect("create writer");
    write.write_arrow_batch(&batch).await.expect("write batch");
    let messages = write.prepare_commit().await.expect("prepare commit");
    write_builder
        .new_commit()
        .commit(messages)
        .await
        .expect("commit batch");
}

fn collect_blob_rows(batches: &[RecordBatch]) -> Vec<(i32, String, Option<Vec<u8>>)> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let pictures = batch
            .column(2)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                ids.value(row),
                names.value(row).to_string(),
                if pictures.is_null(row) {
                    None
                } else {
                    Some(pictures.value(row).to_vec())
                },
            ));
        }
    }
    rows.sort_by_key(|(id, _, _)| *id);
    rows
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
    assert!(result.is_ok(), "failed to create database: {result:?}");

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
    assert!(result.is_ok(), "failed to drop database: {result:?}");

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
        "expected empty tables list, got: {tables:?}"
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
    assert!(table.is_ok(), "failed to get table: {table:?}");
}

/// The grant is scoped to the columns the plan requested (like Java passing
/// `readType.getFieldNames()`): a wider read against a scoped grant fails
/// closed until it re-plans.
#[cfg(not(windows))]
#[tokio::test]
async fn test_query_auth_grant_scoped_to_planned_columns() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = format!("file://{}", tmp.path().display());

    // Real data, so the scoped plan actually produces splits to assert on.
    let mut fs_options = Options::new();
    fs_options.set(CatalogOptions::WAREHOUSE, &warehouse);
    let fs_catalog = FileSystemCatalog::new(fs_options).expect("create filesystem catalog");
    fs_catalog
        .create_database("default", true, HashMap::new())
        .await
        .unwrap();
    let columns = || {
        Schema::builder()
            .column("id", DataType::BigInt(BigIntType::new()))
            .column("name", DataType::VarChar(VarCharType::new(255).unwrap()))
            .option("bucket", "1")
            .option("bucket-key", "id")
    };
    let identifier = Identifier::new("default", "qa_scope");
    fs_catalog
        .create_table(&identifier, columns().build().unwrap(), false)
        .await
        .unwrap();
    let writer = fs_catalog.get_table(&identifier).await.unwrap();
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, true),
        ArrowField::new("name", ArrowDataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2])),
            Arc::new(arrow_array::StringArray::from(vec!["a", "b"])),
        ],
    )
    .unwrap();
    write_batch(&writer, batch, "u1").await;

    let ctx = setup_catalog(vec!["default"]).await;
    ctx.server.add_table_with_schema(
        "default",
        "qa_scope",
        columns()
            .option("query-auth.enabled", "true")
            .build()
            .unwrap(),
        &format!("{warehouse}/default.db/qa_scope"),
    );
    let table = ctx.catalog.get_table(&identifier).await.unwrap();

    // Plan a projection of {id}: the grant is scoped to that column and stamped
    // on this plan's splits (not a shared slot on the table).
    let mut projected = table.new_read_builder();
    projected.with_projection(&["id"]).unwrap();
    let scoped_plan = projected.new_scan().plan().await.unwrap();
    assert!(
        !scoped_plan.splits().is_empty(),
        "the scoped plan must produce splits for this assertion to mean anything"
    );
    assert!(
        projected
            .new_read()
            .unwrap()
            .to_arrow(scoped_plan.splits())
            .is_ok(),
        "the planned projection must read under its own grant"
    );

    // The grant rides on the splits it was planned with, so handing those
    // splits to a wider read must fail closed rather than widen the scope.
    let wide = table.new_read_builder().new_read().unwrap();
    match wide.to_arrow(scoped_plan.splits()) {
        Err(paimon::Error::Unsupported { message }) => assert!(
            message.contains("outside the authorized set"),
            "unexpected rejection: {message}"
        ),
        Ok(_) => panic!("a wider read must not run under the {{id}} plan's scoped grant"),
        Err(e) => panic!("unexpected error: {e}"),
    }

    // A separate full-table read re-authorizes for all columns when it plans.
    table.new_read_builder().new_scan().plan().await.unwrap();
}

/// Java #8447 baseline: a query-auth row filter disables count-based limit
/// pushdown, so a limited read still reaches authorized rows in later files.
#[cfg(not(windows))]
#[tokio::test]
async fn test_query_auth_row_filter_reads_past_limit_pushdown() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = format!("file://{}", tmp.path().display());

    // Write two commits (-> two files) through a plain FileSystemCatalog table.
    let mut fs_options = Options::new();
    fs_options.set(CatalogOptions::WAREHOUSE, &warehouse);
    let fs_catalog = FileSystemCatalog::new(fs_options).expect("create filesystem catalog");
    fs_catalog
        .create_database("default", true, HashMap::new())
        .await
        .unwrap();
    let write_schema = Schema::builder()
        .column("id", DataType::BigInt(BigIntType::new()))
        .option("bucket", "1")
        .option("bucket-key", "id")
        .build()
        .unwrap();
    let identifier = Identifier::new("default", "qa_limit");
    fs_catalog
        .create_table(&identifier, write_schema, false)
        .await
        .unwrap();
    let writer = fs_catalog.get_table(&identifier).await.unwrap();
    let int_batch = |ids: Vec<i64>| {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::Int64,
            true,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids))]).unwrap()
    };
    write_batch(&writer, int_batch(vec![1, 2, 3, 4]), "u1").await;
    write_batch(&writer, int_batch(vec![5, 6, 7, 8]), "u2").await;

    // Read the same files through the REST catalog with query-auth enabled and
    // a row filter of id >= 6 (all matches live in the SECOND file).
    let ctx = setup_catalog(vec!["default"]).await;
    let read_schema = Schema::builder()
        .column("id", DataType::BigInt(BigIntType::new()))
        .option("bucket", "1")
        .option("bucket-key", "id")
        .option("query-auth.enabled", "true")
        .build()
        .unwrap();
    ctx.server.add_table_with_schema(
        "default",
        "qa_limit",
        read_schema,
        &format!("{warehouse}/default.db/qa_limit"),
    );
    ctx.server.set_auth_response(
        "default",
        "qa_limit",
        AuthTableQueryResponse {
            filter: Some(vec![
                r#"{"kind":"LEAF","transform":{"name":"FIELD_REF","fieldRef":{"index":0,"name":"id","type":"BIGINT"}},"function":"GREATER_OR_EQUAL","literals":[6]}"#
                    .to_string(),
            ]),
            column_masking: None,
        },
    );
    let table = ctx.catalog.get_table(&identifier).await.unwrap();

    let mut builder = table.new_read_builder();
    builder.with_limit(2);
    let plan = builder.new_scan().plan().await.unwrap();
    // The filter runs as a residual pass, so the plan must not cap splits by
    // the unfiltered limit and must report its row counts as inexact.
    assert!(!plan.row_counts_exact());
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut ids: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec![6, 7, 8],
        "authorized rows beyond file 1 must appear"
    );
}

/// Java #8570 baseline: a cross-column mask (`alias := UPPER(name)`) and a row
/// filter on an unprojected column (`score`) must still enforce when the caller
/// projects neither `name` nor `score` — the read is widened with the grant's
/// columns and every batch of every split is projected back to the caller's
/// columns (no auth-added column may leak from later splits).
#[cfg(not(windows))]
#[tokio::test]
async fn test_query_auth_cross_column_mask_with_narrow_projection() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = format!("file://{}", tmp.path().display());

    let mut fs_options = Options::new();
    fs_options.set(CatalogOptions::WAREHOUSE, &warehouse);
    let fs_catalog = FileSystemCatalog::new(fs_options).expect("create filesystem catalog");
    fs_catalog
        .create_database("default", true, HashMap::new())
        .await
        .unwrap();
    let columns = || {
        Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("name", DataType::VarChar(VarCharType::new(255).unwrap()))
            .column("alias", DataType::VarChar(VarCharType::new(255).unwrap()))
            .column("score", DataType::BigInt(BigIntType::new()))
            .option("bucket", "1")
            .option("bucket-key", "id")
    };
    let identifier = Identifier::new("default", "qa_cross_mask");
    fs_catalog
        .create_table(&identifier, columns().build().unwrap(), false)
        .await
        .unwrap();
    let writer = fs_catalog.get_table(&identifier).await.unwrap();
    let batch = |ids: Vec<i32>, names: Vec<&str>, aliases: Vec<&str>, scores: Vec<i64>| {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, true),
            ArrowField::new("name", ArrowDataType::Utf8, true),
            ArrowField::new("alias", ArrowDataType::Utf8, true),
            ArrowField::new("score", ArrowDataType::Int64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(StringArray::from(aliases)),
                Arc::new(Int64Array::from(scores)),
            ],
        )
        .unwrap()
    };
    // Two commits -> two files, so enforcement is exercised across splits.
    write_batch(
        &writer,
        batch(vec![1, 2], vec!["ann", "bob"], vec!["x", "y"], vec![5, 15]),
        "u1",
    )
    .await;
    write_batch(
        &writer,
        batch(vec![3, 4], vec!["cid", "dan"], vec!["z", "w"], vec![20, 8]),
        "u2",
    )
    .await;

    let ctx = setup_catalog(vec!["default"]).await;
    ctx.server.add_table_with_schema(
        "default",
        "qa_cross_mask",
        columns()
            .option("query-auth.enabled", "true")
            .build()
            .unwrap(),
        &format!("{warehouse}/default.db/qa_cross_mask"),
    );
    ctx.server.set_auth_response(
        "default",
        "qa_cross_mask",
        AuthTableQueryResponse {
            // Row filter on `score` (index 3), which the caller does not project.
            filter: Some(vec![
                r#"{"kind":"LEAF","transform":{"name":"FIELD_REF","fieldRef":{"index":3,"name":"score","type":"BIGINT"}},"function":"GREATER_OR_EQUAL","literals":[10]}"#
                    .to_string(),
            ]),
            // Cross-column mask: `alias` is overwritten from `name` (index 1),
            // which the caller does not project either.
            column_masking: Some(HashMap::from([(
                "alias".to_string(),
                r#"{"name":"UPPER","inputs":[{"index":1,"name":"name","type":"STRING"}]}"#
                    .to_string(),
            )])),
        },
    );
    let table = ctx.catalog.get_table(&identifier).await.unwrap();

    let mut builder = table.new_read_builder();
    builder.with_projection(&["id", "alias"]).unwrap();
    let plan = builder.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    let mut rows = Vec::new();
    for b in &batches {
        assert_eq!(
            b.schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>(),
            vec!["id", "alias"],
            "auth-added columns (name, score) must not leak from any split"
        );
        let ids = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let aliases = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        for r in 0..b.num_rows() {
            rows.push((ids.value(r), aliases.value(r).to_string()));
        }
    }
    rows.sort_unstable();
    assert_eq!(
        rows,
        vec![(2, "BOB".to_string()), (3, "CID".to_string())],
        "filter must drop score<10 rows in both files and alias must be UPPER(name)"
    );
}

/// Visible end-to-end demo of query-auth enforcement over a mock REST catalog:
/// the same files are read once with no grant (raw) and once through a
/// `query-auth.enabled` table whose per-user grant applies a row filter
/// (`salary >= 90000`) plus column masking (`name -> UPPER(name)`). Run with:
///   cargo test -p paimon --test rest_catalog_test query_auth_enforcement_demo -- --nocapture
#[cfg(not(windows))]
#[tokio::test]
async fn test_query_auth_enforcement_demo() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = format!("file://{}", tmp.path().display());

    // --- Write demo_employees (id, name, salary) via a plain FileSystemCatalog ---
    let mut fs_options = Options::new();
    fs_options.set(CatalogOptions::WAREHOUSE, &warehouse);
    let fs_catalog = FileSystemCatalog::new(fs_options).expect("create filesystem catalog");
    fs_catalog
        .create_database("default", true, HashMap::new())
        .await
        .unwrap();
    let columns = || {
        Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("name", DataType::VarChar(VarCharType::new(255).unwrap()))
            .column("salary", DataType::BigInt(BigIntType::new()))
            .option("bucket", "1")
            .option("bucket-key", "id")
    };
    let identifier = Identifier::new("default", "demo_employees");
    fs_catalog
        .create_table(&identifier, columns().build().unwrap(), false)
        .await
        .unwrap();
    let writer = fs_catalog.get_table(&identifier).await.unwrap();

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, true),
        ArrowField::new("name", ArrowDataType::Utf8, true),
        ArrowField::new("salary", ArrowDataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                "alice", "bob", "charlie", "diana", "eve",
            ])),
            Arc::new(Int64Array::from(vec![120000, 85000, 95000, 70000, 99000])),
        ],
    )
    .unwrap();
    write_batch(&writer, batch, "u1").await;

    let dump = |label: &str, batches: &[RecordBatch]| {
        println!("\n  {label}");
        println!("    {:<4} {:<10} {:>8}", "id", "name", "salary");
        for b in batches {
            let ids = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            let names = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            let sal = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
            for r in 0..b.num_rows() {
                println!(
                    "    {:<4} {:<10} {:>8}",
                    ids.value(r),
                    names.value(r),
                    sal.value(r)
                );
            }
        }
    };

    // --- Raw read (no query-auth) ---
    let raw: Vec<RecordBatch> = {
        let b = writer.new_read_builder();
        b.new_read()
            .unwrap()
            .to_arrow(b.new_scan().plan().await.unwrap().splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap()
    };
    dump("RAW (no grant): all rows, real names", &raw);

    // --- Enforced read through the REST catalog with a per-user grant ---
    let ctx = setup_catalog(vec!["default"]).await;
    ctx.server.add_table_with_schema(
        "default",
        "demo_employees",
        columns()
            .option("query-auth.enabled", "true")
            .build()
            .unwrap(),
        &format!("{warehouse}/default.db/demo_employees"),
    );
    ctx.server.set_auth_response(
        "default",
        "demo_employees",
        AuthTableQueryResponse {
            // Row filter: salary >= 90000 (field index 2).
            filter: Some(vec![
                r#"{"kind":"LEAF","transform":{"name":"FIELD_REF","fieldRef":{"index":2,"name":"salary","type":"BIGINT"}},"function":"GREATER_OR_EQUAL","literals":[90000]}"#
                    .to_string(),
            ]),
            // Column masking: name -> UPPER(name) (field index 1).
            column_masking: Some(HashMap::from([(
                "name".to_string(),
                r#"{"name":"UPPER","inputs":[{"index":1,"name":"name","type":"STRING"}]}"#
                    .to_string(),
            )])),
        },
    );
    let table = ctx.catalog.get_table(&identifier).await.unwrap();
    let b = table.new_read_builder();
    // Plan first: scan planning fetches + verifies the per-user grant (mirroring
    // Java `CatalogEnvironment.tableQueryAuth()`) and authorizes the shared read
    // state; only then may the sync read gate (`new_read`/`to_arrow`) proceed.
    let plan = b.new_scan().plan().await.unwrap();
    let enforced: Vec<RecordBatch> = b
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    dump("ENFORCED (grant: salary>=90000, name->UPPER)", &enforced);

    let mut rows: Vec<(i32, String, i64)> = enforced
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let sal = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..batch.num_rows())
                .map(|r| (ids.value(r), names.value(r).to_string(), sal.value(r)))
                .collect::<Vec<_>>()
        })
        .collect();
    rows.sort_unstable();
    assert_eq!(
        rows,
        vec![
            (1, "ALICE".to_string(), 120000),
            (3, "CHARLIE".to_string(), 95000),
            (5, "EVE".to_string(), 99000),
        ],
        "row filter must drop salary<90000 and masking must uppercase name"
    );
}

/// Query-auth: scan planning transparently fetches the per-user grant
/// (mirroring Java's `CatalogEnvironment.tableQueryAuth()`); an unrestricted
/// user reads everything, a filtered/masked user gets an enforced read, and
/// paths that cannot enforce stay fail-closed.
#[tokio::test]
async fn test_catalog_get_table_query_auth() {
    let ctx = setup_catalog(vec!["default"]).await;
    let schema = Schema::builder()
        .column("id", DataType::BigInt(BigIntType::new()))
        .option("query-auth.enabled", "true")
        .build()
        .expect("Failed to build schema");
    ctx.server.add_table_with_schema(
        "default",
        "qa",
        schema,
        "file:///tmp/test_warehouse/default.db/qa",
    );
    let identifier = Identifier::new("default", "qa");
    let table = ctx.catalog.get_table(&identifier).await.unwrap();

    // Reading no splits yields no rows, so it is allowed even before planning;
    // real splits carrying no grant fail closed (read_builder unit tests).
    let ungranted = table.new_read_builder().new_read().unwrap();
    assert!(ungranted.to_arrow(&[]).is_ok());

    // The mock /auth endpoint reports unrestricted by default: planning a scan
    // authorizes the table and stamps the grant on its splits.
    let builder = table.new_read_builder();
    builder
        .new_scan()
        .plan()
        .await
        .expect("unrestricted user should be able to plan a query-auth scan");

    // A parseable row filter (Java Predicate JSON) grants a filtered read:
    // planning and building the read succeed; the filter is enforced inside
    // `to_arrow`.
    ctx.server.set_auth_response(
        "default",
        "qa",
        AuthTableQueryResponse {
            filter: Some(vec![
                r#"{"kind":"LEAF","transform":{"name":"FIELD_REF","fieldRef":{"index":0,"name":"id","type":"BIGINT"}},"function":"GREATER_THAN","literals":[5]}"#
                    .to_string(),
            ]),
            column_masking: None,
        },
    );
    let table = ctx.catalog.get_table(&identifier).await.unwrap();
    let builder = table.new_read_builder();
    builder
        .new_scan()
        .plan()
        .await
        .expect("a parseable row filter should allow planning a (filtered) read");
    // ... but paths that bypass the row filter stay strictly fail-closed.
    let err = table
        .new_vector_search_builder()
        .execute()
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("query-auth.enabled"),
        "search must stay fail-closed for a filtered user, got: {err}"
    );

    // An unparseable filter fails the plan and keeps the table fail-closed.
    ctx.server.set_auth_response(
        "default",
        "qa",
        AuthTableQueryResponse {
            filter: Some(vec!["{\"kind\":\"CUSTOM\"}".to_string()]),
            column_masking: None,
        },
    );
    let fresh = ctx.catalog.get_table(&identifier).await.unwrap();
    assert!(fresh.new_read_builder().new_scan().plan().await.is_err());

    // Parseable column masking grants a (masked) read; a caller predicate on
    // the masked column is rejected (it would leak the raw value).
    ctx.server.set_auth_response(
        "default",
        "qa",
        AuthTableQueryResponse {
            filter: None,
            column_masking: Some(HashMap::from([(
                "id".to_string(),
                "{\"name\":\"NULL\"}".to_string(),
            )])),
        },
    );
    let table = ctx.catalog.get_table(&identifier).await.unwrap();
    let builder = table.new_read_builder();
    builder.new_scan().plan().await.expect("masked read plans");
    // A caller predicate on a masked column fails closed at plan time (pruning
    // on its raw value would leak it); the same guard runs again in `to_arrow`.
    let mut filtered = table.new_read_builder();
    filtered.with_filter(
        PredicateBuilder::new(table.schema().fields())
            .equal("id", Datum::Long(1))
            .unwrap(),
    );
    let Err(err) = filtered.new_scan().plan().await else {
        panic!("a caller predicate on a masked column must fail closed");
    };
    assert!(
        err.to_string().contains("masked column"),
        "a caller predicate on a masked column must fail closed, got: {err}"
    );

    // Every plan re-authorizes (like Java): revoking down to an unparseable
    // grant fails the plan, and a read without a stamped grant stays closed.
    ctx.server.set_auth_response(
        "default",
        "qa",
        AuthTableQueryResponse {
            filter: Some(vec!["{\"kind\":\"CUSTOM\"}".to_string()]),
            column_masking: None,
        },
    );
    assert!(table.new_read_builder().new_scan().plan().await.is_err());
}

#[tokio::test]
async fn test_rest_env_get_table_reuses_catalog_environment() {
    let ctx = setup_catalog(vec!["default"]).await;

    ctx.server.add_table_with_schema(
        "default",
        "current_table",
        test_schema(),
        "file:///tmp/test_warehouse/default.db/current_table",
    );
    ctx.server.add_table_with_schema(
        "default",
        "upstream_table",
        test_schema(),
        "file:///tmp/test_warehouse/default.db/upstream_table",
    );

    let current = ctx
        .catalog
        .get_table(&Identifier::new("default", "current_table"))
        .await
        .expect("current table should load");
    let upstream = current
        .rest_env()
        .expect("REST table should carry RESTEnv")
        .get_table(&Identifier::new("default", "upstream_table"))
        .await
        .expect("upstream table should load via RESTEnv");

    assert_eq!(upstream.identifier().full_name(), "default.upstream_table");
    assert!(upstream.rest_env().is_some());
}

// This regression uses FileSystemCatalog to write real table files before reading
// them back through RESTCatalog. FileSystemCatalog directory listing is skipped
// on Windows elsewhere for the same opendal `fs` StripPrefixError.
#[cfg(not(windows))]
#[tokio::test]
async fn test_blob_view_prescan_filters_invalid_filtered_out_reference() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = format!("file://{}", tmp.path().display());

    let mut fs_options = Options::new();
    fs_options.set(CatalogOptions::WAREHOUSE, &warehouse);
    let fs_catalog = FileSystemCatalog::new(fs_options).expect("create filesystem catalog");
    fs_catalog
        .create_database("default", true, HashMap::new())
        .await
        .unwrap();

    let source_id = Identifier::new("default", "blob_source");
    let source_schema = blob_schema(&[]);
    fs_catalog
        .create_table(&source_id, source_schema.clone(), false)
        .await
        .unwrap();
    let source = fs_catalog.get_table(&source_id).await.unwrap();
    write_batch(
        &source,
        blob_batch(
            vec![1, 2],
            vec!["Alice", "Bob"],
            vec![b"alice".to_vec(), b"bob".to_vec()],
        ),
        "source-writer",
    )
    .await;

    let view_id = Identifier::new("default", "blob_view_target");
    let view_schema = blob_schema(&[("blob-view-field", "picture")]);
    fs_catalog
        .create_table(&view_id, view_schema.clone(), false)
        .await
        .unwrap();
    let view = fs_catalog.get_table(&view_id).await.unwrap();

    let picture_field_id = source
        .schema()
        .fields()
        .iter()
        .find(|field| field.name() == "picture")
        .unwrap()
        .id();
    let filtered_out_bad_ref = BlobViewStruct::new(source_id.clone(), picture_field_id, 99)
        .serialize()
        .unwrap();
    let kept_ref = BlobViewStruct::new(source_id.clone(), picture_field_id, 1)
        .serialize()
        .unwrap();
    write_batch(
        &view,
        blob_batch(
            vec![1, 2],
            vec!["Filtered", "Kept"],
            vec![filtered_out_bad_ref, kept_ref],
        ),
        "view-writer",
    )
    .await;

    let prefix = "mock-test";
    let mut defaults = HashMap::new();
    defaults.insert("prefix".to_string(), prefix.to_string());
    let server = start_mock_server(
        "test_warehouse".to_string(),
        warehouse.clone(),
        ConfigResponse::new(defaults),
        vec!["default".to_string()],
    )
    .await;
    server.add_table_with_schema("default", "blob_source", source_schema, source.location());
    server.add_table_with_schema("default", "blob_view_target", view_schema, view.location());

    let url = server.url().expect("Failed to get server URL");
    let mut rest_options = Options::new();
    rest_options.set("uri", &url);
    rest_options.set("warehouse", "test_warehouse");
    rest_options.set("token.provider", "bear");
    rest_options.set("token", "test_token");
    let rest_catalog = RESTCatalog::new(rest_options, true)
        .await
        .expect("create rest catalog");

    let rest_view = rest_catalog.get_table(&view_id).await.unwrap();
    let predicate = PredicateBuilder::new(rest_view.schema().fields())
        .equal("id", Datum::Int(2))
        .unwrap();
    let mut read_builder = rest_view.new_read_builder();
    read_builder.with_filter(predicate);
    let plan = read_builder.new_scan().plan().await.unwrap();
    let read = read_builder.new_read().unwrap();
    let batches = read
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    assert_eq!(
        collect_blob_rows(&batches),
        vec![(2, "Kept".to_string(), Some(b"bob".to_vec()))]
    );
}

#[cfg(not(windows))]
#[tokio::test]
async fn test_rest_catalog_reads_format_table() {
    use parquet::arrow::ArrowWriter;
    use std::fs::File;

    let tmp = tempfile::tempdir().unwrap();
    let format_path = format!("file://{}", tmp.path().display());

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("name", ArrowDataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["alice", "bob"])),
        ],
    )
    .unwrap();
    let file = File::create(tmp.path().join("part-0.parquet")).unwrap();
    let mut writer = ArrowWriter::try_new(file, arrow_schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let ctx = setup_catalog(vec!["default"]).await;
    let schema = Schema::builder()
        .column("id", DataType::BigInt(BigIntType::new()))
        .column("name", DataType::VarChar(VarCharType::new(255).unwrap()))
        .option("type", "format-table")
        .option("file.format", "parquet")
        .build()
        .unwrap();
    let identifier = Identifier::new("default", "format_users");
    ctx.server
        .add_table_with_schema("default", "format_users", schema, &format_path);

    let table = ctx.catalog.get_table(&identifier).await.unwrap();
    assert_eq!(table.location(), format_path);
    assert_eq!(table.schema().options().get("path"), Some(&format_path));
    assert!(table.new_write_builder().new_write().is_err());

    let read_builder = table.new_read_builder();
    let plan = read_builder.new_scan().plan().await.unwrap();
    assert_eq!(plan.splits().len(), 1);

    let read = read_builder.new_read().unwrap();
    let batches = read
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);

    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let names = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(ids.values(), &[1, 2]);
    assert_eq!(names.value(0), "alice");
    assert_eq!(names.value(1), "bob");

    let mut limited_builder = table.new_read_builder();
    limited_builder.with_limit(1);
    let limited_plan = limited_builder.new_scan().plan().await.unwrap();
    let limited_read = limited_builder.new_read().unwrap();
    let limited_batches = limited_read
        .to_arrow(limited_plan.splits())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        limited_batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        1
    );
}

#[cfg(not(windows))]
#[tokio::test]
async fn test_rest_catalog_prunes_format_table_partition_filter() {
    use parquet::arrow::ArrowWriter;
    use std::fs::{create_dir_all, File};

    let tmp = tempfile::tempdir().unwrap();
    let format_path = format!("file://{}", tmp.path().display());
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("name", ArrowDataType::Utf8, true),
    ]));
    for (dt, id, name) in [("2024-01-01", 1_i64, "alice"), ("2024-01-02", 2_i64, "bob")] {
        let dir = tmp.path().join(format!("dt={dt}"));
        create_dir_all(&dir).unwrap();
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![id])),
                Arc::new(StringArray::from(vec![name])),
            ],
        )
        .unwrap();
        let file = File::create(dir.join("part-0.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    let ctx = setup_catalog(vec!["default"]).await;
    let schema = Schema::builder()
        .column("dt", DataType::VarChar(VarCharType::new(32).unwrap()))
        .column("id", DataType::BigInt(BigIntType::new()))
        .column("name", DataType::VarChar(VarCharType::new(255).unwrap()))
        .partition_keys(["dt"])
        .option("type", "format-table")
        .option("file.format", "parquet")
        .build()
        .unwrap();
    let identifier = Identifier::new("default", "format_partitioned_users");
    ctx.server
        .add_table_with_schema("default", "format_partitioned_users", schema, &format_path);

    let table = ctx.catalog.get_table(&identifier).await.unwrap();
    let predicate = PredicateBuilder::new(table.schema().fields())
        .equal("dt", Datum::String("2024-01-02".to_string()))
        .unwrap();
    let mut read_builder = table.new_read_builder();
    read_builder.with_filter(predicate);
    let plan = read_builder.new_scan().plan().await.unwrap();
    assert_eq!(plan.splits().len(), 1);
    assert!(plan.splits()[0].bucket_path().contains("dt=2024-01-02"));

    let read = read_builder.new_read().unwrap();
    let batches = read
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);
    let dts = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let ids = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(dts.value(0), "2024-01-02");
    assert_eq!(ids.value(0), 2);

    let missing_predicate = PredicateBuilder::new(table.schema().fields())
        .equal("dt", Datum::String("2024-01-03".to_string()))
        .unwrap();
    let mut read_builder = table.new_read_builder();
    read_builder.with_filter(missing_predicate);
    let plan = read_builder.new_scan().plan().await.unwrap();
    assert!(plan.splits().is_empty());
}

#[cfg(not(windows))]
#[tokio::test]
async fn test_rest_catalog_reads_format_table_value_only_partition_path() {
    use parquet::arrow::ArrowWriter;
    use std::fs::{create_dir_all, File};

    let tmp = tempfile::tempdir().unwrap();
    let format_path = format!("file://{}", tmp.path().display());
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("name", ArrowDataType::Utf8, true),
    ]));
    for (dt, id, name) in [("2024-01-01", 1_i64, "alice"), ("2024-01-02", 2_i64, "bob")] {
        let dir = tmp.path().join(dt);
        create_dir_all(&dir).unwrap();
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![id])),
                Arc::new(StringArray::from(vec![name])),
            ],
        )
        .unwrap();
        let file = File::create(dir.join("part-0.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, arrow_schema.clone(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    let ctx = setup_catalog(vec!["default"]).await;
    let schema = Schema::builder()
        .column("dt", DataType::VarChar(VarCharType::new(32).unwrap()))
        .column("id", DataType::BigInt(BigIntType::new()))
        .column("name", DataType::VarChar(VarCharType::new(255).unwrap()))
        .partition_keys(["dt"])
        .option("type", "format-table")
        .option("file.format", "parquet")
        .option("format-table.partition-path-only-value", "true")
        .build()
        .unwrap();
    let identifier = Identifier::new("default", "format_value_only_partitioned_users");
    ctx.server.add_table_with_schema(
        "default",
        "format_value_only_partitioned_users",
        schema,
        &format_path,
    );

    let table = ctx.catalog.get_table(&identifier).await.unwrap();
    let predicate = PredicateBuilder::new(table.schema().fields())
        .equal("dt", Datum::String("2024-01-02".to_string()))
        .unwrap();
    let mut read_builder = table.new_read_builder();
    read_builder.with_filter(predicate);
    let plan = read_builder.new_scan().plan().await.unwrap();
    assert_eq!(plan.splits().len(), 1);
    assert!(plan.splits()[0].bucket_path().ends_with("/2024-01-02"));

    let read = read_builder.new_read().unwrap();
    let batches = read
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let dts = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(dts.value(0), "2024-01-02");
}

#[tokio::test]
async fn test_catalog_get_table_not_found() {
    let ctx = setup_catalog(vec!["default"]).await;

    let identifier = Identifier::new("default", "non_existent");
    let result = ctx.catalog.get_table(&identifier).await;
    assert!(result.is_err(), "getting non-existent table should fail");
}

/// When no data token is vended (`data_token_enabled=false` or external
/// table), `get_table` must propagate catalog options to FileIO so an
/// OSS-pathed table picks up `fs.oss.*` keys. Java parity:
/// `RESTCatalog.fileIOFromOptions`.
#[tokio::test]
async fn test_catalog_get_table_propagates_oss_options_in_else_branch() {
    let prefix = "mock-test";
    let mut defaults = HashMap::new();
    defaults.insert("prefix".to_string(), prefix.to_string());
    let config = ConfigResponse::new(defaults);

    let server = start_mock_server(
        "test_warehouse".to_string(),
        "/tmp/test_warehouse".to_string(),
        config,
        vec!["default".to_string()],
    )
    .await;

    let url = server.url().expect("Failed to get server URL");
    let mut options = Options::new();
    options.set("uri", &url);
    options.set("warehouse", "test_warehouse");
    options.set("token.provider", "bear");
    options.set("token", "test_token");
    options.set("fs.oss.endpoint", "https://oss-cn-shanghai.aliyuncs.com");
    options.set("fs.oss.accessKeyId", "test-ak");
    options.set("fs.oss.accessKeySecret", "test-sk");

    let catalog = RESTCatalog::new(options, true)
        .await
        .expect("create catalog");

    let schema = test_schema();
    server.add_table_with_schema(
        "default",
        "oss_table",
        schema,
        "oss://test-bucket/warehouse/default.db/oss_table",
    );

    let identifier = Identifier::new("default", "oss_table");
    let result = catalog.get_table(&identifier).await;
    assert!(
        result.is_ok(),
        "expected get_table to succeed when fs.oss.* keys are present in catalog options; \
         got {result:?}"
    );
}

#[tokio::test]
async fn test_catalog_create_table() {
    let ctx = setup_catalog(vec!["default"]).await;

    let schema = test_schema();
    let identifier = Identifier::new("default", "new_table");

    let result = ctx.catalog.create_table(&identifier, schema, false).await;
    assert!(result.is_ok(), "failed to create table: {result:?}");

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
    assert!(result.is_ok(), "failed to drop table: {result:?}");

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
    assert!(result.is_ok(), "failed to rename table: {result:?}");

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
async fn test_catalog_alter_table() {
    let ctx = setup_catalog(vec!["default"]).await;

    let identifier = Identifier::new("default", "some_table");
    ctx.catalog
        .create_table(&identifier, test_schema(), false)
        .await
        .unwrap();

    // alter_table on an existing table succeeds (client builds the request and
    // POSTs it; the mock accepts it).
    let changes = vec![SchemaChange::update_column_comment(
        "id".to_string(),
        "the id".to_string(),
    )];
    let result = ctx.catalog.alter_table(&identifier, changes, false).await;
    assert!(result.is_ok(), "alter_table should succeed: {result:?}");

    // alter_table on a missing table: error, unless ignore_if_not_exists.
    let missing = Identifier::new("default", "ghost");
    assert!(ctx
        .catalog
        .alter_table(&missing, vec![], false)
        .await
        .is_err());
    ctx.catalog
        .alter_table(&missing, vec![], true)
        .await
        .unwrap();
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

#[tokio::test]
async fn test_catalog_get_view() {
    let ctx = setup_catalog(vec!["default"]).await;
    let schema: ViewSchema = serde_json::from_value(serde_json::json!({
        "fields": [{"id": 0, "name": "id", "type": "INT"}],
        "query": "SELECT id FROM source",
        "dialects": {"datafusion": "SELECT id FROM source WHERE id > 0"},
        "comment": null,
        "options": {}
    }))
    .unwrap();
    ctx.server.add_view("default", "active_ids", schema);

    let view = ctx
        .catalog
        .get_view(&Identifier::new("default", "active_ids"))
        .await
        .unwrap();

    assert_eq!(view.full_name(), "default.active_ids");
    assert_eq!(
        view.query_for("datafusion"),
        "SELECT id FROM source WHERE id > 0"
    );
}

#[tokio::test]
async fn test_catalog_create_view() {
    let ctx = setup_catalog(vec!["default"]).await;
    let schema = ViewSchema::new(
        serde_json::from_value(serde_json::json!([
            {"id": 0, "name": "id", "type": "INT"}
        ]))
        .unwrap(),
        "SELECT id FROM source".to_string(),
        HashMap::from([(
            "datafusion".to_string(),
            "SELECT id FROM source".to_string(),
        )]),
        None,
        HashMap::new(),
    );
    let identifier = Identifier::new("default", "active_ids");

    ctx.catalog
        .create_view(&identifier, schema, false)
        .await
        .unwrap();

    let view = ctx.catalog.get_view(&identifier).await.unwrap();
    assert_eq!(view.query_for("datafusion"), "SELECT id FROM source");
}

#[tokio::test]
async fn test_catalog_drop_view() {
    let ctx = setup_catalog(vec!["default"]).await;
    let identifier = Identifier::new("default", "active_ids");
    ctx.catalog
        .create_view(
            &identifier,
            ViewSchema::new(
                Vec::new(),
                "SELECT 1".to_string(),
                HashMap::new(),
                None,
                HashMap::new(),
            ),
            false,
        )
        .await
        .unwrap();

    ctx.catalog.drop_view(&identifier, false).await.unwrap();

    assert!(ctx.catalog.list_views("default").await.unwrap().is_empty());
    assert!(matches!(
        ctx.catalog.get_view(&identifier).await.unwrap_err(),
        paimon::Error::ViewNotExist { full_name } if full_name == "default.active_ids"
    ));
}

#[tokio::test]
async fn test_catalog_drop_missing_view_honors_ignore_if_not_exists() {
    let ctx = setup_catalog(vec!["default"]).await;
    let identifier = Identifier::new("default", "missing");

    assert!(matches!(
        ctx.catalog.drop_view(&identifier, false).await.unwrap_err(),
        paimon::Error::ViewNotExist { full_name } if full_name == "default.missing"
    ));
    ctx.catalog.drop_view(&identifier, true).await.unwrap();
}

#[tokio::test]
async fn test_catalog_drop_view_if_exists_does_not_hide_other_errors() {
    let ctx = setup_catalog(vec!["default"]).await;
    let identifier = Identifier::new("default", "active_ids");

    ctx.server
        .set_drop_view_error_status(Some(StatusCode::FORBIDDEN));
    assert!(matches!(
        ctx.catalog.drop_view(&identifier, true).await.unwrap_err(),
        paimon::Error::RestApi {
            source: paimon::api::RestError::Forbidden { .. }
        }
    ));

    ctx.server
        .set_drop_view_error_status(Some(StatusCode::INTERNAL_SERVER_ERROR));
    assert!(matches!(
        ctx.catalog.drop_view(&identifier, true).await.unwrap_err(),
        paimon::Error::RestApi {
            source: paimon::api::RestError::ServiceFailure { .. }
        }
    ));
}

#[tokio::test]
async fn test_catalog_create_view_missing_database() {
    let ctx = setup_catalog(vec!["default"]).await;
    let identifier = Identifier::new("missing_db", "active_ids");
    let schema = ViewSchema::new(
        Vec::new(),
        "SELECT 1".to_string(),
        HashMap::new(),
        None,
        HashMap::new(),
    );

    let error = ctx
        .catalog
        .create_view(&identifier, schema, false)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        paimon::Error::DatabaseNotExist { database } if database == "missing_db"
    ));
}

#[tokio::test]
async fn test_catalog_create_view_already_exists() {
    let ctx = setup_catalog(vec!["default"]).await;
    let identifier = Identifier::new("default", "active_ids");
    let schema = ViewSchema::new(
        Vec::new(),
        "SELECT 1".to_string(),
        HashMap::new(),
        None,
        HashMap::new(),
    );
    ctx.catalog
        .create_view(&identifier, schema.clone(), false)
        .await
        .unwrap();

    let error = ctx
        .catalog
        .create_view(&identifier, schema, false)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        paimon::Error::ViewAlreadyExist { full_name } if full_name == "default.active_ids"
    ));
}

#[tokio::test]
async fn test_catalog_create_view_ignore_if_exists() {
    let ctx = setup_catalog(vec!["default"]).await;
    let identifier = Identifier::new("default", "active_ids");
    let schema = ViewSchema::new(
        Vec::new(),
        "SELECT 1".to_string(),
        HashMap::new(),
        None,
        HashMap::new(),
    );
    ctx.catalog
        .create_view(&identifier, schema.clone(), false)
        .await
        .unwrap();

    ctx.catalog
        .create_view(&identifier, schema, true)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_catalog_list_views() {
    let ctx = setup_catalog(vec!["default"]).await;
    let schema: ViewSchema = serde_json::from_value(serde_json::json!({
        "fields": [{"id": 0, "name": "id", "type": "INT"}],
        "query": "SELECT 1 AS id",
        "dialects": {},
        "comment": null,
        "options": {}
    }))
    .unwrap();
    ctx.server.add_view("default", "v2", schema.clone());
    ctx.server.add_view("default", "v1", schema);

    assert_eq!(
        ctx.catalog.list_views("default").await.unwrap(),
        vec!["v1", "v2"]
    );
}

#[tokio::test]
async fn test_catalog_get_function() {
    let ctx = setup_catalog(vec!["default"]).await;
    let input_params: Vec<DataField> = serde_json::from_value(serde_json::json!([
        {"id": 0, "name": "x", "type": "INT"}
    ]))
    .unwrap();
    let return_params: Vec<DataField> = serde_json::from_value(serde_json::json!([
        {"id": 0, "name": "result", "type": "INT"}
    ]))
    .unwrap();
    ctx.server.add_function(Function::new(
        Identifier::new("default", "plus_one"),
        Some(input_params),
        Some(return_params),
        true,
        HashMap::from([(
            "datafusion".to_string(),
            FunctionDefinition::Sql {
                definition: "x + 1".to_string(),
            },
        )]),
        None,
        HashMap::new(),
    ));

    let function = ctx
        .catalog
        .get_function(&Identifier::new("default", "plus_one"))
        .await
        .unwrap();

    assert_eq!(function.full_name(), "default.plus_one");
    assert_eq!(
        function
            .definition("datafusion")
            .and_then(FunctionDefinition::sql),
        Some("x + 1")
    );
}

#[tokio::test]
async fn test_catalog_create_function() {
    let ctx = setup_catalog(vec!["default"]).await;
    let function = Function::new(
        Identifier::new("default", "plus_one"),
        Some(
            serde_json::from_value(serde_json::json!([
                {"id": 0, "name": "x", "type": "BIGINT"}
            ]))
            .unwrap(),
        ),
        Some(
            serde_json::from_value(serde_json::json!([
                {"id": 0, "name": "result", "type": "BIGINT"}
            ]))
            .unwrap(),
        ),
        true,
        HashMap::from([(
            "datafusion".to_string(),
            FunctionDefinition::Sql {
                definition: "x + 1".to_string(),
            },
        )]),
        None,
        HashMap::new(),
    );

    ctx.catalog.create_function(&function, false).await.unwrap();

    let stored = ctx
        .catalog
        .get_function(function.identifier())
        .await
        .unwrap();
    assert_eq!(stored, function);
}

#[tokio::test]
async fn test_catalog_create_function_already_exists() {
    let ctx = setup_catalog(vec!["default"]).await;
    let function = Function::new(
        Identifier::new("default", "answer"),
        Some(Vec::new()),
        Some(
            serde_json::from_value(serde_json::json!([
                {"id": 0, "name": "result", "type": "INT"}
            ]))
            .unwrap(),
        ),
        true,
        HashMap::from([(
            "datafusion".to_string(),
            FunctionDefinition::Sql {
                definition: "42".to_string(),
            },
        )]),
        None,
        HashMap::new(),
    );
    ctx.catalog.create_function(&function, false).await.unwrap();

    let error = ctx
        .catalog
        .create_function(&function, false)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        paimon::Error::FunctionAlreadyExist { full_name }
            if full_name == "default.answer"
    ));
}

#[tokio::test]
async fn test_catalog_create_function_ignore_if_exists() {
    let ctx = setup_catalog(vec!["default"]).await;
    let function = Function::new(
        Identifier::new("default", "answer"),
        Some(Vec::new()),
        Some(Vec::new()),
        true,
        HashMap::new(),
        None,
        HashMap::new(),
    );
    ctx.catalog.create_function(&function, false).await.unwrap();

    ctx.catalog.create_function(&function, true).await.unwrap();
}

#[tokio::test]
async fn test_catalog_create_function_missing_database() {
    let ctx = setup_catalog(vec!["default"]).await;
    let function = Function::new(
        Identifier::new("missing_db", "answer"),
        Some(Vec::new()),
        Some(Vec::new()),
        true,
        HashMap::new(),
        None,
        HashMap::new(),
    );

    let error = ctx
        .catalog
        .create_function(&function, false)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        paimon::Error::DatabaseNotExist { database } if database == "missing_db"
    ));
}

#[tokio::test]
async fn test_catalog_list_functions() {
    let ctx = setup_catalog(vec!["default"]).await;
    let return_params: Vec<DataField> = serde_json::from_value(serde_json::json!([
        {"id": 0, "name": "result", "type": "INT"}
    ]))
    .unwrap();
    for name in ["zeta", "alpha"] {
        ctx.server.add_function(Function::new(
            Identifier::new("default", name),
            Some(Vec::new()),
            Some(return_params.clone()),
            true,
            HashMap::from([(
                "datafusion".to_string(),
                FunctionDefinition::Sql {
                    definition: "1".to_string(),
                },
            )]),
            None,
            HashMap::new(),
        ));
    }

    assert_eq!(
        ctx.catalog.list_functions("default").await.unwrap(),
        vec!["alpha", "zeta"]
    );
}

#[tokio::test]
async fn test_catalog_get_missing_view_maps_error() {
    let ctx = setup_catalog(vec!["default"]).await;

    let error = ctx
        .catalog
        .get_view(&Identifier::new("default", "missing"))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        paimon::Error::ViewNotExist { full_name } if full_name == "default.missing"
    ));
}

#[tokio::test]
async fn test_catalog_get_missing_function_maps_error() {
    let ctx = setup_catalog(vec!["default"]).await;

    let error = ctx
        .catalog
        .get_function(&Identifier::new("default", "missing"))
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        paimon::Error::FunctionNotExist { full_name } if full_name == "default.missing"
    ));
}

#[tokio::test]
async fn test_catalog_maps_unsupported_view_and_function_endpoints() {
    let ctx = setup_catalog(vec!["default"]).await;
    ctx.server.set_view_function_endpoints_unsupported();

    assert!(matches!(
        ctx.catalog
            .create_view(
                &Identifier::new("default", "view"),
                ViewSchema::new(
                    Vec::new(),
                    "SELECT 1".to_string(),
                    HashMap::new(),
                    None,
                    HashMap::new(),
                ),
                false,
            )
            .await
            .unwrap_err(),
        paimon::Error::Unsupported { .. }
    ));
    assert!(matches!(
        ctx.catalog.list_views("default").await.unwrap_err(),
        paimon::Error::Unsupported { .. }
    ));
    assert!(matches!(
        ctx.catalog
            .get_view(&Identifier::new("default", "view"))
            .await
            .unwrap_err(),
        paimon::Error::Unsupported { .. }
    ));
    assert!(matches!(
        ctx.catalog
            .drop_view(&Identifier::new("default", "view"), true)
            .await
            .unwrap_err(),
        paimon::Error::Unsupported { .. }
    ));
    assert!(matches!(
        ctx.catalog.list_functions("default").await.unwrap_err(),
        paimon::Error::Unsupported { .. }
    ));
    assert!(matches!(
        ctx.catalog
            .create_function(
                &Function::new(
                    Identifier::new("default", "function"),
                    Some(Vec::new()),
                    Some(Vec::new()),
                    true,
                    HashMap::new(),
                    None,
                    HashMap::new(),
                ),
                false,
            )
            .await
            .unwrap_err(),
        paimon::Error::Unsupported { .. }
    ));
    assert!(matches!(
        ctx.catalog
            .get_function(&Identifier::new("default", "function"))
            .await
            .unwrap_err(),
        paimon::Error::Unsupported { .. }
    ));
}
