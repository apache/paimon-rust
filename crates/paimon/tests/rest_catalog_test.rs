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
use paimon::api::ConfigResponse;
use paimon::catalog::{Catalog, Function, FunctionDefinition, Identifier, RESTCatalog, ViewSchema};
use paimon::common::Options;
use paimon::spec::{
    BigIntType, BlobType, BlobViewStruct, DataField, DataType, Datum, IntType, PartitionStatistics,
    PredicateBuilder, Schema, SchemaChange, VarCharType,
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

// ==================== Partition Tests ====================

#[tokio::test]
async fn test_rest_catalog_skips_empty_and_batches_idempotent_create_partitions() {
    let ctx = setup_catalog(vec!["default"]).await;
    let identifier = Identifier::new("default", "managed_table");
    ctx.server.add_table("default", "managed_table");
    let partition_specs = (0..1001)
        .map(|value| HashMap::from([("dt".to_string(), value.to_string())]))
        .collect::<Vec<_>>();

    ctx.catalog
        .create_partitions(&identifier, Vec::new(), false)
        .await
        .unwrap();
    ctx.catalog
        .create_partitions(&identifier, partition_specs.clone(), true)
        .await
        .unwrap();

    let calls = ctx.server.create_partitions_calls();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].2.partition_specs, partition_specs[..1000]);
    assert_eq!(calls[1].2.partition_specs, partition_specs[1000..]);
    assert!(calls.iter().all(|(_, _, request)| request.ignore_if_exists));
}

#[tokio::test]
async fn test_rest_catalog_keeps_non_idempotent_create_in_one_request() {
    let ctx = setup_catalog(vec!["default"]).await;
    let identifier = Identifier::new("default", "managed_table");
    ctx.server.add_table("default", "managed_table");
    let partition_specs = (0..2500)
        .map(|value| HashMap::from([("dt".to_string(), value.to_string())]))
        .collect::<Vec<_>>();

    ctx.catalog
        .create_partitions(&identifier, partition_specs.clone(), false)
        .await
        .unwrap();

    let calls = ctx.server.create_partitions_calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].2.partition_specs, partition_specs);
    assert!(!calls[0].2.ignore_if_exists);
}

#[tokio::test]
async fn test_rest_catalog_sends_partition_statistics_with_their_batch() {
    let ctx = setup_catalog(vec!["default"]).await;
    let identifier = Identifier::new("default", "managed_table");
    ctx.server.add_table("default", "managed_table");
    let specs = (0..1001)
        .map(|value| HashMap::from([("dt".to_string(), value.to_string())]))
        .collect::<Vec<_>>();
    let statistic = |spec: &HashMap<String, String>| PartitionStatistics {
        spec: spec.clone(),
        record_count: 1,
        file_size_in_bytes: 2,
        file_count: 3,
        last_file_creation_time: 4,
        total_buckets: -1,
    };

    // Reported for the last partition and the first, in that order.
    ctx.catalog
        .create_partitions_with_statistics(
            &identifier,
            specs.clone(),
            true,
            Some(vec![statistic(&specs[1000]), statistic(&specs[0])]),
            true,
        )
        .await
        .unwrap();

    let calls = ctx.server.create_partitions_calls();
    assert_eq!(calls.len(), 2);
    assert_eq!(
        calls[0].2.partition_statistics,
        Some(vec![statistic(&specs[0])])
    );
    assert_eq!(
        calls[1].2.partition_statistics,
        Some(vec![statistic(&specs[1000])])
    );
    assert!(calls
        .iter()
        .all(|(_, _, request)| request.replace_statistics == Some(true)));

    // A report the catalog could not place is refused before anything is sent.
    let unknown = HashMap::from([("dt".to_string(), "x".to_string())]);
    for statistics in [
        vec![statistic(&unknown)],
        vec![statistic(&specs[0]), statistic(&specs[0])],
    ] {
        let error = ctx
            .catalog
            .create_partitions_with_statistics(
                &identifier,
                specs.clone(),
                true,
                Some(statistics),
                true,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(error, paimon::Error::DataInvalid { .. }),
            "{error}"
        );
    }
    assert_eq!(ctx.server.create_partitions_calls().len(), 2);
}

#[tokio::test]
async fn test_rest_catalog_maps_create_partition_conflict() {
    let ctx = setup_catalog(vec!["default"]).await;
    let identifier = Identifier::new("default", "managed_table");
    ctx.server
        .set_create_partitions_error_status(Some(StatusCode::CONFLICT));

    let error = ctx
        .catalog
        .create_partitions(
            &identifier,
            vec![HashMap::from([(
                "dt".to_string(),
                "2026-07-22".to_string(),
            )])],
            false,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        paimon::Error::DataInvalid { message, .. }
            if message.contains("default.managed_table")
                && message.contains("already exist")
    ));
}

#[tokio::test]
async fn test_rest_catalog_maps_invalid_partition_request() {
    let ctx = setup_catalog(vec!["default"]).await;
    let identifier = Identifier::new("default", "managed_table");
    ctx.server
        .set_create_partitions_error_status(Some(StatusCode::BAD_REQUEST));

    let error = ctx
        .catalog
        .create_partitions(
            &identifier,
            vec![HashMap::from([(
                "unknown".to_string(),
                "value".to_string(),
            )])],
            false,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        paimon::Error::DataInvalid { message, .. }
            if message.contains("default.managed_table")
    ));
}

#[tokio::test]
async fn test_rest_catalog_create_partitions_maps_missing_table() {
    let ctx = setup_catalog(vec!["default"]).await;
    let identifier = Identifier::new("default", "missing");

    let error = ctx
        .catalog
        .create_partitions(
            &identifier,
            vec![HashMap::from([(
                "dt".to_string(),
                "2026-07-22".to_string(),
            )])],
            false,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        paimon::Error::TableNotExist { full_name } if full_name == "default.missing"
    ));
}

#[tokio::test]
async fn test_rest_catalog_looks_up_partitions_by_names_in_batches() {
    let ctx = setup_catalog(vec!["default"]).await;
    let identifier = Identifier::new("default", "managed_table");
    ctx.server.add_table("default", "managed_table");
    let spec = |value: usize| HashMap::from([("dt".to_string(), value.to_string())]);
    let registered = (0..2500).step_by(2).map(spec).collect::<Vec<_>>();
    ctx.server
        .set_table_partitions("default", "managed_table", registered.clone());
    let requested = (0..2500).map(spec).collect::<Vec<_>>();

    assert!(ctx
        .catalog
        .list_partitions_by_names(&identifier, Vec::new())
        .await
        .unwrap()
        .is_empty());
    let found = ctx
        .catalog
        .list_partitions_by_names(&identifier, requested.clone())
        .await
        .unwrap();

    assert_eq!(
        found
            .into_iter()
            .map(|partition| partition.spec)
            .collect::<Vec<_>>(),
        registered
    );
    let calls = ctx
        .server
        .table_partition_list_by_names_calls("default", "managed_table");
    assert_eq!(
        calls.iter().map(Vec::len).collect::<Vec<_>>(),
        vec![1000, 1000, 500]
    );
    assert_eq!(calls.concat(), requested);
}

#[tokio::test]
async fn test_rest_catalog_looks_up_partitions_by_names_through_the_listing_when_unsupported() {
    let ctx = setup_catalog(vec!["default"]).await;
    let identifier = Identifier::new("default", "managed_table");
    ctx.server.add_table("default", "managed_table");
    let registered = HashMap::from([("dt".to_string(), "2026-07-22".to_string())]);
    let missing = HashMap::from([("dt".to_string(), "2026-07-23".to_string())]);
    ctx.server
        .set_table_partitions("default", "managed_table", vec![registered.clone()]);
    ctx.server
        .set_list_partitions_by_names_error_status(Some(StatusCode::NOT_IMPLEMENTED));

    let found = ctx
        .catalog
        .list_partitions_by_names(&identifier, vec![registered.clone(), missing])
        .await
        .unwrap();

    assert_eq!(
        found
            .into_iter()
            .map(|partition| partition.spec)
            .collect::<Vec<_>>(),
        vec![registered]
    );
    assert_eq!(
        ctx.server
            .table_partition_list_by_names_calls("default", "managed_table")
            .len(),
        1
    );
    assert_eq!(
        ctx.server
            .table_partition_list_name_patterns("default", "managed_table"),
        vec![None]
    );
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

#[tokio::test]
async fn test_load_table_returns_external_for_declared_type() {
    use paimon::catalog::LoadedTable;
    use paimon::spec::TableType;

    let ctx = setup_catalog(vec!["default"]).await;
    let schema = Schema::builder()
        .column("id", DataType::BigInt(BigIntType::new()))
        .option("type", "iceberg-table")
        .build()
        .unwrap();
    ctx.server
        .add_table_with_schema("default", "ice_t", schema, "file:///unused");

    let identifier = Identifier::new("default", "ice_t");
    let routed = ctx.catalog.load_table(&identifier).await.unwrap();
    assert!(
        matches!(routed, LoadedTable::External(ref e) if e.declared() == TableType::IcebergTable),
        "{routed:?}"
    );
}

#[tokio::test]
async fn test_load_table_constructs_native_object_table() {
    use paimon::catalog::LoadedTable;

    let object_dir = tempfile::TempDir::new().unwrap();
    std::fs::write(object_dir.path().join("object.txt"), b"object").unwrap();
    let object_location = format!("file://{}", object_dir.path().display());

    let ctx = setup_catalog(vec!["default"]).await;
    let schema = Schema::builder()
        .column("ignored", DataType::BigInt(BigIntType::new()))
        .option("type", "object-table")
        .build()
        .unwrap();
    ctx.server
        .add_table_with_schema("default", "objects", schema, &object_location);

    let loaded = ctx
        .catalog
        .load_table(&Identifier::new("default", "objects"))
        .await
        .unwrap();
    let LoadedTable::Object(table) = loaded else {
        panic!("expected a native object table, got {loaded:?}");
    };
    assert_eq!(table.location(), object_location);
    #[cfg(not(windows))]
    assert_eq!(
        table
            .list_objects()
            .await
            .unwrap()
            .iter()
            .map(|entry| entry.path())
            .collect::<Vec<_>>(),
        vec!["object.txt"]
    );
}

#[tokio::test]
async fn test_load_table_fails_closed_on_query_auth() {
    let ctx = setup_catalog(vec!["default"]).await;
    let schema = Schema::builder()
        .column("id", DataType::BigInt(BigIntType::new()))
        .option("type", "iceberg-table")
        .option("query-auth.enabled", "true")
        .build()
        .unwrap();
    ctx.server
        .add_table_with_schema("default", "authed_ice_t", schema, "file:///unused");

    let identifier = Identifier::new("default", "authed_ice_t");
    let err = ctx.catalog.load_table(&identifier).await.unwrap_err();
    assert!(
        matches!(err, paimon::Error::Unsupported { ref message } if message.contains("query-auth")),
        "{err:?}"
    );
}

#[tokio::test]
async fn test_load_table_constructs_paimon_for_undeclared_type() {
    use paimon::catalog::LoadedTable;

    let ctx = setup_catalog(vec!["default"]).await;
    let schema = Schema::builder()
        .column("id", DataType::BigInt(BigIntType::new()))
        .build()
        .unwrap();
    ctx.server
        .add_table_with_schema("default", "plain_t", schema, "file:///unused");

    let identifier = Identifier::new("default", "plain_t");
    let routed = ctx.catalog.load_table(&identifier).await.unwrap();
    match routed {
        LoadedTable::Paimon(table) => {
            assert_eq!(table.identifier().full_name(), "default.plain_t");
        }
        other => panic!("expected a constructed Paimon table, got {other:?}"),
    }
}

#[tokio::test]
async fn test_get_table_fails_closed_on_iceberg_table() {
    let ctx = setup_catalog(vec!["default"]).await;
    let schema = Schema::builder()
        .column("id", DataType::BigInt(BigIntType::new()))
        .option("type", "iceberg-table")
        .build()
        .unwrap();
    ctx.server
        .add_table_with_schema("default", "raw_ice_t", schema, "file:///unused");

    let err = ctx
        .catalog
        .get_table(&Identifier::new("default", "raw_ice_t"))
        .await
        .unwrap_err();
    assert!(
        matches!(err, paimon::Error::Unsupported { ref message }
            if message.contains("cannot be read as a Paimon table")),
        "{err:?}"
    );
}

#[tokio::test]
async fn test_load_table_parses_declared_type_case_insensitively() {
    use paimon::catalog::LoadedTable;
    use paimon::spec::TableType;

    let ctx = setup_catalog(vec!["default"]).await;
    let schema = Schema::builder()
        .column("id", DataType::BigInt(BigIntType::new()))
        .option("type", "ICEBERG-TABLE")
        .build()
        .unwrap();
    ctx.server
        .add_table_with_schema("default", "upper_ice_t", schema, "file:///unused");

    let routed = ctx
        .catalog
        .load_table(&Identifier::new("default", "upper_ice_t"))
        .await
        .unwrap();
    assert!(
        matches!(routed, LoadedTable::External(ref e) if e.declared() == TableType::IcebergTable),
        "{routed:?}"
    );
}

#[tokio::test]
async fn test_load_table_rejects_unknown_declared_type() {
    let ctx = setup_catalog(vec!["default"]).await;
    let valid = Schema::builder()
        .column("id", DataType::BigInt(BigIntType::new()))
        .build()
        .unwrap();
    let mut payload = serde_json::to_value(&valid).unwrap();
    payload["options"] = serde_json::json!({ "type": "delta-table" });
    let schema: Schema = serde_json::from_value(payload).unwrap();
    ctx.server
        .add_table_with_schema("default", "delta_t", schema, "file:///unused");

    let err = ctx
        .catalog
        .load_table(&Identifier::new("default", "delta_t"))
        .await
        .unwrap_err();
    assert!(
        matches!(err, paimon::Error::Unsupported { ref message }
            if message.contains("unknown table type")),
        "{err:?}"
    );
}
