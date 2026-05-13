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

//! Paimon system tables end-to-end via DataFusion SQL.

mod common;

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use paimon::catalog::Identifier;
use paimon::{Catalog, CatalogOptions, FileSystemCatalog, Options};
use paimon_datafusion::SQLContext;

const FIXTURE_TABLE: &str = "test_tantivy_fulltext";

fn extract_test_warehouse() -> (tempfile::TempDir, String) {
    let archive_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("testdata/test_tantivy_fulltext.tar.gz");
    let file = std::fs::File::open(&archive_path)
        .unwrap_or_else(|e| panic!("Failed to open {}: {e}", archive_path.display()));
    let decoder = flate2::read::GzDecoder::new(file);
    let mut archive = tar::Archive::new(decoder);

    let tmp = tempfile::tempdir().expect("Failed to create temp dir");
    let db_dir = tmp.path().join("default.db");
    std::fs::create_dir_all(&db_dir).unwrap();
    archive.unpack(&db_dir).unwrap();

    let warehouse = format!("file://{}", tmp.path().display());
    (tmp, warehouse)
}

async fn create_context() -> (SQLContext, Arc<dyn Catalog>, tempfile::TempDir) {
    let (tmp, warehouse) = extract_test_warehouse();
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse);
    let catalog = FileSystemCatalog::new(options).expect("Failed to create catalog");
    let catalog: Arc<dyn Catalog> = Arc::new(catalog);

    let mut ctx = SQLContext::new();
    ctx.register_catalog("paimon", catalog.clone())
        .await
        .expect("Failed to register catalog");
    (ctx, catalog, tmp)
}

async fn run_sql(ctx: &SQLContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql)
        .await
        .unwrap_or_else(|e| panic!("Failed to plan `{sql}`: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("Failed to execute `{sql}`: {e}"))
}

#[tokio::test]
async fn test_options_system_table() {
    let (ctx, catalog, _tmp) = create_context().await;
    let sql = format!("SELECT key, value FROM paimon.default.{FIXTURE_TABLE}$options");
    let batches = run_sql(&ctx, &sql).await;

    assert!(!batches.is_empty(), "$options should return ≥1 batch");
    let schema = batches[0].schema();
    assert_eq!(schema.field(0).name(), "key");
    assert_eq!(schema.field(1).name(), "value");

    let mut actual: Vec<(String, String)> = Vec::new();
    for batch in &batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("key column is Utf8");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("value column is Utf8");
        for i in 0..batch.num_rows() {
            actual.push((keys.value(i).to_string(), values.value(i).to_string()));
        }
    }
    actual.sort();

    let identifier = Identifier::new("default".to_string(), FIXTURE_TABLE.to_string());
    let table = catalog
        .get_table(&identifier)
        .await
        .expect("fixture table should load");
    let mut expected: Vec<(String, String)> = table
        .schema()
        .options()
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    expected.sort();

    assert_eq!(actual, expected, "$options rows should match table options");
}

#[tokio::test]
async fn test_unknown_system_table_name_returns_not_found() {
    let (ctx, _catalog, _tmp) = create_context().await;
    let sql = format!("SELECT * FROM paimon.default.{FIXTURE_TABLE}$nonsense");
    let err = ctx
        .sql(&sql)
        .await
        .expect_err("unknown system table should not resolve");
    let msg = err.to_string();
    assert!(
        msg.contains("nonsense") || msg.to_lowercase().contains("not found"),
        "unexpected error for unknown system table: {msg}"
    );
}

#[tokio::test]
async fn test_schemas_system_table() {
    let (ctx, catalog, _tmp) = create_context().await;
    let sql = format!("SELECT * FROM paimon.default.{FIXTURE_TABLE}$schemas");
    let batches = run_sql(&ctx, &sql).await;

    assert!(!batches.is_empty(), "$schemas should return ≥1 batch");

    let arrow_schema = batches[0].schema();
    let expected_columns = [
        ("schema_id", DataType::Int64),
        ("fields", DataType::Utf8),
        ("partition_keys", DataType::Utf8),
        ("primary_keys", DataType::Utf8),
        ("options", DataType::Utf8),
        ("comment", DataType::Utf8),
        (
            "update_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
        ),
    ];
    for (i, (name, dtype)) in expected_columns.iter().enumerate() {
        let field = arrow_schema.field(i);
        assert_eq!(field.name(), name, "column {i} name");
        assert_eq!(field.data_type(), dtype, "column {i} type");
    }

    let identifier = Identifier::new("default".to_string(), FIXTURE_TABLE.to_string());
    let table = catalog
        .get_table(&identifier)
        .await
        .expect("fixture table should load");
    let all_schemas = table
        .schema_manager()
        .list_all()
        .await
        .expect("list_all should succeed");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows,
        all_schemas.len(),
        "$schemas rows should match list_all() length"
    );

    let mut ids: Vec<i64> = Vec::new();
    for batch in &batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("schema_id is Int64");
        for i in 0..batch.num_rows() {
            ids.push(col.value(i));
        }
    }
    let mut sorted = ids.clone();
    sorted.sort_unstable();
    assert_eq!(ids, sorted, "schema_id column should be ascending");

    // The last row's JSON columns must round-trip to the current schema.
    let last_batch = batches.last().unwrap();
    let last_idx = last_batch.num_rows() - 1;
    let latest = table.schema();
    let json_columns: [(usize, &str, String); 4] = [
        (
            1,
            "fields",
            serde_json::to_string(latest.fields()).expect("serialize fields"),
        ),
        (
            2,
            "partition_keys",
            serde_json::to_string(latest.partition_keys()).expect("serialize partition_keys"),
        ),
        (
            3,
            "primary_keys",
            serde_json::to_string(latest.primary_keys()).expect("serialize primary_keys"),
        ),
        (
            4,
            "options",
            serde_json::to_string(latest.options()).expect("serialize options"),
        ),
    ];
    for (col_idx, col_name, expected) in &json_columns {
        let col = last_batch
            .column(*col_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("column {col_name} is not Utf8"));
        // Parse both sides before comparing: `options` is a HashMap whose
        // JSON key order is non-deterministic across `HashMap` instances.
        let actual: serde_json::Value = serde_json::from_str(col.value(last_idx))
            .unwrap_or_else(|e| panic!("parse actual `{col_name}`: {e}"));
        let expected: serde_json::Value = serde_json::from_str(expected)
            .unwrap_or_else(|e| panic!("parse expected `{col_name}`: {e}"));
        assert_eq!(
            actual, expected,
            "latest-row `{col_name}` JSON should round-trip"
        );
    }
}

#[tokio::test]
async fn test_missing_base_table_for_system_table_errors() {
    let (ctx, _catalog, _tmp) = create_context().await;
    let sql = "SELECT * FROM paimon.default.does_not_exist$options";
    let err = ctx
        .sql(sql)
        .await
        .expect_err("missing base table should error");
    let msg = err.to_string();
    assert!(
        msg.contains("does_not_exist") && msg.contains("$options"),
        "expected error to mention both base table and system name, got: {msg}"
    );
}

#[tokio::test]
async fn test_snapshots_system_table() {
    let (ctx, catalog, _tmp) = create_context().await;
    let sql = format!("SELECT * FROM paimon.default.{FIXTURE_TABLE}$snapshots");
    let batches = run_sql(&ctx, &sql).await;

    assert!(!batches.is_empty(), "$snapshots should return ≥1 batch");

    let arrow_schema = batches[0].schema();
    let expected_columns = [
        ("snapshot_id", DataType::Int64),
        ("schema_id", DataType::Int64),
        ("commit_user", DataType::Utf8),
        ("commit_identifier", DataType::Int64),
        ("commit_kind", DataType::Utf8),
        (
            "commit_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
        ),
        ("base_manifest_list", DataType::Utf8),
        ("delta_manifest_list", DataType::Utf8),
        ("changelog_manifest_list", DataType::Utf8),
        ("total_record_count", DataType::Int64),
        ("delta_record_count", DataType::Int64),
        ("changelog_record_count", DataType::Int64),
        ("watermark", DataType::Int64),
        ("next_row_id", DataType::Int64),
    ];
    for (i, (name, dtype)) in expected_columns.iter().enumerate() {
        let field = arrow_schema.field(i);
        assert_eq!(field.name(), name, "column {i} name");
        assert_eq!(field.data_type(), dtype, "column {i} type");
    }

    // Row count must match the snapshot directory listing.
    let identifier = Identifier::new("default".to_string(), FIXTURE_TABLE.to_string());
    let table = catalog
        .get_table(&identifier)
        .await
        .expect("fixture table should load");
    let sm =
        paimon::table::SnapshotManager::new(table.file_io().clone(), table.location().to_string());
    let all = sm.list_all().await.expect("list_all should succeed");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows,
        all.len(),
        "$snapshots rows should match list_all() length"
    );

    // snapshot_id column must be ascending.
    let mut ids: Vec<i64> = Vec::new();
    for batch in &batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("snapshot_id is Int64");
        for i in 0..batch.num_rows() {
            ids.push(col.value(i));
        }
    }
    let mut sorted = ids.clone();
    sorted.sort_unstable();
    assert_eq!(ids, sorted, "snapshot_id should be ascending");

    // commit_kind column must contain a known variant.
    let last_batch = batches.last().unwrap();
    let kind_col = last_batch
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("commit_kind is Utf8");
    let kind = kind_col.value(last_batch.num_rows() - 1);
    assert!(
        ["APPEND", "COMPACT", "OVERWRITE", "ANALYZE"].contains(&kind),
        "unexpected commit_kind: {kind}"
    );
}

#[tokio::test]
async fn test_branches_system_table_empty_when_no_branch_dir() {
    let (ctx, _catalog, _tmp) = create_context().await;
    let sql = format!("SELECT * FROM paimon.default.{FIXTURE_TABLE}$branches");
    let batches = run_sql(&ctx, &sql).await;

    // Schema must be present even with zero rows.
    assert!(!batches.is_empty(), "$branches should return ≥1 batch");
    let arrow_schema = batches[0].schema();
    let expected_columns = [
        ("branch_name", DataType::Utf8),
        (
            "create_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
        ),
    ];
    for (i, (name, dtype)) in expected_columns.iter().enumerate() {
        let field = arrow_schema.field(i);
        assert_eq!(field.name(), name, "column {i} name");
        assert_eq!(field.data_type(), dtype, "column {i} type");
    }
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0, "fixture has no branch dir, expected 0 rows");
}

#[tokio::test]
async fn test_branches_system_table_with_seeded_branches() {
    let (ctx, _catalog, tmp) = create_context().await;

    let table_dir = tmp.path().join("default.db").join(FIXTURE_TABLE);
    let branch_dir = table_dir.join("branch");
    std::fs::create_dir_all(&branch_dir).expect("create branch dir");
    std::fs::create_dir_all(branch_dir.join("branch-b1")).unwrap();
    std::fs::create_dir_all(branch_dir.join("branch-b2")).unwrap();

    let sql = format!("SELECT branch_name FROM paimon.default.{FIXTURE_TABLE}$branches");
    let batches = run_sql(&ctx, &sql).await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "expected two seeded branches");

    let mut names: Vec<String> = Vec::new();
    for batch in &batches {
        let names_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("branch_name is Utf8");
        for i in 0..batch.num_rows() {
            names.push(names_col.value(i).to_string());
        }
    }
    let mut sorted_names = names.clone();
    sorted_names.sort();
    assert_eq!(names, sorted_names, "branch_name should be ascending");
    assert_eq!(names, vec!["b1".to_string(), "b2".to_string()]);
}

#[tokio::test]
async fn test_tags_system_table_empty_when_no_tag_dir() {
    let (ctx, _catalog, _tmp) = create_context().await;
    let sql = format!("SELECT * FROM paimon.default.{FIXTURE_TABLE}$tags");
    let batches = run_sql(&ctx, &sql).await;

    // Schema must be present even with zero rows.
    assert!(!batches.is_empty(), "$tags should return ≥1 batch");
    let arrow_schema = batches[0].schema();
    let expected_columns = [
        ("tag_name", DataType::Utf8),
        ("snapshot_id", DataType::Int64),
        ("schema_id", DataType::Int64),
        (
            "commit_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
        ),
        ("record_count", DataType::Int64),
        (
            "create_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
        ),
        ("time_retained", DataType::Utf8),
    ];
    for (i, (name, dtype)) in expected_columns.iter().enumerate() {
        let field = arrow_schema.field(i);
        assert_eq!(field.name(), name, "column {i} name");
        assert_eq!(field.data_type(), dtype, "column {i} type");
    }
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0, "fixture has no tag dir, expected 0 rows");
}

#[tokio::test]
async fn test_tags_system_table_with_seeded_tags() {
    let (ctx, catalog, tmp) = create_context().await;

    let identifier = Identifier::new("default".to_string(), FIXTURE_TABLE.to_string());
    let table = catalog.get_table(&identifier).await.unwrap();
    let sm =
        paimon::table::SnapshotManager::new(table.file_io().clone(), table.location().to_string());
    let earliest = sm.list_all().await.unwrap().into_iter().next().unwrap();

    let table_dir = tmp.path().join("default.db").join(FIXTURE_TABLE);
    let tag_dir = table_dir.join("tag");
    std::fs::create_dir_all(&tag_dir).expect("create tag dir");
    let src = table_dir
        .join("snapshot")
        .join(format!("snapshot-{}", earliest.id()));
    std::fs::copy(&src, tag_dir.join("tag-v1")).unwrap();
    std::fs::copy(&src, tag_dir.join("tag-v2")).unwrap();

    let sql = format!("SELECT tag_name, snapshot_id FROM paimon.default.{FIXTURE_TABLE}$tags");
    let batches = run_sql(&ctx, &sql).await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "expected two seeded tags");

    let mut names: Vec<String> = Vec::new();
    let mut snap_ids: Vec<i64> = Vec::new();
    for batch in &batches {
        let names_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("tag_name is Utf8");
        let snap_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("snapshot_id is Int64");
        for i in 0..batch.num_rows() {
            names.push(names_col.value(i).to_string());
            snap_ids.push(snap_col.value(i));
        }
    }
    let mut sorted_names = names.clone();
    sorted_names.sort();
    assert_eq!(names, sorted_names, "tag_name should be ascending");
    assert_eq!(names, vec!["v1".to_string(), "v2".to_string()]);
    assert_eq!(snap_ids, vec![earliest.id(), earliest.id()]);
}

#[tokio::test]
async fn test_manifests_system_table() {
    let (ctx, catalog, _tmp) = create_context().await;
    let sql = format!("SELECT * FROM paimon.default.{FIXTURE_TABLE}$manifests");
    let batches = run_sql(&ctx, &sql).await;

    assert!(!batches.is_empty(), "$manifests should return ≥1 batch");
    let arrow_schema = batches[0].schema();
    let expected_columns = [
        ("file_name", DataType::Utf8),
        ("file_size", DataType::Int64),
        ("num_added_files", DataType::Int64),
        ("num_deleted_files", DataType::Int64),
        ("schema_id", DataType::Int64),
        ("min_partition_stats", DataType::Utf8),
        ("max_partition_stats", DataType::Utf8),
        ("min_row_id", DataType::Int64),
        ("max_row_id", DataType::Int64),
    ];
    for (i, (name, dtype)) in expected_columns.iter().enumerate() {
        let field = arrow_schema.field(i);
        assert_eq!(field.name(), name, "column {i} name");
        assert_eq!(field.data_type(), dtype, "column {i} type");
    }

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows > 0, "fixture should have at least one manifest");

    for batch in &batches {
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("file_name is Utf8");
        let sizes = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("file_size is Int64");
        for i in 0..batch.num_rows() {
            assert!(!names.value(i).is_empty(), "file_name must be non-empty");
            assert!(sizes.value(i) >= 0, "file_size must be non-negative");
        }
    }

    let identifier = Identifier::new("default".to_string(), FIXTURE_TABLE.to_string());
    let table = catalog.get_table(&identifier).await.unwrap();
    let sm =
        paimon::table::SnapshotManager::new(table.file_io().clone(), table.location().to_string());
    let latest = sm
        .get_latest_snapshot()
        .await
        .unwrap()
        .expect("fixture has snapshots");
    let mut expected = paimon::spec::ManifestList::read(
        table.file_io(),
        &sm.manifest_path(latest.base_manifest_list()),
    )
    .await
    .unwrap()
    .len();
    expected += paimon::spec::ManifestList::read(
        table.file_io(),
        &sm.manifest_path(latest.delta_manifest_list()),
    )
    .await
    .unwrap()
    .len();
    if let Some(changelog) = latest.changelog_manifest_list() {
        expected += paimon::spec::ManifestList::read(table.file_io(), &sm.manifest_path(changelog))
            .await
            .unwrap()
            .len();
    }
    assert_eq!(
        total_rows, expected,
        "$manifests rows should match base + delta + changelog manifest entries of the latest snapshot"
    );
}

#[tokio::test]
async fn test_manifests_system_table_partition_stats() {
    let (_tmp, sql_context) = common::setup_sql_context().await;
    common::exec(
        &sql_context,
        "CREATE TABLE paimon.test_db.manifest_stats (id INT, pt INT) PARTITIONED BY (pt)",
    )
    .await;
    common::exec(
        &sql_context,
        "INSERT INTO paimon.test_db.manifest_stats VALUES (1, 1), (2, 2)",
    )
    .await;

    let batches = sql_context
        .sql(
            "SELECT min_partition_stats, max_partition_stats \
             FROM paimon.test_db.manifest_stats$manifests",
        )
        .await
        .expect("$manifests query should plan")
        .collect()
        .await
        .expect("$manifests query should execute");

    let mut stats = Vec::new();
    for batch in &batches {
        let mins = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("min_partition_stats is Utf8");
        let maxs = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("max_partition_stats is Utf8");
        for row in 0..batch.num_rows() {
            stats.push((
                (!mins.is_null(row)).then(|| mins.value(row).to_string()),
                (!maxs.is_null(row)).then(|| maxs.value(row).to_string()),
            ));
        }
    }
    stats.sort();

    assert!(
        !stats.is_empty(),
        "$manifests should return partition stats"
    );

    let min_partition = stats
        .iter()
        .filter_map(|(min, _)| min.as_deref())
        .map(single_int_partition_stat)
        .min();
    let max_partition = stats
        .iter()
        .filter_map(|(_, max)| max.as_deref())
        .map(single_int_partition_stat)
        .max();

    assert_eq!(min_partition, Some(1));
    assert_eq!(max_partition, Some(2));
}

fn single_int_partition_stat(value: &str) -> i32 {
    value
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .expect("partition stats should use row cast braces")
        .parse()
        .expect("partition stats should contain one int partition value")
}
