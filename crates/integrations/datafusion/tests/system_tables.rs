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

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use paimon::catalog::Identifier;
use paimon::{Catalog, CatalogOptions, FileSystemCatalog, Options};
use paimon_datafusion::PaimonCatalogProvider;

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

async fn create_context() -> (SessionContext, Arc<dyn Catalog>, tempfile::TempDir) {
    let (tmp, warehouse) = extract_test_warehouse();
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse);
    let catalog = FileSystemCatalog::new(options).expect("Failed to create catalog");
    let catalog: Arc<dyn Catalog> = Arc::new(catalog);

    let ctx = SessionContext::new();
    ctx.register_catalog(
        "paimon",
        Arc::new(PaimonCatalogProvider::new(Arc::clone(&catalog))),
    );
    (ctx, catalog, tmp)
}

async fn run_sql(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
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
