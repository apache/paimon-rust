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

#![cfg(feature = "mosaic")]

//! Mosaic file format read compatibility tests.

use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, Int64Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use paimon::{Catalog, CatalogOptions, FileSystemCatalog, Options};
use paimon_datafusion::SQLContext;

const FIXTURE_TABLE: &str = "test_mosaic_read";

fn extract_test_warehouse() -> (tempfile::TempDir, String) {
    let archive_path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/test_mosaic_read.tar.gz");
    let file = std::fs::File::open(&archive_path)
        .unwrap_or_else(|e| panic!("Failed to open {}: {e}", archive_path.display()));
    let decoder = flate2::read::GzDecoder::new(file);
    let mut archive = tar::Archive::new(decoder);

    let tmp = tempfile::tempdir().expect("Failed to create temp dir");
    let db_dir = tmp.path().join("default.db");
    std::fs::create_dir_all(&db_dir).expect("Failed to create default database dir");
    archive.unpack(&db_dir).expect("Failed to extract fixture");

    let warehouse = format!("file://{}", tmp.path().display());
    (tmp, warehouse)
}

async fn create_context() -> (tempfile::TempDir, SQLContext) {
    let (tmp, warehouse) = extract_test_warehouse();
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse);
    let catalog = FileSystemCatalog::new(options).expect("Failed to create catalog");
    let catalog: Arc<dyn Catalog> = Arc::new(catalog);

    let mut ctx = SQLContext::new();
    ctx.register_catalog("paimon", catalog)
        .await
        .expect("Failed to register catalog");
    (tmp, ctx)
}

async fn run_sql(ctx: &SQLContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql)
        .await
        .unwrap_or_else(|e| panic!("Failed to plan `{sql}`: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("Failed to execute `{sql}`: {e}"))
}

fn collect_id_name_score(batches: &[RecordBatch]) -> Vec<(i32, String, i64)> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column_by_name("id")
            .and_then(|column| column.as_any().downcast_ref::<Int32Array>())
            .expect("id column");
        let names = batch
            .column_by_name("name")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .expect("name column");
        let scores = batch
            .column_by_name("score")
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .expect("score column");

        for row in 0..batch.num_rows() {
            rows.push((
                ids.value(row),
                names.value(row).to_string(),
                scores.value(row),
            ));
        }
    }
    rows
}

fn collect_name_id(batches: &[RecordBatch]) -> Vec<(String, i32)> {
    let mut rows = Vec::new();
    for batch in batches {
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("first column should be name");
        let ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("second column should be id");

        for row in 0..batch.num_rows() {
            rows.push((names.value(row).to_string(), ids.value(row)));
        }
    }
    rows
}

fn contains_mosaic_file(path: &Path) -> bool {
    let entries = std::fs::read_dir(path)
        .unwrap_or_else(|e| panic!("Failed to read {}: {e}", path.display()));
    for entry in entries {
        let path = entry.expect("Failed to read dir entry").path();
        if path.is_dir() {
            if contains_mosaic_file(&path) {
                return true;
            }
        } else if path
            .extension()
            .is_some_and(|extension| extension == "mosaic")
        {
            return true;
        }
    }
    false
}

#[tokio::test]
async fn test_read_pypaimon_mosaic_fixture() {
    let (tmp, ctx) = create_context().await;
    assert!(
        contains_mosaic_file(tmp.path()),
        "expected fixture to contain a Mosaic data file"
    );

    let rows = collect_id_name_score(
        &run_sql(
            &ctx,
            &format!("SELECT id, name, score FROM paimon.default.{FIXTURE_TABLE} ORDER BY id"),
        )
        .await,
    );
    assert_eq!(
        rows,
        vec![
            (1, "Alice".to_string(), 10),
            (2, "Bob".to_string(), 20),
            (3, "Carol".to_string(), 30),
        ]
    );

    let projection_rows = collect_name_id(
        &run_sql(
            &ctx,
            &format!("SELECT name, id FROM paimon.default.{FIXTURE_TABLE} ORDER BY id"),
        )
        .await,
    );
    assert_eq!(
        projection_rows,
        vec![
            ("Alice".to_string(), 1),
            ("Bob".to_string(), 2),
            ("Carol".to_string(), 3),
        ]
    );

    let filtered_rows = collect_id_name_score(
        &run_sql(
            &ctx,
            &format!("SELECT id, name, score FROM paimon.default.{FIXTURE_TABLE} WHERE id = 2"),
        )
        .await,
    );
    assert_eq!(filtered_rows, vec![(2, "Bob".to_string(), 20)]);
}
