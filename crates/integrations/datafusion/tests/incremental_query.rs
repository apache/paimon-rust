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

//! Integration tests for the `paimon_incremental_query` table function.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use paimon::{CatalogOptions, FileSystemCatalog, Options};
use paimon_datafusion::SQLContext;
use tempfile::TempDir;

async fn run(ctx: &SQLContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql).await.unwrap().collect().await.unwrap()
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

/// Append-only table with one row per snapshot: snapshot 1 = (1,'a'),
/// snapshot 2 = (2,'b'), snapshot 3 = (3,'c').
async fn setup_three_snapshots() -> (TempDir, SQLContext) {
    let temp_dir = TempDir::new().expect("temp dir");
    let warehouse = format!("file://{}", temp_dir.path().display());
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse);
    let catalog = Arc::new(FileSystemCatalog::new(options).unwrap());
    let mut ctx = SQLContext::new();
    ctx.register_catalog("paimon", catalog).await.unwrap();

    run(&ctx, "CREATE TABLE paimon.default.t (id INT, name STRING)").await;
    run(&ctx, "INSERT INTO paimon.default.t VALUES (1, 'a')").await;
    run(&ctx, "INSERT INTO paimon.default.t VALUES (2, 'b')").await;
    run(&ctx, "INSERT INTO paimon.default.t VALUES (3, 'c')").await;
    (temp_dir, ctx)
}
/// Collects the `id` column across batches into a sorted vector.
fn sorted_ids(batches: &[RecordBatch]) -> Vec<i32> {
    use datafusion::arrow::array::{Array, Int32Array};
    let mut ids = Vec::new();
    for batch in batches {
        let col = batch
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id is Int32");
        for i in 0..col.len() {
            ids.push(col.value(i));
        }
    }
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn test_auto_range_is_start_exclusive_end_inclusive() {
    let (_tmp, ctx) = setup_three_snapshots().await;

    // (1, 3]: snapshots 2 and 3 only — start is exclusive, end inclusive.
    let batches = run(
        &ctx,
        "SELECT * FROM paimon_incremental_query('default.t', 1, 3)",
    )
    .await;
    assert_eq!(total_rows(&batches), 2);
    assert_eq!(sorted_ids(&batches), vec![2, 3]);

    // (0, 3]: every snapshot.
    let all = run(
        &ctx,
        "SELECT * FROM paimon_incremental_query('default.t', 0, 3)",
    )
    .await;
    assert_eq!(sorted_ids(&all), vec![1, 2, 3]);

    // (3, 3]: empty range.
    let empty = run(
        &ctx,
        "SELECT * FROM paimon_incremental_query('default.t', 3, 3)",
    )
    .await;
    assert_eq!(total_rows(&empty), 0);
}

#[tokio::test]
async fn test_explicit_delta_mode_matches_auto() {
    let (_tmp, ctx) = setup_three_snapshots().await;
    let batches = run(
        &ctx,
        "SELECT * FROM paimon_incremental_query('default.t', 0, 2, 'delta')",
    )
    .await;
    assert_eq!(sorted_ids(&batches), vec![1, 2]);
}

#[tokio::test]
async fn test_projection_selects_single_column() {
    let (_tmp, ctx) = setup_three_snapshots().await;
    let batches = run(
        &ctx,
        "SELECT id FROM paimon_incremental_query('default.t', 0, 3) ORDER BY id",
    )
    .await;
    assert_eq!(batches[0].num_columns(), 1);
    assert_eq!(sorted_ids(&batches), vec![1, 2, 3]);
}
#[tokio::test]
async fn test_audit_log_suffix_prepends_rowkind() {
    use datafusion::arrow::array::{Array, StringArray};
    let (_tmp, ctx) = setup_three_snapshots().await;
    let batches = run(
        &ctx,
        "SELECT * FROM paimon_incremental_query('default.t$audit_log', 0, 3, 'delta')",
    )
    .await;
    assert_eq!(total_rows(&batches), 3);
    let batch = &batches[0];
    assert_eq!(
        batch.schema().field(0).name(),
        "rowkind",
        "audit-log output must lead with the rowkind column"
    );
    let rowkind = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("rowkind is Utf8");
    // Append-only rows are all inserts.
    for i in 0..rowkind.len() {
        assert_eq!(rowkind.value(i), "+I");
    }
}

async fn expect_error(ctx: &SQLContext, sql: &str) -> String {
    match ctx.sql(sql).await {
        Err(e) => e.to_string(),
        Ok(df) => df
            .collect()
            .await
            .expect_err("expected the query to fail")
            .to_string(),
    }
}

#[tokio::test]
async fn test_rejects_unknown_mode() {
    let (_tmp, ctx) = setup_three_snapshots().await;
    let err = expect_error(
        &ctx,
        "SELECT * FROM paimon_incremental_query('default.t', 0, 1, 'bogus')",
    )
    .await;
    assert!(
        err.contains("unknown scan mode"),
        "error must name the bad mode: {err}"
    );
}

#[tokio::test]
async fn test_rejects_bad_arity() {
    let (_tmp, ctx) = setup_three_snapshots().await;
    let err = expect_error(
        &ctx,
        "SELECT * FROM paimon_incremental_query('default.t', 0)",
    )
    .await;
    assert!(
        err.contains("requires 3 or 4 arguments"),
        "error must explain the arity: {err}"
    );
}

#[tokio::test]
async fn test_rejects_end_before_start() {
    let (_tmp, ctx) = setup_three_snapshots().await;
    let err = expect_error(
        &ctx,
        "SELECT * FROM paimon_incremental_query('default.t', 5, 2)",
    )
    .await;
    assert!(
        err.contains("must be >="),
        "error must explain the range order: {err}"
    );
}
