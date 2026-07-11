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

//! Integration tests for `paimon_incremental_query` TVF.
//!
//! Uses pure Rust `changelog-producer=input` tables (no compact / lookup).

mod common;

use std::sync::Arc;

use arrow_array::{Array, Int32Array, Int8Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use datafusion::arrow::array::StringArray;
use paimon::catalog::Identifier;
use paimon::spec::VALUE_KIND_FIELD_NAME;
use paimon::{Catalog, FileSystemCatalog};

async fn setup_table_with_three_snapshots(sql: &paimon_datafusion::SQLContext) {
    common::exec(
        sql,
        "CREATE TABLE paimon.test_db.inc_t (
            id INT NOT NULL,
            value INT,
            PRIMARY KEY (id)
        ) WITH ('bucket' = '1', 'changelog-producer' = 'input')",
    )
    .await;
    common::exec(sql, "INSERT INTO paimon.test_db.inc_t VALUES (1, 10)").await;
    common::exec(sql, "INSERT INTO paimon.test_db.inc_t VALUES (2, 20)").await;
    common::exec(sql, "INSERT INTO paimon.test_db.inc_t VALUES (3, 30)").await;
}

fn collect_id_value(batches: &[datafusion::arrow::record_batch::RecordBatch]) -> Vec<(i32, i32)> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("id column");
        let values = batch
            .column_by_name("value")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("value column");
        for row in 0..batch.num_rows() {
            rows.push((ids.value(row), values.value(row)));
        }
    }
    rows.sort_unstable();
    rows
}

fn collect_audit_rows(
    batches: &[datafusion::arrow::record_batch::RecordBatch],
) -> Vec<(String, i32, i32)> {
    let mut rows = Vec::new();
    for batch in batches {
        let kinds = batch
            .column_by_name("rowkind")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("rowkind");
        let ids = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("id");
        let values = batch
            .column_by_name("value")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("value");
        for row in 0..batch.num_rows() {
            rows.push((
                kinds.value(row).to_string(),
                ids.value(row),
                values.value(row),
            ));
        }
    }
    rows.sort_unstable();
    rows
}

/// Commit a single-row input-changelog delete (`RowKind::Delete` = 3).
async fn commit_input_changelog_delete(
    catalog: &Arc<FileSystemCatalog>,
    table_name: &str,
    id: i32,
) {
    let table = catalog
        .get_table(&Identifier::new("test_db", table_name))
        .await
        .expect("table should exist");
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new(VALUE_KIND_FIELD_NAME, ArrowDataType::Int8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![id])),
            Arc::new(Int8Array::from(vec![3])), // RowKind::Delete
        ],
    )
    .unwrap();
    let wb = table.new_write_builder();
    let mut writer = wb.new_write().expect("writer");
    writer
        .write_arrow_batch(&batch)
        .await
        .expect("write changelog delete");
    let messages = writer.prepare_commit().await.expect("prepare");
    wb.new_commit()
        .commit(messages)
        .await
        .expect("commit changelog delete");
}

/// 3-arg form defaults to Auto (input producer → changelog semantics).
#[tokio::test]
async fn incremental_query_without_audit_log_returns_delta_rows() {
    let (_tmp, ctx) = common::setup_sql_context().await;
    setup_table_with_three_snapshots(&ctx).await;

    let batches = ctx
        .sql("SELECT id, value FROM paimon_incremental_query('paimon.test_db.inc_t', 0, 2)")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("execute");

    assert_eq!(collect_id_value(&batches), vec![(1, 10), (2, 20)]);
}

#[tokio::test]
async fn incremental_query_with_audit_log_suffix_exposes_rowkind() {
    let (_tmp, ctx) = common::setup_sql_context().await;
    setup_table_with_three_snapshots(&ctx).await;

    let batches = ctx
        .sql(
            "SELECT rowkind, id, value \
             FROM paimon_incremental_query('paimon.test_db.inc_t$audit_log', 0, 1)",
        )
        .await
        .expect("plan")
        .collect()
        .await
        .expect("execute");

    let batch = &batches[0];
    assert_eq!(batch.schema().field(0).name(), "rowkind");
    let kinds = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(kinds.iter().all(|k| k == Some("+I")));
}

#[tokio::test]
async fn incremental_query_explicit_auto_and_changelog_match_default() {
    let (_tmp, ctx) = common::setup_sql_context().await;
    setup_table_with_three_snapshots(&ctx).await;

    let default_rows = ctx
        .sql("SELECT id, value FROM paimon_incremental_query('paimon.test_db.inc_t', 0, 1)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let auto_rows = ctx
        .sql("SELECT id, value FROM paimon_incremental_query('paimon.test_db.inc_t', 0, 1, 'auto')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let changelog_rows = ctx
        .sql(
            "SELECT id, value FROM paimon_incremental_query('paimon.test_db.inc_t', 0, 1, 'changelog')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(
        collect_id_value(&default_rows),
        collect_id_value(&auto_rows)
    );
    assert_eq!(
        collect_id_value(&default_rows),
        collect_id_value(&changelog_rows)
    );
}

#[tokio::test]
async fn incremental_query_explicit_delta_mode() {
    let (_tmp, ctx) = common::setup_sql_context().await;
    setup_table_with_three_snapshots(&ctx).await;

    let batches = ctx
        .sql(
            "SELECT id, value FROM paimon_incremental_query('paimon.test_db.inc_t', 0, 2, 'delta')",
        )
        .await
        .expect("plan")
        .collect()
        .await
        .expect("execute");

    // Delta reads data files from APPEND snapshots in (0, 2] → rows from snapshots 1 and 2.
    assert_eq!(collect_id_value(&batches), vec![(1, 10), (2, 20)]);
}

#[tokio::test]
async fn incremental_query_diff_mode_on_pk_table_with_audit_log() {
    let (_tmp, ctx) = common::setup_sql_context().await;
    common::exec(
        &ctx,
        "CREATE TABLE paimon.test_db.inc_pk (
            id INT NOT NULL,
            value INT,
            PRIMARY KEY (id)
        ) WITH ('bucket' = '1', 'merge-engine' = 'deduplicate')",
    )
    .await;
    common::exec(&ctx, "INSERT INTO paimon.test_db.inc_pk VALUES (1, 10)").await;
    common::exec(&ctx, "INSERT INTO paimon.test_db.inc_pk VALUES (2, 20)").await;
    // PK deduplicate merge treats a later insert with the same key as an update.
    common::exec(&ctx, "INSERT INTO paimon.test_db.inc_pk VALUES (1, 11)").await;

    let batches = ctx
        .sql(
            "SELECT rowkind, id, value \
             FROM paimon_incremental_query('paimon.test_db.inc_pk$audit_log', 1, 3, 'diff')",
        )
        .await
        .expect("plan")
        .collect()
        .await
        .expect("execute");

    let rows = collect_audit_rows(&batches);
    assert!(
        rows.iter()
            .any(|(k, id, v)| k == "-U" && *id == 1 && *v == 10),
        "diff audit_log should include -U before image, got {rows:?}"
    );
    assert!(
        rows.iter()
            .any(|(k, id, v)| k == "+U" && *id == 1 && *v == 11),
        "diff audit_log should include +U after image, got {rows:?}"
    );
}

#[tokio::test]
async fn incremental_query_rejects_invalid_scan_mode() {
    let (_tmp, ctx) = common::setup_sql_context().await;
    setup_table_with_three_snapshots(&ctx).await;

    let err = ctx
        .sql("SELECT * FROM paimon_incremental_query('paimon.test_db.inc_t', 0, 1, 'nope')")
        .await
        .expect_err("invalid scan_mode should fail at planning");
    let msg = err.to_string();
    assert!(
        msg.to_lowercase().contains("scan_mode") || msg.contains("nope"),
        "unexpected error: {msg}"
    );
}

#[tokio::test]
async fn incremental_query_rejects_illegal_snapshot_range() {
    let (_tmp, ctx) = common::setup_sql_context().await;
    setup_table_with_three_snapshots(&ctx).await;

    let err = ctx
        .sql("SELECT * FROM paimon_incremental_query('paimon.test_db.inc_t', 0, 99)")
        .await
        .expect("plan may succeed; execution/planning of scan fails")
        .collect()
        .await
        .expect_err("out-of-range end snapshot should fail");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("snapshot") || msg.contains("99") || msg.contains("range"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn incremental_query_supports_projection_and_filter() {
    let (_tmp, ctx) = common::setup_sql_context().await;
    setup_table_with_three_snapshots(&ctx).await;

    let batches = ctx
        .sql("SELECT id FROM paimon_incremental_query('paimon.test_db.inc_t', 0, 2) WHERE id = 2")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("execute");

    let mut ids = Vec::new();
    for batch in &batches {
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "id");
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            ids.push(col.value(row));
        }
    }
    assert_eq!(ids, vec![2]);
}

#[tokio::test]
async fn incremental_query_audit_log_sees_delete_rowkind() {
    let (tmp, catalog) = common::create_test_env();
    let ctx = common::create_sql_context(Arc::clone(&catalog)).await;
    common::exec(&ctx, "CREATE SCHEMA paimon.test_db").await;

    common::exec(
        &ctx,
        "CREATE TABLE paimon.test_db.inc_del (
            id INT NOT NULL,
            PRIMARY KEY (id)
        ) WITH ('bucket' = '1', 'changelog-producer' = 'input')",
    )
    .await;
    common::exec(&ctx, "INSERT INTO paimon.test_db.inc_del VALUES (1)").await;
    common::exec(&ctx, "INSERT INTO paimon.test_db.inc_del VALUES (2)").await;
    commit_input_changelog_delete(&catalog, "inc_del", 1).await;
    let _ = tmp;

    let batches = ctx
        .sql(
            "SELECT rowkind, id FROM paimon_incremental_query('paimon.test_db.inc_del$audit_log', 1, 3)",
        )
        .await
        .expect("plan")
        .collect()
        .await
        .expect("execute");

    let mut kinds = Vec::new();
    for batch in &batches {
        let kind_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let id_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            kinds.push((kind_col.value(row).to_string(), id_col.value(row)));
        }
    }
    kinds.sort_unstable();
    assert!(
        kinds.iter().any(|(k, id)| k == "-D" && *id == 1),
        "audit_log incremental query should expose -D, got {kinds:?}"
    );
}
