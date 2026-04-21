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

//! Tests for COUNT(*) pushdown optimization via statistics.
//!
//! DataFusion's `aggregate_statistics` optimizer rule can replace a
//! scan+aggregate subtree with a literal projection when the source
//! reports `Precision::Exact(num_rows)` from `partition_statistics()`.
//!
//! These tests verify that:
//! - COUNT(*) pushdown succeeds when statistics are exact (no PaimonTableScan in plan)
//! - COUNT(*) pushdown fails (falls back to scanning) when statistics are inexact

mod common;

use std::sync::Arc;

use datafusion::arrow::array::Int64Array;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use paimon_datafusion::PaimonSqlHandler;

/// Creates a test handler with a table ready for inserts.
async fn setup_table(schema_sql: &str) -> (tempfile::TempDir, PaimonSqlHandler) {
    let (tmp, handler) = common::setup_handler().await;
    handler
        .sql(&format!("CREATE TABLE paimon.test_db.t {schema_sql}"))
        .await
        .expect("CREATE TABLE should succeed");
    (tmp, handler)
}

/// Creates a test handler with a partitioned table ready for inserts.
async fn setup_partitioned_table(
    columns: &str,
    partition_cols: &str,
) -> (tempfile::TempDir, PaimonSqlHandler) {
    let (tmp, handler) = common::setup_handler().await;
    handler
        .sql(&format!(
            "CREATE TABLE paimon.test_db.t ({columns}) PARTITIONED BY ({partition_cols})"
        ))
        .await
        .expect("CREATE TABLE should succeed");
    (tmp, handler)
}

/// Checks if the physical plan contains a PaimonTableScan (meaning data would be scanned).
fn plan_contains_scan(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let plan_str = displayable(plan.as_ref()).indent(true).to_string();
    plan_str.contains("PaimonTableScan")
}

/// Creates a physical plan for the given SQL and checks if COUNT was pushed down.
/// Returns Ok(plan) if pushdown succeeded (no scan in plan), Err(plan) if it fell back to scanning.
async fn verify_count_pushdown(
    handler: &PaimonSqlHandler,
    sql: &str,
) -> Result<Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>> {
    let df = handler.sql(sql).await.expect("Query should succeed");
    let plan = df
        .create_physical_plan()
        .await
        .expect("Physical plan should succeed");

    if plan_contains_scan(&plan) {
        Err(plan)
    } else {
        Ok(plan)
    }
}

/// Executes a COUNT(*) query and returns the count value.
async fn run_count_query(handler: &PaimonSqlHandler, sql: &str) -> i64 {
    let batches = handler
        .sql(sql)
        .await
        .expect("Query should succeed")
        .collect()
        .await
        .expect("Collect should succeed");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "COUNT(*) should return exactly one row");

    let count_array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("COUNT should return Int64");

    count_array.value(0)
}

// ============================================================================
// Test: COUNT(*) without filter should push down
// ============================================================================

#[tokio::test]
async fn test_count_star_no_filter_pushes_down() {
    let (_tmp, handler) = setup_table("(id INT, value INT)").await;

    handler
        .sql("INSERT INTO paimon.test_db.t VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    verify_count_pushdown(&handler, "SELECT COUNT(*) FROM paimon.test_db.t")
        .await
        .expect("COUNT(*) should push down (no filter)");

    let count = run_count_query(&handler, "SELECT COUNT(*) FROM paimon.test_db.t").await;
    assert_eq!(count, 3, "COUNT(*) should return 3");
}

// ============================================================================
// Test: COUNT(*) with data filter should NOT push down (must scan)
// ============================================================================

#[tokio::test]
async fn test_count_star_with_data_filter_does_not_push_down() {
    let (_tmp, handler) = setup_table("(id INT, value INT)").await;

    handler
        .sql("INSERT INTO paimon.test_db.t VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let plan = verify_count_pushdown(
        &handler,
        "SELECT COUNT(*) FROM paimon.test_db.t WHERE id > 1",
    )
    .await;

    assert!(
        plan.is_err(),
        "COUNT(*) with data filter should NOT push down - must scan data"
    );

    let count = run_count_query(
        &handler,
        "SELECT COUNT(*) FROM paimon.test_db.t WHERE id > 1",
    )
    .await;
    assert_eq!(count, 2, "COUNT(*) should return 2");
}

// ============================================================================
// Test: COUNT(*) with equality filter on non-partition column should NOT push down
// ============================================================================

#[tokio::test]
async fn test_count_star_with_non_partition_equality_does_not_push_down() {
    let (_tmp, handler) = setup_table("(id INT, value INT)").await;

    handler
        .sql("INSERT INTO paimon.test_db.t VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let plan = verify_count_pushdown(
        &handler,
        "SELECT COUNT(*) FROM paimon.test_db.t WHERE id = 2",
    )
    .await;

    assert!(
        plan.is_err(),
        "COUNT(*) with non-partition equality filter should NOT push down"
    );

    let count = run_count_query(
        &handler,
        "SELECT COUNT(*) FROM paimon.test_db.t WHERE id = 2",
    )
    .await;
    assert_eq!(count, 1, "COUNT(*) should return 1");
}

// ============================================================================
// Test: COUNT(*) on table with single row should push down
// ============================================================================

#[tokio::test]
async fn test_count_star_single_row_pushes_down() {
    let (_tmp, handler) = setup_table("(id INT, value INT)").await;

    handler
        .sql("INSERT INTO paimon.test_db.t VALUES (1, 10)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    verify_count_pushdown(&handler, "SELECT COUNT(*) FROM paimon.test_db.t")
        .await
        .expect("COUNT(*) should push down");

    let count = run_count_query(&handler, "SELECT COUNT(*) FROM paimon.test_db.t").await;
    assert_eq!(count, 1, "COUNT(*) should return 1");
}

// ============================================================================
// Test: COUNT(*) on empty table should push down (return 0)
// ============================================================================

#[tokio::test]
async fn test_count_star_empty_table_pushes_down() {
    let (_tmp, handler) = setup_table("(id INT, value INT)").await;

    verify_count_pushdown(&handler, "SELECT COUNT(*) FROM paimon.test_db.t")
        .await
        .expect("COUNT(*) on empty table should push down");

    let count = run_count_query(&handler, "SELECT COUNT(*) FROM paimon.test_db.t").await;
    assert_eq!(count, 0, "COUNT(*) should return 0");
}

// ============================================================================
// Test: COUNT(*) with exact partition filter should push down
// ============================================================================

#[tokio::test]
async fn test_count_star_with_partition_filter_pushes_down() {
    let (_tmp, handler) = setup_partitioned_table("id INT, value INT, dt STRING", "dt").await;

    handler
        .sql("INSERT INTO paimon.test_db.t VALUES (1, 10, '2024-01-01'), (2, 20, '2024-01-01'), (3, 30, '2024-01-02')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    verify_count_pushdown(
        &handler,
        "SELECT COUNT(*) FROM paimon.test_db.t WHERE dt = '2024-01-01'",
    )
    .await
    .expect("COUNT(*) with partition filter should push down");

    let count = run_count_query(
        &handler,
        "SELECT COUNT(*) FROM paimon.test_db.t WHERE dt = '2024-01-01'",
    )
    .await;
    assert_eq!(count, 2, "COUNT(*) should return 2");
}

// ============================================================================
// Test: COUNT(*) with mixed partition + data filter should NOT push down
// ============================================================================

#[tokio::test]
async fn test_count_star_with_mixed_partition_data_filter_does_not_push_down() {
    let (_tmp, handler) = setup_partitioned_table("id INT, value INT, dt STRING", "dt").await;

    handler
        .sql("INSERT INTO paimon.test_db.t VALUES (1, 10, '2024-01-01'), (2, 20, '2024-01-01'), (3, 30, '2024-01-02')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let plan = verify_count_pushdown(
        &handler,
        "SELECT COUNT(*) FROM paimon.test_db.t WHERE dt = '2024-01-01' AND value > 15",
    )
    .await;

    assert!(
        plan.is_err(),
        "COUNT(*) with mixed partition + data filter should NOT push down"
    );

    let count = run_count_query(
        &handler,
        "SELECT COUNT(*) FROM paimon.test_db.t WHERE dt = '2024-01-01' AND value > 15",
    )
    .await;
    assert_eq!(count, 1, "COUNT(*) should return 1");
}

// ============================================================================
// Test: COUNT(*) with partition IN filter should push down
// ============================================================================

#[tokio::test]
async fn test_count_star_with_partition_in_filter_pushes_down() {
    let (_tmp, handler) = setup_partitioned_table("id INT, value INT, dt STRING", "dt").await;

    handler
        .sql("INSERT INTO paimon.test_db.t VALUES (1, 10, '2024-01-01'), (2, 20, '2024-01-02'), (3, 30, '2024-01-03')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    verify_count_pushdown(
        &handler,
        "SELECT COUNT(*) FROM paimon.test_db.t WHERE dt IN ('2024-01-01', '2024-01-02')",
    )
    .await
    .expect("COUNT(*) with partition IN filter should push down");

    let count = run_count_query(
        &handler,
        "SELECT COUNT(*) FROM paimon.test_db.t WHERE dt IN ('2024-01-01', '2024-01-02')",
    )
    .await;
    assert_eq!(count, 2, "COUNT(*) should return 2");
}

// ============================================================================
// Test: COUNT(*) on partitioned table without filter should push down
// ============================================================================

#[tokio::test]
async fn test_count_star_partitioned_no_filter_pushes_down() {
    let (_tmp, handler) = setup_partitioned_table("id INT, value INT, dt STRING", "dt").await;

    handler
        .sql("INSERT INTO paimon.test_db.t VALUES (1, 10, '2024-01-01'), (2, 20, '2024-01-02')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    verify_count_pushdown(&handler, "SELECT COUNT(*) FROM paimon.test_db.t")
        .await
        .expect("COUNT(*) without filter should push down");

    let count = run_count_query(&handler, "SELECT COUNT(*) FROM paimon.test_db.t").await;
    assert_eq!(count, 2, "COUNT(*) should return 2");
}
