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

//! MERGE INTO integration tests for data evolution tables.
//!
//! Covers row tracking, `_ROW_ID` stability, multiple merges, self-merge,
//! join on `_ROW_ID`, and error path validation.
//! Reference: Java Paimon's `RowTrackingTestBase`.

use std::sync::Arc;

use arrow_array::{Int32Array, Int64Array, StringArray};
use paimon::catalog::Identifier;
use paimon::table::SnapshotManager;
use paimon::{Catalog, CatalogOptions, FileSystemCatalog, Options};
use paimon_datafusion::SQLContext;
use tempfile::TempDir;

// ======================= Helpers =======================

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

async fn setup_data_evolution_table(sql_context: &SQLContext) {
    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA failed");
    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.target (\
                id INT NOT NULL, name STRING, value INT\
            ) WITH (\
                'row-tracking.enabled' = 'true'\
            )",
        )
        .await
        .expect("CREATE TABLE failed");
}

async fn enable_data_evolution(sql_context: &SQLContext) {
    sql_context
        .sql("ALTER TABLE paimon.test_db.target SET TBLPROPERTIES('data-evolution.enabled' = 'true')")
        .await
        .expect("ALTER TABLE failed");
}

async fn collect_rows_3col(sql_context: &SQLContext, sql: &str) -> Vec<(i32, String, i32)> {
    let batches = sql_context.sql(sql).await.unwrap().collect().await.unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
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
        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((ids.value(i), names.value(i).to_string(), values.value(i)));
        }
    }
    rows
}

async fn collect_row_ids(sql_context: &SQLContext, sql: &str) -> Vec<(i64, i32)> {
    let batches = sql_context.sql(sql).await.unwrap().collect().await.unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let row_ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((row_ids.value(i), ids.value(i)));
        }
    }
    rows
}

async fn assert_merge_error(sql_context: &SQLContext, sql: &str, expected_substring: &str) {
    let result = sql_context.sql(sql).await;
    assert!(
        result.is_err(),
        "Expected error containing '{expected_substring}', but got Ok"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains(expected_substring),
        "Error message '{err_msg}' does not contain '{expected_substring}'"
    );
}

async fn register_source(sql_context: &SQLContext, sql: &str) {
    sql_context.sql(sql).await.unwrap().collect().await.unwrap();
}

// ======================= Functional Tests =======================

#[tokio::test]
async fn test_row_id_values_after_insert() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;
    setup_data_evolution_table(&sql_context).await;

    sql_context
        .sql("INSERT INTO paimon.test_db.target (id, name, value) VALUES (1, 'alice', 10), (2, 'bob', 20), (3, 'charlie', 30)")
        .await.unwrap().collect().await.unwrap();

    enable_data_evolution(&sql_context).await;

    let batches = sql_context
        .sql("SELECT \"_ROW_ID\" FROM paimon.test_db.target ORDER BY \"_ROW_ID\"")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let mut row_ids = Vec::new();
    for batch in &batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            row_ids.push(col.value(i));
        }
    }

    assert_eq!(
        row_ids,
        vec![0, 1, 2],
        "_ROW_ID should be 0-based sequential"
    );
    // Verify uniqueness
    let mut deduped = row_ids.clone();
    deduped.sort();
    deduped.dedup();
    assert_eq!(
        deduped.len(),
        row_ids.len(),
        "_ROW_ID values must be unique"
    );
}

#[tokio::test]
async fn test_row_id_stability_after_merge_into() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;
    setup_data_evolution_table(&sql_context).await;

    sql_context
        .sql("INSERT INTO paimon.test_db.target (id, name, value) VALUES (1, 'alice', 10), (2, 'bob', 20), (3, 'charlie', 30)")
        .await.unwrap().collect().await.unwrap();

    enable_data_evolution(&sql_context).await;

    // Capture _ROW_ID -> id mapping before merge
    let before = collect_row_ids(
        &sql_context,
        "SELECT \"_ROW_ID\", id FROM paimon.test_db.target ORDER BY id",
    )
    .await;

    // Register source and execute MERGE INTO
    register_source(
        &sql_context,
        "CREATE TEMPORARY TABLE paimon.test_db.source1 AS SELECT * FROM (VALUES (1, 'ALICE'), (3, 'CHARLIE')) AS t(id, name)",
    )
    .await;

    sql_context
        .sql(
            "MERGE INTO paimon.test_db.target t USING paimon.test_db.source1 s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Capture _ROW_ID -> id mapping after merge
    let after = collect_row_ids(
        &sql_context,
        "SELECT \"_ROW_ID\", id FROM paimon.test_db.target ORDER BY id",
    )
    .await;

    // _ROW_ID must be identical for all rows
    assert_eq!(
        before, after,
        "_ROW_ID values must not change after MERGE INTO"
    );

    // Verify data correctness
    let rows = collect_rows_3col(
        &sql_context,
        "SELECT id, name, value FROM paimon.test_db.target ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            (1, "ALICE".to_string(), 10),
            (2, "bob".to_string(), 20),
            (3, "CHARLIE".to_string(), 30),
        ]
    );
}

#[tokio::test]
async fn test_multiple_merge_into_different_columns() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;
    setup_data_evolution_table(&sql_context).await;

    sql_context
        .sql("INSERT INTO paimon.test_db.target (id, name, value) VALUES (1, 'alice', 10), (2, 'bob', 20)")
        .await.unwrap().collect().await.unwrap();

    enable_data_evolution(&sql_context).await;

    // First MERGE: update name for id=1
    register_source(
        &sql_context,
        "CREATE TEMPORARY TABLE paimon.test_db.src_name AS SELECT * FROM (VALUES (1, 'ALICE')) AS t(id, name)",
    )
    .await;
    sql_context
        .sql(
            "MERGE INTO paimon.test_db.target t USING paimon.test_db.src_name s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Second MERGE: update value for id=2
    register_source(
        &sql_context,
        "CREATE TEMPORARY TABLE paimon.test_db.src_value AS SELECT * FROM (VALUES (2, 200)) AS t(id, value)",
    )
    .await;
    sql_context
        .sql(
            "MERGE INTO paimon.test_db.target t USING paimon.test_db.src_value s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET value = s.value",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_rows_3col(
        &sql_context,
        "SELECT id, name, value FROM paimon.test_db.target ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![(1, "ALICE".to_string(), 10), (2, "bob".to_string(), 200),]
    );
}

#[tokio::test]
async fn test_merge_into_with_non_paimon_source() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;
    setup_data_evolution_table(&sql_context).await;

    sql_context
        .sql("INSERT INTO paimon.test_db.target (id, name, value) VALUES (1, 'alice', 10), (2, 'bob', 20)")
        .await.unwrap().collect().await.unwrap();

    enable_data_evolution(&sql_context).await;

    // Source is a plain DataFusion in-memory table, not Paimon
    register_source(
        &sql_context,
        "CREATE TEMPORARY TABLE paimon.test_db.df_source AS SELECT * FROM (VALUES (2, 'BOB_UPDATED')) AS t(id, name)",
    )
    .await;

    sql_context
        .sql(
            "MERGE INTO paimon.test_db.target t USING paimon.test_db.df_source s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_rows_3col(
        &sql_context,
        "SELECT id, name, value FROM paimon.test_db.target ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            (1, "alice".to_string(), 10),
            (2, "BOB_UPDATED".to_string(), 20),
        ]
    );
}

#[tokio::test]
async fn test_merge_into_join_on_row_id() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;
    setup_data_evolution_table(&sql_context).await;

    sql_context
        .sql("INSERT INTO paimon.test_db.target (id, name, value) VALUES (1, 'alice', 10), (2, 'bob', 20), (3, 'charlie', 30)")
        .await.unwrap().collect().await.unwrap();

    enable_data_evolution(&sql_context).await;

    // Get _ROW_ID for id=2
    let row_id_map = collect_row_ids(
        &sql_context,
        "SELECT \"_ROW_ID\", id FROM paimon.test_db.target ORDER BY id",
    )
    .await;
    let row_id_of_2 = row_id_map.iter().find(|(_, id)| *id == 2).unwrap().0;

    // Create source with that row_id
    register_source(
        &sql_context,
        &format!(
            "CREATE TEMPORARY TABLE paimon.test_db.rid_source AS SELECT * FROM (VALUES ({row_id_of_2}, 'BOB')) AS t(row_id, name)"
        ),
    )
    .await;

    sql_context
        .sql(
            "MERGE INTO paimon.test_db.target t USING paimon.test_db.rid_source s ON t.\"_ROW_ID\" = s.row_id \
             WHEN MATCHED THEN UPDATE SET name = s.name",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_rows_3col(
        &sql_context,
        "SELECT id, name, value FROM paimon.test_db.target ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            (1, "alice".to_string(), 10),
            (2, "BOB".to_string(), 20),
            (3, "charlie".to_string(), 30),
        ]
    );
}

#[tokio::test]
async fn test_self_merge() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;
    setup_data_evolution_table(&sql_context).await;

    sql_context
        .sql("INSERT INTO paimon.test_db.target (id, name, value) VALUES (1, 'alice', 10), (2, 'bob', 20)")
        .await.unwrap().collect().await.unwrap();

    enable_data_evolution(&sql_context).await;

    // Self-merge: target used as both target and source
    sql_context
        .sql(
            "MERGE INTO paimon.test_db.target t USING paimon.test_db.target s \
             ON t.\"_ROW_ID\" = s.\"_ROW_ID\" \
             WHEN MATCHED THEN UPDATE SET name = UPPER(s.name)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_rows_3col(
        &sql_context,
        "SELECT id, name, value FROM paimon.test_db.target ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![(1, "ALICE".to_string(), 10), (2, "BOB".to_string(), 20),]
    );
}

#[tokio::test]
async fn test_row_count_after_merge() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;
    setup_data_evolution_table(&sql_context).await;

    sql_context
        .sql("INSERT INTO paimon.test_db.target (id, name, value) VALUES (1, 'alice', 10), (2, 'bob', 20), (3, 'charlie', 30)")
        .await.unwrap().collect().await.unwrap();

    enable_data_evolution(&sql_context).await;

    // Snapshot 1: 3 rows inserted
    let table = catalog
        .get_table(&Identifier::new("test_db", "target"))
        .await
        .unwrap();
    let snap_mgr = SnapshotManager::new(table.file_io().clone(), table.location().to_string());
    let snap1 = snap_mgr.get_latest_snapshot().await.unwrap().unwrap();
    assert_eq!(snap1.next_row_id(), Some(3));

    // MERGE INTO: update 1 row
    register_source(
        &sql_context,
        "CREATE TEMPORARY TABLE paimon.test_db.src_count AS SELECT * FROM (VALUES (1, 'ALICE')) AS t(id, name)",
    )
    .await;
    sql_context
        .sql(
            "MERGE INTO paimon.test_db.target t USING paimon.test_db.src_count s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Snapshot 2: MERGE INTO should NOT allocate new row IDs
    let snap2 = snap_mgr.get_latest_snapshot().await.unwrap().unwrap();
    assert_eq!(
        snap2.next_row_id(),
        Some(3),
        "MERGE INTO should not allocate new row IDs"
    );
}

// ======================= Error Path Tests =======================

// ======================= WHEN NOT MATCHED THEN INSERT Tests =======================

#[tokio::test]
async fn test_merge_into_update_and_insert() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;
    setup_data_evolution_table(&sql_context).await;

    sql_context
        .sql("INSERT INTO paimon.test_db.target (id, name, value) VALUES (2, 'bob', 20), (3, 'charlie', 30)")
        .await.unwrap().collect().await.unwrap();

    enable_data_evolution(&sql_context).await;

    register_source(
        &sql_context,
        "CREATE TEMPORARY TABLE paimon.test_db.src_ui AS SELECT * FROM (VALUES (1, 'alice', 11), (2, 'BOB', 22)) AS t(id, name, value)",
    )
    .await;

    sql_context
        .sql(
            "MERGE INTO paimon.test_db.target t USING paimon.test_db.src_ui s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name \
             WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (s.id, s.name, s.value)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_rows_3col(
        &sql_context,
        "SELECT id, name, value FROM paimon.test_db.target ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            (1, "alice".to_string(), 11),   // inserted
            (2, "BOB".to_string(), 20),     // updated (name only)
            (3, "charlie".to_string(), 30), // untouched
        ]
    );
}

#[tokio::test]
async fn test_merge_into_insert_only() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;
    setup_data_evolution_table(&sql_context).await;

    sql_context
        .sql("INSERT INTO paimon.test_db.target (id, name, value) VALUES (2, 'bob', 20), (3, 'charlie', 30)")
        .await.unwrap().collect().await.unwrap();

    enable_data_evolution(&sql_context).await;

    register_source(
        &sql_context,
        "CREATE TEMPORARY TABLE paimon.test_db.src_io AS SELECT * FROM (VALUES (1, 'alice', 11), (2, 'BOB', 22)) AS t(id, name, value)",
    )
    .await;

    // Only INSERT, no MATCHED clause — matched row id=2 should be untouched
    sql_context
        .sql(
            "MERGE INTO paimon.test_db.target t USING paimon.test_db.src_io s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (s.id, s.name, s.value)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_rows_3col(
        &sql_context,
        "SELECT id, name, value FROM paimon.test_db.target ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            (1, "alice".to_string(), 11),   // inserted
            (2, "bob".to_string(), 20),     // untouched
            (3, "charlie".to_string(), 30), // untouched
        ]
    );
}

#[tokio::test]
async fn test_merge_into_insert_all_columns() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;
    setup_data_evolution_table(&sql_context).await;

    sql_context
        .sql("INSERT INTO paimon.test_db.target (id, name, value) VALUES (2, 'bob', 20)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    enable_data_evolution(&sql_context).await;

    // Source schema matches target: (id, name, value)
    register_source(
        &sql_context,
        "CREATE TEMPORARY TABLE paimon.test_db.src_star AS SELECT * FROM (VALUES (1, 'alice', 10), (2, 'BOB', 22)) AS t(id, name, value)",
    )
    .await;

    sql_context
        .sql(
            "MERGE INTO paimon.test_db.target t USING paimon.test_db.src_star s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name \
             WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (s.id, s.name, s.value)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_rows_3col(
        &sql_context,
        "SELECT id, name, value FROM paimon.test_db.target ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            (1, "alice".to_string(), 10), // inserted
            (2, "BOB".to_string(), 20),   // updated name
        ]
    );
}

#[tokio::test]
async fn test_merge_into_insert_partial_columns() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;
    setup_data_evolution_table(&sql_context).await;

    sql_context
        .sql("INSERT INTO paimon.test_db.target (id, name, value) VALUES (2, 'bob', 20)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    enable_data_evolution(&sql_context).await;

    register_source(
        &sql_context,
        "CREATE TEMPORARY TABLE paimon.test_db.src_partial AS SELECT * FROM (VALUES (1, 'alice'), (2, 'BOB')) AS t(id, name)",
    )
    .await;

    // INSERT only id and name, value should be NULL (but our schema has INT, so this tests partial insert)
    sql_context
        .sql(
            "MERGE INTO paimon.test_db.target t USING paimon.test_db.src_partial s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (s.id, s.name, 0)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_rows_3col(
        &sql_context,
        "SELECT id, name, value FROM paimon.test_db.target ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            (1, "alice".to_string(), 0), // inserted with value=0
            (2, "bob".to_string(), 20),  // untouched
        ]
    );
}

#[tokio::test]
async fn test_merge_into_insert_with_predicate() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;
    setup_data_evolution_table(&sql_context).await;

    sql_context
        .sql("INSERT INTO paimon.test_db.target (id, name, value) VALUES (1, 'alice', 10)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    enable_data_evolution(&sql_context).await;

    // Source has 3 rows, only id=1 matches target
    register_source(
        &sql_context,
        "CREATE TEMPORARY TABLE paimon.test_db.src_pred AS SELECT * FROM (VALUES (1, 'ALICE', 11), (2, 'bob', 20), (3, 'charlie', 30)) AS t(id, name, value)",
    )
    .await;

    // Only insert when value > 25
    sql_context
        .sql(
            "MERGE INTO paimon.test_db.target t USING paimon.test_db.src_pred s ON t.id = s.id \
             WHEN NOT MATCHED AND s.value > 25 THEN INSERT (id, name, value) VALUES (s.id, s.name, s.value)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_rows_3col(
        &sql_context,
        "SELECT id, name, value FROM paimon.test_db.target ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            (1, "alice".to_string(), 10), // untouched (matched but no UPDATE clause)
            (3, "charlie".to_string(), 30), // inserted (value=30 > 25)
                                          // id=2 not inserted (value=20 <= 25)
        ]
    );
}

#[tokio::test]
async fn test_merge_into_row_id_for_inserted_rows() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;
    setup_data_evolution_table(&sql_context).await;

    sql_context
        .sql("INSERT INTO paimon.test_db.target (id, name, value) VALUES (2, 'bob', 20), (3, 'charlie', 30)")
        .await.unwrap().collect().await.unwrap();

    enable_data_evolution(&sql_context).await;

    // Before merge: _ROW_ID should be 0, 1
    let before = collect_row_ids(
        &sql_context,
        "SELECT \"_ROW_ID\", id FROM paimon.test_db.target ORDER BY id",
    )
    .await;
    assert_eq!(before, vec![(0, 2), (1, 3)]);

    register_source(
        &sql_context,
        "CREATE TEMPORARY TABLE paimon.test_db.src_rid AS SELECT * FROM (VALUES (1, 'alice', 11), (2, 'BOB', 22)) AS t(id, name, value)",
    )
    .await;

    sql_context
        .sql(
            "MERGE INTO paimon.test_db.target t USING paimon.test_db.src_rid s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name \
             WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (s.id, s.name, s.value)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // After merge: existing rows keep their _ROW_ID, new row gets next available
    let after = collect_row_ids(
        &sql_context,
        "SELECT \"_ROW_ID\", id FROM paimon.test_db.target ORDER BY id",
    )
    .await;

    // id=1 is new → _ROW_ID=2 (next after 0,1)
    // id=2 updated → _ROW_ID=0 (preserved)
    // id=3 untouched → _ROW_ID=1 (preserved)
    assert_eq!(after, vec![(2, 1), (0, 2), (1, 3)]);

    // Verify next_row_id in snapshot
    let table = catalog
        .get_table(&Identifier::new("test_db", "target"))
        .await
        .unwrap();
    let snap_mgr = SnapshotManager::new(table.file_io().clone(), table.location().to_string());
    let snap = snap_mgr.get_latest_snapshot().await.unwrap().unwrap();
    assert_eq!(
        snap.next_row_id(),
        Some(3),
        "next_row_id should be 3 after inserting 1 new row"
    );
}

#[tokio::test]
async fn test_merge_into_rejects_duplicate_matched_updates() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;
    setup_data_evolution_table(&sql_context).await;

    sql_context
        .sql("INSERT INTO paimon.test_db.target (id, name, value) VALUES (1, 'alice', 10)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    enable_data_evolution(&sql_context).await;

    register_source(
        &sql_context,
        "CREATE TEMPORARY TABLE paimon.test_db.src_dup AS SELECT * FROM (VALUES (1, 'ALICE'), (1, 'ALICIA')) AS t(id, name)",
    )
    .await;

    assert_merge_error(
        &sql_context,
        "MERGE INTO paimon.test_db.target t USING paimon.test_db.src_dup s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET name = s.name",
        "duplicate UPDATE",
    )
    .await;
}

#[tokio::test]
async fn test_when_matched_delete_with_deletion_vectors() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;
    setup_data_evolution_table(&sql_context).await;

    sql_context
        .sql("INSERT INTO paimon.test_db.target (id, name, value) VALUES (1, 'alice', 10), (2, 'bob', 20)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    sql_context
        .sql(
            "ALTER TABLE paimon.test_db.target SET TBLPROPERTIES(\
                'data-evolution.enabled' = 'true',\
                'deletion-vectors.enabled' = 'true'\
            )",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    register_source(
        &sql_context,
        "CREATE TEMPORARY TABLE paimon.test_db.src_del AS SELECT * FROM (VALUES (1)) AS t(id)",
    )
    .await;

    sql_context
        .sql(
            "MERGE INTO paimon.test_db.target t USING paimon.test_db.src_del s ON t.id = s.id \
             WHEN MATCHED THEN DELETE",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_rows_3col(
        &sql_context,
        "SELECT id, name, value FROM paimon.test_db.target ORDER BY id",
    )
    .await;
    assert_eq!(rows, vec![(2, "bob".to_string(), 20)]);
}

#[tokio::test]
async fn test_rejects_multiple_when_matched() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;
    setup_data_evolution_table(&sql_context).await;

    sql_context
        .sql("INSERT INTO paimon.test_db.target (id, name, value) VALUES (1, 'alice', 10)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    enable_data_evolution(&sql_context).await;

    register_source(
        &sql_context,
        "CREATE TEMPORARY TABLE paimon.test_db.src_multi AS SELECT * FROM (VALUES (1, 'ALICE')) AS t(id, name)",
    )
    .await;

    assert_merge_error(
        &sql_context,
        "MERGE INTO paimon.test_db.target t USING paimon.test_db.src_multi s ON t.id = s.id \
         WHEN MATCHED AND t.id = 1 THEN UPDATE SET name = s.name \
         WHEN MATCHED THEN UPDATE SET name = 'default'",
        "WHEN MATCHED AND <predicate> is not yet supported",
    )
    .await;
}

#[tokio::test]
async fn test_rejects_partition_column_in_set() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .unwrap();
    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.part_target (\
                pt STRING, id INT NOT NULL, name STRING\
            ) PARTITIONED BY (pt) WITH (\
                'row-tracking.enabled' = 'true'\
            )",
        )
        .await
        .unwrap();

    sql_context
        .sql("INSERT INTO paimon.test_db.part_target (pt, id, name) VALUES ('a', 1, 'alice')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    sql_context.sql("ALTER TABLE paimon.test_db.part_target SET TBLPROPERTIES('data-evolution.enabled' = 'true')").await.unwrap();

    register_source(
        &sql_context,
        "CREATE TEMPORARY TABLE paimon.test_db.src_pt AS SELECT * FROM (VALUES (1, 'b')) AS t(id, pt)",
    )
    .await;

    assert_merge_error(
        &sql_context,
        "MERGE INTO paimon.test_db.part_target t USING paimon.test_db.src_pt s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET pt = s.pt",
        "Cannot update partition column",
    )
    .await;
}

#[tokio::test]
async fn test_rejects_table_without_row_tracking() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .unwrap();
    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.no_tracking (\
                id INT NOT NULL, name STRING\
            )",
        )
        .await
        .unwrap();

    sql_context
        .sql("INSERT INTO paimon.test_db.no_tracking (id, name) VALUES (1, 'alice')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    sql_context.sql("ALTER TABLE paimon.test_db.no_tracking SET TBLPROPERTIES('data-evolution.enabled' = 'true')").await.unwrap();

    register_source(
        &sql_context,
        "CREATE TEMPORARY TABLE paimon.test_db.src_nrt AS SELECT * FROM (VALUES (1, 'ALICE')) AS t(id, name)",
    )
    .await;

    assert_merge_error(
        &sql_context,
        "MERGE INTO paimon.test_db.no_tracking t USING paimon.test_db.src_nrt s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET name = s.name",
        "row-tracking.enabled",
    )
    .await;
}

#[tokio::test]
async fn test_successive_merges_read_file_group() {
    // Verifies that a second MERGE INTO correctly reads columns from the file group
    // (base file + partial-column files created by the first merge), not just a single file.
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;
    setup_data_evolution_table(&sql_context).await;

    sql_context
        .sql("INSERT INTO paimon.test_db.target (id, name, value) VALUES (1, 'alice', 10), (2, 'bob', 20)")
        .await.unwrap().collect().await.unwrap();

    enable_data_evolution(&sql_context).await;

    // First MERGE: update 'name' column → creates a partial-column file for 'name'
    register_source(
        &sql_context,
        "CREATE TEMPORARY TABLE paimon.test_db.src_m1 AS SELECT * FROM (VALUES (1, 'ALICE'), (2, 'BOB')) AS t(id, name)",
    )
    .await;
    sql_context
        .sql(
            "MERGE INTO paimon.test_db.target t USING paimon.test_db.src_m1 s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Verify first merge result
    let rows = collect_rows_3col(
        &sql_context,
        "SELECT id, name, value FROM paimon.test_db.target ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![(1, "ALICE".to_string(), 10), (2, "BOB".to_string(), 20),]
    );

    // Second MERGE: update 'name' again → must read the merged 'name' from file group
    // (base file has original 'name', partial file has updated 'name' from first merge)
    register_source(
        &sql_context,
        "CREATE TEMPORARY TABLE paimon.test_db.src_m2 AS SELECT * FROM (VALUES (1, 'Alice_v2')) AS t(id, name)",
    )
    .await;
    sql_context
        .sql(
            "MERGE INTO paimon.test_db.target t USING paimon.test_db.src_m2 s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_rows_3col(
        &sql_context,
        "SELECT id, name, value FROM paimon.test_db.target ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![(1, "Alice_v2".to_string(), 10), (2, "BOB".to_string(), 20),]
    );
}

#[tokio::test]
async fn test_successive_merges_different_columns_read_file_group() {
    // First merge updates 'name', second merge updates 'value'.
    // The second merge must correctly read 'value' from the file group
    // even though a partial-column file for 'name' now exists.
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;
    setup_data_evolution_table(&sql_context).await;

    sql_context
        .sql("INSERT INTO paimon.test_db.target (id, name, value) VALUES (1, 'alice', 10), (2, 'bob', 20)")
        .await.unwrap().collect().await.unwrap();

    enable_data_evolution(&sql_context).await;

    // First MERGE: update 'name'
    register_source(
        &sql_context,
        "CREATE TEMPORARY TABLE paimon.test_db.src_dc1 AS SELECT * FROM (VALUES (1, 'ALICE')) AS t(id, name)",
    )
    .await;
    sql_context
        .sql(
            "MERGE INTO paimon.test_db.target t USING paimon.test_db.src_dc1 s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Second MERGE: update 'value' — reads from file group (base + name-partial)
    register_source(
        &sql_context,
        "CREATE TEMPORARY TABLE paimon.test_db.src_dc2 AS SELECT * FROM (VALUES (1, 100), (2, 200)) AS t(id, value)",
    )
    .await;
    sql_context
        .sql(
            "MERGE INTO paimon.test_db.target t USING paimon.test_db.src_dc2 s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET value = s.value",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_rows_3col(
        &sql_context,
        "SELECT id, name, value FROM paimon.test_db.target ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![(1, "ALICE".to_string(), 100), (2, "bob".to_string(), 200),]
    );
}

#[tokio::test]
async fn test_merge_insert_reordered_columns() {
    // Verifies that INSERT with columns in a different order than the table schema
    // still maps data correctly (columns matched by name, not position).
    // Table schema: (id INT, name STRING, value INT)
    // INSERT specifies: (value, name, id) — reversed order
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;
    setup_data_evolution_table(&sql_context).await;

    sql_context
        .sql("INSERT INTO paimon.test_db.target (id, name, value) VALUES (1, 'alice', 10)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    enable_data_evolution(&sql_context).await;

    register_source(
        &sql_context,
        "CREATE TEMPORARY TABLE paimon.test_db.src_reorder AS SELECT * FROM (VALUES (2, 'bob', 20), (1, 'ALICE', 11)) AS t(id, name, value)",
    )
    .await;

    // INSERT columns in reversed order: (value, name, id)
    sql_context
        .sql(
            "MERGE INTO paimon.test_db.target t USING paimon.test_db.src_reorder s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (value, name, id) VALUES (s.value, s.name, s.id)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_rows_3col(
        &sql_context,
        "SELECT id, name, value FROM paimon.test_db.target ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            (1, "alice".to_string(), 10), // untouched (matched, no UPDATE clause)
            (2, "bob".to_string(), 20),   // inserted — columns must be correctly mapped
        ]
    );
}

#[tokio::test]
async fn test_merge_insert_reordered_columns_on_partitioned_table() {
    // Verifies column reordering on a partitioned table where mis-mapping
    // would cause data to land in the wrong partition.
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .unwrap();
    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.part_tbl (\
                dt STRING, id INT NOT NULL, name STRING\
            ) PARTITIONED BY (dt) WITH (\
                'row-tracking.enabled' = 'true'\
            )",
        )
        .await
        .unwrap();

    sql_context
        .sql("INSERT INTO paimon.test_db.part_tbl (dt, id, name) VALUES ('2024-01-01', 1, 'alice')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    sql_context.sql("ALTER TABLE paimon.test_db.part_tbl SET TBLPROPERTIES('data-evolution.enabled' = 'true')").await.unwrap();

    register_source(
        &sql_context,
        "CREATE TEMPORARY TABLE paimon.test_db.src_pt_reorder AS SELECT * FROM (VALUES (2, 'bob', '2024-02-01'), (1, 'ALICE', '2024-01-01')) AS t(id, name, dt)",
    )
    .await;

    // INSERT with columns in different order than table schema: (name, id, dt) vs table (dt, id, name)
    sql_context
        .sql(
            "MERGE INTO paimon.test_db.part_tbl t USING paimon.test_db.src_pt_reorder s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (name, id, dt) VALUES (s.name, s.id, s.dt)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = sql_context
        .sql("SELECT dt, id, name FROM paimon.test_db.part_tbl ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let mut rows = Vec::new();
    for batch in &batches {
        let dts = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((
                dts.value(i).to_string(),
                ids.value(i),
                names.value(i).to_string(),
            ));
        }
    }

    assert_eq!(
        rows,
        vec![
            ("2024-01-01".to_string(), 1, "alice".to_string()), // untouched
            ("2024-02-01".to_string(), 2, "bob".to_string()), // inserted — dt must be partition, not name
        ]
    );
}

#[tokio::test]
async fn test_rejects_table_with_primary_keys() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .unwrap();
    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.pk_target (\
                id INT NOT NULL, name STRING, PRIMARY KEY (id)\
            ) WITH (\
                'row-tracking.enabled' = 'true'\
            )",
        )
        .await
        .unwrap();

    register_source(
        &sql_context,
        "CREATE TEMPORARY TABLE paimon.test_db.src_pk AS SELECT * FROM (VALUES (1, 'ALICE')) AS t(id, name)",
    )
    .await;

    sql_context.sql("ALTER TABLE paimon.test_db.pk_target SET TBLPROPERTIES('data-evolution.enabled' = 'true')").await.unwrap();

    assert_merge_error(
        &sql_context,
        "MERGE INTO paimon.test_db.pk_target t USING paimon.test_db.src_pk s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET name = s.name",
        "does not support primary keys",
    )
    .await;
}

#[tokio::test]
async fn test_rejects_primary_key_table_without_data_evolution() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;

    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .unwrap();
    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.pk_plain_target (\
                id INT NOT NULL, name STRING, PRIMARY KEY (id)\
            ) WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    register_source(
        &sql_context,
        "CREATE TEMPORARY TABLE paimon.test_db.src_pk_plain AS SELECT * FROM (VALUES (1, 'ALICE')) AS t(id, name)",
    )
    .await;

    assert_merge_error(
        &sql_context,
        "MERGE INTO paimon.test_db.pk_plain_target t USING paimon.test_db.src_pk_plain s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET name = s.name",
        "primary-key tables without data-evolution",
    )
    .await;
}
