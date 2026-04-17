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

//! E2E integration tests for cross-partition update PK tables via DataFusion SQL.
//!
//! Cross-partition mode: PK does NOT include partition fields, bucket=-1 (dynamic).
//! A record's partition can change over time, requiring DELETE in old partition
//! and ADD in new partition.

mod common;

use common::{collect_id_name, collect_id_value, create_handler, create_test_env, setup_handler};
use datafusion::arrow::array::Int32Array;
use paimon::catalog::Identifier;
use paimon::Catalog;

/// Cross-partition update: PK does NOT include partition field.
/// A record's partition can change — old partition gets a DELETE, new partition gets an ADD.
#[tokio::test]
async fn test_cross_partition_update_basic() {
    let (_tmp, handler) = setup_handler().await;

    // PK is only "id", partition is "dt" — PK does NOT include partition field
    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_cross_pt (
                dt STRING, id INT NOT NULL, value INT,
                PRIMARY KEY (id)
            ) PARTITIONED BY (dt STRING)",
        )
        .await
        .unwrap();

    // First commit: id=1,2 in partition "2024-01-01"
    handler
        .sql(
            "INSERT INTO paimon.test_db.t_cross_pt VALUES \
             ('2024-01-01', 1, 10), ('2024-01-01', 2, 20)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_id_value(
        &handler,
        "SELECT id, value FROM paimon.test_db.t_cross_pt ORDER BY id",
    )
    .await;
    assert_eq!(rows, vec![(1, 10), (2, 20)]);

    // Second commit: id=1 moves to partition "2024-01-02"
    handler
        .sql("INSERT INTO paimon.test_db.t_cross_pt VALUES ('2024-01-02', 1, 100)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Read back — id=1 should have value=100, id=2 unchanged
    let rows = collect_id_value(
        &handler,
        "SELECT id, value FROM paimon.test_db.t_cross_pt ORDER BY id",
    )
    .await;
    assert_eq!(rows, vec![(1, 100), (2, 20)]);
}

/// Cross-partition: multiple records moving between partitions in one commit.
#[tokio::test]
async fn test_cross_partition_update_multiple_keys() {
    let (_tmp, handler) = setup_handler().await;

    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_cross_multi (
                dt STRING, id INT NOT NULL, name STRING,
                PRIMARY KEY (id)
            ) PARTITIONED BY (dt STRING)",
        )
        .await
        .unwrap();

    // First commit: 3 records in partition "a"
    handler
        .sql(
            "INSERT INTO paimon.test_db.t_cross_multi VALUES \
             ('a', 1, 'alice'), ('a', 2, 'bob'), ('a', 3, 'carol')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Second commit: id=1 moves to "b", id=2 moves to "c", id=3 stays in "a" with update
    handler
        .sql(
            "INSERT INTO paimon.test_db.t_cross_multi VALUES \
             ('b', 1, 'alice-v2'), ('c', 2, 'bob-v2'), ('a', 3, 'carol-v2')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_id_name(
        &handler,
        "SELECT id, name FROM paimon.test_db.t_cross_multi ORDER BY id",
    )
    .await;

    assert_eq!(
        rows,
        vec![
            (1, "alice-v2".to_string()),
            (2, "bob-v2".to_string()),
            (3, "carol-v2".to_string()),
        ]
    );
}

/// Cross-partition: three commits with records bouncing between partitions.
#[tokio::test]
async fn test_cross_partition_update_three_commits() {
    let (_tmp, handler) = setup_handler().await;

    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_cross_3c (
                dt STRING, id INT NOT NULL, value INT,
                PRIMARY KEY (id)
            ) PARTITIONED BY (dt STRING)",
        )
        .await
        .unwrap();

    // Commit 1: id=1 in "a", id=2 in "b"
    handler
        .sql(
            "INSERT INTO paimon.test_db.t_cross_3c VALUES \
             ('a', 1, 10), ('b', 2, 20)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Commit 2: id=1 moves to "b", id=2 moves to "a", add id=3 in "a"
    handler
        .sql(
            "INSERT INTO paimon.test_db.t_cross_3c VALUES \
             ('b', 1, 100), ('a', 2, 200), ('a', 3, 30)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Commit 3: id=1 moves back to "a", id=3 moves to "b"
    handler
        .sql(
            "INSERT INTO paimon.test_db.t_cross_3c VALUES \
             ('a', 1, 1000), ('b', 3, 300)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_id_value(
        &handler,
        "SELECT id, value FROM paimon.test_db.t_cross_3c ORDER BY id",
    )
    .await;

    assert_eq!(rows, vec![(1, 1000), (2, 200), (3, 300)]);
}

/// Cross-partition: new records in different partitions (no migration, just new keys).
#[tokio::test]
async fn test_cross_partition_new_keys_no_migration() {
    let (_tmp, handler) = setup_handler().await;

    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_cross_new (
                dt STRING, id INT NOT NULL, value INT,
                PRIMARY KEY (id)
            ) PARTITIONED BY (dt STRING)",
        )
        .await
        .unwrap();

    // All new keys in different partitions — no cross-partition migration
    handler
        .sql(
            "INSERT INTO paimon.test_db.t_cross_new VALUES \
             ('a', 1, 10), ('b', 2, 20), ('c', 3, 30)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_id_value(
        &handler,
        "SELECT id, value FROM paimon.test_db.t_cross_new ORDER BY id",
    )
    .await;
    assert_eq!(rows, vec![(1, 10), (2, 20), (3, 30)]);

    // Add more new keys
    handler
        .sql(
            "INSERT INTO paimon.test_db.t_cross_new VALUES \
             ('a', 4, 40), ('b', 5, 50)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_id_value(
        &handler,
        "SELECT id, value FROM paimon.test_db.t_cross_new ORDER BY id",
    )
    .await;
    assert_eq!(rows, vec![(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)]);
}

/// Cross-partition update: verify via scan_all_files that the old partition
/// receives a data file containing DELETE records (_VALUE_KIND=1) when a record
/// migrates to a new partition.
#[tokio::test]
async fn test_cross_partition_delete_file_in_old_partition() {
    let (_tmp, catalog) = create_test_env();
    let handler = create_handler(catalog.clone());
    handler
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA failed");

    // PK = (id), partition = dt — cross-partition mode
    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_cross_dv (
                dt STRING, id INT NOT NULL, value INT,
                PRIMARY KEY (id)
            ) PARTITIONED BY (dt STRING)",
        )
        .await
        .unwrap();

    // Commit 1: id=1,2 in partition "a", id=3 in partition "b"
    handler
        .sql(
            "INSERT INTO paimon.test_db.t_cross_dv VALUES \
             ('a', 1, 10), ('a', 2, 20), ('b', 3, 30)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Verify initial state: 2 data files (one per partition)
    let table = catalog
        .get_table(&Identifier::new("test_db", "t_cross_dv"))
        .await
        .unwrap();
    let plan = table
        .new_read_builder()
        .new_scan()
        .with_scan_all_files()
        .plan()
        .await
        .unwrap();
    let initial_file_count: usize = plan.splits().iter().map(|s| s.data_files().len()).sum();
    assert_eq!(
        initial_file_count, 2,
        "After commit 1: 2 data files (one per partition)"
    );

    // Commit 2: id=1 moves from partition "a" to partition "c"
    handler
        .sql("INSERT INTO paimon.test_db.t_cross_dv VALUES ('c', 1, 100)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Verify via scan_all_files: old partition "a" should have a new file
    // containing the DELETE record for id=1 (written with _VALUE_KIND=1)
    let table = catalog
        .get_table(&Identifier::new("test_db", "t_cross_dv"))
        .await
        .unwrap();
    let plan = table
        .new_read_builder()
        .new_scan()
        .with_scan_all_files()
        .plan()
        .await
        .unwrap();

    // Collect per-partition file counts and row counts
    let mut partition_file_counts: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    let mut partition_row_counts: std::collections::HashMap<String, Vec<i64>> =
        std::collections::HashMap::new();
    for split in plan.splits() {
        let dt = split.partition().get_string(0).unwrap().to_string();
        let count = split.data_files().len();
        *partition_file_counts.entry(dt.clone()).or_insert(0) += count;
        for file in split.data_files() {
            partition_row_counts
                .entry(dt.clone())
                .or_default()
                .push(file.row_count);
        }
    }

    // Partition "a": original file (2 rows) + DELETE file (1 row for id=1 migrating away)
    assert_eq!(
        partition_file_counts.get("a").copied().unwrap_or(0),
        2,
        "Partition 'a' should have 2 files (original + delete record)"
    );
    // The delete file should have row_count=1 (the DELETE record for id=1)
    let a_rows = partition_row_counts.get("a").unwrap();
    assert!(
        a_rows.contains(&1),
        "Partition 'a' should have a file with row_count=1 (the DELETE record), got {:?}",
        a_rows
    );

    // Partition "b": unchanged, still 1 file
    assert_eq!(
        partition_file_counts.get("b").copied().unwrap_or(0),
        1,
        "Partition 'b' should still have 1 file (untouched)"
    );

    // Partition "c": new file for id=1
    assert_eq!(
        partition_file_counts.get("c").copied().unwrap_or(0),
        1,
        "Partition 'c' should have 1 file (new ADD)"
    );

    // Final read: dedup should produce correct results
    let rows = collect_id_value(
        &handler,
        "SELECT id, value FROM paimon.test_db.t_cross_dv ORDER BY id",
    )
    .await;
    assert_eq!(rows, vec![(1, 100), (2, 20), (3, 30)]);

    // Commit 3: id=2 also moves from "a" to "b", id=3 stays in "b" with update
    handler
        .sql(
            "INSERT INTO paimon.test_db.t_cross_dv VALUES \
             ('b', 2, 200), ('b', 3, 300)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Verify partition "a" now has another DELETE file for id=2
    let table = catalog
        .get_table(&Identifier::new("test_db", "t_cross_dv"))
        .await
        .unwrap();
    let plan = table
        .new_read_builder()
        .new_scan()
        .with_scan_all_files()
        .plan()
        .await
        .unwrap();

    let mut partition_file_counts_3: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    for split in plan.splits() {
        let dt = split.partition().get_string(0).unwrap().to_string();
        *partition_file_counts_3.entry(dt).or_insert(0) += split.data_files().len();
    }

    // Partition "a": original (2 rows) + delete for id=1 + delete for id=2 = 3 files
    assert_eq!(
        partition_file_counts_3.get("a").copied().unwrap_or(0),
        3,
        "Partition 'a' should have 3 files (original + 2 delete records)"
    );

    // Partition "b": original (1 row) + new commit (2 rows: id=2 ADD + id=3 update) = 2 files
    assert_eq!(
        partition_file_counts_3.get("b").copied().unwrap_or(0),
        2,
        "Partition 'b' should have 2 files"
    );

    // Final read after 3 commits
    let rows = collect_id_value(
        &handler,
        "SELECT id, value FROM paimon.test_db.t_cross_dv ORDER BY id",
    )
    .await;
    assert_eq!(rows, vec![(1, 100), (2, 200), (3, 300)]);
}

/// Cross-partition + FIRST_ROW: key already in another partition → discard new row.
#[tokio::test]
async fn test_cross_partition_first_row_skip() {
    let (_tmp, catalog) = create_test_env();
    let handler = create_handler(catalog.clone());
    handler
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA failed");

    // PK is only "id", partition is "dt", merge-engine = first-row
    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_cross_fr (
                dt STRING, id INT NOT NULL, value INT,
                PRIMARY KEY (id)
            ) PARTITIONED BY (dt STRING)
            WITH ('merge-engine' = 'first-row')",
        )
        .await
        .unwrap();

    // Commit 1: id=1,2 in partition "a"
    handler
        .sql(
            "INSERT INTO paimon.test_db.t_cross_fr VALUES \
             ('a', 1, 10), ('a', 2, 20)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Commit 2: try to move id=1 to partition "b" and insert new id=3 in "b"
    // FIRST_ROW should discard id=1 (already exists in "a"), but accept id=3
    handler
        .sql(
            "INSERT INTO paimon.test_db.t_cross_fr VALUES \
             ('b', 1, 100), ('b', 3, 30)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Use scan_all_files to verify (FIRST_ROW skips level-0 in normal reads)
    let table = catalog
        .get_table(&Identifier::new("test_db", "t_cross_fr"))
        .await
        .unwrap();
    let rb = table.new_read_builder();
    let plan = rb.new_scan().with_scan_all_files().plan().await.unwrap();

    // Partition "a" should still have its original file only (no DELETE file)
    // because FIRST_ROW discards the new row instead of migrating
    let mut partition_file_counts: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    for split in plan.splits() {
        let dt = split.partition().get_string(0).unwrap().to_string();
        *partition_file_counts.entry(dt).or_insert(0) += split.data_files().len();
    }
    assert_eq!(
        partition_file_counts.get("a").copied().unwrap_or(0),
        1,
        "Partition 'a' should have 1 file (no DELETE generated for FIRST_ROW)"
    );
    // Partition "b" should have 1 file with only id=3 (id=1 was skipped)
    assert_eq!(
        partition_file_counts.get("b").copied().unwrap_or(0),
        1,
        "Partition 'b' should have 1 file (only id=3)"
    );

    // Verify data via scan_all_files read
    let read = rb.new_read().unwrap();
    let batches: Vec<_> = futures::TryStreamExt::try_collect(read.to_arrow(plan.splits()).unwrap())
        .await
        .unwrap();
    let mut rows: Vec<(i32, i32)> = Vec::new();
    for batch in &batches {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let vals = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((ids.value(i), vals.value(i)));
        }
    }
    rows.sort();
    // id=1 keeps original value 10, id=2 unchanged, id=3 is new
    assert_eq!(rows, vec![(1, 10), (2, 20), (3, 30)]);
}

/// Regression: partial PK/partition overlap — `PARTITIONED BY (pt1, pt2) + PK (pt1, id)`.
/// pt2 is NOT in PK, so cross-partition mode must be triggered.
/// When same id is written under different pt2, old partition should get a DELETE.
#[tokio::test]
async fn test_cross_partition_partial_pk_partition_overlap() {
    let (_tmp, handler) = setup_handler().await;

    // PK = (pt1, id), partition = (pt1, pt2) — pt2 is NOT in PK → cross-partition
    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_cross_partial (
                pt1 STRING, pt2 STRING, id INT NOT NULL, value INT,
                PRIMARY KEY (pt1, id)
            ) PARTITIONED BY (pt1 STRING, pt2 STRING)",
        )
        .await
        .unwrap();

    // Commit 1: id=1 in (pt1='a', pt2='x')
    handler
        .sql(
            "INSERT INTO paimon.test_db.t_cross_partial VALUES \
             ('a', 'x', 1, 10), ('a', 'x', 2, 20)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_id_value(
        &handler,
        "SELECT id, value FROM paimon.test_db.t_cross_partial ORDER BY id",
    )
    .await;
    assert_eq!(rows, vec![(1, 10), (2, 20)]);

    // Commit 2: id=1 moves to (pt1='a', pt2='y') — different pt2
    handler
        .sql("INSERT INTO paimon.test_db.t_cross_partial VALUES ('a', 'y', 1, 100)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // After dedup, id=1 should have value=100 and only appear once
    let rows = collect_id_value(
        &handler,
        "SELECT id, value FROM paimon.test_db.t_cross_partial ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![(1, 100), (2, 20)],
        "id=1 should be deduplicated across pt2 partitions"
    );
}

/// Regression: _VALUE_KIND schema stability across batches in cross-partition mode.
/// A cross-partition writer must always include _VALUE_KIND in the Arrow schema,
/// even when the current batch has no cross-partition migrations. Otherwise,
/// KeyValueFileWriter's concat_batches fails with a schema mismatch when a later
/// batch introduces deletes.
///
/// This test writes two commits to the same cross-partition table:
/// 1. First commit: all new keys (no migration → no deletes)
/// 2. Second commit: migrates keys (produces deletes)
/// Both must succeed without schema errors.
#[tokio::test]
async fn test_cross_partition_value_kind_schema_stability() {
    let (_tmp, handler) = setup_handler().await;

    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_cross_vk (
                dt STRING, id INT NOT NULL, value INT,
                PRIMARY KEY (id)
            ) PARTITIONED BY (dt STRING)",
        )
        .await
        .unwrap();

    // Commit 1: all new keys, no migration — _VALUE_KIND must still be added
    handler
        .sql(
            "INSERT INTO paimon.test_db.t_cross_vk VALUES \
             ('a', 1, 10), ('a', 2, 20), ('b', 3, 30)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Commit 2: id=1 migrates from "a" to "b" — produces DELETE in "a"
    handler
        .sql(
            "INSERT INTO paimon.test_db.t_cross_vk VALUES \
             ('b', 1, 100), ('a', 2, 200)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_id_value(
        &handler,
        "SELECT id, value FROM paimon.test_db.t_cross_vk ORDER BY id",
    )
    .await;
    assert_eq!(rows, vec![(1, 100), (2, 200), (3, 30)]);
}
