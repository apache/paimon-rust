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

//! E2E integration tests for primary-key tables via DataFusion SQL.
//!
//! Covers: basic write+read, dedup within/across commits, partitioned PK tables,
//! multi-bucket, column projection, FirstRow merge engine, sequence.field,
//! INSERT OVERWRITE, filter pushdown, cross-split merge correctness,
//! aggregation merge engine, and error cases.
//!
//! Dynamic bucket and cross-partition tests are in separate files:
//! - `dynamic_bucket_tables.rs`
//! - `cross_partition_tables.rs`

mod common;

use common::{
    collect_id_name, collect_id_value, collect_int_int_str, create_sql_context, create_test_env,
    row_count, setup_sql_context,
};
use datafusion::arrow::array::{Array, Int32Array, Int64Array, StringArray};
use paimon::catalog::Identifier;
use paimon::Catalog;

// ======================= Basic PK Write + Read =======================

/// Basic: CREATE TABLE with PK, INSERT, SELECT — verifies round-trip.
#[tokio::test]
async fn test_pk_basic_write_read() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t1 (
                id INT NOT NULL, name STRING,
                PRIMARY KEY (id)
            ) WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    sql_context
        .sql("INSERT INTO paimon.test_db.t1 VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_id_name(
        &sql_context,
        "SELECT id, name FROM paimon.test_db.t1 ORDER BY id",
    )
    .await;

    assert_eq!(
        rows,
        vec![
            (1, "alice".to_string()),
            (2, "bob".to_string()),
            (3, "carol".to_string()),
        ]
    );
}

/// Partial-update merge engine: keep latest non-null value for each field.
#[tokio::test]
async fn test_pk_partial_update_fixed_bucket_e2e() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_partial_update (
                id INT NOT NULL, v_int INT, v_str STRING,
                PRIMARY KEY (id)
            ) WITH ('bucket' = '1', 'merge-engine' = 'partial-update')",
        )
        .await
        .unwrap();

    sql_context
        .sql(
            "INSERT INTO paimon.test_db.t_partial_update VALUES
             (1, 10, 'old-1'),
             (2, 20, 'old-2')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    sql_context
        .sql(
            "INSERT INTO paimon.test_db.t_partial_update VALUES
             (1, CAST(NULL AS INT), 'new-1'),
             (2, 200, CAST(NULL AS STRING)),
             (3, 30, CAST(NULL AS STRING))",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    sql_context
        .sql(
            "INSERT INTO paimon.test_db.t_partial_update VALUES
             (1, 111, CAST(NULL AS STRING))",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = sql_context
        .sql("SELECT id, v_int, v_str FROM paimon.test_db.t_partial_update ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .unwrap();
        let ints = batch
            .column_by_name("v_int")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .unwrap();
        let strs = batch
            .column_by_name("v_str")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((
                ids.value(i),
                if ints.is_null(i) {
                    None
                } else {
                    Some(ints.value(i))
                },
                if strs.is_null(i) {
                    None
                } else {
                    Some(strs.value(i).to_string())
                },
            ));
        }
    }

    assert_eq!(
        rows,
        vec![
            (1, Some(111), Some("new-1".to_string())),
            (2, Some(200), Some("old-2".to_string())),
            (3, Some(30), None),
        ]
    );
}

/// Partial updates of one key within a single INSERT are merged at flush
/// (mirrors Java MergeTreeWriter#flushWriteBuffer): the flushed file holds
/// one row per key, so SELECT and COUNT(*) agree.
#[tokio::test]
async fn test_pk_partial_update_merges_within_single_commit() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_pu_flush_merge (
                id INT NOT NULL, v_int INT, v_str STRING,
                PRIMARY KEY (id)
            ) WITH ('bucket' = '1', 'merge-engine' = 'partial-update')",
        )
        .await
        .unwrap();

    // Three partial updates of key 1 plus key 2 in ONE commit: the writer
    // must merge key 1 down to a single physical row.
    sql_context
        .sql(
            "INSERT INTO paimon.test_db.t_pu_flush_merge VALUES
             (1, 10, CAST(NULL AS STRING)),
             (1, CAST(NULL AS INT), 'hello'),
             (1, 100, CAST(NULL AS STRING)),
             (2, 200, 'world')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Field-wise merge result: latest non-null per column.
    let batches = sql_context
        .sql("SELECT id, v_int, v_str FROM paimon.test_db.t_pu_flush_merge ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        collect_int_int_str(&batches),
        vec![(1, 100, "hello".to_string()), (2, 200, "world".to_string())]
    );

    // COUNT(*) must agree with SELECT: physical rows now equal merged rows.
    let batches = sql_context
        .sql("SELECT COUNT(*) FROM paimon.test_db.t_pu_flush_merge")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 2, "COUNT(*) must count merged rows");
}

// ======================= Dedup Within Single Commit =======================

/// Duplicate keys in a single INSERT — last value wins (Deduplicate engine).
#[tokio::test]
async fn test_pk_dedup_within_single_commit() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_dedup (
                id INT NOT NULL, value INT,
                PRIMARY KEY (id)
            ) WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    sql_context
        .sql("INSERT INTO paimon.test_db.t_dedup VALUES (1, 10), (2, 20), (1, 100), (2, 200)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_id_value(
        &sql_context,
        "SELECT id, value FROM paimon.test_db.t_dedup ORDER BY id",
    )
    .await;

    // Last occurrence wins for deduplicate merge engine
    assert_eq!(rows, vec![(1, 100), (2, 200)]);
}

// ======================= Dedup Across Commits =======================

/// Two commits with overlapping keys — second commit's values win.
#[tokio::test]
async fn test_pk_dedup_across_commits() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_cross (
                id INT NOT NULL, name STRING,
                PRIMARY KEY (id)
            ) WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    // First commit
    sql_context
        .sql("INSERT INTO paimon.test_db.t_cross VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Second commit: update id=1,3, add id=4
    sql_context
        .sql("INSERT INTO paimon.test_db.t_cross VALUES (1, 'alice-v2'), (3, 'carol-v2'), (4, 'dave')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_id_name(
        &sql_context,
        "SELECT id, name FROM paimon.test_db.t_cross ORDER BY id",
    )
    .await;

    assert_eq!(
        rows,
        vec![
            (1, "alice-v2".to_string()),
            (2, "bob".to_string()),
            (3, "carol-v2".to_string()),
            (4, "dave".to_string()),
        ]
    );
}

// ======================= Three Commits =======================

/// Three successive commits — verifies sequence number tracking across commits.
#[tokio::test]
async fn test_pk_three_commits() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_three (
                id INT NOT NULL, value INT,
                PRIMARY KEY (id)
            ) WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    sql_context
        .sql("INSERT INTO paimon.test_db.t_three VALUES (1, 10), (2, 20)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    sql_context
        .sql("INSERT INTO paimon.test_db.t_three VALUES (2, 200), (3, 30)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    sql_context
        .sql("INSERT INTO paimon.test_db.t_three VALUES (1, 100), (3, 300), (4, 40)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_id_value(
        &sql_context,
        "SELECT id, value FROM paimon.test_db.t_three ORDER BY id",
    )
    .await;

    assert_eq!(rows, vec![(1, 100), (2, 200), (3, 300), (4, 40)]);
}

// ======================= Partitioned PK Table =======================

/// Partitioned PK table: dedup happens per-partition independently.
#[tokio::test]
async fn test_pk_partitioned_write_read() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_part (
                dt STRING, id INT NOT NULL, name STRING,
                PRIMARY KEY (dt, id)
            ) PARTITIONED BY (dt)
            WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    sql_context
        .sql(
            "INSERT INTO paimon.test_db.t_part VALUES \
             ('2024-01-01', 1, 'alice'), ('2024-01-01', 2, 'bob'), \
             ('2024-01-02', 1, 'carol'), ('2024-01-02', 2, 'dave')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_id_name(
        &sql_context,
        "SELECT id, name FROM paimon.test_db.t_part ORDER BY id, name",
    )
    .await;

    assert_eq!(
        rows,
        vec![
            (1, "alice".to_string()),
            (1, "carol".to_string()),
            (2, "bob".to_string()),
            (2, "dave".to_string()),
        ]
    );
}

/// Partitioned PK table: dedup across commits within same partition.
#[tokio::test]
async fn test_pk_partitioned_dedup_across_commits() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_part_dedup (
                dt STRING, id INT NOT NULL, name STRING,
                PRIMARY KEY (dt, id)
            ) PARTITIONED BY (dt)
            WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    sql_context
        .sql(
            "INSERT INTO paimon.test_db.t_part_dedup VALUES \
             ('2024-01-01', 1, 'alice'), ('2024-01-01', 2, 'bob'), \
             ('2024-01-02', 1, 'carol')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Update within partition 2024-01-01
    sql_context
        .sql(
            "INSERT INTO paimon.test_db.t_part_dedup VALUES \
             ('2024-01-01', 1, 'alice-v2'), ('2024-01-02', 2, 'dave')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = sql_context
        .sql("SELECT dt, id, name FROM paimon.test_db.t_part_dedup ORDER BY dt, id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let mut rows = Vec::new();
    for batch in &batches {
        let dts = batch
            .column_by_name("dt")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .unwrap();
        let ids = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .unwrap();
        let names = batch
            .column_by_name("name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
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
            ("2024-01-01".to_string(), 1, "alice-v2".to_string()),
            ("2024-01-01".to_string(), 2, "bob".to_string()),
            ("2024-01-02".to_string(), 1, "carol".to_string()),
            ("2024-01-02".to_string(), 2, "dave".to_string()),
        ]
    );
}

/// Partition filter on PK table — only matching partition returned.
#[tokio::test]
async fn test_pk_partitioned_filter() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_part_filter (
                dt STRING, id INT NOT NULL, name STRING,
                PRIMARY KEY (dt, id)
            ) PARTITIONED BY (dt)
            WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    sql_context
        .sql(
            "INSERT INTO paimon.test_db.t_part_filter VALUES \
             ('2024-01-01', 1, 'alice'), ('2024-01-01', 2, 'bob'), \
             ('2024-01-02', 3, 'carol')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_id_name(
        &sql_context,
        "SELECT id, name FROM paimon.test_db.t_part_filter WHERE dt = '2024-01-01' ORDER BY id",
    )
    .await;

    assert_eq!(rows, vec![(1, "alice".to_string()), (2, "bob".to_string())]);
}

// ======================= Multi-Bucket PK Table =======================

/// Multiple buckets: rows are distributed by PK hash, dedup still works.
#[tokio::test]
async fn test_pk_multi_bucket() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_mbucket (
                id INT NOT NULL, value INT,
                PRIMARY KEY (id)
            ) WITH ('bucket' = '4')",
        )
        .await
        .unwrap();

    sql_context
        .sql(
            "INSERT INTO paimon.test_db.t_mbucket VALUES \
             (1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60), (7, 70), (8, 80)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Update some keys
    sql_context
        .sql("INSERT INTO paimon.test_db.t_mbucket VALUES (2, 200), (5, 500), (8, 800)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_id_value(
        &sql_context,
        "SELECT id, value FROM paimon.test_db.t_mbucket ORDER BY id",
    )
    .await;

    assert_eq!(
        rows,
        vec![
            (1, 10),
            (2, 200),
            (3, 30),
            (4, 40),
            (5, 500),
            (6, 60),
            (7, 70),
            (8, 800),
        ]
    );
}

// ======================= Column Projection =======================

/// SELECT only a subset of columns from a PK table.
#[tokio::test]
async fn test_pk_column_projection() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_proj (
                id INT NOT NULL, name STRING, value INT,
                PRIMARY KEY (id)
            ) WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    sql_context
        .sql("INSERT INTO paimon.test_db.t_proj VALUES (1, 'alice', 10), (2, 'bob', 20)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Update id=1
    sql_context
        .sql("INSERT INTO paimon.test_db.t_proj VALUES (1, 'alice-v2', 100)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Project only name
    let batches = sql_context
        .sql("SELECT name FROM paimon.test_db.t_proj ORDER BY name")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let mut names = Vec::new();
    for batch in &batches {
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            names.push(arr.value(i).to_string());
        }
    }
    names.sort();
    assert_eq!(names, vec!["alice-v2", "bob"]);

    // Project only value
    let rows = collect_id_value(
        &sql_context,
        "SELECT id, value FROM paimon.test_db.t_proj ORDER BY id",
    )
    .await;
    assert_eq!(rows, vec![(1, 100), (2, 20)]);
}

// ======================= Sequence Field =======================

/// sequence.field: dedup uses the specified field instead of system sequence number.
#[tokio::test]
async fn test_pk_sequence_field() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_seqf (
                id INT NOT NULL, version INT, name STRING,
                PRIMARY KEY (id)
            ) WITH ('bucket' = '1', 'sequence.field' = 'version')",
        )
        .await
        .unwrap();

    // First commit: version=2 for id=1
    sql_context
        .sql("INSERT INTO paimon.test_db.t_seqf VALUES (1, 2, 'alice-v2'), (2, 1, 'bob-v1')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Second commit: version=1 for id=1 (older), version=2 for id=2 (newer)
    sql_context
        .sql("INSERT INTO paimon.test_db.t_seqf VALUES (1, 1, 'alice-v1'), (2, 2, 'bob-v2')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_id_name(
        &sql_context,
        "SELECT id, name FROM paimon.test_db.t_seqf ORDER BY id",
    )
    .await;

    assert_eq!(
        rows,
        vec![
            (1, "alice-v2".to_string()), // version=2 wins over version=1
            (2, "bob-v2".to_string()),   // version=2 wins over version=1
        ]
    );
}

// ======================= INSERT OVERWRITE =======================

/// INSERT OVERWRITE on a partitioned PK table replaces the partition.
#[tokio::test]
async fn test_pk_insert_overwrite_partitioned() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_overwrite (
                dt STRING, id INT NOT NULL, name STRING,
                PRIMARY KEY (dt, id)
            ) PARTITIONED BY (dt)
            WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    sql_context
        .sql(
            "INSERT INTO paimon.test_db.t_overwrite VALUES \
             ('2024-01-01', 1, 'alice'), ('2024-01-01', 2, 'bob'), \
             ('2024-01-02', 3, 'carol')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Overwrite partition 2024-01-01
    sql_context
        .sql("INSERT OVERWRITE paimon.test_db.t_overwrite VALUES ('2024-01-01', 10, 'new_alice')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = sql_context
        .sql("SELECT dt, id, name FROM paimon.test_db.t_overwrite ORDER BY dt, id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let mut rows = Vec::new();
    for batch in &batches {
        let dts = batch
            .column_by_name("dt")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .unwrap();
        let ids = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .unwrap();
        let names = batch
            .column_by_name("name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
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
            ("2024-01-01".to_string(), 10, "new_alice".to_string()),
            ("2024-01-02".to_string(), 3, "carol".to_string()),
        ]
    );
}

/// INSERT OVERWRITE with explicit PARTITION clause (Hive-style static partition).
#[tokio::test]
async fn test_pk_insert_overwrite_with_partition_clause() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_ow_part (
                dt STRING, id INT NOT NULL, name STRING,
                PRIMARY KEY (dt, id)
            ) PARTITIONED BY (dt)
            WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    // Insert data into two partitions
    sql_context
        .sql(
            "INSERT INTO paimon.test_db.t_ow_part VALUES \
             ('2024-01-01', 1, 'alice'), ('2024-01-01', 2, 'bob'), \
             ('2024-01-02', 3, 'carol')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Overwrite partition dt='2024-01-01' using Hive-style PARTITION clause.
    // The SELECT only provides non-partition columns (id, name).
    sql_context
        .sql(
            "INSERT OVERWRITE paimon.test_db.t_ow_part PARTITION (dt = '2024-01-01') \
             VALUES (10, 'new_alice'), (20, 'new_bob')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = sql_context
        .sql("SELECT dt, id, name FROM paimon.test_db.t_ow_part ORDER BY dt, id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let mut rows = Vec::new();
    for batch in &batches {
        let dts = batch
            .column_by_name("dt")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .unwrap();
        let ids = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .unwrap();
        let names = batch
            .column_by_name("name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((
                dts.value(i).to_string(),
                ids.value(i),
                names.value(i).to_string(),
            ));
        }
    }

    // Partition 2024-01-01 overwritten, 2024-01-02 untouched
    assert_eq!(
        rows,
        vec![
            ("2024-01-01".to_string(), 10, "new_alice".to_string()),
            ("2024-01-01".to_string(), 20, "new_bob".to_string()),
            ("2024-01-02".to_string(), 3, "carol".to_string()),
        ]
    );
}

/// INSERT OVERWRITE with partial PARTITION clause on a multi-level partitioned table.
/// Only specifies dt, region comes from the source query (dynamic partition).
#[tokio::test]
async fn test_pk_insert_overwrite_partial_partition_clause() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_multi_part (
                dt STRING, region STRING, id INT NOT NULL, name STRING,
                PRIMARY KEY (dt, region, id)
            ) PARTITIONED BY (dt, region)
            WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    // Insert data into multiple partitions
    sql_context
        .sql(
            "INSERT INTO paimon.test_db.t_multi_part VALUES \
             ('2024-01-01', 'us', 1, 'alice'), \
             ('2024-01-01', 'eu', 2, 'bob'), \
             ('2024-01-02', 'us', 3, 'carol')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Overwrite only dt='2024-01-01', region comes from VALUES (dynamic).
    // This should overwrite all sub-partitions under dt='2024-01-01' that appear in the data.
    sql_context
        .sql(
            "INSERT OVERWRITE paimon.test_db.t_multi_part PARTITION (dt = '2024-01-01') \
             VALUES ('us', 10, 'new_alice')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = sql_context
        .sql("SELECT dt, region, id, name FROM paimon.test_db.t_multi_part ORDER BY dt, region, id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let mut rows = Vec::new();
    for batch in &batches {
        let dts = batch
            .column_by_name("dt")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .unwrap();
        let regions = batch
            .column_by_name("region")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .unwrap();
        let ids = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .unwrap();
        let names = batch
            .column_by_name("name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((
                dts.value(i).to_string(),
                regions.value(i).to_string(),
                ids.value(i),
                names.value(i).to_string(),
            ));
        }
    }

    // dt='2024-01-01' fully overwritten (static partition overwrite deletes all sub-partitions).
    // dt='2024-01-01'/region='eu' (bob) is deleted because the entire dt='2024-01-01' partition is replaced.
    // dt='2024-01-02'/region='us' untouched.
    assert_eq!(
        rows,
        vec![
            (
                "2024-01-01".to_string(),
                "us".to_string(),
                10,
                "new_alice".to_string()
            ),
            (
                "2024-01-02".to_string(),
                "us".to_string(),
                3,
                "carol".to_string()
            ),
        ]
    );
}

/// INSERT OVERWRITE with PARTITION clause and empty source truncates the partition.
#[tokio::test]
async fn test_pk_insert_overwrite_partition_truncate() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_trunc (
                dt STRING, id INT NOT NULL, name STRING,
                PRIMARY KEY (dt, id)
            ) PARTITIONED BY (dt)
            WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    sql_context
        .sql(
            "INSERT INTO paimon.test_db.t_trunc VALUES \
             ('2024-01-01', 1, 'alice'), ('2024-01-01', 2, 'bob'), \
             ('2024-01-02', 3, 'carol')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Overwrite dt='2024-01-01' with empty source — should truncate that partition
    sql_context
        .sql(
            "INSERT OVERWRITE paimon.test_db.t_trunc PARTITION (dt = '2024-01-01') \
             SELECT id, name FROM paimon.test_db.t_trunc WHERE false",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = sql_context
        .sql("SELECT dt, id, name FROM paimon.test_db.t_trunc ORDER BY dt, id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let mut rows = Vec::new();
    for batch in &batches {
        let dts = batch
            .column_by_name("dt")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .unwrap();
        let ids = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .unwrap();
        let names = batch
            .column_by_name("name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((
                dts.value(i).to_string(),
                ids.value(i),
                names.value(i).to_string(),
            ));
        }
    }

    // dt='2024-01-01' truncated, dt='2024-01-02' untouched
    assert_eq!(
        rows,
        vec![("2024-01-02".to_string(), 3, "carol".to_string()),]
    );
}

/// PARTITION clause with a non-partition column should fail.
#[tokio::test]
async fn test_pk_insert_overwrite_partition_non_partition_column_error() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_err (
                dt STRING, id INT NOT NULL, name STRING,
                PRIMARY KEY (dt, id)
            ) PARTITIONED BY (dt)
            WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    let result = sql_context
        .sql(
            "INSERT OVERWRITE paimon.test_db.t_err PARTITION (name = 'alice') \
             VALUES (1)",
        )
        .await;

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("not a partition column"),
        "Expected 'not a partition column' error, got: {err_msg}"
    );
}

/// All-dynamic PARTITION clause (no static values) should use dynamic partition overwrite,
/// not drop all partitions.
#[tokio::test]
async fn test_pk_insert_overwrite_dynamic_partition_preserves_other_partitions() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_dyn (
                dt STRING, id INT NOT NULL, name STRING,
                PRIMARY KEY (dt, id)
            ) PARTITIONED BY (dt)
            WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    sql_context
        .sql(
            "INSERT INTO paimon.test_db.t_dyn VALUES \
             ('2024-01-01', 1, 'alice'), ('2024-01-02', 2, 'bob')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Dynamic partition overwrite: PARTITION (dt) with no static value.
    // Should only overwrite partitions present in the source data.
    sql_context
        .sql(
            "INSERT OVERWRITE paimon.test_db.t_dyn PARTITION (dt) \
             VALUES ('2024-01-01', 10, 'new_alice')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = sql_context
        .sql("SELECT dt, id, name FROM paimon.test_db.t_dyn ORDER BY dt, id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let mut rows = Vec::new();
    for batch in &batches {
        let dts = batch
            .column_by_name("dt")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .unwrap();
        let ids = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .unwrap();
        let names = batch
            .column_by_name("name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((
                dts.value(i).to_string(),
                ids.value(i),
                names.value(i).to_string(),
            ));
        }
    }

    // dt='2024-01-01' overwritten, dt='2024-01-02' preserved
    assert_eq!(
        rows,
        vec![
            ("2024-01-01".to_string(), 10, "new_alice".to_string()),
            ("2024-01-02".to_string(), 2, "bob".to_string()),
        ]
    );
}

/// Source query with wrong column count should fail even when the result is empty.
#[tokio::test]
async fn test_pk_insert_overwrite_empty_source_wrong_columns_error() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_empty_err (
                dt STRING, id INT NOT NULL, name STRING,
                PRIMARY KEY (dt, id)
            ) PARTITIONED BY (dt)
            WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    sql_context
        .sql(
            "INSERT INTO paimon.test_db.t_empty_err VALUES \
             ('2024-01-01', 1, 'alice')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Source only produces `id` but target expects `id, name` — should fail
    let result = sql_context
        .sql(
            "INSERT OVERWRITE paimon.test_db.t_empty_err PARTITION (dt = '2024-01-01') \
             SELECT id FROM paimon.test_db.t_empty_err WHERE false",
        )
        .await;

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("expected 2 non-partition columns"),
        "Expected column count mismatch error, got: {err_msg}"
    );
}

/// Explicit target column list after PARTITION should reorder source columns to match schema.
#[tokio::test]
async fn test_pk_insert_overwrite_with_after_columns_reorder() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_reorder (
                dt STRING, id INT NOT NULL, name STRING,
                PRIMARY KEY (dt, id)
            ) PARTITIONED BY (dt)
            WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    // Insert with columns in reversed order: (name, id) instead of schema order (id, name)
    sql_context
        .sql(
            "INSERT OVERWRITE paimon.test_db.t_reorder (name, id) PARTITION (dt = '2024-01-01') \
             VALUES ('alice', 1), ('bob', 2)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = sql_context
        .sql("SELECT dt, id, name FROM paimon.test_db.t_reorder ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let mut rows = Vec::new();
    for batch in &batches {
        let dts = batch
            .column_by_name("dt")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .unwrap();
        let ids = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .unwrap();
        let names = batch
            .column_by_name("name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((
                dts.value(i).to_string(),
                ids.value(i),
                names.value(i).to_string(),
            ));
        }
    }

    // Values should be correctly mapped: name='alice'/id=1, name='bob'/id=2
    assert_eq!(
        rows,
        vec![
            ("2024-01-01".to_string(), 1, "alice".to_string()),
            ("2024-01-01".to_string(), 2, "bob".to_string()),
        ]
    );
}

// ======================= Composite Primary Key =======================

/// Composite PK with multiple columns.
#[tokio::test]
async fn test_pk_composite_key() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_composite (
                region STRING NOT NULL, id INT NOT NULL, value INT,
                PRIMARY KEY (region, id)
            ) WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    sql_context
        .sql(
            "INSERT INTO paimon.test_db.t_composite VALUES \
             ('us', 1, 10), ('eu', 1, 20), ('us', 2, 30)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Update (us, 1) — (eu, 1) should be untouched
    sql_context
        .sql("INSERT INTO paimon.test_db.t_composite VALUES ('us', 1, 100)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = sql_context
        .sql("SELECT region, id, value FROM paimon.test_db.t_composite ORDER BY region, id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let mut rows = Vec::new();
    for batch in &batches {
        let regions = batch
            .column_by_name("region")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .unwrap();
        let ids = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .unwrap();
        let vals = batch
            .column_by_name("value")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((regions.value(i).to_string(), ids.value(i), vals.value(i)));
        }
    }

    assert_eq!(
        rows,
        vec![
            ("eu".to_string(), 1, 20),  // untouched
            ("us".to_string(), 1, 100), // updated
            ("us".to_string(), 2, 30),  // untouched
        ]
    );
}

// ======================= Empty Table Read =======================

/// Reading an empty PK table returns zero rows.
#[tokio::test]
async fn test_pk_empty_table_read() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_empty (
                id INT NOT NULL, name STRING,
                PRIMARY KEY (id)
            ) WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    let count = row_count(&sql_context, "SELECT id, name FROM paimon.test_db.t_empty").await;
    assert_eq!(count, 0);
}

// ======================= Large Batch Dedup =======================

/// Many rows with overlapping keys in a single commit.
#[tokio::test]
async fn test_pk_large_batch_dedup() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_large (
                id INT NOT NULL, value INT,
                PRIMARY KEY (id)
            ) WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    // Insert 100 rows, then overwrite all with new values
    let mut values1 = Vec::new();
    let mut values2 = Vec::new();
    for i in 1..=100 {
        values1.push(format!("({i}, {i})")); // id=i, value=i
        values2.push(format!("({i}, {})", i * 10)); // id=i, value=i*10
    }

    sql_context
        .sql(&format!(
            "INSERT INTO paimon.test_db.t_large VALUES {}",
            values1.join(", ")
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    sql_context
        .sql(&format!(
            "INSERT INTO paimon.test_db.t_large VALUES {}",
            values2.join(", ")
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let count = row_count(&sql_context, "SELECT * FROM paimon.test_db.t_large").await;
    assert_eq!(count, 100, "Dedup should keep exactly 100 unique keys");

    // Spot-check a few values
    let rows = collect_id_value(
        &sql_context,
        "SELECT id, value FROM paimon.test_db.t_large WHERE id IN (1, 50, 100) ORDER BY id",
    )
    .await;
    assert_eq!(rows, vec![(1, 10), (50, 500), (100, 1000)]);
}

// ======================= Partitioned + Multi-Bucket =======================

/// Partitioned PK table with multiple buckets.
#[tokio::test]
async fn test_pk_partitioned_multi_bucket() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_part_mb (
                dt STRING, id INT NOT NULL, value INT,
                PRIMARY KEY (dt, id)
            ) PARTITIONED BY (dt)
            WITH ('bucket' = '2')",
        )
        .await
        .unwrap();

    sql_context
        .sql(
            "INSERT INTO paimon.test_db.t_part_mb VALUES \
             ('2024-01-01', 1, 10), ('2024-01-01', 2, 20), \
             ('2024-01-01', 3, 30), ('2024-01-01', 4, 40), \
             ('2024-01-02', 1, 100), ('2024-01-02', 2, 200)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Update across partitions
    sql_context
        .sql(
            "INSERT INTO paimon.test_db.t_part_mb VALUES \
             ('2024-01-01', 2, 222), ('2024-01-02', 1, 111)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = sql_context
        .sql("SELECT dt, id, value FROM paimon.test_db.t_part_mb ORDER BY dt, id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let mut rows = Vec::new();
    for batch in &batches {
        let dts = batch
            .column_by_name("dt")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .unwrap();
        let ids = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .unwrap();
        let vals = batch
            .column_by_name("value")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((dts.value(i).to_string(), ids.value(i), vals.value(i)));
        }
    }

    assert_eq!(
        rows,
        vec![
            ("2024-01-01".to_string(), 1, 10),
            ("2024-01-01".to_string(), 2, 222),
            ("2024-01-01".to_string(), 3, 30),
            ("2024-01-01".to_string(), 4, 40),
            ("2024-01-02".to_string(), 1, 111),
            ("2024-01-02".to_string(), 2, 200),
        ]
    );
}

// ======================= Error Cases =======================

/// PK table with changelog-producer=input should write through DataFusion SQL.
#[tokio::test]
async fn test_pk_input_changelog_write_read() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_changelog (
                id INT NOT NULL, name STRING,
                PRIMARY KEY (id)
            ) WITH ('bucket' = '1', 'changelog-producer' = 'input')",
        )
        .await
        .unwrap();

    sql_context
        .sql(
            "INSERT INTO paimon.test_db.t_changelog VALUES
                (1, 'alice'), (1, 'bob'), (2, 'carol')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_id_name(
        &sql_context,
        "SELECT id, name FROM paimon.test_db.t_changelog ORDER BY id",
    )
    .await;

    assert_eq!(rows, vec![(1, "bob".to_string()), (2, "carol".to_string())]);
}

// ======================= String Primary Key =======================

/// PK table with STRING primary key.
#[tokio::test]
async fn test_pk_string_key() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_strpk (
                code STRING NOT NULL, name STRING,
                PRIMARY KEY (code)
            ) WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    sql_context
        .sql(
            "INSERT INTO paimon.test_db.t_strpk VALUES \
             ('A001', 'alice'), ('B002', 'bob'), ('C003', 'carol')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Update A001
    sql_context
        .sql("INSERT INTO paimon.test_db.t_strpk VALUES ('A001', 'alice-v2')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = sql_context
        .sql("SELECT code, name FROM paimon.test_db.t_strpk ORDER BY code")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let mut rows = Vec::new();
    for batch in &batches {
        let codes = batch
            .column_by_name("code")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .unwrap();
        let names = batch
            .column_by_name("name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((codes.value(i).to_string(), names.value(i).to_string()));
        }
    }

    assert_eq!(
        rows,
        vec![
            ("A001".to_string(), "alice-v2".to_string()),
            ("B002".to_string(), "bob".to_string()),
            ("C003".to_string(), "carol".to_string()),
        ]
    );
}

// ======================= Multiple Value Columns =======================

/// PK table with many value columns — verifies all columns survive dedup.
#[tokio::test]
async fn test_pk_multiple_value_columns() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_multi_val (
                id INT NOT NULL, col_a INT, col_b STRING, col_c INT,
                PRIMARY KEY (id)
            ) WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    sql_context
        .sql("INSERT INTO paimon.test_db.t_multi_val VALUES (1, 10, 'x', 100), (2, 20, 'y', 200)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    sql_context
        .sql("INSERT INTO paimon.test_db.t_multi_val VALUES (1, 11, 'xx', 111)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = sql_context
        .sql("SELECT id, col_a, col_b, col_c FROM paimon.test_db.t_multi_val ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .unwrap();
        let as_ = batch
            .column_by_name("col_a")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .unwrap();
        let bs = batch
            .column_by_name("col_b")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .unwrap();
        let cs = batch
            .column_by_name("col_c")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((
                ids.value(i),
                as_.value(i),
                bs.value(i).to_string(),
                cs.value(i),
            ));
        }
    }

    assert_eq!(
        rows,
        vec![
            (1, 11, "xx".to_string(), 111), // updated
            (2, 20, "y".to_string(), 200),  // untouched
        ]
    );
}

// ======================= FirstRow Engine: INSERT OVERWRITE =======================

/// INSERT OVERWRITE on a partitioned FirstRow-engine PK table should delete
/// level-0 files. Before the fix, `skip_level_zero` was applied in the overwrite
/// scan path, causing level-0 files to survive the overwrite.
///
/// Verifies via TableScan (scan_all_files) that the overwrite correctly produces
/// delete entries for level-0 files, leaving only the new file per partition.
#[tokio::test]
async fn test_pk_first_row_insert_overwrite() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;
    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA failed");

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_fr_ow (
                dt STRING, id INT NOT NULL, name STRING,
                PRIMARY KEY (dt, id)
            ) PARTITIONED BY (dt)
            WITH ('bucket' = '1', 'merge-engine' = 'first-row')",
        )
        .await
        .unwrap();

    // First commit: two partitions, creates level-0 files
    sql_context
        .sql(
            "INSERT INTO paimon.test_db.t_fr_ow VALUES \
             ('2024-01-01', 1, 'alice'), ('2024-01-01', 2, 'bob'), \
             ('2024-01-02', 3, 'carol')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Verify via scan_all_files: 2 level-0 files (one per partition)
    let table = catalog
        .get_table(&Identifier::new("test_db", "t_fr_ow"))
        .await
        .unwrap();
    let plan = table
        .new_read_builder()
        .new_scan()
        .with_scan_all_files()
        .plan()
        .await
        .unwrap();
    let file_count: usize = plan.splits().iter().map(|s| s.data_files().len()).sum();
    assert_eq!(
        file_count, 2,
        "After INSERT: 2 level-0 files (one per partition)"
    );

    // INSERT OVERWRITE partition 2024-01-01 — must delete old level-0 file
    sql_context
        .sql("INSERT OVERWRITE paimon.test_db.t_fr_ow VALUES ('2024-01-01', 10, 'new_alice')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let table = catalog
        .get_table(&Identifier::new("test_db", "t_fr_ow"))
        .await
        .unwrap();
    let plan = table
        .new_read_builder()
        .new_scan()
        .with_scan_all_files()
        .plan()
        .await
        .unwrap();
    let file_count: usize = plan.splits().iter().map(|s| s.data_files().len()).sum();
    assert_eq!(
        file_count, 2,
        "After OVERWRITE: 2 files (1 replaced for 2024-01-01 + 1 unchanged for 2024-01-02)"
    );

    // Second overwrite on the same partition — no stale files should accumulate
    sql_context
        .sql("INSERT OVERWRITE paimon.test_db.t_fr_ow VALUES ('2024-01-01', 20, 'newer_alice')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let table = catalog
        .get_table(&Identifier::new("test_db", "t_fr_ow"))
        .await
        .unwrap();
    let plan = table
        .new_read_builder()
        .new_scan()
        .with_scan_all_files()
        .plan()
        .await
        .unwrap();
    let file_count: usize = plan.splits().iter().map(|s| s.data_files().len()).sum();
    assert_eq!(
        file_count, 2,
        "After second OVERWRITE: still 2 files (no stale level-0 files accumulated)"
    );
}

// ======================= Postpone Bucket (bucket = -2) =======================

/// Postpone bucket files are invisible to normal SELECT but visible via scan_all_files.
#[tokio::test]
async fn test_postpone_write_invisible_to_select() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;
    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA failed");

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_postpone (
                id INT NOT NULL, value INT,
                PRIMARY KEY (id)
            ) WITH ('bucket' = '-2')",
        )
        .await
        .unwrap();

    // Write data
    sql_context
        .sql("INSERT INTO paimon.test_db.t_postpone VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // scan_all_files should find the postpone file
    let table = catalog
        .get_table(&Identifier::new("test_db", "t_postpone"))
        .await
        .unwrap();
    let plan = table
        .new_read_builder()
        .new_scan()
        .with_scan_all_files()
        .plan()
        .await
        .unwrap();
    let file_count: usize = plan.splits().iter().map(|s| s.data_files().len()).sum();
    assert_eq!(file_count, 1, "scan_all_files should find 1 postpone file");

    // Normal SELECT should return 0 rows (postpone files are invisible)
    let count = row_count(&sql_context, "SELECT * FROM paimon.test_db.t_postpone").await;
    assert_eq!(count, 0, "SELECT should return 0 rows for postpone table");
}

/// INSERT OVERWRITE on a postpone table should replace old files with new ones.
#[tokio::test]
async fn test_postpone_insert_overwrite() {
    let (_tmp, catalog) = create_test_env();
    let sql_context = create_sql_context(catalog.clone()).await;
    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA failed");

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_postpone_ow (
                id INT NOT NULL, value INT,
                PRIMARY KEY (id)
            ) WITH ('bucket' = '-2')",
        )
        .await
        .unwrap();

    // First commit
    sql_context
        .sql("INSERT INTO paimon.test_db.t_postpone_ow VALUES (1, 10), (2, 20)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let table = catalog
        .get_table(&Identifier::new("test_db", "t_postpone_ow"))
        .await
        .unwrap();
    let plan = table
        .new_read_builder()
        .new_scan()
        .with_scan_all_files()
        .plan()
        .await
        .unwrap();
    let file_count: usize = plan.splits().iter().map(|s| s.data_files().len()).sum();
    assert_eq!(file_count, 1, "After INSERT: 1 postpone file");

    // INSERT OVERWRITE should replace old file
    sql_context
        .sql("INSERT OVERWRITE paimon.test_db.t_postpone_ow VALUES (3, 30)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let table = catalog
        .get_table(&Identifier::new("test_db", "t_postpone_ow"))
        .await
        .unwrap();
    let plan = table
        .new_read_builder()
        .new_scan()
        .with_scan_all_files()
        .plan()
        .await
        .unwrap();
    let file_count: usize = plan.splits().iter().map(|s| s.data_files().len()).sum();
    assert_eq!(
        file_count, 1,
        "After OVERWRITE: only 1 new file (old file deleted)"
    );
}

// ======================= Bucket Keys Regression =======================

/// Regression: partitioned PK fixed-bucket table — query with partition + PK
/// predicate must return rows. Before the fix, `bucket_keys()` returned full
/// primary keys (including partition columns), while the read path used
/// `trimmed_primary_keys()`, causing bucket pruning to target the wrong bucket.
#[tokio::test]
async fn test_pk_partitioned_fixed_bucket_predicate_query() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_bk_pred (
                pt STRING, id INT NOT NULL, value INT,
                PRIMARY KEY (pt, id)
            ) PARTITIONED BY (pt)
            WITH ('bucket' = '2')",
        )
        .await
        .unwrap();

    sql_context
        .sql(
            "INSERT INTO paimon.test_db.t_bk_pred VALUES \
             ('a', 1, 10), ('a', 2, 20), ('b', 3, 30), ('b', 4, 40)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Query with both partition and PK columns in predicate
    let rows = collect_id_value(
        &sql_context,
        "SELECT id, value FROM paimon.test_db.t_bk_pred WHERE pt = 'a' AND id = 1",
    )
    .await;
    assert_eq!(rows, vec![(1, 10)], "Predicate query must find the row");

    let rows = collect_id_value(
        &sql_context,
        "SELECT id, value FROM paimon.test_db.t_bk_pred WHERE pt = 'b' AND id = 4",
    )
    .await;
    assert_eq!(rows, vec![(4, 40)], "Predicate query must find the row");
}

// ======================= DV + Deduplicate Regression =======================

/// Regression: DV-enabled Deduplicate PK table must not error on read.
/// Before the fix, removing the DV guard caused level-0 files to reach
/// KeyValueFileReader which rejects deletion-vector files with a hard error.
/// With the guard restored, level-0 files are skipped in scan (DV mode relies
/// on compaction to produce higher-level files).
#[tokio::test]
async fn test_pk_dv_deduplicate_read_no_error() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_dv_dedup (
                id INT NOT NULL, value INT,
                PRIMARY KEY (id)
            ) WITH ('bucket' = '1', 'deletion-vectors.enabled' = 'true')",
        )
        .await
        .unwrap();

    sql_context
        .sql("INSERT INTO paimon.test_db.t_dv_dedup VALUES (1, 10), (2, 20)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Second commit with overlapping key — creates level-0 files
    sql_context
        .sql("INSERT INTO paimon.test_db.t_dv_dedup VALUES (2, 200), (3, 30)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Read must not error. DV mode skips level-0 files, so only compacted
    // (level > 0) files are visible. Without compaction, all files are level-0
    // and get skipped — count may be 0, but the read must succeed without error.
    // Before the fix, this would hard-fail with "KeyValueFileReader does not
    // support deletion vectors".
    let result = sql_context
        .sql("SELECT * FROM paimon.test_db.t_dv_dedup")
        .await
        .unwrap()
        .collect()
        .await;
    assert!(
        result.is_ok(),
        "DV + Deduplicate read should not error: {:?}",
        result.err()
    );
}

// ======================= Cross-Split Merge Correctness =======================

/// Regression: a 1-byte split target forces every data file into its own
/// split candidate. Files holding versions of the same key overlap on key
/// range and must still be merged into a single row — previously each split
/// emitted its own (stale) version.
#[tokio::test]
async fn test_pk_dedup_merges_across_tiny_splits() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_tiny_split (
                id INT NOT NULL, value INT,
                PRIMARY KEY (id)
            ) WITH (
                'bucket' = '1',
                'source.split.target-size' = '1b',
                'source.split.open-file-cost' = '1b'
            )",
        )
        .await
        .unwrap();

    for value in [10, 20, 30] {
        sql_context
            .sql(&format!(
                "INSERT INTO paimon.test_db.t_tiny_split VALUES (1, {value})"
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
    }

    let rows = collect_id_value(
        &sql_context,
        "SELECT id, value FROM paimon.test_db.t_tiny_split",
    )
    .await;
    assert_eq!(rows, vec![(1, 30)]);
}

/// LIMIT must not be starved by merge-needed splits: the three versions of
/// key 1 share one split whose physical row count (3) overstates its single
/// logical row. Such splits report an unknown merged row count, so limit
/// pushdown cannot stop before the split holding key 2.
#[tokio::test]
async fn test_pk_limit_not_starved_by_merge_splits() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_tiny_split_limit (
                id INT NOT NULL, value INT,
                PRIMARY KEY (id)
            ) WITH (
                'bucket' = '1',
                'source.split.target-size' = '1b',
                'source.split.open-file-cost' = '1b'
            )",
        )
        .await
        .unwrap();

    // Three versions of key 1 (one overlapping section), then key 2.
    for value in [10, 20, 30] {
        sql_context
            .sql(&format!(
                "INSERT INTO paimon.test_db.t_tiny_split_limit VALUES (1, {value})"
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
    }
    sql_context
        .sql("INSERT INTO paimon.test_db.t_tiny_split_limit VALUES (2, 200)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Two logical rows exist; LIMIT 2 must return both.
    let returned = row_count(
        &sql_context,
        "SELECT id, value FROM paimon.test_db.t_tiny_split_limit LIMIT 2",
    )
    .await;
    assert_eq!(returned, 2, "LIMIT 2 must yield 2 rows");

    // COUNT(*) must reflect logical rows, not the physical (pre-merge) count
    // that DataFusion could otherwise read from exact scan statistics.
    let batches = sql_context
        .sql("SELECT COUNT(*) FROM paimon.test_db.t_tiny_split_limit")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 2, "COUNT(*) must count merged rows");
}

/// Partial-update files can hold several physical rows of one key (the
/// writer keeps all rows for read-side field-wise merge), so their splits
/// must never report physical row counts as merged row counts: COUNT(*) has
/// to count merged rows and LIMIT must not be starved.
#[tokio::test]
async fn test_pk_partial_update_count_and_limit_see_merged_rows() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_pu_count (
                id INT NOT NULL, v_int INT, v_str STRING,
                PRIMARY KEY (id)
            ) WITH (
                'bucket' = '1',
                'merge-engine' = 'partial-update',
                'source.split.target-size' = '1b',
                'source.split.open-file-cost' = '1b'
            )",
        )
        .await
        .unwrap();

    // One INSERT writes three partial updates of key 1 into a single file,
    // plus an independent key 2.
    sql_context
        .sql(
            "INSERT INTO paimon.test_db.t_pu_count VALUES
             (1, 10, CAST(NULL AS STRING)),
             (1, CAST(NULL AS INT), 'hello'),
             (1, 100, CAST(NULL AS STRING)),
             (2, 200, 'world')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // COUNT(*) must count merged rows, not the physical rows a single file
    // holds (DataFusion may answer COUNT(*) from exact scan statistics).
    let batches = sql_context
        .sql("SELECT COUNT(*) FROM paimon.test_db.t_pu_count")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 2, "COUNT(*) must count merged rows");

    // Two logical rows exist; LIMIT 2 must not be starved by the
    // multi-version file of key 1.
    let returned = row_count(
        &sql_context,
        "SELECT id, v_int FROM paimon.test_db.t_pu_count LIMIT 2",
    )
    .await;
    assert_eq!(returned, 2, "LIMIT 2 must yield 2 rows");
}

/// Same regression for the partial-update engine: per-column updates of one
/// key spread over three commits/files must merge into a single row even
/// when the split target would otherwise separate the files.
#[tokio::test]
async fn test_pk_partial_update_merges_across_tiny_splits() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_tiny_split_pu (
                id INT NOT NULL, v_int INT, v_str STRING,
                PRIMARY KEY (id)
            ) WITH (
                'bucket' = '1',
                'merge-engine' = 'partial-update',
                'source.split.target-size' = '1b',
                'source.split.open-file-cost' = '1b'
            )",
        )
        .await
        .unwrap();

    sql_context
        .sql("INSERT INTO paimon.test_db.t_tiny_split_pu VALUES (1, 10, CAST(NULL AS STRING))")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    sql_context
        .sql("INSERT INTO paimon.test_db.t_tiny_split_pu VALUES (1, CAST(NULL AS INT), 'hello')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    sql_context
        .sql("INSERT INTO paimon.test_db.t_tiny_split_pu VALUES (1, 100, CAST(NULL AS STRING))")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = sql_context
        .sql("SELECT id, v_int, v_str FROM paimon.test_db.t_tiny_split_pu")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        collect_int_int_str(&batches),
        vec![(1, 100, "hello".to_string())]
    );
}

// ======================= Aggregation Engine =======================

/// Basic: aggregation engine sums numeric column and concatenates string
/// column across overlapping primary keys.
#[tokio::test]
async fn test_pk_aggregation_sum_and_listagg_fixed_multi_bucket_e2e() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_agg_sum (
                id INT NOT NULL, amount INT, tag STRING,
                PRIMARY KEY (id)
            ) WITH (
                'bucket' = '4',
                'merge-engine' = 'aggregation',
                'fields.amount.aggregate-function' = 'sum',
                'fields.tag.aggregate-function' = 'listagg',
                'fields.tag.list-agg-delimiter' = '|'
            )",
        )
        .await
        .unwrap();

    sql_context
        .sql(
            "INSERT INTO paimon.test_db.t_agg_sum VALUES \
             (1, 10, 'a'), (2, 20, 'x')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    sql_context
        .sql(
            "INSERT INTO paimon.test_db.t_agg_sum VALUES \
             (1, 5, 'b'), (2, 7, CAST(NULL AS STRING)), (3, 99, 'solo')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = sql_context
        .sql("SELECT id, amount, tag FROM paimon.test_db.t_agg_sum ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let mut rows: Vec<(i32, Option<i32>, Option<String>)> = Vec::new();
    for batch in &batches {
        let ids = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .unwrap();
        let amounts = batch
            .column_by_name("amount")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .unwrap();
        let tags = batch
            .column_by_name("tag")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((
                ids.value(i),
                if amounts.is_null(i) {
                    None
                } else {
                    Some(amounts.value(i))
                },
                if tags.is_null(i) {
                    None
                } else {
                    Some(tags.value(i).to_string())
                },
            ));
        }
    }

    assert_eq!(
        rows,
        vec![
            (1, Some(15), Some("a|b".to_string())),
            (2, Some(27), Some("x".to_string())),
            (3, Some(99), Some("solo".to_string())),
        ]
    );
}

/// `fields.default-aggregate-function` applies to any column without an
/// explicit per-field aggregator.
#[tokio::test]
async fn test_pk_aggregation_default_function() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_agg_default (
                id INT NOT NULL, a INT, b STRING,
                PRIMARY KEY (id)
            ) WITH (
                'bucket' = '1',
                'merge-engine' = 'aggregation',
                'fields.default-aggregate-function' = 'last_non_null_value'
            )",
        )
        .await
        .unwrap();

    sql_context
        .sql("INSERT INTO paimon.test_db.t_agg_default VALUES (1, 10, 'old')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    sql_context
        .sql(
            "INSERT INTO paimon.test_db.t_agg_default VALUES \
             (1, CAST(NULL AS INT), 'new')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    sql_context
        .sql("INSERT INTO paimon.test_db.t_agg_default VALUES (1, 99, CAST(NULL AS STRING))")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = sql_context
        .sql("SELECT id, a, b FROM paimon.test_db.t_agg_default")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    let batch = &batches[0];
    let id = batch
        .column_by_name("id")
        .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
        .unwrap();
    let a = batch
        .column_by_name("a")
        .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
        .unwrap();
    let b = batch
        .column_by_name("b")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .unwrap();
    assert_eq!(id.value(0), 1);
    assert_eq!(a.value(0), 99); // latest non-null int across the three commits
    assert_eq!(b.value(0), "new"); // latest non-null string
}

/// Mixed aggregators in a single table: sum / max / bool_or / first_non_null_value.
#[tokio::test]
async fn test_pk_aggregation_mixed_aggregators() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_agg_mixed (
                id INT NOT NULL, total INT, peak INT, ok BOOLEAN, first_seen STRING,
                PRIMARY KEY (id)
            ) WITH (
                'bucket' = '1',
                'merge-engine' = 'aggregation',
                'fields.total.aggregate-function' = 'sum',
                'fields.peak.aggregate-function' = 'max',
                'fields.ok.aggregate-function' = 'bool_or',
                'fields.first_seen.aggregate-function' = 'first_non_null_value'
            )",
        )
        .await
        .unwrap();

    sql_context
        .sql(
            "INSERT INTO paimon.test_db.t_agg_mixed VALUES \
             (1, 10, 5, false, 'a'), \
             (1, 5, 8, true, 'b'), \
             (1, 3, 7, false, 'c')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = sql_context
        .sql("SELECT id, total, peak, ok, first_seen FROM paimon.test_db.t_agg_mixed")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    use datafusion::arrow::array::BooleanArray;
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    let batch = &batches[0];
    let total = batch
        .column_by_name("total")
        .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
        .unwrap();
    let peak = batch
        .column_by_name("peak")
        .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
        .unwrap();
    let ok = batch
        .column_by_name("ok")
        .and_then(|c| c.as_any().downcast_ref::<BooleanArray>())
        .unwrap();
    let first_seen = batch
        .column_by_name("first_seen")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .unwrap();
    assert_eq!(total.value(0), 18); // 10 + 5 + 3
    assert_eq!(peak.value(0), 8); // max(5, 8, 7)
    assert!(ok.value(0)); // bool_or = true if any is true
    assert_eq!(first_seen.value(0), "a"); // first non-null wins
}

/// `sequence.field` forces the named column to `last_value`, even when the
/// user explicitly configures another aggregator for it.
#[tokio::test]
async fn test_pk_aggregation_sequence_field_forced_last_value() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_agg_seq (
                id INT NOT NULL, amount INT, ts INT,
                PRIMARY KEY (id)
            ) WITH (
                'bucket' = '1',
                'merge-engine' = 'aggregation',
                'sequence.field' = 'ts',
                'fields.amount.aggregate-function' = 'sum',
                'fields.ts.aggregate-function' = 'sum'
            )",
        )
        .await
        .unwrap();

    sql_context
        .sql(
            "INSERT INTO paimon.test_db.t_agg_seq VALUES \
             (1, 10, 100), (1, 20, 250)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = sql_context
        .sql("SELECT id, amount, ts FROM paimon.test_db.t_agg_seq")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    let batch = &batches[0];
    let amount = batch
        .column_by_name("amount")
        .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
        .unwrap();
    let ts = batch
        .column_by_name("ts")
        .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
        .unwrap();
    assert_eq!(amount.value(0), 30); // sum still applies
    assert_eq!(ts.value(0), 250); // forced last_value over sum
}

/// Aggregation engine reads must surface Unsupported when a DELETE/UPDATE
/// row appears.
#[tokio::test]
async fn test_pk_aggregation_rejects_delete() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_agg_del (
                id INT NOT NULL, amount INT,
                PRIMARY KEY (id)
            ) WITH (
                'bucket' = '1',
                'merge-engine' = 'aggregation',
                'fields.amount.aggregate-function' = 'sum'
            )",
        )
        .await
        .unwrap();

    sql_context
        .sql("INSERT INTO paimon.test_db.t_agg_del VALUES (1, 10), (2, 20)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // DELETE may either fail at planning or surface Unsupported at execution.
    // Either way the error must mention the aggregation engine refusing the
    // retract row; we explicitly assert both branches so a future parser
    // change cannot silently turn this into a no-op pass.
    let plan_result = sql_context
        .sql("DELETE FROM paimon.test_db.t_agg_del WHERE id = 1")
        .await;
    match plan_result {
        Ok(df) => {
            let exec = df.collect().await;
            assert!(exec.is_err(), "DELETE on aggregation table should fail");
            let msg = format!("{:?}", exec.err().unwrap());
            assert!(
                msg.contains("aggregation")
                    || msg.contains("DELETE")
                    || msg.contains("UPDATE_BEFORE"),
                "expected aggregation engine to reject DELETE at execution, got {msg}"
            );
        }
        Err(e) => {
            let msg = format!("{e:?}");
            assert!(
                msg.contains("aggregation")
                    || msg.contains("DELETE")
                    || msg.contains("Unsupported"),
                "expected aggregation engine to reject DELETE at planning, got {msg}"
            );
        }
    }
}

/// `merge-engine=aggregation` with no per-field nor default aggregate-function
/// should still work: each value column falls back to `last_non_null_value`,
/// matching Java `AggregateMergeFunction#getAggFuncName`.
#[tokio::test]
async fn test_pk_aggregation_default_fallback_is_last_non_null_value() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_agg_fallback (
                id INT NOT NULL, amount INT,
                PRIMARY KEY (id)
            ) WITH (
                'bucket' = '1',
                'merge-engine' = 'aggregation'
            )",
        )
        .await
        .unwrap();

    sql_context
        .sql("INSERT INTO paimon.test_db.t_agg_fallback VALUES (1, 10), (1, 20)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = sql_context
        .sql("SELECT id, amount FROM paimon.test_db.t_agg_fallback")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    let amount = batches[0]
        .column_by_name("amount")
        .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
        .unwrap();
    assert_eq!(amount.value(0), 20); // last_non_null_value
}

/// CREATE TABLE should reject unsupported aggregation knobs in basic mode.
#[tokio::test]
async fn test_pk_aggregation_rejects_unsupported_options_at_create() {
    let (_tmp, sql_context) = setup_sql_context().await;

    let err = sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_agg_bad (
                id INT NOT NULL, amount INT,
                PRIMARY KEY (id)
            ) WITH (
                'bucket' = '1',
                'merge-engine' = 'aggregation',
                'fields.amount.aggregate-function' = 'sum',
                'fields.amount.ignore-retract' = 'true'
            )",
        )
        .await
        .expect_err("CREATE TABLE with ignore-retract should fail in basic mode");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("ignore-retract"),
        "expected create-time rejection to mention ignore-retract, got {msg}"
    );
}

/// CREATE TABLE should reject `fields.<typo>.aggregate-function` referring to
/// a non-existent column, so misconfigured aggregation metadata cannot be
/// persisted.
#[tokio::test]
async fn test_pk_aggregation_create_table_rejects_unknown_field() {
    let (_tmp, sql_context) = setup_sql_context().await;

    let err = sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_agg_typo (
                id INT NOT NULL, amount INT,
                PRIMARY KEY (id)
            ) WITH (
                'bucket' = '1',
                'merge-engine' = 'aggregation',
                'fields.amout.aggregate-function' = 'sum'
            )",
        )
        .await
        .expect_err("CREATE TABLE with unknown field should fail at create time");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("amout") && msg.contains("amount"),
        "expected unknown-field error to name the typo and surface the available \
         columns, got {msg}"
    );
}

/// CREATE TABLE should reject `fields.<col>.aggregate-function = '<unknown>'`
/// at create time rather than only failing the first SELECT.
#[tokio::test]
async fn test_pk_aggregation_create_table_rejects_unknown_function() {
    let (_tmp, sql_context) = setup_sql_context().await;

    let err = sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_agg_badfn (
                id INT NOT NULL, amount INT,
                PRIMARY KEY (id)
            ) WITH (
                'bucket' = '1',
                'merge-engine' = 'aggregation',
                'fields.amount.aggregate-function' = 'sume'
            )",
        )
        .await
        .expect_err("CREATE TABLE with unknown function should fail at create time");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("sume"),
        "expected unknown-function error to surface the bad name, got {msg}"
    );
}

/// CREATE TABLE should reject aggregate-function/column type incompatibility
/// (e.g. `sum` on a STRING column) at create time.  This is stricter than
/// Java upstream, which defers the check to the first read/write.
#[tokio::test]
async fn test_pk_aggregation_create_table_rejects_incompatible_type() {
    let (_tmp, sql_context) = setup_sql_context().await;

    let err = sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_agg_badtype (
                id INT NOT NULL, tag STRING,
                PRIMARY KEY (id)
            ) WITH (
                'bucket' = '1',
                'merge-engine' = 'aggregation',
                'fields.tag.aggregate-function' = 'sum'
            )",
        )
        .await
        .expect_err("CREATE TABLE with incompatible function/type should fail at create time");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("sum") && msg.contains("tag"),
        "expected incompatible-type error to mention the function and field, got {msg}"
    );
}

/// CREATE TABLE must accept a function/type pair that the runtime would
/// ignore: `sequence.field` columns are forced to `last_value` and primary-key
/// columns get no aggregator, so type compatibility is not checked for them.
#[tokio::test]
async fn test_pk_aggregation_create_table_accepts_ignored_function_on_seq_and_pk() {
    let (_tmp, sql_context) = setup_sql_context().await;

    // `listagg` is incompatible with INT, but `amount` is the sequence field
    // (forced to last_value) and `id` is a PK (copied through), so both
    // configurations are usable at runtime and must pass CREATE TABLE.
    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_agg_seq_pk_ok (
                id INT NOT NULL, amount INT, v INT,
                PRIMARY KEY (id)
            ) WITH (
                'bucket' = '1',
                'merge-engine' = 'aggregation',
                'sequence.field' = 'amount',
                'fields.amount.aggregate-function' = 'listagg',
                'fields.id.aggregate-function' = 'listagg',
                'fields.v.aggregate-function' = 'sum'
            )",
        )
        .await
        .expect("CREATE TABLE with runtime-ignored function/type pairs should succeed");
}

/// All-NULL aggregation group on a nullable `sum` column should emit NULL
/// rather than 0 or an error: nothing was observed, so there is no
/// arithmetic result to surface.
#[tokio::test]
async fn test_pk_aggregation_sum_all_null_emits_null_for_nullable_column() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_agg_null (
                id INT NOT NULL, amount INT,
                PRIMARY KEY (id)
            ) WITH (
                'bucket' = '1',
                'merge-engine' = 'aggregation',
                'fields.amount.aggregate-function' = 'sum'
            )",
        )
        .await
        .unwrap();

    sql_context
        .sql(
            "INSERT INTO paimon.test_db.t_agg_null VALUES \
             (1, CAST(NULL AS INT)), (1, CAST(NULL AS INT))",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = sql_context
        .sql("SELECT id, amount FROM paimon.test_db.t_agg_null")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    let amount = batches[0]
        .column_by_name("amount")
        .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
        .unwrap();
    assert!(amount.is_null(0), "sum over all-NULL group should be NULL");
}

/// Regression guard: end-to-end SELECT on an aggregation table must traverse
/// the KeyValueFileReader path (TableRead::to_arrow → read_pk → read_kv),
/// not silently fall through to read_raw.  The basic correctness assertion
/// (sum aggregation) implies this routing — a fallthrough to read_raw would
/// return the raw rows unmerged, breaking the sum.
#[tokio::test]
async fn test_pk_aggregation_routing_uses_kv_path() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_agg_route (
                id INT NOT NULL, amount INT,
                PRIMARY KEY (id)
            ) WITH (
                'bucket' = '1',
                'merge-engine' = 'aggregation',
                'fields.amount.aggregate-function' = 'sum'
            )",
        )
        .await
        .unwrap();

    // Two rows with the same key in a single INSERT — read_raw would return 2
    // rows; read_kv (with AggregateMergeFunction) collapses them into 1 with
    // amount=30.
    sql_context
        .sql("INSERT INTO paimon.test_db.t_agg_route VALUES (1, 10), (1, 20)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let n = row_count(&sql_context, "SELECT * FROM paimon.test_db.t_agg_route").await;
    assert_eq!(
        n, 1,
        "aggregation table must collapse same-PK rows; got {n} rows which suggests \
         to_arrow fell through to read_raw"
    );
    let batches = sql_context
        .sql("SELECT amount FROM paimon.test_db.t_agg_route")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let amount = batches[0]
        .column_by_name("amount")
        .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
        .unwrap();
    assert_eq!(amount.value(0), 30);
}

/// Regression: `COUNT(*)` pushes an empty projection down to the scan, so the
/// KV merge read path must preserve the row count when reordering a batch with
/// zero columns. Aggregation tables route through that path; without an
/// explicit `with_row_count`, the reordered batch would report 0 rows and
/// `COUNT(*)` would collapse to 0 even though the merge produced rows.
#[tokio::test]
async fn test_pk_aggregation_count_star_empty_projection() {
    let (_tmp, sql_context) = setup_sql_context().await;

    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.t_agg_count (
                id INT NOT NULL, amount INT,
                PRIMARY KEY (id)
            ) WITH (
                'bucket' = '1',
                'merge-engine' = 'aggregation',
                'fields.amount.aggregate-function' = 'sum'
            )",
        )
        .await
        .unwrap();

    // Two commits with overlapping primary keys so the read path must merge:
    // raw row count is 5, but the table holds 3 distinct keys (1, 2, 3).
    sql_context
        .sql("INSERT INTO paimon.test_db.t_agg_count VALUES (1, 10), (2, 20)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    sql_context
        .sql("INSERT INTO paimon.test_db.t_agg_count VALUES (1, 5), (2, 7), (3, 99)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = sql_context
        .sql("SELECT COUNT(*) FROM paimon.test_db.t_agg_count")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        count.value(0),
        3,
        "COUNT(*) over an aggregation table must reflect merged rows; a 0/wrong \
         count means the empty-projection reorder dropped the row count"
    );
}
