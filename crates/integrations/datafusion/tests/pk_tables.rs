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
//! INSERT OVERWRITE, filter pushdown, and error cases.

use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::prelude::SessionContext;
use paimon::catalog::Identifier;
use paimon::{Catalog, CatalogOptions, FileSystemCatalog, Options};
use paimon_datafusion::{PaimonCatalogProvider, PaimonRelationPlanner, PaimonSqlHandler};
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

fn create_handler(catalog: Arc<FileSystemCatalog>) -> PaimonSqlHandler {
    let ctx = SessionContext::new();
    ctx.register_catalog(
        "paimon",
        Arc::new(PaimonCatalogProvider::new(catalog.clone())),
    );
    ctx.register_relation_planner(Arc::new(PaimonRelationPlanner::new()))
        .expect("Failed to register relation planner");
    PaimonSqlHandler::new(ctx, catalog, "paimon")
}

async fn setup_handler() -> (TempDir, PaimonSqlHandler) {
    let (tmp, catalog) = create_test_env();
    let handler = create_handler(catalog);
    handler
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA failed");
    (tmp, handler)
}

async fn collect_id_name(handler: &PaimonSqlHandler, sql: &str) -> Vec<(i32, String)> {
    let batches = handler.sql(sql).await.unwrap().collect().await.unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("id column");
        let names = batch
            .column_by_name("name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("name column");
        for i in 0..batch.num_rows() {
            rows.push((ids.value(i), names.value(i).to_string()));
        }
    }
    rows.sort_by_key(|(id, _)| *id);
    rows
}

async fn collect_id_value(handler: &PaimonSqlHandler, sql: &str) -> Vec<(i32, i32)> {
    let batches = handler.sql(sql).await.unwrap().collect().await.unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("id column");
        let vals = batch
            .column_by_name("value")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("value column");
        for i in 0..batch.num_rows() {
            rows.push((ids.value(i), vals.value(i)));
        }
    }
    rows.sort_by_key(|(id, _)| *id);
    rows
}

async fn row_count(handler: &PaimonSqlHandler, sql: &str) -> usize {
    let batches = handler.sql(sql).await.unwrap().collect().await.unwrap();
    batches.iter().map(|b| b.num_rows()).sum()
}

// ======================= Basic PK Write + Read =======================

/// Basic: CREATE TABLE with PK, INSERT, SELECT — verifies round-trip.
#[tokio::test]
async fn test_pk_basic_write_read() {
    let (_tmp, handler) = setup_handler().await;

    handler
        .sql(
            "CREATE TABLE paimon.test_db.t1 (
                id INT NOT NULL, name STRING,
                PRIMARY KEY (id)
            ) WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    handler
        .sql("INSERT INTO paimon.test_db.t1 VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_id_name(
        &handler,
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

// ======================= Dedup Within Single Commit =======================

/// Duplicate keys in a single INSERT — last value wins (Deduplicate engine).
#[tokio::test]
async fn test_pk_dedup_within_single_commit() {
    let (_tmp, handler) = setup_handler().await;

    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_dedup (
                id INT NOT NULL, value INT,
                PRIMARY KEY (id)
            ) WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    handler
        .sql("INSERT INTO paimon.test_db.t_dedup VALUES (1, 10), (2, 20), (1, 100), (2, 200)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_id_value(
        &handler,
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
    let (_tmp, handler) = setup_handler().await;

    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_cross (
                id INT NOT NULL, name STRING,
                PRIMARY KEY (id)
            ) WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    // First commit
    handler
        .sql("INSERT INTO paimon.test_db.t_cross VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Second commit: update id=1,3, add id=4
    handler
        .sql("INSERT INTO paimon.test_db.t_cross VALUES (1, 'alice-v2'), (3, 'carol-v2'), (4, 'dave')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_id_name(
        &handler,
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
    let (_tmp, handler) = setup_handler().await;

    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_three (
                id INT NOT NULL, value INT,
                PRIMARY KEY (id)
            ) WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    handler
        .sql("INSERT INTO paimon.test_db.t_three VALUES (1, 10), (2, 20)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    handler
        .sql("INSERT INTO paimon.test_db.t_three VALUES (2, 200), (3, 30)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    handler
        .sql("INSERT INTO paimon.test_db.t_three VALUES (1, 100), (3, 300), (4, 40)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_id_value(
        &handler,
        "SELECT id, value FROM paimon.test_db.t_three ORDER BY id",
    )
    .await;

    assert_eq!(rows, vec![(1, 100), (2, 200), (3, 300), (4, 40)]);
}

// ======================= Partitioned PK Table =======================

/// Partitioned PK table: dedup happens per-partition independently.
#[tokio::test]
async fn test_pk_partitioned_write_read() {
    let (_tmp, handler) = setup_handler().await;

    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_part (
                dt STRING, id INT NOT NULL, name STRING,
                PRIMARY KEY (dt, id)
            ) PARTITIONED BY (dt STRING)
            WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    handler
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
        &handler,
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
    let (_tmp, handler) = setup_handler().await;

    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_part_dedup (
                dt STRING, id INT NOT NULL, name STRING,
                PRIMARY KEY (dt, id)
            ) PARTITIONED BY (dt STRING)
            WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    handler
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
    handler
        .sql(
            "INSERT INTO paimon.test_db.t_part_dedup VALUES \
             ('2024-01-01', 1, 'alice-v2'), ('2024-01-02', 2, 'dave')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = handler
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
    let (_tmp, handler) = setup_handler().await;

    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_part_filter (
                dt STRING, id INT NOT NULL, name STRING,
                PRIMARY KEY (dt, id)
            ) PARTITIONED BY (dt STRING)
            WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    handler
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
        &handler,
        "SELECT id, name FROM paimon.test_db.t_part_filter WHERE dt = '2024-01-01' ORDER BY id",
    )
    .await;

    assert_eq!(rows, vec![(1, "alice".to_string()), (2, "bob".to_string())]);
}

// ======================= Multi-Bucket PK Table =======================

/// Multiple buckets: rows are distributed by PK hash, dedup still works.
#[tokio::test]
async fn test_pk_multi_bucket() {
    let (_tmp, handler) = setup_handler().await;

    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_mbucket (
                id INT NOT NULL, value INT,
                PRIMARY KEY (id)
            ) WITH ('bucket' = '4')",
        )
        .await
        .unwrap();

    handler
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
    handler
        .sql("INSERT INTO paimon.test_db.t_mbucket VALUES (2, 200), (5, 500), (8, 800)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_id_value(
        &handler,
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
    let (_tmp, handler) = setup_handler().await;

    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_proj (
                id INT NOT NULL, name STRING, value INT,
                PRIMARY KEY (id)
            ) WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    handler
        .sql("INSERT INTO paimon.test_db.t_proj VALUES (1, 'alice', 10), (2, 'bob', 20)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Update id=1
    handler
        .sql("INSERT INTO paimon.test_db.t_proj VALUES (1, 'alice-v2', 100)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Project only name
    let batches = handler
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
        &handler,
        "SELECT id, value FROM paimon.test_db.t_proj ORDER BY id",
    )
    .await;
    assert_eq!(rows, vec![(1, 100), (2, 20)]);
}

// ======================= Sequence Field =======================

/// sequence.field: dedup uses the specified field instead of system sequence number.
#[tokio::test]
async fn test_pk_sequence_field() {
    let (_tmp, handler) = setup_handler().await;

    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_seqf (
                id INT NOT NULL, version INT, name STRING,
                PRIMARY KEY (id)
            ) WITH ('bucket' = '1', 'sequence.field' = 'version')",
        )
        .await
        .unwrap();

    // First commit: version=2 for id=1
    handler
        .sql("INSERT INTO paimon.test_db.t_seqf VALUES (1, 2, 'alice-v2'), (2, 1, 'bob-v1')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Second commit: version=1 for id=1 (older), version=2 for id=2 (newer)
    handler
        .sql("INSERT INTO paimon.test_db.t_seqf VALUES (1, 1, 'alice-v1'), (2, 2, 'bob-v2')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_id_name(
        &handler,
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
    let (_tmp, handler) = setup_handler().await;

    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_overwrite (
                dt STRING, id INT NOT NULL, name STRING,
                PRIMARY KEY (dt, id)
            ) PARTITIONED BY (dt STRING)
            WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    handler
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
    handler
        .sql("INSERT OVERWRITE paimon.test_db.t_overwrite VALUES ('2024-01-01', 10, 'new_alice')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = handler
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

// ======================= Composite Primary Key =======================

/// Composite PK with multiple columns.
#[tokio::test]
async fn test_pk_composite_key() {
    let (_tmp, handler) = setup_handler().await;

    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_composite (
                region STRING NOT NULL, id INT NOT NULL, value INT,
                PRIMARY KEY (region, id)
            ) WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    handler
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
    handler
        .sql("INSERT INTO paimon.test_db.t_composite VALUES ('us', 1, 100)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = handler
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
    let (_tmp, handler) = setup_handler().await;

    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_empty (
                id INT NOT NULL, name STRING,
                PRIMARY KEY (id)
            ) WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    let count = row_count(&handler, "SELECT id, name FROM paimon.test_db.t_empty").await;
    assert_eq!(count, 0);
}

// ======================= Large Batch Dedup =======================

/// Many rows with overlapping keys in a single commit.
#[tokio::test]
async fn test_pk_large_batch_dedup() {
    let (_tmp, handler) = setup_handler().await;

    handler
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

    handler
        .sql(&format!(
            "INSERT INTO paimon.test_db.t_large VALUES {}",
            values1.join(", ")
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    handler
        .sql(&format!(
            "INSERT INTO paimon.test_db.t_large VALUES {}",
            values2.join(", ")
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let count = row_count(&handler, "SELECT * FROM paimon.test_db.t_large").await;
    assert_eq!(count, 100, "Dedup should keep exactly 100 unique keys");

    // Spot-check a few values
    let rows = collect_id_value(
        &handler,
        "SELECT id, value FROM paimon.test_db.t_large WHERE id IN (1, 50, 100) ORDER BY id",
    )
    .await;
    assert_eq!(rows, vec![(1, 10), (50, 500), (100, 1000)]);
}

// ======================= Partitioned + Multi-Bucket =======================

/// Partitioned PK table with multiple buckets.
#[tokio::test]
async fn test_pk_partitioned_multi_bucket() {
    let (_tmp, handler) = setup_handler().await;

    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_part_mb (
                dt STRING, id INT NOT NULL, value INT,
                PRIMARY KEY (dt, id)
            ) PARTITIONED BY (dt STRING)
            WITH ('bucket' = '2')",
        )
        .await
        .unwrap();

    handler
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
    handler
        .sql(
            "INSERT INTO paimon.test_db.t_part_mb VALUES \
             ('2024-01-01', 2, 222), ('2024-01-02', 1, 111)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = handler
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

/// PK table with bucket=-1 (dynamic bucket) should be rejected.
#[tokio::test]
async fn test_pk_reject_dynamic_bucket() {
    let (_tmp, handler) = setup_handler().await;

    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_dyn (
                id INT NOT NULL, name STRING,
                PRIMARY KEY (id)
            )",
        )
        .await
        .unwrap();

    // bucket defaults to -1, PK write should fail
    let result = handler
        .sql("INSERT INTO paimon.test_db.t_dyn VALUES (1, 'alice')")
        .await;

    // The error may come from sql() or collect()
    let is_err = match result {
        Err(_) => true,
        Ok(df) => df.collect().await.is_err(),
    };
    assert!(is_err, "PK table with dynamic bucket should reject writes");
}

/// PK table with changelog-producer=input should be rejected.
#[tokio::test]
async fn test_pk_reject_changelog_producer_input() {
    let (_tmp, handler) = setup_handler().await;

    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_changelog (
                id INT NOT NULL, name STRING,
                PRIMARY KEY (id)
            ) WITH ('bucket' = '1', 'changelog-producer' = 'input')",
        )
        .await
        .unwrap();

    let result = handler
        .sql("INSERT INTO paimon.test_db.t_changelog VALUES (1, 'alice')")
        .await;

    let is_err = match result {
        Err(_) => true,
        Ok(df) => df.collect().await.is_err(),
    };
    assert!(
        is_err,
        "PK table with changelog-producer=input should reject writes"
    );
}

// ======================= String Primary Key =======================

/// PK table with STRING primary key.
#[tokio::test]
async fn test_pk_string_key() {
    let (_tmp, handler) = setup_handler().await;

    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_strpk (
                code STRING NOT NULL, name STRING,
                PRIMARY KEY (code)
            ) WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    handler
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
    handler
        .sql("INSERT INTO paimon.test_db.t_strpk VALUES ('A001', 'alice-v2')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = handler
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
    let (_tmp, handler) = setup_handler().await;

    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_multi_val (
                id INT NOT NULL, col_a INT, col_b STRING, col_c INT,
                PRIMARY KEY (id)
            ) WITH ('bucket' = '1')",
        )
        .await
        .unwrap();

    handler
        .sql("INSERT INTO paimon.test_db.t_multi_val VALUES (1, 10, 'x', 100), (2, 20, 'y', 200)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    handler
        .sql("INSERT INTO paimon.test_db.t_multi_val VALUES (1, 11, 'xx', 111)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = handler
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
    let handler = create_handler(catalog.clone());
    handler
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA failed");

    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_fr_ow (
                dt STRING, id INT NOT NULL, name STRING,
                PRIMARY KEY (dt, id)
            ) PARTITIONED BY (dt STRING)
            WITH ('bucket' = '1', 'merge-engine' = 'first-row')",
        )
        .await
        .unwrap();

    // First commit: two partitions, creates level-0 files
    handler
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
    handler
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
    handler
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
    let handler = create_handler(catalog.clone());
    handler
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA failed");

    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_postpone (
                id INT NOT NULL, value INT,
                PRIMARY KEY (id)
            ) WITH ('bucket' = '-2')",
        )
        .await
        .unwrap();

    // Write data
    handler
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
    let count = row_count(&handler, "SELECT * FROM paimon.test_db.t_postpone").await;
    assert_eq!(count, 0, "SELECT should return 0 rows for postpone table");
}

/// INSERT OVERWRITE on a postpone table should replace old files with new ones.
#[tokio::test]
async fn test_postpone_insert_overwrite() {
    let (_tmp, catalog) = create_test_env();
    let handler = create_handler(catalog.clone());
    handler
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA failed");

    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_postpone_ow (
                id INT NOT NULL, value INT,
                PRIMARY KEY (id)
            ) WITH ('bucket' = '-2')",
        )
        .await
        .unwrap();

    // First commit
    handler
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
    handler
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
