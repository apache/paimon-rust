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

//! E2E integration tests for dynamic bucket (bucket=-1) PK tables via DataFusion SQL.

mod common;

use common::{collect_id_name, collect_id_value, create_handler, create_test_env, setup_handler};
use datafusion::arrow::array::{Int32Array, StringArray};
use paimon::catalog::Identifier;
use paimon::spec::{IndexManifest, IndexManifestEntry};
use paimon::{Catalog, CatalogOptions, FileSystemCatalog, Options, SnapshotManager};

/// PK table with bucket=-1 (dynamic bucket) should write and read correctly.
#[tokio::test]
async fn test_pk_dynamic_bucket() {
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

    // First insert — no 'bucket' option, defaults to -1 (dynamic bucket)
    handler
        .sql("INSERT INTO paimon.test_db.t_dyn VALUES (1, 'alice'), (2, 'bob')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_id_name(
        &handler,
        "SELECT id, name FROM paimon.test_db.t_dyn ORDER BY id",
    )
    .await;
    assert_eq!(rows, vec![(1, "alice".to_string()), (2, "bob".to_string())]);

    // Second insert with overlapping key — dedup should keep latest
    handler
        .sql("INSERT INTO paimon.test_db.t_dyn VALUES (2, 'bobby'), (3, 'carol')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_id_name(
        &handler,
        "SELECT id, name FROM paimon.test_db.t_dyn ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![
            (1, "alice".to_string()),
            (2, "bobby".to_string()),
            (3, "carol".to_string()),
        ]
    );
}

/// Dynamic bucket with partitioned table.
#[tokio::test]
async fn test_pk_dynamic_bucket_partitioned() {
    let (_tmp, handler) = setup_handler().await;

    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_dyn_part (
                dt STRING, id INT NOT NULL, value INT,
                PRIMARY KEY (dt, id)
            ) PARTITIONED BY (dt STRING)",
        )
        .await
        .unwrap();

    handler
        .sql(
            "INSERT INTO paimon.test_db.t_dyn_part VALUES \
             ('2024-01-01', 1, 10), ('2024-01-01', 2, 20), \
             ('2024-01-02', 1, 100), ('2024-01-02', 2, 200)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Update within each partition
    handler
        .sql(
            "INSERT INTO paimon.test_db.t_dyn_part VALUES \
             ('2024-01-01', 1, 11), ('2024-01-02', 2, 222)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let batches = handler
        .sql("SELECT dt, id, value FROM paimon.test_db.t_dyn_part ORDER BY dt, id")
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
    rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

    assert_eq!(
        rows,
        vec![
            ("2024-01-01".to_string(), 1, 11),
            ("2024-01-01".to_string(), 2, 20),
            ("2024-01-02".to_string(), 1, 100),
            ("2024-01-02".to_string(), 2, 222),
        ]
    );
}

/// Dynamic bucket with three commits — verifies sequence number tracking.
#[tokio::test]
async fn test_pk_dynamic_bucket_three_commits() {
    let (_tmp, handler) = setup_handler().await;

    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_dyn3 (
                id INT NOT NULL, value INT,
                PRIMARY KEY (id)
            )",
        )
        .await
        .unwrap();

    handler
        .sql("INSERT INTO paimon.test_db.t_dyn3 VALUES (1, 10), (2, 20)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    handler
        .sql("INSERT INTO paimon.test_db.t_dyn3 VALUES (2, 200), (3, 30)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    handler
        .sql("INSERT INTO paimon.test_db.t_dyn3 VALUES (1, 100), (3, 300), (4, 40)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let rows = collect_id_value(
        &handler,
        "SELECT id, value FROM paimon.test_db.t_dyn3 ORDER BY id",
    )
    .await;

    assert_eq!(rows, vec![(1, 100), (2, 200), (3, 300), (4, 40)]);
}

/// Helper: read HASH index entries from a table's latest snapshot.
async fn read_hash_index_entries(table: &paimon::Table) -> Vec<IndexManifestEntry> {
    let sm = SnapshotManager::new(table.file_io().clone(), table.location().to_string());
    let snapshot = sm
        .get_latest_snapshot()
        .await
        .unwrap()
        .expect("no snapshot");
    let index_manifest_name = snapshot.index_manifest().expect("no index manifest");
    let path = format!("{}/manifest/{}", table.location(), index_manifest_name);
    let entries = IndexManifest::read(table.file_io(), &path).await.unwrap();
    entries
        .into_iter()
        .filter(|e| e.index_file.index_type == "HASH")
        .collect()
}

/// Helper: read raw hash values from a hash index file (flat i32 little-endian).
async fn read_hash_file(table: &paimon::Table, file_name: &str) -> Vec<i32> {
    let path = format!("{}/index/{}", table.location(), file_name);
    let input = table.file_io().new_input(&path).unwrap();
    let content = input.read().await.unwrap();
    assert!(content.len() % 4 == 0);
    (0..content.len() / 4)
        .map(|i| {
            let off = i * 4;
            i32::from_be_bytes([
                content[off],
                content[off + 1],
                content[off + 2],
                content[off + 3],
            ])
        })
        .collect()
}

/// Collect all hash values from all HASH index files of a table's latest snapshot.
async fn collect_all_hashes(table: &paimon::Table) -> Vec<i32> {
    let entries = read_hash_index_entries(table).await;
    let mut all_hashes = Vec::new();
    for entry in &entries {
        let hashes = read_hash_file(table, &entry.index_file.file_name).await;
        all_hashes.extend(hashes);
    }
    all_hashes.sort();
    all_hashes
}

/// INSERT OVERWRITE on an unpartitioned dynamic bucket table should replace
/// all data and rebuild the HASH index from scratch (old index entries cleared).
#[tokio::test]
async fn test_pk_dynamic_bucket_insert_overwrite() {
    let (_tmp, catalog) = create_test_env();
    let handler = create_handler(catalog.clone());
    handler
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA failed");

    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_dyn_ow (
                id INT NOT NULL, name STRING,
                PRIMARY KEY (id)
            )",
        )
        .await
        .unwrap();

    // Commit 1: insert 3 rows
    handler
        .sql("INSERT INTO paimon.test_db.t_dyn_ow VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let table = catalog
        .get_table(&Identifier::new("test_db", "t_dyn_ow"))
        .await
        .unwrap();
    let hashes_before = collect_all_hashes(&table).await;
    assert_eq!(hashes_before.len(), 3, "Should have 3 hash entries");

    // INSERT OVERWRITE with only 2 rows — old index entries must be cleared
    handler
        .sql("INSERT OVERWRITE paimon.test_db.t_dyn_ow VALUES (10, 'new_a'), (20, 'new_b')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Verify data
    let rows = collect_id_name(
        &handler,
        "SELECT id, name FROM paimon.test_db.t_dyn_ow ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        vec![(10, "new_a".to_string()), (20, "new_b".to_string())]
    );

    // Verify HASH index: should have exactly 2 entries (not 3+2=5)
    let table = catalog
        .get_table(&Identifier::new("test_db", "t_dyn_ow"))
        .await
        .unwrap();
    let hashes_after = collect_all_hashes(&table).await;
    assert_eq!(
        hashes_after.len(),
        2,
        "HASH index should have 2 entries after overwrite, got {}",
        hashes_after.len()
    );

    // Old hashes should not be present
    for h in &hashes_before {
        assert!(
            !hashes_after.contains(h),
            "Old hash {h} should not survive overwrite"
        );
    }
}

/// INSERT OVERWRITE on a partitioned dynamic bucket table should only clear
/// index entries for the overwritten partition, keeping other partitions intact.
#[tokio::test]
async fn test_pk_dynamic_bucket_partitioned_insert_overwrite() {
    let (_tmp, catalog) = create_test_env();
    let handler = create_handler(catalog.clone());
    handler
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .expect("CREATE SCHEMA failed");

    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_dyn_part_ow (
                dt STRING, id INT NOT NULL, value INT,
                PRIMARY KEY (dt, id)
            ) PARTITIONED BY (dt STRING)",
        )
        .await
        .unwrap();

    // Commit 1: two partitions
    handler
        .sql(
            "INSERT INTO paimon.test_db.t_dyn_part_ow VALUES \
             ('a', 1, 10), ('a', 2, 20), \
             ('b', 3, 30), ('b', 4, 40)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let table = catalog
        .get_table(&Identifier::new("test_db", "t_dyn_part_ow"))
        .await
        .unwrap();
    let entries_before = read_hash_index_entries(&table).await;
    // Should have index entries for both partitions
    assert!(
        entries_before.len() >= 2,
        "Should have index entries for both partitions"
    );

    // INSERT OVERWRITE partition 'a' with only 1 row
    handler
        .sql("INSERT OVERWRITE paimon.test_db.t_dyn_part_ow VALUES ('a', 5, 50)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Verify data: partition 'a' replaced, partition 'b' untouched
    let rows = collect_id_value(
        &handler,
        "SELECT id, value FROM paimon.test_db.t_dyn_part_ow ORDER BY id",
    )
    .await;
    assert_eq!(rows, vec![(3, 30), (4, 40), (5, 50)]);

    // Verify HASH index: partition 'b' entries should survive,
    // partition 'a' should have exactly 1 entry (not 2+1=3)
    let table = catalog
        .get_table(&Identifier::new("test_db", "t_dyn_part_ow"))
        .await
        .unwrap();
    let entries_after = read_hash_index_entries(&table).await;

    // Count entries per partition
    let mut partition_entry_counts: std::collections::HashMap<Vec<u8>, usize> =
        std::collections::HashMap::new();
    for entry in &entries_after {
        *partition_entry_counts
            .entry(entry.partition.clone())
            .or_insert(0) += 1;
    }

    // Total hash count: partition 'b' had 2 keys, partition 'a' now has 1 key
    let total_hashes: i64 = entries_after
        .iter()
        .map(|e| e.index_file.row_count as i64)
        .sum();
    assert_eq!(
        total_hashes, 3,
        "Total hash entries should be 3 (2 from 'b' + 1 from 'a'), got {total_hashes}"
    );
}

/// Read the Spark-provisioned dynamic_bucket_pk_table, write the same data
/// into a new dynamic bucket table, and verify the HASH index values are identical.
///
/// Spark may distribute rows across multiple buckets due to parallelism, so we
/// compare the aggregate set of hash values rather than per-bucket entries.
///
/// Requires: `make docker-up` + colima copy (see dev/spark/README.md).
#[tokio::test]
#[ignore]
async fn test_read_spark_dynamic_bucket_and_compare_index() {
    // --- Read from Spark-provisioned table ---
    let warehouse =
        std::env::var("PAIMON_TEST_WAREHOUSE").unwrap_or_else(|_| "/tmp/paimon-warehouse".into());
    let mut opts = Options::new();
    opts.set(CatalogOptions::WAREHOUSE, &warehouse);
    let spark_catalog = FileSystemCatalog::new(opts).unwrap();
    let spark_table = spark_catalog
        .get_table(&Identifier::new("default", "dynamic_bucket_pk_table"))
        .await
        .unwrap();

    // Read all rows from the Spark table
    let plan = spark_table
        .new_read_builder()
        .new_scan()
        .plan()
        .await
        .unwrap();
    let reader = spark_table.new_read_builder().new_read().unwrap();
    let stream = reader.to_arrow(plan.splits()).unwrap();

    use arrow_array::RecordBatch;
    use futures::StreamExt;
    let batches: Vec<RecordBatch> = stream
        .map(|r: paimon::Result<RecordBatch>| r.unwrap())
        .collect()
        .await;
    let mut all_rows: Vec<(i32, String)> = Vec::new();
    for batch in &batches {
        let ids = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .unwrap();
        let names = batch
            .column_by_name("name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .unwrap();
        for i in 0..batch.num_rows() {
            all_rows.push((ids.value(i), names.value(i).to_string()));
        }
    }
    all_rows.sort_by_key(|(id, _)| *id);

    // Verify Spark data: two commits with overlapping keys
    assert_eq!(
        all_rows,
        vec![
            (1, "alice".to_string()),
            (2, "bob-v2".to_string()),
            (3, "carol-v2".to_string()),
            (4, "dave".to_string()),
        ]
    );

    // --- Write the same data into a new dynamic bucket table ---
    let (_tmp, handler) = setup_handler().await;

    handler
        .sql(
            "CREATE TABLE paimon.test_db.t_dyn_cmp (
                id INT NOT NULL, name STRING,
                PRIMARY KEY (id)
            )",
        )
        .await
        .unwrap();

    // Replicate the same two commits as provision.py
    handler
        .sql("INSERT INTO paimon.test_db.t_dyn_cmp VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    handler
        .sql(
            "INSERT INTO paimon.test_db.t_dyn_cmp VALUES \
             (2, 'bob-v2'), (3, 'carol-v2'), (4, 'dave')",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Verify written data matches
    let rows = collect_id_name(
        &handler,
        "SELECT id, name FROM paimon.test_db.t_dyn_cmp ORDER BY id",
    )
    .await;
    assert_eq!(rows, all_rows);

    // --- Compare HASH index values ---
    let rust_catalog = {
        let mut opts = Options::new();
        opts.set(
            CatalogOptions::WAREHOUSE,
            format!("file://{}", _tmp.path().display()),
        );
        FileSystemCatalog::new(opts).unwrap()
    };
    let rust_table = rust_catalog
        .get_table(&Identifier::new("test_db", "t_dyn_cmp"))
        .await
        .unwrap();

    let spark_hashes = collect_all_hashes(&spark_table).await;
    let rust_hashes = collect_all_hashes(&rust_table).await;

    assert_eq!(
        spark_hashes, rust_hashes,
        "HASH index values mismatch between Spark and Rust tables"
    );
}
