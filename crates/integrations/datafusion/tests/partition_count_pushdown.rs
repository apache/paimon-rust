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

//! `SELECT <partition cols>, COUNT(*) ... GROUP BY <partition cols>` must be
//! answered from manifests — no table scan in the plan — and still agree with
//! counting rows, including on data-evolution tables.

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::physical_plan::displayable;
use paimon::catalog::Identifier;
use paimon::spec::IndexManifest;
use paimon::table::SnapshotManager;
use paimon::{Catalog, CatalogOptions, FileSystemCatalog, Options};
use paimon_datafusion::SQLContext;
use tempfile::TempDir;

async fn exec(ctx: &SQLContext, sql: &str) {
    ctx.sql(sql)
        .await
        .unwrap_or_else(|e| panic!("Failed to plan `{sql}`: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("Failed to execute `{sql}`: {e}"));
}

/// Rows as `(leading columns joined by '|', trailing Int64 count)`, sorted.
async fn rows(ctx: &SQLContext, sql: &str) -> Vec<(String, i64)> {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let mut out = Vec::new();
    for batch in &batches {
        let last = batch.num_columns() - 1;
        let counts = batch
            .column(last)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("last column of `{sql}` must be Int64"));
        for row in 0..batch.num_rows() {
            let key = (0..last)
                .map(|c| array_value_to_string(batch.column(c), row).unwrap())
                .collect::<Vec<_>>()
                .join("|");
            assert!(!counts.is_null(row));
            out.push((key, counts.value(row)));
        }
    }
    out.sort();
    out
}

async fn scans_table(ctx: &SQLContext, sql: &str) -> bool {
    let plan = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let rendered = displayable(plan.as_ref()).indent(true).to_string();
    rendered.contains("PaimonTableScan")
}

fn row(key: &str, count: i64) -> (String, i64) {
    (key.to_string(), count)
}

/// A data-evolution table partitioned by `(dt, content_key)` whose `name` column
/// was rewritten by MERGE INTO, so several files cover the same rows.
async fn setup() -> (TempDir, Arc<FileSystemCatalog>, SQLContext) {
    let temp_dir = TempDir::new().unwrap();
    let mut options = Options::new();
    options.set(
        CatalogOptions::WAREHOUSE,
        format!("file://{}", temp_dir.path().display()),
    );
    let catalog = Arc::new(FileSystemCatalog::new(options).unwrap());
    let mut ctx = SQLContext::new();
    ctx.register_catalog("paimon", catalog.clone())
        .await
        .unwrap();

    exec(&ctx, "CREATE SCHEMA paimon.test_db").await;
    exec(
        &ctx,
        "CREATE TABLE paimon.test_db.t (\
            id INT NOT NULL, name STRING, dt STRING, content_key STRING\
         ) PARTITIONED BY (dt, content_key) WITH (\
            'row-tracking.enabled' = 'true',\
            'data-evolution.enabled' = 'true'\
         )",
    )
    .await;
    exec(
        &ctx,
        "INSERT INTO paimon.test_db.t (id, name, dt, content_key) VALUES \
            (1, 'a', '2024-01-01', 'head'), (2, 'b', '2024-01-01', 'head'), \
            (3, 'c', '2024-01-01', 'tail'), (4, 'd', '2024-01-02', 'head')",
    )
    .await;
    exec(
        &ctx,
        "INSERT INTO paimon.test_db.t (id, name, dt, content_key) VALUES \
            (5, 'e', '2024-01-02', 'head'), (6, 'f', '2024-01-03', 'tail')",
    )
    .await;
    exec(
        &ctx,
        "CREATE TEMPORARY TABLE paimon.test_db.src AS \
         SELECT * FROM (VALUES (1, 'a2'), (2, 'b2'), (4, 'd2')) AS s(id, name)",
    )
    .await;
    exec(
        &ctx,
        "MERGE INTO paimon.test_db.t t USING paimon.test_db.src s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET name = s.name",
    )
    .await;
    (temp_dir, catalog, ctx)
}

#[tokio::test]
async fn test_grouped_count_with_partition_filter_is_answered_from_manifests() {
    let (_tmp, _catalog, ctx) = setup().await;
    let sql = "SELECT dt, COUNT(*) AS row_count FROM paimon.test_db.t \
               WHERE content_key = 'head' GROUP BY dt";

    assert!(
        !scans_table(&ctx, sql).await,
        "count must not scan the table"
    );
    assert_eq!(
        rows(&ctx, sql).await,
        vec![row("2024-01-01", 2), row("2024-01-02", 2)]
    );

    // COUNT(id) is not rewritten and really reads the rows: an independent oracle.
    let oracle = "SELECT dt, COUNT(id) FROM paimon.test_db.t \
                  WHERE content_key = 'head' GROUP BY dt";
    assert!(scans_table(&ctx, oracle).await);
    assert_eq!(rows(&ctx, oracle).await, rows(&ctx, sql).await);
}

#[tokio::test]
async fn test_manifest_reads_are_deferred_until_execution() {
    let (_tmp, catalog, ctx) = setup().await;
    let sql = "SELECT dt, COUNT(*) FROM paimon.test_db.t GROUP BY dt";
    let plan = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let rendered = displayable(plan.as_ref()).indent(true).to_string();
    assert!(rendered.contains("PartitionRowCountExec"), "{rendered}");
    assert!(!rendered.contains("PaimonTableScan"), "{rendered}");

    // If planning had already read the manifests, deleting the list now would
    // not affect this plan. Lazy execution must observe the missing file.
    let table = catalog
        .get_table(&Identifier::new("test_db", "t"))
        .await
        .unwrap();
    let snapshots = SnapshotManager::new(table.file_io().clone(), table.location().to_string());
    let snapshot = snapshots.get_latest_snapshot().await.unwrap().unwrap();
    table
        .file_io()
        .delete_file(&snapshots.manifest_path(snapshot.delta_manifest_list()))
        .await
        .unwrap();

    ctx.sql(&format!("EXPLAIN {sql}"))
        .await
        .unwrap()
        .collect()
        .await
        .expect("EXPLAIN must not read manifests");
    assert!(
        datafusion::physical_plan::collect(plan, ctx.ctx().task_ctx())
            .await
            .is_err(),
        "manifest reads must happen during execution"
    );
}

#[tokio::test]
async fn test_grouped_count_shapes() {
    let (_tmp, _catalog, ctx) = setup().await;

    let all_keys =
        "SELECT dt, content_key, COUNT(*) FROM paimon.test_db.t GROUP BY dt, content_key";
    assert!(!scans_table(&ctx, all_keys).await);
    assert_eq!(
        rows(&ctx, all_keys).await,
        vec![
            row("2024-01-01|head", 2),
            row("2024-01-01|tail", 1),
            row("2024-01-02|head", 2),
            row("2024-01-03|tail", 1),
        ]
    );

    let filtered = "SELECT content_key, COUNT(*) AS c FROM paimon.test_db.t \
                    WHERE dt IN ('2024-01-01', '2024-01-03') \
                    GROUP BY content_key HAVING COUNT(*) > 1 ORDER BY c DESC";
    assert!(!scans_table(&ctx, filtered).await);
    assert_eq!(
        rows(&ctx, filtered).await,
        vec![row("head", 2), row("tail", 2)]
    );

    let ungrouped = "SELECT COUNT(*) FROM paimon.test_db.t WHERE content_key = 'tail'";
    assert!(!scans_table(&ctx, ungrouped).await);
    assert_eq!(rows(&ctx, ungrouped).await, vec![row("", 2)]);

    let no_match = "SELECT COUNT(*) FROM paimon.test_db.t WHERE content_key = 'nope'";
    assert_eq!(rows(&ctx, no_match).await, vec![row("", 0)]);
    let no_match_grouped = "SELECT dt, COUNT(*) FROM paimon.test_db.t \
                            WHERE content_key = 'nope' GROUP BY dt";
    assert!(rows(&ctx, no_match_grouped).await.is_empty());

    let historical = "SELECT dt, COUNT(*) FROM paimon.test_db.t VERSION AS OF 1 GROUP BY dt";
    assert!(!scans_table(&ctx, historical).await);
    assert_eq!(
        rows(&ctx, historical).await,
        vec![row("2024-01-01", 3), row("2024-01-02", 1)]
    );
}

#[tokio::test]
async fn test_ineligible_counts_still_scan() {
    let (_tmp, _catalog, ctx) = setup().await;

    // A data-column filter cannot be decided from manifests.
    let data_filter = "SELECT dt, COUNT(*) FROM paimon.test_db.t WHERE id > 2 GROUP BY dt";
    assert!(scans_table(&ctx, data_filter).await);
    assert_eq!(
        rows(&ctx, data_filter).await,
        vec![
            row("2024-01-01", 1),
            row("2024-01-02", 2),
            row("2024-01-03", 1)
        ]
    );

    // Nor can grouping by a data column.
    let data_group = "SELECT name, COUNT(*) FROM paimon.test_db.t GROUP BY name";
    assert!(scans_table(&ctx, data_group).await);
    assert_eq!(rows(&ctx, data_group).await.len(), 6);

    exec(
        &ctx,
        "CREATE TABLE paimon.test_db.pk (id INT NOT NULL, dt STRING NOT NULL, v INT, \
            PRIMARY KEY (id, dt)) PARTITIONED BY (dt) WITH ('bucket' = '1')",
    )
    .await;
    exec(
        &ctx,
        "INSERT INTO paimon.test_db.pk VALUES (1, 'a', 1), (2, 'a', 1)",
    )
    .await;
    // A second version of key 1: two physical rows, one logical row.
    exec(&ctx, "INSERT INTO paimon.test_db.pk VALUES (1, 'a', 2)").await;

    let primary_key = "SELECT dt, COUNT(*) FROM paimon.test_db.pk GROUP BY dt";
    assert!(scans_table(&ctx, primary_key).await);
    assert_eq!(rows(&ctx, primary_key).await, vec![row("a", 2)]);
}

/// A deletion vector that does not record its cardinality leaves the manifests
/// unable to give an exact count; the rewritten plan must then count by reading.
#[tokio::test]
async fn test_unknown_deletion_vector_cardinality_falls_back_to_reading() {
    let temp_dir = TempDir::new().unwrap();
    let mut options = Options::new();
    options.set(
        CatalogOptions::WAREHOUSE,
        format!("file://{}", temp_dir.path().display()),
    );
    let catalog = Arc::new(FileSystemCatalog::new(options).unwrap());
    let mut ctx = SQLContext::new();
    ctx.register_catalog("paimon", catalog.clone())
        .await
        .unwrap();

    exec(&ctx, "CREATE SCHEMA paimon.test_db").await;
    exec(
        &ctx,
        "CREATE TABLE paimon.test_db.t (id INT NOT NULL, name STRING, dt STRING) \
         PARTITIONED BY (dt) WITH (\
            'row-tracking.enabled' = 'true',\
            'data-evolution.enabled' = 'true',\
            'deletion-vectors.enabled' = 'true'\
         )",
    )
    .await;
    exec(
        &ctx,
        "INSERT INTO paimon.test_db.t (id, name, dt) VALUES \
            (1, 'a', '2024-01-01'), (2, 'b', '2024-01-01'), (3, 'c', '2024-01-02')",
    )
    .await;
    exec(
        &ctx,
        "CREATE TEMPORARY TABLE paimon.test_db.del AS SELECT * FROM (VALUES (1)) AS s(id)",
    )
    .await;
    exec(
        &ctx,
        "MERGE INTO paimon.test_db.t t USING paimon.test_db.del s ON t.id = s.id \
         WHEN MATCHED THEN DELETE",
    )
    .await;

    let sql = "SELECT dt, COUNT(*) FROM paimon.test_db.t GROUP BY dt";
    let expected = vec![row("2024-01-01", 1), row("2024-01-02", 1)];
    assert!(!scans_table(&ctx, sql).await);
    assert_eq!(rows(&ctx, sql).await, expected);

    // Erase the cardinalities, as written by producers that never recorded them.
    let table = catalog
        .get_table(&Identifier::new("test_db", "t"))
        .await
        .unwrap();
    let snapshots = SnapshotManager::new(table.file_io().clone(), table.location().to_string());
    let snapshot = snapshots.get_latest_snapshot().await.unwrap().unwrap();
    let path = snapshots.manifest_path(snapshot.index_manifest().unwrap());
    let mut entries = IndexManifest::read(table.file_io(), &path).await.unwrap();
    let mut erased = 0;
    for entry in &mut entries {
        for vector in entry
            .index_file
            .deletion_vectors_ranges
            .iter_mut()
            .flat_map(|ranges| ranges.values_mut())
        {
            vector.cardinality = None;
            erased += 1;
        }
    }
    assert!(
        erased > 0,
        "the delete must have produced a deletion vector"
    );
    table.file_io().delete_file(&path).await.unwrap();
    IndexManifest::write(table.file_io(), &path, &entries)
        .await
        .unwrap();

    // The fallback is selected lazily during execution, so it is intentionally
    // absent from the physical plan produced above.
    assert!(!scans_table(&ctx, sql).await);
    assert_eq!(rows(&ctx, sql).await, expected);
}

#[tokio::test]
async fn test_ungrouped_count_on_unpartitioned_and_empty_tables() {
    let (_tmp, _catalog, ctx) = setup().await;
    exec(&ctx, "CREATE TABLE paimon.test_db.flat (id INT NOT NULL)").await;

    let sql = "SELECT COUNT(*) FROM paimon.test_db.flat";
    assert!(!scans_table(&ctx, sql).await);
    assert_eq!(rows(&ctx, sql).await, vec![row("", 0)]);

    exec(&ctx, "INSERT INTO paimon.test_db.flat VALUES (1), (2)").await;
    exec(&ctx, "INSERT INTO paimon.test_db.flat VALUES (3)").await;
    assert_eq!(rows(&ctx, sql).await, vec![row("", 3)]);
}
