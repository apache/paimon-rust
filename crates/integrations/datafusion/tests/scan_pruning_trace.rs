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

//! Self-contained baselines for scan-pruning trace counters.

mod common;

use std::collections::HashMap;

use datafusion::arrow::array::Int64Array;
use datafusion::physical_plan::displayable;
use paimon::catalog::Identifier;
use paimon::spec::{Datum, PredicateBuilder};
use paimon::{Catalog, Table};

async fn setup_trace_table() -> (tempfile::TempDir, std::sync::Arc<paimon::FileSystemCatalog>) {
    let (tmp, catalog) = common::create_test_env();
    let sql_context = common::create_sql_context(catalog.clone()).await;
    sql_context
        .sql("CREATE SCHEMA paimon.test_db")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    sql_context
        .sql(
            "CREATE TABLE paimon.test_db.trace_append (
                id INT, value INT, dt STRING
             ) PARTITIONED BY (dt)",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    common::exec(
        &sql_context,
        "INSERT INTO paimon.test_db.trace_append VALUES
         (1, 10, '2024-01-01'), (2, 20, '2024-01-01')",
    )
    .await;
    common::exec(
        &sql_context,
        "INSERT INTO paimon.test_db.trace_append VALUES
         (100, 1000, '2024-01-02'), (101, 1010, '2024-01-02')",
    )
    .await;
    common::exec(
        &sql_context,
        "INSERT INTO paimon.test_db.trace_append VALUES
         (200, 2000, '2024-01-03'), (201, 2010, '2024-01-03')",
    )
    .await;

    (tmp, catalog)
}

async fn load_table(
    catalog: &std::sync::Arc<paimon::FileSystemCatalog>,
    table_name: &str,
) -> Table {
    catalog
        .get_table(&Identifier::new("test_db", table_name))
        .await
        .unwrap()
}

fn trace_counter(plan_text: &str, key: &str) -> usize {
    let needle = format!("{key}=");
    let start = plan_text
        .find(&needle)
        .unwrap_or_else(|| panic!("trace counter {key} not found in plan:\n{plan_text}"))
        + needle.len();
    let end = plan_text[start..]
        .find(|c: char| !c.is_ascii_digit())
        .map(|offset| start + offset)
        .unwrap_or(plan_text.len());
    plan_text[start..end].parse().unwrap()
}

fn trace_manifest_counts(plan_text: &str) -> (usize, usize) {
    let needle = "manifests=";
    let start = plan_text
        .find(needle)
        .unwrap_or_else(|| panic!("manifest counts not found in plan:\n{plan_text}"))
        + needle.len();
    let rest = &plan_text[start..];
    let slash = rest
        .find('/')
        .unwrap_or_else(|| panic!("manifest counts missing slash in plan:\n{plan_text}"));
    let after: usize = rest[..slash].parse().unwrap();
    let before_len = rest[slash + 1..]
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(rest.len() - slash - 1);
    let before: usize = rest[slash + 1..slash + 1 + before_len].parse().unwrap();
    (after, before)
}

#[tokio::test]
async fn test_scan_trace_records_partition_pruning() {
    let (_tmp, catalog) = setup_trace_table().await;
    let table = load_table(&catalog, "trace_append").await;

    let (_all_plan, all_trace) = table
        .new_read_builder()
        .new_scan()
        .plan_with_trace()
        .await
        .unwrap();
    assert_eq!(all_trace.snapshot_id, Some(3));
    assert_eq!(all_trace.final_files, 3);

    let fields = table.schema().fields();
    let pb = PredicateBuilder::new(fields);
    let mut partition_reader = table.new_read_builder();
    partition_reader.with_filter(
        pb.equal("dt", Datum::String("2024-01-01".to_string()))
            .unwrap(),
    );
    let (_partition_plan, partition_trace) =
        partition_reader.new_scan().plan_with_trace().await.unwrap();
    assert!(
        partition_trace.manifest_files_after_partition_pruning
            < partition_trace.manifest_files_before_partition_pruning
            || partition_trace.manifest_entries_pruned_by_partition > 0,
        "partition predicate should prune at manifest or entry level: {partition_trace:?}"
    );
    assert!(
        partition_trace.final_files < all_trace.final_files,
        "partition pruning should reduce final files: all={all_trace:?}, filtered={partition_trace:?}"
    );
}

#[tokio::test]
async fn test_sql_between_records_partition_pruning_trace() {
    let (_tmp, catalog) = setup_trace_table().await;
    let sql_context = common::create_sql_context(catalog).await;
    let sql = "SELECT id, value FROM paimon.test_db.trace_append
               WHERE dt BETWEEN '2024-01-01' AND '2024-01-02'";

    let plan = sql_context
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let plan_text = displayable(plan.as_ref()).indent(true).to_string();
    let (manifests_after, manifests_before) = trace_manifest_counts(&plan_text);
    let partition_pruned = trace_counter(&plan_text, "partition_pruned");
    assert!(
        manifests_after < manifests_before || partition_pruned > 0,
        "SQL BETWEEN over partition column should prune at manifest or entry level:\n{plan_text}"
    );

    let rows = common::collect_id_value(&sql_context, sql).await;
    assert_eq!(rows, vec![(1, 10), (2, 20), (100, 1000), (101, 1010)]);
}

#[tokio::test]
async fn test_count_star_uses_statistics_without_scan_trace() {
    let (_tmp, catalog) = setup_trace_table().await;
    let sql_context = common::create_sql_context(catalog).await;
    let sql = "SELECT COUNT(*) FROM paimon.test_db.trace_append";

    let df = sql_context.sql(sql).await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let plan_text = displayable(plan.as_ref()).indent(true).to_string();
    assert!(
        !plan_text.contains("PaimonTableScan"),
        "COUNT(*) should be satisfied from exact scan statistics without data scan:\n{plan_text}"
    );
    assert!(
        !plan_text.contains("trace="),
        "COUNT(*) statistics rewrite should remove the scan node before trace display:\n{plan_text}"
    );

    let batches = sql_context.sql(sql).await.unwrap().collect().await.unwrap();
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 6);
}

#[tokio::test]
async fn test_scan_trace_records_bucket_limit_and_time_travel() {
    let (tmp, catalog) = common::create_test_env();
    let sql_context = common::create_sql_context(catalog.clone()).await;
    common::exec(&sql_context, "CREATE SCHEMA paimon.test_db").await;
    common::exec(
        &sql_context,
        "CREATE TABLE paimon.test_db.trace_pk (
            id INT NOT NULL, value INT,
            PRIMARY KEY (id)
         ) WITH ('bucket' = '4')",
    )
    .await;
    common::exec(
        &sql_context,
        "INSERT INTO paimon.test_db.trace_pk VALUES
         (1, 10), (2, 20), (3, 30), (4, 40)",
    )
    .await;
    common::exec(
        &sql_context,
        "INSERT INTO paimon.test_db.trace_pk VALUES
         (5, 50), (6, 60), (7, 70), (8, 80)",
    )
    .await;

    let table = load_table(&catalog, "trace_pk").await;
    let fields = table.schema().fields();
    let pb = PredicateBuilder::new(fields);
    let mut bucket_reader = table.new_read_builder();
    bucket_reader.with_filter(pb.equal("id", Datum::Int(1)).unwrap());
    let (_bucket_plan, bucket_trace) = bucket_reader.new_scan().plan_with_trace().await.unwrap();
    assert!(
        bucket_trace.manifest_entries_pruned_by_bucket > 0,
        "bucket-key predicate should prune manifest entries by bucket: {bucket_trace:?}"
    );

    let (_full_plan, full_trace) = table
        .new_read_builder()
        .new_scan()
        .plan_with_trace()
        .await
        .unwrap();
    let mut limit_reader = table.new_read_builder();
    limit_reader.with_limit(1);
    let (_limit_plan, limit_trace) = limit_reader.new_scan().plan_with_trace().await.unwrap();
    assert!(
        limit_trace.limit_early_stopped,
        "LIMIT should stop split construction early when no data residual exists: {limit_trace:?}"
    );
    assert!(
        limit_trace.split_candidates_built < full_trace.split_candidates_built,
        "LIMIT should build fewer split candidates: limited={limit_trace:?}, full={full_trace:?}"
    );

    let snapshot_one_table = table.copy_with_options(HashMap::from([(
        "scan.version".to_string(),
        "1".to_string(),
    )]));
    let (_snapshot_one_plan, snapshot_one_trace) = snapshot_one_table
        .new_read_builder()
        .new_scan()
        .plan_with_trace()
        .await
        .unwrap();
    assert_eq!(snapshot_one_trace.snapshot_id, Some(1));

    let (_latest_plan, latest_trace) = table
        .new_read_builder()
        .new_scan()
        .plan_with_trace()
        .await
        .unwrap();
    assert_eq!(latest_trace.snapshot_id, Some(2));
    assert!(
        snapshot_one_trace.final_files < latest_trace.final_files,
        "time travel should plan from the selected snapshot: snapshot1={snapshot_one_trace:?}, latest={latest_trace:?}"
    );

    drop(tmp);
}

#[tokio::test]
async fn test_physical_plan_displays_scan_trace_summary() {
    let (_tmp, catalog) = setup_trace_table().await;
    let sql_context = common::create_sql_context(catalog).await;
    let plan = sql_context
        .sql("SELECT id FROM paimon.test_db.trace_append LIMIT 1")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let plan_text = displayable(plan.as_ref()).indent(true).to_string();

    assert!(
        plan_text.contains("trace=") && plan_text.contains("splits_before_limit="),
        "physical plan should include scan trace summary:\n{plan_text}"
    );
}
