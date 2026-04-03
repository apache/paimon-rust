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

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array, StringArray};
use datafusion::catalog::CatalogProvider;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{col, lit, TableProviderFilterPushDown};
use datafusion::prelude::{SessionConfig, SessionContext};
use paimon::catalog::Identifier;
use paimon::{Catalog, CatalogOptions, FileSystemCatalog, Options};
use paimon_datafusion::{PaimonCatalogProvider, PaimonRelationPlanner, PaimonTableProvider};

fn get_test_warehouse() -> String {
    std::env::var("PAIMON_TEST_WAREHOUSE").unwrap_or_else(|_| "/tmp/paimon-warehouse".to_string())
}

fn create_catalog() -> FileSystemCatalog {
    let warehouse = get_test_warehouse();
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse);
    FileSystemCatalog::new(options).expect("Failed to create catalog")
}

async fn create_context(table_name: &str) -> SessionContext {
    let provider = create_provider(table_name).await;
    let ctx = SessionContext::new();
    ctx.register_table(table_name, Arc::new(provider))
        .expect("Failed to register table");

    ctx
}

async fn create_provider(table_name: &str) -> PaimonTableProvider {
    let catalog = create_catalog();
    let identifier = Identifier::new("default", table_name);
    let table = catalog
        .get_table(&identifier)
        .await
        .expect("Failed to get table");

    PaimonTableProvider::try_new(table).expect("Failed to create table provider")
}

async fn read_rows(table_name: &str) -> Vec<(i32, String)> {
    let batches = collect_query(table_name, &format!("SELECT id, name FROM {table_name}"))
        .await
        .expect("Failed to collect query result");

    assert!(
        !batches.is_empty(),
        "Expected at least one batch from table {table_name}"
    );

    let mut actual_rows = extract_id_name_rows(&batches);
    actual_rows.sort_by_key(|(id, _)| *id);
    actual_rows
}

async fn collect_query(
    table_name: &str,
    sql: &str,
) -> datafusion::error::Result<Vec<datafusion::arrow::record_batch::RecordBatch>> {
    let ctx = create_context(table_name).await;

    ctx.sql(sql).await?.collect().await
}

fn extract_id_name_rows(
    batches: &[datafusion::arrow::record_batch::RecordBatch],
) -> Vec<(i32, String)> {
    let mut rows = Vec::new();
    for batch in batches {
        let id_array = batch
            .column_by_name("id")
            .and_then(|column| column.as_any().downcast_ref::<Int32Array>())
            .expect("Expected Int32Array for id column");
        let name_array = batch
            .column_by_name("name")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .expect("Expected StringArray for name column");

        for row_index in 0..batch.num_rows() {
            rows.push((
                id_array.value(row_index),
                name_array.value(row_index).to_string(),
            ));
        }
    }
    rows
}

#[tokio::test]
async fn test_read_log_table_via_datafusion() {
    let actual_rows = read_rows("simple_log_table").await;
    let expected_rows = vec![
        (1, "alice".to_string()),
        (2, "bob".to_string()),
        (3, "carol".to_string()),
    ];

    assert_eq!(
        actual_rows, expected_rows,
        "Rows should match expected values"
    );
}

#[tokio::test]
async fn test_read_primary_key_table_via_datafusion() {
    let actual_rows = read_rows("simple_dv_pk_table").await;
    let expected_rows = vec![
        (1, "alice-v2".to_string()),
        (2, "bob-v2".to_string()),
        (3, "carol-v2".to_string()),
        (4, "dave-v2".to_string()),
        (5, "eve-v2".to_string()),
        (6, "frank-v1".to_string()),
    ];

    assert_eq!(
        actual_rows, expected_rows,
        "Primary key table rows should match expected values"
    );
}

#[tokio::test]
async fn test_projection_via_datafusion() {
    let batches = collect_query("simple_log_table", "SELECT id FROM simple_log_table")
        .await
        .expect("Subset projection should succeed");

    assert!(
        !batches.is_empty(),
        "Expected at least one batch from projected query"
    );

    let mut actual_ids = Vec::new();
    for batch in &batches {
        let schema = batch.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            field_names,
            vec!["id"],
            "Projected query should only return 'id' column"
        );

        let id_array = batch
            .column_by_name("id")
            .and_then(|col| col.as_any().downcast_ref::<Int32Array>())
            .expect("Expected Int32Array for id column");
        for i in 0..id_array.len() {
            actual_ids.push(id_array.value(i));
        }
    }

    actual_ids.sort();
    assert_eq!(
        actual_ids,
        vec![1, 2, 3],
        "Projected id values should match"
    );
}

#[tokio::test]
async fn test_supports_partition_filters_pushdown() {
    let provider = create_provider("multi_partitioned_log_table").await;
    let partition_filter = col("dt").eq(lit("2024-01-01"));
    let mixed_and_filter = col("dt").eq(lit("2024-01-01")).and(col("id").gt(lit(1)));
    let data_filter = col("id").gt(lit(1));

    let supports = provider
        .supports_filters_pushdown(&[&partition_filter, &mixed_and_filter, &data_filter])
        .expect("supports_filters_pushdown should succeed");

    assert_eq!(
        supports,
        vec![
            TableProviderFilterPushDown::Exact,
            TableProviderFilterPushDown::Inexact,
            TableProviderFilterPushDown::Inexact,
        ]
    );
}

/// Verifies that `PaimonTableProvider::scan()` produces more than one
/// execution partition for a multi-partition table, and that the reported
/// partition count is still capped by `target_partitions`.
#[tokio::test]
async fn test_scan_partition_count_respects_session_config() {
    let provider = create_provider("partitioned_log_table").await;

    // With generous target_partitions, the plan should expose more than one partition.
    let config = SessionConfig::new().with_target_partitions(8);
    let ctx = SessionContext::new_with_config(config);
    let state = ctx.state();
    let plan = provider
        .scan(&state, None, &[], None)
        .await
        .expect("scan() should succeed");

    let partition_count = plan.properties().output_partitioning().partition_count();
    assert!(
        partition_count > 1,
        "partitioned_log_table should produce >1 partitions, got {partition_count}"
    );

    // With target_partitions=1, all splits must be coalesced into a single partition
    let config_single = SessionConfig::new().with_target_partitions(1);
    let ctx_single = SessionContext::new_with_config(config_single);
    let state_single = ctx_single.state();
    let plan_single = provider
        .scan(&state_single, None, &[], None)
        .await
        .expect("scan() should succeed with target_partitions=1");

    assert_eq!(
        plan_single
            .properties()
            .output_partitioning()
            .partition_count(),
        1,
        "target_partitions=1 should coalesce all splits into exactly 1 partition"
    );
}

#[tokio::test]
async fn test_partition_filter_query_via_datafusion() {
    let batches = collect_query(
        "partitioned_log_table",
        "SELECT id, name FROM partitioned_log_table WHERE dt = '2024-01-01'",
    )
    .await
    .expect("Partition filter query should succeed");

    let mut actual_rows = extract_id_name_rows(&batches);
    actual_rows.sort_by_key(|(id, _)| *id);
    assert_eq!(
        actual_rows,
        vec![(1, "alice".to_string()), (2, "bob".to_string())]
    );
}

#[tokio::test]
async fn test_multi_partition_filter_query_via_datafusion() {
    let batches = collect_query(
        "multi_partitioned_log_table",
        "SELECT id, name FROM multi_partitioned_log_table WHERE dt = '2024-01-01' AND hr = 10",
    )
    .await
    .expect("Multi-partition filter query should succeed");

    let mut actual_rows = extract_id_name_rows(&batches);
    actual_rows.sort_by_key(|(id, _)| *id);
    assert_eq!(
        actual_rows,
        vec![(1, "alice".to_string()), (2, "bob".to_string())]
    );
}

#[tokio::test]
async fn test_mixed_and_filter_keeps_residual_datafusion_filter() {
    let batches = collect_query(
        "partitioned_log_table",
        "SELECT id, name FROM partitioned_log_table WHERE dt = '2024-01-01' AND id > 1",
    )
    .await
    .expect("Mixed filter query should succeed");

    let actual_rows = extract_id_name_rows(&batches);

    assert_eq!(actual_rows, vec![(2, "bob".to_string())]);
}

/// Test limit pushdown: ensures that LIMIT queries return the correct number of rows.
#[tokio::test]
async fn test_limit_pushdown() {
    // Test append-only table (simple_log_table)
    {
        let batches = collect_query(
            "simple_log_table",
            "SELECT id, name FROM simple_log_table LIMIT 2",
        )
        .await
        .expect("Limit query should succeed");

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2, "LIMIT 2 should return exactly 2 rows");
    }

    // Test data evolution table
    {
        let batches = collect_query(
            "data_evolution_table",
            "SELECT id, name FROM data_evolution_table LIMIT 3",
        )
        .await
        .expect("Limit query on data evolution table should succeed");

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 3,
            "LIMIT 3 should return exactly 3 rows for data evolution table"
        );

        // Verify the data is from the merged result (not raw files)
        let mut rows = extract_id_name_rows(&batches);
        rows.sort_by_key(|(id, _)| *id);

        // LIMIT 3 returns ids 1, 2, 3 with merged values
        assert_eq!(
            rows,
            vec![
                (1, "alice-v2".to_string()),
                (2, "bob".to_string()),
                (3, "carol-v2".to_string()),
            ],
            "Data evolution table LIMIT 3 should return merged rows"
        );
    }
}

// ======================= Catalog Provider Tests =======================
#[tokio::test]
async fn test_query_via_catalog_provider() {
    let catalog = create_catalog();
    let provider = PaimonCatalogProvider::new(Arc::new(catalog));

    let ctx = SessionContext::new();
    ctx.register_catalog("paimon", Arc::new(provider));

    let df = ctx
        .sql("SELECT id, name FROM paimon.default.simple_log_table")
        .await
        .expect("Failed to execute query");

    let batches = df.collect().await.expect("Failed to collect results");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "Expected 3 rows from simple_log_table");
}

#[tokio::test]
async fn test_missing_database_returns_no_schema() {
    let catalog = create_catalog();
    let provider = PaimonCatalogProvider::new(Arc::new(catalog));

    assert!(
        provider.schema("definitely_missing_database").is_none(),
        "missing databases should not resolve to a schema provider"
    );
}

// ======================= Time Travel Tests =======================

/// Helper: create a SessionContext with catalog + relation planner for time travel.
/// Uses BigQuery dialect to enable `FOR SYSTEM_TIME AS OF` syntax.
async fn create_time_travel_context() -> SessionContext {
    let catalog = create_catalog();
    let config = SessionConfig::new().set_str("datafusion.sql_parser.dialect", "BigQuery");
    let ctx = SessionContext::new_with_config(config);
    ctx.register_catalog(
        "paimon",
        Arc::new(PaimonCatalogProvider::new(Arc::new(catalog))),
    );
    ctx.register_relation_planner(Arc::new(PaimonRelationPlanner::new()))
        .expect("Failed to register relation planner");
    ctx
}

#[tokio::test]
async fn test_time_travel_by_snapshot_id() {
    let ctx = create_time_travel_context().await;

    // Snapshot 1: should contain only the first insert (alice, bob)
    let batches = ctx
        .sql("SELECT id, name FROM paimon.default.time_travel_table FOR SYSTEM_TIME AS OF 1")
        .await
        .expect("time travel query should parse")
        .collect()
        .await
        .expect("time travel query should execute");

    let mut rows = extract_id_name_rows(&batches);
    rows.sort_by_key(|(id, _)| *id);
    assert_eq!(
        rows,
        vec![(1, "alice".to_string()), (2, "bob".to_string())],
        "Snapshot 1 should contain only the first batch of rows"
    );

    // Snapshot 2 (latest): should contain all rows
    let batches = ctx
        .sql("SELECT id, name FROM paimon.default.time_travel_table FOR SYSTEM_TIME AS OF 2")
        .await
        .expect("time travel query should parse")
        .collect()
        .await
        .expect("time travel query should execute");

    let mut rows = extract_id_name_rows(&batches);
    rows.sort_by_key(|(id, _)| *id);
    assert_eq!(
        rows,
        vec![
            (1, "alice".to_string()),
            (2, "bob".to_string()),
            (3, "carol".to_string()),
            (4, "dave".to_string()),
        ],
        "Snapshot 2 should contain all rows"
    );
}

#[tokio::test]
async fn test_time_travel_by_tag_name() {
    let ctx = create_time_travel_context().await;

    // Tag 'snapshot1' points to snapshot 1: should contain only (alice, bob)
    let batches = ctx
        .sql("SELECT id, name FROM paimon.default.time_travel_table FOR SYSTEM_TIME AS OF 'snapshot1'")
        .await
        .expect("tag time travel query should parse")
        .collect()
        .await
        .expect("tag time travel query should execute");

    let mut rows = extract_id_name_rows(&batches);
    rows.sort_by_key(|(id, _)| *id);
    assert_eq!(
        rows,
        vec![(1, "alice".to_string()), (2, "bob".to_string())],
        "Tag 'snapshot1' should contain only the first batch of rows"
    );

    // Tag 'snapshot2' points to snapshot 2: should contain all rows
    let batches = ctx
        .sql("SELECT id, name FROM paimon.default.time_travel_table FOR SYSTEM_TIME AS OF 'snapshot2'")
        .await
        .expect("tag time travel query should parse")
        .collect()
        .await
        .expect("tag time travel query should execute");

    let mut rows = extract_id_name_rows(&batches);
    rows.sort_by_key(|(id, _)| *id);
    assert_eq!(
        rows,
        vec![
            (1, "alice".to_string()),
            (2, "bob".to_string()),
            (3, "carol".to_string()),
            (4, "dave".to_string()),
        ],
        "Tag 'snapshot2' should contain all rows"
    );
}

/// Verifies that data evolution merge correctly NULL-fills columns that no file in a
/// merge group provides (e.g. a newly added column after MERGE INTO on old rows).
/// Without the fix, `active_file_indices` would be empty and rows would be silently lost.
#[tokio::test]
async fn test_data_evolution_drop_column_null_fill() {
    let batches = collect_query(
        "data_evolution_drop_column",
        "SELECT id, name, extra FROM data_evolution_drop_column",
    )
    .await
    .expect("data_evolution_drop_column query should succeed");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 3,
        "Should return 3 rows (not silently drop rows from merge groups missing the new column)"
    );

    let mut rows: Vec<(i32, String, Option<String>)> = Vec::new();
    for batch in &batches {
        let id_array = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("Expected Int32Array for id");
        let name_array = batch
            .column_by_name("name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("Expected StringArray for name");
        let extra_array = batch
            .column_by_name("extra")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("Expected StringArray for extra");

        for i in 0..batch.num_rows() {
            let extra = if extra_array.is_null(i) {
                None
            } else {
                Some(extra_array.value(i).to_string())
            };
            rows.push((id_array.value(i), name_array.value(i).to_string(), extra));
        }
    }
    rows.sort_by_key(|(id, _, _)| *id);

    assert_eq!(
        rows,
        vec![
            (1, "alice-v2".to_string(), None),
            (2, "bob".to_string(), None),
            (3, "carol".to_string(), Some("new".to_string())),
        ],
        "Old rows should have extra=NULL, new row should have extra='new'"
    );
}
