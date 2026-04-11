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

use std::collections::HashMap;
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

async fn create_provider_with_options(
    table_name: &str,
    extra_options: HashMap<String, String>,
) -> PaimonTableProvider {
    let catalog = create_catalog();
    let identifier = Identifier::new("default", table_name);
    let table = catalog
        .get_table(&identifier)
        .await
        .expect("Failed to get table")
        .copy_with_options(extra_options);

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
/// Uses Databricks dialect to enable `VERSION AS OF` and `TIMESTAMP AS OF` syntax.
async fn create_time_travel_context() -> SessionContext {
    let catalog = create_catalog();
    let config = SessionConfig::new().set_str("datafusion.sql_parser.dialect", "Databricks");
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
        .sql("SELECT id, name FROM paimon.default.time_travel_table VERSION AS OF 1")
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
        .sql("SELECT id, name FROM paimon.default.time_travel_table VERSION AS OF 2")
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
    // Tag-based time travel uses `scan.version` option directly since
    // `VERSION AS OF` in SQL only accepts numeric values.
    let provider = create_provider_with_options(
        "time_travel_table",
        HashMap::from([("scan.version".to_string(), "snapshot1".to_string())]),
    )
    .await;

    let ctx = SessionContext::new();
    ctx.register_table("time_travel_table", Arc::new(provider))
        .expect("Failed to register table");

    // Tag 'snapshot1' points to snapshot 1: should contain only (alice, bob)
    let batches = ctx
        .sql("SELECT id, name FROM time_travel_table")
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
    let provider2 = create_provider_with_options(
        "time_travel_table",
        HashMap::from([("scan.version".to_string(), "snapshot2".to_string())]),
    )
    .await;

    let ctx2 = SessionContext::new();
    ctx2.register_table("time_travel_table", Arc::new(provider2))
        .expect("Failed to register table");

    let batches = ctx2
        .sql("SELECT id, name FROM time_travel_table")
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

#[tokio::test]
async fn test_time_travel_conflicting_selectors_fail() {
    let provider = create_provider_with_options(
        "time_travel_table",
        HashMap::from([("scan.timestamp-millis".to_string(), "1234".to_string())]),
    )
    .await;

    let config = SessionConfig::new().set_str("datafusion.sql_parser.dialect", "Databricks");
    let ctx = SessionContext::new_with_config(config);
    ctx.register_table("time_travel_table", Arc::new(provider))
        .expect("Failed to register table");
    ctx.register_relation_planner(Arc::new(PaimonRelationPlanner::new()))
        .expect("Failed to register relation planner");

    let err = ctx
        .sql("SELECT id, name FROM time_travel_table VERSION AS OF 2")
        .await
        .expect("time travel query should parse")
        .collect()
        .await
        .expect_err("conflicting time-travel selectors should fail");

    let message = err.to_string();
    assert!(
        message.contains("Only one time-travel selector may be set"),
        "unexpected conflict error: {message}"
    );
    assert!(
        message.contains("scan.version"),
        "conflict error should mention scan.version: {message}"
    );
}

#[tokio::test]
async fn test_time_travel_invalid_version_fails() {
    let provider = create_provider_with_options(
        "time_travel_table",
        HashMap::from([("scan.version".to_string(), "nonexistent-tag".to_string())]),
    )
    .await;

    let ctx = SessionContext::new();
    ctx.register_table("time_travel_table", Arc::new(provider))
        .expect("Failed to register table");

    let err = ctx
        .sql("SELECT id, name FROM time_travel_table")
        .await
        .expect("query should parse")
        .collect()
        .await
        .expect_err("invalid version should fail");

    let message = err.to_string();
    assert!(
        message.contains("is not a valid tag name or snapshot id"),
        "unexpected invalid version error: {message}"
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

// ======================= Complex Type Tests =======================

#[tokio::test]
async fn test_read_complex_type_table_via_datafusion() {
    let batches = collect_query(
        "complex_type_table",
        "SELECT id, int_array, string_map, row_field FROM complex_type_table ORDER BY id",
    )
    .await
    .expect("Complex type query should succeed");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "Expected 3 rows from complex_type_table");

    // Verify column types exist and are correct
    for batch in &batches {
        let schema = batch.schema();
        assert!(
            schema.field_with_name("int_array").is_ok(),
            "int_array column should exist"
        );
        assert!(
            schema.field_with_name("string_map").is_ok(),
            "string_map column should exist"
        );
        assert!(
            schema.field_with_name("row_field").is_ok(),
            "row_field column should exist"
        );
    }

    // Extract and verify data using Arrow arrays
    let mut rows: Vec<(i32, String, String, String)> = Vec::new();
    for batch in &batches {
        let id_array = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("Expected Int32Array for id");
        let int_array_col = batch.column_by_name("int_array").expect("int_array");
        let string_map_col = batch.column_by_name("string_map").expect("string_map");
        let row_field_col = batch.column_by_name("row_field").expect("row_field");

        for i in 0..batch.num_rows() {
            use datafusion::arrow::util::display::ArrayFormatter;
            let fmt_opts = datafusion::arrow::util::display::FormatOptions::default();

            let arr_fmt = ArrayFormatter::try_new(int_array_col.as_ref(), &fmt_opts).unwrap();
            let map_fmt = ArrayFormatter::try_new(string_map_col.as_ref(), &fmt_opts).unwrap();
            let row_fmt = ArrayFormatter::try_new(row_field_col.as_ref(), &fmt_opts).unwrap();

            rows.push((
                id_array.value(i),
                arr_fmt.value(i).to_string(),
                map_fmt.value(i).to_string(),
                row_fmt.value(i).to_string(),
            ));
        }
    }
    rows.sort_by_key(|(id, _, _, _)| *id);

    assert_eq!(rows[0].0, 1);
    assert_eq!(rows[0].1, "[1, 2, 3]");
    assert_eq!(rows[0].2, "{a: 10, b: 20}");
    assert_eq!(rows[0].3, "{name: alice, value: 100}");

    assert_eq!(rows[1].0, 2);
    assert_eq!(rows[1].1, "[4, 5]");
    assert_eq!(rows[1].2, "{c: 30}");
    assert_eq!(rows[1].3, "{name: bob, value: 200}");

    assert_eq!(rows[2].0, 3);
    assert_eq!(rows[2].1, "[]");
    assert_eq!(rows[2].2, "{}");
    assert_eq!(rows[2].3, "{name: carol, value: 300}");
}

#[tokio::test]
async fn test_select_row_id_from_data_evolution_table() {
    use datafusion::arrow::array::Int64Array;

    let ctx = create_context("data_evolution_table").await;

    let batches = ctx
        .sql(r#"SELECT "_ROW_ID", id, name FROM data_evolution_table"#)
        .await
        .expect("SQL should parse")
        .collect()
        .await
        .expect("query should execute");

    assert!(!batches.is_empty());
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows > 0);

    for batch in &batches {
        let row_id_col = batch
            .column_by_name("_ROW_ID")
            .expect("_ROW_ID column should exist");
        let row_id_array = row_id_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("_ROW_ID should be Int64");
        for i in 0..batch.num_rows() {
            assert!(
                row_id_array.is_valid(i),
                "_ROW_ID should not be null for data evolution table"
            );
            assert!(row_id_array.value(i) >= 0);
        }
    }
}

#[tokio::test]
async fn test_filter_row_id_from_data_evolution_table() {
    use datafusion::arrow::array::Int64Array;

    let ctx = create_context("data_evolution_table").await;

    let all_batches = ctx
        .sql(r#"SELECT "_ROW_ID" FROM data_evolution_table"#)
        .await
        .expect("SQL")
        .collect()
        .await
        .expect("collect");
    let all_count: usize = all_batches.iter().map(|b| b.num_rows()).sum();

    let filtered_batches = ctx
        .sql(r#"SELECT "_ROW_ID", id FROM data_evolution_table WHERE "_ROW_ID" = 0"#)
        .await
        .expect("SQL")
        .collect()
        .await
        .expect("collect");
    let filtered_count: usize = filtered_batches.iter().map(|b| b.num_rows()).sum();

    assert!(filtered_count <= all_count);
    for batch in &filtered_batches {
        let row_id_array = batch
            .column_by_name("_ROW_ID")
            .expect("_ROW_ID")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64");
        for i in 0..batch.num_rows() {
            assert_eq!(row_id_array.value(i), 0);
        }
    }
}

// ======================= Full-Text Search Tests =======================

#[cfg(feature = "fulltext")]
mod fulltext_tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::prelude::SessionContext;
    use paimon::{Catalog, CatalogOptions, FileSystemCatalog, Options};
    use paimon_datafusion::{register_full_text_search, PaimonCatalogProvider};

    /// Extract the bundled tar.gz into a temp dir and return (tempdir, warehouse_path).
    fn extract_test_warehouse() -> (tempfile::TempDir, String) {
        let archive_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("testdata/test_tantivy_fulltext.tar.gz");
        let file = std::fs::File::open(&archive_path)
            .unwrap_or_else(|e| panic!("Failed to open {}: {e}", archive_path.display()));
        let decoder = flate2::read::GzDecoder::new(file);
        let mut archive = tar::Archive::new(decoder);

        let tmp = tempfile::tempdir().expect("Failed to create temp dir");
        let db_dir = tmp.path().join("default.db");
        std::fs::create_dir_all(&db_dir).unwrap();
        archive.unpack(&db_dir).unwrap();

        let warehouse = format!("file://{}", tmp.path().display());
        (tmp, warehouse)
    }

    async fn create_fulltext_context() -> (SessionContext, tempfile::TempDir) {
        let (tmp, warehouse) = extract_test_warehouse();
        let mut options = Options::new();
        options.set(CatalogOptions::WAREHOUSE, warehouse);
        let catalog = FileSystemCatalog::new(options).expect("Failed to create catalog");
        let catalog: Arc<dyn Catalog> = Arc::new(catalog);

        let ctx = SessionContext::new();
        ctx.register_catalog(
            "paimon",
            Arc::new(PaimonCatalogProvider::new(Arc::clone(&catalog))),
        );
        register_full_text_search(&ctx, catalog, "default");
        (ctx, tmp)
    }

    fn extract_id_content_rows(
        batches: &[datafusion::arrow::record_batch::RecordBatch],
    ) -> Vec<(i32, String)> {
        let mut rows = Vec::new();
        for batch in batches {
            let id_array = batch
                .column_by_name("id")
                .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
                .expect("Expected Int32Array for id");
            let content_array = batch
                .column_by_name("content")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .expect("Expected StringArray for content");
            for i in 0..batch.num_rows() {
                rows.push((id_array.value(i), content_array.value(i).to_string()));
            }
        }
        rows.sort_by_key(|(id, _)| *id);
        rows
    }

    /// Search for 'paimon' — rows 0, 2, 4 mention "paimon".
    #[tokio::test]
    async fn test_full_text_search_paimon() {
        let (ctx, _tmp) = create_fulltext_context().await;
        let batches = ctx
            .sql("SELECT id, content FROM full_text_search('paimon.default.test_tantivy_fulltext', 'content', 'paimon', 10)")
            .await
            .expect("SQL should parse")
            .collect()
            .await
            .expect("query should execute");

        let rows = extract_id_content_rows(&batches);
        let ids: Vec<i32> = rows.iter().map(|(id, _)| *id).collect();
        assert_eq!(
            ids,
            vec![0, 2, 4],
            "Searching 'paimon' should match rows 0, 2, 4"
        );
    }

    /// Search for 'tantivy' — only row 1.
    #[tokio::test]
    async fn test_full_text_search_tantivy() {
        let (ctx, _tmp) = create_fulltext_context().await;
        let batches = ctx
            .sql("SELECT id, content FROM full_text_search('paimon.default.test_tantivy_fulltext', 'content', 'tantivy', 10)")
            .await
            .expect("SQL should parse")
            .collect()
            .await
            .expect("query should execute");

        let rows = extract_id_content_rows(&batches);
        let ids: Vec<i32> = rows.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, vec![1], "Searching 'tantivy' should match row 1");
    }

    /// Search for 'search' — rows 1, 3 mention "full-text search".
    #[tokio::test]
    async fn test_full_text_search_search() {
        let (ctx, _tmp) = create_fulltext_context().await;
        let batches = ctx
            .sql("SELECT id, content FROM full_text_search('paimon.default.test_tantivy_fulltext', 'content', 'search', 10)")
            .await
            .expect("SQL should parse")
            .collect()
            .await
            .expect("query should execute");

        let rows = extract_id_content_rows(&batches);
        let ids: Vec<i32> = rows.iter().map(|(id, _)| *id).collect();
        assert!(ids.contains(&1), "Searching 'search' should match row 1");
        assert!(ids.contains(&3), "Searching 'search' should match row 3");
    }
}
