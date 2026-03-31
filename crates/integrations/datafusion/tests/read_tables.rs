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

use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::prelude::SessionContext;
use paimon::catalog::Identifier;
use paimon::{Catalog, FileSystemCatalog};
use paimon_datafusion::PaimonTableProvider;

fn get_test_warehouse() -> String {
    std::env::var("PAIMON_TEST_WAREHOUSE").unwrap_or_else(|_| "/tmp/paimon-warehouse".to_string())
}

async fn create_context(table_name: &str) -> SessionContext {
    let warehouse = get_test_warehouse();
    let catalog = FileSystemCatalog::new(warehouse).expect("Failed to create catalog");
    let identifier = Identifier::new("default", table_name);
    let table = catalog
        .get_table(&identifier)
        .await
        .expect("Failed to get table");

    let provider = PaimonTableProvider::try_new(table).expect("Failed to create table provider");
    let ctx = SessionContext::new();
    ctx.register_table(table_name, Arc::new(provider))
        .expect("Failed to register table");

    ctx
}

async fn read_rows(table_name: &str) -> Vec<(i32, String)> {
    let batches = collect_query(table_name, &format!("SELECT id, name FROM {table_name}"))
        .await
        .expect("Failed to collect query result");

    assert!(
        !batches.is_empty(),
        "Expected at least one batch from table {table_name}"
    );

    let mut actual_rows = Vec::new();
    for batch in &batches {
        let id_array = batch
            .column_by_name("id")
            .and_then(|column| column.as_any().downcast_ref::<Int32Array>())
            .expect("Expected Int32Array for id column");
        let name_array = batch
            .column_by_name("name")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .expect("Expected StringArray for name column");

        for row_index in 0..batch.num_rows() {
            actual_rows.push((
                id_array.value(row_index),
                name_array.value(row_index).to_string(),
            ));
        }
    }

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

/// Verifies that `PaimonTableProvider::scan()` produces more than one
/// execution partition for a multi-partition table, and that the reported
/// partition count is still capped by `target_partitions`.
#[tokio::test]
async fn test_scan_partition_count_respects_session_config() {
    use datafusion::datasource::TableProvider;
    use datafusion::prelude::SessionConfig;

    let warehouse = get_test_warehouse();
    let catalog = FileSystemCatalog::new(warehouse).expect("Failed to create catalog");
    let identifier = Identifier::new("default", "partitioned_log_table");
    let table = catalog
        .get_table(&identifier)
        .await
        .expect("Failed to get table");

    let provider = PaimonTableProvider::try_new(table).expect("Failed to create table provider");

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
