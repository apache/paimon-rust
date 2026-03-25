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
async fn test_subset_projection_returns_not_implemented() {
    let error = collect_query("simple_log_table", "SELECT id FROM simple_log_table")
        .await
        .expect_err("Subset projection should be rejected until projection support lands");

    assert!(
        error
            .to_string()
            .contains("does not yet support subset or reordered projections"),
        "Expected explicit unsupported projection error, got: {error}"
    );
}
