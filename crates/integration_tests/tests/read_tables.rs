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
// software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Integration tests for reading Paimon tables provisioned by Spark.

use arrow_array::{Int32Array, StringArray};
use futures::TryStreamExt;
use paimon::catalog::Identifier;
use paimon::{Catalog, FileSystemCatalog};

/// Get the test warehouse path from environment variable or use default.
fn get_test_warehouse() -> String {
    std::env::var("PAIMON_TEST_WAREHOUSE").unwrap_or_else(|_| "/tmp/paimon-warehouse".to_string())
}

async fn read_rows(table_name: &str) -> Vec<(i32, String)> {
    let warehouse = get_test_warehouse();
    let catalog = FileSystemCatalog::new(warehouse).expect("Failed to create catalog");

    let identifier = Identifier::new("default", table_name);
    let table = catalog
        .get_table(&identifier)
        .await
        .expect("Failed to get table");

    let read_builder = table.new_read_builder();
    let read = read_builder.new_read().expect("Failed to create read");
    let scan = read_builder.new_scan();
    let plan = scan.plan().await.expect("Failed to plan scan");

    let stream = read
        .to_arrow(plan.splits())
        .expect("Failed to create arrow stream");

    let batches: Vec<_> = stream
        .try_collect()
        .await
        .expect("Failed to collect batches");

    assert!(
        !batches.is_empty(),
        "Expected at least one batch from table {table_name}"
    );

    let mut actual_rows: Vec<(i32, String)> = Vec::new();

    for batch in &batches {
        let id_array = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("Expected Int32Array for id column");
        let name_array = batch
            .column_by_name("name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("Expected StringArray for name column");

        for i in 0..batch.num_rows() {
            actual_rows.push((id_array.value(i), name_array.value(i).to_string()));
        }
    }

    actual_rows.sort_by_key(|(id, _)| *id);
    actual_rows
}

#[tokio::test]
async fn test_read_log_table() {
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
async fn test_read_dv_primary_key_table() {
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
        "DV-enabled PK table should only expose the latest row per key"
    );
}
