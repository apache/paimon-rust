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

//! Integration tests for reading Paimon tables provisioned by Spark.

use arrow_array::{Int32Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use paimon::catalog::Identifier;
use paimon::{Catalog, Error, FileSystemCatalog, Plan};
use std::collections::HashSet;

fn get_test_warehouse() -> String {
    std::env::var("PAIMON_TEST_WAREHOUSE").unwrap_or_else(|_| "/tmp/paimon-warehouse".to_string())
}

async fn scan_and_read(table_name: &str) -> (Plan, Vec<RecordBatch>) {
    scan_and_read_with_projection(table_name, None).await
}

async fn scan_and_read_with_projection(
    table_name: &str,
    projection: Option<&[&str]>,
) -> (Plan, Vec<RecordBatch>) {
    let table = get_test_table(table_name).await;

    let mut read_builder = table.new_read_builder();
    if let Some(cols) = projection {
        read_builder.with_projection(cols);
    }
    let scan = read_builder.new_scan();
    let plan = scan.plan().await.expect("Failed to plan scan");

    let read = read_builder.new_read().expect("Failed to create read");
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
    (plan, batches)
}

fn extract_id_name(batches: &[RecordBatch]) -> Vec<(i32, String)> {
    let mut rows = Vec::new();
    for batch in batches {
        let id = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("Expected Int32Array for id");
        let name = batch
            .column_by_name("name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("Expected StringArray for name");
        for i in 0..batch.num_rows() {
            rows.push((id.value(i), name.value(i).to_string()));
        }
    }
    rows.sort_by_key(|(id, _)| *id);
    rows
}

#[tokio::test]
async fn test_read_log_table() {
    let (plan, batches) = scan_and_read("simple_log_table").await;

    // Non-partitioned table: partition should be a valid arity=0 BinaryRow
    // deserialized from manifest bytes, not a stub without backing data.
    for split in plan.splits() {
        let partition = split.partition();
        assert_eq!(partition.arity(), 0);
        assert!(
            !partition.is_empty(),
            "Non-partitioned split should have backing data from manifest deserialization"
        );
    }

    let actual = extract_id_name(&batches);
    let expected = vec![
        (1, "alice".to_string()),
        (2, "bob".to_string()),
        (3, "carol".to_string()),
    ];
    assert_eq!(actual, expected, "Rows should match expected values");
}

#[tokio::test]
async fn test_read_dv_primary_key_table() {
    let (_, batches) = scan_and_read("simple_dv_pk_table").await;
    let actual = extract_id_name(&batches);
    let expected = vec![
        (1, "alice-v2".to_string()),
        (2, "bob-v2".to_string()),
        (3, "carol-v2".to_string()),
        (4, "dave-v2".to_string()),
        (5, "eve-v2".to_string()),
        (6, "frank-v1".to_string()),
    ];
    assert_eq!(
        actual, expected,
        "DV-enabled PK table should only expose the latest row per key"
    );
}

#[tokio::test]
async fn test_read_partitioned_log_table() {
    let (plan, batches) = scan_and_read("partitioned_log_table").await;

    let mut seen_partitions: HashSet<String> = HashSet::new();
    for split in plan.splits() {
        let partition = split.partition();
        assert_eq!(partition.arity(), 1);
        assert!(!partition.is_empty());
        let dt = partition.get_string(0).expect("Failed to decode dt");
        let expected_suffix = format!("dt={dt}/bucket-{}", split.bucket());
        assert!(
            split.bucket_path().ends_with(&expected_suffix),
            "bucket_path should end with '{expected_suffix}', got: {}",
            split.bucket_path()
        );
        seen_partitions.insert(dt.to_string());
    }
    assert_eq!(
        seen_partitions,
        HashSet::from(["2024-01-01".into(), "2024-01-02".into()])
    );

    let mut rows: Vec<(i32, String, String)> = Vec::new();
    for batch in &batches {
        let id = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("id");
        let name = batch
            .column_by_name("name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("name");
        let dt = batch
            .column_by_name("dt")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("dt");
        for i in 0..batch.num_rows() {
            rows.push((id.value(i), name.value(i).into(), dt.value(i).into()));
        }
    }
    rows.sort_by_key(|(id, _, _)| *id);

    assert_eq!(
        rows,
        vec![
            (1, "alice".into(), "2024-01-01".into()),
            (2, "bob".into(), "2024-01-01".into()),
            (3, "carol".into(), "2024-01-02".into()),
        ]
    );
}

#[tokio::test]
async fn test_read_multi_partitioned_log_table() {
    let (plan, batches) = scan_and_read("multi_partitioned_log_table").await;

    let mut seen_partitions: HashSet<(String, i32)> = HashSet::new();
    for split in plan.splits() {
        let partition = split.partition();
        assert_eq!(partition.arity(), 2);
        assert!(!partition.is_empty());
        let dt = partition.get_string(0).expect("Failed to decode dt");
        let hr = partition.get_int(1).expect("Failed to decode hr");
        let expected_suffix = format!("dt={dt}/hr={hr}/bucket-{}", split.bucket());
        assert!(
            split.bucket_path().ends_with(&expected_suffix),
            "bucket_path should end with '{expected_suffix}', got: {}",
            split.bucket_path()
        );
        seen_partitions.insert((dt.to_string(), hr));
    }
    assert_eq!(
        seen_partitions,
        HashSet::from([
            ("2024-01-01".into(), 10),
            ("2024-01-01".into(), 20),
            ("2024-01-02".into(), 10),
        ])
    );

    let mut rows: Vec<(i32, String, String, i32)> = Vec::new();
    for batch in &batches {
        let id = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("id");
        let name = batch
            .column_by_name("name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("name");
        let dt = batch
            .column_by_name("dt")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("dt");
        let hr = batch
            .column_by_name("hr")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("hr");
        for i in 0..batch.num_rows() {
            rows.push((
                id.value(i),
                name.value(i).into(),
                dt.value(i).into(),
                hr.value(i),
            ));
        }
    }
    rows.sort_by_key(|(id, _, _, _)| *id);

    assert_eq!(
        rows,
        vec![
            (1, "alice".into(), "2024-01-01".into(), 10),
            (2, "bob".into(), "2024-01-01".into(), 10),
            (3, "carol".into(), "2024-01-01".into(), 20),
            (4, "dave".into(), "2024-01-02".into(), 10),
        ]
    );
}

#[tokio::test]
async fn test_read_partitioned_dv_pk_table() {
    let (plan, batches) = scan_and_read("partitioned_dv_pk_table").await;

    // Verify partition metadata on each split.
    let mut seen_partitions: HashSet<String> = HashSet::new();
    for split in plan.splits() {
        let partition = split.partition();
        assert_eq!(partition.arity(), 1);
        assert!(!partition.is_empty());
        let dt = partition.get_string(0).expect("Failed to decode dt");
        let expected_suffix = format!("dt={dt}/bucket-{}", split.bucket());
        assert!(
            split.bucket_path().ends_with(&expected_suffix),
            "bucket_path should end with '{expected_suffix}', got: {}",
            split.bucket_path()
        );
        seen_partitions.insert(dt.to_string());
    }
    assert_eq!(
        seen_partitions,
        HashSet::from(["2024-01-01".into(), "2024-01-02".into()])
    );

    let mut rows: Vec<(i32, String, String)> = Vec::new();
    for batch in &batches {
        let id = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("id");
        let name = batch
            .column_by_name("name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("name");
        let dt = batch
            .column_by_name("dt")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("dt");
        for i in 0..batch.num_rows() {
            rows.push((id.value(i), name.value(i).into(), dt.value(i).into()));
        }
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.2.cmp(&b.2)));

    assert_eq!(
        rows,
        vec![
            (1, "alice-v2".into(), "2024-01-01".into()),
            (1, "alice-v1".into(), "2024-01-02".into()),
            (2, "bob-v2".into(), "2024-01-01".into()),
            (3, "carol-v2".into(), "2024-01-02".into()),
            (4, "dave-v2".into(), "2024-01-02".into()),
        ]
    );
}

async fn get_test_table(table_name: &str) -> paimon::Table {
    let warehouse = get_test_warehouse();
    let catalog = FileSystemCatalog::new(warehouse).expect("Failed to create catalog");
    let identifier = Identifier::new("default", table_name);
    catalog
        .get_table(&identifier)
        .await
        .expect("Failed to get table")
}

#[tokio::test]
async fn test_read_with_column_projection() {
    let (_, batches) =
        scan_and_read_with_projection("partitioned_log_table", Some(&["name", "id"])).await;

    // Verify that output schema preserves caller-specified column order.
    for batch in &batches {
        let schema = batch.schema();
        let batch_field_names: Vec<&str> =
            schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            batch_field_names,
            vec!["name", "id"],
            "RecordBatch schema should preserve caller-specified order"
        );
        assert!(
            batch.column_by_name("dt").is_none(),
            "Non-projected column 'dt' should be absent"
        );
    }

    let actual = extract_id_name(&batches);
    let expected = vec![
        (1, "alice".to_string()),
        (2, "bob".to_string()),
        (3, "carol".to_string()),
    ];
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn test_read_projection_empty() {
    let table = get_test_table("simple_log_table").await;

    let mut read_builder = table.new_read_builder();
    read_builder.with_projection(&[]);
    let read = read_builder
        .new_read()
        .expect("Empty projection should succeed");

    assert_eq!(
        read.read_type().len(),
        0,
        "Empty projection should produce empty read_type"
    );

    let plan = table
        .new_read_builder()
        .new_scan()
        .plan()
        .await
        .expect("Failed to plan scan");

    let stream = read
        .to_arrow(plan.splits())
        .expect("Failed to create arrow stream");
    let batches: Vec<RecordBatch> = stream
        .try_collect()
        .await
        .expect("Failed to collect batches");
    assert!(!batches.is_empty());

    for batch in &batches {
        assert_eq!(
            batch.num_columns(),
            0,
            "Empty projection should produce 0-column batches"
        );
    }
}

#[tokio::test]
async fn test_read_projection_unknown_column() {
    let table = get_test_table("simple_log_table").await;

    let mut read_builder = table.new_read_builder();
    read_builder.with_projection(&["id", "nonexistent_column"]);
    let err = read_builder
        .new_read()
        .expect_err("Unknown columns should fail");

    assert!(
        matches!(
            &err,
            Error::ColumnNotExist {
                full_name,
                column,
            } if full_name == "default.simple_log_table" && column == "nonexistent_column"
        ),
        "Expected ColumnNotExist for nonexistent_column, got: {err:?}"
    );
}

#[tokio::test]
async fn test_read_projection_all_invalid() {
    let table = get_test_table("simple_log_table").await;

    let mut read_builder = table.new_read_builder();
    read_builder.with_projection(&["nonexistent_a", "nonexistent_b"]);
    let err = read_builder
        .new_read()
        .expect_err("All-invalid projection should fail");

    assert!(
        matches!(
            &err,
            Error::ColumnNotExist {
                full_name,
                column,
            } if full_name == "default.simple_log_table" && column == "nonexistent_a"
        ),
        "Expected ColumnNotExist for nonexistent_a, got: {err:?}"
    );
}

#[tokio::test]
async fn test_read_projection_duplicate_column() {
    let table = get_test_table("simple_log_table").await;

    let mut read_builder = table.new_read_builder();
    read_builder.with_projection(&["id", "id"]);
    let err = read_builder
        .new_read()
        .expect_err("Duplicate projection should fail");

    assert!(
        matches!(&err, Error::ConfigInvalid { message } if message.contains("Duplicate projection column 'id'")),
        "Expected ConfigInvalid for duplicate projection, got: {err:?}"
    );
}
