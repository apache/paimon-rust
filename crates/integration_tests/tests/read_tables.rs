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
use paimon::api::ConfigResponse;
use paimon::catalog::{Identifier, RESTCatalog};
use paimon::common::Options;
use paimon::spec::{DataType, IntType, Predicate, Schema, VarCharType};
use paimon::{Catalog, Error, FileSystemCatalog, Plan};
use std::collections::{HashMap, HashSet};

#[path = "../../paimon/tests/mock_server.rs"]
mod mock_server;
use mock_server::start_mock_server;

fn get_test_warehouse() -> String {
    std::env::var("PAIMON_TEST_WAREHOUSE").unwrap_or_else(|_| "/tmp/paimon-warehouse".to_string())
}

async fn scan_and_read<C: Catalog + ?Sized>(
    catalog: &C,
    table_name: &str,
    projection: Option<&[&str]>,
) -> (Plan, Vec<RecordBatch>) {
    let table = get_table_from_catalog(catalog, table_name).await;

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

async fn get_table_from_catalog<C: Catalog + ?Sized>(
    catalog: &C,
    table_name: &str,
) -> paimon::Table {
    let identifier = Identifier::new("default", table_name);
    catalog
        .get_table(&identifier)
        .await
        .expect("Failed to get table")
}

fn create_file_system_catalog() -> FileSystemCatalog {
    let warehouse = get_test_warehouse();
    FileSystemCatalog::new(warehouse).expect("Failed to create FileSystemCatalog")
}

async fn scan_and_read_with_fs_catalog(
    table_name: &str,
    projection: Option<&[&str]>,
) -> (Plan, Vec<RecordBatch>) {
    let catalog = create_file_system_catalog();
    scan_and_read(&catalog, table_name, projection).await
}

async fn scan_and_read_with_filter(
    table: &paimon::Table,
    filter: Predicate,
) -> (Plan, Vec<RecordBatch>) {
    let mut read_builder = table.new_read_builder();
    read_builder.with_filter(filter);
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

fn extract_id_name_dt(batches: &[RecordBatch]) -> Vec<(i32, String, String)> {
    let mut rows = Vec::new();
    for batch in batches {
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
    rows
}

fn extract_plan_partitions(plan: &Plan) -> HashSet<String> {
    plan.splits()
        .iter()
        .map(|split| {
            split
                .partition()
                .get_string(0)
                .expect("Failed to decode dt")
                .to_string()
        })
        .collect()
}

fn extract_plan_multi_partitions(plan: &Plan) -> HashSet<(String, i32)> {
    plan.splits()
        .iter()
        .map(|split| {
            let partition = split.partition();
            (
                partition.get_string(0).expect("dt").to_string(),
                partition.get_int(1).expect("hr"),
            )
        })
        .collect()
}

#[tokio::test]
async fn test_read_log_table() {
    let (plan, batches) = scan_and_read_with_fs_catalog("simple_log_table", None).await;

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
    let (_, batches) = scan_and_read_with_fs_catalog("simple_dv_pk_table", None).await;
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
    let (plan, batches) = scan_and_read_with_fs_catalog("partitioned_log_table", None).await;

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
    let (plan, batches) = scan_and_read_with_fs_catalog("multi_partitioned_log_table", None).await;

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
    let (plan, batches) = scan_and_read_with_fs_catalog("partitioned_dv_pk_table", None).await;

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

#[tokio::test]
async fn test_read_with_column_projection() {
    let (_, batches) =
        scan_and_read_with_fs_catalog("partitioned_log_table", Some(&["name", "id"])).await;

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
    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "simple_log_table").await;

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
    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "simple_log_table").await;

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
    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "simple_log_table").await;

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
    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "simple_log_table").await;

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

// ---------------------------------------------------------------------------
// Partition filter integration tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_read_partitioned_table_with_filter() {
    use paimon::spec::{Datum, PredicateBuilder};

    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "partitioned_log_table").await;
    // Build a filter: dt = '2024-01-01'
    let schema = table.schema();
    let pb = PredicateBuilder::new(schema.fields());
    let filter = pb
        .equal("dt", Datum::String("2024-01-01".into()))
        .expect("Failed to build predicate");

    let (plan, batches) = scan_and_read_with_filter(&table, filter).await;
    let seen_partitions = extract_plan_partitions(&plan);
    assert_eq!(
        seen_partitions,
        HashSet::from(["2024-01-01".into()]),
        "Only the filtered partition should be in the plan"
    );

    let rows = extract_id_name_dt(&batches);
    assert_eq!(
        rows,
        vec![
            (1, "alice".into(), "2024-01-01".into()),
            (2, "bob".into(), "2024-01-01".into()),
        ]
    );
}

#[tokio::test]
async fn test_read_multi_partitioned_table_with_filter() {
    use paimon::spec::{Datum, Predicate, PredicateBuilder};

    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "multi_partitioned_log_table").await;
    let schema = table.schema();
    let pb = PredicateBuilder::new(schema.fields());

    // Filter: dt = '2024-01-01' AND hr = 10
    let filter = Predicate::and(vec![
        pb.equal("dt", Datum::String("2024-01-01".into())).unwrap(),
        pb.equal("hr", Datum::Int(10)).unwrap(),
    ]);

    let (plan, batches) = scan_and_read_with_filter(&table, filter).await;
    let partitions = extract_plan_multi_partitions(&plan);
    assert_eq!(
        partitions,
        HashSet::from([("2024-01-01".into(), 10)]),
        "Only dt=2024-01-01, hr=10 should survive"
    );

    let actual = extract_id_name(&batches);
    assert_eq!(
        actual,
        vec![(1, "alice".to_string()), (2, "bob".to_string()),],
        "Only rows from dt=2024-01-01, hr=10 should be returned"
    );
}

#[tokio::test]
async fn test_read_partitioned_table_data_only_filter_preserves_all_partitions() {
    use paimon::spec::{Datum, PredicateBuilder};

    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "partitioned_log_table").await;
    let schema = table.schema();
    let pb = PredicateBuilder::new(schema.fields());

    // Data-only filter: id > 10 — should NOT prune any partitions,
    // and is still ignored at read level in Phase 2.
    let filter = pb
        .greater_than("id", Datum::Int(10))
        .expect("Failed to build predicate");

    let (plan, batches) = scan_and_read_with_filter(&table, filter).await;
    let seen_partitions = extract_plan_partitions(&plan);
    assert_eq!(
        seen_partitions,
        HashSet::from(["2024-01-01".into(), "2024-01-02".into()]),
        "Data-only filter should not prune any partitions"
    );

    let actual = extract_id_name(&batches);
    assert_eq!(
        actual,
        vec![
            (1, "alice".to_string()),
            (2, "bob".to_string()),
            (3, "carol".to_string()),
        ],
        "Data predicate is not applied at read level; all rows are still returned"
    );
}

/// Mixed AND: partition predicate prunes partitions, but data predicate is
/// silently ignored — all rows from the matching partition are returned.
#[tokio::test]
async fn test_read_partitioned_table_mixed_and_filter() {
    use paimon::spec::{Datum, Predicate, PredicateBuilder};

    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "partitioned_log_table").await;
    let schema = table.schema();
    let pb = PredicateBuilder::new(schema.fields());

    // dt = '2024-01-01' AND id > 10
    // Partition conjunct (dt) is applied; data conjunct (id) is NOT.
    let filter = Predicate::and(vec![
        pb.equal("dt", Datum::String("2024-01-01".into())).unwrap(),
        pb.greater_than("id", Datum::Int(10)).unwrap(),
    ]);

    let (plan, batches) = scan_and_read_with_filter(&table, filter).await;
    let seen_partitions = extract_plan_partitions(&plan);
    assert_eq!(
        seen_partitions,
        HashSet::from(["2024-01-01".into()]),
        "Only dt=2024-01-01 should survive"
    );

    let actual = extract_id_name(&batches);
    assert_eq!(
        actual,
        vec![(1, "alice".to_string()), (2, "bob".to_string())],
        "Data predicate (id > 10) is NOT applied — all rows from matching partition returned"
    );
}

/// Mixed OR: `dt = '...' OR id > 10` cannot be split into a pure partition
/// predicate, so no partitions should be pruned.
#[tokio::test]
async fn test_read_partitioned_table_mixed_or_filter_preserves_all() {
    use paimon::spec::{Datum, Predicate, PredicateBuilder};

    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "partitioned_log_table").await;
    let schema = table.schema();
    let pb = PredicateBuilder::new(schema.fields());

    // dt = '2024-01-01' OR id > 10 — mixed OR is not safely splittable.
    let filter = Predicate::or(vec![
        pb.equal("dt", Datum::String("2024-01-01".into())).unwrap(),
        pb.greater_than("id", Datum::Int(10)).unwrap(),
    ]);

    let (plan, batches) = scan_and_read_with_filter(&table, filter).await;
    let seen_partitions = extract_plan_partitions(&plan);
    assert_eq!(
        seen_partitions,
        HashSet::from(["2024-01-01".into(), "2024-01-02".into()]),
        "Mixed OR should not prune any partitions"
    );

    let actual = extract_id_name(&batches);
    assert_eq!(
        actual,
        vec![
            (1, "alice".to_string()),
            (2, "bob".to_string()),
            (3, "carol".to_string()),
        ],
        "All rows should be returned when pruning is not possible"
    );
}

/// Filter that matches no existing partition — all entries pruned, 0 splits.
#[tokio::test]
async fn test_read_partitioned_table_filter_matches_no_partition() {
    use paimon::spec::{Datum, PredicateBuilder};

    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "partitioned_log_table").await;
    let schema = table.schema();
    let pb = PredicateBuilder::new(schema.fields());

    // dt = '9999-12-31' matches no partition.
    let filter = pb
        .equal("dt", Datum::String("9999-12-31".into()))
        .expect("Failed to build predicate");

    let mut read_builder = table.new_read_builder();
    read_builder.with_filter(filter);
    let scan = read_builder.new_scan();
    let plan = scan.plan().await.expect("Failed to plan scan");

    assert!(
        plan.splits().is_empty(),
        "No splits should survive when filter matches no partition"
    );
}

#[tokio::test]
async fn test_read_partitioned_table_eval_row_error_fails_plan() {
    use paimon::spec::{ArrayType, DataType, Datum, IntType, PredicateOperator};

    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "partitioned_log_table").await;
    let dt_index = table
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == "dt")
        .expect("dt partition column should exist");

    // Use an unsupported DataType in a partition leaf so remapping succeeds
    // but `eval_row` fails during partition pruning.
    let filter = Predicate::Leaf {
        column: "dt".into(),
        index: dt_index,
        data_type: DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
        op: PredicateOperator::Eq,
        literals: vec![Datum::Int(42)],
    };

    let mut read_builder = table.new_read_builder();
    read_builder.with_filter(filter);

    let err = read_builder
        .new_scan()
        .plan()
        .await
        .expect_err("eval_row error should fail-fast during planning");

    assert!(
        matches!(&err, Error::Unsupported { message } if message.contains("extract_datum")),
        "Expected extract_datum unsupported error, got: {err:?}"
    );
}

// ======================= REST Catalog read tests ===============================

/// Build a simple test schema matching the Spark-provisioned tables (id INT, name VARCHAR).
fn simple_log_schema() -> Schema {
    Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("name", DataType::VarChar(VarCharType::string_type()))
        .build()
        .expect("Failed to build schema")
}

/// Start a mock REST server backed by Spark-provisioned data on disk,
/// register the given tables, and return a connected `RESTCatalog`.
async fn setup_rest_catalog_with_tables(
    table_configs: &[(&str, &str, Schema)],
) -> (mock_server::RESTServer, RESTCatalog) {
    let catalog_path = get_test_warehouse();
    // Use a simple warehouse name (no slashes) to avoid URL-encoding issues
    let warehouse_name = "test_warehouse";
    let prefix = "mock-test";
    let mut defaults = HashMap::new();
    defaults.insert("prefix".to_string(), prefix.to_string());
    let config = ConfigResponse::new(defaults);

    let server = start_mock_server(
        warehouse_name.to_string(),
        catalog_path.clone(),
        config,
        vec!["default".to_string()],
    )
    .await;

    // Register each table with its schema and the real on-disk path
    for (database, table_name, schema) in table_configs {
        let table_path = format!("{}/{}.db/{}", catalog_path, database, table_name);
        server.add_table_with_schema(database, table_name, schema.clone(), &table_path);
    }

    let url = server.url().expect("Failed to get server URL");
    let mut options = Options::new();
    options.set("uri", &url);
    options.set("warehouse", warehouse_name);
    options.set("token.provider", "bear");
    options.set("token", "test_token");

    let catalog = RESTCatalog::new(options, true)
        .await
        .expect("Failed to create RESTCatalog");

    (server, catalog)
}

/// Test reading an append-only (log) table via REST catalog backed by mock server.
///
/// The mock server returns table metadata pointing to Spark-provisioned data on disk.
#[tokio::test]
async fn test_rest_catalog_read_append_table() {
    let table_name = "simple_log_table";
    let (_server, catalog) =
        setup_rest_catalog_with_tables(&[("default", table_name, simple_log_schema())]).await;

    let (plan, batches) = scan_and_read(&catalog, table_name, None).await;

    assert!(
        !plan.splits().is_empty(),
        "REST append table should have at least one split"
    );

    assert!(
        !batches.is_empty(),
        "REST append table should produce at least one batch"
    );

    let actual = extract_id_name(&batches);
    let expected = vec![
        (1, "alice".to_string()),
        (2, "bob".to_string()),
        (3, "carol".to_string()),
    ];
    assert_eq!(
        actual, expected,
        "REST catalog append table rows should match expected values"
    );
}
