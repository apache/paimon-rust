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

use arrow_array::{
    Array, ArrowPrimitiveType, Int32Array, Int64Array, ListArray, MapArray, RecordBatch,
    StringArray, StructArray,
};
use futures::TryStreamExt;
use paimon::api::ConfigResponse;
use paimon::catalog::{Identifier, RESTCatalog};
use paimon::common::Options;
use paimon::spec::{DataType, IntType, Predicate, Schema, VarCharType};
use paimon::{Catalog, CatalogOptions, Error, FileSystemCatalog, Plan};
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
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse);
    FileSystemCatalog::new(options).expect("Failed to create FileSystemCatalog")
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
async fn test_read_partitioned_table_data_only_filter_prunes_all_files() {
    use paimon::spec::{Datum, PredicateBuilder};

    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "partitioned_log_table").await;
    let schema = table.schema();
    let pb = PredicateBuilder::new(schema.fields());

    let filter = pb
        .greater_than("id", Datum::Int(10))
        .expect("Failed to build predicate");

    let (plan, batches) = scan_and_read_with_filter(&table, filter).await;
    let seen_partitions = extract_plan_partitions(&plan);
    assert_eq!(
        seen_partitions,
        HashSet::<String>::new(),
        "Data-only filter should prune all files when stats prove no match"
    );

    let actual = extract_id_name(&batches);
    assert_eq!(
        actual,
        Vec::<(i32, String)>::new(),
        "No rows should be planned when stats prove the predicate is unsatisfiable"
    );
}

#[tokio::test]
async fn test_read_partitioned_table_mixed_and_filter() {
    use paimon::spec::{Datum, Predicate, PredicateBuilder};

    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "partitioned_log_table").await;
    let schema = table.schema();
    let pb = PredicateBuilder::new(schema.fields());

    let filter = Predicate::and(vec![
        pb.equal("dt", Datum::String("2024-01-01".into())).unwrap(),
        pb.greater_than("id", Datum::Int(10)).unwrap(),
    ]);

    let (plan, batches) = scan_and_read_with_filter(&table, filter).await;
    let seen_partitions = extract_plan_partitions(&plan);
    assert_eq!(
        seen_partitions,
        HashSet::<String>::new(),
        "The matching partition should also be pruned when file stats prove no match"
    );

    let actual = extract_id_name(&batches);
    assert_eq!(
        actual,
        Vec::<(i32, String)>::new(),
        "No rows should remain after partition pruning and data stats pruning"
    );
}

#[tokio::test]
async fn test_read_partitioned_table_data_only_filter_keeps_matching_partition() {
    use paimon::spec::{Datum, PredicateBuilder};

    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "partitioned_log_table").await;
    let schema = table.schema();
    let pb = PredicateBuilder::new(schema.fields());

    let filter = pb
        .greater_than("id", Datum::Int(2))
        .expect("Failed to build predicate");

    let (plan, batches) = scan_and_read_with_filter(&table, filter).await;
    let seen_partitions = extract_plan_partitions(&plan);
    assert_eq!(
        seen_partitions,
        HashSet::from(["2024-01-02".into()]),
        "Only files whose stats may satisfy the predicate should remain in the plan"
    );

    let actual = extract_id_name(&batches);
    assert_eq!(
        actual,
        vec![(3, "carol".to_string())],
        "Only rows from files that survive stats pruning should be returned"
    );
}

/// Java-style inclusive projection can still extract partition predicates from
/// an OR of mixed AND branches.
#[tokio::test]
async fn test_read_multi_partitioned_table_or_of_mixed_ands_prunes_partitions() {
    use paimon::spec::{Datum, Predicate, PredicateBuilder};

    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "multi_partitioned_log_table").await;
    let schema = table.schema();
    let pb = PredicateBuilder::new(schema.fields());

    let filter = Predicate::or(vec![
        Predicate::and(vec![
            pb.equal("dt", Datum::String("2024-01-01".into())).unwrap(),
            pb.equal("hr", Datum::Int(10)).unwrap(),
            pb.greater_than("id", Datum::Int(10)).unwrap(),
        ]),
        Predicate::and(vec![
            pb.equal("dt", Datum::String("2024-01-01".into())).unwrap(),
            pb.equal("hr", Datum::Int(20)).unwrap(),
        ]),
    ]);

    let (plan, batches) = scan_and_read_with_filter(&table, filter).await;
    let seen_partitions = extract_plan_multi_partitions(&plan);
    assert_eq!(
        seen_partitions,
        HashSet::from([("2024-01-01".into(), 10), ("2024-01-01".into(), 20)]),
        "Inclusive projection should prune the dt=2024-01-02 partition"
    );

    let actual = extract_id_name(&batches);
    assert_eq!(
        actual,
        vec![
            (1, "alice".to_string()),
            (2, "bob".to_string()),
            (3, "carol".to_string()),
        ],
        "All rows from the surviving partitions should be returned"
    );
}

/// A directly mixed OR like `dt = '...' OR id > 10` is still not safely
/// splittable into a partition predicate, so no partitions should be pruned.
#[tokio::test]
async fn test_read_partitioned_table_mixed_or_filter_preserves_all() {
    use paimon::spec::{Datum, Predicate, PredicateBuilder};

    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "partitioned_log_table").await;
    let schema = table.schema();
    let pb = PredicateBuilder::new(schema.fields());

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

/// A filter that matches no partition should produce no splits.
#[tokio::test]
async fn test_read_partitioned_table_filter_matches_no_partition() {
    use paimon::spec::{Datum, PredicateBuilder};

    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "partitioned_log_table").await;
    let schema = table.schema();
    let pb = PredicateBuilder::new(schema.fields());

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

/// Using an unsupported DataType in a partition predicate should fail-open:
/// the plan succeeds and returns all partitions (no pruning).
#[tokio::test]
async fn test_read_partitioned_table_eval_row_error_fails_open() {
    use paimon::spec::{ArrayType, DataType, Datum, IntType, PredicateOperator};

    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "partitioned_log_table").await;
    let dt_index = table
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == "dt")
        .expect("dt partition column should exist");

    // Use an unsupported partition type so remapping succeeds but `eval_row` fails.
    // The entry-level filter catches the error and fails open (keeps the entry).
    let filter = Predicate::Leaf {
        column: "dt".into(),
        index: dt_index,
        data_type: DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
        op: PredicateOperator::Eq,
        literals: vec![Datum::Int(42)],
    };

    let mut read_builder = table.new_read_builder();
    read_builder.with_filter(filter);

    let plan = read_builder
        .new_scan()
        .plan()
        .await
        .expect("Plan should succeed (fail-open on unsupported type)");

    // All partitions should survive since the predicate evaluation fails open.
    let seen_partitions = extract_plan_partitions(&plan);
    assert_eq!(
        seen_partitions,
        HashSet::from(["2024-01-01".into(), "2024-01-02".into()]),
        "Unsupported predicate type should fail-open and keep all partitions"
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
        let table_path = format!("{catalog_path}/{database}.db/{table_name}");
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

// ---------------------------------------------------------------------------
// Data Evolution integration tests
// ---------------------------------------------------------------------------

/// Test reading a data-evolution enabled append-only table.
///
/// The table is provisioned by Spark with `data-evolution.enabled=true` and
/// `row-tracking.enabled=true`. Multiple inserts produce files with `first_row_id`
/// set, exercising the data evolution scan and read path.
#[tokio::test]
async fn test_read_data_evolution_table() {
    let (plan, batches) = scan_and_read_with_fs_catalog("data_evolution_table", None).await;

    assert!(
        !plan.splits().is_empty(),
        "Data evolution table should have at least one split"
    );

    let mut rows: Vec<(i32, String, i32)> = Vec::new();
    for batch in &batches {
        let id = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("id");
        let name = batch
            .column_by_name("name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("name");
        let value = batch
            .column_by_name("value")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("value");
        for i in 0..batch.num_rows() {
            rows.push((id.value(i), name.value(i).to_string(), value.value(i)));
        }
    }
    rows.sort_by_key(|(id, _, _)| *id);

    assert_eq!(
        rows,
        vec![
            (1, "alice-v2".into(), 100),
            (2, "bob".into(), 200),
            (3, "carol-v2".into(), 300),
            (4, "dave".into(), 400),
            (5, "eve".into(), 500),
        ],
        "Data evolution table should return merged rows after MERGE INTO"
    );
}

/// Test reading a data-evolution table with column projection.
#[tokio::test]
async fn test_read_data_evolution_table_with_projection() {
    let (_, batches) =
        scan_and_read_with_fs_catalog("data_evolution_table", Some(&["value", "id"])).await;

    for batch in &batches {
        let schema = batch.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            field_names,
            vec!["value", "id"],
            "Projection order should be preserved"
        );
        assert!(
            batch.column_by_name("name").is_none(),
            "Non-projected column 'name' should be absent"
        );
    }

    let mut rows: Vec<(i32, i32)> = Vec::new();
    for batch in &batches {
        let id = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("id");
        let value = batch
            .column_by_name("value")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("value");
        for i in 0..batch.num_rows() {
            rows.push((id.value(i), value.value(i)));
        }
    }
    rows.sort_by_key(|(id, _)| *id);

    assert_eq!(
        rows,
        vec![(1, 100), (2, 200), (3, 300), (4, 400), (5, 500)],
        "Projected data evolution read should return correct values"
    );
}

// ---------------------------------------------------------------------------
// Limit pushdown integration tests
// ---------------------------------------------------------------------------

/// Helper function to scan and read with limit pushdown.
async fn plan_table(table: &paimon::Table, limit: Option<usize>) -> Plan {
    let mut read_builder = table.new_read_builder();
    if let Some(limit) = limit {
        read_builder.with_limit(limit);
    }
    let scan = read_builder.new_scan();
    scan.plan().await.expect("Failed to plan scan")
}

/// Test limit pushdown: when limit is smaller than total rows, fewer data files may be generated.
#[tokio::test]
async fn test_limit_pushdown() {
    let catalog = create_file_system_catalog();

    // Test limit pushdown for data evolution table
    let table = get_table_from_catalog(&catalog, "data_evolution_table").await;

    // Get full plan without limit
    let full_plan = plan_table(&table, None).await;
    let full_data_split_count: usize = full_plan.splits().iter().count();

    // Get the plan with limit = 2
    let limited_plan = plan_table(&table, Some(2)).await;
    let limited_data_split_count: usize = limited_plan.splits().iter().count();

    // For data evolution tables, limit pushdown at split level uses merged_row_count
    // The limited data split count should be < full data split count
    assert!(
        limited_data_split_count < full_data_split_count,
        "Limit pushdown should reduce data split count for data evolution table: limited={limited_data_split_count}, full={full_data_split_count}"
    );

    // Verify data evolution splits have merged_row_count
    for split in full_plan.splits() {
        let merged_count = split.merged_row_count().expect(
            "Data evolution table should have merged_row_count (all files should have first_row_id)",
        );
        // merged_row_count should be < row_count (overlapping ranges reduce count)
        assert!(
            merged_count < split.row_count(),
            "merged_row_count ({}) should be < row_count ({})",
            merged_count,
            split.row_count()
        );
    }
}

// ---------------------------------------------------------------------------
// Schema Evolution integration tests
// ---------------------------------------------------------------------------

/// Test reading a table after ALTER TABLE ADD COLUMNS.
/// Old data files lack the new column; reader should fill nulls.
#[tokio::test]
async fn test_read_schema_evolution_add_column() {
    let (_, batches) = scan_and_read_with_fs_catalog("schema_evolution_add_column", None).await;

    let mut rows: Vec<(i32, String, Option<i32>)> = Vec::new();
    for batch in &batches {
        let id = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("id");
        let name = batch
            .column_by_name("name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("name");
        let age = batch
            .column_by_name("age")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("age");
        for i in 0..batch.num_rows() {
            let age_val = if age.is_null(i) {
                None
            } else {
                Some(age.value(i))
            };
            rows.push((id.value(i), name.value(i).to_string(), age_val));
        }
    }
    rows.sort_by_key(|(id, _, _)| *id);

    assert_eq!(
        rows,
        vec![
            (1, "alice".into(), None),
            (2, "bob".into(), None),
            (3, "carol".into(), Some(30)),
            (4, "dave".into(), Some(40)),
        ],
        "Old rows should have null for added column 'age'"
    );
}

/// Test reading a table after ALTER TABLE ALTER COLUMN TYPE (INT -> BIGINT).
/// Old data files have INT; reader should cast to BIGINT.
#[tokio::test]
async fn test_read_schema_evolution_type_promotion() {
    let (_, batches) = scan_and_read_with_fs_catalog("schema_evolution_type_promotion", None).await;

    // Verify the value column is Int64 (BIGINT) in all batches
    for batch in &batches {
        let value_col = batch.column_by_name("value").expect("value column");
        assert_eq!(
            value_col.data_type(),
            &arrow_array::types::Int64Type::DATA_TYPE,
            "value column should be Int64 (BIGINT) after type promotion"
        );
    }

    let mut rows: Vec<(i32, i64)> = Vec::new();
    for batch in &batches {
        let id = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("id");
        let value = batch
            .column_by_name("value")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .expect("value as Int64Array");
        for i in 0..batch.num_rows() {
            rows.push((id.value(i), value.value(i)));
        }
    }
    rows.sort_by_key(|(id, _)| *id);

    assert_eq!(
        rows,
        vec![(1, 100i64), (2, 200i64), (3, 3_000_000_000i64)],
        "INT values should be promoted to BIGINT, including values > INT_MAX"
    );
}

/// Stats pruning should treat a newly added column as all-NULL for old files.
#[tokio::test]
async fn test_stats_pruning_schema_evolution_added_column_eq_prunes_old_files() {
    use paimon::spec::{Datum, PredicateBuilder};

    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "schema_evolution_add_column").await;
    let pb = PredicateBuilder::new(table.schema().fields());
    let filter = pb
        .equal("age", Datum::Int(30))
        .expect("Failed to build predicate");

    let (plan, batches) = scan_and_read_with_filter(&table, filter).await;
    assert_eq!(
        plan.splits().len(),
        1,
        "Only the file written after ADD COLUMN should survive stats pruning"
    );

    let actual = extract_id_name(&batches);
    assert_eq!(
        actual,
        vec![(3, "carol".to_string())],
        "Old files missing 'age' and rows with age != 30 should be pruned"
    );
}

/// Stats pruning should keep only old files for IS NULL on a newly added column.
#[tokio::test]
async fn test_stats_pruning_schema_evolution_added_column_is_null_prunes_new_files() {
    use paimon::spec::PredicateBuilder;

    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "schema_evolution_add_column").await;
    let pb = PredicateBuilder::new(table.schema().fields());
    let filter = pb.is_null("age").expect("Failed to build predicate");

    let (plan, batches) = scan_and_read_with_filter(&table, filter).await;
    assert_eq!(
        plan.splits().len(),
        1,
        "Only files missing 'age' should survive stats pruning for age IS NULL"
    );

    let actual = extract_id_name(&batches);
    assert_eq!(
        actual,
        vec![(1, "alice".to_string()), (2, "bob".to_string())],
        "New files with non-null age should be pruned for age IS NULL"
    );
}

/// Stats pruning should still work after INT -> BIGINT type promotion.
#[tokio::test]
async fn test_stats_pruning_schema_evolution_type_promotion_prunes_old_int_files() {
    use paimon::spec::{Datum, PredicateBuilder};

    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "schema_evolution_type_promotion").await;
    let pb = PredicateBuilder::new(table.schema().fields());
    let filter = pb
        .greater_than("value", Datum::Long(250))
        .expect("Failed to build predicate");

    let (plan, batches) = scan_and_read_with_filter(&table, filter).await;
    assert_eq!(
        plan.splits().len(),
        1,
        "Old INT files should still be pruned using promoted BIGINT predicates"
    );

    let mut rows: Vec<(i32, i64)> = Vec::new();
    for batch in &batches {
        let id = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("id");
        let value = batch
            .column_by_name("value")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .expect("value");
        for i in 0..batch.num_rows() {
            rows.push((id.value(i), value.value(i)));
        }
    }
    rows.sort_by_key(|(id, _)| *id);

    assert_eq!(
        rows,
        vec![(3, 3_000_000_000i64)],
        "Only the BIGINT file should remain after value > 250 pruning"
    );
}

/// Test reading a data-evolution table after ALTER TABLE ADD COLUMNS.
/// Old files lack the new column; reader should fill nulls even in data evolution mode.
#[tokio::test]
async fn test_read_data_evolution_add_column() {
    let (_, batches) = scan_and_read_with_fs_catalog("data_evolution_add_column", None).await;

    let mut rows: Vec<(i32, String, i32, Option<String>)> = Vec::new();
    for batch in &batches {
        let id = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("id");
        let name = batch
            .column_by_name("name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("name");
        let value = batch
            .column_by_name("value")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("value");
        let extra = batch
            .column_by_name("extra")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("extra");
        for i in 0..batch.num_rows() {
            let extra_val = if extra.is_null(i) {
                None
            } else {
                Some(extra.value(i).to_string())
            };
            rows.push((
                id.value(i),
                name.value(i).to_string(),
                value.value(i),
                extra_val,
            ));
        }
    }
    rows.sort_by_key(|(id, _, _, _)| *id);

    assert_eq!(
        rows,
        vec![
            (1, "alice-v2".into(), 100, None),
            (2, "bob".into(), 200, None),
            (3, "carol".into(), 300, Some("new".into())),
            (4, "dave".into(), 400, Some("new".into())),
        ],
        "Data evolution + add column: old rows should have null for 'extra', MERGE INTO updates name"
    );
}

/// Test reading a data-evolution table after ALTER TABLE ALTER COLUMN TYPE (INT -> BIGINT).
/// Old files have INT; reader should cast to BIGINT in data evolution mode.
#[tokio::test]
async fn test_read_data_evolution_type_promotion() {
    let (_, batches) = scan_and_read_with_fs_catalog("data_evolution_type_promotion", None).await;

    // Verify the value column is Int64 (BIGINT) in all batches
    for batch in &batches {
        let value_col = batch.column_by_name("value").expect("value column");
        assert_eq!(
            value_col.data_type(),
            &arrow_array::types::Int64Type::DATA_TYPE,
            "value column should be Int64 (BIGINT) after type promotion in data evolution mode"
        );
    }

    let mut rows: Vec<(i32, i64)> = Vec::new();
    for batch in &batches {
        let id = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("id");
        let value = batch
            .column_by_name("value")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .expect("value as Int64Array");
        for i in 0..batch.num_rows() {
            rows.push((id.value(i), value.value(i)));
        }
    }
    rows.sort_by_key(|(id, _)| *id);

    assert_eq!(
        rows,
        vec![(1, 999i64), (2, 200i64), (3, 3_000_000_000i64)],
        "Data evolution + type promotion: INT should be cast to BIGINT, MERGE INTO updates value"
    );
}

/// Test reading a table after ALTER TABLE DROP COLUMN.
/// Old data files have the dropped column; reader should ignore it.
#[tokio::test]
async fn test_read_schema_evolution_drop_column() {
    let (_, batches) = scan_and_read_with_fs_catalog("schema_evolution_drop_column", None).await;

    // Verify the dropped column 'score' is not present in the output.
    for batch in &batches {
        assert!(
            batch.column_by_name("score").is_none(),
            "Dropped column 'score' should not appear in output"
        );
    }

    let mut rows: Vec<(i32, String)> = Vec::new();
    for batch in &batches {
        let id = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("id");
        let name = batch
            .column_by_name("name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("name");
        for i in 0..batch.num_rows() {
            rows.push((id.value(i), name.value(i).to_string()));
        }
    }
    rows.sort_by_key(|(id, _)| *id);

    assert_eq!(
        rows,
        vec![
            (1, "alice".into()),
            (2, "bob".into()),
            (3, "carol".into()),
            (4, "dave".into()),
        ],
        "Old rows should be readable after DROP COLUMN, with only remaining columns"
    );
}

// ---------------------------------------------------------------------------
// Complex type integration tests
// ---------------------------------------------------------------------------

/// Test reading a table with complex types: ARRAY<INT>, MAP<STRING, INT>, STRUCT<name: STRING, value: INT>.
#[tokio::test]
async fn test_read_complex_type_table() {
    let (_, batches) = scan_and_read_with_fs_catalog("complex_type_table", None).await;

    #[allow(clippy::type_complexity)]
    let mut rows: Vec<(i32, Vec<i32>, Vec<(String, i32)>, (String, i32))> = Vec::new();
    for batch in &batches {
        let id = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("id");
        let int_array = batch
            .column_by_name("int_array")
            .and_then(|c| c.as_any().downcast_ref::<ListArray>())
            .expect("int_array as ListArray");
        let string_map = batch
            .column_by_name("string_map")
            .and_then(|c| c.as_any().downcast_ref::<MapArray>())
            .expect("string_map as MapArray");
        let row_field = batch
            .column_by_name("row_field")
            .and_then(|c| c.as_any().downcast_ref::<StructArray>())
            .expect("row_field as StructArray");

        for i in 0..batch.num_rows() {
            // Extract ARRAY<INT>
            let list_values = int_array.value(i);
            let int_arr = list_values
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("list element as Int32Array");
            let arr_vals: Vec<i32> = (0..int_arr.len()).map(|j| int_arr.value(j)).collect();

            // Extract MAP<STRING, INT>
            let map_val = string_map.value(i);
            let map_struct = map_val
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("map entries as StructArray");
            let keys = map_struct
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("map keys");
            let values = map_struct
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("map values");
            let mut map_entries: Vec<(String, i32)> = (0..keys.len())
                .map(|j| (keys.value(j).to_string(), values.value(j)))
                .collect();
            map_entries.sort_by(|a, b| a.0.cmp(&b.0));

            // Extract STRUCT<name: STRING, value: INT>
            let struct_name = row_field
                .column_by_name("name")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .expect("struct name");
            let struct_value = row_field
                .column_by_name("value")
                .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
                .expect("struct value");

            rows.push((
                id.value(i),
                arr_vals,
                map_entries,
                (struct_name.value(i).to_string(), struct_value.value(i)),
            ));
        }
    }
    rows.sort_by_key(|(id, _, _, _)| *id);

    assert_eq!(
        rows,
        vec![
            (
                1,
                vec![1, 2, 3],
                vec![("a".into(), 10), ("b".into(), 20)],
                ("alice".into(), 100),
            ),
            (2, vec![4, 5], vec![("c".into(), 30)], ("bob".into(), 200),),
            (3, vec![], vec![], ("carol".into(), 300),),
        ],
        "Complex type table should return correct ARRAY, MAP, and STRUCT values"
    );
}

// ---------------------------------------------------------------------------
// PK-without-DV and non-PK-with-DV tests
// ---------------------------------------------------------------------------

/// Reading a primary-key table without deletion vectors should return an Unsupported error.
#[tokio::test]
async fn test_read_pk_table_without_dv_returns_error() {
    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "simple_pk_table").await;

    let read_builder = table.new_read_builder();
    let scan = read_builder.new_scan();
    let plan = scan.plan().await.expect("Failed to plan scan");
    assert!(
        !plan.splits().is_empty(),
        "PK table should have splits to read"
    );

    let read = table.new_read_builder().new_read();
    let result = read
        .expect("new_read should succeed")
        .to_arrow(plan.splits());
    let err = result
        .err()
        .expect("Reading PK table without DV should fail");

    assert!(
        matches!(&err, Error::Unsupported { message } if message.contains("primary-key")),
        "Expected Unsupported error about primary-key tables, got: {err:?}"
    );
}

/// Reading a non-PK (append-only) table with deletion vectors enabled should work correctly.
/// Level-0 files must NOT be filtered out since there is no PK merge.
#[tokio::test]
async fn test_read_non_pk_table_with_dv() {
    let (_, batches) = scan_and_read_with_fs_catalog("simple_dv_log_table", None).await;
    let actual = extract_id_name(&batches);
    let expected = vec![
        (1, "alice".to_string()),
        (2, "bob".to_string()),
        (3, "carol".to_string()),
    ];
    assert_eq!(
        actual, expected,
        "Non-PK table with DV enabled should return all rows (level-0 files kept)"
    );
}

/// Postpone bucket PK table (bucket = -2): uncompacted data sits in bucket-postpone
/// and should NOT be visible to batch readers. The plan should produce no splits.
#[tokio::test]
async fn test_read_postpone_bucket_pk_table_returns_empty() {
    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "postpone_bucket_pk_table").await;

    let read_builder = table.new_read_builder();
    let scan = read_builder.new_scan();
    let plan = scan.plan().await.expect("Failed to plan scan");

    assert!(
        plan.splits().is_empty(),
        "Postpone bucket PK table should have no visible splits before compaction"
    );
}

// ---------------------------------------------------------------------------
// Data evolution predicate filtering tests
// ---------------------------------------------------------------------------

/// Data evolution group-level predicate filtering: after group_by_overlapping_row_id,
/// merged stats across files in each group should allow pruning entire groups.
#[tokio::test]
async fn test_data_evolution_table_with_filter() {
    use paimon::spec::{Datum, PredicateBuilder};

    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "data_evolution_table").await;
    let pb = PredicateBuilder::new(table.schema().fields());

    // Filter: value > 300 should keep only groups containing rows with value > 300.
    // Expected rows after merge: (4, 'dave', 400), (5, 'eve', 500)
    let filter = pb
        .greater_than("value", Datum::Int(300))
        .expect("Failed to build predicate");

    let (plan, batches) = scan_and_read_with_filter(&table, filter).await;

    // The first batch (rows 1-3) was MERGE INTO'd, creating overlapping row_id groups.
    // Their max value is 300, so the group should be pruned by value > 300.
    // The second batch (rows 4-5) has values 400, 500 and should survive.
    assert!(
        !plan.splits().is_empty(),
        "Some splits should survive the filter"
    );

    let mut rows: Vec<(i32, String, i32)> = Vec::new();
    for batch in &batches {
        let id = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("id");
        let name = batch
            .column_by_name("name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("name");
        let value = batch
            .column_by_name("value")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("value");
        for i in 0..batch.num_rows() {
            rows.push((id.value(i), name.value(i).to_string(), value.value(i)));
        }
    }
    rows.sort_by_key(|(id, _, _)| *id);

    assert_eq!(
        rows,
        vec![(4, "dave".into(), 400), (5, "eve".into(), 500),],
        "Data evolution group-level pruning should filter out groups where value <= 300"
    );
}

// ---------------------------------------------------------------------------
// Bucket predicate filtering tests
// ---------------------------------------------------------------------------

/// Bucket predicate filtering: when filtering by bucket key (primary key) with an
/// equality predicate, only splits whose bucket matches the computed target bucket
/// should survive. This tests the full pipeline: extract bucket predicate → compute
/// target bucket via MurmurHash3 → filter manifest entries by bucket.
#[tokio::test]
async fn test_bucket_predicate_filtering() {
    use paimon::spec::{Datum, PredicateBuilder};

    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "multi_bucket_pk_table").await;
    let schema = table.schema();
    let pb = PredicateBuilder::new(schema.fields());

    // Get full plan without filter to see all buckets
    let full_plan = plan_table(&table, None).await;
    let all_buckets: HashSet<i32> = full_plan.splits().iter().map(|s| s.bucket()).collect();
    assert!(
        all_buckets.len() > 1,
        "multi_bucket_pk_table should have data in multiple buckets, got: {all_buckets:?}"
    );

    // Filter by id = 1 (bucket key). This should compute the target bucket and
    // only return splits from that bucket.
    let filter = pb
        .equal("id", Datum::Int(1))
        .expect("Failed to build predicate");
    let (plan, batches) = scan_and_read_with_filter(&table, filter).await;

    let filtered_buckets: HashSet<i32> = plan.splits().iter().map(|s| s.bucket()).collect();
    assert_eq!(
        filtered_buckets.len(),
        1,
        "Bucket predicate filtering should narrow to exactly one bucket, got: {filtered_buckets:?}"
    );
    assert!(
        filtered_buckets.is_subset(&all_buckets),
        "Filtered bucket should be one of the original buckets"
    );

    let actual = extract_id_name(&batches);
    // Bucket filtering is at the bucket level, not row level. Other rows that
    // hash to the same bucket will also be returned.
    let ids: HashSet<i32> = actual.iter().map(|(id, _)| *id).collect();
    assert!(
        ids.contains(&1),
        "Row with id=1 should be in the filtered result, got: {actual:?}"
    );
    // Verify we got fewer rows than the full table (8 rows)
    assert!(
        actual.len() < 8,
        "Bucket filtering should return fewer rows than the full table, got: {}",
        actual.len()
    );
}

/// Bucket predicate filtering with IN predicate: multiple target buckets.
#[tokio::test]
async fn test_bucket_predicate_filtering_in() {
    use paimon::spec::{Datum, PredicateBuilder};

    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "multi_bucket_pk_table").await;
    let schema = table.schema();
    let pb = PredicateBuilder::new(schema.fields());

    // Filter by id IN (1, 5) — may hash to different buckets
    let filter = pb
        .is_in("id", vec![Datum::Int(1), Datum::Int(5)])
        .expect("Failed to build predicate");
    let (plan, batches) = scan_and_read_with_filter(&table, filter).await;

    let filtered_buckets: HashSet<i32> = plan.splits().iter().map(|s| s.bucket()).collect();
    assert!(
        filtered_buckets.len() <= 2,
        "IN predicate with 2 values should produce at most 2 target buckets, got: {filtered_buckets:?}"
    );

    let actual = extract_id_name(&batches);
    // Should contain exactly id=1 and id=5
    let ids: HashSet<i32> = actual.iter().map(|(id, _)| *id).collect();
    assert!(
        ids.contains(&1) && ids.contains(&5),
        "Should return rows for id=1 and id=5, got: {actual:?}"
    );
}

// ---------------------------------------------------------------------------
// Time travel integration tests
// ---------------------------------------------------------------------------

/// Time travel by snapshot id: snapshot 1 should return only the first batch.
#[tokio::test]
async fn test_time_travel_by_snapshot_id() {
    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "time_travel_table").await;

    // Snapshot 1: (1, 'alice'), (2, 'bob')
    let table_snap1 = table.copy_with_options(HashMap::from([(
        "scan.version".to_string(),
        "1".to_string(),
    )]));
    let rb = table_snap1.new_read_builder();
    let plan = rb.new_scan().plan().await.expect("plan snap1");
    let read = rb.new_read().expect("read snap1");
    let batches: Vec<RecordBatch> = read
        .to_arrow(plan.splits())
        .expect("stream")
        .try_collect()
        .await
        .expect("collect");
    let actual = extract_id_name(&batches);
    assert_eq!(
        actual,
        vec![(1, "alice".into()), (2, "bob".into())],
        "Snapshot 1 should contain only the first batch"
    );

    // Snapshot 2: (1, 'alice'), (2, 'bob'), (3, 'carol'), (4, 'dave')
    let table_snap2 = table.copy_with_options(HashMap::from([(
        "scan.version".to_string(),
        "2".to_string(),
    )]));
    let rb2 = table_snap2.new_read_builder();
    let plan2 = rb2.new_scan().plan().await.expect("plan snap2");
    let read2 = rb2.new_read().expect("read snap2");
    let batches2: Vec<RecordBatch> = read2
        .to_arrow(plan2.splits())
        .expect("stream")
        .try_collect()
        .await
        .expect("collect");
    let actual2 = extract_id_name(&batches2);
    assert_eq!(
        actual2,
        vec![
            (1, "alice".into()),
            (2, "bob".into()),
            (3, "carol".into()),
            (4, "dave".into()),
        ],
        "Snapshot 2 should contain all rows"
    );
}

/// Time travel by tag name.
#[tokio::test]
async fn test_time_travel_by_tag_name() {
    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "time_travel_table").await;

    // Tag 'snapshot1' -> snapshot 1: (1, 'alice'), (2, 'bob')
    let table_tag1 = table.copy_with_options(HashMap::from([(
        "scan.version".to_string(),
        "snapshot1".to_string(),
    )]));
    let rb = table_tag1.new_read_builder();
    let plan = rb.new_scan().plan().await.expect("plan tag1");
    let read = rb.new_read().expect("read tag1");
    let batches: Vec<RecordBatch> = read
        .to_arrow(plan.splits())
        .expect("stream")
        .try_collect()
        .await
        .expect("collect");
    let actual = extract_id_name(&batches);
    assert_eq!(
        actual,
        vec![(1, "alice".into()), (2, "bob".into())],
        "Tag 'snapshot1' should return snapshot 1 data"
    );

    // Tag 'snapshot2' -> snapshot 2: all 4 rows
    let table_tag2 = table.copy_with_options(HashMap::from([(
        "scan.version".to_string(),
        "snapshot2".to_string(),
    )]));
    let rb2 = table_tag2.new_read_builder();
    let plan2 = rb2.new_scan().plan().await.expect("plan tag2");
    let read2 = rb2.new_read().expect("read tag2");
    let batches2: Vec<RecordBatch> = read2
        .to_arrow(plan2.splits())
        .expect("stream")
        .try_collect()
        .await
        .expect("collect");
    let actual2 = extract_id_name(&batches2);
    assert_eq!(
        actual2,
        vec![
            (1, "alice".into()),
            (2, "bob".into()),
            (3, "carol".into()),
            (4, "dave".into()),
        ],
        "Tag 'snapshot2' should return all rows"
    );
}

#[tokio::test]
async fn test_time_travel_conflicting_selectors_fail() {
    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "time_travel_table").await;

    let conflicted = table.copy_with_options(HashMap::from([
        ("scan.version".to_string(), "snapshot1".to_string()),
        ("scan.timestamp-millis".to_string(), "1234".to_string()),
    ]));

    let plan_err = conflicted
        .new_read_builder()
        .new_scan()
        .plan()
        .await
        .expect_err("conflicting time-travel selectors should fail");

    match plan_err {
        Error::DataInvalid { message, .. } => {
            assert!(
                message.contains("Only one time-travel selector may be set"),
                "unexpected conflict error: {message}"
            );
            assert!(
                message.contains("scan.version"),
                "conflict error should mention scan.version: {message}"
            );
            assert!(
                message.contains("scan.timestamp-millis"),
                "conflict error should mention scan.timestamp-millis: {message}"
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test]
async fn test_time_travel_invalid_version_fails() {
    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "time_travel_table").await;

    let invalid = table.copy_with_options(HashMap::from([(
        "scan.version".to_string(),
        "nonexistent-tag".to_string(),
    )]));

    let plan_err = invalid
        .new_read_builder()
        .new_scan()
        .plan()
        .await
        .expect_err("invalid version should fail");

    match plan_err {
        Error::DataInvalid { message, .. } => {
            assert!(
                message.contains("is not a valid tag name or snapshot id"),
                "unexpected invalid version error: {message}"
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Data evolution + drop column tests
// ---------------------------------------------------------------------------

/// Data evolution + drop column: old rows that were MERGE INTO'd should have NULL
/// for the newly added column (no file in the merge group provides it).
#[tokio::test]
async fn test_read_data_evolution_drop_column() {
    let (_, batches) = scan_and_read_with_fs_catalog("data_evolution_drop_column", None).await;

    let mut rows: Vec<(i32, String, i32, Option<String>)> = Vec::new();
    for batch in &batches {
        let id = batch
            .column_by_name("id")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("id");
        let name = batch
            .column_by_name("name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("name");
        let value = batch
            .column_by_name("value")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("value");
        let extra = batch
            .column_by_name("extra")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("extra");
        for i in 0..batch.num_rows() {
            let extra_val = if extra.is_null(i) {
                None
            } else {
                Some(extra.value(i).to_string())
            };
            rows.push((
                id.value(i),
                name.value(i).to_string(),
                value.value(i),
                extra_val,
            ));
        }
    }
    rows.sort_by_key(|(id, _, _, _)| *id);

    assert_eq!(
        rows,
        vec![
            (1, "alice-v2".into(), 100, None),
            (2, "bob".into(), 200, None),
            (3, "carol".into(), 300, Some("new".into())),
        ],
        "Old rows should have NULL for 'extra' (added after MERGE INTO), new rows should have it"
    );
}

// ---------------------------------------------------------------------------
// Limit pushdown with data predicates test
// ---------------------------------------------------------------------------

/// Limit pushdown must be disabled when data predicates exist.
/// Otherwise merged_row_count (pre-filter) could cause early stop, returning
/// fewer rows than the limit after filtering.
#[tokio::test]
async fn test_limit_pushdown_disabled_with_data_predicates() {
    use paimon::spec::{Datum, PredicateBuilder};

    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "data_evolution_table").await;
    let pb = PredicateBuilder::new(table.schema().fields());

    // Filter: value >= 100 (matches all rows). With limit=2, if limit pushdown
    // were applied, it might stop after the first split (merged_row_count >= 2)
    // but that split's rows might all be filtered out by a stricter predicate.
    // Here we use a lenient predicate to verify the plan still includes enough splits.
    let filter = pb
        .greater_than("value", Datum::Int(0))
        .expect("Failed to build predicate");

    let mut read_builder = table.new_read_builder();
    read_builder.with_filter(filter);
    read_builder.with_limit(2);
    let scan = read_builder.new_scan();
    let plan = scan.plan().await.expect("Failed to plan scan");

    // With data predicates, limit pushdown should be disabled, so we should get
    // the same number of splits as without limit.
    let full_plan = plan_table(&table, None).await;
    assert_eq!(
        plan.splits().len(),
        full_plan.splits().len(),
        "With data predicates, limit pushdown should be disabled — split count should match full plan"
    );
}

// ---------------------------------------------------------------------------
// String bucket key tests (variable-length hash compatibility with Java)
// ---------------------------------------------------------------------------

/// Helper to extract (code, value) rows from batches.
fn extract_code_value(batches: &[RecordBatch]) -> Vec<(String, i32)> {
    let mut rows = Vec::new();
    for batch in batches {
        let code = batch
            .column_by_name("code")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .expect("code");
        let value = batch
            .column_by_name("value")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("value");
        for i in 0..batch.num_rows() {
            rows.push((code.value(i).to_string(), value.value(i)));
        }
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

/// Bucket predicate filtering with short string keys (<=7 bytes, inline encoding).
#[tokio::test]
async fn test_bucket_predicate_filtering_short_string_key() {
    use paimon::spec::{Datum, PredicateBuilder};

    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "string_bucket_short_key").await;
    let pb = PredicateBuilder::new(table.schema().fields());

    let full_plan = plan_table(&table, None).await;
    let all_buckets: HashSet<i32> = full_plan.splits().iter().map(|s| s.bucket()).collect();
    assert!(
        all_buckets.len() > 1,
        "string_bucket_short_key should have data in multiple buckets, got: {all_buckets:?}"
    );

    // Filter by code = 'aaa' (short string, inline BinaryRow encoding)
    let filter = pb
        .equal("code", Datum::String("aaa".into()))
        .expect("Failed to build predicate");
    let (plan, batches) = scan_and_read_with_filter(&table, filter).await;

    let filtered_buckets: HashSet<i32> = plan.splits().iter().map(|s| s.bucket()).collect();
    assert_eq!(
        filtered_buckets.len(),
        1,
        "Short string bucket filtering should narrow to one bucket, got: {filtered_buckets:?}"
    );

    let actual = extract_code_value(&batches);
    let codes: HashSet<&str> = actual.iter().map(|(c, _)| c.as_str()).collect();
    assert!(
        codes.contains("aaa"),
        "Row with code='aaa' should be in the result, got: {actual:?}"
    );
    assert!(
        actual.len() < 8,
        "Bucket filtering should return fewer rows than the full table, got: {}",
        actual.len()
    );
}

/// Bucket predicate filtering with long string keys (>7 bytes, variable-length encoding).
#[tokio::test]
async fn test_bucket_predicate_filtering_long_string_key() {
    use paimon::spec::{Datum, PredicateBuilder};

    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "string_bucket_long_key").await;
    let pb = PredicateBuilder::new(table.schema().fields());

    let full_plan = plan_table(&table, None).await;
    let all_buckets: HashSet<i32> = full_plan.splits().iter().map(|s| s.bucket()).collect();
    assert!(
        all_buckets.len() > 1,
        "string_bucket_long_key should have data in multiple buckets, got: {all_buckets:?}"
    );

    // Filter by code = 'alpha-long-key' (>7 bytes, var-length BinaryRow encoding with 8-byte padding)
    let filter = pb
        .equal("code", Datum::String("alpha-long-key".into()))
        .expect("Failed to build predicate");
    let (plan, batches) = scan_and_read_with_filter(&table, filter).await;

    let filtered_buckets: HashSet<i32> = plan.splits().iter().map(|s| s.bucket()).collect();
    assert_eq!(
        filtered_buckets.len(),
        1,
        "Long string bucket filtering should narrow to one bucket, got: {filtered_buckets:?}"
    );

    let actual = extract_code_value(&batches);
    let codes: HashSet<&str> = actual.iter().map(|(c, _)| c.as_str()).collect();
    assert!(
        codes.contains("alpha-long-key"),
        "Row with code='alpha-long-key' should be in the result, got: {actual:?}"
    );
    assert!(
        actual.len() < 8,
        "Bucket filtering should return fewer rows than the full table, got: {}",
        actual.len()
    );
}

// ---------------------------------------------------------------------------
// Data Evolution Row ID Range Filter integration tests
// ---------------------------------------------------------------------------

async fn scan_and_read_with_row_ranges(
    table: &paimon::Table,
    row_ranges: Vec<paimon::RowRange>,
) -> (Plan, Vec<RecordBatch>) {
    let mut read_builder = table.new_read_builder();
    read_builder.with_row_ranges(row_ranges);
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

fn extract_id_name_value(batches: &[RecordBatch]) -> Vec<(i32, String, i32)> {
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
        let value = batch
            .column_by_name("value")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .expect("value");
        for i in 0..batch.num_rows() {
            rows.push((id.value(i), name.value(i).to_string(), value.value(i)));
        }
    }
    rows.sort_by_key(|(id, _, _)| *id);
    rows
}

#[tokio::test]
async fn test_read_data_evolution_table_with_row_ranges() {
    use paimon::RowRange;

    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "data_evolution_table").await;

    let (full_plan, full_batches) = scan_and_read(&catalog, "data_evolution_table", None).await;
    let full_rows = extract_id_name_value(&full_batches);
    let full_row_count: usize = full_batches.iter().map(|b| b.num_rows()).sum();
    assert!(full_row_count > 0);

    let mut min_row_id = i64::MAX;
    let mut max_row_id_exclusive = i64::MIN;
    for split in full_plan.splits() {
        for file in split.data_files() {
            if let Some(fid) = file.first_row_id {
                min_row_id = min_row_id.min(fid);
                max_row_id_exclusive = max_row_id_exclusive.max(fid + file.row_count);
            }
        }
    }
    assert!(min_row_id < max_row_id_exclusive);

    let mid = min_row_id + (max_row_id_exclusive - min_row_id) / 2;
    let (filtered_plan, filtered_batches) =
        scan_and_read_with_row_ranges(&table, vec![RowRange::new(min_row_id, mid)]).await;

    let filtered_row_count: usize = filtered_batches.iter().map(|b| b.num_rows()).sum();
    let filtered_rows = extract_id_name_value(&filtered_batches);

    assert!(
        filtered_row_count < full_row_count || mid >= max_row_id_exclusive,
        "filtered={filtered_row_count}, full={full_row_count}"
    );
    for row in &filtered_rows {
        assert!(
            full_rows.contains(row),
            "Filtered row {row:?} not in full result"
        );
    }
    assert!(filtered_plan.splits().len() <= full_plan.splits().len());
}

#[tokio::test]
async fn test_read_data_evolution_table_with_empty_row_ranges() {
    use paimon::RowRange;

    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "data_evolution_table").await;

    let (plan, batches) =
        scan_and_read_with_row_ranges(&table, vec![RowRange::new(999_999, 1_000_000)]).await;

    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(row_count, 0);
    assert!(plan.splits().is_empty());
}

#[tokio::test]
async fn test_read_data_evolution_table_with_full_row_ranges() {
    use paimon::RowRange;

    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "data_evolution_table").await;

    let (_, full_batches) = scan_and_read(&catalog, "data_evolution_table", None).await;
    let full_rows = extract_id_name_value(&full_batches);

    let (_, filtered_batches) =
        scan_and_read_with_row_ranges(&table, vec![RowRange::new(0, i64::MAX)]).await;
    let filtered_rows = extract_id_name_value(&filtered_batches);

    assert_eq!(filtered_rows, full_rows);
}

#[tokio::test]
async fn test_read_data_evolution_table_with_row_id_projection() {
    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "data_evolution_table").await;

    // Project _ROW_ID along with regular columns
    let mut read_builder = table.new_read_builder();
    read_builder.with_projection(&["_ROW_ID", "id", "name"]);
    let scan = read_builder.new_scan();
    let plan = scan.plan().await.expect("Failed to plan scan");

    let read = read_builder.new_read().expect("Failed to create read");
    let stream = read
        .to_arrow(plan.splits())
        .expect("Failed to create arrow stream");
    let batches: Vec<RecordBatch> = stream
        .try_collect()
        .await
        .expect("Failed to collect batches");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows > 0, "Should have rows");

    // Verify _ROW_ID column exists and contains non-negative values
    let mut row_ids: Vec<i64> = Vec::new();
    for batch in &batches {
        let row_id_col = batch
            .column_by_name("_ROW_ID")
            .expect("_ROW_ID column should exist");
        let row_id_array = row_id_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("_ROW_ID should be Int64");
        for i in 0..batch.num_rows() {
            row_ids.push(row_id_array.value(i));
        }
    }

    assert_eq!(row_ids.len(), total_rows);
    assert!(
        row_ids.iter().all(|&id| id >= 0),
        "All _ROW_ID values should be non-negative"
    );
    // _ROW_ID values should be unique
    let unique: std::collections::HashSet<i64> = row_ids.iter().copied().collect();
    assert_eq!(
        unique.len(),
        row_ids.len(),
        "_ROW_ID values should be unique"
    );
}

#[tokio::test]
async fn test_read_data_evolution_table_only_row_id_with_row_ranges() {
    use paimon::RowRange;

    let catalog = create_file_system_catalog();
    let table = get_table_from_catalog(&catalog, "data_evolution_table").await;

    // Get full row ID range
    let full_rb = table.new_read_builder();
    let full_plan = full_rb.new_scan().plan().await.expect("plan");
    let mut min_row_id = i64::MAX;
    let mut max_row_id = i64::MIN;
    for split in full_plan.splits() {
        for file in split.data_files() {
            if let Some(fid) = file.first_row_id {
                min_row_id = min_row_id.min(fid);
                max_row_id = max_row_id.max(fid + file.row_count - 1);
            }
        }
    }

    // Project only _ROW_ID with a partial row range
    let mid = min_row_id + (max_row_id - min_row_id) / 2;
    let mut read_builder = table.new_read_builder();
    read_builder.with_projection(&["_ROW_ID"]);
    read_builder.with_row_ranges(vec![RowRange::new(min_row_id, mid)]);
    let scan = read_builder.new_scan();
    let plan = scan.plan().await.expect("plan");

    let read = read_builder.new_read().expect("read");
    let stream = read.to_arrow(plan.splits()).expect("stream");
    let batches: Vec<RecordBatch> = stream.try_collect().await.expect("collect");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows > 0, "Should have rows");
    // Should have fewer rows than full table due to row_ranges filtering
    let full_read = table.new_read_builder().new_read().expect("read");
    let full_count: usize = full_read
        .to_arrow(full_plan.splits())
        .expect("stream")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect")
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert!(
        total_rows <= full_count,
        "Row range filtered count ({total_rows}) should be <= full count ({full_count})"
    );
}

// ---------------------------------------------------------------------------
// Full types integration tests (parquet + orc + avro)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_read_full_types_table() {
    use arrow_array::{
        BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
        Int16Array, Int64Array, Int8Array, ListArray, MapArray, StructArray,
        TimestampMicrosecondArray,
    };

    let (_, batches) = scan_and_read_with_fs_catalog("full_types_table", None).await;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "full_types_table should have 3 rows");

    // Collect all rows sorted by id.
    // We verify primitive types via a tuple, then complex/extra decimal types separately.
    #[allow(clippy::type_complexity)]
    let mut rows: Vec<(
        i32,
        bool,
        i8,
        i16,
        i32,
        i64,
        f32,
        f64,
        i128,
        i128,
        i128,
        String,
        Vec<u8>,
        i32,
        i64,
        i64,
        Vec<i32>,
        Vec<(String, i32)>,
        (String, i32),
    )> = Vec::new();
    for batch in &batches {
        let id = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let col_boolean = batch
            .column_by_name("col_boolean")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let col_tinyint = batch
            .column_by_name("col_tinyint")
            .unwrap()
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap();
        let col_smallint = batch
            .column_by_name("col_smallint")
            .unwrap()
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap();
        let col_int = batch
            .column_by_name("col_int")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let col_bigint = batch
            .column_by_name("col_bigint")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let col_float = batch
            .column_by_name("col_float")
            .unwrap()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        let col_double = batch
            .column_by_name("col_double")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let col_decimal = batch
            .column_by_name("col_decimal")
            .unwrap()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        let col_decimal5 = batch
            .column_by_name("col_decimal5")
            .unwrap()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        let col_decimal38 = batch
            .column_by_name("col_decimal38")
            .unwrap()
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        let col_string = batch
            .column_by_name("col_string")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let col_binary = batch
            .column_by_name("col_binary")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let col_date = batch
            .column_by_name("col_date")
            .unwrap()
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        let col_ts = batch
            .column_by_name("col_timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let col_ts_ltz = batch
            .column_by_name("col_timestamp_ltz")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let col_array = batch
            .column_by_name("col_array")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let col_map = batch
            .column_by_name("col_map")
            .unwrap()
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap();
        let col_struct = batch
            .column_by_name("col_struct")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        for i in 0..batch.num_rows() {
            // Extract ARRAY<INT>
            let list_values = col_array.value(i);
            let int_arr = list_values.as_any().downcast_ref::<Int32Array>().unwrap();
            let arr_vals: Vec<i32> = (0..int_arr.len()).map(|j| int_arr.value(j)).collect();

            // Extract MAP<STRING, INT>
            let map_val = col_map.value(i);
            let map_struct = map_val.as_any().downcast_ref::<StructArray>().unwrap();
            let keys = map_struct
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let values = map_struct
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let mut map_entries: Vec<(String, i32)> = (0..keys.len())
                .map(|j| (keys.value(j).to_string(), values.value(j)))
                .collect();
            map_entries.sort_by(|a, b| a.0.cmp(&b.0));

            // Extract STRUCT<name: STRING, value: INT>
            let struct_name = col_struct
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let struct_value = col_struct
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();

            rows.push((
                id.value(i),
                col_boolean.value(i),
                col_tinyint.value(i),
                col_smallint.value(i),
                col_int.value(i),
                col_bigint.value(i),
                col_float.value(i),
                col_double.value(i),
                col_decimal.value(i),
                col_decimal5.value(i),
                col_decimal38.value(i),
                col_string.value(i).to_string(),
                col_binary.value(i).to_vec(),
                col_date.value(i),
                col_ts.value(i),
                col_ts_ltz.value(i),
                arr_vals,
                map_entries,
                (struct_name.value(i).to_string(), struct_value.value(i)),
            ));
        }
    }
    rows.sort_by_key(|r| r.0);

    assert_eq!(rows.len(), 3);

    // id=1 (parquet)
    let r = &rows[0];
    assert_eq!(r.0, 1);
    assert!(r.1); // boolean = true
    assert_eq!(r.2, 1i8); // tinyint
    assert_eq!(r.3, 100i16); // smallint
    assert_eq!(r.4, 1000); // int
    assert_eq!(r.5, 100000i64); // bigint
    assert!((r.6 - 1.5f32).abs() < f32::EPSILON); // float
    assert!((r.7 - 2.5f64).abs() < f64::EPSILON); // double
    assert_eq!(r.8, 12345); // decimal(10,2) = 123.45 * 100
    assert_eq!(r.9, 12345); // decimal(5,0) = 12345
    assert_eq!(r.10, 12_345_678_901_234_567_890_000i128); // decimal(38,18) 12345.678901234567890 * 10^18
    assert_eq!(r.11, "parquet-hello"); // string
    assert_eq!(r.12, vec![0xDE, 0xAD, 0xBE, 0xEF]); // binary
    assert_eq!(r.13, 19723); // date: 2024-01-01 days since epoch
    assert_eq!(r.14, 1_704_103_200_123_456); // ts micros
    assert_eq!(r.15, 1_704_103_200_123_456); // ts_ltz micros
    assert_eq!(r.16, vec![1, 2, 3]); // array
    assert_eq!(r.17, vec![("a".into(), 10), ("b".into(), 20)]); // map
    assert_eq!(r.18, ("alice".into(), 100)); // struct

    // id=2 (orc)
    let r = &rows[1];
    assert_eq!(r.0, 2);
    assert!(!r.1); // boolean = false
    assert_eq!(r.2, 2i8);
    assert_eq!(r.3, 200i16);
    assert_eq!(r.4, 2000);
    assert_eq!(r.5, 200000i64);
    assert!((r.6 - 3.5f32).abs() < f32::EPSILON);
    assert!((r.7 - 4.5f64).abs() < f64::EPSILON);
    assert_eq!(r.8, 67890); // 678.90 * 100
    assert_eq!(r.9, 99999); // decimal(5,0)
    assert_eq!(r.10, 99_999_999_999_999_999_999_000i128); // decimal(38,18) 99999.999999999999999 * 10^18
    assert_eq!(r.11, "orc-world");
    assert_eq!(r.12, vec![0xCA, 0xFE, 0xBA, 0xBE]);
    assert_eq!(r.13, 19889); // date: 2024-06-15 days since epoch
    assert_eq!(r.14, 1_718_454_600_456_789);
    assert_eq!(r.15, 1_718_454_600_456_789);
    assert_eq!(r.16, vec![4, 5]); // array
    assert_eq!(r.17, vec![("c".into(), 30)]); // map
    assert_eq!(r.18, ("bob".into(), 200)); // struct

    // id=3 (avro)
    let r = &rows[2];
    assert_eq!(r.0, 3);
    assert!(r.1); // boolean = true
    assert_eq!(r.2, 3i8);
    assert_eq!(r.3, 300i16);
    assert_eq!(r.4, 3000);
    assert_eq!(r.5, 300000i64);
    assert!((r.6 - 5.5f32).abs() < f32::EPSILON);
    assert!((r.7 - 6.5f64).abs() < f64::EPSILON);
    assert_eq!(r.8, 99999); // 999.99 * 100
    assert_eq!(r.9, 0); // decimal(5,0)
    assert_eq!(r.10, 1); // decimal(38,18) = 0.000000000000000001 * 10^18
    assert_eq!(r.11, "avro-test");
    assert_eq!(r.12, vec![0x01, 0x02, 0x03, 0x04]);
    assert_eq!(r.13, 20453); // date: 2025-12-31 days since epoch
    assert_eq!(r.14, 1_767_225_599_999_999);
    assert_eq!(r.15, 1_767_225_599_999_999);
    assert_eq!(r.16, vec![6]); // array
    assert_eq!(r.17, vec![("d".into(), 40), ("e".into(), 50)]); // map
    assert_eq!(r.18, ("carol".into(), 300)); // struct
}
