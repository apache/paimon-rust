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

//! E2E integration tests for append-only (no primary key) tables.
//!
//! Covers: unpartitioned, partitioned, bucket=-1, fixed bucket,
//! multiple commits, column projection, and bucket predicate filtering.

use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use futures::TryStreamExt;
use paimon::catalog::Identifier;
use paimon::io::FileIOBuilder;
use paimon::spec::{DataType, IntType, Schema, TableSchema, VarCharType};
use paimon::table::Table;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn memory_file_io() -> paimon::io::FileIO {
    FileIOBuilder::new("memory").build().unwrap()
}

async fn setup_dirs(file_io: &paimon::io::FileIO, table_path: &str) {
    file_io
        .mkdirs(&format!("{table_path}/snapshot/"))
        .await
        .unwrap();
    file_io
        .mkdirs(&format!("{table_path}/manifest/"))
        .await
        .unwrap();
}

fn make_table(file_io: &paimon::io::FileIO, table_path: &str, schema: TableSchema) -> Table {
    Table::new(
        file_io.clone(),
        Identifier::new("default", "test"),
        table_path.to_string(),
        schema,
        None,
    )
}

fn int_batch(ids: Vec<i32>, values: Vec<i32>) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("value", ArrowDataType::Int32, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(Int32Array::from(values)),
        ],
    )
    .unwrap()
}

fn partitioned_batch(pts: Vec<&str>, ids: Vec<i32>) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("pt", ArrowDataType::Utf8, false),
        ArrowField::new("id", ArrowDataType::Int32, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(pts)),
            Arc::new(Int32Array::from(ids)),
        ],
    )
    .unwrap()
}

fn collect_int_col(batches: &[RecordBatch], col: &str) -> Vec<i32> {
    let mut vals: Vec<i32> = batches
        .iter()
        .flat_map(|b| {
            let idx = b.schema().index_of(col).unwrap();
            b.column(idx)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    vals.sort();
    vals
}

fn collect_string_col(batches: &[RecordBatch], col: &str) -> Vec<String> {
    let mut vals: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            let idx = b.schema().index_of(col).unwrap();
            let arr = b
                .column(idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..arr.len())
                .map(|i| arr.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    vals.sort();
    vals
}

/// Write batches → commit → scan → read, return all batches.
async fn write_commit_read(table: &Table, batches: Vec<RecordBatch>) -> Vec<RecordBatch> {
    let wb = table.new_write_builder();
    let mut tw = wb.new_write().unwrap();
    for batch in &batches {
        tw.write_arrow_batch(batch).await.unwrap();
    }
    wb.new_commit()
        .commit(tw.prepare_commit().await.unwrap())
        .await
        .unwrap();

    let rb = table.new_read_builder();
    let plan = rb.new_scan().plan().await.unwrap();
    let read = rb.new_read().unwrap();
    read.to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

// ---------------------------------------------------------------------------
// Unpartitioned, bucket = -1 (default)
// ---------------------------------------------------------------------------

fn unpartitioned_schema() -> TableSchema {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .build()
        .unwrap();
    TableSchema::new(0, &schema)
}

#[tokio::test]
async fn test_unpartitioned_single_batch() {
    let file_io = memory_file_io();
    let path = "memory:/append_unpart_single";
    setup_dirs(&file_io, path).await;
    let table = make_table(&file_io, path, unpartitioned_schema());

    let result = write_commit_read(&table, vec![int_batch(vec![1, 2, 3], vec![10, 20, 30])]).await;

    assert_eq!(collect_int_col(&result, "id"), vec![1, 2, 3]);
    assert_eq!(collect_int_col(&result, "value"), vec![10, 20, 30]);
}

#[tokio::test]
async fn test_unpartitioned_multiple_batches() {
    let file_io = memory_file_io();
    let path = "memory:/append_unpart_multi";
    setup_dirs(&file_io, path).await;
    let table = make_table(&file_io, path, unpartitioned_schema());

    let result = write_commit_read(
        &table,
        vec![
            int_batch(vec![1, 2], vec![10, 20]),
            int_batch(vec![3, 4, 5], vec![30, 40, 50]),
        ],
    )
    .await;

    assert_eq!(collect_int_col(&result, "id"), vec![1, 2, 3, 4, 5]);
}

#[tokio::test]
async fn test_unpartitioned_two_commits() {
    let file_io = memory_file_io();
    let path = "memory:/append_unpart_two_commits";
    setup_dirs(&file_io, path).await;
    let table = make_table(&file_io, path, unpartitioned_schema());

    // First commit
    let wb = table.new_write_builder();
    let mut tw = wb.new_write().unwrap();
    tw.write_arrow_batch(&int_batch(vec![1, 2], vec![10, 20]))
        .await
        .unwrap();
    wb.new_commit()
        .commit(tw.prepare_commit().await.unwrap())
        .await
        .unwrap();

    // Second commit
    let mut tw2 = wb.new_write().unwrap();
    tw2.write_arrow_batch(&int_batch(vec![3, 4], vec![30, 40]))
        .await
        .unwrap();
    wb.new_commit()
        .commit(tw2.prepare_commit().await.unwrap())
        .await
        .unwrap();

    // Read all
    let rb = table.new_read_builder();
    let plan = rb.new_scan().plan().await.unwrap();
    let read = rb.new_read().unwrap();
    let result: Vec<RecordBatch> = read
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    assert_eq!(collect_int_col(&result, "id"), vec![1, 2, 3, 4]);
}

#[tokio::test]
async fn test_unpartitioned_projection() {
    let file_io = memory_file_io();
    let path = "memory:/append_unpart_proj";
    setup_dirs(&file_io, path).await;
    let table = make_table(&file_io, path, unpartitioned_schema());

    // Write
    let wb = table.new_write_builder();
    let mut tw = wb.new_write().unwrap();
    tw.write_arrow_batch(&int_batch(vec![1, 2, 3], vec![10, 20, 30]))
        .await
        .unwrap();
    wb.new_commit()
        .commit(tw.prepare_commit().await.unwrap())
        .await
        .unwrap();

    // Read with projection
    let mut rb = table.new_read_builder();
    rb.with_projection(&["value"]);
    let plan = rb.new_scan().plan().await.unwrap();
    let read = rb.new_read().unwrap();
    let result: Vec<RecordBatch> = read
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    assert_eq!(result[0].schema().fields().len(), 1);
    assert_eq!(result[0].schema().field(0).name(), "value");
    assert_eq!(collect_int_col(&result, "value"), vec![10, 20, 30]);
}

// ---------------------------------------------------------------------------
// Unpartitioned, fixed bucket
// ---------------------------------------------------------------------------

fn fixed_bucket_schema(buckets: i32) -> TableSchema {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .option("bucket", buckets.to_string())
        .option("bucket-key", "id")
        .build()
        .unwrap();
    TableSchema::new(0, &schema)
}

#[tokio::test]
async fn test_fixed_bucket_write_read() {
    let file_io = memory_file_io();
    let path = "memory:/append_fixed_bucket";
    setup_dirs(&file_io, path).await;
    let table = make_table(&file_io, path, fixed_bucket_schema(4));

    let result = write_commit_read(
        &table,
        vec![int_batch(
            vec![1, 2, 3, 4, 5, 6, 7, 8],
            vec![10, 20, 30, 40, 50, 60, 70, 80],
        )],
    )
    .await;

    assert_eq!(collect_int_col(&result, "id"), vec![1, 2, 3, 4, 5, 6, 7, 8]);
}

#[tokio::test]
async fn test_fixed_bucket_scan_filters_by_bucket() {
    use paimon::spec::{Datum, PredicateBuilder};

    let file_io = memory_file_io();
    let path = "memory:/append_bucket_filter";
    setup_dirs(&file_io, path).await;
    let table = make_table(&file_io, path, fixed_bucket_schema(4));

    // Write enough data to spread across buckets
    let wb = table.new_write_builder();
    let mut tw = wb.new_write().unwrap();
    tw.write_arrow_batch(&int_batch(
        vec![1, 2, 3, 4, 5, 6, 7, 8],
        vec![10, 20, 30, 40, 50, 60, 70, 80],
    ))
    .await
    .unwrap();
    wb.new_commit()
        .commit(tw.prepare_commit().await.unwrap())
        .await
        .unwrap();

    // Full scan — should have multiple buckets
    let full_rb = table.new_read_builder();
    let full_plan = full_rb.new_scan().plan().await.unwrap();
    let all_buckets: std::collections::HashSet<i32> =
        full_plan.splits().iter().map(|s| s.bucket()).collect();

    if all_buckets.len() <= 1 {
        // All rows hashed to same bucket — can't test filtering
        return;
    }

    // Filter by id = 1 — should narrow to one bucket
    let pb = PredicateBuilder::new(table.schema().fields());
    let filter = pb.equal("id", Datum::Int(1)).unwrap();

    let mut rb = table.new_read_builder();
    rb.with_filter(filter);
    let plan = rb.new_scan().plan().await.unwrap();
    let filtered_buckets: std::collections::HashSet<i32> =
        plan.splits().iter().map(|s| s.bucket()).collect();

    assert_eq!(
        filtered_buckets.len(),
        1,
        "Bucket predicate should narrow to one bucket, got: {filtered_buckets:?}"
    );
    assert!(filtered_buckets.is_subset(&all_buckets));

    // Read and verify id=1 is in the result
    let read = rb.new_read().unwrap();
    let result: Vec<RecordBatch> = read
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let ids = collect_int_col(&result, "id");
    assert!(ids.contains(&1));
}

// ---------------------------------------------------------------------------
// Partitioned, bucket = -1
// ---------------------------------------------------------------------------

fn partitioned_schema() -> TableSchema {
    let schema = Schema::builder()
        .column("pt", DataType::VarChar(VarCharType::string_type()))
        .column("id", DataType::Int(IntType::new()))
        .partition_keys(["pt"])
        .build()
        .unwrap();
    TableSchema::new(0, &schema)
}

#[tokio::test]
async fn test_partitioned_write_read() {
    let file_io = memory_file_io();
    let path = "memory:/append_partitioned";
    setup_dirs(&file_io, path).await;
    let table = make_table(&file_io, path, partitioned_schema());

    let result = write_commit_read(
        &table,
        vec![partitioned_batch(
            vec!["a", "b", "a", "b"],
            vec![1, 2, 3, 4],
        )],
    )
    .await;

    let total: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 4);
    assert_eq!(collect_int_col(&result, "id"), vec![1, 2, 3, 4]);
    assert_eq!(collect_string_col(&result, "pt"), vec!["a", "a", "b", "b"]);
}

#[tokio::test]
async fn test_partitioned_two_commits() {
    let file_io = memory_file_io();
    let path = "memory:/append_part_two_commits";
    setup_dirs(&file_io, path).await;
    let table = make_table(&file_io, path, partitioned_schema());

    let wb = table.new_write_builder();

    // First commit: partition "a"
    let mut tw1 = wb.new_write().unwrap();
    tw1.write_arrow_batch(&partitioned_batch(vec!["a", "a"], vec![1, 2]))
        .await
        .unwrap();
    wb.new_commit()
        .commit(tw1.prepare_commit().await.unwrap())
        .await
        .unwrap();

    // Second commit: partition "b"
    let mut tw2 = wb.new_write().unwrap();
    tw2.write_arrow_batch(&partitioned_batch(vec!["b", "b"], vec![3, 4]))
        .await
        .unwrap();
    wb.new_commit()
        .commit(tw2.prepare_commit().await.unwrap())
        .await
        .unwrap();

    // Read all
    let rb = table.new_read_builder();
    let plan = rb.new_scan().plan().await.unwrap();
    let read = rb.new_read().unwrap();
    let result: Vec<RecordBatch> = read
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    assert_eq!(collect_int_col(&result, "id"), vec![1, 2, 3, 4]);
    assert_eq!(collect_string_col(&result, "pt"), vec!["a", "a", "b", "b"]);
}

#[tokio::test]
async fn test_partitioned_scan_partition_filter() {
    use paimon::spec::{Datum, PredicateBuilder};

    let file_io = memory_file_io();
    let path = "memory:/append_part_filter";
    setup_dirs(&file_io, path).await;
    let table = make_table(&file_io, path, partitioned_schema());

    // Write data to two partitions
    let wb = table.new_write_builder();
    let mut tw = wb.new_write().unwrap();
    tw.write_arrow_batch(&partitioned_batch(
        vec!["a", "b", "a", "b"],
        vec![1, 2, 3, 4],
    ))
    .await
    .unwrap();
    wb.new_commit()
        .commit(tw.prepare_commit().await.unwrap())
        .await
        .unwrap();

    // Filter by pt = "a"
    let pb = PredicateBuilder::new(table.schema().fields());
    let filter = pb.equal("pt", Datum::String("a".into())).unwrap();

    let mut rb = table.new_read_builder();
    rb.with_filter(filter);
    let plan = rb.new_scan().plan().await.unwrap();

    // Only partition "a" splits should survive
    for split in plan.splits() {
        let pt = split.partition().get_string(0).unwrap().to_string();
        assert_eq!(pt, "a");
    }

    let read = rb.new_read().unwrap();
    let result: Vec<RecordBatch> = read
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    assert_eq!(collect_int_col(&result, "id"), vec![1, 3]);
    assert_eq!(collect_string_col(&result, "pt"), vec!["a", "a"]);
}

// ---------------------------------------------------------------------------
// Partitioned + fixed bucket
// ---------------------------------------------------------------------------

fn partitioned_bucket_schema(buckets: i32) -> TableSchema {
    let schema = Schema::builder()
        .column("pt", DataType::VarChar(VarCharType::string_type()))
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .partition_keys(["pt"])
        .option("bucket", buckets.to_string())
        .option("bucket-key", "id")
        .build()
        .unwrap();
    TableSchema::new(0, &schema)
}

fn partitioned_value_batch(pts: Vec<&str>, ids: Vec<i32>, values: Vec<i32>) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("pt", ArrowDataType::Utf8, false),
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("value", ArrowDataType::Int32, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(pts)),
            Arc::new(Int32Array::from(ids)),
            Arc::new(Int32Array::from(values)),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn test_partitioned_fixed_bucket_write_read() {
    let file_io = memory_file_io();
    let path = "memory:/append_part_bucket";
    setup_dirs(&file_io, path).await;
    let table = make_table(&file_io, path, partitioned_bucket_schema(2));

    let wb = table.new_write_builder();
    let mut tw = wb.new_write().unwrap();
    tw.write_arrow_batch(&partitioned_value_batch(
        vec!["a", "a", "b", "b"],
        vec![1, 2, 3, 4],
        vec![10, 20, 30, 40],
    ))
    .await
    .unwrap();
    wb.new_commit()
        .commit(tw.prepare_commit().await.unwrap())
        .await
        .unwrap();

    let rb = table.new_read_builder();
    let plan = rb.new_scan().plan().await.unwrap();
    let read = rb.new_read().unwrap();
    let result: Vec<RecordBatch> = read
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    assert_eq!(collect_int_col(&result, "id"), vec![1, 2, 3, 4]);
    assert_eq!(collect_int_col(&result, "value"), vec![10, 20, 30, 40]);
}

// ---------------------------------------------------------------------------
// Unsupported: primary key table should be rejected
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_reject_primary_key_table() {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .build()
        .unwrap();
    let table_schema = TableSchema::new(0, &schema);

    let file_io = memory_file_io();
    let path = "memory:/append_reject_pk";
    let table = make_table(&file_io, path, table_schema);

    let result = table.new_write_builder().new_write();
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert!(
        matches!(&err, paimon::Error::Unsupported { message } if message.contains("primary keys")),
        "Expected Unsupported error for PK table, got: {err:?}"
    );
}

#[tokio::test]
async fn test_reject_fixed_bucket_without_bucket_key() {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .option("bucket", "4")
        .build()
        .unwrap();
    let table_schema = TableSchema::new(0, &schema);

    let file_io = memory_file_io();
    let path = "memory:/append_reject_no_bucket_key";
    let table = make_table(&file_io, path, table_schema);

    let result = table.new_write_builder().new_write();
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert!(
        matches!(&err, paimon::Error::Unsupported { message } if message.contains("bucket-key")),
        "Expected Unsupported error for missing bucket-key, got: {err:?}"
    );
}
