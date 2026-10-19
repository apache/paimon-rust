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

//! Partial append inputs may omit hash keys when their bucket is supplied.

mod common;

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs};
use futures::TryStreamExt;
use paimon::spec::{DataType, IntType, Schema, TableSchema};
use paimon::table::Table;

async fn table(partitioned: bool, bucket_keys: &str) -> Table {
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("pt", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .option("bucket", "4")
        .option("bucket-key", bucket_keys);
    if partitioned {
        schema = schema.partition_keys(["pt"]);
    }
    let (io, table) = memory_table(
        "memory:/precomputed_subset",
        TableSchema::new(0, &schema.build().unwrap()),
    );
    setup_dirs(&io, table.location()).await;
    persist_table_schema(&io, table.location(), table.schema()).await;
    table
}

fn batch(columns: &[&str], partition: i32, value: i32) -> RecordBatch {
    RecordBatch::try_from_iter(columns.iter().map(|name| {
        let value = match *name {
            "pt" => partition,
            "value" => value,
            _ => unreachable!(),
        };
        (*name, Arc::new(Int32Array::from(vec![value])) as ArrayRef)
    }))
    .unwrap()
}

async fn rows(table: &Table) -> Vec<(Option<i32>, Option<i32>, i32)> {
    let builder = table.new_read_builder();
    let plan = builder.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut result = Vec::new();
    for batch in batches {
        let values = ["id", "pt", "value"].map(|name| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
        });
        for row in 0..batch.num_rows() {
            result.push((
                values[0].is_valid(row).then(|| values[0].value(row)),
                values[1].is_valid(row).then(|| values[1].value(row)),
                values[2].value(row),
            ));
        }
    }
    result.sort();
    result
}

#[tokio::test]
async fn fixed_bucket_subsets_can_omit_hash_columns_for_upstream_assignment() {
    for partitioned in [false, true] {
        let table = table(partitioned, "id").await;
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        let columns = if partitioned {
            vec!["value", "pt"]
        } else {
            vec!["value"]
        };
        writer
            .with_write_type(columns.iter().map(|name| name.to_string()).collect())
            .unwrap();
        let input = batch(&columns, 7, 42);
        // Automatic routing must fail before staging anything, while leaving
        // the same partial writer usable with a bucket supplied upstream.
        let before = table
            .file_io()
            .list_status_recursive(table.location())
            .await
            .unwrap();
        let error = writer.write_arrow_batch(&input).await.unwrap_err();
        assert!(error.to_string().contains("routing column 'id'"));
        assert_eq!(
            table
                .file_io()
                .list_status_recursive(table.location())
                .await
                .unwrap()
                .len(),
            before.len()
        );
        writer
            .write_arrow(std::slice::from_ref(&input), Some(2))
            .await
            .unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].bucket, 2);
        assert_eq!(messages[0].new_files[0].row_count, 1);
        assert_eq!(
            messages[0].new_files[0].write_cols,
            Some(columns.iter().map(|name| name.to_string()).collect())
        );
        builder.new_commit().commit(messages).await.unwrap();
        assert_eq!(
            rows(&table).await,
            vec![(None, partitioned.then_some(7), 42)]
        );
    }
}

#[tokio::test]
async fn partial_upstream_overwrite_keeps_other_partitions_and_omitted_columns() {
    let table = table(true, "id,value").await;
    let builder = table.new_write_builder();
    let mut seed = builder.new_write().unwrap();
    for (id, partition, value) in [(1, 7, 10), (2, 8, 20)] {
        let input = RecordBatch::try_from_iter([
            ("id", Arc::new(Int32Array::from(vec![id])) as ArrayRef),
            (
                "pt",
                Arc::new(Int32Array::from(vec![partition])) as ArrayRef,
            ),
            ("value", Arc::new(Int32Array::from(vec![value])) as ArrayRef),
        ])
        .unwrap();
        seed.write_arrow(std::slice::from_ref(&input), Some(0))
            .await
            .unwrap();
    }
    builder
        .new_commit()
        .commit(seed.prepare_commit().await.unwrap())
        .await
        .unwrap();

    let builder = table.new_write_builder().with_overwrite();
    let mut writer = builder.new_write().unwrap();
    writer
        .with_write_type(vec!["value".into(), "pt".into()])
        .unwrap();
    writer
        .write_arrow(
            std::slice::from_ref(&batch(&["value", "pt"], 7, 42)),
            Some(2),
        )
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(messages[0].bucket, 2);
    builder
        .new_commit()
        .overwrite(
            messages,
            Some(std::collections::HashMap::from([(
                "pt".into(),
                Some(paimon::spec::Datum::Int(7)),
            )])),
        )
        .await
        .unwrap();
    assert_eq!(
        rows(&table).await,
        vec![(None, Some(7), 42), (Some(2), Some(8), 20)]
    );
}

#[tokio::test]
async fn partial_explicit_bucket_groups_validate_before_staging() {
    let table = table(true, "id").await;
    let mut writer = table.new_write_builder().new_write().unwrap();
    writer
        .with_write_type(vec!["value".into(), "pt".into()])
        .unwrap();
    let input = RecordBatch::try_from_iter([
        (
            "value",
            Arc::new(Int32Array::from(vec![42, 43])) as ArrayRef,
        ),
        ("pt", Arc::new(Int32Array::from(vec![7, 8])) as ArrayRef),
    ])
    .unwrap();
    let before = table
        .file_io()
        .list_status_recursive(table.location())
        .await
        .unwrap()
        .len();
    for bucket in [-1, 4] {
        assert!(writer
            .write_arrow(std::slice::from_ref(&input), Some(bucket))
            .await
            .unwrap_err()
            .to_string()
            .contains("Bucket id"));
    }
    assert!(writer
        .write_arrow(std::slice::from_ref(&input), Some(2))
        .await
        .unwrap_err()
        .to_string()
        .contains("multiple partitions"));
    assert_eq!(
        table
            .file_io()
            .list_status_recursive(table.location())
            .await
            .unwrap()
            .len(),
        before
    );
    writer
        .write_arrow(std::slice::from_ref(&input.slice(0, 1)), Some(2))
        .await
        .unwrap();
    table
        .new_write_builder()
        .new_commit()
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();
    assert_eq!(rows(&table).await, vec![(None, Some(7), 42)]);
}

#[tokio::test]
async fn empty_partial_input_does_not_require_automatic_bucket_keys() {
    let table = table(false, "id").await;
    let mut writer = table.new_write_builder().new_write().unwrap();
    writer.with_write_type(vec!["value".into()]).unwrap();
    writer
        .write_arrow_batch(&batch(&["value"], 7, 42).slice(0, 0))
        .await
        .unwrap();
    assert!(writer.prepare_commit().await.unwrap().is_empty());
    // Empty inputs neither lock nor invalidate the selected type.
    writer.with_write_type(vec!["value".into()]).unwrap();
    writer
        .write_arrow(std::slice::from_ref(&batch(&["value"], 7, 42)), Some(2))
        .await
        .unwrap();
    table
        .new_write_builder()
        .new_commit()
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();
    assert_eq!(rows(&table).await, vec![(None, None, 42)]);
}
