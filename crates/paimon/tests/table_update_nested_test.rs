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

mod common;

use std::sync::Arc;

use arrow_array::builder::{Int32Builder, ListBuilder};
use arrow_array::{Array, ArrayRef, Int32Array, ListArray, RecordBatch, StringArray};
use arrow_schema::{DataType as ArrowType, Field};
use futures::TryStreamExt;
use paimon::spec::{ArrayType, DataType, IntType, Schema, TableSchema};
use paimon::table::Table;

use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs, write_batch};

async fn table() -> Table {
    let path = "memory:/nested_upsert";
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            "value",
            DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
        )
        .column("tag", DataType::Int(IntType::new()))
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        .build()
        .unwrap();
    let (io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    table
}

fn batch(ids: Vec<i32>, values: Vec<Option<Vec<Option<i32>>>>, tags: Vec<i32>) -> RecordBatch {
    let mut builder = ListBuilder::new(Int32Builder::new()).with_field(Arc::new(Field::new(
        "item",
        ArrowType::Int32,
        true,
    )));
    for value in values {
        if let Some(values) = value {
            for value in values {
                builder.values().append_option(value);
            }
            builder.append(true);
        } else {
            builder.append(false);
        }
    }
    // The upsert source may reorder top-level fields and use Arrow child aliases.
    RecordBatch::try_from_iter([
        ("tag", Arc::new(Int32Array::from(tags)) as ArrayRef),
        ("value", Arc::new(builder.finish()) as ArrayRef),
        ("id", Arc::new(Int32Array::from(ids)) as ArrayRef),
    ])
    .unwrap()
}

#[tokio::test]
async fn nested_upsert_normalizes_before_matching_and_appending() {
    let table = table().await;
    let seed = batch(vec![1, 1, 2], vec![None, None, None], vec![10, 11, 12]);
    write_batch(&table, &seed.project(&[2, 1, 0]).unwrap()).await;
    let mut update = table.new_write_builder().new_update().unwrap();
    update.with_update_type(vec!["value".into()]).unwrap();
    let source = batch(
        vec![0, 1, 1, 3],
        vec![
            Some(vec![Some(999)]),
            Some(vec![Some(99)]),
            Some(vec![Some(10), None]),
            Some(vec![]),
        ],
        vec![20, 21, 22, 23],
    );
    let messages = update
        .upsert_by_arrow_with_key(
            vec![source.slice(1, 1), source.slice(2, 2)],
            vec!["id".into()],
        )
        .await
        .unwrap();
    table
        .new_write_builder()
        .new_commit()
        .commit(messages)
        .await
        .unwrap();
    let read = table.new_read_builder();
    let plan = read.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = read
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let tags = batch
            .column_by_name("tag")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            let value = (!values.is_null(i)).then(|| {
                values
                    .value(i)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>()
            });
            rows.push((ids.value(i), tags.value(i), value));
        }
    }
    rows.sort();
    assert_eq!(
        rows,
        vec![
            (1, 10, Some(vec![Some(10), None])),
            (1, 11, Some(vec![Some(10), None])),
            (2, 12, None),
            (3, 23, Some(vec![]))
        ]
    );
}

#[tokio::test]
async fn invalid_nested_upsert_batch_creates_no_staged_files() {
    let table = table().await;
    let seed = batch(vec![1], vec![None], vec![10]);
    write_batch(&table, &seed.project(&[2, 1, 0]).unwrap()).await;
    let paths = || async {
        let mut files: Vec<_> = table
            .file_io()
            .list_status_recursive(table.location())
            .await
            .unwrap()
            .into_iter()
            .map(|status| status.path)
            .filter(|path| path.ends_with(".parquet"))
            .collect();
        files.sort();
        files
    };
    let before = paths().await;
    let invalid = RecordBatch::try_from_iter([
        ("tag", Arc::new(Int32Array::from(vec![20])) as ArrayRef),
        (
            "value",
            Arc::new(StringArray::from(vec!["not an array"])) as ArrayRef,
        ),
        ("id", Arc::new(Int32Array::from(vec![2])) as ArrayRef),
    ])
    .unwrap();
    let update = table.new_write_builder().new_update().unwrap();
    assert!(update
        .upsert_by_arrow_with_key(vec![seed.clone(), invalid], vec!["id".into()])
        .await
        .is_err());
    assert_eq!(paths().await, before);
    let messages = update
        .upsert_by_arrow_with_key(vec![seed], vec!["id".into()])
        .await
        .unwrap();
    table
        .new_write_builder()
        .new_commit()
        .commit(messages)
        .await
        .unwrap();
}
