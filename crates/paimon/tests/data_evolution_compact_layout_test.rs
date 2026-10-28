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

use arrow_array::{
    types::Float32Type, Array, ArrayRef, FixedSizeListArray, Float32Array, Int32Array, RecordBatch,
};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs, write_batch};
use futures::TryStreamExt;
use paimon::spec::{DataType, FloatType, IntType, Schema, SchemaChange, TableSchema, VectorType};
use paimon::table::Table;

async fn vector_rows(table: &Table) -> Vec<(i32, Option<Vec<f32>>)> {
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
        let vectors = batch
            .column_by_name("embedding")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let value = (!vectors.is_null(row)).then(|| {
                let vector = vectors.value(row);
                vector
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            });
            rows.push((ids.value(row), value));
        }
    }
    rows.sort_by_key(|row| row.0);
    rows
}

#[tokio::test]
async fn compact_rejects_historical_inline_vectors_after_enabling_dedicated_format() {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            "embedding",
            DataType::Vector(
                VectorType::try_new(true, 2, DataType::Float(FloatType::new())).unwrap(),
            ),
        )
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        .option("compaction.min.file-num", "2")
        .build()
        .unwrap();
    let location = format!("memory:/de_compact_vector_layout/{}", uuid::Uuid::new_v4());
    let (io, original) = memory_table(&location, TableSchema::new(0, &schema));
    setup_dirs(&io, &location).await;
    persist_table_schema(&io, &location, original.schema()).await;
    for id in [1, 2] {
        write_batch(
            &original,
            &RecordBatch::try_from_iter([
                ("id", Arc::new(Int32Array::from(vec![id])) as ArrayRef),
                (
                    "embedding",
                    Arc::new(
                        FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                            vec![Some(vec![Some(id as f32), Some(10.0)])],
                            2,
                        ),
                    ) as ArrayRef,
                ),
            ])
            .unwrap(),
        )
        .await;
    }
    let expected = vec![(1, Some(vec![1.0, 10.0])), (2, Some(vec![2.0, 10.0]))];
    assert_eq!(vector_rows(&original).await, expected);
    let original_plan = original
        .new_compact_builder()
        .new_planner()
        .unwrap()
        .plan()
        .await
        .unwrap();
    let schema = original
        .schema()
        .apply_changes(vec![SchemaChange::SetOption {
            key: "vector.file.format".into(),
            value: "parquet".into(),
        }])
        .unwrap();
    persist_table_schema(&io, &location, &schema).await;
    let evolved = Table::new(io, original.identifier().clone(), location, schema, None);
    assert_eq!(
        vector_rows(&evolved).await,
        expected,
        "the supported reader must still see inline vectors before compaction"
    );
    let builder = evolved.new_compact_builder();
    let error = builder.new_planner().unwrap().plan().await.unwrap_err();
    assert!(matches!(error, paimon::Error::Unsupported { .. }));
    assert!(error
        .to_string()
        .contains("historical inline field 'embedding'"));
    // Workers independently enforce the layout guard when handed a serialized
    // task, even if its captured table schema matches the current options.
    let mut task = serde_json::to_value(&original_plan.tasks()[0]).unwrap();
    task["context"]["schema"] = serde_json::to_value(evolved.schema()).unwrap();
    let task: paimon::table::CompactTask = serde_json::from_value(task).unwrap();
    let error = builder
        .new_executor()
        .unwrap()
        .execute(&task)
        .await
        .unwrap_err();
    assert!(matches!(error, paimon::Error::Unsupported { .. }));
    assert_eq!(
        evolved
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        Some(2)
    );
    assert_eq!(
        vector_rows(&evolved).await,
        expected,
        "rejected compaction must preserve every historical inline vector"
    );
}
