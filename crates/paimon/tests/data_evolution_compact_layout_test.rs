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

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{
    types::Float32Type, ArrayRef, FixedSizeListArray, Int32Array, Int64Array, LargeBinaryArray,
    RecordBatch,
};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs, write_batch};
use futures::TryStreamExt;
use paimon::spec::{
    BlobDescriptor, BlobType, DataType, FloatType, IntType, Schema, SchemaChange, TableSchema,
    VectorType,
};
use paimon::table::Table;

fn payload_value(payload: &ArrayRef, row: usize) -> ArrayRef {
    assert!(!payload.is_null(row));
    if let Some(vector) = payload.as_any().downcast_ref::<FixedSizeListArray>() {
        vector.value(row)
    } else {
        payload.slice(row, 1)
    }
}

async fn rows(table: &Table) -> BTreeMap<i32, ArrayRef> {
    let table = table.copy_with_options([("blob-as-descriptor".into(), "true".into())].into());
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
    let mut rows = BTreeMap::new();
    for batch in batches {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(rows
                .insert(
                    ids.value(row),
                    payload_value(batch.column_by_name("payload").unwrap(), row)
                )
                .is_none());
        }
    }
    rows
}

#[tokio::test]
async fn compact_rejects_historical_inline_fields_after_enabling_dedicated_format() {
    for blob in [false, true] {
        let data_type = if blob {
            DataType::Blob(BlobType::new())
        } else {
            DataType::Vector(
                VectorType::try_new(true, 2, DataType::Float(FloatType::new())).unwrap(),
            )
        };
        let mut schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("payload", data_type)
            .option("row-tracking.enabled", "true")
            .option("data-evolution.enabled", "true")
            .option("compaction.min.file-num", "2");
        if blob {
            schema = schema.option("blob-descriptor-field", "payload");
        }
        let schema = schema.build().unwrap();
        let location = format!("memory:/de_compact_layout/{}", uuid::Uuid::new_v4());
        let (io, original) = memory_table(&location, TableSchema::new(0, &schema));
        setup_dirs(&io, &location).await;
        persist_table_schema(&io, &location, original.schema()).await;
        let mut expected = BTreeMap::new();
        for id in [1, 2] {
            let payload: ArrayRef = if blob {
                let descriptor =
                    BlobDescriptor::new(format!("memory:/payload/{id}"), 0, 10).serialize();
                Arc::new(LargeBinaryArray::from(vec![Some(descriptor.as_slice())]))
            } else {
                Arc::new(
                    FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                        vec![Some(vec![Some(id as f32), Some(10.0)])],
                        2,
                    ),
                )
            };
            expected.insert(id, payload_value(&payload, 0));
            write_batch(
                &original,
                &RecordBatch::try_from_iter([
                    ("id", Arc::new(Int32Array::from(vec![id])) as ArrayRef),
                    ("payload", payload),
                ])
                .unwrap(),
            )
            .await;
        }
        assert_eq!(rows(&original).await, expected);
        let original_plan = original
            .new_compact_builder()
            .new_planner()
            .unwrap()
            .plan()
            .await
            .unwrap();
        let schema = original
            .schema()
            .apply_changes(vec![if blob {
                SchemaChange::RemoveOption {
                    key: "blob-descriptor-field".into(),
                }
            } else {
                SchemaChange::SetOption {
                    key: "vector.file.format".into(),
                    value: "parquet".into(),
                }
            }])
            .unwrap();
        persist_table_schema(&io, &location, &schema).await;
        let evolved = Table::new(io, original.identifier().clone(), location, schema, None);
        assert_eq!(
            rows(&evolved).await,
            expected,
            "the reader must still see inline values before compaction"
        );
        let builder = evolved.new_compact_builder().with_blob_compaction(blob);
        let error = builder.new_planner().unwrap().plan().await.unwrap_err();
        assert!(matches!(error, paimon::Error::Unsupported { .. }));
        assert!(error
            .to_string()
            .contains("historical inline field 'payload'"));
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
            rows(&evolved).await,
            expected,
            "rejected compaction must preserve every historical inline value"
        );
        if blob {
            let evolved = evolved
                .copy_with_options([("compaction.min.file-num".into(), "100".into())].into());
            for value in [b"new".as_slice(), b"latest".as_slice()] {
                let builder = evolved.new_write_builder();
                let messages = builder
                    .new_update()
                    .unwrap()
                    .update_by_arrow_with_row_id(vec![RecordBatch::try_from_iter([
                        ("_ROW_ID", Arc::new(Int64Array::from(vec![0])) as ArrayRef),
                        (
                            "payload",
                            Arc::new(LargeBinaryArray::from(vec![Some(value)])) as ArrayRef,
                        ),
                    ])
                    .unwrap()])
                    .await
                    .unwrap();
                builder.new_commit().commit(messages).await.unwrap();
            }
            let expected = rows(&evolved).await;
            let builder = evolved.new_compact_builder().with_blob_compaction(true);
            let plan = builder.new_planner().unwrap().plan().await.unwrap();
            assert_eq!(plan.tasks().len(), 1);
            assert_eq!(plan.tasks()[0].files().len(), 2);
            assert!(plan.tasks()[0]
                .files()
                .iter()
                .all(|file| file.file_name.ends_with(".blob")));
            let error = builder
                .new_executor()
                .unwrap()
                .execute(&plan.tasks()[0])
                .await
                .unwrap_err();
            assert!(
                matches!(error, paimon::Error::Unsupported { .. }),
                "{error}"
            );
            assert!(
                error
                    .to_string()
                    .contains("historical inline field 'payload'"),
                "{error}"
            );
            assert_eq!(rows(&evolved).await, expected);
            assert_eq!(
                evolved
                    .snapshot_manager()
                    .get_latest_snapshot_id()
                    .await
                    .unwrap(),
                Some(4)
            );
        }
    }
}
