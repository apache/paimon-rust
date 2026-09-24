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

use arrow_array::{ArrayRef, Int32Array, ListArray, RecordBatch, StructArray};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs};
use paimon::spec::{
    ArrayType, DataField, DataType, IntType, PredicateBuilder, RowType, Schema, TableSchema,
};
use std::sync::Arc;

#[tokio::test]
async fn nested_leaf_nulls_do_not_prune_non_null_parent_columns() {
    let path = "memory:/pk_value_stats/nested_leaf_nulls";
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            "payload",
            DataType::Row(RowType::new(vec![DataField::new(
                2,
                "child".to_string(),
                DataType::Int(IntType::new()),
            )])),
        )
        .column(
            "items",
            DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
        )
        .primary_key(["id"])
        .option("bucket", "1")
        .option("deletion-vectors.enabled", "true")
        .option("metadata.stats-mode", "full")
        .build()
        .unwrap();
    let (file_io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&file_io, path).await;
    persist_table_schema(&file_io, path, table.schema()).await;

    let child_field = Arc::new(ArrowField::new("child", ArrowDataType::Int32, true));
    let payload: ArrayRef = Arc::new(StructArray::from(vec![(
        child_field.clone(),
        Arc::new(Int32Array::from(vec![None, None])) as ArrayRef,
    )]));
    let items: ArrayRef = Arc::new(ListArray::new(
        Arc::new(ArrowField::new("element", ArrowDataType::Int32, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 2])),
        Arc::new(Int32Array::from(vec![None, None])),
        None,
    ));
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new(
                "payload",
                ArrowDataType::Struct(vec![child_field].into()),
                true,
            ),
            ArrowField::new("items", items.data_type().clone(), true),
        ])),
        vec![Arc::new(Int32Array::from(vec![1, 2])), payload, items],
    )
    .unwrap();

    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(messages.len(), 1);
    let file = &messages[0].new_files[0];
    assert_eq!(file.value_stats_cols, Some(vec!["id".to_string()]));
    assert_eq!(file.value_stats.null_counts(), &vec![Some(0)]);
    builder.new_commit().commit(messages).await.unwrap();

    let plain = table.new_read_builder();
    assert_eq!(
        plain
            .new_scan()
            .with_scan_all_files()
            .plan()
            .await
            .unwrap()
            .splits()
            .len(),
        1
    );
    for column in ["payload", "items"] {
        let mut filtered = table.new_read_builder();
        filtered.with_filter(
            PredicateBuilder::new(table.schema().fields())
                .is_not_null(column)
                .unwrap(),
        );
        assert_eq!(
            filtered
                .new_scan()
                .with_scan_all_files()
                .plan()
                .await
                .unwrap()
                .splits()
                .len(),
            1,
            "{column} IS NOT NULL must retain the data file",
        );
    }
}
