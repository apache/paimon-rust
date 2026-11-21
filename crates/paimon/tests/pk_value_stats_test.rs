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

#[tokio::test]
async fn floating_file_bounds_follow_java_order() {
    use arrow_array::{Float32Array, Float64Array};
    use paimon::spec::{BinaryRow, DoubleType, FloatType};
    for (values, low, high) in [
        (vec![0.0], 0.0, 0.0),
        (vec![-0.0], -0.0, -0.0),
        (vec![0.0, -0.0], -0.0, 0.0),
        (vec![-0.0, 0.0], -0.0, 0.0),
        (vec![5.0, f64::NAN], 5.0, f64::NAN),
        (vec![f64::NAN, 5.0], 5.0, f64::NAN),
        (vec![-f64::NAN, 5.0], 5.0, f64::NAN),
        (
            vec![f64::NEG_INFINITY, f64::INFINITY, f64::NAN],
            f64::NEG_INFINITY,
            f64::NAN,
        ),
    ] {
        let path = "memory:/pk_value_stats/floating";
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("k", DataType::Double(DoubleType::new()))
            .column("f", DataType::Float(FloatType::new()))
            .primary_key(["k", "id"])
            .option("bucket", "1")
            .option("metadata.stats-mode", "full")
            .build()
            .unwrap();
        let (io, table) = memory_table(path, TableSchema::new(0, &schema));
        setup_dirs(&io, path).await;
        persist_table_schema(&io, path, table.schema()).await;
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        for (id, &v) in values.iter().enumerate() {
            let batch = RecordBatch::try_from_iter([
                (
                    "id",
                    Arc::new(Int32Array::from(vec![id as i32])) as ArrayRef,
                ),
                ("k", Arc::new(Float64Array::from(vec![v]))),
                ("f", Arc::new(Float32Array::from(vec![v as f32]))),
            ])
            .unwrap();
            writer.write_arrow_batch(&batch).await.unwrap();
        }
        let msgs = writer.prepare_commit().await.unwrap();
        builder.new_commit().commit(msgs).await.unwrap();
        let plan = table.new_read_builder().new_scan().plan().await.unwrap();
        let files = plan.splits()[0].data_files();
        assert_eq!(files.len(), 1);
        let file = &files[0];
        let bytes = io
            .new_input(&format!("{path}/bucket-0/{}", file.file_name))
            .unwrap()
            .read()
            .await
            .unwrap();
        let mut physical =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes)
                .unwrap()
                .build()
                .unwrap();
        let batch = physical.next().unwrap().unwrap();
        let keys = batch
            .column_by_name("k")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for (key_stats, value_stats, key, expected, row) in [
            (
                file.key_stats.min_values(),
                file.value_stats.min_values(),
                &file.min_key,
                low,
                0,
            ),
            (
                file.key_stats.max_values(),
                file.value_stats.max_values(),
                &file.max_key,
                high,
                batch.num_rows() - 1,
            ),
        ] {
            let ks = BinaryRow::from_serialized_bytes(key_stats).unwrap();
            let vs = BinaryRow::from_serialized_bytes(value_stats).unwrap();
            for actual in [
                ks.get_double(0).unwrap(),
                vs.get_double(1).unwrap(),
                f64::from(vs.get_float(2).unwrap()),
            ] {
                if expected.is_nan() {
                    assert!(actual.is_nan());
                } else {
                    assert_eq!(actual.to_bits(), expected.to_bits());
                }
            }
            // The composite endpoints refer to the physical run, even when its Arrow
            // ordering of a negative NaN differs from scalar Java statistics order.
            let endpoint = BinaryRow::from_serialized_bytes(key).unwrap();
            assert_eq!(
                endpoint.get_double(0).unwrap().to_bits(),
                keys.value(row).to_bits()
            );
            assert_eq!(endpoint.get_int(1).unwrap(), ids.value(row));
        }
    }
}
