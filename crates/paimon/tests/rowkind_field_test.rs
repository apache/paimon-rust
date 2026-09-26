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

//! Integration tests for `rowkind.field` (mirrors Java `BatchFileStoreITCase`).

#[path = "common/rowkind_helpers.rs"]
mod rowkind_helpers;

use rowkind_helpers::{
    make_batch_with_rowkind, make_batch_with_rowkind_and_value_kind, memory_table,
    persist_table_schema, rowkind_field_schema, scan_id_values, scan_pk_value_kind, setup_dirs,
    write_batch, write_batch_expect_err,
};
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{Int32Builder, ListBuilder};
use arrow_array::{
    new_null_array, Array, ArrayRef, BinaryArray, Int32Array, Int8Array, ListArray, RecordBatch,
    StringArray, StructArray,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use futures::StreamExt;
use paimon::arrow::paimon_type_to_arrow;
use paimon::spec::{
    ArrayType, DataField, DataType, IntType, RowKind, RowType, Schema, TableSchema, VarBinaryType,
    VarCharType, VALUE_KIND_FIELD_NAME,
};

#[tokio::test]
async fn aggregation_delete_only_stays_invisible_with_sum_and_product() {
    for function in ["sum", "product"] {
        let path = format!("memory:/rowkind_field/delete_only_{function}");
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .primary_key(["id"])
            .option("bucket", "1")
            .option("merge-engine", "aggregation")
            .option("fields.value.aggregate-function", function)
            .build()
            .unwrap();
        let (file_io, table) = memory_table(&path, TableSchema::new(0, &schema));
        setup_dirs(&file_io, &path).await;
        persist_table_schema(&file_io, &path, table.schema()).await;
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                ArrowField::new("id", ArrowDataType::Int32, false),
                ArrowField::new("value", ArrowDataType::Int32, true),
                ArrowField::new(VALUE_KIND_FIELD_NAME, ArrowDataType::Int8, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![Some(20)])),
                Arc::new(Int8Array::from(vec![3])),
            ],
        )
        .unwrap();
        write_batch(&table, &batch).await;
        assert!(scan_id_values(&table).await.is_empty(), "{function}");
    }
}

#[tokio::test]
async fn partial_update_singleton_delete_stays_invisible_without_delete_options() {
    let path = "memory:/rowkind_field/partial_update_delete_only";
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("merge-engine", "partial-update")
        .build()
        .unwrap();
    let (file_io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&file_io, path).await;
    persist_table_schema(&file_io, path, table.schema()).await;
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("value", ArrowDataType::Int32, true),
            ArrowField::new(VALUE_KIND_FIELD_NAME, ArrowDataType::Int8, false),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![Some(20)])),
            Arc::new(Int8Array::from(vec![3])),
        ],
    )
    .unwrap();
    write_batch(&table, &batch).await;
    assert!(scan_id_values(&table).await.is_empty());
}

#[tokio::test]
async fn aggregation_hll_compact_auxiliary_survives_three_commits() {
    let path = "memory:/rowkind_field/hll_three_commits";
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            "value",
            DataType::VarBinary(VarBinaryType::new(65535).unwrap()),
        )
        .primary_key(["id"])
        .option("bucket", "1")
        .option("merge-engine", "aggregation")
        .option("fields.value.aggregate-function", "hll_sketch")
        .build()
        .unwrap();
    let (file_io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&file_io, path).await;
    persist_table_schema(&file_io, path, table.schema()).await;
    let fixture = include_bytes!("../src/table/goldens/hll_java_compact_aux.bin");
    for _ in 0..3 {
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                ArrowField::new("id", ArrowDataType::Int32, false),
                ArrowField::new("value", ArrowDataType::Binary, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(BinaryArray::from(vec![Some(fixture.as_slice())])),
            ],
        )
        .unwrap();
        write_batch(&table, &batch).await;
    }
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let mut stream = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap();
    let batch = stream.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    let value = batch
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap()
        .value(0);
    assert_eq!(value[5] & 8, 8);
    let mut for_rust_reader = value.to_vec();
    for_rust_reader[5] &= !8;
    let estimate = datasketches::hll::HllSketch::deserialize(&for_rust_reader)
        .unwrap()
        .estimate();
    assert!((estimate - 200552.41133627715).abs() < 1e-6);
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn aggregation_product_retracts_singleton_delete_in_later_commit() {
    let path = "memory:/rowkind_field/product_retract_across_commits";
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("merge-engine", "aggregation")
        .option("fields.value.aggregate-function", "product")
        .build()
        .unwrap();
    let (file_io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&file_io, path).await;
    persist_table_schema(&file_io, path, table.schema()).await;

    for (value, kind) in [(100, 0), (20, 3)] {
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                ArrowField::new("id", ArrowDataType::Int32, false),
                ArrowField::new("value", ArrowDataType::Int32, false),
                ArrowField::new(VALUE_KIND_FIELD_NAME, ArrowDataType::Int8, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![value])),
                Arc::new(Int8Array::from(vec![kind])),
            ],
        )
        .unwrap();
        write_batch(&table, &batch).await;
    }
    assert_eq!(scan_id_values(&table).await.get(&1), Some(&5));
}

#[tokio::test]
async fn aggregation_collect_retracts_singleton_delete_in_later_commit() {
    let path = "memory:/rowkind_field/collect_retract_across_commits";
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            "value",
            DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
        )
        .primary_key(["id"])
        .option("bucket", "1")
        .option("merge-engine", "aggregation")
        .option("fields.value.aggregate-function", "collect")
        .build()
        .unwrap();
    let (file_io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&file_io, path).await;
    persist_table_schema(&file_io, path, table.schema()).await;
    for (values, kind) in [(&[1, 2, 2][..], 0), (&[2][..], 3)] {
        let mut list = ListBuilder::new(Int32Builder::new()).with_field(Arc::new(ArrowField::new(
            "element",
            ArrowDataType::Int32,
            true,
        )));
        for &value in values {
            list.values().append_value(value);
        }
        list.append(true);
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                ArrowField::new("id", ArrowDataType::Int32, false),
                ArrowField::new(
                    "value",
                    ArrowDataType::List(Arc::new(ArrowField::new(
                        "element",
                        ArrowDataType::Int32,
                        true,
                    ))),
                    true,
                ),
                ArrowField::new(VALUE_KIND_FIELD_NAME, ArrowDataType::Int8, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(list.finish()),
                Arc::new(Int8Array::from(vec![kind])),
            ],
        )
        .unwrap();
        write_batch(&table, &batch).await;
    }
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let mut stream = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap();
    let batch = stream.next().await.unwrap().unwrap();
    let list = batch
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let values = list.value(0);
    let values = values.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(values.values().as_ref(), &[1, 2]);
    assert!(stream.next().await.is_none());
}

fn partial_update_batch(version: i32, value: Option<i32>, kind: i8) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("version", ArrowDataType::Int32, false),
            ArrowField::new("value", ArrowDataType::Int32, true),
            ArrowField::new(VALUE_KIND_FIELD_NAME, ArrowDataType::Int8, false),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![version])),
            Arc::new(Int32Array::from(vec![value])),
            Arc::new(Int8Array::from(vec![kind])),
        ],
    )
    .unwrap()
}

async fn scan_single_version_value(table: &paimon::table::Table) -> (i32, Option<i32>) {
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let mut stream = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap();
    let batch = stream.next().await.unwrap().unwrap();
    let values = batch
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let versions = batch
        .column_by_name("version")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert!(stream.next().await.is_none());
    (
        versions.value(0),
        (!values.is_null(0)).then(|| values.value(0)),
    )
}

async fn partial_update_table(
    path: &str,
    function: &str,
    whole_row_delete: bool,
    ignore_retract: bool,
) -> paimon::table::Table {
    let mut builder = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("version", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("merge-engine", "partial-update")
        .option("fields.version.sequence-group", "value")
        .option("fields.value.aggregate-function", function);
    if whole_row_delete {
        builder = builder.option("partial-update.remove-record-on-sequence-group", "version");
    }
    if ignore_retract {
        builder = builder.option("fields.value.ignore-retract", "true");
    }
    let schema = builder.build().unwrap();
    let (file_io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&file_io, path).await;
    persist_table_schema(&file_io, path, table.schema()).await;
    table
}

#[tokio::test]
async fn partial_update_sum_retracts_singleton_delete_in_later_commit() {
    let table = partial_update_table(
        "memory:/rowkind_field/partial_sum_retract",
        "sum",
        false,
        false,
    )
    .await;
    write_batch(&table, &partial_update_batch(1, Some(100), 0)).await;
    write_batch(&table, &partial_update_batch(2, Some(20), 3)).await;
    assert_eq!(scan_single_version_value(&table).await, (2, Some(80)));
}

#[tokio::test]
async fn partial_update_first_non_null_keeps_state_after_whole_row_delete() {
    let table = partial_update_table(
        "memory:/rowkind_field/partial_first_non_null_delete",
        "first_non_null_value",
        true,
        false,
    )
    .await;
    write_batch(&table, &partial_update_batch(1, Some(100), 0)).await;
    write_batch(&table, &partial_update_batch(2, None, 3)).await;
    write_batch(&table, &partial_update_batch(3, Some(5), 0)).await;
    assert_eq!(scan_single_version_value(&table).await, (3, None));
}

#[tokio::test]
async fn partial_update_ignore_retract_first_value_preserves_uninitialized_state() {
    let table = partial_update_table(
        "memory:/rowkind_field/ignore_retract_first_value",
        "first_value",
        false,
        true,
    )
    .await;
    write_batch(&table, &partial_update_batch(3, Some(20), 3)).await;
    write_batch(&table, &partial_update_batch(2, Some(5), 0)).await;
    assert_eq!(scan_single_version_value(&table).await, (3, Some(20)));
}

#[tokio::test]
async fn partial_update_ignore_retract_first_non_null_keeps_older_null() {
    let table = partial_update_table(
        "memory:/rowkind_field/ignore_retract_first_non_null",
        "first_non_null_value",
        false,
        true,
    )
    .await;
    write_batch(&table, &partial_update_batch(3, Some(10), 0)).await;
    write_batch(&table, &partial_update_batch(1, None, 0)).await;
    assert_eq!(scan_single_version_value(&table).await, (3, None));
}

#[tokio::test]
async fn partial_update_last_non_null_reverses_over_null_delete_payload() {
    let table = partial_update_table(
        "memory:/rowkind_field/last_non_null_delete",
        "last_non_null_value",
        true,
        false,
    )
    .await;
    write_batch(&table, &partial_update_batch(1, Some(100), 0)).await;
    write_batch(&table, &partial_update_batch(2, None, 3)).await;
    write_batch(&table, &partial_update_batch(1, Some(5), 0)).await;
    assert_eq!(scan_single_version_value(&table).await, (2, Some(5)));
}

#[tokio::test]
async fn partial_update_nested_reverse_keeps_older_rows_beyond_count_limit() {
    let path = "memory:/rowkind_field/nested_reverse_count_limit";
    let items_type = DataType::Array(ArrayType::new(DataType::Row(RowType::new(vec![
        DataField::new(3, "item_id".into(), DataType::Int(IntType::new())),
    ]))));
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("version", DataType::Int(IntType::new()))
        .column("items", items_type.clone())
        .primary_key(["id"])
        .option("bucket", "1")
        .option("merge-engine", "partial-update")
        .option("fields.version.sequence-group", "items")
        .option("fields.items.aggregate-function", "nested_update")
        .option("fields.items.count-limit", "1")
        .build()
        .unwrap();
    let (file_io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&file_io, path).await;
    persist_table_schema(&file_io, path, table.schema()).await;

    let items_arrow_type = paimon_type_to_arrow(&items_type).unwrap();
    let ArrowDataType::List(element) = &items_arrow_type else {
        panic!("expected ARRAY<ROW>");
    };
    let ArrowDataType::Struct(fields) = element.data_type() else {
        panic!("expected ROW element");
    };
    let rows = StructArray::try_new(
        fields.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef],
        None,
    )
    .unwrap();
    let older_items = ListArray::try_new(
        element.clone(),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2])),
        Arc::new(rows),
        None,
    )
    .unwrap();
    let batch_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("version", ArrowDataType::Int32, false),
        ArrowField::new("items", items_arrow_type.clone(), true),
    ]));
    for (version, items) in [
        (3, new_null_array(&items_arrow_type, 1)),
        (1, Arc::new(older_items) as ArrayRef),
    ] {
        let batch = RecordBatch::try_new(
            Arc::clone(&batch_schema),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![version])),
                items,
            ],
        )
        .unwrap();
        write_batch(&table, &batch).await;
    }

    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let mut stream = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap();
    let batch = stream.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    let versions = batch
        .column_by_name("version")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(versions.value(0), 3);
    let items = batch
        .column_by_name("items")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(0);
    let items = items.as_any().downcast_ref::<StructArray>().unwrap();
    let item_ids = items
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(item_ids.values().as_ref(), &[1, 2]);
    assert!(stream.next().await.is_none());
}

fn keyed_items_type() -> DataType {
    DataType::Array(ArrayType::new(DataType::Row(RowType::new(vec![
        DataField::new(3, "item_id".into(), DataType::Int(IntType::new())),
        DataField::new(4, "amount".into(), DataType::Int(IntType::new())),
    ]))))
}

fn nested_items_batch(
    items_type: &DataType,
    version: Option<i32>,
    items: Option<&[(i32, i32)]>,
    kind: i8,
) -> RecordBatch {
    let items_arrow_type = paimon_type_to_arrow(items_type).unwrap();
    let item_array: ArrayRef = if let Some(items) = items {
        let ArrowDataType::List(element) = &items_arrow_type else {
            panic!("expected ARRAY<ROW>");
        };
        let ArrowDataType::Struct(fields) = element.data_type() else {
            panic!("expected ROW element");
        };
        let rows = StructArray::try_new(
            fields.clone(),
            vec![
                Arc::new(Int32Array::from(
                    items.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                )),
                Arc::new(Int32Array::from(
                    items.iter().map(|(_, amount)| *amount).collect::<Vec<_>>(),
                )),
            ],
            None,
        )
        .unwrap();
        Arc::new(
            ListArray::try_new(
                element.clone(),
                OffsetBuffer::new(ScalarBuffer::from(vec![0, items.len() as i32])),
                Arc::new(rows),
                None,
            )
            .unwrap(),
        )
    } else {
        new_null_array(&items_arrow_type, 1)
    };
    let mut fields = vec![ArrowField::new("id", ArrowDataType::Int32, false)];
    let mut columns: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(vec![1]))];
    if let Some(version) = version {
        fields.push(ArrowField::new("version", ArrowDataType::Int32, false));
        columns.push(Arc::new(Int32Array::from(vec![version])));
    }
    fields.push(ArrowField::new("items", items_arrow_type, true));
    columns.push(item_array);
    fields.push(ArrowField::new(
        VALUE_KIND_FIELD_NAME,
        ArrowDataType::Int8,
        false,
    ));
    columns.push(Arc::new(Int8Array::from(vec![kind])));
    RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).unwrap()
}

async fn scan_keyed_items(table: &paimon::table::Table) -> Vec<(i32, i32)> {
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let mut stream = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap();
    let batch = stream.next().await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    let values = batch
        .column_by_name("items")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .value(0);
    let rows = values.as_any().downcast_ref::<StructArray>().unwrap();
    let ids = rows
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let amounts = rows
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let mut result = (0..rows.len())
        .map(|index| (ids.value(index), amounts.value(index)))
        .collect::<Vec<_>>();
    result.sort_unstable();
    assert!(stream.next().await.is_none());
    result
}

#[tokio::test]
async fn partial_update_raw_nested_accumulator_normalizes_on_next_agg_or_retract() {
    let items_type = keyed_items_type();
    for (function, final_kind, expected) in [
        ("nested_update", 0, vec![(1, 20), (2, 30)]),
        ("nested_partial_update", 0, vec![(1, 20), (2, 30)]),
        ("nested_update", 3, vec![(1, 20)]),
    ] {
        let path = format!("memory:/rowkind_field/nested_raw_{function}_{final_kind}");
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("version", DataType::Int(IntType::new()))
            .column("items", items_type.clone())
            .primary_key(["id"])
            .option("bucket", "1")
            .option("merge-engine", "partial-update")
            .option("fields.version.sequence-group", "items")
            .option("fields.items.aggregate-function", function)
            .option("fields.items.nested-key", "item_id")
            .build()
            .unwrap();
        let (file_io, table) = memory_table(&path, TableSchema::new(0, &schema));
        setup_dirs(&file_io, &path).await;
        persist_table_schema(&file_io, &path, table.schema()).await;
        write_batch(&table, &nested_items_batch(&items_type, Some(3), None, 0)).await;
        write_batch(
            &table,
            &nested_items_batch(&items_type, Some(1), Some(&[(1, 10), (1, 20)]), 0),
        )
        .await;
        write_batch(
            &table,
            &nested_items_batch(&items_type, Some(4), Some(&[(2, 30)]), final_kind),
        )
        .await;
        assert_eq!(
            scan_keyed_items(&table).await,
            expected,
            "{function}, {final_kind}"
        );
    }
}

#[tokio::test]
async fn aggregation_whole_row_delete_preserves_nested_values_beyond_count_limit() {
    let path = "memory:/rowkind_field/aggregation_raw_nested_delete";
    let items_type = keyed_items_type();
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("items", items_type.clone())
        .primary_key(["id"])
        .option("bucket", "1")
        .option("merge-engine", "aggregation")
        .option("aggregation.remove-record-on-delete", "true")
        .option("fields.items.aggregate-function", "nested_update")
        .option("fields.items.count-limit", "1")
        .build()
        .unwrap();
    let (file_io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&file_io, path).await;
    persist_table_schema(&file_io, path, table.schema()).await;
    write_batch(&table, &nested_items_batch(&items_type, None, None, 0)).await;
    write_batch(
        &table,
        &nested_items_batch(&items_type, None, Some(&[(1, 10), (2, 20)]), 3),
    )
    .await;
    write_batch(&table, &nested_items_batch(&items_type, None, None, 0)).await;
    assert_eq!(scan_keyed_items(&table).await, vec![(1, 10), (2, 20)]);
}

#[tokio::test]
async fn aggregation_whole_row_delete_preserves_blank_listagg_value() {
    let path = "memory:/rowkind_field/aggregation_raw_listagg_delete";
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::VarChar(VarCharType::string_type()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("merge-engine", "aggregation")
        .option("aggregation.remove-record-on-delete", "true")
        .option("fields.value.aggregate-function", "listagg")
        .build()
        .unwrap();
    let (file_io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&file_io, path).await;
    persist_table_schema(&file_io, path, table.schema()).await;
    for (value, kind) in [(Some("old"), 0), (Some(" "), 3), (None, 0)] {
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                ArrowField::new("id", ArrowDataType::Int32, false),
                ArrowField::new("value", ArrowDataType::Utf8, true),
                ArrowField::new(VALUE_KIND_FIELD_NAME, ArrowDataType::Int8, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec![value])),
                Arc::new(Int8Array::from(vec![kind])),
            ],
        )
        .unwrap();
        write_batch(&table, &batch).await;
    }
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let mut stream = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap();
    let batch = stream.next().await.unwrap().unwrap();
    let values = batch
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(values.value(0), " ");
    assert!(stream.next().await.is_none());
}

fn table_with_options(
    table: &paimon::table::Table,
    options: HashMap<String, String>,
) -> paimon::table::Table {
    table.copy_with_options(options)
}

#[tokio::test]
async fn rowkind_field_insert_then_delete() {
    let table_path = "memory:/rowkind_field/insert_delete";
    let schema = rowkind_field_schema("rf", &[]);
    let (file_io, table) = memory_table(table_path, schema);
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(
        &table,
        &make_batch_with_rowkind(vec![1], vec![1], vec!["+I"], "rf"),
    )
    .await;
    assert_eq!(
        scan_pk_value_kind(&table, "rf").await,
        vec![(1, 1, "+I".to_string())]
    );

    write_batch(
        &table,
        &make_batch_with_rowkind(vec![1], vec![2], vec!["-D"], "rf"),
    )
    .await;
    assert!(scan_id_values(&table).await.is_empty());
}

#[tokio::test]
async fn rowkind_field_update_tokens() {
    let table_path = "memory:/rowkind_field/update_tokens";
    let schema = rowkind_field_schema("rf", &[]);
    let (file_io, table) = memory_table(table_path, schema);
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(
        &table,
        &make_batch_with_rowkind(vec![1], vec![1], vec!["+I"], "rf"),
    )
    .await;
    write_batch(
        &table,
        &make_batch_with_rowkind(vec![1], vec![1], vec!["-U"], "rf"),
    )
    .await;
    assert!(scan_id_values(&table).await.is_empty());

    write_batch(
        &table,
        &make_batch_with_rowkind(vec![1], vec![10], vec!["+U"], "rf"),
    )
    .await;
    assert_eq!(
        scan_pk_value_kind(&table, "rf").await,
        vec![(1, 10, "+U".to_string())]
    );
}

#[tokio::test]
async fn rowkind_field_ignore_delete() {
    let table_path = "memory:/rowkind_field/ignore_delete";
    let schema = rowkind_field_schema("kind", &[("ignore-delete", "true")]);
    let (file_io, table) = memory_table(table_path, schema);
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(
        &table,
        &make_batch_with_rowkind(vec![1], vec![10], vec!["+I"], "kind"),
    )
    .await;
    assert_eq!(
        scan_pk_value_kind(&table, "kind").await,
        vec![(1, 10, "+I".to_string())]
    );

    write_batch(
        &table,
        &make_batch_with_rowkind(vec![1], vec![10], vec!["-D"], "kind"),
    )
    .await;
    assert_eq!(
        scan_pk_value_kind(&table, "kind").await,
        vec![(1, 10, "+I".to_string())]
    );

    write_batch(
        &table,
        &make_batch_with_rowkind(vec![1], vec![20], vec!["+I"], "kind"),
    )
    .await;
    assert_eq!(
        scan_pk_value_kind(&table, "kind").await,
        vec![(1, 20, "+I".to_string())]
    );
}

#[tokio::test]
async fn aggregation_ignore_delete_supports_generated_and_explicit_row_kinds() {
    for explicit_kind in [true, false] {
        for option in [
            "ignore-delete",
            "first-row.ignore-delete",
            "deduplicate.ignore-delete",
            "partial-update.ignore-delete",
        ] {
            let path = format!("memory:/rowkind_field/aggregation_{option}_{explicit_kind}");
            let mut schema = Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .column("kind", DataType::VarChar(VarCharType::string_type()))
                .primary_key(["id"])
                .option("bucket", "1")
                .option("merge-engine", "aggregation")
                .option("fields.value.aggregate-function", "sum")
                .option(option, "true");
            if !explicit_kind {
                schema = schema.option("rowkind.field", "kind");
            }
            let (file_io, table) =
                memory_table(&path, TableSchema::new(0, &schema.build().unwrap()));
            setup_dirs(&file_io, &path).await;
            persist_table_schema(&file_io, &path, table.schema()).await;
            let make_batch = |ids, values, kinds: Vec<&str>| {
                let value_kinds = kinds
                    .iter()
                    .map(|kind| RowKind::from_short_string(kind).unwrap().to_value())
                    .collect::<Vec<_>>();
                let batch = make_batch_with_rowkind(ids, values, kinds, "kind");
                if !explicit_kind {
                    return batch;
                }
                let mut fields = batch.schema().fields().to_vec();
                fields.push(Arc::new(ArrowField::new(
                    VALUE_KIND_FIELD_NAME,
                    ArrowDataType::Int8,
                    false,
                )));
                let mut columns = batch.columns().to_vec();
                columns.push(Arc::new(Int8Array::from(value_kinds)));
                RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).unwrap()
            };

            write_batch(
                &table,
                &make_batch(
                    vec![1, 1, 1, 1, 2],
                    vec![10, 7, 20, 8, 100],
                    vec!["+I", "-D", "+U", "-U", "-D"],
                ),
            )
            .await;
            assert_eq!(
                scan_id_values(&table).await,
                [(1, 30)].into_iter().collect(),
                "{option}, explicit={explicit_kind}"
            );

            // A retract-only commit cannot cancel an earlier sum or add a new key.
            write_batch(
                &table,
                &make_batch(vec![1, 2], vec![30, 100], vec!["-D", "-U"]),
            )
            .await;
            assert_eq!(
                scan_id_values(&table).await,
                [(1, 30)].into_iter().collect(),
                "{option}, explicit={explicit_kind}"
            );

            // The explicit global option takes precedence over a deprecated alias.
            let table = table_with_options(
                &table,
                HashMap::from([("ignore-delete".to_string(), "false".to_string())]),
            );
            write_batch(&table, &make_batch(vec![1], vec![5], vec!["-D"])).await;
            assert_eq!(
                scan_id_values(&table).await,
                [(1, 25)].into_iter().collect(),
                "{option}, explicit={explicit_kind}"
            );
        }
    }
}

#[tokio::test]
async fn rowkind_field_ignore_update_before() {
    let table_path = "memory:/rowkind_field/ignore_update_before";
    let schema = rowkind_field_schema("kind", &[]);
    let (file_io, table) = memory_table(table_path, schema);
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(
        &table,
        &make_batch_with_rowkind(vec![1, 2], vec![10, 20], vec!["+I", "+I"], "kind"),
    )
    .await;
    write_batch(
        &table,
        &make_batch_with_rowkind(vec![2], vec![20], vec!["-U"], "kind"),
    )
    .await;
    assert_eq!(
        scan_pk_value_kind(&table, "kind").await,
        vec![(1, 10, "+I".to_string())]
    );

    let table = table_with_options(
        &table,
        HashMap::from([("ignore-update-before".to_string(), "true".to_string())]),
    );
    assert_eq!(
        scan_pk_value_kind(&table, "kind").await,
        vec![(1, 10, "+I".to_string())],
        "after option change before filtered -U"
    );

    write_batch(
        &table,
        &make_batch_with_rowkind(vec![1], vec![10], vec!["-U"], "kind"),
    )
    .await;
    assert_eq!(
        scan_pk_value_kind(&table, "kind").await,
        vec![(1, 10, "+I".to_string())]
    );

    write_batch(
        &table,
        &make_batch_with_rowkind(vec![1], vec![10], vec!["-D"], "kind"),
    )
    .await;
    assert!(scan_id_values(&table).await.is_empty());
}

#[tokio::test]
async fn rowkind_field_rejects_illegal_token() {
    let table_path = "memory:/rowkind_field/illegal_token";
    let schema = rowkind_field_schema("rf", &[]);
    let (file_io, table) = memory_table(table_path, schema);
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    let err = write_batch_expect_err(
        &table,
        &make_batch_with_rowkind(vec![1], vec![1], vec!["INSERT"], "rf"),
    )
    .await;
    assert!(
        matches!(err, paimon::Error::DataInvalid { ref message, .. }
            if message.contains("Unsupported short string")),
        "got {err:?}"
    );
}

#[tokio::test]
async fn rowkind_field_rejects_value_kind_conflict() {
    let table_path = "memory:/rowkind_field/value_kind_conflict";
    let schema = rowkind_field_schema("rf", &[]);
    let (file_io, table) = memory_table(table_path, schema);
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    let err = write_batch_expect_err(
        &table,
        &make_batch_with_rowkind_and_value_kind(vec![1], vec![1], vec!["+I"], "rf"),
    )
    .await;
    assert!(
        matches!(err, paimon::Error::DataInvalid { ref message, .. }
            if message.contains("_VALUE_KIND")),
        "got {err:?}"
    );
}
