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

use arrow_array::{
    Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, Int32Array, ListArray, MapArray,
    RecordBatch, StringArray, StructArray,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs};
use futures::TryStreamExt;
use paimon::spec::{
    ArrayType, BinaryType, DataField, DataType, IntType, MapType, RowType, Schema, TableSchema,
    VarCharType,
};
use std::sync::Arc;

fn input_batch() -> RecordBatch {
    // PyArrow names list children `item`; the Paimon table schema names them
    // `element`. The nested ROW exercises the same normalization recursively.
    let item = Arc::new(ArrowField::new("item", ArrowDataType::Int32, true));
    let item_type = ArrowDataType::List(item.clone());
    let ids: ArrayRef = Arc::new(Int32Array::from(vec![2, 1]));
    let items: ArrayRef = Arc::new(ListArray::new(
        item.clone(),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 3])),
        Arc::new(Int32Array::from(vec![20, 21, 10])),
        None,
    ));
    let tags: ArrayRef = Arc::new(ListArray::new(
        item,
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 3])),
        Arc::new(Int32Array::from(vec![200, 100, 101])),
        None,
    ));
    let tags_field = Arc::new(ArrowField::new("tags", item_type.clone(), true));
    let payload: ArrayRef = Arc::new(StructArray::from(vec![(tags_field.clone(), tags)]));
    let raw: ArrayRef = Arc::new(
        FixedSizeBinaryArray::try_from_iter([b"two!".as_slice(), b"one!".as_slice()].into_iter())
            .unwrap(),
    );
    let map_values: ArrayRef = Arc::new(ListArray::new(
        Arc::new(ArrowField::new("item", ArrowDataType::Int32, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 3])),
        Arc::new(Int32Array::from(vec![200, 100, 101])),
        None,
    ));
    let map_entries = StructArray::try_new(
        vec![
            Arc::new(ArrowField::new("key", ArrowDataType::Utf8, false)),
            Arc::new(ArrowField::new("value", item_type.clone(), true)),
        ]
        .into(),
        vec![Arc::new(StringArray::from(vec!["two", "one"])), map_values],
        None,
    )
    .unwrap();
    let map_entries_field = Arc::new(ArrowField::new(
        "entries",
        map_entries.data_type().clone(),
        false,
    ));
    let attributes: ArrayRef = Arc::new(
        MapArray::try_new(
            map_entries_field.clone(),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 2])),
            map_entries,
            None,
            false,
        )
        .unwrap(),
    );

    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, true),
            ArrowField::new("items", item_type, true),
            ArrowField::new(
                "payload",
                ArrowDataType::Struct(vec![tags_field].into()),
                true,
            ),
            ArrowField::new("raw", ArrowDataType::FixedSizeBinary(4), true),
            ArrowField::new(
                "attributes",
                ArrowDataType::Map(map_entries_field, false),
                true,
            ),
        ])),
        vec![ids, items, payload, raw, attributes],
    )
    .unwrap()
}

async fn check_nested_write(primary_key: bool) {
    let path = if primary_key {
        "memory:/nested_native_compat/pk"
    } else {
        "memory:/nested_native_compat/append"
    };
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            "items",
            DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
        )
        .column(
            "payload",
            DataType::Row(RowType::new(vec![DataField::new(
                3,
                "tags".into(),
                DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
            )])),
        )
        .column("raw", DataType::Binary(BinaryType::new(4).unwrap()))
        .column(
            "attributes",
            DataType::Map(MapType::new(
                DataType::VarChar(VarCharType::string_type()),
                DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
            )),
        )
        .option("bucket", "1");
    if primary_key {
        schema = schema.primary_key(["id"]);
    } else {
        schema = schema.option("bucket-key", "id");
    }
    let (io, table) = memory_table(path, TableSchema::new(0, &schema.build().unwrap()));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;

    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(&input_batch()).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    builder.new_commit().commit(messages).await.unwrap();

    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let items = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let payload = batch
            .column(2)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let raw = batch
            .column(3)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let attributes = batch.column(4).as_any().downcast_ref::<MapArray>().unwrap();
        let attribute_values = attributes
            .entries()
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let ArrowDataType::List(attribute_item) = attribute_values.data_type() else {
            unreachable!()
        };
        assert_eq!(attribute_item.name(), "element");
        let ArrowDataType::List(item_field) = items.data_type() else {
            unreachable!()
        };
        assert_eq!(item_field.name(), "element");
        let tags = payload
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let ArrowDataType::List(tag_field) = tags.data_type() else {
            unreachable!()
        };
        assert_eq!(tag_field.name(), "element");
        for row in 0..batch.num_rows() {
            match ids.value(row) {
                1 => {
                    assert_eq!(items.value(row).len(), 1);
                    assert_eq!(tags.value(row).len(), 2);
                    assert_eq!(raw.value(row), b"one!");
                    assert_eq!(attributes.value_length(row), 1);
                }
                2 => {
                    assert_eq!(items.value(row).len(), 2);
                    assert_eq!(tags.value(row).len(), 1);
                    assert_eq!(raw.value(row), b"two!");
                    assert_eq!(attributes.value_length(row), 1);
                }
                id => panic!("unexpected id {id}"),
            }
        }
    }
}

#[tokio::test]
async fn append_accepts_pyarrow_list_alias_and_fixed_binary() {
    check_nested_write(false).await;
}

#[tokio::test]
async fn primary_key_accepts_pyarrow_list_alias_and_fixed_binary() {
    check_nested_write(true).await;
}
