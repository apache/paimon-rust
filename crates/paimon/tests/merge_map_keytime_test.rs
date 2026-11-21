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

//! Real-table regressions for `merge_map_with_keytime` review fixes:
//! NaN rejection (FLOAT/DOUBLE) and UTF-8 BinaryString CHAR/VARCHAR order.

#[path = "common/rowkind_helpers.rs"]
mod rowkind_helpers;

use rowkind_helpers::{memory_table, persist_table_schema, setup_dirs, write_batch};
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, Float32Array, Float64Array, Int32Array, MapArray, RecordBatch, StringArray,
    StructArray,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use futures::StreamExt;
use paimon::arrow::paimon_type_to_arrow;
use paimon::spec::{
    DataField, DataType, DoubleType, FloatType, IntType, MapType, RowType, Schema, TableSchema,
    VarCharType,
};
use paimon::table::Table;

fn updates_type(ts: DataType) -> DataType {
    DataType::Map(MapType::new(
        DataType::VarChar(VarCharType::string_type()),
        DataType::Row(RowType::new(vec![
            DataField::new(3, "payload".into(), DataType::Int(IntType::new())),
            DataField::new(4, "ts".into(), ts),
        ])),
    ))
}

async fn keytime_table(path: &str, ts: DataType) -> Table {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("updates", updates_type(ts))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("merge-engine", "aggregation")
        .option(
            "fields.updates.aggregate-function",
            "merge_map_with_keytime",
        )
        .build()
        .unwrap();
    let (file_io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&file_io, path).await;
    persist_table_schema(&file_io, path, table.schema()).await;
    table
}

fn single_entry_batch(table: &Table, payload: i32, ts: ArrayRef) -> RecordBatch {
    let updates_dt = table
        .schema()
        .fields()
        .iter()
        .find(|f| f.name() == "updates")
        .unwrap()
        .data_type();
    let ArrowDataType::Map(entries_field, ordered) = paimon_type_to_arrow(updates_dt).unwrap()
    else {
        unreachable!()
    };
    let ArrowDataType::Struct(entry_fields) = entries_field.data_type() else {
        unreachable!()
    };
    let ArrowDataType::Struct(value_fields) = entry_fields[1].data_type() else {
        unreachable!()
    };
    let values = StructArray::try_new(
        value_fields.clone(),
        vec![Arc::new(Int32Array::from(vec![payload])), ts],
        None,
    )
    .unwrap();
    let entries = StructArray::try_new(
        entry_fields.clone(),
        vec![Arc::new(StringArray::from(vec!["k"])), Arc::new(values)],
        None,
    )
    .unwrap();
    let maps = MapArray::try_new(
        entries_field,
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
        entries,
        None,
        ordered,
    )
    .unwrap();
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("updates", maps.data_type().clone(), true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(maps) as ArrayRef,
        ],
    )
    .unwrap()
}

async fn write_or_merge_err(table: &Table, batch: &RecordBatch) -> Result<(), String> {
    let builder = table.new_write_builder();
    let mut w = builder.new_write().unwrap();
    if let Err(err) = w.write_arrow_batch(batch).await {
        return Err(err.to_string());
    }
    let msgs = match w.prepare_commit().await {
        Ok(msgs) => msgs,
        Err(err) => return Err(err.to_string()),
    };
    if let Err(err) = builder.new_commit().commit(msgs).await {
        return Err(err.to_string());
    }
    // Merge across commits happens on read for aggregation tables.
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let mut stream = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap();
    while let Some(next) = stream.next().await {
        next.map_err(|err| err.to_string())?;
    }
    Ok(())
}

async fn scan_payload(table: &Table) -> i32 {
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let mut stream = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap();
    let batch = stream.next().await.unwrap().unwrap();
    let maps = batch
        .column_by_name("updates")
        .unwrap()
        .as_any()
        .downcast_ref::<MapArray>()
        .unwrap();
    let entries = maps.value(0);
    let values = entries
        .column(1)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    values
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .value(0)
}

async fn assert_nan_rejected_on_table(path: &str, ts: DataType, nan: ArrayRef, zero: ArrayRef) {
    let table = keytime_table(path, ts).await;
    write_batch(&table, &single_entry_batch(&table, 11, zero)).await;
    let err = write_or_merge_err(&table, &single_entry_batch(&table, 22, nan))
        .await
        .expect_err("NaN key-time must be rejected");
    assert!(
        err.contains("rejects NaN") || err.contains("NaN"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn real_table_float_nan_rejected_across_commits() {
    assert_nan_rejected_on_table(
        "memory:/merge_map_keytime/float_nan",
        DataType::Float(FloatType::new()),
        Arc::new(Float32Array::from(vec![f32::NAN])),
        Arc::new(Float32Array::from(vec![0.0f32])),
    )
    .await;
}

#[tokio::test]
async fn real_table_double_nan_rejected_across_commits() {
    assert_nan_rejected_on_table(
        "memory:/merge_map_keytime/double_nan",
        DataType::Double(DoubleType::new()),
        Arc::new(Float64Array::from(vec![f64::NAN])),
        Arc::new(Float64Array::from(vec![0.0f64])),
    )
    .await;
}

#[tokio::test]
async fn real_table_float_finite_flush_groups_agree() {
    for (path, first, second, expected) in [
        (
            "memory:/merge_map_keytime/float_group_a",
            (11, 0.0f32),
            (33, 1.0f32),
            33,
        ),
        (
            "memory:/merge_map_keytime/float_group_b",
            (33, 1.0f32),
            (11, 0.0f32),
            33,
        ),
    ] {
        let table = keytime_table(path, DataType::Float(FloatType::new())).await;
        write_batch(
            &table,
            &single_entry_batch(&table, first.0, Arc::new(Float32Array::from(vec![first.1]))),
        )
        .await;
        write_batch(
            &table,
            &single_entry_batch(
                &table,
                second.0,
                Arc::new(Float32Array::from(vec![second.1])),
            ),
        )
        .await;
        assert_eq!(scan_payload(&table).await, expected, "{path}");
    }
}

#[tokio::test]
async fn real_table_varchar_utf8_e000_then_u10000_keeps_newer() {
    let table = keytime_table(
        "memory:/merge_map_keytime/utf8_order",
        DataType::VarChar(VarCharType::string_type()),
    )
    .await;
    // Reviewer boundary: U+E000 then U+10000 — UTF-8 bytes keep payload 22.
    write_batch(
        &table,
        &single_entry_batch(&table, 11, Arc::new(StringArray::from(vec!["\u{E000}"]))),
    )
    .await;
    write_batch(
        &table,
        &single_entry_batch(&table, 22, Arc::new(StringArray::from(vec!["\u{10000}"]))),
    )
    .await;
    assert_eq!(scan_payload(&table).await, 22);
}
