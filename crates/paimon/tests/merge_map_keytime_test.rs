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

//! Public-table regressions for `merge_map_with_keytime` review fixes.

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

fn null_row_batch(table: &Table) -> RecordBatch {
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
        vec![
            Arc::new(Int32Array::from(vec![None])),
            Arc::new(Int32Array::from(vec![None])),
        ],
        Some(vec![false].into()),
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

async fn latest_snapshot_id(table: &Table) -> Option<i64> {
    table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .ok()
        .flatten()
        .map(|snapshot| snapshot.id())
}

async fn write_expect_fail_no_snapshot(table: &Table, batch: &RecordBatch, needle: &str) {
    let before = latest_snapshot_id(table).await;
    let builder = table.new_write_builder();
    let mut w = builder.new_write().unwrap();
    let err = match w.write_arrow_batch(batch).await {
        Err(err) => err,
        Ok(()) => match w.prepare_commit().await {
            Err(err) => err,
            Ok(msgs) => builder
                .new_commit()
                .commit(msgs)
                .await
                .expect_err("commit must fail for rejected keytime write"),
        },
    };
    let message = err.to_string();
    assert!(
        message.contains(needle),
        "expected error containing {needle:?}, got {message}"
    );
    assert_eq!(
        latest_snapshot_id(table).await,
        before,
        "rejected write must not publish a new snapshot"
    );
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

#[tokio::test]
async fn real_table_singleton_float_nan_fails_before_snapshot() {
    let table = keytime_table(
        "memory:/merge_map_keytime/singleton_float_nan",
        DataType::Float(FloatType::new()),
    )
    .await;
    // Singleton PK group: must still reject NaN before publish.
    write_expect_fail_no_snapshot(
        &table,
        &single_entry_batch(&table, 22, Arc::new(Float32Array::from(vec![f32::NAN]))),
        "rejects NaN",
    )
    .await;
}

#[tokio::test]
async fn real_table_singleton_double_nan_fails_before_snapshot() {
    let table = keytime_table(
        "memory:/merge_map_keytime/singleton_double_nan",
        DataType::Double(DoubleType::new()),
    )
    .await;
    write_expect_fail_no_snapshot(
        &table,
        &single_entry_batch(&table, 22, Arc::new(Float64Array::from(vec![f64::NAN]))),
        "rejects NaN",
    )
    .await;
}

#[tokio::test]
async fn real_table_null_row_tombstone_fails_before_snapshot() {
    let table = keytime_table(
        "memory:/merge_map_keytime/null_row_tombstone",
        DataType::Int(IntType::new()),
    )
    .await;
    write_batch(
        &table,
        &single_entry_batch(&table, 11, Arc::new(Int32Array::from(vec![10]))),
    )
    .await;
    let after_first = latest_snapshot_id(&table).await;
    assert!(after_first.is_some());
    write_expect_fail_no_snapshot(&table, &null_row_batch(&table), "rejects NULL ROW").await;
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

#[test]
fn schema_rejects_overwrite_on_equal_ts_with_sequence_field() {
    let err = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("seq", DataType::Int(IntType::new()))
        .column("updates", updates_type(DataType::Int(IntType::new())))
        .primary_key(["id"])
        .option("merge-engine", "aggregation")
        .option("sequence.field", "seq")
        .option(
            "fields.updates.aggregate-function",
            "merge_map_with_keytime",
        )
        .option("fields.updates.overwrite-on-equal-ts", "true")
        .build()
        .unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("overwrite-on-equal-ts=true") && message.contains("sequence.field"),
        "{message}"
    );
}

#[test]
fn schema_rejects_invalid_overwrite_on_equal_ts() {
    let err = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("updates", updates_type(DataType::Int(IntType::new())))
        .primary_key(["id"])
        .option("merge-engine", "aggregation")
        .option(
            "fields.updates.aggregate-function",
            "merge_map_with_keytime",
        )
        .option("fields.updates.overwrite-on-equal-ts", "yes")
        .build()
        .unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("overwrite-on-equal-ts") && message.contains("yes"),
        "{message}"
    );
}

#[test]
fn schema_rejects_default_keytime_overwrite_with_sequence_field() {
    // No per-field aggregate-function: the MAP column uses the table default.
    let err = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("seq", DataType::Int(IntType::new()))
        .column("updates", updates_type(DataType::Int(IntType::new())))
        .primary_key(["id"])
        .option("merge-engine", "aggregation")
        .option("sequence.field", "seq")
        .option(
            "fields.default-aggregate-function",
            "merge_map_with_keytime",
        )
        .option("fields.updates.overwrite-on-equal-ts", "true")
        .build()
        .unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("overwrite-on-equal-ts=true") && message.contains("sequence.field"),
        "{message}"
    );
}
