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
    make_array, types::Int32Type, Array, ArrayRef, Int32Array, Int8Array, LargeStringArray,
    ListArray, MapArray, RecordBatch, StringArray, StructArray,
};
use arrow_schema::{DataType as ArrowType, Field, Schema as ArrowSchema};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs};
use futures::TryStreamExt;
use paimon::spec::{
    ArrayType, DataField, DataType, IntType, MapType, RowType, Schema, TableSchema, VarCharType,
};
use std::sync::Arc;

fn payload(mode: &str, valid: bool) -> (DataType, ArrayRef) {
    let values = if valid {
        vec![Some(10), Some(42)]
    } else {
        vec![None, Some(42)]
    };
    let (logical, actual): (_, ArrayRef) = if mode == "array" {
        (
            DataType::Array(ArrayType::new(DataType::Int(IntType::with_nullable(false)))),
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
                values.into_iter().map(|value| Some(vec![value])),
            )),
        )
    } else if mode == "row" {
        (
            DataType::Row(RowType::new(vec![DataField::new(
                3,
                "value".into(),
                DataType::Int(IntType::with_nullable(false)),
            )])),
            Arc::new(StructArray::from(vec![(
                Arc::new(Field::new("value", ArrowType::Int32, true)),
                Arc::new(Int32Array::from(values)) as ArrayRef,
            )])),
        )
    } else {
        let entries = StructArray::from(vec![
            (
                Arc::new(Field::new("key", ArrowType::Utf8, false)),
                Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("value", ArrowType::Int32, true)),
                Arc::new(Int32Array::from(values)) as ArrayRef,
            ),
        ]);
        (
            DataType::Map(MapType::new(
                DataType::VarChar(VarCharType::string_type()),
                DataType::Int(IntType::with_nullable(false)),
            )),
            Arc::new(
                MapArray::try_new(
                    Arc::new(Field::new("entries", entries.data_type().clone(), false)),
                    arrow_buffer::OffsetBuffer::new(vec![0, 1, 2].into()),
                    entries,
                    None,
                    false,
                )
                .unwrap(),
            ),
        )
    };
    // The Arrow C Data interface can carry declared NOT NULL children whose
    // values do not satisfy that declaration, as PyArrow permits constructing.
    let expected = paimon::arrow::paimon_type_to_arrow(&logical).unwrap();
    let mut data = actual.to_data();
    if let ArrowType::Map(entries, _) = &expected {
        let entry_data = unsafe {
            data.child_data()[0]
                .clone()
                .into_builder()
                .data_type(entries.data_type().clone())
                .build_unchecked()
        };
        data = unsafe {
            data.into_builder()
                .child_data(vec![entry_data])
                .build_unchecked()
        };
    }
    let data = unsafe { data.into_builder().data_type(expected).build_unchecked() };
    (logical, make_array(data))
}

async fn check_ignored_children(
    mode: &str,
    generated: bool,
    large_string: bool,
    bucket: Option<i32>,
) {
    let (payload_type, payload) = payload(mode, false);
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("op", DataType::VarChar(VarCharType::string_type()))
        .column("payload", payload_type)
        .primary_key(["id"])
        .option("bucket", "1")
        .option("ignore-delete", "true");
    if generated {
        schema = schema.option("rowkind.field", "op");
    } else {
        schema = schema.option("changelog-producer", "input");
    }
    let path = "memory:/filtered-children";
    let (io, table) = memory_table(path, TableSchema::new(0, &schema.build().unwrap()));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    let op: ArrayRef = if large_string {
        Arc::new(LargeStringArray::from(vec!["-D", "+I"]))
    } else {
        Arc::new(StringArray::from(vec!["-D", "+I"]))
    };
    let mut batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
        ("op", op),
        ("payload", payload),
    ])
    .unwrap();
    if !generated {
        let mut fields = batch.schema().fields().to_vec();
        fields.push(Arc::new(Field::new("_VALUE_KIND", ArrowType::Int8, false)));
        let mut columns = batch.columns().to_vec();
        columns.push(Arc::new(Int8Array::from(vec![3, 0])));
        batch = RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).unwrap();
    }
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow(&[batch], bucket).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(
        messages
            .iter()
            .flat_map(|message| &message.new_files)
            .map(|file| file.row_count)
            .sum::<i64>(),
        1
    );
    let files = messages
        .iter()
        .flat_map(|message| message.new_files.iter().chain(&message.new_changelog_files))
        .collect::<Vec<_>>();
    assert_eq!(files.len(), if generated { 1 } else { 2 });
    assert!(files.iter().all(|file| file.row_count == 1
        && file.min_sequence_number == 0
        && file.max_sequence_number == 0));
    builder.new_commit().commit(messages).await.unwrap();
    let read_builder = table.new_read_builder();
    let plan = read_builder.new_scan().plan().await.unwrap();
    let batches: Vec<_> = read_builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.value(0), 2);
}

#[tokio::test]
async fn ignored_nested_nulls_do_not_fail_writes() {
    for mode in ["array", "row", "map"] {
        for generated in [false, true] {
            for large_string in [false, true] {
                for bucket in [None, Some(0)] {
                    check_ignored_children(mode, generated, large_string, bucket).await;
                }
            }
        }
    }
}

#[tokio::test]
async fn surviving_nested_nulls_still_fail_writes() {
    for mode in ["array", "row", "map"] {
        let (logical, payload) = payload(mode, false);
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("op", DataType::VarChar(VarCharType::string_type()))
            .column("payload", logical)
            .primary_key(["id"])
            .option("bucket", "1")
            .option("rowkind.field", "op")
            .option("ignore-delete", "true")
            .build()
            .unwrap();
        let (_, table) = memory_table("memory:/retained-children", TableSchema::new(0, &schema));
        let mut writer = table.new_write_builder().new_write().unwrap();
        let batch = RecordBatch::try_from_iter([
            ("id", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
            (
                "op",
                Arc::new(StringArray::from(vec!["+I", "+I"])) as ArrayRef,
            ),
            ("payload", payload),
        ])
        .unwrap();
        assert!(writer.write_arrow_batch(&batch).await.is_err(), "{mode}");
        assert!(writer.prepare_commit().await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn ignored_rows_keep_java_top_level_nullability_and_schema_checks() {
    for engine in ["deduplicate", "partial-update", "aggregation"] {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("op", DataType::VarChar(VarCharType::string_type()))
            .column("seq", DataType::Int(IntType::with_nullable(false)))
            .column("payload", DataType::Int(IntType::with_nullable(false)))
            .primary_key(["id"])
            .option("bucket", "1")
            .option("merge-engine", engine)
            .option("sequence.field", "seq")
            .option("rowkind.field", "op")
            .option("ignore-delete", "true")
            .build()
            .unwrap();
        let (_, table) = memory_table("memory:/filtered-nullability", TableSchema::new(0, &schema));
        for (kind, id, seq, payload, valid) in [
            ("-D", Some(1), Some(1), None, engine == "deduplicate"),
            ("-U", Some(1), Some(1), None, false),
            ("-D", None, Some(1), Some(42), false),
            ("-D", Some(1), None, Some(42), false),
        ] {
            let batch = RecordBatch::try_from_iter([
                ("id", Arc::new(Int32Array::from(vec![id])) as ArrayRef),
                ("op", Arc::new(StringArray::from(vec![kind])) as ArrayRef),
                ("seq", Arc::new(Int32Array::from(vec![seq])) as ArrayRef),
                (
                    "payload",
                    Arc::new(Int32Array::from(vec![payload])) as ArrayRef,
                ),
            ])
            .unwrap();
            let mut writer = table.new_write_builder().new_write().unwrap();
            let result = writer.write_arrow_batch(&batch).await;
            assert_eq!(
                result.is_ok(),
                valid,
                "{engine} {kind} {id:?} {seq:?}: {result:?}"
            );
            assert!(writer.prepare_commit().await.unwrap().is_empty());
        }
        let wrong = RecordBatch::try_from_iter([
            ("id", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
            ("op", Arc::new(StringArray::from(vec!["-D"])) as ArrayRef),
            ("seq", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
            (
                "payload",
                Arc::new(StringArray::from(vec!["wrong type"])) as ArrayRef,
            ),
        ])
        .unwrap();
        let mut writer = table.new_write_builder().new_write().unwrap();
        assert!(writer.write_arrow_batch(&wrong).await.is_err());
        assert!(writer.prepare_commit().await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn empty_and_filtered_rowkind_inputs_preserve_transport_validation() {
    for generated in [false, true] {
        let mut schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("op", DataType::VarChar(VarCharType::string_type()))
            .primary_key(["id"])
            .option("bucket", "1")
            .option("ignore-delete", "true");
        if generated {
            schema = schema.option("rowkind.field", "op");
        }
        let (_, table) = memory_table(
            "memory:/empty-filtered",
            TableSchema::new(0, &schema.build().unwrap()),
        );
        let mut writer = table.new_write_builder().new_write().unwrap();
        let input = RecordBatch::try_from_iter([
            ("id", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
            (
                "op",
                Arc::new(LargeStringArray::from(vec!["-D"])) as ArrayRef,
            ),
        ])
        .unwrap();
        if generated {
            writer
                .write_arrow_batch(&RecordBatch::new_empty(input.schema()))
                .await
                .unwrap();
            writer.write_arrow_batch(&input).await.unwrap();
        }
        for misplaced in [false, true] {
            let kind: ArrayRef = if misplaced {
                Arc::new(Int8Array::from(vec![3]))
            } else {
                Arc::new(Int32Array::from(vec![3]))
            };
            let mut columns = vec![
                ("id", input.column(0).clone()),
                ("op", input.column(1).clone()),
            ];
            columns.insert(if misplaced { 0 } else { 2 }, ("_VALUE_KIND", kind));
            let wrong = RecordBatch::try_from_iter(columns).unwrap();
            assert!(writer.write_arrow_batch(&wrong).await.is_err());
        }
        assert!(writer.prepare_commit().await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn completely_filtered_batches_do_not_lock_writer_configuration() {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("op", DataType::VarChar(VarCharType::string_type()))
        .option("rowkind.field", "op")
        .option("ignore-delete", "true")
        .build()
        .unwrap();
    let (io, table) = memory_table(
        "memory:/filtered-configuration",
        TableSchema::new(0, &schema),
    );
    setup_dirs(&io, table.location()).await;
    persist_table_schema(&io, table.location(), table.schema()).await;
    let batch = |kind| {
        RecordBatch::try_from_iter([
            ("id", Arc::new(Int32Array::from(vec![2])) as ArrayRef),
            ("op", Arc::new(StringArray::from(vec![kind])) as ArrayRef),
        ])
        .unwrap()
    };
    let mut writer = table.new_write_builder().new_write().unwrap();
    writer.write_arrow_batch(&batch("-D")).await.unwrap();
    writer.with_write_type(vec!["id".into()]).unwrap();
    writer.with_blob_consumer(None).unwrap();
    writer.with_blob_uri_reader_factory(None).unwrap();
    writer.write_arrow_batch(&batch("+I")).await.unwrap();
    assert!(writer.with_write_type(vec!["id".into()]).is_err());
    assert!(writer.with_blob_consumer(None).is_err());
    assert!(writer.with_blob_uri_reader_factory(None).is_err());
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(
        messages
            .iter()
            .flat_map(|message| &message.new_files)
            .map(|file| file.row_count)
            .sum::<i64>(),
        1
    );
    table
        .new_write_builder()
        .new_commit()
        .commit(messages)
        .await
        .unwrap();
    let reader = table.new_read_builder();
    let plan = reader.new_scan().plan().await.unwrap();
    let batches: Vec<_> = reader
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert!(batches
        .iter()
        .all(|batch| batch.column_by_name("op").unwrap().is_null(0)));
}
