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

use super::managed_blob_reference::{ManagedBlobReference, ManagedBlobReferenceCollector};
use super::table_write::tests::{setup_dirs, test_file_io};
use super::{Table, TableCommit, TableWrite};
use crate::arrow::format::create_format_reader;
use crate::catalog::Identifier;
use crate::io::FileIO;
use crate::spec::{
    bucket_path_under, ArrayType, BigIntType, BlobDescriptor, BlobType, DataField, DataType,
    IntType, MapType, Schema, TableSchema, TinyIntType, VarCharType, SEQUENCE_NUMBER_FIELD_ID,
    SEQUENCE_NUMBER_FIELD_NAME, VALUE_KIND_FIELD_ID, VALUE_KIND_FIELD_NAME,
};
use arrow_array::{
    Array, Int32Array, LargeBinaryArray, ListArray, MapArray, RecordBatch, StringArray, StructArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use futures::TryStreamExt;
use std::sync::Arc;

fn scalar_batch_with_kinds(rows: &[(i32, Option<&[u8]>, i8)]) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("payload", ArrowDataType::LargeBinary, true),
        ArrowField::new(VALUE_KIND_FIELD_NAME, ArrowDataType::Int8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(
                rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(LargeBinaryArray::from(
                rows.iter().map(|(_, value, _)| *value).collect::<Vec<_>>(),
            )),
            Arc::new(arrow_array::Int8Array::from(
                rows.iter().map(|(_, _, kind)| *kind).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn scalar_table(file_io: &FileIO, path: &str, extra_options: &[(&str, &str)]) -> Table {
    let mut builder = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("payload", DataType::Blob(BlobType::new()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("file.format", "parquet");
    for &(key, value) in extra_options {
        builder = builder.option(key, value);
    }
    let schema = builder.build().unwrap();
    Table::new(
        file_io.clone(),
        Identifier::new("default", "managed_blob_test"),
        path.to_string(),
        TableSchema::new(0, &schema),
        None,
    )
}

fn nested_table(file_io: &FileIO, path: &str) -> Table {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            "items",
            DataType::Array(ArrayType::new(DataType::Blob(BlobType::new()))),
        )
        .column(
            "named",
            DataType::Map(MapType::new(
                DataType::VarChar(VarCharType::string_type()),
                DataType::Blob(BlobType::new()),
            )),
        )
        .primary_key(["id"])
        .option("bucket", "1")
        .build()
        .unwrap();
    Table::new(
        file_io.clone(),
        Identifier::new("default", "managed_blob_nested"),
        path.to_string(),
        TableSchema::new(0, &schema),
        None,
    )
}

fn nested_batch(table: &Table) -> RecordBatch {
    let schema = crate::arrow::build_target_arrow_schema(table.schema().fields()).unwrap();
    let ArrowDataType::List(element) = schema.field(1).data_type() else {
        panic!("ARRAY<BLOB> must use Arrow List");
    };
    let array = ListArray::try_new(
        element.clone(),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 3, 4, 4, 5])),
        Arc::new(LargeBinaryArray::from(vec![
            Some(b"alpha".as_slice()),
            None,
            Some(b"beta".as_slice()),
            Some(b"hidden-array-child".as_slice()),
            Some(b"gamma".as_slice()),
        ])),
        Some(NullBuffer::from(vec![true, false, true, true])),
    )
    .unwrap();
    let ArrowDataType::Map(entries_field, ordered) = schema.field(2).data_type() else {
        panic!("MAP<X, BLOB> must use Arrow Map");
    };
    let ArrowDataType::Struct(entry_fields) = entries_field.data_type() else {
        panic!("MAP<X, BLOB> entries must be Struct");
    };
    let entries = StructArray::try_new(
        entry_fields.clone(),
        vec![
            Arc::new(StringArray::from(vec!["one", "two", "hidden", "three"])),
            Arc::new(LargeBinaryArray::from(vec![
                Some(b"first".as_slice()),
                None,
                Some(b"hidden-map-child".as_slice()),
                Some(b"third".as_slice()),
            ])),
        ],
        None,
    )
    .unwrap();
    let map = MapArray::try_new(
        entries_field.clone(),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 3, 3, 4])),
        entries,
        Some(NullBuffer::from(vec![true, false, true, true])),
        *ordered,
    )
    .unwrap();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(array),
            Arc::new(map),
        ],
    )
    .unwrap()
}

fn array_values(array: &ListArray, row: usize) -> Option<Vec<Option<Vec<u8>>>> {
    if array.is_null(row) {
        return None;
    }
    let values = array.value(row);
    let values = values.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
    Some(
        (0..values.len())
            .map(|index| values.is_valid(index).then(|| values.value(index).to_vec()))
            .collect(),
    )
}

fn map_values(array: &MapArray, row: usize) -> Option<Vec<(String, Option<Vec<u8>>)>> {
    if array.is_null(row) {
        return None;
    }
    let entries = array.value(row);
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let values = entries
        .column(1)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    Some(
        (0..entries.len())
            .map(|index| {
                (
                    keys.value(index).to_string(),
                    values.is_valid(index).then(|| values.value(index).to_vec()),
                )
            })
            .collect(),
    )
}

fn scalar_batch(rows: &[(i32, Option<&[u8]>)]) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("payload", ArrowDataType::LargeBinary, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(
                rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(LargeBinaryArray::from(
                rows.iter().map(|(_, value)| *value).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

async fn read_scalar_rows(table: &Table) -> Vec<(i32, Option<Vec<u8>>)> {
    let builder = table.new_read_builder();
    let plan = builder.new_scan().plan().await.unwrap();
    let reader = builder.new_read().unwrap();
    let batches: Vec<RecordBatch> = reader
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut rows = batches
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let payloads = batch
                .column(1)
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap();
            (0..batch.num_rows()).map(|row| {
                (
                    ids.value(row),
                    payloads.is_valid(row).then(|| payloads.value(row).to_vec()),
                )
            })
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|(id, _)| *id);
    rows
}

#[tokio::test]
async fn primary_key_blob_is_externalized_and_read_back() {
    let file_io = test_file_io();
    let path = "memory:/managed_blob_pk_scalar";
    setup_dirs(&file_io, path).await;
    let table = scalar_table(&file_io, path, &[]);

    let mut writer = TableWrite::new(&table, "test-user".to_string()).unwrap();
    writer
        .write_arrow_batch(&scalar_batch(&[
            (2, Some(b"world")),
            (1, Some(b"hello")),
            (3, None),
        ]))
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].new_files.len(), 1);
    let file = &messages[0].new_files[0];
    assert!(file.file_name.ends_with(".parquet"));
    assert_eq!(
        file.extra_files,
        vec![format!("{}.blobref", file.file_name)]
    );
    let bucket_dir = bucket_path_under(path, "", messages[0].bucket);
    let file_path = format!("{bucket_dir}/{}", file.file_name);
    let sidecar = file_io
        .new_input(&format!("{file_path}.blobref"))
        .unwrap()
        .read()
        .await
        .unwrap();
    assert_eq!(&sidecar[..4], &0x50424c52_i32.to_be_bytes());
    assert_eq!(sidecar[4], 1);
    assert_eq!(i32::from_be_bytes(sidecar[5..9].try_into().unwrap()), 1);

    // The physical Parquet column contains descriptors, not the BLOB payload.
    let fields = vec![
        DataField::new(
            SEQUENCE_NUMBER_FIELD_ID,
            SEQUENCE_NUMBER_FIELD_NAME.to_string(),
            DataType::BigInt(BigIntType::new()),
        ),
        DataField::new(
            VALUE_KIND_FIELD_ID,
            VALUE_KIND_FIELD_NAME.to_string(),
            DataType::TinyInt(TinyIntType::new()),
        ),
        table.schema().fields()[0].clone(),
        table.schema().fields()[1].clone(),
    ];
    let format_reader = create_format_reader(&file_path, false, &fields).unwrap();
    let input = file_io.new_input(&file_path).unwrap();
    let batches: Vec<RecordBatch> = format_reader
        .read_batch_stream(
            Box::new(input.reader().await.unwrap()),
            file.file_size as u64,
            &fields,
            None,
            None,
            None,
        )
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let physical = batches[0]
        .column(3)
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    let mut descriptors = Vec::new();
    for row in 0..physical.len() {
        if physical.is_valid(row) {
            let descriptor = BlobDescriptor::deserialize(physical.value(row)).unwrap();
            assert!(descriptor.uri().ends_with(".managed.blob"));
            descriptors.push(descriptor);
        }
    }
    assert_eq!(descriptors.len(), 2);

    TableCommit::new(table.clone(), "test-user".to_string())
        .commit(messages)
        .await
        .unwrap();
    assert_eq!(
        read_scalar_rows(&table).await,
        vec![
            (1, Some(b"hello".to_vec())),
            (2, Some(b"world".to_vec())),
            (3, None),
        ]
    );
}

#[tokio::test]
async fn primary_key_array_and_map_blob_values_round_trip_through_managed_packs() {
    let file_io = test_file_io();
    let path = "memory:/managed_blob_pk_nested";
    setup_dirs(&file_io, path).await;
    let table = nested_table(&file_io, path);
    let mut writer = TableWrite::new(&table, "test-user".to_string()).unwrap();
    writer
        .write_arrow_batch(&nested_batch(&table))
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    let data_file = &messages[0].new_files[0];
    assert_eq!(
        data_file.extra_files,
        vec![format!("{}.blobref", data_file.file_name)]
    );
    let sidecar_path = format!(
        "{}/{}.blobref",
        bucket_path_under(path, "", messages[0].bucket),
        data_file.file_name
    );
    let sidecar = file_io
        .new_input(&sidecar_path)
        .unwrap()
        .read()
        .await
        .unwrap();
    // One pack per managed field, with multiple values inside each pack.
    assert_eq!(i32::from_be_bytes(sidecar[5..9].try_into().unwrap()), 2);
    TableCommit::new(table.clone(), "test-user".to_string())
        .commit(messages)
        .await
        .unwrap();

    let builder = table.new_read_builder();
    let plan = builder.new_scan().plan().await.unwrap();
    let read = builder.new_read().unwrap();
    let batches: Vec<RecordBatch> = read
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let arrays = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let maps = batch.column(2).as_any().downcast_ref::<MapArray>().unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                ids.value(row),
                array_values(arrays, row),
                map_values(maps, row),
            ));
        }
    }
    rows.sort_by_key(|(id, _, _)| *id);
    assert_eq!(
        rows,
        vec![
            (
                1,
                Some(vec![Some(b"alpha".to_vec()), None, Some(b"beta".to_vec())]),
                Some(vec![
                    ("one".to_string(), Some(b"first".to_vec())),
                    ("two".to_string(), None)
                ]),
            ),
            (2, None, None),
            (3, Some(Vec::new()), Some(Vec::new())),
            (
                4,
                Some(vec![Some(b"gamma".to_vec())]),
                Some(vec![("three".to_string(), Some(b"third".to_vec()))]),
            ),
        ]
    );
}

#[tokio::test]
async fn primary_key_blob_updates_and_deletes_follow_merge_order() {
    let file_io = test_file_io();
    let path = "memory:/managed_blob_pk_updates";
    setup_dirs(&file_io, path).await;
    let table = scalar_table(&file_io, path, &[]);

    for batch in [
        scalar_batch(&[(1, Some(b"first")), (2, Some(b"survivor"))]),
        scalar_batch(&[(1, Some(b"second"))]),
    ] {
        let mut writer = TableWrite::new(&table, "test-user".to_string()).unwrap();
        writer.write_arrow_batch(&batch).await.unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        TableCommit::new(table.clone(), "test-user".to_string())
            .commit(messages)
            .await
            .unwrap();
    }
    assert_eq!(
        read_scalar_rows(&table).await,
        vec![
            (1, Some(b"second".to_vec())),
            (2, Some(b"survivor".to_vec()))
        ]
    );

    let mut writer = TableWrite::new(&table, "test-user".to_string()).unwrap();
    writer
        .write_arrow_batch(&scalar_batch_with_kinds(&[(1, Some(b"ignored"), 3)]))
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].new_files.len(), 1);
    let delete_file = &messages[0].new_files[0];
    assert_eq!(delete_file.delete_row_count, Some(1));
    let sidecar_path = format!(
        "{}/{}.blobref",
        bucket_path_under(path, "", messages[0].bucket),
        delete_file.file_name
    );
    let sidecar = file_io
        .new_input(&sidecar_path)
        .unwrap()
        .read()
        .await
        .unwrap();
    assert_eq!(i32::from_be_bytes(sidecar[5..9].try_into().unwrap()), 0);
    TableCommit::new(table.clone(), "test-user".to_string())
        .commit(messages)
        .await
        .unwrap();
    assert_eq!(
        read_scalar_rows(&table).await,
        vec![(2, Some(b"survivor".to_vec()))]
    );
}

#[tokio::test]
async fn primary_key_blob_first_row_keeps_the_earliest_value() {
    let file_io = test_file_io();
    let path = "memory:/managed_blob_pk_first_row";
    setup_dirs(&file_io, path).await;
    let table = scalar_table(&file_io, path, &[("merge-engine", "first-row")]);
    let mut writer = TableWrite::new(&table, "test-user".to_string()).unwrap();
    writer
        .write_arrow_batch(&scalar_batch(&[
            (2, Some(b"other")),
            (1, Some(b"first")),
            (1, Some(b"second")),
        ]))
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(messages[0].new_files[0].row_count, 2);
    TableCommit::new(table.clone(), "test-user".to_string())
        .commit(messages)
        .await
        .unwrap();
    // First-row scans normally skip level-0 files until compaction, like Java.
    // Inspect the just-written files explicitly to test the writer's merge.
    let builder = table.new_read_builder();
    let plan = builder
        .new_scan()
        .with_scan_all_files()
        .plan()
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut rows = batches
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let payloads = batch
                .column(1)
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap();
            (0..batch.num_rows()).map(|row| (ids.value(row), payloads.value(row).to_vec()))
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|(id, _)| *id);
    assert_eq!(rows, vec![(1, b"first".to_vec()), (2, b"other".to_vec())]);
}

#[tokio::test]
async fn primary_key_blob_partial_update_keeps_the_last_non_null_value() {
    let file_io = test_file_io();
    let path = "memory:/managed_blob_pk_partial_update";
    setup_dirs(&file_io, path).await;
    let table = scalar_table(&file_io, path, &[("merge-engine", "partial-update")]);
    let mut writer = TableWrite::new(&table, "test-user".to_string()).unwrap();
    writer
        .write_arrow_batch(&scalar_batch(&[
            (1, Some(b"first")),
            (1, None),
            (2, Some(b"other")),
        ]))
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(messages[0].new_files[0].row_count, 2);
    TableCommit::new(table.clone(), "test-user".to_string())
        .commit(messages)
        .await
        .unwrap();
    assert_eq!(
        read_scalar_rows(&table).await,
        vec![(1, Some(b"first".to_vec())), (2, Some(b"other".to_vec()))]
    );
}

#[tokio::test]
async fn primary_key_blob_projection_and_descriptor_mode() {
    let file_io = test_file_io();
    let path = "memory:/managed_blob_pk_projection";
    setup_dirs(&file_io, path).await;
    let table = scalar_table(&file_io, path, &[]);
    let mut writer = TableWrite::new(&table, "test-user".to_string()).unwrap();
    writer
        .write_arrow_batch(&scalar_batch(&[(1, Some(b"one")), (2, None)]))
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    TableCommit::new(table.clone(), "test-user".to_string())
        .commit(messages)
        .await
        .unwrap();

    let mut projected = table.new_read_builder();
    projected.with_projection(&["payload"]).unwrap();
    let plan = projected.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = projected
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    let values = batches
        .iter()
        .flat_map(|batch| {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap();
            (0..col.len()).map(|row| col.is_valid(row).then(|| col.value(row).to_vec()))
        })
        .collect::<Vec<_>>();
    assert_eq!(values, vec![Some(b"one".to_vec()), None]);

    let descriptor_table = scalar_table(&file_io, path, &[("blob-as-descriptor", "true")]);
    let descriptors = read_scalar_rows(&descriptor_table).await;
    assert_eq!(descriptors[0].0, 1);
    let descriptor = BlobDescriptor::deserialize(descriptors[0].1.as_ref().unwrap()).unwrap();
    assert!(descriptor.uri().ends_with(".managed.blob"));
    assert_eq!(descriptors[1], (2, None));
}

#[tokio::test]
async fn primary_key_blob_rolls_packs_and_records_every_reachable_pack() {
    let file_io = test_file_io();
    let path = "memory:/managed_blob_pk_roll";
    setup_dirs(&file_io, path).await;
    let table = scalar_table(&file_io, path, &[("blob.target-file-size", "1b")]);
    let mut writer = TableWrite::new(&table, "test-user".to_string()).unwrap();
    writer
        .write_arrow_batch(&scalar_batch(&[
            (1, Some(b"a")),
            (2, Some(b"bb")),
            (3, Some(b"ccc")),
        ]))
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    let file = &messages[0].new_files[0];
    let sidecar_path = format!(
        "{}/{}.blobref",
        bucket_path_under(path, "", messages[0].bucket),
        file.file_name
    );
    let sidecar = file_io
        .new_input(&sidecar_path)
        .unwrap()
        .read()
        .await
        .unwrap();
    assert_eq!(i32::from_be_bytes(sidecar[5..9].try_into().unwrap()), 3);
    let managed_packs = file_io
        .list_status(&format!("{}/", bucket_path_under(path, "", 0)))
        .await
        .unwrap()
        .into_iter()
        .filter(|entry| entry.path.ends_with(".managed.blob"))
        .collect::<Vec<_>>();
    assert_eq!(managed_packs.len(), 3);
    TableCommit::new(table.clone(), "test-user".to_string())
        .commit(messages)
        .await
        .unwrap();
    assert_eq!(
        read_scalar_rows(&table).await,
        vec![
            (1, Some(b"a".to_vec())),
            (2, Some(b"bb".to_vec())),
            (3, Some(b"ccc".to_vec())),
        ]
    );
}

#[tokio::test]
async fn primary_key_blob_copies_input_descriptor_into_managed_pack() {
    let file_io = test_file_io();
    let path = "memory:/managed_blob_pk_descriptor_copy";
    setup_dirs(&file_io, path).await;
    let source = "memory:/managed_blob_source.bin";
    file_io
        .new_output(source)
        .unwrap()
        .write(bytes::Bytes::from_static(b"prefixPAYLOADsuffix"))
        .await
        .unwrap();
    let input = BlobDescriptor::new(source.to_string(), 6, 7).serialize();
    let table = scalar_table(&file_io, path, &[]);
    let mut writer = TableWrite::new(&table, "test-user".to_string()).unwrap();
    writer
        .write_arrow_batch(&scalar_batch(&[(1, Some(&input))]))
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    TableCommit::new(table.clone(), "test-user".to_string())
        .commit(messages)
        .await
        .unwrap();
    // Removing the source proves the table now refers to its managed copy.
    file_io.delete_file(source).await.unwrap();
    assert_eq!(
        read_scalar_rows(&table).await,
        vec![(1, Some(b"PAYLOAD".to_vec()))]
    );
}

#[tokio::test]
async fn closing_uncommitted_primary_key_writer_removes_managed_packs() {
    let file_io = test_file_io();
    let path = "memory:/managed_blob_pk_abort";
    setup_dirs(&file_io, path).await;
    let table = scalar_table(&file_io, path, &[("blob.target-file-size", "1b")]);
    let mut writer = TableWrite::new(&table, "test-user".to_string()).unwrap();
    writer
        .write_arrow_batch(&scalar_batch(&[(1, Some(b"a")), (2, Some(b"b"))]))
        .await
        .unwrap();
    writer.close().await;
    let files = file_io
        .list_status(&format!("{}/", bucket_path_under(path, "", 0)))
        .await
        .unwrap();
    assert!(files
        .iter()
        .all(|entry| !entry.path.ends_with(".managed.blob")));
}

#[test]
fn blobref_serialization_matches_java_modified_utf_and_checksum() {
    let mut refs = ManagedBlobReferenceCollector::default();
    refs.references_for_test([
        ManagedBlobReference {
            storage_root: "memory:/é/😀".to_string(),
            file_name: "a.managed.blob".to_string(),
        },
        ManagedBlobReference {
            storage_root: "memory:/a".to_string(),
            file_name: "z.managed.blob".to_string(),
        },
    ]);
    let bytes = refs.serialize().unwrap();
    assert_eq!(&bytes[..4], &0x50424c52_i32.to_be_bytes());
    assert_eq!(bytes[4], 1);
    assert_eq!(i32::from_be_bytes(bytes[5..9].try_into().unwrap()), 2);
    let payload_end = bytes.len() - 4;
    assert_eq!(
        &bytes[payload_end..],
        &crc32fast::hash(&bytes[4..payload_end]).to_be_bytes()
    );
    // Java's writeUTF encodes supplementary code points as two surrogate units.
    assert!(bytes
        .windows(6)
        .any(|window| window == [0xed, 0xa0, 0xbd, 0xed, 0xb8, 0x80]));
}
