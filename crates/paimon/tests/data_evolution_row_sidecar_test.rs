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

use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs};
use futures::TryStreamExt;
use paimon::io::FileIO;
use paimon::spec::{DataType, IntType, Schema, TableSchema, VarCharType};
use paimon::table::{CommitMessage, RowRange, Table};
use std::collections::HashMap;
use std::sync::Arc;

#[tokio::test]
async fn append_registers_aligned_row_sidecars_in_data_file_metadata() {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::VarChar(VarCharType::string_type()))
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        .option("data-evolution.row-sidecar.enabled", "true")
        .option("target-file-row-num", "2")
        .build()
        .unwrap();
    let (io, table) = memory_table("memory:/de_row_sidecar", TableSchema::new(0, &schema));
    setup_dirs(&io, table.location()).await;
    persist_table_schema(&io, table.location(), table.schema()).await;
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    for ids in [vec![1, 2], vec![3]] {
        let values: Vec<String> = ids.iter().map(|id| format!("value-{id}")).collect();
        writer
            .write_arrow_batch(
                &RecordBatch::try_from_iter([
                    (
                        "id",
                        Arc::new(Int32Array::from(ids)) as arrow_array::ArrayRef,
                    ),
                    (
                        "value",
                        Arc::new(StringArray::from(values)) as arrow_array::ArrayRef,
                    ),
                ])
                .unwrap(),
            )
            .await
            .unwrap();
    }
    let messages = writer.prepare_commit().await.unwrap();
    let files: Vec<_> = messages
        .iter()
        .flat_map(|message| &message.new_files)
        .collect();
    assert_eq!(files.len(), 2);
    for file in &files {
        assert_eq!(file.extra_files, vec![format!("{}.row", file.file_name)]);
        for path in file.collect_files(&format!("{}/bucket-0", table.location())) {
            assert!(io.exists(&path).await.unwrap(), "{path}");
        }
    }
    builder.new_commit().commit(messages).await.unwrap();
    assert_eq!(
        table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap()
            .next_row_id(),
        Some(3)
    );
}

async fn table(options: &[(&str, &str)]) -> Table {
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::VarChar(VarCharType::string_type()))
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        .option("deletion-vectors.enabled", "true")
        .option("data-evolution.row-sidecar.enabled", "true");
    for (name, value) in options {
        schema = schema.option(*name, *value);
    }
    let schema = TableSchema::new(0, &schema.build().unwrap());
    let (io, table) = memory_table("memory:/row_sidecar", schema);
    setup_dirs(&io, table.location()).await;
    persist_table_schema(&io, table.location(), table.schema()).await;
    table
}

fn batch(start: i32, count: i32) -> RecordBatch {
    RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int32Array::from_iter_values(start..start + count)) as ArrayRef,
        ),
        (
            "value",
            Arc::new(StringArray::from_iter_values(
                (start..start + count).map(|id| format!("v{id}")),
            )) as ArrayRef,
        ),
    ])
    .unwrap()
}

async fn append(table: &Table, start: i32, count: i32) -> Vec<CommitMessage> {
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer
        .write_arrow_batch(&batch(start, count))
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    // Closing a producer after prepare must retain both handed-off outputs.
    writer.close().await;
    builder.new_commit().commit(messages.clone()).await.unwrap();
    messages
}

async fn read(table: &Table, ranges: Option<Vec<RowRange>>) -> paimon::Result<Vec<RecordBatch>> {
    let mut builder = table.new_read_builder();
    builder.with_projection(&["id", "value", "_ROW_ID"])?;
    if let Some(ranges) = ranges {
        builder.with_row_ranges(ranges);
    }
    let plan = builder.new_scan().plan().await?;
    builder
        .new_read()?
        .to_arrow(plan.splits())?
        .try_collect()
        .await
}

fn rows(batches: &[RecordBatch]) -> Vec<(i32, String, i64)> {
    batches
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let row_ids = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..batch.num_rows())
                .map(|row| (ids.value(row), values.value(row).into(), row_ids.value(row)))
                .collect::<Vec<_>>()
        })
        .collect()
}

async fn remove_primary(io: &FileIO, table: &Table, messages: &[CommitMessage]) {
    for file in messages.iter().flat_map(|message| &message.new_files) {
        io.delete_file(&file.data_file_path(&format!("{}/bucket-0", table.location())))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn sparse_ranges_use_sidecar_with_nonzero_ids_external_paths_and_read_option_disabled() {
    for external in [false, true] {
        let options = if external {
            vec![("data-file.external-paths", "memory:/external%20sidecars")]
        } else {
            vec![]
        };
        let table = table(&options).await;
        append(&table, -20, 20).await;
        let messages = append(&table, 0, 100).await;
        remove_primary(table.file_io(), &table, &messages).await;
        let read_table = table.copy_with_options(HashMap::from([(
            "data-evolution.row-sidecar.enabled".into(),
            "false".into(),
        )]));
        let actual = read(
            &read_table,
            Some(vec![
                RowRange::new(25, 26),
                RowRange::new(26, 27),
                RowRange::new(150, 200),
            ]),
        )
        .await
        .unwrap();
        assert_eq!(
            rows(&actual),
            vec![
                (5, "v5".into(), 25),
                (6, "v6".into(), 26),
                (7, "v7".into(), 27)
            ]
        );
    }
}

#[tokio::test]
async fn selection_thresholds_are_inclusive_but_full_reads_keep_primary() {
    for (count, max_rows, max_ratio, expected_sidecar) in [
        (5, "5", "0.05", true),
        (6, "4096", "0.05", false),
        (5, "4", "0.05", false),
        (6, "6", "0.06", true),
        (100, "4096", "1", false),
    ] {
        let table = table(&[
            ("data-evolution.row-sidecar.max-selected-rows", max_rows),
            ("data-evolution.row-sidecar.max-selection-ratio", max_ratio),
        ])
        .await;
        let messages = append(&table, 0, 100).await;
        remove_primary(table.file_io(), &table, &messages).await;
        let result = read(&table, Some(vec![RowRange::new(0, count - 1)])).await;
        if expected_sidecar {
            assert_eq!(rows(&result.unwrap()).len(), count as usize);
        } else {
            assert!(
                result.is_err(),
                "count={count}, max rows={max_rows}, max ratio={max_ratio}"
            );
        }
        assert!(read(&table, None).await.is_err());
    }
}

#[tokio::test]
async fn missing_sidecar_is_an_error_for_selected_reads() {
    let table = table(&[]).await;
    let messages = append(&table, 0, 100).await;
    let file = &messages[0].new_files[0];
    let sidecar = file.aligned_file_path(
        &format!("{}/bucket-0", table.location()),
        &file.extra_files[0],
    );
    table.file_io().delete_file(&sidecar).await.unwrap();
    let error = read(&table, Some(vec![RowRange::new(5, 5)]))
        .await
        .unwrap_err();
    assert!(error.to_string().contains(".row"), "{error}");
    // Whole-file reads never require the auxiliary file.
    assert_eq!(rows(&read(&table, None).await.unwrap()).len(), 100);
}

#[tokio::test]
async fn corrupt_sidecar_is_an_error_for_selected_reads() {
    let table = table(&[]).await;
    let messages = append(&table, 0, 100).await;
    let file = &messages[0].new_files[0];
    let path = file.aligned_file_path(
        &format!("{}/bucket-0", table.location()),
        &file.extra_files[0],
    );
    table
        .file_io()
        .new_output(&path)
        .unwrap()
        .write(bytes::Bytes::from_static(b"corrupt"))
        .await
        .unwrap();
    assert!(read(&table, Some(vec![RowRange::new(5, 5)])).await.is_err());
}

#[tokio::test]
async fn updated_columns_and_deletion_vectors_are_applied_when_reading_sidecars() {
    for incremental in [false, true] {
        let table = table(&[]).await;
        let initial = append(&table, 0, 100).await;
        let input = RecordBatch::try_from_iter([
            (
                "_ROW_ID",
                Arc::new(Int64Array::from(vec![5, 7])) as ArrayRef,
            ),
            (
                "value",
                Arc::new(StringArray::from(vec!["changed-5", "changed-7"])) as ArrayRef,
            ),
        ])
        .unwrap();
        let update = table.new_write_builder().new_update().unwrap();
        let messages = if incremental {
            update
                .new_update_by_row_id()
                .await
                .unwrap()
                .update_columns(vec![input], vec!["value".into()])
                .await
                .unwrap()
        } else {
            update
                .update_by_arrow_with_row_id(vec![input])
                .await
                .unwrap()
        };
        assert!(messages
            .iter()
            .flat_map(|m| &m.new_files)
            .all(|file| file.extra_files == vec![format!("{}.row", file.file_name)]));
        table
            .new_write_builder()
            .new_commit()
            .commit(messages.clone())
            .await
            .unwrap();
        let deletions = update.delete_by_row_id(vec![6]).await.unwrap();
        table
            .new_write_builder()
            .new_commit()
            .commit(deletions)
            .await
            .unwrap();
        remove_primary(table.file_io(), &table, &initial).await;
        remove_primary(table.file_io(), &table, &messages).await;
        assert_eq!(
            rows(&read(&table, Some(vec![RowRange::new(5, 7)])).await.unwrap()),
            vec![(5, "changed-5".into(), 5), (7, "changed-7".into(), 7)]
        );
    }
}

#[tokio::test]
async fn explicit_abort_deletes_sidecar_while_writer_close_keeps_prepared_files() {
    let table = table(&[]).await;
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(&batch(0, 5)).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    let paths = messages[0].new_files[0].collect_files(&format!("{}/bucket-0", table.location()));
    assert_eq!(paths.len(), 2);
    writer.close().await;
    for path in &paths {
        assert!(table.file_io().exists(path).await.unwrap());
    }
    builder.new_commit().abort(&messages).await.unwrap();
    for path in &paths {
        assert!(!table.file_io().exists(path).await.unwrap());
    }
    // A producer close before prepare owns and removes all its current files.
    writer.write_arrow_batch(&batch(5, 5)).await.unwrap();
    writer.close().await;
    assert!(table
        .file_io()
        .list_status_recursive(&format!("{}/bucket-0", table.location()))
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn file_index_and_limit_do_not_turn_full_reads_into_sidecar_reads() {
    use paimon::spec::{Datum, PredicateBuilder};
    let table = table(&[("file-index.bitmap.columns", "id")]).await;
    let messages = append(&table, 0, 100).await;
    for file in messages.iter().flat_map(|m| &m.new_files) {
        let sidecar = file
            .extra_files
            .iter()
            .find(|name| name.ends_with(".row"))
            .unwrap();
        table
            .file_io()
            .delete_file(
                &file.aligned_file_path(&format!("{}/bucket-0", table.location()), sidecar),
            )
            .await
            .unwrap();
    }
    // A one-row bitmap selection must still decode the existing primary file.
    for requested_ranges in [None, Some(vec![RowRange::new(0, 99)])] {
        let mut builder = table.new_read_builder();
        builder.with_filter(
            PredicateBuilder::new(table.schema().fields())
                .equal("id", Datum::Int(5))
                .unwrap(),
        );
        if let Some(ranges) = requested_ranges {
            builder.with_row_ranges(ranges);
        }
        let plan = builder.new_scan().plan().await.unwrap();
        let actual: Vec<RecordBatch> = builder
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(actual.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    }
    let mut builder = table.new_read_builder();
    builder.with_limit(1);
    let plan = builder.new_scan().plan().await.unwrap();
    let actual: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(actual.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let update = table.new_write_builder().new_update().unwrap();
    let input = RecordBatch::try_from_iter([
        ("_ROW_ID", Arc::new(Int64Array::from(vec![0])) as ArrayRef),
        (
            "value",
            Arc::new(StringArray::from(vec!["changed"])) as ArrayRef,
        ),
    ])
    .unwrap();
    let messages = update
        .update_by_arrow_with_row_id(vec![input])
        .await
        .unwrap();
    table
        .new_write_builder()
        .new_commit()
        .commit(messages.clone())
        .await
        .unwrap();
    for file in messages.iter().flat_map(|m| &m.new_files) {
        let name = file
            .extra_files
            .iter()
            .find(|name| name.ends_with(".row"))
            .unwrap();
        table
            .file_io()
            .delete_file(&file.aligned_file_path(&format!("{}/bucket-0", table.location()), name))
            .await
            .unwrap();
    }
    let mut builder = table.new_read_builder();
    builder.with_limit(1);
    let plan = builder.new_scan().plan().await.unwrap();
    let actual: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(actual[0].num_rows(), 1);
    assert_eq!(
        actual[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "changed"
    );
}

#[tokio::test]
async fn inline_vector_sidecar_preserves_values_and_parent_nulls() {
    use arrow_array::{
        builder::{FixedSizeListBuilder, Float32Builder},
        Array, FixedSizeListArray, Float32Array,
    };
    use paimon::spec::{FloatType, VectorType};
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            "embedding",
            DataType::Vector(VectorType::new(3, DataType::Float(FloatType::new())).unwrap()),
        )
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        .option("data-evolution.row-sidecar.enabled", "true")
        .build()
        .unwrap();
    let (io, table) = memory_table("memory:/inline_vector_row", TableSchema::new(0, &schema));
    setup_dirs(&io, table.location()).await;
    persist_table_schema(&io, table.location(), table.schema()).await;
    let mut values = FixedSizeListBuilder::new(Float32Builder::new(), 3).with_field(Arc::new(
        arrow_schema::Field::new("element", arrow_schema::DataType::Float32, true),
    ));
    for id in 0..100 {
        for dimension in 0..3 {
            values.values().append_value((id * 3 + dimension) as f32);
        }
        values.append(id != 6);
    }
    let data = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int32Array::from_iter_values(0..100)) as ArrayRef,
        ),
        ("embedding", Arc::new(values.finish()) as ArrayRef),
    ])
    .unwrap();
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(&data).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(messages[0].new_files[0].extra_files.len(), 1);
    builder.new_commit().commit(messages.clone()).await.unwrap();
    remove_primary(&io, &table, &messages).await;
    let mut builder = table.new_read_builder();
    builder.with_row_ranges(vec![RowRange::new(5, 6)]);
    let plan = builder.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let vectors = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .unwrap();
    assert_eq!(vectors.len(), 2);
    assert!(vectors.is_null(1));
    let values = vectors.value(0);
    assert_eq!(
        values
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[15.0, 16.0, 17.0]
    );
}

#[tokio::test]
async fn inline_blob_views_generate_normal_sidecars() {
    use arrow_array::LargeBinaryArray;
    use paimon::catalog::Identifier;
    use paimon::spec::{BlobType, BlobViewStruct};
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("payload", DataType::Blob(BlobType::new()))
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        .option("data-evolution.row-sidecar.enabled", "true")
        .option("blob-view-field", "payload")
        .build()
        .unwrap();
    let (io, table) = memory_table("memory:/blob_view_row", TableSchema::new(0, &schema));
    setup_dirs(&io, table.location()).await;
    persist_table_schema(&io, table.location(), table.schema()).await;
    let value = BlobViewStruct::new(Identifier::new("db", "source"), 1, 2)
        .serialize()
        .unwrap();
    let data = RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
        (
            "payload",
            Arc::new(LargeBinaryArray::from_iter([None::<&[u8]>])) as ArrayRef,
        ),
    ])
    .unwrap();
    let mut writer = table.new_write_builder().new_write().unwrap();
    writer.write_arrow_batch(&data).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    let files: Vec<_> = messages.iter().flat_map(|m| &m.new_files).collect();
    assert_eq!(files.len(), 1);
    assert_eq!(
        files[0].extra_files,
        vec![format!("{}.row", files[0].file_name)]
    );
    let unresolved = RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(vec![2])) as ArrayRef),
        (
            "payload",
            Arc::new(LargeBinaryArray::from_iter([Some(value.as_slice())])) as ArrayRef,
        ),
    ])
    .unwrap();
    let error = writer.write_arrow_batch(&unresolved).await.unwrap_err();
    assert!(error.to_string().contains("BlobView is not resolved"));
}

#[test]
fn invalid_row_sidecar_selection_options_are_rejected() {
    use paimon::spec::CoreOptions;
    let options = HashMap::from([(
        "data-evolution.row-sidecar.enabled".into(),
        "invalid".into(),
    )]);
    assert!(CoreOptions::new(&options)
        .data_evolution_row_sidecar_enabled()
        .is_err());
    for value in ["0", "-1", "invalid"] {
        let options = HashMap::from([(
            "data-evolution.row-sidecar.max-selected-rows".into(),
            value.into(),
        )]);
        assert!(CoreOptions::new(&options)
            .data_evolution_row_sidecar_max_selected_rows()
            .is_err());
    }
    for value in ["0", "-0.1", "1.1", "invalid"] {
        let options = HashMap::from([(
            "data-evolution.row-sidecar.max-selection-ratio".into(),
            value.into(),
        )]);
        assert!(CoreOptions::new(&options)
            .data_evolution_row_sidecar_max_selection_ratio()
            .is_err());
    }
}

#[tokio::test]
async fn automatic_row_block_flush_releases_memory_reservations() {
    use paimon::resource::ResourceContext;
    let table = table(&[("write.parquet-buffer-size", "1b")]).await;
    let resources = ResourceContext::builder()
        .memory_limit(512 * 1024)
        .build()
        .unwrap();
    let builder = table.new_write_builder().with_resources(resources.clone());
    let mut writer = builder.new_write().unwrap();
    for index in 0..100 {
        writer
            .write_arrow_batch(&batch(index * 1000, 1000))
            .await
            .unwrap();
        assert!(resources.metrics().reserved_memory_bytes < 256 * 1024);
    }
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(resources.metrics().reserved_memory_bytes, 0);
    assert_eq!(messages[0].new_files[0].row_count, 100000);
}

#[tokio::test]
async fn inline_blob_sidecars_materialize_references_and_keep_raw_payloads_after_merging() {
    use arrow_array::{Array, LargeBinaryArray};
    use paimon::spec::{BlobDescriptor, BlobType};
    for option in ["blob-descriptor-field", "blob.stored-descriptor-fields"] {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("payload", DataType::Blob(BlobType::new()))
            .column("value", DataType::Int(IntType::new()))
            .option("row-tracking.enabled", "true")
            .option("data-evolution.enabled", "true")
            .option("data-evolution.row-sidecar.enabled", "true")
            .option(option, "payload")
            .build()
            .unwrap();
        let (io, table) = memory_table("memory:/inline_blob_sidecar", TableSchema::new(0, &schema));
        setup_dirs(&io, table.location()).await;
        persist_table_schema(&io, table.location(), table.schema()).await;
        // The actual payload itself is a valid descriptor. Format provenance,
        // rather than inspecting magic bytes, must distinguish BlobData.
        let payload = BlobDescriptor::new("memory:/must-not-read".into(), 0, 1).serialize();
        let source = [
            b"prefix".as_slice(),
            payload.as_slice(),
            b"suffix".as_slice(),
        ]
        .concat();
        io.new_output("memory:/source")
            .unwrap()
            .write(bytes::Bytes::from(source))
            .await
            .unwrap();
        let reference =
            BlobDescriptor::new("memory:/source".into(), 6, payload.len() as i64).serialize();
        let input = RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int32Array::from_iter_values(0..100)) as ArrayRef,
            ),
            (
                "payload",
                Arc::new(LargeBinaryArray::from_iter(
                    (0..100).map(|id| (id != 6).then_some(reference.as_slice())),
                )) as ArrayRef,
            ),
            (
                "value",
                Arc::new(Int32Array::from_iter_values(0..100)) as ArrayRef,
            ),
        ])
        .unwrap();
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        writer.write_arrow_batch(&input).await.unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        builder.new_commit().commit(messages.clone()).await.unwrap();
        let descriptor_table = table.copy_with_options(HashMap::from([(
            "blob-as-descriptor".into(),
            "true".into(),
        )]));
        for ranges in [None, Some(vec![RowRange::new(5, 5)])] {
            let mut read = descriptor_table.new_read_builder();
            read.with_projection(&["payload"]).unwrap();
            if let Some(ranges) = ranges {
                read.with_row_ranges(ranges);
            }
            let plan = read.new_scan().plan().await.unwrap();
            let actual: Vec<RecordBatch> = read
                .new_read()
                .unwrap()
                .to_arrow(plan.splits())
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            let blobs = actual[0]
                .column(0)
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap();
            assert_eq!(blobs.value(0), reference);
        }
        let update = table.new_write_builder().new_update().unwrap();
        let changed = RecordBatch::try_from_iter([
            ("_ROW_ID", Arc::new(Int64Array::from(vec![5])) as ArrayRef),
            ("value", Arc::new(Int32Array::from(vec![999])) as ArrayRef),
        ])
        .unwrap();
        let updates = update
            .update_by_arrow_with_row_id(vec![changed])
            .await
            .unwrap();
        table
            .new_write_builder()
            .new_commit()
            .commit(updates.clone())
            .await
            .unwrap();
        io.delete_file("memory:/source").await.unwrap();
        remove_primary(&io, &table, &messages).await;
        remove_primary(&io, &table, &updates).await;
        for with_row_id in [false, true] {
            let mut read = table.new_read_builder();
            read.with_projection(if with_row_id {
                &["id", "payload", "value", "_ROW_ID"]
            } else {
                &["id", "payload", "value"]
            })
            .unwrap();
            read.with_row_ranges(vec![RowRange::new(5, 6)]);
            let plan = read.new_scan().plan().await.unwrap();
            let actual: Vec<RecordBatch> = read
                .new_read()
                .unwrap()
                .to_arrow(plan.splits())
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            assert_eq!(actual.len(), 1);
            let blobs = actual[0]
                .column(1)
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap();
            assert_eq!(blobs.value(0), payload);
            assert!(blobs.is_null(1));
            let values = actual[0]
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(values.values().as_ref(), &[999, 6]);
            assert!(actual[0]
                .schema()
                .field(1)
                .metadata()
                .keys()
                .all(|key| !key.contains("blob-data")));
        }
    }
}
