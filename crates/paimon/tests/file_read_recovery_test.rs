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

use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs};
use futures::TryStreamExt;
use paimon::spec::{DataType, IntType, Schema, TableSchema};
use paimon::table::{CommitMessage, Table};
use std::collections::HashMap;
use std::sync::Arc;

async fn table(mode: &str) -> Table {
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()));
    if mode == "pk" {
        schema = schema.primary_key(["id"]).option("bucket", "1");
    } else {
        schema = schema.option("row-tracking.enabled", "true");
    }
    if mode == "evolution" {
        schema = schema.option("data-evolution.enabled", "true");
    }
    let (io, table) = memory_table(
        "memory:/file_recovery",
        TableSchema::new(0, &schema.build().unwrap()),
    );
    setup_dirs(&io, table.location()).await;
    persist_table_schema(&io, table.location(), table.schema()).await;
    table
}

fn batch(start: i32) -> RecordBatch {
    RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int32Array::from_iter_values(start..start + 10)) as ArrayRef,
        ),
        (
            "value",
            Arc::new(Int32Array::from_iter_values(start + 100..start + 110)) as ArrayRef,
        ),
    ])
    .unwrap()
}

async fn append(table: &Table, start: i32) -> Vec<CommitMessage> {
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(&batch(start)).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    writer.close().await;
    builder.new_commit().commit(messages.clone()).await.unwrap();
    messages
}

fn path(table: &Table, messages: &[CommitMessage]) -> String {
    messages[0].new_files[0].data_file_path(&format!("{}/bucket-0", table.location()))
}

async fn read(table: &Table, projection: &[&str]) -> paimon::Result<Vec<RecordBatch>> {
    let mut builder = table.new_read_builder();
    builder.with_projection(projection)?;
    let plan = builder.new_scan().plan().await?;
    builder
        .new_read()?
        .to_arrow(plan.splits())?
        .try_collect()
        .await
}

fn ids(batches: &[RecordBatch]) -> Vec<i32> {
    let mut ids: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn only_the_requested_file_failure_is_ignored() {
    for mode in ["append", "evolution", "pk"] {
        for corrupt in [false, true] {
            let table = table(mode).await;
            let first = append(&table, 0).await;
            let second = append(&table, 10).await;
            let bad_path = path(&table, &first);
            if corrupt {
                table
                    .file_io()
                    .new_output(&bad_path)
                    .unwrap()
                    .write(bytes::Bytes::from_static(b"corrupt parquet"))
                    .await
                    .unwrap();
            } else {
                table.file_io().delete_file(&bad_path).await.unwrap();
            }
            let (option, other) = if corrupt {
                ("scan.ignore-corrupt-files", "scan.ignore-lost-files")
            } else {
                ("scan.ignore-lost-files", "scan.ignore-corrupt-files")
            };
            for options in [
                HashMap::new(),
                HashMap::from([(option.into(), "false".into())]),
                HashMap::from([(other.into(), "true".into())]),
            ] {
                assert!(
                    read(&table.copy_with_options(options), &["id", "value"])
                        .await
                        .is_err(),
                    "mode={mode}, corrupt={corrupt}"
                );
            }
            let tolerant = table.copy_with_options(HashMap::from([(option.into(), "true".into())]));
            assert_eq!(
                ids(&read(&tolerant, &["id", "value"]).await.unwrap()),
                (10..20).collect::<Vec<_>>()
            );
            let empty_columns = read(&tolerant, &[]).await.unwrap();
            assert_eq!(
                empty_columns
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>(),
                10
            );
            if mode != "pk" {
                let output = read(&tolerant, &["id", "_ROW_ID"]).await.unwrap();
                for batch in output {
                    let id = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    let row_id = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    for i in 0..batch.num_rows() {
                        assert_eq!(i64::from(id.value(i)), row_id.value(i));
                    }
                }
            }
            assert!(table
                .file_io()
                .exists(&path(&table, &second))
                .await
                .unwrap());
            assert_eq!(table.file_io().exists(&bad_path).await.unwrap(), corrupt);
        }
    }
}

#[tokio::test]
async fn a_skipped_column_file_stops_only_its_data_evolution_group() {
    for corrupt in [false, true] {
        let table = table("evolution").await;
        append(&table, 0).await;
        append(&table, 10).await;
        let builder = table.new_write_builder();
        let updates = builder
            .new_update()
            .unwrap()
            .update_by_arrow_with_row_id(vec![RecordBatch::try_from_iter([
                (
                    "_ROW_ID",
                    Arc::new(Int64Array::from_iter_values(0..10)) as ArrayRef,
                ),
                (
                    "value",
                    Arc::new(Int32Array::from(vec![999; 10])) as ArrayRef,
                ),
            ])
            .unwrap()])
            .await
            .unwrap();
        builder.new_commit().commit(updates.clone()).await.unwrap();
        let bad_path = path(&table, &updates);
        if corrupt {
            table
                .file_io()
                .new_output(&bad_path)
                .unwrap()
                .write(bytes::Bytes::from_static(b"corrupt"))
                .await
                .unwrap();
        } else {
            table.file_io().delete_file(&bad_path).await.unwrap();
        }
        // An unrequested column is not opened and cannot suppress the id scan.
        assert_eq!(
            ids(&read(&table, &["id"]).await.unwrap()),
            (0..20).collect::<Vec<_>>()
        );
        assert!(read(&table, &["id", "value"]).await.is_err());
        let option = if corrupt {
            "scan.ignore-corrupt-files"
        } else {
            "scan.ignore-lost-files"
        };
        let tolerant = table.copy_with_options(HashMap::from([(option.into(), "true".into())]));
        let actual = read(&tolerant, &["id", "value"]).await.unwrap();
        assert_eq!(ids(&actual), (10..20).collect::<Vec<_>>());
        for batch in actual {
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(values.values().to_vec(), (110..120).collect::<Vec<_>>());
        }
    }
}

#[tokio::test]
async fn ignoring_corrupt_files_does_not_hide_resource_exhaustion() {
    let table = table("append").await;
    append(&table, 0).await;
    let table = table.copy_with_options(HashMap::from([(
        "scan.ignore-corrupt-files".into(),
        "true".into(),
    )]));
    let resources = paimon::resource::ResourceContext::builder()
        .memory_limit(0)
        .build()
        .unwrap();
    let mut builder = table.new_read_builder();
    builder.with_resources(resources.clone());
    let plan = builder.new_scan().plan().await.unwrap();
    let error = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err();
    assert!(
        matches!(error, paimon::Error::ResourceExhausted { .. }),
        "{error}"
    );
    assert_eq!(resources.metrics().reserved_memory_bytes, 0);
}

#[tokio::test]
async fn external_blob_reference_failure_is_not_a_data_file_failure() {
    use arrow_array::LargeBinaryArray;
    use paimon::spec::{BlobDescriptor, BlobType};
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("payload", DataType::Blob(BlobType::new()))
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        .option("blob-descriptor-field", "payload")
        .option("scan.ignore-lost-files", "true")
        .option("scan.ignore-corrupt-files", "true")
        .build()
        .unwrap();
    let (io, table) = memory_table("memory:/blob_failure", TableSchema::new(0, &schema));
    setup_dirs(&io, table.location()).await;
    persist_table_schema(&io, table.location(), table.schema()).await;
    let reference = BlobDescriptor::new("memory:/missing-payload".into(), 0, 1).serialize();
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer
        .write_arrow_batch(
            &RecordBatch::try_from_iter([
                ("id", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
                (
                    "payload",
                    Arc::new(LargeBinaryArray::from(vec![reference.as_slice()])) as ArrayRef,
                ),
            ])
            .unwrap(),
        )
        .await
        .unwrap();
    builder
        .new_commit()
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();
    assert_eq!(ids(&read(&table, &["id"]).await.unwrap()), vec![1]);
    let error = read(&table, &["id", "payload"]).await.unwrap_err();
    assert!(error.to_string().contains("missing-payload"), "{error}");
}

#[tokio::test]
async fn decode_failure_keeps_returned_batches_and_releases_prefetch_resources() {
    use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter};
    use parquet::file::properties::WriterProperties;
    for with_resources in [false, true] {
        let table = table("append").await;
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        let data = batch(0);
        writer.write_arrow_batch(&data).await.unwrap();
        let mut messages = writer.prepare_commit().await.unwrap();
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .set_dictionary_enabled(false)
            .build();
        let mut parquet =
            ArrowWriter::try_new(Vec::new(), data.schema(), Some(properties)).unwrap();
        parquet.write(&data).unwrap();
        let mut bytes = parquet.into_inner().unwrap();
        let meta =
            ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes.clone())).unwrap();
        let (offset, size) = meta.metadata().row_group(1).column(0).byte_range();
        bytes[offset as usize..(offset + size) as usize].fill(0);
        messages[0].new_files[0].file_size = bytes.len() as i64;
        table
            .file_io()
            .new_output(&path(&table, &messages))
            .unwrap()
            .write(bytes::Bytes::from(bytes))
            .await
            .unwrap();
        builder.new_commit().commit(messages).await.unwrap();
        append(&table, 10).await;
        // A lost-files option does not ignore a failure after reader creation.
        let lost_only = table.copy_with_options(HashMap::from([(
            "scan.ignore-lost-files".into(),
            "true".into(),
        )]));
        assert!(read(&lost_only, &["id"]).await.is_err());
        let tolerant = table.copy_with_options(HashMap::from([
            ("scan.ignore-corrupt-files".into(), "true".into()),
            ("read.batch-size".into(), "2".into()),
        ]));
        let resources = paimon::resource::ResourceContext::builder()
            .memory_limit(1024 * 1024)
            .build()
            .unwrap();
        let mut builder = tolerant.new_read_builder();
        builder.with_projection(&["id", "_ROW_ID"]).unwrap();
        if with_resources {
            builder.with_resources(resources.clone());
        }
        let plan = builder.new_scan().plan().await.unwrap();
        let output = builder
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(ids(&output), [vec![0, 1], (10..20).collect()].concat());
        for batch in output {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let positions = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                assert_eq!(i64::from(ids.value(i)), positions.value(i));
            }
        }
        // Parallel decoders release their permits after the completion
        // message reaches the consumer. Drain that async handoff, as in the
        // Parquet cancellation tests, and fail if any reservation remains.
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while resources.metrics().reserved_memory_bytes != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(resources.metrics().reserved_memory_bytes, 0);
    }
}
