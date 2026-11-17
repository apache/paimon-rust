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

use arrow_array::{Array, ArrayRef, Int32Array, LargeBinaryArray, RecordBatch};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs};
use futures::TryStreamExt;
use paimon::io::FileRead;
use paimon::spec::{BlobType, DataType, IntType, Schema, TableSchema, VideoFrameDescriptor};
use paimon::table::Table;
use std::sync::Arc;

async fn table(size_roll: bool) -> Table {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("frame", DataType::Blob(BlobType::new()))
        .column("payload", DataType::Blob(BlobType::new()))
        .option("data-evolution.enabled", "true")
        .option("data-evolution.row-sidecar.enabled", "true")
        .option("row-tracking.enabled", "true")
        .option("video-frame-field", "frame")
        .option("target-file-row-num", if size_roll { "100" } else { "3" })
        .option(
            "blob.target-file-size",
            if size_roll { "5 B" } else { "1 MB" },
        )
        .build()
        .unwrap();
    let (io, table) = memory_table("memory:/videos", TableSchema::new(0, &schema));
    setup_dirs(&io, table.location()).await;
    persist_table_schema(&io, table.location(), table.schema()).await;
    io.new_output("memory:/source")
        .unwrap()
        .write(b"prefixabcWXYZ".to_vec().into())
        .await
        .unwrap();
    table
}

fn input() -> RecordBatch {
    let descriptors: Vec<_> = [
        (6, 3, 2),
        (6, 3, 3),
        (9, 4, 7),
        (6, 3, 10),
        (9, -1, 8),
        (6, 3, i64::MAX),
    ]
    .into_iter()
    .map(|(offset, length, frame)| {
        VideoFrameDescriptor::new("memory:/source".into(), offset, length, frame)
            .unwrap()
            .serialize()
    })
    .collect();
    RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6])) as ArrayRef,
        ),
        (
            "frame",
            Arc::new(LargeBinaryArray::from(vec![
                Some(descriptors[0].as_slice()),
                Some(&descriptors[1]),
                Some(&descriptors[2]),
                None,
                Some(&descriptors[3]),
                Some(&descriptors[4]),
                Some(&descriptors[5]),
            ])) as ArrayRef,
        ),
        (
            "payload",
            Arc::new(LargeBinaryArray::from(vec![Some(b"blob".as_slice()); 7])) as ArrayRef,
        ),
    ])
    .unwrap()
}

#[tokio::test]
async fn video_columns_round_trip_with_ordinary_blobs_and_rolled_row_ranges() {
    for size_roll in [false, true] {
        let table = table(size_roll).await;
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        let input = input();
        writer.write_arrow_batch(&input.slice(0, 1)).await.unwrap();
        writer.write_arrow_batch(&input.slice(1, 2)).await.unwrap();
        writer.write_arrow_batch(&input.slice(3, 4)).await.unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        let files: Vec<_> = messages
            .iter()
            .flat_map(|message| &message.new_files)
            .collect();
        let videos: Vec<_> = files
            .iter()
            .filter(|file| file.file_name.ends_with(".video"))
            .collect();
        assert!(
            videos.len() > 1,
            "both row and physical byte limits must roll video files"
        );
        assert_eq!(videos.iter().map(|file| file.row_count).sum::<i64>(), 7);
        assert!(videos
            .iter()
            .all(|file| file.write_cols == Some(vec!["frame".into()])));
        assert!(files.iter().any(|file| file.file_name.ends_with(".blob")
            && file.write_cols == Some(vec!["payload".into()])));
        builder.new_commit().commit(messages).await.unwrap();
        for descriptor_mode in [false, true] {
            let read_table = table.copy_with_options(
                [("blob-as-descriptor".into(), descriptor_mode.to_string())]
                    .into_iter()
                    .collect(),
            );
            let reader = read_table.new_read_builder();
            let plan = reader.new_scan().plan().await.unwrap();
            let batches: Vec<RecordBatch> = reader
                .new_read()
                .unwrap()
                .to_arrow(plan.splits())
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            let mut rows = Vec::new();
            for batch in batches {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let frames = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    let value = if frames.is_null(row) {
                        None
                    } else {
                        let frame = VideoFrameDescriptor::deserialize(frames.value(row)).unwrap();
                        let payload = frame.payload_descriptor();
                        assert!(payload.uri().ends_with(".video"));
                        let bytes = table
                            .file_io()
                            .new_input(payload.uri())
                            .unwrap()
                            .reader()
                            .await
                            .unwrap()
                            .read(
                                payload.offset() as u64
                                    ..(payload.offset() + payload.length()) as u64,
                            )
                            .await
                            .unwrap();
                        Some((frame.frame_index(), bytes.to_vec()))
                    };
                    rows.push((ids.value(row), value));
                }
            }
            rows.sort_by_key(|row| row.0);
            assert_eq!(
                rows,
                vec![
                    (0, Some((2, b"abc".to_vec()))),
                    (1, Some((3, b"abc".to_vec()))),
                    (2, Some((7, b"WXYZ".to_vec()))),
                    (3, None),
                    (4, Some((10, b"abc".to_vec()))),
                    (5, Some((8, b"WXYZ".to_vec()))),
                    (6, Some((i64::MAX, b"abc".to_vec())))
                ]
            );
        }
    }
}

#[tokio::test]
async fn video_table_rejects_consumers_before_any_output_is_created() {
    let table = table(false).await;
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    let error = writer
        .with_blob_consumer(Some(Arc::new(
            |_: &str, _: Option<&paimon::spec::BlobDescriptor>| Ok(false),
        )))
        .err()
        .unwrap();
    assert!(error.to_string().contains("BlobConsumer is not supported"));
    writer.with_blob_consumer(None).unwrap();
    writer
        .write_arrow_batch(&input().slice(0, 1))
        .await
        .unwrap();
    assert!(!writer.prepare_commit().await.unwrap().is_empty());
}
