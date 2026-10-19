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

use arrow_array::{ArrayRef, Int32Array, LargeBinaryArray, RecordBatch};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs};
use futures::TryStreamExt;
use opendal::Operator;
use paimon::spec::{BlobType, DataType, IntType, Manifest, ManifestList, Schema, TableSchema};
use paimon::{
    catalog::Identifier,
    io::{FileIOBuilder, FileIOProvider},
    table::Table,
};
use std::sync::{Arc, Mutex};

fn local_wall_millis() -> i64 {
    // Chrono Local ignores TZ on Windows. Resolve the explicit test zone
    // independently, so the same wall-time expectations cover every platform.
    let zone = std::env::var("TZ")
        .ok()
        .and_then(|name| jiff::tz::TimeZone::get(&name).ok())
        .unwrap_or_else(jiff::tz::TimeZone::system);
    jiff::Timestamp::now()
        .to_zoned(zone)
        .datetime()
        .to_zoned(jiff::tz::TimeZone::UTC)
        .unwrap()
        .timestamp()
        .as_millisecond()
}

#[derive(Debug)]
struct IndexOutputProvider {
    operator: Operator,
    index_output_times: Mutex<Vec<i64>>,
}

#[async_trait::async_trait]
impl FileIOProvider for IndexOutputProvider {
    async fn create(&self, path: &str) -> paimon::Result<(Operator, String)> {
        let relative = path.strip_prefix("s3://bucket/").unwrap().to_string();
        if relative.ends_with(".index") {
            // Force a distinct millisecond at the actual auxiliary output,
            // without relying on a storage delay or scheduling a sleep.
            let before = local_wall_millis();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            let completed = loop {
                let now = local_wall_millis();
                if now > before {
                    break now;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "wall clock did not advance"
                );
                tokio::task::yield_now().await;
            };
            self.index_output_times.lock().unwrap().push(completed);
        }
        Ok((self.operator.clone(), relative))
    }
}

async fn check_creation_time_after_index_output(primary_key: bool) {
    let provider = Arc::new(IndexOutputProvider {
        operator: Operator::from_config(opendal::services::MemoryConfig::default()).unwrap(),
        index_output_times: Mutex::new(Vec::new()),
    });
    let io = FileIOBuilder::new("unused")
        .with_provider(provider.clone())
        .build()
        .unwrap();
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .option("file-index.bitmap.columns", "id")
        .option("file-index.in-manifest-threshold", "0b");
    if primary_key {
        schema = schema.primary_key(["id"]).option("bucket", "1");
    }
    let path = "s3://bucket/creation-time-index";
    let table = Table::new(
        io.clone(),
        Identifier::new("default", "creation_time_index"),
        path.into(),
        TableSchema::new(0, &schema.build().unwrap()),
        None,
    );
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    let batch =
        RecordBatch::try_from_iter([("id", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef)])
            .unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    let file = &messages[0].new_files[0];
    let index_output_time = {
        let index_times = provider.index_output_times.lock().unwrap();
        assert_eq!(index_times.len(), 1);
        index_times[0]
    };
    assert!(
        file.creation_time.unwrap().timestamp_millis() >= index_output_time,
        "creation time must be assigned after the auxiliary index output: {:?} vs {}",
        file.creation_time,
        index_output_time,
    );
    assert_eq!(file.extra_files, vec![format!("{}.index", file.file_name)]);
    let index = format!("{path}/bucket-0/{}", file.extra_files[0]);
    assert!(io.new_input(&index).unwrap().metadata().await.unwrap().size > 0);
}

#[tokio::test]
async fn append_creation_time_is_assigned_after_index_output() {
    check_creation_time_after_index_output(false).await;
}

#[tokio::test]
async fn pk_creation_time_is_assigned_after_index_output() {
    check_creation_time_after_index_output(true).await;
}

#[test]
fn writers_persist_local_creation_times_in_all_data_and_changelog_files() {
    const CHILD: &str = "PAIMON_WRITER_CREATION_TIME_ZONE_CHILD";
    if std::env::var_os(CHILD).is_some() {
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(check_writers());
        return;
    }
    for zone in ["Asia/Shanghai", "America/New_York"] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "writers_persist_local_creation_times_in_all_data_and_changelog_files",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env("TZ", zone)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{zone}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

async fn check_writers() {
    for mode in [
        "append",
        "pk",
        "input-changelog",
        "postpone",
        "dedicated-blob",
    ] {
        let mut schema = Schema::builder().column("id", DataType::Int(IntType::new()));
        let payload_type = if mode == "dedicated-blob" {
            DataType::Blob(BlobType::new())
        } else {
            DataType::Int(IntType::new())
        };
        schema = schema
            .column("payload", payload_type)
            .option("file.format", "parquet");
        if matches!(mode, "pk" | "input-changelog" | "postpone") {
            schema = schema
                .primary_key(["id"])
                .option("bucket", if mode == "postpone" { "-2" } else { "1" });
        }
        if mode == "input-changelog" {
            schema = schema
                .option("changelog-producer", "input")
                .option("changelog-file.format", "parquet");
        }
        if mode == "dedicated-blob" {
            schema = schema
                .option("data-evolution.enabled", "true")
                .option("row-tracking.enabled", "true")
                .option("blob-field", "payload");
        }
        let path = format!("memory:/creation_time_{mode}");
        let (io, table) = memory_table(&path, TableSchema::new(0, &schema.build().unwrap()));
        setup_dirs(&io, &path).await;
        persist_table_schema(&io, &path, table.schema()).await;
        let payload: ArrayRef = if mode == "dedicated-blob" {
            Arc::new(LargeBinaryArray::from(vec![Some(b"blob".as_slice())]))
        } else {
            Arc::new(Int32Array::from(vec![10]))
        };
        let batch = RecordBatch::try_from_iter([
            ("id", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
            ("payload", payload),
        ])
        .unwrap();
        let before = local_wall_millis();
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        writer.write_arrow_batch(&batch).await.unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        let after = local_wall_millis();
        let files = messages
            .iter()
            .flat_map(|message| message.new_files.iter().chain(&message.new_changelog_files))
            .collect::<Vec<_>>();
        assert!(!files.is_empty(), "{mode}");
        for file in &files {
            let wall = file.creation_time.unwrap();
            assert!(
                (before..=after).contains(&wall.timestamp_millis()),
                "{mode}: {} outside local wall range {before}..={after}",
                wall.timestamp_millis()
            );
            assert_eq!(
                wall.timestamp_subsec_nanos() % 1_000_000,
                0,
                "{mode}: creation time must have TIMESTAMP(3) precision"
            );
            assert!(file.creation_time_epoch_millis().unwrap().is_some());
        }
        if mode == "input-changelog" {
            assert!(messages
                .iter()
                .any(|message| !message.new_changelog_files.is_empty()));
        }
        if mode == "dedicated-blob" {
            assert!(files.iter().any(|file| file.file_name.ends_with(".blob")));
        }
        let mut expected_times = messages
            .iter()
            .flat_map(|message| &message.new_files)
            .map(|file| file.creation_time)
            .collect::<Vec<_>>();
        expected_times.sort();
        builder.new_commit().commit(messages).await.unwrap();
        writer.close().await;
        let snapshot = table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        let manifest_dir = format!("{}/manifest", table.location());
        let manifests = ManifestList::read(
            &io,
            &format!("{manifest_dir}/{}", snapshot.delta_manifest_list()),
        )
        .await
        .unwrap();
        let mut persisted_times = Vec::new();
        for manifest in manifests {
            persisted_times.extend(
                Manifest::read(&io, &format!("{manifest_dir}/{}", manifest.file_name()))
                    .await
                    .unwrap()
                    .iter()
                    .map(|entry| entry.file().creation_time),
            );
        }
        persisted_times.sort();
        assert_eq!(
            persisted_times, expected_times,
            "{mode}: commit must retain local wall time"
        );
        if mode == "postpone" {
            // Pending files enter normal reads only after a fixed-bucket rewrite.
            // Their committed metadata must still have the same Java timestamp.
            continue;
        }
        let mut read = table.new_read_builder();
        read.with_projection(&["id"]).unwrap();
        let plan = read.new_scan().plan().await.unwrap();
        let batches: Vec<RecordBatch> = read
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            1,
            "{mode}"
        );
    }
}

#[tokio::test]
async fn postpone_creation_time_is_assigned_when_file_result_is_finished() {
    use std::time::{Duration, Instant};

    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("payload", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "-2")
        .build()
        .unwrap();
    let path = "memory:/postpone_creation_at_result";
    let (io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&io, path).await;
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
        ("payload", Arc::new(Int32Array::from(vec![10])) as ArrayRef),
    ])
    .unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    let opened = local_wall_millis();
    // Advance one wall-clock millisecond after opening the file, without a
    // sleep or any process-global time-zone change.
    let started = Instant::now();
    let before_finish = loop {
        let current = local_wall_millis();
        if current > opened {
            break current;
        }
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "wall clock did not advance"
        );
        tokio::task::yield_now().await;
    };
    let messages = writer.prepare_commit().await.unwrap();
    let finished = local_wall_millis();
    let file = &messages[0].new_files[0];
    assert!(
        (before_finish..=finished).contains(&file.creation_time.unwrap().timestamp_millis()),
        "Postpone metadata must use result time, not the earlier file-open time"
    );
    writer.close().await;
}

#[tokio::test]
async fn postpone_rolling_results_keep_arrival_order_and_creation_times() {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("payload", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "-2")
        .option("target-file-row-num", "1")
        .build()
        .unwrap();
    let path = "memory:/postpone_creation_roll_order";
    let (io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&io, path).await;
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    let mut windows = Vec::new();
    for value in [30, 10, 20] {
        let batch = RecordBatch::try_from_iter([
            ("id", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
            (
                "payload",
                Arc::new(Int32Array::from(vec![value])) as ArrayRef,
            ),
        ])
        .unwrap();
        let before = local_wall_millis();
        writer.write_arrow_batch(&batch).await.unwrap();
        let after = local_wall_millis();
        windows.push(before..=after);
    }
    let messages = writer.prepare_commit().await.unwrap();
    let files = &messages[0].new_files;
    assert_eq!(files.len(), 3);
    let mut previous_time = i64::MIN;
    for (index, (file, window)) in files.iter().zip(windows).enumerate() {
        assert_eq!(file.min_sequence_number, index as i64);
        assert_eq!(file.max_sequence_number, index as i64);
        let time = file.creation_time.unwrap().timestamp_millis();
        assert!(
            window.contains(&time),
            "Rolled file must get its timestamp before the next input batch"
        );
        assert!(
            time >= previous_time,
            "Java creation-time replay must preserve this writer's arrival order"
        );
        previous_time = time;
    }
    writer.close().await;
}
