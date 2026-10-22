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

use super::*;
use crate::catalog::Identifier;
use crate::io::FileIOBuilder;
use crate::spec::{BlobDescriptor, BlobType, CommitKind, IntType, Schema, TableSchema};
use arrow_array::{Int32Array, LargeBinaryArray};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

async fn table(fixed: bool, blob: bool) -> Table {
    let io = FileIOBuilder::new("memory").build().unwrap();
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            "value",
            if blob {
                DataType::Blob(BlobType::new())
            } else {
                DataType::Int(IntType::new())
            },
        )
        .column("pt", DataType::Int(IntType::new()))
        .partition_keys(["pt"]);
    if fixed {
        schema = schema.option("bucket", "2").option("bucket-key", "id");
    } else {
        schema = schema
            .option("row-tracking.enabled", "true")
            .option("data-evolution.enabled", "true");
    }
    let table = Table::new(
        io,
        Identifier::new("default", "append_lifecycle"),
        "memory:/append_lifecycle".into(),
        TableSchema::new(0, &schema.build().unwrap()),
        None,
    );
    table
        .file_io()
        .new_output(&table.schema_manager().schema_path(0))
        .unwrap()
        .write(serde_json::to_vec(table.schema()).unwrap().into())
        .await
        .unwrap();
    table
}

fn batch(partition: i32, blob: bool) -> RecordBatch {
    let value: ArrayRef = if blob {
        Arc::new(LargeBinaryArray::from(vec![b"payload".as_slice()]))
    } else {
        Arc::new(Int32Array::from(vec![20]))
    };
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(vec![10])) as ArrayRef),
        ("value", value),
        (
            "pt",
            Arc::new(Int32Array::from(vec![partition])) as ArrayRef,
        ),
    ])
    .unwrap()
}

async fn write(writer: &mut TableWrite, fixed: bool, partition: i32) {
    writer
        .write_arrow(&[batch(partition, false)], fixed.then_some(0))
        .await
        .unwrap();
}

fn assert_columns(messages: &[CommitMessage], column: &str) {
    assert_eq!(messages.len(), 1);
    assert!(!messages[0].new_files.is_empty());
    for file in &messages[0].new_files {
        assert_eq!(file.write_cols, Some(vec!["pt".into(), column.into()]));
        assert_eq!(file.row_count, 1);
    }
}

async fn snapshot(table: &Table, id: i64, user: &str, identifier: i64) {
    let snapshot = Snapshot::builder()
        .version(3)
        .id(id)
        .schema_id(0)
        .base_manifest_list(String::new())
        .delta_manifest_list(String::new())
        .commit_user(user.into())
        .commit_identifier(identifier)
        .commit_kind(CommitKind::APPEND)
        .time_millis(0)
        .build();
    let manager = table.snapshot_manager();
    table
        .file_io()
        .new_output(&manager.snapshot_path(id))
        .unwrap()
        .write(serde_json::to_vec(&snapshot).unwrap().into())
        .await
        .unwrap();
    manager.write_latest_hint(id).await.unwrap();
}

#[tokio::test]
async fn unaware_append_recaptures_type_only_after_an_idle_prepare() {
    for idle_prepares in 0..=2 {
        let table = table(false, false).await;
        let mut writer = TableWrite::new(&table, "owner".into()).unwrap();
        writer
            .with_write_type(vec!["pt".into(), "id".into()])
            .unwrap();
        write(&mut writer, false, 1).await;
        assert_columns(&writer.prepare_commit().await.unwrap(), "id");
        writer
            .with_write_type(vec!["pt".into(), "value".into()])
            .unwrap();
        for _ in 0..idle_prepares {
            assert!(writer.prepare_commit().await.unwrap().is_empty());
        }
        write(&mut writer, false, 1).await;
        assert_columns(
            &writer.prepare_commit().await.unwrap(),
            if idle_prepares == 0 { "id" } else { "value" },
        );
    }
}

#[tokio::test]
async fn empty_prepares_do_not_capture_append_factory_settings() {
    let table = table(false, false).await;
    let mut writer = TableWrite::new(&table, "owner".into()).unwrap();
    writer
        .with_write_type(vec!["pt".into(), "id".into()])
        .unwrap();
    for _ in 0..2 {
        assert!(writer.prepare_commit().await.unwrap().is_empty());
    }
    writer
        .with_write_type(vec!["pt".into(), "value".into()])
        .unwrap();
    write(&mut writer, false, 1).await;
    assert_columns(&writer.prepare_commit().await.unwrap(), "value");
}

#[tokio::test]
async fn idle_append_retirement_recaptures_consumers_and_close_releases_them() {
    for idle_prepares in 0..=2 {
        let table = table(false, true).await;
        let mut writer = TableWrite::new(&table, "owner".into()).unwrap();
        let old_calls = Arc::new(AtomicUsize::new(0));
        let new_calls = Arc::new(AtomicUsize::new(0));
        let consumer = |calls: Arc<AtomicUsize>| {
            Arc::new(move |_: &str, descriptor: Option<&BlobDescriptor>| {
                if descriptor.is_some() {
                    calls.fetch_add(1, Ordering::Relaxed);
                }
                Ok(false)
            }) as Arc<dyn crate::spec::BlobConsumer>
        };
        let old = consumer(old_calls.clone());
        let old_ref = Arc::downgrade(&old);
        writer.with_blob_consumer(Some(old)).unwrap();
        writer.write_arrow_batch(&batch(1, true)).await.unwrap();
        writer.prepare_commit().await.unwrap();
        let new = consumer(new_calls.clone());
        let new_ref = Arc::downgrade(&new);
        writer.with_blob_consumer(Some(new)).unwrap();
        for _ in 0..idle_prepares {
            assert!(writer.prepare_commit().await.unwrap().is_empty());
        }
        assert_eq!(old_ref.upgrade().is_some(), idle_prepares == 0);
        writer.write_arrow_batch(&batch(1, true)).await.unwrap();
        writer.prepare_commit().await.unwrap();
        assert_eq!(
            old_calls.load(Ordering::Relaxed),
            if idle_prepares == 0 { 2 } else { 1 }
        );
        assert_eq!(
            new_calls.load(Ordering::Relaxed),
            usize::from(idle_prepares != 0)
        );
        writer.close().await;
        assert!(old_ref.upgrade().is_none());
        assert!(new_ref.upgrade().is_none());
    }
}

#[tokio::test]
async fn fixed_append_waits_for_a_strictly_newer_committed_identifier_of_its_user() {
    let table = table(true, false).await;
    let mut writer = TableWrite::new(&table, "owner".into()).unwrap();
    writer
        .with_write_type(vec!["pt".into(), "id".into()])
        .unwrap();
    write(&mut writer, true, 1).await;
    assert_columns(
        &writer.prepare_commit_with_identifier(1).await.unwrap(),
        "id",
    );
    writer
        .with_write_type(vec!["pt".into(), "value".into()])
        .unwrap();
    for (id, user, identifier) in [(1, "owner", 1), (2, "owner", 1), (3, "other", 99)] {
        snapshot(&table, id, user, identifier).await;
        assert!(writer
            .prepare_commit_with_identifier(2)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(writer.append_write_configs.len(), 1);
    }
    snapshot(&table, 4, "owner", 2).await;
    assert!(writer
        .prepare_commit_with_identifier(3)
        .await
        .unwrap()
        .is_empty());
    write(&mut writer, true, 1).await;
    assert_columns(
        &writer.prepare_commit_with_identifier(3).await.unwrap(),
        "value",
    );
}

#[tokio::test]
async fn fixed_append_keeps_unknown_prepare_identifiers() {
    for initial_identifier in [None, Some(1)] {
        let table = table(true, false).await;
        let mut writer = TableWrite::new(&table, "owner".into()).unwrap();
        writer
            .with_write_type(vec!["pt".into(), "id".into()])
            .unwrap();
        write(&mut writer, true, 1).await;
        writer
            .prepare_commit_internal(initial_identifier)
            .await
            .unwrap();
        writer
            .with_write_type(vec!["pt".into(), "value".into()])
            .unwrap();
        snapshot(&table, 1, "owner", 2).await;
        assert!(writer.prepare_commit().await.unwrap().is_empty());
        if initial_identifier.is_none() {
            assert!(writer
                .prepare_commit_with_identifier(3)
                .await
                .unwrap()
                .is_empty());
        }
        write(&mut writer, true, 1).await;
        assert_columns(&writer.prepare_commit().await.unwrap(), "id");
    }
}

#[tokio::test]
async fn fixed_append_stops_at_an_expired_snapshot_gap() {
    let table = table(true, false).await;
    let mut writer = TableWrite::new(&table, "owner".into()).unwrap();
    writer
        .with_write_type(vec!["pt".into(), "id".into()])
        .unwrap();
    write(&mut writer, true, 1).await;
    writer.prepare_commit_with_identifier(1).await.unwrap();
    writer
        .with_write_type(vec!["pt".into(), "value".into()])
        .unwrap();
    snapshot(&table, 1, "owner", 2).await;
    snapshot(&table, 3, "other", 99).await;
    assert!(writer
        .prepare_commit_with_identifier(3)
        .await
        .unwrap()
        .is_empty());
    write(&mut writer, true, 1).await;
    assert_columns(
        &writer.prepare_commit_with_identifier(3).await.unwrap(),
        "id",
    );
}

#[tokio::test]
async fn fixed_append_metadata_errors_leave_pending_output_available() {
    let table = table(true, false).await;
    let mut writer = TableWrite::new(&table, "owner".into()).unwrap();
    writer
        .with_write_type(vec!["pt".into(), "id".into()])
        .unwrap();
    write(&mut writer, true, 1).await;
    writer.prepare_commit_with_identifier(1).await.unwrap();
    writer
        .with_write_type(vec!["pt".into(), "value".into()])
        .unwrap();
    write(&mut writer, true, 2).await;
    let manager = table.snapshot_manager();
    table
        .file_io()
        .new_output(&manager.snapshot_path(1))
        .unwrap()
        .write(bytes::Bytes::from_static(b"invalid snapshot JSON"))
        .await
        .unwrap();
    manager.write_latest_hint(1).await.unwrap();
    assert!(matches!(
        writer.prepare_commit_with_identifier(2).await,
        Err(crate::Error::DataInvalid { .. })
    ));
    snapshot(&table, 1, "owner", 1).await;
    assert_columns(
        &writer.prepare_commit_with_identifier(2).await.unwrap(),
        "value",
    );
}

#[tokio::test]
async fn failed_prepare_preserves_exposed_blob_files_after_releasing_contexts() {
    let table = table(false, true).await;
    let exposed = Arc::new(Mutex::new(Vec::new()));
    let captured = exposed.clone();
    let mut writer = TableWrite::new(&table, "owner".into()).unwrap();
    writer
        .with_blob_consumer(Some(Arc::new(
            move |_: &str, descriptor: Option<&BlobDescriptor>| {
                if let Some(descriptor) = descriptor {
                    captured.lock().unwrap().push(descriptor.uri().to_string());
                }
                Ok(false)
            },
        )))
        .unwrap();
    writer.write_arrow_batch(&batch(1, true)).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    // Model the successful partition outputs collected before another
    // partition's prepare failed. These files have not been handed off.
    writer.fail_prepare(&messages).await;
    let exposed = exposed.lock().unwrap().clone();
    assert!(!exposed.is_empty());
    for path in exposed {
        assert!(table.file_io().exists(&path).await.unwrap());
    }
    assert!(writer.append_write_configs.is_empty());
    assert!(writer.blob_consumer.is_none());
    assert!(!table
        .file_io()
        .list_status_recursive(table.location())
        .await
        .unwrap()
        .iter()
        .any(|file| file.path.ends_with(".parquet")));
}
