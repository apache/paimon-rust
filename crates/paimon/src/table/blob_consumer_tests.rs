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

use super::{Table, TableWrite};
use crate::catalog::Identifier;
use crate::io::{FileIOBuilder, FileRead};
use crate::spec::{BlobDescriptor, BlobType, DataType, IntType, Schema, TableSchema};
use arrow_array::{ArrayRef, Int32Array, LargeBinaryArray, RecordBatch};
use std::sync::{Arc, Mutex};

fn table(path: &str, primary_key: bool, options: &[(&str, &str)]) -> Table {
    let io = FileIOBuilder::new("memory").build().unwrap();
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("payload", DataType::Blob(BlobType::new()));
    if primary_key {
        schema = schema.primary_key(["id"]).option("bucket", "1");
    } else {
        schema = schema
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true");
    }
    for (name, value) in options {
        schema = schema.option(*name, *value);
    }
    Table::new(
        io,
        Identifier::new("db", "t"),
        path.into(),
        TableSchema::new(0, &schema.build().unwrap()),
        None,
    )
}

fn batch() -> RecordBatch {
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
        (
            "payload",
            Arc::new(LargeBinaryArray::from(vec![
                Some(b"payload".as_slice()),
                None,
            ])) as ArrayRef,
        ),
    ])
    .unwrap()
}

#[tokio::test]
async fn append_consumer_survives_file_rolls_and_checkpoint_recreation() {
    let table = table(
        "memory:/consumer-checkpoints",
        false,
        &[
            ("target-file-row-num", "1"),
            ("blob.copy-buffer-size", "2 B"),
        ],
    );
    let events = Arc::new(Mutex::new(Vec::new()));
    let received = events.clone();
    let mut writer = TableWrite::new(&table, "consumer".into()).unwrap();
    writer
        .with_blob_consumer(Some(Arc::new(
            move |name: &str, descriptor: Option<&BlobDescriptor>| {
                assert_eq!(name, "payload");
                received.lock().unwrap().push(descriptor.cloned());
                Ok(true)
            },
        )))
        .unwrap();
    for _ in 0..2 {
        writer.write_arrow_batch(&batch()).await.unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        assert_eq!(
            messages[0]
                .new_files
                .iter()
                .filter(|file| file.file_name.ends_with(".blob"))
                .map(|file| file.row_count)
                .sum::<i64>(),
            2
        );
        assert!(writer.with_blob_consumer(None).is_err());
    }
    writer.close().await;
    let events = events.lock().unwrap().clone();
    assert_eq!(events.len(), 4);
    for (index, descriptor) in events.into_iter().enumerate() {
        if index % 2 == 1 {
            assert!(descriptor.is_none());
        } else {
            let descriptor = descriptor.unwrap();
            assert_eq!(descriptor.length(), 7);
            let reader = table
                .file_io()
                .new_input(descriptor.uri())
                .unwrap()
                .reader()
                .await
                .unwrap();
            let start = descriptor.offset() as u64;
            assert_eq!(
                reader.read(start..start + 7).await.unwrap().as_ref(),
                b"payload"
            );
        }
    }
}

#[tokio::test]
async fn callback_failure_preserves_files_from_previous_prepare() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let table = table("memory:/consumer-failed-checkpoint", false, &[]);
    let fail = Arc::new(AtomicBool::new(false));
    let callback_fail = fail.clone();
    let mut writer = TableWrite::new(&table, "consumer".into()).unwrap();
    writer
        .with_blob_consumer(Some(Arc::new(
            move |_: &str, _: Option<&BlobDescriptor>| {
                if callback_fail.load(Ordering::Relaxed) {
                    Err(crate::Error::DataInvalid {
                        message: "consumer failed".into(),
                        source: None,
                    })
                } else {
                    Ok(false)
                }
            },
        )))
        .unwrap();
    writer.write_arrow_batch(&batch()).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    let mut paths = Vec::new();
    for file in &messages[0].new_files {
        let path = format!("{}/bucket-0/{}", table.location(), file.file_name);
        paths.push((
            path.clone(),
            table
                .file_io()
                .new_input(&path)
                .unwrap()
                .read()
                .await
                .unwrap(),
        ));
    }
    fail.store(true, Ordering::Relaxed);
    assert!(writer
        .write_arrow_batch(&batch())
        .await
        .unwrap_err()
        .to_string()
        .contains("consumer failed"));
    writer.close().await;
    for (path, expected) in paths {
        assert_eq!(
            table
                .file_io()
                .new_input(&path)
                .unwrap()
                .read()
                .await
                .unwrap(),
            expected
        );
    }
}

#[tokio::test]
async fn explicit_commit_abort_can_discard_consumer_files() {
    let table = table("memory:/consumer-explicit-abort", false, &[]);
    let mut writer = TableWrite::new(&table, "consumer".into()).unwrap();
    writer
        .with_blob_consumer(Some(Arc::new(|_: &str, _: Option<&BlobDescriptor>| {
            Ok(false)
        })))
        .unwrap();
    writer.write_arrow_batch(&batch()).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    writer.close().await;
    assert!(!table
        .file_io()
        .list_status_recursive(table.location())
        .await
        .unwrap()
        .is_empty());
    let commit = super::TableCommit::new(table.clone(), "consumer".into());
    commit.abort(&messages).await.unwrap();
    assert!(table
        .file_io()
        .list_status_recursive(table.location())
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn primary_key_blob_consumer_is_ignored_like_java() {
    let table = table(
        "memory:/consumer-primary-key",
        true,
        &[("blob.copy-buffer-size", "2 B")],
    );
    let mut writer = TableWrite::new(&table, "consumer".into()).unwrap();
    writer
        .with_blob_consumer(Some(Arc::new(|_: &str, _: Option<&BlobDescriptor>| {
            panic!("Java PK managed packs do not use the append BlobConsumer")
        })))
        .unwrap();
    writer.write_arrow_batch(&batch()).await.unwrap();
    assert!(!writer.prepare_commit().await.unwrap()[0]
        .new_files
        .is_empty());
    writer.close().await;
}

#[test]
fn invalid_copy_buffers_are_rejected_before_data_is_written() {
    for primary_key in [false, true] {
        for value in ["0 B", "-1 B", "2147483648 B", "invalid"] {
            let table = table(
                "memory:/consumer-invalid-buffer",
                primary_key,
                &[("blob.copy-buffer-size", value)],
            );
            assert!(TableWrite::new(&table, "consumer".into())
                .err()
                .unwrap()
                .to_string()
                .contains("blob.copy-buffer-size"));
        }
    }
}
