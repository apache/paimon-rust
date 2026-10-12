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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

#[derive(Default)]
struct RecordingCommitCallback {
    contexts: Mutex<Vec<CommitCallbackContext>>,
    fail: bool,
}

#[async_trait::async_trait]
impl CommitCallback for RecordingCommitCallback {
    async fn call(&self, context: &CommitCallbackContext) -> Result<()> {
        self.contexts.lock().unwrap().push(context.clone());
        if self.fail {
            return Err(crate::Error::DataInvalid {
                message: "callback rejected notification".into(),
                source: None,
            });
        }
        Ok(())
    }
}

struct CallbackTestPublisher {
    manager: SnapshotManager,
    behavior: &'static str,
    attempts: AtomicUsize,
}

#[async_trait::async_trait]
impl SnapshotCommit for CallbackTestPublisher {
    async fn commit(
        &self,
        _: Option<&str>,
        snapshot: &Snapshot,
        _: &str,
        _: &[PartitionStatistics],
    ) -> Result<bool> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        if self.behavior == "reject" || (self.behavior == "conflict" && attempt == 0) {
            return Ok(false);
        }
        assert!(self.manager.commit_snapshot(snapshot).await?);
        if self.behavior == "lose-response" {
            return Err(crate::Error::UnexpectedError {
                message: "publication response lost".into(),
                source: None,
            });
        }
        Ok(true)
    }
}

#[tokio::test]
async fn commit_callback_gets_assigned_metadata_after_publication() {
    for behavior in ["success", "lose-response", "conflict"] {
        let io = test_file_io();
        let path = format!("memory:/callback-{behavior}");
        setup_dirs(&io, &path).await;
        let table = test_table_with_options(
            &io,
            &path,
            HashMap::from([("row-tracking.enabled".into(), "true".into())]),
        );
        let mut commit = TableCommit::new(table, "callback-user".into());
        commit.commit_min_retry_wait_ms = 0;
        commit.commit_max_retry_wait_ms = 0;
        let publisher = Arc::new(CallbackTestPublisher {
            manager: commit.snapshot_manager.clone(),
            behavior,
            attempts: AtomicUsize::new(0),
        });
        commit.snapshot_commit = publisher.clone();
        let callback = Arc::new(RecordingCommitCallback::default());
        commit.add_commit_callback(callback.clone()).unwrap();
        let mut message = append_message("data.parquet");
        message.partition = vec![0, 0, 0, 0];
        message.new_files[0].file_source = Some(0);
        let mut min = BinaryRowBuilder::new(2);
        min.write_int(0, 1);
        min.write_string(1, "first");
        let mut max = BinaryRowBuilder::new(2);
        max.write_int(0, 10);
        max.write_string(1, "last");
        let expected_stats = BinaryTableStats::new(
            min.build().to_serialized_bytes(),
            max.build().to_serialized_bytes(),
            vec![Some(0), Some(1)],
        );
        message.new_files[0].value_stats = expected_stats.clone();
        message
            .new_changelog_files
            .push(test_data_file("changelog.parquet", 10));
        let properties = HashMap::from([("source".into(), "test".into())]);
        commit
            .clone()
            .with_snapshot_properties(properties.clone())
            .commit_with_identifier(vec![message], 7)
            .await
            .unwrap();
        let latest = commit
            .snapshot_manager
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        let persisted_entries = commit.read_delta_entries(None, &latest).await.unwrap();
        let contexts = callback.contexts.lock().unwrap();
        assert_eq!(contexts.len(), 1);
        let context = &contexts[0];
        assert_eq!(context.snapshot, latest);
        assert_eq!(context.identifier, 7);
        assert_eq!(context.snapshot.properties(), Some(&properties));
        assert_eq!(
            context.commit_entries.len(),
            1,
            "changelog is not a data-file delta"
        );
        let file = context.commit_entries[0].file();
        assert_eq!(file.value_stats, expected_stats);
        assert_eq!(file.first_row_id, Some(0));
        assert_eq!(file.min_sequence_number, 1);
        assert_eq!(file.max_sequence_number, 1);
        assert_eq!(context.commit_entries, persisted_entries);
        assert_eq!(
            publisher.attempts.load(Ordering::SeqCst),
            if behavior == "conflict" { 2 } else { 1 }
        );
    }
}

#[tokio::test]
async fn commit_callback_overwrite_and_truncate_include_deletes() {
    let io = test_file_io();
    let path = "memory:/callback-overwrite";
    setup_dirs(&io, path).await;
    let mut commit = setup_commit(&io, path);
    commit
        .commit_with_identifier(vec![append_message("old.parquet")], 1)
        .await
        .unwrap();
    let callback = Arc::new(RecordingCommitCallback::default());
    commit.add_commit_callback(callback.clone()).unwrap();
    commit
        .overwrite(vec![append_message("new.parquet")], None)
        .await
        .unwrap();
    commit.truncate_table().await.unwrap();
    let contexts = callback.contexts.lock().unwrap();
    assert_eq!(contexts.len(), 2);
    let delta = &contexts[0].commit_entries;
    assert_eq!(contexts[0].snapshot.commit_kind(), &CommitKind::OVERWRITE);
    assert_eq!(delta.len(), 2);
    assert_eq!(*delta[0].kind(), FileKind::Delete);
    assert_eq!(delta[0].file().file_name, "old.parquet");
    assert_eq!(*delta[1].kind(), FileKind::Add);
    assert_eq!(delta[1].file().file_name, "new.parquet");
    assert_eq!(contexts[1].commit_entries.len(), 1);
    assert_eq!(*contexts[1].commit_entries[0].kind(), FileKind::Delete);
}

#[tokio::test]
async fn commit_callback_error_preserves_published_files_and_stops_later_callbacks() {
    for behavior in ["success", "lose-response"] {
        let io = test_file_io();
        let path = format!("memory:/callback-error-{behavior}");
        setup_dirs(&io, &path).await;
        let data_path = format!("{path}/bucket-0/data.parquet");
        io.new_output(&data_path)
            .unwrap()
            .write(bytes::Bytes::from_static(b"data"))
            .await
            .unwrap();
        let mut commit = setup_commit(&io, &path);
        commit.commit_min_retry_wait_ms = 0;
        commit.commit_max_retry_wait_ms = 0;
        let publisher = Arc::new(CallbackTestPublisher {
            manager: commit.snapshot_manager.clone(),
            behavior,
            attempts: AtomicUsize::new(0),
        });
        commit.snapshot_commit = publisher.clone();
        let first = Arc::new(RecordingCommitCallback {
            fail: true,
            ..Default::default()
        });
        let second = Arc::new(RecordingCommitCallback::default());
        commit.add_commit_callback(first.clone()).unwrap();
        commit.add_commit_callback(second.clone()).unwrap();
        let error = commit
            .commit_with_identifier(vec![append_message("data.parquet")], 7)
            .await
            .unwrap_err();
        assert!(format!("{error:?}").contains("callback rejected notification"));
        let latest = commit
            .snapshot_manager
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest.id(), 1);
        assert_eq!(
            commit
                .read_delta_entries(None, &latest)
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(io.exists(&data_path).await.unwrap());
        assert_eq!(first.contexts.lock().unwrap().len(), 1);
        assert!(second.contexts.lock().unwrap().is_empty());
        assert_eq!(publisher.attempts.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn commit_callback_only_runs_for_confirmed_snapshots() {
    let io = test_file_io();
    let path = "memory:/callback-empty-rejected";
    setup_dirs(&io, path).await;
    let mut commit = setup_commit(&io, path);
    let callback = Arc::new(RecordingCommitCallback::default());
    commit.add_commit_callback(callback.clone()).unwrap();
    commit.commit_with_identifier(Vec::new(), 1).await.unwrap();
    assert!(callback.contexts.lock().unwrap().is_empty());
    commit
        .clone()
        .with_ignore_empty_commit(false)
        .commit_with_identifier(Vec::new(), 2)
        .await
        .unwrap();
    assert_eq!(callback.contexts.lock().unwrap().len(), 1);
    assert!(callback.contexts.lock().unwrap()[0]
        .commit_entries
        .is_empty());
    commit.commit_max_retries = 0;
    commit.snapshot_commit = Arc::new(CallbackTestPublisher {
        manager: commit.snapshot_manager.clone(),
        behavior: "reject",
        attempts: AtomicUsize::new(0),
    });
    assert!(commit
        .commit_with_identifier(vec![append_message("rejected.parquet")], 3)
        .await
        .is_err());
    assert_eq!(callback.contexts.lock().unwrap().len(), 1);
    assert_eq!(
        commit
            .snapshot_manager
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap()
            .id(),
        1
    );
}
