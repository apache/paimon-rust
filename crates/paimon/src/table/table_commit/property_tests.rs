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

#[tokio::test]
async fn snapshot_properties_are_not_inherited_between_commits() {
    let io = test_file_io();
    let path = "memory:/snapshot-call-properties";
    setup_dirs(&io, path).await;
    let commit = setup_commit(&io, path).with_ignore_empty_commit(false);
    let properties = HashMap::from([
        ("source".into(), "capture".into()),
        ("label".into(), "中文".into()),
    ]);
    let configured = commit.clone().with_snapshot_properties(properties.clone());
    configured
        .commit_with_identifier(vec![append_message("data.parquet")], 1)
        .await
        .unwrap();
    let first = latest_snapshot(&io, path).await.unwrap();
    assert_eq!(first.properties(), Some(&properties));

    // Empty snapshots also carry current-call metadata, not inherited values.
    commit.commit_with_identifier(Vec::new(), 2).await.unwrap();
    let second = latest_snapshot(&io, path).await.unwrap();
    assert_eq!(second.properties(), None);
    assert!(serde_json::to_value(&second)
        .unwrap()
        .get("properties")
        .is_none());

    configured
        .clone()
        .with_snapshot_properties(HashMap::new())
        .commit_with_identifier(Vec::new(), 3)
        .await
        .unwrap();
    assert_eq!(latest_snapshot(&io, path).await.unwrap().properties(), None);

    configured
        .commit_with_identifier(Vec::new(), 4)
        .await
        .unwrap();
    let fourth = latest_snapshot(&io, path).await.unwrap();
    assert_eq!(fourth.properties(), Some(&properties));
    assert_eq!(fourth.delta_record_count(), Some(0));
    assert_eq!(fourth.total_record_count(), first.total_record_count());
}

#[tokio::test]
async fn snapshot_properties_overwrite_recovery_preserves_original_metadata() {
    let io = test_file_io();
    let path = "memory:/snapshot-overwrite-properties";
    setup_dirs(&io, path).await;
    let commit = setup_commit(&io, path);
    commit
        .commit_with_identifier(vec![append_message("old.parquet")], 1)
        .await
        .unwrap();
    let properties = HashMap::from([("source".into(), "overwrite".into())]);
    let configured = commit.clone().with_snapshot_properties(properties.clone());
    configured
        .overwrite_with_identifier(Vec::new(), None, 2)
        .await
        .unwrap();
    let snapshot = latest_snapshot(&io, path).await.unwrap();
    assert_eq!(snapshot.properties(), Some(&properties));
    assert_eq!(snapshot.commit_kind(), &CommitKind::OVERWRITE);
    assert_eq!(snapshot.total_record_count(), Some(0));

    let other = TableCommit::new(test_table(&io, path), "other-user".into());
    other
        .commit_with_identifier(vec![append_message("intervening.parquet")], 1)
        .await
        .unwrap();
    // Recovered identities retain their original metadata and do not apply
    // the caller's new properties or remove an intervening writer's data.
    configured
        .with_snapshot_properties(HashMap::from([("source".into(), "retry".into())]))
        .overwrite_with_identifier(Vec::new(), None, 2)
        .await
        .unwrap();
    let latest = latest_snapshot(&io, path).await.unwrap();
    assert_eq!(latest.id(), 3);
    assert_eq!(latest.properties(), None);
    assert_eq!(active_entries(&io, path, &latest).await.len(), 1);
    assert_eq!(
        commit
            .snapshot_manager
            .get_snapshot(2)
            .await
            .unwrap()
            .properties(),
        Some(&properties)
    );
}

struct PropertyPublicationFault {
    manager: SnapshotManager,
    response_lost: bool,
    calls: std::sync::atomic::AtomicUsize,
    attempts: std::sync::Mutex<Vec<Snapshot>>,
}

#[async_trait::async_trait]
impl SnapshotCommit for PropertyPublicationFault {
    async fn commit(
        &self,
        _base_snapshot_uuid: Option<&str>,
        snapshot: &Snapshot,
        _branch: &str,
        _statistics: &[PartitionStatistics],
    ) -> Result<bool> {
        self.attempts.lock().unwrap().push(snapshot.clone());
        let first = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0;
        if first && !self.response_lost {
            return Ok(false);
        }
        let published = self.manager.commit_snapshot(snapshot).await?;
        if first && self.response_lost {
            return Err(crate::Error::UnexpectedError {
                message: "response lost after publication".into(),
                source: None,
            });
        }
        Ok(published)
    }
}

#[tokio::test]
async fn snapshot_properties_survive_conflict_and_uncertain_publication() {
    for response_lost in [false, true] {
        let io = test_file_io();
        let path = format!("memory:/snapshot-properties-fault-{response_lost}");
        setup_dirs(&io, &path).await;
        let mut commit = setup_commit(&io, &path);
        commit.commit_min_retry_wait_ms = 0;
        commit.commit_max_retry_wait_ms = 0;
        let fault = Arc::new(PropertyPublicationFault {
            manager: commit.snapshot_manager.clone(),
            response_lost,
            calls: std::sync::atomic::AtomicUsize::new(0),
            attempts: std::sync::Mutex::new(Vec::new()),
        });
        commit.snapshot_commit = fault.clone();
        let data_path = format!("{path}/bucket-0/prepared.parquet");
        io.new_output(&data_path)
            .unwrap()
            .write(bytes::Bytes::from_static(b"prepared file retained"))
            .await
            .unwrap();
        let properties = HashMap::from([("source".into(), "retried".into())]);
        commit
            .with_snapshot_properties(properties.clone())
            .commit_with_identifier(vec![append_message("prepared.parquet")], 7)
            .await
            .unwrap();
        let snapshot = latest_snapshot(&io, &path).await.unwrap();
        assert_eq!(snapshot.id(), 1);
        assert_eq!(snapshot.properties(), Some(&properties));
        assert_eq!(active_entries(&io, &path, &snapshot).await.len(), 1);
        assert!(io.exists(&data_path).await.unwrap());
        let attempts = fault.attempts.lock().unwrap();
        assert_eq!(attempts.len(), if response_lost { 1 } else { 2 });
        assert!(attempts
            .iter()
            .all(|attempt| attempt.properties() == Some(&properties)));
    }
}
