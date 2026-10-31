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
async fn global_index_delete_rejects_a_previously_removed_file() {
    let io = test_file_io();
    let path = "memory:/global-index-stale-delete";
    setup_dirs(&io, path).await;
    let mut commit = setup_commit(&io, path);
    let callback = Arc::new(RecordingCommitCallback::default());
    commit.add_commit_callback(callback.clone()).unwrap();
    let file = test_global_index_file("global.index", 0, 0, 9);
    let physical = format!("{path}/index/{}", file.file_name);
    io.new_output(&physical)
        .unwrap()
        .write(bytes::Bytes::from_static(b"retained index"))
        .await
        .unwrap();
    let mut message = append_message("data.parquet");
    message.new_index_files.push(file.clone());
    commit.commit(vec![message]).await.unwrap();
    let mut delete = CommitMessage::new(vec![], 0, vec![]);
    delete.deleted_index_files.push(file);
    commit
        .commit_with_identifier(vec![delete.clone()], 2)
        .await
        .unwrap();
    let snapshot = latest_snapshot(&io, path).await.unwrap();
    assert_eq!(snapshot.id(), 2);
    let error = commit
        .commit_with_identifier(vec![delete], 3)
        .await
        .expect_err("a stale global index DELETE must fail, not become an empty commit");
    assert!(error.to_string().contains("does not exist"), "{error}");
    assert_eq!(latest_snapshot(&io, path).await.unwrap(), snapshot);
    assert_eq!(callback.contexts.lock().unwrap().len(), 2);
    assert!(io.exists(&physical).await.unwrap());
    assert_eq!(active_entries(&io, path, &snapshot).await.len(), 1);
}

#[tokio::test]
async fn global_index_delete_checks_complete_current_metadata() {
    for mismatch in [
        "duplicate",
        "type",
        "bucket",
        "size",
        "row-count",
        "external-path",
        "row-range",
        "field-id",
        "extra-fields",
        "empty-extra-fields",
        "index-meta",
        "source-meta",
    ] {
        let io = test_file_io();
        let path = format!("memory:/global-index-delete-{mismatch}");
        setup_dirs(&io, &path).await;
        let mut commit = setup_commit(&io, &path);
        let callback = Arc::new(RecordingCommitCallback::default());
        commit.add_commit_callback(callback.clone()).unwrap();
        let original = test_global_index_file("global.index", 0, 0, 9);
        let physical = format!("{path}/index/{}", original.file_name);
        io.new_output(&physical)
            .unwrap()
            .write(bytes::Bytes::from_static(b"retained index"))
            .await
            .unwrap();
        let mut seed = append_message("data.parquet");
        seed.new_index_files.push(original.clone());
        commit.commit(vec![seed]).await.unwrap();
        let snapshot = latest_snapshot(&io, &path).await.unwrap();
        let mut file = original.clone();
        let mut delete = CommitMessage::new(vec![], 0, vec![]);
        match mismatch {
            "duplicate" => delete.deleted_index_files.push(original.clone()),
            "type" => file.index_type = "btree".into(),
            "bucket" => delete.bucket = 1,
            "size" => file.file_size += 1,
            "row-count" => file.row_count += 1,
            "external-path" => file.external_path = Some(format!("{physical}.other")),
            "row-range" => file.global_index_meta.as_mut().unwrap().row_range_start += 1,
            "field-id" => file.global_index_meta.as_mut().unwrap().index_field_id += 1,
            "extra-fields" => {
                file.global_index_meta.as_mut().unwrap().extra_field_ids = Some(vec![1])
            }
            "empty-extra-fields" => {
                file.global_index_meta.as_mut().unwrap().extra_field_ids = Some(vec![])
            }
            "index-meta" => file.global_index_meta.as_mut().unwrap().index_meta = Some(vec![1]),
            "source-meta" => file.global_index_meta.as_mut().unwrap().source_meta = Some(vec![1]),
            _ => unreachable!(),
        }
        delete.deleted_index_files.push(file);
        let error = commit
            .commit_with_identifier(vec![delete], 2)
            .await
            .expect_err("a metadata-stale global index DELETE must fail");
        let expected = if matches!(mismatch, "duplicate" | "type") {
            "does not exist"
        } else {
            "metadata does not match"
        };
        assert!(error.to_string().contains(expected), "{mismatch}: {error}");
        assert_eq!(
            latest_snapshot(&io, &path).await.unwrap(),
            snapshot,
            "{mismatch}"
        );
        assert_eq!(callback.contexts.lock().unwrap().len(), 1, "{mismatch}");
        assert!(io.exists(&physical).await.unwrap(), "{mismatch}");
        let entries = IndexManifest::read(
            &io,
            &format!("{path}/manifest/{}", snapshot.index_manifest().unwrap()),
        )
        .await
        .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].index_file, original);
    }
}

#[test]
fn stale_hash_index_deletes_remain_tolerated() {
    let mut file = test_global_index_file("hash", 0, 0, 9);
    file.index_type = "HASH".into();
    file.global_index_meta = None;
    let mut deletion = add_index_entry(file);
    deletion.kind = FileKind::Delete;
    assert!(TableCommit::merge_index_entries(
        vec![],
        &[deletion],
        index_commit::IndexDomain::GlobalRowIds
    )
    .unwrap()
    .is_empty());
}

#[tokio::test]
async fn global_index_delete_recovers_a_successful_publication_before_validating_again() {
    for behavior in ["lose-response", "conflict"] {
        let io = test_file_io();
        let path = format!("memory:/global-index-delete-recovery-{behavior}");
        setup_dirs(&io, &path).await;
        let mut commit = setup_commit(&io, &path);
        commit.commit_min_retry_wait_ms = 0;
        commit.commit_max_retry_wait_ms = 0;
        let file = test_global_index_file("global.index", 0, 0, 9);
        let physical = format!("{path}/index/{}", file.file_name);
        io.new_output(&physical)
            .unwrap()
            .write(bytes::Bytes::from_static(b"retained index"))
            .await
            .unwrap();
        let mut seed = append_message("data.parquet");
        seed.new_index_files.push(file.clone());
        commit.commit(vec![seed]).await.unwrap();
        let before = latest_snapshot(&io, &path).await.unwrap();
        let callback = Arc::new(RecordingCommitCallback::default());
        commit.add_commit_callback(callback.clone()).unwrap();
        let publisher = Arc::new(CallbackTestPublisher {
            manager: commit.snapshot_manager.clone(),
            behavior,
            attempts: AtomicUsize::new(0),
        });
        commit.snapshot_commit = publisher.clone();
        let mut delete = CommitMessage::new(vec![], 0, vec![]);
        delete.deleted_index_files.push(file.clone());
        commit
            .commit_with_identifier(vec![delete], 2)
            .await
            .unwrap();
        let snapshot = latest_snapshot(&io, &path).await.unwrap();
        assert_eq!(snapshot.id(), 2);
        assert!(snapshot.index_manifest().is_none());
        assert_eq!(
            publisher.attempts.load(Ordering::SeqCst),
            if behavior == "lose-response" { 1 } else { 2 }
        );
        {
            let contexts = callback.contexts.lock().unwrap();
            assert_eq!(contexts.len(), 1);
            assert_eq!(contexts[0].snapshot, snapshot);
            assert!(contexts[0].commit_entries.is_empty());
        }
        assert!(io.exists(&physical).await.unwrap());
        let old_entries = IndexManifest::read(
            &io,
            &format!("{path}/manifest/{}", before.index_manifest().unwrap()),
        )
        .await
        .unwrap();
        assert_eq!(old_entries.len(), 1);
        assert_eq!(old_entries[0].index_file, file);
    }
}
