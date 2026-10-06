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
async fn abort_preserves_all_prepared_message_files() {
    let io = test_file_io();
    let path = "memory:/preserve-all-message-files";
    setup_dirs(&io, path).await;
    let commit = setup_commit(&io, path);
    let mut message = append_message("data.parquet");
    message.new_files[0].extra_files = vec!["data.parquet.index".into()];
    let mut blob = test_data_file("payload.blob", 10);
    blob.external_path = Some(format!("{path}/external/payload.blob"));
    message.new_files.push(blob);
    message.new_changelog_files = vec![test_data_file("changelog.parquet", 10)];
    message.compact_before = vec![test_data_file("old.parquet", 10)];
    message.deleted_files = message.compact_before.clone();
    message.compact_after = vec![test_data_file("compact.parquet", 10)];
    message.compact_changelog_files = vec![test_data_file("compact-changelog.parquet", 10)];
    let mut hash = test_global_index_file("hash", 0, 0, 9);
    hash.index_type = "HASH".into();
    hash.global_index_meta = None;
    message.new_index_files = vec![hash, test_deletion_vector_index_file("dv", "data.parquet")];
    message.compact_new_index_files = vec![test_global_index_file("global", 0, 0, 9)];
    let paths = [
        "bucket-0/data.parquet", "bucket-0/data.parquet.index", "external/payload.blob",
        "bucket-0/changelog.parquet", "bucket-0/old.parquet", "bucket-0/compact.parquet",
        "bucket-0/compact-changelog.parquet", "index/hash", "index/dv", "index/global",
    ].map(|name| format!("{path}/{name}"));
    for file in &paths {
        io.new_output(file).unwrap().write(bytes::Bytes::from_static(b"keep")).await.unwrap();
    }
    for _ in 0..2 {
        commit.abort(std::slice::from_ref(&message)).await.unwrap();
        for file in &paths {
            assert_eq!(io.new_input(file).unwrap().read().await.unwrap(), bytes::Bytes::from_static(b"keep"), "{file}");
        }
    }
    assert!(latest_snapshot(&io, path).await.is_none());
}

#[tokio::test]
async fn abort_preserves_files_after_a_lost_commit_response() {
    for publish_first in [false, true] {
        for identifier in [BATCH_COMMIT_IDENTIFIER, 42] {
            let io = test_file_io();
            let path = format!("memory:/abort-response-loss-{publish_first}-{identifier}");
            setup_dirs(&io, &path).await;
            let mut commit = setup_commit(&io, &path);
            commit.commit_max_retries = 0;
            commit.snapshot_commit = Arc::new(LostResponseCommit {
                manager: commit.snapshot_manager.clone(),
                calls: std::sync::atomic::AtomicUsize::new(0),
                publish_first,
            });
            let mut message = append_message("data.parquet");
            message.new_files[0].extra_files = vec!["data.parquet.index".into()];
            message.new_changelog_files = vec![test_data_file("changelog.parquet", 10)];
            message.new_index_files = vec![test_global_index_file("global", 0, 0, 9)];
            let paths = ["bucket-0/data.parquet", "bucket-0/data.parquet.index", "bucket-0/changelog.parquet", "index/global"]
                .map(|name| format!("{path}/{name}"));
            for file in &paths {
                io.new_output(file).unwrap().write(bytes::Bytes::from_static(b"prepared")).await.unwrap();
            }
            let error = commit.commit_with_identifier(vec![message.clone()], identifier).await.unwrap_err();
            assert!(error.to_string().contains("outcome may be unknown"), "{error}");
            let manifests = manifest_paths(&io, &path).await;
            commit.abort(std::slice::from_ref(&message)).await.unwrap();
            commit.abort(std::slice::from_ref(&message)).await.unwrap();
            for file in &paths {
                assert!(io.exists(file).await.unwrap(), "{file}");
            }
            assert_eq!(manifest_paths(&io, &path).await, manifests);
            assert_eq!(latest_snapshot(&io, &path).await.is_some(), publish_first);
            commit.filter_and_commit_with_identifier(vec![message], identifier).await.unwrap();
            let snapshot = latest_snapshot(&io, &path).await.unwrap();
            assert_eq!(snapshot.id(), 1);
            assert_eq!(snapshot.total_record_count(), Some(10));
            assert_eq!(active_entries(&io, &path, &snapshot).await.len(), 1);
            assert!(snapshot.changelog_manifest_list().is_some());
            assert!(snapshot.index_manifest().is_some());
        }
    }
}

#[tokio::test]
async fn guarded_retry_preserves_already_published_files() {
    let io = test_file_io();
    let path = "memory:/guarded-retry-preserves-files";
    setup_dirs(&io, path).await;
    let commit = setup_commit(&io, path);
    commit.commit(vec![append_message("original")]).await.unwrap();
    let index_path = format!("{path}/index/global");
    io.new_output(&index_path).unwrap().write(bytes::Bytes::from_static(b"index")).await.unwrap();
    let mut message = CommitMessage::new(vec![], 0, vec![]);
    message.new_index_files = vec![test_global_index_file("global", 0, 0, 9)];
    commit.commit_if_latest_snapshot(vec![message.clone()], 1).await.unwrap();
    let snapshot = latest_snapshot(&io, path).await.unwrap();
    let error = commit.commit_if_latest_snapshot(vec![message.clone()], 1).await.unwrap_err();
    assert!(error.to_string().contains("Snapshot changed"), "{error}");
    commit.abort(&[message]).await.unwrap();
    assert!(io.exists(&index_path).await.unwrap());
    assert_eq!(latest_snapshot(&io, path).await.unwrap().id(), snapshot.id());
    assert!(snapshot.index_manifest().is_some());
}
