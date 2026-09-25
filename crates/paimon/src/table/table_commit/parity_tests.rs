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

fn append_message(name: &str) -> CommitMessage {
    CommitMessage::new(vec![], 0, vec![test_data_file(name, 10)])
}

fn dv_message(name: &str, data_file: &str) -> CommitMessage {
    let mut message = CommitMessage::new(vec![], 0, vec![]);
    message.new_index_files = vec![test_deletion_vector_index_file(name, data_file)];
    message
}

#[tokio::test]
async fn small_manifests_remain_unchanged_across_many_commits() {
    let io = test_file_io();
    let path = "memory:/no-small-manifest-merge";
    setup_dirs(&io, path).await;
    let table = test_table_with_options(
        &io,
        path,
        HashMap::from([("manifest.target-file-size".into(), "8 mb".into())]),
    );
    let commit = TableCommit::new(table, "test-user".into());
    let manifest_dir = format!("{path}/manifest");
    let mut original_manifests = Vec::new();
    let mut original_bytes = HashMap::new();

    // Every previous delta must remain an unchanged base manifest across
    // repeated commits, even though all these files fit within the target size.
    for id in 0..35 {
        commit
            .commit(vec![append_message(&format!("data-{id}.parquet"))])
            .await
            .unwrap();
        let snapshot = latest_snapshot(&io, path).await.unwrap();
        let base = ManifestList::read(
            &io,
            &format!("{manifest_dir}/{}", snapshot.base_manifest_list()),
        )
        .await
        .unwrap();
        assert_eq!(
            base, original_manifests,
            "commit {id} rewrote historical manifests"
        );
        let delta = ManifestList::read(
            &io,
            &format!("{manifest_dir}/{}", snapshot.delta_manifest_list()),
        )
        .await
        .unwrap();
        assert_eq!(delta.len(), 1);
        let manifest_path = format!("{manifest_dir}/{}", delta[0].file_name());
        let bytes = io.new_input(&manifest_path).unwrap().read().await.unwrap();
        assert!(original_bytes.insert(manifest_path, bytes).is_none());
        original_manifests.extend(delta);
    }

    assert!(
        original_manifests
            .iter()
            .map(ManifestFileMeta::file_size)
            .sum::<i64>()
            < commit.manifest_target_size
    );
    let stored_manifests = manifest_paths(&io, path)
        .await
        .into_iter()
        .filter(|path| {
            let name = path.rsplit('/').next().unwrap();
            name.starts_with("manifest-") && !name.starts_with("manifest-list-")
        })
        .collect::<HashSet<_>>();
    assert_eq!(stored_manifests, original_bytes.keys().cloned().collect());
    for (path, expected) in original_bytes {
        assert_eq!(io.new_input(&path).unwrap().read().await.unwrap(), expected);
    }
    let snapshot = latest_snapshot(&io, path).await.unwrap();
    assert_eq!(snapshot.total_record_count(), Some(350));
    assert_eq!(active_entries(&io, path, &snapshot).await.len(), 35);
}

#[tokio::test]
async fn duplicate_dv_for_one_data_file_is_rejected() {
    let io = test_file_io();
    let path = "memory:/duplicate-dv";
    setup_dirs(&io, path).await;
    let commit = setup_commit(&io, path);
    commit
        .commit(vec![append_message("data.parquet")])
        .await
        .unwrap();
    let error = commit
        .commit(vec![
            dv_message("dv-a", "data.parquet"),
            dv_message("dv-b", "data.parquet"),
        ])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("deletion vector"), "{error}");
    assert_eq!(latest_snapshot(&io, path).await.unwrap().id(), 1);
}

#[tokio::test]
async fn dv_replacement_rejects_missing_old_index() {
    let io = test_file_io();
    let path = "memory:/stale-dv-replacement";
    setup_dirs(&io, path).await;
    let commit = setup_commit(&io, path);
    commit
        .commit(vec![append_message("data.parquet"), dv_message("current-dv", "data.parquet")])
        .await
        .unwrap();
    let mut message = dv_message("new-dv", "data.parquet");
    message
        .deleted_index_files
        .push(test_deletion_vector_index_file("stale-dv", "data.parquet"));
    assert!(commit.commit(vec![message]).await.is_err());
    assert_eq!(latest_snapshot(&io, path).await.unwrap().id(), 1);
}

#[tokio::test]
async fn dv_replacement_is_overwrite_and_preserves_unrelated_vectors() {
    let io = test_file_io();
    let path = "memory:/dv-replacement";
    setup_dirs(&io, path).await;
    let commit = setup_commit(&io, path);
    commit
        .commit(vec![
            append_message("data-a"),
            append_message("data-b"),
            dv_message("old", "data-a"),
            dv_message("other", "data-b"),
        ])
        .await
        .unwrap();
    let mut message = dv_message("new", "data-a");
    message
        .deleted_index_files
        .push(test_deletion_vector_index_file("old", "data-a"));
    commit.commit(vec![message]).await.unwrap();
    let snapshot = latest_snapshot(&io, path).await.unwrap();
    assert_eq!(snapshot.commit_kind(), &CommitKind::OVERWRITE);
    let entries =
        TableCommit::read_prev_index_entries(&io, &format!("{path}/manifest"), &Some(snapshot))
            .await
            .unwrap();
    let names = entries
        .iter()
        .map(|entry| entry.index_file.file_name.as_str())
        .collect::<HashSet<_>>();
    assert_eq!(names, HashSet::from(["new", "other"]));
}

#[tokio::test]
async fn unrelated_dv_delete_does_not_bypass_indexed_column_policy() {
    for action in ["THROW_ERROR", "DROP_PARTITION_INDEX"] {
        let io = test_file_io();
        let path = format!("memory:/indexed-update-{action}");
        setup_dirs(&io, &path).await;
        let table = test_table_with_options(
            &io,
            &path,
            HashMap::from([(
                "global-index.column-update-action".to_string(),
                action.to_string(),
            )]),
        );
        let commit = TableCommit::new(table, "test-user".into());
        let mut initial = dv_message("old-dv", "data");
        initial.new_files.push(test_data_file("data", 10));
        initial
            .new_index_files
            .push(test_global_index_file("global", 0, 0, 9));
        commit.commit(vec![initial]).await.unwrap();
        let mut update = append_message("update.parquet");
        update.new_files[0].write_cols = Some(vec!["id".into()]);
        update.new_files[0].first_row_id = Some(0);
        let mut delete = dv_message("new-dv", "data");
        delete
            .deleted_index_files
            .push(test_deletion_vector_index_file("old-dv", "data"));
        let result = commit.commit(vec![update, delete]).await;
        if action == "THROW_ERROR" {
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("globally indexed columns"));
            assert_eq!(latest_snapshot(&io, &path).await.unwrap().id(), 1);
        } else {
            result.unwrap();
            let snapshot = latest_snapshot(&io, &path).await;
            let entries =
                TableCommit::read_prev_index_entries(&io, &format!("{path}/manifest"), &snapshot)
                    .await
                    .unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].index_file.file_name, "new-dv");
        }
    }
}

async fn save_schema(io: &FileIO, path: &str, id: i64) {
    let mut json = serde_json::to_value(test_schema()).unwrap();
    json["id"] = id.into();
    io.new_output(&format!("{path}/schema/schema-{id}"))
        .unwrap()
        .write(serde_json::to_vec(&json).unwrap().into())
        .await
        .unwrap();
}

#[tokio::test]
async fn snapshot_uses_latest_schema_but_files_keep_writer_schema() {
    let io = test_file_io();
    let path = "memory:/latest-schema";
    setup_dirs(&io, path).await;
    let commit = setup_commit(&io, path);
    save_schema(&io, path, 1).await;
    commit
        .commit(vec![append_message("old-writer.parquet")])
        .await
        .unwrap();
    let snapshot = latest_snapshot(&io, path).await.unwrap();
    assert_eq!(snapshot.schema_id(), 1);
    assert_eq!(
        active_entries(&io, path, &snapshot).await[0]
            .file()
            .schema_id,
        0
    );
}

#[tokio::test]
async fn index_commit_inherits_watermark_and_schema_compatible_statistics() {
    let io = test_file_io();
    let path = "memory:/inherited-metadata";
    setup_dirs(&io, path).await;
    let commit = setup_commit(&io, path);
    commit
        .commit(vec![append_message("data.parquet")])
        .await
        .unwrap();
    let mut initial = serde_json::to_value(latest_snapshot(&io, path).await.unwrap()).unwrap();
    initial["watermark"] = 1234.into();
    initial["statistics"] = "stats-1".into();
    io.new_output(&format!("{path}/snapshot/snapshot-1"))
        .unwrap()
        .write(serde_json::to_vec(&initial).unwrap().into())
        .await
        .unwrap();
    io.new_output(&format!("{path}/statistics/stats-1"))
        .unwrap()
        .write(bytes::Bytes::from_static(
            br#"{"schemaId":0,"snapshotId":1,"colStats":{}}"#,
        ))
        .await
        .unwrap();
    let mut index = CommitMessage::new(vec![], 0, vec![]);
    index
        .new_index_files
        .push(test_global_index_file("global", 0, 0, 9));
    commit.commit(vec![index]).await.unwrap();
    let snapshot = latest_snapshot(&io, path).await.unwrap();
    assert_eq!(snapshot.watermark(), Some(1234));
    assert_eq!(snapshot.statistics(), Some("stats-1"));

    save_schema(&io, path, 1).await;
    // Older writers may carry statistics across a schema change. Even if the
    // snapshot already uses the latest schema, the statistics' own schema must match.
    let mut stale_statistics = serde_json::to_value(&snapshot).unwrap();
    stale_statistics["schemaId"] = 1.into();
    io.new_output(&format!("{path}/snapshot/snapshot-2"))
        .unwrap()
        .write(serde_json::to_vec(&stale_statistics).unwrap().into())
        .await
        .unwrap();
    commit
        .commit(vec![append_message("new.parquet")])
        .await
        .unwrap();
    let snapshot = latest_snapshot(&io, path).await.unwrap();
    assert_eq!(snapshot.watermark(), Some(1234));
    assert_eq!(snapshot.statistics(), None);
}

struct LostResponseCommit {
    manager: SnapshotManager,
    calls: std::sync::atomic::AtomicUsize,
    publish_first: bool,
}

#[async_trait::async_trait]
impl SnapshotCommit for LostResponseCommit {
    async fn commit(&self, _: Option<&str>, snapshot: &Snapshot, _: &[PartitionStatistics]) -> Result<bool> {
        let attempt = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if attempt == 0 {
            if self.publish_first {
                assert!(self.manager.commit_snapshot(snapshot).await?);
            }
            return Err(crate::Error::IoUnsupported {
                message: "lost commit response".into(),
            });
        }
        self.manager.commit_snapshot(snapshot).await
    }
}

#[tokio::test]
async fn publication_errors_retry_without_duplicate_snapshots() {
    for publish_first in [false, true] {
        let io = test_file_io();
        let path = format!("memory:/publication-error-{publish_first}");
        setup_dirs(&io, &path).await;
        let mut commit = setup_commit(&io, &path);
        commit.commit_min_retry_wait_ms = 0;
        commit.commit_max_retry_wait_ms = 0;
        let publisher = Arc::new(LostResponseCommit {
            manager: commit.snapshot_manager.clone(),
            calls: std::sync::atomic::AtomicUsize::new(0),
            publish_first,
        });
        commit.snapshot_commit = publisher.clone();
        commit
            .commit_with_identifier(vec![append_message("data.parquet")], 42)
            .await
            .unwrap();
        let snapshot = latest_snapshot(&io, &path).await.unwrap();
        assert_eq!(snapshot.id(), 1);
        assert_eq!(snapshot.total_record_count(), Some(10));
        assert_eq!(
            publisher.calls.load(std::sync::atomic::Ordering::SeqCst),
            if publish_first { 1 } else { 2 }
        );
        assert_eq!(active_entries(&io, &path, &snapshot).await.len(), 1);
    }
}

#[tokio::test]
async fn mixed_append_cannot_bypass_stale_dv_check() {
    let io = test_file_io();
    let path = "memory:/mixed-stale-dv";
    setup_dirs(&io, path).await;
    let commit = setup_data_evolution_commit(&io, path);
    for name in ["data", "concurrent"] {
        let mut message = append_message(name);
        message.new_files[0].file_source = Some(0);
        if name == "concurrent" {
            message.new_index_files = vec![test_deletion_vector_index_file("first-dv", "data")];
        }
        commit.commit(vec![message]).await.unwrap();
    }
    let mut delete = dv_message("dv", "data");
    delete.check_from_snapshot = Some(1);
    let mut append = append_message("another-file");
    append.new_files[0].file_source = Some(0);
    let error = commit.commit(vec![append, delete]).await.unwrap_err();
    assert!(
        error.to_string().contains("Conflicting deletion vectors"),
        "{error}"
    );
    assert_eq!(latest_snapshot(&io, path).await.unwrap().id(), 2);
}

async fn manifest_paths(io: &FileIO, path: &str) -> HashSet<String> {
    io.list_status(&format!("{path}/manifest/"))
        .await
        .unwrap()
        .into_iter()
        .filter(|status| !status.is_dir)
        .map(|status| status.path)
        .collect()
}

#[tokio::test]
async fn preparation_failure_cleans_new_metadata_and_preserves_old_snapshots() {
    let io = test_file_io();
    let path = "memory:/prepare-failure";
    setup_dirs(&io, path).await;
    let table = test_table_with_options(
        &io,
        path,
        HashMap::from([("manifest.sidecar.enabled".into(), "true".into())]),
    );
    let commit = TableCommit::new(table, "test-user".into());
    for name in ["first", "second"] {
        commit.commit(vec![append_message(name)]).await.unwrap();
    }
    let before = manifest_paths(&io, path).await;
    let original = latest_snapshot(&io, path).await.unwrap();
    let mut value = serde_json::to_value(&original).unwrap();
    value["statistics"] = "invalid-statistics".into();
    io.new_output(&format!("{path}/snapshot/snapshot-2"))
        .unwrap()
        .write(serde_json::to_vec(&value).unwrap().into())
        .await
        .unwrap();
    io.new_output(&format!("{path}/statistics/invalid-statistics"))
        .unwrap()
        .write(bytes::Bytes::from_static(b"broken JSON"))
        .await
        .unwrap();
    let mut message = append_message("pending");
    message
        .new_changelog_files
        .push(test_data_file("changelog", 10));
    message
        .new_index_files
        .push(test_global_index_file("pending-index", 0, 0, 9));
    let error = commit.commit(vec![message]).await.unwrap_err();
    assert!(
        error.to_string().contains("Invalid statistics metadata"),
        "{error}"
    );
    assert_eq!(manifest_paths(&io, path).await, before, "failed preparation must clean lists, delta/changelog manifests, sidecars and index manifests");
    assert_eq!(active_entries(&io, path, &original).await.len(), 2);
    assert_eq!(latest_snapshot(&io, path).await.unwrap().id(), 2);
}

#[derive(Debug)]
struct FailSecondManifest {
    operator: opendal::Operator,
    failed: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl crate::io::FileIOProvider for FailSecondManifest {
    async fn create(&self, path: &str) -> Result<(opendal::Operator, String)> {
        let name = path.rsplit('/').next().unwrap();
        if name.starts_with("manifest-")
            && !name.starts_with("manifest-list-")
            && name.ends_with("-1")
            && !self.failed.swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(crate::Error::IoUnsupported {
                message: "injected second manifest write failure".into(),
            });
        }
        Ok((
            self.operator.clone(),
            path.trim_start_matches("memory:/").to_string(),
        ))
    }
}

#[tokio::test]
async fn rolling_manifest_failure_cleans_previous_chunks_and_sidecars() {
    let provider = Arc::new(FailSecondManifest {
        operator: opendal::Operator::new(opendal::services::Memory::default()).unwrap(),
        failed: std::sync::atomic::AtomicBool::new(false),
    });
    let io = FileIOBuilder::new("memory")
        .with_provider(provider.clone())
        .build()
        .unwrap();
    let path = "memory:/rolling-failure";
    setup_dirs(&io, path).await;
    let table = test_table_with_options(
        &io,
        path,
        HashMap::from([
            ("manifest.sidecar.enabled".into(), "true".into()),
            ("manifest.target-file-size".into(), "1 b".into()),
        ]),
    );
    let commit = TableCommit::new(table, "test-user".into());
    let files = (0..2001)
        .map(|id| test_data_file(&format!("{id}.parquet"), 1))
        .collect();
    let error = commit
        .commit(vec![CommitMessage::new(vec![], 0, files)])
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("injected second manifest"),
        "{error}"
    );
    assert!(provider.failed.load(std::sync::atomic::Ordering::SeqCst));
    assert!(manifest_paths(&io, path).await.is_empty());
    assert!(latest_snapshot(&io, path).await.is_none());
}

#[tokio::test]
async fn uncertain_guarded_commit_does_not_abort_published_index_files() {
    let io = test_file_io();
    let path = "memory:/uncertain-index";
    setup_dirs(&io, path).await;
    let mut commit = setup_commit(&io, path);
    commit.commit(vec![append_message("data")]).await.unwrap();
    commit.commit_max_retries = 0;
    commit.snapshot_commit = Arc::new(LostResponseCommit {
        manager: commit.snapshot_manager.clone(),
        calls: std::sync::atomic::AtomicUsize::new(0),
        publish_first: true,
    });
    let index_path = format!("{path}/index/global");
    io.new_output(&index_path)
        .unwrap()
        .write(bytes::Bytes::from_static(b"index"))
        .await
        .unwrap();
    let mut index = CommitMessage::new(vec![], 0, vec![]);
    index
        .new_index_files
        .push(test_global_index_file("global", 0, 0, 9));
    let error = commit
        .commit_if_latest_snapshot_with_identifier(vec![index.clone()], 1, 42)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("outcome may be unknown"),
        "{error}"
    );
    assert!(io.exists(&index_path).await.unwrap());
    let snapshot = latest_snapshot(&io, path).await.unwrap();
    assert_eq!(snapshot.id(), 2);
    assert!(snapshot.index_manifest().is_some());
    commit
        .filter_and_commit_with_identifier(vec![index], 42)
        .await
        .unwrap();
    assert_eq!(latest_snapshot(&io, path).await.unwrap().id(), 2);
}

#[tokio::test]
async fn dv_publication_response_loss_uses_overwrite_identity() {
    let io = test_file_io();
    let path = "memory:/dv-response-loss";
    setup_dirs(&io, path).await;
    let mut commit = setup_commit(&io, path);
    commit.commit_min_retry_wait_ms = 0;
    commit.commit_max_retry_wait_ms = 0;
    let publisher = Arc::new(LostResponseCommit {
        manager: commit.snapshot_manager.clone(),
        calls: std::sync::atomic::AtomicUsize::new(0),
        publish_first: true,
    });
    commit.snapshot_commit = publisher.clone();
    commit
        .commit_with_identifier(vec![append_message("data"), dv_message("dv", "data")], 7)
        .await
        .unwrap();
    assert_eq!(
        latest_snapshot(&io, path).await.unwrap().commit_kind(),
        &CommitKind::OVERWRITE
    );
    assert_eq!(publisher.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn rest_commit_uses_catalog_snapshot_schema_and_retry_identity() {
    use crate::api::rest_api::RESTApi;
    use crate::common::Options;
    use axum::{
        body::Bytes,
        http::{Method, Uri},
        Json, Router,
    };
    use std::sync::Mutex;
    let io = test_file_io();
    let path = "memory:/rest-commit-parity";
    setup_dirs(&io, path).await;
    let seed = setup_commit(&io, path);
    seed.commit(vec![append_message("original")]).await.unwrap();
    let mut value = serde_json::to_value(latest_snapshot(&io, path).await.unwrap()).unwrap();
    value["id"] = 7.into();
    let snapshot = Arc::new(Mutex::new(
        serde_json::from_value::<Snapshot>(value).unwrap(),
    ));
    let posts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let handler_snapshot = snapshot.clone();
    let handler_posts = posts.clone();
    let app = Router::new().fallback(move |method: Method, uri: Uri, body: Bytes| {
        let snapshot = handler_snapshot.clone();
        let posts = handler_posts.clone();
        async move {
            let response = if method == Method::POST && uri.path().ends_with("/commit") {
                posts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let next: Snapshot = serde_json::from_value(request["snapshot"].clone()).unwrap();
                assert_eq!(next.id(), snapshot.lock().unwrap().id() + 1);
                *snapshot.lock().unwrap() = next;
                // Force the retry/deduplication path after catalog publication.
                serde_json::json!({"success": false})
            } else if uri.path().ends_with("/snapshot") {
                serde_json::json!({"snapshot": {"snapshot": *snapshot.lock().unwrap(), "recordCount": 10}})
            } else {
                serde_json::json!({"schemaId": 3})
            };
            Json(response)
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut options = Options::new();
    options.set("uri", format!("http://{}", listener.local_addr().unwrap()));
    options.set("prefix", "test");
    options.set("token.provider", "bear");
    options.set("token", "test-token");
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let api = Arc::new(RESTApi::new(options.clone(), false).await.unwrap());
    let identifier = Identifier::new("database", "table");
    let env =
        crate::table::RESTEnv::new(identifier.clone(), "uuid".into(), api, options, false, None);
    let table = Table::new(
        io.clone(),
        identifier,
        path.into(),
        test_schema(),
        Some(env),
    );
    let mut commit = TableCommit::new(table, "rest-writer".into());
    commit.commit_min_retry_wait_ms = 0;
    commit.commit_max_retry_wait_ms = 0;
    let messages = vec![append_message("new")];
    commit
        .commit_if_latest_snapshot_with_identifier(messages.clone(), 7, 42)
        .await
        .unwrap();
    commit
        .filter_and_commit_with_identifier(messages, 42)
        .await
        .unwrap();
    let latest = snapshot.lock().unwrap().clone();
    assert_eq!(latest.id(), 8);
    assert_eq!(latest.schema_id(), 3);
    assert_eq!(latest.total_record_count(), Some(20));
    assert_eq!(posts.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(!io
        .exists(&format!("{path}/snapshot/snapshot-8"))
        .await
        .unwrap());
    assert_eq!(active_entries(&io, path, &latest).await.len(), 2);
    server.abort();
}
