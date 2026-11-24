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

#[test]
fn row_id_delete_must_match_the_current_file_layout() {
    let commit = setup_data_evolution_commit(&test_file_io(), "memory:/row_id_delete_layout");
    let mut file = test_data_file("data-reassigned.parquet", 10);
    file.first_row_id = Some(10);
    let current = ManifestEntry::new(FileKind::Add, vec![], 0, -1, file.clone(), 2);
    file.first_row_id = Some(0);
    let stale_delete = ManifestEntry::new(FileKind::Delete, vec![], 0, -1, file, 2);
    let error = commit
        .check_row_id_existence(&[current], &[stale_delete], Some(20), &CommitKind::APPEND)
        .expect_err("a DELETE must not cancel the current file after row-ID reassignment");
    assert!(error.to_string().contains("Row ID existence conflict"));
    assert!(matches!(
        error,
        crate::Error::RowIdExistenceConflict {
            first_row_id: Some(0),
            row_count: 10,
            bucket: 0,
            current_first_row_id: Some(10),
            current_row_count: Some(10),
            ..
        }
    ));
}

#[test]
fn row_id_existence_preserves_missing_range_details() {
    let commit = setup_data_evolution_commit(&test_file_io(), "memory:/missing_row_id_range");
    let mut file = test_data_file("opaque '(4): file.parquet", 4);
    file.first_row_id = Some(7);
    let entry = ManifestEntry::new(FileKind::Add, vec![], 3, -1, file, 2);
    let error = commit
        .check_row_id_existence(&[], &[entry], Some(20), &CommitKind::APPEND)
        .unwrap_err();
    assert!(matches!(error, crate::Error::RowIdExistenceConflict {
        ref file_name, first_row_id: Some(7), row_count: 4, bucket: 3,
        current_first_row_id: None, current_row_count: None
    } if file_name == "opaque '(4): file.parquet"));
}

#[test]
fn compact_row_id_existence_requires_contiguous_normal_coverage() {
    let commit = setup_data_evolution_commit(&test_file_io(), "memory:/compact_range_existence");
    let entry = |name: &str, first: i64, count: i64| {
        let mut file = test_data_file(name, count);
        file.first_row_id = Some(first);
        ManifestEntry::new(FileKind::Add, vec![], 0, -1, file, 2)
    };
    let output = entry("compact.parquet", 0, 10);
    let contiguous = [entry("a.parquet", 0, 5), entry("b.parquet", 5, 5)];
    commit.check_row_id_existence(&contiguous, std::slice::from_ref(&output), Some(10), &CommitKind::COMPACT).unwrap();
    assert!(commit.check_row_id_existence(&contiguous, std::slice::from_ref(&output), Some(10), &CommitKind::APPEND).is_err(), "DML must still match an exact source range");
    let gap = [entry("a.parquet", 0, 4), entry("b.parquet", 5, 5), entry("stale.blob", 0, 10)];
    assert!(commit.check_row_id_existence(&gap, &[output], Some(10), &CommitKind::COMPACT).is_err(), "neither a gap nor a dedicated file can supply normal row slots");
}

#[test]
fn row_id_delete_metadata_check_does_not_require_next_row_id() {
    let commit = setup_data_evolution_commit(&test_file_io(), "memory:/delete_without_counter");
    for (current_id, current_count) in [(None, 10), (Some(0), 11)] {
        let mut file = test_data_file("same.parquet", current_count);
        file.first_row_id = current_id;
        let current = ManifestEntry::new(FileKind::Add, vec![], 0, -1, file, 2);
        let mut old = test_data_file("same.parquet", 10);
        old.first_row_id = Some(0);
        let old = ManifestEntry::new(FileKind::Delete, vec![], 0, -1, old, 2);
        assert!(
            matches!(commit.check_row_id_existence(&[current], &[old], None, &CommitKind::APPEND),
            Err(crate::Error::RowIdExistenceConflict { current_first_row_id, current_row_count: Some(count), .. })
            if current_first_row_id == current_id && count == current_count)
        );
    }
    let file = test_data_file("ordinary.parquet", 10);
    let current = ManifestEntry::new(FileKind::Add, vec![], 0, -1, file.clone(), 2);
    let delete = ManifestEntry::new(FileKind::Delete, vec![], 0, -1, file, 2);
    commit
        .check_row_id_existence(&[current], &[delete], None, &CommitKind::APPEND)
        .unwrap();
    commit.check_row_id_existence(&[], &[], None, &CommitKind::APPEND).unwrap();
}

fn row_id_check_message(name: &str, column: &str) -> CommitMessage {
    let mut file = test_data_file(name, 10);
    file.first_row_id = Some(0);
    file.file_source = Some(0);
    file.write_cols = Some(vec![column.into()]);
    let mut message = CommitMessage::new(EMPTY_SERIALIZED_ROW.clone(), 0, vec![file]);
    message.check_from_snapshot = Some(1);
    message
}

struct ConcurrentManifestReplacement {
    manager: SnapshotManager,
    replacement: Snapshot,
}

#[async_trait::async_trait]
impl SnapshotCommit for ConcurrentManifestReplacement {
    async fn commit(
        &self,
        _: Option<&str>,
        snapshot: &Snapshot,
        _: &str,
        _: &[PartitionStatistics],
    ) -> Result<bool> {
        if snapshot.id() == self.replacement.id() {
            assert!(self.manager.commit_snapshot(&self.replacement).await?);
            return Ok(false);
        }
        self.manager.commit_snapshot(snapshot).await
    }
}

async fn reassigned_snapshot(
    commit: &TableCommit,
    initial: &Snapshot,
    id: i64,
) -> (Snapshot, Vec<ManifestEntry>) {
    let io = commit.table.file_io();
    let mut reassigned = active_entries(io, commit.table.location(), initial).await;
    assert_eq!(reassigned[0].file().first_row_id, Some(0));
    reassigned[0].file.first_row_id = Some(10);

    // Java row-ID reassignment replaces the base manifest and publishes an empty delta.
    let manifest_dir = commit.snapshot_manager.manifest_dir();
    let base_metas = commit
        .write_manifest_files(
            io,
            &manifest_dir,
            "reassigned",
            &reassigned,
            &mut Vec::new(),
        )
        .await
        .unwrap();
    let base_list = "manifest-list-reassigned-base";
    let delta_list = "manifest-list-reassigned-delta";
    ManifestList::write(io, &format!("{manifest_dir}/{base_list}"), &base_metas)
        .await
        .unwrap();
    ManifestList::write(io, &format!("{manifest_dir}/{delta_list}"), &[])
        .await
        .unwrap();
    let replacement = Snapshot::builder()
        .version(3)
        .id(id)
        .schema_id(initial.schema_id())
        .base_manifest_list(base_list.to_string())
        .delta_manifest_list(delta_list.to_string())
        .commit_user("row-id-reassignment".to_string())
        .commit_identifier(BATCH_COMMIT_IDENTIFIER)
        .commit_kind(CommitKind::OVERWRITE)
        .time_millis(current_time_millis())
        .total_record_count(initial.total_record_count())
        .delta_record_count(Some(0))
        .next_row_id(Some(20))
        .build();
    (replacement, reassigned)
}

#[tokio::test]
async fn row_id_update_retry_reloads_base_after_overwrite() {
    let io = test_file_io();
    let path = "memory:/row-id-reassignment-during-commit";
    setup_dirs(&io, path).await;
    let mut commit = setup_data_evolution_commit(&io, path);
    let mut base = append_message("base.parquet");
    base.partition = EMPTY_SERIALIZED_ROW.clone();
    base.new_files[0].file_source = Some(0);
    commit.commit_with_identifier(vec![base], 1).await.unwrap();

    let initial = latest_snapshot(&io, path).await.unwrap();
    let (replacement, reassigned) = reassigned_snapshot(&commit, &initial, 2).await;
    commit.snapshot_commit = Arc::new(ConcurrentManifestReplacement {
        manager: commit.snapshot_manager.clone(),
        replacement,
    });
    commit.commit_min_retry_wait_ms = 0;
    commit.commit_max_retry_wait_ms = 0;

    let error = commit
        .commit_with_identifier(vec![row_id_check_message("staged.parquet", "name")], 2)
        .await
        .expect_err("the old range no longer exists after the concurrent overwrite");
    assert!(matches!(
        error,
        crate::Error::RowIdExistenceConflict {
            first_row_id: Some(0),
            ..
        }
    ));
    let latest = latest_snapshot(&io, path).await.unwrap();
    assert_eq!(latest.id(), 2);
    assert_eq!(active_entries(&io, path, &latest).await, reassigned);
}

#[tokio::test]
async fn row_id_retry_uses_catalog_overwrite_even_with_an_older_storage_snapshot() {
    use crate::api::rest_api::RESTApi;
    use crate::common::Options;
    use axum::{
        http::{Method, Uri},
        Json, Router,
    };
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };

    for reuse_id in [false, true] {
        let io = test_file_io();
        let path = "memory:/rest-row-id-reassignment";
        setup_dirs(&io, path).await;
        let seed = setup_data_evolution_commit(&io, path);
        let mut base = append_message("base.parquet");
        base.partition = EMPTY_SERIALIZED_ROW.clone();
        base.new_files[0].file_source = Some(0);
        seed.commit_with_identifier(vec![base], 1).await.unwrap();
        let initial = latest_snapshot(&io, path).await.unwrap();
        let (replacement, reassigned) = reassigned_snapshot(&seed, &initial, 2).await;

        // Simulate storage still exposing snapshot 2 from the previous history.
        seed.clone()
            .with_ignore_empty_commit(false)
            .commit_with_identifier(Vec::new(), 2)
            .await
            .unwrap();
        let stale = latest_snapshot(&io, path).await.unwrap();

        let latest = Arc::new(Mutex::new(if reuse_id { stale } else { initial }));
        let commits = Arc::new(AtomicUsize::new(0));
        let catalog_head = latest.clone();
        let catalog_commits = commits.clone();
        let schema = serde_json::to_value(seed.table.schema()).unwrap();
        let app = Router::new().fallback(move |method: Method, uri: Uri| async move {
            let body = if method == Method::POST && uri.path().ends_with("/commit") {
                if catalog_commits.fetch_add(1, Ordering::Relaxed) == 0 {
                    *catalog_head.lock().unwrap() = replacement;
                    serde_json::json!({"success": false})
                } else {
                    serde_json::json!({"success": true})
                }
            } else if uri.path().ends_with("/snapshot") {
                serde_json::json!({"snapshot": {"snapshot": *catalog_head.lock().unwrap()}})
            } else {
                serde_json::json!({"schemaId": 0, "schema": schema})
            };
            Json(body)
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut options = Options::new();
        options.set("uri", format!("http://{}", listener.local_addr().unwrap()));
        options.set("token.provider", "bear");
        options.set("token", "test-token");
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let api = Arc::new(RESTApi::new(options.clone(), false).await.unwrap());
        let cache = crate::io::FileIOCacheContext::from_props(options.to_map()).unwrap();
        let identifier = seed.table.identifier().clone();
        let env = crate::table::RESTEnv::new(
            identifier.clone(),
            "table-uuid".into(),
            api,
            options,
            false,
            None,
            cache,
        );
        let table = Table::new(
            io.clone(),
            identifier,
            path.into(),
            seed.table.schema().clone(),
            Some(env),
        );
        let mut commit = TableCommit::new(table, "rest-writer".into());
        commit.commit_min_retry_wait_ms = 0;
        commit.commit_max_retry_wait_ms = 0;

        let result = commit
            .commit_with_identifier(vec![row_id_check_message("staged.parquet", "name")], 2)
            .await;
        assert!(
            matches!(
                result,
                Err(crate::Error::RowIdExistenceConflict {
                    first_row_id: Some(0),
                    ..
                })
            ),
            "reuse_id={reuse_id}, {result:?}"
        );
        assert_eq!(commits.load(Ordering::Relaxed), 1);
        let head = latest.lock().unwrap().clone();
        assert_eq!(active_entries(&io, path, &head).await, reassigned);
        server.abort();
    }
}

async fn row_id_check_history(path: &str) -> TableCommit {
    row_id_check_history_with_io(&test_file_io(), path).await
}

async fn row_id_check_history_with_io(io: &FileIO, path: &str) -> TableCommit {
    setup_dirs(io, path).await;
    let commit = setup_data_evolution_commit(io, path);
    let mut file = test_data_file("base.parquet", 10);
    file.file_source = Some(0);
    commit
        .commit_with_identifier(
            vec![CommitMessage::new(
                EMPTY_SERIALIZED_ROW.clone(),
                0,
                vec![file],
            )],
            1,
        )
        .await
        .unwrap();
    commit
        .commit_with_identifier(vec![row_id_check_message("concurrent.parquet", "name")], 2)
        .await
        .unwrap();
    commit
}

#[tokio::test]
async fn row_id_retry_reloads_overwritten_history_before_a_new_append() {
    let path = "memory:/row-id-reassignment-before-append";
    let io = FileIOBuilder::new("memory")
        .with_prop("local-cache.enabled", "true")
        .build()
        .unwrap();
    let commit = row_id_check_history_with_io(&io, path).await;
    let manager = &commit.snapshot_manager;
    let initial = manager.get_snapshot(1).await.unwrap();
    let cached_reader = commit.table.snapshot_manager();
    let cached_snapshot = cached_reader.get_snapshot(2).await.unwrap();
    let cached_files = active_entries(&io, path, &cached_snapshot).await;

    // Rolling back to 1 lets an overwrite reuse ID 2. A subsequent append
    // hides it behind the cached retry's lower boundary. An external writer
    // cannot invalidate this instance's local block cache.
    let (replacement, _) = reassigned_snapshot(&commit, &initial, 2).await;
    let (storage, relative_path) = io.create_static(&manager.snapshot_path(2)).unwrap();
    storage
        .write(&relative_path, serde_json::to_vec(&replacement).unwrap())
        .await
        .unwrap();
    assert!(cached_reader
        .get_snapshot(2)
        .await
        .unwrap()
        .has_same_read_metadata(&cached_snapshot));
    let mut value = serde_json::to_value(replacement).unwrap();
    value["id"] = 3.into();
    value["commitKind"] = "APPEND".into();
    let latest: Snapshot = serde_json::from_value(value).unwrap();
    assert!(manager.commit_snapshot(&latest).await.unwrap());

    let changes = commit.messages_to_entries(&[row_id_check_message("staged.parquet", "name")]);
    let retry = RetryState {
        latest_snapshot: Some(cached_snapshot),
        base_data_files: Some(cached_files),
        publication_error: None,
    };
    let result = commit
        .detect_commit_conflicts(
            &Some(latest),
            Some(&retry),
            &changes,
            &CommitKind::APPEND,
            Some(1),
        )
        .await;
    assert!(matches!(
        result,
        Err(crate::Error::RowIdExistenceConflict {
            first_row_id: Some(0),
            ..
        })
    ));
}

#[tokio::test]
async fn standalone_row_id_check_rejects_logical_changes_without_deleting_staged_files() {
    let commit = row_id_check_history("memory:/standalone_row_id_check").await;
    let io = commit.table.file_io();
    let path = "memory:/standalone_row_id_check/bucket-0/staged.parquet";
    io.new_output(path)
        .unwrap()
        .write(bytes::Bytes::from_static(b"prepared"))
        .await
        .unwrap();
    let error = commit
        .check_row_id_conflicts(&[row_id_check_message("staged.parquet", "name")], 2, 1)
        .await
        .unwrap_err();
    assert!(matches!(error, crate::Error::DataInvalid { .. }));
    assert!(error.to_string().contains("multiple MERGE INTO"));
    assert!(
        io.exists(path).await.unwrap(),
        "a failed check must retain prepared files"
    );
    assert_eq!(
        commit
            .snapshot_manager
            .get_latest_snapshot_id()
            .await
            .unwrap(),
        Some(2)
    );
    commit
        .check_row_id_conflicts(&[row_id_check_message("id-only.parquet", "id")], 2, 1)
        .await
        .unwrap();
}

#[tokio::test]
async fn standalone_row_id_check_skips_compaction_history() {
    let commit = row_id_check_history("memory:/standalone_compact_history").await;
    // Replace only the fixture's commit kind. The same overlapping delta must
    // be ignored when it describes physical compaction rather than a DML write.
    let snapshot = commit.snapshot_manager.get_snapshot(2).await.unwrap();
    let mut json = serde_json::to_value(snapshot).unwrap();
    json["commitKind"] = "COMPACT".into();
    let path = format!("{}/snapshot-2", commit.snapshot_manager.snapshot_dir());
    commit
        .table
        .file_io()
        .new_output(&path)
        .unwrap()
        .write(serde_json::to_vec(&json).unwrap().into())
        .await
        .unwrap();
    commit
        .check_row_id_conflicts(&[row_id_check_message("staged.parquet", "name")], 2, 1)
        .await
        .unwrap();
}

#[tokio::test]
async fn standalone_row_id_check_validates_baselines_and_accepts_compatible_schema() {
    let commit = row_id_check_history("memory:/standalone_check_validation").await;
    let message = row_id_check_message("staged.parquet", "name");
    for (latest, baseline) in [(2, 0), (1, 2), (0, 1), (2, -1), (2, 2)] {
        assert!(matches!(
            commit
                .check_row_id_conflicts(std::slice::from_ref(&message), latest, baseline)
                .await,
            Err(crate::Error::DataInvalid { .. })
        ));
    }
    assert!(matches!(
        commit.check_row_id_conflicts(&[], 2, 1).await,
        Err(crate::Error::DataInvalid { .. })
    ));
    assert!(commit
        .check_row_id_conflicts(std::slice::from_ref(&message), 3, 1)
        .await
        .is_err());
    save_schema(
        commit.table.file_io(),
        "memory:/standalone_check_validation",
        1,
    )
    .await;
    let error = commit
        .check_row_id_conflicts(&[message], 2, 1)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("multiple MERGE INTO"));
    commit
        .check_row_id_conflicts(&[row_id_check_message("id-only.parquet", "id")], 2, 1)
        .await
        .unwrap();
}

#[tokio::test]
async fn standalone_row_id_check_fails_closed_for_missing_baseline_and_assembled_authorization() {
    let commit = row_id_check_history("memory:/standalone_missing_baseline").await;
    commit.snapshot_manager.delete_snapshot(1).await.unwrap();
    assert!(commit
        .check_row_id_conflicts(&[row_id_check_message("staged.parquet", "name")], 2, 1)
        .await
        .is_err());
    let guarded = TableCommit::new(crate::table::query_auth_table(), "guarded".into());
    let error = guarded
        .check_row_id_conflicts(&[row_id_check_message("staged.parquet", "name")], 2, 1)
        .await
        .unwrap_err();
    assert!(matches!(error, crate::Error::Unsupported { .. }));
    assert!(error.to_string().contains("query-auth.enabled"));
}
