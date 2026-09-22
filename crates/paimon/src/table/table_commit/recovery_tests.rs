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
async fn empty_unpartitioned_overwrite_clears_old_rows() {
    let io = test_file_io();
    let path = "memory:/audit-empty-overwrite";
    setup_dirs(&io, path).await;
    let commit = setup_commit(&io, path);
    commit
        .commit(vec![append_message("old.parquet")])
        .await
        .unwrap();
    commit.overwrite(Vec::new(), None).await.unwrap();
    let snapshot = latest_snapshot(&io, path).await.unwrap();
    assert_eq!(
        active_entries(&io, path, &snapshot).await.len(),
        0,
        "Empty unpartitioned overwrite must remove old data; snapshot={} count={:?}",
        snapshot.id(),
        snapshot.total_record_count()
    );
}

#[tokio::test]
async fn recovery_rejects_duplicate_file_after_identity_expiry() {
    let io = test_file_io();
    let path = "memory:/audit-expired-identity";
    setup_dirs(&io, path).await;
    let a = TableCommit::new(test_table(&io, path), "writer-a".into());
    let b = TableCommit::new(test_table(&io, path), "writer-b".into());
    let first = append_message("a.parquet");
    for name in ["a.parquet", "b.parquet"] {
        io.new_output(&format!("{path}/bucket-0/{name}"))
            .unwrap()
            .write(bytes::Bytes::from_static(
                b"physical file retained by latest snapshot",
            ))
            .await
            .unwrap();
    }
    a.commit_with_identifier(vec![first.clone()], 7)
        .await
        .unwrap();
    b.commit_with_identifier(vec![append_message("b.parquet")], 1)
        .await
        .unwrap();
    // Expiration removes the old snapshot, but both data files remain referenced by snapshot 2.
    let manager = a.table.snapshot_manager();
    manager.delete_snapshot(1).await.unwrap();
    manager.write_earliest_hint(2).await.unwrap();
    assert!(io
        .exists(&format!("{path}/bucket-0/a.parquet"))
        .await
        .unwrap());
    let result = a
        .filter_and_commit_with_identifier(vec![first.clone()], 7)
        .await;
    let snapshot = manager.get_latest_snapshot().await.unwrap().unwrap();
    let active_rows: i64 = active_entries(&io, path, &snapshot)
        .await
        .iter()
        .map(|e| e.file().row_count)
        .sum();
    assert!(result.is_err(), "Duplicate replay must be rejected; result={result:?}, latest={}, snapshot count={:?}, actual active rows={active_rows}", snapshot.id(), snapshot.total_record_count());
    assert_eq!(snapshot.id(), 2);
    assert_eq!(snapshot.total_record_count(), Some(active_rows));
    let result = a.filter_and_commit(vec![(7, vec![first])]).await;
    assert!(
        result.is_err(),
        "Batch recovery must reject duplicate replay: {result:?}"
    );
    assert_eq!(
        manager.get_latest_snapshot().await.unwrap().unwrap().id(),
        2
    );
}

#[tokio::test]
async fn recovery_rejects_missing_file_after_identity_expiry() {
    let io = test_file_io();
    let path = "memory:/audit-expired-missing-file";
    setup_dirs(&io, path).await;
    let a = TableCommit::new(test_table(&io, path), "writer-a".into());
    let b = TableCommit::new(test_table(&io, path), "writer-b".into());
    let first = append_message("a.parquet");
    let data_path = format!("{path}/bucket-0/a.parquet");
    io.new_output(&data_path)
        .unwrap()
        .write(bytes::Bytes::from_static(b"data"))
        .await
        .unwrap();
    a.commit_with_identifier(vec![first.clone()], 7)
        .await
        .unwrap();
    b.truncate_table().await.unwrap();
    let manager = a.table.snapshot_manager();
    manager.delete_snapshot(1).await.unwrap();
    manager.write_earliest_hint(2).await.unwrap();
    io.delete_file(&data_path).await.unwrap();
    let result = a
        .filter_and_commit_with_identifier(vec![first.clone()], 7)
        .await;
    let snapshot = manager.get_latest_snapshot().await.unwrap().unwrap();
    assert!(result.is_err(), "Replay referencing cleaned file must fail; result={result:?}, latest={}, count={:?}, physical_exists={}", snapshot.id(), snapshot.total_record_count(), io.exists(&data_path).await.unwrap());
    let error = a
        .filter_and_commit(vec![(7, vec![first])])
        .await
        .unwrap_err();
    assert!(error.to_string().contains(&data_path), "{error}");
    assert_eq!(
        manager.get_latest_snapshot().await.unwrap().unwrap().id(),
        2
    );
}

#[tokio::test]
async fn recovery_checks_data_changelog_extra_and_index_paths() {
    let io = test_file_io();
    let path = "memory:/recovery-paths";
    setup_dirs(&io, path).await;
    let table = test_table_with_options(
        &io,
        path,
        HashMap::from([("index-file-in-data-file-dir".into(), "true".into())]),
    );
    let commit = TableCommit::new(table, "recover".into());
    let mut message = append_message("data");
    message.new_files[0].external_path = Some(format!("{path}/external/data"));
    message.new_files[0].extra_files = vec!["data.index".into()];
    let mut changelog = test_data_file("changelog", 10);
    changelog.extra_files = vec!["changelog.index".into()];
    message.new_changelog_files.push(changelog);
    let mut bucket_index = test_global_index_file("hash", 0, 0, 9);
    bucket_index.index_type = "HASH".into();
    bucket_index.global_index_meta = None;
    let mut external_index = test_global_index_file("external-index", 1, 0, 9);
    external_index.external_path = Some(format!("{path}/external/index"));
    message.new_index_files = vec![
        bucket_index,
        test_global_index_file("global", 0, 0, 9),
        external_index,
    ];
    let paths = [
        "external/data",
        "external/data.index",
        "bucket-0/changelog",
        "bucket-0/changelog.index",
        "bucket-0/hash",
        "index/global",
        "external/index",
    ]
    .map(|name| format!("{path}/{name}"));
    for file in &paths {
        io.new_output(file)
            .unwrap()
            .write(bytes::Bytes::from_static(b"prepared"))
            .await
            .unwrap();
    }
    for missing in &paths {
        io.delete_file(missing).await.unwrap();
        let error = commit
            .filter_and_commit_with_identifier(vec![message.clone()], 1)
            .await
            .unwrap_err();
        assert!(error.to_string().contains(missing), "{error}");
        let error = commit
            .filter_and_commit(vec![(1, vec![message.clone()])])
            .await
            .unwrap_err();
        assert!(error.to_string().contains(missing), "{error}");
        assert!(latest_snapshot(&io, path).await.is_none());
        io.new_output(missing)
            .unwrap()
            .write(bytes::Bytes::from_static(b"prepared"))
            .await
            .unwrap();
    }
    commit
        .filter_and_commit_with_identifier(vec![message.clone()], 1)
        .await
        .unwrap();
    assert_eq!(latest_snapshot(&io, path).await.unwrap().id(), 1);
    // A known committed identity is filtered before files are checked.
    io.delete_file(&paths[0]).await.unwrap();
    commit
        .filter_and_commit_with_identifier(vec![message], 1)
        .await
        .unwrap();
    assert_eq!(latest_snapshot(&io, path).await.unwrap().id(), 1);
}

#[tokio::test]
async fn batch_recovery_validates_all_groups_before_publication() {
    let io = test_file_io();
    let path = "memory:/batch-recovery";
    setup_dirs(&io, path).await;
    let commit = setup_commit(&io, path);
    let first = append_message("first.parquet");
    let second = append_message("second.parquet");
    let first_path = format!("{path}/bucket-0/first.parquet");
    let second_path = format!("{path}/bucket-0/second.parquet");
    io.new_output(&first_path)
        .unwrap()
        .write(bytes::Bytes::from_static(b"prepared"))
        .await
        .unwrap();
    let error = commit
        .filter_and_commit(vec![(7, vec![first.clone()]), (7, vec![second.clone()])])
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("Duplicate commit identifier 7"),
        "{error}"
    );
    assert!(latest_snapshot(&io, path).await.is_none());

    let pending = vec![(8, vec![second]), (7, vec![first])];
    let error = commit.filter_and_commit(pending.clone()).await.unwrap_err();
    assert!(error.to_string().contains(&second_path), "{error}");
    assert!(latest_snapshot(&io, path).await.is_none());
    io.new_output(&second_path)
        .unwrap()
        .write(bytes::Bytes::from_static(b"prepared"))
        .await
        .unwrap();
    assert_eq!(commit.filter_and_commit(pending.clone()).await.unwrap(), 2);
    let snapshot = latest_snapshot(&io, path).await.unwrap();
    assert_eq!(snapshot.id(), 2);
    assert_eq!(snapshot.commit_identifier(), 8);
    assert_eq!(snapshot.total_record_count(), Some(20));
    io.delete_file(&first_path).await.unwrap();
    io.delete_file(&second_path).await.unwrap();
    assert_eq!(commit.filter_and_commit(pending).await.unwrap(), 0);
    assert_eq!(latest_snapshot(&io, path).await.unwrap().id(), 2);
}

#[tokio::test]
async fn dv_commits_allow_unrelated_writes_but_reject_replaced_vectors() {
    let io = test_file_io();
    let path = "memory:/dv-concurrency";
    setup_dirs(&io, path).await;
    let table = Table::new(
        io.clone(),
        Identifier::new("default", "dv"),
        path.into(),
        test_partitioned_schema().copy_with_options(HashMap::from([
            ("data-evolution.enabled".into(), "true".into()),
            ("row-tracking.enabled".into(), "true".into()),
        ])),
        None,
    );
    let commit = TableCommit::new(table, "dv".into());
    let part_a = partition_bytes("a");
    let part_b = partition_bytes("b");
    let append = |partition, name| {
        let mut file = test_data_file(name, 10);
        file.file_source = Some(0);
        CommitMessage::new(partition, 0, vec![file])
    };
    commit
        .commit(vec![
            append(part_a.clone(), "data-a"),
            append(part_b.clone(), "data-b"),
        ])
        .await
        .unwrap();
    let mut delete_a = dv_message("dv-a", "data-a");
    delete_a.partition = part_a.clone();
    delete_a.check_from_snapshot = Some(1);
    let mut delete_b = dv_message("dv-b", "data-b");
    delete_b.partition = part_b.clone();
    delete_b.check_from_snapshot = Some(1);
    commit.commit(vec![delete_a]).await.unwrap();
    // Mixed APPEND in A and DELETE in B must scan both relevant partitions.
    commit
        .commit(vec![append(part_a.clone(), "append-a"), delete_b])
        .await
        .unwrap();
    let mut replace = dv_message("replacement", "data-a");
    replace.partition = part_a.clone();
    replace.deleted_index_files = vec![test_deletion_vector_index_file("dv-a", "data-a")];
    replace.check_from_snapshot = Some(2);
    commit.commit(vec![replace.clone()]).await.unwrap();
    replace.new_index_files[0].file_name = "stale-replacement".into();
    let error = commit.commit(vec![replace]).await.unwrap_err();
    assert!(
        error.to_string().contains("missing deletion vector"),
        "{error}"
    );
    assert_eq!(latest_snapshot(&io, path).await.unwrap().id(), 4);
    let snapshot = latest_snapshot(&io, path).await;
    let indexes = TableCommit::read_prev_index_entries(&io, &format!("{path}/manifest"), &snapshot)
        .await
        .unwrap();
    assert_eq!(
        indexes
            .iter()
            .map(|entry| entry.index_file.file_name.as_str())
            .collect::<HashSet<_>>(),
        HashSet::from(["replacement", "dv-b"])
    );
}

#[tokio::test]
async fn dv_references_must_survive_concurrent_and_same_commit_data_deletion() {
    for concurrent in [true, false] {
        let io = test_file_io();
        let path = format!("memory:/dv-removed-data-{concurrent}");
        setup_dirs(&io, &path).await;
        let commit = setup_commit(&io, &path);
        let data = test_data_file("data", 10);
        commit
            .commit(vec![CommitMessage::new(vec![], 0, vec![data.clone()])])
            .await
            .unwrap();
        let mut dv = dv_message("dv", "data");
        dv.check_from_snapshot = Some(1);
        if concurrent {
            commit.truncate_table().await.unwrap();
        } else {
            dv.deleted_files = vec![data];
        }
        let error = commit.commit(vec![dv]).await.unwrap_err();
        assert!(
            error.to_string().contains("references missing data file"),
            "{error}"
        );
        assert_eq!(
            latest_snapshot(&io, &path).await.unwrap().id(),
            if concurrent { 2 } else { 1 }
        );
    }
    let io = test_file_io();
    let path = "memory:/dv-retained-on-deleted-file";
    setup_dirs(&io, path).await;
    let commit = setup_commit(&io, path);
    let data = test_data_file("data", 10);
    commit
        .commit(vec![
            CommitMessage::new(vec![], 0, vec![data.clone()]),
            dv_message("dv", "data"),
        ])
        .await
        .unwrap();
    let mut deletion = CommitMessage::new(vec![], 0, vec![]);
    deletion.deleted_files = vec![data];
    assert!(commit
        .commit(vec![deletion.clone()])
        .await
        .unwrap_err()
        .to_string()
        .contains("references missing data file"));
    deletion.deleted_index_files = vec![test_deletion_vector_index_file("dv", "data")];
    commit.commit(vec![deletion]).await.unwrap();
    let snapshot = latest_snapshot(&io, path).await.unwrap();
    assert!(snapshot.index_manifest().is_none());
    assert!(active_entries(&io, path, &snapshot).await.is_empty());
}

#[tokio::test]
async fn explicit_row_id_update_still_checks_indexed_columns() {
    let io = test_file_io();
    let path = "memory:/explicit-row-id-update";
    setup_dirs(&io, path).await;
    let commit = setup_commit(&io, path);
    let mut index = CommitMessage::new(vec![], 0, vec![]);
    index.new_index_files = vec![test_global_index_file("global", 0, 0, 9)];
    commit.commit(vec![index]).await.unwrap();
    let mut update = append_message("update");
    update.new_files[0].write_cols = Some(vec!["id".into(), crate::spec::ROW_ID_FIELD_NAME.into()]);
    let error = commit.commit(vec![update]).await.unwrap_err();
    assert!(
        error.to_string().contains("globally indexed columns"),
        "{error}"
    );
}

struct ConcurrentDvCommit {
    table: Table,
    change: &'static str,
    calls: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl SnapshotCommit for ConcurrentDvCommit {
    async fn commit(&self, snapshot: &Snapshot, _: &[PartitionStatistics]) -> Result<bool> {
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            let other = TableCommit::new(self.table.clone(), "concurrent".into());
            match self.change {
                "append" => other.commit(vec![append_message("unrelated")]).await?,
                "delete" => other.truncate_table().await?,
                "dv" => {
                    other
                        .commit(vec![dv_message("competing-dv", "data")])
                        .await?
                }
                _ => unreachable!(),
            }
            return Ok(false);
        }
        self.table
            .snapshot_manager()
            .commit_snapshot(snapshot)
            .await
    }
}

#[tokio::test]
async fn dv_retry_revalidates_data_files_and_vectors() {
    for change in ["append", "delete", "dv"] {
        let io = test_file_io();
        let path = format!("memory:/dv-retry-{change}");
        setup_dirs(&io, &path).await;
        let mut commit = setup_commit(&io, &path);
        commit.commit(vec![append_message("data")]).await.unwrap();
        commit.commit_min_retry_wait_ms = 0;
        commit.commit_max_retry_wait_ms = 0;
        let publisher = Arc::new(ConcurrentDvCommit {
            table: commit.table.clone(),
            change,
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        commit.snapshot_commit = publisher.clone();
        let mut deletion = dv_message("dv", "data");
        deletion.check_from_snapshot = Some(1);
        let result = commit.commit(vec![deletion]).await;
        let snapshot = latest_snapshot(&io, &path).await.unwrap();
        if change == "append" {
            result.unwrap();
            assert_eq!(snapshot.id(), 3);
            assert_eq!(snapshot.total_record_count(), Some(20));
            assert_eq!(publisher.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        } else {
            let error = result.unwrap_err().to_string();
            assert!(
                error.contains(if change == "delete" {
                    "references missing data file"
                } else {
                    "Conflicting deletion vectors"
                }),
                "{error}"
            );
            assert_eq!(snapshot.id(), 2);
            assert_eq!(publisher.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        }
    }
}

#[tokio::test]
async fn rest_delete_writer_pins_catalog_snapshot_and_preserves_vectors() {
    use crate::api::rest_api::RESTApi;
    use crate::common::Options;
    use arrow_array::{Array, Int32Array, RecordBatch, StringArray};
    use axum::{
        body::Bytes,
        http::{Method, Uri},
        Json, Router,
    };
    use futures::TryStreamExt;
    use std::sync::Mutex;
    let io = test_file_io();
    let path = "memory:/audit-rest-dv";
    setup_dirs(&io, path).await;
    let schema = test_data_evolution_schema().copy_with_options(HashMap::from([(
        "deletion-vectors.enabled".into(),
        "true".into(),
    )]));
    let seed_table = Table::new(
        io.clone(),
        Identifier::new("database", "table"),
        path.into(),
        schema.clone(),
        None,
    );
    let mut writer = crate::table::TableWrite::new(&seed_table, "seed".into()).unwrap();
    let batch = RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int32Array::from(vec![0, 1, 2])) as arrow_array::ArrayRef,
        ),
        (
            "name",
            Arc::new(StringArray::from(vec!["zero", "one", "two"])) as arrow_array::ArrayRef,
        ),
    ])
    .unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    TableCommit::new(seed_table, "seed".into())
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();
    let mut value = serde_json::to_value(latest_snapshot(&io, path).await.unwrap()).unwrap();
    value["id"] = 7.into();
    let snapshot = Arc::new(Mutex::new(
        serde_json::from_value::<Snapshot>(value).unwrap(),
    ));
    let posts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let loads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let handler_loads = loads.clone();
    let handler_snapshot = snapshot.clone();
    let handler_posts = posts.clone();
    let app = Router::new().fallback(move |method: Method, uri: Uri, body: Bytes| {
        let loads = handler_loads.clone();
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
                loads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                serde_json::json!({"snapshot": {"snapshot": *snapshot.lock().unwrap(), "recordCount": 10}})
            } else {
                serde_json::json!({"schemaId": 0})
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
    let table = Table::new(io.clone(), identifier, path.into(), schema, Some(env));
    let mut commit = TableCommit::new(table.clone(), "rest-writer".into());
    commit.commit_min_retry_wait_ms = 0;
    commit.commit_max_retry_wait_ms = 0;
    async fn remaining_ids(table: &Table) -> Vec<i32> {
        let builder = table.new_read_builder();
        let plan = builder.new_scan().plan().await.unwrap();
        let read = builder.new_read().unwrap();
        let batches = read
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let mut ids = Vec::new();
        for batch in batches {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            ids.extend((0..column.len()).map(|i| column.value(i)));
        }
        ids.sort();
        ids
    }
    for row_id in [0, 1] {
        let mut deletion = table.new_write_builder().new_delete().unwrap();
        deletion.add_row_ids([row_id]).unwrap();
        let before = loads.load(std::sync::atomic::Ordering::SeqCst);
        let messages = deletion.prepare_commit().await.unwrap();
        assert_eq!(loads.load(std::sync::atomic::Ordering::SeqCst) - before, 1);
        assert_eq!(messages[0].check_from_snapshot, Some(7 + row_id));
        commit
            .commit_with_identifier(messages, 42 + row_id)
            .await
            .unwrap();
        assert_eq!(
            remaining_ids(&table).await,
            if row_id == 0 { vec![1, 2] } else { vec![2] }
        );
    }
    let mut left = table.new_write_builder().new_delete().unwrap();
    left.add_row_ids([2]).unwrap();
    let mut right = table.new_write_builder().new_delete().unwrap();
    right.add_row_ids([2]).unwrap();
    let left = left.prepare_commit().await.unwrap();
    let right = right.prepare_commit().await.unwrap();
    commit.commit_with_identifier(left, 44).await.unwrap();
    let error = commit.commit_with_identifier(right, 45).await.unwrap_err();
    assert!(
        error.to_string().contains("missing deletion vector"),
        "{error}"
    );
    assert!(remaining_ids(&table).await.is_empty());
    assert_eq!(snapshot.lock().unwrap().id(), 10);
    assert_eq!(posts.load(std::sync::atomic::Ordering::SeqCst), 3);
    for id in 7..=10 {
        assert!(!io
            .exists(&format!("{path}/snapshot/snapshot-{id}"))
            .await
            .unwrap());
    }
    server.abort();
}
