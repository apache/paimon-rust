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

mod pk_dv_retry {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch};
    use indexmap::IndexMap;
    use roaring::RoaringBitmap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn batch(ids: Vec<i32>, level: i32) -> RecordBatch {
        RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int32Array::from(ids.clone())) as arrow_array::ArrayRef,
            ),
            (
                "value",
                Arc::new(Int32Array::from(
                    ids.iter()
                        .map(|id| id * if level == 0 { 100 } else { 10 })
                        .collect::<Vec<_>>(),
                )),
            ),
        ])
        .unwrap()
    }

    async fn write(table: &Table, ids: Vec<i32>, level: i32) -> (Vec<u8>, DataFileMeta) {
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        writer.write_arrow_batch(&batch(ids, level)).await.unwrap();
        let mut messages = writer.prepare_commit().await.unwrap();
        for file in &mut messages[0].new_files {
            file.level = level;
        }
        let output = (
            messages[0].partition.clone(),
            messages[0].new_files[0].clone(),
        );
        builder.new_commit().commit(messages).await.unwrap();
        output
    }

    async fn vectors(
        table: &Table,
    ) -> (
        IndexMap<String, RoaringBitmap>,
        Vec<crate::spec::IndexFileMeta>,
    ) {
        let snapshot = table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        crate::table::deletion_vector_index::read_bucket(
            table,
            &EMPTY_SERIALIZED_ROW,
            0,
            &snapshot,
            None,
        )
        .await
        .unwrap()
    }

    async fn replace_vectors(table: &Table, masks: &[(&str, &[u32])]) {
        let mut message = CommitMessage::new(EMPTY_SERIALIZED_ROW.to_vec(), 0, vec![]);
        message.deleted_index_files = vectors(table).await.1;
        message.new_index_files.push(
            crate::table::deletion_vector_index::write_bucket(
                table,
                &message.partition,
                0,
                masks
                    .iter()
                    .map(|(name, positions)| {
                        (name.to_string(), positions.iter().copied().collect())
                    })
                    .collect(),
            )
            .await
            .unwrap(),
        );
        table
            .new_write_builder()
            .new_commit()
            .commit(vec![message])
            .await
            .unwrap();
    }

    async fn retry_table() -> Table {
        let path = format!("memory:/pk-dv-retry/{}", uuid::Uuid::new_v4());
        let mut options = crate::Options::new();
        options.set("warehouse", &path);
        options.set("local-cache.enabled", "true");
        let io = crate::catalog::FileSystemCatalog::new(options)
            .unwrap()
            .file_io()
            .clone();
        setup_dirs(&io, &path).await;
        let schema = crate::spec::Schema::builder()
            .option("write-only", "true")
            .column("id", DataType::Int(crate::spec::IntType::new()))
            .column("value", DataType::Int(crate::spec::IntType::new()))
            .primary_key(["id"])
            .option("bucket", "1")
            .option("deletion-vectors.enabled", "true")
            .option("changelog-producer", "lookup")
            .option("read.batch-size", "7")
            .build()
            .unwrap();
        let table = Table::new(
            io.clone(),
            Identifier::new("default", "pk_dv_retry"),
            path.clone(),
            TableSchema::new(0, &schema),
            None,
        );
        io.mkdirs(&format!("{path}/schema")).await.unwrap();
        io.new_output(&format!("{path}/schema/schema-0"))
            .unwrap()
            .write(serde_json::to_vec(table.schema()).unwrap().into())
            .await
            .unwrap();
        table
    }

    async fn fixture() -> (Table, String, String) {
        let table = retry_table().await;
        let (partition, anchor) = write(&table, (0..1000).collect(), 5).await;
        assert_eq!(partition.as_slice(), EMPTY_SERIALIZED_ROW.as_slice());
        let (_, unrelated) = write(&table, vec![2000, 2001], 5).await;
        replace_vectors(
            &table,
            &[(&anchor.file_name, &[500]), (&unrelated.file_name, &[0])],
        )
        .await;
        (table, anchor.file_name, unrelated.file_name)
    }

    async fn dv_paths(table: &Table, snapshot: &Snapshot) -> Vec<String> {
        index_paths(table, snapshot, DELETION_VECTORS_INDEX_TYPE).await
    }

    async fn index_paths(table: &Table, snapshot: &Snapshot, index_type: &str) -> Vec<String> {
        let entries = IndexManifest::read(
            table.file_io(),
            &table
                .snapshot_manager()
                .manifest_path(snapshot.index_manifest().unwrap()),
        )
        .await
        .unwrap();
        let commit = table.new_write_builder().new_commit();
        entries
            .into_iter()
            .filter(|entry| entry.index_file.index_type == index_type)
            .map(|entry| {
                committed_index_file_path(
                    table.location(),
                    &commit.bucket_path(&entry.partition, entry.bucket).unwrap(),
                    table.schema().core_options().index_file_in_data_file_dir(),
                    &entry.index_file,
                )
            })
            .collect()
    }

    struct RacingCommit {
        table: Table,
        calls: AtomicUsize,
        publish_first: bool,
        paths: std::sync::Mutex<Vec<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl SnapshotCommit for RacingCommit {
        async fn commit(
            &self,
            _: Option<&str>,
            snapshot: &Snapshot,
            _: &str,
            _: &[PartitionStatistics],
        ) -> Result<bool> {
            let paths = if snapshot.commit_kind() == &CommitKind::OVERWRITE {
                let mut paths = index_paths(&self.table, snapshot, "bitmap").await;
                for entry in
                    active_entries(self.table.file_io(), self.table.location(), snapshot).await
                {
                    let bucket = self
                        .table
                        .data_file_bucket_path(entry.partition(), entry.bucket())?;
                    paths.extend(entry.file().collect_files(&bucket));
                }
                paths
            } else {
                dv_paths(&self.table, snapshot).await
            };
            self.paths.lock().unwrap().push(paths);
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                if self.publish_first {
                    assert!(
                        self.table
                            .snapshot_manager()
                            .commit_snapshot(snapshot)
                            .await?
                    );
                    return Err(crate::Error::IoUnsupported {
                        message: "lost commit response".into(),
                    });
                }
                write(&self.table, vec![4000], 0).await;
                return Ok(false);
            }
            self.table
                .snapshot_manager()
                .commit_snapshot(snapshot)
                .await
        }
    }

    #[tokio::test]
    async fn compact_retry_preserves_submitted_dv_containers() {
        for publish_first in [false, true] {
            let (table, anchor, unrelated) = fixture().await;
            let old = table
                .snapshot_manager()
                .get_latest_snapshot()
                .await
                .unwrap()
                .unwrap();
            let original_paths = dv_paths(&table, &old).await;
            write(&table, vec![42], 0).await;
            let builder = table.new_compact_builder();
            let plan = builder.new_planner().unwrap().plan().await.unwrap();
            assert_eq!(plan.tasks().len(), 1);
            assert!(plan.tasks()[0].files().iter().all(|file| file.level == 0));
            let result = builder
                .new_executor()
                .unwrap()
                .execute(&plan.tasks()[0])
                .await
                .unwrap();
            assert_eq!(result.files()[0].level, 4);
            assert!(!result.changelog_files().is_empty());
            let publisher = Arc::new(RacingCommit {
                table: table.clone(),
                calls: AtomicUsize::new(0),
                publish_first,
                paths: std::sync::Mutex::new(vec![]),
            });
            let mut commit = builder.new_commit().unwrap();
            commit.snapshot_commit = publisher.clone();
            commit.commit_min_retry_wait_ms = 0;
            commit.commit_max_retry_wait_ms = 0;
            let metrics = commit.commit_compact(vec![result.clone()]).await.unwrap();
            assert_eq!(
                publisher.calls.load(Ordering::SeqCst),
                if publish_first { 1 } else { 2 }
            );
            let attempts = publisher.paths.lock().unwrap().clone();
            assert_eq!(attempts[0].len(), 1);
            assert!(!original_paths.contains(&attempts[0][0]));
            for path in original_paths.iter().chain(attempts.iter().flatten()) {
                assert!(
                    table.file_io().exists(path).await.unwrap(),
                    "deleted retained or published DV: {path}"
                );
            }
            let bucket = commit.bucket_path(plan.tasks()[0].partition(), 0).unwrap();
            for file in result.files().iter().chain(result.changelog_files()) {
                for path in file.collect_files(&bucket) {
                    assert!(table.file_io().exists(&path).await.unwrap());
                }
            }
            let masks = vectors(&table).await.0;
            assert_eq!(masks[&anchor], [42, 500].into_iter().collect());
            assert_eq!(masks[&unrelated], [0].into_iter().collect());
            let after = table
                .snapshot_manager()
                .get_latest_snapshot_id()
                .await
                .unwrap();
            assert_eq!(commit.commit_compact(vec![result]).await.unwrap(), metrics);
            assert_eq!(
                table
                    .snapshot_manager()
                    .get_latest_snapshot_id()
                    .await
                    .unwrap(),
                after
            );
        }
    }

    async fn read_ids(table: &Table) -> Vec<i32> {
        use arrow_array::{cast::AsArray, types::Int32Type};
        use futures::TryStreamExt;

        let read = table.new_read_builder();
        let plan = read.new_scan().plan().await.unwrap();
        let batches: Vec<_> = read
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_primitive::<Int32Type>()
                    .values()
                    .iter()
                    .copied()
            })
            .collect()
    }

    fn indexed_overwrite_table(table: &Table) -> Table {
        table.copy_with_options(HashMap::from([
            ("write-only".into(), "false".into()),
            ("pk-bitmap.index.columns".into(), "value".into()),
            ("write.parquet-buffer-size".into(), "1 B".into()),
        ]))
    }

    async fn overlapping_inputs(builder: &crate::table::WriteBuilder<'_>) -> Vec<CommitMessage> {
        let mut writer = builder.new_write().unwrap();
        for ids in [vec![1, 2], vec![2, 3]] {
            writer.write_arrow_batch(&batch(ids, 0)).await.unwrap();
        }
        let inputs = writer.prepare_commit().await.unwrap();
        assert_eq!(inputs[0].new_files.len(), 2);
        inputs
    }

    #[tokio::test]
    async fn lookup_overwrite_retry_preserves_submitted_files_and_staged_inputs() {
        for publish_first in [false, true] {
            let raw = retry_table().await;
            let table = indexed_overwrite_table(&raw);
            let publisher = Arc::new(RacingCommit {
                table: raw,
                calls: AtomicUsize::new(0),
                publish_first,
                paths: Default::default(),
            });
            let builder = table.new_write_builder().with_overwrite();
            let mut commit = builder.new_commit();
            commit.snapshot_commit = publisher.clone();
            commit.commit_min_retry_wait_ms = 0;
            commit.commit_max_retry_wait_ms = 0;
            let inputs = overlapping_inputs(&builder).await;
            let bucket = commit.bucket_path(&inputs[0].partition, 0).unwrap();
            let staged: Vec<_> = inputs
                .iter()
                .flat_map(|msg| &msg.new_files)
                .flat_map(|file| file.collect_files(&bucket))
                .collect();
            assert_eq!(staged.len(), 2);
            commit
                .overwrite_with_identifier(inputs, None, 7)
                .await
                .unwrap();

            let attempts = publisher.paths.lock().unwrap().clone();
            assert_eq!(attempts.len(), if publish_first { 1 } else { 2 });
            for paths in &attempts {
                assert_eq!(
                    paths.len(),
                    2,
                    "expected a PK index and a replacement data file: {paths:?}"
                );
                for path in paths {
                    assert!(table.file_io().exists(path).await.unwrap(), "{path}");
                }
            }
            for path in &staged {
                assert!(
                    table.file_io().exists(path).await.unwrap(),
                    "missing staged input: {path}"
                );
            }
            assert_eq!(read_ids(&table).await, [1, 2, 3]);
        }
    }

    #[tokio::test]
    async fn lookup_overwrite_reclaims_only_unsubmitted_files_when_overtaken() {
        async fn files(table: &Table) -> HashSet<String> {
            table
                .file_io()
                .list_status_recursive(table.location())
                .await
                .unwrap()
                .into_iter()
                .filter(|file| !file.is_dir)
                .map(|file| file.path)
                .collect()
        }

        let raw = retry_table().await;
        let table = indexed_overwrite_table(&raw);
        let builder = table.new_write_builder().with_overwrite();
        let inputs = overlapping_inputs(&builder).await;
        let commit = builder.new_commit();
        let mut plan = overwrite_plan(None, commit.messages_to_entries(&inputs));
        let resolved = commit
            .resolve_commit(&mut plan, &None, None, false)
            .await
            .unwrap();

        // The writer planned against an empty table. Another writer claims ID 1
        // before this attempt can submit its replacement run and bitmap.
        write(&raw, vec![4], 0).await;
        let before = files(&table).await;
        let attempt = commit
            .try_commit_once(resolved, &None, 7, true)
            .await
            .unwrap();
        assert!(
            matches!(attempt, CommitAttemptResult::Retry(state) if state.publication_error.is_none())
        );
        assert_eq!(files(&table).await, before);

        commit
            .overwrite_with_identifier(inputs, None, 7)
            .await
            .unwrap();
        assert_eq!(read_ids(&table).await, [1, 2, 3]);
    }

    #[tokio::test]
    async fn lookup_overwrite_keeps_retained_branch_readable_after_snapshot_id_reuse() {
        struct ReplayAfterRollback(Table);

        #[async_trait::async_trait]
        impl SnapshotCommit for ReplayAfterRollback {
            async fn commit(
                &self,
                _: Option<&str>,
                snapshot: &Snapshot,
                _: &str,
                _: &[PartitionStatistics],
            ) -> Result<bool> {
                let table = &self.0;
                let manager = table.snapshot_manager();
                if snapshot.id() == 2 {
                    assert!(manager.commit_snapshot(snapshot).await?);
                    table
                        .tag_manager()
                        .create("before-rollback", snapshot)
                        .await?;
                    crate::table::BranchManager::new(
                        table.file_io().clone(),
                        table.location().into(),
                    )
                    .create_branch_from_tag("retained", "before-rollback")
                    .await?;
                    assert_eq!(
                        read_ids(&table.copy_with_branch("retained").await?).await,
                        [1, 2, 3]
                    );

                    manager.write_latest_hint(1).await?;
                    manager.delete_snapshot(2).await?;
                    table.tag_manager().delete("before-rollback").await?;
                    write(table, vec![5], 0).await;
                    assert_ne!(manager.get_snapshot(2).await?.uuid(), snapshot.uuid());
                }
                // Replaying the original write now collides with the reused ID.
                manager.commit_snapshot(snapshot).await
            }
        }

        let raw = retry_table().await;
        write(&raw, vec![4], 0).await;
        let table = indexed_overwrite_table(&raw);
        let builder = table.new_write_builder().with_overwrite();
        let mut commit = builder.new_commit();
        commit.snapshot_commit = Arc::new(ReplayAfterRollback(raw));
        commit
            .overwrite_with_identifier(overlapping_inputs(&builder).await, None, 7)
            .await
            .unwrap();
        assert_eq!(read_ids(&table).await, [1, 2, 3]);

        let retained = table.copy_with_branch("retained").await.unwrap();
        assert_eq!(read_ids(&retained).await, [1, 2, 3]);
        let original = retained.snapshot_manager().get_snapshot(2).await.unwrap();
        let indexes = index_paths(&retained, &original, "bitmap").await;
        assert_eq!(indexes.len(), 1);
        assert!(retained.file_io().exists(&indexes[0]).await.unwrap());
    }

    #[cfg(feature = "storage-oss")]
    #[tokio::test]
    async fn lookup_overwrite_recovers_after_oss_replays_a_published_put() {
        use axum::extract::State;
        use axum::http::{HeaderMap, StatusCode};
        use axum::{routing::put, Router};

        async fn put_snapshot(
            State((manager, puts)): State<(SnapshotManager, Arc<AtomicUsize>)>,
            headers: HeaderMap,
            body: bytes::Bytes,
        ) -> (StatusCode, &'static str) {
            assert_eq!(headers["x-oss-forbid-overwrite"], "true");
            puts.fetch_add(1, Ordering::SeqCst);
            let stored = manager
                .file_io()
                .write_atomic_if_not_exists(&manager.snapshot_path(1), body)
                .await
                .unwrap();
            if stored {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "<Error><Code>QpsLimitExceeded</Code><Message>retry</Message></Error>",
                )
            } else {
                (
                    StatusCode::CONFLICT,
                    "<Error><Code>FileAlreadyExists</Code><Message>exists</Message></Error>",
                )
            }
        }

        let table = indexed_overwrite_table(&retry_table().await);
        // Only the snapshot PUT goes through OSS. Serve it from the same storage
        // that the table uses for recovery and subsequent reads.
        let puts = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route("/bucket/table/snapshot/snapshot-1", put(put_snapshot))
            .with_state((table.snapshot_manager(), puts.clone()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = opendal_service_oss::OssConfig::default();
        config.endpoint = Some(format!("http://{}", listener.local_addr().unwrap()));
        config.bucket = "bucket".into();
        config.addressing_style = Some("path".into());
        config.skip_signature = true;
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let transport = opendal::HttpTransporter::new(
            opendal_http_transport_reqwest::ReqwestTransport::default(),
        );
        let operator = opendal::Operator::from_config(config)
            .unwrap()
            .with_context(opendal::OperationContext::new().with_http_transport(transport))
            .layer(opendal_layer_retry::RetryLayer::default().with_max_times(1));
        let io = FileIOBuilder::new("file")
            .with_fs_operator(operator)
            .build()
            .unwrap();
        let builder = table.new_write_builder().with_overwrite();
        let inputs = overlapping_inputs(&builder).await;
        let mut commit = builder.new_commit();
        commit.snapshot_commit = Arc::new(crate::table::RenamingSnapshotCommit::new(
            SnapshotManager::new(io, "/table".into()),
        ));
        commit
            .overwrite_with_identifier(inputs, None, 7)
            .await
            .unwrap();
        server.abort();

        assert_eq!(puts.load(Ordering::SeqCst), 2);
        assert_eq!(read_ids(&table).await, [1, 2, 3]);
        let snapshot = table.snapshot_manager().get_snapshot(1).await.unwrap();
        let indexes = index_paths(&table, &snapshot, "bitmap").await;
        assert_eq!(indexes.len(), 1);
        assert!(table.file_io().exists(&indexes[0]).await.unwrap());
    }

    #[tokio::test]
    async fn selected_dv_container_repacking_compares_masks_and_rebases_unrelated_rows() {
        for selected_changed in [false, true] {
            let (table, anchor, unrelated) = fixture().await;
            let (_, selected) = write(&table, vec![42, 43], 0).await;
            replace_vectors(
                &table,
                &[
                    (&anchor, &[500]),
                    (&unrelated, &[0]),
                    (&selected.file_name, &[0]),
                ],
            )
            .await;
            let builder = table.new_compact_builder();
            let plan = builder.new_planner().unwrap().plan().await.unwrap();
            let result = builder
                .new_executor()
                .unwrap()
                .execute(&plan.tasks()[0])
                .await
                .unwrap();
            assert_eq!(result.files()[0].row_count, 1);
            let captured = table
                .snapshot_manager()
                .get_latest_snapshot()
                .await
                .unwrap()
                .unwrap();
            let old_paths = dv_paths(&table, &captured).await;
            replace_vectors(
                &table,
                &[
                    (&anchor, &[500, 501]),
                    (&unrelated, &[0]),
                    (
                        &selected.file_name,
                        if selected_changed { &[0, 1] } else { &[0] },
                    ),
                ],
            )
            .await;
            let latest = table
                .snapshot_manager()
                .get_latest_snapshot()
                .await
                .unwrap()
                .unwrap();
            assert_ne!(dv_paths(&table, &latest).await, old_paths);
            let published = builder
                .new_commit()
                .unwrap()
                .commit_compact(vec![result])
                .await;
            if selected_changed {
                assert!(published
                    .unwrap_err()
                    .to_string()
                    .contains("input deletion vectors changed"));
                assert_eq!(
                    table
                        .snapshot_manager()
                        .get_latest_snapshot_id()
                        .await
                        .unwrap(),
                    Some(latest.id())
                );
            } else {
                published.unwrap();
                let masks = vectors(&table).await.0;
                assert_eq!(masks[&anchor], [43, 500, 501].into_iter().collect());
                assert_eq!(masks[&unrelated], [0].into_iter().collect());
                assert!(!masks.contains_key(&selected.file_name));
            }
            for path in old_paths {
                assert!(table.file_io().exists(&path).await.unwrap());
            }
        }
    }
}
