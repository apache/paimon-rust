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
    use futures::TryStreamExt;
    use indexmap::IndexMap;
    use roaring::RoaringBitmap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    async fn write(table: &Table, ids: Vec<i32>, level: i32) -> (Vec<u8>, DataFileMeta) {
        let batch = RecordBatch::try_from_iter([
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
        .unwrap();
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        writer.write_arrow_batch(&batch).await.unwrap();
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

    async fn rows(table: &Table) -> Vec<(i32, i32)> {
        let read = table.new_read_builder();
        let plan = read.new_scan().plan().await.unwrap();
        let batches: Vec<RecordBatch> = read
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        batch_rows(batches)
    }

    fn batch_rows(batches: Vec<RecordBatch>) -> Vec<(i32, i32)> {
        let mut rows = Vec::new();
        for batch in batches {
            let ids = batch["id"].as_any().downcast_ref::<Int32Array>().unwrap();
            let values = batch["value"]
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            rows.extend(
                ids.values()
                    .iter()
                    .copied()
                    .zip(values.values().iter().copied()),
            );
        }
        rows.sort_unstable();
        rows
    }

    struct InterruptOverwriteOnce(std::sync::atomic::AtomicBool);

    #[async_trait::async_trait]
    impl CommitCallback for InterruptOverwriteOnce {
        async fn call(&self, context: &CommitCallbackContext) -> Result<()> {
            if context.snapshot.commit_kind() == &CommitKind::OVERWRITE
                && !self.0.swap(true, Ordering::SeqCst)
            {
                return Err(crate::Error::DataInvalid {
                    message: "interrupt overwrite maintenance".into(),
                    source: None,
                });
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn overwrite_recovery_refreshes_dv_on_its_inputs() {
        overwrite_recovery_after_dv_update(true).await;
    }

    #[tokio::test]
    async fn overwrite_recovery_refreshes_history_and_leaves_later_append_pending() {
        overwrite_recovery_after_dv_update(false).await;
    }

    async fn overwrite_recovery_after_dv_update(mask_overwrite_input: bool) {
        let (write_only, _, _) = fixture().await;
        let table =
            write_only.copy_with_options(HashMap::from([("write-only".into(), "false".into())]));
        let builder = table
            .new_write_builder()
            .with_commit_user("overwrite-recovery")
            .unwrap()
            .with_overwrite();
        let batch = RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int32Array::from(vec![1, 2])) as arrow_array::ArrayRef,
            ),
            (
                "value",
                Arc::new(Int32Array::from(vec![10, 20])) as arrow_array::ArrayRef,
            ),
        ])
        .unwrap();
        let mut writer = builder.new_write().unwrap();
        writer.write_arrow_batch(&batch).await.unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        let input = messages[0].new_files[0].file_name.clone();
        let mut commit = builder.new_commit();
        commit
            .add_commit_callback(Arc::new(InterruptOverwriteOnce(false.into())))
            .unwrap();
        let error = commit
            .overwrite_with_identifier(messages.clone(), None, 7)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("interrupt overwrite maintenance"),
            "{error}"
        );

        let manager = table.snapshot_manager();
        let original = manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(original.commit_kind(), &CommitKind::OVERWRITE);
        let (_, pending) = write(&write_only, vec![3, 4], 0).await;
        let masked_file = if mask_overwrite_input {
            &input
        } else {
            &pending.file_name
        };
        replace_vectors(&write_only, &[(masked_file, &[0])]).await;
        let modified = manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(modified.commit_kind(), &CommitKind::OVERWRITE);
        assert!(modified.index_manifest().is_some());

        let first = commit
            .overwrite_with_identifier(messages.clone(), None, 7)
            .await;
        let second = commit.overwrite_with_identifier(messages, None, 7).await;
        assert!(
            first.is_ok() && second.is_ok(),
            "first: {first:?}, retry: {second:?}"
        );
        let recovered = manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(recovered.id(), modified.id() + 1);
        assert_eq!(recovered.commit_kind(), &CommitKind::COMPACT);
        assert_eq!(recovered.changelog_record_count().unwrap_or_default(), 0);
        let active =
            crate::table::snapshot_manifest::read_snapshot_entries(&manager, &recovered, None)
                .await
                .unwrap();
        assert_eq!(active.len(), 2);
        assert!(active.iter().any(|entry| entry.file() == &pending));
        assert!(active.iter().all(|entry| entry.file().file_name != input));
        assert_eq!(
            active
                .iter()
                .filter(|entry| entry.file().level > 0)
                .map(|entry| entry.file().row_count)
                .sum::<i64>(),
            if mask_overwrite_input { 1 } else { 2 }
        );
        assert_eq!(
            rows(&table).await,
            if mask_overwrite_input {
                vec![(2, 20)]
            } else {
                vec![(1, 10), (2, 20)]
            }
        );

        let builder = table.new_compact_builder();
        let plan = builder.new_planner().unwrap().plan().await.unwrap();
        let result = builder
            .new_executor()
            .unwrap()
            .execute(&plan.tasks()[0])
            .await
            .unwrap();
        builder
            .new_commit()
            .unwrap()
            .commit_compact(vec![result])
            .await
            .unwrap();
        let completed = manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(
            completed.changelog_record_count(),
            Some(if mask_overwrite_input { 2 } else { 1 })
        );
        assert_eq!(
            rows(&table).await,
            if mask_overwrite_input {
                vec![(2, 20), (3, 300), (4, 400)]
            } else {
                vec![(1, 10), (2, 20), (4, 400)]
            }
        );
    }

    async fn fixture() -> (Table, String, String) {
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
            .filter(|entry| entry.index_file.index_type == DELETION_VECTORS_INDEX_TYPE)
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

    struct OverwriteCasRace {
        table: Table,
        compact_calls: AtomicUsize,
        rejected_paths: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl SnapshotCommit for OverwriteCasRace {
        async fn commit(
            &self,
            _: Option<&str>,
            snapshot: &Snapshot,
            _: &str,
            _: &[PartitionStatistics],
        ) -> Result<bool> {
            if snapshot.commit_kind() == &CommitKind::COMPACT
                && self.compact_calls.fetch_add(1, Ordering::SeqCst) == 0
            {
                let commit = self.table.new_write_builder().new_commit();
                let paths = commit
                    .read_delta_entries(None, snapshot)
                    .await?
                    .into_iter()
                    .filter(|entry| *entry.kind() == FileKind::Add)
                    .map(|entry| {
                        let bucket = commit.bucket_path(entry.partition(), entry.bucket())?;
                        Ok(entry.file().collect_files(&bucket))
                    })
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .flatten()
                    .collect();
                *self.rejected_paths.lock().unwrap() = paths;
                let (_, pending) = write(&self.table, vec![3, 4], 0).await;
                replace_vectors(&self.table, &[(&pending.file_name, &[0])]).await;
                return Ok(false);
            }
            self.table
                .snapshot_manager()
                .commit_snapshot(snapshot)
                .await
        }
    }

    #[tokio::test]
    async fn overwrite_recovery_after_cas_loss_refreshes_dv_and_preserves_append_changelog() {
        let (write_only, _, _) = fixture().await;
        let table =
            write_only.copy_with_options(HashMap::from([("write-only".into(), "false".into())]));
        let builder = table
            .new_write_builder()
            .with_commit_user("overwrite-cas-recovery")
            .unwrap()
            .with_overwrite();
        let batch = RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int32Array::from(vec![1, 2])) as arrow_array::ArrayRef,
            ),
            (
                "value",
                Arc::new(Int32Array::from(vec![10, 20])) as arrow_array::ArrayRef,
            ),
        ])
        .unwrap();
        let mut writer = builder.new_write().unwrap();
        writer.write_arrow_batch(&batch).await.unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        let publisher = Arc::new(OverwriteCasRace {
            table: write_only,
            compact_calls: AtomicUsize::new(0),
            rejected_paths: Default::default(),
        });
        let mut commit = builder.new_commit();
        commit.snapshot_commit = publisher.clone();
        let error = commit
            .overwrite_with_identifier(messages.clone(), None, 7)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Concurrent PK deletion-vector compaction"),
            "{error}"
        );
        assert_eq!(publisher.compact_calls.load(Ordering::SeqCst), 1);
        let rejected_paths = publisher.rejected_paths.lock().unwrap().clone();
        assert!(!rejected_paths.is_empty());
        for path in rejected_paths {
            assert!(!table.file_io().exists(&path).await.unwrap(), "{path}");
        }
        let manager = table.snapshot_manager();
        let raced = manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(raced.commit_kind(), &CommitKind::OVERWRITE);

        commit
            .overwrite_with_identifier(messages.clone(), None, 7)
            .await
            .unwrap();
        let recovered = manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(recovered.id(), raced.id() + 1);
        assert_eq!(recovered.commit_kind(), &CommitKind::COMPACT);
        assert_eq!(recovered.changelog_record_count().unwrap_or_default(), 0);
        assert_eq!(rows(&table).await, vec![(1, 10), (2, 20)]);
        commit
            .overwrite_with_identifier(messages, None, 7)
            .await
            .unwrap();
        assert_eq!(publisher.compact_calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            manager.get_latest_snapshot_id().await.unwrap(),
            Some(recovered.id())
        );

        let builder = table.new_compact_builder();
        let plan = builder.new_planner().unwrap().plan().await.unwrap();
        assert_eq!(plan.tasks().len(), 1);
        assert!(plan.tasks()[0].files().iter().any(|file| file.level == 0));
        let result = builder
            .new_executor()
            .unwrap()
            .execute(&plan.tasks()[0])
            .await
            .unwrap();
        builder
            .new_commit()
            .unwrap()
            .commit_compact(vec![result])
            .await
            .unwrap();
        let completed = manager.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(rows(&table).await, vec![(1, 10), (2, 20), (4, 400)]);
        assert_eq!(completed.changelog_record_count(), Some(1));
        let read = table.new_read_builder();
        let changes = read
            .new_incremental_scan(
                crate::table::IncrementalScanMode::Changelog,
                recovered.id(),
                completed.id(),
            )
            .plan_combined()
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = read
            .new_read()
            .unwrap()
            .to_arrow(changes.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(batch_rows(batches), vec![(4, 400)]);
    }

    struct LostAutomaticCompactResponse {
        table: Table,
        replayed: bool,
    }

    #[async_trait::async_trait]
    impl SnapshotCommit for LostAutomaticCompactResponse {
        async fn commit(
            &self,
            _: Option<&str>,
            snapshot: &Snapshot,
            _: &str,
            _: &[PartitionStatistics],
        ) -> Result<bool> {
            let published = self
                .table
                .snapshot_manager()
                .commit_snapshot(snapshot)
                .await?;
            if snapshot.commit_kind() != &CommitKind::COMPACT {
                return Ok(published);
            }
            assert!(published);
            if self.replayed {
                // A replay of a successful conditional write sees an existing snapshot.
                Ok(false)
            } else {
                Err(crate::Error::IoUnsupported {
                    message: "lost automatic compaction response".into(),
                })
            }
        }
    }

    #[tokio::test]
    async fn automatic_compaction_retains_published_outputs_after_lost_response() {
        for replayed in [false, true] {
            let (table, anchor, _) = fixture().await;
            let table = table.copy_with_options(HashMap::from([
                ("write-only".into(), "false".into()),
                ("commit.max-retries".into(), "0".into()),
            ]));
            let builder = table.new_write_builder();
            let mut writer = builder.new_write().unwrap();
            let batch = RecordBatch::try_from_iter([
                (
                    "id",
                    Arc::new(Int32Array::from(vec![42])) as arrow_array::ArrayRef,
                ),
                (
                    "value",
                    Arc::new(Int32Array::from(vec![4200])) as arrow_array::ArrayRef,
                ),
            ])
            .unwrap();
            writer.write_arrow_batch(&batch).await.unwrap();
            let messages = writer.prepare_commit().await.unwrap();
            let mut commit = builder.new_commit();
            commit.snapshot_commit = Arc::new(LostAutomaticCompactResponse {
                table: table.clone(),
                replayed,
            });
            let error = commit
                .commit_with_identifier(messages.clone(), 7)
                .await
                .unwrap_err();
            assert!(
                error.to_string().contains("outcome may be unknown"),
                "{error}"
            );
            let manager = table.snapshot_manager();
            let published = manager.get_latest_snapshot().await.unwrap().unwrap();
            assert_eq!(published.commit_kind(), &CommitKind::COMPACT);
            commit
                .filter_and_commit_with_identifier(messages, 7)
                .await
                .unwrap();
            assert_eq!(
                manager.get_latest_snapshot_id().await.unwrap(),
                Some(published.id())
            );
            assert_eq!(
                vectors(&table).await.0[&anchor],
                [42, 500].into_iter().collect()
            );
            let matching: Vec<_> = rows(&table)
                .await
                .into_iter()
                .filter(|(id, _)| *id == 42)
                .collect();
            assert_eq!(matching, vec![(42, 4200)]);
        }
    }

    struct CompetingCompactCommit {
        table: Table,
        commit: TableCommit,
        result: std::sync::Mutex<Option<crate::table::CompactResult>>,
        calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl SnapshotCommit for CompetingCompactCommit {
        async fn commit(
            &self,
            _: Option<&str>,
            snapshot: &Snapshot,
            _: &str,
            _: &[PartitionStatistics],
        ) -> Result<bool> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let competing = self.result.lock().unwrap().take();
            if let Some(result) = competing {
                self.commit.commit_compact(vec![result]).await?;
                return Ok(false);
            }
            self.table
                .snapshot_manager()
                .commit_snapshot(snapshot)
                .await
        }
    }

    #[tokio::test]
    async fn compact_cas_retry_rechecks_same_bucket_history() {
        let (table, _, _) = fixture().await;
        write(&table, (3000..3100).collect(), 0).await;
        let builder = table.new_compact_builder();
        let plan = builder.new_planner().unwrap().plan().await.unwrap();
        let result = builder
            .new_executor()
            .unwrap()
            .execute(&plan.tasks()[0])
            .await
            .unwrap();
        builder
            .new_commit()
            .unwrap()
            .commit_compact(vec![result])
            .await
            .unwrap();

        let competing_table = table.copy_with_options(HashMap::from([
            ("num-sorted-run.compaction-trigger".into(), "2".into()),
            (
                "compaction.max-size-amplification-percent".into(),
                "0".into(),
            ),
        ]));
        let competing = competing_table.new_compact_builder();
        let plan = competing.new_planner().unwrap().plan().await.unwrap();
        assert!(plan.tasks()[0].files().iter().all(|file| file.level > 0));
        let result = competing
            .new_executor()
            .unwrap()
            .execute(&plan.tasks()[0])
            .await
            .unwrap();
        let publisher = Arc::new(CompetingCompactCommit {
            table: table.clone(),
            commit: competing.new_commit().unwrap(),
            result: std::sync::Mutex::new(Some(result)),
            calls: AtomicUsize::new(0),
        });

        write(&table, vec![4000], 0).await;
        let builder = table.new_compact_builder();
        let plan = builder.new_planner().unwrap().plan().await.unwrap();
        assert!(plan.tasks()[0].files().iter().all(|file| file.level == 0));
        let result = builder
            .new_executor()
            .unwrap()
            .execute(&plan.tasks()[0])
            .await
            .unwrap();
        let mut commit = builder.new_commit().unwrap();
        commit.snapshot_commit = publisher.clone();
        commit.commit_min_retry_wait_ms = 0;
        commit.commit_max_retry_wait_ms = 0;
        let error = commit.commit_compact(vec![result]).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Concurrent PK deletion-vector compaction"),
            "{error}"
        );
        assert_eq!(publisher.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            table
                .snapshot_manager()
                .get_latest_snapshot_id()
                .await
                .unwrap(),
            Some(plan.snapshot_id().unwrap() + 1)
        );
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
            let paths = dv_paths(&self.table, snapshot).await;
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
                        message: "lost compact response".into(),
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
    async fn compact_cas_loss_cleans_only_new_dv_but_publication_error_preserves_it() {
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
            assert_eq!(
                table.file_io().exists(&attempts[0][0]).await.unwrap(),
                publish_first
            );
            for path in original_paths.iter().chain(attempts.last().unwrap()) {
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

    #[tokio::test]
    async fn selected_dv_container_repacking_in_an_overwrite_invalidates_compaction() {
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
            let reason = if selected_changed {
                "input deletion vectors changed"
            } else {
                "Concurrent PK deletion-vector compaction"
            };
            let error = published.unwrap_err();
            assert!(error.to_string().contains(reason), "{error}");
            assert_eq!(
                table
                    .snapshot_manager()
                    .get_latest_snapshot_id()
                    .await
                    .unwrap(),
                Some(latest.id())
            );
            for path in old_paths {
                assert!(table.file_io().exists(&path).await.unwrap());
            }
        }
    }
}
