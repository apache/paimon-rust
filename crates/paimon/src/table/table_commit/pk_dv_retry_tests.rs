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
