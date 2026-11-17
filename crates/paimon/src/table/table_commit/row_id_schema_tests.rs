// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

mod row_id_schema {
    use super::*;
    use crate::spec::{BlobType, DataField, FloatType, IntType, RowType, VectorType};
    use std::sync::Mutex;

    async fn persist(table: &Table, schema: &TableSchema) {
        table
            .file_io()
            .new_output(&format!(
                "{}/schema/schema-{}",
                table.location(),
                schema.id()
            ))
            .unwrap()
            .write(serde_json::to_vec(schema).unwrap().into())
            .await
            .unwrap();
    }

    fn evolve(table: &Table, recreate: bool) -> TableSchema {
        let mut schema = serde_json::to_value(table.schema()).unwrap();
        schema["id"] = 1.into();
        if recreate {
            schema["fields"][1]["id"] = 2.into();
            schema["highestFieldId"] = 2.into();
        } else {
            schema["fields"][1]["name"] = "renamed".into();
        }
        serde_json::from_value(schema).unwrap()
    }

    async fn fixture(path: &str) -> TableCommit {
        let io = test_file_io();
        setup_dirs(&io, path).await;
        let commit = setup_data_evolution_commit(&io, path);
        persist(&commit.table, commit.table.schema()).await;
        let mut file = test_data_file("base.parquet", 10);
        file.file_source = Some(0);
        commit
            .commit(vec![CommitMessage::new(
                EMPTY_SERIALIZED_ROW.clone(),
                0,
                vec![file],
            )])
            .await
            .unwrap();
        commit
    }

    #[tokio::test]
    async fn standalone_check_tracks_renamed_ids_across_snapshot_schemas() {
        for overlapping in [false, true] {
            let path = format!("memory:/cross-schema-check-{overlapping}");
            let commit = fixture(&path).await;
            let schema = evolve(&commit.table, false);
            persist(&commit.table, &schema).await;
            let latest = commit
                .table
                .copy_with_resolved_schema(schema, "main")
                .unwrap();
            let mut concurrent = row_id_check_message("concurrent.parquet", "renamed");
            concurrent.new_files[0].schema_id = 1;
            latest
                .new_write_builder()
                .new_commit()
                .commit(vec![concurrent])
                .await
                .unwrap();
            let staged =
                row_id_check_message("staged.parquet", if overlapping { "name" } else { "id" });
            let result = commit.check_row_id_conflicts(&[staged], 2, 1).await;
            if overlapping {
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("multiple MERGE INTO"));
            } else {
                result.unwrap();
            }
        }
    }

    struct EvolveBeforePublication {
        table: Table,
        recreate: bool,
        attempts: Mutex<Vec<Snapshot>>,
    }

    #[async_trait::async_trait]
    impl SnapshotCommit for EvolveBeforePublication {
        async fn commit(
            &self,
            _: Option<&str>,
            snapshot: &Snapshot,
            _: &str,
            _: &[PartitionStatistics],
        ) -> Result<bool> {
            let first = {
                let mut attempts = self.attempts.lock().unwrap();
                attempts.push(snapshot.clone());
                attempts.len() == 1
            };
            if first {
                let schema = evolve(&self.table, self.recreate);
                persist(&self.table, &schema).await;
                let current = self.table.copy_with_resolved_schema(schema, "main")?;
                let mut file = test_data_file("competing-append.parquet", 1);
                file.schema_id = 1;
                file.file_source = Some(0);
                current
                    .new_write_builder()
                    .new_commit()
                    .commit(vec![CommitMessage::new(
                        EMPTY_SERIALIZED_ROW.clone(),
                        0,
                        vec![file],
                    )])
                    .await?;
                return Ok(false);
            }
            self.table
                .snapshot_manager()
                .commit_snapshot(snapshot)
                .await
        }
    }

    #[tokio::test]
    async fn publication_retry_revalidates_update_ids_for_append_and_overwrite() {
        for overwrite in [false, true] {
            for recreate in [false, true] {
                let path = format!("memory:/row-id-schema-retry-{overwrite}-{recreate}");
                let mut commit = fixture(&path).await;
                let mut staged = row_id_check_message("staged.parquet", "name");
                if overwrite {
                    staged
                        .new_index_files
                        .push(test_deletion_vector_index_file("delete.dv", "base.parquet"));
                }
                let staged_path = format!("{path}/bucket-0/staged.parquet");
                commit
                    .table
                    .file_io()
                    .new_output(&staged_path)
                    .unwrap()
                    .write(bytes::Bytes::from_static(b"prepared update"))
                    .await
                    .unwrap();
                let publisher = Arc::new(EvolveBeforePublication {
                    table: commit.table.clone(),
                    recreate,
                    attempts: Mutex::new(vec![]),
                });
                commit.snapshot_commit = publisher.clone();
                commit.commit_min_retry_wait_ms = 0;
                commit.commit_max_retry_wait_ms = 0;
                let result = commit.commit(vec![staged]).await;
                let snapshot = commit
                    .snapshot_manager
                    .get_latest_snapshot()
                    .await
                    .unwrap()
                    .unwrap();
                let attempts = publisher.attempts.lock().unwrap().clone();
                assert_eq!(attempts[0].schema_id(), 0);
                assert_eq!(
                    attempts[0].commit_kind(),
                    if overwrite {
                        &CommitKind::OVERWRITE
                    } else {
                        &CommitKind::APPEND
                    }
                );
                if recreate {
                    assert!(result.unwrap_err().to_string().contains("dropped field"));
                    assert_eq!(snapshot.id(), 2);
                    assert_eq!(attempts.len(), 1, "reject before another publication");
                } else {
                    result.unwrap();
                    assert_eq!(snapshot.id(), 3);
                    assert_eq!(attempts.len(), 2);
                    assert_eq!(attempts[1].schema_id(), 1);
                    let entries = active_entries(commit.table.file_io(), &path, &snapshot).await;
                    assert!(entries
                        .iter()
                        .any(|entry| entry.file().file_name == "staged.parquet"
                            && entry.file().schema_id == 0));
                }
                assert!(commit.table.file_io().exists(&staged_path).await.unwrap());
            }
        }
    }

    #[tokio::test]
    async fn compact_write_cols_exclude_dedicated_field_conflicts() {
        for nested in [false, true] {
            let schema = crate::spec::Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("payload", DataType::Blob(BlobType::new()))
                .column(
                    "embedding",
                    DataType::Vector(
                        VectorType::try_new(true, 4, DataType::Float(FloatType::new())).unwrap(),
                    ),
                )
                .option("data-evolution.enabled", "true")
                .option("row-tracking.enabled", "true")
                .option("data-evolution.write-cols-optimization.enabled", "true")
                .option("data-evolution.nested-field.enabled", nested.to_string())
                .option("vector.file.format", "vortex")
                .option("write-only", "true")
                .build()
                .unwrap();
            let table = Table::new(
                test_file_io(),
                Identifier::new("default", "compact_cols"),
                "memory:/compact_cols".into(),
                TableSchema::new(0, &schema),
                None,
            );
            let commit = table.new_write_builder().new_commit();
            let mut file = test_data_file("normal.parquet", 10);
            file.first_row_id = Some(0);
            assert_eq!(
                commit.write_field_ids(&file).await.unwrap(),
                HashSet::from([0])
            );
            setup_dirs(table.file_io(), table.location()).await;
            let mut base = test_data_file("base.parquet", 10);
            base.file_source = Some(0);
            commit
                .commit(vec![CommitMessage::new(
                    EMPTY_SERIALIZED_ROW.clone(),
                    0,
                    vec![base],
                )])
                .await
                .unwrap();
            file.file_source = Some(0);
            let mut normal_update =
                CommitMessage::new(EMPTY_SERIALIZED_ROW.clone(), 0, vec![file.clone()]);
            normal_update.check_from_snapshot = Some(1);
            commit.commit(vec![normal_update]).await.unwrap();
            for column in ["payload", "embedding"] {
                file.write_cols = Some(vec![column.into()]);
                assert!(!commit.write_field_ids(&file).await.unwrap().contains(&0));
                let mut staged =
                    CommitMessage::new(EMPTY_SERIALIZED_ROW.clone(), 0, vec![file.clone()]);
                staged.new_files[0].file_name = if column == "payload" {
                    "staged.blob"
                } else {
                    "staged.vector.vortex"
                }
                .into();
                staged.check_from_snapshot = Some(1);
                commit
                    .check_row_id_conflicts(&[staged], 2, 1)
                    .await
                    .unwrap();
            }
        }
    }

    #[tokio::test]
    async fn commits_reject_dropped_descendants_in_written_complex_columns() {
        for container in ["row", "array", "map", "multiset"] {
            let child = DataType::Row(RowType::new(vec![DataField::new(
                0,
                "value".into(),
                DataType::Int(IntType::new()),
            )]));
            let data_type = match container {
                "array" => DataType::Array(crate::spec::ArrayType::new(child)),
                "multiset" => DataType::Multiset(crate::spec::MultisetType::new(child)),
                "map" => DataType::Map(crate::spec::MapType::new(
                    DataType::Int(IntType::new()),
                    child,
                )),
                _ => child,
            };
            let schema = crate::spec::Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("profile", data_type)
                .option("data-evolution.enabled", "true")
                .option("row-tracking.enabled", "true")
                .option("write-only", "true")
                .build()
                .unwrap();
            let path = format!("memory:/nested-schema-{container}");
            let io = test_file_io();
            setup_dirs(&io, &path).await;
            let table = Table::new(
                io,
                Identifier::new("default", "nested"),
                path.clone(),
                TableSchema::new(0, &schema),
                None,
            );
            persist(&table, table.schema()).await;
            let mut base = test_data_file("base.parquet", 10);
            base.file_source = Some(0);
            let commit = table.new_write_builder().new_commit();
            commit
                .commit(vec![CommitMessage::new(
                    EMPTY_SERIALIZED_ROW.clone(),
                    0,
                    vec![base],
                )])
                .await
                .unwrap();
            let mut changed = serde_json::to_value(table.schema()).unwrap();
            changed["id"] = 1.into();
            changed["highestFieldId"] = 3.into();
            let mut ty = &mut changed["fields"][1]["type"];
            if container == "array" || container == "multiset" {
                ty = &mut ty["element"];
            }
            if container == "map" {
                ty = &mut ty["value"];
            }
            ty["fields"][0]["id"] = 3.into();
            let changed: TableSchema = serde_json::from_value(changed).unwrap();
            persist(&table, &changed).await;
            let staged = row_id_check_message("staged.parquet", "profile");
            let error = commit.commit(vec![staged]).await.unwrap_err();
            assert!(
                error.to_string().contains("dropped field"),
                "{container}: {error}"
            );
            assert_eq!(
                commit
                    .snapshot_manager
                    .get_latest_snapshot_id()
                    .await
                    .unwrap(),
                Some(1)
            );
            // Dropping a descendant in an untouched column must not prevent
            // an independent partial update from publishing.
            commit
                .commit(vec![row_id_check_message("id-only.parquet", "id")])
                .await
                .unwrap();
            assert_eq!(
                commit
                    .snapshot_manager
                    .get_latest_snapshot_id()
                    .await
                    .unwrap(),
                Some(2)
            );
        }
    }

    #[tokio::test]
    async fn partial_row_write_ignores_dropped_unwritten_sibling() {
        let schema = crate::spec::Schema::builder()
            .column(
                "profile",
                DataType::Row(RowType::new(vec![
                    DataField::new(0, "keep".into(), DataType::Int(IntType::new())),
                    DataField::new(0, "drop".into(), DataType::Int(IntType::new())),
                ])),
            )
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true")
            .option("data-evolution.nested-field.enabled", "true")
            .option("write-only", "true")
            .build()
            .unwrap();
        let path = "memory:/partial-row-drop-sibling";
        let io = test_file_io();
        setup_dirs(&io, path).await;
        let table = Table::new(
            io,
            Identifier::new("default", "nested"),
            path.into(),
            TableSchema::new(0, &schema),
            None,
        );
        persist(&table, table.schema()).await;
        let commit = table.new_write_builder().new_commit();
        let mut base = test_data_file("base.parquet", 10);
        base.file_source = Some(0);
        commit
            .commit(vec![CommitMessage::new(
                EMPTY_SERIALIZED_ROW.clone(),
                0,
                vec![base],
            )])
            .await
            .unwrap();
        let mut changed = serde_json::to_value(table.schema()).unwrap();
        changed["id"] = 1.into();
        changed["fields"][0]["type"]["fields"]
            .as_array_mut()
            .unwrap()
            .pop();
        persist(&table, &serde_json::from_value(changed).unwrap()).await;
        commit
            .commit(vec![row_id_check_message("keep.parquet", "profile.keep")])
            .await
            .unwrap();
        let snapshot = commit
            .snapshot_manager
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.id(), 2);
        assert_eq!(snapshot.schema_id(), 1);
    }
}
