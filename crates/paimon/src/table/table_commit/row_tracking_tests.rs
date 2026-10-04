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

// Persisted metadata must follow Java RowTrackingCommitUtils, including entries
// written by other engines with sequence numbers that are already assigned.
#[tokio::test]
async fn row_tracking_sequence_assignment_preserves_existing_versions() {
    let io = test_file_io();
    let path = "memory:/row-tracking-sequences";
    setup_dirs(&io, path).await;
    let commit = setup_data_evolution_commit(&io, path);
    let mut initial = test_data_file("initial.parquet", 10);
    initial.file_source = Some(0);
    commit
        .commit(vec![CommitMessage::new(
            EMPTY_SERIALIZED_ROW.clone(),
            0,
            vec![initial],
        )])
        .await
        .unwrap();

    for (index, min, max, expected_min, expected_max) in [
        (2, 0, 0, 2, 2),
        (3, 1, 0, 1, 3),
        (4, 1, 2, 1, 2),
        (5, 0, 2, 5, 5),
    ] {
        let mut file = test_data_file(&format!("partial-{index}.parquet"), 10);
        file.file_source = Some(0);
        file.first_row_id = Some(0);
        file.write_cols = Some(vec!["name".into()]);
        file.min_sequence_number = min;
        file.max_sequence_number = max;
        file.column_max_sequence_numbers = Some(vec![max]);
        let mut message = CommitMessage::new(EMPTY_SERIALIZED_ROW.clone(), 0, vec![file]);
        message.check_from_snapshot = Some(index - 1);
        commit.commit(vec![message]).await.unwrap();
        let snapshot = latest_snapshot(&io, path).await.unwrap();
        assert_eq!(snapshot.id(), index);
        assert_eq!(snapshot.next_row_id(), Some(10));
        let entries = commit.read_delta_entries(None, &snapshot).await.unwrap();
        let file = entries[0].file();
        assert_eq!(
            (file.min_sequence_number, file.max_sequence_number),
            (expected_min, expected_max)
        );
        assert_eq!(file.first_row_id, Some(0));
        assert_eq!(file.column_max_sequence_numbers, Some(vec![max]));
    }
}

#[tokio::test]
async fn row_tracking_overwrite_retains_deleted_file_sequences() {
    let io = test_file_io();
    let path = "memory:/row-tracking-overwrite-sequences";
    setup_dirs(&io, path).await;
    let commit = setup_row_tracking_commit(&io, path);
    let mut file = test_data_file("initial.parquet", 10);
    file.file_source = Some(0);
    commit
        .commit(vec![CommitMessage::new(
            EMPTY_SERIALIZED_ROW.clone(),
            0,
            vec![file],
        )])
        .await
        .unwrap();
    let mut replacement = test_data_file("replacement.parquet", 4);
    replacement.file_source = Some(0);
    commit
        .overwrite(
            vec![CommitMessage::new(
                EMPTY_SERIALIZED_ROW.clone(),
                0,
                vec![replacement],
            )],
            None,
        )
        .await
        .unwrap();
    let snapshot = latest_snapshot(&io, path).await.unwrap();
    assert_eq!(snapshot.next_row_id(), Some(14));
    let entries = commit.read_delta_entries(None, &snapshot).await.unwrap();
    assert_eq!(entries.len(), 2);
    for entry in entries {
        let expected = if *entry.kind() == FileKind::Delete {
            (1, 0)
        } else {
            (2, 10)
        };
        assert_eq!(entry.file().min_sequence_number, expected.0);
        assert_eq!(entry.file().max_sequence_number, expected.0);
        assert_eq!(entry.file().first_row_id, Some(expected.1));
    }
}

#[tokio::test]
async fn row_tracking_partition_grouping_follows_java_option() {
    for enabled in [None, Some(true), Some(false)] {
        let io = test_file_io();
        let path = format!("memory:/row-tracking-partition-{enabled:?}");
        setup_dirs(&io, &path).await;
        let mut options = HashMap::from([("row-tracking.enabled".to_string(), "true".to_string())]);
        if let Some(enabled) = enabled {
            options.insert(
                "row-tracking.partition-group-on-commit".into(),
                enabled.to_string(),
            );
        }
        let table = test_partitioned_table(&io, &path).copy_with_options(options);
        let commit = TableCommit::new(table, "test-user".into());
        let messages = [("a", "a-1", 2), ("b", "b-1", 3), ("a", "a-2", 4)]
            .into_iter()
            .map(|(partition, name, count)| {
                let mut file = test_data_file(&format!("{name}.parquet"), count);
                file.file_source = Some(0);
                CommitMessage::new(partition_bytes(partition), 0, vec![file])
            })
            .collect();
        commit.commit(messages).await.unwrap();
        let snapshot = latest_snapshot(&io, &path).await.unwrap();
        assert_eq!(snapshot.next_row_id(), Some(9));
        let entries = commit.read_delta_entries(None, &snapshot).await.unwrap();
        let actual: Vec<_> = entries
            .iter()
            .map(|e| (e.file().file_name.as_str(), e.file().first_row_id))
            .collect();
        let expected = if enabled == Some(false) {
            vec![
                ("a-1.parquet", Some(0)),
                ("b-1.parquet", Some(2)),
                ("a-2.parquet", Some(5)),
            ]
        } else {
            vec![
                ("a-1.parquet", Some(0)),
                ("a-2.parquet", Some(2)),
                ("b-1.parquet", Some(6)),
            ]
        };
        assert_eq!(actual, expected, "grouping={enabled:?}");
    }
}

#[test]
fn row_tracking_groups_preserve_independently_rolled_blob_columns() {
    fn entry(partition: &str, name: &str, rows: i64, column: Option<&str>) -> ManifestEntry {
        let mut file = test_data_file(name, rows);
        file.file_source = Some(0);
        file.write_cols = column.map(|name| vec![name.to_string()]);
        ManifestEntry::new(FileKind::Add, partition_bytes(partition), 0, -1, file, 2)
    }
    // Writers can interleave partitions while each Blob column rolls at a
    // different size. Grouping must retain normal-file / Blob-file order.
    let entries = vec![
        entry("a", "a-0.parquet", 5, None),
        entry("a", "a-left-0.blob", 2, Some("left")),
        entry("a", "a-left-1.blob", 3, Some("left")),
        entry("a", "a-right-0.blob", 5, Some("right")),
        entry("b", "b-0.parquet", 2, None),
        entry("b", "b-left-0.blob", 2, Some("left")),
        entry("a", "a-1.parquet", 3, None),
        entry("a", "a-left-2.blob", 3, Some("left")),
        entry("a", "a-right-1.blob", 1, Some("right")),
        entry("a", "a-right-2.blob", 2, Some("right")),
    ];
    let (entries, next) =
        row_tracking::assign_row_tracking(7, 10, row_tracking::group_by_partition(entries))
            .unwrap();
    assert_eq!(next, 20);
    let row_ids: Vec<_> = entries
        .iter()
        .map(|entry| (entry.file().file_name.as_str(), entry.file().first_row_id))
        .collect();
    assert_eq!(
        row_ids,
        vec![
            ("a-0.parquet", Some(10)),
            ("a-left-0.blob", Some(10)),
            ("a-left-1.blob", Some(12)),
            ("a-right-0.blob", Some(10)),
            ("a-1.parquet", Some(15)),
            ("a-left-2.blob", Some(15)),
            ("a-right-1.blob", Some(15)),
            ("a-right-2.blob", Some(16)),
            ("b-0.parquet", Some(18)),
            ("b-left-0.blob", Some(18)),
        ]
    );
    assert!(entries.iter().all(
        |entry| entry.file().min_sequence_number == 7 && entry.file().max_sequence_number == 7
    ));
}

#[test]
fn row_tracking_keeps_explicit_row_ids_and_completed_rewrite_metadata() {
    let mut embedded = test_data_file("embedded.parquet", 3);
    embedded.file_source = Some(0);
    embedded.write_cols = Some(vec!["id".into(), crate::spec::ROW_ID_FIELD_NAME.into()]);
    embedded.min_sequence_number = 2;
    embedded.max_sequence_number = 0;
    let mut rewritten = test_data_file("rewritten.parquet", 6);
    rewritten.file_source = Some(1);
    rewritten.first_row_id = Some(20);
    rewritten.min_sequence_number = 2;
    rewritten.max_sequence_number = 4;
    let entries = [embedded, rewritten]
        .into_iter()
        .map(|file| ManifestEntry::new(FileKind::Add, EMPTY_SERIALIZED_ROW.clone(), 0, -1, file, 2))
        .collect();
    let (entries, next) = row_tracking::assign_row_tracking(9, 100, entries).unwrap();
    assert_eq!(next, 100);
    assert_eq!(entries[0].file().first_row_id, None);
    assert_eq!(entries[0].file().min_sequence_number, 2);
    assert_eq!(entries[0].file().max_sequence_number, 9);
    assert_eq!(entries[1].file().first_row_id, Some(20));
    assert_eq!(entries[1].file().min_sequence_number, 2);
    assert_eq!(entries[1].file().max_sequence_number, 4);
}

struct ConcurrentRowTrackingAppend {
    table: Table,
    attempts: std::sync::Mutex<Vec<Snapshot>>,
}

#[async_trait::async_trait]
impl SnapshotCommit for ConcurrentRowTrackingAppend {
    async fn commit(
        &self,
        _: Option<&str>,
        snapshot: &Snapshot,
        _: &[PartitionStatistics],
    ) -> Result<bool> {
        let first = {
            let mut attempts = self.attempts.lock().unwrap();
            attempts.push(snapshot.clone());
            attempts.len() == 1
        };
        if first {
            let mut file = test_data_file("concurrent.parquet", 3);
            file.file_source = Some(0);
            TableCommit::new(self.table.clone(), "other".into())
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
async fn row_tracking_retry_reassigns_unpublished_ids_and_is_idempotent() {
    let io = test_file_io();
    let path = "memory:/row-tracking-retry";
    setup_dirs(&io, path).await;
    let table = test_data_evolution_table(&io, path).copy_with_options(HashMap::from([
        ("manifest.sidecar.enabled".into(), "true".into()),
        ("manifest.target-file-size".into(), "1 B".into()),
    ]));
    let mut commit = TableCommit::new(table.clone(), "job".into());
    commit.commit_min_retry_wait_ms = 0;
    commit.commit_max_retry_wait_ms = 0;
    let publisher = Arc::new(ConcurrentRowTrackingAppend {
        table,
        attempts: std::sync::Mutex::new(Vec::new()),
    });
    commit.snapshot_commit = publisher.clone();
    let mut file = test_data_file("pending.parquet", 4);
    file.file_source = Some(0);
    let message = CommitMessage::new(EMPTY_SERIALIZED_ROW.clone(), 0, vec![file]);
    commit
        .commit_with_identifier(vec![message.clone()], 42)
        .await
        .unwrap();
    let attempts = publisher.attempts.lock().unwrap().clone();
    assert_eq!(
        attempts
            .iter()
            .map(|s| (s.id(), s.next_row_id()))
            .collect::<Vec<_>>(),
        vec![(1, Some(4)), (2, Some(7))]
    );
    let snapshot = latest_snapshot(&io, path).await.unwrap();
    assert_eq!(snapshot.id(), 2);
    assert_eq!(snapshot.next_row_id(), Some(7));
    let entries = commit.read_delta_entries(None, &snapshot).await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].file().first_row_id, Some(3));
    assert_eq!(entries[0].file().min_sequence_number, 2);
    assert_eq!(entries[0].file().max_sequence_number, 2);
    let before = manifest_paths(&io, path).await;
    commit
        .filter_and_commit_with_identifier(vec![message], 42)
        .await
        .unwrap();
    assert_eq!(publisher.attempts.lock().unwrap().len(), 2);
    assert_eq!(manifest_paths(&io, path).await, before);
    assert_eq!(
        latest_snapshot(&io, path).await.unwrap().next_row_id(),
        Some(7)
    );
}

#[tokio::test]
async fn row_tracking_dedicated_updates_require_one_complete_normal_range() {
    // The latest snapshot may contain two adjacent normal ranges after another
    // engine rewrites a former [0, 9] file. A stale Blob must not span both.
    for extension in ["blob", "vector.vortex"] {
        for (start, count, valid) in [
            (1, 3, true),
            (0, 5, true),
            (0, 10, false),
            (4, 2, false),
            (9, 2, false),
        ] {
            let io = test_file_io();
            let path = format!("memory:/dedicated-range-{extension}-{start}-{count}");
            setup_dirs(&io, &path).await;
            let commit = setup_data_evolution_commit(&io, &path);
            let files = ["first.parquet", "second.parquet"]
                .into_iter()
                .map(|name| {
                    let mut file = test_data_file(name, 5);
                    file.file_source = Some(0);
                    file
                })
                .collect();
            commit
                .commit(vec![CommitMessage::new(
                    EMPTY_SERIALIZED_ROW.clone(),
                    0,
                    files,
                )])
                .await
                .unwrap();
            let mut file = test_data_file(&format!("update.{extension}"), count);
            file.file_source = Some(0);
            file.first_row_id = Some(start);
            file.write_cols = Some(vec!["name".into()]);
            let mut message = CommitMessage::new(EMPTY_SERIALIZED_ROW.clone(), 0, vec![file]);
            message.check_from_snapshot = Some(1);
            let result = commit.commit(vec![message]).await;
            if valid {
                result.unwrap();
                assert_eq!(latest_snapshot(&io, &path).await.unwrap().id(), 2);
            } else {
                let error = result.expect_err("dedicated range must fit in one normal file");
                assert!(
                    error.to_string().contains("Row ID existence conflict"),
                    "{error}"
                );
                assert_eq!(latest_snapshot(&io, &path).await.unwrap().id(), 1);
            }
        }
    }
}

#[test]
fn row_tracking_dedicated_file_cannot_supply_normal_range_existence() {
    let io = test_file_io();
    let commit = setup_data_evolution_commit(&io, "memory:/dedicated-existence");
    let entry = |name: &str, start: i64, rows: i64| {
        let mut file = test_data_file(name, rows);
        file.file_source = Some(0);
        file.first_row_id = Some(start);
        file.write_cols = Some(vec!["name".into()]);
        ManifestEntry::new(FileKind::Add, EMPTY_SERIALIZED_ROW.clone(), 0, -1, file, 2)
    };
    let base = vec![entry("first.parquet", 0, 5), entry("stale.blob", 0, 10)];
    for name in ["update.blob", "update.parquet"] {
        let delta = vec![entry(name, 0, 10)];
        assert!(commit
            .check_row_id_existence(&base, &delta, Some(10))
            .is_err());
    }
    // The merged-layout check must also catch an older dedicated file that
    // remains after a change to the normal file boundaries.
    let merged = vec![
        entry("first.parquet", 0, 5),
        entry("second.parquet", 5, 5),
        entry("stale.blob", 0, 10),
    ];
    assert!(commit
        .check_row_id_range_conflicts(&CommitKind::APPEND, Some(1), &merged)
        .is_err());
    assert!(commit
        .check_row_id_range_conflicts(&CommitKind::APPEND, None, &merged)
        .is_ok());
}

#[tokio::test]
async fn row_tracking_ignore_index_update_preserves_index_metadata() {
    let io = test_file_io();
    let path = "memory:/ignore-index-update";
    setup_dirs(&io, path).await;
    let table = test_data_evolution_table(&io, path).copy_with_options(HashMap::from([(
        "global-index.column-update-action".into(),
        "IGNORE".into(),
    )]));
    let commit = TableCommit::new(table, "test-user".into());
    let mut file = test_data_file("initial.parquet", 10);
    file.file_source = Some(0);
    let mut seed = CommitMessage::new(EMPTY_SERIALIZED_ROW.clone(), 0, vec![file]);
    let index = test_global_index_file("global-name.index", 1, 0, 9);
    seed.new_index_files.push(index.clone());
    commit.commit(vec![seed]).await.unwrap();
    let mut partial = test_data_file("updated.parquet", 10);
    partial.file_source = Some(0);
    partial.first_row_id = Some(0);
    partial.write_cols = Some(vec!["name".into()]);
    let mut update = CommitMessage::new(EMPTY_SERIALIZED_ROW.clone(), 0, vec![partial]);
    update.check_from_snapshot = Some(1);
    commit.commit(vec![update]).await.unwrap();
    let snapshot = latest_snapshot(&io, path).await.unwrap();
    assert_eq!(snapshot.id(), 2);
    assert_eq!(snapshot.next_row_id(), Some(10));
    let index_entries = IndexManifest::read(
        &io,
        &format!("{path}/manifest/{}", snapshot.index_manifest().unwrap()),
    )
    .await
    .unwrap();
    assert_eq!(index_entries.len(), 1);
    assert_eq!(index_entries[0].index_file, index);
    assert_eq!(index_entries[0].kind, FileKind::Add);
}
