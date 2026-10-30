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

use super::*;
use bytes::Bytes;
use indexmap::IndexMap;
use paimon::spec::{DataFileMeta, IndexFileMeta, IndexManifest, IndexManifestEntry, Snapshot};
use paimon::table::CommitMessage;
use roaring::RoaringBitmap;

async fn dv_table(options: &[(&str, &str)]) -> Table {
    let mut properties = vec![
        ("deletion-vectors.enabled", "true"),
        ("deletion-vectors.merge-on-read", "false"),
        ("read.batch-size", "7"),
    ];
    properties.extend_from_slice(options);
    table(&properties).await
}

async fn latest(table: &Table) -> Snapshot {
    table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap()
}

async fn compact(table: &Table) -> Vec<CompactResult> {
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let results = execute(table, &plan).await;
    builder
        .new_commit()
        .unwrap()
        .commit_compact(results.clone())
        .await
        .unwrap();
    results
}

async fn seed(table: &Table) -> DataFileMeta {
    write_batch(table, &make_batch((0..1000).collect(), (0..1000).collect())).await;
    let results = compact(table).await;
    assert_eq!(results.len(), 1);
    let file = results[0].files()[0].clone();
    assert_eq!(file.level, 5);
    file
}

async fn all_files(table: &Table) -> Vec<DataFileMeta> {
    table
        .new_read_builder()
        .new_scan()
        .with_scan_all_files()
        .plan()
        .await
        .unwrap()
        .splits()
        .iter()
        .flat_map(|split| split.data_files())
        .cloned()
        .collect()
}

pub(super) async fn indexes(table: &Table) -> Vec<IndexManifestEntry> {
    match latest(table).await.index_manifest() {
        Some(name) => IndexManifest::read(
            table.file_io(),
            &table.snapshot_manager().manifest_path(name),
        )
        .await
        .unwrap(),
        None => vec![],
    }
}

fn index_path(table: &Table, bucket: i32, file: &IndexFileMeta) -> String {
    if table.schema().core_options().index_file_in_data_file_dir() {
        let directory = if bucket == -2 {
            "bucket-postpone".into()
        } else {
            format!("bucket-{bucket}")
        };
        format!("{}/{directory}/{}", table.location(), file.file_name)
    } else {
        format!("{}/index/{}", table.location(), file.file_name)
    }
}

// Java v1 envelope: version, then [length, magic+roaring, checksum] per file.
pub(super) async fn write_vectors(
    table: &Table,
    bucket: i32,
    deletions: &[(&str, &[u32])],
) -> IndexFileMeta {
    let mut bytes = vec![1];
    let mut ranges = IndexMap::new();
    for (name, positions) in deletions {
        let bitmap: RoaringBitmap = positions.iter().copied().collect();
        let mut payload = 1581511376u32.to_be_bytes().to_vec();
        bitmap.serialize_into(&mut payload).unwrap();
        ranges.insert(
            name.to_string(),
            paimon::spec::DeletionVectorMeta {
                offset: bytes.len() as i32,
                length: payload.len() as i32,
                cardinality: Some(bitmap.len() as i64),
            },
        );
        bytes.extend_from_slice(&(payload.len() as i32).to_be_bytes());
        bytes.extend_from_slice(&payload);
        bytes.extend_from_slice(&crc32fast::hash(&payload).to_be_bytes());
    }
    let file = IndexFileMeta {
        index_type: "DELETION_VECTORS".into(),
        file_name: format!("index-{}-1", uuid::Uuid::new_v4()),
        file_size: bytes.len() as i64,
        row_count: ranges.len() as i64,
        deletion_vectors_ranges: Some(ranges),
        external_path: None,
        global_index_meta: None,
    };
    let path = index_path(table, bucket, &file);
    table
        .file_io()
        .mkdirs(path.rsplit_once('/').unwrap().0)
        .await
        .unwrap();
    table
        .file_io()
        .new_output(&path)
        .unwrap()
        .write(Bytes::from(bytes))
        .await
        .unwrap();
    file
}

async fn set_vectors(table: &Table, task: &CompactTask, deletions: &[(&str, &[u32])]) {
    let mut message = CommitMessage::new(task.partition().to_vec(), task.bucket(), vec![]);
    message
        .new_index_files
        .push(write_vectors(table, task.bucket(), deletions).await);
    message.deleted_index_files = indexes(table)
        .await
        .into_iter()
        .filter(|entry| {
            let ranges = &entry.index_file.deletion_vectors_ranges;
            entry.partition == task.partition()
                && entry.bucket == task.bucket()
                && ranges.as_ref().is_some_and(|ranges| {
                    deletions.iter().any(|(name, _)| ranges.contains_key(*name))
                })
        })
        .map(|entry| entry.index_file)
        .collect();
    table
        .new_write_builder()
        .new_commit()
        .commit(vec![message])
        .await
        .unwrap();
}

pub(super) async fn positions(table: &Table, name: &str) -> RoaringBitmap {
    for entry in indexes(table).await {
        let ranges = entry.index_file.deletion_vectors_ranges.as_ref();
        if let Some(meta) = ranges.and_then(|ranges| ranges.get(name)) {
            let bytes = table
                .file_io()
                .new_input(&index_path(table, entry.bucket, &entry.index_file))
                .unwrap()
                .read()
                .await
                .unwrap();
            let offset = meta.offset as usize;
            assert_eq!(&bytes[offset + 4..offset + 8], &1581511376u32.to_be_bytes());
            return RoaringBitmap::deserialize_from(
                &bytes[offset + 8..offset + 4 + meta.length as usize],
            )
            .unwrap();
        }
    }
    RoaringBitmap::new()
}

#[tokio::test]
async fn dv_incremental_compact_keeps_history_unique_for_all_changelog_modes() {
    for producer in ["none", "input", "lookup"] {
        for bucket in ["1", "-1"] {
            let table = dv_table(&[
                ("changelog-producer", producer),
                ("bucket", bucket),
                (
                    "index-file-in-data-file-dir",
                    if bucket == "1" { "true" } else { "false" },
                ),
            ])
            .await;
            let anchor = seed(&table).await;
            let before = latest(&table).await;
            let original = (0..1000).map(|key| (key, key)).collect::<Vec<_>>();
            write_batch(
                &table,
                &make_batch_with_kinds(
                    vec![257, 258, 2000],
                    vec![25700, 999, 20000],
                    vec![0, 3, 0],
                ),
            )
            .await;
            assert_eq!(rows(&table).await, original, "default DV scan excludes L0");
            let hashes = indexes(&table)
                .await
                .into_iter()
                .filter(|entry| entry.index_file.index_type == "HASH")
                .collect::<Vec<_>>();
            let builder = table.new_compact_builder();
            let plan = builder.new_planner().unwrap().plan().await.unwrap();
            assert_eq!(plan.tasks().len(), 1);
            assert!(plan.tasks()[0].files().iter().all(|file| file.level == 0));
            let results = execute(&table, &plan).await;
            assert!(results[0]
                .files()
                .iter()
                .all(|file| file.level == 4 && file.delete_row_count == Some(0)));
            let commit = builder.new_commit().unwrap();
            let mut missing_updates = serde_json::to_value(&results).unwrap();
            for result in missing_updates.as_array_mut().unwrap() {
                assert!(result
                    .as_object_mut()
                    .unwrap()
                    .remove("deletion_vector_updates")
                    .is_some());
            }
            assert!(commit
                .commit_compact(serde_json::from_value(missing_updates.clone()).unwrap())
                .await
                .is_err());
            assert_eq!(latest(&table).await.id(), plan.snapshot_id().unwrap());
            let mut legacy = serde_json::to_value(&results).unwrap();
            for result in legacy.as_array_mut().unwrap() {
                result["task"]
                    .as_object_mut()
                    .unwrap()
                    .remove("output_level");
            }
            commit
                .commit_compact(serde_json::from_value(legacy).unwrap())
                .await
                .unwrap();
            let after = latest(&table).await;
            commit
                .commit_compact(serde_json::from_value(missing_updates).unwrap())
                .await
                .unwrap();
            commit.commit_compact(results).await.unwrap();
            assert_eq!(latest(&table).await.id(), after.id());
            let mut expected = original
                .iter()
                .copied()
                .filter(|(key, _)| *key != 258)
                .collect::<Vec<_>>();
            expected[257].1 = 25700;
            expected.push((2000, 20000));
            assert_eq!(
                rows(&table).await,
                expected,
                "{producer}/{bucket}: no duplicate history keys"
            );
            assert_eq!(
                positions(&table, &anchor.file_name)
                    .await
                    .iter()
                    .collect::<Vec<_>>(),
                vec![257, 258]
            );
            assert!(all_files(&table).await.contains(&anchor));
            assert_eq!(
                rows(&table.copy_with_pinned_snapshot(Some(&before))).await,
                original
            );
            assert_eq!(
                indexes(&table)
                    .await
                    .into_iter()
                    .filter(|entry| entry.index_file.index_type == "HASH")
                    .collect::<Vec<_>>(),
                hashes
            );
            if producer == "lookup" {
                let mut expected = vec![
                    ("-U".into(), 257, 257),
                    ("+U".into(), 257, 25700),
                    ("-D".into(), 258, 258),
                    ("+I".into(), 2000, 20000),
                ];
                expected.sort();
                assert_eq!(
                    super::lookup_changelog_tests::events(&table, before.id(), after.id()).await,
                    expected
                );
            } else {
                assert_eq!(after.changelog_record_count(), None);
            }
        }
    }
}

#[tokio::test]
async fn dv_winning_delete_can_publish_empty_legacy_output_and_retry() {
    let table = dv_table(&[("changelog-producer", "lookup")]).await;
    let anchor = seed(&table).await;
    let before = latest(&table).await;
    write_batch(
        &table,
        &make_batch_with_kinds(vec![257], vec![999], vec![3]),
    )
    .await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let results = execute(&table, &plan).await;
    assert!(results[0].files().is_empty());
    let changelog = &results[0].changelog_files()[0];
    assert_eq!(
        (
            changelog.level,
            changelog.row_count,
            changelog.min_sequence_number,
            changelog.max_sequence_number
        ),
        (4, 1, 257, 257)
    );
    let mut legacy = serde_json::to_value(&results).unwrap();
    legacy[0]["task"]
        .as_object_mut()
        .unwrap()
        .remove("output_level");
    let commit = builder.new_commit().unwrap();
    let mut wrong_level = legacy.clone();
    wrong_level[0]["changelog_files"][0]["_LEVEL"] = 1.into();
    let wrong_level: Vec<CompactResult> = serde_json::from_value(wrong_level).unwrap();
    assert_eq!(wrong_level[0].changelog_files()[0].level, 1);
    let captured = latest(&table).await.id();
    assert!(commit.commit_compact(wrong_level).await.is_err());
    assert_eq!(latest(&table).await.id(), captured);
    commit
        .commit_compact(serde_json::from_value(legacy).unwrap())
        .await
        .unwrap();
    let after = latest(&table).await;
    commit.commit_compact(results).await.unwrap();
    assert_eq!(latest(&table).await.id(), after.id());
    assert_eq!(
        super::lookup_changelog_tests::events(&table, before.id(), after.id()).await,
        vec![("-D".into(), 257, 257)]
    );
    assert_eq!(
        positions(&table, &anchor.file_name)
            .await
            .iter()
            .collect::<Vec<_>>(),
        vec![257]
    );
    assert_eq!(
        rows(&table).await,
        (0..1000)
            .filter(|key| *key != 257)
            .map(|key| (key, key))
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn dv_filters_selected_rows_and_keeps_other_ranges_in_a_shared_container() {
    let table = dv_table(&[]).await;
    let anchor = seed(&table).await;
    write_batch(&table, &make_batch(vec![42, 43], vec![4200, 4300])).await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let selected = plan.tasks()[0].files()[0].file_name.clone();
    set_vectors(
        &table,
        &plan.tasks()[0],
        &[(&selected, &[0]), (&anchor.file_name, &[500])],
    )
    .await;
    let before = latest(&table).await;
    let old_rows = rows(&table).await;
    compact(&table).await;
    let expected = (0..1000)
        .filter(|key| *key != 500)
        .map(|key| (key, if key == 43 { 4300 } else { key }))
        .collect::<Vec<_>>();
    assert_eq!(rows(&table).await, expected);
    assert_eq!(
        positions(&table, &anchor.file_name)
            .await
            .iter()
            .collect::<Vec<_>>(),
        vec![43, 500]
    );
    assert!(positions(&table, &selected).await.is_empty());
    assert!(!all_files(&table)
        .await
        .iter()
        .any(|file| file.file_name == selected));
    assert_eq!(
        rows(&table.copy_with_pinned_snapshot(Some(&before))).await,
        old_rows
    );
}

#[tokio::test]
async fn dv_commit_accepts_concurrent_l0_but_rejects_changed_inputs_or_missing_targets() {
    for change in [
        "l0",
        "unrelated-target-dv",
        "target-dv",
        "input-dv",
        "missing-target",
    ] {
        let table = dv_table(&[]).await;
        let anchor = seed(&table).await;
        let anchor_name = anchor.file_name.clone();
        write_batch(&table, &make_batch(vec![42], vec![9000])).await;
        let builder = table.new_compact_builder();
        let plan = builder.new_planner().unwrap().plan().await.unwrap();
        let results = execute(&table, &plan).await;
        match change {
            "l0" => write_batch(&table, &make_batch(vec![42], vec![9001])).await,
            "unrelated-target-dv" | "target-dv" => {
                let position = if change == "target-dv" { 42 } else { 500 };
                set_vectors(&table, &plan.tasks()[0], &[(&anchor_name, &[position])]).await
            }
            "input-dv" => {
                set_vectors(
                    &table,
                    &plan.tasks()[0],
                    &[(&plan.tasks()[0].files()[0].file_name, &[0])],
                )
                .await
            }
            "missing-target" => {
                let mut removal = CommitMessage::new(
                    plan.tasks()[0].partition().to_vec(),
                    plan.tasks()[0].bucket(),
                    vec![],
                );
                removal.deleted_files.push(anchor);
                table
                    .new_write_builder()
                    .new_commit()
                    .commit(vec![removal])
                    .await
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let before = latest(&table).await.id();
        let commit = builder.new_commit().unwrap();
        let result = commit.commit_compact(results.clone()).await;
        if matches!(change, "l0" | "unrelated-target-dv") {
            result.unwrap();
            let published = latest(&table).await.id();
            commit.commit_compact(results).await.unwrap();
            assert_eq!(latest(&table).await.id(), published);
            assert_eq!(
                rows(&table).await.iter().find(|(key, _)| *key == 42),
                Some(&(42, 9000))
            );
            if change == "l0" {
                compact(&table).await;
                assert_eq!(
                    rows(&table).await.iter().find(|(key, _)| *key == 42),
                    Some(&(42, 9001))
                );
            } else {
                assert_eq!(
                    positions(&table, &anchor_name)
                        .await
                        .iter()
                        .collect::<Vec<_>>(),
                    vec![42, 500]
                );
                let visible = rows(&table).await;
                assert_eq!(visible.len(), 999);
                assert!(!visible.iter().any(|(key, _)| *key == 500));
            }
        } else {
            assert!(result.is_err(), "{change}");
            assert_eq!(latest(&table).await.id(), before);
        }
    }
}

#[tokio::test]
async fn dv_planning_checks_persisted_schema_even_when_its_id_matches_current_options() {
    let original = table(&[]).await;
    write_batch(&original, &make_batch(vec![1], vec![10])).await;
    let dv = original.copy_with_options(std::collections::HashMap::from([(
        "deletion-vectors.enabled".into(),
        "true".into(),
    )]));
    assert_eq!(dv.schema().id(), original.schema().id());
    let error = dv
        .new_compact_builder()
        .new_planner()
        .unwrap()
        .plan()
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("without deletion vectors"),
        "{error}"
    );
}

#[tokio::test]
async fn dv_repeated_updates_keep_one_bucket_container_and_old_snapshot_masks() {
    for bucket in ["1", "-1"] {
        let table = dv_table(&[("bucket", bucket)]).await;
        let anchor = seed(&table).await;
        // Keep this intermediate run larger than the next L0 file, so the
        // planner looks it up rather than selecting it for a rewrite.
        write_batch(
            &table,
            &make_batch((0..256).collect(), (4200..4456).collect()),
        )
        .await;
        let first = compact(&table).await;
        let intermediate = first[0].files()[0].clone();
        assert_eq!(intermediate.row_count, 256);
        let previous = latest(&table).await;
        let previous_rows = rows(&table).await;
        write_batch(&table, &make_batch(vec![42], vec![4201])).await;
        compact(&table).await;

        let vectors = indexes(&table)
            .await
            .into_iter()
            .filter(|entry| entry.index_file.index_type == "DELETION_VECTORS")
            .collect::<Vec<_>>();
        assert_eq!(
            vectors.len(),
            1,
            "Java combines DV entries by bucket: {bucket}"
        );
        assert_eq!(
            positions(&table, &anchor.file_name)
                .await
                .iter()
                .collect::<Vec<_>>(),
            (0..256).collect::<Vec<_>>()
        );
        assert_eq!(
            positions(&table, &intermediate.file_name)
                .await
                .iter()
                .collect::<Vec<_>>(),
            vec![42]
        );
        assert_eq!(
            rows(&table).await,
            (0..1000)
                .map(|key| {
                    (
                        key,
                        if key == 42 {
                            4201
                        } else if key < 256 {
                            key + 4200
                        } else {
                            key
                        },
                    )
                })
                .collect::<Vec<_>>()
        );
        assert_eq!(
            rows(&table.copy_with_pinned_snapshot(Some(&previous))).await,
            previous_rows
        );
    }
}
