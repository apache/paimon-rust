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
use paimon::spec::{DeletionVectorMeta, IndexFileMeta, IndexManifest};
use paimon::table::CommitMessage;

async fn dv_table(options: &[(&str, &str)]) -> Table {
    let mut configured = vec![("deletion-vectors.enabled", "true")];
    configured.extend_from_slice(options);
    table(&configured).await
}

async fn compact(table: &Table) -> paimon::table::CompactMetrics {
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    builder
        .new_commit()
        .unwrap()
        .commit_compact(execute(table, &plan).await)
        .await
        .unwrap()
}

async fn indexes(table: &Table) -> Vec<paimon::spec::IndexManifestEntry> {
    let snapshot = table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    match snapshot.index_manifest() {
        Some(name) => IndexManifest::read(
            table.file_io(),
            &table.snapshot_manager().manifest_path(name),
        )
        .await
        .unwrap(),
        None => vec![],
    }
}

// Write the same v1 index envelope as Java's BitmapDeletionVector. The selected
// file can be L0 or already compacted; positions are local physical row offsets.
pub(super) async fn delete_positions(table: &Table, file_name: &str, positions: &[u32]) {
    let plan = table
        .new_read_builder()
        .new_scan()
        .with_scan_all_files()
        .plan()
        .await
        .unwrap();
    let split = plan
        .splits()
        .iter()
        .find(|split| {
            split
                .data_files()
                .iter()
                .any(|file| file.file_name == file_name)
        })
        .unwrap();
    let mut message = CommitMessage::new(
        split.partition().to_serialized_bytes(),
        split.bucket(),
        vec![],
    );
    message
        .new_index_files
        .push(write_vector_index(table, split, &[(file_name, positions)]).await);
    for entry in indexes(table).await {
        if entry.partition == message.partition
            && entry.bucket == message.bucket
            && entry
                .index_file
                .deletion_vectors_ranges
                .as_ref()
                .is_some_and(|ranges| ranges.contains_key(file_name))
        {
            message.deleted_index_files.push(entry.index_file);
        }
    }
    table
        .new_write_builder()
        .new_commit()
        .commit(vec![message])
        .await
        .unwrap();
}

async fn write_vector_index(
    table: &Table,
    split: &paimon::table::DataSplit,
    deletions: &[(&str, &[u32])],
) -> IndexFileMeta {
    let mut bytes = vec![1];
    let mut ranges = IndexMap::new();
    for (file_name, positions) in deletions {
        let bitmap: roaring::RoaringBitmap = positions.iter().copied().collect();
        let mut payload = 1581511376u32.to_be_bytes().to_vec();
        bitmap.serialize_into(&mut payload).unwrap();
        let meta = DeletionVectorMeta {
            offset: bytes.len() as i32,
            length: payload.len() as i32,
            cardinality: Some(positions.len() as i64),
        };
        bytes.extend_from_slice(&meta.length.to_be_bytes());
        bytes.extend_from_slice(&payload);
        bytes.extend_from_slice(&crc32fast::hash(&payload).to_be_bytes());
        ranges.insert(file_name.to_string(), meta);
    }
    let name = format!("index-{}-1", uuid::Uuid::new_v4());
    let path = if table.schema().core_options().index_file_in_data_file_dir() {
        format!("{}/{}", split.bucket_path(), name)
    } else {
        format!("{}/index/{}", table.location(), name)
    };
    table
        .file_io()
        .mkdirs(path.rsplit_once('/').unwrap().0)
        .await
        .unwrap();
    let size = bytes.len() as i64;
    table
        .file_io()
        .new_output(&path)
        .unwrap()
        .write(Bytes::from(bytes))
        .await
        .unwrap();
    IndexFileMeta {
        index_type: "DELETION_VECTORS".into(),
        file_name: name,
        file_size: size,
        row_count: ranges.len() as i64,
        deletion_vectors_ranges: Some(ranges),
        external_path: None,
        global_index_meta: None,
    }
}

#[tokio::test]
async fn dv_compact_materializes_all_engines_and_continues_merging() {
    for engine in ["deduplicate", "partial-update", "aggregation"] {
        for bucket in ["1", "-1"] {
            let mut options = vec![
                ("merge-engine", engine),
                ("bucket", bucket),
                ("read.batch-size", "1"),
                ("target-file-row-num", "1"),
            ];
            if engine == "aggregation" {
                options.push(("fields.value.aggregate-function", "sum"));
            }
            let table = dv_table(&options).await;
            write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
            write_batch(&table, &make_batch(vec![1, 3], vec![5, 30])).await;
            assert!(rows(&table).await.is_empty());
            let metrics = compact(&table).await;
            assert_eq!(metrics.rows_written, 3, "{engine}/{bucket}");
            let value = match engine {
                "aggregation" => 15,
                _ => 5,
            };
            assert_eq!(rows(&table).await, vec![(1, value), (2, 20), (3, 30)]);
            assert!(table
                .new_compact_builder()
                .new_planner()
                .unwrap()
                .plan()
                .await
                .unwrap()
                .tasks()
                .is_empty());
            write_batch(&table, &make_batch(vec![1], vec![7])).await;
            compact(&table).await;
            let value = match engine {
                "aggregation" => 22,
                _ => 7,
            };
            assert_eq!(rows(&table).await, vec![(1, value), (2, 20), (3, 30)]);
        }
    }
}

#[tokio::test]
async fn dv_compact_drops_retracts_and_allows_an_empty_bucket() {
    let table = dv_table(&[("changelog-producer", "input")]).await;
    write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
    write_batch(
        &table,
        &make_batch_with_kinds(vec![1, 2, 3], vec![10, 20, 30], vec![3, 1, 3]),
    )
    .await;
    let before = table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    let metrics = compact(&table).await;
    assert_eq!(metrics.rows_written, 0);
    assert_eq!(metrics.files_added, 0);
    assert!(rows(&table).await.is_empty());
    let after = table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.commit_kind(), &CommitKind::COMPACT);
    assert_eq!(before.changelog_record_count(), Some(3));
    assert_eq!(after.changelog_record_count(), None);
}

#[tokio::test]
async fn dv_compact_filters_physical_positions_before_aggregation_and_preserves_history() {
    for in_bucket in ["true", "false"] {
        let table = dv_table(&[
            ("merge-engine", "aggregation"),
            ("fields.value.aggregate-function", "sum"),
            ("index-file-in-data-file-dir", in_bucket),
            ("data-file.path-directory", "custom"),
        ])
        .await;
        write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
        let plan = table
            .new_read_builder()
            .new_scan()
            .with_scan_all_files()
            .plan()
            .await
            .unwrap();
        let old_name = plan.splits()[0].data_files()[0].file_name.clone();
        delete_positions(&table, &old_name, &[0]).await;
        write_batch(&table, &make_batch(vec![1], vec![15])).await;
        let before = table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap();
        compact(&table).await;
        assert_eq!(rows(&table).await, vec![(1, 15), (2, 20)]);
        assert!(indexes(&table).await.is_empty());
        let history = table
            .copy_with_pinned_snapshot(Some(&before))
            .copy_with_options(std::collections::HashMap::from([(
                "deletion-vectors.merge-on-read".into(),
                "true".into(),
            )]));
        assert_eq!(rows(&history).await, vec![(1, 15), (2, 20)]);
    }
}

#[tokio::test]
async fn dv_compact_reclaims_a_positive_level_run_when_selected_by_incremental_compaction() {
    let table = dv_table(&[]).await;
    write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
    compact(&table).await;
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    delete_positions(&table, &plan.splits()[0].data_files()[0].file_name, &[0, 1]).await;
    let unchanged = indexes(&table).await;
    let plan = table
        .new_compact_builder()
        .new_planner()
        .unwrap()
        .plan()
        .await
        .unwrap();
    assert!(
        plan.tasks().is_empty(),
        "no full-compaction path for one positive-level run"
    );
    assert_eq!(indexes(&table).await, unchanged);
    write_batch(&table, &make_batch(vec![3], vec![30])).await;
    let metrics = compact(&table).await;
    assert_eq!(metrics.rows_written, 1);
    assert!(indexes(&table).await.is_empty());
    assert_eq!(rows(&table).await, vec![(3, 30)]);
}

#[tokio::test]
async fn dv_compact_rejects_concurrent_deletions_on_inputs_but_accepts_l0_appends() {
    let table = dv_table(&[]).await;
    write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let results = execute(&table, &plan).await;
    delete_positions(&table, &plan.tasks()[0].files()[0].file_name, &[0]).await;
    let error = builder
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("deletion vector"), "{error}");
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let results = execute(&table, &plan).await;
    write_batch(&table, &make_batch(vec![1], vec![99])).await;
    let committer = builder.new_commit().unwrap();
    let metrics = committer.commit_compact(results.clone()).await.unwrap();
    assert_eq!(metrics.rows_written, 1);
    assert_eq!(rows(&table).await, vec![(2, 20)]);
    assert_eq!(committer.commit_compact(results).await.unwrap(), metrics);
    compact(&table).await;
    assert_eq!(rows(&table).await, vec![(1, 99), (2, 20)]);
}

#[tokio::test]
async fn dv_compact_obeys_user_sequence_and_output_format() {
    for format in ["parquet", "orc", "avro", "row"] {
        let table = dv_table(&[
            ("file.format", format),
            ("file.compression", "none"),
            ("sequence.field", "value"),
            ("read.batch-size", "1"),
            ("write-buffer-size", "1b"),
        ])
        .await;
        write_batch(&table, &make_batch(vec![1, 2, 3], vec![100, 20, 30])).await;
        write_batch(
            &table,
            &make_batch_with_kinds(vec![1, 2, 3], vec![10, 21, 29], vec![0, 3, 3]),
        )
        .await;
        compact(&table).await;
        assert_eq!(rows(&table).await, vec![(1, 100), (3, 30)], "{format}");
        let plan = table.new_read_builder().new_scan().plan().await.unwrap();
        assert!(plan
            .splits()
            .iter()
            .flat_map(|split| split.data_files())
            .all(|file| file.level > 0 && file.delete_row_count == Some(0)));
    }
}

#[tokio::test]
async fn dv_compact_subsets_keep_other_vectors_and_latest_dynamic_hash_assignments() {
    let table = dv_table(&[("bucket", "-1"), ("dynamic-bucket.target-row-num", "1")]).await;
    write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
    let builder = table.new_compact_builder();
    let initial = builder.new_planner().unwrap().plan().await.unwrap();
    assert_eq!(initial.tasks().len(), 2);
    for task in initial.tasks() {
        delete_positions(&table, &task.files()[0].file_name, &[0]).await;
    }
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let results = execute(&table, &plan).await;
    let other_bucket = plan.tasks()[1].bucket();
    let other_dv = indexes(&table)
        .await
        .into_iter()
        .find(|entry| {
            entry.bucket == other_bucket && entry.index_file.index_type == "DELETION_VECTORS"
        })
        .unwrap();
    write_batch(&table, &make_batch(vec![3], vec![30])).await;
    let expected_hashes = indexes(&table)
        .await
        .into_iter()
        .filter(|entry| entry.index_file.index_type == "HASH")
        .collect::<Vec<_>>();
    let committer = builder.new_commit().unwrap();
    committer
        .commit_compact(vec![results[0].clone()])
        .await
        .unwrap();
    let remaining = indexes(&table).await;
    assert_eq!(
        remaining
            .iter()
            .filter(|entry| entry.index_file.index_type == "HASH")
            .cloned()
            .collect::<Vec<_>>(),
        expected_hashes
    );
    assert_eq!(
        remaining
            .iter()
            .filter(|entry| entry.index_file.index_type == "DELETION_VECTORS")
            .collect::<Vec<_>>(),
        vec![&other_dv]
    );
    committer
        .commit_compact(vec![results[1].clone()])
        .await
        .unwrap();
    assert!(rows(&table).await.is_empty());
    compact(&table).await;
    assert_eq!(rows(&table).await, vec![(3, 30)]);
}

#[tokio::test]
async fn dv_compact_keeps_concurrent_file_ranges_in_a_shared_immutable_index() {
    let table = dv_table(&[]).await;
    write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
    let scan = table
        .new_read_builder()
        .new_scan()
        .with_scan_all_files()
        .plan()
        .await
        .unwrap();
    let split = &scan.splits()[0];
    let old_file = &split.data_files()[0].file_name;
    let write = table.new_write_builder();
    let mut writer = write.new_write().unwrap();
    writer
        .write_arrow_batch(&make_batch(vec![3], vec![30]))
        .await
        .unwrap();
    let pending = writer.prepare_commit().await.unwrap();
    let new_file = pending[0].new_files[0].file_name.clone();
    let shared = write_vector_index(&table, split, &[(old_file, &[0]), (&new_file, &[0])]).await;
    let mut original = shared.clone();
    original
        .deletion_vectors_ranges
        .as_mut()
        .unwrap()
        .shift_remove(&new_file);
    original.row_count = 1;
    let mut deletion = CommitMessage::new(
        split.partition().to_serialized_bytes(),
        split.bucket(),
        vec![],
    );
    deletion.new_index_files.push(original.clone());
    write
        .new_commit()
        .commit(vec![deletion.clone()])
        .await
        .unwrap();
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let results = execute(&table, &plan).await;
    // Add a range without changing the captured file's vector or its immutable
    // bytes. The appended file's vector must survive the planned replacement.
    write.new_commit().commit(pending).await.unwrap();
    writer.close().await;
    deletion.new_index_files = vec![shared.clone()];
    deletion.deleted_index_files = vec![original];
    write.new_commit().commit(vec![deletion]).await.unwrap();
    builder
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap();
    let remaining = indexes(&table).await;
    assert_eq!(remaining.len(), 1);
    let index = &remaining[0].index_file;
    assert_eq!(index.row_count, 1);
    let ranges = index.deletion_vectors_ranges.as_ref().unwrap();
    assert_eq!(ranges.len(), 1);
    assert_eq!(ranges[&new_file].cardinality, Some(1));
    let visible = table.copy_with_options(std::collections::HashMap::from([(
        "deletion-vectors.merge-on-read".into(),
        "true".into(),
    )]));
    assert_eq!(rows(&visible).await, vec![(2, 20)]);
    assert_eq!(rows(&table).await, vec![(2, 20)]);
    compact(&table).await;
    assert_eq!(rows(&table).await, vec![(2, 20)]);
    assert!(indexes(&table).await.is_empty());
}

#[tokio::test]
async fn dv_compact_rejects_serialized_outputs_with_unknown_or_nonzero_delete_counts() {
    let table = dv_table(&[]).await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let results = execute(&table, &plan).await;
    for count in [serde_json::Value::Null, serde_json::json!(1)] {
        let mut invalid = serde_json::to_value(&results).unwrap();
        *invalid[0]["files"][0].get_mut("_DELETE_ROW_COUNT").unwrap() = count;
        let invalid = serde_json::from_value(invalid).unwrap();
        assert!(builder
            .new_commit()
            .unwrap()
            .commit_compact(invalid)
            .await
            .unwrap_err()
            .to_string()
            .contains("output file"));
    }
    builder
        .new_commit()
        .unwrap()
        .commit_compact(results)
        .await
        .unwrap();
    assert_eq!(rows(&table).await, vec![(1, 10)]);
}
