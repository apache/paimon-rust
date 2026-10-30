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

use super::deletion_vector_tests::{indexes, positions, write_vectors};
use super::postpone_tests::execute;
use super::*;
use paimon::spec::{DataFileMeta, Snapshot};
use paimon::table::CommitMessage;
use paimon::table::PostponeCompactResult;

async fn dv_table(options: &[(&str, &str)]) -> Table {
    let mut configured = vec![
        ("bucket", "-2"),
        ("deletion-vectors.enabled", "true"),
        ("postpone.default-bucket-num", "1"),
    ];
    configured.extend_from_slice(options);
    table(&configured).await
}

async fn latest(table: &Table) -> Snapshot {
    table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap()
}

async fn compact(table: &Table) -> Vec<PostponeCompactResult> {
    let builder = table.new_postpone_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let results = execute(table, &plan).await;
    builder
        .new_commit()
        .unwrap()
        .commit_postpone_compact(results.clone())
        .await
        .unwrap();
    results
}

async fn files(table: &Table) -> Vec<(i32, DataFileMeta)> {
    table
        .new_read_builder()
        .new_scan()
        .with_scan_all_files()
        .plan()
        .await
        .unwrap()
        .splits()
        .iter()
        .flat_map(|split| {
            split
                .data_files()
                .iter()
                .cloned()
                .map(|file| (split.bucket(), file))
        })
        .collect()
}

async fn set_vectors(
    table: &Table,
    partition: &[u8],
    bucket: i32,
    masks: &[(&str, &[u32])],
    unknown_cardinality: bool,
) {
    let mut vector = write_vectors(table, bucket, masks).await;
    if unknown_cardinality {
        for meta in vector
            .deletion_vectors_ranges
            .as_mut()
            .unwrap()
            .values_mut()
        {
            meta.cardinality = None;
        }
    }
    let mut message = CommitMessage::new(partition.to_vec(), bucket, vec![]);
    message.new_index_files.push(vector);
    message.deleted_index_files = indexes(table)
        .await
        .into_iter()
        .filter(|entry| {
            entry.partition == partition
                && entry.bucket == bucket
                && entry.index_file.index_type == "DELETION_VECTORS"
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

#[tokio::test]
async fn postpone_dv_publishes_unique_live_rows_for_all_producers_and_formats() {
    for producer in ["none", "input", "lookup"] {
        for format in ["parquet", "orc", "avro", "row"] {
            let table = dv_table(&[
                ("changelog-producer", producer),
                ("file.format", format),
                ("file.compression", "none"),
                ("postpone.default-bucket-num", "2"),
                ("read.batch-size", "1"),
            ])
            .await;
            write_batch(&table, &make_batch(vec![5, 1, 2, 1], vec![50, 10, 20, 11])).await;
            compact(&table).await;
            let before = latest(&table).await;
            assert_eq!(rows(&table).await, vec![(1, 11), (2, 20), (5, 50)]);
            write_batch(
                &table,
                &make_batch_with_kinds(vec![2, 1, 3], vec![999, 12, 30], vec![3, 2, 0]),
            )
            .await;
            let result = compact(&table).await;
            assert!(result
                .iter()
                .flat_map(|result| result.files())
                .all(|file| file.delete_row_count == Some(0)));
            assert_eq!(
                rows(&table).await,
                vec![(1, 12), (3, 30), (5, 50)],
                "{producer}/{format}"
            );
            assert_eq!(
                rows(&table.copy_with_pinned_snapshot(Some(&before))).await,
                vec![(1, 11), (2, 20), (5, 50)]
            );
            if producer == "lookup" {
                assert_eq!(
                    lookup_changelog_tests::events(&table, before.id(), latest(&table).await.id())
                        .await,
                    vec![
                        ("+I".into(), 3, 30),
                        ("+U".into(), 1, 12),
                        ("-D".into(), 2, 20),
                        ("-U".into(), 1, 11)
                    ]
                );
            }
            let fixed = table.new_postpone_fixed_bucket_write_builder().unwrap();
            let mut writer = fixed.new_write().unwrap();
            writer
                .write_arrow_batch(&make_batch(vec![1, 4], vec![13, 40]))
                .await
                .unwrap();
            fixed
                .new_commit()
                .commit(writer.prepare_commit().await.unwrap())
                .await
                .unwrap();
            compact(&table).await;
            assert_eq!(rows(&table).await, vec![(1, 13), (3, 30), (4, 40), (5, 50)]);
        }
    }
}

#[tokio::test]
async fn postpone_dv_filters_pending_l0_and_history_and_retires_their_ranges() {
    for colocated in ["false", "true"] {
        let table = dv_table(&[
            ("index-file-in-data-file-dir", colocated),
            ("read.batch-size", "1"),
        ])
        .await;
        write_batch(&table, &make_batch(vec![1, 2, 3], vec![10, 20, 30])).await;
        compact(&table).await;
        let history = files(&table).await[0].1.file_name.clone();
        write_batch(&table, &make_batch(vec![1, 4], vec![11, 40])).await;
        let pending = files(&table)
            .await
            .into_iter()
            .find(|(bucket, _)| *bucket == -2)
            .unwrap()
            .1
            .file_name;
        let fixed = table.new_postpone_fixed_bucket_write_builder().unwrap();
        let mut writer = fixed.new_write().unwrap();
        writer
            .write_arrow_batch(&make_batch(vec![5, 6], vec![50, 60]))
            .await
            .unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        let l0 = messages[0].new_files[0].file_name.clone();
        fixed.new_commit().commit(messages).await.unwrap();
        let partition = paimon::spec::EMPTY_SERIALIZED_ROW.clone();
        set_vectors(&table, &partition, -2, &[(&pending, &[0])], true).await;
        set_vectors(
            &table,
            &partition,
            0,
            &[(&history, &[1]), (&l0, &[1])],
            false,
        )
        .await;
        let before = latest(&table).await;
        let old_rows = rows(&table).await;
        compact(&table).await;
        assert_eq!(rows(&table).await, vec![(1, 10), (3, 30), (4, 40), (5, 50)]);
        assert!(indexes(&table).await.is_empty());
        assert_eq!(
            rows(&table.copy_with_pinned_snapshot(Some(&before))).await,
            old_rows
        );
    }
}

#[tokio::test]
async fn postpone_dv_rejects_changed_selected_masks() {
    for selected_bucket in [-2, 0] {
        let table = dv_table(&[]).await;
        write_batch(&table, &make_batch(vec![1], vec![10])).await;
        if selected_bucket == 0 {
            compact(&table).await;
        }
        write_batch(&table, &make_batch(vec![2], vec![20])).await;
        let builder = table.new_postpone_compact_builder();
        let plan = builder.new_planner().unwrap().plan().await.unwrap();
        let result = execute(&table, &plan).await;
        let selected = files(&table)
            .await
            .into_iter()
            .find(|(bucket, _)| *bucket == selected_bucket)
            .unwrap()
            .1
            .file_name;
        set_vectors(
            &table,
            plan.tasks()[0].partition(),
            selected_bucket,
            &[(&selected, &[0])],
            false,
        )
        .await;
        let snapshot = latest(&table).await.id();
        let error = builder
            .new_commit()
            .unwrap()
            .commit_postpone_compact(result)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("deletion vectors changed"));
        assert_eq!(latest(&table).await.id(), snapshot);
        compact(&table).await;
        assert!(!files(&table)
            .await
            .iter()
            .any(|(_, file)| file.file_name == selected));
    }
}

#[tokio::test]
async fn postpone_dv_preserves_concurrent_pending_masks_and_validates_serialized_results() {
    let table = dv_table(&[]).await;
    write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
    let original = files(&table).await[0].1.file_name.clone();
    set_vectors(
        &table,
        &paimon::spec::EMPTY_SERIALIZED_ROW,
        -2,
        &[(&original, &[1])],
        false,
    )
    .await;
    let builder = table.new_postpone_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let result = execute(&table, &plan).await;
    let mut missing = serde_json::to_value(&result).unwrap();
    missing[0]
        .as_object_mut()
        .unwrap()
        .remove("deletion_vector_updates");
    let missing: Vec<PostponeCompactResult> = serde_json::from_value(missing).unwrap();
    let commit = builder.new_commit().unwrap();
    assert!(commit
        .commit_postpone_compact(missing.clone())
        .await
        .is_err());
    write_batch(&table, &make_batch(vec![99, 100], vec![990, 1000])).await;
    let pending = files(&table)
        .await
        .into_iter()
        .find(|(_, file)| file.file_name != original)
        .unwrap()
        .1
        .file_name;
    set_vectors(
        &table,
        plan.tasks()[0].partition(),
        -2,
        &[(&original, &[1]), (&pending, &[0])],
        false,
    )
    .await;
    let metrics = commit
        .commit_postpone_compact(result.clone())
        .await
        .unwrap();
    assert_eq!(
        commit.commit_postpone_compact(missing).await.unwrap(),
        metrics
    );
    assert_eq!(rows(&table).await, vec![(1, 10)]);
    assert_eq!(
        positions(&table, &pending).await.iter().collect::<Vec<_>>(),
        vec![0]
    );
    compact(&table).await;
    assert_eq!(rows(&table).await, vec![(1, 10), (100, 1000)]);
    assert!(indexes(&table).await.is_empty());
}

#[tokio::test]
async fn postpone_dv_can_publish_empty_data_after_delete_or_mask() {
    for mask in [false, true] {
        let table = dv_table(&[("changelog-producer", "lookup")]).await;
        if !mask {
            write_batch(&table, &make_batch(vec![1], vec![10])).await;
            compact(&table).await;
        }
        let start = table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap()
            .unwrap_or(0);
        write_batch(
            &table,
            &make_batch_with_kinds(vec![1], vec![10], vec![if mask { 0 } else { 3 }]),
        )
        .await;
        let builder = table.new_postpone_compact_builder();
        if mask {
            let plan = builder.new_planner().unwrap().plan().await.unwrap();
            set_vectors(
                &table,
                plan.tasks()[0].partition(),
                -2,
                &[(&plan.tasks()[0].inputs()[0].file().file_name, &[0])],
                false,
            )
            .await;
        }
        let result = compact(&table).await;
        assert!(result[0].files().next().is_none());
        assert!(rows(&table).await.is_empty());
        assert!(files(&table).await.is_empty());
        assert!(indexes(&table).await.is_empty());
        if !mask {
            assert_eq!(
                lookup_changelog_tests::events(&table, start, latest(&table).await.id()).await,
                vec![("-D".into(), 1, 10)]
            );
        }
    }
}

#[tokio::test]
async fn postpone_dv_filters_row_positions_in_both_pending_layouts() {
    for java_layout in [false, true] {
        let table = dv_table(&[
            ("file.format", "row"),
            ("file.compression", "none"),
            ("read.batch-size", "1"),
        ])
        .await;
        if java_layout {
            let fixed_table = table.copy_with_options(std::collections::HashMap::from([(
                "bucket".into(),
                "1".into(),
            )]));
            let builder = fixed_table.new_write_builder();
            let mut writer = builder.new_write().unwrap();
            writer
                .write_arrow_batch(&make_batch(vec![2, 1], vec![20, 10]))
                .await
                .unwrap();
            let mut messages = writer.prepare_commit().await.unwrap();
            for message in &mut messages {
                for file in &mut message.new_files {
                    file.external_path =
                        Some(format!("{}/bucket-0/{}", table.location(), file.file_name));
                    file.file_name = format!("data--u-java-s-0-w-{}", file.file_name);
                }
                message.bucket = -2;
                message.total_buckets = None;
            }
            table
                .new_write_builder()
                .new_commit()
                .commit(messages)
                .await
                .unwrap();
        } else {
            write_batch(&table, &make_batch(vec![2, 1], vec![20, 10])).await;
        }
        let pending = files(&table).await[0].1.file_name.clone();
        set_vectors(
            &table,
            &paimon::spec::EMPTY_SERIALIZED_ROW,
            -2,
            &[(&pending, &[0])],
            true,
        )
        .await;
        compact(&table).await;
        assert_eq!(
            rows(&table).await,
            if java_layout {
                vec![(2, 20)]
            } else {
                vec![(1, 10)]
            }
        );
    }
}

#[tokio::test]
async fn postpone_dv_cannot_enable_dv_only_through_temporary_options() {
    let original = table(&[("bucket", "-2")]).await;
    write_batch(&original, &make_batch(vec![1], vec![10])).await;
    let dv = original.copy_with_options(std::collections::HashMap::from([(
        "deletion-vectors.enabled".into(),
        "true".into(),
    )]));
    let error = dv
        .new_postpone_compact_builder()
        .new_planner()
        .unwrap()
        .plan()
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("cannot migrate files written without deletion vectors"));
    assert_eq!(latest(&original).await.id(), 1);
}

#[tokio::test]
async fn postpone_dv_partition_subset_keeps_unselected_partition_vectors() {
    let schema = Schema::builder()
        .column("pt", DataType::VarChar(VarCharType::string_type()))
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .primary_key(["pt", "id"])
        .partition_keys(["pt"])
        .option("bucket", "-2")
        .option("postpone.default-bucket-num", "1")
        .option("deletion-vectors.enabled", "true")
        .build()
        .unwrap();
    let table = table_with_schema(TableSchema::new(0, &schema)).await;
    write_batch(
        &table,
        &make_partitioned_batch(vec!["a", "a", "b"], vec![1, 2, 3], vec![10, 20, 30]),
    )
    .await;
    compact(&table).await;
    let state = table.new_read_builder().new_scan().plan().await.unwrap();
    let untouched = state
        .splits()
        .iter()
        .find(|split| split.partition().get_string(0).unwrap() == "b")
        .unwrap();
    let untouched_file = untouched.data_files()[0].file_name.clone();
    write_batch(
        &table,
        &make_partitioned_batch(vec!["a", "b"], vec![1, 3], vec![11, 31]),
    )
    .await;
    let filter = PredicateBuilder::new(table.schema().fields())
        .equal("pt", Datum::String("a".into()))
        .unwrap();
    let builder = table
        .new_postpone_compact_builder()
        .with_partition_filter(filter)
        .unwrap();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    assert_eq!(plan.tasks().len(), 1);
    let result = execute(&table, &plan).await;
    set_vectors(
        &table,
        &untouched.partition().to_serialized_bytes(),
        0,
        &[(&untouched_file, &[0])],
        false,
    )
    .await;
    builder
        .new_commit()
        .unwrap()
        .commit_postpone_compact(result)
        .await
        .unwrap();
    assert_eq!(
        positions(&table, &untouched_file)
            .await
            .iter()
            .collect::<Vec<_>>(),
        vec![0]
    );
    assert_eq!(rows(&table).await, vec![(1, 11), (2, 20)]);
    compact(&table).await;
    assert_eq!(rows(&table).await, vec![(1, 11), (2, 20), (3, 31)]);
    assert!(indexes(&table).await.is_empty());
}
