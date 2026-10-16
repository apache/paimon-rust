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
use crate::table::vector_search_test_utils::{
    de_vector_table, id_gt_filter, pk_vector_table, vector_test_table_at,
};
use crate::vector_search::ScoredRowIds;
use std::collections::HashMap;

fn selected_split() -> DataSplit {
    DataSplit::deserialize_split_v1(include_bytes!("../goldens/split_v1_data.bin"))
        .unwrap()
        .with_selected_row_ranges(vec![RowRange::new(1, 1)])
        .with_scores(vec![0.75])
        .unwrap()
}

#[tokio::test]
async fn pk_supplied_result_preserves_snapshot_splits_and_ignores_planning_hints() {
    // There are no snapshots or manifests to resolve. These belong to the
    // captured result, exactly as Java PrimaryKeyBatchScan.preProcessPlan.
    let table = pk_vector_table(&[("pk-vector.index.columns", "embedding")]);
    let split = selected_split();
    let mut builder = table.new_read_builder();
    builder.with_limit(0).with_filter(id_gt_filter(&table, 100));
    let scan = builder
        .new_scan()
        .with_global_index_result(GlobalIndexResult::from_splits(
            Some(42),
            vec![split.clone()],
        ))
        .unwrap()
        .with_shard(1, 2)
        .unwrap();
    let plan = scan.plan().await.unwrap();
    let stats_plan = scan
        .clone()
        .with_stats(crate::table::ScanStatsMode::Lightweight)
        .unwrap()
        .plan()
        .await
        .unwrap();
    let scan_stats = stats_plan.scan_stats().cloned().unwrap();
    for plan in [plan, stats_plan] {
        assert_eq!(plan.snapshot_id(), Some(42));
        assert_eq!(plan.splits(), std::slice::from_ref(&split));
        assert_eq!(
            plan.splits()[0].serialize_split_v1().unwrap(),
            split.serialize_split_v1().unwrap()
        );
    }
    assert_eq!(scan_stats.snapshot_id, Some(42));
    assert_eq!(scan_stats.manifest_entries_read, 0);
    assert_eq!(scan_stats.final_splits, 1);
    assert_eq!(scan_stats.final_files, split.data_files().len());
    assert_eq!(
        scan_stats.planned_data_file_bytes,
        split
            .data_files()
            .iter()
            .map(|f| f.file_size as u64)
            .sum::<u64>()
    );
}

#[tokio::test]
async fn empty_pk_result_retains_its_snapshot_and_does_not_scan_latest() {
    let table = pk_vector_table(&[("pk-vector.index.columns", "embedding")]);
    for snapshot_id in [None, Some(7)] {
        let scan = table
            .new_read_builder()
            .new_scan()
            .with_global_index_result(GlobalIndexResult::from_splits(snapshot_id, vec![]))
            .unwrap();
        let plan = scan.plan().await.unwrap();
        assert!(plan.splits().is_empty());
        assert_eq!(plan.snapshot_id(), snapshot_id);
    }
}

#[tokio::test]
async fn result_address_spaces_and_conflicting_selections_are_validated() {
    let pk = pk_vector_table(&[("pk-vector.index.columns", "embedding")]);
    let de = de_vector_table().await;
    let append = vector_test_table_at("memory:/append_index_result");
    assert!(pk
        .new_read_builder()
        .new_scan()
        .with_global_index_result(GlobalIndexResult::from_row_ranges(vec![]))
        .is_err());
    for table in [&de, &append] {
        assert!(table
            .new_read_builder()
            .new_scan()
            .with_global_index_result(GlobalIndexResult::from_splits(None, vec![]))
            .is_err());
    }
    assert!(append
        .new_read_builder()
        .new_scan()
        .with_global_index_result(GlobalIndexResult::from_row_ranges(vec![]))
        .is_err());
    let mut builder = de.new_read_builder();
    builder.with_row_ranges(vec![]);
    assert!(builder
        .new_scan()
        .with_global_index_result(GlobalIndexResult::from_row_ranges(vec![]))
        .is_err());
    let scan = de
        .new_read_builder()
        .new_scan()
        .with_global_index_result(GlobalIndexResult::from_row_ranges(vec![]))
        .unwrap();
    assert!(scan.clone().with_row_ranges(vec![]).plan().await.is_err());
    assert!(scan.clone().with_scan_all_files().plan().await.is_err());
    assert!(scan.with_chunk_shuffle(1, 1).is_err());
    assert!(de
        .new_read_builder()
        .new_scan()
        .with_chunk_shuffle(1, 1)
        .unwrap()
        .with_global_index_result(GlobalIndexResult::from_row_ranges(vec![]))
        .is_err());
}

#[tokio::test]
async fn de_scores_follow_final_ranges_after_distribution_and_limit() {
    let table = de_vector_table().await;
    let hits =
        GlobalIndexResult::from_scored_row_ids(ScoredRowIds::new(vec![2, 0], vec![0.8, 0.1]));
    let mut builder = table.new_read_builder();
    builder.with_projection(&["id"]).unwrap();
    let scan = builder
        .new_scan()
        .with_global_index_result(hits.clone())
        .unwrap();
    let plan = scan.plan().await.unwrap();
    let statistics_plan = scan
        .clone()
        .with_stats(crate::table::ScanStatsMode::Complete)
        .unwrap()
        .plan()
        .await
        .unwrap();
    assert_eq!(statistics_plan.splits(), plan.splits());
    let scan_stats = statistics_plan.scan_stats().unwrap();
    assert!(scan_stats.complete);
    assert_eq!(scan_stats.snapshot_id, plan.snapshot_id());
    assert_eq!(
        scan_stats.planned_data_file_bytes,
        plan.planned_data_file_bytes()
    );
    let ranges = plan
        .splits()
        .iter()
        .flat_map(|s| s.row_ranges().unwrap())
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(ranges, vec![RowRange::new(0, 0), RowRange::new(2, 2)]);
    let scores = plan
        .splits()
        .iter()
        .flat_map(|s| s.scores().unwrap())
        .copied()
        .collect::<Vec<_>>();
    assert_eq!(scores, vec![0.1, 0.8]);
    let selected = builder
        .new_scan()
        .with_global_index_result(hits)
        .unwrap()
        .with_row_position_slice(1, 3)
        .unwrap()
        .plan()
        .await
        .unwrap();
    assert_eq!(selected.snapshot_id(), plan.snapshot_id());
    assert_eq!(selected.splits().len(), 1);
    assert_eq!(
        selected.splits()[0].row_ranges(),
        Some([RowRange::new(2, 2)].as_slice())
    );
    assert_eq!(selected.splits()[0].scores(), Some([0.8].as_slice()));
    let wire = selected.splits()[0].serialize_split_v1().unwrap();
    assert_eq!(
        DataSplit::deserialize_split_v1(&wire).unwrap(),
        selected.splits()[0]
    );
    builder.with_limit(1);
    let plan = builder
        .new_scan()
        .with_global_index_result(GlobalIndexResult::from_scored_row_ids(ScoredRowIds::new(
            vec![2, 0],
            vec![0.8, 0.1],
        )))
        .unwrap()
        .plan()
        .await
        .unwrap();
    // LIMIT is a conservative split hint; every retained position keeps its score.
    assert!(plan
        .splits()
        .iter()
        .all(|s| s.scores().unwrap().len() == s.row_count() as usize));
}

#[tokio::test]
async fn de_search_results_can_be_reused_by_an_ordinary_scan() {
    let table = de_vector_table().await;
    let result = table
        .new_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vector(vec![1.0, 0.0])
        .with_limit(2)
        .execute()
        .await
        .unwrap();
    let expected = result
        .row_ids()
        .unwrap()
        .row_ids
        .iter()
        .copied()
        .zip(result.row_ids().unwrap().scores.iter().copied())
        .collect::<HashMap<_, _>>();
    let plan = result
        .table()
        .unwrap()
        .new_read_builder()
        .new_scan()
        .with_global_index_result(result.clone())
        .unwrap()
        .plan()
        .await
        .unwrap();
    let mut actual = HashMap::new();
    for split in plan.splits() {
        let ids = split
            .row_ranges()
            .unwrap()
            .iter()
            .flat_map(|r| r.from()..=r.to());
        actual.extend(
            ids.zip(split.scores().unwrap().iter().copied())
                .map(|(id, score)| (id as u64, score)),
        );
    }
    assert_eq!(plan.snapshot_id(), result.snapshot_id());
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn invalid_scored_ids_do_not_become_a_full_scan() {
    let table = de_vector_table().await;
    for hits in [
        ScoredRowIds {
            row_ids: vec![0],
            scores: vec![],
        },
        ScoredRowIds::new(vec![0, 0], vec![1.0, 2.0]),
        ScoredRowIds::new(vec![u64::MAX], vec![1.0]),
    ] {
        assert!(table
            .new_read_builder()
            .new_scan()
            .with_global_index_result(GlobalIndexResult::from_scored_row_ids(hits))
            .unwrap()
            .plan()
            .await
            .is_err());
    }
}

#[tokio::test]
async fn supplied_pk_result_cannot_bypass_required_authorization() {
    let table = pk_vector_table(&[("pk-vector.index.columns", "embedding")]).copy_with_options(
        HashMap::from([("query-auth.enabled".into(), "true".into())]),
    );
    let scan = table
        .new_read_builder()
        .new_scan()
        .with_global_index_result(GlobalIndexResult::from_splits(
            Some(42),
            vec![selected_split()],
        ))
        .unwrap();
    assert!(scan.plan().await.is_err());
    assert!(scan
        .clone()
        .with_stats(crate::table::ScanStatsMode::Lightweight)
        .unwrap()
        .plan()
        .await
        .is_err());
}

#[tokio::test]
async fn de_combined_incremental_results_retain_the_window_and_scores() {
    use crate::table::{IncrementalScanMode, TableCommit};
    use arrow_array::{types::Float32Type, Int32Array, ListArray, RecordBatch};
    use std::sync::Arc;

    let table = de_vector_table().await;
    let start = table
        .snapshot_manager()
        .get_latest_snapshot_id()
        .await
        .unwrap()
        .unwrap();
    let builder = table.new_write_builder().with_commit_user("next").unwrap();
    let mut writer = builder.new_write().unwrap();
    let batch = RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int32Array::from(vec![4])) as arrow_array::ArrayRef,
        ),
        (
            "embedding",
            Arc::new(ListArray::from_iter_primitive::<Float32Type, _, _>([Some(
                vec![Some(1.0), Some(0.0)],
            )])) as arrow_array::ArrayRef,
        ),
    ])
    .unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    TableCommit::new(table.clone(), "next".into())
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();
    let end = table
        .snapshot_manager()
        .get_latest_snapshot_id()
        .await
        .unwrap()
        .unwrap();
    let scan = table
        .new_read_builder()
        .new_incremental_scan(IncrementalScanMode::Delta, start, end)
        .with_global_index_result(GlobalIndexResult::from_scored_row_ids(ScoredRowIds::new(
            vec![0, 3],
            vec![0.1, 0.9],
        )))
        .unwrap();
    assert!(scan.plan().await.is_err());
    let plan = scan.plan_combined().await.unwrap();
    let statistics_plan = scan
        .with_stats(crate::table::ScanStatsMode::Complete)
        .unwrap()
        .plan_combined()
        .await
        .unwrap();
    assert_eq!(statistics_plan.splits(), plan.splits());
    let scan_stats = statistics_plan.scan_stats().unwrap();
    assert!(scan_stats.complete);
    assert_eq!(scan_stats.snapshot_id, Some(end));
    assert_eq!(
        scan_stats.planned_data_file_bytes,
        plan.planned_data_file_bytes()
    );
    assert_eq!(plan.snapshot_id(), Some(end));
    assert_eq!(plan.splits().len(), 1);
    assert_eq!(
        plan.splits()[0].row_ranges(),
        Some([RowRange::new(3, 3)].as_slice())
    );
    assert_eq!(plan.splits()[0].scores(), Some([0.9].as_slice()));
    assert!(table
        .new_read_builder()
        .new_incremental_scan(IncrementalScanMode::Delta, start, end)
        .with_global_index_result(GlobalIndexResult::from_splits(Some(end), vec![]))
        .is_err());
}

#[tokio::test]
async fn external_selections_need_a_table_scan_for_reads() {
    let ranges = GlobalIndexResult::from_row_ranges(vec![RowRange::new(0, i64::MAX)]);
    assert_eq!(
        ranges.len(),
        usize::try_from(i128::from(i64::MAX) + 1).unwrap_or(usize::MAX)
    );
    let split = selected_split();
    let physical = GlobalIndexResult::from_splits(Some(42), vec![split.clone()]);
    assert_eq!(physical.len(), 1);
    assert_eq!(
        physical.serialize_primary_key_splits().unwrap(),
        vec![split.serialize_split_v1().unwrap()]
    );
    for result in [
        ranges,
        physical,
        GlobalIndexResult::from_scored_row_ids(ScoredRowIds::new(vec![1], vec![0.5])),
    ] {
        let error = match result.new_read_builder().read().await {
            Ok(_) => panic!("external selection unexpectedly retained a source table"),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("require a result returned by a table search"));
    }
}
