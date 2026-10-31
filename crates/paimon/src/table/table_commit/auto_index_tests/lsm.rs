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
use crate::spec::{Datum, PredicateBuilder};
use crate::table::global_index_scanner::GlobalIndexScanner;
use crate::table::RowRange;

mod native;

async fn scalar_hits(table: &Table, files: &[IndexManifestEntry], id: i32) -> Vec<RowRange> {
    let scanner = GlobalIndexScanner::create(
        table.file_io(),
        table.location(),
        1,
        i64::MAX,
        i64::MAX,
        files,
        table.schema().fields(),
    )
    .unwrap()
    .unwrap();
    scanner
        .matching_ranges(
            &PredicateBuilder::new(table.schema().fields())
                .equal("id", Datum::Int(id))
                .unwrap(),
        )
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn small_appends_keep_leaf_indexes_until_the_default_merge_trigger() {
    let table = table(Some(true), false)
        .await
        .copy_with_options(HashMap::from([(
            "sorted-index.records-per-range".into(),
            "64".into(),
        )]));
    let commit = TableCommit::new(table.clone(), "lsm-writer".into());
    let mut historical = Vec::new();
    let mut last_input = Vec::new();
    for id in 1..=5 {
        let previous = if id == 1 {
            vec![]
        } else {
            indexes(&table).await
        };
        let input = messages(&table, vec![id]).await;
        commit
            .commit_with_identifier(input.clone(), i64::from(id))
            .await
            .unwrap();
        let current = indexes(&table).await;
        assert_eq!(
            scalar_hits(&table, &current, id).await,
            vec![RowRange::new(i64::from(id - 1), i64::from(id - 1))]
        );
        if id < 5 {
            assert_eq!(
                current.len(),
                2 * id as usize,
                "each append must add a leaf per column"
            );
            assert!(
                previous.iter().all(|file| current.contains(file)),
                "small writes must retain existing payloads"
            );
            for kind in ["btree", "bitmap"] {
                let mut ranges = current
                    .iter()
                    .filter(|entry| entry.index_file.index_type == kind)
                    .map(|entry| {
                        let meta = entry.index_file.global_index_meta.as_ref().unwrap();
                        (meta.row_range_start, meta.row_range_end)
                    })
                    .collect::<Vec<_>>();
                ranges.sort_unstable();
                assert_eq!(
                    ranges,
                    (0..i64::from(id)).map(|row| (row, row)).collect::<Vec<_>>()
                );
            }
        } else {
            assert_eq!(
                current.len(),
                2,
                "the fifth adjacent run triggers one merged payload per column"
            );
            assert!(previous.iter().all(|file| !current.contains(file)));
            for file in &current {
                let meta = file.index_file.global_index_meta.as_ref().unwrap();
                assert_eq!((meta.row_range_start, meta.row_range_end), (0, 4));
            }
        }
        historical.extend(previous);
        last_input = input;
    }
    for file in historical {
        assert!(
            table
                .file_io()
                .exists(&format!(
                    "{}/index/{}",
                    table.location(),
                    file.index_file.file_name
                ))
                .await
                .unwrap(),
            "merged inputs remain available to historical snapshots"
        );
    }
    let indexed = latest(&table).await;
    commit
        .filter_and_commit_with_identifier(last_input, 5)
        .await
        .unwrap();
    assert_eq!(
        latest(&table).await,
        indexed,
        "recovery cannot publish duplicate leaf or merged indexes"
    );
    assert_complete(&table).await;
}

#[tokio::test]
async fn tiered_selection_merges_small_tail_runs_without_rebuilding_the_large_base() {
    let table = table(Some(true), false)
        .await
        .copy_with_options(HashMap::from([
            ("sorted-index.records-per-range".into(), "64".into()),
            (
                "global-index.num-sorted-run.compaction-trigger".into(),
                "3".into(),
            ),
        ]));
    let commit = TableCommit::new(table.clone(), "tiered-writer".into());
    commit
        .commit(messages(&table, (0..8).collect()).await)
        .await
        .unwrap();
    let base = indexes(&table).await;
    commit
        .commit(messages(&table, vec![8]).await)
        .await
        .unwrap();
    let second = indexes(&table).await;
    assert_eq!(second.len(), 4);
    assert!(base.iter().all(|file| second.contains(file)));
    commit
        .commit(messages(&table, vec![9]).await)
        .await
        .unwrap();
    let current = indexes(&table).await;
    assert_eq!(
        current.len(),
        4,
        "retain the large run and merge the two small adjacent runs"
    );
    assert!(
        base.iter().all(|file| current.contains(file)),
        "size-ratio selection must not pull in the large base"
    );
    for file in current.iter().filter(|file| !base.contains(file)) {
        let meta = file.index_file.global_index_meta.as_ref().unwrap();
        assert_eq!((meta.row_range_start, meta.row_range_end), (8, 9));
    }
    for id in [0, 8, 9] {
        assert_eq!(
            scalar_hits(&table, &current, id).await,
            vec![RowRange::new(i64::from(id), i64::from(id))]
        );
    }
    assert_complete(&table).await;
}

#[tokio::test]
async fn invalid_index_lsm_parameters_fail_before_data_publication() {
    for (key, value) in [
        ("global-index.num-sorted-run.compaction-trigger", "0"),
        ("global-index.num-sorted-run.compaction-trigger", "invalid"),
        ("global-index.compaction.size-ratio", "-1"),
        (
            "global-index.compaction.max-size-amplification-percent",
            "-1",
        ),
    ] {
        let original = table(Some(true), false).await;
        let table = original.copy_with_options(HashMap::from([(key.into(), value.into())]));
        let error = TableCommit::new(table.clone(), "invalid-lsm".into())
            .commit(messages(&table, vec![1]).await)
            .await
            .expect_err("validate LSM policy before writing the data snapshot");
        assert!(error.to_string().contains(key), "{error}");
        assert!(table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
async fn column_specific_merge_triggers_and_shard_boundaries_are_respected() {
    let table = table(Some(true), false)
        .await
        .copy_with_options(HashMap::from([
            ("sorted-index.records-per-range".into(), "4".into()),
            ("global-index.num-sorted-run.compaction-trigger".into(), "5".into()),
            ("global-index.compaction.size-ratio".into(), "99".into()),
            ("global-index.compaction.max-size-amplification-percent".into(), "9".into()),
            (
                "fields.id.global-index.btree.index-options".into(),
                r#"{"global-index.num-sorted-run.compaction-trigger":2,"global-index.compaction.size-ratio":1,"global-index.compaction.max-size-amplification-percent":200}"#.into(),
            ),
        ]));
    let commit = TableCommit::new(table.clone(), "column-triggers".into());
    for id in [0, 1] {
        commit
            .commit(messages(&table, vec![id]).await)
            .await
            .unwrap();
    }
    let current = indexes(&table).await;
    assert_eq!(
        current
            .iter()
            .filter(|file| file.index_file.index_type == "btree")
            .count(),
        1
    );
    assert_eq!(
        current
            .iter()
            .filter(|file| file.index_file.index_type == "bitmap")
            .count(),
        2
    );
    let before = current;
    // One physical source crosses the row-ID shard boundary.
    commit
        .commit(messages(&table, vec![2, 3, 4]).await)
        .await
        .unwrap();
    let current = indexes(&table).await;
    for file in current
        .iter()
        .filter(|file| file.index_file.index_type == "bitmap")
    {
        let meta = file.index_file.global_index_meta.as_ref().unwrap();
        assert_eq!(meta.row_range_start / 4, meta.row_range_end / 4);
    }
    assert!(before
        .iter()
        .filter(|file| file.index_file.index_type == "bitmap")
        .all(|file| current.contains(file)));
    for id in 0..5 {
        assert_eq!(
            scalar_hits(&table, &current, id).await,
            vec![RowRange::new(i64::from(id), i64::from(id))]
        );
    }
    assert_complete(&table).await;
}

#[tokio::test]
async fn merged_runs_recover_an_uncertain_publication_without_duplicate_outputs() {
    let table = table(Some(true), false)
        .await
        .copy_with_options(HashMap::from([
            ("sorted-index.records-per-range".into(), "64".into()),
            (
                "global-index.num-sorted-run.compaction-trigger".into(),
                "3".into(),
            ),
        ]));
    let seed = TableCommit::new(table.clone(), "merge-recovery".into());
    for id in 1..=2 {
        seed.commit_with_identifier(messages(&table, vec![id]).await, i64::from(id))
            .await
            .unwrap();
    }
    let before = indexes(&table).await;
    let input = messages(&table, vec![3]).await;
    let mut commit = TableCommit::new(table.clone(), "merge-recovery".into());
    commit.snapshot_commit = Arc::new(InjectIndexPublication {
        table: table.clone(),
        mode: Publication::LoseResponse,
        fired: AtomicBool::new(false),
    });
    commit
        .commit_with_identifier(input.clone(), 3)
        .await
        .unwrap();
    let completed = latest(&table).await;
    let current = indexes(&table).await;
    assert_eq!(current.len(), 2);
    assert!(before.iter().all(|file| !current.contains(file)));
    for _ in 0..2 {
        seed.filter_and_commit_with_identifier(input.clone(), 3)
            .await
            .unwrap();
        assert_eq!(latest(&table).await, completed);
        assert_eq!(indexes(&table).await, current);
    }
    assert_complete(&table).await;
}
