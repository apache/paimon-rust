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
use crate::spec::{GlobalIndexMeta, IndexFileMeta};

async fn source(rows: i32) -> (Table, Definition, Vec<ManifestEntry>) {
    let table = crate::table::table_commit::auto_index_tests::table(Some(false), false)
        .await
        .copy_with_options(HashMap::from([(
            "sorted-index.records-per-range".into(),
            "64".into(),
        )]));
    TableCommit::new(table.clone(), "metadata-source".into())
        .commit(
            crate::table::table_commit::auto_index_tests::messages(&table, (0..rows).collect())
                .await,
        )
        .await
        .unwrap();
    let snapshot = table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    let data = crate::table::snapshot_manifest::read_snapshot_entries(
        &table.snapshot_manager(),
        &snapshot,
        None,
    )
    .await
    .unwrap();
    let definition = definitions(&table)
        .unwrap()
        .into_iter()
        .find(|definition| definition.column == "id")
        .unwrap();
    (table, definition, data)
}

fn entry(data: &ManifestEntry, name: &str, start: i64, end: i64) -> IndexManifestEntry {
    IndexManifestEntry {
        kind: FileKind::Add,
        partition: data.partition().to_vec(),
        bucket: data.bucket(),
        version: 1,
        index_file: IndexFileMeta {
            index_type: "btree".into(),
            file_name: name.into(),
            file_size: 17,
            row_count: end - start + 1,
            deletion_vectors_ranges: None,
            external_path: None,
            global_index_meta: Some(GlobalIndexMeta {
                row_range_start: start,
                row_range_end: end,
                index_field_id: 0,
                extra_field_ids: None,
                source_meta: None,
                index_meta: None,
            }),
        },
    }
}

#[tokio::test]
async fn missing_ranges_leave_compatible_fragments_untouched_below_the_trigger() {
    let (table, definition, data) = source(4).await;
    let indexes = vec![
        entry(&data[0], "first.index", 0, 0),
        entry(&data[0], "middle.index", 2, 2),
    ];
    let plan = plan(&table, &definition, &data, &indexes).await.unwrap();
    assert!(plan.needs_build);
    assert!(plan.deletions.is_empty());
    assert_eq!(
        plan.build_ranges.values().next().unwrap(),
        &vec![RowRange::new(1, 1), RowRange::new(3, 3)]
    );
}

#[tokio::test]
async fn compaction_does_not_bridge_missing_logical_rows() {
    let (table, definition, mut data) = source(4).await;
    data[0].file.row_count = 2;
    let mut other = data[0].clone();
    other.file.file_name = "other.parquet".into();
    other.file.first_row_id = Some(10);
    data.push(other);
    let indexes = vec![
        entry(&data[0], "zero.index", 0, 0),
        entry(&data[0], "one.index", 1, 1),
        entry(&data[0], "ten.index", 10, 10),
        entry(&data[0], "eleven.index", 11, 11),
    ];
    let table = table.copy_with_options(HashMap::from([(
        "global-index.num-sorted-run.compaction-trigger".into(),
        "3".into(),
    )]));
    let plan = plan(&table, &definition, &data, &indexes).await.unwrap();
    assert!(
        !plan.needs_build,
        "two runs in each connected group cannot become one index across a row-ID hole"
    );
    assert!(plan.deletions.is_empty());
}

#[tokio::test]
async fn unchanged_logical_sources_reuse_runs_but_removed_rows_and_overlaps_are_rebuilt() {
    let (table, definition, mut data) = source(4).await;
    let indexes = vec![
        entry(&data[0], "first.index", 0, 1),
        entry(&data[0], "second.index", 2, 3),
    ];
    data[0].file.file_name = "compacted.parquet".into();
    let unchanged = plan(&table, &definition, &data, &indexes).await.unwrap();
    assert!(
        !unchanged.needs_build,
        "physical compaction alone must not invalidate logical index runs"
    );
    assert!(unchanged.deletions.is_empty());
    let overlapping = vec![
        entry(&data[0], "wide.index", 0, 2),
        entry(&data[0], "overlap.index", 1, 3),
    ];
    let repaired = plan(&table, &definition, &data, &overlapping)
        .await
        .unwrap();
    assert_eq!(repaired.deletions[0].deleted_index_files.len(), 2);
    assert_eq!(
        repaired.build_ranges.values().next().unwrap(),
        &vec![RowRange::new(0, 3)]
    );
    data[0].file.row_count = 2;
    let removed = plan(
        &table,
        &definition,
        &data,
        &[entry(&data[0], "stale.index", 0, 3)],
    )
    .await
    .unwrap();
    assert_eq!(removed.deletions[0].deleted_index_files.len(), 1);
    assert_eq!(
        removed.build_ranges.values().next().unwrap(),
        &vec![RowRange::new(0, 1)]
    );
}
