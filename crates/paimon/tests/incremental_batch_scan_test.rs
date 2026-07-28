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

mod common;

use arrow_array::{Array, Int32Array, RecordBatch};
use futures::TryStreamExt;
use paimon::table::IncrementalScanMode;

use common::incremental_helpers::{
    make_batch, make_batch_with_kinds, make_partitioned_batch, memory_table, partitioned_pk_schema,
    persist_table_schema, pk_schema, setup_dirs, write_batch, write_partitioned,
};

fn collect_pairs(batches: &[RecordBatch]) -> Vec<(i32, i32)> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((ids.value(row), values.value(row)));
        }
    }
    rows.sort_unstable();
    rows
}

async fn read_incremental_pairs(
    table: &paimon::table::Table,
    mode: IncrementalScanMode,
    start_exclusive: i64,
    end_inclusive: i64,
) -> Vec<(i32, i32)> {
    let builder = table.new_read_builder();
    let plan = builder
        .new_incremental_scan(mode, start_exclusive, end_inclusive)
        .plan()
        .await
        .unwrap();
    let read = table.new_read_builder().new_read().unwrap();
    let batches: Vec<RecordBatch> = read
        .to_incremental_arrow(&plan)
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    collect_pairs(&batches)
}

async fn read_current_pairs(table: &paimon::table::Table) -> Vec<(i32, i32)> {
    let builder = table.new_read_builder();
    let plan = builder.new_scan().plan().await.unwrap();
    let read = table.new_read_builder().new_read().unwrap();
    let batches: Vec<RecordBatch> = read
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    collect_pairs(&batches)
}

async fn plan_incremental(
    table: &paimon::table::Table,
    mode: IncrementalScanMode,
    start_exclusive: i64,
    end_inclusive: i64,
) -> Result<paimon::table::IncrementalPlan, paimon::Error> {
    table
        .new_read_builder()
        .new_incremental_scan(mode, start_exclusive, end_inclusive)
        .plan()
        .await
}

/// Start exclusive / end inclusive: (0, 2] includes both appends; (1, 2] only the second.
#[tokio::test]
async fn delta_between_snapshots_reads_only_append_snapshots_in_left_open_range() {
    let table_path = "memory:/incremental_batch/delta_range";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "none"),
            ("merge-engine", "deduplicate"),
            ("bucket", "1"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    write_batch(&table, &make_batch(vec![2], vec![20])).await;

    let rows = read_incremental_pairs(&table, IncrementalScanMode::Delta, 0, 2).await;
    assert_eq!(rows, vec![(1, 10), (2, 20)]);

    let rows = read_incremental_pairs(&table, IncrementalScanMode::Delta, 1, 2).await;
    assert_eq!(rows, vec![(2, 20)]);
}

#[tokio::test]
async fn auto_uses_delta_when_changelog_producer_is_none() {
    let table_path = "memory:/incremental_batch/auto_delta";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "none"),
            ("merge-engine", "deduplicate"),
            ("bucket", "1"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(&table, &make_batch(vec![3], vec![30])).await;
    write_batch(&table, &make_batch(vec![4], vec![40])).await;

    let delta = read_incremental_pairs(&table, IncrementalScanMode::Delta, 0, 2).await;
    let auto = read_incremental_pairs(&table, IncrementalScanMode::Auto, 0, 2).await;
    assert_eq!(auto, delta);
}

/// Empty range (start == end) yields no splits / no rows.
#[tokio::test]
async fn delta_empty_range_returns_no_rows() {
    let table_path = "memory:/incremental_batch/delta_empty";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "none"),
            ("merge-engine", "deduplicate"),
            ("bucket", "1"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;

    let plan = plan_incremental(&table, IncrementalScanMode::Delta, 1, 1)
        .await
        .unwrap();
    assert!(plan.splits().is_empty());
    assert_eq!(plan.mode(), IncrementalScanMode::Delta);

    let rows = read_incremental_pairs(&table, IncrementalScanMode::Delta, 1, 1).await;
    assert!(rows.is_empty());
}

/// Out-of-bounds ranges fail loudly with DataInvalid.
#[tokio::test]
async fn delta_rejects_out_of_range_snapshot_ids() {
    let table_path = "memory:/incremental_batch/delta_oob";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "none"),
            ("merge-engine", "deduplicate"),
            ("bucket", "1"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;

    // end past latest
    let err = plan_incremental(&table, IncrementalScanMode::Delta, 0, 99)
        .await
        .unwrap_err();
    assert!(
        matches!(err, paimon::Error::DataInvalid { .. }),
        "expected DataInvalid for end > latest, got {err:?}"
    );

    // start below earliest - 1 (earliest=1, min_start=0)
    let err = plan_incremental(&table, IncrementalScanMode::Delta, -2, 1)
        .await
        .unwrap_err();
    assert!(
        matches!(err, paimon::Error::DataInvalid { .. }),
        "expected DataInvalid for start < earliest-1, got {err:?}"
    );

    // start > end
    let err = plan_incremental(&table, IncrementalScanMode::Delta, 2, 1)
        .await
        .unwrap_err();
    assert!(
        matches!(err, paimon::Error::DataInvalid { .. }),
        "expected DataInvalid for start > end, got {err:?}"
    );
}

/// Partition filter from ReadBuilder is pushed into the delta plan path.
#[tokio::test]
async fn incremental_delta_scan_applies_partition_filter_from_read_builder() {
    use paimon::spec::{Datum, PredicateBuilder};
    use std::collections::HashMap;

    let table_path = "memory:/incremental_batch/delta_partition_filter";
    let (file_io, mut table) = memory_table(table_path, partitioned_pk_schema("1"));
    table = table.copy_with_options(HashMap::from([(
        "changelog-producer".to_string(),
        "none".to_string(),
    )]));
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_partitioned(&table, make_partitioned_batch(vec!["a"], vec![1], vec![10])).await;
    write_partitioned(&table, make_partitioned_batch(vec!["b"], vec![2], vec![20])).await;

    let filter = PredicateBuilder::new(table.schema().fields())
        .equal("pt", Datum::String("a".to_string()))
        .unwrap();
    let mut builder = table.new_read_builder();
    builder
        .with_projection(&["id", "value"])
        .unwrap()
        .with_filter(filter);
    let plan = builder
        .new_incremental_scan(IncrementalScanMode::Delta, 0, 2)
        .plan()
        .await
        .unwrap();
    let read = builder.new_read().unwrap();
    let batches: Vec<RecordBatch> = read
        .to_incremental_arrow(&plan)
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    assert_eq!(collect_pairs(&batches), vec![(1, 10)]);
}

/// Changelog mode reads existing changelog_manifest_list data files.
#[tokio::test]
async fn changelog_between_snapshots_reads_changelog_manifest_files() {
    let table_path = "memory:/incremental_batch/changelog_range";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "input"),
            ("merge-engine", "deduplicate"),
            ("bucket", "1"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    let builder = table.new_write_builder();
    let mut write = builder.new_write().unwrap();
    write
        .write_arrow_batch(&make_batch_with_kinds(vec![1, 1], vec![10, 20], vec![0, 2]))
        .await
        .unwrap();
    let messages = write.prepare_commit().await.unwrap();
    builder.new_commit().commit(messages).await.unwrap();

    let rows = read_incremental_pairs(&table, IncrementalScanMode::Changelog, 0, 1).await;
    assert_eq!(rows, vec![(1, 10), (1, 20)]);
}

#[tokio::test]
async fn partial_update_ignore_delete_filters_data_and_input_changelog() {
    let table_path = "memory:/incremental_batch/partial_update_ignore_delete";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "input"),
            ("merge-engine", "partial-update"),
            ("partial-update.ignore-delete", "true"),
            ("bucket", "1"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(
        &table,
        &make_batch_with_kinds(vec![1, 1, 2, 1], vec![10, 999, 200, 20], vec![0, 3, 1, 2]),
    )
    .await;

    assert_eq!(read_current_pairs(&table).await, vec![(1, 20)]);
    assert_eq!(
        read_incremental_pairs(&table, IncrementalScanMode::Changelog, 0, 1).await,
        vec![(1, 10), (1, 20)]
    );
}

/// Multi-snapshot changelog range is left-open / right-closed and ordered by snapshot id.
#[tokio::test]
async fn changelog_multi_snapshot_range_is_ordered_and_left_open() {
    let table_path = "memory:/incremental_batch/changelog_multi";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "input"),
            ("merge-engine", "deduplicate"),
            ("bucket", "1"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(&table, &make_batch_with_kinds(vec![1], vec![10], vec![0])).await;
    write_batch(&table, &make_batch_with_kinds(vec![2], vec![20], vec![0])).await;

    let all = read_incremental_pairs(&table, IncrementalScanMode::Changelog, 0, 2).await;
    assert_eq!(all, vec![(1, 10), (2, 20)]);

    let second_only = read_incremental_pairs(&table, IncrementalScanMode::Changelog, 1, 2).await;
    assert_eq!(second_only, vec![(2, 20)]);
}

/// Auto resolves to Changelog when producer is not `none`.
#[tokio::test]
async fn auto_uses_changelog_when_producer_is_input() {
    let table_path = "memory:/incremental_batch/auto_changelog";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "input"),
            ("merge-engine", "deduplicate"),
            ("bucket", "1"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(
        &table,
        &make_batch_with_kinds(vec![1, 1], vec![10, 20], vec![0, 2]),
    )
    .await;

    let plan = plan_incremental(&table, IncrementalScanMode::Auto, 0, 1)
        .await
        .unwrap();
    assert_eq!(plan.mode(), IncrementalScanMode::Changelog);

    let auto = read_incremental_pairs(&table, IncrementalScanMode::Auto, 0, 1).await;
    let changelog = read_incremental_pairs(&table, IncrementalScanMode::Changelog, 0, 1).await;
    assert_eq!(auto, changelog);
    assert_eq!(auto, vec![(1, 10), (1, 20)]);
}

/// Partition filter from ReadBuilder is pushed into the changelog plan path.
#[tokio::test]
async fn incremental_changelog_scan_applies_partition_filter_from_read_builder() {
    use paimon::spec::{Datum, PredicateBuilder};
    use std::collections::HashMap;

    let table_path = "memory:/incremental_batch/changelog_partition_filter";
    let (file_io, mut table) = memory_table(table_path, partitioned_pk_schema("1"));
    table = table.copy_with_options(HashMap::from([(
        "changelog-producer".to_string(),
        "input".to_string(),
    )]));
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    let builder = table.new_write_builder();
    let mut write = builder.new_write().unwrap();
    // Two partitions in one commit → one snapshot with both changelog files.
    let schema = std::sync::Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("pt", arrow_schema::DataType::Utf8, false),
        arrow_schema::Field::new("id", arrow_schema::DataType::Int32, false),
        arrow_schema::Field::new("value", arrow_schema::DataType::Int32, false),
        arrow_schema::Field::new("_VALUE_KIND", arrow_schema::DataType::Int8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            std::sync::Arc::new(arrow_array::StringArray::from(vec!["a", "b"])),
            std::sync::Arc::new(arrow_array::Int32Array::from(vec![1, 2])),
            std::sync::Arc::new(arrow_array::Int32Array::from(vec![10, 20])),
            std::sync::Arc::new(arrow_array::Int8Array::from(vec![0, 0])),
        ],
    )
    .unwrap();
    write.write_arrow_batch(&batch).await.unwrap();
    let messages = write.prepare_commit().await.unwrap();
    builder.new_commit().commit(messages).await.unwrap();

    let filter = PredicateBuilder::new(table.schema().fields())
        .equal("pt", Datum::String("a".to_string()))
        .unwrap();
    let mut builder = table.new_read_builder();
    builder
        .with_projection(&["id", "value"])
        .unwrap()
        .with_filter(filter);
    let plan = builder
        .new_incremental_scan(IncrementalScanMode::Changelog, 0, 1)
        .plan()
        .await
        .unwrap();
    let read = builder.new_read().unwrap();
    let batches: Vec<RecordBatch> = read
        .to_incremental_arrow(&plan)
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    assert_eq!(collect_pairs(&batches), vec![(1, 10)]);
}

#[tokio::test]
async fn diff_between_snapshots_returns_after_image_rows() {
    let table_path = "memory:/incremental_batch/diff_after_image";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "none"),
            ("merge-engine", "deduplicate"),
            ("bucket", "1"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
    write_batch(&table, &make_batch(vec![2, 3], vec![25, 30])).await;

    let rows = read_incremental_pairs(&table, IncrementalScanMode::Diff, 1, 2).await;
    assert_eq!(rows, vec![(2, 25), (3, 30)]);
}

#[tokio::test]
async fn diff_identical_rows_are_skipped_from_after_image() {
    let table_path = "memory:/incremental_batch/diff_identical";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "none"),
            ("merge-engine", "deduplicate"),
            ("bucket", "1"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;

    let rows = read_incremental_pairs(&table, IncrementalScanMode::Diff, 1, 2).await;
    assert_eq!(rows, vec![(2, 20)]);
}

#[tokio::test]
async fn diff_projection_without_primary_key_still_compares_full_rows() {
    let table_path = "memory:/incremental_batch/diff_projection_without_pk";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "none"),
            ("merge-engine", "deduplicate"),
            ("bucket", "1"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    write_batch(&table, &make_batch(vec![1], vec![20])).await;

    let mut builder = table.new_read_builder();
    builder.with_projection(&["value"]).unwrap();
    let plan = builder
        .new_incremental_scan(IncrementalScanMode::Diff, 1, 2)
        .plan()
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_incremental_arrow(&plan)
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let values: Vec<i32> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect();
    assert_eq!(values, vec![20]);
}

#[tokio::test]
async fn diff_change_outside_projection_is_not_missed() {
    let table_path = "memory:/incremental_batch/diff_unprojected_change";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "none"),
            ("merge-engine", "deduplicate"),
            ("bucket", "1"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    write_batch(&table, &make_batch(vec![1], vec![20])).await;

    let mut builder = table.new_read_builder();
    builder.with_projection(&["id"]).unwrap();
    let plan = builder
        .new_incremental_scan(IncrementalScanMode::Diff, 1, 2)
        .plan()
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_incremental_arrow(&plan)
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let ids: Vec<i32> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect();
    assert_eq!(ids, vec![1]);
}

#[tokio::test]
async fn diff_null_to_zero_is_reported_as_change() {
    use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use paimon::spec::{DataType, IntType, Schema, TableSchema};
    use std::sync::Arc;

    let table_path = "memory:/incremental_batch/diff_null_to_zero";
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::with_nullable(true)))
        .primary_key(["id"])
        .option("changelog-producer", "none")
        .option("merge-engine", "deduplicate")
        .option("bucket", "1")
        .option("bucket-key", "id")
        .build()
        .unwrap();
    let (file_io, table) = memory_table(table_path, TableSchema::new(0, &schema));
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    let make_nullable_batch = |value| {
        RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("id", ArrowDataType::Int32, false),
                Field::new("value", ArrowDataType::Int32, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![value])),
            ],
        )
        .unwrap()
    };
    write_batch(&table, &make_nullable_batch(None)).await;
    write_batch(&table, &make_nullable_batch(Some(0))).await;

    let rows = read_incremental_pairs(&table, IncrementalScanMode::Diff, 1, 2).await;
    assert_eq!(rows, vec![(1, 0)]);
}

#[tokio::test]
async fn diff_ignores_scan_limit_when_planning_full_states() {
    let table_path = "memory:/incremental_batch/diff_limit";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "none"),
            ("merge-engine", "deduplicate"),
            ("bucket", "4"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(&table, &make_batch(vec![1, 8], vec![10, 80])).await;
    write_batch(&table, &make_batch(vec![1, 8], vec![11, 81])).await;

    let mut builder = table.new_read_builder();
    builder.with_limit(1);
    let plan = builder
        .new_incremental_scan(IncrementalScanMode::Diff, 1, 2)
        .plan()
        .await
        .unwrap();
    let pair_count = plan
        .splits()
        .iter()
        .filter(|split| matches!(split, paimon::table::IncrementalSplit::DiffPair { .. }))
        .count();
    assert!(pair_count >= 2, "limit must not truncate Diff state pairs");

    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_incremental_arrow(&plan)
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(collect_pairs(&batches), vec![(1, 11), (8, 81)]);
}

#[tokio::test]
async fn diff_empty_projection_preserves_changed_row_count() {
    let table_path = "memory:/incremental_batch/diff_empty_projection";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "none"),
            ("merge-engine", "deduplicate"),
            ("bucket", "1"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    write_batch(&table, &make_batch(vec![1], vec![20])).await;

    let mut builder = table.new_read_builder();
    builder.with_projection(&[]).unwrap();
    let plan = builder
        .new_incremental_scan(IncrementalScanMode::Diff, 1, 2)
        .plan()
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_incremental_arrow(&plan)
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert!(batches.iter().all(|batch| batch.num_columns() == 0));
}

#[tokio::test]
async fn diff_rejects_row_ranges_instead_of_dropping_them() {
    use paimon::table::RowRange;

    let table_path = "memory:/incremental_batch/diff_row_ranges";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "none"),
            ("merge-engine", "deduplicate"),
            ("bucket", "1"),
            ("row-tracking.enabled", "true"),
            ("target-file-size", "1b"),
            ("source.split.target-size", "1b"),
            ("source.split.open-file-cost", "1b"),
            ("num-sorted-run.compaction-trigger", "100"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    write_batch(&table, &make_batch(vec![3], vec![30])).await;
    write_batch(&table, &make_batch(vec![1], vec![11])).await;

    let mut builder = table.new_read_builder();
    builder.with_row_ranges(vec![RowRange::new(1, 2)]);
    let err = builder
        .new_incremental_scan(IncrementalScanMode::Diff, 2, 3)
        .plan()
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            paimon::Error::Unsupported { ref message } if message.contains("_ROW_ID")
        ),
        "Diff must reject _ROW_ID row-range filters instead of dropping them: {err:?}"
    );
}

#[tokio::test]
async fn diff_reads_more_than_128_files_in_one_side() {
    let table_path = "memory:/incremental_batch/diff_many_files";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "none"),
            ("merge-engine", "deduplicate"),
            ("bucket", "1"),
            ("target-file-size", "1b"),
            ("source.split.target-size", "1b"),
            ("source.split.open-file-cost", "1b"),
            ("num-sorted-run.compaction-trigger", "1000"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;

    for id in 0..129 {
        write_batch(&table, &make_batch(vec![id], vec![10])).await;
    }
    write_batch(&table, &make_batch(vec![0], vec![11])).await;

    let plan = plan_incremental(&table, IncrementalScanMode::Diff, 129, 130)
        .await
        .unwrap();
    let file_count = plan
        .splits()
        .iter()
        .map(|split| match split {
            paimon::table::IncrementalSplit::DiffPair { before, .. } => before
                .iter()
                .map(|split| split.data_files().len())
                .sum::<usize>(),
            paimon::table::IncrementalSplit::Data(_) => 0,
        })
        .sum::<usize>();
    assert!(file_count > 128, "test requires more than 128 before files");

    assert_eq!(
        read_incremental_pairs(&table, IncrementalScanMode::Diff, 129, 130).await,
        vec![(0, 11)]
    );
}

#[tokio::test]
async fn diff_rejects_start_before_earliest_snapshot() {
    let table_path = "memory:/incremental_batch/diff_earliest";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "none"),
            ("merge-engine", "deduplicate"),
            ("bucket", "1"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;

    let err = plan_incremental(&table, IncrementalScanMode::Diff, 0, 1)
        .await
        .unwrap_err();
    assert!(
        matches!(err, paimon::Error::DataInvalid { .. }),
        "expected DataInvalid, got {err:?}"
    );
}

#[tokio::test]
async fn diff_rejects_non_deduplicate_merge_engine() {
    for merge_engine in ["partial-update", "aggregation", "first-row"] {
        let table_path = format!("memory:/incremental_batch/diff_engine_{merge_engine}");
        let (file_io, table) = memory_table(
            &table_path,
            pk_schema(&[
                ("changelog-producer", "none"),
                ("merge-engine", merge_engine),
                ("bucket", "1"),
            ]),
        );
        setup_dirs(&file_io, &table_path).await;
        persist_table_schema(&file_io, &table_path, table.schema()).await;
        write_batch(&table, &make_batch(vec![1], vec![10])).await;
        write_batch(&table, &make_batch(vec![2], vec![20])).await;

        let err = plan_incremental(&table, IncrementalScanMode::Diff, 1, 2)
            .await
            .unwrap_err();
        assert!(
            matches!(err, paimon::Error::Unsupported { .. }),
            "merge-engine={merge_engine} expected Unsupported, got {err:?}"
        );
    }
}

#[tokio::test]
async fn diff_rejects_table_without_primary_keys() {
    use paimon::spec::{DataType, IntType, Schema, TableSchema};

    let table_path = "memory:/incremental_batch/diff_without_primary_keys";
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .option("changelog-producer", "none")
        .option("merge-engine", "deduplicate")
        .option("bucket", "1")
        .option("bucket-key", "id")
        .build()
        .unwrap();
    let (file_io, table) = memory_table(table_path, TableSchema::new(0, &schema));
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    write_batch(&table, &make_batch(vec![2], vec![20])).await;

    let err = plan_incremental(&table, IncrementalScanMode::Diff, 1, 2)
        .await
        .unwrap_err();
    assert!(
        matches!(err, paimon::Error::Unsupported { ref message } if message.contains("primary keys")),
        "expected Unsupported for a table without primary keys, got {err:?}"
    );
}

#[test]
fn incremental_plan_rejects_data_split_in_diff_mode() {
    use paimon::spec::BinaryRow;
    use paimon::table::{DataSplitBuilder, IncrementalPlan, IncrementalSplit};

    let split = DataSplitBuilder::new()
        .with_snapshot(1)
        .with_partition(BinaryRow::new(0))
        .with_bucket(0)
        .with_bucket_path("memory:/incremental_batch/bucket-0".to_string())
        .with_total_buckets(1)
        .with_data_files(Vec::new())
        .build()
        .unwrap();
    let err = IncrementalPlan::try_new(
        IncrementalScanMode::Diff,
        vec![IncrementalSplit::Data(split)],
    )
    .unwrap_err();
    assert!(
        matches!(err, paimon::Error::DataInvalid { ref message, .. } if message.contains("Data split")),
        "Diff plan must reject Data splits instead of silently skipping them: {err:?}"
    );
}

#[test]
fn incremental_plan_rejects_diff_pair_with_mismatched_bucket_metadata() {
    use paimon::spec::BinaryRow;
    use paimon::table::{DataSplitBuilder, IncrementalPlan, IncrementalSplit};

    let split = |bucket| {
        DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(bucket)
            .with_bucket_path(format!("memory:/incremental_batch/bucket-{bucket}"))
            .with_total_buckets(2)
            .with_data_files(Vec::new())
            .build()
            .unwrap()
    };
    let err = IncrementalPlan::try_new(
        IncrementalScanMode::Diff,
        vec![IncrementalSplit::DiffPair {
            before: vec![split(0)],
            after: vec![split(1)],
        }],
    )
    .unwrap_err();
    assert!(
        matches!(err, paimon::Error::DataInvalid { ref message, .. } if message.contains("partition buckets")),
        "Diff plan must reject pairs that cross partition buckets: {err:?}"
    );
}

#[test]
fn incremental_plan_rejects_partial_or_inconsistent_diff_states() {
    use paimon::spec::BinaryRow;
    use paimon::table::{DataSplitBuilder, IncrementalPlan, IncrementalSplit, RowRange};

    let split = |snapshot, with_row_ranges| {
        let mut builder = DataSplitBuilder::new()
            .with_snapshot(snapshot)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("memory:/incremental_batch/bucket-0".to_string())
            .with_total_buckets(1)
            .with_data_files(Vec::new());
        if with_row_ranges {
            builder = builder.with_row_ranges(vec![RowRange::new(0, 1)]);
        }
        builder.build().unwrap()
    };

    let err = IncrementalPlan::try_new(
        IncrementalScanMode::Diff,
        vec![IncrementalSplit::DiffPair {
            before: vec![split(1, true)],
            after: vec![split(2, false)],
        }],
    )
    .unwrap_err();
    assert!(
        matches!(err, paimon::Error::DataInvalid { ref message, .. } if message.contains("row ranges")),
        "Diff plan must reject partial physical row ranges: {err:?}"
    );

    let err = IncrementalPlan::try_new(
        IncrementalScanMode::Diff,
        vec![IncrementalSplit::DiffPair {
            before: vec![split(2, false)],
            after: vec![split(1, false)],
        }],
    )
    .unwrap_err();
    assert!(
        matches!(err, paimon::Error::DataInvalid { ref message, .. } if message.contains("earlier")),
        "Diff plan must reject reversed snapshot states: {err:?}"
    );

    let err = IncrementalPlan::try_new(
        IncrementalScanMode::Diff,
        vec![
            IncrementalSplit::DiffPair {
                before: vec![split(1, false)],
                after: Vec::new(),
            },
            IncrementalSplit::DiffPair {
                before: vec![split(2, false)],
                after: Vec::new(),
            },
        ],
    )
    .unwrap_err();
    assert!(
        matches!(err, paimon::Error::DataInvalid { ref message, .. } if message.contains("before snapshots")),
        "Diff plan must reject mixed before snapshots: {err:?}"
    );
}

#[tokio::test]
async fn diff_rejects_bucket_rescale_between_snapshots() {
    use paimon::spec::SchemaChange;

    let table_path = "memory:/incremental_batch/diff_bucket_rescale";
    let (file_io, table) = memory_table(
        table_path,
        pk_schema(&[
            ("changelog-producer", "none"),
            ("merge-engine", "deduplicate"),
            ("bucket", "1"),
        ]),
    );
    setup_dirs(&file_io, table_path).await;
    persist_table_schema(&file_io, table_path, table.schema()).await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;

    let schema = table
        .schema()
        .apply_changes(vec![SchemaChange::set_option(
            "bucket".to_string(),
            "2".to_string(),
        )])
        .unwrap();
    let table = paimon::table::Table::new(
        file_io.clone(),
        table.identifier().clone(),
        table_path.to_string(),
        schema,
        None,
    );
    persist_table_schema(&file_io, table_path, table.schema()).await;
    write_batch(&table, &make_batch(vec![2], vec![20])).await;

    let err = plan_incremental(&table, IncrementalScanMode::Diff, 1, 2)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            paimon::Error::Unsupported { ref message } if message.contains("bucket rescale")
        ),
        "expected Unsupported for bucket rescale, got {err:?}"
    );
}
