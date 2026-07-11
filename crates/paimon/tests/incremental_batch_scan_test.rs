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
    make_batch, make_partitioned_batch, memory_table, partitioned_pk_schema, persist_table_schema,
    pk_schema, setup_dirs, write_batch, write_partitioned,
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

/// Changelog mode is reserved but not implemented in this PR.
#[tokio::test]
async fn changelog_mode_is_unsupported() {
    let table_path = "memory:/incremental_batch/changelog_unsupported";
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

    let err = plan_incremental(&table, IncrementalScanMode::Changelog, 0, 1)
        .await
        .unwrap_err();
    assert!(
        matches!(err, paimon::Error::Unsupported { .. }),
        "expected Unsupported for Changelog, got {err:?}"
    );
}
