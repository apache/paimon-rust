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

use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs, write_batch};
use futures::TryStreamExt;
use paimon::spec::{
    DataType, Datum, IntType, Predicate, PredicateBuilder, Schema, TableSchema, VarCharType,
};
use paimon::table::{IncrementalScanMode, Plan, ReadBuilder, Table};
use std::sync::Arc;

async fn partitioned(mode: &str) -> Table {
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("p", DataType::VarChar(VarCharType::string_type()))
        .column("value", DataType::Int(IntType::new()))
        .column("region", DataType::VarChar(VarCharType::string_type()))
        // Reverse partition order relative to the table's field order.
        .partition_keys(["region", "p"]);
    if mode == "pk" {
        schema = schema
            .primary_key(["id", "p", "region"])
            .option("bucket", "1");
    } else if mode == "de" {
        schema = schema
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true");
    }
    let path = format!("memory:/explicit-partitions-{mode}");
    let (io, table) = memory_table(&path, TableSchema::new(0, &schema.build().unwrap()));
    setup_dirs(&io, &path).await;
    persist_table_schema(&io, &path, table.schema()).await;
    write_batch(
        &table,
        &batch(&[
            (1, "a", 10, "east"),
            (2, "b", 20, "east"),
            (3, "a", 30, "west"),
            (4, "b", 40, "west"),
        ]),
    )
    .await;
    table
}

fn batch(rows: &[(i32, &str, i32, &str)]) -> RecordBatch {
    RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.0))) as ArrayRef,
        ),
        (
            "p",
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))) as ArrayRef,
        ),
        (
            "value",
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.2))) as ArrayRef,
        ),
        (
            "region",
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.3))) as ArrayRef,
        ),
    ])
    .unwrap()
}

fn equal(table: &Table, field: &str, value: &str) -> Predicate {
    PredicateBuilder::new(table.schema().fields())
        .equal(field, Datum::String(value.into()))
        .unwrap()
}

async fn ids(builder: &ReadBuilder<'_>, plan: &Plan) -> Vec<i32> {
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut result = Vec::new();
    for batch in batches {
        assert_eq!(
            batch.schema().fields().len(),
            1,
            "selector/filter fields must stay hidden"
        );
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        result.extend(column.values());
    }
    result.sort_unstable();
    result
}

#[tokio::test]
async fn partition_selector_is_scan_only_and_independent_of_setter_order() {
    for mode in ["append", "pk", "de"] {
        let table = partitioned(mode).await;
        for selector_first in [true, false] {
            let mut builder = table.new_read_builder();
            builder.with_projection(&["id"]).unwrap();
            let row_filter = equal(&table, "p", "a");
            let selector = equal(&table, "region", "east");
            if selector_first {
                builder
                    .with_partition_filter(Some(selector))
                    .unwrap()
                    .with_filter(row_filter);
            } else {
                builder
                    .with_filter(row_filter)
                    .with_partition_filter(Some(selector))
                    .unwrap();
            }
            let plan = builder.new_scan().plan().await.unwrap();
            // Ordinary filters may further prune files by data statistics.
            // The explicit selector must still restrict every planned partition.
            assert!(!plan.splits().is_empty(), "{mode}");
            assert!(plan
                .splits()
                .iter()
                .all(|s| s.partition().get_string(0).unwrap() == "east"));
            assert_eq!(ids(&builder, &plan).await, [1], "{mode}");
            // A separately configured reader must enforce its row predicate
            // even though it was not given the scan-only selector.
            let mut reader = table.new_read_builder();
            reader
                .with_projection(&["id"])
                .unwrap()
                .with_filter(equal(&table, "p", "a"));
            assert_eq!(ids(&reader, &plan).await, [1], "{mode}");
        }
    }
}

#[tokio::test]
async fn replacing_and_clearing_a_selector_preserves_the_row_filter() {
    for mode in ["append", "pk", "de"] {
        let table = partitioned(mode).await;
        let mut builder = table.new_read_builder();
        builder
            .with_projection(&["id"])
            .unwrap()
            .with_filter(equal(&table, "p", "a"));
        builder
            .with_partition_filter(Some(equal(&table, "region", "east")))
            .unwrap();
        let captured = builder.clone();
        builder
            .with_partition_filter(Some(equal(&table, "region", "west")))
            .unwrap();
        assert_eq!(
            ids(&builder, &builder.new_scan().plan().await.unwrap()).await,
            [3]
        );
        assert_eq!(
            ids(&captured, &captured.new_scan().plan().await.unwrap()).await,
            [1]
        );
        builder.with_partition_filter(None).unwrap();
        let plan = builder.new_scan().plan().await.unwrap();
        assert_eq!(plan.splits().len(), 2);
        assert_eq!(ids(&builder, &plan).await, [1, 3]);
        // A later ordinary-filter setter does not reset the explicit selector.
        builder
            .with_partition_filter(Some(equal(&table, "region", "west")))
            .unwrap()
            .with_filter(equal(&table, "p", "b"));
        assert_eq!(
            ids(&builder, &builder.new_scan().plan().await.unwrap()).await,
            [4]
        );
    }
}

#[tokio::test]
async fn contradictory_partition_predicates_do_not_return_rows_or_truncate_matches() {
    for mode in ["append", "pk", "de"] {
        let table = partitioned(mode).await;
        let mut builder = table.new_read_builder();
        builder
            .with_projection(&["id"])
            .unwrap()
            .with_limit(1)
            .with_filter(equal(&table, "region", "west"));
        builder
            .with_partition_filter(Some(equal(&table, "region", "east")))
            .unwrap();
        let plan = builder.new_scan().plan().await.unwrap();
        assert!(ids(&builder, &plan).await.is_empty());
        // A broader selector plus LIMIT cannot count the east files before a
        // west row has survived the ordinary partition predicate.
        builder
            .with_partition_filter(Some(Predicate::or(vec![
                equal(&table, "region", "east"),
                equal(&table, "region", "west"),
            ])))
            .unwrap();
        let result = ids(&builder, &builder.new_scan().plan().await.unwrap()).await;
        assert!(!result.is_empty());
        assert!(result.iter().all(|id| [3, 4].contains(id)));
        // Append/PK callers enforce the final row count; DE enforces it in core.
        if mode == "de" {
            assert_eq!(result.len(), 1);
        }
    }
}

#[tokio::test]
async fn selector_survives_incremental_stream_checkpoint_and_restore() {
    for mode in ["append", "pk", "de"] {
        let table = partitioned(mode).await;
        let mut builder = table.new_read_builder();
        builder.with_projection(&["id"]).unwrap();
        builder
            .with_partition_filter(Some(equal(&table, "region", "east")))
            .unwrap();
        let mut stream = builder.new_stream_scan().unwrap();
        assert_eq!(
            ids(&builder, &stream.plan().await.unwrap().unwrap()).await,
            [1, 2]
        );
        assert_eq!(stream.checkpoint(), Some(2));
        write_batch(
            &table,
            &batch(&[(5, "a", 50, "west"), (6, "b", 60, "east")]),
        )
        .await;
        let delta = builder
            .new_incremental_scan(IncrementalScanMode::Delta, 1, 2)
            .plan_combined()
            .await
            .unwrap();
        assert_eq!(ids(&builder, &delta).await, [6]);
        assert_eq!(
            ids(&builder, &stream.plan().await.unwrap().unwrap()).await,
            [6]
        );
        stream.restore(Some(2)).unwrap();
        assert_eq!(
            ids(&builder, &stream.plan().await.unwrap().unwrap()).await,
            [6]
        );
    }
}

#[tokio::test]
async fn invalid_selector_fails_before_mutating_the_previous_selection() {
    let table = partitioned("append").await;
    let mut builder = table.new_read_builder();
    builder.with_projection(&["id"]).unwrap();
    builder
        .with_partition_filter(Some(equal(&table, "region", "east")))
        .unwrap();
    let nonpartition = PredicateBuilder::new(table.schema().fields())
        .equal("id", Datum::Int(3))
        .unwrap();
    for invalid in [
        nonpartition.clone(),
        Predicate::or(vec![equal(&table, "p", "a"), nonpartition]),
    ] {
        let error = builder.with_partition_filter(Some(invalid)).unwrap_err();
        assert!(error.to_string().contains("only partition keys"));
        assert_eq!(
            ids(&builder, &builder.new_scan().plan().await.unwrap()).await,
            [1, 2]
        );
    }
}

#[tokio::test]
async fn direct_table_read_filter_selects_independently_planned_partitions() {
    for mode in ["append", "pk", "de"] {
        let table = partitioned(mode).await;
        let plan = table.new_read_builder().new_scan().plan().await.unwrap();
        let mut builder = table.new_read_builder();
        builder.with_projection(&["id"]).unwrap();
        let read = builder
            .new_read()
            .unwrap()
            .with_filter(equal(&table, "p", "a"));
        let batches = read
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let mut actual = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>();
        actual.sort_unstable();
        assert_eq!(actual, [1, 3], "{mode}");
    }
}

#[tokio::test]
async fn ordinary_partition_filter_survives_diff_workers_and_audit_images() {
    let table = partitioned("pk").await;
    write_batch(
        &table,
        &batch(&[(1, "a", 100, "east"), (2, "b", 200, "east")]),
    )
    .await;
    let plan = table
        .new_read_builder()
        .new_incremental_scan(IncrementalScanMode::Diff, 1, 2)
        .plan()
        .await
        .unwrap();
    let mut builder = table.new_read_builder();
    builder
        .with_projection(&["id"])
        .unwrap()
        .with_filter(equal(&table, "p", "a"));
    let read = builder.new_read().unwrap();
    let rows = read
        .to_incremental_arrow(&plan)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(rows.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
    assert_eq!(
        rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        1
    );
    let audit = read
        .to_audit_log_arrow(&plan)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(audit.iter().map(|batch| batch.num_rows()).sum::<usize>(), 2);
    for batch in audit {
        assert!(batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .iter()
            .all(|id| *id == 1));
    }
}
