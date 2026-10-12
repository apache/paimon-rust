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

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, Int32Array, Int64Array, Int8Array, RecordBatch, TimestampMicrosecondArray,
    TimestampMillisecondArray,
};
use futures::TryStreamExt;
use paimon::spec::{
    BigIntType, DataType, Datum, IntType, LocalZonedTimestampType, Predicate, PredicateBuilder,
    Schema, SchemaChange, TableSchema, TimestampType, TinyIntType,
};
use paimon::table::Table;

use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs, write_batch};

async fn ids(table: &Table, predicate: Predicate) -> Vec<i32> {
    let mut builder = table.new_read_builder();
    builder
        .with_filter(predicate)
        .with_projection(&["id"])
        .unwrap();
    let plan = builder.new_scan().plan().await.unwrap();
    let batches = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let mut ids = batches
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
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn converted_predicates_filter_after_schema_evolution_for_every_table_mode() {
    for mode in ["append", "pk", "de"] {
        let mut schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("v", DataType::BigInt(BigIntType::new()))
            .column("t", DataType::Timestamp(TimestampType::new(3).unwrap()))
            .option("read.batch-size", "1");
        if mode == "pk" {
            schema = schema.primary_key(["id"]).option("bucket", "1");
        }
        if mode == "de" {
            schema = schema
                .option("data-evolution.enabled", "true")
                .option("row-tracking.enabled", "true");
        }
        let (io, old) = memory_table(
            &format!("memory:/evolution_cast_{mode}"),
            TableSchema::new(0, &schema.build().unwrap()),
        );
        setup_dirs(&io, old.location()).await;
        persist_table_schema(&io, old.location(), old.schema()).await;
        write_batch(
            &old,
            &RecordBatch::try_from_iter([
                ("id", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
                (
                    "v",
                    Arc::new(Int64Array::from(vec![Some(128), Some(-129), None])) as ArrayRef,
                ),
                (
                    "t",
                    Arc::new(TimestampMillisecondArray::from(vec![
                        Some(123),
                        Some(-1),
                        None,
                    ])) as ArrayRef,
                ),
            ])
            .unwrap(),
        )
        .await;
        // A single-value file's bounds must not prune values after truncation
        // or wrapping. A wide old-file range can hide this stats regression.
        write_batch(
            &old,
            &RecordBatch::try_from_iter([
                ("id", Arc::new(Int32Array::from(vec![5])) as ArrayRef),
                ("v", Arc::new(Int64Array::from(vec![128])) as ArrayRef),
                (
                    "t",
                    Arc::new(TimestampMillisecondArray::from(vec![123])) as ArrayRef,
                ),
            ])
            .unwrap(),
        )
        .await;
        let schema = old
            .schema()
            .apply_changes(vec![
                SchemaChange::update_column_type("v".into(), DataType::TinyInt(TinyIntType::new())),
                SchemaChange::update_column_type(
                    "t".into(),
                    DataType::Timestamp(TimestampType::new(1).unwrap()),
                ),
            ])
            .unwrap();
        persist_table_schema(&io, old.location(), &schema).await;
        let current = old.copy_with_resolved_schema(schema, "main").unwrap();
        write_batch(
            &current,
            &RecordBatch::try_from_iter([
                ("id", Arc::new(Int32Array::from(vec![4])) as ArrayRef),
                ("v", Arc::new(Int8Array::from(vec![7])) as ArrayRef),
                (
                    "t",
                    Arc::new(TimestampMillisecondArray::from(vec![500])) as ArrayRef,
                ),
            ])
            .unwrap(),
        )
        .await;
        let pb = PredicateBuilder::new(current.schema().fields());
        let narrowed = pb.equal("v", Datum::TinyInt(-128)).unwrap();
        let truncated = pb
            .equal(
                "t",
                Datum::Timestamp {
                    millis: 100,
                    nanos: 0,
                },
            )
            .unwrap();
        assert_eq!(
            ids(&current, narrowed.clone()).await,
            vec![1, 5],
            "{mode} numeric"
        );
        assert_eq!(
            ids(&current, truncated.clone()).await,
            vec![1, 5],
            "{mode} timestamp"
        );
        assert_eq!(
            ids(&current, Predicate::and(vec![narrowed, truncated])).await,
            vec![1, 5],
            "{mode} compound"
        );
        assert_eq!(
            ids(&current, pb.is_null("v").unwrap()).await,
            vec![3],
            "{mode} NULL"
        );
    }
}

#[tokio::test]
async fn epoch_seconds_evolution_reads_mixed_files_and_filters_for_every_table_mode() {
    for mode in ["append", "pk", "de"] {
        let mut schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("t", DataType::Timestamp(TimestampType::new(6).unwrap()))
            .column(
                "l",
                DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(6).unwrap()),
            )
            .column("s", DataType::BigInt(BigIntType::new()))
            .option("read.batch-size", "1");
        if mode == "pk" {
            schema = schema.primary_key(["id"]).option("bucket", "1");
        }
        if mode == "de" {
            schema = schema
                .option("data-evolution.enabled", "true")
                .option("row-tracking.enabled", "true");
        }
        let (io, old) = memory_table(
            &format!("memory:/epoch_seconds_{mode}"),
            TableSchema::new(0, &schema.build().unwrap()),
        );
        setup_dirs(&io, old.location()).await;
        persist_table_schema(&io, old.location(), old.schema()).await;
        let times = TimestampMicrosecondArray::from(vec![
            Some(42_000_001),
            Some(-1),
            None,
            Some(42_000_001),
        ]);
        let batch = RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef,
            ),
            ("t", Arc::new(times.clone()) as ArrayRef),
            ("l", Arc::new(times.with_timezone("UTC")) as ArrayRef),
            (
                "s",
                Arc::new(Int64Array::from(vec![
                    Some(42),
                    Some(-42),
                    None,
                    Some(i64::MAX),
                ])) as ArrayRef,
            ),
        ])
        .unwrap();
        write_batch(&old, &batch.slice(0, 3)).await;
        // A single-value old file must survive stats pruning in the new units.
        write_batch(&old, &batch.slice(3, 1)).await;
        let schema = old
            .schema()
            .apply_changes(vec![
                SchemaChange::update_column_type("t".into(), DataType::BigInt(BigIntType::new())),
                SchemaChange::update_column_type("l".into(), DataType::BigInt(BigIntType::new())),
                SchemaChange::update_column_type(
                    "s".into(),
                    DataType::Timestamp(TimestampType::new(6).unwrap()),
                ),
            ])
            .unwrap();
        persist_table_schema(&io, old.location(), &schema).await;
        let current = old.copy_with_resolved_schema(schema, "main").unwrap();
        write_batch(
            &current,
            &RecordBatch::try_from_iter([
                ("id", Arc::new(Int32Array::from(vec![5])) as ArrayRef),
                ("t", Arc::new(Int64Array::from(vec![42])) as ArrayRef),
                ("l", Arc::new(Int64Array::from(vec![42])) as ArrayRef),
                (
                    "s",
                    Arc::new(TimestampMicrosecondArray::from(vec![42_000_000])) as ArrayRef,
                ),
            ])
            .unwrap(),
        )
        .await;

        let builder = current.new_read_builder();
        let plan = builder.new_scan().plan().await.unwrap();
        let batches = builder
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let mut rows = Vec::new();
        for batch in batches {
            let raw = batch
                .columns()
                .iter()
                .skip(1)
                .map(|array| arrow_cast::cast(array, &arrow_schema::DataType::Int64).unwrap())
                .collect::<Vec<_>>();
            let values = raw
                .iter()
                .map(|array| array.as_any().downcast_ref::<Int64Array>().unwrap())
                .collect::<Vec<_>>();
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for index in 0..batch.num_rows() {
                let value = |column: usize| {
                    values[column]
                        .is_valid(index)
                        .then(|| values[column].value(index))
                };
                rows.push((ids.value(index), value(0), value(1), value(2)));
            }
        }
        rows.sort_unstable_by_key(|row| row.0);
        assert_eq!(
            rows,
            vec![
                (1, Some(42), Some(42), Some(42_000_000)),
                (2, Some(-1), Some(-1), Some(-42_000_000)),
                (3, None, None, None),
                (4, Some(42), Some(42), Some(-1_000_000)),
                (5, Some(42), Some(42), Some(42_000_000)),
            ],
            "{mode}"
        );
        let pb = PredicateBuilder::new(current.schema().fields());
        for name in ["t", "l"] {
            assert_eq!(
                ids(&current, pb.equal(name, Datum::Long(42)).unwrap()).await,
                vec![1, 4, 5],
                "{mode} {name} predicate-only field"
            );
            assert_eq!(
                ids(&current, pb.less_than(name, Datum::Long(0)).unwrap()).await,
                vec![2],
                "{mode} {name} pre-epoch"
            );
        }
        assert_eq!(
            ids(
                &current,
                pb.equal(
                    "s",
                    Datum::Timestamp {
                        millis: -1000,
                        nanos: 0
                    }
                )
                .unwrap()
            )
            .await,
            vec![4],
            "{mode} wrapped single-value file"
        );
        assert_eq!(
            ids(&current, pb.is_null("s").unwrap()).await,
            vec![3],
            "{mode} NULL"
        );
    }
}
