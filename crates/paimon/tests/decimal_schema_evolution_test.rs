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

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int32Array, RecordBatch,
    StringArray,
};
use futures::TryStreamExt;
use paimon::spec::{
    DataType, Datum, DecimalType, DoubleType, FloatType, IntType, Predicate, PredicateBuilder,
    Schema, SchemaChange, TableSchema, VarCharType,
};
use paimon::table::Table;

use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs, write_batch};

async fn read(table: &Table, predicate: Option<Predicate>, only_id: bool) -> Vec<RecordBatch> {
    let mut builder = table.new_read_builder();
    if let Some(predicate) = predicate {
        builder.with_filter(predicate);
    }
    if only_id {
        builder.with_projection(&["id"]).unwrap();
    }
    let plan = builder.new_scan().plan().await.unwrap();
    builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

async fn ids(table: &Table, predicate: Predicate) -> Vec<i32> {
    let mut ids = read(table, Some(predicate), true)
        .await
        .into_iter()
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
    ids.sort_unstable();
    ids
}

fn decimal(precision: u32, scale: u32) -> DataType {
    DataType::Decimal(DecimalType::new(precision, scale).unwrap())
}

#[tokio::test]
async fn decimal_casts_read_mixed_files_and_filter_in_all_table_modes() {
    for mode in ["append", "pk", "de"] {
        let mut schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("d", DataType::Double(DoubleType::new()))
            .column("f", DataType::Float(FloatType::new()))
            .column("s", DataType::VarChar(VarCharType::string_type()))
            .column("wide", DataType::Float(FloatType::new()))
            .option("read.batch-size", "1");
        if mode == "pk" {
            schema = schema.primary_key(["id"]).option("bucket", "1");
        } else if mode == "de" {
            schema = schema
                .option("data-evolution.enabled", "true")
                .option("row-tracking.enabled", "true");
        }
        let (io, old) = memory_table(
            &format!("memory:/decimal_evolution_{mode}"),
            TableSchema::new(0, &schema.build().unwrap()),
        );
        setup_dirs(&io, old.location()).await;
        persist_table_schema(&io, old.location(), old.schema()).await;
        let batch = RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int32Array::from(vec![0, 1, 2, 3])) as ArrayRef,
            ),
            (
                "d",
                Arc::new(Float64Array::from(vec![
                    Some(1.005),
                    Some(-1.005),
                    Some(999.995),
                    None,
                ])) as ArrayRef,
            ),
            (
                "f",
                Arc::new(Float32Array::from(vec![
                    Some(1.005),
                    Some(-1.005),
                    Some(999.995),
                    None,
                ])) as ArrayRef,
            ),
            (
                "s",
                Arc::new(StringArray::from(vec![
                    Some("1e-2"),
                    Some("+.005"),
                    Some("1e100"),
                    None,
                ])) as ArrayRef,
            ),
            (
                "wide",
                Arc::new(Float32Array::from(vec![
                    Some(1.2345679),
                    Some(-1.2345679),
                    Some(1e30),
                    None,
                ])) as ArrayRef,
            ),
        ])
        .unwrap();
        write_batch(&old, &batch).await;
        // Single-value stats must not prune a match after decimal rounding.
        let single = RecordBatch::try_new(
            batch.schema(),
            vec![
                Arc::new(Int32Array::from(vec![4])) as ArrayRef,
                batch.column(1).slice(0, 1),
                batch.column(2).slice(0, 1),
                batch.column(3).slice(0, 1),
                batch.column(4).slice(0, 1),
            ],
        )
        .unwrap();
        write_batch(&old, &single).await;
        let schema = old
            .schema()
            .apply_changes(vec![
                SchemaChange::update_column_type("d".into(), decimal(5, 2)),
                SchemaChange::update_column_type("f".into(), decimal(5, 2)),
                SchemaChange::update_column_type("s".into(), decimal(5, 2)),
                SchemaChange::update_column_type("wide".into(), decimal(38, 20)),
            ])
            .unwrap();
        persist_table_schema(&io, old.location(), &schema).await;
        let current = old.copy_with_resolved_schema(schema, "main").unwrap();
        let new = RecordBatch::try_from_iter([
            ("id", Arc::new(Int32Array::from(vec![5])) as ArrayRef),
            (
                "d",
                Arc::new(
                    Decimal128Array::from(vec![101])
                        .with_precision_and_scale(5, 2)
                        .unwrap(),
                ) as ArrayRef,
            ),
            (
                "f",
                Arc::new(
                    Decimal128Array::from(vec![100])
                        .with_precision_and_scale(5, 2)
                        .unwrap(),
                ) as ArrayRef,
            ),
            (
                "s",
                Arc::new(
                    Decimal128Array::from(vec![1])
                        .with_precision_and_scale(5, 2)
                        .unwrap(),
                ) as ArrayRef,
            ),
            (
                "wide",
                Arc::new(
                    Decimal128Array::from(vec![123_456_788_063_049_320_000])
                        .with_precision_and_scale(38, 20)
                        .unwrap(),
                ) as ArrayRef,
            ),
        ])
        .unwrap();
        write_batch(&current, &new).await;
        let mut values = BTreeMap::new();
        for batch in read(&current, None, false).await {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let decimals = batch.columns()[1..]
                .iter()
                .map(|c| c.as_any().downcast_ref::<Decimal128Array>().unwrap())
                .collect::<Vec<_>>();
            for i in 0..batch.num_rows() {
                values.insert(
                    keys.value(i),
                    decimals
                        .iter()
                        .map(|c| (!c.is_null(i)).then(|| c.value(i)))
                        .collect::<Vec<_>>(),
                );
            }
        }
        let first = vec![
            Some(101),
            Some(100),
            Some(1),
            Some(123_456_788_063_049_320_000),
        ];
        assert_eq!(
            values,
            BTreeMap::from([
                (0, first.clone()),
                (
                    1,
                    vec![
                        Some(-101),
                        Some(-100),
                        Some(1),
                        Some(-123_456_788_063_049_320_000)
                    ]
                ),
                (2, vec![None, Some(99_999), None, None]),
                (3, vec![None; 4]),
                (4, first.clone()),
                (5, first),
            ]),
            "{mode}"
        );
        let pb = PredicateBuilder::new(current.schema().fields());
        for (column, unscaled, precision, scale) in [
            ("d", 101, 5, 2),
            ("f", 100, 5, 2),
            ("wide", 123_456_788_063_049_320_000, 38, 20),
        ] {
            assert_eq!(
                ids(
                    &current,
                    pb.equal(
                        column,
                        Datum::Decimal {
                            unscaled,
                            precision,
                            scale
                        }
                    )
                    .unwrap()
                )
                .await,
                vec![0, 4, 5],
                "{mode} {column}"
            );
        }
        assert_eq!(
            ids(
                &current,
                pb.equal(
                    "s",
                    Datum::Decimal {
                        unscaled: 1,
                        precision: 5,
                        scale: 2
                    }
                )
                .unwrap()
            )
            .await,
            vec![0, 1, 4, 5],
            "{mode} exponent"
        );
        assert_eq!(
            ids(&current, pb.is_null("d").unwrap()).await,
            vec![2, 3],
            "{mode} overflow"
        );
    }
}
