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

//! FLOAT/DOUBLE filters treat a NaN with the sign bit set like any other NaN,
//! as Java's `Float.compare` / `Double.compare` do.

mod common;

use std::sync::Arc;

use arrow_array::{ArrayRef, Float32Array, Float64Array, Int32Array, RecordBatch};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs, write_batch};
use futures::TryStreamExt;
use paimon::spec::{
    DataType, Datum, DoubleType, FloatType, IntType, Predicate, PredicateBuilder, Schema,
    TableSchema,
};
use paimon::table::Table;

// What x86 produces for `0.0 / 0.0`.
const NEGATIVE_NAN_F32: u32 = 0xffc0_0000;
const NEGATIVE_NAN_F64: u64 = 0xfff8_0000_0000_0000;

async fn table() -> Table {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("f", DataType::Float(FloatType::new()))
        .column("d", DataType::Double(DoubleType::new()))
        .build()
        .unwrap();
    let path = "memory:/float_negative_nan_filter";
    let (io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    let f = vec![0.0, f32::from_bits(NEGATIVE_NAN_F32), -2.0, f32::NAN];
    let d = vec![0.0, f64::from_bits(NEGATIVE_NAN_F64), -2.0, f64::NAN];
    write_batch(
        &table,
        &RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int32Array::from(vec![0, 1, 2, 3])) as ArrayRef,
            ),
            ("f", Arc::new(Float32Array::from(f)) as ArrayRef),
            ("d", Arc::new(Float64Array::from(d)) as ArrayRef),
        ])
        .unwrap(),
    )
    .await;
    table
}

async fn read_ids(table: &Table, filter: Predicate) -> Vec<i32> {
    let mut read = table.new_read_builder();
    read.with_projection(&["id"]).unwrap();
    read.with_filter(filter);
    let plan = read.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = read
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut ids: Vec<i32> = batches
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
        .collect();
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn negative_nan_rows_filter_like_nan() {
    // Rows: 0 -> 0.0, 1 -> NaN with the sign bit, 2 -> -2.0, 3 -> NaN.
    let table = table().await;
    let pb = PredicateBuilder::new(table.schema().fields());
    for (column, value, nan, negative_nan) in [
        (
            "f",
            Datum::Float as fn(f32) -> Datum,
            Datum::Float(f32::NAN),
            Datum::Float(f32::from_bits(NEGATIVE_NAN_F32)),
        ),
        (
            "d",
            |v| Datum::Double(f64::from(v)),
            Datum::Double(f64::NAN),
            Datum::Double(f64::from_bits(NEGATIVE_NAN_F64)),
        ),
    ] {
        let cases = [
            (pb.less_than(column, value(-1.0)).unwrap(), vec![2]),
            (pb.greater_than(column, value(-1.0)).unwrap(), vec![0, 1, 3]),
            (
                pb.between(column, value(-3.0), value(-1.0)).unwrap(),
                vec![2],
            ),
            (pb.equal(column, nan.clone()).unwrap(), vec![1, 3]),
            (pb.not_equal(column, nan.clone()).unwrap(), vec![0, 2]),
            (pb.equal(column, negative_nan).unwrap(), vec![1, 3]),
            (
                pb.is_in(column, vec![nan, value(0.0)]).unwrap(),
                vec![0, 1, 3],
            ),
        ];
        for (predicate, expected) in cases {
            assert_eq!(
                read_ids(&table, predicate.clone()).await,
                expected,
                "{predicate}"
            );
        }
    }
}
