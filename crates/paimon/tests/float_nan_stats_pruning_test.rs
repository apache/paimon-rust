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

//! FLOAT/DOUBLE stats pruning must keep files whose NaN rows are left out of
//! min/max.

mod common;

use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, Int32Array, RecordBatch};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs, write_batch};
use futures::TryStreamExt;
use paimon::spec::{
    DataType, Datum, DoubleType, IntType, Predicate, PredicateBuilder, Schema, TableSchema,
};
use paimon::table::Table;

async fn table_with_files(files: &[&[(i32, f64)]]) -> Table {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("f", DataType::Double(DoubleType::new()))
        .build()
        .unwrap();
    let path = "memory:/float_nan_stats_pruning";
    let (io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    for rows in files {
        let ids: Vec<i32> = rows.iter().map(|row| row.0).collect();
        let values: Vec<f64> = rows.iter().map(|row| row.1).collect();
        write_batch(
            &table,
            &RecordBatch::try_from_iter([
                ("id", Arc::new(Int32Array::from(ids)) as ArrayRef),
                ("f", Arc::new(Float64Array::from(values)) as ArrayRef),
            ])
            .unwrap(),
        )
        .await;
    }
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
async fn range_filters_keep_nan_rows_left_out_of_file_stats() {
    // The first file reports `max == 0.0`, but Java orders its NaN row above
    // every number.
    let table = table_with_files(&[&[(0, 0.0), (1, f64::NAN)], &[(2, 2.0)]]).await;
    let pb = PredicateBuilder::new(table.schema().fields());
    let cases = [
        (
            pb.greater_than("f", Datum::Double(1.0)).unwrap(),
            vec![1, 2],
        ),
        (
            pb.greater_or_equal("f", Datum::Double(1.0)).unwrap(),
            vec![1, 2],
        ),
        (
            pb.not_between("f", Datum::Double(-1.0), Datum::Double(1.0))
                .unwrap(),
            vec![1, 2],
        ),
        (
            Predicate::Not(Box::new(pb.less_than("f", Datum::Double(1.0)).unwrap())),
            vec![1, 2],
        ),
        (pb.less_than("f", Datum::Double(1.0)).unwrap(), vec![0]),
    ];
    for (predicate, expected) in cases {
        assert_eq!(
            read_ids(&table, predicate.clone()).await,
            expected,
            "{predicate}"
        );
    }
}
