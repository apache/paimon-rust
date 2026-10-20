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

use arrow_array::{Int32Array, RecordBatch};
use common::incremental_helpers::{
    make_batch, memory_table, persist_table_schema, setup_dirs, write_batch,
};
use futures::TryStreamExt;
use paimon::spec::{DataType, Datum, IntType, Predicate, PredicateBuilder, Schema, TableSchema};
use paimon::table::{DataSplit, DataSplitBuilder, ReadBuilder, RowRange};

async fn ids(builder: &ReadBuilder<'_>, splits: &[DataSplit]) -> Vec<i32> {
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow(splits)
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut result = batches
        .iter()
        .flat_map(|batch| {
            assert_eq!(batch.schema().fields().len(), 1);
            assert_eq!(batch.schema().field(0).name(), "id");
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    result.sort_unstable();
    result
}

#[tokio::test]
async fn a_reader_applies_its_row_id_filter_to_an_existing_plan() {
    for evolution in [false, true] {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .option("row-tracking.enabled", "true")
            .option("data-evolution.enabled", evolution.to_string())
            .option("read.batch-size", "2")
            .build()
            .unwrap();
        let (io, table) = memory_table("memory:/preplanned_row_id", TableSchema::new(0, &schema));
        setup_dirs(&io, table.location()).await;
        persist_table_schema(&io, table.location(), table.schema()).await;
        write_batch(&table, &make_batch(vec![10, 11, 12], vec![100, 110, 120])).await;
        write_batch(&table, &make_batch(vec![13, 14, 15], vec![130, 140, 150])).await;
        let plan = table.new_read_builder().new_scan().plan().await.unwrap();
        let fields = table.schema().fields_with_read_metadata();
        let pb = PredicateBuilder::new(&fields);
        let cases = [
            (pb.equal("_ROW_ID", Datum::Long(1)).unwrap(), vec![11]),
            (
                pb.equal("_SEQUENCE_NUMBER", Datum::Long(2)).unwrap(),
                vec![13, 14, 15],
            ),
            (
                Predicate::and(vec![
                    pb.greater_or_equal("_ROW_ID", Datum::Long(1)).unwrap(),
                    pb.less_than("_ROW_ID", Datum::Long(5)).unwrap(),
                    pb.greater_than("value", Datum::Int(120)).unwrap(),
                ]),
                vec![13, 14],
            ),
            (
                Predicate::or(vec![
                    pb.equal("_ROW_ID", Datum::Long(1)).unwrap(),
                    pb.equal("value", Datum::Int(140)).unwrap(),
                ]),
                vec![11, 14],
            ),
            (
                Predicate::or(vec![
                    pb.equal("_SEQUENCE_NUMBER", Datum::Long(1)).unwrap(),
                    pb.equal("_ROW_ID", Datum::Long(4)).unwrap(),
                ]),
                vec![10, 11, 12, 14],
            ),
            (
                Predicate::and(vec![
                    pb.equal("_ROW_ID", Datum::Long(1)).unwrap(),
                    pb.equal("_ROW_ID", Datum::Long(2)).unwrap(),
                ]),
                vec![],
            ),
        ];
        for (predicate, expected) in cases {
            let mut builder = table.new_read_builder();
            builder
                .with_projection(&["id"])
                .unwrap()
                .with_filter(predicate.clone());
            assert_eq!(
                ids(&builder, plan.splits()).await,
                expected,
                "evolution={evolution}, {predicate:?}"
            );
            let mut count = builder.clone();
            count.with_projection(&[]).unwrap();
            let counted: Vec<_> = count
                .new_read()
                .unwrap()
                .to_arrow(plan.splits())
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            assert!(counted.iter().all(|batch| batch.num_columns() == 0));
            assert_eq!(
                counted.iter().map(RecordBatch::num_rows).sum::<usize>(),
                expected.len()
            );
            if evolution {
                builder.with_limit(1);
                let limited = ids(&builder, plan.splits()).await;
                assert_eq!(limited.len(), expected.len().min(1));
                assert!(limited.iter().all(|id| expected.contains(id)));
            }
        }

        let mut selected = table.new_read_builder();
        selected.with_row_ranges(vec![RowRange::new(1, 4)]);
        let selected = selected.new_scan().plan().await.unwrap();
        let mut reader = table.new_read_builder();
        reader
            .with_projection(&["id"])
            .unwrap()
            .with_filter(Predicate::or(vec![
                pb.equal("_ROW_ID", Datum::Long(0)).unwrap(),
                pb.equal("_ROW_ID", Datum::Long(2)).unwrap(),
                pb.equal("_ROW_ID", Datum::Long(4)).unwrap(),
                pb.equal("_ROW_ID", Datum::Long(5)).unwrap(),
            ]));
        assert_eq!(ids(&reader, selected.splits()).await, vec![12, 14]);
        // A new filter replaces its old derived range as well as the residual.
        reader.with_filter(pb.equal("id", Datum::Int(13)).unwrap());
        assert_eq!(ids(&reader, plan.splits()).await, vec![13]);
    }
}

#[tokio::test]
async fn physical_slices_use_the_readers_coordinate_system_before_filtering() {
    for tracked in [false, true] {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("value", DataType::Int(IntType::new()))
            .option("row-tracking.enabled", tracked.to_string())
            .option("read.batch-size", "2")
            .build()
            .unwrap();
        let (io, table) = memory_table("memory:/physical_slices", TableSchema::new(0, &schema));
        setup_dirs(&io, table.location()).await;
        persist_table_schema(&io, table.location(), table.schema()).await;
        let mut seen = std::collections::HashSet::new();
        let mut files = Vec::new();
        let mut bucket_path = String::new();
        for start in [10, 20, 30] {
            let values = (start..start + 4).collect::<Vec<_>>();
            write_batch(&table, &make_batch(values.clone(), values)).await;
            let plan = table.new_read_builder().new_scan().plan().await.unwrap();
            for split in plan.splits() {
                bucket_path = split.bucket_path().to_string();
                for file in split.data_files() {
                    if seen.insert(file.file_name.clone()) {
                        files.push(file.clone());
                    }
                }
            }
        }
        assert_eq!(files.len(), 3);
        // Drop the middle file and reverse the others. Stable row IDs retain
        // their gaps; raw offsets restart at zero in this supplied split.
        let selected = vec![files[2].clone(), files[0].clone()];
        let mut offset = 0;
        let ranges = selected
            .iter()
            .zip([(2, 4), (1, 3)])
            .map(|(file, (start, end))| {
                let base = if tracked {
                    file.first_row_id.unwrap()
                } else {
                    offset
                };
                offset += file.row_count;
                RowRange::new(base + start, base + end - 1)
            })
            .collect();
        let split = DataSplitBuilder::new()
            .with_snapshot(3)
            .with_partition(paimon::spec::BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(bucket_path)
            .with_data_files(selected)
            .with_raw_convertible(true)
            .with_row_ranges(ranges)
            .build()
            .unwrap();
        let mut reader = table.new_read_builder();
        reader.with_projection(&["id"]).unwrap();
        assert_eq!(
            ids(&reader, std::slice::from_ref(&split)).await,
            vec![11, 12, 32, 33],
            "tracked={tracked}"
        );
        reader.with_filter(
            PredicateBuilder::new(table.schema().fields())
                .greater_or_equal("id", Datum::Int(12))
                .unwrap(),
        );
        assert_eq!(
            ids(&reader, std::slice::from_ref(&split)).await,
            vec![12, 32, 33],
            "tracked={tracked}"
        );
        reader.with_projection(&[]).unwrap();
        let counted = reader
            .new_read()
            .unwrap()
            .to_arrow(&[split])
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(counted.iter().all(|batch| batch.num_columns() == 0));
        assert_eq!(counted.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    }
}
