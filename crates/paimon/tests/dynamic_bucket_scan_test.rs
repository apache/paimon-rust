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

use arrow_array::{Int32Array, Int8Array, RecordBatch, StringArray};
use common::incremental_helpers::{
    make_partitioned_batch, memory_table, persist_table_schema, setup_dirs, write_batch,
};
use futures::TryStreamExt;
use paimon::spec::{
    DataField, DataType, Datum, IntType, PredicateBuilder, Schema, TableSchema, TinyIntType,
    VarCharType, VALUE_KIND_FIELD_ID, VALUE_KIND_FIELD_NAME,
};
use paimon::table::{IncrementalScanMode, ReadBuilder};

async fn events(
    builder: &ReadBuilder<'_>,
    splits: &[paimon::DataSplit],
) -> Vec<(String, i32, i32, i8)> {
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow(splits)
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut events: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            let pt = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let id = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let value = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let kind = batch
                .column(3)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap();
            (0..batch.num_rows())
                .map(|i| {
                    (
                        pt.value(i).to_string(),
                        id.value(i),
                        value.value(i),
                        kind.value(i),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect();
    events.sort_unstable();
    events
}

#[tokio::test]
async fn dynamic_and_cross_partition_scans_preserve_migration_events() {
    for cross_partition in [false, true] {
        for engine in ["deduplicate", "first-row"] {
            let schema = Schema::builder()
                .column("pt", DataType::VarChar(VarCharType::string_type()))
                .column("id", DataType::Int(IntType::new()))
                .column("value", DataType::Int(IntType::new()))
                .primary_key(if cross_partition {
                    vec!["id"]
                } else {
                    vec!["id", "pt"]
                })
                .partition_keys(["pt"])
                .option("bucket", "-1")
                .option("merge-engine", engine)
                .option("dynamic-bucket.target-row-num", "1")
                .option("source.split.target-size", "1b")
                .option("source.split.open-file-cost", "1b")
                .build()
                .unwrap();
            let path = format!("memory:/bucket_scan/{cross_partition}/{engine}");
            let (io, table) = memory_table(&path, TableSchema::new(0, &schema));
            setup_dirs(&io, &path).await;
            persist_table_schema(&io, &path, table.schema()).await;
            write_batch(
                &table,
                &make_partitioned_batch(vec!["a", "a"], vec![1, 2], vec![10, 20]),
            )
            .await;
            // Reopening the writer must restore the key-to-partition/bucket index.
            write_batch(
                &table,
                &make_partitioned_batch(vec!["b", "a"], vec![1, 3], vec![99, 30]),
            )
            .await;
            let mut expected = vec![
                ("a".into(), 1, 10, 0),
                ("a".into(), 2, 20, 0),
                ("a".into(), 3, 30, 0),
            ];
            if !(cross_partition && engine == "first-row") {
                expected.push(("b".into(), 1, 99, 0));
            }
            if cross_partition && engine == "deduplicate" {
                // Java DeleteExistingProcessor replaces the partition fields
                // of the incoming row and emits DELETE, not UPDATE_BEFORE.
                expected.push(("a".into(), 1, 99, 3));
            }
            expected.sort_unstable();
            for partition in [None, Some("a"), Some("b")] {
                let mut builder = table.new_read_builder();
                let mut fields = table.schema().fields().to_vec();
                fields.push(DataField::new(
                    VALUE_KIND_FIELD_ID,
                    VALUE_KIND_FIELD_NAME.to_string(),
                    DataType::TinyInt(TinyIntType::new()),
                ));
                builder.with_read_type(fields);
                if let Some(partition) = partition {
                    builder.with_filter(
                        PredicateBuilder::new(table.schema().fields())
                            .equal("pt", Datum::String(partition.into()))
                            .unwrap(),
                    );
                }
                let plan = builder
                    .new_incremental_scan(IncrementalScanMode::Delta, 0, 2)
                    .plan_combined_delta()
                    .await
                    .unwrap();
                assert!(plan.splits().iter().all(|s| s.is_streaming()));
                let expected: Vec<_> = expected
                    .iter()
                    .filter(|event| partition.is_none_or(|p| event.0 == p))
                    .cloned()
                    .collect();
                assert_eq!(
                    events(&builder, plan.splits()).await,
                    expected,
                    "cross_partition={cross_partition}, engine={engine}, partition={partition:?}"
                );
            }
            let builder = table.new_read_builder();
            let plan = builder
                .new_scan()
                .with_scan_all_files()
                .plan()
                .await
                .unwrap();
            assert!(plan.splits().iter().any(|s| s.bucket() > 0));
            let batches: Vec<RecordBatch> = builder
                .new_read()
                .unwrap()
                .to_arrow(plan.splits())
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            let mut actual: Vec<_> = batches
                .iter()
                .flat_map(|batch| {
                    let pt = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    let id = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    let value = batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    (0..batch.num_rows())
                        .map(|i| (pt.value(i).to_string(), id.value(i), value.value(i)))
                        .collect::<Vec<_>>()
                })
                .collect();
            actual.sort_unstable();
            let mut current: Vec<_> = expected
                .iter()
                .filter(|event| {
                    event.3 == 0
                        && !(cross_partition
                            && engine == "deduplicate"
                            && event.0 == "a"
                            && event.1 == 1)
                })
                .map(|event| (event.0.clone(), event.1, event.2))
                .collect();
            current.sort_unstable();
            assert_eq!(actual, current);
        }
    }
}
