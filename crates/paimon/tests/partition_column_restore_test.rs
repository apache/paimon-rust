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

use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, RecordBatch};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs};
use futures::TryStreamExt;
use paimon::spec::{DataType, Datum, IntType, Predicate, PredicateBuilder, Schema, TableSchema};
use paimon::table::{DataEvolutionWriter, Table};

async fn table(evolution: bool) -> Table {
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("pt", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .partition_keys(["pt"])
        .option("bucket", if evolution { "-1" } else { "1" })
        .option(
            "row-tracking.enabled",
            if evolution { "true" } else { "false" },
        )
        .option(
            "data-evolution.enabled",
            if evolution { "true" } else { "false" },
        );
    if !evolution {
        schema = schema.option("bucket-key", "id");
    }
    let schema = schema.build().unwrap();
    let path = "memory:/partition-column-restore";
    let (io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    table
}

async fn write_without_partition(table: &Table) {
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer.with_write_type(vec!["value".into()]).unwrap();
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
        (
            "pt",
            Arc::new(Int32Array::from(vec![Some(7), None])) as ArrayRef,
        ),
        (
            "value",
            Arc::new(Int32Array::from(vec![42, 43])) as ArrayRef,
        ),
    ])
    .unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert!(messages
        .iter()
        .flat_map(|message| &message.new_files)
        .all(|file| { file.write_cols.as_ref() == Some(&vec!["value".to_owned()]) }));
    builder.new_commit().commit(messages).await.unwrap();
}

async fn rows(
    table: &Table,
    filter: Option<Predicate>,
    value_only: bool,
) -> Vec<(Option<i32>, i32)> {
    let mut read = table.new_read_builder();
    if let Some(filter) = filter {
        read.with_filter(filter);
    }
    if value_only {
        read.with_projection(&["value"]).unwrap();
    }
    let plan = read.new_scan().plan().await.unwrap();
    assert!(
        !plan.splits().is_empty(),
        "partition candidates must survive planning"
    );
    let batches: Vec<RecordBatch> = read
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut result = Vec::new();
    for batch in batches {
        let pt = batch
            .column_by_name("pt")
            .map(|column| column.as_any().downcast_ref::<Int32Array>().unwrap());
        let values = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            result.push((
                pt.and_then(|array| (!array.is_null(row)).then(|| array.value(row))),
                values.value(row),
            ));
        }
    }
    result.sort();
    result
}

fn partition_or_value(table: &Table) -> Predicate {
    let pb = PredicateBuilder::new(table.schema().fields());
    Predicate::or(vec![
        pb.equal("pt", Datum::Int(7)).unwrap(),
        pb.equal("value", Datum::Int(999)).unwrap(),
    ])
}

#[tokio::test]
async fn absent_partition_values_precede_raw_and_evolution_residuals() {
    for evolution in [false, true] {
        let table = table(evolution).await;
        write_without_partition(&table).await;
        assert_eq!(
            rows(&table, None, false).await,
            vec![(None, 43), (Some(7), 42)]
        );
        // Mixed data/partition predicates cannot be reduced to split pruning.
        assert_eq!(
            rows(&table, Some(partition_or_value(&table)), false).await,
            vec![(Some(7), 42)]
        );
        assert_eq!(
            rows(&table, Some(partition_or_value(&table)), true).await,
            vec![(None, 42)]
        );
        if evolution {
            // An omitted ordinary field still represents NULL, so its
            // statistics may prune it. Only partition metadata is special.
            let mut read = table.new_read_builder();
            read.with_filter(
                PredicateBuilder::new(table.schema().fields())
                    .equal("id", Datum::Int(1))
                    .unwrap(),
            );
            assert!(read.new_scan().plan().await.unwrap().splits().is_empty());
        }
    }
}

#[tokio::test]
async fn absent_partition_values_survive_evolution_column_merge() {
    let table = table(true).await;
    write_without_partition(&table).await;
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let first_row_id = plan
        .splits()
        .iter()
        .find(|split| {
            !split.partition().is_null_at(0) && split.partition().get_int(0).unwrap() == 7
        })
        .unwrap()
        .data_files()[0]
        .first_row_id
        .unwrap();
    let mut writer = DataEvolutionWriter::new(&table, vec!["value".into()]).unwrap();
    writer
        .add_matched_batch(
            RecordBatch::try_from_iter([
                (
                    "_ROW_ID",
                    Arc::new(Int64Array::from(vec![first_row_id])) as ArrayRef,
                ),
                ("value", Arc::new(Int32Array::from(vec![99])) as ArrayRef),
            ])
            .unwrap(),
        )
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    table
        .new_write_builder()
        .new_commit()
        .commit(messages)
        .await
        .unwrap();
    assert_eq!(
        rows(&table, None, false).await,
        vec![(None, 43), (Some(7), 99)]
    );
    assert_eq!(
        rows(&table, Some(partition_or_value(&table)), true).await,
        vec![(None, 99)]
    );
}
