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

use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use paimon::spec::{DataType, Datum, IntType, PredicateBuilder, Schema, TableSchema, VarCharType};
use paimon::table::{CommitMessage, Table, TableCommit};

use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs};

type Row = (i32, Option<i32>, Option<String>);

async fn table(engine: &str, bucket: &str, input_changelog: bool) -> Table {
    let mut builder = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .column("text", DataType::VarChar(VarCharType::string_type()))
        .primary_key(["id"])
        .option("bucket", bucket)
        .option("merge-engine", engine)
        .option("deletion-vectors.enabled", "true")
        .option("source.split.target-size", "1b")
        .option("source.split.open-file-cost", "1b");
    if engine == "aggregation" {
        builder = builder
            .option("fields.value.aggregate-function", "sum")
            .option("fields.text.aggregate-function", "last_non_null_value");
    }
    if input_changelog {
        builder = builder.option("changelog-producer", "input");
    }
    let (io, table) = memory_table(
        "memory:/dv_merge_engine",
        TableSchema::new(0, &builder.build().unwrap()),
    );
    setup_dirs(&io, table.location()).await;
    persist_table_schema(&io, table.location(), table.schema()).await;
    table
}

fn batch(ids: Vec<i32>, values: Vec<Option<i32>>, texts: Vec<Option<&str>>) -> RecordBatch {
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(ids)) as ArrayRef),
        ("value", Arc::new(Int32Array::from(values)) as ArrayRef),
        ("text", Arc::new(StringArray::from(texts)) as ArrayRef),
    ])
    .unwrap()
}

async fn write(table: &Table, batches: Vec<RecordBatch>, identifier: i64) -> Vec<CommitMessage> {
    let builder = table
        .new_write_builder()
        .with_commit_user("dv-test")
        .unwrap();
    let mut writer = builder.new_write().unwrap();
    for batch in batches {
        writer.write_arrow_batch(&batch).await.unwrap();
    }
    let messages = writer.prepare_commit().await.unwrap();
    TableCommit::new(table.clone(), "dv-test".into())
        .commit_with_identifier(messages.clone(), identifier)
        .await
        .unwrap();
    writer.close().await;
    messages
}

async fn rows(table: &Table) -> Vec<Row> {
    let builder = table.new_read_builder();
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
        let texts = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                ids.value(row),
                values.is_valid(row).then(|| values.value(row)),
                texts.is_valid(row).then(|| texts.value(row).to_string()),
            ));
        }
    }
    rows.sort();
    rows
}

#[tokio::test]
async fn dv_merge_engines_fold_inputs_and_restore_across_commits() {
    for engine in ["partial-update", "aggregation"] {
        for bucket in ["1", "-1"] {
            for input_changelog in [false, true] {
                let table = table(engine, bucket, input_changelog).await;
                let first = write(
                    &table,
                    vec![
                        batch(
                            vec![1, 2],
                            vec![Some(10), Some(20)],
                            vec![Some("first"), Some("second")],
                        ),
                        batch(vec![1], vec![None], vec![Some("patched")]),
                    ],
                    1,
                )
                .await;
                assert_eq!(
                    first
                        .iter()
                        .flat_map(|m| &m.new_files)
                        .map(|f| f.row_count)
                        .sum::<i64>(),
                    2
                );
                assert!(first
                    .iter()
                    .flat_map(|m| &m.new_files)
                    .all(|f| f.level == 0));
                assert_eq!(
                    first
                        .iter()
                        .flat_map(|m| &m.new_changelog_files)
                        .map(|f| f.row_count)
                        .sum::<i64>(),
                    if input_changelog { 3 } else { 0 }
                );
                // Java does not expose uncompacted level-0 files by default in DV mode.
                assert!(rows(&table).await.is_empty());
                let visible = table.copy_with_options(std::collections::HashMap::from([(
                    "deletion-vectors.merge-on-read".into(),
                    "true".into(),
                )]));
                assert_eq!(
                    rows(&visible).await,
                    vec![
                        (1, Some(10), Some("patched".into())),
                        (2, Some(20), Some("second".into())),
                    ]
                );
                write(
                    &table,
                    vec![batch(
                        vec![1, 3],
                        vec![Some(3), Some(30)],
                        vec![None, Some("third")],
                    )],
                    2,
                )
                .await;
                assert_eq!(
                    rows(&visible).await,
                    vec![
                        (
                            1,
                            Some(if engine == "aggregation" { 13 } else { 3 }),
                            Some("patched".into())
                        ),
                        (2, Some(20), Some("second".into())),
                        (3, Some(30), Some("third".into())),
                    ]
                );
                assert!(rows(&table).await.is_empty());
                assert_eq!(
                    table
                        .snapshot_manager()
                        .get_latest_snapshot()
                        .await
                        .unwrap()
                        .unwrap()
                        .id(),
                    2
                );
            }
        }
    }
}

#[tokio::test]
async fn dv_merge_engine_filters_are_applied_after_merging() {
    for engine in ["partial-update", "aggregation"] {
        let table = table(engine, "1", false).await;
        write(
            &table,
            vec![batch(vec![1], vec![Some(5)], vec![Some("old")])],
            1,
        )
        .await;
        write(&table, vec![batch(vec![1], vec![Some(6)], vec![None])], 2).await;
        let visible = table.copy_with_options(std::collections::HashMap::from([(
            "deletion-vectors.merge-on-read".into(),
            "true".into(),
        )]));
        let predicates = PredicateBuilder::new(visible.schema().fields());
        for (predicate, expected) in [
            (
                predicates.greater_than("value", Datum::Int(10)).unwrap(),
                if engine == "aggregation" { 1 } else { 0 },
            ),
            (
                predicates
                    .equal("text", Datum::String("old".into()))
                    .unwrap(),
                1,
            ),
        ] {
            let mut builder = visible.new_read_builder();
            builder
                .with_projection(&["id"])
                .unwrap()
                .with_filter(predicate)
                .with_limit(1);
            let plan = builder.new_scan().plan().await.unwrap();
            let batches = builder
                .new_read()
                .unwrap()
                .to_arrow(plan.splits())
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(
                batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                expected
            );
            for batch in batches {
                assert_eq!(batch.schema().fields().len(), 1);
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                assert!(ids.values().iter().all(|id| *id == 1));
            }
        }
    }
}

#[tokio::test]
async fn dv_merge_engines_keep_inputs_across_buffer_flushes() {
    for engine in ["partial-update", "aggregation"] {
        let table =
            table(engine, "1", true)
                .await
                .copy_with_options(std::collections::HashMap::from([
                    ("write.parquet-buffer-size".into(), "1b".into()),
                    ("deletion-vectors.merge-on-read".into(), "true".into()),
                ]));
        let messages = write(
            &table,
            vec![
                batch(vec![1], vec![Some(1)], vec![Some("first")]),
                batch(vec![1], vec![Some(2)], vec![None]),
                batch(vec![1], vec![Some(3)], vec![Some("last")]),
            ],
            1,
        )
        .await;
        assert_eq!(messages.iter().flat_map(|m| &m.new_files).count(), 3);
        assert_eq!(
            messages
                .iter()
                .flat_map(|m| &m.new_changelog_files)
                .map(|f| f.row_count)
                .sum::<i64>(),
            3
        );
        assert_eq!(
            rows(&table).await,
            vec![(
                1,
                Some(if engine == "aggregation" { 6 } else { 3 }),
                Some("last".into())
            )]
        );
    }
}

#[tokio::test]
async fn dv_postpone_fixed_bucket_writers_merge_and_overwrite() {
    for engine in ["deduplicate", "partial-update", "aggregation"] {
        let table =
            table(engine, "-2", true)
                .await
                .copy_with_options(std::collections::HashMap::from([(
                    "postpone.default-bucket-num".into(),
                    "3".into(),
                )]));
        for (identifier, inputs, expected) in [
            (
                1,
                vec![
                    batch(vec![1], vec![Some(10)], vec![Some("first")]),
                    batch(vec![1], vec![Some(20)], vec![None]),
                ],
                if engine == "aggregation" { 30 } else { 20 },
            ),
            (
                2,
                vec![batch(vec![1], vec![Some(30)], vec![Some("after")])],
                if engine == "aggregation" { 60 } else { 30 },
            ),
        ] {
            let builder = table
                .new_postpone_fixed_bucket_write_builder()
                .unwrap()
                .with_commit_user("fixed-dv")
                .unwrap();
            let mut writer = builder.new_write().unwrap();
            for data in inputs {
                writer.write_arrow_batch(&data).await.unwrap();
            }
            let messages = writer.prepare_commit().await.unwrap();
            assert!(messages
                .iter()
                .all(|m| m.bucket >= 0 && m.bucket < 3 && m.total_buckets == Some(3)));
            assert!(messages
                .iter()
                .flat_map(|m| &m.new_files)
                .all(|f| f.level == 0 && f.row_count == 1));
            builder
                .new_commit()
                .commit_with_identifier(messages, identifier)
                .await
                .unwrap();
            writer.close().await;
            assert!(rows(&table).await.is_empty());
            let visible = table.copy_with_options(std::collections::HashMap::from([(
                "deletion-vectors.merge-on-read".into(),
                "true".into(),
            )]));
            let text = if identifier == 2 {
                Some("after".into())
            } else if engine == "deduplicate" {
                None
            } else {
                Some("first".into())
            };
            assert_eq!(rows(&visible).await, vec![(1, Some(expected), text)]);
        }
        let replacement = table.copy_with_options(std::collections::HashMap::from([(
            "postpone.default-bucket-num".into(),
            "5".into(),
        )]));
        let builder = replacement
            .new_postpone_fixed_bucket_write_builder()
            .unwrap()
            .with_commit_user("fixed-dv-overwrite")
            .unwrap()
            .with_overwrite();
        let mut writer = builder.new_write().unwrap();
        writer
            .write_arrow_batch(&batch(vec![1], vec![Some(100)], vec![None]))
            .await
            .unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        assert!(messages
            .iter()
            .all(|m| m.total_buckets == Some(5) && m.new_changelog_files.is_empty()));
        builder.new_commit().commit(messages).await.unwrap();
        writer.close().await;
        let visible = table.copy_with_options(std::collections::HashMap::from([(
            "deletion-vectors.merge-on-read".into(),
            "true".into(),
        )]));
        assert_eq!(rows(&visible).await, vec![(1, Some(100), None)]);
    }
}
