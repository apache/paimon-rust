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

use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow_array::{Int32Array, StringArray};
use arrow_schema::{DataType as ArrowType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;
use opendal::{services::MemoryConfig, Operator};

use crate::catalog::Identifier;
use crate::file_index::evaluator::evaluate_file_index;
use crate::file_index::file_index_result::FileIndexResult;
use crate::io::{FileIO, FileIOBuilder, FileIOProvider};
use crate::spec::{
    BooleanType, DataFileMeta, Datum, IntType, Predicate, PredicateBuilder, Schema, TableSchema,
};
use crate::table::data_evolution_writer::DataEvolutionPartialWriter;
use crate::table::{DataSplitBuilder, SchemaManager, Table};

fn schema(options: &[(&str, &str)]) -> Schema {
    let mut builder = Schema::builder()
        .column("id", crate::spec::DataType::Int(IntType::new()))
        .column("value", crate::spec::DataType::Int(IntType::new()));
    for (key, value) in options {
        builder = builder.option(*key, *value);
    }
    builder.build().unwrap()
}

async fn table(io: FileIO, schema: Schema) -> Table {
    let path = format!("memory:/append-index-test-{}", uuid::Uuid::new_v4());
    let schema = TableSchema::new(0, &schema);
    for dir in ["schema", "snapshot", "manifest"] {
        io.mkdirs(&format!("{path}/{dir}")).await.unwrap();
    }
    io.new_output(&format!("{path}/schema/schema-0"))
        .unwrap()
        .write(Bytes::from(serde_json::to_vec(&schema).unwrap()))
        .await
        .unwrap();
    Table::new(
        io,
        Identifier::new("default", "indexed_append"),
        path,
        schema,
        None,
    )
}

fn memory_io() -> FileIO {
    FileIOBuilder::new("memory").build().unwrap()
}

fn batch(ids: Vec<Option<i32>>, values: Vec<Option<i32>>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", ArrowType::Int32, true),
            Field::new("value", ArrowType::Int32, true),
        ])),
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(Int32Array::from(values)),
        ],
    )
    .unwrap()
}

fn rows(batches: &[RecordBatch]) -> Vec<(Option<i32>, Option<i32>)> {
    let mut result: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
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
            ids.iter().zip(values.iter())
        })
        .collect();
    result.sort_unstable();
    result
}

async fn query(
    table: &Table,
    enabled: bool,
    predicate: Option<Predicate>,
) -> Vec<(Option<i32>, Option<i32>)> {
    let table = table.copy_with_options(HashMap::from([(
        "file-index.read.enabled".to_string(),
        enabled.to_string(),
    )]));
    let mut builder = table.new_read_builder();
    if let Some(predicate) = predicate {
        builder.with_filter(predicate);
    }
    let plan = builder.new_scan().plan().await.unwrap();
    let batches: Vec<_> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    rows(&batches)
}

async fn evaluate(
    table: &Table,
    bucket_path: &str,
    file: &DataFileMeta,
    predicate: Predicate,
) -> FileIndexResult {
    evaluate_file_index(
        table.file_io(),
        bucket_path,
        file,
        table.schema().fields(),
        table.schema().fields(),
        &[predicate],
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn test_file_index_append_commit_reload_and_rolling() {
    for identifier in [
        "bitmap",
        "bloom-filter",
        "both",
        "range-bitmap",
        "bsi",
        "all",
    ] {
        for rolling in [false, true] {
            for threshold in ["0 B", "1 MB"] {
                let mut options = vec![
                    ("target-file-size", if rolling { "1 B" } else { "128 MB" }),
                    ("file-index.in-manifest-threshold", threshold),
                    ("file-index.read.enabled", "false"),
                ];
                if matches!(identifier, "bitmap" | "both" | "all") {
                    options.push(("file-index.bitmap.columns", " id, value, id "));
                }
                if matches!(identifier, "bloom-filter" | "both" | "all") {
                    options.extend([
                        ("file-index.bloom-filter.columns", "id"),
                        ("file-index.bloom-filter.id.items", "10"),
                    ]);
                }
                if matches!(identifier, "range-bitmap" | "all") {
                    options.push(("file-index.range-bitmap.columns", "id, value"));
                    options.push(("file-index.range-bitmap.id.chunk-size", "0b"));
                }
                if matches!(identifier, "bsi" | "all") {
                    options.push(("file-index.bsi.columns", "id, value"));
                }
                let table = table(memory_io(), schema(&options)).await;
                let builder = table.new_write_builder();
                let mut writer = builder.new_write().unwrap();
                writer
                    .write_arrow_batch(&batch(vec![], vec![]))
                    .await
                    .unwrap();
                assert!(writer.prepare_commit().await.unwrap().is_empty());
                let sliced = batch(
                    vec![Some(99), Some(1), None, Some(99)],
                    vec![Some(99), Some(10), Some(20), Some(99)],
                )
                .slice(1, 2);
                writer.write_arrow_batch(&sliced).await.unwrap();
                writer
                    .write_arrow_batch(&batch(vec![Some(3)], vec![None]))
                    .await
                    .unwrap();
                let messages = writer.prepare_commit().await.unwrap();
                let expected_files = if rolling { 2 } else { 1 };
                let files: Vec<_> = messages.iter().flat_map(|m| &m.new_files).collect();
                assert_eq!(files.len(), expected_files);
                assert_eq!(files.iter().map(|f| f.row_count).sum::<i64>(), 3);
                for file in files {
                    assert_eq!(file.embedded_index.is_some(), threshold != "0 B");
                    assert_eq!(file.extra_files.len(), usize::from(threshold == "0 B"));
                }
                builder.new_commit().commit(messages).await.unwrap();
                let persisted =
                    SchemaManager::new(table.file_io().clone(), table.location().to_string())
                        .latest()
                        .await
                        .unwrap()
                        .unwrap();
                let reloaded = Table::new(
                    table.file_io().clone(),
                    Identifier::new("default", "reloaded"),
                    table.location().to_string(),
                    persisted.as_ref().clone(),
                    None,
                );
                let read_builder = reloaded.new_read_builder();
                let plan = read_builder.new_scan().plan().await.unwrap();
                let predicates = PredicateBuilder::new(reloaded.schema().fields());
                for split in plan.splits() {
                    for file in split.data_files() {
                        assert_eq!(file.embedded_index.is_some(), threshold != "0 B");
                        if identifier != "bloom-filter" {
                            let expected = FileIndexResult::Selection(
                                if file.row_count == 1 { vec![] } else { vec![1] }
                                    .into_iter()
                                    .collect(),
                            );
                            assert_eq!(
                                evaluate(
                                    &reloaded,
                                    split.bucket_path(),
                                    file,
                                    predicates.is_null("id").unwrap()
                                )
                                .await,
                                expected
                            );
                            let expected = FileIndexResult::Selection(
                                if rolling && file.row_count == 2 {
                                    vec![]
                                } else {
                                    vec![if rolling { 0 } else { 2 }]
                                }
                                .into_iter()
                                .collect(),
                            );
                            assert_eq!(
                                evaluate(
                                    &reloaded,
                                    split.bucket_path(),
                                    file,
                                    predicates.is_null("value").unwrap()
                                )
                                .await,
                                expected
                            );
                        }
                    }
                }
                for enabled in [false, true] {
                    assert_eq!(
                        query(&reloaded, enabled, None).await,
                        vec![(None, Some(20)), (Some(1), Some(10)), (Some(3), None)]
                    );
                    assert_eq!(
                        query(
                            &reloaded,
                            enabled,
                            Some(predicates.equal("id", Datum::Int(3)).unwrap())
                        )
                        .await,
                        vec![(Some(3), None)]
                    );
                    assert_eq!(
                        query(&reloaded, enabled, Some(predicates.is_null("id").unwrap())).await,
                        vec![(None, Some(20))]
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn test_file_index_unconfigured_and_read_flag_only() {
    for options in [vec![], vec![("file-index.read.enabled", "true")]] {
        let table = table(memory_io(), schema(&options)).await;
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        writer
            .write_arrow_batch(&batch(vec![Some(1)], vec![None]))
            .await
            .unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        let file = &messages[0].new_files[0];
        assert!(file.embedded_index.is_none());
        assert!(file.extra_files.is_empty());
        builder.new_commit().commit(messages).await.unwrap();
        assert_eq!(query(&table, true, None).await, vec![(Some(1), None)]);
    }
}

#[tokio::test]
async fn test_file_index_skips_unsupported_identifier_groups() {
    for supported in [
        vec![],
        vec!["bitmap"],
        vec!["bloom-filter"],
        vec!["bitmap", "bloom-filter"],
    ] {
        let mut options = vec![
            ("file-index.future-index.columns", "missing[nested]"),
            (
                "file-index.future-index.missing[nested].version",
                "upstream-specific",
            ),
            ("file-index.unknown.columns", ""),
            ("file-index.in-manifest-threshold", "1 MB"),
        ];
        if supported.contains(&"bitmap") {
            options.push(("file-index.bitmap.columns", "id"));
        }
        if supported.contains(&"bloom-filter") {
            options.push(("file-index.bloom-filter.columns", "id"));
            options.push(("file-index.bloom-filter.id.items", "10"));
        }
        let table = table(memory_io(), schema(&options)).await;
        let config =
            FileIndexOptions::parse(table.schema().options(), table.schema().fields()).unwrap();
        if supported.is_empty() {
            assert!(config.is_none());
        } else {
            let config = config.unwrap();
            assert_eq!(config.columns.len(), 1);
            assert_eq!(config.columns[0].field.name(), "id");
            assert_eq!(
                config.columns[0]
                    .indexes
                    .keys()
                    .map(String::as_str)
                    .collect::<Vec<_>>(),
                supported
            );
        }
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        let data = batch(vec![Some(1), Some(3)], vec![None, Some(30)]);
        writer.write_arrow_batch(&data).await.unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        let file = &messages[0].new_files[0];
        assert_eq!(file.embedded_index.is_some(), !supported.is_empty());
        assert!(file.extra_files.is_empty());
        builder.new_commit().commit(messages).await.unwrap();
        assert_eq!(query(&table, true, None).await, rows(&[data]));
        let predicate = PredicateBuilder::new(table.schema().fields())
            .equal("id", Datum::Int(3))
            .unwrap();
        assert_eq!(
            query(&table, true, Some(predicate)).await,
            vec![(Some(3), Some(30))]
        );
    }
}

#[test]
fn test_file_index_range_bitmap_enables_generation() {
    assert!(FileIndexerFactory::is_supported("range-bitmap"));
    let schema = schema(&[("file-index.range-bitmap.columns", "id")]);
    assert!(FileIndexOptions::parse(schema.options(), schema.fields())
        .unwrap()
        .is_some());
}

#[test]
fn test_file_index_skips_unsupported_options_without_columns() {
    let schema = schema(&[("file-index.future-index.version", "upstream-specific")]);
    assert!(FileIndexOptions::parse(schema.options(), schema.fields())
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn test_file_index_threshold_boundary_and_abort() {
    let data = batch(vec![Some(1), None, Some(3)], vec![None, None, None]);
    let table_schema = schema(&[("file-index.bitmap.columns", "id")]);
    let config = FileIndexOptions::parse(table_schema.options(), table_schema.fields())
        .unwrap()
        .unwrap();
    assert_eq!(config.in_manifest_threshold, 500);
    let mut index = config.create_writer().unwrap();
    index.write(&data).unwrap();
    let size = index.serialize().unwrap().len();
    for threshold in [size - 1, size, size + 1] {
        let raw = format!("{threshold} B");
        let table = table(
            memory_io(),
            schema(&[
                ("file-index.bitmap.columns", "id"),
                ("file-index.in-manifest-threshold", &raw),
            ]),
        )
        .await;
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        writer.write_arrow_batch(&data).await.unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        let file = &messages[0].new_files[0];
        assert_eq!(file.embedded_index.is_some(), size <= threshold);
        let bucket_path = format!("{}/bucket-0", table.location());
        let paths = file.collect_files(&bucket_path);
        for path in &paths {
            assert!(table.file_io().exists(path).await.unwrap());
        }
        let bytes = if let Some(bytes) = &file.embedded_index {
            Bytes::copy_from_slice(bytes)
        } else {
            table
                .file_io()
                .new_input(&paths[1])
                .unwrap()
                .read()
                .await
                .unwrap()
        };
        assert_eq!(bytes.len(), size);
        builder.new_commit().abort(&messages).await.unwrap();
        for path in paths {
            assert!(!table.file_io().exists(&path).await.unwrap());
        }
    }
}

#[tokio::test]
async fn test_file_index_invalid_configuration_fails_before_writing() {
    let cases = vec![
        vec![("file-index.range-bitmap.columns", "missing")],
        vec![("file-index.range-bitmap.columns", "id[nested]")],
        vec![("file-index.range-bitmap.id.chunk-size", "0b")],
        vec![
            ("file-index.range-bitmap.columns", "id"),
            ("file-index.range-bitmap.id.chunk-size", "2gb"),
        ],
        vec![
            ("file-index.range-bitmap.columns", "id"),
            ("file-index.range-bitmap.id.version", "1"),
        ],
        vec![("file-index.bitmap.columns", "missing")],
        vec![("file-index.bitmap.version", "2")],
        vec![("file-index.bitmap.columns", "")],
        vec![("file-index.bitmap.columns", "id,")],
        vec![("file-index.bitmap.columns", "id[nested]")],
        vec![
            ("file-index.bitmap.columns", "id"),
            ("file-index.bitmap.id.version", "1"),
        ],
        vec![
            ("file-index.bitmap.columns", "id"),
            ("file-index.bitmap.id.index-block-size", "0 B"),
        ],
        vec![
            ("file-index.bloom-filter.columns", "id"),
            ("file-index.bloom-filter.id.items", "0"),
        ],
        vec![
            ("file-index.bloom-filter.columns", "id"),
            ("file-index.bloom-filter.id.fpp", "1.5"),
        ],
        vec![("file-index.bloom-filter.id.items", "10")],
        vec![
            ("file-index.bitmap.columns", "id"),
            ("file-index.bitmap.id.typo", "1"),
        ],
        vec![("file-index.in-manifest-threshold", "invalid")],
        vec![("file-index.in-manifest-threshold", "-1")],
        vec![("file-index.in-manifest-threshold", "9223372036854775807 TB")],
    ];
    for mut options in cases {
        options.push(("file-index.future-index.columns", "id"));
        options.push(("file-index.future-index.id.version", "upstream-specific"));
        let table = table(memory_io(), schema(&options)).await;
        assert!(
            table.new_write_builder().new_write().is_err(),
            "{options:?}"
        );
        assert!(!table
            .file_io()
            .exists(&format!("{}/bucket-0", table.location()))
            .await
            .unwrap());
    }
    let schema = Schema::builder()
        .column("flag", crate::spec::DataType::Boolean(BooleanType::new()))
        .option("file-index.bloom-filter.columns", "flag")
        .build()
        .unwrap();
    assert!(matches!(
        FileIndexOptions::parse(schema.options(), schema.fields()),
        Err(Error::Unsupported { .. })
    ));
}

#[tokio::test]
async fn test_file_index_data_evolution_base_file() {
    for identifier in ["bitmap", "range-bitmap"] {
        let schema = Schema::builder()
            .column("id", crate::spec::DataType::Int(IntType::new()))
            .column("value", crate::spec::DataType::Int(IntType::new()))
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true")
            .option(format!("file-index.{identifier}.columns"), "id")
            .build()
            .unwrap();
        let table = table(memory_io(), schema).await;
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        writer
            .write_arrow_batch(&pk_batch(&[3, 1, 2], &[30, 10, 20]))
            .await
            .unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        assert_eq!(messages[0].new_files.len(), 1);
        assert!(messages[0].new_files[0].embedded_index.is_some());
        builder.new_commit().commit(messages).await.unwrap();
        let plan = table.new_read_builder().new_scan().plan().await.unwrap();
        let file = &plan.splits()[0].data_files()[0];
        assert_eq!(file.first_row_id, Some(0));
        let predicate = PredicateBuilder::new(table.schema().fields())
            .equal("id", Datum::Int(1))
            .unwrap();
        assert_eq!(
            evaluate(
                &table,
                plan.splits()[0].bucket_path(),
                file,
                predicate.clone()
            )
            .await,
            FileIndexResult::Selection([1].into_iter().collect())
        );
        assert_eq!(
            query(&table, true, Some(predicate)).await,
            vec![(Some(1), Some(10))]
        );
    }
}

#[tokio::test]
async fn test_file_index_data_evolution_partial_file_projects_column_positions() {
    for threshold in ["0 B", "1 MB"] {
        let schema = Schema::builder()
            .column("id", crate::spec::DataType::Int(IntType::new()))
            .column("value", crate::spec::DataType::Int(IntType::new()))
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true")
            .option("file-index.bitmap.columns", "id,value")
            .option("file-index.in-manifest-threshold", threshold)
            .build()
            .unwrap();
        let table = table(memory_io(), schema).await;
        let builder = table.new_write_builder();
        let mut base_writer = builder.new_write().unwrap();
        base_writer
            .write_arrow_batch(&pk_batch(&[1, 2, 3], &[10, 20, 30]))
            .await
            .unwrap();
        builder
            .new_commit()
            .commit(base_writer.prepare_commit().await.unwrap())
            .await
            .unwrap();

        let mut partial_writer =
            DataEvolutionPartialWriter::new(&table, vec!["value".to_string()]).unwrap();
        let partial_batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Field::new(
                "value",
                ArrowType::Int32,
                true,
            )])),
            vec![Arc::new(Int32Array::from(vec![30, 11, 20]))],
        )
        .unwrap();
        partial_writer
            .write_partial_batch(
                crate::spec::EMPTY_BINARY_ROW.to_serialized_bytes(),
                0,
                0,
                1,
                partial_batch,
            )
            .await
            .unwrap();
        let messages = partial_writer.prepare_commit().await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].new_files.len(), 1);
        let file = &messages[0].new_files[0];
        assert_eq!(
            file.write_cols.as_deref(),
            Some(["value".to_string()].as_slice())
        );
        assert_eq!(file.embedded_index.is_some(), threshold != "0 B");
        assert_eq!(file.extra_files.len(), usize::from(threshold == "0 B"));
        let bucket_dir = format!("{}/bucket-0", table.location());
        let predicate_builder = PredicateBuilder::new(table.schema().fields());
        assert_eq!(
            evaluate(
                &table,
                &bucket_dir,
                file,
                predicate_builder.equal("value", Datum::Int(11)).unwrap(),
            )
            .await,
            FileIndexResult::Selection([1].into_iter().collect())
        );
        assert_eq!(
            evaluate(
                &table,
                &bucket_dir,
                file,
                predicate_builder.equal("id", Datum::Int(1)).unwrap(),
            )
            .await,
            FileIndexResult::Remain
        );
        builder.new_commit().commit(messages).await.unwrap();
        assert_eq!(
            query(&table, true, None).await,
            vec![
                (Some(1), Some(30)),
                (Some(2), Some(11)),
                (Some(3), Some(20))
            ]
        );
    }
}

fn primary_key_schema(options: &[(&str, &str)]) -> Schema {
    let mut builder = Schema::builder()
        .column("id", crate::spec::DataType::Int(IntType::new()))
        .column("value", crate::spec::DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("target-file-size", "128 MB");
    for (key, value) in options {
        builder = builder.option(*key, *value);
    }
    builder.build().unwrap()
}

async fn indexed_pk_table(options: &[(&str, &str)]) -> Table {
    table(memory_io(), primary_key_schema(options)).await
}

fn pk_batch(ids: &[i32], values: &[i32]) -> RecordBatch {
    assert_eq!(ids.len(), values.len());
    batch(
        ids.iter().copied().map(Some).collect(),
        values.iter().copied().map(Some).collect(),
    )
}

async fn pk_query(table: &Table, enabled: bool, predicate: Option<Predicate>) -> Vec<(i32, i32)> {
    query(table, enabled, predicate)
        .await
        .into_iter()
        .map(|(id, value)| (id.unwrap(), value.unwrap()))
        .collect()
}

#[tokio::test]
async fn test_pk_file_index_follows_sorted_deduplicated_rows() {
    for engine in ["deduplicate", "first-row"] {
        for threshold in ["0 B", "1 MB"] {
            for index_type in ["bitmap", "range-bitmap", "bsi"] {
                let index_option = match index_type {
                    "bitmap" => "file-index.bitmap.columns",
                    "range-bitmap" => "file-index.range-bitmap.columns",
                    _ => "file-index.bsi.columns",
                };
                let table = indexed_pk_table(&[
                    ("merge-engine", engine),
                    (index_option, "id,value"),
                    ("file-index.in-manifest-threshold", threshold),
                ])
                .await;
                let builder = table.new_write_builder();
                let mut writer = builder.new_write().unwrap();
                // Arrival order differs from file order and both keys repeat.
                writer
                    .write_arrow_batch(&pk_batch(&[3, 1, 2], &[30, 10, 20]))
                    .await
                    .unwrap();
                writer
                    .write_arrow_batch(&pk_batch(&[1, 3], &[11, 31]))
                    .await
                    .unwrap();
                let mut messages = writer.prepare_commit().await.unwrap();
                if engine == "first-row" {
                    // First-row batch scans expose materialized files, not L0.
                    for message in &mut messages {
                        for file in &mut message.new_files {
                            file.level = 1;
                        }
                    }
                }
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].new_files.len(), 1);
                let file = &messages[0].new_files[0];
                assert_eq!(file.row_count, 3);
                assert_eq!(file.embedded_index.is_some(), threshold != "0 B");
                assert_eq!(file.extra_files.len(), usize::from(threshold == "0 B"));
                for path in file.collect_files(&format!("{}/bucket-0", table.location())) {
                    assert!(table.file_io().exists(&path).await.unwrap(), "{path}");
                }
                builder.new_commit().commit(messages).await.unwrap();

                let expected = if engine == "first-row" {
                    vec![(1, 10), (2, 20), (3, 30)]
                } else {
                    vec![(1, 11), (2, 20), (3, 31)]
                };
                assert_eq!(pk_query(&table, false, None).await, expected);
                assert_eq!(pk_query(&table, true, None).await, expected);
                let plan = table.new_read_builder().new_scan().plan().await.unwrap();
                let predicates = PredicateBuilder::new(table.schema().fields());
                let file = &plan.splits()[0].data_files()[0];
                let id_one = evaluate(
                    &table,
                    plan.splits()[0].bucket_path(),
                    file,
                    predicates.equal("id", Datum::Int(1)).unwrap(),
                )
                .await;
                assert_eq!(
                    id_one,
                    FileIndexResult::Selection([0].into_iter().collect())
                );
                let last_value = if engine == "first-row" { 30 } else { 31 };
                let value_predicate = predicates.equal("value", Datum::Int(last_value)).unwrap();
                assert_eq!(
                    evaluate(
                        &table,
                        plan.splits()[0].bucket_path(),
                        file,
                        value_predicate.clone()
                    )
                    .await,
                    FileIndexResult::Selection([2].into_iter().collect()),
                    "engine={engine}, index={index_type}, threshold={threshold}"
                );
                assert_eq!(
                    pk_query(&table, true, Some(value_predicate)).await,
                    vec![(3, last_value)]
                );
            }
        }
    }
}

#[tokio::test]
async fn test_pk_file_index_abort_removes_data_and_sidecar() {
    for threshold in ["0 B", "1 MB"] {
        let table = indexed_pk_table(&[
            ("file-index.bitmap.columns", "id,value"),
            ("file-index.in-manifest-threshold", threshold),
        ])
        .await;
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        writer
            .write_arrow_batch(&pk_batch(&[2, 1], &[20, 10]))
            .await
            .unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        let file = &messages[0].new_files[0];
        let paths = file.collect_files(&format!("{}/bucket-0", table.location()));
        assert_eq!(paths.len(), if threshold == "0 B" { 2 } else { 1 });
        for path in &paths {
            assert!(table.file_io().exists(path).await.unwrap());
        }
        builder.new_commit().abort(&messages).await.unwrap();
        for path in &paths {
            assert!(!table.file_io().exists(path).await.unwrap());
        }
    }
}

#[tokio::test]
async fn test_postpone_file_index_preserves_arrival_order_and_rolls() {
    for threshold in ["0 B", "1 MB"] {
        for rolling in [false, true] {
            let table = indexed_pk_table(&[
                ("bucket", "-2"),
                ("target-file-size", if rolling { "1 B" } else { "128 MB" }),
                ("file-index.bitmap.columns", "id,value"),
                ("file-index.in-manifest-threshold", threshold),
            ])
            .await;
            let builder = table.new_write_builder();
            let mut writer = builder.new_write().unwrap();
            writer
                .write_arrow_batch(&pk_batch(&[3, 1, 3], &[30, 10, 31]))
                .await
                .unwrap();
            writer
                .write_arrow_batch(&pk_batch(&[2, 1], &[20, 11]))
                .await
                .unwrap();
            let messages = writer.prepare_commit().await.unwrap();
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].bucket, crate::spec::POSTPONE_BUCKET);
            assert_eq!(messages[0].new_files.len(), if rolling { 2 } else { 1 });
            let predicates = PredicateBuilder::new(table.schema().fields());
            let bucket_dir = format!("{}/bucket-postpone", table.location());
            for (position, file) in messages[0].new_files.iter().enumerate() {
                assert_eq!(file.embedded_index.is_some(), threshold != "0 B");
                assert_eq!(file.extra_files.len(), usize::from(threshold == "0 B"));
                let expected_id_3: roaring::RoaringBitmap = if position == 0 {
                    [0, 2].into_iter().collect()
                } else {
                    roaring::RoaringBitmap::new()
                };
                let expected_id_1: roaring::RoaringBitmap = if rolling {
                    [1].into_iter().collect()
                } else {
                    [1, 4].into_iter().collect()
                };
                let result = evaluate(
                    &table,
                    &bucket_dir,
                    file,
                    predicates.equal("id", Datum::Int(3)).unwrap(),
                )
                .await;
                assert_eq!(result, FileIndexResult::Selection(expected_id_3));
                let result = evaluate(
                    &table,
                    &bucket_dir,
                    file,
                    predicates.equal("id", Datum::Int(1)).unwrap(),
                )
                .await;
                assert_eq!(result, FileIndexResult::Selection(expected_id_1));
                for path in file.collect_files(&bucket_dir) {
                    assert!(table.file_io().exists(&path).await.unwrap(), "{path}");
                }
            }
            builder.new_commit().abort(&messages).await.unwrap();
            for file in &messages[0].new_files {
                for path in file.collect_files(&bucket_dir) {
                    assert!(!table.file_io().exists(&path).await.unwrap(), "{path}");
                }
            }
        }
    }
}

#[tokio::test]
async fn test_postpone_file_index_omits_value_kind_column() {
    let table = indexed_pk_table(&[
        ("bucket", "-2"),
        ("file-index.bitmap.columns", "id,value"),
        ("file-index.in-manifest-threshold", "1 MB"),
    ])
    .await;
    let write_batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", ArrowType::Int32, true),
            Field::new("value", ArrowType::Int32, true),
            Field::new("_VALUE_KIND", ArrowType::Int8, false),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![Some(3), Some(1), Some(3)])),
            Arc::new(Int32Array::from(vec![Some(30), Some(10), Some(31)])),
            Arc::new(arrow_array::Int8Array::from(vec![0, 3, 2])),
        ],
    )
    .unwrap();
    let mut writer = table.new_write_builder().new_write().unwrap();
    writer.write_arrow_batch(&write_batch).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    let file = &messages[0].new_files[0];
    let predicate = PredicateBuilder::new(table.schema().fields())
        .equal("value", Datum::Int(31))
        .unwrap();
    assert_eq!(
        evaluate(
            &table,
            &format!("{}/bucket-postpone", table.location()),
            file,
            predicate,
        )
        .await,
        FileIndexResult::Selection([2].into_iter().collect())
    );
}

#[tokio::test]
async fn test_file_index_uses_partition_bucket_file_row_order() {
    let schema = Schema::builder()
        .column("id", crate::spec::DataType::Int(IntType::new()))
        .column("value", crate::spec::DataType::Int(IntType::new()))
        .partition_keys(["value"])
        .option("bucket", "2")
        .option("bucket-key", "id")
        .option("file-index.bitmap.columns", "id,value")
        .option("file-index.range-bitmap.columns", "id,value")
        .option("file-index.in-manifest-threshold", "0 B")
        .build()
        .unwrap();
    let table = table(memory_io(), schema).await;
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    let data = batch(
        vec![Some(3), Some(1), Some(1), Some(3)],
        vec![Some(1), Some(2), Some(1), Some(2)],
    );
    writer.write_arrow_batch(&data).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(
        messages
            .iter()
            .map(|m| &m.partition)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        2
    );
    builder.new_commit().commit(messages).await.unwrap();
    let read_builder = table.new_read_builder();
    let plan = read_builder.new_scan().plan().await.unwrap();
    for split in plan.splits() {
        for file in split.data_files() {
            let single = DataSplitBuilder::new()
                .with_snapshot(1)
                .with_partition(split.partition().clone())
                .with_bucket(split.bucket())
                .with_bucket_path(split.bucket_path().to_string())
                .with_total_buckets(2)
                .with_data_files(vec![file.clone()])
                .build()
                .unwrap();
            let batches: Vec<RecordBatch> = read_builder
                .new_read()
                .unwrap()
                .to_arrow(&[single])
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            let expected: roaring::RoaringBitmap = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .iter()
                })
                .enumerate()
                .filter_map(|(row, id)| (id == Some(3)).then_some(row as u32))
                .collect();
            let predicates = PredicateBuilder::new(table.schema().fields());
            for predicate in [
                predicates.equal("id", Datum::Int(3)).unwrap(),
                predicates.greater_than("id", Datum::Int(2)).unwrap(),
            ] {
                let actual = evaluate(&table, split.bucket_path(), file, predicate).await;
                match actual {
                    FileIndexResult::Selection(rows) => assert_eq!(rows, expected),
                    FileIndexResult::Skip => assert!(expected.is_empty()),
                    FileIndexResult::Remain => panic!("index predicate must select physical rows"),
                }
            }
        }
    }
    assert_eq!(query(&table, true, None).await, rows(&[data]));
}

#[tokio::test]
async fn test_file_index_other_append_formats() {
    for format in ["row"]
        .into_iter()
        .chain(cfg!(feature = "vortex").then_some("vortex"))
    {
        let table = table(
            memory_io(),
            schema(&[
                ("file.format", format),
                ("file-index.bitmap.columns", "id"),
                ("file-index.in-manifest-threshold", "0 B"),
            ]),
        )
        .await;
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        writer
            .write_arrow_batch(&batch(
                vec![Some(1), None, Some(3)],
                vec![None, Some(2), Some(3)],
            ))
            .await
            .unwrap();
        builder
            .new_commit()
            .commit(writer.prepare_commit().await.unwrap())
            .await
            .unwrap();
        let predicate = PredicateBuilder::new(table.schema().fields())
            .equal("id", Datum::Int(3))
            .unwrap();
        for enabled in [false, true] {
            assert_eq!(
                query(&table, enabled, Some(predicate.clone())).await,
                vec![(Some(3), Some(3))]
            );
        }
    }
}

#[derive(Debug)]
struct StorageProbe {
    op: Operator,
    data_accesses: AtomicUsize,
    index_accesses: AtomicUsize,
    fail_index_at: usize,
}

impl StorageProbe {
    fn new(fail_index_at: usize) -> Arc<Self> {
        Arc::new(Self {
            op: Operator::from_config(MemoryConfig::default()).unwrap(),
            data_accesses: AtomicUsize::new(0),
            index_accesses: AtomicUsize::new(0),
            fail_index_at,
        })
    }
    fn io(self: &Arc<Self>) -> FileIO {
        memory_io().with_provider(self.clone())
    }
}

#[async_trait::async_trait]
impl FileIOProvider for StorageProbe {
    async fn create(&self, path: &str) -> Result<(Operator, String)> {
        let relative = path.strip_prefix("memory:/").unwrap().to_string();
        if path.ends_with(".parquet") || path.ends_with(".row") {
            self.data_accesses.fetch_add(1, Ordering::SeqCst);
        }
        if path.ends_with(".index")
            && self.index_accesses.fetch_add(1, Ordering::SeqCst) + 1 == self.fail_index_at
        {
            self.op
                .write(&relative, Bytes::from_static(b"partial index"))
                .await
                .unwrap();
            return Err(Error::DataInvalid {
                message: "Injected sidecar write failure".to_string(),
                source: None,
            });
        }
        Ok((self.op.clone(), relative))
    }
}

#[tokio::test]
async fn test_file_index_prunes_without_opening_data_file() {
    for threshold in ["0 B", "1 MB"] {
        let storage = StorageProbe::new(0);
        let table = table(
            storage.io(),
            schema(&[
                ("file-index.bitmap.columns", "id"),
                ("file-index.in-manifest-threshold", threshold),
            ]),
        )
        .await;
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        writer
            .write_arrow_batch(&batch(vec![Some(1), Some(3)], vec![None, None]))
            .await
            .unwrap();
        builder
            .new_commit()
            .commit(writer.prepare_commit().await.unwrap())
            .await
            .unwrap();
        let predicate = PredicateBuilder::new(table.schema().fields())
            .equal("id", Datum::Int(2))
            .unwrap();
        let mut read_builder = table.new_read_builder();
        read_builder.with_filter(predicate.clone());
        let (_, trace) = read_builder.new_scan().plan_with_trace().await.unwrap();
        assert_eq!(trace.final_files, 1, "statistics must retain the file");
        storage.data_accesses.store(0, Ordering::SeqCst);
        assert!(query(&table, true, Some(predicate.clone()))
            .await
            .is_empty());
        assert_eq!(storage.data_accesses.load(Ordering::SeqCst), 0);
        assert!(query(&table, false, Some(predicate)).await.is_empty());
        assert!(storage.data_accesses.load(Ordering::SeqCst) > 0);
    }
}

#[tokio::test]
async fn test_pk_file_index_prunes_before_sort_merge_opens_data() {
    for threshold in ["0 B", "1 MB"] {
        for identifier in ["bitmap", "bsi"] {
            let storage = StorageProbe::new(0);
            let schema = primary_key_schema(&[
                (
                    if identifier == "bitmap" {
                        "file-index.bitmap.columns"
                    } else {
                        "file-index.bsi.columns"
                    },
                    "id",
                ),
                ("file-index.in-manifest-threshold", threshold),
            ]);
            let table = table(storage.io(), schema).await;
            let builder = table.new_write_builder();
            let mut writer = builder.new_write().unwrap();
            writer
                .write_arrow_batch(&pk_batch(&[3, 1], &[30, 10]))
                .await
                .unwrap();
            builder
                .new_commit()
                .commit(writer.prepare_commit().await.unwrap())
                .await
                .unwrap();
            let predicate = PredicateBuilder::new(table.schema().fields())
                .equal("id", Datum::Int(2))
                .unwrap();
            let mut read_builder = table.new_read_builder();
            read_builder.with_filter(predicate.clone());
            let (_, trace) = read_builder.new_scan().plan_with_trace().await.unwrap();
            assert_eq!(trace.final_files, 1, "stats must retain the file");
            storage.data_accesses.store(0, Ordering::SeqCst);
            assert!(pk_query(&table, true, Some(predicate.clone()))
                .await
                .is_empty());
            assert_eq!(storage.data_accesses.load(Ordering::SeqCst), 0);
            assert!(pk_query(&table, false, Some(predicate)).await.is_empty());
            assert!(storage.data_accesses.load(Ordering::SeqCst) > 0);
        }
    }
}

#[tokio::test]
async fn test_data_evolution_file_index_prunes_independent_file() {
    for threshold in ["0 B", "1 MB"] {
        let storage = StorageProbe::new(0);
        let schema = Schema::builder()
            .column("id", crate::spec::DataType::Int(IntType::new()))
            .column("value", crate::spec::DataType::Int(IntType::new()))
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true")
            .option("file-index.bsi.columns", "id")
            .option("file-index.in-manifest-threshold", threshold)
            .build()
            .unwrap();
        let table = table(storage.io(), schema).await;
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        writer
            .write_arrow_batch(&pk_batch(&[1, 3], &[10, 30]))
            .await
            .unwrap();
        builder
            .new_commit()
            .commit(writer.prepare_commit().await.unwrap())
            .await
            .unwrap();
        let predicate = PredicateBuilder::new(table.schema().fields())
            .equal("id", Datum::Int(2))
            .unwrap();
        let mut read_builder = table.new_read_builder();
        read_builder.with_filter(predicate.clone());
        let (_, trace) = read_builder.new_scan().plan_with_trace().await.unwrap();
        assert_eq!(trace.final_files, 1, "stats must retain the file");
        storage.data_accesses.store(0, Ordering::SeqCst);
        assert!(query(&table, true, Some(predicate.clone()))
            .await
            .is_empty());
        assert_eq!(storage.data_accesses.load(Ordering::SeqCst), 0);
        assert!(query(&table, false, Some(predicate)).await.is_empty());
        assert!(storage.data_accesses.load(Ordering::SeqCst) > 0);
    }
}

#[tokio::test]
async fn test_pk_file_index_keeps_old_versions_for_non_key_filter() {
    for identifier in ["bitmap", "bsi"] {
        let table = indexed_pk_table(&[
            (
                if identifier == "bitmap" {
                    "file-index.bitmap.columns"
                } else {
                    "file-index.bsi.columns"
                },
                "id,value",
            ),
            ("file-index.in-manifest-threshold", "0 B"),
        ])
        .await;
        let builder = table.new_write_builder();
        let mut old_writer = builder.new_write().unwrap();
        old_writer
            .write_arrow_batch(&pk_batch(&[1, 2], &[10, 20]))
            .await
            .unwrap();
        builder
            .new_commit()
            .commit(old_writer.prepare_commit().await.unwrap())
            .await
            .unwrap();
        let mut new_writer = builder.new_write().unwrap();
        new_writer
            .write_arrow_batch(&pk_batch(&[1, 3], &[11, 30]))
            .await
            .unwrap();
        builder
            .new_commit()
            .commit(new_writer.prepare_commit().await.unwrap())
            .await
            .unwrap();
        let predicates = PredicateBuilder::new(table.schema().fields());
        for (predicate, expected) in [
            (predicates.equal("value", Datum::Int(10)).unwrap(), vec![]),
            (
                predicates.equal("value", Datum::Int(11)).unwrap(),
                vec![(1, 11)],
            ),
            (
                predicates.equal("id", Datum::Int(1)).unwrap(),
                vec![(1, 11)],
            ),
            (
                predicates.equal("id", Datum::Int(2)).unwrap(),
                vec![(2, 20)],
            ),
        ] {
            assert_eq!(
                pk_query(&table, true, Some(predicate.clone())).await,
                expected
            );
            assert_eq!(pk_query(&table, false, Some(predicate)).await, expected);
        }
    }
}

#[tokio::test]
async fn test_pk_file_index_excludes_input_changelog_files() {
    for threshold in ["0 B", "1 MB"] {
        let table = indexed_pk_table(&[
            ("changelog-producer", "input"),
            ("file-index.bsi.columns", "id,value"),
            ("file-index.in-manifest-threshold", threshold),
        ])
        .await;
        assert_eq!(
            table
                .schema()
                .options()
                .get("changelog-producer")
                .map(String::as_str),
            Some("input")
        );
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        writer
            .write_arrow_batch(&pk_batch(&[2, 1, 1], &[20, 10, 11]))
            .await
            .unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].new_files.len(), 1);
        assert_eq!(messages[0].new_changelog_files.len(), 1);
        let data = &messages[0].new_files[0];
        let changelog = &messages[0].new_changelog_files[0];
        assert_eq!(data.row_count, 2);
        assert_eq!(changelog.row_count, 3);
        assert_eq!(data.embedded_index.is_some(), threshold != "0 B");
        assert_eq!(data.extra_files.len(), usize::from(threshold == "0 B"));
        assert!(changelog.embedded_index.is_none());
        assert!(changelog.extra_files.is_empty());
        let bucket_dir = format!("{}/bucket-0", table.location());
        let predicate = PredicateBuilder::new(table.schema().fields())
            .equal("id", Datum::Int(1))
            .unwrap();
        assert_eq!(
            evaluate(&table, &bucket_dir, data, predicate.clone()).await,
            FileIndexResult::Selection([0].into_iter().collect())
        );
        assert_eq!(
            evaluate(&table, &bucket_dir, changelog, predicate).await,
            FileIndexResult::Remain
        );
        builder.new_commit().commit(messages).await.unwrap();
        assert_eq!(pk_query(&table, true, None).await, vec![(1, 11), (2, 20)]);
    }
}

#[tokio::test]
async fn test_pk_file_index_crosses_sorted_chunk_boundary() {
    let table = indexed_pk_table(&[
        ("file-index.bsi.columns", "id,value"),
        ("file-index.in-manifest-threshold", "0 B"),
    ])
    .await;
    let ids: Vec<i32> = (0..5000).rev().collect();
    let values: Vec<i32> = ids.iter().map(|id| id * 2).collect();
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer
        .write_arrow_batch(&pk_batch(&ids, &values))
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(messages[0].new_files.len(), 1);
    let file = &messages[0].new_files[0];
    assert_eq!(file.row_count, 5000);
    let bucket_dir = format!("{}/bucket-0", table.location());
    let predicates = PredicateBuilder::new(table.schema().fields());
    for id in [0, 4095, 4096, 4999] {
        assert_eq!(
            evaluate(
                &table,
                &bucket_dir,
                file,
                predicates.equal("id", Datum::Int(id)).unwrap(),
            )
            .await,
            FileIndexResult::Selection([id as u32].into_iter().collect())
        );
    }
    builder.new_commit().commit(messages).await.unwrap();
    for id in [0, 4095, 4096, 4999] {
        let predicate = predicates.equal("id", Datum::Int(id)).unwrap();
        assert_eq!(
            pk_query(&table, true, Some(predicate)).await,
            vec![(id, id * 2)]
        );
    }
}

#[tokio::test]
async fn test_dynamic_bucket_indexed_commit_keeps_hash_and_changelog_indexes() {
    let schema = Schema::builder()
        .column(
            "pt",
            crate::spec::DataType::VarChar(crate::spec::VarCharType::string_type()),
        )
        .column("id", crate::spec::DataType::Int(IntType::new()))
        .column("value", crate::spec::DataType::Int(IntType::new()))
        .partition_keys(["pt"])
        .primary_key(["pt", "id"])
        .option("changelog-producer", "input")
        .option("file-index.bsi.columns", "id")
        .option("file-index.in-manifest-threshold", "0 B")
        .build()
        .unwrap();
    let table = table(memory_io(), schema).await;
    let input = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("pt", ArrowType::Utf8, true),
            Field::new("id", ArrowType::Int32, true),
            Field::new("value", ArrowType::Int32, true),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["a", "a"])),
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Int32Array::from(vec![10, 20])),
        ],
    )
    .unwrap();
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(&input).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].new_files.len(), 1);
    assert_eq!(messages[0].new_changelog_files.len(), 1);
    assert_eq!(messages[0].new_index_files.len(), 1);
    assert_eq!(messages[0].new_index_files[0].index_type, "HASH");
    assert_eq!(messages[0].new_files[0].extra_files.len(), 1);
    assert!(messages[0].new_changelog_files[0].extra_files.is_empty());
    builder.new_commit().commit(messages).await.unwrap();
    let mut read_builder = table.new_read_builder();
    read_builder.with_projection(&["id", "value"]).unwrap();
    let plan = read_builder.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = read_builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        rows(&batches),
        vec![(Some(1), Some(10)), (Some(2), Some(20))]
    );
}

#[tokio::test]
async fn test_postpone_indexed_partition_sidecars_are_aborted() {
    let schema = Schema::builder()
        .column(
            "pt",
            crate::spec::DataType::VarChar(crate::spec::VarCharType::string_type()),
        )
        .column("id", crate::spec::DataType::Int(IntType::new()))
        .column("value", crate::spec::DataType::Int(IntType::new()))
        .partition_keys(["pt"])
        .primary_key(["pt", "id"])
        .option("bucket", "-2")
        .option("file-index.bsi.columns", "id")
        .option("file-index.in-manifest-threshold", "0 B")
        .build()
        .unwrap();
    let table = table(memory_io(), schema).await;
    let input = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("pt", ArrowType::Utf8, true),
            Field::new("id", ArrowType::Int32, true),
            Field::new("value", ArrowType::Int32, true),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "a", "b"])),
            Arc::new(Int32Array::from(vec![3, 2, 1, 4])),
            Arc::new(Int32Array::from(vec![30, 20, 10, 40])),
        ],
    )
    .unwrap();
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(&input).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    assert_eq!(messages.len(), 2);
    let computer = crate::spec::PartitionComputer::new(
        table.schema().partition_keys(),
        table.schema().fields(),
        "__DEFAULT_PARTITION__",
        false,
    )
    .unwrap();
    let mut selected = 0;
    let predicates = PredicateBuilder::new(table.schema().fields());
    for message in &messages {
        assert_eq!(message.bucket, crate::spec::POSTPONE_BUCKET);
        assert_eq!(message.new_files.len(), 1);
        let file = &message.new_files[0];
        assert_eq!(file.extra_files.len(), 1);
        let partition = crate::spec::BinaryRow::from_serialized_bytes(&message.partition).unwrap();
        let partition_path = computer.generate_partition_path(&partition).unwrap();
        let bucket_dir = format!("{}/{partition_path}/bucket-postpone", table.location());
        let result = evaluate(
            &table,
            &bucket_dir,
            file,
            predicates.equal("id", Datum::Int(3)).unwrap(),
        )
        .await;
        if result.remain() {
            assert_eq!(
                result,
                FileIndexResult::Selection([0].into_iter().collect())
            );
            selected += 1;
        }
        for path in file.collect_files(&bucket_dir) {
            assert!(table.file_io().exists(&path).await.unwrap());
        }
    }
    assert_eq!(selected, 1);
    builder.new_commit().abort(&messages).await.unwrap();
    for message in &messages {
        let partition = crate::spec::BinaryRow::from_serialized_bytes(&message.partition).unwrap();
        let partition_path = computer.generate_partition_path(&partition).unwrap();
        let bucket_dir = format!("{}/{partition_path}/bucket-postpone", table.location());
        for file in &message.new_files {
            for path in file.collect_files(&bucket_dir) {
                assert!(!table.file_io().exists(&path).await.unwrap());
            }
        }
    }
}

#[tokio::test]
async fn test_partial_column_index_remaps_reordered_fields() {
    let schema = Schema::builder()
        .column("id", crate::spec::DataType::Int(IntType::new()))
        .column("a", crate::spec::DataType::Int(IntType::new()))
        .column("b", crate::spec::DataType::Int(IntType::new()))
        .option("data-evolution.enabled", "true")
        .option("row-tracking.enabled", "true")
        .option("file-index.bitmap.columns", "a,b")
        .build()
        .unwrap();
    let table = table(memory_io(), schema).await;
    let mut writer =
        DataEvolutionPartialWriter::new(&table, vec!["b".to_string(), "a".to_string()]).unwrap();
    let partial_batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("b", ArrowType::Int32, true),
            Field::new("a", ArrowType::Int32, true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![100, 200, 300])),
            Arc::new(Int32Array::from(vec![10, 20, 30])),
        ],
    )
    .unwrap();
    writer
        .write_partial_batch(
            crate::spec::EMPTY_BINARY_ROW.to_serialized_bytes(),
            0,
            0,
            0,
            partial_batch,
        )
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    let file = &messages[0].new_files[0];
    assert_eq!(
        file.write_cols.as_ref().unwrap(),
        &vec!["b".to_string(), "a".to_string()]
    );
    let bucket_dir = format!("{}/bucket-0", table.location());
    let predicates = PredicateBuilder::new(table.schema().fields());
    for (column, value, row) in [("a", 20, 1), ("b", 300, 2)] {
        assert_eq!(
            evaluate(
                &table,
                &bucket_dir,
                file,
                predicates.equal(column, Datum::Int(value)).unwrap(),
            )
            .await,
            FileIndexResult::Selection([row].into_iter().collect())
        );
    }
}

#[tokio::test]
async fn test_range_bitmap_append_range_pruning() {
    for format in ["parquet", "row"] {
        for threshold in ["0 B", "1 MB"] {
            let storage = StorageProbe::new(0);
            let table = table(
                storage.io(),
                schema(&[
                    ("file.format", format),
                    ("file-index.range-bitmap.columns", "id"),
                    ("file-index.range-bitmap.id.chunk-size", "0b"),
                    ("file-index.in-manifest-threshold", threshold),
                ]),
            )
            .await;
            let builder = table.new_write_builder();
            let mut writer = builder.new_write().unwrap();
            writer
                .write_arrow_batch(&batch(vec![Some(9), None, Some(1), Some(9)], vec![None; 4]))
                .await
                .unwrap();
            builder
                .new_commit()
                .commit(writer.prepare_commit().await.unwrap())
                .await
                .unwrap();
            let predicates = PredicateBuilder::new(table.schema().fields());
            let missing = predicates
                .between("id", Datum::Int(3), Datum::Int(7))
                .unwrap();
            let mut scan = table.new_read_builder();
            scan.with_filter(missing.clone());
            let (_, trace) = scan.new_scan().plan_with_trace().await.unwrap();
            assert_eq!(trace.final_files, 1, "statistics must retain the file");
            storage.data_accesses.store(0, Ordering::SeqCst);
            assert!(query(&table, true, Some(missing.clone())).await.is_empty());
            assert_eq!(storage.data_accesses.load(Ordering::SeqCst), 0);
            assert!(query(&table, false, Some(missing)).await.is_empty());
            assert!(storage.data_accesses.load(Ordering::SeqCst) > 0);

            let plan = table.new_read_builder().new_scan().plan().await.unwrap();
            let split = &plan.splits()[0];
            let file = &split.data_files()[0];
            assert_eq!(file.embedded_index.is_some(), threshold != "0 B");
            assert_eq!(file.extra_files.len(), usize::from(threshold == "0 B"));
            for (predicate, positions, expected) in [
                (
                    predicates.less_than("id", Datum::Int(5)).unwrap(),
                    vec![2],
                    vec![(Some(1), None)],
                ),
                (
                    predicates.greater_or_equal("id", Datum::Int(9)).unwrap(),
                    vec![0, 3],
                    vec![(Some(9), None); 2],
                ),
                (
                    predicates.is_null("id").unwrap(),
                    vec![1],
                    vec![(None, None)],
                ),
            ] {
                assert_eq!(
                    evaluate(&table, split.bucket_path(), file, predicate.clone()).await,
                    FileIndexResult::Selection(positions.into_iter().collect())
                );
                for enabled in [false, true] {
                    assert_eq!(
                        query(&table, enabled, Some(predicate.clone())).await,
                        expected
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn test_file_index_bloom_false_positive_keeps_residual_filter() {
    let table = table(
        memory_io(),
        schema(&[
            ("file-index.bloom-filter.columns", "id"),
            ("file-index.bloom-filter.id.items", "1"),
            ("file-index.bloom-filter.id.fpp", "0.99"),
        ]),
    )
    .await;
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer
        .write_arrow_batch(&batch(vec![Some(1), Some(10_000)], vec![None, None]))
        .await
        .unwrap();
    builder
        .new_commit()
        .commit(writer.prepare_commit().await.unwrap())
        .await
        .unwrap();
    let read_builder = table.new_read_builder();
    let plan = read_builder.new_scan().plan().await.unwrap();
    let split = &plan.splits()[0];
    let predicates = PredicateBuilder::new(table.schema().fields());
    let mut false_positive = None;
    for candidate in 2..10_000 {
        let predicate = predicates.equal("id", Datum::Int(candidate)).unwrap();
        if evaluate(
            &table,
            split.bucket_path(),
            &split.data_files()[0],
            predicate.clone(),
        )
        .await
            == FileIndexResult::Remain
        {
            false_positive = Some(predicate);
            break;
        }
    }
    let predicate = false_positive.expect("high-FPP filter should have an in-range false positive");
    for enabled in [false, true] {
        assert!(query(&table, enabled, Some(predicate.clone()))
            .await
            .is_empty());
    }
}

#[tokio::test]
async fn test_file_index_sidecar_failure_cleans_all_partitions_and_rolled_files() {
    for partitioned in [false, true] {
        let storage = StorageProbe::new(2);
        let mut schema = schema(&[
            ("file-index.bitmap.columns", "id"),
            ("target-file-size", "1 B"),
            ("file-index.in-manifest-threshold", "0 B"),
        ]);
        if partitioned {
            schema = Schema::builder()
                .column("id", crate::spec::DataType::Int(IntType::new()))
                .column("value", crate::spec::DataType::Int(IntType::new()))
                .partition_keys(["value"])
                .option("file-index.bitmap.columns", "id")
                .option("file-index.in-manifest-threshold", "0 B")
                .build()
                .unwrap();
        }
        let table = table(storage.io(), schema).await;
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        for value in 1..=3 {
            writer
                .write_arrow_batch(&batch(vec![Some(value)], vec![Some(value)]))
                .await
                .unwrap();
        }
        let error = writer.prepare_commit().await.unwrap_err();
        assert!(error.to_string().contains("Injected sidecar"), "{error}");
        let files = table
            .file_io()
            .list_status_recursive(table.location())
            .await
            .unwrap();
        assert!(
            !files
                .iter()
                .any(|f| f.path.ends_with(".parquet") || f.path.ends_with(".index")),
            "{files:?}"
        );
    }
}

#[tokio::test]
async fn test_file_index_serialization_failure_cleans_data() {
    for rolling in [false, true] {
        let name = "a".repeat(65536);
        let schema = Schema::builder()
            .column(&name, crate::spec::DataType::Int(IntType::new()))
            .option("file-index.bitmap.columns", &name)
            .option("target-file-size", if rolling { "1 B" } else { "128 MB" })
            .build()
            .unwrap();
        let table = table(memory_io(), schema).await;
        let data = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Field::new(
                &name,
                ArrowType::Int32,
                true,
            )])),
            vec![Arc::new(Int32Array::from(vec![Some(1)]))],
        )
        .unwrap();
        let builder = table.new_write_builder();
        let mut writer = builder.new_write().unwrap();
        writer.write_arrow_batch(&data).await.unwrap();
        assert!(matches!(
            writer.prepare_commit().await,
            Err(Error::FileIndexFormatInvalid { .. })
        ));
        let files = table
            .file_io()
            .list_status_recursive(table.location())
            .await
            .unwrap();
        assert!(!files
            .iter()
            .any(|f| f.path.ends_with(".parquet") || f.path.ends_with(".index")));
    }
}
