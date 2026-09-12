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

use arrow_array::Int32Array;
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
    for identifier in ["bitmap", "bloom-filter", "both"] {
        for rolling in [false, true] {
            for threshold in ["0 B", "1 MB"] {
                let mut options = vec![
                    ("target-file-size", if rolling { "1 B" } else { "128 MB" }),
                    ("file-index.in-manifest-threshold", threshold),
                    ("file-index.read.enabled", "false"),
                ];
                if identifier != "bloom-filter" {
                    options.push(("file-index.bitmap.columns", " id, value, id "));
                }
                if identifier != "bitmap" {
                    options.extend([
                        ("file-index.bloom-filter.columns", "id"),
                        ("file-index.bloom-filter.id.items", "10"),
                    ]);
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
        vec![("file-index.bitmap.columns", "missing")],
        vec![("file-index.unknown.columns", "id")],
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
    for options in cases {
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
async fn test_file_index_rejects_unsupported_table_write_modes() {
    for schema in [
        Schema::builder()
            .column("id", crate::spec::DataType::Int(IntType::new()))
            .primary_key(["id"])
            .option("bucket", "1")
            .option("file-index.bitmap.columns", "id")
            .build()
            .unwrap(),
        Schema::builder()
            .column("id", crate::spec::DataType::Int(IntType::new()))
            .option("data-evolution.enabled", "true")
            .option("row-tracking.enabled", "true")
            .option("file-index.bitmap.columns", "id")
            .build()
            .unwrap(),
    ] {
        let table = table(memory_io(), schema).await;
        let error = match table.new_write_builder().new_write() {
            Ok(_) => panic!("unsupported write mode must reject index generation"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("FileIndex generation supports ordinary append writes only"),
            "{error}"
        );
    }
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
            let expected = batches
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
            let actual = evaluate(
                &table,
                split.bucket_path(),
                file,
                PredicateBuilder::new(table.schema().fields())
                    .equal("id", Datum::Int(3))
                    .unwrap(),
            )
            .await;
            match actual {
                FileIndexResult::Selection(rows) => assert_eq!(rows, expected),
                FileIndexResult::Skip => assert!(roaring::RoaringBitmap::is_empty(&expected)),
                FileIndexResult::Remain => panic!("bitmap equality must select physical rows"),
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
        if path.ends_with(".parquet") {
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
        assert!(writer.prepare_commit().await.is_err());
        assert!(writer
            .write_arrow_batch(&batch(vec![Some(4)], vec![Some(4)]))
            .await
            .is_err());
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
