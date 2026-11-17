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
use crate::spec::{CommitKind, DataFileMeta, Snapshot};
use crate::table::{CommitMessage, CompactResult};
use std::collections::{BTreeMap, BTreeSet, HashMap};

async fn table_with_fields(fields: Vec<(&str, DataType)>, options: &[(&str, &str)]) -> Table {
    let path = format!("memory:/managed_blob_compaction/{}", uuid::Uuid::new_v4());
    let mut catalog_options = crate::Options::new();
    catalog_options.set("warehouse", &path);
    catalog_options.set("local-cache.enabled", "true");
    let catalog = crate::catalog::FileSystemCatalog::new(catalog_options).unwrap();
    let io = catalog.file_io().clone();
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("write-only", "true");
    for (name, data_type) in fields {
        schema = schema.column(name, data_type);
    }
    for &(name, value) in options {
        schema = schema.option(name, value);
    }
    let table = Table::new(
        io.clone(),
        Identifier::new("default", "managed_blob_compaction"),
        path.clone(),
        TableSchema::new(0, &schema.build().unwrap()),
        None,
    );
    setup_dirs(&io, &path).await;
    io.new_output(&format!("{path}/schema/schema-0"))
        .unwrap()
        .write(serde_json::to_vec(table.schema()).unwrap().into())
        .await
        .unwrap();
    table
}

async fn table(options: &[(&str, &str)]) -> Table {
    table_with_fields(vec![("payload", DataType::Blob(BlobType::new()))], options).await
}

async fn write(table: &Table, batch: &RecordBatch) -> Vec<CommitMessage> {
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(batch).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    builder.new_commit().commit(messages.clone()).await.unwrap();
    messages
}

async fn latest(table: &Table) -> Snapshot {
    table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap()
}

async fn execute(table: &Table) -> Vec<CompactResult> {
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    assert!(!plan.tasks().is_empty());
    let executor = builder.new_executor().unwrap();
    let mut results = Vec::new();
    for task in plan.tasks() {
        results.push(executor.execute(task).await.unwrap());
    }
    results
}

async fn compact(table: &Table) -> Vec<CompactResult> {
    let results = execute(table).await;
    table
        .new_compact_builder()
        .new_commit()
        .unwrap()
        .commit_compact(results.clone())
        .await
        .unwrap();
    assert_eq!(latest(table).await.commit_kind(), &CommitKind::COMPACT);
    results
}

async fn physical(table: &Table, bucket: i32, file: &DataFileMeta) -> RecordBatch {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let path = format!(
        "{}/{}",
        bucket_path_under(table.location(), "", bucket),
        file.file_name
    );
    let bytes = table
        .file_io()
        .new_input(&path)
        .unwrap()
        .read()
        .await
        .unwrap();
    let batches = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .unwrap()
        .build()
        .unwrap()
        .map(|batch| batch.unwrap())
        .collect::<Vec<_>>();
    arrow_select::concat::concat_batches(&batches[0].schema(), &batches).unwrap()
}

fn scalar_descriptors(batch: &RecordBatch) -> BTreeMap<i32, Option<Vec<u8>>> {
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let values = batch
        .column_by_name("payload")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .unwrap();
    (0..batch.num_rows())
        .map(|row| {
            (
                ids.value(row),
                values.is_valid(row).then(|| values.value(row).to_vec()),
            )
        })
        .collect()
}

async fn assert_references(
    table: &Table,
    bucket: i32,
    file: &DataFileMeta,
    descriptors: &[Vec<u8>],
) {
    assert_eq!(
        file.extra_files,
        vec![format!("{}.blobref", file.file_name)]
    );
    let mut expected = ManagedBlobReferenceCollector::default();
    expected.references_for_test(descriptors.iter().map(|bytes| {
        let descriptor = BlobDescriptor::deserialize(bytes).unwrap();
        let (root, name) = descriptor.uri().rsplit_once('/').unwrap();
        ManagedBlobReference {
            storage_root: root.to_string(),
            file_name: name.to_string(),
        }
    }));
    let path = format!(
        "{}/{}",
        bucket_path_under(table.location(), "", bucket),
        file.extra_files[0]
    );
    let actual = table
        .file_io()
        .new_input(&path)
        .unwrap()
        .read()
        .await
        .unwrap();
    assert_eq!(actual.as_ref(), expected.serialize().unwrap());
}

async fn pack_bytes(table: &Table) -> BTreeMap<String, bytes::Bytes> {
    let mut packs = BTreeMap::new();
    for entry in table
        .file_io()
        .list_status_recursive(table.location())
        .await
        .unwrap()
    {
        if entry.path.ends_with(".managed.blob") {
            let bytes = table
                .file_io()
                .new_input(&entry.path)
                .unwrap()
                .read()
                .await
                .unwrap();
            packs.insert(entry.path, bytes);
        }
    }
    packs
}

async fn paths(table: &Table) -> BTreeSet<String> {
    table
        .file_io()
        .list_status_recursive(table.location())
        .await
        .unwrap()
        .into_iter()
        .filter(|entry| !entry.is_dir)
        .map(|entry| entry.path)
        .collect()
}

fn value_rows(table: &Table, batch: &RecordBatch) -> BTreeMap<i32, Vec<arrow_array::ArrayRef>> {
    let projection = table
        .schema()
        .fields()
        .iter()
        .map(|field| batch.schema().index_of(field.name()).unwrap())
        .collect::<Vec<_>>();
    let values = batch.project(&projection).unwrap();
    let ids = values
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    (0..values.num_rows())
        .map(|row| {
            (
                ids.value(row),
                values
                    .columns()
                    .iter()
                    .map(|column| column.slice(row, 1))
                    .collect(),
            )
        })
        .collect()
}

async fn written_rows(
    table: &Table,
    messages: &[CommitMessage],
) -> BTreeMap<i32, Vec<arrow_array::ArrayRef>> {
    let mut rows = BTreeMap::new();
    for message in messages {
        for file in &message.new_files {
            rows.extend(value_rows(
                table,
                &physical(table, message.bucket, file).await,
            ));
        }
    }
    rows
}

async fn read_batches(table: &Table) -> Vec<RecordBatch> {
    let builder = table.new_read_builder();
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

#[tokio::test]
async fn scalar_compaction_preserves_descriptors_and_rebuilds_each_output_reference_set() {
    for pack_size in ["1 b", "128 mb"] {
        let table = table(&[
            ("blob.target-file-size", pack_size),
            ("target-file-row-num", "2"),
        ])
        .await;
        let initial = write(
            &table,
            &scalar_batch(&[
                (1, Some(b"old")),
                (2, Some(b"survivor")),
                (3, Some(b"deleted")),
            ]),
        )
        .await;
        let first = scalar_descriptors(&physical(&table, 0, &initial[0].new_files[0]).await);
        let updates = write(
            &table,
            &scalar_batch_with_kinds(&[
                (1, Some(b"new"), 2),
                (3, Some(b"ignored-delete"), 3),
                (4, None, 0),
            ]),
        )
        .await;
        let second = scalar_descriptors(&physical(&table, 0, &updates[0].new_files[0]).await);
        let packs = pack_bytes(&table).await;
        let results = compact(&table).await;
        let expected = BTreeMap::from([
            (1, second[&1].clone()),
            (2, first[&2].clone()),
            (3, None),
            (4, None),
        ]);
        let mut actual = BTreeMap::new();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].files().len(), 2);
        for file in results[0].files() {
            let descriptors = scalar_descriptors(&physical(&table, 0, file).await);
            assert_references(
                &table,
                0,
                file,
                &descriptors.values().flatten().cloned().collect::<Vec<_>>(),
            )
            .await;
            actual.extend(descriptors);
        }
        assert_eq!(actual, expected);
        assert_eq!(pack_bytes(&table).await, packs);
        assert_eq!(
            read_scalar_rows(&table).await,
            vec![
                (1, Some(b"new".to_vec())),
                (2, Some(b"survivor".to_vec())),
                (4, None)
            ]
        );
    }
}

#[tokio::test]
async fn collection_compaction_preserves_merge_winners_nulls_and_empty_collections() {
    for engine in ["deduplicate", "partial-update", "first-row"] {
        let table = table_with_fields(
            vec![
                (
                    "items",
                    DataType::Array(ArrayType::new(DataType::Blob(BlobType::new()))),
                ),
                (
                    "named",
                    DataType::Map(MapType::new(
                        DataType::VarChar(VarCharType::string_type()),
                        DataType::Blob(BlobType::new()),
                    )),
                ),
            ],
            &[
                ("merge-engine", engine),
                ("target-file-row-num", "2"),
                ("blob.target-file-size", "1 b"),
            ],
        )
        .await;
        let batch = nested_batch(&table);
        let initial = write(&table, &batch).await;
        let mut expected = written_rows(&table, &initial).await;
        let mut columns = batch.columns().to_vec();
        columns[0] = Arc::new(Int32Array::from(vec![4, 5, 6, 7]));
        let updates = write(
            &table,
            &RecordBatch::try_new(batch.schema(), columns).unwrap(),
        )
        .await;
        for (key, row) in written_rows(&table, &updates).await {
            if engine != "first-row" || !expected.contains_key(&key) {
                expected.insert(key, row);
            }
        }
        let packs = pack_bytes(&table).await;
        let results = compact(&table).await;
        let mut actual = BTreeMap::new();
        for result in &results {
            for file in result.files() {
                let batch = physical(&table, 0, file).await;
                let items = batch
                    .column_by_name("items")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap();
                let named = batch
                    .column_by_name("named")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<MapArray>()
                    .unwrap();
                let mut descriptors = Vec::new();
                for row in 0..batch.num_rows() {
                    descriptors.extend(array_values(items, row).into_iter().flatten().flatten());
                    descriptors.extend(
                        map_values(named, row)
                            .into_iter()
                            .flatten()
                            .filter_map(|(_, value)| value),
                    );
                }
                assert_references(&table, 0, file, &descriptors).await;
                actual.extend(value_rows(&table, &batch));
            }
        }
        assert_eq!(actual, expected, "{engine}");
        assert_eq!(pack_bytes(&table).await, packs);
        assert_eq!(
            read_batches(&table)
                .await
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            7
        );
    }
}

#[tokio::test]
async fn partial_update_compaction_retains_blob_fields_from_different_input_files() {
    let table = table_with_fields(
        vec![
            ("payload", DataType::Blob(BlobType::new())),
            ("other", DataType::Blob(BlobType::new())),
        ],
        &[("merge-engine", "partial-update")],
    )
    .await;
    let batch = |payload: Option<&[u8]>, other: Option<&[u8]>| {
        RecordBatch::try_new(
            crate::arrow::build_target_arrow_schema(table.schema().fields()).unwrap(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(LargeBinaryArray::from(vec![payload])),
                Arc::new(LargeBinaryArray::from(vec![other])),
            ],
        )
        .unwrap()
    };
    let initial = write(&table, &batch(Some(b"retained"), Some(b"old"))).await;
    let updates = write(&table, &batch(None, Some(b"replacement"))).await;
    let first = physical(&table, 0, &initial[0].new_files[0]).await;
    let second = physical(&table, 0, &updates[0].new_files[0]).await;
    let binary = |batch: &RecordBatch, name: &str| {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap()
            .value(0)
            .to_vec()
    };
    let expected = vec![binary(&first, "payload"), binary(&second, "other")];
    let packs = pack_bytes(&table).await;
    let results = compact(&table).await;
    assert_eq!(results[0].files().len(), 1);
    let file = &results[0].files()[0];
    let output = physical(&table, 0, file).await;
    assert_eq!(
        vec![binary(&output, "payload"), binary(&output, "other")],
        expected
    );
    assert_references(&table, 0, file, &expected).await;
    let rows = read_batches(&table).await;
    assert_eq!(binary(&rows[0], "payload"), b"retained");
    assert_eq!(binary(&rows[0], "other"), b"replacement");
    assert_eq!(pack_bytes(&table).await, packs);
}

#[tokio::test]
async fn first_row_compaction_reuses_retained_positive_level_blob_history() {
    let table = table(&[("merge-engine", "first-row")]).await;
    write(&table, &scalar_batch(&[(1, Some(b"first"))])).await;
    compact(&table).await;
    write(
        &table,
        &scalar_batch(&[(1, Some(b"second")), (2, Some(b"other"))]),
    )
    .await;
    let packs = pack_bytes(&table).await;
    compact(&table).await;
    assert_eq!(
        read_scalar_rows(&table).await,
        vec![(1, Some(b"first".to_vec())), (2, Some(b"other".to_vec()))]
    );
    assert_eq!(pack_bytes(&table).await, packs);
}

#[tokio::test]
async fn automatic_blob_compaction_uses_the_existing_merge_and_bucket_paths() {
    for engine in ["deduplicate", "partial-update", "first-row"] {
        for bucket in ["1", "-1"] {
            let table = table(&[
                ("write-only", "false"),
                ("merge-engine", engine),
                ("bucket", bucket),
                ("dynamic-bucket.target-row-num", "1"),
                ("num-sorted-run.compaction-trigger", "2"),
                ("compaction.min.file-num", "2"),
                ("compaction.max-size-amplification-percent", "0"),
            ])
            .await;
            write(
                &table,
                &scalar_batch(&[(1, Some(b"old")), (2, Some(b"unchanged"))]),
            )
            .await;
            write(&table, &scalar_batch(&[(1, Some(b"new"))])).await;
            assert_eq!(
                latest(&table).await.commit_kind(),
                &CommitKind::COMPACT,
                "{engine}, {bucket}"
            );
            let expected = if engine == "first-row" {
                b"old"
            } else {
                b"new"
            };
            assert_eq!(
                read_scalar_rows(&table).await,
                vec![
                    (1, Some(expected.to_vec())),
                    (2, Some(b"unchanged".to_vec()))
                ]
            );
            let plan = table.new_read_builder().new_scan().plan().await.unwrap();
            if bucket == "-1" {
                assert!(plan.splits().iter().any(|split| split.bucket() > 0));
            }
            let mut compacted_files = 0;
            for split in plan.splits() {
                for file in split.data_files() {
                    if file.file_source == Some(1) {
                        assert!(file.level > 0);
                        compacted_files += 1;
                    }
                    let batch = physical(&table, split.bucket(), file).await;
                    let descriptors = scalar_descriptors(&batch)
                        .into_values()
                        .flatten()
                        .collect::<Vec<_>>();
                    assert_references(&table, split.bucket(), file, &descriptors).await;
                }
            }
            assert!(compacted_files > 0);
        }
    }
}

#[tokio::test]
async fn mixed_inline_descriptor_and_view_fields_remain_outside_managed_references() {
    let table = table_with_fields(
        vec![
            ("payload", DataType::Blob(BlobType::new())),
            ("descriptor", DataType::Blob(BlobType::new())),
            ("view", DataType::Blob(BlobType::new())),
        ],
        &[
            ("blob-descriptor-field", "descriptor"),
            ("blob-view-field", "view"),
        ],
    )
    .await;
    let descriptor =
        BlobDescriptor::new("memory:/upstream/external.managed.blob".into(), 17, 42).serialize();
    let view = crate::spec::BlobViewStruct::new(Identifier::new("upstream", "media"), 2, 123)
        .serialize()
        .unwrap();
    let batch = RecordBatch::try_new(
        crate::arrow::build_target_arrow_schema(table.schema().fields()).unwrap(),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(LargeBinaryArray::from(vec![Some(b"managed".as_slice())])),
            Arc::new(LargeBinaryArray::from(vec![Some(descriptor.as_slice())])),
            Arc::new(LargeBinaryArray::from(vec![Some(view.as_slice())])),
        ],
    )
    .unwrap();
    let messages = write(&table, &batch).await;
    let expected = written_rows(&table, &messages).await;
    let packs = pack_bytes(&table).await;
    let results = compact(&table).await;
    let file = &results[0].files()[0];
    let output = physical(&table, 0, file).await;
    assert_eq!(value_rows(&table, &output), expected);
    assert_references(
        &table,
        0,
        file,
        &scalar_descriptors(&output)
            .into_values()
            .flatten()
            .collect::<Vec<_>>(),
    )
    .await;
    assert_eq!(pack_bytes(&table).await, packs);
}

#[tokio::test]
async fn deduplicate_blob_compaction_preserves_existing_deletion_vector_behavior() {
    let table = table(&[("deletion-vectors.enabled", "true")]).await;
    write(
        &table,
        &scalar_batch(&[(1, Some(b"old")), (2, Some(b"deleted"))]),
    )
    .await;
    compact(&table).await;
    write(
        &table,
        &scalar_batch_with_kinds(&[(1, Some(b"new"), 2), (2, None, 3)]),
    )
    .await;
    let packs = pack_bytes(&table).await;
    let results = compact(&table).await;
    for result in &results {
        for file in result.files() {
            assert_eq!(file.delete_row_count, Some(0));
            let batch = physical(&table, 0, file).await;
            let descriptors = scalar_descriptors(&batch)
                .into_values()
                .flatten()
                .collect::<Vec<_>>();
            assert_references(&table, 0, file, &descriptors).await;
        }
    }
    assert_eq!(
        read_scalar_rows(&table).await,
        vec![(1, Some(b"new".to_vec()))]
    );
    assert_eq!(pack_bytes(&table).await, packs);
}

#[tokio::test]
async fn aborting_unsubmitted_blob_compaction_only_removes_owned_output_files() {
    let table = table(&[("target-file-row-num", "1")]).await;
    write(
        &table,
        &scalar_batch(&[(1, Some(b"old")), (2, Some(b"keep"))]),
    )
    .await;
    write(&table, &scalar_batch(&[(1, Some(b"new"))])).await;
    let before = paths(&table).await;
    let packs = pack_bytes(&table).await;
    let results = execute(&table).await;
    assert!(paths(&table).await.len() > before.len());
    table
        .new_compact_builder()
        .new_commit()
        .unwrap()
        .abort_compact(&results)
        .await
        .unwrap();
    assert_eq!(paths(&table).await, before);
    assert_eq!(pack_bytes(&table).await, packs);
    assert_eq!(latest(&table).await.id(), 2);
    assert_eq!(
        read_scalar_rows(&table).await,
        vec![(1, Some(b"new".to_vec())), (2, Some(b"keep".to_vec()))]
    );
}

#[tokio::test]
async fn blob_compaction_cleans_rolled_sidecars_after_a_late_input_failure() {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("payload", DataType::Blob(BlobType::new()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("write-only", "true")
        .option("read.batch-size", "1")
        .option("write-buffer-size", "1 b")
        .option("target-file-row-num", "1")
        .build()
        .unwrap();
    let (table, io) =
        crate::table::maintenance_test_utils::table(TableSchema::new(0, &schema)).await;
    write(&table, &scalar_batch(&[(1, Some(b"first"))])).await;
    // The merge reader looks ahead before emitting the preceding key.
    write(&table, &scalar_batch(&[(2, Some(b"second"))])).await;
    let messages = write(&table, &scalar_batch(&[(3, Some(b"third"))])).await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    table
        .file_io()
        .delete_file(&format!(
            "{}/{}",
            bucket_path_under(table.location(), "", 0),
            messages[0].new_files[0].file_name
        ))
        .await
        .unwrap();
    let before = paths(&table).await;
    let packs = pack_bytes(&table).await;
    assert!(builder
        .new_executor()
        .unwrap()
        .execute(&plan.tasks()[0])
        .await
        .is_err());
    assert!(io
        .observed_paths()
        .iter()
        .any(|path| path.ends_with(".blobref") && !before.contains(path)));
    assert_eq!(paths(&table).await, before);
    assert_eq!(pack_bytes(&table).await, packs);
    assert_eq!(latest(&table).await.id(), 3);
}

#[tokio::test]
async fn blob_compaction_cleans_partial_sidecars_and_previously_rolled_files() {
    use crate::io::{FileIOBuilder, FileIOProvider};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    #[derive(Debug)]
    struct FailSidecar {
        operator: opendal::Operator,
        remaining: AtomicUsize,
        attempted: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl FileIOProvider for FailSidecar {
        async fn create(&self, path: &str) -> crate::Result<(opendal::Operator, String)> {
            let relative = path.strip_prefix("memory:/").unwrap().to_string();
            if path.ends_with(".blobref") {
                if let Ok(remaining) =
                    self.remaining
                        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
                            count.checked_sub(1)
                        })
                {
                    let previous = {
                        let mut attempted = self.attempted.lock().unwrap();
                        let previous = attempted.last().cloned();
                        attempted.push(relative.clone());
                        previous
                    };
                    if remaining == 1 {
                        if let Some(previous) = previous {
                            // The earlier file's real sidecar must exist before failure.
                            assert!(self.operator.stat(&previous).await.is_ok());
                        }
                        // Model a failed write that left a partial object behind.
                        self.operator
                            .write(&relative, bytes::Bytes::from_static(b"partial blobref"))
                            .await?;
                        return Err(crate::Error::UnexpectedError {
                            message: "Injected partial blobref write failure".into(),
                            source: None,
                        });
                    }
                }
            }
            Ok((self.operator.clone(), relative))
        }
    }

    for fail_on in [1, 2] {
        let provider = Arc::new(FailSidecar {
            operator: opendal::Operator::from_config(opendal::services::MemoryConfig::default())
                .unwrap(),
            remaining: AtomicUsize::new(0),
            attempted: Mutex::new(Vec::new()),
        });
        let io = FileIOBuilder::new("memory")
            .with_provider(provider.clone())
            .build()
            .unwrap();
        let path = format!("memory:/blobref_failure/{}", uuid::Uuid::new_v4());
        let table = scalar_table(
            &io,
            &path,
            &[("write-only", "true"), ("target-file-row-num", "1")],
        );
        setup_dirs(&io, &path).await;
        io.new_output(&table.schema_manager().schema_path(0))
            .unwrap()
            .write(serde_json::to_vec(table.schema()).unwrap().into())
            .await
            .unwrap();
        write(
            &table,
            &scalar_batch(&[(1, Some(b"first")), (2, Some(b"second"))]),
        )
        .await;
        let before = paths(&table).await;
        let packs = pack_bytes(&table).await;
        let builder = table.new_compact_builder();
        let plan = builder.new_planner().unwrap().plan().await.unwrap();
        provider.remaining.store(fail_on, Ordering::SeqCst);
        let error = builder
            .new_executor()
            .unwrap()
            .execute(&plan.tasks()[0])
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("Injected partial blobref write failure"));
        assert_eq!(provider.attempted.lock().unwrap().len(), fail_on);
        assert_eq!(provider.remaining.load(Ordering::SeqCst), 0);
        assert_eq!(paths(&table).await, before);
        assert_eq!(pack_bytes(&table).await, packs);
        assert_eq!(latest(&table).await.id(), 1);
        assert_eq!(
            read_scalar_rows(&table).await,
            vec![(1, Some(b"first".to_vec())), (2, Some(b"second".to_vec()))]
        );
    }
}

#[tokio::test]
async fn blob_compaction_preserves_snapshot_reads_retries_and_stale_input_conflicts() {
    let table = table(&[]).await;
    write(&table, &scalar_batch(&[(1, Some(b"old"))])).await;
    let old = table.copy_with_pinned_snapshot(Some(&latest(&table).await));
    write(&table, &scalar_batch(&[(1, Some(b"new"))])).await;
    let results = execute(&table).await;
    let stale = execute(&table).await;
    let builder = table.new_compact_builder();
    let commit = builder.new_commit().unwrap();
    let metrics = commit.commit_compact(results.clone()).await.unwrap();
    let snapshot = latest(&table).await;
    assert_eq!(commit.commit_compact(results).await.unwrap(), metrics);
    assert_eq!(latest(&table).await.id(), snapshot.id());
    assert!(commit.commit_compact(stale).await.is_err());
    assert_eq!(latest(&table).await.id(), snapshot.id());
    assert_eq!(
        read_scalar_rows(&old).await,
        vec![(1, Some(b"old".to_vec()))]
    );
    assert_eq!(
        read_scalar_rows(&table).await,
        vec![(1, Some(b"new".to_vec()))]
    );
}

#[tokio::test]
async fn blob_compaction_keeps_existing_configuration_boundaries() {
    let table = table(&[]).await;
    for options in [
        vec![("merge-engine", "aggregation")],
        vec![("changelog-producer", "input")],
        vec![("changelog-producer", "lookup")],
        vec![("data-file.external-paths", "memory:/external")],
        vec![("pk-clustering-override", "true")],
        vec![("bucket", "-2")],
        vec![("row-tracking.enabled", "true")],
        vec![("blob-descriptor-field", "payload")],
        vec![
            ("deletion-vectors.enabled", "true"),
            ("merge-engine", "partial-update"),
        ],
    ] {
        let table = table.copy_with_options(
            options
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect::<HashMap<_, _>>(),
        );
        assert!(
            table.new_compact_builder().new_planner().is_err(),
            "{:?}",
            table.schema().options()
        );
    }
    let first_row =
        table.copy_with_options(HashMap::from([("merge-engine".into(), "first-row".into())]));
    let without_cache = Table::new(
        test_file_io(),
        first_row.identifier().clone(),
        first_row.location().into(),
        first_row.schema().clone(),
        None,
    );
    assert!(
        matches!(without_cache.new_compact_builder().new_executor(), Err(crate::Error::ConfigInvalid { message }) if message.contains("local-cache"))
    );
}

#[tokio::test]
async fn blob_compaction_does_not_open_payload_packs_or_input_reference_sidecars() {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("payload", DataType::Blob(BlobType::new()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("write-only", "true")
        .build()
        .unwrap();
    let (table, io) =
        crate::table::maintenance_test_utils::table(TableSchema::new(0, &schema)).await;
    write(
        &table,
        &scalar_batch(&[(1, Some(b"old")), (2, Some(b"keep"))]),
    )
    .await;
    write(&table, &scalar_batch(&[(1, Some(b"new"))])).await;
    io.watch(
        paths(&table)
            .await
            .into_iter()
            .filter(|path| path.ends_with(".managed.blob") || path.ends_with(".blobref"))
            .collect(),
    );
    compact(&table).await;
    assert!(!io.counts().is_empty());
    assert!(
        io.counts().values().all(|count| *count == 0),
        "{:?}",
        io.counts()
    );
}
