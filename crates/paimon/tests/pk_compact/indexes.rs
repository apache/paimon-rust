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
use paimon::spec::{should_read_pk_index_source, IndexManifest, PrimaryKeyIndexSourceMeta};
use std::collections::HashSet;

#[tokio::test]
async fn independently_committed_buckets_rebase_without_conflicting_local_ordinals() {
    let table = table(&[("bucket", "2"), ("pk-btree.index.columns", "value")]).await;
    write_batch(&table, &make_batch((0..100).collect(), (0..100).collect())).await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    assert_eq!(plan.tasks().len(), 2);
    let results = execute(&table, &plan).await;
    let commit = builder.new_commit().unwrap();
    commit
        .commit_compact(vec![results[0].clone()])
        .await
        .unwrap();
    let first = indexes(&table).await;
    commit
        .commit_compact(vec![results[1].clone()])
        .await
        .unwrap();
    let active = indexes(&table).await;
    assert_eq!(active.len(), 2);
    assert!(
        active.contains(&first[0]),
        "concurrent unrelated bucket index must survive"
    );
    assert_ne!(active[0].bucket, active[1].bucket);
    assert!(active.iter().all(|e| e
        .index_file
        .global_index_meta
        .as_ref()
        .unwrap()
        .row_range_start
        == 0));
    for value in [0, 42, 99] {
        assert_eq!(filtered_rows(&table, value).await, vec![(value, value)]);
    }
    commit
        .commit_compact(vec![results[0].clone()])
        .await
        .unwrap();
    commit
        .commit_compact(vec![results[1].clone()])
        .await
        .unwrap();
    assert_eq!(latest(&table).await.id(), 3);
}

#[tokio::test]
async fn concurrently_added_compacted_source_requires_a_new_index_build() {
    let table = table(&[("pk-btree.index.columns", "value")]).await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let results = execute(&table, &plan).await;
    let write = table.new_write_builder();
    let mut writer = write.new_write().unwrap();
    writer
        .write_arrow_batch(&make_batch(vec![256], vec![2560]))
        .await
        .unwrap();
    let mut messages = writer.prepare_commit().await.unwrap();
    for message in &mut messages {
        for file in &mut message.new_files {
            file.level = results[0].files()[0].level;
            file.file_source = Some(1);
        }
    }
    write.new_commit().commit(messages).await.unwrap();
    let error = builder
        .new_commit()
        .unwrap()
        .commit_compact(results.clone())
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("complete compacted level"),
        "{error}"
    );
    assert_eq!(latest(&table).await.id(), 2);
    assert!(indexes(&table).await.is_empty());
    for entry in results[0].index_entries().unwrap() {
        assert!(table
            .file_io()
            .exists(&index_path(&table, entry))
            .await
            .unwrap());
    }
}

#[tokio::test]
async fn removed_declaration_still_retires_indexes_whose_sources_are_rewritten() {
    let table = table(&[("pk-btree.index.columns", "value")]).await;
    write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
    compact(&table).await;
    let old = indexes(&table).await[0].clone();
    let schema = table
        .schema()
        .apply_changes(vec![paimon::spec::SchemaChange::RemoveOption {
            key: "pk-btree.index.columns".into(),
        }])
        .unwrap();
    persist_table_schema(table.file_io(), table.location(), &schema).await;
    let current = table.copy_with_resolved_schema(schema, "main").unwrap();
    write_batch(&current, &make_batch(vec![1, 2], vec![11, 22])).await;
    compact(&current).await;
    assert!(indexes(&current).await.is_empty());
    assert!(table
        .file_io()
        .exists(&index_path(&table, &old))
        .await
        .unwrap());
    assert_eq!(rows(&current).await, vec![(1, 11), (2, 22)]);
}

#[tokio::test]
async fn failed_index_stat_cleans_current_prior_indexes_and_private_data_outputs() {
    use std::collections::{BTreeSet, HashMap};
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    };
    #[derive(Debug)]
    struct FailIndexStat {
        root: String,
        op: opendal::Operator,
        calls: Mutex<HashMap<String, usize>>,
        failed: AtomicBool,
    }
    #[async_trait::async_trait]
    impl paimon::io::FileIOProvider for FailIndexStat {
        async fn create(&self, path: &str) -> paimon::Result<(opendal::Operator, String)> {
            if path.contains("bitmap-pk-index-") {
                let mut calls = self.calls.lock().unwrap();
                let count = calls.entry(path.into()).or_default();
                *count += 1;
                if *count == 2 {
                    self.failed.store(true, Ordering::SeqCst);
                    return Err(paimon::Error::UnexpectedError {
                        message: "injected PK payload stat failure".into(),
                        source: None,
                    });
                }
            }
            Ok((
                self.op.clone(),
                path.strip_prefix(&self.root)
                    .unwrap()
                    .trim_start_matches('/')
                    .into(),
            ))
        }
    }
    fn files(root: &std::path::Path) -> BTreeSet<std::path::PathBuf> {
        fn visit(path: &std::path::Path, output: &mut BTreeSet<std::path::PathBuf>) {
            for entry in std::fs::read_dir(path).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    visit(&path, output);
                } else {
                    output.insert(path);
                }
            }
        }
        let mut output = BTreeSet::new();
        visit(root, &mut output);
        output
    }
    let dir = tempfile::tempdir().unwrap();
    let location = dir.path().to_str().unwrap().to_string();
    let mut config = opendal_service_fs::FsConfig::default();
    config.root = Some(location.clone());
    let provider = Arc::new(FailIndexStat {
        root: location.clone(),
        op: opendal::Operator::from_config(config).unwrap(),
        calls: Mutex::new(HashMap::new()),
        failed: AtomicBool::new(false),
    });
    let io = paimon::io::FileIOBuilder::new("file")
        .with_provider(provider.clone())
        .build()
        .unwrap();
    let seed_io = paimon::io::FileIOBuilder::new("file").build().unwrap();
    let table = Table::new(
        seed_io.clone(),
        paimon::catalog::Identifier::new("default", "cleanup"),
        location.clone(),
        pk_schema(&[
            ("write-only", "true"),
            ("pk-btree.index.columns", "id"),
            ("pk-bitmap.index.columns", "value"),
        ]),
        None,
    );
    setup_dirs(&seed_io, &location).await;
    persist_table_schema(&seed_io, &location, table.schema()).await;
    write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
    let table = Table::new(
        io,
        table.identifier().clone(),
        location,
        table.schema().clone(),
        None,
    );
    let before = files(dir.path());
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let error = builder
        .new_executor()
        .unwrap()
        .execute(&plan.tasks()[0])
        .await
        .expect_err("stat must fail");
    assert!(
        error
            .to_string()
            .contains("injected PK payload stat failure"),
        "{error}"
    );
    assert!(provider.failed.load(Ordering::SeqCst));
    assert_eq!(
        files(dir.path()),
        before,
        "all unpublished data and index outputs must be removed"
    );
    assert_eq!(latest(&table).await.id(), 1);
    assert_eq!(rows(&table).await, vec![(1, 10), (2, 20)]);
}

#[tokio::test]
async fn retained_history_publishes_new_pk_index_and_deletion_vectors_together() {
    let table = table(&[
        ("pk-btree.index.columns", "value"),
        ("deletion-vectors.enabled", "true"),
        ("deletion-vectors.merge-on-read", "false"),
    ])
    .await;
    write_batch(
        &table,
        &make_batch((0..1000).collect(), (0..1000).collect()),
    )
    .await;
    compact(&table).await;
    let original = indexes(&table).await[0].clone();
    let historical = table.copy_with_pinned_snapshot(Some(&latest(&table).await));
    write_batch(&table, &make_batch(vec![42], vec![42000])).await;
    compact(&table).await;
    let active = indexes(&table).await;
    assert_eq!(
        active
            .iter()
            .filter(|e| e.index_file.index_type == "btree")
            .count(),
        2
    );
    assert!(active.contains(&original));
    assert!(active
        .iter()
        .any(|e| e.index_file.index_type == "DELETION_VECTORS"));
    assert_eq!(filtered_rows(&table, 42).await, vec![]);
    assert_eq!(filtered_rows(&table, 42000).await, vec![(42, 42000)]);
    assert_eq!(filtered_rows(&historical, 42).await, vec![(42, 42)]);
}

#[tokio::test]
async fn missing_payload_and_tampered_source_cannot_publish_and_do_not_abort_outputs() {
    for case in ["missing", "omitted", "source"] {
        let table = table(&[("pk-btree.index.columns", "value")]).await;
        write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
        let builder = table.new_compact_builder();
        let plan = builder.new_planner().unwrap().plan().await.unwrap();
        let results = execute(&table, &plan).await;
        let path = index_path(&table, &results[0].index_entries().unwrap()[0]);
        let results = if case == "missing" {
            table.file_io().delete_file(&path).await.unwrap();
            results
        } else {
            let mut json = serde_json::to_value(&results).unwrap();
            if case == "omitted" {
                json[0]["index_entries"] = serde_json::json!([]);
            } else {
                let source = PrimaryKeyIndexSourceMeta::new(
                    results[0].files()[0].level,
                    vec![paimon::spec::PrimaryKeyIndexSourceFile::new(
                        "wrong-source.parquet".into(),
                        2,
                    )
                    .unwrap()],
                )
                .unwrap();
                json[0]["index_entries"][0]["_GLOBAL_INDEX"]["_SOURCE_META"] =
                    serde_json::to_value(source.serialize().unwrap()).unwrap();
            }
            serde_json::from_value(json).unwrap()
        };
        let error = builder
            .new_commit()
            .unwrap()
            .commit_compact(results.clone())
            .await
            .unwrap_err();
        let expected = match case {
            "missing" => "does not exist",
            "omitted" => "Missing PK index output",
            _ => "complete compacted level",
        };
        assert!(error.to_string().contains(expected), "{error}");
        assert_eq!(latest(&table).await.id(), 1);
        for file in results[0].files() {
            assert!(table
                .file_io()
                .exists(&format!("{}/bucket-0/{}", table.location(), file.file_name))
                .await
                .unwrap());
        }
        assert_eq!(
            table.file_io().exists(&path).await.unwrap(),
            case != "missing"
        );
    }
}

#[tokio::test]
async fn malformed_declarations_fail_before_automatic_data_publication() {
    for (key, value) in [
        ("pk-btree.index.columns", "value,value"),
        ("pk-bitmap.index.columns", "missing"),
        (
            "fields.value.pk-btree.index.options",
            r#"{"block-size":[]}"#,
        ),
        (
            "fields.value.pk-btree.index.options",
            r#"{"btree-index.block-size":"1kb","block-size":"2kb"}"#,
        ),
    ] {
        let original = table(&[("pk-btree.index.columns", "value"), (key, value)]).await;
        let table = original
            .copy_with_resolved_schema(
                original
                    .schema()
                    .copy_with_options(std::collections::HashMap::from([(
                        "write-only".into(),
                        "false".into(),
                    )])),
                "main",
            )
            .unwrap();
        persist_table_schema(table.file_io(), table.location(), table.schema()).await;
        let write = table.new_write_builder();
        let mut writer = write.new_write().unwrap();
        writer
            .write_arrow_batch(&make_batch(vec![1], vec![10]))
            .await
            .unwrap();
        let error = write
            .new_commit()
            .commit(writer.prepare_commit().await.unwrap())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("index"), "{error}");
        assert_eq!(
            table
                .snapshot_manager()
                .get_latest_snapshot_id()
                .await
                .unwrap(),
            None
        );
    }
}

#[tokio::test]
async fn all_native_families_build_with_physical_null_gaps_and_field_options() {
    use arrow_array::{
        builder::{Float32Builder, Int32Builder, ListBuilder},
        ArrayRef,
    };
    use paimon::spec::{ArrayType, FloatType};
    use std::sync::Arc;
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .column(
            "tags",
            DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
        )
        .column("body", DataType::VarChar(VarCharType::string_type()))
        .column("text", DataType::VarChar(VarCharType::string_type()))
        .column(
            "embedding",
            DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
        )
        .primary_key(["id"])
        .option("bucket", "1")
        .option("deletion-vectors.enabled", "true")
        .option("deletion-vectors.merge-on-read", "false")
        .option("pk-btree.index.columns", "value")
        .option("pk-multivalue.index.columns", "tags")
        .option("pk-fm.index.columns", "body")
        .option("pk-full-text.index.columns", "text")
        .option("pk-vector.index.columns", "embedding")
        .option("fields.embedding.pk-vector.index.type", "ivf-flat")
        .option("fields.embedding.pk-vector.distance.metric", "l2")
        .option(
            "fields.embedding.pk-vector.index.options",
            r#"{"dimension":2,"nlist":1}"#,
        )
        .option(
            "fields.text.pk-full-text.index.options",
            r#"{"lower-case":false}"#,
        )
        .option("global-index.search-mode", "fast")
        .option("target-file-row-num", "1")
        .build()
        .unwrap();
    let table = table_with_schema(TableSchema::new(0, &schema)).await;
    let mut tags = ListBuilder::new(Int32Builder::new()).with_field(Arc::new(
        arrow_schema::Field::new("element", arrow_schema::DataType::Int32, true),
    ));
    let mut vectors = ListBuilder::new(Float32Builder::new()).with_field(Arc::new(
        arrow_schema::Field::new("element", arrow_schema::DataType::Float32, true),
    ));
    for row in 0..3 {
        if row == 1 {
            tags.append(false);
            vectors.append(false);
        } else {
            tags.values().append_value(row);
            tags.values().append_value(row);
            tags.append(true);
            vectors.values().append_value(row as f32);
            vectors.values().append_value(0.);
            vectors.append(true);
        }
    }
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
        (
            "value",
            Arc::new(Int32Array::from(vec![Some(10), None, Some(30)])) as ArrayRef,
        ),
        ("tags", Arc::new(tags.finish()) as ArrayRef),
        (
            "body",
            Arc::new(StringArray::from(vec![Some("alpha"), None, Some("beta")])) as ArrayRef,
        ),
        (
            "text",
            Arc::new(StringArray::from(vec![Some("Needle"), None, Some("hay")])) as ArrayRef,
        ),
        ("embedding", Arc::new(vectors.finish()) as ArrayRef),
    ])
    .unwrap();
    write_batch(&table, &batch).await;
    compact(&table).await;
    let active = indexes(&table).await;
    assert_eq!(
        active
            .iter()
            .map(|e| e.index_file.index_type.as_str())
            .collect::<HashSet<_>>(),
        HashSet::from(["btree", "multivalue", "fm", "full-text", "ivf-flat"])
    );
    for entry in &active {
        let source = PrimaryKeyIndexSourceMeta::from_global_index_meta(
            entry.index_file.global_index_meta.as_ref().unwrap(),
        )
        .unwrap();
        assert_eq!(entry.index_file.row_count, 3);
        assert_eq!(source.source_files().len(), 3);
        assert!(table
            .file_io()
            .exists(&index_path(&table, entry))
            .await
            .unwrap());
    }
    let vector = table
        .new_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vector(vec![2., 0.])
        .with_limit(2)
        .execute()
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = vector
        .new_read_builder()
        .read()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let ids = batches
        .iter()
        .flat_map(|b| {
            b.column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        ids,
        vec![3, 1],
        "NULL vector must leave a physical ordinal gap"
    );
    for (term, expected) in [("Needle", vec![1]), ("needle", vec![])] {
        let query = serde_json::json!({"match":{"query":term}}).to_string();
        let text = table
            .new_full_text_search_builder()
            .with_text_column("text")
            .with_query_text(&query)
            .with_limit(3)
            .execute()
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = text
            .new_read_builder()
            .read()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let ids = batches
            .iter()
            .flat_map(|b| {
                b.column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            ids, expected,
            "field tokenizer options must survive native serialization"
        );
    }
}

#[tokio::test]
async fn vector_definition_changes_replace_old_payloads_atomically() {
    use arrow_array::{
        builder::{Float32Builder, ListBuilder},
        ArrayRef,
    };
    use paimon::spec::{ArrayType, FloatType};
    use std::sync::Arc;
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            "embedding",
            DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
        )
        .primary_key(["id"])
        .option("bucket", "1")
        .option("pk-vector.index.columns", "embedding")
        .option("deletion-vectors.enabled", "true")
        .option("deletion-vectors.merge-on-read", "false")
        .option("fields.embedding.pk-vector.index.type", "ivf-flat")
        .option("fields.embedding.pk-vector.distance.metric", "l2")
        .option(
            "fields.embedding.pk-vector.index.options",
            r#"{"dimension":2,"nlist":1}"#,
        )
        .build()
        .unwrap();
    for (kind, metric) in [("ivf-flat", "inner_product"), ("ivf-sq", "l2")] {
        let table = table_with_schema(TableSchema::new(0, &schema)).await;
        let batch = || {
            let mut vectors = ListBuilder::new(Float32Builder::new()).with_field(Arc::new(
                arrow_schema::Field::new("element", arrow_schema::DataType::Float32, true),
            ));
            for value in [1., 100.] {
                vectors.values().append_slice(&[value, 0.]);
                vectors.append(true);
            }
            RecordBatch::try_from_iter([
                ("id", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
                ("embedding", Arc::new(vectors.finish()) as ArrayRef),
            ])
            .unwrap()
        };
        write_batch(&table, &batch()).await;
        compact(&table).await;
        let old = indexes(&table).await;
        let evolved = table
            .schema()
            .apply_changes(vec![
                paimon::spec::SchemaChange::set_option(
                    "fields.embedding.pk-vector.index.type".into(),
                    kind.into(),
                ),
                paimon::spec::SchemaChange::set_option(
                    "fields.embedding.pk-vector.distance.metric".into(),
                    metric.into(),
                ),
            ])
            .unwrap();
        persist_table_schema(table.file_io(), table.location(), &evolved).await;
        let current = table.copy_with_resolved_schema(evolved, "main").unwrap();
        write_batch(&current, &batch()).await;
        compact(&current).await;
        let active = indexes(&current).await;
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].index_file.index_type, kind);
        assert_ne!(active[0].index_file.file_name, old[0].index_file.file_name);
        assert!(table
            .file_io()
            .exists(&index_path(&table, &old[0]))
            .await
            .unwrap());
        let result = current
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1., 0.])
            .with_limit(1)
            .execute()
            .await
            .unwrap();
        let batches: Vec<RecordBatch> = result
            .new_read_builder()
            .read()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let id = batches[0]
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0);
        assert_eq!(id, if metric == "inner_product" { 2 } else { 1 });
    }
}

#[tokio::test]
async fn automatic_compaction_maintains_indexes_and_recovers_callback_failure() {
    use paimon::table::{CommitCallback, CommitCallbackContext};
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    struct FailOnce(AtomicBool);
    #[async_trait::async_trait]
    impl CommitCallback for FailOnce {
        async fn call(&self, context: &CommitCallbackContext) -> paimon::Result<()> {
            if context.snapshot.commit_kind() == &CommitKind::COMPACT
                && !self.0.swap(true, Ordering::SeqCst)
            {
                return Err(paimon::Error::UnexpectedError {
                    message: "pk-index callback failure".into(),
                    source: None,
                });
            }
            Ok(())
        }
    }
    let original = table(&[
        ("pk-bitmap.index.columns", "value"),
        ("compaction.force-up-level-0", "true"),
    ])
    .await;
    let table = original
        .copy_with_resolved_schema(
            original
                .schema()
                .copy_with_options(std::collections::HashMap::from([(
                    "write-only".into(),
                    "false".into(),
                )])),
            "main",
        )
        .unwrap();
    persist_table_schema(table.file_io(), table.location(), table.schema()).await;
    let write = table.new_write_builder();
    let mut writer = write.new_write().unwrap();
    writer
        .write_arrow_batch(&make_batch(vec![1, 2], vec![10, 20]))
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    let mut commit = write.new_commit();
    commit
        .add_commit_callback(Arc::new(FailOnce(AtomicBool::new(false))))
        .unwrap();
    assert!(commit
        .commit_with_identifier(messages.clone(), 7)
        .await
        .unwrap_err()
        .to_string()
        .contains("pk-index callback failure"));
    let snapshot = latest(&table).await;
    assert_eq!(snapshot.commit_kind(), &CommitKind::COMPACT);
    assert_eq!(snapshot.id(), 2);
    let active = indexes(&table).await;
    assert_eq!(active.len(), 1);
    commit
        .filter_and_commit_with_identifier(messages, 7)
        .await
        .unwrap();
    assert_eq!(latest(&table).await.id(), 2);
    assert_eq!(indexes(&table).await, active);
    assert_eq!(filtered_rows(&table, 20).await, vec![(2, 20)]);
}

async fn latest(table: &Table) -> paimon::spec::Snapshot {
    table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap()
}

async fn indexes(table: &Table) -> Vec<paimon::spec::IndexManifestEntry> {
    let snapshot = latest(table).await;
    match snapshot.index_manifest() {
        Some(name) => IndexManifest::read(
            table.file_io(),
            &table.snapshot_manager().manifest_path(name),
        )
        .await
        .unwrap(),
        None => vec![],
    }
}

async fn compact(table: &Table) -> Vec<CompactResult> {
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    assert!(!plan.tasks().is_empty());
    let results = execute(table, &plan).await;
    builder
        .new_commit()
        .unwrap()
        .commit_compact(results.clone())
        .await
        .unwrap();
    results
}

fn index_path(table: &Table, entry: &paimon::spec::IndexManifestEntry) -> String {
    if let Some(path) = &entry.index_file.external_path {
        return path.clone();
    }
    if table.schema().core_options().index_file_in_data_file_dir() {
        format!(
            "{}/bucket-{}/{}",
            table.location(),
            entry.bucket,
            entry.index_file.file_name
        )
    } else {
        format!("{}/index/{}", table.location(), entry.index_file.file_name)
    }
}

async fn filtered_rows(table: &Table, value: i32) -> Vec<(i32, i32)> {
    let mut builder = table.new_read_builder();
    builder.with_filter(
        PredicateBuilder::new(table.schema().fields())
            .equal("value", Datum::Int(value))
            .unwrap(),
    );
    let plan = builder.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        rows.extend((0..batch.num_rows()).map(|row| (ids.value(row), values.value(row))));
    }
    rows.sort_unstable();
    rows
}

#[tokio::test]
async fn indexes_preserve_nulls_tombstones_file_order_and_incremental_history() {
    for family in ["btree", "bitmap"] {
        for in_data in ["true", "false"] {
            let key = format!("pk-{family}.index.columns");
            let table = table(&[
                (&key, "value"),
                ("index-file-in-data-file-dir", in_data),
                ("target-file-row-num", "13"),
                ("read.batch-size", "7"),
                ("write-buffer-size", "1kb"),
            ])
            .await;
            let batch = RecordBatch::try_from_iter([
                (
                    "id",
                    std::sync::Arc::new(Int32Array::from_iter_values(0..150))
                        as arrow_array::ArrayRef,
                ),
                (
                    "value",
                    std::sync::Arc::new(Int32Array::from(
                        (0..150).map(|v| (v != 4).then_some(v)).collect::<Vec<_>>(),
                    )) as arrow_array::ArrayRef,
                ),
            ])
            .unwrap();
            write_batch(&table, &batch).await;
            let first = compact(&table).await;
            assert!(first[0].files().len() > 1);
            let historical = table.copy_with_pinned_snapshot(Some(&latest(&table).await));
            let original = indexes(&table).await;
            let source = PrimaryKeyIndexSourceMeta::from_global_index_meta(
                original[0].index_file.global_index_meta.as_ref().unwrap(),
            )
            .unwrap();
            assert!(source
                .source_files()
                .windows(2)
                .all(|p| p[0].file_name() < p[1].file_name()));
            for value in [0, 13, 100, 149] {
                assert_eq!(filtered_rows(&table, value).await, vec![(value, value)]);
            }
            write_batch(
                &table,
                &make_batch_with_kinds(vec![13, 100], vec![1300, 100], vec![0, 3]),
            )
            .await;
            compact(&table).await;
            assert_eq!(filtered_rows(&table, 13).await, vec![]);
            assert_eq!(filtered_rows(&table, 100).await, vec![]);
            assert_eq!(filtered_rows(&table, 1300).await, vec![(13, 1300)]);
            assert_eq!(filtered_rows(&historical, 13).await, vec![(13, 13)]);
            let active = indexes(&table).await;
            assert!(
                active.iter().any(|entry| entry == &original[0]),
                "unselected higher-level source and index must survive"
            );
            assert_eq!(active.len(), 2);
            assert!(active.iter().all(|e| e.index_file.index_type == family));
            for entry in original {
                assert!(table
                    .file_io()
                    .exists(&index_path(&table, &entry))
                    .await
                    .unwrap());
            }
        }
    }
}

#[tokio::test]
async fn legacy_results_require_reexecution_but_completed_results_recover_without_sources() {
    let table = table(&[("pk-btree.index.columns", "value")]).await;
    write_batch(&table, &make_batch(vec![1], vec![10])).await;
    let builder = table
        .new_compact_builder()
        .with_commit_user("pk-recovery")
        .unwrap();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let results = execute(&table, &plan).await;
    let mut legacy = serde_json::to_value(&results).unwrap();
    for result in legacy.as_array_mut().unwrap() {
        result.as_object_mut().unwrap().remove("index_entries");
    }
    let legacy: Vec<CompactResult> = serde_json::from_value(legacy).unwrap();
    let commit = builder.new_commit().unwrap();
    let error = commit.commit_compact(legacy.clone()).await.unwrap_err();
    assert!(
        error.to_string().contains("missing index maintenance"),
        "{error}"
    );
    assert_eq!(latest(&table).await.id(), 1);
    commit.commit_compact(results.clone()).await.unwrap();
    let before = indexes(&table).await;
    table
        .file_io()
        .delete_file(&table.snapshot_manager().snapshot_path(1))
        .await
        .unwrap();
    commit.commit_compact(legacy).await.unwrap();
    commit.commit_compact(results).await.unwrap();
    assert_eq!(latest(&table).await.id(), 2);
    assert_eq!(indexes(&table).await, before);
}

#[tokio::test]
async fn concurrent_l0_append_rebases_but_changed_index_membership_is_rejected() {
    for conflict in [false, true] {
        let table = table(&[("pk-btree.index.columns", "value")]).await;
        write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
        let builder = table.new_compact_builder();
        let plan = builder.new_planner().unwrap().plan().await.unwrap();
        let results = execute(&table, &plan).await;
        if conflict {
            // Index-only concurrent publication for the same bucket/field/level.
            let prepared = results[0].index_entries().unwrap()[0].clone();
            let mut message =
                paimon::table::CommitMessage::new(prepared.partition, prepared.bucket, vec![]);
            message.new_index_files.push(prepared.index_file);
            table
                .new_write_builder()
                .new_commit()
                .commit(vec![message])
                .await
                .unwrap();
        } else {
            write_batch(&table, &make_batch(vec![1, 3], vec![11, 30])).await;
        }
        let outcome = builder
            .new_commit()
            .unwrap()
            .commit_compact(results.clone())
            .await;
        if conflict {
            let error = outcome.unwrap_err();
            assert!(
                error.to_string().contains("snapshot was replaced")
                    || error.to_string().contains("index changed concurrently"),
                "{error}"
            );
            for entry in results[0].index_entries().unwrap() {
                assert!(
                    table
                        .file_io()
                        .exists(&index_path(&table, entry))
                        .await
                        .unwrap(),
                    "failed commit must retain prepared output"
                );
            }
        } else {
            outcome.unwrap();
            assert_eq!(filtered_rows(&table, 10).await, vec![]);
            assert_eq!(filtered_rows(&table, 11).await, vec![(1, 11)]);
            assert_eq!(rows(&table).await, vec![(1, 11), (2, 20), (3, 30)]);
            assert_eq!(indexes(&table).await.len(), 1);
        }
    }
}

#[tokio::test]
async fn compaction_retires_old_indexes_and_preserves_hash_and_deletion_vectors() {
    for mode in ["fixed", "dynamic", "dv"] {
        let mut options = vec![("pk-bitmap.index.columns", "value")];
        if mode == "dynamic" {
            options.push(("bucket", "-1"));
        }
        if mode == "dv" {
            options.extend([
                ("deletion-vectors.enabled", "true"),
                ("deletion-vectors.merge-on-read", "false"),
            ]);
        }
        let table = table(&options).await;
        write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
        compact(&table).await;
        let old = indexes(&table).await;
        let old_pk = old
            .iter()
            .find(|e| e.index_file.index_type == "bitmap")
            .unwrap()
            .clone();
        let old_snapshot = table.copy_with_pinned_snapshot(Some(&latest(&table).await));
        write_batch(&table, &make_batch(vec![1, 2], vec![11, 22])).await;
        compact(&table).await;
        let active = indexes(&table).await;
        assert!(!active
            .iter()
            .any(|e| e.index_file.file_name == old_pk.index_file.file_name));
        assert_eq!(
            active
                .iter()
                .filter(|e| e.index_file.index_type == "bitmap")
                .count(),
            1
        );
        if mode == "dynamic" {
            assert!(active.iter().any(|e| e.index_file.index_type == "HASH"));
        }
        assert_eq!(rows(&table).await, vec![(1, 11), (2, 22)]);
        assert_eq!(rows(&old_snapshot).await, vec![(1, 10), (2, 20)]);
        assert!(table
            .file_io()
            .exists(&index_path(&table, &old_pk))
            .await
            .unwrap());
    }
}

#[tokio::test]
async fn compact_atomically_maintains_source_backed_primary_key_indexes() {
    let table = table(&[
        ("pk-btree.index.columns", "value"),
        ("target-file-row-num", "2"),
    ])
    .await;
    write_batch(&table, &make_batch(vec![1, 2], vec![10, 20])).await;
    write_batch(&table, &make_batch(vec![2, 3], vec![22, 30])).await;
    let builder = table
        .new_compact_builder()
        .with_commit_user("pk-index")
        .unwrap();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    let executor = builder.new_executor().unwrap();
    let mut results = Vec::new();
    for task in plan.tasks() {
        results.push(executor.execute(task).await.unwrap());
    }
    let commit = builder.new_commit().unwrap();
    commit.commit_compact(results.clone()).await.unwrap();
    let snapshot = table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(*snapshot.commit_kind(), CommitKind::COMPACT);
    assert_eq!(
        snapshot.id(),
        3,
        "data and indexes must publish in the same transaction"
    );
    let indexes = IndexManifest::read(
        table.file_io(),
        &table.snapshot_manager().manifest_path(
            snapshot
                .index_manifest()
                .expect("PK compaction must publish the declared index"),
        ),
    )
    .await
    .unwrap();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].index_file.index_type, "btree");
    let meta = indexes[0].index_file.global_index_meta.as_ref().unwrap();
    let source = PrimaryKeyIndexSourceMeta::from_global_index_meta(meta).unwrap();
    let data = table
        .new_read_builder()
        .new_scan()
        .with_scan_all_files()
        .plan()
        .await
        .unwrap();
    let active = data
        .splits()
        .iter()
        .flat_map(|split| split.data_files())
        .filter(|file| should_read_pk_index_source(file))
        .collect::<Vec<_>>();
    assert_eq!(
        source
            .source_files()
            .iter()
            .map(|file| (file.file_name(), file.row_count()))
            .collect::<HashSet<_>>(),
        active
            .iter()
            .map(|file| (file.file_name.as_str(), file.row_count))
            .collect()
    );
    assert_eq!(meta.row_range_start, 0);
    assert_eq!(meta.row_range_end, 2);
    assert_eq!(rows(&table).await, vec![(1, 10), (2, 22), (3, 30)]);
    commit.commit_compact(results).await.unwrap();
    assert_eq!(
        table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap()
            .id(),
        3
    );
}
