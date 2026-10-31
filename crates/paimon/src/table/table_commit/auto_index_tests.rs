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
use crate::catalog::Identifier;
use crate::io::FileIOBuilder;
use crate::spec::{IntType, Schema as TableSchemaBuilder, TableSchema};
use crate::table::global_index_build_common::GlobalIndexBuildView;
use arrow_array::{Int32Array, RecordBatch};
use futures::TryStreamExt;
use std::sync::atomic::{AtomicBool, Ordering};

mod search;

async fn table(enabled: Option<bool>, compact: bool) -> Table {
    let mut schema = TableSchemaBuilder::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        .option("global-index.btree.index-columns", "id")
        .option("global-index.bitmap.index-columns", "value")
        .option("sorted-index.records-per-range", "2")
        .option(
            "compaction.min.file-num",
            if compact { "2" } else { "1000" },
        )
        .option("commit.min-retry-wait", "0 ms")
        .option("commit.max-retry-wait", "0 ms");
    if let Some(enabled) = enabled {
        schema = schema.option("write-only", (!enabled).to_string());
    }
    let location = format!("memory:/auto-index-build/{}", uuid::Uuid::new_v4());
    let table = Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("default", "auto_index"),
        location,
        TableSchema::new(0, &schema.build().unwrap()),
        None,
    );
    table
        .file_io()
        .mkdirs(&format!("{}/schema", table.location()))
        .await
        .unwrap();
    table
        .file_io()
        .mkdirs(&format!("{}/snapshot", table.location()))
        .await
        .unwrap();
    table
        .file_io()
        .new_output(&table.schema_manager().schema_path(0))
        .unwrap()
        .write(serde_json::to_vec(table.schema()).unwrap().into())
        .await
        .unwrap();
    table
}

async fn messages(table: &Table, ids: Vec<i32>) -> Vec<CommitMessage> {
    let values = ids.iter().map(|id| id * 10).collect::<Vec<_>>();
    let batch = RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int32Array::from(ids)) as arrow_array::ArrayRef,
        ),
        (
            "value",
            Arc::new(Int32Array::from(values)) as arrow_array::ArrayRef,
        ),
    ])
    .unwrap();
    let builder = table.new_write_builder();
    let mut write = builder.new_write().unwrap();
    write.write_arrow_batch(&batch).await.unwrap();
    write.prepare_commit().await.unwrap()
}

async fn latest(table: &Table) -> Snapshot {
    table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap()
}

async fn indexes(table: &Table) -> Vec<IndexManifestEntry> {
    let snapshot = latest(table).await;
    match snapshot.index_manifest() {
        None => vec![],
        Some(name) => IndexManifest::read(
            table.file_io(),
            &table.snapshot_manager().manifest_path(name),
        )
        .await
        .unwrap(),
    }
}

async fn assert_complete(table: &Table) {
    for (column, kind) in [("id", "btree"), ("value", "bitmap")] {
        assert!(table
            .new_global_index_build_builder()
            .with_index_column(column)
            .with_index_type(kind)
            .build()
            .await
            .unwrap()
            .is_empty());
    }
    let entries = indexes(table).await;
    assert!(!entries.is_empty());
    for entry in entries {
        let file = entry.index_file;
        let path = file
            .external_path
            .unwrap_or_else(|| format!("{}/index/{}", table.location(), file.file_name));
        assert!(
            table.file_io().exists(&path).await.unwrap(),
            "published index file must survive recovery"
        );
    }
}

#[tokio::test]
async fn write_only_controls_automatic_maintenance_and_defaults_to_enabled() {
    for enabled in [None, Some(false), Some(true)] {
        let table = table(enabled, false).await;
        TableCommit::new(table.clone(), "writer".into())
            .commit(messages(&table, vec![1, 2]).await)
            .await
            .unwrap();
        assert_eq!(
            latest(&table).await.id(),
            if enabled != Some(false) { 2 } else { 1 }
        );
        if enabled != Some(false) {
            assert_complete(&table).await;
        } else {
            assert!(indexes(&table).await.is_empty());
        }
    }
}

#[tokio::test]
async fn write_only_ignores_declarations_and_absent_declarations_are_a_noop() {
    let original = table(Some(false), false).await;
    let disabled = original.copy_with_options(HashMap::from([(
        "global-index.btree.index-columns".into(),
        "id,id".into(),
    )]));
    TableCommit::new(disabled.clone(), "disabled".into())
        .commit(messages(&disabled, vec![1]).await)
        .await
        .unwrap();
    assert!(indexes(&disabled).await.is_empty());
    for (key, value) in [
        ("write-only", "invalid"),
        ("row-tracking.enabled", "false"),
        ("bucket", "1"),
    ] {
        let original = table(Some(true), false).await;
        let table = original.copy_with_options(HashMap::from([(key.into(), value.into())]));
        let error = TableCommit::new(table.clone(), "invalid".into())
            .commit(messages(&original, vec![1]).await)
            .await
            .unwrap_err();
        assert!(error.to_string().contains(key) || error.to_string().contains("unaware-bucket"));
        assert!(table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .is_none());
    }
    let original = table(Some(true), false).await;
    let mut options = original.schema().options().clone();
    options.remove("global-index.btree.index-columns");
    options.remove("global-index.bitmap.index-columns");
    let missing = original
        .copy_with_resolved_schema(
            original.schema().copy_with_replaced_options(options),
            original.branch(),
        )
        .unwrap();
    TableCommit::new(missing.clone(), "missing".into())
        .commit(messages(&missing, vec![1]).await)
        .await
        .unwrap();
    assert_eq!(latest(&missing).await.id(), 1);
    assert!(indexes(&missing).await.is_empty());
}

#[tokio::test]
async fn data_evolution_and_query_index_switches_do_not_gate_automatic_builds() {
    for (evolution, query_indexes) in [(false, false), (false, true), (true, false)] {
        let table = table(Some(true), false)
            .await
            .copy_with_options(HashMap::from([
                ("data-evolution.enabled".into(), evolution.to_string()),
                ("global-index.enabled".into(), query_indexes.to_string()),
            ]));
        TableCommit::new(table.clone(), "writer".into())
            .commit(messages(&table, vec![1, 2]).await)
            .await
            .unwrap();
        assert_eq!(latest(&table).await.id(), 2);
        assert_complete(&table).await;
    }
}

#[tokio::test]
async fn append_only_adds_missing_ranges_and_empty_commits_do_nothing() {
    let table = table(Some(true), false).await;
    let commit = TableCommit::new(table.clone(), "writer".into());
    let first = messages(&table, vec![1, 2]).await;
    commit
        .commit_with_identifier(first.clone(), 7)
        .await
        .unwrap();
    let original = indexes(&table).await;
    commit
        .commit_with_identifier(messages(&table, vec![3, 4]).await, 8)
        .await
        .unwrap();
    let all = indexes(&table).await;
    assert_eq!(all.len(), original.len() * 2);
    assert!(original.iter().all(|entry| all.contains(entry)));
    assert_complete(&table).await;
    let mut read = table.new_read_builder();
    read.with_filter(
        crate::spec::PredicateBuilder::new(table.schema().fields())
            .equal("id", Datum::Int(3))
            .unwrap(),
    );
    let plan = read.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = read
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let found = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    assert_eq!(found, vec![3], "queries must see newly indexed rows");
    let before = latest(&table).await.id();
    commit.commit(vec![]).await.unwrap();
    assert_eq!(commit.filter_and_commit(vec![(7, first)]).await.unwrap(), 0);
    assert_eq!(latest(&table).await.id(), before);
}

#[tokio::test]
async fn changed_shards_replace_fragmented_indexes_and_preserve_historical_payloads() {
    let table = table(Some(true), false)
        .await
        .copy_with_options(HashMap::from([(
            "sorted-index.records-per-range".into(),
            "4".into(),
        )]));
    let commit = TableCommit::new(table.clone(), "writer".into());
    commit
        .commit_with_identifier(messages(&table, vec![1]).await, 6)
        .await
        .unwrap();
    let previous = indexes(&table).await;
    let input = messages(&table, vec![2]).await;
    commit
        .commit_with_identifier(input.clone(), 7)
        .await
        .unwrap();
    let current = indexes(&table).await;
    assert_eq!(current.len(), 2, "one payload per field and row-ID shard");
    for entry in &current {
        assert!(!previous.contains(entry));
        let meta = entry.index_file.global_index_meta.as_ref().unwrap();
        assert_eq!((meta.row_range_start, meta.row_range_end), (0, 1));
    }
    for entry in previous {
        assert!(
            table
                .file_io()
                .exists(&format!(
                    "{}/index/{}",
                    table.location(),
                    entry.index_file.file_name
                ))
                .await
                .unwrap(),
            "retired payloads must remain readable by historical snapshots"
        );
    }
    commit
        .filter_and_commit_with_identifier(input, 7)
        .await
        .unwrap();
    assert_eq!(latest(&table).await.id(), 4);
    assert_complete(&table).await;
}

#[tokio::test]
async fn manual_compaction_maintains_indexes_and_recovers_after_index_publication_failure() {
    let original = table(Some(false), true).await;
    let seed = TableCommit::new(original.clone(), "seed".into());
    seed.commit(messages(&original, vec![1]).await)
        .await
        .unwrap();
    seed.commit(messages(&original, vec![2]).await)
        .await
        .unwrap();
    let table = original.copy_with_options(HashMap::from([
        ("write-only".into(), "false".into()),
        ("commit.max-retries".into(), "0".into()),
    ]));
    let builder = table.new_compact_builder();
    let plan = builder.new_planner().unwrap().plan().await.unwrap();
    assert!(!plan.tasks().is_empty());
    let executor = builder.new_executor().unwrap();
    let mut results = Vec::new();
    for task in plan.tasks() {
        results.push(executor.execute(task).await.unwrap());
    }
    let mut commit = TableCommit::new(table.clone(), "compact-operator".into());
    commit.snapshot_commit = Arc::new(InjectIndexPublication {
        table: table.clone(),
        mode: Publication::FailBeforePublish,
        fired: AtomicBool::new(false),
    });
    assert!(commit
        .commit_compact(results.clone())
        .await
        .unwrap_err()
        .to_string()
        .contains("automatic index build failed"));
    let compact = latest(&table).await;
    assert_eq!(compact.commit_kind(), &CommitKind::COMPACT);
    let recovery = TableCommit::new(table.clone(), "compact-operator".into());
    for _ in 0..2 {
        recovery.commit_compact(results.clone()).await.unwrap();
        assert_eq!(latest(&table).await.id(), compact.id() + 1);
        assert_complete(&table).await;
    }
}

#[tokio::test]
async fn allowed_column_updates_rebuild_affected_indexes_from_current_values() {
    use arrow_array::Int64Array;
    let table = table(Some(true), false)
        .await
        .copy_with_options(HashMap::from([(
            "global-index.column-update-action".into(),
            "DROP_PARTITION_INDEX".into(),
        )]));
    let commit = TableCommit::new(table.clone(), "writer".into());
    commit
        .commit_with_identifier(messages(&table, vec![1, 2]).await, 6)
        .await
        .unwrap();
    let previous = indexes(&table).await;
    let builder = table.new_write_builder();
    let mut writer = builder
        .new_data_evolution_writer(vec!["value".into()])
        .unwrap();
    writer
        .add_matched_batch(
            RecordBatch::try_from_iter(vec![
                (
                    "_ROW_ID",
                    Arc::new(Int64Array::from(vec![0])) as arrow_array::ArrayRef,
                ),
                (
                    "value",
                    Arc::new(Int32Array::from(vec![99])) as arrow_array::ArrayRef,
                ),
            ])
            .unwrap(),
        )
        .unwrap();
    let input = writer.prepare_commit().await.unwrap();
    commit
        .commit_with_identifier(input.clone(), 7)
        .await
        .unwrap();
    assert_complete(&table).await;
    let current = indexes(&table).await;
    assert!(current
        .iter()
        .filter(|entry| entry.index_file.index_type == "bitmap")
        .all(|entry| !previous.contains(entry)));
    let mut read = table.new_read_builder();
    read.with_filter(
        crate::spec::PredicateBuilder::new(table.schema().fields())
            .equal("value", Datum::Int(99))
            .unwrap(),
    );
    let plan = read.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = read
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let before = latest(&table).await.id();
    commit
        .filter_and_commit_with_identifier(input, 7)
        .await
        .unwrap();
    assert_eq!(latest(&table).await.id(), before);
}

#[tokio::test]
async fn column_lists_and_java_style_scalar_options_are_resolved_per_field() {
    let original = table(Some(true), false).await;
    let mut options = original.schema().options().clone();
    options.remove("global-index.bitmap.index-columns");
    options.insert(
        "global-index.btree.index-columns".into(),
        " value, id ".into(),
    );
    options.insert(
        "fields.id.global-index.btree.index-options".into(),
        r#"{"file-version":2,"block-size":1024,"bloom-filter.enabled":true}"#.into(),
    );
    options.insert(
        "fields.value.global-index.btree.index-options".into(),
        r#"{"file-version":1,"block-size":2048}"#.into(),
    );
    let table = original
        .copy_with_resolved_schema(
            original.schema().copy_with_replaced_options(options),
            original.branch(),
        )
        .unwrap();
    TableCommit::new(table.clone(), "writer".into())
        .commit(messages(&table, vec![1, 2]).await)
        .await
        .unwrap();
    let files = indexes(&table).await;
    assert_eq!(files.len(), 2);
    assert!(files
        .iter()
        .all(|entry| entry.index_file.index_type == "btree"));
    let ids = files
        .iter()
        .map(|entry| {
            entry
                .index_file
                .global_index_meta
                .as_ref()
                .unwrap()
                .index_field_id
        })
        .collect::<HashSet<_>>();
    assert_eq!(ids, HashSet::from([0, 1]));
    for entry in &files {
        let file = &entry.index_file;
        let bytes = table
            .file_io()
            .new_input(&format!("{}/index/{}", table.location(), file.file_name))
            .unwrap()
            .read()
            .await
            .unwrap();
        let footer = crate::btree::BTreeFileFooter::read_footer(
            &bytes[bytes.len() - crate::btree::BTreeFileFooter::ENCODED_LENGTH..],
        )
        .unwrap();
        let field_id = file.global_index_meta.as_ref().unwrap().index_field_id;
        assert_eq!(footer.version, if field_id == 0 { 2 } else { 1 });
        assert_eq!(footer.bloom_filter_handle.is_some(), field_id == 0);
    }
    for column in ["id", "value"] {
        assert!(table
            .new_global_index_build_builder()
            .with_index_column(column)
            .build()
            .await
            .unwrap()
            .is_empty());
    }
}

#[tokio::test]
async fn vector_definitions_require_a_supported_algorithm_and_metric() {
    use crate::spec::{ArrayType, FloatType};
    let schema = TableSchemaBuilder::builder()
        .column(
            "embedding",
            DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
        )
        .option("row-tracking.enabled", "true")
        .option("global-index.vector.index-columns", "embedding")
        .build()
        .unwrap();
    let original = Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("default", "vector_options"),
        "memory:/vector_options".into(),
        TableSchema::new(0, &schema),
        None,
    );
    assert!(TableCommit::new(original.clone(), "writer".into())
        .auto_index_enabled(true)
        .await
        .unwrap_err()
        .to_string()
        .contains("fields.embedding.global-index.vector.index-type"));
    for (key, value) in [
        ("fields.embedding.global-index.vector.index-type", "btree"),
        (
            "fields.embedding.global-index.vector.distance-metric",
            "not-a-metric",
        ),
        (
            "fields.embedding.global-index.vector.index-options",
            r#"{"dimension":0}"#,
        ),
        (
            "fields.embedding.global-index.vector.index-options",
            r#"{"dimension":[]}"#,
        ),
    ] {
        let table = original
            .copy_with_options(HashMap::from([(
                "fields.embedding.global-index.vector.index-type".into(),
                "ivf-flat".into(),
            )]))
            .copy_with_options(HashMap::from([(key.into(), value.into())]));
        assert!(
            TableCommit::new(table, "invalid".into())
                .auto_index_enabled(true)
                .await
                .is_err(),
            "{key}={value}"
        );
    }
}

#[tokio::test]
async fn declarations_are_validated_before_publishing_data() {
    for (key, value) in [
        ("global-index.btree.index-columns", ""),
        ("global-index.btree.index-columns", "missing"),
        ("global-index.btree.index-columns", "id,id"),
        ("global-index.btree.index-columns", "id,"),
        ("global-index.bitmap.index-columns", "id"),
        ("fields.id.global-index.btree.index-options", "[]"),
        ("fields.id.global-index.btree.index-options", "not-json"),
        (
            "fields.id.global-index.btree.index-options",
            r#"{"sorted-index.records-per-range":0}"#,
        ),
        (
            "fields.id.global-index.btree.index-options",
            r#"{"compression":null}"#,
        ),
        ("fields.id.global-index.btree.index-options", r#"{"":1}"#),
        (
            "fields.id.global-index.btree.index-options",
            r#"{"block-size":"not-a-size"}"#,
        ),
    ] {
        let table = table(Some(true), false)
            .await
            .copy_with_options(HashMap::from([(key.into(), value.into())]));
        let result = TableCommit::new(table.clone(), "invalid".into())
            .commit(messages(&table, vec![1]).await)
            .await;
        assert!(result.is_err(), "{key}={value}");
        assert!(table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .is_none());
    }
}

struct FailDataCallbackOnce(AtomicBool);

#[async_trait::async_trait]
impl CommitCallback for FailDataCallbackOnce {
    async fn call(&self, context: &CommitCallbackContext) -> Result<()> {
        if !context.commit_entries.is_empty() && !self.0.swap(true, Ordering::SeqCst) {
            return Err(crate::Error::UnexpectedError {
                message: "injected data callback failure".into(),
                source: None,
            });
        }
        Ok(())
    }
}

#[tokio::test]
async fn recovery_finishes_indexing_after_data_publication_for_append_and_overwrite() {
    for overwrite in [false, true] {
        let table = table(Some(true), false).await;
        let input = messages(&table, vec![1, 2]).await;
        let mut commit = TableCommit::new(table.clone(), "recover".into());
        commit
            .add_commit_callback(Arc::new(FailDataCallbackOnce(AtomicBool::new(false))))
            .unwrap();
        let result = if overwrite {
            commit
                .overwrite_with_identifier(input.clone(), None, 7)
                .await
        } else {
            commit.commit_with_identifier(input.clone(), 7).await
        };
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("injected data callback failure"));
        assert_eq!(latest(&table).await.id(), 1);
        for _ in 0..2 {
            if overwrite {
                commit
                    .overwrite_with_identifier(input.clone(), None, 7)
                    .await
                    .unwrap();
            } else {
                assert_eq!(
                    commit
                        .filter_and_commit(vec![(7, input.clone())])
                        .await
                        .unwrap(),
                    0
                );
            }
            assert_eq!(latest(&table).await.id(), 2);
            assert_complete(&table).await;
        }
    }
}

enum Publication {
    Conflict,
    LoseResponse,
    FailBeforePublish,
}

struct InjectIndexPublication {
    table: Table,
    mode: Publication,
    fired: AtomicBool,
}

#[async_trait::async_trait]
impl SnapshotCommit for InjectIndexPublication {
    async fn commit(
        &self,
        _base: Option<&str>,
        snapshot: &Snapshot,
        _branch: &str,
        _statistics: &[PartitionStatistics],
    ) -> Result<bool> {
        if snapshot.commit_user().contains("-auto-index-build-")
            && !self.fired.swap(true, Ordering::SeqCst)
        {
            match self.mode {
                Publication::Conflict => {
                    // A concurrent writer finishes the exact same coverage first.
                    let other = TableCommit::new(self.table.clone(), "competing-writer".into());
                    other.commit(messages(&self.table, vec![99]).await).await?;
                    return Ok(false);
                }
                Publication::LoseResponse => {
                    assert!(
                        self.table
                            .snapshot_manager()
                            .commit_snapshot(snapshot)
                            .await?
                    );
                    return Err(crate::Error::UnexpectedError {
                        message: "lost index response".into(),
                        source: None,
                    });
                }
                Publication::FailBeforePublish => {
                    return Err(crate::Error::UnexpectedError {
                        message: "index publisher unavailable".into(),
                        source: None,
                    })
                }
            }
        }
        self.table
            .snapshot_manager()
            .commit_snapshot(snapshot)
            .await
    }
}

#[tokio::test]
async fn definite_conflict_replans_coverage_and_preserves_competing_indexes() {
    let table = table(Some(true), false).await;
    let mut commit = TableCommit::new(table.clone(), "writer".into());
    commit.snapshot_commit = Arc::new(InjectIndexPublication {
        table: table.clone(),
        mode: Publication::Conflict,
        fired: AtomicBool::new(false),
    });
    commit
        .commit_with_identifier(messages(&table, vec![1, 2]).await, 7)
        .await
        .unwrap();
    assert_eq!(latest(&table).await.id(), 3);
    assert_complete(&table).await;
    assert_eq!(
        table
            .file_io()
            .list_status(&format!("{}/index", table.location()))
            .await
            .unwrap()
            .iter()
            .filter(|file| !file.is_dir)
            .count(),
        indexes(&table).await.len(),
        "a definite conflict must clean up only its unpublished index files",
    );
}

#[tokio::test]
async fn conflicting_replacement_replans_instead_of_deleting_a_competitors_payloads() {
    let table = table(Some(true), false).await;
    TableCommit::new(table.clone(), "seed".into())
        .commit(messages(&table, vec![1]).await)
        .await
        .unwrap();
    let historical = indexes(&table).await;
    let mut commit = TableCommit::new(table.clone(), "writer".into());
    commit.snapshot_commit = Arc::new(InjectIndexPublication {
        table: table.clone(),
        mode: Publication::Conflict,
        fired: AtomicBool::new(false),
    });
    commit
        .commit_with_identifier(messages(&table, vec![2]).await, 7)
        .await
        .unwrap();
    assert_eq!(latest(&table).await.id(), 5);
    assert_complete(&table).await;
    let current = indexes(&table).await;
    assert_eq!(current.len(), 4);
    assert!(historical.iter().all(|entry| !current.contains(entry)));
    let physical = table
        .file_io()
        .list_status(&format!("{}/index", table.location()))
        .await
        .unwrap();
    assert_eq!(
        physical.iter().filter(|file| !file.is_dir).count(),
        current.len() + historical.len(),
        "discard only this round's private files and retain historical payloads"
    );
}

#[tokio::test]
async fn lost_index_response_is_recovered_without_deleting_published_files() {
    let table = table(Some(true), false).await;
    let mut commit = TableCommit::new(table.clone(), "writer".into());
    commit.snapshot_commit = Arc::new(InjectIndexPublication {
        table: table.clone(),
        mode: Publication::LoseResponse,
        fired: AtomicBool::new(false),
    });
    let input = messages(&table, vec![1, 2]).await;
    commit
        .commit_with_identifier(input.clone(), 7)
        .await
        .unwrap();
    commit
        .filter_and_commit_with_identifier(input, 7)
        .await
        .unwrap();
    assert_eq!(latest(&table).await.id(), 2);
    assert_complete(&table).await;
}

#[tokio::test]
async fn exhausted_index_retries_preserve_data_and_recover_with_the_same_identifier() {
    let table = table(Some(true), false)
        .await
        .copy_with_options(HashMap::from([("commit.max-retries".into(), "0".into())]));
    let input = messages(&table, vec![1, 2]).await;
    let mut commit = TableCommit::new(table.clone(), "writer".into());
    commit.snapshot_commit = Arc::new(InjectIndexPublication {
        table: table.clone(),
        mode: Publication::FailBeforePublish,
        fired: AtomicBool::new(false),
    });
    let error = commit
        .commit_with_identifier(input.clone(), 7)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("Data commit 7 succeeded"));
    assert!(error.to_string().contains("index publisher unavailable"));
    assert_eq!(latest(&table).await.id(), 1);
    let recovery = TableCommit::new(table.clone(), "writer".into());
    recovery
        .filter_and_commit_with_identifier(input.clone(), 7)
        .await
        .unwrap();
    recovery
        .filter_and_commit_with_identifier(input, 7)
        .await
        .unwrap();
    assert_eq!(latest(&table).await.id(), 2);
    assert_complete(&table).await;
}

#[tokio::test]
async fn recovery_resumes_indexing_after_compaction_already_completed() {
    let table = table(Some(true), true)
        .await
        .copy_with_options(HashMap::from([("commit.max-retries".into(), "0".into())]));
    TableCommit::new(table.clone(), "seed".into())
        .commit(messages(&table, vec![1]).await)
        .await
        .unwrap();
    let input = messages(&table, vec![2]).await;
    let mut commit = TableCommit::new(table.clone(), "writer".into());
    commit.snapshot_commit = Arc::new(InjectIndexPublication {
        table: table.clone(),
        mode: Publication::FailBeforePublish,
        fired: AtomicBool::new(false),
    });
    assert!(commit
        .commit_with_identifier(input.clone(), 7)
        .await
        .unwrap_err()
        .to_string()
        .contains("automatic index build failed"));
    let compact = latest(&table).await;
    assert_eq!(compact.commit_kind(), &CommitKind::COMPACT);
    let recovery = TableCommit::new(table.clone(), "writer".into());
    for _ in 0..2 {
        recovery
            .filter_and_commit_with_identifier(input.clone(), 7)
            .await
            .unwrap();
        assert_eq!(latest(&table).await.id(), compact.id() + 1);
        assert_complete(&table).await;
    }
}

struct RacingRecoveries {
    table: Table,
    ready: tokio::sync::Barrier,
}

#[async_trait::async_trait]
impl SnapshotCommit for RacingRecoveries {
    async fn commit(
        &self,
        _base: Option<&str>,
        snapshot: &Snapshot,
        _branch: &str,
        _statistics: &[PartitionStatistics],
    ) -> Result<bool> {
        self.ready.wait().await;
        self.table
            .snapshot_manager()
            .commit_snapshot(snapshot)
            .await
    }
}

#[tokio::test]
async fn simultaneous_recoveries_publish_one_index_snapshot() {
    let table = table(Some(true), false).await;
    let input = messages(&table, vec![1, 2]).await;
    let mut original = TableCommit::new(table.clone(), "writer".into());
    original
        .add_commit_callback(Arc::new(FailDataCallbackOnce(AtomicBool::new(false))))
        .unwrap();
    assert!(original
        .commit_with_identifier(input.clone(), 7)
        .await
        .is_err());
    let mut recovery = TableCommit::new(table.clone(), "writer".into());
    recovery.snapshot_commit = Arc::new(RacingRecoveries {
        table: table.clone(),
        ready: tokio::sync::Barrier::new(2),
    });
    let (first, second) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(
            recovery.filter_and_commit_with_identifier(input.clone(), 7),
            recovery.filter_and_commit_with_identifier(input.clone(), 7),
        )
    })
    .await
    .expect("recoveries must converge after one CAS conflict");
    first.unwrap();
    second.unwrap();
    assert_eq!(latest(&table).await.id(), 2);
    assert_complete(&table).await;
    assert_eq!(
        table
            .file_io()
            .list_status(&format!("{}/index", table.location()))
            .await
            .unwrap()
            .iter()
            .filter(|file| !file.is_dir)
            .count(),
        indexes(&table).await.len(),
        "the losing recovery must leave no private output behind",
    );
}

#[tokio::test]
async fn captured_builds_ignore_later_appends_and_validation_creates_no_files() {
    let table = table(Some(false), false).await;
    let commit = TableCommit::new(table.clone(), "writer".into());
    commit
        .commit(messages(&table, vec![1, 2]).await)
        .await
        .unwrap();
    let source = latest(&table).await;
    commit
        .commit(messages(&table, vec![3, 4]).await)
        .await
        .unwrap();
    let mut builder = table.new_global_index_build_builder();
    builder.with_index_column("id");
    builder.validate().await.unwrap();
    assert_eq!(latest(&table).await.id(), 2);
    assert!(indexes(&table).await.is_empty());
    let built = builder
        .build_with_view(
            GlobalIndexBuildView::capture(&table, Some(source))
                .await
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(built.len(), 1);
    let meta = built[0].new_index_files[0]
        .global_index_meta
        .as_ref()
        .unwrap();
    assert_eq!((meta.row_range_start, meta.row_range_end), (0, 1));
    commit.abort(&built).await.unwrap();
}

#[tokio::test]
async fn overwrite_replaces_coverage_without_resurrecting_old_rows() {
    let table = table(Some(true), false).await;
    let commit = TableCommit::new(table.clone(), "writer".into());
    commit
        .commit_with_identifier(messages(&table, vec![1, 2]).await, 6)
        .await
        .unwrap();
    let old_files = indexes(&table).await;
    let input = messages(&table, vec![3, 4]).await;
    for _ in 0..2 {
        commit
            .overwrite_with_identifier(input.clone(), None, 7)
            .await
            .unwrap();
        assert_complete(&table).await;
        let current = indexes(&table).await;
        assert!(current.iter().all(|entry| !old_files.contains(entry)));
        assert_eq!(latest(&table).await.id(), 4);
    }
}

#[tokio::test]
async fn automatic_indexes_preserve_branch_reference_write_restrictions() {
    let table = table(Some(true), false).await;
    TableCommit::new(table.clone(), "main".into())
        .commit(messages(&table, vec![1, 2]).await)
        .await
        .unwrap();
    let main_files = indexes(&table).await;
    crate::table::branch_manager::BranchManager::new(
        table.file_io().clone(),
        table.location().into(),
    )
    .create_branch("dev")
    .await
    .unwrap();
    let branch = table
        .copy_with_resolved_schema(table.schema().clone(), "dev")
        .unwrap();
    let commit = TableCommit::new(branch.clone(), "branch".into());
    let input = messages(&branch, vec![3, 4]).await;
    let error = commit
        .commit_with_identifier(input.clone(), 7)
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("Writing to Paimon branch 'dev' is not supported"));
    let error = commit
        .filter_and_commit_with_identifier(input, 7)
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("Writing to Paimon branch 'dev' is not supported"));
    assert_eq!(latest(&table).await.id(), 2);
    assert_eq!(indexes(&table).await, main_files);
    assert!(branch
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn native_builders_share_automatic_incremental_and_captured_snapshot_contracts() {
    use crate::spec::{ArrayType, FloatType, VarCharType};
    use arrow_array::builder::{Float32Builder, ListBuilder};
    use arrow_array::{ArrayRef, StringArray};

    let original = table(Some(true), false).await;
    let mut options = original.schema().options().clone();
    options.remove("global-index.btree.index-columns");
    options.remove("global-index.bitmap.index-columns");
    let schema = TableSchemaBuilder::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("text", DataType::VarChar(VarCharType::string_type()))
        .column(
            "embedding",
            DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
        )
        .options(options)
        .option("global-index.full-text.index-columns", " text ")
        .option(
            "fields.text.global-index.full-text.index-options",
            r#"{"lower-case":true}"#,
        )
        .option("global-index.vector.index-columns", "embedding")
        .option(
            "fields.embedding.global-index.vector.index-type",
            "ivf-flat",
        )
        .option(
            "fields.embedding.global-index.vector.index-options",
            r#"{"dimension":2,"nlist":1}"#,
        )
        .build()
        .unwrap();
    let table = original
        .copy_with_resolved_schema(TableSchema::new(0, &schema), original.branch())
        .unwrap();
    table
        .file_io()
        .new_output(&table.schema_manager().schema_path(0))
        .unwrap()
        .write(serde_json::to_vec(table.schema()).unwrap().into())
        .await
        .unwrap();
    let mut vectors = ListBuilder::new(Float32Builder::new());
    for _ in 0..4 {
        vectors.values().append_value(1.);
        vectors.values().append_value(0.);
        vectors.append(true);
    }
    let batch = RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef,
        ),
        (
            "text",
            Arc::new(StringArray::from(vec!["paimon index"; 4])) as ArrayRef,
        ),
        ("embedding", Arc::new(vectors.finish()) as ArrayRef),
    ])
    .unwrap();
    let commit = TableCommit::new(table.clone(), "native".into());
    for _ in 0..2 {
        let builder = table.new_write_builder();
        let mut write = builder.new_write().unwrap();
        write.write_arrow_batch(&batch).await.unwrap();
        commit
            .commit(write.prepare_commit().await.unwrap())
            .await
            .unwrap();
    }
    for (column, kind) in [("text", "full-text"), ("embedding", "ivf-flat")] {
        let options = if kind == "full-text" {
            HashMap::new()
        } else {
            HashMap::from([
                ("ivf-flat.dimension".into(), "2".into()),
                ("ivf-flat.nlist".into(), "1".into()),
            ])
        };
        let mut builder = table.new_global_index_build_builder();
        builder
            .with_index_column(column)
            .with_index_type(kind)
            .with_options(options);
        assert!(builder.build().await.unwrap().is_empty());
        // Snapshot 1 precedes both rounds; the later append must not enter this build.
        let source = table.snapshot_manager().get_snapshot(1).await.unwrap();
        let built = builder
            .build_with_view(
                GlobalIndexBuildView::capture(&table, Some(source))
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(built.len(), 1, "{kind}");
        let meta = built[0].new_index_files[0]
            .global_index_meta
            .as_ref()
            .unwrap();
        assert_eq!((meta.row_range_start, meta.row_range_end), (0, 3), "{kind}");
        commit.abort(&built).await.unwrap();
    }
    assert_eq!(latest(&table).await.id(), 4);
    let files = indexes(&table).await;
    assert_eq!(
        files.len(),
        2,
        "each native index should have one current shard after maintenance"
    );
    let full_text = files
        .iter()
        .find(|entry| entry.index_file.index_type == "full-text")
        .unwrap();
    let options: serde_json::Value = serde_json::from_slice(
        full_text
            .index_file
            .global_index_meta
            .as_ref()
            .unwrap()
            .index_meta
            .as_ref()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(options["lower-case"], "true");
}
