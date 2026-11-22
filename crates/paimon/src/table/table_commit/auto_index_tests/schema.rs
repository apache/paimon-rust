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
use crate::spec::{
    DataEvolutionIndexSourceMeta, FloatType, IndexFileMeta, PredicateBuilder, SchemaChange,
    VarCharType,
};
use arrow_array::{ArrayRef, Float32Array, StringArray};

async fn evolve(table: &Table, changes: Vec<SchemaChange>) -> Table {
    let schema = table.schema().apply_changes(changes).unwrap();
    table
        .file_io()
        .new_output(&table.schema_manager().schema_path(schema.id()))
        .unwrap()
        .write(serde_json::to_vec(&schema).unwrap().into())
        .await
        .unwrap();
    table
        .copy_with_resolved_schema(schema, table.branch())
        .unwrap()
}

async fn scalar_table(family: &str) -> Table {
    evolve(
        &table(Some(true), false).await,
        vec![
            SchemaChange::remove_option("global-index.btree.index-columns".into()),
            SchemaChange::remove_option("global-index.bitmap.index-columns".into()),
            SchemaChange::set_option(
                format!("global-index.{family}.index-columns"),
                "value".into(),
            ),
            SchemaChange::set_option("sorted-index.records-per-range".into(), "64".into()),
            SchemaChange::set_option("global-index.enabled".into(), "true".into()),
        ],
    )
    .await
}

async fn float_messages(table: &Table, id: i32, value: f32) -> Vec<CommitMessage> {
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(vec![id])) as ArrayRef),
        (
            "value",
            Arc::new(Float32Array::from(vec![value])) as ArrayRef,
        ),
    ])
    .unwrap();
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    writer.prepare_commit().await.unwrap()
}

async fn matching_ids(table: &Table, value: Datum, indexed: bool) -> Vec<i32> {
    let table = table.copy_with_options(HashMap::from([(
        "global-index.enabled".into(),
        indexed.to_string(),
    )]));
    let mut read = table.new_read_builder();
    read.with_filter(
        PredicateBuilder::new(table.schema().fields())
            .equal("value", value)
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
    batches
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
        .collect()
}

async fn assert_scalar_complete(table: &Table, family: &str) {
    assert!(table
        .new_global_index_build_builder()
        .with_index_column("value")
        .with_index_type(family)
        .build()
        .await
        .unwrap()
        .is_empty());
}

fn index_source(file: &IndexFileMeta) -> DataEvolutionIndexSourceMeta {
    DataEvolutionIndexSourceMeta::deserialize(
        file.global_index_meta
            .as_ref()
            .unwrap()
            .source_meta
            .as_deref()
            .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn scalar_type_changes_retire_same_shard_payloads_in_both_directions() {
    for family in ["btree", "bitmap"] {
        let original = scalar_table(family).await;
        TableCommit::new(original.clone(), "int-source".into())
            .commit(messages(&original, vec![1]).await)
            .await
            .unwrap();
        let integer_payloads = indexes(&original).await;
        assert_eq!(integer_payloads.len(), 1);
        let changed = evolve(
            &original,
            vec![SchemaChange::UpdateColumnType {
                field_names: vec!["value".into()],
                new_data_type: DataType::Float(FloatType::new()),
                keep_nullability: true,
            }],
        )
        .await;
        TableCommit::new(changed.clone(), "float-source".into())
            .commit(float_messages(&changed, 2, 20.).await)
            .await
            .unwrap();
        for indexed in [false, true] {
            assert_eq!(
                matching_ids(&changed, Datum::Float(10.), indexed).await,
                vec![1],
                "{family}, indexed={indexed}: INT keys cannot be read as FLOAT keys"
            );
        }
        let float_payloads = indexes(&changed).await;
        assert!(integer_payloads
            .iter()
            .all(|entry| !float_payloads.contains(entry)));
        assert_scalar_complete(&changed, family).await;

        // Remove and restore the declaration around the reverse type change.
        // A type guard on currently declared columns alone cannot protect it.
        let key = format!("global-index.{family}.index-columns");
        let removed = evolve(&changed, vec![SchemaChange::remove_option(key.clone())]).await;
        let reverted = evolve(
            &removed,
            vec![SchemaChange::UpdateColumnType {
                field_names: vec!["value".into()],
                new_data_type: DataType::Int(IntType::new()),
                keep_nullability: true,
            }],
        )
        .await;
        let restored = evolve(
            &reverted,
            vec![SchemaChange::set_option(key, "value".into())],
        )
        .await;
        TableCommit::new(restored.clone(), "int-again".into())
            .commit(messages(&restored, vec![3]).await)
            .await
            .unwrap();
        for indexed in [false, true] {
            assert_eq!(
                matching_ids(&restored, Datum::Int(10), indexed).await,
                vec![1],
                "{family}, indexed={indexed}: FLOAT keys cannot survive a type round trip"
            );
        }
        let current = indexes(&restored).await;
        assert!(float_payloads.iter().all(|entry| !current.contains(entry)));
        assert_scalar_complete(&restored, family).await;
    }
}

#[tokio::test]
async fn scalar_writer_option_changes_preserve_compatible_leaf_payloads() {
    for family in ["btree", "bitmap"] {
        let original = scalar_table(family).await;
        TableCommit::new(original.clone(), "original-options".into())
            .commit(messages(&original, vec![1]).await)
            .await
            .unwrap();
        let previous = indexes(&original).await;
        let changed = evolve(
            &original,
            vec![SchemaChange::set_option(
                format!("fields.value.global-index.{family}.index-options"),
                r#"{"compression":"zstd"}"#.into(),
            )],
        )
        .await;
        TableCommit::new(changed.clone(), "changed-options".into())
            .commit(messages(&changed, vec![2]).await)
            .await
            .unwrap();
        let current = indexes(&changed).await;
        assert_eq!(current.len(), 2);
        assert!(previous.iter().all(|entry| current.contains(entry)));
        assert_eq!(matching_ids(&changed, Datum::Int(10), true).await, vec![1]);
        assert_eq!(matching_ids(&changed, Datum::Int(20), true).await, vec![2]);
        assert_scalar_complete(&changed, family).await;
    }
}

#[tokio::test]
async fn stale_scalar_build_keys_record_the_actual_builder_schema_and_are_retired() {
    for family in ["btree", "bitmap"] {
        let original = scalar_table(family).await;
        let raw = original.copy_with_options(HashMap::from([("write-only".into(), "true".into())]));
        TableCommit::new(raw.clone(), "raw-int-source".into())
            .commit(messages(&raw, vec![1]).await)
            .await
            .unwrap();
        let source = latest(&original).await;
        let changed = evolve(
            &original,
            vec![SchemaChange::UpdateColumnType {
                field_names: vec!["value".into()],
                new_data_type: DataType::Float(FloatType::new()),
                keep_nullability: true,
            }],
        )
        .await;
        let raw = changed.copy_with_options(HashMap::from([("write-only".into(), "true".into())]));
        TableCommit::new(raw.clone(), "raw-float-source".into())
            .commit(float_messages(&raw, 2, 20.).await)
            .await
            .unwrap();
        assert!(indexes(&changed).await.is_empty());
        let prepared = original
            .new_global_index_build_builder()
            .with_index_column("value")
            .with_index_type(family)
            .build()
            .await
            .unwrap();
        assert_eq!(prepared.len(), 1);
        let built = index_source(&prepared[0].new_index_files[0]);
        assert_eq!(built.scan_snapshot_id(), latest(&changed).await.id());
        assert_eq!(built.source_schema_id(), original.schema().id());
        TableCommit::new(changed.clone(), "publish-stale-keys".into())
            .commit(prepared)
            .await
            .unwrap();
        let stale = indexes(&changed).await;
        assert_eq!(
            matching_ids(&changed, Datum::Float(10.), true).await,
            vec![1]
        );
        assert_eq!(
            matching_ids(&changed, Datum::Float(20.), true).await,
            vec![2]
        );
        TableCommit::new(changed.clone(), "replace-stale-keys".into())
            .commit(float_messages(&changed, 3, 30.).await)
            .await
            .unwrap();
        let current = indexes(&changed).await;
        assert!(stale.iter().all(|entry| !current.contains(entry)));
        assert_eq!(
            matching_ids(&changed, Datum::Float(10.), true).await,
            vec![1]
        );
        assert_eq!(
            matching_ids(&changed, Datum::Float(20.), true).await,
            vec![2]
        );

        // A current handle may still intentionally build cast keys from an
        // older captured snapshot. The source metadata records both the
        // captured snapshot and the FLOAT builder schema.
        let source_snapshot_id = source.id();
        let prepared = changed
            .new_global_index_build_builder()
            .with_index_column("value")
            .with_index_type(family)
            .build_with_view(
                GlobalIndexBuildView::capture(&changed, Some(source))
                    .await
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(prepared.len(), 1);
        assert_eq!(prepared[0].new_index_files[0].row_count, 1);
        let built = index_source(&prepared[0].new_index_files[0]);
        assert_eq!(built.scan_snapshot_id(), source_snapshot_id);
        assert_eq!(built.source_schema_id(), changed.schema().id());
        TableCommit::new(changed.clone(), "abort-cast-probe".into())
            .abort(&prepared)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn manual_scalar_build_replaces_incompatible_coverage_after_schema_only_change() {
    for family in ["btree", "bitmap"] {
        let original = scalar_table(family).await;
        TableCommit::new(original.clone(), "manual-build-source".into())
            .commit(messages(&original, vec![1]).await)
            .await
            .unwrap();
        let snapshot = latest(&original).await;
        let previous = indexes(&original).await;
        assert_eq!(previous.len(), 1);

        let changed = evolve(
            &original,
            vec![SchemaChange::UpdateColumnType {
                field_names: vec!["value".into()],
                new_data_type: DataType::Float(FloatType::new()),
                keep_nullability: true,
            }],
        )
        .await;
        assert_eq!(latest(&changed).await.id(), snapshot.id());
        assert_eq!(
            matching_ids(&changed, Datum::Float(10.), true).await,
            vec![1]
        );

        let count = changed
            .new_global_index_build_builder()
            .with_index_column("value")
            .with_index_type(family)
            .execute()
            .await
            .unwrap();
        assert_eq!(count, 1);
        let current = indexes(&changed).await;
        assert_eq!(current.len(), 1);
        assert_ne!(
            previous[0].index_file.file_name,
            current[0].index_file.file_name
        );
        let built = index_source(&current[0].index_file);
        assert_eq!(built.scan_snapshot_id(), snapshot.id());
        assert_eq!(built.source_schema_id(), changed.schema().id());
        assert_eq!(
            matching_ids(&changed, Datum::Float(10.), true).await,
            vec![1]
        );
    }
}

#[tokio::test]
async fn scalar_compatibility_uses_each_source_schema_and_composite_field() {
    let original = scalar_table("btree").await;
    TableCommit::new(original.clone(), "composite-field-source".into())
        .commit(messages(&original, vec![1]).await)
        .await
        .unwrap();
    let changed = evolve(
        &original,
        vec![SchemaChange::UpdateColumnType {
            field_names: vec!["value".into()],
            new_data_type: DataType::Float(FloatType::new()),
            keep_nullability: true,
        }],
    )
    .await;
    let mut file = indexes(&original).await.remove(0).index_file;
    let source = index_source(&file);
    let mut schema = crate::table::global_index_schema::GlobalIndexSchema::new(&changed);

    assert!(!schema.compatible(&file).await);
    let meta = file.global_index_meta.as_mut().unwrap();
    let changed_field = meta.index_field_id;
    meta.index_field_id = changed.schema().fields()[0].id();
    assert!(schema.compatible(&file).await);

    file.global_index_meta.as_mut().unwrap().extra_field_ids = Some(vec![changed_field]);
    assert!(!schema.compatible(&file).await);
    for index_type in ["full-text", "ivf-pq"] {
        file.index_type = index_type.into();
        assert!(schema.compatible(&file).await);
    }
    file.index_type = "btree".into();
    file.global_index_meta.as_mut().unwrap().source_meta = Some(
        DataEvolutionIndexSourceMeta::new_with_schema_id(
            source.scan_snapshot_id(),
            changed.schema().id(),
        )
        .unwrap()
        .serialize(),
    );
    assert!(schema.compatible(&file).await);
}

#[tokio::test]
async fn scalar_build_schema_proof_does_not_depend_on_source_snapshot_retention() {
    for family in ["btree", "bitmap"] {
        let original = scalar_table(family).await;
        TableCommit::new(original.clone(), "retained-payload".into())
            .commit(messages(&original, vec![1]).await)
            .await
            .unwrap();
        let previous = indexes(&original).await;
        let source = index_source(&previous[0].index_file);
        let source_path = original
            .snapshot_manager()
            .snapshot_path(source.scan_snapshot_id());
        original.file_io().delete_file(&source_path).await.unwrap();
        assert!(!original.file_io().exists(&source_path).await.unwrap());
        let changed = evolve(
            &original,
            vec![SchemaChange::set_option(
                "read.batch-size".into(),
                "2".into(),
            )],
        )
        .await;
        TableCommit::new(changed.clone(), "append-after-expiration".into())
            .commit(messages(&changed, vec![2]).await)
            .await
            .unwrap();
        let current = indexes(&changed).await;
        assert_eq!(current.len(), 2);
        assert!(previous.iter().all(|entry| current.contains(entry)));
        assert_eq!(matching_ids(&changed, Datum::Int(10), true).await, vec![1]);
        assert_eq!(matching_ids(&changed, Datum::Int(20), true).await, vec![2]);
    }
}

async fn legacy_payloads(table: &Table) -> Vec<IndexManifestEntry> {
    let mut messages = Vec::new();
    for entry in indexes(table).await {
        let old = entry.index_file;
        let mut legacy = old.clone();
        legacy.file_name =
            crate::table::index_file_path::new_global_index_file_name(&old.index_type);
        let mut source = b"DEIX".to_vec();
        source.extend_from_slice(&1i32.to_be_bytes());
        source.extend_from_slice(&index_source(&old).scan_snapshot_id().to_be_bytes());
        legacy.global_index_meta.as_mut().unwrap().source_meta = Some(source);
        table
            .file_io()
            .copy_file(
                &format!("{}/index/{}", table.location(), old.file_name),
                &format!("{}/index/{}", table.location(), legacy.file_name),
            )
            .await
            .unwrap();
        let mut message = CommitMessage::new(entry.partition, entry.bucket, vec![]);
        message.deleted_index_files.push(old);
        message.new_index_files.push(legacy);
        messages.push(message);
    }
    TableCommit::new(table.clone(), "legacy-source-metadata".into())
        .commit(messages)
        .await
        .unwrap();
    indexes(table).await
}

#[tokio::test]
async fn legacy_scalar_files_skip_type_history_validation() {
    for family in ["btree", "bitmap"] {
        for history in ["same-type", "round-trip", "missing"] {
            let original = scalar_table(family).await;
            TableCommit::new(original.clone(), "legacy-source".into())
                .commit(messages(&original, vec![1]).await)
                .await
                .unwrap();
            let previous = legacy_payloads(&original).await;
            let changed = if history == "round-trip" {
                let float = evolve(
                    &original,
                    vec![SchemaChange::UpdateColumnType {
                        field_names: vec!["value".into()],
                        new_data_type: DataType::Float(FloatType::new()),
                        keep_nullability: true,
                    }],
                )
                .await;
                evolve(
                    &float,
                    vec![SchemaChange::UpdateColumnType {
                        field_names: vec!["value".into()],
                        new_data_type: DataType::Int(IntType::new()),
                        keep_nullability: true,
                    }],
                )
                .await
            } else {
                if history == "missing" {
                    original
                        .file_io()
                        .delete_file(&original.schema_manager().schema_path(0))
                        .await
                        .unwrap();
                }
                evolve(
                    &original,
                    vec![SchemaChange::set_option(
                        format!("fields.value.global-index.{family}.index-options"),
                        r#"{"compression":"zstd"}"#.into(),
                    )],
                )
                .await
            };
            TableCommit::new(changed.clone(), "legacy-maintenance".into())
                .commit(messages(&changed, vec![2]).await)
                .await
                .unwrap();
            let current = indexes(&changed).await;
            assert!(
                previous.iter().all(|entry| current.contains(entry)),
                "{family}, history={history}"
            );
            assert_eq!(matching_ids(&changed, Datum::Int(10), true).await, vec![1]);
            assert_eq!(matching_ids(&changed, Datum::Int(20), true).await, vec![2]);
            let before = current;
            TableCommit::new(changed.clone(), "stable-provenance".into())
                .commit(messages(&changed, vec![3]).await)
                .await
                .unwrap();
            let after = indexes(&changed).await;
            assert!(before.iter().all(|entry| after.contains(entry)));
        }
    }
}

async fn text_messages(table: &Table, id: i32, text: &str) -> Vec<CommitMessage> {
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(vec![id])) as ArrayRef),
        (
            "value",
            Arc::new(Int32Array::from(vec![id * 10])) as ArrayRef,
        ),
        ("text", Arc::new(StringArray::from(vec![text])) as ArrayRef),
    ])
    .unwrap();
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    writer.prepare_commit().await.unwrap()
}

async fn text_matches(table: &Table, query: &str, mode: &str) -> Vec<u64> {
    let table = table.copy_with_options(HashMap::from([(
        "full-text-index.search-mode".into(),
        mode.into(),
    )]));
    let mut ids = table
        .new_full_text_search_builder()
        .with_text_column("text")
        .with_query_text(query)
        .with_limit(10)
        .execute()
        .await
        .unwrap()
        .row_ids()
        .unwrap()
        .row_ids
        .clone();
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn full_text_split_read_rechecks_scalar_index_schema() {
    let original = evolve(
        &analyzer_table().await,
        vec![SchemaChange::set_option(
            "global-index.btree.index-columns".into(),
            "value".into(),
        )],
    )
    .await;
    TableCommit::new(original.clone(), "mixed-search-source".into())
        .commit(text_messages(&original, 1, "sentinel").await)
        .await
        .unwrap();
    let changed = evolve(
        &original,
        vec![
            SchemaChange::UpdateColumnType {
                field_names: vec!["value".into()],
                new_data_type: DataType::Float(FloatType::new()),
                keep_nullability: true,
            },
            SchemaChange::set_option("scalar-index.prefilter.search-mode".into(), "full".into()),
        ],
    )
    .await;
    let mut search = changed.new_full_text_search_builder();
    search
        .with_text_column("text")
        .with_query_text("sentinel")
        .with_limit(10);
    let plan = search.new_scan().unwrap().scan().await.unwrap();
    search.with_filter(
        PredicateBuilder::new(changed.schema().fields())
            .equal("value", Datum::Float(10.))
            .unwrap(),
    );

    let direct = search.execute().await.unwrap();
    assert_eq!(direct.row_ids().unwrap().row_ids, vec![0]);
    let split_read = search.new_read().unwrap().read(plan).await.unwrap();
    assert_eq!(split_read.row_ids().unwrap().row_ids, vec![0]);
}

async fn analyzer_table() -> Table {
    evolve(
        &table(Some(true), false).await,
        vec![
            SchemaChange::remove_option("global-index.btree.index-columns".into()),
            SchemaChange::remove_option("global-index.bitmap.index-columns".into()),
            SchemaChange::add_column("text".into(), DataType::VarChar(VarCharType::string_type())),
            SchemaChange::set_option("global-index.full-text.index-columns".into(), "text".into()),
            SchemaChange::set_option("global-index.row-count-per-shard".into(), "64".into()),
        ],
    )
    .await
}

#[tokio::test]
async fn full_text_analyzer_change_rebuilds_same_shard_tokens() {
    let original = analyzer_table().await;
    TableCommit::new(original.clone(), "default-analyzer".into())
        .commit(text_messages(&original, 1, "MiXeD").await)
        .await
        .unwrap();
    let previous = indexes(&original).await;
    assert_eq!(previous.len(), 1);
    let persisted: HashMap<String, String> = serde_json::from_slice(
        previous[0]
            .index_file
            .global_index_meta
            .as_ref()
            .unwrap()
            .index_meta
            .as_ref()
            .unwrap(),
    )
    .unwrap();
    assert!(
        persisted.is_empty(),
        "lower-case=true is the native default"
    );
    assert_eq!(text_matches(&original, "mixed", "fast").await, vec![0]);
    let historical = original.copy_with_pinned_snapshot(Some(&latest(&original).await));

    let changed = evolve(
        &original,
        vec![SchemaChange::set_option(
            "fields.text.global-index.full-text.index-options".into(),
            r#"{"lower-case":false}"#.into(),
        )],
    )
    .await;
    // Both rows remain in the same 64-row shard, and two runs are below the
    // default merge trigger. Definition compatibility must still retire the old analyzer.
    TableCommit::new(changed.clone(), "case-sensitive-analyzer".into())
        .commit(text_messages(&changed, 2, "new").await)
        .await
        .unwrap();
    let native = [
        ("mixed", text_matches(&changed, "mixed", "fast").await),
        ("MiXeD", text_matches(&changed, "MiXeD", "fast").await),
        ("new", text_matches(&changed, "new", "fast").await),
    ];
    assert!(
        native[0].1.is_empty(),
        "the old lower-case analyzer must not produce a false positive"
    );
    assert_eq!(native[1].1, vec![0]);
    assert_eq!(native[2].1, vec![1]);
    let current = indexes(&changed).await;
    assert!(previous.iter().all(|entry| !current.contains(entry)));
    assert_eq!(text_matches(&historical, "mixed", "fast").await, vec![0]);

    changed
        .new_global_index_drop_builder()
        .with_index_column("text")
        .with_index_type("full-text")
        .execute()
        .await
        .unwrap();
    assert!(indexes(&changed).await.is_empty());
    for mode in ["full", "detail"] {
        for (query, expected) in &native {
            assert_eq!(
                text_matches(&changed, query, mode).await,
                *expected,
                "{query}, {mode}"
            );
        }
    }
}

#[tokio::test]
async fn full_text_explicit_default_analyzer_preserves_compatible_leaf() {
    let original = analyzer_table().await;
    TableCommit::new(original.clone(), "implicit-default-analyzer".into())
        .commit(text_messages(&original, 1, "MiXeD").await)
        .await
        .unwrap();
    let previous = indexes(&original).await;
    let changed = evolve(
        &original,
        vec![SchemaChange::set_option(
            "fields.text.global-index.full-text.index-options".into(),
            r#"{"lower-case":true}"#.into(),
        )],
    )
    .await;
    TableCommit::new(changed.clone(), "explicit-default-analyzer".into())
        .commit(text_messages(&changed, 2, "new").await)
        .await
        .unwrap();
    let current = indexes(&changed).await;
    assert_eq!(
        current.len(),
        2,
        "equivalent defaults must retain independent runs"
    );
    assert!(previous.iter().all(|entry| current.contains(entry)));
    assert_eq!(text_matches(&changed, "mixed", "fast").await, vec![0]);
    assert_eq!(text_matches(&changed, "new", "fast").await, vec![1]);
}

#[tokio::test]
async fn full_text_unproven_analyzer_metadata_requires_rebuilding() {
    for index_meta in [None, Some(b"invalid-options".to_vec())] {
        let table = analyzer_table().await;
        TableCommit::new(table.clone(), "known-analyzer".into())
            .commit(text_messages(&table, 1, "MiXeD").await)
            .await
            .unwrap();
        let previous = indexes(&table).await.remove(0);
        let mut legacy = previous.index_file.clone();
        legacy.file_name = crate::table::index_file_path::new_global_index_file_name("full-text");
        legacy.global_index_meta.as_mut().unwrap().index_meta = index_meta;
        table
            .file_io()
            .copy_file(
                &format!(
                    "{}/index/{}",
                    table.location(),
                    previous.index_file.file_name
                ),
                &format!("{}/index/{}", table.location(), legacy.file_name),
            )
            .await
            .unwrap();
        let mut replacement = CommitMessage::new(previous.partition, previous.bucket, vec![]);
        replacement.deleted_index_files.push(previous.index_file);
        replacement.new_index_files.push(legacy.clone());
        TableCommit::new(table.clone(), "unproven-analyzer".into())
            .commit(vec![replacement])
            .await
            .unwrap();
        TableCommit::new(table.clone(), "append-to-unproven-analyzer".into())
            .commit(text_messages(&table, 2, "new").await)
            .await
            .unwrap();
        assert!(indexes(&table)
            .await
            .iter()
            .all(|entry| entry.index_file.file_name != legacy.file_name));
        assert_eq!(text_matches(&table, "mixed", "fast").await, vec![0]);
        assert_eq!(text_matches(&table, "new", "fast").await, vec![1]);
    }
}
