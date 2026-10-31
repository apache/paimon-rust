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
use crate::catalog::{Catalog, FileSystemCatalog};
use crate::spec::{ArrayType, FloatType, SchemaChange, VarCharType};
use crate::Options;
use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{ArrayRef, StringArray};

#[tokio::test]
async fn manual_builds_and_planned_shards_share_declarations_and_preserve_explicit_overrides() {
    for override_metric in [None, Some("l2")] {
        let (_catalog, table) = search_table(&[
            ("write-only", "true"),
            ("global-index.vector.index-columns", "embedding"),
            (
                "fields.embedding.global-index.vector.index-type",
                "ivf-flat",
            ),
            (
                "fields.embedding.global-index.vector.distance-metric",
                "cosine",
            ),
            (
                "fields.embedding.global-index.vector.index-options",
                r#"{"dimension":2,"nlist":1}"#,
            ),
            ("fields.embedding.distance.metric", "l2"),
            ("global-index.full-text.index-columns", "text"),
            (
                "fields.text.global-index.full-text.index-options",
                r#"{"lower-case":false}"#,
            ),
        ])
        .await;
        let commit = TableCommit::new(table.clone(), "writer".into());
        commit
            .commit(search_messages(&table, &[[100., 1.], [1., 1.]]).await)
            .await
            .unwrap();
        assert!(indexes(&table).await.is_empty());
        let before = vector_scores(&table, "full").await;
        let mut vector = table.new_global_index_build_builder();
        vector
            .with_index_column("embedding")
            .with_index_type("ivf-flat");
        if let Some(metric) = override_metric {
            // Native aliases in explicit builder options override qualified declarations.
            vector.with_options(HashMap::from([("metric".into(), metric.into())]));
            assert_eq!(vector.execute().await.unwrap(), 1);
        } else {
            let plan = vector.plan().await.unwrap();
            let mut prepared = Vec::new();
            for shard in plan.shards() {
                prepared.extend(vector.build_shard(&shard).await.unwrap());
            }
            assert_eq!(prepared.len(), 1);
            commit.commit(prepared).await.unwrap();
        }
        let mut text = table.new_global_index_build_builder();
        text.with_index_column("text").with_index_type("full-text");
        if override_metric.is_some() {
            text.with_options(HashMap::from([(
                "full-text.lower-case".into(),
                "true".into(),
            )]));
        }
        assert_eq!(text.execute().await.unwrap(), 1);
        for mode in ["full", "detail"] {
            let after = vector_scores(&table, mode).await;
            if override_metric.is_none() {
                assert_eq!(after.row_ids, before.row_ids);
                for (raw, indexed) in before.scores.iter().zip(&after.scores) {
                    assert!((raw - indexed).abs() < 1e-5);
                }
            } else {
                assert_eq!(after.row_ids, vec![1, 0]);
                assert!((after.scores[0] - 0.5).abs() < 1e-5);
            }
            let view = table.copy_with_options(HashMap::from([(
                "full-text-index.search-mode".into(),
                mode.into(),
            )]));
            let result = view
                .new_full_text_search_builder()
                .with_text_column("text")
                .with_query_text("foo")
                .with_limit(10)
                .execute()
                .await
                .unwrap();
            let actual = result
                .row_ids()
                .unwrap()
                .row_ids
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>();
            let expected = if override_metric.is_some() {
                vec![0, 1]
            } else {
                vec![1]
            };
            assert_eq!(actual, expected.into_iter().collect());
        }
    }
}

async fn search_table(options: &[(&str, &str)]) -> (FileSystemCatalog, Table) {
    let catalog = FileSystemCatalog::new(Options::from_map(HashMap::from([(
        "warehouse".into(),
        format!("memory:/auto-index-search/{}", uuid::Uuid::new_v4()),
    )])))
    .unwrap();
    catalog
        .create_database("default", false, HashMap::new())
        .await
        .unwrap();
    let mut schema = TableSchemaBuilder::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("text", DataType::VarChar(VarCharType::string_type()))
        .column("folded_text", DataType::VarChar(VarCharType::string_type()))
        .column(
            "embedding",
            DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
        )
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        .option("global-index.enabled", "true")
        .option("global-index.row-count-per-shard", "2")
        .option("compaction.min.file-num", "1000")
        .option("commit.min-retry-wait", "0 ms")
        .option("commit.max-retry-wait", "0 ms");
    for (key, value) in options {
        schema = schema.option(*key, *value);
    }
    let id = Identifier::new("default", "search");
    catalog
        .create_table(&id, schema.build().unwrap(), false)
        .await
        .unwrap();
    let table = catalog.get_table(&id).await.unwrap();
    (catalog, table)
}

async fn search_messages(table: &Table, vectors: &[[f32; 2]]) -> Vec<CommitMessage> {
    let mut builder = ListBuilder::new(Float32Builder::new());
    for vector in vectors {
        builder.values().append_slice(vector);
        builder.append(true);
    }
    let texts = (0..vectors.len())
        .map(|i| if i % 2 == 0 { "FOO" } else { "foo" })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int32Array::from(
                (0..vectors.len() as i32).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "text",
            Arc::new(StringArray::from(texts.clone())) as ArrayRef,
        ),
        (
            "folded_text",
            Arc::new(StringArray::from(texts)) as ArrayRef,
        ),
        ("embedding", Arc::new(builder.finish()) as ArrayRef),
    ])
    .unwrap();
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    writer.prepare_commit().await.unwrap()
}

async fn vector_scores(table: &Table, mode: &str) -> crate::vector_search::ScoredRowIds {
    table
        .new_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vector(vec![1., 0.])
        .with_limit(4)
        .with_options(HashMap::from([(
            "vector-index.search-mode".into(),
            mode.into(),
        )]))
        .execute()
        .await
        .unwrap()
        .row_ids()
        .unwrap()
        .clone()
}

#[tokio::test]
async fn full_and_detail_vector_fallback_share_declared_metrics_with_indexes() {
    for metric in [None, Some("cosine"), Some("l2")] {
        let mut options = vec![
            ("global-index.vector.index-columns", "embedding"),
            (
                "fields.embedding.global-index.vector.index-type",
                "ivf-flat",
            ),
            (
                "fields.embedding.global-index.vector.index-options",
                r#"{"dimension":2,"nlist":1}"#,
            ),
            // A declared metric (including its default) has the same build-over-table
            // precedence as explicit native builder options.
            ("fields.embedding.distance.metric", "l2"),
            ("fields.embedding.index-type", "diskann"),
        ];
        if let Some(metric) = metric {
            options.push((
                "fields.embedding.global-index.vector.distance-metric",
                metric,
            ));
        }
        let (_catalog, table) = search_table(&options).await;
        TableCommit::new(table.clone(), "writer".into())
            .commit(search_messages(&table, &[[100., 1.], [1., 1.]]).await)
            .await
            .unwrap();
        let data = table.snapshot_manager().get_snapshot(1).await.unwrap();
        let raw = table.copy_with_pinned_snapshot(Some(&data));
        for mode in ["full", "detail"] {
            let before = vector_scores(&raw, mode).await;
            let after = vector_scores(&table, mode).await;
            let expected = if metric == Some("l2") {
                vec![1, 0]
            } else {
                vec![0, 1]
            };
            assert_eq!(before.row_ids, expected, "{metric:?} {mode}: raw ranking");
            assert_eq!(
                after.row_ids, expected,
                "{metric:?} {mode}: indexed ranking"
            );
            for (raw, indexed) in before.scores.iter().zip(&after.scores) {
                assert!(
                    (raw - indexed).abs() < 1e-5,
                    "{metric:?} {mode}: {raw} != {indexed}"
                );
            }
            let expected_score = match metric {
                None => 100.,
                Some("cosine") => 100. / 10001_f32.sqrt(),
                Some("l2") => 0.5,
                _ => unreachable!(),
            };
            assert!((before.scores[0] - expected_score).abs() < 1e-5);
        }
        // Explicit query metrics retain precedence over the table declaration.
        let overridden = raw
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1., 0.])
            .with_limit(1)
            .with_options(HashMap::from([
                ("vector-index.search-mode".into(), "full".into()),
                ("metric".into(), "l2".into()),
            ]))
            .execute()
            .await
            .unwrap();
        assert_eq!(overridden.row_ids().unwrap().row_ids, vec![1]);
    }
}

#[tokio::test]
async fn full_and_detail_full_text_fallback_share_per_column_options_in_mixed_coverage() {
    let (_catalog, table) = search_table(&[
        ("global-index.full-text.index-columns", "text,folded_text"),
        (
            "fields.text.global-index.full-text.index-options",
            r#"{"lower-case":false}"#,
        ),
        (
            "fields.folded_text.global-index.full-text.index-options",
            r#"{"lower-case":true}"#,
        ),
    ])
    .await;
    let commit = TableCommit::new(table.clone(), "writer".into());
    for _ in 0..2 {
        commit
            .commit(search_messages(&table, &[[1., 0.], [1., 0.]]).await)
            .await
            .unwrap();
    }
    for snapshot_id in 1..=4 {
        let snapshot = table
            .snapshot_manager()
            .get_snapshot(snapshot_id)
            .await
            .unwrap();
        let view = table.copy_with_pinned_snapshot(Some(&snapshot));
        let count = if snapshot_id <= 2 { 2 } else { 4 };
        for mode in ["full", "detail"] {
            let view = view.copy_with_options(HashMap::from([(
                "full-text-index.search-mode".into(),
                mode.into(),
            )]));
            for column in ["text", "folded_text"] {
                let result = view
                    .new_full_text_search_builder()
                    .with_text_column(column)
                    .with_query_text("foo")
                    .with_limit(10)
                    .execute()
                    .await
                    .unwrap();
                let actual = result
                    .row_ids()
                    .unwrap()
                    .row_ids
                    .iter()
                    .copied()
                    .collect::<std::collections::BTreeSet<_>>();
                let expected = (0..count)
                    .filter(|id| column == "folded_text" || id % 2 == 1)
                    .collect::<std::collections::BTreeSet<_>>();
                assert_eq!(
                    actual, expected,
                    "snapshot={snapshot_id} mode={mode} column={column}"
                );
            }
        }
    }
}

#[tokio::test]
async fn vector_algorithm_and_metric_changes_atomically_replace_incompatible_payloads() {
    for (key, value, expected_type, expected_metric) in [
        (
            "fields.embedding.global-index.vector.index-type",
            "ivf-sq",
            "ivf-sq",
            "inner_product",
        ),
        (
            "fields.embedding.global-index.vector.distance-metric",
            "cosine",
            "ivf-flat",
            "cosine",
        ),
    ] {
        let (catalog, table) = search_table(&[
            ("global-index.vector.index-columns", "embedding"),
            (
                "fields.embedding.global-index.vector.index-type",
                "ivf-flat",
            ),
            (
                "fields.embedding.global-index.vector.index-options",
                r#"{"dimension":2,"nlist":1}"#,
            ),
        ])
        .await;
        TableCommit::new(table.clone(), "seed".into())
            .commit(search_messages(&table, &[[100., 1.], [1., 1.]]).await)
            .await
            .unwrap();
        let historical = indexes(&table).await;
        let snapshot = latest(&table).await;
        catalog
            .alter_table(
                table.identifier(),
                vec![SchemaChange::set_option(key.into(), value.into())],
                false,
            )
            .await
            .unwrap();
        let changed = catalog.get_table(table.identifier()).await.unwrap();
        let input = search_messages(&changed, &[[2., 0.], [0., 1.]]).await;
        let commit = TableCommit::new(changed.clone(), "writer".into());
        let mut interrupted = TableCommit::new(
            changed.copy_with_options(HashMap::from([("commit.max-retries".into(), "0".into())])),
            "writer".into(),
        );
        interrupted.snapshot_commit = Arc::new(InjectIndexPublication {
            table: changed.clone(),
            mode: Publication::FailBeforePublish,
            fired: AtomicBool::new(false),
        });
        assert!(interrupted
            .commit_with_identifier(input.clone(), 7)
            .await
            .is_err());
        assert_eq!(latest(&changed).await.id(), 3);
        assert_eq!(
            indexes(&changed).await,
            historical,
            "failed replacement must retain all previous references"
        );
        commit
            .filter_and_commit_with_identifier(input.clone(), 7)
            .await
            .unwrap();
        assert_eq!(
            latest(&changed).await.id(),
            4,
            "replace every incompatible shard in one publication"
        );
        let current = indexes(&changed).await;
        assert_eq!(current.len(), 2);
        for entry in &current {
            assert_eq!(entry.index_file.index_type, expected_type);
            assert!(
                !historical.contains(entry),
                "every incompatible full shard must be replaced"
            );
            let native: HashMap<String, String> = serde_json::from_slice(
                entry
                    .index_file
                    .global_index_meta
                    .as_ref()
                    .unwrap()
                    .index_meta
                    .as_ref()
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(native["metric"], expected_metric);
        }
        for mode in ["full", "detail"] {
            let scores = vector_scores(&changed, mode).await;
            assert_eq!(scores.row_ids.len(), 4);
            if expected_metric == "cosine" {
                assert_eq!(scores.row_ids, vec![2, 0, 1, 3]);
                assert!((scores.scores[0] - 1.).abs() < 1e-5);
            }
            assert_eq!(
                vector_scores(&table.copy_with_pinned_snapshot(Some(&snapshot)), mode)
                    .await
                    .row_ids
                    .len(),
                2
            );
        }
        // The data snapshot retains the previous definition until the one replacement commit.
        let pending = changed.snapshot_manager().get_snapshot(3).await.unwrap();
        let pending = changed.copy_with_pinned_snapshot(Some(&pending));
        let pending_snapshot = pending.travel_snapshot().unwrap();
        let entries = IndexManifest::read(
            changed.file_io(),
            &changed
                .snapshot_manager()
                .manifest_path(pending_snapshot.index_manifest().unwrap()),
        )
        .await
        .unwrap();
        assert_eq!(entries, historical);
        for entry in historical {
            assert!(changed
                .file_io()
                .exists(&format!(
                    "{}/index/{}",
                    changed.location(),
                    entry.index_file.file_name
                ))
                .await
                .unwrap());
        }
        let completed = latest(&changed).await.id();
        commit
            .filter_and_commit_with_identifier(input, 7)
            .await
            .unwrap();
        assert_eq!(
            latest(&changed).await.id(),
            completed,
            "recovery must not publish another replacement"
        );
    }
}
