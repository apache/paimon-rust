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
use crate::spec::{ArrayType, FloatType, VarCharType};
use crate::table::maintenance_test_utils::{table as counted_table, MetadataIo};
use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{ArrayRef, StringArray};
use std::collections::BTreeSet;

async fn native_table(kind: &str, options: &str) -> (Table, Arc<MetadataIo>) {
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
        .option("global-index.row-count-per-shard", "4096")
        .option("compaction.min.file-num", "1000")
        .option("commit.min-retry-wait", "0 ms")
        .option("commit.max-retry-wait", "0 ms");
    schema = if kind == "full-text" {
        schema.option("global-index.full-text.index-columns", "text")
    } else {
        schema
            .option("global-index.vector.index-columns", "embedding")
            .option("fields.embedding.global-index.vector.index-type", kind)
            .option(
                "fields.embedding.global-index.vector.index-options",
                options,
            )
    };
    counted_table(TableSchema::new(0, &schema.build().unwrap())).await
}

async fn native_messages(table: &Table, vectors: &[Option<[f32; 2]>]) -> Vec<CommitMessage> {
    let start = table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .and_then(|snapshot| snapshot.next_row_id())
        .unwrap_or(0);
    let ids = (start..start + vectors.len() as i64)
        .map(|id| i32::try_from(id).unwrap())
        .collect::<Vec<_>>();
    let mut list = ListBuilder::new(Float32Builder::new());
    for vector in vectors {
        if let Some(vector) = vector {
            list.values().append_slice(vector);
            list.append(true);
        } else {
            list.append(false);
        }
    }
    let texts = Arc::new(StringArray::from_iter_values(
        ids.iter().map(|id| format!("sentinel row {id}")),
    )) as ArrayRef;
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(ids)) as ArrayRef),
        ("text", texts.clone()),
        ("folded_text", texts),
        ("embedding", Arc::new(list.finish()) as ArrayRef),
    ])
    .unwrap();
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    writer.prepare_commit().await.unwrap()
}

async fn top_vector(table: &Table, mode: &str) -> u64 {
    table
        .new_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vector(vec![1., 0.])
        .with_limit(1)
        .with_options(HashMap::from([(
            "vector-index.search-mode".into(),
            mode.into(),
        )]))
        .execute()
        .await
        .unwrap()
        .row_ids()
        .unwrap()
        .row_ids[0]
}

fn new_payloads(table: &Table, counter: &MetadataIo, before: &BTreeSet<String>) -> usize {
    let prefix = format!("{}/index/", table.location());
    counter
        .observed_paths()
        .difference(before)
        .filter(|path| path.starts_with(&prefix))
        .count()
}

#[tokio::test]
async fn vector_leaf_training_uses_non_null_counts_and_preserves_global_row_offsets() {
    for (kind, options) in [
        (
            "ivf-flat",
            r#"{"dimension":2,"global-index.num-sorted-run.compaction-trigger":5,"global-index.compaction.size-ratio":1,"global-index.compaction.max-size-amplification-percent":200}"#,
        ),
        ("ivf-flat", r#"{"dimension":2,"nlist":64}"#),
        ("ivf-pq", r#"{"dimension":2,"nlist":1,"pq.m":1}"#),
    ] {
        let (table, counter) = native_table(kind, options).await;
        let commit = TableCommit::new(table.clone(), "vector-leaves".into());
        commit
            .commit(native_messages(&table, &[Some([0., 1.]); 8]).await)
            .await
            .unwrap();
        let base = indexes(&table).await;
        let base_sources = counter
            .observed_paths()
            .into_iter()
            .filter(|path| path.ends_with(".parquet"))
            .collect::<Vec<_>>();
        assert!(!base_sources.is_empty());
        counter.watch(base_sources);
        let paths = counter.observed_paths();
        commit
            .commit(native_messages(&table, &[Some([1., 0.])]).await)
            .await
            .unwrap();
        assert!(
            counter.counts().values().all(|count| *count == 0),
            "independent leaves must not read base data: {:?}",
            counter.counts()
        );
        assert_eq!(new_payloads(&table, &counter, &paths), 1);
        let current = indexes(&table).await;
        assert_eq!(current.len(), 2, "{kind}");
        assert!(base.iter().all(|file| current.contains(file)));
        assert_eq!(top_vector(&table, "fast").await, 8, "{kind}");
        let mut sparse = vec![None; 9];
        sparse[8] = Some([2., 0.]);
        commit
            .commit(native_messages(&table, &sparse).await)
            .await
            .unwrap();
        let current = indexes(&table).await;
        assert_eq!(current.len(), 3, "{kind}");
        let leaf = current
            .iter()
            .find(|file| {
                file.index_file
                    .global_index_meta
                    .as_ref()
                    .unwrap()
                    .row_range_start
                    == 9
            })
            .unwrap();
        assert_eq!(
            leaf.index_file.row_count, 9,
            "global metadata counts physical rows, not vectors"
        );
        assert_eq!(
            leaf.index_file
                .global_index_meta
                .as_ref()
                .unwrap()
                .row_range_end,
            17
        );
        assert_eq!(top_vector(&table, "fast").await, 17, "{kind}");
        assert!(current
            .iter()
            .all(|file| file.index_file.index_type == kind));
    }
}

#[tokio::test]
async fn vector_size_budgets_expand_dense_and_sparse_leaves_before_native_training() {
    for sparse in [false, true] {
        let (table, counter) =
            native_table("ivf-flat", r#"{"dimension":2,"max-bytes-per-vector":32}"#).await;
        let commit = TableCommit::new(table.clone(), "budget-leaf".into());
        commit
            .commit(native_messages(&table, &vec![Some([0., 1.]); 1000]).await)
            .await
            .unwrap();
        let base = indexes(&table).await;
        let paths = counter.observed_paths();
        let mut tail = vec![None; if sparse { 9 } else { 1 }];
        let last = tail.len() - 1;
        tail[last] = Some([2., 0.]);
        commit
            .commit(native_messages(&table, &tail).await)
            .await
            .unwrap();
        assert_eq!(
            new_payloads(&table, &counter, &paths),
            1,
            "an untrainable leaf must not produce an intermediate native index"
        );
        let current = indexes(&table).await;
        assert_eq!(current.len(), 1);
        assert!(!current.contains(&base[0]));
        let file = &current[0].index_file;
        assert_eq!(file.index_type, "ivf-flat");
        assert_eq!(file.row_count, 1000 + tail.len() as i64);
        let meta = file.global_index_meta.as_ref().unwrap();
        assert_eq!(
            (meta.row_range_start, meta.row_range_end),
            (0, 999 + tail.len() as i64)
        );
        for mode in ["fast", "full", "detail"] {
            assert_eq!(top_vector(&table, mode).await, 999 + tail.len() as u64);
        }
        assert!(table
            .file_io()
            .exists(&format!(
                "{}/index/{}",
                table.location(),
                base[0].index_file.file_name
            ))
            .await
            .unwrap());
    }
}

#[tokio::test]
async fn all_null_vector_leaves_keep_java_empty_build_semantics_and_recover() {
    let (table, counter) = native_table("ivf-flat", r#"{"dimension":2}"#).await;
    let commit = TableCommit::new(table.clone(), "null-leaf".into());
    commit
        .commit_with_identifier(native_messages(&table, &[Some([0., 1.]); 8]).await, 1)
        .await
        .unwrap();
    let base = indexes(&table).await;
    let paths = counter.observed_paths();
    let input = native_messages(&table, &[None; 3]).await;
    commit
        .commit_with_identifier(input.clone(), 2)
        .await
        .unwrap();
    let completed = latest(&table).await;
    assert_eq!(indexes(&table).await, base);
    assert_eq!(
        new_payloads(&table, &counter, &paths),
        0,
        "Java does not publish zero-byte DE markers"
    );
    commit
        .filter_and_commit_with_identifier(input, 2)
        .await
        .unwrap();
    assert_eq!(latest(&table).await, completed);
    assert_eq!(indexes(&table).await, base);
    commit
        .commit(native_messages(&table, &[Some([2., 0.])]).await)
        .await
        .unwrap();
    let current = indexes(&table).await;
    assert_eq!(current.len(), 2);
    assert!(base.iter().all(|file| current.contains(file)));
    let leaf = current
        .iter()
        .find(|file| {
            file.index_file
                .global_index_meta
                .as_ref()
                .unwrap()
                .row_range_start
                == 8
        })
        .unwrap();
    assert_eq!(leaf.index_file.row_count, 4);
    assert_eq!(top_vector(&table, "fast").await, 11);
}

async fn text_rows(table: &Table) -> BTreeSet<u64> {
    table
        .new_full_text_search_builder()
        .with_text_column("text")
        .with_query_text("sentinel")
        .with_limit(100)
        .execute()
        .await
        .unwrap()
        .row_ids()
        .unwrap()
        .row_ids
        .iter()
        .copied()
        .collect()
}

#[tokio::test]
async fn full_text_leaves_and_tiered_merges_preserve_hits_filters_and_history() {
    let (table, counter) = native_table("full-text", "{}").await;
    let commit = TableCommit::new(table.clone(), "text-lsm".into());
    commit
        .commit(native_messages(&table, &[None; 8]).await)
        .await
        .unwrap();
    let base = indexes(&table).await;
    let mut history = None;
    for row in 8..12 {
        let paths = counter.observed_paths();
        commit
            .commit(native_messages(&table, &[None]).await)
            .await
            .unwrap();
        assert_eq!(
            new_payloads(&table, &counter, &paths),
            1,
            "one final payload per append or chosen merge"
        );
        assert_eq!(text_rows(&table).await, (0..=row).collect());
        if row == 10 {
            history = Some(latest(&table).await);
        }
    }
    let current = indexes(&table).await;
    assert_eq!(current.len(), 2);
    assert!(base.iter().all(|file| current.contains(file)));
    let pinned = table.copy_with_pinned_snapshot(history.as_ref());
    assert_eq!(text_rows(&pinned).await, (0..11).collect());
    let predicate = PredicateBuilder::new(table.schema().fields())
        .equal("id", Datum::Int(11))
        .unwrap();
    for (mode, expected) in [("fast", vec![]), ("full", vec![11]), ("detail", vec![11])] {
        let filtered = table.copy_with_options(HashMap::from([(
            "scalar-index.search-mode".into(),
            mode.into(),
        )]));
        let result = filtered
            .new_full_text_search_builder()
            .with_text_column("text")
            .with_query_text("sentinel")
            .with_filter(predicate.clone())
            .with_limit(1)
            .execute()
            .await
            .unwrap();
        assert_eq!(result.row_ids().unwrap().row_ids, expected, "{mode}");
    }
    let result = table
        .new_full_text_search_builder()
        .with_text_column("text")
        .with_query_text("sentinel")
        .with_include_row_ids(std::iter::once(8_u64).collect())
        .with_limit(1)
        .execute()
        .await
        .unwrap();
    assert_eq!(result.row_ids().unwrap().row_ids, vec![8]);
}
