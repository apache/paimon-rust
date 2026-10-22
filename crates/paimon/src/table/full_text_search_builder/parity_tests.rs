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

//! Real index/read coverage for the public full-text plan contract.
use super::*;
use crate::catalog::Identifier;
use crate::io::FileIOBuilder;
use crate::spec::{DataType, Datum, IntType, PredicateBuilder, Schema, TableSchema, VarCharType};
use crate::table::{TableCommit, TableWrite};
use arrow_array::{ArrayRef, Int32Array};
use arrow_schema::{DataType as ArrowType, Field, Schema as ArrowSchema};
use std::sync::Arc;

#[path = "append_tests.rs"]
mod append_tests;

fn new_schema(partitioned: bool) -> Schema {
    let mut schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("text", DataType::VarChar(VarCharType::string_type()))
        .column("label", DataType::VarChar(VarCharType::string_type()))
        .column("pt", DataType::Int(IntType::new()))
        .options(HashMap::from([
            ("bucket".into(), "-1".into()),
            ("row-tracking.enabled".into(), "true".into()),
            ("data-evolution.enabled".into(), "true".into()),
            ("global-index.enabled".into(), "true".into()),
            ("global-index.row-count-per-shard".into(), "4".into()),
            ("scalar-index.search-mode".into(), "full".into()),
            ("full-text-index.search-mode".into(), "full".into()),
        ]));
    if partitioned {
        schema = schema.partition_keys(["pt"]);
    }
    schema.build().unwrap()
}

fn new_table(partitioned: bool) -> Table {
    let schema = new_schema(partitioned);
    Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("default", "text"),
        format!("memory:/text-parity-{}", uuid::Uuid::new_v4()),
        TableSchema::new(0, &schema),
        None,
    )
}

async fn append(table: &Table, start: i32, count: i32) {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", ArrowType::Int32, true),
        Field::new("text", ArrowType::Utf8, true),
        Field::new("label", ArrowType::Utf8, true),
        Field::new("pt", ArrowType::Int32, true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from((start..start + count).collect::<Vec<_>>())),
        Arc::new(StringArray::from(
            (start..start + count)
                .map(|id| if id == 3 { None } else { Some("paimon lake") })
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            (start..start + count)
                .map(|id| if id % 2 == 1 { "keep" } else { "drop" })
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int32Array::from(
            (start..start + count).map(|id| id / 4).collect::<Vec<_>>(),
        )),
    ];
    let mut write = TableWrite::new(table, "data".into()).unwrap();
    write
        .write_arrow_batch(&RecordBatch::try_new(schema, columns).unwrap())
        .await
        .unwrap();
    TableCommit::new(table.clone(), "data".into())
        .commit(write.prepare_commit().await.unwrap())
        .await
        .unwrap();
}

async fn index(table: &Table, kind: &str, column: &str) {
    table
        .new_global_index_build_builder()
        .with_index_type(kind)
        .with_index_column(column)
        .execute()
        .await
        .unwrap();
}

async fn append_labels(table: &Table, start: i32, labels: &[Option<&str>]) {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", ArrowType::Int32, true),
        Field::new("text", ArrowType::Utf8, true),
        Field::new("label", ArrowType::Utf8, true),
        Field::new("pt", ArrowType::Int32, true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from_iter_values(
            start..start + labels.len() as i32,
        )),
        Arc::new(StringArray::from(vec!["paimon"; labels.len()])),
        Arc::new(StringArray::from(labels.to_vec())),
        Arc::new(Int32Array::from(vec![0; labels.len()])),
    ];
    let mut write = TableWrite::new(table, "data".into()).unwrap();
    write
        .write_arrow_batch(&RecordBatch::try_new(schema, columns).unwrap())
        .await
        .unwrap();
    TableCommit::new(table.clone(), "data".into())
        .commit(write.prepare_commit().await.unwrap())
        .await
        .unwrap();
}

fn query(table: &Table) -> FullTextSearchBuilder<'_> {
    let mut builder = table.new_full_text_search_builder();
    builder
        .with_query("text", r#"{"match":{"query":"paimon"}}"#)
        .with_limit(64);
    builder
}

fn ids(result: crate::table::GlobalIndexResult) -> Vec<u64> {
    let mut ids = result.into_row_ids().unwrap().row_ids;
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn full_text_result_reads_projection_and_scores_from_the_captured_snapshot() {
    let table = new_table(false);
    append(&table, 0, 8).await;
    index(&table, "full-text", "text").await;
    let result = query(&table).execute_scored().await.unwrap();
    let snapshot = result.snapshot_id().unwrap();
    let expected = result.row_ids().unwrap();
    assert!(result.positions().is_err());
    append(&table, 8, 4).await;
    assert!(
        table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await
            .unwrap()
            .unwrap()
            > snapshot
    );

    let batches: Vec<RecordBatch> = result
        .new_read_builder()
        .with_projection(&["label", "id"])
        .read()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut user_ids = Vec::new();
    let mut scores = Vec::new();
    for batch in batches {
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            ["label", "id", "__paimon_search_score"]
        );
        user_ids.extend_from_slice(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
        );
        scores.extend_from_slice(
            batch
                .column(2)
                .as_any()
                .downcast_ref::<arrow_array::Float32Array>()
                .unwrap()
                .values(),
        );
    }
    assert_eq!(
        user_ids,
        expected
            .row_ids
            .iter()
            .map(|id| *id as i32)
            .collect::<Vec<_>>()
    );
    assert_eq!(scores, expected.scores);
    let batches: Vec<RecordBatch> = result
        .new_read_builder()
        .with_projection(&[])
        .read()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        expected.len()
    );
    assert!(batches.iter().all(|batch| batch.num_columns() == 1));
    assert!(result
        .new_read_builder()
        .with_projection(&["missing"])
        .read()
        .await
        .is_err());
}

#[tokio::test]
async fn scalar_modes_and_refinement_follow_java_before_top_k() {
    let table = new_table(false);
    append(&table, 0, 8).await;
    index(&table, "full-text", "text").await;
    let pb = PredicateBuilder::new(table.schema().fields());
    let keep = pb.equal("label", Datum::String("keep".into())).unwrap();
    for (mode, expected) in [
        ("fast", vec![]),
        ("full", vec![1, 5, 7]),
        ("detail", vec![1, 5, 7]),
    ] {
        let table = table.copy_with_options(HashMap::from([(
            "scalar-index.search-mode".into(),
            mode.into(),
        )]));
        assert_eq!(
            ids(query(&table)
                .with_filter(keep.clone())
                .execute_scored()
                .await
                .unwrap()),
            expected
        );
    }
    index(&table, "btree", "label").await;
    let fast = table.copy_with_options(HashMap::from([(
        "scalar-index.search-mode".into(),
        "fast".into(),
    )]));
    assert_eq!(
        ids(query(&fast)
            .with_filter(keep)
            .execute_scored()
            .await
            .unwrap()),
        vec![1, 5, 7]
    );
    let candidate = pb.contains("label", Datum::String("eep".into())).unwrap();
    assert_eq!(
        ids(query(&fast)
            .with_filter(candidate.clone())
            .execute_scored()
            .await
            .unwrap()),
        vec![1, 5, 7]
    );
    let no_refine = fast.copy_with_options(HashMap::from([(
        "global-index.filter.refine-from-data".into(),
        "false".into(),
    )]));
    assert!(query(&no_refine)
        .with_filter(candidate)
        .execute_scored()
        .await
        .unwrap()
        .is_empty());
    let high = pb.greater_or_equal("id", Datum::Int(5)).unwrap();
    let mut search = query(&table);
    search.with_limit(1).with_filter(high);
    assert_eq!(ids(search.execute_scored().await.unwrap()), vec![5]);
}

#[tokio::test]
async fn empty_string_filters_preserve_java_search_refinement() {
    for kind in ["btree", "bitmap"] {
        let table = new_table(false);
        append_labels(
            &table,
            0,
            &[
                Some(""),
                None,
                Some("keep"),
                Some("drop"),
                Some(""),
                Some("keep"),
                Some("drop"),
                None,
            ],
        )
        .await;
        index(&table, "full-text", "text").await;
        index(&table, kind, "label").await;
        let pb = PredicateBuilder::new(table.schema().fields());
        let filters = [
            (
                "like-empty",
                pb.like("label", Datum::String("".into()), None).unwrap(),
            ),
            (
                "like-percent",
                pb.like("label", Datum::String("%".into()), None).unwrap(),
            ),
            (
                "like-double-percent",
                pb.like("label", Datum::String("%%".into()), None).unwrap(),
            ),
            (
                "contains",
                pb.contains("label", Datum::String("".into())).unwrap(),
            ),
            (
                "ends-with",
                pb.ends_with("label", Datum::String("".into())).unwrap(),
            ),
            (
                "starts-with",
                pb.starts_with("label", Datum::String("".into())).unwrap(),
            ),
        ];
        for partial in [false, true] {
            if partial {
                append_labels(&table, 8, &[Some(""), None, Some("keep"), Some("drop")]).await;
            }
            for refine in [false, true] {
                let table = table.copy_with_options(HashMap::from([(
                    "global-index.filter.refine-from-data".into(),
                    refine.to_string(),
                )]));
                for (name, filter) in &filters {
                    let mut expected = if refine || *name == "starts-with" {
                        if *name == "like-empty" {
                            vec![0, 4]
                        } else {
                            vec![0, 2, 3, 4, 5, 6]
                        }
                    } else {
                        vec![]
                    };
                    if partial {
                        expected.extend(if *name == "like-empty" {
                            vec![8]
                        } else {
                            vec![8, 10, 11]
                        });
                    }
                    assert_eq!(
                        ids(query(&table)
                            .with_filter(filter.clone())
                            .execute_scored()
                            .await
                            .unwrap()),
                        expected,
                        "index={kind}, filter={name}, refine={refine}, partial={partial}",
                    );
                }
            }
        }
    }
}

async fn user_ids(table: &Table, result: crate::table::GlobalIndexResult) -> Vec<i32> {
    let mut builder = table.new_read_builder();
    builder.with_projection(&["id", ROW_ID_FIELD_NAME]).unwrap();
    let plan = builder.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut mapping = HashMap::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let row_ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            mapping.insert(row_ids.value(row) as u64, ids.value(row));
        }
    }
    let mut values = result
        .row_ids()
        .unwrap()
        .row_ids
        .iter()
        .map(|id| mapping[id])
        .collect::<Vec<_>>();
    values.sort_unstable();
    values
}

#[tokio::test]
async fn partition_filters_apply_to_indexed_and_raw_corpora() {
    let table = new_table(true);
    append(&table, 0, 8).await;
    index(&table, "full-text", "text").await;
    append(&table, 8, 4).await;
    let pb = PredicateBuilder::new(table.schema().fields());
    let filter = pb.greater_or_equal("pt", Datum::Int(1)).unwrap();
    for mode in ["full", "detail"] {
        let table = table.copy_with_options(HashMap::from([(
            "full-text-index.search-mode".into(),
            mode.into(),
        )]));
        assert_eq!(
            user_ids(
                &table,
                query(&table)
                    .with_partition_filter(filter.clone())
                    .unwrap()
                    .execute_scored()
                    .await
                    .unwrap()
            )
            .await,
            (4..12).collect::<Vec<_>>()
        );
        let filter = Predicate::and(vec![
            filter.clone(),
            pb.equal("label", Datum::String("keep".into())).unwrap(),
        ]);
        assert_eq!(
            user_ids(
                &table,
                query(&table)
                    .with_filter(filter)
                    .execute_scored()
                    .await
                    .unwrap()
            )
            .await,
            vec![5, 7, 9, 11]
        );
    }
}

#[tokio::test]
async fn scan_read_pins_snapshot_and_keeps_query_configuration_on_reader() {
    let table = new_table(false);
    append(&table, 0, 8).await;
    index(&table, "full-text", "text").await;
    let builder = query(&table);
    let scan = builder.new_scan().unwrap();
    let read = builder.new_read().unwrap();
    let plan = scan.scan().await.unwrap();
    assert_eq!(plan.snapshot_id(), Some(2));
    append(&table, 8, 4).await;
    assert_eq!(
        ids(read.read(plan.clone()).await.unwrap()),
        vec![0, 1, 2, 4, 5, 6, 7]
    );
    assert_eq!(
        ids(query(&table).execute_scored().await.unwrap()),
        vec![0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11]
    );
    let pb = PredicateBuilder::new(table.schema().fields());
    let mut other = query(&table);
    other.with_filter(pb.equal("id", Datum::Int(1)).unwrap());
    assert_eq!(
        ids(other.new_read().unwrap().read(plan.clone()).await.unwrap()),
        vec![1]
    );
    let foreign = new_table(false);
    assert!(query(&foreign)
        .new_read()
        .unwrap()
        .read(plan)
        .await
        .is_err());
}

#[tokio::test]
async fn empty_plans_stay_empty_after_append_and_validate_query_first() {
    let table = new_table(false);
    let builder = query(&table);
    let plan = builder.new_scan().unwrap().scan().await.unwrap();
    assert_eq!(plan.snapshot_id(), None);
    append(&table, 0, 4).await;
    index(&table, "full-text", "text").await;
    assert!(builder
        .new_read()
        .unwrap()
        .read(plan)
        .await
        .unwrap()
        .is_empty());
    let empty = new_table(false);
    assert!(query(&empty)
        .with_limit(0)
        .execute_scored()
        .await
        .unwrap_err()
        .to_string()
        .contains("positive"));
    assert!(query(&empty)
        .with_query("missing", r#"{"match":{"query":"paimon"}}"#)
        .new_scan()
        .is_err());
    assert!(query(&empty)
        .with_query("id", r#"{"match":{"query":"paimon"}}"#)
        .new_scan()
        .is_err());
}

#[tokio::test]
async fn raw_filter_keeps_bm25_statistics_of_the_whole_corpus() {
    let table = new_table(false);
    append(&table, 0, 4).await;
    index(&table, "full-text", "text").await;
    append(&table, 4, 8).await;
    let unfiltered = query(&table).execute_scored().await.unwrap();
    let pb = PredicateBuilder::new(table.schema().fields());
    let filtered = query(&table)
        .with_filter(pb.equal("label", Datum::String("keep".into())).unwrap())
        .execute_scored()
        .await
        .unwrap();
    assert_eq!(ids(filtered.clone()), vec![1, 5, 7, 9, 11]);
    let unfiltered = unfiltered.into_row_ids().unwrap();
    let filtered = filtered.into_row_ids().unwrap();
    let scores: HashMap<_, _> = unfiltered
        .row_ids
        .into_iter()
        .zip(unfiltered.scores)
        .collect();
    for (id, score) in filtered.row_ids.into_iter().zip(filtered.scores) {
        assert_eq!(score, scores[&id]);
    }
}

#[tokio::test]
async fn snapshot_pinning_preserves_schema_only_renames() {
    use crate::catalog::{Catalog, FileSystemCatalog};
    use crate::common::Options;
    use crate::spec::SchemaChange;

    let catalog = FileSystemCatalog::new(Options::from_map(HashMap::from([(
        "warehouse".into(),
        format!("memory:/text-schema-rename-{}", uuid::Uuid::new_v4()),
    )])))
    .unwrap();
    catalog
        .create_database("default", false, HashMap::new())
        .await
        .unwrap();
    let identifier = Identifier::new("default", "text");
    catalog
        .create_table(&identifier, new_schema(false), false)
        .await
        .unwrap();
    let table = catalog.get_table(&identifier).await.unwrap();
    append(&table, 0, 4).await;
    index(&table, "full-text", "text").await;
    append(&table, 4, 4).await;
    catalog
        .alter_table(
            &identifier,
            vec![SchemaChange::rename_column("text".into(), "body".into())],
            false,
        )
        .await
        .unwrap();
    let table = catalog.get_table(&identifier).await.unwrap();
    for mode in ["full", "detail"] {
        let table = table.copy_with_options(HashMap::from([(
            "full-text-index.search-mode".into(),
            mode.into(),
        )]));
        let mut builder = table.new_full_text_search_builder();
        builder
            .with_query("body", r#"{"match":{"query":"paimon"}}"#)
            .with_limit(64);
        assert_eq!(
            ids(builder.execute_scored().await.unwrap()),
            vec![0, 1, 2, 4, 5, 6, 7]
        );
        let plan = builder.new_scan().unwrap().scan().await.unwrap();
        let plan: FullTextScanPlan =
            serde_json::from_slice(&serde_json::to_vec(&plan).unwrap()).unwrap();
        assert_eq!(
            ids(builder.new_read().unwrap().read(plan).await.unwrap()),
            vec![0, 1, 2, 4, 5, 6, 7]
        );
    }
}

#[tokio::test]
async fn scalar_search_pre_filters_exclude_composite_indexes() {
    let table = new_table(false);
    append(&table, 0, 8).await;
    index(&table, "full-text", "text").await;
    table
        .new_global_index_build_builder()
        .with_index_type("btree")
        .with_index_columns(&["label", "id"])
        .execute()
        .await
        .unwrap();
    let pb = PredicateBuilder::new(table.schema().fields());
    let predicate = Predicate::and(vec![
        pb.equal("label", Datum::String("keep".into())).unwrap(),
        pb.greater_than("id", Datum::Int(0)).unwrap(),
    ]);
    for (mode, expected) in [
        ("fast", vec![]),
        ("full", vec![1, 5, 7]),
        ("detail", vec![1, 5, 7]),
    ] {
        let table = table.copy_with_options(HashMap::from([(
            "scalar-index.search-mode".into(),
            mode.into(),
        )]));
        assert_eq!(
            ids(query(&table)
                .with_filter(predicate.clone())
                .execute_scored()
                .await
                .unwrap()),
            expected
        );
    }
}

struct WireExecutor {
    table: Table,
    work: std::sync::Mutex<Vec<serde_json::Value>>,
    batch_sizes: std::sync::Mutex<Vec<usize>>,
    corrupt_scores: bool,
}

#[async_trait::async_trait]
impl crate::table::SearchTaskExecutor for WireExecutor {
    async fn execute(
        &self,
        tasks: Vec<crate::table::SearchTask>,
    ) -> crate::Result<Vec<crate::table::SearchTaskResult>> {
        self.batch_sizes.lock().unwrap().push(tasks.len());
        let mut results = Vec::new();
        let (context, frames) = crate::table::SearchTask::serialize_batch(&tasks)?;
        for (task, frame) in tasks.into_iter().zip(frames) {
            let bytes = task.serialize()?;
            self.work
                .lock()
                .unwrap()
                .push(serde_json::from_slice(&bytes).unwrap());
            let mut output: serde_json::Value =
                serde_json::from_slice(&self.table.execute_search_task(&frame, &context).await?)
                    .unwrap();
            if self.corrupt_scores {
                output["payload"]["scores"] = json!([]);
            }
            results.push(crate::table::SearchTaskResult::deserialize(
                &serde_json::to_vec(&output).unwrap(),
            )?);
        }
        // Completion order belongs to the scheduler. Core re-associates the
        // results before deterministic merge and floating-point accumulation.
        results.reverse();
        Ok(results)
    }
}

#[tokio::test]
async fn scheduled_full_text_preserves_filters_global_top_k_raw_corpus_and_snapshot() {
    let table = new_table(false).copy_with_options(HashMap::from([(
        "source.split.target-size".into(),
        "1b".into(),
    )]));
    append(&table, 0, 8).await;
    index(&table, "full-text", "text").await;
    append(&table, 8, 4).await;
    append(&table, 12, 4).await;
    let pb = PredicateBuilder::new(table.schema().fields());
    let mut builder = query(&table);
    builder
        .with_filter(pb.equal("label", Datum::String("keep".into())).unwrap())
        .with_include_row_ids([1, 5, 9, 13].into_iter().collect())
        .with_limit(3);
    let plan = builder.new_scan().unwrap().scan().await.unwrap();
    let snapshot = plan.snapshot_id().unwrap();
    let read = builder.new_read().unwrap();
    // A worker starts from a current table. Its task must restore the captured
    // snapshot and use the exact data splits selected for the raw corpus.
    append(&table, 16, 4).await;
    let expected = read.read(plan.clone()).await.unwrap();
    let executor = WireExecutor {
        table: table.clone(),
        work: Default::default(),
        batch_sizes: Default::default(),
        corrupt_scores: false,
    };
    let actual = read.read_with_executor(plan, &executor).await.unwrap();
    assert_eq!(actual.snapshot_id(), Some(snapshot));
    let actual = actual.into_row_ids().unwrap();
    let expected = expected.into_row_ids().unwrap();
    assert_eq!(actual.row_ids, expected.row_ids);
    assert_eq!(actual.scores, expected.scores);
    assert_eq!(actual.row_ids, vec![1, 5, 9]);
    let work = executor.work.lock().unwrap();
    assert_eq!(
        work.iter()
            .filter(|task| task["kind"] == "fulltext-index")
            .count(),
        2
    );
    let raw = work
        .iter()
        .filter(|task| task["kind"] == "fulltext-rows")
        .collect::<Vec<_>>();
    assert_eq!(
        raw.len(),
        1,
        "raw BM25 relevance requires one shared corpus"
    );
    assert!(raw[0]["payload"]["splits"].as_array().unwrap().len() >= 2);
    assert!(work.iter().all(|task| task["snapshot"]["id"] == snapshot));
}

#[tokio::test]
async fn scheduled_full_text_rejects_malformed_worker_scores() {
    let table = new_table(false);
    append(&table, 0, 4).await;
    index(&table, "full-text", "text").await;
    let builder = query(&table);
    let plan = builder.new_scan().unwrap().scan().await.unwrap();
    let executor = WireExecutor {
        table: table.clone(),
        work: Default::default(),
        batch_sizes: Default::default(),
        corrupt_scores: true,
    };
    let error = builder
        .new_read()
        .unwrap()
        .read_with_executor(plan, &executor)
        .await
        .unwrap_err();
    assert!(
        matches!(error, crate::Error::DataInvalid { ref message, .. }
        if message.contains("invalid row IDs or scores")),
        "{error}"
    );
}

#[tokio::test]
async fn scheduled_full_text_rejects_wrong_reader_field_before_scheduling() {
    let table = new_table(false);
    append(&table, 0, 4).await;
    index(&table, "full-text", "text").await;
    let plan = query(&table).new_scan().unwrap().scan().await.unwrap();
    let mut builder = query(&table);
    builder.with_text_column("label");
    let executor = WireExecutor {
        table: table.clone(),
        work: Default::default(),
        batch_sizes: Default::default(),
        corrupt_scores: false,
    };
    let error = builder
        .new_read()
        .unwrap()
        .read_with_executor(plan, &executor)
        .await
        .unwrap_err();
    assert!(
        matches!(error, crate::Error::DataInvalid { ref message, .. }
        if message.contains("reader's field and index type")),
        "{error}"
    );
    assert!(executor.work.lock().unwrap().is_empty());
}

#[tokio::test]
async fn scheduled_full_text_keeps_raw_partition_pruning() {
    let table = new_table(true);
    append(&table, 0, 4).await;
    index(&table, "full-text", "text").await;
    append(&table, 4, 4).await; // An uncovered partition outside the query.
    append_labels(&table, 8, &[Some("keep"); 4]).await; // More rows in pt=0.
    let pb = PredicateBuilder::new(table.schema().fields());
    let mut builder = query(&table);
    builder
        .with_partition_filter(pb.equal("pt", Datum::Int(0)).unwrap())
        .unwrap();
    let plan = builder.new_scan().unwrap().scan().await.unwrap();
    let read = builder.new_read().unwrap();
    let expected = read
        .read(plan.clone())
        .await
        .unwrap()
        .into_row_ids()
        .unwrap();
    let executor = WireExecutor {
        table: table.clone(),
        work: Default::default(),
        batch_sizes: Default::default(),
        corrupt_scores: false,
    };
    let actual = read
        .read_with_executor(plan, &executor)
        .await
        .unwrap()
        .into_row_ids()
        .unwrap();
    assert_eq!(actual.row_ids, expected.row_ids);
    assert_eq!(actual.scores, expected.scores);
    let mut ids = actual.row_ids;
    ids.sort_unstable();
    assert_eq!(ids, vec![0, 1, 2, 8, 9, 10, 11]);
}

#[tokio::test]
async fn scheduled_full_text_bounds_index_task_windows_and_keeps_all_candidates() {
    use crate::table::search_task::SEARCH_TASK_WINDOW;

    let table = new_table(false).copy_with_options(HashMap::from([
        ("global-index.row-count-per-shard".into(), "1".into()),
        ("full-text-index.search-mode".into(), "fast".into()),
    ]));
    // Include the null-only shard: planning still schedules its empty index.
    // Two full windows and one last shard must retain every known matching row.
    let row_count = SEARCH_TASK_WINDOW * 2 + 1;
    append(&table, 0, row_count as i32).await;
    index(&table, "full-text", "text").await;
    let mut builder = query(&table);
    builder.with_limit(row_count);
    let plan = builder.new_scan().unwrap().scan().await.unwrap();
    let files = plan
        .splits()
        .iter()
        .flat_map(|split| match split {
            crate::table::SearchScanSplit::Index { index_files, .. } => index_files.clone(),
            _ => vec![],
        })
        .map(|file| file.file_name)
        .collect::<Vec<_>>();
    let read = builder.new_read().unwrap();
    let expected = read
        .read(plan.clone())
        .await
        .unwrap()
        .into_row_ids()
        .unwrap();
    let executor = WireExecutor {
        table: table.clone(),
        work: Default::default(),
        batch_sizes: Default::default(),
        corrupt_scores: false,
    };
    let actual = read
        .read_with_executor(plan, &executor)
        .await
        .unwrap()
        .into_row_ids()
        .unwrap();
    assert_eq!(actual.row_ids, expected.row_ids);
    assert_eq!(actual.scores, expected.scores);
    let mut ids = actual.row_ids;
    ids.sort_unstable();
    assert_eq!(
        ids,
        (0..row_count as u64)
            .filter(|id| *id != 3)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        *executor.batch_sizes.lock().unwrap(),
        vec![SEARCH_TASK_WINDOW, SEARCH_TASK_WINDOW, 1]
    );
    let work = executor.work.lock().unwrap();
    let actual_files = work
        .iter()
        .map(|task| {
            serde_json::from_value::<IndexManifestEntry>(task["payload"]["entry"].clone())
                .unwrap()
                .index_file
                .file_name
        })
        .collect::<Vec<_>>();
    assert_eq!(actual_files, files, "windows must retain manifest order");
}

#[tokio::test]
async fn data_plans_execute_only_the_supplied_sources_after_roundtrip() {
    let table = new_table(false);
    append(&table, 0, 8).await;
    index(&table, "full-text", "text").await;
    append(&table, 8, 4).await;
    let builder = query(&table);
    let scanned = builder.new_scan().unwrap().scan().await.unwrap();
    let snapshot = scanned.snapshot().cloned();
    let index_source = scanned.splits().iter().find(|split| {
        matches!(split, crate::table::SearchScanSplit::Index { row_range, .. } if row_range.from() == 0)
    }).unwrap().clone();
    let raw_source = scanned
        .splits()
        .iter()
        .find(|split| matches!(split, crate::table::SearchScanSplit::Raw { .. }))
        .unwrap()
        .clone();
    let index_only = FullTextScanPlan::new(snapshot.clone(), vec![index_source]).unwrap();
    let raw_only = FullTextScanPlan::new(snapshot.clone(), vec![raw_source]).unwrap();
    let empty = FullTextScanPlan::new(snapshot, vec![]).unwrap();
    append(&table, 12, 4).await;
    for (plan, expected) in [
        (index_only, vec![0, 1, 2]),
        (raw_only, vec![8, 9, 10, 11]),
        (empty, vec![]),
    ] {
        let encoded = serde_json::to_vec(&plan).unwrap();
        let plan: FullTextScanPlan = serde_json::from_slice(&encoded).unwrap();
        let result = builder.new_read().unwrap().read(plan).await.unwrap();
        assert_eq!(result.snapshot_id(), scanned.snapshot_id());
        assert_eq!(ids(result), expected);
    }
}

#[tokio::test]
async fn data_plan_without_snapshot_never_resolves_latest() {
    let table = new_table(false);
    append(&table, 0, 4).await;
    index(&table, "full-text", "text").await;
    let builder = query(&table);
    let source = builder.new_scan().unwrap().scan().await.unwrap();
    assert!(FullTextScanPlan::new(None, source.splits().to_vec()).is_err());
    let malformed = serde_json::json!({ "snapshot": null, "splits": source.splits() });
    assert!(serde_json::from_value::<FullTextScanPlan>(malformed).is_err());
    let plan = FullTextScanPlan::new(None, vec![]).unwrap();
    let result = builder.new_read().unwrap().read(plan).await.unwrap();
    assert_eq!(result.snapshot_id(), None);
    assert!(result.is_empty());
}

#[tokio::test]
async fn data_plan_applies_reader_partition_filter_to_supplied_sources() {
    let table = new_table(true);
    append(&table, 0, 8).await;
    index(&table, "full-text", "text").await;
    append(&table, 8, 4).await;
    let source = query(&table).new_scan().unwrap().scan().await.unwrap();
    let plan = FullTextScanPlan::new(source.snapshot().cloned(), source.splits().to_vec()).unwrap();
    let pb = PredicateBuilder::new(table.schema().fields());
    let mut builder = query(&table);
    builder
        .with_partition_filter(pb.equal("pt", Datum::Int(1)).unwrap())
        .unwrap();
    assert_eq!(
        user_ids(
            &table,
            builder.new_read().unwrap().read(plan).await.unwrap()
        )
        .await,
        vec![4, 5, 6, 7]
    );
}

#[tokio::test]
async fn data_plan_does_not_reselect_search_sources_from_snapshot_manifest() {
    let table = new_table(false).copy_with_options(HashMap::from([(
        "deletion-vectors.enabled".into(),
        "false".into(),
    )]));
    append(&table, 0, 8).await;
    index(&table, "full-text", "text").await;
    let builder = query(&table);
    let scanned = builder.new_scan().unwrap().scan().await.unwrap();
    let mut snapshot = serde_json::to_value(scanned.snapshot().unwrap()).unwrap();
    snapshot["indexManifest"] = serde_json::json!("missing-search-source-manifest");
    let snapshot = serde_json::from_value(snapshot).unwrap();
    let source = scanned
        .splits()
        .iter()
        .find(|split| {
            matches!(
                split,
                crate::table::SearchScanSplit::Index { row_range, .. } if row_range.from() == 0
            )
        })
        .unwrap()
        .clone();
    let plan = FullTextScanPlan::new(Some(snapshot), vec![source]).unwrap();
    assert_eq!(
        ids(builder.new_read().unwrap().read(plan).await.unwrap()),
        vec![0, 1, 2],
    );
}

#[tokio::test]
async fn data_plan_retains_scalar_metadata_for_a_later_reader_filter() {
    let table = new_table(false).copy_with_options(HashMap::from([(
        "scalar-index.search-mode".into(),
        "fast".into(),
    )]));
    append(&table, 0, 8).await;
    index(&table, "full-text", "text").await;
    index(&table, "bitmap", "label").await;
    let plan = query(&table).new_scan().unwrap().scan().await.unwrap();
    let pb = PredicateBuilder::new(table.schema().fields());
    let mut builder = query(&table);
    builder.with_filter(pb.equal("label", Datum::String("keep".into())).unwrap());
    assert_eq!(
        ids(builder.new_read().unwrap().read(plan).await.unwrap()),
        vec![1, 5, 7],
    );
}

#[tokio::test]
async fn index_only_data_plan_applies_reader_partitions_across_commits() {
    let table = new_table(true);
    append(&table, 0, 4).await;
    append(&table, 4, 4).await;
    index(&table, "full-text", "text").await;
    let captured = query(&table).new_scan().unwrap().scan().await.unwrap();
    let plan = FullTextScanPlan::new(
        captured.snapshot().cloned(),
        captured
            .splits()
            .iter()
            .filter(|split| matches!(split, crate::table::SearchScanSplit::Index { .. }))
            .cloned()
            .collect(),
    )
    .unwrap();
    let pb = PredicateBuilder::new(table.schema().fields());
    for (partition, expected) in [(0, vec![0, 1, 2]), (1, vec![4, 5, 6, 7])] {
        let mut reader = query(&table);
        reader
            .with_partition_filter(pb.equal("pt", Datum::Int(partition)).unwrap())
            .unwrap();
        let result = reader.new_read().unwrap().read(plan.clone()).await.unwrap();
        assert_eq!(user_ids(&table, result).await, expected);
    }
}
