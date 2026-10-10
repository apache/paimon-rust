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

fn query(table: &Table) -> FullTextSearchBuilder<'_> {
    let mut builder = table.new_full_text_search_builder();
    builder
        .with_query("text", r#"{"match":{"query":"paimon"}}"#)
        .with_limit(64);
    builder
}

fn ids(result: SearchResult) -> Vec<u64> {
    let mut ids = result.row_ids;
    ids.sort_unstable();
    ids
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

async fn user_ids(table: &Table, result: SearchResult) -> Vec<i32> {
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
async fn scan_read_pin_snapshot_and_validate_plan_origin() {
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
    assert!(other
        .new_read()
        .unwrap()
        .read(plan.clone())
        .await
        .unwrap_err()
        .to_string()
        .contains("different table, column or filter"));
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

    let directory = tempfile::TempDir::new().unwrap();
    let catalog = FileSystemCatalog::new(Options::from_map(HashMap::from([(
        "warehouse".into(),
        directory.path().to_str().unwrap().into(),
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
        assert_eq!(plan.table.schema().id(), table.schema().id());
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
