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

//! Non-data-evolution append search uses real Parquet data and global indexes.

use super::*;
use crate::deletion_vector::DeletionVector;
use crate::spec::{DeletionVectorMeta, IndexFileMeta};
use crate::table::CommitMessage;
use roaring::RoaringBitmap;

fn append_table(partitioned: bool) -> Table {
    new_table(partitioned).copy_with_options(HashMap::from([
        ("data-evolution.enabled".into(), "false".into()),
        ("file.format".into(), "parquet".into()),
        ("full-text-index.search-mode".into(), "fast".into()),
        ("scalar-index.search-mode".into(), "fast".into()),
    ]))
}

fn keep_filter(table: &Table) -> Predicate {
    PredicateBuilder::new(table.schema().fields())
        .equal("label", Datum::String("keep".into()))
        .unwrap()
}

#[tokio::test]
async fn scalar_modes_preserve_missing_and_partial_coverage() {
    let table = append_table(false);
    append(&table, 0, 4).await;
    index(&table, "full-text", "text").await;
    let filter = keep_filter(&table);
    for (mode, expected) in [("fast", vec![]), ("full", vec![1]), ("detail", vec![1])] {
        let table = table.copy_with_options(HashMap::from([(
            "scalar-index.search-mode".into(),
            mode.into(),
        )]));
        assert_eq!(
            ids(query(&table)
                .with_filter(filter.clone())
                .execute()
                .await
                .unwrap()),
            expected,
            "missing scalar coverage, mode={mode}",
        );
    }

    index(&table, "btree", "label").await;
    append(&table, 4, 4).await;
    index(&table, "full-text", "text").await;
    append(&table, 8, 4).await;
    for (mode, expected) in [
        ("fast", vec![1]),
        ("full", vec![1, 5, 7]),
        ("detail", vec![1, 5, 7]),
    ] {
        let table = table.copy_with_options(HashMap::from([(
            "scalar-index.search-mode".into(),
            mode.into(),
        )]));
        assert_eq!(
            ids(query(&table)
                .with_filter(filter.clone())
                .execute()
                .await
                .unwrap()),
            expected,
            "partial scalar coverage, mode={mode}",
        );
    }
}

#[tokio::test]
async fn candidate_refinement_keeps_rust_defaults_and_original_scores() {
    let table = append_table(false);
    append_labels(
        &table,
        0,
        &[
            None,
            Some("drop"),
            Some("keep"),
            Some("drop"),
            Some("keep"),
            Some("keeper"),
            Some("other"),
            Some("keep"),
        ],
    )
    .await;
    index(&table, "full-text", "text").await;
    index(&table, "btree", "label").await;
    assert!(table
        .schema()
        .core_options()
        .global_index_filter_refine_from_data());
    let pb = PredicateBuilder::new(table.schema().fields());
    let filter = Predicate::and(vec![
        pb.contains("label", Datum::String("eep".into())).unwrap(),
        pb.greater_or_equal("id", Datum::Int(5)).unwrap(),
    ]);
    let all = query(&table)
        .execute()
        .await
        .unwrap()
        .into_row_ids()
        .unwrap();
    let filtered = query(&table)
        .with_filter(filter.clone())
        .with_limit(2)
        .execute()
        .await
        .unwrap();
    assert_eq!(ids(filtered.clone()), vec![5, 7]);
    let filtered = filtered.into_row_ids().unwrap();
    for (id, score) in filtered.row_ids.iter().zip(&filtered.scores) {
        let position = all.row_ids.iter().position(|row| row == id).unwrap();
        assert_eq!(*score, all.scores[position]);
    }

    let no_refine = table.copy_with_options(HashMap::from([(
        "global-index.filter.refine-from-data".into(),
        "false".into(),
    )]));
    assert!(query(&no_refine)
        .with_filter(filter)
        .execute()
        .await
        .unwrap()
        .is_empty());
    assert!(query(&table)
        .with_filter(pb.equal("label", Datum::String("missing".into())).unwrap())
        .execute()
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn partition_scalar_and_include_filters_precede_top_k() {
    for kind in ["btree", "bitmap"] {
        let table = append_table(true);
        append(&table, 0, 8).await;
        index(&table, "full-text", "text").await;
        index(&table, kind, "label").await;
        append(&table, 8, 4).await;
        let pb = PredicateBuilder::new(table.schema().fields());
        let filter = Predicate::and(vec![
            pb.greater_or_equal("pt", Datum::Int(1)).unwrap(),
            keep_filter(&table),
        ]);
        let mut builder = query(&table);
        builder.with_filter(filter).with_limit(2);
        let result = builder.execute().await.unwrap();
        assert_eq!(user_ids(&table, result.clone()).await, vec![5, 7]);
        // Partition commit order assigns global IDs independently of user IDs.
        let last = *result.row_ids().unwrap().row_ids.iter().max().unwrap();
        builder.with_include_row_ids([last, 9].into_iter().collect());
        assert_eq!(
            user_ids(&table, builder.execute().await.unwrap()).await,
            vec![7],
        );
        builder.with_include_row_ids(RoaringTreemap::new());
        assert!(builder.execute().await.unwrap().is_empty());
    }
}

async fn delete_row(table: &Table, row_id: i64) {
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let (split, file) = plan
        .splits()
        .iter()
        .flat_map(|split| split.data_files().iter().map(move |file| (split, file)))
        .find(|(_, file)| {
            let first = file.first_row_id.unwrap();
            first <= row_id && row_id < first + file.row_count
        })
        .unwrap();
    let position = u32::try_from(row_id - file.first_row_id.unwrap()).unwrap();
    let dv = DeletionVector::from_bitmap(RoaringBitmap::from_iter([position]));
    let serialized = dv.serialize_to_bytes().unwrap();
    let length = i32::from_be_bytes(serialized[..4].try_into().unwrap());
    let mut bytes = vec![1];
    bytes.extend_from_slice(&serialized);
    let file_size = bytes.len() as i64;
    let file_name = format!("index-{}-1", uuid::Uuid::new_v4());
    table
        .file_io()
        .new_output(&format!("{}/index/{file_name}", table.location()))
        .unwrap()
        .write(bytes::Bytes::from(bytes))
        .await
        .unwrap();
    let mut message = CommitMessage::new(
        split.partition().to_serialized_bytes(),
        split.bucket(),
        vec![],
    );
    message.new_index_files.push(IndexFileMeta {
        index_type: "DELETION_VECTORS".into(),
        file_name,
        file_size,
        row_count: 1,
        deletion_vectors_ranges: Some(indexmap::IndexMap::from([(
            file.file_name.clone(),
            DeletionVectorMeta {
                offset: 1,
                length,
                cardinality: Some(1),
            },
        )])),
        external_path: None,
        global_index_meta: None,
    });
    TableCommit::new(table.clone(), "delete".into())
        .commit(vec![message])
        .await
        .unwrap();
}

#[tokio::test]
async fn deletion_vectors_filter_exact_index_hits_before_top_k_and_pin_snapshot() {
    let table = append_table(false);
    append(&table, 0, 4).await;
    append(&table, 4, 4).await;
    index(&table, "full-text", "text").await;
    index(&table, "btree", "label").await;
    let table = table.copy_with_options(HashMap::from([(
        "deletion-vectors.enabled".into(),
        "true".into(),
    )]));
    let mut builder = query(&table);
    builder.with_filter(keep_filter(&table)).with_limit(2);
    let read = builder.new_read().unwrap();
    let plan = builder.new_scan().unwrap().scan().await.unwrap();
    delete_row(&table, 5).await;

    assert_eq!(ids(read.read(plan).await.unwrap()), vec![1, 5]);
    let result = builder.execute().await.unwrap();
    assert_eq!(ids(result.clone()), vec![1, 7]);
    let batches: Vec<RecordBatch> = result
        .new_read_builder()
        .with_projection(&["id"])
        .read()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let values = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, vec![1, 7]);
}

#[tokio::test]
async fn removed_files_do_not_supply_exact_index_hits() {
    let table = append_table(false);
    append(&table, 0, 4).await;
    append(&table, 4, 4).await;
    index(&table, "full-text", "text").await;
    index(&table, "btree", "label").await;
    let mut builder = query(&table);
    builder.with_filter(keep_filter(&table)).with_limit(1);
    let read = builder.new_read().unwrap();
    let plan = builder.new_scan().unwrap().scan().await.unwrap();
    let data = table.new_read_builder().new_scan().plan().await.unwrap();
    let split = data
        .splits()
        .iter()
        .find(|split| {
            split
                .data_files()
                .iter()
                .any(|file| file.first_row_id == Some(0))
        })
        .unwrap();
    let mut message = CommitMessage::new(
        split.partition().to_serialized_bytes(),
        split.bucket(),
        vec![],
    );
    message.deleted_files = split
        .data_files()
        .iter()
        .filter(|file| file.first_row_id == Some(0))
        .cloned()
        .collect();
    TableCommit::new(table.clone(), "remove".into())
        .commit(vec![message])
        .await
        .unwrap();
    assert_eq!(ids(read.read(plan).await.unwrap()), vec![1]);
    assert_eq!(ids(builder.execute().await.unwrap()), vec![5]);
}

#[tokio::test]
async fn empty_filtered_plans_stay_empty_after_index_build() {
    let table = append_table(false);
    let mut builder = query(&table);
    builder.with_filter(keep_filter(&table));
    let read = builder.new_read().unwrap();
    let plan = builder.new_scan().unwrap().scan().await.unwrap();
    assert_eq!(plan.snapshot_id(), None);
    append(&table, 0, 4).await;
    index(&table, "full-text", "text").await;
    index(&table, "btree", "label").await;
    assert!(read.read(plan).await.unwrap().is_empty());
    assert_eq!(ids(builder.execute().await.unwrap()), vec![1]);
}

#[tokio::test]
async fn unsupported_append_filter_combinations_fail_before_empty_snapshot() {
    let table = append_table(false);
    for (option, value, expected) in [
        (
            "row-tracking.enabled",
            "false",
            "row-tracked Parquet append",
        ),
        ("file.format", "orc", "row-tracked Parquet append"),
        ("file.format", "avro", "row-tracked Parquet append"),
        ("full-text-index.search-mode", "full", "FAST"),
        ("full-text-index.search-mode", "detail", "FAST"),
        ("query-auth.enabled", "true", "query"),
    ] {
        let table = table.copy_with_options(HashMap::from([(option.into(), value.into())]));
        let error = query(&table)
            .with_filter(keep_filter(&table))
            .execute()
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains(expected),
            "{option}={value}: {error}"
        );
    }
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("text", DataType::VarChar(VarCharType::string_type()))
        .column("label", DataType::VarChar(VarCharType::string_type()))
        .primary_key(["id"])
        .build()
        .unwrap();
    let pk = Table::new(
        table.file_io().clone(),
        table.identifier().clone(),
        table.location().into(),
        TableSchema::new(0, &schema),
        None,
    );
    assert!(query(&pk)
        .with_filter(keep_filter(&pk))
        .execute()
        .await
        .unwrap_err()
        .to_string()
        .contains("row-tracked Parquet append"));
}
