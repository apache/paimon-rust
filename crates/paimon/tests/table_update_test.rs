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

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use paimon::spec::{DataType, IntType, Schema, TableSchema};
use paimon::table::{CommitMessage, Table};

use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs, write_batch};

async fn evolution_table() -> Table {
    let path = "memory:/table_update";
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .column("score", DataType::Int(IntType::new()))
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        .option("deletion-vectors.enabled", "true")
        .build()
        .unwrap();
    let (io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    table
}

async fn inline_blob_table(option: &str) -> Table {
    let path = "memory:/inline_blob_update";
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("payload", DataType::Blob(paimon::spec::BlobType::new()))
        .column("value", DataType::Int(IntType::new()))
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        .option("deletion-vectors.enabled", "true")
        .option(option, "payload")
        .build()
        .unwrap();
    let (io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    table
}

fn inline_reference(option: &str, id: i64) -> Vec<u8> {
    if option == "blob-view-field" {
        paimon::spec::BlobViewStruct::new(
            paimon::catalog::Identifier::new("missing", "upstream"),
            5,
            id,
        )
        .serialize()
        .unwrap()
    } else {
        // These files do not exist: staging a partial update must preserve
        // references without resolving or copying external payloads.
        paimon::spec::BlobDescriptor::new(format!("memory:/missing/{id}"), 0, 10).serialize()
    }
}

fn inline_batch(table: &Table, ids: Vec<i32>, values: &[Option<Vec<u8>>]) -> RecordBatch {
    RecordBatch::try_new(
        paimon::arrow::build_target_arrow_schema(table.schema().fields()).unwrap(),
        vec![
            Arc::new(Int32Array::from(ids.clone())),
            Arc::new(arrow_array::LargeBinaryArray::from_iter(
                values.iter().map(|value| value.as_deref()),
            )),
            Arc::new(Int32Array::from(
                ids.iter().map(|id| id * 10).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

async fn inline_rows(table: &Table) -> Vec<(i32, Option<Vec<u8>>, i32)> {
    use arrow_array::Array;
    let read_table = table.copy_with_options(HashMap::from([
        ("blob-as-descriptor".into(), "true".into()),
        ("blob-view.resolve.enabled".into(), "false".into()),
    ]));
    let builder = read_table.new_read_builder();
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
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let payloads = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::LargeBinaryArray>()
            .unwrap();
        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                ids.value(row),
                payloads.is_valid(row).then(|| payloads.value(row).to_vec()),
                values.value(row),
            ));
        }
    }
    rows.sort_by_key(|row| row.0);
    rows
}

#[tokio::test]
async fn inline_blob_row_id_updates_keep_unmatched_references_and_nulls() {
    for option in [
        "blob-descriptor-field",
        "blob.stored-descriptor-fields",
        "blob-view-field",
    ] {
        for incremental in [false, true] {
            let table = inline_blob_table(option).await;
            let before = vec![
                Some(inline_reference(option, 0)),
                None,
                Some(inline_reference(option, 2)),
                Some(inline_reference(option, 3)),
            ];
            write_batch(&table, &inline_batch(&table, vec![0, 1, 2, 3], &before)).await;
            let changed = Some(inline_reference(option, 20));
            let input = RecordBatch::try_from_iter(vec![
                (
                    "_ROW_ID",
                    Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                ),
                (
                    "payload",
                    Arc::new(arrow_array::LargeBinaryArray::from_iter([
                        None,
                        changed.as_deref(),
                    ])) as ArrayRef,
                ),
            ])
            .unwrap();
            let update = table.new_write_builder().new_update().unwrap();
            let messages = if incremental {
                let mut writer = update.new_update_by_row_id().await.unwrap();
                writer
                    .update_columns(vec![input], vec!["payload".into()])
                    .await
                    .unwrap()
            } else {
                update
                    .update_by_arrow_with_row_id(vec![input])
                    .await
                    .unwrap()
            };
            assert!(!messages.is_empty());
            for file in messages.iter().flat_map(|message| &message.new_files) {
                assert!(file.file_name.ends_with(".parquet"));
                assert_eq!(file.write_cols, Some(vec!["payload".into()]));
                assert_eq!(file.first_row_id, Some(0));
                assert_eq!(file.row_count, 4);
            }
            commit(&table, messages).await;
            assert_eq!(
                inline_rows(&table).await,
                vec![
                    (0, before[0].clone(), 0),
                    (1, None, 10),
                    (2, changed, 20),
                    (3, before[3].clone(), 30),
                ]
            );
        }
    }
}

#[tokio::test]
async fn inline_blob_upsert_and_delete_preserve_reference_layout() {
    for option in [
        "blob-descriptor-field",
        "blob.stored-descriptor-fields",
        "blob-view-field",
    ] {
        let table = inline_blob_table(option).await;
        let old = inline_reference(option, 0);
        write_batch(
            &table,
            &inline_batch(&table, vec![0, 1], &[Some(old.clone()), None]),
        )
        .await;
        let new = inline_reference(option, 20);
        let source = inline_batch(&table, vec![0, 2], &[Some(new.clone()), None]);
        let update = table.new_write_builder().new_update().unwrap();
        let messages = update
            .upsert_by_arrow_with_key(vec![source], vec!["id".into()])
            .await
            .unwrap();
        assert!(messages
            .iter()
            .flat_map(|message| &message.new_files)
            .all(|file| file.file_name.ends_with(".parquet")));
        commit(&table, messages).await;
        assert_eq!(
            inline_rows(&table).await,
            vec![(0, Some(new), 0), (1, None, 10), (2, None, 20)]
        );
        let update = table.new_write_builder().new_update().unwrap();
        let messages = update.delete_by_row_id(vec![0, 0]).await.unwrap();
        commit(&table, messages).await;
        assert_eq!(
            inline_rows(&table).await,
            vec![(1, None, 10), (2, None, 20)]
        );
    }
}

#[tokio::test]
async fn inline_blob_failed_update_does_not_stage_files() {
    let table = inline_blob_table("blob-view-field").await;
    write_batch(&table, &inline_batch(&table, vec![0, 1], &[None, None])).await;
    let before = parquet_files(&table).await;
    let update = table.new_write_builder().new_update().unwrap();
    let input = RecordBatch::try_from_iter(vec![
        (
            "_ROW_ID",
            Arc::new(Int64Array::from(vec![0, 0])) as ArrayRef,
        ),
        (
            "payload",
            Arc::new(arrow_array::LargeBinaryArray::from(vec![
                Some(b"a".as_slice()),
                Some(b"b".as_slice()),
            ])) as ArrayRef,
        ),
    ])
    .unwrap();
    let error = update
        .update_by_arrow_with_row_id(vec![input])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("duplicate UPDATE operations"));
    assert_eq!(parquet_files(&table).await, before);
    assert_eq!(inline_rows(&table).await, vec![(0, None, 0), (1, None, 10)]);
}

fn batch(columns: &[(&str, Vec<i32>)]) -> RecordBatch {
    RecordBatch::try_from_iter(columns.iter().map(|(name, values)| {
        (
            *name,
            Arc::new(Int32Array::from(values.clone())) as ArrayRef,
        )
    }))
    .unwrap()
}

#[tokio::test]
async fn inline_blob_alias_rejects_malformed_writes_and_updates() {
    let table = inline_blob_table("blob.stored-descriptor-fields").await;
    let invalid = b"not a descriptor".to_vec();
    let mut write = table.new_write_builder().new_write().unwrap();
    let error = write
        .write_arrow_batch(&inline_batch(&table, vec![0], &[Some(invalid.clone())]))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("serialized BlobDescriptor"),
        "{error}"
    );
    assert!(parquet_files(&table).await.is_empty());
    write_batch(&table, &inline_batch(&table, vec![0], &[None])).await;
    let before = parquet_files(&table).await;
    let input = RecordBatch::try_from_iter(vec![
        ("_ROW_ID", Arc::new(Int64Array::from(vec![0])) as ArrayRef),
        (
            "payload",
            Arc::new(arrow_array::LargeBinaryArray::from(vec![Some(
                invalid.as_slice(),
            )])) as ArrayRef,
        ),
    ])
    .unwrap();
    let error = table
        .new_write_builder()
        .new_update()
        .unwrap()
        .update_by_arrow_with_row_id(vec![input])
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("serialized BlobDescriptor"),
        "{error}"
    );
    assert_eq!(parquet_files(&table).await, before);
    assert_eq!(inline_rows(&table).await, vec![(0, None, 0)]);
}

#[tokio::test]
async fn normal_column_update_metadata_matches_java_blob_layout() {
    for option in [
        "blob-field",
        "blob-descriptor-field",
        "blob.stored-descriptor-fields",
        "blob-view-field",
    ] {
        for optimize in [false, true] {
            let table = inline_blob_table(option)
                .await
                .copy_with_options(HashMap::from([
                    (
                        "data-evolution.write-cols-optimization.enabled".into(),
                        optimize.to_string(),
                    ),
                    ("metadata.stats-mode".into(), "full".into()),
                ]));
            persist_table_schema(table.file_io(), table.location(), table.schema()).await;
            let reference = if option == "blob-field" {
                b"raw payload".to_vec()
            } else {
                inline_reference(option, 0)
            };
            write_batch(&table, &inline_batch(&table, vec![0], &[Some(reference)])).await;
            let original = inline_rows(&table).await[0].1.clone();
            let messages = table
                .new_write_builder()
                .new_update()
                .unwrap()
                .update_by_arrow_with_row_id(vec![matched(
                    vec![0],
                    &[("id", vec![1]), ("value", vec![100])],
                )])
                .await
                .unwrap();
            let file = &messages[0].new_files[0];
            let expected = if optimize && option == "blob-field" {
                None
            } else {
                Some(vec!["id".to_string(), "value".to_string()])
            };
            assert_eq!(file.write_cols, expected);
            assert_eq!(file.first_row_id, Some(0));
            commit(&table, messages).await;
            assert_eq!(inline_rows(&table).await, vec![(1, original, 100)]);
        }
    }
}

#[tokio::test]
async fn inline_blob_history_restores_layout_after_drop_and_add() {
    for option in [
        "blob-descriptor-field",
        "blob.stored-descriptor-fields",
        "blob-view-field",
    ] {
        let table = inline_blob_table(option).await;
        let reference = inline_reference(option, 0);
        write_batch(
            &table,
            &inline_batch(&table, vec![0, 1], &[Some(reference.clone()), None]),
        )
        .await;
        let update = table.new_write_builder().new_update().unwrap();
        commit(
            &table,
            update
                .update_by_arrow_with_row_id(vec![matched(vec![0], &[("value", vec![100])])])
                .await
                .unwrap(),
        )
        .await;
        let expected = inline_rows(&table).await;
        let schema = table
            .schema()
            .apply_changes(vec![
                paimon::spec::SchemaChange::drop_column("payload".into()),
                paimon::spec::SchemaChange::AddColumn {
                    field_names: vec!["reference".into()],
                    data_type: DataType::Blob(paimon::spec::BlobType::new()),
                    comment: Some("__BLOB_DESCRIPTOR_FIELD".into()),
                    column_move: None,
                },
            ])
            .unwrap();
        persist_table_schema(table.file_io(), table.location(), &schema).await;
        let current = table.copy_with_resolved_schema(schema, "main").unwrap();
        let historical = current
            .copy_with_time_travel_strict(HashMap::from([
                ("scan.snapshot-id".into(), "2".into()),
                ("read.batch-size".into(), "1".into()),
            ]))
            .await
            .unwrap();
        assert_eq!(
            historical.schema().options().get(option).unwrap(),
            "payload"
        );
        assert_eq!(inline_rows(&historical).await, expected);
        if option == "blob.stored-descriptor-fields" {
            let snapshot = table
                .snapshot_manager()
                .get_latest_snapshot()
                .await
                .unwrap()
                .unwrap();
            table
                .tag_manager()
                .create("before_drop", &snapshot)
                .await
                .unwrap();
            let branches =
                paimon::table::BranchManager::new(table.file_io().clone(), table.location().into());
            branches
                .create_branch_from_tag("dev", "before_drop")
                .await
                .unwrap();
            persist_table_schema(
                table.file_io(),
                &branches.branch_path("dev"),
                current.schema(),
            )
            .await;
            let overridden = table
                .copy_with_options(HashMap::from([("blob-descriptor-field".into(), "".into())]));
            let branch = overridden.copy_with_branch("dev").await.unwrap();
            let history = branch
                .copy_with_time_travel_strict(HashMap::from([(
                    "scan.snapshot-id".into(),
                    "2".into(),
                )]))
                .await
                .unwrap();
            assert_eq!(inline_rows(&history).await, expected);
            let resolved = overridden
                .copy_with_resolved_schema(current.schema().clone(), "main")
                .unwrap();
            let history = resolved
                .copy_with_time_travel_strict(HashMap::from([(
                    "scan.snapshot-id".into(),
                    "2".into(),
                )]))
                .await
                .unwrap();
            assert_eq!(inline_rows(&history).await, expected);
        }
        // An explicit alias override keeps the canonical/fallback group from
        // being restored, including across successive copies.
        let overridden = current
            .copy_with_options(HashMap::from([(
                "blob.stored-descriptor-fields".into(),
                "".into(),
            )]))
            .copy_with_time_travel_strict(HashMap::from([("scan.snapshot-id".into(), "2".into())]))
            .await
            .unwrap();
        assert_eq!(
            overridden
                .schema()
                .options()
                .get("blob-descriptor-field")
                .unwrap(),
            "reference"
        );
        assert_eq!(
            overridden
                .schema()
                .options()
                .get("blob.stored-descriptor-fields")
                .unwrap(),
            ""
        );
    }
}

fn matched(row_ids: Vec<i64>, columns: &[(&str, Vec<i32>)]) -> RecordBatch {
    let input = batch(columns);
    let mut named = vec![("_ROW_ID", Arc::new(Int64Array::from(row_ids)) as ArrayRef)];
    let schema = input.schema();
    named.extend(
        schema
            .fields()
            .iter()
            .zip(input.columns())
            .map(|(field, array)| (field.name().as_str(), array.clone())),
    );
    RecordBatch::try_from_iter(named).unwrap()
}

async fn seed(table: &Table) {
    write_batch(
        table,
        &batch(&[
            ("id", vec![1, 2, 3]),
            ("value", vec![10, 20, 30]),
            ("score", vec![100, 200, 300]),
        ]),
    )
    .await;
}

async fn commit(table: &Table, messages: Vec<CommitMessage>) {
    table
        .new_write_builder()
        .new_commit()
        .commit(messages)
        .await
        .unwrap();
}

async fn read_rows(table: &Table) -> Vec<Vec<i32>> {
    let builder = table.new_read_builder();
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
        let arrays: Vec<_> = batch
            .columns()
            .iter()
            .map(|array| array.as_any().downcast_ref::<Int32Array>().unwrap())
            .collect();
        for row in 0..batch.num_rows() {
            rows.push(arrays.iter().map(|array| array.value(row)).collect());
        }
    }
    rows.sort();
    rows
}

async fn parquet_files(table: &Table) -> Vec<String> {
    let mut paths: Vec<_> = table
        .file_io()
        .list_status_recursive(table.location())
        .await
        .unwrap()
        .into_iter()
        .map(|status| status.path)
        .filter(|path| path.ends_with(".parquet"))
        .collect();
    paths.sort();
    paths
}

#[tokio::test]
async fn row_id_update_infers_columns_and_reuses_configuration() {
    let table = evolution_table().await;
    seed(&table).await;
    let mut update = table.new_write_builder().new_update().unwrap();
    let messages = update
        .update_by_arrow_with_row_id(vec![
            matched(vec![0], &[("value", vec![11])]),
            matched(vec![1], &[("value", vec![22])]),
        ])
        .await
        .unwrap();
    commit(&table, messages).await;
    assert_eq!(
        read_rows(&table).await,
        vec![vec![1, 11, 100], vec![2, 22, 200], vec![3, 30, 300]]
    );

    update
        .with_update_type(vec!["score".into(), "score".into()])
        .unwrap();
    assert!(update
        .with_update_type(vec!["value".into(), "missing".into()])
        .is_err());
    // Invalid configuration must leave the previous selection intact.
    let messages = update
        .update_by_arrow_with_row_id(vec![matched(
            vec![0],
            &[("value", vec![999]), ("score", vec![101])],
        )])
        .await
        .unwrap();
    commit(&table, messages).await;

    // Selecting the full schema restores per-input inference, like Python.
    update
        .with_update_type(vec!["score".into(), "id".into(), "value".into()])
        .unwrap();
    commit(
        &table,
        update
            .update_by_arrow_with_row_id(vec![matched(vec![2], &[("score", vec![303])])])
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        read_rows(&table).await,
        vec![vec![1, 11, 101], vec![2, 22, 200], vec![3, 30, 303]]
    );
}

#[tokio::test]
async fn grouped_updates_allow_disjoint_columns_on_the_same_file() {
    let table = evolution_table().await;
    seed(&table).await;
    let update = table.new_write_builder().new_update().unwrap();
    let messages = update
        .update_by_arrow_batches_with_row_id(
            vec![
                vec![
                    matched(vec![0], &[("value", vec![11])]),
                    matched(vec![1], &[("value", vec![22])]),
                ],
                vec![matched(vec![0, 2], &[("score", vec![101, 303])])],
            ]
            .into_iter()
            .map(Ok),
        )
        .await
        .unwrap();
    commit(&table, messages).await;
    assert_eq!(
        read_rows(&table).await,
        vec![vec![1, 11, 101], vec![2, 22, 200], vec![3, 30, 303]]
    );
}

#[tokio::test]
async fn overlapping_groups_preserve_previously_prepared_columns() {
    let table = evolution_table().await;
    seed(&table).await;
    let before = parquet_files(&table).await;
    let update = table.new_write_builder().new_update().unwrap();
    // The score writer succeeds before the value writer encounters different
    // logical inputs touching the same file, even though their row IDs differ.
    let error = update
        .update_by_arrow_batches_with_row_id(
            vec![
                vec![matched(
                    vec![0],
                    &[("score", vec![101]), ("value", vec![11])],
                )],
                vec![matched(vec![2], &[("value", vec![33])])],
            ]
            .into_iter()
            .map(Ok),
        )
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("overlapping first_row_ids by column"),
        "{error}"
    );
    let after = parquet_files(&table).await;
    assert!(after.len() > before.len());
    assert!(before.iter().all(|path| after.contains(path)));
    assert_eq!(
        read_rows(&table).await,
        vec![vec![1, 10, 100], vec![2, 20, 200], vec![3, 30, 300]]
    );
}

#[tokio::test]
async fn row_id_update_rejects_inconsistent_chunks_and_empty_selection() {
    let table = evolution_table().await;
    seed(&table).await;
    let before = parquet_files(&table).await;
    let mut update = table.new_write_builder().new_update().unwrap();
    let error = update
        .update_by_arrow_with_row_id(vec![
            matched(vec![0], &[("value", vec![11])]),
            matched(vec![1], &[("score", vec![202])]),
        ])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("must have the same columns"));
    let error = update
        .update_by_arrow_with_row_id(vec![batch(&[("value", vec![])])])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("_ROW_ID"));
    update.with_update_type(vec![]).unwrap();
    let error = update
        .update_by_arrow_with_row_id(vec![matched(vec![], &[("value", vec![])])])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("column_names cannot be empty"));
    assert!(update
        .update_by_arrow_batches_with_row_id(vec![].into_iter().map(Ok))
        .await
        .unwrap()
        .is_empty());
    assert_eq!(parquet_files(&table).await, before);
}

#[tokio::test]
async fn grouped_updates_on_an_empty_table_preserve_empty_input_semantics() {
    let table = evolution_table().await;
    let update = table.new_write_builder().new_update().unwrap();
    assert!(update
        .update_by_arrow_batches_with_row_id(
            vec![
                vec![matched(vec![], &[("value", vec![])])],
                vec![matched(vec![], &[("score", vec![])])],
            ]
            .into_iter()
            .map(Ok)
        )
        .await
        .unwrap()
        .is_empty());
    let error = update
        .update_by_arrow_batches_with_row_id(
            vec![
                vec![matched(vec![0], &[("value", vec![11])])],
                vec![matched(vec![0], &[("score", vec![101])])],
            ]
            .into_iter()
            .map(Ok),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("No files with row tracking"));
    assert!(parquet_files(&table).await.is_empty());
}

#[tokio::test]
async fn configured_upsert_and_delete_are_core_operations() {
    let table = evolution_table().await;
    write_batch(
        &table,
        &batch(&[
            ("id", vec![1, 1, 2]),
            ("value", vec![10, 11, 20]),
            ("score", vec![100, 101, 200]),
        ]),
    )
    .await;
    let builder = table
        .new_write_builder()
        .with_commit_user("core-update")
        .unwrap();
    let mut update = builder.new_update().unwrap();
    update.with_update_type(vec!["value".into()]).unwrap();
    let messages = update
        .upsert_by_arrow_with_key(
            vec![batch(&[
                ("id", vec![1, 3]),
                ("value", vec![12, 30]),
                ("score", vec![999, 300]),
            ])],
            vec!["id".into()],
        )
        .await
        .unwrap();
    builder.new_commit().commit(messages).await.unwrap();
    assert_eq!(
        read_rows(&table).await,
        vec![
            vec![1, 12, 100],
            vec![1, 12, 101],
            vec![2, 20, 200],
            vec![3, 30, 300]
        ]
    );
    let snapshot = table
        .snapshot_manager()
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.commit_user(), "core-update");

    let messages = update.delete_by_row_id(vec![0, 0, 2]).await.unwrap();
    builder.new_commit().commit(messages).await.unwrap();
    assert_eq!(
        read_rows(&table).await,
        vec![vec![1, 12, 101], vec![3, 30, 300]]
    );
    assert!(update.delete_by_row_id(vec![]).await.unwrap().is_empty());
}

#[tokio::test]
async fn row_id_update_rejects_invalid_casts_without_writing_nulls() {
    let table = evolution_table().await;
    seed(&table).await;
    let before = parquet_files(&table).await;
    let update = table.new_write_builder().new_update().unwrap();
    for values in [
        Arc::new(StringArray::from(vec!["bad"])) as ArrayRef,
        Arc::new(Int64Array::from(vec![i64::from(i32::MAX) + 1])),
    ] {
        let input = RecordBatch::try_from_iter([
            ("_ROW_ID", Arc::new(Int64Array::from(vec![0])) as ArrayRef),
            ("value", values),
        ])
        .unwrap();
        assert!(update
            .update_by_arrow_with_row_id(vec![input])
            .await
            .is_err());
        assert_eq!(parquet_files(&table).await, before);
    }
    let input = RecordBatch::try_from_iter([
        ("_ROW_ID", Arc::new(Int64Array::from(vec![0])) as ArrayRef),
        ("value", Arc::new(Int64Array::from(vec![42])) as ArrayRef),
    ])
    .unwrap();
    commit(
        &table,
        update
            .update_by_arrow_with_row_id(vec![input])
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(
        read_rows(&table).await,
        vec![vec![1, 42, 100], vec![2, 20, 200], vec![3, 30, 300]]
    );
}

#[tokio::test]
async fn empty_update_type_means_all_columns_for_upsert_only() {
    let table = evolution_table().await;
    seed(&table).await;
    let mut update = table.new_write_builder().new_update().unwrap();
    update.with_update_type(vec![]).unwrap();
    let messages = update
        .upsert_by_arrow_with_key(
            vec![batch(&[
                ("id", vec![1]),
                ("value", vec![11]),
                ("score", vec![101]),
            ])],
            vec!["id".into()],
        )
        .await
        .unwrap();
    commit(&table, messages).await;
    assert_eq!(
        read_rows(&table).await,
        vec![vec![1, 11, 101], vec![2, 20, 200], vec![3, 30, 300]]
    );
    assert!(update
        .update_by_arrow_with_row_id(vec![matched(vec![0], &[("value", vec![12])])])
        .await
        .is_err());
}

#[tokio::test]
async fn update_builder_rejects_overwrite_and_time_travel() {
    let table = evolution_table().await;
    assert!(table
        .new_write_builder()
        .with_overwrite()
        .new_update()
        .is_err());
    let historical =
        table.copy_with_options(HashMap::from([("scan.snapshot-id".into(), "1".into())]));
    assert!(historical.new_write_builder().new_update().is_err());
}

#[tokio::test]
async fn row_id_factory_shares_snapshot_and_selects_columns_per_call() {
    let table = evolution_table().await;
    seed(&table).await;
    let mut update = table.new_write_builder().new_update().unwrap();
    update.with_update_type(vec!["id".into()]).unwrap();
    let mut updater = update.new_update_by_row_id().await.unwrap();
    // The factory does not inherit the high-level selected columns. It pins
    // the original file index even if new rows arrive before the first call.
    seed(&table).await;
    assert!(updater
        .update_columns(
            vec![matched(vec![3], &[("value", vec![99])])],
            vec!["value".into()]
        )
        .await
        .is_err());
    let first = updater
        .update_columns(
            vec![matched(vec![0], &[("value", vec![11])])],
            vec!["value".into()],
        )
        .await
        .unwrap();
    let both = updater
        .update_columns(
            vec![matched(vec![1], &[("score", vec![222])])],
            vec!["score".into()],
        )
        .await
        .unwrap();
    assert!(both.len() > first.len());
    assert_eq!(both.len(), updater.commit_messages().len());
    assert!(both
        .iter()
        .all(|message| message.check_from_snapshot == Some(1)));
    assert!(updater
        .update_columns(
            vec![matched(vec![2], &[("value", vec![33])])],
            vec!["value".into()]
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("overlapping first_row_ids"));
    // A validation error leaves earlier messages available to commit.
    commit(&table, updater.commit_messages().to_vec()).await;
    assert_eq!(
        read_rows(&table).await,
        vec![
            vec![1, 10, 100],
            vec![1, 11, 100],
            vec![2, 20, 200],
            vec![2, 20, 222],
            vec![3, 30, 300],
            vec![3, 30, 300],
        ]
    );
}

#[tokio::test]
async fn row_id_abort_preserves_prepared_and_published_updates() {
    for published in [false, true] {
        let table = evolution_table().await;
        seed(&table).await;
        let builder = table.new_write_builder();
        let update = builder.new_update().unwrap();
        let mut updater = update.new_update_by_row_id().await.unwrap();
        let messages = updater
            .update_columns(
                vec![matched(vec![0], &[("value", vec![11])])],
                vec!["value".into()],
            )
            .await
            .unwrap();
        let prepared_files = parquet_files(&table).await;
        let snapshot_id = table
            .snapshot_manager()
            .get_latest_snapshot()
            .await
            .unwrap()
            .unwrap()
            .id();
        if published {
            builder.new_commit().commit(messages.clone()).await.unwrap();
        }
        updater.abort().await.unwrap();
        updater.abort().await.unwrap();
        assert!(updater.commit_messages().is_empty());
        assert_eq!(parquet_files(&table).await, prepared_files);
        if !published {
            assert_eq!(
                table
                    .snapshot_manager()
                    .get_latest_snapshot()
                    .await
                    .unwrap()
                    .unwrap()
                    .id(),
                snapshot_id
            );
            assert_eq!(
                read_rows(&table).await,
                vec![vec![1, 10, 100], vec![2, 20, 200], vec![3, 30, 300]]
            );
            builder.new_commit().commit(messages).await.unwrap();
        }
        assert_eq!(
            read_rows(&table).await,
            vec![vec![1, 11, 100], vec![2, 20, 200], vec![3, 30, 300]]
        );
    }
}

#[tokio::test]
async fn grouped_input_failure_preserves_files_and_the_cause() {
    let table = evolution_table().await;
    seed(&table).await;
    let before = parquet_files(&table).await;
    let update = table.new_write_builder().new_update().unwrap();
    let error = update
        .update_by_arrow_batches_with_row_id(vec![
            Ok(vec![matched(vec![0], &[("value", vec![11])])]),
            Err(paimon::Error::DataInvalid {
                message: "input generator failed".into(),
                source: None,
            }),
        ])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("input generator failed"));
    let after = parquet_files(&table).await;
    assert!(after.len() > before.len());
    assert!(before.iter().all(|path| after.contains(path)));
    assert_eq!(
        read_rows(&table).await,
        vec![vec![1, 10, 100], vec![2, 20, 200], vec![3, 30, 300]]
    );
}

#[tokio::test]
async fn core_assignments_validate_before_staging_and_preserve_chunk_alignment() {
    use paimon::table::UpdateAssignment;
    let table = evolution_table().await;
    seed(&table).await;
    let mut writer = table
        .new_write_builder()
        .new_data_evolution_writer(vec!["value".into(), "score".into()])
        .unwrap();
    let matched = vec![
        matched(vec![2, 0], &[("id", vec![3, 1])]),
        matched(vec![1], &[("id", vec![2])]),
    ];
    for invalid in [
        Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
        Arc::new(StringArray::from(vec!["1", "bad", "3"])),
        Arc::new(arrow_array::Float64Array::from(vec![1.0, 2.5, 3.0])),
        Arc::new(Int64Array::from(vec![1, i64::MAX, 3])),
    ] {
        assert!(writer
            .add_assigned_batches(
                matched.clone(),
                vec![
                    ("value".into(), UpdateAssignment::Array(vec![invalid])),
                    (
                        "score".into(),
                        UpdateAssignment::Scalar(Arc::new(Int32Array::from(vec![9])))
                    ),
                ]
            )
            .is_err());
    }
    writer
        .add_assigned_batches(
            matched,
            vec![
                (
                    "value".into(),
                    UpdateAssignment::Array(vec![
                        Arc::new(Int64Array::from(Vec::<i64>::new())),
                        Arc::new(Int64Array::from(vec![33])),
                        Arc::new(Int64Array::from(vec![11, 22])),
                    ]),
                ),
                (
                    "score".into(),
                    UpdateAssignment::Scalar(Arc::new(Int32Array::from(vec![999]))),
                ),
            ],
        )
        .unwrap();
    commit(&table, writer.prepare_commit().await.unwrap()).await;
    assert_eq!(
        read_rows(&table).await,
        vec![vec![1, 11, 999], vec![2, 22, 999], vec![3, 33, 999]]
    );
}

#[tokio::test]
async fn predicate_update_filters_exactly_and_calls_functions_per_file_group() {
    use paimon::spec::{Datum, PredicateBuilder};
    use paimon::table::UpdateAssignment;
    use std::sync::Mutex;
    let table = evolution_table().await;
    seed(&table).await;
    seed(&table).await;
    let mut update = table.new_write_builder().new_update().unwrap();
    commit(&table, update.delete_by_row_id(vec![1]).await.unwrap()).await;
    // Predicate updates choose assignment columns, independently of the
    // row-ID/upsert column selection on the reusable high-level updater.
    update.with_update_type(vec!["id".into()]).unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let calls = seen.clone();
    let predicate = PredicateBuilder::new(table.schema().fields())
        .greater_or_equal("id", Datum::Int(2))
        .unwrap();
    let messages = update
        .update_by_predicate(
            Some(predicate),
            vec![
                (
                    "value".into(),
                    UpdateAssignment::Function(Arc::new(move |batches| {
                        calls
                            .lock()
                            .unwrap()
                            .push(batches.iter().map(RecordBatch::num_rows).sum::<usize>());
                        batches
                            .iter()
                            .map(|batch| {
                                assert_eq!(
                                    batch
                                        .schema()
                                        .fields()
                                        .iter()
                                        .map(|f| f.name().as_str())
                                        .collect::<Vec<_>>(),
                                    vec!["value", "_ROW_ID"]
                                );
                                let values = batch
                                    .column(0)
                                    .as_any()
                                    .downcast_ref::<Int32Array>()
                                    .unwrap();
                                Ok(Arc::new(Int32Array::from_iter(
                                    values.iter().map(|v| v.map(|v| v + 1)),
                                )) as ArrayRef)
                            })
                            .collect()
                    })),
                ),
                (
                    "score".into(),
                    UpdateAssignment::Scalar(Arc::new(Int32Array::from(vec![999]))),
                ),
            ],
            vec!["value".into(), "value".into()],
        )
        .await
        .unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![1, 2]);
    assert!(messages
        .iter()
        .all(|message| message.check_from_snapshot == Some(3)));
    commit(&table, messages).await;
    assert_eq!(
        read_rows(&table).await,
        vec![
            vec![1, 10, 100],
            vec![1, 10, 100],
            vec![2, 21, 999],
            vec![3, 31, 999],
            vec![3, 31, 999],
        ]
    );
}

#[tokio::test]
async fn predicate_array_assignments_span_file_groups() {
    use paimon::table::UpdateAssignment;
    let table = evolution_table().await;
    seed(&table).await;
    seed(&table).await;
    let update = table.new_write_builder().new_update().unwrap();
    let messages = update
        .update_by_predicate(
            None,
            vec![(
                "score".into(),
                UpdateAssignment::Array(vec![
                    Arc::new(Int64Array::from(vec![11, 12])),
                    Arc::new(Int64Array::from(vec![13, 14, 15, 16])),
                ]),
            )],
            vec![],
        )
        .await
        .unwrap();
    commit(&table, messages).await;
    assert_eq!(
        read_rows(&table).await,
        vec![
            vec![1, 10, 11],
            vec![1, 10, 14],
            vec![2, 20, 12],
            vec![2, 20, 15],
            vec![3, 30, 13],
            vec![3, 30, 16],
        ]
    );
}

#[tokio::test]
async fn predicate_callback_failure_preserves_earlier_group_files() {
    use paimon::table::UpdateAssignment;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let table = evolution_table().await;
    seed(&table).await;
    seed(&table).await;
    let before = parquet_files(&table).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let error = table
        .new_write_builder()
        .new_update()
        .unwrap()
        .update_by_predicate(
            None,
            vec![(
                "value".into(),
                UpdateAssignment::Function(Arc::new(move |batches| {
                    if seen.fetch_add(1, Ordering::SeqCst) == 1 {
                        return Err(paimon::Error::DataInvalid {
                            message: "second callback failed".into(),
                            source: None,
                        });
                    }
                    Ok(batches
                        .iter()
                        .map(|batch| batch.column_by_name("value").unwrap().clone())
                        .collect())
                })),
            )],
            vec!["value".into()],
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("second callback failed"));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let after = parquet_files(&table).await;
    assert!(after.len() > before.len());
    assert!(before.iter().all(|path| after.contains(path)));
}

#[tokio::test]
async fn predicate_validation_and_no_matches_do_not_evaluate_assignments() {
    use paimon::spec::{Datum, PredicateBuilder};
    use paimon::table::UpdateAssignment;
    let table = evolution_table().await;
    seed(&table).await;
    let update = table.new_write_builder().new_update().unwrap();
    let panic_function = UpdateAssignment::Function(Arc::new(|_| panic!("must not evaluate")));
    for (assignments, columns, error) in [
        (vec![], vec![], "assignments must not be empty"),
        (
            vec![("value".into(), panic_function.clone())],
            vec![],
            "require read_columns",
        ),
        (
            vec![("value".into(), panic_function.clone())],
            vec!["missing".into()],
            "Read column missing",
        ),
        (
            vec![("missing".into(), panic_function.clone())],
            vec!["value".into()],
            "Column missing",
        ),
        (
            vec![
                ("value".into(), panic_function.clone()),
                ("score".into(), UpdateAssignment::Array(vec![])),
            ],
            vec!["value".into()],
            "cannot be combined",
        ),
    ] {
        assert!(update
            .update_by_predicate(None, assignments, columns)
            .await
            .unwrap_err()
            .to_string()
            .contains(error));
    }
    let predicate = PredicateBuilder::new(table.schema().fields())
        .equal("id", Datum::Int(99))
        .unwrap();
    assert!(update
        .update_by_predicate(
            Some(predicate),
            vec![
                ("value".into(), panic_function),
                (
                    "score".into(),
                    UpdateAssignment::DeferredScalar(Arc::new(|| panic!("unused scalar")))
                ),
            ],
            vec!["value".into()]
        )
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn predicate_update_preserves_previous_partial_values_outside_matches() {
    use paimon::spec::{Datum, PredicateBuilder};
    use paimon::table::UpdateAssignment;
    let table = evolution_table().await;
    seed(&table).await;
    let update = table.new_write_builder().new_update().unwrap();
    let messages = update
        .update_by_arrow_with_row_id(vec![matched(vec![0], &[("value", vec![11])])])
        .await
        .unwrap();
    commit(&table, messages).await;
    let predicate = PredicateBuilder::new(table.schema().fields())
        .equal("id", Datum::Int(2))
        .unwrap();
    let messages = update
        .update_by_predicate(
            Some(predicate),
            vec![(
                "value".into(),
                UpdateAssignment::Scalar(Arc::new(Int32Array::from(vec![22]))),
            )],
            vec![],
        )
        .await
        .unwrap();
    commit(&table, messages).await;
    assert_eq!(
        read_rows(&table).await,
        vec![vec![1, 11, 100], vec![2, 22, 200], vec![3, 30, 300]]
    );
}

#[tokio::test]
async fn predicate_assignments_keep_row_id_order_when_later_groups_have_deltas() {
    use paimon::table::UpdateAssignment;
    use std::sync::Mutex;
    for callable in [false, true] {
        let table = evolution_table().await;
        seed(&table).await;
        write_batch(
            &table,
            &batch(&[
                ("id", vec![4, 5, 6]),
                ("value", vec![40, 50, 60]),
                ("score", vec![400, 500, 600]),
            ]),
        )
        .await;
        let update = table.new_write_builder().new_update().unwrap();
        let messages = update
            .update_by_arrow_with_row_id(vec![matched(vec![3], &[("value", vec![44])])])
            .await
            .unwrap();
        commit(&table, messages).await;
        let seen = Arc::new(Mutex::new(Vec::new()));
        let calls = seen.clone();
        let (assignment, read_columns) = if callable {
            (
                UpdateAssignment::Function(Arc::new(move |batches| {
                    batches
                        .iter()
                        .map(|batch| {
                            let ids = batch
                                .column_by_name("_ROW_ID")
                                .unwrap()
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap();
                            calls.lock().unwrap().extend(ids.values().iter().copied());
                            Ok(Arc::new(Int32Array::from_iter_values(
                                ids.values().iter().map(|id| *id as i32 + 11),
                            )) as ArrayRef)
                        })
                        .collect()
                })),
                vec!["id".into()],
            )
        } else {
            (
                UpdateAssignment::Array(vec![
                    Arc::new(Int32Array::from(vec![11, 12])),
                    Arc::new(Int32Array::from(vec![13, 14, 15, 16])),
                ]),
                vec![],
            )
        };
        let messages = update
            .update_by_predicate(None, vec![("score".into(), assignment)], read_columns)
            .await
            .unwrap();
        if callable {
            assert_eq!(*seen.lock().unwrap(), vec![0, 1, 2, 3, 4, 5]);
        }
        commit(&table, messages).await;
        assert_eq!(
            read_rows(&table).await,
            vec![
                vec![1, 10, 11],
                vec![2, 20, 12],
                vec![3, 30, 13],
                vec![4, 44, 14],
                vec![5, 50, 15],
                vec![6, 60, 16],
            ]
        );
    }
}

#[tokio::test]
async fn row_id_integer_widths_work_for_direct_grouped_and_incremental_updates() {
    use arrow_array::UInt64Array;
    let table = evolution_table().await;
    seed(&table).await;
    let update = table.new_write_builder().new_update().unwrap();
    let input = |row_ids: ArrayRef, value: i32| {
        RecordBatch::try_from_iter([
            ("_ROW_ID", row_ids),
            ("value", Arc::new(Int32Array::from(vec![value])) as ArrayRef),
        ])
        .unwrap()
    };
    commit(
        &table,
        update
            .update_by_arrow_with_row_id(vec![input(Arc::new(Int32Array::from(vec![0])), 11)])
            .await
            .unwrap(),
    )
    .await;
    commit(
        &table,
        update
            .update_by_arrow_batches_with_row_id(vec![Ok(vec![input(
                Arc::new(UInt64Array::from(vec![1])),
                22,
            )])])
            .await
            .unwrap(),
    )
    .await;
    let mut writer = table
        .new_write_builder()
        .new_data_evolution_writer(vec!["value".into()])
        .unwrap();
    writer
        .add_matched_batch(input(Arc::new(Int32Array::from(vec![2])), 33))
        .unwrap();
    commit(&table, writer.prepare_commit().await.unwrap()).await;
    assert_eq!(
        read_rows(&table).await,
        vec![vec![1, 11, 100], vec![2, 22, 200], vec![3, 33, 300]]
    );
    let before = parquet_files(&table).await;
    assert!(update
        .update_by_arrow_with_row_id(vec![
            input(Arc::new(UInt64Array::from(vec![u64::MAX])), 99,)
        ])
        .await
        .is_err());
    assert_eq!(parquet_files(&table).await, before);
}

#[tokio::test]
async fn row_id_nested_overlap_uses_leaf_identity_across_calls() {
    use arrow_array::StructArray;
    use paimon::spec::{DataField, RowType};
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            "profile",
            DataType::Row(RowType::new(vec![
                DataField::new(2, "a".into(), DataType::Int(IntType::new())),
                DataField::new(3, "b".into(), DataType::Int(IntType::new())),
            ])),
        )
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        .option("data-evolution.nested-field.enabled", "true")
        .build()
        .unwrap();
    let path = "memory:/nested_overlap";
    let (io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&io, path).await;
    persist_table_schema(&io, path, table.schema()).await;
    let arrow_schema = paimon::arrow::build_target_arrow_schema(table.schema().fields()).unwrap();
    let arrow_schema::DataType::Struct(fields) = arrow_schema.field(1).data_type() else {
        panic!("ROW")
    };
    let profile = |a: Vec<i32>, b: Vec<i32>| {
        Arc::new(StructArray::new(
            fields.clone(),
            vec![Arc::new(Int32Array::from(a)), Arc::new(Int32Array::from(b))],
            None,
        )) as ArrayRef
    };
    write_batch(
        &table,
        &RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                profile(vec![10, 20], vec![100, 200]),
            ],
        )
        .unwrap(),
    )
    .await;
    let whole = RecordBatch::try_from_iter([
        ("_ROW_ID", Arc::new(Int64Array::from(vec![0])) as ArrayRef),
        ("profile", profile(vec![11], vec![101])),
    ])
    .unwrap();
    let child = matched(vec![1], &[("profile.a", vec![22])]);
    let update = table.new_write_builder().new_update().unwrap();
    for reverse in [false, true] {
        let mut updater = update.new_update_by_row_id().await.unwrap();
        let calls = if reverse {
            [(&child, "profile.a"), (&whole, "profile")]
        } else {
            [(&whole, "profile"), (&child, "profile.a")]
        };
        updater
            .update_columns(vec![calls[0].0.clone()], vec![calls[0].1.into()])
            .await
            .unwrap();
        let staged_files = parquet_files(&table).await;
        let messages = updater.commit_messages().len();
        let error = updater
            .update_columns(vec![calls[1].0.clone()], vec![calls[1].1.into()])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("overlapping first_row_ids"));
        assert_eq!(updater.commit_messages().len(), messages);
        assert_eq!(parquet_files(&table).await, staged_files);
        updater.abort().await.unwrap();
        assert_eq!(parquet_files(&table).await, staged_files);
    }
    // Disjoint sibling leaves of the same file group remain valid.
    let mut updater = update.new_update_by_row_id().await.unwrap();
    updater
        .update_columns(vec![child], vec!["profile.a".into()])
        .await
        .unwrap();
    let messages = updater
        .update_columns(
            vec![matched(vec![0], &[("profile.b", vec![101])])],
            vec!["profile.b".into()],
        )
        .await
        .unwrap();
    commit(&table, messages).await;
    let read = table.new_read_builder();
    let plan = read.new_scan().plan().await.unwrap();
    let batches: Vec<RecordBatch> = read
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut actual = Vec::new();
    for batch in batches {
        let profile = batch
            .column_by_name("profile")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let a = profile
            .column_by_name("a")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let b = profile
            .column_by_name("b")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        actual.extend((0..batch.num_rows()).map(|row| (a.value(row), b.value(row))));
    }
    actual.sort();
    assert_eq!(actual, vec![(10, 101), (22, 200)]);
}

#[tokio::test]
async fn row_id_update_ignores_empty_chunks_before_integer_normalization() {
    let table = evolution_table().await;
    seed(&table).await;
    let mut by_row_id = table
        .new_write_builder()
        .new_update()
        .unwrap()
        .new_update_by_row_id()
        .await
        .unwrap();
    let error = by_row_id
        .update_columns(vec![batch(&[("value", vec![])])], vec!["value".into()])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("must contain _ROW_ID"));
    assert!(by_row_id.commit_messages().is_empty());
    for unsigned in [false, true] {
        let table = evolution_table().await;
        seed(&table).await;
        let ids: ArrayRef = if unsigned {
            Arc::new(arrow_array::UInt32Array::from(vec![0, 2]))
        } else {
            Arc::new(Int32Array::from(vec![0, 2]))
        };
        let input = RecordBatch::try_from_iter([
            ("_ROW_ID", ids),
            (
                "value",
                Arc::new(Int32Array::from(vec![99, 77])) as ArrayRef,
            ),
        ])
        .unwrap();
        let update = table.new_write_builder().new_update().unwrap();
        let messages = update
            .update_by_arrow_with_row_id(vec![
                input.slice(0, 0),
                input.slice(0, 1),
                input.slice(1, 0),
                input.slice(1, 1),
                input.slice(2, 0),
            ])
            .await
            .unwrap();
        commit(&table, messages).await;
        assert_eq!(
            read_rows(&table).await,
            vec![vec![1, 99, 100], vec![2, 20, 200], vec![3, 77, 300]]
        );
    }
}
