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

use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, Int8Array, RecordBatch};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs};
use futures::TryStreamExt;
use paimon::spec::{
    BigIntType, DataField, DataType, Datum, IntType, Predicate, PredicateOperator, Schema,
    TableSchema,
};
use paimon::table::Table;
use std::sync::Arc;

async fn table() -> Table {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("v", DataType::Int(IntType::new()))
        .column("w", DataType::Int(IntType::new()))
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        .build()
        .unwrap();
    create_table(schema).await
}

async fn create_table(schema: Schema) -> Table {
    let (io, table) = memory_table("memory:/system_columns", TableSchema::new(0, &schema));
    setup_dirs(&io, table.location()).await;
    persist_table_schema(&io, table.location(), table.schema()).await;
    table
}

async fn append(table: &Table, start: i32) {
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    let batch = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int32Array::from(vec![start, start + 1])) as ArrayRef,
        ),
        ("v", Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef),
        ("w", Arc::new(Int32Array::from(vec![100, 200])) as ArrayRef),
    ])
    .unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    writer.close().await;
    builder.new_commit().commit(messages).await.unwrap();
}

async fn update(table: &Table, field: &str, row_id: i64, value: i32) {
    let builder = table.new_write_builder();
    let messages = builder
        .new_update()
        .unwrap()
        .update_by_arrow_with_row_id(vec![RecordBatch::try_from_iter([
            (
                "_ROW_ID",
                Arc::new(Int64Array::from(vec![row_id])) as ArrayRef,
            ),
            (field, Arc::new(Int32Array::from(vec![value])) as ArrayRef),
        ])
        .unwrap()])
        .await
        .unwrap();
    builder.new_commit().commit(messages).await.unwrap();
}

fn sequence_field() -> DataField {
    DataField::new(
        paimon::spec::SEQUENCE_NUMBER_FIELD_ID,
        "_SEQUENCE_NUMBER".into(),
        DataType::BigInt(BigIntType::with_nullable(false)),
    )
}

async fn sequences(table: &Table) -> paimon::Result<Vec<i64>> {
    let mut builder = table.new_read_builder();
    builder.with_read_type(vec![sequence_field()]);
    let plan = builder.new_scan().plan().await?;
    let batches: Vec<_> = builder
        .new_read()?
        .to_arrow(plan.splits())?
        .try_collect()
        .await?;
    let mut values = Vec::new();
    for batch in batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(column.null_count(), 0);
        values.extend(column.values().iter().copied());
    }
    values.sort_unstable();
    Ok(values)
}

#[tokio::test]
async fn partial_files_supply_the_latest_sequence_without_a_physical_column() {
    let table = table().await;
    append(&table, 0).await;
    assert_eq!(sequences(&table).await.unwrap(), vec![1, 1]);
    update(&table, "v", 0, 77).await;
    assert_eq!(sequences(&table).await.unwrap(), vec![2, 2]);
    update(&table, "w", 1, 88).await;
    assert_eq!(sequences(&table).await.unwrap(), vec![3, 3]);
}

fn sequence_predicate(op: PredicateOperator, values: &[i64]) -> Predicate {
    // A system column has no position in the logical schema. This deliberately
    // aliases the id column to catch accidental index-based stats/filtering.
    Predicate::Leaf {
        column: "_SEQUENCE_NUMBER".into(),
        index: 0,
        data_type: DataType::BigInt(BigIntType::with_nullable(false)),
        op,
        literals: values.iter().map(|v| Datum::Long(*v)).collect(),
    }
}

async fn matching_ids(table: &Table, predicate: Predicate, project_sequence: bool) -> Vec<i32> {
    let mut builder = table.new_read_builder();
    let projection = if project_sequence {
        vec!["id", "_SEQUENCE_NUMBER"]
    } else {
        vec!["id"]
    };
    builder
        .with_projection(&projection)
        .unwrap()
        .with_filter(predicate);
    let plan = builder.new_scan().plan().await.unwrap();
    let batches: Vec<_> = builder
        .new_read()
        .unwrap()
        .to_arrow(plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut ids = Vec::new();
    for batch in batches {
        assert_eq!(batch.num_columns(), projection.len());
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        ids.extend(values.values().iter().copied());
    }
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn sequence_predicates_use_metadata_before_and_after_partial_updates() {
    let table = table().await;
    append(&table, 100).await;
    append(&table, 200).await;
    for project in [false, true] {
        assert_eq!(
            matching_ids(
                &table,
                sequence_predicate(PredicateOperator::Eq, &[1]),
                project
            )
            .await,
            vec![100, 101]
        );
        assert_eq!(
            matching_ids(
                &table,
                sequence_predicate(PredicateOperator::Eq, &[2]),
                project
            )
            .await,
            vec![200, 201]
        );
    }
    update(&table, "v", 0, 77).await;
    for project in [false, true] {
        assert_eq!(
            matching_ids(
                &table,
                sequence_predicate(PredicateOperator::Eq, &[3]),
                project
            )
            .await,
            vec![100, 101]
        );
        assert_eq!(
            matching_ids(
                &table,
                sequence_predicate(PredicateOperator::Lt, &[3]),
                project
            )
            .await,
            vec![200, 201]
        );
        assert_eq!(
            matching_ids(
                &table,
                sequence_predicate(PredicateOperator::IsNull, &[]),
                project
            )
            .await,
            Vec::<i32>::new()
        );
        assert_eq!(
            matching_ids(
                &table,
                sequence_predicate(PredicateOperator::IsNotNull, &[]),
                project
            )
            .await,
            vec![100, 101, 200, 201]
        );
    }
    let id = paimon::spec::PredicateBuilder::new(table.schema().fields())
        .equal("id", Datum::Int(200))
        .unwrap();
    assert_eq!(
        matching_ids(
            &table,
            Predicate::or(vec![sequence_predicate(PredicateOperator::Eq, &[3]), id]),
            false
        )
        .await,
        vec![100, 101, 200]
    );
    assert_eq!(
        matching_ids(
            &table,
            Predicate::Not(Box::new(sequence_predicate(PredicateOperator::Eq, &[3]))),
            false
        )
        .await,
        vec![200, 201]
    );
}

#[tokio::test]
async fn physical_versions_are_kept_and_null_versions_use_manifest_metadata() {
    use parquet::arrow::ArrowWriter;
    let table = table().await;
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int32Array::from(vec![100, 101])) as ArrayRef),
        ("v", Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef),
        ("w", Arc::new(Int32Array::from(vec![100, 200])) as ArrayRef),
    ])
    .unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    let mut messages = writer.prepare_commit().await.unwrap();
    writer.close().await;
    let schema = batch.schema();
    let mut columns: Vec<_> = schema
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, array)| (field.name().as_str(), array.clone()))
        .collect();
    columns.push((
        "_SEQUENCE_NUMBER",
        Arc::new(Int64Array::from(vec![Some(7), None])) as ArrayRef,
    ));
    let physical = RecordBatch::try_from_iter(columns).unwrap();
    let mut parquet = ArrowWriter::try_new(Vec::new(), physical.schema(), None).unwrap();
    parquet.write(&physical).unwrap();
    let data = bytes::Bytes::from(parquet.into_inner().unwrap());
    let file = &mut messages[0].new_files[0];
    file.file_size = data.len() as i64;
    table
        .file_io()
        .new_output(&format!("{}/bucket-0/{}", table.location(), file.file_name))
        .unwrap()
        .write(data)
        .await
        .unwrap();
    builder.new_commit().commit(messages).await.unwrap();
    assert_eq!(sequences(&table).await.unwrap(), vec![1, 7]);
    assert_eq!(
        matching_ids(
            &table,
            sequence_predicate(PredicateOperator::Eq, &[7]),
            false
        )
        .await,
        vec![100]
    );
    assert_eq!(
        matching_ids(
            &table,
            sequence_predicate(PredicateOperator::Eq, &[1]),
            false
        )
        .await,
        vec![101]
    );
}

#[tokio::test]
async fn blob_versions_cannot_replace_the_normal_metadata_provider() {
    use arrow_array::LargeBinaryArray;
    use paimon::spec::{BinaryRow, BlobType};
    use paimon::table::DataSplitBuilder;
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("payload", DataType::Blob(BlobType::new()))
        .option("row-tracking.enabled", "true")
        .option("data-evolution.enabled", "true")
        .build()
        .unwrap();
    let table = create_table(schema).await;
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer
        .write_arrow_batch(
            &RecordBatch::try_from_iter([
                ("id", Arc::new(Int32Array::from(vec![100, 101])) as ArrayRef),
                (
                    "payload",
                    Arc::new(LargeBinaryArray::from(vec![
                        Some(b"a".as_slice()),
                        Some(b"b".as_slice()),
                    ])) as ArrayRef,
                ),
            ])
            .unwrap(),
        )
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    writer.close().await;
    builder.new_commit().commit(messages).await.unwrap();
    let mut files = table
        .new_read_builder()
        .new_scan()
        .plan()
        .await
        .unwrap()
        .splits()[0]
        .data_files()
        .to_vec();
    let blob = files
        .iter_mut()
        .find(|file| file.file_name.ends_with(".blob"))
        .unwrap();
    blob.max_sequence_number = 99;
    // An unrequested payload file must not be read just to supply metadata.
    table
        .file_io()
        .delete_file(&format!("{}/bucket-0/{}", table.location(), blob.file_name))
        .await
        .unwrap();
    let split = DataSplitBuilder::new()
        .with_snapshot(1)
        .with_partition(BinaryRow::new(0))
        .with_bucket(0)
        .with_total_buckets(1)
        .with_bucket_path(format!("{}/bucket-0", table.location()))
        .with_data_files(files)
        .build()
        .unwrap();
    let mut read = table.new_read_builder();
    read.with_projection(&["id", "_SEQUENCE_NUMBER"]).unwrap();
    let batches: Vec<_> = read
        .new_read()
        .unwrap()
        .to_arrow(&[split])
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    for batch in batches {
        let seq = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(seq.values().as_ref(), &[1, 1]);
    }
}

#[tokio::test]
async fn primary_key_metadata_never_uses_the_user_aggregate_function() {
    for engine in ["deduplicate", "first-row", "partial-update", "aggregation"] {
        let mut schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("v", DataType::Int(IntType::new()))
            .primary_key(["id"])
            .option("bucket", "1")
            .option("merge-engine", engine);
        if engine == "aggregation" {
            schema = schema.option("fields.default-aggregate-function", "sum");
        }
        let schema = schema.build().unwrap();
        let table = create_table(schema).await;
        let table = if engine == "first-row" {
            // A Java pk-clustering table permits first-row DVs. Reproduce the
            // resolved catalog options and read level-0 files without compaction.
            table.copy_with_options(std::collections::HashMap::from([
                ("deletion-vectors.enabled".into(), "true".into()),
                ("deletion-vectors.merge-on-read".into(), "true".into()),
                ("pk-clustering-override".into(), "true".into()),
            ]))
        } else {
            table
        };
        for _ in 0..3 {
            let builder = table.new_write_builder();
            let mut writer = builder.new_write().unwrap();
            writer
                .write_arrow_batch(
                    &RecordBatch::try_from_iter([
                        ("id", Arc::new(Int32Array::from(vec![100])) as ArrayRef),
                        ("v", Arc::new(Int32Array::from(vec![10])) as ArrayRef),
                    ])
                    .unwrap(),
                )
                .await
                .unwrap();
            let messages = writer.prepare_commit().await.unwrap();
            writer.close().await;
            builder.new_commit().commit(messages).await.unwrap();
        }
        let sequence = if engine == "first-row" { 0 } else { 2 };
        assert_eq!(
            matching_ids(
                &table,
                sequence_predicate(PredicateOperator::Eq, &[sequence]),
                false
            )
            .await,
            vec![100]
        );
        assert_eq!(
            matching_ids(
                &table,
                sequence_predicate(PredicateOperator::NotEq, &[sequence]),
                true
            )
            .await,
            Vec::<i32>::new()
        );
        let mut reader = table.new_read_builder();
        reader.with_projection(&["v", "_SEQUENCE_NUMBER"]).unwrap();
        let plan = reader.new_scan().plan().await.unwrap();
        let batches: Vec<_> = reader
            .new_read()
            .unwrap()
            .to_arrow(plan.splits())
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(batches.len(), 1);
        let value = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            value.value(0),
            if engine == "aggregation" { 30 } else { 10 }
        );
        let seq = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(seq.value(0), sequence);
    }
}

async fn retract_metadata(engine: &str) {
    for kind in [1_i8, 3] {
        for one_file in [false, true] {
            let mut schema = Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("g", DataType::Int(IntType::new()))
                .column("v", DataType::Int(IntType::new()))
                .primary_key(["id"])
                .option("bucket", "1")
                .option("merge-engine", engine);
            schema = if engine == "aggregation" {
                schema
                    .option("fields.default-aggregate-function", "sum")
                    .option("aggregation.remove-record-on-delete", "false")
            } else {
                schema.option("fields.g.sequence-group", "v")
            };
            let table = create_table(schema.build().unwrap()).await;
            let operations = [(1, 10, 0_i8), (2, 5, kind)];
            for rows in operations.chunks(if one_file { 2 } else { 1 }) {
                let builder = table.new_write_builder();
                let mut writer = builder.new_write().unwrap();
                writer
                    .write_arrow_batch(
                        &RecordBatch::try_from_iter([
                            (
                                "id",
                                Arc::new(Int32Array::from(vec![100; rows.len()])) as ArrayRef,
                            ),
                            (
                                "g",
                                Arc::new(Int32Array::from(
                                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                                )) as ArrayRef,
                            ),
                            (
                                "v",
                                Arc::new(Int32Array::from(
                                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                                )) as ArrayRef,
                            ),
                            (
                                "_VALUE_KIND",
                                Arc::new(Int8Array::from(
                                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                                )) as ArrayRef,
                            ),
                        ])
                        .unwrap(),
                    )
                    .await
                    .unwrap();
                let messages = writer.prepare_commit().await.unwrap();
                writer.close().await;
                builder.new_commit().commit(messages).await.unwrap();
            }
            assert_eq!(
                sequences(&table).await.unwrap(),
                vec![1],
                "{engine}, kind={kind}, one_file={one_file}"
            );
            assert_eq!(
                matching_ids(
                    &table,
                    sequence_predicate(PredicateOperator::Eq, &[1]),
                    false
                )
                .await,
                vec![100]
            );
            let mut reader = table.new_read_builder();
            reader.with_projection(&["v", "_SEQUENCE_NUMBER"]).unwrap();
            let plan = reader.new_scan().plan().await.unwrap();
            let batches: Vec<_> = reader
                .new_read()
                .unwrap()
                .to_arrow(plan.splits())
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            let values = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            if engine == "aggregation" {
                assert_eq!(values.value(0), 5);
            } else {
                assert!(values.is_null(0));
            }
        }
    }
}

#[tokio::test]
async fn aggregation_retractions_keep_the_latest_sequence() {
    retract_metadata("aggregation").await;
}

#[tokio::test]
async fn partial_update_retractions_keep_the_latest_sequence() {
    retract_metadata("partial-update").await;
}
