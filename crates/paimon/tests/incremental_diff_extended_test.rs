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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod common;

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, Int32Array, ListArray, MapArray, RecordBatch, StringArray, StructArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::DataType as ArrowDataType;
use bytes::Bytes;
use futures::TryStreamExt;
use indexmap::IndexMap;
use paimon::arrow::build_target_arrow_schema;
use paimon::spec::{
    ArrayType, DataField, DataType, DeletionVectorMeta, FileKind, IndexFileMeta, IndexManifest,
    IndexManifestEntry, IntType, MapType, MultisetType, RowType, Schema, TableSchema, VarCharType,
};
use paimon::table::{IncrementalPlan, IncrementalScanMode, IncrementalSplit, Table};
use roaring::RoaringBitmap;

use common::incremental_helpers::{
    make_batch, memory_table, persist_table_schema, pk_schema, setup_dirs, write_batch,
};

type StringMap = Option<Vec<(&'static str, Option<i32>)>>;
type StringBag = Option<Vec<(&'static str, i32)>>;

#[derive(Clone)]
struct NestedRow {
    id: i32,
    payload: Option<Option<i32>>,
    items: Option<Vec<Option<i32>>>,
    attributes: StringMap,
    bag: StringBag,
}

impl NestedRow {
    fn new(id: i32) -> Self {
        Self {
            id,
            payload: None,
            items: None,
            attributes: None,
            bag: None,
        }
    }

    fn payload(mut self, value: Option<i32>) -> Self {
        self.payload = Some(value);
        self
    }

    fn items(mut self, values: Vec<Option<i32>>) -> Self {
        self.items = Some(values);
        self
    }

    fn attributes(mut self, values: Vec<(&'static str, Option<i32>)>) -> Self {
        self.attributes = Some(values);
        self
    }

    fn bag(mut self, values: Vec<(&'static str, i32)>) -> Self {
        self.bag = Some(values);
        self
    }
}

fn nested_schema() -> TableSchema {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            "payload",
            DataType::Row(RowType::new(vec![DataField::new(
                2,
                "child".to_string(),
                DataType::Int(IntType::new()),
            )])),
        )
        .column(
            "items",
            DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
        )
        .column(
            "attributes",
            DataType::Map(MapType::new(
                DataType::VarChar(VarCharType::string_type()),
                DataType::Int(IntType::new()),
            )),
        )
        .column(
            "bag",
            DataType::Multiset(MultisetType::new(DataType::VarChar(
                VarCharType::string_type(),
            ))),
        )
        .primary_key(["id"])
        .option("bucket", "1")
        .option("merge-engine", "deduplicate")
        .build()
        .unwrap();
    TableSchema::new(0, &schema)
}

fn map_array(arrow_type: &ArrowDataType, values: &[StringMap]) -> MapArray {
    let ArrowDataType::Map(entries_field, sorted) = arrow_type else {
        panic!("expected Arrow Map")
    };
    let ArrowDataType::Struct(entry_fields) = entries_field.data_type() else {
        panic!("expected Map entries struct")
    };
    let mut offsets = vec![0_i32];
    let mut keys = Vec::new();
    let mut items = Vec::new();
    let mut valid = Vec::new();
    for map in values {
        valid.push(map.is_some());
        if let Some(map) = map {
            for (key, value) in map {
                keys.push(*key);
                items.push(*value);
            }
        }
        offsets.push(keys.len() as i32);
    }
    let entries = StructArray::try_new(
        entry_fields.clone(),
        vec![
            Arc::new(StringArray::from(keys)) as ArrayRef,
            Arc::new(Int32Array::from(items)) as ArrayRef,
        ],
        None,
    )
    .unwrap();
    MapArray::try_new(
        Arc::clone(entries_field),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        entries,
        Some(NullBuffer::from(valid)),
        *sorted,
    )
    .unwrap()
}

fn nested_batch(table: &Table, rows: &[NestedRow]) -> RecordBatch {
    let schema = build_target_arrow_schema(table.schema().fields()).unwrap();
    let ArrowDataType::Struct(payload_fields) = schema.field(1).data_type() else {
        panic!("expected payload struct")
    };
    let child = Int32Array::from(
        rows.iter()
            .map(|row| row.payload.flatten())
            .collect::<Vec<_>>(),
    );
    let payload = StructArray::try_new(
        payload_fields.clone(),
        vec![Arc::new(child)],
        Some(NullBuffer::from(
            rows.iter()
                .map(|row| row.payload.is_some())
                .collect::<Vec<_>>(),
        )),
    )
    .unwrap();

    let ArrowDataType::List(element_field) = schema.field(2).data_type() else {
        panic!("expected items list")
    };
    let mut offsets = vec![0_i32];
    let mut elements: Vec<Option<i32>> = Vec::new();
    let mut list_valid = Vec::new();
    for row in rows {
        list_valid.push(row.items.is_some());
        if let Some(items) = &row.items {
            elements.extend(items);
        }
        offsets.push(elements.len() as i32);
    }
    let items = ListArray::new(
        Arc::clone(element_field),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        Arc::new(Int32Array::from(elements)),
        Some(NullBuffer::from(list_valid)),
    );

    let attributes = rows
        .iter()
        .map(|row| row.attributes.clone())
        .collect::<Vec<_>>();
    let bag = rows
        .iter()
        .map(|row| {
            row.bag.as_ref().map(|values| {
                values
                    .iter()
                    .map(|(key, count)| (*key, Some(*count)))
                    .collect::<Vec<_>>()
            })
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(
                rows.iter().map(|row| row.id).collect::<Vec<_>>(),
            )),
            Arc::new(payload),
            Arc::new(items),
            Arc::new(map_array(schema.field(3).data_type(), &attributes)),
            Arc::new(map_array(schema.field(4).data_type(), &bag)),
        ],
    )
    .unwrap()
}

async fn nested_table(path: &str) -> Table {
    let (file_io, table) = memory_table(path, nested_schema());
    setup_dirs(&file_io, path).await;
    persist_table_schema(&file_io, path, table.schema()).await;
    table
}

async fn diff_plan(table: &Table, start: i64, end: i64) -> IncrementalPlan {
    table
        .new_read_builder()
        .new_incremental_scan(IncrementalScanMode::Diff, start, end)
        .plan()
        .await
        .unwrap()
}

async fn diff_batches(table: &Table, plan: &IncrementalPlan) -> Vec<RecordBatch> {
    table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_incremental_arrow(plan)
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

fn ids_at(batches: &[RecordBatch], index: usize) -> Vec<i32> {
    let mut ids = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(index)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn diff_detects_each_nested_value_type_and_skips_unchanged_rows() {
    let table = nested_table("memory:/incremental_diff/nested_values").await;
    let before = vec![
        NestedRow::new(1).payload(Some(10)),
        NestedRow::new(2).items(vec![Some(1), None]),
        NestedRow::new(3).attributes(vec![("a", Some(1))]),
        NestedRow::new(4).bag(vec![("x", 2)]),
        NestedRow::new(5)
            .payload(None)
            .items(vec![])
            .attributes(vec![])
            .bag(vec![]),
        NestedRow::new(6),
    ];
    let after = vec![
        NestedRow::new(1).payload(Some(11)),
        NestedRow::new(2).items(vec![Some(1), Some(2)]),
        NestedRow::new(3).attributes(vec![("a", Some(2))]),
        NestedRow::new(4).bag(vec![("x", 3)]),
        before[4].clone(),
        before[5].clone(),
        NestedRow::new(7).items(vec![]),
    ];
    write_batch(&table, &nested_batch(&table, &before)).await;
    write_batch(&table, &nested_batch(&table, &after)).await;

    let plan = diff_plan(&table, 1, 2).await;
    let batches = diff_batches(&table, &plan).await;
    assert_eq!(ids_at(&batches, 0), vec![1, 2, 3, 4, 7]);

    let audit_batches: Vec<RecordBatch> = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_audit_log_arrow(&plan)
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut changes = Vec::new();
    for batch in audit_batches {
        let kinds = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            changes.push((ids.value(row), kinds.value(row).to_string()));
        }
    }
    changes.sort();
    assert_eq!(
        changes,
        vec![
            (1, "+U".to_string()),
            (1, "-U".to_string()),
            (2, "+U".to_string()),
            (2, "-U".to_string()),
            (3, "+U".to_string()),
            (3, "-U".to_string()),
            (4, "+U".to_string()),
            (4, "-U".to_string()),
            (7, "+I".to_string()),
        ]
    );
}

#[tokio::test]
async fn diff_nested_null_empty_and_projection_semantics() {
    let table = nested_table("memory:/incremental_diff/nested_nulls").await;
    let before = vec![
        NestedRow::new(1),
        NestedRow::new(2).payload(None),
        NestedRow::new(3).items(vec![]),
        NestedRow::new(4).attributes(vec![]),
        NestedRow::new(5).bag(vec![]),
        NestedRow::new(6).items(vec![None]),
    ];
    let after = vec![
        NestedRow::new(1).payload(None),
        NestedRow::new(2).payload(Some(0)),
        NestedRow::new(3),
        NestedRow::new(4),
        NestedRow::new(5),
        NestedRow::new(6).items(vec![Some(0)]),
    ];
    write_batch(&table, &nested_batch(&table, &before)).await;
    write_batch(&table, &nested_batch(&table, &after)).await;

    assert_eq!(
        ids_at(
            &diff_batches(&table, &diff_plan(&table, 1, 2).await).await,
            0
        ),
        vec![1, 2, 3, 4, 5, 6]
    );

    let mut builder = table.new_read_builder();
    builder.with_projection(&["id"]).unwrap();
    let plan = builder
        .new_incremental_scan(IncrementalScanMode::Diff, 1, 2)
        .plan()
        .await
        .unwrap();
    let batches: Vec<RecordBatch> = builder
        .new_read()
        .unwrap()
        .to_incremental_arrow(&plan)
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(ids_at(&batches, 0), vec![1, 2, 3, 4, 5, 6]);
}

/// Install a real bitmap and index manifest for one historical snapshot. The
/// data writer currently emits L0 files for DV tables; patching only snapshot
/// index metadata models the materialized, Java-written per-file DV state.
async fn install_snapshot_dv(
    table: &Table,
    snapshot_id: i64,
    partition: &[u8],
    bucket: i32,
    data_file_name: &str,
    deleted_positions: &[u32],
) {
    const MAGIC_NUMBER: i32 = 1581511376;
    let mut bitmap = RoaringBitmap::new();
    for position in deleted_positions {
        bitmap.insert(*position);
    }
    let mut bitmap_bytes = Vec::new();
    bitmap.serialize_into(&mut bitmap_bytes).unwrap();
    let bitmap_length = 4 + bitmap_bytes.len() as i32;
    let mut dv_bytes = Vec::new();
    dv_bytes.extend_from_slice(&bitmap_length.to_be_bytes());
    dv_bytes.extend_from_slice(&MAGIC_NUMBER.to_be_bytes());
    dv_bytes.extend_from_slice(&bitmap_bytes);
    dv_bytes.extend_from_slice(&0_i32.to_be_bytes());

    let dv_name = format!("diff-dv-{snapshot_id}");
    let dv_path = format!("{}/index/{dv_name}", table.location());
    table
        .file_io()
        .mkdirs(&format!("{}/index", table.location()))
        .await
        .unwrap();
    table
        .file_io()
        .new_output(&dv_path)
        .unwrap()
        .write(Bytes::from(dv_bytes.clone()))
        .await
        .unwrap();
    let entry = IndexManifestEntry {
        version: 1,
        kind: FileKind::Add,
        partition: partition.to_vec(),
        bucket,
        index_file: IndexFileMeta {
            index_type: "DELETION_VECTORS".to_string(),
            file_name: dv_name,
            file_size: dv_bytes.len() as i64,
            row_count: 1,
            deletion_vectors_ranges: Some(IndexMap::from([(
                data_file_name.to_string(),
                DeletionVectorMeta {
                    offset: 0,
                    length: bitmap_length,
                    cardinality: Some(deleted_positions.len() as i64),
                },
            )])),
            external_path: None,
            global_index_meta: None,
        },
    };
    let manifest_name = format!("diff-index-{snapshot_id}");
    IndexManifest::write(
        table.file_io(),
        &format!("{}/manifest/{manifest_name}", table.location()),
        &[entry],
    )
    .await
    .unwrap();

    let manager = table.snapshot_manager();
    let snapshot = manager.get_snapshot(snapshot_id).await.unwrap();
    let mut metadata = serde_json::to_value(snapshot).unwrap();
    metadata["indexManifest"] = serde_json::json!(manifest_name);
    table
        .file_io()
        .new_output(&manager.snapshot_path(snapshot_id))
        .unwrap()
        .write(Bytes::from(serde_json::to_vec(&metadata).unwrap()))
        .await
        .unwrap();
}

async fn dv_table(path: &str) -> (Table, Vec<u8>, i32, String) {
    let (file_io, table) = memory_table(
        path,
        pk_schema(&[
            ("deletion-vectors.enabled", "true"),
            ("deletion-vectors.merge-on-read", "true"),
            ("merge-engine", "deduplicate"),
        ]),
    );
    setup_dirs(&file_io, path).await;
    persist_table_schema(&file_io, path, table.schema()).await;
    write_batch(&table, &make_batch(vec![1, 2, 3], vec![10, 20, 30])).await;
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let split = &plan.splits()[0];
    assert_eq!(split.data_files().len(), 1);
    let location = (
        split.partition().to_serialized_bytes(),
        split.bucket(),
        split.data_files()[0].file_name.clone(),
    );
    (table, location.0, location.1, location.2)
}

fn dv_attached(plan: &IncrementalPlan, before: bool) -> bool {
    plan.splits().iter().any(|split| {
        let IncrementalSplit::DiffPair {
            before: before_splits,
            after: after_splits,
        } = split
        else {
            panic!("Diff plan must contain only split pairs")
        };
        let side = if before { before_splits } else { after_splits };
        side.iter().any(|split| {
            split
                .data_deletion_files()
                .is_some_and(|files| files.iter().any(Option::is_some))
        })
    })
}

#[tokio::test]
async fn diff_uses_the_deletion_vectors_of_each_snapshot() {
    let (table, partition, bucket, first_file) =
        dv_table("memory:/incremental_diff/dv_history").await;
    write_batch(&table, &make_batch(vec![4], vec![40])).await;
    write_batch(&table, &make_batch(vec![2, 3], vec![25, 35])).await;
    install_snapshot_dv(&table, 2, &partition, bucket, &first_file, &[1]).await;
    install_snapshot_dv(&table, 3, &partition, bucket, &first_file, &[1, 2]).await;

    let first_diff = diff_plan(&table, 1, 2).await;
    assert!(!dv_attached(&first_diff, true));
    assert!(dv_attached(&first_diff, false));
    let first_batches = diff_batches(&table, &first_diff).await;
    assert_eq!(ids_at(&first_batches, 0), vec![4]);

    let first_audit: Vec<RecordBatch> = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_audit_log_arrow(&first_diff)
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut first_events = first_audit
        .iter()
        .flat_map(|batch| {
            let kinds = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let ids = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            (0..batch.num_rows())
                .map(|row| (ids.value(row), kinds.value(row).to_string()))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    first_events.sort();
    assert_eq!(
        first_events,
        vec![(2, "-D".to_string()), (4, "+I".to_string())]
    );

    let second_diff = diff_plan(&table, 2, 3).await;
    assert!(dv_attached(&second_diff, true));
    assert!(dv_attached(&second_diff, false));
    let second_batches = diff_batches(&table, &second_diff).await;
    assert_eq!(ids_at(&second_batches, 0), vec![2, 3]);
    let mut values = second_batches
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            (0..batch.num_rows())
                .map(|row| (ids.value(row), values.value(row)))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    values.sort();
    assert_eq!(values, vec![(2, 25), (3, 35)]);
}

async fn write_level_one_batch(table: &Table, batch: &RecordBatch) {
    let builder = table.new_write_builder();
    let mut writer = builder.new_write().unwrap();
    writer.write_arrow_batch(batch).await.unwrap();
    let mut messages = writer.prepare_commit().await.unwrap();
    for message in &mut messages {
        for file in &mut message.new_files {
            file.level = 1;
            file.delete_row_count = Some(0);
        }
    }
    builder.new_commit().commit(messages).await.unwrap();
}

#[tokio::test]
async fn diff_reads_materialized_dv_files_without_merge_on_read() {
    let path = "memory:/incremental_diff/dv_materialized";
    let (file_io, table) = memory_table(
        path,
        pk_schema(&[
            ("deletion-vectors.enabled", "true"),
            ("merge-engine", "deduplicate"),
        ]),
    );
    setup_dirs(&file_io, path).await;
    persist_table_schema(&file_io, path, table.schema()).await;
    write_level_one_batch(&table, &make_batch(vec![1, 2, 3], vec![10, 20, 30])).await;
    let first_plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let first_split = &first_plan.splits()[0];
    let partition = first_split.partition().to_serialized_bytes();
    let bucket = first_split.bucket();
    let first_file = first_split.data_files()[0].file_name.clone();
    write_level_one_batch(&table, &make_batch(vec![4], vec![40])).await;
    install_snapshot_dv(&table, 2, &partition, bucket, &first_file, &[1]).await;

    let plan = diff_plan(&table, 1, 2).await;
    assert!(!dv_attached(&plan, true));
    assert!(dv_attached(&plan, false));
    assert_eq!(ids_at(&diff_batches(&table, &plan).await, 0), vec![4]);

    let audit_batches: Vec<RecordBatch> = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_audit_log_arrow(&plan)
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut events = Vec::new();
    for batch in audit_batches {
        let kinds = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            events.push((ids.value(row), kinds.value(row).to_string()));
        }
    }
    events.sort();
    assert_eq!(events, vec![(2, "-D".to_string()), (4, "+I".to_string())]);
}

#[tokio::test]
async fn diff_fails_when_a_historical_deletion_vector_is_missing() {
    let (table, partition, bucket, first_file) =
        dv_table("memory:/incremental_diff/dv_missing").await;
    write_batch(&table, &make_batch(vec![4], vec![40])).await;
    install_snapshot_dv(&table, 1, &partition, bucket, &first_file, &[1]).await;
    let plan = diff_plan(&table, 1, 2).await;
    assert!(dv_attached(&plan, true));

    table
        .file_io()
        .delete_file(&format!("{}/index/diff-dv-1", table.location()))
        .await
        .unwrap();
    let result = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_incremental_arrow(&plan)
        .unwrap()
        .try_collect::<Vec<_>>()
        .await;
    assert!(
        result.is_err(),
        "a missing historical DV must not be ignored"
    );
}

fn vector_batch(table: &Table, rows: &[(i32, Option<[f32; 3]>)]) -> RecordBatch {
    use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};

    let schema = build_target_arrow_schema(table.schema().fields()).unwrap();
    let ArrowDataType::FixedSizeList(element, length) = schema.field(1).data_type() else {
        panic!("expected VECTOR to map to a fixed-size list")
    };
    let mut builder =
        FixedSizeListBuilder::new(Float32Builder::new(), *length).with_field(Arc::clone(element));
    for (_, vector) in rows {
        if let Some(vector) = vector {
            for value in vector {
                builder.values().append_value(*value);
            }
            builder.append(true);
        } else {
            for _ in 0..*length {
                builder.values().append_null();
            }
            builder.append(false);
        }
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(
                rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(builder.finish()),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn diff_compares_vectors_by_elements_and_preserves_float_semantics() {
    use paimon::spec::{FloatType, VectorType};

    let path = "memory:/incremental_diff/vector_values";
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            "embedding",
            DataType::Vector(VectorType::new(3, DataType::Float(FloatType::new())).unwrap()),
        )
        .primary_key(["id"])
        .option("bucket", "1")
        .option("merge-engine", "deduplicate")
        .build()
        .unwrap();
    let (file_io, table) = memory_table(path, TableSchema::new(0, &schema));
    setup_dirs(&file_io, path).await;
    persist_table_schema(&file_io, path, table.schema()).await;

    let before = [
        (1, Some([1.0, 2.0, 3.0])),
        (2, Some([f32::NAN, 2.0, 3.0])),
        (3, Some([-0.0, 2.0, 3.0])),
        (4, None),
        (5, Some([5.0, 5.0, 5.0])),
    ];
    let after = [
        (1, Some([1.0, 2.0, 4.0])),
        (2, Some([f32::from_bits(0xffc0_0001), 2.0, 3.0])),
        (3, Some([0.0, 2.0, 3.0])),
        (4, Some([0.0, 0.0, 0.0])),
        (5, Some([5.0, 5.0, 5.0])),
        (6, Some([6.0, 6.0, 6.0])),
    ];
    write_batch(&table, &vector_batch(&table, &before)).await;
    write_batch(&table, &vector_batch(&table, &after)).await;

    let plan = diff_plan(&table, 1, 2).await;
    assert_eq!(
        ids_at(&diff_batches(&table, &plan).await, 0),
        vec![1, 3, 4, 6]
    );
    let audit: Vec<RecordBatch> = table
        .new_read_builder()
        .new_read()
        .unwrap()
        .to_audit_log_arrow(&plan)
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut events = audit
        .iter()
        .flat_map(|batch| {
            let kinds = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let ids = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            (0..batch.num_rows())
                .map(|row| (ids.value(row), kinds.value(row).to_string()))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    events.sort();
    assert_eq!(events.len(), 7);
    assert_eq!(events.iter().filter(|(id, _)| *id == 2).count(), 0);
    assert_eq!(events.iter().filter(|(id, _)| *id == 6).count(), 1);
}
