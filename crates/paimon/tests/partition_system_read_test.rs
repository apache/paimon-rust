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

use arrow_array::{Int32Array, Int64Array, RecordBatch, StringArray};
use common::incremental_helpers::{memory_table, persist_table_schema, setup_dirs, write_batch};
use paimon::spec::{DataType, IntType, Schema, TableSchema, VarCharType};
use paimon::table::Table;
use std::sync::Arc;

async fn table() -> Table {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("pt", DataType::VarChar(VarCharType::string_type()))
        .partition_keys(["pt"])
        .option("bucket", "8")
        .option("bucket-key", "id")
        .option("deletion-vectors.enabled", "true")
        .build()
        .unwrap();
    let (io, table) = memory_table("memory:/partition_metadata", TableSchema::new(0, &schema));
    setup_dirs(&io, table.location()).await;
    persist_table_schema(&io, table.location(), table.schema()).await;
    table
}

async fn append(table: &Table, ids: Vec<i32>, partitions: Vec<Option<&str>>) {
    let batch = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int32Array::from(ids)) as arrow_array::ArrayRef,
        ),
        (
            "pt",
            Arc::new(StringArray::from(partitions)) as arrow_array::ArrayRef,
        ),
    ])
    .unwrap();
    write_batch(table, &batch).await;
}

fn strings(batch: &RecordBatch, column: &str) -> Vec<String> {
    batch
        .column_by_name(column)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|value| value.unwrap().to_string())
        .collect()
}

fn longs(batch: &RecordBatch, column: &str) -> Vec<Option<i64>> {
    batch
        .column_by_name(column)
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect()
}

#[tokio::test]
async fn partition_statistics_follow_captured_snapshots_and_declared_buckets() {
    let table = table().await;
    let read = table.new_system_read("partitions").unwrap();
    let empty = read.read().await.unwrap();
    assert_eq!(empty.num_rows(), 0);
    assert_eq!(empty.schema(), read.schema());
    let fixed_empty = table.copy_with_pinned_snapshot(None);
    append(&table, vec![1, 2, 3], vec![Some("z"), None, Some("z")]).await;
    let first = table.resolve_read_snapshot().await.unwrap().unwrap();
    let historical = table.copy_with_pinned_snapshot(Some(&first));
    append(&table, vec![4], vec![Some("a")]).await;
    let batch = read.read().await.unwrap();
    assert_eq!(
        strings(&batch, "partition"),
        ["pt=__DEFAULT_PARTITION__", "pt=a", "pt=z"]
    );
    assert_eq!(longs(&batch, "record_count"), [Some(1), Some(1), Some(2)]);
    let buckets = batch
        .column_by_name("total_buckets")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert!(buckets.iter().all(|value| value == Some(8)));
    assert_eq!(longs(&batch, "deleted_record_count"), [Some(0); 3]);
    assert_eq!(
        historical
            .new_system_read("partitions")
            .unwrap()
            .read()
            .await
            .unwrap()
            .num_rows(),
        2
    );
    assert_eq!(historical.partition_stats().await.unwrap().len(), 2);
    assert!(fixed_empty.partition_stats().await.unwrap().is_empty());
    assert!(
        paimon::catalog::list_partitions_from_file_system(&fixed_empty)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        fixed_empty
            .new_system_read("partitions")
            .unwrap()
            .read()
            .await
            .unwrap()
            .num_rows(),
        0
    );
    for partition in paimon::catalog::list_partitions_from_file_system(&table)
        .await
        .unwrap()
    {
        assert_eq!(partition.total_buckets, 8);
    }
}

#[tokio::test]
async fn deletion_count_filter_reads_hidden_column_and_projection_skips_index_io() {
    use paimon::spec::{
        DeletionVectorMeta, FileKind, IndexFileMeta, IndexManifest, IndexManifestEntry, Predicate,
        PredicateOperator,
    };
    let table = table().await;
    append(&table, vec![1, 2], vec![Some("a"), Some("b")]).await;
    let plan = table.new_read_builder().new_scan().plan().await.unwrap();
    let mut entries = Vec::new();
    for split in plan.splits() {
        let cardinality = if split.partition().get_string(0).unwrap() == "a" {
            Some(1)
        } else {
            None
        };
        entries.push(IndexManifestEntry {
            version: 1,
            kind: FileKind::Add,
            partition: split.partition().to_serialized_bytes(),
            bucket: split.bucket(),
            index_file: IndexFileMeta {
                index_type: "DELETION_VECTORS".into(),
                file_name: "unused-index-file".into(),
                file_size: 1,
                row_count: 1,
                deletion_vectors_ranges: Some(
                    [(
                        split.data_files()[0].file_name.clone(),
                        DeletionVectorMeta {
                            offset: 0,
                            length: 1,
                            cardinality,
                        },
                    )]
                    .into_iter()
                    .collect(),
                ),
                external_path: None,
                global_index_meta: None,
            },
        });
    }
    let manager = table.snapshot_manager();
    let path = manager.manifest_path("test-index-manifest");
    IndexManifest::write(table.file_io(), &path, &entries)
        .await
        .unwrap();
    let snapshot = table.resolve_read_snapshot().await.unwrap().unwrap();
    let mut json = serde_json::to_value(&snapshot).unwrap();
    json["indexManifest"] = serde_json::json!("test-index-manifest");
    table
        .file_io()
        .new_output(&manager.snapshot_path(snapshot.id()))
        .unwrap()
        .write(serde_json::to_vec(&json).unwrap().into())
        .await
        .unwrap();
    let mut read = table.new_system_read("partitions").unwrap();
    assert_eq!(
        longs(&read.read().await.unwrap(), "deleted_record_count"),
        [Some(1), None]
    );
    read.with_projection(&["partition"]).unwrap();
    read.with_filter(Predicate::Leaf {
        column: "deleted_record_count".into(),
        index: 999,
        data_type: DataType::Int(IntType::new()),
        op: PredicateOperator::IsNull,
        literals: vec![],
    })
    .unwrap();
    read.with_limit(1);
    assert_eq!(strings(&read.read().await.unwrap(), "partition"), ["pt=b"]);
    table.file_io().delete_file(&path).await.unwrap();
    let mut projected = table.new_system_read("partitions").unwrap();
    projected.with_projection(&["partition"]).unwrap();
    assert_eq!(projected.read().await.unwrap().num_rows(), 2);
    assert!(read.read().await.is_err());
}
