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

use super::extraction::extract_vectors_from_batches;
use super::planning::{plan_lumina_shards, LuminaIndexShard};
use super::validation::{
    checked_i64, effective_lumina_options, find_index_field, validate_vector_field,
};
use super::writer::{abort_on_build_error, temp_lumina_path, TempFileGuard};
use crate::catalog::Identifier;
use crate::io::FileIO;
use crate::io::FileIOBuilder;
use crate::lumina::{LUMINA_DIMENSION_OPTION, LUMINA_IDENTIFIER};
use crate::spec::stats::BinaryTableStats;
use crate::spec::{
    ArrayType, BinaryRow, CoreOptions, DataField, DataFileMeta, DataType, DoubleType, FileKind,
    FloatType, GlobalIndexMeta, IndexFileMeta, IndexManifest, IntType, ManifestEntry, Schema,
    TableSchema, VectorType, ROW_ID_FIELD_NAME,
};
use crate::table::source::exclude_row_ranges;
use crate::table::{CommitMessage, RowRange, SnapshotManager, Table, TableCommit, TableWrite};
use crate::{Error, Result};
use arrow_array::builder::{FixedSizeListBuilder, Float32Builder, Int64Builder, ListBuilder};
use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;

fn data_file(name: &str, first_row_id: Option<i64>, row_count: i64) -> DataFileMeta {
    DataFileMeta {
        file_name: name.to_string(),
        file_size: 128,
        row_count,
        min_key: vec![],
        max_key: vec![],
        key_stats: BinaryTableStats::new(vec![], vec![], vec![]),
        value_stats: BinaryTableStats::new(vec![], vec![], vec![]),
        min_sequence_number: 0,
        max_sequence_number: 0,
        schema_id: 0,
        level: 0,
        extra_files: vec![],
        creation_time: Some(
            "2024-09-06T07:45:55.039+00:00"
                .parse::<DateTime<Utc>>()
                .unwrap(),
        ),
        delete_row_count: None,
        embedded_index: None,
        first_row_id,
        write_cols: None,
        external_path: None,
        file_source: None,
        value_stats_cols: None,
        column_max_sequence_numbers: None,
    }
}

fn manifest_entry(file: DataFileMeta) -> ManifestEntry {
    manifest_entry_with_bucket(file, 0, 1)
}

fn manifest_entry_with_bucket(
    file: DataFileMeta,
    bucket: i32,
    total_buckets: i32,
) -> ManifestEntry {
    ManifestEntry::new(FileKind::Add, vec![], bucket, total_buckets, file, 2)
}

fn table_options(rows_per_shard: &str) -> HashMap<String, String> {
    HashMap::from([
        ("row-tracking.enabled".to_string(), "true".to_string()),
        ("data-evolution.enabled".to_string(), "true".to_string()),
        ("global-index.enabled".to_string(), "true".to_string()),
        (
            "global-index.row-count-per-shard".to_string(),
            rows_per_shard.to_string(),
        ),
    ])
}

fn test_table(options: HashMap<String, String>) -> Table {
    test_table_with_io(
        FileIOBuilder::new("memory").build().unwrap(),
        "memory:/test_lumina_builder",
        Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column(
                "embedding",
                DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
            )
            .options(options)
            .build()
            .unwrap(),
    )
}

fn test_table_with_schema(schema: Schema) -> Table {
    test_table_with_io(
        FileIOBuilder::new("memory").build().unwrap(),
        "memory:/test_lumina_builder",
        schema,
    )
}

fn test_table_with_io(file_io: FileIO, table_path: &str, schema: Schema) -> Table {
    Table::new(
        file_io,
        Identifier::new("default", "test_table"),
        table_path.to_string(),
        TableSchema::new(0, &schema),
        None,
    )
}

fn vector_schema_builder(options: HashMap<String, String>) -> crate::spec::SchemaBuilder {
    Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            "embedding",
            DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
        )
        .options(options)
}

fn plan(entries: Vec<ManifestEntry>, rows_per_shard: i64) -> Result<Vec<LuminaIndexShard>> {
    plan_with_indexed(entries, rows_per_shard, &[])
}

fn plan_with_indexed(
    entries: Vec<ManifestEntry>,
    rows_per_shard: i64,
    indexed: &[RowRange],
) -> Result<Vec<LuminaIndexShard>> {
    let table = test_table(table_options(&rows_per_shard.to_string()));
    let core = CoreOptions::new(table.schema().options());
    plan_lumina_shards(
        table.location(),
        table.schema().partition_keys(),
        table.schema().fields(),
        &core,
        1,
        entries,
        rows_per_shard,
        indexed,
    )
}

#[test]
fn test_planner_splits_single_file_across_shards() {
    let shards = plan(vec![manifest_entry(data_file("a", Some(0), 25))], 10).unwrap();

    assert_eq!(
        shards
            .iter()
            .map(|s| (s.row_range_start, s.row_range_end))
            .collect::<Vec<_>>(),
        vec![(0, 9), (10, 19), (20, 24)]
    );
}

#[test]
fn test_planner_merges_contiguous_files() {
    let shards = plan(
        vec![
            manifest_entry(data_file("a", Some(0), 5)),
            manifest_entry(data_file("b", Some(5), 5)),
        ],
        20,
    )
    .unwrap();

    assert_eq!(shards.len(), 1);
    assert_eq!((shards[0].row_range_start, shards[0].row_range_end), (0, 9));
    assert_eq!(
        shards[0]
            .files
            .iter()
            .map(|f| f.file_name.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b"]
    );
}

#[test]
fn test_planner_keeps_source_buckets_separate() {
    let shards = plan(
        vec![
            manifest_entry_with_bucket(data_file("a", Some(0), 5), 0, 2),
            manifest_entry_with_bucket(data_file("b", Some(5), 5), 1, 2),
        ],
        20,
    )
    .unwrap();

    assert_eq!(shards.len(), 2);
    assert_eq!(
        shards
            .iter()
            .map(|s| (
                s.source_bucket,
                s.total_buckets,
                s.row_range_start,
                s.row_range_end
            ))
            .collect::<Vec<_>>(),
        vec![(0, 2, 0, 4), (1, 2, 5, 9)]
    );
}

#[test]
fn test_planner_splits_gap_into_separate_groups() {
    let shards = plan(
        vec![
            manifest_entry(data_file("a", Some(0), 5)),
            manifest_entry(data_file("b", Some(10), 5)),
        ],
        20,
    )
    .unwrap();

    assert_eq!(
        shards
            .iter()
            .map(|s| (s.row_range_start, s.row_range_end))
            .collect::<Vec<_>>(),
        vec![(0, 4), (10, 14)]
    );
}

#[test]
fn test_planner_rejects_missing_first_row_id() {
    let err = plan(vec![manifest_entry(data_file("a", None, 5))], 10)
        .expect_err("missing first_row_id should fail");
    assert!(
        matches!(err, Error::DataInvalid { message, .. } if message.contains("missing first_row_id"))
    );
}

#[test]
fn test_planner_rejects_invalid_rows_per_shard() {
    let err = plan(vec![manifest_entry(data_file("a", Some(0), 5))], 0)
        .expect_err("invalid rows per shard should fail");
    assert!(
        matches!(err, Error::DataInvalid { message, .. } if message.contains("row-count-per-shard"))
    );
}

#[test]
fn test_validate_vector_field_accepts_array_float() {
    let field = DataField::new(
        0,
        "embedding".to_string(),
        DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
    );
    assert!(validate_vector_field(&field).is_ok());
}

#[test]
fn test_validate_vector_field_accepts_vector_float() {
    let field = DataField::new(
        0,
        "embedding".to_string(),
        DataType::Vector(VectorType::try_new(true, 4, DataType::Float(FloatType::new())).unwrap()),
    );
    assert!(validate_vector_field(&field).is_ok());
}

#[test]
fn test_validate_vector_field_rejects_vector_double() {
    let field = DataField::new(
        0,
        "embedding".to_string(),
        DataType::Vector(
            VectorType::try_new(true, 4, DataType::Double(DoubleType::new())).unwrap(),
        ),
    );
    let err = validate_vector_field(&field).expect_err("VECTOR<DOUBLE> must be rejected");
    assert!(matches!(err, Error::DataInvalid { .. }));
}

#[test]
fn test_effective_options_vector_absent_inserts_length() {
    let field = DataField::new(
        0,
        "embedding".to_string(),
        DataType::Vector(
            VectorType::try_new(true, 256, DataType::Float(FloatType::new())).unwrap(),
        ),
    );
    let opts = effective_lumina_options(&field, HashMap::new()).unwrap();
    assert_eq!(
        opts.get(LUMINA_DIMENSION_OPTION).map(String::as_str),
        Some("256")
    );
}

#[test]
fn test_effective_options_vector_matching_option_ok() {
    let field = DataField::new(
        0,
        "embedding".to_string(),
        DataType::Vector(
            VectorType::try_new(true, 256, DataType::Float(FloatType::new())).unwrap(),
        ),
    );
    let resolved = HashMap::from([(LUMINA_DIMENSION_OPTION.to_string(), "256".to_string())]);
    let opts = effective_lumina_options(&field, resolved).unwrap();
    assert_eq!(
        opts.get(LUMINA_DIMENSION_OPTION).map(String::as_str),
        Some("256")
    );
}

#[test]
fn test_effective_options_vector_mismatch_errors() {
    let field = DataField::new(
        0,
        "embedding".to_string(),
        DataType::Vector(
            VectorType::try_new(true, 256, DataType::Float(FloatType::new())).unwrap(),
        ),
    );
    let resolved = HashMap::from([(LUMINA_DIMENSION_OPTION.to_string(), "128".to_string())]);
    let err =
        effective_lumina_options(&field, resolved).expect_err("dimension mismatch must error");
    assert!(matches!(err, Error::ConfigInvalid { .. }));
}

#[test]
fn test_effective_options_array_unchanged() {
    let field = DataField::new(
        0,
        "embedding".to_string(),
        DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
    );
    // No dimension option set: array path must NOT inject one.
    let opts = effective_lumina_options(&field, HashMap::new()).unwrap();
    assert!(!opts.contains_key(LUMINA_DIMENSION_OPTION));
}

#[test]
fn test_vector_without_option_propagates_dimension_to_native_options() {
    use crate::lumina::{LuminaVectorIndexOptions, KEY_DIMENSION};
    let field = DataField::new(
        0,
        "embedding".to_string(),
        DataType::Vector(
            VectorType::try_new(true, 256, DataType::Float(FloatType::new())).unwrap(),
        ),
    );
    // No lumina.index.dimension set by the user.
    let resolved = effective_lumina_options(&field, HashMap::new()).unwrap();
    let opts = LuminaVectorIndexOptions::new(&resolved).unwrap();

    assert_eq!(
        opts.dimension, 256,
        "local dimension must be N, not default 128"
    );
    assert_eq!(
        opts.to_lumina_options()
            .get(KEY_DIMENSION)
            .map(String::as_str),
        Some("256"),
        "native index.dimension must be N, not default 128"
    );
}

#[tokio::test]
async fn test_execute_rejects_primary_key_table() {
    let table = test_table_with_schema(
        vector_schema_builder(HashMap::new())
            .primary_key(["id"])
            .build()
            .unwrap(),
    );

    let err = table
        .new_lumina_index_build_builder()
        .with_index_column("embedding")
        .execute()
        .await
        .expect_err("primary-key table should fail before native build");

    assert!(
        matches!(err, Error::Unsupported { message } if message.contains("primary-key tables"))
    );
}

#[tokio::test]
async fn test_execute_rejects_deletion_vectors_table() {
    let mut options = table_options("10");
    options.insert("deletion-vectors.enabled".to_string(), "true".to_string());
    let table = test_table(options);

    let err = table
        .new_lumina_index_build_builder()
        .with_index_column("embedding")
        .execute()
        .await
        .expect_err("deletion vectors table should fail before native build");

    assert!(
        matches!(err, Error::Unsupported { message } if message.contains("deletion-vectors.enabled=true"))
    );
}

fn vector_batch(rows: Vec<Option<Vec<Option<f32>>>>, row_ids: Vec<Option<i64>>) -> RecordBatch {
    let mut vector_builder = ListBuilder::new(Float32Builder::new());
    for row in rows {
        match row {
            Some(values) => {
                for value in values {
                    match value {
                        Some(value) => vector_builder.values().append_value(value),
                        None => vector_builder.values().append_null(),
                    }
                }
                vector_builder.append(true);
            }
            None => vector_builder.append(false),
        }
    }
    let mut row_id_builder = Int64Builder::new();
    for row_id in row_ids {
        match row_id {
            Some(value) => row_id_builder.append_value(value),
            None => row_id_builder.append_null(),
        }
    }
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new(
            "embedding",
            ArrowDataType::List(Arc::new(ArrowField::new(
                "item",
                ArrowDataType::Float32,
                true,
            ))),
            true,
        ),
        ArrowField::new(ROW_ID_FIELD_NAME, ArrowDataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(vector_builder.finish()) as ArrayRef,
            Arc::new(row_id_builder.finish()) as ArrayRef,
        ],
    )
    .unwrap()
}

#[test]
fn test_extract_vectors_accepts_list_float32_and_row_ids() {
    let batch = vector_batch(
        vec![
            Some(vec![Some(1.0), Some(2.0)]),
            Some(vec![Some(3.0), Some(4.0)]),
        ],
        vec![Some(10), Some(11)],
    );

    let vectors = extract_vectors_from_batches(&[batch], "embedding", 2, 10, 2).unwrap();

    assert_eq!(vectors, vec![1.0, 2.0, 3.0, 4.0]);
}

#[test]
fn test_extract_vectors_rejects_null_vector() {
    let batch = vector_batch(vec![None], vec![Some(0)]);

    let err = extract_vectors_from_batches(&[batch], "embedding", 2, 0, 1)
        .expect_err("null vector should fail");

    assert!(matches!(err, Error::DataInvalid { message, .. } if message.contains("null vector")));
}

#[test]
fn test_extract_vectors_rejects_null_element() {
    let batch = vector_batch(vec![Some(vec![Some(1.0), None])], vec![Some(0)]);

    let err = extract_vectors_from_batches(&[batch], "embedding", 2, 0, 1)
        .expect_err("null element should fail");

    assert!(
        matches!(err, Error::DataInvalid { message, .. } if message.contains("null vector element"))
    );
}

#[test]
fn test_extract_vectors_rejects_dimension_mismatch() {
    let batch = vector_batch(vec![Some(vec![Some(1.0)])], vec![Some(0)]);

    let err = extract_vectors_from_batches(&[batch], "embedding", 2, 0, 1)
        .expect_err("dimension mismatch should fail");

    assert!(
        matches!(err, Error::DataInvalid { message, .. } if message.contains("dimension mismatch"))
    );
}

#[test]
fn test_extract_vectors_rejects_row_id_gap() {
    let batch = vector_batch(
        vec![
            Some(vec![Some(1.0), Some(2.0)]),
            Some(vec![Some(3.0), Some(4.0)]),
        ],
        vec![Some(0), Some(2)],
    );

    let err = extract_vectors_from_batches(&[batch], "embedding", 2, 0, 2)
        .expect_err("row id gap should fail");

    assert!(
        matches!(err, Error::DataInvalid { message, .. } if message.contains("expected _ROW_ID"))
    );
}

#[test]
fn test_extract_vectors_rejects_non_list_float32() {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("embedding", ArrowDataType::Int32, false),
        ArrowField::new(ROW_ID_FIELD_NAME, ArrowDataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(0)])) as ArrayRef,
        ],
    )
    .unwrap();

    let err = extract_vectors_from_batches(&[batch], "embedding", 2, 0, 1)
        .expect_err("non-list vector should fail");

    assert!(matches!(err, Error::DataInvalid { message, .. } if message.contains("List<Float32>")));
}

fn fixed_size_vector_batch(
    rows: Vec<Option<Vec<f32>>>,
    row_ids: Vec<Option<i64>>,
    len: i32,
) -> RecordBatch {
    let element_field = Arc::new(ArrowField::new("element", ArrowDataType::Float32, true));
    let mut builder =
        FixedSizeListBuilder::new(Float32Builder::new(), len).with_field(element_field);
    for row in rows {
        match row {
            Some(values) => {
                for v in values {
                    builder.values().append_value(v);
                }
                builder.append(true);
            }
            None => {
                for _ in 0..len {
                    builder.values().append_value(0.0);
                }
                builder.append(false);
            }
        }
    }
    let mut row_id_builder = Int64Builder::new();
    for row_id in row_ids {
        match row_id {
            Some(value) => row_id_builder.append_value(value),
            None => row_id_builder.append_null(),
        }
    }
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new(
            "embedding",
            ArrowDataType::FixedSizeList(
                Arc::new(ArrowField::new("element", ArrowDataType::Float32, true)),
                len,
            ),
            true,
        ),
        ArrowField::new(ROW_ID_FIELD_NAME, ArrowDataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(builder.finish()) as ArrayRef,
            Arc::new(row_id_builder.finish()) as ArrayRef,
        ],
    )
    .unwrap()
}

#[test]
fn test_extract_vectors_accepts_fixed_size_list_float32() {
    let batch = fixed_size_vector_batch(
        vec![Some(vec![1.0, 2.0]), Some(vec![3.0, 4.0])],
        vec![Some(10), Some(11)],
        2,
    );
    let vectors = extract_vectors_from_batches(&[batch], "embedding", 2, 10, 2).unwrap();
    assert_eq!(vectors, vec![1.0, 2.0, 3.0, 4.0]);
}

#[test]
fn test_extract_vectors_fixed_size_list_rejects_null_vector() {
    let batch = fixed_size_vector_batch(vec![None], vec![Some(0)], 2);
    let err = extract_vectors_from_batches(&[batch], "embedding", 2, 0, 1)
        .expect_err("null vector should fail");
    assert!(matches!(err, Error::DataInvalid { message, .. } if message.contains("null vector")));
}

#[test]
fn test_extract_vectors_fixed_size_list_dimension_mismatch() {
    // Column is FixedSizeList of length 3, but caller expects dimension 2.
    let batch = fixed_size_vector_batch(vec![Some(vec![1.0, 2.0, 3.0])], vec![Some(0)], 3);
    let err = extract_vectors_from_batches(&[batch], "embedding", 2, 0, 1)
        .expect_err("dimension mismatch should fail");
    assert!(
        matches!(err, Error::DataInvalid { message, .. } if message.contains("dimension mismatch"))
    );
}

#[test]
fn test_extract_vectors_fixed_size_list_rejects_null_element() {
    // A non-null vector row whose second child element is null. Mirrors the
    // List path's test_extract_vectors_rejects_null_element so both layouts
    // reject null elements identically.
    let element_field = Arc::new(ArrowField::new("element", ArrowDataType::Float32, true));
    let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), 2).with_field(element_field);
    builder.values().append_value(1.0);
    builder.values().append_null();
    builder.append(true);
    let row_ids = Arc::new(Int64Array::from(vec![Some(0)])) as ArrayRef;
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new(
            "embedding",
            ArrowDataType::FixedSizeList(
                Arc::new(ArrowField::new("element", ArrowDataType::Float32, true)),
                2,
            ),
            true,
        ),
        ArrowField::new(ROW_ID_FIELD_NAME, ArrowDataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(builder.finish()) as ArrayRef, row_ids],
    )
    .unwrap();

    let err = extract_vectors_from_batches(&[batch], "embedding", 2, 0, 1)
        .expect_err("null element should fail");

    assert!(
        matches!(err, Error::DataInvalid { message, .. } if message.contains("null vector element"))
    );
}

#[test]
fn test_checked_metadata_conversion_supports_i64_file_size() {
    let above_i32_max = i32::MAX as u64 + 1;
    assert_eq!(
        checked_i64(above_i32_max, "Index file is too large").unwrap(),
        i64::from(i32::MAX) + 1
    );

    let err = checked_i64(i64::MAX as u64 + 1, "Index file is too large")
        .expect_err("file size above i64::MAX should fail");
    assert!(matches!(err, Error::DataInvalid { message, .. } if message.contains("too large")));
}

#[test]
fn test_temp_file_guard_cleans_up_on_drop() {
    let path = temp_lumina_path();
    std::fs::write(&path, b"temporary lumina data").unwrap();
    {
        let _guard = TempFileGuard::new(path.clone());
        assert!(path.exists());
    }
    assert!(!path.exists());
}

#[tokio::test]
async fn test_abort_on_build_error_removes_completed_index_files() {
    let file_io = FileIOBuilder::new("memory").build().unwrap();
    let table_path = "memory:/test_lumina_abort_cleanup";
    let table = test_table_with_io(
        file_io.clone(),
        table_path,
        vector_schema_builder(table_options("1")).build().unwrap(),
    );
    file_io
        .mkdirs(&format!("{table_path}/index/"))
        .await
        .unwrap();

    let file_name = "completed-lumina-shard.index";
    let index_path = format!("{table_path}/index/{file_name}");
    file_io
        .new_output(&index_path)
        .unwrap()
        .write(Bytes::from_static(b"completed shard"))
        .await
        .unwrap();
    let mut message = CommitMessage::new(vec![], 0, vec![]);
    message.new_index_files = vec![IndexFileMeta {
        index_type: LUMINA_IDENTIFIER.to_string(),
        file_name: file_name.to_string(),
        file_size: 15,
        row_count: 1,
        deletion_vectors_ranges: None,
        global_index_meta: None,
    }];

    let commit = TableCommit::new(table, "test-lumina-abort".to_string());
    let result = abort_on_build_error::<()>(
        &commit,
        &[message],
        Err(Error::UnexpectedError {
            message: "second shard failed".to_string(),
            source: None,
        }),
    )
    .await;

    assert!(result.is_err());
    assert!(file_io.get_status(&index_path).await.is_err());
}

async fn setup_dirs(file_io: &FileIO, table_path: &str) {
    file_io
        .mkdirs(&format!("{table_path}/snapshot/"))
        .await
        .unwrap();
    file_io
        .mkdirs(&format!("{table_path}/manifest/"))
        .await
        .unwrap();
}

fn build_vector_batch(ids: Vec<i32>, vectors: Vec<Vec<f32>>) -> RecordBatch {
    let element_field = Arc::new(ArrowField::new("element", ArrowDataType::Float32, true));
    let mut vector_builder =
        ListBuilder::new(Float32Builder::new()).with_field(element_field.clone());
    for vector in vectors {
        for value in vector {
            vector_builder.values().append_value(value);
        }
        vector_builder.append(true);
    }
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("embedding", ArrowDataType::List(element_field), true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)) as ArrayRef,
            Arc::new(vector_builder.finish()) as ArrayRef,
        ],
    )
    .unwrap()
}

// Manual run with a local Lumina native library:
// LUMINA_LIB_PATH=/path/to/liblumina_py.so cargo test -p paimon \
//     table::lumina_index_build_builder::tests::test_execute_writes_lumina_index_manifest \
//     --features fulltext,vortex -- --ignored --exact
#[tokio::test]
#[ignore = "requires LUMINA_LIB_PATH; see manual run command above"]
async fn test_execute_writes_lumina_index_manifest() {
    let file_io = FileIOBuilder::new("memory").build().unwrap();
    let table_path = "memory:/test_lumina_builder_e2e";
    setup_dirs(&file_io, table_path).await;

    let mut options = table_options("10");
    options.insert("lumina.index.dimension".to_string(), "2".to_string());
    options.insert("lumina.encoding.type".to_string(), "rawf32".to_string());
    let table = test_table_with_io(
        file_io.clone(),
        table_path,
        vector_schema_builder(options).build().unwrap(),
    );

    let mut table_write = TableWrite::new(&table, "test-user".to_string()).unwrap();
    table_write
        .write_arrow_batch(&build_vector_batch(
            vec![1, 2],
            vec![vec![1.0, 0.0], vec![0.0, 1.0]],
        ))
        .await
        .unwrap();
    let messages = table_write.prepare_commit().await.unwrap();
    TableCommit::new(table.clone(), "test-user".to_string())
        .commit(messages)
        .await
        .unwrap();

    let shard_count = table
        .new_lumina_index_build_builder()
        .with_index_column("embedding")
        .execute()
        .await
        .unwrap();
    assert_eq!(shard_count, 1);

    let snapshot_manager = SnapshotManager::new(file_io.clone(), table_path.to_string());
    let snapshot = snapshot_manager
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.id(), 2);
    assert_eq!(snapshot.total_record_count(), Some(2));
    assert_eq!(snapshot.delta_record_count(), Some(0));
    assert_eq!(snapshot.next_row_id(), Some(2));

    let index_manifest = snapshot.index_manifest().expect("index manifest");
    let index_entries =
        IndexManifest::read(&file_io, &format!("{table_path}/manifest/{index_manifest}"))
            .await
            .unwrap();
    assert_eq!(index_entries.len(), 1);

    let index_file = &index_entries[0].index_file;
    assert_eq!(index_file.index_type, LUMINA_IDENTIFIER);
    assert!(index_file.file_name.starts_with("lumina-global-index-"));
    assert_eq!(index_file.row_count, 2);
    assert!(index_file.file_size > 0);

    let global_meta = index_file
        .global_index_meta
        .as_ref()
        .expect("global index meta");
    assert_eq!(global_meta.row_range_start, 0);
    assert_eq!(global_meta.row_range_end, 1);
    assert_eq!(global_meta.index_field_id, 1);
    assert!(global_meta
        .index_meta
        .as_ref()
        .is_some_and(|m| !m.is_empty()));

    let index_path = format!("{table_path}/index/{}", index_file.file_name);
    let status = file_io.get_status(&index_path).await.unwrap();
    assert_eq!(index_file.file_size as u64, status.size);
}

fn lumina_e2e_options(rows_per_shard: &str) -> HashMap<String, String> {
    let mut options = table_options(rows_per_shard);
    options.insert("lumina.index.dimension".to_string(), "2".to_string());
    options.insert("lumina.encoding.type".to_string(), "rawf32".to_string());
    options
}

fn lumina_e2e_table(table_path: &str, rows_per_shard: &str) -> Table {
    test_table_with_io(
        FileIOBuilder::new("memory").build().unwrap(),
        table_path,
        vector_schema_builder(lumina_e2e_options(rows_per_shard))
            .build()
            .unwrap(),
    )
}

async fn write_vectors(table: &Table, ids: Vec<i32>, vectors: Vec<Vec<f32>>) {
    let mut table_write = TableWrite::new(table, "test-user".to_string()).unwrap();
    table_write
        .write_arrow_batch(&build_vector_batch(ids, vectors))
        .await
        .unwrap();
    let messages = table_write.prepare_commit().await.unwrap();
    TableCommit::new(table.clone(), "test-user".to_string())
        .commit(messages)
        .await
        .unwrap();
}

/// Commit a synthetic Lumina `IndexFileMeta` covering `[start, end]` for
/// `field_id` directly into the index manifest, without invoking the native
/// builder. Mirrors the btree mid-hole test so the incremental gap logic can
/// be exercised in CI where the native Lumina library is unavailable.
async fn commit_synthetic_lumina_index(table: &Table, field_id: i32, start: i64, end: i64) {
    let synthetic = IndexFileMeta {
        index_type: LUMINA_IDENTIFIER.to_string(),
        file_name: format!("lumina-synthetic-{start}-{end}.index"),
        file_size: 1,
        row_count: (end - start + 1),
        deletion_vectors_ranges: None,
        global_index_meta: Some(GlobalIndexMeta {
            row_range_start: start,
            row_range_end: end,
            index_field_id: field_id,
            extra_field_ids: None,
            source_meta: None,
            index_meta: None,
        }),
    };
    let mut message = CommitMessage::new(BinaryRow::new(0).to_serialized_bytes(), 0, vec![]);
    message.new_index_files = vec![synthetic];
    TableCommit::new(table.clone(), "test-user".to_string())
        .commit(vec![message])
        .await
        .unwrap();
}

async fn latest_lumina_index_files(table: &Table) -> Vec<IndexFileMeta> {
    let snapshot_manager =
        SnapshotManager::new(table.file_io().clone(), table.location().to_string());
    let snapshot = snapshot_manager
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    let Some(index_manifest_name) = snapshot.index_manifest() else {
        return Vec::new();
    };
    IndexManifest::read(
        table.file_io(),
        &snapshot_manager.manifest_path(index_manifest_name),
    )
    .await
    .unwrap()
    .into_iter()
    .filter(|entry| entry.kind == FileKind::Add && entry.index_file.index_type == LUMINA_IDENTIFIER)
    .map(|entry| entry.index_file)
    .collect()
}

/// Row-id coverage of the committed data files, read back from the data
/// manifest (never hard-coded) and merged into contiguous ranges. Mirrors
/// how `execute` gathers `manifest_entries`.
async fn data_row_id_coverage(table: &Table) -> Vec<RowRange> {
    let snapshot_manager =
        SnapshotManager::new(table.file_io().clone(), table.location().to_string());
    let snapshot = snapshot_manager
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    let entries = table
        .new_read_builder()
        .new_scan()
        .with_scan_all_files()
        .plan_manifest_entries(&snapshot)
        .await
        .unwrap();
    let ranges = entries
        .iter()
        .filter(|entry| *entry.kind() == FileKind::Add)
        .filter_map(|entry| {
            entry
                .file()
                .row_id_range()
                .map(|(start, end)| RowRange::new(start, end))
        })
        .collect::<Vec<_>>();
    crate::table::merge_row_ranges(ranges)
}

/// Second build with the whole coverage already indexed must be a clean
/// no-op (returns 0), not an overlap error. Reaches `Ok(0)` before the
/// native build, so it runs in CI without the Lumina library. This is the
/// core bug fix: today the second call errors with the overlap message.
#[tokio::test]
async fn lumina_second_build_without_new_data_is_noop() {
    let table_path = "memory:/test_lumina_second_build_noop";
    let table = lumina_e2e_table(table_path, "10");
    setup_dirs(table.file_io(), table_path).await;

    write_vectors(
        &table,
        vec![1, 2, 3],
        vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![1.0, 1.0]],
    )
    .await;

    // Fully index the coverage via a synthetic manifest entry.
    let coverage = data_row_id_coverage(&table).await;
    assert_eq!(coverage.len(), 1, "data must be one contiguous range");
    let field_id = find_index_field(&table, "embedding").unwrap().id();
    commit_synthetic_lumina_index(&table, field_id, coverage[0].from(), coverage[0].to()).await;

    let names_before = latest_lumina_index_files(&table)
        .await
        .iter()
        .map(|f| f.file_name.clone())
        .collect::<std::collections::BTreeSet<_>>();
    assert!(!names_before.is_empty());

    let built = table
        .new_lumina_index_build_builder()
        .with_index_column("embedding")
        .execute()
        .await
        .unwrap();
    assert_eq!(built, 0, "fully-indexed table must build nothing on re-run");

    let names_after = latest_lumina_index_files(&table)
        .await
        .iter()
        .map(|f| f.file_name.clone())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        names_before, names_after,
        "re-run must not add or remove index manifest entries"
    );
}

/// Build over an already-indexed prefix, then append new rows: the second
/// build must target only the appended gap and must NOT fail with the
/// overlap error. Without the native Lumina library the gap build surfaces a
/// library-load error (not the overlap error); with it present it succeeds.
#[tokio::test]
async fn lumina_incremental_build_indexes_only_new_rows() {
    let table_path = "memory:/test_lumina_incremental";
    let table = lumina_e2e_table(table_path, "10");
    setup_dirs(table.file_io(), table_path).await;

    // Initial batch, then mark it fully indexed via a synthetic entry.
    write_vectors(
        &table,
        vec![1, 2, 3],
        vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![1.0, 1.0]],
    )
    .await;
    let indexed_coverage = data_row_id_coverage(&table).await;
    assert_eq!(indexed_coverage.len(), 1);
    let n = indexed_coverage[0].to() + 1;
    let field_id = find_index_field(&table, "embedding").unwrap().id();
    commit_synthetic_lumina_index(
        &table,
        field_id,
        indexed_coverage[0].from(),
        indexed_coverage[0].to(),
    )
    .await;

    // Append a second batch (new row-ids [n..]).
    write_vectors(
        &table,
        vec![4, 5, 6],
        vec![vec![2.0, 0.0], vec![0.0, 2.0], vec![2.0, 2.0]],
    )
    .await;

    // White-box: fed the real indexed ranges from the manifest, the planner
    // must target only the appended gap [n, ..], never the already-indexed
    // prefix. Computed before `execute` so it is independent of whether the
    // native build (which needs the Lumina library) runs.
    let snapshot_manager =
        SnapshotManager::new(table.file_io().clone(), table.location().to_string());
    let snapshot = snapshot_manager
        .get_latest_snapshot()
        .await
        .unwrap()
        .unwrap();
    let manifest_entries = table
        .new_read_builder()
        .new_scan()
        .with_scan_all_files()
        .plan_manifest_entries(&snapshot)
        .await
        .unwrap();
    let indexed = crate::table::global_index_build_common::indexed_row_ranges(
        &table,
        snapshot.index_manifest(),
        LUMINA_IDENTIFIER,
        field_id,
        None,
    )
    .await
    .unwrap();
    let core = CoreOptions::new(table.schema().options());
    let shards = plan_lumina_shards(
        table.location(),
        table.schema().partition_keys(),
        table.schema().fields(),
        &core,
        snapshot.id(),
        manifest_entries,
        10,
        &indexed,
    )
    .unwrap();
    assert!(!shards.is_empty(), "appended gap must produce build shards");
    for shard in &shards {
        assert!(
            shard.row_range_start >= n,
            "shard [{}, {}] must start at or after the indexed prefix end {n}",
            shard.row_range_start,
            shard.row_range_end
        );
    }

    // End-to-end: the incremental build must no longer fail with the overlap
    // error. Without the native Lumina library the gap build surfaces a
    // library-load error instead; with it present it succeeds.
    let result = table
        .new_lumina_index_build_builder()
        .with_index_column("embedding")
        .execute()
        .await;
    match result {
        Ok(_) => {}
        Err(Error::DataInvalid { message, .. }) => {
            assert!(
                !message.contains("overlaps requested row range"),
                "incremental build must not fail with the overlap error; got: {message}"
            );
        }
        Err(other) => panic!("unexpected error from incremental build: {other:?}"),
    }
}

/// Regression: a first build (no existing index) must equal the pre-change
/// full build -- subtracting an empty `indexed` yields full coverage.
#[test]
fn lumina_first_build_indexes_full_coverage() {
    let full = plan(vec![manifest_entry(data_file("a", Some(0), 25))], 10).unwrap();
    let gapped =
        plan_with_indexed(vec![manifest_entry(data_file("a", Some(0), 25))], 10, &[]).unwrap();
    // Empty `indexed` must not alter the shard layout.
    assert_eq!(
        full.iter()
            .map(|s| (s.row_range_start, s.row_range_end))
            .collect::<Vec<_>>(),
        gapped
            .iter()
            .map(|s| (s.row_range_start, s.row_range_end))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        full.iter()
            .map(|s| (s.row_range_start, s.row_range_end))
            .collect::<Vec<_>>(),
        vec![(0, 9), (10, 19), (20, 24)],
        "first build must cover the full row range across shards"
    );
}

/// Planner-level mid-coverage hole, mirroring btree's
/// `incremental_build_splits_gap_around_mid_coverage_indexed_hole`: with a
/// single shard cell (rows_per_shard large enough to hold all data) the grid
/// never splits, so the only split is the indexed hole itself. An indexed
/// range strictly inside the data coverage must carve the build into exactly
/// the two contiguous segments on either side of the hole -- both bounds
/// pinned, and neither segment may span or touch the hole.
#[test]
fn lumina_plan_splits_gap_around_mid_coverage_indexed_hole() {
    // Data row-ids [0, 9]; one shard cell [0, 99] so the grid never splits.
    let n = 9;
    let hole_start = 4;
    let hole_end = 6;
    let shards = plan_with_indexed(
        vec![manifest_entry(data_file("a", Some(0), n + 1))],
        100,
        &[RowRange::new(hole_start, hole_end)],
    )
    .unwrap();

    let ranges = shards
        .iter()
        .map(|s| (s.row_range_start, s.row_range_end))
        .collect::<Vec<_>>();
    // Exactly the two contiguous segments around the hole.
    assert_eq!(
        ranges,
        vec![(0, hole_start - 1), (hole_end + 1, n)],
        "mid-coverage hole must split into exactly the two segments around it"
    );
    // Every emitted range is contiguous and none spans or touches the hole.
    for (start, end) in &ranges {
        assert!(end >= start, "range must be non-empty: [{start}, {end}]");
        assert!(
            *end < hole_start || *start > hole_end,
            "shard [{start}, {end}] must not overlap indexed hole [{hole_start}, {hole_end}]"
        );
    }
    // Together the shards cover exactly coverage - indexed.
    let expected = exclude_row_ranges(
        &[RowRange::new(0, n)],
        &[RowRange::new(hole_start, hole_end)],
    )
    .into_iter()
    .map(|r| (r.from(), r.to()))
    .collect::<Vec<_>>();
    assert_eq!(
        ranges, expected,
        "shards must cover exactly coverage minus the indexed hole"
    );
}

/// Planner-level incremental prefix. Strengthens
/// `lumina_incremental_build_indexes_only_new_rows`, which asserted only a
/// one-sided lower bound (`row_range_start >= n`): an indexed prefix [0, k]
/// must leave EXACTLY the suffix [k+1, N] on both bounds, split along the
/// shard grid, with nothing re-indexed inside the prefix.
#[test]
fn lumina_plan_incremental_prefix_leaves_suffix() {
    // Data row-ids [0, 24], rows_per_shard = 10 -> cells [0,9],[10,19],[20,29].
    // Indexed prefix [0, 9] fully fills the first cell, so the build must be
    // exactly [10, 19] and [20, 24] (the suffix split along the grid).
    let n = 24;
    let k = 9; // prefix [0, k] == the first full shard cell
    let shards = plan_with_indexed(
        vec![manifest_entry(data_file("a", Some(0), n + 1))],
        10,
        &[RowRange::new(0, k)],
    )
    .unwrap();

    let ranges = shards
        .iter()
        .map(|s| (s.row_range_start, s.row_range_end))
        .collect::<Vec<_>>();
    assert_eq!(
        ranges,
        vec![(k + 1, 19), (20, n)],
        "indexed prefix must leave exactly the suffix, split along the shard grid"
    );
    // Both bounds pinned (this is what the one-sided existing check omits).
    assert_eq!(ranges.first().unwrap().0, k + 1, "suffix must start at k+1");
    assert_eq!(ranges.last().unwrap().1, n, "suffix must end at N");
    // Contiguous, and no shard reaches back into the indexed prefix.
    for pair in ranges.windows(2) {
        assert_eq!(
            pair[1].0,
            pair[0].1 + 1,
            "ranges must be contiguous: {:?} then {:?}",
            pair[0],
            pair[1]
        );
    }
    for (start, end) in &ranges {
        assert!(
            *start > k,
            "shard [{start}, {end}] must not re-index the prefix [0, {k}]"
        );
    }
}
