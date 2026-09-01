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

use super::extraction::validate_vector_batch;
use super::planning::{plan_vindex_shards, VindexIndexShard};
use super::validation::{
    checked_training_sample_index, checked_training_vector_count, checked_vector_bytes,
    find_index_field, validate_vector_field,
};

const INDEX_DIR: &str = "index";
use crate::catalog::Identifier;
use crate::io::{FileIO, FileIOBuilder};
use crate::spec::stats::BinaryTableStats;
use crate::spec::{
    ArrayType, BinaryRow, CoreOptions, DataField, DataFileMeta, DataType, FileKind, FloatType,
    GlobalIndexMeta, IndexFileMeta, IndexManifest, IntType, ManifestEntry, Schema, TableSchema,
    ROW_ID_FIELD_NAME,
};
use crate::table::source::exclude_row_ranges;
use crate::table::{CommitMessage, RowRange, SnapshotManager, Table, TableCommit, TableWrite};
use crate::vindex::IVF_FLAT_IDENTIFIER;
use crate::{Error, Result};
use arrow_array::builder::{FixedSizeListBuilder, Float32Builder, Int64Builder, ListBuilder};
use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
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
    ManifestEntry::new(FileKind::Add, vec![], 0, 1, file, 2)
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
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            "embedding",
            DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
        )
        .options(options)
        .build()
        .unwrap();
    Table::new(
        FileIOBuilder::new("memory").build().unwrap(),
        Identifier::new("default", "test_table"),
        "memory:/test_vindex_builder".to_string(),
        TableSchema::new(0, &schema),
        None,
    )
}

fn plan(entries: Vec<ManifestEntry>, rows_per_shard: i64) -> Result<Vec<VindexIndexShard>> {
    plan_with_indexed(entries, rows_per_shard, &[])
}

fn plan_with_indexed(
    entries: Vec<ManifestEntry>,
    rows_per_shard: i64,
    indexed: &[RowRange],
) -> Result<Vec<VindexIndexShard>> {
    let table = test_table(table_options(&rows_per_shard.to_string()));
    let core = CoreOptions::new(table.schema().options());
    plan_vindex_shards(
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
fn test_planner_rejects_missing_first_row_id() {
    let err = plan(vec![manifest_entry(data_file("a", None, 5))], 10)
        .expect_err("missing first_row_id should fail");
    assert!(
        matches!(err, Error::DataInvalid { message, .. } if message.contains("missing first_row_id"))
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

fn extract_vectors_from_batches(
    batches: &[RecordBatch],
    index_column: &str,
    dimension: i32,
    row_range_start: i64,
    expected_row_count: i64,
) -> Result<Vec<f32>> {
    let dimension = usize::try_from(dimension).map_err(|e| Error::DataInvalid {
        message: format!("Invalid vindex dimension: {dimension}"),
        source: Some(Box::new(e)),
    })?;
    let mut expected_row_id = row_range_start;
    let mut vectors = Vec::new();
    for batch in batches {
        vectors.extend_from_slice(
            validate_vector_batch(batch, index_column, dimension, &mut expected_row_id)?.values,
        );
    }
    if expected_row_id - row_range_start != expected_row_count {
        return Err(Error::DataInvalid {
            message: format!(
                "vindex vector extraction expected {expected_row_count} rows, got {}",
                expected_row_id - row_range_start
            ),
            source: None,
        });
    }
    Ok(vectors)
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
fn test_extract_vectors_rejects_dimension_mismatch() {
    let batch = vector_batch(vec![Some(vec![Some(1.0)])], vec![Some(0)]);

    let err = extract_vectors_from_batches(&[batch], "embedding", 2, 0, 1)
        .expect_err("dimension mismatch should fail");

    assert!(
        matches!(err, Error::DataInvalid { message, .. } if message.contains("dimension mismatch"))
    );
}

#[test]
fn test_extract_vectors_handles_sliced_list_offsets() {
    let batch = vector_batch(
        vec![
            Some(vec![None, Some(0.0)]),
            Some(vec![Some(1.0), Some(2.0)]),
            Some(vec![Some(3.0), Some(4.0)]),
        ],
        vec![Some(9), Some(10), Some(11)],
    )
    .slice(1, 2);

    let vectors = extract_vectors_from_batches(&[batch], "embedding", 2, 10, 2).unwrap();

    assert_eq!(vectors, vec![1.0, 2.0, 3.0, 4.0]);
}

#[test]
fn test_extract_vectors_handles_sliced_fixed_size_list() {
    let mut vectors = FixedSizeListBuilder::new(Float32Builder::new(), 2);
    for row in [[0.0, 0.0], [1.0, 2.0], [3.0, 4.0]] {
        vectors.values().append_slice(&row);
        vectors.append(true);
    }
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new(
            "embedding",
            ArrowDataType::FixedSizeList(
                Arc::new(ArrowField::new("item", ArrowDataType::Float32, true)),
                2,
            ),
            true,
        ),
        ArrowField::new(ROW_ID_FIELD_NAME, ArrowDataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(vectors.finish()) as ArrayRef,
            Arc::new(Int64Array::from(vec![9, 10, 11])) as ArrayRef,
        ],
    )
    .unwrap()
    .slice(1, 2);

    let vectors = extract_vectors_from_batches(&[batch], "embedding", 2, 10, 2).unwrap();

    assert_eq!(vectors, vec![1.0, 2.0, 3.0, 4.0]);
}

#[test]
fn test_training_sample_count_and_indexes_match_java() {
    assert_eq!(checked_training_vector_count(10, 0.01).unwrap(), 1);
    assert_eq!(checked_training_vector_count(10, 0.25).unwrap(), 3);
    assert_eq!(checked_training_vector_count(10, 1.0).unwrap(), 10);
    assert_eq!(checked_training_vector_count(3, 0.9).unwrap(), 3);
    assert_eq!(
        (0..4)
            .map(|sample| checked_training_sample_index(sample, 10, 4).unwrap())
            .collect::<Vec<_>>(),
        vec![0, 2, 5, 7]
    );
    assert!(checked_vector_bytes(usize::MAX, 2).is_err());
    assert!(checked_training_sample_index(usize::MAX, usize::MAX, 1).is_err());
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

fn vindex_schema_builder(options: HashMap<String, String>) -> crate::spec::SchemaBuilder {
    Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            "embedding",
            DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
        )
        .options(options)
}

fn vindex_e2e_options(rows_per_shard: &str) -> HashMap<String, String> {
    let mut options = table_options(rows_per_shard);
    // A small, valid IVF config so the (optional) native build can run; the
    // no-op/incremental fix is exercised before or independently of it.
    options.insert("ivf-flat.dimension".to_string(), "2".to_string());
    options.insert("ivf-flat.nlist".to_string(), "2".to_string());
    options
}

fn vindex_e2e_table(table_path: &str, rows_per_shard: &str) -> Table {
    test_table_with_io(
        FileIOBuilder::new("memory").build().unwrap(),
        table_path,
        vindex_schema_builder(vindex_e2e_options(rows_per_shard))
            .build()
            .unwrap(),
    )
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

/// Commit a synthetic vindex `IndexFileMeta` covering `[start, end]` for
/// `field_id` directly into the index manifest, without invoking the native
/// builder. Mirrors the Lumina/btree tests so the incremental gap logic can
/// be exercised without a trained vector index. Writes the same `index_type`
/// (`ivf-flat`) the builder-under-test uses, so the gap helper matches it.
async fn commit_synthetic_vindex_index(table: &Table, field_id: i32, start: i64, end: i64) {
    let synthetic = IndexFileMeta {
        index_type: IVF_FLAT_IDENTIFIER.to_string(),
        file_name: format!("vector-ivf-flat-synthetic-{start}-{end}.index"),
        file_size: 1,
        row_count: end - start + 1,
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

async fn latest_vindex_index_files(table: &Table) -> Vec<IndexFileMeta> {
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
    .filter(|entry| {
        entry.kind == FileKind::Add && entry.index_file.index_type == IVF_FLAT_IDENTIFIER
    })
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
/// native build, so it runs in CI without a trained index. This is the core
/// bug fix: today the second call errors with the overlap message.
#[tokio::test]
async fn vindex_second_build_without_new_data_is_noop() {
    let table_path = "memory:/test_vindex_second_build_noop";
    let table = vindex_e2e_table(table_path, "10");
    setup_dirs(table.file_io(), table_path).await;

    write_vectors(&table, vec![1, 2], vec![vec![1.0, 0.0], vec![0.0, 1.0]]).await;
    write_vectors(&table, vec![3], vec![vec![1.0, 1.0]]).await;

    // Fully index the coverage via a synthetic manifest entry.
    let coverage = data_row_id_coverage(&table).await;
    assert_eq!(coverage.len(), 1, "data must be one contiguous range");
    let field_id = find_index_field(&table, "embedding").unwrap().id();
    commit_synthetic_vindex_index(&table, field_id, coverage[0].from(), coverage[0].to()).await;

    let names_before = latest_vindex_index_files(&table)
        .await
        .iter()
        .map(|f| f.file_name.clone())
        .collect::<std::collections::BTreeSet<_>>();
    assert!(!names_before.is_empty());

    let built = table
        .new_vindex_index_build_builder(IVF_FLAT_IDENTIFIER)
        .with_index_column("embedding")
        .execute()
        .await
        .unwrap();
    assert_eq!(built, 0, "fully-indexed table must build nothing on re-run");

    let names_after = latest_vindex_index_files(&table)
        .await
        .iter()
        .map(|f| f.file_name.clone())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        names_before, names_after,
        "re-run must not add or remove index manifest entries"
    );
}

/// Real end-to-end incremental build. `paimon-vindex-core` is pure Rust and
/// trains/serializes an IVF-flat index in CI without a native lib, so this
/// asserts SUCCESS end-to-end (mirroring btree's incremental test): build #1
/// indexes the initial rows, an appended batch is indexed by build #2, every
/// new index file's row range lies entirely in the appended gap `[n, ..]`
/// (`n` derived from the manifest, never hard-coded), and build-#1's index
/// files are retained untouched (append-only). No overlap error, no tolerated
/// native-build failure -- the build must actually succeed.
#[tokio::test]
async fn vindex_incremental_build_indexes_only_new_rows() {
    let table_path = "memory:/test_vindex_incremental";
    let table = vindex_e2e_table(table_path, "10");
    setup_dirs(table.file_io(), table_path).await;

    // Build #1 over the initial batch via a real end-to-end build.
    write_vectors(
        &table,
        vec![1, 2, 3],
        vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![1.0, 1.0]],
    )
    .await;
    let first_built = table
        .new_vindex_index_build_builder(IVF_FLAT_IDENTIFIER)
        .with_index_column("embedding")
        .with_options(HashMap::from([(
            "ivf-flat.train.sample-ratio".to_string(),
            "0.9".to_string(),
        )]))
        .execute()
        .await
        .unwrap();
    assert!(first_built > 0, "first build must index the initial rows");

    // First appended row-id, derived from the data manifest (never hard-coded).
    let indexed_coverage = data_row_id_coverage(&table).await;
    assert_eq!(indexed_coverage.len(), 1);
    let n = indexed_coverage[0].to() + 1;

    let first_names = latest_vindex_index_files(&table)
        .await
        .iter()
        .map(|f| f.file_name.clone())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(first_names.len(), 1, "one shard must write one index file");

    // Append a second batch (new row-ids [n..]).
    write_vectors(
        &table,
        vec![4, 5, 6],
        vec![vec![2.0, 0.0], vec![0.0, 2.0], vec![2.0, 2.0]],
    )
    .await;

    // End-to-end: build #2 must SUCCEED and index the appended rows.
    let second_built = table
        .new_vindex_index_build_builder(IVF_FLAT_IDENTIFIER)
        .with_index_column("embedding")
        .execute()
        .await
        .unwrap();
    assert!(second_built > 0, "appended rows must be indexed");

    let all_files = latest_vindex_index_files(&table).await;
    let all_names = all_files
        .iter()
        .map(|f| f.file_name.clone())
        .collect::<std::collections::BTreeSet<_>>();

    // Every build-#1 file is still present (append-only, no rewrite/delete).
    assert!(
        first_names.iter().all(|name| all_names.contains(name)),
        "build #1 index files must be retained untouched"
    );

    // Every build-#2 file covers only the appended gap [n, ..], never the
    // already-indexed prefix.
    let new_files = all_files
        .iter()
        .filter(|f| !first_names.contains(&f.file_name))
        .collect::<Vec<_>>();
    assert!(!new_files.is_empty(), "build #2 must add new index files");
    for file in new_files {
        let meta = file
            .global_index_meta
            .as_ref()
            .expect("global index meta on new vindex file");
        assert!(
            meta.row_range_start >= n,
            "new index file range must start at or after {n}, got [{}, {}]",
            meta.row_range_start,
            meta.row_range_end
        );
    }
}

#[tokio::test]
async fn vindex_build_cleans_written_shards_when_later_shard_fails() {
    let table_path = "memory:/test_vindex_abort_written_shard";
    let table = vindex_e2e_table(table_path, "2");
    setup_dirs(table.file_io(), table_path).await;
    write_vectors(
        &table,
        vec![1, 2, 3],
        vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![1.0]],
    )
    .await;

    let error = table
        .new_vindex_index_build_builder(IVF_FLAT_IDENTIFIER)
        .with_index_column("embedding")
        .execute()
        .await
        .expect_err("the second shard has an invalid vector dimension");

    assert!(error.to_string().contains("dimension mismatch"));
    assert!(table
        .file_io()
        .list_status(&format!("{table_path}/{INDEX_DIR}/"))
        .await
        .unwrap()
        .is_empty());
}

/// A field that already carries a DIFFERENT index type (`lumina`) over an
/// overlapping row range must not block a vindex (`ivf-flat`) build on the
/// same field: the two indexes have distinct identities and coexist. Before
/// the full-identity fix, the overlap guard keyed only on field id + range
/// and spuriously rejected this build with the "overlaps requested row
/// range" error.
#[tokio::test]
async fn vindex_build_coexists_with_different_index_type_on_same_field() {
    let table_path = "memory:/test_vindex_coexist_diff_type";
    let table = vindex_e2e_table(table_path, "10");
    setup_dirs(table.file_io(), table_path).await;

    write_vectors(
        &table,
        vec![1, 2, 3],
        vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![1.0, 1.0]],
    )
    .await;

    // Pre-existing `lumina` index covering the full data range on the SAME
    // field the vindex build will target.
    let coverage = data_row_id_coverage(&table).await;
    assert_eq!(coverage.len(), 1, "data must be one contiguous range");
    let field_id = find_index_field(&table, "embedding").unwrap().id();
    let lumina = IndexFileMeta {
        index_type: "lumina".to_string(),
        file_name: "lumina-synthetic-0.index".to_string(),
        file_size: 1,
        row_count: (coverage[0].to() - coverage[0].from() + 1) as i64,
        deletion_vectors_ranges: None,
        global_index_meta: Some(GlobalIndexMeta {
            row_range_start: coverage[0].from(),
            row_range_end: coverage[0].to(),
            index_field_id: field_id,
            extra_field_ids: None,
            source_meta: None,
            index_meta: None,
        }),
    };
    let mut message = CommitMessage::new(BinaryRow::new(0).to_serialized_bytes(), 0, vec![]);
    message.new_index_files = vec![lumina];
    TableCommit::new(table.clone(), "test-user".to_string())
        .commit(vec![message])
        .await
        .unwrap();

    // Building `ivf-flat` on the same field must NOT trip the overlap guard.
    // A native-build failure over the tiny synthetic dataset is tolerated;
    // only the overlap error is forbidden.
    let result = table
        .new_vindex_index_build_builder(IVF_FLAT_IDENTIFIER)
        .with_index_column("embedding")
        .execute()
        .await;
    match result {
        Ok(_) => {}
        Err(Error::DataInvalid { message, .. }) => {
            assert!(
                    !message.contains("overlaps requested row range"),
                    "vindex build must coexist with a different-type index on the same field; got: {message}"
                );
        }
        Err(other) => panic!("unexpected error from vindex build: {other:?}"),
    }
}

/// Regression: a first build (no existing index) must equal the pre-change
/// full build -- subtracting an empty `indexed` yields full coverage.
#[test]
fn vindex_first_build_indexes_full_coverage() {
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

/// Planner-level mid-coverage hole, mirroring btree/lumina: with a single
/// shard cell (rows_per_shard large enough to hold all data) the grid never
/// splits, so the only split is the indexed hole itself. An indexed range
/// strictly inside the data coverage must carve the build into exactly the
/// two contiguous segments on either side of the hole -- both bounds pinned,
/// and neither segment may span or touch the hole.
#[test]
fn vindex_plan_splits_gap_around_mid_coverage_indexed_hole() {
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
/// `vindex_incremental_build_indexes_only_new_rows`, which asserts only a
/// one-sided lower bound (`row_range_start >= n`): an indexed prefix [0, k]
/// must leave EXACTLY the suffix [k+1, N] on both bounds, split along the
/// shard grid, with nothing re-indexed inside the prefix.
#[test]
fn vindex_plan_incremental_prefix_leaves_suffix() {
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
