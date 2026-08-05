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

//! End-to-end acceptance gate for primary-key vector search.
//!
//! Builds a complete, self-contained primary-key table in a temporary directory
//! entirely from Rust — data file, a real vindex IVF-flat ANN index segment, and
//! the snapshot/manifest/index-manifest metadata — then reads it back through the
//! public `new_vector_search_builder()` API and asserts both the search result
//! (`execute_scored()` -> `row_ids`/`scores`) and the materialized rows
//! (`execute_read()` -> Arrow batches, best-first order, `__paimon_search_score`).
//!
//! Why Rust-built rather than a committed cross-language fixture: the Java
//! primary-key vector ANN segment is an opaque native Lumina format that cannot
//! be reproduced byte-for-byte here, whereas the Rust read path is backed by the
//! vindex (IVF) segment format. This test therefore validates the Rust read path
//! against real vindex IVF segment bytes it produces itself, with no committed
//! binaries and nothing skipped.
//!
//! Two constraints the primary-key read path enforces are satisfied by hand
//! (mirroring Java `PrimaryKeyIndexSourcePolicy` and `PrimaryKeyIndexSourceMeta`):
//!   1. Only a compacted (`file_source == COMPACT`), non-level-0 data file backs
//!      the index, so the written file's meta is cloned with `level = 1` and
//!      `file_source = Some(1)`.
//!   2. `GlobalIndexMeta.source_meta` must be the Java `PrimaryKeyIndexSourceMeta` frame
//!      (big-endian ints/longs, `writeUTF` file names), assembled below.
//!
//! Determinism: every fixture uses `nlist = 1`, so the single IVF inverted list
//! is scanned exhaustively and the search is exact; datasets are chosen so the
//! top-k distances have strict gaps, making the best-first order unique and
//! immune to tie-breaks or IVF approximation.

use std::collections::HashMap;
use std::io::Cursor;

use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
use arrow_array::{Array, ArrayRef, FixedSizeListArray, Float32Array, Int32Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use bytes::Bytes;
use futures::TryStreamExt;
use paimon::catalog::Identifier;
use paimon::io::{FileIO, FileIOBuilder};
use paimon::spec::{
    DataFileMeta, DataType, FloatType, GlobalIndexMeta, IndexFileMeta, IntType, Schema,
    TableSchema, VectorType,
};
// Used only by the residual end-to-end test, which is gated off Windows.
#[cfg(not(windows))]
use paimon::spec::{Datum, Predicate, PredicateBuilder};
use paimon::table::{CommitMessage, SchemaManager, Table, TableCommit};
use paimon_vindex_core::index::{VectorIndexConfig, VectorIndexTrainer, VectorIndexWriter};
use paimon_vindex_core::io::PosWriter;
use std::sync::Arc;

/// Vector dimension for the test datasets.
const DIM: usize = 4;
/// The primary-key vector column name.
const VECTOR_COLUMN: &str = "embedding";
/// vindex index type (IVF-flat); matches `IndexFileMeta.index_type`.
const INDEX_TYPE: &str = "ivf-flat";

/// vindex L2 distance is the *squared* L2 (see paimon-vindex-core `fvec_l2sqr`),
/// and the primary-key vector metric is `l2`, whose `distance_to_score` is
/// `1 / (1 + distance)`. Kept in one place so both tests agree with the kernel.
fn l2_score(distance: f32) -> f32 {
    1.0 / (1.0 + distance)
}

/// Brute-force exact squared-L2 top-k over the fixture rows, returning
/// `(physical_position, squared_l2_distance)` best-first. Physical position ==
/// row index == global row id (the fixture pins `first_row_id = 0`). This is the
/// ground truth the fixture is validated against, derived from the data rather
/// than hand-tabulated, so the assertions cannot drift from the vectors.
fn analytic_topk(query: &[f32], vectors: &[[f32; DIM]], k: usize) -> Vec<(u64, f32)> {
    let mut scored: Vec<(u64, f32)> = vectors
        .iter()
        .enumerate()
        .map(|(pos, v)| {
            let dist: f32 = v
                .iter()
                .zip(query.iter())
                .map(|(a, b)| (a - b) * (a - b))
                .sum();
            (pos as u64, dist)
        })
        .collect();
    scored.sort_by(|a, b| a.1.total_cmp(&b.1));
    scored.truncate(k);
    scored
}

/// Table options that route searches into the primary-key vector branch (the
/// `VectorSearchBuilder` detects a primary-key table with a PK-vector index and
/// takes the bucket-local ANN path). Default search mode is FAST, so only the ANN
/// segment is consulted (no exact fallback).
///
/// `deletion-vectors.enabled = true` (and merge-on-read left at its default
/// `false`) is what makes the table expose physical rows directly, the
/// precondition Java `PrimaryKeyVectorScan` requires before a residual data
/// predicate (`with_filter`) may be applied post-recall. This fixture never
/// actually deletes a row, so no deletion files are written; the option only
/// satisfies the residual guard and does not otherwise change the write/read
/// path (default `deduplicate` merge-engine, no deletion files -> no DV factory
/// is built on read).
fn table_options() -> Vec<(String, String)> {
    vec![
        ("bucket".to_string(), "1".to_string()),
        ("deletion-vectors.enabled".to_string(), "true".to_string()),
        (
            "pk-vector.index.columns".to_string(),
            VECTOR_COLUMN.to_string(),
        ),
        (
            format!("fields.{VECTOR_COLUMN}.pk-vector.index.type"),
            INDEX_TYPE.to_string(),
        ),
        (
            format!("fields.{VECTOR_COLUMN}.pk-vector.distance.metric"),
            "l2".to_string(),
        ),
    ]
}

/// Primary-key schema `(id INT PRIMARY KEY, embedding VECTOR<FLOAT>)`.
fn pk_vector_schema() -> TableSchema {
    let mut builder = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            VECTOR_COLUMN,
            DataType::Vector(
                VectorType::try_new(true, DIM as u32, DataType::Float(FloatType::new())).unwrap(),
            ),
        )
        .primary_key(["id"]);
    for (k, v) in table_options() {
        builder = builder.option(k, v);
    }
    TableSchema::new(0, &builder.build().unwrap())
}

/// Arrow batch matching the table schema: `id` (== physical position) plus a
/// `FixedSizeList<Float32>` vector column named to match paimon's target Arrow
/// schema (`element`).
fn data_batch(vectors: &[[f32; DIM]]) -> RecordBatch {
    let ids: Vec<i32> = (0..vectors.len() as i32).collect();

    let element_field = Arc::new(ArrowField::new("element", ArrowDataType::Float32, true));
    let mut vector_builder = FixedSizeListBuilder::new(Float32Builder::new(), DIM as i32)
        .with_field(element_field.clone());
    for vector in vectors {
        for &value in vector {
            vector_builder.values().append_value(value);
        }
        vector_builder.append(true);
    }

    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new(
            VECTOR_COLUMN,
            ArrowDataType::FixedSizeList(element_field, DIM as i32),
            true,
        ),
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

/// Encode one Java `DataOutput#writeUTF` value (u16-BE byte length + modified
/// UTF-8). ASCII file names are the common case; multibyte handling mirrors the
/// round-trip helper in `PrimaryKeyIndexSourceMeta`'s own tests.
fn java_write_utf(s: &str) -> Vec<u8> {
    let mut body = Vec::new();
    for c in s.encode_utf16() {
        if (0x0001..=0x007F).contains(&c) {
            body.push(c as u8);
        } else if c > 0x07FF {
            body.push(0xE0 | (c >> 12) as u8);
            body.push(0x80 | ((c >> 6) & 0x3F) as u8);
            body.push(0x80 | (c & 0x3F) as u8);
        } else {
            body.push(0xC0 | (c >> 6) as u8);
            body.push(0x80 | (c & 0x3F) as u8);
        }
    }
    let mut out = (body.len() as u16).to_be_bytes().to_vec();
    out.extend_from_slice(&body);
    out
}

/// Assemble the `_SOURCE_META` frame the way Java `PrimaryKeyIndexSourceMeta` writes it
/// and `PrimaryKeyIndexSourceMeta::deserialize` expects: `i32-BE version=1`, `i32-BE
/// data_level`, `i32-BE count`, then per source file a `writeUTF` name and an
/// `i64-BE` row count. No trailing bytes. Source files are listed in global
/// ordinal order.
fn source_meta_bytes(data_level: i32, files: &[(&str, i64)]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&1i32.to_be_bytes()); // version
    out.extend_from_slice(&data_level.to_be_bytes());
    out.extend_from_slice(&(files.len() as i32).to_be_bytes());
    for (name, rows) in files {
        out.extend_from_slice(&java_write_utf(name));
        out.extend_from_slice(&rows.to_be_bytes());
    }
    out
}

/// Build a real vindex IVF-flat index segment over `vectors` (label == physical
/// position) and write it into `{table}/index/{file_name}`. Container format and
/// API usage mirror `VindexIndexBuildBuilder::build_index_file`, so the segment
/// is readable by `VindexVectorGlobalIndexReader::visit_vector_search`.
///
/// `nlist = 1` keeps the search exact (a single inverted list scanned in full).
/// The vindex `metric = l2` matches the table's `pk-vector.distance.metric = l2`,
/// so distances agree.
async fn write_ann_segment(
    file_io: &FileIO,
    table_location: &str,
    file_name: &str,
    vectors: &[[f32; DIM]],
) -> u64 {
    let n = vectors.len();
    let flat: Vec<f32> = vectors.iter().flat_map(|v| v.iter().copied()).collect();
    let ids: Vec<i64> = (0..n as i64).collect();

    let native_options = HashMap::from([
        ("index.type".to_string(), "ivf_flat".to_string()),
        ("dimension".to_string(), DIM.to_string()),
        ("nlist".to_string(), "1".to_string()),
        ("metric".to_string(), "l2".to_string()),
    ]);
    let config = VectorIndexConfig::from_options(&native_options).unwrap();

    let training = VectorIndexTrainer::train(config, &flat, n).unwrap();
    let mut writer = VectorIndexWriter::new(training);
    writer.add_vectors(&ids, &flat, n).unwrap();
    let mut bytes = Vec::new();
    {
        let mut output = PosWriter::new(&mut bytes);
        writer.write(&mut output).unwrap();
    }

    let index_dir = format!("{}/index", table_location.trim_end_matches('/'));
    file_io.mkdirs(&index_dir).await.unwrap();
    let index_path = format!("{index_dir}/{file_name}");
    let file_size = bytes.len() as u64;
    file_io
        .new_output(&index_path)
        .unwrap()
        .write(Bytes::from(bytes))
        .await
        .unwrap();
    file_size
}

/// Round-trip the segment bytes through the reader in isolation, asserting the
/// analytic expectation. This proves the produced vindex bytes are readable and
/// the distances match before the full table read path is exercised.
fn assert_segment_reads_back(bytes: &[u8], query: &[f32], expected: &[(u64, f32)]) {
    use paimon_vindex_core::index::{VectorIndexReader, VectorSearchParams};
    let mut reader = VectorIndexReader::open(Cursor::new(bytes.to_vec())).unwrap();
    reader.optimize_for_search().unwrap();
    let (labels, distances) = reader
        .search(query, VectorSearchParams::new(expected.len(), 1))
        .unwrap();
    // Pair and sort best-first (smallest distance) to compare with the analytic
    // expectation regardless of the reader's internal ordering.
    let mut pairs: Vec<(i64, f32)> = labels.into_iter().zip(distances).collect();
    pairs.sort_by(|a, b| a.1.total_cmp(&b.1));
    let got: Vec<(u64, f32)> = pairs
        .into_iter()
        .map(|(label, distance)| (label as u64, distance))
        .collect();
    for ((got_id, got_d), (want_id, want_d)) in got.iter().zip(expected.iter()) {
        assert_eq!(got_id, want_id, "segment label diverges from expected");
        assert!(
            (got_d - want_d).abs() < 1e-3,
            "segment distance diverges: got {got_d}, want {want_d}"
        );
    }
}

/// Open a table from the local filesystem, loading its latest schema.
async fn open_table(file_io: &FileIO, location: &str) -> Table {
    let schema = SchemaManager::new(file_io.clone(), location.to_string())
        .latest()
        .await
        .expect("failed to list schemas")
        .expect("table has no schema");
    Table::new(
        file_io.clone(),
        Identifier::new("default", "pkvector_baseline"),
        location.to_string(),
        (*schema).clone(),
        None,
    )
}

/// Build a complete self-contained primary-key vector table over `vectors` in a
/// fresh temp dir: persist the schema, write a real data file, apply the two
/// PK-vector constraints to its meta, build+commit a real vindex ANN segment, and
/// verify the segment reads back against `analytic_topk`. Returns the temp dir
/// (kept alive by the caller) and the opened table.
async fn build_table(
    query: &[f32],
    vectors: &[[f32; DIM]],
    k: usize,
) -> (tempfile::TempDir, Table) {
    // Default fixture: `first_row_id = Some(0)`, so a global row id equals its
    // physical position. Tests that must decouple the two use
    // `build_table_with_first_row_id` directly.
    build_table_with_first_row_id(query, vectors, k, Some(0)).await
}

/// As `build_table`, but pins the indexed data file's `first_row_id` to the
/// caller's value. A non-zero (or absent) `first_row_id` breaks the "global row
/// id == physical position" coincidence, so the read path must key candidate
/// selection, position recovery, and score alignment off the file-local physical
/// position rather than a global row id.
async fn build_table_with_first_row_id(
    query: &[f32],
    vectors: &[[f32; DIM]],
    k: usize,
    first_row_id: Option<i64>,
) -> (tempfile::TempDir, Table) {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let location = format!("file://{}", tmp.path().display());
    let file_io = FileIOBuilder::new("file").build().unwrap();

    // Table layout dirs, then persist the schema.
    for dir in ["schema", "snapshot", "manifest", "index"] {
        file_io.mkdirs(&format!("{location}/{dir}")).await.unwrap();
    }
    let schema = pk_vector_schema();
    file_io
        .new_output(&format!("{location}/schema/schema-{}", schema.id()))
        .unwrap()
        .write(Bytes::from(serde_json::to_vec(&schema).unwrap()))
        .await
        .unwrap();

    let table = open_table(&file_io, &location).await;

    // Write a real data file via the public write path to obtain a genuine
    // DataFileMeta (real file name, row count, stats, file size). Its stats type
    // is crate-private, so we reuse this meta rather than construct one.
    let write_builder = table.new_write_builder();
    let mut writer = write_builder.new_write().unwrap();
    writer
        .write_arrow_batch(&data_batch(vectors))
        .await
        .unwrap();
    let write_messages = writer.prepare_commit().await.unwrap();
    assert_eq!(
        write_messages.len(),
        1,
        "single bucket -> one write message"
    );
    let written = &write_messages[0];
    assert_eq!(written.new_files.len(), 1, "single data file expected");
    let base_meta = written.new_files[0].clone();
    let bucket = written.bucket;
    let partition = written.partition.clone();
    let data_file_name = base_meta.file_name.clone();
    let row_count = base_meta.row_count;

    // Constraint 1 (PrimaryKeyIndexSourcePolicy.shouldRead): only a compacted,
    // non-level-0 file backs the PK-vector index. Clone the real meta and set
    // level > 0 + file_source == COMPACT (1). `first_row_id` is caller-controlled:
    // real Java PK tables never write it (row-tracking is forbidden), so a correct
    // read path must not depend on `first_row_id == 0` for physical-position
    // recovery.
    let indexed_meta = DataFileMeta {
        level: 1,
        file_source: Some(1),
        first_row_id,
        ..base_meta
    };

    // Build and persist the real vindex ANN segment; verify it reads back before
    // wiring it into the table.
    let index_file_name = "vector-ivf-flat-pkvector-baseline.index".to_string();
    let index_file_size = write_ann_segment(&file_io, &location, &index_file_name, vectors).await;
    {
        let bytes = file_io
            .new_input(&format!("{location}/index/{index_file_name}"))
            .unwrap()
            .read()
            .await
            .unwrap();
        assert_segment_reads_back(&bytes, query, &analytic_topk(query, vectors, k));
    }

    // Constraint 2: GlobalIndexMeta.source_meta must be the Java PrimaryKeyIndexSourceMeta
    // frame naming the backing data file(s) in ordinal order. Here one source file
    // owns all rows, so ordinal == physical position.
    let vector_field_id = schema
        .fields()
        .iter()
        .find(|f| f.name() == VECTOR_COLUMN)
        .expect("vector field present")
        .id();
    let index_file = IndexFileMeta {
        index_type: INDEX_TYPE.to_string(),
        file_name: index_file_name,
        file_size: i64::try_from(index_file_size).unwrap(),
        row_count: i32::try_from(row_count).unwrap(),
        deletion_vectors_ranges: None,
        global_index_meta: Some(GlobalIndexMeta {
            row_range_start: 0,
            row_range_end: row_count - 1,
            index_field_id: vector_field_id,
            extra_field_ids: None,
            source_meta: Some(source_meta_bytes(
                indexed_meta.level,
                &[(&data_file_name, row_count)],
            )),
            index_meta: None,
            build_schema_id: None,
        }),
    };

    // Commit the indexed data file together with the ANN index segment in one
    // snapshot. TableCommit writes the data manifest, index manifest, and snapshot.
    let mut message = CommitMessage::new(partition, bucket, vec![indexed_meta]);
    message.new_index_files = vec![index_file];
    TableCommit::new(table.clone(), "pkvector-baseline".to_string())
        .commit(vec![message])
        .await
        .unwrap();

    (tmp, table)
}

/// Run `execute_read()` and flatten the stream into per-row `(id, score)` tuples
/// in emission order (best-first), returning the collected batches too for
/// schema / row-content assertions.
async fn read_id_and_scores(
    table: &Table,
    query: Vec<f32>,
    limit: usize,
    projection: Option<&[&str]>,
) -> (Vec<i32>, Vec<f32>, Vec<RecordBatch>) {
    let mut builder = table.new_vector_search_builder();
    builder
        .with_vector_column(VECTOR_COLUMN)
        .with_query_vector(query)
        .with_limit(limit);
    if let Some(cols) = projection {
        builder.with_projection(cols);
    }
    let batches = builder
        .execute_read()
        .await
        .expect("primary-key vector read failed")
        .try_collect::<Vec<_>>()
        .await
        .expect("collecting read batches failed");

    let ids: Vec<i32> = batches
        .iter()
        .flat_map(|b| {
            let idx = b.schema().index_of("id").unwrap();
            b.column(idx)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    let scores: Vec<f32> = batches
        .iter()
        .flat_map(|b| {
            let idx = b.schema().index_of("__paimon_search_score").unwrap();
            b.column(idx)
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    (ids, scores, batches)
}

/// Extract the materialized vector column across all batches, one `Vec<f32>` per
/// row in emission order.
fn collect_vectors(batches: &[RecordBatch]) -> Vec<Vec<f32>> {
    let mut out = Vec::new();
    for batch in batches {
        let idx = batch.schema().index_of(VECTOR_COLUMN).unwrap();
        let fsl = batch
            .column(idx)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .expect("vector column must materialize as FixedSizeList");
        for row in 0..fsl.len() {
            let values = fsl.value(row);
            let floats = values
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("vector element must be Float32");
            out.push(floats.values().to_vec());
        }
    }
    out
}

/// Fixture #1: distances 1 < 41 < 67 < 181, top-3 = rows 0, 4, 5. Here the
/// best-first order (0, 4, 5) happens to be ascending physical position, so this
/// case cannot by itself catch a "position order" regression — that gap is closed
/// by the discriminating fixture below. This case pins the search result and the
/// score alignment on the read path.
fn fixture_smoke() -> ([f32; DIM], Vec<[f32; DIM]>) {
    let query = [9.0, 0.0, 0.0, 0.0];
    let vectors = vec![
        [10.0, 0.0, 0.0, 0.0], // row 0 -> (9-10)^2                = 1
        [0.0, 10.0, 0.0, 0.0], // row 1 -> 81 + 100                = 181
        [0.0, 0.0, 10.0, 0.0], // row 2 -> 181
        [0.0, 0.0, 0.0, 10.0], // row 3 -> 181
        [5.0, 5.0, 0.0, 0.0],  // row 4 -> 16 + 25                 = 41
        [1.0, 1.0, 1.0, 1.0],  // row 5 -> 64 + 1 + 1 + 1          = 67
    ];
    (query, vectors)
}

/// Fixture #2 (discriminating): the nearest neighbour sits at physical position
/// 5, the second nearest at position 1, the third at position 3, so the
/// best-first order [5, 1, 3] is NOT the ascending physical-position order
/// [1, 3, 5]. If the read path ever degraded to emitting rows in physical
/// position order, the ordering assertion below would fail.
///
///   query [10,0,0,0]
///   pos0 [0,4,0,0] -> 100 + 16 = 116
///   pos1 [8,0,0,0] -> 4                 (2nd nearest)
///   pos2 [0,0,5,0] -> 100 + 25 = 125
///   pos3 [7,0,0,0] -> 9                 (3rd nearest)
///   pos4 [0,0,0,6] -> 100 + 36 = 136
///   pos5 [9,0,0,0] -> 1                 (nearest)
/// Strict gaps 1 < 4 < 9 < 116 < 125 < 136 make the top-3 order unique.
fn fixture_discriminating() -> ([f32; DIM], Vec<[f32; DIM]>) {
    let query = [10.0, 0.0, 0.0, 0.0];
    let vectors = vec![
        [0.0, 4.0, 0.0, 0.0], // pos 0
        [8.0, 0.0, 0.0, 0.0], // pos 1
        [0.0, 0.0, 5.0, 0.0], // pos 2
        [7.0, 0.0, 0.0, 0.0], // pos 3
        [0.0, 0.0, 0.0, 6.0], // pos 4
        [9.0, 0.0, 0.0, 0.0], // pos 5
    ];
    (query, vectors)
}

// Gated off Windows: the fixture table location is a `file://` URL built from a
// temp dir path, which `FileIO` cannot derive on Windows (see #397); the sibling
// `rest_catalog_test` gates its identical `file://` tempdir tests the same way.
#[cfg(not(windows))]
#[tokio::test]
async fn pk_vector_end_to_end_returns_expected_row_ids_and_scores() {
    let (query, vectors) = fixture_smoke();
    let (_tmp, table) = build_table(&query, &vectors, 3).await;

    let expected = analytic_topk(&query, &vectors, 3);
    let expected_row_ids: Vec<u64> = expected.iter().map(|(id, _)| *id).collect();
    let expected_scores: Vec<f32> = expected.iter().map(|(_, d)| l2_score(*d)).collect();

    // A primary-key vector table exposes no global row ids, so the search-only
    // `execute_scored()` path is unsupported and must fail loud, directing callers
    // to the materialized `execute_read()` path exercised below.
    let scored_err = table
        .new_vector_search_builder()
        .with_vector_column(VECTOR_COLUMN)
        .with_query_vector(query.to_vec())
        .with_limit(3)
        .execute_scored()
        .await
        .expect_err("primary-key execute_scored must fail loud");
    assert!(
        format!("{scored_err:?}").contains("execute_read"),
        "primary-key execute_scored should point at execute_read, got: {scored_err:?}"
    );

    // Search-and-read: execute_read() materializes the matching rows best-first
    // with a `__paimon_search_score` column, hiding `_ROW_ID`/`_PKEY_VECTOR_POSITION`.
    // Projection ['id'] excludes the vector column.
    let (ids, scores, batches) = read_id_and_scores(&table, query.to_vec(), 3, Some(&["id"])).await;

    let expected_ids: Vec<i32> = expected_row_ids.iter().map(|&id| id as i32).collect();
    assert_eq!(ids, expected_ids, "materialized rows must be best-first");
    assert_eq!(scores.len(), 3);
    for (got, want) in scores.iter().zip(&expected_scores) {
        assert!(
            (got - want).abs() < 1e-4,
            "materialized score diverges: got {got}, want {want}"
        );
    }
    for batch in &batches {
        assert!(
            batch.schema().index_of("_ROW_ID").is_err(),
            "_ROW_ID must not leak into read output"
        );
        assert!(
            batch.schema().index_of("_PKEY_VECTOR_POSITION").is_err(),
            "_PKEY_VECTOR_POSITION must not leak into read output"
        );
        assert!(
            batch.schema().index_of(VECTOR_COLUMN).is_err(),
            "projection ['id'] must exclude the vector column"
        );
    }
}

/// Closes the "best-first == physical position order" discriminative gap: reads
/// back a fixture whose nearest neighbours are at physical positions 5, 1, 3 (in
/// that order) and asserts the materialized output is emitted best-first
/// [5, 1, 3], not in ascending physical position [1, 3, 5]. Also asserts the full
/// row content (id + vector values) and the aligned `__paimon_search_score`, with no
/// `_ROW_ID`/`_PKEY_VECTOR_POSITION` leaking.
// Gated off Windows for the same `file://` tempdir reason as the test above.
#[cfg(not(windows))]
#[tokio::test]
async fn pk_vector_read_orders_rows_best_first_not_by_position() {
    let (query, vectors) = fixture_discriminating();
    let (_tmp, table) = build_table(&query, &vectors, 3).await;

    let expected = analytic_topk(&query, &vectors, 3);
    let expected_ids: Vec<i32> = expected.iter().map(|(id, _)| *id as i32).collect();
    // The whole point of this fixture: best-first order != ascending position.
    assert_eq!(
        expected_ids,
        vec![5, 1, 3],
        "fixture must produce best-first order distinct from physical position order"
    );
    let mut position_order = expected_ids.clone();
    position_order.sort_unstable();
    assert_ne!(
        expected_ids, position_order,
        "fixture is only discriminating if best-first != ascending position"
    );

    // Default projection (all user columns): id + vector column materialize.
    let (ids, scores, batches) = read_id_and_scores(&table, query.to_vec(), 3, None).await;

    // Row order == best-first, NOT physical position order. A regression to
    // position order would emit [1, 3, 5] and fail here.
    assert_eq!(
        ids, expected_ids,
        "materialized rows must be best-first [5, 1, 3], not position order [1, 3, 5]"
    );

    // Row content: the materialized vector for each emitted row equals the source
    // vector at that physical position.
    let got_vectors = collect_vectors(&batches);
    assert_eq!(got_vectors.len(), 3, "three rows expected");
    for (row_idx, (id, _)) in expected.iter().enumerate() {
        assert_eq!(
            got_vectors[row_idx],
            vectors[*id as usize].to_vec(),
            "materialized vector for row id {id} diverges from source data"
        );
    }

    // Score alignment: `__paimon_search_score` matches metric.distance_to_score for
    // each emitted row, in best-first order.
    assert_eq!(scores.len(), 3);
    for (got, (_, distance)) in scores.iter().zip(&expected) {
        assert!(
            (got - l2_score(*distance)).abs() < 1e-4,
            "materialized score diverges: got {got}, want {}",
            l2_score(*distance)
        );
    }

    // Hidden metadata columns must not leak into the output.
    for batch in &batches {
        assert!(
            batch.schema().index_of("_ROW_ID").is_err(),
            "_ROW_ID must not leak into read output"
        );
        assert!(
            batch.schema().index_of("_PKEY_VECTOR_POSITION").is_err(),
            "_PKEY_VECTOR_POSITION must not leak into read output"
        );
        // Default projection keeps the user vector column.
        assert!(
            batch.schema().index_of(VECTOR_COLUMN).is_ok(),
            "default projection must materialize the vector column"
        );
    }
}

/// Shared body for the two physical-coordinate contract tests below. Builds the
/// discriminating fixture with the caller's `first_row_id`, reads it back with the
/// default projection, and asserts the materialized rows are the file-LOCAL
/// best-first top-k (id column, vector content, aligned `__paimon_search_score`),
/// invariant to `first_row_id`. The discriminating fixture makes best-first order
/// [5, 1, 3] distinct from ascending physical position [1, 3, 5], so a
/// position/global-id confusion cannot pass by coincidence.
#[cfg(not(windows))]
async fn assert_discriminating_local_read(first_row_id: Option<i64>) {
    let (query, vectors) = fixture_discriminating();
    let (_tmp, table) = build_table_with_first_row_id(&query, &vectors, 3, first_row_id).await;

    let expected = analytic_topk(&query, &vectors, 3);
    let expected_ids: Vec<i32> = expected.iter().map(|(id, _)| *id as i32).collect();
    assert_eq!(
        expected_ids,
        vec![5, 1, 3],
        "fixture must produce best-first order distinct from physical position order"
    );
    let expected_scores: Vec<f32> = expected.iter().map(|(_, d)| l2_score(*d)).collect();

    // Default projection: id + vector column materialize. The read path must
    // return the correct FILE-LOCAL rows regardless of `first_row_id`.
    let (ids, scores, batches) = read_id_and_scores(&table, query.to_vec(), 3, None).await;

    assert_eq!(
        ids, expected_ids,
        "materialized `id` column must be file-local best-first [5, 1, 3] and \
         independent of the data file's first_row_id"
    );

    // Row content: each emitted row's vector equals the source vector at that
    // file-local physical position (a global-id offset would fetch the wrong row).
    let got_vectors = collect_vectors(&batches);
    assert_eq!(got_vectors.len(), 3, "three rows expected");
    for (row_idx, (id, _)) in expected.iter().enumerate() {
        assert_eq!(
            got_vectors[row_idx],
            vectors[*id as usize].to_vec(),
            "materialized vector for row id {id} diverges from source data"
        );
    }

    // Score alignment must also key off the file-local position.
    assert_eq!(scores.len(), 3);
    for (got, want) in scores.iter().zip(&expected_scores) {
        assert!(
            (got - want).abs() < 1e-4,
            "materialized score diverges: got {got}, want {want}"
        );
    }
}

/// Physical-coordinate contract: a primary-key vector read must select rows,
/// recover positions, and align scores by the file-LOCAL physical position, and
/// must NOT require the data file to carry a `first_row_id`. Real Java primary-key
/// tables never write `first_row_id` (row-tracking is forbidden for PK tables), so
/// this fixture pins the indexed data file's `first_row_id = None` — the same
/// shape the committed Java fixture has, but over a Rust-built table with a
/// discriminating dataset (best-first [5, 1, 3] != ascending position [1, 3, 5]).
/// A read path that keys candidate selection or position recovery off a global row
/// id (rather than the file-local physical position) cannot satisfy this.
// Gated off Windows for the same `file://` tempdir reason as the tests above.
#[cfg(not(windows))]
#[tokio::test]
async fn execute_read_without_first_row_id_selects_local_positions() {
    assert_discriminating_local_read(None).await;
}

/// Regression pin for the same physical-coordinate contract with a present but
/// deliberately NON-aligned `first_row_id = Some(100)`: recovering the file-local
/// physical position must not be offset by `first_row_id`. This holds on the
/// current code (the global-range round-trip is symmetric) and must keep holding
/// after the read path switches to file-local coordinates, so a fix that reads
/// local positions yet leaves a stray `first_row_id` offset would surface here.
// Gated off Windows for the same `file://` tempdir reason as the tests above.
#[cfg(not(windows))]
#[tokio::test]
async fn execute_read_ignores_nonzero_first_row_id() {
    assert_discriminating_local_read(Some(100)).await;
}

/// Fixture #3 (residual): the unrestricted nearest neighbours sit at low ids
/// (0, 1, 2), but the residual predicate `id >= 3` excludes exactly those, so
/// the residual result set is disjoint from the unfiltered one. Among the rows
/// the residual keeps (ids 3, 4, 5) the best-first order is [4, 5, 3], which is
/// neither the ascending id order [3, 4, 5] nor a prefix of the unfiltered
/// order — proving the residual genuinely reshapes the result.
///
///   query [10,0,0,0]
///   pos0 [10,0,0,0] ->  0   (unfiltered nearest; excluded by id >= 3)
///   pos1 [ 9,0,0,0] ->  1   (excluded)
///   pos2 [ 8,0,0,0] ->  4   (excluded)
///   pos3 [ 5,0,0,0] -> 25   (kept; farthest of the kept rows)
///   pos4 [ 7,0,0,0] ->  9   (kept; nearest of the kept rows)
///   pos5 [ 6,0,0,0] -> 16   (kept)
/// Strict gaps 0 < 1 < 4 < 9 < 16 < 25 make every top-k order unique.
///   unfiltered top-3 = [0, 1, 2]
///   residual (id >= 3) top-3 = [4, 5, 3]
#[cfg(not(windows))]
fn fixture_residual() -> ([f32; DIM], Vec<[f32; DIM]>) {
    let query = [10.0, 0.0, 0.0, 0.0];
    let vectors = vec![
        [10.0, 0.0, 0.0, 0.0], // pos 0 -> 0
        [9.0, 0.0, 0.0, 0.0],  // pos 1 -> 1
        [8.0, 0.0, 0.0, 0.0],  // pos 2 -> 4
        [5.0, 0.0, 0.0, 0.0],  // pos 3 -> 25
        [7.0, 0.0, 0.0, 0.0],  // pos 4 -> 9
        [6.0, 0.0, 0.0, 0.0],  // pos 5 -> 16
    ];
    (query, vectors)
}

/// Run `execute_read()` with a residual `filter` attached via `with_filter` and
/// flatten the stream into per-row `(id, score)` tuples in emission order
/// (best-first), returning the collected batches too for schema / row-content
/// assertions. Mirrors `read_id_and_scores` but exercises the residual path.
#[cfg(not(windows))]
async fn read_id_and_scores_filtered(
    table: &Table,
    query: Vec<f32>,
    limit: usize,
    filter: Predicate,
) -> (Vec<i32>, Vec<f32>, Vec<RecordBatch>) {
    let mut builder = table.new_vector_search_builder();
    builder
        .with_vector_column(VECTOR_COLUMN)
        .with_query_vector(query)
        .with_limit(limit)
        .with_filter(filter);
    let batches = builder
        .execute_read()
        .await
        .expect("primary-key vector residual read failed")
        .try_collect::<Vec<_>>()
        .await
        .expect("collecting residual read batches failed");

    let ids: Vec<i32> = batches
        .iter()
        .flat_map(|b| {
            let idx = b.schema().index_of("id").unwrap();
            b.column(idx)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    let scores: Vec<f32> = batches
        .iter()
        .flat_map(|b| {
            let idx = b.schema().index_of("__paimon_search_score").unwrap();
            b.column(idx)
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    (ids, scores, batches)
}

/// End-to-end coverage of the residual data predicate (`with_filter`) on the
/// public primary-key vector path. Mirrors Java `PrimaryKeyVectorRead`'s
/// residual-filter support: the residual columns are re-read per candidate file,
/// the surviving physical positions are folded into recall, and only rows
/// satisfying the predicate remain — best-first and Top-K preserved.
///
/// The fixture is built so the residual actually changes the result set: the
/// unfiltered top-3 is [0, 1, 2] while the residual (`id >= 3`) top-3 is
/// [4, 5, 3]. The two sets are disjoint, so a residual that silently did nothing
/// (or was ignored) would surface here.
// Gated off Windows for the same `file://` tempdir reason as the tests above.
#[cfg(not(windows))]
#[tokio::test]
async fn pk_vector_residual_filter_excludes_non_matching_rows() {
    let (query, vectors) = fixture_residual();
    let (_tmp, table) = build_table(&query, &vectors, 3).await;

    // Ground truth: unrestricted top-3, and the top-3 restricted to ids >= 3
    // (the residual only ranks rows whose id passes the predicate).
    let unfiltered = analytic_topk(&query, &vectors, 3);
    let unfiltered_ids: Vec<u64> = unfiltered.iter().map(|(id, _)| *id).collect();
    assert_eq!(
        unfiltered_ids,
        vec![0, 1, 2],
        "fixture guard: unfiltered top-3 must be [0, 1, 2]"
    );

    let residual_threshold = 3;
    let mut residual_ranked: Vec<(u64, f32)> = analytic_topk(&query, &vectors, vectors.len())
        .into_iter()
        .filter(|(id, _)| *id >= residual_threshold)
        .collect();
    residual_ranked.truncate(3);
    let expected_ids: Vec<i32> = residual_ranked.iter().map(|(id, _)| *id as i32).collect();
    let expected_scores: Vec<f32> = residual_ranked.iter().map(|(_, d)| l2_score(*d)).collect();
    // The whole point of this fixture: residual result != unfiltered result, and
    // best-first over the kept rows is not ascending id order.
    assert_eq!(
        expected_ids,
        vec![4, 5, 3],
        "fixture guard: residual (id >= 3) top-3 must be best-first [4, 5, 3]"
    );

    // Build the residual predicate on the data column `id` via the public
    // PredicateBuilder, exactly as a caller would.
    let residual = PredicateBuilder::new(table.schema().fields())
        .greater_or_equal("id", Datum::Int(residual_threshold as i32))
        .expect("build residual predicate on id");

    // A primary-key vector table exposes no global row ids, so `execute_scored()`
    // is unsupported on this path — with or without a residual filter — and must
    // fail loud, directing callers to the materialized `execute_read()` used below.
    let unfiltered_err = table
        .new_vector_search_builder()
        .with_vector_column(VECTOR_COLUMN)
        .with_query_vector(query.to_vec())
        .with_limit(3)
        .execute_scored()
        .await
        .expect_err("primary-key execute_scored must fail loud");
    assert!(
        format!("{unfiltered_err:?}").contains("execute_read"),
        "got: {unfiltered_err:?}"
    );

    let residual_scored_err = table
        .new_vector_search_builder()
        .with_vector_column(VECTOR_COLUMN)
        .with_query_vector(query.to_vec())
        .with_limit(3)
        .with_filter(residual.clone())
        .execute_scored()
        .await
        .expect_err("primary-key execute_scored must fail loud with a residual too");
    assert!(
        format!("{residual_scored_err:?}").contains("execute_read"),
        "got: {residual_scored_err:?}"
    );

    // Search-and-read with the residual: default projection materializes id +
    // vector column, best-first, with an aligned `__paimon_search_score`.
    let (ids, scores, batches) =
        read_id_and_scores_filtered(&table, query.to_vec(), 3, residual).await;

    assert_eq!(
        ids, expected_ids,
        "residual read must emit only kept rows, best-first [4, 5, 3]"
    );
    for &id in &ids {
        assert!(
            id >= residual_threshold as i32,
            "residual read returned id {id} that fails the predicate id >= {residual_threshold}"
        );
    }

    // Row content: the materialized vector for each emitted row equals the source
    // vector at that physical position.
    let got_vectors = collect_vectors(&batches);
    assert_eq!(got_vectors.len(), 3, "three kept rows expected");
    for (row_idx, (id, _)) in residual_ranked.iter().enumerate() {
        assert_eq!(
            got_vectors[row_idx],
            vectors[*id as usize].to_vec(),
            "materialized vector for row id {id} diverges from source data"
        );
    }

    // Score alignment on the residual read path.
    assert_eq!(scores.len(), 3);
    for (got, want) in scores.iter().zip(&expected_scores) {
        assert!(
            (got - want).abs() < 1e-4,
            "residual read score diverges: got {got}, want {want}"
        );
    }

    // Hidden metadata columns must not leak, even on the residual path.
    for batch in &batches {
        assert!(
            batch.schema().index_of("_ROW_ID").is_err(),
            "_ROW_ID must not leak into residual read output"
        );
        assert!(
            batch.schema().index_of("_PKEY_VECTOR_POSITION").is_err(),
            "_PKEY_VECTOR_POSITION must not leak into residual read output"
        );
        assert!(
            batch.schema().index_of(VECTOR_COLUMN).is_ok(),
            "default projection must materialize the vector column"
        );
    }
}

// --- Exact-rerank coverage --------------------------------------------------
//
// The refine factor over-fetches approximate (ANN) candidates and reorders them
// by exact distance re-read from the data file. The two tests below drive that
// end-to-end through the public builder: one where the approximate order and the
// exact order genuinely differ (so the reorder is observable), and one where the
// search yields no indexed candidates (so the rerank must be skipped cleanly).

/// The query-side option key that requests exact rerank for the `embedding`
/// PK-vector column. `fields.<col>.ivf.refine-factor` is one of the aliases the
/// builder accepts for an `ivf-flat` index (the `ivf.` prefix, `refine-factor`
/// suffix); it is set as a query option so it takes precedence over table options.
fn refine_factor_option(factor: usize) -> HashMap<String, String> {
    HashMap::from([(
        format!("fields.{VECTOR_COLUMN}.ivf.refine-factor"),
        factor.to_string(),
    )])
}

/// Primary-key schema `(id INT PRIMARY KEY, embedding VECTOR<FLOAT>)` with extra
/// table options merged on top of [`table_options`]. Used to pin table-level
/// options (e.g. `global-index.search-mode`) that the query-side `with_options`
/// cannot influence.
fn pk_vector_schema_with(extra: &[(&str, &str)]) -> TableSchema {
    let mut builder = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            VECTOR_COLUMN,
            DataType::Vector(
                VectorType::try_new(true, DIM as u32, DataType::Float(FloatType::new())).unwrap(),
            ),
        )
        .primary_key(["id"]);
    for (k, v) in table_options() {
        builder = builder.option(k, v);
    }
    for (k, v) in extra {
        builder = builder.option(k.to_string(), v.to_string());
    }
    TableSchema::new(0, &builder.build().unwrap())
}

/// Persist `schema` and write one real data file over `data_vectors` in a fresh
/// temp dir, returning the temp dir, the opened table, the `FileIO`, the table
/// location, the written file's meta rewritten to satisfy the two PK-vector
/// constraints (compacted, non-level-0, `first_row_id = 0`), and its
/// bucket / partition.
async fn write_schema_and_data(
    schema: &TableSchema,
    data_vectors: &[[f32; DIM]],
) -> (
    tempfile::TempDir,
    Table,
    FileIO,
    String,
    DataFileMeta,
    Vec<u8>,
    i32,
) {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let location = format!("file://{}", tmp.path().display());
    let file_io = FileIOBuilder::new("file").build().unwrap();

    for dir in ["schema", "snapshot", "manifest", "index"] {
        file_io.mkdirs(&format!("{location}/{dir}")).await.unwrap();
    }
    file_io
        .new_output(&format!("{location}/schema/schema-{}", schema.id()))
        .unwrap()
        .write(Bytes::from(serde_json::to_vec(schema).unwrap()))
        .await
        .unwrap();

    let table = open_table(&file_io, &location).await;

    let write_builder = table.new_write_builder();
    let mut writer = write_builder.new_write().unwrap();
    writer
        .write_arrow_batch(&data_batch(data_vectors))
        .await
        .unwrap();
    let write_messages = writer.prepare_commit().await.unwrap();
    assert_eq!(
        write_messages.len(),
        1,
        "single bucket -> one write message"
    );
    let written = &write_messages[0];
    assert_eq!(written.new_files.len(), 1, "single data file expected");
    let base_meta = written.new_files[0].clone();

    let bucket = written.bucket;
    let partition = written.partition.clone();
    let indexed_meta = DataFileMeta {
        level: 1,
        file_source: Some(1),
        first_row_id: Some(0),
        ..base_meta
    };
    (
        tmp,
        table,
        file_io,
        location,
        indexed_meta,
        partition,
        bucket,
    )
}

/// Read `execute_read()` into `(id, score)` tuples (best-first) plus the batches,
/// mirroring [`read_id_and_scores`] but with caller-supplied query options so the
/// refine factor can be requested.
async fn read_id_and_scores_with_options(
    table: &Table,
    query: Vec<f32>,
    limit: usize,
    options: HashMap<String, String>,
) -> (Vec<i32>, Vec<f32>, Vec<RecordBatch>) {
    let mut builder = table.new_vector_search_builder();
    builder
        .with_vector_column(VECTOR_COLUMN)
        .with_query_vector(query)
        .with_limit(limit)
        .with_options(options);
    let batches = builder
        .execute_read()
        .await
        .expect("primary-key vector rerank read failed")
        .try_collect::<Vec<_>>()
        .await
        .expect("collecting rerank read batches failed");

    let ids: Vec<i32> = batches
        .iter()
        .flat_map(|b| {
            let idx = b.schema().index_of("id").unwrap();
            b.column(idx)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    let scores: Vec<f32> = batches
        .iter()
        .flat_map(|b| {
            let idx = b.schema().index_of("__paimon_search_score").unwrap();
            b.column(idx)
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    (ids, scores, batches)
}

/// Fixture where the ANN-approximate order and the exact-distance order are
/// disjoint. The data file holds `data_vectors` (the truth the rerank re-reads),
/// while the ANN segment is built over `index_vectors` (a perturbed copy), so the
/// approximate recall order differs from the exact order over the true data.
///
///   query [10,0,0,0]
///   position | index_vectors (ANN sees)         | data_vectors (truth)
///   ---------+----------------------------------+---------------------------
///   0        | [0,0,0,0]  -> 100                 | [9,0,0,0] ->   1  (exact 1st)
///   1        | [0,0,1,0]  -> 101                 | [8,0,0,0] ->   4  (exact 2nd)
///   2        | [0,0,2,0]  -> 104                 | [7,0,0,0] ->   9  (exact 3rd)
///   3        | [9,0,0,0]  ->   1  (ANN 3rd)      | [0,0,0,0] -> 100
///   4        | [9.5,0,0,0]->   0.25 (ANN 2nd)    | [0,5,0,0] -> 125
///   5        | [10,0,0,0] ->   0  (ANN 1st)      | [0,0,6,0] -> 136
///
/// ANN top-3 = [5, 4, 3]; exact top-3 = [0, 1, 2]. The two are disjoint, so if the
/// rerank never ran (gate broken or factor unset) the output would be [5, 4, 3].
fn fixture_rerank_divergent() -> ([f32; DIM], Vec<[f32; DIM]>, Vec<[f32; DIM]>) {
    let query = [10.0, 0.0, 0.0, 0.0];
    let index_vectors = vec![
        [0.0, 0.0, 0.0, 0.0],  // pos 0
        [0.0, 0.0, 1.0, 0.0],  // pos 1
        [0.0, 0.0, 2.0, 0.0],  // pos 2
        [9.0, 0.0, 0.0, 0.0],  // pos 3
        [9.5, 0.0, 0.0, 0.0],  // pos 4
        [10.0, 0.0, 0.0, 0.0], // pos 5
    ];
    let data_vectors = vec![
        [9.0, 0.0, 0.0, 0.0], // pos 0
        [8.0, 0.0, 0.0, 0.0], // pos 1
        [7.0, 0.0, 0.0, 0.0], // pos 2
        [0.0, 0.0, 0.0, 0.0], // pos 3
        [0.0, 5.0, 0.0, 0.0], // pos 4
        [0.0, 0.0, 6.0, 0.0], // pos 5
    ];
    (query, index_vectors, data_vectors)
}

/// refine_factor > 0 reorders the emitted rows to the exact-distance ground truth:
/// the ANN-approximate order (built over a perturbed copy of the vectors) differs
/// from the exact order over the true data, and the rerank corrects it.
///
/// The fixture builds the ANN segment over `index_vectors` while the data file
/// holds `data_vectors`, so the approximate recall order [5, 4, 3] is disjoint
/// from the exact order [0, 1, 2]. With `refine-factor = 2` the search over-fetches
/// `limit * 2 = 6` candidates (the whole bucket), then re-reads each candidate's
/// true vector and reorders by exact distance. The emitted rows must be the exact
/// top-3 [0, 1, 2] with scores computed from the true distances.
// Gated off Windows for the same `file://` tempdir reason as the tests above.
#[cfg(not(windows))]
#[tokio::test]
async fn pk_vector_refine_factor_matches_exact_ground_truth() {
    let (query, index_vectors, data_vectors) = fixture_rerank_divergent();
    let k = 3;

    // Ground truths: the ANN recall order (exact over the perturbed index vectors,
    // since nlist = 1 is exhaustive) and the exact order over the true data.
    let ann_order: Vec<i32> = analytic_topk(&query, &index_vectors, k)
        .iter()
        .map(|(id, _)| *id as i32)
        .collect();
    let exact = analytic_topk(&query, &data_vectors, k);
    let exact_ids: Vec<i32> = exact.iter().map(|(id, _)| *id as i32).collect();
    let exact_scores: Vec<f32> = exact.iter().map(|(_, d)| l2_score(*d)).collect();
    assert_eq!(ann_order, vec![5, 4, 3], "fixture guard: ANN order");
    assert_eq!(exact_ids, vec![0, 1, 2], "fixture guard: exact order");
    assert_ne!(
        ann_order, exact_ids,
        "fixture is only discriminating if ANN order differs from exact order"
    );

    // Build a table whose ANN segment encodes the perturbed order while the data
    // file holds the true vectors, and confirm the segment really recalls the
    // perturbed order before the read path runs.
    let schema = pk_vector_schema();
    let (_tmp, table, file_io, location, indexed_meta, partition, bucket) =
        write_schema_and_data(&schema, &data_vectors).await;
    let data_file_name = indexed_meta.file_name.clone();
    let row_count = indexed_meta.row_count;

    let index_file_name = "vector-ivf-flat-pkvector-rerank.index".to_string();
    let index_file_size =
        write_ann_segment(&file_io, &location, &index_file_name, &index_vectors).await;
    {
        let bytes = file_io
            .new_input(&format!("{location}/index/{index_file_name}"))
            .unwrap()
            .read()
            .await
            .unwrap();
        // The segment recalls the perturbed order, not the true-data order.
        assert_segment_reads_back(&bytes, &query, &analytic_topk(&query, &index_vectors, k));
    }

    let vector_field_id = schema
        .fields()
        .iter()
        .find(|f| f.name() == VECTOR_COLUMN)
        .expect("vector field present")
        .id();
    let index_file = IndexFileMeta {
        index_type: INDEX_TYPE.to_string(),
        file_name: index_file_name,
        file_size: i64::try_from(index_file_size).unwrap(),
        row_count: i32::try_from(row_count).unwrap(),
        deletion_vectors_ranges: None,
        global_index_meta: Some(GlobalIndexMeta {
            row_range_start: 0,
            row_range_end: row_count - 1,
            index_field_id: vector_field_id,
            extra_field_ids: None,
            source_meta: Some(source_meta_bytes(
                indexed_meta.level,
                &[(&data_file_name, row_count)],
            )),
            index_meta: None,
            build_schema_id: None,
        }),
    };
    let mut message = CommitMessage::new(partition, bucket, vec![indexed_meta]);
    message.new_index_files = vec![index_file];
    TableCommit::new(table.clone(), "pkvector-rerank".to_string())
        .commit(vec![message])
        .await
        .unwrap();

    // Sanity anchor: with NO refine factor the same fixture emits the raw ANN
    // recall order [5, 4, 3] (the rerank never runs), so any reordering below is
    // attributable to the refine factor and not to the fixture happening to agree.
    let (unset_ids, _unset_scores, _unset_batches) =
        read_id_and_scores_with_options(&table, query.to_vec(), k, HashMap::new()).await;
    assert_eq!(
        unset_ids, ann_order,
        "without a refine factor the rows must stay in raw ANN order [5, 4, 3]"
    );

    // With the refine factor set, the emitted rows are the EXACT top-3 [0, 1, 2],
    // not the approximate order [5, 4, 3]: the rerank reordered them.
    let (ids, scores, batches) =
        read_id_and_scores_with_options(&table, query.to_vec(), k, refine_factor_option(2)).await;
    assert_eq!(
        ids, exact_ids,
        "refine factor must reorder rows to exact top-3 [0, 1, 2], not ANN order [5, 4, 3]"
    );

    // Row content: each emitted row's vector equals the TRUE data vector at that
    // physical position (the rerank read the data file, not the ANN segment).
    let got_vectors = collect_vectors(&batches);
    assert_eq!(got_vectors.len(), k, "three rows expected");
    for (row_idx, (id, _)) in exact.iter().enumerate() {
        assert_eq!(
            got_vectors[row_idx],
            data_vectors[*id as usize].to_vec(),
            "materialized vector for row id {id} diverges from true data"
        );
    }

    // Scores are computed from the recomputed exact distances over the true data.
    assert_eq!(scores.len(), k);
    for (got, want) in scores.iter().zip(&exact_scores) {
        assert!(
            (got - want).abs() < 1e-4,
            "reranked score diverges: got {got}, want {want}"
        );
    }
}

/// Gating: with a positive refine factor set but a search that yields ZERO
/// indexed (ANN) candidates, the exact rerank must be skipped — no error, no
/// spurious position read — and the exact-fallback rows come through unchanged.
///
/// The table carries a compacted, non-level-0 data file but NO ANN index segment,
/// and `global-index.search-mode = full` enables the exact data-file fallback. The
/// search therefore produces exact-fallback candidates while `search.indexed` is
/// empty, which is exactly the `refine_factor > 0 && !search.indexed.is_empty()`
/// gate's short-circuit path. The emitted rows must be the exact top-3 the fallback
/// found, identical to what a refine-unset run would emit.
// Gated off Windows for the same `file://` tempdir reason as the tests above.
#[cfg(not(windows))]
#[tokio::test]
async fn pk_vector_refine_factor_with_no_indexed_candidates_is_noop() {
    // Reuse the discriminating fixture's true-data layout so best-first order
    // [5, 1, 3] is distinct from ascending position, proving the exact fallback
    // ranked correctly rather than emitting rows in file order.
    let (query, data_vectors) = fixture_discriminating();
    let k = 3;
    let exact = analytic_topk(&query, &data_vectors, k);
    let exact_ids: Vec<i32> = exact.iter().map(|(id, _)| *id as i32).collect();
    let exact_scores: Vec<f32> = exact.iter().map(|(_, d)| l2_score(*d)).collect();
    assert_eq!(exact_ids, vec![5, 1, 3], "fixture guard: exact order");

    // FULL search mode enables the exact data-file fallback; no ANN segment is
    // committed, so the ANN candidate list stays empty.
    let schema = pk_vector_schema_with(&[("global-index.search-mode", "full")]);
    let (_tmp, table, _file_io, _location, indexed_meta, partition, bucket) =
        write_schema_and_data(&schema, &data_vectors).await;

    // Commit the data file WITHOUT any index segment: the plan has a searchable
    // bucket but zero ANN segments -> zero indexed candidates.
    let message = CommitMessage::new(partition, bucket, vec![indexed_meta]);
    TableCommit::new(table.clone(), "pkvector-rerank-noop".to_string())
        .commit(vec![message])
        .await
        .unwrap();

    // A positive refine factor is set, but with no indexed candidates the rerank is
    // gated off: execute_read must not error, and returns the exact-fallback rows.
    let (ids, scores, batches) =
        read_id_and_scores_with_options(&table, query.to_vec(), k, refine_factor_option(2)).await;
    assert_eq!(
        ids, exact_ids,
        "with no indexed candidates the exact-fallback rows must pass through unchanged"
    );

    let got_vectors = collect_vectors(&batches);
    assert_eq!(got_vectors.len(), k, "three rows expected");
    for (row_idx, (id, _)) in exact.iter().enumerate() {
        assert_eq!(
            got_vectors[row_idx],
            data_vectors[*id as usize].to_vec(),
            "materialized vector for row id {id} diverges from true data"
        );
    }
    assert_eq!(scores.len(), k);
    for (got, want) in scores.iter().zip(&exact_scores) {
        assert!(
            (got - want).abs() < 1e-4,
            "exact-fallback score diverges: got {got}, want {want}"
        );
    }
}

/// An invalid refine factor must fail loud even when the table is empty: the
/// factor is resolved before planning, so config validity does not depend on
/// whether the table currently has searchable data. Regression for a resolution
/// that ran only after the empty-plan early return. Covers both invalid forms
/// the reviewer named (`0` and a non-integer) and both option sources (query and
/// table), since the fix moved the resolution of both sources ahead of planning.
#[cfg(not(target_os = "windows"))]
#[tokio::test]
async fn pk_vector_invalid_refine_factor_fails_loud_on_empty_table() {
    // Persist only the schema (optionally with extra table options): no snapshot,
    // so the PK-vector plan is empty.
    async fn empty_table(file_io: &FileIO, location: &str, extra: &[(&str, &str)]) -> Table {
        file_io.mkdirs(&format!("{location}/schema")).await.unwrap();
        let schema = pk_vector_schema_with(extra);
        file_io
            .new_output(&format!("{location}/schema/schema-{}", schema.id()))
            .unwrap()
            .write(Bytes::from(serde_json::to_vec(&schema).unwrap()))
            .await
            .unwrap();
        open_table(file_io, location).await
    }

    let refine_key = format!("fields.{VECTOR_COLUMN}.ivf.refine-factor");

    // Case 1: non-integer via a QUERY option -> "Invalid ... Must be an integer".
    {
        let tmp = tempfile::tempdir().expect("create temp dir");
        let location = format!("file://{}", tmp.path().display());
        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table = empty_table(&file_io, &location, &[]).await;
        let mut builder = table.new_vector_search_builder();
        builder
            .with_vector_column(VECTOR_COLUMN)
            .with_query_vector(vec![0.0f32; DIM])
            .with_limit(3)
            .with_options(HashMap::from([(refine_key.clone(), "abc".to_string())]));
        let err = match builder.execute_read().await {
            Ok(_) => panic!("a non-integer refine factor must fail loud on an empty table"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("Invalid vector refine factor"),
            "expected a non-integer refine-factor error, got: {err}"
        );
    }

    // Case 2: explicit `0` via a QUERY option -> "must be positive" (0 means
    // "omit"; it cannot be requested).
    {
        let tmp = tempfile::tempdir().expect("create temp dir");
        let location = format!("file://{}", tmp.path().display());
        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table = empty_table(&file_io, &location, &[]).await;
        let mut builder = table.new_vector_search_builder();
        builder
            .with_vector_column(VECTOR_COLUMN)
            .with_query_vector(vec![0.0f32; DIM])
            .with_limit(3)
            .with_options(HashMap::from([(refine_key.clone(), "0".to_string())]));
        let err = match builder.execute_read().await {
            Ok(_) => panic!("a zero refine factor must fail loud on an empty table"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("must be positive"),
            "expected a positive-refine-factor error, got: {err}"
        );
    }

    // Case 3: non-integer via a TABLE option (the fix moved table-option
    // resolution ahead of planning too).
    {
        let tmp = tempfile::tempdir().expect("create temp dir");
        let location = format!("file://{}", tmp.path().display());
        let file_io = FileIOBuilder::new("file").build().unwrap();
        let table = empty_table(&file_io, &location, &[(refine_key.as_str(), "abc")]).await;
        let mut builder = table.new_vector_search_builder();
        builder
            .with_vector_column(VECTOR_COLUMN)
            .with_query_vector(vec![0.0f32; DIM])
            .with_limit(3);
        let err = match builder.execute_read().await {
            Ok(_) => panic!("a non-integer table refine factor must fail loud on an empty table"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("Invalid vector refine factor"),
            "expected a non-integer refine-factor error, got: {err}"
        );
    }
}
