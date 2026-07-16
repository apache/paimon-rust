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
//! (`execute_read()` -> Arrow batches, best-first order, `_PKEY_VECTOR_SCORE`).
//!
//! Why Rust-built rather than a committed cross-language fixture: the Java
//! primary-key vector ANN segment is an opaque native Lumina format that cannot
//! be reproduced byte-for-byte here, whereas the Rust read path is backed by the
//! vindex (IVF) segment format. This test therefore validates the Rust read path
//! against real vindex IVF segment bytes it produces itself, with no committed
//! binaries and nothing skipped.
//!
//! Two constraints the primary-key read path enforces are satisfied by hand
//! (mirroring Java `PrimaryKeyIndexSourcePolicy` and `PkVectorSourceMeta`):
//!   1. Only a compacted (`file_source == COMPACT`), non-level-0 data file backs
//!      the index, so the written file's meta is cloned with `level = 1` and
//!      `file_source = Some(1)`.
//!   2. `GlobalIndexMeta.source_meta` must be the Java `PkVectorSourceMeta` frame
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

/// Table options that route searches into the primary-key vector branch
/// (`VectorSearchBuilder::execute_primary_key_vector_search`). Default search
/// mode is FAST, so only the ANN segment is consulted (no exact fallback).
fn table_options() -> Vec<(String, String)> {
    vec![
        ("bucket".to_string(), "1".to_string()),
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
/// round-trip helper in `PkVectorSourceMeta`'s own tests.
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

/// Assemble the `_SOURCE_META` frame the way Java `PkVectorSourceMeta` writes it
/// and `PkVectorSourceMeta::deserialize` expects: `i32-BE version=1`, `i32-BE
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
    // level > 0 + file_source == COMPACT (1). Pin first_row_id = 0 so the global
    // row id equals the physical position.
    let indexed_meta = DataFileMeta {
        level: 1,
        file_source: Some(1),
        first_row_id: Some(0),
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

    // Constraint 2: GlobalIndexMeta.source_meta must be the Java PkVectorSourceMeta
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
        file_size: i32::try_from(index_file_size).unwrap(),
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
            let idx = b.schema().index_of("_PKEY_VECTOR_SCORE").unwrap();
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

    // Search path: execute_scored() -> row ids + scores.
    let result = table
        .new_vector_search_builder()
        .with_vector_column(VECTOR_COLUMN)
        .with_query_vector(query.to_vec())
        .with_limit(3)
        .execute_scored()
        .await
        .expect("primary-key vector search failed");

    assert_eq!(
        result.row_ids, expected_row_ids,
        "row ids diverge from the analytic expectation"
    );
    assert_eq!(
        result.scores.len(),
        expected_scores.len(),
        "score count diverges from the analytic expectation"
    );
    for (got, want) in result.scores.iter().zip(&expected_scores) {
        assert!(
            (got - want).abs() < 1e-4,
            "score diverges from the analytic expectation: got {got}, want {want}"
        );
    }

    // Search-and-read: execute_read() materializes the matching rows best-first
    // with a `_PKEY_VECTOR_SCORE` column, hiding `_ROW_ID`/`_PKEY_VECTOR_POSITION`.
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
/// row content (id + vector values) and the aligned `_PKEY_VECTOR_SCORE`, with no
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

    // Score alignment: `_PKEY_VECTOR_SCORE` matches metric.distance_to_score for
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
