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

//! End-to-end acceptance gate for BATCH primary-key vector search.
//!
//! Builds a complete, self-contained primary-key vector table entirely from Rust
//! (mirroring `pk_vector_baseline_test`), then reads it back through the public
//! batch surface `new_batch_vector_search_builder().execute_read()` and asserts:
//!   - batch-of-one == the single-query `execute_read`;
//!   - an N-query batch yields one stream per query, each matching the
//!     corresponding independent single-query read (arity + order + independence);
//!   - an empty snapshot yields N empty streams (arity preserved);
//!   - the PK batch `execute()` (scored) fails loud, directing to `execute_read`;
//!   - a shared residual filter reshapes every query's rows with no cross-query
//!     bleed.

use std::collections::HashMap;

use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
use arrow_array::{Array, ArrayRef, Float32Array, Int32Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use bytes::Bytes;
use futures::TryStreamExt;
use paimon::catalog::Identifier;
use paimon::io::{FileIO, FileIOBuilder};
use paimon::spec::{
    ArrayType, DataFileMeta, DataType, Datum, FloatType, GlobalIndexMeta, IndexFileMeta, IntType,
    Predicate, PredicateBuilder, Schema, TableSchema, VectorType,
};
use paimon::table::{ArrowRecordBatchStream, CommitMessage, SchemaManager, Table, TableCommit};
use paimon_vindex_core::index::{VectorIndexConfig, VectorIndexTrainer, VectorIndexWriter};
use paimon_vindex_core::io::PosWriter;
use std::sync::Arc;

const DIM: usize = 4;
const VECTOR_COLUMN: &str = "embedding";
const INDEX_TYPE: &str = "ivf-flat";

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

fn pk_vector_array_schema() -> TableSchema {
    let mut builder = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            VECTOR_COLUMN,
            DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
        )
        .primary_key(["id"]);
    for (k, v) in table_options() {
        builder = builder.option(k, v);
    }
    builder = builder.option(format!("{INDEX_TYPE}.dimension"), DIM.to_string());
    TableSchema::new(0, &builder.build().unwrap())
}

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

fn source_meta_bytes(data_level: i32, files: &[(&str, i64)]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&1i32.to_be_bytes());
    out.extend_from_slice(&data_level.to_be_bytes());
    out.extend_from_slice(&(files.len() as i32).to_be_bytes());
    for (name, rows) in files {
        out.extend_from_slice(&java_write_utf(name));
        out.extend_from_slice(&rows.to_be_bytes());
    }
    out
}

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

async fn open_table(file_io: &FileIO, location: &str) -> Table {
    let schema = SchemaManager::new(file_io.clone(), location.to_string())
        .latest()
        .await
        .expect("failed to list schemas")
        .expect("table has no schema");
    Table::new(
        file_io.clone(),
        Identifier::new("default", "pkvector_batch"),
        location.to_string(),
        (*schema).clone(),
        None,
    )
}

/// Build a complete self-contained primary-key vector table over `vectors` with a
/// real vindex ANN segment committed in one snapshot. Returns the temp dir (kept
/// alive by the caller) and the opened table.
async fn build_table(vectors: &[[f32; DIM]]) -> (tempfile::TempDir, Table) {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let location = format!("file://{}", tmp.path().display());
    let file_io = FileIOBuilder::new("file").build().unwrap();

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

    let write_builder = table.new_write_builder();
    let mut writer = write_builder.new_write().unwrap();
    writer
        .write_arrow_batch(&data_batch(vectors))
        .await
        .unwrap();
    let write_messages = writer.prepare_commit().await.unwrap();
    let written = &write_messages[0];
    let base_meta = written.new_files[0].clone();
    let bucket = written.bucket;
    let partition = written.partition.clone();
    let data_file_name = base_meta.file_name.clone();
    let row_count = base_meta.row_count;

    let indexed_meta = DataFileMeta {
        level: 1,
        file_source: Some(1),
        first_row_id: Some(0),
        ..base_meta
    };

    let index_file_name = "vector-ivf-flat-pkvector-batch.index".to_string();
    let index_file_size = write_ann_segment(&file_io, &location, &index_file_name, vectors).await;

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

    let mut message = CommitMessage::new(partition, bucket, vec![indexed_meta]);
    message.new_index_files = vec![index_file];
    TableCommit::new(table.clone(), "pkvector-batch".to_string())
        .commit(vec![message])
        .await
        .unwrap();

    (tmp, table)
}

/// Build a bare table with a schema but NO snapshot (empty). Used for the
/// empty-snapshot arity test.
async fn build_empty_table() -> (tempfile::TempDir, Table) {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let location = format!("file://{}", tmp.path().display());
    let file_io = FileIOBuilder::new("file").build().unwrap();
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
    (tmp, table)
}

async fn build_empty_array_table() -> (tempfile::TempDir, Table) {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let location = format!("file://{}", tmp.path().display());
    let file_io = FileIOBuilder::new("file").build().unwrap();
    for dir in ["schema", "snapshot", "manifest", "index"] {
        file_io.mkdirs(&format!("{location}/{dir}")).await.unwrap();
    }
    let schema = pk_vector_array_schema();
    file_io
        .new_output(&format!("{location}/schema/schema-{}", schema.id()))
        .unwrap()
        .write(Bytes::from(serde_json::to_vec(&schema).unwrap()))
        .await
        .unwrap();
    let table = open_table(&file_io, &location).await;
    (tmp, table)
}

/// Flatten a materialized best-first stream into per-row `(id, score)` tuples.
async fn drain_ids_and_scores(stream: ArrowRecordBatchStream) -> (Vec<i32>, Vec<f32>) {
    let batches = stream
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
    (ids, scores)
}

/// Single-query `execute_read` into `(id, score)` tuples.
async fn single_read(table: &Table, query: Vec<f32>, limit: usize) -> (Vec<i32>, Vec<f32>) {
    let mut builder = table.new_vector_search_builder();
    builder
        .with_vector_column(VECTOR_COLUMN)
        .with_query_vector(query)
        .with_limit(limit);
    let stream = builder
        .execute_read()
        .await
        .expect("single-query read failed");
    drain_ids_and_scores(stream).await
}

fn fixture() -> Vec<[f32; DIM]> {
    // Distances make each query's top-k order unique.
    vec![
        [0.0, 4.0, 0.0, 0.0], // pos 0
        [8.0, 0.0, 0.0, 0.0], // pos 1
        [0.0, 0.0, 5.0, 0.0], // pos 2
        [7.0, 0.0, 0.0, 0.0], // pos 3
        [0.0, 0.0, 0.0, 6.0], // pos 4
        [9.0, 0.0, 0.0, 0.0], // pos 5
    ]
}

// Gated off Windows for the same `file://` tempdir reason as `pk_vector_baseline_test`.
#[cfg(not(windows))]
#[tokio::test]
async fn batch_of_one_equals_single_read() {
    let vectors = fixture();
    let (_tmp, table) = build_table(&vectors).await;
    let query = vec![10.0, 0.0, 0.0, 0.0];

    let (single_ids, single_scores) = single_read(&table, query.clone(), 3).await;

    let mut builder = table.new_batch_vector_search_builder();
    let mut streams = builder
        .with_vector_column(VECTOR_COLUMN)
        .with_query_vectors(vec![query])
        .with_limit(3)
        .execute_read()
        .await
        .expect("batch-of-one read failed");
    assert_eq!(streams.len(), 1, "batch-of-one yields exactly one stream");
    let (batch_ids, batch_scores) = drain_ids_and_scores(streams.remove(0)).await;

    assert_eq!(batch_ids, single_ids);
    assert_eq!(batch_scores.len(), single_scores.len());
    for (got, want) in batch_scores.iter().zip(&single_scores) {
        assert!((got - want).abs() < 1e-6, "score diverges: {got} vs {want}");
    }
}

#[cfg(not(windows))]
#[tokio::test]
async fn n_query_batch_matches_n_independent_single_reads() {
    let vectors = fixture();
    let (_tmp, table) = build_table(&vectors).await;
    let queries = vec![
        vec![10.0, 0.0, 0.0, 0.0], // nearest the x-axis vectors
        vec![0.0, 0.0, 6.0, 0.0],  // nearest pos 2 / pos 4
        vec![0.0, 5.0, 0.0, 0.0],  // nearest pos 0
    ];

    // Independent single reads, one per query.
    let mut expected = Vec::new();
    for q in &queries {
        expected.push(single_read(&table, q.clone(), 3).await);
    }

    let mut builder = table.new_batch_vector_search_builder();
    let streams = builder
        .with_vector_column(VECTOR_COLUMN)
        .with_query_vectors(queries.clone())
        .with_limit(3)
        .execute_read()
        .await
        .expect("batch read failed");
    assert_eq!(
        streams.len(),
        queries.len(),
        "one stream per query, in input order"
    );

    for (i, stream) in streams.into_iter().enumerate() {
        let (ids, scores) = drain_ids_and_scores(stream).await;
        let (want_ids, want_scores) = &expected[i];
        assert_eq!(&ids, want_ids, "query {i} rows must match its single read");
        assert_eq!(scores.len(), want_scores.len());
        for (got, want) in scores.iter().zip(want_scores) {
            assert!(
                (got - want).abs() < 1e-6,
                "query {i} score diverges: {got} vs {want}"
            );
        }
    }
}

#[cfg(not(windows))]
#[tokio::test]
async fn empty_snapshot_yields_n_empty_streams() {
    let (_tmp, table) = build_empty_table().await;
    let queries = vec![
        vec![1.0, 0.0, 0.0, 0.0],
        vec![0.0, 1.0, 0.0, 0.0],
        vec![0.0, 0.0, 1.0, 0.0],
    ];

    let mut builder = table.new_batch_vector_search_builder();
    let streams = builder
        .with_vector_column(VECTOR_COLUMN)
        .with_query_vectors(queries.clone())
        .with_limit(3)
        .execute_read()
        .await
        .expect("empty-snapshot batch read failed");
    assert_eq!(
        streams.len(),
        queries.len(),
        "arity preserved: one empty stream per query"
    );
    for stream in streams {
        let (ids, _scores) = drain_ids_and_scores(stream).await;
        assert!(ids.is_empty(), "no-hit query must yield an empty stream");
    }
}

#[cfg(not(windows))]
#[tokio::test]
async fn pk_batch_execute_scored_fails_loud() {
    let vectors = fixture();
    let (_tmp, table) = build_table(&vectors).await;
    let err = table
        .new_batch_vector_search_builder()
        .with_vector_column(VECTOR_COLUMN)
        .with_query_vectors(vec![vec![10.0, 0.0, 0.0, 0.0]])
        .with_limit(3)
        .execute()
        .await
        .expect_err("PK batch execute() must fail loud");
    assert!(
        format!("{err:?}").contains("execute_read"),
        "PK batch execute() should point at execute_read, got: {err:?}"
    );
}

/// A shared residual filter reshapes every query's rows (excluding low ids) with
/// no cross-query bleed: each query still ranks only the residual-allowed rows by
/// its own distance.
#[cfg(not(windows))]
#[tokio::test]
async fn shared_residual_filter_applies_per_query_without_bleed() {
    // Fixture where filtering id >= 3 changes each query's result set.
    let vectors = vec![
        [10.0, 0.0, 0.0, 0.0], // pos 0
        [9.0, 0.0, 0.0, 0.0],  // pos 1
        [8.0, 0.0, 0.0, 0.0],  // pos 2
        [5.0, 0.0, 0.0, 0.0],  // pos 3
        [7.0, 0.0, 0.0, 0.0],  // pos 4
        [6.0, 0.0, 0.0, 0.0],  // pos 5
    ];
    let (_tmp, table) = build_table(&vectors).await;
    let threshold = 3;
    let build_filter = || -> Predicate {
        PredicateBuilder::new(table.schema().fields())
            .greater_or_equal("id", Datum::Int(threshold))
            .expect("build residual predicate on id")
    };

    let queries = vec![vec![10.0, 0.0, 0.0, 0.0], vec![5.0, 0.0, 0.0, 0.0]];

    // Expected per-query truth: rank residual-allowed rows (id >= 3) by distance.
    let expected: Vec<Vec<i32>> = queries
        .iter()
        .map(|q| {
            let mut ranked: Vec<(u64, f32)> = analytic_topk(q, &vectors, vectors.len())
                .into_iter()
                .filter(|(id, _)| *id >= threshold as u64)
                .collect();
            ranked.truncate(3);
            ranked.iter().map(|(id, _)| *id as i32).collect()
        })
        .collect();
    // Guard: the two queries produce different residual orders (no accidental
    // bleed-agnostic fixture).
    assert_ne!(
        expected[0], expected[1],
        "queries must differ post-residual"
    );

    let mut builder = table.new_batch_vector_search_builder();
    let streams = builder
        .with_vector_column(VECTOR_COLUMN)
        .with_query_vectors(queries.clone())
        .with_limit(3)
        .with_filter(build_filter())
        .execute_read()
        .await
        .expect("residual batch read failed");
    assert_eq!(streams.len(), queries.len());

    for (i, stream) in streams.into_iter().enumerate() {
        let (ids, _scores) = drain_ids_and_scores(stream).await;
        for &id in &ids {
            assert!(
                id >= threshold,
                "query {i} returned id {id} failing residual id >= {threshold}"
            );
        }
        assert_eq!(ids, expected[i], "query {i} residual order must be its own");
    }
}

/// A table with no primary-key vector index is not a valid target for the batch
/// materialized read: it produces scored global row ids, not physical rows. The
/// batch `execute_read` must reject it up front rather than silently route it
/// through the primary-key materialization path.
// Gated off Windows for the same `file://` tempdir reason as `pk_vector_baseline_test`.
#[cfg(not(windows))]
#[tokio::test]
async fn batch_execute_read_on_non_pk_vector_table_fails_loud() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let location = format!("file://{}", tmp.path().display());
    let file_io = FileIOBuilder::new("file").build().unwrap();
    for dir in ["schema", "snapshot", "manifest", "index"] {
        file_io.mkdirs(&format!("{location}/{dir}")).await.unwrap();
    }

    // A plain primary-key table over a vector column but WITHOUT the
    // `pk-vector.index.columns` option, so no PK-vector index is configured.
    let mut builder = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            VECTOR_COLUMN,
            DataType::Vector(
                VectorType::try_new(true, DIM as u32, DataType::Float(FloatType::new())).unwrap(),
            ),
        )
        .primary_key(["id"]);
    builder = builder.option("bucket".to_string(), "1".to_string());
    let schema = TableSchema::new(0, &builder.build().unwrap());
    file_io
        .new_output(&format!("{location}/schema/schema-{}", schema.id()))
        .unwrap()
        .write(Bytes::from(serde_json::to_vec(&schema).unwrap()))
        .await
        .unwrap();

    let table = open_table(&file_io, &location).await;

    let mut batch = table.new_batch_vector_search_builder();
    let result = batch
        .with_vector_column(VECTOR_COLUMN)
        .with_query_vectors(vec![vec![1.0, 0.0, 0.0, 0.0]])
        .with_limit(3)
        .execute_read()
        .await;
    let err = match result {
        Ok(_) => panic!("batch execute_read on a non-PK-vector table must fail loud"),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("primary-key vector path"),
        "expected a message directing to the primary-key vector path, got: {err}"
    );
}

/// A malformed query (wrong dimension) must fail loud even when the plan is
/// empty. Query validation runs before planning, so an empty snapshot cannot
/// mask a bad query by returning empty streams.
// Gated off Windows for the same `file://` tempdir reason as `pk_vector_baseline_test`.
#[cfg(not(windows))]
#[tokio::test]
async fn empty_snapshot_still_rejects_malformed_query() {
    let (_tmp, table) = build_empty_table().await;
    // DIM is 4; a 3-element query is wrong-dimension.
    let queries = vec![vec![1.0, 0.0, 0.0], vec![0.0, 1.0, 0.0, 0.0]];

    let mut builder = table.new_batch_vector_search_builder();
    let result = builder
        .with_vector_column(VECTOR_COLUMN)
        .with_query_vectors(queries)
        .with_limit(3)
        .execute_read()
        .await;
    let err = match result {
        Ok(_) => panic!("a wrong-dimension query must fail loud even on an empty snapshot"),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("dimension does not match"),
        "expected a dimension-mismatch error, got: {err}"
    );
}

/// ARRAY<FLOAT> is a valid PK-vector column. Batch query validation must still
/// reject NaN/Inf before planning, otherwise an empty snapshot would mask the bad
/// input by returning normal empty streams.
// Gated off Windows for the same `file://` tempdir reason as `pk_vector_baseline_test`.
#[cfg(not(windows))]
#[tokio::test]
async fn empty_array_snapshot_still_rejects_non_finite_query() {
    let (_tmp, table) = build_empty_array_table().await;
    let queries = vec![vec![1.0, f32::NAN, 0.0, 0.0], vec![0.0, 1.0, 0.0, 0.0]];

    let mut builder = table.new_batch_vector_search_builder();
    let result = builder
        .with_vector_column(VECTOR_COLUMN)
        .with_query_vectors(queries)
        .with_limit(3)
        .execute_read()
        .await;
    let err = match result {
        Ok(_) => panic!("a NaN query must fail loud for ARRAY<FLOAT> even on an empty snapshot"),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("must be finite"),
        "expected a finite-query error, got: {err}"
    );
}

/// A non-positive limit must fail loud even when the plan is empty. Limit
/// validation runs before planning, so an empty snapshot cannot mask a zero
/// limit by returning empty streams.
// Gated off Windows for the same `file://` tempdir reason as `pk_vector_baseline_test`.
#[cfg(not(windows))]
#[tokio::test]
async fn empty_snapshot_still_rejects_zero_limit() {
    let (_tmp, table) = build_empty_table().await;

    let mut builder = table.new_batch_vector_search_builder();
    let result = builder
        .with_vector_column(VECTOR_COLUMN)
        .with_query_vectors(vec![vec![1.0, 0.0, 0.0, 0.0]])
        .with_limit(0)
        .execute_read()
        .await;
    let err = match result {
        Ok(_) => panic!("a zero limit must fail loud even on an empty snapshot"),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("limit must be positive"),
        "expected a positive-limit error, got: {err}"
    );
}

/// A filter set on a batch `execute()` (the scored / data-evolution path) must
/// fail loud rather than silently drop the predicate: that path never reads
/// physical rows, so it cannot honor a residual filter. Mirrors the single-query
/// `execute_scored` guard.
// Gated off Windows for the same `file://` tempdir reason as `pk_vector_baseline_test`.
#[cfg(not(windows))]
#[tokio::test]
async fn batch_execute_with_filter_on_non_pk_vector_table_fails_loud() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let location = format!("file://{}", tmp.path().display());
    let file_io = FileIOBuilder::new("file").build().unwrap();
    for dir in ["schema", "snapshot", "manifest", "index"] {
        file_io.mkdirs(&format!("{location}/{dir}")).await.unwrap();
    }

    // A plain table over a vector column WITHOUT a PK-vector index, so a scored
    // batch `execute()` takes the data-evolution fall-through path.
    let mut builder = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            VECTOR_COLUMN,
            DataType::Vector(
                VectorType::try_new(true, DIM as u32, DataType::Float(FloatType::new())).unwrap(),
            ),
        )
        .primary_key(["id"]);
    builder = builder.option("bucket".to_string(), "1".to_string());
    let schema = TableSchema::new(0, &builder.build().unwrap());
    file_io
        .new_output(&format!("{location}/schema/schema-{}", schema.id()))
        .unwrap()
        .write(Bytes::from(serde_json::to_vec(&schema).unwrap()))
        .await
        .unwrap();

    let table = open_table(&file_io, &location).await;
    let filter = PredicateBuilder::new(table.schema().fields())
        .greater_or_equal("id", Datum::Int(1))
        .expect("build filter on id");

    let mut batch = table.new_batch_vector_search_builder();
    let err = batch
        .with_vector_column(VECTOR_COLUMN)
        .with_query_vectors(vec![vec![1.0, 0.0, 0.0, 0.0]])
        .with_limit(3)
        .with_filter(filter)
        .execute()
        .await
        .expect_err("a filter on the data-evolution batch path must fail loud");
    assert!(
        err.to_string()
            .contains("only supported on the primary-key vector path"),
        "expected a filter-unsupported error, got: {err}"
    );
}
