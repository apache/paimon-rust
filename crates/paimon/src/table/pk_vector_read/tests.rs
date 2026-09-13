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

use super::*;
use crate::catalog::Identifier;
use crate::io::{FileIO, FileIOBuilder};
use crate::lumina::LuminaIndexMeta;
use crate::spec::stats::BinaryTableStats;
use crate::spec::{
    BinaryRow, DataField, DataFileMeta, DataType, Datum, FloatType, IntType, Predicate,
    PredicateBuilder, Schema, TableSchema,
};
use crate::table::data_file_reader::DataFileReader;
use crate::table::pk_vector_orchestrator::{PkVectorCandidate, PkVectorSearchSplit};
use crate::table::source::DataSplitBuilder;
use crate::table::vector_scan::Scan;
use crate::table::vector_search_test_utils::{build_vindex_segment_bytes, pk_vector_table};
use crate::table::{Table, TableCommit, TableWrite};
use crate::vindex::pkvector::bucket::BucketAnnSegment;
use crate::vindex::pkvector::metric::VectorSearchMetric;
use crate::vindex::IVF_FLAT_IDENTIFIER;
use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use paimon_vindex_core::index::VectorIndexReader as VIndexReader;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::{Arc, Mutex, Once};

const VECTOR_SEARCH_LOG_TARGET: &str = "paimon::vector_search";

static VECTOR_SEARCH_TEST_LOGGER: VectorSearchTestLogger = VectorSearchTestLogger;

static VECTOR_SEARCH_TEST_LOGS: Mutex<Vec<String>> = Mutex::new(Vec::new());

struct VectorSearchTestLogger;

impl log::Log for VectorSearchTestLogger {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.target() == VECTOR_SEARCH_LOG_TARGET && metadata.level() <= log::Level::Debug
    }

    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata()) {
            VECTOR_SEARCH_TEST_LOGS
                .lock()
                .unwrap()
                .push(record.args().to_string());
        }
    }

    fn flush(&self) {}
}

fn reset_vector_search_test_logs() {
    static INIT: Once = Once::new();
    INIT.call_once(|| log::set_logger(&VECTOR_SEARCH_TEST_LOGGER).unwrap());
    log::set_max_level(log::LevelFilter::Debug);
    VECTOR_SEARCH_TEST_LOGS.lock().unwrap().clear();
}

fn pk_data_file(name: &str, row_count: i64, first_row_id: Option<i64>) -> DataFileMeta {
    DataFileMeta {
        file_name: name.to_string(),
        file_size: 1,
        row_count,
        min_key: Vec::new(),
        max_key: Vec::new(),
        key_stats: BinaryTableStats::empty(),
        value_stats: BinaryTableStats::empty(),
        min_sequence_number: 0,
        max_sequence_number: 0,
        schema_id: 1,
        level: 0,
        extra_files: Vec::new(),
        creation_time: None,
        delete_row_count: None,
        embedded_index: None,
        file_source: None,
        value_stats_cols: None,
        external_path: None,
        first_row_id,
        write_cols: None,
        column_max_sequence_numbers: None,
    }
}

fn pk_search_split(bucket: i32, files: Vec<DataFileMeta>) -> PkVectorSearchSplit {
    PkVectorSearchSplit {
        data_split: DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(bucket)
            .with_bucket_path(format!("memory:/t/bucket-{bucket}"))
            .with_total_buckets(1)
            .with_data_files(files)
            .build()
            .unwrap(),
        ann_segments: Vec::new(),
        active_files: Vec::new(),
    }
}

fn pk_candidate(
    split_index: usize,
    bucket: i32,
    file: &str,
    pos: i64,
    distance: f32,
) -> PkVectorCandidate {
    PkVectorCandidate {
        split_index,
        partition: BinaryRow::new(0),
        bucket,
        data_file_name: file.to_string(),
        row_position: pos,
        distance,
    }
}

// Candidate with a fixed empty (arity-0) partition and bucket 0, keyed only by
// (split_index, file, position) — the dimensions the rerank core groups on.
fn cand_at(split_index: usize, file: &str, pos: i64, dist: f32) -> PkVectorCandidate {
    pk_candidate(split_index, 0, file, pos, dist)
}

/// The single data-file name every rerank fixture writes.
const RERANK_FILE: &str = "part-0.parquet";

/// Serialize a Paimon deletion-vector blob covering `deleted_rows` and write it
/// at `path`, returning the matching `DeletionFile`. Byte layout mirrors the
/// position-read tests: `[length][magic][roaring bitmap][0]`.
async fn write_deletion_blob(
    file_io: &FileIO,
    path: &str,
    deleted_rows: &[u32],
) -> crate::table::source::DeletionFile {
    use roaring::RoaringBitmap;

    const MAGIC_NUMBER: i32 = 1581511376;
    let mut bitmap = RoaringBitmap::new();
    for row in deleted_rows {
        bitmap.insert(*row);
    }
    let mut bitmap_bytes = Vec::new();
    bitmap.serialize_into(&mut bitmap_bytes).unwrap();
    let bitmap_length = 4 + bitmap_bytes.len() as i32;
    let mut blob = Vec::new();
    blob.extend_from_slice(&bitmap_length.to_be_bytes());
    blob.extend_from_slice(&MAGIC_NUMBER.to_be_bytes());
    blob.extend_from_slice(&bitmap_bytes);
    blob.extend_from_slice(&0i32.to_be_bytes());
    file_io
        .new_output(path)
        .unwrap()
        .write(bytes::Bytes::from(blob))
        .await
        .unwrap();
    crate::table::source::DeletionFile::new(
        path.to_string(),
        0,
        bitmap_length as i64,
        Some(deleted_rows.len() as i64),
    )
}

/// Write a single-file vector data file (`FixedSizeList<Float32>` of width
/// `dim`) holding `rows` (a `None` entry is a NULL vector row) as Parquet, and
/// return a vector-only `DataFileReader`, the enclosing `PkVectorSearchSplit`,
/// and the vector `DataField`. When `deleted_rows` is non-empty a deletion
/// vector covering those physical positions is attached to the split, so the
/// position read drops them exactly as `PkVectorIndexedSplitRead::read` does.
///
/// This is the position-only analogue of the old `ArrayReader`: rerank now
/// re-reads real stored rows through `PkVectorPositionRead`, so the fixtures
/// exercise that path rather than an in-memory preloaded column.
async fn vector_rerank_fixture(
    table_path: &str,
    dim: u32,
    rows: &[Option<Vec<f32>>],
    deleted_rows: &[u32],
) -> (DataFileReader, PkVectorSearchSplit, DataField) {
    use crate::arrow::build_target_arrow_schema;
    use crate::arrow::format::{FormatFileWriter, ParquetFormatWriter};
    use crate::spec::VectorType;
    use crate::table::schema_manager::SchemaManager;

    let vector_type = VectorType::try_new(true, dim, DataType::Float(FloatType::new())).unwrap();
    let vector_field = DataField::new(0, "embedding".to_string(), DataType::Vector(vector_type));
    let read_fields = vec![vector_field.clone()];
    let arrow_schema = build_target_arrow_schema(&read_fields).unwrap();

    let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), dim as i32).with_field(
        Arc::new(ArrowField::new("element", ArrowDataType::Float32, true)),
    );
    for row in rows {
        match row {
            Some(values) => {
                for v in values {
                    builder.values().append_value(*v);
                }
                builder.append(true);
            }
            None => {
                for _ in 0..dim {
                    builder.values().append_value(0.0);
                }
                builder.append(false);
            }
        }
    }
    let vec_array = builder.finish();
    let batch =
        arrow_array::RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(vec_array)]).unwrap();

    let file_io = FileIOBuilder::new("memory").build().unwrap();
    let bucket_path = format!("{table_path}/bucket-0");
    let output = file_io
        .new_output(&format!("{bucket_path}/{RERANK_FILE}"))
        .unwrap();
    let mut writer: Box<dyn FormatFileWriter> = Box::new(
        ParquetFormatWriter::new(
            &output,
            arrow_schema.clone(),
            "zstd",
            1,
            None,
            &HashMap::new(),
        )
        .await
        .unwrap(),
    );
    writer.write(&batch).await.unwrap();
    let file_size = writer.close().await.unwrap().file_size;

    let schema_id = 1;
    let file_meta = pk_data_file(RERANK_FILE, rows.len() as i64, Some(0));
    let file_meta = DataFileMeta {
        file_size: file_size as i64,
        schema_id,
        ..file_meta
    };

    let mut split_builder = DataSplitBuilder::new()
        .with_snapshot(1)
        .with_partition(BinaryRow::new(0))
        .with_bucket(0)
        .with_bucket_path(bucket_path)
        .with_total_buckets(1)
        .with_data_files(vec![file_meta]);
    if !deleted_rows.is_empty() {
        let df =
            write_deletion_blob(&file_io, &format!("{table_path}/index/dv-0"), deleted_rows).await;
        split_builder = split_builder.with_data_deletion_files(vec![Some(df)]);
    }
    let data_split = split_builder.build().unwrap();
    let split = PkVectorSearchSplit {
        data_split,
        ann_segments: Vec::new(),
        active_files: Vec::new(),
    };

    let schema_manager = SchemaManager::new(file_io.clone(), table_path.to_string());
    let reader = DataFileReader::new(
        file_io,
        schema_manager,
        schema_id,
        read_fields.clone(),
        read_fields,
        Vec::new(),
    );
    (reader, split, vector_field)
}

fn pk_split_with_lumina_segment(path: &str, metric: &str) -> PkVectorSearchSplit {
    let mut split = pk_search_split(0, vec![pk_data_file("file-a", 3, Some(0))]);
    let source_meta = crate::spec::PrimaryKeyIndexSourceMeta::new(
        1,
        vec![crate::spec::PrimaryKeyIndexSourceFile::new("file-a".to_string(), 3).unwrap()],
    )
    .unwrap();
    let mut segment = BucketAnnSegment::for_test(source_meta);
    segment.path = path.to_string();
    // Lumina stores its metric in the serialized index metadata blob, not in
    // the segment file bytes. `deserialize` requires both keys present.
    let meta = crate::lumina::LuminaIndexMeta::new(HashMap::from([
        ("index.dimension".to_string(), "2".to_string()),
        ("distance.metric".to_string(), metric.to_string()),
    ]));
    segment.index_meta = meta.serialize().unwrap();
    split.ann_segments = vec![segment];
    split
}

/// One Java `DataOutput#writeUTF` value (u16-BE length + modified UTF-8), used
/// to assemble the `PrimaryKeyIndexSourceMeta` frame below.
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

/// The Java `PrimaryKeyIndexSourceMeta` frame: `i32-BE version=1`, `i32-BE
/// data_level`, `i32-BE count`, then per source file a `writeUTF` name and an
/// `i64-BE` row count.
fn pk_source_meta_bytes(data_level: i32, files: &[(&str, i64)]) -> Vec<u8> {
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

/// Build a committed primary-key vector table (memory FS) over `vectors`
/// (dimension 2): write a real data file via the write path, promote its meta
/// to a compacted, non-level-0 file (the PK index-source precondition), then
/// build + commit a real vindex IVF-flat ANN segment naming that file. Single
/// bucket, `nlist = 1`, so the ANN search is exact. Returns the opened table,
/// ready for vector search.
async fn build_committed_pk_vector_table(vectors: &[[f32; 2]]) -> Table {
    use crate::spec::{GlobalIndexMeta, IndexFileMeta, VectorType};
    use crate::table::CommitMessage;
    use bytes::Bytes;
    use paimon_vindex_core::index::{VectorIndexConfig, VectorIndexTrainer, VectorIndexWriter};
    use paimon_vindex_core::io::PosWriter;

    const DIM: usize = 2;
    let table_path = "memory:/pk_vector_route_test";
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column(
            "embedding",
            DataType::Vector(
                VectorType::try_new(true, DIM as u32, DataType::Float(FloatType::new())).unwrap(),
            ),
        )
        .primary_key(["id"])
        .option("bucket", "1")
        .option("deletion-vectors.enabled", "true")
        .option("pk-vector.index.columns", "embedding")
        .option("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER)
        .option("fields.embedding.pk-vector.distance.metric", "l2")
        .build()
        .unwrap();
    let file_io = FileIOBuilder::new("memory").build().unwrap();
    let table = Table::new(
        file_io.clone(),
        Identifier::new("default", "pk_vector_route_test"),
        table_path.to_string(),
        TableSchema::new(0, &schema),
        None,
    );
    for dir in ["snapshot", "manifest", "index"] {
        file_io
            .mkdirs(&format!("{table_path}/{dir}"))
            .await
            .unwrap();
    }

    // id + FixedSizeList<Float32> batch matching the table's target schema.
    let ids: Vec<i32> = (0..vectors.len() as i32).collect();
    let element_field = Arc::new(ArrowField::new("element", ArrowDataType::Float32, true));
    let mut vec_builder = FixedSizeListBuilder::new(Float32Builder::new(), DIM as i32)
        .with_field(element_field.clone());
    for v in vectors {
        for &x in v {
            vec_builder.values().append_value(x);
        }
        vec_builder.append(true);
    }
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new(
            "embedding",
            ArrowDataType::FixedSizeList(element_field, DIM as i32),
            true,
        ),
    ]));
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int32Array::from(ids)) as ArrayRef,
            Arc::new(vec_builder.finish()) as ArrayRef,
        ],
    )
    .unwrap();

    // Real data-file meta via the write path (these messages are not committed
    // as-is; the meta is promoted below and committed with the index).
    let mut writer = TableWrite::new(&table, "route-test".to_string()).unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    let base = &messages[0];
    let base_meta = base.new_files[0].clone();
    let bucket = base.bucket;
    let partition = base.partition.clone();
    let data_file_name = base_meta.file_name.clone();
    let row_count = base_meta.row_count;

    // PK index-source precondition: compacted, non-level-0, first_row_id pinned.
    let indexed_meta = DataFileMeta {
        level: 1,
        file_source: Some(1),
        first_row_id: Some(0),
        ..base_meta
    };

    // Real vindex IVF-flat segment (nlist=1 -> exact) over the vectors.
    let n = vectors.len();
    let flat: Vec<f32> = vectors.iter().flat_map(|v| v.iter().copied()).collect();
    let seg_ids: Vec<i64> = (0..n as i64).collect();
    let native = HashMap::from([
        ("index.type".to_string(), "ivf_flat".to_string()),
        ("dimension".to_string(), DIM.to_string()),
        ("nlist".to_string(), "1".to_string()),
        ("metric".to_string(), "l2".to_string()),
    ]);
    let config = VectorIndexConfig::from_options(&native).unwrap();
    let training = VectorIndexTrainer::train(config, &flat, n).unwrap();
    let mut ann_writer = VectorIndexWriter::new(training);
    ann_writer.add_vectors(&seg_ids, &flat, n).unwrap();
    let mut seg_bytes = Vec::new();
    {
        let mut out = PosWriter::new(&mut seg_bytes);
        ann_writer.write(&mut out).unwrap();
    }
    let index_file_name = "vector-ivf-flat-route.index".to_string();
    let index_file_size = seg_bytes.len() as u64;
    file_io
        .new_output(&format!("{table_path}/index/{index_file_name}"))
        .unwrap()
        .write(Bytes::from(seg_bytes))
        .await
        .unwrap();

    let vector_field_id = schema
        .fields()
        .iter()
        .find(|f| f.name() == "embedding")
        .unwrap()
        .id();
    let index_file = IndexFileMeta {
        index_type: IVF_FLAT_IDENTIFIER.to_string(),
        file_name: index_file_name,
        file_size: i64::try_from(index_file_size).unwrap(),
        row_count,
        deletion_vectors_ranges: None,
        external_path: None,
        global_index_meta: Some(GlobalIndexMeta {
            row_range_start: 0,
            row_range_end: row_count - 1,
            index_field_id: vector_field_id,
            extra_field_ids: None,
            source_meta: Some(pk_source_meta_bytes(1, &[(&data_file_name, row_count)])),
            index_meta: None,
        }),
    };

    let mut message = CommitMessage::new(partition, bucket, vec![indexed_meta]);
    message.new_index_files = vec![index_file];
    TableCommit::new(table.clone(), "route-test".to_string())
        .commit(vec![message])
        .await
        .unwrap();
    table
}

#[tokio::test]
async fn rerank_aligns_recomputed_distance_by_position_column() {
    use crate::arrow::build_target_arrow_schema;
    use crate::arrow::format::{FormatFileWriter, ParquetFormatWriter};
    use crate::spec::VectorType;
    use crate::table::schema_manager::SchemaManager;

    // A vector data file with 4 physical rows: positions 0,1,3 hold vectors
    // and position 2 (a NON-candidate) holds a NULL vector. Candidates sit at
    // non-contiguous positions {1, 3}. The ANN-reported distances are
    // deliberately reversed relative to the true stored vectors; after rerank
    // each candidate must carry compute_distance(query, vec_at_its_position),
    // proving alignment is by the _PKEY_VECTOR_POSITION column value, not batch
    // order. Position 2's NULL is never read (it is not a candidate), so it
    // cannot trip the null-vector guard.
    let vector_type = VectorType::try_new(true, 2, DataType::Float(FloatType::new())).unwrap();
    let vector_field = DataField::new(0, "embedding".to_string(), DataType::Vector(vector_type));
    let read_fields = vec![vector_field.clone()];
    let arrow_schema = build_target_arrow_schema(&read_fields).unwrap();

    // pos0=[7,0], pos1=[1,0], pos2=NULL, pos3=[4,0].
    let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), 2).with_field(Arc::new(
        ArrowField::new("element", ArrowDataType::Float32, true),
    ));
    for row in [
        Some([7.0f32, 0.0]),
        Some([1.0, 0.0]),
        None,
        Some([4.0, 0.0]),
    ] {
        match row {
            Some([a, b]) => {
                builder.values().append_value(a);
                builder.values().append_value(b);
                builder.append(true);
            }
            None => {
                builder.values().append_value(0.0);
                builder.values().append_value(0.0);
                builder.append(false);
            }
        }
    }
    let vec_array = builder.finish();
    let batch =
        arrow_array::RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(vec_array)]).unwrap();

    let file_io = FileIOBuilder::new("memory").build().unwrap();
    let table_path = "memory:/rerank_positional";
    let bucket_path = format!("{table_path}/bucket-0");
    let file_name = "part-0.parquet";
    let output = file_io
        .new_output(&format!("{bucket_path}/{file_name}"))
        .unwrap();
    let mut writer: Box<dyn FormatFileWriter> = Box::new(
        ParquetFormatWriter::new(
            &output,
            arrow_schema.clone(),
            "zstd",
            1,
            None,
            &HashMap::new(),
        )
        .await
        .unwrap(),
    );
    writer.write(&batch).await.unwrap();
    let file_size = writer.close().await.unwrap().file_size;

    let schema_id = 1;
    let file_meta = pk_data_file(file_name, 4, Some(0));
    let file_meta = DataFileMeta {
        file_size: file_size as i64,
        schema_id,
        ..file_meta
    };
    let data_split = DataSplitBuilder::new()
        .with_snapshot(1)
        .with_partition(BinaryRow::new(0))
        .with_bucket(0)
        .with_bucket_path(bucket_path)
        .with_total_buckets(1)
        .with_data_files(vec![file_meta])
        .build()
        .unwrap();
    let split = PkVectorSearchSplit {
        data_split,
        ann_segments: Vec::new(),
        active_files: Vec::new(),
    };

    let schema_manager = SchemaManager::new(file_io.clone(), table_path.to_string());
    let reader = DataFileReader::new(
        file_io,
        schema_manager,
        schema_id,
        read_fields.clone(),
        read_fields.clone(),
        Vec::new(),
    );

    let query = vec![1.0f32, 0.0];
    // ANN-reported distances reversed vs. truth: pos1 reported worse (0.9) than
    // pos3 (0.1), but the true L2 distances are pos1=0 and pos3=9.
    let indexed = vec![cand_at(0, file_name, 1, 0.9), cand_at(0, file_name, 3, 0.1)];

    let out = rerank_indexed_positional(
        &reader,
        indexed,
        &[split],
        &query,
        VectorSearchMetric::L2,
        2,
        &vector_field,
    )
    .await
    .unwrap();

    // Best-first after exact recompute: pos1 (d=0) then pos3 (d=9), each
    // carrying the distance computed from its OWN position's stored vector.
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].row_position, 1);
    assert_eq!(out[0].distance, 0.0);
    assert_eq!(out[1].row_position, 3);
    assert_eq!(out[1].distance, 9.0);
}

#[tokio::test]
async fn rerank_recomputes_distance_and_reorders() {
    // pos0=[9,0], pos1=[1,0]; query=[1,0]. The ANN-reported distances are
    // reversed relative to the truth (pos0 reported best at 0.1, pos1 worst at
    // 0.9), so an implementation that trusted the ANN order would emit pos0
    // first. Exact L2 recompute yields pos0=64, pos1=0, so the output must
    // reorder to pos1-then-pos0 with the recomputed distances.
    let (reader, split, vector_field) = vector_rerank_fixture(
        "memory:/rerank_reorder",
        2,
        &[Some(vec![9.0, 0.0]), Some(vec![1.0, 0.0])],
        &[],
    )
    .await;
    let query = vec![1.0f32, 0.0];
    let indexed = vec![
        cand_at(0, RERANK_FILE, 0, 0.1),
        cand_at(0, RERANK_FILE, 1, 0.9),
    ];

    let out = rerank_indexed_positional(
        &reader,
        indexed,
        &[split],
        &query,
        VectorSearchMetric::L2,
        2,
        &vector_field,
    )
    .await
    .unwrap();

    assert_eq!(out.len(), 2);
    assert_eq!(out[0].row_position, 1);
    assert_eq!(out[0].distance, 0.0);
    assert_eq!(out[1].row_position, 0);
    assert_eq!(out[1].distance, 64.0);
    // Order genuinely changed vs. the ANN-reported best-first (which was pos0).
    assert!(out[0].distance < out[1].distance);
}

#[tokio::test]
async fn rerank_is_independent_of_fast_mode_reranks_indexed() {
    // The rerank core takes only the indexed (fast-path) candidates and always
    // recomputes their true distance; there is no fast/exact switch that can
    // skip it. The single candidate carries a bogus ANN distance (0.42) but its
    // stored vector equals the query, so the recomputed L2 distance is exactly
    // 0.0 — proving the indexed candidate WAS reranked rather than passed
    // through with its ANN distance.
    let (reader, split, vector_field) =
        vector_rerank_fixture("memory:/rerank_indexed", 2, &[Some(vec![1.0, 0.0])], &[]).await;
    let query = vec![1.0f32, 0.0];
    let indexed = vec![cand_at(0, RERANK_FILE, 0, 0.42)];

    let out = rerank_indexed_positional(
        &reader,
        indexed,
        &[split],
        &query,
        VectorSearchMetric::L2,
        1,
        &vector_field,
    )
    .await
    .unwrap();

    assert_eq!(out.len(), 1);
    assert_eq!(out[0].row_position, 0);
    assert_ne!(out[0].distance, 0.42);
    assert_eq!(out[0].distance, 0.0);
}

#[tokio::test]
async fn rerank_fails_loud_on_null_vector() {
    // A NULL vector stored AT a candidate position must fail loud rather than
    // silently scoring it: the candidate genuinely has no vector to rerank on.
    let (reader, split, vector_field) =
        vector_rerank_fixture("memory:/rerank_null", 2, &[None], &[]).await;
    let query = vec![1.0f32, 0.0];
    let indexed = vec![cand_at(0, RERANK_FILE, 0, 0.1)];

    let err = rerank_indexed_positional(
        &reader,
        indexed,
        &[split],
        &query,
        VectorSearchMetric::L2,
        1,
        &vector_field,
    )
    .await
    .err()
    .expect("null vector at a candidate position must fail loud");
    assert!(
        matches!(err, crate::Error::DataInvalid { ref message, .. } if message.contains("null vector")),
        "unexpected error: {err:?}"
    );
}

#[tokio::test]
async fn rerank_fails_loud_on_leftover_candidate() {
    // pos1 is deleted by the deletion vector, so the position read returns no
    // row for it. The search path already DV-filters, so a deleted candidate
    // reaching rerank is a real inconsistency: the leftover guard must fail
    // loud rather than silently dropping the candidate.
    let (reader, split, vector_field) = vector_rerank_fixture(
        "memory:/rerank_leftover",
        2,
        &[Some(vec![1.0, 0.0]), Some(vec![2.0, 0.0])],
        &[1],
    )
    .await;
    let query = vec![1.0f32, 0.0];
    let indexed = vec![
        cand_at(0, RERANK_FILE, 0, 0.1),
        cand_at(0, RERANK_FILE, 1, 0.9),
    ];

    let err = rerank_indexed_positional(
        &reader,
        indexed,
        &[split],
        &query,
        VectorSearchMetric::L2,
        2,
        &vector_field,
    )
    .await
    .err()
    .expect("a candidate returning no row must fail loud");
    assert!(
        matches!(err, crate::Error::DataInvalid { ref message, .. } if message.contains("failed to read")),
        "unexpected error: {err:?}"
    );
}

#[tokio::test]
async fn rerank_fails_loud_on_dimension_mismatch() {
    // Stored vectors are 3-dimensional but the query is 2-dimensional. The
    // vector extraction validates each stored row against the query dimension
    // and fails loud, so the recompute never runs against mismatched vectors.
    let (reader, split, vector_field) =
        vector_rerank_fixture("memory:/rerank_dim", 3, &[Some(vec![1.0, 0.0, 0.0])], &[]).await;
    let query = vec![1.0f32, 0.0];
    let indexed = vec![cand_at(0, RERANK_FILE, 0, 0.1)];

    let err = rerank_indexed_positional(
        &reader,
        indexed,
        &[split],
        &query,
        VectorSearchMetric::L2,
        1,
        &vector_field,
    )
    .await
    .err()
    .expect("dimension mismatch must fail loud");
    assert!(
        matches!(err, crate::Error::DataInvalid { ref message, .. } if message.contains("dimension")),
        "unexpected error: {err:?}"
    );
}

#[tokio::test]
async fn rerank_fails_loud_on_duplicate_candidate_position() {
    // Two candidates addressing the same (split_index, file, position) is a
    // programming error upstream: the dedup guard fires before any read.
    let (reader, split, vector_field) =
        vector_rerank_fixture("memory:/rerank_dup", 2, &[Some(vec![1.0, 0.0])], &[]).await;
    let query = vec![1.0f32, 0.0];
    let indexed = vec![
        cand_at(0, RERANK_FILE, 0, 0.1),
        cand_at(0, RERANK_FILE, 0, 0.9),
    ];

    let err = rerank_indexed_positional(
        &reader,
        indexed,
        &[split],
        &query,
        VectorSearchMetric::L2,
        2,
        &vector_field,
    )
    .await
    .err()
    .expect("duplicate candidate position must fail loud");
    assert!(
        matches!(err, crate::Error::DataInvalid { ref message, .. } if message.contains("duplicate")),
        "unexpected error: {err:?}"
    );
}

#[tokio::test]
async fn rerank_fails_loud_on_unexpected_position() {
    // Every position the read surfaces must resolve to a candidate keyed by
    // (split_index, file, position). Here the plan carries two splits for the
    // SAME (partition, bucket, file), so `split_index_of` resolves the file to
    // the LAST plan index (1). The single candidate is tagged with split_index
    // 0, so its by_key entry is (0, file, 0) while the read looks up
    // (1, file, 0). The lookup misses and the unexpected-position guard fires
    // rather than silently dropping the surfaced row.
    let (reader, split, vector_field) =
        vector_rerank_fixture("memory:/rerank_unexpected", 2, &[Some(vec![1.0, 0.0])], &[]).await;
    let query = vec![1.0f32, 0.0];
    let indexed = vec![cand_at(0, RERANK_FILE, 0, 0.1)];

    // Two plan entries for the same file: split_index_of ends up mapping the
    // file to plan index 1, not the candidate's split_index 0.
    let dup = PkVectorSearchSplit {
        data_split: split.data_split.clone(),
        ann_segments: Vec::new(),
        active_files: Vec::new(),
    };
    let plan = vec![dup, split];

    let err = rerank_indexed_positional(
        &reader,
        indexed,
        &plan,
        &query,
        VectorSearchMetric::L2,
        1,
        &vector_field,
    )
    .await
    .err()
    .expect("a read position absent from the candidate map must fail loud");
    assert!(
        matches!(err, crate::Error::DataInvalid { ref message, .. } if message.contains("unexpected position")),
        "unexpected error: {err:?}"
    );
}

#[tokio::test]
async fn rerank_fails_loud_on_file_not_in_plan() {
    // A candidate references a (partition, bucket, file) that is absent from
    // plan_splits. build_indexed_splits groups it into an indexed split, but the
    // split_index_of lookup — built only from plan_splits — has no entry, so the
    // kernel fails loud rather than reading an unplanned file.
    let (reader, _split, vector_field) =
        vector_rerank_fixture("memory:/rerank_noplan", 2, &[Some(vec![1.0, 0.0])], &[]).await;
    let query = vec![1.0f32, 0.0];
    let indexed = vec![cand_at(0, RERANK_FILE, 0, 0.1)];

    // Empty plan: the candidate's file resolves in no plan split.
    let err = rerank_indexed_positional(
        &reader,
        indexed,
        &[],
        &query,
        VectorSearchMetric::L2,
        1,
        &vector_field,
    )
    .await
    .err()
    .expect("a candidate file absent from the plan must fail loud");
    assert!(
        matches!(err, crate::Error::DataInvalid { ref message, .. } if message.contains("not found in plan")),
        "unexpected error: {err:?}"
    );
}

#[tokio::test]
async fn rerank_reads_only_candidate_positions_not_whole_column() {
    // A 6-row file where every NON-candidate position (0, 2, 4, 5) holds a NULL
    // vector "poison" and only the two candidate positions (1, 3) hold real
    // vectors. The rerank read is told to fetch only positions {1, 3}; every
    // row it surfaces is looked up in the candidate map, and any position not in
    // the map trips the "unexpected position" guard (a surfaced NULL row would
    // additionally trip the null-vector guard). So if the read had surfaced any
    // of the poison rows, rerank would fail. It succeeds and returns exactly the
    // two candidates at positions {1, 3}, which proves the position selection
    // reaching the read contained only the candidate positions (not the whole
    // column).
    let rows = &[
        None,                 // pos0 poison (non-candidate)
        Some(vec![1.0, 0.0]), // pos1 candidate
        None,                 // pos2 poison (non-candidate)
        Some(vec![3.0, 0.0]), // pos3 candidate
        None,                 // pos4 poison (non-candidate)
        None,                 // pos5 poison (non-candidate)
    ];
    let (reader, split, vector_field) =
        vector_rerank_fixture("memory:/rerank_spy", 2, rows, &[]).await;
    let query = vec![1.0f32, 0.0];
    let indexed = vec![
        cand_at(0, RERANK_FILE, 1, 0.9),
        cand_at(0, RERANK_FILE, 3, 0.1),
    ];

    let out = rerank_indexed_positional(
        &reader,
        indexed,
        &[split],
        &query,
        VectorSearchMetric::L2,
        2,
        &vector_field,
    )
    .await
    .unwrap_or_else(|e| {
        panic!("only candidate positions are read, so the poison NULLs never decode: {e:?}")
    });

    assert_eq!(out.len(), 2, "exactly the candidate count of rows was read");
    let mut positions: Vec<i64> = out.iter().map(|c| c.row_position).collect();
    positions.sort_unstable();
    assert_eq!(
        positions,
        vec![1, 3],
        "only candidate positions reached the read"
    );
    // Recomputed distances confirm each surviving row is its own candidate's vector.
    assert_eq!(out[0].row_position, 1);
    assert_eq!(out[0].distance, 0.0);
    assert_eq!(out[1].row_position, 3);
    assert_eq!(out[1].distance, 4.0);
}

#[test]
fn verify_segment_metric_accepts_matching_lumina_metric() {
    // Lumina segment metadata says cosine; configured cosine => Ok. No segment
    // file bytes are needed on the Lumina path.
    let split = pk_split_with_lumina_segment("seg-lumina", "cosine");
    let segment = &split.ann_segments[0];
    let lumina_metric = LuminaIndexMeta::deserialize(&segment.index_meta)
        .unwrap()
        .metric()
        .unwrap();
    verify_segment_metric(
        VectorSearchMetric::Cosine,
        VectorSearchMetric::from_lumina(lumina_metric),
    )
    .expect("matching lumina metric must pass");
}

#[test]
fn verify_segment_metric_rejects_mismatched_lumina_metric() {
    // Lumina segment metadata says l2; configured inner_product => fail loud,
    // naming both metrics.
    let split = pk_split_with_lumina_segment("seg-lumina", "l2");
    let segment = &split.ann_segments[0];
    let lumina_metric = LuminaIndexMeta::deserialize(&segment.index_meta)
        .unwrap()
        .metric()
        .unwrap();
    let err = verify_segment_metric(
        VectorSearchMetric::InnerProduct,
        VectorSearchMetric::from_lumina(lumina_metric),
    )
    .expect_err("mismatched lumina metric must fail loud");
    assert!(
        matches!(err, crate::Error::DataInvalid { ref message, .. }
            if message.contains("does not match configured metric")
                && message.contains("l2")
                && message.contains("inner_product")),
        "unexpected error: {err:?}"
    );
}

#[test]
fn verify_segment_metric_accepts_matching_vindex_metric() {
    // Real IVF segment trained with L2; configured metric L2 => Ok.
    let bytes = bytes::Bytes::from(build_vindex_segment_bytes("l2"));
    let reader = VIndexReader::open(Cursor::new(bytes)).unwrap();
    verify_segment_metric(
        VectorSearchMetric::L2,
        VectorSearchMetric::from_vindex(reader.metadata().metric),
    )
    .expect("matching metric must pass");
}

#[test]
fn verify_segment_metric_rejects_mismatched_vindex_metric() {
    // Real IVF segment trained with L2; configured metric Cosine => fail loud.
    let bytes = bytes::Bytes::from(build_vindex_segment_bytes("l2"));
    let reader = VIndexReader::open(Cursor::new(bytes)).unwrap();
    let err = verify_segment_metric(
        VectorSearchMetric::Cosine,
        VectorSearchMetric::from_vindex(reader.metadata().metric),
    )
    .expect_err("mismatched metric must fail loud");
    assert!(
        matches!(err, crate::Error::DataInvalid { ref message, .. }
            if message.contains("does not match configured metric")
                && message.contains("l2")
                && message.contains("cosine")),
        "unexpected error: {err:?}"
    );
}

// ---- Search results retain scored positions and the planned source context. ----
#[tokio::test]
async fn reader_uses_planned_splits_after_index_manifest_is_removed() {
    let table = build_committed_pk_vector_table(&[[1.0, 0.0], [0.0, 1.0], [1.0, 1.0]]).await;
    let options = HashMap::new();
    let query = [0.0, 1.0];
    let params =
        PkVectorSearchParams::resolve(&table, &options, None, "embedding", &[&query], 2).unwrap();
    let scan = crate::table::pk_vector_scan::PkVectorScan::new(
        &table,
        params.vector_field.id(),
        params.index_type.clone(),
        None,
    );
    let read = PkVectorRead::new(&table, &options, None, "embedding", &[&query], 2, params);
    let plan = scan.plan().await.unwrap();
    let snapshot_id = plan.snapshot_id;
    let manager = table.snapshot_manager();
    let snapshot = manager.get_snapshot(snapshot_id).await.unwrap();
    let manifest_path = manager.manifest_path(snapshot.index_manifest().unwrap());
    table.file_io().delete_file(&manifest_path).await.unwrap();
    assert!(
        scan.plan().await.is_err(),
        "a second plan must need the removed manifest"
    );

    // Reading must use the original per-bucket source context, without replanning.
    let mut results = read.read(plan).await.unwrap();
    assert_eq!(results.len(), 1);
    let result = results.pop().unwrap();
    assert_eq!(result.snapshot_id(), Some(snapshot_id));
    assert_eq!(
        result
            .positions()
            .unwrap()
            .iter()
            .map(|c| c.row_position)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(
        result
            .positions()
            .unwrap()
            .iter()
            .map(|c| c.score)
            .collect::<Vec<_>>(),
        vec![1.0, 0.5]
    );
    assert_eq!(result.indexed_splits().unwrap().len(), 1);

    let batches: Vec<RecordBatch> = result
        .new_read_builder()
        .with_projection(&["id"])
        .read()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let ids: Vec<i32> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect();
    assert_eq!(
        ids,
        vec![1, 2],
        "materialization must also use the retained plan"
    );
}

#[tokio::test]
async fn execute_returns_scored_positions_and_publishes_diagnostics() {
    reset_vector_search_test_logs();
    let _timing = crate::vindex::enable_vector_search_timing_for_test();
    // query [0,1]: squared-L2 distances pos1=0 < pos2=1 < pos0=2, so the
    // strict-gap top-2 is [pos1, pos2] (best-first, not physical order).
    let table = build_committed_pk_vector_table(&[[1.0, 0.0], [0.0, 1.0], [1.0, 1.0]]).await;
    let result = table
        .new_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vector(vec![0.0, 1.0])
        .with_limit(2)
        .execute()
        .await
        .unwrap();
    let positions = result.positions().unwrap();

    // Two nearest neighbours recalled, best-first, without materialization.
    assert_eq!(positions.len(), 2, "top-2 positions expected");
    assert_eq!(positions[0].row_position, 1, "nearest is position 1");
    assert_eq!(positions[1].row_position, 2, "second nearest is position 2");
    assert_eq!(
        positions.iter().map(|p| p.score).collect::<Vec<_>>(),
        vec![1.0, 0.5]
    );

    // Every position retains the source file and snapshot needed for a later read.
    assert_eq!(result.snapshot_id(), Some(1), "first commit -> snapshot 1");
    let splits = result.indexed_splits().unwrap();
    assert_eq!(splits.len(), 1);
    assert_eq!(splits[0].split.snapshot_id(), 1);
    assert!(
        positions.iter().all(|p| {
            p.data_file_name == splits[0].split.data_files()[0].file_name
                && p.bucket == splits[0].split.bucket()
                && p.partition.to_serialized_bytes()
                    == splits[0].split.partition().to_serialized_bytes()
        }),
        "positions must refer to their retained source split"
    );

    let logs = VECTOR_SEARCH_TEST_LOGS.lock().unwrap();
    assert!(
        logs.iter().any(|entry| {
            entry.contains("event=paimon_vindex_reader")
                && entry.contains("vector-ivf-flat-route.index")
        }),
        "PK vector search must publish vindex reader timing"
    );
    assert!(
        logs.iter().any(|entry| {
            entry.contains("event=paimon_vector_range_io")
                && entry.contains("vector-ivf-flat-route.index")
        }),
        "PK vector search must publish range-I/O timing"
    );
}

/// A table with no snapshot yields empty positions and splits without inventing
/// a snapshot or changing the result's PK address space.
#[tokio::test]
async fn execute_empty_plan_yields_empty_pk_result() {
    let table = pk_vector_table(&[
        ("pk-vector.index.columns", "embedding"),
        ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
        ("fields.embedding.pk-vector.distance.metric", "l2"),
    ]);
    let result = table
        .new_vector_search_builder()
        .with_vector_column("embedding")
        .with_query_vector(vec![1.0; 128])
        .with_limit(3)
        .execute()
        .await
        .unwrap();
    assert!(
        result.positions().unwrap().is_empty(),
        "no data -> no positions"
    );
    assert!(
        result.indexed_splits().unwrap().is_empty(),
        "no data -> no source splits"
    );
    assert_eq!(result.snapshot_id(), None);
    assert!(result.row_ids().is_err());
}

/// The vector residual is derived from the DATA conjuncts of the filter:
/// partition-only conjuncts are enforced by scan planning (`PkVectorScan`
/// pushes the whole filter through the normal scan) and must not enter the
/// per-row residual, so a partition-only filter yields no residual at all.
#[test]
fn residual_uses_only_data_conjuncts_of_the_filter() {
    use crate::spec::VarCharType;
    use crate::table::bucket_filter::split_partition_and_data_predicates;

    // Partitioned table: `dt` (partition key) + `id`.
    let schema = Schema::builder()
        .column("dt", DataType::VarChar(VarCharType::string_type()))
        .column("id", DataType::Int(IntType::new()))
        .partition_keys(["dt"])
        .build()
        .unwrap();
    let ts = TableSchema::new(0, &schema);
    let fields = ts.fields();
    let partition_keys = ts.partition_keys();
    let pb = PredicateBuilder::new(fields);

    // Partition-only `dt = 'a'` -> no residual data predicate (residual skipped;
    // the partition is enforced by planning alone).
    let (_p, data) = split_partition_and_data_predicates(
        pb.equal("dt", Datum::String("a".to_string())).unwrap(),
        fields,
        partition_keys,
    );
    assert!(
        data.is_empty(),
        "partition-only filter must leave no residual data predicate"
    );

    // Data-only `id > 5` -> kept as the residual.
    let (_p, data) = split_partition_and_data_predicates(
        pb.greater_than("id", Datum::Int(5)).unwrap(),
        fields,
        partition_keys,
    );
    assert_eq!(data.len(), 1, "data-only filter must remain the residual");

    // `dt = 'a' AND id > 5` -> only the data conjunct enters the residual.
    let (_p, data) = split_partition_and_data_predicates(
        Predicate::and(vec![
            pb.equal("dt", Datum::String("a".to_string())).unwrap(),
            pb.greater_than("id", Datum::Int(5)).unwrap(),
        ]),
        fields,
        partition_keys,
    );
    assert_eq!(
        data.len(),
        1,
        "AND(partition, data) residual must drop the partition conjunct"
    );

    // `dt = 'a' OR id > 5` is a single mixed conjunct: it is NOT partition-only,
    // so it stays whole in the residual (evaluated against the materialized
    // partition column), rather than being dropped or split.
    let mixed = Predicate::or(vec![
        pb.equal("dt", Datum::String("a".to_string())).unwrap(),
        pb.greater_than("id", Datum::Int(5)).unwrap(),
    ]);
    let (_p, data) = split_partition_and_data_predicates(mixed.clone(), fields, partition_keys);
    assert_eq!(
        data,
        vec![mixed],
        "a mixed partition/data conjunct must stay whole in the residual"
    );
}
