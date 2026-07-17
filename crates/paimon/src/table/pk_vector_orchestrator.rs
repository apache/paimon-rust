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

//! Primary-key vector read orchestration (Rust equivalent of Java
//! `PrimaryKeyVectorRead` + `PrimaryKeyVectorResult.splits()`).
//!
//! Per-bucket search via `bucket_search`, cross-bucket global Top-K merge,
//! grouping survivors by data file into `PkVectorIndexedSplit`s, and lazy
//! materialization via `PkVectorIndexedSplitRead`.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use crate::deletion_vector::DeletionVector;
use crate::spec::BinaryRow;
use crate::table::data_file_reader::DataFileReader;
use crate::table::pk_vector_indexed_split_read::PkVectorIndexedSplit;
use crate::table::source::{DataSplit, DataSplitBuilder, RowRange};
use crate::vindex::pkvector::ann::PkVectorAnnSearcher;
use crate::vindex::pkvector::bucket::{bucket_search, BucketActiveFile, BucketAnnSegment};
use crate::vindex::pkvector::metric::{java_float_compare, VectorSearchMetric};
use crate::vindex::pkvector::reader::PkVectorReader;
use crate::vindex::pkvector::result::PkVectorSearchResult;

fn data_invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

/// Validate a hit's physical row position against its data file, mirroring the
/// bounds Java `PrimaryKeyVectorResult.splits()` enforces per candidate: the
/// position must be non-negative, within the file's row count, and fit in an
/// `i32`. A position outside this range means a corrupt ANN index or malformed
/// source metadata resolved to a bogus ordinal; fail loud rather than emit a
/// wrong row.
pub(crate) fn validate_row_position(
    file_name: &str,
    row_position: i64,
    row_count: i64,
) -> crate::Result<()> {
    if row_position < 0 || row_position >= row_count || row_position > i32::MAX as i64 {
        return Err(data_invalid(format!(
            "vector search hit position {row_position} out of range for {file_name} (row count {row_count})"
        )));
    }
    Ok(())
}

/// One bucket's search input. Rust equivalent of Java
/// `BucketVectorSearchSplit`. Constructed from a snapshot/manifest plan by
/// `PkVectorScan`.
pub(crate) struct PkVectorSearchSplit {
    /// The bucket's combined data split (>= 1 data file); source of the
    /// partition/bucket/bucket_path/snapshot, the per-file `DataFileMeta`, and the
    /// deletion files. Its `data_files()` is the authority for re-associating a hit's
    /// file name back to a `DataFileMeta`.
    pub data_split: DataSplit,
    /// ANN payload segments for this bucket.
    pub ann_segments: Vec<BucketAnnSegment>,
    /// Files eligible for exact fallback.
    pub active_files: Vec<BucketActiveFile>,
}

/// A `bucket_search` hit tagged with its source bucket. `partition`/`bucket` are
/// the cross-bucket merge dimensions a lone `PkVectorSearchResult` lacks;
/// `split_index` is the re-association handle back to
/// `splits[split_index].data_split`.
pub(crate) struct PkVectorCandidate {
    pub split_index: usize,
    pub partition: BinaryRow,
    pub bucket: i32,
    pub data_file_name: String,
    pub row_position: i64,
    pub distance: f32,
}

/// 5-level BEST_FIRST (smallest = best) key. Level 1 orders distance with
/// `java_float_compare` so a NaN distance (e.g. from a non-finite stored vector
/// under inner product) sorts last rather than winning Top-1. Level 2 uses the
/// partition's serialized bytes; Rust `Vec<u8>::cmp` is unsigned lexicographic
/// then shorter-is-less, exactly the spec's contract (`[0x7f] < [0x80] < [0xff]`).
fn candidate_cmp(a: &PkVectorCandidate, b: &PkVectorCandidate) -> Ordering {
    java_float_compare(a.distance, b.distance)
        .then_with(|| {
            a.partition
                .to_serialized_bytes()
                .cmp(&b.partition.to_serialized_bytes())
        })
        .then_with(|| a.bucket.cmp(&b.bucket))
        .then_with(|| a.data_file_name.cmp(&b.data_file_name))
        .then_with(|| a.row_position.cmp(&b.row_position))
}

/// Collect all candidates, order BEST_FIRST, keep the best `limit`.
fn global_top_k(mut candidates: Vec<PkVectorCandidate>, limit: usize) -> Vec<PkVectorCandidate> {
    candidates.sort_by(candidate_cmp);
    candidates.truncate(limit);
    candidates
}

/// Group Top-K survivors by `(partition, bucket, data_file_name)`, re-associate
/// each group's file to its real `DataFileMeta` + aligned deletion file in the
/// source bucket split, and build one `PkVectorIndexedSplit` per file. Groups are
/// emitted in ascending group-key order (deterministic file/position output
/// order). Mirrors Java `PrimaryKeyVectorResult.splits()`.
pub(crate) fn build_indexed_splits(
    survivors: Vec<PkVectorCandidate>,
    splits: &[PkVectorSearchSplit],
    metric: VectorSearchMetric,
) -> crate::Result<Vec<PkVectorIndexedSplit>> {
    // Group key: (partition bytes, bucket, file name). BTreeMap keeps ascending
    // group order deterministically. Value: (split_index, Vec<(position, distance)>).
    use std::collections::BTreeMap;
    type GroupKey = (Vec<u8>, i32, String);
    let mut groups: BTreeMap<GroupKey, (usize, Vec<(i64, f32)>)> = BTreeMap::new();
    for c in survivors {
        let key = (
            c.partition.to_serialized_bytes(),
            c.bucket,
            c.data_file_name.clone(),
        );
        let entry = groups
            .entry(key)
            .or_insert_with(|| (c.split_index, Vec::new()));
        // A (partition, bucket, file_name) group must map to a single source
        // split. Two candidates sharing the group key but tagged with different
        // split_index means malformed input (e.g. duplicate buckets);
        // silently merging them would materialize against the wrong split, so
        // fail loud instead.
        if entry.0 != c.split_index {
            return Err(data_invalid(format!(
                "vector search hits for {} map to different splits ({} and {})",
                c.data_file_name, entry.0, c.split_index
            )));
        }
        entry.1.push((c.row_position, c.distance));
    }

    let mut out = Vec::with_capacity(groups.len());
    for ((_partition, _bucket, file_name), (split_index, mut hits)) in groups {
        // Sort positions ascending; reject duplicate (file, position).
        hits.sort_by_key(|(pos, _)| *pos);
        for pair in hits.windows(2) {
            if pair[0].0 == pair[1].0 {
                return Err(data_invalid(format!(
                    "duplicate (file, position) in vector search result: {} @ {}",
                    file_name, pair[0].0
                )));
            }
        }

        // Re-associate the file to its DataFileMeta + aligned deletion file.
        let source = &splits[split_index].data_split;
        let file_idx = source
            .data_files()
            .iter()
            .position(|f| f.file_name == file_name)
            .ok_or_else(|| {
                data_invalid(format!(
                    "vector search hit references data file {file_name} not present in its bucket split"
                ))
            })?;
        let file_meta = source.data_files()[file_idx].clone();
        let deletion_file = source
            .data_deletion_files()
            .and_then(|dfs| dfs.get(file_idx).cloned().flatten());

        // Every hit's physical position must be in range for its data file.
        for &(pos, _) in &hits {
            validate_row_position(&file_name, pos, file_meta.row_count)?;
        }

        // Coalesce ascending positions into inclusive ranges; scores aligned to
        // ascending-position order.
        let mut row_ranges: Vec<RowRange> = Vec::new();
        let mut scores: Vec<f32> = Vec::with_capacity(hits.len());
        let mut start = hits[0].0;
        let mut end = hits[0].0;
        scores.push(metric.distance_to_score(hits[0].1));
        for &(pos, distance) in &hits[1..] {
            if pos == end + 1 {
                end = pos;
            } else {
                row_ranges.push(RowRange::new(start, end));
                start = pos;
                end = pos;
            }
            scores.push(metric.distance_to_score(distance));
        }
        row_ranges.push(RowRange::new(start, end));

        let mut builder = DataSplitBuilder::new()
            .with_snapshot(source.snapshot_id())
            .with_partition(source.partition().clone())
            .with_bucket(source.bucket())
            .with_bucket_path(source.bucket_path().to_string())
            .with_total_buckets(source.total_buckets())
            .with_data_files(vec![file_meta]);
        if let Some(df) = deletion_file {
            builder = builder.with_data_deletion_files(vec![Some(df)]);
        }
        let split = builder.build()?;

        out.push(PkVectorIndexedSplit {
            split,
            row_ranges,
            scores: Some(scores),
        });
    }
    Ok(out)
}

/// Build one bucket's DV map: keys are the union of active-file names and all
/// ANN-source file names, so an ANN-source file not in `active_files` still gets
/// its DV. Uses one split-level factory. (Search-time DV; materialization loads
/// its own DV again — an accepted redundancy between the search and
/// materialization phases.)
async fn build_bucket_dv_map(
    reader: &DataFileReader,
    split: &PkVectorSearchSplit,
) -> crate::Result<HashMap<String, Arc<DeletionVector>>> {
    let factory = reader.build_split_dv_factory(&split.data_split).await?;
    let mut names: Vec<&str> = split
        .active_files
        .iter()
        .map(|f| f.file_name.as_str())
        .collect();
    for segment in &split.ann_segments {
        for source in segment.source_meta.source_files() {
            names.push(source.file_name());
        }
    }
    let mut dvs = HashMap::new();
    for name in names {
        if dvs.contains_key(name) {
            continue;
        }
        if let Some(dv) = DataFileReader::deletion_vector_for_file(factory.as_ref(), name) {
            dvs.insert(name.to_string(), dv);
        }
    }
    Ok(dvs)
}

/// Read orchestrator for the PK-table vector search path. Mirrors Java
/// `PrimaryKeyVectorRead` + `PrimaryKeyVectorResult.splits()`.
pub(crate) struct PkVectorOrchestrator {
    reader: DataFileReader,
}

impl PkVectorOrchestrator {
    pub(crate) fn new(reader: DataFileReader) -> Self {
        Self { reader }
    }

    /// Run the eager per-bucket search + cross-bucket global Top-K and return the
    /// best-first survivors (through the full 5-level tie-break, raw distance
    /// preserved). The exact-reader factory is split-scoped: it receives the
    /// current split index and split so a caller can build a reader keyed to the
    /// specific split/file. `skip_exact_fallback` forwards to `bucket_search`.
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::type_complexity)]
    pub(crate) async fn search_candidates(
        &self,
        splits: &[PkVectorSearchSplit],
        query: &[f32],
        metric: VectorSearchMetric,
        limit: usize,
        ann_searcher: Option<&dyn PkVectorAnnSearcher>,
        exact_reader_factory: &mut (dyn FnMut(
            usize,
            &PkVectorSearchSplit,
            &BucketActiveFile,
        ) -> crate::Result<Box<dyn PkVectorReader>>
                  + Send),
        search_options: &HashMap<String, String>,
        skip_exact_fallback: bool,
    ) -> crate::Result<Vec<PkVectorCandidate>> {
        // Eager input-shape validation (Java checkArgument parity).
        if limit == 0 {
            return Err(data_invalid("vector search limit must be positive"));
        }
        if query.is_empty() {
            return Err(data_invalid("vector search query must not be empty"));
        }

        // Eager per-bucket search -> tagged candidates.
        let mut candidates: Vec<PkVectorCandidate> = Vec::new();
        for (split_index, split) in splits.iter().enumerate() {
            let dvs = build_bucket_dv_map(&self.reader, split).await?;
            // Wrap the split-scoped factory into bucket_search's per-file signature.
            let mut bucket_factory =
                |file: &BucketActiveFile| exact_reader_factory(split_index, split, file);
            let results = bucket_search(
                ann_searcher,
                &split.ann_segments,
                &split.active_files,
                &dvs,
                &mut bucket_factory,
                query,
                metric,
                limit,
                search_options,
                skip_exact_fallback,
            )?;
            for PkVectorSearchResult {
                data_file_name,
                row_position,
                distance,
            } in results
            {
                candidates.push(PkVectorCandidate {
                    split_index,
                    partition: split.data_split.partition().clone(),
                    bucket: split.data_split.bucket(),
                    data_file_name,
                    row_position,
                    distance,
                });
            }
        }

        Ok(global_top_k(candidates, limit))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::DataFileMeta;

    fn data_file(name: &str, row_count: i64) -> DataFileMeta {
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
            first_row_id: Some(0),
            write_cols: None,
        }
    }

    fn bucket_split(bucket: i32, files: Vec<DataFileMeta>) -> DataSplit {
        DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(bucket)
            .with_bucket_path(format!("memory:/pkvo/bucket-{bucket}"))
            .with_total_buckets(1)
            .with_data_files(files)
            .build()
            .unwrap()
    }

    fn search_split(bucket: i32, files: Vec<DataFileMeta>) -> PkVectorSearchSplit {
        PkVectorSearchSplit {
            data_split: bucket_split(bucket, files),
            ann_segments: Vec::new(),
            active_files: Vec::new(),
        }
    }

    // Candidate carrying an empty (arity-0) partition, matching bucket_split's partition.
    fn cand(
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

    fn candidate(
        split_index: usize,
        partition_bytes: Vec<u8>,
        bucket: i32,
        file: &str,
        pos: i64,
        distance: f32,
    ) -> PkVectorCandidate {
        PkVectorCandidate {
            split_index,
            partition: BinaryRow::from_bytes(1, partition_bytes),
            bucket,
            data_file_name: file.to_string(),
            row_position: pos,
            distance,
        }
    }

    fn ids(c: &[PkVectorCandidate]) -> Vec<(i32, String, i64)> {
        c.iter()
            .map(|c| (c.bucket, c.data_file_name.clone(), c.row_position))
            .collect()
    }

    #[test]
    fn merges_global_top_k_with_deterministic_ties() {
        // Java PrimaryKeyVectorReadTest.testMergesGlobalTopKWithDeterministicTies:
        // (b1,file-c,pos0,d=2), (b1,file-b,pos1,d=1), (b0,file-a,pos2,d=1), limit=2.
        // Same partition -> drop file-c (d=2); two d=1 tie on bucket: 0 < 1.
        // Result: [(0,"file-a",2), (1,"file-b",1)].
        let part = vec![0x00];
        let survivors = global_top_k(
            vec![
                candidate(0, part.clone(), 1, "file-c", 0, 2.0),
                candidate(0, part.clone(), 1, "file-b", 1, 1.0),
                candidate(1, part.clone(), 0, "file-a", 2, 1.0),
            ],
            2,
        );
        assert_eq!(
            ids(&survivors),
            vec![(0, "file-a".to_string(), 2), (1, "file-b".to_string(), 1)]
        );
    }

    #[test]
    fn orders_partition_bytes_as_unsigned() {
        // Guards against signed-byte comparison: 0x7f < 0x80 < 0xff (unsigned).
        // Equal distance so level 2 (partition bytes) decides.
        let survivors = global_top_k(
            vec![
                candidate(2, vec![0xff], 0, "f", 0, 1.0),
                candidate(0, vec![0x7f], 0, "f", 0, 1.0),
                candidate(1, vec![0x80], 0, "f", 0, 1.0),
            ],
            3,
        );
        assert_eq!(
            survivors
                .iter()
                .map(|c| c.partition.to_serialized_bytes().pop().unwrap())
                .collect::<Vec<u8>>(),
            vec![0x7f, 0x80, 0xff]
        );
    }

    #[test]
    fn truncates_to_limit() {
        let part = vec![0x00];
        let survivors = global_top_k(
            vec![
                candidate(0, part.clone(), 0, "f", 0, 3.0),
                candidate(0, part.clone(), 0, "f", 1, 1.0),
                candidate(0, part.clone(), 0, "f", 2, 2.0),
            ],
            1,
        );
        assert_eq!(ids(&survivors), vec![(0, "f".to_string(), 1)]); // smallest distance
    }

    #[test]
    fn empty_candidates_yield_empty() {
        let survivors = global_top_k(Vec::new(), 5);
        assert!(survivors.is_empty());
    }

    #[test]
    fn builds_two_splits_with_ascending_position_ordered_scores() {
        // One bucket, two files. file-a hits at global order [pos=10, pos=2];
        // build must reorder to positions [2,10] with scores [score(2), score(10)].
        let splits = vec![search_split(
            0,
            vec![data_file("file-a", 20), data_file("file-b", 20)],
        )];
        let survivors = vec![
            cand(0, 0, "file-a", 10, 3.0),
            cand(0, 0, "file-b", 5, 2.0),
            cand(0, 0, "file-a", 2, 1.0),
        ];
        let built = build_indexed_splits(survivors, &splits, VectorSearchMetric::L2).unwrap();
        assert_eq!(built.len(), 2);

        // Group order is ascending (partition, bucket, name): file-a before file-b.
        let a = &built[0];
        assert_eq!(a.split.data_files()[0].file_name, "file-a");
        assert_eq!(
            a.row_ranges,
            vec![RowRange::new(2, 2), RowRange::new(10, 10)]
        );
        // scores in ascending-position order: score(d=1.0) for pos2, score(d=3.0) for pos10.
        assert_eq!(
            a.scores.as_deref(),
            Some(
                [
                    VectorSearchMetric::L2.distance_to_score(1.0),
                    VectorSearchMetric::L2.distance_to_score(3.0),
                ]
                .as_slice()
            )
        );

        let b = &built[1];
        assert_eq!(b.split.data_files()[0].file_name, "file-b");
        assert_eq!(b.row_ranges, vec![RowRange::new(5, 5)]);
    }

    #[test]
    fn coalesces_consecutive_positions_into_one_range() {
        let splits = vec![search_split(0, vec![data_file("f", 10)])];
        let survivors = vec![
            cand(0, 0, "f", 0, 1.0),
            cand(0, 0, "f", 1, 1.0),
            cand(0, 0, "f", 2, 1.0),
            cand(0, 0, "f", 5, 1.0),
        ];
        let built = build_indexed_splits(survivors, &splits, VectorSearchMetric::L2).unwrap();
        assert_eq!(
            built[0].row_ranges,
            vec![RowRange::new(0, 2), RowRange::new(5, 5)]
        );
    }

    #[test]
    fn rejects_file_absent_from_bucket_split() {
        let splits = vec![search_split(0, vec![data_file("known", 10)])];
        let survivors = vec![cand(0, 0, "unknown", 0, 1.0)];
        // PkVectorIndexedSplit has no Debug; map the Ok value away so expect_err's
        // `T: Debug` bound is satisfied without touching the shared type.
        let err = build_indexed_splits(survivors, &splits, VectorSearchMetric::L2)
            .map(|_| ())
            .expect_err("unknown file must error");
        assert!(
            format!("{err:?}").contains("unknown") || format!("{err:?}").contains("not"),
            "got: {err:?}"
        );
    }

    #[test]
    fn rejects_duplicate_file_position() {
        let splits = vec![search_split(0, vec![data_file("f", 10)])];
        let survivors = vec![cand(0, 0, "f", 3, 1.0), cand(0, 0, "f", 3, 2.0)];
        let err = build_indexed_splits(survivors, &splits, VectorSearchMetric::L2)
            .map(|_| ())
            .expect_err("duplicate (file,pos) must error");
        assert!(format!("{err:?}").contains("duplicate"), "got: {err:?}");
    }

    #[test]
    fn rejects_same_group_key_from_different_splits() {
        // Two buckets share (partition, bucket, file_name) but sit at different
        // split_index. Silently merging them would materialize against the wrong
        // split; fail loud instead (defensive guard against malformed
        // input). Both search_splits have the empty partition + bucket 0 + file "f".
        let splits = vec![
            search_split(0, vec![data_file("f", 10)]),
            search_split(0, vec![data_file("f", 10)]),
        ];
        let survivors = vec![cand(0, 0, "f", 1, 1.0), cand(1, 0, "f", 2, 1.0)];
        let err = build_indexed_splits(survivors, &splits, VectorSearchMetric::L2)
            .map(|_| ())
            .expect_err("same group key from different splits must error");
        assert!(
            format!("{err:?}").contains("different splits")
                || format!("{err:?}").contains("distinct splits"),
            "got: {err:?}"
        );
    }
}

#[cfg(test)]
mod e2e_tests {
    use super::*;
    use crate::arrow::build_target_arrow_schema;
    use crate::io::{FileIO, FileIOBuilder};
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{
        DataField, DataFileMeta, DataType, IntType, PkVectorSourceFile, PkVectorSourceMeta,
    };
    use crate::table::pk_vector_indexed_split_read::PkVectorIndexedSplitRead;
    use crate::table::pk_vector_position_read::{
        PKEY_VECTOR_POSITION_COLUMN, PKEY_VECTOR_SCORE_COLUMN,
    };
    use crate::table::schema_manager::SchemaManager;
    use crate::table::source::DeletionFile;
    use crate::vindex::pkvector::reader::test_support::ArrayReader;
    use arrow_array::{Array, Float32Array, Int32Array, Int64Array, RecordBatch};
    use bytes::Bytes;
    use futures::TryStreamExt;
    use paimon_mosaic_core::spec::COMPRESSION_NONE;
    use paimon_mosaic_core::writer::{MosaicWriter, OutputFile, WriterOptions};
    use roaring::RoaringBitmap;
    use std::collections::HashSet;
    use std::io;

    struct MemOutputFile {
        data: Vec<u8>,
    }
    impl MemOutputFile {
        fn new() -> Self {
            Self { data: Vec::new() }
        }
    }
    impl OutputFile for MemOutputFile {
        fn write(&mut self, data: &[u8]) -> io::Result<()> {
            self.data.extend_from_slice(data);
            Ok(())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
        fn pos(&self) -> u64 {
            self.data.len() as u64
        }
    }

    fn id_field() -> DataField {
        DataField::new(0, "id".to_string(), DataType::Int(IntType::new()))
    }
    fn id_fields() -> Vec<DataField> {
        vec![id_field()]
    }
    fn id_batch(ids: Vec<i32>) -> RecordBatch {
        let schema = build_target_arrow_schema(&id_fields()).unwrap();
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids))]).unwrap()
    }

    fn data_file(file_name: &str, file_size: i64, row_count: i64) -> DataFileMeta {
        DataFileMeta {
            file_name: file_name.to_string(),
            file_size,
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
            first_row_id: Some(0),
            write_cols: None,
        }
    }

    fn write_mosaic_single_group(batch: &RecordBatch) -> Bytes {
        let out = MemOutputFile::new();
        let mut writer = MosaicWriter::new(
            out,
            batch.schema().as_ref(),
            WriterOptions {
                compression: COMPRESSION_NONE,
                num_buckets: 2,
                row_group_max_size: u64::MAX,
                ..Default::default()
            },
        )
        .unwrap();
        writer.write_batch(batch).unwrap();
        writer.close().unwrap();
        Bytes::from(writer.output().data.to_vec())
    }

    async fn write_deletion_file(
        file_io: &FileIO,
        path: &str,
        deleted_rows: &[u32],
    ) -> DeletionFile {
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
            .write(Bytes::from(blob))
            .await
            .unwrap();
        DeletionFile::new(
            path.to_string(),
            0,
            bitmap_length as i64,
            Some(deleted_rows.len() as i64),
        )
    }

    fn make_reader(file_io: FileIO, table_path: &str) -> DataFileReader {
        let schema_manager = SchemaManager::new(file_io.clone(), table_path.to_string());
        DataFileReader::new(
            file_io,
            schema_manager,
            1,
            id_fields(),
            id_fields(),
            Vec::new(),
        )
    }

    /// Write one mosaic data file into a bucket path and return its `DataFileMeta`.
    async fn write_file(
        file_io: &FileIO,
        bucket_path: &str,
        file_name: &str,
        ids: Vec<i32>,
    ) -> DataFileMeta {
        let row_count = ids.len() as i64;
        let data = write_mosaic_single_group(&id_batch(ids));
        file_io
            .new_output(&format!("{bucket_path}/{file_name}"))
            .unwrap()
            .write(data.clone())
            .await
            .unwrap();
        data_file(file_name, data.len() as i64, row_count)
    }

    fn ann_segment(sources: &[(&str, i64)]) -> BucketAnnSegment {
        BucketAnnSegment::for_test(
            PkVectorSourceMeta::new(
                1,
                sources
                    .iter()
                    .map(|(n, r)| PkVectorSourceFile::new((*n).to_string(), *r).unwrap())
                    .collect(),
            )
            .unwrap(),
        )
    }

    fn active(name: &str, rows: i64) -> BucketActiveFile {
        BucketActiveFile {
            file_name: name.to_string(),
            row_count: rows,
        }
    }

    fn column_by_name<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a Arc<dyn Array>> {
        batch
            .schema()
            .index_of(name)
            .ok()
            .map(|idx| batch.column(idx))
    }
    fn collect_i32(batches: &[RecordBatch], name: &str) -> Vec<i32> {
        batches
            .iter()
            .flat_map(|b| {
                column_by_name(b, name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }
    fn collect_i64(batches: &[RecordBatch], name: &str) -> Vec<i64> {
        batches
            .iter()
            .flat_map(|b| {
                column_by_name(b, name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }
    fn collect_f32(batches: &[RecordBatch], name: &str) -> Vec<f32> {
        batches
            .iter()
            .flat_map(|b| {
                column_by_name(b, name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    fn l2_score(distance: f32) -> f32 {
        VectorSearchMetric::L2.distance_to_score(distance)
    }

    // Fake ANN searcher returning preset hits.
    struct FakeAnn {
        hits: Vec<PkVectorSearchResult>,
    }
    impl PkVectorAnnSearcher for FakeAnn {
        fn search(
            &self,
            _segment: &BucketAnnSegment,
            _query: &[f32],
            _metric: VectorSearchMetric,
            _limit: usize,
            _active_source_files: &HashSet<String>,
            _dvs: &HashMap<String, Arc<DeletionVector>>,
            _opts: &HashMap<String, String>,
        ) -> crate::Result<Vec<PkVectorSearchResult>> {
            Ok(self.hits.clone())
        }
    }

    /// Run the eager per-bucket search + global Top-K, group survivors into indexed
    /// splits, then materialize each split in file/position order. This is the
    /// materialization path the production best-first read reorders on top of; the
    /// tests below drive it directly through its `pub(crate)` components.
    #[allow(clippy::too_many_arguments)]
    async fn materialize_via_splits(
        reader: DataFileReader,
        splits: &[PkVectorSearchSplit],
        query: &[f32],
        metric: VectorSearchMetric,
        limit: usize,
        ann: Option<&dyn PkVectorAnnSearcher>,
        factory: &mut (dyn FnMut(&BucketActiveFile) -> crate::Result<Box<dyn PkVectorReader>>
                  + Send),
        opts: &HashMap<String, String>,
    ) -> crate::Result<Vec<RecordBatch>> {
        let orch = PkVectorOrchestrator::new(reader.clone());
        // Wrap the per-file factory into the split-scoped shape search_candidates
        // expects; the split index/split are unused here.
        let mut wrapped = |_: usize, _: &PkVectorSearchSplit, f: &BucketActiveFile| factory(f);
        let survivors = orch
            .search_candidates(splits, query, metric, limit, ann, &mut wrapped, opts, false)
            .await?;
        let indexed_splits = build_indexed_splits(survivors, splits, metric)?;
        let mut out = Vec::new();
        for indexed in indexed_splits {
            let batches: Vec<RecordBatch> = PkVectorIndexedSplitRead::new(reader.clone())
                .read(&indexed)?
                .try_collect()
                .await?;
            out.extend(batches);
        }
        Ok(out)
    }

    #[tokio::test]
    async fn eager_rejects_zero_limit() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let reader = make_reader(file_io, "memory:/pkvo_zero");
        let splits: Vec<PkVectorSearchSplit> = Vec::new();
        let mut factory = |_: usize,
                           _: &PkVectorSearchSplit,
                           _: &BucketActiveFile|
         -> crate::Result<Box<dyn PkVectorReader>> {
            unreachable!("no bucket search on eager-rejected input")
        };
        let opts = HashMap::new();
        let err = PkVectorOrchestrator::new(reader)
            .search_candidates(
                &splits,
                &[0.0, 0.0],
                VectorSearchMetric::L2,
                0,
                None,
                &mut factory,
                &opts,
                false,
            )
            .await
            .map(|_| ())
            .expect_err("limit == 0 must be rejected eagerly");
        assert!(format!("{err:?}").contains("positive"), "got: {err:?}");
    }

    #[tokio::test]
    async fn eager_rejects_empty_query() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let reader = make_reader(file_io, "memory:/pkvo_empty_query");
        let splits: Vec<PkVectorSearchSplit> = Vec::new();
        let mut factory = |_: usize,
                           _: &PkVectorSearchSplit,
                           _: &BucketActiveFile|
         -> crate::Result<Box<dyn PkVectorReader>> {
            unreachable!("no bucket search on eager-rejected input")
        };
        let opts = HashMap::new();
        let err = PkVectorOrchestrator::new(reader)
            .search_candidates(
                &splits,
                &[],
                VectorSearchMetric::L2,
                5,
                None,
                &mut factory,
                &opts,
                false,
            )
            .await
            .map(|_| ())
            .expect_err("empty query must be rejected eagerly");
        assert!(format!("{err:?}").contains("empty"), "got: {err:?}");
    }

    #[tokio::test]
    async fn single_bucket_ann_plus_exact_merge_materializes_with_position_and_score() {
        // One bucket, two files: "ann.mosaic" is ANN-covered (FakeAnn hit at pos 1,
        // distance 0.25); "exact.mosaic" is exact fallback (ArrayReader). Covered
        // files are NOT re-scanned by the exact fallback.
        let table_path = "memory:/pkvo_single";
        let bucket_path = format!("{table_path}/bucket-0");
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let ann_meta = write_file(&file_io, &bucket_path, "ann.mosaic", vec![100, 101, 102]).await;
        let exact_meta = write_file(&file_io, &bucket_path, "exact.mosaic", vec![200, 201]).await;

        let data_split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(bucket_path)
            .with_total_buckets(1)
            .with_data_files(vec![ann_meta, exact_meta])
            .build()
            .unwrap();
        let split = PkVectorSearchSplit {
            data_split,
            ann_segments: vec![ann_segment(&[("ann.mosaic", 3)])],
            active_files: vec![active("ann.mosaic", 3), active("exact.mosaic", 2)],
        };

        let ann = FakeAnn {
            hits: vec![PkVectorSearchResult {
                data_file_name: "ann.mosaic".to_string(),
                row_position: 1,
                distance: 0.25,
            }],
        };
        // Exact fallback scans only "exact.mosaic": pos0 {1,0} d=1.0, pos1 {2,0} d=4.0.
        let mut factory = |f: &BucketActiveFile| -> crate::Result<Box<dyn PkVectorReader>> {
            let vectors = match f.file_name.as_str() {
                "exact.mosaic" => vec![Some(vec![1.0, 0.0]), Some(vec![2.0, 0.0])],
                other => panic!("unexpected exact scan of covered file {other}"),
            };
            Ok(Box::new(ArrayReader::new(2, vectors)))
        };
        let opts = HashMap::new();
        let batches = materialize_via_splits(
            make_reader(file_io, table_path),
            &[split],
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            3,
            Some(&ann),
            &mut factory,
            &opts,
        )
        .await
        .unwrap();

        // Output is ascending group (file name) then ascending position:
        // ann.mosaic pos1 -> id 101; exact.mosaic pos0,1 -> ids 200,201.
        assert_eq!(collect_i32(&batches, "id"), vec![101, 200, 201]);
        assert_eq!(
            collect_i64(&batches, PKEY_VECTOR_POSITION_COLUMN),
            vec![1, 0, 1]
        );
        assert_eq!(
            collect_f32(&batches, PKEY_VECTOR_SCORE_COLUMN),
            vec![l2_score(0.25), l2_score(1.0), l2_score(4.0)]
        );
        for batch in &batches {
            assert!(
                column_by_name(batch, "_ROW_ID").is_none(),
                "_ROW_ID must not leak"
            );
        }
    }

    #[tokio::test]
    async fn multi_bucket_merge_keeps_global_top_k() {
        // Two buckets, exact-only. limit=3 < 6 total hits; surviving rows are the
        // global-best 3 by distance across both buckets.
        let table_path = "memory:/pkvo_multi";
        let file_io = FileIOBuilder::new("memory").build().unwrap();

        let b0_path = format!("{table_path}/bucket-0");
        let b0_meta = write_file(&file_io, &b0_path, "b0.mosaic", vec![10, 11, 12]).await;
        let split0 = PkVectorSearchSplit {
            data_split: DataSplitBuilder::new()
                .with_snapshot(1)
                .with_partition(BinaryRow::new(0))
                .with_bucket(0)
                .with_bucket_path(b0_path)
                .with_total_buckets(2)
                .with_data_files(vec![b0_meta])
                .build()
                .unwrap(),
            ann_segments: Vec::new(),
            active_files: vec![active("b0.mosaic", 3)],
        };

        let b1_path = format!("{table_path}/bucket-1");
        let b1_meta = write_file(&file_io, &b1_path, "b1.mosaic", vec![20, 21, 22]).await;
        let split1 = PkVectorSearchSplit {
            data_split: DataSplitBuilder::new()
                .with_snapshot(1)
                .with_partition(BinaryRow::new(0))
                .with_bucket(1)
                .with_bucket_path(b1_path)
                .with_total_buckets(2)
                .with_data_files(vec![b1_meta])
                .build()
                .unwrap(),
            ann_segments: Vec::new(),
            active_files: vec![active("b1.mosaic", 3)],
        };

        // b0: x = 1,4,6 -> d = 1,16,36. b1: x = 2,3,5 -> d = 4,9,25.
        // Global best 3: d1 (b0 pos0 id10), d4 (b1 pos0 id20), d9 (b1 pos1 id21).
        let mut factory = |f: &BucketActiveFile| -> crate::Result<Box<dyn PkVectorReader>> {
            let vectors = match f.file_name.as_str() {
                "b0.mosaic" => vec![
                    Some(vec![1.0, 0.0]),
                    Some(vec![4.0, 0.0]),
                    Some(vec![6.0, 0.0]),
                ],
                "b1.mosaic" => vec![
                    Some(vec![2.0, 0.0]),
                    Some(vec![3.0, 0.0]),
                    Some(vec![5.0, 0.0]),
                ],
                other => panic!("unexpected file {other}"),
            };
            Ok(Box::new(ArrayReader::new(2, vectors)))
        };
        let opts = HashMap::new();
        let batches = materialize_via_splits(
            make_reader(file_io, table_path),
            &[split0, split1],
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            3,
            None,
            &mut factory,
            &opts,
        )
        .await
        .unwrap();

        // Ascending group order: bucket0 "b0.mosaic" pos0 -> 10; bucket1 "b1.mosaic"
        // pos0,1 -> 20,21.
        assert_eq!(collect_i32(&batches, "id"), vec![10, 20, 21]);
        assert_eq!(
            collect_i64(&batches, PKEY_VECTOR_POSITION_COLUMN),
            vec![0, 0, 1]
        );
        assert_eq!(
            collect_f32(&batches, PKEY_VECTOR_SCORE_COLUMN),
            vec![l2_score(1.0), l2_score(4.0), l2_score(9.0)]
        );
    }

    #[tokio::test]
    async fn dv_deleted_exact_position_is_absent_from_output() {
        // Exact fallback over "d.mosaic" with a DV deleting position 1. The deleted
        // position is absent; remaining position/score alignment holds.
        let table_path = "memory:/pkvo_dv";
        let bucket_path = format!("{table_path}/bucket-0");
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let meta = write_file(&file_io, &bucket_path, "d.mosaic", vec![30, 31, 32, 33]).await;
        let df = write_deletion_file(&file_io, &format!("{table_path}/index/dv-0"), &[1]).await;
        let data_split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(bucket_path)
            .with_total_buckets(1)
            .with_data_files(vec![meta])
            .with_data_deletion_files(vec![Some(df)])
            .build()
            .unwrap();
        let split = PkVectorSearchSplit {
            data_split,
            ann_segments: Vec::new(),
            active_files: vec![active("d.mosaic", 4)],
        };

        // pos0 {1,0} d=1, pos1 {2,0} d=4 (DELETED), pos2 {3,0} d=9, pos3 {0,0} d=0.
        let mut factory = |f: &BucketActiveFile| -> crate::Result<Box<dyn PkVectorReader>> {
            let vectors = match f.file_name.as_str() {
                "d.mosaic" => vec![
                    Some(vec![1.0, 0.0]),
                    Some(vec![2.0, 0.0]),
                    Some(vec![3.0, 0.0]),
                    Some(vec![0.0, 0.0]),
                ],
                other => panic!("unexpected file {other}"),
            };
            Ok(Box::new(ArrayReader::new(2, vectors)))
        };
        let opts = HashMap::new();
        let batches = materialize_via_splits(
            make_reader(file_io, table_path),
            &[split],
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            4,
            None,
            &mut factory,
            &opts,
        )
        .await
        .unwrap();

        // Position 1 (id 31) is absent. Remaining ascending positions 0,2,3.
        assert_eq!(collect_i32(&batches, "id"), vec![30, 32, 33]);
        assert_eq!(
            collect_i64(&batches, PKEY_VECTOR_POSITION_COLUMN),
            vec![0, 2, 3]
        );
        assert_eq!(
            collect_f32(&batches, PKEY_VECTOR_SCORE_COLUMN),
            vec![l2_score(1.0), l2_score(9.0), l2_score(0.0)]
        );
    }

    #[tokio::test]
    async fn output_is_file_position_order_not_best_first() {
        // Best-first order (by distance) differs from file/position order. Output must
        // be ascending file/position order.
        let table_path = "memory:/pkvo_order";
        let bucket_path = format!("{table_path}/bucket-0");
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let meta = write_file(&file_io, &bucket_path, "o.mosaic", vec![40, 41, 42]).await;
        let split = PkVectorSearchSplit {
            data_split: DataSplitBuilder::new()
                .with_snapshot(1)
                .with_partition(BinaryRow::new(0))
                .with_bucket(0)
                .with_bucket_path(bucket_path)
                .with_total_buckets(1)
                .with_data_files(vec![meta])
                .build()
                .unwrap(),
            ann_segments: Vec::new(),
            active_files: vec![active("o.mosaic", 3)],
        };

        // pos0 {3,0} d=9, pos1 {1,0} d=1, pos2 {2,0} d=4. Best-first = [1,2,0].
        let mut factory = |f: &BucketActiveFile| -> crate::Result<Box<dyn PkVectorReader>> {
            let vectors = match f.file_name.as_str() {
                "o.mosaic" => vec![
                    Some(vec![3.0, 0.0]),
                    Some(vec![1.0, 0.0]),
                    Some(vec![2.0, 0.0]),
                ],
                other => panic!("unexpected file {other}"),
            };
            Ok(Box::new(ArrayReader::new(2, vectors)))
        };
        let opts = HashMap::new();
        let batches = materialize_via_splits(
            make_reader(file_io, table_path),
            &[split],
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            3,
            None,
            &mut factory,
            &opts,
        )
        .await
        .unwrap();

        // Ascending physical position order, not best-first distance order.
        assert_eq!(collect_i32(&batches, "id"), vec![40, 41, 42]);
        assert_eq!(
            collect_i64(&batches, PKEY_VECTOR_POSITION_COLUMN),
            vec![0, 1, 2]
        );
        // Scores aligned to ascending position: d=9,1,4.
        assert_eq!(
            collect_f32(&batches, PKEY_VECTOR_SCORE_COLUMN),
            vec![l2_score(9.0), l2_score(1.0), l2_score(4.0)]
        );
    }

    #[tokio::test]
    async fn search_candidates_returns_best_first_survivors() {
        // One bucket, exact-only, three rows; limit 2. Best-first by distance.
        let table_path = "memory:/pkvo_candidates";
        let bucket_path = format!("{table_path}/bucket-0");
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let meta = write_file(&file_io, &bucket_path, "c.mosaic", vec![1, 2, 3]).await;
        let split = PkVectorSearchSplit {
            data_split: DataSplitBuilder::new()
                .with_snapshot(1)
                .with_partition(BinaryRow::new(0))
                .with_bucket(0)
                .with_bucket_path(bucket_path)
                .with_total_buckets(1)
                .with_data_files(vec![meta])
                .build()
                .unwrap(),
            ann_segments: Vec::new(),
            active_files: vec![active("c.mosaic", 3)],
        };
        // pos0 {3,0} d=9, pos1 {1,0} d=1, pos2 {2,0} d=4.
        let mut factory = |_: usize,
                           _: &PkVectorSearchSplit,
                           f: &BucketActiveFile|
         -> crate::Result<Box<dyn PkVectorReader>> {
            assert_eq!(f.file_name, "c.mosaic");
            Ok(Box::new(ArrayReader::new(
                2,
                vec![
                    Some(vec![3.0, 0.0]),
                    Some(vec![1.0, 0.0]),
                    Some(vec![2.0, 0.0]),
                ],
            )))
        };
        let opts = HashMap::new();
        let cands = PkVectorOrchestrator::new(make_reader(file_io, table_path))
            .search_candidates(
                &[split],
                &[0.0, 0.0],
                VectorSearchMetric::L2,
                2,
                None,
                &mut factory,
                &opts,
                false,
            )
            .await
            .unwrap();
        // Best-first: pos1 (d=1), pos2 (d=4).
        assert_eq!(
            cands
                .iter()
                .map(|c| (c.row_position, c.distance))
                .collect::<Vec<_>>(),
            vec![(1, 1.0), (2, 4.0)]
        );
    }

    #[tokio::test]
    async fn search_candidates_fast_mode_skips_exact_factory() {
        let table_path = "memory:/pkvo_fast";
        let bucket_path = format!("{table_path}/bucket-0");
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let meta = write_file(&file_io, &bucket_path, "f.mosaic", vec![1, 2]).await;
        let split = PkVectorSearchSplit {
            data_split: DataSplitBuilder::new()
                .with_snapshot(1)
                .with_partition(BinaryRow::new(0))
                .with_bucket(0)
                .with_bucket_path(bucket_path)
                .with_total_buckets(1)
                .with_data_files(vec![meta])
                .build()
                .unwrap(),
            ann_segments: Vec::new(),
            active_files: vec![active("f.mosaic", 2)],
        };
        let mut factory = |_: usize,
                           _: &PkVectorSearchSplit,
                           _: &BucketActiveFile|
         -> crate::Result<Box<dyn PkVectorReader>> {
            unreachable!("fast mode must not read exact")
        };
        let opts = HashMap::new();
        let cands = PkVectorOrchestrator::new(make_reader(file_io, table_path))
            .search_candidates(
                &[split],
                &[0.0, 0.0],
                VectorSearchMetric::L2,
                2,
                None,
                &mut factory,
                &opts,
                true,
            )
            .await
            .unwrap();
        assert!(cands.is_empty());
    }

    #[test]
    fn validate_row_position_bounds() {
        // In range.
        assert!(validate_row_position("f", 0, 3).is_ok());
        assert!(validate_row_position("f", 2, 3).is_ok());
        // Negative, at/over row count, and past i32::MAX all fail loud.
        assert!(validate_row_position("f", -1, 3).is_err());
        assert!(validate_row_position("f", 3, 3).is_err());
        assert!(validate_row_position("f", i32::MAX as i64 + 1, i64::MAX).is_err());
        let err = validate_row_position("data-1", 9, 3).unwrap_err();
        assert!(err.to_string().contains("out of range") && err.to_string().contains("data-1"));
    }
}
