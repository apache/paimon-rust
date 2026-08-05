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

//! Primary-key full-text search read foundation: the score-based candidate type,
//! its cross-bucket score-descending Top-K, and the FAST-only materialized read
//! orchestration (plan the buckets, search each, fuse by score, materialize the
//! winning physical rows best-score-first with a `__paimon_search_score` column).

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};

use arrow_array::RecordBatch;
use futures::{stream, TryStreamExt};

use crate::deletion_vector::DeletionVector;
use crate::io::FileIO;
use crate::spec::BinaryRow;
use crate::table::data_file_reader::DataFileReader;
use crate::table::pk_full_text_bucket_search::search_bucket;
use crate::table::pk_full_text_scan::{PrimaryKeyFullTextScanPlan, PrimaryKeyFullTextSearchSplit};
use crate::table::pk_vector_indexed_split_read::{PkVectorIndexedSplit, PkVectorIndexedSplitRead};
use crate::table::source::DataSplitBuilder;
use crate::table::vector_search_builder::{
    collect_ranked_rows, reorder_and_strip_position, RankedRow,
};
use crate::table::{ArrowRecordBatchStream, RowRange};

fn data_invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

/// A full-text search hit tagged with its source bucket. `partition`/`bucket` are
/// the cross-bucket merge dimensions carried explicitly (mirroring Java
/// `PrimaryKeySearchPosition`) so the tie-break does not have to re-derive them
/// from `split_index`; `split_index` is the re-association handle back to the
/// planned split. `score` is the final full-text relevance score (higher is
/// better) and is validated finite at construction.
#[derive(Clone)]
pub(crate) struct PrimaryKeyFullTextCandidate {
    pub(crate) split_index: usize,
    pub(crate) partition: BinaryRow,
    pub(crate) bucket: i32,
    pub(crate) score: f32,
    pub(crate) data_file_name: String,
    pub(crate) row_position: i64,
}

impl PrimaryKeyFullTextCandidate {
    /// Build a candidate, rejecting a non-finite `score` (NaN / ±Infinity).
    /// A non-finite score means a corrupt ranking (e.g. a malformed archive or a
    /// bogus scorer output) and would poison the score-descending order, so fail
    /// loud rather than emit it. Mirrors Java `PrimaryKeySearchPosition`.
    pub(crate) fn new(
        split_index: usize,
        partition: BinaryRow,
        bucket: i32,
        score: f32,
        data_file_name: String,
        row_position: i64,
    ) -> crate::Result<Self> {
        if !score.is_finite() {
            return Err(data_invalid(format!(
                "full-text search score must be finite, got {score} for {data_file_name} @ {row_position}"
            )));
        }
        Ok(Self {
            split_index,
            partition,
            bucket,
            score,
            data_file_name,
            row_position,
        })
    }
}

/// 5-level best-first (largest score = best) key. Level 1 orders `score`
/// DESCENDING via `f32::total_cmp` (comparing `other` against `self`) so higher
/// scores sort first; scores are already validated finite at construction. Level
/// 2 uses the partition's serialized bytes; Rust `Vec<u8>::cmp` is unsigned
/// lexicographic then shorter-is-less, exactly the spec's contract
/// (`[0x7f] < [0x80] < [0xff]`). The remaining levels break ties deterministically
/// by bucket, data file name, then physical row position.
fn candidate_cmp(a: &PrimaryKeyFullTextCandidate, b: &PrimaryKeyFullTextCandidate) -> Ordering {
    b.score
        .total_cmp(&a.score)
        .then_with(|| {
            a.partition
                .to_serialized_bytes()
                .cmp(&b.partition.to_serialized_bytes())
        })
        .then_with(|| a.bucket.cmp(&b.bucket))
        .then_with(|| a.data_file_name.cmp(&b.data_file_name))
        .then_with(|| a.row_position.cmp(&b.row_position))
}

/// Collect all candidates, order score-descending (best-first) with the
/// deterministic tie-break, and keep the best `limit`.
pub(crate) fn top_k_by_score(
    mut candidates: Vec<PrimaryKeyFullTextCandidate>,
    limit: usize,
) -> Vec<PrimaryKeyFullTextCandidate> {
    candidates.sort_by(candidate_cmp);
    candidates.truncate(limit);
    candidates
}

/// Group best-score survivors into one single-file `PkVectorIndexedSplit` per
/// `(partition, bucket, data_file)`, re-associating each file to its
/// `DataFileMeta` + aligned deletion file in the source bucket split. Mirrors the
/// vector `build_indexed_splits`, but carries the raw full-text scores verbatim
/// (`scores: Some(..)`) rather than applying `distance_to_score`: the full-text
/// scores are already final relevance scores, so any transform would corrupt them
/// (spec D1). Groups are emitted in ascending group-key order for a deterministic
/// file/position materialization order; the caller reorders back to best-first.
pub(crate) fn build_full_text_indexed_splits(
    survivors: Vec<PrimaryKeyFullTextCandidate>,
    splits: &[PrimaryKeyFullTextSearchSplit],
) -> crate::Result<Vec<PkVectorIndexedSplit>> {
    // Group key: (partition bytes, bucket, file name). BTreeMap keeps ascending
    // group order deterministically. Value: (split_index, Vec<(position, score)>).
    type GroupKey = (Vec<u8>, i32, String);
    let mut groups: BTreeMap<GroupKey, (usize, Vec<(i64, f32)>)> = BTreeMap::new();
    for candidate in survivors {
        let key = (
            candidate.partition.to_serialized_bytes(),
            candidate.bucket,
            candidate.data_file_name.clone(),
        );
        let entry = groups
            .entry(key)
            .or_insert_with(|| (candidate.split_index, Vec::new()));
        // A (partition, bucket, file) group must map to one source split.
        // Divergent split_index means malformed input (e.g. duplicate buckets);
        // materializing against the wrong split would be a silent wrong-read.
        if entry.0 != candidate.split_index {
            return Err(data_invalid(format!(
                "full-text search hits for {} map to different splits ({} and {})",
                candidate.data_file_name, entry.0, candidate.split_index
            )));
        }
        entry.1.push((candidate.row_position, candidate.score));
    }

    let mut out = Vec::with_capacity(groups.len());
    for ((_partition, _bucket, file_name), (split_index, mut hits)) in groups {
        // Sort positions ascending; reject a duplicate (file, position).
        hits.sort_by_key(|(pos, _)| *pos);
        for pair in hits.windows(2) {
            if pair[0].0 == pair[1].0 {
                return Err(data_invalid(format!(
                    "duplicate (file, position) in full-text search result: {} @ {}",
                    file_name, pair[0].0
                )));
            }
        }

        // Re-associate the file to its DataFileMeta + aligned deletion file.
        let source = &splits
            .get(split_index)
            .ok_or_else(|| {
                data_invalid(format!(
                    "full-text search hit references split index {split_index} out of range (splits: {})",
                    splits.len()
                ))
            })?
            .data_split;
        let file_idx = source
            .data_files()
            .iter()
            .position(|f| f.file_name == file_name)
            .ok_or_else(|| {
                data_invalid(format!(
                    "full-text search hit references data file {file_name} not present in its bucket split"
                ))
            })?;
        let file_meta = source.data_files()[file_idx].clone();
        let deletion_file = source
            .data_deletion_files()
            .and_then(|dfs| dfs.get(file_idx).cloned().flatten());

        // Every hit's physical position must be in range for its data file.
        for &(pos, _) in &hits {
            if pos < 0 || pos >= file_meta.row_count {
                return Err(data_invalid(format!(
                    "full-text search position {pos} is outside data file {file_name} row count {}",
                    file_meta.row_count
                )));
            }
        }

        // Coalesce ascending positions into inclusive ranges; scores are the raw
        // full-text scores aligned to ascending-position order.
        let mut row_ranges: Vec<RowRange> = Vec::new();
        let mut scores: Vec<f32> = Vec::with_capacity(hits.len());
        let mut start = hits[0].0;
        let mut end = hits[0].0;
        scores.push(hits[0].1);
        for &(pos, score) in &hits[1..] {
            if pos == end + 1 {
                end = pos;
            } else {
                row_ranges.push(RowRange::new(start, end));
                start = pos;
                end = pos;
            }
            scores.push(score);
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

/// The primary-key full-text route's search output plus the source context a
/// later materialization (or a hybrid fusion across routes) needs. `candidates`
/// are the best-score-first survivors; `splits` are the planned per-bucket source
/// splits their `split_index` refers into; `snapshot_id` is the snapshot the plan
/// resolved. The splits are borrowed from the originating plan (kept alive by the
/// caller) because `split_index` is only meaningful against that plan. Produced by
/// [`PrimaryKeyFullTextRead::search_route`].
pub(crate) struct PrimaryKeyFullTextRouteResult<'a> {
    pub(crate) candidates: Vec<PrimaryKeyFullTextCandidate>,
    // Read by the hybrid route consumer (fuses routes before materializing); the
    // materialized read reaches candidates directly and holds the plan itself.
    pub(crate) snapshot_id: i64,
    pub(crate) splits: &'a [PrimaryKeyFullTextSearchSplit],
}

/// FAST-only primary-key full-text materialized read. Given a planned set of
/// per-bucket search splits, it searches each bucket through the full-text archive
/// reader, fuses the hits cross-bucket by score, materializes the winning physical
/// rows, and re-orders them best-score-first with a `__paimon_search_score` column
/// (the internal `_PKEY_VECTOR_POSITION` column is stripped). Mirrors Java
/// `PrimaryKeyFullTextRead` feeding its result splits into an ordinary table read:
/// the search decides which rows, the reader decides which columns.
///
/// The planning + FAST-mode guard live in the caller (`FullTextSearchBuilder`);
/// this type is the Table-free search+materialize core (it holds only the
/// predicate-free materialization reader, the `FileIO`, and the table path for the
/// archive directory), so it can be driven end-to-end from a hand-built plan.
pub(crate) struct PrimaryKeyFullTextRead {
    file_io: FileIO,
    materialize_reader: DataFileReader,
    table_path: String,
}

impl PrimaryKeyFullTextRead {
    pub(crate) fn new(
        file_io: FileIO,
        materialize_reader: DataFileReader,
        table_path: String,
    ) -> Self {
        Self {
            file_io,
            materialize_reader,
            table_path,
        }
    }

    /// Search every planned bucket and fuse the hits by score into the global
    /// best-score-first Top-`limit`, WITHOUT materializing any rows. Returns the
    /// surviving candidates together with the route source context (the plan's
    /// resolved snapshot id and its per-bucket source splits) so a caller can fuse
    /// them with another route before materializing. [`read`](Self::read) is
    /// layered on top of this. `query` is passed verbatim to the archive reader
    /// (spec D2).
    pub(crate) async fn search_route<'p>(
        &self,
        plan: &'p PrimaryKeyFullTextScanPlan,
        query: &str,
        limit: usize,
    ) -> crate::Result<PrimaryKeyFullTextRouteResult<'p>> {
        // Every planned split must belong to the plan's resolved snapshot; mixing
        // snapshots would search/materialize physical rows against the wrong
        // version. The scan already pins one snapshot, so a mismatch is malformed
        // input — fail loud rather than read inconsistently (mirrors Java
        // `PrimaryKeyFullTextRead`).
        for split in &plan.splits {
            if split.data_split.snapshot_id() != plan.snapshot_id {
                return Err(data_invalid(format!(
                    "full-text search split snapshot id {} does not match plan snapshot {}",
                    split.data_split.snapshot_id(),
                    plan.snapshot_id
                )));
            }
        }

        // Per-bucket search -> collected candidates. The bucket DVs are loaded from
        // the same split the materialization later reads (an accepted redundancy
        // between the search and materialization phases, mirroring the vector path).
        let mut candidates: Vec<PrimaryKeyFullTextCandidate> = Vec::new();
        for (split_index, split) in plan.splits.iter().enumerate() {
            let dv_factory = self
                .materialize_reader
                .build_split_dv_factory(&split.data_split)
                .await?;
            let mut dvs: HashMap<String, DeletionVector> = HashMap::new();
            for file in split.data_split.data_files() {
                if let Some(dv) =
                    DataFileReader::deletion_vector_for_file(dv_factory.as_ref(), &file.file_name)
                {
                    dvs.insert(file.file_name.clone(), (*dv).clone());
                }
            }
            let mut hits = search_bucket(
                split,
                query,
                limit,
                &dvs,
                &self.file_io,
                &self.table_path,
                split_index,
            )
            .await?;
            candidates.append(&mut hits);
        }

        // Global cross-bucket fusion: score-descending Top-`limit`.
        let survivors = top_k_by_score(candidates, limit);
        Ok(PrimaryKeyFullTextRouteResult {
            candidates: survivors,
            snapshot_id: plan.snapshot_id,
            splits: &plan.splits,
        })
    }

    /// Search every planned bucket, fuse the hits by score into a global Top-`limit`,
    /// materialize the winning rows, and emit them best-score-first with the
    /// unified score column. An empty plan or an empty result yields an empty
    /// stream. `query` is passed verbatim to the archive reader (spec D2).
    pub(crate) async fn read(
        &self,
        plan: &PrimaryKeyFullTextScanPlan,
        query: &str,
        limit: usize,
    ) -> crate::Result<ArrowRecordBatchStream> {
        let survivors = self.search_route(plan, query, limit).await?.candidates;
        if survivors.is_empty() {
            return Ok(Box::pin(stream::empty()));
        }

        // Rank each survivor by its best-first position, keyed by its FULL physical
        // position `(partition bytes, bucket, file, row position)` so the physical
        // materialization order can be reduced back to best-first (gap 8). Keying on
        // just file+position would collide across partitions/buckets.
        let mut rank_of: HashMap<(Vec<u8>, i32, String, i64), usize> = HashMap::new();
        for (rank, c) in survivors.iter().enumerate() {
            rank_of.insert(
                (
                    c.partition.to_serialized_bytes(),
                    c.bucket,
                    c.data_file_name.clone(),
                    c.row_position,
                ),
                rank,
            );
        }

        let indexed_splits = build_full_text_indexed_splits(survivors, &plan.splits)?;

        // Materialize every indexed split, retaining each batch and, per row, the
        // (rank, batch_index, row_index) tuple so we can reorder to best-first.
        // Top-K is small, so full in-memory collection is acceptable.
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut ranked: Vec<RankedRow> = Vec::new();
        for indexed in indexed_splits {
            let partition_bytes = indexed.split.partition().to_serialized_bytes();
            let bucket = indexed.split.bucket();
            let file_name = indexed.split.data_files()[0].file_name.clone();
            let mut stream =
                PkVectorIndexedSplitRead::new(self.materialize_reader.clone()).read(&indexed)?;
            while let Some(batch) = stream.try_next().await? {
                let batch_index = batches.len();
                collect_ranked_rows(
                    &batch,
                    batch_index,
                    &partition_bytes,
                    bucket,
                    &file_name,
                    &rank_of,
                    &mut ranked,
                )?;
                batches.push(batch);
            }
        }

        // Reorder to best-first and drop the internal position column.
        let output = reorder_and_strip_position(&batches, ranked)?;
        Ok(Box::pin(stream::iter(output.into_iter().map(Ok))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::BinaryRow;

    fn candidate(
        split_index: usize,
        partition_bytes: Vec<u8>,
        bucket: i32,
        file: &str,
        pos: i64,
        score: f32,
    ) -> PrimaryKeyFullTextCandidate {
        PrimaryKeyFullTextCandidate::new(
            split_index,
            BinaryRow::from_bytes(1, partition_bytes),
            bucket,
            score,
            file.to_string(),
            pos,
        )
        .unwrap()
    }

    fn ids(c: &[PrimaryKeyFullTextCandidate]) -> Vec<(i32, String, i64)> {
        c.iter()
            .map(|c| (c.bucket, c.data_file_name.clone(), c.row_position))
            .collect()
    }

    #[test]
    fn orders_score_descending() {
        let part = vec![0x00];
        let out = top_k_by_score(
            vec![
                candidate(0, part.clone(), 0, "f", 0, 1.0),
                candidate(0, part.clone(), 0, "f", 1, 3.0),
                candidate(0, part.clone(), 0, "f", 2, 2.0),
            ],
            3,
        );
        assert_eq!(
            out.iter().map(|c| c.score).collect::<Vec<f32>>(),
            vec![3.0, 2.0, 1.0]
        );
    }

    #[test]
    fn tie_break_orders_partition_bytes_as_unsigned() {
        // Equal score so partition bytes decide: 0x7f < 0x80 < 0xff (unsigned).
        let out = top_k_by_score(
            vec![
                candidate(2, vec![0xff], 0, "f", 0, 1.0),
                candidate(0, vec![0x7f], 0, "f", 0, 1.0),
                candidate(1, vec![0x80], 0, "f", 0, 1.0),
            ],
            3,
        );
        assert_eq!(
            out.iter()
                .map(|c| c.partition.to_serialized_bytes().pop().unwrap())
                .collect::<Vec<u8>>(),
            vec![0x7f, 0x80, 0xff]
        );
    }

    #[test]
    fn tie_break_orders_bucket_then_file_then_position() {
        // Equal score and equal partition; lower levels decide in order:
        // bucket asc, then data_file_name asc, then row_position asc.
        let part = vec![0x00];
        let out = top_k_by_score(
            vec![
                candidate(0, part.clone(), 1, "b", 9, 5.0),
                candidate(0, part.clone(), 0, "b", 0, 5.0),
                candidate(0, part.clone(), 0, "a", 7, 5.0),
                candidate(0, part.clone(), 0, "a", 3, 5.0),
            ],
            4,
        );
        assert_eq!(
            ids(&out),
            vec![
                (0, "a".to_string(), 3),
                (0, "a".to_string(), 7),
                (0, "b".to_string(), 0),
                (1, "b".to_string(), 9),
            ]
        );
    }

    #[test]
    fn truncates_to_limit() {
        let part = vec![0x00];
        let out = top_k_by_score(
            vec![
                candidate(0, part.clone(), 0, "f", 0, 1.0),
                candidate(0, part.clone(), 0, "f", 1, 3.0),
                candidate(0, part.clone(), 0, "f", 2, 2.0),
            ],
            1,
        );
        // Only the single best score survives.
        assert_eq!(ids(&out), vec![(0, "f".to_string(), 1)]);
        assert_eq!(out[0].score, 3.0);
    }

    #[test]
    fn empty_candidates_yield_empty() {
        assert!(top_k_by_score(Vec::new(), 5).is_empty());
    }

    #[test]
    fn new_rejects_non_finite_scores() {
        let mk = |score: f32| {
            PrimaryKeyFullTextCandidate::new(0, BinaryRow::new(0), 0, score, "f".to_string(), 0)
        };
        assert!(mk(f32::NAN).is_err());
        assert!(mk(f32::INFINITY).is_err());
        assert!(mk(f32::NEG_INFINITY).is_err());
        // A finite score is accepted.
        assert!(mk(1.5).is_ok());
    }
}

#[cfg(test)]
mod read_tests {
    use super::*;
    use crate::arrow::build_target_arrow_schema;
    use crate::io::{FileIO, FileIOBuilder};
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{DataField, DataFileMeta, DataType, GlobalIndexMeta, IndexFileMeta, IntType};
    use crate::table::pk_full_text_bucket_state::PK_FULL_TEXT_INDEX_TYPE;
    use crate::table::pk_full_text_scan::PrimaryKeyFullTextSearchSplit;
    use crate::table::pk_vector_position_read::{PKEY_VECTOR_POSITION_COLUMN, SEARCH_SCORE_COLUMN};
    use crate::table::schema_manager::SchemaManager;
    use crate::table::source::{DataSplit, DataSplitBuilder, DeletionFile};
    use arrow_array::{Array, Float32Array, Int32Array, RecordBatch};
    use bytes::Bytes;
    use paimon_ftindex_core::io::PosWriter;
    use paimon_ftindex_core::{FullTextIndexConfig, FullTextIndexWriter};
    use paimon_mosaic_core::spec::COMPRESSION_NONE;
    use paimon_mosaic_core::writer::{MosaicWriter, OutputFile, WriterOptions};
    use std::io;
    use std::sync::Arc;

    const FILE_SOURCE_COMPACT: i32 = 1;

    // ---- helpers ----

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

    fn write_mosaic(batch: &RecordBatch) -> Bytes {
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

    /// Build a tiny full-text archive in memory via the core writer.
    fn build_archive(docs: &[(i64, &str)]) -> Vec<u8> {
        let mut writer = FullTextIndexWriter::new(FullTextIndexConfig::new()).unwrap();
        for (row_id, text) in docs {
            writer.add_document(*row_id, (*text).to_string()).unwrap();
        }
        let mut out = PosWriter::new(Vec::<u8>::new());
        writer.write(&mut out).unwrap();
        out.into_inner()
    }

    async fn write_bytes(file_io: &FileIO, path: &str, bytes: Vec<u8>) {
        file_io
            .new_output(path)
            .unwrap()
            .write(Bytes::from(bytes))
            .await
            .unwrap();
    }

    /// One Java `DataOutput#writeUTF` value (u16-BE length + modified UTF-8).
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

    /// A `_SOURCE_META` frame (version 1) for a level and its ordered sources.
    fn frame(data_level: i32, files: &[(&str, i64)]) -> Vec<u8> {
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

    fn ft_payload(file_name: &str, files: &[(&str, i64)]) -> IndexFileMeta {
        let total: i64 = files.iter().map(|(_, r)| *r).sum();
        IndexFileMeta {
            index_type: PK_FULL_TEXT_INDEX_TYPE.into(),
            file_name: file_name.into(),
            file_size: 1,
            row_count: total as i32,
            deletion_vectors_ranges: None,
            global_index_meta: Some(GlobalIndexMeta {
                row_range_start: 0,
                row_range_end: 0,
                index_field_id: 1,
                extra_field_ids: None,
                index_meta: None,
                source_meta: Some(frame(1, files)),
                build_schema_id: None,
            }),
        }
    }

    fn data_file(file_name: &str, file_size: i64, row_count: i64) -> DataFileMeta {
        DataFileMeta {
            file_name: file_name.into(),
            file_size,
            row_count,
            min_key: Vec::new(),
            max_key: Vec::new(),
            key_stats: BinaryTableStats::empty(),
            value_stats: BinaryTableStats::empty(),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id: 1,
            level: 1,
            extra_files: Vec::new(),
            creation_time: None,
            delete_row_count: None,
            embedded_index: None,
            file_source: Some(FILE_SOURCE_COMPACT),
            value_stats_cols: None,
            external_path: None,
            first_row_id: Some(0),
            write_cols: None,
        }
    }

    async fn write_deletion_file(
        file_io: &FileIO,
        path: &str,
        deleted_rows: &[u32],
    ) -> DeletionFile {
        const MAGIC_NUMBER: i32 = 1581511376;
        let mut bitmap = roaring::RoaringBitmap::new();
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

    /// Build the physical mosaic + a single-file `DataSplit`, plus a predicate-free
    /// `DataFileReader` projecting `id`. `deleted_rows`, when non-empty, attaches a DV.
    async fn build_data(
        file_io: &FileIO,
        table_path: &str,
        ids: Vec<i32>,
        deleted_rows: &[u32],
    ) -> (DataFileReader, DataSplit) {
        let bucket_path = format!("{table_path}/bucket-0");
        let file_name = "d0.mosaic";
        let row_count = ids.len() as i64;
        let data = write_mosaic(&id_batch(ids));
        write_bytes(
            file_io,
            &format!("{bucket_path}/{file_name}"),
            data.to_vec(),
        )
        .await;

        let mut split_builder = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(bucket_path)
            .with_total_buckets(1)
            .with_data_files(vec![data_file(file_name, data.len() as i64, row_count)]);
        if !deleted_rows.is_empty() {
            let df =
                write_deletion_file(file_io, &format!("{table_path}/index/dv-0"), deleted_rows)
                    .await;
            split_builder = split_builder.with_data_deletion_files(vec![Some(df)]);
        }
        let split = split_builder.build().unwrap();

        let schema_manager = SchemaManager::new(file_io.clone(), table_path.to_string());
        let reader = DataFileReader::new(
            file_io.clone(),
            schema_manager,
            1,
            id_fields(),
            id_fields(),
            Vec::new(),
        );
        (reader, split)
    }

    fn column_i32(batch: &RecordBatch, name: &str) -> Vec<i32> {
        let idx = batch.schema().index_of(name).unwrap();
        batch
            .column(idx)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    async fn collect(stream: ArrowRecordBatchStream) -> Vec<RecordBatch> {
        stream.try_collect::<Vec<_>>().await.unwrap()
    }

    // ---- build_full_text_indexed_splits: raw scores, no distance transform ----
    #[test]
    fn build_splits_carries_raw_scores_aligned_by_position() {
        let split = PrimaryKeyFullTextSearchSplit::new(
            DataSplitBuilder::new()
                .with_snapshot(1)
                .with_partition(BinaryRow::new(0))
                .with_bucket(0)
                .with_bucket_path("memory:/t/bucket-0".to_string())
                .with_total_buckets(1)
                .with_data_files(vec![data_file("d0.mosaic", 1, 4)])
                .build()
                .unwrap(),
            vec![ft_payload("ft-0", &[("d0.mosaic", 4)])],
            Vec::new(),
        )
        .unwrap();

        // Out-of-order candidates; scores must survive verbatim, aligned to
        // ascending position order (no distance_to_score transform).
        let cands = vec![
            PrimaryKeyFullTextCandidate::new(0, BinaryRow::new(0), 0, 0.1, "d0.mosaic".into(), 3)
                .unwrap(),
            PrimaryKeyFullTextCandidate::new(0, BinaryRow::new(0), 0, 0.9, "d0.mosaic".into(), 0)
                .unwrap(),
            PrimaryKeyFullTextCandidate::new(0, BinaryRow::new(0), 0, 0.5, "d0.mosaic".into(), 2)
                .unwrap(),
        ];
        let out = build_full_text_indexed_splits(cands, std::slice::from_ref(&split)).unwrap();
        assert_eq!(out.len(), 1);
        // positions 0,2,3 -> ranges [0,0],[2,3]; scores aligned ascending: 0.9,0.5,0.1.
        assert_eq!(
            out[0].row_ranges,
            vec![RowRange::new(0, 0), RowRange::new(2, 3)]
        );
        assert_eq!(out[0].scores, Some(vec![0.9, 0.5, 0.1]));
    }

    #[test]
    fn build_splits_rejects_duplicate_position() {
        let split = PrimaryKeyFullTextSearchSplit::new(
            DataSplitBuilder::new()
                .with_snapshot(1)
                .with_partition(BinaryRow::new(0))
                .with_bucket(0)
                .with_bucket_path("memory:/t/bucket-0".to_string())
                .with_total_buckets(1)
                .with_data_files(vec![data_file("d0.mosaic", 1, 4)])
                .build()
                .unwrap(),
            vec![ft_payload("ft-0", &[("d0.mosaic", 4)])],
            Vec::new(),
        )
        .unwrap();
        let cands = vec![
            PrimaryKeyFullTextCandidate::new(0, BinaryRow::new(0), 0, 0.9, "d0.mosaic".into(), 1)
                .unwrap(),
            PrimaryKeyFullTextCandidate::new(0, BinaryRow::new(0), 0, 0.5, "d0.mosaic".into(), 1)
                .unwrap(),
        ];
        assert!(build_full_text_indexed_splits(cands, std::slice::from_ref(&split)).is_err());
    }

    // ---- (a) round-trip: archive -> search -> materialize -> best-score-first ----
    #[tokio::test]
    async fn execute_read_materializes_rows_best_score_first() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/pk_ft_roundtrip";
        // pos2 ("alpha alpha alpha") scores higher for "alpha" than pos0 ("alpha").
        let archive = build_archive(&[
            (0, "alpha"),
            (1, "beta"),
            (2, "alpha alpha alpha"),
            (3, "gamma"),
        ]);
        write_bytes(&file_io, &format!("{table_path}/index/ft-0"), archive).await;

        let (reader, split) = build_data(&file_io, table_path, vec![100, 101, 102, 103], &[]).await;
        let plan = PrimaryKeyFullTextScanPlan {
            snapshot_id: 1,
            splits: vec![PrimaryKeyFullTextSearchSplit::new(
                split,
                vec![ft_payload("ft-0", &[("d0.mosaic", 4)])],
                Vec::new(),
            )
            .unwrap()],
        };

        let read = PrimaryKeyFullTextRead::new(file_io.clone(), reader, table_path.to_string());
        let batches = collect(
            read.read(&plan, r#"{"match":{"query":"alpha"}}"#, 10)
                .await
                .unwrap(),
        )
        .await;

        // Best-score-first: pos2 (id 102, higher tf) then pos0 (id 100).
        assert_eq!(column_i32(&batches[0], "id"), vec![102, 100]);
        // The unified score column is present; the internal position column is not.
        assert!(batches[0].schema().index_of(SEARCH_SCORE_COLUMN).is_ok());
        assert!(batches[0]
            .schema()
            .index_of(PKEY_VECTOR_POSITION_COLUMN)
            .is_err());
        // Scores are descending (best first) and finite.
        let scores: Vec<f32> = batches
            .iter()
            .flat_map(|b| {
                let idx = b.schema().index_of(SEARCH_SCORE_COLUMN).unwrap();
                b.column(idx)
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert!(
            scores[0] >= scores[1],
            "scores must be best-first: {scores:?}"
        );
    }

    // ---- (b) a DV-deleted row is absent from the materialized result ----
    #[tokio::test]
    async fn execute_read_excludes_dv_deleted_row() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/pk_ft_dv";
        let archive = build_archive(&[
            (0, "alpha"),
            (1, "beta"),
            (2, "alpha alpha alpha"),
            (3, "gamma"),
        ]);
        write_bytes(&file_io, &format!("{table_path}/index/ft-0"), archive).await;

        // Delete physical position 2 (the strong "alpha" hit) -> only pos0 survives.
        let (reader, split) =
            build_data(&file_io, table_path, vec![100, 101, 102, 103], &[2]).await;
        let plan = PrimaryKeyFullTextScanPlan {
            snapshot_id: 1,
            splits: vec![PrimaryKeyFullTextSearchSplit::new(
                split,
                vec![ft_payload("ft-0", &[("d0.mosaic", 4)])],
                Vec::new(),
            )
            .unwrap()],
        };

        let read = PrimaryKeyFullTextRead::new(file_io.clone(), reader, table_path.to_string());
        let batches = collect(
            read.read(&plan, r#"{"match":{"query":"alpha"}}"#, 10)
                .await
                .unwrap(),
        )
        .await;

        let ids: Vec<i32> = batches.iter().flat_map(|b| column_i32(b, "id")).collect();
        assert_eq!(ids, vec![100], "DV-deleted row (id 102) must be absent");
    }

    // ---- empty result -> empty stream (no candidates) ----
    #[tokio::test]
    async fn execute_read_no_match_yields_empty_stream() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/pk_ft_empty";
        let archive = build_archive(&[(0, "alpha"), (1, "beta")]);
        write_bytes(&file_io, &format!("{table_path}/index/ft-0"), archive).await;
        let (reader, split) = build_data(&file_io, table_path, vec![100, 101], &[]).await;
        let plan = PrimaryKeyFullTextScanPlan {
            snapshot_id: 1,
            splits: vec![PrimaryKeyFullTextSearchSplit::new(
                split,
                vec![ft_payload("ft-0", &[("d0.mosaic", 2)])],
                Vec::new(),
            )
            .unwrap()],
        };
        let read = PrimaryKeyFullTextRead::new(file_io.clone(), reader, table_path.to_string());
        let batches = collect(
            read.read(&plan, r#"{"match":{"query":"zeta"}}"#, 10)
                .await
                .unwrap(),
        )
        .await;
        assert!(batches.is_empty(), "no match must yield an empty stream");
    }

    // ---- search_route: candidate-only producer returns candidates + context ----
    #[tokio::test]
    async fn search_route_returns_candidates_and_source_context() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/pk_ft_route";
        // pos2 ("alpha alpha alpha") scores higher for "alpha" than pos0 ("alpha").
        let archive = build_archive(&[
            (0, "alpha"),
            (1, "beta"),
            (2, "alpha alpha alpha"),
            (3, "gamma"),
        ]);
        write_bytes(&file_io, &format!("{table_path}/index/ft-0"), archive).await;

        let (reader, split) = build_data(&file_io, table_path, vec![100, 101, 102, 103], &[]).await;
        let plan = PrimaryKeyFullTextScanPlan {
            snapshot_id: 1,
            splits: vec![PrimaryKeyFullTextSearchSplit::new(
                split,
                vec![ft_payload("ft-0", &[("d0.mosaic", 4)])],
                Vec::new(),
            )
            .unwrap()],
        };

        let read = PrimaryKeyFullTextRead::new(file_io.clone(), reader, table_path.to_string());
        let route = read
            .search_route(&plan, r#"{"match":{"query":"alpha"}}"#, 10)
            .await
            .unwrap();

        // Two docs contain "alpha" -> two best-score-first candidates, no
        // materialization performed.
        assert_eq!(route.candidates.len(), 2, "both alpha hits must survive");
        assert!(
            route.candidates[0].score >= route.candidates[1].score,
            "candidates must be best-score-first: {:?}",
            route.candidates.iter().map(|c| c.score).collect::<Vec<_>>()
        );
        // The strong "alpha alpha alpha" hit (physical position 2) ranks first.
        assert_eq!(route.candidates[0].row_position, 2);
        assert_eq!(route.candidates[1].row_position, 0);

        // Source context: the plan's snapshot and its per-bucket source splits are
        // carried through so a caller can materialize (or fuse) without the plan.
        assert_eq!(route.snapshot_id, 1);
        assert_eq!(route.splits.len(), 1, "one bucket split expected");
        // The candidate's split_index refers into the returned splits.
        assert!(route
            .candidates
            .iter()
            .all(|c| c.split_index < route.splits.len()));
    }

    /// A real (non-empty) plan whose query matches nothing still reports the
    /// plan's pinned snapshot id and its source splits — a zero-candidate route
    /// must not lose the snapshot the cross-route consistency guard requires.
    #[tokio::test]
    async fn search_route_zero_candidates_still_carries_snapshot() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/pk_ft_route_empty";
        let archive = build_archive(&[(0, "alpha"), (1, "beta")]);
        write_bytes(&file_io, &format!("{table_path}/index/ft-0"), archive).await;

        let (reader, split) = build_data(&file_io, table_path, vec![100, 101], &[]).await;
        let plan = PrimaryKeyFullTextScanPlan {
            snapshot_id: 1,
            splits: vec![PrimaryKeyFullTextSearchSplit::new(
                split,
                vec![ft_payload("ft-0", &[("d0.mosaic", 2)])],
                Vec::new(),
            )
            .unwrap()],
        };

        let read = PrimaryKeyFullTextRead::new(file_io.clone(), reader, table_path.to_string());
        let route = read
            .search_route(&plan, r#"{"match":{"query":"zeta"}}"#, 10)
            .await
            .unwrap();

        assert!(route.candidates.is_empty(), "no doc matches 'zeta'");
        assert_eq!(route.snapshot_id, 1, "real snapshot id survives zero hits");
        assert_eq!(route.splits.len(), 1, "source splits still carried through");
    }
}
