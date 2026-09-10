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

//! Primary-key vector indexed-split read-path contract.
//!
//! `PkVectorIndexedSplit` carries one data file + inclusive physical-position
//! ranges + an optional aligned score array. `PkVectorIndexedSplitRead` validates
//! the split, expands the ranges into an ascending position set and a
//! `position -> score` map, and delegates to the sibling `PkVectorPositionRead`.
//! It is a pure consumer: no bucket/ANN search, no cross-bucket merge, no
//! serialization.

use std::collections::BTreeMap;

use futures::StreamExt;

use crate::spec::DataFileMeta;
use crate::table::data_file_reader::DataFileReader;
use crate::table::pk_vector_position_read::PkVectorPositionRead;
use crate::table::source::DataSplit;
use crate::table::{ArrowRecordBatchStream, RowRange};

fn data_invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

/// One data file, the physical rows of it a primary-key vector search selected, and
/// their scores.
///
/// Carries the same three payloads as Java `org.apache.paimon.globalindex.IndexedSplit`
/// -- the data split, the selected ranges, the aligned scores -- and is what a search
/// hands back for an ordinary read to consume. Unlike Java's, which has a public
/// constructor and hands out its list and array by reference, this one is read-only
/// from outside:
///
/// * `row_ranges` are 0-based PHYSICAL positions within the one data file, inclusive
///   on both ends, strictly ascending and non-overlapping. That is the OPPOSITE of
///   what [`DataSplit::row_ranges`](crate::table::DataSplit) means, which is stable
///   global row ids on the append/data-evolution path. Making the field writable would
///   turn that distinction from an invariant into a comment.
/// * The read expands the ranges into one position per row before touching storage, so
///   a caller-built range is a caller-built allocation. A search's ranges are Top-K
///   sized; an arbitrary one is not.
/// * `scores`, when present, is aligned to the expanded-range order. Nothing outside
///   the crate can keep that alignment true.
///
/// So: readable from anywhere, constructed only by a search. And not serialized, which
/// is the largest difference from Java's: `IndexedSplit` has its own MAGIC/VERSION wire
/// frame and goes through `SplitSerializer`, so a Java plan can cross a process
/// boundary. This one cannot, because both halves of the read run in one address space.
#[derive(Clone)]
pub struct PkVectorIndexedSplit {
    pub(crate) split: DataSplit,
    pub(crate) row_ranges: Vec<RowRange>,
    pub(crate) scores: Option<Vec<f32>>,
}

impl PkVectorIndexedSplit {
    /// The bucket split this file belongs to: its partition, bucket, and the one data
    /// file the selected rows live in.
    pub fn data_split(&self) -> &DataSplit {
        &self.split
    }

    /// The selected rows, as inclusive file-local PHYSICAL position ranges, ascending.
    pub fn row_ranges(&self) -> &[RowRange] {
        &self.row_ranges
    }

    /// The score of each selected row, in the same ascending-position order the ranges
    /// expand to. Always `Some` for a split a search returned;
    /// [`TableRead::to_arrow_indexed`](crate::table::TableRead::to_arrow_indexed) rejects
    /// one without scores, because its declared output schema always carries
    /// `__paimon_search_score`.
    pub fn scores(&self) -> Option<&[f32]> {
        self.scores.as_deref()
    }
}

/// One read's splits must name one snapshot, and must not name a file twice.
///
/// Concatenating one search result per bucket is the documented way to fan in, and a
/// bare slice carries none of the invariants Java's result object holds for it.
///
/// What this CANNOT catch, because a bare slice has no provenance: a search that matched
/// nothing returns no splits, so its snapshot never reaches the slice -- an empty result
/// from one snapshot mixed with hits from another is indistinguishable from a search
/// over the second alone. Nor can it see across C handles: each search returns its own,
/// and a C caller that reads each handle separately gets each validated alone. Both are
/// consequences of returning splits rather than Java's snapshot-bearing result, which
/// was the settled shape.
///
/// What it does check:
///
/// * **One snapshot.** Java checks every source split against the snapshot its result
///   owns (`PrimaryKeyScoredResult.sourceFiles`). Without this, a caller merging across
///   a snapshot boundary would silently read rows from two table states.
/// * **No repeated file.** Java rejects a duplicate physical position outright
///   (`PrimaryKeyScoredResult`, "contains duplicate physical position"). Here the same
///   split arriving twice -- a retried worker, an overlapping fan-in -- would read its
///   rows twice and return duplicates that look like real hits. Each split holds
///   exactly one data file, so one `(partition, bucket, file)` per split is the whole
///   check.
pub(crate) fn validate_fan_in(splits: &[PkVectorIndexedSplit]) -> crate::Result<()> {
    let mut ids = splits.iter().map(|s| s.split.snapshot_id());
    if let Some(first) = ids.next() {
        for id in ids {
            if id != first {
                return Err(data_invalid(format!(
                    "indexed splits must all come from one snapshot, got {first} and {id}"
                )));
            }
        }
    }

    let mut seen = std::collections::HashSet::with_capacity(splits.len());
    for split in splits {
        // A split that does not hold exactly one file is malformed, but saying so is
        // `validate_and_expand`'s job -- it has the precise message. Skip rather than
        // index, so a malformed split reports itself instead of panicking here.
        let Some(file) = split.split.data_files().first().map(|f| &f.file_name) else {
            continue;
        };
        let key = (
            split.split.partition().to_serialized_bytes(),
            split.split.bucket(),
            file.as_str(),
        );
        if !seen.insert(key) {
            return Err(data_invalid(format!(
                "indexed splits must not name the same data file twice, got {file} in bucket {} \
                 more than once",
                split.split.bucket()
            )));
        }
    }
    Ok(())
}

/// Expand inclusive physical-position ranges into an ascending `Vec<i64>` of
/// positions, validating bounds and ordering. Ranges must be non-empty, each
/// within `[0, row_count)`, strictly ascending and non-overlapping (touching
/// ranges allowed). Expansion is inclusive `from..=to`.
pub(crate) fn expand_ranges(ranges: &[RowRange], row_count: i64) -> crate::Result<Vec<i64>> {
    if ranges.is_empty() {
        return Err(data_invalid("indexed split must select at least one row"));
    }
    let mut positions = Vec::new();
    let mut prev_to: Option<i64> = None;
    for range in ranges {
        if range.from() < 0 || range.to() >= row_count {
            return Err(data_invalid(format!(
                "indexed-split range [{}, {}] is outside [0, {})",
                range.from(),
                range.to(),
                row_count
            )));
        }
        if let Some(prev) = prev_to {
            // Rejects overlap AND descending order; touching ranges
            // (range.from() == prev + 1) are accepted.
            if range.from() <= prev {
                return Err(data_invalid(
                    "indexed-split ranges must be ascending and non-overlapping",
                ));
            }
        }
        for position in range.from()..=range.to() {
            positions.push(position);
        }
        prev_to = Some(range.to());
    }
    Ok(positions)
}

/// Eagerly validate the split and expand it into `(file_meta, positions, scores)`.
/// Single-file check, range bounds/ordering, and score-length alignment happen
/// here so their errors surface at the `read(...)` call site.
#[allow(clippy::type_complexity)]
fn validate_and_expand(
    split: &PkVectorIndexedSplit,
) -> crate::Result<(DataFileMeta, Vec<i64>, Option<BTreeMap<i64, f32>>)> {
    let files = split.split.data_files();
    if files.len() != 1 {
        return Err(data_invalid(
            "indexed split for a primary-key table must contain exactly one data file",
        ));
    }
    let file_meta = files[0].clone();
    let positions = expand_ranges(&split.row_ranges, file_meta.row_count)?;

    let score_map = match &split.scores {
        Some(scores) => {
            if scores.len() != positions.len() {
                return Err(data_invalid(format!(
                    "indexed-split scores length {} does not match selected positions {}",
                    scores.len(),
                    positions.len()
                )));
            }
            // positions is ascending == expanded-range order; zip aligns each
            // score to its position.
            Some(
                positions
                    .iter()
                    .copied()
                    .zip(scores.iter().copied())
                    .collect(),
            )
        }
        None => None,
    };

    Ok((file_meta, positions, score_map))
}

/// One split, validated and expanded, ready for
/// [`PkVectorIndexedSplitRead::read_prepared`].
///
/// Holds the cloned `DataSplit` as well as the file meta: the read needs the split
/// itself for the split-level deletion-vector factory and the file's location, so
/// preparing only `(file_meta, positions, scores)` would not be enough to defer.
pub(crate) struct PreparedIndexedSplit {
    data_split: DataSplit,
    file_meta: DataFileMeta,
    positions: Vec<i64>,
    score_map: Option<BTreeMap<i64, f32>>,
}

impl PreparedIndexedSplit {
    /// Whether the split carried scores, i.e. whether the read will emit a
    /// `__paimon_search_score` column for it.
    pub(crate) fn has_scores(&self) -> bool {
        self.score_map.is_some()
    }

    /// The one data file this split selects rows from.
    pub(crate) fn file_name(&self) -> &str {
        &self.file_meta.file_name
    }
}

/// Validate a split and expand its ranges WITHOUT touching storage.
///
/// Pure metadata work, so a caller materializing several splits can run it over all of
/// them up front and fail before emitting any row. Storage access -- schema resolution,
/// deletion-vector load, opening the file -- stays in the stream.
pub(crate) fn prepare_indexed_split(
    split: &PkVectorIndexedSplit,
) -> crate::Result<PreparedIndexedSplit> {
    let (file_meta, positions, score_map) = validate_and_expand(split)?;
    Ok(PreparedIndexedSplit {
        data_split: split.split.clone(),
        file_meta,
        positions,
        score_map,
    })
}

/// Validates a `PkVectorIndexedSplit` and delegates its materialization to the
/// sibling `PkVectorPositionRead`. Rust equivalent of Java `PrimaryKeyIndexedSplitRead`.
pub(crate) struct PkVectorIndexedSplitRead {
    reader: DataFileReader,
}

impl PkVectorIndexedSplitRead {
    pub(crate) fn new(reader: DataFileReader) -> Self {
        Self { reader }
    }

    /// Validate the split eagerly, then return a lazy stream that derives
    /// `data_fields`/deletion-vector and delegates to `PkVectorPositionRead`.
    /// Eager errors (single-file, range bounds/ordering, score length) surface
    /// here; lazy errors (schema/DV load, the position reader's own guards) surface
    /// on first poll. A caller with several splits should
    /// [`prepare_indexed_split`] them all first and use
    /// [`read_prepared`](Self::read_prepared).
    pub(crate) fn read(
        &self,
        split: &PkVectorIndexedSplit,
    ) -> crate::Result<ArrowRecordBatchStream> {
        self.read_prepared(prepare_indexed_split(split)?)
    }

    /// As [`Self::read`], but for a split a caller already validated and expanded.
    ///
    /// A caller reading SEVERAL splits into one stream uses this so every split's
    /// metadata is checked before the first row of the first split is emitted --
    /// otherwise a malformed split N is reported only after 1..N-1 have already been
    /// handed out. Everything that touches storage stays lazy.
    pub(crate) fn read_prepared(
        &self,
        prepared: PreparedIndexedSplit,
    ) -> crate::Result<ArrowRecordBatchStream> {
        let PreparedIndexedSplit {
            data_split,
            file_meta,
            positions,
            score_map,
        } = prepared;
        let reader = self.reader.clone();

        let stream = async_stream::try_stream! {
            let dv_factory = reader.build_split_dv_factory(&data_split).await?;
            let dv = DataFileReader::deletion_vector_for_file(
                dv_factory.as_ref(),
                &file_meta.file_name,
            );
            let data_fields = reader.derive_data_fields(&file_meta).await?;

            let inner = PkVectorPositionRead::new(&reader).read(
                &data_split,
                file_meta,
                data_fields,
                dv,
                positions,
                score_map,
            )?;
            futures::pin_mut!(inner);
            while let Some(batch) = inner.next().await {
                yield batch?;
            }
        };
        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::DataFileMeta;
    use crate::table::source::DataSplitBuilder;

    fn data_file(row_count: i64) -> DataFileMeta {
        DataFileMeta {
            file_name: "part-0.mosaic".to_string(),
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
            column_max_sequence_numbers: None,
        }
    }

    fn split_with_files(files: Vec<DataFileMeta>) -> DataSplit {
        DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(crate::spec::BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("memory:/pkvisr/bucket-0".to_string())
            .with_total_buckets(1)
            .with_data_files(files)
            .build()
            .unwrap()
    }

    #[test]
    fn fan_in_rejects_mixed_snapshots_and_repeated_files() {
        let one = |snapshot: i64, file: &str| {
            let mut split = indexed_split(4, vec![RowRange::new(0, 0)], None);
            split.split = split_named(snapshot, file);
            split
        };

        // Two different files at one snapshot: the ordinary fan-in.
        assert!(validate_fan_in(&[one(1, "a"), one(1, "b")]).is_ok());

        let err = validate_fan_in(&[one(1, "a"), one(2, "b")])
            .map(|_| ())
            .expect_err("a read must not mix snapshots");
        assert!(format!("{err:?}").contains("one snapshot"), "got: {err:?}");

        let err = validate_fan_in(&[one(1, "a"), one(1, "a")])
            .map(|_| ())
            .expect_err("the same file twice would return its rows twice");
        assert!(format!("{err:?}").contains("twice"), "got: {err:?}");
    }

    fn split_named(snapshot: i64, file: &str) -> DataSplit {
        let mut meta = data_file(4);
        meta.file_name = file.to_string();
        DataSplitBuilder::new()
            .with_snapshot(snapshot)
            .with_partition(crate::spec::BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("memory:/pkvisr/bucket-0".to_string())
            .with_total_buckets(1)
            .with_data_files(vec![meta])
            .build()
            .unwrap()
    }

    fn indexed_split(
        row_count: i64,
        row_ranges: Vec<RowRange>,
        scores: Option<Vec<f32>>,
    ) -> PkVectorIndexedSplit {
        PkVectorIndexedSplit {
            split: split_with_files(vec![data_file(row_count)]),
            row_ranges,
            scores,
        }
    }

    #[test]
    fn expand_single_contiguous_range() {
        let positions = expand_ranges(&[RowRange::new(0, 3)], 10).unwrap();
        assert_eq!(positions, vec![0, 1, 2, 3]);
    }

    #[test]
    fn expand_multiple_ascending_ranges() {
        // Non-overlapping with a gap, plus an adjacent (touching) pair.
        let positions = expand_ranges(
            &[
                RowRange::new(0, 1),
                RowRange::new(2, 2),
                RowRange::new(5, 6),
            ],
            10,
        )
        .unwrap();
        assert_eq!(positions, vec![0, 1, 2, 5, 6]);
    }

    #[test]
    fn expand_single_row_range() {
        let positions = expand_ranges(&[RowRange::new(4, 4)], 5).unwrap();
        assert_eq!(positions, vec![4]);
    }

    #[test]
    fn expand_rejects_empty_ranges() {
        let err = expand_ranges(&[], 5).expect_err("empty ranges must error");
        assert!(
            format!("{err:?}").contains("at least one row"),
            "got: {err:?}"
        );
    }

    #[test]
    fn expand_rejects_range_at_or_past_row_count() {
        let err = expand_ranges(&[RowRange::new(3, 5)], 5).expect_err("to >= row_count must error");
        assert!(
            format!("{err:?}").contains("outside [0, 5)"),
            "got: {err:?}"
        );
    }

    #[test]
    fn expand_rejects_negative_from() {
        let err = expand_ranges(&[RowRange::new(-1, 2)], 5).expect_err("negative from must error");
        assert!(format!("{err:?}").contains("outside"), "got: {err:?}");
    }

    #[test]
    fn expand_rejects_overlapping_ranges() {
        // range[1].from()==2 <= range[0].to()==3 -> overlap.
        let err = expand_ranges(&[RowRange::new(0, 3), RowRange::new(2, 4)], 10)
            .expect_err("overlapping ranges must error");
        assert!(
            format!("{err:?}").contains("ascending and non-overlapping"),
            "got: {err:?}"
        );
    }

    #[test]
    fn expand_rejects_descending_ranges() {
        let err = expand_ranges(&[RowRange::new(5, 6), RowRange::new(0, 1)], 10)
            .expect_err("descending ranges must error");
        assert!(
            format!("{err:?}").contains("ascending and non-overlapping"),
            "got: {err:?}"
        );
    }

    #[test]
    fn validate_rejects_multi_file_split() {
        let split = PkVectorIndexedSplit {
            split: split_with_files(vec![data_file(5), data_file(5)]),
            row_ranges: vec![RowRange::new(0, 0)],
            scores: None,
        };
        let err = validate_and_expand(&split).expect_err("multi-file split must error");
        assert!(
            format!("{err:?}").contains("exactly one data file"),
            "got: {err:?}"
        );
    }

    #[test]
    fn validate_builds_score_map_for_non_contiguous_ranges() {
        let split = indexed_split(
            10,
            vec![
                RowRange::new(0, 0),
                RowRange::new(2, 2),
                RowRange::new(5, 5),
            ],
            Some(vec![0.9, 0.5, 0.1]),
        );
        let (_file, positions, score_map) = validate_and_expand(&split).unwrap();
        assert_eq!(positions, vec![0, 2, 5]);
        let score_map = score_map.unwrap();
        assert_eq!(score_map, BTreeMap::from([(0, 0.9f32), (2, 0.5), (5, 0.1)]));
    }

    #[test]
    fn validate_no_scores_yields_none() {
        let split = indexed_split(5, vec![RowRange::new(0, 1)], None);
        let (_file, positions, score_map) = validate_and_expand(&split).unwrap();
        assert_eq!(positions, vec![0, 1]);
        assert!(score_map.is_none());
    }

    #[test]
    fn validate_rejects_score_length_mismatch() {
        // select 2 positions but supply 1 score.
        let split = indexed_split(5, vec![RowRange::new(0, 1)], Some(vec![0.5]));
        let err = validate_and_expand(&split).expect_err("score length mismatch must error");
        assert!(format!("{err:?}").contains("scores length"), "got: {err:?}");
    }
}

#[cfg(test)]
mod e2e_tests {
    use super::*;
    use crate::arrow::build_target_arrow_schema;
    use crate::io::FileIOBuilder;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{DataField, DataFileMeta, DataType, IntType};
    use crate::table::data_file_reader::DataFileReader;
    use crate::table::pk_vector_position_read::{PKEY_VECTOR_POSITION_COLUMN, SEARCH_SCORE_COLUMN};
    use crate::table::schema_manager::SchemaManager;
    use crate::table::source::{DataSplit, DataSplitBuilder, DeletionFile};
    use arrow_array::{Array, Float32Array, Int32Array, Int64Array, RecordBatch};
    use bytes::Bytes;
    use futures::TryStreamExt;
    use paimon_mosaic_core::spec::COMPRESSION_NONE;
    use paimon_mosaic_core::writer::{MosaicWriter, OutputFile, WriterOptions};
    use roaring::RoaringBitmap;
    use std::io;
    use std::sync::Arc;

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
            column_max_sequence_numbers: None,
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

    fn write_mosaic_multi_group(batches: &[RecordBatch]) -> Bytes {
        let out = MemOutputFile::new();
        let mut writer = MosaicWriter::new(
            out,
            batches[0].schema().as_ref(),
            WriterOptions {
                compression: COMPRESSION_NONE,
                num_buckets: 2,
                row_group_max_size: 1,
                ..Default::default()
            },
        )
        .unwrap();
        for batch in batches {
            writer.write_batch(batch).unwrap();
        }
        writer.close().unwrap();
        Bytes::from(writer.output().data.to_vec())
    }

    async fn write_deletion_file(
        file_io: &crate::io::FileIO,
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

    /// Build a predicate-free `DataFileReader` over an in-memory mosaic file plus
    /// the matching split. `deleted_rows`, when non-empty, writes a DV into the split.
    async fn build_reader_and_split(
        table_path: &str,
        data: &Bytes,
        row_count: i64,
        deleted_rows: &[u32],
    ) -> (DataFileReader, DataSplit) {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let bucket_path = format!("{table_path}/bucket-0");
        let file_name = "part-0.mosaic";
        file_io
            .new_output(&format!("{bucket_path}/{file_name}"))
            .unwrap()
            .write(data.clone())
            .await
            .unwrap();

        let mut split_builder = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(crate::spec::BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(bucket_path)
            .with_total_buckets(1)
            .with_data_files(vec![data_file(file_name, data.len() as i64, row_count)]);
        if !deleted_rows.is_empty() {
            let df =
                write_deletion_file(&file_io, &format!("{table_path}/index/dv-0"), deleted_rows)
                    .await;
            split_builder = split_builder.with_data_deletion_files(vec![Some(df)]);
        }
        let split = split_builder.build().unwrap();

        let schema_manager = SchemaManager::new(file_io.clone(), table_path.to_string());
        let reader = DataFileReader::new(
            file_io,
            schema_manager,
            1,
            id_fields(),
            id_fields(),
            Vec::new(),
        );
        (reader, split)
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

    #[tokio::test]
    async fn reads_ranges_with_position_column_no_scores() {
        // rows id=[10,11,12], ranges [0..=0, 2..=2] -> ids [10,12], positions [0,2];
        // no _ROW_ID leak, no __paimon_search_score.
        let data = write_mosaic_single_group(&id_batch(vec![10, 11, 12]));
        let (reader, split) = build_reader_and_split("memory:/pkvisr_basic", &data, 3, &[]).await;
        let indexed = PkVectorIndexedSplit {
            split,
            row_ranges: vec![RowRange::new(0, 0), RowRange::new(2, 2)],
            scores: None,
        };

        let batches = PkVectorIndexedSplitRead::new(reader)
            .read(&indexed)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(collect_i32(&batches, "id"), vec![10, 12]);
        assert_eq!(
            collect_i64(&batches, PKEY_VECTOR_POSITION_COLUMN),
            vec![0, 2]
        );
        for batch in &batches {
            assert!(
                column_by_name(batch, "_ROW_ID").is_none(),
                "_ROW_ID must not leak"
            );
            assert!(column_by_name(batch, SEARCH_SCORE_COLUMN).is_none());
        }
    }

    #[tokio::test]
    async fn reads_ranges_with_aligned_scores() {
        let data = write_mosaic_single_group(&id_batch(vec![10, 11, 12, 13]));
        let (reader, split) = build_reader_and_split("memory:/pkvisr_scores", &data, 4, &[]).await;
        let indexed = PkVectorIndexedSplit {
            split,
            row_ranges: vec![RowRange::new(0, 0), RowRange::new(2, 3)],
            scores: Some(vec![0.9, 0.5, 0.1]),
        };

        let batches = PkVectorIndexedSplitRead::new(reader)
            .read(&indexed)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(collect_i32(&batches, "id"), vec![10, 12, 13]);
        assert_eq!(
            collect_i64(&batches, PKEY_VECTOR_POSITION_COLUMN),
            vec![0, 2, 3]
        );
        assert_eq!(
            collect_f32(&batches, SEARCH_SCORE_COLUMN),
            vec![0.9, 0.5, 0.1]
        );
    }

    #[tokio::test]
    async fn deletion_vector_drops_selected_position_and_its_score() {
        // select positions [0,1,2,3] via ranges, scores aligned; DV deletes position 1.
        // -> returned positions [0,2,3], scores [.4,.2,.1]; input scores still had key 1.
        let data = write_mosaic_single_group(&id_batch(vec![10, 11, 12, 13]));
        let (reader, split) = build_reader_and_split("memory:/pkvisr_dv", &data, 4, &[1]).await;
        let indexed = PkVectorIndexedSplit {
            split,
            row_ranges: vec![RowRange::new(0, 3)],
            scores: Some(vec![0.4, 0.3, 0.2, 0.1]),
        };

        let batches = PkVectorIndexedSplitRead::new(reader)
            .read(&indexed)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(collect_i32(&batches, "id"), vec![10, 12, 13]);
        assert_eq!(
            collect_i64(&batches, PKEY_VECTOR_POSITION_COLUMN),
            vec![0, 2, 3]
        );
        assert_eq!(
            collect_f32(&batches, SEARCH_SCORE_COLUMN),
            vec![0.4, 0.2, 0.1]
        );
    }

    #[tokio::test]
    async fn alignment_holds_across_multiple_batches() {
        // Three row groups [10,11] [12,13] [14,15] -> reader yields >1 batch.
        // ranges [1..=2, 4..=4] span batch boundaries.
        let data = write_mosaic_multi_group(&[
            id_batch(vec![10, 11]),
            id_batch(vec![12, 13]),
            id_batch(vec![14, 15]),
        ]);
        let (reader, split) =
            build_reader_and_split("memory:/pkvisr_multibatch", &data, 6, &[]).await;
        let indexed = PkVectorIndexedSplit {
            split,
            row_ranges: vec![RowRange::new(1, 2), RowRange::new(4, 4)],
            scores: Some(vec![0.9, 0.5, 0.1]),
        };

        let batches = PkVectorIndexedSplitRead::new(reader)
            .read(&indexed)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert!(
            batches.len() > 1,
            "expected multiple batches, got {}",
            batches.len()
        );
        assert_eq!(collect_i32(&batches, "id"), vec![11, 12, 14]);
        assert_eq!(
            collect_i64(&batches, PKEY_VECTOR_POSITION_COLUMN),
            vec![1, 2, 4]
        );
        assert_eq!(
            collect_f32(&batches, SEARCH_SCORE_COLUMN),
            vec![0.9, 0.5, 0.1]
        );
    }

    #[tokio::test]
    async fn multi_file_split_errors_at_call_site() {
        // Eager validation: multi-file split errors from read(...) itself, not the stream.
        let data = write_mosaic_single_group(&id_batch(vec![10, 11]));
        let (reader, _split) =
            build_reader_and_split("memory:/pkvisr_multifile", &data, 2, &[]).await;
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(crate::spec::BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("memory:/pkvisr_multifile/bucket-0".to_string())
            .with_total_buckets(1)
            .with_data_files(vec![
                data_file("part-0.mosaic", data.len() as i64, 2),
                data_file("part-1.mosaic", data.len() as i64, 2),
            ])
            .build()
            .unwrap();
        let indexed = PkVectorIndexedSplit {
            split,
            row_ranges: vec![RowRange::new(0, 0)],
            scores: None,
        };

        let err = PkVectorIndexedSplitRead::new(reader)
            .read(&indexed)
            .err()
            .expect("multi-file split must error eagerly");
        assert!(
            format!("{err:?}").contains("exactly one data file"),
            "got: {err:?}"
        );
    }
}
