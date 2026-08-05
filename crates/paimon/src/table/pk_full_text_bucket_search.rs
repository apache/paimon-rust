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

//! Bucket-local primary-key full-text search: the algorithmic core that turns a
//! planned bucket split into scored candidates.
//!
//! Mirror of Java `PrimaryKeyFullTextBucketSearch.searchRankings`. For every
//! current payload in the split it lays the payload's ordered source files out as
//! a cumulative archive: file `i` owns archive rows `[offset_i, offset_i +
//! row_count_i)`, where `offset_i` is the running sum of prior source row counts.
//! A source is *active* iff its data file is present among the split's data files.
//! When any source is inactive or any active source carries deletions, an include
//! allow-list of live archive rows is built (active ranges minus deletion-vector
//! positions, each deletion mapped to `offset + local_position`); otherwise the
//! whole archive is read. Each returned archive row is mapped back to a physical
//! `(data file, row position)` and emitted as a score-tagged candidate.

use std::collections::HashMap;

use roaring::RoaringTreemap;

use crate::deletion_vector::DeletionVector;
use crate::ftindex::reader::FullTextArchiveReader;
use crate::io::FileIO;
use crate::spec::PrimaryKeyIndexSourceMeta;
use crate::table::pk_full_text_read::PrimaryKeyFullTextCandidate;
use crate::table::pk_full_text_scan::PrimaryKeyFullTextSearchSplit;

fn data_invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

/// One source data file laid out at its cumulative archive `offset`. `active` is
/// true iff the file is present among the split's data files; only active sources
/// contribute live rows and may own a returned archive row. Mirrors the Java
/// inner `SourceRange`.
struct SourceRange {
    file_name: String,
    offset: i64,
    row_count: i64,
    active: bool,
}

impl SourceRange {
    fn contains(&self, row_id: i64) -> bool {
        row_id >= self.offset && row_id < self.offset + self.row_count
    }
}

/// A payload's archive layout after validation: its ordered source ranges, the
/// total archive row count, and the optional live-row include allow-list
/// (`None` = read the whole archive; mirrors Java's `needsInclude ? … : null`).
struct PreparedPayload {
    source_ranges: Vec<SourceRange>,
    total_row_count: i64,
    include: Option<RoaringTreemap>,
}

/// Lay a payload's source files out as a cumulative archive and decide the
/// include allow-list, validating each active source's row count against its
/// data file. Mirrors the per-payload body of Java `searchRankings`.
///
/// `active_row_counts` maps an active data file name to its `DataFileMeta`
/// row count; a source absent from the map is inactive (its data file is not in
/// the split). `dvs` is keyed by data file name.
fn prepare_payload(
    source_meta: &PrimaryKeyIndexSourceMeta,
    active_row_counts: &HashMap<String, i64>,
    dvs: &HashMap<String, DeletionVector>,
    payload_file_name: &str,
) -> crate::Result<PreparedPayload> {
    let mut source_ranges: Vec<SourceRange> = Vec::new();
    let mut needs_include = false;
    let mut total_row_count: i64 = 0;
    for source in source_meta.source_files() {
        let active_row_count = active_row_counts.get(source.file_name());
        if let Some(&data_row_count) = active_row_count {
            if source.row_count() != data_row_count {
                return Err(data_invalid(format!(
                    "full-text payload {payload_file_name} source row count does not match data file {}",
                    source.file_name()
                )));
            }
        }
        let has_deletions = dvs.get(source.file_name()).is_some_and(|dv| !dv.is_empty());
        let active = active_row_count.is_some();
        needs_include |= !active || has_deletions;
        source_ranges.push(SourceRange {
            file_name: source.file_name().to_string(),
            offset: total_row_count,
            row_count: source.row_count(),
            active,
        });
        total_row_count = total_row_count
            .checked_add(source.row_count())
            .ok_or_else(|| data_invalid("full-text source row counts overflow i64"))?;
    }

    let include = if needs_include {
        Some(live_rows(&source_ranges, dvs)?)
    } else {
        None
    };
    Ok(PreparedPayload {
        source_ranges,
        total_row_count,
        include,
    })
}

/// Union of active source archive ranges `[offset, offset + row_count)` minus the
/// deletion-vector positions mapped to archive positions (`offset + local`).
/// Mirrors Java `liveRows`, including the fail-loud guard on out-of-range
/// deletion positions.
fn live_rows(
    source_ranges: &[SourceRange],
    dvs: &HashMap<String, DeletionVector>,
) -> crate::Result<RoaringTreemap> {
    let mut include = RoaringTreemap::new();
    let mut deleted = RoaringTreemap::new();
    for source in source_ranges {
        if !source.active {
            continue;
        }
        if source.row_count > 0 {
            let start = source.offset as u64;
            let end = (source.offset + source.row_count) as u64;
            include.insert_range(start..end);
        }
        if let Some(dv) = dvs.get(&source.file_name) {
            if !dv.is_empty() {
                let row_count = source.row_count as u64;
                for position in dv.iter() {
                    if position >= row_count {
                        return Err(data_invalid(format!(
                            "deletion vector contains invalid row position {position} for source {}",
                            source.file_name
                        )));
                    }
                    deleted.insert(source.offset as u64 + position);
                }
            }
        }
    }
    include -= &deleted;
    Ok(include)
}

/// Resolve the active source owning `row_id`, failing loud if the id is outside
/// `[0, total_row_count)` or lands in an inactive source (mirrors Java's two
/// `checkArgument`s around `request.source(rowId)`).
fn owning_active_source(
    source_ranges: &[SourceRange],
    total_row_count: i64,
    row_id: i64,
) -> crate::Result<&SourceRange> {
    if row_id < 0 || row_id >= total_row_count {
        return Err(data_invalid(format!(
            "full-text index returned archive row position {row_id} outside row count {total_row_count}"
        )));
    }
    match source_ranges.iter().find(|source| source.contains(row_id)) {
        Some(source) if source.active => Ok(source),
        _ => Err(data_invalid(format!(
            "full-text index returned row position {row_id} from an inactive source"
        ))),
    }
}

/// Search one bucket's full-text payloads and return its scored candidates
/// (unsorted; the read path fuses them cross-bucket via `top_k_by_score`).
///
/// `dvs` is keyed by data file name; `table_path` roots the index directory
/// (`{table_path}/index/{payload}`). Mirrors Java
/// `PrimaryKeyFullTextBucketSearch.searchRankings`, flattened to one candidate
/// list per bucket.
pub(crate) async fn search_bucket(
    split: &PrimaryKeyFullTextSearchSplit,
    query: &str,
    limit: usize,
    dvs: &HashMap<String, DeletionVector>,
    file_io: &FileIO,
    table_path: &str,
    split_index: usize,
) -> crate::Result<Vec<PrimaryKeyFullTextCandidate>> {
    if limit == 0 {
        return Err(data_invalid("full-text search limit must be positive"));
    }

    let data_split = &split.data_split;
    let partition = data_split.partition().clone();
    let bucket = data_split.bucket();

    // Active data files by name, with a duplicate guard (mirror Java's `files`
    // map). The split constructor already rejects duplicates, but re-checking
    // keeps the invariant local to the search.
    let mut active_row_counts: HashMap<String, i64> = HashMap::new();
    for file in data_split.data_files() {
        if active_row_counts
            .insert(file.file_name.clone(), file.row_count)
            .is_some()
        {
            return Err(data_invalid(format!(
                "duplicate full-text source file {}",
                file.file_name
            )));
        }
    }

    let mut candidates: Vec<PrimaryKeyFullTextCandidate> = Vec::new();
    for payload in &split.current_payloads {
        let gim = payload.global_index_meta.as_ref().ok_or_else(|| {
            data_invalid(format!(
                "full-text payload {} has no global index meta",
                payload.file_name
            ))
        })?;
        let source_meta = PrimaryKeyIndexSourceMeta::from_global_index_meta(gim)?;
        let prepared = prepare_payload(&source_meta, &active_row_counts, dvs, &payload.file_name)?;

        // An empty include means every row is deleted or inactive: nothing to
        // search in this payload (mirror Java's `include.isEmpty()` skip).
        if let Some(include) = &prepared.include {
            if include.is_empty() {
                continue;
            }
        }

        let path = format!("{table_path}/index/{}", payload.file_name);
        let input = file_io.new_input(&path)?;
        let reader = FullTextArchiveReader::from_input_file(&input).await?;
        let hits = match &prepared.include {
            Some(include) => reader.search_with_include(query, limit, include)?,
            None => reader.search(query, limit)?,
        };

        for (&row_id, &score) in hits.row_ids.iter().zip(hits.scores.iter()) {
            if row_id < 0 || row_id >= prepared.total_row_count {
                return Err(data_invalid(format!(
                    "full-text index returned archive row position {row_id} outside row count {}",
                    prepared.total_row_count
                )));
            }
            // Defensive re-check of the engine's include filtering (mirror Java).
            if let Some(include) = &prepared.include {
                if !include.contains(row_id as u64) {
                    continue;
                }
            }
            let source =
                owning_active_source(&prepared.source_ranges, prepared.total_row_count, row_id)?;
            candidates.push(PrimaryKeyFullTextCandidate::new(
                split_index,
                partition.clone(),
                bucket,
                score,
                source.file_name.clone(),
                row_id - source.offset,
            )?);
        }
    }

    Ok(candidates)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{
        BinaryRow, DataFileMeta, GlobalIndexMeta, IndexFileMeta, PrimaryKeyIndexSourceFile,
    };
    use crate::table::source::{DataSplit, DataSplitBuilder};
    use bytes::Bytes;
    use paimon_ftindex_core::io::PosWriter;
    use paimon_ftindex_core::{FullTextIndexConfig, FullTextIndexWriter};
    use roaring::RoaringBitmap;

    const PK_FULL_TEXT_INDEX_TYPE: &str = "full-text";
    const FILE_SOURCE_COMPACT: i32 = 1;

    fn source_range(file_name: &str, offset: i64, row_count: i64, active: bool) -> SourceRange {
        SourceRange {
            file_name: file_name.to_string(),
            offset,
            row_count,
            active,
        }
    }

    // ---- helpers to build synthetic archives + source-meta payloads ----

    /// Build a tiny full-text archive in memory via FT-PR1's core writer.
    fn build_archive(docs: &[(i64, &str)]) -> Vec<u8> {
        let mut writer = FullTextIndexWriter::new(FullTextIndexConfig::new()).unwrap();
        for (row_id, text) in docs {
            writer.add_document(*row_id, (*text).to_string()).unwrap();
        }
        let mut out = PosWriter::new(Vec::<u8>::new());
        writer.write(&mut out).unwrap();
        out.into_inner()
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

    fn gim(field_id: i32, source_meta: Vec<u8>) -> GlobalIndexMeta {
        GlobalIndexMeta {
            row_range_start: 0,
            row_range_end: 0,
            index_field_id: field_id,
            extra_field_ids: None,
            index_meta: None,
            source_meta: Some(source_meta),
            build_schema_id: None,
        }
    }

    fn ft_payload(file_name: &str, level: i32, files: &[(&str, i64)]) -> IndexFileMeta {
        let total: i64 = files.iter().map(|(_, r)| *r).sum();
        IndexFileMeta {
            index_type: PK_FULL_TEXT_INDEX_TYPE.into(),
            file_name: file_name.into(),
            file_size: 1,
            row_count: total as i32,
            deletion_vectors_ranges: None,
            global_index_meta: Some(gim(7, frame(level, files))),
        }
    }

    fn dfm(name: &str, rows: i64) -> DataFileMeta {
        DataFileMeta {
            file_name: name.into(),
            file_size: 1,
            row_count: rows,
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

    fn data_split(files: Vec<DataFileMeta>) -> DataSplit {
        DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path("memory:/t/bucket-0".to_string())
            .with_total_buckets(1)
            .with_data_files(files)
            .build()
            .unwrap()
    }

    async fn write_archive(file_io: &FileIO, path: &str, bytes: Vec<u8>) {
        let output = file_io.new_output(path).unwrap();
        output.write(Bytes::from(bytes)).await.unwrap();
    }

    fn source_meta(files: &[(&str, i64)]) -> PrimaryKeyIndexSourceMeta {
        let sources = files
            .iter()
            .map(|(n, r)| PrimaryKeyIndexSourceFile::new((*n).to_string(), *r).unwrap())
            .collect();
        PrimaryKeyIndexSourceMeta::new(1, sources).unwrap()
    }

    // ---- (a) include=None: all-active, no DV, all matches mapped ----
    #[tokio::test]
    async fn include_none_maps_all_matches_to_positions() {
        // d0 owns archive rows 0..3, d1 owns 3..7; "alpha" at 0,2,4,6.
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/ftbs_none";
        let bytes = build_archive(&[
            (0, "alpha"),
            (1, "beta"),
            (2, "alpha"),
            (3, "beta"),
            (4, "alpha"),
            (5, "beta"),
            (6, "alpha"),
        ]);
        write_archive(&file_io, &format!("{table_path}/index/ft-0"), bytes).await;

        let split = PrimaryKeyFullTextSearchSplit::new(
            data_split(vec![dfm("d0", 3), dfm("d1", 4)]),
            vec![ft_payload("ft-0", 1, &[("d0", 3), ("d1", 4)])],
            Vec::new(),
        )
        .unwrap();

        let dvs: HashMap<String, DeletionVector> = HashMap::new();
        let out = search_bucket(
            &split,
            r#"{"match":{"query":"alpha"}}"#,
            10,
            &dvs,
            &file_io,
            table_path,
            0,
        )
        .await
        .unwrap();

        let mut got: Vec<(String, i64)> = out
            .iter()
            .map(|c| (c.data_file_name.clone(), c.row_position))
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![
                ("d0".to_string(), 0),
                ("d0".to_string(), 2),
                ("d1".to_string(), 1),
                ("d1".to_string(), 3),
            ]
        );
    }

    // ---- (b) a DV-deleted archive position is excluded from results ----
    #[tokio::test]
    async fn deletion_vector_excludes_matched_position() {
        // "beta" at archive rows 1,3,5; DV deletes d0 local pos 1 (archive 1).
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/ftbs_dv";
        let bytes = build_archive(&[
            (0, "alpha"),
            (1, "beta"),
            (2, "alpha"),
            (3, "beta"),
            (4, "alpha"),
            (5, "beta"),
            (6, "alpha"),
        ]);
        write_archive(&file_io, &format!("{table_path}/index/ft-0"), bytes).await;

        let split = PrimaryKeyFullTextSearchSplit::new(
            data_split(vec![dfm("d0", 3), dfm("d1", 4)]),
            vec![ft_payload("ft-0", 1, &[("d0", 3), ("d1", 4)])],
            Vec::new(),
        )
        .unwrap();

        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(1); // d0 local position 1
        let mut dvs: HashMap<String, DeletionVector> = HashMap::new();
        dvs.insert("d0".to_string(), DeletionVector::from_bitmap(bitmap));

        let out = search_bucket(
            &split,
            r#"{"match":{"query":"beta"}}"#,
            10,
            &dvs,
            &file_io,
            table_path,
            0,
        )
        .await
        .unwrap();

        let mut got: Vec<(String, i64)> = out
            .iter()
            .map(|c| (c.data_file_name.clone(), c.row_position))
            .collect();
        got.sort();
        // Archive row 1 (d0 pos 1) is deleted; only d1 rows 3,5 survive.
        assert_eq!(got, vec![("d1".to_string(), 0), ("d1".to_string(), 2)]);
    }

    // ---- (c) a row_id landing in an inactive source → Err ----
    #[test]
    fn inactive_source_row_id_errors() {
        // d0 active 0..3, d1 inactive 3..7.
        let ranges = vec![
            source_range("d0", 0, 3, true),
            source_range("d1", 3, 4, false),
        ];
        assert!(owning_active_source(&ranges, 7, 1).is_ok());
        assert!(owning_active_source(&ranges, 7, 4).is_err());
    }

    // ---- (e) a row_id out of the archive's total range → Err ----
    #[test]
    fn out_of_range_row_id_errors() {
        let ranges = vec![source_range("d0", 0, 3, true)];
        assert!(owning_active_source(&ranges, 3, 2).is_ok());
        assert!(owning_active_source(&ranges, 3, 3).is_err()); // == total
        assert!(owning_active_source(&ranges, 3, -1).is_err());
    }

    // ---- (d) source row_count != active DataFileMeta.row_count → Err ----
    #[test]
    fn source_row_count_mismatch_errors() {
        let meta = source_meta(&[("d0", 3), ("d1", 4)]);
        let dvs: HashMap<String, DeletionVector> = HashMap::new();
        // Active d1 has 5 rows on disk but the payload recorded 4 → mismatch.
        let mut active: HashMap<String, i64> = HashMap::new();
        active.insert("d0".to_string(), 3);
        active.insert("d1".to_string(), 5);
        assert!(prepare_payload(&meta, &active, &dvs, "ft-0").is_err());

        // Matching row counts prepare cleanly.
        let mut ok_active: HashMap<String, i64> = HashMap::new();
        ok_active.insert("d0".to_string(), 3);
        ok_active.insert("d1".to_string(), 4);
        assert!(prepare_payload(&meta, &ok_active, &dvs, "ft-0").is_ok());
    }

    // ---- include decision: None when clean, Some when a source is inactive ----
    #[test]
    fn include_is_none_only_when_clean() {
        let meta = source_meta(&[("d0", 3), ("d1", 4)]);
        let dvs: HashMap<String, DeletionVector> = HashMap::new();

        // Both active, no DV → include None (read everything).
        let mut all_active: HashMap<String, i64> = HashMap::new();
        all_active.insert("d0".to_string(), 3);
        all_active.insert("d1".to_string(), 4);
        let clean = prepare_payload(&meta, &all_active, &dvs, "ft-0").unwrap();
        assert!(clean.include.is_none());
        assert_eq!(clean.total_row_count, 7);

        // d1 inactive → include Some, containing only d0's live rows 0..3.
        let mut only_d0: HashMap<String, i64> = HashMap::new();
        only_d0.insert("d0".to_string(), 3);
        let partial = prepare_payload(&meta, &only_d0, &dvs, "ft-0").unwrap();
        let include = partial.include.expect("inactive source forces include");
        assert_eq!(include.iter().collect::<Vec<u64>>(), vec![0, 1, 2]);
    }

    // ---- live_rows subtracts DV positions mapped by offset ----
    #[test]
    fn live_rows_subtracts_mapped_deletions() {
        let ranges = vec![
            source_range("d0", 0, 3, true),
            source_range("d1", 3, 4, true),
        ];
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(2); // d1 local pos 2 → archive 5
        let mut dvs: HashMap<String, DeletionVector> = HashMap::new();
        dvs.insert("d1".to_string(), DeletionVector::from_bitmap(bitmap));

        let live = live_rows(&ranges, &dvs).unwrap();
        assert_eq!(live.iter().collect::<Vec<u64>>(), vec![0, 1, 2, 3, 4, 6]);
    }

    // ---- live_rows fails loud on an out-of-range deletion position ----
    #[test]
    fn live_rows_rejects_out_of_range_deletion() {
        let ranges = vec![source_range("d0", 0, 3, true)];
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(3); // >= row_count 3
        let mut dvs: HashMap<String, DeletionVector> = HashMap::new();
        dvs.insert("d0".to_string(), DeletionVector::from_bitmap(bitmap));
        assert!(live_rows(&ranges, &dvs).is_err());
    }

    // ---- limit must be positive ----
    #[tokio::test]
    async fn zero_limit_errors() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let split = PrimaryKeyFullTextSearchSplit::new(
            data_split(vec![dfm("d0", 3)]),
            vec![ft_payload("ft-0", 1, &[("d0", 3)])],
            Vec::new(),
        )
        .unwrap();
        let dvs: HashMap<String, DeletionVector> = HashMap::new();
        assert!(search_bucket(
            &split,
            r#"{"match":{"query":"x"}}"#,
            0,
            &dvs,
            &file_io,
            "memory:/x",
            0
        )
        .await
        .is_err());
    }
}
