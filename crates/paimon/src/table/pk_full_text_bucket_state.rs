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

//! Per-bucket current/stale reconciliation for primary-key full-text index
//! payloads, aligned to active data files by level.
//!
//! Mirrors Java `PkFullTextBucketIndexState.fromActiveDataFiles`: a payload is
//! *current* only when it exactly covers the live source files at its level and
//! passes its own per-payload validation. Payloads that no longer match the live
//! data files, fail to parse, or fail their singleton validation (bad row count,
//! bad row range, or an overflowing source row total) are collected as *stale*
//! (not an error); payloads that are not full-text or carry no global index meta
//! are ignored. Only cross-payload conflicts across the surviving current set
//! (a duplicate payload file name, or a source data file covered by two current
//! payloads) fail loud.

use std::collections::{BTreeMap, HashSet};

use indexmap::IndexMap;

use crate::spec::{
    should_read_pk_index_source, DataFileMeta, GlobalIndexMeta, IndexFileMeta,
    PrimaryKeyIndexSourceFile, PrimaryKeyIndexSourceMeta,
};

pub(crate) const PK_FULL_TEXT_INDEX_TYPE: &str = "full-text";

fn data_invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

/// Per-payload singleton validation, mirroring Java's public
/// `PkFullTextBucketIndexState` payload constructor guarded by a `try/catch`
/// that routes any `RuntimeException` to stale: the payload's declared row count
/// must equal the sum of its source rows, and its row range must span exactly
/// `[0, total - 1]`. The sum is computed with `checked_add` so an overflowing
/// total fails the check (Java `Math.addExact` throws → caught → stale) instead
/// of panicking. Returns `false` on any defect so the caller routes the payload
/// to stale.
fn payload_matches_source(
    payload: &IndexFileMeta,
    global_meta: &GlobalIndexMeta,
    source_meta: &PrimaryKeyIndexSourceMeta,
) -> bool {
    let mut total: i64 = 0;
    for source in source_meta.source_files() {
        match total.checked_add(source.row_count()) {
            Some(next) => total = next,
            None => return false, // overflow → stale
        }
    }
    i64::from(payload.row_count) == total
        && global_meta.row_range_start == 0
        && global_meta.row_range_end == total - 1
}

/// Reconciled full-text payloads for a single bucket.
// The read path consumes this once it lands; until then the state has no
// production caller.
#[allow(dead_code)]
pub(crate) struct PkFullTextBucketState {
    // Consumed by the read path once it lands, kept to identify the indexed field.
    text_field_id: i32,
    current_payloads: Vec<IndexFileMeta>,
    stale_payloads: Vec<IndexFileMeta>,
    // Maps each covered source data file to its current payload. Insertion order
    // follows the current-payload order and, within a payload, its source-file
    // order, so the read path can build deterministically ordered splits from it.
    payload_by_source_file: IndexMap<String, IndexFileMeta>,
}

#[allow(dead_code)]
impl PkFullTextBucketState {
    pub(crate) fn from_active_data_files(
        text_field_id: i32,
        active_data_files: &[DataFileMeta],
        active_payloads: Vec<IndexFileMeta>,
    ) -> crate::Result<Self> {
        // Desired live source set per level (sorted by file name).
        let mut sources_by_level: BTreeMap<i32, Vec<PrimaryKeyIndexSourceFile>> = BTreeMap::new();
        for file in active_data_files {
            if should_read_pk_index_source(file) {
                sources_by_level.entry(file.level).or_default().push(
                    PrimaryKeyIndexSourceFile::new(file.file_name.clone(), file.row_count)?,
                );
            }
        }
        for sources in sources_by_level.values_mut() {
            sources.sort_by(|a, b| a.file_name().cmp(b.file_name()));
        }

        // Classify payloads. A payload lands in `by_level` only when it parses,
        // matches the desired live source set exactly, and passes its own
        // per-payload validation; every other single-payload defect routes it to
        // stale (mirroring Java's `try/catch (RuntimeException → stale)`).
        let mut by_level: BTreeMap<i32, Vec<(IndexFileMeta, PrimaryKeyIndexSourceMeta)>> =
            BTreeMap::new();
        let mut stale: Vec<IndexFileMeta> = Vec::new();
        for payload in active_payloads {
            if payload.index_type != PK_FULL_TEXT_INDEX_TYPE {
                continue; // IGNORED
            }
            let Some(global_meta) = payload.global_index_meta.as_ref() else {
                continue; // IGNORED
            };
            // Field id is checked before the source-meta is even parsed: a payload
            // that indexes a different text field is stale only when it carries
            // source-meta, and is otherwise skipped without ever decoding it.
            if global_meta.index_field_id != text_field_id {
                if global_meta.source_meta.is_some() {
                    stale.push(payload); // STALE (wrong field, has source-meta)
                }
                continue; // wrong field, no source-meta → IGNORED
            }
            // Matching field: parse source-meta. A parse failure or absent
            // source-meta makes the payload stale (mirrors Java decoding it inside
            // the per-candidate try/catch).
            let source_meta = match global_meta
                .source_meta
                .as_ref()
                .map(|b| PrimaryKeyIndexSourceMeta::deserialize(b))
            {
                Some(Ok(m)) => m,
                // No source-meta or a parse failure: cannot be a current payload.
                Some(Err(_)) | None => {
                    stale.push(payload);
                    continue;
                }
            };
            // Desired-set match (exact, order-sensitive).
            let matches_desired = sources_by_level
                .get(&source_meta.data_level())
                .is_some_and(|desired| desired.as_slice() == source_meta.source_files());
            if !matches_desired {
                stale.push(payload); // STALE (missing level or set mismatch)
                continue;
            }
            // Per-payload singleton validation (row total via checked_add, row
            // count, row range). Java runs this inside try/catch, so any failure
            // — including an overflowing row total — routes the payload to stale,
            // never an error.
            if !payload_matches_source(&payload, global_meta, &source_meta) {
                stale.push(payload); // STALE (per-payload defect)
                continue;
            }
            by_level
                .entry(source_meta.data_level())
                .or_default()
                .push((payload, source_meta));
        }

        // Per level: exactly one survivor is current; >1 → all stale.
        let mut current: Vec<(IndexFileMeta, PrimaryKeyIndexSourceMeta)> = Vec::new();
        for (_level, mut payloads) in by_level {
            if payloads.len() == 1 {
                current.push(payloads.remove(0));
            } else {
                stale.extend(payloads.into_iter().map(|(payload, _)| payload));
            }
        }

        // Cross-payload conflicts across the current set are the only fail-loud
        // cases (the surviving payloads already passed per-payload validation):
        // a duplicate payload file name, or a source data file covered twice.
        let mut payload_by_source_file: IndexMap<String, IndexFileMeta> = IndexMap::new();
        let mut seen_payloads: HashSet<String> = HashSet::new();
        let mut current_payloads: Vec<IndexFileMeta> = Vec::with_capacity(current.len());
        for (payload, source_meta) in current {
            if !seen_payloads.insert(payload.file_name.clone()) {
                return Err(data_invalid(format!(
                    "full-text payload {} appears more than once",
                    payload.file_name
                )));
            }
            for source in source_meta.source_files() {
                if payload_by_source_file
                    .insert(source.file_name().to_string(), payload.clone())
                    .is_some()
                {
                    return Err(data_invalid(format!(
                        "source data file {} is covered by more than one full-text payload",
                        source.file_name()
                    )));
                }
            }
            current_payloads.push(payload);
        }

        Ok(Self {
            text_field_id,
            current_payloads,
            stale_payloads: stale,
            payload_by_source_file,
        })
    }

    pub(crate) fn current_payloads(&self) -> &[IndexFileMeta] {
        &self.current_payloads
    }
    pub(crate) fn stale_payloads(&self) -> &[IndexFileMeta] {
        &self.stale_payloads
    }
    pub(crate) fn payload_by_source_file(&self) -> &IndexMap<String, IndexFileMeta> {
        &self.payload_by_source_file
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::GlobalIndexMeta;

    /// COMPACT file source discriminant (matches Java `FileSource.COMPACT`).
    const FILE_SOURCE_COMPACT: i32 = 1;

    /// A COMPACT data file at `level` (>0), so `should_read_pk_index_source`
    /// accepts it as a live source.
    fn dfm(name: &str, rows: i64, level: i32) -> DataFileMeta {
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
            level,
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

    /// One Java `DataOutput#writeUTF` value (u16-BE length + modified UTF-8) for
    /// the ASCII file names used in these tests.
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

    /// A valid `_SOURCE_META` frame (version 1) for the given level and files.
    fn frame(data_level: i32, files: &[(&str, i64)]) -> Vec<u8> {
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

    /// Build a full-text `IndexFileMeta` payload with a `GlobalIndexMeta`.
    fn payload(
        file_name: &str,
        index_type: &str,
        row_count: i32,
        global_index_meta: Option<GlobalIndexMeta>,
    ) -> IndexFileMeta {
        IndexFileMeta {
            index_type: index_type.into(),
            file_name: file_name.into(),
            file_size: 1,
            row_count,
            deletion_vectors_ranges: None,
            global_index_meta,
        }
    }

    /// A `GlobalIndexMeta` with a source-meta blob and matching row range.
    fn gim(field_id: i32, start: i64, end: i64, source_meta: Option<Vec<u8>>) -> GlobalIndexMeta {
        GlobalIndexMeta {
            row_range_start: start,
            row_range_end: end,
            index_field_id: field_id,
            extra_field_ids: None,
            index_meta: None,
            source_meta,
            build_schema_id: None,
        }
    }

    // (a) exact match → current.
    #[test]
    fn exact_match_is_current() {
        let files = [dfm("d0.parquet", 100, 1)];
        let payloads = vec![payload(
            "ft-0",
            PK_FULL_TEXT_INDEX_TYPE,
            100,
            Some(gim(7, 0, 99, Some(frame(1, &[("d0.parquet", 100)])))),
        )];
        let state = PkFullTextBucketState::from_active_data_files(7, &files, payloads).unwrap();
        assert_eq!(state.current_payloads().len(), 1);
        assert_eq!(state.current_payloads()[0].file_name, "ft-0");
        assert!(state.stale_payloads().is_empty());
        assert_eq!(state.payload_by_source_file().len(), 1);
        assert!(state.payload_by_source_file().contains_key("d0.parquet"));
    }

    // (b) source drift (source set no longer matches live files) → stale.
    #[test]
    fn source_drift_is_stale() {
        let files = [dfm("d0.parquet", 100, 1)];
        // Payload references a file that is no longer live at level 1.
        let payloads = vec![payload(
            "ft-0",
            PK_FULL_TEXT_INDEX_TYPE,
            50,
            Some(gim(7, 0, 49, Some(frame(1, &[("stale.parquet", 50)])))),
        )];
        let state = PkFullTextBucketState::from_active_data_files(7, &files, payloads).unwrap();
        assert!(state.current_payloads().is_empty());
        assert_eq!(state.stale_payloads().len(), 1);
        assert_eq!(state.stale_payloads()[0].file_name, "ft-0");
    }

    // (b') reorder-only drift → stale. The desired set is sorted by file name, so
    // a payload whose source-meta lists the same files in a different order fails
    // the order-sensitive equality gate (pins that the match is order-sensitive).
    #[test]
    fn reordered_source_files_are_stale() {
        let files = [dfm("a.parquet", 100, 1), dfm("b.parquet", 200, 1)];
        // Same files and row counts, but listed b-before-a instead of the sorted
        // a-before-b, so the exact List.equals gate rejects it.
        let payloads = vec![payload(
            "ft-0",
            PK_FULL_TEXT_INDEX_TYPE,
            300,
            Some(gim(
                7,
                0,
                299,
                Some(frame(1, &[("b.parquet", 200), ("a.parquet", 100)])),
            )),
        )];
        let state = PkFullTextBucketState::from_active_data_files(7, &files, payloads).unwrap();
        assert!(state.current_payloads().is_empty());
        assert_eq!(state.stale_payloads().len(), 1);
        assert_eq!(state.stale_payloads()[0].file_name, "ft-0");
    }
    #[test]
    fn two_payloads_same_level_are_both_stale() {
        let files = [dfm("d0.parquet", 100, 1)];
        let source = frame(1, &[("d0.parquet", 100)]);
        let payloads = vec![
            payload(
                "ft-a",
                PK_FULL_TEXT_INDEX_TYPE,
                100,
                Some(gim(7, 0, 99, Some(source.clone()))),
            ),
            payload(
                "ft-b",
                PK_FULL_TEXT_INDEX_TYPE,
                100,
                Some(gim(7, 0, 99, Some(source))),
            ),
        ];
        let state = PkFullTextBucketState::from_active_data_files(7, &files, payloads).unwrap();
        assert!(state.current_payloads().is_empty());
        let stale: Vec<&str> = state
            .stale_payloads()
            .iter()
            .map(|p| p.file_name.as_str())
            .collect();
        assert_eq!(stale.len(), 2);
        assert!(stale.contains(&"ft-a") && stale.contains(&"ft-b"));
    }

    // (d) wrong index_type → ignored (in neither list).
    #[test]
    fn wrong_index_type_is_ignored() {
        let files = [dfm("d0.parquet", 100, 1)];
        let payloads = vec![payload(
            "gi-0",
            "GLOBAL_INDEX",
            100,
            Some(gim(7, 0, 99, Some(frame(1, &[("d0.parquet", 100)])))),
        )];
        let state = PkFullTextBucketState::from_active_data_files(7, &files, payloads).unwrap();
        assert!(state.current_payloads().is_empty());
        assert!(state.stale_payloads().is_empty());
    }

    // (e) no global_index_meta → ignored (in neither list).
    #[test]
    fn no_global_index_meta_is_ignored() {
        let files = [dfm("d0.parquet", 100, 1)];
        let payloads = vec![payload("ft-0", PK_FULL_TEXT_INDEX_TYPE, 100, None)];
        let state = PkFullTextBucketState::from_active_data_files(7, &files, payloads).unwrap();
        assert!(state.current_payloads().is_empty());
        assert!(state.stale_payloads().is_empty());
    }

    // (f) field-id mismatch (with source-meta present) → stale.
    #[test]
    fn field_id_mismatch_is_stale() {
        let files = [dfm("d0.parquet", 100, 1)];
        let payloads = vec![payload(
            "ft-0",
            PK_FULL_TEXT_INDEX_TYPE,
            100,
            // index_field_id 9 != text_field_id 7.
            Some(gim(9, 0, 99, Some(frame(1, &[("d0.parquet", 100)])))),
        )];
        let state = PkFullTextBucketState::from_active_data_files(7, &files, payloads).unwrap();
        assert!(state.current_payloads().is_empty());
        assert_eq!(state.stale_payloads().len(), 1);
        assert_eq!(state.stale_payloads()[0].file_name, "ft-0");
    }

    // (f') field-id mismatch with NO source-meta → ignored (skipped, not stale):
    // a payload indexing another field that carries no source metadata is another
    // field's concern, so it is dropped without ever being decoded.
    #[test]
    fn field_id_mismatch_without_source_meta_is_ignored() {
        let files = [dfm("d0.parquet", 100, 1)];
        let payloads = vec![payload(
            "ft-other",
            PK_FULL_TEXT_INDEX_TYPE,
            100,
            // index_field_id 9 != text_field_id 7, and no source-meta blob.
            Some(gim(9, 0, 99, None)),
        )];
        let state = PkFullTextBucketState::from_active_data_files(7, &files, payloads).unwrap();
        assert!(state.current_payloads().is_empty());
        assert!(state.stale_payloads().is_empty());
        assert!(state.payload_by_source_file().is_empty());
    }

    // (g) corrupt source-meta bytes → stale.
    #[test]
    fn corrupt_source_meta_is_stale() {
        let files = [dfm("d0.parquet", 100, 1)];
        let payloads = vec![payload(
            "ft-0",
            PK_FULL_TEXT_INDEX_TYPE,
            100,
            Some(gim(7, 0, 99, Some(vec![0xFF, 0x00, 0x01]))),
        )];
        let state = PkFullTextBucketState::from_active_data_files(7, &files, payloads).unwrap();
        assert!(state.current_payloads().is_empty());
        assert_eq!(state.stale_payloads().len(), 1);
        assert_eq!(state.stale_payloads()[0].file_name, "ft-0");
    }

    // (h) desired level missing from live files → stale.
    #[test]
    fn missing_desired_level_is_stale() {
        // Live files are all at level 1; payload's source-meta claims level 2.
        let files = [dfm("d0.parquet", 100, 1)];
        let payloads = vec![payload(
            "ft-0",
            PK_FULL_TEXT_INDEX_TYPE,
            100,
            Some(gim(7, 0, 99, Some(frame(2, &[("d0.parquet", 100)])))),
        )];
        let state = PkFullTextBucketState::from_active_data_files(7, &files, payloads).unwrap();
        assert!(state.current_payloads().is_empty());
        assert_eq!(state.stale_payloads().len(), 1);
        assert_eq!(state.stale_payloads()[0].file_name, "ft-0");
    }

    // (l) duplicate payload file_name across levels in the current set → Err.
    #[test]
    fn duplicate_payload_file_name_is_error() {
        // Same file name lives at two levels, each with a matching current payload
        // that happens to share the same index file name.
        let files = [dfm("d0.parquet", 100, 1), dfm("d1.parquet", 200, 2)];
        let payloads = vec![
            payload(
                "ft-dup",
                PK_FULL_TEXT_INDEX_TYPE,
                100,
                Some(gim(7, 0, 99, Some(frame(1, &[("d0.parquet", 100)])))),
            ),
            payload(
                "ft-dup",
                PK_FULL_TEXT_INDEX_TYPE,
                200,
                Some(gim(7, 0, 199, Some(frame(2, &[("d1.parquet", 200)])))),
            ),
        ];
        assert!(PkFullTextBucketState::from_active_data_files(7, &files, payloads).is_err());
    }

    // (i) row_count mismatch (per-payload defect) → stale, NOT error.
    #[test]
    fn row_count_mismatch_is_stale() {
        let files = [dfm("d0.parquet", 100, 1)];
        // Source rows total 100 but payload declares 99.
        let payloads = vec![payload(
            "ft-0",
            PK_FULL_TEXT_INDEX_TYPE,
            99,
            Some(gim(7, 0, 98, Some(frame(1, &[("d0.parquet", 100)])))),
        )];
        let state = PkFullTextBucketState::from_active_data_files(7, &files, payloads).unwrap();
        assert!(state.current_payloads().is_empty());
        assert_eq!(state.stale_payloads().len(), 1);
        assert_eq!(state.stale_payloads()[0].file_name, "ft-0");
    }

    // (j) row_range mismatch alone (row_count correct, per-payload defect) →
    // stale, NOT error.
    #[test]
    fn row_range_mismatch_is_stale() {
        let files = [dfm("d0.parquet", 100, 1)];
        // row_count matches total (100) but row range end is wrong (should be 99).
        let payloads = vec![payload(
            "ft-0",
            PK_FULL_TEXT_INDEX_TYPE,
            100,
            Some(gim(7, 0, 50, Some(frame(1, &[("d0.parquet", 100)])))),
        )];
        let state = PkFullTextBucketState::from_active_data_files(7, &files, payloads).unwrap();
        assert!(state.current_payloads().is_empty());
        assert_eq!(state.stale_payloads().len(), 1);
        assert_eq!(state.stale_payloads()[0].file_name, "ft-0");
    }

    // (k) source row counts overflow i64 (per-payload defect) → stale, NOT a
    // panic and NOT error.
    #[test]
    fn source_row_count_overflow_is_stale() {
        // Two live level-1 files whose row counts sum past i64::MAX; the payload
        // matches the desired set exactly, so only the overflowing row total can
        // reject it — and it must do so without panicking.
        let files = [dfm("a.parquet", i64::MAX, 1), dfm("b.parquet", i64::MAX, 1)];
        let payloads = vec![payload(
            "ft-0",
            PK_FULL_TEXT_INDEX_TYPE,
            0,
            Some(gim(
                7,
                0,
                0,
                Some(frame(
                    1,
                    &[("a.parquet", i64::MAX), ("b.parquet", i64::MAX)],
                )),
            )),
        )];
        let state = PkFullTextBucketState::from_active_data_files(7, &files, payloads).unwrap();
        assert!(state.current_payloads().is_empty());
        assert_eq!(state.stale_payloads().len(), 1);
        assert_eq!(state.stale_payloads()[0].file_name, "ft-0");
    }

    // (m) a source data file covered by two current payloads → Err.
    #[test]
    fn source_double_covered_is_error() {
        // The same source file name is live at two levels; two current payloads
        // (one per level) each cover it, colliding in payload_by_source_file.
        let files = [dfm("shared.parquet", 100, 1), dfm("shared.parquet", 200, 2)];
        let payloads = vec![
            payload(
                "ft-a",
                PK_FULL_TEXT_INDEX_TYPE,
                100,
                Some(gim(7, 0, 99, Some(frame(1, &[("shared.parquet", 100)])))),
            ),
            payload(
                "ft-b",
                PK_FULL_TEXT_INDEX_TYPE,
                200,
                Some(gim(7, 0, 199, Some(frame(2, &[("shared.parquet", 200)])))),
            ),
        ];
        assert!(PkFullTextBucketState::from_active_data_files(7, &files, payloads).is_err());
    }
}
