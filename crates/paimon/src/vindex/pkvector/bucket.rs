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

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;

use super::ann::PkVectorAnnSearcher;
use super::data_invalid;
use super::exact::exact_search;
use super::metric::{java_float_compare, VectorSearchMetric};
use super::reader::PkVectorReader;
use super::result::PkVectorSearchResult;
use crate::deletion_vector::DeletionVector;
use crate::spec::PkVectorSourceMeta;

/// One ANN segment to be searched by the bucket kernel. `source_meta` resolves
/// segment ordinals back to physical `(data file, position)` and drives live-row
/// masking; the remaining fields address the segment's index file for the ANN
/// scorer that reads it.
pub(crate) struct BucketAnnSegment {
    pub source_meta: PkVectorSourceMeta,
    /// Resolved index-file path (globally unique; the scorer's preload key).
    pub path: String,
    pub file_size: u64,
    pub index_meta: Vec<u8>,
}

#[cfg(test)]
impl BucketAnnSegment {
    /// Build a segment with dummy index-file fields for tests that exercise only
    /// `source_meta`-driven logic.
    pub(crate) fn for_test(source_meta: PkVectorSourceMeta) -> Self {
        Self {
            source_meta,
            path: "seg".to_string(),
            file_size: 0,
            index_meta: Vec::new(),
        }
    }
}

/// A data file participating in the bucket search, with its row count. Used by
/// the bucket kernel to plan exact vs. ANN search over active files.
pub(crate) struct BucketActiveFile {
    pub file_name: String,
    pub row_count: i64,
}

/// Total BEST_FIRST order over results: distance ASC, then data_file_name ASC,
/// then row_position ASC. `java_float_compare` sorts NaN distances last (never
/// best), matching Java `Float.compare`, and is panic-free.
fn best_first(a: &PkVectorSearchResult, b: &PkVectorSearchResult) -> Ordering {
    java_float_compare(a.distance, b.distance)
        .then_with(|| a.data_file_name.cmp(&b.data_file_name))
        .then_with(|| a.row_position.cmp(&b.row_position))
}

/// A candidate wrapped so a max-heap keeps the WORST (BEST_FIRST-largest)
/// candidate on top; popping evicts the least-wanted one. Mirrors the
/// `PriorityQueue<>(limit, BEST_FIRST.reversed())` in Java
/// `PrimaryKeyVectorBucketSearch`.
struct WorstFirst(PkVectorSearchResult);

impl PartialEq for WorstFirst {
    fn eq(&self, other: &Self) -> bool {
        best_first(&self.0, &other.0) == Ordering::Equal
    }
}
impl Eq for WorstFirst {}
impl PartialOrd for WorstFirst {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for WorstFirst {
    fn cmp(&self, other: &Self) -> Ordering {
        best_first(&self.0, &other.0)
    }
}

/// Add `candidate` to a bounded (size `limit`) BEST_FIRST Top-K max-heap: push if
/// under capacity, else replace the current worst iff the candidate beats it.
/// `O(log limit)` per call. Mirrors Java `PrimaryKeyVectorBucketSearch.add`.
fn add_candidate(heap: &mut BinaryHeap<WorstFirst>, candidate: PkVectorSearchResult, limit: usize) {
    if heap.len() < limit {
        heap.push(WorstFirst(candidate));
    } else if heap
        .peek()
        .is_some_and(|worst| best_first(&candidate, &worst.0) == Ordering::Less)
    {
        heap.pop();
        heap.push(WorstFirst(candidate));
    }
}

/// Active data files whose rows are already covered by an ANN segment's source
/// metadata, matched by both file name AND row count. The bucket exact fallback
/// skips these files, so a caller that preloads exact readers should preload
/// only the *uncovered* active files (`active_files` minus this set) rather than
/// reading every active file's vector column up front. A source naming an
/// inactive file, or one whose row count disagrees with the active file, is not
/// covered here; `bucket_search` rejects the row-count mismatch separately.
pub(crate) fn covered_source_files(
    ann_segments: &[BucketAnnSegment],
    active_files: &[BucketActiveFile],
) -> HashSet<String> {
    let row_counts: HashMap<&str, i64> = active_files
        .iter()
        .map(|f| (f.file_name.as_str(), f.row_count))
        .collect();
    let mut covered = HashSet::new();
    for segment in ann_segments {
        for source in segment.source_meta.source_files() {
            if row_counts
                .get(source.file_name())
                .is_some_and(|&rc| rc == source.row_count())
            {
                covered.insert(source.file_name().to_string());
            }
        }
    }
    covered
}

/// ANN + exact data-file fallback search for one snapshot bucket. Mirrors Java
/// `org.apache.paimon.index.pkvector.PrimaryKeyVectorBucketSearch.search`.
///
/// `ann_searcher` may be `None` only when there are no ANN segments; segments
/// present with `None` is an error.
#[allow(clippy::too_many_arguments)]
pub(crate) fn bucket_search(
    ann_searcher: Option<&dyn PkVectorAnnSearcher>,
    ann_segments: &[BucketAnnSegment],
    active_files: &[BucketActiveFile],
    deletion_vectors: &HashMap<String, Arc<DeletionVector>>,
    exact_reader_factory: &mut dyn FnMut(
        &BucketActiveFile,
    ) -> crate::Result<Box<dyn PkVectorReader>>,
    query: &[f32],
    metric: VectorSearchMetric,
    limit: usize,
    search_options: &HashMap<String, String>,
    skip_exact_fallback: bool,
) -> crate::Result<Vec<PkVectorSearchResult>> {
    if limit == 0 {
        return Err(data_invalid("vector search limit must be positive"));
    }

    let mut files_by_name: HashMap<&str, &BucketActiveFile> = HashMap::new();
    for file in active_files {
        if file.row_count < 0 {
            return Err(data_invalid(format!(
                "active data file {} row count must not be negative: {}",
                file.file_name, file.row_count
            )));
        }
        if files_by_name
            .insert(file.file_name.as_str(), file)
            .is_some()
        {
            return Err(data_invalid(format!(
                "duplicate data file: {}",
                file.file_name
            )));
        }
    }

    // Validate ANN segments mirror Java PkVectorBucketIndexState constructor checks:
    // (1) payload file uniqueness, (2) no source file covered by multiple segments.
    let mut segments_by_path: HashMap<&str, usize> = HashMap::new();
    let mut source_to_segment: HashMap<&str, &str> = HashMap::new();
    for (idx, segment) in ann_segments.iter().enumerate() {
        if segments_by_path
            .insert(segment.path.as_str(), idx)
            .is_some()
        {
            return Err(data_invalid(format!(
                "ANN segment payload {} appears more than once",
                segment.path
            )));
        }
        for source in segment.source_meta.source_files() {
            if let Some(&prior_segment_path) = source_to_segment.get(source.file_name()) {
                return Err(data_invalid(format!(
                    "source data file {} is covered by both ANN segments {} and {}",
                    source.file_name(),
                    prior_segment_path,
                    segment.path
                )));
            }
            source_to_segment.insert(source.file_name(), segment.path.as_str());
        }
    }

    let mut heap: BinaryHeap<WorstFirst> = BinaryHeap::with_capacity(limit + 1);
    let active_source_files: HashSet<String> =
        files_by_name.keys().map(|name| name.to_string()).collect();
    // Active files whose rows an ANN segment already covers; the exact fallback
    // skips them. Same rule the caller's exact-reader preload uses, so both agree
    // on which files still need an exact reader.
    let covered = covered_source_files(ann_segments, active_files);

    for segment in ann_segments {
        // An active ANN source with a mismatched row count is corruption (the
        // ordinal-to-position mapping would be wrong). An inactive source (no
        // matching active file) is skipped: it was compacted away and its ordinal
        // range is masked out of the ANN live-row bitmap. Mirrors Java master
        // `PrimaryKeyVectorBucketSearch` (`file == null` -> continue).
        for source in segment.source_meta.source_files() {
            if let Some(active) = files_by_name.get(source.file_name()) {
                if active.row_count != source.row_count() {
                    return Err(data_invalid(format!(
                        "ANN source {} does not match the active data file",
                        source.file_name()
                    )));
                }
            }
        }
        let searcher = ann_searcher.ok_or_else(|| data_invalid("ANN search is not configured"))?;
        for result in searcher.search(
            segment,
            query,
            metric,
            limit,
            &active_source_files,
            deletion_vectors,
            search_options,
        )? {
            add_candidate(&mut heap, result, limit);
        }
    }

    if !skip_exact_fallback {
        for file in active_files {
            if covered.contains(&file.file_name) {
                continue;
            }
            let dv = deletion_vectors.get(&file.file_name).cloned();
            let is_excluded = move |position: i64| -> bool {
                match &dv {
                    Some(dv) => u64::try_from(position)
                        .map(|p| dv.is_deleted(p))
                        .unwrap_or(false),
                    None => false,
                }
            };
            let mut reader = exact_reader_factory(file)?;
            for result in exact_search(
                &file.file_name,
                reader.as_mut(),
                query,
                metric,
                limit,
                &is_excluded,
            )? {
                add_candidate(&mut heap, result, limit);
            }
        }
    }

    let mut results: Vec<PkVectorSearchResult> = heap.into_iter().map(|w| w.0).collect();
    results.sort_by(best_first);
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::PkVectorSourceFile;
    use crate::vindex::pkvector::ann::PkVectorAnnSearcher;
    use crate::vindex::pkvector::reader::test_support::ArrayReader;
    use roaring::RoaringBitmap;
    use std::cell::RefCell;

    fn meta(files: &[(&str, i64)]) -> PkVectorSourceMeta {
        PkVectorSourceMeta::new(
            1,
            files
                .iter()
                .map(|(n, r)| PkVectorSourceFile::new((*n).into(), *r).unwrap())
                .collect(),
        )
        .unwrap()
    }

    fn active(name: &str, rows: i64) -> BucketActiveFile {
        BucketActiveFile {
            file_name: name.into(),
            row_count: rows,
        }
    }

    /// Fake ANN searcher returning preset results and recording calls.
    struct FakeAnnSearcher {
        result: Vec<PkVectorSearchResult>,
    }
    impl PkVectorAnnSearcher for FakeAnnSearcher {
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
            Ok(self.result.clone())
        }
    }

    #[test]
    fn test_rejects_non_positive_limit() {
        let mut factory =
            |_: &BucketActiveFile| -> crate::Result<Box<dyn PkVectorReader>> { unreachable!() };
        let err = bucket_search(
            None,
            &[],
            &[],
            &HashMap::new(),
            &mut factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            0,
            &HashMap::new(),
            false,
        )
        .unwrap_err();
        assert!(err.to_string().contains("positive"));
    }

    #[test]
    fn test_bounded_heap_evicts_by_best_first_tiebreak_over_limit() {
        // All candidates share distance 1.0, so eviction is decided purely by the
        // BEST_FIRST tie-break (data_file_name ASC, then row_position ASC). Feed
        // more than `limit` ANN hits and assert the kept set is the smallest
        // (file, position) pairs in that order. Locks the bounded-heap merge.
        let segment = BucketAnnSegment::for_test(meta(&[("data-1", 3)]));
        let hit = |file: &str, pos: i64| PkVectorSearchResult {
            data_file_name: file.into(),
            row_position: pos,
            distance: 1.0,
        };
        // Deliberately unsorted input across two files at the same distance.
        let ann = FakeAnnSearcher {
            result: vec![
                hit("data-2", 0),
                hit("data-1", 2),
                hit("data-1", 0),
                hit("data-2", 1),
                hit("data-1", 1),
            ],
        };
        let mut factory =
            |_: &BucketActiveFile| -> crate::Result<Box<dyn PkVectorReader>> { unreachable!() };
        let results = bucket_search(
            Some(&ann),
            &[segment],
            &[active("data-1", 3)],
            &HashMap::new(),
            &mut factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            3,
            &HashMap::new(),
            false,
        )
        .unwrap();
        // Top-3 BEST_FIRST: (data-1,0), (data-1,1), (data-1,2) — the larger
        // data_file_name "data-2" entries are evicted despite equal distance.
        assert_eq!(
            results
                .iter()
                .map(|r| (r.data_file_name.as_str(), r.row_position))
                .collect::<Vec<_>>(),
            vec![("data-1", 0), ("data-1", 1), ("data-1", 2)]
        );
    }

    #[test]
    fn nan_ann_hit_never_evicts_finite_candidate_from_top1() {
        // The core failure mode from review: an ANN hit with a negative-NaN
        // distance must not win the single bucket Top-1 slot over a finite hit.
        // Under f32::total_cmp the -NaN would rank best and evict the finite
        // candidate here in the bucket heap, before any cross-bucket merge.
        let negative_nan = f32::from_bits(0xffc00000);
        assert!(negative_nan.is_nan());
        let segment = BucketAnnSegment::for_test(meta(&[("data-1", 2)]));
        let ann = FakeAnnSearcher {
            result: vec![
                PkVectorSearchResult {
                    data_file_name: "data-1".into(),
                    row_position: 0,
                    distance: negative_nan,
                },
                PkVectorSearchResult {
                    data_file_name: "data-1".into(),
                    row_position: 1,
                    distance: -1.0,
                },
            ],
        };
        let mut factory =
            |_: &BucketActiveFile| -> crate::Result<Box<dyn PkVectorReader>> { unreachable!() };
        let results = bucket_search(
            Some(&ann),
            &[segment],
            &[active("data-1", 2)],
            &HashMap::new(),
            &mut factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            1,
            &HashMap::new(),
            false,
        )
        .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].row_position, 1);
        assert_eq!(results[0].distance, -1.0);
    }

    #[test]
    fn test_merges_ann_and_exact_without_rescanning_covered_files() {
        // data-1 is ANN-covered; data-2 is exact fallback. Factory must never be
        // called for data-1.
        let segment = BucketAnnSegment::for_test(meta(&[("data-1", 2)]));
        let ann = FakeAnnSearcher {
            result: vec![PkVectorSearchResult {
                data_file_name: "data-1".into(),
                row_position: 1,
                distance: 0.5,
            }],
        };
        let calls = RefCell::new(Vec::<String>::new());
        let mut factory = |f: &BucketActiveFile| -> crate::Result<Box<dyn PkVectorReader>> {
            calls.borrow_mut().push(f.file_name.clone());
            // data-2 vectors: pos0 {1,0} dist 1.0, pos1 {3,0} dist 9.0
            Ok(Box::new(ArrayReader::new(
                2,
                vec![Some(vec![1.0, 0.0]), Some(vec![3.0, 0.0])],
            )))
        };
        let results = bucket_search(
            Some(&ann),
            &[segment],
            &[active("data-1", 2), active("data-2", 2)],
            &HashMap::new(),
            &mut factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            2,
            &HashMap::new(),
            false,
        )
        .unwrap();
        assert_eq!(
            results,
            vec![
                PkVectorSearchResult {
                    data_file_name: "data-1".into(),
                    row_position: 1,
                    distance: 0.5
                },
                PkVectorSearchResult {
                    data_file_name: "data-2".into(),
                    row_position: 0,
                    distance: 1.0
                },
            ]
        );
        assert_eq!(calls.borrow().as_slice(), &["data-2".to_string()]);
    }

    #[test]
    fn test_exact_fallback_merges_files_and_applies_deletion_vectors() {
        // No ANN. data-1 pos0 {0,0} deleted; remaining candidates merge across files.
        let calls = RefCell::new(0);
        let mut factory = |f: &BucketActiveFile| -> crate::Result<Box<dyn PkVectorReader>> {
            *calls.borrow_mut() += 1;
            let vectors = match f.file_name.as_str() {
                "data-1" => vec![Some(vec![0.0, 0.0]), Some(vec![2.0, 0.0])],
                "data-2" => vec![Some(vec![1.0, 0.0]), None],
                _ => unreachable!(),
            };
            Ok(Box::new(ArrayReader::new(2, vectors)))
        };
        let mut dvs: HashMap<String, Arc<DeletionVector>> = HashMap::new();
        let mut bm = RoaringBitmap::new();
        bm.insert(0); // data-1 position 0 deleted
        dvs.insert("data-1".into(), Arc::new(DeletionVector::from_bitmap(bm)));

        let results = bucket_search(
            None,
            &[],
            &[active("data-1", 2), active("data-2", 2)],
            &dvs,
            &mut factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            2,
            &HashMap::new(),
            false,
        )
        .unwrap();
        // Candidates: data-2 pos0 {1,0} dist 1.0; data-1 pos1 {2,0} dist 4.0.
        // (data-1 pos0 deleted, data-2 pos1 null.)
        assert_eq!(
            results,
            vec![
                PkVectorSearchResult {
                    data_file_name: "data-2".into(),
                    row_position: 0,
                    distance: 1.0
                },
                PkVectorSearchResult {
                    data_file_name: "data-1".into(),
                    row_position: 1,
                    distance: 4.0
                },
            ]
        );
    }

    #[test]
    fn test_rejects_duplicate_active_file_name() {
        let mut factory =
            |_: &BucketActiveFile| -> crate::Result<Box<dyn PkVectorReader>> { unreachable!() };
        let err = bucket_search(
            None,
            &[],
            &[active("dup", 1), active("dup", 1)],
            &HashMap::new(),
            &mut factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            1,
            &HashMap::new(),
            false,
        )
        .unwrap_err();
        assert!(err.to_string().contains("duplicate") || err.to_string().contains("Duplicate"));
    }

    #[test]
    fn test_rejects_ann_source_row_count_mismatch_for_active_file() {
        let ann = FakeAnnSearcher { result: vec![] };
        // Segment references data-1 with 2 rows, but the active file has 3 rows.
        // An active source with a mismatched row count is still a hard error.
        let segment = BucketAnnSegment::for_test(meta(&[("data-1", 2)]));
        let mut factory =
            |_: &BucketActiveFile| -> crate::Result<Box<dyn PkVectorReader>> { unreachable!() };
        let err = bucket_search(
            Some(&ann),
            &[segment],
            &[active("data-1", 3)],
            &HashMap::new(),
            &mut factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            1,
            &HashMap::new(),
            false,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("does not match") || err.to_string().contains("ANN source")
        );
    }

    #[test]
    fn test_skips_inactive_ann_source_and_searches_active_ones() {
        // Segment covers [data-1, data-2] but only data-1 is still active
        // (data-2 was compacted away). Java master skips the inactive source
        // instead of failing the whole query; data-2 is neither covered (so it
        // is not treated as ANN-covered) nor an active file (so it is not exact
        // scanned). The ANN searcher still runs for the segment.
        let segment = BucketAnnSegment::for_test(meta(&[("data-1", 2), ("data-2", 2)]));
        let ann = FakeAnnSearcher {
            result: vec![PkVectorSearchResult {
                data_file_name: "data-1".into(),
                row_position: 0,
                distance: 0.5,
            }],
        };
        let calls = RefCell::new(Vec::<String>::new());
        let mut factory = |f: &BucketActiveFile| -> crate::Result<Box<dyn PkVectorReader>> {
            calls.borrow_mut().push(f.file_name.clone());
            unreachable!("only data-1 is active and it is ANN-covered")
        };
        let results = bucket_search(
            Some(&ann),
            &[segment],
            &[active("data-1", 2)],
            &HashMap::new(),
            &mut factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            2,
            &HashMap::new(),
            false,
        )
        .unwrap();
        assert_eq!(
            results,
            vec![PkVectorSearchResult {
                data_file_name: "data-1".into(),
                row_position: 0,
                distance: 0.5
            }]
        );
        // No exact fallback ran: data-1 is ANN-covered, data-2 is not active.
        assert!(calls.borrow().is_empty());
    }

    #[test]
    fn test_rejects_segments_without_ann_searcher() {
        let segment = BucketAnnSegment::for_test(meta(&[("data-1", 2)]));
        let mut factory =
            |_: &BucketActiveFile| -> crate::Result<Box<dyn PkVectorReader>> { unreachable!() };
        let err = bucket_search(
            None,
            &[segment],
            &[active("data-1", 2)],
            &HashMap::new(),
            &mut factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            1,
            &HashMap::new(),
            false,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("ANN search is not configured")
                || err.to_string().contains("not configured")
        );
    }

    #[test]
    fn test_skip_exact_fallback_does_not_call_factory() {
        // No ANN segments, two active files. With skip_exact_fallback = true the
        // factory must never be called and the result is empty.
        let mut factory =
            |_: &BucketActiveFile| -> crate::Result<Box<dyn PkVectorReader>> { unreachable!() };
        let results = bucket_search(
            None,
            &[],
            &[active("data-1", 2), active("data-2", 2)],
            &HashMap::new(),
            &mut factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            2,
            &HashMap::new(),
            true, // skip_exact_fallback
        )
        .unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_rejects_duplicate_ann_segment_path() {
        let seg1 = BucketAnnSegment {
            source_meta: meta(&[("data-1", 2)]),
            path: "duplicate-path".to_string(),
            file_size: 100,
            index_meta: vec![1, 2, 3],
        };
        let seg2 = BucketAnnSegment {
            source_meta: meta(&[("data-2", 2)]),
            path: "duplicate-path".to_string(),
            file_size: 200,
            index_meta: vec![4, 5, 6],
        };
        let ann = FakeAnnSearcher { result: vec![] };
        let mut factory =
            |_: &BucketActiveFile| -> crate::Result<Box<dyn PkVectorReader>> { unreachable!() };
        let err = bucket_search(
            Some(&ann),
            &[seg1, seg2],
            &[active("data-1", 2), active("data-2", 2)],
            &HashMap::new(),
            &mut factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            1,
            &HashMap::new(),
            false,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("duplicate-path")
                && err.to_string().contains("appears more than once")
        );
    }

    #[test]
    fn test_rejects_source_file_covered_by_multiple_segments() {
        let seg1 = BucketAnnSegment {
            source_meta: meta(&[("data-1", 2)]),
            path: "segment-1".to_string(),
            file_size: 100,
            index_meta: vec![1, 2, 3],
        };
        let seg2 = BucketAnnSegment {
            source_meta: meta(&[("data-1", 2), ("data-2", 2)]),
            path: "segment-2".to_string(),
            file_size: 200,
            index_meta: vec![4, 5, 6],
        };
        let ann = FakeAnnSearcher { result: vec![] };
        let mut factory =
            |_: &BucketActiveFile| -> crate::Result<Box<dyn PkVectorReader>> { unreachable!() };
        let err = bucket_search(
            Some(&ann),
            &[seg1, seg2],
            &[active("data-1", 2), active("data-2", 2)],
            &HashMap::new(),
            &mut factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            1,
            &HashMap::new(),
            false,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("data-1")
                && err.to_string().contains("covered by both")
                && err.to_string().contains("segment-1")
                && err.to_string().contains("segment-2")
        );
    }

    #[test]
    fn test_negative_active_row_count_rejected() {
        let mut factory =
            |_: &BucketActiveFile| -> crate::Result<Box<dyn PkVectorReader>> { unreachable!() };
        let err = bucket_search(
            None,
            &[],
            &[active("data-1", -1)],
            &HashMap::new(),
            &mut factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            1,
            &HashMap::new(),
            false,
        )
        .unwrap_err();
        assert!(err.to_string().contains("row count") || err.to_string().contains("-1"));
    }

    #[test]
    fn covered_source_files_matches_by_name_and_row_count() {
        // "data-1" is an active ANN source with matching row count -> covered.
        // "data-2" is active but its row count disagrees with the ANN source -> not
        // covered (bucket_search rejects that separately). "data-3" is an active
        // file with no ANN source -> not covered (it needs an exact reader).
        let segment = BucketAnnSegment::for_test(meta(&[("data-1", 3), ("data-2", 9)]));
        let active = vec![
            active("data-1", 3),
            active("data-2", 2),
            active("data-3", 5),
        ];
        let covered = covered_source_files(&[segment], &active);
        assert!(covered.contains("data-1"));
        assert!(!covered.contains("data-2"));
        assert!(!covered.contains("data-3"));
        assert_eq!(covered.len(), 1);
    }

    #[test]
    fn covered_source_files_ignores_inactive_source() {
        // ANN source names a file that is not active (compacted away) -> not
        // covered, and no active file needs it.
        let segment = BucketAnnSegment::for_test(meta(&[("gone", 4)]));
        let active = vec![active("data-1", 3)];
        let covered = covered_source_files(&[segment], &active);
        assert!(covered.is_empty());
    }
}
