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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::bucket::BucketAnnSegment;
use super::data_invalid;
use super::metric::{java_float_compare, VectorSearchMetric};
use super::result::PkVectorSearchResult;
use crate::deletion_vector::DeletionVector;
use crate::spec::{PkVectorSourceFile, PkVectorSourceMeta};
use crate::vector_search::VectorSearch;

/// Build the live-row-id mask for the ANN reader's `include_row_ids` filter, in
/// segment-ordinal space (source files concatenated in order). Mirrors Java
/// `PkVectorAnnSegmentSearcher.liveRowPositions`.
///
/// Only source files present in `active_source_files` contribute live ordinals;
/// inactive sources' ordinal ranges are masked out entirely (their rows are no
/// longer readable in this snapshot). Deletion vectors are applied only to active
/// sources.
///
/// Returns `None` only when every source file is active AND no deletion vector is
/// relevant — nothing to mask. Otherwise returns the masked live ids.
pub(crate) fn build_live_row_ids(
    source_files: &[PkVectorSourceFile],
    active_source_files: &HashSet<String>,
    deletion_vectors: &HashMap<String, Arc<DeletionVector>>,
) -> crate::Result<Option<roaring::RoaringTreemap>> {
    let all_active = source_files
        .iter()
        .all(|f| active_source_files.contains(f.file_name()));
    let has_relevant_dv = source_files
        .iter()
        .any(|f| deletion_vectors.contains_key(f.file_name()));
    if all_active && !has_relevant_dv {
        return Ok(None);
    }

    let mut live = roaring::RoaringTreemap::new();
    let mut deleted = roaring::RoaringTreemap::new();
    let mut file_offset: u64 = 0;
    for source_file in source_files {
        let row_count = u64::try_from(source_file.row_count())
            .map_err(|_| data_invalid("vector source row count must not be negative"))?;
        let end = file_offset
            .checked_add(row_count)
            .ok_or_else(|| data_invalid("vector source row counts overflow u64"))?;
        let active = active_source_files.contains(source_file.file_name());
        if active && row_count > 0 {
            live.insert_range(file_offset..end);
        }
        if active {
            if let Some(dv) = deletion_vectors.get(source_file.file_name()) {
                for position in dv.iter() {
                    let global = file_offset.checked_add(position).ok_or_else(|| {
                        data_invalid("vector source deleted position overflows u64")
                    })?;
                    deleted.insert(global);
                }
            }
        }
        file_offset = end;
    }
    live -= deleted;
    Ok(Some(live))
}

/// Map ANN `(ordinal, score)` pairs to physical `(data file, position)` results,
/// validating ordinals against source metadata, rejecting hits that resolve to an
/// inactive source file, and rejecting hits on snapshot-deleted rows. Mirrors the
/// post-processing loop of Java `PkVectorAnnSegmentSearcher.search`. Results are
/// sorted BEST_FIRST.
pub(crate) fn map_ann_results(
    scored: &[(u64, f32)],
    source_meta: &PkVectorSourceMeta,
    active_source_files: &HashSet<String>,
    deletion_vectors: &HashMap<String, Arc<DeletionVector>>,
    metric: VectorSearchMetric,
) -> crate::Result<Vec<PkVectorSearchResult>> {
    let mut results = Vec::with_capacity(scored.len());
    for &(ordinal, score) in scored {
        let ordinal_i64 = i64::try_from(ordinal)
            .map_err(|_| data_invalid(format!("ANN ordinal {ordinal} exceeds i64::MAX")))?;
        let (data_file_name, row_position) = source_meta.resolve(ordinal_i64)?;
        if !active_source_files.contains(&data_file_name) {
            return Err(data_invalid(format!(
                "ANN segment returned inactive source {data_file_name}"
            )));
        }
        if let Some(dv) = deletion_vectors.get(&data_file_name) {
            let pos = u64::try_from(row_position)
                .map_err(|_| data_invalid("resolved row position must not be negative"))?;
            if dv.is_deleted(pos) {
                return Err(data_invalid(format!(
                    "ANN segment returned snapshot-deleted row position {row_position} in {data_file_name}"
                )));
            }
        }
        results.push(PkVectorSearchResult {
            data_file_name,
            row_position,
            distance: metric.score_to_distance(score),
        });
    }
    results.sort_by(|a, b| {
        java_float_compare(a.distance, b.distance)
            .then_with(|| a.data_file_name.cmp(&b.data_file_name))
            .then_with(|| a.row_position.cmp(&b.row_position))
    });
    Ok(results)
}

/// One ANN segment's search dependency for the bucket kernel. Bucket tests fake
/// this (mirroring Java's mock of `PkVectorAnnSegmentSearcher`).
///
/// `Send + Sync` so a `&dyn PkVectorAnnSearcher` can be held across the `.await`
/// points of the async search path (the returned future is spawned on a `Send`
/// runtime by callers such as the DataFusion integration).
pub(crate) trait PkVectorAnnSearcher: Send + Sync {
    #[allow(clippy::too_many_arguments)]
    fn search(
        &self,
        segment: &BucketAnnSegment,
        query: &[f32],
        metric: VectorSearchMetric,
        limit: usize,
        active_source_files: &HashSet<String>,
        deletion_vectors: &HashMap<String, Arc<DeletionVector>>,
        search_options: &HashMap<String, String>,
    ) -> crate::Result<Vec<PkVectorSearchResult>>;
}

/// Scorer seam: drives the underlying vindex ANN reader. Returns `ordinal ->
/// score` (higher-is-better). Any negative labels are skipped by the existing
/// `vindex` reader (`collect_results` drops `row_id < 0`), so this seam only
/// ever yields non-negative `u64` ordinals — no signed-label handling is needed
/// downstream.
///
/// The production scorer drives `VindexVectorGlobalIndexReader::visit_vector_search`
/// with a segment's index bytes; tests inject a synthetic scorer. The adapter's
/// own logic (live-row masking, ordinal mapping, deletion checks, ordering) is
/// exercised independently of the scorer.
pub(crate) type Scorer = Box<
    dyn Fn(&BucketAnnSegment, &VectorSearch) -> crate::Result<Option<HashMap<u64, f32>>>
        + Send
        + Sync,
>;

/// Structural vindex-backed `PkVectorAnnSearcher`. Composes the pure helpers
/// (`build_live_row_ids`, `map_ann_results`) around the scorer seam.
pub(crate) struct VindexAnnSearcher {
    field_name: String,
    scorer: Scorer,
}

impl VindexAnnSearcher {
    pub(crate) fn new(field_name: String, scorer: Scorer) -> Self {
        Self { field_name, scorer }
    }
}

impl PkVectorAnnSearcher for VindexAnnSearcher {
    fn search(
        &self,
        segment: &BucketAnnSegment,
        query: &[f32],
        metric: VectorSearchMetric,
        limit: usize,
        active_source_files: &HashSet<String>,
        deletion_vectors: &HashMap<String, Arc<DeletionVector>>,
        search_options: &HashMap<String, String>,
    ) -> crate::Result<Vec<PkVectorSearchResult>> {
        if limit == 0 {
            return Err(data_invalid("vector search limit must be positive"));
        }
        let source_files = segment.source_meta.source_files();
        let mut search = VectorSearch::new(query.to_vec(), limit, self.field_name.clone())?
            .with_options(search_options.clone());
        if let Some(live) = build_live_row_ids(source_files, active_source_files, deletion_vectors)?
        {
            search = search.with_include_row_ids(live);
        }
        let scored = match (self.scorer)(segment, &search)? {
            Some(map) => map,
            None => return Ok(Vec::new()),
        };
        let scored: Vec<(u64, f32)> = scored.into_iter().collect();
        map_ann_results(
            &scored,
            &segment.source_meta,
            active_source_files,
            deletion_vectors,
            metric,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use roaring::RoaringBitmap;

    fn source_meta(files: &[(&str, i64)]) -> PkVectorSourceMeta {
        let files = files
            .iter()
            .map(|(name, rows)| PkVectorSourceFile::new((*name).to_string(), *rows).unwrap())
            .collect();
        PkVectorSourceMeta::new(1, files).unwrap()
    }

    fn dv(deleted: &[u32]) -> Arc<DeletionVector> {
        let mut bitmap = RoaringBitmap::new();
        for &p in deleted {
            bitmap.insert(p);
        }
        Arc::new(DeletionVector::from_bitmap(bitmap))
    }

    fn active_set(names: &[&str]) -> HashSet<String> {
        names.iter().map(|n| (*n).to_string()).collect()
    }

    #[test]
    fn test_build_live_row_ids_none_when_all_active_and_no_relevant_dv() {
        let files = [PkVectorSourceFile::new("f0".into(), 3).unwrap()];
        let active = active_set(&["f0"]);
        // All active + empty map -> None.
        assert!(build_live_row_ids(&files, &active, &HashMap::new())
            .unwrap()
            .is_none());
        // All active + non-empty map but no matching file name -> None.
        let mut dvs = HashMap::new();
        dvs.insert("other".to_string(), dv(&[0]));
        assert!(build_live_row_ids(&files, &active, &dvs).unwrap().is_none());
    }

    #[test]
    fn test_build_live_row_ids_masks_inactive_source_ordinal_range() {
        // f0 rows 0..3 (global 0,1,2), f1 rows 0..2 (global 3,4). f1 is inactive,
        // so its whole ordinal range is masked out; f0 stays fully live. No DV.
        let files = vec![
            PkVectorSourceFile::new("f0".into(), 3).unwrap(),
            PkVectorSourceFile::new("f1".into(), 2).unwrap(),
        ];
        let live = build_live_row_ids(&files, &active_set(&["f0"]), &HashMap::new())
            .unwrap()
            .unwrap();
        assert_eq!(live.iter().collect::<Vec<u64>>(), vec![0, 1, 2]);
    }

    #[test]
    fn test_build_live_row_ids_masks_deleted_positions_with_file_offsets() {
        // f0 rows 0..3 (global 0,1,2), f1 rows 0..2 (global 3,4).
        let files = vec![
            PkVectorSourceFile::new("f0".into(), 3).unwrap(),
            PkVectorSourceFile::new("f1".into(), 2).unwrap(),
        ];
        let mut dvs = HashMap::new();
        dvs.insert("f0".to_string(), dv(&[1])); // deletes global 1
        dvs.insert("f1".to_string(), dv(&[0])); // deletes global 3
        let live = build_live_row_ids(&files, &active_set(&["f0", "f1"]), &dvs)
            .unwrap()
            .unwrap();
        assert_eq!(live.iter().collect::<Vec<u64>>(), vec![0, 2, 4]);
    }

    #[test]
    fn test_map_ann_results_maps_ordinals_to_positions_and_scores() {
        let meta = source_meta(&[("f0", 3), ("f1", 5)]);
        // ordinal 3 -> (f1, 0); ordinal 0 -> (f0, 0). l2 score_to_distance(0.5)=1.0.
        let scored = [(3u64, 0.5f32), (0u64, 0.5f32)];
        let results = map_ann_results(
            &scored,
            &meta,
            &active_set(&["f0", "f1"]),
            &HashMap::new(),
            VectorSearchMetric::L2,
        )
        .unwrap();
        assert_eq!(
            results,
            vec![
                PkVectorSearchResult {
                    data_file_name: "f0".into(),
                    row_position: 0,
                    distance: 1.0
                },
                PkVectorSearchResult {
                    data_file_name: "f1".into(),
                    row_position: 0,
                    distance: 1.0
                },
            ]
        );
    }

    #[test]
    fn test_map_ann_results_rejects_out_of_range_ordinal() {
        let meta = source_meta(&[("f0", 3)]);
        let err = map_ann_results(
            &[(3u64, 0.5)],
            &meta,
            &active_set(&["f0"]),
            &HashMap::new(),
            VectorSearchMetric::L2,
        )
        .unwrap_err();
        assert!(err.to_string().contains("out of range") || err.to_string().contains("ordinal"));
    }

    #[test]
    fn test_map_ann_results_rejects_hit_resolving_to_inactive_source() {
        // ordinal 3 resolves to f1, which is not in the active set -> error.
        let meta = source_meta(&[("f0", 3), ("f1", 5)]);
        let err = map_ann_results(
            &[(3u64, 0.5)],
            &meta,
            &active_set(&["f0"]),
            &HashMap::new(),
            VectorSearchMetric::L2,
        )
        .unwrap_err();
        assert!(err.to_string().contains("inactive"));
    }

    #[test]
    fn test_map_ann_results_rejects_hit_on_deleted_position() {
        let meta = source_meta(&[("f0", 3)]);
        let mut dvs = HashMap::new();
        dvs.insert("f0".to_string(), dv(&[1])); // position 1 deleted
        let err = map_ann_results(
            &[(1u64, 0.5)],
            &meta,
            &active_set(&["f0"]),
            &dvs,
            VectorSearchMetric::L2,
        )
        .unwrap_err();
        assert!(err.to_string().contains("deleted"));
    }

    #[test]
    fn test_vindex_adapter_composes_live_rows_and_maps_results() {
        // Scorer records the VectorSearch it received and returns synthetic ordinals.
        // The scorer must be `'static` and `Send + Sync`, so share the recording
        // cells via `Arc<Mutex<..>>` moved into the closure rather than borrowing
        // locals.
        use std::sync::{Arc, Mutex};
        let seen_limit = Arc::new(Mutex::new(0usize));
        let seen_has_filter = Arc::new(Mutex::new(false));
        let scorer_limit = Arc::clone(&seen_limit);
        let scorer_has_filter = Arc::clone(&seen_has_filter);
        let searcher = VindexAnnSearcher::new(
            "embedding".to_string(),
            Box::new(move |_segment: &BucketAnnSegment, search: &VectorSearch| {
                *scorer_limit.lock().unwrap() = search.limit;
                *scorer_has_filter.lock().unwrap() = search.include_row_ids.is_some();
                let mut scores = HashMap::new();
                scores.insert(3u64, 0.5f32); // -> (f1, 0)
                scores.insert(0u64, 0.25f32); // -> (f0, 0), l2 dist 3.0
                Ok(Some(scores))
            }),
        );
        let segment = BucketAnnSegment::for_test({
            use crate::spec::{PkVectorSourceFile, PkVectorSourceMeta};
            PkVectorSourceMeta::new(
                1,
                vec![
                    PkVectorSourceFile::new("f0".into(), 3).unwrap(),
                    PkVectorSourceFile::new("f1".into(), 5).unwrap(),
                ],
            )
            .unwrap()
        });
        let mut dvs = HashMap::new();
        dvs.insert("f0".to_string(), dv(&[1]));
        let results = searcher
            .search(
                &segment,
                &[0.0, 0.0],
                VectorSearchMetric::L2,
                2,
                &active_set(&["f0", "f1"]),
                &dvs,
                &HashMap::new(),
            )
            .unwrap();
        // Sorted BEST_FIRST by distance: (f1,0) dist 1.0 then (f0,0) dist 3.0.
        assert_eq!(results[0].data_file_name, "f1");
        assert_eq!(results[1].data_file_name, "f0");
        assert_eq!(*seen_limit.lock().unwrap(), 2);
        assert!(
            *seen_has_filter.lock().unwrap(),
            "DV present -> include_row_ids set"
        );
    }

    #[test]
    fn test_vindex_adapter_rejects_non_positive_limit() {
        let searcher = VindexAnnSearcher::new(
            "embedding".to_string(),
            Box::new(|_: &BucketAnnSegment, _: &VectorSearch| Ok(None)),
        );
        let segment = BucketAnnSegment::for_test({
            use crate::spec::{PkVectorSourceFile, PkVectorSourceMeta};
            PkVectorSourceMeta::new(1, vec![PkVectorSourceFile::new("f0".into(), 1).unwrap()])
                .unwrap()
        });
        let err = searcher
            .search(
                &segment,
                &[0.0, 0.0],
                VectorSearchMetric::L2,
                0,
                &active_set(&["f0"]),
                &HashMap::new(),
                &HashMap::new(),
            )
            .unwrap_err();
        assert!(err.to_string().contains("positive"));
    }

    #[test]
    fn test_vindex_adapter_empty_scorer_result_is_empty() {
        let searcher = VindexAnnSearcher::new(
            "embedding".to_string(),
            Box::new(|_: &BucketAnnSegment, _: &VectorSearch| Ok(None)),
        );
        let segment = BucketAnnSegment::for_test({
            use crate::spec::{PkVectorSourceFile, PkVectorSourceMeta};
            PkVectorSourceMeta::new(1, vec![PkVectorSourceFile::new("f0".into(), 1).unwrap()])
                .unwrap()
        });
        let results = searcher
            .search(
                &segment,
                &[0.0, 0.0],
                VectorSearchMetric::L2,
                2,
                &active_set(&["f0"]),
                &HashMap::new(),
                &HashMap::new(),
            )
            .unwrap();
        assert!(results.is_empty());
    }
}
