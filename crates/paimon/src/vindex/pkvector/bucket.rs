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

use futures::future::BoxFuture;
use futures::stream::{self, StreamExt, TryStreamExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::ann::PkVectorAnnSearcher;
use super::data_invalid;
use super::metric::{java_float_compare, VectorSearchMetric};
use super::result::PkVectorSearchResult;
use crate::deletion_vector::DeletionVector;
use crate::spec::PkVectorSourceMeta;

/// Search one uncovered data file for its per-query exact Top-K. Returns one
/// bounded, BEST_FIRST list per query (outer index aligns to the `queries` slice
/// passed to the closure). The table-layer implementation streams the file's
/// vector column one Arrow batch at a time, so peak memory is one batch plus the
/// bounded heaps, not the whole column. `is_excluded(position)` folds residual +
/// deletion-vector exclusion (built by `bucket_search`, which owns those inputs).
pub(crate) type ExactFileSearchFuture<'a> =
    BoxFuture<'a, crate::Result<Vec<Vec<PkVectorSearchResult>>>>;

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

/// Extract the sole per-query list from a single-query exact-file search result,
/// failing loud if the closure returned no lists.
fn single_query_result(
    per_query: Vec<Vec<PkVectorSearchResult>>,
) -> crate::Result<Vec<PkVectorSearchResult>> {
    per_query
        .into_iter()
        .next()
        .ok_or_else(|| data_invalid("exact file search returned no per-query results"))
}

/// Verify a multi-query exact-file search returned exactly one list per query,
/// failing loud on an arity mismatch.
fn validate_per_query_len(
    per_query: Vec<Vec<PkVectorSearchResult>>,
    expected: usize,
) -> crate::Result<Vec<Vec<PkVectorSearchResult>>> {
    if per_query.len() != expected {
        return Err(data_invalid(format!(
            "exact file search returned {} result lists for {} queries",
            per_query.len(),
            expected
        )));
    }
    Ok(per_query)
}

/// Build the per-position exclusion predicate for one uncovered exact file: a
/// physical position is excluded if the deletion vector marks it deleted, or
/// (when a residual allow-list is present) if it is not in the allow-list. Folds
/// the residual ∩ DV exclusion the exact search applies per row. The returned
/// closure borrows the allow-list for its lifetime.
fn position_excluder(
    dv: Option<Arc<DeletionVector>>,
    residual_allowed: Option<&roaring::RoaringTreemap>,
) -> impl Fn(i64) -> bool + Sync + '_ {
    move |position: i64| -> bool {
        let dv_deleted = match &dv {
            Some(dv) => u64::try_from(position)
                .map(|p| dv.is_deleted(p))
                .unwrap_or(false),
            None => false,
        };
        if dv_deleted {
            return true;
        }
        match residual_allowed {
            // No residual restriction: the row is allowed.
            None => false,
            // Residual present: exclude positions outside the allow-list.
            Some(allowed) => match u64::try_from(position) {
                Ok(p) => !allowed.contains(p),
                Err(_) => true,
            },
        }
    }
}

/// Active data files whose rows are already covered by an ANN segment's source
/// metadata, matched by both file name AND row count. The bucket exact fallback
/// skips these files (an ANN segment already covers their rows), so they never
/// need an exact reader. A source naming an inactive file, or one whose row
/// count disagrees with the active file, is not covered here; `bucket_search`
/// rejects the row-count mismatch separately.
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

/// Acquire one slot from the shared global-index search concurrency budget, if a
/// budget is set. The returned guard holds the slot until it is dropped, so the
/// caller must keep it alive for the duration of the leaf exact-file I/O it gates.
///
/// A `None` budget means the leaf runs ungated (no cap) — the function does not
/// require any particular `concurrency` value; the orchestrator simply passes
/// `None` on the strictly sequential `concurrency <= 1` path (which needs no
/// gating). A `Some` budget is a single [`Semaphore`] shared across every bucket
/// and every exact file of one search, so total in-flight exact-file I/O is capped
/// at N regardless of how many buckets and files fan out — mirroring Java's single
/// shared `GlobalIndexReadThreadPool`. Only leaf exact-file work acquires a permit;
/// bucket orchestration never holds one, so it cannot starve leaf work (the async
/// analogue of Java's "start from the caller" note in
/// `PrimaryKeyVectorRead.searchBuckets`).
async fn acquire_search_permit(
    budget: &Option<Arc<Semaphore>>,
) -> crate::Result<Option<OwnedSemaphorePermit>> {
    match budget {
        Some(semaphore) => {
            let permit = semaphore.clone().acquire_owned().await.map_err(|e| {
                crate::Error::UnexpectedError {
                    message: "global-index search concurrency budget was closed".to_string(),
                    source: Some(Box::new(e)),
                }
            })?;
            Ok(Some(permit))
        }
        None => Ok(None),
    }
}

/// Separately bounded approximate-index and exact-fallback candidates for one
/// bucket. The approximate list may be over-fetched (for later exact reranking)
/// while the exact-fallback list stays bounded to the caller's final limit.
#[derive(Debug)]
pub(crate) struct BucketSearchResult {
    pub(crate) indexed: Vec<PkVectorSearchResult>,
    pub(crate) exact: Vec<PkVectorSearchResult>,
}

/// ANN + exact data-file fallback search for one snapshot bucket. Mirrors Java
/// `org.apache.paimon.index.pkvector.PrimaryKeyVectorBucketSearch.search`.
///
/// `ann_searcher` may be `None` only when there are no ANN segments; segments
/// present with `None` is an error.
///
/// `residual_ranges` (when `Some`) is a residual-predicate allow-list keyed by
/// data-file name whose value is the set of physical row positions in that file
/// that pass the predicate; only those rows may produce candidates. `None` applies
/// no residual restriction (every row is allowed). A file absent from the map (or
/// with an empty set) has no allowed rows and produces no candidates. Mirrors Java
/// `rowRangesByFile`.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::type_complexity)]
pub(crate) async fn bucket_search(
    ann_searcher: Option<&dyn PkVectorAnnSearcher>,
    ann_segments: &[BucketAnnSegment],
    active_files: &[BucketActiveFile],
    deletion_vectors: &HashMap<String, Arc<DeletionVector>>,
    exact_file_search: &(dyn for<'a> Fn(
        &'a BucketActiveFile,
        &'a [&'a [f32]],
        VectorSearchMetric,
        usize,
        &'a (dyn Fn(i64) -> bool + Sync),
    ) -> ExactFileSearchFuture<'a>
          + Send
          + Sync),
    query: &[f32],
    metric: VectorSearchMetric,
    indexed_limit: usize,
    exact_limit: usize,
    search_options: &HashMap<String, String>,
    skip_exact_fallback: bool,
    residual_ranges: Option<&HashMap<String, roaring::RoaringTreemap>>,
    concurrency: usize,
    search_budget: Option<Arc<Semaphore>>,
) -> crate::Result<BucketSearchResult> {
    if indexed_limit == 0 {
        return Err(data_invalid("vector search limit must be positive"));
    }
    if exact_limit == 0 {
        return Err(data_invalid("vector search limit must be positive"));
    }
    // Validate-before-OPEN: a non-finite query element fails loud before any
    // exact-file search closure is invoked (so before any file stream is opened).
    // The per-file closure additionally validates the query dimension before it
    // polls its stream (validate-before-POLL).
    if let Some(i) = query.iter().position(|v| !v.is_finite()) {
        return Err(data_invalid(format!(
            "query vector element at position {i} must be finite"
        )));
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

    let mut indexed_heap: BinaryHeap<WorstFirst> = BinaryHeap::with_capacity(indexed_limit + 1);
    let mut exact_heap: BinaryHeap<WorstFirst> = BinaryHeap::with_capacity(exact_limit + 1);
    let active_source_files: HashSet<String> =
        files_by_name.keys().map(|name| name.to_string()).collect();
    // Active files whose rows an ANN segment already covers; the exact fallback
    // skips them, so the lazy exact-reader factory is never invoked for those files.
    let covered = covered_source_files(ann_segments, active_files);

    // The ANN searcher is synchronous CPU work (no `.await`), so iterating segments
    // sequentially is intentional: fanning it out would need `spawn_blocking`. Only
    // the exact-fallback file reads below (which are async I/O) are parallelized.
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
            indexed_limit,
            &active_source_files,
            deletion_vectors,
            search_options,
            residual_ranges,
        )? {
            add_candidate(&mut indexed_heap, result, indexed_limit);
        }
    }

    if !skip_exact_fallback {
        // Collect the eligible uncovered files (active-file order preserved) with
        // their per-position exclusion predicate. A file with no residual-allowed
        // rows is skipped without reading.
        #[allow(clippy::type_complexity)]
        let mut tasks: Vec<(&BucketActiveFile, Box<dyn Fn(i64) -> bool + Sync>)> = Vec::new();
        for file in active_files {
            if covered.contains(&file.file_name) {
                continue;
            }
            // Residual allow-list: when present, only rows whose physical position
            // passes the predicate may produce candidates. A file with no entry (or
            // an empty entry) has no allowed rows, so it is skipped without reading.
            let residual_allowed: Option<&roaring::RoaringTreemap> = match residual_ranges {
                Some(ranges) => match ranges.get(&file.file_name) {
                    Some(allowed) if !allowed.is_empty() => Some(allowed),
                    _ => continue,
                },
                None => None,
            };
            let dv = deletion_vectors.get(&file.file_name).cloned();
            tasks.push((file, Box::new(position_excluder(dv, residual_allowed))));
        }

        // Search each uncovered file for its exact Top-K. The caller passes a
        // single-query slice and each search returns one per-query list. The
        // per-file results feed the bounded heap, which is order-independent, so
        // the merge does not depend on which file finished first. `concurrency == 1`
        // takes a plain sequential loop so the file visit order is strictly
        // deterministic; larger values fan the file searches out with
        // `buffer_unordered`, each acquiring one slot of the shared `search_budget`
        // so total in-flight exact-file I/O across all buckets is capped at N.
        let queries: [&[f32]; 1] = [query];
        let per_file: Vec<Vec<PkVectorSearchResult>> = if concurrency <= 1 {
            let mut out = Vec::with_capacity(tasks.len());
            for (file, is_excluded) in &tasks {
                let per_query =
                    exact_file_search(file, &queries, metric, exact_limit, is_excluded.as_ref())
                        .await?;
                out.push(single_query_result(per_query)?);
            }
            out
        } else {
            stream::iter(tasks.iter().map(|(file, is_excluded)| {
                let queries = &queries;
                let budget = search_budget.clone();
                async move {
                    let _permit = acquire_search_permit(&budget).await?;
                    let per_query =
                        exact_file_search(file, queries, metric, exact_limit, is_excluded.as_ref())
                            .await?;
                    single_query_result(per_query)
                }
            }))
            .buffer_unordered(concurrency)
            .try_collect::<Vec<_>>()
            .await?
        };
        for results in per_file {
            for result in results {
                add_candidate(&mut exact_heap, result, exact_limit);
            }
        }
    }

    let mut indexed: Vec<PkVectorSearchResult> = indexed_heap.into_iter().map(|w| w.0).collect();
    indexed.sort_by(best_first);
    let mut exact: Vec<PkVectorSearchResult> = exact_heap.into_iter().map(|w| w.0).collect();
    exact.sort_by(best_first);
    Ok(BucketSearchResult { indexed, exact })
}

/// Multi-query ANN + exact fallback search for one snapshot bucket, returning one
/// [`BucketSearchResult`] per query (outer index aligned to `queries`). N queries
/// share one pass over the ANN segments (the reader is opened once per segment and
/// every query is searched against it) and one stream per uncovered exact file (the
/// stream is opened once and every query is scored from it), with independent
/// per-query bounded heaps so no query's candidates bleed into another's.
///
/// `queries.len() == 1` short-circuits to the single-query [`bucket_search`] and
/// wraps its result in a one-element `Vec`, so a batch-of-one is byte-identical to
/// the single-query path (including tie ordering). The remaining arguments match
/// [`bucket_search`].
#[allow(clippy::too_many_arguments)]
#[allow(clippy::type_complexity)]
pub(crate) async fn bucket_search_batch(
    ann_searcher: Option<&dyn PkVectorAnnSearcher>,
    ann_segments: &[BucketAnnSegment],
    active_files: &[BucketActiveFile],
    deletion_vectors: &HashMap<String, Arc<DeletionVector>>,
    exact_file_search: &(dyn for<'a> Fn(
        &'a BucketActiveFile,
        &'a [&'a [f32]],
        VectorSearchMetric,
        usize,
        &'a (dyn Fn(i64) -> bool + Sync),
    ) -> ExactFileSearchFuture<'a>
          + Send
          + Sync),
    queries: &[&[f32]],
    metric: VectorSearchMetric,
    indexed_limit: usize,
    exact_limit: usize,
    search_options: &HashMap<String, String>,
    skip_exact_fallback: bool,
    residual_ranges: Option<&HashMap<String, roaring::RoaringTreemap>>,
    concurrency: usize,
    search_budget: Option<Arc<Semaphore>>,
) -> crate::Result<Vec<BucketSearchResult>> {
    if queries.is_empty() {
        return Err(data_invalid("vector search requires at least one query"));
    }
    // A batch-of-one routes to the single-query body so its tie ordering is
    // byte-identical to the pre-batch path (the multi-query heaps could in
    // principle differ).
    if queries.len() == 1 {
        let single = bucket_search(
            ann_searcher,
            ann_segments,
            active_files,
            deletion_vectors,
            exact_file_search,
            queries[0],
            metric,
            indexed_limit,
            exact_limit,
            search_options,
            skip_exact_fallback,
            residual_ranges,
            concurrency,
            search_budget,
        )
        .await?;
        return Ok(vec![single]);
    }

    if indexed_limit == 0 {
        return Err(data_invalid("vector search limit must be positive"));
    }
    if exact_limit == 0 {
        return Err(data_invalid("vector search limit must be positive"));
    }
    // Validate-before-OPEN: any non-finite query element fails loud before any
    // exact-file search closure is invoked (so before any file stream is opened).
    // The per-file closure additionally validates the query dimension before it
    // polls its stream (validate-before-POLL).
    for query in queries {
        if let Some(i) = query.iter().position(|v| !v.is_finite()) {
            return Err(data_invalid(format!(
                "query vector element at position {i} must be finite"
            )));
        }
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

    let mut indexed_heaps: Vec<BinaryHeap<WorstFirst>> = (0..queries.len())
        .map(|_| BinaryHeap::with_capacity(indexed_limit + 1))
        .collect();
    let mut exact_heaps: Vec<BinaryHeap<WorstFirst>> = (0..queries.len())
        .map(|_| BinaryHeap::with_capacity(exact_limit + 1))
        .collect();
    let active_source_files: HashSet<String> =
        files_by_name.keys().map(|name| name.to_string()).collect();
    let covered = covered_source_files(ann_segments, active_files);

    // The ANN searcher is synchronous CPU work (no `.await`), so iterating segments
    // sequentially is intentional: fanning it out would need `spawn_blocking`. Only
    // the exact-fallback file reads below (which are async I/O) are parallelized.
    for segment in ann_segments {
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
        // One shared reader per segment searches all queries; fan the per-query
        // hits into their own bounded heaps.
        let per_query = searcher.search_batch(
            segment,
            queries,
            metric,
            indexed_limit,
            &active_source_files,
            deletion_vectors,
            search_options,
            residual_ranges,
        )?;
        if per_query.len() != queries.len() {
            return Err(data_invalid(format!(
                "ANN batch search returned {} result lists for {} queries",
                per_query.len(),
                queries.len()
            )));
        }
        for (results, heap) in per_query.into_iter().zip(indexed_heaps.iter_mut()) {
            for result in results {
                add_candidate(heap, result, indexed_limit);
            }
        }
    }

    if !skip_exact_fallback {
        // Eligible uncovered files (active-file order preserved) with their
        // per-position exclusion predicate; a file with no residual-allowed rows is
        // skipped without reading.
        #[allow(clippy::type_complexity)]
        let mut tasks: Vec<(&BucketActiveFile, Box<dyn Fn(i64) -> bool + Sync>)> = Vec::new();
        for file in active_files {
            if covered.contains(&file.file_name) {
                continue;
            }
            let residual_allowed: Option<&roaring::RoaringTreemap> = match residual_ranges {
                Some(ranges) => match ranges.get(&file.file_name) {
                    Some(allowed) if !allowed.is_empty() => Some(allowed),
                    _ => continue,
                },
                None => None,
            };
            let dv = deletion_vectors.get(&file.file_name).cloned();
            tasks.push((file, Box::new(position_excluder(dv, residual_allowed))));
        }

        // One shared stream per file scores every query into its own per-query
        // list. The per-file lists feed per-query bounded heaps (order-independent),
        // so the fan-in does not depend on which file finished first: collect every
        // file's per-query result, then merge. `concurrency == 1` uses a strictly
        // sequential loop; larger values fan the file searches out with
        // `buffer_unordered`, each acquiring one slot of the shared `search_budget`
        // so total in-flight exact-file I/O across all buckets is capped at N.
        let per_file: Vec<Vec<Vec<PkVectorSearchResult>>> = if concurrency <= 1 {
            let mut out = Vec::with_capacity(tasks.len());
            for (file, is_excluded) in &tasks {
                let per_query =
                    exact_file_search(file, queries, metric, exact_limit, is_excluded.as_ref())
                        .await?;
                out.push(validate_per_query_len(per_query, queries.len())?);
            }
            out
        } else {
            stream::iter(tasks.iter().map(|(file, is_excluded)| {
                let budget = search_budget.clone();
                async move {
                    let _permit = acquire_search_permit(&budget).await?;
                    let per_query =
                        exact_file_search(file, queries, metric, exact_limit, is_excluded.as_ref())
                            .await?;
                    validate_per_query_len(per_query, queries.len())
                }
            }))
            .buffer_unordered(concurrency)
            .try_collect::<Vec<_>>()
            .await?
        };
        for per_query in per_file {
            for (results, heap) in per_query.into_iter().zip(exact_heaps.iter_mut()) {
                for result in results {
                    add_candidate(heap, result, exact_limit);
                }
            }
        }
    }

    let mut out = Vec::with_capacity(queries.len());
    for (indexed_heap, exact_heap) in indexed_heaps.into_iter().zip(exact_heaps) {
        let mut indexed: Vec<PkVectorSearchResult> =
            indexed_heap.into_iter().map(|w| w.0).collect();
        indexed.sort_by(best_first);
        let mut exact: Vec<PkVectorSearchResult> = exact_heap.into_iter().map(|w| w.0).collect();
        exact.sort_by(best_first);
        out.push(BucketSearchResult { indexed, exact });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::PkVectorSourceFile;
    use crate::vindex::pkvector::ann::PkVectorAnnSearcher;
    use crate::vindex::pkvector::exact::exact_search;
    use crate::vindex::pkvector::reader::test_support::ArrayReader;
    use roaring::RoaringBitmap;

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

    /// Coerce a closure into the higher-ranked per-file exact-search shape so its
    /// returned future borrows for exactly the argument lifetime. Closure return
    /// types cannot express this borrow through inference alone, so the bound is
    /// supplied here. The closure is `Fn + Send + Sync` (called concurrently by
    /// the parallel search), not `FnMut`.
    fn as_search<F>(f: F) -> F
    where
        F: for<'a> Fn(
                &'a BucketActiveFile,
                &'a [&'a [f32]],
                VectorSearchMetric,
                usize,
                &'a (dyn Fn(i64) -> bool + Sync),
            ) -> ExactFileSearchFuture<'a>
            + Send
            + Sync,
    {
        f
    }

    /// Build a per-file exact-search closure that runs the reference `exact_search`
    /// over an in-memory `ArrayReader` of `vectors`, returning one per-query list.
    /// The caller wires a single query, so the returned outer `Vec` has one element.
    #[allow(clippy::type_complexity)]
    fn array_search(
        vectors: Vec<Option<Vec<f32>>>,
    ) -> impl for<'a> Fn(
        &'a BucketActiveFile,
        &'a [&'a [f32]],
        VectorSearchMetric,
        usize,
        &'a (dyn Fn(i64) -> bool + Sync),
    ) -> ExactFileSearchFuture<'a>
           + Send
           + Sync {
        let vectors = Arc::new(vectors);
        as_search(
            move |file: &BucketActiveFile,
                  queries: &[&[f32]],
                  metric: VectorSearchMetric,
                  exact_limit: usize,
                  is_excluded: &(dyn Fn(i64) -> bool + Sync)|
                  -> ExactFileSearchFuture<'_> {
                let dimension = vectors
                    .first()
                    .and_then(|v| v.as_ref())
                    .map_or(0, |v| v.len());
                let owned: Vec<Option<Vec<f32>>> = (*vectors).clone();
                let file_name = file.file_name.clone();
                let query: Vec<f32> = queries[0].to_vec();
                Box::pin(async move {
                    let mut reader = ArrayReader::new(dimension, owned);
                    let results = exact_search(
                        &file_name,
                        &mut reader,
                        &query,
                        metric,
                        exact_limit,
                        is_excluded,
                    )?;
                    Ok(vec![results])
                })
            },
        )
    }

    /// A per-file exact-search closure that must never be invoked.
    #[allow(clippy::type_complexity)]
    fn unreachable_search() -> impl for<'a> Fn(
        &'a BucketActiveFile,
        &'a [&'a [f32]],
        VectorSearchMetric,
        usize,
        &'a (dyn Fn(i64) -> bool + Sync),
    ) -> ExactFileSearchFuture<'a>
           + Send
           + Sync {
        as_search(
            |_: &BucketActiveFile,
             _: &[&[f32]],
             _: VectorSearchMetric,
             _: usize,
             _: &(dyn Fn(i64) -> bool + Sync)|
             -> ExactFileSearchFuture<'_> { Box::pin(async { unreachable!() }) },
        )
    }

    /// Fake ANN searcher returning preset results and recording calls.
    struct FakeAnnSearcher {
        result: Vec<PkVectorSearchResult>,
    }
    impl PkVectorAnnSearcher for FakeAnnSearcher {
        fn search_batch(
            &self,
            _segment: &BucketAnnSegment,
            queries: &[&[f32]],
            _metric: VectorSearchMetric,
            _limit: usize,
            _active_source_files: &HashSet<String>,
            _dvs: &HashMap<String, Arc<DeletionVector>>,
            _opts: &HashMap<String, String>,
            _residual_ranges: Option<&HashMap<String, roaring::RoaringTreemap>>,
        ) -> crate::Result<Vec<Vec<PkVectorSearchResult>>> {
            Ok(queries.iter().map(|_| self.result.clone()).collect())
        }
    }

    #[tokio::test]
    async fn dual_limit_bounds_indexed_and_exact_independently() {
        // One ANN segment covering "ann.mosaic" (3 rows) and one uncovered exact
        // file "exact.mosaic" (3 rows). indexed_limit = 3 lets all ANN hits through;
        // exact_limit = 1 keeps only the single closest exact hit.
        let segment = BucketAnnSegment::for_test(meta(&[("ann.mosaic", 3)]));
        // Fake ANN searcher returns three hits at increasing distance.
        let ann = FakeAnnSearcher {
            result: vec![
                PkVectorSearchResult {
                    data_file_name: "ann.mosaic".into(),
                    row_position: 0,
                    distance: 0.1,
                },
                PkVectorSearchResult {
                    data_file_name: "ann.mosaic".into(),
                    row_position: 1,
                    distance: 0.2,
                },
                PkVectorSearchResult {
                    data_file_name: "ann.mosaic".into(),
                    row_position: 2,
                    distance: 0.3,
                },
            ],
        };
        // Exact file has three rows; query nearest is position 0.
        let factory = array_search(vec![
            Some(vec![1.0, 0.0]),
            Some(vec![9.0, 0.0]),
            Some(vec![8.0, 0.0]),
        ]);
        let active_files = vec![active("ann.mosaic", 3), active("exact.mosaic", 3)];
        let dvs: HashMap<String, Arc<DeletionVector>> = HashMap::new();
        let opts = HashMap::new();

        let out = bucket_search(
            Some(&ann),
            &[segment],
            &active_files,
            &dvs,
            &factory,
            &[1.0, 0.0],
            VectorSearchMetric::L2,
            3, // indexed_limit
            1, // exact_limit
            &opts,
            false,
            None,
            1,
            None,
        )
        .await
        .unwrap();

        assert_eq!(
            out.indexed.len(),
            3,
            "indexed heap keeps indexed_limit hits"
        );
        assert_eq!(out.exact.len(), 1, "exact heap bounded to exact_limit");
        assert_eq!(out.exact[0].data_file_name, "exact.mosaic");
        assert_eq!(out.exact[0].row_position, 0);
    }

    #[tokio::test]
    async fn test_rejects_non_positive_limit() {
        let factory = unreachable_search();
        let err = bucket_search(
            None,
            &[],
            &[],
            &HashMap::new(),
            &factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            0,
            0,
            &HashMap::new(),
            false,
            None,
            1,
            None,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("positive"));
    }

    #[tokio::test]
    async fn test_bounded_heap_evicts_by_best_first_tiebreak_over_limit() {
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
        let factory = unreachable_search();
        let out = bucket_search(
            Some(&ann),
            &[segment],
            &[active("data-1", 3)],
            &HashMap::new(),
            &factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            3,
            3,
            &HashMap::new(),
            false,
            None,
            1,
            None,
        )
        .await
        .unwrap();
        let mut results = out.indexed.clone();
        results.extend(out.exact.clone());
        results.sort_by(best_first);
        results.truncate(3);
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

    #[tokio::test]
    async fn nan_ann_hit_never_evicts_finite_candidate_from_top1() {
        // The core failure mode: an ANN hit with a negative-NaN distance must not
        // win the single bucket Top-1 slot over a finite hit. Under f32::total_cmp
        // the -NaN would rank best and evict the finite candidate here in the
        // bucket heap, before any cross-bucket merge.
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
        let factory = unreachable_search();
        let out = bucket_search(
            Some(&ann),
            &[segment],
            &[active("data-1", 2)],
            &HashMap::new(),
            &factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            1,
            1,
            &HashMap::new(),
            false,
            None,
            1,
            None,
        )
        .await
        .unwrap();
        let mut results = out.indexed.clone();
        results.extend(out.exact.clone());
        results.sort_by(best_first);
        results.truncate(1);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].row_position, 1);
        assert_eq!(results[0].distance, -1.0);
    }

    #[tokio::test]
    async fn test_merges_ann_and_exact_without_rescanning_covered_files() {
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
        let calls = std::sync::Mutex::new(Vec::<String>::new());
        let factory = as_search(
            |file: &BucketActiveFile,
             queries: &[&[f32]],
             metric: VectorSearchMetric,
             exact_limit: usize,
             is_excluded: &(dyn Fn(i64) -> bool + Sync)|
             -> ExactFileSearchFuture<'_> {
                calls.lock().unwrap().push(file.file_name.clone());
                // data-2 vectors: pos0 {1,0} dist 1.0, pos1 {3,0} dist 9.0
                let file_name = file.file_name.clone();
                let query = queries[0].to_vec();
                Box::pin(async move {
                    let mut reader =
                        ArrayReader::new(2, vec![Some(vec![1.0, 0.0]), Some(vec![3.0, 0.0])]);
                    Ok(vec![exact_search(
                        &file_name,
                        &mut reader,
                        &query,
                        metric,
                        exact_limit,
                        is_excluded,
                    )?])
                })
            },
        );
        let out = bucket_search(
            Some(&ann),
            &[segment],
            &[active("data-1", 2), active("data-2", 2)],
            &HashMap::new(),
            &factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            2,
            2,
            &HashMap::new(),
            false,
            None,
            1,
            None,
        )
        .await
        .unwrap();
        let mut results = out.indexed.clone();
        results.extend(out.exact.clone());
        results.sort_by(best_first);
        results.truncate(2);
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
        assert_eq!(calls.lock().unwrap().as_slice(), &["data-2".to_string()]);
    }

    #[tokio::test]
    async fn test_exact_fallback_merges_files_and_applies_deletion_vectors() {
        // No ANN. data-1 pos0 {0,0} deleted; remaining candidates merge across files.
        let calls = std::sync::Mutex::new(0usize);
        let factory = as_search(
            |file: &BucketActiveFile,
             queries: &[&[f32]],
             metric: VectorSearchMetric,
             exact_limit: usize,
             is_excluded: &(dyn Fn(i64) -> bool + Sync)|
             -> ExactFileSearchFuture<'_> {
                *calls.lock().unwrap() += 1;
                let vectors = match file.file_name.as_str() {
                    "data-1" => vec![Some(vec![0.0, 0.0]), Some(vec![2.0, 0.0])],
                    "data-2" => vec![Some(vec![1.0, 0.0]), None],
                    _ => unreachable!(),
                };
                let file_name = file.file_name.clone();
                let query = queries[0].to_vec();
                Box::pin(async move {
                    let mut reader = ArrayReader::new(2, vectors);
                    Ok(vec![exact_search(
                        &file_name,
                        &mut reader,
                        &query,
                        metric,
                        exact_limit,
                        is_excluded,
                    )?])
                })
            },
        );
        let mut dvs: HashMap<String, Arc<DeletionVector>> = HashMap::new();
        let mut bm = RoaringBitmap::new();
        bm.insert(0); // data-1 position 0 deleted
        dvs.insert("data-1".into(), Arc::new(DeletionVector::from_bitmap(bm)));

        let out = bucket_search(
            None,
            &[],
            &[active("data-1", 2), active("data-2", 2)],
            &dvs,
            &factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            2,
            2,
            &HashMap::new(),
            false,
            None,
            1,
            None,
        )
        .await
        .unwrap();
        // Candidates: data-2 pos0 {1,0} dist 1.0; data-1 pos1 {2,0} dist 4.0.
        // (data-1 pos0 deleted, data-2 pos1 null.)
        let mut results = out.indexed.clone();
        results.extend(out.exact.clone());
        results.sort_by(best_first);
        results.truncate(2);
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

    #[tokio::test]
    async fn test_rejects_duplicate_active_file_name() {
        let factory = unreachable_search();
        let err = bucket_search(
            None,
            &[],
            &[active("dup", 1), active("dup", 1)],
            &HashMap::new(),
            &factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            1,
            1,
            &HashMap::new(),
            false,
            None,
            1,
            None,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("duplicate") || err.to_string().contains("Duplicate"));
    }

    #[tokio::test]
    async fn test_rejects_ann_source_row_count_mismatch_for_active_file() {
        let ann = FakeAnnSearcher { result: vec![] };
        // Segment references data-1 with 2 rows, but the active file has 3 rows.
        // An active source with a mismatched row count is still a hard error.
        let segment = BucketAnnSegment::for_test(meta(&[("data-1", 2)]));
        let factory = unreachable_search();
        let err = bucket_search(
            Some(&ann),
            &[segment],
            &[active("data-1", 3)],
            &HashMap::new(),
            &factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            1,
            1,
            &HashMap::new(),
            false,
            None,
            1,
            None,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains("does not match") || err.to_string().contains("ANN source")
        );
    }

    #[tokio::test]
    async fn test_skips_inactive_ann_source_and_searches_active_ones() {
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
        let calls = std::sync::Mutex::new(Vec::<String>::new());
        let factory = as_search(
            |file: &BucketActiveFile,
             _: &[&[f32]],
             _: VectorSearchMetric,
             _: usize,
             _: &(dyn Fn(i64) -> bool + Sync)|
             -> ExactFileSearchFuture<'_> {
                calls.lock().unwrap().push(file.file_name.clone());
                Box::pin(async { unreachable!("only data-1 is active and it is ANN-covered") })
            },
        );
        let out = bucket_search(
            Some(&ann),
            &[segment],
            &[active("data-1", 2)],
            &HashMap::new(),
            &factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            2,
            2,
            &HashMap::new(),
            false,
            None,
            1,
            None,
        )
        .await
        .unwrap();
        let mut results = out.indexed.clone();
        results.extend(out.exact.clone());
        results.sort_by(best_first);
        results.truncate(2);
        assert_eq!(
            results,
            vec![PkVectorSearchResult {
                data_file_name: "data-1".into(),
                row_position: 0,
                distance: 0.5
            }]
        );
        // No exact fallback ran: data-1 is ANN-covered, data-2 is not active.
        assert!(calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_rejects_segments_without_ann_searcher() {
        let segment = BucketAnnSegment::for_test(meta(&[("data-1", 2)]));
        let factory = unreachable_search();
        let err = bucket_search(
            None,
            &[segment],
            &[active("data-1", 2)],
            &HashMap::new(),
            &factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            1,
            1,
            &HashMap::new(),
            false,
            None,
            1,
            None,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains("ANN search is not configured")
                || err.to_string().contains("not configured")
        );
    }

    #[tokio::test]
    async fn test_skip_exact_fallback_does_not_call_factory() {
        // No ANN segments, two active files. With skip_exact_fallback = true the
        // factory must never be called and the result is empty.
        let factory = unreachable_search();
        let out = bucket_search(
            None,
            &[],
            &[active("data-1", 2), active("data-2", 2)],
            &HashMap::new(),
            &factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            2,
            2,
            &HashMap::new(),
            true, // skip_exact_fallback
            None,
            1,
            None,
        )
        .await
        .unwrap();
        assert!(out.indexed.is_empty());
        assert!(out.exact.is_empty());
    }

    #[tokio::test]
    async fn test_rejects_duplicate_ann_segment_path() {
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
        let factory = unreachable_search();
        let err = bucket_search(
            Some(&ann),
            &[seg1, seg2],
            &[active("data-1", 2), active("data-2", 2)],
            &HashMap::new(),
            &factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            1,
            1,
            &HashMap::new(),
            false,
            None,
            1,
            None,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains("duplicate-path")
                && err.to_string().contains("appears more than once")
        );
    }

    #[tokio::test]
    async fn test_rejects_source_file_covered_by_multiple_segments() {
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
        let factory = unreachable_search();
        let err = bucket_search(
            Some(&ann),
            &[seg1, seg2],
            &[active("data-1", 2), active("data-2", 2)],
            &HashMap::new(),
            &factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            1,
            1,
            &HashMap::new(),
            false,
            None,
            1,
            None,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains("data-1")
                && err.to_string().contains("covered by both")
                && err.to_string().contains("segment-1")
                && err.to_string().contains("segment-2")
        );
    }

    #[tokio::test]
    async fn test_negative_active_row_count_rejected() {
        let factory = unreachable_search();
        let err = bucket_search(
            None,
            &[],
            &[active("data-1", -1)],
            &HashMap::new(),
            &factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            1,
            1,
            &HashMap::new(),
            false,
            None,
            1,
            None,
        )
        .await
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

    fn treemap(positions: &[u64]) -> roaring::RoaringTreemap {
        let mut t = roaring::RoaringTreemap::new();
        for &p in positions {
            t.insert(p);
        }
        t
    }

    #[tokio::test]
    async fn test_exact_residual_allow_list_restricts_positions() {
        // No ANN. data-1 has 3 rows: pos0 {1,0} dist 1.0, pos1 {2,0} dist 4.0,
        // pos2 {3,0} dist 9.0. residual allows only {0, 2} -> pos1 excluded even
        // though it is not deletion-vector deleted.
        let factory = array_search(vec![
            Some(vec![1.0, 0.0]),
            Some(vec![2.0, 0.0]),
            Some(vec![3.0, 0.0]),
        ]);
        let mut residual: HashMap<String, roaring::RoaringTreemap> = HashMap::new();
        residual.insert("data-1".into(), treemap(&[0, 2]));
        let out = bucket_search(
            None,
            &[],
            &[active("data-1", 3)],
            &HashMap::new(),
            &factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            5,
            5,
            &HashMap::new(),
            false,
            Some(&residual),
            1,
            None,
        )
        .await
        .unwrap();
        let mut results = out.indexed.clone();
        results.extend(out.exact.clone());
        results.sort_by(best_first);
        results.truncate(5);
        assert_eq!(
            results,
            vec![
                PkVectorSearchResult {
                    data_file_name: "data-1".into(),
                    row_position: 0,
                    distance: 1.0
                },
                PkVectorSearchResult {
                    data_file_name: "data-1".into(),
                    row_position: 2,
                    distance: 9.0
                },
            ]
        );
    }

    #[tokio::test]
    async fn test_exact_residual_file_absent_from_map_is_skipped_without_reading() {
        // residual covers only data-1; data-2 has no entry -> no allowed rows, so
        // data-2 is skipped entirely (its factory reader is never built).
        let calls = std::sync::Mutex::new(Vec::<String>::new());
        let factory = as_search(
            |file: &BucketActiveFile,
             queries: &[&[f32]],
             metric: VectorSearchMetric,
             exact_limit: usize,
             is_excluded: &(dyn Fn(i64) -> bool + Sync)|
             -> ExactFileSearchFuture<'_> {
                calls.lock().unwrap().push(file.file_name.clone());
                let file_name = file.file_name.clone();
                let query = queries[0].to_vec();
                Box::pin(async move {
                    let mut reader =
                        ArrayReader::new(2, vec![Some(vec![1.0, 0.0]), Some(vec![2.0, 0.0])]);
                    Ok(vec![exact_search(
                        &file_name,
                        &mut reader,
                        &query,
                        metric,
                        exact_limit,
                        is_excluded,
                    )?])
                })
            },
        );
        let mut residual: HashMap<String, roaring::RoaringTreemap> = HashMap::new();
        residual.insert("data-1".into(), treemap(&[0, 1]));
        let out = bucket_search(
            None,
            &[],
            &[active("data-1", 2), active("data-2", 2)],
            &HashMap::new(),
            &factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            5,
            5,
            &HashMap::new(),
            false,
            Some(&residual),
            1,
            None,
        )
        .await
        .unwrap();
        let mut results = out.indexed.clone();
        results.extend(out.exact.clone());
        results.sort_by(best_first);
        results.truncate(5);
        // Only data-1 rows appear; data-2 was never read.
        assert!(results.iter().all(|r| r.data_file_name == "data-1"));
        assert_eq!(calls.lock().unwrap().as_slice(), &["data-1".to_string()]);
    }

    #[tokio::test]
    async fn test_exact_residual_empty_set_file_is_skipped_without_reading() {
        // data-1 has an entry but it is empty -> no allowed rows, skipped without
        // reading. Mirrors a file with no residual matches.
        let calls = std::sync::Mutex::new(0usize);
        let factory = as_search(
            |_: &BucketActiveFile,
             _: &[&[f32]],
             _: VectorSearchMetric,
             _: usize,
             _: &(dyn Fn(i64) -> bool + Sync)|
             -> ExactFileSearchFuture<'_> {
                *calls.lock().unwrap() += 1;
                Box::pin(async {
                    unreachable!("data-1 has an empty allow set and must not be read")
                })
            },
        );
        let mut residual: HashMap<String, roaring::RoaringTreemap> = HashMap::new();
        residual.insert("data-1".into(), treemap(&[]));
        let out = bucket_search(
            None,
            &[],
            &[active("data-1", 3)],
            &HashMap::new(),
            &factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            5,
            5,
            &HashMap::new(),
            false,
            Some(&residual),
            1,
            None,
        )
        .await
        .unwrap();
        assert!(out.indexed.is_empty());
        assert!(out.exact.is_empty());
        assert_eq!(*calls.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn test_exact_residual_intersects_with_deletion_vector() {
        // residual allows {0, 1, 2} but the deletion vector deletes pos0; the
        // surviving candidates are the residual-allowed AND not-deleted rows.
        let factory = array_search(vec![
            Some(vec![1.0, 0.0]),
            Some(vec![2.0, 0.0]),
            Some(vec![3.0, 0.0]),
        ]);
        let mut dvs: HashMap<String, Arc<DeletionVector>> = HashMap::new();
        let mut bm = RoaringBitmap::new();
        bm.insert(0); // pos0 deleted
        dvs.insert("data-1".into(), Arc::new(DeletionVector::from_bitmap(bm)));
        let mut residual: HashMap<String, roaring::RoaringTreemap> = HashMap::new();
        residual.insert("data-1".into(), treemap(&[0, 1, 2]));
        let out = bucket_search(
            None,
            &[],
            &[active("data-1", 3)],
            &dvs,
            &factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            5,
            5,
            &HashMap::new(),
            false,
            Some(&residual),
            1,
            None,
        )
        .await
        .unwrap();
        let mut results = out.indexed.clone();
        results.extend(out.exact.clone());
        results.sort_by(best_first);
        results.truncate(5);
        assert_eq!(
            results.iter().map(|r| r.row_position).collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[tokio::test]
    async fn test_non_finite_query_fails_before_opening_any_file() {
        // A non-finite query element must fail loud before the exact-file search
        // closure is ever invoked (validate-before-OPEN). The recording closure
        // flips a flag if called; the flag must stay false.
        let called = std::sync::atomic::AtomicBool::new(false);
        let factory = as_search(
            |_: &BucketActiveFile,
             _: &[&[f32]],
             _: VectorSearchMetric,
             _: usize,
             _: &(dyn Fn(i64) -> bool + Sync)|
             -> ExactFileSearchFuture<'_> {
                called.store(true, std::sync::atomic::Ordering::SeqCst);
                Box::pin(async { unreachable!("closure must not run for a malformed query") })
            },
        );
        let err = bucket_search(
            None,
            &[],
            &[active("data-1", 2)],
            &HashMap::new(),
            &factory,
            &[f32::NAN, 0.0],
            VectorSearchMetric::L2,
            2,
            2,
            &HashMap::new(),
            false,
            None,
            1,
            None,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("finite"), "got: {err}");
        assert!(
            !called.load(std::sync::atomic::Ordering::SeqCst),
            "the exact-file search closure must not be invoked for a malformed query"
        );
    }

    /// Build a per-file exact-search closure that runs the reference `exact_search`
    /// over an in-memory `ArrayReader` of `vectors`, looping ALL queries into one
    /// list per query (the outer `Vec` is aligned to `queries`). Used by the
    /// multi-query `bucket_search_batch` tests.
    #[allow(clippy::type_complexity)]
    fn array_search_batch(
        vectors: Vec<Option<Vec<f32>>>,
    ) -> impl for<'a> Fn(
        &'a BucketActiveFile,
        &'a [&'a [f32]],
        VectorSearchMetric,
        usize,
        &'a (dyn Fn(i64) -> bool + Sync),
    ) -> ExactFileSearchFuture<'a>
           + Send
           + Sync {
        let vectors = Arc::new(vectors);
        as_search(
            move |file: &BucketActiveFile,
                  queries: &[&[f32]],
                  metric: VectorSearchMetric,
                  exact_limit: usize,
                  is_excluded: &(dyn Fn(i64) -> bool + Sync)|
                  -> ExactFileSearchFuture<'_> {
                let dimension = vectors
                    .first()
                    .and_then(|v| v.as_ref())
                    .map_or(0, |v| v.len());
                let file_name = file.file_name.clone();
                let owned_queries: Vec<Vec<f32>> = queries.iter().map(|q| q.to_vec()).collect();
                let vectors = Arc::clone(&vectors);
                Box::pin(async move {
                    let mut out = Vec::with_capacity(owned_queries.len());
                    for query in &owned_queries {
                        let mut reader = ArrayReader::new(dimension, (*vectors).clone());
                        out.push(exact_search(
                            &file_name,
                            &mut reader,
                            query,
                            metric,
                            exact_limit,
                            is_excluded,
                        )?);
                    }
                    Ok(out)
                })
            },
        )
    }

    #[tokio::test]
    async fn test_bucket_search_batch_of_one_equals_single_query() {
        // A batch-of-one must equal the single-query `bucket_search` exactly.
        let segment = BucketAnnSegment::for_test(meta(&[("ann.mosaic", 3)]));
        let ann = FakeAnnSearcher {
            result: vec![
                PkVectorSearchResult {
                    data_file_name: "ann.mosaic".into(),
                    row_position: 0,
                    distance: 0.1,
                },
                PkVectorSearchResult {
                    data_file_name: "ann.mosaic".into(),
                    row_position: 1,
                    distance: 0.2,
                },
            ],
        };
        let active_files = vec![active("ann.mosaic", 3), active("exact.mosaic", 3)];
        let dvs: HashMap<String, Arc<DeletionVector>> = HashMap::new();
        let opts = HashMap::new();
        let query = [1.0f32, 0.0];

        let single = {
            let factory = array_search(vec![
                Some(vec![1.0, 0.0]),
                Some(vec![9.0, 0.0]),
                Some(vec![8.0, 0.0]),
            ]);
            bucket_search(
                Some(&ann),
                &[BucketAnnSegment::for_test(meta(&[("ann.mosaic", 3)]))],
                &active_files,
                &dvs,
                &factory,
                &query,
                VectorSearchMetric::L2,
                3,
                2,
                &opts,
                false,
                None,
                1,
                None,
            )
            .await
            .unwrap()
        };

        let factory = array_search_batch(vec![
            Some(vec![1.0, 0.0]),
            Some(vec![9.0, 0.0]),
            Some(vec![8.0, 0.0]),
        ]);
        let query_ref: &[f32] = &query;
        let batch = bucket_search_batch(
            Some(&ann),
            &[segment],
            &active_files,
            &dvs,
            &factory,
            &[query_ref],
            VectorSearchMetric::L2,
            3,
            2,
            &opts,
            false,
            None,
            1,
            None,
        )
        .await
        .unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].indexed, single.indexed);
        assert_eq!(batch[0].exact, single.exact);
    }

    #[tokio::test]
    async fn test_bucket_search_batch_per_query_heaps_independent() {
        // Two queries over one exact file with a shared residual/DV; each query's
        // Top-K comes from its own heap (no cross-query bleed). Query 0 is nearest
        // pos 0; query 1 is nearest pos 3.
        let factory = array_search_batch(vec![
            Some(vec![0.0, 0.0]),
            Some(vec![1.0, 0.0]),
            Some(vec![2.0, 0.0]),
            Some(vec![3.0, 0.0]),
        ]);
        let q0: &[f32] = &[0.0, 0.0];
        let q1: &[f32] = &[3.0, 0.0];
        let out = bucket_search_batch(
            None,
            &[],
            &[active("data-1", 4)],
            &HashMap::new(),
            &factory,
            &[q0, q1],
            VectorSearchMetric::L2,
            1,
            1,
            &HashMap::new(),
            false,
            None,
            1,
            None,
        )
        .await
        .unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].exact.len(), 1);
        assert_eq!(out[0].exact[0].row_position, 0);
        assert_eq!(out[1].exact.len(), 1);
        assert_eq!(out[1].exact[0].row_position, 3);
    }

    #[tokio::test]
    async fn test_bucket_search_batch_shares_one_read_per_file_across_queries() {
        // The exact-file closure is invoked ONCE per uncovered file regardless of
        // the query count (the shared stream scores all queries).
        let calls = std::sync::Mutex::new(Vec::<String>::new());
        let factory = as_search(
            |file: &BucketActiveFile,
             queries: &[&[f32]],
             metric: VectorSearchMetric,
             exact_limit: usize,
             is_excluded: &(dyn Fn(i64) -> bool + Sync)|
             -> ExactFileSearchFuture<'_> {
                calls.lock().unwrap().push(file.file_name.clone());
                let file_name = file.file_name.clone();
                let owned: Vec<Vec<f32>> = queries.iter().map(|q| q.to_vec()).collect();
                Box::pin(async move {
                    let mut out = Vec::with_capacity(owned.len());
                    for query in &owned {
                        let mut reader =
                            ArrayReader::new(2, vec![Some(vec![1.0, 0.0]), Some(vec![3.0, 0.0])]);
                        out.push(exact_search(
                            &file_name,
                            &mut reader,
                            query,
                            metric,
                            exact_limit,
                            is_excluded,
                        )?);
                    }
                    Ok(out)
                })
            },
        );
        let q0: &[f32] = &[0.0, 0.0];
        let q1: &[f32] = &[4.0, 0.0];
        let out = bucket_search_batch(
            None,
            &[],
            &[active("data-1", 2)],
            &HashMap::new(),
            &factory,
            &[q0, q1],
            VectorSearchMetric::L2,
            2,
            2,
            &HashMap::new(),
            false,
            None,
            1,
            None,
        )
        .await
        .unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(
            calls.lock().unwrap().as_slice(),
            &["data-1".to_string()],
            "one shared read per file for the whole batch"
        );
    }

    #[tokio::test]
    async fn test_bucket_search_batch_malformed_query_fails_before_opening_any_file() {
        // A non-finite element in ANY query fails loud before the exact-file search
        // closure is ever invoked (validate-before-OPEN).
        let called = std::sync::atomic::AtomicBool::new(false);
        let factory = as_search(
            |_: &BucketActiveFile,
             _: &[&[f32]],
             _: VectorSearchMetric,
             _: usize,
             _: &(dyn Fn(i64) -> bool + Sync)|
             -> ExactFileSearchFuture<'_> {
                called.store(true, std::sync::atomic::Ordering::SeqCst);
                Box::pin(async { unreachable!("closure must not run for a malformed query") })
            },
        );
        let good: &[f32] = &[0.0, 0.0];
        let bad: &[f32] = &[f32::NAN, 0.0];
        let err = bucket_search_batch(
            None,
            &[],
            &[active("data-1", 2)],
            &HashMap::new(),
            &factory,
            &[good, bad],
            VectorSearchMetric::L2,
            2,
            2,
            &HashMap::new(),
            false,
            None,
            1,
            None,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("finite"), "got: {err}");
        assert!(
            !called.load(std::sync::atomic::Ordering::SeqCst),
            "the exact-file search closure must not be invoked for a malformed batch"
        );
    }

    #[tokio::test]
    async fn test_exact_file_search_order_is_sequential_at_concurrency_one() {
        // At concurrency == 1 the per-exact-file loop must visit uncovered files in
        // strict active-file (submission) order. A recording closure captures the
        // visit order; it must match the active-file order deterministically.
        let calls = std::sync::Mutex::new(Vec::<String>::new());
        let factory = as_search(
            |file: &BucketActiveFile,
             queries: &[&[f32]],
             metric: VectorSearchMetric,
             exact_limit: usize,
             is_excluded: &(dyn Fn(i64) -> bool + Sync)|
             -> ExactFileSearchFuture<'_> {
                calls.lock().unwrap().push(file.file_name.clone());
                let file_name = file.file_name.clone();
                let query = queries[0].to_vec();
                Box::pin(async move {
                    let mut reader = ArrayReader::new(2, vec![Some(vec![1.0, 0.0])]);
                    Ok(vec![exact_search(
                        &file_name,
                        &mut reader,
                        &query,
                        metric,
                        exact_limit,
                        is_excluded,
                    )?])
                })
            },
        );
        let out = bucket_search(
            None,
            &[],
            &[
                active("data-a", 1),
                active("data-b", 1),
                active("data-c", 1),
                active("data-d", 1),
            ],
            &HashMap::new(),
            &factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            8,
            8,
            &HashMap::new(),
            false,
            None,
            1,
            None,
        )
        .await
        .unwrap();
        assert_eq!(out.exact.len(), 4);
        assert_eq!(
            calls.lock().unwrap().as_slice(),
            &[
                "data-a".to_string(),
                "data-b".to_string(),
                "data-c".to_string(),
                "data-d".to_string(),
            ],
            "concurrency == 1 must visit files in strict active-file order"
        );
    }

    #[tokio::test]
    async fn test_parallel_exact_files_tie_and_nan_rank_deterministically_out_of_order() {
        // Two uncovered files whose candidates share a distance (a tie decided by
        // the BEST_FIRST file/position tie-break) plus a NaN-distance candidate that
        // must sort LAST. The futures complete OUT OF ORDER: the alphabetically
        // first file (data-a) yields more times before returning, so under
        // buffer_unordered(concurrency > 1) data-b completes first. The bounded-heap
        // merge is order-independent, so the final best-first ranking must still be
        // the deterministic BEST_FIRST order (identical to a serial run).
        let negative_nan = f32::from_bits(0xffc00000);
        assert!(negative_nan.is_nan());
        let factory = as_search(
            move |file: &BucketActiveFile,
                  queries: &[&[f32]],
                  _metric: VectorSearchMetric,
                  exact_limit: usize,
                  _is_excluded: &(dyn Fn(i64) -> bool + Sync)|
                  -> ExactFileSearchFuture<'_> {
                let name = file.file_name.clone();
                // data-a is submitted first but yields more, so it finishes last.
                let yields = if name == "data-a" { 8 } else { 0 };
                let _ = (queries, exact_limit);
                Box::pin(async move {
                    for _ in 0..yields {
                        tokio::task::yield_now().await;
                    }
                    // Each file returns two candidates: one tied distance 1.0 and one
                    // NaN distance that must never outrank a finite candidate.
                    let results = vec![
                        PkVectorSearchResult {
                            data_file_name: name.clone(),
                            row_position: 0,
                            distance: 1.0,
                        },
                        PkVectorSearchResult {
                            data_file_name: name.clone(),
                            row_position: 1,
                            distance: negative_nan,
                        },
                    ];
                    Ok(vec![results])
                })
            },
        );

        let run = |concurrency: usize| {
            let factory = &factory;
            async move {
                let out = bucket_search(
                    None,
                    &[],
                    &[active("data-a", 2), active("data-b", 2)],
                    &HashMap::new(),
                    factory,
                    &[0.0, 0.0],
                    VectorSearchMetric::L2,
                    8,
                    8,
                    &HashMap::new(),
                    false,
                    None,
                    concurrency,
                    None,
                )
                .await
                .unwrap();
                out.exact
                    .iter()
                    .map(|r| {
                        (
                            r.data_file_name.clone(),
                            r.row_position,
                            r.distance.is_nan(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
        };

        let serial = run(1).await;
        let parallel = run(4).await;
        // Serial == parallel, and the deterministic BEST_FIRST order is: tied
        // distance-1.0 candidates first (data-a before data-b by file name), then the
        // NaN-distance candidates last (again data-a before data-b).
        assert_eq!(
            serial,
            vec![
                ("data-a".to_string(), 0, false),
                ("data-b".to_string(), 0, false),
                ("data-a".to_string(), 1, true),
                ("data-b".to_string(), 1, true),
            ]
        );
        assert_eq!(
            parallel, serial,
            "parallel ranking must equal serial ranking"
        );
    }
}
