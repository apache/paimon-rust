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
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::ann::PkVectorAnnSearcher;
use super::data_invalid;
use super::metric::{java_float_compare, VectorSearchMetric};
use super::result::PkVectorSearchResult;
use crate::deletion_vector::DeletionVector;
use crate::spec::PrimaryKeyIndexSourceMeta as PkVectorSourceMeta;
use crate::vindex::executor::{
    acquire_process_global_search_permit, drain_indexed_jobs, execute_global_index,
};

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
///
/// `Clone` so one segment's search inputs can be moved into a dedicated global-index
/// executor leaf (the ANN CPU search runs off the async worker). Cloning copies only
/// the segment's addressing metadata (path, size, `index_meta`, source meta). Each
/// leaf opens its own segment source lazily and drops it after scoring; vindex
/// sources stay range-backed instead of holding the full index bytes.
#[derive(Clone)]
pub(crate) struct BucketAnnSegment {
    pub source_meta: PkVectorSourceMeta,
    /// Resolved index-file path (globally unique; the key the loader reads by).
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

/// Concurrency budget for one search's global-index leaf work. Mirrors Java's
/// two-level shape: a shared process pool with an available-CPU floor, plus a
/// per-query limit (`per_query`) that keeps a single query from occupying more
/// than its configured `thread_num` slots.
///
/// - `production(n)` — the real read path: per-query semaphore of `n` permits AND
///   the shared process pool. Used even when `n <= 1`; concurrent queries may
///   share the CPU-sized process pool, matching Java's cached executor plus its
///   per-caller `SemaphoredDelegatingExecutor`.
/// - `per_query_only(sem)` / `shared_for_test(sem)` — tests only: an explicit
///   per-query semaphore with NO process-global gating, so a shared static cannot
///   pollute a cap assertion.
///
/// A `None` budget (passed only by low-level tests that opt out of gating) runs the
/// leaf ungated; production never passes `None`.
#[derive(Clone)]
pub(crate) struct SearchBudget {
    /// Per-query permit source: caps ONE query's in-flight leaves at `thread_num`.
    per_query: Option<Arc<Semaphore>>,
    /// When set, also draw from the process-global semaphore. Its effective size
    /// is at least the available CPU count and grows with larger requested values,
    /// matching Java's shared cached executor. `None` skips global gating (tests).
    process_global_capacity: Option<usize>,
}

impl SearchBudget {
    /// The production budget: per-query cap of `concurrency` plus the shared
    /// process pool. Applied even when `concurrency <= 1`.
    pub(crate) fn production(concurrency: usize) -> Self {
        Self {
            per_query: Some(Arc::new(Semaphore::new(concurrency.max(1)))),
            process_global_capacity: Some(concurrency.max(1)),
        }
    }

    /// A per-query-only budget for tests that assert a single query's cap in
    /// isolation. No process-global gating, so a static initialized by another test
    /// cannot change the observed peak.
    #[cfg(test)]
    pub(crate) fn per_query_only(permits: usize) -> Self {
        Self {
            per_query: Some(Arc::new(Semaphore::new(permits))),
            process_global_capacity: None,
        }
    }

    /// A budget backed by an explicit, caller-provided semaphore, with NO
    /// process-global gating. Two concurrent searches given the SAME `Arc<Semaphore>`
    /// share one cap deterministically — the "deliberately shared" test seam that
    /// exercises cross-query capping (what the process-global static does in
    /// production) without touching the shared static (so tests stay isolated).
    #[cfg(test)]
    pub(crate) fn shared_for_test(semaphore: Arc<Semaphore>) -> Self {
        Self {
            per_query: Some(semaphore),
            process_global_capacity: None,
        }
    }

    /// Acquire one slot: per-query permit FIRST, then the process-global permit.
    /// This ordering prevents a job from holding a scarce global permit while it
    /// merely waits for its own query-local permit. Both guards are returned and
    /// must be kept alive for the duration of the leaf work (an ANN segment's
    /// blocking CPU search or an exact file's async I/O).
    async fn acquire(&self) -> crate::Result<SearchPermit> {
        let per_query = match &self.per_query {
            Some(sem) => Some(acquire_owned(sem.clone()).await?),
            None => None,
        };
        let process_global = match self.process_global_capacity {
            Some(cap) => Some(acquire_process_global_search_permit(cap).await?),
            None => None,
        };
        Ok(SearchPermit {
            _per_query: per_query,
            _process_global: process_global,
        })
    }
}

/// Guard holding the acquired permits (per-query and/or process-global) until it is
/// dropped. Held for the whole leaf, including inside the dedicated executor task
/// (whose work continues if the awaiting future is dropped), so the budget stays
/// accurate.
struct SearchPermit {
    _per_query: Option<OwnedSemaphorePermit>,
    _process_global: Option<tokio::sync::SemaphorePermit<'static>>,
}

async fn acquire_owned(sem: Arc<Semaphore>) -> crate::Result<OwnedSemaphorePermit> {
    sem.acquire_owned()
        .await
        .map_err(|e| crate::Error::UnexpectedError {
            message: "global-index search concurrency budget was closed".to_string(),
            source: Some(Box::new(e)),
        })
}

/// Acquire one slot from a search's concurrency budget, if one is set. The returned
/// guard holds the slot(s) until dropped, so the caller must keep it alive for the
/// duration of the leaf work it gates. A `None` budget (passed only by low-level
/// tests) runs the leaf ungated; production always supplies a `SearchBudget`,
/// including on the `concurrency <= 1` sequential path.
async fn acquire_search_permit(
    budget: &Option<SearchBudget>,
) -> crate::Result<Option<SearchPermit>> {
    match budget {
        Some(budget) => Ok(Some(budget.acquire().await?)),
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

/// One completed leaf of the merged per-bucket search: either an ANN segment's
/// per-query hit lists (fold into the INDEXED heaps) or an uncovered exact file's
/// per-query hit lists (fold into the EXACT heaps). ANN and exact leaves run in one
/// combined concurrency window under the shared budget (mirroring Java's single
/// `allOf` over ANN + exact futures) rather than ANN-fully-then-exact; the tag says
/// which heap set each result feeds. Both carry one list per query (the single-query
/// path uses a 1-element outer vec).
enum BucketLeaf {
    Indexed(Vec<Vec<PkVectorSearchResult>>),
    Exact(Vec<Vec<PkVectorSearchResult>>),
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
    ann_searcher: Option<Arc<dyn PkVectorAnnSearcher>>,
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
    search_budget: Option<SearchBudget>,
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

    // ANN segment search. Each segment's scorer is synchronous CPU work (the
    // faiss-like vindex search, which may itself use Rayon), so running it inline
    // would monopolize the async worker and serialize every segment across the
    // whole read. Instead each segment is a dedicated-executor leaf gated by the
    // shared `search_budget`, mirroring Java's single `GlobalIndexReadThreadPool`
    // where every ANN segment is a pool task. `concurrency <= 1` still goes through
    // the dedicated executor (so a long CPU search never blocks the runtime) but strictly
    // one at a time.
    //
    // Structural validation of ALL segments happens before any leaf launches, so a
    // malformed later segment fails loud without half the segments having already
    // scored. Row-count validation runs BEFORE resolving the searcher so a
    // corruption diagnostic is not masked by an "ANN search is not configured"
    // error when both are wrong.
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
    }
    let searcher = if ann_segments.is_empty() {
        None
    } else {
        Some(
            ann_searcher
                .as_ref()
                .ok_or_else(|| data_invalid("ANN search is not configured"))?
                .clone(),
        )
    };

    // Merged leaf stream: ANN segment searches and uncovered exact-file searches run
    // in ONE combined concurrency window under the shared budget — mirroring Java's
    // single `allOf` over the ANN and exact futures — instead of ANN-fully-then-exact.
    // Leaves are ordered ANN-first (segment order), then exact (active-file order), so
    // `drain_indexed_jobs` (which awaits ALL leaves and returns the lowest-ordinal
    // error) yields a deterministic error: first ANN error by segment order, else
    // first exact error by active-file order. At `concurrency <= 1` the ANN-first
    // order reproduces sequential ANN-then-exact execution. Each leaf is tagged so its
    // results fold into the correct heap; heaps are order-independent, so completion
    // order does not affect the final Top-K.
    //
    // Owned exact-query slice (one element for the single-query path) so exact leaves
    // borrow no locals across the await.
    let queries: [&[f32]; 1] = [query];

    // ANN leaves: shared owned inputs, one dedicated-executor scorer per segment.
    let ann_shared = searcher.as_ref().map(|searcher| {
        (
            searcher.clone(),
            residual_ranges.map(|r| Arc::new(r.clone())),
            Arc::new(active_source_files.clone()),
            Arc::new(deletion_vectors.clone()),
            Arc::new(search_options.clone()),
            Arc::<[f32]>::from(query.to_vec()),
        )
    });

    // Eligible uncovered exact files (active-file order) with their exclusion
    // predicate; a file with no residual-allowed rows is skipped without reading.
    #[allow(clippy::type_complexity)]
    let mut exact_tasks: Vec<(&BucketActiveFile, Box<dyn Fn(i64) -> bool + Sync>)> = Vec::new();
    if !skip_exact_fallback {
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
            exact_tasks.push((file, Box::new(position_excluder(dv, residual_allowed))));
        }
    }

    // Build the ANN-first-then-exact leaf futures.
    let mut leaves: Vec<BoxFuture<'_, crate::Result<BucketLeaf>>> = Vec::new();
    if let Some((
        searcher,
        residual_arc,
        active_source_files,
        deletion_vectors,
        search_options,
        query_owned,
    )) = ann_shared
    {
        for segment in ann_segments {
            let searcher = searcher.clone();
            let segment = segment.clone();
            let active_source_files = active_source_files.clone();
            let deletion_vectors = deletion_vectors.clone();
            let search_options = search_options.clone();
            let residual_arc = residual_arc.clone();
            let query_owned = query_owned.clone();
            let budget = search_budget.clone();
            leaves.push(Box::pin(async move {
                // One permit spans BOTH the async load and the dedicated executor
                // score. `load_segment_source` must not acquire a second permit or
                // it would deadlock at capacity 1.
                let permit = acquire_search_permit(&budget).await?;
                let segment_source = searcher.load_segment_source(&segment).await?;
                let hits = execute_global_index("ANN segment search task failed", move || {
                    let _permit = permit;
                    searcher.search_source(
                        &segment,
                        segment_source,
                        &query_owned,
                        metric,
                        indexed_limit,
                        &active_source_files,
                        &deletion_vectors,
                        &search_options,
                        residual_arc.as_deref(),
                    )
                })
                .await?;
                Ok(BucketLeaf::Indexed(vec![hits]))
            }));
        }
    }
    for (file, is_excluded) in &exact_tasks {
        let queries = &queries;
        let budget = search_budget.clone();
        leaves.push(Box::pin(async move {
            let _permit = acquire_search_permit(&budget).await?;
            let per_query =
                exact_file_search(file, queries, metric, exact_limit, is_excluded.as_ref()).await?;
            Ok(BucketLeaf::Exact(vec![single_query_result(per_query)?]))
        }));
    }

    // Drive all leaves in one window; drain-all + lowest-ordinal error.
    let outs = drain_indexed_jobs(leaves.into_iter(), concurrency).await?;
    for leaf in outs {
        match leaf {
            BucketLeaf::Indexed(per_query) => {
                for result in per_query.into_iter().next().unwrap_or_default() {
                    add_candidate(&mut indexed_heap, result, indexed_limit);
                }
            }
            BucketLeaf::Exact(per_query) => {
                for result in per_query.into_iter().next().unwrap_or_default() {
                    add_candidate(&mut exact_heap, result, exact_limit);
                }
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
    ann_searcher: Option<Arc<dyn PkVectorAnnSearcher>>,
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
    search_budget: Option<SearchBudget>,
) -> crate::Result<Vec<BucketSearchResult>> {
    if queries.is_empty() {
        return Err(data_invalid("vector search requires at least one query"));
    }
    // A batch-of-one routes to the single-query body so its tie ordering is
    // byte-identical to the pre-batch path (the multi-query heaps could in
    // principle differ).
    if queries.len() == 1 {
        let single = bucket_search(
            ann_searcher.clone(),
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

    // ANN segment search (multi-query). As in the single-query path, each segment's
    // synchronous scorer runs as a dedicated-executor leaf gated by the shared
    // `search_budget` so segments across all buckets share one budget and never
    // monopolize an async worker. All structural validation runs before any leaf
    // launches. One shared reader per segment searches every query; the per-query
    // hits fan into their own bounded heaps after the parallel stage.
    // Row-count validation runs BEFORE resolving the searcher so a corruption
    // diagnostic is not masked by an "ANN search is not configured" error when both
    // are wrong (mirrors the single-query path and Java's up-front checks).
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
    }
    let searcher = if ann_segments.is_empty() {
        None
    } else {
        Some(
            ann_searcher
                .as_ref()
                .ok_or_else(|| data_invalid("ANN search is not configured"))?
                .clone(),
        )
    };

    // Merged leaf stream (see the single-query `bucket_search` for the rationale):
    // ANN segment searches and uncovered exact-file searches run in ONE combined
    // concurrency window under the shared budget, ANN-first then exact, drained
    // together with a deterministic lowest-ordinal error. Each leaf carries one hit
    // list per query; the fan-in zips per-query results into the per-query heaps.
    let query_count = queries.len();
    let ann_shared = searcher.as_ref().map(|searcher| {
        let queries_owned: Arc<Vec<Vec<f32>>> =
            Arc::new(queries.iter().map(|q| q.to_vec()).collect());
        (
            searcher.clone(),
            residual_ranges.map(|r| Arc::new(r.clone())),
            Arc::new(active_source_files.clone()),
            Arc::new(deletion_vectors.clone()),
            Arc::new(search_options.clone()),
            queries_owned,
        )
    });

    #[allow(clippy::type_complexity)]
    let mut exact_tasks: Vec<(&BucketActiveFile, Box<dyn Fn(i64) -> bool + Sync>)> = Vec::new();
    if !skip_exact_fallback {
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
            exact_tasks.push((file, Box::new(position_excluder(dv, residual_allowed))));
        }
    }

    let mut leaves: Vec<BoxFuture<'_, crate::Result<BucketLeaf>>> = Vec::new();
    if let Some((
        searcher,
        residual_arc,
        active_source_files,
        deletion_vectors,
        search_options,
        queries_owned,
    )) = ann_shared
    {
        for segment in ann_segments {
            let searcher = searcher.clone();
            let segment = segment.clone();
            let active_source_files = active_source_files.clone();
            let deletion_vectors = deletion_vectors.clone();
            let search_options = search_options.clone();
            let residual_arc = residual_arc.clone();
            let queries_owned = queries_owned.clone();
            let budget = search_budget.clone();
            leaves.push(Box::pin(async move {
                let permit = acquire_search_permit(&budget).await?;
                let segment_source = searcher.load_segment_source(&segment).await?;
                let per_query = execute_global_index("ANN segment search task failed", move || {
                    let _permit = permit;
                    let query_refs: Vec<&[f32]> =
                        queries_owned.iter().map(|q| q.as_slice()).collect();
                    let per_query = searcher.search_batch_source(
                        &segment,
                        segment_source,
                        &query_refs,
                        metric,
                        indexed_limit,
                        &active_source_files,
                        &deletion_vectors,
                        &search_options,
                        residual_arc.as_deref(),
                    )?;
                    if per_query.len() != query_count {
                        return Err(data_invalid(format!(
                            "ANN batch search returned {} result lists for {} queries",
                            per_query.len(),
                            query_count
                        )));
                    }
                    Ok(per_query)
                })
                .await?;
                Ok(BucketLeaf::Indexed(per_query))
            }));
        }
    }
    for (file, is_excluded) in &exact_tasks {
        let budget = search_budget.clone();
        leaves.push(Box::pin(async move {
            let _permit = acquire_search_permit(&budget).await?;
            let per_query =
                exact_file_search(file, queries, metric, exact_limit, is_excluded.as_ref()).await?;
            Ok(BucketLeaf::Exact(validate_per_query_len(
                per_query,
                query_count,
            )?))
        }));
    }

    let outs = drain_indexed_jobs(leaves.into_iter(), concurrency).await?;
    for leaf in outs {
        match leaf {
            BucketLeaf::Indexed(per_query) => {
                for (results, heap) in per_query.into_iter().zip(indexed_heaps.iter_mut()) {
                    for result in results {
                        add_candidate(heap, result, indexed_limit);
                    }
                }
            }
            BucketLeaf::Exact(per_query) => {
                for (results, heap) in per_query.into_iter().zip(exact_heaps.iter_mut()) {
                    for result in results {
                        add_candidate(heap, result, exact_limit);
                    }
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
    use crate::spec::PrimaryKeyIndexSourceFile as PkVectorSourceFile;
    use crate::vindex::executor::ensure_global_index_executor_capacity;
    use crate::vindex::pkvector::ann::PkVectorAnnSearcher;
    use crate::vindex::pkvector::exact::exact_search;
    use crate::vindex::pkvector::reader::test_support::ArrayReader;
    use bytes::Bytes;
    use roaring::RoaringBitmap;

    /// Trivial ANN-segment loader for fakes: returns empty owned bytes (the fakes
    /// model behavior above physical index decoding and ignore `segment_bytes`).
    fn empty_ann_loader(
        _segment: &BucketAnnSegment,
    ) -> futures::future::BoxFuture<'static, crate::Result<Bytes>> {
        Box::pin(async { Ok(Bytes::new()) })
    }

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
    #[derive(Clone)]
    struct FakeAnnSearcher {
        result: Vec<PkVectorSearchResult>,
    }
    impl PkVectorAnnSearcher for FakeAnnSearcher {
        fn load_segment(
            &self,
            segment: &BucketAnnSegment,
        ) -> futures::future::BoxFuture<'static, crate::Result<Bytes>> {
            empty_ann_loader(segment)
        }
        fn search_batch(
            &self,
            _segment: &BucketAnnSegment,
            _segment_bytes: Bytes,
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
            Some(Arc::new(ann.clone())),
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
            Some(Arc::new(ann.clone())),
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
            Some(Arc::new(ann.clone())),
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
            Some(Arc::new(ann.clone())),
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
            Some(Arc::new(ann.clone())),
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
            Some(Arc::new(ann.clone())),
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
            Some(Arc::new(ann.clone())),
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
            Some(Arc::new(ann.clone())),
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
                Some(Arc::new(ann.clone())),
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
            Some(Arc::new(ann.clone())),
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

    const SCORER_START_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
    const SCORER_RELEASE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

    static GATED_SCORER_TEST_LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> =
        std::sync::OnceLock::new();

    async fn lock_gated_scorer_tests() -> tokio::sync::MutexGuard<'static, ()> {
        GATED_SCORER_TEST_LOCK
            .get_or_init(|| tokio::sync::Mutex::new(()))
            .lock()
            .await
    }

    type ScorerIdentity = (usize, String);
    type StartedScorer = (ScorerIdentity, std::sync::mpsc::Sender<()>);

    /// Reports a scorer's identity to the test task, then blocks until that task
    /// releases it. Both sides use timeouts so a scheduling regression fails instead
    /// of hanging the suite.
    #[derive(Clone)]
    struct ScorerGate {
        started: tokio::sync::mpsc::UnboundedSender<StartedScorer>,
    }

    impl ScorerGate {
        fn wait_for_release(&self, query_id: usize, segment: &str) -> crate::Result<()> {
            let (release, released) = std::sync::mpsc::channel();
            self.started
                .send(((query_id, segment.to_string()), release))
                .map_err(|_| data_invalid("scorer gate closed before recording a start"))?;
            released
                .recv_timeout(SCORER_RELEASE_TIMEOUT)
                .map_err(|error| {
                    data_invalid(format!(
                        "scorer gate did not release query {query_id} segment '{segment}': {error}"
                    ))
                })?;
            Ok(())
        }
    }

    fn scorer_gate() -> (
        ScorerGate,
        tokio::sync::mpsc::UnboundedReceiver<StartedScorer>,
    ) {
        let (started, receiver) = tokio::sync::mpsc::unbounded_channel();
        (ScorerGate { started }, receiver)
    }

    async fn wait_for_started_scorers(
        receiver: &mut tokio::sync::mpsc::UnboundedReceiver<StartedScorer>,
        count: usize,
    ) -> Result<Vec<StartedScorer>, String> {
        let mut scorers = Vec::with_capacity(count);
        while scorers.len() < count {
            let scorer = match tokio::time::timeout(SCORER_START_TIMEOUT, receiver.recv()).await {
                Ok(Some(scorer)) => scorer,
                Ok(None) => {
                    return Err("scorer gate closed before all starts arrived".to_string());
                }
                Err(_) => {
                    receiver.close();
                    return Err(format!(
                        "timed out waiting for {count} scorer starts; observed {}",
                        scorers.len()
                    ));
                }
            };
            scorers.push(scorer);
        }
        Ok(scorers)
    }

    fn release_started_scorers(scorers: Vec<StartedScorer>) -> Vec<ScorerIdentity> {
        let mut identities = Vec::with_capacity(scorers.len());
        for (identity, release) in scorers {
            identities.push(identity);
            let _ = release.send(());
        }
        identities.sort();
        identities
    }

    async fn expect_started_scorers(
        receiver: &mut tokio::sync::mpsc::UnboundedReceiver<StartedScorer>,
        expected: &[(usize, &str)],
    ) -> Result<(), String> {
        let scorers = wait_for_started_scorers(receiver, expected.len()).await?;
        let observed = release_started_scorers(scorers);
        let mut expected = expected
            .iter()
            .map(|(query_id, segment)| (*query_id, (*segment).to_string()))
            .collect::<Vec<_>>();
        expected.sort();
        if observed != expected {
            return Err(format!(
                "unexpected scorer starts: expected {expected:?}, observed {observed:?}"
            ));
        }
        Ok(())
    }

    /// ANN searcher probe that records the peak number of `search_batch` calls
    /// running simultaneously. Concurrency tests attach a gate so the test task can
    /// inspect which scorers are active before releasing them.
    struct PeakProbeAnn {
        inflight: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        peak: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        gate: Option<ScorerGate>,
        query_id: usize,
        gated_segment: Option<&'static str>,
    }
    impl PkVectorAnnSearcher for PeakProbeAnn {
        fn load_segment(
            &self,
            segment: &BucketAnnSegment,
        ) -> futures::future::BoxFuture<'static, crate::Result<Bytes>> {
            empty_ann_loader(segment)
        }
        fn search_batch(
            &self,
            segment: &BucketAnnSegment,
            _segment_bytes: Bytes,
            queries: &[&[f32]],
            _metric: VectorSearchMetric,
            _limit: usize,
            _active_source_files: &HashSet<String>,
            _dvs: &HashMap<String, Arc<DeletionVector>>,
            _opts: &HashMap<String, String>,
            _residual_ranges: Option<&HashMap<String, roaring::RoaringTreemap>>,
        ) -> crate::Result<Vec<Vec<PkVectorSearchResult>>> {
            use std::sync::atomic::Ordering::SeqCst;
            let current = self.inflight.fetch_add(1, SeqCst) + 1;
            self.peak.fetch_max(current, SeqCst);
            let gate_result = if let Some(gate) = &self.gate {
                if self.gated_segment.is_none_or(|gated| gated == segment.path) {
                    gate.wait_for_release(self.query_id, &segment.path)
                } else {
                    Ok(())
                }
            } else {
                std::thread::sleep(std::time::Duration::from_millis(50));
                Ok(())
            };
            self.inflight.fetch_sub(1, SeqCst);
            gate_result?;
            Ok(queries.iter().map(|_| Vec::new()).collect())
        }
    }

    /// `n` ANN segments in one bucket, each with a distinct payload path and a
    /// distinct (covered) source file. Returns the searcher + its peak counter so a
    /// test can assert how many segment searches ran at once.
    fn n_segment_bucket(
        n: usize,
        gate: Option<ScorerGate>,
    ) -> (
        Vec<BucketAnnSegment>,
        Vec<BucketActiveFile>,
        std::sync::Arc<PeakProbeAnn>,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) {
        let mut segments = Vec::with_capacity(n);
        let mut actives = Vec::with_capacity(n);
        for i in 0..n {
            let src = format!("cov-{i}");
            segments.push(BucketAnnSegment {
                source_meta: meta(&[(src.as_str(), 2)]),
                path: format!("seg-{i}"),
                file_size: 0,
                index_meta: Vec::new(),
            });
            actives.push(active(&src, 2));
        }
        let peak = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let probe = std::sync::Arc::new(PeakProbeAnn {
            inflight: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            peak: peak.clone(),
            gate,
            query_id: 0,
            gated_segment: None,
        });
        (segments, actives, probe, peak)
    }

    #[tokio::test]
    async fn ann_segments_search_in_parallel_at_concurrency_above_one() {
        let _test_guard = lock_gated_scorer_tests().await;
        ensure_global_index_executor_capacity(4);
        let (gate, mut started) = scorer_gate();
        let (segments, active_files, probe, _peak) = n_segment_bucket(2, Some(gate));
        let searcher: Arc<dyn PkVectorAnnSearcher> = probe;
        let factory = unreachable_search();
        let search = async {
            bucket_search(
                Some(searcher),
                &segments,
                &active_files,
                &HashMap::new(),
                &factory,
                &[0.0, 0.0],
                VectorSearchMetric::L2,
                8,
                8,
                &HashMap::new(),
                false,
                None,
                4,
                Some(SearchBudget::per_query_only(4)),
            )
            .await
        };
        let observe = expect_started_scorers(&mut started, &[(0, "seg-0"), (0, "seg-1")]);
        let (out, observed) = tokio::join!(search, observe);
        observed.expect("two ANN segment scorers must start before release");
        let out = out.unwrap();
        assert!(out.indexed.is_empty());
    }

    #[tokio::test]
    async fn ann_segments_search_serially_at_concurrency_one() {
        // At concurrency 1 the ANN segments must NOT overlap: peak simultaneous
        // calls is exactly 1 (still off the async worker on the dedicated executor,
        // but one at a time). Guards the one-worker budget without regressing inline.
        let (segments, active_files, probe, peak) = n_segment_bucket(3, None);
        let searcher: Arc<dyn PkVectorAnnSearcher> = probe;
        let factory = unreachable_search();
        bucket_search(
            Some(searcher),
            &segments,
            &active_files,
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
        assert_eq!(
            peak.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "concurrency == 1 must search ANN segments one at a time"
        );
    }

    #[tokio::test]
    async fn ann_segment_search_respects_shared_budget() {
        // Four ANN segments, shared budget of 2: at most 2 segment searches may run
        // at once even though concurrency (fan-out width) is 4. Mirrors Java's single
        // shared pool sized to threadNum capping total in-flight leaf work.
        let _test_guard = lock_gated_scorer_tests().await;
        ensure_global_index_executor_capacity(4);
        let (gate, mut started) = scorer_gate();
        let (segments, active_files, probe, peak) = n_segment_bucket(4, Some(gate));
        let searcher: Arc<dyn PkVectorAnnSearcher> = probe;
        let factory = unreachable_search();
        let shared = Arc::new(Semaphore::new(2));
        let search = async {
            bucket_search(
                Some(searcher),
                &segments,
                &active_files,
                &HashMap::new(),
                &factory,
                &[0.0, 0.0],
                VectorSearchMetric::L2,
                8,
                8,
                &HashMap::new(),
                false,
                None,
                4,
                Some(SearchBudget::shared_for_test(shared.clone())),
            )
            .await
        };
        let observe = async {
            let first = wait_for_started_scorers(&mut started, 2).await?;
            let first_available = shared.available_permits();
            let mut identities = release_started_scorers(first);

            let second = wait_for_started_scorers(&mut started, 2).await?;
            let second_available = shared.available_permits();
            identities.extend(release_started_scorers(second));
            identities.sort();
            Ok::<_, String>((identities, [first_available, second_available]))
        };
        let (out, observed) = tokio::join!(search, observe);
        let (identities, available_permits) =
            observed.expect("all four gated ANN scorers must start in two waves");
        out.unwrap();
        assert_eq!(
            identities,
            vec![
                (0, "seg-0".to_string()),
                (0, "seg-1".to_string()),
                (0, "seg-2".to_string()),
                (0, "seg-3".to_string()),
            ],
            "every ANN segment must pass through the controlled gate exactly once"
        );
        assert_eq!(
            available_permits,
            [0, 0],
            "both shared-budget permits must remain held while each scorer wave is gated"
        );
        let observed = peak.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            observed, 2,
            "shared budget must cap concurrent ANN segment searches at exactly 2"
        );
    }

    #[tokio::test]
    async fn malformed_ann_source_fails_before_any_segment_search_runs() {
        // A segment whose ANN source row count disagrees with the active file is
        // corruption. The mismatch must be detected during up-front structural
        // validation, BEFORE any segment search leaf is spawned — so the probe's
        // peak stays 0. Guards Codex's "validate all segments before launching any
        // job" requirement.
        let (mut segments, _active_files, probe, peak) = n_segment_bucket(2, None);
        // Break segment 1's source row count vs the active file (active says 2).
        segments[1] = BucketAnnSegment {
            source_meta: meta(&[("cov-1", 99)]),
            path: "seg-1".to_string(),
            file_size: 0,
            index_meta: Vec::new(),
        };
        let searcher: Arc<dyn PkVectorAnnSearcher> = probe;
        let factory = unreachable_search();
        let err = bucket_search(
            Some(searcher),
            &segments,
            &[active("cov-0", 2), active("cov-1", 2)],
            &HashMap::new(),
            &factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            8,
            8,
            &HashMap::new(),
            false,
            None,
            4,
            Some(SearchBudget::per_query_only(4)),
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains("does not match"),
            "expected a row-count mismatch error, got: {err}"
        );
        assert_eq!(
            peak.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "no ANN segment search may run when structural validation fails"
        );
    }

    #[tokio::test]
    async fn batch_ann_segments_search_in_parallel_at_concurrency_above_one() {
        // Multi-query (2 queries) routes through the `bucket_search_batch` multi-query
        // path (not the batch-of-one short-circuit), so this covers the batch ANN
        // parallel loop specifically. Two segments at concurrency 4 must overlap.
        let _test_guard = lock_gated_scorer_tests().await;
        ensure_global_index_executor_capacity(4);
        let (gate, mut started) = scorer_gate();
        let (segments, active_files, probe, _peak) = n_segment_bucket(2, Some(gate));
        let searcher: Arc<dyn PkVectorAnnSearcher> = probe;
        let factory = unreachable_search();
        let q0: &[f32] = &[0.0, 0.0];
        let q1: &[f32] = &[1.0, 1.0];
        let search = async {
            bucket_search_batch(
                Some(searcher),
                &segments,
                &active_files,
                &HashMap::new(),
                &factory,
                &[q0, q1],
                VectorSearchMetric::L2,
                8,
                8,
                &HashMap::new(),
                false,
                None,
                4,
                Some(SearchBudget::per_query_only(4)),
            )
            .await
        };
        let observe = expect_started_scorers(&mut started, &[(0, "seg-0"), (0, "seg-1")]);
        let (out, observed) = tokio::join!(search, observe);
        observed.expect("two batch ANN segment scorers must start before release");
        let out = out.unwrap();
        assert_eq!(out.len(), 2, "one result list per query");
    }

    /// ANN searcher whose `search_batch` panics, exercising the dedicated
    /// executor's panic-to-error mapping.
    struct PanicAnn;
    impl PkVectorAnnSearcher for PanicAnn {
        fn load_segment(
            &self,
            segment: &BucketAnnSegment,
        ) -> futures::future::BoxFuture<'static, crate::Result<Bytes>> {
            empty_ann_loader(segment)
        }
        fn search_batch(
            &self,
            _segment: &BucketAnnSegment,
            _segment_bytes: Bytes,
            _queries: &[&[f32]],
            _metric: VectorSearchMetric,
            _limit: usize,
            _active_source_files: &HashSet<String>,
            _dvs: &HashMap<String, Arc<DeletionVector>>,
            _opts: &HashMap<String, String>,
            _residual_ranges: Option<&HashMap<String, roaring::RoaringTreemap>>,
        ) -> crate::Result<Vec<Vec<PkVectorSearchResult>>> {
            panic!("scorer panic to exercise JoinError mapping");
        }
    }

    #[tokio::test]
    async fn ann_segment_scorer_panic_maps_to_unexpected_error() {
        // A panic inside the dedicated-executor ANN leaf must surface as a mapped
        // `UnexpectedError` ("ANN segment search task failed"), not abort the runtime
        // or hang. Uses the parallel path (concurrency 4).
        let (segments, active_files, _probe, _peak) = n_segment_bucket(1, None);
        let searcher: Arc<dyn PkVectorAnnSearcher> = Arc::new(PanicAnn);
        let factory = unreachable_search();
        let err = bucket_search(
            Some(searcher),
            &segments,
            &active_files,
            &HashMap::new(),
            &factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            8,
            8,
            &HashMap::new(),
            false,
            None,
            4,
            Some(SearchBudget::per_query_only(4)),
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains("ANN segment search task failed"),
            "panic must map to UnexpectedError, got: {err}"
        );
    }

    /// ANN searcher that counts every completed `search_batch` call and returns an
    /// `Err` (naming the segment's source file) for any segment whose source file is
    /// in `fail_files`. A configurable pre-return spin makes the erroring segment
    /// finish FIRST while the others are still running, so a short-circuiting drain
    /// would surface a non-deterministic / non-lowest-index error and skip jobs.
    struct DrainProbeAnn {
        completed: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        fail_files: HashSet<String>,
        slow_files: HashSet<String>,
    }
    impl PkVectorAnnSearcher for DrainProbeAnn {
        fn load_segment(
            &self,
            segment: &BucketAnnSegment,
        ) -> futures::future::BoxFuture<'static, crate::Result<Bytes>> {
            empty_ann_loader(segment)
        }
        fn search_batch(
            &self,
            segment: &BucketAnnSegment,
            _segment_bytes: Bytes,
            queries: &[&[f32]],
            _metric: VectorSearchMetric,
            _limit: usize,
            _active_source_files: &HashSet<String>,
            _dvs: &HashMap<String, Arc<DeletionVector>>,
            _opts: &HashMap<String, String>,
            _residual_ranges: Option<&HashMap<String, roaring::RoaringTreemap>>,
        ) -> crate::Result<Vec<Vec<PkVectorSearchResult>>> {
            let file = segment.source_meta.source_files()[0]
                .file_name()
                .to_string();
            // Non-failing "slow" segments block so they are still in flight when the
            // fast-failing segment returns — proving the drain awaits them anyway.
            if self.slow_files.contains(&file) {
                std::thread::sleep(std::time::Duration::from_millis(80));
            }
            self.completed
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.fail_files.contains(&file) {
                return Err(data_invalid(format!("boom in {file}")));
            }
            Ok(queries.iter().map(|_| Vec::new()).collect())
        }
    }

    #[tokio::test]
    async fn drain_awaits_all_jobs_and_returns_lowest_index_error() {
        // 4 segments (seg-0..seg-3 over cov-0..cov-3). Segments 1 and 2 error; 0 and 3
        // are slow. The drain must (a) run ALL 4 scorers to completion even though
        // seg-1 errors early, and (b) surface seg-1's error (lowest index), not seg-2's
        // and not whichever finished first.
        let (segments, active_files, _probe, _peak) = n_segment_bucket(4, None);
        let completed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let searcher: Arc<dyn PkVectorAnnSearcher> = Arc::new(DrainProbeAnn {
            completed: completed.clone(),
            fail_files: HashSet::from(["cov-1".to_string(), "cov-2".to_string()]),
            slow_files: HashSet::from(["cov-0".to_string(), "cov-3".to_string()]),
        });
        let factory = unreachable_search();
        let err = bucket_search(
            Some(searcher),
            &segments,
            &active_files,
            &HashMap::new(),
            &factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            8,
            8,
            &HashMap::new(),
            false,
            None,
            4,
            Some(SearchBudget::per_query_only(4)),
        )
        .await
        .unwrap_err();
        // (b) lowest-index erroring segment wins, deterministically.
        assert!(
            err.to_string().contains("boom in cov-1"),
            "must surface the lowest-index error (cov-1), got: {err}"
        );
        // (a) every segment's scorer ran to completion — no job was skipped by an
        // early short-circuit.
        assert_eq!(
            completed.load(std::sync::atomic::Ordering::SeqCst),
            4,
            "all 4 ANN segment jobs must run to completion before the error returns"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shared_budget_caps_ann_across_concurrent_searches() {
        // Two independent searches (simulating two concurrent PK-vector queries), each
        // with 2 ANN segments and concurrency 4, but sharing ONE budget semaphore of
        // 2 permits (the deliberately-shared test seam). Their COMBINED in-flight ANN
        // count must never exceed 2 — proving the shared budget caps ANN work ACROSS
        // queries, which is what the process-global static does in production. Before
        // the fix, each query built its own Semaphore, so the combined peak could
        // reach 2 (segments) x 2 (queries) = 4.
        let _test_guard = lock_gated_scorer_tests().await;
        ensure_global_index_executor_capacity(4);
        let (gate, mut started) = scorer_gate();
        let inflight = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let peak = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let shared = Arc::new(Semaphore::new(2));

        let run = |query_id: usize, budget: SearchBudget| {
            let gate = gate.clone();
            let inflight = inflight.clone();
            let peak = peak.clone();
            async move {
                let (segments, active_files, _p, _pk) = n_segment_bucket(2, None);
                let searcher: Arc<dyn PkVectorAnnSearcher> = Arc::new(PeakProbeAnn {
                    inflight,
                    peak,
                    gate: Some(gate),
                    query_id,
                    gated_segment: Some("seg-0"),
                });
                let factory = unreachable_search();
                bucket_search(
                    Some(searcher),
                    &segments,
                    &active_files,
                    &HashMap::new(),
                    &factory,
                    &[0.0, 0.0],
                    VectorSearchMetric::L2,
                    8,
                    8,
                    &HashMap::new(),
                    false,
                    None,
                    4,
                    Some(budget),
                )
                .await?;
                Ok::<_, crate::Error>(())
            }
        };

        // Both searches share the SAME budget semaphore.
        let a = run(0, SearchBudget::shared_for_test(shared.clone()));
        let b = run(1, SearchBudget::shared_for_test(shared.clone()));
        let observe = async {
            let scorers = wait_for_started_scorers(&mut started, 2).await?;
            let available_permits = shared.available_permits();
            let identities = release_started_scorers(scorers);
            Ok::<_, String>((identities, available_permits))
        };
        let (a_out, b_out, observed) = tokio::join!(a, b, observe);
        let (identities, available_permits) =
            observed.expect("one scorer from each query must start before release");
        a_out.unwrap();
        b_out.unwrap();
        assert_eq!(
            identities,
            vec![(0, "seg-0".to_string()), (1, "seg-0".to_string())],
            "the controlled gate must observe scorer overlap across both queries"
        );
        assert_eq!(
            available_permits, 0,
            "both shared-budget permits must be held by the gated cross-query scorers"
        );

        let observed = peak.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            observed, 2,
            "shared budget must cap ANN across concurrent searches at exactly 2"
        );
    }

    /// ANN searcher whose score performs a BOUNDED two-way handshake with a matching
    /// exact-file probe: it signals arrival, then waits (with a timeout) for the
    /// exact leaf to signal too. If both arrive within the timeout they were in
    /// flight simultaneously — deterministic overlap detection with no sleeps and no
    /// unbounded wait. If they cannot overlap (e.g. a two-phase scheduler), the leaf
    /// that runs first times out waiting for the peer and RETURNS (setting no overlap
    /// flag), so the test fails cleanly on the assertion rather than hanging forever.
    struct HandshakeAnn {
        /// ANN sends here on arrival; the exact side receives it.
        ann_here: std::sync::mpsc::SyncSender<()>,
        /// ANN receives the exact side's arrival here.
        exact_here: std::sync::Arc<std::sync::Mutex<std::sync::mpsc::Receiver<()>>>,
        overlapped: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }
    impl PkVectorAnnSearcher for HandshakeAnn {
        fn load_segment(
            &self,
            segment: &BucketAnnSegment,
        ) -> futures::future::BoxFuture<'static, crate::Result<Bytes>> {
            empty_ann_loader(segment)
        }
        fn search_batch(
            &self,
            _segment: &BucketAnnSegment,
            _segment_bytes: Bytes,
            queries: &[&[f32]],
            _metric: VectorSearchMetric,
            _limit: usize,
            _active_source_files: &HashSet<String>,
            _dvs: &HashMap<String, Arc<DeletionVector>>,
            _opts: &HashMap<String, String>,
            _residual_ranges: Option<&HashMap<String, roaring::RoaringTreemap>>,
        ) -> crate::Result<Vec<Vec<PkVectorSearchResult>>> {
            // Runs on the blocking pool. Announce arrival, then wait (bounded) for the
            // exact leaf. Both arriving proves overlap; a timeout means no overlap.
            let _ = self.ann_here.try_send(());
            let got = self
                .exact_here
                .lock()
                .unwrap()
                .recv_timeout(std::time::Duration::from_secs(5));
            if got.is_ok() {
                self.overlapped
                    .store(true, std::sync::atomic::Ordering::SeqCst);
            }
            Ok(queries.iter().map(|_| Vec::new()).collect())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ann_and_exact_leaves_overlap_within_one_bucket() {
        // One bucket with one ANN segment (covers "cov") AND one uncovered exact file
        // ("ex"). The ANN leaf (blocking) and the exact leaf (via spawn_blocking) do a
        // bounded two-way handshake: each signals arrival and waits (5s cap) for the
        // other. Both arriving proves they overlap. A two-phase scheduler (all ANN,
        // then all exact) cannot get both in flight, so the leaf that runs first
        // times out waiting for the peer (which never co-runs) and `overlapped` stays
        // false — the assertion fails cleanly within ~5s, not by hanging.
        let (ann_tx, ann_rx) = std::sync::mpsc::sync_channel::<()>(1);
        let (exact_tx, exact_rx) = std::sync::mpsc::sync_channel::<()>(1);
        let ann_rx = std::sync::Arc::new(std::sync::Mutex::new(ann_rx));
        let exact_rx = std::sync::Arc::new(std::sync::Mutex::new(exact_rx));
        let overlapped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        let segment = BucketAnnSegment::for_test(meta(&[("cov", 2)]));
        let searcher: Arc<dyn PkVectorAnnSearcher> = Arc::new(HandshakeAnn {
            ann_here: ann_tx,
            exact_here: exact_rx.clone(),
            overlapped: overlapped.clone(),
        });

        // Exact-file probe: rendezvous on the same channels via `spawn_blocking`
        // (so it can block-wait alongside the ANN blocking leaf), also bounded.
        let ex_ann_rx = ann_rx.clone();
        let factory = as_search(
            move |file: &BucketActiveFile,
                  queries: &[&[f32]],
                  _: VectorSearchMetric,
                  _: usize,
                  _: &(dyn Fn(i64) -> bool + Sync)|
                  -> ExactFileSearchFuture<'_> {
                let ex_ann_rx = ex_ann_rx.clone();
                let exact_tx = exact_tx.clone();
                let n = queries.len();
                let _ = file;
                Box::pin(async move {
                    tokio::task::spawn_blocking(move || {
                        let _ = exact_tx.try_send(());
                        let _ = ex_ann_rx
                            .lock()
                            .unwrap()
                            .recv_timeout(std::time::Duration::from_secs(5));
                    })
                    .await
                    .expect("exact handshake task");
                    Ok(vec![Vec::new(); n])
                })
            },
        );

        let out = bucket_search(
            Some(searcher),
            &[segment],
            &[active("cov", 2), active("ex", 2)],
            &HashMap::new(),
            &factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            8,
            8,
            &HashMap::new(),
            false, // NOT fast: exact fallback runs
            None,
            4,
            Some(SearchBudget::per_query_only(4)),
        )
        .await
        .unwrap();
        assert!(out.indexed.is_empty() && out.exact.is_empty());
        assert!(
            overlapped.load(std::sync::atomic::Ordering::SeqCst),
            "ANN and exact leaves of one bucket must overlap (both completed the handshake)"
        );
    }

    /// ANN searcher whose `load_segment` records which segment paths it loaded and
    /// hands the loaded bytes through to `search_batch`, which asserts it received
    /// exactly those bytes. Proves the leaf loads per-segment via the seam (not from
    /// an up-front map) and that the loaded bytes reach the scorer.
    struct RecordingLoaderAnn {
        loaded: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    }
    impl PkVectorAnnSearcher for RecordingLoaderAnn {
        fn load_segment(
            &self,
            segment: &BucketAnnSegment,
        ) -> futures::future::BoxFuture<'static, crate::Result<Bytes>> {
            let loaded = self.loaded.clone();
            let path = segment.path.clone();
            // Bytes are the segment path itself, so `search_batch` can verify the
            // exact bytes from THIS segment's load arrived (not some other segment's).
            Box::pin(async move {
                loaded.lock().unwrap().push(path.clone());
                Ok(Bytes::from(path.into_bytes()))
            })
        }
        fn search_batch(
            &self,
            segment: &BucketAnnSegment,
            segment_bytes: Bytes,
            queries: &[&[f32]],
            _metric: VectorSearchMetric,
            _limit: usize,
            _active_source_files: &HashSet<String>,
            _dvs: &HashMap<String, Arc<DeletionVector>>,
            _opts: &HashMap<String, String>,
            _residual_ranges: Option<&HashMap<String, roaring::RoaringTreemap>>,
        ) -> crate::Result<Vec<Vec<PkVectorSearchResult>>> {
            // The bytes handed to the scorer must be exactly this segment's loaded
            // bytes (its path), proving load→score threads the right payload.
            assert_eq!(segment_bytes.as_ref(), segment.path.as_bytes());
            Ok(queries.iter().map(|_| Vec::new()).collect())
        }
    }

    #[tokio::test]
    async fn ann_segment_bytes_are_loaded_lazily_per_segment_via_the_seam() {
        // Two ANN segments; the searcher's `load_segment` is invoked once per segment
        // during the bucket search (lazily, in each leaf), and the bytes it returns
        // are the ones passed to that segment's scorer. This is the fused
        // load→score path (no up-front all-segments preload map).
        let loaded = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let searcher: Arc<dyn PkVectorAnnSearcher> = Arc::new(RecordingLoaderAnn {
            loaded: loaded.clone(),
        });
        let seg_a = BucketAnnSegment {
            source_meta: meta(&[("cov-a", 2)]),
            path: "seg-a".to_string(),
            file_size: 0,
            index_meta: Vec::new(),
        };
        let seg_b = BucketAnnSegment {
            source_meta: meta(&[("cov-b", 2)]),
            path: "seg-b".to_string(),
            file_size: 0,
            index_meta: Vec::new(),
        };
        let factory = unreachable_search();
        bucket_search(
            Some(searcher),
            &[seg_a, seg_b],
            &[active("cov-a", 2), active("cov-b", 2)],
            &HashMap::new(),
            &factory,
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            8,
            8,
            &HashMap::new(),
            false,
            None,
            4,
            Some(SearchBudget::per_query_only(4)),
        )
        .await
        .unwrap();
        let mut got = loaded.lock().unwrap().clone();
        got.sort();
        assert_eq!(
            got,
            vec!["seg-a".to_string(), "seg-b".to_string()],
            "each ANN segment must be loaded exactly once via load_segment during the search"
        );
    }
}
