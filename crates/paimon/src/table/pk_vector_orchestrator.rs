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

use roaring::RoaringTreemap;

use futures::stream::{self, StreamExt, TryStreamExt};
use tokio::sync::Semaphore;

use crate::deletion_vector::DeletionVector;
use crate::spec::BinaryRow;
use crate::table::data_file_reader::DataFileReader;
use crate::table::pk_vector_indexed_split_read::PkVectorIndexedSplit;
use crate::table::source::{DataSplit, DataSplitBuilder, RowRange};
use crate::vindex::pkvector::ann::PkVectorAnnSearcher;
use crate::vindex::pkvector::bucket::{
    bucket_search_batch, BucketActiveFile, BucketAnnSegment, ExactFileSearchFuture,
};
use crate::vindex::pkvector::metric::{java_float_compare, VectorSearchMetric};
use crate::vindex::pkvector::result::PkVectorSearchResult;

fn data_invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

/// Coerce a closure into the higher-ranked split-scoped exact-file search shape
/// expected by [`PkVectorOrchestrator::search_candidates`], binding the returned
/// future's borrow to the arguments' lifetime. Callers building a search closure
/// use this so the higher-ranked bound is supplied where inference cannot. The
/// closure is `Fn + Send + Sync` (it is called concurrently by the parallel
/// search), not `FnMut`.
#[allow(clippy::type_complexity)]
pub(crate) fn as_split_exact_file_search<F>(f: F) -> F
where
    F: for<'s, 'a> Fn(
            usize,
            &'s PkVectorSearchSplit,
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

/// Coerce a closure into the per-file exact-file search shape `bucket_search`
/// expects, supplying the higher-ranked bound closure inference cannot express.
#[allow(clippy::type_complexity)]
fn as_bucket_exact_file_search<F>(f: F) -> F
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
#[derive(Clone)]
pub(crate) struct PkVectorCandidate {
    pub split_index: usize,
    pub partition: BinaryRow,
    pub bucket: i32,
    pub data_file_name: String,
    pub row_position: i64,
    pub distance: f32,
}

/// Best-first indexed (approximate) and exact-fallback candidate lists, each
/// already globally bounded. The indexed list may be over-fetched for a later
/// exact rerank; the exact list is bounded to the caller's final limit.
pub(crate) struct OrchestratorSearchResult {
    pub(crate) indexed: Vec<PkVectorCandidate>,
    pub(crate) exact: Vec<PkVectorCandidate>,
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

/// Merge already-bounded indexed and exact candidate lists into a single
/// best-first list truncated to `limit`. The final global Top-K over the union
/// is what guarantees the merged result is independent of how the two inputs
/// were individually bounded.
pub(crate) fn merge_candidates(
    indexed: Vec<PkVectorCandidate>,
    exact: Vec<PkVectorCandidate>,
    limit: usize,
) -> Vec<PkVectorCandidate> {
    let mut all = indexed;
    all.extend(exact);
    global_top_k(all, limit)
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
        let source = &splits
            .get(split_index)
            .ok_or_else(|| {
                data_invalid(format!(
                    "vector search hit references split index {split_index} out of range (splits: {})",
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
    /// indexed (approximate) and exact-fallback survivors as two separate
    /// best-first lists (each through the full 5-level tie-break, raw distance
    /// preserved). The indexed list is bounded to `indexed_limit` (over-fetched
    /// for a later exact rerank); the exact list is bounded to `limit`. The
    /// exact-reader factory is split-scoped: it receives the current split index
    /// and split so a caller can build a reader keyed to the specific split/file.
    /// `skip_exact_fallback` forwards to `bucket_search`.
    ///
    /// `residual_by_split`, when present, carries one per-file allow-list of
    /// physical row positions per split (indexed parallel to `splits`): only
    /// positions listed for a file may survive that bucket's search. A file
    /// absent from its split's map (or mapped to an empty set) contributes no
    /// candidates. `None` applies no residual filtering. The slice must have the
    /// same length as `splits`.
    ///
    /// This is the single-query wrapper over
    /// [`search_candidates_batch`](Self::search_candidates_batch): it searches the
    /// one query and returns its sole result, so its output is byte-identical to
    /// the batch-of-one path.
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::type_complexity)]
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn search_candidates(
        &self,
        splits: &[PkVectorSearchSplit],
        query: &[f32],
        metric: VectorSearchMetric,
        limit: usize,
        indexed_limit: usize,
        ann_searcher: Option<&dyn PkVectorAnnSearcher>,
        exact_file_search: &(dyn for<'s, 'a> Fn(
            usize,
            &'s PkVectorSearchSplit,
            &'a BucketActiveFile,
            &'a [&'a [f32]],
            VectorSearchMetric,
            usize,
            &'a (dyn Fn(i64) -> bool + Sync),
        ) -> ExactFileSearchFuture<'a>
              + Send
              + Sync),
        search_options: &HashMap<String, String>,
        skip_exact_fallback: bool,
        residual_by_split: Option<&[HashMap<String, RoaringTreemap>]>,
        concurrency: usize,
    ) -> crate::Result<OrchestratorSearchResult> {
        let mut results = self
            .search_candidates_batch(
                splits,
                &[query],
                metric,
                limit,
                indexed_limit,
                ann_searcher,
                exact_file_search,
                search_options,
                skip_exact_fallback,
                residual_by_split,
                concurrency,
            )
            .await?;
        debug_assert_eq!(results.len(), 1);
        Ok(results.remove(0))
    }

    /// Multi-query variant of [`search_candidates`](Self::search_candidates):
    /// share ONE per-bucket plan (splits, DV maps, opened readers) across all N
    /// queries and return one [`OrchestratorSearchResult`] per query (outer index
    /// aligned to `queries`). Per split, the bucket state is built once and
    /// `bucket_search_batch` fans all queries over the shared readers into
    /// per-query bounded heaps; after every split, each query's indexed/exact
    /// lists get their own cross-bucket global Top-K. No query's candidates bleed
    /// into another's (independent per-query heaps).
    ///
    /// The residual allow-list depends only on the filter and the plan, not the
    /// query vector, so the SAME `residual_by_split` slice is shared across every
    /// query. Input-shape validation (positive limits, non-empty query, residual
    /// count) is applied per query / once as appropriate.
    ///
    /// `concurrency` is the global fan-out limit (Java `GLOBAL_INDEX_THREAD_NUM`):
    /// `1` runs the buckets and their files strictly sequentially, larger values fan
    /// them out with `buffer_unordered`. To match Java's single shared
    /// `GlobalIndexReadThreadPool`, a single [`Semaphore`] budget of `concurrency`
    /// permits is shared across BOTH the per-bucket and per-exact-file fan-outs and
    /// acquired only around leaf exact-file I/O, so total in-flight exact-file
    /// searches are capped at `concurrency` overall — not `concurrency` per bucket,
    /// which would allow up to `concurrency * concurrency`. Each bucket's per-query
    /// results feed per-query cross-bucket global Top-K heaps, which are
    /// order-independent, so the output does not depend on which bucket finished
    /// first; results are collected and merged into the correct per-query slot after
    /// the parallel stage.
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::type_complexity)]
    pub(crate) async fn search_candidates_batch(
        &self,
        splits: &[PkVectorSearchSplit],
        queries: &[&[f32]],
        metric: VectorSearchMetric,
        limit: usize,
        indexed_limit: usize,
        ann_searcher: Option<&dyn PkVectorAnnSearcher>,
        exact_file_search: &(dyn for<'s, 'a> Fn(
            usize,
            &'s PkVectorSearchSplit,
            &'a BucketActiveFile,
            &'a [&'a [f32]],
            VectorSearchMetric,
            usize,
            &'a (dyn Fn(i64) -> bool + Sync),
        ) -> ExactFileSearchFuture<'a>
              + Send
              + Sync),
        search_options: &HashMap<String, String>,
        skip_exact_fallback: bool,
        residual_by_split: Option<&[HashMap<String, RoaringTreemap>]>,
        concurrency: usize,
    ) -> crate::Result<Vec<OrchestratorSearchResult>> {
        // Eager input-shape validation (Java checkArgument parity).
        if queries.is_empty() {
            return Err(data_invalid("vector search requires at least one query"));
        }
        if limit == 0 {
            return Err(data_invalid("vector search limit must be positive"));
        }
        if indexed_limit == 0 {
            return Err(data_invalid("vector indexed search limit must be positive"));
        }
        for query in queries {
            if query.is_empty() {
                return Err(data_invalid("vector search query must not be empty"));
            }
        }
        if let Some(per_split) = residual_by_split {
            if per_split.len() != splits.len() {
                return Err(data_invalid(
                    "residual range map count does not match split count",
                ));
            }
        }

        // Eager per-bucket search -> per-query tagged candidates, kept split by
        // path. One inner Vec per query.
        let mut indexed_candidates: Vec<Vec<PkVectorCandidate>> =
            (0..queries.len()).map(|_| Vec::new()).collect();
        let mut exact_candidates: Vec<Vec<PkVectorCandidate>> =
            (0..queries.len()).map(|_| Vec::new()).collect();

        // One shared concurrency budget for the WHOLE search, mirroring Java's single
        // `GlobalIndexReadThreadPool`: the per-bucket and per-exact-file fan-outs draw
        // slots from the SAME N permits, so total in-flight exact-file I/O is capped at
        // N across all buckets and files (not N per bucket, which would allow N*N).
        // Only leaf exact-file work acquires a permit; bucket orchestration never holds
        // one, so it cannot starve leaf work. `concurrency <= 1` takes the strictly
        // sequential path at both levels and needs no budget.
        let search_budget = (concurrency > 1).then(|| Arc::new(Semaphore::new(concurrency)));

        // One lazy future per bucket. Each builds its own DV map + per-file search
        // closure, searches all queries against the bucket, and returns per-query
        // (indexed, exact) candidate lists already tagged with the bucket's
        // partition/bucket/split_index. The futures are not polled until driven
        // below, so the sequential branch observes buckets in strict split order.
        let per_bucket = splits.iter().enumerate().map(|(split_index, split)| {
            let search_budget = search_budget.clone();
            async move {
                let dvs = build_bucket_dv_map(&self.reader, split).await?;
                // Adapt the split-scoped search closure to bucket_search's per-file
                // closure by binding the current split index/split. The coercion helper
                // ties the produced future's borrow to the arguments, which closure
                // inference cannot express on its own.
                let bucket_search_closure = as_bucket_exact_file_search(
                    |file: &BucketActiveFile,
                     queries: &[&[f32]],
                     metric: VectorSearchMetric,
                     exact_limit: usize,
                     is_excluded: &(dyn Fn(i64) -> bool + Sync)|
                     -> ExactFileSearchFuture<'_> {
                        exact_file_search(
                            split_index,
                            split,
                            file,
                            queries,
                            metric,
                            exact_limit,
                            is_excluded,
                        )
                    },
                );
                let residual_ranges = residual_by_split.map(|per_split| &per_split[split_index]);
                let per_query = bucket_search_batch(
                    ann_searcher,
                    &split.ann_segments,
                    &split.active_files,
                    &dvs,
                    &bucket_search_closure,
                    queries,
                    metric,
                    indexed_limit,
                    limit,
                    search_options,
                    skip_exact_fallback,
                    residual_ranges,
                    concurrency,
                    search_budget,
                )
                .await?;
                if per_query.len() != queries.len() {
                    return Err(data_invalid(format!(
                        "bucket search returned {} result lists for {} queries",
                        per_query.len(),
                        queries.len()
                    )));
                }
                let tag = |PkVectorSearchResult {
                               data_file_name,
                               row_position,
                               distance,
                           }: PkVectorSearchResult| PkVectorCandidate {
                    split_index,
                    partition: split.data_split.partition().clone(),
                    bucket: split.data_split.bucket(),
                    data_file_name,
                    row_position,
                    distance,
                };
                let tagged: Vec<(Vec<PkVectorCandidate>, Vec<PkVectorCandidate>)> = per_query
                    .into_iter()
                    .map(|result| {
                        (
                            result.indexed.into_iter().map(&tag).collect(),
                            result.exact.into_iter().map(&tag).collect(),
                        )
                    })
                    .collect();
                Ok::<_, crate::Error>(tagged)
            }
        });

        // Drive the per-bucket futures. `concurrency == 1` uses a strictly
        // sequential loop so buckets are searched in split order; larger values fan
        // them out with `buffer_unordered`. Either way each bucket's per-query lists
        // are collected and only then folded into the per-query candidate
        // accumulators, and the final per-query `global_top_k` is order-independent
        // (deterministic `candidate_cmp`), so the result does not depend on bucket
        // completion order.
        let collected: Vec<Vec<(Vec<PkVectorCandidate>, Vec<PkVectorCandidate>)>> =
            if concurrency <= 1 {
                let mut out = Vec::with_capacity(splits.len());
                for fut in per_bucket {
                    out.push(fut.await?);
                }
                out
            } else {
                stream::iter(per_bucket)
                    .buffer_unordered(concurrency)
                    .try_collect::<Vec<_>>()
                    .await?
            };
        for tagged in collected {
            for (query_index, (indexed, exact)) in tagged.into_iter().enumerate() {
                indexed_candidates[query_index].extend(indexed);
                exact_candidates[query_index].extend(exact);
            }
        }

        Ok(indexed_candidates
            .into_iter()
            .zip(exact_candidates)
            .map(|(indexed, exact)| OrchestratorSearchResult {
                indexed: global_top_k(indexed, indexed_limit),
                exact: global_top_k(exact, limit),
            })
            .collect())
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
    fn merge_candidates_takes_global_top_k_over_union() {
        // indexed and exact each already bounded; the union's global Top-K must be
        // independent of how each side was bounded.
        let indexed = vec![cand(0, 0, "f", 0, 0.1), cand(0, 0, "f", 1, 0.4)];
        let exact = vec![cand(0, 0, "g", 0, 0.2)];
        let out = merge_candidates(indexed, exact, 2);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].distance, 0.1);
        assert_eq!(out[1].distance, 0.2); // 0.2 (exact) beats 0.4 (indexed)
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
    fn build_indexed_splits_fails_loud_on_out_of_range_split_index() {
        // A malformed candidate whose split_index is beyond the splits slice must
        // fail loud, not panic on a bare slice index.
        let splits = vec![search_split(0, vec![data_file("f", 10)])];
        let survivors = vec![cand(5, 0, "f", 0, 1.0)];
        let err = build_indexed_splits(survivors, &splits, VectorSearchMetric::L2)
            .map(|_| ())
            .expect_err("out-of-range split index must error");
        assert!(format!("{err:?}").contains("out of range"), "got: {err:?}");
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
    use crate::table::pk_vector_position_read::{PKEY_VECTOR_POSITION_COLUMN, SEARCH_SCORE_COLUMN};
    use crate::table::schema_manager::SchemaManager;
    use crate::table::source::DeletionFile;
    use crate::vindex::pkvector::exact::exact_search;
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

    /// Coerce a closure into the split-scoped exact-file search shape, supplying
    /// the higher-ranked bound closure inference cannot express. The closure is
    /// `Fn + Send + Sync`.
    #[allow(clippy::type_complexity)]
    fn as_split_search<F>(f: F) -> F
    where
        F: for<'s, 'a> Fn(
                usize,
                &'s PkVectorSearchSplit,
                &'a BucketActiveFile,
                &'a [&'a [f32]],
                VectorSearchMetric,
                usize,
                &'a (dyn Fn(i64) -> bool + Sync),
            ) -> ExactFileSearchFuture<'a>
            + Send
            + Sync,
    {
        as_split_exact_file_search(f)
    }

    /// A split-scoped search closure that must never be invoked.
    #[allow(clippy::type_complexity)]
    fn unreachable_split_search() -> impl for<'s, 'a> Fn(
        usize,
        &'s PkVectorSearchSplit,
        &'a BucketActiveFile,
        &'a [&'a [f32]],
        VectorSearchMetric,
        usize,
        &'a (dyn Fn(i64) -> bool + Sync),
    ) -> ExactFileSearchFuture<'a>
           + Send
           + Sync {
        as_split_search(
            |_: usize,
             _: &PkVectorSearchSplit,
             _: &BucketActiveFile,
             _: &[&[f32]],
             _: VectorSearchMetric,
             _: usize,
             _: &(dyn Fn(i64) -> bool + Sync)|
             -> ExactFileSearchFuture<'_> {
                Box::pin(async { unreachable!("closure must not be invoked in this test") })
            },
        )
    }

    #[allow(clippy::type_complexity)]
    fn as_file_search<F>(f: F) -> F
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

    /// Build a per-file search closure that runs the reference `exact_search` over
    /// an in-memory `ArrayReader` whose vectors come from `vectors_for(file_name)`.
    /// The caller wires a single query, so the returned outer `Vec` has one element.
    #[allow(clippy::type_complexity)]
    fn file_array_search(
        vectors_for: fn(&str) -> Vec<Option<Vec<f32>>>,
    ) -> impl for<'a> Fn(
        &'a BucketActiveFile,
        &'a [&'a [f32]],
        VectorSearchMetric,
        usize,
        &'a (dyn Fn(i64) -> bool + Sync),
    ) -> ExactFileSearchFuture<'a>
           + Send
           + Sync {
        as_file_search(
            move |file: &BucketActiveFile,
                  queries: &[&[f32]],
                  metric: VectorSearchMetric,
                  exact_limit: usize,
                  is_excluded: &(dyn Fn(i64) -> bool + Sync)|
                  -> ExactFileSearchFuture<'_> {
                let vectors = vectors_for(&file.file_name);
                let dimension = vectors
                    .first()
                    .and_then(|v| v.as_ref())
                    .map_or(2, |v| v.len());
                let file_name = file.file_name.clone();
                let query = queries[0].to_vec();
                Box::pin(async move {
                    let mut reader = ArrayReader::new(dimension, vectors);
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
        )
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
            Ok(queries.iter().map(|_| self.hits.clone()).collect())
        }
    }

    /// Run the eager per-bucket search + global Top-K, group survivors into indexed
    /// splits, then materialize each split in file/position order. This is the
    /// materialization path the production best-first read reorders on top of; the
    /// tests below drive it directly through its `pub(crate)` components.
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::type_complexity)]
    async fn materialize_via_splits(
        reader: DataFileReader,
        splits: &[PkVectorSearchSplit],
        query: &[f32],
        metric: VectorSearchMetric,
        limit: usize,
        ann: Option<&dyn PkVectorAnnSearcher>,
        search: &(dyn for<'a> Fn(
            &'a BucketActiveFile,
            &'a [&'a [f32]],
            VectorSearchMetric,
            usize,
            &'a (dyn Fn(i64) -> bool + Sync),
        ) -> ExactFileSearchFuture<'a>
              + Send
              + Sync),
        opts: &HashMap<String, String>,
    ) -> crate::Result<Vec<RecordBatch>> {
        let orch = PkVectorOrchestrator::new(reader.clone());
        // Wrap the per-file search into the split-scoped shape search_candidates
        // expects; the split index/split are unused here.
        let wrapped = as_split_search(
            |_: usize,
             _: &PkVectorSearchSplit,
             file: &BucketActiveFile,
             queries: &[&[f32]],
             metric: VectorSearchMetric,
             exact_limit: usize,
             is_excluded: &(dyn Fn(i64) -> bool + Sync)|
             -> ExactFileSearchFuture<'_> {
                search(file, queries, metric, exact_limit, is_excluded)
            },
        );
        let result = orch
            .search_candidates(
                splits, query, metric, limit, limit, ann, &wrapped, opts, false, None, 1,
            )
            .await?;
        // Merge the two bounded lists into the best-first survivors the
        // materialization path expects.
        let survivors = merge_candidates(result.indexed, result.exact, limit);
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
        let factory = unreachable_split_search();
        let opts = HashMap::new();
        let err = PkVectorOrchestrator::new(reader)
            .search_candidates(
                &splits,
                &[0.0, 0.0],
                VectorSearchMetric::L2,
                0,
                0,
                None,
                &factory,
                &opts,
                false,
                None,
                1,
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
        let factory = unreachable_split_search();
        let opts = HashMap::new();
        let err = PkVectorOrchestrator::new(reader)
            .search_candidates(
                &splits,
                &[],
                VectorSearchMetric::L2,
                5,
                5,
                None,
                &factory,
                &opts,
                false,
                None,
                1,
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
        let factory = file_array_search(|file_name| match file_name {
            "exact.mosaic" => vec![Some(vec![1.0, 0.0]), Some(vec![2.0, 0.0])],
            other => panic!("unexpected exact scan of covered file {other}"),
        });
        let opts = HashMap::new();
        let batches = materialize_via_splits(
            make_reader(file_io, table_path),
            &[split],
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            3,
            Some(&ann),
            &factory,
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
            collect_f32(&batches, SEARCH_SCORE_COLUMN),
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
        let factory = file_array_search(|file_name| match file_name {
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
        });
        let opts = HashMap::new();
        let batches = materialize_via_splits(
            make_reader(file_io, table_path),
            &[split0, split1],
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            3,
            None,
            &factory,
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
            collect_f32(&batches, SEARCH_SCORE_COLUMN),
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
        let factory = file_array_search(|file_name| match file_name {
            "d.mosaic" => vec![
                Some(vec![1.0, 0.0]),
                Some(vec![2.0, 0.0]),
                Some(vec![3.0, 0.0]),
                Some(vec![0.0, 0.0]),
            ],
            other => panic!("unexpected file {other}"),
        });
        let opts = HashMap::new();
        let batches = materialize_via_splits(
            make_reader(file_io, table_path),
            &[split],
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            4,
            None,
            &factory,
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
            collect_f32(&batches, SEARCH_SCORE_COLUMN),
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
        let factory = file_array_search(|file_name| match file_name {
            "o.mosaic" => vec![
                Some(vec![3.0, 0.0]),
                Some(vec![1.0, 0.0]),
                Some(vec![2.0, 0.0]),
            ],
            other => panic!("unexpected file {other}"),
        });
        let opts = HashMap::new();
        let batches = materialize_via_splits(
            make_reader(file_io, table_path),
            &[split],
            &[0.0, 0.0],
            VectorSearchMetric::L2,
            3,
            None,
            &factory,
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
            collect_f32(&batches, SEARCH_SCORE_COLUMN),
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
        let factory = as_split_search(
            |_: usize,
             _: &PkVectorSearchSplit,
             file: &BucketActiveFile,
             queries: &[&[f32]],
             metric: VectorSearchMetric,
             exact_limit: usize,
             is_excluded: &(dyn Fn(i64) -> bool + Sync)|
             -> ExactFileSearchFuture<'_> {
                assert_eq!(file.file_name, "c.mosaic");
                let file_name = file.file_name.clone();
                let query = queries[0].to_vec();
                Box::pin(async move {
                    let mut reader = ArrayReader::new(
                        2,
                        vec![
                            Some(vec![3.0, 0.0]),
                            Some(vec![1.0, 0.0]),
                            Some(vec![2.0, 0.0]),
                        ],
                    );
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
        let opts = HashMap::new();
        let result = PkVectorOrchestrator::new(make_reader(file_io, table_path))
            .search_candidates(
                &[split],
                &[0.0, 0.0],
                VectorSearchMetric::L2,
                2,
                2,
                None,
                &factory,
                &opts,
                false,
                None,
                1,
            )
            .await
            .unwrap();
        // Merge the two bounded lists into the best-first survivors.
        let cands = merge_candidates(result.indexed, result.exact, 2);
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
    async fn search_candidates_applies_residual_ranges() {
        // Same single bucket / three rows as above, but a per-split residual map
        // allows only physical positions {0, 2}. The best recalled hit (pos1,
        // d=1) is filtered out; the survivors are the allowed positions in
        // best-first order: pos2 (d=4) then pos0 (d=9). This proves the residual
        // allow-list is threaded through to the bucket search rather than merely
        // stored.
        let table_path = "memory:/pkvo_residual";
        let bucket_path = format!("{table_path}/bucket-0");
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let meta = write_file(&file_io, &bucket_path, "r.mosaic", vec![1, 2, 3]).await;
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
            active_files: vec![active("r.mosaic", 3)],
        };
        // pos0 {3,0} d=9, pos1 {1,0} d=1, pos2 {2,0} d=4.
        let factory = as_split_search(
            |_: usize,
             _: &PkVectorSearchSplit,
             file: &BucketActiveFile,
             queries: &[&[f32]],
             metric: VectorSearchMetric,
             exact_limit: usize,
             is_excluded: &(dyn Fn(i64) -> bool + Sync)|
             -> ExactFileSearchFuture<'_> {
                assert_eq!(file.file_name, "r.mosaic");
                let file_name = file.file_name.clone();
                let query = queries[0].to_vec();
                Box::pin(async move {
                    let mut reader = ArrayReader::new(
                        2,
                        vec![
                            Some(vec![3.0, 0.0]),
                            Some(vec![1.0, 0.0]),
                            Some(vec![2.0, 0.0]),
                        ],
                    );
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
        // Allow only positions 0 and 2 for "r.mosaic"; pos1 (the best hit) is
        // excluded by the residual.
        let mut allowed = RoaringTreemap::new();
        allowed.insert(0);
        allowed.insert(2);
        let residual_by_split = vec![HashMap::from([("r.mosaic".to_string(), allowed)])];
        let opts = HashMap::new();
        let result = PkVectorOrchestrator::new(make_reader(file_io, table_path))
            .search_candidates(
                &[split],
                &[0.0, 0.0],
                VectorSearchMetric::L2,
                3,
                3,
                None,
                &factory,
                &opts,
                false,
                Some(&residual_by_split),
                1,
            )
            .await
            .unwrap();
        // Merge the two bounded lists into the best-first survivors.
        let cands = merge_candidates(result.indexed, result.exact, 3);
        // Best-first among allowed positions: pos2 (d=4) then pos0 (d=9).
        assert_eq!(
            cands
                .iter()
                .map(|c| (c.row_position, c.distance))
                .collect::<Vec<_>>(),
            vec![(2, 4.0), (0, 9.0)]
        );
    }

    #[tokio::test]
    async fn search_candidates_rejects_residual_length_mismatch() {
        // A residual slice whose length differs from the split count is a caller
        // bug (the map is indexed by split); it must fail loud rather than panic
        // or silently misalign.
        let table_path = "memory:/pkvo_residual_mismatch";
        let bucket_path = format!("{table_path}/bucket-0");
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let meta = write_file(&file_io, &bucket_path, "m.mosaic", vec![1, 2]).await;
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
            active_files: vec![active("m.mosaic", 2)],
        };
        let factory = unreachable_split_search();
        // Two residual maps for a single split.
        let residual_by_split: Vec<HashMap<String, RoaringTreemap>> =
            vec![HashMap::new(), HashMap::new()];
        let opts = HashMap::new();
        let err = PkVectorOrchestrator::new(make_reader(file_io, table_path))
            .search_candidates(
                &[split],
                &[0.0, 0.0],
                VectorSearchMetric::L2,
                3,
                3,
                None,
                &factory,
                &opts,
                false,
                Some(&residual_by_split),
                1,
            )
            .await
            .map(|_| ())
            .expect_err("residual length mismatch must fail loud");
        assert!(
            format!("{err:?}").contains("does not match split count"),
            "got: {err:?}"
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
        let factory = unreachable_split_search();
        let opts = HashMap::new();
        let result = PkVectorOrchestrator::new(make_reader(file_io, table_path))
            .search_candidates(
                &[split],
                &[0.0, 0.0],
                VectorSearchMetric::L2,
                2,
                2,
                None,
                &factory,
                &opts,
                true,
                None,
                1,
            )
            .await
            .unwrap();
        assert!(result.indexed.is_empty());
        assert!(result.exact.is_empty());
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

    /// A split-scoped multi-query search closure running `exact_search` per query
    /// over a fresh `ArrayReader` of `vectors`, returning one list per query. Used
    /// by the batch orchestrator tests.
    #[allow(clippy::type_complexity)]
    fn split_array_search_batch(
        vectors: Vec<Option<Vec<f32>>>,
    ) -> impl for<'s, 'a> Fn(
        usize,
        &'s PkVectorSearchSplit,
        &'a BucketActiveFile,
        &'a [&'a [f32]],
        VectorSearchMetric,
        usize,
        &'a (dyn Fn(i64) -> bool + Sync),
    ) -> ExactFileSearchFuture<'a>
           + Send
           + Sync {
        let vectors = Arc::new(vectors);
        as_split_search(
            move |_: usize,
                  _: &PkVectorSearchSplit,
                  file: &BucketActiveFile,
                  queries: &[&[f32]],
                  metric: VectorSearchMetric,
                  exact_limit: usize,
                  is_excluded: &(dyn Fn(i64) -> bool + Sync)|
                  -> ExactFileSearchFuture<'_> {
                let dimension = vectors
                    .first()
                    .and_then(|v| v.as_ref())
                    .map_or(2, |v| v.len());
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
    async fn search_candidates_batch_of_one_equals_single() {
        // A batch-of-one must equal the single-query `search_candidates` exactly.
        let table_path = "memory:/pkvo_batch_one";
        let bucket_path = format!("{table_path}/bucket-0");
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let meta = write_file(&file_io, &bucket_path, "c.mosaic", vec![1, 2, 3]).await;
        let make_split = || PkVectorSearchSplit {
            data_split: DataSplitBuilder::new()
                .with_snapshot(1)
                .with_partition(BinaryRow::new(0))
                .with_bucket(0)
                .with_bucket_path(bucket_path.clone())
                .with_total_buckets(1)
                .with_data_files(vec![meta.clone()])
                .build()
                .unwrap(),
            ann_segments: Vec::new(),
            active_files: vec![active("c.mosaic", 3)],
        };
        // pos0 {3,0} d=9, pos1 {1,0} d=1, pos2 {2,0} d=4.
        let vectors = vec![
            Some(vec![3.0, 0.0]),
            Some(vec![1.0, 0.0]),
            Some(vec![2.0, 0.0]),
        ];
        let query: &[f32] = &[0.0, 0.0];
        let opts = HashMap::new();

        let single = PkVectorOrchestrator::new(make_reader(file_io.clone(), table_path))
            .search_candidates(
                &[make_split()],
                query,
                VectorSearchMetric::L2,
                2,
                2,
                None,
                &split_array_search_batch(vectors.clone()),
                &opts,
                false,
                None,
                1,
            )
            .await
            .unwrap();

        let batch = PkVectorOrchestrator::new(make_reader(file_io, table_path))
            .search_candidates_batch(
                &[make_split()],
                &[query],
                VectorSearchMetric::L2,
                2,
                2,
                None,
                &split_array_search_batch(vectors),
                &opts,
                false,
                None,
                1,
            )
            .await
            .unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(
            batch[0]
                .exact
                .iter()
                .map(|c| (c.row_position, c.distance))
                .collect::<Vec<_>>(),
            single
                .exact
                .iter()
                .map(|c| (c.row_position, c.distance))
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn search_candidates_batch_per_query_results_independent() {
        // Two queries over one exact file; each query's Top-K comes from its own
        // heap with no cross-query bleed. q0 nearest pos1 (x=1); q1 nearest pos0
        // (x=3).
        let table_path = "memory:/pkvo_batch_indep";
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
        // pos0 {3,0}, pos1 {1,0}, pos2 {2,0}.
        let vectors = vec![
            Some(vec![3.0, 0.0]),
            Some(vec![1.0, 0.0]),
            Some(vec![2.0, 0.0]),
        ];
        let q0: &[f32] = &[1.0, 0.0]; // nearest pos1 (dist 0)
        let q1: &[f32] = &[3.0, 0.0]; // nearest pos0 (dist 0)
        let opts = HashMap::new();
        let out = PkVectorOrchestrator::new(make_reader(file_io, table_path))
            .search_candidates_batch(
                &[split],
                &[q0, q1],
                VectorSearchMetric::L2,
                1,
                1,
                None,
                &split_array_search_batch(vectors),
                &opts,
                false,
                None,
                1,
            )
            .await
            .unwrap();
        assert_eq!(out.len(), 2);
        let m0 = merge_candidates(out[0].indexed.clone(), out[0].exact.clone(), 1);
        let m1 = merge_candidates(out[1].indexed.clone(), out[1].exact.clone(), 1);
        assert_eq!(m0.len(), 1);
        assert_eq!(m0[0].row_position, 1);
        assert_eq!(m1.len(), 1);
        assert_eq!(m1[0].row_position, 0);
    }

    #[tokio::test]
    async fn search_candidates_bucket_order_is_sequential_at_concurrency_one() {
        // At concurrency == 1 the per-bucket loop must search buckets in strict
        // split order. A split-scoped recording closure captures the visited
        // split_index sequence; it must match the split order deterministically.
        let table_path = "memory:/pkvo_bucket_order";
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let mut splits = Vec::new();
        for bucket in 0..4i32 {
            let bucket_path = format!("{table_path}/bucket-{bucket}");
            let meta = write_file(&file_io, &bucket_path, "d.mosaic", vec![bucket]).await;
            splits.push(PkVectorSearchSplit {
                data_split: DataSplitBuilder::new()
                    .with_snapshot(1)
                    .with_partition(BinaryRow::new(0))
                    .with_bucket(bucket)
                    .with_bucket_path(bucket_path)
                    .with_total_buckets(4)
                    .with_data_files(vec![meta])
                    .build()
                    .unwrap(),
                ann_segments: Vec::new(),
                active_files: vec![active("d.mosaic", 1)],
            });
        }
        let visited = std::sync::Mutex::new(Vec::<usize>::new());
        let factory = as_split_search(
            |split_index: usize,
             _: &PkVectorSearchSplit,
             file: &BucketActiveFile,
             queries: &[&[f32]],
             metric: VectorSearchMetric,
             exact_limit: usize,
             is_excluded: &(dyn Fn(i64) -> bool + Sync)|
             -> ExactFileSearchFuture<'_> {
                visited.lock().unwrap().push(split_index);
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
        let opts = HashMap::new();
        PkVectorOrchestrator::new(make_reader(file_io, table_path))
            .search_candidates(
                &splits,
                &[0.0, 0.0],
                VectorSearchMetric::L2,
                8,
                8,
                None,
                &factory,
                &opts,
                false,
                None,
                1,
            )
            .await
            .unwrap();
        assert_eq!(
            visited.lock().unwrap().as_slice(),
            &[0usize, 1, 2, 3],
            "concurrency == 1 must search buckets in strict split order"
        );
    }

    #[tokio::test]
    async fn search_candidates_serial_equals_parallel_with_ties() {
        // Multiple buckets whose exact candidates share a distance (a tie decided by
        // the cross-bucket candidate_cmp) plus NaN-distance candidates that must sort
        // last. Running at concurrency == 1 and concurrency > 1 must yield the same
        // deterministic best-first survivors regardless of bucket completion order.
        let table_path = "memory:/pkvo_serial_parallel";
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let negative_nan = f32::from_bits(0xffc00000);
        let mut splits = Vec::new();
        for bucket in 0..4i32 {
            let bucket_path = format!("{table_path}/bucket-{bucket}");
            let meta = write_file(&file_io, &bucket_path, "d.mosaic", vec![bucket, bucket]).await;
            splits.push(PkVectorSearchSplit {
                data_split: DataSplitBuilder::new()
                    .with_snapshot(1)
                    .with_partition(BinaryRow::new(0))
                    .with_bucket(bucket)
                    .with_bucket_path(bucket_path)
                    .with_total_buckets(4)
                    .with_data_files(vec![meta])
                    .build()
                    .unwrap(),
                ann_segments: Vec::new(),
                active_files: vec![active("d.mosaic", 2)],
            });
        }
        // Each bucket's exact file yields the same two candidates: pos0 distance 1.0
        // (tied across buckets) and pos1 a NaN distance (must sort last). Lower
        // split_index buckets yield more, so they complete last under
        // buffer_unordered.
        let factory = as_split_search(
            move |split_index: usize,
                  _: &PkVectorSearchSplit,
                  file: &BucketActiveFile,
                  _: &[&[f32]],
                  _: VectorSearchMetric,
                  _: usize,
                  _: &(dyn Fn(i64) -> bool + Sync)|
                  -> ExactFileSearchFuture<'_> {
                let file_name = file.file_name.clone();
                let yields = (4 - split_index) * 2;
                Box::pin(async move {
                    for _ in 0..yields {
                        tokio::task::yield_now().await;
                    }
                    Ok(vec![vec![
                        PkVectorSearchResult {
                            data_file_name: file_name.clone(),
                            row_position: 0,
                            distance: 1.0,
                        },
                        PkVectorSearchResult {
                            data_file_name: file_name,
                            row_position: 1,
                            distance: negative_nan,
                        },
                    ]])
                })
            },
        );
        let opts = HashMap::new();
        let serial_result = PkVectorOrchestrator::new(make_reader(file_io.clone(), table_path))
            .search_candidates(
                &splits,
                &[0.0, 0.0],
                VectorSearchMetric::L2,
                8,
                8,
                None,
                &factory,
                &opts,
                false,
                None,
                1,
            )
            .await
            .unwrap();
        let serial: Vec<(i32, i64, bool)> =
            merge_candidates(serial_result.indexed, serial_result.exact, 8)
                .iter()
                .map(|c| (c.bucket, c.row_position, c.distance.is_nan()))
                .collect();
        let parallel_result = PkVectorOrchestrator::new(make_reader(file_io, table_path))
            .search_candidates(
                &splits,
                &[0.0, 0.0],
                VectorSearchMetric::L2,
                8,
                8,
                None,
                &factory,
                &opts,
                false,
                None,
                4,
            )
            .await
            .unwrap();
        let parallel: Vec<(i32, i64, bool)> =
            merge_candidates(parallel_result.indexed, parallel_result.exact, 8)
                .iter()
                .map(|c| (c.bucket, c.row_position, c.distance.is_nan()))
                .collect();
        // Deterministic order: four tied distance-1.0 candidates by bucket asc, then
        // four NaN candidates by bucket asc.
        assert_eq!(
            serial,
            vec![
                (0, 0, false),
                (1, 0, false),
                (2, 0, false),
                (3, 0, false),
                (0, 1, true),
                (1, 1, true),
                (2, 1, true),
                (3, 1, true),
            ]
        );
        assert_eq!(
            parallel, serial,
            "parallel survivors must equal serial survivors"
        );
    }

    #[tokio::test]
    async fn search_candidates_peak_concurrency_capped_across_buckets() {
        // Reproduces the reviewer's scenario: 2 buckets x 2 exact files with
        // concurrency = 2. The per-bucket and per-exact-file fan-outs draw from ONE
        // shared budget, so at most `concurrency` (2) exact-file searches run at once
        // across ALL buckets. Before the shared budget each level capped at N
        // independently, so 2 buckets x 2 files = 4 file searches ran simultaneously.
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let table_path = "memory:/pkvo_peak_concurrency";
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let mut splits = Vec::new();
        for bucket in 0..2i32 {
            let bucket_path = format!("{table_path}/bucket-{bucket}");
            let meta_a = write_file(&file_io, &bucket_path, "a.mosaic", vec![bucket]).await;
            let meta_b = write_file(&file_io, &bucket_path, "b.mosaic", vec![bucket]).await;
            splits.push(PkVectorSearchSplit {
                data_split: DataSplitBuilder::new()
                    .with_snapshot(1)
                    .with_partition(BinaryRow::new(0))
                    .with_bucket(bucket)
                    .with_bucket_path(bucket_path)
                    .with_total_buckets(2)
                    .with_data_files(vec![meta_a, meta_b])
                    .build()
                    .unwrap(),
                ann_segments: Vec::new(),
                active_files: vec![active("a.mosaic", 1), active("b.mosaic", 1)],
            });
        }

        // Shared (current, peak) in-flight counters. Each exact-file search increments
        // on entry (after acquiring its budget slot), yields so overlapping searches
        // are observable, then decrements; `peak` is the max simultaneous count.
        let counters = Arc::new((AtomicUsize::new(0), AtomicUsize::new(0)));
        let counters_in_closure = counters.clone();
        let factory = as_split_search(
            move |_: usize,
                  _: &PkVectorSearchSplit,
                  file: &BucketActiveFile,
                  _: &[&[f32]],
                  _: VectorSearchMetric,
                  _: usize,
                  _: &(dyn Fn(i64) -> bool + Sync)|
                  -> ExactFileSearchFuture<'_> {
                let counters = counters_in_closure.clone();
                let file_name = file.file_name.clone();
                Box::pin(async move {
                    let current = counters.0.fetch_add(1, Ordering::SeqCst) + 1;
                    counters.1.fetch_max(current, Ordering::SeqCst);
                    for _ in 0..8 {
                        tokio::task::yield_now().await;
                    }
                    counters.0.fetch_sub(1, Ordering::SeqCst);
                    Ok(vec![vec![PkVectorSearchResult {
                        data_file_name: file_name,
                        row_position: 0,
                        distance: 1.0,
                    }]])
                })
            },
        );

        let opts = HashMap::new();
        PkVectorOrchestrator::new(make_reader(file_io, table_path))
            .search_candidates(
                &splits,
                &[0.0],
                VectorSearchMetric::L2,
                8,
                8,
                None,
                &factory,
                &opts,
                false,
                None,
                2,
            )
            .await
            .unwrap();

        let peak = counters.1.load(Ordering::SeqCst);
        assert!(
            peak <= 2,
            "shared budget must cap concurrent exact-file searches at concurrency (2); \
             observed peak {peak} (independent per-level fan-out would reach 4)"
        );
        assert!(
            peak >= 2,
            "test must actually exercise cross-bucket overlap; observed peak {peak}"
        );
    }
}
