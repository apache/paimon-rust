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

use bytes::Bytes;
use futures::future::BoxFuture;

use super::bucket::BucketAnnSegment;
use super::data_invalid;
use super::metric::{java_float_compare, VectorSearchMetric};
use super::result::PkVectorSearchResult;
use super::{FileRowSelection, FileRowSelections};
use crate::deletion_vector::DeletionVector;
use crate::spec::{
    PrimaryKeyIndexSourceFile as PkVectorSourceFile,
    PrimaryKeyIndexSourceMeta as PkVectorSourceMeta,
};
use crate::vector_search::VectorSearch;
use crate::vindex::range_reader::VindexFileReader;

/// Build the live-row-id mask for the ANN reader's `include_row_ids` filter, in
/// segment-ordinal space (source files concatenated in order). Mirrors Java
/// `PkVectorAnnSegmentSearcher.liveRowPositions`.
///
/// Only source files present in `active_source_files` contribute live ordinals;
/// inactive sources' ordinal ranges are masked out entirely (their rows are no
/// longer readable in this snapshot). Deletion vectors are applied only to active
/// sources.
///
/// The most live row ids one ANN segment's mask may hold.
///
/// Mirrors Java `LuminaVectorGlobalIndexReader.toScopedIds`, which refuses an
/// include-set above `Integer.MAX_VALUE` before allocating the dense array the
/// backend is handed. The number is Paimon's own for this exact quantity at this
/// exact seam, not one picked here.
///
/// The limit is on what goes INTO the mask, charged before each insertion, so
/// nothing is ever allocated for a span that could not be handed over anyway. That
/// is what makes it a bound rather than a complaint: on the bucket-split route the
/// source row counts arrive on the wire (`PrimaryKeyIndexSourceMeta` is decoded from
/// the split's own `GlobalIndexMeta`, and only its sign is checked), and once
/// anything makes a mask necessary an unrestricted file is inserted wholesale at
/// whatever size that count claims -- `i64::MAX` never returns.
///
/// Because it counts insertions, a segment that CLAIMS an impossible number of rows
/// but puts few or none in the mask -- all sources inactive, an explicitly empty
/// selection, a narrow range -- is not turned away. It is marginally stricter than
/// Java in one direction: Java checks the set after subtracting deletion vectors,
/// while the allocation being bounded here happens as rows go in.
const MAX_LIVE_ROW_IDS: u64 = i32::MAX as u64;

/// Charge `rows` against what is left of [`MAX_LIVE_ROW_IDS`], before they are
/// inserted.
fn charge_live_rows(remaining: &mut u64, rows: u64) -> crate::Result<()> {
    *remaining = remaining.checked_sub(rows).ok_or_else(|| {
        data_invalid(format!(
            "vector search would filter more than {MAX_LIVE_ROW_IDS} live rows in one \
             ANN segment"
        ))
    })?;
    Ok(())
}

/// `row_selections` restricts each source file to the rows a pre-filter allows,
/// keyed by data-file name. A file with **no entry is unrestricted**, an empty
/// entry excludes it, and a non-empty one limits it — see [`FileRowSelection`].
/// Mirrors Java `rowRangesByFile`.
///
/// Returns `None` when nothing is restricted, every source file is active, AND no
/// deletion vector is relevant — nothing to mask, so the ANN backend searches
/// unfiltered. Otherwise returns the masked live ids.
///
/// Java's condition is `allSourcesActive && deletionVectors.isEmpty() &&
/// rowRangesByFile.isEmpty()`. The selections half is mirrored exactly (whole-map,
/// not this segment's own files). The deletion-vector half is NOT: ours is
/// segment-local, so a deletion vector on a file this segment does not index leaves
/// it unfiltered where Java would mask it. That predates the bucket-split route and
/// applies to the manifest route too.
pub(crate) fn build_live_row_ids(
    source_files: &[PkVectorSourceFile],
    active_source_files: &HashSet<String>,
    deletion_vectors: &HashMap<String, Arc<DeletionVector>>,
    row_selections: Option<&FileRowSelections>,
) -> crate::Result<Option<roaring::RoaringTreemap>> {
    let all_active = source_files
        .iter()
        .all(|f| active_source_files.contains(f.file_name()));
    let has_relevant_dv = source_files
        .iter()
        .any(|f| deletion_vectors.contains_key(f.file_name()));
    // Java's own condition, `rowRangesByFile.isEmpty()`, over the whole bucket-level
    // map. Narrowing it to "no entry for one of THIS segment's sources" would leave
    // more segments unfiltered, but it also changes which backend entry point they
    // take (`search` vs `search_with_filter`), and those can differ in recall. Not
    // worth diverging for.
    let nothing_restricted = row_selections.is_none_or(FileRowSelections::is_empty);
    if nothing_restricted && all_active && !has_relevant_dv {
        return Ok(None);
    }

    let mut live = roaring::RoaringTreemap::new();
    let mut deleted = roaring::RoaringTreemap::new();
    let mut file_offset: u64 = 0;
    let mut budget = MAX_LIVE_ROW_IDS;
    for source_file in source_files {
        let row_count = u64::try_from(source_file.row_count())
            .map_err(|_| data_invalid("vector source row count must not be negative"))?;
        let end = file_offset
            .checked_add(row_count)
            .ok_or_else(|| data_invalid("vector source row counts overflow u64"))?;
        let active = active_source_files.contains(source_file.file_name());
        if active && row_count > 0 {
            match row_selections.and_then(|selections| selections.get(source_file.file_name())) {
                // Unrestricted: the whole active file range is live. This is the
                // no-entry case Java spells as `rowRanges == null`.
                None => {
                    charge_live_rows(&mut budget, row_count)?;
                    live.insert_range(file_offset..end);
                }
                // Restricted to intervals. Added interval-wise, never position-wise:
                // these bounds ride in on an engine-supplied split, so walking them
                // would be unbounded work driven by untrusted numbers. Mirrors Java
                // `live.addRange(range.addOffset(fileOffset))`.
                Some(FileRowSelection::Ranges(ranges)) => {
                    for range in ranges {
                        // Java checks each range against the SOURCE file's row count.
                        // On the bucket-split route that count came off the wire as
                        // well (`PrimaryKeyIndexSourceMeta` is decoded from the
                        // split's own `GlobalIndexMeta`), so this rejects a range
                        // that disagrees with its own segment -- it is not a
                        // resource bound.
                        let from = u64::try_from(range.from()).map_err(|_| {
                            data_invalid("vector pre-filter range bound must not be negative")
                        })?;
                        let to = u64::try_from(range.to()).map_err(|_| {
                            data_invalid("vector pre-filter range bound must not be negative")
                        })?;
                        if to >= row_count {
                            return Err(data_invalid(format!(
                                "pre-filter range [{from}, {to}] is out of range for source file {} ({} rows)",
                                source_file.file_name(),
                                row_count
                            )));
                        }
                        charge_live_rows(&mut budget, to - from + 1)?;
                        live.insert_range((file_offset + from)..=(file_offset + to));
                    }
                }
                // Restricted to positions a residual predicate left behind. Bounded
                // by the rows that read actually returned, so walking them is safe.
                Some(FileRowSelection::Positions(allowed)) => {
                    // `len` plus a maximum of `row_count - 1` can only describe the
                    // full set; inserting it as one range subsumes the per-position
                    // bound check below.
                    if allowed.len() == row_count && allowed.max() == Some(row_count - 1) {
                        charge_live_rows(&mut budget, row_count)?;
                        live.insert_range(file_offset..end);
                    } else {
                        charge_live_rows(&mut budget, allowed.len())?;
                        for position in allowed.iter() {
                            if position >= row_count {
                                return Err(data_invalid(format!(
                                    "residual position {position} is out of range for source file {} ({} rows)",
                                    source_file.file_name(),
                                    row_count
                                )));
                            }
                            let global = file_offset.checked_add(position).ok_or_else(|| {
                                data_invalid("vector residual position overflows u64")
                            })?;
                            live.insert(global);
                        }
                    }
                }
            }
        }
        if active {
            if let Some(dv) = deletion_vectors.get(source_file.file_name()) {
                for position in dv.iter() {
                    // Bound the position against ITS OWN source file, as the residual
                    // path above already does. Source files share one ordinal space
                    // (`global = file_offset + position`), so a position past this
                    // file's rows does not fall out of the mask -- it lands inside the
                    // NEXT source file's range and deletes one of that file's rows.
                    if position >= row_count {
                        return Err(data_invalid(format!(
                            "deleted position {position} is out of range for source file {} ({} rows)",
                            source_file.file_name(),
                            row_count
                        )));
                    }
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
    row_selections: Option<&FileRowSelections>,
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
        let pos = u64::try_from(row_position)
            .map_err(|_| data_invalid("resolved row position must not be negative"))?;
        if let Some(dv) = deletion_vectors.get(&data_file_name) {
            if dv.is_deleted(pos) {
                return Err(data_invalid(format!(
                    "ANN segment returned snapshot-deleted row position {row_position} in {data_file_name}"
                )));
            }
        }
        // A file with no entry is unrestricted, so only an entry can reject.
        if let Some(selection) =
            row_selections.and_then(|selections| selections.get(&data_file_name))
        {
            if !selection.contains(pos) {
                return Err(data_invalid(format!(
                    "ANN segment returned row position {row_position} in {data_file_name} outside the row selection for that file"
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
/// `Send + Sync` so an `Arc<dyn PkVectorAnnSearcher>` can be cloned into concurrent
/// leaf futures and moved onto the dedicated global-index executor.
pub(crate) trait PkVectorAnnSearcher: Send + Sync {
    /// Load one buffered ANN segment. Runs on the async side of the bucket leaf,
    /// BEFORE the blocking score, so the bytes are loaded lazily per segment and
    /// dropped after that leaf.
    /// Returns a `'static` boxed future so it borrows nothing from `self`/`segment`
    /// past the await (production clones `FileIO` + the path into the future).
    fn load_segment(&self, segment: &BucketAnnSegment) -> BoxFuture<'static, crate::Result<Bytes>>;

    /// Production source loader. Must NOT acquire a search-concurrency permit:
    /// the bucket leaf already holds one across both this load and the subsequent
    /// score, so a second acquisition would deadlock at capacity 1.
    fn load_segment_source(
        &self,
        segment: &BucketAnnSegment,
    ) -> BoxFuture<'static, crate::Result<AnnSegmentSource>> {
        let future = self.load_segment(segment);
        Box::pin(async move { future.await.map(AnnSegmentSource::Buffered) })
    }

    /// Search one ANN segment for a batch of query vectors, returning one
    /// BEST_FIRST result list per query (outer index aligned to `queries`). The
    /// live-row mask (selections ∩ DV) is query-independent, so it is built once and
    /// then cloned into each query; only the per-query scores differ. Buffered callers
    /// pass the bytes from `load_segment` by value so they cannot outlive the leaf.
    #[allow(clippy::too_many_arguments)]
    fn search_batch(
        &self,
        segment: &BucketAnnSegment,
        segment_bytes: Bytes,
        queries: &[&[f32]],
        metric: VectorSearchMetric,
        limit: usize,
        active_source_files: &HashSet<String>,
        deletion_vectors: &HashMap<String, Arc<DeletionVector>>,
        search_options: &HashMap<String, String>,
        row_selections: Option<&FileRowSelections>,
    ) -> crate::Result<Vec<Vec<PkVectorSearchResult>>>;

    #[allow(clippy::too_many_arguments)]
    fn search_batch_source(
        &self,
        segment: &BucketAnnSegment,
        segment_source: AnnSegmentSource,
        queries: &[&[f32]],
        metric: VectorSearchMetric,
        limit: usize,
        active_source_files: &HashSet<String>,
        deletion_vectors: &HashMap<String, Arc<DeletionVector>>,
        search_options: &HashMap<String, String>,
        row_selections: Option<&FileRowSelections>,
    ) -> crate::Result<Vec<Vec<PkVectorSearchResult>>> {
        match segment_source {
            AnnSegmentSource::Buffered(bytes) => self.search_batch(
                segment,
                bytes,
                queries,
                metric,
                limit,
                active_source_files,
                deletion_vectors,
                search_options,
                row_selections,
            ),
            AnnSegmentSource::Vindex(_) => Err(data_invalid(
                "ANN searcher does not support a range-backed segment source",
            )),
        }
    }

    /// Single-query wrapper over `search_batch`: searches the one query and
    /// returns its result list. Asserts the batch produced exactly one list.
    #[allow(dead_code)]
    #[allow(clippy::too_many_arguments)]
    fn search(
        &self,
        segment: &BucketAnnSegment,
        segment_bytes: Bytes,
        query: &[f32],
        metric: VectorSearchMetric,
        limit: usize,
        active_source_files: &HashSet<String>,
        deletion_vectors: &HashMap<String, Arc<DeletionVector>>,
        search_options: &HashMap<String, String>,
        row_selections: Option<&FileRowSelections>,
    ) -> crate::Result<Vec<PkVectorSearchResult>> {
        let mut results = self.search_batch(
            segment,
            segment_bytes,
            &[query],
            metric,
            limit,
            active_source_files,
            deletion_vectors,
            search_options,
            row_selections,
        )?;
        if results.len() != 1 {
            return Err(data_invalid(format!(
                "ANN batch search returned {} result lists for a single query",
                results.len()
            )));
        }
        Ok(results.pop().expect("length checked to be 1"))
    }

    #[allow(clippy::too_many_arguments)]
    fn search_source(
        &self,
        segment: &BucketAnnSegment,
        segment_source: AnnSegmentSource,
        query: &[f32],
        metric: VectorSearchMetric,
        limit: usize,
        active_source_files: &HashSet<String>,
        deletion_vectors: &HashMap<String, Arc<DeletionVector>>,
        search_options: &HashMap<String, String>,
        row_selections: Option<&FileRowSelections>,
    ) -> crate::Result<Vec<PkVectorSearchResult>> {
        let mut results = self.search_batch_source(
            segment,
            segment_source,
            &[query],
            metric,
            limit,
            active_source_files,
            deletion_vectors,
            search_options,
            row_selections,
        )?;
        if results.len() != 1 {
            return Err(data_invalid(format!(
                "ANN batch search returned {} result lists for a single query",
                results.len()
            )));
        }
        Ok(results.pop().expect("length checked to be 1"))
    }
}

/// Batch scorer seam: drives the underlying vindex ANN reader for a batch of
/// searches over ONE segment, opening the reader once and issuing one backend batch
/// search (mirroring Java's shared-reader `visitBatchVectorSearch`). Returns
/// one `ordinal -> score` map (higher-is-better) per input search, aligned to the
/// `searches` slice. Any negative labels are skipped by the existing `vindex`
/// reader (`collect_results` drops `row_id < 0`), so this seam only ever yields
/// non-negative `u64` ordinals — no signed-label handling is needed downstream.
///
/// The production scorer drives one backend reader from a typed segment source;
/// tests inject a synthetic buffered scorer. The adapter's
/// own logic (live-row masking, ordinal mapping, deletion checks, ordering) is
/// exercised independently of the scorer.
#[cfg(test)]
pub(crate) type BatchScorer = Box<
    dyn Fn(
            &BucketAnnSegment,
            Bytes,
            &[VectorSearch],
        ) -> crate::Result<Vec<Option<HashMap<u64, f32>>>>
        + Send
        + Sync,
>;

pub(crate) type SourceBatchScorer = Box<
    dyn Fn(
            &BucketAnnSegment,
            AnnSegmentSource,
            &[VectorSearch],
        ) -> crate::Result<Vec<Option<HashMap<u64, f32>>>>
        + Send
        + Sync,
>;

pub(crate) enum AnnSegmentSource {
    Buffered(Bytes),
    Vindex(VindexFileReader),
}

/// Test-only buffered loader retained for simple fake searchers.
#[cfg(test)]
pub(crate) type SegmentLoader =
    Box<dyn Fn(&BucketAnnSegment) -> BoxFuture<'static, crate::Result<Bytes>> + Send + Sync>;

/// Production loader for either buffered Lumina data or a range-backed vindex
/// source. It runs before the dedicated-executor score and must not acquire a
/// second search permit; the bucket leaf already holds one across both phases.
pub(crate) type SourceSegmentLoader = Box<
    dyn Fn(&BucketAnnSegment) -> BoxFuture<'static, crate::Result<AnnSegmentSource>> + Send + Sync,
>;

/// Structural ANN-backed `PkVectorAnnSearcher`. Composes the pure helpers
/// (`build_live_row_ids`, `map_ann_results`) around the batch scorer seam, and
/// carries the async segment loader so each segment source is opened lazily in its
/// own bucket leaf and dropped after scoring (no up-front all-segments map).
pub(crate) struct VindexAnnSearcher {
    field_name: String,
    scorer: SourceBatchScorer,
    loader: SourceSegmentLoader,
}

impl VindexAnnSearcher {
    #[cfg(test)]
    pub(crate) fn new(field_name: String, scorer: BatchScorer, loader: SegmentLoader) -> Self {
        let source_scorer: SourceBatchScorer =
            Box::new(move |segment, source, searches| match source {
                AnnSegmentSource::Buffered(bytes) => scorer(segment, bytes, searches),
                AnnSegmentSource::Vindex(_) => Err(data_invalid(
                    "buffered ANN scorer received a range-backed segment source",
                )),
            });
        let source_loader: SourceSegmentLoader = Box::new(move |segment| {
            let future = loader(segment);
            Box::pin(async move { future.await.map(AnnSegmentSource::Buffered) })
        });
        Self::new_with_source(field_name, source_scorer, source_loader)
    }

    pub(crate) fn new_with_source(
        field_name: String,
        scorer: SourceBatchScorer,
        loader: SourceSegmentLoader,
    ) -> Self {
        Self {
            field_name,
            scorer,
            loader,
        }
    }
}

impl PkVectorAnnSearcher for VindexAnnSearcher {
    fn load_segment(&self, segment: &BucketAnnSegment) -> BoxFuture<'static, crate::Result<Bytes>> {
        let future = (self.loader)(segment);
        Box::pin(async move {
            match future.await? {
                AnnSegmentSource::Buffered(bytes) => Ok(bytes),
                AnnSegmentSource::Vindex(_) => Err(data_invalid(
                    "range-backed ANN segment cannot be converted to buffered bytes",
                )),
            }
        })
    }

    fn load_segment_source(
        &self,
        segment: &BucketAnnSegment,
    ) -> BoxFuture<'static, crate::Result<AnnSegmentSource>> {
        (self.loader)(segment)
    }

    fn search_batch(
        &self,
        segment: &BucketAnnSegment,
        segment_bytes: Bytes,
        queries: &[&[f32]],
        metric: VectorSearchMetric,
        limit: usize,
        active_source_files: &HashSet<String>,
        deletion_vectors: &HashMap<String, Arc<DeletionVector>>,
        search_options: &HashMap<String, String>,
        row_selections: Option<&FileRowSelections>,
    ) -> crate::Result<Vec<Vec<PkVectorSearchResult>>> {
        self.search_batch_source(
            segment,
            AnnSegmentSource::Buffered(segment_bytes),
            queries,
            metric,
            limit,
            active_source_files,
            deletion_vectors,
            search_options,
            row_selections,
        )
    }

    fn search_batch_source(
        &self,
        segment: &BucketAnnSegment,
        segment_source: AnnSegmentSource,
        queries: &[&[f32]],
        metric: VectorSearchMetric,
        limit: usize,
        active_source_files: &HashSet<String>,
        deletion_vectors: &HashMap<String, Arc<DeletionVector>>,
        search_options: &HashMap<String, String>,
        row_selections: Option<&FileRowSelections>,
    ) -> crate::Result<Vec<Vec<PkVectorSearchResult>>> {
        if limit == 0 {
            return Err(data_invalid("vector search limit must be positive"));
        }
        let source_files = segment.source_meta.source_files();
        // The live-row mask depends only on the segment's sources, the active set,
        // the deletion vectors, and the selections — none of which vary by query —
        // so it is BUILT once, then cloned into each query's search. Handing every
        // query one `Arc` instead would save those clones, but it also moves a
        // filtered search from Lumina's per-query scalar calls onto its native batch
        // call; worth doing, not here.
        let live = build_live_row_ids(
            source_files,
            active_source_files,
            deletion_vectors,
            row_selections,
        )?;
        let mut searches = Vec::with_capacity(queries.len());
        for query in queries {
            let mut search = VectorSearch::new(query.to_vec(), limit, self.field_name.clone())?
                .with_options(search_options.clone());
            if let Some(live) = &live {
                search = search.with_include_row_ids(live.clone());
            }
            searches.push(search);
        }
        let scored_batch = (self.scorer)(segment, segment_source, &searches)?;
        if scored_batch.len() != queries.len() {
            return Err(data_invalid(format!(
                "ANN batch scorer returned {} result maps for {} queries",
                scored_batch.len(),
                queries.len()
            )));
        }
        let mut out = Vec::with_capacity(queries.len());
        for scored in scored_batch {
            let results = match scored {
                Some(map) => {
                    let scored: Vec<(u64, f32)> = map.into_iter().collect();
                    map_ann_results(
                        &scored,
                        &segment.source_meta,
                        active_source_files,
                        deletion_vectors,
                        row_selections,
                        metric,
                    )?
                }
                None => Vec::new(),
            };
            out.push(results);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use roaring::RoaringBitmap;

    /// A trivial loader returning empty bytes — the synthetic scorers below ignore
    /// their `segment_bytes` (they model behavior above physical index decoding).
    fn empty_loader() -> SegmentLoader {
        Box::new(|_: &BucketAnnSegment| Box::pin(async { Ok(Bytes::new()) }))
    }

    /// Build a `VindexAnnSearcher` with a trivial (empty-bytes) loader, for tests
    /// that only exercise the scorer/adapter logic.
    fn vindex_searcher(field: &str, scorer: BatchScorer) -> VindexAnnSearcher {
        VindexAnnSearcher::new(field.to_string(), scorer, empty_loader())
    }

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

    /// A residual selection: the physical positions of one file that passed a data
    /// predicate.
    fn positions(at: &[u64]) -> FileRowSelection {
        let mut t = roaring::RoaringTreemap::new();
        for &p in at {
            t.insert(p);
        }
        FileRowSelection::Positions(t)
    }

    /// A pre-filter selection in the interval form an engine's split carries.
    fn ranges(bounds: &[(i64, i64)]) -> FileRowSelection {
        FileRowSelection::Ranges(
            bounds
                .iter()
                .map(|(from, to)| crate::table::RowRange::new(*from, *to))
                .collect(),
        )
    }

    fn active_set(names: &[&str]) -> HashSet<String> {
        names.iter().map(|n| (*n).to_string()).collect()
    }

    #[test]
    fn test_build_live_row_ids_none_when_all_active_and_no_relevant_dv() {
        let files = [PkVectorSourceFile::new("f0".into(), 3).unwrap()];
        let active = active_set(&["f0"]);
        // All active + empty map -> None.
        assert!(build_live_row_ids(&files, &active, &HashMap::new(), None)
            .unwrap()
            .is_none());
        // All active + non-empty map but no matching file name -> None.
        let mut dvs = HashMap::new();
        dvs.insert("other".to_string(), dv(&[0]));
        assert!(build_live_row_ids(&files, &active, &dvs, None)
            .unwrap()
            .is_none());
    }

    #[test]
    fn an_empty_selection_map_leaves_the_ann_search_unfiltered() {
        // The no-pre-filter shape: a Java split that narrowed nothing. Java returns
        // null here (`rowRangesByFile.isEmpty()`), and the ANN backend then searches
        // without a filter. Handing it an all-permitting mask instead costs an
        // 8-byte id per live row and takes the filtered code path.
        let files = [PkVectorSourceFile::new("f0".into(), 3).unwrap()];
        let selections = HashMap::new();
        assert!(
            build_live_row_ids(
                &files,
                &active_set(&["f0"]),
                &HashMap::new(),
                Some(&selections)
            )
            .unwrap()
            .is_none(),
            "nothing is restricted, so nothing should be masked"
        );
    }

    #[test]
    fn a_source_file_no_one_restricted_stays_whole() {
        // Java reads a missing entry as "every row of this file" and records one only
        // for a file its pre-filter narrowed. f1 is narrowed, f0 is not, so f0 must
        // stay whole rather than drop out of the search.
        let files = vec![
            PkVectorSourceFile::new("f0".into(), 3).unwrap(),
            PkVectorSourceFile::new("f1".into(), 2).unwrap(),
        ];
        let mut selections = HashMap::new();
        selections.insert("f1".to_string(), positions(&[1]));
        let live = build_live_row_ids(
            &files,
            &active_set(&["f0", "f1"]),
            &HashMap::new(),
            Some(&selections),
        )
        .unwrap()
        .unwrap();
        // f0 global 0,1,2 all live; f1 global 3,4 restricted to position 1 -> 4.
        assert_eq!(live.iter().collect::<Vec<u64>>(), vec![0, 1, 2, 4]);
    }

    #[test]
    fn test_build_live_row_ids_masks_inactive_source_ordinal_range() {
        // f0 rows 0..3 (global 0,1,2), f1 rows 0..2 (global 3,4). f1 is inactive,
        // so its whole ordinal range is masked out; f0 stays fully live. No DV.
        let files = vec![
            PkVectorSourceFile::new("f0".into(), 3).unwrap(),
            PkVectorSourceFile::new("f1".into(), 2).unwrap(),
        ];
        let live = build_live_row_ids(&files, &active_set(&["f0"]), &HashMap::new(), None)
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
        let live = build_live_row_ids(&files, &active_set(&["f0", "f1"]), &dvs, None)
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
            None,
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
            None,
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
            None,
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
            None,
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
        let searcher = vindex_searcher(
            "embedding",
            Box::new(
                move |_segment: &BucketAnnSegment, _bytes: Bytes, searches: &[VectorSearch]| {
                    let search = &searches[0];
                    *scorer_limit.lock().unwrap() = search.limit;
                    *scorer_has_filter.lock().unwrap() =
                        search.effective_include_row_ids().is_some();
                    let mut scores = HashMap::new();
                    scores.insert(3u64, 0.5f32); // -> (f1, 0)
                    scores.insert(0u64, 0.25f32); // -> (f0, 0), l2 dist 3.0
                    Ok(vec![Some(scores)])
                },
            ),
        );
        let segment = BucketAnnSegment::for_test({
            use crate::spec::{
                PrimaryKeyIndexSourceFile as PkVectorSourceFile,
                PrimaryKeyIndexSourceMeta as PkVectorSourceMeta,
            };
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
                Bytes::new(),
                &[0.0, 0.0],
                VectorSearchMetric::L2,
                2,
                &active_set(&["f0", "f1"]),
                &dvs,
                &HashMap::new(),
                None,
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
        let searcher = vindex_searcher(
            "embedding",
            Box::new(
                |_: &BucketAnnSegment, _bytes: Bytes, searches: &[VectorSearch]| {
                    Ok(vec![None; searches.len()])
                },
            ),
        );
        let segment = BucketAnnSegment::for_test({
            use crate::spec::{
                PrimaryKeyIndexSourceFile as PkVectorSourceFile,
                PrimaryKeyIndexSourceMeta as PkVectorSourceMeta,
            };
            PkVectorSourceMeta::new(1, vec![PkVectorSourceFile::new("f0".into(), 1).unwrap()])
                .unwrap()
        });
        let err = searcher
            .search(
                &segment,
                Bytes::new(),
                &[0.0, 0.0],
                VectorSearchMetric::L2,
                0,
                &active_set(&["f0"]),
                &HashMap::new(),
                &HashMap::new(),
                None,
            )
            .unwrap_err();
        assert!(err.to_string().contains("positive"));
    }

    #[test]
    fn test_vindex_adapter_empty_scorer_result_is_empty() {
        let searcher = vindex_searcher(
            "embedding",
            Box::new(
                |_: &BucketAnnSegment, _bytes: Bytes, searches: &[VectorSearch]| {
                    Ok(vec![None; searches.len()])
                },
            ),
        );
        let segment = BucketAnnSegment::for_test({
            use crate::spec::{
                PrimaryKeyIndexSourceFile as PkVectorSourceFile,
                PrimaryKeyIndexSourceMeta as PkVectorSourceMeta,
            };
            PkVectorSourceMeta::new(1, vec![PkVectorSourceFile::new("f0".into(), 1).unwrap()])
                .unwrap()
        });
        let results = searcher
            .search(
                &segment,
                Bytes::new(),
                &[0.0, 0.0],
                VectorSearchMetric::L2,
                2,
                &active_set(&["f0"]),
                &HashMap::new(),
                &HashMap::new(),
                None,
            )
            .unwrap();
        assert!(results.is_empty());
    }

    fn treemap(positions: &[u64]) -> roaring::RoaringTreemap {
        let mut t = roaring::RoaringTreemap::new();
        for &p in positions {
            t.insert(p);
        }
        t
    }

    #[test]
    fn test_build_live_row_ids_residual_intersects_with_active_and_dv() {
        // f0 rows 0..3 (global 0,1,2), f1 rows 0..2 (global 3,4). Both active.
        // dv on f0 deletes pos1 (global 1). residual allows f0={0,1}; f1 has no
        // entry, which is "unrestricted", so it keeps both its rows. Result: f0
        // keeps {0} (1 is residual-allowed but deleted, 2 not residual-allowed),
        // f1 keeps globals 3 and 4. (In production the residual producer registers
        // every active file, so an absent one does not arise there.)
        let files = vec![
            PkVectorSourceFile::new("f0".into(), 3).unwrap(),
            PkVectorSourceFile::new("f1".into(), 2).unwrap(),
        ];
        let mut dvs = HashMap::new();
        dvs.insert("f0".to_string(), dv(&[1]));
        let mut residual = HashMap::new();
        residual.insert(
            "f0".to_string(),
            FileRowSelection::Positions(treemap(&[0, 1])),
        );
        let live = build_live_row_ids(&files, &active_set(&["f0", "f1"]), &dvs, Some(&residual))
            .unwrap()
            .unwrap();
        assert_eq!(live.iter().collect::<Vec<u64>>(), vec![0, 3, 4]);
    }

    #[test]
    fn test_whole_file_allow_list_matches_having_no_residual_at_all() {
        // An adapter spells "unrestricted" out as an explicit whole-file allow-list.
        // That has to land on the same live set the no-residual path produces, since
        // it is the same statement said two ways.
        let files = vec![
            PkVectorSourceFile::new("f0".into(), 3).unwrap(),
            PkVectorSourceFile::new("f1".into(), 2).unwrap(),
        ];
        let active = active_set(&["f0", "f1"]);
        let mut residual = HashMap::new();
        residual.insert(
            "f0".to_string(),
            FileRowSelection::Positions(treemap(&[0, 1, 2])),
        );
        residual.insert(
            "f1".to_string(),
            FileRowSelection::Positions(treemap(&[0, 1])),
        );

        let spelled_out = build_live_row_ids(&files, &active, &HashMap::new(), Some(&residual))
            .unwrap()
            .unwrap();
        assert_eq!(
            spelled_out.iter().collect::<Vec<u64>>(),
            vec![0, 1, 2, 3, 4]
        );
    }

    #[test]
    fn test_whole_file_allow_list_still_applies_the_deletion_vector() {
        // The whole-file shortcut must not skip deletion vectors: f0 allows every
        // row, but position 1 is deleted and has to stay out.
        let files = vec![PkVectorSourceFile::new("f0".into(), 3).unwrap()];
        let mut dvs = HashMap::new();
        dvs.insert("f0".to_string(), dv(&[1]));
        let mut residual = HashMap::new();
        residual.insert(
            "f0".to_string(),
            FileRowSelection::Positions(treemap(&[0, 1, 2])),
        );
        let live = build_live_row_ids(&files, &active_set(&["f0"]), &dvs, Some(&residual))
            .unwrap()
            .unwrap();
        assert_eq!(live.iter().collect::<Vec<u64>>(), vec![0, 2]);
    }

    #[test]
    fn a_deletion_vector_position_past_its_own_file_is_refused() {
        // f0 holds 3 rows (global 0,1,2) and f1 holds 2 (global 3,4). A deletion
        // vector on f0 naming position 3 has no row of f0 to delete; without the
        // bound it becomes global 0 + 3 = 3, which is f1's FIRST row. The mask would
        // come back well-formed, having silently dropped a row of a different file.
        let files = vec![
            PkVectorSourceFile::new("f0".into(), 3).unwrap(),
            PkVectorSourceFile::new("f1".into(), 2).unwrap(),
        ];
        let mut dvs = HashMap::new();
        dvs.insert("f0".to_string(), dv(&[3]));
        let error = build_live_row_ids(&files, &active_set(&["f0", "f1"]), &dvs, None)
            .map(|_| ())
            .expect_err("a position past f0's rows names no row of f0");
        assert!(
            error
                .to_string()
                .contains("out of range for source file f0"),
            "{error}"
        );

        // The last VALID position of f0 still deletes f0's own row, and f1 is
        // untouched -- the bound is `>= row_count`, not one row tighter.
        let mut dvs = HashMap::new();
        dvs.insert("f0".to_string(), dv(&[2]));
        let live = build_live_row_ids(&files, &active_set(&["f0", "f1"]), &dvs, None)
            .unwrap()
            .unwrap();
        assert_eq!(live.iter().collect::<Vec<u64>>(), vec![0, 1, 3, 4]);
    }

    #[test]
    fn test_build_live_row_ids_residual_maps_positions_across_file_offsets() {
        // f0 rows global 0,1,2; f1 rows global 3,4. residual allows f0={2}, f1={1}.
        // f1 physical pos 1 -> global 3 + 1 = 4. Result {2, 4}. No DV.
        let files = vec![
            PkVectorSourceFile::new("f0".into(), 3).unwrap(),
            PkVectorSourceFile::new("f1".into(), 2).unwrap(),
        ];
        let mut residual = HashMap::new();
        residual.insert("f0".to_string(), FileRowSelection::Positions(treemap(&[2])));
        residual.insert("f1".to_string(), FileRowSelection::Positions(treemap(&[1])));
        let live = build_live_row_ids(
            &files,
            &active_set(&["f0", "f1"]),
            &HashMap::new(),
            Some(&residual),
        )
        .unwrap()
        .unwrap();
        assert_eq!(live.iter().collect::<Vec<u64>>(), vec![2, 4]);
    }

    #[test]
    fn test_build_live_row_ids_residual_some_returns_mask_even_when_all_active_no_dv() {
        // All active, no DV: without residual this returns None. With a residual
        // present, a mask is always required.
        let files = [PkVectorSourceFile::new("f0".into(), 3).unwrap()];
        let mut residual = HashMap::new();
        residual.insert(
            "f0".to_string(),
            FileRowSelection::Positions(treemap(&[0, 2])),
        );
        let live = build_live_row_ids(
            &files,
            &active_set(&["f0"]),
            &HashMap::new(),
            Some(&residual),
        )
        .unwrap()
        .expect("residual present -> mask required");
        assert_eq!(live.iter().collect::<Vec<u64>>(), vec![0, 2]);
    }

    #[test]
    fn test_build_live_row_ids_rejects_out_of_range_residual_position() {
        // Source file "f0" has 3 rows (valid positions 0..=2). A residual allow-list
        // naming position 3 is out of range and must fail loud, not be skipped.
        let files = source_meta(&[("f0", 3)]);
        let mut residual = HashMap::new();
        residual.insert(
            "f0".to_string(),
            FileRowSelection::Positions(treemap(&[0, 3])),
        );
        let err = build_live_row_ids(
            files.source_files(),
            &active_set(&["f0"]),
            &HashMap::new(),
            Some(&residual),
        )
        .unwrap_err();
        assert!(err.to_string().contains("out of range"));
    }

    #[test]
    fn map_ann_results_accepts_a_hit_in_a_file_no_one_restricted() {
        // The tri-state on the validation side: the map restricts f0, says nothing
        // about f1, and a hit in f1 must be accepted. Reading f1's absence as "no
        // rows" would turn every hit in an unrestricted sibling into an error.
        let meta = source_meta(&[("f0", 3), ("f1", 5)]);
        let mut selections = HashMap::new();
        selections.insert("f0".to_string(), ranges(&[(0, 0)]));
        let results = map_ann_results(
            &[(3, 0.5)], // ordinal 3 -> (f1, 0)
            &meta,
            &active_set(&["f0", "f1"]),
            &HashMap::new(),
            Some(&selections),
            VectorSearchMetric::L2,
        )
        .expect("a hit in an unrestricted file is allowed");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].data_file_name, "f1");
        assert_eq!(results[0].row_position, 0);
    }

    #[test]
    fn test_map_ann_results_rejects_hit_outside_residual_allow_list() {
        // ordinal 1 -> (f0, 1). Residual allows only {0} in f0, so a hit at position 1
        // (e.g. an ANN reader that ignored include_row_ids) must fail loud.
        let meta = source_meta(&[("f0", 3)]);
        let mut residual = HashMap::new();
        residual.insert("f0".to_string(), FileRowSelection::Positions(treemap(&[0])));
        let err = map_ann_results(
            &[(1u64, 0.5)],
            &meta,
            &active_set(&["f0"]),
            &HashMap::new(),
            Some(&residual),
            VectorSearchMetric::L2,
        )
        .unwrap_err();
        assert!(err.to_string().contains("outside the row selection"));
    }

    #[test]
    fn test_vindex_adapter_sets_include_row_ids_to_residual_intersection() {
        // Recording scorer captures the include_row_ids the adapter built. All
        // active, no DV, residual f0={0,2} -> include_row_ids must equal {0,2}.
        use std::sync::{Arc, Mutex};
        let seen_rows: Arc<Mutex<Option<Vec<u64>>>> = Arc::new(Mutex::new(None));
        let scorer_rows = Arc::clone(&seen_rows);
        let searcher = vindex_searcher(
            "embedding",
            Box::new(
                move |_segment: &BucketAnnSegment, _bytes: Bytes, searches: &[VectorSearch]| {
                    *scorer_rows.lock().unwrap() = searches[0]
                        .effective_include_row_ids()
                        .map(|t| t.iter().collect::<Vec<u64>>());
                    Ok(vec![None; searches.len()])
                },
            ),
        );
        let segment = BucketAnnSegment::for_test(source_meta(&[("f0", 3)]));
        let mut residual = HashMap::new();
        residual.insert(
            "f0".to_string(),
            FileRowSelection::Positions(treemap(&[0, 2])),
        );
        searcher
            .search(
                &segment,
                Bytes::new(),
                &[0.0, 0.0],
                VectorSearchMetric::L2,
                2,
                &active_set(&["f0"]),
                &HashMap::new(),
                &HashMap::new(),
                Some(&residual),
            )
            .unwrap();
        assert_eq!(seen_rows.lock().unwrap().clone(), Some(vec![0, 2]));
    }

    #[test]
    fn the_no_prefilter_shape_reaches_the_backend_with_no_filter_at_all() {
        // The backend-facing half of the no-pre-filter case. `include_row_ids` is
        // what Lumina turns into a Vec<u64> of every live id and a filtered search;
        // for a query that filters nothing it must stay unset, exactly as on the
        // manifest route.
        use std::sync::{Arc, Mutex};
        let seen: Arc<Mutex<Option<bool>>> = Arc::new(Mutex::new(None));
        let scorer_seen = Arc::clone(&seen);
        let searcher = vindex_searcher(
            "embedding",
            Box::new(
                move |_segment: &BucketAnnSegment, _bytes: Bytes, searches: &[VectorSearch]| {
                    *scorer_seen.lock().unwrap() =
                        Some(searches[0].effective_include_row_ids().is_some());
                    Ok(vec![None; searches.len()])
                },
            ),
        );
        let segment = BucketAnnSegment::for_test(source_meta(&[("f0", 3)]));
        let no_prefilter: FileRowSelections = HashMap::new();
        searcher
            .search(
                &segment,
                Bytes::new(),
                &[0.0, 0.0],
                VectorSearchMetric::L2,
                2,
                &active_set(&["f0"]),
                &HashMap::new(),
                &HashMap::new(),
                Some(&no_prefilter),
            )
            .unwrap();
        assert_eq!(
            seen.lock().unwrap().clone(),
            Some(false),
            "a split that narrowed nothing must not take the filtered ANN path"
        );
    }

    #[test]
    fn a_range_past_the_source_row_count_is_rejected() {
        // The range bounds ride in on an engine-supplied split. Java checks each one
        // against the SOURCE file's row count, so a range that disagrees with its own
        // segment is rejected. On this route that count came off the wire as well, so
        // this is a consistency check between the split's own two numbers -- NOT a
        // bound on how much the insert can allocate.
        let files = [PkVectorSourceFile::new("f0".into(), 3).unwrap()];
        let mut selections = HashMap::new();
        selections.insert("f0".to_string(), ranges(&[(0, 1 << 40)]));
        let error = build_live_row_ids(
            &files,
            &active_set(&["f0"]),
            &HashMap::new(),
            Some(&selections),
        )
        .map(|_| ())
        .expect_err("a range beyond the source file cannot be inserted");
        assert!(error.to_string().contains("out of range"), "{error}");
    }

    #[test]
    fn a_live_set_beyond_what_the_backend_can_filter_is_rejected() {
        // A mask is necessary (one sibling is restricted), so `huge` is inserted
        // wholesale at whatever its wire-supplied row count claims. Without a limit
        // on what gets inserted this OOM-kills the process.
        let files = vec![
            PkVectorSourceFile::new("huge".into(), i64::MAX).unwrap(),
            PkVectorSourceFile::new("small".into(), 1).unwrap(),
        ];
        let mut selections = HashMap::new();
        selections.insert("small".to_string(), ranges(&[(0, 0)]));
        let error = build_live_row_ids(
            &files,
            &active_set(&["huge", "small"]),
            &HashMap::new(),
            Some(&selections),
        )
        .map(|_| ())
        .expect_err("more live rows than the backend can be handed");
        assert!(error.to_string().contains("more than"), "{error}");
    }

    #[test]
    fn a_deletion_vector_alone_also_reaches_the_live_row_limit() {
        // Same exposure with no pre-filter at all: one deletion vector on one of
        // this segment's own sources is enough to make a mask necessary.
        let files = vec![PkVectorSourceFile::new("huge".into(), i64::MAX).unwrap()];
        let mut dvs = HashMap::new();
        dvs.insert("huge".to_string(), dv(&[0]));
        let error = build_live_row_ids(&files, &active_set(&["huge"]), &dvs, None)
            .map(|_| ())
            .expect_err("more live rows than the backend can be handed");
        assert!(error.to_string().contains("more than"), "{error}");
    }

    #[test]
    fn a_huge_claim_with_nothing_live_is_not_rejected() {
        // The limit is on rows actually put in the mask, not on what the metadata
        // claims. These three all claim more rows than any backend could filter and
        // all leave the mask empty, so none of them may be turned away.
        let huge = vec![PkVectorSourceFile::new("huge".into(), i64::MAX).unwrap()];

        // Explicitly excluded.
        let mut excluded = HashMap::new();
        excluded.insert("huge".to_string(), ranges(&[]));
        let live = build_live_row_ids(
            &huge,
            &active_set(&["huge"]),
            &HashMap::new(),
            Some(&excluded),
        )
        .expect("an excluded file puts nothing in the mask")
        .expect("a selection was present");
        assert!(live.is_empty());

        // Inactive: its whole ordinal range is masked out anyway.
        let live = build_live_row_ids(&huge, &active_set(&[]), &HashMap::new(), None)
            .expect("an inactive source puts nothing in the mask")
            .expect("a source was inactive");
        assert!(live.is_empty());

        // Narrowly restricted: two rows out of an impossible claim.
        let mut narrow = HashMap::new();
        narrow.insert("huge".to_string(), ranges(&[(0, 1)]));
        let live = build_live_row_ids(
            &huge,
            &active_set(&["huge"]),
            &HashMap::new(),
            Some(&narrow),
        )
        .expect("a narrow selection puts two rows in the mask")
        .expect("a selection was present");
        assert_eq!(live.iter().collect::<Vec<u64>>(), vec![0, 1]);
    }

    #[test]
    fn the_live_row_limit_is_javas_dense_filter_limit() {
        // Java rejects an include-set above `Integer.MAX_VALUE` before it allocates
        // the dense array (`LuminaVectorGlobalIndexReader.toScopedIds`). Exactly that
        // many rows is what a backend can still be handed; one more is not.
        let at_limit = vec![PkVectorSourceFile::new("f0".into(), i32::MAX as i64).unwrap()];
        let mut dvs = HashMap::new();
        dvs.insert("f0".to_string(), dv(&[0]));
        assert!(
            build_live_row_ids(&at_limit, &active_set(&["f0"]), &dvs, None).is_ok(),
            "exactly the limit is allowed"
        );

        // One row past it, even though the deletion vector would bring the FINAL set
        // back under: the charge is on insertion, because that is where the
        // allocation happens.
        let over = vec![PkVectorSourceFile::new("f0".into(), i32::MAX as i64 + 1).unwrap()];
        assert!(
            build_live_row_ids(&over, &active_set(&["f0"]), &dvs, None).is_err(),
            "one row past the limit is not"
        );
    }

    /// A treemap holding `0..=to`, built as one run so the test itself stays cheap.
    fn positions_through(to: u64) -> roaring::RoaringTreemap {
        let mut t = roaring::RoaringTreemap::new();
        t.insert_range(0..=to);
        t
    }

    #[test]
    fn an_oversized_range_selection_is_charged() {
        // The `Ranges` charge site, distinct from the unrestricted one: the file is
        // restricted, so it never reaches the whole-file insert.
        let files = vec![PkVectorSourceFile::new("f0".into(), i32::MAX as i64 + 1).unwrap()];
        let mut selections = HashMap::new();
        selections.insert("f0".to_string(), ranges(&[(0, i32::MAX as i64)]));
        let error = build_live_row_ids(
            &files,
            &active_set(&["f0"]),
            &HashMap::new(),
            Some(&selections),
        )
        .map(|_| ())
        .expect_err("a range this wide cannot be filtered");
        assert!(error.to_string().contains("more than"), "{error}");
    }

    #[test]
    fn an_oversized_whole_file_position_set_is_charged() {
        // The `Positions` whole-file shortcut: `len` equals the row count and the
        // maximum is the last row, so it inserts as one range.
        let rows = i32::MAX as u64 + 1;
        let files = vec![PkVectorSourceFile::new("f0".into(), rows as i64).unwrap()];
        let mut selections = HashMap::new();
        selections.insert(
            "f0".to_string(),
            FileRowSelection::Positions(positions_through(rows - 1)),
        );
        let error = build_live_row_ids(
            &files,
            &active_set(&["f0"]),
            &HashMap::new(),
            Some(&selections),
        )
        .map(|_| ())
        .expect_err("a whole-file position set this large cannot be filtered");
        assert!(error.to_string().contains("more than"), "{error}");
    }

    #[test]
    fn an_oversized_sparse_position_set_is_charged() {
        // The per-position `Positions` path: the set is large but is NOT the whole
        // file, so the shortcut above does not apply and the loop would walk it.
        let rows = i32::MAX as u64 + 5;
        let files = vec![PkVectorSourceFile::new("f0".into(), rows as i64).unwrap()];
        let mut selections = HashMap::new();
        selections.insert(
            "f0".to_string(),
            FileRowSelection::Positions(positions_through(i32::MAX as u64)),
        );
        let error = build_live_row_ids(
            &files,
            &active_set(&["f0"]),
            &HashMap::new(),
            Some(&selections),
        )
        .map(|_| ())
        .expect_err("a position set this large cannot be filtered");
        assert!(error.to_string().contains("more than"), "{error}");
    }

    #[test]
    fn an_empty_range_list_excludes_the_source_file() {
        // Java's empty `List<Range>`: present and permitting nothing, the opposite of
        // absent.
        let files = vec![
            PkVectorSourceFile::new("f0".into(), 3).unwrap(),
            PkVectorSourceFile::new("f1".into(), 2).unwrap(),
        ];
        let mut selections = HashMap::new();
        selections.insert("f0".to_string(), ranges(&[]));
        let live = build_live_row_ids(
            &files,
            &active_set(&["f0", "f1"]),
            &HashMap::new(),
            Some(&selections),
        )
        .unwrap()
        .unwrap();
        // f0 contributes nothing; f1 is unlisted, so both its rows stay live.
        assert_eq!(live.iter().collect::<Vec<u64>>(), vec![3, 4]);
    }

    #[test]
    fn range_selections_map_onto_their_source_file_offset() {
        // f0 rows global 0,1,2; f1 rows global 3,4. f1 restricted to [1, 1]. This
        // pins the offset arithmetic, not the interval-wise insertion -- a
        // per-position loop would produce the same bitmap.
        let files = vec![
            PkVectorSourceFile::new("f0".into(), 3).unwrap(),
            PkVectorSourceFile::new("f1".into(), 2).unwrap(),
        ];
        let mut selections = HashMap::new();
        selections.insert("f0".to_string(), ranges(&[(0, 0), (2, 2)]));
        selections.insert("f1".to_string(), ranges(&[(1, 1)]));
        let live = build_live_row_ids(
            &files,
            &active_set(&["f0", "f1"]),
            &HashMap::new(),
            Some(&selections),
        )
        .unwrap()
        .unwrap();
        assert_eq!(live.iter().collect::<Vec<u64>>(), vec![0, 2, 4]);
    }

    #[test]
    fn test_search_batch_of_one_equals_single_query() {
        // The single-query `search` wrapper must return exactly what
        // `search_batch(&[q])[0]` returns for the same inputs.
        let make = || {
            vindex_searcher(
                "embedding",
                Box::new(
                    |_: &BucketAnnSegment, _bytes: Bytes, searches: &[VectorSearch]| {
                        let mut out = Vec::with_capacity(searches.len());
                        for _ in searches {
                            let mut scores = HashMap::new();
                            scores.insert(3u64, 0.5f32); // -> (f1, 0)
                            scores.insert(0u64, 0.25f32); // -> (f0, 0)
                            out.push(Some(scores));
                        }
                        Ok(out)
                    },
                ),
            )
        };
        let meta = source_meta(&[("f0", 3), ("f1", 5)]);
        let single = make()
            .search(
                &BucketAnnSegment::for_test(meta.clone()),
                Bytes::new(),
                &[0.0, 0.0],
                VectorSearchMetric::L2,
                2,
                &active_set(&["f0", "f1"]),
                &HashMap::new(),
                &HashMap::new(),
                None,
            )
            .unwrap();
        let query: &[f32] = &[0.0, 0.0];
        let batch = make()
            .search_batch(
                &BucketAnnSegment::for_test(meta),
                Bytes::new(),
                &[query],
                VectorSearchMetric::L2,
                2,
                &active_set(&["f0", "f1"]),
                &HashMap::new(),
                &HashMap::new(),
                None,
            )
            .unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0], single);
    }

    #[test]
    fn test_search_batch_returns_independent_per_query_results() {
        // Two queries route to different synthetic scores; each result list is
        // mapped from that query's own scores, over the same live-row mask.
        let searcher = vindex_searcher(
            "embedding",
            Box::new(
                |_: &BucketAnnSegment, _bytes: Bytes, searches: &[VectorSearch]| {
                    let mut out = Vec::with_capacity(searches.len());
                    for (i, _) in searches.iter().enumerate() {
                        let mut scores = HashMap::new();
                        // Query 0 -> ordinal 0 (f0,0); query 1 -> ordinal 3 (f1,0).
                        if i == 0 {
                            scores.insert(0u64, 0.5f32);
                        } else {
                            scores.insert(3u64, 0.5f32);
                        }
                        out.push(Some(scores));
                    }
                    Ok(out)
                },
            ),
        );
        let q0: &[f32] = &[0.0, 0.0];
        let q1: &[f32] = &[1.0, 1.0];
        let results = searcher
            .search_batch(
                &BucketAnnSegment::for_test(source_meta(&[("f0", 3), ("f1", 5)])),
                Bytes::new(),
                &[q0, q1],
                VectorSearchMetric::L2,
                2,
                &active_set(&["f0", "f1"]),
                &HashMap::new(),
                &HashMap::new(),
                None,
            )
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0][0].data_file_name, "f0");
        assert_eq!(results[1][0].data_file_name, "f1");
    }

    #[test]
    fn test_search_batch_fails_loud_on_result_count_mismatch() {
        // A batch scorer that returns the wrong number of result maps is corruption
        // and must fail loud, not be silently padded/truncated.
        let searcher = vindex_searcher(
            "embedding",
            Box::new(|_: &BucketAnnSegment, _: Bytes, _: &[VectorSearch]| {
                // Only one map returned regardless of query count.
                Ok(vec![None])
            }),
        );
        let q0: &[f32] = &[0.0, 0.0];
        let q1: &[f32] = &[1.0, 1.0];
        let err = searcher
            .search_batch(
                &BucketAnnSegment::for_test(source_meta(&[("f0", 3)])),
                Bytes::new(),
                &[q0, q1],
                VectorSearchMetric::L2,
                2,
                &active_set(&["f0"]),
                &HashMap::new(),
                &HashMap::new(),
                None,
            )
            .unwrap_err();
        assert!(
            err.to_string().contains("2 queries") || err.to_string().contains("result maps"),
            "got: {err}"
        );
    }
}
