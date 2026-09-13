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

//! Executes primary-key vector searches and materializes hits by bucket/file position.

use crate::arrow::format::FilePredicates;
use crate::arrow::residual::{evaluate_predicates_mask, widen_scan_fields};
use crate::lumina::reader::LuminaVectorGlobalIndexReader;
use crate::lumina::LuminaIndexMeta;
use crate::spec::{CoreOptions, DataField, Predicate};
use crate::table::bucket_filter::split_partition_and_data_predicates;
use crate::table::data_file_reader::DataFileReader;
use crate::table::pk_vector_data_file_reader::{
    append_batch_vectors, DataFilePkVectorReaderFactory,
};
use crate::table::pk_vector_indexed_split_read::{expand_ranges, PkVectorIndexedSplitRead};
use crate::table::pk_vector_orchestrator::{
    as_split_exact_file_search, build_indexed_splits, merge_candidates, OrchestratorSearchResult,
    PkVectorCandidate, PkVectorOrchestrator, PkVectorSearchSplit,
};
use crate::table::pk_vector_position_read::{PkVectorPositionRead, PKEY_VECTOR_POSITION_COLUMN};
use crate::table::pk_vector_scan::PkVectorScanPlan;
use crate::table::pk_vector_search_params::PkVectorSearchParams;
use crate::table::row_id_predicate::intersect_sorted_ranges;
use crate::table::source::DataSplit;
use crate::table::vector_read::Read;
use crate::table::vector_search_common::{
    collect_ranked_rows, current_tokio_runtime_handle, log_vindex_range_io_stats,
    reorder_and_strip_position, vindex_concurrency_limits, RankedRow, VectorIndexBackend,
};
use crate::table::{ArrowRecordBatchStream, RowRange, Table};
use crate::vector_search::{GlobalIndexIOMeta, SearchResult, VectorSearch};
use crate::vindex::pkvector::ann::{AnnSegmentSource, PkVectorAnnSearcher, VindexAnnSearcher};
use crate::vindex::pkvector::bucket::{BucketActiveFile, BucketAnnSegment, ExactFileSearchFuture};
use crate::vindex::pkvector::metric::VectorSearchMetric;
use crate::vindex::pkvector::RowRangesByFile;
use crate::vindex::range_reader::{RangeReadLimiter, VindexFileReader};
use crate::vindex::reader::VindexVectorGlobalIndexReader;
use arrow_array::{Array, Int64Array, RecordBatch};
use futures::{stream, TryStreamExt};
use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::sync::Arc;

pub(super) struct PkVectorRead {
    table: Table,
    options: HashMap<String, String>,
    filter: Option<Predicate>,
    vector_column: String,
    queries: Vec<Vec<f32>>,
    limit: usize,
    params: PkVectorSearchParams,
}

impl PkVectorRead {
    /// Construct a reader from parameters resolved and validated by the builder.
    pub(super) fn new(
        table: &Table,
        options: &HashMap<String, String>,
        filter: Option<&Predicate>,
        vector_column: &str,
        queries: &[&[f32]],
        limit: usize,
        params: PkVectorSearchParams,
    ) -> Self {
        Self {
            table: table.clone(),
            options: options.clone(),
            filter: filter.cloned(),
            vector_column: vector_column.to_string(),
            queries: queries.iter().map(|query| query.to_vec()).collect(),
            limit,
            params,
        }
    }
}

impl Read for PkVectorRead {
    type Plan = PkVectorScanPlan;

    async fn read(&self, plan: PkVectorScanPlan) -> crate::Result<Vec<SearchResult>> {
        let core = CoreOptions::new(self.table.schema().options());
        let queries: Vec<&[f32]> = self.queries.iter().map(Vec::as_slice).collect();
        let candidates = search_pk_candidates_batch_with_plan(
            &self.table,
            &self.options,
            self.filter.as_ref(),
            &core,
            &self.vector_column,
            &queries,
            self.limit,
            &plan,
            &self.params,
        )
        .await?;
        let table = Arc::new(self.table.clone());
        candidates
            .into_iter()
            .map(|candidates| {
                SearchResult::from_primary_key(
                    table.clone(),
                    plan.snapshot_id,
                    candidates,
                    &plan.splits,
                    self.params.metric,
                )
            })
            .collect()
    }
}

/// Materialize one best-first candidate list into an Arrow stream, best-first,
/// with a `__paimon_search_score` column and `_PKEY_VECTOR_POSITION` stripped.
/// An empty candidate list yields an empty stream (never skipped) so a batch
/// caller preserves per-query arity. `materialize_reader` must project the
/// output columns (predicate-free). Both the single-query and batch read paths
/// use this so their materialization is identical.
pub(super) async fn materialize_positions(
    positions: &[crate::vector_search::PrimaryKeySearchPosition],
    indexed_splits: &[crate::table::pk_vector_indexed_split_read::PkVectorIndexedSplit],
    materialize_reader: &DataFileReader,
) -> crate::Result<ArrowRecordBatchStream> {
    if positions.is_empty() {
        return Ok(Box::pin(stream::empty()));
    }
    let rank_of = positions
        .iter()
        .enumerate()
        .map(|(rank, p)| {
            (
                (
                    p.partition.to_serialized_bytes(),
                    p.bucket,
                    p.data_file_name.clone(),
                    p.row_position,
                ),
                rank,
            )
        })
        .collect();

    // Materialize every indexed split, retaining each batch and, per row, the
    // (rank, batch_index, row_index) tuple so we can reorder to best-first.
    // Top-K is small, so full in-memory collection is acceptable.
    let mut batches: Vec<RecordBatch> = Vec::new();
    let mut ranked: Vec<RankedRow> = Vec::new();
    for indexed in indexed_splits {
        let partition_bytes = indexed.split.partition().to_serialized_bytes();
        let bucket = indexed.split.bucket();
        let file_name = indexed.split.data_files()[0].file_name.clone();
        let mut stream = PkVectorIndexedSplitRead::new(materialize_reader.clone()).read(indexed)?;
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

    // Reorder to best-first and drop the position column.
    let output = reorder_and_strip_position(&batches, ranked)?;
    Ok(Box::pin(stream::iter(output.into_iter().map(Ok))))
}

/// Intersect the plan's physical row ranges with the residual predicate's ranges.
/// The plan omits unrestricted files; the residual registers every active file,
/// including empty results. A file listed only by the plan must therefore stay
/// excluded when a residual exists. Files absent from both inputs stay absent.
/// Both inputs are sorted and merged, so intersection never expands large spans.
fn intersect_row_ranges_by_split(
    physical: Option<&[RowRangesByFile]>,
    residual: Option<Vec<RowRangesByFile>>,
    split_count: usize,
) -> crate::Result<Option<Vec<RowRangesByFile>>> {
    if let Some(maps) = physical {
        if maps.len() != split_count {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "plan carries {} physical row allow-lists for {split_count} splits",
                    maps.len()
                ),
                source: None,
            });
        }
    }
    if let Some(maps) = residual.as_ref() {
        if maps.len() != split_count {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "residual carries {} row allow-lists for {split_count} splits",
                    maps.len()
                ),
                source: None,
            });
        }
    }
    match (physical, residual) {
        (None, residual) => Ok(residual),
        (Some(physical), None) => Ok(Some(physical.to_vec())),
        (Some(physical), Some(residual)) => Ok(Some(
            physical
                .iter()
                .zip(residual)
                .map(|(physical, mut residual)| {
                    for (file, ranges) in physical {
                        // The residual covers every active file. A missing entry
                        // must not restore rows excluded by that residual.
                        let allowed = residual.entry(file.clone()).or_default();
                        *allowed = intersect_sorted_ranges(ranges, allowed);
                    }
                    residual
                })
                .collect(),
        )),
    }
}

/// Search a resolved plan across all queries before reranking and merging.
/// Concurrency is derived from the actual plan, including external split subsets.
#[allow(clippy::too_many_arguments)]
async fn search_pk_raw_candidates_batch_with_plan(
    table: &Table,
    query_options: &HashMap<String, String>,
    filter: Option<&Predicate>,
    core: &CoreOptions<'_>,
    pk_col: &str,
    queries: &[&[f32]],
    limit: usize,
    plan: &PkVectorScanPlan,
    params: &PkVectorSearchParams,
) -> crate::Result<Vec<OrchestratorSearchResult>> {
    // An empty plan has nothing to search. Returned before the backend is resolved
    // so a table with no searchable data never errors on an unrecognized index type.
    if plan.splits.is_empty() {
        return Ok(queries
            .iter()
            .map(|_| OrchestratorSearchResult {
                indexed: Vec::new(),
                exact: Vec::new(),
            })
            .collect());
    }

    let metric = params.metric;
    let concurrency = params.concurrency;
    let index_type = params.index_type.clone();
    let vector_field = params.vector_field.clone();
    let skip_exact_fallback = params.skip_exact_fallback;
    let indexed_limit = params.indexed_limit;

    // Resolve the vector index backend from the single configured index type.
    // Java enforces one index type per PK table and Rust filters segments to it,
    // so one backend serves every segment. Computed after the empty-plan return so
    // an empty table never errors on an unrecognized type.
    let backend = VectorIndexBackend::from_index_type(&index_type).ok_or_else(|| {
        crate::Error::DataInvalid {
            message: format!("unsupported PK vector index backend/type: '{index_type}'"),
            source: None,
        }
    })?;
    let (batch_index_parallelism, range_read_concurrency) = match backend {
        VectorIndexBackend::Vindex => vindex_concurrency_limits(
            core,
            plan.splits
                .iter()
                .map(|split| split.ann_segments.len())
                .sum(),
            concurrency,
        )?,
        VectorIndexBackend::Lumina => (1, 0),
    };

    // Production data-file reader, mirroring `table_read.rs::new_data_file_reader`
    // but projecting only the vector column with no predicates.
    let reader = DataFileReader::new(
        table.file_io().clone(),
        table.schema_manager().clone(),
        table.schema().id(),
        table.schema().fields().to_vec(),
        vec![vector_field.clone()],
        Vec::new(),
    );

    // Real ANN scorer + loader. Each segment source is opened lazily inside its
    // bucket leaf and dropped after scoring. Lumina keeps its buffered-byte path;
    // vindex remains range-backed and reads only metadata and probed lists.
    let options = {
        let mut o = table.schema().options().clone();
        o.extend(query_options.clone());
        o
    };
    let search_options = options.clone();
    let field_name = pk_col.to_string();

    let loader_io = table.file_io().clone();
    let loader_range_read_limiter = match backend {
        VectorIndexBackend::Vindex => Some(RangeReadLimiter::new(range_read_concurrency)),
        VectorIndexBackend::Lumina => None,
    };
    let loader: crate::vindex::pkvector::ann::SourceSegmentLoader = Box::new(
        move |segment: &BucketAnnSegment| {
            let io = loader_io.clone();
            let range_read_limiter = loader_range_read_limiter.clone();
            let path = segment.path.clone();
            let file_size = segment.file_size;
            Box::pin(async move {
                let input = io.new_input(&path)?;
                match backend {
                    VectorIndexBackend::Lumina => input
                        .read()
                        .await
                        .map(AnnSegmentSource::Buffered)
                        .map_err(|error| crate::Error::DataInvalid {
                            message: format!("failed to read ANN index file '{path}': {error}"),
                            source: None,
                        }),
                    VectorIndexBackend::Vindex => {
                        let file_reader =
                            input
                                .reader()
                                .await
                                .map_err(|error| crate::Error::DataInvalid {
                                    message: format!(
                                        "failed to open ANN index file '{path}' for range reads: {error}"
                                    ),
                                    source: None,
                                })?;
                        Ok(AnnSegmentSource::Vindex(
                            VindexFileReader::new_with_limiter(
                                Arc::new(file_reader),
                                current_tokio_runtime_handle()?,
                                range_read_limiter.expect("Vindex range-read limiter"),
                                file_size,
                                path,
                            ),
                        ))
                    }
                }
            })
        },
    );

    let scorer: crate::vindex::pkvector::ann::SourceBatchScorer = Box::new(
        move |segment: &BucketAnnSegment, source: AnnSegmentSource, searches: &[VectorSearch]| {
            let io_meta = GlobalIndexIOMeta::new(
                segment.path.clone(),
                segment.file_size,
                segment.index_meta.clone(),
            );
            match (backend, source) {
                (VectorIndexBackend::Lumina, AnnSegmentSource::Buffered(data)) => {
                    let lumina_metric =
                        LuminaIndexMeta::deserialize(&segment.index_meta)?.metric()?;
                    verify_segment_metric(metric, VectorSearchMetric::from_lumina(lumina_metric))?;
                    let mut reader = LuminaVectorGlobalIndexReader::new(io_meta, options.clone());
                    reader.visit_batch_vector_search(searches, |_| Ok(Cursor::new(data)))
                }
                (VectorIndexBackend::Vindex, AnnSegmentSource::Vindex(source)) => {
                    let range_io_stats = source.range_io_stats();
                    let mut reader = VindexVectorGlobalIndexReader::new(io_meta, options.clone())
                        .with_batch_index_parallelism(batch_index_parallelism);
                    let results = reader.visit_batch_vector_search_validated(
                        searches,
                        |_| Ok(source),
                        |metadata| {
                            verify_segment_metric(
                                metric,
                                VectorSearchMetric::from_vindex(metadata.metric),
                            )
                        },
                    )?;
                    if let Some(stats) = range_io_stats {
                        log_vindex_range_io_stats(&segment.path, searches.len(), &stats);
                    }
                    Ok(results)
                }
                (VectorIndexBackend::Lumina, AnnSegmentSource::Vindex(_))
                | (VectorIndexBackend::Vindex, AnnSegmentSource::Buffered(_)) => {
                    Err(crate::Error::DataInvalid {
                        message: format!(
                            "ANN segment '{}' was loaded with the wrong backend source",
                            segment.path
                        ),
                        source: None,
                    })
                }
            }
        },
    );
    let ann_searcher: Arc<dyn PkVectorAnnSearcher> = Arc::new(VindexAnnSearcher::new_with_source(
        field_name, scorer, loader,
    ));

    // Resolve data predicates before recall so both ANN and exact Top-K honor
    // them. Partition-only predicates were already applied by the scan. The
    // residual depends on the filter and plan, so its file-local ranges are
    // shared by all queries. A file with an empty range list is skipped.
    let residual_by_split: Option<Vec<RowRangesByFile>> = match filter {
        Some(filter) => {
            // The whole filter is pushed into scan planning (`PkVectorScan`), where
            // partition-only conjuncts already prune partitions/files. Re-applying
            // them as a per-row residual would be redundant, so keep only the data
            // conjuncts here — a partition-only filter then needs no residual at
            // all. Mixed partition/data conjuncts stay whole in `data_predicates`
            // and evaluate against the materialized partition column (partition
            // columns are physically present in primary-key data files), so there
            // is no missing-column case to reject.
            let (_partition_predicate, data_predicates) = split_partition_and_data_predicates(
                filter.clone(),
                table.schema().fields(),
                table.schema().partition_keys(),
            );
            if data_predicates.is_empty() {
                None
            } else {
                let file_predicates = FilePredicates {
                    predicates: data_predicates,
                    row_filter_factory: None,
                    file_fields: table.schema().fields().to_vec(),
                };
                let residual_read_type = widen_scan_fields(&[], Some(&file_predicates));
                let residual_reader = DataFileReader::new(
                    table.file_io().clone(),
                    table.schema_manager().clone(),
                    table.schema().id(),
                    table.schema().fields().to_vec(),
                    residual_read_type,
                    Vec::new(),
                );
                let mut per_split = Vec::with_capacity(plan.splits.len());
                for (index, split) in plan.splits.iter().enumerate() {
                    // The plan's selection for this split, so the residual is
                    // evaluated over the rows an engine-supplied split allows rather
                    // than over the whole file.
                    let allowed_rows = plan
                        .physical_row_ranges_by_split
                        .as_ref()
                        .and_then(|per_split| per_split.get(index));
                    per_split.push(
                        residual_row_ranges_by_file(
                            &residual_reader,
                            &split.data_split,
                            &split.active_files,
                            &file_predicates,
                            allowed_rows,
                        )
                        .await?,
                    );
                }
                Some(per_split)
            }
        }
        None => None,
    };
    // Preserve the external plan's ranges as well as the residual restriction.
    let row_ranges_by_split = intersect_row_ranges_by_split(
        plan.physical_row_ranges_by_split.as_deref(),
        residual_by_split,
        plan.splits.len(),
    )?;

    // Build the exact-fallback search on demand: the kernel calls this only for a
    // file it actually searches (uncovered by ANN, residual-allowed, and only when
    // the search mode is not FAST). Everything the future needs is cloned/owned up
    // front so it borrows neither the split nor the file across the await. The
    // search streams the file's vector column one Arrow batch at a time into
    // per-query bounded heaps (all queries share one stream).
    let reader_for_factory = reader.clone();
    let vector_field_for_factory = vector_field.clone();
    // The plan's own per-file selection, so an exact fallback reads only the rows an
    // engine-supplied split allows. `is_excluded` still rejects on top of it, but it
    // cannot un-read a row.
    let physical_for_factory = plan.physical_row_ranges_by_split.clone();
    let factory = as_split_exact_file_search(
        move |split_index: usize,
              split: &PkVectorSearchSplit,
              file: &BucketActiveFile,
              queries: &[&[f32]],
              metric: VectorSearchMetric,
              exact_limit: usize,
              is_excluded: &(dyn Fn(i64) -> bool + Sync)|
              -> ExactFileSearchFuture<'_> {
            let reader = reader_for_factory.clone();
            let vector_field = vector_field_for_factory.clone();
            let data_split = split.data_split.clone();
            let active = BucketActiveFile {
                file_name: file.file_name.clone(),
                row_count: file.row_count,
            };
            let owned_queries: Vec<Vec<f32>> = queries.iter().map(|q| q.to_vec()).collect();
            let allowed_rows = physical_for_factory.as_ref().and_then(|per_split| {
                per_split
                    .get(split_index)
                    .and_then(|per_file| per_file.get(&active.file_name))
                    .cloned()
            });
            Box::pin(async move {
                let factory = DataFilePkVectorReaderFactory::new(reader, data_split, vector_field)?;
                let query_refs: Vec<&[f32]> = owned_queries.iter().map(|q| q.as_slice()).collect();
                factory
                    .search_file(
                        &active,
                        &query_refs,
                        metric,
                        exact_limit,
                        is_excluded,
                        allowed_rows.as_deref(),
                    )
                    .await
            })
        },
    );

    // Resolve the refine factor from the query options first, then fall back to the
    // table options; a positive factor over-fetches indexed (approximate)
    // candidates so the exact rerank below has a wider pool to reorder. Factor 0
    // (unset) leaves `indexed_limit == limit`, byte-identical to the no-rerank
    // path. The two option maps are kept distinct (query options passed separately
    // from table options) so a broad query key cannot be overridden by a more
    // specific table key: query options take precedence as a whole. `search_options`
    // above is the merged view used only to drive the ANN read.

    let searches: Vec<OrchestratorSearchResult> = PkVectorOrchestrator::new(reader)
        .search_candidates_batch(
            &plan.splits,
            queries,
            metric,
            limit,
            indexed_limit,
            Some(ann_searcher),
            &factory,
            &search_options,
            skip_exact_fallback,
            row_ranges_by_split.as_deref(),
            concurrency,
        )
        .await?;

    Ok(searches)
}

/// Search an already-resolved plan and return one merged, best-first candidate list
/// per query: the raw layer above, followed by the optional exact rerank of the
/// approximate candidates and the merge with the exact-fallback candidates.
#[allow(clippy::too_many_arguments)]
async fn search_pk_candidates_batch_with_plan(
    table: &Table,
    query_options: &HashMap<String, String>,
    filter: Option<&Predicate>,
    core: &CoreOptions<'_>,
    pk_col: &str,
    queries: &[&[f32]],
    limit: usize,
    plan: &PkVectorScanPlan,
    params: &PkVectorSearchParams,
) -> crate::Result<Vec<Vec<PkVectorCandidate>>> {
    let searches = search_pk_raw_candidates_batch_with_plan(
        table,
        query_options,
        filter,
        core,
        pk_col,
        queries,
        limit,
        plan,
        params,
    )
    .await?;

    let metric = params.metric;
    let refine_factor = params.refine_factor;
    let vector_field = params.vector_field.clone();

    // Per query: exact rerank of the approximate candidates when a refine factor is
    // set (exact-fallback candidates are already exact and are not reranked), then
    // merge the (possibly reranked) indexed list with the exact list into one
    // best-first list bounded to the caller's limit. With no refine factor the
    // rerank is a plain merge, byte-identical to the no-rerank path. Each query
    // reranks its OWN indexed candidates.
    let mut per_query_candidates = Vec::with_capacity(searches.len());
    for (query_index, search) in searches.into_iter().enumerate() {
        let query_vector = queries[query_index];
        let indexed = if refine_factor > 0 && !search.indexed.is_empty() {
            // Vector-only reader (project just the vector field); the position read
            // appends _PKEY_VECTOR_POSITION itself and injects _ROW_ID internally.
            let rerank_reader = DataFileReader::new(
                table.file_io().clone(),
                table.schema_manager().clone(),
                table.schema().id(),
                table.schema().fields().to_vec(),
                vec![vector_field.clone()],
                Vec::new(),
            );
            rerank_indexed_positional(
                &rerank_reader,
                search.indexed,
                &plan.splits,
                query_vector,
                metric,
                limit,
                &vector_field,
            )
            .await?
        } else {
            search.indexed
        };
        per_query_candidates.push(merge_candidates(indexed, search.exact, limit));
    }

    Ok(per_query_candidates)
}

/// Read the plan's allowed physical rows and return merged ranges matching the
/// residual, as in Java `PrimaryKeyVectorRead.residualRowRanges`.
///
/// The reader projects predicate columns without pushing the predicate down, so
/// each emitted row can be mapped back to its physical position. This uses neither
/// `_ROW_ID` nor `first_row_id`. Ascending matches are coalesced as they arrive.
///
/// Missing `allowed_rows` entries permit the entire file; empty ranges skip it
/// without a read. Every active file gets an output entry, including empty results,
/// so a rejected file cannot become unrestricted in the search kernel. Inactive
/// files are not searched and need no residual read.
///
/// `reader` must be predicate-free; `residual.file_fields` resolves predicate
/// indices against each emitted batch by name.
async fn residual_row_ranges_by_file(
    reader: &DataFileReader,
    split: &DataSplit,
    active_files: &[BucketActiveFile],
    residual: &FilePredicates,
    allowed_rows: Option<&RowRangesByFile>,
) -> crate::Result<RowRangesByFile> {
    let scan_fields = reader.read_type().to_vec();
    let active_names: HashSet<&str> = active_files.iter().map(|f| f.file_name.as_str()).collect();
    let mut out: RowRangesByFile = HashMap::new();
    for file_meta in split.data_files() {
        // Only files the bucket search actually recalls from need residual
        // positions; skip everything else to avoid a wasted read.
        if !active_names.contains(file_meta.file_name.as_str()) {
            continue;
        }
        // A file the plan lists an EMPTY range list for permits nothing; registering
        // it empty says so and costs no read. A file the plan does not list at all
        // is unrestricted, so the residual is evaluated over the whole file.
        let selection = match allowed_rows.and_then(|by_file| by_file.get(&file_meta.file_name)) {
            Some(ranges) if ranges.is_empty() => {
                out.entry(file_meta.file_name.clone()).or_default();
                continue;
            }
            Some(ranges) => Some(ranges.clone()),
            None => None,
        };
        let data_fields = reader.derive_data_fields(file_meta).await?;
        let mut stream = match selection.clone() {
            Some(ranges) => reader.read_single_file_stream_local_ranges(
                split,
                file_meta.clone(),
                data_fields,
                None,
                ranges,
            )?,
            None => {
                reader.read_single_file_stream(split, file_meta.clone(), data_fields, None, None)?
            }
        };
        // Register the file up front so a file whose rows all fail the residual
        // still appears in the map (empty set).
        let ranges = out.entry(file_meta.file_name.clone()).or_default();
        // Rows arrive in ascending physical order, and the read emitted exactly what
        // was selected (no pushdown predicate, no deletion vector), so walking the
        // selection in step with the rows recovers each row's file-local position.
        let mut selected: Box<dyn Iterator<Item = i64> + Send> = match &selection {
            Some(ranges) => Box::new(
                ranges
                    .clone()
                    .into_iter()
                    .flat_map(|range| range.from()..=range.to()),
            ),
            None => Box::new(0..file_meta.row_count.max(0)),
        };
        while let Some(batch) = stream.try_next().await? {
            let num_rows = batch.num_rows();
            let mask = evaluate_predicates_mask(
                &batch,
                &residual.predicates,
                &residual.file_fields,
                &scan_fields,
            )?;
            for row_index in 0..num_rows {
                let position = selected.next().ok_or_else(|| crate::Error::DataInvalid {
                    message: format!(
                        "residual scan of '{}' emitted more rows than the selection allows",
                        file_meta.file_name
                    ),
                    source: None,
                })?;
                let keep = match &mask {
                    // NULL follows the same NULL -> false convention the Arrow filter
                    // kernel applies, so a null mask slot drops the row.
                    Some(mask) => mask.is_valid(row_index) && mask.value(row_index),
                    // No predicate contributed a mask (identity) -> keep every row.
                    None => true,
                };
                if keep {
                    // Reads return ascending physical positions, so coalesce
                    // consecutive matches directly into Java's range form.
                    match ranges.last_mut() {
                        Some(last) if last.to().checked_add(1) == Some(position) => {
                            *last = RowRange::new(last.from(), position);
                        }
                        _ => ranges.push(RowRange::new(position, position)),
                    }
                }
            }
        }
        if selected.next().is_some() {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "residual scan of '{}' emitted fewer rows than the selection allows",
                    file_meta.file_name
                ),
                source: None,
            });
        }
    }
    Ok(out)
}

fn verify_segment_metric(
    configured: VectorSearchMetric,
    segment_metric: VectorSearchMetric,
) -> crate::Result<()> {
    if segment_metric != configured {
        return Err(crate::Error::DataInvalid {
            message: format!(
                "ANN segment metric {} does not match configured metric {}",
                segment_metric.as_str(),
                configured.as_str()
            ),
            source: None,
        });
    }
    Ok(())
}

/// Rerank approximate (indexed) candidates by rereading ONLY their candidate
/// positions and recomputing the exact distance, then keep the best `limit`.
///
/// Unlike a whole-column preload, this reuses [`PkVectorPositionRead`] to read
/// just the selected physical rows of each hit file (positions -> row ranges ->
/// local ranges), so a rerank over a large ANN-covered file touches only the
/// candidate rows. Mirrors Java's IndexedSplit rerank.
///
/// Each returned row is matched back to its candidate by the
/// `_PKEY_VECTOR_POSITION` column VALUE (never batch order). The recomputed
/// distance is written into the ORIGINAL candidate so `split_index` /
/// partition / bucket survive (`build_indexed_splits` does not carry
/// `split_index`). A DV loaded exactly as [`PkVectorIndexedSplitRead::read`]
/// does drops deleted positions, so a candidate at a deleted position returns no
/// row and trips the leftover guard — a deleted candidate reaching rerank is a
/// real inconsistency (the search path already DV-filters), so fail loud.
#[allow(clippy::too_many_arguments)]
async fn rerank_indexed_positional(
    rerank_reader: &DataFileReader,
    indexed: Vec<PkVectorCandidate>,
    plan_splits: &[PkVectorSearchSplit],
    query_vector: &[f32],
    metric: VectorSearchMetric,
    limit: usize,
    vector_field: &DataField,
) -> crate::Result<Vec<PkVectorCandidate>> {
    // Original per-position candidates keyed by (split_index, file, position);
    // the recomputed distance is written back into these so split_index and
    // partition/bucket survive (build_indexed_splits does not carry split_index).
    let mut by_key: HashMap<(usize, String, i64), PkVectorCandidate> = HashMap::new();
    for c in &indexed {
        if by_key
            .insert(
                (c.split_index, c.data_file_name.clone(), c.row_position),
                c.clone(),
            )
            .is_some()
        {
            return Err(crate::Error::DataInvalid {
                message: "duplicate primary-key vector candidate for reranking".to_string(),
                source: None,
            });
        }
    }

    // Rebuild the split_index lookup by (partition bytes, bucket, file): the
    // indexed split exposes partition/bucket/file but not split_index.
    let mut split_index_of: HashMap<(Vec<u8>, i32, String), usize> = HashMap::new();
    for (i, s) in plan_splits.iter().enumerate() {
        let p = s.data_split.partition().to_serialized_bytes();
        let b = s.data_split.bucket();
        for f in s.data_split.data_files() {
            split_index_of.insert((p.clone(), b, f.file_name.clone()), i);
        }
    }

    // Every candidate must reference a (partition, bucket, file) that the plan
    // actually carries. Checking up front — before build_indexed_splits, which
    // indexes plan_splits by split_index — turns an absent file into a fail-loud
    // error rather than an out-of-range panic, and keeps the per-split lookup
    // below a self-consistent backstop.
    for c in &indexed {
        let key = (
            c.partition.to_serialized_bytes(),
            c.bucket,
            c.data_file_name.clone(),
        );
        if !split_index_of.contains_key(&key) {
            return Err(crate::Error::DataInvalid {
                message: format!("rerank split for {} not found in plan", c.data_file_name),
                source: None,
            });
        }
    }

    // Group the candidates into per-file indexed splits (position ranges + file
    // meta), reusing the exact grouping/validation the materialization path uses.
    let indexed_splits = build_indexed_splits(indexed, plan_splits, metric)?;

    let dimension = query_vector.len();
    let mut reranked: Vec<PkVectorCandidate> = Vec::new();
    for split in indexed_splits {
        let data_split = split.split.clone();
        let file_meta = data_split.data_files()[0].clone();
        let file_name = file_meta.file_name.clone();
        let partition_bytes = data_split.partition().to_serialized_bytes();
        let bucket = data_split.bucket();
        let split_index = *split_index_of
            .get(&(partition_bytes, bucket, file_name.clone()))
            .ok_or_else(|| crate::Error::DataInvalid {
                message: format!("rerank split for {file_name} not found in plan"),
                source: None,
            })?;

        // DV loaded exactly as PkVectorIndexedSplitRead::read does; skipping it
        // would score deleted rows.
        let dv_factory = rerank_reader.build_split_dv_factory(&data_split).await?;
        let dv = DataFileReader::deletion_vector_for_file(dv_factory.as_ref(), &file_name);
        let data_fields = rerank_reader.derive_data_fields(&file_meta).await?;

        // Positions from the split's row_ranges (ascending); read only those.
        let positions = expand_ranges(&split.row_ranges, file_meta.row_count)?;
        let mut stream = PkVectorPositionRead::new(rerank_reader).read(
            &data_split,
            file_meta,
            data_fields,
            dv,
            positions,
            None, // no scores; rerank recomputes distance
        )?;

        while let Some(batch) = stream.try_next().await? {
            let pos_idx = batch
                .schema()
                .index_of(PKEY_VECTOR_POSITION_COLUMN)
                .map_err(|_| crate::Error::DataInvalid {
                    message: format!("rerank batch missing {PKEY_VECTOR_POSITION_COLUMN} column"),
                    source: None,
                })?;
            let pos_col = batch
                .column(pos_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: format!("{PKEY_VECTOR_POSITION_COLUMN} column is not Int64"),
                    source: None,
                })?;
            let mut vectors: Vec<Option<Vec<f32>>> = Vec::new();
            append_batch_vectors(&batch, vector_field.name(), dimension, &mut vectors)?;
            for (row, vector) in vectors.iter().enumerate() {
                let position = pos_col.value(row);
                let mut candidate = by_key
                    .remove(&(split_index, file_name.clone(), position))
                    .ok_or_else(|| crate::Error::DataInvalid {
                        message: format!("rerank read unexpected position {file_name}@{position}"),
                        source: None,
                    })?;
                let vector = vector.as_ref().ok_or_else(|| crate::Error::DataInvalid {
                    message: format!(
                        "primary-key vector candidate {file_name}@{position} contains a null vector"
                    ),
                    source: None,
                })?;
                candidate.distance = metric.compute_distance(query_vector, vector);
                reranked.push(candidate);
            }
        }
    }

    if !by_key.is_empty() {
        return Err(crate::Error::DataInvalid {
            message: format!(
                "failed to read {} primary-key vector candidate(s) for reranking",
                by_key.len()
            ),
            source: None,
        });
    }

    Ok(merge_candidates(reranked, Vec::new(), limit))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod residual_row_ranges_tests;
