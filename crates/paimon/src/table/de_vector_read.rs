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

//! Reads global vector indexes, scores raw vectors, and materializes global-row-ID results.

use crate::io::{FileIO, FileRead};
use crate::lumina::reader::LuminaVectorGlobalIndexReader;
use crate::lumina::{LuminaIndexMeta, LuminaVectorMetric};
use crate::spec::{
    row_id_data_field, CoreOptions, DataField, DataType, FileKind, GlobalIndexSearchMode,
    IndexFileMeta, IndexManifestEntry, ROW_ID_FIELD_NAME,
};
use crate::table::de_vector_scan::DeVectorScanPlan;
use crate::table::global_index_scanner::{
    deleted_row_ranges_for_data_evolution_dvs, search_limit_with_deleted_rows,
    unindexed_ranges_for_global_index_entries, RowRangeIndex,
};
use crate::table::index_file_path::IndexFileLocation;
use crate::table::pk_vector_position_read::SEARCH_SCORE_COLUMN;
use crate::table::row_id_predicate::intersect_sorted_ranges;
use crate::table::vector_read::Read;
use crate::table::vector_search_common::{
    configured_refine_factor, indexed_search_limit, log_vindex_range_io_stats, normalize_metric,
    resolve_materialize_read_type, vindex_concurrency_limits, VectorIndexBackend,
};
use crate::table::{
    find_field_id_by_name, merge_row_ranges, ArrowRecordBatchStream, RowRange, Table,
};
use crate::vector_search::{GlobalIndexIOMeta, ScoredRowIds, SearchResult, VectorSearch};
use crate::vindex::executor::{
    acquire_process_global_search_permit, drain_indexed_jobs,
    ensure_global_index_executor_capacity, execute_global_index_with_guard,
};
use crate::vindex::range_reader::{RangeReadLimiter, VindexFileReader};
use crate::vindex::reader::VindexVectorGlobalIndexReader;
use crate::vindex::{is_vindex_index_type, vector_search_timing_enabled};
use arrow_array::{Array, FixedSizeListArray, Float32Array, Int64Array, ListArray, RecordBatch};
use arrow_select::interleave::interleave_record_batch;
use futures::{stream, TryStreamExt};
use paimon_vindex_core::blas::sgemm_a_bt;
use paimon_vindex_core::diskann_io::DISKANN_HEADER_SIZE;
use paimon_vindex_core::distance::MetricType;
use paimon_vindex_core::index::VectorIndexReader as VIndexReader;
use paimon_vindex_core::io::SeekRead;
use roaring::RoaringTreemap;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Owns the queries and executes the snapshot-scoped plan from `DeVectorScan`.
pub(super) struct DeVectorRead {
    vector_searches: Vec<VectorSearch>,
}

impl DeVectorRead {
    /// Validate query parameters before planning, including for empty tables and
    /// filters. Snapshot-dependent row-ID filters are supplied by the plan.
    pub(super) fn new(
        vector_column: &str,
        queries: &[&[f32]],
        limit: usize,
        options: &HashMap<String, String>,
    ) -> crate::Result<Self> {
        if vector_column.is_empty() {
            return Err(crate::Error::ConfigInvalid {
                message: "Vector column must be set via with_vector_column()".to_string(),
            });
        }
        if queries.is_empty() {
            return Err(crate::Error::ConfigInvalid {
                message: "Query vectors must be set via with_query_vectors()".to_string(),
            });
        }
        let vector_searches = queries
            .iter()
            .map(|query| {
                VectorSearch::new(query.to_vec(), limit, vector_column.to_string())
                    .map(|search| search.with_options(options.clone()))
            })
            .collect::<crate::Result<_>>()?;
        Ok(Self { vector_searches })
    }

    /// Validate a result read, including projections over an empty result.
    pub(super) fn read_type(
        table: &Table,
        vector_column: &str,
        projection: Option<&[String]>,
    ) -> crate::Result<Vec<DataField>> {
        // Validate the target column exists and is a vector-bearing type before any
        // work. The data-evolution search returns an empty result for an unknown
        // field (its scored-path behavior), which would make a typo'd or scalar
        // column look like a normal empty read here — violating the result reader's
        // fail-loud contract (a C/Doris caller would see EOF, not an input error).
        // Reject it up front instead.
        let field = table
            .schema()
            .fields()
            .iter()
            .find(|f| f.name() == vector_column)
            .ok_or_else(|| crate::Error::DataInvalid {
                message: format!("vector search column '{vector_column}' does not exist"),
                source: None,
            })?;
        // Require a FLOAT-element vector column: `ARRAY<FLOAT>` or `VECTOR<FLOAT>`,
        // matching the element type the vector index/search operates on. An
        // `ARRAY<INT>` (or any non-float element) is not a searchable vector column.
        let is_float_vector = match field.data_type() {
            DataType::Vector(t) => matches!(t.element_type(), DataType::Float(_)),
            DataType::Array(t) => matches!(t.element_type(), DataType::Float(_)),
            _ => false,
        };
        if !is_float_vector {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "vector search column '{vector_column}' must be a FLOAT vector column \
                     (ARRAY<FLOAT> or VECTOR<FLOAT>), got {:?}",
                    field.data_type()
                ),
                source: None,
            });
        }

        resolve_materialize_read_type(table, projection)
    }
}

pub(super) async fn materialize_row_ids(
    pinned_table: &Table,
    sr: &ScoredRowIds,
    mut read_type: Vec<DataField>,
) -> crate::Result<ArrowRecordBatchStream> {
    if sr.is_empty() {
        return Ok(Box::pin(stream::empty()));
    }
    // rank = ordinal in the best-first scored result; score = the aligned score.
    // Build ranges first (validates ids fit in i64::MAX) before constructing the map.
    let ranges = sr.to_row_ranges()?;
    let mut rank_score_of: HashMap<i64, (usize, f32)> = HashMap::new();
    for (rank, (&id, &score)) in sr.row_ids.iter().zip(sr.scores.iter()).enumerate() {
        rank_score_of.insert(id as i64, (rank, score));
    }

    // Add _ROW_ID as the join key for score alignment; it is stripped before output.
    if !read_type.iter().any(|f| f.name() == ROW_ID_FIELD_NAME) {
        read_type.push(row_id_data_field());
    }

    let mut read_builder = pinned_table.new_read_builder();
    read_builder
        .with_read_type(read_type)
        .with_row_ranges(ranges);
    let scan = read_builder.new_scan();
    let plan = scan.plan().await?;
    let table_read = read_builder.new_read()?;
    let mut stream = table_read.to_arrow(plan.splits())?;

    let mut batches: Vec<RecordBatch> = Vec::new();
    while let Some(batch) = stream.try_next().await? {
        batches.push(batch);
    }
    let output = attach_scores_by_row_id(&batches, &rank_score_of, sr.len())?;
    Ok(Box::pin(stream::iter(output.into_iter().map(Ok))))
}

impl Read for DeVectorRead {
    type Plan = DeVectorScanPlan;

    async fn read(&self, plan: DeVectorScanPlan) -> crate::Result<Vec<SearchResult>> {
        let DeVectorScanPlan {
            table,
            index_entries,
            include_row_ids,
            next_row_id,
            timing,
            skip_search,
        } = plan;
        let table = Arc::new(table);
        let make_results = |results: Vec<ScoredRowIds>| {
            results
                .into_iter()
                .zip(&self.vector_searches)
                .map(|(hits, query)| {
                    SearchResult::from_row_ids(table.clone(), query.field_name.clone(), hits)
                })
                .collect()
        };
        let pinned_table = &table;
        let timing_enabled = timing.is_some();
        let (total_start, setup, snapshot_elapsed, manifest) = match timing {
            Some(timing) => (
                Some(timing.total_start),
                timing.setup,
                timing.snapshot,
                timing.manifest,
            ),
            None => (None, Duration::ZERO, Duration::ZERO, Duration::ZERO),
        };
        let evaluate_start = (timing_enabled && !skip_search).then(Instant::now);
        let results = if skip_search {
            vec![ScoredRowIds::empty(); self.vector_searches.len()]
        } else {
            let mut vector_searches = Cow::Borrowed(self.vector_searches.as_slice());
            if let Some(include_row_ids) = include_row_ids {
                for search in vector_searches.to_mut() {
                    search.set_shared_include_row_ids(Arc::clone(&include_row_ids));
                }
            }
            evaluate_batch_vector_search(
                VectorSearchEvaluation {
                    table: Some(pinned_table),
                    file_io: pinned_table.file_io(),
                    table_path: pinned_table.location(),
                    table_options: pinned_table.schema().options(),
                    schema_fields: pinned_table.schema().fields(),
                    next_row_id,
                },
                &index_entries,
                &vector_searches,
            )
            .await?
        };
        if let Some(total_start) = total_start {
            let total = total_start.elapsed();
            let evaluate = evaluate_start.map_or(Duration::ZERO, |start| start.elapsed());
            let children = setup
                .saturating_add(snapshot_elapsed)
                .saturating_add(manifest)
                .saturating_add(evaluate);
            let result_count = results
                .iter()
                .map(|result| result.row_ids.len())
                .sum::<usize>();
            log::debug!(
                target: "paimon::vector_search",
                "event=paimon_vector_search_api nq={} index_entries={} result_count={} total_ms={:.3} setup_ms={:.3} snapshot_ms={:.3} manifest_ms={:.3} evaluate_ms={:.3} unattributed_ms={:.3}",
                self.vector_searches.len(),
                index_entries.len(),
                result_count,
                total.as_secs_f64() * 1000.0,
                setup.as_secs_f64() * 1000.0,
                snapshot_elapsed.as_secs_f64() * 1000.0,
                manifest.as_secs_f64() * 1000.0,
                evaluate.as_secs_f64() * 1000.0,
                total.saturating_sub(children).as_secs_f64() * 1000.0,
            );
        }
        Ok(make_results(results))
    }
}

const RAW_SCORE_MATRIX_MIN_QUERY_COUNT: usize = 4;

const RAW_SCORE_MATRIX_TARGET_ELEMENTS: usize = 1 << 20;

const RAW_TOP_K_MIN_PARTITION_SIZE: usize = 1 << 12;

async fn execute_vindex_searches<S: SeekRead + 'static, G: Send + 'static>(
    io_meta: GlobalIndexIOMeta,
    options: HashMap<String, String>,
    vector_searches: Vec<VectorSearch>,
    source: S,
    file_name: String,
    index_parallelism: usize,
    guard: G,
) -> crate::Result<Vec<Option<HashMap<u64, f32>>>> {
    let panic_context = if vector_searches.len() > 1 {
        "vindex global-index batch search task failed"
    } else {
        "vindex global-index search task failed"
    };
    execute_global_index_with_guard(panic_context, guard, move || {
        let mut reader = VindexVectorGlobalIndexReader::new(io_meta, options)
            .with_batch_index_parallelism(index_parallelism);
        reader
            .visit_batch_vector_search(&vector_searches, |_| Ok(source))
            .map_err(|e| crate::Error::DataInvalid {
                message: format!("Failed to read vindex index file '{}': {}", file_name, e),
                source: Some(Box::new(e)),
            })
    })
    .await
}

#[derive(Clone, Copy)]
struct VectorSearchEvaluation<'a> {
    table: Option<&'a Table>,
    file_io: &'a FileIO,
    table_path: &'a str,
    table_options: &'a HashMap<String, String>,
    schema_fields: &'a [DataField],
    next_row_id: Option<i64>,
}

#[derive(Default)]
struct IndexSearchTiming {
    permit_wait: Duration,
    file_reader_open: Duration,
}

#[cfg(test)]
async fn evaluate_vector_search(
    evaluation: VectorSearchEvaluation<'_>,
    index_entries: &[IndexManifestEntry],
    vector_search: &VectorSearch,
) -> crate::Result<Vec<RowRange>> {
    let results = evaluate_batch_vector_search(
        evaluation,
        index_entries,
        std::slice::from_ref(vector_search),
    )
    .await?;
    crate::table::vector_search_common::take_only_result(results, "vector search")?.to_row_ranges()
}

async fn evaluate_batch_vector_search(
    evaluation: VectorSearchEvaluation<'_>,
    index_entries: &[IndexManifestEntry],
    vector_searches: &[VectorSearch],
) -> crate::Result<Vec<ScoredRowIds>> {
    let timing_enabled = vector_search_timing_enabled();
    let total_start = timing_enabled.then(Instant::now);
    if vector_searches.is_empty() {
        return Ok(Vec::new());
    }

    let table_path = evaluation.table_path.trim_end_matches('/');
    let core_options = CoreOptions::new(evaluation.table_options);
    let search_mode = core_options.vector_index_search_mode()?;
    let field_name = &vector_searches[0].field_name;
    if vector_searches
        .iter()
        .any(|vector_search| vector_search.field_name != *field_name)
    {
        return Err(crate::Error::DataInvalid {
            message: "Batch vector search requires all query vectors to use the same field"
                .to_string(),
            source: None,
        });
    }
    let search_options = vector_searches[0].options.clone();
    if vector_searches
        .iter()
        .any(|vector_search| vector_search.options != search_options)
    {
        return Err(crate::Error::DataInvalid {
            message: "Batch vector search requires all query vectors to use the same options"
                .to_string(),
            source: None,
        });
    }

    let field_id = match find_field_id_by_name(evaluation.schema_fields, field_name) {
        Some(id) => id,
        None => return Ok(vec![ScoredRowIds::empty(); vector_searches.len()]),
    };

    let vector_entries: Vec<_> = index_entries
        .iter()
        .filter(|e| {
            e.kind == FileKind::Add
                && VectorIndexBackend::from_index_type(&e.index_file.index_type).is_some()
                && e.index_file
                    .global_index_meta
                    .as_ref()
                    .is_some_and(|m| m.index_field_id == field_id)
        })
        .collect();

    if vector_entries.is_empty() && search_mode == GlobalIndexSearchMode::Fast {
        return Ok(vec![ScoredRowIds::empty(); vector_searches.len()]);
    }

    let deletion_vector_start = timing_enabled.then(Instant::now);
    let deleted_row_index = if core_options.data_evolution_enabled() {
        match evaluation.table {
            Some(table) => {
                let ranges =
                    deleted_row_ranges_for_data_evolution_dvs(table, index_entries).await?;
                (!ranges.is_empty()).then(|| RowRangeIndex::create(ranges))
            }
            None => None,
        }
    } else {
        None
    };
    let deletion_vector = deletion_vector_start.map_or(Duration::ZERO, |start| start.elapsed());

    let max_limit = vector_searches
        .iter()
        .map(|vector_search| vector_search.limit)
        .max()
        .unwrap_or(0);
    let refine_factor = match vector_entries.first() {
        Some(entry) => configured_refine_factor(
            &search_options,
            evaluation.table_options,
            field_name,
            &entry.index_file.index_type,
        )?,
        None => 0,
    };
    let index_search_limit = indexed_search_limit(max_limit, refine_factor)?;

    let vector_entry_count = vector_entries.len();
    let vector_search_plans = if let Some(include_row_ids) =
        shared_batch_include_row_ids(vector_searches)
    {
        let ranges = vector_entries
            .iter()
            .map(|entry| {
                let meta = entry.index_file.global_index_meta.as_ref().ok_or_else(|| {
                    crate::Error::DataInvalid {
                        message: format!(
                            "Vector index '{}' is missing global index metadata",
                            entry.index_file.file_name
                        ),
                        source: None,
                    }
                })?;
                Ok((meta.row_range_start, meta.row_range_end))
            })
            .collect::<crate::Result<Vec<_>>>()?;
        vector_entries
            .iter()
            .copied()
            .zip(localize_shared_include_row_ids(
                include_row_ids.as_ref(),
                &ranges,
            )?)
            .filter_map(|(entry, local_filter)| local_filter.map(|filter| (entry, Some(filter))))
            .collect::<Vec<_>>()
    } else {
        vector_entries
            .iter()
            .copied()
            .map(|entry| (entry, None))
            .collect::<Vec<_>>()
    };
    let mut permit_wait = Duration::ZERO;
    let mut file_reader_open = Duration::ZERO;
    let mut index_search = Duration::ZERO;
    let mut merge = Duration::ZERO;
    let mut refine = Duration::ZERO;
    let mut raw_fallback = Duration::ZERO;
    let mut merged = vec![ScoredRowIds::empty(); vector_searches.len()];
    if !vector_entries.is_empty() {
        let index_search_start = timing_enabled.then(Instant::now);
        let concurrency = core_options.global_index_thread_num()?;
        if concurrency > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Global index thread count must not exceed {}",
                    tokio::sync::Semaphore::MAX_PERMITS
                ),
                source: None,
            });
        }
        ensure_global_index_executor_capacity(concurrency);
        let vindex_entry_count = vector_entries
            .iter()
            .filter(|entry| is_vindex_index_type(&entry.index_file.index_type))
            .count();
        let (batch_index_parallelism, range_read_limiter) = if vindex_entry_count == 0 {
            (1, None)
        } else {
            let (index_parallelism, range_read_concurrency) =
                vindex_concurrency_limits(&core_options, vindex_entry_count, concurrency)?;
            (
                index_parallelism,
                Some(RangeReadLimiter::new(range_read_concurrency)),
            )
        };
        let futures: Vec<_> = vector_search_plans
            .into_iter()
            .map(|(entry, shared_local_filter)| {
                let range_read_limiter = range_read_limiter.clone();
                let global_meta = entry.index_file.global_index_meta.as_ref().unwrap();
                let backend = VectorIndexBackend::from_index_type(&entry.index_file.index_type)
                    .expect("filtered vector index type");
                let path = IndexFileLocation::Global { table_path }
                    .resolve(&entry.index_file.file_name, entry.index_file.external_path.as_deref());
                let file_name = entry.index_file.file_name.clone();
                let file_size = entry.index_file.file_size as u64;
                let index_meta_bytes = global_meta.index_meta.clone().unwrap_or_default();
                let row_range_start = global_meta.row_range_start;
                let row_range_end = global_meta.row_range_end;
                let index_limit = search_limit_with_deleted_rows(
                    index_search_limit,
                    row_range_start,
                    row_range_end,
                    deleted_row_index.as_ref(),
                )
                .min(i32::MAX as usize);
                let mut vector_searches = vector_searches.to_vec();
                for vector_search in &mut vector_searches {
                    vector_search.limit = index_limit;
                }
                let mut options = evaluation.table_options.clone();
                options.extend(search_options.clone());
                let input = evaluation.file_io.new_input(&path);
                async move {
                    if let Some(local_filter) = shared_local_filter {
                        let local_filter = Arc::new(local_filter);
                        for vector_search in &mut vector_searches {
                            vector_search
                                .set_shared_include_row_ids(Arc::clone(&local_filter));
                        }
                    } else {
                        for vector_search in &mut vector_searches {
                            if let Some(include_row_ids) =
                                vector_search.effective_include_row_ids()
                            {
                                vector_search.set_shared_include_row_ids(Arc::new(
                                    localize_include_row_ids(
                                        include_row_ids,
                                        row_range_start,
                                        row_range_end,
                                    )?,
                                ));
                            }
                        }
                    }
                    if vector_searches.iter().all(|search| {
                        search
                            .effective_include_row_ids()
                            .is_some_and(|row_ids| row_ids.is_empty())
                    }) {
                        return Ok((
                            vec![ScoredRowIds::empty(); vector_searches.len()],
                            IndexSearchTiming::default(),
                        ));
                    }
                    let permit_start = timing_enabled.then(Instant::now);
                    let permit = acquire_process_global_search_permit(concurrency).await?;
                    let permit_wait =
                        permit_start.map_or(Duration::ZERO, |start| start.elapsed());
                    let input = input?;
                    let query_count = vector_searches.len();
                    let mut file_reader_open = Duration::ZERO;
                    let mut full_file_read = None;
                    let io_meta =
                        GlobalIndexIOMeta::new(file_name.clone(), file_size, index_meta_bytes);
                    let results = match backend {
                        VectorIndexBackend::Lumina => {
                            let read_start = timing_enabled.then(Instant::now);
                            let data = input.read().await.map_err(|e| {
                                crate::Error::DataInvalid {
                                    message: format!(
                                        "Failed to read {} index file '{}': {}",
                                        backend.error_name(),
                                        file_name,
                                        e
                                    ),
                                    source: None,
                                }
                            })?;
                            if let Some(start) = read_start {
                                full_file_read = Some((start.elapsed(), data.len()));
                            }
                            execute_global_index_with_guard(
                                "Lumina global-index batch search task failed",
                                permit,
                                move || {
                                    let mut reader =
                                        LuminaVectorGlobalIndexReader::new(io_meta, options);
                                    reader.visit_batch_vector_search(&vector_searches, |_| {
                                        Ok(Cursor::new(data))
                                    })
                                },
                            )
                            .await?
                        }
                        VectorIndexBackend::Vindex => {
                            match tokio::runtime::Handle::try_current() {
                                Ok(runtime) => {
                                    let file_reader_open_start =
                                        timing_enabled.then(Instant::now);
                                    let file_reader = input.reader().await.map_err(|e| {
                                        crate::Error::DataInvalid {
                                            message: format!(
                                                "Failed to open vindex file '{}' for range reads: {}",
                                                file_name, e
                                            ),
                                            source: None,
                                        }
                                    })?;
                                    file_reader_open = file_reader_open_start
                                        .map_or(Duration::ZERO, |start| start.elapsed());
                                    let source = VindexFileReader::new_with_limiter(
                                        Arc::new(file_reader),
                                        runtime,
                                        range_read_limiter.expect("Vindex range-read limiter"),
                                        file_size,
                                        file_name.clone(),
                                    );
                                    let range_io_stats = source.range_io_stats();
                                    let results = execute_vindex_searches(
                                        io_meta,
                                        options,
                                        vector_searches,
                                        source,
                                        file_name.clone(),
                                        batch_index_parallelism,
                                        permit,
                                    )
                                    .await?;
                                    if let Some(stats) = range_io_stats {
                                        log_vindex_range_io_stats(
                                            &file_name,
                                            query_count,
                                            &stats,
                                        );
                                    }
                                    results
                                }
                                Err(_) if query_count > 1 => {
                                    let read_start = timing_enabled.then(Instant::now);
                                    let data = input.read().await.map_err(|e| {
                                        crate::Error::DataInvalid {
                                            message: format!(
                                                "Failed to read vindex index file '{}': {}",
                                                file_name, e
                                            ),
                                            source: None,
                                        }
                                    })?;
                                    if let Some(start) = read_start {
                                        full_file_read = Some((start.elapsed(), data.len()));
                                    }
                                    execute_vindex_searches(
                                        io_meta,
                                        options,
                                        vector_searches,
                                        Cursor::new(data),
                                        file_name.clone(),
                                        batch_index_parallelism,
                                        permit,
                                    )
                                    .await?
                                }
                                Err(error) => {
                                    return Err(crate::Error::UnexpectedError {
                                        message:
                                            "Vector index range reader requires a Tokio runtime"
                                                .to_string(),
                                        source: Some(Box::new(error)),
                                    });
                                }
                            }
                        }
                    };
                    if let Some((read, returned_bytes)) = full_file_read {
                        log::debug!(
                            target: "paimon::vector_search",
                            "event=paimon_vector_full_file_io backend={} file={} nq={} requested_bytes={} returned_bytes={} read_ms={:.3}",
                            backend.error_name(),
                            file_name,
                            query_count,
                            file_size,
                            returned_bytes,
                            read.as_secs_f64() * 1000.0,
                        );
                    }
                    if results.len() != query_count {
                        return Err(crate::Error::DataInvalid {
                            message: format!(
                                "Batch vector search backend returned {} results for {} query vectors",
                                results.len(),
                                query_count
                            ),
                            source: None,
                        });
                    }

                    Ok::<_, crate::Error>((
                        results
                            .into_iter()
                            .map(|result| match result {
                                Some(scored_map) => ScoredRowIds::from_scored_map(scored_map)
                                    .offset(row_range_start),
                                None => ScoredRowIds::empty(),
                            })
                            .collect::<Vec<_>>(),
                        IndexSearchTiming {
                            permit_wait,
                            file_reader_open,
                        },
                    ))
                }
            })
            .collect();

        let results = drain_indexed_jobs(futures.into_iter(), concurrency).await?;
        index_search = index_search_start.map_or(Duration::ZERO, |start| start.elapsed());
        let merge_start = timing_enabled.then(Instant::now);
        for (per_entry, entry_timing) in &results {
            permit_wait = permit_wait.saturating_add(entry_timing.permit_wait);
            file_reader_open = file_reader_open.saturating_add(entry_timing.file_reader_open);
            for (query_index, result) in per_entry.iter().enumerate() {
                merged[query_index] = merged[query_index].or(result);
            }
        }
        merge = merge_start.map_or(Duration::ZERO, |start| start.elapsed());
    }

    if refine_factor != 0 {
        let refine_start = timing_enabled.then(Instant::now);
        merged = maybe_rerank_indexed_batch_results(
            evaluation,
            index_entries,
            field_id,
            field_name,
            vector_searches,
            merged,
            index_search_limit,
        )
        .await?;
        refine = refine_start.map_or(Duration::ZERO, |start| start.elapsed());
    }

    if search_mode != GlobalIndexSearchMode::Fast {
        let raw_fallback_start = timing_enabled.then(Instant::now);
        let detail_ranges = if search_mode == GlobalIndexSearchMode::Detail {
            let table = evaluation.table.ok_or_else(|| crate::Error::DataInvalid {
                message: "Vector raw search in detail mode requires table context".to_string(),
                source: None,
            })?;
            detail_data_ranges_for_table(table).await?
        } else {
            Vec::new()
        };
        let field_ids = HashSet::from([field_id]);
        let raw_ranges = unindexed_ranges_for_global_index_entries(
            index_entries,
            &field_ids,
            search_mode,
            evaluation.next_row_id,
            &detail_ranges,
            is_vector_global_index_file,
        );
        if !raw_ranges.is_empty() {
            let table = evaluation.table.ok_or_else(|| crate::Error::DataInvalid {
                message: "Vector raw search requires table context".to_string(),
                source: None,
            })?;
            let metric_start = timing_enabled.then(Instant::now);
            let metric = resolve_raw_vector_metric(
                evaluation.file_io,
                table_path,
                evaluation.table_options,
                index_entries,
                field_id,
                field_name,
            )
            .await?;
            let metric_resolve = metric_start.map_or(Duration::ZERO, |start| start.elapsed());
            let (raw_results, raw_timing) =
                read_raw_batch_vector_search(table, vector_searches, &raw_ranges, metric).await?;
            if let Some(raw_timing) = raw_timing {
                log::debug!(
                    target: "paimon::vector_search",
                    "event=paimon_vector_raw_fallback nq={} row_ranges={} metric_resolve_ms={:.3} raw_plan_ms={:.3} split_count={} file_count={} raw_stream_wait_ms={:.3} raw_score_cpu_ms={:.3} arrow_batches={} arrow_rows={} total_raw_read_ms={:.3}",
                    vector_searches.len(),
                    raw_ranges.len(),
                    metric_resolve.as_secs_f64() * 1000.0,
                    raw_timing.plan.as_secs_f64() * 1000.0,
                    raw_timing.split_count,
                    raw_timing.file_count,
                    raw_timing.stream_wait.as_secs_f64() * 1000.0,
                    raw_timing.score_cpu.as_secs_f64() * 1000.0,
                    raw_timing.batch_count,
                    raw_timing.row_count,
                    raw_timing.total.as_secs_f64() * 1000.0,
                );
            }
            for (query_index, result) in raw_results.iter().enumerate() {
                merged[query_index] = merged[query_index].or(result);
            }
        }
        raw_fallback = raw_fallback_start.map_or(Duration::ZERO, |start| start.elapsed());
    }

    let finalize_start = timing_enabled.then(Instant::now);
    let results = merged
        .into_iter()
        .zip(vector_searches)
        .map(|(result, vector_search)| {
            Ok(result
                .without_deleted_row_ranges(deleted_row_index.as_ref())?
                .top_k(vector_search.limit))
        })
        .collect::<crate::Result<Vec<_>>>()?;
    let finalize = finalize_start.map_or(Duration::ZERO, |start| start.elapsed());
    if let Some(total_start) = total_start {
        let total = total_start.elapsed();
        let children = deletion_vector
            .saturating_add(index_search)
            .saturating_add(merge)
            .saturating_add(refine)
            .saturating_add(raw_fallback)
            .saturating_add(finalize);
        let result_count = results
            .iter()
            .map(|result| result.row_ids.len())
            .sum::<usize>();
        log::debug!(
            target: "paimon::vector_search",
            "event=paimon_vector_search_evaluate nq={} index_entries={} index_files={} result_count={} refine_factor={} total_ms={:.3} deletion_vector_ms={:.3} index_search_ms={:.3} global_permit_wait_sum_ms={:.3} file_reader_open_sum_ms={:.3} merge_ms={:.3} refine_ms={:.3} raw_fallback_ms={:.3} finalize_ms={:.3} unattributed_ms={:.3}",
            vector_searches.len(),
            index_entries.len(),
            vector_entry_count,
            result_count,
            refine_factor,
            total.as_secs_f64() * 1000.0,
            deletion_vector.as_secs_f64() * 1000.0,
            index_search.as_secs_f64() * 1000.0,
            permit_wait.as_secs_f64() * 1000.0,
            file_reader_open.as_secs_f64() * 1000.0,
            merge.as_secs_f64() * 1000.0,
            refine.as_secs_f64() * 1000.0,
            raw_fallback.as_secs_f64() * 1000.0,
            finalize.as_secs_f64() * 1000.0,
            total.saturating_sub(children).as_secs_f64() * 1000.0,
        );
    }
    Ok(results)
}

fn is_vector_global_index_file(index_file: &IndexFileMeta) -> bool {
    VectorIndexBackend::from_index_type(&index_file.index_type).is_some()
}

/// Collect materialized DE rows, join each row's `(rank, score)` by its global
/// `_ROW_ID`, reorder to the search rank order, append the `__paimon_search_score`
/// column, and drop `_ROW_ID`. Every row must map to a search candidate and the
/// total materialized count must equal `expected_len`; a miss or count mismatch
/// fails loud rather than silently dropping or NaN-scoring a row. Empty input
/// yields no batches.
fn attach_scores_by_row_id(
    batches: &[RecordBatch],
    rank_score_of: &HashMap<i64, (usize, f32)>,
    expected_len: usize,
) -> crate::Result<Vec<RecordBatch>> {
    // (rank, batch_index, row_index, score) per materialized row.
    let mut ranked: Vec<(usize, usize, usize, f32)> = Vec::new();
    for (batch_index, batch) in batches.iter().enumerate() {
        let row_id_idx =
            batch
                .schema()
                .index_of(ROW_ID_FIELD_NAME)
                .map_err(|_| crate::Error::DataInvalid {
                    message: format!("materialized batch missing {ROW_ID_FIELD_NAME} column"),
                    source: None,
                })?;
        let col = batch.column(row_id_idx);
        let ids =
            col.as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: format!("{ROW_ID_FIELD_NAME} column is not Int64"),
                    source: None,
                })?;
        for row_index in 0..batch.num_rows() {
            if ids.is_null(row_index) {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "materialized DE vector row has null {ROW_ID_FIELD_NAME}; cannot align score"
                    ),
                    source: None,
                });
            }
            let id = ids.value(row_index);
            let (rank, score) =
                *rank_score_of
                    .get(&id)
                    .ok_or_else(|| crate::Error::DataInvalid {
                        message: format!(
                        "materialized DE vector row (row id {id}) has no matching search candidate"
                    ),
                        source: None,
                    })?;
            ranked.push((rank, batch_index, row_index, score));
        }
    }

    if ranked.len() != expected_len {
        return Err(crate::Error::DataInvalid {
            message: format!(
                "DE vector materialization produced {} rows but search returned {expected_len}",
                ranked.len()
            ),
            source: None,
        });
    }
    if ranked.is_empty() {
        return Ok(Vec::new());
    }

    ranked.sort_by_key(|r| r.0);
    let indices: Vec<(usize, usize)> = ranked.iter().map(|r| (r.1, r.2)).collect();
    let refs: Vec<&RecordBatch> = batches.iter().collect();
    let reordered =
        interleave_record_batch(&refs, &indices).map_err(|e| crate::Error::DataInvalid {
            message: format!("failed to reorder DE vector search rows: {e}"),
            source: None,
        })?;

    // Drop _ROW_ID.
    let row_id_idx = reordered
        .schema()
        .index_of(ROW_ID_FIELD_NAME)
        .map_err(|_| crate::Error::DataInvalid {
            message: format!("reordered batch missing {ROW_ID_FIELD_NAME} column"),
            source: None,
        })?;
    let keep: Vec<usize> = (0..reordered.num_columns())
        .filter(|i| *i != row_id_idx)
        .collect();
    let stripped = reordered
        .project(&keep)
        .map_err(|e| crate::Error::DataInvalid {
            message: format!("failed to drop {ROW_ID_FIELD_NAME} column: {e}"),
            source: None,
        })?;

    // Append the score column in rank order.
    let scores: Vec<f32> = ranked.iter().map(|r| r.3).collect();
    let score_array: Arc<dyn Array> = Arc::new(Float32Array::from(scores));
    let mut fields: Vec<Arc<arrow_schema::Field>> =
        stripped.schema().fields().iter().cloned().collect();
    fields.push(Arc::new(arrow_schema::Field::new(
        SEARCH_SCORE_COLUMN,
        arrow_schema::DataType::Float32,
        false,
    )));
    let out_schema = Arc::new(arrow_schema::Schema::new(fields));
    let mut columns = stripped.columns().to_vec();
    columns.push(score_array);
    let out = RecordBatch::try_new(out_schema, columns).map_err(|e| crate::Error::DataInvalid {
        message: format!("failed to append DE vector score column: {e}"),
        source: None,
    })?;
    Ok(vec![out])
}

async fn maybe_rerank_indexed_batch_results(
    evaluation: VectorSearchEvaluation<'_>,
    index_entries: &[IndexManifestEntry],
    field_id: i32,
    field_name: &str,
    vector_searches: &[VectorSearch],
    results: Vec<ScoredRowIds>,
    index_search_limit: usize,
) -> crate::Result<Vec<ScoredRowIds>> {
    let timing_enabled = vector_search_timing_enabled();
    let total_start = timing_enabled.then(Instant::now);
    let mut candidate_searches = Vec::with_capacity(vector_searches.len());
    let mut candidate_results = Vec::with_capacity(vector_searches.len());
    let mut union_candidates = RoaringTreemap::new();
    let mut candidate_references = 0usize;

    for (result, vector_search) in results.into_iter().zip(vector_searches) {
        let candidates = result.top_k(index_search_limit);
        candidate_references = candidate_references.saturating_add(candidates.row_ids.len());
        let mut include_row_ids = RoaringTreemap::new();
        for &row_id in &candidates.row_ids {
            include_row_ids.insert(row_id);
            union_candidates.insert(row_id);
        }

        let mut candidate_search = vector_search.clone();
        candidate_search.set_shared_include_row_ids(Arc::new(include_row_ids));
        candidate_searches.push(candidate_search);
        candidate_results.push(candidates);
    }

    if union_candidates.iter().next().is_none() {
        return Ok(candidate_results);
    }

    let table = evaluation.table.ok_or_else(|| crate::Error::DataInvalid {
        message: "Vector index rerank requires table context".to_string(),
        source: None,
    })?;
    let unique_candidates = union_candidates.len();
    let raw_ranges = sorted_row_ids_to_row_ranges(union_candidates.iter())?;
    let metric_start = timing_enabled.then(Instant::now);
    let metric = resolve_raw_vector_metric(
        evaluation.file_io,
        evaluation.table_path.trim_end_matches('/'),
        evaluation.table_options,
        index_entries,
        field_id,
        field_name,
    )
    .await?;
    let metric_resolve = metric_start.map_or(Duration::ZERO, |start| start.elapsed());

    let (results, raw_timing) =
        read_raw_batch_vector_search(table, &candidate_searches, &raw_ranges, metric).await?;
    if let (Some(total_start), Some(raw_timing)) = (total_start, raw_timing) {
        log::debug!(
            target: "paimon::vector_search",
            "event=paimon_vector_refine nq={} candidate_references={} unique_candidates={} row_ranges={} metric_resolve_ms={:.3} raw_plan_ms={:.3} split_count={} file_count={} raw_stream_wait_ms={:.3} raw_score_cpu_ms={:.3} arrow_batches={} arrow_rows={} total_refine_ms={:.3}",
            vector_searches.len(),
            candidate_references,
            unique_candidates,
            raw_ranges.len(),
            metric_resolve.as_secs_f64() * 1000.0,
            raw_timing.plan.as_secs_f64() * 1000.0,
            raw_timing.split_count,
            raw_timing.file_count,
            raw_timing.stream_wait.as_secs_f64() * 1000.0,
            raw_timing.score_cpu.as_secs_f64() * 1000.0,
            raw_timing.batch_count,
            raw_timing.row_count,
            total_start.elapsed().as_secs_f64() * 1000.0,
        );
    }
    Ok(results)
}

fn sorted_row_ids_to_row_ranges(
    row_ids: impl IntoIterator<Item = u64>,
) -> crate::Result<Vec<RowRange>> {
    let mut row_ids = row_ids.into_iter();
    let Some(first) = row_ids.next() else {
        return Ok(Vec::new());
    };
    let mut start = row_id_to_i64_for_range(first)?;
    let mut end = start;
    let mut ranges = Vec::new();
    for row_id in row_ids {
        let row_id = row_id_to_i64_for_range(row_id)?;
        if end.checked_add(1) == Some(row_id) {
            end = row_id;
        } else {
            ranges.push(RowRange::new(start, end));
            start = row_id;
            end = row_id;
        }
    }
    ranges.push(RowRange::new(start, end));
    Ok(ranges)
}

fn row_id_to_i64_for_range(row_id: u64) -> crate::Result<i64> {
    i64::try_from(row_id).map_err(|_| crate::Error::DataInvalid {
        message: format!(
            "Vector search row id {row_id} exceeds i64::MAX and cannot be converted to RowRange"
        ),
        source: None,
    })
}

fn shared_batch_include_row_ids(vector_searches: &[VectorSearch]) -> Option<&Arc<RoaringTreemap>> {
    let first = vector_searches.first()?.shared_include_row_ids.as_ref()?;
    vector_searches
        .iter()
        .skip(1)
        .all(|search| {
            search
                .shared_include_row_ids
                .as_ref()
                .is_some_and(|include_row_ids| Arc::ptr_eq(first, include_row_ids))
        })
        .then_some(first)
}

fn prune_raw_ranges_by_include_row_ids(
    raw_ranges: &[RowRange],
    vector_searches: &[VectorSearch],
) -> crate::Result<Vec<RowRange>> {
    if vector_searches
        .iter()
        .any(|search| search.effective_include_row_ids().is_none())
    {
        return Ok(raw_ranges.to_vec());
    }

    let include_ranges =
        if let Some(include_row_ids) = shared_batch_include_row_ids(vector_searches) {
            sorted_row_ids_to_row_ranges(include_row_ids.iter())?
        } else {
            let mut union = RoaringTreemap::new();
            for include_row_ids in vector_searches
                .iter()
                .filter_map(VectorSearch::effective_include_row_ids)
            {
                for row_id in include_row_ids.iter() {
                    union.insert(row_id);
                }
            }
            sorted_row_ids_to_row_ranges(union.iter())?
        };
    Ok(intersect_sorted_ranges(raw_ranges, &include_ranges))
}

fn localize_include_row_ids(
    include_row_ids: &RoaringTreemap,
    row_range_start: i64,
    row_range_end: i64,
) -> crate::Result<RoaringTreemap> {
    let start = u64::try_from(row_range_start).map_err(|_| crate::Error::DataInvalid {
        message: format!("Negative vector index row range start: {row_range_start}"),
        source: None,
    })?;
    let end = u64::try_from(row_range_end).map_err(|_| crate::Error::DataInvalid {
        message: format!("Negative vector index row range end: {row_range_end}"),
        source: None,
    })?;
    let mut localized = RoaringTreemap::new();
    for row_id in include_row_ids.iter() {
        if row_id >= start && row_id <= end {
            localized.insert(row_id - start);
        }
    }
    Ok(localized)
}

fn localize_shared_include_row_ids(
    include_row_ids: &RoaringTreemap,
    ranges: &[(i64, i64)],
) -> crate::Result<Vec<Option<RoaringTreemap>>> {
    let mut validated_ranges = Vec::with_capacity(ranges.len());
    for (index, &(start, end)) in ranges.iter().enumerate() {
        if start < 0 || end < start {
            return Err(crate::Error::DataInvalid {
                message: format!("Invalid vector index row range [{start}, {end}]"),
                source: None,
            });
        }
        validated_ranges.push((start as u64, end as u64, index));
    }
    validated_ranges.sort_unstable_by_key(|(start, _, _)| *start);

    let mut localized = (0..ranges.len())
        .map(|_| RoaringTreemap::new())
        .collect::<Vec<_>>();
    let mut active = Vec::<usize>::new();
    let mut next_range = 0usize;
    for row_id in include_row_ids.iter() {
        while next_range < validated_ranges.len() && validated_ranges[next_range].0 <= row_id {
            active.push(next_range);
            next_range += 1;
        }
        active.retain(|range_index| validated_ranges[*range_index].1 >= row_id);
        for range_index in &active {
            let (start, _, original_index) = validated_ranges[*range_index];
            localized[original_index].insert(row_id - start);
        }
        if next_range == validated_ranges.len() && active.is_empty() {
            break;
        }
    }

    Ok(localized
        .into_iter()
        .map(|filter| (!filter.is_empty()).then_some(filter))
        .collect())
}

async fn detail_data_ranges_for_table(table: &Table) -> crate::Result<Vec<RowRange>> {
    let plan = table
        .new_read_builder()
        .new_scan()
        .with_scan_all_files()
        .plan()
        .await?;
    let mut ranges = Vec::new();
    for split in plan.splits() {
        for file in split.data_files() {
            if let Some((from, to)) = file.row_id_range() {
                ranges.push(RowRange::new(from, to));
            }
        }
    }
    Ok(merge_row_ranges(ranges))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RawVectorMetric {
    L2,
    Cosine,
    InnerProduct,
}

impl RawVectorMetric {
    fn parse(value: &str) -> crate::Result<Self> {
        Self::parse_normalized(&normalize_metric(value)).ok_or_else(|| crate::Error::DataInvalid {
            message: format!("Unknown vector search metric: {value}"),
            source: None,
        })
    }

    fn parse_normalized(value: &str) -> Option<Self> {
        match value {
            "l2" => Some(Self::L2),
            "cosine" => Some(Self::Cosine),
            "inner_product" => Some(Self::InnerProduct),
            _ => None,
        }
    }

    fn from_lumina(metric: LuminaVectorMetric) -> Self {
        match metric {
            LuminaVectorMetric::L2 => Self::L2,
            LuminaVectorMetric::Cosine => Self::Cosine,
            LuminaVectorMetric::InnerProduct => Self::InnerProduct,
        }
    }

    fn from_vindex(metric: MetricType) -> Self {
        match metric {
            MetricType::L2 => Self::L2,
            MetricType::Cosine => Self::Cosine,
            MetricType::InnerProduct => Self::InnerProduct,
        }
    }
}

async fn resolve_raw_vector_metric(
    file_io: &FileIO,
    table_path: &str,
    table_options: &HashMap<String, String>,
    index_entries: &[IndexManifestEntry],
    field_id: i32,
    field_name: &str,
) -> crate::Result<RawVectorMetric> {
    for entry in index_entries {
        if entry.kind != FileKind::Add {
            continue;
        }
        let Some(global_meta) = entry.index_file.global_index_meta.as_ref() else {
            continue;
        };
        if global_meta.index_field_id != field_id {
            continue;
        }
        let Some(backend) = VectorIndexBackend::from_index_type(&entry.index_file.index_type)
        else {
            continue;
        };
        match backend {
            VectorIndexBackend::Lumina => {
                if let Some(index_meta) = global_meta.index_meta.as_ref() {
                    if !index_meta.is_empty() {
                        let metric = LuminaIndexMeta::deserialize(index_meta)?.metric()?;
                        return Ok(RawVectorMetric::from_lumina(metric));
                    }
                }
            }
            VectorIndexBackend::Vindex => {
                if let Some(index_meta) = global_meta.index_meta.as_ref() {
                    if let Ok(options) =
                        serde_json::from_slice::<HashMap<String, String>>(index_meta)
                    {
                        if let Some(metric) = options.get("metric") {
                            if let Some(metric) =
                                RawVectorMetric::parse_normalized(&normalize_metric(metric))
                            {
                                return Ok(metric);
                            }
                        }
                    }
                }
                let path = IndexFileLocation::Global { table_path }.resolve(
                    &entry.index_file.file_name,
                    entry.index_file.external_path.as_deref(),
                );
                let input = file_io.new_input(&path)?;
                let read_error = |e| crate::Error::DataInvalid {
                    message: format!(
                        "Failed to read vindex index file '{}' for raw search metric: {}",
                        entry.index_file.file_name, e
                    ),
                    source: Some(Box::new(e)),
                };
                let header_size = if entry.index_file.file_size > 0 {
                    (entry.index_file.file_size as u64).min(DISKANN_HEADER_SIZE as u64)
                } else {
                    input
                        .metadata()
                        .await
                        .map_err(&read_error)?
                        .size
                        .min(DISKANN_HEADER_SIZE as u64)
                };
                let file_reader = input.reader().await.map_err(&read_error)?;
                let bytes = file_reader.read(0..header_size).await.map_err(read_error)?;
                let reader = VIndexReader::open(Cursor::new(bytes)).map_err(|e| {
                    crate::Error::DataInvalid {
                        message: format!(
                            "Failed to open paimon-vindex-core reader for raw search metric: {}",
                            e
                        ),
                        source: Some(Box::new(e)),
                    }
                })?;
                return Ok(RawVectorMetric::from_vindex(reader.metadata().metric));
            }
        }
    }

    configured_raw_vector_metric(table_options, field_name)
}

fn configured_raw_vector_metric(
    options: &HashMap<String, String>,
    field_name: &str,
) -> crate::Result<RawVectorMetric> {
    let direct_keys = [
        format!("fields.{field_name}.distance.metric"),
        format!("fields.{field_name}.metric"),
        "test.vector.metric".to_string(),
        "lumina.distance.metric".to_string(),
        "distance.metric".to_string(),
        "metric".to_string(),
    ];
    for key in direct_keys {
        if let Some(value) = options.get(&key) {
            return RawVectorMetric::parse(value);
        }
    }

    let mut inferred = None;
    for (key, value) in options {
        if !(key.ends_with(".distance.metric") || key.ends_with(".metric")) {
            continue;
        }
        let normalized = normalize_metric(value);
        let Some(metric) = RawVectorMetric::parse_normalized(&normalized) else {
            continue;
        };
        if let Some(existing) = inferred {
            if existing != metric {
                return Ok(RawVectorMetric::L2);
            }
        } else {
            inferred = Some(metric);
        }
    }
    Ok(inferred.unwrap_or(RawVectorMetric::L2))
}

#[derive(Default)]
struct RawVectorReadTiming {
    plan: Duration,
    stream_wait: Duration,
    score_cpu: Duration,
    total: Duration,
    split_count: usize,
    file_count: usize,
    batch_count: usize,
    row_count: usize,
}

async fn read_raw_batch_vector_search(
    table: &Table,
    vector_searches: &[VectorSearch],
    raw_ranges: &[RowRange],
    metric: RawVectorMetric,
) -> crate::Result<(Vec<ScoredRowIds>, Option<RawVectorReadTiming>)> {
    let timing_enabled = vector_search_timing_enabled();
    let total_start = timing_enabled.then(Instant::now);
    if vector_searches.is_empty() {
        return Ok((Vec::new(), None));
    }
    if raw_ranges.is_empty() {
        return Ok((vec![ScoredRowIds::empty(); vector_searches.len()], None));
    }
    let raw_ranges = prune_raw_ranges_by_include_row_ids(raw_ranges, vector_searches)?;
    if raw_ranges.is_empty() {
        return Ok((vec![ScoredRowIds::empty(); vector_searches.len()], None));
    }

    let field_name = &vector_searches[0].field_name;
    if vector_searches
        .iter()
        .any(|vector_search| vector_search.field_name != *field_name)
    {
        return Err(crate::Error::DataInvalid {
            message: "Batch vector raw search requires all query vectors to use the same field"
                .to_string(),
            source: None,
        });
    }

    let plan_start = timing_enabled.then(Instant::now);
    let mut read_builder = table.new_read_builder();
    read_builder
        .with_projection(&[field_name.as_str(), ROW_ID_FIELD_NAME])?
        .with_row_ranges(raw_ranges);
    let plan = read_builder.new_scan().plan().await?;
    let plan_elapsed = plan_start.map_or(Duration::ZERO, |start| start.elapsed());
    let split_count = plan.splits().len();
    let file_count = plan
        .splits()
        .iter()
        .map(|split| split.data_files().len())
        .sum();
    if plan.splits().is_empty() {
        return Ok((
            vec![ScoredRowIds::empty(); vector_searches.len()],
            total_start.map(|start| RawVectorReadTiming {
                plan: plan_elapsed,
                total: start.elapsed(),
                ..RawVectorReadTiming::default()
            }),
        ));
    }
    let read = read_builder.new_read()?;
    let mut stream = read.to_arrow(plan.splits())?;

    let scoring_plan = RawScoringPlan::new(vector_searches, metric);
    let mut top_k = vector_searches
        .iter()
        .map(|vector_search| RawScoreTopK::new(vector_search.limit))
        .collect::<Vec<_>>();
    let mut timing = timing_enabled.then(|| RawVectorReadTiming {
        plan: plan_elapsed,
        split_count,
        file_count,
        ..RawVectorReadTiming::default()
    });
    loop {
        let stream_wait_start = timing_enabled.then(Instant::now);
        let batch = stream.try_next().await?;
        if let (Some(timing), Some(stream_wait_start)) = (&mut timing, stream_wait_start) {
            timing.stream_wait = timing
                .stream_wait
                .saturating_add(stream_wait_start.elapsed());
        }
        let Some(batch) = batch else {
            break;
        };
        if let Some(timing) = &mut timing {
            timing.batch_count += 1;
            timing.row_count = timing.row_count.saturating_add(batch.num_rows());
        }
        let score_start = timing_enabled.then(Instant::now);
        collect_raw_batch_vector_batch(&batch, vector_searches, metric, &scoring_plan, &mut top_k)?;
        if let (Some(timing), Some(score_start)) = (&mut timing, score_start) {
            timing.score_cpu = timing.score_cpu.saturating_add(score_start.elapsed());
        }
    }

    if let (Some(timing), Some(total_start)) = (&mut timing, total_start) {
        timing.total = total_start.elapsed();
    }
    Ok((
        top_k
            .into_iter()
            .map(RawScoreTopK::into_search_result)
            .collect(),
        timing,
    ))
}

struct RawScoringPlan {
    all_query_indices: Vec<usize>,
    shared_filter_groups: Vec<SharedRawFilterGroup>,
    candidate_query_indices: HashMap<u64, Vec<usize>>,
    query_l2_squared_norms: Vec<f32>,
    dense_query_dimension: Option<usize>,
    dense_query_matrix: Option<Vec<f32>>,
}

struct SharedRawFilterGroup {
    include_row_ids: Arc<RoaringTreemap>,
    query_indices: Vec<usize>,
}

impl RawScoringPlan {
    fn new(vector_searches: &[VectorSearch], metric: RawVectorMetric) -> Self {
        let mut all_query_indices = Vec::new();
        let mut shared_filter_groups = Vec::new();
        let mut candidate_query_indices: HashMap<u64, Vec<usize>> = HashMap::new();
        let query_l2_squared_norms = vector_searches
            .iter()
            .map(|vector_search| match metric {
                RawVectorMetric::L2 | RawVectorMetric::Cosine => vector_search
                    .vector
                    .iter()
                    .map(|value| value * value)
                    .sum::<f32>(),
                RawVectorMetric::InnerProduct => 0.0,
            })
            .collect();

        if let Some(include_row_ids) = shared_batch_include_row_ids(vector_searches) {
            shared_filter_groups.push(SharedRawFilterGroup {
                include_row_ids: Arc::clone(include_row_ids),
                query_indices: (0..vector_searches.len()).collect(),
            });
        } else {
            for (query_index, vector_search) in vector_searches.iter().enumerate() {
                if let Some(include_row_ids) = vector_search.effective_include_row_ids() {
                    for row_id in include_row_ids.iter() {
                        candidate_query_indices
                            .entry(row_id)
                            .or_default()
                            .push(query_index);
                    }
                } else {
                    all_query_indices.push(query_index);
                }
            }
        }

        let dense_query_dimension = all_query_indices
            .first()
            .map(|&query_index| vector_searches[query_index].vector.len());
        let dense_query_matrix = dense_query_dimension.and_then(|dimension| {
            all_query_indices
                .iter()
                .all(|&query_index| vector_searches[query_index].vector.len() == dimension)
                .then(|| {
                    let mut matrix =
                        Vec::with_capacity(all_query_indices.len().saturating_mul(dimension));
                    for &query_index in &all_query_indices {
                        matrix.extend_from_slice(&vector_searches[query_index].vector);
                    }
                    matrix
                })
        });

        Self {
            all_query_indices,
            shared_filter_groups,
            candidate_query_indices,
            query_l2_squared_norms,
            dense_query_dimension,
            dense_query_matrix,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct RawScoredRow {
    row_id: u64,
    score: f32,
}

impl RawScoredRow {
    fn strongest_first(a: &Self, b: &Self) -> Ordering {
        b.score
            .total_cmp(&a.score)
            .then_with(|| a.row_id.cmp(&b.row_id))
    }
}

struct RawScoreTopK {
    limit: usize,
    candidates: Vec<RawScoredRow>,
}

impl RawScoreTopK {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            candidates: Vec::with_capacity(limit.min(1024).saturating_add(1)),
        }
    }

    fn offer(&mut self, row_id: u64, score: f32) {
        if self.limit == 0 {
            return;
        }
        self.candidates.push(RawScoredRow { row_id, score });
        if self.candidates.len() >= self.partition_size() {
            self.reduce_to_limit();
        }
    }

    fn offer_many<I>(&mut self, candidates: I)
    where
        I: IntoIterator<Item = RawScoredRow>,
    {
        if self.limit == 0 {
            return;
        }
        self.candidates.extend(candidates);
        if self.candidates.len() >= self.partition_size() {
            self.reduce_to_limit();
        }
    }

    fn partition_size(&self) -> usize {
        self.limit
            .saturating_mul(2)
            .max(RAW_TOP_K_MIN_PARTITION_SIZE)
    }

    fn reduce_to_limit(&mut self) {
        if self.candidates.len() <= self.limit {
            return;
        }
        // Partition only after a substantial candidate block has accumulated.
        // Each partition is linear in its input, so all reductions are O(n)
        // amortized; only the final K survivors are fully sorted.
        self.candidates
            .select_nth_unstable_by(self.limit, RawScoredRow::strongest_first);
        self.candidates.truncate(self.limit);
    }

    fn into_search_result(mut self) -> ScoredRowIds {
        self.reduce_to_limit();
        self.candidates
            .sort_unstable_by(RawScoredRow::strongest_first);
        let rows = self.candidates;
        let mut row_ids = Vec::with_capacity(rows.len());
        let mut scores = Vec::with_capacity(rows.len());
        for row in rows {
            row_ids.push(row.row_id);
            scores.push(row.score);
        }
        ScoredRowIds::new(row_ids, scores)
    }
}

fn collect_raw_batch_vector_batch(
    batch: &RecordBatch,
    vector_searches: &[VectorSearch],
    metric: RawVectorMetric,
    scoring_plan: &RawScoringPlan,
    top_k_out: &mut [RawScoreTopK],
) -> crate::Result<()> {
    if vector_searches.is_empty() {
        return Ok(());
    }
    if top_k_out.len() != vector_searches.len() {
        return Err(crate::Error::DataInvalid {
            message: "Raw batch vector search output buffers must match query vector count"
                .to_string(),
            source: None,
        });
    }

    let field_name = &vector_searches[0].field_name;
    if vector_searches
        .iter()
        .any(|vector_search| vector_search.field_name != *field_name)
    {
        return Err(crate::Error::DataInvalid {
            message: "Batch vector raw search requires all query vectors to use the same field"
                .to_string(),
            source: None,
        });
    }

    let vector_index =
        batch
            .schema()
            .index_of(field_name)
            .map_err(|e| crate::Error::DataInvalid {
                message: format!(
                    "Vector column '{}' not found in raw search batch: {}",
                    field_name, e
                ),
                source: None,
            })?;
    let row_id_index =
        batch
            .schema()
            .index_of(ROW_ID_FIELD_NAME)
            .map_err(|e| crate::Error::DataInvalid {
                message: format!("_ROW_ID column not found in raw search batch: {e}"),
                source: None,
            })?;

    let row_ids = batch
        .column(row_id_index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| crate::Error::DataInvalid {
            message: "Vector raw search requires non-null Int64 _ROW_ID".to_string(),
            source: None,
        })?;

    let column = batch.column(vector_index);
    enum VectorLayout<'a> {
        List(&'a ListArray),
        Fixed(&'a FixedSizeListArray),
    }
    let layout = if let Some(a) = column.as_any().downcast_ref::<ListArray>() {
        VectorLayout::List(a)
    } else if let Some(a) = column.as_any().downcast_ref::<FixedSizeListArray>() {
        VectorLayout::Fixed(a)
    } else {
        return Err(crate::Error::DataInvalid {
            message: "Vector raw search requires Arrow List<Float32> or FixedSizeList<Float32>"
                .to_string(),
            source: None,
        });
    };
    let values = match layout {
        VectorLayout::List(a) => a.values(),
        VectorLayout::Fixed(a) => a.values(),
    }
    .as_any()
    .downcast_ref::<Float32Array>()
    .ok_or_else(|| crate::Error::DataInvalid {
        message: "Vector raw search requires Float32 vector elements".to_string(),
        source: None,
    })?;

    let use_dense_matrix = scoring_plan.all_query_indices.len() >= RAW_SCORE_MATRIX_MIN_QUERY_COUNT;
    let dense_dimension = use_dense_matrix
        .then_some(scoring_plan.dense_query_dimension)
        .flatten();
    let mut dense_row_ids = Vec::with_capacity(batch.num_rows());
    let mut dense_vectors = Vec::with_capacity(
        batch
            .num_rows()
            .saturating_mul(dense_dimension.unwrap_or_default()),
    );
    for row in 0..batch.num_rows() {
        if row_ids.is_null(row) {
            return Err(crate::Error::DataInvalid {
                message: "Vector raw search found null _ROW_ID".to_string(),
                source: None,
            });
        }
        let row_id = row_id_to_u64(row_ids.value(row))?;
        let is_null = match layout {
            VectorLayout::List(a) => a.is_null(row),
            VectorLayout::Fixed(a) => a.is_null(row),
        };
        if is_null {
            continue;
        }

        let (start, end) = match layout {
            VectorLayout::List(a) => {
                let offsets = a.value_offsets();
                (offsets[row] as usize, offsets[row + 1] as usize)
            }
            VectorLayout::Fixed(a) => {
                let len = a.value_length() as usize;
                let start = a.value_offset(row) as usize;
                (start, start + len)
            }
        };
        ensure_raw_vector_values_not_null(values, start, end)?;

        let raw_row = RawVectorRow {
            row_id,
            values,
            start,
            end,
        };
        if let Some(dimension) = dense_dimension {
            ensure_raw_vector_dimension(end - start, dimension)?;
            if scoring_plan.dense_query_matrix.is_none() {
                let &query_index = scoring_plan
                    .all_query_indices
                    .iter()
                    .find(|&&query_index| vector_searches[query_index].vector.len() != dimension)
                    .expect("a missing dense matrix requires inconsistent query dimensions");
                ensure_raw_vector_dimension(dimension, vector_searches[query_index].vector.len())?;
            }
            dense_row_ids.push(row_id);
            dense_vectors.extend_from_slice(&values.values()[start..end]);
        } else {
            for &query_index in &scoring_plan.all_query_indices {
                offer_raw_vector_score(
                    raw_row,
                    query_index,
                    metric,
                    vector_searches,
                    scoring_plan,
                    top_k_out,
                )?;
            }
        }
        if let Some(query_indices) = scoring_plan.candidate_query_indices.get(&row_id) {
            for &query_index in query_indices {
                offer_raw_vector_score(
                    raw_row,
                    query_index,
                    metric,
                    vector_searches,
                    scoring_plan,
                    top_k_out,
                )?;
            }
        }
        for group in &scoring_plan.shared_filter_groups {
            if group.include_row_ids.contains(row_id) {
                for &query_index in &group.query_indices {
                    offer_raw_vector_score(
                        raw_row,
                        query_index,
                        metric,
                        vector_searches,
                        scoring_plan,
                        top_k_out,
                    )?;
                }
            }
        }
    }

    if !dense_row_ids.is_empty() {
        let query_matrix = scoring_plan
            .dense_query_matrix
            .as_deref()
            .expect("dense query dimensions were validated above");
        let dimension = dense_dimension.expect("dense rows require dense queries");
        let queries_per_chunk = (RAW_SCORE_MATRIX_TARGET_ELEMENTS / dense_row_ids.len())
            .max(1)
            .min(scoring_plan.all_query_indices.len());
        for (query_chunk_index, query_indices) in scoring_plan
            .all_query_indices
            .chunks(queries_per_chunk)
            .enumerate()
        {
            let query_start = query_chunk_index * queries_per_chunk * dimension;
            let query_end = query_start + query_indices.len() * dimension;
            let scores = compute_raw_vector_score_matrix(
                &dense_vectors,
                dense_row_ids.len(),
                &query_matrix[query_start..query_end],
                query_indices.len(),
                dimension,
                &scoring_plan.query_l2_squared_norms,
                query_indices,
                metric,
            )?;
            for (matrix_query_index, &query_index) in query_indices.iter().enumerate() {
                let query_scores = &scores[matrix_query_index * dense_row_ids.len()
                    ..(matrix_query_index + 1) * dense_row_ids.len()];
                top_k_out[query_index].offer_many(
                    dense_row_ids
                        .iter()
                        .zip(query_scores)
                        .map(|(&row_id, &score)| RawScoredRow { row_id, score }),
                );
            }
        }
    }

    Ok(())
}

fn ensure_raw_vector_dimension(stored_len: usize, query_len: usize) -> crate::Result<()> {
    if stored_len != query_len {
        return Err(crate::Error::DataInvalid {
            message: format!(
                "Query vector dimension mismatch: raw row has {}, but query has {}",
                stored_len, query_len
            ),
            source: None,
        });
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn compute_raw_vector_score_matrix(
    stored_vectors: &[f32],
    row_count: usize,
    query_vectors: &[f32],
    query_count: usize,
    dimension: usize,
    query_l2_squared_norms: &[f32],
    query_indices: &[usize],
    metric: RawVectorMetric,
) -> crate::Result<Vec<f32>> {
    let score_count =
        row_count
            .checked_mul(query_count)
            .ok_or_else(|| crate::Error::DataInvalid {
                message: "Vector raw search score matrix is too large".to_string(),
                source: None,
            })?;
    debug_assert_eq!(stored_vectors.len(), row_count * dimension);
    debug_assert_eq!(query_vectors.len(), query_count * dimension);
    debug_assert_eq!(query_indices.len(), query_count);

    let mut scores = vec![0.0; score_count];
    // Query × stored-vector^T produces a query-major score matrix. Each query's
    // scores are contiguous, which feeds partial Top-K without strided reads.
    sgemm_a_bt(
        query_count,
        row_count,
        dimension,
        1.0,
        query_vectors,
        stored_vectors,
        0.0,
        &mut scores,
    );
    if metric == RawVectorMetric::InnerProduct {
        return Ok(scores);
    }

    let stored_l2_squared_norms = stored_vectors
        .chunks_exact(dimension)
        .map(|vector| vector.iter().map(|value| value * value).sum::<f32>())
        .collect::<Vec<_>>();
    for (matrix_query_index, &query_index) in query_indices.iter().enumerate() {
        for (row_index, &stored_l2_squared_norm) in stored_l2_squared_norms.iter().enumerate() {
            let score = &mut scores[matrix_query_index * row_count + row_index];
            let query_l2_squared_norm = query_l2_squared_norms[query_index];
            *score = match metric {
                RawVectorMetric::L2 => {
                    let squared_distance =
                        stored_l2_squared_norm + query_l2_squared_norm - 2.0 * *score;
                    // The norm/dot reconstruction loses the low-order difference when two
                    // large vectors are close. Estimate a conservative accumulation-error
                    // bound and preserve the former scalar semantics inside that region.
                    let roundoff_bound = (stored_l2_squared_norm.abs()
                        + query_l2_squared_norm.abs()
                        + 2.0 * score.abs())
                        * f32::EPSILON
                        * (dimension as f32 + 2.0)
                        * 4.0;
                    if !squared_distance.is_finite() || squared_distance <= roundoff_bound {
                        let stored =
                            &stored_vectors[row_index * dimension..(row_index + 1) * dimension];
                        let query = &query_vectors
                            [matrix_query_index * dimension..(matrix_query_index + 1) * dimension];
                        compute_raw_vector_l2_score(query, stored)
                    } else {
                        1.0 / (1.0 + squared_distance)
                    }
                }
                RawVectorMetric::Cosine => {
                    let denominator = stored_l2_squared_norm.sqrt() * query_l2_squared_norm.sqrt();
                    if denominator == 0.0 {
                        0.0
                    } else {
                        *score / denominator
                    }
                }
                RawVectorMetric::InnerProduct => unreachable!(),
            };
        }
    }
    Ok(scores)
}

fn ensure_raw_vector_values_not_null(
    values: &Float32Array,
    start: usize,
    end: usize,
) -> crate::Result<()> {
    if values.null_count() == 0 {
        return Ok(());
    }
    for value_index in start..end {
        if values.is_null(value_index) {
            return Err(crate::Error::DataInvalid {
                message: "Vector raw search found null vector element".to_string(),
                source: None,
            });
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct RawVectorRow<'a> {
    row_id: u64,
    values: &'a Float32Array,
    start: usize,
    end: usize,
}

fn offer_raw_vector_score(
    row: RawVectorRow<'_>,
    query_index: usize,
    metric: RawVectorMetric,
    vector_searches: &[VectorSearch],
    scoring_plan: &RawScoringPlan,
    top_k_out: &mut [RawScoreTopK],
) -> crate::Result<()> {
    let vector_search = &vector_searches[query_index];
    let stored_len = row.end - row.start;
    ensure_raw_vector_dimension(stored_len, vector_search.vector.len())?;
    let score = compute_raw_vector_score_from_values(
        &vector_search.vector,
        scoring_plan.query_l2_squared_norms[query_index],
        row.values,
        row.start,
        row.end,
        metric,
    );
    top_k_out[query_index].offer(row.row_id, score);
    Ok(())
}

fn compute_raw_vector_score_from_values(
    query: &[f32],
    query_l2_squared_norm: f32,
    values: &Float32Array,
    start: usize,
    end: usize,
    metric: RawVectorMetric,
) -> f32 {
    debug_assert_eq!(query.len(), end - start);
    match metric {
        RawVectorMetric::L2 => compute_raw_vector_l2_score(query, &values.values()[start..end]),
        RawVectorMetric::Cosine => {
            let mut dot = 0.0;
            let mut norm_b = 0.0;
            for (q, value_index) in query.iter().zip(start..end) {
                let stored = values.value(value_index);
                dot += q * stored;
                norm_b += stored * stored;
            }
            let denominator = query_l2_squared_norm.sqrt() * norm_b.sqrt();
            if denominator == 0.0 {
                0.0
            } else {
                dot / denominator
            }
        }
        RawVectorMetric::InnerProduct => query
            .iter()
            .zip(start..end)
            .map(|(q, value_index)| q * values.value(value_index))
            .sum(),
    }
}

fn compute_raw_vector_l2_score(query: &[f32], stored: &[f32]) -> f32 {
    let squared_distance = query
        .iter()
        .zip(stored)
        .map(|(query_value, stored_value)| {
            let difference = query_value - stored_value;
            difference * difference
        })
        .sum::<f32>();
    1.0 / (1.0 + squared_distance)
}

fn row_id_to_u64(row_id: i64) -> crate::Result<u64> {
    u64::try_from(row_id).map_err(|_| crate::Error::DataInvalid {
        message: format!("Negative _ROW_ID {row_id} cannot be used for global index search"),
        source: None,
    })
}

#[cfg(test)]
fn compute_raw_vector_score(query: &[f32], stored: &[f32], metric: RawVectorMetric) -> f32 {
    match metric {
        RawVectorMetric::L2 => compute_raw_vector_l2_score(query, stored),
        RawVectorMetric::Cosine => {
            let mut dot = 0.0;
            let mut norm_a = 0.0;
            let mut norm_b = 0.0;
            for (q, s) in query.iter().zip(stored.iter()) {
                dot += q * s;
                norm_a += q * q;
                norm_b += s * s;
            }
            let denominator = norm_a.sqrt() * norm_b.sqrt();
            if denominator == 0.0 {
                0.0
            } else {
                dot / denominator
            }
        }
        RawVectorMetric::InnerProduct => query.iter().zip(stored.iter()).map(|(q, s)| q * s).sum(),
    }
}

#[cfg(test)]
mod tests;
