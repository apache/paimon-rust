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

use crate::arrow::format::FilePredicates;
use crate::arrow::residual::{evaluate_predicates_mask, widen_scan_fields};
use crate::io::FileIO;
use crate::lumina::reader::LuminaVectorGlobalIndexReader;
use crate::lumina::{
    is_lumina_index_type, LuminaIndexMeta, LuminaVectorIndexOptions, LuminaVectorMetric,
};
use crate::spec::{
    BigIntType, CoreOptions, DataField, DataType, FileKind, GlobalIndexSearchMode, IndexFileMeta,
    IndexManifest, IndexManifestEntry, Predicate, ROW_ID_FIELD_ID, ROW_ID_FIELD_NAME,
};
use crate::table::data_file_reader::DataFileReader;
use crate::table::global_index_scanner::{
    deleted_row_ranges_for_data_evolution_dvs, search_limit_with_deleted_rows,
    unindexed_ranges_for_global_index_entries, RowRangeIndex,
};
use crate::table::pk_vector_data_file_reader::{
    append_batch_vectors, DataFilePkVectorReaderFactory,
};
use crate::table::pk_vector_indexed_split_read::{expand_ranges, PkVectorIndexedSplitRead};
use crate::table::pk_vector_orchestrator::{
    as_split_exact_file_search, build_indexed_splits, merge_candidates, OrchestratorSearchResult,
    PkVectorCandidate, PkVectorOrchestrator, PkVectorSearchSplit,
};
use crate::table::pk_vector_position_read::{
    PkVectorPositionRead, PKEY_VECTOR_POSITION_COLUMN, SEARCH_SCORE_COLUMN,
};
use crate::table::pk_vector_scan::{PkVectorScan, PkVectorScanPlan};
use crate::table::read_builder::resolve_projected_fields;
use crate::table::source::DataSplit;
use crate::table::{
    find_field_id_by_name, merge_row_ranges, ArrowRecordBatchStream, RowRange, Table,
};
use crate::vector_search::{GlobalIndexIOMeta, SearchResult, VectorSearch};
use crate::vindex::pkvector::ann::VindexAnnSearcher;
use crate::vindex::pkvector::bucket::{BucketActiveFile, BucketAnnSegment, ExactFileSearchFuture};
use crate::vindex::pkvector::exact::validate_query;
use crate::vindex::pkvector::metric::VectorSearchMetric;
use crate::vindex::reader::VindexVectorGlobalIndexReader;
use crate::vindex::{is_vindex_index_type, VindexVectorIndexOptions};
use arrow_array::{Array, FixedSizeListArray, Float32Array, Int64Array, ListArray, RecordBatch};
use arrow_select::interleave::interleave_record_batch;
use futures::{stream, TryStreamExt};
use paimon_vindex_core::distance::MetricType;
use paimon_vindex_core::index::VectorIndexReader as VIndexReader;
use roaring::RoaringTreemap;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::io::Cursor;
use std::sync::Arc;

const INDEX_DIR: &str = "index";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum VectorIndexBackend {
    Lumina,
    Vindex,
}

impl VectorIndexBackend {
    fn from_index_type(index_type: &str) -> Option<Self> {
        if is_lumina_index_type(index_type) {
            Some(Self::Lumina)
        } else if is_vindex_index_type(index_type) {
            Some(Self::Vindex)
        } else {
            None
        }
    }

    fn error_name(self) -> &'static str {
        match self {
            Self::Lumina => "Lumina",
            Self::Vindex => "vindex",
        }
    }
}

pub struct VectorSearchBuilder<'a> {
    table: &'a Table,
    vector_column: Option<String>,
    query_vector: Option<Vec<f32>>,
    limit: Option<usize>,
    options: HashMap<String, String>,
    projection: Option<Vec<String>>,
    filter: Option<Predicate>,
}

pub struct BatchVectorSearchBuilder<'a> {
    table: &'a Table,
    vector_column: Option<String>,
    query_vectors: Option<Vec<Vec<f32>>>,
    limit: Option<usize>,
    options: HashMap<String, String>,
    projection: Option<Vec<String>>,
    filter: Option<Predicate>,
}

impl<'a> VectorSearchBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            vector_column: None,
            query_vector: None,
            limit: None,
            options: HashMap::new(),
            projection: None,
            filter: None,
        }
    }

    pub fn with_vector_column(&mut self, name: &str) -> &mut Self {
        self.vector_column = Some(name.to_string());
        self
    }

    pub fn with_query_vector(&mut self, vector: Vec<f32>) -> &mut Self {
        self.query_vector = Some(vector);
        self
    }

    pub fn with_limit(&mut self, limit: usize) -> &mut Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_options(&mut self, options: HashMap<String, String>) -> &mut Self {
        self.options = options;
        self
    }

    /// Attach a residual scalar predicate applied *after* vector recall on the
    /// primary-key vector path: each recalled candidate file is re-read and only
    /// rows satisfying `filter` survive, folded into the search so best-first
    /// order and Top-K still hold. Mirrors Java `PrimaryKeyVectorRead`'s
    /// residual-filter support. Only the primary-key vector path consumes it, and
    /// only when the table exposes physical rows directly (deletion vectors
    /// enabled without merge-on-read); otherwise the query fails loud. A query
    /// that does not resolve to the primary-key vector path (no PK-vector index,
    /// or a non-PK-vector column) also fails loud rather than silently ignoring
    /// the filter.
    ///
    /// The whole predicate is both pushed into the scan — where it prunes whole
    /// data files by their column stats — and applied per row as a residual over
    /// the surviving files, so results stay exact. Sub-file row-range narrowing is
    /// not performed; a surviving file is re-read in full for the residual.
    pub fn with_filter(&mut self, filter: Predicate) -> &mut Self {
        self.filter = Some(filter);
        self
    }

    /// Restrict the columns materialized by [`execute_read`](Self::execute_read)
    /// to `cols` (plus the always-appended `__paimon_search_score`). Without this
    /// call `execute_read` materializes every user table column. Only affects
    /// `execute_read`; the search-only paths ignore it.
    pub fn with_projection(&mut self, cols: &[&str]) -> &mut Self {
        self.projection = Some(cols.iter().map(|c| c.to_string()).collect());
        self
    }

    pub async fn execute(&self) -> crate::Result<Vec<RowRange>> {
        self.execute_scored().await?.to_row_ranges()
    }

    pub async fn execute_scored(&self) -> crate::Result<SearchResult> {
        // Fail closed: returns data-derived row ranges outside `TableScan`/`TableRead`.
        let core = CoreOptions::new(self.table.schema().options());
        core.ensure_read_authorized()?;
        let vector_column =
            self.vector_column
                .as_deref()
                .ok_or_else(|| crate::Error::ConfigInvalid {
                    message: "Vector column must be set via with_vector_column()".to_string(),
                })?;
        let query_vector =
            self.query_vector
                .as_ref()
                .ok_or_else(|| crate::Error::ConfigInvalid {
                    message: "Query vector must be set via with_query_vector()".to_string(),
                })?;
        let limit = self.limit.ok_or_else(|| crate::Error::ConfigInvalid {
            message: "Limit must be set via with_limit()".to_string(),
        })?;

        // Primary-key vector search branch: mirrors Java `PrimaryKeyVectorRead`.
        // Only taken when the table enables the PK-vector index AND this query
        // targets a configured PK-vector column; otherwise fall through to the
        // data-evolution (DE) global-index path below.
        //
        // Membership is resolved via the non-erroring columns accessor so a
        // malformed PK-vector config (e.g. a blank list) cannot abort an unrelated
        // DE query. A query that does target the PK-vector column fails loud here:
        // the PK path produces physical positions, not global row ids, so scored
        // search is unsupported and callers must use `execute_read` instead.
        if core.primary_key_vector_index_enabled() {
            let targets_pk_column = core
                .primary_key_vector_index_columns()
                .ok()
                .is_some_and(|cols| cols.iter().any(|c| c == vector_column));
            if targets_pk_column {
                return Err(crate::Error::DataInvalid {
                    message: "primary-key vector search does not produce global row ids; use the materialized read (execute_read) instead".to_string(),
                    source: None,
                });
            }
        }

        // The data-evolution (global-index) fall-through path cannot honor a
        // residual filter — it never reads physical rows. Rather than silently
        // drop the predicate and return unfiltered results, fail loud when a
        // filter is set on a query that does not resolve to the primary-key
        // vector path.
        if self.filter.is_some() {
            return Err(crate::Error::DataInvalid {
                message: "vector search filter is only supported on the primary-key vector path"
                    .to_string(),
                source: None,
            });
        }

        let mut batch_builder = BatchVectorSearchBuilder::new(self.table);
        let mut results = batch_builder
            .with_vector_column(vector_column)
            .with_query_vectors(vec![query_vector.clone()])
            .with_limit(limit)
            .with_options(self.options.clone())
            .execute()
            .await?;

        debug_assert_eq!(results.len(), 1);
        Ok(results.remove(0))
    }

    /// Run the vector search and materialize the matching rows as Arrow batches,
    /// ordered best-first. Supported for both primary-key vector indexes and
    /// data-evolution (global-index) vector search; a query targeting a column
    /// that is neither fails loud. Output columns are the projected user table
    /// columns (all user columns by default, or those set via
    /// [`with_projection`](Self::with_projection)) plus `__paimon_search_score`;
    /// `_ROW_ID` and `_PKEY_VECTOR_POSITION` are always hidden.
    pub async fn execute_read(&self) -> crate::Result<ArrowRecordBatchStream> {
        // Fail closed: returns data outside `TableScan`/`TableRead`.
        let core = CoreOptions::new(self.table.schema().options());
        core.ensure_read_authorized()?;
        let vector_column =
            self.vector_column
                .as_deref()
                .ok_or_else(|| crate::Error::ConfigInvalid {
                    message: "Vector column must be set via with_vector_column()".to_string(),
                })?;
        let query_vector =
            self.query_vector
                .as_ref()
                .ok_or_else(|| crate::Error::ConfigInvalid {
                    message: "Query vector must be set via with_query_vector()".to_string(),
                })?;
        let limit = self.limit.ok_or_else(|| crate::Error::ConfigInvalid {
            message: "Limit must be set via with_limit()".to_string(),
        })?;

        // Only the primary-key vector path can materialize rows. The data-evolution
        // (global-index) path returns data-derived row-ids, not table rows, so a
        // read against it (or against a non-PK-vector column) fails loud.
        if core.primary_key_vector_index_enabled() {
            let targets_pk_column = core
                .primary_key_vector_index_columns()
                .ok()
                .is_some_and(|cols| cols.iter().any(|c| c == vector_column));
            if targets_pk_column {
                let pk_col = core.primary_key_vector_index_column()?;
                return self
                    .execute_primary_key_vector_read(&core, &pk_col, query_vector, limit)
                    .await;
            }
        }

        // Data-evolution (global-index) vector search: materialize rows from the
        // scored global row-ids and attach the unified score column. A non-vector
        // column or a set filter fails loud inside execute_scored below.
        self.execute_de_vector_read().await
    }

    /// Materialize the best-first data-evolution vector search hits into Arrow
    /// rows. The global-index search returns global `_ROW_ID`s and their scores; a
    /// subsequent row-range read materializes those rows, and each row's score is
    /// joined back by `_ROW_ID`. Output columns are the projected user table
    /// columns (all user columns by default) plus `__paimon_search_score`; `_ROW_ID`
    /// is always hidden. A filter is unsupported here and fails loud inside
    /// `execute_scored`.
    async fn execute_de_vector_read(&self) -> crate::Result<ArrowRecordBatchStream> {
        // Validate the target column exists and is a vector-bearing type before any
        // work. The data-evolution search returns an empty result for an unknown
        // field (its scored-path behavior), which would make a typo'd or scalar
        // column look like a normal empty read here — violating `execute_read`'s
        // fail-loud contract (a C/Doris caller would see EOF, not an input error).
        // Reject it up front instead.
        let vector_column =
            self.vector_column
                .as_deref()
                .ok_or_else(|| crate::Error::ConfigInvalid {
                    message: "Vector column must be set via with_vector_column()".to_string(),
                })?;
        let field = self
            .table
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

        let sr = self.execute_scored().await?;

        // Resolve the projected user columns up front so an invalid projection
        // fails loud even when the result is empty.
        let mut read_type = self.resolve_materialize_read_type()?;

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

        let mut read_builder = self.table.new_read_builder();
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

    /// Single-query wrapper over
    /// [`plan_and_search_pk_candidates_batch`]: plan once, search the one query,
    /// and return its candidate list. Output is byte-identical to the batch-of-one
    /// path.
    async fn plan_and_search_pk_candidates(
        &self,
        core: &CoreOptions<'_>,
        pk_col: &str,
        query_vector: &[f32],
        limit: usize,
    ) -> crate::Result<(Vec<PkVectorCandidate>, PkVectorScanPlan, VectorSearchMetric)> {
        let (mut candidates, plan, metric) = plan_and_search_pk_candidates_batch(
            self.table,
            &self.options,
            self.filter.as_ref(),
            core,
            pk_col,
            &[query_vector],
            limit,
        )
        .await?;
        debug_assert_eq!(candidates.len(), 1);
        Ok((candidates.remove(0), plan, metric))
    }

    /// Materialize the best-first PK-vector search hits into Arrow rows. Mirrors
    /// Java `PrimaryKeyVectorRead` feeding its result splits into an ordinary table
    /// read: the search decides which rows, a subsequent read decides which
    /// columns.
    ///
    /// Output columns are the projected user table columns (all user columns when
    /// [`with_projection`](Self::with_projection) was not called) plus
    /// `__paimon_search_score`; `_ROW_ID` and `_PKEY_VECTOR_POSITION` are always
    /// hidden. Rows are emitted best-first (the candidate order), which differs
    /// from the file/position order the orchestrator materializes in.
    async fn execute_primary_key_vector_read(
        &self,
        core: &CoreOptions<'_>,
        pk_col: &str,
        query_vector: &[f32],
        limit: usize,
    ) -> crate::Result<ArrowRecordBatchStream> {
        let (candidates, plan, metric) = self
            .plan_and_search_pk_candidates(core, pk_col, query_vector, limit)
            .await?;

        // Resolve the materialization read-type up front so an invalid projection
        // (unknown column, or a reserved metadata / row-id name) fails loud
        // unconditionally, even when the plan is empty and no rows will be read.
        // Default (no `with_projection`) is every user table column.
        let read_type = self.resolve_materialize_read_type()?;

        // A separate, predicate-free materialization reader projecting the user
        // columns (the search reader projects only the vector column). Mirrors
        // `table_read.rs::new_data_file_reader` with an empty predicate list.
        let materialize_reader = DataFileReader::new(
            self.table.file_io().clone(),
            self.table.schema_manager().clone(),
            self.table.schema().id(),
            self.table.schema().fields().to_vec(),
            read_type,
            Vec::new(),
        );

        Self::materialize_candidates(candidates, &plan, metric, &materialize_reader).await
    }

    /// Materialize one best-first candidate list into an Arrow stream, best-first,
    /// with a `__paimon_search_score` column and `_PKEY_VECTOR_POSITION` stripped.
    /// An empty candidate list yields an empty stream (never skipped) so a batch
    /// caller preserves per-query arity. `materialize_reader` must project the
    /// output columns (predicate-free). Both the single-query and batch read paths
    /// use this so their materialization is identical.
    async fn materialize_candidates(
        candidates: Vec<PkVectorCandidate>,
        plan: &PkVectorScanPlan,
        metric: VectorSearchMetric,
        materialize_reader: &DataFileReader,
    ) -> crate::Result<ArrowRecordBatchStream> {
        if candidates.is_empty() {
            return Ok(Box::pin(stream::empty()));
        }

        // Rank each candidate by its best-first position, then reduce the physical
        // materialization order back to best-first. The orchestrator emits rows in
        // ascending (partition, bucket, file, position); the rank map keyed by
        // (partition bytes, bucket, file, position) recovers the candidate order.
        let mut rank_of: HashMap<(Vec<u8>, i32, String, i64), usize> = HashMap::new();
        for (rank, c) in candidates.iter().enumerate() {
            rank_of.insert(
                (
                    c.partition.to_serialized_bytes(),
                    c.bucket,
                    c.data_file_name.clone(),
                    c.row_position,
                ),
                rank,
            );
        }

        let indexed_splits = build_indexed_splits(candidates, &plan.splits, metric)?;

        // Materialize every indexed split, retaining each batch and, per row, the
        // (rank, batch_index, row_index) tuple so we can reorder to best-first.
        // Top-K is small, so full in-memory collection is acceptable.
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut ranked: Vec<RankedRow> = Vec::new();
        for indexed in indexed_splits {
            let partition_bytes = indexed.split.partition().to_serialized_bytes();
            let bucket = indexed.split.bucket();
            let file_name = indexed.split.data_files()[0].file_name.clone();
            let mut stream =
                PkVectorIndexedSplitRead::new(materialize_reader.clone()).read(&indexed)?;
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

    /// Resolve the projected fields for the materialization read-type. Default
    /// (no projection set) is all user table fields; otherwise the requested
    /// names resolved via `resolve_projected_fields`. Rejects reserved metadata
    /// names and `_ROW_ID` so a user cannot request a hidden column.
    fn resolve_materialize_read_type(&self) -> crate::Result<Vec<DataField>> {
        let fields = match &self.projection {
            None => self.table.schema().fields().to_vec(),
            Some(names) => {
                for name in names {
                    if is_reserved_read_column(name) {
                        return Err(crate::Error::DataInvalid {
                            message: format!(
                                "vector search read projection must not request reserved column '{name}'"
                            ),
                            source: None,
                        });
                    }
                }
                resolve_projected_fields(
                    self.table.identifier().full_name(),
                    self.table.schema().fields(),
                    names,
                    true,
                )?
            }
        };
        // The default projection returns every user column, so a user column
        // whose name collides with an injected metadata column must be rejected
        // on the resolved field list too — not only when explicitly requested.
        ensure_no_reserved_read_columns(&fields)?;
        Ok(fields)
    }
}

/// Names a read injects as metadata columns — `__paimon_search_score`,
/// `_PKEY_VECTOR_POSITION`, and `_ROW_ID` — that a materialized read type must
/// not reuse for a user column.
fn is_reserved_read_column(name: &str) -> bool {
    name == PKEY_VECTOR_POSITION_COLUMN || name == SEARCH_SCORE_COLUMN || name == ROW_ID_FIELD_NAME
}

/// Reject a materialized read type whose resolved fields contain a reserved
/// metadata column name. Applied to the RESOLVED field list so the default
/// (all user columns) projection is covered, not only an explicit one.
fn ensure_no_reserved_read_columns(fields: &[DataField]) -> crate::Result<()> {
    for field in fields {
        if is_reserved_read_column(field.name()) {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "vector search read projection must not include reserved column '{}'",
                    field.name()
                ),
                source: None,
            });
        }
    }
    Ok(())
}

/// Batch PK-vector search core shared by the single and batch builders: plan ONE
/// per-bucket split set, segment preload, ANN scorer, exact-fallback search
/// closure, and residual allow-list (all query-independent), then run
/// `search_candidates_batch` ONCE so N queries share the opened readers. Per
/// query, the approximate candidates are exact-reranked (when a refine factor is
/// set) and merged with the exact fallback into one best-first list bounded to
/// `limit`. Returns one candidate list per query (outer index aligned to
/// `queries`), together with the shared plan and resolved metric so the caller can
/// materialize each query's rows. An empty plan yields one empty candidate list
/// per query.
///
/// The residual allow-list depends only on `filter` and the plan, NOT the query
/// vector, so it is computed once and the SAME slice is shared across all queries.
/// Rerank stays per-query (each query reranks its own indexed list).
#[allow(clippy::too_many_arguments)]
async fn plan_and_search_pk_candidates_batch(
    table: &Table,
    query_options: &HashMap<String, String>,
    filter: Option<&Predicate>,
    core: &CoreOptions<'_>,
    pk_col: &str,
    queries: &[&[f32]],
    limit: usize,
) -> crate::Result<(
    Vec<Vec<PkVectorCandidate>>,
    PkVectorScanPlan,
    VectorSearchMetric,
)> {
    // Residual pre-filter guard, mirroring Java `PrimaryKeyVectorScan`. A data
    // predicate set via `with_filter` is applied post-recall by re-reading each
    // candidate file's physical rows (see below). That physical-position filtering
    // only agrees with the bucket search when the table exposes physical rows
    // directly: deletion vectors enabled and merge-on-read disabled. Under
    // merge-on-read (or without deletion vectors) a read merges multiple key
    // versions, so a scalar filter could retain a stale version whose live version
    // does not match — a silent wrong-read. Reject such queries rather than answer
    // them incorrectly. No filter → nothing to guard, so the search-only and read
    // paths are unaffected.
    let physical_row_read =
        core.deletion_vectors_enabled() && !core.deletion_vectors_merge_on_read();
    if filter.is_some() && !physical_row_read {
        return Err(crate::Error::DataInvalid {
            message:
                "primary-key vector pre-filter requires deletion vectors without merge-on-read"
                    .to_string(),
            source: None,
        });
    }
    // `primary_key_vector_distance_metric` returns a validated name; re-parse into
    // the enum for the numeric semantics.
    let metric = VectorSearchMetric::parse(&core.primary_key_vector_distance_metric(pk_col)?)?;
    // Fan-out limit for the per-bucket and per-exact-file search (Java
    // `GLOBAL_INDEX_THREAD_NUM`); `1` reproduces strictly sequential execution.
    let concurrency = core.global_index_thread_num()?;
    let index_type = core.primary_key_vector_index_type(pk_col)?;
    let field_id = find_field_id_by_name(table.schema().fields(), pk_col).ok_or_else(|| {
        crate::Error::DataInvalid {
            message: format!("PK-vector column '{pk_col}' not found in schema"),
            source: None,
        }
    })?;
    let vector_field = table
        .schema()
        .fields()
        .iter()
        .find(|f| f.name() == pk_col)
        .cloned()
        .ok_or_else(|| crate::Error::DataInvalid {
            message: format!("PK-vector column '{pk_col}' not found in schema"),
            source: None,
        })?;

    let search_mode = core.global_index_search_mode()?;
    let skip_exact_fallback = search_mode == GlobalIndexSearchMode::Fast;

    // A non-positive limit is invalid regardless of the plan; reject it before
    // planning so an empty plan cannot mask it with empty results.
    if limit == 0 {
        return Err(crate::Error::DataInvalid {
            message: "vector search limit must be positive".to_string(),
            source: None,
        });
    }

    // Resolve the refine factor from the query options first, then fall back to
    // the table options; a positive factor over-fetches indexed (approximate)
    // candidates so the exact rerank below has a wider pool to reorder. Factor 0
    // (unset) leaves `indexed_limit == limit`, byte-identical to the no-rerank
    // path. The two option maps are kept distinct (query options passed
    // separately from table options) so a broad query key cannot be overridden
    // by a more specific table key: query options take precedence as a whole.
    // Resolved before planning so an invalid factor (e.g. a non-numeric value)
    // fails loud regardless of whether the table currently has searchable data.
    let refine_factor =
        configured_refine_factor(query_options, table.schema().options(), pk_col, &index_type)?;
    let indexed_limit = indexed_search_limit(limit, refine_factor)?;

    // Validate every query against the vector column's dimension (and finiteness)
    // before planning or any read, so a malformed query fails loud even when the
    // plan turns out empty. VECTOR<FLOAT> carries the dimension in its type;
    // ARRAY<FLOAT> gets the index dimension from the same vindex option resolver
    // used by index reads. Both valid PK-vector column shapes must reject NaN/Inf
    // up front, not only after a non-empty plan opens readers.
    if let Some(dimension) = pk_vector_query_dimension(
        table.schema().options(),
        query_options,
        &index_type,
        &vector_field,
    )? {
        for query in queries {
            validate_query(query, dimension)?;
        }
    }

    let plan = PkVectorScan::new(table, field_id, index_type.clone(), filter.cloned())
        .plan()
        .await?;
    if plan.splits.is_empty() {
        return Ok((vec![Vec::new(); queries.len()], plan, metric));
    }

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

    // Real ANN scorer: preload each segment's bytes (keyed by resolved, globally
    // unique path) and drive the vindex reader from memory. The reader is opened
    // once per segment and every query in the batch is searched against it,
    // mirroring the shared-reader batch search.
    let segment_bytes = preload_segment_bytes(table.file_io(), &plan.splits).await?;
    // Fail loud on a config/segment metric mismatch before scoring, mirroring Java
    // `PkVectorAnnSegmentSearcher.search`.
    verify_pk_vector_segment_metrics(&plan.splits, &segment_bytes, metric, backend)?;
    let options = {
        let mut o = table.schema().options().clone();
        o.extend(query_options.clone());
        o
    };
    let search_options = options.clone();
    let field_name = pk_col.to_string();
    let scorer: crate::vindex::pkvector::ann::BatchScorer = Box::new(
        move |segment: &BucketAnnSegment, searches: &[VectorSearch]| {
            let data = segment_bytes
                .get(&segment.path)
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: "missing preloaded ANN bytes for segment".to_string(),
                    source: None,
                })?
                .clone();
            let io_meta = GlobalIndexIOMeta::new(
                segment.path.clone(),
                segment.file_size,
                segment.index_meta.clone(),
            );
            match backend {
                VectorIndexBackend::Lumina => {
                    let mut reader = LuminaVectorGlobalIndexReader::new(io_meta, options.clone());
                    reader.visit_batch_vector_search(searches, |_| Ok(Cursor::new(data)))
                }
                VectorIndexBackend::Vindex => {
                    let mut reader = VindexVectorGlobalIndexReader::new(io_meta, options.clone());
                    reader.visit_batch_vector_search(searches, |_| Ok(Cursor::new(data)))
                }
            }
        },
    );
    let ann_searcher = VindexAnnSearcher::new(field_name, scorer);

    // Residual (post-recall) filtering: for each candidate file, re-read its
    // physical rows and keep the positions whose rows satisfy the filter. The
    // per-split allow-list is threaded into the bucket search so the residual folds
    // into recall (best-first order and Top-K are preserved). Built only when a
    // filter is set; otherwise `None` leaves the search unfiltered. The residual
    // depends only on the filter and the plan, not the query vector, so it is
    // computed once here and shared across every query in the batch. The residual
    // reader projects only the predicate columns and carries no pushdown;
    // `residual_positions_by_file` recovers each surviving row's file-local
    // physical position from its ordinal in the unfiltered scan (no `_ROW_ID`, no
    // `first_row_id`). A file the allow-list leaves empty is skipped by the bucket
    // search without opening an exact reader.
    let residual_by_split: Option<Vec<HashMap<String, RoaringTreemap>>> = match filter {
        Some(filter) => {
            let file_predicates = FilePredicates {
                predicates: vec![filter.clone()],
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
            for split in &plan.splits {
                per_split.push(
                    residual_positions_by_file(
                        &residual_reader,
                        &split.data_split,
                        &split.active_files,
                        &file_predicates,
                    )
                    .await?,
                );
            }
            Some(per_split)
        }
        None => None,
    };

    // Build the exact-fallback search on demand: the kernel calls this only for a
    // file it actually searches (uncovered by ANN, residual-allowed, and only when
    // the search mode is not FAST). Everything the future needs is cloned/owned up
    // front so it borrows neither the split nor the file across the await. The
    // search streams the file's vector column one Arrow batch at a time into
    // per-query bounded heaps (all queries share one stream).
    let reader_for_factory = reader.clone();
    let vector_field_for_factory = vector_field.clone();
    let factory = as_split_exact_file_search(
        move |_split_index: usize,
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
            Box::pin(async move {
                let factory = DataFilePkVectorReaderFactory::new(reader, data_split, vector_field)?;
                let query_refs: Vec<&[f32]> = owned_queries.iter().map(|q| q.as_slice()).collect();
                factory
                    .search_file(&active, &query_refs, metric, exact_limit, is_excluded)
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
            Some(&ann_searcher),
            &factory,
            &search_options,
            skip_exact_fallback,
            residual_by_split.as_deref(),
            concurrency,
        )
        .await?;

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

    Ok((per_query_candidates, plan, metric))
}

impl<'a> BatchVectorSearchBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            vector_column: None,
            query_vectors: None,
            limit: None,
            options: HashMap::new(),
            projection: None,
            filter: None,
        }
    }

    pub fn with_vector_column(&mut self, name: &str) -> &mut Self {
        self.vector_column = Some(name.to_string());
        self
    }

    pub fn with_query_vectors(&mut self, vectors: Vec<Vec<f32>>) -> &mut Self {
        self.query_vectors = Some(vectors);
        self
    }

    pub fn with_limit(&mut self, limit: usize) -> &mut Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_options(&mut self, options: HashMap<String, String>) -> &mut Self {
        self.options = options;
        self
    }

    /// Attach a residual scalar predicate applied *after* vector recall on the
    /// primary-key vector path, shared across every query in the batch. Mirrors
    /// the single [`VectorSearchBuilder::with_filter`]: only the primary-key
    /// vector path (via [`execute_read`](Self::execute_read)) consumes it, and only
    /// when the table exposes physical rows directly (deletion vectors without
    /// merge-on-read); otherwise the query fails loud.
    pub fn with_filter(&mut self, filter: Predicate) -> &mut Self {
        self.filter = Some(filter);
        self
    }

    /// Restrict the columns materialized by [`execute_read`](Self::execute_read) to
    /// `cols` (plus the always-appended `__paimon_search_score`). Without this call
    /// `execute_read` materializes every user table column. Only affects
    /// `execute_read`; `execute` ignores it.
    pub fn with_projection(&mut self, cols: &[&str]) -> &mut Self {
        self.projection = Some(cols.iter().map(|c| c.to_string()).collect());
        self
    }

    pub async fn execute(&self) -> crate::Result<Vec<SearchResult>> {
        // Fail closed: like `execute_read` and the single-query builder, this
        // returns data-derived row ids/scores outside `TableScan`/`TableRead`,
        // so it must refuse a `query-auth.enabled` table before any fast path
        // (an empty snapshot would otherwise return empty results and bypass it).
        let core = CoreOptions::new(self.table.schema().options());
        core.ensure_read_authorized()?;
        let vector_column =
            self.vector_column
                .as_deref()
                .ok_or_else(|| crate::Error::ConfigInvalid {
                    message: "Vector column must be set via with_vector_column()".to_string(),
                })?;
        if vector_column.is_empty() {
            return Err(crate::Error::ConfigInvalid {
                message: "Vector column must be set via with_vector_column()".to_string(),
            });
        }

        let query_vectors =
            self.query_vectors
                .as_ref()
                .ok_or_else(|| crate::Error::ConfigInvalid {
                    message: "Query vectors must be set via with_query_vectors()".to_string(),
                })?;
        if query_vectors.is_empty() {
            return Err(crate::Error::ConfigInvalid {
                message: "Query vectors must be set via with_query_vectors()".to_string(),
            });
        }

        let limit = self.limit.ok_or_else(|| crate::Error::ConfigInvalid {
            message: "Limit must be set via with_limit()".to_string(),
        })?;

        // A primary-key vector table exposes no global row ids, so scored batch
        // search is unsupported: `execute()` returns `SearchResult`s (global row
        // ids). Fail loud and direct callers to the materialized batch
        // `execute_read`, mirroring the single-query builder's PK guard. Membership
        // is resolved via the non-erroring columns accessor so a malformed
        // PK-vector config cannot abort an unrelated DE query.
        if core.primary_key_vector_index_enabled() {
            let targets_pk_column = core
                .primary_key_vector_index_columns()
                .ok()
                .is_some_and(|cols| cols.iter().any(|c| c == vector_column));
            if targets_pk_column {
                return Err(crate::Error::DataInvalid {
                    message: "primary-key vector search does not produce global row ids; use the materialized read (execute_read) instead".to_string(),
                    source: None,
                });
            }
        }

        // The data-evolution (global-index) fall-through path cannot honor a
        // residual filter — it never reads physical rows. Rather than silently
        // drop the predicate and return unfiltered results, fail loud when a
        // filter is set on a batch that does not resolve to the primary-key
        // vector path, mirroring the single-query builder.
        if self.filter.is_some() {
            return Err(crate::Error::DataInvalid {
                message: "vector search filter is only supported on the primary-key vector path"
                    .to_string(),
                source: None,
            });
        }

        let vector_searches = query_vectors
            .iter()
            .map(|vector| {
                VectorSearch::new(vector.clone(), limit, vector_column.to_string())
                    .map(|search| search.with_options(self.options.clone()))
            })
            .collect::<crate::Result<Vec<_>>>()?;

        let snapshot_manager = self.table.snapshot_manager();

        let snapshot = match snapshot_manager.get_latest_snapshot().await? {
            Some(s) => s,
            None => return Ok(vec![SearchResult::empty(); vector_searches.len()]),
        };

        let index_entries = match snapshot.index_manifest() {
            Some(index_manifest_name) => {
                let manifest_path = snapshot_manager.manifest_path(index_manifest_name);
                IndexManifest::read(self.table.file_io(), &manifest_path).await?
            }
            None => Vec::new(),
        };

        evaluate_batch_vector_search(
            VectorSearchEvaluation {
                table: Some(self.table),
                file_io: self.table.file_io(),
                table_path: self.table.location(),
                table_options: self.table.schema().options(),
                schema_fields: self.table.schema().fields(),
                next_row_id: snapshot.next_row_id(),
            },
            &index_entries,
            &vector_searches,
        )
        .await
    }

    /// Run a batch of vector searches and materialize each query's matching rows as
    /// a best-first Arrow stream. Supported only for the primary-key vector path
    /// (which alone can materialize physical rows). The returned `Vec` is aligned
    /// strictly to the input query order and its length always equals the query
    /// count — a query with no hits yields an empty stream, never a missing entry.
    /// If ANY query errors (e.g. a malformed vector) the whole call fails loud with
    /// no partial `Vec` of streams. Output columns are the projected user table
    /// columns (all user columns by default, or those set via
    /// [`with_projection`](Self::with_projection)) plus `__paimon_search_score`;
    /// `_ROW_ID` and `_PKEY_VECTOR_POSITION` are always hidden.
    ///
    /// A data-evolution (global-index) table fails loud: its batch search returns
    /// scored global row-ids, not materialized rows, so callers use
    /// [`execute`](Self::execute) instead.
    pub async fn execute_read(&self) -> crate::Result<Vec<ArrowRecordBatchStream>> {
        // Fail closed: returns data outside `TableScan`/`TableRead`.
        let core = CoreOptions::new(self.table.schema().options());
        core.ensure_read_authorized()?;
        let vector_column =
            self.vector_column
                .as_deref()
                .ok_or_else(|| crate::Error::ConfigInvalid {
                    message: "Vector column must be set via with_vector_column()".to_string(),
                })?;
        let query_vectors =
            self.query_vectors
                .as_ref()
                .ok_or_else(|| crate::Error::ConfigInvalid {
                    message: "Query vectors must be set via with_query_vectors()".to_string(),
                })?;
        if query_vectors.is_empty() {
            return Err(crate::Error::ConfigInvalid {
                message: "Query vectors must be set via with_query_vectors()".to_string(),
            });
        }
        let limit = self.limit.ok_or_else(|| crate::Error::ConfigInvalid {
            message: "Limit must be set via with_limit()".to_string(),
        })?;

        // Only the primary-key vector path can materialize rows. The data-evolution
        // (global-index) path returns data-derived row-ids, not table rows, so a
        // batch read against it (or a non-PK-vector column) fails loud, directing
        // callers to `execute()`.
        let targets_pk_column = core.primary_key_vector_index_enabled()
            && core
                .primary_key_vector_index_columns()
                .ok()
                .is_some_and(|cols| cols.iter().any(|c| c == vector_column));
        if !targets_pk_column {
            return Err(crate::Error::DataInvalid {
                message: "batch vector read is only supported on the primary-key vector path; data-evolution batch search returns scored row ids, use execute() instead".to_string(),
                source: None,
            });
        }

        let pk_col = core.primary_key_vector_index_column()?;
        let query_refs: Vec<&[f32]> = query_vectors.iter().map(|q| q.as_slice()).collect();

        // Resolve the materialization read-type up front so an invalid projection
        // (unknown column, or a reserved metadata / row-id name) fails loud
        // unconditionally, before any read — a whole-call failure, not a partial
        // Vec.
        let read_type = self.resolve_materialize_read_type()?;

        // One shared plan / segment preload / residual across all N queries; the
        // per-query candidate lists come back in strict input order. Any query
        // error (or a shared-plan error) propagates here, so no partial Vec is
        // returned.
        let (per_query_candidates, plan, metric) = plan_and_search_pk_candidates_batch(
            self.table,
            &self.options,
            self.filter.as_ref(),
            &core,
            &pk_col,
            &query_refs,
            limit,
        )
        .await?;

        let materialize_reader = DataFileReader::new(
            self.table.file_io().clone(),
            self.table.schema_manager().clone(),
            self.table.schema().id(),
            self.table.schema().fields().to_vec(),
            read_type,
            Vec::new(),
        );

        // Materialize each query's candidates into its own stream, preserving
        // arity: an empty candidate list yields an empty stream. Build every stream
        // before returning so a materialization error fails the whole call with no
        // partial Vec.
        let mut streams = Vec::with_capacity(per_query_candidates.len());
        for candidates in per_query_candidates {
            streams.push(
                VectorSearchBuilder::materialize_candidates(
                    candidates,
                    &plan,
                    metric,
                    &materialize_reader,
                )
                .await?,
            );
        }
        Ok(streams)
    }

    /// Resolve the projected fields for the materialization read-type. Default
    /// (no projection set) is all user table fields; otherwise the requested names
    /// resolved via `resolve_projected_fields`. Rejects reserved metadata names and
    /// `_ROW_ID` so a user cannot request a hidden column. Mirrors the single
    /// builder's resolver.
    fn resolve_materialize_read_type(&self) -> crate::Result<Vec<DataField>> {
        let fields = match &self.projection {
            None => self.table.schema().fields().to_vec(),
            Some(names) => {
                for name in names {
                    if is_reserved_read_column(name) {
                        return Err(crate::Error::DataInvalid {
                            message: format!(
                                "vector search read projection must not request reserved column '{name}'"
                            ),
                            source: None,
                        });
                    }
                }
                resolve_projected_fields(
                    self.table.identifier().full_name(),
                    self.table.schema().fields(),
                    names,
                    true,
                )?
            }
        };
        // The default projection returns every user column, so a user column
        // whose name collides with an injected metadata column must be rejected
        // on the resolved field list too — not only when explicitly requested.
        ensure_no_reserved_read_columns(&fields)?;
        Ok(fields)
    }
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

#[cfg(test)]
async fn evaluate_vector_search(
    evaluation: VectorSearchEvaluation<'_>,
    index_entries: &[IndexManifestEntry],
    vector_search: &VectorSearch,
) -> crate::Result<Vec<RowRange>> {
    let mut results = evaluate_batch_vector_search(
        evaluation,
        index_entries,
        std::slice::from_ref(vector_search),
    )
    .await?;
    debug_assert_eq!(results.len(), 1);
    results.remove(0).to_row_ranges()
}

async fn evaluate_batch_vector_search(
    evaluation: VectorSearchEvaluation<'_>,
    index_entries: &[IndexManifestEntry],
    vector_searches: &[VectorSearch],
) -> crate::Result<Vec<SearchResult>> {
    if vector_searches.is_empty() {
        return Ok(Vec::new());
    }

    let table_path = evaluation.table_path.trim_end_matches('/');
    let core_options = CoreOptions::new(evaluation.table_options);
    let search_mode = core_options.global_index_search_mode()?;
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
        None => return Ok(vec![SearchResult::empty(); vector_searches.len()]),
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
        return Ok(vec![SearchResult::empty(); vector_searches.len()]);
    }

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

    let mut merged = vec![SearchResult::empty(); vector_searches.len()];
    if !vector_entries.is_empty() {
        let futures: Vec<_> = vector_entries
            .into_iter()
            .map(|entry| {
                let global_meta = entry.index_file.global_index_meta.as_ref().unwrap();
                let backend = VectorIndexBackend::from_index_type(&entry.index_file.index_type)
                    .expect("filtered vector index type");
                let path = format!("{table_path}/{INDEX_DIR}/{}", entry.index_file.file_name);
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
                    let input = input?;
                    let bytes = input.read().await.map_err(|e| crate::Error::DataInvalid {
                        message: format!(
                            "Failed to read {} index file '{}': {}",
                            backend.error_name(),
                            file_name,
                            e
                        ),
                        source: None,
                    })?;

                    let io_meta =
                        GlobalIndexIOMeta::new(file_name.clone(), file_size, index_meta_bytes);
                    let data = bytes.to_vec();
                    let results = match backend {
                        VectorIndexBackend::Lumina => {
                            let mut reader = LuminaVectorGlobalIndexReader::new(io_meta, options);
                            reader.visit_batch_vector_search(&vector_searches, |_| {
                                Ok(Cursor::new(data))
                            })?
                        }
                        VectorIndexBackend::Vindex => {
                            let mut reader = VindexVectorGlobalIndexReader::new(io_meta, options);
                            reader.visit_batch_vector_search(&vector_searches, |_| {
                                Ok(Cursor::new(data))
                            })?
                        }
                    };
                    if results.len() != vector_searches.len() {
                        return Err(crate::Error::DataInvalid {
                            message: format!(
                                "Batch vector search backend returned {} results for {} query vectors",
                                results.len(),
                                vector_searches.len()
                            ),
                            source: None,
                        });
                    }

                    Ok::<_, crate::Error>(
                        results
                            .into_iter()
                            .map(|result| match result {
                                Some(scored_map) => SearchResult::from_scored_map(scored_map)
                                    .offset(row_range_start),
                                None => SearchResult::empty(),
                            })
                            .collect::<Vec<_>>(),
                    )
                }
            })
            .collect();

        let results = futures::future::try_join_all(futures).await?;
        for per_entry in &results {
            for (query_index, result) in per_entry.iter().enumerate() {
                merged[query_index] = merged[query_index].or(result);
            }
        }
    }

    if refine_factor != 0 {
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
    }

    if search_mode != GlobalIndexSearchMode::Fast {
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
            let metric = resolve_raw_vector_metric(
                evaluation.file_io,
                table_path,
                evaluation.table_options,
                index_entries,
                field_id,
                field_name,
            )
            .await?;
            let raw_results =
                read_raw_batch_vector_search(table, vector_searches, &raw_ranges, metric).await?;
            for (query_index, result) in raw_results.iter().enumerate() {
                merged[query_index] = merged[query_index].or(result);
            }
        }
    }

    merged
        .into_iter()
        .zip(vector_searches)
        .map(|(result, vector_search)| {
            Ok(result
                .without_deleted_row_ranges(deleted_row_index.as_ref())?
                .top_k(vector_search.limit))
        })
        .collect()
}

fn is_vector_global_index_file(index_file: &IndexFileMeta) -> bool {
    VectorIndexBackend::from_index_type(&index_file.index_type).is_some()
}

/// Compute, per data file in `split`, the set of file-LOCAL physical row
/// positions whose rows satisfy the residual predicate. Mirrors the
/// row-collecting half of Java `PrimaryKeyVectorRead`'s `executeFilter`: the
/// predicate is NOT pushed down (a pushed filter would drop rows before their
/// position could be recovered). Instead `reader` projects only the residual
/// columns and carries no pushdown predicate; every physical row is scanned in
/// file order, the residual is evaluated here at the Arrow level, and each
/// surviving row's file-local 0-based position is its running ordinal in the scan.
/// This needs no `_ROW_ID` and no `first_row_id` — real primary-key tables never
/// write one.
///
/// Every *active* data file in the split gets an entry, possibly empty. The
/// bucket search treats an absent entry and an empty entry identically (the file
/// contributes no candidates), so the empty entries only make the map cover every
/// active file. Non-active files (e.g. level-0 files the bucket search excludes)
/// are skipped entirely: they are never searched, so re-reading them would be
/// wasted IO.
///
/// `reader` must be predicate-free and project the residual columns;
/// `residual.file_fields` are the fields the residual leaf indices point into
/// (resolved by name against each emitted batch).
async fn residual_positions_by_file(
    reader: &DataFileReader,
    split: &DataSplit,
    active_files: &[BucketActiveFile],
    residual: &FilePredicates,
) -> crate::Result<HashMap<String, RoaringTreemap>> {
    let scan_fields = reader.read_type().to_vec();
    let active_names: HashSet<&str> = active_files.iter().map(|f| f.file_name.as_str()).collect();
    let mut out: HashMap<String, RoaringTreemap> = HashMap::new();
    for file_meta in split.data_files() {
        // Only files the bucket search actually recalls from need residual
        // positions; skip everything else to avoid a wasted read.
        if !active_names.contains(file_meta.file_name.as_str()) {
            continue;
        }
        let data_fields = reader.derive_data_fields(file_meta).await?;
        let mut stream =
            reader.read_single_file_stream(split, file_meta.clone(), data_fields, None, None)?;
        // Register the file up front so a file whose rows all fail the residual
        // still appears in the map (empty set).
        let positions = out.entry(file_meta.file_name.clone()).or_default();
        // The scan has no row selection and no DV, so rows arrive in physical file
        // order with no gaps: each row's file-local 0-based position is its running
        // ordinal `base + row_index`.
        let mut base: u64 = 0;
        while let Some(batch) = stream.try_next().await? {
            let num_rows = batch.num_rows();
            let mask = evaluate_predicates_mask(
                &batch,
                &residual.predicates,
                &residual.file_fields,
                &scan_fields,
            )?;
            match mask {
                Some(mask) => {
                    for row_index in 0..num_rows {
                        // NULL follows the same NULL -> false convention the Arrow
                        // filter kernel applies, so a null mask slot drops the row.
                        if mask.is_valid(row_index) && mask.value(row_index) {
                            positions.insert(base + row_index as u64);
                        }
                    }
                }
                // No predicate contributed a mask (identity) -> keep every row.
                None => {
                    for row_index in 0..num_rows {
                        positions.insert(base + row_index as u64);
                    }
                }
            }
            base += num_rows as u64;
        }
    }
    Ok(out)
}

/// Preload every ANN segment's bytes into a map keyed by the resolved (globally
/// unique) segment path. The scorer closure reads from this map so the vindex
/// reader is driven from memory without per-search IO.
async fn preload_segment_bytes(
    file_io: &FileIO,
    splits: &[PkVectorSearchSplit],
) -> crate::Result<HashMap<String, Vec<u8>>> {
    let mut out = HashMap::new();
    for split in splits {
        for segment in &split.ann_segments {
            if out.contains_key(&segment.path) {
                continue;
            }
            let input = file_io.new_input(&segment.path)?;
            let bytes = input.read().await.map_err(|e| crate::Error::DataInvalid {
                message: format!("failed to read ANN index file '{}': {e}", segment.path),
                source: None,
            })?;
            out.insert(segment.path.clone(), bytes.to_vec());
        }
    }
    Ok(out)
}

/// Fail loud when an ANN segment was trained with a metric other than the
/// configured one, mirroring the search-time `checkArgument` in Java
/// `PkVectorAnnSegmentSearcher.search`. Opens each distinct segment's preloaded
/// bytes once and compares its trained metric against `configured`.
fn verify_pk_vector_segment_metrics(
    splits: &[PkVectorSearchSplit],
    segment_bytes: &HashMap<String, Vec<u8>>,
    configured: VectorSearchMetric,
    backend: VectorIndexBackend,
) -> crate::Result<()> {
    let mut checked: HashSet<&str> = HashSet::new();
    for split in splits {
        for segment in &split.ann_segments {
            if !checked.insert(segment.path.as_str()) {
                continue;
            }
            let segment_metric = match backend {
                VectorIndexBackend::Lumina => {
                    // Lumina records its metric in the serialized index metadata
                    // (`index_meta`), not in the segment file bytes.
                    let lumina_metric =
                        LuminaIndexMeta::deserialize(&segment.index_meta)?.metric()?;
                    VectorSearchMetric::from_lumina(lumina_metric)
                }
                VectorIndexBackend::Vindex => {
                    let bytes = segment_bytes.get(&segment.path).ok_or_else(|| {
                        crate::Error::DataInvalid {
                            message: format!(
                                "missing preloaded ANN bytes for segment '{}'",
                                segment.path
                            ),
                            source: None,
                        }
                    })?;
                    let reader = VIndexReader::open(Cursor::new(bytes.clone())).map_err(|e| {
                        crate::Error::DataInvalid {
                            message: format!(
                                "failed to open ANN index file '{}' for metric check: {e}",
                                segment.path
                            ),
                            source: Some(Box::new(e)),
                        }
                    })?;
                    VectorSearchMetric::from_vindex(reader.metadata().metric)
                }
            };
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
        }
    }
    Ok(())
}

fn pk_vector_query_dimension(
    table_options: &HashMap<String, String>,
    query_options: &HashMap<String, String>,
    index_type: &str,
    vector_field: &DataField,
) -> crate::Result<Option<usize>> {
    match vector_field.data_type() {
        DataType::Vector(vector_type)
            if matches!(vector_type.element_type(), DataType::Float(_)) =>
        {
            Ok(Some(vector_type.length() as usize))
        }
        DataType::Array(array_type) if matches!(array_type.element_type(), DataType::Float(_)) => {
            // Resolve the dimension per the configured backend. An `ARRAY<FLOAT>`
            // column carries no dimension in its type, so it comes from options —
            // but the option shape differs by backend. Lumina is not a vindex
            // index type, so routing it through `VindexVectorIndexOptions` would
            // reject it as unsupported before planning (even on an empty table).
            if is_lumina_index_type(index_type) {
                // Lumina reads `lumina.index.dimension` (default 128) from the
                // merged table+query options, matching `resolve_lumina_options`.
                let mut merged = table_options.clone();
                merged.extend(query_options.clone());
                let dimension = LuminaVectorIndexOptions::new(&merged)?.dimension;
                Ok(Some(dimension as usize))
            } else {
                Ok(Some(
                    VindexVectorIndexOptions::new(
                        table_options,
                        query_options,
                        index_type,
                        vector_field,
                    )?
                    .dimension(),
                ))
            }
        }
        _ => Ok(None),
    }
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

/// One materialized row tagged with its best-first `rank` and its `(batch_index,
/// row_index)` location in the retained materialization batches.
struct RankedRow {
    rank: usize,
    batch_index: usize,
    row_index: usize,
}

/// For each row in a materialized batch, look up its best-first rank via the
/// `(partition bytes, bucket, file, position)` key and record its location. The
/// `_PKEY_VECTOR_POSITION` column supplies the physical position; every row must
/// map to a candidate rank (the batch came from that candidate's file), so a miss
/// fails loud rather than silently dropping a row.
#[allow(clippy::too_many_arguments)]
fn collect_ranked_rows(
    batch: &RecordBatch,
    batch_index: usize,
    partition_bytes: &[u8],
    bucket: i32,
    file_name: &str,
    rank_of: &HashMap<(Vec<u8>, i32, String, i64), usize>,
    out: &mut Vec<RankedRow>,
) -> crate::Result<()> {
    let position_idx = batch
        .schema()
        .index_of(PKEY_VECTOR_POSITION_COLUMN)
        .map_err(|_| crate::Error::DataInvalid {
            message: format!("materialized batch missing {PKEY_VECTOR_POSITION_COLUMN} column"),
            source: None,
        })?;
    let positions = batch
        .column(position_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| crate::Error::DataInvalid {
            message: format!("{PKEY_VECTOR_POSITION_COLUMN} column is not Int64"),
            source: None,
        })?;
    for row_index in 0..batch.num_rows() {
        let position = positions.value(row_index);
        let key = (
            partition_bytes.to_vec(),
            bucket,
            file_name.to_string(),
            position,
        );
        let rank = *rank_of.get(&key).ok_or_else(|| crate::Error::DataInvalid {
            message: format!(
                "materialized row (file {file_name}, position {position}) has no matching search candidate"
            ),
            source: None,
        })?;
        out.push(RankedRow {
            rank,
            batch_index,
            row_index,
        });
    }
    Ok(())
}

/// Reorder the materialized rows into best-first order and drop the internal
/// `_PKEY_VECTOR_POSITION` column, yielding a single output batch (empty input
/// yields no batches). The projected user columns and `__paimon_search_score` are
/// retained.
fn reorder_and_strip_position(
    batches: &[RecordBatch],
    mut ranked: Vec<RankedRow>,
) -> crate::Result<Vec<RecordBatch>> {
    if ranked.is_empty() {
        return Ok(Vec::new());
    }
    ranked.sort_by_key(|r| r.rank);
    let indices: Vec<(usize, usize)> = ranked
        .iter()
        .map(|r| (r.batch_index, r.row_index))
        .collect();
    let refs: Vec<&RecordBatch> = batches.iter().collect();
    let reordered =
        interleave_record_batch(&refs, &indices).map_err(|e| crate::Error::DataInvalid {
            message: format!("failed to reorder vector search read rows: {e}"),
            source: None,
        })?;

    // Drop the internal position column; keep every other column (projected user
    // columns + __paimon_search_score) in order.
    let position_idx = reordered
        .schema()
        .index_of(PKEY_VECTOR_POSITION_COLUMN)
        .map_err(|_| crate::Error::DataInvalid {
            message: format!("reordered batch missing {PKEY_VECTOR_POSITION_COLUMN} column"),
            source: None,
        })?;
    let keep: Vec<usize> = (0..reordered.num_columns())
        .filter(|i| *i != position_idx)
        .collect();
    let projected = reordered
        .project(&keep)
        .map_err(|e| crate::Error::DataInvalid {
            message: format!("failed to drop position column: {e}"),
            source: None,
        })?;
    Ok(vec![projected])
}

/// The `_ROW_ID` field to append to a data-evolution read type so the reader
/// fills each row's global id. Mirrors the field the `DataEvolutionReader`
/// recognizes (Int64 / `BigInt`, nullable): a data file lacking `first_row_id`
/// yields nulls here, which `attach_scores_by_row_id` then fails loud on rather
/// than mis-aligning scores.
fn row_id_data_field() -> DataField {
    DataField::new(
        ROW_ID_FIELD_ID,
        ROW_ID_FIELD_NAME.to_string(),
        DataType::BigInt(BigIntType::with_nullable(true)),
    )
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

fn indexed_search_limit(limit: usize, refine_factor: usize) -> crate::Result<usize> {
    if refine_factor == 0 {
        return Ok(limit);
    }
    let search_limit =
        limit
            .checked_mul(refine_factor)
            .ok_or_else(|| crate::Error::ConfigInvalid {
                message: format!(
                    "Vector search limit overflow: limit={limit}, refine factor={refine_factor}"
                ),
            })?;
    if search_limit > i32::MAX as usize {
        return Err(crate::Error::ConfigInvalid {
            message: format!(
                "Vector search limit overflow: limit={limit}, refine factor={refine_factor}"
            ),
        });
    }
    Ok(search_limit)
}

async fn maybe_rerank_indexed_batch_results(
    evaluation: VectorSearchEvaluation<'_>,
    index_entries: &[IndexManifestEntry],
    field_id: i32,
    field_name: &str,
    vector_searches: &[VectorSearch],
    results: Vec<SearchResult>,
    index_search_limit: usize,
) -> crate::Result<Vec<SearchResult>> {
    let mut candidate_searches = Vec::with_capacity(vector_searches.len());
    let mut candidate_results = Vec::with_capacity(vector_searches.len());
    let mut union_candidates = RoaringTreemap::new();

    for (result, vector_search) in results.into_iter().zip(vector_searches) {
        let candidates = result.top_k(index_search_limit);
        let mut include_row_ids = RoaringTreemap::new();
        for &row_id in &candidates.row_ids {
            include_row_ids.insert(row_id);
            union_candidates.insert(row_id);
        }

        let mut candidate_search = vector_search.clone();
        candidate_search.include_row_ids = Some(include_row_ids);
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
    let raw_ranges = sorted_row_ids_to_row_ranges(union_candidates.iter())?;
    let metric = resolve_raw_vector_metric(
        evaluation.file_io,
        evaluation.table_path.trim_end_matches('/'),
        evaluation.table_options,
        index_entries,
        field_id,
        field_name,
    )
    .await?;

    read_raw_batch_vector_search(table, &candidate_searches, &raw_ranges, metric).await
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

fn normalize_metric(metric: &str) -> String {
    metric.to_ascii_lowercase().replace('-', "_")
}

fn indexed_type_prefixes(field_name: &str, index_type: &str) -> Vec<String> {
    let mut prefixes = Vec::new();
    add_refine_prefixes(&mut prefixes, &format!("fields.{field_name}."), index_type);
    add_refine_prefixes(&mut prefixes, "", index_type);
    prefixes
}

fn add_refine_prefixes(prefixes: &mut Vec<String>, base: &str, index_type: &str) {
    if !index_type.is_empty() {
        prefixes.push(format!("{base}{index_type}."));
        let normalized = normalize_metric(index_type);
        if normalized != index_type {
            prefixes.push(format!("{base}{normalized}."));
        }
        if normalized.starts_with("ivf") {
            prefixes.push(format!("{base}ivf."));
        }
    }
    prefixes.push(base.to_string());
}

fn configured_refine_factor(
    search_options: &HashMap<String, String>,
    table_options: &HashMap<String, String>,
    field_name: &str,
    index_type: &str,
) -> crate::Result<usize> {
    if let Some(value) =
        configured_refine_factor_from_options(search_options, field_name, index_type)
    {
        return parse_refine_factor(&value);
    }
    if let Some(value) =
        configured_refine_factor_from_options(table_options, field_name, index_type)
    {
        return parse_refine_factor(&value);
    }
    Ok(0)
}

fn configured_refine_factor_from_options(
    options: &HashMap<String, String>,
    field_name: &str,
    index_type: &str,
) -> Option<String> {
    for prefix in indexed_type_prefixes(field_name, index_type) {
        for suffix in [
            "refine_factor",
            "refine-factor",
            "rerank_factor",
            "rerank-factor",
        ] {
            if let Some(value) = options.get(&(prefix.clone() + suffix)) {
                return Some(value.trim().to_string());
            }
        }
    }
    None
}

fn parse_refine_factor(value: &str) -> crate::Result<usize> {
    let factor = value
        .parse::<usize>()
        .map_err(|_| crate::Error::ConfigInvalid {
            message: format!("Invalid vector refine factor: {value}. Must be an integer."),
        })?;
    if factor == 0 {
        return Err(crate::Error::ConfigInvalid {
            message: format!("Vector refine factor must be positive, got: {value}"),
        });
    }
    Ok(factor)
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
                let path = format!("{table_path}/{INDEX_DIR}/{}", entry.index_file.file_name);
                let input = file_io.new_input(&path)?;
                let bytes = input.read().await.map_err(|e| crate::Error::DataInvalid {
                    message: format!(
                        "Failed to read vindex index file '{}' for raw search metric: {}",
                        entry.index_file.file_name, e
                    ),
                    source: None,
                })?;
                let reader = VIndexReader::open(Cursor::new(bytes.to_vec())).map_err(|e| {
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

async fn read_raw_batch_vector_search(
    table: &Table,
    vector_searches: &[VectorSearch],
    raw_ranges: &[RowRange],
    metric: RawVectorMetric,
) -> crate::Result<Vec<SearchResult>> {
    if vector_searches.is_empty() {
        return Ok(Vec::new());
    }
    if raw_ranges.is_empty() {
        return Ok(vec![SearchResult::empty(); vector_searches.len()]);
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

    let mut read_builder = table.new_read_builder();
    read_builder
        .with_projection(&[field_name.as_str(), ROW_ID_FIELD_NAME])?
        .with_row_ranges(raw_ranges.to_vec());
    let plan = read_builder.new_scan().plan().await?;
    if plan.splits().is_empty() {
        return Ok(vec![SearchResult::empty(); vector_searches.len()]);
    }
    let read = read_builder.new_read()?;
    let mut stream = read.to_arrow(plan.splits())?;

    let scoring_plan = RawScoringPlan::new(vector_searches, metric);
    let mut top_k = vector_searches
        .iter()
        .map(|vector_search| RawScoreTopK::new(vector_search.limit))
        .collect::<Vec<_>>();
    while let Some(batch) = stream.try_next().await? {
        collect_raw_batch_vector_batch(&batch, vector_searches, metric, &scoring_plan, &mut top_k)?;
    }

    Ok(top_k
        .into_iter()
        .map(RawScoreTopK::into_search_result)
        .collect())
}

struct RawScoringPlan {
    all_query_indices: Vec<usize>,
    candidate_query_indices: HashMap<u64, Vec<usize>>,
    query_l2_norms: Vec<f32>,
}

impl RawScoringPlan {
    fn new(vector_searches: &[VectorSearch], metric: RawVectorMetric) -> Self {
        let mut all_query_indices = Vec::new();
        let mut candidate_query_indices: HashMap<u64, Vec<usize>> = HashMap::new();
        let query_l2_norms = vector_searches
            .iter()
            .map(|vector_search| match metric {
                RawVectorMetric::Cosine => vector_search
                    .vector
                    .iter()
                    .map(|value| value * value)
                    .sum::<f32>()
                    .sqrt(),
                RawVectorMetric::L2 | RawVectorMetric::InnerProduct => 0.0,
            })
            .collect();

        for (query_index, vector_search) in vector_searches.iter().enumerate() {
            if let Some(include_row_ids) = &vector_search.include_row_ids {
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

        Self {
            all_query_indices,
            candidate_query_indices,
            query_l2_norms,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct RawScoredRow {
    row_id: u64,
    score: f32,
}

impl Eq for RawScoredRow {}

impl PartialOrd for RawScoredRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RawScoredRow {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .score
            .total_cmp(&self.score)
            .then_with(|| self.row_id.cmp(&other.row_id))
    }
}

impl RawScoredRow {
    fn is_stronger_than(&self, other: &Self) -> bool {
        self.score
            .total_cmp(&other.score)
            .then_with(|| other.row_id.cmp(&self.row_id))
            == Ordering::Greater
    }
}

struct RawScoreTopK {
    limit: usize,
    heap: BinaryHeap<RawScoredRow>,
}

impl RawScoreTopK {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            heap: BinaryHeap::with_capacity(limit.min(1024).saturating_add(1)),
        }
    }

    fn offer(&mut self, row_id: u64, score: f32) {
        if self.limit == 0 {
            return;
        }
        let entry = RawScoredRow { row_id, score };
        if self.heap.len() < self.limit {
            self.heap.push(entry);
        } else if self
            .heap
            .peek()
            .is_some_and(|weakest| entry.is_stronger_than(weakest))
        {
            self.heap.pop();
            self.heap.push(entry);
        }
    }

    fn into_search_result(self) -> SearchResult {
        let mut rows = self.heap.into_vec();
        rows.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.row_id.cmp(&b.row_id))
        });
        let mut row_ids = Vec::with_capacity(rows.len());
        let mut scores = Vec::with_capacity(rows.len());
        for row in rows {
            row_ids.push(row.row_id);
            scores.push(row.score);
        }
        SearchResult::new(row_ids, scores)
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
                (row * len, (row + 1) * len)
            }
        };
        ensure_raw_vector_values_not_null(values, start, end)?;

        let raw_row = RawVectorRow {
            row_id,
            values,
            start,
            end,
        };
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
    }

    Ok(())
}

fn ensure_raw_vector_values_not_null(
    values: &Float32Array,
    start: usize,
    end: usize,
) -> crate::Result<()> {
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
    if stored_len != vector_search.vector.len() {
        return Err(crate::Error::DataInvalid {
            message: format!(
                "Query vector dimension mismatch: raw row has {}, but query has {}",
                stored_len,
                vector_search.vector.len()
            ),
            source: None,
        });
    }
    let score = compute_raw_vector_score_from_values(
        &vector_search.vector,
        scoring_plan.query_l2_norms[query_index],
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
    query_l2_norm: f32,
    values: &Float32Array,
    start: usize,
    end: usize,
    metric: RawVectorMetric,
) -> f32 {
    debug_assert_eq!(query.len(), end - start);
    match metric {
        RawVectorMetric::L2 => {
            let sum_sq = query
                .iter()
                .zip(start..end)
                .map(|(q, value_index)| {
                    let diff = q - values.value(value_index);
                    diff * diff
                })
                .sum::<f32>();
            1.0 / (1.0 + sum_sq)
        }
        RawVectorMetric::Cosine => {
            let mut dot = 0.0;
            let mut norm_b = 0.0;
            for (q, value_index) in query.iter().zip(start..end) {
                let stored = values.value(value_index);
                dot += q * stored;
                norm_b += stored * stored;
            }
            let denominator = query_l2_norm * norm_b.sqrt();
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

fn row_id_to_u64(row_id: i64) -> crate::Result<u64> {
    u64::try_from(row_id).map_err(|_| crate::Error::DataInvalid {
        message: format!("Negative _ROW_ID {row_id} cannot be used for global index search"),
        source: None,
    })
}

#[cfg(test)]
fn compute_raw_vector_score(query: &[f32], stored: &[f32], metric: RawVectorMetric) -> f32 {
    match metric {
        RawVectorMetric::L2 => {
            let sum_sq = query
                .iter()
                .zip(stored.iter())
                .map(|(q, s)| {
                    let diff = q - s;
                    diff * diff
                })
                .sum::<f32>();
            1.0 / (1.0 + sum_sq)
        }
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
mod tests {
    use super::*;
    use crate::catalog::Identifier;
    use crate::io::FileIOBuilder;
    use crate::lumina::{LEGACY_LUMINA_VECTOR_ANN_IDENTIFIER, LUMINA_IDENTIFIER};
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{
        ArrayType, BinaryRow, DataFileMeta, DataType, Datum, FloatType, GlobalIndexMeta,
        IndexFileMeta, IndexManifestEntry, IntType, PredicateBuilder, Schema, TableSchema,
    };
    use crate::table::source::DataSplitBuilder;
    use crate::table::{TableCommit, TableWrite};
    use crate::vindex::IVF_FLAT_IDENTIFIER;
    use arrow_array::builder::{FixedSizeListBuilder, Float32Builder, ListBuilder};
    use arrow_array::ArrayRef;
    use arrow_array::Int32Array;
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use std::sync::Arc;

    fn l2_score(distance: f32) -> f32 {
        VectorSearchMetric::L2.distance_to_score(distance)
    }

    fn make_field(id: i32, name: &str) -> DataField {
        DataField::new(id, name.to_string(), DataType::Int(IntType::default()))
    }

    fn vector_test_table() -> Table {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column(
                "embedding",
                DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
            )
            .build()
            .unwrap();
        Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("default", "vector_test"),
            "memory:/vector_test".to_string(),
            TableSchema::new(0, &schema),
            None,
        )
    }

    fn eval_context<'a>(
        file_io: &'a FileIO,
        options: &'a HashMap<String, String>,
        fields: &'a [DataField],
        next_row_id: Option<i64>,
    ) -> VectorSearchEvaluation<'a> {
        VectorSearchEvaluation {
            table: None,
            file_io,
            table_path: "memory:///test_table",
            table_options: options,
            schema_fields: fields,
            next_row_id,
        }
    }

    #[test]
    fn test_find_field_id_by_name() {
        let fields = vec![make_field(1, "id"), make_field(2, "embedding")];
        assert_eq!(find_field_id_by_name(&fields, "embedding"), Some(2));
        assert_eq!(find_field_id_by_name(&fields, "nonexistent"), None);
    }

    #[test]
    fn test_raw_vector_score_matches_java_metric_semantics() {
        let l2 = compute_raw_vector_score(&[1.0, 2.0], &[1.0, 4.0], RawVectorMetric::L2);
        assert!((l2 - 0.2).abs() < 1e-6);
        assert_eq!(
            compute_raw_vector_score(&[1.0, 2.0], &[3.0, 4.0], RawVectorMetric::InnerProduct),
            11.0
        );
        let cosine = compute_raw_vector_score(&[1.0, 0.0], &[1.0, 1.0], RawVectorMetric::Cosine);
        assert!((cosine - std::f32::consts::FRAC_1_SQRT_2).abs() < 1e-6);
        assert_eq!(
            compute_raw_vector_score(&[0.0, 0.0], &[1.0, 1.0], RawVectorMetric::Cosine),
            0.0
        );
    }

    #[test]
    fn test_configured_raw_vector_metric_precedence_and_conflict_default() {
        let mut options = HashMap::new();
        options.insert(
            "fields.embedding.distance.metric".to_string(),
            "inner-product".to_string(),
        );
        options.insert("metric".to_string(), "cosine".to_string());
        assert_eq!(
            configured_raw_vector_metric(&options, "embedding").unwrap(),
            RawVectorMetric::InnerProduct
        );

        options.clear();
        options.insert("foo.metric".to_string(), "cosine".to_string());
        options.insert("bar.distance.metric".to_string(), "l2".to_string());
        assert_eq!(
            configured_raw_vector_metric(&options, "embedding").unwrap(),
            RawVectorMetric::L2
        );
    }

    #[test]
    fn test_configured_refine_factor_precedence_and_aliases() {
        let table_options = HashMap::from([(
            "fields.embedding.ivf.refine-factor".to_string(),
            "3".to_string(),
        )]);
        let search_options = HashMap::from([(
            "fields.embedding.ivf_flat.rerank_factor".to_string(),
            "2".to_string(),
        )]);
        assert_eq!(
            configured_refine_factor(
                &search_options,
                &table_options,
                "embedding",
                IVF_FLAT_IDENTIFIER,
            )
            .unwrap(),
            2
        );

        assert_eq!(
            configured_refine_factor(
                &HashMap::new(),
                &table_options,
                "embedding",
                IVF_FLAT_IDENTIFIER,
            )
            .unwrap(),
            3
        );

        let global_options = HashMap::from([("rerank-factor".to_string(), "4".to_string())]);
        assert_eq!(
            configured_refine_factor(
                &HashMap::new(),
                &global_options,
                "embedding",
                LUMINA_IDENTIFIER,
            )
            .unwrap(),
            4
        );
    }

    #[test]
    fn test_configured_refine_factor_rejects_invalid_values() {
        let zero_options = HashMap::from([("refine_factor".to_string(), "0".to_string())]);
        let err = configured_refine_factor(
            &zero_options,
            &HashMap::new(),
            "embedding",
            LUMINA_IDENTIFIER,
        )
        .unwrap_err();
        assert!(err.to_string().contains("must be positive"));

        let invalid_options = HashMap::from([("refine_factor".to_string(), "abc".to_string())]);
        let err = configured_refine_factor(
            &invalid_options,
            &HashMap::new(),
            "embedding",
            LUMINA_IDENTIFIER,
        )
        .unwrap_err();
        assert!(err.to_string().contains("Must be an integer"));

        assert!(indexed_search_limit(i32::MAX as usize, 2).is_err());
    }

    #[test]
    fn test_collect_raw_batch_vector_batch_preserves_query_order() {
        let element_field = Arc::new(ArrowField::new("element", ArrowDataType::Float32, true));
        let mut builder =
            FixedSizeListBuilder::new(Float32Builder::new(), 2).with_field(element_field);
        for vector in [[1.0, 0.0], [0.0, 1.0], [0.8, 0.2]] {
            builder.values().append_value(vector[0]);
            builder.values().append_value(vector[1]);
            builder.append(true);
        }
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new(
                "embedding",
                ArrowDataType::FixedSizeList(
                    Arc::new(ArrowField::new("element", ArrowDataType::Float32, true)),
                    2,
                ),
                true,
            ),
            ArrowField::new(ROW_ID_FIELD_NAME, ArrowDataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(builder.finish()) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(10), Some(11), Some(12)])) as ArrayRef,
            ],
        )
        .unwrap();
        let searches = vec![
            VectorSearch::new(vec![1.0, 0.0], 1, "embedding".to_string()).unwrap(),
            VectorSearch::new(vec![0.0, 1.0], 1, "embedding".to_string()).unwrap(),
        ];
        let scoring_plan = RawScoringPlan::new(&searches, RawVectorMetric::L2);
        let mut top_k = searches
            .iter()
            .map(|search| RawScoreTopK::new(search.limit))
            .collect::<Vec<_>>();

        collect_raw_batch_vector_batch(
            &batch,
            &searches,
            RawVectorMetric::L2,
            &scoring_plan,
            &mut top_k,
        )
        .unwrap();
        let results = top_k
            .into_iter()
            .map(RawScoreTopK::into_search_result)
            .collect::<Vec<_>>();

        assert_eq!(results[0].row_ids, vec![10]);
        assert_eq!(results[1].row_ids, vec![11]);
    }

    #[test]
    fn test_collect_raw_batch_vector_batch_scores_only_include_row_ids() {
        let element_field = Arc::new(ArrowField::new("element", ArrowDataType::Float32, true));
        let mut builder =
            FixedSizeListBuilder::new(Float32Builder::new(), 2).with_field(element_field);
        for vector in [[1.0, 0.0], [0.0, 1.0], [0.8, 0.2]] {
            builder.values().append_value(vector[0]);
            builder.values().append_value(vector[1]);
            builder.append(true);
        }
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new(
                "embedding",
                ArrowDataType::FixedSizeList(
                    Arc::new(ArrowField::new("element", ArrowDataType::Float32, true)),
                    2,
                ),
                true,
            ),
            ArrowField::new(ROW_ID_FIELD_NAME, ArrowDataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(builder.finish()) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(10), Some(11), Some(12)])) as ArrayRef,
            ],
        )
        .unwrap();
        let mut include_row_ids = RoaringTreemap::new();
        include_row_ids.insert(12);
        let searches = vec![
            VectorSearch::new(vec![1.0, 0.0], 2, "embedding".to_string())
                .unwrap()
                .with_include_row_ids(include_row_ids),
        ];
        let scoring_plan = RawScoringPlan::new(&searches, RawVectorMetric::L2);
        let mut top_k = searches
            .iter()
            .map(|search| RawScoreTopK::new(search.limit))
            .collect::<Vec<_>>();

        collect_raw_batch_vector_batch(
            &batch,
            &searches,
            RawVectorMetric::L2,
            &scoring_plan,
            &mut top_k,
        )
        .unwrap();
        let results = top_k
            .into_iter()
            .map(RawScoreTopK::into_search_result)
            .collect::<Vec<_>>();

        assert_eq!(results[0].row_ids, vec![12]);
        assert_eq!(results[0].scores.len(), 1);
    }

    #[tokio::test]
    async fn test_batch_vector_search_requires_vectors() {
        let table = vector_test_table();
        let err = table
            .new_batch_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vectors(Vec::new())
            .with_limit(1)
            .execute()
            .await
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("Query vectors must be set via with_query_vectors()"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_batch_vector_search_rejects_zero_limit() {
        let table = vector_test_table();
        let err = table
            .new_batch_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vectors(vec![vec![1.0]])
            .with_limit(0)
            .execute()
            .await
            .unwrap_err();

        assert!(
            err.to_string().contains("Limit must be between 1"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_batch_evaluate_no_matching_field_returns_empty_per_query() {
        let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        let fields = vec![make_field(1, "id")];
        let searches = vec![
            VectorSearch::new(vec![1.0], 10, "embedding".to_string()).unwrap(),
            VectorSearch::new(vec![0.0], 10, "embedding".to_string()).unwrap(),
        ];
        let options = HashMap::new();

        let entry = make_lumina_entry(
            "test.idx",
            LEGACY_LUMINA_VECTOR_ANN_IDENTIFIER,
            FileKind::Add,
            99,
        );

        let results = evaluate_batch_vector_search(
            eval_context(&file_io, &options, &fields, None),
            &[entry],
            &searches,
        )
        .await
        .unwrap();

        assert_eq!(results.len(), searches.len());
        assert!(results.iter().all(SearchResult::is_empty));
    }

    #[tokio::test]
    async fn test_evaluate_no_matching_entries() {
        let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        let fields = vec![make_field(1, "id"), make_field(2, "embedding")];
        let vs = VectorSearch::new(vec![1.0, 2.0], 10, "embedding".to_string()).unwrap();
        let options = HashMap::new();

        let entry = IndexManifestEntry {
            kind: FileKind::Add,
            partition: vec![],
            bucket: 0,
            index_file: IndexFileMeta {
                index_type: "btree".to_string(),
                file_name: "test.idx".to_string(),
                file_size: 100,
                row_count: 10,
                deletion_vectors_ranges: None,
                global_index_meta: None,
            },
            version: 1,
        };

        let result = evaluate_vector_search(
            eval_context(&file_io, &options, &fields, None),
            &[entry],
            &vs,
        )
        .await
        .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_evaluate_ignores_non_vector_index_type() {
        let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        let fields = vec![make_field(2, "embedding")];
        let vs = VectorSearch::new(vec![1.0], 10, "embedding".to_string()).unwrap();
        let options = HashMap::new();

        let entry = make_lumina_entry("test.idx", "btree", FileKind::Add, 2);

        let result = evaluate_vector_search(
            eval_context(&file_io, &options, &fields, None),
            &[entry],
            &vs,
        )
        .await
        .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_evaluate_full_mode_without_vector_entries_uses_raw_path() {
        let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        let fields = vec![make_field(2, "embedding")];
        let vs = VectorSearch::new(vec![1.0], 10, "embedding".to_string()).unwrap();
        let options = HashMap::from([("global-index.search-mode".to_string(), "full".to_string())]);

        let err = evaluate_vector_search(
            eval_context(&file_io, &options, &fields, Some(10)),
            &[],
            &vs,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("Vector raw search requires table context"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_evaluate_no_matching_field() {
        let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        let fields = vec![make_field(1, "id")];
        let vs = VectorSearch::new(vec![1.0], 10, "embedding".to_string()).unwrap();
        let options = HashMap::new();

        let entry = make_lumina_entry(
            "test.idx",
            LEGACY_LUMINA_VECTOR_ANN_IDENTIFIER,
            FileKind::Add,
            99,
        );

        let result = evaluate_vector_search(
            eval_context(&file_io, &options, &fields, None),
            &[entry],
            &vs,
        )
        .await
        .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_evaluate_skips_delete_entries() {
        let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        let fields = vec![make_field(2, "embedding")];
        let vs = VectorSearch::new(vec![1.0], 10, "embedding".to_string()).unwrap();
        let options = HashMap::new();

        let entry = make_lumina_entry(
            "test.idx",
            LEGACY_LUMINA_VECTOR_ANN_IDENTIFIER,
            FileKind::Delete,
            2,
        );

        let result = evaluate_vector_search(
            eval_context(&file_io, &options, &fields, None),
            &[entry],
            &vs,
        )
        .await
        .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_evaluate_accepts_canonical_lumina_index_type() {
        let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        let fields = vec![make_field(2, "embedding")];
        let vs = VectorSearch::new(vec![1.0], 10, "embedding".to_string()).unwrap();
        let options = HashMap::new();

        let entry = make_lumina_entry("missing.idx", LUMINA_IDENTIFIER, FileKind::Add, 2);

        let err = evaluate_vector_search(
            eval_context(&file_io, &options, &fields, None),
            &[entry],
            &vs,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("Failed to read Lumina index file 'missing.idx'"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_evaluate_accepts_legacy_lumina_index_type() {
        let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        let fields = vec![make_field(2, "embedding")];
        let vs = VectorSearch::new(vec![1.0], 10, "embedding".to_string()).unwrap();
        let options = HashMap::new();

        let entry = make_lumina_entry(
            "missing.idx",
            LEGACY_LUMINA_VECTOR_ANN_IDENTIFIER,
            FileKind::Add,
            2,
        );

        let err = evaluate_vector_search(
            eval_context(&file_io, &options, &fields, None),
            &[entry],
            &vs,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("Failed to read Lumina index file 'missing.idx'"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_evaluate_accepts_vindex_index_type() {
        let file_io = crate::io::FileIOBuilder::new("memory").build().unwrap();
        let fields = vec![make_field(2, "embedding")];
        let vs = VectorSearch::new(vec![1.0], 10, "embedding".to_string()).unwrap();
        let options = HashMap::new();

        let entry = make_lumina_entry("missing.idx", IVF_FLAT_IDENTIFIER, FileKind::Add, 2);

        let err = evaluate_vector_search(
            eval_context(&file_io, &options, &fields, None),
            &[entry],
            &vs,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("Failed to read vindex index file 'missing.idx'"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_execute_fails_closed_when_query_auth_enabled() {
        let table = crate::table::query_auth_table();
        let err = table
            .new_vector_search_builder()
            .execute()
            .await
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::Unsupported { ref message } if message.contains("query-auth.enabled")),
            "vector search must fail closed for a query-auth table"
        );
    }

    #[tokio::test]
    async fn test_batch_execute_fails_closed_when_query_auth_enabled() {
        // The batch scored entry returns data-derived row ids/scores outside
        // `TableScan`/`TableRead`, so it must fail closed under
        // `query-auth.enabled` exactly like the single-query builder. Its config
        // is otherwise valid, so without the guard the empty-snapshot fast path
        // would return empty results and silently bypass authorization.
        let table = crate::table::query_auth_table();
        let err = table
            .new_batch_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vectors(vec![vec![1.0, 2.0]])
            .with_limit(5)
            .execute()
            .await
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::Unsupported { ref message } if message.contains("query-auth.enabled")),
            "batch vector search must fail closed for a query-auth table, got: {err:?}"
        );
    }

    fn pk_data_file(name: &str, row_count: i64, first_row_id: Option<i64>) -> DataFileMeta {
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
            first_row_id,
            write_cols: None,
        }
    }

    fn pk_search_split(bucket: i32, files: Vec<DataFileMeta>) -> PkVectorSearchSplit {
        PkVectorSearchSplit {
            data_split: DataSplitBuilder::new()
                .with_snapshot(1)
                .with_partition(BinaryRow::new(0))
                .with_bucket(bucket)
                .with_bucket_path(format!("memory:/t/bucket-{bucket}"))
                .with_total_buckets(1)
                .with_data_files(files)
                .build()
                .unwrap(),
            ann_segments: Vec::new(),
            active_files: Vec::new(),
        }
    }

    fn pk_candidate(
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

    // Candidate with a fixed empty (arity-0) partition and bucket 0, keyed only by
    // (split_index, file, position) — the dimensions the rerank core groups on.
    fn cand_at(split_index: usize, file: &str, pos: i64, dist: f32) -> PkVectorCandidate {
        pk_candidate(split_index, 0, file, pos, dist)
    }

    /// The single data-file name every rerank fixture writes.
    const RERANK_FILE: &str = "part-0.parquet";

    /// Serialize a Paimon deletion-vector blob covering `deleted_rows` and write it
    /// at `path`, returning the matching `DeletionFile`. Byte layout mirrors the
    /// position-read tests: `[length][magic][roaring bitmap][0]`.
    async fn write_deletion_blob(
        file_io: &FileIO,
        path: &str,
        deleted_rows: &[u32],
    ) -> crate::table::source::DeletionFile {
        use roaring::RoaringBitmap;

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
            .write(bytes::Bytes::from(blob))
            .await
            .unwrap();
        crate::table::source::DeletionFile::new(
            path.to_string(),
            0,
            bitmap_length as i64,
            Some(deleted_rows.len() as i64),
        )
    }

    /// Write a single-file vector data file (`FixedSizeList<Float32>` of width
    /// `dim`) holding `rows` (a `None` entry is a NULL vector row) as Parquet, and
    /// return a vector-only `DataFileReader`, the enclosing `PkVectorSearchSplit`,
    /// and the vector `DataField`. When `deleted_rows` is non-empty a deletion
    /// vector covering those physical positions is attached to the split, so the
    /// position read drops them exactly as `PkVectorIndexedSplitRead::read` does.
    ///
    /// This is the position-only analogue of the old `ArrayReader`: rerank now
    /// re-reads real stored rows through `PkVectorPositionRead`, so the fixtures
    /// exercise that path rather than an in-memory preloaded column.
    async fn vector_rerank_fixture(
        table_path: &str,
        dim: u32,
        rows: &[Option<Vec<f32>>],
        deleted_rows: &[u32],
    ) -> (DataFileReader, PkVectorSearchSplit, DataField) {
        use crate::arrow::build_target_arrow_schema;
        use crate::arrow::format::{FormatFileWriter, ParquetFormatWriter};
        use crate::spec::VectorType;
        use crate::table::schema_manager::SchemaManager;

        let vector_type =
            VectorType::try_new(true, dim, DataType::Float(FloatType::new())).unwrap();
        let vector_field =
            DataField::new(0, "embedding".to_string(), DataType::Vector(vector_type));
        let read_fields = vec![vector_field.clone()];
        let arrow_schema = build_target_arrow_schema(&read_fields).unwrap();

        let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), dim as i32).with_field(
            Arc::new(ArrowField::new("element", ArrowDataType::Float32, true)),
        );
        for row in rows {
            match row {
                Some(values) => {
                    for v in values {
                        builder.values().append_value(*v);
                    }
                    builder.append(true);
                }
                None => {
                    for _ in 0..dim {
                        builder.values().append_value(0.0);
                    }
                    builder.append(false);
                }
            }
        }
        let vec_array = builder.finish();
        let batch =
            arrow_array::RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(vec_array)])
                .unwrap();

        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let bucket_path = format!("{table_path}/bucket-0");
        let output = file_io
            .new_output(&format!("{bucket_path}/{RERANK_FILE}"))
            .unwrap();
        let mut writer: Box<dyn FormatFileWriter> = Box::new(
            ParquetFormatWriter::new(
                &output,
                arrow_schema.clone(),
                "zstd",
                1,
                None,
                &HashMap::new(),
            )
            .await
            .unwrap(),
        );
        writer.write(&batch).await.unwrap();
        let file_size = writer.close().await.unwrap().file_size;

        let schema_id = 1;
        let file_meta = pk_data_file(RERANK_FILE, rows.len() as i64, Some(0));
        let file_meta = DataFileMeta {
            file_size: file_size as i64,
            schema_id,
            ..file_meta
        };

        let mut split_builder = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(bucket_path)
            .with_total_buckets(1)
            .with_data_files(vec![file_meta]);
        if !deleted_rows.is_empty() {
            let df =
                write_deletion_blob(&file_io, &format!("{table_path}/index/dv-0"), deleted_rows)
                    .await;
            split_builder = split_builder.with_data_deletion_files(vec![Some(df)]);
        }
        let data_split = split_builder.build().unwrap();
        let split = PkVectorSearchSplit {
            data_split,
            ann_segments: Vec::new(),
            active_files: Vec::new(),
        };

        let schema_manager = SchemaManager::new(file_io.clone(), table_path.to_string());
        let reader = DataFileReader::new(
            file_io,
            schema_manager,
            schema_id,
            read_fields.clone(),
            read_fields,
            Vec::new(),
        );
        (reader, split, vector_field)
    }

    #[tokio::test]
    async fn rerank_aligns_recomputed_distance_by_position_column() {
        use crate::arrow::build_target_arrow_schema;
        use crate::arrow::format::{FormatFileWriter, ParquetFormatWriter};
        use crate::spec::VectorType;
        use crate::table::schema_manager::SchemaManager;

        // A vector data file with 4 physical rows: positions 0,1,3 hold vectors
        // and position 2 (a NON-candidate) holds a NULL vector. Candidates sit at
        // non-contiguous positions {1, 3}. The ANN-reported distances are
        // deliberately reversed relative to the true stored vectors; after rerank
        // each candidate must carry compute_distance(query, vec_at_its_position),
        // proving alignment is by the _PKEY_VECTOR_POSITION column value, not batch
        // order. Position 2's NULL is never read (it is not a candidate), so it
        // cannot trip the null-vector guard.
        let vector_type = VectorType::try_new(true, 2, DataType::Float(FloatType::new())).unwrap();
        let vector_field =
            DataField::new(0, "embedding".to_string(), DataType::Vector(vector_type));
        let read_fields = vec![vector_field.clone()];
        let arrow_schema = build_target_arrow_schema(&read_fields).unwrap();

        // pos0=[7,0], pos1=[1,0], pos2=NULL, pos3=[4,0].
        let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), 2).with_field(Arc::new(
            ArrowField::new("element", ArrowDataType::Float32, true),
        ));
        for row in [
            Some([7.0f32, 0.0]),
            Some([1.0, 0.0]),
            None,
            Some([4.0, 0.0]),
        ] {
            match row {
                Some([a, b]) => {
                    builder.values().append_value(a);
                    builder.values().append_value(b);
                    builder.append(true);
                }
                None => {
                    builder.values().append_value(0.0);
                    builder.values().append_value(0.0);
                    builder.append(false);
                }
            }
        }
        let vec_array = builder.finish();
        let batch =
            arrow_array::RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(vec_array)])
                .unwrap();

        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/rerank_positional";
        let bucket_path = format!("{table_path}/bucket-0");
        let file_name = "part-0.parquet";
        let output = file_io
            .new_output(&format!("{bucket_path}/{file_name}"))
            .unwrap();
        let mut writer: Box<dyn FormatFileWriter> = Box::new(
            ParquetFormatWriter::new(
                &output,
                arrow_schema.clone(),
                "zstd",
                1,
                None,
                &HashMap::new(),
            )
            .await
            .unwrap(),
        );
        writer.write(&batch).await.unwrap();
        let file_size = writer.close().await.unwrap().file_size;

        let schema_id = 1;
        let file_meta = pk_data_file(file_name, 4, Some(0));
        let file_meta = DataFileMeta {
            file_size: file_size as i64,
            schema_id,
            ..file_meta
        };
        let data_split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(bucket_path)
            .with_total_buckets(1)
            .with_data_files(vec![file_meta])
            .build()
            .unwrap();
        let split = PkVectorSearchSplit {
            data_split,
            ann_segments: Vec::new(),
            active_files: Vec::new(),
        };

        let schema_manager = SchemaManager::new(file_io.clone(), table_path.to_string());
        let reader = DataFileReader::new(
            file_io,
            schema_manager,
            schema_id,
            read_fields.clone(),
            read_fields.clone(),
            Vec::new(),
        );

        let query = vec![1.0f32, 0.0];
        // ANN-reported distances reversed vs. truth: pos1 reported worse (0.9) than
        // pos3 (0.1), but the true L2 distances are pos1=0 and pos3=9.
        let indexed = vec![cand_at(0, file_name, 1, 0.9), cand_at(0, file_name, 3, 0.1)];

        let out = rerank_indexed_positional(
            &reader,
            indexed,
            &[split],
            &query,
            VectorSearchMetric::L2,
            2,
            &vector_field,
        )
        .await
        .unwrap();

        // Best-first after exact recompute: pos1 (d=0) then pos3 (d=9), each
        // carrying the distance computed from its OWN position's stored vector.
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].row_position, 1);
        assert_eq!(out[0].distance, 0.0);
        assert_eq!(out[1].row_position, 3);
        assert_eq!(out[1].distance, 9.0);
    }

    #[tokio::test]
    async fn rerank_recomputes_distance_and_reorders() {
        // pos0=[9,0], pos1=[1,0]; query=[1,0]. The ANN-reported distances are
        // reversed relative to the truth (pos0 reported best at 0.1, pos1 worst at
        // 0.9), so an implementation that trusted the ANN order would emit pos0
        // first. Exact L2 recompute yields pos0=64, pos1=0, so the output must
        // reorder to pos1-then-pos0 with the recomputed distances.
        let (reader, split, vector_field) = vector_rerank_fixture(
            "memory:/rerank_reorder",
            2,
            &[Some(vec![9.0, 0.0]), Some(vec![1.0, 0.0])],
            &[],
        )
        .await;
        let query = vec![1.0f32, 0.0];
        let indexed = vec![
            cand_at(0, RERANK_FILE, 0, 0.1),
            cand_at(0, RERANK_FILE, 1, 0.9),
        ];

        let out = rerank_indexed_positional(
            &reader,
            indexed,
            &[split],
            &query,
            VectorSearchMetric::L2,
            2,
            &vector_field,
        )
        .await
        .unwrap();

        assert_eq!(out.len(), 2);
        assert_eq!(out[0].row_position, 1);
        assert_eq!(out[0].distance, 0.0);
        assert_eq!(out[1].row_position, 0);
        assert_eq!(out[1].distance, 64.0);
        // Order genuinely changed vs. the ANN-reported best-first (which was pos0).
        assert!(out[0].distance < out[1].distance);
    }

    #[tokio::test]
    async fn rerank_is_independent_of_fast_mode_reranks_indexed() {
        // The rerank core takes only the indexed (fast-path) candidates and always
        // recomputes their true distance; there is no fast/exact switch that can
        // skip it. The single candidate carries a bogus ANN distance (0.42) but its
        // stored vector equals the query, so the recomputed L2 distance is exactly
        // 0.0 — proving the indexed candidate WAS reranked rather than passed
        // through with its ANN distance.
        let (reader, split, vector_field) =
            vector_rerank_fixture("memory:/rerank_indexed", 2, &[Some(vec![1.0, 0.0])], &[]).await;
        let query = vec![1.0f32, 0.0];
        let indexed = vec![cand_at(0, RERANK_FILE, 0, 0.42)];

        let out = rerank_indexed_positional(
            &reader,
            indexed,
            &[split],
            &query,
            VectorSearchMetric::L2,
            1,
            &vector_field,
        )
        .await
        .unwrap();

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].row_position, 0);
        assert_ne!(out[0].distance, 0.42);
        assert_eq!(out[0].distance, 0.0);
    }

    #[tokio::test]
    async fn rerank_fails_loud_on_null_vector() {
        // A NULL vector stored AT a candidate position must fail loud rather than
        // silently scoring it: the candidate genuinely has no vector to rerank on.
        let (reader, split, vector_field) =
            vector_rerank_fixture("memory:/rerank_null", 2, &[None], &[]).await;
        let query = vec![1.0f32, 0.0];
        let indexed = vec![cand_at(0, RERANK_FILE, 0, 0.1)];

        let err = rerank_indexed_positional(
            &reader,
            indexed,
            &[split],
            &query,
            VectorSearchMetric::L2,
            1,
            &vector_field,
        )
        .await
        .err()
        .expect("null vector at a candidate position must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. } if message.contains("null vector")),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn rerank_fails_loud_on_leftover_candidate() {
        // pos1 is deleted by the deletion vector, so the position read returns no
        // row for it. The search path already DV-filters, so a deleted candidate
        // reaching rerank is a real inconsistency: the leftover guard must fail
        // loud rather than silently dropping the candidate.
        let (reader, split, vector_field) = vector_rerank_fixture(
            "memory:/rerank_leftover",
            2,
            &[Some(vec![1.0, 0.0]), Some(vec![2.0, 0.0])],
            &[1],
        )
        .await;
        let query = vec![1.0f32, 0.0];
        let indexed = vec![
            cand_at(0, RERANK_FILE, 0, 0.1),
            cand_at(0, RERANK_FILE, 1, 0.9),
        ];

        let err = rerank_indexed_positional(
            &reader,
            indexed,
            &[split],
            &query,
            VectorSearchMetric::L2,
            2,
            &vector_field,
        )
        .await
        .err()
        .expect("a candidate returning no row must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. } if message.contains("failed to read")),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn rerank_fails_loud_on_dimension_mismatch() {
        // Stored vectors are 3-dimensional but the query is 2-dimensional. The
        // vector extraction validates each stored row against the query dimension
        // and fails loud, so the recompute never runs against mismatched vectors.
        let (reader, split, vector_field) =
            vector_rerank_fixture("memory:/rerank_dim", 3, &[Some(vec![1.0, 0.0, 0.0])], &[]).await;
        let query = vec![1.0f32, 0.0];
        let indexed = vec![cand_at(0, RERANK_FILE, 0, 0.1)];

        let err = rerank_indexed_positional(
            &reader,
            indexed,
            &[split],
            &query,
            VectorSearchMetric::L2,
            1,
            &vector_field,
        )
        .await
        .err()
        .expect("dimension mismatch must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. } if message.contains("dimension")),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn rerank_fails_loud_on_duplicate_candidate_position() {
        // Two candidates addressing the same (split_index, file, position) is a
        // programming error upstream: the dedup guard fires before any read.
        let (reader, split, vector_field) =
            vector_rerank_fixture("memory:/rerank_dup", 2, &[Some(vec![1.0, 0.0])], &[]).await;
        let query = vec![1.0f32, 0.0];
        let indexed = vec![
            cand_at(0, RERANK_FILE, 0, 0.1),
            cand_at(0, RERANK_FILE, 0, 0.9),
        ];

        let err = rerank_indexed_positional(
            &reader,
            indexed,
            &[split],
            &query,
            VectorSearchMetric::L2,
            2,
            &vector_field,
        )
        .await
        .err()
        .expect("duplicate candidate position must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. } if message.contains("duplicate")),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn rerank_fails_loud_on_unexpected_position() {
        // Every position the read surfaces must resolve to a candidate keyed by
        // (split_index, file, position). Here the plan carries two splits for the
        // SAME (partition, bucket, file), so `split_index_of` resolves the file to
        // the LAST plan index (1). The single candidate is tagged with split_index
        // 0, so its by_key entry is (0, file, 0) while the read looks up
        // (1, file, 0). The lookup misses and the unexpected-position guard fires
        // rather than silently dropping the surfaced row.
        let (reader, split, vector_field) =
            vector_rerank_fixture("memory:/rerank_unexpected", 2, &[Some(vec![1.0, 0.0])], &[])
                .await;
        let query = vec![1.0f32, 0.0];
        let indexed = vec![cand_at(0, RERANK_FILE, 0, 0.1)];

        // Two plan entries for the same file: split_index_of ends up mapping the
        // file to plan index 1, not the candidate's split_index 0.
        let dup = PkVectorSearchSplit {
            data_split: split.data_split.clone(),
            ann_segments: Vec::new(),
            active_files: Vec::new(),
        };
        let plan = vec![dup, split];

        let err = rerank_indexed_positional(
            &reader,
            indexed,
            &plan,
            &query,
            VectorSearchMetric::L2,
            1,
            &vector_field,
        )
        .await
        .err()
        .expect("a read position absent from the candidate map must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. } if message.contains("unexpected position")),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn rerank_fails_loud_on_file_not_in_plan() {
        // A candidate references a (partition, bucket, file) that is absent from
        // plan_splits. build_indexed_splits groups it into an indexed split, but the
        // split_index_of lookup — built only from plan_splits — has no entry, so the
        // kernel fails loud rather than reading an unplanned file.
        let (reader, _split, vector_field) =
            vector_rerank_fixture("memory:/rerank_noplan", 2, &[Some(vec![1.0, 0.0])], &[]).await;
        let query = vec![1.0f32, 0.0];
        let indexed = vec![cand_at(0, RERANK_FILE, 0, 0.1)];

        // Empty plan: the candidate's file resolves in no plan split.
        let err = rerank_indexed_positional(
            &reader,
            indexed,
            &[],
            &query,
            VectorSearchMetric::L2,
            1,
            &vector_field,
        )
        .await
        .err()
        .expect("a candidate file absent from the plan must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. } if message.contains("not found in plan")),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn rerank_reads_only_candidate_positions_not_whole_column() {
        // A 6-row file where every NON-candidate position (0, 2, 4, 5) holds a NULL
        // vector "poison" and only the two candidate positions (1, 3) hold real
        // vectors. The rerank read is told to fetch only positions {1, 3}; every
        // row it surfaces is looked up in the candidate map, and any position not in
        // the map trips the "unexpected position" guard (a surfaced NULL row would
        // additionally trip the null-vector guard). So if the read had surfaced any
        // of the poison rows, rerank would fail. It succeeds and returns exactly the
        // two candidates at positions {1, 3}, which proves the position selection
        // reaching the read contained only the candidate positions (not the whole
        // column).
        let rows = &[
            None,                 // pos0 poison (non-candidate)
            Some(vec![1.0, 0.0]), // pos1 candidate
            None,                 // pos2 poison (non-candidate)
            Some(vec![3.0, 0.0]), // pos3 candidate
            None,                 // pos4 poison (non-candidate)
            None,                 // pos5 poison (non-candidate)
        ];
        let (reader, split, vector_field) =
            vector_rerank_fixture("memory:/rerank_spy", 2, rows, &[]).await;
        let query = vec![1.0f32, 0.0];
        let indexed = vec![
            cand_at(0, RERANK_FILE, 1, 0.9),
            cand_at(0, RERANK_FILE, 3, 0.1),
        ];

        let out = rerank_indexed_positional(
            &reader,
            indexed,
            &[split],
            &query,
            VectorSearchMetric::L2,
            2,
            &vector_field,
        )
        .await
        .unwrap_or_else(|e| {
            panic!("only candidate positions are read, so the poison NULLs never decode: {e:?}")
        });

        assert_eq!(out.len(), 2, "exactly the candidate count of rows was read");
        let mut positions: Vec<i64> = out.iter().map(|c| c.row_position).collect();
        positions.sort_unstable();
        assert_eq!(
            positions,
            vec![1, 3],
            "only candidate positions reached the read"
        );
        // Recomputed distances confirm each surviving row is its own candidate's vector.
        assert_eq!(out[0].row_position, 1);
        assert_eq!(out[0].distance, 0.0);
        assert_eq!(out[1].row_position, 3);
        assert_eq!(out[1].distance, 4.0);
    }

    /// Build a real vindex IVF-flat segment trained with `metric`, returning the
    /// serialized bytes. `nlist = 1` keeps training trivial and deterministic; the
    /// only thing the metric check cares about is the persisted metadata metric.
    fn build_vindex_segment_bytes(metric: &str) -> Vec<u8> {
        use paimon_vindex_core::index::{VectorIndexConfig, VectorIndexTrainer, VectorIndexWriter};
        use paimon_vindex_core::io::PosWriter;

        const DIM: usize = 2;
        let vectors: Vec<f32> = vec![1.0, 0.0, 0.0, 1.0, 1.0, 1.0];
        let n = vectors.len() / DIM;
        let ids: Vec<i64> = (0..n as i64).collect();
        let options = HashMap::from([
            ("index.type".to_string(), "ivf_flat".to_string()),
            ("dimension".to_string(), DIM.to_string()),
            ("nlist".to_string(), "1".to_string()),
            ("metric".to_string(), metric.to_string()),
        ]);
        let config = VectorIndexConfig::from_options(&options).unwrap();
        let training = VectorIndexTrainer::train(config, &vectors, n).unwrap();
        let mut writer = VectorIndexWriter::new(training);
        writer.add_vectors(&ids, &vectors, n).unwrap();
        let mut bytes = Vec::new();
        {
            let mut output = PosWriter::new(&mut bytes);
            writer.write(&mut output).unwrap();
        }
        bytes
    }

    /// A `PkVectorSearchSplit` carrying a single ANN segment addressed by `path`.
    fn pk_split_with_segment(path: &str) -> PkVectorSearchSplit {
        let mut split = pk_search_split(0, vec![pk_data_file("file-a", 3, Some(0))]);
        let source_meta = crate::spec::PkVectorSourceMeta::new(
            1,
            vec![crate::spec::PkVectorSourceFile::new("file-a".to_string(), 3).unwrap()],
        )
        .unwrap();
        let mut segment = BucketAnnSegment::for_test(source_meta);
        segment.path = path.to_string();
        split.ann_segments = vec![segment];
        split
    }

    fn pk_split_with_lumina_segment(path: &str, metric: &str) -> PkVectorSearchSplit {
        let mut split = pk_search_split(0, vec![pk_data_file("file-a", 3, Some(0))]);
        let source_meta = crate::spec::PkVectorSourceMeta::new(
            1,
            vec![crate::spec::PkVectorSourceFile::new("file-a".to_string(), 3).unwrap()],
        )
        .unwrap();
        let mut segment = BucketAnnSegment::for_test(source_meta);
        segment.path = path.to_string();
        // Lumina stores its metric in the serialized index metadata blob, not in
        // the segment file bytes. `deserialize` requires both keys present.
        let meta = crate::lumina::LuminaIndexMeta::new(HashMap::from([
            ("index.dimension".to_string(), "2".to_string()),
            ("distance.metric".to_string(), metric.to_string()),
        ]));
        segment.index_meta = meta.serialize().unwrap();
        split.ann_segments = vec![segment];
        split
    }

    #[test]
    fn verify_pk_vector_segment_metrics_accepts_matching_lumina_metric() {
        // Lumina segment metadata says cosine; configured cosine => Ok. No segment
        // file bytes are needed on the Lumina path.
        let splits = vec![pk_split_with_lumina_segment("seg-lumina", "cosine")];
        let segment_bytes = HashMap::new();
        verify_pk_vector_segment_metrics(
            &splits,
            &segment_bytes,
            VectorSearchMetric::Cosine,
            VectorIndexBackend::Lumina,
        )
        .expect("matching lumina metric must pass");
    }

    #[test]
    fn verify_pk_vector_segment_metrics_rejects_mismatched_lumina_metric() {
        // Lumina segment metadata says l2; configured inner_product => fail loud,
        // naming both metrics.
        let splits = vec![pk_split_with_lumina_segment("seg-lumina", "l2")];
        let segment_bytes = HashMap::new();
        let err = verify_pk_vector_segment_metrics(
            &splits,
            &segment_bytes,
            VectorSearchMetric::InnerProduct,
            VectorIndexBackend::Lumina,
        )
        .expect_err("mismatched lumina metric must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("does not match configured metric")
                    && message.contains("l2")
                    && message.contains("inner_product")),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn from_index_type_classifies_lumina_and_vindex() {
        assert_eq!(
            VectorIndexBackend::from_index_type("lumina"),
            Some(VectorIndexBackend::Lumina)
        );
        assert_eq!(
            VectorIndexBackend::from_index_type("lumina-vector-ann"),
            Some(VectorIndexBackend::Lumina)
        );
        assert_eq!(
            VectorIndexBackend::from_index_type("ivf-flat"),
            Some(VectorIndexBackend::Vindex)
        );
        // `diskann` is Lumina's internal index type, not a top-level index type.
        assert_eq!(VectorIndexBackend::from_index_type("diskann"), None);
    }

    #[test]
    fn verify_pk_vector_segment_metrics_accepts_matching_metric() {
        // Real IVF segment trained with L2; configured metric L2 => Ok.
        let bytes = build_vindex_segment_bytes("l2");
        let splits = vec![pk_split_with_segment("seg-l2")];
        let segment_bytes = HashMap::from([("seg-l2".to_string(), bytes)]);
        verify_pk_vector_segment_metrics(
            &splits,
            &segment_bytes,
            VectorSearchMetric::L2,
            VectorIndexBackend::Vindex,
        )
        .expect("matching metric must pass");
    }

    #[test]
    fn verify_pk_vector_segment_metrics_rejects_mismatched_metric() {
        // Real IVF segment trained with L2; configured metric Cosine => fail loud.
        let bytes = build_vindex_segment_bytes("l2");
        let splits = vec![pk_split_with_segment("seg-l2")];
        let segment_bytes = HashMap::from([("seg-l2".to_string(), bytes)]);
        let err = verify_pk_vector_segment_metrics(
            &splits,
            &segment_bytes,
            VectorSearchMetric::Cosine,
            VectorIndexBackend::Vindex,
        )
        .expect_err("mismatched metric must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("does not match configured metric")
                    && message.contains("l2")
                    && message.contains("cosine")),
            "unexpected error: {err:?}"
        );
    }

    fn pk_vector_table(options: &[(&str, &str)]) -> Table {
        let mut builder = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column(
                "embedding",
                DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
            );
        for (k, v) in options {
            builder = builder.option(*k, *v);
        }
        let schema = builder.build().unwrap();
        Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("default", "pk_vector_test"),
            "memory:/pk_vector_test".to_string(),
            TableSchema::new(0, &schema),
            None,
        )
    }

    /// A data-evolution (global-index) vector table with a committed IVF-flat
    /// index over the `embedding` column: row-tracking + data-evolution +
    /// global-index enabled so committed data files carry `first_row_id` and the
    /// search returns global row-ids that `execute_read` can materialize. The
    /// returned table has one committed batch of `(id, embedding)` rows and a real
    /// vindex index built end-to-end.
    async fn de_vector_table() -> Table {
        let table_path = "memory:/de_vector_search_test";
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column(
                "embedding",
                DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
            )
            .option("row-tracking.enabled", "true")
            .option("data-evolution.enabled", "true")
            .option("global-index.enabled", "true")
            .option("global-index.row-count-per-shard", "10")
            .option("ivf-flat.dimension", "2")
            .option("ivf-flat.nlist", "2")
            .build()
            .unwrap();
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table = Table::new(
            file_io.clone(),
            Identifier::new("default", "de_vector_test"),
            table_path.to_string(),
            TableSchema::new(0, &schema),
            None,
        );
        file_io
            .mkdirs(&format!("{table_path}/snapshot/"))
            .await
            .unwrap();
        file_io
            .mkdirs(&format!("{table_path}/manifest/"))
            .await
            .unwrap();

        let ids = vec![1, 2, 3];
        let vectors = vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![1.0, 1.0]];
        let element_field = Arc::new(ArrowField::new("element", ArrowDataType::Float32, true));
        let mut vector_builder =
            ListBuilder::new(Float32Builder::new()).with_field(element_field.clone());
        for vector in vectors {
            for value in vector {
                vector_builder.values().append_value(value);
            }
            vector_builder.append(true);
        }
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("embedding", ArrowDataType::List(element_field), true),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int32Array::from(ids)) as ArrayRef,
                Arc::new(vector_builder.finish()) as ArrayRef,
            ],
        )
        .unwrap();

        let mut table_write = TableWrite::new(&table, "test-user".to_string()).unwrap();
        table_write.write_arrow_batch(&batch).await.unwrap();
        let messages = table_write.prepare_commit().await.unwrap();
        TableCommit::new(table.clone(), "test-user".to_string())
            .commit(messages)
            .await
            .unwrap();

        let built = table
            .new_vindex_index_build_builder(IVF_FLAT_IDENTIFIER)
            .with_index_column("embedding")
            .execute()
            .await
            .unwrap();
        assert!(built > 0, "DE fixture must build a global vector index");
        table
    }

    #[tokio::test]
    async fn pk_branch_disabled_falls_through_to_de_path() {
        // No pk-vector.index.columns: behaves exactly as the DE path. With no
        // snapshot the DE path returns an empty result; the PK branch must not
        // intercept it.
        let table = pk_vector_table(&[]);
        let result = table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0])
            .with_limit(5)
            .execute_scored()
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn pk_branch_execute_scored_fails_loud() {
        // On a PK-vector table `execute_scored` reports global row ids, which the
        // PK path cannot produce (physical (file, position) coords, no global ids).
        // It must fail loud rather than fabricate ids; callers use `execute_read`.
        let table = pk_vector_table(&[
            ("pk-vector.index.columns", "embedding"),
            ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
            ("fields.embedding.pk-vector.distance.metric", "l2"),
        ]);
        let err = table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0])
            .with_limit(5)
            .execute_scored()
            .await
            .map(|_| ())
            .expect_err("execute_scored on a PK-vector column must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("does not produce global row ids")),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn pk_branch_other_column_falls_through_to_de_path() {
        // pk-vector index configured for "embedding", but the query targets a
        // different column -> the PK branch must not intercept; DE path (no
        // snapshot) yields empty. Discriminator: the PK column carries a
        // DELIBERATELY INVALID distance metric, which the PK branch parses eagerly
        // (`VectorSearchMetric::parse`) and would fail on. So a regression that
        // dropped the `pk_col == vector_column` guard and ran the PK branch for
        // "other" would surface as Err here, not Ok(empty) -- the assertion
        // therefore proves the DE path ran, not merely that the result is empty.
        let table = pk_vector_table(&[
            ("pk-vector.index.columns", "embedding"),
            ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
            (
                "fields.embedding.pk-vector.distance.metric",
                "not-a-real-metric",
            ),
        ]);
        let result = table
            .new_vector_search_builder()
            .with_vector_column("other")
            .with_query_vector(vec![1.0])
            .with_limit(5)
            .execute_scored()
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn pk_branch_multi_column_config_does_not_break_unrelated_de_query() {
        // A malformed multi-column PK-vector config ("a,b") must not abort an
        // unrelated DE vector query. The query targets a column NOT among the
        // configured PK-vector columns, so membership resolution short-circuits
        // before the exactly-one-column rule fires -- the query falls through to
        // the DE path (no snapshot -> empty) instead of surfacing the "must name
        // exactly one column" error.
        let table = pk_vector_table(&[
            ("pk-vector.index.columns", "a,b"),
            ("fields.a.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
            ("fields.a.pk-vector.distance.metric", "l2"),
        ]);
        let result = table
            .new_vector_search_builder()
            .with_vector_column("other")
            .with_query_vector(vec![1.0])
            .with_limit(5)
            .execute_scored()
            .await;
        match result {
            Ok(search) => assert!(search.is_empty()),
            Err(err) => panic!(
                "unrelated DE query must not error on a malformed multi-column PK config: {err}"
            ),
        }
    }

    /// `id > threshold` built against the table's user fields (leaf index resolves
    /// against `table.schema().fields()`).
    fn id_gt_filter(table: &Table, threshold: i32) -> Predicate {
        PredicateBuilder::new(table.schema().fields())
            .greater_than("id", Datum::Int(threshold))
            .unwrap()
    }

    #[tokio::test]
    async fn execute_read_filter_without_deletion_vectors_fails_loud() {
        let table = pk_vector_table(&[
            ("pk-vector.index.columns", "embedding"),
            ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
            ("fields.embedding.pk-vector.distance.metric", "l2"),
        ]);
        let filter = id_gt_filter(&table, 2);
        let err = table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0])
            .with_limit(5)
            .with_filter(filter)
            .execute_read()
            .await
            .map(|_| ())
            .expect_err("read filter without deletion vectors must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("deletion vectors without merge-on-read")),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn execute_scored_filter_on_non_pk_vector_path_fails_loud() {
        // No PK-vector index configured, so `execute_scored` would fall through to
        // the data-evolution path, which never consumes the filter. Silently
        // returning unfiltered rows is a wrong-read; the query must fail loud
        // instead.
        let table = pk_vector_table(&[]);
        let filter = id_gt_filter(&table, 2);
        let err = table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0])
            .with_limit(5)
            .with_filter(filter)
            .execute_scored()
            .await
            .map(|_| ())
            .expect_err("filter on the non-PK-vector path must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("only supported on the primary-key vector path")),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn execute_read_filter_with_merge_on_read_fails_loud() {
        // Deletion vectors enabled BUT merge-on-read on: still rejected, because a
        // merge-on-read scan can surface stale key versions that a physical-row
        // filter cannot reconcile.
        let table = pk_vector_table(&[
            ("pk-vector.index.columns", "embedding"),
            ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
            ("fields.embedding.pk-vector.distance.metric", "l2"),
            ("deletion-vectors.enabled", "true"),
            ("deletion-vectors.merge-on-read", "true"),
        ]);
        let filter = id_gt_filter(&table, 2);
        let err = table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0])
            .with_limit(5)
            .with_filter(filter)
            .execute_read()
            .await
            .map(|_| ())
            .expect_err("merge-on-read filter must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("deletion vectors without merge-on-read")),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn execute_read_filter_with_deletion_vectors_passes_guard() {
        // Deletion vectors enabled, merge-on-read off (default): the residual guard
        // passes. With no snapshot the plan is empty, so the (guarded) filter path
        // simply yields an empty stream rather than erroring — proving the guard
        // admits a legal filtered query.
        let table = pk_vector_table(&[
            ("pk-vector.index.columns", "embedding"),
            ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
            ("fields.embedding.pk-vector.distance.metric", "l2"),
            ("deletion-vectors.enabled", "true"),
            // Pin the index dimension so the query vector below matches it; the
            // up-front dimension guard runs before this test's residual guard.
            ("fields.embedding.dimension", "4"),
        ]);
        let filter = id_gt_filter(&table, 2);
        let mut stream = table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0; 4])
            .with_limit(5)
            .with_filter(filter)
            .execute_read()
            .await
            .expect("guarded filter query must be admitted");
        assert!(stream.try_next().await.unwrap().is_none());
    }

    fn make_lumina_entry(
        file_name: &str,
        index_type: &str,
        kind: FileKind,
        index_field_id: i32,
    ) -> IndexManifestEntry {
        IndexManifestEntry {
            kind,
            partition: vec![],
            bucket: 0,
            index_file: IndexFileMeta {
                index_type: index_type.to_string(),
                file_name: file_name.to_string(),
                file_size: 100,
                row_count: 10,
                deletion_vectors_ranges: None,
                global_index_meta: Some(GlobalIndexMeta {
                    row_range_start: 0,
                    row_range_end: 9,
                    index_field_id,
                    extra_field_ids: None,
                    source_meta: None,
                    index_meta: None,
                }),
            },
            version: 1,
        }
    }

    // ---- Task B: search-and-read (`execute_read`) tests ----

    /// Build a small materialization batch: user column `id: Int32`, the internal
    /// `_PKEY_VECTOR_POSITION: Int64`, and `__paimon_search_score: Float32` (mirroring
    /// what `PkVectorIndexedSplitRead` emits for a single file).
    fn materialized_batch(rows: &[(i32, i64, f32)]) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new(PKEY_VECTOR_POSITION_COLUMN, ArrowDataType::Int64, false),
            ArrowField::new(SEARCH_SCORE_COLUMN, ArrowDataType::Float32, false),
        ]));
        let ids = Int32Array::from(rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>());
        let positions = Int64Array::from(rows.iter().map(|(_, pos, _)| *pos).collect::<Vec<_>>());
        let scores = Float32Array::from(rows.iter().map(|(_, _, s)| *s).collect::<Vec<_>>());
        RecordBatch::try_new(
            schema,
            vec![Arc::new(ids), Arc::new(positions), Arc::new(scores)],
        )
        .unwrap()
    }

    fn i32_col(batch: &RecordBatch, name: &str) -> Vec<i32> {
        let idx = batch.schema().index_of(name).unwrap();
        batch
            .column(idx)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec()
    }
    fn f32_col(batch: &RecordBatch, name: &str) -> Vec<f32> {
        let idx = batch.schema().index_of(name).unwrap();
        batch
            .column(idx)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    #[test]
    fn reorder_and_strip_position_recovers_best_first_and_drops_position() {
        // Single file, one bucket. The materialization reader emits rows in
        // ascending physical position [pos0, pos1, pos2] -> ids [40,41,42]. The
        // search candidates ranked them best-first as pos1(rank0), pos2(rank1),
        // pos0(rank2), which is NEITHER position order nor score order-by-batch.
        // The reorder must yield ids [41,42,40] and drop _PKEY_VECTOR_POSITION.
        let batch = materialized_batch(&[
            (40, 0, l2_score(9.0)),
            (41, 1, l2_score(1.0)),
            (42, 2, l2_score(4.0)),
        ]);
        let batches = vec![batch];
        let part = BinaryRow::new(0).to_serialized_bytes();
        let mut rank_of: HashMap<(Vec<u8>, i32, String, i64), usize> = HashMap::new();
        rank_of.insert((part.clone(), 0, "o.mosaic".to_string(), 1), 0);
        rank_of.insert((part.clone(), 0, "o.mosaic".to_string(), 2), 1);
        rank_of.insert((part.clone(), 0, "o.mosaic".to_string(), 0), 2);

        let mut ranked = Vec::new();
        collect_ranked_rows(&batches[0], 0, &part, 0, "o.mosaic", &rank_of, &mut ranked).unwrap();
        let out = reorder_and_strip_position(&batches, ranked).unwrap();
        assert_eq!(out.len(), 1);
        let out = &out[0];

        // Best-first row order, not ascending position order.
        assert_eq!(i32_col(out, "id"), vec![41, 42, 40]);
        // Score column preserved and aligned to the reordered rows.
        assert_eq!(
            f32_col(out, SEARCH_SCORE_COLUMN),
            vec![l2_score(1.0), l2_score(4.0), l2_score(9.0)]
        );
        // Position column dropped; _ROW_ID never present.
        assert!(out.schema().index_of(PKEY_VECTOR_POSITION_COLUMN).is_err());
        assert!(out.schema().index_of("_ROW_ID").is_err());
    }

    #[test]
    fn reorder_and_strip_position_merges_rows_across_files() {
        // Two files (two materialization batches). Best-first interleaves them:
        // file-b pos0 (rank0), file-a pos1 (rank1), file-a pos0 (rank2). The
        // reorder must pull rows from both batches into one best-first output.
        let batch_a = materialized_batch(&[(10, 0, l2_score(9.0)), (11, 1, l2_score(1.0))]);
        let batch_b = materialized_batch(&[(20, 0, l2_score(0.5))]);
        let batches = vec![batch_a, batch_b];
        let part = BinaryRow::new(0).to_serialized_bytes();
        let mut rank_of: HashMap<(Vec<u8>, i32, String, i64), usize> = HashMap::new();
        rank_of.insert((part.clone(), 0, "b".to_string(), 0), 0);
        rank_of.insert((part.clone(), 0, "a".to_string(), 1), 1);
        rank_of.insert((part.clone(), 0, "a".to_string(), 0), 2);

        let mut ranked = Vec::new();
        collect_ranked_rows(&batches[0], 0, &part, 0, "a", &rank_of, &mut ranked).unwrap();
        collect_ranked_rows(&batches[1], 1, &part, 0, "b", &rank_of, &mut ranked).unwrap();
        let out = reorder_and_strip_position(&batches, ranked).unwrap();
        assert_eq!(i32_col(&out[0], "id"), vec![20, 11, 10]);
        assert_eq!(
            f32_col(&out[0], SEARCH_SCORE_COLUMN),
            vec![l2_score(0.5), l2_score(1.0), l2_score(9.0)]
        );
    }

    #[test]
    fn reorder_and_strip_position_empty_yields_no_batches() {
        let out = reorder_and_strip_position(&[], Vec::new()).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn collect_ranked_rows_missing_candidate_fails_loud() {
        // A materialized position with no candidate rank must fail loud rather than
        // silently drop the row.
        let batch = materialized_batch(&[(40, 7, l2_score(1.0))]);
        let part = BinaryRow::new(0).to_serialized_bytes();
        let rank_of: HashMap<(Vec<u8>, i32, String, i64), usize> = HashMap::new();
        let mut ranked = Vec::new();
        let err = collect_ranked_rows(&batch, 0, &part, 0, "f", &rank_of, &mut ranked)
            .expect_err("missing candidate must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. } if message.contains("no matching search candidate")),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn attach_scores_reorders_by_rank_not_score() {
        use arrow_array::{Int32Array, Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;

        // Two rows materialized in row-id order [10, 20]; ranks say 20 is best (rank 0),
        // 10 is rank 1. Scores tie at 0.5 to prove ordering follows rank, not score.
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(ROW_ID_FIELD_NAME, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![100, 200])),
                Arc::new(Int64Array::from(vec![10, 20])),
            ],
        )
        .unwrap();
        let mut map = HashMap::new();
        map.insert(20i64, (0usize, 0.5f32));
        map.insert(10i64, (1usize, 0.5f32));

        let out = attach_scores_by_row_id(&[batch], &map, 2).unwrap();
        assert_eq!(out.len(), 1);
        let b = &out[0];
        // _ROW_ID stripped, score appended.
        assert!(b.schema().index_of(ROW_ID_FIELD_NAME).is_err());
        let score_idx = b.schema().index_of("__paimon_search_score").unwrap();
        assert_eq!(
            b.schema().field(score_idx).data_type(),
            &arrow_schema::DataType::Float32
        );
        // Row order is rank order: id 200 (rank 0) first, then id 100 (rank 1).
        let ids = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(ids.values(), &[200, 100]);
    }

    #[test]
    fn attach_scores_fails_on_unknown_row_id() {
        use arrow_array::{Int32Array, Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(ROW_ID_FIELD_NAME, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![99])),
            ],
        )
        .unwrap();
        let map: HashMap<i64, (usize, f32)> = HashMap::new(); // no entry for 99
        let err = attach_scores_by_row_id(&[batch], &map, 1).unwrap_err();
        assert!(matches!(err, crate::Error::DataInvalid { .. }));
    }

    #[test]
    fn attach_scores_fails_on_count_mismatch() {
        use arrow_array::{Int32Array, Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(ROW_ID_FIELD_NAME, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![10])),
            ],
        )
        .unwrap();
        let mut map = HashMap::new();
        map.insert(10i64, (0usize, 0.5f32));
        // expected_len 2 but only 1 row materialized.
        let err = attach_scores_by_row_id(&[batch], &map, 2).unwrap_err();
        assert!(matches!(err, crate::Error::DataInvalid { .. }));
    }

    #[test]
    fn attach_scores_fails_on_null_row_id() {
        use arrow_array::{Int32Array, Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;
        // _ROW_ID column has a NULL at row 1; the map contains the non-null id, so
        // the failure is specifically the null (not an unknown id).
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(ROW_ID_FIELD_NAME, DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![Some(10i64), None])),
            ],
        )
        .unwrap();
        let mut map = HashMap::new();
        map.insert(10i64, (0usize, 0.5f32));
        let err = attach_scores_by_row_id(&[batch], &map, 2).unwrap_err();
        assert!(matches!(err, crate::Error::DataInvalid { .. }));
    }

    #[test]
    fn attach_scores_fails_on_wrong_type_row_id() {
        use arrow_array::{Int32Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;
        // _ROW_ID column is Int32, not Int64: the downcast fails loud.
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(ROW_ID_FIELD_NAME, DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![10])),
            ],
        )
        .unwrap();
        let mut map = HashMap::new();
        map.insert(10i64, (0usize, 0.5f32));
        let err = attach_scores_by_row_id(&[batch], &map, 1).unwrap_err();
        assert!(matches!(err, crate::Error::DataInvalid { .. }));
    }

    #[tokio::test]
    async fn execute_read_de_table_empty_snapshot_yields_empty_stream() {
        // No pk-vector index configured and no snapshot: execute_read routes to the
        // data-evolution path, whose search finds nothing and returns an empty
        // stream (not an error).
        let table = pk_vector_table(&[]);
        let mut stream = table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0])
            .with_limit(5)
            .execute_read()
            .await
            .expect("DE read over an empty table must succeed with no rows");
        let mut rows = 0usize;
        while let Some(batch) = stream.try_next().await.unwrap() {
            rows += batch.num_rows();
        }
        assert_eq!(rows, 0, "empty DE table must yield no rows");
    }

    #[tokio::test]
    async fn execute_read_unknown_column_fails_loud() {
        // pk-vector index configured for "embedding", but the query targets a
        // column that does not exist. The read path must fail loud rather than
        // fall through to the data-evolution path and return an empty stream (a
        // typo must not look like a normal empty read through the C API).
        let table = pk_vector_table(&[
            ("pk-vector.index.columns", "embedding"),
            ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
            ("fields.embedding.pk-vector.distance.metric", "l2"),
        ]);
        let err = match table
            .new_vector_search_builder()
            .with_vector_column("other")
            .with_query_vector(vec![1.0])
            .with_limit(5)
            .execute_read()
            .await
        {
            Ok(_) => panic!("unknown vector column must fail loud on execute_read"),
            Err(e) => e,
        };
        assert!(
            matches!(&err, crate::Error::DataInvalid { message, .. } if message.contains("does not exist")),
            "expected a does-not-exist error, got: {err}"
        );
    }

    #[tokio::test]
    async fn execute_read_scalar_column_fails_loud() {
        // A scalar (non-vector) column targeted by a vector read must fail loud,
        // not return an empty data-evolution stream.
        let table = pk_vector_table(&[
            ("pk-vector.index.columns", "embedding"),
            ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
            ("fields.embedding.pk-vector.distance.metric", "l2"),
        ]);
        let err = match table
            .new_vector_search_builder()
            .with_vector_column("id") // scalar Int column
            .with_query_vector(vec![1.0])
            .with_limit(5)
            .execute_read()
            .await
        {
            Ok(_) => panic!("scalar vector column must fail loud on execute_read"),
            Err(e) => e,
        };
        assert!(
            matches!(&err, crate::Error::DataInvalid { message, .. } if message.contains("must be a FLOAT vector column")),
            "expected a not-a-vector-column error, got: {err}"
        );
    }

    #[tokio::test]
    async fn execute_read_non_float_vector_column_fails_loud() {
        // An ARRAY<INT> column is not a searchable vector column (the index/search
        // operates on FLOAT elements). It must fail loud rather than fall through
        // to the DE path and return an empty stream.
        use crate::spec::{ArrayType, IntType, Schema, TableSchema};
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column(
                "embedding",
                DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
            )
            .build()
            .unwrap();
        let table = Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("default", "de_non_float_vector"),
            "memory:/de_non_float_vector".to_string(),
            TableSchema::new(0, &schema),
            None,
        );
        let err = match table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0])
            .with_limit(5)
            .execute_read()
            .await
        {
            Ok(_) => panic!("ARRAY<INT> vector column must fail loud on execute_read"),
            Err(e) => e,
        };
        assert!(
            matches!(&err, crate::Error::DataInvalid { message, .. } if message.contains("must be a FLOAT vector column")),
            "expected a FLOAT-vector-column error, got: {err}"
        );
    }

    #[tokio::test]
    async fn execute_read_empty_plan_reserved_projection_fails_loud() {
        // Empty plan (no snapshot) must still fail loud on a reserved-name
        // projection: projection validity does not depend on whether the search
        // matched any rows. A regression that resolved the projection only after
        // the `candidates.is_empty()` early return would yield an empty stream here
        // instead of an error.
        let table = pk_vector_table(&[
            ("pk-vector.index.columns", "embedding"),
            ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
            ("fields.embedding.pk-vector.distance.metric", "l2"),
            // Pin the index dimension so the query vector below matches it; the
            // up-front dimension guard runs before this test's reserved-projection
            // guard, so a mismatched query would mask the error under test.
            ("fields.embedding.dimension", "4"),
        ]);
        for reserved in [
            ROW_ID_FIELD_NAME,
            PKEY_VECTOR_POSITION_COLUMN,
            SEARCH_SCORE_COLUMN,
        ] {
            let mut builder = table.new_vector_search_builder();
            builder
                .with_vector_column("embedding")
                .with_query_vector(vec![1.0; 4])
                .with_limit(5)
                .with_projection(&["id", reserved]);
            let err = builder
                .execute_read()
                .await
                .map(|_| ())
                .expect_err("empty plan + reserved projection must fail loud");
            assert!(
                matches!(err, crate::Error::DataInvalid { ref message, .. }
                    if message.contains("reserved column")),
                "unexpected error for {reserved}: {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn execute_read_empty_plan_lumina_array_float_is_admitted() {
        // A Lumina PK-vector `ARRAY<FLOAT>` column is a valid configuration, but
        // batch query dimension validation routed every `ARRAY<FLOAT>` column
        // through the vindex resolver, which rejects `lumina` as an unsupported
        // index type before planning — failing even an empty table. The
        // dimension must be resolved per the configured backend, so a
        // well-formed Lumina query is admitted and (with no snapshot) yields an
        // empty stream rather than an "Unsupported vindex index type" error.
        let table = pk_vector_table(&[
            ("pk-vector.index.columns", "embedding"),
            (
                "fields.embedding.pk-vector.index.type",
                crate::lumina::LUMINA_IDENTIFIER,
            ),
            ("fields.embedding.pk-vector.distance.metric", "l2"),
            ("lumina.index.dimension", "4"),
        ]);
        let mut stream = table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0; 4])
            .with_limit(5)
            .execute_read()
            .await
            .expect(
                "Lumina ARRAY<FLOAT> query must be admitted, not rejected as unsupported vindex",
            );
        assert!(stream.try_next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn execute_read_projection_reserved_name_fails_loud() {
        // Projecting a reserved metadata / row-id column must fail loud. The guard
        // lives in `resolve_materialize_read_type`, which `execute_read` invokes
        // before the empty-plan early return; assert on the resolver directly here.
        let table = pk_vector_table(&[
            ("pk-vector.index.columns", "embedding"),
            ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
            ("fields.embedding.pk-vector.distance.metric", "l2"),
        ]);
        for reserved in [
            ROW_ID_FIELD_NAME,
            PKEY_VECTOR_POSITION_COLUMN,
            SEARCH_SCORE_COLUMN,
        ] {
            let mut builder = table.new_vector_search_builder();
            builder
                .with_vector_column("embedding")
                .with_query_vector(vec![1.0])
                .with_limit(5)
                .with_projection(&["id", reserved]);
            let err = builder
                .resolve_materialize_read_type()
                .expect_err("reserved projection must fail loud");
            assert!(
                matches!(err, crate::Error::DataInvalid { ref message, .. }
                    if message.contains("reserved column")),
                "unexpected error for {reserved}: {err:?}"
            );
        }
    }

    #[test]
    fn resolve_materialize_read_type_default_is_all_user_columns() {
        // No with_projection -> every user table column (id + embedding).
        let table = pk_vector_table(&[
            ("pk-vector.index.columns", "embedding"),
            ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
            ("fields.embedding.pk-vector.distance.metric", "l2"),
        ]);
        let builder = table.new_vector_search_builder();
        let fields = builder.resolve_materialize_read_type().unwrap();
        let names: Vec<&str> = fields.iter().map(|f| f.name()).collect();
        assert_eq!(names, vec!["id", "embedding"]);
    }

    /// A PK-vector table whose user schema carries an extra column named
    /// `reserved`, used to prove reserved metadata names are rejected even when
    /// they arrive via the default (all-columns) projection.
    fn pk_vector_table_with_extra_column(reserved: &str) -> Table {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column(
                "embedding",
                DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
            )
            .column(reserved, DataType::Int(IntType::new()))
            .option("pk-vector.index.columns", "embedding")
            .option("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER)
            .option("fields.embedding.pk-vector.distance.metric", "l2")
            .build()
            .unwrap();
        Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("default", "reserved_col_test"),
            "memory:/reserved_col_test".to_string(),
            TableSchema::new(0, &schema),
            None,
        )
    }

    #[test]
    fn resolve_materialize_read_type_default_rejects_reserved_user_column() {
        // The default (all-columns) projection must reject a user column whose
        // name collides with an injected metadata column, not only columns named
        // in an explicit projection. Otherwise it silently passes on an empty
        // result and collides with the metadata columns the read attaches.
        let table = pk_vector_table_with_extra_column(SEARCH_SCORE_COLUMN);
        let builder = table.new_vector_search_builder();
        let err = builder.resolve_materialize_read_type().unwrap_err();
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("reserved column")),
            "single-query default projection must reject reserved user column, got: {err:?}"
        );
    }

    #[test]
    fn batch_resolve_materialize_read_type_default_rejects_reserved_user_column() {
        // Same guard on the batch resolver.
        let table = pk_vector_table_with_extra_column(PKEY_VECTOR_POSITION_COLUMN);
        let builder = table.new_batch_vector_search_builder();
        let err = builder.resolve_materialize_read_type().unwrap_err();
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("reserved column")),
            "batch default projection must reject reserved user column, got: {err:?}"
        );
    }

    #[test]
    fn resolve_materialize_read_type_projection_selects_named_columns() {
        let table = pk_vector_table(&[
            ("pk-vector.index.columns", "embedding"),
            ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
            ("fields.embedding.pk-vector.distance.metric", "l2"),
        ]);
        let mut builder = table.new_vector_search_builder();
        builder.with_projection(&["id"]);
        let fields = builder.resolve_materialize_read_type().unwrap();
        let names: Vec<&str> = fields.iter().map(|f| f.name()).collect();
        assert_eq!(names, vec!["id"]);
    }

    #[tokio::test]
    async fn de_execute_read_materializes_rows_with_score() {
        // A data-evolution vector table with a committed global index: execute_read
        // must materialize one row per scored hit and carry the unified score
        // column, in best-first rank order.
        let table = de_vector_table().await;
        let query = vec![1.0, 0.0];

        let scored = table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(query.clone())
            .with_limit(3)
            .execute_scored()
            .await
            .unwrap();
        assert!(!scored.is_empty(), "DE search must return hits");

        let mut stream = table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(query)
            .with_limit(3)
            .execute_read()
            .await
            .unwrap();

        let mut rows = 0usize;
        let mut saw_score = false;
        while let Some(batch) = stream.try_next().await.unwrap() {
            rows += batch.num_rows();
            saw_score |= batch.schema().index_of(SEARCH_SCORE_COLUMN).is_ok();
        }
        assert_eq!(
            rows,
            scored.len(),
            "DE read must emit exactly the scored result count"
        );
        assert!(
            saw_score,
            "DE read output must carry the search score column"
        );
    }

    #[tokio::test]
    async fn de_execute_read_with_filter_fails_loud() {
        // A filter on the data-evolution path is unsupported (the DE path never
        // reads physical rows), so execute_read must fail loud rather than drop the
        // predicate. The guard lives in execute_scored.
        let table = de_vector_table().await;
        let filter = id_gt_filter(&table, 1);
        let err = table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0, 0.0])
            .with_limit(3)
            .with_filter(filter)
            .execute_read()
            .await
            .map(|_| ())
            .expect_err("DE read with a filter must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { .. }),
            "unexpected error: {err:?}"
        );
    }
}

/// Tests for [`residual_positions_by_file`]: the residual predicate is applied at
/// the Arrow level (no pushdown) against the predicate columns, and each surviving
/// row's file-local physical position is recovered from its ordinal in the
/// unfiltered scan (no `_ROW_ID`, no `first_row_id`).
#[cfg(test)]
mod residual_positions_tests {
    use super::*;
    use crate::arrow::build_target_arrow_schema;
    use crate::arrow::format::FilePredicates;
    use crate::io::FileIOBuilder;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{
        BigIntType, BinaryRow, DataField, DataFileMeta, DataType, Datum, IntType, PredicateBuilder,
        ROW_ID_FIELD_ID, ROW_ID_FIELD_NAME,
    };
    use crate::table::data_file_reader::DataFileReader;
    use crate::table::schema_manager::SchemaManager;
    use crate::table::source::{DataSplit, DataSplitBuilder};
    use arrow_array::{Int32Array, RecordBatch};
    use bytes::Bytes;
    use paimon_mosaic_core::spec::COMPRESSION_NONE;
    use paimon_mosaic_core::writer::{MosaicWriter, OutputFile, WriterOptions};
    use std::io;
    use std::sync::Arc;

    struct MemOutputFile {
        data: Vec<u8>,
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

    fn row_id_field() -> DataField {
        DataField::new(
            ROW_ID_FIELD_ID,
            ROW_ID_FIELD_NAME.to_string(),
            DataType::BigInt(BigIntType::new()),
        )
    }

    fn id_batch(ids: Vec<i32>) -> RecordBatch {
        let schema = build_target_arrow_schema(&[id_field()]).unwrap();
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids))]).unwrap()
    }

    fn write_mosaic(batch: &RecordBatch) -> Bytes {
        let mut writer = MosaicWriter::new(
            MemOutputFile { data: Vec::new() },
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

    fn data_file(
        file_name: &str,
        file_size: i64,
        row_count: i64,
        first_row_id: Option<i64>,
    ) -> DataFileMeta {
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
            first_row_id,
            write_cols: None,
        }
    }

    /// Build a predicate-free reader (read_type = `id` + `_ROW_ID`) over a split
    /// containing `files` (each `(name, ids, first_row_id)`), written as Mosaic
    /// data files in the same bucket. The returned active-file list covers every
    /// file (all files active).
    async fn build_reader_and_split(
        table_path: &str,
        files: &[(&str, Vec<i32>, i64)],
    ) -> (DataFileReader, DataSplit, Vec<BucketActiveFile>) {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let bucket_path = format!("{table_path}/bucket-0");
        let mut metas = Vec::new();
        let mut active_files = Vec::new();
        for (name, ids, first_row_id) in files {
            let data = write_mosaic(&id_batch(ids.clone()));
            file_io
                .new_output(&format!("{bucket_path}/{name}"))
                .unwrap()
                .write(data.clone())
                .await
                .unwrap();
            metas.push(data_file(
                name,
                data.len() as i64,
                ids.len() as i64,
                Some(*first_row_id),
            ));
            active_files.push(BucketActiveFile {
                file_name: name.to_string(),
                row_count: ids.len() as i64,
            });
        }
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(bucket_path)
            .with_total_buckets(1)
            .with_data_files(metas)
            .build()
            .unwrap();
        let reader = DataFileReader::new(
            file_io.clone(),
            SchemaManager::new(file_io, table_path.to_string()),
            1,
            vec![id_field()],
            vec![id_field(), row_id_field()],
            Vec::new(),
        );
        (reader, split, active_files)
    }

    /// `id > threshold`, with `file_fields` = `[id]` so the leaf index resolves.
    fn residual_id_gt(threshold: i32) -> FilePredicates {
        let pred = PredicateBuilder::new(&[id_field()])
            .greater_than("id", Datum::Int(threshold))
            .unwrap();
        FilePredicates {
            predicates: vec![pred],
            row_filter_factory: None,
            file_fields: vec![id_field()],
        }
    }

    fn sorted(t: &roaring::RoaringTreemap) -> Vec<u64> {
        t.iter().collect()
    }

    #[tokio::test]
    async fn test_residual_selects_matching_positions() {
        // ids [1,2,3,4,5] at first_row_id 0; id > 2 -> ids 3,4,5 -> positions 2,3,4.
        let (reader, split, active) = build_reader_and_split(
            "memory:/rpf_basic",
            &[("part-0.mosaic", vec![1, 2, 3, 4, 5], 0)],
        )
        .await;
        let map = residual_positions_by_file(&reader, &split, &active, &residual_id_gt(2))
            .await
            .unwrap();
        assert_eq!(sorted(&map["part-0.mosaic"]), vec![2, 3, 4]);
    }

    #[tokio::test]
    async fn test_residual_matches_none_yields_empty_entry() {
        // id > 100 matches nothing; the file still gets a (present, empty) entry.
        let (reader, split, active) =
            build_reader_and_split("memory:/rpf_none", &[("part-0.mosaic", vec![1, 2, 3], 0)])
                .await;
        let map = residual_positions_by_file(&reader, &split, &active, &residual_id_gt(100))
            .await
            .unwrap();
        assert!(map.contains_key("part-0.mosaic"));
        assert!(map["part-0.mosaic"].is_empty());
    }

    #[tokio::test]
    async fn test_residual_matches_all_yields_full_set() {
        let (reader, split, active) =
            build_reader_and_split("memory:/rpf_all", &[("part-0.mosaic", vec![1, 2, 3], 0)]).await;
        let map = residual_positions_by_file(&reader, &split, &active, &residual_id_gt(0))
            .await
            .unwrap();
        assert_eq!(sorted(&map["part-0.mosaic"]), vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn test_residual_positions_are_file_local_across_files() {
        // Two files with distinct first_row_id; positions must be 0-based within
        // each file, not global. id > 3 keeps ids 4,5 in both -> positions {3,4}.
        let (reader, split, active) = build_reader_and_split(
            "memory:/rpf_multi",
            &[
                ("part-0.mosaic", vec![1, 2, 3, 4, 5], 0),
                ("part-1.mosaic", vec![1, 2, 3, 4, 5], 100),
            ],
        )
        .await;
        let map = residual_positions_by_file(&reader, &split, &active, &residual_id_gt(3))
            .await
            .unwrap();
        assert_eq!(sorted(&map["part-0.mosaic"]), vec![3, 4]);
        assert_eq!(sorted(&map["part-1.mosaic"]), vec![3, 4]);
    }

    #[tokio::test]
    async fn test_non_active_files_are_skipped() {
        // Two files in the split, but only `part-0.mosaic` is active. The bucket
        // search never recalls from `part-1.mosaic` (level-0 / non-active), so it
        // must not appear in the residual map — and even though it lacks a
        // `first_row_id`, the query still succeeds because non-active files are
        // skipped before the guard.
        let (reader, split, mut active) = build_reader_and_split(
            "memory:/rpf_nonactive",
            &[("part-0.mosaic", vec![1, 2, 3, 4, 5], 0)],
        )
        .await;
        // Append a non-active file (missing first_row_id) directly to the split's
        // data files, but leave it out of the active list.
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let bucket_path = "memory:/rpf_nonactive/bucket-0";
        let data = write_mosaic(&id_batch(vec![9, 9, 9]));
        file_io
            .new_output(&format!("{bucket_path}/part-1.mosaic"))
            .unwrap()
            .write(data.clone())
            .await
            .unwrap();
        let mut metas = split.data_files().to_vec();
        metas.push(data_file("part-1.mosaic", data.len() as i64, 3, None));
        // `active` already lists only part-0.mosaic; keep it that way.
        let _ = &mut active;
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(bucket_path.to_string())
            .with_total_buckets(1)
            .with_data_files(metas)
            .build()
            .unwrap();
        let map = residual_positions_by_file(&reader, &split, &active, &residual_id_gt(2))
            .await
            .unwrap();
        assert_eq!(sorted(&map["part-0.mosaic"]), vec![2, 3, 4]);
        assert!(
            !map.contains_key("part-1.mosaic"),
            "non-active file must be skipped"
        );
    }

    #[tokio::test]
    async fn test_missing_first_row_id_recovers_local_positions() {
        // Real primary-key data files carry no `first_row_id`. Positions are
        // recovered from each row's ordinal in the scan, so the residual still
        // works: ids [1,2,3] with id > 0 -> all match -> local positions [0,1,2].
        let (reader, split, active) = build_reader_and_split_no_first_row_id().await;
        let map = residual_positions_by_file(&reader, &split, &active, &residual_id_gt(0))
            .await
            .expect("missing first_row_id must not fail the residual read");
        assert_eq!(sorted(&map["part-0.mosaic"]), vec![0, 1, 2]);
    }

    async fn build_reader_and_split_no_first_row_id(
    ) -> (DataFileReader, DataSplit, Vec<BucketActiveFile>) {
        let table_path = "memory:/rpf_nofrid";
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let bucket_path = format!("{table_path}/bucket-0");
        let data = write_mosaic(&id_batch(vec![1, 2, 3]));
        file_io
            .new_output(&format!("{bucket_path}/part-0.mosaic"))
            .unwrap()
            .write(data.clone())
            .await
            .unwrap();
        let split = DataSplitBuilder::new()
            .with_snapshot(1)
            .with_partition(BinaryRow::new(0))
            .with_bucket(0)
            .with_bucket_path(bucket_path)
            .with_total_buckets(1)
            .with_data_files(vec![data_file("part-0.mosaic", data.len() as i64, 3, None)])
            .build()
            .unwrap();
        let reader = DataFileReader::new(
            file_io.clone(),
            SchemaManager::new(file_io, table_path.to_string()),
            1,
            vec![id_field()],
            vec![id_field(), row_id_field()],
            Vec::new(),
        );
        // The lone file is active and carries no first_row_id, exercising the
        // ordinal-based position recovery.
        let active = vec![BucketActiveFile {
            file_name: "part-0.mosaic".to_string(),
            row_count: 3,
        }];
        (reader, split, active)
    }
}
