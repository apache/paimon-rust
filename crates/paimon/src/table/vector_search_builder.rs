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

use crate::io::FileIO;
use crate::lumina::reader::LuminaVectorGlobalIndexReader;
use crate::lumina::{is_lumina_index_type, LuminaIndexMeta, LuminaVectorMetric};
use crate::spec::{
    CoreOptions, DataField, FileKind, GlobalIndexSearchMode, IndexFileMeta, IndexManifest,
    IndexManifestEntry, ROW_ID_FIELD_NAME,
};
use crate::table::data_file_reader::DataFileReader;
use crate::table::global_index_scanner::{
    deleted_row_ranges_for_data_evolution_dvs, search_limit_with_deleted_rows,
    unindexed_ranges_for_global_index_entries, RowRangeIndex,
};
use crate::table::pk_vector_data_file_reader::DataFilePkVectorReaderFactory;
use crate::table::pk_vector_indexed_split_read::PkVectorIndexedSplitRead;
use crate::table::pk_vector_orchestrator::{
    build_indexed_splits, validate_row_position, PkVectorCandidate, PkVectorOrchestrator,
    PkVectorSearchSplit,
};
use crate::table::pk_vector_position_read::{
    PKEY_VECTOR_POSITION_COLUMN, PKEY_VECTOR_SCORE_COLUMN,
};
use crate::table::pk_vector_scan::{PkVectorScan, PkVectorScanPlan};
use crate::table::read_builder::resolve_projected_fields;
use crate::table::{
    find_field_id_by_name, merge_row_ranges, ArrowRecordBatchStream, RowRange, Table,
};
use crate::vector_search::{GlobalIndexIOMeta, SearchResult, VectorSearch};
use crate::vindex::is_vindex_index_type;
use crate::vindex::pkvector::ann::VindexAnnSearcher;
use crate::vindex::pkvector::bucket::{covered_source_files, BucketActiveFile, BucketAnnSegment};
use crate::vindex::pkvector::metric::VectorSearchMetric;
use crate::vindex::pkvector::reader::PkVectorReader;
use crate::vindex::reader::VindexVectorGlobalIndexReader;
use arrow_array::{Array, FixedSizeListArray, Float32Array, Int64Array, ListArray, RecordBatch};
use arrow_select::interleave::interleave_record_batch;
use futures::{stream, TryStreamExt};
use paimon_vindex_core::distance::MetricType;
use paimon_vindex_core::index::VectorIndexReader as VIndexReader;
use roaring::RoaringTreemap;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::io::Cursor;

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
}

pub struct BatchVectorSearchBuilder<'a> {
    table: &'a Table,
    vector_column: Option<String>,
    query_vectors: Option<Vec<Vec<f32>>>,
    limit: Option<usize>,
    options: HashMap<String, String>,
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

    /// Restrict the columns materialized by [`execute_read`](Self::execute_read)
    /// to `cols` (plus the always-appended `_PKEY_VECTOR_SCORE`). Without this
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
        // Membership is resolved first via the non-erroring columns accessor so a
        // malformed PK-vector config (e.g. more than one column, or a blank list)
        // cannot abort an unrelated DE query. The exactly-one-column rule is
        // enforced only once this query is known to target a PK-vector column,
        // keeping fail-loud behavior for a genuinely-broken config on the path
        // where erroring is correct.
        if core.primary_key_vector_index_enabled() {
            let targets_pk_column = core
                .primary_key_vector_index_columns()
                .ok()
                .is_some_and(|cols| cols.iter().any(|c| c == vector_column));
            if targets_pk_column {
                let pk_col = core.primary_key_vector_index_column()?;
                return self
                    .execute_primary_key_vector_search(&core, &pk_col, query_vector, limit)
                    .await;
            }
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
    /// ordered best-first. Only supported for primary-key vector indexes; a
    /// data-evolution table or a query targeting a non-PK-vector column fails
    /// loud. Output columns are the projected user table columns (all user
    /// columns by default, or those set via
    /// [`with_projection`](Self::with_projection)) plus `_PKEY_VECTOR_SCORE`;
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

        Err(crate::Error::DataInvalid {
            message: "vector search read is only supported for primary-key vector indexes".into(),
            source: None,
        })
    }

    /// Run the primary-key bucket-local vector search: plan the per-bucket splits,
    /// build the real vindex ANN scorer and (outside FAST mode) the exact-fallback
    /// readers, run the orchestrator, and convert the best-first candidates into a
    /// `SearchResult`. Mirrors Java `PrimaryKeyVectorRead`.
    async fn execute_primary_key_vector_search(
        &self,
        core: &CoreOptions<'_>,
        pk_col: &str,
        query_vector: &[f32],
        limit: usize,
    ) -> crate::Result<SearchResult> {
        let (candidates, plan, metric) = self
            .plan_and_search_pk_candidates(core, pk_col, query_vector, limit)
            .await?;
        candidates_to_search_result(&candidates, &plan.splits, metric)
    }

    /// Shared PK-vector search core for both the search-only and search-and-read
    /// paths: plan the per-bucket splits, verify the configured metric against each
    /// ANN segment, build the real vindex ANN scorer and (outside FAST mode) the
    /// exact-fallback readers, and run the orchestrator. Returns the best-first
    /// candidates together with the plan and resolved metric so the caller can
    /// either serialize them to a `SearchResult` or materialize their rows. An
    /// empty plan yields empty candidates.
    async fn plan_and_search_pk_candidates(
        &self,
        core: &CoreOptions<'_>,
        pk_col: &str,
        query_vector: &[f32],
        limit: usize,
    ) -> crate::Result<(Vec<PkVectorCandidate>, PkVectorScanPlan, VectorSearchMetric)> {
        // Residual-filter guard: PK vector search accepts partition filters only.
        // This builder exposes no data-predicate setter, so there is nothing to
        // reject here; the guard mirrors Java `checkArgument(filter == null)` and,
        // if a filter setter is ever added, it must error rather than be ignored.

        // `primary_key_vector_distance_metric` returns a validated name; re-parse
        // into the enum for the numeric semantics.
        let metric = VectorSearchMetric::parse(&core.primary_key_vector_distance_metric(pk_col)?)?;
        let index_type = core.primary_key_vector_index_type(pk_col)?;
        let field_id =
            find_field_id_by_name(self.table.schema().fields(), pk_col).ok_or_else(|| {
                crate::Error::DataInvalid {
                    message: format!("PK-vector column '{pk_col}' not found in schema"),
                    source: None,
                }
            })?;
        let vector_field = self
            .table
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

        let plan = PkVectorScan::new(self.table, field_id, index_type)
            .plan()
            .await?;
        if plan.splits.is_empty() {
            return Ok((Vec::new(), plan, metric));
        }

        // Production data-file reader, mirroring `table_read.rs::new_data_file_reader`
        // but projecting only the vector column with no predicates.
        let reader = DataFileReader::new(
            self.table.file_io().clone(),
            self.table.schema_manager().clone(),
            self.table.schema().id(),
            self.table.schema().fields().to_vec(),
            vec![vector_field.clone()],
            Vec::new(),
        );

        // Real ANN scorer: preload each segment's bytes (keyed by resolved,
        // globally unique path) and drive the vindex reader from memory.
        let segment_bytes = preload_segment_bytes(self.table.file_io(), &plan.splits).await?;
        // Fail loud on a config/segment metric mismatch before scoring, mirroring
        // Java `PkVectorAnnSegmentSearcher.search`.
        verify_pk_vector_segment_metrics(&plan.splits, &segment_bytes, metric)?;
        let options = {
            let mut o = self.table.schema().options().clone();
            o.extend(self.options.clone());
            o
        };
        let search_options = options.clone();
        let field_name = pk_col.to_string();
        let scorer: crate::vindex::pkvector::ann::Scorer =
            Box::new(move |segment: &BucketAnnSegment, search: &VectorSearch| {
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
                let mut reader = VindexVectorGlobalIndexReader::new(io_meta, options.clone());
                reader.visit_vector_search(search, |_| Ok(Cursor::new(data)))
            });
        let ann_searcher = VindexAnnSearcher::new(field_name, scorer);

        // Exact-fallback readers, keyed by (split_index, file_name). In FAST mode
        // the kernel never invokes the factory, so skip the in-memory column read
        // entirely. Otherwise preload only the *uncovered* active files: files an
        // ANN segment already covers never reach the exact fallback, so reading
        // their vector column here would be wasted IO/memory. Mirrors Java, which
        // creates a `PkVectorReader` lazily only for uncovered files.
        let mut exact_readers: HashMap<(usize, String), Box<dyn PkVectorReader>> = HashMap::new();
        if !skip_exact_fallback {
            for (split_index, split) in plan.splits.iter().enumerate() {
                let covered = covered_source_files(&split.ann_segments, &split.active_files);
                let factory = DataFilePkVectorReaderFactory::new(
                    reader.clone(),
                    split.data_split.clone(),
                    vector_field.clone(),
                )?;
                for active in &split.active_files {
                    if covered.contains(&active.file_name) {
                        continue;
                    }
                    let r = factory.create(active).await?;
                    exact_readers.insert((split_index, active.file_name.clone()), r);
                }
            }
        }
        let mut factory = |split_index: usize,
                           _split: &PkVectorSearchSplit,
                           file: &BucketActiveFile|
         -> crate::Result<Box<dyn PkVectorReader>> {
            exact_readers
                .remove(&(split_index, file.file_name.clone()))
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: format!("no preloaded exact reader for {}", file.file_name),
                    source: None,
                })
        };

        let candidates = PkVectorOrchestrator::new(reader)
            .search_candidates(
                &plan.splits,
                query_vector,
                metric,
                limit,
                Some(&ann_searcher),
                &mut factory,
                &search_options,
                skip_exact_fallback,
            )
            .await?;

        Ok((candidates, plan, metric))
    }

    /// Materialize the best-first PK-vector search hits into Arrow rows. Mirrors
    /// Java `PrimaryKeyVectorRead` feeding its result splits into an ordinary table
    /// read: the search decides which rows, a subsequent read decides which
    /// columns.
    ///
    /// Output columns are the projected user table columns (all user columns when
    /// [`with_projection`](Self::with_projection) was not called) plus
    /// `_PKEY_VECTOR_SCORE`; `_ROW_ID` and `_PKEY_VECTOR_POSITION` are always
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

        if candidates.is_empty() {
            return Ok(Box::pin(stream::empty()));
        }

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
                    if name == PKEY_VECTOR_POSITION_COLUMN
                        || name == PKEY_VECTOR_SCORE_COLUMN
                        || name == ROW_ID_FIELD_NAME
                    {
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
        Ok(fields)
    }
}

impl<'a> BatchVectorSearchBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            vector_column: None,
            query_vectors: None,
            limit: None,
            options: HashMap::new(),
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

    pub async fn execute(&self) -> crate::Result<Vec<SearchResult>> {
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
) -> crate::Result<()> {
    let mut checked: HashSet<&str> = HashSet::new();
    for split in splits {
        for segment in &split.ann_segments {
            if !checked.insert(segment.path.as_str()) {
                continue;
            }
            let bytes =
                segment_bytes
                    .get(&segment.path)
                    .ok_or_else(|| crate::Error::DataInvalid {
                        message: format!(
                            "missing preloaded ANN bytes for segment '{}'",
                            segment.path
                        ),
                        source: None,
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
            let segment_metric = reader.metadata().metric;
            if VectorSearchMetric::from_vindex(segment_metric) != configured {
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

/// candidate order (no re-sort). Each candidate's global row id is
/// `first_row_id + row_position` of the data file it references; the score is
/// derived from the raw distance via the metric. A candidate referencing a file
/// absent from its split, or a file with no `first_row_id`, fails loud.
fn candidates_to_search_result(
    candidates: &[PkVectorCandidate],
    splits: &[PkVectorSearchSplit],
    metric: VectorSearchMetric,
) -> crate::Result<SearchResult> {
    let mut row_ids = Vec::with_capacity(candidates.len());
    let mut scores = Vec::with_capacity(candidates.len());
    for c in candidates {
        let split = splits
            .get(c.split_index)
            .ok_or_else(|| crate::Error::DataInvalid {
                message: format!("candidate split_index {} out of range", c.split_index),
                source: None,
            })?;
        let file_meta = split
            .data_split
            .data_files()
            .iter()
            .find(|f| f.file_name == c.data_file_name)
            .ok_or_else(|| crate::Error::DataInvalid {
                message: format!(
                    "candidate references data file {} not present in its split",
                    c.data_file_name
                ),
                source: None,
            })?;
        let first_row_id = file_meta
            .first_row_id
            .ok_or_else(|| crate::Error::DataInvalid {
                message: format!("data file {} has no first_row_id", c.data_file_name),
                source: None,
            })?;
        validate_row_position(&c.data_file_name, c.row_position, file_meta.row_count)?;
        let global =
            first_row_id
                .checked_add(c.row_position)
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: "global row id overflows i64".to_string(),
                    source: None,
                })?;
        row_ids.push(
            u64::try_from(global).map_err(|_| crate::Error::DataInvalid {
                message: format!("negative global row id {global}"),
                source: None,
            })?,
        );
        scores.push(metric.distance_to_score(c.distance));
    }
    // Order preserved: best-first, as produced by the orchestrator.
    Ok(SearchResult::new(row_ids, scores))
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
/// yields no batches). The projected user columns and `_PKEY_VECTOR_SCORE` are
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
    // columns + _PKEY_VECTOR_SCORE) in order.
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
        ArrayType, BinaryRow, DataFileMeta, DataType, FloatType, GlobalIndexMeta, IndexFileMeta,
        IndexManifestEntry, IntType, Schema, TableSchema,
    };
    use crate::table::source::DataSplitBuilder;
    use crate::vindex::IVF_FLAT_IDENTIFIER;
    use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
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

    #[test]
    fn candidates_to_search_result_global_row_id_and_best_first_order() {
        // Two files in one split with different first_row_id. The helper is a pure
        // order-preserving map: the orchestrator already established best-first
        // order upstream, so the candidate INPUT order here is deliberately NOT in
        // score order and NOT in (file, position) order. This proves the helper
        // preserves the given sequence rather than sorting.
        let splits = vec![pk_search_split(
            0,
            vec![
                pk_data_file("file-a", 100, Some(1000)),
                pk_data_file("file-b", 100, Some(5000)),
            ],
        )];
        // Input sequence (NOT sorted by score, NOT sorted by file/position):
        //   c0: file-b pos5 d=2.0  -> WORST distance, appears FIRST
        //   c1: file-b pos1 d=1.0  -> tie with c2
        //   c2: file-a pos2 d=1.0  -> tie with c1
        // A score-based best-first re-sort would produce [c1, c2, c0] (worst last);
        // a (file, position) re-sort would produce [c2 (file-a), c1, c0]. Both
        // differ from the input order, so the exact assertion below discriminates.
        let candidates = vec![
            pk_candidate(0, 0, "file-b", 5, 2.0),
            pk_candidate(0, 0, "file-b", 1, 1.0),
            pk_candidate(0, 0, "file-a", 2, 1.0),
        ];
        let result = candidates_to_search_result(&candidates, &splits, VectorSearchMetric::L2)
            .expect("conversion succeeds");
        // global_row_id = first_row_id + position; INPUT order preserved (not sorted).
        assert_eq!(result.row_ids, vec![5005, 5001, 1002]);
        assert_eq!(
            result.scores,
            vec![
                VectorSearchMetric::L2.distance_to_score(2.0),
                VectorSearchMetric::L2.distance_to_score(1.0),
                VectorSearchMetric::L2.distance_to_score(1.0),
            ]
        );
    }

    #[test]
    fn candidates_to_search_result_absent_first_row_id_fails_loud() {
        let splits = vec![pk_search_split(0, vec![pk_data_file("file-a", 100, None)])];
        let candidates = vec![pk_candidate(0, 0, "file-a", 0, 1.0)];
        let err = candidates_to_search_result(&candidates, &splits, VectorSearchMetric::L2)
            .expect_err("absent first_row_id must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. } if message.contains("first_row_id")),
            "unexpected error: {err:?}"
        );
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

    #[test]
    fn verify_pk_vector_segment_metrics_accepts_matching_metric() {
        // Real IVF segment trained with L2; configured metric L2 => Ok.
        let bytes = build_vindex_segment_bytes("l2");
        let splits = vec![pk_split_with_segment("seg-l2")];
        let segment_bytes = HashMap::from([("seg-l2".to_string(), bytes)]);
        verify_pk_vector_segment_metrics(&splits, &segment_bytes, VectorSearchMetric::L2)
            .expect("matching metric must pass");
    }

    #[test]
    fn verify_pk_vector_segment_metrics_rejects_mismatched_metric() {
        // Real IVF segment trained with L2; configured metric Cosine => fail loud.
        let bytes = build_vindex_segment_bytes("l2");
        let splits = vec![pk_split_with_segment("seg-l2")];
        let segment_bytes = HashMap::from([("seg-l2".to_string(), bytes)]);
        let err =
            verify_pk_vector_segment_metrics(&splits, &segment_bytes, VectorSearchMetric::Cosine)
                .expect_err("mismatched metric must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("does not match configured metric")
                    && message.contains("l2")
                    && message.contains("cosine")),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn candidates_to_search_result_missing_file_fails_loud() {
        let splits = vec![pk_search_split(
            0,
            vec![pk_data_file("known", 100, Some(0))],
        )];
        let candidates = vec![pk_candidate(0, 0, "unknown", 0, 1.0)];
        let err = candidates_to_search_result(&candidates, &splits, VectorSearchMetric::L2)
            .expect_err("missing file must fail loud");
        assert!(matches!(err, crate::Error::DataInvalid { .. }));
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
    async fn pk_branch_enabled_empty_plan_returns_empty() {
        // pk-vector.index.columns set, but no snapshot -> empty plan -> empty result.
        let table = pk_vector_table(&[
            ("pk-vector.index.columns", "embedding"),
            ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
            ("fields.embedding.pk-vector.distance.metric", "l2"),
        ]);
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
    /// `_PKEY_VECTOR_POSITION: Int64`, and `_PKEY_VECTOR_SCORE: Float32` (mirroring
    /// what `PkVectorIndexedSplitRead` emits for a single file).
    fn materialized_batch(rows: &[(i32, i64, f32)]) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new(PKEY_VECTOR_POSITION_COLUMN, ArrowDataType::Int64, false),
            ArrowField::new(PKEY_VECTOR_SCORE_COLUMN, ArrowDataType::Float32, false),
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
            f32_col(out, PKEY_VECTOR_SCORE_COLUMN),
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
            f32_col(&out[0], PKEY_VECTOR_SCORE_COLUMN),
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

    #[tokio::test]
    async fn execute_read_de_table_fails_loud() {
        // No pk-vector index configured: execute_read must fail loud (the DE path
        // has no row materialization).
        let table = pk_vector_table(&[]);
        let err = table
            .new_vector_search_builder()
            .with_vector_column("embedding")
            .with_query_vector(vec![1.0])
            .with_limit(5)
            .execute_read()
            .await
            .map(|_| ())
            .expect_err("DE read must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("only supported for primary-key")),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn execute_read_non_pk_column_fails_loud() {
        // pk-vector index configured for "embedding", but the query targets a
        // different column -> read is unsupported.
        let table = pk_vector_table(&[
            ("pk-vector.index.columns", "embedding"),
            ("fields.embedding.pk-vector.index.type", IVF_FLAT_IDENTIFIER),
            ("fields.embedding.pk-vector.distance.metric", "l2"),
        ]);
        let err = table
            .new_vector_search_builder()
            .with_vector_column("other")
            .with_query_vector(vec![1.0])
            .with_limit(5)
            .execute_read()
            .await
            .map(|_| ())
            .expect_err("non-PK column read must fail loud");
        assert!(
            matches!(err, crate::Error::DataInvalid { ref message, .. }
                if message.contains("only supported for primary-key")),
            "unexpected error: {err:?}"
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
        ]);
        for reserved in [
            ROW_ID_FIELD_NAME,
            PKEY_VECTOR_POSITION_COLUMN,
            PKEY_VECTOR_SCORE_COLUMN,
        ] {
            let mut builder = table.new_vector_search_builder();
            builder
                .with_vector_column("embedding")
                .with_query_vector(vec![1.0])
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
            PKEY_VECTOR_SCORE_COLUMN,
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
}
