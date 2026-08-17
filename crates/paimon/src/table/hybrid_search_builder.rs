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

//! Hybrid search builder for combining multiple search routes.
//!
//! Reference: `org.apache.paimon.table.source.HybridSearchBuilder`.

use std::collections::{BTreeMap, HashMap};

use arrow_array::RecordBatch;
use futures::{stream, TryStreamExt};

use crate::spec::{CoreOptions, SCAN_SNAPSHOT_ID_OPTION};
use crate::table::data_file_reader::DataFileReader;
use crate::table::pk_search_position::PrimaryKeySearchPosition;
use crate::table::pk_search_ranker::{self, Ranking};
use crate::table::pk_vector_indexed_split_read::{PkVectorIndexedSplit, PkVectorIndexedSplitRead};
use crate::table::pk_vector_orchestrator::build_indexed_splits;
use crate::table::source::DataSplit;
use crate::table::vector_search_builder::{
    collect_ranked_rows, ensure_no_reserved_read_columns, reorder_and_strip_position, RankedRow,
};
use crate::table::{ArrowRecordBatchStream, RowRange, Table};
use crate::vector_search::SearchResult;

#[cfg(feature = "fulltext")]
use crate::spec::GlobalIndexSearchMode;
#[cfg(feature = "fulltext")]
use crate::table::find_field_id_by_name;
#[cfg(feature = "fulltext")]
use crate::table::pk_full_text_read::{build_full_text_indexed_splits, PrimaryKeyFullTextRead};
#[cfg(feature = "fulltext")]
use crate::table::pk_full_text_scan::PrimaryKeyFullTextScan;

const RRF_K: f32 = 60.0;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HybridSearchRanker {
    Rrf,
    WeightedScore,
    Mrr,
}

impl HybridSearchRanker {
    pub const RRF: &'static str = "rrf";
    pub const WEIGHTED_SCORE: &'static str = "weighted_score";
    pub const MRR: &'static str = "mrr";

    pub fn parse(ranker: &str) -> crate::Result<Self> {
        match ranker.trim().to_ascii_lowercase().as_str() {
            "" | Self::RRF => Ok(Self::Rrf),
            Self::WEIGHTED_SCORE => Ok(Self::WeightedScore),
            Self::MRR => Ok(Self::Mrr),
            _ => Err(crate::Error::ConfigInvalid {
                message: format!("Unsupported hybrid ranker: {ranker}"),
            }),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rrf => Self::RRF,
            Self::WeightedScore => Self::WEIGHTED_SCORE,
            Self::Mrr => Self::MRR,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HybridSearchRouteKind {
    Vector,
    FullText,
}

#[derive(Clone, Debug)]
pub struct HybridSearchRoute {
    kind: HybridSearchRouteKind,
    field_name: String,
    vector: Option<Vec<f32>>,
    full_text_query: Option<String>,
    limit: usize,
    weight: f32,
    options: HashMap<String, String>,
}

impl HybridSearchRoute {
    pub fn vector(
        field_name: impl Into<String>,
        vector: Vec<f32>,
        limit: usize,
        weight: f32,
        options: HashMap<String, String>,
    ) -> crate::Result<Self> {
        let field_name = field_name.into();
        Self::validate_common(&field_name, limit, weight)?;
        if vector.is_empty() {
            return Err(crate::Error::DataInvalid {
                message: "Search vector cannot be empty".to_string(),
                source: None,
            });
        }
        Ok(Self {
            kind: HybridSearchRouteKind::Vector,
            field_name,
            vector: Some(vector),
            full_text_query: None,
            limit,
            weight,
            options,
        })
    }

    pub fn full_text(
        field_name: impl Into<String>,
        query: impl Into<String>,
        limit: usize,
        weight: f32,
        options: HashMap<String, String>,
    ) -> crate::Result<Self> {
        if !options.is_empty() {
            return Err(crate::Error::ConfigInvalid {
                message: "Full-text hybrid route options are not supported yet".to_string(),
            });
        }

        let field_name = field_name.into();
        let query = query.into();
        Self::validate_common(&field_name, limit, weight)?;
        if query.is_empty() {
            return Err(crate::Error::ConfigInvalid {
                message: "Full-text route query cannot be empty".to_string(),
            });
        }

        Ok(Self {
            kind: HybridSearchRouteKind::FullText,
            field_name,
            vector: None,
            full_text_query: Some(query),
            limit,
            weight,
            options,
        })
    }

    fn validate_common(field_name: &str, limit: usize, weight: f32) -> crate::Result<()> {
        if field_name.is_empty() {
            return Err(crate::Error::DataInvalid {
                message: "Field name cannot be null or empty".to_string(),
                source: None,
            });
        }
        if limit == 0 {
            return Err(crate::Error::ConfigInvalid {
                message: "Limit must be positive".to_string(),
            });
        }
        if !weight.is_finite() || weight <= 0.0 {
            return Err(crate::Error::ConfigInvalid {
                message: format!("Weight must be finite and positive, got: {weight}"),
            });
        }
        Ok(())
    }

    pub fn kind(&self) -> HybridSearchRouteKind {
        self.kind
    }

    pub fn field_name(&self) -> &str {
        &self.field_name
    }

    pub fn vector_value(&self) -> Option<&[f32]> {
        self.vector.as_deref()
    }

    pub fn full_text_query(&self) -> Option<&str> {
        self.full_text_query.as_deref()
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn weight(&self) -> f32 {
        self.weight
    }

    pub fn options(&self) -> &HashMap<String, String> {
        &self.options
    }
}

pub struct HybridSearchBuilder<'a> {
    table: &'a Table,
    routes: Vec<HybridSearchRoute>,
    limit: Option<usize>,
    ranker: HybridSearchRanker,
}

impl<'a> HybridSearchBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            routes: Vec::new(),
            limit: None,
            ranker: HybridSearchRanker::Rrf,
        }
    }

    pub fn add_route(&mut self, route: HybridSearchRoute) -> &mut Self {
        self.routes.push(route);
        self
    }

    pub fn add_vector_route(
        &mut self,
        field_name: &str,
        vector: Vec<f32>,
        limit: usize,
        weight: f32,
        options: HashMap<String, String>,
    ) -> crate::Result<&mut Self> {
        self.routes.push(HybridSearchRoute::vector(
            field_name, vector, limit, weight, options,
        )?);
        Ok(self)
    }

    pub fn add_full_text_route(
        &mut self,
        field_name: &str,
        query: &str,
        limit: usize,
        weight: f32,
        options: HashMap<String, String>,
    ) -> crate::Result<&mut Self> {
        self.routes.push(HybridSearchRoute::full_text(
            field_name, query, limit, weight, options,
        )?);
        Ok(self)
    }

    pub fn with_limit(&mut self, limit: usize) -> &mut Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_ranker(&mut self, ranker: &str) -> crate::Result<&mut Self> {
        self.ranker = HybridSearchRanker::parse(ranker)?;
        Ok(self)
    }

    pub fn with_rrf_ranker(&mut self) -> &mut Self {
        self.ranker = HybridSearchRanker::Rrf;
        self
    }

    pub fn with_weighted_score_ranker(&mut self) -> &mut Self {
        self.ranker = HybridSearchRanker::WeightedScore;
        self
    }

    pub fn with_mrr_ranker(&mut self) -> &mut Self {
        self.ranker = HybridSearchRanker::Mrr;
        self
    }

    pub async fn execute(&self) -> crate::Result<Vec<RowRange>> {
        self.execute_scored().await?.to_row_ranges()
    }

    pub async fn execute_scored(&self) -> crate::Result<SearchResult> {
        let core = CoreOptions::new(self.table.schema().options());
        core.ensure_read_authorized()?;
        let limit = self.limit.ok_or_else(|| crate::Error::ConfigInvalid {
            message: "Limit must be set via with_limit()".to_string(),
        })?;
        if self.routes.is_empty() {
            return Err(crate::Error::ConfigInvalid {
                message: "Routes cannot be empty".to_string(),
            });
        }

        // A primary-key hybrid fuses PHYSICAL positions, not global row ids, so a
        // scored/row-range result is unsupported on it: fail loud and direct callers
        // to the materialized `execute_read`. A mixed PK/global set of routes cannot
        // be fused at all (different address spaces) and also fails loud. The
        // append/data-evolution global path is unchanged (mirrors Java `rank`).
        match self.classify_routes(&core)? {
            HybridAddressSpace::PrimaryKey => {
                return Err(crate::Error::DataInvalid {
                    message: "primary-key hybrid search does not produce global row ids; use the \
                              materialized read (execute_read) instead"
                        .to_string(),
                    source: None,
                });
            }
            HybridAddressSpace::Global => {}
        }

        let mut route_results = Vec::with_capacity(self.routes.len());
        for route in &self.routes {
            let result = match route.kind {
                HybridSearchRouteKind::Vector => {
                    let mut builder = self.table.new_vector_search_builder();
                    builder
                        .with_vector_column(&route.field_name)
                        .with_query_vector(route.vector.clone().expect("validated vector route"))
                        .with_limit(route.limit)
                        .with_options(route.options.clone());
                    builder.execute_scored().await?
                }
                HybridSearchRouteKind::FullText => {
                    execute_full_text_route(self.table, route).await?
                }
            };
            if !result.is_empty() {
                route_results.push(WeightedRouteResult {
                    result,
                    weight: route.weight,
                });
            }
        }

        Ok(rank_results(self.ranker, &route_results, limit))
    }

    /// Materialize the fused hybrid search hits into Arrow rows, best-fused-score
    /// first, with a `__paimon_search_score` column appended. Only the primary-key
    /// hybrid path can materialize rows: every route must resolve to a physical
    /// primary-key route (vector column configured in `pk-vector.index.columns`,
    /// full-text column in `pk-full-text.index.columns` with data-evolution
    /// disabled). A mixed PK/global set of routes fails loud, and an all-global
    /// (append/data-evolution) hybrid is unsupported here — those use
    /// `execute`/`execute_scored`. Mirrors Java `HybridSearchBuilderImpl` PK path.
    pub async fn execute_read(&self) -> crate::Result<ArrowRecordBatchStream> {
        let core = CoreOptions::new(self.table.schema().options());
        core.ensure_read_authorized()?;
        let limit = self.limit.ok_or_else(|| crate::Error::ConfigInvalid {
            message: "Limit must be set via with_limit()".to_string(),
        })?;
        if self.routes.is_empty() {
            return Err(crate::Error::ConfigInvalid {
                message: "Routes cannot be empty".to_string(),
            });
        }

        match self.classify_routes(&core)? {
            HybridAddressSpace::PrimaryKey => {
                self.execute_primary_key_hybrid_read(&core, limit).await
            }
            HybridAddressSpace::Global => Err(crate::Error::Unsupported {
                message: "materialized hybrid read (execute_read) is only supported on the \
                          primary-key hybrid path; use execute/execute_scored for the \
                          append/data-evolution path"
                    .to_string(),
            }),
        }
    }

    /// Classify the route set as an all-primary-key or an all-global hybrid,
    /// failing loud when the two address spaces are mixed (mirrors Java `rank`).
    fn classify_routes(&self, core: &CoreOptions<'_>) -> crate::Result<HybridAddressSpace> {
        let mut any_pk = false;
        let mut any_global = false;
        for route in &self.routes {
            if route_is_primary_key(core, route) {
                any_pk = true;
            } else {
                any_global = true;
            }
        }
        if any_pk && any_global {
            return Err(crate::Error::Unsupported {
                message: "Hybrid search cannot combine physical primary-key positions and global \
                          row-id address spaces."
                    .to_string(),
            });
        }
        Ok(if any_pk {
            HybridAddressSpace::PrimaryKey
        } else {
            HybridAddressSpace::Global
        })
    }

    /// The all-primary-key hybrid read: run each route's candidate producer against
    /// the ONE pinned snapshot, convert its candidates to shared physical positions,
    /// fuse them via the configured ranker (each route uses its own limit, the
    /// fusion uses the builder limit), select the physical source files the fused
    /// positions reference, build indexed splits carrying the FUSED raw scores,
    /// materialize, and reorder best-fused-first while stripping the internal
    /// position column.
    async fn execute_primary_key_hybrid_read(
        &self,
        core: &CoreOptions<'_>,
        limit: usize,
    ) -> crate::Result<ArrowRecordBatchStream> {
        // The materialized read projects every user column and then appends the
        // internal `_PKEY_VECTOR_POSITION` and `__paimon_search_score` columns; a
        // user column whose name collides with either (or with `_ROW_ID`) would be
        // shadowed by a positional lookup, corrupting the reorder/strip and the
        // fused score. Reject it up front — before any route runs and even when the
        // fused result is empty — reusing the primary-key vector read's guard.
        ensure_no_reserved_read_columns(self.table.schema().fields())?;

        // Snapshot pinning: resolve ONE snapshot for the whole primary-key hybrid
        // read and plan every route against it, mirroring Java
        // `HybridSearchBuilderImpl.routeBuilders()` (which resolves the snapshot
        // once and injects it into every route builder). Only the "read latest"
        // case is racy — a concurrent commit landing between the two route plans
        // would otherwise pick different snapshots — because every time-travel
        // selector resolves deterministically to the same snapshot on each plan.
        // So pin latest once via `scan.snapshot-id` and leave the already-pinned
        // (time-travel) paths to resolve their own fixed snapshot.
        let pinned_table = self.resolve_pinned_route_table().await?;
        let route_table: &Table = pinned_table.as_ref().unwrap_or(self.table);

        // Per-route search: positions (converted from candidates) + single-file
        // source splits + the snapshot each route's plan pinned.
        let mut routes: Vec<PkRoute> = Vec::with_capacity(self.routes.len());
        for route in &self.routes {
            let pk_route = match route.kind {
                HybridSearchRouteKind::Vector => {
                    self.pk_vector_route(route_table, core, route).await?
                }
                HybridSearchRouteKind::FullText => {
                    self.pk_full_text_route(route_table, core, route).await?
                }
            };
            routes.push(pk_route);
        }

        // Snapshot pinning: every route's plan must resolve the same snapshot, so
        // never fuse across versions (mirrors Java `rankPhysical`'s mismatch guard).
        // With a pinned snapshot above this is a defensive guard, not the primary
        // mechanism.
        check_single_snapshot(routes.iter().map(|r| r.snapshot_id))?;

        // Fuse the per-route rankings into a single best-first position list bounded
        // by the builder limit.
        let fused = self.fuse_positions(&routes, limit)?;
        if fused.is_empty() {
            return Ok(Box::pin(stream::empty()));
        }

        // Physical sources: collect the single-file source splits across routes,
        // validating cross-route metadata/DV consistency, then select only the files
        // the fused positions reference (mirrors Java `physicalSources`).
        let available = collect_physical_sources(&routes)?;

        // Rank each fused position by its best-first ordinal via its FULL physical
        // key so the file/position materialization order can be reduced back to
        // best-first.
        let mut rank_of: HashMap<(Vec<u8>, i32, String, i64), usize> = HashMap::new();
        for (rank, position) in fused.iter().enumerate() {
            rank_of.insert(
                (
                    position.partition().to_serialized_bytes(),
                    position.bucket(),
                    position.data_file_name().to_string(),
                    position.row_position(),
                ),
                rank,
            );
        }

        // Build the materialization splits DIRECTLY from the fused positions,
        // carrying the FUSED raw scores (no distance conversion): those are the
        // final relevance scores the output must expose.
        let indexed_splits = build_hybrid_indexed_splits(&fused, &available)?;

        // A predicate-free materialization reader projecting every user column; the
        // indexed-split read appends the score column itself.
        let materialize_reader = DataFileReader::new(
            self.table.file_io().clone(),
            self.table.schema_manager().clone(),
            self.table.schema().id(),
            self.table.schema().fields().to_vec(),
            self.table.schema().fields().to_vec(),
            Vec::new(),
        );

        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut ranked: Vec<RankedRow> = Vec::new();
        for indexed in indexed_splits {
            let partition_bytes = indexed.split.partition().to_serialized_bytes();
            let bucket = indexed.split.bucket();
            let file_name = indexed.split.data_files()[0].file_name.clone();
            let mut read_stream =
                PkVectorIndexedSplitRead::new(materialize_reader.clone()).read(&indexed)?;
            while let Some(batch) = read_stream.try_next().await? {
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

        let output = reorder_and_strip_position(&batches, ranked)?;
        Ok(Box::pin(stream::iter(output.into_iter().map(Ok))))
    }

    /// Resolve the ONE snapshot every primary-key route must plan against, as an
    /// optional pinned table copy. Mirrors Java
    /// `HybridSearchBuilderImpl.routeBuilders()`, which resolves a single snapshot
    /// up front and injects it into every route builder.
    ///
    /// A time-travel selector (`scan.version` / `scan.timestamp-millis` /
    /// `scan.snapshot-id` / `scan.tag-name`) already resolves deterministically to
    /// the same snapshot on every route plan, so no extra pinning is needed —
    /// return `None` and let each route resolve it. Only the default "read latest"
    /// path is racy: a concurrent commit landing between two route plans would let
    /// them resolve different snapshots. For that path resolve the latest snapshot
    /// id ONCE and return a table copy pinned to it via `scan.snapshot-id`, so both
    /// routes plan the same version. A table with no snapshot at all also returns
    /// `None` (nothing to pin; every route plans empty).
    async fn resolve_pinned_route_table(&self) -> crate::Result<Option<Table>> {
        let core = CoreOptions::new(self.table.schema().options());
        // Already targeting a fixed snapshot (resolved travel copy or a selector
        // that resolves deterministically): every route agrees without pinning.
        if self.table.has_resolved_travel_snapshot() || core.try_time_travel_selector()?.is_some() {
            return Ok(None);
        }
        // Read-latest: pin the current latest snapshot once so a concurrent commit
        // cannot split the routes across versions.
        let Some(latest) = self
            .table
            .snapshot_manager()
            .get_latest_snapshot_id()
            .await?
        else {
            return Ok(None);
        };
        let pinned = self.table.copy_with_options(HashMap::from([(
            SCAN_SNAPSHOT_ID_OPTION.to_string(),
            latest.to_string(),
        )]));
        Ok(Some(pinned))
    }

    /// Run the vector route's primary-key candidate producer and convert its hits
    /// into shared physical positions (distance → score via the resolved metric),
    /// keeping the route's single-file source splits and pinned snapshot.
    async fn pk_vector_route(
        &self,
        table: &Table,
        core: &CoreOptions<'_>,
        route: &HybridSearchRoute,
    ) -> crate::Result<PkRoute> {
        let vector = route.vector.as_deref().expect("validated vector route");
        let mut builder = table.new_vector_search_builder();
        builder
            .with_vector_column(&route.field_name)
            .with_query_vector(vector.to_vec())
            .with_limit(route.limit)
            .with_options(route.options.clone());
        let result = builder
            .search_pk_route(core, &route.field_name, vector, route.limit)
            .await?;
        let positions = result
            .candidates
            .iter()
            .map(|candidate| {
                PrimaryKeySearchPosition::from_vector_candidate(candidate, result.metric)
            })
            .collect::<crate::Result<Vec<_>>>()?;
        let source_splits = build_indexed_splits(result.candidates, &result.splits, result.metric)?
            .into_iter()
            .map(|split| split.split)
            .collect();
        Ok(PkRoute {
            positions,
            source_splits,
            snapshot_id: result.snapshot_id,
            weight: route.weight as f64,
        })
    }

    #[cfg(feature = "fulltext")]
    async fn pk_full_text_route(
        &self,
        table: &Table,
        core: &CoreOptions<'_>,
        route: &HybridSearchRoute,
    ) -> crate::Result<PkRoute> {
        // FAST-only: the primary-key full-text read searches compaction-visible
        // payloads and rejects FULL/DETAIL loud rather than silently degrading,
        // mirroring `full_text_search_builder::execute_read` and Java
        // `PrimaryKeyFullTextRead.checkFastSearchMode`.
        if core.full_text_index_search_mode()? != GlobalIndexSearchMode::Fast {
            return Err(crate::Error::DataInvalid {
                message: "primary-key full-text search supports only the FAST global-index search \
                          mode"
                    .to_string(),
                source: None,
            });
        }

        let query = route
            .full_text_query
            .as_deref()
            .expect("validated full-text route");
        let field_id = find_field_id_by_name(table.schema().fields(), &route.field_name)
            .ok_or_else(|| crate::Error::DataInvalid {
                message: format!(
                    "full-text search column '{}' does not exist",
                    route.field_name
                ),
                source: None,
            })?;
        let plan = PrimaryKeyFullTextScan::new(table, field_id, None)
            .plan()
            .await?;
        let materialize_reader = DataFileReader::new(
            table.file_io().clone(),
            table.schema_manager().clone(),
            table.schema().id(),
            table.schema().fields().to_vec(),
            table.schema().fields().to_vec(),
            Vec::new(),
        );
        let read = PrimaryKeyFullTextRead::new(
            table.file_io().clone(),
            materialize_reader,
            table.location().trim_end_matches('/').to_string(),
        );
        let result = read.search_route(&plan, query, route.limit).await?;
        let positions = result
            .candidates
            .iter()
            .map(PrimaryKeySearchPosition::from_full_text_candidate)
            .collect::<crate::Result<Vec<_>>>()?;
        let source_splits = build_full_text_indexed_splits(result.candidates, result.splits)?
            .into_iter()
            .map(|split| split.split)
            .collect();
        Ok(PkRoute {
            positions,
            source_splits,
            snapshot_id: result.snapshot_id,
            weight: route.weight as f64,
        })
    }

    #[cfg(not(feature = "fulltext"))]
    async fn pk_full_text_route(
        &self,
        _table: &Table,
        _core: &CoreOptions<'_>,
        _route: &HybridSearchRoute,
    ) -> crate::Result<PkRoute> {
        Err(crate::Error::ConfigInvalid {
            message: "primary-key full-text hybrid routes require the fulltext feature".to_string(),
        })
    }

    /// Fuse the per-route physical rankings via the configured ranker. Each route
    /// contributes a weighted ranking only when it has positions (mirrors Java
    /// `rankPhysical`), and the fusion is bounded to the builder limit.
    fn fuse_positions(
        &self,
        routes: &[PkRoute],
        limit: usize,
    ) -> crate::Result<Vec<PrimaryKeySearchPosition>> {
        let mut rankings = Vec::with_capacity(routes.len());
        for route in routes {
            if !route.positions.is_empty() {
                rankings.push(Ranking::new(route.positions.clone(), route.weight)?);
            }
        }
        match self.ranker {
            HybridSearchRanker::Rrf => pk_search_ranker::weighted_rrf(&rankings, limit),
            HybridSearchRanker::WeightedScore => pk_search_ranker::weighted_score(&rankings, limit),
            HybridSearchRanker::Mrr => pk_search_ranker::weighted_mrr(&rankings, limit),
        }
    }
}

/// One primary-key route's fusion input: its converted physical positions, the
/// single-file source splits its hits reference (for physical-sources
/// materialization), the snapshot its plan pinned, and its fusion weight.
struct PkRoute {
    positions: Vec<PrimaryKeySearchPosition>,
    source_splits: Vec<DataSplit>,
    snapshot_id: i64,
    weight: f64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HybridAddressSpace {
    PrimaryKey,
    Global,
}

/// Whether a route resolves to a physical primary-key route. Vector: the queried
/// column is a configured `pk-vector.index.columns` entry (membership via the
/// non-erroring accessor so a malformed config cannot misclassify an unrelated
/// route). Full-text: data-evolution disabled AND the column configured in
/// `pk-full-text.index.columns` (only when the fulltext feature is built).
fn route_is_primary_key(core: &CoreOptions<'_>, route: &HybridSearchRoute) -> bool {
    match route.kind {
        HybridSearchRouteKind::Vector => {
            core.primary_key_vector_index_enabled()
                && core
                    .primary_key_vector_index_columns()
                    .ok()
                    .is_some_and(|cols| cols.iter().any(|c| c == route.field_name()))
        }
        HybridSearchRouteKind::FullText => {
            #[cfg(feature = "fulltext")]
            {
                !core.data_evolution_enabled()
                    && core
                        .primary_key_full_text_index_columns()
                        .iter()
                        .any(|c| c == route.field_name())
            }
            #[cfg(not(feature = "fulltext"))]
            {
                let _ = core;
                false
            }
        }
    }
}

/// Assert every primary-key route pinned the same snapshot, returning it. Mirrors
/// the `checkArgument` in Java `rankPhysical`: fusing across snapshots would
/// search/materialize physical rows against inconsistent versions.
fn check_single_snapshot(
    snapshot_ids: impl IntoIterator<Item = i64>,
) -> crate::Result<Option<i64>> {
    let mut pinned: Option<i64> = None;
    for snapshot_id in snapshot_ids {
        match pinned {
            None => pinned = Some(snapshot_id),
            Some(existing) if existing != snapshot_id => {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "primary-key hybrid routes must use the same snapshot, but found {existing} and {snapshot_id}"
                    ),
                    source: None,
                });
            }
            _ => {}
        }
    }
    Ok(pinned)
}

/// Physical file identity for the physical-sources map: `(partition bytes, bucket,
/// data file name)`.
type PhysicalFileKey = (Vec<u8>, i32, String);

/// Collect the single-file source splits across all routes into a map keyed by
/// physical file identity, validating that a file appearing in more than one route
/// carries consistent metadata and deletion-file state. Mirrors Java
/// `physicalSources`'s duplicate-consistency guard.
fn collect_physical_sources(
    routes: &[PkRoute],
) -> crate::Result<HashMap<PhysicalFileKey, DataSplit>> {
    let mut available: HashMap<PhysicalFileKey, DataSplit> = HashMap::new();
    for route in routes {
        for split in &route.source_splits {
            if split.data_files().len() != 1 {
                return Err(crate::Error::DataInvalid {
                    message: "primary-key scored source split must contain exactly one data file"
                        .to_string(),
                    source: None,
                });
            }
            let file = &split.data_files()[0];
            let key = (
                split.partition().to_serialized_bytes(),
                split.bucket(),
                file.file_name.clone(),
            );
            match available.get(&key) {
                None => {
                    available.insert(key, split.clone());
                }
                Some(previous) => {
                    let prev_file = &previous.data_files()[0];
                    let prev_dv = previous
                        .data_deletion_files()
                        .and_then(|dfs| dfs.first().cloned().flatten());
                    let cur_dv = split
                        .data_deletion_files()
                        .and_then(|dfs| dfs.first().cloned().flatten());
                    let consistent = previous.snapshot_id() == split.snapshot_id()
                        && previous.bucket_path() == split.bucket_path()
                        && previous.total_buckets() == split.total_buckets()
                        && prev_file.file_size == file.file_size
                        && prev_file.row_count == file.row_count
                        && prev_dv == cur_dv;
                    if !consistent {
                        return Err(crate::Error::DataInvalid {
                            message: format!(
                                "primary-key hybrid routes contain inconsistent metadata for data file {}",
                                file.file_name
                            ),
                            source: None,
                        });
                    }
                }
            }
        }
    }
    Ok(available)
}

/// Group the fused positions by physical file, look up each file's single-file
/// source split, and build one `PkVectorIndexedSplit` per file carrying the FUSED
/// raw scores aligned to ascending position order. A fused position referencing a
/// file no route sourced fails loud (mirrors Java `physicalSources`'s selection).
fn build_hybrid_indexed_splits(
    fused: &[PrimaryKeySearchPosition],
    available: &HashMap<PhysicalFileKey, DataSplit>,
) -> crate::Result<Vec<PkVectorIndexedSplit>> {
    // BTreeMap keeps a deterministic ascending group order. Value: (position, score).
    let mut groups: BTreeMap<PhysicalFileKey, Vec<(i64, f32)>> = BTreeMap::new();
    for position in fused {
        let key = (
            position.partition().to_serialized_bytes(),
            position.bucket(),
            position.data_file_name().to_string(),
        );
        groups
            .entry(key)
            .or_default()
            .push((position.row_position(), position.score()));
    }

    let mut out = Vec::with_capacity(groups.len());
    for (key, mut hits) in groups {
        let source = available
            .get(&key)
            .ok_or_else(|| crate::Error::DataInvalid {
                message: format!(
                    "primary-key hybrid result references unknown data file {}",
                    key.2
                ),
                source: None,
            })?;
        // Fused positions are physically unique, so no duplicate position within a
        // file; sort ascending and coalesce into inclusive ranges with aligned
        // fused scores.
        hits.sort_by_key(|(pos, _)| *pos);
        let mut row_ranges: Vec<RowRange> = Vec::new();
        let mut scores: Vec<f32> = Vec::with_capacity(hits.len());
        let mut start = hits[0].0;
        let mut end = hits[0].0;
        scores.push(hits[0].1);
        for &(pos, score) in &hits[1..] {
            if pos == end + 1 {
                end = pos;
            } else {
                row_ranges.push(RowRange::new(start, end));
                start = pos;
                end = pos;
            }
            scores.push(score);
        }
        row_ranges.push(RowRange::new(start, end));

        out.push(PkVectorIndexedSplit {
            split: source.clone(),
            row_ranges,
            scores: Some(scores),
        });
    }
    Ok(out)
}

#[cfg(feature = "fulltext")]
async fn execute_full_text_route(
    table: &Table,
    route: &HybridSearchRoute,
) -> crate::Result<SearchResult> {
    let mut builder = table.new_full_text_search_builder();
    builder
        .with_text_column(&route.field_name)
        .with_query_text(
            route
                .full_text_query
                .as_deref()
                .expect("validated full-text route"),
        )
        .with_limit(route.limit);
    let result = builder.execute_scored().await?;
    Ok(SearchResult::new(result.row_ids, result.scores))
}

#[cfg(not(feature = "fulltext"))]
async fn execute_full_text_route(
    _table: &Table,
    _route: &HybridSearchRoute,
) -> crate::Result<SearchResult> {
    Err(crate::Error::ConfigInvalid {
        message: "Full-text hybrid routes require the fulltext feature".to_string(),
    })
}

struct WeightedRouteResult {
    result: SearchResult,
    weight: f32,
}

fn rank_results(
    ranker: HybridSearchRanker,
    route_results: &[WeightedRouteResult],
    limit: usize,
) -> SearchResult {
    match ranker {
        HybridSearchRanker::Rrf => rrf(route_results, limit),
        HybridSearchRanker::WeightedScore => weighted_score(route_results, limit),
        HybridSearchRanker::Mrr => mrr(route_results, limit),
    }
}

fn rrf(route_results: &[WeightedRouteResult], limit: usize) -> SearchResult {
    let mut scores = HashMap::new();
    for route_result in route_results {
        for (rank, (row_id, _score)) in ranked_row_ids(&route_result.result).iter().enumerate() {
            let contribution = route_result.weight / (RRF_K + rank as f32 + 1.0);
            add_score(&mut scores, *row_id, contribution);
        }
    }
    top_k(scores, limit)
}

fn mrr(route_results: &[WeightedRouteResult], limit: usize) -> SearchResult {
    let mut scores = HashMap::new();
    for route_result in route_results {
        for (rank, (row_id, _score)) in ranked_row_ids(&route_result.result).iter().enumerate() {
            let contribution = route_result.weight / (rank as f32 + 1.0);
            add_score(&mut scores, *row_id, contribution);
        }
    }
    top_k(scores, limit)
}

fn weighted_score(route_results: &[WeightedRouteResult], limit: usize) -> SearchResult {
    let mut scores = HashMap::new();
    for route_result in route_results {
        let ranked = ranked_row_ids(&route_result.result);
        if ranked.is_empty() {
            continue;
        }

        let (mut min, mut max) = (f32::INFINITY, f32::NEG_INFINITY);
        for (_row_id, score) in &ranked {
            min = min.min(*score);
            max = max.max(*score);
        }
        let range = max - min;

        for (row_id, score) in ranked {
            let normalized = if range > 0.0 {
                (score - min) / range
            } else {
                1.0
            };
            add_score(&mut scores, row_id, route_result.weight * normalized);
        }
    }
    top_k(scores, limit)
}

fn ranked_row_ids(result: &SearchResult) -> Vec<(u64, f32)> {
    let mut best_scores = HashMap::new();
    for (&row_id, &score) in result.row_ids.iter().zip(&result.scores) {
        best_scores
            .entry(row_id)
            .and_modify(|old: &mut f32| {
                if score > *old {
                    *old = score;
                }
            })
            .or_insert(score);
    }

    let mut ranked: Vec<_> = best_scores.into_iter().collect();
    ranked.sort_by(|(left_id, left_score), (right_id, right_score)| {
        right_score
            .partial_cmp(left_score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left_id.cmp(right_id))
    });
    ranked
}

fn add_score(scores: &mut HashMap<u64, f32>, row_id: u64, score: f32) {
    scores
        .entry(row_id)
        .and_modify(|old_score| *old_score += score)
        .or_insert(score);
}

fn top_k(scores: HashMap<u64, f32>, limit: usize) -> SearchResult {
    if scores.is_empty() || limit == 0 {
        return SearchResult::empty();
    }

    let mut entries: Vec<_> = scores.into_iter().collect();
    entries.sort_by(|(left_id, left_score), (right_id, right_score)| {
        right_score
            .partial_cmp(left_score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left_id.cmp(right_id))
    });
    entries.truncate(limit);

    let (row_ids, scores): (Vec<_>, Vec<_>) = entries.into_iter().unzip();
    SearchResult::new(row_ids, scores)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route_result(row_ids: Vec<u64>, scores: Vec<f32>, weight: f32) -> WeightedRouteResult {
        WeightedRouteResult {
            result: SearchResult::new(row_ids, scores),
            weight,
        }
    }

    #[test]
    fn test_rrf_prefers_overlap() {
        let ranked = rank_results(
            HybridSearchRanker::Rrf,
            &[
                route_result(vec![1, 2], vec![0.9, 0.8], 1.0),
                route_result(vec![2, 3], vec![0.95, 0.1], 1.0),
            ],
            1,
        );

        assert_eq!(ranked.row_ids, vec![2]);
    }

    #[test]
    fn test_weighted_score_min_max_normalizes_per_route() {
        let ranked = rank_results(
            HybridSearchRanker::WeightedScore,
            &[
                route_result(vec![1, 2, 3], vec![10.0, 5.0, 0.0], 2.0),
                route_result(vec![1, 2, 3], vec![100.0, 50.0, 0.0], 1.0),
            ],
            3,
        );

        let scores: HashMap<_, _> = ranked.row_ids.into_iter().zip(ranked.scores).collect();
        assert!((scores[&1] - 3.0).abs() < 1e-6);
        assert!((scores[&2] - 1.5).abs() < 1e-6);
        assert!((scores[&3] - 0.0).abs() < 1e-6);
    }

    #[test]
    fn test_mrr_uses_reciprocal_rank_without_constant() {
        let ranked = rank_results(
            HybridSearchRanker::Mrr,
            &[
                route_result(vec![1, 2], vec![0.9, 0.8], 1.0),
                route_result(vec![2, 3], vec![0.95, 0.1], 1.0),
            ],
            2,
        );

        assert_eq!(ranked.row_ids[0], 2);
        assert!(ranked.scores[0] > ranked.scores[1]);
    }

    // (b) Snapshot pinning: routes that pinned different snapshots must fail loud
    // (mirror Java `rankPhysical`'s mismatch guard); equal snapshots are accepted.
    #[test]
    fn check_single_snapshot_rejects_mismatch() {
        assert_eq!(check_single_snapshot(std::iter::empty()).unwrap(), None);
        assert_eq!(check_single_snapshot([7, 7, 7]).unwrap(), Some(7));
        let err = check_single_snapshot([7, 8]).unwrap_err();
        assert!(
            format!("{err:?}").contains("same snapshot"),
            "snapshot mismatch must fail loud, got: {err:?}"
        );
    }
}

#[cfg(all(test, feature = "fulltext"))]
mod pk_hybrid_tests {
    use super::*;
    use crate::catalog::Identifier;
    use crate::io::{FileIO, FileIOBuilder};
    use crate::spec::{
        DataFileMeta, DataType, FloatType, GlobalIndexMeta, IndexFileMeta, IntType, Schema,
        TableSchema, VarCharType, VectorType,
    };
    use crate::table::pk_full_text_bucket_state::PK_FULL_TEXT_INDEX_TYPE;
    use crate::table::pk_vector_position_read::{PKEY_VECTOR_POSITION_COLUMN, SEARCH_SCORE_COLUMN};
    use crate::table::schema_manager::SchemaManager;
    use crate::table::{CommitMessage, Table, TableCommit};
    use arrow_array::{
        builder::{FixedSizeListBuilder, Float32Builder},
        Array, ArrayRef, Float32Array, Int32Array, RecordBatch, StringArray,
    };
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use bytes::Bytes;
    use paimon_ftindex_core::io::PosWriter as FtPosWriter;
    use paimon_ftindex_core::{FullTextIndexConfig, FullTextIndexWriter};
    use paimon_vindex_core::index::{VectorIndexConfig, VectorIndexTrainer, VectorIndexWriter};
    use paimon_vindex_core::io::PosWriter as VindexPosWriter;
    use std::collections::HashMap as StdHashMap;
    use std::sync::Arc;

    const DIM: usize = 4;
    const VECTOR_COLUMN: &str = "embedding";
    const TEXT_COLUMN: &str = "body";
    const VECTOR_INDEX_TYPE: &str = "ivf-flat";

    /// Table options routing BOTH a vector column and a text column to the
    /// primary-key physical read paths (data-evolution left off).
    fn table_options() -> Vec<(String, String)> {
        vec![
            ("bucket".to_string(), "1".to_string()),
            ("deletion-vectors.enabled".to_string(), "true".to_string()),
            (
                "pk-vector.index.columns".to_string(),
                VECTOR_COLUMN.to_string(),
            ),
            (
                format!("fields.{VECTOR_COLUMN}.pk-vector.index.type"),
                VECTOR_INDEX_TYPE.to_string(),
            ),
            (
                format!("fields.{VECTOR_COLUMN}.pk-vector.distance.metric"),
                "l2".to_string(),
            ),
            (
                "pk-full-text.index.columns".to_string(),
                TEXT_COLUMN.to_string(),
            ),
        ]
    }

    fn pk_schema(extra: &[(&str, &str)]) -> TableSchema {
        let mut builder = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column(
                VECTOR_COLUMN,
                DataType::Vector(
                    VectorType::try_new(true, DIM as u32, DataType::Float(FloatType::new()))
                        .unwrap(),
                ),
            )
            .column(TEXT_COLUMN, DataType::VarChar(VarCharType::string_type()))
            .primary_key(["id"]);
        for (k, v) in table_options() {
            builder = builder.option(k, v);
        }
        for (k, v) in extra {
            builder = builder.option(*k, *v);
        }
        TableSchema::new(0, &builder.build().unwrap())
    }

    fn data_batch(ids: &[i32], vectors: &[[f32; DIM]], texts: &[&str]) -> RecordBatch {
        let element_field = Arc::new(ArrowField::new("element", ArrowDataType::Float32, true));
        let mut vector_builder = FixedSizeListBuilder::new(Float32Builder::new(), DIM as i32)
            .with_field(element_field.clone());
        for vector in vectors {
            for &value in vector {
                vector_builder.values().append_value(value);
            }
            vector_builder.append(true);
        }
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new(
                VECTOR_COLUMN,
                ArrowDataType::FixedSizeList(element_field, DIM as i32),
                true,
            ),
            ArrowField::new(TEXT_COLUMN, ArrowDataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids.to_vec())) as ArrayRef,
                Arc::new(vector_builder.finish()) as ArrayRef,
                Arc::new(StringArray::from(texts.to_vec())) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn java_write_utf(s: &str) -> Vec<u8> {
        let mut body = Vec::new();
        for c in s.encode_utf16() {
            if (0x0001..=0x007F).contains(&c) {
                body.push(c as u8);
            } else if c > 0x07FF {
                body.push(0xE0 | (c >> 12) as u8);
                body.push(0x80 | ((c >> 6) & 0x3F) as u8);
                body.push(0x80 | (c & 0x3F) as u8);
            } else {
                body.push(0xC0 | (c >> 6) as u8);
                body.push(0x80 | (c & 0x3F) as u8);
            }
        }
        let mut out = (body.len() as u16).to_be_bytes().to_vec();
        out.extend_from_slice(&body);
        out
    }

    fn source_meta_bytes(data_level: i32, files: &[(&str, i64)]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&1i32.to_be_bytes());
        out.extend_from_slice(&data_level.to_be_bytes());
        out.extend_from_slice(&(files.len() as i32).to_be_bytes());
        for (name, rows) in files {
            out.extend_from_slice(&java_write_utf(name));
            out.extend_from_slice(&rows.to_be_bytes());
        }
        out
    }

    async fn write_bytes(file_io: &FileIO, path: &str, bytes: Vec<u8>) {
        file_io
            .new_output(path)
            .unwrap()
            .write(Bytes::from(bytes))
            .await
            .unwrap();
    }

    /// Build a real vindex IVF-flat ANN segment over `vectors` (label == physical
    /// position); `nlist = 1` keeps the search exact.
    async fn write_ann_segment(
        file_io: &FileIO,
        location: &str,
        file_name: &str,
        vectors: &[[f32; DIM]],
    ) -> u64 {
        let n = vectors.len();
        let flat: Vec<f32> = vectors.iter().flat_map(|v| v.iter().copied()).collect();
        let ids: Vec<i64> = (0..n as i64).collect();
        let native_options = StdHashMap::from([
            ("index.type".to_string(), "ivf_flat".to_string()),
            ("dimension".to_string(), DIM.to_string()),
            ("nlist".to_string(), "1".to_string()),
            ("metric".to_string(), "l2".to_string()),
        ]);
        let config = VectorIndexConfig::from_options(&native_options).unwrap();
        let training = VectorIndexTrainer::train(config, &flat, n).unwrap();
        let mut writer = VectorIndexWriter::new(training);
        writer.add_vectors(&ids, &flat, n).unwrap();
        let mut bytes = Vec::new();
        {
            let mut output = VindexPosWriter::new(&mut bytes);
            writer.write(&mut output).unwrap();
        }
        let file_size = bytes.len() as u64;
        write_bytes(file_io, &format!("{location}/index/{file_name}"), bytes).await;
        file_size
    }

    /// Build a full-text archive (row id == physical position) via the native core.
    fn build_archive(docs: &[(i64, &str)]) -> Vec<u8> {
        let mut writer = FullTextIndexWriter::new(FullTextIndexConfig::new()).unwrap();
        for (row_id, text) in docs {
            writer.add_document(*row_id, (*text).to_string()).unwrap();
        }
        let mut out = FtPosWriter::new(Vec::<u8>::new());
        writer.write(&mut out).unwrap();
        out.into_inner()
    }

    async fn open_table(file_io: &FileIO, location: &str) -> Table {
        let schema = SchemaManager::new(file_io.clone(), location.to_string())
            .latest()
            .await
            .expect("failed to list schemas")
            .expect("table has no schema");
        Table::new(
            file_io.clone(),
            Identifier::new("default", "pk_hybrid"),
            location.to_string(),
            (*schema).clone(),
            None,
        )
    }

    /// Build a complete self-contained primary-key hybrid table on an in-memory
    /// FileIO: persist the schema, write a real data file, build+commit a real
    /// vindex ANN segment AND a full-text archive over the SAME compacted data
    /// file. Returns the opened table.
    async fn build_hybrid_table(
        location: &str,
        ids: &[i32],
        vectors: &[[f32; DIM]],
        texts: &[&str],
        extra_options: &[(&str, &str)],
    ) -> Table {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        for dir in ["schema", "snapshot", "manifest", "index"] {
            file_io.mkdirs(&format!("{location}/{dir}")).await.unwrap();
        }
        let schema = pk_schema(extra_options);
        write_bytes(
            &file_io,
            &format!("{location}/schema/schema-{}", schema.id()),
            serde_json::to_vec(&schema).unwrap(),
        )
        .await;

        let table = open_table(&file_io, location).await;

        // Write a real data file via the public write path.
        let write_builder = table.new_write_builder();
        let mut writer = write_builder.new_write().unwrap();
        writer
            .write_arrow_batch(&data_batch(ids, vectors, texts))
            .await
            .unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        assert_eq!(messages.len(), 1, "single bucket -> one write message");
        let written = &messages[0];
        assert_eq!(written.new_files.len(), 1, "single data file expected");
        let base_meta = written.new_files[0].clone();
        let bucket = written.bucket;
        let partition = written.partition.clone();
        let data_file_name = base_meta.file_name.clone();
        let row_count = base_meta.row_count;

        // Only a compacted, non-level-0 file backs the primary-key indices.
        let indexed_meta = DataFileMeta {
            level: 1,
            file_source: Some(1),
            first_row_id: Some(0),
            ..base_meta
        };

        let vector_field_id = schema
            .fields()
            .iter()
            .find(|f| f.name() == VECTOR_COLUMN)
            .unwrap()
            .id();
        let text_field_id = schema
            .fields()
            .iter()
            .find(|f| f.name() == TEXT_COLUMN)
            .unwrap()
            .id();

        // Vector ANN segment.
        let vector_index_name = "vector-ivf-flat-pk-hybrid.index".to_string();
        let vector_index_size =
            write_ann_segment(&file_io, location, &vector_index_name, vectors).await;
        let vector_index = IndexFileMeta {
            index_type: VECTOR_INDEX_TYPE.to_string(),
            file_name: vector_index_name,
            file_size: i64::try_from(vector_index_size).unwrap(),
            row_count: i32::try_from(row_count).unwrap(),
            deletion_vectors_ranges: None,
            global_index_meta: Some(GlobalIndexMeta {
                row_range_start: 0,
                row_range_end: row_count - 1,
                index_field_id: vector_field_id,
                extra_field_ids: None,
                source_meta: Some(source_meta_bytes(
                    indexed_meta.level,
                    &[(&data_file_name, row_count)],
                )),
                index_meta: None,
            }),
        };

        // Full-text archive (row id == physical position).
        let ft_index_name = "full-text-pk-hybrid.index".to_string();
        let docs: Vec<(i64, &str)> = texts
            .iter()
            .enumerate()
            .map(|(pos, text)| (pos as i64, *text))
            .collect();
        let archive = build_archive(&docs);
        write_bytes(
            &file_io,
            &format!("{location}/index/{ft_index_name}"),
            archive,
        )
        .await;
        let ft_index = IndexFileMeta {
            index_type: PK_FULL_TEXT_INDEX_TYPE.to_string(),
            file_name: ft_index_name,
            file_size: 1,
            row_count: i32::try_from(row_count).unwrap(),
            deletion_vectors_ranges: None,
            global_index_meta: Some(GlobalIndexMeta {
                row_range_start: 0,
                row_range_end: row_count - 1,
                index_field_id: text_field_id,
                extra_field_ids: None,
                source_meta: Some(source_meta_bytes(1, &[(&data_file_name, row_count)])),
                index_meta: None,
            }),
        };

        let mut message = CommitMessage::new(partition, bucket, vec![indexed_meta]);
        message.new_index_files = vec![vector_index, ft_index];
        TableCommit::new(table.clone(), "pk-hybrid".to_string())
            .commit(vec![message])
            .await
            .unwrap();

        table
    }

    fn column_i32(batches: &[RecordBatch], name: &str) -> Vec<i32> {
        batches
            .iter()
            .flat_map(|b| {
                let idx = b.schema().index_of(name).unwrap();
                b.column(idx)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    fn column_f32(batches: &[RecordBatch], name: &str) -> Vec<f32> {
        batches
            .iter()
            .flat_map(|b| {
                let idx = b.schema().index_of(name).unwrap();
                b.column(idx)
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    // (a) End-to-end PK hybrid: fuse a vector route and a full-text route, best
    // fused first, with the score column and without the internal position column.
    // Data is chosen so the fused order differs from BOTH route-only orders,
    // proving the ranker actually combines the two routes.
    #[tokio::test]
    async fn end_to_end_pk_hybrid_fuses_vector_and_full_text() {
        // Vector nearest -> pos0,1,2,3 (query [10,0,0,0]).
        let vectors = vec![
            [10.0, 0.0, 0.0, 0.0], // pos0 dist 0
            [9.0, 0.0, 0.0, 0.0],  // pos1 dist 1
            [8.0, 0.0, 0.0, 0.0],  // pos2 dist 4
            [7.0, 0.0, 0.0, 0.0],  // pos3 dist 9
        ];
        // Full-text "alpha" -> pos2 (tf 3) then pos0 (tf 1).
        let texts = vec!["alpha", "beta", "alpha alpha alpha", "gamma"];
        let ids = [100, 101, 102, 103];
        let table = build_hybrid_table("memory:/pk_hybrid_e2e", &ids, &vectors, &texts, &[]).await;

        let mut builder = table.new_hybrid_search_builder();
        builder
            .add_vector_route(
                VECTOR_COLUMN,
                vec![10.0, 0.0, 0.0, 0.0],
                4,
                1.0,
                HashMap::new(),
            )
            .unwrap()
            .add_full_text_route(
                TEXT_COLUMN,
                r#"{"match":{"query":"alpha"}}"#,
                4,
                1.0,
                HashMap::new(),
            )
            .unwrap();
        builder.with_limit(3).with_rrf_ranker();

        let batches = builder
            .execute_read()
            .await
            .expect("pk hybrid execute_read failed")
            .try_collect::<Vec<_>>()
            .await
            .expect("collecting hybrid read batches failed");

        // RRF: pos0 = 1/61 + 1/62 (vec r1, ft r2); pos2 = 1/63 + 1/61 (vec r3, ft r1);
        // pos1 = 1/62 (vec r2). Fused best-first: 100, 102, 101. This differs from
        // the vector-only order (100,101,102) and the full-text-only order (102,100),
        // so it can only come from fusing BOTH routes.
        let ids_out = column_i32(&batches, "id");
        assert_eq!(ids_out, vec![100, 102, 101], "fused best-first order");

        // The unified score column is present, descending (best first); the internal
        // position column is stripped.
        let scores = column_f32(&batches, SEARCH_SCORE_COLUMN);
        assert_eq!(scores.len(), 3);
        assert!(
            scores[0] >= scores[1] && scores[1] >= scores[2],
            "scores must be best-first: {scores:?}"
        );
        for batch in &batches {
            assert!(
                batch
                    .schema()
                    .index_of(PKEY_VECTOR_POSITION_COLUMN)
                    .is_err(),
                "internal position column must be stripped"
            );
            assert!(
                batch.schema().index_of(SEARCH_SCORE_COLUMN).is_ok(),
                "score column must be present"
            );
        }
    }

    // (c) A mixed PK/global route set must fail loud on execute_read.
    #[tokio::test]
    async fn mixed_pk_and_global_routes_fail_loud() {
        let table = build_hybrid_table(
            "memory:/pk_hybrid_mixed",
            &[100, 101],
            &[[10.0, 0.0, 0.0, 0.0], [9.0, 0.0, 0.0, 0.0]],
            &["alpha", "beta"],
            &[],
        )
        .await;

        // The vector route is a PK route; the full-text route targets a column NOT
        // configured for PK full-text -> global address space -> mixed -> fail loud.
        let mut builder = table.new_hybrid_search_builder();
        builder
            .add_vector_route(
                VECTOR_COLUMN,
                vec![10.0, 0.0, 0.0, 0.0],
                2,
                1.0,
                HashMap::new(),
            )
            .unwrap()
            .add_full_text_route(
                "not_indexed",
                r#"{"match":{"query":"alpha"}}"#,
                2,
                1.0,
                HashMap::new(),
            )
            .unwrap();
        builder.with_limit(2);

        let err = match builder.execute_read().await {
            Ok(_) => panic!("mixed PK/global routes must fail loud"),
            Err(e) => e,
        };
        assert!(
            format!("{err:?}").contains("address spaces"),
            "mixed PK/global must fail loud, got: {err:?}"
        );
    }

    // (d) The PK hybrid path produces physical positions, not global row ids:
    // execute / execute_scored must fail loud and point at execute_read.
    #[tokio::test]
    async fn pk_hybrid_execute_and_scored_fail_loud() {
        let table = build_hybrid_table(
            "memory:/pk_hybrid_guard",
            &[100, 101],
            &[[10.0, 0.0, 0.0, 0.0], [9.0, 0.0, 0.0, 0.0]],
            &["alpha", "beta"],
            &[],
        )
        .await;

        let mut builder = table.new_hybrid_search_builder();
        builder
            .add_vector_route(
                VECTOR_COLUMN,
                vec![10.0, 0.0, 0.0, 0.0],
                2,
                1.0,
                HashMap::new(),
            )
            .unwrap()
            .add_full_text_route(
                TEXT_COLUMN,
                r#"{"match":{"query":"alpha"}}"#,
                2,
                1.0,
                HashMap::new(),
            )
            .unwrap();
        builder.with_limit(2);

        let scored_err = builder.execute_scored().await.unwrap_err();
        assert!(
            format!("{scored_err:?}").contains("execute_read"),
            "PK hybrid execute_scored must point at execute_read, got: {scored_err:?}"
        );
        let execute_err = builder.execute().await.unwrap_err();
        assert!(
            format!("{execute_err:?}").contains("execute_read"),
            "PK hybrid execute must point at execute_read, got: {execute_err:?}"
        );
    }

    /// A primary-key hybrid table whose user schema carries an extra column named
    /// `reserved`, used to prove the materialized read rejects a reserved metadata
    /// name arriving via the default (all-columns) projection.
    ///
    /// Deserialized from JSON rather than built through `Schema::builder`, because
    /// `Schema::new` rejects reserved system field names at create time. The read
    /// guard exists for exactly this case: metadata written by another engine.
    fn pk_hybrid_table_with_reserved_column(reserved: &str) -> Table {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let options: HashMap<String, String> = table_options().into_iter().collect();
        let schema: TableSchema = serde_json::from_value(serde_json::json!({
            "version": TableSchema::CURRENT_VERSION,
            "id": 0,
            "fields": [
                {"id": 0, "name": "id", "type": "INT NOT NULL"},
                {
                    "id": 1,
                    "name": VECTOR_COLUMN,
                    "type": {"type": "VECTOR", "element": "FLOAT", "length": DIM},
                },
                {"id": 2, "name": TEXT_COLUMN, "type": "STRING"},
                {"id": 3, "name": reserved, "type": "STRING"},
            ],
            "highestFieldId": 3,
            "partitionKeys": [],
            "primaryKeys": ["id"],
            "options": options,
            "timeMillis": 0,
        }))
        .expect("reserved-column schema should deserialize");
        Table::new(
            file_io,
            Identifier::new("default", "pk_hybrid_reserved"),
            "memory:/pk_hybrid_reserved".to_string(),
            schema,
            None,
        )
    }

    // A user column colliding with an injected metadata column
    // (`_PKEY_VECTOR_POSITION`, `__paimon_search_score`, or `_ROW_ID`) must make the
    // primary-key hybrid materialized read fail loud up front — before any route
    // runs and regardless of results — mirroring the primary-key vector read guard.
    #[tokio::test]
    async fn pk_hybrid_read_rejects_reserved_user_column() {
        for reserved in ["_PKEY_VECTOR_POSITION", "__paimon_search_score", "_ROW_ID"] {
            let table = pk_hybrid_table_with_reserved_column(reserved);
            let mut builder = table.new_hybrid_search_builder();
            builder
                .add_vector_route(
                    VECTOR_COLUMN,
                    vec![1.0, 0.0, 0.0, 0.0],
                    2,
                    1.0,
                    HashMap::new(),
                )
                .unwrap();
            builder.with_limit(2);
            let err = builder
                .execute_read()
                .await
                .err()
                .expect("reserved user column must fail loud");
            assert!(
                matches!(&err, crate::Error::DataInvalid { message, .. }
                    if message.contains("reserved column")),
                "unexpected error for {reserved}: {err:?}"
            );
        }
    }

    // (e) DE regression: a hybrid over non-PK routes (no PK configs) still takes the
    // global-row-id execute_scored path unchanged. An empty table yields an empty
    // result without hitting the PK guard.
    #[tokio::test]
    async fn global_hybrid_execute_scored_unchanged() {
        // A table without any PK index configuration: both routes resolve to the
        // append/data-evolution global path.
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column(VECTOR_COLUMN, {
                    DataType::Vector(
                        VectorType::try_new(true, DIM as u32, DataType::Float(FloatType::new()))
                            .unwrap(),
                    )
                })
                .column(TEXT_COLUMN, DataType::VarChar(VarCharType::string_type()))
                .option("row-tracking.enabled", "true")
                .build()
                .unwrap(),
        );
        let table = Table::new(
            file_io,
            Identifier::new("default", "global_hybrid"),
            "memory:/global_hybrid".to_string(),
            schema,
            None,
        );

        let mut builder = table.new_hybrid_search_builder();
        builder
            .add_vector_route(
                VECTOR_COLUMN,
                vec![1.0, 0.0, 0.0, 0.0],
                2,
                1.0,
                HashMap::new(),
            )
            .unwrap()
            .add_full_text_route(
                TEXT_COLUMN,
                r#"{"match":{"query":"alpha"}}"#,
                2,
                1.0,
                HashMap::new(),
            )
            .unwrap();
        builder.with_limit(2);

        // No snapshot -> both routes empty -> fused empty; the DE path must NOT hit
        // the PK fail-loud guard.
        let result = builder
            .execute_scored()
            .await
            .expect("DE hybrid path failed");
        assert!(
            result.is_empty(),
            "empty table yields empty DE hybrid result"
        );
    }

    // (f) FAST-only guard: a primary-key hybrid with a full-text route under a
    // non-FAST global-index search mode (FULL) must fail loud, mirroring
    // `full_text_search_builder::execute_read` and Java `PrimaryKeyFullTextRead`.
    #[tokio::test]
    async fn pk_hybrid_full_text_route_rejects_non_fast_mode() {
        let table = build_hybrid_table(
            "memory:/pk_hybrid_full_mode",
            &[100, 101, 102, 103],
            &[
                [10.0, 0.0, 0.0, 0.0],
                [9.0, 0.0, 0.0, 0.0],
                [8.0, 0.0, 0.0, 0.0],
                [7.0, 0.0, 0.0, 0.0],
            ],
            &["alpha", "beta", "alpha alpha alpha", "gamma"],
            &[("full-text-index.search-mode", "full")],
        )
        .await;

        let mut builder = table.new_hybrid_search_builder();
        builder
            .add_vector_route(
                VECTOR_COLUMN,
                vec![10.0, 0.0, 0.0, 0.0],
                4,
                1.0,
                HashMap::new(),
            )
            .unwrap()
            .add_full_text_route(
                TEXT_COLUMN,
                r#"{"match":{"query":"alpha"}}"#,
                4,
                1.0,
                HashMap::new(),
            )
            .unwrap();
        builder.with_limit(3);

        let err = match builder.execute_read().await {
            Ok(_) => panic!("non-FAST global-index search mode must fail loud"),
            Err(e) => e,
        };
        assert!(
            format!("{err:?}").contains("FAST"),
            "PK hybrid full-text route under FULL mode must fail loud, got: {err:?}"
        );
    }

    // (g) Snapshot pinning: the read-latest path resolves ONE snapshot up front and
    // passes it to every route (mirror Java `routeBuilders()`). The pinned table
    // carries `scan.snapshot-id` set to the resolved latest snapshot, so both
    // routes plan against the same version instead of each re-resolving latest.
    #[tokio::test]
    async fn pk_hybrid_read_latest_pins_one_snapshot_for_all_routes() {
        let table = build_hybrid_table(
            "memory:/pk_hybrid_pin",
            &[100, 101],
            &[[10.0, 0.0, 0.0, 0.0], [9.0, 0.0, 0.0, 0.0]],
            &["alpha", "beta"],
            &[],
        )
        .await;

        let mut builder = table.new_hybrid_search_builder();
        builder
            .add_vector_route(
                VECTOR_COLUMN,
                vec![10.0, 0.0, 0.0, 0.0],
                2,
                1.0,
                HashMap::new(),
            )
            .unwrap();
        builder.with_limit(2);

        let pinned = builder
            .resolve_pinned_route_table()
            .await
            .expect("pinning must succeed")
            .expect("read-latest path must pin a snapshot");
        // First commit -> snapshot 1; the pinned copy targets exactly it.
        assert_eq!(
            pinned.schema().options().get(SCAN_SNAPSHOT_ID_OPTION),
            Some(&"1".to_string()),
            "read-latest hybrid must pin the resolved latest snapshot id"
        );
    }
}
