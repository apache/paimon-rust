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

//! Vector search planning contract, shared by DE and primary-key scans.

use std::future::Future;
use std::sync::Arc;

use roaring::RoaringTreemap;

use crate::spec::{CoreOptions, Predicate, Snapshot};
use crate::table::de_vector_scan::{DeVectorScan, PreparedVectorSearchFilter};
use crate::table::pk_vector_scan::{PkVectorScan, PkVectorScanPlan};
use crate::table::vector_search_common::{
    targets_primary_key_column, validate_global_vector_index_types,
};
use crate::table::SearchScanSplit;
use crate::table::{find_field_id_by_name, BucketVectorSearchSplit, Table};
use std::collections::{HashMap, HashSet};

/// Resolve one snapshot and the work a vector reader will consume.
///
/// The associated plan keeps global row IDs and physical bucket positions in
/// separate types. A reader cannot accidentally consume another route's plan.
pub(super) trait Scan: Sync {
    type Plan;

    fn plan(&self) -> impl Future<Output = crate::Result<Self::Plan>> + Send;
}

/// Snapshot and source work shared by single-query and batch readers.
///
/// A plan is ordinary transportable data. Query vectors, filters, options,
/// authorization and filesystem handles belong to the reader that consumes it.
#[derive(Clone, Debug, serde::Serialize)]
pub struct VectorScanPlan {
    snapshot: Option<Snapshot>,
    splits: Vec<SearchScanSplit>,
}

impl VectorScanPlan {
    /// Construct a plan from authoritative source work. No table, snapshot
    /// manager or index manifest is consulted. `None` denotes an empty table.
    pub fn new(snapshot: Option<Snapshot>, splits: Vec<SearchScanSplit>) -> crate::Result<Self> {
        if snapshot.is_none() && !splits.is_empty() {
            return Err(crate::Error::DataInvalid {
                message: "A vector source plan without a snapshot must be empty".into(),
                source: None,
            });
        }
        let primary_key = splits
            .iter()
            .any(|source| matches!(source, SearchScanSplit::PrimaryKey { .. }));
        if primary_key
            && splits
                .iter()
                .any(|source| !matches!(source, SearchScanSplit::PrimaryKey { .. }))
        {
            return Err(crate::Error::DataInvalid {
                message: "A vector source plan cannot mix global row IDs and primary-key positions"
                    .into(),
                source: None,
            });
        }
        if !primary_key {
            super::search_scan_split::global_entries_from_splits(&splits)?;
        }
        if let Some(snapshot) = &snapshot {
            for split in &splits {
                if let SearchScanSplit::PrimaryKey { data_split, .. } = split {
                    if data_split.snapshot_id() != 0 && data_split.snapshot_id() != snapshot.id() {
                        return Err(crate::Error::DataInvalid {
                            message: "Primary-key vector split belongs to another snapshot".into(),
                            source: None,
                        });
                    }
                }
            }
        }
        Ok(Self { snapshot, splits })
    }

    /// The complete snapshot captured by planning, including retained tags.
    pub fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }

    /// The exact sources selected by the planner. An empty or restricted list
    /// never causes the reader to discover additional index or raw sources.
    pub fn splits(&self) -> &[SearchScanSplit] {
        &self.splits
    }

    pub fn snapshot_id(&self) -> Option<i64> {
        self.snapshot.as_ref().map(Snapshot::id)
    }
}

impl<'de> serde::Deserialize<'de> for VectorScanPlan {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        struct Sources {
            snapshot: Option<Snapshot>,
            splits: Vec<SearchScanSplit>,
        }
        let Sources { snapshot, splits } =
            <Sources as serde::Deserialize>::deserialize(deserializer)?;
        Self::new(snapshot, splits).map_err(serde::de::Error::custom)
    }
}

async fn primary_key_sources(
    table: &Table,
    context: &PlanContext,
    plan: &PkVectorScanPlan,
) -> crate::Result<Vec<SearchScanSplit>> {
    let selected =
        super::pk_vector_read::filtered_row_ranges(table, context.filter.as_ref(), plan).await?;
    plan.splits
        .iter()
        .enumerate()
        .map(|(index, split)| {
            let payloads = plan
                .payloads
                .get(&(
                    split.data_split.partition().to_serialized_bytes(),
                    split.data_split.bucket(),
                ))
                .into_iter()
                .flatten()
                .filter(|payload| {
                    split.ann_segments.iter().any(|segment| {
                        let location = super::index_file_path::IndexFileLocation::BucketLocal {
                            table_path: table.location(),
                            bucket_path: split.data_split.bucket_path(),
                            index_file_in_data_file_dir: table
                                .schema()
                                .core_options()
                                .index_file_in_data_file_dir(),
                        };
                        segment.path
                            == location
                                .resolve(&payload.file_name, payload.external_path.as_deref())
                    })
                })
                .cloned()
                .collect();
            let covered = split
                .ann_segments
                .iter()
                .flat_map(|segment| segment.source_meta.source_files())
                .map(|file| file.file_name())
                .collect::<HashSet<_>>();
            Ok(SearchScanSplit::PrimaryKey {
                data_split: split.data_split.clone(),
                payloads,
                uncovered_data_files: split
                    .active_files
                    .iter()
                    .filter(|file| !covered.contains(file.file_name.as_str()))
                    .map(|file| file.file_name.clone())
                    .collect(),
                row_ranges_by_file: selected
                    .as_ref()
                    .map(|ranges| ranges[index].clone())
                    .unwrap_or_default(),
            })
        })
        .collect()
}

/// Validated reader/scan configuration. This is never part of a source plan.
#[derive(Clone)]
pub(super) struct PlanContext {
    pub(super) column: String,
    pub(super) filter: Option<Predicate>,
    pub(super) include_row_ids: Option<Arc<RoaringTreemap>>,
    pub(super) prepared_snapshot: Option<Option<Snapshot>>,
}

impl PlanContext {
    pub(super) fn new(
        table: &Table,
        column: &str,
        filter: Option<&Predicate>,
        include_row_ids: Option<&Arc<RoaringTreemap>>,
        prepared: Option<&PreparedVectorSearchFilter>,
    ) -> crate::Result<Self> {
        let core = CoreOptions::new(table.schema().options());
        core.ensure_read_authorized()?;
        if let Some(prepared) = prepared {
            if table.location().trim_end_matches('/')
                != prepared.table().location().trim_end_matches('/')
                || table.branch() != prepared.table().branch()
            {
                return Err(crate::Error::DataInvalid {
                    message: "Prepared vector search filter belongs to a different table"
                        .to_string(),
                    source: None,
                });
            }
            CoreOptions::new(prepared.table().schema().options()).ensure_read_authorized()?;
        }
        if column.is_empty() {
            return Err(crate::Error::ConfigInvalid {
                message: "Vector column must be set via with_vector_column()".to_string(),
            });
        }
        if targets_primary_key_column(&core, column)
            && (include_row_ids.is_some() || prepared.is_some())
        {
            return Err(crate::Error::DataInvalid {
                message: "global row-ID filters cannot be applied to primary-key file positions; use with_filter()".to_string(), source: None,
            });
        }
        Ok(Self {
            column: column.to_string(),
            filter: filter.cloned(),
            include_row_ids: prepared
                .map(PreparedVectorSearchFilter::include_row_ids)
                .or(include_row_ids)
                .cloned(),
            prepared_snapshot: prepared.map(|filter| filter.table().travel_snapshot().cloned()),
        })
    }
}

/// Creates query-independent plans for DE or primary-key vector search.
pub struct VectorScan {
    table: Table,
    context: PlanContext,
    scan: VectorScanKind,
    options: HashMap<String, String>,
}

enum VectorScanKind {
    DataEvolution(Box<DeVectorScan>),
    PrimaryKey(Box<PkVectorScan>),
}

impl VectorScan {
    pub(super) fn new(
        table: &Table,
        column: &str,
        filter: Option<&Predicate>,
        include_row_ids: Option<&Arc<RoaringTreemap>>,
        prepared: Option<&PreparedVectorSearchFilter>,
    ) -> crate::Result<Self> {
        let context = PlanContext::new(table, column, filter, include_row_ids, prepared)?;
        let core = CoreOptions::new(table.schema().options());
        let scan = if targets_primary_key_column(&core, column) {
            let column = core.primary_key_vector_index_column()?;
            let field_id =
                find_field_id_by_name(table.schema().fields(), &column).ok_or_else(|| {
                    crate::Error::DataInvalid {
                        message: format!("PK-vector column '{column}' not found in schema"),
                        source: None,
                    }
                })?;
            VectorScanKind::PrimaryKey(Box::new(PkVectorScan::new(
                table,
                field_id,
                super::pk_index_options::vector_index_type(table.schema().options(), &column)?
                    .to_string(),
                filter.cloned(),
            )))
        } else {
            VectorScanKind::DataEvolution(Box::new(DeVectorScan::new(
                table,
                filter,
                include_row_ids,
                prepared,
            )))
        };
        Ok(Self {
            table: table.clone(),
            context,
            scan,
            options: HashMap::new(),
        })
    }

    pub(super) fn with_options(mut self, options: &HashMap<String, String>) -> Self {
        self.options = options.clone();
        self
    }

    pub async fn plan(&self) -> crate::Result<VectorScanPlan> {
        match &self.scan {
            VectorScanKind::DataEvolution(scan) => {
                let plan = scan.plan().await?;
                if let Some(field_id) =
                    find_field_id_by_name(plan.table.schema().fields(), &self.context.column)
                {
                    validate_global_vector_index_types(
                        &plan.index_entries,
                        field_id,
                        &self.context.column,
                    )?;
                }
                let snapshot = plan.table.travel_snapshot().cloned();
                let splits = if plan.skip_search {
                    vec![]
                } else {
                    super::search_scan_split::global_search_splits(
                        &plan.table,
                        &plan.index_entries,
                        &self.context.column,
                        self.context.filter.as_ref(),
                        &self.options,
                        false,
                    )
                    .await?
                };
                VectorScanPlan::new(snapshot, splits)
            }
            VectorScanKind::PrimaryKey(scan) => {
                let plan = scan.plan().await?;
                let splits = primary_key_sources(&self.table, &self.context, &plan).await?;
                VectorScanPlan::new(plan.snapshot, splits)
            }
        }
    }

    /// Adapt Java bucket work to the same transportable source plan. A positive
    /// bucket snapshot ID is loaded directly. Unversioned buckets capture the
    /// scan's configured snapshot once here; the returned plan never follows latest.
    pub async fn plan_from_bucket_splits(
        &self,
        splits: Vec<BucketVectorSearchSplit>,
    ) -> crate::Result<VectorScanPlan> {
        let VectorScanKind::PrimaryKey(scan) = &self.scan else {
            return Err(crate::Error::DataInvalid {
                message: "bucket splits require a primary-key vector scan".into(),
                source: None,
            });
        };
        // Preserve manifest-form payload metadata beside the canonical bucket
        // assembler's validated segments, so the resulting plan is transportable.
        let payloads = splits
            .iter()
            .map(|split| {
                let key = (
                    split.data_split().partition().to_serialized_bytes(),
                    split.data_split().bucket(),
                );
                let files = split
                    .payload_files()
                    .iter()
                    .map(|payload| crate::spec::IndexFileMeta {
                        index_type: payload.index_type().to_string(),
                        file_name: payload.file_name().to_string(),
                        file_size: payload.file_size(),
                        row_count: payload.row_count(),
                        deletion_vectors_ranges: payload.deletion_vectors_ranges().cloned(),
                        external_path: payload.external_path().map(str::to_string),
                        global_index_meta: Some(payload.global_index_meta().clone()),
                    })
                    .collect();
                (key, files)
            })
            .collect();
        let mut plan = scan.plan_for_bucket_vector_splits(splits)?;
        plan.payloads = payloads;
        let snapshot = match self
            .table
            .travel_snapshot()
            .filter(|snapshot| snapshot.id() == plan.snapshot_id)
        {
            Some(snapshot) => snapshot.clone(),
            None if plan.snapshot_id == 0 => {
                self.table.resolve_read_snapshot().await?.ok_or_else(|| {
                    crate::Error::DataInvalid {
                        message: "Bucket search sources require a captured table snapshot".into(),
                        source: None,
                    }
                })?
            }
            None => {
                self.table
                    .snapshot_manager()
                    .get_snapshot(plan.snapshot_id)
                    .await?
            }
        };
        let sources = primary_key_sources(&self.table, &self.context, &plan).await?;
        VectorScanPlan::new(Some(snapshot), sources)
    }
}
