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

//! Snapshot-pinned, query-independent full-text planning.

use super::*;
use crate::spec::Predicate;
use crate::table::bucket_filter::split_partition_and_data_predicates;
use crate::table::partition_filter::PartitionFilter;

#[derive(Clone, PartialEq)]
pub(super) struct PlanContext {
    location: String,
    branch: String,
    pub(super) column: String,
    pub(super) filter: Option<Predicate>,
    pub(super) include_row_ids: Option<RoaringTreemap>,
}

impl PlanContext {
    pub(super) fn new(builder: &FullTextSearchBuilder<'_>) -> crate::Result<Self> {
        CoreOptions::new(builder.table.schema().options()).ensure_read_authorized()?;
        let column = builder
            .text_column
            .as_deref()
            .ok_or_else(|| crate::Error::ConfigInvalid {
                message: "Text column must be set via with_query() or with_text_column()".into(),
            })?;
        let field = builder
            .table
            .schema()
            .fields()
            .iter()
            .find(|field| field.name() == column)
            .ok_or_else(|| crate::Error::ConfigInvalid {
                message: format!("Text column '{column}' does not exist"),
            })?;
        if !matches!(
            field.data_type(),
            crate::spec::DataType::Char(_) | crate::spec::DataType::VarChar(_)
        ) {
            return Err(crate::Error::ConfigInvalid {
                message: format!("Full-text column '{column}' must be a character string"),
            });
        }
        if resolves_to_pk_full_text_path(&builder.table.schema().core_options(), column) {
            return Err(crate::Error::DataInvalid { message: "primary-key full-text search does not produce global row ids; use the materialized read (execute_read) instead".into(), source: None });
        }
        Ok(Self {
            location: builder.table.location().trim_end_matches('/').into(),
            branch: builder.table.branch().into(),
            column: column.into(),
            filter: builder.filter.clone(),
            include_row_ids: builder.include_row_ids.clone(),
        })
    }
}

/// One selected snapshot and its partition-pruned index entries. A plan remains
/// usable after its scan is dropped, and does not re-resolve the latest snapshot.
#[derive(Clone)]
pub struct FullTextScanPlan {
    pub(super) context: PlanContext,
    pub(super) table: Table,
    pub(super) entries: Vec<IndexManifestEntry>,
    pub(super) next_row_id: Option<i64>,
    pub(super) snapshot_id: Option<i64>,
    pub(super) partition_filter: Option<Predicate>,
    pub(super) row_filter: Option<Predicate>,
}

impl FullTextScanPlan {
    pub fn snapshot_id(&self) -> Option<i64> {
        self.snapshot_id
    }
}

/// Full-text planning follows Java FullTextScan: select one snapshot and prune
/// index manifest entries by partition. Queries and limits belong to the reader.
pub struct FullTextScan {
    table: Table,
    context: PlanContext,
}

impl FullTextScan {
    pub(super) fn new(builder: &FullTextSearchBuilder<'_>) -> crate::Result<Self> {
        Ok(Self {
            table: builder.table.clone(),
            context: PlanContext::new(builder)?,
        })
    }

    pub async fn scan(&self) -> crate::Result<FullTextScanPlan> {
        CoreOptions::new(self.table.schema().options()).ensure_read_authorized()?;
        let mut plan = FullTextScanPlan {
            context: self.context.clone(),
            table: self.table.clone(),
            entries: Vec::new(),
            next_row_id: None,
            snapshot_id: None,
            partition_filter: None,
            row_filter: None,
        };
        let mut partition_pruner = None;
        if let Some(filter) = &self.context.filter {
            let (partition, data) = split_partition_and_data_predicates(
                filter.clone(),
                self.table.schema().fields(),
                self.table.schema().partition_keys(),
            );
            if let Some(partition) = partition {
                let fields = self.table.schema().partition_fields();
                partition_pruner =
                    Some(PartitionFilter::from_predicate(partition.clone(), &fields));
                let mapping = self
                    .table
                    .schema()
                    .partition_keys()
                    .iter()
                    .map(|key| {
                        self.table
                            .schema()
                            .fields()
                            .iter()
                            .position(|field| field.name() == key)
                    })
                    .collect::<Vec<_>>();
                plan.partition_filter =
                    Some(partition.remap_field_index(&mapping).ok_or_else(|| {
                        crate::Error::DataInvalid {
                            message:
                                "Cannot bind full-text partition predicate to the table schema"
                                    .into(),
                            source: None,
                        }
                    })?);
            }
            plan.row_filter = (!data.is_empty()).then(|| Predicate::and(data));
        }
        if plan.row_filter.is_some() && !self.table.schema().core_options().data_evolution_enabled()
        {
            return Err(crate::Error::Unsupported {
                message: "Full-text row filters require a data-evolution table".into(),
            });
        }
        let Some(snapshot) = crate::table::time_travel::resolve_snapshot(&self.table).await? else {
            return Ok(plan);
        };
        plan.snapshot_id = Some(snapshot.id());
        plan.next_row_id = snapshot.next_row_id();
        // Pin the data without changing the schema already bound by the query.
        // A schema-only rename does not create a new data snapshot.
        plan.table = self.table.copy_with_pinned_snapshot(&snapshot);
        if let Some(name) = snapshot.index_manifest() {
            let path = self.table.snapshot_manager().manifest_path(name);
            plan.entries = IndexManifest::read(self.table.file_io(), &path).await?;
        }
        if let Some(partition) = partition_pruner {
            let mut entries = Vec::new();
            for entry in plan.entries {
                if partition.matches_entry(&entry.partition)? {
                    entries.push(entry);
                }
            }
            plan.entries = entries;
        }
        Ok(plan)
    }
}
