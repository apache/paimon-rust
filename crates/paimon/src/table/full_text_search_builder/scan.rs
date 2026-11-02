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
    pub(super) column: String,
    pub(super) filter: Option<Predicate>,
    pub(super) include_row_ids: Option<RoaringTreemap>,
    pub(super) primary_key_field_id: Option<i32>,
}

impl PlanContext {
    pub(super) fn new(table: &Table, config: &FullTextSearchConfig) -> crate::Result<Self> {
        let core = table.schema().core_options();
        core.ensure_read_authorized()?;
        let column = config
            .text_column
            .as_deref()
            .ok_or_else(|| crate::Error::ConfigInvalid {
                message: "Text column must be set via with_query() or with_text_column()".into(),
            })?;
        let field = table
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
        let has_row_filter = config.filter.as_ref().is_some_and(|filter| {
            let (_, data) = split_partition_and_data_predicates(
                filter.clone(),
                table.schema().fields(),
                table.schema().partition_keys(),
            );
            !data.is_empty()
        });
        if has_row_filter && !core.data_evolution_enabled() {
            if !table.schema().primary_keys().is_empty()
                || !core.row_tracking_enabled()
                || core.file_format() != "parquet"
            {
                return Err(crate::Error::Unsupported {
                    message: "Non-data-evolution full-text row filters require a row-tracked Parquet append table".into(),
                });
            }
            if core.full_text_index_search_mode()? != GlobalIndexSearchMode::Fast {
                return Err(crate::Error::Unsupported {
                    message: "Non-data-evolution full-text row filters support only the FAST full-text index search mode".into(),
                });
            }
        }
        let primary_key_field_id =
            resolves_to_pk_full_text_path(&core, column, !table.schema().primary_keys().is_empty())
                .then_some(field.id());
        if primary_key_field_id.is_some() && config.include_row_ids.is_some() {
            return Err(crate::Error::Unsupported {
                message:
                    "primary-key full-text search uses physical file positions, not global row IDs"
                        .into(),
            });
        }
        Ok(Self {
            column: column.into(),
            filter: config.filter.clone(),
            include_row_ids: config.include_row_ids.clone(),
            primary_key_field_id,
        })
    }
}

/// A captured snapshot and its authoritative search sources.
///
/// Plans contain transportable metadata only. The reader supplies the table,
/// filesystem, authorization, schema and query configuration.
#[derive(Clone, serde::Serialize)]
pub struct FullTextScanPlan {
    snapshot: Option<crate::spec::Snapshot>,
    splits: Vec<crate::table::SearchScanSplit>,
}

impl<'de> serde::Deserialize<'de> for FullTextScanPlan {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct PlanData {
            snapshot: Option<crate::spec::Snapshot>,
            splits: Vec<crate::table::SearchScanSplit>,
        }
        let plan = PlanData::deserialize(deserializer)?;
        Self::new(plan.snapshot, plan.splits).map_err(serde::de::Error::custom)
    }
}

impl FullTextScanPlan {
    pub fn new(
        snapshot: Option<crate::spec::Snapshot>,
        splits: Vec<crate::table::SearchScanSplit>,
    ) -> crate::Result<Self> {
        if snapshot.is_none() && !splits.is_empty() {
            return Err(crate::Error::DataInvalid {
                message: "Full-text sources require a captured snapshot".into(),
                source: None,
            });
        }
        let primary_key = splits
            .iter()
            .any(|split| matches!(split, crate::table::SearchScanSplit::PrimaryKey { .. }));
        if primary_key
            && splits
                .iter()
                .any(|split| !matches!(split, crate::table::SearchScanSplit::PrimaryKey { .. }))
        {
            return Err(crate::Error::DataInvalid {
                message: "Full-text plans cannot mix global and primary-key sources".into(),
                source: None,
            });
        }
        if !primary_key {
            super::super::search_scan_split::global_entries_from_splits(&splits)?;
        }
        if let Some(snapshot) = &snapshot {
            for split in &splits {
                if let crate::table::SearchScanSplit::PrimaryKey { data_split, .. } = split {
                    if data_split.snapshot_id() != 0 && data_split.snapshot_id() != snapshot.id() {
                        return Err(crate::Error::DataInvalid {
                            message: "Full-text source snapshot does not match its plan".into(),
                            source: None,
                        });
                    }
                }
            }
        }
        Ok(Self { snapshot, splits })
    }

    /// The snapshot captured by the scan; accessing it never resolves latest.
    pub fn snapshot(&self) -> Option<&crate::spec::Snapshot> {
        self.snapshot.as_ref()
    }

    /// Source metadata for inspection or transport to another search reader.
    pub fn splits(&self) -> &[crate::table::SearchScanSplit] {
        &self.splits
    }

    pub fn snapshot_id(&self) -> Option<i64> {
        self.snapshot.as_ref().map(crate::spec::Snapshot::id)
    }
}

/// Full-text planning follows Java FullTextScan: select one snapshot and prune
/// index manifest entries by partition. Queries and limits belong to the reader.
pub struct FullTextScan {
    table: Table,
    context: PlanContext,
}

impl FullTextScan {
    pub(super) fn new(table: &Table, config: &FullTextSearchConfig) -> crate::Result<Self> {
        Ok(Self {
            table: table.clone(),
            context: PlanContext::new(table, config)?,
        })
    }

    pub async fn scan(&self) -> crate::Result<FullTextScanPlan> {
        CoreOptions::new(self.table.schema().options()).ensure_read_authorized()?;
        let (partition_filter, _) = bind_filters(&self.table, self.context.filter.as_ref())?;
        let Some(snapshot) = crate::table::time_travel::resolve_snapshot(&self.table).await? else {
            return FullTextScanPlan::new(None, vec![]);
        };
        // Pin only the data: a schema-only rename does not create a snapshot.
        let table = self.table.copy_with_pinned_snapshot(Some(&snapshot));
        let splits = if let Some(field_id) = self.context.primary_key_field_id {
            PrimaryKeyFullTextScan::new(&table, field_id, self.context.filter.clone())
                .plan()
                .await?
                .splits
                .into_iter()
                .map(|split| crate::table::SearchScanSplit::PrimaryKey {
                    data_split: split.data_split,
                    payloads: split.current_payloads,
                    uncovered_data_files: split.uncovered_data_files,
                    row_ranges_by_file: Default::default(),
                })
                .collect()
        } else {
            let mut entries = if let Some(name) = snapshot.index_manifest() {
                let path = table.snapshot_manager().manifest_path(name);
                IndexManifest::read(table.file_io(), &path).await?
            } else {
                vec![]
            };
            if partition_filter.is_some() {
                let (partition, _) = split_partition_and_data_predicates(
                    self.context.filter.clone().unwrap(),
                    table.schema().fields(),
                    table.schema().partition_keys(),
                );
                let fields = table.schema().partition_fields();
                let pruner = PartitionFilter::from_predicate(partition.unwrap(), &fields);
                let mut selected = Vec::new();
                for entry in entries {
                    if pruner.matches_entry(&entry.partition)? {
                        selected.push(entry);
                    }
                }
                entries = selected;
            }
            super::super::search_scan_split::global_search_splits(
                &table,
                &entries,
                &self.context.column,
                self.context.filter.as_ref(),
                &Default::default(),
                true,
            )
            .await?
        };
        FullTextScanPlan::new(Some(snapshot), splits)
    }
}

pub(super) fn bind_filters(
    table: &Table,
    filter: Option<&Predicate>,
) -> crate::Result<(Option<Predicate>, Option<Predicate>)> {
    let Some(filter) = filter else {
        return Ok((None, None));
    };
    let (partition, data) = split_partition_and_data_predicates(
        filter.clone(),
        table.schema().fields(),
        table.schema().partition_keys(),
    );
    let partition = partition
        .map(|partition| {
            let mapping = table
                .schema()
                .partition_keys()
                .iter()
                .map(|key| {
                    table
                        .schema()
                        .fields()
                        .iter()
                        .position(|field| field.name() == key)
                })
                .collect::<Vec<_>>();
            partition
                .remap_field_index(&mapping)
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: "Cannot bind full-text partition predicate to the table schema".into(),
                    source: None,
                })
        })
        .transpose()?;
    Ok((partition, (!data.is_empty()).then(|| Predicate::and(data))))
}
