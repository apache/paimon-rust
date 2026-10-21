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

//! Executes full-text queries over a snapshot-scoped plan.

use super::scan::PlanContext;
use super::*;

/// An owned reader supplies table access and query configuration for captured
/// source plans. Queries and limits can change without another source scan.
pub struct FullTextRead {
    table: Table,
    context: PlanContext,
    search: FullTextSearch,
}

impl FullTextRead {
    pub(super) fn new(builder: &FullTextSearchBuilder<'_>) -> crate::Result<Self> {
        CoreOptions::new(builder.table.schema().options()).ensure_read_authorized()?;
        let limit = builder.limit.filter(|limit| *limit > 0).ok_or_else(|| {
            crate::Error::ConfigInvalid {
                message: "Limit must be positive, set via with_limit()".into(),
            }
        })?;
        let context = PlanContext::new(builder)?;
        let query = builder
            .query_text
            .as_deref()
            .ok_or_else(|| crate::Error::ConfigInvalid {
                message: "Query text must be set via with_query() or with_query_text()".into(),
            })?;
        if context.primary_key_field_id.is_some() {
            if builder
                .table
                .schema()
                .core_options()
                .full_text_index_search_mode()?
                != GlobalIndexSearchMode::Fast
            {
                return Err(crate::Error::DataInvalid { message: "primary-key full-text search supports only the FAST global-index search mode".into(), source: None });
            }
            if let Some(filter) = &context.filter {
                let (_, data) = super::super::bucket_filter::split_partition_and_data_predicates(
                    filter.clone(),
                    builder.table.schema().fields(),
                    builder.table.schema().partition_keys(),
                );
                if !data.is_empty() {
                    return Err(crate::Error::Unsupported {
                        message:
                            "Primary-key full-text search does not support non-partition filters"
                                .into(),
                    });
                }
            }
        }
        let query = if builder.query_is_dsl || context.primary_key_field_id.is_some() {
            query.to_string()
        } else {
            normalize_query_text(query, &context.column)?
        };
        let search = FullTextSearch::new(query, limit, context.column.clone())?;
        Ok(Self {
            table: builder.table.clone(),
            context,
            search,
        })
    }

    pub async fn read(
        &self,
        plan: FullTextScanPlan,
    ) -> crate::Result<crate::table::GlobalIndexResult> {
        self.read_internal(plan, None).await
    }

    /// Execute shard work through a scheduler while retaining the plan's
    /// snapshot, filtering, and final global ranking in core.
    pub(crate) async fn read_with_executor(
        &self,
        plan: FullTextScanPlan,
        executor: &dyn crate::table::SearchTaskExecutor,
    ) -> crate::Result<crate::table::GlobalIndexResult> {
        self.read_internal(plan, Some(executor)).await
    }

    async fn read_internal(
        &self,
        plan: FullTextScanPlan,
        executor: Option<&dyn crate::table::SearchTaskExecutor>,
    ) -> crate::Result<crate::table::GlobalIndexResult> {
        CoreOptions::new(self.table.schema().options()).ensure_read_authorized()?;
        if executor.is_some() && !self.table.schema().core_options().data_evolution_enabled() {
            return Err(crate::Error::Unsupported {
                message: "Scheduled full-text search requires a data-evolution table".into(),
            });
        }
        let table = self.table.copy_with_pinned_snapshot(plan.snapshot());
        let table = std::sync::Arc::new(table);
        if self.context.primary_key_field_id.is_some() {
            let partition_pruner = self.context.filter.as_ref().and_then(|filter| {
                let (partition, _) =
                    super::super::bucket_filter::split_partition_and_data_predicates(
                        filter.clone(),
                        table.schema().fields(),
                        table.schema().partition_keys(),
                    );
                partition.map(|partition| {
                    super::super::partition_filter::PartitionFilter::from_predicate(
                        partition,
                        &table.schema().partition_fields(),
                    )
                })
            });
            let mut splits = Vec::new();
            for split in plan.splits() {
                let crate::table::SearchScanSplit::PrimaryKey {
                    data_split,
                    payloads,
                    uncovered_data_files,
                    row_ranges_by_file,
                } = split
                else {
                    return Err(crate::Error::DataInvalid {
                        message: "Primary-key full-text read requires primary-key sources".into(),
                        source: None,
                    });
                };
                if let Some(pruner) = &partition_pruner {
                    if !pruner.matches_entry(&data_split.partition().to_serialized_bytes())? {
                        continue;
                    }
                }
                if !row_ranges_by_file.is_empty() {
                    return Err(crate::Error::DataInvalid {
                        message: "Full-text sources do not support vector row-range restrictions"
                            .into(),
                        source: None,
                    });
                }
                let data_split = if data_split.snapshot_id() == 0 {
                    let mut builder = crate::table::DataSplitBuilder::new()
                        .with_snapshot(plan.snapshot_id().unwrap_or(0))
                        .with_partition(data_split.partition().clone())
                        .with_bucket(data_split.bucket())
                        .with_bucket_path(data_split.bucket_path().to_string())
                        .with_total_buckets(data_split.total_buckets())
                        .with_data_files(data_split.data_files().to_vec())
                        .with_raw_convertible(data_split.raw_convertible())
                        .with_streaming(data_split.is_streaming());
                    if let Some(dvs) = data_split.data_deletion_files() {
                        builder = builder.with_data_deletion_files(dvs.to_vec());
                    }
                    builder.build()?
                } else {
                    data_split.clone()
                };
                splits.push(
                    super::super::pk_full_text_scan::PrimaryKeyFullTextSearchSplit::new(
                        data_split,
                        payloads.clone(),
                        uncovered_data_files.clone(),
                    )?,
                );
            }
            let source = super::super::pk_full_text_scan::PrimaryKeyFullTextScanPlan {
                snapshot_id: plan.snapshot_id().unwrap_or(0),
                splits,
            };
            let materialize = DataFileReader::new(
                table.file_io().clone(),
                table.schema_manager().clone(),
                table.schema().id(),
                table.schema().fields().to_vec(),
                table.data_fields_extractor(),
                table.schema().fields().to_vec(),
                Vec::new(),
            )
            .with_table_options(table.schema().options().clone());
            let read = PrimaryKeyFullTextRead::new(
                table.file_io().clone(),
                materialize,
                table.location().trim_end_matches('/').to_string(),
                table.schema().core_options().index_file_in_data_file_dir(),
            );
            let result = read
                .search_route(&source, &self.search.query_text, self.search.limit)
                .await?;
            return crate::table::GlobalIndexResult::from_full_text_primary_key(
                table,
                result.snapshot_id,
                result.candidates,
                result.splits,
            );
        }
        for split in plan.splits() {
            match split {
                crate::table::SearchScanSplit::Index { index_files, .. } => {
                    let field_id =
                        find_field_id_by_name(table.schema().fields(), &self.context.column);
                    if index_files.iter().any(|file| {
                        file.index_type != FULL_TEXT_INDEX_TYPE
                            || file
                                .global_index_meta
                                .as_ref()
                                .map(|meta| meta.index_field_id)
                                != field_id
                    }) {
                        return Err(crate::Error::DataInvalid {
                            message:
                                "Full-text source does not match the reader's field and index type"
                                    .into(),
                            source: None,
                        });
                    }
                }
                crate::table::SearchScanSplit::PrimaryKey { .. } => {
                    return Err(crate::Error::DataInvalid {
                        message: "Global full-text read requires global row-ID sources".into(),
                        source: None,
                    });
                }
                _ => {}
            }
        }
        let entries = super::super::search_scan_split::global_entries_from_splits(plan.splits())?;
        let (partition_filter, row_filter) =
            super::scan::bind_filters(&table, self.context.filter.as_ref())?;
        let mut search = self.search.clone();
        search.include_row_ids = self.context.include_row_ids.clone();
        let hits = evaluate_full_text_search_with_executor(
            FullTextSearchEvaluation {
                table: Some(&table),
                file_io: table.file_io(),
                table_path: table.location(),
                table_options: table.schema().options(),
                schema_fields: table.schema().fields(),
                next_row_id: plan.snapshot().and_then(crate::spec::Snapshot::next_row_id),
                partition_filter: partition_filter.as_ref(),
                row_filter: row_filter.as_ref(),
                source_splits: Some(plan.splits()),
            },
            &entries,
            &search,
            executor,
        )
        .await?;
        Ok(crate::table::GlobalIndexResult::from_global_search(
            table,
            crate::vector_search::ScoredRowIds::new(hits.row_ids, hits.scores),
        ))
    }
}
