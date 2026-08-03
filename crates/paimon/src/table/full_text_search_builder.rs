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

//! Full-text search builder for Paimon tables.
//!
//! Reference: [FullTextSearchBuilderImpl.java](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/table/source/FullTextSearchBuilderImpl.java)

use crate::full_text::{FullTextSearch, SearchResult};
use crate::io::FileIO;
use crate::spec::{
    CoreOptions, DataField, FileKind, GlobalIndexSearchMode, IndexFileMeta, IndexManifest,
    IndexManifestEntry, ROW_ID_FIELD_NAME,
};
use crate::table::data_file_reader::DataFileReader;
use crate::table::full_text_index_adapter::{search_full_text_file, search_full_text_index};
use crate::table::global_index_scanner::{
    deleted_row_ranges_for_data_evolution_dvs, search_limit_with_deleted_rows,
    unindexed_ranges_for_global_index_entries, RowRangeIndex,
};
use crate::table::pk_full_text_read::PrimaryKeyFullTextRead;
use crate::table::pk_full_text_scan::PrimaryKeyFullTextScan;
use crate::table::{
    find_field_id_by_name, merge_row_ranges, ArrowRecordBatchStream, RowRange, Table,
};
use arrow_array::{Array, Int64Array, LargeStringArray, RecordBatch, StringArray};
use futures::{stream, StreamExt, TryStreamExt};
use paimon_ftindex_core::io::PosWriter;
use paimon_ftindex_core::{FullTextIndexConfig, FullTextIndexWriter};
use roaring::RoaringTreemap;
use serde_json::json;
use std::collections::{HashMap, HashSet};

const INDEX_DIR: &str = "index";
const FULL_TEXT_INDEX_TYPE: &str = "full-text";
const FULL_TEXT_INDEX_SEARCH_CONCURRENCY: usize = 8;

/// Builder for executing full-text search on a Paimon table.
///
/// Usage:
/// ```ignore
/// let result = table.new_full_text_search_builder()
///     .with_text_column("content")
///     .with_query_text("hello world")
///     .with_limit(10)
///     .execute()
///     .await?;
/// ```
///
/// Reference: `org.apache.paimon.table.source.FullTextSearchBuilder`
pub struct FullTextSearchBuilder<'a> {
    table: &'a Table,
    text_column: Option<String>,
    query_text: Option<String>,
    limit: Option<usize>,
    include_row_ids: Option<RoaringTreemap>,
}

impl<'a> FullTextSearchBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            text_column: None,
            query_text: None,
            limit: None,
            include_row_ids: None,
        }
    }

    /// Set the text column to search.
    pub fn with_text_column(&mut self, name: &str) -> &mut Self {
        self.text_column = Some(name.to_string());
        self
    }

    /// Set the query text to search for.
    pub fn with_query_text(&mut self, query: &str) -> &mut Self {
        self.query_text = Some(query.to_string());
        self
    }

    /// Set candidate row IDs to include in the search.
    pub fn with_include_row_ids(&mut self, include_row_ids: RoaringTreemap) -> &mut Self {
        self.include_row_ids = Some(include_row_ids);
        self
    }

    /// Set the top-k limit for results.
    pub fn with_limit(&mut self, limit: usize) -> &mut Self {
        self.limit = Some(limit);
        self
    }

    /// Execute the full-text search and return row ranges.
    ///
    /// This reads the latest snapshot, loads the index manifest, and evaluates
    /// the search against full-text indexes.
    ///
    /// Reference: `FullTextSearchBuilder.executeLocal()`
    pub async fn execute(&self) -> crate::Result<Vec<RowRange>> {
        Ok(self.execute_scored().await?.to_row_ranges())
    }

    pub async fn execute_scored(&self) -> crate::Result<SearchResult> {
        // Fail closed: returns data-derived row ranges outside `TableScan`/`TableRead`.
        let core = CoreOptions::new(self.table.schema().options());
        core.ensure_read_authorized()?;
        let text_column =
            self.text_column
                .as_deref()
                .ok_or_else(|| crate::Error::ConfigInvalid {
                    message: "Text column must be set via with_text_column()".to_string(),
                })?;
        let query_text = self
            .query_text
            .as_deref()
            .ok_or_else(|| crate::Error::ConfigInvalid {
                message: "Query text must be set via with_query_text()".to_string(),
            })?;
        let limit = self.limit.ok_or_else(|| crate::Error::ConfigInvalid {
            message: "Limit must be set via with_limit()".to_string(),
        })?;

        // Primary-key full-text search does not produce global row-ids: it maps
        // hits to physical `(data file, row position)` pairs. A scored/row-range
        // search is therefore unsupported on the PK path — callers must use
        // `execute_read`. Fail loud rather than fall through to the append/DE
        // global-index path (which would search the wrong index and could return
        // an empty or wrong result). Mirrors the vector builder.
        if resolves_to_pk_full_text_path(&core, text_column) {
            return Err(crate::Error::DataInvalid {
                message: "primary-key full-text search does not produce global row ids; use the \
                          materialized read (execute_read) instead"
                    .to_string(),
                source: None,
            });
        }

        let mut search = FullTextSearch::new(
            normalize_query_text(query_text, text_column)?,
            limit,
            text_column.to_string(),
        )?;
        if let Some(include_row_ids) = &self.include_row_ids {
            search = search.with_include_row_ids(include_row_ids.clone());
        }

        let snapshot_manager = self.table.snapshot_manager();

        let snapshot = match snapshot_manager.get_latest_snapshot().await? {
            Some(s) => s,
            None => return Ok(SearchResult::empty()),
        };

        let index_entries = match snapshot.index_manifest() {
            Some(index_manifest_name) => {
                let manifest_path = snapshot_manager.manifest_path(index_manifest_name);
                IndexManifest::read(self.table.file_io(), &manifest_path).await?
            }
            None => Vec::new(),
        };

        evaluate_full_text_search(
            FullTextSearchEvaluation {
                table: Some(self.table),
                file_io: self.table.file_io(),
                table_path: self.table.location(),
                table_options: self.table.schema().options(),
                schema_fields: self.table.schema().fields(),
                next_row_id: snapshot.next_row_id(),
            },
            &index_entries,
            &search,
        )
        .await
    }

    /// Run the full-text search and materialize the matching rows as Arrow batches,
    /// ordered best-score-first with a `__paimon_search_score` column appended (the
    /// internal `_PKEY_VECTOR_POSITION` column is never exposed).
    ///
    /// Only the primary-key full-text path can materialize rows: it produces
    /// physical `(data file, row position)` hits that a subsequent read turns into
    /// table rows. Dispatch mirrors Java `primaryKeyFullTextDefinition` — the PK
    /// path is taken only when data-evolution is DISABLED and the queried column is
    /// a configured `pk-full-text.index.columns` entry. The PK full-text read is
    /// FAST-mode only; `FULL`/`DETAIL` fail loud (no silent degrade). A query that
    /// does not resolve to the PK path also fails loud rather than silently
    /// returning nothing, since the append/data-evolution materialized read is not
    /// supported here.
    pub async fn execute_read(&self) -> crate::Result<ArrowRecordBatchStream> {
        // Fail closed: returns data outside `TableScan`/`TableRead`.
        let core = CoreOptions::new(self.table.schema().options());
        core.ensure_read_authorized()?;
        let text_column =
            self.text_column
                .as_deref()
                .ok_or_else(|| crate::Error::ConfigInvalid {
                    message: "Text column must be set via with_text_column()".to_string(),
                })?;
        let query_text = self
            .query_text
            .as_deref()
            .ok_or_else(|| crate::Error::ConfigInvalid {
                message: "Query text must be set via with_query_text()".to_string(),
            })?;
        let limit = self.limit.ok_or_else(|| crate::Error::ConfigInvalid {
            message: "Limit must be set via with_limit()".to_string(),
        })?;

        if !resolves_to_pk_full_text_path(&core, text_column) {
            return Err(crate::Error::Unsupported {
                message: "materialized full-text read (execute_read) is only supported on the \
                          primary-key full-text path (data-evolution disabled and the column \
                          configured in pk-full-text.index.columns)"
                    .to_string(),
            });
        }

        // FAST-only: reject FULL/DETAIL loud rather than silently degrading.
        if core.global_index_search_mode()? != GlobalIndexSearchMode::Fast {
            return Err(crate::Error::DataInvalid {
                message: "primary-key full-text search supports only the FAST global-index search \
                          mode"
                    .to_string(),
                source: None,
            });
        }

        // Reject a non-positive limit at construction, before the empty-plan fast
        // path — an empty table must not mask an invalid limit (mirrors Java
        // `PrimaryKeyFullTextRead`, and matches `search_bucket`'s own guard).
        if limit == 0 {
            return Err(crate::Error::ConfigInvalid {
                message: "Limit must be positive".to_string(),
            });
        }

        // Resolve the queried column's schema field id for the scan/field-id guard.
        let field_id = find_field_id_by_name(self.table.schema().fields(), text_column)
            .ok_or_else(|| crate::Error::DataInvalid {
                message: format!("full-text search column '{text_column}' does not exist"),
                source: None,
            })?;

        let plan = PrimaryKeyFullTextScan::new(self.table, field_id, None)
            .plan()
            .await?;
        if plan.splits.is_empty() {
            return Ok(Box::pin(stream::empty()));
        }

        // A predicate-free materialization reader projecting the user table
        // columns (mirrors `table_read.rs::new_data_file_reader` with an empty
        // predicate list). The PK read appends the score column itself.
        let materialize_reader = DataFileReader::new(
            self.table.file_io().clone(),
            self.table.schema_manager().clone(),
            self.table.schema().id(),
            self.table.schema().fields().to_vec(),
            self.table.schema().fields().to_vec(),
            Vec::new(),
        );
        let read = PrimaryKeyFullTextRead::new(
            self.table.file_io().clone(),
            materialize_reader,
            self.table.location().trim_end_matches('/').to_string(),
        );
        read.read(&plan, query_text, limit).await
    }
}

/// Whether a query on `text_column` resolves to the primary-key full-text read
/// path. Mirrors Java `primaryKeyFullTextDefinition`: taken only when
/// data-evolution is DISABLED and the column is a configured
/// `pk-full-text.index.columns` entry (membership via the non-erroring accessor so
/// a malformed config cannot abort an unrelated append/DE query).
fn resolves_to_pk_full_text_path(core: &CoreOptions<'_>, text_column: &str) -> bool {
    !core.data_evolution_enabled()
        && core
            .primary_key_full_text_index_columns()
            .iter()
            .any(|c| c == text_column)
}

/// Evaluate a full-text search query against full-text indexes found in the index manifest.
struct FullTextSearchEvaluation<'a> {
    table: Option<&'a Table>,
    file_io: &'a FileIO,
    table_path: &'a str,
    table_options: &'a HashMap<String, String>,
    schema_fields: &'a [DataField],
    next_row_id: Option<i64>,
}

async fn evaluate_full_text_search(
    evaluation: FullTextSearchEvaluation<'_>,
    index_entries: &[IndexManifestEntry],
    search: &FullTextSearch,
) -> crate::Result<SearchResult> {
    let table_path = evaluation.table_path.trim_end_matches('/');
    let core_options = CoreOptions::new(evaluation.table_options);
    let search_mode = core_options.global_index_search_mode()?;

    let field_id = match find_field_id_by_name(evaluation.schema_fields, &search.field_name) {
        Some(id) => id,
        None => return Ok(SearchResult::empty()),
    };

    // Collect full-text entries for the target field.
    let fulltext_entries: Vec<_> = index_entries
        .iter()
        .filter(|e| {
            e.kind == FileKind::Add
                && e.index_file.index_type == FULL_TEXT_INDEX_TYPE
                && e.index_file
                    .global_index_meta
                    .as_ref()
                    .is_some_and(|m| m.index_field_id == field_id)
        })
        .collect();
    let has_fulltext_entries = !fulltext_entries.is_empty();

    if !has_fulltext_entries && search_mode == GlobalIndexSearchMode::Fast {
        return Ok(SearchResult::empty());
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

    let mut merged = SearchResult::empty();
    if !fulltext_entries.is_empty() {
        let plans =
            plan_full_text_index_searches(fulltext_entries, search.include_row_ids.as_ref())?;
        let results = futures::stream::iter(plans)
            .map(|plan| {
                let entry = plan.entry;
                let global_meta = entry.index_file.global_index_meta.as_ref().unwrap();
                let path = format!("{table_path}/{INDEX_DIR}/{}", entry.index_file.file_name);
                let file_name = entry.index_file.file_name.clone();
                let query_text = search.query_text.clone();
                let local_filter = plan.local_filter;
                let row_range_start = global_meta.row_range_start;
                let row_range_end = global_meta.row_range_end;
                let limit = search_limit_with_deleted_rows(
                    search.limit,
                    row_range_start,
                    row_range_end,
                    deleted_row_index.as_ref(),
                );
                async move {
                    let input = evaluation.file_io.new_input(&path)?;
                    let reader = Box::new(input.reader().await?);
                    let result =
                        search_full_text_index(reader, file_name, query_text, limit, local_filter)
                            .await?;
                    Ok::<_, crate::Error>(search_result_from_core(result)?.offset(row_range_start))
                }
            })
            .buffered(FULL_TEXT_INDEX_SEARCH_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        for r in &results {
            merged = merged.or(r);
        }
    }

    if search_mode != GlobalIndexSearchMode::Fast && has_fulltext_entries {
        let detail_ranges = if search_mode == GlobalIndexSearchMode::Detail {
            let table = evaluation.table.ok_or_else(|| crate::Error::DataInvalid {
                message: "Full-text raw search in detail mode requires table context".to_string(),
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
            is_full_text_index_file,
        );
        if !raw_ranges.is_empty() {
            let table = evaluation.table.ok_or_else(|| crate::Error::DataInvalid {
                message: "Full-text raw search requires table context".to_string(),
                source: None,
            })?;
            let raw_result =
                read_raw_full_text_search(table, search, &raw_ranges, evaluation.table_options)
                    .await?;
            merged = merged.without_row_ranges(&raw_ranges)?;
            merged = merged.or(&raw_result);
        }
    }

    Ok(merged
        .without_deleted_row_ranges(deleted_row_index.as_ref())?
        .top_k(search.limit))
}

struct FullTextIndexSearchPlan {
    entry: IndexManifestEntry,
    local_filter: Option<RoaringTreemap>,
}

fn plan_full_text_index_searches(
    entries: Vec<&IndexManifestEntry>,
    include_row_ids: Option<&RoaringTreemap>,
) -> crate::Result<Vec<FullTextIndexSearchPlan>> {
    let mut ranges = Vec::with_capacity(entries.len());
    for (index, entry) in entries.iter().enumerate() {
        let meta = entry.index_file.global_index_meta.as_ref().ok_or_else(|| {
            crate::Error::DataInvalid {
                message: format!(
                    "Full-text index '{}' is missing global index metadata",
                    entry.index_file.file_name
                ),
                source: None,
            }
        })?;
        if meta.row_range_start < 0 || meta.row_range_end < meta.row_range_start {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Invalid full-text row range [{}, {}] for '{}'",
                    meta.row_range_start, meta.row_range_end, entry.index_file.file_name
                ),
                source: None,
            });
        }
        ranges.push((
            meta.row_range_start as u64,
            meta.row_range_end as u64,
            index,
        ));
    }
    ranges.sort_unstable_by_key(|(start, _, _)| *start);

    let Some(include_row_ids) = include_row_ids else {
        return Ok(entries
            .into_iter()
            .map(|entry| FullTextIndexSearchPlan {
                entry: entry.clone(),
                local_filter: None,
            })
            .collect());
    };

    let mut local_filters = (0..entries.len())
        .map(|_| RoaringTreemap::new())
        .collect::<Vec<_>>();
    let mut active = Vec::<usize>::new();
    let mut next_range = 0usize;
    for row_id in include_row_ids.iter() {
        while next_range < ranges.len() && ranges[next_range].0 <= row_id {
            active.push(next_range);
            next_range += 1;
        }
        active.retain(|range_index| ranges[*range_index].1 >= row_id);
        for range_index in &active {
            let (start, _, original_index) = ranges[*range_index];
            local_filters[original_index].insert(row_id - start);
        }
        if next_range == ranges.len() && active.is_empty() {
            break;
        }
    }

    let mut plans = Vec::with_capacity(entries.len());
    for (entry, local_filter) in entries.into_iter().zip(local_filters) {
        if local_filter.is_empty() {
            continue;
        }
        plans.push(FullTextIndexSearchPlan {
            entry: entry.clone(),
            local_filter: Some(local_filter),
        });
    }
    Ok(plans)
}

fn is_full_text_index_file(index_file: &IndexFileMeta) -> bool {
    index_file.index_type == FULL_TEXT_INDEX_TYPE
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

async fn read_raw_full_text_search(
    table: &Table,
    search: &FullTextSearch,
    raw_ranges: &[RowRange],
    table_options: &HashMap<String, String>,
) -> crate::Result<SearchResult> {
    if raw_ranges.is_empty() {
        return Ok(SearchResult::empty());
    }
    let raw_ranges = merge_row_ranges(raw_ranges.to_vec());
    let row_range_start = raw_ranges.first().map(RowRange::from).unwrap_or(0);
    let row_range_end = raw_ranges
        .last()
        .map(RowRange::to)
        .unwrap_or(row_range_start);

    let mut read_builder = table.new_read_builder();
    read_builder
        .with_projection(&[search.field_name.as_str(), ROW_ID_FIELD_NAME])?
        .with_row_ranges(raw_ranges);
    let plan = read_builder.new_scan().plan().await?;
    if plan.splits().is_empty() {
        return Ok(SearchResult::empty());
    }
    let read = read_builder.new_read()?;
    let mut stream = read.to_arrow(plan.splits())?;

    let config = raw_full_text_config(table_options)?;
    let mut writer = tokio::task::spawn_blocking(move || {
        FullTextIndexWriter::new(config).map_err(full_text_error)
    })
    .await
    .map_err(raw_index_task_error)??;
    let mut document_count = 0usize;
    while let Some(batch) = stream.try_next().await? {
        let documents = raw_full_text_batch_documents(&batch, search, row_range_start)?;
        document_count += documents.len();
        writer = tokio::task::spawn_blocking(move || {
            for (row_id, text) in documents {
                writer.add_document(row_id, text).map_err(full_text_error)?;
            }
            Ok::<_, crate::Error>(writer)
        })
        .await
        .map_err(raw_index_task_error)??;
    }

    if document_count == 0 {
        return Ok(SearchResult::empty());
    }
    let temporary_index = tokio::task::spawn_blocking(move || {
        let mut temporary_index =
            tempfile::NamedTempFile::new().map_err(|error| crate::Error::UnexpectedError {
                message: format!("Failed to create temporary full-text index: {error}"),
                source: None,
            })?;
        writer
            .write(&mut PosWriter::new(temporary_index.as_file_mut()))
            .map_err(full_text_error)?;
        temporary_index.as_file_mut().sync_all().map_err(|error| {
            crate::Error::UnexpectedError {
                message: format!("Failed to flush temporary full-text index: {error}"),
                source: None,
            }
        })?;
        Ok::<_, crate::Error>(temporary_index)
    })
    .await
    .map_err(raw_index_task_error)??;
    let index_file = temporary_index
        .reopen()
        .map_err(|error| crate::Error::UnexpectedError {
            message: format!("Failed to reopen temporary full-text index: {error}"),
            source: None,
        })?;
    let local_filter = match search.include_row_ids.as_ref() {
        Some(include_row_ids) => Some(local_filter(
            include_row_ids,
            row_range_start,
            row_range_end,
        )?),
        None => None,
    };
    let result = search_full_text_file(
        index_file,
        "raw-full-text.index".to_string(),
        search.query_text.clone(),
        search.limit,
        local_filter,
    )
    .await?;
    search_result_from_core(result).map(|result| result.offset(row_range_start))
}

fn raw_index_task_error(error: tokio::task::JoinError) -> crate::Error {
    crate::Error::UnexpectedError {
        message: format!("Full-text raw index task failed: {error}"),
        source: None,
    }
}

fn raw_full_text_batch_documents(
    batch: &RecordBatch,
    search: &FullTextSearch,
    row_range_start: i64,
) -> crate::Result<Vec<(i64, String)>> {
    let text_index =
        batch
            .schema()
            .index_of(&search.field_name)
            .map_err(|e| crate::Error::DataInvalid {
                message: format!(
                    "Full-text column '{}' not found in raw search batch: {}",
                    search.field_name, e
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
            message: "Full-text raw search requires non-null Int64 _ROW_ID".to_string(),
            source: None,
        })?;
    let column = batch.column(text_index);

    if let Some(strings) = column.as_any().downcast_ref::<StringArray>() {
        let mut documents = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if let Some(document) = raw_full_text_document(
                row_ids,
                row,
                get_string_value(strings, row),
                row_range_start,
            )? {
                documents.push(document);
            }
        }
        return Ok(documents);
    }

    if let Some(strings) = column.as_any().downcast_ref::<LargeStringArray>() {
        let mut documents = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if let Some(document) = raw_full_text_document(
                row_ids,
                row,
                get_large_string_value(strings, row),
                row_range_start,
            )? {
                documents.push(document);
            }
        }
        return Ok(documents);
    }

    Err(crate::Error::DataInvalid {
        message: "Full-text raw search requires Utf8 or LargeUtf8 text column".to_string(),
        source: None,
    })
}

fn get_string_value(strings: &StringArray, row: usize) -> Option<&str> {
    if strings.is_null(row) {
        None
    } else {
        Some(strings.value(row))
    }
}

fn get_large_string_value(strings: &LargeStringArray, row: usize) -> Option<&str> {
    if strings.is_null(row) {
        None
    } else {
        Some(strings.value(row))
    }
}

fn raw_full_text_document(
    row_ids: &Int64Array,
    row: usize,
    text: Option<&str>,
    row_range_start: i64,
) -> crate::Result<Option<(i64, String)>> {
    if row_ids.is_null(row) {
        return Err(crate::Error::DataInvalid {
            message: "Full-text raw search found null _ROW_ID".to_string(),
            source: None,
        });
    }
    let row_id = row_ids.value(row);
    if row_id < 0 {
        return Err(crate::Error::DataInvalid {
            message: format!("Negative _ROW_ID {row_id} cannot be used for global index search"),
            source: None,
        });
    }
    match text {
        Some(text) => {
            let local_row_id =
                row_id
                    .checked_sub(row_range_start)
                    .ok_or_else(|| crate::Error::DataInvalid {
                        message: format!(
                        "Raw full-text row id {row_id} is before row range start {row_range_start}"
                    ),
                        source: None,
                    })?;
            Ok(Some((local_row_id, text.to_string())))
        }
        None => Ok(None),
    }
}

fn raw_full_text_config(
    table_options: &HashMap<String, String>,
) -> crate::Result<FullTextIndexConfig> {
    let native_options: HashMap<String, String> = table_options
        .iter()
        .filter_map(|(key, value)| {
            key.strip_prefix("full-text.")
                .map(|native_key| (native_key.to_string(), value.clone()))
        })
        .collect();
    FullTextIndexConfig::from_options(&native_options).map_err(full_text_error)
}

fn local_filter(
    global: &RoaringTreemap,
    row_range_start: i64,
    row_range_end: i64,
) -> crate::Result<RoaringTreemap> {
    if row_range_start < 0 || row_range_end < row_range_start {
        return Err(crate::Error::DataInvalid {
            message: format!("Invalid full-text row range [{row_range_start}, {row_range_end}]"),
            source: None,
        });
    }

    let start = u64::try_from(row_range_start).map_err(|_| crate::Error::DataInvalid {
        message: format!("Invalid negative row range start {row_range_start}"),
        source: None,
    })?;
    let end = u64::try_from(row_range_end).map_err(|_| crate::Error::DataInvalid {
        message: format!("Invalid negative row range end {row_range_end}"),
        source: None,
    })?;
    let mut local = RoaringTreemap::new();
    for row_id in global
        .iter()
        .filter(|row_id| *row_id >= start && *row_id <= end)
    {
        local.insert(row_id - start);
    }
    Ok(local)
}

fn normalize_query_text(query_text: &str, _field_name: &str) -> crate::Result<String> {
    if is_full_text_query_dsl(query_text) {
        return Ok(query_text.to_string());
    }
    serde_json::to_string(&json!({
        "match": {
            "query": query_text,
        }
    }))
    .map_err(|e| crate::Error::ConfigInvalid {
        message: format!("Failed to build full-text query JSON: {e}"),
    })
}

fn is_full_text_query_dsl(query_text: &str) -> bool {
    let Ok(serde_json::Value::Object(object)) =
        serde_json::from_str::<serde_json::Value>(query_text)
    else {
        return false;
    };
    object.keys().any(|key| {
        matches!(
            key.as_str(),
            "match" | "multi_match" | "match_phrase" | "phrase" | "boolean" | "boost"
        )
    })
}

fn search_result_from_core(
    result: paimon_ftindex_core::FullTextSearchResult,
) -> crate::Result<SearchResult> {
    let mut row_ids = Vec::with_capacity(result.row_ids.len());
    for row_id in result.row_ids {
        row_ids.push(
            u64::try_from(row_id).map_err(|_| crate::Error::DataInvalid {
                message: format!("Full-text index returned negative row id {row_id}"),
                source: None,
            })?,
        );
    }
    Ok(SearchResult::new(row_ids, result.scores))
}

fn full_text_error(error: impl std::fmt::Display) -> crate::Error {
    crate::Error::UnexpectedError {
        message: format!("Full-text index error: {error}"),
        source: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Identifier;
    use crate::io::FileIOBuilder;
    use crate::spec::{DataType, GlobalIndexMeta, IntType, Schema, TableSchema, VarCharType};
    use crate::table::table_write::TableWrite;
    use crate::table::TableCommit;
    use arrow_array::StringArray;
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use bytes::Bytes;
    use paimon_ftindex_core::io::SliceReader;
    use paimon_ftindex_core::FullTextIndexReader;
    use roaring::RoaringTreemap;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_evaluate_full_mode_without_fulltext_entries_returns_empty() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let fields = vec![DataField::new(
            1,
            "body".to_string(),
            DataType::Int(IntType::default()),
        )];
        let search = FullTextSearch::new("hello".to_string(), 10, "body".to_string()).unwrap();
        let options = HashMap::from([("global-index.search-mode".to_string(), "full".to_string())]);

        let result = evaluate_full_text_search(
            FullTextSearchEvaluation {
                table: None,
                file_io: &file_io,
                table_path: "memory:///test_table",
                table_options: &options,
                schema_fields: &fields,
                next_row_id: Some(10),
            },
            &[],
            &search,
        )
        .await
        .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_execute_full_mode_without_index_manifest_returns_empty() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/full_text_raw_no_manifest";
        setup_dirs(&file_io, table_path).await;
        let table = full_text_raw_table(&file_io, table_path);

        let mut table_write = TableWrite::new(&table, "test-user".to_string()).unwrap();
        table_write
            .write_arrow_batch(&text_batch(vec![
                "alphaft lakehouse",
                "betafft gammaft",
                "alphaft indexed",
                "gammaft fallback",
                "alphaft queried",
            ]))
            .await
            .unwrap();
        let messages = table_write.prepare_commit().await.unwrap();
        TableCommit::new(table.clone(), "test-user".to_string())
            .commit(messages)
            .await
            .unwrap();

        let mut builder = table.new_full_text_search_builder();
        builder
            .with_text_column("body")
            .with_query_text("alphaft")
            .with_limit(10);
        let row_ranges = builder.execute().await.unwrap();

        assert!(row_ranges.is_empty());
    }

    #[tokio::test]
    async fn test_raw_full_text_search_applies_include_row_ids() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/full_text_raw_with_filter";
        setup_dirs(&file_io, table_path).await;
        let table = full_text_raw_table(&file_io, table_path);

        let mut table_write = TableWrite::new(&table, "test-user".to_string()).unwrap();
        table_write
            .write_arrow_batch(&text_batch(vec!["hello world", "hello filtered"]))
            .await
            .unwrap();
        let messages = table_write.prepare_commit().await.unwrap();
        TableCommit::new(table.clone(), "test-user".to_string())
            .commit(messages)
            .await
            .unwrap();

        let mut filter = RoaringTreemap::new();
        filter.insert(1);

        let search = FullTextSearch::new(
            r#"{"match":{"column":"text","query":"hello"}}"#.to_string(),
            10,
            "body".to_string(),
        )
        .unwrap()
        .with_include_row_ids(filter);
        let row_ranges = read_raw_full_text_search(
            &table,
            &search,
            &[RowRange::new(0, 1)],
            table.schema().options(),
        )
        .await
        .unwrap()
        .to_row_ranges();

        assert_eq!(row_ranges, vec![RowRange::new(1, 1)]);
    }

    #[tokio::test]
    async fn test_evaluate_reads_new_full_text_index_file() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/full_text_indexed";
        setup_dirs(&file_io, table_path).await;
        file_io
            .mkdirs(&format!("{table_path}/index/"))
            .await
            .unwrap();

        let mut writer =
            FullTextIndexWriter::new(FullTextIndexConfig::new().with_text_fields(["body"]))
                .unwrap();
        writer
            .add_document_fields(0, [("body", "hello world")])
            .unwrap();
        writer
            .add_document_fields(1, [("body", "goodbye")])
            .unwrap();
        let mut index_bytes = Vec::new();
        writer.write(&mut PosWriter::new(&mut index_bytes)).unwrap();
        file_io
            .new_output(&format!("{table_path}/index/ft-0"))
            .unwrap()
            .write(Bytes::from(index_bytes.clone()))
            .await
            .unwrap();

        let fields = vec![DataField::new(
            1,
            "body".to_string(),
            DataType::VarChar(VarCharType::string_type()),
        )];
        let search = FullTextSearch::new(
            normalize_query_text("hello", "body").unwrap(),
            10,
            "body".to_string(),
        )
        .unwrap();
        let entry = IndexManifestEntry {
            kind: FileKind::Add,
            partition: Vec::new(),
            bucket: 0,
            index_file: IndexFileMeta {
                index_type: FULL_TEXT_INDEX_TYPE.to_string(),
                file_name: "ft-0".to_string(),
                file_size: i64::try_from(index_bytes.len()).unwrap(),
                row_count: 2,
                deletion_vectors_ranges: None,
                global_index_meta: Some(GlobalIndexMeta {
                    row_range_start: 100,
                    row_range_end: 101,
                    index_field_id: 1,
                    extra_field_ids: None,
                    index_meta: None,
                    source_meta: None,
                }),
            },
            version: 1,
        };
        let result = evaluate_full_text_search(
            FullTextSearchEvaluation {
                table: None,
                file_io: &file_io,
                table_path,
                table_options: &HashMap::new(),
                schema_fields: &fields,
                next_row_id: Some(102),
            },
            &[entry],
            &search,
        )
        .await
        .unwrap();

        assert_eq!(result.row_ids, vec![100]);
    }

    #[tokio::test]
    async fn test_indexed_full_text_search_offsets_include_row_ids() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/full_text_indexed_filter";
        setup_dirs(&file_io, table_path).await;
        file_io
            .mkdirs(&format!("{table_path}/index/"))
            .await
            .unwrap();

        let mut writer =
            FullTextIndexWriter::new(FullTextIndexConfig::new().with_text_fields(["body"]))
                .unwrap();
        writer
            .add_document_fields(0, [("body", "hello world")])
            .unwrap();
        writer
            .add_document_fields(1, [("body", "hello world")])
            .unwrap();
        let mut index_bytes = Vec::new();
        writer.write(&mut PosWriter::new(&mut index_bytes)).unwrap();
        file_io
            .new_output(&format!("{table_path}/index/ft-filter"))
            .unwrap()
            .write(Bytes::from(index_bytes.clone()))
            .await
            .unwrap();

        let fields = vec![DataField::new(
            1,
            "body".to_string(),
            DataType::VarChar(VarCharType::string_type()),
        )];
        let mut filter = RoaringTreemap::new();
        filter.insert(101);
        let search = FullTextSearch::new(
            normalize_query_text("hello", "body").unwrap(),
            10,
            "body".to_string(),
        )
        .unwrap()
        .with_include_row_ids(filter);
        let entry = IndexManifestEntry {
            kind: FileKind::Add,
            partition: Vec::new(),
            bucket: 0,
            index_file: IndexFileMeta {
                index_type: FULL_TEXT_INDEX_TYPE.to_string(),
                file_name: "ft-filter".to_string(),
                file_size: i64::try_from(index_bytes.len()).unwrap(),
                row_count: 2,
                deletion_vectors_ranges: None,
                global_index_meta: Some(GlobalIndexMeta {
                    row_range_start: 100,
                    row_range_end: 101,
                    index_field_id: 1,
                    extra_field_ids: None,
                    index_meta: None,
                    source_meta: None,
                }),
            },
            version: 1,
        };
        let missing_empty_filter_entry = full_text_index_entry("missing-filtered-shard", 200, 201);

        let result = evaluate_full_text_search(
            FullTextSearchEvaluation {
                table: None,
                file_io: &file_io,
                table_path,
                table_options: &HashMap::new(),
                schema_fields: &fields,
                next_row_id: Some(102),
            },
            &[entry, missing_empty_filter_entry],
            &search,
        )
        .await
        .unwrap();

        assert_eq!(result.row_ids, vec![101]);
    }

    #[test]
    fn test_normalize_query_text_only_passes_query_dsl_objects() {
        let normalized = normalize_query_text("123", "body").unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&normalized).unwrap(),
            serde_json::json!({"match": {"query": "123"}})
        );

        let normalized = normalize_query_text("\"hello\"", "body").unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&normalized).unwrap(),
            serde_json::json!({"match": {"query": "\"hello\""}})
        );

        let dsl = r#"{"match":{"query":"hello"}}"#;
        assert_eq!(normalize_query_text(dsl, "body").unwrap(), dsl);
    }

    #[test]
    fn test_raw_full_text_config_preserves_native_text_fields() {
        let options = HashMap::from([(
            "full-text.text-fields".to_string(),
            "native_text,secondary".to_string(),
        )]);

        let config = raw_full_text_config(&options).unwrap();

        assert_eq!(config.text_fields, vec!["native_text", "secondary"]);
        assert_eq!(config.default_text_field(), "native_text");
    }

    #[test]
    fn test_raw_batches_keep_one_bm25_corpus_and_configured_field() {
        let options = HashMap::from([(
            "full-text.text-field".to_string(),
            "native_text".to_string(),
        )]);
        let config = raw_full_text_config(&options).unwrap();
        let search = FullTextSearch::new(
            r#"{"match":{"column":"native_text","query":"rare common"}}"#.to_string(),
            10,
            "body".to_string(),
        )
        .unwrap();
        let batches = [
            raw_text_batch(&[10, 11], &["rare common", "common common common common"]),
            raw_text_batch(&[20, 21], &["rare", "unrelated"]),
        ];

        let mut batched_writer = FullTextIndexWriter::new(config.clone()).unwrap();
        let mut all_documents = Vec::new();
        for batch in &batches {
            let documents = raw_full_text_batch_documents(batch, &search, 10).unwrap();
            for (row_id, text) in &documents {
                batched_writer.add_document(*row_id, text).unwrap();
            }
            all_documents.extend(documents);
        }
        let mut reference_writer = FullTextIndexWriter::new(config).unwrap();
        for (row_id, text) in all_documents {
            reference_writer.add_document(row_id, text).unwrap();
        }

        let query = search.query_text.as_str();
        let batched_result = write_and_search_test_index(&mut batched_writer, query);
        let reference_result = write_and_search_test_index(&mut reference_writer, query);

        assert!(!batched_result.row_ids.is_empty());
        assert_eq!(batched_result.row_ids, reference_result.row_ids);
        assert_eq!(batched_result.scores, reference_result.scores);
    }

    fn raw_text_batch(row_ids: &[i64], texts: &[&str]) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("body", ArrowDataType::Utf8, true),
            ArrowField::new(ROW_ID_FIELD_NAME, ArrowDataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(texts.to_vec())),
                Arc::new(Int64Array::from(row_ids.to_vec())),
            ],
        )
        .unwrap()
    }

    fn write_and_search_test_index(
        writer: &mut FullTextIndexWriter,
        query: &str,
    ) -> paimon_ftindex_core::FullTextSearchResult {
        let mut bytes = Vec::new();
        writer.write(&mut PosWriter::new(&mut bytes)).unwrap();
        FullTextIndexReader::open(SliceReader::new(bytes))
            .unwrap()
            .search(query, 10)
            .unwrap()
    }

    #[test]
    fn test_plan_full_text_index_searches_partitions_once_and_preserves_order() {
        let entries = [
            full_text_index_entry("first", 100, 109),
            full_text_index_entry("second", 0, 9),
            full_text_index_entry("overlap", 104, 106),
            full_text_index_entry("empty", 200, 209),
        ];
        let mut include_row_ids = RoaringTreemap::new();
        include_row_ids.insert(1);
        include_row_ids.insert(105);
        include_row_ids.insert(999);

        let plans = plan_full_text_index_searches(entries.iter().collect(), Some(&include_row_ids))
            .unwrap();

        assert_eq!(
            plans
                .iter()
                .map(|plan| plan.entry.index_file.file_name.as_str())
                .collect::<Vec<_>>(),
            vec!["first", "second", "overlap"]
        );
        let decoded = plans
            .iter()
            .map(|plan| {
                plan.local_filter
                    .as_ref()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(decoded, vec![vec![5], vec![1], vec![1]]);
    }

    fn full_text_index_entry(name: &str, start: i64, end: i64) -> IndexManifestEntry {
        IndexManifestEntry {
            kind: FileKind::Add,
            partition: Vec::new(),
            bucket: 0,
            index_file: IndexFileMeta {
                index_type: FULL_TEXT_INDEX_TYPE.to_string(),
                file_name: name.to_string(),
                file_size: 0,
                row_count: i32::try_from(end - start + 1).unwrap(),
                deletion_vectors_ranges: None,
                global_index_meta: Some(GlobalIndexMeta {
                    row_range_start: start,
                    row_range_end: end,
                    index_field_id: 1,
                    extra_field_ids: None,
                    index_meta: None,
                    source_meta: None,
                }),
            },
            version: 1,
        }
    }

    async fn setup_dirs(file_io: &FileIO, table_path: &str) {
        file_io
            .mkdirs(&format!("{table_path}/snapshot/"))
            .await
            .unwrap();
        file_io
            .mkdirs(&format!("{table_path}/manifest/"))
            .await
            .unwrap();
    }

    fn full_text_raw_table(file_io: &FileIO, table_path: &str) -> Table {
        let schema = Schema::builder()
            .column("body", DataType::VarChar(VarCharType::string_type()))
            .option("row-tracking.enabled", "true")
            .option("global-index.search-mode", "full")
            .build()
            .unwrap();
        Table::new(
            file_io.clone(),
            Identifier::new("default", "full_text_raw_no_manifest"),
            table_path.to_string(),
            TableSchema::new(0, &schema),
            None,
        )
    }

    fn text_batch(values: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "body",
            ArrowDataType::Utf8,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values))]).unwrap()
    }

    #[tokio::test]
    async fn test_execute_fails_closed_when_query_auth_enabled() {
        let table = crate::table::query_auth_table();
        let err = table
            .new_full_text_search_builder()
            .execute()
            .await
            .unwrap_err();
        assert!(
            matches!(err, crate::Error::Unsupported { ref message } if message.contains("query-auth.enabled")),
            "full-text search must fail closed for a query-auth table"
        );
    }

    /// A primary-key full-text table: data-evolution off, `body` configured as a
    /// `pk-full-text.index.columns` entry, so a `body` query resolves to the PK
    /// full-text path. `extra` appends/overrides options (e.g. the search mode).
    fn pk_full_text_table(name: &str, extra: &[(&str, &str)]) -> Table {
        let mut builder = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("body", DataType::VarChar(VarCharType::string_type()))
            .primary_key(["id"])
            .option("bucket", "1")
            .option("deletion-vectors.enabled", "true")
            .option("pk-full-text.index.columns", "body");
        for (k, v) in extra {
            builder = builder.option(*k, *v);
        }
        let schema = builder.build().unwrap();
        Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("default", name),
            format!("memory:/{name}"),
            TableSchema::new(0, &schema),
            None,
        )
    }

    // (d) The PK full-text path produces physical positions, not global row-ids:
    // `execute` / `execute_scored` must fail loud and point callers at execute_read.
    #[tokio::test]
    async fn pk_full_text_execute_fails_loud_use_execute_read() {
        let table = pk_full_text_table("pk_ft_execute_loud", &[]);
        let err = table
            .new_full_text_search_builder()
            .with_text_column("body")
            .with_query_text(r#"{"match":{"query":"alpha"}}"#)
            .with_limit(10)
            .execute()
            .await
            .unwrap_err();
        assert!(
            matches!(&err, crate::Error::DataInvalid { message, .. } if message.contains("execute_read")),
            "PK full-text execute must fail loud pointing at execute_read, got: {err}"
        );
    }

    #[tokio::test]
    async fn pk_full_text_execute_scored_fails_loud_use_execute_read() {
        let table = pk_full_text_table("pk_ft_scored_loud", &[]);
        let err = table
            .new_full_text_search_builder()
            .with_text_column("body")
            .with_query_text(r#"{"match":{"query":"alpha"}}"#)
            .with_limit(10)
            .execute_scored()
            .await
            .unwrap_err();
        assert!(
            matches!(&err, crate::Error::DataInvalid { message, .. } if message.contains("global row ids")),
            "PK full-text execute_scored must fail loud, got: {err}"
        );
    }

    // (c) FAST-only: FULL / DETAIL global-index search modes must fail loud on the
    // PK full-text read rather than silently degrade.
    #[tokio::test]
    async fn pk_full_text_execute_read_rejects_full_mode() {
        let table = pk_full_text_table("pk_ft_full_mode", &[("global-index.search-mode", "full")]);
        let err = match table
            .new_full_text_search_builder()
            .with_text_column("body")
            .with_query_text(r#"{"match":{"query":"alpha"}}"#)
            .with_limit(10)
            .execute_read()
            .await
        {
            Ok(_) => panic!("FULL mode PK full-text read must fail loud"),
            Err(e) => e,
        };
        assert!(
            matches!(&err, crate::Error::DataInvalid { message, .. } if message.contains("FAST")),
            "FULL mode PK full-text read must fail loud, got: {err}"
        );
    }

    #[tokio::test]
    async fn pk_full_text_execute_read_rejects_detail_mode() {
        let table = pk_full_text_table(
            "pk_ft_detail_mode",
            &[("global-index.search-mode", "detail")],
        );
        let err = match table
            .new_full_text_search_builder()
            .with_text_column("body")
            .with_query_text(r#"{"match":{"query":"alpha"}}"#)
            .with_limit(10)
            .execute_read()
            .await
        {
            Ok(_) => panic!("DETAIL mode PK full-text read must fail loud"),
            Err(e) => e,
        };
        assert!(
            matches!(&err, crate::Error::DataInvalid { message, .. } if message.contains("FAST")),
            "DETAIL mode PK full-text read must fail loud, got: {err}"
        );
    }

    // (e) A non-PK (append) table: execute_read is unsupported and must fail loud
    // rather than silently return nothing; the append execute/execute_scored path
    // stays unaffected (exercised by the raw-search tests above).
    #[tokio::test]
    async fn append_execute_read_fails_loud_unsupported() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table = full_text_raw_table(&file_io, "memory:/append_ft_execute_read");
        let err = match table
            .new_full_text_search_builder()
            .with_text_column("body")
            .with_query_text(r#"{"match":{"query":"alpha"}}"#)
            .with_limit(10)
            .execute_read()
            .await
        {
            Ok(_) => panic!("append full-text execute_read must fail loud"),
            Err(e) => e,
        };
        assert!(
            matches!(&err, crate::Error::Unsupported { message } if message.contains("primary-key full-text path")),
            "append full-text execute_read must fail loud as unsupported, got: {err}"
        );
    }

    // Dispatch: with data-evolution ENABLED, a configured pk-full-text column must
    // NOT resolve to the PK path (mirrors Java `primaryKeyFullTextDefinition`), so
    // execute_scored takes the append/DE path instead of the PK fail-loud guard.
    #[tokio::test]
    async fn data_evolution_enabled_does_not_take_pk_path() {
        let de_on = HashMap::from([
            ("pk-full-text.index.columns".to_string(), "body".to_string()),
            ("data-evolution.enabled".to_string(), "true".to_string()),
        ]);
        let core = CoreOptions::new(&de_on);
        assert!(
            !resolves_to_pk_full_text_path(&core, "body"),
            "data-evolution on must not resolve to the PK full-text path"
        );
        // Off + configured column -> PK path; a non-configured column -> not PK.
        let de_off =
            HashMap::from([("pk-full-text.index.columns".to_string(), "body".to_string())]);
        let core_off = CoreOptions::new(&de_off);
        assert!(resolves_to_pk_full_text_path(&core_off, "body"));
        assert!(!resolves_to_pk_full_text_path(&core_off, "other"));
    }

    // A non-positive limit must fail loud at construction, even on an empty table
    // (before the empty-plan fast path).
    #[tokio::test]
    async fn pk_full_text_execute_read_rejects_zero_limit() {
        let table = pk_full_text_table("pk_ft_zero_limit", &[]);
        let err = match table
            .new_full_text_search_builder()
            .with_text_column("body")
            .with_query_text(r#"{"match":{"query":"alpha"}}"#)
            .with_limit(0)
            .execute_read()
            .await
        {
            Ok(_) => panic!("zero limit PK full-text read must fail loud"),
            Err(e) => e,
        };
        assert!(
            matches!(&err, crate::Error::ConfigInvalid { message } if message.contains("positive")),
            "zero limit must fail loud, got: {err}"
        );
    }
}
