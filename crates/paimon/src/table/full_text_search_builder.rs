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

use crate::io::{FileIO, FileIOBuilder};
use crate::spec::{
    CoreOptions, DataField, FileKind, GlobalIndexSearchMode, IndexFileMeta, IndexManifest,
    IndexManifestEntry, ROW_ID_FIELD_NAME,
};
use crate::table::global_index_scanner::{
    deleted_row_ranges_for_data_evolution_dvs, search_limit_with_deleted_rows,
    unindexed_ranges_for_global_index_entries, RowRangeIndex,
};
use crate::table::snapshot_manager::SnapshotManager;
use crate::table::{find_field_id_by_name, merge_row_ranges, RowRange, Table};
use crate::tantivy::full_text_search::{FullTextSearch, SearchResult};
use crate::tantivy::reader::TantivyFullTextReader;
use crate::tantivy::writer::TantivyFullTextWriter;
use arrow_array::{Array, Int64Array, LargeStringArray, RecordBatch, StringArray};
use futures::TryStreamExt;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

const INDEX_DIR: &str = "index";
const TANTIVY_FULLTEXT_INDEX_TYPE: &str = "tantivy-fulltext";

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
}

impl<'a> FullTextSearchBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            text_column: None,
            query_text: None,
            limit: None,
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

    /// Set the top-k limit for results.
    pub fn with_limit(&mut self, limit: usize) -> &mut Self {
        self.limit = Some(limit);
        self
    }

    /// Execute the full-text search and return row ranges.
    ///
    /// This reads the latest snapshot, loads the index manifest, and evaluates
    /// the search against Tantivy indexes.
    ///
    /// Reference: `FullTextSearchBuilder.executeLocal()`
    pub async fn execute(&self) -> crate::Result<Vec<RowRange>> {
        // Fail closed: returns data-derived row ranges outside `TableScan`/`TableRead`.
        CoreOptions::new(self.table.schema().options()).ensure_read_authorized()?;
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

        let search = FullTextSearch::new(query_text.to_string(), limit, text_column.to_string())?;

        let snapshot_manager = SnapshotManager::new(
            self.table.file_io().clone(),
            self.table.location().to_string(),
        );

        let snapshot = match snapshot_manager.get_latest_snapshot().await? {
            Some(s) => s,
            None => return Ok(Vec::new()),
        };

        let index_entries = match snapshot.index_manifest() {
            Some(index_manifest_name) => {
                let manifest_path = format!(
                    "{}/manifest/{}",
                    self.table.location().trim_end_matches('/'),
                    index_manifest_name
                );
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
}

/// Evaluate a full-text search query against Tantivy indexes found in the index manifest.
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
) -> crate::Result<Vec<RowRange>> {
    let table_path = evaluation.table_path.trim_end_matches('/');
    let core_options = CoreOptions::new(evaluation.table_options);
    let search_mode = core_options.global_index_search_mode()?;

    let field_id = match find_field_id_by_name(evaluation.schema_fields, &search.field_name) {
        Some(id) => id,
        None => return Ok(Vec::new()),
    };

    // Collect tantivy fulltext entries for the target field.
    let fulltext_entries: Vec<_> = index_entries
        .iter()
        .filter(|e| {
            e.kind == FileKind::Add
                && e.index_file.index_type == TANTIVY_FULLTEXT_INDEX_TYPE
                && e.index_file
                    .global_index_meta
                    .as_ref()
                    .is_some_and(|m| m.index_field_id == field_id)
        })
        .collect();

    if fulltext_entries.is_empty() && search_mode == GlobalIndexSearchMode::Fast {
        return Ok(Vec::new());
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
        let futures: Vec<_> = fulltext_entries
            .into_iter()
            .map(|entry| {
                let global_meta = entry.index_file.global_index_meta.as_ref().unwrap();
                let path = format!("{table_path}/{INDEX_DIR}/{}", entry.index_file.file_name);
                let file_name = entry.index_file.file_name.clone();
                let query_text = search.query_text.clone();
                let row_range_start = global_meta.row_range_start;
                let row_range_end = global_meta.row_range_end;
                let limit = search_limit_with_deleted_rows(
                    search.limit,
                    row_range_start,
                    row_range_end,
                    deleted_row_index.as_ref(),
                );
                let input = evaluation.file_io.new_input(&path);
                async move {
                    let input = input?;
                    let reader = TantivyFullTextReader::from_input_file(&input)
                        .await
                        .map_err(|e| crate::Error::UnexpectedError {
                            message: format!(
                                "Failed to open Tantivy full-text index '{}': {}",
                                file_name, e
                            ),
                            source: None,
                        })?;
                    let result = reader.search(&query_text, limit)?;
                    Ok::<_, crate::Error>(result.offset(row_range_start))
                }
            })
            .collect();

        let results = futures::future::try_join_all(futures).await?;
        for r in &results {
            merged = merged.or(r);
        }
    }

    if search_mode != GlobalIndexSearchMode::Fast {
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
            is_tantivy_fulltext_index_file,
        );
        if !raw_ranges.is_empty() {
            let table = evaluation.table.ok_or_else(|| crate::Error::DataInvalid {
                message: "Full-text raw search requires table context".to_string(),
                source: None,
            })?;
            let raw_result = read_raw_full_text_search(table, search, &raw_ranges).await?;
            merged = merged.or(&raw_result);
        }
    }

    Ok(merged
        .without_deleted_row_ranges(deleted_row_index.as_ref())?
        .top_k(search.limit)
        .to_row_ranges())
}

fn is_tantivy_fulltext_index_file(index_file: &IndexFileMeta) -> bool {
    index_file.index_type == TANTIVY_FULLTEXT_INDEX_TYPE
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
) -> crate::Result<SearchResult> {
    if raw_ranges.is_empty() {
        return Ok(SearchResult::empty());
    }

    let mut read_builder = table.new_read_builder();
    read_builder
        .with_projection(&[search.field_name.as_str(), ROW_ID_FIELD_NAME])?
        .with_row_ranges(raw_ranges.to_vec());
    let plan = read_builder.new_scan().plan().await?;
    if plan.splits().is_empty() {
        return Ok(SearchResult::empty());
    }
    let read = read_builder.new_read()?;
    let mut stream = read.to_arrow(plan.splits())?;

    let mut writer = TantivyFullTextWriter::new()?;
    while let Some(batch) = stream.try_next().await? {
        add_raw_full_text_batch(&batch, search, &mut writer)?;
    }

    let memory_io = FileIOBuilder::new("memory").build()?;
    let output = memory_io.new_output(&format!("/raw-fulltext-{}.archive", Uuid::new_v4()))?;
    if !writer.finish(&output).await? {
        return Ok(SearchResult::empty());
    }
    let input = output.to_input_file();
    let reader = TantivyFullTextReader::from_input_file(&input)
        .await
        .map_err(|e| crate::Error::UnexpectedError {
            message: format!("Failed to open raw Tantivy full-text index: {e}"),
            source: None,
        })?;
    reader.search(&search.query_text, search.limit)
}

fn add_raw_full_text_batch(
    batch: &RecordBatch,
    search: &FullTextSearch,
    writer: &mut TantivyFullTextWriter,
) -> crate::Result<()> {
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
        for row in 0..batch.num_rows() {
            add_raw_full_text_row(row_ids, row, get_string_value(strings, row), writer)?;
        }
        return Ok(());
    }

    if let Some(strings) = column.as_any().downcast_ref::<LargeStringArray>() {
        for row in 0..batch.num_rows() {
            add_raw_full_text_row(row_ids, row, get_large_string_value(strings, row), writer)?;
        }
        return Ok(());
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

fn add_raw_full_text_row(
    row_ids: &Int64Array,
    row: usize,
    text: Option<&str>,
    writer: &mut TantivyFullTextWriter,
) -> crate::Result<()> {
    if row_ids.is_null(row) {
        return Err(crate::Error::DataInvalid {
            message: "Full-text raw search found null _ROW_ID".to_string(),
            source: None,
        });
    }
    let row_id = u64::try_from(row_ids.value(row)).map_err(|_| crate::Error::DataInvalid {
        message: format!(
            "Negative _ROW_ID {} cannot be used for global index search",
            row_ids.value(row)
        ),
        source: None,
    })?;
    writer.add_document(row_id, text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Identifier;
    use crate::spec::{DataType, IntType, Schema, TableSchema, VarCharType};
    use crate::table::table_write::TableWrite;
    use crate::table::TableCommit;
    use arrow_array::StringArray;
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_evaluate_full_mode_without_fulltext_entries_uses_raw_path() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let fields = vec![DataField::new(
            1,
            "body".to_string(),
            DataType::Int(IntType::default()),
        )];
        let search = FullTextSearch::new("hello".to_string(), 10, "body".to_string()).unwrap();
        let options = HashMap::from([("global-index.search-mode".to_string(), "full".to_string())]);

        let err = evaluate_full_text_search(
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
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("Full-text raw search requires table context"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_execute_full_mode_without_index_manifest_searches_raw_rows() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let table_path = "memory:/full_text_raw_no_manifest";
        setup_dirs(&file_io, table_path).await;
        let table = full_text_raw_table(&file_io, table_path);

        let mut table_write = TableWrite::new(&table, "test-user".to_string()).unwrap();
        table_write
            .write_arrow_batch(&text_batch(vec!["hello world", "goodbye"]))
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
            .with_query_text("hello")
            .with_limit(10);
        let row_ranges = builder.execute().await.unwrap();

        assert_eq!(row_ranges, vec![RowRange::new(0, 0)]);
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
}
