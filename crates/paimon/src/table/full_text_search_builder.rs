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

use crate::spec::{DataField, FileKind, IndexManifest};
use crate::table::snapshot_manager::SnapshotManager;
use crate::table::{RowRange, Table};
use crate::tantivy::full_text_search::{FullTextSearch, SearchResult};
use crate::tantivy::reader::TantivyFullTextReader;

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

        let index_manifest_name = match snapshot.index_manifest() {
            Some(name) => name.to_string(),
            None => return Ok(Vec::new()),
        };

        let manifest_path = format!(
            "{}/manifest/{}",
            self.table.location().trim_end_matches('/'),
            index_manifest_name
        );
        let index_entries = IndexManifest::read(self.table.file_io(), &manifest_path).await?;

        evaluate_full_text_search(
            self.table.file_io(),
            self.table.location(),
            &index_entries,
            &search,
            self.table.schema().fields(),
        )
        .await
    }
}

/// Evaluate a full-text search query against Tantivy indexes found in the index manifest.
async fn evaluate_full_text_search(
    file_io: &crate::io::FileIO,
    table_path: &str,
    index_entries: &[crate::spec::IndexManifestEntry],
    search: &FullTextSearch,
    schema_fields: &[DataField],
) -> crate::Result<Vec<RowRange>> {
    let table_path = table_path.trim_end_matches('/');

    let field_id = match find_field_id_by_name(schema_fields, &search.field_name) {
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

    if fulltext_entries.is_empty() {
        return Ok(Vec::new());
    }

    let futures: Vec<_> = fulltext_entries
        .into_iter()
        .map(|entry| {
            let global_meta = entry.index_file.global_index_meta.as_ref().unwrap();
            let path = format!("{table_path}/{INDEX_DIR}/{}", entry.index_file.file_name);
            let file_name = entry.index_file.file_name.clone();
            let query_text = search.query_text.clone();
            let limit = search.limit;
            let row_range_start = global_meta.row_range_start;
            let input = file_io.new_input(&path);
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
    let mut merged = SearchResult::empty();
    for r in &results {
        merged = merged.or(r);
    }

    Ok(merged.top_k(search.limit).to_row_ranges())
}

fn find_field_id_by_name(fields: &[DataField], name: &str) -> Option<i32> {
    fields.iter().find(|f| f.name() == name).map(|f| f.id())
}
