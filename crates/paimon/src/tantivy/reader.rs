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

//! Tantivy full-text index reader.
//!
//! Reads a Tantivy index from an archive (packed by `TantivyFullTextWriter`)
//! and performs full-text search queries.
//!
//! Reference: `org.apache.paimon.tantivy.index.TantivyFullTextGlobalIndexReader`

use crate::io::{FileRead, InputFile};
use crate::tantivy::directory::ArchiveDirectory;
use crate::tantivy::full_text_search::SearchResult;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::{Index, IndexReader, ReloadPolicy};

/// Reader for a Tantivy full-text index stored in archive format.
pub struct TantivyFullTextReader {
    reader: IndexReader,
    index: Index,
}

impl TantivyFullTextReader {
    /// Open a reader from an `InputFile` (on-demand reading, no full load).
    pub async fn from_input_file(input: &InputFile) -> crate::Result<Self> {
        let metadata = input.metadata().await?;
        let reader = input.reader().await?;
        Self::from_reader(reader, metadata.size).await
    }

    /// Open a reader from an async `FileRead` and file size.
    pub async fn from_reader(reader: impl FileRead, file_size: u64) -> crate::Result<Self> {
        let directory = ArchiveDirectory::from_reader(reader, file_size)
            .await
            .map_err(|e| crate::Error::UnexpectedError {
                message: format!("Failed to parse Tantivy archive: {}", e),
                source: None,
            })?;

        let index = Index::open(directory).map_err(|e| crate::Error::UnexpectedError {
            message: format!("Failed to open Tantivy index from archive: {}", e),
            source: None,
        })?;

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(|e| crate::Error::UnexpectedError {
                message: format!("Failed to create Tantivy reader: {}", e),
                source: None,
            })?;

        Ok(Self { reader, index })
    }

    /// Search the index and return top-N results ranked by score.
    pub fn search(&self, query_text: &str, limit: usize) -> crate::Result<SearchResult> {
        let schema = self.index.schema();
        let text_field = schema
            .get_field("text")
            .map_err(|_| crate::Error::UnexpectedError {
                message: "Tantivy schema missing 'text' field".to_string(),
                source: None,
            })?;

        let searcher = self.reader.searcher();
        let query_parser = QueryParser::for_index(&self.index, vec![text_field]);
        let query =
            query_parser
                .parse_query(query_text)
                .map_err(|e| crate::Error::UnexpectedError {
                    message: format!("Failed to parse query '{}': {}", query_text, e),
                    source: None,
                })?;

        let top_docs = searcher
            .search(&query, &TopDocs::with_limit(limit))
            .map_err(|e| crate::Error::UnexpectedError {
                message: format!("Tantivy search failed: {}", e),
                source: None,
            })?;

        let mut row_ids = Vec::with_capacity(top_docs.len());
        let mut scores = Vec::with_capacity(top_docs.len());

        for (score, doc_address) in &top_docs {
            let segment_reader = searcher.segment_reader(doc_address.segment_ord);
            let fast_fields = segment_reader.fast_fields().u64("row_id").map_err(|e| {
                crate::Error::UnexpectedError {
                    message: format!("Failed to get row_id fast field: {}", e),
                    source: None,
                }
            })?;
            let row_id = fast_fields.first(doc_address.doc_id).ok_or_else(|| {
                crate::Error::UnexpectedError {
                    message: format!(
                        "Missing row_id for doc_id {} in segment {}",
                        doc_address.doc_id, doc_address.segment_ord
                    ),
                    source: None,
                }
            })?;
            row_ids.push(row_id);
            scores.push(*score);
        }

        Ok(SearchResult::new(row_ids, scores))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIOBuilder;
    use crate::tantivy::writer::TantivyFullTextWriter;

    async fn create_test_reader() -> TantivyFullTextReader {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let mut writer = TantivyFullTextWriter::new().unwrap();
        writer
            .add_document(0, Some("the quick brown fox jumps over the lazy dog"))
            .unwrap();
        writer
            .add_document(1, Some("rust programming language"))
            .unwrap();
        writer
            .add_document(2, Some("apache paimon data lake"))
            .unwrap();
        writer
            .add_document(3, Some("full text search with tantivy"))
            .unwrap();
        writer
            .add_document(4, Some("the fox is quick and brown"))
            .unwrap();
        let output = file_io.new_output("/test_reader_index.archive").unwrap();
        writer.finish(&output).await.unwrap();
        TantivyFullTextReader::from_input_file(&output.to_input_file())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_search_basic() {
        let reader = create_test_reader().await;
        let result = reader.search("fox", 10).unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.row_ids.contains(&0));
        assert!(result.row_ids.contains(&4));
    }

    #[tokio::test]
    async fn test_search_limit() {
        let reader = create_test_reader().await;
        let result = reader.search("fox", 1).unwrap();
        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn test_search_no_match() {
        let reader = create_test_reader().await;
        let result = reader.search("nonexistent", 10).unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_search_scored() {
        let reader = create_test_reader().await;
        let result = reader.search("tantivy", 10).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.row_ids.contains(&3));
        assert!(result.scores[0] > 0.0);
    }

    #[tokio::test]
    async fn test_search_with_offset() {
        let reader = create_test_reader().await;
        let result = reader.search("fox", 10).unwrap();
        let offset_result = result.offset(1000);
        assert!(offset_result.row_ids.contains(&1000));
        assert!(offset_result.row_ids.contains(&1004));
    }
}
