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

//! Tantivy full-text index writer.
//!
//! Writes documents (rowId + text) to a local Tantivy index, then packs the
//! index directory into a single archive file (Big-Endian, Java-compatible)
//! and writes it to an `OutputFile`.
//!
//! Reference: `org.apache.paimon.tantivy.index.TantivyFullTextGlobalIndexWriter`

use bytes::Bytes;
use std::io::Read;

use crate::io::OutputFile;
use tantivy::schema::{Field, NumericOptions, Schema, TEXT};
use tantivy::{Index, IndexWriter, TantivyDocument};

/// Builds the fixed schema: `row_id` (u64 fast+stored+indexed) + `text` (full-text).
fn build_schema() -> (Schema, Field, Field) {
    let mut builder = Schema::builder();
    let row_id_field = builder.add_u64_field(
        "row_id",
        NumericOptions::default()
            .set_stored()
            .set_indexed()
            .set_fast(),
    );
    let text_field = builder.add_text_field("text", TEXT);
    (builder.build(), row_id_field, text_field)
}

/// Writer for creating a Tantivy full-text index and packing it into an archive.
pub struct TantivyFullTextWriter {
    writer: IndexWriter,
    row_id_field: Field,
    text_field: Field,
    temp_dir: tempfile::TempDir,
    row_count: u64,
}

impl TantivyFullTextWriter {
    /// Create a new writer. The index is built in a temporary directory.
    pub fn new() -> crate::Result<Self> {
        let temp_dir = tempfile::tempdir().map_err(|e| crate::Error::UnexpectedError {
            message: format!("Failed to create temp directory for Tantivy index: {}", e),
            source: None,
        })?;

        let (schema, row_id_field, text_field) = build_schema();
        let index = Index::create_in_dir(temp_dir.path(), schema).map_err(|e| {
            crate::Error::UnexpectedError {
                message: format!("Failed to create Tantivy index: {}", e),
                source: None,
            }
        })?;
        let writer = index
            .writer(50_000_000)
            .map_err(|e| crate::Error::UnexpectedError {
                message: format!("Failed to create Tantivy writer: {}", e),
                source: None,
            })?;

        Ok(Self {
            writer,
            row_id_field,
            text_field,
            temp_dir,
            row_count: 0,
        })
    }

    /// Add a document with the given row ID and text content.
    /// If text is None, the row ID is still incremented (null value).
    pub fn add_document(&mut self, row_id: u64, text: Option<&str>) -> crate::Result<()> {
        if let Some(text) = text {
            let mut doc = TantivyDocument::new();
            doc.add_u64(self.row_id_field, row_id);
            doc.add_text(self.text_field, text);
            self.writer
                .add_document(doc)
                .map_err(|e| crate::Error::UnexpectedError {
                    message: format!("Failed to add document: {}", e),
                    source: None,
                })?;
        }
        self.row_count += 1;
        Ok(())
    }

    /// Commit, pack the index into an archive, and write it to the given `OutputFile`.
    ///
    /// Returns `false` if no documents were written (nothing is written to the file).
    ///
    /// Reference: `TantivyFullTextGlobalIndexWriter.packIndex()`
    pub async fn finish(mut self, output: &OutputFile) -> crate::Result<bool> {
        if self.row_count == 0 {
            return Ok(false);
        }

        self.writer
            .commit()
            .map_err(|e| crate::Error::UnexpectedError {
                message: format!("Failed to commit Tantivy index: {}", e),
                source: None,
            })?;

        // Drop the writer to release file locks before packing.
        drop(self.writer);

        // Stream the archive directly to the OutputFile.
        let mut file_writer = output.writer().await?;

        // Collect file entries from the temp directory.
        let mut entries: Vec<(String, std::path::PathBuf)> = Vec::new();
        for entry in
            std::fs::read_dir(self.temp_dir.path()).map_err(|e| crate::Error::UnexpectedError {
                message: format!("Failed to read Tantivy index directory: {}", e),
                source: None,
            })?
        {
            let entry = entry.map_err(|e| crate::Error::UnexpectedError {
                message: format!("Failed to read directory entry: {}", e),
                source: None,
            })?;
            if entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
                let name = entry.file_name().to_string_lossy().to_string();
                entries.push((name, entry.path()));
            }
        }

        // Write file count (4 bytes BE).
        file_writer
            .write(Bytes::from((entries.len() as i32).to_be_bytes().to_vec()))
            .await?;

        // Write each file: name_len + name + data_len + data (chunked).
        const CHUNK_SIZE: usize = 4 * 1024 * 1024; // 4 MiB
        for (name, path) in &entries {
            let name_bytes = name.as_bytes();
            file_writer
                .write(Bytes::from(
                    (name_bytes.len() as i32).to_be_bytes().to_vec(),
                ))
                .await?;
            file_writer.write(Bytes::from(name_bytes.to_vec())).await?;

            let file_len = std::fs::metadata(path)
                .map_err(|e| crate::Error::UnexpectedError {
                    message: format!("Failed to stat index file '{}': {}", name, e),
                    source: None,
                })?
                .len();
            file_writer
                .write(Bytes::from((file_len as i64).to_be_bytes().to_vec()))
                .await?;

            let mut file =
                std::fs::File::open(path).map_err(|e| crate::Error::UnexpectedError {
                    message: format!("Failed to open index file '{}': {}", name, e),
                    source: None,
                })?;
            let mut buf = vec![0u8; CHUNK_SIZE];
            loop {
                let n = file
                    .read(&mut buf)
                    .map_err(|e| crate::Error::UnexpectedError {
                        message: format!("Failed to read index file '{}': {}", name, e),
                        source: None,
                    })?;
                if n == 0 {
                    break;
                }
                file_writer.write(Bytes::copy_from_slice(&buf[..n])).await?;
            }
        }

        file_writer.close().await?;
        Ok(true)
    }

    /// Number of rows processed (including nulls).
    pub fn row_count(&self) -> u64 {
        self.row_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIOBuilder;
    use crate::tantivy::reader::TantivyFullTextReader;

    #[tokio::test]
    async fn test_write_and_read_roundtrip() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();

        let mut writer = TantivyFullTextWriter::new().unwrap();
        writer.add_document(0, Some("hello world")).unwrap();
        writer.add_document(1, Some("foo bar baz")).unwrap();
        writer.add_document(2, None).unwrap();
        writer.add_document(3, Some("hello again")).unwrap();

        let output = file_io.new_output("/test_index.archive").unwrap();
        let written = writer.finish(&output).await.unwrap();
        assert!(written);

        let input = output.to_input_file();
        let reader = TantivyFullTextReader::from_input_file(&input)
            .await
            .unwrap();
        let result = reader.search("hello", 10).unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.row_ids.contains(&0));
        assert!(result.row_ids.contains(&3));
    }

    #[tokio::test]
    async fn test_empty_writer() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let output = file_io.new_output("/empty_index.archive").unwrap();

        let writer = TantivyFullTextWriter::new().unwrap();
        let written = writer.finish(&output).await.unwrap();
        assert!(!written);
        assert!(!output.exists().await.unwrap());
    }
}
