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

mod avro;
mod orc;
mod parquet;

use crate::io::{FileRead, OutputFile};
use crate::spec::{DataField, Predicate};
use crate::table::{ArrowRecordBatchStream, RowRange};
use crate::Error;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;

/// Predicates with the file-level field context needed for pushdown.
/// Only used by formats that support predicate pushdown (e.g. Parquet).
pub(crate) struct FilePredicates {
    /// Predicates with indices already remapped to file-level fields.
    pub predicates: Vec<Predicate>,
    /// File-level fields (full file schema), used for stats access and row filtering.
    pub file_fields: Vec<DataField>,
}

/// Format-agnostic file reader that produces Arrow RecordBatch streams.
///
/// Each implementation (Parquet, ORC, ...) handles:
/// - Column projection
/// - Predicate pushdown (row-group/stripe pruning + row-level filtering)
/// - Row range selection
#[async_trait]
pub(crate) trait FormatFileReader: Send + Sync {
    /// Read a single data file, returning a stream of RecordBatches
    /// containing only the projected columns (using names from the file's schema).
    ///
    /// `row_selection` is a pre-merged list of 0-based inclusive row ranges
    /// (DV + row_ranges already combined by the caller).
    async fn read_batch_stream(
        &self,
        reader: Box<dyn FileRead>,
        file_size: u64,
        read_fields: &[DataField],
        predicates: Option<&FilePredicates>,
        batch_size: Option<usize>,
        row_selection: Option<Vec<RowRange>>,
    ) -> crate::Result<ArrowRecordBatchStream>;
}

/// Format-agnostic file writer that streams Arrow RecordBatches directly to storage.
///
/// Each implementation (Parquet, ORC, ...) handles format-specific encoding.
/// Usage: create via [`create_format_writer`], call [`write`](FormatFileWriter::write)
/// for each batch, then [`close`](FormatFileWriter::close) to finalize the file.
#[async_trait]
pub(crate) trait FormatFileWriter: Send {
    /// Write a RecordBatch to the underlying storage.
    async fn write(&mut self, batch: &RecordBatch) -> crate::Result<()>;

    /// Number of bytes written so far (approximate, before close).
    fn num_bytes(&self) -> usize;

    /// Number of bytes buffered in the current row group (not yet flushed).
    fn in_progress_size(&self) -> usize;

    /// Flush the current row group to storage without closing the file.
    async fn flush(&mut self) -> crate::Result<()>;

    /// Flush and close the writer, finalizing the file on storage.
    /// Returns the total number of bytes written.
    async fn close(self: Box<Self>) -> crate::Result<u64>;
}

/// Create a format reader based on the file extension.
pub(crate) fn create_format_reader(path: &str) -> crate::Result<Box<dyn FormatFileReader>> {
    if path.to_ascii_lowercase().ends_with(".parquet") {
        Ok(Box::new(parquet::ParquetFormatReader))
    } else if path.to_ascii_lowercase().ends_with(".orc") {
        Ok(Box::new(orc::OrcFormatReader))
    } else if path.to_ascii_lowercase().ends_with(".avro") {
        Ok(Box::new(avro::AvroFormatReader))
    } else {
        Err(Error::Unsupported {
            message: format!(
                "unsupported file format: expected .parquet, .orc, or .avro, got: {path}"
            ),
        })
    }
}

/// Create a format writer that streams directly to storage.
pub(crate) async fn create_format_writer(
    output: &OutputFile,
    schema: SchemaRef,
    compression: &str,
    zstd_level: i32,
) -> crate::Result<Box<dyn FormatFileWriter>> {
    let path = output.location();
    if path.to_ascii_lowercase().ends_with(".parquet") {
        Ok(Box::new(
            parquet::ParquetFormatWriter::new(output, schema, compression, zstd_level).await?,
        ))
    } else {
        Err(Error::Unsupported {
            message: format!("unsupported write format: expected .parquet, got: {path}"),
        })
    }
}
