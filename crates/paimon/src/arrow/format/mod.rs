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
pub(crate) mod blob;
mod mosaic;
mod orc;
pub(crate) mod parquet;
mod row;
mod shredding;
#[cfg(feature = "vortex")]
mod vortex;

#[cfg(test)]
pub(crate) use parquet::ParquetFormatWriter;

use crate::io::{FileRead, OutputFile};
use crate::spec::stats::BinaryTableStats;
use crate::spec::{DataField, Predicate};
use crate::table::{ArrowRecordBatchStream, RowRange};
use crate::Error;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use std::collections::HashMap;

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
/// - Predicate pushdown where supported (row-group/stripe pruning and, for
///   some formats, row-level filtering)
/// - Row range selection
#[async_trait]
pub(crate) trait FormatFileReader: Send + Sync {
    /// Read a single data file, returning a stream of RecordBatches containing
    /// at least the projected columns (using names from the file's schema). A
    /// reader MAY include extra columns it needed to scan (e.g. predicate columns
    /// for residual filtering); the caller (`DataFileReader`) projects to the
    /// requested output by name, so extra columns are harmless.
    ///
    /// Predicate exactness is per-format, NOT a blanket guarantee:
    /// - Parquet, ORC, Avro, Row, and Vortex apply the predicate **exactly** —
    ///   each emitted batch contains only rows matching the pushed-down predicate
    ///   (native pushdown for pruning + a row-level residual pass for the rest).
    /// - Blob does not evaluate predicates at all; Mosaic applies only
    ///   stats-level (row-group) pruning. For those, non-matching rows may
    ///   survive and the caller must not assume exactness.
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

    /// Commit per-field metadata into the file footer, called by the shredding
    /// wrapper just before [`close`](FormatFileWriter::close).
    ///
    /// Only formats whose footer can carry per-field key/value metadata
    /// (Parquet) implement this; the default errors, mirroring Java's
    /// `FormatFileWriter.commitShreddingMetadata` support matrix.
    fn commit_field_metadata(
        &mut self,
        _field_metadata: &crate::arrow::shredding::FieldMetadata,
    ) -> crate::Result<()> {
        Err(Error::Unsupported {
            message: "committing shredding field metadata is not supported by this format"
                .to_string(),
        })
    }

    /// Flush and close the writer, finalizing the file on storage.
    async fn close(self: Box<Self>) -> crate::Result<FormatWriteResult>;
}

pub(crate) struct FormatWriteResult {
    pub(crate) file_size: u64,
    pub(crate) value_stats: Option<FormatValueStats>,
}

pub(crate) struct FormatValueStats {
    pub(crate) stats: BinaryTableStats,
    pub(crate) columns: Option<Vec<String>>,
}

impl FormatWriteResult {
    pub(crate) fn new(file_size: u64) -> Self {
        Self {
            file_size,
            value_stats: None,
        }
    }

    pub(crate) fn with_value_stats(
        file_size: u64,
        value_stats: BinaryTableStats,
        columns: Option<Vec<String>>,
    ) -> Self {
        Self {
            file_size,
            value_stats: Some(FormatValueStats {
                stats: value_stats,
                columns,
            }),
        }
    }
}

/// Create a format reader based on the file extension.
pub(crate) fn create_format_reader(
    path: &str,
    blob_as_descriptor: bool,
    read_fields: &[DataField],
) -> crate::Result<Box<dyn FormatFileReader>> {
    let lower = path.to_ascii_lowercase();
    let reader: Box<dyn FormatFileReader> = if lower.ends_with(".parquet") {
        Box::new(parquet::ParquetFormatReader)
    } else if lower.ends_with(".blob") {
        Box::new(blob::BlobFormatReader::new(
            path.to_string(),
            blob_as_descriptor,
        ))
    } else if lower.ends_with(".orc") {
        Box::new(orc::OrcFormatReader)
    } else if lower.ends_with(".avro") {
        Box::new(avro::AvroFormatReader)
    } else if lower.ends_with(".row") {
        Box::new(row::RowFormatReader)
    } else {
        if lower.ends_with(".mosaic") {
            return Ok(shredding::maybe_wrap_reader(
                Box::new(mosaic::MosaicFormatReader),
                read_fields,
            ));
        }
        #[cfg(feature = "vortex")]
        if lower.ends_with(".vortex") {
            return Ok(shredding::maybe_wrap_reader(
                Box::new(vortex::VortexFormatReader),
                read_fields,
            ));
        }
        return Err(Error::Unsupported {
            message: format!(
                "unsupported file format: expected {}, got: {path}",
                supported_read_formats().join(", ")
            ),
        });
    };
    Ok(shredding::maybe_wrap_reader(reader, read_fields))
}

fn supported_read_formats() -> Vec<&'static str> {
    vec![
        ".parquet",
        ".blob",
        ".orc",
        ".avro",
        ".row",
        ".mosaic",
        #[cfg(feature = "vortex")]
        ".vortex",
    ]
}

/// Create a format writer that streams directly to storage.
pub(crate) async fn create_format_writer(
    output: &OutputFile,
    schema: SchemaRef,
    compression: &str,
    zstd_level: i32,
    file_io: Option<crate::io::FileIO>,
    write_fields: Option<&[DataField]>,
    format_options: Option<&HashMap<String, String>>,
) -> crate::Result<Box<dyn FormatFileWriter>> {
    let path = output.location();
    let lower = path.to_ascii_lowercase();
    if lower.ends_with(".parquet") {
        let writer_factory = Box::new(parquet::ParquetPhysicalWriterFactory::new(
            output,
            compression,
            zstd_level,
            format_options.cloned().unwrap_or_default(),
        ));
        shredding::ShreddingFormatWriter::create(
            writer_factory,
            schema,
            write_fields,
            format_options,
            compression,
        )
        .await
    } else if lower.ends_with(".blob") {
        Ok(Box::new(
            blob::BlobFormatWriter::new(output, file_io).await?,
        ))
    } else if lower.ends_with(".row") {
        let row_type = match write_fields {
            Some(fields) => fields.to_vec(),
            None => row::row_type_from_arrow_schema(&schema)?,
        };
        Ok(Box::new(
            row::RowFormatWriter::new(output, schema, row_type, zstd_level).await?,
        ))
    } else {
        #[cfg(feature = "vortex")]
        if lower.ends_with(".vortex") {
            return Ok(Box::new(
                vortex::VortexFormatWriter::new(output, schema).await?,
            ));
        }
        Err(Error::Unsupported {
            message: format!("unsupported write format: expected .parquet, .row, got: {path}"),
        })
    }
}
