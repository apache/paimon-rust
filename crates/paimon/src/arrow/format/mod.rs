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
mod avro_write;
pub(crate) mod blob;
mod delta_varint;
mod metadata_cache;
mod mosaic;
mod mosaic_write;
mod orc;
pub(crate) mod parquet;
mod row;
mod shredding;
pub(crate) mod text;
mod variant_projection;
pub(crate) mod video;
#[cfg(feature = "vortex")]
mod vortex;

pub(crate) use mosaic::MosaicPrefetchOptions;
#[cfg(test)]
pub(crate) use parquet::ParquetFormatWriter;

use super::ReadBudget;
use super::RowFilterFactory;
use crate::io::{FileIO, FileRead, OutputFile};
use crate::spec::stats::BinaryTableStats;
use crate::spec::{DataField, Predicate};
use crate::table::{ArrowRecordBatchStream, RowRange};
use crate::Error;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

/// Predicates with the file-level field context needed for pushdown.
/// Only used by formats that support predicate pushdown (e.g. Parquet).
pub(crate) struct FilePredicates {
    /// Predicates with indices already remapped to file-level fields.
    pub predicates: Vec<Predicate>,
    /// Optional engine-specific decoder filter factory.
    pub row_filter_factory: Option<Arc<dyn RowFilterFactory>>,
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
    /// Choose the fields the decoder must actually read. Most columnar formats
    /// can read the projection, while positional formats need the complete
    /// physical data schema to decode each row.
    fn select_read_fields(
        &self,
        _data_schema_fields: &[DataField],
        projected_fields: &[DataField],
    ) -> Vec<DataField> {
        projected_fields.to_vec()
    }

    /// Read a single data file, returning a stream of RecordBatches containing
    /// at least the projected columns (using names from the file's schema). A
    /// reader MAY include extra columns it needed to scan (e.g. predicate columns
    /// for residual filtering); the caller (`DataFileReader`) projects to the
    /// requested output by name, so extra columns are harmless.
    ///
    /// Predicate exactness is per-format, NOT a blanket guarantee:
    /// - Parquet, ORC, Avro, Row, Mosaic, Vortex, CSV, JSON, and TEXT apply the predicate **exactly** —
    ///   each emitted batch contains only rows matching the pushed-down predicate
    ///   (native pushdown for pruning + a row-level residual pass for the rest).
    /// - Blob does not evaluate predicates at all. Non-matching rows may survive,
    ///   and the caller must not assume exactness.
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

    /// Whether this writer still retains batch data. This can differ from
    /// `in_progress_size` while a format is inferring its physical schema.
    fn retains_batch_data(&self) -> bool {
        self.in_progress_size() != 0
    }

    /// Number of input rows still represented by buffered data, when known.
    /// Unlike `in_progress_size`, this allows accounting to release the part
    /// of a batch already written by an automatic row-group flush.
    fn pending_rows(&self) -> Option<usize> {
        None
    }

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

/// Account for batches retained by a format writer until its next flush.
/// The charge is an estimate of input Arrow buffers, not encoded allocations.
pub(crate) fn with_write_resources(
    writer: Box<dyn FormatFileWriter>,
    resources: Option<&crate::resource::ResourceContext>,
) -> Box<dyn FormatFileWriter> {
    match resources {
        Some(resources) => Box::new(ResourceFormatWriter {
            inner: writer,
            reservation: resources.reservation(),
            batches: VecDeque::new(),
            charged_rows: 0,
        }),
        None => writer,
    }
}

struct ResourceFormatWriter {
    inner: Box<dyn FormatFileWriter>,
    reservation: crate::resource::MemoryReservation,
    batches: VecDeque<BufferedBatchCharge>,
    charged_rows: usize,
}

struct BufferedBatchCharge {
    rows: usize,
    bytes: usize,
}

impl ResourceFormatWriter {
    fn release_flushed(&mut self) -> crate::Result<()> {
        let Some(pending_rows) = self.inner.pending_rows() else {
            if !self.inner.retains_batch_data() {
                self.batches.clear();
                self.charged_rows = 0;
                self.reservation.try_resize(0)?;
            }
            return Ok(());
        };
        if pending_rows == 0 {
            self.batches.clear();
            self.charged_rows = 0;
            return self.reservation.try_resize(0);
        }
        if pending_rows > self.charged_rows {
            // Keep the full charge if a writer reports more rows than we have
            // observed; releasing any amount would risk under-accounting.
            return Ok(());
        }
        let mut flushed_rows = self.charged_rows - pending_rows;
        let mut released_bytes = 0;
        while flushed_rows > 0 {
            let batch = self.batches.front_mut().expect("charged rows remain");
            let consumed_rows = flushed_rows.min(batch.rows);
            let remaining_rows = batch.rows - consumed_rows;
            let remaining_bytes = (batch.bytes as u128 * remaining_rows as u128)
                .div_ceil(batch.rows as u128) as usize;
            released_bytes += batch.bytes - remaining_bytes;
            flushed_rows -= consumed_rows;
            if remaining_rows == 0 {
                self.batches.pop_front();
            } else {
                batch.rows = remaining_rows;
                batch.bytes = remaining_bytes;
            }
        }
        self.charged_rows = pending_rows;
        self.reservation
            .try_resize(self.reservation.size() - released_bytes)
    }
}

#[async_trait]
impl FormatFileWriter for ResourceFormatWriter {
    async fn write(&mut self, batch: &RecordBatch) -> crate::Result<()> {
        let bytes = if batch.num_rows() == 0 {
            0
        } else {
            batch.get_array_memory_size()
        };
        self.reservation.try_grow(bytes)?;
        self.inner.write(batch).await?;
        if batch.num_rows() != 0 {
            self.batches.push_back(BufferedBatchCharge {
                rows: batch.num_rows(),
                bytes,
            });
            self.charged_rows += batch.num_rows();
        }
        self.release_flushed()
    }

    fn num_bytes(&self) -> usize {
        self.inner.num_bytes()
    }

    fn in_progress_size(&self) -> usize {
        self.inner.in_progress_size()
    }

    fn retains_batch_data(&self) -> bool {
        self.inner.retains_batch_data()
    }

    fn pending_rows(&self) -> Option<usize> {
        self.inner.pending_rows()
    }

    async fn flush(&mut self) -> crate::Result<()> {
        self.inner.flush().await?;
        self.release_flushed()
    }

    fn commit_field_metadata(
        &mut self,
        metadata: &crate::arrow::shredding::FieldMetadata,
    ) -> crate::Result<()> {
        self.inner.commit_field_metadata(metadata)
    }

    async fn close(self: Box<Self>) -> crate::Result<FormatWriteResult> {
        let Self {
            inner, reservation, ..
        } = *self;
        let result = inner.close().await;
        drop(reservation);
        result
    }
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

/// Rows in a data file of the given format, read from its footer alone, or `None` when the
/// format keeps no row count there and every row would have to be decoded to count them.
pub(crate) async fn read_file_row_count(
    file_io: &FileIO,
    format: &str,
    path: &str,
    file_size: u64,
) -> crate::Result<Option<i64>> {
    match format.to_ascii_lowercase().as_str() {
        "parquet" => {
            let reader = file_io.new_input(path)?.reader().await?;
            parquet::read_row_count(Box::new(reader), file_size)
                .await
                .map(Some)
        }
        "orc" => {
            let reader = file_io.new_input(path)?.reader().await?;
            orc::read_row_count(Box::new(reader), file_size)
                .await
                .map(Some)
        }
        _ => Ok(None),
    }
}

/// Create a format reader based on the file extension.
#[cfg(test)]
pub(crate) fn create_format_reader(
    path: &str,
    blob_as_descriptor: bool,
    read_fields: &[DataField],
) -> crate::Result<Box<dyn FormatFileReader>> {
    create_format_reader_with_budget(
        path,
        blob_as_descriptor,
        FormatReadFields {
            data_schema: read_fields,
            projected: read_fields,
        },
        &HashMap::new(),
        None,
        blob::DEFAULT_BLOB_READ_PARALLELISM,
        MosaicPrefetchOptions::default(),
    )
    .map(|configured| configured.reader)
}

pub(crate) struct ConfiguredFormatReader {
    pub reader: Box<dyn FormatFileReader>,
    pub read_fields: Vec<DataField>,
}

pub(crate) struct FormatReadFields<'a> {
    pub data_schema: &'a [DataField],
    pub projected: &'a [DataField],
}

/// Create a format reader with table options and runtime read resources.
pub(crate) fn create_format_reader_with_budget(
    path: &str,
    blob_as_descriptor: bool,
    fields: FormatReadFields<'_>,
    table_options: &HashMap<String, String>,
    parquet_read_budget: Option<Arc<ReadBudget>>,
    blob_parallelism: usize,
    mosaic_prefetch: MosaicPrefetchOptions,
) -> crate::Result<ConfiguredFormatReader> {
    let lower = path.to_ascii_lowercase();
    let reader: Box<dyn FormatFileReader> = if lower.ends_with(".parquet") {
        Box::new(parquet::ParquetFormatReader::with_options(
            table_options,
            parquet_read_budget,
        )?)
    } else if lower.ends_with(".blob") {
        Box::new(
            blob::BlobFormatReader::new(path.to_string(), blob_as_descriptor)
                .with_blob_parallelism(blob_parallelism),
        )
    } else if lower.ends_with(".video") {
        Box::new(video::VideoFormatReader::new(path.to_string()))
    } else if lower.ends_with(".orc") {
        Box::new(orc::OrcFormatReader)
    } else if let Some((kind, compression)) = text::TextKind::from_path(path) {
        Box::new(text::TextFormatReader::new(
            kind,
            compression,
            table_options,
        )?)
    } else if lower.ends_with(".avro") {
        Box::new(avro::AvroFormatReader)
    } else if lower.ends_with(".row") {
        Box::new(row::RowFormatReader)
    } else if lower.ends_with(".mosaic") {
        Box::new(mosaic::MosaicFormatReader::with_prefetch(mosaic_prefetch))
    } else {
        #[cfg(feature = "vortex")]
        if lower.ends_with(".vortex") {
            return Ok(configure_format_reader(
                Box::new(vortex::VortexFormatReader),
                fields,
            ));
        }
        return Err(Error::Unsupported {
            message: format!(
                "unsupported file format: expected {}, got: {path}",
                supported_read_formats().join(", ")
            ),
        });
    };
    Ok(configure_format_reader(reader, fields))
}

fn configure_format_reader(
    reader: Box<dyn FormatFileReader>,
    fields: FormatReadFields<'_>,
) -> ConfiguredFormatReader {
    let read_fields = reader.select_read_fields(fields.data_schema, fields.projected);
    ConfiguredFormatReader {
        reader: shredding::maybe_wrap_reader(reader, &read_fields),
        read_fields,
    }
}

fn supported_read_formats() -> Vec<&'static str> {
    vec![
        ".parquet",
        ".blob",
        ".video",
        ".orc",
        ".csv",
        ".json",
        ".text",
        ".avro",
        ".row",
        ".mosaic",
        #[cfg(feature = "vortex")]
        ".vortex",
    ]
}

fn supported_write_formats() -> Vec<&'static str> {
    vec![
        ".parquet",
        ".blob",
        ".orc",
        ".csv",
        ".json",
        ".text",
        ".avro",
        ".row",
        ".mosaic",
        #[cfg(feature = "vortex")]
        ".vortex",
    ]
}

/// Reject unavailable factories before a table writer accepts any data.
pub(crate) fn validate_write_format(format: &str) -> crate::Result<()> {
    let extension = format!(".{}", format.to_ascii_lowercase());
    let supported = supported_write_formats();
    if supported.contains(&extension.as_str()) {
        return Ok(());
    }
    Err(Error::Unsupported {
        message: format!(
            "unsupported write format '{format}': expected {}",
            supported.join(", ")
        ),
    })
}

/// Reusable factory for one rolling writer, mirroring Java FormatWriterFactory.
/// Output and compression belong to each file; schema/options belong to the factory.
#[async_trait]
pub(crate) trait FormatWriterFactory: Send + Sync {
    async fn create_writer(
        &self,
        output: &OutputFile,
        compression: &str,
    ) -> crate::Result<Box<dyn FormatFileWriter>>;

    /// Some plan factories need the previous close callback before the next file.
    fn needs_completed_file_stats(&self) -> bool {
        false
    }
}

/// Build a factory once per rolling writer. Statistics describe logical value
/// fields independently of a shredding plan's physical schema (notably for PK files).
pub(crate) fn create_format_writer_factory(
    file_format: &str,
    schema: SchemaRef,
    zstd_level: i32,
    file_io: Option<FileIO>,
    write_fields: Option<&[DataField]>,
    format_options: Option<&HashMap<String, String>>,
    stats_fields: Option<&[DataField]>,
) -> crate::Result<Arc<dyn FormatWriterFactory>> {
    let file_format = file_format.rsplit('.').next().unwrap_or_default();
    if file_format.eq_ignore_ascii_case("parquet") {
        let raw = Arc::new(parquet::ParquetWriterFactory::new(
            schema,
            write_fields.map(<[_]>::to_vec),
            zstd_level,
            format_options.cloned().unwrap_or_default(),
            stats_fields.map(<[_]>::to_vec),
        ));
        return shredding::wrap_writer_factory(raw, write_fields, format_options);
    }
    // Direct format writers can bypass table-schema validation. Never silently
    // drop a requested MAP layout when selecting an unsupported format.
    if let (Some(fields), Some(options)) = (write_fields, format_options) {
        if !crate::arrow::shredding::map::detect_map_shredding_fields(fields, options)?.is_empty() {
            crate::spec::map_shredding::validate_format("file.format", file_format)?;
        }
    }
    if file_format.eq_ignore_ascii_case("blob") {
        return Ok(Arc::new(blob::BlobWriterFactory::new(
            file_io,
            write_fields.and_then(|fields| fields.first()),
            format_options,
        )?));
    }
    Ok(Arc::new(PlainFormatWriterFactory {
        schema,
        zstd_level,
        write_fields: write_fields.map(<[_]>::to_vec),
        format_options: format_options.cloned(),
    }))
}

/// Convenience entry point for writing a single physical file.
pub(crate) async fn create_format_writer(
    output: &OutputFile,
    schema: SchemaRef,
    compression: &str,
    zstd_level: i32,
    file_io: Option<FileIO>,
    write_fields: Option<&[DataField]>,
    format_options: Option<&HashMap<String, String>>,
) -> crate::Result<Box<dyn FormatFileWriter>> {
    let file_format = output.location().rsplit('.').next().unwrap_or_default();
    create_format_writer_factory(
        file_format,
        schema,
        zstd_level,
        file_io,
        write_fields,
        format_options,
        None,
    )?
    .create_writer(output, compression)
    .await
}

struct PlainFormatWriterFactory {
    schema: SchemaRef,
    zstd_level: i32,
    write_fields: Option<Vec<DataField>>,
    format_options: Option<HashMap<String, String>>,
}

#[async_trait]
impl FormatWriterFactory for PlainFormatWriterFactory {
    async fn create_writer(
        &self,
        output: &OutputFile,
        compression: &str,
    ) -> crate::Result<Box<dyn FormatFileWriter>> {
        let schema = self.schema.clone();
        let zstd_level = self.zstd_level;
        let write_fields = self.write_fields.as_deref();
        let format_options = self.format_options.as_ref();
        let path = output.location();
        let lower = path.to_ascii_lowercase();
        if lower.ends_with(".orc") {
            if !matches!(
                compression.to_ascii_lowercase().as_str(),
                "" | "none" | "uncompressed"
            ) {
                return Err(Error::Unsupported {
                    message: format!(
                        "ORC compression '{compression}' is not supported by the current writer"
                    ),
                });
            }
            Ok(Box::new(orc::OrcFormatWriter::new(output, schema).await?))
        } else if let Some((kind, path_compression)) = text::TextKind::from_path(path) {
            let compression = text::TextCompression::from_name(compression)?;
            if compression != path_compression {
                return Err(Error::ConfigInvalid {
                    message: format!(
                        "Text file compression {:?} does not match file suffix of {path}",
                        compression
                    ),
                });
            }
            Ok(Box::new(
                text::TextFormatWriter::new(output, schema, kind, compression, format_options)
                    .await?,
            ))
        } else if lower.ends_with(".avro") {
            let fields = match write_fields {
                Some(fields) => fields.to_vec(),
                None => row::row_type_from_arrow_schema(&schema)?,
            };
            Ok(Box::new(
                avro_write::AvroFormatWriter::new(
                    output,
                    schema,
                    fields,
                    compression,
                    zstd_level,
                    format_options,
                )
                .await?,
            ))
        } else if lower.ends_with(".row") {
            let row_type = match write_fields {
                Some(fields) => fields.to_vec(),
                None => row::row_type_from_arrow_schema(&schema)?,
            };
            Ok(Box::new(
                row::RowFormatWriter::new(output, schema, row_type, zstd_level).await?,
            ))
        } else if lower.ends_with(".mosaic") {
            Ok(Box::new(
                mosaic_write::MosaicFormatWriter::new(
                    output,
                    schema,
                    compression,
                    zstd_level,
                    write_fields,
                    format_options,
                )
                .await?,
            ))
        } else {
            #[cfg(feature = "vortex")]
            if lower.ends_with(".vortex") {
                return Ok(Box::new(
                    vortex::VortexFormatWriter::new(output, schema).await?,
                ));
            }
            Err(Error::Unsupported {
                message: format!(
                    "unsupported write format: expected {}, got: {path}",
                    supported_write_formats().join(", ")
                ),
            })
        }
    }
}

fn timestamp_millis_schema(schema: &SchemaRef) -> SchemaRef {
    let fields = schema
        .fields()
        .iter()
        .map(timestamp_millis_field)
        .collect::<Vec<_>>();
    Arc::new(arrow_schema::Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    ))
}

fn timestamp_millis_field(field: &arrow_schema::FieldRef) -> arrow_schema::FieldRef {
    let data_type = timestamp_millis_data_type(field.data_type());
    if &data_type == field.data_type() {
        field.clone()
    } else {
        Arc::new(field.as_ref().clone().with_data_type(data_type))
    }
}

fn timestamp_millis_data_type(data_type: &arrow_schema::DataType) -> arrow_schema::DataType {
    use arrow_schema::DataType as ArrowDataType;

    match data_type {
        ArrowDataType::Timestamp(arrow_schema::TimeUnit::Second, timezone) => {
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Millisecond, timezone.clone())
        }
        ArrowDataType::List(field) => ArrowDataType::List(timestamp_millis_field(field)),
        ArrowDataType::LargeList(field) => ArrowDataType::LargeList(timestamp_millis_field(field)),
        ArrowDataType::FixedSizeList(field, size) => {
            ArrowDataType::FixedSizeList(timestamp_millis_field(field), *size)
        }
        ArrowDataType::Struct(fields) => ArrowDataType::Struct(
            fields
                .iter()
                .map(timestamp_millis_field)
                .collect::<Vec<_>>()
                .into(),
        ),
        ArrowDataType::Map(field, sorted) => {
            ArrowDataType::Map(timestamp_millis_field(field), *sorted)
        }
        _ => data_type.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::{DataType, IntType};

    #[test]
    fn write_format_validation_matches_compiled_factories() {
        for extension in supported_write_formats() {
            validate_write_format(extension.trim_start_matches('.')).unwrap();
        }
        validate_write_format("PARQUET").unwrap();
        assert!(matches!(
            validate_write_format("lance"),
            Err(Error::Unsupported { .. })
        ));
        assert_eq!(
            validate_write_format("vortex").is_ok(),
            cfg!(feature = "vortex")
        );
    }

    #[test]
    fn format_selects_physical_or_projected_fields() {
        let data_schema_fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "value".to_string(), DataType::Int(IntType::new())),
        ];
        let projected_fields = &data_schema_fields[1..];
        for (path, expected) in [
            ("data.row", data_schema_fields.as_slice()),
            ("data.parquet", projected_fields),
            ("data.orc", projected_fields),
            ("data.mosaic", projected_fields),
        ] {
            let configured = create_format_reader_with_budget(
                path,
                false,
                FormatReadFields {
                    data_schema: &data_schema_fields,
                    projected: projected_fields,
                },
                &HashMap::new(),
                None,
                blob::DEFAULT_BLOB_READ_PARALLELISM,
                MosaicPrefetchOptions::default(),
            )
            .unwrap();
            assert_eq!(configured.read_fields.as_slice(), expected, "{path}");
        }
    }

    #[tokio::test]
    async fn direct_writer_rejects_unsupported_map_shredding_before_creating_file() {
        use crate::spec::{MapType, VarCharType};
        let fields = vec![DataField::new(
            0,
            "tags".into(),
            DataType::Map(MapType::new(
                DataType::VarChar(VarCharType::string_type())
                    .copy_with_nullable(false)
                    .unwrap(),
                DataType::Int(IntType::new()),
            )),
        )];
        let schema = crate::arrow::build_target_arrow_schema(&fields).unwrap();
        // The actual output format must be checked even when table options
        // name Parquet (or the caller does not go through TableWrite).
        let options = HashMap::from([
            ("file.format".into(), "parquet".into()),
            (
                "fields.tags.map.storage-layout".into(),
                "shared-shredding".into(),
            ),
        ]);
        let io = FileIOBuilder::new("memory").build().unwrap();
        for format in ["orc", "ORC", "avro", "row"] {
            let path = format!("memory:/unsupported-map/data.{format}");
            let output = io.new_output(&path).unwrap();
            let error = create_format_writer(
                &output,
                schema.clone(),
                "none",
                0,
                None,
                Some(&fields),
                Some(&options),
            )
            .await
            .err()
            .expect("unsupported MAP layout must fail before writing");
            assert!(
                error.to_string().contains("only supports parquet"),
                "{error}"
            );
            assert!(!io.exists(&path).await.unwrap());
        }
        // A default layout still reaches the existing ORC type validation;
        // it must not be rejected as an unsupported shredding configuration.
        let options = HashMap::from([("fields.tags.map.storage-layout".into(), "default".into())]);
        let output = io.new_output("memory:/ordinary-map/data.orc").unwrap();
        let error = create_format_writer(
            &output,
            schema,
            "none",
            0,
            None,
            Some(&fields),
            Some(&options),
        )
        .await
        .err()
        .expect("ORC's existing writer does not support MAP columns");
        assert!(
            error
                .to_string()
                .contains("ORC writer does not support column"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn create_format_writer_error_lists_every_supported_format() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let output = file_io
            .new_output("memory:/unsupported/data.unknown")
            .unwrap();
        let schema = Arc::new(arrow_schema::Schema::empty());

        let err = match create_format_writer(&output, schema, "zstd", 1, None, None, None).await {
            Ok(_) => panic!("unknown is not a writable format"),
            Err(err) => err,
        };

        let Error::Unsupported { message } = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        for format in supported_write_formats() {
            assert!(
                message.contains(format),
                "{format} missing from write-format error: {message}"
            );
        }
        assert!(message.contains("data.unknown"), "message: {message}");
    }
}
