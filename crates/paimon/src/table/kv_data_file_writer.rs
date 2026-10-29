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

//! Physical KV file output shared by write-buffer flushes and compaction.
//! Inputs are already ordered thin batches: [sequence, kind, value columns...].

use super::data_file_index_writer::{DataFileIndexWriter, FileIndexOptions};
use super::data_file_path_factory::{DataFilePath, DataFilePathFactory};
use super::managed_blob_reference::ManagedBlobReferences;
use crate::arrow::format::{
    create_format_writer_factory, with_write_resources, FormatFileWriter, FormatWriterFactory,
};
use crate::io::FileIO;
use crate::resource::ResourceContext;
use crate::spec::stats::{BinaryTableStats, ColumnStatsCollector};
use crate::spec::{
    data_file_to_file_index_file_name, extract_datum_from_arrow, BigIntType, BinaryRowBuilder,
    CoreOptions, DataField, DataFileMeta, DataType, RowKind, TinyIntType, KEY_FIELD_ID_START,
    SEQUENCE_NUMBER_FIELD_ID, SEQUENCE_NUMBER_FIELD_NAME, VALUE_KIND_FIELD_ID,
    VALUE_KIND_FIELD_NAME,
};
use crate::{Error, Result};
use arrow_array::{Array, Int64Array, Int8Array, RecordBatch, UInt64Array};
use arrow_schema::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef,
};
use std::collections::HashMap;
use std::sync::Arc;

/// Immutable physical-file settings; no sequence allocator or merge-engine state.
#[derive(Clone, Copy)]
pub(super) struct KeyValueFileOptions<'a> {
    pub file_io: &'a FileIO,
    pub paths: &'a DataFilePathFactory,
    pub table_options: &'a HashMap<String, String>,
    pub value_fields: &'a [DataField],
    pub primary_key_indices: &'a [usize],
    pub primary_key_types: &'a [DataType],
    pub schema_id: i64,
    pub file_format: &'a str,
    pub file_prefix: &'a str,
    pub file_compression: &'a str,
    pub file_compression_zstd_level: i32,
    pub write_buffer_size: i64,
    pub level: i32,
    pub file_source: i32,
    pub is_changelog: bool,
    pub index_options: Option<&'a FileIndexOptions>,
    pub managed_blob_enabled: bool,
    pub resources: Option<&'a ResourceContext>,
}

struct RowKeyProjection {
    schema: Arc<ArrowSchema>,
    column_indices: Vec<usize>,
}

impl RowKeyProjection {
    fn project(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let columns = self
            .column_indices
            .iter()
            .map(|&index| Arc::clone(batch.column(index)))
            .collect();
        RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(|error| {
            crate::Error::DataInvalid {
                message: format!("Failed to prepend ROW key columns: {error}"),
                source: None,
            }
        })
    }
}

impl<'a> KeyValueFileOptions<'a> {
    pub(super) fn new_format_writer_factory(
        &self,
        mut schema: arrow_schema::SchemaRef,
    ) -> Result<Arc<dyn FormatWriterFactory>> {
        schema = canonical_schema(&schema)?;
        let mut physical_fields = build_physical_fields(&schema, self.value_fields)?;
        let mut options = self.table_options.clone();
        let mode = CoreOptions::new(&options)
            .pk_file_metadata_stats_mode(self.level, self.is_changelog)?
            .to_string();
        options.insert("metadata.stats-mode".to_string(), mode);
        let file_format = self.file_format;
        if file_format.eq_ignore_ascii_case("row") {
            let projection = self.row_key_projection(&schema)?;
            let key_count = self.primary_key_indices.len();
            physical_fields = projection
                .column_indices
                .iter()
                .enumerate()
                .map(|(index, &source)| {
                    let field = physical_fields[source].clone();
                    if index < key_count {
                        let id = field.id() + KEY_FIELD_ID_START;
                        field
                            .with_id(id)
                            .with_name(projection.schema.field(index).name().clone())
                    } else {
                        field
                    }
                })
                .collect();
            schema = projection.schema;
        }
        create_format_writer_factory(
            file_format,
            schema,
            self.file_compression_zstd_level,
            None,
            Some(&physical_fields),
            Some(&options),
            Some(self.value_fields),
        )
    }

    fn row_key_projection(&self, schema: &Arc<ArrowSchema>) -> Result<RowKeyProjection> {
        let mut fields = Vec::new();
        let mut column_indices = Vec::new();
        // These indices contain only trimmed keys; partition key columns are
        // stored in the value portion but do not belong in the key prefix.
        for &index in self.primary_key_indices {
            let key = &self.value_fields[index];
            let source =
                schema
                    .index_of(key.name())
                    .map_err(|error| crate::Error::DataInvalid {
                        message: format!(
                            "ROW key '{}' is absent from the physical schema: {error}",
                            key.name()
                        ),
                        source: None,
                    })?;
            let arrow_field = schema.field(source);
            fields.push(Arc::new(ArrowField::new(
                format!("_KEY_{}", key.name()),
                arrow_field.data_type().clone(),
                arrow_field.is_nullable(),
            )));
            column_indices.push(source);
        }
        fields.extend(schema.fields().iter().cloned());
        column_indices.extend(0..schema.fields().len());
        Ok(RowKeyProjection {
            schema: Arc::new(ArrowSchema::new_with_metadata(
                fields,
                schema.metadata().clone(),
            )),
            column_indices,
        })
    }

    pub(super) async fn open(
        self,
        schema: &SchemaRef,
        factory: &dyn FormatWriterFactory,
        ordinal: usize,
    ) -> Result<KeyValueDataFileWriter<'a>> {
        let schema = canonical_schema(schema)?;
        let row_projection = self
            .file_format
            .eq_ignore_ascii_case("row")
            .then(|| self.row_key_projection(&schema))
            .transpose()?;
        let index = if self.is_changelog {
            None
        } else {
            self.index_options
                .map(FileIndexOptions::create_writer)
                .transpose()?
        };
        let blob_references = ManagedBlobReferences::new(
            self.value_fields,
            &CoreOptions::new(self.table_options),
            &schema,
            self.managed_blob_enabled,
            self.is_changelog,
        )?;
        let name = format!(
            "{}{}-{}.{}",
            self.file_prefix,
            uuid::Uuid::new_v4(),
            ordinal,
            self.file_format
        );
        let location = self.paths.new_path(&name)?;
        self.file_io
            .mkdirs(&format!("{}/", location.parent()))
            .await?;
        let output = self.file_io.new_output(&location.path)?;
        let writer = match factory.create_writer(&output, self.file_compression).await {
            Ok(writer) => with_write_resources(writer, self.resources),
            Err(error) => {
                let _ = self.file_io.delete_file(&location.path).await;
                return Err(error);
            }
        };
        Ok(KeyValueDataFileWriter {
            options: self,
            schema,
            row_projection,
            name,
            location,
            writer: Some(writer),
            index,
            blob_references,
            key_columns: self.primary_key_indices.iter().map(|i| i + 2).collect(),
            key_stats: ColumnStatsCollector::new(self.primary_key_types),
            rows: 0,
            min_key: vec![],
            max_key: vec![],
            min_sequence: i64::MAX,
            max_sequence: i64::MIN,
            deletes: 0,
            buffered_input_bytes: 0,
            needs_memory_roll: false,
        })
    }
}

/// One physical file. Flush releases format buffers; close finishes the file.
pub(super) struct KeyValueDataFileWriter<'a> {
    options: KeyValueFileOptions<'a>,
    schema: SchemaRef,
    row_projection: Option<RowKeyProjection>,
    name: String,
    location: DataFilePath,
    writer: Option<Box<dyn FormatFileWriter>>,
    index: Option<DataFileIndexWriter>,
    blob_references: Option<ManagedBlobReferences>,
    key_columns: Vec<usize>,
    key_stats: ColumnStatsCollector<'a>,
    rows: usize,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
    min_sequence: i64,
    max_sequence: i64,
    deletes: i64,
    buffered_input_bytes: usize,
    needs_memory_roll: bool,
}

impl KeyValueDataFileWriter<'_> {
    pub(super) async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        // Normalize Arrow field metadata/nullability without changing column order
        // or projecting physical rows back through the logical write-buffer schema.
        let batch = RecordBatch::try_new(self.schema.clone(), batch.columns().to_vec())
            .map_err(|e| invalid(format!("Invalid physical KV batch: {e}")))?;
        let sequences = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("KV sequence column must be Int64"))?;
        let kinds = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| invalid("KV kind column must be Int8"))?;
        for row in 0..batch.num_rows() {
            self.min_sequence = self.min_sequence.min(sequences.value(row));
            self.max_sequence = self.max_sequence.max(sequences.value(row));
            if RowKind::from_value(kinds.value(row))?.is_retract() {
                self.deletes += 1;
            }
        }
        if self.rows == 0 {
            self.min_key = self.key(&batch, 0)?;
        }
        self.max_key = self.key(&batch, batch.num_rows() - 1)?;
        self.key_stats.collect(&batch, &self.key_columns)?;
        ManagedBlobReferences::collect(&mut self.blob_references, &batch)?;
        let projected = self
            .row_projection
            .as_ref()
            .map(|p| p.project(&batch))
            .transpose()?;
        let writer = self.writer.as_mut().unwrap();
        writer.write(projected.as_ref().unwrap_or(&batch)).await?;
        if let Some(index) = self.index.as_mut() {
            let columns = (2..batch.num_columns()).collect::<Vec<_>>();
            let values = batch
                .project(&columns)
                .map_err(|e| invalid(format!("Failed to project KV index values: {e}")))?;
            index.write(&values)?;
        }
        self.rows += batch.num_rows();
        self.buffered_input_bytes = self
            .buffered_input_bytes
            .saturating_add(batch.get_array_memory_size());
        if !retains_input_rows(writer.as_ref()) {
            self.buffered_input_bytes = 0;
        }
        self.needs_memory_roll = false;
        if self.buffered_input_bytes as i64 >= self.options.write_buffer_size
            || writer.in_progress_size() as i64 >= self.options.write_buffer_size
        {
            writer.flush().await?;
            // Mosaic/Vortex may retain their pending rows after flush. Closing
            // is the bounded fallback when a format cannot release its buffer.
            self.needs_memory_roll = retains_input_rows(writer.as_ref());
            if !self.needs_memory_roll {
                self.buffered_input_bytes = 0;
            }
        }
        Ok(())
    }

    fn key(&self, batch: &RecordBatch, row: usize) -> Result<Vec<u8>> {
        let mut builder = BinaryRowBuilder::new(self.key_columns.len() as i32);
        for (pos, (&column, ty)) in self
            .key_columns
            .iter()
            .zip(self.options.primary_key_types)
            .enumerate()
        {
            match extract_datum_from_arrow(batch, row, column, ty)? {
                Some(value) => builder.write_datum(pos, &value, ty),
                None => builder.set_null_at(pos),
            }
        }
        Ok(builder.build_serialized())
    }

    pub(super) fn rows(&self) -> usize {
        self.rows
    }
    pub(super) fn num_bytes(&self) -> usize {
        self.writer.as_ref().unwrap().num_bytes()
    }

    pub(super) async fn close(mut self) -> Result<DataFileMeta> {
        let result = self.finish().await;
        if result.is_err() {
            self.abort().await;
        }
        result
    }

    async fn finish(&mut self) -> Result<DataFileMeta> {
        if self.rows == 0 {
            return Err(invalid("Cannot close an empty key-value file"));
        }
        let result = self.writer.take().unwrap().close().await?;
        let (value_stats, value_stats_cols) = result
            .value_stats
            .map(|stats| (stats.stats, stats.columns))
            .unwrap_or_else(|| (BinaryTableStats::empty(), Some(vec![])));
        let mut meta = DataFileMeta {
            file_name: self.name.clone(),
            file_size: result.file_size as i64,
            row_count: self.rows as i64,
            min_key: std::mem::take(&mut self.min_key),
            max_key: std::mem::take(&mut self.max_key),
            key_stats: self.key_stats.finish(),
            value_stats,
            min_sequence_number: self.min_sequence,
            max_sequence_number: self.max_sequence,
            schema_id: self.options.schema_id,
            level: self.options.level,
            extra_files: vec![],
            creation_time: None,
            delete_row_count: Some(self.deletes),
            embedded_index: None,
            file_source: Some(self.options.file_source),
            value_stats_cols,
            external_path: self.location.external_path.clone(),
            first_row_id: None,
            write_cols: None,
            column_max_sequence_numbers: None,
        };
        if let Some(index) = self.index.take() {
            let bytes = index.serialize()?;
            if bytes.len() as u64 > self.options.index_options.unwrap().in_manifest_threshold as u64
            {
                let name = data_file_to_file_index_file_name(&self.name);
                self.options
                    .file_io
                    .new_output(&format!("{}/{}", self.location.parent(), name))?
                    .write(bytes)
                    .await?;
                meta.extra_files.push(name);
            } else {
                meta.embedded_index = Some(bytes.to_vec());
            }
        }
        ManagedBlobReferences::finish(
            self.blob_references.take(),
            self.options.file_io,
            &self.location.path,
            self.location.parent(),
            &mut meta,
        )
        .await?;
        meta.creation_time = Some(crate::common::date_time::current_local_timestamp());
        Ok(meta)
    }

    pub(super) async fn abort(mut self) {
        if let Some(writer) = self.writer.take() {
            let _ = writer.close().await;
        }
        let _ = self.options.file_io.delete_file(&self.location.path).await;
        if self.options.index_options.is_some() && !self.options.is_changelog {
            let path = format!(
                "{}/{}",
                self.location.parent(),
                data_file_to_file_index_file_name(&self.name)
            );
            let _ = self.options.file_io.delete_file(&path).await;
        }
    }
}

/// Roll an ordered KV stream independently of input batch and buffer boundaries.
pub(super) struct KeyValueRollingWriter<'a> {
    options: KeyValueFileOptions<'a>,
    target_size: i64,
    target_rows: usize,
    factory: Option<Arc<dyn FormatWriterFactory>>,
    current: Option<KeyValueDataFileWriter<'a>>,
    files: Vec<DataFileMeta>,
}

impl<'a> KeyValueRollingWriter<'a> {
    pub(super) fn new(
        options: KeyValueFileOptions<'a>,
        target_size: i64,
        target_rows: i64,
    ) -> Self {
        Self {
            options,
            target_size,
            target_rows: target_rows.try_into().unwrap_or(usize::MAX),
            factory: None,
            current: None,
            files: vec![],
        }
    }

    pub(super) async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let mut offset = 0;
        while offset < batch.num_rows() {
            if self.current.is_none() {
                if self.factory.is_none() {
                    self.factory = Some(self.options.new_format_writer_factory(batch.schema())?);
                }
                self.current = Some(
                    self.options
                        .open(
                            &batch.schema(),
                            self.factory.as_ref().unwrap().as_ref(),
                            self.files.len(),
                        )
                        .await?,
                );
            }
            let file = self.current.as_mut().unwrap();
            let rows = (self.target_rows - file.rows())
                .min(batch.num_rows() - offset)
                .min(4096);
            if offset == 0 && rows == batch.num_rows() {
                file.write(batch).await?;
            } else {
                // A zero-copy slice retains the whole source allocation and is
                // charged as such by ResourceFormatWriter. Materialize bounded
                // chunks so each row limit/format write owns only its rows.
                let indices =
                    UInt64Array::from_iter_values((offset..offset + rows).map(|i| i as u64));
                let chunk = arrow_select::take::take_record_batch(batch, &indices)
                    .map_err(|e| invalid(format!("Failed to materialize KV output chunk: {e}")))?;
                file.write(&chunk).await?;
            }
            offset += rows;
            if file.rows() >= self.target_rows
                || file.num_bytes() as i64 >= self.target_size
                || file.needs_memory_roll
            {
                self.close_current().await?;
            }
        }
        Ok(())
    }

    async fn close_current(&mut self) -> Result<()> {
        if let Some(file) = self.current.take() {
            self.files.push(file.close().await?);
        }
        Ok(())
    }

    pub(super) async fn finish(&mut self) -> Result<()> {
        self.close_current().await
    }

    pub(super) async fn prepare_commit(&mut self) -> Result<Vec<DataFileMeta>> {
        self.finish().await?;
        Ok(std::mem::take(&mut self.files))
    }

    pub(super) async fn abort(&mut self) {
        if let Some(file) = self.current.take() {
            file.abort().await;
        }
        for file in self.files.drain(..) {
            for path in file.collect_files(self.options.paths.bucket_path()) {
                let _ = self.options.file_io.delete_file(&path).await;
            }
        }
    }
}

fn canonical_schema(schema: &ArrowSchema) -> Result<SchemaRef> {
    if schema.fields().len() < 2
        || schema.field(0).name() != SEQUENCE_NUMBER_FIELD_NAME
        || schema.field(1).name() != VALUE_KIND_FIELD_NAME
    {
        return Err(invalid(
            "Expected thin KV columns: sequence, kind, then values",
        ));
    }
    let mut fields = schema.fields().to_vec();
    fields[0] = Arc::new(ArrowField::new(
        SEQUENCE_NUMBER_FIELD_NAME,
        ArrowDataType::Int64,
        false,
    ));
    fields[1] = Arc::new(ArrowField::new(
        VALUE_KIND_FIELD_NAME,
        ArrowDataType::Int8,
        false,
    ));
    Ok(Arc::new(ArrowSchema::new(fields)))
}

fn invalid(message: impl Into<String>) -> Error {
    Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

fn retains_input_rows(writer: &dyn FormatFileWriter) -> bool {
    // ROW keeps a nonempty block header after flush, even with no pending rows.
    writer
        .pending_rows()
        .map_or_else(|| writer.retains_batch_data(), |rows| rows != 0)
}

fn build_physical_fields(
    physical_schema: &ArrowSchema,
    value_fields: &[DataField],
) -> Result<Vec<DataField>> {
    physical_schema
        .fields()
        .iter()
        .map(|arrow_field| match arrow_field.name().as_str() {
            SEQUENCE_NUMBER_FIELD_NAME => Ok(DataField::new(
                SEQUENCE_NUMBER_FIELD_ID,
                SEQUENCE_NUMBER_FIELD_NAME.into(),
                DataType::BigInt(BigIntType::new()),
            )),
            VALUE_KIND_FIELD_NAME => Ok(DataField::new(
                VALUE_KIND_FIELD_ID,
                VALUE_KIND_FIELD_NAME.into(),
                DataType::TinyInt(TinyIntType::new()),
            )),
            name => value_fields
                .iter()
                .find(|field| field.name() == name)
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: format!(
                        "Physical file column '{name}' is missing from the table schema"
                    ),
                    source: None,
                })
                .and_then(|field| {
                    // The table remains logically NOT NULL, but Java's KV
                    // value type makes non-key DELETE payloads nullable.
                    Ok(DataField::new(
                        field.id(),
                        field.name().into(),
                        field
                            .data_type()
                            .copy_with_nullable(arrow_field.is_nullable())?,
                    )
                    .with_description(field.description().map(str::to_owned))
                    .with_default_value(field.default_value().map(str::to_owned)))
                }),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::IntType;
    use arrow_array::Int32Array;

    async fn write_with_budget(
        format: &str,
        buffer: &str,
        rows: usize,
        batches: usize,
        limit: usize,
        expected_files: usize,
    ) {
        let io = FileIOBuilder::new("memory").build().unwrap();
        let path = format!("memory:/kv_stream/{}", uuid::Uuid::new_v4());
        let options = HashMap::from([
            ("write-buffer-size".into(), buffer.into()),
            ("file.block-size".into(), "64mb".into()),
            ("file.compression".into(), "zstd".into()),
        ]);
        let core = CoreOptions::new(&options);
        let fields = vec![
            DataField::new(0, "id".into(), DataType::Int(IntType::new())),
            DataField::new(1, "value".into(), DataType::Int(IntType::new())),
        ];
        let types = vec![DataType::Int(IntType::new())];
        let paths = DataFilePathFactory::new(&path, "", 0, &options).unwrap();
        let resources = ResourceContext::builder()
            .memory_limit(limit)
            .build()
            .unwrap();
        let mut writer = KeyValueRollingWriter::new(
            KeyValueFileOptions {
                file_io: &io,
                paths: &paths,
                table_options: &options,
                value_fields: &fields,
                primary_key_indices: &[0],
                primary_key_types: &types,
                schema_id: 0,
                file_format: format,
                file_prefix: "data-",
                file_compression: "zstd",
                file_compression_zstd_level: 0,
                write_buffer_size: core.write_buffer_size().unwrap() as i64,
                level: 1,
                file_source: 1,
                is_changelog: false,
                index_options: None,
                managed_blob_enabled: false,
                resources: Some(&resources),
            },
            256 * 1024 * 1024,
            i64::MAX,
        );
        for batch in 0..batches {
            let start = batch * rows;
            let data = RecordBatch::try_from_iter([
                (
                    SEQUENCE_NUMBER_FIELD_NAME,
                    Arc::new(Int64Array::from_iter_values(
                        (start..start + rows).map(|i| i as i64),
                    )) as arrow_array::ArrayRef,
                ),
                (
                    VALUE_KIND_FIELD_NAME,
                    Arc::new(Int8Array::from(vec![0; rows])) as arrow_array::ArrayRef,
                ),
                (
                    "id",
                    Arc::new(Int32Array::from_iter_values(
                        (start..start + rows).map(|i| i as i32),
                    )) as arrow_array::ArrayRef,
                ),
                (
                    "value",
                    Arc::new(Int32Array::from(vec![10; rows])) as arrow_array::ArrayRef,
                ),
            ])
            .unwrap();
            writer.write(&data).await.unwrap();
        }
        let files = writer.prepare_commit().await.unwrap();
        assert_eq!(files.len(), expected_files, "{format}");
        assert_eq!(
            files.iter().map(|file| file.row_count).sum::<i64>(),
            (rows * batches) as i64
        );
        assert_eq!(resources.metrics().reserved_memory_bytes, 0);
        assert!(resources.metrics().peak_reserved_memory_bytes <= limit);
    }

    #[tokio::test]
    async fn large_input_batches_charge_only_materialized_output_chunks() {
        write_with_budget("parquet", "64mb", 32768, 1, 1024 * 1024, 1).await;
    }

    #[tokio::test]
    async fn formats_that_retain_rows_after_flush_keep_bounded_output_files() {
        #[cfg(feature = "vortex")]
        let formats = ["mosaic", "vortex"];
        #[cfg(not(feature = "vortex"))]
        let formats = ["mosaic"];
        for format in formats {
            write_with_budget(format, "8kb", 1024, 8, 64 * 1024, 8).await;
        }
    }
}
