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

//! Java-compatible `.row` file format.
//!
//! Layout reference:
//! `org.apache.paimon.format.row.RowFormatWriter` in paimon-java.

use super::{FilePredicates, FormatFileReader, FormatFileWriter};
use crate::arrow::{arrow_to_paimon_type, build_target_arrow_schema, paimon_type_to_arrow};
use crate::io::{FileRead, FileWrite, OutputFile};
use crate::spec::{DataField, DataType, IntType};
use crate::table::{ArrowRecordBatchStream, RowRange};
use crate::Error;
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder, StringBuilder,
    Time32MillisecondBuilder, TimestampMicrosecondBuilder, TimestampMillisecondBuilder,
    TimestampNanosecondBuilder,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, ListArray, MapArray, RecordBatch,
    RecordBatchOptions, StringArray, StructArray, Time32MillisecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
};
use arrow_buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType as ArrowDataType, Field, Fields, SchemaRef, TimeUnit};
use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use std::sync::Arc;

pub(crate) struct RowFormatReader;

pub(crate) struct RowFormatWriter {
    writer: Box<dyn FileWrite>,
    row_type: Vec<DataField>,
    block_size_threshold: usize,
    zstd_level: i32,
    block_writer: RowBlockWriter,
    block_compressed_sizes: Vec<i64>,
    block_uncompressed_sizes: Vec<i64>,
    block_row_starts: Vec<i64>,
    bytes_written: u64,
    total_row_count: i64,
}

const DEFAULT_BLOCK_SIZE: usize = 65_536;
const DEFAULT_BATCH_SIZE: usize = 1024;
const ROW_BLOCK_READ_CONCURRENCY: usize = 8;
const FOOTER_SIZE: u64 = 32;
const ROW_MAGIC: i32 = 0x524F5753;
const ROW_VERSION: u8 = 1;

impl RowFormatWriter {
    pub(crate) async fn new(
        output: &OutputFile,
        schema: SchemaRef,
        row_type: Vec<DataField>,
        zstd_level: i32,
    ) -> crate::Result<Self> {
        validate_supported_types(&row_type)?;
        validate_arrow_schema_for_row(&schema, &row_type)?;

        Ok(Self {
            writer: output.writer().await?,
            row_type,
            block_size_threshold: DEFAULT_BLOCK_SIZE,
            zstd_level,
            block_writer: RowBlockWriter::new(),
            block_compressed_sizes: Vec::new(),
            block_uncompressed_sizes: Vec::new(),
            block_row_starts: Vec::new(),
            bytes_written: 0,
            total_row_count: 0,
        })
    }

    async fn flush_block(&mut self) -> crate::Result<()> {
        if self.block_writer.row_count() == 0 {
            return Ok(());
        }

        self.block_row_starts
            .push(self.total_row_count - self.block_writer.row_count() as i64);
        let uncompressed = self.block_writer.finish();
        self.block_uncompressed_sizes
            .push(
                i64::try_from(uncompressed.len()).map_err(|e| Error::DataInvalid {
                    message: "Row block is too large".to_string(),
                    source: Some(Box::new(e)),
                })?,
            );

        let compressed = zstd::bulk::compress(&uncompressed, self.zstd_level).map_err(|e| {
            Error::DataInvalid {
                message: format!("Failed to compress row block: {e}"),
                source: Some(Box::new(e)),
            }
        })?;
        self.block_compressed_sizes
            .push(
                i64::try_from(compressed.len()).map_err(|e| Error::DataInvalid {
                    message: "Compressed row block is too large".to_string(),
                    source: Some(Box::new(e)),
                })?,
            );
        self.bytes_written += compressed.len() as u64;
        self.writer.write(Bytes::from(compressed)).await?;
        self.block_writer.reset();
        Ok(())
    }
}

#[async_trait]
impl FormatFileWriter for RowFormatWriter {
    async fn write(&mut self, batch: &RecordBatch) -> crate::Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        validate_arrow_schema_for_row(&batch.schema(), &self.row_type)?;
        if batch.num_columns() != self.row_type.len() {
            return Err(Error::DataInvalid {
                message: format!(
                    ".row writer expected {} columns, got {}",
                    self.row_type.len(),
                    batch.num_columns()
                ),
                source: None,
            });
        }

        for row_idx in 0..batch.num_rows() {
            self.block_writer
                .write_row(batch, row_idx, &self.row_type)?;
            self.total_row_count += 1;
            if self.block_writer.estimated_size() >= self.block_size_threshold {
                self.flush_block().await?;
            }
        }
        Ok(())
    }

    fn num_bytes(&self) -> usize {
        self.bytes_written as usize + self.block_writer.estimated_size()
    }

    fn in_progress_size(&self) -> usize {
        self.block_writer.estimated_size()
    }

    async fn flush(&mut self) -> crate::Result<()> {
        self.flush_block().await
    }

    async fn close(mut self: Box<Self>) -> crate::Result<u64> {
        self.flush_block().await?;

        let index_offset = self.bytes_written;
        let block_count = self.block_compressed_sizes.len();
        let index_bytes = RowBlockIndex::new(
            self.block_compressed_sizes,
            self.block_uncompressed_sizes,
            self.block_row_starts,
        )?
        .to_bytes()?;
        let index_length = index_bytes.len();
        self.writer.write(Bytes::from(index_bytes)).await?;
        self.bytes_written += index_length as u64;

        let footer = RowFileFooter {
            total_row_count: self.total_row_count,
            block_count: i32::try_from(block_count).map_err(|e| Error::DataInvalid {
                message: "Too many row blocks".to_string(),
                source: Some(Box::new(e)),
            })?,
            index_offset: i64::try_from(index_offset).map_err(|e| Error::DataInvalid {
                message: "Row index offset exceeds i64".to_string(),
                source: Some(Box::new(e)),
            })?,
            index_length: i32::try_from(index_length).map_err(|e| Error::DataInvalid {
                message: "Row index is too large".to_string(),
                source: Some(Box::new(e)),
            })?,
        };
        let footer_bytes = footer.to_bytes();
        self.writer.write(Bytes::from(footer_bytes)).await?;
        self.bytes_written += FOOTER_SIZE;
        self.writer.close().await?;
        Ok(self.bytes_written)
    }
}

pub(super) fn row_type_from_arrow_schema(schema: &SchemaRef) -> crate::Result<Vec<DataField>> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            let data_type = arrow_to_paimon_type(field.data_type(), field.is_nullable())?;
            Ok(DataField::new(idx as i32, field.name().clone(), data_type))
        })
        .collect()
}

#[async_trait]
impl FormatFileReader for RowFormatReader {
    async fn read_batch_stream(
        &self,
        reader: Box<dyn FileRead>,
        file_size: u64,
        read_fields: &[DataField],
        _predicates: Option<&FilePredicates>,
        batch_size: Option<usize>,
        row_selection: Option<Vec<RowRange>>,
    ) -> crate::Result<ArrowRecordBatchStream> {
        validate_supported_types(read_fields)?;
        if file_size < FOOTER_SIZE {
            return Err(Error::DataInvalid {
                message: format!(".row file is too small: {file_size} bytes"),
                source: None,
            });
        }

        let footer_start = file_size - FOOTER_SIZE;
        let footer_bytes = reader.read(footer_start..file_size).await?;
        let footer = RowFileFooter::from_bytes(footer_bytes.as_ref())?;
        if footer.index_offset < 0 || footer.index_length < 0 {
            return Err(Error::DataInvalid {
                message: format!(
                    "Invalid .row footer index offset/length: {}/{}",
                    footer.index_offset, footer.index_length
                ),
                source: None,
            });
        }
        let index_start = footer.index_offset as u64;
        let index_end = index_start
            .checked_add(footer.index_length as u64)
            .ok_or_else(|| Error::DataInvalid {
                message: ".row index range overflows u64".to_string(),
                source: None,
            })?;
        if index_end > footer_start {
            return Err(Error::DataInvalid {
                message: format!(
                    ".row index range [{index_start}, {index_end}) exceeds footer start {footer_start}"
                ),
                source: None,
            });
        }
        let index_bytes = reader.read(index_start..index_end).await?;
        let index = RowBlockIndex::from_bytes(index_bytes.as_ref())?;
        let footer_block_count =
            usize::try_from(footer.block_count).map_err(|e| Error::DataInvalid {
                message: format!("Invalid .row footer block count {}", footer.block_count),
                source: Some(Box::new(e)),
            })?;
        if index.block_count() != footer_block_count {
            return Err(Error::DataInvalid {
                message: format!(
                    ".row footer block count {} does not match index block count {}",
                    footer.block_count,
                    index.block_count()
                ),
                source: None,
            });
        }
        let total_rows =
            usize::try_from(footer.total_row_count).map_err(|e| Error::DataInvalid {
                message: format!("Invalid .row total row count {}", footer.total_row_count),
                source: Some(Box::new(e)),
            })?;
        index.validate_for_file(total_rows, index_start)?;
        validate_row_selection(total_rows, row_selection.as_deref())?;

        let schema = build_target_arrow_schema(read_fields)?;
        let row_type = read_fields.to_vec();
        let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_SIZE);

        let blocks_to_read = blocks_to_read(&index, total_rows, row_selection.as_deref());
        Ok(try_stream! {
            let mut blocks = futures::stream::iter(blocks_to_read.into_iter().map(|block_idx| {
                read_row_block(reader.as_ref(), &index, block_idx)
            }))
            .buffered(ROW_BLOCK_READ_CONCURRENCY);

            while let Some(block) = blocks.next().await {
                let RowBlockPayload { block_idx, data } = block?;
                let block_start = index.block_row_start(block_idx);
                let block_end = if block_idx + 1 < index.block_count() {
                    index.block_row_start(block_idx + 1)
                } else {
                    total_rows
                };
                let selected = selected_local_indices(block_start, block_end, row_selection.as_deref());
                if selected.is_empty() {
                    continue;
                }

                for chunk in selected.chunks(batch_size) {
                    let batch = decode_row_block(&data, &row_type, schema.clone(), chunk)?;
                    yield batch;
                }
            }
        }
        .boxed())
    }
}

struct RowBlockPayload {
    block_idx: usize,
    data: Vec<u8>,
}

async fn read_row_block(
    reader: &dyn FileRead,
    index: &RowBlockIndex,
    block_idx: usize,
) -> crate::Result<RowBlockPayload> {
    let offset = index.block_offset(block_idx);
    let compressed_size = index.block_compressed_size(block_idx);
    let uncompressed_size = index.block_uncompressed_size(block_idx)?;
    let offset = u64::try_from(offset).map_err(|e| Error::DataInvalid {
        message: format!(".row block {block_idx} offset {offset} cannot fit u64"),
        source: Some(Box::new(e)),
    })?;
    let compressed_size = u64::try_from(compressed_size).map_err(|e| Error::DataInvalid {
        message: format!(".row block {block_idx} compressed size {compressed_size} cannot fit u64"),
        source: Some(Box::new(e)),
    })?;
    let end = offset
        .checked_add(compressed_size)
        .ok_or_else(|| Error::DataInvalid {
            message: format!(".row block {block_idx} byte range overflows u64"),
            source: None,
        })?;
    let compressed = reader.read(offset..end).await?;
    let data = zstd::bulk::decompress(compressed.as_ref(), uncompressed_size).map_err(|e| {
        Error::DataInvalid {
            message: format!("Failed to decompress .row block {block_idx}: {e}"),
            source: Some(Box::new(e)),
        }
    })?;
    Ok(RowBlockPayload { block_idx, data })
}

fn validate_arrow_schema_for_row(schema: &SchemaRef, fields: &[DataField]) -> crate::Result<()> {
    if schema.fields().len() != fields.len() {
        return Err(Error::DataInvalid {
            message: format!(
                ".row writer expected {} schema fields, got {}",
                fields.len(),
                schema.fields().len()
            ),
            source: None,
        });
    }

    for (arrow_field, field) in schema.fields().iter().zip(fields) {
        validate_arrow_type_for_row_field(
            field.name(),
            arrow_field.data_type(),
            field.data_type(),
        )?;
    }
    Ok(())
}

fn validate_arrow_type_for_row_field(
    field_name: &str,
    arrow_type: &ArrowDataType,
    paimon_type: &DataType,
) -> crate::Result<()> {
    let matches = match (arrow_type, paimon_type) {
        (ArrowDataType::Boolean, DataType::Boolean(_))
        | (ArrowDataType::Int8, DataType::TinyInt(_))
        | (ArrowDataType::Int16, DataType::SmallInt(_))
        | (ArrowDataType::Int32, DataType::Int(_))
        | (ArrowDataType::Int64, DataType::BigInt(_))
        | (ArrowDataType::Float32, DataType::Float(_))
        | (ArrowDataType::Float64, DataType::Double(_))
        | (ArrowDataType::Utf8, DataType::Char(_) | DataType::VarChar(_))
        | (
            ArrowDataType::Binary,
            DataType::Binary(_) | DataType::VarBinary(_) | DataType::Blob(_),
        )
        | (ArrowDataType::Date32, DataType::Date(_))
        | (ArrowDataType::Time32(TimeUnit::Millisecond), DataType::Time(_)) => true,
        (ArrowDataType::Timestamp(unit, tz), DataType::Timestamp(t)) => {
            *unit == timestamp_time_unit_for_precision(t.precision()) && tz.is_none()
        }
        (ArrowDataType::Timestamp(unit, tz), DataType::LocalZonedTimestamp(t)) => {
            *unit == timestamp_time_unit_for_precision(t.precision()) && tz.is_some()
        }
        (ArrowDataType::Decimal128(precision, scale), DataType::Decimal(d)) => {
            u32::from(*precision) == d.precision() && u32::try_from(*scale).ok() == Some(d.scale())
        }
        (ArrowDataType::List(child), DataType::Array(a)) => {
            validate_arrow_type_for_row_field(field_name, child.data_type(), a.element_type())?;
            true
        }
        (ArrowDataType::Map(entries, _), DataType::Map(m)) => {
            validate_arrow_map_entries(
                field_name,
                entries.data_type(),
                m.key_type(),
                m.value_type(),
            )?;
            true
        }
        (ArrowDataType::Map(entries, _), DataType::Multiset(m)) => {
            validate_arrow_map_entries(
                field_name,
                entries.data_type(),
                m.element_type(),
                &DataType::Int(IntType::new()),
            )?;
            true
        }
        (ArrowDataType::Struct(arrow_fields), DataType::Row(r)) => {
            if arrow_fields.len() != r.fields().len() {
                return Err(Error::Unsupported {
                    message: format!(
                        ".row writer field '{field_name}' expects {} struct fields, got {}",
                        r.fields().len(),
                        arrow_fields.len()
                    ),
                });
            }
            for (arrow_field, child_field) in arrow_fields.iter().zip(r.fields()) {
                validate_arrow_type_for_row_field(
                    child_field.name(),
                    arrow_field.data_type(),
                    child_field.data_type(),
                )?;
            }
            true
        }
        _ => false,
    };

    if matches {
        Ok(())
    } else {
        Err(Error::Unsupported {
            message: format!(
                ".row writer does not support Arrow type {arrow_type:?} for field '{field_name}' inferred as {paimon_type:?}"
            ),
        })
    }
}

fn validate_arrow_map_entries(
    field_name: &str,
    entries_type: &ArrowDataType,
    key_type: &DataType,
    value_type: &DataType,
) -> crate::Result<()> {
    let ArrowDataType::Struct(entries) = entries_type else {
        return Err(Error::Unsupported {
            message: format!(
                ".row writer map field '{field_name}' expects struct entries, got {entries_type:?}"
            ),
        });
    };
    if entries.len() != 2 {
        return Err(Error::Unsupported {
            message: format!(
                ".row writer map field '{field_name}' expects key/value entries, got {} fields",
                entries.len()
            ),
        });
    }
    validate_arrow_type_for_row_field(field_name, entries[0].data_type(), key_type)?;
    validate_arrow_type_for_row_field(field_name, entries[1].data_type(), value_type)
}

fn timestamp_time_unit_for_precision(precision: u32) -> TimeUnit {
    match precision {
        0..=3 => TimeUnit::Millisecond,
        4..=6 => TimeUnit::Microsecond,
        _ => TimeUnit::Nanosecond,
    }
}

fn validate_supported_types(fields: &[DataField]) -> crate::Result<()> {
    for field in fields {
        validate_supported_type(field.data_type())?;
    }
    Ok(())
}

fn validate_supported_type(data_type: &DataType) -> crate::Result<()> {
    match data_type {
        DataType::Boolean(_)
        | DataType::TinyInt(_)
        | DataType::SmallInt(_)
        | DataType::Int(_)
        | DataType::BigInt(_)
        | DataType::Float(_)
        | DataType::Double(_)
        | DataType::Char(_)
        | DataType::VarChar(_)
        | DataType::Binary(_)
        | DataType::VarBinary(_)
        | DataType::Blob(_)
        | DataType::Date(_)
        | DataType::Time(_)
        | DataType::Timestamp(_)
        | DataType::LocalZonedTimestamp(_)
        | DataType::Decimal(_) => Ok(()),
        DataType::Array(a) => validate_supported_type(a.element_type()),
        DataType::Map(m) => {
            validate_supported_type(m.key_type())?;
            validate_supported_type(m.value_type())
        }
        DataType::Multiset(m) => validate_supported_type(m.element_type()),
        DataType::Row(r) => {
            for child in r.fields() {
                validate_supported_type(child.data_type())?;
            }
            Ok(())
        }
    }
}

struct RowBlockWriter {
    data: Vec<u8>,
    offsets: Vec<i32>,
}

impl RowBlockWriter {
    fn new() -> Self {
        Self {
            data: Vec::with_capacity(DEFAULT_BLOCK_SIZE),
            offsets: Vec::new(),
        }
    }

    fn row_count(&self) -> usize {
        self.offsets.len()
    }

    fn estimated_size(&self) -> usize {
        self.data.len() + self.offsets.len() * 4 + 4
    }

    fn write_row(
        &mut self,
        batch: &RecordBatch,
        row_idx: usize,
        fields: &[DataField],
    ) -> crate::Result<()> {
        self.offsets.push(
            i32::try_from(self.data.len()).map_err(|e| Error::DataInvalid {
                message: "Row block offset exceeds i32".to_string(),
                source: Some(Box::new(e)),
            })?,
        );
        let header_size = fields.len().div_ceil(8);
        let header_start = self.data.len();
        self.data.resize(header_start + header_size, 0);

        for (col_idx, field) in fields.iter().enumerate() {
            let array = batch.column(col_idx);
            if array.is_null(row_idx) {
                self.data[header_start + col_idx / 8] |= 1 << (col_idx % 8);
            } else {
                write_field_value(&mut self.data, array, row_idx, field.data_type())?;
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> Vec<u8> {
        let mut out = std::mem::take(&mut self.data);
        for offset in &self.offsets {
            out.extend_from_slice(&offset.to_le_bytes());
        }
        out.extend_from_slice(&(self.offsets.len() as i32).to_le_bytes());
        self.offsets.clear();
        out
    }

    fn reset(&mut self) {
        self.data.clear();
        self.offsets.clear();
    }
}

fn write_field_value(
    out: &mut Vec<u8>,
    array: &ArrayRef,
    row_idx: usize,
    data_type: &DataType,
) -> crate::Result<()> {
    match data_type {
        DataType::Boolean(_) => write_bool(
            out,
            downcast::<BooleanArray>(array, data_type)?.value(row_idx),
        ),
        DataType::TinyInt(_) => {
            out.push(downcast::<Int8Array>(array, data_type)?.value(row_idx) as u8)
        }
        DataType::SmallInt(_) => out.extend_from_slice(
            &downcast::<Int16Array>(array, data_type)?
                .value(row_idx)
                .to_le_bytes(),
        ),
        DataType::Int(_) => out.extend_from_slice(
            &downcast::<Int32Array>(array, data_type)?
                .value(row_idx)
                .to_le_bytes(),
        ),
        DataType::BigInt(_) => out.extend_from_slice(
            &downcast::<Int64Array>(array, data_type)?
                .value(row_idx)
                .to_le_bytes(),
        ),
        DataType::Float(_) => out.extend_from_slice(
            &downcast::<Float32Array>(array, data_type)?
                .value(row_idx)
                .to_bits()
                .to_le_bytes(),
        ),
        DataType::Double(_) => out.extend_from_slice(
            &downcast::<Float64Array>(array, data_type)?
                .value(row_idx)
                .to_bits()
                .to_le_bytes(),
        ),
        DataType::Char(_) | DataType::VarChar(_) => write_bytes(
            out,
            downcast::<StringArray>(array, data_type)?
                .value(row_idx)
                .as_bytes(),
        ),
        DataType::Binary(_) | DataType::VarBinary(_) | DataType::Blob(_) => write_bytes(
            out,
            downcast::<BinaryArray>(array, data_type)?.value(row_idx),
        ),
        DataType::Date(_) => out.extend_from_slice(
            &downcast::<Date32Array>(array, data_type)?
                .value(row_idx)
                .to_le_bytes(),
        ),
        DataType::Time(_) => out.extend_from_slice(
            &downcast::<Time32MillisecondArray>(array, data_type)?
                .value(row_idx)
                .to_le_bytes(),
        ),
        DataType::Timestamp(t) => write_timestamp(out, array, row_idx, t.precision())?,
        DataType::LocalZonedTimestamp(t) => write_timestamp(out, array, row_idx, t.precision())?,
        DataType::Decimal(d) => {
            let value = downcast::<Decimal128Array>(array, data_type)?.value(row_idx);
            if d.precision() <= 18 {
                let compact = i64::try_from(value).map_err(|e| Error::DataInvalid {
                    message: format!("Decimal value {value} does not fit compact i64 encoding"),
                    source: Some(Box::new(e)),
                })?;
                out.extend_from_slice(&compact.to_le_bytes());
            } else {
                write_bytes(out, &i128_to_java_bigint_bytes(value));
            }
        }
        DataType::Array(a) => {
            let list = downcast::<ListArray>(array, data_type)?;
            let offsets = list.value_offsets();
            let start = offsets[row_idx] as usize;
            let end = offsets[row_idx + 1] as usize;
            write_array_slice(out, list.values(), start, end, a.element_type())?;
        }
        DataType::Map(m) => {
            let map = downcast::<MapArray>(array, data_type)?;
            write_map_value(out, map, row_idx, m.key_type(), m.value_type())?;
        }
        DataType::Multiset(m) => {
            let map = downcast::<MapArray>(array, data_type)?;
            let count_type = DataType::Int(IntType::new());
            write_map_value(out, map, row_idx, m.element_type(), &count_type)?;
        }
        DataType::Row(r) => {
            let row = downcast::<StructArray>(array, data_type)?;
            write_struct_row(out, row, row_idx, r.fields())?;
        }
    }
    Ok(())
}

fn write_array_slice(
    out: &mut Vec<u8>,
    values: &ArrayRef,
    start: usize,
    end: usize,
    element_type: &DataType,
) -> crate::Result<()> {
    if end < start {
        return Err(Error::DataInvalid {
            message: format!(".row array has invalid offsets [{start}, {end})"),
            source: None,
        });
    }
    let len = end - start;
    write_var_u32_checked(out, len)?;
    let null_bitmap_bytes = len.div_ceil(8);
    let null_start = out.len();
    out.resize(null_start + null_bitmap_bytes, 0);
    for local_idx in 0..len {
        let value_idx = start + local_idx;
        if values.is_null(value_idx) {
            out[null_start + local_idx / 8] |= 1 << (local_idx % 8);
        } else {
            write_field_value(out, values, value_idx, element_type)?;
        }
    }
    Ok(())
}

fn write_map_value(
    out: &mut Vec<u8>,
    map: &MapArray,
    row_idx: usize,
    key_type: &DataType,
    value_type: &DataType,
) -> crate::Result<()> {
    let offsets = map.value_offsets();
    let start = offsets[row_idx] as usize;
    let end = offsets[row_idx + 1] as usize;
    write_array_slice(out, map.keys(), start, end, key_type)?;
    write_array_slice(out, map.values(), start, end, value_type)
}

fn write_struct_row(
    out: &mut Vec<u8>,
    row: &StructArray,
    row_idx: usize,
    fields: &[DataField],
) -> crate::Result<()> {
    if row.num_columns() != fields.len() {
        return Err(Error::DataInvalid {
            message: format!(
                ".row struct expected {} columns, got {}",
                fields.len(),
                row.num_columns()
            ),
            source: None,
        });
    }
    let header_size = fields.len().div_ceil(8);
    let header_start = out.len();
    out.resize(header_start + header_size, 0);
    for (field_idx, field) in fields.iter().enumerate() {
        let array = row.column(field_idx);
        if array.is_null(row_idx) {
            out[header_start + field_idx / 8] |= 1 << (field_idx % 8);
        } else {
            write_field_value(out, array, row_idx, field.data_type())?;
        }
    }
    Ok(())
}

fn downcast<'a, T: Array + 'static>(
    array: &'a ArrayRef,
    expected: &DataType,
) -> crate::Result<&'a T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| Error::DataInvalid {
            message: format!(
                ".row array type mismatch for {:?}: got {:?}",
                expected,
                array.data_type()
            ),
            source: None,
        })
}

fn write_bool(out: &mut Vec<u8>, value: bool) {
    out.push(if value { 1 } else { 0 });
}

fn write_bytes(out: &mut Vec<u8>, value: &[u8]) {
    write_var_u32(out, value.len() as u32);
    out.extend_from_slice(value);
}

fn write_var_u32_checked(out: &mut Vec<u8>, value: usize) -> crate::Result<()> {
    let value = u32::try_from(value).map_err(|e| Error::DataInvalid {
        message: ".row variable-length value exceeds u32".to_string(),
        source: Some(Box::new(e)),
    })?;
    write_var_u32(out, value);
    Ok(())
}

fn write_var_u32(out: &mut Vec<u8>, mut value: u32) {
    while (value & !0x7f) != 0 {
        out.push(((value & 0x7f) as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn write_timestamp(
    out: &mut Vec<u8>,
    array: &ArrayRef,
    row_idx: usize,
    precision: u32,
) -> crate::Result<()> {
    let (millis, nanos_of_milli) = match array.data_type() {
        ArrowDataType::Timestamp(TimeUnit::Millisecond, _) => (
            downcast::<TimestampMillisecondArray>(array, &DataType::Timestamp(Default::default()))?
                .value(row_idx),
            0,
        ),
        ArrowDataType::Timestamp(TimeUnit::Microsecond, _) => {
            let micros = downcast::<TimestampMicrosecondArray>(
                array,
                &DataType::Timestamp(Default::default()),
            )?
            .value(row_idx);
            (
                micros.div_euclid(1_000),
                (micros.rem_euclid(1_000) * 1_000) as i32,
            )
        }
        ArrowDataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let nanos = downcast::<TimestampNanosecondArray>(
                array,
                &DataType::Timestamp(Default::default()),
            )?
            .value(row_idx);
            (
                nanos.div_euclid(1_000_000),
                nanos.rem_euclid(1_000_000) as i32,
            )
        }
        other => {
            return Err(Error::DataInvalid {
                message: format!(".row timestamp expects Arrow Timestamp, got {other:?}"),
                source: None,
            })
        }
    };

    out.extend_from_slice(&millis.to_le_bytes());
    if precision > 3 {
        write_var_u32(out, nanos_of_milli as u32);
    }
    Ok(())
}

fn decode_row_block(
    data: &[u8],
    fields: &[DataField],
    schema: SchemaRef,
    positions: &[usize],
) -> crate::Result<RecordBatch> {
    if fields.is_empty() {
        return RecordBatch::try_new_with_options(
            schema,
            Vec::new(),
            &RecordBatchOptions::new().with_row_count(Some(positions.len())),
        )
        .map_err(|e| Error::UnexpectedError {
            message: format!("Failed to build empty .row RecordBatch: {e}"),
            source: Some(Box::new(e)),
        });
    }

    let row_count = read_block_row_count(data)?;
    let offset_array_start =
        data.len()
            .checked_sub(4 + row_count * 4)
            .ok_or_else(|| Error::DataInvalid {
                message: ".row block is too small for row offsets".to_string(),
                source: None,
            })?;
    let mut builders = fields
        .iter()
        .map(|f| ColumnBuilder::new(f.data_type(), positions.len()))
        .collect::<crate::Result<Vec<_>>>()?;
    let header_size = fields.len().div_ceil(8);

    for &local_idx in positions {
        if local_idx >= row_count {
            return Err(Error::DataInvalid {
                message: format!(
                    ".row selected local index {local_idx} exceeds block row count {row_count}"
                ),
                source: None,
            });
        }
        let offset = read_i32_le(data, offset_array_start + local_idx * 4)? as usize;
        let header_end = offset + header_size;
        if header_end > offset_array_start {
            return Err(Error::DataInvalid {
                message: format!(
                    ".row row header range [{offset}, {header_end}) exceeds data section end {offset_array_start}"
                ),
                source: None,
            });
        }
        let mut input = BlockInput {
            data,
            position: header_end,
            data_end: offset_array_start,
        };
        for (field_idx, field) in fields.iter().enumerate() {
            if (data[offset + field_idx / 8] & (1 << (field_idx % 8))) != 0 {
                builders[field_idx].append_null();
            } else {
                builders[field_idx].read_append(&mut input, field.data_type())?;
            }
        }
    }

    let columns = builders
        .into_iter()
        .map(ColumnBuilder::finish)
        .collect::<crate::Result<Vec<_>>>()?;
    RecordBatch::try_new(schema, columns).map_err(|e| Error::UnexpectedError {
        message: format!("Failed to build .row RecordBatch: {e}"),
        source: Some(Box::new(e)),
    })
}

fn read_block_row_count(data: &[u8]) -> crate::Result<usize> {
    if data.len() < 4 {
        return Err(Error::DataInvalid {
            message: ".row block is too small for row count".to_string(),
            source: None,
        });
    }
    let raw = read_i32_le(data, data.len() - 4)?;
    if raw < 0 {
        return Err(Error::DataInvalid {
            message: format!(".row block has negative row count {raw}"),
            source: None,
        });
    }
    let row_count = raw as usize;
    if data.len() < 4 + row_count * 4 {
        return Err(Error::DataInvalid {
            message: format!(
                ".row block row count {row_count} exceeds block length {}",
                data.len()
            ),
            source: None,
        });
    }
    Ok(row_count)
}

struct BlockInput<'a> {
    data: &'a [u8],
    position: usize,
    data_end: usize,
}

impl BlockInput<'_> {
    fn read_exact<const N: usize>(&mut self) -> crate::Result<[u8; N]> {
        let end = self.position + N;
        if end > self.data_end {
            return Err(Error::DataInvalid {
                message: format!(
                    ".row read {N} bytes at offset {} exceeds data section end {}",
                    self.position, self.data_end
                ),
                source: None,
            });
        }
        let bytes = self.data[self.position..end].try_into().unwrap();
        self.position = end;
        Ok(bytes)
    }

    fn read_byte(&mut self) -> crate::Result<u8> {
        if self.position >= self.data_end {
            return Err(Error::DataInvalid {
                message: format!(
                    ".row read byte at offset {} exceeds data section end {}",
                    self.position, self.data_end
                ),
                source: None,
            });
        }
        let value = self.data[self.position];
        self.position += 1;
        Ok(value)
    }

    fn read_bool(&mut self) -> crate::Result<bool> {
        self.read_byte().map(|b| b != 0)
    }

    fn read_i16(&mut self) -> crate::Result<i16> {
        self.read_exact::<2>().map(i16::from_le_bytes)
    }

    fn read_i32(&mut self) -> crate::Result<i32> {
        self.read_exact::<4>().map(i32::from_le_bytes)
    }

    fn read_i64(&mut self) -> crate::Result<i64> {
        self.read_exact::<8>().map(i64::from_le_bytes)
    }

    fn read_f32(&mut self) -> crate::Result<f32> {
        self.read_i32().map(|v| f32::from_bits(v as u32))
    }

    fn read_f64(&mut self) -> crate::Result<f64> {
        self.read_i64().map(|v| f64::from_bits(v as u64))
    }

    fn read_var_u32(&mut self) -> crate::Result<u32> {
        let mut result = 0u32;
        let mut shift = 0u32;
        loop {
            let b = self.read_byte()?;
            result |= u32::from(b & 0x7f) << shift;
            if (b & 0x80) == 0 {
                return Ok(result);
            }
            shift += 7;
            if shift >= 32 {
                return Err(Error::DataInvalid {
                    message: ".row varint overflow".to_string(),
                    source: None,
                });
            }
        }
    }

    fn read_bytes(&mut self) -> crate::Result<Vec<u8>> {
        let len = self.read_var_u32()? as usize;
        let end = self.position + len;
        if end > self.data_end {
            return Err(Error::DataInvalid {
                message: format!(
                    ".row byte field range [{}, {end}) exceeds data section end {}",
                    self.position, self.data_end
                ),
                source: None,
            });
        }
        let value = self.data[self.position..end].to_vec();
        self.position = end;
        Ok(value)
    }

    fn skip(&mut self, len: usize) -> crate::Result<usize> {
        let start = self.position;
        let end = self.position + len;
        if end > self.data_end {
            return Err(Error::DataInvalid {
                message: format!(
                    ".row skip range [{start}, {end}) exceeds data section end {}",
                    self.data_end
                ),
                source: None,
            });
        }
        self.position = end;
        Ok(start)
    }
}

enum ColumnBuilder {
    Bool(BooleanBuilder),
    I8(Int8Builder),
    I16(Int16Builder),
    I32(Int32Builder),
    I64(Int64Builder),
    F32(Float32Builder),
    F64(Float64Builder),
    String(StringBuilder),
    Binary(BinaryBuilder),
    Date(Date32Builder),
    Time(Time32MillisecondBuilder),
    TimestampMs(TimestampMillisecondBuilder),
    TimestampUs(TimestampMicrosecondBuilder),
    TimestampNs(TimestampNanosecondBuilder),
    Decimal(Decimal128Builder),
    Array {
        field: Arc<Field>,
        offsets: Vec<i32>,
        validities: Vec<bool>,
        values: Box<ColumnBuilder>,
    },
    Map {
        entries_field: Arc<Field>,
        offsets: Vec<i32>,
        validities: Vec<bool>,
        keys: Box<ColumnBuilder>,
        values: Box<ColumnBuilder>,
        sorted: bool,
    },
    Row {
        fields: Fields,
        columns: Vec<ColumnBuilder>,
        validities: Vec<bool>,
        len: usize,
    },
}

impl ColumnBuilder {
    fn new(data_type: &DataType, capacity: usize) -> crate::Result<Self> {
        Ok(match data_type {
            DataType::Boolean(_) => Self::Bool(BooleanBuilder::with_capacity(capacity)),
            DataType::TinyInt(_) => Self::I8(Int8Builder::with_capacity(capacity)),
            DataType::SmallInt(_) => Self::I16(Int16Builder::with_capacity(capacity)),
            DataType::Int(_) => Self::I32(Int32Builder::with_capacity(capacity)),
            DataType::BigInt(_) => Self::I64(Int64Builder::with_capacity(capacity)),
            DataType::Float(_) => Self::F32(Float32Builder::with_capacity(capacity)),
            DataType::Double(_) => Self::F64(Float64Builder::with_capacity(capacity)),
            DataType::Char(_) | DataType::VarChar(_) => Self::String(StringBuilder::new()),
            DataType::Binary(_) | DataType::VarBinary(_) | DataType::Blob(_) => {
                Self::Binary(BinaryBuilder::new())
            }
            DataType::Date(_) => Self::Date(Date32Builder::with_capacity(capacity)),
            DataType::Time(_) => Self::Time(Time32MillisecondBuilder::with_capacity(capacity)),
            DataType::Timestamp(t) => timestamp_builder(t.precision(), false, capacity),
            DataType::LocalZonedTimestamp(t) => timestamp_builder(t.precision(), true, capacity),
            DataType::Decimal(d) => Self::Decimal(
                Decimal128Builder::with_capacity(capacity)
                    .with_precision_and_scale(
                        u8::try_from(d.precision()).map_err(|e| Error::DataInvalid {
                            message: "Decimal precision exceeds Arrow range".to_string(),
                            source: Some(Box::new(e)),
                        })?,
                        i8::try_from(d.scale()).map_err(|e| Error::DataInvalid {
                            message: "Decimal scale exceeds Arrow range".to_string(),
                            source: Some(Box::new(e)),
                        })?,
                    )
                    .map_err(|e| Error::DataInvalid {
                        message: format!("Failed to create Decimal128Builder: {e}"),
                        source: Some(Box::new(e)),
                    })?,
            ),
            DataType::Array(a) => Self::Array {
                field: Arc::new(Field::new(
                    "element",
                    paimon_type_to_arrow(a.element_type())?,
                    a.element_type().is_nullable(),
                )),
                offsets: vec![0],
                validities: Vec::with_capacity(capacity),
                values: Box::new(ColumnBuilder::new(a.element_type(), capacity)?),
            },
            DataType::Map(m) => Self::Map {
                entries_field: map_entries_field(
                    m.key_type(),
                    m.value_type(),
                    false,
                    m.value_type().is_nullable(),
                )?,
                offsets: vec![0],
                validities: Vec::with_capacity(capacity),
                keys: Box::new(ColumnBuilder::new(m.key_type(), capacity)?),
                values: Box::new(ColumnBuilder::new(m.value_type(), capacity)?),
                sorted: false,
            },
            DataType::Multiset(m) => {
                let count_type = DataType::Int(IntType::new());
                Self::Map {
                    entries_field: map_entries_field(
                        m.element_type(),
                        &count_type,
                        m.element_type().is_nullable(),
                        false,
                    )?,
                    offsets: vec![0],
                    validities: Vec::with_capacity(capacity),
                    keys: Box::new(ColumnBuilder::new(m.element_type(), capacity)?),
                    values: Box::new(ColumnBuilder::new(&count_type, capacity)?),
                    sorted: false,
                }
            }
            DataType::Row(r) => Self::Row {
                fields: arrow_fields_for_row(r.fields())?,
                columns: r
                    .fields()
                    .iter()
                    .map(|f| ColumnBuilder::new(f.data_type(), capacity))
                    .collect::<crate::Result<Vec<_>>>()?,
                validities: Vec::with_capacity(capacity),
                len: 0,
            },
        })
    }

    fn append_null(&mut self) {
        match self {
            Self::Bool(b) => b.append_null(),
            Self::I8(b) => b.append_null(),
            Self::I16(b) => b.append_null(),
            Self::I32(b) => b.append_null(),
            Self::I64(b) => b.append_null(),
            Self::F32(b) => b.append_null(),
            Self::F64(b) => b.append_null(),
            Self::String(b) => b.append_null(),
            Self::Binary(b) => b.append_null(),
            Self::Date(b) => b.append_null(),
            Self::Time(b) => b.append_null(),
            Self::TimestampMs(b) => b.append_null(),
            Self::TimestampUs(b) => b.append_null(),
            Self::TimestampNs(b) => b.append_null(),
            Self::Decimal(b) => b.append_null(),
            Self::Array {
                offsets,
                validities,
                ..
            }
            | Self::Map {
                offsets,
                validities,
                ..
            } => {
                offsets.push(*offsets.last().unwrap());
                validities.push(false);
            }
            Self::Row {
                columns,
                validities,
                len,
                ..
            } => {
                *len += 1;
                validities.push(false);
                for column in columns {
                    column.append_null();
                }
            }
        }
    }

    fn read_append(
        &mut self,
        input: &mut BlockInput<'_>,
        data_type: &DataType,
    ) -> crate::Result<()> {
        match (self, data_type) {
            (Self::Bool(b), DataType::Boolean(_)) => b.append_value(input.read_bool()?),
            (Self::I8(b), DataType::TinyInt(_)) => b.append_value(input.read_byte()? as i8),
            (Self::I16(b), DataType::SmallInt(_)) => b.append_value(input.read_i16()?),
            (Self::I32(b), DataType::Int(_)) => b.append_value(input.read_i32()?),
            (Self::I64(b), DataType::BigInt(_)) => b.append_value(input.read_i64()?),
            (Self::F32(b), DataType::Float(_)) => b.append_value(input.read_f32()?),
            (Self::F64(b), DataType::Double(_)) => b.append_value(input.read_f64()?),
            (Self::String(b), DataType::Char(_) | DataType::VarChar(_)) => {
                let bytes = input.read_bytes()?;
                let value = String::from_utf8(bytes).map_err(|e| Error::DataInvalid {
                    message: format!(".row string field is not valid UTF-8: {e}"),
                    source: Some(Box::new(e)),
                })?;
                b.append_value(value);
            }
            (Self::Binary(b), DataType::Binary(_) | DataType::VarBinary(_) | DataType::Blob(_)) => {
                b.append_value(input.read_bytes()?);
            }
            (Self::Date(b), DataType::Date(_)) => b.append_value(input.read_i32()?),
            (Self::Time(b), DataType::Time(_)) => b.append_value(input.read_i32()?),
            (Self::TimestampMs(b), DataType::Timestamp(_) | DataType::LocalZonedTimestamp(_)) => {
                b.append_value(read_timestamp_value(input, TimeUnit::Millisecond)?);
            }
            (Self::TimestampUs(b), DataType::Timestamp(_) | DataType::LocalZonedTimestamp(_)) => {
                b.append_value(read_timestamp_value(input, TimeUnit::Microsecond)?);
            }
            (Self::TimestampNs(b), DataType::Timestamp(_) | DataType::LocalZonedTimestamp(_)) => {
                b.append_value(read_timestamp_value(input, TimeUnit::Nanosecond)?);
            }
            (Self::Decimal(b), DataType::Decimal(d)) => {
                let value = if d.precision() <= 18 {
                    i128::from(input.read_i64()?)
                } else {
                    java_bigint_bytes_to_i128(&input.read_bytes()?)?
                };
                b.append_value(value);
            }
            (
                Self::Array {
                    offsets,
                    validities,
                    values,
                    ..
                },
                DataType::Array(a),
            ) => {
                let size = read_elements_into(input, a.element_type(), values)?;
                push_nested_offset(offsets, size)?;
                validities.push(true);
            }
            (
                Self::Map {
                    offsets,
                    validities,
                    keys,
                    values,
                    ..
                },
                DataType::Map(m),
            ) => {
                let key_count = read_elements_into(input, m.key_type(), keys)?;
                let value_count = read_elements_into(input, m.value_type(), values)?;
                if key_count != value_count {
                    return Err(Error::DataInvalid {
                        message: format!(
                            ".row map key/value element counts differ: {key_count}/{value_count}"
                        ),
                        source: None,
                    });
                }
                push_nested_offset(offsets, key_count)?;
                validities.push(true);
            }
            (
                Self::Map {
                    offsets,
                    validities,
                    keys,
                    values,
                    ..
                },
                DataType::Multiset(m),
            ) => {
                let count_type = DataType::Int(IntType::new());
                let key_count = read_elements_into(input, m.element_type(), keys)?;
                let value_count = read_elements_into(input, &count_type, values)?;
                if key_count != value_count {
                    return Err(Error::DataInvalid {
                        message: format!(
                            ".row multiset key/count element counts differ: {key_count}/{value_count}"
                        ),
                        source: None,
                    });
                }
                push_nested_offset(offsets, key_count)?;
                validities.push(true);
            }
            (
                Self::Row {
                    columns,
                    validities,
                    len,
                    ..
                },
                DataType::Row(r),
            ) => {
                read_struct_into(input, r.fields(), columns)?;
                *len += 1;
                validities.push(true);
            }
            (_, other) => {
                return Err(Error::DataInvalid {
                    message: format!("Mismatched .row column builder for {other:?}"),
                    source: None,
                })
            }
        }
        Ok(())
    }

    fn finish(self) -> crate::Result<ArrayRef> {
        Ok(match self {
            Self::Bool(mut b) => Arc::new(b.finish()),
            Self::I8(mut b) => Arc::new(b.finish()),
            Self::I16(mut b) => Arc::new(b.finish()),
            Self::I32(mut b) => Arc::new(b.finish()),
            Self::I64(mut b) => Arc::new(b.finish()),
            Self::F32(mut b) => Arc::new(b.finish()),
            Self::F64(mut b) => Arc::new(b.finish()),
            Self::String(mut b) => Arc::new(b.finish()),
            Self::Binary(mut b) => Arc::new(b.finish()),
            Self::Date(mut b) => Arc::new(b.finish()),
            Self::Time(mut b) => Arc::new(b.finish()),
            Self::TimestampMs(mut b) => Arc::new(b.finish()),
            Self::TimestampUs(mut b) => Arc::new(b.finish()),
            Self::TimestampNs(mut b) => Arc::new(b.finish()),
            Self::Decimal(mut b) => Arc::new(b.finish()),
            Self::Array {
                field,
                offsets,
                validities,
                values,
            } => Arc::new(
                ListArray::try_new(
                    field,
                    OffsetBuffer::new(ScalarBuffer::from(offsets)),
                    values.finish()?,
                    Some(null_buffer(validities)),
                )
                .map_err(|e| Error::UnexpectedError {
                    message: format!("Failed to build .row ListArray: {e}"),
                    source: Some(Box::new(e)),
                })?,
            ),
            Self::Map {
                entries_field,
                offsets,
                validities,
                keys,
                values,
                sorted,
            } => {
                let entry_fields = match entries_field.data_type() {
                    ArrowDataType::Struct(fields) => fields.clone(),
                    other => {
                        return Err(Error::DataInvalid {
                            message: format!(
                                ".row map entries field must be struct, got {other:?}"
                            ),
                            source: None,
                        })
                    }
                };
                let entries = StructArray::try_new(
                    entry_fields,
                    vec![keys.finish()?, values.finish()?],
                    None,
                )
                .map_err(|e| Error::UnexpectedError {
                    message: format!("Failed to build .row Map entries: {e}"),
                    source: Some(Box::new(e)),
                })?;
                Arc::new(
                    MapArray::try_new(
                        entries_field,
                        OffsetBuffer::new(ScalarBuffer::from(offsets)),
                        entries,
                        Some(null_buffer(validities)),
                        sorted,
                    )
                    .map_err(|e| Error::UnexpectedError {
                        message: format!("Failed to build .row MapArray: {e}"),
                        source: Some(Box::new(e)),
                    })?,
                )
            }
            Self::Row {
                fields,
                columns,
                validities,
                len,
            } => {
                let nulls = Some(null_buffer(validities));
                if fields.is_empty() {
                    Arc::new(StructArray::new_empty_fields(len, nulls))
                } else {
                    Arc::new(
                        StructArray::try_new(
                            fields,
                            columns
                                .into_iter()
                                .map(ColumnBuilder::finish)
                                .collect::<crate::Result<Vec<_>>>()?,
                            nulls,
                        )
                        .map_err(|e| Error::UnexpectedError {
                            message: format!("Failed to build .row StructArray: {e}"),
                            source: Some(Box::new(e)),
                        })?,
                    )
                }
            }
        })
    }
}

fn null_buffer(validities: Vec<bool>) -> NullBuffer {
    NullBuffer::new(BooleanBuffer::from(validities))
}

fn push_nested_offset(offsets: &mut Vec<i32>, size: usize) -> crate::Result<()> {
    let current = *offsets.last().unwrap();
    let next = current
        .checked_add(i32::try_from(size).map_err(|e| Error::DataInvalid {
            message: ".row nested value has too many elements".to_string(),
            source: Some(Box::new(e)),
        })?)
        .ok_or_else(|| Error::DataInvalid {
            message: ".row nested offsets overflow i32".to_string(),
            source: None,
        })?;
    offsets.push(next);
    Ok(())
}

fn read_elements_into(
    input: &mut BlockInput<'_>,
    element_type: &DataType,
    builder: &mut ColumnBuilder,
) -> crate::Result<usize> {
    let size = input.read_var_u32()? as usize;
    let null_bitmap_bytes = size.div_ceil(8);
    let null_start = input.skip(null_bitmap_bytes)?;
    for idx in 0..size {
        if (input.data[null_start + idx / 8] & (1 << (idx % 8))) != 0 {
            builder.append_null();
        } else {
            builder.read_append(input, element_type)?;
        }
    }
    Ok(size)
}

fn read_struct_into(
    input: &mut BlockInput<'_>,
    fields: &[DataField],
    builders: &mut [ColumnBuilder],
) -> crate::Result<()> {
    if fields.len() != builders.len() {
        return Err(Error::DataInvalid {
            message: format!(
                ".row struct reader expected {} builders, got {}",
                fields.len(),
                builders.len()
            ),
            source: None,
        });
    }
    let header_size = fields.len().div_ceil(8);
    let header_start = input.skip(header_size)?;
    for (idx, field) in fields.iter().enumerate() {
        if (input.data[header_start + idx / 8] & (1 << (idx % 8))) != 0 {
            builders[idx].append_null();
        } else {
            builders[idx].read_append(input, field.data_type())?;
        }
    }
    Ok(())
}

fn arrow_fields_for_row(fields: &[DataField]) -> crate::Result<Fields> {
    let fields = fields
        .iter()
        .map(|field| {
            Ok(Arc::new(Field::new(
                field.name(),
                paimon_type_to_arrow(field.data_type())?,
                field.data_type().is_nullable(),
            )))
        })
        .collect::<crate::Result<Vec<_>>>()?;
    Ok(fields.into())
}

fn map_entries_field(
    key_type: &DataType,
    value_type: &DataType,
    key_nullable: bool,
    value_nullable: bool,
) -> crate::Result<Arc<Field>> {
    let fields: Fields = vec![
        Arc::new(Field::new(
            "key",
            paimon_type_to_arrow(key_type)?,
            key_nullable,
        )),
        Arc::new(Field::new(
            "value",
            paimon_type_to_arrow(value_type)?,
            value_nullable,
        )),
    ]
    .into();
    Ok(Arc::new(Field::new(
        "entries",
        ArrowDataType::Struct(fields),
        false,
    )))
}

fn timestamp_builder(precision: u32, timezone: bool, capacity: usize) -> ColumnBuilder {
    match precision {
        0..=3 => {
            let builder = TimestampMillisecondBuilder::with_capacity(capacity);
            if timezone {
                ColumnBuilder::TimestampMs(builder.with_timezone("UTC"))
            } else {
                ColumnBuilder::TimestampMs(builder)
            }
        }
        4..=6 => {
            let builder = TimestampMicrosecondBuilder::with_capacity(capacity);
            if timezone {
                ColumnBuilder::TimestampUs(builder.with_timezone("UTC"))
            } else {
                ColumnBuilder::TimestampUs(builder)
            }
        }
        _ => {
            let builder = TimestampNanosecondBuilder::with_capacity(capacity);
            if timezone {
                ColumnBuilder::TimestampNs(builder.with_timezone("UTC"))
            } else {
                ColumnBuilder::TimestampNs(builder)
            }
        }
    }
}

fn read_timestamp_value(input: &mut BlockInput<'_>, unit: TimeUnit) -> crate::Result<i64> {
    let millis = input.read_i64()?;
    Ok(match unit {
        TimeUnit::Millisecond => millis,
        TimeUnit::Microsecond => {
            let nanos = i64::from(input.read_var_u32()?);
            millis * 1_000 + nanos / 1_000
        }
        TimeUnit::Nanosecond => {
            let nanos = i64::from(input.read_var_u32()?);
            millis * 1_000_000 + nanos
        }
        TimeUnit::Second => millis / 1_000,
    })
}

#[derive(Debug, Clone, Copy)]
struct RowFileFooter {
    total_row_count: i64,
    block_count: i32,
    index_offset: i64,
    index_length: i32,
}

impl RowFileFooter {
    fn to_bytes(self) -> Vec<u8> {
        let mut buf = vec![0u8; FOOTER_SIZE as usize];
        buf[0..8].copy_from_slice(&self.total_row_count.to_le_bytes());
        buf[8..12].copy_from_slice(&self.block_count.to_le_bytes());
        buf[12..20].copy_from_slice(&self.index_offset.to_le_bytes());
        buf[20..24].copy_from_slice(&self.index_length.to_le_bytes());
        buf[24] = ROW_VERSION;
        buf[28..32].copy_from_slice(&ROW_MAGIC.to_le_bytes());
        buf
    }

    fn from_bytes(buf: &[u8]) -> crate::Result<Self> {
        if buf.len() != FOOTER_SIZE as usize {
            return Err(Error::DataInvalid {
                message: format!(".row footer must be {FOOTER_SIZE} bytes, got {}", buf.len()),
                source: None,
            });
        }
        let magic = read_i32_le(buf, 28)?;
        if magic != ROW_MAGIC {
            return Err(Error::DataInvalid {
                message: format!(
                    "Invalid .row magic: expected 0x{ROW_MAGIC:08X}, got 0x{magic:08X}"
                ),
                source: None,
            });
        }
        let version = buf[24];
        if version != ROW_VERSION {
            return Err(Error::DataInvalid {
                message: format!("Unsupported .row version: {version}"),
                source: None,
            });
        }
        Ok(Self {
            total_row_count: read_i64_le(buf, 0)?,
            block_count: read_i32_le(buf, 8)?,
            index_offset: read_i64_le(buf, 12)?,
            index_length: read_i32_le(buf, 20)?,
        })
    }
}

struct RowBlockIndex {
    block_offsets: Vec<i64>,
    block_compressed_sizes: Vec<i64>,
    block_uncompressed_sizes: Vec<i64>,
    block_row_starts: Vec<i64>,
}

impl RowBlockIndex {
    fn new(
        block_compressed_sizes: Vec<i64>,
        block_uncompressed_sizes: Vec<i64>,
        block_row_starts: Vec<i64>,
    ) -> crate::Result<Self> {
        validate_block_index_arrays(
            &block_compressed_sizes,
            &block_uncompressed_sizes,
            &block_row_starts,
        )?;
        let block_offsets = compute_offsets(&block_compressed_sizes)?;
        Ok(Self {
            block_offsets,
            block_compressed_sizes,
            block_uncompressed_sizes,
            block_row_starts,
        })
    }

    fn to_bytes(&self) -> crate::Result<Vec<u8>> {
        let mut out = Vec::new();
        write_index_array(&mut out, &self.block_compressed_sizes)?;
        write_index_array(&mut out, &self.block_uncompressed_sizes)?;
        write_index_array(&mut out, &self.block_row_starts)?;
        Ok(out)
    }

    fn from_bytes(bytes: &[u8]) -> crate::Result<Self> {
        let mut pos = 0usize;
        let block_compressed_sizes = read_index_array(bytes, &mut pos)?;
        let block_uncompressed_sizes = read_index_array(bytes, &mut pos)?;
        let block_row_starts = read_index_array(bytes, &mut pos)?;
        if pos != bytes.len() {
            return Err(Error::DataInvalid {
                message: format!(".row block index has {} trailing bytes", bytes.len() - pos),
                source: None,
            });
        }
        if block_compressed_sizes.len() != block_uncompressed_sizes.len()
            || block_compressed_sizes.len() != block_row_starts.len()
        {
            return Err(Error::DataInvalid {
                message: ".row block index arrays have different lengths".to_string(),
                source: None,
            });
        }
        Self::new(
            block_compressed_sizes,
            block_uncompressed_sizes,
            block_row_starts,
        )
    }

    fn block_count(&self) -> usize {
        self.block_compressed_sizes.len()
    }

    fn block_offset(&self, idx: usize) -> i64 {
        self.block_offsets[idx]
    }

    fn block_compressed_size(&self, idx: usize) -> i64 {
        self.block_compressed_sizes[idx]
    }

    fn block_uncompressed_size(&self, idx: usize) -> crate::Result<usize> {
        usize::try_from(self.block_uncompressed_sizes[idx]).map_err(|e| Error::DataInvalid {
            message: format!(
                ".row block {idx} uncompressed size {} cannot fit usize",
                self.block_uncompressed_sizes[idx]
            ),
            source: Some(Box::new(e)),
        })
    }

    fn block_row_start(&self, idx: usize) -> usize {
        self.block_row_starts[idx] as usize
    }

    fn validate_for_file(&self, total_rows: usize, index_start: u64) -> crate::Result<()> {
        if self.block_count() == 0 {
            if total_rows == 0 {
                return Ok(());
            }
            return Err(Error::DataInvalid {
                message: format!(".row index has no blocks for {total_rows} rows"),
                source: None,
            });
        }
        if self.block_row_starts[0] != 0 {
            return Err(Error::DataInvalid {
                message: format!(
                    ".row first block row start must be 0, got {}",
                    self.block_row_starts[0]
                ),
                source: None,
            });
        }
        for idx in 0..self.block_count() {
            let row_start =
                usize::try_from(self.block_row_starts[idx]).map_err(|e| Error::DataInvalid {
                    message: format!(
                        ".row block {idx} row start {} cannot fit usize",
                        self.block_row_starts[idx]
                    ),
                    source: Some(Box::new(e)),
                })?;
            if row_start >= total_rows {
                return Err(Error::DataInvalid {
                    message: format!(
                        ".row block {idx} row start {row_start} exceeds total rows {total_rows}"
                    ),
                    source: None,
                });
            }
            let offset =
                u64::try_from(self.block_offsets[idx]).map_err(|e| Error::DataInvalid {
                    message: format!(
                        ".row block {idx} offset {} cannot fit u64",
                        self.block_offsets[idx]
                    ),
                    source: Some(Box::new(e)),
                })?;
            let compressed_size = u64::try_from(self.block_compressed_sizes[idx]).map_err(|e| {
                Error::DataInvalid {
                    message: format!(
                        ".row block {idx} compressed size {} cannot fit u64",
                        self.block_compressed_sizes[idx]
                    ),
                    source: Some(Box::new(e)),
                }
            })?;
            let end = offset
                .checked_add(compressed_size)
                .ok_or_else(|| Error::DataInvalid {
                    message: format!(".row block {idx} byte range overflows u64"),
                    source: None,
                })?;
            if end > index_start {
                return Err(Error::DataInvalid {
                    message: format!(
                        ".row block {idx} byte range [{offset}, {end}) exceeds index start {index_start}"
                    ),
                    source: None,
                });
            }
        }
        Ok(())
    }
}

fn validate_block_index_arrays(
    compressed_sizes: &[i64],
    uncompressed_sizes: &[i64],
    row_starts: &[i64],
) -> crate::Result<()> {
    if compressed_sizes.len() != uncompressed_sizes.len()
        || compressed_sizes.len() != row_starts.len()
    {
        return Err(Error::DataInvalid {
            message: ".row block index arrays have different lengths".to_string(),
            source: None,
        });
    }
    let mut previous_row_start = None;
    for idx in 0..compressed_sizes.len() {
        if compressed_sizes[idx] < 0 {
            return Err(Error::DataInvalid {
                message: format!(
                    ".row block {idx} has negative compressed size {}",
                    compressed_sizes[idx]
                ),
                source: None,
            });
        }
        if uncompressed_sizes[idx] < 0 {
            return Err(Error::DataInvalid {
                message: format!(
                    ".row block {idx} has negative uncompressed size {}",
                    uncompressed_sizes[idx]
                ),
                source: None,
            });
        }
        let row_start = row_starts[idx];
        if row_start < 0 {
            return Err(Error::DataInvalid {
                message: format!(".row block {idx} has negative row start {row_start}"),
                source: None,
            });
        }
        if previous_row_start.is_some_and(|previous| row_start <= previous) {
            return Err(Error::DataInvalid {
                message: format!(
                    ".row block row starts must be strictly increasing, got {row_start} after {}",
                    previous_row_start.unwrap()
                ),
                source: None,
            });
        }
        previous_row_start = Some(row_start);
    }
    Ok(())
}

fn compute_offsets(sizes: &[i64]) -> crate::Result<Vec<i64>> {
    let mut offsets = Vec::with_capacity(sizes.len());
    let mut offset = 0i64;
    for size in sizes {
        offsets.push(offset);
        offset = offset
            .checked_add(*size)
            .ok_or_else(|| Error::DataInvalid {
                message: ".row block offsets overflow i64".to_string(),
                source: None,
            })?;
    }
    Ok(offsets)
}

fn write_index_array(out: &mut Vec<u8>, values: &[i64]) -> crate::Result<()> {
    let encoded = encode_delta_varints(values);
    let len = u32::try_from(encoded.len()).map_err(|e| Error::DataInvalid {
        message: ".row encoded index array is too large".to_string(),
        source: Some(Box::new(e)),
    })?;
    write_var_u32(out, len);
    out.extend_from_slice(&encoded);
    Ok(())
}

fn read_index_array(bytes: &[u8], pos: &mut usize) -> crate::Result<Vec<i64>> {
    let (len, consumed) = decode_var_u32(&bytes[*pos..])?;
    *pos += consumed;
    let len = len as usize;
    let end = *pos + len;
    if end > bytes.len() {
        return Err(Error::DataInvalid {
            message: format!(".row index array length {len} exceeds remaining bytes"),
            source: None,
        });
    }
    let values = decode_delta_varints(&bytes[*pos..end])?;
    *pos = end;
    Ok(values)
}

fn encode_delta_varints(values: &[i64]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut prev = 0i64;
    for (idx, value) in values.iter().copied().enumerate() {
        let delta = if idx == 0 { value } else { value - prev };
        prev = value;
        encode_zigzag_varint(delta, &mut out);
    }
    out
}

fn decode_delta_varints(bytes: &[u8]) -> crate::Result<Vec<i64>> {
    let mut values = Vec::new();
    let mut pos = 0usize;
    let mut prev = 0i64;
    while pos < bytes.len() {
        let (delta, consumed) = decode_zigzag_varint(&bytes[pos..])?;
        pos += consumed;
        let value = if values.is_empty() {
            delta
        } else {
            prev.checked_add(delta).ok_or_else(|| Error::DataInvalid {
                message: ".row delta-varint overflow".to_string(),
                source: None,
            })?
        };
        prev = value;
        values.push(value);
    }
    Ok(values)
}

fn encode_zigzag_varint(value: i64, out: &mut Vec<u8>) {
    let mut remaining = ((value << 1) ^ (value >> 63)) as u64;
    while (remaining & !0x7f) != 0 {
        out.push(((remaining & 0x7f) as u8) | 0x80);
        remaining >>= 7;
    }
    out.push(remaining as u8);
}

fn decode_zigzag_varint(bytes: &[u8]) -> crate::Result<(i64, usize)> {
    let mut value = 0u64;
    let mut shift = 0u32;
    for (idx, byte) in bytes.iter().copied().enumerate() {
        value |= u64::from(byte & 0x7f) << shift;
        if (byte & 0x80) == 0 {
            let decoded = ((value >> 1) as i64) ^ (-((value & 1) as i64));
            return Ok((decoded, idx + 1));
        }
        shift += 7;
        if shift > 63 {
            return Err(Error::DataInvalid {
                message: ".row zigzag varint overflow".to_string(),
                source: None,
            });
        }
    }
    Err(Error::DataInvalid {
        message: "Unexpected end of .row zigzag varint".to_string(),
        source: None,
    })
}

fn decode_var_u32(bytes: &[u8]) -> crate::Result<(u32, usize)> {
    let mut result = 0u32;
    let mut shift = 0u32;
    for (idx, byte) in bytes.iter().copied().enumerate() {
        result |= u32::from(byte & 0x7f) << shift;
        if (byte & 0x80) == 0 {
            return Ok((result, idx + 1));
        }
        shift += 7;
        if shift >= 32 {
            return Err(Error::DataInvalid {
                message: ".row varint overflow".to_string(),
                source: None,
            });
        }
    }
    Err(Error::DataInvalid {
        message: "Unexpected end of .row varint".to_string(),
        source: None,
    })
}

fn read_i32_le(buf: &[u8], offset: usize) -> crate::Result<i32> {
    let end = offset + 4;
    buf.get(offset..end)
        .and_then(|s| s.try_into().ok())
        .map(i32::from_le_bytes)
        .ok_or_else(|| Error::DataInvalid {
            message: format!(
                ".row read i32 at {offset} exceeds buffer length {}",
                buf.len()
            ),
            source: None,
        })
}

fn read_i64_le(buf: &[u8], offset: usize) -> crate::Result<i64> {
    let end = offset + 8;
    buf.get(offset..end)
        .and_then(|s| s.try_into().ok())
        .map(i64::from_le_bytes)
        .ok_or_else(|| Error::DataInvalid {
            message: format!(
                ".row read i64 at {offset} exceeds buffer length {}",
                buf.len()
            ),
            source: None,
        })
}

fn validate_row_selection(total_rows: usize, selection: Option<&[RowRange]>) -> crate::Result<()> {
    let Some(selection) = selection else {
        return Ok(());
    };
    for range in selection {
        if range.from() < 0 {
            return Err(Error::DataInvalid {
                message: format!(
                    ".row selection must be non-negative, got [{}..={}]",
                    range.from(),
                    range.to()
                ),
                source: None,
            });
        }
        if total_rows != 0 && range.to() as usize >= total_rows {
            return Err(Error::DataInvalid {
                message: format!(
                    ".row selection [{}..={}] exceeds available rows {total_rows}",
                    range.from(),
                    range.to()
                ),
                source: None,
            });
        }
    }
    Ok(())
}

fn blocks_to_read(
    index: &RowBlockIndex,
    total_rows: usize,
    selection: Option<&[RowRange]>,
) -> Vec<usize> {
    let mut result = Vec::new();
    for block_idx in 0..index.block_count() {
        let start = index.block_row_start(block_idx);
        let end = if block_idx + 1 < index.block_count() {
            index.block_row_start(block_idx + 1)
        } else {
            total_rows
        };
        let intersects = selection.is_none_or(|ranges| {
            ranges
                .iter()
                .any(|r| (r.from() as usize) < end && (r.to() as usize) >= start)
        });
        if intersects {
            result.push(block_idx);
        }
    }
    result
}

fn selected_local_indices(
    block_start: usize,
    block_end: usize,
    selection: Option<&[RowRange]>,
) -> Vec<usize> {
    match selection {
        None => (0..block_end - block_start).collect(),
        Some(ranges) => {
            let mut result = Vec::new();
            for range in ranges {
                let start = (range.from() as usize).max(block_start);
                let end = ((range.to() as usize) + 1).min(block_end);
                if start < end {
                    result.extend((start - block_start)..(end - block_start));
                }
            }
            result
        }
    }
}

fn i128_to_java_bigint_bytes(value: i128) -> Vec<u8> {
    let bytes = value.to_be_bytes();
    let pad = if value < 0 { 0xff } else { 0x00 };
    let mut idx = 0;
    while idx + 1 < bytes.len() && bytes[idx] == pad && ((bytes[idx + 1] & 0x80) == (pad & 0x80)) {
        idx += 1;
    }
    bytes[idx..].to_vec()
}

fn java_bigint_bytes_to_i128(bytes: &[u8]) -> crate::Result<i128> {
    if bytes.is_empty() {
        return Ok(0);
    }
    if bytes.len() > 16 {
        let pad = if bytes[0] & 0x80 != 0 { 0xff } else { 0x00 };
        if bytes[..bytes.len() - 16].iter().any(|b| *b != pad) {
            return Err(Error::DataInvalid {
                message: "Decimal BigInteger bytes exceed i128 range".to_string(),
                source: None,
            });
        }
    }
    let mut out = if bytes[0] & 0x80 != 0 {
        [0xffu8; 16]
    } else {
        [0u8; 16]
    };
    let copy_len = bytes.len().min(16);
    out[16 - copy_len..].copy_from_slice(&bytes[bytes.len() - copy_len..]);
    Ok(i128::from_be_bytes(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::test_util::BytesFileRead;
    use crate::io::FileIOBuilder;
    use crate::spec::{
        ArrayType, BigIntType, BooleanType, DataType, DateType, DecimalType, DoubleType, FloatType,
        IntType, MapType, MultisetType, RowType, TimeType, TimestampType, VarBinaryType,
        VarCharType,
    };
    use futures::TryStreamExt;
    use std::ops::Range;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    fn expect_data_invalid<T>(result: crate::Result<T>) -> String {
        match result {
            Ok(_) => panic!("expected DataInvalid"),
            Err(Error::DataInvalid { message, .. }) => message,
            Err(err) => panic!("expected DataInvalid, got {err:?}"),
        }
    }

    fn encoded_index_arrays(
        compressed_sizes: &[i64],
        uncompressed_sizes: &[i64],
        row_starts: &[i64],
    ) -> Vec<u8> {
        let mut bytes = Vec::new();
        write_index_array(&mut bytes, compressed_sizes).unwrap();
        write_index_array(&mut bytes, uncompressed_sizes).unwrap();
        write_index_array(&mut bytes, row_starts).unwrap();
        bytes
    }

    #[test]
    fn row_block_index_rejects_invalid_values() {
        let message = expect_data_invalid(RowBlockIndex::from_bytes(&encoded_index_arrays(
            &[-1],
            &[1],
            &[0],
        )));
        assert!(message.contains("negative compressed size"));

        let message = expect_data_invalid(RowBlockIndex::from_bytes(&encoded_index_arrays(
            &[1, 1],
            &[1, 1],
            &[0, 0],
        )));
        assert!(message.contains("strictly increasing"));

        let message = expect_data_invalid(RowBlockIndex::new(
            vec![i64::MAX, 1],
            vec![1, 1],
            vec![0, 1],
        ));
        assert!(message.contains("offsets overflow"));
    }

    #[test]
    fn row_block_index_validates_file_bounds() {
        let index = RowBlockIndex::new(vec![10], vec![20], vec![0]).unwrap();
        let message = expect_data_invalid(index.validate_for_file(1, 5));
        assert!(message.contains("exceeds index start"));

        let index = RowBlockIndex::new(vec![1], vec![1], vec![2]).unwrap();
        let message = expect_data_invalid(index.validate_for_file(3, 10));
        assert!(message.contains("first block row start must be 0"));
    }

    #[tokio::test]
    async fn row_writer_reader_roundtrip_primitives_and_selection() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/row-roundtrip/data-1.row";
        let output = file_io.new_output(path).unwrap();
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "name".to_string(),
                DataType::VarChar(VarCharType::string_type()),
            ),
            DataField::new(2, "flag".to_string(), DataType::Boolean(BooleanType::new())),
            DataField::new(
                3,
                "amount".to_string(),
                DataType::Decimal(DecimalType::new(20, 2).unwrap()),
            ),
            DataField::new(
                4,
                "ts".to_string(),
                DataType::Timestamp(TimestampType::new(6).unwrap()),
            ),
            DataField::new(
                5,
                "payload".to_string(),
                DataType::VarBinary(
                    VarBinaryType::try_new(true, VarBinaryType::MAX_LENGTH).unwrap(),
                ),
            ),
        ];
        let schema = build_target_arrow_schema(&fields).unwrap();
        let mut writer = RowFormatWriter::new(&output, schema.clone(), fields.clone(), 1)
            .await
            .unwrap();
        let decimal = Decimal128Array::from(vec![Some(12345_i128), None, Some(-42_i128)])
            .with_precision_and_scale(20, 2)
            .unwrap();
        let ts = TimestampMicrosecondArray::from(vec![Some(1_234_567), Some(2_000_001), None]);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("ccc")])),
                Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
                Arc::new(decimal),
                Arc::new(ts),
                Arc::new(BinaryArray::from(vec![
                    Some(b"x".as_slice()),
                    None,
                    Some(b"zzz".as_slice()),
                ])),
            ],
        )
        .unwrap();
        writer.write(&batch).await.unwrap();
        Box::new(writer).close().await.unwrap();

        let input = file_io.new_input(path).unwrap();
        let bytes = input.read().await.unwrap();
        let batches = RowFormatReader
            .read_batch_stream(
                Box::new(BytesFileRead(bytes.clone())),
                bytes.len() as u64,
                &fields,
                None,
                Some(8),
                Some(vec![RowRange::new(1, 2)]),
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 2);
        assert_eq!(ids.value(1), 3);
        let names = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(names.is_null(0));
        assert_eq!(names.value(1), "ccc");
    }

    #[tokio::test]
    async fn row_reader_selection_across_blocks() {
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "payload".to_string(),
                DataType::VarChar(VarCharType::string_type()),
            ),
        ];
        let schema = build_target_arrow_schema(&fields).unwrap();
        let payload = "x".repeat(DEFAULT_BLOCK_SIZE + 1);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![
                    payload.as_str(),
                    payload.as_str(),
                    payload.as_str(),
                ])),
            ],
        )
        .unwrap();

        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/row-block-selection/data.row";
        let output = file_io.new_output(path).unwrap();
        let mut writer = RowFormatWriter::new(&output, schema, fields.clone(), 1)
            .await
            .unwrap();
        writer.write(&batch).await.unwrap();
        Box::new(writer).close().await.unwrap();

        let bytes = file_io.new_input(path).unwrap().read().await.unwrap();
        let batches = RowFormatReader
            .read_batch_stream(
                Box::new(BytesFileRead(bytes.clone())),
                bytes.len() as u64,
                &fields,
                None,
                Some(8),
                Some(vec![RowRange::new(1, 2)]),
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let ids = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![2, 3]);
    }

    #[tokio::test]
    async fn row_reader_prefetches_blocks_with_bounded_parallelism() {
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "payload".to_string(),
                DataType::VarChar(VarCharType::string_type()),
            ),
        ];
        let schema = build_target_arrow_schema(&fields).unwrap();
        let payload = "x".repeat(DEFAULT_BLOCK_SIZE + 1);
        let row_count = ROW_BLOCK_READ_CONCURRENCY + 4;
        let ids = (0..row_count as i32).collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids.clone())),
                Arc::new(StringArray::from(vec![payload.as_str(); row_count])),
            ],
        )
        .unwrap();

        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/row-block-prefetch/data.row";
        let output = file_io.new_output(path).unwrap();
        let mut writer = RowFormatWriter::new(&output, schema, fields.clone(), 1)
            .await
            .unwrap();
        writer.write(&batch).await.unwrap();
        Box::new(writer).close().await.unwrap();

        let bytes = file_io.new_input(path).unwrap().read().await.unwrap();
        let reader = TrackingFileRead::new(bytes.clone());
        let batches = RowFormatReader
            .read_batch_stream(
                Box::new(reader.clone()),
                bytes.len() as u64,
                &fields,
                None,
                Some(row_count),
                None,
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let actual = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, ids);
        assert!(reader.max_in_flight() > 1);
        assert!(reader.max_in_flight() <= ROW_BLOCK_READ_CONCURRENCY);
    }

    #[tokio::test]
    async fn row_writer_rejects_unsupported_arrow_physical_type() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![Field::new(
            "name",
            ArrowDataType::LargeUtf8,
            true,
        )]));
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let output = file_io
            .new_output("memory:/row-unsupported/data.row")
            .unwrap();

        let row_type = vec![DataField::new(
            0,
            "name".to_string(),
            DataType::VarChar(VarCharType::string_type()),
        )];
        let err = match RowFormatWriter::new(&output, schema, row_type, 1).await {
            Ok(_) => panic!("LargeUtf8 should be rejected at writer creation"),
            Err(err) => err,
        };
        assert!(matches!(err, Error::Unsupported { .. }));
    }

    #[tokio::test]
    async fn row_writer_rejects_arrow_schema_that_differs_from_table_schema() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![Field::new(
            "amount",
            ArrowDataType::Decimal128(18, 2),
            true,
        )]));
        let row_type = vec![DataField::new(
            0,
            "amount".to_string(),
            DataType::Decimal(DecimalType::new(20, 2).unwrap()),
        )];
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let output = file_io
            .new_output("memory:/row-schema-mismatch/data.row")
            .unwrap();

        let err = match RowFormatWriter::new(&output, schema, row_type, 1).await {
            Ok(_) => panic!("Mismatched decimal precision should be rejected"),
            Err(err) => err,
        };
        assert!(matches!(err, Error::Unsupported { .. }));
    }

    #[tokio::test]
    async fn row_writer_reader_roundtrip_nested_types() {
        let string_type = DataType::VarChar(VarCharType::string_type());
        let fields = vec![
            DataField::new(
                0,
                "nums".to_string(),
                DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
            ),
            DataField::new(
                1,
                "labels".to_string(),
                DataType::Map(MapType::new(
                    string_type.clone(),
                    DataType::Int(IntType::new()),
                )),
            ),
            DataField::new(
                2,
                "bag".to_string(),
                DataType::Multiset(MultisetType::new(string_type.clone())),
            ),
            DataField::new(
                3,
                "nested".to_string(),
                DataType::Row(RowType::new(vec![
                    DataField::new(10, "score".to_string(), DataType::Int(IntType::new())),
                    DataField::new(11, "name".to_string(), string_type),
                ])),
            ),
        ];
        let schema = build_target_arrow_schema(&fields).unwrap();

        let nums = ListArray::try_new(
            Arc::new(Field::new("element", ArrowDataType::Int32, true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 3, 3, 5])),
            Arc::new(Int32Array::from(vec![
                Some(1),
                None,
                Some(3),
                Some(4),
                Some(5),
            ])),
            Some(null_buffer(vec![true, false, true])),
        )
        .unwrap();

        let labels = test_map_array(
            vec![0, 2, 2, 3],
            vec![true, false, true],
            false,
            true,
            vec![Some("a"), Some("b"), Some("c")],
            vec![Some(10), None, Some(30)],
        );
        let bag = test_map_array(
            vec![0, 2, 2, 3],
            vec![true, false, true],
            true,
            false,
            vec![Some("x"), Some("y"), Some("z")],
            vec![Some(2), Some(1), Some(4)],
        );

        let nested = StructArray::try_new(
            vec![
                Arc::new(Field::new("score", ArrowDataType::Int32, true)),
                Arc::new(Field::new("name", ArrowDataType::Utf8, true)),
            ]
            .into(),
            vec![
                Arc::new(Int32Array::from(vec![Some(7), None, Some(9)])),
                Arc::new(StringArray::from(vec![Some("seven"), None, Some("nine")])),
            ],
            Some(null_buffer(vec![true, false, true])),
        )
        .unwrap();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(nums),
                Arc::new(labels),
                Arc::new(bag),
                Arc::new(nested),
            ],
        )
        .unwrap();

        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/row-nested/data.row";
        let output = file_io.new_output(path).unwrap();
        let mut writer = RowFormatWriter::new(&output, schema, fields.clone(), 1)
            .await
            .unwrap();
        writer.write(&batch).await.unwrap();
        Box::new(writer).close().await.unwrap();

        let bytes = file_io.new_input(path).unwrap().read().await.unwrap();
        let batches = RowFormatReader
            .read_batch_stream(
                Box::new(BytesFileRead(bytes.clone())),
                bytes.len() as u64,
                &fields,
                None,
                Some(8),
                None,
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);

        let nums = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(nums.value_offsets(), &[0, 3, 3, 5]);
        assert!(nums.is_null(1));
        let nums_values = nums.values().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(nums_values.value(0), 1);
        assert!(nums_values.is_null(1));
        assert_eq!(nums_values.value(4), 5);

        let labels = batch.column(1).as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(labels.value_offsets(), &[0, 2, 2, 3]);
        assert!(labels.is_null(1));
        let label_keys = labels
            .keys()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let label_values = labels
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(label_keys.value(0), "a");
        assert_eq!(label_keys.value(2), "c");
        assert!(label_values.is_null(1));
        assert_eq!(label_values.value(2), 30);

        let bag = batch.column(2).as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(bag.value_offsets(), &[0, 2, 2, 3]);
        assert!(bag.is_null(1));
        let bag_keys = bag.keys().as_any().downcast_ref::<StringArray>().unwrap();
        let bag_counts = bag.values().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(bag_keys.value(0), "x");
        assert_eq!(bag_keys.value(2), "z");
        assert_eq!(bag_counts.value(0), 2);
        assert_eq!(bag_counts.value(2), 4);

        let nested = batch
            .column(3)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(nested.is_null(1));
        let scores = nested
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = nested
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(scores.value(0), 7);
        assert!(scores.is_null(1));
        assert_eq!(scores.value(2), 9);
        assert_eq!(names.value(0), "seven");
        assert!(names.is_null(1));
        assert_eq!(names.value(2), "nine");
    }

    #[tokio::test]
    async fn row_reader_reads_java_generated_fixture() {
        let fields = java_fixture_fields();
        let bytes = java_fixture_bytes();
        let file_size = bytes.len() as u64;
        let batches = RowFormatReader
            .read_batch_stream(
                Box::new(BytesFileRead(bytes.clone())),
                file_size,
                &fields,
                None,
                Some(8),
                None,
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.values(), &[1, 2]);

        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "java");
        assert!(names.is_null(1));

        let amounts = batch
            .column(2)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(amounts.value(0), 12_345_678_901_234_567_890_i128);
        assert_eq!(amounts.value(1), -12_345_678_901_234_567_890_i128);

        let ts = batch
            .column(3)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), 1_700_000_000_123_456_i64);
        assert_eq!(ts.value(1), 0);

        let scores = batch
            .column(4)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(scores.value_offsets(), &[0, 3, 3]);
        let score_values = scores
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(score_values.value(0), 1);
        assert!(score_values.is_null(1));
        assert_eq!(score_values.value(2), 3);

        let attrs = batch.column(5).as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(attrs.value_offsets(), &[0, 2, 2]);
        assert!(attrs.is_null(1));
        let attr_keys = attrs.keys().as_any().downcast_ref::<StringArray>().unwrap();
        let attr_values = attrs
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(attr_keys.value(0), "a");
        assert_eq!(attr_keys.value(1), "b");
        assert_eq!(attr_values.value(0), 10);
        assert!(attr_values.is_null(1));

        let nested = batch
            .column(6)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let flags = nested
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let notes = nested
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(flags.value(0));
        assert!(!flags.value(1));
        assert_eq!(notes.value(0), "inner");
        assert!(notes.is_null(1));
    }

    #[tokio::test]
    async fn row_writer_matches_java_generated_fixture_payload() {
        let fields = java_fixture_fields();
        let schema = build_target_arrow_schema(&fields).unwrap();
        let batch = java_fixture_batch(schema.clone());
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let output = file_io
            .new_output("memory:/row-java-fixture-byte-match/data.row")
            .unwrap();
        let mut writer = RowFormatWriter::new(&output, schema, fields.clone(), 1)
            .await
            .unwrap();
        writer.write(&batch).await.unwrap();
        Box::new(writer).close().await.unwrap();

        let actual = file_io
            .new_input("memory:/row-java-fixture-byte-match/data.row")
            .unwrap()
            .read()
            .await
            .unwrap();
        assert_eq!(
            decompressed_blocks(actual.as_ref()),
            decompressed_blocks(java_fixture_bytes().as_ref())
        );
    }

    const JAVA_ROW_FIXTURE_HEX: &str =
        "28b52ffd0048ed020072c51423a0458d01833de0d809ef0901c6ad0f0d10d48ac5b2099bbd87\
         ea72333e5ef7cd6e2953fffecbbb6210181333e1c1fd9b488e9450a1ff6fb7d1a8efdf7edd\
         7610b2570b3078aac0430a1706e72e5231227e3d0200206053803302cc0102c8010100020000\
         0000000000010000006600000000000000080000000100000053574f52";

    fn java_fixture_bytes() -> Bytes {
        Bytes::from(decode_hex(JAVA_ROW_FIXTURE_HEX))
    }

    fn java_fixture_fields() -> Vec<DataField> {
        let string_type = DataType::VarChar(VarCharType::with_nullable(true, 20).unwrap());
        vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "name".to_string(), string_type.clone()),
            DataField::new(
                2,
                "amount".to_string(),
                DataType::Decimal(DecimalType::new(20, 2).unwrap()),
            ),
            DataField::new(
                3,
                "ts".to_string(),
                DataType::Timestamp(TimestampType::new(6).unwrap()),
            ),
            DataField::new(
                4,
                "scores".to_string(),
                DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
            ),
            DataField::new(
                5,
                "attrs".to_string(),
                DataType::Map(MapType::new(
                    DataType::VarChar(VarCharType::with_nullable(false, 20).unwrap()),
                    DataType::Int(IntType::new()),
                )),
            ),
            DataField::new(
                6,
                "nested".to_string(),
                DataType::Row(RowType::new(vec![
                    DataField::new(0, "flag".to_string(), DataType::Boolean(BooleanType::new())),
                    DataField::new(1, "note".to_string(), string_type),
                ])),
            ),
        ]
    }

    fn java_fixture_batch(schema: SchemaRef) -> RecordBatch {
        let amounts = Decimal128Array::from(vec![
            Some(12_345_678_901_234_567_890_i128),
            Some(-12_345_678_901_234_567_890_i128),
        ])
        .with_precision_and_scale(20, 2)
        .unwrap();
        let scores = ListArray::try_new(
            Arc::new(Field::new("element", ArrowDataType::Int32, true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 3, 3])),
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
            None,
        )
        .unwrap();
        let attrs = test_map_array(
            vec![0, 2, 2],
            vec![true, false],
            false,
            true,
            vec![Some("a"), Some("b")],
            vec![Some(10), None],
        );
        let nested = StructArray::try_new(
            vec![
                Arc::new(Field::new("flag", ArrowDataType::Boolean, true)),
                Arc::new(Field::new("note", ArrowDataType::Utf8, true)),
            ]
            .into(),
            vec![
                Arc::new(BooleanArray::from(vec![true, false])),
                Arc::new(StringArray::from(vec![Some("inner"), None])),
            ],
            None,
        )
        .unwrap();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("java"), None])),
                Arc::new(amounts),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(1_700_000_000_123_456_i64),
                    Some(0),
                ])),
                Arc::new(scores),
                Arc::new(attrs),
                Arc::new(nested),
            ],
        )
        .unwrap()
    }

    fn decompressed_blocks(file: &[u8]) -> Vec<Vec<u8>> {
        let footer_start = file.len() - FOOTER_SIZE as usize;
        let footer = RowFileFooter::from_bytes(&file[footer_start..]).unwrap();
        let index_start = footer.index_offset as usize;
        let index_end = index_start + footer.index_length as usize;
        let index = RowBlockIndex::from_bytes(&file[index_start..index_end]).unwrap();

        (0..index.block_count())
            .map(|idx| {
                let start = index.block_offsets[idx] as usize;
                let end = start + index.block_compressed_sizes[idx] as usize;
                zstd::bulk::decompress(
                    &file[start..end],
                    index.block_uncompressed_sizes[idx] as usize,
                )
                .unwrap()
            })
            .collect()
    }

    #[derive(Clone)]
    struct TrackingFileRead {
        bytes: Bytes,
        in_flight: Arc<AtomicUsize>,
        max_in_flight: Arc<AtomicUsize>,
    }

    impl TrackingFileRead {
        fn new(bytes: Bytes) -> Self {
            Self {
                bytes,
                in_flight: Arc::new(AtomicUsize::new(0)),
                max_in_flight: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn max_in_flight(&self) -> usize {
            self.max_in_flight.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl FileRead for TrackingFileRead {
        async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
            let in_flight = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(in_flight, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(10)).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            Ok(self.bytes.slice(range.start as usize..range.end as usize))
        }
    }

    fn test_map_array(
        offsets: Vec<i32>,
        validities: Vec<bool>,
        key_nullable: bool,
        value_nullable: bool,
        keys: Vec<Option<&str>>,
        values: Vec<Option<i32>>,
    ) -> MapArray {
        let entries = StructArray::try_new(
            vec![
                Arc::new(Field::new("key", ArrowDataType::Utf8, key_nullable)),
                Arc::new(Field::new("value", ArrowDataType::Int32, value_nullable)),
            ]
            .into(),
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(Int32Array::from(values)),
            ],
            None,
        )
        .unwrap();
        MapArray::try_new(
            Arc::new(Field::new(
                "entries",
                ArrowDataType::Struct(entries.fields().clone()),
                false,
            )),
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            entries,
            Some(null_buffer(validities)),
            false,
        )
        .unwrap()
    }

    fn decode_hex(s: &str) -> Vec<u8> {
        let compact = s.split_whitespace().collect::<String>();
        assert_eq!(compact.len() % 2, 0);
        (0..compact.len())
            .step_by(2)
            .map(|idx| u8::from_str_radix(&compact[idx..idx + 2], 16).unwrap())
            .collect()
    }

    #[tokio::test]
    async fn row_reader_supports_empty_projection() {
        let fields = vec![DataField::new(
            0,
            "id".to_string(),
            DataType::BigInt(BigIntType::new()),
        )];
        let schema = build_target_arrow_schema(&fields).unwrap();
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let output = file_io.new_output("memory:/row-empty/data.row").unwrap();
        let mut writer = RowFormatWriter::new(&output, schema.clone(), fields.clone(), 1)
            .await
            .unwrap();
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))])
                .unwrap();
        writer.write(&batch).await.unwrap();
        Box::new(writer).close().await.unwrap();
        let bytes = file_io
            .new_input("memory:/row-empty/data.row")
            .unwrap()
            .read()
            .await
            .unwrap();

        let batches = RowFormatReader
            .read_batch_stream(
                Box::new(BytesFileRead(bytes.clone())),
                bytes.len() as u64,
                &[],
                None,
                Some(2),
                None,
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches.len(), 2);
        assert!(batches[0].columns().is_empty());
        assert_eq!(batches[0].num_rows(), 2);
        assert!(batches[1].columns().is_empty());
        assert_eq!(batches[1].num_rows(), 1);
    }

    #[test]
    fn java_bigint_bytes_are_minimal_twos_complement() {
        assert_eq!(i128_to_java_bigint_bytes(0), vec![0]);
        assert_eq!(i128_to_java_bigint_bytes(127), vec![0x7f]);
        assert_eq!(i128_to_java_bigint_bytes(128), vec![0x00, 0x80]);
        assert_eq!(i128_to_java_bigint_bytes(-1), vec![0xff]);
        assert_eq!(i128_to_java_bigint_bytes(-129), vec![0xff, 0x7f]);
        for value in [0, 127, 128, -1, -129, i64::MAX as i128 + 1] {
            let bytes = i128_to_java_bigint_bytes(value);
            assert_eq!(java_bigint_bytes_to_i128(&bytes).unwrap(), value);
        }
    }

    #[test]
    fn validates_supported_types() {
        validate_supported_types(&[
            DataField::new(0, "d".to_string(), DataType::Date(DateType::new())),
            DataField::new(
                1,
                "t".to_string(),
                DataType::Time(TimeType::new(3).unwrap()),
            ),
            DataField::new(2, "f".to_string(), DataType::Float(FloatType::new())),
            DataField::new(3, "g".to_string(), DataType::Double(DoubleType::new())),
        ])
        .unwrap();
    }
}
