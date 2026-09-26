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

//! Line-oriented Format Table files. Java uses a positional CSV schema, JSON
//! objects keyed by field name, and exactly one string column for TEXT.

use super::{FilePredicates, FormatFileReader, FormatFileWriter, FormatWriteResult};
use crate::arrow::build_target_arrow_schema;
use crate::io::{FileRead, FileWrite, OutputFile};
use crate::spec::DataField;
use crate::table::{ArrowRecordBatchStream, RowRange};
use crate::Error;
use arrow_array::{
    Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, LargeBinaryArray, RecordBatch, StringArray,
};
use arrow_schema::{DataType, SchemaRef};
use async_trait::async_trait;
use base64::Engine;
use bytes::Bytes;
use futures::{stream, StreamExt};
use std::collections::HashMap;
use std::io::{BufReader, Cursor, Read, Write};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TextKind {
    Csv,
    Json,
    Text,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TextCompression {
    None,
    Gzip,
    Bzip2,
    Deflate,
    Snappy,
    Lz4,
    Zstd,
}

impl TextCompression {
    pub(crate) fn from_name(name: &str) -> crate::Result<Self> {
        match name.to_ascii_lowercase().as_str() {
            "" | "none" | "uncompressed" => Ok(Self::None),
            "gzip" => Ok(Self::Gzip),
            "bzip2" => Ok(Self::Bzip2),
            "deflate" => Ok(Self::Deflate),
            "snappy" => Ok(Self::Snappy),
            "lz4" => Ok(Self::Lz4),
            "zstd" => Ok(Self::Zstd),
            _ => Err(Error::Unsupported {
                message: format!("Unsupported text file compression '{name}'"),
            }),
        }
    }

    pub(crate) fn extension(self) -> Option<&'static str> {
        match self {
            Self::None => None,
            Self::Gzip => Some("gz"),
            Self::Bzip2 => Some("bz2"),
            Self::Deflate => Some("deflate"),
            Self::Snappy => Some("snappy"),
            Self::Lz4 => Some("lz4"),
            Self::Zstd => Some("zst"),
        }
    }

    fn from_extension(extension: &str) -> Option<Self> {
        match extension {
            "gz" => Some(Self::Gzip),
            "bz2" => Some(Self::Bzip2),
            "deflate" => Some(Self::Deflate),
            "snappy" => Some(Self::Snappy),
            "lz4" => Some(Self::Lz4),
            "zst" => Some(Self::Zstd),
            _ => None,
        }
    }

    fn decode(self, input: &[u8]) -> crate::Result<Vec<u8>> {
        let mut output = Vec::new();
        match self {
            Self::None => output.extend_from_slice(input),
            Self::Gzip => {
                flate2::read::MultiGzDecoder::new(input)
                    .read_to_end(&mut output)
                    .map_err(compression_error)?;
            }
            Self::Bzip2 => {
                bzip2::read::MultiBzDecoder::new(input)
                    .read_to_end(&mut output)
                    .map_err(compression_error)?;
            }
            Self::Deflate => {
                flate2::read::ZlibDecoder::new(input)
                    .read_to_end(&mut output)
                    .map_err(compression_error)?;
            }
            Self::Snappy | Self::Lz4 => return decode_hadoop_blocks(input, self),
            Self::Zstd => output = zstd::stream::decode_all(input).map_err(compression_error)?,
        };
        Ok(output)
    }
}

enum TextEncoder {
    None,
    Gzip(flate2::write::GzEncoder<Vec<u8>>),
    Bzip2(bzip2::write::BzEncoder<Vec<u8>>),
    Deflate(flate2::write::ZlibEncoder<Vec<u8>>),
    Snappy,
    Lz4,
    Zstd(zstd::stream::write::Encoder<'static, Vec<u8>>),
}

impl TextEncoder {
    fn new(compression: TextCompression) -> crate::Result<Self> {
        Ok(match compression {
            TextCompression::None => Self::None,
            TextCompression::Gzip => Self::Gzip(flate2::write::GzEncoder::new(
                Vec::new(),
                flate2::Compression::default(),
            )),
            TextCompression::Bzip2 => Self::Bzip2(bzip2::write::BzEncoder::new(
                Vec::new(),
                bzip2::Compression::default(),
            )),
            TextCompression::Deflate => Self::Deflate(flate2::write::ZlibEncoder::new(
                Vec::new(),
                flate2::Compression::default(),
            )),
            TextCompression::Snappy => Self::Snappy,
            TextCompression::Lz4 => Self::Lz4,
            TextCompression::Zstd => Self::Zstd(
                zstd::stream::write::Encoder::new(Vec::new(), 0).map_err(compression_error)?,
            ),
        })
    }

    fn write(&mut self, input: Vec<u8>) -> crate::Result<Vec<u8>> {
        match self {
            Self::None => Ok(input),
            Self::Gzip(encoder) => {
                encoder.write_all(&input).map_err(compression_error)?;
                Ok(std::mem::take(encoder.get_mut()))
            }
            Self::Bzip2(encoder) => {
                encoder.write_all(&input).map_err(compression_error)?;
                Ok(std::mem::take(encoder.get_mut()))
            }
            Self::Deflate(encoder) => {
                encoder.write_all(&input).map_err(compression_error)?;
                Ok(std::mem::take(encoder.get_mut()))
            }
            Self::Snappy => encode_hadoop_blocks(&input, TextCompression::Snappy),
            Self::Lz4 => encode_hadoop_blocks(&input, TextCompression::Lz4),
            Self::Zstd(encoder) => {
                encoder.write_all(&input).map_err(compression_error)?;
                Ok(std::mem::take(encoder.get_mut()))
            }
        }
    }

    fn finish(self) -> crate::Result<Vec<u8>> {
        match self {
            Self::None => Ok(Vec::new()),
            Self::Gzip(encoder) => encoder.finish().map_err(compression_error),
            Self::Bzip2(encoder) => encoder.finish().map_err(compression_error),
            Self::Deflate(encoder) => encoder.finish().map_err(compression_error),
            Self::Snappy | Self::Lz4 => Ok(0_u32.to_be_bytes().to_vec()),
            Self::Zstd(encoder) => encoder.finish().map_err(compression_error),
        }
    }
}

impl TextKind {
    fn extension(self) -> &'static str {
        match self {
            Self::Csv => ".csv",
            Self::Json => ".json",
            Self::Text => ".text",
        }
    }

    pub(super) fn from_path(path: &str) -> Option<(Self, TextCompression)> {
        let path = path.to_ascii_lowercase();
        for kind in [Self::Csv, Self::Json, Self::Text] {
            if path.ends_with(kind.extension()) {
                return Some((kind, TextCompression::None));
            }
            if let Some((_, suffix)) = path.rsplit_once(&format!("{}.", kind.extension())) {
                if let Some(codec) = TextCompression::from_extension(suffix) {
                    return Some((kind, codec));
                }
            }
        }
        None
    }
}

pub(crate) fn matches_compressed_extension(file_name: &str, format_extension: &str) -> bool {
    [".gz", ".bz2", ".deflate", ".snappy", ".lz4", ".zst"]
        .iter()
        .any(|suffix| {
            file_name
                .strip_suffix(suffix)
                .is_some_and(|stem| stem.ends_with(format_extension))
        })
}

fn compression_error(error: std::io::Error) -> Error {
    Error::DataInvalid {
        message: format!("Invalid compressed text file: {error}"),
        source: Some(Box::new(error)),
    }
}

// Hadoop's SnappyCodec and Lz4Codec wrap raw codec blocks with a big-endian
// uncompressed length followed by one or more length-prefixed compressed chunks.
fn encode_hadoop_blocks(input: &[u8], codec: TextCompression) -> crate::Result<Vec<u8>> {
    const BLOCK_SIZE: usize = 256 * 1024 - 2048;
    let mut output = Vec::new();
    for block in input.chunks(BLOCK_SIZE) {
        let compressed = match codec {
            TextCompression::Snappy => {
                snap::raw::Encoder::new()
                    .compress_vec(block)
                    .map_err(|error| Error::DataInvalid {
                        message: format!("Snappy compression failed: {error}"),
                        source: Some(Box::new(error)),
                    })?
            }
            TextCompression::Lz4 => lz4_flex::block::compress(block),
            _ => unreachable!(),
        };
        output.extend_from_slice(&(block.len() as u32).to_be_bytes());
        output.extend_from_slice(&(compressed.len() as u32).to_be_bytes());
        output.extend_from_slice(&compressed);
    }
    Ok(output)
}

fn decode_hadoop_blocks(input: &[u8], codec: TextCompression) -> crate::Result<Vec<u8>> {
    let mut position = 0;
    let mut output = Vec::new();
    while position < input.len() {
        let original_size = read_block_length(input, &mut position)?;
        if original_size == 0 {
            if position != input.len() {
                return Err(invalid_block("bytes after final block"));
            }
            return Ok(output);
        }
        let block_end = output
            .len()
            .checked_add(original_size)
            .ok_or_else(|| invalid_block("block size overflow"))?;
        while output.len() < block_end {
            let compressed_size = read_block_length(input, &mut position)?;
            if compressed_size == 0 || compressed_size > input.len() - position {
                return Err(invalid_block("invalid compressed chunk length"));
            }
            let chunk = &input[position..position + compressed_size];
            position += compressed_size;
            let decoded = match codec {
                TextCompression::Snappy => snap::raw::Decoder::new()
                    .decompress_vec(chunk)
                    .map_err(|error| Error::DataInvalid {
                        message: format!("Invalid Snappy block: {error}"),
                        source: Some(Box::new(error)),
                    })?,
                TextCompression::Lz4 => {
                    lz4_flex::block::decompress(chunk, block_end - output.len()).map_err(
                        |error| Error::DataInvalid {
                            message: format!("Invalid LZ4 block: {error}"),
                            source: Some(Box::new(error)),
                        },
                    )?
                }
                _ => unreachable!(),
            };
            if decoded.is_empty() || decoded.len() > block_end - output.len() {
                return Err(invalid_block("decompressed chunk exceeds block length"));
            }
            output.extend_from_slice(&decoded);
        }
    }
    Ok(output)
}

fn read_block_length(input: &[u8], position: &mut usize) -> crate::Result<usize> {
    let bytes = input
        .get(*position..*position + 4)
        .ok_or_else(|| invalid_block("truncated length"))?;
    *position += 4;
    Ok(u32::from_be_bytes(bytes.try_into().unwrap()) as usize)
}

fn invalid_block(message: &str) -> Error {
    Error::DataInvalid {
        message: format!("Invalid Hadoop compressed text block: {message}"),
        source: None,
    }
}

#[cfg(test)]
mod compression_tests {
    use super::*;

    #[test]
    fn hadoop_block_codecs_read_multiple_blocks_and_reject_truncation() {
        let input = (0..600_000)
            .map(|value| (value % 251) as u8)
            .collect::<Vec<_>>();
        for codec in [TextCompression::Snappy, TextCompression::Lz4] {
            let mut encoded = encode_hadoop_blocks(&input, codec).unwrap();
            encoded.extend_from_slice(&0_u32.to_be_bytes());
            assert_eq!(codec.decode(&encoded).unwrap(), input);
            encoded.truncate(encoded.len() - 2);
            assert!(codec.decode(&encoded).is_err());
        }
    }
}

#[derive(Clone)]
struct TextOptions {
    line_delimiter: String,
    field_delimiter: u8,
    quote: u8,
    escape: u8,
    header: bool,
    null_literal: String,
}

impl TextOptions {
    fn new(kind: TextKind, options: &HashMap<String, String>) -> crate::Result<Self> {
        let prefix = match kind {
            TextKind::Csv => "csv",
            TextKind::Json => "json",
            TextKind::Text => "text",
        };
        let get = |key: &str, fallback: &str| {
            options
                .get(&format!("{prefix}.{key}"))
                .or_else(|| options.get(fallback))
                .cloned()
        };
        let line_delimiter = get("line-delimiter", "lineSep").unwrap_or_else(|| "\n".into());
        if line_delimiter.is_empty() {
            return Err(Error::ConfigInvalid {
                message: format!("{prefix}.line-delimiter must not be empty"),
            });
        }
        let byte = |key: &str, fallback: &str, default: u8| -> crate::Result<u8> {
            let value = get(key, fallback).unwrap_or_else(|| (default as char).to_string());
            if value.len() != 1 {
                return Err(Error::ConfigInvalid {
                    message: format!("{prefix}.{key} must be one ASCII character"),
                });
            }
            Ok(value.as_bytes()[0])
        };
        Ok(Self {
            line_delimiter,
            field_delimiter: byte("field-delimiter", "delimiter", b',')?,
            quote: byte("quote-character", "quote", b'"')?,
            escape: byte("escape-character", "escape", b'\\')?,
            header: get("include-header", "header")
                .is_some_and(|value| value.eq_ignore_ascii_case("true")),
            null_literal: get("null-literal", "nullvalue").unwrap_or_default(),
        })
    }
}

pub(super) struct TextFormatReader {
    kind: TextKind,
    compression: TextCompression,
    options: TextOptions,
}

impl TextFormatReader {
    pub(super) fn new(
        kind: TextKind,
        compression: TextCompression,
        options: &HashMap<String, String>,
    ) -> crate::Result<Self> {
        Ok(Self {
            kind,
            compression,
            options: TextOptions::new(kind, options)?,
        })
    }
}

#[async_trait]
impl FormatFileReader for TextFormatReader {
    fn select_read_fields(
        &self,
        data_schema_fields: &[DataField],
        projected_fields: &[DataField],
    ) -> Vec<DataField> {
        // CSV and TEXT have no field names in their data rows. A projection
        // still has to decode their original physical column positions.
        match self.kind {
            TextKind::Csv | TextKind::Text => data_schema_fields.to_vec(),
            TextKind::Json => projected_fields.to_vec(),
        }
    }

    async fn read_batch_stream(
        &self,
        reader: Box<dyn FileRead>,
        file_size: u64,
        read_fields: &[DataField],
        predicates: Option<&FilePredicates>,
        batch_size: Option<usize>,
        row_selection: Option<Vec<RowRange>>,
    ) -> crate::Result<ArrowRecordBatchStream> {
        if row_selection.is_some() {
            return Err(Error::Unsupported {
                message: "Row selection is not supported for line-oriented formats".into(),
            });
        }
        let bytes = self.compression.decode(&reader.read(0..file_size).await?)?;
        let fields = crate::arrow::residual::widen_scan_fields(read_fields, predicates);
        let schema = build_target_arrow_schema(&fields)?;
        if matches!(self.kind, TextKind::Csv) {
            validate_csv_schema(&schema)?;
        }
        let normalized =
            if self.options.line_delimiter == "\n" || !matches!(self.kind, TextKind::Json) {
                bytes.to_vec()
            } else {
                String::from_utf8(bytes.to_vec())
                    .map_err(|e| Error::DataInvalid {
                        message: format!("Invalid UTF-8 in text file: {e}"),
                        source: Some(Box::new(e)),
                    })?
                    .replace(&self.options.line_delimiter, "\n")
                    .into_bytes()
            };
        let normalized = if matches!(self.kind, TextKind::Json) {
            transform_json_lines(&normalized, &schema, false)?
        } else {
            normalized
        };
        let size = batch_size.unwrap_or(1024).max(1);
        let batches = match self.kind {
            TextKind::Csv => {
                let content = String::from_utf8(normalized).map_err(|e| Error::DataInvalid {
                    message: format!("Invalid UTF-8 in CSV file: {e}"),
                    source: Some(Box::new(e)),
                })?;
                let lines = content.split_terminator(&self.options.line_delimiter);
                let rows = lines
                    .skip(usize::from(self.options.header))
                    .map(|line| {
                        let line = if self.options.line_delimiter == "\n" {
                            line.strip_suffix('\r').unwrap_or(line)
                        } else {
                            line
                        };
                        let row = parse_csv_line(line, &self.options)?;
                        if row.len() != schema.fields().len() {
                            return Err(Error::DataInvalid {
                                message: format!(
                                    "CSV row has {} fields, expected {}",
                                    row.len(),
                                    schema.fields().len()
                                ),
                                source: None,
                            });
                        }
                        Ok(row)
                    })
                    .collect::<crate::Result<Vec<_>>>()?;
                rows.chunks(size)
                    .map(|chunk| {
                        let columns = schema
                            .fields()
                            .iter()
                            .enumerate()
                            .map(|(index, field)| {
                                let strings: ArrayRef = Arc::new(StringArray::from_iter(
                                    chunk
                                        .iter()
                                        .map(|row: &Vec<Option<String>>| row[index].as_deref()),
                                ));
                                csv_cast_column(&strings, field.data_type())
                            })
                            .collect::<crate::Result<Vec<_>>>()?;
                        RecordBatch::try_new(schema.clone(), columns).map_err(arrow_error)
                    })
                    .collect::<crate::Result<Vec<_>>>()?
            }
            TextKind::Json => arrow_json::ReaderBuilder::new(schema)
                .with_batch_size(size)
                .build(BufReader::new(Cursor::new(normalized)))
                .map_err(arrow_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(arrow_error)?,
            TextKind::Text => {
                validate_text_schema(&schema)?;
                let lines = String::from_utf8(normalized).map_err(|e| Error::DataInvalid {
                    message: format!("Invalid UTF-8 in text file: {e}"),
                    source: Some(Box::new(e)),
                })?;
                lines
                    .split_terminator(&self.options.line_delimiter)
                    .map(|line| {
                        if self.options.line_delimiter == "\n" {
                            line.strip_suffix('\r').unwrap_or(line)
                        } else {
                            line
                        }
                    })
                    .collect::<Vec<_>>()
                    .chunks(size)
                    .map(|chunk| {
                        RecordBatch::try_new(
                            schema.clone(),
                            vec![Arc::new(StringArray::from(chunk.to_vec()))],
                        )
                        .map_err(arrow_error)
                    })
                    .collect::<crate::Result<Vec<_>>>()?
            }
        };
        let predicates = predicates.map(|fp| FilePredicates {
            predicates: fp.predicates.clone(),
            row_filter_factory: None,
            file_fields: fp.file_fields.clone(),
        });
        Ok(stream::iter(
            batches
                .into_iter()
                .map(move |batch| match predicates.as_ref() {
                    Some(fp) => crate::arrow::residual::filter_record_batch_by_predicates(
                        batch, fp, &fields,
                    ),
                    None => Ok(batch),
                }),
        )
        .boxed())
    }
}

fn validate_text_schema(schema: &SchemaRef) -> crate::Result<()> {
    if schema.fields().len() != 1 || schema.field(0).data_type() != &DataType::Utf8 {
        return Err(Error::Unsupported {
            message: "Text format only supports a single string column".into(),
        });
    }
    Ok(())
}

fn validate_csv_schema(schema: &SchemaRef) -> crate::Result<()> {
    for field in schema.fields() {
        if !matches!(
            field.data_type(),
            DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal32(_, _)
                | DataType::Decimal64(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::FixedSizeBinary(_)
                | DataType::Date32
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Timestamp(_, _)
        ) {
            return Err(Error::Unsupported {
                message: format!(
                    "Unsupported data type for CSV format: {} ({:?})",
                    field.name(),
                    field.data_type()
                ),
            });
        }
    }
    Ok(())
}

fn arrow_error(error: arrow_schema::ArrowError) -> Error {
    Error::DataInvalid {
        message: format!("Failed to decode line-oriented format: {error}"),
        source: Some(Box::new(error)),
    }
}

fn csv_value_to_string(column: &ArrayRef, row: usize) -> Result<String, arrow_schema::ArrowError> {
    if let Some(binary) = column.as_any().downcast_ref::<BinaryArray>() {
        return Ok(base64::engine::general_purpose::STANDARD.encode(binary.value(row)));
    }
    if let Some(binary) = column.as_any().downcast_ref::<LargeBinaryArray>() {
        return Ok(base64::engine::general_purpose::STANDARD.encode(binary.value(row)));
    }
    if let Some(binary) = column.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        return Ok(base64::engine::general_purpose::STANDARD.encode(binary.value(row)));
    }
    arrow_cast::display::array_value_to_string(column, row)
}

fn parse_csv_line(line: &str, options: &TextOptions) -> crate::Result<Vec<Option<String>>> {
    let bytes = line.as_bytes();
    let mut position = 0;
    let mut fields = Vec::new();
    loop {
        let quoted = bytes.get(position) == Some(&options.quote);
        if quoted {
            position += 1;
        }
        let mut value = Vec::new();
        if quoted {
            loop {
                let byte = *bytes.get(position).ok_or_else(|| Error::DataInvalid {
                    message: "Unterminated quoted CSV field".into(),
                    source: None,
                })?;
                position += 1;
                if byte == options.quote {
                    if bytes.get(position) == Some(&options.quote) {
                        value.push(options.quote);
                        position += 1;
                        continue;
                    }
                    break;
                }
                if byte == options.escape
                    && bytes
                        .get(position)
                        .is_some_and(|next| *next == options.quote || *next == options.escape)
                {
                    value.push(bytes[position]);
                    position += 1;
                } else {
                    value.push(byte);
                }
            }
            if position < bytes.len() && bytes[position] != options.field_delimiter {
                return Err(Error::DataInvalid {
                    message: "Unexpected bytes after quoted CSV field".into(),
                    source: None,
                });
            }
        } else {
            while position < bytes.len() && bytes[position] != options.field_delimiter {
                value.push(bytes[position]);
                position += 1;
            }
        }
        let value = String::from_utf8(value).map_err(|e| Error::DataInvalid {
            message: format!("Invalid UTF-8 in CSV field: {e}"),
            source: Some(Box::new(e)),
        })?;
        fields.push(if !quoted && value == options.null_literal {
            None
        } else {
            Some(value)
        });
        if position == bytes.len() {
            break;
        }
        position += 1;
    }
    Ok(fields)
}

fn csv_cast_column(column: &ArrayRef, target: &DataType) -> crate::Result<ArrayRef> {
    if matches!(
        target,
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_)
    ) {
        let values = column.as_any().downcast_ref::<StringArray>().unwrap();
        let decoded = values
            .iter()
            .map(|value| {
                value
                    .map(|value| {
                        base64::engine::general_purpose::STANDARD
                            .decode(value)
                            .map_err(|e| Error::DataInvalid {
                                message: format!("Invalid Base64 in CSV binary field: {e}"),
                                source: Some(Box::new(e)),
                            })
                    })
                    .transpose()
            })
            .collect::<crate::Result<Vec<_>>>()?;
        return Ok(match target {
            DataType::Binary => {
                Arc::new(BinaryArray::from_iter(decoded.iter().map(|v| v.as_deref())))
            }
            DataType::LargeBinary => Arc::new(LargeBinaryArray::from_iter(
                decoded.iter().map(|v| v.as_deref()),
            )),
            DataType::FixedSizeBinary(_) => {
                let binary: ArrayRef =
                    Arc::new(BinaryArray::from_iter(decoded.iter().map(|v| v.as_deref())));
                return arrow_cast::cast(&binary, target).map_err(arrow_error);
            }
            _ => unreachable!(),
        });
    }
    arrow_cast::cast(column, target).map_err(arrow_error)
}

/// Normalize Java's Base64 binary and string boolean values for Arrow JSON,
/// while retaining the exact tokens of unrelated numeric fields.
fn transform_json_lines(bytes: &[u8], schema: &SchemaRef, to_java: bool) -> crate::Result<Vec<u8>> {
    if !schema
        .fields()
        .iter()
        .any(|f| contains_json_conversion(f.data_type()))
    {
        return Ok(bytes.to_vec());
    }
    let mut output = Vec::with_capacity(bytes.len());
    for line in bytes.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        let mut value: serde_json::Value =
            serde_json::from_slice(line).map_err(|e| Error::DataInvalid {
                message: format!("Invalid JSON line: {e}"),
                source: Some(Box::new(e)),
            })?;
        for field in schema.fields() {
            if let Some(child) = value.get_mut(field.name()) {
                transform_json_value(child, field.data_type(), to_java)?;
            }
        }
        serde_json::to_writer(&mut output, &value).map_err(|e| Error::DataInvalid {
            message: format!("Failed to encode JSON line: {e}"),
            source: Some(Box::new(e)),
        })?;
        output.push(b'\n');
    }
    Ok(output)
}

fn contains_json_conversion(data_type: &DataType) -> bool {
    match data_type {
        DataType::Boolean
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_) => true,
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| contains_json_conversion(field.data_type())),
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            contains_json_conversion(field.data_type())
        }
        DataType::Map(field, _) => contains_json_conversion(field.data_type()),
        _ => false,
    }
}

fn transform_json_value(
    value: &mut serde_json::Value,
    data_type: &DataType,
    to_java: bool,
) -> crate::Result<()> {
    if value.is_null() {
        return Ok(());
    }
    match data_type {
        DataType::Boolean => {
            if to_java {
                if let Some(boolean) = value.as_bool() {
                    *value = serde_json::Value::String(boolean.to_string());
                }
            } else if let Some(boolean) = value.as_str() {
                if boolean.eq_ignore_ascii_case("true") {
                    *value = serde_json::Value::Bool(true);
                } else if boolean.eq_ignore_ascii_case("false") {
                    *value = serde_json::Value::Bool(false);
                }
            }
        }
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            let encoded = value.as_str().ok_or_else(|| Error::DataInvalid {
                message: "JSON binary value must be a string".into(),
                source: None,
            })?;
            let bytes = if to_java {
                hex::decode(encoded).map_err(|e| Error::DataInvalid {
                    message: format!("Invalid hex from Arrow JSON binary writer: {e}"),
                    source: Some(Box::new(e)),
                })?
            } else {
                base64::engine::general_purpose::STANDARD
                    .decode(encoded)
                    .map_err(|e| Error::DataInvalid {
                        message: format!("Invalid Base64 in JSON binary field: {e}"),
                        source: Some(Box::new(e)),
                    })?
            };
            *value = serde_json::Value::String(if to_java {
                base64::engine::general_purpose::STANDARD.encode(bytes)
            } else {
                hex::encode(bytes)
            });
        }
        DataType::Struct(fields) => {
            for field in fields {
                if let Some(child) = value.get_mut(field.name()) {
                    transform_json_value(child, field.data_type(), to_java)?;
                }
            }
        }
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            if let Some(items) = value.as_array_mut() {
                for item in items {
                    transform_json_value(item, field.data_type(), to_java)?;
                }
            }
        }
        DataType::Map(field, _) => {
            if let DataType::Struct(fields) = field.data_type() {
                if let Some(values) = value.as_object_mut() {
                    for item in values.values_mut() {
                        transform_json_value(item, fields[1].data_type(), to_java)?;
                    }
                }
            }
        }
        _ => {}
    }
    Ok(())
}

pub(super) struct TextFormatWriter {
    writer: Box<dyn FileWrite>,
    schema: SchemaRef,
    kind: TextKind,
    options: TextOptions,
    bytes_written: usize,
    encoder: Option<TextEncoder>,
    header_written: bool,
}

impl TextFormatWriter {
    pub(super) async fn new(
        output: &OutputFile,
        schema: SchemaRef,
        kind: TextKind,
        compression: TextCompression,
        options: Option<&HashMap<String, String>>,
    ) -> crate::Result<Self> {
        if matches!(kind, TextKind::Text) {
            validate_text_schema(&schema)?;
        }
        if matches!(kind, TextKind::Csv) {
            validate_csv_schema(&schema)?;
        }
        let options = TextOptions::new(kind, &options.cloned().unwrap_or_default())?;
        let encoder = TextEncoder::new(compression)?;
        Ok(Self {
            writer: output.writer().await?,
            schema,
            kind,
            options,
            bytes_written: 0,
            encoder: Some(encoder),
            header_written: false,
        })
    }
}

#[async_trait]
impl FormatFileWriter for TextFormatWriter {
    async fn write(&mut self, batch: &RecordBatch) -> crate::Result<()> {
        if batch.schema() != self.schema {
            return Err(Error::DataInvalid {
                message: "Text format batch schema differs from file schema".into(),
                source: None,
            });
        }
        let mut bytes = Vec::new();
        match self.kind {
            TextKind::Csv => {
                if self.options.header && !self.header_written {
                    let names = self.schema.fields().iter().map(|f| Some(f.name().as_str()));
                    append_csv_row(&mut bytes, names, &self.options)?;
                    self.header_written = true;
                }
                for row in 0..batch.num_rows() {
                    let values = batch.columns().iter().map(|column| {
                        if column.is_null(row) {
                            None
                        } else {
                            Some(csv_value_to_string(column, row))
                        }
                    });
                    let values = values
                        .map(|value| value.transpose())
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(arrow_error)?;
                    append_csv_row(
                        &mut bytes,
                        values.iter().map(|v| v.as_deref()),
                        &self.options,
                    )?;
                }
            }
            TextKind::Json => {
                let mut writer = arrow_json::WriterBuilder::new()
                    .with_explicit_nulls(true)
                    .build::<_, arrow_json::writer::LineDelimited>(&mut bytes);
                writer.write(batch).map_err(arrow_error)?;
                writer.finish().map_err(arrow_error)?;
                drop(writer);
                bytes = transform_json_lines(&bytes, &self.schema, true)?;
                if self.options.line_delimiter != "\n" {
                    bytes = String::from_utf8(bytes)
                        .map_err(|e| Error::DataInvalid {
                            message: e.to_string(),
                            source: Some(Box::new(e)),
                        })?
                        .replace('\n', &self.options.line_delimiter)
                        .into_bytes();
                }
            }
            TextKind::Text => {
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    if !values.is_null(row) {
                        bytes.extend_from_slice(values.value(row).as_bytes());
                    }
                    bytes.extend_from_slice(self.options.line_delimiter.as_bytes());
                }
            }
        }
        let compressed = self.encoder.as_mut().unwrap().write(bytes)?;
        self.bytes_written += compressed.len();
        if !compressed.is_empty() {
            self.writer.write(Bytes::from(compressed)).await?;
        }
        Ok(())
    }

    fn num_bytes(&self) -> usize {
        self.bytes_written
    }
    fn in_progress_size(&self) -> usize {
        0
    }
    fn retains_batch_data(&self) -> bool {
        false
    }
    async fn flush(&mut self) -> crate::Result<()> {
        Ok(())
    }
    async fn close(mut self: Box<Self>) -> crate::Result<FormatWriteResult> {
        let compressed = self.encoder.take().unwrap().finish()?;
        self.bytes_written += compressed.len();
        if !compressed.is_empty() {
            self.writer.write(Bytes::from(compressed)).await?;
        }
        self.writer.close().await?;
        Ok(FormatWriteResult::new(self.bytes_written as u64))
    }
}

fn append_csv_row<'a>(
    out: &mut Vec<u8>,
    values: impl Iterator<Item = Option<&'a str>>,
    options: &TextOptions,
) -> crate::Result<()> {
    for (index, value) in values.enumerate() {
        if index > 0 {
            out.push(options.field_delimiter);
        }
        let Some(value) = value else {
            out.extend_from_slice(options.null_literal.as_bytes());
            continue;
        };
        if value.contains(&options.line_delimiter)
            || (options.line_delimiter == "\n" && (value.contains('\r') || value.contains('\n')))
        {
            return Err(Error::DataInvalid {
                message: "CSV value contains the row separator".into(),
                source: None,
            });
        }
        let quoted = value.as_bytes().contains(&options.field_delimiter)
            || value.as_bytes().contains(&options.quote)
            || value.as_bytes().contains(&options.escape)
            || value
                .as_bytes()
                .contains(&options.line_delimiter.as_bytes()[0])
            || value == options.null_literal;
        if quoted {
            out.push(options.quote);
        }
        for byte in value.bytes() {
            if quoted && (byte == options.quote || byte == options.escape) {
                out.push(options.escape);
            }
            out.push(byte);
        }
        if quoted {
            out.push(options.quote);
        }
    }
    out.extend_from_slice(options.line_delimiter.as_bytes());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Field, Schema};

    #[test]
    fn nested_java_json_boolean_strings_are_normalized() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "outer",
            DataType::Struct(
                vec![
                    Field::new("flag", DataType::Boolean, true),
                    Field::new(
                        "flags",
                        DataType::List(Arc::new(Field::new("item", DataType::Boolean, true))),
                        true,
                    ),
                ]
                .into(),
            ),
            true,
        )]));
        let input = b"{\"outer\":{\"flag\":\"true\",\"flags\":[\"false\",null,\"true\"]}}\n";
        let normalized = transform_json_lines(input, &schema, false).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&normalized).unwrap();
        assert_eq!(value["outer"]["flag"], true);
        assert_eq!(
            value["outer"]["flags"],
            serde_json::json!([false, null, true])
        );

        let java = transform_json_lines(&normalized, &schema, true).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&java).unwrap();
        assert_eq!(value["outer"]["flag"], "true");
        assert_eq!(
            value["outer"]["flags"],
            serde_json::json!(["false", null, "true"])
        );
    }
}
