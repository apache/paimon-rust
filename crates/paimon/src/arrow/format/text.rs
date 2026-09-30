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
use std::io::{self, BufReader, Cursor, Read, Write};
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

/// Decode one Hadoop block at a time. A malformed length must not allocate an
/// attacker-controlled amount of memory before the file can be rejected.
struct HadoopBlockReader<R> {
    reader: R,
    codec: TextCompression,
    block: Vec<u8>,
    position: usize,
    finished: bool,
}

impl<R: Read> HadoopBlockReader<R> {
    fn new(reader: R, codec: TextCompression) -> Self {
        Self {
            reader,
            codec,
            block: Vec::new(),
            position: 0,
            finished: false,
        }
    }

    fn load_block(&mut self) -> io::Result<()> {
        const MAX_BLOCK_SIZE: usize = 64 * 1024 * 1024;
        let mut length = [0; 4];
        if self.reader.read(&mut length[..1])? == 0 {
            self.finished = true;
            return Ok(());
        }
        self.reader.read_exact(&mut length[1..])?;
        let original_size = u32::from_be_bytes(length) as usize;
        if original_size == 0 {
            if self.reader.read(&mut length[..1])? != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "bytes after final block",
                ));
            }
            self.finished = true;
            return Ok(());
        }
        if original_size > MAX_BLOCK_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Hadoop block is too large",
            ));
        }
        self.block.clear();
        while self.block.len() < original_size {
            self.reader.read_exact(&mut length)?;
            let compressed_size = u32::from_be_bytes(length) as usize;
            if compressed_size == 0 || compressed_size > MAX_BLOCK_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid compressed chunk length",
                ));
            }
            let mut compressed = vec![0; compressed_size];
            self.reader.read_exact(&mut compressed)?;
            let remaining = original_size - self.block.len();
            let decoded = match self.codec {
                TextCompression::Snappy => {
                    let decoded_len = snap::raw::decompress_len(&compressed)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                    if decoded_len > remaining {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Snappy chunk exceeds block length",
                        ));
                    }
                    snap::raw::Decoder::new()
                        .decompress_vec(&compressed)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                }
                TextCompression::Lz4 => lz4_flex::block::decompress(&compressed, remaining)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                _ => unreachable!(),
            };
            if decoded.is_empty() || decoded.len() > remaining {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "decoded chunk exceeds block length",
                ));
            }
            self.block.extend_from_slice(&decoded);
        }
        self.position = 0;
        Ok(())
    }
}

impl<R: Read> Read for HadoopBlockReader<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        if self.position == self.block.len() && !self.finished {
            self.load_block()?;
        }
        if self.finished {
            return Ok(0);
        }
        let size = output.len().min(self.block.len() - self.position);
        output[..size].copy_from_slice(&self.block[self.position..self.position + size]);
        self.position += size;
        Ok(size)
    }
}

#[cfg(test)]
mod compression_tests {
    use super::*;
    use crate::spec::{DataType as PaimonDataType, VarCharType};
    use futures::TryStreamExt;
    use std::ops::Range;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountedReader {
        bytes: Bytes,
        read_bytes: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl FileRead for CountedReader {
        async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
            self.read_bytes
                .fetch_add((range.end - range.start) as usize, Ordering::SeqCst);
            Ok(self.bytes.slice(range.start as usize..range.end as usize))
        }
    }

    struct PanickingReader;

    #[async_trait]
    impl FileRead for PanickingReader {
        async fn read(&self, _range: Range<u64>) -> crate::Result<Bytes> {
            panic!("simulated text read panic");
        }
    }

    #[test]
    fn hadoop_block_codecs_read_multiple_blocks_and_reject_truncation() {
        let input = (0..600_000)
            .map(|value| (value % 251) as u8)
            .collect::<Vec<_>>();
        for codec in [TextCompression::Snappy, TextCompression::Lz4] {
            let mut encoded = encode_hadoop_blocks(&input, codec).unwrap();
            encoded.extend_from_slice(&0_u32.to_be_bytes());
            let mut streamed = Vec::new();
            HadoopBlockReader::new(Cursor::new(&encoded), codec)
                .read_to_end(&mut streamed)
                .unwrap();
            assert_eq!(streamed, input);
            encoded.truncate(encoded.len() - 2);
            assert!(HadoopBlockReader::new(Cursor::new(&encoded), codec)
                .read_to_end(&mut Vec::new())
                .is_err());
        }
    }

    #[test]
    fn reads_hadoop_342_java_codec_outputs() {
        // Produced by Hadoop 3.4.2 SnappyCodec and Lz4Codec from the same input.
        // Hadoop does not add an explicit terminal block in these files.
        let expected = [b"hello,payload\n".repeat(20), (0..16).collect::<Vec<_>>()].concat();
        for (codec, encoded) in [
            (
                TextCompression::Snappy,
                "0000012800000030a8023468656c6c6f2c7061796c6f61640afe0e00fe0e00fe0e00fe0e00190e3c000102030405060708090a0b0c0d0e0f",
            ),
            (
                TextCompression::Lz4,
                "0000012800000024ef68656c6c6f2c7061796c6f61640a0e00f7f001000102030405060708090a0b0c0d0e0f",
            ),
        ] {
            let encoded = hex::decode(encoded).unwrap();
            let mut actual = Vec::new();
            HadoopBlockReader::new(Cursor::new(encoded), codec)
                .read_to_end(&mut actual)
                .unwrap();
            assert_eq!(actual, expected, "{codec:?}");
        }
    }

    #[test]
    fn line_delimiter_crosses_read_chunk_boundary() {
        let mut input = vec![b'a'; 64 * 1024 - 1];
        input.extend_from_slice(b"||tail||");
        let mut lines = DelimitedLines::new(Cursor::new(input), "||");
        assert_eq!(lines.next_line().unwrap().unwrap().len(), 64 * 1024 - 1);
        assert_eq!(lines.next_line().unwrap().unwrap(), b"tail");
        assert!(lines.next_line().unwrap().is_none());
    }

    #[tokio::test]
    async fn text_reader_emits_first_batch_before_reading_whole_file() {
        let bytes = Bytes::from("row\n".repeat(250_000));
        let size = bytes.len();
        let read_bytes = Arc::new(AtomicUsize::new(0));
        let reader =
            TextFormatReader::new(TextKind::Text, TextCompression::None, &HashMap::new()).unwrap();
        let fields = [DataField::new(
            0,
            "line".to_string(),
            PaimonDataType::VarChar(VarCharType::string_type()),
        )];
        let mut stream = reader
            .read_batch_stream(
                Box::new(CountedReader {
                    bytes,
                    read_bytes: read_bytes.clone(),
                }),
                size as u64,
                &fields,
                None,
                Some(1024),
                None,
            )
            .await
            .unwrap();
        assert_eq!(stream.next().await.unwrap().unwrap().num_rows(), 1024);
        assert!(read_bytes.load(Ordering::SeqCst) < size / 2);
    }

    #[tokio::test]
    async fn unterminated_final_line_is_returned_across_batches() {
        let fields = [DataField::new(
            0,
            "line".to_string(),
            PaimonDataType::VarChar(VarCharType::string_type()),
        )];
        for kind in [TextKind::Text, TextKind::Csv, TextKind::Json] {
            for rows in [1, 2050] {
                let line = if kind == TextKind::Json {
                    b"{\"line\":\"value\"}".as_slice()
                } else {
                    b"value".as_slice()
                };
                let mut input = Vec::new();
                for index in 0..rows {
                    if index > 0 {
                        input.push(b'\n');
                    }
                    input.extend_from_slice(line);
                }
                for compression in [
                    TextCompression::None,
                    TextCompression::Gzip,
                    TextCompression::Bzip2,
                    TextCompression::Deflate,
                    TextCompression::Snappy,
                    TextCompression::Lz4,
                    TextCompression::Zstd,
                ] {
                    let mut encoder = TextEncoder::new(compression).unwrap();
                    let mut encoded = encoder.write(input.clone()).unwrap();
                    encoded.extend_from_slice(&encoder.finish().unwrap());
                    let size = encoded.len();
                    let reader = TextFormatReader::new(kind, compression, &HashMap::new()).unwrap();
                    let batches: Vec<RecordBatch> = reader
                        .read_batch_stream(
                            Box::new(CountedReader {
                                bytes: Bytes::from(encoded),
                                read_bytes: Arc::new(AtomicUsize::new(0)),
                            }),
                            size as u64,
                            &fields,
                            None,
                            Some(1024),
                            None,
                        )
                        .await
                        .unwrap()
                        .try_collect()
                        .await
                        .unwrap();
                    assert_eq!(
                        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                        rows,
                        "kind={kind:?}, compression={compression:?}"
                    );
                    assert_eq!(
                        batches
                            .iter()
                            .map(RecordBatch::num_rows)
                            .collect::<Vec<_>>(),
                        if rows == 1 {
                            vec![1]
                        } else {
                            vec![1024, 1024, 2]
                        },
                        "kind={kind:?}, compression={compression:?}"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn text_reader_reports_worker_panic() {
        let fields = [DataField::new(
            0,
            "line".to_string(),
            PaimonDataType::VarChar(VarCharType::string_type()),
        )];
        let reader =
            TextFormatReader::new(TextKind::Text, TextCompression::None, &HashMap::new()).unwrap();
        let result: crate::Result<Vec<RecordBatch>> = reader
            .read_batch_stream(Box::new(PanickingReader), 1, &fields, None, None, None)
            .await
            .unwrap()
            .try_collect()
            .await;
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("text decoding task failed"));
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
        let fields = crate::arrow::residual::widen_scan_fields(read_fields, predicates);
        let schema = build_target_arrow_schema(&fields)?;
        match self.kind {
            TextKind::Csv => validate_csv_schema(&schema)?,
            TextKind::Text => validate_text_schema(&schema)?,
            TextKind::Json => {}
        }
        let kind = self.kind;
        let compression = self.compression;
        let options = self.options.clone();
        let size = batch_size.unwrap_or(1024).max(1);
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        let chunks = stream::try_unfold((reader, 0_u64), move |(reader, start)| async move {
            if start >= file_size {
                return Ok::<_, io::Error>(None);
            }
            let end = start.saturating_add(64 * 1024).min(file_size);
            let bytes = reader.read(start..end).await.map_err(io::Error::other)?;
            if bytes.len() != (end - start) as usize {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "text file changed while reading",
                ));
            }
            Ok(Some((bytes, (reader, end))))
        });
        let source =
            tokio_util::io::SyncIoBridge::new(tokio_util::io::StreamReader::new(Box::pin(chunks)));
        let worker = tokio::task::spawn_blocking(move || {
            let source: Box<dyn Read> = match compression {
                TextCompression::None => Box::new(source),
                TextCompression::Gzip => Box::new(flate2::read::MultiGzDecoder::new(source)),
                TextCompression::Bzip2 => Box::new(bzip2::read::MultiBzDecoder::new(source)),
                TextCompression::Deflate => Box::new(flate2::read::ZlibDecoder::new(source)),
                TextCompression::Snappy | TextCompression::Lz4 => {
                    Box::new(HadoopBlockReader::new(source, compression))
                }
                TextCompression::Zstd => match zstd::stream::read::Decoder::new(source) {
                    Ok(decoder) => Box::new(decoder),
                    Err(error) => {
                        let _ = sender.blocking_send(Err(compression_error(error)));
                        return;
                    }
                },
            };
            if let Err(error) = decode_text_batches(source, kind, options, schema, size, &sender) {
                let _ = sender.blocking_send(Err(error));
            }
        });
        let predicates = predicates.map(|fp| FilePredicates {
            predicates: fp.predicates.clone(),
            row_filter_factory: None,
            file_fields: fp.file_fields.clone(),
        });
        Ok(stream::unfold(
            (receiver, Some(worker)),
            |(mut receiver, mut worker)| async move {
                match receiver.recv().await {
                    Some(batch) => Some((batch, (receiver, worker))),
                    None => {
                        let worker = worker.take()?;
                        worker.await.err().map(|error| {
                            (
                                Err(Error::UnexpectedError {
                                    message: "text decoding task failed".to_string(),
                                    source: Some(Box::new(error)),
                                }),
                                (receiver, None),
                            )
                        })
                    }
                }
            },
        )
        .map(move |batch| {
            let batch = batch?;
            match predicates.as_ref() {
                Some(fp) => {
                    crate::arrow::residual::filter_record_batch_by_predicates(batch, fp, &fields)
                }
                None => Ok(batch),
            }
        })
        .boxed())
    }
}

struct DelimitedLines<R> {
    reader: BufReader<R>,
    delimiter: Vec<u8>,
    pending: Vec<u8>,
    search_from: usize,
    eof: bool,
}

impl<R: Read> DelimitedLines<R> {
    fn new(reader: R, delimiter: &str) -> Self {
        Self {
            reader: BufReader::new(reader),
            delimiter: delimiter.as_bytes().to_vec(),
            pending: Vec::new(),
            search_from: 0,
            eof: false,
        }
    }

    fn next_line(&mut self) -> io::Result<Option<Vec<u8>>> {
        loop {
            if let Some(relative) = self.pending[self.search_from..]
                .windows(self.delimiter.len())
                .position(|window| window == self.delimiter)
            {
                let position = self.search_from + relative;
                let rest = self.pending.split_off(position + self.delimiter.len());
                let mut line = std::mem::replace(&mut self.pending, rest);
                line.truncate(position);
                self.search_from = 0;
                return Ok(Some(line));
            }
            if self.eof {
                self.search_from = 0;
                return Ok((!self.pending.is_empty()).then(|| std::mem::take(&mut self.pending)));
            }
            self.search_from = self
                .pending
                .len()
                .saturating_sub(self.delimiter.len().saturating_sub(1));
            let mut buffer = [0; 64 * 1024];
            let read = self.reader.read(&mut buffer)?;
            if read == 0 {
                self.eof = true;
            } else {
                self.pending.extend_from_slice(&buffer[..read]);
            }
        }
    }
}

fn decode_text_batches(
    source: Box<dyn Read>,
    kind: TextKind,
    options: TextOptions,
    schema: SchemaRef,
    size: usize,
    sender: &tokio::sync::mpsc::Sender<crate::Result<RecordBatch>>,
) -> crate::Result<()> {
    let mut lines = DelimitedLines::new(source, &options.line_delimiter);
    let mut first_line = true;
    loop {
        let mut chunk = Vec::new();
        while chunk.len() < size {
            let Some(mut line) = lines.next_line().map_err(text_read_error)? else {
                break;
            };
            if first_line && kind == TextKind::Csv && options.header {
                first_line = false;
                continue;
            }
            first_line = false;
            if options.line_delimiter == "\n" && line.last() == Some(&b'\r') {
                line.pop();
            }
            chunk.push(line);
        }
        if chunk.is_empty() {
            break;
        }
        for batch in decode_text_chunk(&chunk, kind, &options, &schema)? {
            if sender.blocking_send(Ok(batch)).is_err() {
                return Ok(());
            }
        }
    }
    Ok(())
}

fn decode_text_chunk(
    lines: &[Vec<u8>],
    kind: TextKind,
    options: &TextOptions,
    schema: &SchemaRef,
) -> crate::Result<Vec<RecordBatch>> {
    match kind {
        TextKind::Csv => {
            let rows = lines
                .iter()
                .map(|line| {
                    let line = std::str::from_utf8(line).map_err(|e| Error::DataInvalid {
                        message: format!("Invalid UTF-8 in CSV file: {e}"),
                        source: Some(Box::new(e)),
                    })?;
                    let row = if line.trim().is_empty() {
                        vec![None; schema.fields().len()]
                    } else {
                        parse_csv_line(line, options)?
                    };
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
            let columns = schema
                .fields()
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    let strings: ArrayRef = Arc::new(StringArray::from_iter(
                        rows.iter().map(|row| row[index].as_deref()),
                    ));
                    csv_cast_column(&strings, field.data_type())
                })
                .collect::<crate::Result<Vec<_>>>()?;
            Ok(vec![
                RecordBatch::try_new(schema.clone(), columns).map_err(arrow_error)?
            ])
        }
        TextKind::Json => {
            let mut input = Vec::new();
            for line in lines {
                input.extend_from_slice(line);
                input.push(b'\n');
            }
            let normalized = transform_json_lines(&input, schema, false)?;
            arrow_json::ReaderBuilder::new(schema.clone())
                .with_batch_size(lines.len())
                .build(BufReader::new(Cursor::new(normalized)))
                .map_err(arrow_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(arrow_error)
        }
        TextKind::Text => {
            let strings = lines
                .iter()
                .map(|line| {
                    std::str::from_utf8(line).map_err(|e| Error::DataInvalid {
                        message: format!("Invalid UTF-8 in text file: {e}"),
                        source: Some(Box::new(e)),
                    })
                })
                .collect::<crate::Result<Vec<_>>>()?;
            Ok(vec![RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(StringArray::from(strings))],
            )
            .map_err(arrow_error)?])
        }
    }
}

fn text_read_error(error: io::Error) -> Error {
    Error::DataInvalid {
        message: format!("Failed to read line-oriented format: {error}"),
        source: Some(Box::new(error)),
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
