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

//! Java BlobElementSerializer record layouts, sharing framing and streamed payload copying.

use super::{
    checked_blob_entry_length, encode_delta_varints_write, validate_read_fields, BlobFieldKind,
    BLOB_ARRAY_MAGIC_NUMBER, BLOB_ARRAY_VERSION, BLOB_FORMAT_VERSION, BLOB_MAGIC_NUMBER_BYTES,
    BLOB_MAP_MAGIC_NUMBER, BLOB_MAP_VERSION,
};
use crate::arrow::format::{FormatFileWriter, FormatWriteResult, FormatWriterFactory};
use crate::io::uri_reader::ReusingBlobRefStreamProvider;
use crate::io::{FileIO, FileRead, FileWrite, OutputFile, UriInputStream, UriReaderFactory};
use crate::spec::{BlobConsumer, BlobDescriptor, CoreOptions, DataField, DataType};
use crate::{Error, Result};
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Int16Array, Int32Array,
    Int64Array, Int8Array, LargeBinaryArray, ListArray, MapArray, RecordBatch, StringArray,
    Time32MillisecondArray,
};
use async_trait::async_trait;
use bytes::Bytes;
use crc32fast::Hasher;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

// Remote FileRead ranges open new requests. Keep read-ahead independent of
// the Java-sized copy buffer so a 4 KiB buffer does not mean 4 KiB GETs.
const SOURCE_READ_SIZE: u64 = 8 * 1024 * 1024;

#[cfg(test)]
#[path = "writer_consumer_tests.rs"]
mod consumer_tests;

#[cfg(test)]
#[path = "writer_uri_reader_tests.rs"]
mod uri_reader_tests;

pub(crate) struct BlobWriterFactory {
    file_io: Option<FileIO>,
    field: Option<DataField>,
    consumer: Option<Arc<dyn BlobConsumer>>,
    uri_reader_factory: Option<Arc<dyn UriReaderFactory>>,
    copy_buffer_size: usize,
}

impl BlobWriterFactory {
    pub(crate) fn new(
        file_io: Option<FileIO>,
        field: Option<&DataField>,
        options: Option<&HashMap<String, String>>,
    ) -> Result<Self> {
        let defaults = HashMap::new();
        Ok(Self {
            file_io,
            field: field.cloned(),
            consumer: None,
            uri_reader_factory: None,
            copy_buffer_size: CoreOptions::new(options.unwrap_or(&defaults))
                .blob_copy_buffer_size()?,
        })
    }

    pub(crate) fn with_consumer(mut self, consumer: Arc<dyn BlobConsumer>) -> Self {
        self.consumer = Some(consumer);
        self
    }

    pub(crate) fn with_uri_reader_factory(mut self, factory: Arc<dyn UriReaderFactory>) -> Self {
        self.uri_reader_factory = Some(factory);
        self
    }
}

#[async_trait]
impl FormatWriterFactory for BlobWriterFactory {
    async fn create_writer(
        &self,
        output: &OutputFile,
        _compression: &str,
    ) -> Result<Box<dyn FormatFileWriter>> {
        let stream = if self.consumer.is_some() {
            output.flushable_writer().await?
        } else {
            output.writer().await?
        };
        Ok(Box::new(
            BlobFormatWriter::from_stream(
                stream,
                output,
                self.file_io.clone(),
                self.field.as_ref(),
            )?
            .with_copy_buffer_size(self.copy_buffer_size)
            .with_uri_reader_factory(self.uri_reader_factory.clone())
            .with_consumer(self.consumer.clone()),
        ))
    }
}

pub(crate) struct BlobFormatWriter {
    writer: Box<dyn FileWrite>,
    file_io: Option<FileIO>,
    kind: BlobFieldKind,
    path: String,
    field_name: String,
    consumer: Option<Arc<dyn BlobConsumer>>,
    uri_reader_factory: Option<Arc<dyn UriReaderFactory>>,
    reference_streams: ReusingBlobRefStreamProvider,
    copy_buffer_size: usize,
    bytes_written: u64,
    lengths: Vec<i64>,
}

fn invalid(message: impl Into<String>) -> Error {
    Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

impl BlobFormatWriter {
    pub(crate) async fn new(
        output: &OutputFile,
        file_io: Option<FileIO>,
        field: Option<&DataField>,
    ) -> Result<Self> {
        Self::from_stream(output.writer().await?, output, file_io, field)
    }

    fn from_stream(
        writer: Box<dyn FileWrite>,
        output: &OutputFile,
        file_io: Option<FileIO>,
        field: Option<&DataField>,
    ) -> Result<Self> {
        let kind = match field {
            Some(field) => validate_read_fields(std::slice::from_ref(field))?.unwrap(),
            None => BlobFieldKind::Scalar,
        };
        if let BlobFieldKind::Map(key_type) = &kind {
            validate_key_type(key_type)?;
        }
        Ok(Self {
            writer,
            file_io,
            kind,
            path: output.location().to_string(),
            field_name: field.map_or_else(String::new, |field| field.name().to_string()),
            consumer: None,
            uri_reader_factory: None,
            reference_streams: ReusingBlobRefStreamProvider::default(),
            copy_buffer_size: 4 * 1024,
            bytes_written: 0,
            lengths: Vec::new(),
        })
    }

    pub(crate) fn with_copy_buffer_size(mut self, size: usize) -> Self {
        debug_assert!(size > 0);
        self.copy_buffer_size = size;
        self
    }

    pub(crate) fn with_uri_reader_factory(
        mut self,
        factory: Option<Arc<dyn UriReaderFactory>>,
    ) -> Self {
        self.uri_reader_factory = factory;
        self
    }

    fn with_consumer(mut self, consumer: Option<Arc<dyn BlobConsumer>>) -> Self {
        self.consumer = consumer;
        self
    }

    fn accept(&self, offset: u64, length: i64) -> Result<bool> {
        match &self.consumer {
            Some(consumer) => consumer.accept(
                &self.field_name,
                Some(&BlobDescriptor::new(
                    self.path.clone(),
                    i64::try_from(offset).map_err(|_| invalid("BLOB offset exceeds i64"))?,
                    length,
                )),
            ),
            None => Ok(false),
        }
    }

    /// The callback range excludes the common entry magic and trailer.
    pub(crate) async fn write_managed_value(&mut self, value: &[u8]) -> Result<(i64, i64)> {
        if !matches!(self.kind, BlobFieldKind::Scalar) {
            return Err(invalid("Managed BLOB packs require scalar elements"));
        }
        let start = self.bytes_written;
        let length = self.write_scalar(value).await?;
        Ok((
            i64::try_from(start + 4).map_err(|_| invalid("Managed BLOB offset exceeds i64"))?,
            length,
        ))
    }

    async fn write_bytes(&mut self, bytes: Bytes, hasher: &mut Hasher) -> Result<()> {
        let end = self
            .bytes_written
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| invalid("BLOB file size overflows u64"))?;
        hasher.update(&bytes);
        self.writer.write(bytes).await?;
        self.bytes_written = end;
        Ok(())
    }

    async fn start_record(&mut self) -> Result<(u64, Hasher)> {
        let start = self.bytes_written;
        let mut hasher = Hasher::new();
        self.write_bytes(
            Bytes::copy_from_slice(&BLOB_MAGIC_NUMBER_BYTES),
            &mut hasher,
        )
        .await?;
        Ok((start, hasher))
    }

    async fn finish_record(&mut self, start: u64, mut hasher: Hasher) -> Result<()> {
        let length = checked_blob_entry_length(self.bytes_written - start - 4)?;
        self.write_bytes(Bytes::copy_from_slice(&length.to_le_bytes()), &mut hasher)
            .await?;
        let crc = hasher.finalize().to_le_bytes();
        // CRC itself is outside its checksum domain.
        self.writer.write(Bytes::copy_from_slice(&crc)).await?;
        self.bytes_written = self
            .bytes_written
            .checked_add(4)
            .ok_or_else(|| invalid("BLOB file size overflows u64"))?;
        self.lengths.push(length);
        Ok(())
    }

    async fn write_scalar(&mut self, value: &[u8]) -> Result<i64> {
        let (start, mut hasher) = self.start_record().await?;
        let length = self.write_payload(value, &mut hasher).await?;
        self.finish_record(start, hasher).await?;
        if self.accept(start + 4, length)? {
            self.writer.flush().await?;
        }
        Ok(length)
    }

    /// Resolve descriptors without materializing a complete referenced object.
    async fn write_payload(&mut self, value: &[u8], hasher: &mut Hasher) -> Result<i64> {
        if !BlobDescriptor::is_blob_descriptor(value) {
            for chunk in value.chunks(self.copy_buffer_size) {
                self.write_bytes(Bytes::copy_from_slice(chunk), hasher)
                    .await?;
            }
            return i64::try_from(value.len()).map_err(|_| invalid("BLOB payload exceeds i64"));
        }
        let descriptor = BlobDescriptor::deserialize(value)?;
        let range = descriptor.range_spec()?;
        if let Some(factory) = self.uri_reader_factory.clone() {
            return self
                .copy_custom_reference(factory.as_ref(), &descriptor, hasher)
                .await;
        }
        let file_io = self.file_io.as_ref().ok_or_else(|| {
            invalid("BlobFormatWriter received a BlobDescriptor but has no FileIO to resolve it")
        })?;
        let input = crate::io::uri_reader::UriInput::new(file_io, descriptor.uri())?;
        let offset = range.offset();
        let length = match range.length() {
            Some(length) => length,
            None => input
                .size()
                .await
                .map_err(|error| Error::UnexpectedError {
                    message: format!(
                        "Failed to read metadata for BlobDescriptor '{}': {error}",
                        crate::io::uri_reader::sanitize_blob_uri(descriptor.uri())
                    ),
                    source: Some(Box::new(error)),
                })?
                .saturating_sub(offset),
        };
        let end = offset
            .checked_add(length)
            .ok_or_else(|| invalid("BlobDescriptor range overflows u64"))?;
        let length_i64 = i64::try_from(length).map_err(|_| invalid("BLOB payload exceeds i64"))?;
        if length == 0 {
            return Ok(0);
        }
        let reader = input.reader_for_range(offset..end).await?;
        self.copy_payload(reader.as_ref(), offset..end, hasher, descriptor.uri())
            .await?;
        Ok(length_i64)
    }

    async fn copy_custom_reference(
        &mut self,
        factory: &dyn UriReaderFactory,
        descriptor: &BlobDescriptor,
        hasher: &mut Hasher,
    ) -> Result<i64> {
        let range = descriptor.range_spec()?;
        let reader = factory.create(descriptor.uri())?;
        let length = match range.length() {
            Some(length) => {
                let mut sources = std::mem::take(&mut self.reference_streams);
                let result = async {
                    sources
                        .prepare(reader, descriptor.uri(), range.offset())
                        .await?;
                    let result = self
                        .copy_uri_stream(sources.stream(), Some(length), hasher)
                        .await;
                    match result {
                        Ok(length) => {
                            sources.advance(length);
                            Ok(length)
                        }
                        Err(error) => {
                            let _ = sources.close().await;
                            Err(error)
                        }
                    }
                }
                .await;
                self.reference_streams = sources;
                result?
            }
            None => {
                // Unknown-length references read to EOF on their own stream;
                // do not consume or replace the cached bounded source.
                let mut stream = reader.new_input_stream(descriptor.uri()).await?;
                let result = async {
                    if range.offset() != 0 {
                        stream.seek(range.offset()).await?;
                    }
                    self.copy_uri_stream(stream.as_mut(), None, hasher).await
                }
                .await;
                let close = stream.close().await;
                let length = result?;
                close?;
                length
            }
        };
        i64::try_from(length).map_err(|_| invalid("BLOB payload exceeds i64"))
    }

    async fn copy_uri_stream(
        &mut self,
        stream: &mut dyn UriInputStream,
        length: Option<u64>,
        hasher: &mut Hasher,
    ) -> Result<u64> {
        let mut copied = 0_u64;
        loop {
            let requested = length.map_or(self.copy_buffer_size, |length| {
                (length - copied).min(self.copy_buffer_size as u64) as usize
            });
            if requested == 0 {
                return Ok(copied);
            }
            let bytes = stream.read(requested).await?;
            if bytes.len() > requested {
                return Err(invalid("URI stream returned more bytes than requested"));
            }
            if bytes.is_empty() {
                if length.is_some_and(|length| length != copied) {
                    return Err(invalid(format!(
                        "Unexpected EOF copying BLOB payload: expected {} bytes, received {copied}",
                        length.unwrap()
                    )));
                }
                return Ok(copied);
            }
            copied = copied
                .checked_add(bytes.len() as u64)
                .filter(|length| *length <= i64::MAX as u64)
                .ok_or_else(|| invalid("BLOB payload exceeds i64"))?;
            self.write_bytes(bytes, hasher).await?;
        }
    }

    async fn copy_payload(
        &mut self,
        reader: &dyn FileRead,
        range: std::ops::Range<u64>,
        hasher: &mut Hasher,
        uri: &str,
    ) -> Result<()> {
        let (offset, end) = (range.start, range.end);
        let mut position = offset;
        while position < end {
            let chunk_end = position.saturating_add(SOURCE_READ_SIZE).min(end);
            let chunk =
                reader
                    .read(position..chunk_end)
                    .await
                    .map_err(|error| Error::UnexpectedError {
                        message: format!(
                        "Failed to read BlobDescriptor '{}' range {position}..{chunk_end}: {error}",
                        crate::io::uri_reader::sanitize_blob_uri(uri)
                    ),
                        source: Some(Box::new(error)),
                    })?;
            if chunk.len() as u64 != chunk_end - position {
                return Err(invalid(format!("Failed to read BlobDescriptor '{}': short read for range {position}..{chunk_end}, expected={} bytes, actual={} bytes",
                    crate::io::uri_reader::sanitize_blob_uri(uri), chunk_end - position, chunk.len())));
            }
            for start in (0..chunk.len()).step_by(self.copy_buffer_size) {
                let end = start.saturating_add(self.copy_buffer_size).min(chunk.len());
                self.write_bytes(chunk.slice(start..end), hasher).await?;
            }
            position = chunk_end;
        }
        Ok(())
    }

    async fn write_blob_values(
        &mut self,
        values: &LargeBinaryArray,
        hasher: &mut Hasher,
    ) -> Result<(Vec<i64>, bool)> {
        let mut lengths = Vec::with_capacity(values.len());
        let mut flush = false;
        for value in values {
            lengths.push(match value {
                None => -1,
                Some(value) => {
                    let position = self.bytes_written;
                    let length = self.write_payload(value, hasher).await?;
                    flush |= self.accept(position, length)?;
                    length
                }
            });
        }
        Ok((lengths, flush))
    }

    async fn write_index(&mut self, values: &[i64], hasher: &mut Hasher) -> Result<i32> {
        let bytes = encode_delta_varints_write(values);
        let length =
            i32::try_from(bytes.len()).map_err(|_| invalid("BLOB element index exceeds i32"))?;
        self.write_bytes(Bytes::from(bytes), hasher).await?;
        Ok(length)
    }

    async fn write_collection(
        &mut self,
        values: &LargeBinaryArray,
        keys: Option<Vec<Vec<u8>>>,
    ) -> Result<()> {
        let (start, mut hasher) = self.start_record().await?;
        let (magic, version) = if keys.is_some() {
            (BLOB_MAP_MAGIC_NUMBER, BLOB_MAP_VERSION)
        } else {
            (BLOB_ARRAY_MAGIC_NUMBER, BLOB_ARRAY_VERSION)
        };
        let count =
            i32::try_from(values.len()).map_err(|_| invalid("BLOB element count exceeds i32"))?;
        let mut header = Vec::with_capacity(9);
        header.extend_from_slice(&magic.to_le_bytes());
        header.push(version);
        header.extend_from_slice(&count.to_le_bytes());
        self.write_bytes(header.into(), &mut hasher).await?;
        let key_lengths = if let Some(keys) = keys {
            let mut lengths = Vec::with_capacity(keys.len());
            for key in keys {
                lengths.push(
                    i64::try_from(key.len()).map_err(|_| invalid("BLOB MAP key exceeds i64"))?,
                );
                self.write_bytes(key.into(), &mut hasher).await?;
            }
            Some(lengths)
        } else {
            None
        };
        let (value_lengths, flush) = self.write_blob_values(values, &mut hasher).await?;
        let key_index_length = if let Some(lengths) = key_lengths {
            Some(self.write_index(&lengths, &mut hasher).await?)
        } else {
            None
        };
        let value_index_length = self.write_index(&value_lengths, &mut hasher).await?;
        if let Some(length) = key_index_length {
            self.write_bytes(Bytes::copy_from_slice(&length.to_le_bytes()), &mut hasher)
                .await?;
        }
        self.write_bytes(
            Bytes::copy_from_slice(&value_index_length.to_le_bytes()),
            &mut hasher,
        )
        .await?;
        self.finish_record(start, hasher).await?;
        if flush {
            self.writer.flush().await?;
        }
        Ok(())
    }
}

#[async_trait]
impl FormatFileWriter for BlobFormatWriter {
    async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_columns() != 1 {
            return Err(invalid("BlobFormatWriter requires exactly one column"));
        }
        let column = batch.column(0);
        let kind = self.kind.clone();
        for row in 0..batch.num_rows() {
            if column.is_null(row) {
                self.lengths.push(-1);
                if let Some(consumer) = &self.consumer {
                    // Java ignores the return value for NULL fields.
                    consumer.accept(&self.field_name, None)?;
                }
                continue;
            }
            match &kind {
                BlobFieldKind::Scalar => {
                    self.write_scalar(blob_values(column.as_ref())?.value(row))
                        .await?;
                }
                BlobFieldKind::Array => {
                    let list = column
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .ok_or_else(|| invalid("ARRAY<BLOB> writer requires ListArray"))?;
                    self.write_collection(blob_values(list.value(row).as_ref())?, None)
                        .await?;
                }
                BlobFieldKind::Map(key_type) => {
                    let map = column
                        .as_any()
                        .downcast_ref::<MapArray>()
                        .ok_or_else(|| invalid("MAP<X, BLOB> writer requires MapArray"))?;
                    let entries = map.value(row);
                    // Java validates all keys before starting the record.
                    let keys = serialize_map_keys(entries.column(0).as_ref(), key_type)?;
                    self.write_collection(blob_values(entries.column(1).as_ref())?, Some(keys))
                        .await?;
                }
            }
        }
        Ok(())
    }

    fn num_bytes(&self) -> usize {
        self.bytes_written as usize
    }
    fn in_progress_size(&self) -> usize {
        0
    }
    async fn flush(&mut self) -> Result<()> {
        self.writer.flush().await
    }
    async fn close(mut self: Box<Self>) -> Result<FormatWriteResult> {
        let footer = async {
            let index = encode_delta_varints_write(&self.lengths);
            let length =
                i32::try_from(index.len()).map_err(|_| invalid("BLOB file index exceeds i32"))?;
            self.writer.write(index.into()).await?;
            self.writer
                .write(Bytes::copy_from_slice(&length.to_le_bytes()))
                .await?;
            self.writer
                .write(Bytes::from_static(&[BLOB_FORMAT_VERSION]))
                .await?;
            Ok::<_, Error>(length)
        }
        .await;
        // Close both source and destination even if the footer or one close
        // fails, keeping the first failure primary, like Java's finally blocks.
        let source_close = self.reference_streams.close().await;
        let output_close = self.writer.close().await;
        let length = footer?;
        source_close?;
        output_close?;
        Ok(FormatWriteResult::new(
            self.bytes_written + length as u64 + 5,
        ))
    }
}

fn blob_values(array: &dyn Array) -> Result<&LargeBinaryArray> {
    array
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .ok_or_else(|| invalid("BLOB values require LargeBinaryArray"))
}

fn validate_key_type(key_type: &DataType) -> Result<()> {
    match key_type {
        DataType::Boolean(_)
        | DataType::TinyInt(_)
        | DataType::SmallInt(_)
        | DataType::Int(_)
        | DataType::BigInt(_)
        | DataType::Date(_)
        | DataType::Time(_)
        | DataType::Decimal(_)
        | DataType::Char(_)
        | DataType::VarChar(_)
        | DataType::Binary(_)
        | DataType::VarBinary(_) => Ok(()),
        other => Err(Error::Unsupported {
            message: format!("Unsupported key type for MAP<X, BLOB>: {other:?}"),
        }),
    }
}

fn serialize_map_keys(array: &dyn Array, key_type: &DataType) -> Result<Vec<Vec<u8>>> {
    let mut seen = HashSet::new();
    let mut keys = Vec::with_capacity(array.len());
    for row in 0..array.len() {
        if array.is_null(row) {
            return Err(invalid("Arrow MAP keys must not be null"));
        }
        macro_rules! fixed {
            ($array:ty) => {
                array
                    .as_any()
                    .downcast_ref::<$array>()
                    .ok_or_else(|| {
                        invalid("MAP<X, BLOB> key array does not match its declared type")
                    })?
                    .value(row)
                    .to_le_bytes()
                    .to_vec()
            };
        }
        let key = match key_type {
            DataType::TinyInt(_) => fixed!(Int8Array),
            DataType::SmallInt(_) => fixed!(Int16Array),
            DataType::Int(_) => fixed!(Int32Array),
            DataType::BigInt(_) => fixed!(Int64Array),
            DataType::Date(_) => fixed!(Date32Array),
            DataType::Time(_) => fixed!(Time32MillisecondArray),
            DataType::Boolean(_) => vec![u8::from(
                array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| invalid("MAP BOOLEAN key requires BooleanArray"))?
                    .value(row),
            )],
            DataType::Char(_) | DataType::VarChar(_) => array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| invalid("MAP string key requires StringArray"))?
                .value(row)
                .as_bytes()
                .to_vec(),
            DataType::Binary(_) | DataType::VarBinary(_) => array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| invalid("MAP binary key requires BinaryArray"))?
                .value(row)
                .to_vec(),
            DataType::Decimal(decimal) => {
                let value = array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .ok_or_else(|| invalid("MAP DECIMAL key requires Decimal128Array"))?
                    .value(row);
                if decimal.precision() <= 18 {
                    i64::try_from(value)
                        .map_err(|_| invalid("Compact MAP DECIMAL key exceeds i64"))?
                        .to_le_bytes()
                        .to_vec()
                } else {
                    // BigInteger.toByteArray(): minimal signed two's-complement, big endian.
                    let bytes = value.to_be_bytes();
                    let mut start = 0;
                    while start < 15
                        && ((bytes[start] == 0 && bytes[start + 1] < 0x80)
                            || (bytes[start] == 0xff && bytes[start + 1] >= 0x80))
                    {
                        start += 1;
                    }
                    bytes[start..].to_vec()
                }
            }
            other => {
                return Err(Error::Unsupported {
                    message: format!("Unsupported key type for MAP<X, BLOB>: {other:?}"),
                })
            }
        };
        if !seen.insert(key.clone()) {
            return Err(invalid("MAP<X, BLOB> keys must be unique"));
        }
        keys.push(key);
    }
    Ok(keys)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{
        BigIntType, BinaryType, BooleanType, CharType, DateType, DecimalType, FloatType, IntType,
        SmallIntType, TimeType, TinyIntType, VarBinaryType, VarCharType,
    };
    use arrow_array::ArrayRef;
    use std::sync::Arc;

    #[tokio::test]
    async fn production_array_records_match_java_golden_bytes_including_crc() {
        use crate::io::{FileIOBuilder, FileRead};
        use crate::spec::{ArrayType, BlobType};
        use arrow_array::builder::{LargeBinaryBuilder, ListBuilder};
        let io = FileIOBuilder::new("memory").build().unwrap();
        let output = io.new_output("memory:/array.blob").unwrap();
        let field = DataField::new(
            0,
            "payloads".into(),
            DataType::Array(ArrayType::new(DataType::Blob(BlobType::new()))),
        );
        let mut list = ListBuilder::new(LargeBinaryBuilder::new());
        list.values().append_value(b"hello");
        list.values().append_null();
        list.values().append_value(b"world");
        list.append(true);
        list.append(false);
        list.append(false);
        list.append(true);
        let batch = RecordBatch::try_from_iter([("payloads", Arc::new(list.finish()) as ArrayRef)])
            .unwrap();
        let mut writer = BlobFormatWriter::new(&output, Some(io.clone()), Some(&field))
            .await
            .unwrap();
        writer.write(&batch).await.unwrap();
        let result = Box::new(writer).close().await.unwrap();
        let written = io
            .new_input("memory:/array.blob")
            .unwrap()
            .reader()
            .await
            .unwrap()
            .read(0..result.file_size)
            .await
            .unwrap();
        let golden = include_bytes!("../../../../testdata/blob/blob-array.blob");
        fn data_end(bytes: &[u8]) -> usize {
            bytes.len()
                - 5
                - i32::from_le_bytes(bytes[bytes.len() - 5..bytes.len() - 1].try_into().unwrap())
                    as usize
        }
        // The fixture's placeholder and our second NULL have no physical
        // record; only their footer indexes differ. All physical records,
        // including framing and CRC, must be byte-identical to Java.
        assert_eq!(&written[..data_end(&written)], &golden[..data_end(golden)]);
    }

    #[test]
    fn map_keys_match_java_serializers_and_reader_for_every_supported_type() {
        let types: Vec<(DataType, ArrayRef)> = vec![
            (
                DataType::Boolean(BooleanType::new()),
                Arc::new(BooleanArray::from(vec![true, false])),
            ),
            (
                DataType::TinyInt(TinyIntType::new()),
                Arc::new(Int8Array::from(vec![i8::MIN, i8::MAX])),
            ),
            (
                DataType::SmallInt(SmallIntType::new()),
                Arc::new(Int16Array::from(vec![i16::MIN, i16::MAX])),
            ),
            (
                DataType::Int(IntType::new()),
                Arc::new(Int32Array::from(vec![i32::MIN, i32::MAX])),
            ),
            (
                DataType::BigInt(BigIntType::new()),
                Arc::new(Int64Array::from(vec![i64::MIN, i64::MAX])),
            ),
            (
                DataType::Date(DateType::new()),
                Arc::new(Date32Array::from(vec![-1, 0])),
            ),
            (
                DataType::Time(TimeType::new(3).unwrap()),
                Arc::new(Time32MillisecondArray::from(vec![0, 123])),
            ),
            (
                DataType::Char(CharType::new(10).unwrap()),
                Arc::new(StringArray::from(vec!["", "你好"])),
            ),
            (
                DataType::VarChar(VarCharType::new(10).unwrap()),
                Arc::new(StringArray::from(vec!["", "你好"])),
            ),
            (
                DataType::Binary(BinaryType::new(2).unwrap()),
                Arc::new(BinaryArray::from_iter_values([b"ab", b"cd"])),
            ),
            (
                DataType::VarBinary(VarBinaryType::new(10).unwrap()),
                Arc::new(BinaryArray::from_iter_values([b"".as_slice(), b"abc"])),
            ),
            (
                DataType::Decimal(DecimalType::new(18, 2).unwrap()),
                Arc::new(
                    Decimal128Array::from(vec![-999, 128])
                        .with_precision_and_scale(18, 2)
                        .unwrap(),
                ),
            ),
            (
                DataType::Decimal(DecimalType::new(38, 2).unwrap()),
                Arc::new(
                    Decimal128Array::from(vec![
                        0,
                        127,
                        128,
                        -128,
                        -129,
                        99999999999999999999999999999999999999,
                    ])
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
                ),
            ),
        ];
        for (data_type, array) in types {
            let keys = serialize_map_keys(array.as_ref(), &data_type).unwrap();
            let bytes: Vec<_> = keys.iter().cloned().map(Bytes::from).collect();
            let restored = super::super::decode_blob_map_keys(&bytes, &data_type).unwrap();
            assert_eq!(restored.to_data(), array.to_data(), "{data_type:?}");
            match data_type {
                DataType::Boolean(_) => assert_eq!(keys, vec![vec![1], vec![0]]),
                DataType::Int(_) => {
                    assert_eq!(keys, vec![vec![0, 0, 0, 128], vec![255, 255, 255, 127]])
                }
                DataType::Decimal(decimal) if decimal.precision() > 18 => assert_eq!(
                    &keys[..5],
                    &[vec![0], vec![127], vec![0, 128], vec![128], vec![255, 127]]
                ),
                _ => {}
            }
        }
    }

    #[test]
    fn duplicate_map_keys_are_rejected_before_the_record() {
        for keys in [
            Arc::new(StringArray::from(vec!["a", "a"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["", ""])) as ArrayRef,
        ] {
            let error = serialize_map_keys(
                keys.as_ref(),
                &DataType::VarChar(VarCharType::new(10).unwrap()),
            )
            .unwrap_err();
            assert!(error.to_string().contains("unique"));
        }
        assert!(validate_key_type(&DataType::Float(FloatType::new())).is_err());
    }
}
