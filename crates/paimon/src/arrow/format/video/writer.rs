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

//! Pack encoded videos once per physical source range and index logical frame runs.

use super::{FOOTER_SIZE, MAGIC, VERSION};
use crate::arrow::format::delta_varint::encode_delta_varints;
use crate::arrow::format::{FormatFileWriter, FormatWriteResult, FormatWriterFactory};
use crate::io::{FileIO, FileWrite, OutputFile, UriInputStream, UriReaderFactory};
use crate::spec::{BlobDescriptor, CoreOptions, DataField, DataType, VideoFrameDescriptor};
use crate::{Error, Result};
use arrow_array::{LargeBinaryArray, RecordBatch};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;

fn invalid(message: impl Into<String>) -> Error {
    Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

pub(crate) struct VideoWriterFactory {
    file_io: Option<FileIO>,
    copy_buffer_size: usize,
    uri_reader_factory: Option<Arc<dyn UriReaderFactory>>,
}

impl VideoWriterFactory {
    pub(crate) fn new(
        file_io: Option<FileIO>,
        fields: Option<&[DataField]>,
        options: Option<&HashMap<String, String>>,
    ) -> Result<Self> {
        if fields.is_some_and(|fields| {
            fields.len() != 1 || !matches!(fields[0].data_type(), DataType::Blob(_))
        }) {
            return Err(invalid("Video format only supports one scalar BLOB field"));
        }
        let defaults = HashMap::new();
        Ok(Self {
            file_io,
            copy_buffer_size: CoreOptions::new(options.unwrap_or(&defaults))
                .blob_copy_buffer_size()?,
            uri_reader_factory: None,
        })
    }

    pub(crate) fn with_uri_reader_factory(
        mut self,
        factory: Option<Arc<dyn UriReaderFactory>>,
    ) -> Self {
        self.uri_reader_factory = factory;
        self
    }
}

#[async_trait]
impl FormatWriterFactory for VideoWriterFactory {
    async fn create_writer(
        &self,
        output: &OutputFile,
        _compression: &str,
    ) -> Result<Box<dyn FormatFileWriter>> {
        Ok(Box::new(VideoFormatWriter::new(
            output.writer().await?,
            self.file_io.clone(),
            self.uri_reader_factory.clone(),
            self.copy_buffer_size,
        )?))
    }
}

struct FrameRun {
    length: i64,
    reference: i64,
    first_frame: i64,
}

pub(crate) struct VideoFormatWriter {
    writer: Box<dyn FileWrite>,
    file_io: Option<FileIO>,
    uri_reader_factory: Option<Arc<dyn UriReaderFactory>>,
    copy_buffer_size: usize,
    bytes_written: u64,
    rows: i64,
    physical_videos: HashMap<(String, i64, i64), i64>,
    physical_lengths: Vec<i64>,
    moov_offsets: Vec<i64>,
    moov_lengths: Vec<i64>,
    runs: Vec<FrameRun>,
}

impl VideoFormatWriter {
    #[cfg(feature = "internal-api")]
    pub(crate) async fn abort(mut self: Box<Self>) -> Result<()> {
        self.writer.close().await
    }

    pub(crate) fn new(
        writer: Box<dyn FileWrite>,
        file_io: Option<FileIO>,
        uri_reader_factory: Option<Arc<dyn UriReaderFactory>>,
        copy_buffer_size: usize,
    ) -> Result<Self> {
        if copy_buffer_size == 0 {
            return Err(invalid("Video copy buffer size must be positive"));
        }
        Ok(Self {
            writer,
            file_io,
            uri_reader_factory,
            copy_buffer_size,
            bytes_written: 0,
            rows: 0,
            physical_videos: HashMap::new(),
            physical_lengths: Vec::new(),
            moov_offsets: Vec::new(),
            moov_lengths: Vec::new(),
            runs: Vec::new(),
        })
    }

    fn append(&mut self, reference: i64, frame: i64) -> Result<()> {
        if self.rows == i64::from(i32::MAX) {
            return Err(invalid("Video row count exceeds i32"));
        }
        self.rows += 1;
        if let Some(run) = self.runs.last_mut() {
            if run.reference == reference
                && (reference < 0 || run.first_frame.checked_add(run.length) == Some(frame))
            {
                run.length += 1;
                return Ok(());
            }
        }
        self.runs.push(FrameRun {
            length: 1,
            reference,
            first_frame: frame,
        });
        Ok(())
    }

    async fn write_bytes(&mut self, bytes: Bytes, locator: &mut MoovLocator) -> Result<()> {
        let end = self
            .bytes_written
            .checked_add(bytes.len() as u64)
            .filter(|end| *end <= i64::MAX as u64)
            .ok_or_else(|| invalid("Video payload size exceeds i64"))?;
        locator.feed(&bytes);
        self.writer.write(bytes).await?;
        self.bytes_written = end;
        Ok(())
    }

    async fn copy_stream(
        &mut self,
        stream: &mut dyn UriInputStream,
        length: Option<u64>,
        locator: &mut MoovLocator,
    ) -> Result<()> {
        let mut copied = 0;
        loop {
            let requested = length.map_or(self.copy_buffer_size, |length| {
                (length - copied).min(self.copy_buffer_size as u64) as usize
            });
            if requested == 0 {
                return Ok(());
            }
            let bytes = stream.read(requested).await?;
            if bytes.len() > requested {
                return Err(invalid("URI stream returned more bytes than requested"));
            }
            if bytes.is_empty() {
                return if length.is_some_and(|length| length != copied) {
                    Err(invalid("Unexpected EOF copying video payload"))
                } else {
                    Ok(())
                };
            }
            copied += bytes.len() as u64;
            self.write_bytes(bytes, locator).await?;
        }
    }

    async fn copy_video(
        &mut self,
        payload: &BlobDescriptor,
        locator: &mut MoovLocator,
    ) -> Result<()> {
        let range = payload.range_spec()?;
        if let Some(factory) = &self.uri_reader_factory {
            if factory.supports_uri(payload.uri())? {
                let reader = factory.create(payload.uri())?;
                let mut stream = reader.new_input_stream(payload.uri()).await?;
                let result = async {
                    if range.offset() != 0 {
                        stream.seek(range.offset()).await?;
                    }
                    self.copy_stream(stream.as_mut(), range.length(), locator)
                        .await
                }
                .await;
                let close = stream.close().await;
                result?;
                return close;
            }
        }
        let io = self.file_io.as_ref().ok_or_else(|| {
            invalid(
                "Video writer requires FileIO or a URI reader factory to resolve frame payloads",
            )
        })?;
        let input = crate::io::uri_reader::UriInput::new(io, payload.uri())?;
        let length = match range.length() {
            Some(length) => length,
            None => input
                .size()
                .await?
                .checked_sub(range.offset())
                .ok_or_else(|| invalid("Video payload offset exceeds source size"))?,
        };
        let end = range
            .offset()
            .checked_add(length)
            .ok_or_else(|| invalid("Video payload range overflows u64"))?;
        if length == 0 {
            return Err(invalid("Encoded video payload must not be empty"));
        }
        let reader = input.reader_for_range(range.offset()..end).await?;
        let mut position = range.offset();
        while position < end {
            // Separate remote read-ahead from the configured copy buffer.
            let chunk_end = position.saturating_add(8 * 1024 * 1024).min(end);
            let chunk = reader.read(position..chunk_end).await?;
            if chunk.len() as u64 != chunk_end - position {
                return Err(invalid("Unexpected EOF copying video payload"));
            }
            for start in (0..chunk.len()).step_by(self.copy_buffer_size) {
                self.write_bytes(
                    chunk
                        .slice(start..start.saturating_add(self.copy_buffer_size).min(chunk.len())),
                    locator,
                )
                .await?;
            }
            position = chunk_end;
        }
        Ok(())
    }

    async fn write_frame(&mut self, value: &[u8]) -> Result<()> {
        // Raw bytes and ordinary BlobDescriptors must never become encoded videos.
        let frame = VideoFrameDescriptor::deserialize(value)?;
        let payload = frame.payload_descriptor();
        let key = (
            payload.uri().to_string(),
            payload.offset(),
            payload.length(),
        );
        let reference = match self.physical_videos.get(&key) {
            Some(reference) => *reference,
            None => {
                let start = self.bytes_written;
                let mut locator = MoovLocator::default();
                self.copy_video(payload, &mut locator).await?;
                let length = self.bytes_written - start;
                if length == 0 {
                    return Err(invalid("Encoded video payload must not be empty"));
                }
                let (offset, moov_length) = locator.finish();
                let reference = self.physical_lengths.len() as i64;
                self.physical_lengths.push(length as i64);
                self.moov_offsets.push(offset);
                self.moov_lengths.push(moov_length);
                self.physical_videos.insert(key, reference);
                reference
            }
        };
        self.append(reference, frame.frame_index())
    }
}

#[async_trait]
impl FormatFileWriter for VideoFormatWriter {
    async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_columns() != 1 {
            return Err(invalid("Video format only supports one scalar BLOB field"));
        }
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .ok_or_else(|| invalid("Video frame descriptors require LargeBinaryArray"))?;
        if batch.num_rows() as u64 > (i64::from(i32::MAX) - self.rows) as u64 {
            return Err(invalid("Video row count exceeds i32"));
        }
        for value in values {
            match value {
                None => self.append(-1, 0)?,
                Some(value) => self.write_frame(value).await?,
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
        let result: Result<FormatWriteResult> = async {
            let indexes = [
                encode_delta_varints(&self.physical_lengths),
                encode_delta_varints(&self.moov_offsets),
                encode_delta_varints(&self.moov_lengths),
                encode_delta_varints(&self.runs.iter().map(|run| run.length).collect::<Vec<_>>()),
                encode_delta_varints(
                    &self
                        .runs
                        .iter()
                        .map(|run| run.reference)
                        .collect::<Vec<_>>(),
                ),
                encode_delta_varints(
                    &self
                        .runs
                        .iter()
                        .map(|run| run.first_frame)
                        .collect::<Vec<_>>(),
                ),
            ];
            let mut footer = Vec::with_capacity(FOOTER_SIZE as usize);
            for index in indexes {
                let length =
                    i32::try_from(index.len()).map_err(|_| invalid("Video index exceeds i32"))?;
                footer.extend_from_slice(&length.to_le_bytes());
                self.writer.write(index.into()).await?;
                self.bytes_written += length as u64;
            }
            footer.extend_from_slice(&MAGIC.to_le_bytes());
            footer.push(VERSION);
            self.writer.write(footer.into()).await?;
            Ok(FormatWriteResult::new(self.bytes_written + FOOTER_SIZE))
        }
        .await;
        let close = self.writer.close().await;
        let result = result?;
        close?;
        Ok(result)
    }
}

/// Bounded-memory top-level MP4 box scanner. Invalid or non-MP4 streams have no hint.
struct MoovLocator {
    position: u64,
    next_box: Option<u64>,
    header: Vec<u8>,
    header_size: usize,
    found: Option<(u64, u64)>,
    valid: bool,
}

impl Default for MoovLocator {
    fn default() -> Self {
        Self {
            position: 0,
            next_box: Some(0),
            header: Vec::with_capacity(16),
            header_size: 8,
            found: None,
            valid: true,
        }
    }
}

impl MoovLocator {
    fn feed(&mut self, data: &[u8]) {
        let end = self.position + data.len() as u64;
        while self.valid {
            let Some(next) = self.next_box.filter(|next| *next < end) else {
                break;
            };
            let start = (next + self.header.len() as u64).saturating_sub(self.position) as usize;
            let count = (self.header_size - self.header.len()).min(data.len() - start);
            self.header.extend_from_slice(&data[start..start + count]);
            if self.header.len() < self.header_size {
                break;
            }
            let size = u32::from_be_bytes(self.header[..4].try_into().unwrap());
            let moov = &self.header[4..8] == b"moov";
            let size = if size == 1 {
                self.header_size = 16;
                if self.header.len() < 16 {
                    continue;
                }
                let size = u64::from_be_bytes(self.header[8..16].try_into().unwrap());
                if size == 0 {
                    self.valid = false;
                    break;
                }
                size
            } else {
                u64::from(size)
            };
            if moov {
                if self.found.is_some() {
                    self.valid = false;
                    break;
                }
                self.found = Some((next, size));
            }
            if size == 0 {
                self.next_box = None;
                break;
            }
            if size < self.header_size as u64 {
                self.valid = false;
                break;
            }
            self.next_box = next.checked_add(size);
            if self.next_box.is_none() {
                self.valid = false;
                break;
            }
            self.header.clear();
            self.header_size = 8;
        }
        self.position = end;
    }

    fn finish(self) -> (i64, i64) {
        if !self.valid
            || self
                .next_box
                .is_some_and(|next| next != self.position || !self.header.is_empty())
        {
            return (0, 0);
        }
        self.found.map_or((0, 0), |(offset, size)| {
            (
                offset as i64,
                if size == 0 {
                    self.position - offset
                } else {
                    size
                } as i64,
            )
        })
    }
}

#[cfg(test)]
mod tests;
