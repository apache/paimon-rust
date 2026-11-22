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

use super::{panic_payload_message, FilePredicates, FormatFileReader};
use crate::arrow::build_target_arrow_schema;
use crate::arrow::residual::{filter_record_batch_by_predicates, widen_scan_fields};
use crate::io::FileRead;
use crate::spec::{DataField, DataType, IntType, MapType, RowType};
use crate::table::{ArrowRecordBatchStream, RowRange};
use crate::Error;
use apache_avro::types::Value;
use apache_avro::Reader;
use arrow_array::{
    BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeListArray, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, ListArray,
    MapArray, RecordBatch, StringArray, StructArray, Time32MillisecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow_buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use bytes::{Buf, Bytes};
use futures::{stream, StreamExt, TryFutureExt};
use std::collections::VecDeque;
use std::io::Read;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex};
use tokio_util::task::AbortOnDropHandle;

/// Reads Avro object container files on demand, one block at a time.
pub(crate) struct AvroFormatReader;

const DEFAULT_BATCH_SIZE: usize = 8192;
const INITIAL_READ_AHEAD: u64 = 512 * 1024;
const MAX_READ_SIZE: u64 = 8 * 1024 * 1024;

#[cfg(test)]
mod streaming_tests;

#[async_trait]
impl FormatFileReader for AvroFormatReader {
    async fn read_batch_stream(
        &self,
        reader: Box<dyn FileRead>,
        file_size: u64,
        read_fields: &[DataField],
        predicates: Option<&FilePredicates>,
        batch_size: Option<usize>,
        row_selection: Option<Vec<RowRange>>,
    ) -> crate::Result<ArrowRecordBatchStream> {
        let scan_fields = widen_scan_fields(read_fields, predicates);
        let target_schema = build_target_arrow_schema(&scan_fields)?;
        let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_SIZE).max(1);
        let predicates = predicates.map(|fp| FilePredicates {
            predicates: fp.predicates.clone(),
            row_filter_factory: None,
            file_fields: fp.file_fields.clone(),
        });

        let context = AvroReadContext {
            fields: scan_fields,
            schema: target_schema,
            predicates,
            batch_size,
        };
        let selection = AvroRowSelection::new(row_selection);
        if selection.is_finished() {
            return Ok(stream::empty().boxed());
        }

        Ok(async move {
            let mut input = BufferedAvroInput::new(reader, file_size);
            let (header, marker) = input.read_header().await?;
            let decoder =
                run_decode_task(move || AvroBatchDecoder::new(header, selection, context)).await?;
            let scan = AvroScan {
                input,
                marker,
                decoder,
            };
            Ok(stream::try_unfold(Some(scan), AvroScan::next_batch))
        }
        .try_flatten_stream()
        .boxed())
    }
}

struct AvroScan {
    input: BufferedAvroInput,
    marker: [u8; 16],
    decoder: AvroBatchDecoder,
}

impl AvroScan {
    fn has_more_to_scan(&self) -> bool {
        !self.decoder.selection.is_finished()
            && (self.decoder.row_position < self.decoder.block_end
                || self.input.offset < self.input.file_size)
    }

    // Owning the scan here lets us drop it before yielding a terminal error
    // or the batch that exhausts the file or row selection.
    async fn next_batch(state: Option<Self>) -> crate::Result<Option<(RecordBatch, Option<Self>)>> {
        let Some(mut scan) = state else {
            return Ok(None);
        };
        while scan.has_more_to_scan() {
            if scan.decoder.row_position == scan.decoder.block_end {
                scan.read_next_selected_block().await?;
                continue;
            }

            let batch;
            (scan.decoder, batch) = run_decode_task(move || scan.decoder.decode_batch()).await?;
            if let Some(batch) = batch {
                if scan.decoder.selection.ranges.is_none() {
                    scan.input.read_ahead = MAX_READ_SIZE;
                }
                let next = scan.has_more_to_scan().then_some(scan);
                return Ok(Some((batch, next)));
            }
        }
        Ok(None)
    }

    async fn read_next_selected_block(&mut self) -> crate::Result<()> {
        let mut encoded = Vec::new();
        while self.input.offset < self.input.file_size {
            encoded.clear();
            let count = self.input.read_length(&mut encoded).await?;
            let size = self.input.read_length(&mut encoded).await?;
            let block_end = self
                .decoder
                .row_position
                .checked_add(count)
                .ok_or_else(|| avro_invalid("Avro row position overflow"))?;
            let selected = count > 0 && self.decoder.selection.overlaps_block(block_end);
            let payload_to_read = if selected {
                size
            } else {
                self.input.advance(size)?;
                0
            };
            self.input
                .read_into(payload_to_read + self.marker.len() as u64, &mut encoded)
                .await?;
            if !encoded.ends_with(&self.marker) {
                return Err(avro_invalid("Avro sync marker mismatch"));
            }
            self.decoder.block_end = block_end;
            if selected {
                *self.decoder.input.lock().unwrap() = encoded.into();
                break;
            }
            self.decoder.row_position = block_end;
        }
        Ok(())
    }
}

struct AvroReadContext {
    fields: Vec<DataField>,
    schema: SchemaRef,
    predicates: Option<FilePredicates>,
    batch_size: usize,
}

/// The Avro reader owns its synchronous input. Sharing the current bytes lets
/// the scan supply successive blocks while keeping the parsed writer schema.
struct AvroBlockInput {
    bytes: Arc<Mutex<Bytes>>,
}

impl Read for AvroBlockInput {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        let mut bytes = self.bytes.lock().unwrap();
        let count = output.len().min(bytes.len());
        bytes.copy_to_slice(&mut output[..count]);
        if bytes.is_empty() {
            *bytes = Bytes::new();
        }
        Ok(count)
    }
}

struct AvroBatchDecoder {
    reader: Reader<'static, AvroBlockInput>,
    input: Arc<Mutex<Bytes>>,
    row_position: u64,
    block_end: u64,
    selection: AvroRowSelection,
    context: AvroReadContext,
}

impl AvroBatchDecoder {
    fn new(
        header: Vec<u8>,
        selection: AvroRowSelection,
        context: AvroReadContext,
    ) -> crate::Result<Self> {
        let input = Arc::new(Mutex::new(header.into()));
        let reader = Reader::new(AvroBlockInput {
            bytes: input.clone(),
        })
        .map_err(avro_decode_error)?;
        Ok(Self {
            reader,
            input,
            row_position: 0,
            block_end: 0,
            selection,
            context,
        })
    }

    fn decode_batch(mut self) -> crate::Result<(Self, Option<RecordBatch>)> {
        let physical_rows =
            (self.block_end - self.row_position).min(self.context.batch_size as u64);
        let mut records = Vec::new();
        // Count discarded rows toward the batch limit so sparse scans stay cancellable.
        for _ in 0..physical_rows {
            if self.selection.is_finished() {
                break;
            }
            let record = self
                .reader
                .next()
                .ok_or_else(|| avro_invalid("Truncated Avro block"))?
                .map_err(avro_decode_error)?;
            if self.selection.select_row(self.row_position) {
                records.push(record);
            }
            self.row_position += 1;
        }
        let batch = if records.is_empty() {
            None
        } else {
            let context = &self.context;
            let batch = records_to_batch(&records, &context.fields, &context.schema)?;
            drop(records);
            Some(match context.predicates.as_ref() {
                Some(fp) => filter_record_batch_by_predicates(batch, fp, &context.fields)?,
                None => batch,
            })
        };
        Ok((self, batch))
    }
}

async fn run_decode_task<T: Send + 'static>(
    task: impl FnOnce() -> crate::Result<T> + Send + 'static,
) -> crate::Result<T> {
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        // Yield between physical batches even when a sparse selection produces no output.
        tokio::task::yield_now().await;
        // apache-avro's bzip2 and xz decoders can panic on corrupt payloads.
        return catch_unwind(AssertUnwindSafe(task)).map_err(|panic| Error::UnexpectedError {
            message: format!(
                "Avro decoding task panicked: {}",
                panic_payload_message(panic)
            ),
            source: None,
        })?;
    };
    // An aborted task can remain in Tokio's blocking queue. The stream owns
    // its captured input until a worker takes it.
    let pending = Arc::new(Mutex::new(Some(task)));
    let queued = Arc::downgrade(&pending);
    let handle = runtime.spawn_blocking(move || {
        let task = queued
            .upgrade()
            .and_then(|pending| pending.lock().unwrap().take())
            .ok_or_else(|| Error::UnexpectedError {
                message: "Avro decoding task was canceled".to_string(),
                source: None,
            })?;
        task()
    });
    AbortOnDropHandle::new(handle)
        .await
        .map_err(|error| Error::UnexpectedError {
            message: "Avro decoding task failed".to_string(),
            source: Some(Box::new(error)),
        })?
}

fn avro_invalid(message: &str) -> Error {
    Error::DataInvalid {
        message: message.to_string(),
        source: None,
    }
}

fn avro_decode_error(error: apache_avro::Error) -> Error {
    Error::UnexpectedError {
        message: format!("Failed to decode Avro file: {error}"),
        source: Some(Box::new(error)),
    }
}

/// Inclusive physical row ranges, consumed in ascending order.
struct AvroRowSelection {
    ranges: Option<VecDeque<RowRange>>,
}

impl AvroRowSelection {
    fn new(ranges: Option<Vec<RowRange>>) -> Self {
        Self {
            ranges: ranges.map(|ranges| {
                crate::table::merge_row_ranges(ranges)
                    .into_iter()
                    .filter_map(|range| range.intersect_inclusive(0, i64::MAX))
                    .collect()
            }),
        }
    }

    fn is_finished(&self) -> bool {
        self.ranges.as_ref().is_some_and(VecDeque::is_empty)
    }

    fn overlaps_block(&self, block_end: u64) -> bool {
        self.ranges.as_ref().is_none_or(|ranges| {
            ranges
                .front()
                .is_some_and(|range| (range.from() as u64) < block_end)
        })
    }

    fn select_row(&mut self, position: u64) -> bool {
        let Some(ranges) = &mut self.ranges else {
            return true;
        };
        let Some(range) = ranges.front() else {
            return false;
        };
        let selected = (range.from() as u64) <= position;
        if range.to() as u64 == position {
            ranges.pop_front();
        }
        selected
    }
}

/// The buffer starts at the current file offset, including after a skipped payload.
struct BufferedAvroInput {
    reader: Box<dyn FileRead>,
    file_size: u64,
    offset: u64,
    buffer: Bytes,
    read_ahead: u64,
}

impl BufferedAvroInput {
    fn new(reader: Box<dyn FileRead>, file_size: u64) -> Self {
        Self {
            reader,
            file_size,
            offset: 0,
            buffer: Bytes::new(),
            read_ahead: INITIAL_READ_AHEAD,
        }
    }

    fn ensure_available(&self, len: u64) -> crate::Result<()> {
        if len > self.file_size - self.offset {
            return Err(avro_invalid("Truncated Avro file"));
        }
        Ok(())
    }

    async fn fill(&mut self, needed: u64) -> crate::Result<()> {
        if self.buffer.is_empty() {
            self.ensure_available(1)?;
            let read_size = needed.clamp(self.read_ahead, MAX_READ_SIZE);
            let end = self.offset.saturating_add(read_size).min(self.file_size);
            self.buffer = self.reader.read(self.offset..end).await?;
            if self.buffer.len() as u64 != end - self.offset {
                return Err(avro_invalid("Short read from Avro file"));
            }
            // Give cancellation a chance between immediately ready cached reads.
            tokio::task::yield_now().await;
        }
        Ok(())
    }

    async fn read_into(&mut self, len: u64, out: &mut Vec<u8>) -> crate::Result<()> {
        self.ensure_available(len)?;
        let mut remaining = usize::try_from(len)
            .map_err(|_| avro_invalid("Avro length exceeds addressable memory"))?;
        out.try_reserve(remaining)
            .map_err(|error| Error::UnexpectedError {
                message: "Failed to allocate Avro buffer".to_string(),
                source: Some(Box::new(error)),
            })?;
        while remaining > 0 {
            self.fill(remaining as u64).await?;
            let count = remaining.min(self.buffer.len());
            out.extend_from_slice(&self.buffer[..count]);
            self.advance(count as u64)?;
            remaining -= count;
        }
        Ok(())
    }

    fn advance(&mut self, len: u64) -> crate::Result<()> {
        self.ensure_available(len)?;
        if len < self.buffer.len() as u64 {
            self.buffer.advance(len as usize);
        } else {
            // An empty slice would still own the exhausted storage window.
            self.buffer = Bytes::new();
        }
        self.offset += len;
        Ok(())
    }

    async fn read_long(&mut self, encoded: &mut Vec<u8>) -> crate::Result<i64> {
        let mut value = 0_u64;
        for shift in (0..70).step_by(7) {
            self.fill(1).await?;
            let byte = self.buffer[0];
            self.advance(1)?;
            encoded.push(byte);
            if shift == 63 && byte > 1 {
                return Err(avro_invalid("Avro long overflow"));
            }
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok((value >> 1) as i64 ^ -((value & 1) as i64));
            }
        }
        Err(avro_invalid("Invalid Avro long"))
    }

    async fn read_length(&mut self, encoded: &mut Vec<u8>) -> crate::Result<u64> {
        u64::try_from(self.read_long(encoded).await?)
            .map_err(|_| avro_invalid("Negative Avro count or length"))
    }

    async fn read_header(&mut self) -> crate::Result<(Vec<u8>, [u8; 16])> {
        let mut header = Vec::new();
        self.read_into(4, &mut header).await?;
        if header.as_slice() != b"Obj\x01" {
            return Err(avro_invalid("Invalid Avro magic bytes"));
        }
        // OCF metadata is a map<string, bytes>. Preserve its encoding, including
        // negative-count map blocks and arbitrary binary user metadata.
        loop {
            let count = self.read_long(&mut header).await?;
            if count == 0 {
                break;
            }
            let metadata_end = if count < 0 {
                let len = self.read_length(&mut header).await?;
                self.ensure_available(len)?;
                Some(self.offset + len)
            } else {
                None
            };
            for _ in 0..count.unsigned_abs() {
                for _ in 0..2 {
                    let len = self.read_length(&mut header).await?;
                    self.read_into(len, &mut header).await?;
                }
            }
            if metadata_end.is_some_and(|end| end != self.offset) {
                return Err(avro_invalid("Invalid Avro metadata block length"));
            }
        }
        self.read_into(16, &mut header).await?;
        let marker = *header.last_chunk().unwrap();
        Ok((header, marker))
    }
}

// ---------------------------------------------------------------------------
// Value access helpers — work directly with apache_avro::types::Value
// ---------------------------------------------------------------------------

/// Find the position of a named field from a populated record.
/// All records in an Avro file share the same schema, so the index is valid
/// for every record.
fn field_index(records: &[Value], name: &str) -> Option<usize> {
    // Null parent ROWs are represented by empty records while building children.
    records.iter().find_map(|record| match record {
        Value::Record(fields) => fields.iter().position(|(n, _)| n == name),
        _ => None,
    })
}

/// Look up a field by cached index, unwrapping unions.
/// If `idx` is `None` (field not in schema or empty records), returns `None` directly
/// without linear search, since all records share the same schema.
fn get_field_at(record: &Value, idx: Option<usize>) -> Option<&Value> {
    match (record, idx) {
        (Value::Record(fields), Some(i)) => fields.get(i).map(|(_, v)| v).and_then(unwrap_value),
        _ => None,
    }
}

/// Unwrap Avro union/null values, returning `None` for null.
fn unwrap_value(v: &Value) -> Option<&Value> {
    match v {
        Value::Null => None,
        Value::Union(_, inner) => unwrap_value(inner),
        other => Some(other),
    }
}

fn value_as_bool(v: &Value) -> Option<bool> {
    match v {
        Value::Boolean(b) => Some(*b),
        _ => None,
    }
}

fn value_as_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Int(n) => Some(i64::from(*n)),
        Value::Long(n) => Some(*n),
        Value::Date(n) | Value::TimeMillis(n) => Some(i64::from(*n)),
        Value::TimeMicros(n)
        | Value::TimestampMillis(n)
        | Value::TimestampMicros(n)
        | Value::TimestampNanos(n)
        | Value::LocalTimestampMillis(n)
        | Value::LocalTimestampMicros(n)
        | Value::LocalTimestampNanos(n) => Some(*n),
        _ => None,
    }
}

fn value_as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Float(f) => Some(f64::from(*f)),
        Value::Double(f) => Some(*f),
        Value::Int(n) => Some(f64::from(*n)),
        Value::Long(n) => Some(*n as f64),
        _ => None,
    }
}

fn value_as_str(v: &Value) -> Option<&str> {
    match v {
        Value::String(s) => Some(s),
        Value::Enum(_, s) => Some(s),
        _ => None,
    }
}

fn value_as_bytes(v: &Value) -> Option<&[u8]> {
    match v {
        Value::Bytes(b) | Value::Fixed(_, b) => Some(b),
        Value::String(s) => Some(s.as_bytes()),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Avro records → Arrow RecordBatch conversion
// ---------------------------------------------------------------------------

fn records_to_batch(
    records: &[Value],
    fields: &[DataField],
    schema: &SchemaRef,
) -> crate::Result<RecordBatch> {
    let num_rows = records.len();
    let mut columns: Vec<Arc<dyn arrow_array::Array>> = Vec::with_capacity(fields.len());

    for field in fields {
        let col = build_column(records, field.name(), field.data_type(), num_rows)?;
        columns.push(col);
    }

    if columns.is_empty() {
        RecordBatch::try_new_with_options(
            schema.clone(),
            columns,
            &arrow_array::RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )
    } else {
        RecordBatch::try_new(schema.clone(), columns)
    }
    .map_err(|e| Error::UnexpectedError {
        message: format!("Failed to build Avro RecordBatch: {e}"),
        source: Some(Box::new(e)),
    })
}

fn build_column(
    records: &[Value],
    name: &str,
    data_type: &DataType,
    num_rows: usize,
) -> crate::Result<Arc<dyn arrow_array::Array>> {
    // Pre-compute field position once; O(1) per-row access thereafter.
    let idx = field_index(records, name);

    Ok(match data_type {
        DataType::Boolean(_) => {
            let arr: BooleanArray = (0..num_rows)
                .map(|i| get_field_at(&records[i], idx).and_then(value_as_bool))
                .collect();
            Arc::new(arr)
        }
        DataType::TinyInt(_) => {
            let arr: Int8Array = (0..num_rows)
                .map(|i| {
                    get_field_at(&records[i], idx)
                        .and_then(value_as_i64)
                        .map(|v| v as i8)
                })
                .collect();
            Arc::new(arr)
        }
        DataType::SmallInt(_) => {
            let arr: Int16Array = (0..num_rows)
                .map(|i| {
                    get_field_at(&records[i], idx)
                        .and_then(value_as_i64)
                        .map(|v| v as i16)
                })
                .collect();
            Arc::new(arr)
        }
        DataType::Int(_) => {
            let arr: Int32Array = (0..num_rows)
                .map(|i| {
                    get_field_at(&records[i], idx)
                        .and_then(value_as_i64)
                        .map(|v| v as i32)
                })
                .collect();
            Arc::new(arr)
        }
        DataType::BigInt(_) => {
            let arr: Int64Array = (0..num_rows)
                .map(|i| get_field_at(&records[i], idx).and_then(value_as_i64))
                .collect();
            Arc::new(arr)
        }
        DataType::Float(_) => {
            let arr: Float32Array = (0..num_rows)
                .map(|i| {
                    get_field_at(&records[i], idx)
                        .and_then(value_as_f64)
                        .map(|v| v as f32)
                })
                .collect();
            Arc::new(arr)
        }
        DataType::Double(_) => {
            let arr: Float64Array = (0..num_rows)
                .map(|i| get_field_at(&records[i], idx).and_then(value_as_f64))
                .collect();
            Arc::new(arr)
        }
        DataType::Char(_) | DataType::VarChar(_) => {
            let arr: StringArray = (0..num_rows)
                .map(|i| get_field_at(&records[i], idx).and_then(value_as_str))
                .collect();
            Arc::new(arr)
        }
        DataType::Binary(_) | DataType::VarBinary(_) => {
            let values: Vec<Option<&[u8]>> = (0..num_rows)
                .map(|i| get_field_at(&records[i], idx).and_then(value_as_bytes))
                .collect();
            let arr: BinaryArray = values.into_iter().collect();
            Arc::new(arr)
        }
        DataType::Blob(_) => {
            let values: Vec<Option<&[u8]>> = (0..num_rows)
                .map(|i| get_field_at(&records[i], idx).and_then(value_as_bytes))
                .collect();
            let arr: LargeBinaryArray = values.into_iter().collect();
            Arc::new(arr)
        }
        DataType::Date(_) => {
            let arr: Date32Array = (0..num_rows)
                .map(|i| {
                    get_field_at(&records[i], idx)
                        .and_then(value_as_i64)
                        .map(|v| v as i32)
                })
                .collect();
            Arc::new(arr)
        }
        // Java writes TIME as an int with the `time-millis` logical type and
        // rejects a precision above 3, so milliseconds is the only width to
        // decode (`AvroSchemaConverter#convertToSchema`).
        DataType::Time(_) => {
            let arr: Time32MillisecondArray = (0..num_rows)
                .map(|i| {
                    get_field_at(&records[i], idx)
                        .and_then(value_as_i64)
                        .map(|v| v as i32)
                })
                .collect();
            Arc::new(arr)
        }
        DataType::Decimal(d) => {
            let precision = u8::try_from(d.precision()).map_err(|_| Error::Unsupported {
                message: "Decimal precision exceeds u8".to_string(),
            })?;
            let scale = i8::try_from(d.scale() as i32).map_err(|_| Error::Unsupported {
                message: "Decimal scale out of i8 range".to_string(),
            })?;
            let arr: Decimal128Array = (0..num_rows)
                .map(|i| {
                    get_field_at(&records[i], idx).and_then(|v| match v {
                        Value::Bytes(b) | Value::Fixed(_, b) => Some(bytes_to_i128_be(b)),
                        Value::Decimal(d) => Vec::<u8>::try_from(d.clone())
                            .ok()
                            .map(|b| bytes_to_i128_be(&b)),
                        Value::BigDecimal(bd) => parse_decimal_string(&bd.to_string(), scale),
                        _ => None,
                    })
                })
                .collect::<Decimal128Array>()
                .with_precision_and_scale(precision, scale)
                .map_err(|e| Error::UnexpectedError {
                    message: format!("Failed to build Decimal128Array: {e}"),
                    source: Some(Box::new(e)),
                })?;
            Arc::new(arr)
        }
        DataType::Timestamp(t) => {
            build_timestamp_column(records, name, num_rows, t.precision(), None)
        }
        DataType::LocalZonedTimestamp(t) => build_timestamp_column(
            records,
            name,
            num_rows,
            t.precision(),
            Some(Arc::from("UTC")),
        ),
        DataType::Array(arr_type) => {
            build_array_column(records, name, arr_type.element_type(), num_rows)?
        }
        DataType::Vector(vector_type) => build_vector_column(
            records,
            name,
            vector_type.element_type(),
            vector_type.length() as usize,
            num_rows,
        )?,
        DataType::Map(map_type) => build_map_column(records, name, map_type, num_rows)?,
        // Java encodes MULTISET<T> as a map from the element to an INT count,
        // sharing the MAP path (`AvroSchemaConverter#extractValueTypeToAvroMap`
        // returns IntType). MAP keys and MULTISET elements are non-nullable
        // in Arrow; counts are non-nullable too.
        DataType::Multiset(multiset_type) => build_map_like_column(
            records,
            name,
            multiset_type.element_type(),
            &DataType::Int(IntType::new()),
            false,
            false,
            num_rows,
        )?,
        DataType::Row(row_type) => build_row_column(records, name, row_type, num_rows)?,
        other => {
            return Err(Error::Unsupported {
                message: format!("Avro reader does not support data type: {other:?}"),
            });
        }
    })
}

fn build_timestamp_column(
    records: &[Value],
    name: &str,
    num_rows: usize,
    precision: u32,
    tz: Option<Arc<str>>,
) -> Arc<dyn arrow_array::Array> {
    let idx = field_index(records, name);
    let values: Vec<Option<i64>> = (0..num_rows)
        .map(|i| get_field_at(&records[i], idx).and_then(value_as_i64))
        .collect();
    match precision {
        0 => Arc::new(
            TimestampSecondArray::from(
                values
                    .into_iter()
                    .map(|value| value.map(|value| value.div_euclid(1_000)))
                    .collect::<Vec<_>>(),
            )
            .with_timezone_opt(tz),
        ),
        1..=3 => Arc::new(TimestampMillisecondArray::from(values).with_timezone_opt(tz)),
        4..=6 => Arc::new(TimestampMicrosecondArray::from(values).with_timezone_opt(tz)),
        _ => Arc::new(TimestampNanosecondArray::from(values).with_timezone_opt(tz)),
    }
}

fn build_array_column(
    records: &[Value],
    name: &str,
    element_type: &DataType,
    num_rows: usize,
) -> crate::Result<Arc<dyn arrow_array::Array>> {
    let arrow_element_type = crate::arrow::paimon_type_to_arrow(element_type)?;
    let arrow_element_field =
        arrow_schema::Field::new("element", arrow_element_type, element_type.is_nullable());

    let idx = field_index(records, name);
    let mut offsets = vec![0i32];
    let mut element_records: Vec<Value> = Vec::new();

    for record in records.iter().take(num_rows) {
        match get_field_at(record, idx) {
            Some(Value::Array(arr)) => {
                for elem in arr {
                    element_records
                        .push(Value::Record(vec![("element".to_string(), elem.clone())]));
                }
                offsets.push(offsets.last().unwrap() + arr.len() as i32);
            }
            _ => {
                offsets.push(*offsets.last().unwrap());
            }
        }
    }

    let element_col = build_column(
        &element_records,
        "element",
        element_type,
        element_records.len(),
    )?;

    let offsets_buf = OffsetBuffer::new(ScalarBuffer::from(offsets));
    let nulls = NullBuffer::new(BooleanBuffer::from(
        (0..num_rows)
            .map(|i| get_field_at(&records[i], idx).is_some())
            .collect::<Vec<_>>(),
    ));

    let list_arr = ListArray::try_new(
        Arc::new(arrow_element_field),
        offsets_buf,
        element_col,
        Some(nulls),
    )
    .map_err(|e| Error::UnexpectedError {
        message: format!("Failed to build ListArray: {e}"),
        source: Some(Box::new(e)),
    })?;
    Ok(Arc::new(list_arr))
}

fn build_vector_column(
    records: &[Value],
    name: &str,
    element_type: &DataType,
    length: usize,
    num_rows: usize,
) -> crate::Result<Arc<dyn arrow_array::Array>> {
    let idx = field_index(records, name);
    let mut elements = Vec::with_capacity(num_rows.saturating_mul(length));
    let mut valid = Vec::with_capacity(num_rows);
    for record in records.iter().take(num_rows) {
        match get_field_at(record, idx) {
            Some(Value::Array(values)) if values.len() == length => {
                valid.push(true);
                elements.extend(values.iter().cloned());
            }
            Some(Value::Array(values)) => {
                return Err(Error::DataInvalid {
                    message: format!(
                        "Avro vector '{name}' has {} elements, expected {length}",
                        values.len()
                    ),
                    source: None,
                });
            }
            Some(other) => {
                return Err(Error::DataInvalid {
                    message: format!("Avro vector '{name}' is not an array: {other:?}"),
                    source: None,
                });
            }
            None => {
                valid.push(false);
                elements.extend(std::iter::repeat_n(Value::Null, length));
            }
        }
    }
    let element_records = elements
        .into_iter()
        .map(|element| Value::Record(vec![("element".to_string(), element)]))
        .collect::<Vec<_>>();
    let values = build_column(
        &element_records,
        "element",
        element_type,
        element_records.len(),
    )?;
    let field = Arc::new(arrow_schema::Field::new(
        "element",
        crate::arrow::paimon_type_to_arrow(element_type)?,
        element_type.is_nullable(),
    ));
    let array = FixedSizeListArray::try_new(
        field,
        length as i32,
        values,
        Some(NullBuffer::new(BooleanBuffer::from(valid))),
    )
    .map_err(|source| Error::DataInvalid {
        message: format!("Cannot build Avro vector '{name}'"),
        source: Some(Box::new(source)),
    })?;
    Ok(Arc::new(array))
}

fn build_map_column(
    records: &[Value],
    name: &str,
    map_type: &MapType,
    num_rows: usize,
) -> crate::Result<Arc<dyn arrow_array::Array>> {
    build_map_like_column(
        records,
        name,
        map_type.key_type(),
        map_type.value_type(),
        // `paimon_type_to_arrow` declares a MAP key non-nullable and lets the
        // value follow its own type. Mirror that exactly or `RecordBatch::try_new`
        // rejects the column.
        false,
        map_type.value_type().is_nullable(),
        num_rows,
    )
}

/// Build a Map array from either Avro map encoding Paimon can produce.
///
/// Avro maps natively support string keys only, so Java writes a map with any
/// other key type as an array of `{key, value}` records instead
/// (`AvroSchemaConverter#isArrayMap`). Both shapes decode into the same Arrow
/// `Map` array here.
///
/// The key and value types are passed separately rather than taken from a
/// `MapType` so `MULTISET<T>` can reuse this with an implicit `INT` count as the
/// value. The two nullability flags must match what `paimon_type_to_arrow`
/// declares for the same type or
/// `RecordBatch::try_new` rejects the column.
fn build_map_like_column(
    records: &[Value],
    name: &str,
    key_type: &DataType,
    value_type: &DataType,
    key_nullable: bool,
    value_nullable: bool,
    num_rows: usize,
) -> crate::Result<Arc<dyn arrow_array::Array>> {
    let arrow_key_type = crate::arrow::paimon_type_to_arrow(key_type)?;
    let arrow_value_type = crate::arrow::paimon_type_to_arrow(value_type)?;

    let idx = field_index(records, name);
    let mut offsets = vec![0i32];
    let mut key_records: Vec<Value> = Vec::new();
    let mut value_records: Vec<Value> = Vec::new();

    for record in records.iter().take(num_rows) {
        match get_field_at(record, idx) {
            Some(Value::Map(map)) => {
                for (k, v) in map {
                    key_records.push(Value::Record(vec![(
                        "key".to_string(),
                        Value::String(k.clone()),
                    )]));
                    value_records.push(Value::Record(vec![("value".to_string(), v.clone())]));
                }
                offsets.push(offsets.last().unwrap() + map.len() as i32);
            }
            // Non-string key: an array of two-field records, written by Java's
            // array-map encoding. Entries missing either field are skipped so a
            // malformed row cannot shift the remaining offsets.
            Some(Value::Array(entries)) => {
                let mut kept = 0i32;
                for entry in entries {
                    let Some(Value::Record(pairs)) = unwrap_value(entry) else {
                        continue;
                    };
                    let key = pairs.iter().find(|(field, _)| field == "key");
                    let value = pairs.iter().find(|(field, _)| field == "value");
                    if let (Some((_, k)), Some((_, v))) = (key, value) {
                        key_records.push(Value::Record(vec![("key".to_string(), k.clone())]));
                        value_records.push(Value::Record(vec![("value".to_string(), v.clone())]));
                        kept += 1;
                    }
                }
                offsets.push(offsets.last().unwrap() + kept);
            }
            _ => {
                offsets.push(*offsets.last().unwrap());
            }
        }
    }

    let key_col = build_column(&key_records, "key", key_type, key_records.len())?;
    let value_col = build_column(&value_records, "value", value_type, value_records.len())?;

    let struct_arr = StructArray::try_new(
        vec![
            Arc::new(arrow_schema::Field::new(
                "key",
                arrow_key_type,
                key_nullable,
            )),
            Arc::new(arrow_schema::Field::new(
                "value",
                arrow_value_type.clone(),
                value_nullable,
            )),
        ]
        .into(),
        vec![key_col, value_col],
        None,
    )
    .map_err(|e| Error::UnexpectedError {
        message: format!("Failed to build map StructArray: {e}"),
        source: Some(Box::new(e)),
    })?;

    let entries_field = arrow_schema::Field::new(
        "entries",
        arrow_schema::DataType::Struct(struct_arr.fields().clone()),
        false,
    );

    let offsets_buf = OffsetBuffer::new(ScalarBuffer::from(offsets));
    let nulls = NullBuffer::new(BooleanBuffer::from(
        (0..num_rows)
            .map(|i| get_field_at(&records[i], idx).is_some())
            .collect::<Vec<_>>(),
    ));

    let map_arr = MapArray::try_new(
        Arc::new(entries_field),
        offsets_buf,
        struct_arr,
        Some(nulls),
        false,
    )
    .map_err(|e| Error::UnexpectedError {
        message: format!("Failed to build MapArray: {e}"),
        source: Some(Box::new(e)),
    })?;
    Ok(Arc::new(map_arr))
}

fn build_row_column(
    records: &[Value],
    name: &str,
    row_type: &RowType,
    num_rows: usize,
) -> crate::Result<Arc<dyn arrow_array::Array>> {
    let idx = field_index(records, name);
    let sub_records: Vec<Value> = (0..num_rows)
        .map(|i| match get_field_at(&records[i], idx) {
            Some(v @ Value::Record(_)) => v.clone(),
            _ => Value::Record(vec![]),
        })
        .collect();

    let mut child_columns: Vec<Arc<dyn arrow_array::Array>> = Vec::new();
    let mut arrow_fields: Vec<Arc<arrow_schema::Field>> = Vec::new();

    for field in row_type.fields() {
        let col = build_column(&sub_records, field.name(), field.data_type(), num_rows)?;
        let arrow_type = crate::arrow::paimon_type_to_arrow(field.data_type())?;
        arrow_fields.push(Arc::new(arrow_schema::Field::new(
            field.name(),
            arrow_type,
            field.data_type().is_nullable(),
        )));
        child_columns.push(col);
    }

    let nulls = NullBuffer::new(BooleanBuffer::from(
        (0..num_rows)
            .map(|i| get_field_at(&records[i], idx).is_some())
            .collect::<Vec<_>>(),
    ));

    let struct_arr = StructArray::try_new(arrow_fields.into(), child_columns, Some(nulls))
        .map_err(|e| Error::UnexpectedError {
            message: format!("Failed to build StructArray: {e}"),
            source: Some(Box::new(e)),
        })?;
    Ok(Arc::new(struct_arr))
}

/// Parse a decimal string (e.g. "999.99") into unscaled i128 with the given scale.
/// For example, "999.99" with scale=2 → 99999; "0.000000000000000001" with scale=18 → 1.
fn parse_decimal_string(s: &str, scale: i8) -> Option<i128> {
    let negative = s.starts_with('-');
    let s = s.strip_prefix('-').unwrap_or(s);
    let (integer_part, frac_part) = match s.find('.') {
        Some(pos) => (&s[..pos], &s[pos + 1..]),
        None => (s, ""),
    };
    let frac_len = frac_part.len() as i8;
    let combined = format!("{integer_part}{frac_part}");
    let unscaled: i128 = combined.parse().ok()?;
    // Adjust if the fractional digits differ from the target scale.
    let result = if frac_len < scale {
        unscaled * 10i128.pow((scale - frac_len) as u32)
    } else if frac_len > scale {
        unscaled / 10i128.pow((frac_len - scale) as u32)
    } else {
        unscaled
    };
    Some(if negative { -result } else { result })
}

/// Decode big-endian two's complement bytes into i128 (Avro decimal encoding).
fn bytes_to_i128_be(bytes: &[u8]) -> i128 {
    if bytes.is_empty() {
        return 0;
    }
    // Sign-extend: if the high bit is set, fill with 0xFF, otherwise 0x00.
    let sign_byte = if bytes[0] & 0x80 != 0 { 0xFF } else { 0x00 };
    let mut buf = [sign_byte; 16];
    let start = 16 - bytes.len();
    buf[start..].copy_from_slice(bytes);
    i128::from_be_bytes(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{
        BigIntType, BlobType, BooleanType, DataField, DataType, DecimalType, DoubleType, FloatType,
        IntType, MultisetType, SmallIntType, TimeType, TinyIntType, VarBinaryType, VarCharType,
    };
    use arrow_array::Array;

    // Helper to build Value variants concisely in tests.
    fn av_int(v: i64) -> Value {
        Value::Long(v)
    }
    fn av_float(v: f64) -> Value {
        Value::Double(v)
    }
    fn av_bool(v: bool) -> Value {
        Value::Boolean(v)
    }
    fn av_str(v: &str) -> Value {
        Value::String(v.to_string())
    }
    fn av_bytes(v: &[u8]) -> Value {
        Value::Bytes(v.to_vec())
    }
    fn av_null() -> Value {
        Value::Null
    }
    fn av_union(val: Value) -> Value {
        Value::Union(1, Box::new(val))
    }

    // -----------------------------------------------------------------------
    // unwrap_value
    // -----------------------------------------------------------------------

    #[test]
    fn first_null_nested_row_does_not_hide_later_children() {
        let typ = DataType::Row(RowType::new(vec![DataField::new(
            1,
            "keep".into(),
            DataType::Int(IntType::new()),
        )]));
        let records = vec![
            Value::Record(vec![("profile".into(), Value::Null)]),
            Value::Record(vec![(
                "profile".into(),
                Value::Record(vec![("keep".into(), Value::Int(7))]),
            )]),
        ];
        let array = build_column(&records, "profile", &typ, records.len()).unwrap();
        let rows = array.as_any().downcast_ref::<StructArray>().unwrap();
        assert!(rows.is_null(0));
        let child = rows
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(child.is_valid(1));
        assert_eq!(child.value(1), 7);
    }

    #[test]
    fn test_unwrap_value_null() {
        assert!(unwrap_value(&av_null()).is_none());
    }

    #[test]
    fn test_unwrap_value_plain() {
        let v = av_int(42);
        assert_eq!(unwrap_value(&v), Some(&Value::Long(42)));
    }

    #[test]
    fn test_unwrap_value_union() {
        let v = av_union(av_int(42));
        assert_eq!(unwrap_value(&v), Some(&Value::Long(42)));
    }

    #[test]
    fn test_unwrap_value_union_null() {
        let v = Value::Union(0, Box::new(Value::Null));
        assert!(unwrap_value(&v).is_none());
    }

    // -----------------------------------------------------------------------
    // get_field_at
    // -----------------------------------------------------------------------

    fn make_record(fields: Vec<(&str, Value)>) -> Value {
        Value::Record(
            fields
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        )
    }

    #[test]
    fn test_get_field_at_present() {
        let record = make_record(vec![("name", av_str("alice"))]);
        assert_eq!(
            get_field_at(&record, Some(0)),
            Some(&Value::String("alice".to_string()))
        );
    }

    #[test]
    fn test_get_field_at_missing() {
        let record = make_record(vec![]);
        assert!(get_field_at(&record, None).is_none());
    }

    #[test]
    fn test_get_field_at_union_wrapped() {
        let record = make_record(vec![("age", av_union(av_int(30)))]);
        assert_eq!(get_field_at(&record, Some(0)), Some(&Value::Long(30)));
    }

    #[test]
    fn test_get_field_at_no_index() {
        let record = make_record(vec![("x", av_int(1)), ("y", av_int(2))]);
        // idx is None — returns None directly without linear search.
        assert!(get_field_at(&record, None).is_none());
    }

    // -----------------------------------------------------------------------
    // build_column + records_to_batch
    // -----------------------------------------------------------------------

    fn make_records(rows: Vec<Vec<(&str, Value)>>) -> Vec<Value> {
        rows.into_iter()
            .map(|fields| {
                Value::Record(
                    fields
                        .into_iter()
                        .map(|(k, v)| (k.to_string(), v))
                        .collect(),
                )
            })
            .collect()
    }

    #[test]
    fn test_build_column_int() {
        let records = make_records(vec![
            vec![("x", av_int(1))],
            vec![("x", av_int(2))],
            vec![("x", av_int(3))],
        ]);
        let col = build_column(&records, "x", &DataType::Int(IntType::new()), 3).unwrap();
        let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(arr.values().as_ref(), &[1, 2, 3]);
    }

    #[test]
    fn test_build_column_bigint() {
        let records = make_records(vec![vec![("v", av_int(100))], vec![("v", av_int(200))]]);
        let col = build_column(&records, "v", &DataType::BigInt(BigIntType::new()), 2).unwrap();
        let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.values().as_ref(), &[100, 200]);
    }

    #[test]
    fn test_build_column_boolean() {
        let records = make_records(vec![
            vec![("b", av_bool(true))],
            vec![("b", av_bool(false))],
        ]);
        let col = build_column(&records, "b", &DataType::Boolean(BooleanType::new()), 2).unwrap();
        let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(arr.value(0));
        assert!(!arr.value(1));
    }

    #[test]
    fn test_build_column_string() {
        let records = make_records(vec![
            vec![("s", av_str("hello"))],
            vec![("s", av_str("world"))],
        ]);
        let col = build_column(
            &records,
            "s",
            &DataType::VarChar(VarCharType::new(100).unwrap()),
            2,
        )
        .unwrap();
        let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.value(0), "hello");
        assert_eq!(arr.value(1), "world");
    }

    #[test]
    fn test_build_column_float_double() {
        let records = make_records(vec![vec![("f", av_float(1.5)), ("d", av_float(2.5))]]);
        let fcol = build_column(&records, "f", &DataType::Float(FloatType::new()), 1).unwrap();
        let dcol = build_column(&records, "d", &DataType::Double(DoubleType::new()), 1).unwrap();
        let farr = fcol.as_any().downcast_ref::<Float32Array>().unwrap();
        let darr = dcol.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((farr.value(0) - 1.5_f32).abs() < f32::EPSILON);
        assert!((darr.value(0) - 2.5_f64).abs() < f64::EPSILON);
    }

    #[test]
    fn test_build_column_with_nulls() {
        let records = make_records(vec![
            vec![("x", av_int(10))],
            vec![("x", av_null())],
            vec![("x", av_int(30))],
        ]);
        let col = build_column(&records, "x", &DataType::Int(IntType::new()), 3).unwrap();
        let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
        assert!(!arr.is_null(0));
        assert!(arr.is_null(1));
        assert!(!arr.is_null(2));
        assert_eq!(arr.value(0), 10);
        assert_eq!(arr.value(2), 30);
    }

    #[test]
    fn test_build_column_tinyint_smallint() {
        let records = make_records(vec![vec![("t", av_int(7)), ("s", av_int(300))]]);
        let tcol = build_column(&records, "t", &DataType::TinyInt(TinyIntType::new()), 1).unwrap();
        let scol =
            build_column(&records, "s", &DataType::SmallInt(SmallIntType::new()), 1).unwrap();
        let tarr = tcol.as_any().downcast_ref::<Int8Array>().unwrap();
        let sarr = scol.as_any().downcast_ref::<Int16Array>().unwrap();
        assert_eq!(tarr.value(0), 7);
        assert_eq!(sarr.value(0), 300);
    }

    #[test]
    fn test_build_column_binary() {
        let records = make_records(vec![
            vec![("b", av_bytes(&[0xDE, 0xAD]))],
            vec![("b", av_bytes(&[0xBE, 0xEF]))],
        ]);
        let col = build_column(
            &records,
            "b",
            &DataType::VarBinary(VarBinaryType::new(100).unwrap()),
            2,
        )
        .unwrap();
        let arr = col.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(arr.value(0), &[0xDE, 0xAD]);
        assert_eq!(arr.value(1), &[0xBE, 0xEF]);
    }

    #[test]
    fn test_build_column_decimal_from_bytes() {
        // 12345 in big-endian two's complement = [0x30, 0x39]
        let records = make_records(vec![
            vec![("d", av_bytes(&[0x30, 0x39]))],
            vec![("d", av_bytes(&[0x01, 0xA4]))], // 420
        ]);
        let col = build_column(
            &records,
            "d",
            &DataType::Decimal(DecimalType::new(10, 2).unwrap()),
            2,
        )
        .unwrap();
        let arr = col.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(arr.value(0), 12345); // 123.45 * 100
        assert_eq!(arr.value(1), 420); // 4.20 * 100
    }

    #[test]
    fn test_bytes_to_i128_be() {
        assert_eq!(bytes_to_i128_be(&[0x30, 0x39]), 12345);
        assert_eq!(bytes_to_i128_be(&[0xFF]), -1);
        assert_eq!(bytes_to_i128_be(&[]), 0);
        assert_eq!(bytes_to_i128_be(&[0x00, 0x01]), 1);
    }

    #[test]
    fn test_build_column_map_with_union() {
        use std::collections::HashMap;
        // Map field wrapped in a union (nullable map), as Avro encodes nullable types.
        let mut map1 = HashMap::new();
        map1.insert("k1".to_string(), av_int(10));
        map1.insert("k2".to_string(), av_int(20));
        let records = vec![
            Value::Record(vec![(
                "m".to_string(),
                Value::Union(1, Box::new(Value::Map(map1))),
            )]),
            Value::Record(vec![(
                "m".to_string(),
                Value::Union(0, Box::new(av_null())),
            )]),
        ];
        let map_type = MapType::new(
            DataType::VarChar(VarCharType::new(100).unwrap()),
            DataType::Int(IntType::new()),
        );
        let col = build_map_column(&records, "m", &map_type, 2).unwrap();
        let arr = col.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(arr.len(), 2);
        // First row has 2 entries, second row is null.
        assert!(!arr.is_null(0));
        assert!(arr.is_null(1));
    }

    /// Java writes a map with a non-string key as an array of `{key, value}`
    /// records. Before this was handled, such a row decoded to a non-null map
    /// with zero entries -- the data was dropped silently.
    #[test]
    fn test_build_column_map_with_non_string_key() {
        let entry = |k: i32, v: &str| {
            Value::Record(vec![
                ("key".to_string(), Value::Int(k)),
                ("value".to_string(), av_str(v)),
            ])
        };
        let records = vec![
            Value::Record(vec![(
                "m".to_string(),
                Value::Array(vec![entry(1, "a"), entry(2, "b")]),
            )]),
            Value::Record(vec![("m".to_string(), Value::Array(vec![entry(7, "c")]))]),
        ];
        let map_type = MapType::new(
            DataType::Int(IntType::new()),
            DataType::VarChar(VarCharType::new(10).unwrap()),
        );
        let col = build_map_column(&records, "m", &map_type, 2).unwrap();
        let arr = col.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr.value(0).len(), 2, "entries must not be dropped");
        assert_eq!(arr.value(1).len(), 1);

        let keys = arr.keys().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(keys.values(), &[1, 2, 7]);
        let values = arr.values().as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(values.value(0), "a");
        assert_eq!(values.value(2), "c");
    }

    /// A malformed array-map entry is skipped without shifting the offsets of
    /// the rows that follow.
    #[test]
    fn test_build_column_array_map_skips_malformed_entries() {
        let records = vec![
            Value::Record(vec![(
                "m".to_string(),
                Value::Array(vec![
                    Value::Record(vec![
                        ("key".to_string(), Value::Int(1)),
                        ("value".to_string(), av_str("a")),
                    ]),
                    // Missing "value" -- skipped.
                    Value::Record(vec![("key".to_string(), Value::Int(2))]),
                    // Not a record at all -- skipped.
                    Value::Int(9),
                ]),
            )]),
            Value::Record(vec![(
                "m".to_string(),
                Value::Array(vec![Value::Record(vec![
                    ("key".to_string(), Value::Int(3)),
                    ("value".to_string(), av_str("c")),
                ])]),
            )]),
        ];
        let map_type = MapType::new(
            DataType::Int(IntType::new()),
            DataType::VarChar(VarCharType::new(10).unwrap()),
        );
        let col = build_map_column(&records, "m", &map_type, 2).unwrap();
        let arr = col.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(arr.value(0).len(), 1);
        assert_eq!(arr.value(1).len(), 1);
        let keys = arr.keys().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(keys.values(), &[1, 3]);
    }

    /// TIME arrives as an int with the `time-millis` logical type.
    #[test]
    fn test_build_column_time_millis() {
        let records = vec![
            Value::Record(vec![("t".to_string(), Value::TimeMillis(3_661_000))]),
            Value::Record(vec![("t".to_string(), av_null())]),
            // Plain ints appear when the writer omits the logical type.
            Value::Record(vec![("t".to_string(), Value::Int(1))]),
        ];
        let col =
            build_column(&records, "t", &DataType::Time(TimeType::new(3).unwrap()), 3).unwrap();
        let arr = col
            .as_any()
            .downcast_ref::<Time32MillisecondArray>()
            .unwrap();
        assert_eq!(arr.value(0), 3_661_000);
        assert!(arr.is_null(1));
        assert_eq!(arr.value(2), 1);
    }

    /// BLOB maps to Avro bytes, exactly like BINARY and VARBINARY.
    #[test]
    fn test_build_column_blob() {
        let records = make_records(vec![
            vec![("b", av_bytes(&[0xDE, 0xAD]))],
            vec![("b", av_null())],
        ]);
        let col = build_column(&records, "b", &DataType::Blob(BlobType::new()), 2).unwrap();
        let arr = col.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        assert_eq!(arr.value(0), &[0xDE, 0xAD]);
        assert!(arr.is_null(1));
    }

    /// MULTISET<T> is an Avro map from the element to an INT count. A string
    /// element uses the native map encoding.
    #[test]
    fn test_build_column_multiset_string_element() {
        use std::collections::HashMap;
        let mut counts = HashMap::new();
        counts.insert("a".to_string(), Value::Int(2));
        let records = vec![Value::Record(vec![("ms".to_string(), Value::Map(counts))])];
        let multiset = DataType::Multiset(MultisetType::new(DataType::VarChar(
            VarCharType::new(10).unwrap(),
        )));
        let col = build_column(&records, "ms", &multiset, 1).unwrap();
        let arr = col.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(arr.value(0).len(), 1);
        let values = arr.values().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(values.value(0), 2);
    }

    /// A non-string element makes Java fall back to the array-map encoding here
    /// too, so the count still has to survive.
    #[test]
    fn test_build_column_multiset_int_element() {
        let records = vec![Value::Record(vec![(
            "ms".to_string(),
            Value::Array(vec![Value::Record(vec![
                ("key".to_string(), Value::Int(5)),
                ("value".to_string(), Value::Int(3)),
            ])]),
        )])];
        let multiset = DataType::Multiset(MultisetType::new(DataType::Int(IntType::new())));
        let col = build_column(&records, "ms", &multiset, 1).unwrap();
        let arr = col.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(arr.value(0).len(), 1);
        let keys = arr.keys().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(keys.value(0), 5);
        let values = arr.values().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(values.value(0), 3);
    }

    /// Going through `build_target_arrow_schema` is what catches a field-level
    /// nullability mismatch: `RecordBatch::try_new` rejects a column whose
    /// declared schema differs from the target, which a bare `build_column`
    /// assertion would not notice.
    #[test]
    fn test_records_to_batch_with_time_blob_and_multiset() {
        let fields = vec![
            DataField::new(
                0,
                "t".to_string(),
                DataType::Time(TimeType::new(3).unwrap()),
            ),
            DataField::new(1, "b".to_string(), DataType::Blob(BlobType::new())),
            DataField::new(
                2,
                "ms".to_string(),
                DataType::Multiset(MultisetType::new(DataType::Int(IntType::new()))),
            ),
        ];
        let schema = crate::arrow::build_target_arrow_schema(&fields).unwrap();
        let records = vec![Value::Record(vec![
            ("t".to_string(), Value::TimeMillis(120_000)),
            ("b".to_string(), av_bytes(&[0x01])),
            (
                "ms".to_string(),
                Value::Array(vec![Value::Record(vec![
                    ("key".to_string(), Value::Int(4)),
                    ("value".to_string(), Value::Int(1)),
                ])]),
            ),
        ])];
        let batch = records_to_batch(&records, &fields, &schema).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 3);
    }

    /// MAP and MULTISET declare key nullability differently, so pin MAP through
    /// the schema path as well.
    #[test]
    fn test_records_to_batch_with_non_string_key_map() {
        let fields = vec![DataField::new(
            0,
            "m".to_string(),
            DataType::Map(MapType::new(
                DataType::Int(IntType::new()),
                DataType::VarChar(VarCharType::new(10).unwrap()),
            )),
        )];
        let schema = crate::arrow::build_target_arrow_schema(&fields).unwrap();
        let records = vec![Value::Record(vec![(
            "m".to_string(),
            Value::Array(vec![Value::Record(vec![
                ("key".to_string(), Value::Int(8)),
                ("value".to_string(), av_str("x")),
            ])]),
        )])];
        let batch = records_to_batch(&records, &fields, &schema).unwrap();
        let arr = batch.column(0).as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(arr.value(0).len(), 1);
    }

    #[test]
    fn test_records_to_batch_basic() {
        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "name".to_string(),
                DataType::VarChar(VarCharType::new(50).unwrap()),
            ),
        ];
        let schema = crate::arrow::build_target_arrow_schema(&fields).unwrap();
        let records = make_records(vec![
            vec![("id", av_int(1)), ("name", av_str("alice"))],
            vec![("id", av_int(2)), ("name", av_str("bob"))],
        ]);
        let batch = records_to_batch(&records, &fields, &schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_records_to_batch_empty() {
        let fields = vec![DataField::new(
            0,
            "id".to_string(),
            DataType::Int(IntType::new()),
        )];
        let schema = crate::arrow::build_target_arrow_schema(&fields).unwrap();
        let records: Vec<Value> = vec![];
        let batch = records_to_batch(&records, &fields, &schema).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }
}
