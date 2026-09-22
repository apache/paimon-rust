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

//! Java-compatible `range-bitmap` file index.
//!
//! The index maps ordered dictionary codes to row positions with a bit-sliced
//! bitmap. Evaluating it produces the same conservative row selection consumed
//! by the Parquet and Mosaic readers, allowing Mosaic to skip unselected row
//! groups before decoding them.

use std::cmp::Ordering;
use std::io::Cursor;

use bytes::Bytes;
use roaring::RoaringBitmap;

use crate::file_index::file_index_reader::FileIndexReader;
use crate::file_index::file_index_result::FileIndexResult;
use crate::spec::{DataType, Datum, PredicateOperator};
use crate::{Error, Result};

pub(crate) mod writer;

const VERSION_1: u8 = 1;
const JAVA_CANONICAL_FLOAT_NAN_BITS: u32 = 0x7fc0_0000;
const JAVA_CANONICAL_DOUBLE_NAN_BITS: u64 = 0x7ff8_0000_0000_0000;

fn format_invalid(message: impl Into<String>) -> Error {
    Error::FileIndexFormatInvalid {
        message: message.into(),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct JavaFloat(u32);

impl JavaFloat {
    fn new(value: f32) -> Self {
        Self(if value.is_nan() {
            JAVA_CANONICAL_FLOAT_NAN_BITS
        } else {
            value.to_bits()
        })
    }

    fn value(self) -> f32 {
        f32::from_bits(self.0)
    }
}

impl PartialOrd for JavaFloat {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for JavaFloat {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value().total_cmp(&other.value())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct JavaDouble(u64);

impl JavaDouble {
    fn new(value: f64) -> Self {
        Self(if value.is_nan() {
            JAVA_CANONICAL_DOUBLE_NAN_BITS
        } else {
            value.to_bits()
        })
    }

    fn value(self) -> f64 {
        f64::from_bits(self.0)
    }
}

impl PartialOrd for JavaDouble {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for JavaDouble {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value().total_cmp(&other.value())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum RangeValue {
    Boolean(bool),
    TinyInt(i8),
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Float(JavaFloat),
    Double(JavaDouble),
    Decimal(i64),
    Date(i32),
    Time(i32),
    Timestamp(i64),
    LocalZonedTimestamp(i64),
    String(String),
}

impl RangeValue {
    fn is_nan(&self) -> bool {
        match self {
            Self::Float(value) => value.value().is_nan(),
            Self::Double(value) => value.value().is_nan(),
            _ => false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RangeValueCodec {
    Boolean,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Float,
    Double,
    Decimal { scale: u32 },
    Date,
    Time,
    TimestampMillis,
    TimestampMicros,
    LocalZonedTimestampMillis,
    LocalZonedTimestampMicros,
    String,
}

impl RangeValueCodec {
    fn try_new(data_type: &DataType) -> Result<Self> {
        match data_type {
            DataType::Boolean(_) => Ok(Self::Boolean),
            DataType::TinyInt(_) => Ok(Self::TinyInt),
            DataType::SmallInt(_) => Ok(Self::SmallInt),
            DataType::Int(_) => Ok(Self::Int),
            DataType::BigInt(_) => Ok(Self::BigInt),
            DataType::Float(_) => Ok(Self::Float),
            DataType::Double(_) => Ok(Self::Double),
            DataType::Decimal(decimal) if decimal.precision() <= 18 => Ok(Self::Decimal {
                scale: decimal.scale(),
            }),
            DataType::Date(_) => Ok(Self::Date),
            DataType::Time(_) => Ok(Self::Time),
            DataType::Timestamp(timestamp) if timestamp.precision() <= 3 => {
                Ok(Self::TimestampMillis)
            }
            DataType::Timestamp(timestamp) if timestamp.precision() <= 6 => {
                Ok(Self::TimestampMicros)
            }
            DataType::LocalZonedTimestamp(timestamp) if timestamp.precision() <= 3 => {
                Ok(Self::LocalZonedTimestampMillis)
            }
            DataType::LocalZonedTimestamp(timestamp) if timestamp.precision() <= 6 => {
                Ok(Self::LocalZonedTimestampMicros)
            }
            DataType::Char(_) | DataType::VarChar(_) => Ok(Self::String),
            _ => Err(Error::Unsupported {
                message: format!(
                    "Range bitmap file index does not support data type {data_type:?}"
                ),
            }),
        }
    }

    fn fixed_length(self) -> Option<usize> {
        match self {
            Self::Boolean | Self::TinyInt => Some(1),
            Self::SmallInt => Some(2),
            Self::Int | Self::Float | Self::Date | Self::Time => Some(4),
            Self::BigInt
            | Self::Double
            | Self::Decimal { .. }
            | Self::TimestampMillis
            | Self::TimestampMicros
            | Self::LocalZonedTimestampMillis
            | Self::LocalZonedTimestampMicros => Some(8),
            Self::String => None,
        }
    }

    fn read_value(self, input: &mut Decoder<'_>) -> Result<RangeValue> {
        Ok(match self {
            Self::Boolean => RangeValue::Boolean(input.read_u8("boolean value")? == 1),
            Self::TinyInt => RangeValue::TinyInt(input.read_u8("tinyint value")? as i8),
            Self::SmallInt => RangeValue::SmallInt(input.read_i16("smallint value")?),
            Self::Int => RangeValue::Int(input.read_i32("int value")?),
            Self::BigInt => RangeValue::BigInt(input.read_i64("bigint value")?),
            Self::Float => RangeValue::Float(JavaFloat::new(input.read_f32("float value")?)),
            Self::Double => RangeValue::Double(JavaDouble::new(input.read_f64("double value")?)),
            Self::Decimal { .. } => RangeValue::Decimal(input.read_i64("decimal value")?),
            Self::Date => RangeValue::Date(input.read_i32("date value")?),
            Self::Time => RangeValue::Time(input.read_i32("time value")?),
            Self::TimestampMillis | Self::TimestampMicros => {
                RangeValue::Timestamp(input.read_i64("timestamp value")?)
            }
            Self::LocalZonedTimestampMillis | Self::LocalZonedTimestampMicros => {
                RangeValue::LocalZonedTimestamp(input.read_i64("local zoned timestamp value")?)
            }
            Self::String => {
                let length = input.read_count("string value length")?;
                let bytes = input.read_exact(length, "string value")?;
                RangeValue::String(
                    std::str::from_utf8(bytes)
                        .map_err(|error| {
                            format_invalid(format!(
                                "invalid UTF-8 range bitmap string value: {error}"
                            ))
                        })?
                        .to_string(),
                )
            }
        })
    }

    fn value(self, datum: &Datum) -> Result<RangeValue> {
        match (self, datum) {
            (Self::Boolean, Datum::Bool(value)) => Ok(RangeValue::Boolean(*value)),
            (Self::TinyInt, Datum::TinyInt(value)) => Ok(RangeValue::TinyInt(*value)),
            (Self::SmallInt, Datum::SmallInt(value)) => Ok(RangeValue::SmallInt(*value)),
            (Self::Int, Datum::Int(value)) => Ok(RangeValue::Int(*value)),
            (Self::BigInt, Datum::Long(value)) => Ok(RangeValue::BigInt(*value)),
            (Self::Float, Datum::Float(value)) => Ok(RangeValue::Float(JavaFloat::new(*value))),
            (Self::Double, Datum::Double(value)) => Ok(RangeValue::Double(JavaDouble::new(*value))),
            (
                Self::Decimal { scale },
                Datum::Decimal {
                    unscaled,
                    scale: datum_scale,
                    ..
                },
            ) if scale == *datum_scale => i64::try_from(*unscaled)
                .map(RangeValue::Decimal)
                .map_err(|_| Error::DataInvalid {
                    message: format!("Decimal unscaled value does not fit i64: {unscaled}"),
                    source: None,
                }),
            (Self::Date, Datum::Date(value)) => Ok(RangeValue::Date(*value)),
            (Self::Time, Datum::Time(value)) => Ok(RangeValue::Time(*value)),
            (Self::TimestampMillis, Datum::Timestamp { millis, nanos }) => {
                Ok(RangeValue::Timestamp(timestamp_millis(*millis, *nanos)?))
            }
            (Self::TimestampMicros, Datum::Timestamp { millis, nanos }) => {
                Ok(RangeValue::Timestamp(timestamp_micros(*millis, *nanos)?))
            }
            (Self::LocalZonedTimestampMillis, Datum::LocalZonedTimestamp { millis, nanos }) => Ok(
                RangeValue::LocalZonedTimestamp(timestamp_millis(*millis, *nanos)?),
            ),
            (Self::LocalZonedTimestampMicros, Datum::LocalZonedTimestamp { millis, nanos }) => Ok(
                RangeValue::LocalZonedTimestamp(timestamp_micros(*millis, *nanos)?),
            ),
            (Self::String, Datum::String(value)) => Ok(RangeValue::String(value.clone())),
            _ => Err(Error::DataInvalid {
                message: format!("Datum {datum:?} does not match range bitmap codec {self:?}"),
                source: None,
            }),
        }
    }
}

fn validate_nanos(nanos: i32) -> Result<()> {
    if (0..=999_999).contains(&nanos) {
        Ok(())
    } else {
        Err(Error::DataInvalid {
            message: format!("Timestamp nanos-of-millisecond is out of range: {nanos}"),
            source: None,
        })
    }
}

// Truncating a predicate literal can remove matching rows from the index
// selection before the residual predicate gets a chance to evaluate them.
fn timestamp_millis(millis: i64, nanos: i32) -> Result<i64> {
    validate_nanos(nanos)?;
    if nanos != 0 {
        return Err(Error::DataInvalid {
            message: format!(
                "Timestamp literal cannot be represented in milliseconds: millis={millis}, nanos={nanos}"
            ),
            source: None,
        });
    }
    Ok(millis)
}

fn timestamp_micros(millis: i64, nanos: i32) -> Result<i64> {
    validate_nanos(nanos)?;
    if nanos % 1_000 != 0 {
        return Err(Error::DataInvalid {
            message: format!(
                "Timestamp literal cannot be represented in microseconds: millis={millis}, nanos={nanos}"
            ),
            source: None,
        });
    }
    millis
        .checked_mul(1_000)
        .and_then(|value| value.checked_add(i64::from(nanos / 1_000)))
        .ok_or_else(|| Error::DataInvalid {
            message: format!(
                "Timestamp cannot be represented in microseconds: millis={millis}, nanos={nanos}"
            ),
            source: None,
        })
}

struct Decoder<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn position(&self) -> usize {
        self.position
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }

    fn read_exact(&mut self, length: usize, field: &str) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| format_invalid(format!("{field} range overflow")))?;
        let value = self.bytes.get(self.position..end).ok_or_else(|| {
            format_invalid(format!(
                "truncated {field}: need {length} bytes, but only {} remain",
                self.remaining()
            ))
        })?;
        self.position = end;
        Ok(value)
    }

    fn read_u8(&mut self, field: &str) -> Result<u8> {
        Ok(self.read_exact(1, field)?[0])
    }

    fn read_i16(&mut self, field: &str) -> Result<i16> {
        Ok(i16::from_be_bytes(
            self.read_exact(2, field)?.try_into().unwrap(),
        ))
    }

    fn read_i32(&mut self, field: &str) -> Result<i32> {
        Ok(i32::from_be_bytes(
            self.read_exact(4, field)?.try_into().unwrap(),
        ))
    }

    fn read_i64(&mut self, field: &str) -> Result<i64> {
        Ok(i64::from_be_bytes(
            self.read_exact(8, field)?.try_into().unwrap(),
        ))
    }

    fn read_f32(&mut self, field: &str) -> Result<f32> {
        Ok(f32::from_bits(self.read_i32(field)? as u32))
    }

    fn read_f64(&mut self, field: &str) -> Result<f64> {
        Ok(f64::from_bits(self.read_i64(field)? as u64))
    }

    fn read_count(&mut self, field: &str) -> Result<usize> {
        let value = self.read_i32(field)?;
        usize::try_from(value).map_err(|_| format_invalid(format!("negative {field}: {value}")))
    }
}

fn read_sized_header<'a>(input: &mut Decoder<'a>, field: &str) -> Result<Decoder<'a>> {
    let length = input.read_count(&format!("{field} header length"))?;
    Ok(Decoder::new(
        input.read_exact(length, &format!("{field} header"))?,
    ))
}

fn ensure_consumed(input: &Decoder<'_>, field: &str) -> Result<()> {
    if input.remaining() == 0 {
        Ok(())
    } else {
        Err(format_invalid(format!(
            "{field} has {} trailing bytes",
            input.remaining()
        )))
    }
}

fn read_version(input: &mut Decoder<'_>, field: &str) -> Result<()> {
    let version = input.read_u8(&format!("{field} version"))?;
    if version == VERSION_1 {
        Ok(())
    } else {
        Err(format_invalid(format!(
            "unsupported {field} version: {version}"
        )))
    }
}

fn push_sorted_value(values: &mut Vec<RangeValue>, value: RangeValue) -> Result<()> {
    if values.last().is_some_and(|previous| previous >= &value) {
        return Err(format_invalid(
            "range bitmap dictionary values are not strictly sorted",
        ));
    }
    values.push(value);
    Ok(())
}

fn parse_dictionary(
    serialized: &[u8],
    codec: RangeValueCodec,
    cardinality: usize,
) -> Result<Vec<RangeValue>> {
    let mut input = Decoder::new(serialized);
    let mut header = read_sized_header(&mut input, "range bitmap dictionary")?;
    read_version(&mut header, "range bitmap dictionary")?;
    let chunk_count = header.read_count("dictionary chunk count")?;
    let offsets_length = header.read_count("dictionary offsets length")?;
    let chunks_length = header.read_count("dictionary chunks length")?;
    ensure_consumed(&header, "range bitmap dictionary header")?;

    let expected_offsets_length = chunk_count
        .checked_mul(4)
        .ok_or_else(|| format_invalid("dictionary offsets length overflow"))?;
    if offsets_length != expected_offsets_length {
        return Err(format_invalid(format!(
            "dictionary offsets length {offsets_length} does not match {chunk_count} chunks"
        )));
    }

    let offsets_bytes = input.read_exact(offsets_length, "dictionary offsets")?;
    let chunks = input.read_exact(chunks_length, "dictionary chunks")?;
    let keys = input.read_exact(input.remaining(), "dictionary keys")?;

    let mut offsets_input = Decoder::new(offsets_bytes);
    let mut offsets = Vec::with_capacity(chunk_count);
    for index in 0..chunk_count {
        let offset = offsets_input.read_count(&format!("dictionary chunk {index} offset"))?;
        if index == 0 && offset != 0 {
            return Err(format_invalid(format!(
                "first dictionary chunk offset must be 0, but was {offset}"
            )));
        }
        if offsets.last().is_some_and(|previous| *previous >= offset) {
            return Err(format_invalid("dictionary chunk offsets do not increase"));
        }
        if offset >= chunks_length {
            return Err(format_invalid(format!(
                "dictionary chunk offset {offset} exceeds chunk area length {chunks_length}"
            )));
        }
        offsets.push(offset);
    }
    if chunk_count == 0 && (chunks_length != 0 || !keys.is_empty()) {
        return Err(format_invalid(
            "empty dictionary contains chunk or key bytes",
        ));
    }

    // Every dictionary value needs at least one byte in the serialized
    // dictionary (fixed-width values store that byte directly, while strings
    // need a length or offset). Reject an impossible cardinality before using
    // the untrusted header value as an allocation size. `try_reserve_exact`
    // then turns a genuine allocation failure into the same fail-open format
    // error as any other malformed index.
    if cardinality > serialized.len() {
        return Err(format_invalid(format!(
            "range bitmap cardinality {cardinality} exceeds dictionary payload size {}",
            serialized.len()
        )));
    }
    let mut values = Vec::new();
    values.try_reserve_exact(cardinality).map_err(|error| {
        format_invalid(format!(
            "failed to allocate range bitmap dictionary for {cardinality} values: {error}"
        ))
    })?;
    let mut expected_key_offset = 0usize;
    for index in 0..chunk_count {
        let start = offsets[index];
        let end = offsets.get(index + 1).copied().unwrap_or(chunks_length);
        let mut chunk = Decoder::new(&chunks[start..end]);
        read_version(&mut chunk, "range bitmap dictionary chunk")?;
        let first = codec.read_value(&mut chunk)?;
        let code = chunk.read_count("dictionary chunk code")?;
        if code != values.len() {
            return Err(format_invalid(format!(
                "dictionary chunk code {code} does not match expected {}",
                values.len()
            )));
        }
        let key_offset = chunk.read_count("dictionary key offset")?;
        if key_offset != expected_key_offset {
            return Err(format_invalid(format!(
                "dictionary key offset {key_offset} does not match expected {expected_key_offset}"
            )));
        }
        let additional_count = chunk.read_count("dictionary chunk value count")?;
        push_sorted_value(&mut values, first)?;

        if let Some(fixed_length) = codec.fixed_length() {
            let keys_length = chunk.read_count("dictionary fixed keys length")?;
            let encoded_fixed_length = chunk.read_count("dictionary fixed key length")?;
            ensure_consumed(&chunk, "fixed dictionary chunk")?;
            if encoded_fixed_length != fixed_length {
                return Err(format_invalid(format!(
                    "dictionary fixed key length {encoded_fixed_length} does not match {fixed_length}"
                )));
            }
            let expected_length = additional_count
                .checked_mul(fixed_length)
                .ok_or_else(|| format_invalid("dictionary fixed keys length overflow"))?;
            if keys_length != expected_length {
                return Err(format_invalid(format!(
                    "dictionary fixed keys length {keys_length} does not match {expected_length}"
                )));
            }
            let key_end = key_offset
                .checked_add(keys_length)
                .ok_or_else(|| format_invalid("dictionary fixed keys range overflow"))?;
            let mut key_input =
                Decoder::new(keys.get(key_offset..key_end).ok_or_else(|| {
                    format_invalid("dictionary fixed keys range exceeds key area")
                })?);
            for _ in 0..additional_count {
                push_sorted_value(&mut values, codec.read_value(&mut key_input)?)?;
            }
            ensure_consumed(&key_input, "dictionary fixed keys")?;
            expected_key_offset = key_end;
        } else {
            let inner_offsets_length = chunk.read_count("dictionary string offsets length")?;
            let keys_length = chunk.read_count("dictionary string keys length")?;
            ensure_consumed(&chunk, "variable dictionary chunk")?;
            let expected_length = additional_count
                .checked_mul(4)
                .ok_or_else(|| format_invalid("dictionary string offsets length overflow"))?;
            if inner_offsets_length != expected_length {
                return Err(format_invalid(format!(
                    "dictionary string offsets length {inner_offsets_length} does not match {expected_length}"
                )));
            }
            let total_length = inner_offsets_length
                .checked_add(keys_length)
                .ok_or_else(|| format_invalid("dictionary string keys length overflow"))?;
            let key_end = key_offset
                .checked_add(total_length)
                .ok_or_else(|| format_invalid("dictionary string keys range overflow"))?;
            let payload = keys
                .get(key_offset..key_end)
                .ok_or_else(|| format_invalid("dictionary string keys range exceeds key area"))?;
            let mut inner_offsets_input = Decoder::new(&payload[..inner_offsets_length]);
            let mut inner_offsets = Vec::with_capacity(additional_count);
            for value_index in 0..additional_count {
                let offset = inner_offsets_input
                    .read_count(&format!("dictionary string {value_index} offset"))?;
                if value_index == 0 && offset != 0 {
                    return Err(format_invalid(format!(
                        "first dictionary string offset must be 0, but was {offset}"
                    )));
                }
                if inner_offsets
                    .last()
                    .is_some_and(|previous| *previous >= offset)
                {
                    return Err(format_invalid("dictionary string offsets do not increase"));
                }
                if offset >= keys_length {
                    return Err(format_invalid(format!(
                        "dictionary string offset {offset} exceeds key length {keys_length}"
                    )));
                }
                inner_offsets.push(offset);
            }
            let encoded_keys = &payload[inner_offsets_length..];
            for value_index in 0..additional_count {
                let start = inner_offsets[value_index];
                let end = inner_offsets
                    .get(value_index + 1)
                    .copied()
                    .unwrap_or(keys_length);
                let mut value_input = Decoder::new(&encoded_keys[start..end]);
                push_sorted_value(&mut values, codec.read_value(&mut value_input)?)?;
                ensure_consumed(&value_input, "dictionary string value")?;
            }
            expected_key_offset = key_end;
        }
    }

    if expected_key_offset != keys.len() {
        return Err(format_invalid(format!(
            "dictionary used {expected_key_offset} of {} key bytes",
            keys.len()
        )));
    }
    if values.len() != cardinality {
        return Err(format_invalid(format!(
            "dictionary contains {} values, expected {cardinality}",
            values.len()
        )));
    }
    Ok(values)
}

fn deserialize_bitmap(bytes: &[u8], row_count: u32, field: &str) -> Result<RoaringBitmap> {
    let mut cursor = Cursor::new(bytes);
    let bitmap = RoaringBitmap::deserialize_from(&mut cursor)
        .map_err(|error| format_invalid(format!("invalid RoaringBitmap for {field}: {error}")))?;
    if cursor.position() != bytes.len() as u64 {
        return Err(format_invalid(format!(
            "RoaringBitmap for {field} consumed {} of {} bytes",
            cursor.position(),
            bytes.len()
        )));
    }
    if let Some(position) = bitmap.max() {
        if position >= row_count {
            return Err(format_invalid(format!(
                "RoaringBitmap row position {position} exceeds row count {row_count}"
            )));
        }
    }
    Ok(bitmap)
}

struct BitSliceIndex {
    existing: RoaringBitmap,
    slices: Vec<RoaringBitmap>,
}

impl BitSliceIndex {
    fn parse(serialized: &[u8], row_count: u32, cardinality: usize) -> Result<Self> {
        let mut input = Decoder::new(serialized);
        let mut header = read_sized_header(&mut input, "range bitmap BSI")?;
        read_version(&mut header, "range bitmap BSI")?;
        let slice_count = header.read_u8("BSI slice count")? as usize;
        if slice_count == 0 || slice_count > 64 {
            return Err(format_invalid(format!(
                "invalid BSI slice count: {slice_count}"
            )));
        }
        if cardinality > 0 {
            // Java's writer constructs the BSI for codes in
            // [0, cardinality - 1] and always emits at least one slice. An
            // undersized BSI silently aliases dictionary codes, while an
            // oversized one can introduce codes the dictionary cannot resolve.
            let max_code = cardinality - 1;
            let required_slice_count = ((usize::BITS - max_code.leading_zeros()) as usize).max(1);
            if slice_count != required_slice_count {
                return Err(format_invalid(format!(
                    "BSI slice count {slice_count} does not match the {required_slice_count} slices required for dictionary cardinality {cardinality}"
                )));
            }
        }
        let existing_length = header.read_count("BSI existence bitmap length")?;
        let indexes_length = header.read_count("BSI indexes length")?;
        let expected_indexes_length = slice_count
            .checked_mul(8)
            .ok_or_else(|| format_invalid("BSI indexes length overflow"))?;
        if indexes_length != expected_indexes_length {
            return Err(format_invalid(format!(
                "BSI indexes length {indexes_length} does not match {slice_count} slices"
            )));
        }
        let indexes = header.read_exact(indexes_length, "BSI indexes")?;
        ensure_consumed(&header, "range bitmap BSI header")?;

        let existing_bytes = input.read_exact(existing_length, "BSI existence bitmap")?;
        let existing = deserialize_bitmap(existing_bytes, row_count, "BSI existence bitmap")?;
        if cardinality == 0 && !existing.is_empty() {
            return Err(format_invalid(
                "empty range bitmap dictionary has non-empty existence bitmap",
            ));
        }
        if cardinality > existing.len() as usize {
            return Err(format_invalid(format!(
                "range bitmap cardinality {cardinality} exceeds {} non-null rows",
                existing.len()
            )));
        }

        let slice_bytes = input.read_exact(input.remaining(), "BSI slice bitmaps")?;
        let mut indexes_input = Decoder::new(indexes);
        let mut slices = Vec::with_capacity(slice_count);
        let mut expected_offset = 0usize;
        for index in 0..slice_count {
            let offset = indexes_input.read_count(&format!("BSI slice {index} offset"))?;
            let length = indexes_input.read_count(&format!("BSI slice {index} length"))?;
            if offset != expected_offset {
                return Err(format_invalid(format!(
                    "BSI slice {index} offset {offset} does not match expected {expected_offset}"
                )));
            }
            let end = offset
                .checked_add(length)
                .ok_or_else(|| format_invalid("BSI slice range overflow"))?;
            let slice = deserialize_bitmap(
                slice_bytes
                    .get(offset..end)
                    .ok_or_else(|| format_invalid("BSI slice exceeds payload"))?,
                row_count,
                &format!("BSI slice {index}"),
            )?;
            let mut outside = slice.clone();
            outside -= &existing;
            if !outside.is_empty() {
                return Err(format_invalid(format!(
                    "BSI slice {index} contains rows outside the existence bitmap"
                )));
            }
            slices.push(slice);
            expected_offset = end;
        }
        if expected_offset != slice_bytes.len() {
            return Err(format_invalid(format!(
                "BSI slices used {expected_offset} of {} bytes",
                slice_bytes.len()
            )));
        }
        let bsi = Self { existing, slices };
        if cardinality > 0 {
            let invalid_codes = bsi.gte(cardinality);
            if !invalid_codes.is_empty() {
                return Err(format_invalid(format!(
                    "BSI contains {} rows with codes outside dictionary cardinality {cardinality}",
                    invalid_codes.len()
                )));
            }
        }
        Ok(bsi)
    }

    fn eq(&self, code: usize) -> RoaringBitmap {
        let mut selected = self.existing.clone();
        for (index, slice) in self.slices.iter().enumerate() {
            if ((code >> index) & 1) == 1 {
                selected &= slice;
            } else {
                selected -= slice;
            }
        }
        selected
    }

    fn gt(&self, code: i64) -> RoaringBitmap {
        if code < 0 {
            return self.existing.clone();
        }
        let code = code as u64;
        let start = code.trailing_ones() as usize;
        let mut state = None;
        for (index, slice) in self.slices.iter().enumerate().skip(start) {
            match &mut state {
                None => state = Some(slice.clone()),
                Some(state) if ((code >> index) & 1) == 1 => *state &= slice,
                Some(state) => *state |= slice,
            }
        }
        let mut selected = state.unwrap_or_default();
        selected &= &self.existing;
        selected
    }

    fn gte(&self, code: usize) -> RoaringBitmap {
        if code == 0 {
            self.existing.clone()
        } else {
            self.gt(code as i64 - 1)
        }
    }
}

/// Java-compatible Range Bitmap V1 reader.
pub(crate) struct RangeBitmapFileIndexReader {
    codec: RangeValueCodec,
    row_count: u32,
    dictionary: Vec<RangeValue>,
    bsi: BitSliceIndex,
}

impl RangeBitmapFileIndexReader {
    pub(crate) fn try_new(data_type: DataType, serialized: Bytes) -> Result<Self> {
        let codec = RangeValueCodec::try_new(&data_type)?;
        let mut input = Decoder::new(&serialized);
        let mut header = read_sized_header(&mut input, "range bitmap")?;
        read_version(&mut header, "range bitmap")?;
        let row_count = u32::try_from(header.read_i32("range bitmap row count")?)
            .map_err(|_| format_invalid("range bitmap row count must be non-negative"))?;
        let cardinality = header.read_count("range bitmap cardinality")?;
        if cardinality > row_count as usize {
            return Err(format_invalid(format!(
                "range bitmap cardinality {cardinality} exceeds row count {row_count}"
            )));
        }
        let min = (cardinality > 0)
            .then(|| codec.read_value(&mut header))
            .transpose()?;
        let max = (cardinality > 0)
            .then(|| codec.read_value(&mut header))
            .transpose()?;
        let dictionary_length = header.read_count("range bitmap dictionary length")?;
        ensure_consumed(&header, "range bitmap header")?;

        let dictionary_bytes = input.read_exact(dictionary_length, "range bitmap dictionary")?;
        let dictionary = parse_dictionary(dictionary_bytes, codec, cardinality)?;
        if dictionary.first() != min.as_ref() || dictionary.last() != max.as_ref() {
            return Err(format_invalid(
                "range bitmap min/max do not match the dictionary",
            ));
        }
        let bsi = BitSliceIndex::parse(
            input.read_exact(input.remaining(), "range bitmap BSI")?,
            row_count,
            cardinality,
        )?;

        Ok(Self {
            codec,
            row_count,
            dictionary,
            bsi,
        })
    }

    fn all_rows(&self) -> RoaringBitmap {
        let mut rows = RoaringBitmap::new();
        rows.insert_range(0..self.row_count);
        rows
    }

    fn is_null(&self) -> RoaringBitmap {
        let mut rows = self.all_rows();
        rows -= &self.bsi.existing;
        rows
    }

    fn not(&self, excluded: &RoaringBitmap) -> RoaringBitmap {
        let mut selected = self.bsi.existing.clone();
        selected -= excluded;
        selected
    }

    fn include_nan_rows(&self, mut selected: RoaringBitmap) -> RoaringBitmap {
        let nan = match self.codec {
            RangeValueCodec::Float => Some(RangeValue::Float(JavaFloat::new(f32::NAN))),
            RangeValueCodec::Double => Some(RangeValue::Double(JavaDouble::new(f64::NAN))),
            _ => None,
        };
        if let Some(nan) = nan {
            selected |= self.eq(&nan);
        }
        selected
    }

    fn eq(&self, value: &RangeValue) -> RoaringBitmap {
        self.dictionary
            .binary_search(value)
            .map_or_else(|_| RoaringBitmap::new(), |code| self.bsi.eq(code))
    }

    fn gt(&self, value: &RangeValue) -> RoaringBitmap {
        match self.dictionary.binary_search(value) {
            Ok(code) => self.bsi.gt(code as i64),
            Err(code) => self.bsi.gte(code),
        }
    }

    fn gte(&self, value: &RangeValue) -> RoaringBitmap {
        let code = self
            .dictionary
            .binary_search(value)
            .unwrap_or_else(|code| code);
        self.bsi.gte(code)
    }

    fn lt(&self, value: &RangeValue) -> RoaringBitmap {
        self.not(&self.gte(value))
    }

    fn lte(&self, value: &RangeValue) -> RoaringBitmap {
        self.not(&self.gt(value))
    }

    fn literals_bitmap(&self, literals: &[Datum], skip_nan: bool) -> Result<RoaringBitmap> {
        let mut selected = RoaringBitmap::new();
        for literal in literals {
            let value = self.codec.value(literal)?;
            if !skip_nan || !value.is_nan() {
                selected |= self.eq(&value);
            }
        }
        Ok(selected)
    }

    pub(crate) fn try_evaluate(
        &self,
        data_type: &DataType,
        operator: PredicateOperator,
        literals: &[Datum],
    ) -> Result<FileIndexResult> {
        if RangeValueCodec::try_new(data_type).ok() != Some(self.codec) {
            return Ok(FileIndexResult::Remain);
        }

        let selected = match operator {
            PredicateOperator::IsNull if literals.is_empty() => self.is_null(),
            PredicateOperator::IsNotNull if literals.is_empty() => self.bsi.existing.clone(),
            PredicateOperator::Eq if literals.len() == 1 => {
                self.eq(&self.codec.value(&literals[0])?)
            }
            PredicateOperator::NotEq if literals.len() == 1 => {
                let value = self.codec.value(&literals[0])?;
                if value.is_nan() {
                    self.bsi.existing.clone()
                } else {
                    self.not(&self.eq(&value))
                }
            }
            PredicateOperator::In => self.literals_bitmap(literals, false)?,
            PredicateOperator::NotIn => self.not(&self.literals_bitmap(literals, true)?),
            PredicateOperator::Lt if literals.len() == 1 => {
                let value = self.codec.value(&literals[0])?;
                if value.is_nan() {
                    return Ok(FileIndexResult::Remain);
                }
                self.include_nan_rows(self.lt(&value))
            }
            PredicateOperator::LtEq if literals.len() == 1 => {
                let value = self.codec.value(&literals[0])?;
                if value.is_nan() {
                    return Ok(FileIndexResult::Remain);
                }
                self.include_nan_rows(self.lte(&value))
            }
            PredicateOperator::Gt if literals.len() == 1 => {
                let value = self.codec.value(&literals[0])?;
                if value.is_nan() {
                    return Ok(FileIndexResult::Remain);
                }
                self.include_nan_rows(self.gt(&value))
            }
            PredicateOperator::GtEq if literals.len() == 1 => {
                let value = self.codec.value(&literals[0])?;
                if value.is_nan() {
                    return Ok(FileIndexResult::Remain);
                }
                self.include_nan_rows(self.gte(&value))
            }
            PredicateOperator::Between if literals.len() == 2 => {
                let lower = self.codec.value(&literals[0])?;
                let upper = self.codec.value(&literals[1])?;
                if lower.is_nan() || upper.is_nan() {
                    return Ok(FileIndexResult::Remain);
                }
                let mut selected = self.gte(&lower);
                selected &= self.lte(&upper);
                self.include_nan_rows(selected)
            }
            _ => return Ok(FileIndexResult::Remain),
        };
        Ok(FileIndexResult::Selection(selected))
    }
}

impl FileIndexReader for RangeBitmapFileIndexReader {
    fn evaluate(
        &self,
        _column: &str,
        _index: usize,
        data_type: &DataType,
        operator: PredicateOperator,
        literals: &[Datum],
    ) -> FileIndexResult {
        // Predicate evaluation must fail open. A malformed lazy value or an
        // incompatible literal can reduce performance, but must never lose rows.
        self.try_evaluate(data_type, operator, literals)
            .unwrap_or(FileIndexResult::Remain)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Float32Array};
    use bytes::{BufMut, BytesMut};

    use super::*;
    use crate::arrow::residual::evaluate_exact_leaf_predicate;
    use crate::spec::{IntType, LocalZonedTimestampType, TimestampType};

    // Generated by Java RangeBitmapFileIndex.Writer on Apache Paimon master
    // commit 2bbdb668f for [1, 3, 5, 7, 9, null, null, 10].
    const JAVA_INT_V1: &str = concat!(
        "00000015010000000800000006000000010000000a000000420000000d010000",
        "0001000000040000001900000000010000000100000000000000000000000500",
        "00001400000004000000030000000500000007000000090000000a0000002201",
        "030000001300000018000000000000001600000016000000140000002a000000",
        "143b3000000100000500020000000400070000003a3000000100000000000200",
        "100000000100030007003a300000010000000000010010000000020003003a30",
        "000001000000000001001000000004000700"
    );
    // Generated by the same Java writer for ["aa", "b", "你好", null, "ccc"].
    const JAVA_STRING_V1: &str = concat!(
        "0000001d01000000050000000400000002616100000006e4bda0e5a5bd000000",
        "520000000d0100000001000000040000001b0000000001000000026161000000",
        "0000000000000000030000000c0000001600000000000000050000000c000000",
        "01620000000363636300000006e4bda0e5a5bd0000001a010200000018000000",
        "10000000000000001400000014000000143a3000000100000000000300100000",
        "0000000100020004003a300000010000000000010010000000010002003a3000",
        "0001000000000001001000000002000400"
    );
    // Generated by the same Java writer for [-0.0, +0.0, 1.5, NaN, null].
    const JAVA_FLOAT_V1: &str = concat!(
        "00000015010000000500000004800000007fc000000000003a0000000d010000",
        "0001000000040000001900000000018000000000000000000000000000000300",
        "00000c00000004000000003fc000007fc000000000001a01020000000f000000",
        "10000000000000001400000014000000143b3000000100000300010000000300",
        "3a300000010000000000010010000000010003003a3000000100000000000100",
        "1000000002000300"
    );

    fn int_type() -> DataType {
        DataType::Int(IntType::new())
    }

    fn reader(bytes: Bytes) -> RangeBitmapFileIndexReader {
        RangeBitmapFileIndexReader::try_new(int_type(), bytes).unwrap()
    }

    #[test]
    fn test_writer_matches_java_v1_bytes() {
        use crate::common::Options;
        use crate::file_index::file_index_writer::FileIndexWriter;
        use writer::RangeBitmapFileIndexWriter;

        let cases = [
            (
                int_type(),
                vec![
                    Some(Datum::Int(1)),
                    Some(Datum::Int(3)),
                    Some(Datum::Int(5)),
                    Some(Datum::Int(7)),
                    Some(Datum::Int(9)),
                    None,
                    None,
                    Some(Datum::Int(10)),
                ],
                JAVA_INT_V1,
            ),
            (
                DataType::VarChar(crate::spec::VarCharType::new(32).unwrap()),
                vec![
                    Some(Datum::String("aa".into())),
                    Some(Datum::String("b".into())),
                    Some(Datum::String("你好".into())),
                    None,
                    Some(Datum::String("ccc".into())),
                ],
                JAVA_STRING_V1,
            ),
            (
                DataType::Float(crate::spec::FloatType::new()),
                vec![
                    Some(Datum::Float(-0.0)),
                    Some(Datum::Float(0.0)),
                    Some(Datum::Float(1.5)),
                    Some(Datum::Float(f32::NAN)),
                    None,
                ],
                JAVA_FLOAT_V1,
            ),
        ];
        for (data_type, values, golden) in cases {
            let mut writer =
                RangeBitmapFileIndexWriter::try_new(data_type, &Options::new()).unwrap();
            for value in &values {
                writer.write(value.as_ref()).unwrap();
            }
            let bytes = writer.serialized_bytes().unwrap();
            assert_eq!(hex::encode(&bytes), golden);
            assert_eq!(writer.serialized_bytes().unwrap(), bytes);
        }
    }

    fn selection(rows: impl IntoIterator<Item = u32>) -> FileIndexResult {
        FileIndexResult::Selection(rows.into_iter().collect())
    }

    fn assert_covers_residual(
        reader: &RangeBitmapFileIndexReader,
        array: &ArrayRef,
        data_type: &DataType,
        operator: PredicateOperator,
        literals: &[Datum],
    ) {
        let residual = evaluate_exact_leaf_predicate(array, data_type, operator, literals).unwrap();
        let indexed = reader.try_evaluate(data_type, operator, literals).unwrap();
        for (row, matches) in residual.iter().enumerate() {
            if matches != Some(true) {
                continue;
            }
            let selected = match &indexed {
                FileIndexResult::Remain => true,
                FileIndexResult::Skip => false,
                FileIndexResult::Selection(rows) => rows.contains(row as u32),
            };
            assert!(
                selected,
                "range bitmap dropped residual match at row {row} for {operator:?} {literals:?}"
            );
        }
    }

    fn single_value_reader(
        codec: RangeValueCodec,
        value: RangeValue,
    ) -> RangeBitmapFileIndexReader {
        let mut existing = RoaringBitmap::new();
        existing.insert(0);
        RangeBitmapFileIndexReader {
            codec,
            row_count: 1,
            dictionary: vec![value],
            bsi: BitSliceIndex {
                existing,
                slices: vec![],
            },
        }
    }

    fn evaluate(
        reader: &RangeBitmapFileIndexReader,
        operator: PredicateOperator,
        literals: &[Datum],
    ) -> FileIndexResult {
        reader
            .try_evaluate(&int_type(), operator, literals)
            .unwrap()
    }

    fn java_all_null_int_index(row_count: u32) -> Bytes {
        let empty = {
            let mut bytes = Vec::new();
            RoaringBitmap::new().serialize_into(&mut bytes).unwrap();
            bytes
        };

        let mut dictionary = BytesMut::new();
        dictionary.put_i32(13);
        dictionary.put_u8(VERSION_1);
        dictionary.put_i32(0);
        dictionary.put_i32(0);
        dictionary.put_i32(0);

        // Java's empty RangeBitmap creates a BSI with 64 empty slices because
        // its maximum dictionary code is -1.
        let slice_count = 64usize;
        let indexes_length = slice_count * 8;
        let mut bsi = BytesMut::new();
        bsi.put_i32((1 + 1 + 4 + 4 + indexes_length) as i32);
        bsi.put_u8(VERSION_1);
        bsi.put_u8(slice_count as u8);
        bsi.put_i32(empty.len() as i32);
        bsi.put_i32(indexes_length as i32);
        for index in 0..slice_count {
            bsi.put_i32((index * empty.len()) as i32);
            bsi.put_i32(empty.len() as i32);
        }
        bsi.extend_from_slice(&empty);
        for _ in 0..slice_count {
            bsi.extend_from_slice(&empty);
        }

        let mut serialized = BytesMut::new();
        serialized.put_i32(13);
        serialized.put_u8(VERSION_1);
        serialized.put_i32(row_count as i32);
        serialized.put_i32(0);
        serialized.put_i32(dictionary.len() as i32);
        serialized.extend_from_slice(&dictionary);
        serialized.extend_from_slice(&bsi);
        serialized.freeze()
    }

    fn int_index_with_bsi_slices(
        dictionary_values: &[i32],
        existing_rows: &[u32],
        slices: &[&[u32]],
    ) -> Bytes {
        assert!(!dictionary_values.is_empty());
        let bitmap_bytes = |positions: &[u32]| {
            let bitmap = positions.iter().copied().collect::<RoaringBitmap>();
            let mut bytes = Vec::new();
            bitmap.serialize_into(&mut bytes).unwrap();
            bytes
        };

        // One fixed-width dictionary chunk. The first value lives in the
        // chunk and the rest in the keys area.
        let mut chunk = BytesMut::new();
        chunk.put_u8(VERSION_1);
        chunk.put_i32(dictionary_values[0]);
        chunk.put_i32(0);
        chunk.put_i32(0);
        chunk.put_i32((dictionary_values.len() - 1) as i32);
        chunk.put_i32(((dictionary_values.len() - 1) * 4) as i32);
        chunk.put_i32(4);

        let mut dictionary = BytesMut::new();
        dictionary.put_i32(13);
        dictionary.put_u8(VERSION_1);
        dictionary.put_i32(1);
        dictionary.put_i32(4);
        dictionary.put_i32(chunk.len() as i32);
        dictionary.put_i32(0);
        dictionary.extend_from_slice(&chunk);
        for value in &dictionary_values[1..] {
            dictionary.put_i32(*value);
        }

        let existing = bitmap_bytes(existing_rows);
        let encoded_slices = slices
            .iter()
            .map(|positions| bitmap_bytes(positions))
            .collect::<Vec<_>>();
        let indexes_length = slices.len() * 8;
        let mut bsi = BytesMut::new();
        bsi.put_i32((1 + 1 + 4 + 4 + indexes_length) as i32);
        bsi.put_u8(VERSION_1);
        bsi.put_u8(slices.len() as u8);
        bsi.put_i32(existing.len() as i32);
        bsi.put_i32(indexes_length as i32);
        let mut offset = 0usize;
        for slice in &encoded_slices {
            bsi.put_i32(offset as i32);
            bsi.put_i32(slice.len() as i32);
            offset += slice.len();
        }
        bsi.extend_from_slice(&existing);
        for slice in encoded_slices {
            bsi.extend_from_slice(&slice);
        }

        let mut serialized = BytesMut::new();
        serialized.put_i32(21);
        serialized.put_u8(VERSION_1);
        serialized.put_i32(existing_rows.iter().copied().max().map_or(0, |max| max + 1) as i32);
        serialized.put_i32(dictionary_values.len() as i32);
        serialized.put_i32(dictionary_values[0]);
        serialized.put_i32(*dictionary_values.last().unwrap());
        serialized.put_i32(dictionary.len() as i32);
        serialized.extend_from_slice(&dictionary);
        serialized.extend_from_slice(&bsi);
        serialized.freeze()
    }

    #[test]
    fn test_java_int_v1_predicates() {
        let reader = reader(Bytes::from(hex::decode(JAVA_INT_V1).unwrap()));

        assert_eq!(
            evaluate(&reader, PredicateOperator::Eq, &[Datum::Int(5)]),
            selection([2])
        );
        assert_eq!(
            evaluate(&reader, PredicateOperator::Eq, &[Datum::Int(6)]),
            selection([])
        );
        assert_eq!(
            evaluate(&reader, PredicateOperator::NotEq, &[Datum::Int(5)]),
            selection([0, 1, 3, 4, 7])
        );
        assert_eq!(
            evaluate(&reader, PredicateOperator::Lt, &[Datum::Int(5)]),
            selection([0, 1])
        );
        assert_eq!(
            evaluate(&reader, PredicateOperator::LtEq, &[Datum::Int(5)]),
            selection([0, 1, 2])
        );
        assert_eq!(
            evaluate(&reader, PredicateOperator::Gt, &[Datum::Int(6)]),
            selection([3, 4, 7])
        );
        assert_eq!(
            evaluate(&reader, PredicateOperator::GtEq, &[Datum::Int(9)]),
            selection([4, 7])
        );
        assert_eq!(
            evaluate(
                &reader,
                PredicateOperator::In,
                &[Datum::Int(1), Datum::Int(10)]
            ),
            selection([0, 7])
        );
        assert_eq!(
            evaluate(
                &reader,
                PredicateOperator::NotIn,
                &[Datum::Int(1), Datum::Int(10)]
            ),
            selection([1, 2, 3, 4])
        );
        assert_eq!(
            evaluate(&reader, PredicateOperator::IsNull, &[]),
            selection([5, 6])
        );
        assert_eq!(
            evaluate(&reader, PredicateOperator::IsNotNull, &[]),
            selection([0, 1, 2, 3, 4, 7])
        );
        assert_eq!(
            evaluate(
                &reader,
                PredicateOperator::Between,
                &[Datum::Int(3), Datum::Int(7)]
            ),
            selection([1, 2, 3])
        );
    }

    #[test]
    fn test_java_string_v1_variable_dictionary() {
        let data_type = DataType::VarChar(crate::spec::VarCharType::new(32).unwrap());
        let reader = RangeBitmapFileIndexReader::try_new(
            data_type.clone(),
            Bytes::from(hex::decode(JAVA_STRING_V1).unwrap()),
        )
        .unwrap();
        let evaluate = |operator, literals: &[Datum]| {
            reader.try_evaluate(&data_type, operator, literals).unwrap()
        };

        assert_eq!(
            evaluate(PredicateOperator::Eq, &[Datum::String("你好".to_string())]),
            selection([2])
        );
        assert_eq!(
            evaluate(PredicateOperator::Gt, &[Datum::String("b".to_string())]),
            selection([2, 4])
        );
        assert_eq!(
            evaluate(
                PredicateOperator::Between,
                &[
                    Datum::String("b".to_string()),
                    Datum::String("ccc".to_string())
                ]
            ),
            selection([1, 4])
        );
    }

    #[test]
    fn test_java_float_v1_covers_residual_zero_and_nan_semantics() {
        let data_type = DataType::Float(crate::spec::FloatType::new());
        let reader = RangeBitmapFileIndexReader::try_new(
            data_type.clone(),
            Bytes::from(hex::decode(JAVA_FLOAT_V1).unwrap()),
        )
        .unwrap();
        let evaluate = |operator, literals: &[Datum]| {
            reader.try_evaluate(&data_type, operator, literals).unwrap()
        };

        // Java canonicalizes every NaN written into the dictionary, so the same
        // payload can represent either NaN sign in the original rows.
        let possible_values: [ArrayRef; 2] = [
            Arc::new(Float32Array::from(vec![
                Some(-0.0),
                Some(0.0),
                Some(1.5),
                Some(f32::NAN),
                None,
            ])),
            Arc::new(Float32Array::from(vec![
                Some(-0.0),
                Some(0.0),
                Some(1.5),
                Some(f32::from_bits(0xffc0_0000)),
                None,
            ])),
        ];
        for values in &possible_values {
            for literal in [
                Datum::Float(-0.0),
                Datum::Float(0.0),
                Datum::Float(f32::NAN),
                Datum::Float(f32::from_bits(0xffc0_0000)),
            ] {
                for operator in [
                    PredicateOperator::Eq,
                    PredicateOperator::NotEq,
                    PredicateOperator::Lt,
                    PredicateOperator::LtEq,
                    PredicateOperator::Gt,
                    PredicateOperator::GtEq,
                    PredicateOperator::In,
                    PredicateOperator::NotIn,
                ] {
                    assert_covers_residual(
                        &reader,
                        values,
                        &data_type,
                        operator,
                        std::slice::from_ref(&literal),
                    );
                }
            }
        }
        assert_eq!(
            evaluate(PredicateOperator::NotEq, &[Datum::Float(f32::NAN)]),
            selection([0, 1, 2, 3])
        );
    }

    #[test]
    fn test_finer_timestamp_literals_fail_open() {
        let cases = [
            (
                DataType::Timestamp(TimestampType::new(3).unwrap()),
                RangeValueCodec::TimestampMillis,
                RangeValue::Timestamp(1_000),
                Datum::Timestamp {
                    millis: 1_000,
                    nanos: 1,
                },
            ),
            (
                DataType::Timestamp(TimestampType::new(6).unwrap()),
                RangeValueCodec::TimestampMicros,
                RangeValue::Timestamp(1_000_000),
                Datum::Timestamp {
                    millis: 1_000,
                    nanos: 1,
                },
            ),
            (
                DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(3).unwrap()),
                RangeValueCodec::LocalZonedTimestampMillis,
                RangeValue::LocalZonedTimestamp(1_000),
                Datum::LocalZonedTimestamp {
                    millis: 1_000,
                    nanos: 1,
                },
            ),
            (
                DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(6).unwrap()),
                RangeValueCodec::LocalZonedTimestampMicros,
                RangeValue::LocalZonedTimestamp(1_000_000),
                Datum::LocalZonedTimestamp {
                    millis: 1_000,
                    nanos: 1,
                },
            ),
        ];

        for (data_type, codec, value, literal) in cases {
            let reader = single_value_reader(codec, value);
            for operator in [
                PredicateOperator::Lt,
                PredicateOperator::NotEq,
                PredicateOperator::NotIn,
            ] {
                assert_eq!(
                    reader.evaluate(
                        "ts",
                        0,
                        &data_type,
                        operator,
                        std::slice::from_ref(&literal)
                    ),
                    FileIndexResult::Remain
                );
            }
        }
    }

    #[test]
    fn test_exact_timestamp_literals_still_use_index() {
        let millis_reader = single_value_reader(
            RangeValueCodec::TimestampMillis,
            RangeValue::Timestamp(1_000),
        );
        assert_eq!(
            millis_reader.evaluate(
                "ts",
                0,
                &DataType::Timestamp(TimestampType::new(3).unwrap()),
                PredicateOperator::Eq,
                &[Datum::Timestamp {
                    millis: 1_000,
                    nanos: 0,
                }],
            ),
            selection([0])
        );

        let micros_reader = single_value_reader(
            RangeValueCodec::TimestampMicros,
            RangeValue::Timestamp(1_000_001),
        );
        assert_eq!(
            micros_reader.evaluate(
                "ts",
                0,
                &DataType::Timestamp(TimestampType::new(6).unwrap()),
                PredicateOperator::Eq,
                &[Datum::Timestamp {
                    millis: 1_000,
                    nanos: 1_000,
                }],
            ),
            selection([0])
        );
    }

    #[test]
    fn test_all_null_index_keeps_every_null_row() {
        let reader = reader(java_all_null_int_index(5));

        assert_eq!(
            evaluate(&reader, PredicateOperator::IsNull, &[]),
            selection(0..5)
        );
        assert_eq!(
            evaluate(&reader, PredicateOperator::IsNotNull, &[]),
            selection([])
        );
        assert_eq!(
            evaluate(&reader, PredicateOperator::Eq, &[Datum::Int(1)]),
            selection([])
        );
    }

    #[test]
    fn test_malformed_bsi_is_rejected() {
        let mut bytes = hex::decode(JAVA_INT_V1).unwrap();
        bytes.pop();
        assert!(matches!(
            RangeBitmapFileIndexReader::try_new(int_type(), Bytes::from(bytes)),
            Err(Error::FileIndexFormatInvalid { .. })
        ));
    }

    #[test]
    fn test_bsi_slice_width_must_match_dictionary_cardinality() {
        let bytes = int_index_with_bsi_slices(&[1, 2, 3], &[0, 1, 2], &[&[1]]);
        assert!(matches!(
            RangeBitmapFileIndexReader::try_new(int_type(), bytes),
            Err(Error::FileIndexFormatInvalid { .. })
        ));
    }

    #[test]
    fn test_bsi_codes_must_fit_dictionary_cardinality() {
        // Row 2 has both bits set, encoding code 3 for a three-value
        // dictionary whose valid codes are 0, 1, and 2.
        let bytes = int_index_with_bsi_slices(&[1, 2, 3], &[0, 1, 2], &[&[1, 2], &[2]]);
        assert!(matches!(
            RangeBitmapFileIndexReader::try_new(int_type(), bytes),
            Err(Error::FileIndexFormatInvalid { .. })
        ));
    }

    #[test]
    fn test_single_value_dictionary_keeps_one_slice() {
        let bytes = int_index_with_bsi_slices(&[7], &[0, 1, 2], &[&[]]);
        let reader = reader(bytes);
        assert_eq!(
            evaluate(&reader, PredicateOperator::Eq, &[Datum::Int(7)]),
            selection([0, 1, 2])
        );
    }
}
