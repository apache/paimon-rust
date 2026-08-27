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

use std::cmp::Ordering;
use std::collections::HashMap;
use std::io::Cursor;
use std::ops::Range;

use bytes::Bytes;
use roaring::RoaringBitmap;

use crate::file_index::file_index_reader::FileIndexReader;
use crate::file_index::file_index_result::FileIndexResult;
use crate::spec::{DataType, Datum, PredicateOperator};
use crate::{Error, Result};

pub(crate) mod writer;

const VERSION_1: u8 = 1;
const VERSION_2: u8 = 2;
const JAVA_CANONICAL_FLOAT_NAN_BITS: u32 = 0x7fc0_0000;
const JAVA_CANONICAL_DOUBLE_NAN_BITS: u64 = 0x7ff8_0000_0000_0000;

fn format_invalid(message: impl Into<String>) -> Error {
    Error::FileIndexFormatInvalid {
        message: message.into(),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BitmapValueCodec {
    Boolean,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Float,
    Double,
    Date,
    Time,
    TimestampMillis,
    TimestampMicros,
    LocalZonedTimestampMillis,
    LocalZonedTimestampMicros,
    String,
}

impl BitmapValueCodec {
    fn try_new(data_type: &DataType) -> Result<Self> {
        match data_type {
            DataType::Boolean(_) => Ok(Self::Boolean),
            DataType::TinyInt(_) => Ok(Self::TinyInt),
            DataType::SmallInt(_) => Ok(Self::SmallInt),
            DataType::Int(_) => Ok(Self::Int),
            DataType::BigInt(_) => Ok(Self::BigInt),
            DataType::Float(_) => Ok(Self::Float),
            DataType::Double(_) => Ok(Self::Double),
            DataType::Date(_) => Ok(Self::Date),
            DataType::Time(_) => Ok(Self::Time),
            DataType::Timestamp(timestamp) if timestamp.precision() <= 3 => {
                Ok(Self::TimestampMillis)
            }
            DataType::Timestamp(_) => Ok(Self::TimestampMicros),
            DataType::LocalZonedTimestamp(timestamp) if timestamp.precision() <= 3 => {
                Ok(Self::LocalZonedTimestampMillis)
            }
            DataType::LocalZonedTimestamp(_) => Ok(Self::LocalZonedTimestampMicros),
            DataType::Char(_) | DataType::VarChar(_) => Ok(Self::String),
            _ => Err(Error::Unsupported {
                message: format!("Bitmap file index does not support data type {data_type:?}"),
            }),
        }
    }

    fn read_value(self, input: &mut Decoder<'_>) -> Result<BitmapValue> {
        Ok(match self {
            Self::Boolean => BitmapValue::Boolean(input.read_bool("boolean value")?),
            Self::TinyInt => BitmapValue::TinyInt(input.read_i8("tinyint value")?),
            Self::SmallInt => BitmapValue::SmallInt(input.read_i16("smallint value")?),
            Self::Int => BitmapValue::Int(input.read_i32("int value")?),
            Self::BigInt => BitmapValue::BigInt(input.read_i64("bigint value")?),
            Self::Float => BitmapValue::Float(JavaFloat::new(input.read_f32("float value")?)),
            Self::Double => BitmapValue::Double(JavaDouble::new(input.read_f64("double value")?)),
            Self::Date => BitmapValue::Date(input.read_i32("date value")?),
            Self::Time => BitmapValue::Time(input.read_i32("time value")?),
            Self::TimestampMillis | Self::TimestampMicros => {
                BitmapValue::Timestamp(input.read_i64("timestamp value")?)
            }
            Self::LocalZonedTimestampMillis | Self::LocalZonedTimestampMicros => {
                BitmapValue::LocalZonedTimestamp(input.read_i64("local zoned timestamp value")?)
            }
            Self::String => {
                let length = input.read_count("string value length")?;
                let bytes = input.read_exact(length, "string value")?;
                let value = std::str::from_utf8(bytes)
                    .map_err(|error| {
                        format_invalid(format!("invalid UTF-8 bitmap string value: {error}"))
                    })?
                    .to_string();
                BitmapValue::String(value)
            }
        })
    }

    fn value(self, datum: &Datum) -> Result<BitmapValue> {
        match (self, datum) {
            (Self::Boolean, Datum::Bool(value)) => Ok(BitmapValue::Boolean(*value)),
            (Self::TinyInt, Datum::TinyInt(value)) => Ok(BitmapValue::TinyInt(*value)),
            (Self::SmallInt, Datum::SmallInt(value)) => Ok(BitmapValue::SmallInt(*value)),
            (Self::Int, Datum::Int(value)) => Ok(BitmapValue::Int(*value)),
            (Self::BigInt, Datum::Long(value)) => Ok(BitmapValue::BigInt(*value)),
            (Self::Float, Datum::Float(value)) => Ok(BitmapValue::Float(JavaFloat::new(*value))),
            (Self::Double, Datum::Double(value)) => {
                Ok(BitmapValue::Double(JavaDouble::new(*value)))
            }
            (Self::Date, Datum::Date(value)) => Ok(BitmapValue::Date(*value)),
            (Self::Time, Datum::Time(value)) => Ok(BitmapValue::Time(*value)),
            (Self::TimestampMillis, Datum::Timestamp { millis, nanos }) => {
                validate_nanos(*nanos)?;
                Ok(BitmapValue::Timestamp(*millis))
            }
            (Self::TimestampMicros, Datum::Timestamp { millis, nanos }) => {
                Ok(BitmapValue::Timestamp(timestamp_micros(*millis, *nanos)?))
            }
            (Self::LocalZonedTimestampMillis, Datum::LocalZonedTimestamp { millis, nanos }) => {
                validate_nanos(*nanos)?;
                Ok(BitmapValue::LocalZonedTimestamp(*millis))
            }
            (Self::LocalZonedTimestampMicros, Datum::LocalZonedTimestamp { millis, nanos }) => Ok(
                BitmapValue::LocalZonedTimestamp(timestamp_micros(*millis, *nanos)?),
            ),
            (Self::String, Datum::String(value)) => Ok(BitmapValue::String(value.clone())),
            _ => Err(Error::DataInvalid {
                message: format!("Datum {datum:?} does not match Bitmap codec {self:?}"),
                source: None,
            }),
        }
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

    fn opposite_zero(self) -> Option<Self> {
        match self.0 {
            0 => Some(Self(0x8000_0000)),
            0x8000_0000 => Some(Self(0)),
            _ => None,
        }
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

    fn opposite_zero(self) -> Option<Self> {
        match self.0 {
            0 => Some(Self(0x8000_0000_0000_0000)),
            0x8000_0000_0000_0000 => Some(Self(0)),
            _ => None,
        }
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

fn timestamp_micros(millis: i64, nanos: i32) -> Result<i64> {
    validate_nanos(nanos)?;
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

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum BitmapValue {
    Boolean(bool),
    TinyInt(i8),
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Float(JavaFloat),
    Double(JavaDouble),
    Date(i32),
    Time(i32),
    Timestamp(i64),
    LocalZonedTimestamp(i64),
    String(String),
}

impl BitmapValue {
    fn equivalent_zero(&self) -> Option<Self> {
        match self {
            Self::Float(value) => value.opposite_zero().map(Self::Float),
            Self::Double(value) => value.opposite_zero().map(Self::Double),
            _ => None,
        }
    }

    fn is_nan(&self) -> bool {
        match self {
            Self::Float(value) => value.value().is_nan(),
            Self::Double(value) => value.value().is_nan(),
            _ => false,
        }
    }
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

    fn read_bool(&mut self, field: &str) -> Result<bool> {
        Ok(self.read_u8(field)? != 0)
    }

    fn read_i8(&mut self, field: &str) -> Result<i8> {
        Ok(self.read_u8(field)? as i8)
    }

    fn read_i16(&mut self, field: &str) -> Result<i16> {
        let bytes = self.read_exact(2, field)?;
        Ok(i16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn read_i32(&mut self, field: &str) -> Result<i32> {
        let bytes = self.read_exact(4, field)?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_i64(&mut self, field: &str) -> Result<i64> {
        let bytes = self.read_exact(8, field)?;
        Ok(i64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
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

#[derive(Debug)]
struct RawEntry {
    key: Option<BitmapValue>,
    offset: i32,
    length: Option<i32>,
}

#[derive(Debug)]
struct BitmapEntry {
    key: Option<BitmapValue>,
    location: BitmapLocation,
}

#[derive(Clone, Debug)]
enum BitmapLocation {
    Singleton(u32),
    Serialized(Range<usize>),
}

#[derive(Debug)]
struct V1Index {
    null_entry: Option<BitmapLocation>,
    entries: HashMap<BitmapValue, BitmapLocation>,
}

#[derive(Debug)]
struct V2Block {
    first_key: BitmapValue,
    range: Range<usize>,
}

#[derive(Debug)]
struct V2Index {
    non_null_count: usize,
    null_entry: Option<BitmapLocation>,
    blocks: Vec<V2Block>,
    body_start: usize,
}

#[derive(Debug)]
enum BitmapIndex {
    V1(V1Index),
    V2(V2Index),
}

fn read_row_count(input: &mut Decoder<'_>) -> Result<u32> {
    let row_count = input.read_i32("row count")?;
    u32::try_from(row_count).map_err(|_| format_invalid(format!("negative row count: {row_count}")))
}

fn validate_entry_count(row_count: u32, non_null_count: usize, has_null: bool) -> Result<()> {
    let distinct_count = non_null_count
        .checked_add(usize::from(has_null))
        .ok_or_else(|| format_invalid("bitmap entry count overflow"))?;
    if distinct_count > row_count as usize {
        return Err(format_invalid(format!(
            "bitmap entry count {distinct_count} exceeds row count {row_count}"
        )));
    }
    Ok(())
}

fn singleton_position(offset: i32, row_count: u32) -> Result<u32> {
    let position = u32::try_from(-1_i64 - i64::from(offset))
        .map_err(|_| format_invalid(format!("invalid singleton offset: {offset}")))?;
    if position >= row_count {
        return Err(format_invalid(format!(
            "singleton row position {position} exceeds row count {row_count}"
        )));
    }
    Ok(position)
}

fn parse_v1(
    serialized: &[u8],
    input: &mut Decoder<'_>,
    codec: BitmapValueCodec,
) -> Result<(u32, Vec<BitmapEntry>)> {
    let row_count = read_row_count(input)?;
    let non_null_count = input.read_count("non-null bitmap count")?;
    let has_null = input.read_bool("has-null flag")?;
    validate_entry_count(row_count, non_null_count, has_null)?;

    let mut raw_entries = Vec::new();
    if has_null {
        raw_entries.push(RawEntry {
            key: None,
            offset: input.read_i32("null bitmap offset")?,
            length: None,
        });
    }
    for index in 0..non_null_count {
        raw_entries.push(RawEntry {
            key: Some(codec.read_value(input)?),
            offset: input.read_i32(&format!("bitmap entry {index} offset"))?,
            length: None,
        });
    }

    let body_start = input.position();
    let body_length = serialized.len() - body_start;
    let positive_offsets = raw_entries
        .iter()
        .filter(|entry| entry.offset >= 0)
        .map(|entry| entry.offset as usize)
        .collect::<Vec<_>>();

    if let Some((first, rest)) = positive_offsets.split_first() {
        if *first != 0 {
            return Err(format_invalid(format!(
                "first V1 bitmap offset must be 0, but was {first}"
            )));
        }
        for offsets in positive_offsets.windows(2) {
            if offsets[0] >= offsets[1] {
                return Err(format_invalid(format!(
                    "V1 bitmap offsets must increase, but found {} then {}",
                    offsets[0], offsets[1]
                )));
            }
        }
        let last = rest.last().unwrap_or(first);
        if *last >= body_length {
            return Err(format_invalid(format!(
                "V1 bitmap offset {} exceeds body length {body_length}",
                last
            )));
        }
    } else if body_length != 0 {
        return Err(format_invalid(format!(
            "V1 bitmap body has {body_length} bytes without a serialized bitmap"
        )));
    }

    let mut positive_index = 0;
    let mut entries = Vec::with_capacity(raw_entries.len());
    for entry in raw_entries {
        let location = if entry.offset < 0 {
            BitmapLocation::Singleton(singleton_position(entry.offset, row_count)?)
        } else {
            let start = positive_offsets[positive_index];
            let end = positive_offsets
                .get(positive_index + 1)
                .copied()
                .unwrap_or(body_length);
            positive_index += 1;
            BitmapLocation::Serialized(body_start + start..body_start + end)
        };
        entries.push(BitmapEntry {
            key: entry.key,
            location,
        });
    }

    Ok((row_count, entries))
}

fn parse_v2(
    serialized: &[u8],
    input: &mut Decoder<'_>,
    codec: BitmapValueCodec,
) -> Result<(u32, V2Index)> {
    let row_count = read_row_count(input)?;
    let non_null_count = input.read_count("non-null bitmap count")?;
    let has_null = input.read_bool("has-null flag")?;
    validate_entry_count(row_count, non_null_count, has_null)?;

    let raw_null_entry = if has_null {
        Some(RawEntry {
            key: None,
            offset: input.read_i32("null bitmap offset")?,
            length: Some(input.read_i32("null bitmap length")?),
        })
    } else {
        None
    };

    let block_count = input.read_count("bitmap index block count")?;
    if block_count > non_null_count || (block_count == 0) != (non_null_count == 0) {
        return Err(format_invalid(format!(
            "bitmap index block count {block_count} is inconsistent with {non_null_count} entries"
        )));
    }

    let mut blocks = Vec::new();
    for index in 0..block_count {
        let key = codec.read_value(input)?;
        if blocks
            .last()
            .is_some_and(|(previous, _): &(BitmapValue, usize)| previous >= &key)
        {
            return Err(format_invalid("V2 bitmap index block keys are not sorted"));
        }
        let offset = input.read_i32(&format!("bitmap index block {index} offset"))?;
        let offset = usize::try_from(offset)
            .map_err(|_| format_invalid(format!("negative bitmap index block offset: {offset}")))?;
        blocks.push((key, offset));
    }

    let bitmap_body_offset = input.read_i32("bitmap body offset")?;
    let bitmap_body_offset = usize::try_from(bitmap_body_offset).map_err(|_| {
        format_invalid(format!("negative bitmap body offset: {bitmap_body_offset}"))
    })?;
    let index_block_start = input.position();
    let body_start = index_block_start
        .checked_add(bitmap_body_offset)
        .ok_or_else(|| format_invalid("bitmap body start overflow"))?;
    if body_start > serialized.len() {
        return Err(format_invalid(format!(
            "bitmap body start {body_start} exceeds payload length {}",
            serialized.len()
        )));
    }

    let mut index_blocks = Vec::with_capacity(blocks.len());
    let mut previous_offset = None;
    let mut blocks = blocks.into_iter().enumerate().peekable();
    while let Some((block_index, (first_key, offset))) = blocks.next() {
        if block_index == 0 && offset != 0 {
            return Err(format_invalid(format!(
                "first bitmap index block offset must be 0, but was {offset}"
            )));
        }
        if let Some(previous_offset) = previous_offset {
            if offset <= previous_offset {
                return Err(format_invalid(format!(
                    "bitmap index block offsets must increase, but found {previous_offset} then {offset}"
                )));
            }
        }
        previous_offset = Some(offset);
        let end = blocks
            .peek()
            .map(|(_, (_, next_offset))| *next_offset)
            .unwrap_or(bitmap_body_offset);
        if end > bitmap_body_offset || end.saturating_sub(offset) < 4 {
            return Err(format_invalid(format!(
                "bitmap index block {block_index} range {offset}..{end} is outside the {bitmap_body_offset}-byte index area"
            )));
        }
        index_blocks.push(V2Block {
            first_key,
            range: index_block_start + offset..index_block_start + end,
        });
    }

    if block_count == 0 && bitmap_body_offset != 0 {
        return Err(format_invalid(format!(
            "V2 bitmap index area has {bitmap_body_offset} bytes without any blocks"
        )));
    }

    let null_entry = raw_null_entry
        .map(|entry| {
            let length = entry
                .length
                .ok_or_else(|| format_invalid("missing V2 null bitmap length"))?;
            v2_location(
                serialized.len(),
                body_start,
                row_count,
                false,
                entry.offset,
                length,
            )
        })
        .transpose()?;

    if non_null_count == 0 {
        let expected_end = match &null_entry {
            Some(BitmapLocation::Serialized(range)) if range.start == body_start => range.end,
            Some(BitmapLocation::Serialized(range)) => {
                return Err(format_invalid(format!(
                    "null bitmap starts at {}, expected {body_start}",
                    range.start
                )))
            }
            _ => body_start,
        };
        if expected_end != serialized.len() {
            return Err(format_invalid(format!(
                "bitmap body ends at {expected_end}, but payload length is {}",
                serialized.len()
            )));
        }
    }

    Ok((
        row_count,
        V2Index {
            non_null_count,
            null_entry,
            blocks: index_blocks,
            body_start,
        },
    ))
}

fn v2_location(
    payload_length: usize,
    body_start: usize,
    row_count: u32,
    non_null: bool,
    offset: i32,
    length: i32,
) -> Result<BitmapLocation> {
    if offset < 0 {
        if (non_null && length != -1) || (!non_null && length <= 0) {
            return Err(format_invalid(format!(
                "singleton bitmap has invalid length {length}"
            )));
        }
        return singleton_position(offset, row_count).map(BitmapLocation::Singleton);
    }

    let length = usize::try_from(length)
        .map_err(|_| format_invalid(format!("negative serialized bitmap length: {length}")))?;
    if length == 0 {
        return Err(format_invalid("serialized bitmap length must be positive"));
    }
    let start = body_start
        .checked_add(offset as usize)
        .ok_or_else(|| format_invalid("serialized bitmap start overflow"))?;
    let end = start
        .checked_add(length)
        .ok_or_else(|| format_invalid("serialized bitmap end overflow"))?;
    if end > payload_length {
        return Err(format_invalid(format!(
            "serialized bitmap range {start}..{end} exceeds payload length {payload_length}"
        )));
    }
    Ok(BitmapLocation::Serialized(start..end))
}

impl V1Index {
    fn try_new(entries: Vec<BitmapEntry>) -> Result<Self> {
        let mut null_entry = None;
        let mut value_entries = HashMap::new();
        for entry in entries {
            match entry.key {
                None if null_entry.is_some() => {
                    return Err(format_invalid("duplicate null bitmap entry"));
                }
                None => null_entry = Some(entry.location),
                Some(key) => {
                    if value_entries.insert(key, entry.location).is_some() {
                        return Err(format_invalid("duplicate bitmap value"));
                    }
                }
            }
        }
        Ok(Self {
            null_entry,
            entries: value_entries,
        })
    }
}

impl V2Index {
    fn find_location(
        &self,
        serialized: &[u8],
        codec: BitmapValueCodec,
        row_count: u32,
        value: &BitmapValue,
    ) -> Result<Option<BitmapLocation>> {
        let Some(block_index) = self
            .blocks
            .partition_point(|block| block.first_key.cmp(value).is_le())
            .checked_sub(1)
        else {
            return Ok(None);
        };
        let block = &self.blocks[block_index];
        let mut input = Decoder::new(&serialized[block.range.clone()]);
        let entry_count = input.read_count("bitmap index block entry count")?;
        if entry_count == 0 || entry_count > self.non_null_count {
            return Err(format_invalid(format!(
                "invalid bitmap index block entry count: {entry_count}"
            )));
        }

        let mut previous_key = None;
        let mut matched = None;
        for entry_index in 0..entry_count {
            let key = codec.read_value(&mut input)?;
            if entry_index == 0 && key != block.first_key {
                return Err(format_invalid(format!(
                    "bitmap index block {block_index} first key does not match its secondary index"
                )));
            }
            if previous_key
                .as_ref()
                .is_some_and(|previous| previous >= &key)
            {
                return Err(format_invalid(format!(
                    "bitmap index block {block_index} keys are not sorted"
                )));
            }
            previous_key = Some(key.clone());

            let location = v2_location(
                serialized.len(),
                self.body_start,
                row_count,
                true,
                input.read_i32("bitmap offset")?,
                input.read_i32("bitmap length")?,
            )?;
            if &key == value {
                matched = Some(location);
            }
        }
        if input.remaining() != 0 {
            return Err(format_invalid(format!(
                "bitmap index block {block_index} has {} trailing bytes",
                input.remaining()
            )));
        }
        Ok(matched)
    }
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

/// Reader for the supported Java Paimon Bitmap V1 and V2 payload types.
pub(crate) struct BitmapFileIndexReader {
    codec: BitmapValueCodec,
    row_count: u32,
    serialized: Bytes,
    index: BitmapIndex,
}

impl BitmapFileIndexReader {
    pub(crate) fn try_new(data_type: DataType, serialized: Bytes) -> Result<Self> {
        let codec = BitmapValueCodec::try_new(&data_type)?;
        let mut input = Decoder::new(&serialized);
        let version = input.read_u8("Bitmap version")?;
        let (row_count, index) = match version {
            VERSION_1 => {
                let (row_count, entries) = parse_v1(&serialized, &mut input, codec)?;
                (row_count, BitmapIndex::V1(V1Index::try_new(entries)?))
            }
            VERSION_2 => {
                let (row_count, index) = parse_v2(&serialized, &mut input, codec)?;
                (row_count, BitmapIndex::V2(index))
            }
            _ => {
                return Err(format_invalid(format!(
                    "unsupported Bitmap version: {version}"
                )))
            }
        };

        Ok(Self {
            codec,
            row_count,
            serialized,
            index,
        })
    }

    fn all_rows(&self) -> RoaringBitmap {
        let mut rows = RoaringBitmap::new();
        rows.insert_range(0..self.row_count);
        rows
    }

    fn bitmap(&self, location: &BitmapLocation, field: &str) -> Result<RoaringBitmap> {
        match location {
            BitmapLocation::Singleton(position) => Ok([*position].into_iter().collect()),
            BitmapLocation::Serialized(range) => {
                deserialize_bitmap(&self.serialized[range.clone()], self.row_count, field)
            }
        }
    }

    fn null_bitmap(&self) -> Result<RoaringBitmap> {
        let location = match &self.index {
            BitmapIndex::V1(index) => index.null_entry.as_ref(),
            BitmapIndex::V2(index) => index.null_entry.as_ref(),
        };
        match location {
            Some(location) => self.bitmap(location, "null bitmap"),
            None => Ok(RoaringBitmap::new()),
        }
    }

    fn literals_bitmap(
        &self,
        literals: &[Datum],
        skip_nan_literals: bool,
    ) -> Result<RoaringBitmap> {
        let mut selection = RoaringBitmap::new();
        for literal in literals {
            let value = self.codec.value(literal)?;
            if skip_nan_literals && value.is_nan() {
                continue;
            }
            let equivalent_zero = value.equivalent_zero();
            for value in std::iter::once(value).chain(equivalent_zero) {
                let location = match &self.index {
                    BitmapIndex::V1(index) => index.entries.get(&value).cloned(),
                    BitmapIndex::V2(index) => {
                        index.find_location(&self.serialized, self.codec, self.row_count, &value)?
                    }
                };
                if let Some(location) = location {
                    selection |= self.bitmap(&location, "value bitmap")?;
                }
            }
        }
        Ok(selection)
    }

    fn complement(&self, excluded: &RoaringBitmap) -> RoaringBitmap {
        let mut selection = self.all_rows();
        selection -= excluded;
        selection
    }

    /// Evaluates a predicate while preserving lazy format errors for direct callers.
    pub(crate) fn try_evaluate(
        &self,
        data_type: &DataType,
        operator: PredicateOperator,
        literals: &[Datum],
    ) -> Result<FileIndexResult> {
        if BitmapValueCodec::try_new(data_type).ok() != Some(self.codec) {
            return Ok(FileIndexResult::Remain);
        }

        let selection = match operator {
            PredicateOperator::Eq if literals.len() == 1 => {
                self.literals_bitmap(literals, false)?
            }
            PredicateOperator::In => self.literals_bitmap(literals, false)?,
            PredicateOperator::NotEq if literals.len() == 1 => {
                // Arrow's scalar predicate kernels treat NaN as unequal to
                // itself. A NaN literal therefore cannot exclude any row, even
                // though Java's bitmap dictionary has a canonical NaN key.
                self.complement(&self.literals_bitmap(literals, true)?)
            }
            PredicateOperator::NotIn => self.complement(&self.literals_bitmap(literals, true)?),
            PredicateOperator::IsNull if literals.is_empty() => self.null_bitmap()?,
            PredicateOperator::IsNotNull if literals.is_empty() => {
                self.complement(&self.null_bitmap()?)
            }
            _ => return Ok(FileIndexResult::Remain),
        };
        Ok(FileIndexResult::Selection(selection))
    }
}

impl FileIndexReader for BitmapFileIndexReader {
    fn evaluate(
        &self,
        _column: &str,
        _index: usize,
        data_type: &DataType,
        operator: PredicateOperator,
        literals: &[Datum],
    ) -> FileIndexResult {
        // The shared reader trait is intentionally infallible. A corrupt lazy
        // section must fail open instead of pruning potentially matching rows.
        self.try_evaluate(data_type, operator, literals)
            .unwrap_or(FileIndexResult::Remain)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Options;
    use crate::spec::{
        BigIntType, BinaryType, BooleanType, CharType, DateType, DoubleType, FloatType, IntType,
        LocalZonedTimestampType, SmallIntType, TimeType, TimestampType, TinyIntType, VarCharType,
    };

    // Generated by Java release-2.0 BitmapFileIndex.Writer at commit ad720da96e7b.
    const JAVA_BOOLEAN_V1: &str = concat!(
        "010000000500000002010000000000fffffffe01000000143a30000001000000",
        "0000010010000000020004003a30000001000000000001001000000000000300"
    );
    const JAVA_BOOLEAN_V2: &str = concat!(
        "0200000005000000020100000000000000140000000100000000000000001600",
        "00000200fffffffeffffffff0100000014000000143a3000000100000000000100",
        "10000000020004003a30000001000000000001001000000000000300"
    );
    const JAVA_TINYINT_V1: &str = concat!(
        "01000000050000000201000000007ffffffffe80000000143a3000000100000000",
        "00010010000000020004003a30000001000000000001001000000000000300"
    );
    const JAVA_TINYINT_V2: &str = concat!(
        "020000000500000002010000000000000014000000018000000000000000160000",
        "00028000000014000000147ffffffffeffffffff3a300000010000000000010010",
        "000000020004003a30000001000000000001001000000000000300"
    );
    const JAVA_SMALLINT_V1: &str = concat!(
        "01000000050000000201000000005ba0fffffffecfc7000000143a300000010000",
        "000000010010000000020004003a30000001000000000001001000000000000300"
    );
    const JAVA_SMALLINT_V2: &str = concat!(
        "02000000050000000201000000000000001400000001cfc7000000000000001800",
        "000002cfc700000014000000145ba0fffffffeffffffff3a30000001000000000001",
        "0010000000020004003a30000001000000000001001000000000000300"
    );
    const JAVA_INT_V1: &str = concat!(
        "01000000050000000201000000000000002afffffffef8a432eb000000143a3000",
        "00010000000000010010000000020004003a300000010000000000010010000000",
        "00000300"
    );
    const JAVA_INT_V2: &str = concat!(
        "02000000050000000201000000000000001400000001f8a432eb00000000000000",
        "1c00000002f8a432eb00000014000000140000002afffffffeffffffff3a300000",
        "010000000000010010000000020004003a30000001000000000001001000000000",
        "000300"
    );
    const JAVA_BIGINT_V1: &str = concat!(
        "01000000050000000201000000007ffffffffffffe37fffffffe800000000000007b",
        "000000143a300000010000000000010010000000020004003a3000000100000000",
        "0001001000000000000300"
    );
    const JAVA_BIGINT_V2: &str = concat!(
        "02000000050000000201000000000000001400000001800000000000007b000000",
        "000000002400000002800000000000007b00000014000000147ffffffffffffe37",
        "fffffffeffffffff3a300000010000000000010010000000020004003a30000001",
        "000000000001001000000000000300"
    );
    const JAVA_FLOAT_V1: &str = concat!(
        "010000000a0000000401000000007fc00000000000148000000000000028000000000000",
        "003cbfc00000000000503a300000010000000000010010000000040009003a3000000100",
        "00000000010010000000030008003a300000010000000000010010000000020007003a30",
        "0000010000000000010010000000010006003a3000000100000000000100100000000000",
        "0500"
    );
    const JAVA_FLOAT_V2: &str = concat!(
        "020000000a0000000401000000000000001400000004bfc0000000000000800000000000",
        "001000000000000000207fc00000000000300000004000000001bfc00000000000500000",
        "00140000000180000000000000280000001400000001000000000000003c000000140000",
        "00017fc0000000000014000000143a300000010000000000010010000000040009003a30",
        "0000010000000000010010000000030008003a3000000100000000000100100000000200",
        "07003a300000010000000000010010000000010006003a30000001000000000001001000",
        "000000000500"
    );
    const JAVA_DOUBLE_V1: &str = concat!(
        "010000000a00000004010000000080000000000000000000001400000000000000000000",
        "00287ff80000000000000000003cbff8000000000000000000503a300000010000000000",
        "010010000000040009003a300000010000000000010010000000020007003a3000000100",
        "00000000010010000000010006003a300000010000000000010010000000030008003a30",
        "000001000000000001001000000000000500"
    );
    const JAVA_DOUBLE_V2: &str = concat!(
        "020000000a0000000401000000000000001400000004bff8000000000000000000008000",
        "000000000000000000140000000000000000000000287ff80000000000000000003c0000",
        "005000000001bff800000000000000000050000000140000000180000000000000000000",
        "0014000000140000000100000000000000000000002800000014000000017ff800000000",
        "00000000003c000000143a300000010000000000010010000000040009003a3000000100",
        "00000000010010000000020007003a300000010000000000010010000000010006003a30",
        "0000010000000000010010000000030008003a3000000100000000000100100000000000",
        "0500"
    );
    const JAVA_DATE_V1: &str = concat!(
        "010000000500000002010000000000004e20fffffffeffffffff000000143a300000",
        "010000000000010010000000020004003a30000001000000000001001000000000",
        "000300"
    );
    const JAVA_DATE_V2: &str = concat!(
        "02000000050000000201000000000000001400000001ffffffff000000000000001c",
        "00000002ffffffff000000140000001400004e20fffffffeffffffff3a3000000100",
        "00000000010010000000020004003a300000010000000000010010000000000003",
        "00"
    );
    const JAVA_TIME_V1: &str = concat!(
        "0100000005000000020100000000000000000000001405265bfffffffffe3a3000",
        "00010000000000010010000000020004003a300000010000000000010010000000",
        "00000300"
    );
    const JAVA_TIME_V2: &str = concat!(
        "020000000500000002010000000000000014000000010000000000000000000000",
        "1c0000000200000000000000140000001405265bfffffffffeffffffff3a300000",
        "010000000000010010000000020004003a30000001000000000001001000000000",
        "000300"
    );
    const JAVA_TIMESTAMP_MILLIS_V1: &str = concat!(
        "01000000050000000201000000000000018bcfe5687bfffffffefffffffff8a432eb",
        "000000143a300000010000000000010010000000020004003a3000000100000000",
        "0001001000000000000300"
    );
    const JAVA_TIMESTAMP_MILLIS_V2: &str = concat!(
        "02000000050000000201000000000000001400000001fffffffff8a432eb000000",
        "000000002400000002fffffffff8a432eb00000014000000140000018bcfe5687b",
        "fffffffeffffffff3a300000010000000000010010000000020004003a30000001",
        "000000000001001000000000000300"
    );
    const JAVA_TIMESTAMP_MICROS_V1: &str = concat!(
        "0100000005000000020100000000ffffffffffffffff0000001400060a2418202240",
        "fffffffe3a300000010000000000010010000000020004003a3000000100000000",
        "0001001000000000000300"
    );
    const JAVA_TIMESTAMP_MICROS_V2: &str = concat!(
        "02000000050000000201000000000000001400000001ffffffffffffffff000000",
        "000000002400000002ffffffffffffffff000000140000001400060a2418202240",
        "fffffffeffffffff3a300000010000000000010010000000020004003a30000001",
        "000000000001001000000000000300"
    );
    const JAVA_STRING_V1: &str = concat!(
        "0100000005000000020100000000000000017afffffffe0000000d5061696d6f6e",
        "2de6b4bee89299000000143a300000010000000000010010000000020004003a30",
        "000001000000000001001000000000000300"
    );
    const JAVA_STRING_V2: &str = concat!(
        "020000000500000002010000000000000014000000010000000d5061696d6f6e2d",
        "e6b4bee89299000000000000002a000000020000000d5061696d6f6e2de6b4be",
        "e892990000001400000014000000017afffffffeffffffff3a300000010000000000",
        "010010000000020004003a30000001000000000001001000000000000300"
    );
    const JAVA_INT_MULTIBLOCK_V2: &str = concat!(
        "020000000500000004000000000400000001000000000000000200000010000000",
        "03000000200000000400000030000000400000000100000001fffffffeffffffff",
        "0000000100000002fffffffcffffffff0000000100000003fffffffdffffffff0000",
        "00010000000400000000000000143a300000010000000000010010000000000004",
        "00"
    );
    const JAVA_INT_MULTIPLE_BODIES_MULTIBLOCK_V2: &str = concat!(
        "020000000800000003010000000000000014000000020000000100000000000000",
        "030000001c0000002c000000020000000100000014000000140000000200000028",
        "0000001400000001000000030000003c000000143a300000010000000000010010",
        "000000020005003a300000010000000000010010000000010006003a3000000100",
        "00000000010010000000000004003a300000010000000000010010000000030007",
        "00"
    );
    const JAVA_STRING_BLOCK_BOUNDARY_V2: &str = concat!(
        "020000000400000003000000000200000001610000000000000002636300000021",
        "0000003300000002000000016100000000000000140000000462626262ffffffff",
        "ffffffff00000001000000026363fffffffdffffffff3a3000000100000000000100",
        "1000000001000300"
    );
    const JAVA_SINGLETON_NULL_V1: &str =
        "01000000030000000201fffffffd00000000ffffffff00000001fffffffe";
    const JAVA_SINGLETON_NULL_V2: &str = concat!(
        "02000000030000000201fffffffd00000012000000010000000000000000000000",
        "1c0000000200000000ffffffffffffffff00000001fffffffeffffffff"
    );
    const JAVA_EMPTY_V1: &str = "01000000000000000000";
    const JAVA_EMPTY_V2: &str = "020000000000000000000000000000000000";
    const JAVA_FLOAT_SINGLETONS_V2: &str = concat!(
        "02000000040000000301fffffffc00000012000000018000000000000000000000",
        "280000000380000000fffffffdffffffff00000000fffffffeffffffff7fc00000",
        "ffffffffffffffff"
    );
    const JAVA_DOUBLE_SINGLETONS_V2: &str = concat!(
        "02000000040000000301fffffffc00000012000000018000000000000000000000",
        "0000000034000000038000000000000000fffffffdffffffff0000000000000000",
        "fffffffeffffffff7ff8000000000000ffffffffffffffff"
    );

    struct Fixture {
        name: &'static str,
        data_type: DataType,
        repeated: Datum,
        singleton: Datum,
        v1: &'static str,
        v2: &'static str,
    }

    fn bytes(encoded: &str) -> Bytes {
        Bytes::from(hex::decode(encoded).unwrap())
    }

    fn selection(rows: impl IntoIterator<Item = u32>) -> FileIndexResult {
        FileIndexResult::Selection(rows.into_iter().collect())
    }

    fn evaluate(
        reader: &BitmapFileIndexReader,
        data_type: &DataType,
        operator: PredicateOperator,
        literals: &[Datum],
    ) -> FileIndexResult {
        reader.evaluate("field", 0, data_type, operator, literals)
    }

    fn fixtures() -> Vec<Fixture> {
        vec![
            Fixture {
                name: "boolean",
                data_type: DataType::Boolean(BooleanType::new()),
                repeated: Datum::Bool(true),
                singleton: Datum::Bool(false),
                v1: JAVA_BOOLEAN_V1,
                v2: JAVA_BOOLEAN_V2,
            },
            Fixture {
                name: "tinyint",
                data_type: DataType::TinyInt(TinyIntType::new()),
                repeated: Datum::TinyInt(-128),
                singleton: Datum::TinyInt(127),
                v1: JAVA_TINYINT_V1,
                v2: JAVA_TINYINT_V2,
            },
            Fixture {
                name: "smallint",
                data_type: DataType::SmallInt(SmallIntType::new()),
                repeated: Datum::SmallInt(-12_345),
                singleton: Datum::SmallInt(23_456),
                v1: JAVA_SMALLINT_V1,
                v2: JAVA_SMALLINT_V2,
            },
            Fixture {
                name: "int",
                data_type: DataType::Int(IntType::new()),
                repeated: Datum::Int(-123_456_789),
                singleton: Datum::Int(42),
                v1: JAVA_INT_V1,
                v2: JAVA_INT_V2,
            },
            Fixture {
                name: "bigint",
                data_type: DataType::BigInt(BigIntType::new()),
                repeated: Datum::Long(i64::MIN + 123),
                singleton: Datum::Long(i64::MAX - 456),
                v1: JAVA_BIGINT_V1,
                v2: JAVA_BIGINT_V2,
            },
            Fixture {
                name: "date",
                data_type: DataType::Date(DateType::new()),
                repeated: Datum::Date(-1),
                singleton: Datum::Date(20_000),
                v1: JAVA_DATE_V1,
                v2: JAVA_DATE_V2,
            },
            Fixture {
                name: "time",
                data_type: DataType::Time(TimeType::new(3).unwrap()),
                repeated: Datum::Time(0),
                singleton: Datum::Time(86_399_999),
                v1: JAVA_TIME_V1,
                v2: JAVA_TIME_V2,
            },
            Fixture {
                name: "timestamp_millis",
                data_type: DataType::Timestamp(TimestampType::new(3).unwrap()),
                repeated: Datum::Timestamp {
                    millis: -123_456_789,
                    nanos: 0,
                },
                singleton: Datum::Timestamp {
                    millis: 1_700_000_000_123,
                    nanos: 0,
                },
                v1: JAVA_TIMESTAMP_MILLIS_V1,
                v2: JAVA_TIMESTAMP_MILLIS_V2,
            },
            Fixture {
                name: "timestamp_micros",
                data_type: DataType::Timestamp(TimestampType::new(6).unwrap()),
                repeated: Datum::Timestamp {
                    millis: -1,
                    nanos: 999_000,
                },
                singleton: Datum::Timestamp {
                    millis: 1_700_000_000_123,
                    nanos: 456_000,
                },
                v1: JAVA_TIMESTAMP_MICROS_V1,
                v2: JAVA_TIMESTAMP_MICROS_V2,
            },
            Fixture {
                name: "local_zoned_timestamp_millis",
                data_type: DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(3).unwrap()),
                repeated: Datum::LocalZonedTimestamp {
                    millis: -123_456_789,
                    nanos: 0,
                },
                singleton: Datum::LocalZonedTimestamp {
                    millis: 1_700_000_000_123,
                    nanos: 0,
                },
                v1: JAVA_TIMESTAMP_MILLIS_V1,
                v2: JAVA_TIMESTAMP_MILLIS_V2,
            },
            Fixture {
                name: "local_zoned_timestamp_micros",
                data_type: DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(6).unwrap()),
                repeated: Datum::LocalZonedTimestamp {
                    millis: -1,
                    nanos: 999_000,
                },
                singleton: Datum::LocalZonedTimestamp {
                    millis: 1_700_000_000_123,
                    nanos: 456_000,
                },
                v1: JAVA_TIMESTAMP_MICROS_V1,
                v2: JAVA_TIMESTAMP_MICROS_V2,
            },
            Fixture {
                name: "char",
                data_type: DataType::Char(CharType::new(20).unwrap()),
                repeated: Datum::String("Paimon-\u{6d3e}\u{8499}".to_string()),
                singleton: Datum::String("z".to_string()),
                v1: JAVA_STRING_V1,
                v2: JAVA_STRING_V2,
            },
            Fixture {
                name: "varchar",
                data_type: DataType::VarChar(VarCharType::new(20).unwrap()),
                repeated: Datum::String("Paimon-\u{6d3e}\u{8499}".to_string()),
                singleton: Datum::String("z".to_string()),
                v1: JAVA_STRING_V1,
                v2: JAVA_STRING_V2,
            },
        ]
    }

    #[test]
    fn test_v2_writer_matches_java_golden_payloads() {
        for fixture in fixtures() {
            let mut writer =
                writer::BitmapFileIndexWriter::try_new(fixture.data_type, &Options::new())
                    .unwrap_or_else(|error| panic!("{} writer failed: {error}", fixture.name));
            for datum in [
                Some(&fixture.repeated),
                Some(&fixture.singleton),
                None,
                Some(&fixture.repeated),
                None,
            ] {
                writer
                    .write(datum)
                    .unwrap_or_else(|error| panic!("{} write failed: {error}", fixture.name));
            }
            assert_eq!(
                writer.serialized_bytes().unwrap(),
                bytes(fixture.v2),
                "{} V2 writer",
                fixture.name
            );
        }
    }

    #[test]
    fn test_v2_writer_matches_java_floating_value_encoding() {
        let float_values = [
            Datum::Float(f32::from_bits(0xffa1_2345)),
            Datum::Float(0.0),
            Datum::Float(-0.0),
        ];
        let mut float_writer = writer::BitmapFileIndexWriter::try_new(
            DataType::Float(FloatType::new()),
            &Options::new(),
        )
        .unwrap();
        for value in &float_values {
            float_writer.write(Some(value)).unwrap();
        }
        float_writer.write(None).unwrap();
        assert_eq!(
            float_writer.serialized_bytes().unwrap(),
            bytes(JAVA_FLOAT_SINGLETONS_V2)
        );

        let double_values = [
            Datum::Double(f64::from_bits(0xfff0_1234_5678_9abc)),
            Datum::Double(0.0),
            Datum::Double(-0.0),
        ];
        let mut double_writer = writer::BitmapFileIndexWriter::try_new(
            DataType::Double(DoubleType::new()),
            &Options::new(),
        )
        .unwrap();
        for value in &double_values {
            double_writer.write(Some(value)).unwrap();
        }
        double_writer.write(None).unwrap();
        assert_eq!(
            double_writer.serialized_bytes().unwrap(),
            bytes(JAVA_DOUBLE_SINGLETONS_V2)
        );
    }

    #[test]
    fn test_v2_writer_round_trip_null_singleton_and_multiple_values() {
        let data_type = DataType::Int(IntType::new());
        let repeated = Datum::Int(-123_456_789);
        let singleton = Datum::Int(42);
        let mut writer =
            writer::BitmapFileIndexWriter::try_new(data_type.clone(), &Options::new()).unwrap();
        for datum in [
            Some(&repeated),
            Some(&singleton),
            None,
            Some(&repeated),
            None,
        ] {
            writer.write(datum).unwrap();
        }

        let serialized = writer.serialized_bytes().unwrap();
        assert_eq!(serialized, bytes(JAVA_INT_V2));
        let reader = BitmapFileIndexReader::try_new(data_type.clone(), serialized).unwrap();
        assert!(matches!(&reader.index, BitmapIndex::V2(_)));
        assert_eq!(
            evaluate(&reader, &data_type, PredicateOperator::Eq, &[repeated]),
            selection([0, 3])
        );
        assert_eq!(
            evaluate(&reader, &data_type, PredicateOperator::Eq, &[singleton]),
            selection([1])
        );
        assert_eq!(
            evaluate(&reader, &data_type, PredicateOperator::IsNull, &[]),
            selection([2, 4])
        );
    }

    #[test]
    fn test_v2_writer_empty_and_singleton_null_payloads() {
        let data_type = DataType::Int(IntType::new());
        let mut empty =
            writer::BitmapFileIndexWriter::try_new(data_type.clone(), &Options::new()).unwrap();
        assert_eq!(empty.serialized_bytes().unwrap(), bytes(JAVA_EMPTY_V2));

        let mut singleton =
            writer::BitmapFileIndexWriter::try_new(data_type.clone(), &Options::new()).unwrap();
        for datum in [Some(&Datum::Int(0)), Some(&Datum::Int(1)), None] {
            singleton.write(datum).unwrap();
        }
        let serialized = singleton.serialized_bytes().unwrap();
        assert_eq!(serialized, bytes(JAVA_SINGLETON_NULL_V2));
        let reader = BitmapFileIndexReader::try_new(data_type.clone(), serialized).unwrap();
        assert_eq!(
            evaluate(&reader, &data_type, PredicateOperator::IsNull, &[]),
            selection([2])
        );

        let mut all_null =
            writer::BitmapFileIndexWriter::try_new(data_type.clone(), &Options::new()).unwrap();
        for _ in 0..3 {
            all_null.write(None).unwrap();
        }
        let reader =
            BitmapFileIndexReader::try_new(data_type.clone(), all_null.serialized_bytes().unwrap())
                .unwrap();
        assert_eq!(
            evaluate(&reader, &data_type, PredicateOperator::IsNull, &[]),
            selection(0..3)
        );
        assert_eq!(
            evaluate(&reader, &data_type, PredicateOperator::IsNotNull, &[]),
            selection([])
        );
    }

    #[test]
    fn test_v2_writer_multiple_index_blocks() {
        let data_type = DataType::Int(IntType::new());
        let mut options = Options::new();
        options.set("index-block-size", "16");
        let mut writer =
            writer::BitmapFileIndexWriter::try_new(data_type.clone(), &options).unwrap();
        for value in [4, 1, 3, 2, 4] {
            writer.write(Some(&Datum::Int(value))).unwrap();
        }

        let serialized = writer.serialized_bytes().unwrap();
        assert_eq!(serialized, bytes(JAVA_INT_MULTIBLOCK_V2));
        let reader = BitmapFileIndexReader::try_new(data_type.clone(), serialized).unwrap();
        let blocks = match &reader.index {
            BitmapIndex::V2(index) => &index.blocks,
            BitmapIndex::V1(_) => panic!("expected V2 index"),
        };
        assert_eq!(blocks.len(), 4);
        assert_eq!(
            evaluate(&reader, &data_type, PredicateOperator::Eq, &[Datum::Int(4)]),
            selection([0, 4])
        );
        assert_eq!(
            evaluate(
                &reader,
                &data_type,
                PredicateOperator::In,
                &[Datum::Int(1), Datum::Int(2), Datum::Int(3)]
            ),
            selection([1, 2, 3])
        );
    }

    #[test]
    fn test_v2_writer_multiple_bitmap_bodies_across_index_blocks() {
        let data_type = DataType::Int(IntType::new());
        let mut options = Options::new();
        options.set("index-block-size", "28");
        let mut writer =
            writer::BitmapFileIndexWriter::try_new(data_type.clone(), &options).unwrap();
        for value in [
            Some(2),
            Some(1),
            None,
            Some(3),
            Some(2),
            None,
            Some(1),
            Some(3),
        ] {
            let datum = value.map(Datum::Int);
            writer.write(datum.as_ref()).unwrap();
        }

        let serialized = writer.serialized_bytes().unwrap();
        assert_eq!(serialized, bytes(JAVA_INT_MULTIPLE_BODIES_MULTIBLOCK_V2));
        let reader = BitmapFileIndexReader::try_new(data_type.clone(), serialized).unwrap();
        let blocks = match &reader.index {
            BitmapIndex::V2(index) => &index.blocks,
            BitmapIndex::V1(_) => panic!("expected V2 index"),
        };
        assert_eq!(blocks.len(), 2);
        assert_eq!(
            evaluate(&reader, &data_type, PredicateOperator::Eq, &[Datum::Int(1)]),
            selection([1, 6])
        );
        assert_eq!(
            evaluate(&reader, &data_type, PredicateOperator::Eq, &[Datum::Int(2)]),
            selection([0, 4])
        );
        assert_eq!(
            evaluate(&reader, &data_type, PredicateOperator::Eq, &[Datum::Int(3)]),
            selection([3, 7])
        );
        assert_eq!(
            evaluate(&reader, &data_type, PredicateOperator::IsNull, &[]),
            selection([2, 5])
        );
    }

    #[test]
    fn test_v2_writer_variable_string_index_block_boundary() {
        let data_type = DataType::VarChar(VarCharType::new(20).unwrap());
        let mut options = Options::new();
        options.set("index-block-size", "33");
        let mut writer =
            writer::BitmapFileIndexWriter::try_new(data_type.clone(), &options).unwrap();
        for value in ["bbbb", "a", "cc", "a"] {
            writer
                .write(Some(&Datum::String(value.to_string())))
                .unwrap();
        }

        let serialized = writer.serialized_bytes().unwrap();
        assert_eq!(serialized, bytes(JAVA_STRING_BLOCK_BOUNDARY_V2));
        let reader = BitmapFileIndexReader::try_new(data_type.clone(), serialized).unwrap();
        let blocks = match &reader.index {
            BitmapIndex::V2(index) => &index.blocks,
            BitmapIndex::V1(_) => panic!("expected V2 index"),
        };
        // 4-byte header + 13-byte "a" entry + 16-byte "bbbb" entry.
        assert_eq!(blocks.len(), 2);
        assert_eq!(
            evaluate(
                &reader,
                &data_type,
                PredicateOperator::Eq,
                &[Datum::String("a".to_string())]
            ),
            selection([1, 3])
        );
        assert_eq!(
            evaluate(
                &reader,
                &data_type,
                PredicateOperator::In,
                &[
                    Datum::String("bbbb".to_string()),
                    Datum::String("cc".to_string())
                ]
            ),
            selection([0, 2])
        );
    }

    #[test]
    fn test_v2_writer_rejects_invalid_config_and_unsupported_type() {
        let data_type = DataType::Int(IntType::new());

        let mut options = Options::new();
        options.set("version", "2");
        options.set("index-block-size", "16 kb");
        assert!(writer::BitmapFileIndexWriter::try_new(data_type.clone(), &options).is_ok());

        for version in ["invalid", "256"] {
            let mut options = Options::new();
            options.set("version", version);
            assert!(matches!(
                writer::BitmapFileIndexWriter::try_new(data_type.clone(), &options),
                Err(Error::ConfigInvalid { .. })
            ));
        }
        for version in ["1", "3"] {
            let mut options = Options::new();
            options.set("version", version);
            assert!(matches!(
                writer::BitmapFileIndexWriter::try_new(data_type.clone(), &options),
                Err(Error::Unsupported { .. })
            ));
        }
        for block_size in ["invalid", "0", "15", "9223372036854775807 tb"] {
            let mut options = Options::new();
            options.set("index-block-size", block_size);
            assert!(matches!(
                writer::BitmapFileIndexWriter::try_new(data_type.clone(), &options),
                Err(Error::ConfigInvalid { .. })
            ));
        }

        let mut options = Options::new();
        options.set("index-block-size", "16");
        assert!(matches!(
            writer::BitmapFileIndexWriter::try_new(DataType::BigInt(BigIntType::new()), &options),
            Err(Error::ConfigInvalid { .. })
        ));

        assert!(matches!(
            writer::BitmapFileIndexWriter::try_new(
                DataType::Binary(BinaryType::new(4).unwrap()),
                &Options::new()
            ),
            Err(Error::Unsupported { .. })
        ));
    }

    #[test]
    fn test_java_v1_v2_golden_payloads_and_predicates() {
        for fixture in fixtures() {
            let v1 = BitmapFileIndexReader::try_new(fixture.data_type.clone(), bytes(fixture.v1))
                .unwrap_or_else(|error| panic!("{} V1 failed: {error}", fixture.name));
            let v2 = BitmapFileIndexReader::try_new(fixture.data_type.clone(), bytes(fixture.v2))
                .unwrap_or_else(|error| panic!("{} V2 failed: {error}", fixture.name));

            let cases = [
                (
                    PredicateOperator::Eq,
                    vec![fixture.repeated.clone()],
                    selection([0, 3]),
                ),
                (
                    PredicateOperator::Eq,
                    vec![fixture.singleton.clone()],
                    selection([1]),
                ),
                (
                    PredicateOperator::In,
                    vec![fixture.repeated.clone(), fixture.singleton.clone()],
                    selection([0, 1, 3]),
                ),
                (
                    PredicateOperator::NotEq,
                    vec![fixture.repeated.clone()],
                    selection([1, 2, 4]),
                ),
                (
                    PredicateOperator::NotIn,
                    vec![fixture.repeated.clone(), fixture.singleton.clone()],
                    selection([2, 4]),
                ),
                (PredicateOperator::IsNull, vec![], selection([2, 4])),
                (PredicateOperator::IsNotNull, vec![], selection([0, 1, 3])),
            ];

            for (operator, literals, expected) in cases {
                let v1_result = evaluate(&v1, &fixture.data_type, operator, &literals);
                let v2_result = evaluate(&v2, &fixture.data_type, operator, &literals);
                assert_eq!(v1_result, expected, "{} V1 {operator}", fixture.name);
                assert_eq!(v2_result, expected, "{} V2 {operator}", fixture.name);
                assert_eq!(v1_result, v2_result, "{} {operator}", fixture.name);
            }
        }
    }

    #[test]
    fn test_java_floating_v1_v2_golden_payloads_and_predicates() {
        let fixtures = [
            (
                "float",
                DataType::Float(FloatType::new()),
                JAVA_FLOAT_V1,
                JAVA_FLOAT_V2,
                Datum::Float(-1.5),
                Datum::Float(0.0),
                Datum::Float(-0.0),
                Datum::Float(f32::from_bits(0xffa1_2345)),
            ),
            (
                "double",
                DataType::Double(DoubleType::new()),
                JAVA_DOUBLE_V1,
                JAVA_DOUBLE_V2,
                Datum::Double(-1.5),
                Datum::Double(0.0),
                Datum::Double(-0.0),
                Datum::Double(f64::from_bits(0xfff0_1234_5678_9abc)),
            ),
        ];

        for (name, data_type, v1, v2, normal, positive_zero, negative_zero, nan) in fixtures {
            let v1 = BitmapFileIndexReader::try_new(data_type.clone(), bytes(v1))
                .unwrap_or_else(|error| panic!("{name} V1 failed: {error}"));
            let v2 = BitmapFileIndexReader::try_new(data_type.clone(), bytes(v2))
                .unwrap_or_else(|error| panic!("{name} V2 failed: {error}"));
            let cases = [
                (
                    PredicateOperator::Eq,
                    vec![normal.clone()],
                    selection([0, 5]),
                ),
                (
                    PredicateOperator::Eq,
                    vec![positive_zero.clone()],
                    selection([1, 2, 6, 7]),
                ),
                (
                    PredicateOperator::Eq,
                    vec![negative_zero.clone()],
                    selection([1, 2, 6, 7]),
                ),
                (PredicateOperator::Eq, vec![nan.clone()], selection([3, 8])),
                (
                    PredicateOperator::In,
                    vec![normal.clone(), positive_zero.clone(), nan.clone()],
                    selection([0, 1, 2, 3, 5, 6, 7, 8]),
                ),
                (
                    PredicateOperator::NotEq,
                    vec![positive_zero.clone()],
                    selection([0, 3, 4, 5, 8, 9]),
                ),
                (
                    PredicateOperator::NotEq,
                    vec![nan.clone()],
                    selection(0..10),
                ),
                (
                    PredicateOperator::NotIn,
                    vec![normal.clone(), nan.clone()],
                    selection([1, 2, 3, 4, 6, 7, 8, 9]),
                ),
                (PredicateOperator::IsNull, vec![], selection([4, 9])),
                (
                    PredicateOperator::IsNotNull,
                    vec![],
                    selection([0, 1, 2, 3, 5, 6, 7, 8]),
                ),
            ];

            for (operator, literals, expected) in cases {
                let v1_result = evaluate(&v1, &data_type, operator, &literals);
                let v2_result = evaluate(&v2, &data_type, operator, &literals);
                assert_eq!(v1_result, expected, "{name} V1 {operator}");
                assert_eq!(v2_result, expected, "{name} V2 {operator}");
                assert_eq!(v1_result, v2_result, "{name} {operator}");
            }
        }
    }

    #[test]
    fn test_timestamp_precision_boundary_and_validation() {
        let timestamp = Datum::Timestamp {
            millis: -1,
            nanos: 999_999,
        };
        assert_eq!(
            BitmapValueCodec::try_new(&DataType::Timestamp(TimestampType::new(3).unwrap()))
                .unwrap()
                .value(&timestamp)
                .unwrap(),
            BitmapValue::Timestamp(-1)
        );
        assert_eq!(
            BitmapValueCodec::try_new(&DataType::Timestamp(TimestampType::new(4).unwrap()))
                .unwrap()
                .value(&timestamp)
                .unwrap(),
            BitmapValue::Timestamp(-1)
        );
        assert_eq!(
            BitmapValueCodec::try_new(&DataType::Timestamp(TimestampType::new(9).unwrap()))
                .unwrap()
                .value(&Datum::Timestamp {
                    millis: 1_700_000_000_123,
                    nanos: 456_999,
                })
                .unwrap(),
            BitmapValue::Timestamp(1_700_000_000_123_456)
        );

        for invalid in [
            Datum::Timestamp {
                millis: 0,
                nanos: -1,
            },
            Datum::Timestamp {
                millis: 0,
                nanos: 1_000_000,
            },
            Datum::Timestamp {
                millis: i64::MAX,
                nanos: 0,
            },
        ] {
            assert!(BitmapValueCodec::TimestampMicros.value(&invalid).is_err());
        }
    }

    #[test]
    fn test_v2_multiple_index_blocks() {
        let data_type = DataType::Int(IntType::new());
        let reader =
            BitmapFileIndexReader::try_new(data_type.clone(), bytes(JAVA_INT_MULTIBLOCK_V2))
                .unwrap();

        assert_eq!(
            evaluate(&reader, &data_type, PredicateOperator::Eq, &[Datum::Int(4)]),
            selection([0, 4])
        );
        assert_eq!(
            evaluate(
                &reader,
                &data_type,
                PredicateOperator::In,
                &[Datum::Int(1), Datum::Int(2), Datum::Int(3)]
            ),
            selection([1, 2, 3])
        );
        assert_eq!(
            evaluate(&reader, &data_type, PredicateOperator::IsNull, &[]),
            selection([])
        );
        assert_eq!(
            evaluate(&reader, &data_type, PredicateOperator::IsNotNull, &[]),
            selection(0..5)
        );
    }

    #[test]
    fn test_singleton_null_and_empty_payloads() {
        let data_type = DataType::Int(IntType::new());
        for payload in [JAVA_SINGLETON_NULL_V1, JAVA_SINGLETON_NULL_V2] {
            let reader = BitmapFileIndexReader::try_new(data_type.clone(), bytes(payload)).unwrap();
            assert_eq!(
                evaluate(&reader, &data_type, PredicateOperator::IsNull, &[]),
                selection([2])
            );
            assert_eq!(
                evaluate(&reader, &data_type, PredicateOperator::IsNotNull, &[]),
                selection([0, 1])
            );
        }

        for payload in [JAVA_EMPTY_V1, JAVA_EMPTY_V2] {
            let reader = BitmapFileIndexReader::try_new(data_type.clone(), bytes(payload)).unwrap();
            for (operator, literals) in [
                (PredicateOperator::In, Vec::new()),
                (PredicateOperator::NotIn, Vec::new()),
                (PredicateOperator::IsNull, Vec::new()),
                (PredicateOperator::IsNotNull, Vec::new()),
            ] {
                assert_eq!(
                    evaluate(&reader, &data_type, operator, &literals),
                    selection([])
                );
            }
        }
    }

    #[test]
    fn test_unsupported_predicates_and_invalid_literals_remain() {
        let data_type = DataType::Int(IntType::new());
        let reader = BitmapFileIndexReader::try_new(data_type.clone(), bytes(JAVA_INT_V2)).unwrap();

        for (operator, literals) in [
            (PredicateOperator::Eq, vec![]),
            (PredicateOperator::Eq, vec![Datum::Int(1), Datum::Int(2)]),
            (PredicateOperator::IsNull, vec![Datum::Int(1)]),
            (PredicateOperator::Gt, vec![Datum::Int(1)]),
            (PredicateOperator::Eq, vec![Datum::Long(42)]),
        ] {
            assert_eq!(
                evaluate(&reader, &data_type, operator, &literals),
                FileIndexResult::Remain
            );
        }
        assert_eq!(
            evaluate(
                &reader,
                &DataType::BigInt(BigIntType::new()),
                PredicateOperator::Eq,
                &[Datum::Long(42)]
            ),
            FileIndexResult::Remain
        );
    }

    fn assert_invalid(data_type: &DataType, payload: impl Into<Bytes>) {
        assert!(matches!(
            BitmapFileIndexReader::try_new(data_type.clone(), payload.into()),
            Err(Error::FileIndexFormatInvalid { .. })
        ));
    }

    fn assert_evaluation_invalid(
        reader: &BitmapFileIndexReader,
        data_type: &DataType,
        operator: PredicateOperator,
        literals: &[Datum],
    ) {
        assert!(matches!(
            reader.try_evaluate(data_type, operator, literals),
            Err(Error::FileIndexFormatInvalid { .. })
        ));
    }

    fn overwrite_i32(bytes: &mut [u8], offset: usize, value: i32) {
        bytes[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
    }

    fn v2_singleton_int_fixture(value_count: usize, entries_per_block: usize) -> Bytes {
        assert!(value_count > 0 && entries_per_block > 0);
        let mut blocks = Vec::new();
        let mut secondary = Vec::new();
        let mut block_offset = 0_i32;

        for start in (0..value_count).step_by(entries_per_block) {
            let end = (start + entries_per_block).min(value_count);
            secondary.push((start as i32, block_offset));

            let mut block = Vec::new();
            block.extend_from_slice(&((end - start) as i32).to_be_bytes());
            for position in start..end {
                block.extend_from_slice(&(position as i32).to_be_bytes());
                block.extend_from_slice(&(-1_i32 - position as i32).to_be_bytes());
                block.extend_from_slice(&(-1_i32).to_be_bytes());
            }
            block_offset += block.len() as i32;
            blocks.push(block);
        }

        let mut payload = vec![VERSION_2];
        payload.extend_from_slice(&(value_count as i32).to_be_bytes());
        payload.extend_from_slice(&(value_count as i32).to_be_bytes());
        payload.push(0);
        payload.extend_from_slice(&(secondary.len() as i32).to_be_bytes());
        for (first_key, offset) in secondary {
            payload.extend_from_slice(&first_key.to_be_bytes());
            payload.extend_from_slice(&offset.to_be_bytes());
        }
        payload.extend_from_slice(&block_offset.to_be_bytes());
        for block in blocks {
            payload.extend_from_slice(&block);
        }
        Bytes::from(payload)
    }

    #[test]
    fn test_v2_keeps_high_cardinality_dictionary_lazy() {
        let data_type = DataType::Int(IntType::new());
        let value_count = 10_000;
        let entries_per_block = 128;
        let payload = v2_singleton_int_fixture(value_count, entries_per_block);
        let payload_ptr = payload.as_ptr();
        let reader = BitmapFileIndexReader::try_new(data_type.clone(), payload.clone()).unwrap();
        let blocks = match &reader.index {
            BitmapIndex::V2(index) => &index.blocks,
            BitmapIndex::V1(_) => panic!("expected V2 index"),
        };
        assert_eq!(reader.serialized.as_ptr(), payload_ptr);
        assert_eq!(blocks.len(), value_count.div_ceil(entries_per_block));
        assert_eq!(
            evaluate(
                &reader,
                &data_type,
                PredicateOperator::In,
                &[Datum::Int(0), Datum::Int(5_000), Datum::Int(9_999)]
            ),
            selection([0, 5_000, 9_999])
        );

        let last_block_start = blocks.last().unwrap().range.start;
        let mut corrupted = payload.to_vec();
        overwrite_i32(&mut corrupted, last_block_start, 0);
        let corrupted =
            BitmapFileIndexReader::try_new(data_type.clone(), Bytes::from(corrupted)).unwrap();
        assert_eq!(
            evaluate(
                &corrupted,
                &data_type,
                PredicateOperator::Eq,
                &[Datum::Int(0)]
            ),
            selection([0])
        );
        assert_evaluation_invalid(
            &corrupted,
            &data_type,
            PredicateOperator::Eq,
            &[Datum::Int(9_999)],
        );
    }

    #[test]
    fn test_rejects_invalid_versions_and_every_truncated_prefix() {
        let data_type = DataType::Int(IntType::new());
        for payload in [vec![], vec![0], vec![3], vec![255]] {
            assert_invalid(&data_type, Bytes::from(payload));
        }

        for valid in [
            hex::decode(JAVA_INT_V1).unwrap(),
            hex::decode(JAVA_INT_V2).unwrap(),
        ] {
            for length in 0..valid.len() {
                let payload = Bytes::copy_from_slice(&valid[..length]);
                match BitmapFileIndexReader::try_new(data_type.clone(), payload) {
                    Err(Error::FileIndexFormatInvalid { .. }) => {}
                    Err(error) => panic!("unexpected error for prefix {length}: {error}"),
                    Ok(reader) => {
                        let null_result =
                            reader.try_evaluate(&data_type, PredicateOperator::IsNull, &[]);
                        let value_result = reader.try_evaluate(
                            &data_type,
                            PredicateOperator::Eq,
                            &[Datum::Int(-123_456_789)],
                        );
                        assert!(
                            matches!(null_result, Err(Error::FileIndexFormatInvalid { .. }))
                                || matches!(
                                    value_result,
                                    Err(Error::FileIndexFormatInvalid { .. })
                                ),
                            "truncated prefix {length} was accepted by every accessed entry"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn test_rejects_invalid_v1_offsets_and_corrupt_bitmaps() {
        let data_type = DataType::Int(IntType::new());
        let valid = hex::decode(JAVA_INT_V1).unwrap();

        let mut singleton_out_of_range = valid.clone();
        overwrite_i32(&mut singleton_out_of_range, 18, -6);
        assert_invalid(&data_type, Bytes::from(singleton_out_of_range));

        let mut offset_out_of_range = valid.clone();
        overwrite_i32(&mut offset_out_of_range, 26, 40);
        assert_invalid(&data_type, Bytes::from(offset_out_of_range));

        let mut offsets_not_increasing = valid.clone();
        overwrite_i32(&mut offsets_not_increasing, 10, 21);
        assert_invalid(&data_type, Bytes::from(offsets_not_increasing));

        let mut corrupt_bitmap = valid.clone();
        corrupt_bitmap[30] = 0;
        let reader =
            BitmapFileIndexReader::try_new(data_type.clone(), Bytes::from(corrupt_bitmap)).unwrap();
        assert_eq!(
            evaluate(
                &reader,
                &data_type,
                PredicateOperator::Eq,
                &[Datum::Int(-123_456_789)]
            ),
            selection([0, 3])
        );
        assert_evaluation_invalid(&reader, &data_type, PredicateOperator::IsNull, &[]);
        assert_eq!(
            evaluate(&reader, &data_type, PredicateOperator::IsNull, &[]),
            FileIndexResult::Remain
        );

        let mut trailing = valid;
        trailing.push(0);
        let reader =
            BitmapFileIndexReader::try_new(data_type.clone(), Bytes::from(trailing)).unwrap();
        assert_evaluation_invalid(
            &reader,
            &data_type,
            PredicateOperator::Eq,
            &[Datum::Int(-123_456_789)],
        );
    }

    #[test]
    fn test_rejects_invalid_v2_offsets_lengths_and_corrupt_bitmaps() {
        let data_type = DataType::Int(IntType::new());
        let valid = hex::decode(JAVA_INT_V2).unwrap();

        let mut block_offset = valid.clone();
        overwrite_i32(&mut block_offset, 26, 1);
        assert_invalid(&data_type, Bytes::from(block_offset));

        let mut body_offset = valid.clone();
        overwrite_i32(&mut body_offset, 30, 100);
        assert_invalid(&data_type, Bytes::from(body_offset));

        let mut bitmap_length = valid.clone();
        overwrite_i32(&mut bitmap_length, 46, 21);
        let reader =
            BitmapFileIndexReader::try_new(data_type.clone(), Bytes::from(bitmap_length)).unwrap();
        assert_evaluation_invalid(
            &reader,
            &data_type,
            PredicateOperator::Eq,
            &[Datum::Int(-123_456_789)],
        );

        let mut singleton_length = valid.clone();
        overwrite_i32(&mut singleton_length, 58, 0);
        let reader =
            BitmapFileIndexReader::try_new(data_type.clone(), Bytes::from(singleton_length))
                .unwrap();
        assert_evaluation_invalid(
            &reader,
            &data_type,
            PredicateOperator::Eq,
            &[Datum::Int(42)],
        );

        let mut null_singleton_out_of_range = valid.clone();
        overwrite_i32(&mut null_singleton_out_of_range, 10, -6);
        assert_invalid(&data_type, Bytes::from(null_singleton_out_of_range));

        let mut corrupt_bitmap = valid.clone();
        corrupt_bitmap[62] = 0;
        let reader =
            BitmapFileIndexReader::try_new(data_type.clone(), Bytes::from(corrupt_bitmap)).unwrap();
        assert_eq!(
            evaluate(
                &reader,
                &data_type,
                PredicateOperator::Eq,
                &[Datum::Int(-123_456_789)]
            ),
            selection([0, 3])
        );
        assert_evaluation_invalid(&reader, &data_type, PredicateOperator::IsNull, &[]);
    }
}
