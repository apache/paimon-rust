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

use std::collections::BTreeMap;

use bytes::{BufMut, Bytes};
use roaring::RoaringBitmap;

use super::{RangeValue, RangeValueCodec, VERSION_1};
use crate::common::options::parse_memory_size;
use crate::common::Options;
use crate::file_index::file_index_writer::FileIndexWriter;
use crate::spec::{DataType, Datum};
use crate::{Error, Result};

/// Java V1 writer for boolean, numeric, string, date and time values.
/// Decimal precision is limited to 18 and timestamp precision to 6.
pub(crate) struct RangeBitmapFileIndexWriter {
    codec: RangeValueCodec,
    chunk_size: usize,
    row_count: u32,
    bitmaps: BTreeMap<RangeValue, RoaringBitmap>,
}

impl RangeBitmapFileIndexWriter {
    pub(crate) fn try_new(data_type: DataType, options: &Options) -> Result<Self> {
        let codec = RangeValueCodec::try_new(&data_type)?;
        let default = match codec {
            RangeValueCodec::Boolean | RangeValueCodec::TinyInt | RangeValueCodec::SmallInt => "0b",
            _ => "16kb",
        };
        let raw = options
            .get("chunk-size")
            .map(String::as_str)
            .unwrap_or(default);
        let size = parse_memory_size(raw).map_err(|error| Error::ConfigInvalid {
            message: format!("Invalid range-bitmap chunk-size '{raw}': {error:?}"),
        })?;
        if !(0..=i64::from(i32::MAX)).contains(&size) {
            return Err(Error::ConfigInvalid {
                message: "Range-bitmap chunk-size must be between 0 and 2147483647 bytes"
                    .to_string(),
            });
        }
        Ok(Self {
            codec,
            chunk_size: size as usize,
            row_count: 0,
            bitmaps: BTreeMap::new(),
        })
    }

    fn dictionary(&self) -> Result<Vec<u8>> {
        let mut offsets = Vec::new();
        let mut chunks = Vec::new();
        let mut keys = Vec::new();
        let mut values = self.bitmaps.keys().enumerate().peekable();
        while let Some((code, first)) = values.next() {
            put_count(&mut offsets, chunks.len())?;
            chunks.put_u8(VERSION_1);
            write_value(&mut chunks, first)?;
            put_count(&mut chunks, code)?;
            put_count(&mut chunks, keys.len())?;

            // Java stores the first key in the chunk header, outside its size budget.
            let mut chunk_keys = Vec::new();
            let mut key_offsets = Vec::new();
            let mut count = 0;
            while let Some((_, value)) = values.peek() {
                let mut encoded = Vec::new();
                write_value(&mut encoded, value)?;
                if encoded.len() > self.chunk_size - chunk_keys.len()
                    || (self.codec.fixed_length().is_none()
                        && key_offsets.len() + 4 > self.chunk_size)
                {
                    break;
                }
                if self.codec.fixed_length().is_none() {
                    put_count(&mut key_offsets, chunk_keys.len())?;
                }
                chunk_keys.extend_from_slice(&encoded);
                count += 1;
                values.next();
            }
            put_count(&mut chunks, count)?;
            if let Some(length) = self.codec.fixed_length() {
                put_count(&mut chunks, chunk_keys.len())?;
                put_count(&mut chunks, length)?;
            } else {
                put_count(&mut chunks, key_offsets.len())?;
                put_count(&mut chunks, chunk_keys.len())?;
            }
            keys.extend_from_slice(&key_offsets);
            keys.extend_from_slice(&chunk_keys);
            checked_count(keys.len())?;
        }
        let mut result = Vec::new();
        result.put_i32(13);
        result.put_u8(VERSION_1);
        put_count(&mut result, offsets.len() / 4)?;
        put_count(&mut result, offsets.len())?;
        put_count(&mut result, chunks.len())?;
        result.extend_from_slice(&offsets);
        result.extend_from_slice(&chunks);
        result.extend_from_slice(&keys);
        checked_count(result.len())?;
        Ok(result)
    }

    fn bsi(&self) -> Result<Vec<u8>> {
        // Java sign-extends cardinality - 1 to a long: an empty dictionary has 64 slices.
        let width = if self.bitmaps.is_empty() {
            64
        } else {
            (usize::BITS - (self.bitmaps.len() - 1).leading_zeros()).max(1) as usize
        };
        let mut existing = RoaringBitmap::new();
        let mut slices = vec![RoaringBitmap::new(); width];
        for (code, rows) in self.bitmaps.values().enumerate() {
            existing |= rows;
            let mut bits = code;
            while bits != 0 {
                slices[bits.trailing_zeros() as usize] |= rows;
                bits &= bits - 1;
            }
        }
        let existing = serialize_bitmap(existing)?;
        let mut indexes = Vec::new();
        let mut body = Vec::new();
        for slice in slices {
            let bytes = serialize_bitmap(slice)?;
            put_count(&mut indexes, body.len())?;
            put_count(&mut indexes, bytes.len())?;
            body.extend_from_slice(&bytes);
        }
        let mut result = Vec::new();
        put_count(&mut result, 10 + indexes.len())?;
        result.put_u8(VERSION_1);
        result.put_u8(width as u8);
        put_count(&mut result, existing.len())?;
        put_count(&mut result, indexes.len())?;
        result.extend_from_slice(&indexes);
        result.extend_from_slice(&existing);
        result.extend_from_slice(&body);
        checked_count(result.len())?;
        Ok(result)
    }
}

impl FileIndexWriter for RangeBitmapFileIndexWriter {
    fn write(&mut self, datum: Option<&Datum>) -> Result<()> {
        if self.row_count == i32::MAX as u32 {
            return Err(Error::DataInvalid {
                message: "Range-bitmap row count exceeds i32::MAX".to_string(),
                source: None,
            });
        }
        if let Some(value) = datum.map(|datum| self.codec.value(datum)).transpose()? {
            self.bitmaps
                .entry(value)
                .or_default()
                .insert(self.row_count);
        }
        self.row_count += 1;
        Ok(())
    }

    fn serialized_bytes(&mut self) -> Result<Bytes> {
        let dictionary = self.dictionary()?;
        let bsi = self.bsi()?;
        let mut header = Vec::new();
        header.put_u8(VERSION_1);
        header.put_u32(self.row_count);
        put_count(&mut header, self.bitmaps.len())?;
        if let Some((min, _)) = self.bitmaps.first_key_value() {
            write_value(&mut header, min)?;
            write_value(&mut header, self.bitmaps.last_key_value().unwrap().0)?;
        }
        put_count(&mut header, dictionary.len())?;
        let mut result = Vec::new();
        put_count(&mut result, header.len())?;
        result.extend_from_slice(&header);
        result.extend_from_slice(&dictionary);
        result.extend_from_slice(&bsi);
        checked_count(result.len())?;
        Ok(Bytes::from(result))
    }

    fn empty(&self) -> bool {
        self.row_count == 0
    }
}

fn checked_count(value: usize) -> Result<i32> {
    i32::try_from(value).map_err(|_| Error::DataInvalid {
        message: "Range-bitmap size exceeds i32::MAX".to_string(),
        source: None,
    })
}

fn put_count(output: &mut Vec<u8>, value: usize) -> Result<()> {
    output.put_i32(checked_count(value)?);
    Ok(())
}

fn write_value(output: &mut Vec<u8>, value: &RangeValue) -> Result<()> {
    match value {
        RangeValue::Boolean(value) => output.put_u8(u8::from(*value)),
        RangeValue::TinyInt(value) => output.put_i8(*value),
        RangeValue::SmallInt(value) => output.put_i16(*value),
        RangeValue::Int(value) | RangeValue::Date(value) | RangeValue::Time(value) => {
            output.put_i32(*value)
        }
        RangeValue::BigInt(value)
        | RangeValue::Decimal(value)
        | RangeValue::Timestamp(value)
        | RangeValue::LocalZonedTimestamp(value) => output.put_i64(*value),
        RangeValue::Float(value) => output.put_u32(value.0),
        RangeValue::Double(value) => output.put_u64(value.0),
        RangeValue::String(value) => {
            put_count(output, value.len())?;
            output.extend_from_slice(value.as_bytes());
        }
    }
    Ok(())
}

fn serialize_bitmap(mut bitmap: RoaringBitmap) -> Result<Vec<u8>> {
    bitmap.optimize();
    let mut bytes = Vec::with_capacity(bitmap.serialized_size());
    bitmap
        .serialize_into(&mut bytes)
        .map_err(|error| Error::DataInvalid {
            message: format!("Failed to serialize range-bitmap: {error}"),
            source: None,
        })?;
    Ok(bytes)
}

#[cfg(test)]
mod tests;
