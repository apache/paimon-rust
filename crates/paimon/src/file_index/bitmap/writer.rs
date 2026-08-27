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

use std::collections::HashMap;
use std::ops::Range;

use bytes::{BufMut, Bytes, BytesMut};
use roaring::RoaringBitmap;

use crate::common::options::parse_memory_size;
use crate::common::Options;
use crate::spec::{DataType, Datum};
use crate::{Error, Result};

use super::{format_invalid, BitmapValue, BitmapValueCodec, VERSION_2};

const VERSION_OPTION: &str = "version";
const INDEX_BLOCK_SIZE_OPTION: &str = "index-block-size";
const DEFAULT_INDEX_BLOCK_SIZE: &str = "16kb";
const INDEX_BLOCK_HEADER_SIZE: usize = 4;
const INDEX_ENTRY_FIXED_SIZE: usize = 8;

/// Writer for Java-compatible Bitmap V2 payloads.
pub(crate) struct BitmapFileIndexWriter {
    codec: BitmapValueCodec,
    index_block_size: usize,
    row_count: u32,
    null_bitmap: RoaringBitmap,
    bitmaps: HashMap<BitmapValue, RoaringBitmap>,
}

impl BitmapFileIndexWriter {
    pub(crate) fn try_new(data_type: DataType, options: &Options) -> Result<Self> {
        let codec = BitmapValueCodec::try_new(&data_type)?;
        validate_version(options)?;
        let index_block_size = parse_index_block_size(options, codec)?;
        Ok(Self {
            codec,
            index_block_size,
            row_count: 0,
            null_bitmap: RoaringBitmap::new(),
            bitmaps: HashMap::new(),
        })
    }

    pub(crate) fn write(&mut self, datum: Option<&Datum>) -> Result<()> {
        if self.row_count == i32::MAX as u32 {
            return Err(Error::DataInvalid {
                message: "Bitmap row count exceeds i32::MAX".to_string(),
                source: None,
            });
        }

        let value = datum.map(|datum| self.codec.value(datum)).transpose()?;
        match value {
            Some(value) => {
                self.bitmaps
                    .entry(value)
                    .or_default()
                    .insert(self.row_count);
            }
            None => {
                self.null_bitmap.insert(self.row_count);
            }
        }
        self.row_count += 1;
        Ok(())
    }

    pub(crate) fn serialized_bytes(&mut self) -> Result<Bytes> {
        let null_bytes = serialize_bitmap(&mut self.null_bitmap)?;
        let mut body = Vec::new();
        let null_entry = if self.null_bitmap.is_empty() {
            None
        } else if self.null_bitmap.len() == 1 {
            Some((
                singleton_offset(self.null_bitmap.min().unwrap())?,
                usize_to_i32(null_bytes.len(), "null bitmap length")?,
            ))
        } else {
            let length = usize_to_i32(null_bytes.len(), "null bitmap length")?;
            body.extend_from_slice(&null_bytes);
            Some((0, length))
        };

        let mut bitmaps = self.bitmaps.iter_mut().collect::<Vec<_>>();
        bitmaps.sort_unstable_by_key(|(key, _)| *key);

        let mut entries = Vec::with_capacity(bitmaps.len());
        for (key, bitmap) in bitmaps {
            let (offset, length) = if bitmap.len() == 1 {
                (singleton_offset(bitmap.min().unwrap())?, -1)
            } else {
                let serialized = serialize_bitmap(bitmap)?;
                let offset = usize_to_i32(body.len(), "bitmap body offset")?;
                let length = usize_to_i32(serialized.len(), "serialized bitmap length")?;
                body.extend_from_slice(&serialized);
                (offset, length)
            };
            entries.push(SerializedEntry {
                key,
                offset,
                length,
            });
        }
        usize_to_i32(body.len(), "bitmap body length")?;

        let blocks = build_index_blocks(&entries, self.index_block_size)?;
        let mut block_offsets = Vec::with_capacity(blocks.len());
        let mut index_area_length = 0usize;
        for block in &blocks {
            block_offsets.push(index_area_length);
            index_area_length = index_area_length
                .checked_add(block_serialized_size(&entries[block.clone()])?)
                .ok_or_else(|| format_invalid("Bitmap index area length overflow"))?;
        }

        let mut output = BytesMut::new();
        output.put_u8(VERSION_2);
        output.put_i32(i32::try_from(self.row_count).map_err(|_| {
            format_invalid(format!(
                "Bitmap row count exceeds i32::MAX: {}",
                self.row_count
            ))
        })?);
        output.put_i32(usize_to_i32(entries.len(), "non-null bitmap count")?);
        output.put_u8(u8::from(null_entry.is_some()));
        if let Some((offset, length)) = null_entry {
            output.put_i32(offset);
            output.put_i32(length);
        }

        output.put_i32(usize_to_i32(blocks.len(), "bitmap index block count")?);
        for (block, offset) in blocks.iter().zip(block_offsets) {
            write_value(&mut output, entries[block.start].key)?;
            output.put_i32(usize_to_i32(offset, "bitmap index block offset")?);
        }
        output.put_i32(usize_to_i32(index_area_length, "bitmap index area length")?);

        for block in blocks {
            output.put_i32(usize_to_i32(block.len(), "bitmap index block entry count")?);
            for entry in &entries[block] {
                write_value(&mut output, entry.key)?;
                output.put_i32(entry.offset);
                output.put_i32(entry.length);
            }
        }
        output.extend_from_slice(&body);
        Ok(output.freeze())
    }
}

struct SerializedEntry<'a> {
    key: &'a BitmapValue,
    offset: i32,
    length: i32,
}

fn validate_version(options: &Options) -> Result<()> {
    let Some(raw) = options.get(VERSION_OPTION) else {
        return Ok(());
    };
    let version = raw.parse::<u8>().map_err(|error| Error::ConfigInvalid {
        message: format!("Invalid Bitmap option {VERSION_OPTION}={raw}: {error}"),
    })?;
    if version != VERSION_2 {
        return Err(Error::Unsupported {
            message: format!(
                "Bitmap writer only supports version {VERSION_2}, but found {version}"
            ),
        });
    }
    Ok(())
}

fn parse_index_block_size(options: &Options, codec: BitmapValueCodec) -> Result<usize> {
    let raw = options
        .get(INDEX_BLOCK_SIZE_OPTION)
        .map(String::as_str)
        .unwrap_or(DEFAULT_INDEX_BLOCK_SIZE);
    let size = parse_memory_size(raw).map_err(|error| Error::ConfigInvalid {
        message: format!("Invalid Bitmap option {INDEX_BLOCK_SIZE_OPTION}={raw}: {error:?}"),
    })?;
    let size = usize::try_from(size).map_err(|_| Error::ConfigInvalid {
        message: format!("Invalid Bitmap option {INDEX_BLOCK_SIZE_OPTION}={raw}: out of range"),
    })?;
    let minimum = INDEX_BLOCK_HEADER_SIZE + INDEX_ENTRY_FIXED_SIZE + minimum_value_size(codec);
    if size < minimum {
        return Err(Error::ConfigInvalid {
            message: format!(
                "Bitmap option {INDEX_BLOCK_SIZE_OPTION} must be at least {minimum} bytes for {codec:?}, but was {size}"
            ),
        });
    }
    Ok(size)
}

fn minimum_value_size(codec: BitmapValueCodec) -> usize {
    match codec {
        BitmapValueCodec::Boolean | BitmapValueCodec::TinyInt => 1,
        BitmapValueCodec::SmallInt => 2,
        BitmapValueCodec::Int
        | BitmapValueCodec::Float
        | BitmapValueCodec::Date
        | BitmapValueCodec::Time
        | BitmapValueCodec::String => 4,
        BitmapValueCodec::BigInt
        | BitmapValueCodec::Double
        | BitmapValueCodec::TimestampMillis
        | BitmapValueCodec::TimestampMicros
        | BitmapValueCodec::LocalZonedTimestampMillis
        | BitmapValueCodec::LocalZonedTimestampMicros => 8,
    }
}

fn singleton_offset(position: u32) -> Result<i32> {
    let position = i32::try_from(position)
        .map_err(|_| format_invalid(format!("Bitmap row position exceeds i32::MAX: {position}")))?;
    (-1_i32)
        .checked_sub(position)
        .ok_or_else(|| format_invalid(format!("Bitmap singleton offset overflow: {position}")))
}

fn serialize_bitmap(bitmap: &mut RoaringBitmap) -> Result<Vec<u8>> {
    bitmap.optimize();
    let mut serialized = Vec::with_capacity(bitmap.serialized_size());
    bitmap
        .serialize_into(&mut serialized)
        .map_err(|error| Error::UnexpectedError {
            message: "Failed to serialize Bitmap RoaringBitmap32".to_string(),
            source: Some(Box::new(error)),
        })?;
    Ok(serialized)
}

fn build_index_blocks(
    entries: &[SerializedEntry<'_>],
    block_size_limit: usize,
) -> Result<Vec<Range<usize>>> {
    let mut blocks = Vec::new();
    let mut block_start = 0usize;
    let mut block_size = INDEX_BLOCK_HEADER_SIZE;

    for (index, entry) in entries.iter().enumerate() {
        let entry_size = INDEX_ENTRY_FIXED_SIZE
            .checked_add(value_serialized_size(entry.key)?)
            .ok_or_else(|| format_invalid("Bitmap index entry size overflow"))?;
        let minimum_block_size = INDEX_BLOCK_HEADER_SIZE
            .checked_add(entry_size)
            .ok_or_else(|| format_invalid("Bitmap index block size overflow"))?;
        if minimum_block_size > block_size_limit {
            return Err(Error::ConfigInvalid {
                message: format!(
                    "Bitmap option {INDEX_BLOCK_SIZE_OPTION}={block_size_limit} bytes cannot fit a {minimum_block_size}-byte index block"
                ),
            });
        }
        if block_size
            .checked_add(entry_size)
            .is_none_or(|size| size > block_size_limit)
        {
            blocks.push(block_start..index);
            block_start = index;
            block_size = INDEX_BLOCK_HEADER_SIZE;
        }
        block_size = block_size
            .checked_add(entry_size)
            .ok_or_else(|| format_invalid("Bitmap index block size overflow"))?;
    }

    if block_start < entries.len() {
        blocks.push(block_start..entries.len());
    }
    Ok(blocks)
}

fn block_serialized_size(entries: &[SerializedEntry<'_>]) -> Result<usize> {
    let mut size = INDEX_BLOCK_HEADER_SIZE;
    for entry in entries {
        let value_size = value_serialized_size(entry.key)?;
        size = size
            .checked_add(INDEX_ENTRY_FIXED_SIZE)
            .and_then(|size| size.checked_add(value_size))
            .ok_or_else(|| format_invalid("Bitmap index block size overflow"))?;
    }
    Ok(size)
}

fn value_serialized_size(value: &BitmapValue) -> Result<usize> {
    match value {
        BitmapValue::Boolean(_) | BitmapValue::TinyInt(_) => Ok(1),
        BitmapValue::SmallInt(_) => Ok(2),
        BitmapValue::Int(_)
        | BitmapValue::Float(_)
        | BitmapValue::Date(_)
        | BitmapValue::Time(_) => Ok(4),
        BitmapValue::BigInt(_)
        | BitmapValue::Double(_)
        | BitmapValue::Timestamp(_)
        | BitmapValue::LocalZonedTimestamp(_) => Ok(8),
        BitmapValue::String(value) => 4usize
            .checked_add(value.len())
            .ok_or_else(|| format_invalid("Bitmap string value size overflow")),
    }
}

fn write_value(output: &mut BytesMut, value: &BitmapValue) -> Result<()> {
    match value {
        BitmapValue::Boolean(value) => output.put_u8(u8::from(*value)),
        BitmapValue::TinyInt(value) => output.put_i8(*value),
        BitmapValue::SmallInt(value) => output.put_i16(*value),
        BitmapValue::Int(value) | BitmapValue::Date(value) | BitmapValue::Time(value) => {
            output.put_i32(*value)
        }
        BitmapValue::BigInt(value)
        | BitmapValue::Timestamp(value)
        | BitmapValue::LocalZonedTimestamp(value) => output.put_i64(*value),
        BitmapValue::Float(value) => output.put_u32(value.0),
        BitmapValue::Double(value) => output.put_u64(value.0),
        BitmapValue::String(value) => {
            output.put_i32(usize_to_i32(value.len(), "Bitmap string value length")?);
            output.extend_from_slice(value.as_bytes());
        }
    }
    Ok(())
}

fn usize_to_i32(value: usize, field: &str) -> Result<i32> {
    i32::try_from(value).map_err(|_| format_invalid(format!("{field} exceeds i32::MAX: {value}")))
}
