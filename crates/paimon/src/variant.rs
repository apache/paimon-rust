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

//! Java-compatible helpers for Paimon Variant values.

use std::collections::{HashMap, HashSet};

use crate::{Error, Result};
use base64::{engine::general_purpose, Engine as _};

const BASIC_TYPE_BITS: u8 = 2;
const BASIC_TYPE_MASK: u8 = 0x3;
const TYPE_INFO_MASK: u8 = 0x3f;
const MAX_SHORT_STR_SIZE: usize = 0x3f;

const PRIMITIVE: u8 = 0;
const SHORT_STR: u8 = 1;
const OBJECT: u8 = 2;
const ARRAY: u8 = 3;

const NULL: u8 = 0;
const TRUE: u8 = 1;
const FALSE: u8 = 2;
const INT1: u8 = 3;
const INT2: u8 = 4;
const INT4: u8 = 5;
const INT8: u8 = 6;
const DOUBLE: u8 = 7;
const DECIMAL4: u8 = 8;
const DECIMAL8: u8 = 9;
const DECIMAL16: u8 = 10;
const DATE: u8 = 11;
const TIMESTAMP: u8 = 12;
const TIMESTAMP_NTZ: u8 = 13;
const FLOAT: u8 = 14;
const BINARY: u8 = 15;
const LONG_STR: u8 = 16;
const UUID: u8 = 20;

const VERSION: u8 = 1;
const VERSION_MASK: u8 = 0x0f;

const U8_MAX: usize = 0xff;
const U16_MAX: usize = 0xffff;
const U24_MAX: usize = 0xff_ffff;
const U32_SIZE: usize = 4;
const SIZE_LIMIT: usize = 128 * 1024 * 1024;
const MAX_NESTING_DEPTH: usize = 1000;

const MAX_DECIMAL4_PRECISION: u8 = 9;
const MAX_DECIMAL8_PRECISION: u8 = 18;
const MAX_DECIMAL16_PRECISION: u8 = 38;
const BINARY_SEARCH_THRESHOLD: usize = 32;

/// An owned Paimon Variant value encoded as Java-compatible `value` and `metadata` buffers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenericVariant {
    value: Vec<u8>,
    metadata: Vec<u8>,
}

impl GenericVariant {
    /// Parse a JSON string into the same binary Variant representation produced by Java Paimon.
    pub fn parse_json(json: &str) -> Result<Self> {
        JsonVariantParser::new(json).parse()
    }

    /// Build an owned Variant from existing buffers after validating the metadata header.
    pub fn from_parts(value: Vec<u8>, metadata: Vec<u8>) -> Result<Self> {
        VariantRef::new(&value, &metadata, 0)?;
        Ok(Self { value, metadata })
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }

    pub fn metadata(&self) -> &[u8] {
        &self.metadata
    }

    pub fn as_ref(&self) -> Result<VariantRef<'_>> {
        VariantRef::new(&self.value, &self.metadata, 0)
    }

    pub fn is_variant_null(&self) -> Result<bool> {
        self.as_ref()?.is_null()
    }

    pub fn get_path(&self, path: &str) -> Result<Option<VariantRef<'_>>> {
        self.as_ref()?.get_path(path)
    }

    pub fn to_json(&self) -> Result<String> {
        self.as_ref()?.to_json()
    }
}

/// A borrowed view into a Variant buffer. Sub-values share the original metadata dictionary.
#[derive(Clone, Copy, Debug)]
pub struct VariantRef<'a> {
    value: &'a [u8],
    metadata: &'a [u8],
    pos: usize,
}

impl<'a> VariantRef<'a> {
    pub fn new(value: &'a [u8], metadata: &'a [u8], pos: usize) -> Result<Self> {
        validate_payload(value, metadata)?;
        Self::new_at(value, metadata, pos)
    }

    fn new_at(value: &'a [u8], metadata: &'a [u8], pos: usize) -> Result<Self> {
        check_index(value, pos)?;
        Ok(Self {
            value,
            metadata,
            pos,
        })
    }

    pub fn kind(&self) -> Result<VariantKind> {
        value_kind(self.value, self.pos)
    }

    pub fn is_null(&self) -> Result<bool> {
        Ok(self.kind()? == VariantKind::Null)
    }

    pub fn value_slice(&self) -> Result<&'a [u8]> {
        let size = value_size(self.value, self.pos)?;
        check_index(self.value, self.pos + size - 1)?;
        Ok(&self.value[self.pos..self.pos + size])
    }

    pub fn metadata(&self) -> &'a [u8] {
        self.metadata
    }

    pub fn to_owned_variant(&self) -> Result<GenericVariant> {
        Ok(GenericVariant {
            value: self.value_slice()?.to_vec(),
            metadata: self.metadata.to_vec(),
        })
    }

    pub fn get_path(&self, path: &str) -> Result<Option<VariantRef<'a>>> {
        let mut current = *self;
        for segment in parse_path(path)? {
            match (segment, current.kind()?) {
                (PathSegment::Key(key), VariantKind::Object) => {
                    let Some(next) = current.get_field_by_key(&key)? else {
                        return Ok(None);
                    };
                    current = next;
                }
                (PathSegment::Index(index), VariantKind::Array) => {
                    let Some(next) = current.get_element_at_index(index)? else {
                        return Ok(None);
                    };
                    current = next;
                }
                _ => return Ok(None),
            }
        }
        Ok(Some(current))
    }

    pub fn get_boolean(&self) -> Result<bool> {
        get_boolean(self.value, self.pos)
    }

    pub fn get_long(&self) -> Result<i64> {
        get_long(self.value, self.pos)
    }

    pub fn get_double(&self) -> Result<f64> {
        get_double(self.value, self.pos)
    }

    pub fn get_float(&self) -> Result<f32> {
        get_float(self.value, self.pos)
    }

    pub fn get_string(&self) -> Result<String> {
        get_string(self.value, self.pos)
    }

    pub fn get_decimal(&self) -> Result<VariantDecimal> {
        get_decimal(self.value, self.pos)
    }

    pub fn to_json(&self) -> Result<String> {
        let mut out = String::new();
        write_json(self.value, self.metadata, self.pos, &mut out)?;
        Ok(out)
    }

    fn get_field_by_key(&self, key: &str) -> Result<Option<VariantRef<'a>>> {
        let layout = object_layout(self.value, self.pos)?;
        if layout.size < BINARY_SEARCH_THRESHOLD {
            for i in 0..layout.size {
                let id = read_unsigned(
                    self.value,
                    layout.id_start + layout.id_size * i,
                    layout.id_size,
                )?;
                if key == get_metadata_key(self.metadata, id)? {
                    let offset = read_unsigned(
                        self.value,
                        layout.offset_start + layout.offset_size * i,
                        layout.offset_size,
                    )?;
                    return VariantRef::new_at(
                        self.value,
                        self.metadata,
                        layout.data_start + offset,
                    )
                    .map(Some);
                }
            }
        } else {
            let mut low = 0usize;
            let mut high = layout.size;
            while low < high {
                let mid = low + (high - low) / 2;
                let id = read_unsigned(
                    self.value,
                    layout.id_start + layout.id_size * mid,
                    layout.id_size,
                )?;
                match java_string_cmp(&get_metadata_key(self.metadata, id)?, key) {
                    std::cmp::Ordering::Less => low = mid + 1,
                    std::cmp::Ordering::Greater => high = mid,
                    std::cmp::Ordering::Equal => {
                        let offset = read_unsigned(
                            self.value,
                            layout.offset_start + layout.offset_size * mid,
                            layout.offset_size,
                        )?;
                        return VariantRef::new_at(
                            self.value,
                            self.metadata,
                            layout.data_start + offset,
                        )
                        .map(Some);
                    }
                }
            }
        }
        Ok(None)
    }

    fn get_element_at_index(&self, index: usize) -> Result<Option<VariantRef<'a>>> {
        let layout = array_layout(self.value, self.pos)?;
        if index >= layout.size {
            return Ok(None);
        }
        let offset = read_unsigned(
            self.value,
            layout.offset_start + layout.offset_size * index,
            layout.offset_size,
        )?;
        VariantRef::new_at(self.value, self.metadata, layout.data_start + offset).map(Some)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VariantKind {
    Object,
    Array,
    Null,
    Boolean,
    Long,
    String,
    Double,
    Decimal,
    Date,
    Timestamp,
    TimestampNtz,
    Float,
    Binary,
    Uuid,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VariantDecimal {
    pub unscaled: i128,
    pub precision: u8,
    pub scale: i8,
}

impl VariantDecimal {
    pub fn to_plain_string(self) -> String {
        decimal_to_plain_string(self.unscaled, self.scale, true)
    }
}

pub fn validate_payload(value: &[u8], metadata: &[u8]) -> Result<()> {
    if metadata.is_empty() || (metadata[0] & VERSION_MASK) != VERSION {
        return data_invalid("Malformed Variant metadata version");
    }
    if value.len() > SIZE_LIMIT || metadata.len() > SIZE_LIMIT {
        return data_invalid("Variant value or metadata exceeds constructor size limit");
    }
    validate_metadata(metadata)?;
    let root_size = validate_value(value, metadata, 0, 0)?;
    if root_size != value.len() {
        return data_invalid("Malformed Variant root size");
    }
    Ok(())
}

fn validate_metadata(metadata: &[u8]) -> Result<()> {
    let offset_size = metadata_offset_size(metadata)?;
    let dict_size = read_unsigned(metadata, 1, offset_size)?;
    let offset_start = 1 + offset_size;
    let string_start = offset_start + (dict_size + 1) * offset_size;
    check_range(metadata, offset_start, (dict_size + 1) * offset_size)?;
    let string_size = read_unsigned(
        metadata,
        offset_start + dict_size * offset_size,
        offset_size,
    )?;
    if string_start.checked_add(string_size) != Some(metadata.len()) {
        return data_invalid("Malformed Variant metadata size");
    }

    let mut previous_offset = 0usize;
    let mut seen = HashSet::with_capacity(dict_size);
    for id in 0..dict_size {
        let offset = read_unsigned(metadata, offset_start + id * offset_size, offset_size)?;
        let next_offset =
            read_unsigned(metadata, offset_start + (id + 1) * offset_size, offset_size)?;
        if offset != previous_offset || offset > next_offset || next_offset > string_size {
            return data_invalid("Malformed Variant metadata offsets");
        }
        previous_offset = next_offset;
        let bytes = &metadata[string_start + offset..string_start + next_offset];
        let key = std::str::from_utf8(bytes).map_err(|e| Error::DataInvalid {
            message: "Malformed Variant metadata UTF-8".to_string(),
            source: Some(Box::new(e)),
        })?;
        if !seen.insert(key) {
            return data_invalid("Malformed Variant metadata duplicate key");
        }
    }
    Ok(())
}

fn validate_value(value: &[u8], metadata: &[u8], pos: usize, depth: usize) -> Result<usize> {
    if depth > MAX_NESTING_DEPTH {
        return data_invalid("Malformed Variant nesting depth");
    }

    match value_kind(value, pos)? {
        VariantKind::Object => validate_object(value, metadata, pos, depth),
        VariantKind::Array => validate_array(value, metadata, pos, depth),
        VariantKind::Null => validate_sized_value(value, pos, value_size(value, pos)?),
        VariantKind::Boolean => {
            get_boolean(value, pos)?;
            validate_sized_value(value, pos, value_size(value, pos)?)
        }
        VariantKind::Long
        | VariantKind::Date
        | VariantKind::Timestamp
        | VariantKind::TimestampNtz => {
            get_long(value, pos)?;
            validate_sized_value(value, pos, value_size(value, pos)?)
        }
        VariantKind::String => {
            get_string(value, pos)?;
            validate_sized_value(value, pos, value_size(value, pos)?)
        }
        VariantKind::Double => {
            get_double(value, pos)?;
            validate_sized_value(value, pos, value_size(value, pos)?)
        }
        VariantKind::Decimal => {
            get_decimal(value, pos)?;
            validate_sized_value(value, pos, value_size(value, pos)?)
        }
        VariantKind::Float => {
            get_float(value, pos)?;
            validate_sized_value(value, pos, value_size(value, pos)?)
        }
        VariantKind::Binary => {
            get_binary(value, pos)?;
            validate_sized_value(value, pos, value_size(value, pos)?)
        }
        VariantKind::Uuid => {
            get_uuid(value, pos)?;
            validate_sized_value(value, pos, value_size(value, pos)?)
        }
    }
}

fn validate_sized_value(value: &[u8], pos: usize, size: usize) -> Result<usize> {
    check_range(value, pos, size)?;
    Ok(size)
}

fn validate_object(value: &[u8], metadata: &[u8], pos: usize, depth: usize) -> Result<usize> {
    let layout = object_layout(value, pos)?;
    let data_size = read_unsigned(
        value,
        layout.offset_start + layout.size * layout.offset_size,
        layout.offset_size,
    )?;
    let data_end = layout
        .data_start
        .checked_add(data_size)
        .ok_or_else(|| Error::DataInvalid {
            message: "Malformed Variant object offsets".to_string(),
            source: None,
        })?;
    if data_end > value.len() {
        return data_invalid("Malformed Variant object offsets");
    }

    let mut ranges = Vec::with_capacity(layout.size);
    let mut previous_key: Option<String> = None;
    for i in 0..layout.size {
        let id = read_unsigned(value, layout.id_start + layout.id_size * i, layout.id_size)?;
        let key = get_metadata_key(metadata, id)?;
        if previous_key
            .as_deref()
            .is_some_and(|previous| java_string_cmp(previous, &key) != std::cmp::Ordering::Less)
        {
            return data_invalid("Malformed Variant object key order");
        }
        previous_key = Some(key);

        let offset = read_unsigned(
            value,
            layout.offset_start + layout.offset_size * i,
            layout.offset_size,
        )?;
        ranges.push(validate_child_range(
            value,
            metadata,
            layout.data_start,
            data_size,
            offset,
            depth + 1,
        )?);
    }

    let final_offset = read_unsigned(
        value,
        layout.offset_start + layout.offset_size * layout.size,
        layout.offset_size,
    )?;
    if final_offset != data_size {
        return data_invalid("Malformed Variant object offsets");
    }
    validate_ranges_cover_data(&mut ranges, data_size)?;
    Ok(layout.data_start - pos + data_size)
}

fn validate_array(value: &[u8], metadata: &[u8], pos: usize, depth: usize) -> Result<usize> {
    let layout = array_layout(value, pos)?;
    let data_size = read_unsigned(
        value,
        layout.offset_start + layout.size * layout.offset_size,
        layout.offset_size,
    )?;
    let data_end = layout
        .data_start
        .checked_add(data_size)
        .ok_or_else(|| Error::DataInvalid {
            message: "Malformed Variant array offsets".to_string(),
            source: None,
        })?;
    if data_end > value.len() {
        return data_invalid("Malformed Variant array offsets");
    }

    let mut previous_offset = 0usize;
    for i in 0..layout.size {
        let offset = read_unsigned(
            value,
            layout.offset_start + layout.offset_size * i,
            layout.offset_size,
        )?;
        let next_offset = read_unsigned(
            value,
            layout.offset_start + layout.offset_size * (i + 1),
            layout.offset_size,
        )?;
        validate_child_value(
            value,
            metadata,
            layout.data_start,
            data_size,
            offset,
            next_offset,
            depth + 1,
        )?;
        previous_offset = validate_offset_order(previous_offset, offset, next_offset, data_size)?;
    }

    let final_offset = read_unsigned(
        value,
        layout.offset_start + layout.offset_size * layout.size,
        layout.offset_size,
    )?;
    if final_offset != data_size || final_offset < previous_offset {
        return data_invalid("Malformed Variant array offsets");
    }
    Ok(layout.data_start - pos + data_size)
}

fn validate_child_value(
    value: &[u8],
    metadata: &[u8],
    data_start: usize,
    data_size: usize,
    offset: usize,
    next_offset: usize,
    depth: usize,
) -> Result<()> {
    if offset > next_offset || next_offset > data_size {
        return data_invalid("Malformed Variant offsets");
    }
    let child_pos = data_start
        .checked_add(offset)
        .ok_or_else(|| Error::DataInvalid {
            message: "Malformed Variant offsets".to_string(),
            source: None,
        })?;
    let child_size = validate_value(value, metadata, child_pos, depth)?;
    if child_size != next_offset - offset {
        return data_invalid("Malformed Variant child size");
    }
    Ok(())
}

fn validate_child_range(
    value: &[u8],
    metadata: &[u8],
    data_start: usize,
    data_size: usize,
    offset: usize,
    depth: usize,
) -> Result<(usize, usize)> {
    if offset > data_size {
        return data_invalid("Malformed Variant offsets");
    }
    let child_pos = data_start
        .checked_add(offset)
        .ok_or_else(|| Error::DataInvalid {
            message: "Malformed Variant offsets".to_string(),
            source: None,
        })?;
    let child_size = validate_value(value, metadata, child_pos, depth)?;
    let end = offset
        .checked_add(child_size)
        .ok_or_else(|| Error::DataInvalid {
            message: "Malformed Variant offsets".to_string(),
            source: None,
        })?;
    if end > data_size {
        return data_invalid("Malformed Variant offsets");
    }
    Ok((offset, end))
}

fn validate_ranges_cover_data(ranges: &mut [(usize, usize)], data_size: usize) -> Result<()> {
    ranges.sort_unstable_by_key(|(start, _)| *start);
    let mut expected_start = 0usize;
    for (start, end) in ranges {
        if *start != expected_start || *end < *start {
            return data_invalid("Malformed Variant offsets");
        }
        expected_start = *end;
    }
    if expected_start != data_size {
        return data_invalid("Malformed Variant offsets");
    }
    Ok(())
}

fn validate_offset_order(
    previous_offset: usize,
    offset: usize,
    next_offset: usize,
    data_size: usize,
) -> Result<usize> {
    if offset != previous_offset || offset > next_offset || next_offset > data_size {
        return data_invalid("Malformed Variant offsets");
    }
    Ok(next_offset)
}

fn data_invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(Error::DataInvalid {
        message: message.into(),
        source: None,
    })
}

fn primitive_header(type_info: u8) -> u8 {
    (type_info << BASIC_TYPE_BITS) | PRIMITIVE
}

fn short_str_header(size: usize) -> u8 {
    ((size as u8) << BASIC_TYPE_BITS) | SHORT_STR
}

fn object_header(large_size: bool, id_size: usize, offset_size: usize) -> u8 {
    (((large_size as u8) << (BASIC_TYPE_BITS + 4))
        | (((id_size - 1) as u8) << (BASIC_TYPE_BITS + 2))
        | (((offset_size - 1) as u8) << BASIC_TYPE_BITS))
        | OBJECT
}

fn array_header(large_size: bool, offset_size: usize) -> u8 {
    (((large_size as u8) << (BASIC_TYPE_BITS + 2)) | (((offset_size - 1) as u8) << BASIC_TYPE_BITS))
        | ARRAY
}

fn integer_size(value: usize) -> Result<usize> {
    if value > SIZE_LIMIT {
        return data_invalid("Variant value exceeds size limit");
    }
    Ok(if value <= U8_MAX {
        1
    } else if value <= U16_MAX {
        2
    } else if value <= U24_MAX {
        3
    } else {
        4
    })
}

fn check_index(bytes: &[u8], pos: usize) -> Result<()> {
    if pos >= bytes.len() {
        return data_invalid("Malformed Variant");
    }
    Ok(())
}

fn check_range(bytes: &[u8], pos: usize, len: usize) -> Result<()> {
    if len == 0 {
        return Ok(());
    }
    let end = pos
        .checked_add(len)
        .and_then(|v| v.checked_sub(1))
        .ok_or_else(|| Error::DataInvalid {
            message: "Malformed Variant".to_string(),
            source: None,
        })?;
    check_index(bytes, end)
}

fn write_le_at(bytes: &mut [u8], pos: usize, value: usize, num_bytes: usize) {
    for i in 0..num_bytes {
        bytes[pos + i] = ((value >> (8 * i)) & 0xff) as u8;
    }
}

fn push_signed_le(bytes: &mut Vec<u8>, value: i128, num_bytes: usize) {
    for i in 0..num_bytes {
        bytes.push(((value >> (8 * i)) & 0xff) as u8);
    }
}

fn read_unsigned(bytes: &[u8], pos: usize, num_bytes: usize) -> Result<usize> {
    check_range(bytes, pos, num_bytes)?;
    let mut result = 0usize;
    for i in 0..num_bytes {
        result |= (bytes[pos + i] as usize) << (8 * i);
    }
    Ok(result)
}

fn read_signed(bytes: &[u8], pos: usize, num_bytes: usize) -> Result<i64> {
    check_range(bytes, pos, num_bytes)?;
    let mut result = 0i64;
    for i in 0..num_bytes {
        result |= (bytes[pos + i] as i64) << (8 * i);
    }
    let shift = 64 - num_bytes * 8;
    Ok((result << shift) >> shift)
}

fn value_kind(value: &[u8], pos: usize) -> Result<VariantKind> {
    check_index(value, pos)?;
    let basic_type = value[pos] & BASIC_TYPE_MASK;
    let type_info = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
    Ok(match basic_type {
        SHORT_STR => VariantKind::String,
        OBJECT => VariantKind::Object,
        ARRAY => VariantKind::Array,
        _ => match type_info {
            NULL => VariantKind::Null,
            TRUE | FALSE => VariantKind::Boolean,
            INT1 | INT2 | INT4 | INT8 => VariantKind::Long,
            DOUBLE => VariantKind::Double,
            DECIMAL4 | DECIMAL8 | DECIMAL16 => VariantKind::Decimal,
            DATE => VariantKind::Date,
            TIMESTAMP => VariantKind::Timestamp,
            TIMESTAMP_NTZ => VariantKind::TimestampNtz,
            FLOAT => VariantKind::Float,
            BINARY => VariantKind::Binary,
            LONG_STR => VariantKind::String,
            UUID => VariantKind::Uuid,
            _ => return data_invalid(format!("Unknown primitive type in Variant: {type_info}")),
        },
    })
}

fn value_size(value: &[u8], pos: usize) -> Result<usize> {
    check_index(value, pos)?;
    let basic_type = value[pos] & BASIC_TYPE_MASK;
    let type_info = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
    match basic_type {
        SHORT_STR => Ok(1 + type_info as usize),
        OBJECT => {
            let layout = object_layout(value, pos)?;
            let data_size = read_unsigned(
                value,
                layout.offset_start + layout.size * layout.offset_size,
                layout.offset_size,
            )?;
            Ok(layout.data_start - pos + data_size)
        }
        ARRAY => {
            let layout = array_layout(value, pos)?;
            let data_size = read_unsigned(
                value,
                layout.offset_start + layout.size * layout.offset_size,
                layout.offset_size,
            )?;
            Ok(layout.data_start - pos + data_size)
        }
        _ => match type_info {
            NULL | TRUE | FALSE => Ok(1),
            INT1 => Ok(2),
            INT2 => Ok(3),
            INT4 | DATE | FLOAT => Ok(5),
            INT8 | DOUBLE | TIMESTAMP | TIMESTAMP_NTZ => Ok(9),
            DECIMAL4 => Ok(6),
            DECIMAL8 => Ok(10),
            DECIMAL16 => Ok(18),
            BINARY | LONG_STR => {
                let len = read_unsigned(value, pos + 1, U32_SIZE)?;
                Ok(1 + U32_SIZE + len)
            }
            UUID => Ok(17),
            _ => data_invalid(format!("Unknown primitive type in Variant: {type_info}")),
        },
    }
}

#[derive(Clone, Copy)]
struct ObjectLayout {
    size: usize,
    id_size: usize,
    offset_size: usize,
    id_start: usize,
    offset_start: usize,
    data_start: usize,
}

fn object_layout(value: &[u8], pos: usize) -> Result<ObjectLayout> {
    check_index(value, pos)?;
    if (value[pos] & BASIC_TYPE_MASK) != OBJECT {
        return data_invalid("Expected Variant object");
    }
    let type_info = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
    let large_size = ((type_info >> 4) & 0x1) != 0;
    let size_bytes = if large_size { U32_SIZE } else { 1 };
    let size = read_unsigned(value, pos + 1, size_bytes)?;
    let id_size = ((type_info >> 2) & 0x3) as usize + 1;
    let offset_size = (type_info & 0x3) as usize + 1;
    let id_start = pos + 1 + size_bytes;
    let offset_start = id_start + size * id_size;
    let data_start = offset_start + (size + 1) * offset_size;
    check_range(value, id_start, size * id_size)?;
    check_range(value, offset_start, (size + 1) * offset_size)?;
    Ok(ObjectLayout {
        size,
        id_size,
        offset_size,
        id_start,
        offset_start,
        data_start,
    })
}

#[derive(Clone, Copy)]
struct ArrayLayout {
    size: usize,
    offset_size: usize,
    offset_start: usize,
    data_start: usize,
}

fn array_layout(value: &[u8], pos: usize) -> Result<ArrayLayout> {
    check_index(value, pos)?;
    if (value[pos] & BASIC_TYPE_MASK) != ARRAY {
        return data_invalid("Expected Variant array");
    }
    let type_info = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
    let large_size = ((type_info >> 2) & 0x1) != 0;
    let size_bytes = if large_size { U32_SIZE } else { 1 };
    let size = read_unsigned(value, pos + 1, size_bytes)?;
    let offset_size = (type_info & 0x3) as usize + 1;
    let offset_start = pos + 1 + size_bytes;
    let data_start = offset_start + (size + 1) * offset_size;
    check_range(value, offset_start, (size + 1) * offset_size)?;
    Ok(ArrayLayout {
        size,
        offset_size,
        offset_start,
        data_start,
    })
}

fn metadata_offset_size(metadata: &[u8]) -> Result<usize> {
    check_index(metadata, 0)?;
    if (metadata[0] & VERSION_MASK) != VERSION {
        return data_invalid("Malformed Variant metadata version");
    }
    Ok(((metadata[0] >> 6) & 0x3) as usize + 1)
}

fn get_metadata_key(metadata: &[u8], id: usize) -> Result<String> {
    let offset_size = metadata_offset_size(metadata)?;
    let dict_size = read_unsigned(metadata, 1, offset_size)?;
    if id >= dict_size {
        return data_invalid("Malformed Variant metadata dictionary id");
    }
    let string_start = 1 + (dict_size + 2) * offset_size;
    let offset = read_unsigned(metadata, 1 + (id + 1) * offset_size, offset_size)?;
    let next_offset = read_unsigned(metadata, 1 + (id + 2) * offset_size, offset_size)?;
    if offset > next_offset {
        return data_invalid("Malformed Variant metadata offsets");
    }
    check_range(metadata, string_start + offset, next_offset - offset)?;
    let bytes = &metadata[string_start + offset..string_start + next_offset];
    std::str::from_utf8(bytes)
        .map(|v| v.to_string())
        .map_err(|e| Error::DataInvalid {
            message: "Malformed Variant metadata UTF-8".to_string(),
            source: Some(Box::new(e)),
        })
}

fn get_boolean(value: &[u8], pos: usize) -> Result<bool> {
    check_index(value, pos)?;
    let basic_type = value[pos] & BASIC_TYPE_MASK;
    let type_info = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
    if basic_type != PRIMITIVE || (type_info != TRUE && type_info != FALSE) {
        return data_invalid("Expected Variant boolean");
    }
    Ok(type_info == TRUE)
}

fn get_long(value: &[u8], pos: usize) -> Result<i64> {
    check_index(value, pos)?;
    if (value[pos] & BASIC_TYPE_MASK) != PRIMITIVE {
        return data_invalid("Expected Variant long/date/timestamp");
    }
    match (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK {
        INT1 => read_signed(value, pos + 1, 1),
        INT2 => read_signed(value, pos + 1, 2),
        INT4 | DATE => read_signed(value, pos + 1, 4),
        INT8 | TIMESTAMP | TIMESTAMP_NTZ => read_signed(value, pos + 1, 8),
        _ => data_invalid("Expected Variant long/date/timestamp"),
    }
}

fn get_double(value: &[u8], pos: usize) -> Result<f64> {
    check_index(value, pos)?;
    if (value[pos] & BASIC_TYPE_MASK) != PRIMITIVE
        || ((value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK) != DOUBLE
    {
        return data_invalid("Expected Variant double");
    }
    Ok(f64::from_bits(read_signed(value, pos + 1, 8)? as u64))
}

fn get_float(value: &[u8], pos: usize) -> Result<f32> {
    check_index(value, pos)?;
    if (value[pos] & BASIC_TYPE_MASK) != PRIMITIVE
        || ((value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK) != FLOAT
    {
        return data_invalid("Expected Variant float");
    }
    Ok(f32::from_bits(read_signed(value, pos + 1, 4)? as u32))
}

fn get_string(value: &[u8], pos: usize) -> Result<String> {
    check_index(value, pos)?;
    let basic_type = value[pos] & BASIC_TYPE_MASK;
    let type_info = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
    let (start, len) = if basic_type == SHORT_STR {
        (pos + 1, type_info as usize)
    } else if basic_type == PRIMITIVE && type_info == LONG_STR {
        (pos + 1 + U32_SIZE, read_unsigned(value, pos + 1, U32_SIZE)?)
    } else {
        return data_invalid("Expected Variant string");
    };
    check_range(value, start, len)?;
    std::str::from_utf8(&value[start..start + len])
        .map(|v| v.to_string())
        .map_err(|e| Error::DataInvalid {
            message: "Malformed Variant string UTF-8".to_string(),
            source: Some(Box::new(e)),
        })
}

fn get_binary(value: &[u8], pos: usize) -> Result<&[u8]> {
    check_index(value, pos)?;
    if (value[pos] & BASIC_TYPE_MASK) != PRIMITIVE
        || ((value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK) != BINARY
    {
        return data_invalid("Expected Variant binary");
    }
    let len = read_unsigned(value, pos + 1, U32_SIZE)?;
    let start = pos + 1 + U32_SIZE;
    check_range(value, start, len)?;
    Ok(&value[start..start + len])
}

fn get_uuid(value: &[u8], pos: usize) -> Result<uuid::Uuid> {
    check_index(value, pos)?;
    if (value[pos] & BASIC_TYPE_MASK) != PRIMITIVE
        || ((value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK) != UUID
    {
        return data_invalid("Expected Variant UUID");
    }
    check_range(value, pos + 1, 16)?;
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&value[pos + 1..pos + 17]);
    Ok(uuid::Uuid::from_bytes(bytes))
}

fn get_decimal(value: &[u8], pos: usize) -> Result<VariantDecimal> {
    check_index(value, pos)?;
    if (value[pos] & BASIC_TYPE_MASK) != PRIMITIVE {
        return data_invalid("Expected Variant decimal");
    }
    let type_info = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
    let scale = value
        .get(pos + 1)
        .copied()
        .ok_or_else(|| Error::DataInvalid {
            message: "Malformed Variant decimal".to_string(),
            source: None,
        })? as i8;
    let (unscaled, max_precision) = match type_info {
        DECIMAL4 => (
            read_signed(value, pos + 2, 4)? as i128,
            MAX_DECIMAL4_PRECISION,
        ),
        DECIMAL8 => (
            read_signed(value, pos + 2, 8)? as i128,
            MAX_DECIMAL8_PRECISION,
        ),
        DECIMAL16 => {
            check_range(value, pos + 2, 16)?;
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(&value[pos + 2..pos + 18]);
            (i128::from_le_bytes(bytes), MAX_DECIMAL16_PRECISION)
        }
        _ => return data_invalid("Expected Variant decimal"),
    };
    let precision = decimal_precision(unscaled);
    if precision > max_precision || scale < 0 || scale as u8 > max_precision {
        return data_invalid("Malformed Variant decimal precision or scale");
    }
    Ok(VariantDecimal {
        unscaled,
        precision,
        scale,
    })
}

fn write_json(value: &[u8], metadata: &[u8], pos: usize, out: &mut String) -> Result<()> {
    match value_kind(value, pos)? {
        VariantKind::Object => {
            let layout = object_layout(value, pos)?;
            out.push('{');
            for i in 0..layout.size {
                if i != 0 {
                    out.push(',');
                }
                let id =
                    read_unsigned(value, layout.id_start + layout.id_size * i, layout.id_size)?;
                let key = get_metadata_key(metadata, id)?;
                out.push_str(
                    &serde_json::to_string(&key).map_err(|e| Error::DataInvalid {
                        message: "Failed to escape Variant object key".to_string(),
                        source: Some(Box::new(e)),
                    })?,
                );
                out.push(':');
                let offset = read_unsigned(
                    value,
                    layout.offset_start + layout.offset_size * i,
                    layout.offset_size,
                )?;
                write_json(value, metadata, layout.data_start + offset, out)?;
            }
            out.push('}');
        }
        VariantKind::Array => {
            let layout = array_layout(value, pos)?;
            out.push('[');
            for i in 0..layout.size {
                if i != 0 {
                    out.push(',');
                }
                let offset = read_unsigned(
                    value,
                    layout.offset_start + layout.offset_size * i,
                    layout.offset_size,
                )?;
                write_json(value, metadata, layout.data_start + offset, out)?;
            }
            out.push(']');
        }
        VariantKind::Null => out.push_str("null"),
        VariantKind::Boolean => out.push_str(if get_boolean(value, pos)? {
            "true"
        } else {
            "false"
        }),
        VariantKind::Long => out.push_str(&get_long(value, pos)?.to_string()),
        VariantKind::String => out.push_str(
            &serde_json::to_string(&get_string(value, pos)?).map_err(|e| Error::DataInvalid {
                message: "Failed to escape Variant string".to_string(),
                source: Some(Box::new(e)),
            })?,
        ),
        VariantKind::Double => out.push_str(&get_double(value, pos)?.to_string()),
        VariantKind::Decimal => out.push_str(&get_decimal(value, pos)?.to_plain_string()),
        VariantKind::Float => out.push_str(&get_float(value, pos)?.to_string()),
        VariantKind::Binary => {
            let encoded = general_purpose::STANDARD.encode(get_binary(value, pos)?);
            out.push_str(
                &serde_json::to_string(&encoded).map_err(|e| Error::DataInvalid {
                    message: "Failed to encode Variant binary".to_string(),
                    source: Some(Box::new(e)),
                })?,
            );
        }
        VariantKind::Uuid => out.push_str(
            &serde_json::to_string(&get_uuid(value, pos)?.to_string()).map_err(|e| {
                Error::DataInvalid {
                    message: "Failed to stringify Variant UUID".to_string(),
                    source: Some(Box::new(e)),
                }
            })?,
        ),
        VariantKind::Date | VariantKind::Timestamp | VariantKind::TimestampNtz => {
            out.push_str(
                &serde_json::to_string(&get_long(value, pos)?.to_string()).map_err(|e| {
                    Error::DataInvalid {
                        message: "Failed to stringify Variant value".to_string(),
                        source: Some(Box::new(e)),
                    }
                })?,
            );
        }
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum PathSegment {
    Key(String),
    Index(usize),
}

fn parse_path(path: &str) -> Result<Vec<PathSegment>> {
    let bytes = path.as_bytes();
    if !bytes.starts_with(b"$") {
        return data_invalid(format!("Invalid Variant path: {path}"));
    }
    let mut pos = 1usize;
    let mut segments = Vec::new();
    while pos < bytes.len() {
        match bytes[pos] {
            b'.' => {
                pos += 1;
                let start = pos;
                while pos < bytes.len() && bytes[pos] != b'.' && bytes[pos] != b'[' {
                    pos += 1;
                }
                if start == pos {
                    return data_invalid(format!("Invalid Variant path: {path}"));
                }
                segments.push(PathSegment::Key(path[start..pos].to_string()));
            }
            b'[' => {
                pos += 1;
                if pos >= bytes.len() {
                    return data_invalid(format!("Invalid Variant path: {path}"));
                }
                if bytes[pos] == b'\'' || bytes[pos] == b'"' {
                    let quote = bytes[pos];
                    pos += 1;
                    let start = pos;
                    while pos < bytes.len() && bytes[pos] != quote {
                        pos += 1;
                    }
                    if pos >= bytes.len() || pos + 1 >= bytes.len() || bytes[pos + 1] != b']' {
                        return data_invalid(format!("Invalid Variant path: {path}"));
                    }
                    segments.push(PathSegment::Key(path[start..pos].to_string()));
                    pos += 2;
                } else {
                    let start = pos;
                    while pos < bytes.len() && bytes[pos].is_ascii_digit() {
                        pos += 1;
                    }
                    if start == pos || pos >= bytes.len() || bytes[pos] != b']' {
                        return data_invalid(format!("Invalid Variant path: {path}"));
                    }
                    let index =
                        path[start..pos]
                            .parse::<usize>()
                            .map_err(|e| Error::DataInvalid {
                                message: format!("Invalid Variant path index: {path}"),
                                source: Some(Box::new(e)),
                            })?;
                    segments.push(PathSegment::Index(index));
                    pos += 1;
                }
            }
            _ => return data_invalid(format!("Invalid Variant path: {path}")),
        }
    }
    Ok(segments)
}

#[derive(Clone)]
struct FieldEntry {
    key: String,
    id: usize,
    offset: usize,
}

struct VariantBuilder {
    value: Vec<u8>,
    dictionary: HashMap<String, usize>,
    dictionary_keys: Vec<Vec<u8>>,
}

impl VariantBuilder {
    fn new() -> Self {
        Self {
            value: Vec::with_capacity(128),
            dictionary: HashMap::new(),
            dictionary_keys: Vec::new(),
        }
    }

    fn result(self) -> Result<GenericVariant> {
        let num_keys = self.dictionary_keys.len();
        let dictionary_string_size = self.dictionary_keys.iter().map(Vec::len).sum::<usize>();
        let max_size = dictionary_string_size.max(num_keys);
        let offset_size = integer_size(max_size)?;
        let offset_start = 1 + offset_size;
        let string_start = offset_start + (num_keys + 1) * offset_size;
        let metadata_size = string_start + dictionary_string_size;
        if metadata_size > SIZE_LIMIT {
            return data_invalid("Variant metadata exceeds size limit");
        }

        let mut metadata = vec![0u8; metadata_size];
        metadata[0] = VERSION | (((offset_size - 1) as u8) << 6);
        write_le_at(&mut metadata, 1, num_keys, offset_size);
        let mut current_offset = 0usize;
        for (i, key) in self.dictionary_keys.iter().enumerate() {
            write_le_at(
                &mut metadata,
                offset_start + i * offset_size,
                current_offset,
                offset_size,
            );
            metadata[string_start + current_offset..string_start + current_offset + key.len()]
                .copy_from_slice(key);
            current_offset += key.len();
        }
        write_le_at(
            &mut metadata,
            offset_start + num_keys * offset_size,
            current_offset,
            offset_size,
        );
        GenericVariant::from_parts(self.value, metadata)
    }

    fn add_key(&mut self, key: &str) -> usize {
        if let Some(id) = self.dictionary.get(key) {
            *id
        } else {
            let id = self.dictionary_keys.len();
            self.dictionary.insert(key.to_string(), id);
            self.dictionary_keys.push(key.as_bytes().to_vec());
            id
        }
    }

    fn append_null(&mut self) {
        self.value.push(primitive_header(NULL));
    }

    fn append_bool(&mut self, value: bool) {
        self.value
            .push(primitive_header(if value { TRUE } else { FALSE }));
    }

    fn append_string(&mut self, value: &str) {
        let bytes = value.as_bytes();
        if bytes.len() > MAX_SHORT_STR_SIZE {
            self.value.push(primitive_header(LONG_STR));
            let pos = self.value.len();
            self.value.resize(pos + U32_SIZE, 0);
            write_le_at(&mut self.value, pos, bytes.len(), U32_SIZE);
        } else {
            self.value.push(short_str_header(bytes.len()));
        }
        self.value.extend_from_slice(bytes);
    }

    fn append_long(&mut self, value: i64) {
        if value == value as i8 as i64 {
            self.value.push(primitive_header(INT1));
            push_signed_le(&mut self.value, value as i128, 1);
        } else if value == value as i16 as i64 {
            self.value.push(primitive_header(INT2));
            push_signed_le(&mut self.value, value as i128, 2);
        } else if value == value as i32 as i64 {
            self.value.push(primitive_header(INT4));
            push_signed_le(&mut self.value, value as i128, 4);
        } else {
            self.value.push(primitive_header(INT8));
            push_signed_le(&mut self.value, value as i128, 8);
        }
    }

    fn append_double(&mut self, value: f64) {
        self.value.push(primitive_header(DOUBLE));
        self.value.extend_from_slice(&value.to_bits().to_le_bytes());
    }

    fn append_decimal(&mut self, decimal: VariantDecimal) {
        if decimal.scale as u8 <= MAX_DECIMAL4_PRECISION
            && decimal.precision <= MAX_DECIMAL4_PRECISION
        {
            self.value.push(primitive_header(DECIMAL4));
            self.value.push(decimal.scale as u8);
            push_signed_le(&mut self.value, decimal.unscaled, 4);
        } else if decimal.scale as u8 <= MAX_DECIMAL8_PRECISION
            && decimal.precision <= MAX_DECIMAL8_PRECISION
        {
            self.value.push(primitive_header(DECIMAL8));
            self.value.push(decimal.scale as u8);
            push_signed_le(&mut self.value, decimal.unscaled, 8);
        } else {
            self.value.push(primitive_header(DECIMAL16));
            self.value.push(decimal.scale as u8);
            self.value
                .extend_from_slice(&decimal.unscaled.to_le_bytes());
        }
    }

    fn finish_object(&mut self, start: usize, mut fields: Vec<FieldEntry>) -> Result<()> {
        fields.sort_by(|a, b| java_string_cmp(&a.key, &b.key));
        for pair in fields.windows(2) {
            if pair[0].key == pair[1].key {
                return data_invalid("VARIANT_DUPLICATE_KEY");
            }
        }

        let data_size = self.value.len() - start;
        let size = fields.len();
        let large_size = size > U8_MAX;
        let size_bytes = if large_size { U32_SIZE } else { 1 };
        let max_id = fields.iter().map(|f| f.id).max().unwrap_or(0);
        let id_size = integer_size(max_id)?;
        let offset_size = integer_size(data_size)?;
        let header_size = 1 + size_bytes + size * id_size + (size + 1) * offset_size;
        self.value.splice(start..start, vec![0; header_size]);
        self.value[start] = object_header(large_size, id_size, offset_size);
        write_le_at(&mut self.value, start + 1, size, size_bytes);
        let id_start = start + 1 + size_bytes;
        let offset_start = id_start + size * id_size;
        for (i, field) in fields.iter().enumerate() {
            write_le_at(&mut self.value, id_start + i * id_size, field.id, id_size);
            write_le_at(
                &mut self.value,
                offset_start + i * offset_size,
                field.offset,
                offset_size,
            );
        }
        write_le_at(
            &mut self.value,
            offset_start + size * offset_size,
            data_size,
            offset_size,
        );
        Ok(())
    }

    fn finish_array(&mut self, start: usize, offsets: Vec<usize>) -> Result<()> {
        let data_size = self.value.len() - start;
        let size = offsets.len();
        let large_size = size > U8_MAX;
        let size_bytes = if large_size { U32_SIZE } else { 1 };
        let offset_size = integer_size(data_size)?;
        let header_size = 1 + size_bytes + (size + 1) * offset_size;
        self.value.splice(start..start, vec![0; header_size]);
        self.value[start] = array_header(large_size, offset_size);
        write_le_at(&mut self.value, start + 1, size, size_bytes);
        let offset_start = start + 1 + size_bytes;
        for (i, offset) in offsets.iter().enumerate() {
            write_le_at(
                &mut self.value,
                offset_start + i * offset_size,
                *offset,
                offset_size,
            );
        }
        write_le_at(
            &mut self.value,
            offset_start + size * offset_size,
            data_size,
            offset_size,
        );
        Ok(())
    }
}

struct JsonVariantParser<'a> {
    input: &'a str,
    bytes: &'a [u8],
    pos: usize,
    builder: VariantBuilder,
}

impl<'a> JsonVariantParser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input,
            bytes: input.as_bytes(),
            pos: 0,
            builder: VariantBuilder::new(),
        }
    }

    fn parse(mut self) -> Result<GenericVariant> {
        self.skip_ws();
        self.parse_value()?;
        self.skip_ws();
        if self.pos != self.bytes.len() {
            return self.parse_error("Trailing characters after JSON value");
        }
        self.builder.result()
    }

    fn parse_value(&mut self) -> Result<()> {
        self.skip_ws();
        let Some(ch) = self.peek() else {
            return self.parse_error("Unexpected end of JSON input");
        };
        match ch {
            b'{' => self.parse_object(),
            b'[' => self.parse_array(),
            b'"' => {
                let value = self.parse_string()?;
                self.builder.append_string(&value);
                Ok(())
            }
            b't' => {
                self.expect_literal("true")?;
                self.builder.append_bool(true);
                Ok(())
            }
            b'f' => {
                self.expect_literal("false")?;
                self.builder.append_bool(false);
                Ok(())
            }
            b'n' => {
                self.expect_literal("null")?;
                self.builder.append_null();
                Ok(())
            }
            b'-' | b'0'..=b'9' => self.parse_number(),
            _ => self.parse_error("Unexpected JSON token"),
        }
    }

    fn parse_object(&mut self) -> Result<()> {
        self.consume(b'{')?;
        let start = self.builder.value.len();
        let mut fields = Vec::new();
        let mut seen = HashSet::new();
        self.skip_ws();
        if self.peek() == Some(b'}') {
            self.pos += 1;
            return self.builder.finish_object(start, fields);
        }
        loop {
            self.skip_ws();
            let key = self.parse_string()?;
            if !seen.insert(key.clone()) {
                return self.parse_error("VARIANT_DUPLICATE_KEY");
            }
            self.skip_ws();
            self.consume(b':')?;
            let id = self.builder.add_key(&key);
            let offset = self.builder.value.len() - start;
            fields.push(FieldEntry { key, id, offset });
            self.parse_value()?;
            self.skip_ws();
            match self.peek() {
                Some(b',') => {
                    self.pos += 1;
                }
                Some(b'}') => {
                    self.pos += 1;
                    break;
                }
                _ => return self.parse_error("Expected ',' or '}' in JSON object"),
            }
        }
        self.builder.finish_object(start, fields)
    }

    fn parse_array(&mut self) -> Result<()> {
        self.consume(b'[')?;
        let start = self.builder.value.len();
        let mut offsets = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b']') {
            self.pos += 1;
            return self.builder.finish_array(start, offsets);
        }
        loop {
            offsets.push(self.builder.value.len() - start);
            self.parse_value()?;
            self.skip_ws();
            match self.peek() {
                Some(b',') => {
                    self.pos += 1;
                }
                Some(b']') => {
                    self.pos += 1;
                    break;
                }
                _ => return self.parse_error("Expected ',' or ']' in JSON array"),
            }
        }
        self.builder.finish_array(start, offsets)
    }

    fn parse_string(&mut self) -> Result<String> {
        self.skip_ws();
        let start = self.pos;
        self.consume(b'"')?;
        let mut escaped = false;
        while let Some(ch) = self.peek() {
            self.pos += 1;
            if escaped {
                escaped = false;
                continue;
            }
            match ch {
                b'\\' => escaped = true,
                b'"' => {
                    return serde_json::from_str(&self.input[start..self.pos]).map_err(|e| {
                        Error::DataInvalid {
                            message: "Invalid JSON string".to_string(),
                            source: Some(Box::new(e)),
                        }
                    });
                }
                _ if ch < 0x20 => return self.parse_error("Invalid control character in string"),
                _ => {}
            }
        }
        self.parse_error("Unterminated JSON string")
    }

    fn parse_number(&mut self) -> Result<()> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.pos += 1;
        }
        match self.peek() {
            Some(b'0') => self.pos += 1,
            Some(b'1'..=b'9') => {
                self.pos += 1;
                while matches!(self.peek(), Some(b'0'..=b'9')) {
                    self.pos += 1;
                }
            }
            _ => return self.parse_error("Invalid JSON number"),
        }
        let mut has_fraction = false;
        if self.peek() == Some(b'.') {
            has_fraction = true;
            self.pos += 1;
            let fraction_start = self.pos;
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.pos += 1;
            }
            if self.pos == fraction_start {
                return self.parse_error("Invalid JSON number fraction");
            }
        }
        let mut has_exponent = false;
        if matches!(self.peek(), Some(b'e' | b'E')) {
            has_exponent = true;
            self.pos += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.pos += 1;
            }
            let exponent_start = self.pos;
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.pos += 1;
            }
            if self.pos == exponent_start {
                return self.parse_error("Invalid JSON number exponent");
            }
        }
        let token = &self.input[start..self.pos];
        if !has_fraction && !has_exponent {
            if let Ok(value) = token.parse::<i64>() {
                self.builder.append_long(value);
                return Ok(());
            }
        }
        if !has_exponent {
            if let Some(decimal) = parse_decimal_token(token)? {
                self.builder.append_decimal(decimal);
                return Ok(());
            }
        }
        let value = token.parse::<f64>().map_err(|e| Error::DataInvalid {
            message: format!("Invalid JSON number: {token}"),
            source: Some(Box::new(e)),
        })?;
        self.builder.append_double(value);
        Ok(())
    }

    fn expect_literal(&mut self, literal: &str) -> Result<()> {
        if self.input[self.pos..].starts_with(literal) {
            self.pos += literal.len();
            Ok(())
        } else {
            self.parse_error(format!("Expected JSON literal {literal}"))
        }
    }

    fn consume(&mut self, expected: u8) -> Result<()> {
        self.skip_ws();
        if self.peek() == Some(expected) {
            self.pos += 1;
            Ok(())
        } else {
            self.parse_error(format!("Expected '{}'", expected as char))
        }
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\n' | b'\r' | b'\t')) {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn parse_error<T>(&self, message: impl Into<String>) -> Result<T> {
        data_invalid(format!("{} at byte {}", message.into(), self.pos))
    }
}

fn parse_decimal_token(token: &str) -> Result<Option<VariantDecimal>> {
    if !token
        .bytes()
        .all(|ch| ch == b'-' || ch == b'.' || ch.is_ascii_digit())
    {
        return Ok(None);
    }
    let negative = token.starts_with('-');
    let unsigned = token.strip_prefix('-').unwrap_or(token);
    let scale = unsigned
        .split_once('.')
        .map(|(_, fraction)| fraction.len())
        .unwrap_or(0);
    if scale > MAX_DECIMAL16_PRECISION as usize {
        return Ok(None);
    }
    let mut digits = String::with_capacity(unsigned.len());
    for ch in unsigned.bytes() {
        if ch != b'.' {
            digits.push(ch as char);
        }
    }
    let significant = digits.trim_start_matches('0');
    let precision = if significant.is_empty() {
        1
    } else {
        significant.len()
    };
    if precision > MAX_DECIMAL16_PRECISION as usize {
        return Ok(None);
    }
    let mut unscaled = digits.parse::<i128>().map_err(|e| Error::DataInvalid {
        message: format!("Invalid decimal Variant number: {token}"),
        source: Some(Box::new(e)),
    })?;
    if negative {
        unscaled = -unscaled;
    }
    Ok(Some(VariantDecimal {
        unscaled,
        precision: precision as u8,
        scale: scale as i8,
    }))
}

fn decimal_precision(unscaled: i128) -> u8 {
    let mut value = unscaled.unsigned_abs();
    if value == 0 {
        return 1;
    }
    let mut precision = 0u8;
    while value > 0 {
        precision += 1;
        value /= 10;
    }
    precision
}

fn decimal_to_plain_string(unscaled: i128, scale: i8, strip_trailing_zeros: bool) -> String {
    if scale <= 0 {
        return unscaled.to_string();
    }
    let negative = unscaled < 0;
    let digits = unscaled.unsigned_abs().to_string();
    let scale = scale as usize;
    let mut result = if digits.len() > scale {
        let split = digits.len() - scale;
        format!("{}.{}", &digits[..split], &digits[split..])
    } else {
        format!("0.{}{}", "0".repeat(scale - digits.len()), digits)
    };
    if strip_trailing_zeros && result.contains('.') {
        while result.ends_with('0') {
            result.pop();
        }
        if result.ends_with('.') {
            result.pop();
        }
    }
    if negative && result != "0" {
        result.insert(0, '-');
    }
    result
}

fn java_string_cmp(left: &str, right: &str) -> std::cmp::Ordering {
    left.encode_utf16().cmp(right.encode_utf16())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_metadata() -> Vec<u8> {
        vec![VERSION, 0, 0]
    }

    #[test]
    fn parse_json_matches_java_basic_object_layout() {
        let variant = GenericVariant::parse_json(r#"{"age":27,"city":"Beijing"}"#).unwrap();
        assert_eq!(
            variant.value(),
            &[
                0x02, 0x02, 0x00, 0x01, 0x00, 0x02, 0x0a, 0x0c, 0x1b, 0x1d, b'B', b'e', b'i', b'j',
                b'i', b'n', b'g'
            ]
        );
        assert_eq!(
            variant.metadata(),
            &[0x01, 0x02, 0x00, 0x03, 0x07, b'a', b'g', b'e', b'c', b'i', b't', b'y']
        );
    }

    #[test]
    fn variant_get_path_reads_objects_and_arrays() {
        let variant =
            GenericVariant::parse_json(r#"{"object":{"name":"Alice"},"array":[1,2,null]}"#)
                .unwrap();
        assert_eq!(
            variant
                .get_path("$.object.name")
                .unwrap()
                .unwrap()
                .get_string()
                .unwrap(),
            "Alice"
        );
        assert_eq!(
            variant
                .get_path("$.array[1]")
                .unwrap()
                .unwrap()
                .get_long()
                .unwrap(),
            2
        );
        assert!(variant
            .get_path("$.array[2]")
            .unwrap()
            .unwrap()
            .is_null()
            .unwrap());
        assert!(variant.get_path("$.array[9]").unwrap().is_none());
    }

    #[test]
    fn parse_json_rejects_duplicate_object_keys() {
        let err = GenericVariant::parse_json(r#"{"a":1,"a":2}"#).unwrap_err();
        assert!(err.to_string().contains("VARIANT_DUPLICATE_KEY"));
    }

    #[test]
    fn validate_payload_rejects_malformed_root_value() {
        let metadata = empty_metadata();
        assert!(validate_payload(&[], &metadata).is_err());

        let truncated_short_string = [short_str_header(3), b'a'];
        assert!(validate_payload(&truncated_short_string, &metadata).is_err());
    }

    #[test]
    fn validate_payload_rejects_bad_object_metadata_ids() {
        let variant = GenericVariant::parse_json(r#"{"a":1}"#).unwrap();
        let mut value = variant.value().to_vec();
        let layout = object_layout(&value, 0).unwrap();
        write_le_at(&mut value, layout.id_start, 1, layout.id_size);

        assert!(validate_payload(&value, variant.metadata()).is_err());
    }

    #[test]
    fn validate_payload_rejects_bad_object_offsets() {
        let variant = GenericVariant::parse_json(r#"{"a":1}"#).unwrap();
        let mut value = variant.value().to_vec();
        let layout = object_layout(&value, 0).unwrap();
        let data_size = value.len() - layout.data_start;
        write_le_at(
            &mut value,
            layout.offset_start + layout.size * layout.offset_size,
            data_size + 1,
            layout.offset_size,
        );

        assert!(validate_payload(&value, variant.metadata()).is_err());
    }

    #[test]
    fn json_number_encoding_keeps_decimal_and_double_distinct() {
        let decimal = GenericVariant::parse_json(r#"{"d":123.4500}"#).unwrap();
        let d = decimal.get_path("$.d").unwrap().unwrap();
        assert_eq!(d.kind().unwrap(), VariantKind::Decimal);
        assert_eq!(d.to_json().unwrap(), "123.45");

        let double = GenericVariant::parse_json(r#"{"d":1.23e10}"#).unwrap();
        assert_eq!(
            double.get_path("$.d").unwrap().unwrap().kind().unwrap(),
            VariantKind::Double
        );
    }
}
