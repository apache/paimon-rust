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

//! BinaryRow: an implementation of InternalRow backed by raw binary bytes,
//! and BinaryRowBuilder for constructing BinaryRow instances.

use crate::spec::murmur_hash::hash_by_words;
use serde::{Deserialize, Serialize};

pub const EMPTY_BINARY_ROW: BinaryRow = BinaryRow::new(0);

/// Highest bit mask for detecting inline vs variable-length encoding.
const HIGHEST_FIRST_BIT: u64 = 0x80 << 56;

/// Mask to extract the 7-bit length from an inline-encoded value.
const HIGHEST_SECOND_TO_EIGHTH_BIT: u64 = 0x7F << 56;

/// An implementation of InternalRow backed by raw binary bytes.
///
/// Binary layout (little-endian):
/// ```text
/// | header (8 bytes) | null bit set (8-byte aligned) | fixed-length (8B per field) | variable-length |
/// ```
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/data/BinaryRow.java>
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinaryRow {
    arity: i32,
    null_bits_size_in_bytes: i32,

    #[serde(with = "serde_bytes")]
    data: Vec<u8>,
}

impl BinaryRow {
    pub const HEADER_SIZE_IN_BYTES: i32 = 8;

    pub const fn cal_bit_set_width_in_bytes(arity: i32) -> i32 {
        ((arity + 63 + Self::HEADER_SIZE_IN_BYTES) / 64) * 8
    }

    pub const fn cal_fix_part_size_in_bytes(arity: i32) -> i32 {
        Self::cal_bit_set_width_in_bytes(arity) + 8 * arity
    }

    pub const fn new(arity: i32) -> Self {
        Self {
            arity,
            null_bits_size_in_bytes: Self::cal_bit_set_width_in_bytes(arity),
            data: Vec::new(),
        }
    }

    pub fn from_bytes(arity: i32, data: Vec<u8>) -> Self {
        let null_bits_size_in_bytes = Self::cal_bit_set_width_in_bytes(arity);
        Self {
            arity,
            null_bits_size_in_bytes,
            data,
        }
    }

    pub fn from_serialized_bytes(data: &[u8]) -> crate::Result<Self> {
        if data.len() < 4 {
            return Err(crate::Error::UnexpectedError {
                message: format!(
                    "BinaryRow: serialized data too short for arity prefix: {} bytes",
                    data.len()
                ),
                source: None,
            });
        }
        let arity = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        Ok(Self::from_bytes(arity, data[4..].to_vec()))
    }

    pub fn arity(&self) -> i32 {
        self.arity
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn is_null_at(&self, pos: usize) -> bool {
        let bit_index = pos + Self::HEADER_SIZE_IN_BYTES as usize;
        let byte_index = bit_index / 8;
        let bit_offset = bit_index % 8;
        (self.data[byte_index] & (1 << bit_offset)) != 0
    }

    fn field_offset(&self, pos: usize) -> usize {
        self.null_bits_size_in_bytes as usize + pos * 8
    }

    fn read_slice<const N: usize>(&self, offset: usize) -> crate::Result<[u8; N]> {
        self.data
            .get(offset..offset + N)
            .and_then(|s| s.try_into().ok())
            .ok_or_else(|| crate::Error::UnexpectedError {
                message: format!(
                    "BinaryRow: read {N} bytes at offset {offset} exceeds data length {}",
                    self.data.len()
                ),
                source: None,
            })
    }

    fn read_byte_at(&self, offset: usize) -> crate::Result<u8> {
        self.data
            .get(offset)
            .copied()
            .ok_or_else(|| crate::Error::UnexpectedError {
                message: format!(
                    "BinaryRow: read 1 byte at offset {offset} exceeds data length {}",
                    self.data.len()
                ),
                source: None,
            })
    }

    fn read_i64_at(&self, offset: usize) -> crate::Result<i64> {
        self.read_slice::<8>(offset).map(i64::from_le_bytes)
    }

    fn read_i32_at(&self, offset: usize) -> crate::Result<i32> {
        self.read_slice::<4>(offset).map(i32::from_le_bytes)
    }

    pub fn get_boolean(&self, pos: usize) -> crate::Result<bool> {
        self.read_byte_at(self.field_offset(pos)).map(|b| b != 0)
    }

    pub fn get_byte(&self, pos: usize) -> crate::Result<i8> {
        self.read_byte_at(self.field_offset(pos)).map(|b| b as i8)
    }

    pub fn get_short(&self, pos: usize) -> crate::Result<i16> {
        self.read_slice::<2>(self.field_offset(pos))
            .map(i16::from_le_bytes)
    }

    pub fn get_int(&self, pos: usize) -> crate::Result<i32> {
        self.read_i32_at(self.field_offset(pos))
    }

    pub fn get_long(&self, pos: usize) -> crate::Result<i64> {
        self.read_i64_at(self.field_offset(pos))
    }

    pub fn get_float(&self, pos: usize) -> crate::Result<f32> {
        self.read_slice::<4>(self.field_offset(pos))
            .map(f32::from_le_bytes)
    }

    pub fn get_double(&self, pos: usize) -> crate::Result<f64> {
        self.read_slice::<8>(self.field_offset(pos))
            .map(f64::from_le_bytes)
    }

    fn resolve_var_length_field(&self, pos: usize) -> crate::Result<(usize, usize)> {
        let field_off = self.field_offset(pos);
        let raw = self.read_i64_at(field_off)? as u64;

        let (start, len) = if raw & HIGHEST_FIRST_BIT == 0 {
            let offset = (raw >> 32) as usize;
            let len = (raw & 0xFFFF_FFFF) as usize;
            (offset, len)
        } else {
            let len = ((raw & HIGHEST_SECOND_TO_EIGHTH_BIT) >> 56) as usize;
            (field_off, len)
        };

        let end = start
            .checked_add(len)
            .ok_or_else(|| crate::Error::UnexpectedError {
                message: format!(
                    "BinaryRow: var-len field at pos {pos}: offset {start} + len {len} overflows"
                ),
                source: None,
            })?;
        if end > self.data.len() {
            return Err(crate::Error::UnexpectedError {
                message: format!(
                    "BinaryRow: var-len field at pos {pos}: range [{start}..{end}) exceeds data length {}",
                    self.data.len()
                ),
                source: None,
            });
        }
        Ok((start, len))
    }

    pub fn get_binary(&self, pos: usize) -> crate::Result<&[u8]> {
        let (start, len) = self.resolve_var_length_field(pos)?;
        Ok(&self.data[start..start + len])
    }

    pub fn get_string(&self, pos: usize) -> crate::Result<&str> {
        let bytes = self.get_binary(pos)?;
        std::str::from_utf8(bytes).map_err(|e| crate::Error::UnexpectedError {
            message: format!("BinaryRow: invalid UTF-8 in string field at pos {pos}: {e}"),
            source: Some(Box::new(e)),
        })
    }

    pub(crate) fn get_decimal_unscaled(&self, pos: usize, precision: u32) -> crate::Result<i128> {
        if precision <= 18 {
            Ok(self.get_long(pos)? as i128)
        } else {
            let bytes = self.get_binary(pos)?;
            if bytes.is_empty() {
                return Err(crate::Error::UnexpectedError {
                    message: format!("BinaryRow: empty bytes for non-compact Decimal at pos {pos}"),
                    source: None,
                });
            }
            let negative = bytes[0] & 0x80 != 0;
            let mut val: i128 = if negative { -1 } else { 0 };
            for &b in bytes {
                val = (val << 8) | (b as i128);
            }
            Ok(val)
        }
    }

    pub(crate) fn get_timestamp_raw(
        &self,
        pos: usize,
        precision: u32,
    ) -> crate::Result<(i64, i32)> {
        if precision <= 3 {
            Ok((self.get_long(pos)?, 0))
        } else {
            let field_off = self.field_offset(pos);
            let offset_and_nano = self.read_i64_at(field_off)? as u64;
            let offset = (offset_and_nano >> 32) as usize;
            let nano_of_milli = offset_and_nano as i32;

            if offset + 8 > self.data.len() {
                return Err(crate::Error::UnexpectedError {
                    message: format!(
                        "BinaryRow: non-compact Timestamp at pos {pos}: offset {offset} + 8 exceeds data length {}",
                        self.data.len()
                    ),
                    source: None,
                });
            }
            let millis = i64::from_le_bytes(self.read_slice::<8>(offset)?);
            Ok((millis, nano_of_milli))
        }
    }

    pub fn hash_code(&self) -> i32 {
        hash_by_words(&self.data)
    }

    /// Build a BinaryRow from typed Datum values using `BinaryRowBuilder`.
    pub fn from_datums(datums: &[(&crate::spec::Datum, &crate::spec::DataType)]) -> Option<Self> {
        let arity = datums.len() as i32;
        let mut builder = BinaryRowBuilder::new(arity);

        for (pos, (datum, data_type)) in datums.iter().enumerate() {
            match datum {
                crate::spec::Datum::Bool(v) => builder.write_boolean(pos, *v),
                crate::spec::Datum::TinyInt(v) => builder.write_byte(pos, *v),
                crate::spec::Datum::SmallInt(v) => builder.write_short(pos, *v),
                crate::spec::Datum::Int(v)
                | crate::spec::Datum::Date(v)
                | crate::spec::Datum::Time(v) => builder.write_int(pos, *v),
                crate::spec::Datum::Long(v) => builder.write_long(pos, *v),
                crate::spec::Datum::Float(v) => builder.write_float(pos, *v),
                crate::spec::Datum::Double(v) => builder.write_double(pos, *v),
                crate::spec::Datum::Timestamp { millis, nanos } => {
                    let precision = match data_type {
                        crate::spec::DataType::Timestamp(ts) => ts.precision(),
                        _ => 3,
                    };
                    if precision <= 3 {
                        builder.write_timestamp_compact(pos, *millis);
                    } else {
                        builder.write_timestamp_non_compact(pos, *millis, *nanos);
                    }
                }
                crate::spec::Datum::LocalZonedTimestamp { millis, nanos } => {
                    let precision = match data_type {
                        crate::spec::DataType::LocalZonedTimestamp(ts) => ts.precision(),
                        _ => 3,
                    };
                    if precision <= 3 {
                        builder.write_timestamp_compact(pos, *millis);
                    } else {
                        builder.write_timestamp_non_compact(pos, *millis, *nanos);
                    }
                }
                crate::spec::Datum::Decimal {
                    unscaled,
                    precision,
                    ..
                } => {
                    if *precision <= 18 {
                        builder.write_decimal_compact(pos, *unscaled as i64);
                    } else {
                        builder.write_decimal_var_len(pos, *unscaled);
                    }
                }
                crate::spec::Datum::String(s) => {
                    if s.len() <= 7 {
                        builder.write_string_inline(pos, s);
                    } else {
                        builder.write_string(pos, s);
                    }
                }
                crate::spec::Datum::Bytes(b) => {
                    if b.len() <= 7 {
                        builder.write_binary_inline(pos, b);
                    } else {
                        builder.write_binary(pos, b);
                    }
                }
            }
        }

        let row = builder.build();
        Some(row)
    }

    pub fn compute_bucket_from_datums(
        datums: &[(&crate::spec::Datum, &crate::spec::DataType)],
        total_buckets: i32,
    ) -> Option<i32> {
        let row = Self::from_datums(datums)?;
        let hash = row.hash_code();
        Some((hash % total_buckets).abs())
    }
}

/// Builder for constructing BinaryRow instances matching Java's BinaryRowWriter layout.
///
/// Layout: header (8 bytes) | null bit set (aligned) | fixed-length (8B per field) | var-length
pub(crate) struct BinaryRowBuilder {
    arity: i32,
    null_bits_size: usize,
    data: Vec<u8>,
}

#[allow(dead_code)]
impl BinaryRowBuilder {
    pub fn new(arity: i32) -> Self {
        let null_bits_size = BinaryRow::cal_bit_set_width_in_bytes(arity) as usize;
        let fixed_part_size = null_bits_size + (arity as usize) * 8;
        Self {
            arity,
            null_bits_size,
            data: vec![0u8; fixed_part_size],
        }
    }

    fn field_offset(&self, pos: usize) -> usize {
        self.null_bits_size + pos * 8
    }

    pub fn set_null_at(&mut self, pos: usize) {
        let bit_index = pos + BinaryRow::HEADER_SIZE_IN_BYTES as usize;
        let byte_index = bit_index / 8;
        let bit_offset = bit_index % 8;
        self.data[byte_index] |= 1 << bit_offset;
        let offset = self.field_offset(pos);
        self.data[offset..offset + 8].fill(0);
    }

    pub fn write_boolean(&mut self, pos: usize, value: bool) {
        let offset = self.field_offset(pos);
        self.data[offset] = u8::from(value);
    }

    pub fn write_byte(&mut self, pos: usize, value: i8) {
        let offset = self.field_offset(pos);
        self.data[offset] = value as u8;
    }

    pub fn write_short(&mut self, pos: usize, value: i16) {
        let offset = self.field_offset(pos);
        self.data[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
    }

    pub fn write_int(&mut self, pos: usize, value: i32) {
        let offset = self.field_offset(pos);
        self.data[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    pub fn write_long(&mut self, pos: usize, value: i64) {
        let offset = self.field_offset(pos);
        self.data[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }

    pub fn write_float(&mut self, pos: usize, value: f32) {
        let offset = self.field_offset(pos);
        self.data[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    pub fn write_double(&mut self, pos: usize, value: f64) {
        let offset = self.field_offset(pos);
        self.data[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }

    /// Write a string to the variable-length part and store offset+length in the fixed part.
    pub fn write_string(&mut self, pos: usize, value: &str) {
        self.write_binary(pos, value.as_bytes());
    }

    /// Write a short string (len <= 7) inline into the fixed part.
    pub fn write_string_inline(&mut self, pos: usize, value: &str) {
        assert!(
            value.len() <= 7,
            "inline string must be <= 7 bytes, got {}",
            value.len()
        );
        self.write_binary_inline(pos, value.as_bytes());
    }

    /// Write binary data to the variable-length part (8-byte aligned, matching Java BinaryRowWriter).
    pub fn write_binary(&mut self, pos: usize, value: &[u8]) {
        let var_offset = self.data.len();
        self.data.extend_from_slice(value);
        // Pad to 8-byte word boundary (Java: roundNumberOfBytesToNearestWord)
        let padding = (8 - (value.len() % 8)) % 8;
        self.data.extend(std::iter::repeat_n(0u8, padding));
        let encoded = ((var_offset as u64) << 32) | (value.len() as u64);
        let offset = self.field_offset(pos);
        self.data[offset..offset + 8].copy_from_slice(&encoded.to_le_bytes());
    }

    /// Write short binary data (len <= 7) inline into the fixed part.
    pub fn write_binary_inline(&mut self, pos: usize, value: &[u8]) {
        assert!(
            value.len() <= 7,
            "inline binary must be <= 7 bytes, got {}",
            value.len()
        );
        let offset = self.field_offset(pos);
        self.data[offset..offset + 8].fill(0);
        self.data[offset..offset + value.len()].copy_from_slice(value);
        self.data[offset + 7] = 0x80 | (value.len() as u8);
    }

    /// Write a compact Decimal (precision <= 18) as its unscaled i64 value.
    pub fn write_decimal_compact(&mut self, pos: usize, unscaled: i64) {
        self.write_long(pos, unscaled);
    }

    /// Write a non-compact Decimal (precision > 18) as big-endian two's complement bytes (8-byte aligned).
    pub fn write_decimal_var_len(&mut self, pos: usize, unscaled: i128) {
        let be_bytes = unscaled.to_be_bytes();
        let mut start = 0;
        while start < 15 {
            let b = be_bytes[start];
            let next = be_bytes[start + 1];
            if (b == 0x00 && next & 0x80 == 0) || (b == 0xFF && next & 0x80 != 0) {
                start += 1;
            } else {
                break;
            }
        }
        let minimal = &be_bytes[start..];

        let var_offset = self.data.len();
        self.data.extend_from_slice(minimal);
        let padding = (8 - (minimal.len() % 8)) % 8;
        self.data.extend(std::iter::repeat_n(0u8, padding));
        let len = minimal.len();
        let encoded = ((var_offset as u64) << 32) | (len as u64);
        let offset = self.field_offset(pos);
        self.data[offset..offset + 8].copy_from_slice(&encoded.to_le_bytes());
    }

    /// Write a compact Timestamp (precision <= 3) as epoch millis.
    pub fn write_timestamp_compact(&mut self, pos: usize, epoch_millis: i64) {
        self.write_long(pos, epoch_millis);
    }

    /// Write a non-compact Timestamp (precision > 3).
    pub fn write_timestamp_non_compact(
        &mut self,
        pos: usize,
        epoch_millis: i64,
        nano_of_milli: i32,
    ) {
        let var_offset = self.data.len();
        self.data.extend_from_slice(&epoch_millis.to_le_bytes());
        let encoded = ((var_offset as u64) << 32) | (nano_of_milli as u32 as u64);
        let offset = self.field_offset(pos);
        self.data[offset..offset + 8].copy_from_slice(&encoded.to_le_bytes());
    }

    pub fn build(self) -> BinaryRow {
        BinaryRow::from_bytes(self.arity, self.data)
    }

    /// Build as Paimon's serialized format: 4-byte BE arity prefix + raw data.
    pub fn build_serialized(self) -> Vec<u8> {
        let mut serialized = Vec::with_capacity(4 + self.data.len());
        serialized.extend_from_slice(&self.arity.to_be_bytes());
        serialized.extend_from_slice(&self.data);
        serialized
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_binary_row() {
        let row = BinaryRow::new(0);
        assert_eq!(row.arity(), 0);
        assert!(row.is_empty());
        assert_eq!(row.data(), &[] as &[u8]);
    }

    #[test]
    fn test_binary_row_constants() {
        assert_eq!(BinaryRow::cal_bit_set_width_in_bytes(0), 8);
        assert_eq!(BinaryRow::cal_bit_set_width_in_bytes(1), 8);
        assert_eq!(BinaryRow::cal_bit_set_width_in_bytes(56), 8);
        assert_eq!(BinaryRow::cal_bit_set_width_in_bytes(57), 16);
    }

    #[test]
    fn test_from_serialized_bytes() {
        let mut builder = BinaryRowBuilder::new(1);
        builder.write_int(0, 42);
        let serialized = builder.build_serialized();

        let row = BinaryRow::from_serialized_bytes(&serialized).unwrap();
        assert_eq!(row.arity(), 1);
        assert!(!row.is_null_at(0));
        assert_eq!(row.get_int(0).unwrap(), 42);
    }

    #[test]
    fn test_from_serialized_bytes_too_short() {
        assert!(BinaryRow::from_serialized_bytes(&[0, 0]).is_err());
    }

    #[test]
    fn test_get_int() {
        let mut builder = BinaryRowBuilder::new(2);
        builder.write_int(0, 42);
        builder.write_int(1, -100);
        let row = builder.build();

        assert!(!row.is_empty());
        assert_eq!(row.arity(), 2);
        assert_eq!(row.get_int(0).unwrap(), 42);
        assert_eq!(row.get_int(1).unwrap(), -100);
    }

    #[test]
    fn test_get_long() {
        let mut builder = BinaryRowBuilder::new(1);
        builder.write_long(0, i64::MAX);
        let row = builder.build();
        assert_eq!(row.get_long(0).unwrap(), i64::MAX);
    }

    #[test]
    fn test_get_short_byte_boolean() {
        let mut builder = BinaryRowBuilder::new(3);
        builder.write_short(0, -32768);
        builder.write_byte(1, -1);
        builder.write_boolean(2, true);
        let row = builder.build();

        assert_eq!(row.get_short(0).unwrap(), -32768);
        assert_eq!(row.get_byte(1).unwrap(), -1);
        assert!(row.get_boolean(2).unwrap());
    }

    #[test]
    fn test_get_float_double() {
        let mut builder = BinaryRowBuilder::new(2);
        builder.write_float(0, 1.5_f32);
        builder.write_double(1, std::f64::consts::PI);
        let row = builder.build();

        assert!((row.get_float(0).unwrap() - 1.5_f32).abs() < f32::EPSILON);
        assert!((row.get_double(1).unwrap() - std::f64::consts::PI).abs() < f64::EPSILON);
    }

    #[test]
    fn test_null_handling() {
        let mut builder = BinaryRowBuilder::new(3);
        builder.write_int(0, 42);
        builder.set_null_at(1);
        builder.write_int(2, 99);
        let row = builder.build();

        assert!(!row.is_null_at(0));
        assert!(row.is_null_at(1));
        assert!(!row.is_null_at(2));
        assert_eq!(row.get_int(0).unwrap(), 42);
        assert_eq!(row.get_int(2).unwrap(), 99);
    }

    #[test]
    fn test_get_string_variable_length() {
        let mut builder = BinaryRowBuilder::new(2);
        builder.write_string(0, "hello");
        builder.write_string(1, "world!");
        let row = builder.build();

        assert_eq!(row.get_string(0).unwrap(), "hello");
        assert_eq!(row.get_string(1).unwrap(), "world!");
    }

    #[test]
    fn test_get_binary_variable_length() {
        let mut builder = BinaryRowBuilder::new(1);
        builder.write_binary(0, b"\x00\x01\x02\x03");
        let row = builder.build();

        assert_eq!(row.get_binary(0).unwrap(), &[0x00, 0x01, 0x02, 0x03]);
    }

    #[test]
    fn test_mixed_types_partition_row() {
        let mut builder = BinaryRowBuilder::new(2);
        builder.write_string(0, "2024-01-01");
        builder.write_int(1, 12);
        let row = builder.build();

        assert_eq!(row.get_string(0).unwrap(), "2024-01-01");
        assert_eq!(row.get_int(1).unwrap(), 12);
    }

    #[test]
    fn test_serde_roundtrip_empty() {
        let row = BinaryRow::new(0);
        let json = serde_json::to_string(&row).unwrap();
        let deserialized: BinaryRow = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.arity(), 0);
        assert!(deserialized.is_empty());
    }

    #[test]
    fn test_serde_roundtrip_populated() {
        let mut builder = BinaryRowBuilder::new(2);
        builder.write_int(0, 42);
        builder.write_string(1, "hello");
        let row = builder.build();

        let json = serde_json::to_string(&row).unwrap();
        let deserialized: BinaryRow = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.arity(), row.arity());
        assert_eq!(deserialized.data(), row.data());
        assert_eq!(deserialized.get_int(0).unwrap(), 42);
        assert_eq!(deserialized.get_string(1).unwrap(), "hello");
    }

    #[test]
    fn test_from_bytes_arity_zero() {
        let data = vec![0u8; 8];
        let row = BinaryRow::from_bytes(0, data);
        assert_eq!(row.arity(), 0);
        assert!(!row.is_empty());
    }

    #[test]
    fn test_new_and_from_bytes_null_bits_size_consistent() {
        for arity in [0, 1, 2, 10, 56, 57, 100] {
            let stub = BinaryRow::new(arity);
            let data = vec![0u8; BinaryRow::cal_fix_part_size_in_bytes(arity) as usize];
            let real = BinaryRow::from_bytes(arity, data);
            assert_eq!(
                stub.null_bits_size_in_bytes, real.null_bits_size_in_bytes,
                "null_bits_size_in_bytes mismatch for arity={arity}"
            );
        }
    }

    #[test]
    fn test_get_string_inline() {
        let mut builder = BinaryRowBuilder::new(2);
        builder.write_string_inline(0, "hi");
        builder.write_string_inline(1, "7_bytes");
        let row = builder.build();

        assert_eq!(row.get_string(0).unwrap(), "hi");
        assert_eq!(row.get_string(1).unwrap(), "7_bytes");
    }

    #[test]
    fn test_get_binary_inline() {
        let mut builder = BinaryRowBuilder::new(1);
        builder.write_binary_inline(0, &[0xDE, 0xAD]);
        let row = builder.build();

        assert_eq!(row.get_binary(0).unwrap(), &[0xDE, 0xAD]);
    }

    #[test]
    fn test_get_decimal_compact() {
        let mut builder = BinaryRowBuilder::new(3);
        builder.write_decimal_compact(0, 12345);
        builder.write_decimal_compact(1, -100);
        builder.write_decimal_compact(2, 0);
        let row = builder.build();

        assert_eq!(row.get_decimal_unscaled(0, 10).unwrap(), 12345);
        assert_eq!(row.get_decimal_unscaled(1, 10).unwrap(), -100);
        assert_eq!(row.get_decimal_unscaled(2, 10).unwrap(), 0);
    }

    #[test]
    fn test_get_decimal_var_len() {
        let mut builder = BinaryRowBuilder::new(2);
        let large_pos: i128 = 10_000_000_000_000_000_000;
        builder.write_decimal_var_len(0, large_pos);
        let large_neg: i128 = -10_000_000_000_000_000_000;
        builder.write_decimal_var_len(1, large_neg);
        let row = builder.build();

        assert_eq!(row.get_decimal_unscaled(0, 20).unwrap(), large_pos);
        assert_eq!(row.get_decimal_unscaled(1, 20).unwrap(), large_neg);
    }

    #[test]
    fn test_get_timestamp_compact() {
        let epoch_millis: i64 = 1_704_067_200_000;
        let mut builder = BinaryRowBuilder::new(1);
        builder.write_timestamp_compact(0, epoch_millis);
        let row = builder.build();

        let (millis, nano) = row.get_timestamp_raw(0, 3).unwrap();
        assert_eq!(millis, epoch_millis);
        assert_eq!(nano, 0);
    }

    #[test]
    fn test_get_timestamp_non_compact() {
        let epoch_millis: i64 = 1_704_067_200_123;
        let nano_of_milli: i32 = 456_000;
        let mut builder = BinaryRowBuilder::new(1);
        builder.write_timestamp_non_compact(0, epoch_millis, nano_of_milli);
        let row = builder.build();

        let (millis, nano) = row.get_timestamp_raw(0, 6).unwrap();
        assert_eq!(millis, epoch_millis);
        assert_eq!(nano, nano_of_milli);
    }
}
