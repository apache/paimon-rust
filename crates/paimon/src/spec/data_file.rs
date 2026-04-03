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

use crate::spec::stats::BinaryTableStats;
use chrono::serde::ts_milliseconds::deserialize as from_millis;
use chrono::serde::ts_milliseconds::serialize as to_millis;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

pub const EMPTY_BINARY_ROW: BinaryRow = BinaryRow::new(0);

/// Highest bit mask for detecting inline vs variable-length encoding.
///
/// If the highest bit of the 8-byte fixed-part value is 1, the data is stored
/// inline (≤7 bytes). If 0, the data is in the variable-length part.
///
/// Reference: `BinarySection.HIGHEST_FIRST_BIT` in Java Paimon.
const HIGHEST_FIRST_BIT: u64 = 0x80 << 56;

/// Mask to extract the 7-bit length from an inline-encoded value.
///
/// Reference: `BinarySection.HIGHEST_SECOND_TO_EIGHTH_BIT` in Java Paimon.
const HIGHEST_SECOND_TO_EIGHTH_BIT: u64 = 0x7F << 56;

/// An implementation of InternalRow backed by raw binary bytes.
///
/// Binary layout (little-endian):
/// ```text
/// | header (8 bytes) | null bit set (8-byte aligned) | fixed-length (8B per field) | variable-length |
/// ```
///
/// - **Header**: byte 0 = RowKind, bytes 1-7 reserved.
/// - **Null bit set**: starts at bit index 8 (skip header bits), 1 bit per field, padded to 8-byte boundary.
/// - **Fixed-length part**: 8 bytes per field. Primitives stored directly; variable-length types store offset+length.
/// - **Variable-length part**: String / Binary data referenced by offset+length from fixed part.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/data/BinaryRow.java>
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinaryRow {
    arity: i32,
    null_bits_size_in_bytes: i32,

    /// Raw binary data backing this row. Empty when constructed via `new()`.
    /// Populated via `from_bytes()` for partition data from manifest entries.
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

    /// Create a BinaryRow stub without backing data.
    /// Use `from_bytes()` to create a BinaryRow with actual data.
    pub const fn new(arity: i32) -> Self {
        Self {
            arity,
            null_bits_size_in_bytes: Self::cal_bit_set_width_in_bytes(arity),
            data: Vec::new(),
        }
    }

    /// Create a BinaryRow from raw binary bytes.
    ///
    /// The `data` must contain the full binary row content:
    /// header + null bit set + fixed-length part + variable-length part.
    /// Does NOT include the 4-byte arity prefix (use `from_serialized_bytes` for that).
    pub fn from_bytes(arity: i32, data: Vec<u8>) -> Self {
        let null_bits_size_in_bytes = Self::cal_bit_set_width_in_bytes(arity);
        Self {
            arity,
            null_bits_size_in_bytes,
            data,
        }
    }

    /// Create a BinaryRow from Paimon's serialized format (e.g. `ManifestEntry._PARTITION`).
    ///
    /// Java `SerializationUtils.serializeBinaryRow()` prepends a 4-byte big-endian arity
    /// before the raw BinaryRow content. This method reads that prefix and strips it,
    /// matching Java `SerializationUtils.deserializeBinaryRow()`.
    ///
    /// Variable-length field offsets inside the BinaryRow content are relative to the
    /// BinaryRow base, so they remain valid after stripping the 4-byte prefix.
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

    /// Number of fields in this row.
    pub fn arity(&self) -> i32 {
        self.arity
    }

    /// Returns `true` if this row has no backing data.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns the raw backing data bytes.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    // ======================== Null check ========================

    /// Check if the field at `pos` is null.
    ///
    /// Null bits start at bit index `HEADER_SIZE_IN_BYTES` (= 8) from the base offset.
    /// Bit layout: byte_index = (bit_index) / 8, bit_within_byte = (bit_index) % 8.
    pub fn is_null_at(&self, pos: usize) -> bool {
        let bit_index = pos + Self::HEADER_SIZE_IN_BYTES as usize;
        let byte_index = bit_index / 8;
        let bit_offset = bit_index % 8;
        (self.data[byte_index] & (1 << bit_offset)) != 0
    }

    // ======================== Internal read helpers ========================

    /// Byte offset of the field value at position `pos` within `self.data`.
    fn field_offset(&self, pos: usize) -> usize {
        self.null_bits_size_in_bytes as usize + pos * 8
    }

    /// Bounds-checked slice read. Returns exactly `N` bytes starting at `offset`.
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

    /// Bounds-checked single byte read.
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

    /// Read a little-endian i64 from `self.data` at the given byte offset.
    fn read_i64_at(&self, offset: usize) -> crate::Result<i64> {
        self.read_slice::<8>(offset).map(i64::from_le_bytes)
    }

    /// Read a little-endian i32 from `self.data` at the given byte offset.
    fn read_i32_at(&self, offset: usize) -> crate::Result<i32> {
        self.read_slice::<4>(offset).map(i32::from_le_bytes)
    }

    // ======================== Fixed-length getters ========================

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

    // ======================== Variable-length getters ========================

    /// Resolve the (start, length) byte range for a variable-length field.
    ///
    /// Encoding in the fixed part (8-byte i64):
    /// - Highest bit = 0: data in variable-length part. offset = upper 32 bits, len = lower 32 bits.
    ///   Actual position = base_offset(0) + offset.
    /// - Highest bit = 1: data inline in fixed part (≤7 bytes). len = bits[62:56].
    ///   Data starts at `field_offset` (little-endian: right after the length/mark byte).
    ///
    /// Returns `Err` if the decoded byte range falls outside the backing data.
    fn resolve_var_length_field(&self, pos: usize) -> crate::Result<(usize, usize)> {
        let field_off = self.field_offset(pos);
        let raw = self.read_i64_at(field_off)? as u64;

        let (start, len) = if raw & HIGHEST_FIRST_BIT == 0 {
            // Variable-length part: offset in upper 32 bits, length in lower 32 bits.
            let offset = (raw >> 32) as usize;
            let len = (raw & 0xFFFF_FFFF) as usize;
            (offset, len)
        } else {
            // Inline: length in bits [62:56], data starts at field_offset (LE).
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

    /// Read the unscaled value of a Decimal field as `i128`.
    ///
    /// - `precision <= 18` (compact): stored as `i64` in the fixed part.
    /// - `precision > 18`: stored as big-endian two's complement bytes in the variable-length part.
    ///
    /// Reference: `BinaryRow.getDecimal` and `DecimalData.isCompact` in Java Paimon.
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
            // Big-endian two's complement, same as Java BigInteger.toByteArray().
            let negative = bytes[0] & 0x80 != 0;
            let mut val: i128 = if negative { -1 } else { 0 };
            for &b in bytes {
                val = (val << 8) | (b as i128);
            }
            Ok(val)
        }
    }

    /// Read the raw components of a Timestamp / LocalZonedTimestamp field.
    ///
    /// Returns `(epoch_millis, nano_of_milli)`.
    ///
    /// - `precision <= 3` (compact): stored as epoch millis `i64` in the fixed part,
    ///   `nano_of_milli` is 0.
    /// - `precision > 3` (non-compact): the fixed 8-byte slot stores
    ///   `(offset << 32) | nanoOfMillisecond`; the variable area at `offset` contains
    ///   an 8-byte `i64` millisecond value.
    ///
    /// Reference: `AbstractBinaryWriter.writeTimestamp` and
    /// `MemorySegmentUtils.readTimestampData` in Java Paimon.
    pub(crate) fn get_timestamp_raw(
        &self,
        pos: usize,
        precision: u32,
    ) -> crate::Result<(i64, i32)> {
        if precision <= 3 {
            Ok((self.get_long(pos)?, 0))
        } else {
            // Read the raw 8-byte fixed slot: high 32 bits = offset, low 32 bits = nanoOfMillisecond.
            let field_off = self.field_offset(pos);
            let offset_and_nano = self.read_i64_at(field_off)? as u64;
            let offset = (offset_and_nano >> 32) as usize;
            let nano_of_milli = offset_and_nano as i32;

            // Read the 8-byte millisecond value from the variable area.
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
}

/// Metadata of a data file.
///
/// Impl References: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/io/DataFileMeta.java>
#[derive(Debug, Eq, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DataFileMeta {
    #[serde(rename = "_FILE_NAME")]
    pub file_name: String,
    #[serde(rename = "_FILE_SIZE")]
    pub file_size: i64,
    // row_count tells the total number of rows (including add & delete) in this file.
    #[serde(rename = "_ROW_COUNT")]
    pub row_count: i64,
    #[serde(rename = "_MIN_KEY", with = "serde_bytes")]
    pub min_key: Vec<u8>,
    #[serde(rename = "_MAX_KEY", with = "serde_bytes")]
    pub max_key: Vec<u8>,
    #[serde(rename = "_KEY_STATS")]
    pub key_stats: BinaryTableStats,
    #[serde(rename = "_VALUE_STATS")]
    pub value_stats: BinaryTableStats,
    #[serde(rename = "_MIN_SEQUENCE_NUMBER")]
    pub min_sequence_number: i64,
    #[serde(rename = "_MAX_SEQUENCE_NUMBER")]
    pub max_sequence_number: i64,
    #[serde(rename = "_SCHEMA_ID")]
    pub schema_id: i64,
    #[serde(rename = "_LEVEL")]
    pub level: i32,
    #[serde(rename = "_EXTRA_FILES")]
    pub extra_files: Vec<String>,
    #[serde(
        rename = "_CREATION_TIME",
        serialize_with = "to_millis",
        deserialize_with = "from_millis"
    )]
    pub creation_time: DateTime<Utc>,
    #[serde(rename = "_DELETE_ROW_COUNT")]
    // rowCount = add_row_count + delete_row_count.
    pub delete_row_count: Option<i64>,
    // file index filter bytes, if it is small, store in data file meta
    #[serde(rename = "_EMBEDDED_FILE_INDEX", with = "serde_bytes")]
    pub embedded_index: Option<Vec<u8>>,

    /// The starting row ID for this file's data (used in data evolution mode).
    #[serde(
        rename = "_FIRST_ROW_ID",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub first_row_id: Option<i64>,

    /// Which table columns this file contains (used in data evolution mode).
    #[serde(
        rename = "_WRITE_COLS",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub write_cols: Option<Vec<String>>,

    /// External path for the data file (e.g. when data is stored outside the table directory).
    #[serde(
        rename = "_EXTERNAL_PATH",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub external_path: Option<String>,
}

impl Display for DataFileMeta {
    fn fmt(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

#[allow(dead_code)]
impl DataFileMeta {
    /// Returns the row ID range `[first_row_id, first_row_id + row_count - 1]` if `first_row_id` is set.
    pub fn row_id_range(&self) -> Option<(i64, i64)> {
        self.first_row_id.map(|fid| (fid, fid + self.row_count - 1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to build a BinaryRow byte buffer matching Java's BinaryRow memory layout.
    ///
    /// Layout: header (8 bytes) | null bit set (aligned) | fixed-length (8B per field) | var-length
    ///
    /// This mimics what Java `BinaryRowWriter` produces.
    struct BinaryRowBuilder {
        arity: i32,
        null_bits_size: usize,
        data: Vec<u8>,
    }

    impl BinaryRowBuilder {
        fn new(arity: i32) -> Self {
            let null_bits_size = BinaryRow::cal_bit_set_width_in_bytes(arity) as usize;
            let fixed_part_size = null_bits_size + (arity as usize) * 8;
            let data = vec![0u8; fixed_part_size];
            Self {
                arity,
                null_bits_size,
                data,
            }
        }

        fn field_offset(&self, pos: usize) -> usize {
            self.null_bits_size + pos * 8
        }

        fn set_null_at(&mut self, pos: usize) {
            let bit_index = pos + BinaryRow::HEADER_SIZE_IN_BYTES as usize;
            let byte_index = bit_index / 8;
            let bit_offset = bit_index % 8;
            self.data[byte_index] |= 1 << bit_offset;
            // Zero out the field value (Java does this too).
            let offset = self.field_offset(pos);
            self.data[offset..offset + 8].fill(0);
        }

        fn write_int(&mut self, pos: usize, value: i32) {
            let offset = self.field_offset(pos);
            let bytes = value.to_le_bytes();
            self.data[offset..offset + 4].copy_from_slice(&bytes);
        }

        fn write_long(&mut self, pos: usize, value: i64) {
            let offset = self.field_offset(pos);
            let bytes = value.to_le_bytes();
            self.data[offset..offset + 8].copy_from_slice(&bytes);
        }

        fn write_short(&mut self, pos: usize, value: i16) {
            let offset = self.field_offset(pos);
            let bytes = value.to_le_bytes();
            self.data[offset..offset + 2].copy_from_slice(&bytes);
        }

        fn write_byte(&mut self, pos: usize, value: i8) {
            let offset = self.field_offset(pos);
            self.data[offset] = value as u8;
        }

        fn write_boolean(&mut self, pos: usize, value: bool) {
            let offset = self.field_offset(pos);
            self.data[offset] = if value { 1 } else { 0 };
        }

        fn write_float(&mut self, pos: usize, value: f32) {
            let offset = self.field_offset(pos);
            let bytes = value.to_le_bytes();
            self.data[offset..offset + 4].copy_from_slice(&bytes);
        }

        fn write_double(&mut self, pos: usize, value: f64) {
            let offset = self.field_offset(pos);
            let bytes = value.to_le_bytes();
            self.data[offset..offset + 8].copy_from_slice(&bytes);
        }

        /// Write a string to the variable-length part and store offset+length in the fixed part.
        fn write_string(&mut self, pos: usize, value: &str) {
            let var_offset = self.data.len();
            self.data.extend_from_slice(value.as_bytes());
            let len = value.len();

            // Encode as: upper 32 bits = offset, lower 32 bits = length.
            let encoded = ((var_offset as u64) << 32) | (len as u64);
            let offset = self.field_offset(pos);
            let bytes = encoded.to_le_bytes();
            self.data[offset..offset + 8].copy_from_slice(&bytes);
        }

        /// Write a short string (len <= 7) inline into the fixed part.
        ///
        /// Encoding (little-endian):
        /// - Byte 7 (highest byte in LE i64): mark bit (0x80) | length (7 bits)
        /// - Bytes 0..len: the actual string data
        ///
        /// This matches Java `AbstractBinaryWriter.writeBinaryToFixLenPart`.
        fn write_string_inline(&mut self, pos: usize, value: &str) {
            assert!(
                value.len() <= 7,
                "inline string must be <= 7 bytes, got {}",
                value.len()
            );
            self.write_binary_inline(pos, value.as_bytes());
        }

        /// Write short binary data (len <= 7) inline into the fixed part.
        fn write_binary_inline(&mut self, pos: usize, value: &[u8]) {
            assert!(
                value.len() <= 7,
                "inline binary must be <= 7 bytes, got {}",
                value.len()
            );
            let offset = self.field_offset(pos);
            // Zero out the 8-byte slot first.
            self.data[offset..offset + 8].fill(0);
            // Write data into lower bytes (LE layout).
            self.data[offset..offset + value.len()].copy_from_slice(value);
            // Write mark + length into the highest byte (byte 7 in LE = offset+7).
            self.data[offset + 7] = 0x80 | (value.len() as u8);
        }

        /// Write a compact Decimal (precision <= 18) as its unscaled i64 value.
        fn write_decimal_compact(&mut self, pos: usize, unscaled: i64) {
            self.write_long(pos, unscaled);
        }

        /// Write a non-compact Decimal (precision > 18) as big-endian two's complement bytes.
        fn write_decimal_var_len(&mut self, pos: usize, unscaled: i128) {
            // Convert i128 to minimal big-endian two's complement.
            let be_bytes = unscaled.to_be_bytes();
            // Find the first significant byte (skip redundant sign-extension bytes).
            let mut start = 0;
            while start < 15 {
                let b = be_bytes[start];
                let next = be_bytes[start + 1];
                // Safe to skip if byte is pure sign extension.
                if (b == 0x00 && next & 0x80 == 0) || (b == 0xFF && next & 0x80 != 0) {
                    start += 1;
                } else {
                    break;
                }
            }
            let minimal = &be_bytes[start..];

            let var_offset = self.data.len();
            self.data.extend_from_slice(minimal);
            let len = minimal.len();
            let encoded = ((var_offset as u64) << 32) | (len as u64);
            let offset = self.field_offset(pos);
            self.data[offset..offset + 8].copy_from_slice(&encoded.to_le_bytes());
        }

        /// Write a compact Timestamp (precision <= 3) as epoch millis.
        fn write_timestamp_compact(&mut self, pos: usize, epoch_millis: i64) {
            self.write_long(pos, epoch_millis);
        }

        /// Write a non-compact Timestamp (precision > 3).
        ///
        /// Matches Java `AbstractBinaryWriter.writeTimestamp`:
        /// - Fixed slot: `(offset << 32) | nanoOfMillisecond`
        /// - Variable area: 8-byte `millisecond` (LE)
        fn write_timestamp_non_compact(
            &mut self,
            pos: usize,
            epoch_millis: i64,
            nano_of_milli: i32,
        ) {
            let var_offset = self.data.len();
            // Variable area: only the 8-byte millis value.
            self.data.extend_from_slice(&epoch_millis.to_le_bytes());
            // Fixed slot: (offset << 32) | nano_of_milli
            let encoded = ((var_offset as u64) << 32) | (nano_of_milli as u32 as u64);
            let offset = self.field_offset(pos);
            self.data[offset..offset + 8].copy_from_slice(&encoded.to_le_bytes());
        }

        fn build(self) -> BinaryRow {
            BinaryRow::from_bytes(self.arity, self.data)
        }
    }

    #[test]
    fn test_empty_binary_row() {
        let row = BinaryRow::new(0);
        assert_eq!(row.arity(), 0);
        assert!(row.is_empty());
        assert_eq!(row.data(), &[] as &[u8]);
    }

    #[test]
    fn test_binary_row_constants() {
        // arity=0: null_bits_size = ((0 + 63 + 8) / 64) * 8 = (71/64)*8 = 1*8 = 8
        assert_eq!(BinaryRow::cal_bit_set_width_in_bytes(0), 8);
        // arity=1: ((1 + 63 + 8) / 64) * 8 = (72/64)*8 = 1*8 = 8
        assert_eq!(BinaryRow::cal_bit_set_width_in_bytes(1), 8);
        // arity=56: ((56 + 63 + 8) / 64) * 8 = (127/64)*8 = 1*8 = 8
        assert_eq!(BinaryRow::cal_bit_set_width_in_bytes(56), 8);
        // arity=57: ((57 + 63 + 8) / 64) * 8 = (128/64)*8 = 2*8 = 16
        assert_eq!(BinaryRow::cal_bit_set_width_in_bytes(57), 16);
    }

    #[test]
    fn test_from_serialized_bytes() {
        // Build a raw BinaryRow with arity=1, int value 42, then prepend 4-byte BE arity prefix
        // to simulate Java SerializationUtils.serializeBinaryRow() format.
        let mut builder = BinaryRowBuilder::new(1);
        builder.write_int(0, 42);
        let raw_row = builder.build();
        let raw_data = raw_row.data();

        let mut serialized = Vec::with_capacity(4 + raw_data.len());
        serialized.extend_from_slice(&1_i32.to_be_bytes());
        serialized.extend_from_slice(raw_data);

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
        assert!(!row.is_null_at(0));
        assert!(!row.is_null_at(1));
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
        builder.write_string(0, "\x00\x01\x02\x03");
        let row = builder.build();

        assert_eq!(row.get_binary(0).unwrap(), &[0x00, 0x01, 0x02, 0x03]);
    }

    #[test]
    fn test_mixed_types_partition_row() {
        // Simulate a typical partition row: (dt: String, hr: Int)
        let mut builder = BinaryRowBuilder::new(2);
        builder.write_string(0, "2024-01-01");
        builder.write_int(1, 12);
        let row = builder.build();

        assert_eq!(row.get_string(0).unwrap(), "2024-01-01");
        assert_eq!(row.get_int(1).unwrap(), 12);
    }

    #[test]
    fn test_serde_roundtrip_empty() {
        // Verify empty BinaryRow serde roundtrip is stable.
        let row = BinaryRow::new(0);
        let json = serde_json::to_string(&row).unwrap();
        let deserialized: BinaryRow = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.arity(), 0);
        assert!(deserialized.is_empty());
    }

    #[test]
    fn test_serde_roundtrip_populated() {
        // Verify a populated BinaryRow roundtrips correctly with data intact.
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
        // Empty row with header only (8 bytes of zeros + 0 fields).
        let data = vec![0u8; 8]; // header only, null_bits_size_in_bytes = 8
        let row = BinaryRow::from_bytes(0, data);
        assert_eq!(row.arity(), 0);
        assert!(!row.is_empty());
    }

    #[test]
    fn test_new_and_from_bytes_null_bits_size_consistent() {
        // Verify that new() and from_bytes() produce the same null_bits_size_in_bytes.
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
        // Test the inline encoding path (len <= 7).
        // Java BinaryRowWriter inlines short strings: highest bit = 1, len in bits[62:56],
        // data occupies the lower bytes of the 8-byte fixed-part slot.
        let mut builder = BinaryRowBuilder::new(2);
        builder.write_string_inline(0, "hi");
        builder.write_string_inline(1, "7_bytes"); // exactly 7 bytes
        let row = builder.build();

        assert_eq!(row.get_string(0).unwrap(), "hi");
        assert_eq!(row.get_string(1).unwrap(), "7_bytes");
    }

    #[test]
    fn test_get_binary_inline() {
        // Test inline binary (len <= 7).
        let mut builder = BinaryRowBuilder::new(1);
        builder.write_binary_inline(0, &[0xDE, 0xAD]);
        let row = builder.build();

        assert_eq!(row.get_binary(0).unwrap(), &[0xDE, 0xAD]);
    }

    #[test]
    fn test_get_decimal_compact() {
        // precision <= 18: stored as i64 unscaled value.
        let mut builder = BinaryRowBuilder::new(3);
        builder.write_decimal_compact(0, 12345); // 12.345 with scale=3
        builder.write_decimal_compact(1, -100); // -0.100 with scale=3
        builder.write_decimal_compact(2, 0); // 0.000 with scale=3
        let row = builder.build();

        assert_eq!(row.get_decimal_unscaled(0, 10).unwrap(), 12345);
        assert_eq!(row.get_decimal_unscaled(1, 10).unwrap(), -100);
        assert_eq!(row.get_decimal_unscaled(2, 10).unwrap(), 0);
    }

    #[test]
    fn test_get_decimal_var_len() {
        // precision > 18: stored as big-endian two's complement in var-len part.
        let mut builder = BinaryRowBuilder::new(2);
        // Large positive: 10^19 = 10_000_000_000_000_000_000
        let large_pos: i128 = 10_000_000_000_000_000_000;
        builder.write_decimal_var_len(0, large_pos);
        // Large negative
        let large_neg: i128 = -10_000_000_000_000_000_000;
        builder.write_decimal_var_len(1, large_neg);
        let row = builder.build();

        assert_eq!(row.get_decimal_unscaled(0, 20).unwrap(), large_pos);
        assert_eq!(row.get_decimal_unscaled(1, 20).unwrap(), large_neg);
    }

    #[test]
    fn test_get_timestamp_compact() {
        // precision <= 3: stored as epoch millis i64.
        let epoch_millis: i64 = 1_704_067_200_000; // 2024-01-01 00:00:00 UTC
        let mut builder = BinaryRowBuilder::new(1);
        builder.write_timestamp_compact(0, epoch_millis);
        let row = builder.build();

        let (millis, nano) = row.get_timestamp_raw(0, 3).unwrap();
        assert_eq!(millis, epoch_millis);
        assert_eq!(nano, 0);
    }

    #[test]
    fn test_get_timestamp_non_compact() {
        // precision > 3: fixed slot = (offset << 32 | nano_of_milli),
        // variable area = 8 bytes millis.
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
