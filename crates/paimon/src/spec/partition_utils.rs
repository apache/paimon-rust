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

//! Partition path generation utilities.
//!
//! Converts a partition `BinaryRow` into an escaped directory path like `dt=2024-01-01/hr=12/`.
//!
//! Reference:
//! - Java `InternalRowPartitionComputer` for value formatting
//! - Java `PartitionPathUtils` for path escaping and assembly

use crate::error::*;
use crate::spec::types::DataType;
use crate::spec::BinaryRow;
use crate::spec::DataField;
use chrono::{Local, NaiveDate, NaiveDateTime, TimeZone, Timelike};

const MILLIS_PER_DAY: i64 = 86_400_000;

/// The directory holding one bucket's data files, `<table>/[<partition>/]bucket-N`.
///
/// Mirrors Java `FileStorePathFactory.bucketPath`. Every consumer that needs a
/// bucket directory — data files, and index files kept beside them — must derive
/// it here: a writer and a reader that disagree by one segment silently lose the
/// file, and there is no compile error to catch that.
///
/// `partition_computer` is `None` for an unpartitioned table, whose buckets sit
/// directly under the table path.
pub(crate) fn bucket_path(
    table_path: &str,
    partition_computer: Option<&PartitionComputer>,
    partition: &BinaryRow,
    bucket: i32,
) -> crate::Result<String> {
    let partition_path = match partition_computer {
        Some(computer) => computer.generate_partition_path(partition)?,
        None => String::new(),
    };
    Ok(bucket_path_under(table_path, &partition_path, bucket))
}

/// [`bucket_path`] for a partition directory that is already computed.
///
/// `partition_path` is empty for an unpartitioned table and otherwise ends with `/`,
/// matching [`PartitionComputer::generate_partition_path`].
pub(crate) fn bucket_path_under(table_path: &str, partition_path: &str, bucket: i32) -> String {
    format!(
        "{}/{partition_path}{}",
        table_path.trim_end_matches('/'),
        crate::spec::bucket_dir_name(bucket)
    )
}

/// Computes partition string values and directory paths from a partition `BinaryRow`.
///
/// Mirrors Java `InternalRowPartitionComputer` — holds resolved partition field metadata
/// and provides both `generate_part_values` (key-value pairs) and `generate_partition_path`
/// (escaped directory path).
///
/// Reference: `org.apache.paimon.utils.InternalRowPartitionComputer` in Java Paimon.
#[derive(Debug, Clone)]
pub(crate) struct PartitionComputer {
    partition_keys: Vec<String>,
    partition_fields: Vec<DataField>,
    default_partition_name: String,
    legacy_partition_name: bool,
}

impl PartitionComputer {
    /// Create a new `PartitionComputer`.
    ///
    /// Resolves partition key names to their `DataField` definitions from `schema_fields`.
    /// Returns an error if any partition key is not found in the schema.
    pub(crate) fn new(
        partition_keys: &[String],
        schema_fields: &[DataField],
        default_partition_name: &str,
        legacy_partition_name: bool,
    ) -> crate::Result<Self> {
        let partition_fields = resolve_partition_fields(partition_keys, schema_fields)?
            .into_iter()
            .cloned()
            .collect();
        Ok(Self {
            partition_keys: partition_keys.to_vec(),
            partition_fields,
            default_partition_name: default_partition_name.to_string(),
            legacy_partition_name,
        })
    }

    /// Generate partition key-value pairs from a `BinaryRow`.
    ///
    /// Returns an ordered list of `(key, value)` tuples, e.g. `[("dt", "2024-01-01"), ("hr", "12")]`.
    /// Null or blank values are replaced by `default_partition_name`.
    ///
    /// This is the Rust equivalent of Java `InternalRowPartitionComputer.generatePartValues()`.
    pub(crate) fn generate_part_values(
        &self,
        row: &BinaryRow,
    ) -> crate::Result<Vec<(String, String)>> {
        self.validate_row(row)?;

        self.partition_keys
            .iter()
            .zip(self.partition_fields.iter())
            .enumerate()
            .map(|(i, (key, field))| {
                let value = format_partition_value(
                    row,
                    i,
                    field.data_type(),
                    &self.default_partition_name,
                    self.legacy_partition_name,
                )?;
                Ok((key.clone(), value))
            })
            .collect()
    }

    /// Generate the partition directory path from a `BinaryRow`.
    ///
    /// Returns a path like `dt=2024-01-01/hr=12/` with escaped key=value segments.
    /// Returns an empty string if `partition_keys` is empty.
    pub(crate) fn generate_partition_path(&self, row: &BinaryRow) -> crate::Result<String> {
        if self.partition_keys.is_empty() {
            return Ok(String::new());
        }

        let part_values = self.generate_part_values(row)?;
        Ok(assemble_partition_path(&part_values))
    }

    /// Validate that the `BinaryRow` is compatible with this computer's partition keys.
    fn validate_row(&self, row: &BinaryRow) -> crate::Result<()> {
        if self.partition_keys.len() != row.arity() as usize {
            return Err(Error::UnexpectedError {
                message: format!(
                    "Partition keys length ({}) does not match row arity ({})",
                    self.partition_keys.len(),
                    row.arity()
                ),
                source: None,
            });
        }

        if row.is_empty() {
            return Err(Error::UnexpectedError {
                message: "Partition row has no backing data but arity > 0".to_string(),
                source: None,
            });
        }

        // Validate that the backing data is large enough for null-bits + fixed-part.
        let min_size = BinaryRow::cal_bit_set_width_in_bytes(row.arity()) as usize
            + (row.arity() as usize) * 8;
        if row.data().len() < min_size {
            return Err(Error::UnexpectedError {
                message: format!(
                    "Partition BinaryRow data too short: need at least {} bytes, got {}",
                    min_size,
                    row.data().len()
                ),
                source: None,
            });
        }

        Ok(())
    }
}

/// Resolve the `DataField` for each partition key from the schema fields, preserving order.
fn resolve_partition_fields<'a>(
    partition_keys: &[String],
    schema_fields: &'a [DataField],
) -> crate::Result<Vec<&'a DataField>> {
    partition_keys
        .iter()
        .map(|key| {
            schema_fields
                .iter()
                .find(|f| f.name() == key)
                .ok_or_else(|| Error::UnexpectedError {
                    message: format!("Partition key '{key}' not found in schema fields"),
                    source: None,
                })
        })
        .collect()
}

/// Assemble escaped `key=value/...` path from partition key-value pairs.
fn assemble_partition_path(part_values: &[(String, String)]) -> String {
    let mut path = String::new();
    for (i, (key, value)) in part_values.iter().enumerate() {
        if i > 0 {
            path.push('/');
        }
        path.push_str(&escape_path_name(key));
        path.push('=');
        path.push_str(&escape_path_name(value));
    }
    path.push('/');
    path
}

/// Format a single partition field value to its string representation.
fn format_partition_value(
    row: &BinaryRow,
    pos: usize,
    data_type: &DataType,
    default_partition_name: &str,
    legacy: bool,
) -> crate::Result<String> {
    if row.is_null_at(pos) {
        return Ok(default_partition_name.to_string());
    }

    let value = match data_type {
        DataType::Boolean(_) => row.get_boolean(pos)?.to_string(),
        DataType::TinyInt(_) => row.get_byte(pos)?.to_string(),
        DataType::SmallInt(_) => row.get_short(pos)?.to_string(),
        DataType::Int(_) => row.get_int(pos)?.to_string(),
        DataType::BigInt(_) => row.get_long(pos)?.to_string(),

        DataType::Char(_) | DataType::VarChar(_) => {
            let s = row.get_string(pos)?;
            if s.trim().is_empty() {
                return Ok(default_partition_name.to_string());
            }
            s.to_string()
        }

        DataType::Binary(_) | DataType::VarBinary(_) if !legacy => {
            // Java's BinaryToStringCastRule wraps the raw bytes in a
            // BinaryString, whose toString decodes them as UTF-8.
            let value = decode_java_utf8(row.get_binary(pos)?);
            if is_java_whitespace_only(&value) {
                return Ok(default_partition_name.to_string());
            }
            value
        }

        DataType::Date(_) => {
            if legacy {
                // Legacy: field.toString() on the epoch-day Integer → raw int value.
                row.get_int(pos)?.to_string()
            } else {
                format_date(row.get_int(pos)?)
            }
        }

        DataType::Decimal(d) => {
            let unscaled = row.get_decimal_unscaled(pos, d.precision())?;
            format_decimal_plain(unscaled, d.scale())
        }

        DataType::Timestamp(t) => {
            let (millis, nano_of_milli) = row.get_timestamp_raw(pos, t.precision())?;
            let dt = millis_to_naive_datetime(millis, nano_of_milli);
            if legacy {
                format_timestamp_legacy(dt)
            } else {
                format_timestamp_non_legacy(dt, t.precision())
            }
        }

        DataType::LocalZonedTimestamp(t) => {
            let (millis, nano_of_milli) = row.get_timestamp_raw(pos, t.precision())?;
            let dt = if legacy {
                // Legacy Timestamp.toString() does not apply timezone conversion.
                millis_to_naive_datetime(millis, nano_of_milli)
            } else {
                // Non-legacy TimestampToStringCastRule applies TimeZone.getDefault().
                epoch_millis_to_local_datetime(millis, nano_of_milli)
            };
            if legacy {
                format_timestamp_legacy(dt)
            } else {
                format_timestamp_non_legacy(dt, t.precision())
            }
        }

        DataType::Time(t) => {
            if legacy {
                // Legacy: field.toString() on the internal int (millis since midnight).
                row.get_int(pos)?.to_string()
            } else {
                format_time(row.get_int(pos)?, t.precision())
            }
        }

        // Float/Double: Rust f32/f64 Display differs from Java Float/Double.toString()
        // on edge-case values. This could silently produce wrong partition paths.
        // Since float partition keys are extremely rare, reject them explicitly.
        DataType::Float(_)
        | DataType::Double(_)
        | DataType::Binary(_)
        | DataType::VarBinary(_)
        | DataType::Variant(_)
        | DataType::Blob(_)
        | DataType::Array(_)
        | DataType::Map(_)
        | DataType::Multiset(_)
        | DataType::Row(_)
        | DataType::Vector(_) => {
            return Err(Error::Unsupported {
                message: format!("{data_type:?} type is not supported as partition key"),
            });
        }
    };

    Ok(value)
}

/// Decode as Java's UTF-8 decoder does for `BinaryString.toString()`.
/// Rust's lossy decoder replaces each byte of a UTF-8 encoded surrogate,
/// while Java replaces the complete malformed surrogate sequence once.
fn decode_java_utf8(mut bytes: &[u8]) -> String {
    let mut decoded = String::with_capacity(bytes.len());
    loop {
        match std::str::from_utf8(bytes) {
            Ok(valid) => {
                decoded.push_str(valid);
                break;
            }
            Err(error) => {
                let valid_len = error.valid_up_to();
                decoded.push_str(std::str::from_utf8(&bytes[..valid_len]).unwrap());
                bytes = &bytes[valid_len..];

                let malformed_len =
                    if bytes.len() >= 2 && bytes[0] == 0xED && (0xA0..=0xBF).contains(&bytes[1]) {
                        // The first two bytes denote a surrogate. Java consumes
                        // its third byte too when it is a continuation byte.
                        if bytes.get(2).is_some_and(|byte| byte & 0xC0 == 0x80) {
                            3
                        } else {
                            2
                        }
                    } else {
                        error.error_len().unwrap_or(bytes.len())
                    };
                decoded.push('\u{FFFD}');
                bytes = &bytes[malformed_len..];
            }
        }
    }
    decoded
}

/// Java `StringUtils.isNullOrWhitespaceOnly` checks each UTF-16 code unit with
/// `Character.isWhitespace`; its whitespace set differs from Rust `str::trim`.
fn is_java_whitespace_only(value: &str) -> bool {
    value.chars().all(|ch| {
        matches!(
            ch,
            '\u{0009}'..='\u{000D}'
                | '\u{001C}'..='\u{0020}'
                | '\u{1680}'
                | '\u{180E}'
                | '\u{2000}'..='\u{2006}'
                | '\u{2008}'..='\u{200A}'
                | '\u{2028}'..='\u{2029}'
                | '\u{205F}'
                | '\u{3000}'
        )
    })
}

/// Format epoch days (since 1970-01-01) to `yyyy-MM-dd`.
fn format_date(epoch_days: i32) -> String {
    // chrono epoch is 0001-01-01; offset = 719_163 days between 0001-01-01 and 1970-01-01.
    let date = NaiveDate::from_num_days_from_ce_opt(epoch_days + 719_163)
        .unwrap_or(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
    date.format("%Y-%m-%d").to_string()
}

/// Format millis-since-midnight to `HH:mm:ss[.f...]` for non-legacy TIME partitions.
///
/// Matches Java `DateTimeUtils.formatTimestampMillis(time, precision)`:
/// outputs up to `precision` fractional digits, stripping trailing zeros.
fn format_time(millis_of_day: i32, precision: u32) -> String {
    let mut ms = millis_of_day;
    // Handle negative millis (same guard as Java)
    while ms < 0 {
        ms += 86_400_000;
    }
    let ms = ms as u32;
    let h = ms / 3_600_000;
    let m = (ms % 3_600_000) / 60_000;
    let s = (ms % 60_000) / 1_000;
    let mut frac_ms = ms % 1_000;

    let hms = format!("{h:02}:{m:02}:{s:02}");
    if precision == 0 || frac_ms == 0 {
        return hms;
    }

    // Emit up to `precision` digits, each extracted from the ms value.
    // Matches Java's digit-by-digit loop with trailing-zero break.
    let mut frac = String::with_capacity(precision as usize);
    let mut remaining = precision;
    while remaining > 0 {
        frac.push((b'0' + (frac_ms / 100) as u8) as char);
        frac_ms = (frac_ms % 100) * 10;
        if frac_ms == 0 {
            break;
        }
        remaining -= 1;
    }

    format!("{hms}.{frac}")
}

/// Format an unscaled i128 value with the given scale to a plain decimal string.
///
/// Matches Java `BigDecimal.toPlainString()` semantics: no scientific notation,
/// with correct sign, leading zeros, and decimal point placement.
fn format_decimal_plain(unscaled: i128, scale: u32) -> String {
    if scale == 0 {
        return unscaled.to_string();
    }

    let negative = unscaled < 0;
    let abs = if unscaled == i128::MIN {
        // i128::MIN.abs() would overflow; handle via wrapping.
        (i128::MAX as u128) + 1
    } else {
        unscaled.unsigned_abs()
    };

    let digits = abs.to_string();
    let scale = scale as usize;

    let result = if digits.len() <= scale {
        // Need leading zeros: e.g. unscaled=5, scale=3 → "0.005"
        let mut s = String::with_capacity(scale + 2);
        s.push_str("0.");
        for _ in 0..(scale - digits.len()) {
            s.push('0');
        }
        s.push_str(&digits);
        s
    } else {
        // Insert decimal point: e.g. unscaled=12345, scale=3 → "12.345"
        let int_len = digits.len() - scale;
        let mut s = String::with_capacity(digits.len() + 1);
        s.push_str(&digits[..int_len]);
        s.push('.');
        s.push_str(&digits[int_len..]);
        s
    };

    if negative {
        let mut s = String::with_capacity(result.len() + 1);
        s.push('-');
        s.push_str(&result);
        s
    } else {
        result
    }
}

/// Convert epoch millis + nano_of_milli to `NaiveDateTime`.
///
/// Follows Java `Timestamp.toLocalDateTime()` semantics.
fn millis_to_naive_datetime(millis: i64, nano_of_milli: i32) -> NaiveDateTime {
    let mut days = millis / MILLIS_PER_DAY;
    let mut millis_of_day = millis % MILLIS_PER_DAY;
    if millis_of_day < 0 {
        days -= 1;
        millis_of_day += MILLIS_PER_DAY;
    }
    let nano_of_day = millis_of_day as u64 * 1_000_000 + nano_of_milli as u64;
    let date = NaiveDate::from_num_days_from_ce_opt(days as i32 + 719_163)
        .unwrap_or(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
    let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(
        (nano_of_day / 1_000_000_000) as u32,
        (nano_of_day % 1_000_000_000) as u32,
    )
    .unwrap_or_default();
    NaiveDateTime::new(date, time)
}

/// Convert epoch millis to local-timezone `NaiveDateTime`.
fn epoch_millis_to_local_datetime(millis: i64, nano_of_milli: i32) -> NaiveDateTime {
    let secs = millis.div_euclid(1000);
    let milli_remainder = millis.rem_euclid(1000) as u32;
    let nanos = milli_remainder * 1_000_000 + nano_of_milli as u32;
    let dt = Local
        .timestamp_opt(secs, nanos)
        .single()
        .unwrap_or_else(|| Local.timestamp_opt(0, 0).unwrap());
    dt.naive_local()
}

/// Format a timestamp using Java `LocalDateTime.toString()` semantics (legacy mode).
///
/// - Omits seconds if seconds == 0 and nanos == 0: `2024-01-01T12:34`
/// - Omits fractional part if nanos == 0: `2024-01-01T12:34:56`
/// - Uses 3, 6, or 9 fractional digits: `2024-01-01T12:34:56.100`
fn format_timestamp_legacy(dt: NaiveDateTime) -> String {
    let nano = dt.nanosecond();
    let sec = dt.second();

    let date_hour_min = dt.format("%Y-%m-%dT%H:%M").to_string();

    if sec == 0 && nano == 0 {
        return date_hour_min;
    }

    let mut result = format!("{date_hour_min}:{sec:02}");
    if nano > 0 {
        let frac = format!("{nano:09}");
        let digits = if nano.is_multiple_of(1_000_000) {
            3
        } else if nano.is_multiple_of(1_000) {
            6
        } else {
            9
        };
        result.push('.');
        result.push_str(&frac[..digits]);
    }
    result
}

/// Format a timestamp using non-legacy `DateTimeUtils.formatTimestamp()` semantics.
///
/// Always uses space separator: `yyyy-MM-dd HH:mm:ss[.fraction]`.
/// Fraction: pad nano to 9 digits, strip trailing zeros while keeping at least `precision` digits.
fn format_timestamp_non_legacy(dt: NaiveDateTime, precision: u32) -> String {
    let nano = dt.nanosecond();
    let ymdhms = dt.format("%Y-%m-%d %H:%M:%S").to_string();

    // Whole seconds keep the declared precision; nonzero fractions are never truncated.
    let nano_str = format!("{nano:09}");
    let mut fraction = &nano_str[..];

    // Strip trailing zeros, but don't go below the precision boundary.
    while fraction.len() > precision as usize && fraction.ends_with('0') {
        fraction = &fraction[..fraction.len() - 1];
    }

    if fraction.is_empty() {
        ymdhms
    } else {
        format!("{ymdhms}.{fraction}")
    }
}

/// Escape a path component following Java `PartitionPathUtils.escapePathName`.
///
/// Characters that need escaping are encoded as `%XX` (uppercase hex).
pub(crate) fn escape_path_name(path: &str) -> String {
    if !path.chars().any(needs_escaping) {
        return path.to_string();
    }

    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut sb = String::with_capacity(path.len() + 8);
    for c in path.chars() {
        if needs_escaping(c) {
            // Escape each byte of the UTF-8 encoding.
            let mut buf = [0u8; 4];
            let encoded = c.encode_utf8(&mut buf);
            for &b in encoded.as_bytes() {
                sb.push('%');
                sb.push(HEX[(b >> 4) as usize] as char);
                sb.push(HEX[(b & 0x0F) as usize] as char);
            }
        } else {
            sb.push(c);
        }
    }
    sb
}

/// Unescape a path component following Java `PartitionPathUtils.unescapePathName`.
pub(crate) fn unescape_path_name(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return None;
            }
            let hi = hex_value(bytes[i + 1])?;
            let lo = hex_value(bytes[i + 2])?;
            out.push((hi << 4) | lo);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// Check if a character needs escaping in partition path names.
///
/// Matches Java `PartitionPathUtils.CHAR_TO_ESCAPE`:
/// - ASCII control characters (0x00-0x1F, 0x7F)
/// - Special characters: `"#%'*/:\=?{}[]^`
fn needs_escaping(c: char) -> bool {
    matches!(
        c,
        '\x00'
            ..='\x1F'
                | '\x7F'
                | '"'
                | '#'
                | '%'
                | '\''
                | '*'
                | '/'
                | ':'
                | '='
                | '?'
                | '\\'
                | '{'
                | '}'
                | '['
                | ']'
                | '^'
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::types::*;
    use crate::spec::DataField;

    // ======================== Test helpers ========================

    struct TestRowBuilder {
        arity: i32,
        null_bits_size: usize,
        data: Vec<u8>,
    }

    impl TestRowBuilder {
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
            let offset = self.field_offset(pos);
            self.data[offset..offset + 8].fill(0);
        }

        fn write_int(&mut self, pos: usize, value: i32) {
            let offset = self.field_offset(pos);
            self.data[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
        }

        fn write_long(&mut self, pos: usize, value: i64) {
            let offset = self.field_offset(pos);
            self.data[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
        }

        fn write_boolean(&mut self, pos: usize, value: bool) {
            let offset = self.field_offset(pos);
            self.data[offset] = u8::from(value);
        }

        fn write_string(&mut self, pos: usize, value: &str) {
            self.write_bytes(pos, value.as_bytes());
        }

        fn write_bytes(&mut self, pos: usize, value: &[u8]) {
            let var_offset = self.data.len();
            self.data.extend_from_slice(value);
            let len = value.len();
            let encoded = ((var_offset as u64) << 32) | (len as u64);
            let offset = self.field_offset(pos);
            self.data[offset..offset + 8].copy_from_slice(&encoded.to_le_bytes());
        }

        fn write_timestamp_compact(&mut self, pos: usize, epoch_millis: i64) {
            self.write_long(pos, epoch_millis);
        }

        fn write_decimal_compact(&mut self, pos: usize, unscaled: i64) {
            self.write_long(pos, unscaled);
        }

        fn build(self) -> BinaryRow {
            BinaryRow::from_bytes(self.arity, self.data)
        }
    }

    fn make_field(name: &str, data_type: DataType) -> DataField {
        DataField::new(0, name.to_string(), data_type)
    }

    const TEST_DEFAULT_PARTITION_NAME: &str = "__DEFAULT_PARTITION__";

    /// Helper: assert single-column partition path for a given type and row writer.
    fn assert_single_partition<F>(
        name: &str,
        data_type: DataType,
        write_fn: F,
        expected: &str,
        legacy: bool,
    ) where
        F: FnOnce(&mut TestRowBuilder),
    {
        let fields = vec![make_field(name, data_type)];
        let keys = vec![name.to_string()];
        let computer =
            PartitionComputer::new(&keys, &fields, TEST_DEFAULT_PARTITION_NAME, legacy).unwrap();
        let mut builder = TestRowBuilder::new(1);
        write_fn(&mut builder);
        let row = builder.build();
        let result = computer.generate_partition_path(&row).unwrap();
        assert_eq!(result, expected);
    }

    /// Helper: assert single-column partition path returns an error.
    fn assert_single_partition_err<F>(name: &str, data_type: DataType, write_fn: F, legacy: bool)
    where
        F: FnOnce(&mut TestRowBuilder),
    {
        let fields = vec![make_field(name, data_type)];
        let keys = vec![name.to_string()];
        let computer =
            PartitionComputer::new(&keys, &fields, TEST_DEFAULT_PARTITION_NAME, legacy).unwrap();
        let mut builder = TestRowBuilder::new(1);
        write_fn(&mut builder);
        let row = builder.build();
        assert!(computer.generate_partition_path(&row).is_err());
    }

    // ======================== Escape tests ========================

    #[test]
    fn test_escape_path_name_no_escape() {
        assert_eq!(escape_path_name("hello"), "hello");
        assert_eq!(escape_path_name("2024-01-01"), "2024-01-01");
    }

    #[test]
    fn test_escape_path_name_special_chars() {
        assert_eq!(escape_path_name("a=b"), "a%3Db");
        assert_eq!(escape_path_name("a/b"), "a%2Fb");
        assert_eq!(escape_path_name("a b"), "a b"); // space is NOT escaped
    }

    #[test]
    fn test_escape_path_name_control_chars() {
        assert_eq!(escape_path_name("a\x01b"), "a%01b");
        assert_eq!(escape_path_name("a\nb"), "a%0Ab");
        assert_eq!(escape_path_name("a\x7Fb"), "a%7Fb");
    }

    #[test]
    fn test_unescape_path_name() {
        assert_eq!(unescape_path_name("a%2Fb"), Some("a/b".to_string()));
        assert_eq!(unescape_path_name("%E4%B8%AD"), Some("中".to_string()));
        assert_eq!(unescape_path_name("a%ZZb"), None);
        assert_eq!(unescape_path_name("a%b"), None);
    }

    // ======================== PartitionComputer tests ========================

    #[test]
    fn test_partition_computer_generate_part_values() {
        let fields = vec![
            make_field("dt", DataType::VarChar(VarCharType::default())),
            make_field("hr", DataType::Int(IntType::new())),
        ];
        let keys = vec!["dt".to_string(), "hr".to_string()];
        let computer =
            PartitionComputer::new(&keys, &fields, TEST_DEFAULT_PARTITION_NAME, true).unwrap();

        let mut builder = TestRowBuilder::new(2);
        builder.write_string(0, "2024-01-01");
        builder.write_int(1, 12);
        let row = builder.build();

        let values = computer.generate_part_values(&row).unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0], ("dt".to_string(), "2024-01-01".to_string()));
        assert_eq!(values[1], ("hr".to_string(), "12".to_string()));
    }

    #[test]
    fn test_partition_computer_generate_path() {
        let fields = vec![
            make_field("dt", DataType::VarChar(VarCharType::default())),
            make_field("hr", DataType::Int(IntType::new())),
        ];
        let keys = vec!["dt".to_string(), "hr".to_string()];
        let computer =
            PartitionComputer::new(&keys, &fields, TEST_DEFAULT_PARTITION_NAME, true).unwrap();

        let mut builder = TestRowBuilder::new(2);
        builder.write_string(0, "2024-01-01");
        builder.write_int(1, 12);
        let row = builder.build();

        let path = computer.generate_partition_path(&row).unwrap();
        assert_eq!(path, "dt=2024-01-01/hr=12/");
    }

    // ======================== Path generation tests ========================

    #[test]
    fn test_empty_partition_keys() {
        let row = BinaryRow::new(0);
        let computer = PartitionComputer::new(&[], &[], TEST_DEFAULT_PARTITION_NAME, true).unwrap();
        let result = computer.generate_partition_path(&row).unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn test_single_string_partition() {
        assert_single_partition(
            "dt",
            DataType::VarChar(VarCharType::default()),
            |b| b.write_string(0, "2024-01-01"),
            "dt=2024-01-01/",
            true,
        );
    }

    #[test]
    fn test_multi_column_partition() {
        let fields = vec![
            make_field("dt", DataType::VarChar(VarCharType::default())),
            make_field("hr", DataType::Int(IntType::new())),
        ];
        let keys = vec!["dt".to_string(), "hr".to_string()];
        let computer =
            PartitionComputer::new(&keys, &fields, TEST_DEFAULT_PARTITION_NAME, true).unwrap();

        let mut builder = TestRowBuilder::new(2);
        builder.write_string(0, "2024-01-01");
        builder.write_int(1, 12);
        let row = builder.build();

        let result = computer.generate_partition_path(&row).unwrap();
        assert_eq!(result, "dt=2024-01-01/hr=12/");
    }

    #[test]
    fn test_null_partition_value() {
        let fields = vec![make_field("dt", DataType::VarChar(VarCharType::default()))];
        let keys = vec!["dt".to_string()];
        let computer =
            PartitionComputer::new(&keys, &fields, TEST_DEFAULT_PARTITION_NAME, true).unwrap();

        let mut builder = TestRowBuilder::new(1);
        builder.set_null_at(0);
        let row = builder.build();

        let result = computer.generate_partition_path(&row).unwrap();
        assert_eq!(result, "dt=__DEFAULT_PARTITION__/");
    }

    #[test]
    fn test_blank_string_partition() {
        // Empty string
        assert_single_partition(
            "dt",
            DataType::VarChar(VarCharType::default()),
            |b| b.write_string(0, ""),
            "dt=__DEFAULT_PARTITION__/",
            true,
        );
        // Whitespace only
        assert_single_partition(
            "dt",
            DataType::VarChar(VarCharType::default()),
            |b| b.write_string(0, "   "),
            "dt=__DEFAULT_PARTITION__/",
            true,
        );
    }

    #[test]
    fn test_boolean_partition() {
        assert_single_partition(
            "flag",
            DataType::Boolean(BooleanType::new()),
            |b| b.write_boolean(0, true),
            "flag=true/",
            true,
        );
    }

    // ======================== Date formatting tests ========================

    #[test]
    fn test_date_formatting() {
        assert_eq!(format_date(0), "1970-01-01");
        assert_eq!(format_date(-1), "1969-12-31");
    }

    #[test]
    fn test_date_partition_legacy() {
        assert_single_partition(
            "dt",
            DataType::Date(DateType::new()),
            |b| b.write_int(0, 19723), // 2024-01-01
            "dt=19723/",
            true,
        );
    }

    #[test]
    fn test_date_partition_non_legacy() {
        assert_single_partition(
            "dt",
            DataType::Date(DateType::new()),
            |b| b.write_int(0, 19723), // 2024-01-01
            "dt=2024-01-01/",
            false,
        );
    }

    // ======================== Decimal formatting tests ========================

    #[test]
    fn test_decimal_plain_string() {
        // Basic cases matching Java BigDecimal.toPlainString()
        assert_eq!(format_decimal_plain(12345, 3), "12.345");
        assert_eq!(format_decimal_plain(-100, 3), "-0.100");
        assert_eq!(format_decimal_plain(5, 3), "0.005");
        assert_eq!(format_decimal_plain(42, 0), "42");
        assert_eq!(format_decimal_plain(0, 3), "0.000");
        assert_eq!(format_decimal_plain(-12345, 2), "-123.45");
        assert_eq!(format_decimal_plain(1, 1), "0.1");
    }

    #[test]
    fn test_decimal_partition() {
        assert_single_partition(
            "amount",
            DataType::Decimal(DecimalType::new(10, 3).unwrap()),
            |b| b.write_decimal_compact(0, 12345), // 12.345
            "amount=12.345/",
            true,
        );
    }

    // ======================== Timestamp formatting tests ========================

    #[test]
    fn test_timestamp_legacy_formatting() {
        // All 3 branches of format_timestamp_legacy:
        // 1. sec==0 && nano==0: omit seconds
        let dt1 = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(12, 34, 0)
            .unwrap();
        assert_eq!(format_timestamp_legacy(dt1), "2024-01-01T12:34");
        // 2. sec>0, nano==0: include seconds, no fraction
        let dt2 = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(12, 34, 56)
            .unwrap();
        assert_eq!(format_timestamp_legacy(dt2), "2024-01-01T12:34:56");
        // Java LocalDateTime.toString() emits fractions in groups of three digits.
        for (nanos, expected) in [
            (100_000_000, "2024-01-01T12:34:56.100"),
            (120_000_000, "2024-01-01T12:34:56.120"),
            (123_000_000, "2024-01-01T12:34:56.123"),
            (123_400_000, "2024-01-01T12:34:56.123400"),
            (123_456_000, "2024-01-01T12:34:56.123456"),
            (123_456_700, "2024-01-01T12:34:56.123456700"),
            (1, "2024-01-01T12:34:56.000000001"),
        ] {
            let dt = NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_nano_opt(12, 34, 56, nanos)
                .unwrap();
            assert_eq!(format_timestamp_legacy(dt), expected);
        }
    }

    #[test]
    fn test_timestamp_non_legacy_formatting() {
        let dt = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_nano_opt(12, 0, 0, 123_000_000)
            .unwrap();
        // Precision is a minimum fraction width, not a truncation boundary.
        assert_eq!(
            format_timestamp_non_legacy(dt, 0),
            "2024-01-01 12:00:00.123"
        );
        // precision=3 → strip trailing zeros down to 3 digits
        assert_eq!(
            format_timestamp_non_legacy(dt, 3),
            "2024-01-01 12:00:00.123"
        );
        // precision=9 → preserve all 9 digits (no stripping since len == precision)
        assert_eq!(
            format_timestamp_non_legacy(dt, 9),
            "2024-01-01 12:00:00.123000000"
        );
    }

    #[test]
    fn test_timestamp_non_legacy_whole_seconds_keep_declared_precision() {
        let dt = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        for (precision, expected) in [
            (0, "2024-01-01 12:00:00"),
            (1, "2024-01-01 12:00:00.0"),
            (3, "2024-01-01 12:00:00.000"),
            (6, "2024-01-01 12:00:00.000000"),
            (9, "2024-01-01 12:00:00.000000000"),
        ] {
            assert_eq!(format_timestamp_non_legacy(dt, precision), expected);
        }
    }

    #[test]
    fn test_timestamp_non_legacy_preserves_significant_fraction_digits() {
        for (nanos, precision, expected) in [
            (120_000_000, 0, "2024-01-01 12:00:00.12"),
            (120_000_000, 3, "2024-01-01 12:00:00.120"),
            (123_456_700, 0, "2024-01-01 12:00:00.1234567"),
            (123_456_700, 3, "2024-01-01 12:00:00.1234567"),
            (123_456_700, 9, "2024-01-01 12:00:00.123456700"),
        ] {
            let dt = NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_nano_opt(12, 0, 0, nanos)
                .unwrap();
            assert_eq!(format_timestamp_non_legacy(dt, precision), expected);
        }
    }

    #[test]
    fn test_timestamp_partition_paths_match_java() {
        for (nanos, precision, legacy, expected) in [
            (0, 0, false, "ts=2024-01-01 12%3A34%3A00/"),
            (0, 3, false, "ts=2024-01-01 12%3A34%3A00.000/"),
            (120_000_000, 0, false, "ts=2024-01-01 12%3A34%3A00.12/"),
            (100_000_000, 3, true, "ts=2024-01-01T12%3A34%3A00.100/"),
            (123_400_000, 6, true, "ts=2024-01-01T12%3A34%3A00.123400/"),
            (
                123_456_700,
                9,
                true,
                "ts=2024-01-01T12%3A34%3A00.123456700/",
            ),
        ] {
            let dt = NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_nano_opt(12, 34, 0, nanos)
                .unwrap();
            let fields = vec![make_field(
                "ts",
                DataType::Timestamp(TimestampType::new(precision).unwrap()),
            )];
            let computer = PartitionComputer::new(
                &["ts".to_string()],
                &fields,
                TEST_DEFAULT_PARTITION_NAME,
                legacy,
            )
            .unwrap();
            let mut builder = crate::spec::BinaryRowBuilder::new(1);
            if precision <= 3 {
                builder.write_timestamp_compact(0, dt.and_utc().timestamp_millis());
            } else {
                builder.write_timestamp_non_compact(
                    0,
                    dt.and_utc().timestamp_millis(),
                    (nanos % 1_000_000) as i32,
                );
            }
            let row = builder.build();
            assert_eq!(computer.generate_partition_path(&row).unwrap(), expected);
        }
    }

    #[test]
    fn test_timestamp_partition_legacy() {
        // 2024-01-01 12:34:00 UTC = epoch millis 1704110040000
        let millis = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(12, 34, 0)
            .unwrap()
            .and_utc()
            .timestamp_millis();

        assert_single_partition(
            "ts",
            DataType::Timestamp(TimestampType::new(3).unwrap()),
            |b| b.write_timestamp_compact(0, millis),
            "ts=2024-01-01T12%3A34/",
            true,
        );
    }

    #[test]
    fn test_timestamp_partition_non_legacy() {
        let millis = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_nano_opt(12, 34, 56, 123_000_000)
            .unwrap()
            .and_utc()
            .timestamp_millis();

        assert_single_partition(
            "ts",
            DataType::Timestamp(TimestampType::new(3).unwrap()),
            |b| b.write_timestamp_compact(0, millis),
            "ts=2024-01-01 12%3A34%3A56.123/",
            false,
        );
    }

    // ======================== Error path tests ========================

    #[test]
    fn test_arity_mismatch() {
        let fields = vec![make_field("dt", DataType::Int(IntType::new()))];
        let keys = vec!["dt".to_string(), "hr".to_string()];

        let mut builder = TestRowBuilder::new(1);
        builder.write_int(0, 1);
        let row = builder.build();

        let result = PartitionComputer::new(&keys, &fields, TEST_DEFAULT_PARTITION_NAME, true);
        // Construction succeeds (field resolution fails for hr), or path generation fails due to arity mismatch
        match result {
            Err(_) => {} // field resolution failed — expected
            Ok(computer) => assert!(computer.generate_partition_path(&row).is_err()),
        }
    }

    #[test]
    fn test_missing_partition_field() {
        let fields = vec![make_field("other", DataType::Int(IntType::new()))];
        let keys = vec!["dt".to_string()];

        let result = PartitionComputer::new(&keys, &fields, TEST_DEFAULT_PARTITION_NAME, true);
        assert!(result.is_err());
    }

    #[test]
    fn test_unsupported_types() {
        // Legacy binary uses byte[].toString() in Java, which contains an
        // allocation-specific identity hash and cannot name a stable path.
        assert_single_partition_err(
            "data",
            DataType::Binary(BinaryType::new(10).unwrap()),
            |b| b.write_int(0, 0),
            true,
        );
        // Array
        assert_single_partition_err(
            "arr",
            DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
            |b| b.write_int(0, 0),
            true,
        );
        // Float
        assert_single_partition_err(
            "f",
            DataType::Float(FloatType::new()),
            |b| b.write_int(0, 0),
            true,
        );
        // Double
        assert_single_partition_err(
            "d",
            DataType::Double(DoubleType::new()),
            |b| b.write_int(0, 0),
            true,
        );
    }

    #[test]
    fn test_binary_partition_uses_utf8_cast_and_path_escaping() {
        assert_single_partition(
            "bin",
            DataType::Binary(BinaryType::new(16).unwrap()),
            |b| b.write_string(0, "a/b=c"),
            "bin=a%2Fb%3Dc/",
            false,
        );
        assert_single_partition(
            "bin",
            DataType::VarBinary(VarBinaryType::new(16).unwrap()),
            |b| b.write_string(0, "汉字"),
            "bin=汉字/",
            false,
        );
        assert_single_partition(
            "bin",
            DataType::VarBinary(VarBinaryType::new(16).unwrap()),
            |b| b.write_string(0, "  "),
            "bin=__DEFAULT_PARTITION__/",
            false,
        );
    }

    #[test]
    fn test_binary_partition_matches_java_whitespace() {
        // JDK 8 Character.isWhitespace includes U+001C and U+180E, but not
        // U+00A0 or U+2007. Rust str::trim differs for controls and NBSP.
        for (value, expected) in [
            (b"\x1c".as_slice(), "bin=__DEFAULT_PARTITION__/"),
            ("\u{180E}".as_bytes(), "bin=__DEFAULT_PARTITION__/"),
            ("\u{00A0}".as_bytes(), "bin=\u{00A0}/"),
            ("\u{2007}".as_bytes(), "bin=\u{2007}/"),
        ] {
            assert_single_partition(
                "bin",
                DataType::VarBinary(VarBinaryType::new(16).unwrap()),
                |b| b.write_bytes(0, value),
                expected,
                false,
            );
        }
    }

    #[test]
    fn test_binary_partition_matches_java_malformed_utf8() {
        // Expected replacements were checked against JDK 8 new String(bytes, UTF_8).
        for (value, expected) in [
            (b"\xED\xA0\x80".as_slice(), "bin=\u{FFFD}/"),
            (b"\xED\xA0".as_slice(), "bin=\u{FFFD}/"),
            (b"\xED\xA0A".as_slice(), "bin=\u{FFFD}A/"),
            (b"\xED\xA0\xFF".as_slice(), "bin=\u{FFFD}\u{FFFD}/"),
            (b"\xE0\x80\x80".as_slice(), "bin=\u{FFFD}\u{FFFD}\u{FFFD}/"),
            (b"\xE2\x82".as_slice(), "bin=\u{FFFD}/"),
            (b"\xE2(\xA1".as_slice(), "bin=\u{FFFD}(\u{FFFD}/"),
        ] {
            assert_single_partition(
                "bin",
                DataType::VarBinary(VarBinaryType::new(16).unwrap()),
                |b| b.write_bytes(0, value),
                expected,
                false,
            );
        }
    }

    #[test]
    fn test_empty_row_with_partition_keys() {
        let fields = vec![make_field("dt", DataType::Int(IntType::new()))];
        let keys = vec!["dt".to_string()];
        let computer =
            PartitionComputer::new(&keys, &fields, TEST_DEFAULT_PARTITION_NAME, true).unwrap();
        let row = BinaryRow::new(1); // empty backing data

        let result = computer.generate_partition_path(&row);
        assert!(result.is_err());
    }

    // ======================== TIME formatting tests ========================

    #[test]
    fn test_format_time() {
        // precision=3, 12:34:56 = 45_296_000 ms
        assert_eq!(format_time(45_296_000, 3), "12:34:56");
        // precision=3, 12:34:56.123 = 45_296_123 ms
        assert_eq!(format_time(45_296_123, 3), "12:34:56.123");
        // precision=3, 00:00:00.100 = 100 ms → trailing zero stripped to .1
        assert_eq!(format_time(100, 3), "00:00:00.1");
        // precision=0, ms fraction ignored
        assert_eq!(format_time(45_296_123, 0), "12:34:56");
        // precision=1, only first digit "1"
        assert_eq!(format_time(45_296_100, 1), "12:34:56.1");
        // precision=2, ms=120 → "12" (2 digits, no trailing zero issue)
        assert_eq!(format_time(45_296_120, 2), "12:34:56.12");
        // precision=0, 00:00:00
        assert_eq!(format_time(0, 0), "00:00:00");
    }

    #[test]
    fn test_time_partition_legacy() {
        assert_single_partition(
            "t",
            DataType::Time(TimeType::new(3).unwrap()),
            |b| b.write_int(0, 45_296_123), // 12:34:56.123
            "t=45296123/",
            true,
        );
    }

    #[test]
    fn test_time_partition_non_legacy() {
        assert_single_partition(
            "t",
            DataType::Time(TimeType::new(3).unwrap()),
            |b| b.write_int(0, 45_296_123), // 12:34:56.123
            "t=12%3A34%3A56.123/",
            false,
        );
    }

    // ======================== Corrupted row tests ========================

    #[test]
    fn test_truncated_row_returns_error() {
        let fields = vec![make_field("dt", DataType::Int(IntType::new()))];
        let keys = vec!["dt".to_string()];
        let computer =
            PartitionComputer::new(&keys, &fields, TEST_DEFAULT_PARTITION_NAME, true).unwrap();

        // Create a BinaryRow with arity=1 but truncated backing data (too short).
        let row = BinaryRow::from_bytes(1, vec![0u8; 4]); // needs >= 16 bytes

        let result = computer.generate_partition_path(&row);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("too short"), "Expected 'too short' in: {msg}");
    }
}
