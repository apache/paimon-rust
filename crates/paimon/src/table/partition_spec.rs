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

//! Java TypeUtils conversion for textual overwrite and truncate partition specs.

use crate::spec::{DataType, Datum};
use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{Field, Schema};
use std::sync::Arc;

/// Convert a Java partition spec, retaining DECIMAL precision overflow as NULL.
pub fn parse_partition_spec_value(
    text: &str,
    data_type: &DataType,
) -> crate::Result<Option<Datum>> {
    let invalid = || crate::Error::DataInvalid {
        message: format!("Invalid partition value '{text}' for {data_type:?}"),
        source: None,
    };
    match data_type {
        // TypeUtils uses valueOf, not the decimal-truncating SQL cast rules.
        DataType::TinyInt(_) | DataType::SmallInt(_) | DataType::Int(_) | DataType::BigInt(_) => {
            let text = if text.is_ascii() {
                std::borrow::Cow::Borrowed(text)
            } else {
                // Integer.valueOf scans Character.digit, unlike SQL's
                // BinaryStringUtils integer cast with its fractional grammar.
                std::borrow::Cow::Owned(
                    text.chars()
                        .map(|ch| {
                            crate::common::number::java_decimal_digit(ch)
                                .map_or(ch, |digit| char::from(b'0' + digit))
                        })
                        .collect::<String>(),
                )
            };
            return match data_type {
                DataType::TinyInt(_) => text.parse().map(Datum::TinyInt),
                DataType::SmallInt(_) => text.parse().map(Datum::SmallInt),
                DataType::Int(_) => text.parse().map(Datum::Int),
                DataType::BigInt(_) => text.parse().map(Datum::Long),
                _ => unreachable!(),
            }
            .map(Some)
            .map_err(|_| invalid());
        }
        DataType::Float(_) => {
            use crate::common::number::{format_java_float, parse_java_float};
            let double = parse_java_float(text, false)?;
            let single = parse_java_float(text, true)? as f32;
            return if double == f64::from(single)
                || format_java_float(single) == format_java_float(double)
            {
                Ok(Some(Datum::Float(single)))
            } else {
                Err(invalid())
            };
        }
        DataType::Double(_) => {
            return crate::common::number::parse_java_float(text, false)
                .map(Datum::Double)
                .map(Some)
        }
        DataType::Time(_) => return parse_time_spec(text).map(Datum::Time).map(Some),
        DataType::Char(ty) if text.chars().count() > ty.length() => return Err(invalid()),
        DataType::VarChar(ty) if text.chars().count() > ty.length() as usize => {
            return Err(invalid())
        }
        DataType::Char(_) | DataType::VarChar(_) => return Ok(Some(Datum::String(text.into()))),
        DataType::Binary(_) => return Ok(Some(Datum::Bytes(text.as_bytes().to_vec()))),
        DataType::VarBinary(ty) => {
            return if text.len() <= ty.length() as usize {
                Ok(Some(Datum::Bytes(text.as_bytes().to_vec())))
            } else {
                Err(invalid())
            };
        }
        DataType::Boolean(_)
        | DataType::Date(_)
        | DataType::Timestamp(_)
        | DataType::LocalZonedTimestamp(_)
        | DataType::Decimal(_) => {}
        _ => return Err(invalid()),
    }
    if matches!(data_type, DataType::Timestamp(ty) if ty.precision() == 0) {
        let digits = text.strip_prefix('-').unwrap_or(text);
        if !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit()) {
            // Java converts numeric seconds with long multiplication. Overflow
            // can leave subsecond milliseconds, which a partition Datum can
            // retain even though an Arrow second-precision array cannot.
            return text
                .parse::<i64>()
                .map(|seconds| {
                    Some(Datum::Timestamp {
                        millis: seconds.wrapping_mul(1_000),
                        nanos: 0,
                    })
                })
                .map_err(|_| invalid());
        }
    }
    let input: ArrayRef = Arc::new(StringArray::from(vec![text]));
    let array = crate::arrow::cast_array_for_schema_evolution(
        &input,
        &DataType::VarChar(crate::spec::VarCharType::string_type()),
        data_type,
    )?;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "partition",
            array.data_type().clone(),
            true,
        )])),
        vec![array],
    )
    .map_err(|_| invalid())?;
    crate::spec::extract_datum_from_arrow(&batch, 0, 0, data_type)
}

fn parse_time_spec(text: &str) -> crate::Result<i32> {
    let invalid = || crate::Error::DataInvalid {
        message: format!("Invalid TIME partition value '{text}'"),
        source: None,
    };
    // BinaryStringUtils.toTime accepts the legacy millis representation.
    let digits = text.strip_prefix('-').unwrap_or(text);
    if !digits.is_empty() && digits.bytes().all(|c| c.is_ascii_digit()) {
        return text.parse().map_err(|_| invalid());
    }
    // DateTimeUtils.parseTime accepts partial clocks and adds the signed zone
    // offset, without restricting clock components to a single day's range.
    let component = |part: &str| -> crate::Result<i32> {
        let part = part.trim_matches(|c| c <= ' ');
        if part.is_empty() || !part.bytes().all(|c| c.is_ascii_digit()) {
            return Err(invalid());
        }
        part.parse().map_err(|_| invalid())
    };
    let zone_at = text.find('-').or_else(|| text.find('+'));
    let (clock, offset) = match zone_at {
        Some(at) => {
            let sign: i32 = if text.as_bytes()[at] == b'-' { -1 } else { 1 };
            let (hours, minutes) = text[at + 1..]
                .split_once(':')
                .unwrap_or((&text[at + 1..], "0"));
            let millis = component(hours)?
                .wrapping_mul(3_600_000)
                .wrapping_add(component(minutes)?.wrapping_mul(60_000));
            (&text[..at], sign.wrapping_mul(millis))
        }
        None => (text, 0),
    };
    let parts = clock.split(':').collect::<Vec<_>>();
    if parts.len() > 3 || parts.len() < 3 && zone_at.is_some_and(|at| text[at + 1..].contains(':'))
    {
        // Java searches for the clock's colons in the complete string. With
        // missing seconds a colon in the offset is not a valid second colon.
        return Err(invalid());
    }
    let hours = component(parts[0])?;
    let minutes = parts.get(1).map(|v| component(v)).transpose()?.unwrap_or(0);
    let (seconds, fraction) = match parts.get(2) {
        Some(value) => {
            let (seconds, fraction) = value.split_once('.').unwrap_or((value, ""));
            (component(seconds)?, fraction.trim_matches(|c| c <= ' '))
        }
        None => (0, ""),
    };
    // Java's fraction parser counts UTF-16 code units, including the fourth
    // unit used for rounding. Supplementary characters occupy two positions.
    let mut millis = 0;
    let mut chars = fraction.encode_utf16();
    for multiplier in [100, 10, 1] {
        let Some(ch) = chars.next() else { break };
        let digit = ch
            .checked_sub(u16::from(b'0'))
            .filter(|digit| *digit < 10)
            .unwrap_or(0);
        millis += i32::from(digit) * multiplier;
    }
    if chars.next().is_some_and(|ch| ch >= u16::from(b'5')) {
        millis += 1;
    }
    Ok(hours
        .wrapping_mul(3_600_000)
        .wrapping_add(minutes.wrapping_mul(60_000))
        .wrapping_add(seconds.wrapping_mul(1000))
        .wrapping_add(millis)
        .wrapping_add(offset))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{
        BooleanType, DateType, DecimalType, FloatType, IntType, TimeType, TimestampType,
    };

    #[test]
    fn partition_specs_accept_java_unicode_and_decimal_null() {
        let integer = DataType::Int(IntType::new());
        for (value, expected) in [("１２", 12), ("١٢", 12), ("-１２", -12), ("+١٢", 12)] {
            assert_eq!(
                parse_partition_spec_value(value, &integer).unwrap(),
                Some(Datum::Int(expected))
            );
        }
        let decimal = DataType::Decimal(DecimalType::new(3, 0).unwrap());
        assert_eq!(parse_partition_spec_value("1000", &decimal).unwrap(), None);
        assert_eq!(parse_partition_spec_value("999.5", &decimal).unwrap(), None);
        assert!(parse_partition_spec_value("not a decimal", &decimal).is_err());
        let time = DataType::Time(TimeType::new(3).unwrap());
        for value in ["12:34+02:30", "12+02:30"] {
            assert!(parse_partition_spec_value(value, &time).is_err());
        }
    }

    #[test]
    fn time_partition_fractions_use_java_utf16_character_positions() {
        let time = DataType::Time(TimeType::new(3).unwrap());
        for (text, millis) in [
            ("12:34:56.😀123", 45_296_001),
            ("12:34:56.1😀23", 45_296_100),
            ("12:34:56.12😀3", 45_296_121),
            ("12:34:56.123😀", 45_296_124),
        ] {
            assert_eq!(
                parse_partition_spec_value(text, &time).unwrap(),
                Some(Datum::Time(millis)),
                "{text}"
            );
        }
    }

    #[test]
    fn numeric_second_precision_partition_specs_keep_java_wrapped_milliseconds() {
        let data_type = DataType::Timestamp(TimestampType::new(0).unwrap());
        for (text, millis) in [
            ("18446744073709552", 384),
            ("-18446744073709552", -384),
            ("9223372036854775807", -1000),
            ("1", 1000),
        ] {
            assert_eq!(
                parse_partition_spec_value(text, &data_type).unwrap(),
                Some(Datum::Timestamp { millis, nanos: 0 }),
                "{text}"
            );
        }
        for text in ["9223372036854775808", "+1", "1.5"] {
            assert!(parse_partition_spec_value(text, &data_type).is_err());
        }
    }

    #[test]
    fn partition_specs_use_java_typed_text_conversion() {
        assert_eq!(
            parse_partition_spec_value("TRUE", &DataType::Boolean(BooleanType::new()))
                .unwrap()
                .unwrap(),
            Datum::Bool(true)
        );
        assert_eq!(
            parse_partition_spec_value("1970-01-02", &DataType::Date(DateType::new()))
                .unwrap()
                .unwrap(),
            Datum::Date(1)
        );
        assert_eq!(
            parse_partition_spec_value("12:34:56.789", &DataType::Time(TimeType::new(3).unwrap()))
                .unwrap()
                .unwrap(),
            Datum::Time(45_296_789)
        );
        assert_eq!(
            parse_partition_spec_value(
                "1970-01-01 00:00:00.123456",
                &DataType::Timestamp(TimestampType::new(1).unwrap())
            )
            .unwrap()
            .unwrap(),
            Datum::Timestamp {
                millis: 100,
                nanos: 0
            }
        );
        assert!(matches!(
            parse_partition_spec_value(
                "12.345",
                &DataType::Decimal(DecimalType::new(6, 2).unwrap())
            )
            .unwrap()
            .unwrap(),
            Datum::Decimal { unscaled: 1235, .. }
        ));
        for text in ["42.9", "2147483648", " 42"] {
            assert!(parse_partition_spec_value(text, &DataType::Int(IntType::new())).is_err());
        }
        let float = DataType::Float(FloatType::new());
        assert_eq!(
            parse_partition_spec_value("1.2", &float).unwrap().unwrap(),
            Datum::Float(1.2)
        );
        assert_eq!(
            parse_partition_spec_value("0x1p2", &float)
                .unwrap()
                .unwrap(),
            Datum::Float(4.0)
        );
        assert!(parse_partition_spec_value("1.23456789", &float).is_err());
        let time = DataType::Time(TimeType::new(0).unwrap());
        for (text, millis) in [
            ("45296789", 45_296_789),
            ("-1", -1),
            ("12:34", 45_240_000),
            ("12:34:56.7896+02:30", 54_296_790),
            ("12:34:56.7896-02:30", 36_296_790),
            ("24:00:00", 86_400_000),
        ] {
            assert_eq!(
                parse_partition_spec_value(text, &time).unwrap().unwrap(),
                Datum::Time(millis)
            );
        }
        for text in ["+1", "12:bad", "12:34:56:78", "2147483648:00"] {
            assert!(parse_partition_spec_value(text, &time).is_err());
        }
    }
}
