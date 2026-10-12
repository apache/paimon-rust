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

//! Temporal character casts: Java formatting, local zone and CHAR/VARCHAR length.

use std::sync::Arc;

use arrow_array::{cast::AsArray, types::Int64Type, ArrayRef, StringArray, Time32MillisecondArray};
use arrow_schema::{DataType as ArrowType, TimeUnit};
use chrono::{DateTime, Local};

use crate::common::date_time::{format_epoch_timestamp, format_time};
use crate::spec::DataType;

/// Return None when this is not a temporal-to-character conversion.
pub(super) fn temporal_to_string(
    source: &ArrayRef,
    source_type: &DataType,
    target_type: &DataType,
) -> crate::Result<Option<ArrayRef>> {
    let (length, pad) = match target_type {
        DataType::Char(t) => (t.length(), true),
        DataType::VarChar(t) => (t.length() as usize, false),
        _ => return Ok(None),
    };
    let values = match source_type {
        DataType::Time(t) => {
            let millis = arrow_cast::cast(source, &ArrowType::Time32(TimeUnit::Millisecond))
                .map_err(|error| invalid(error.to_string()))?;
            let millis = millis
                .as_any()
                .downcast_ref::<Time32MillisecondArray>()
                .ok_or_else(|| invalid("Expected TIME array"))?;
            millis
                .iter()
                .map(|value| value.map(|value| format_time(value, t.precision())))
                .collect::<Vec<_>>()
        }
        DataType::Timestamp(_) | DataType::LocalZonedTimestamp(_) => {
            let (precision, local) = match source_type {
                DataType::Timestamp(t) => (t.precision(), false),
                DataType::LocalZonedTimestamp(t) => (t.precision(), true),
                _ => unreachable!(),
            };
            let ArrowType::Timestamp(unit, _) = source.data_type() else {
                return Err(invalid("Expected TIMESTAMP array"));
            };
            let raw = arrow_cast::cast(source, &ArrowType::Int64)
                .map_err(|error| invalid(error.to_string()))?;
            raw.as_primitive::<Int64Type>()
                .iter()
                .map(|value| {
                    value
                        .map(|value| {
                            let millis = match unit {
                                TimeUnit::Second => value.wrapping_mul(1000),
                                TimeUnit::Millisecond => value,
                                TimeUnit::Microsecond => value.div_euclid(1000),
                                TimeUnit::Nanosecond => value.div_euclid(1_000_000),
                            };
                            let seconds = millis.div_euclid(1000);
                            let sub_milli = match unit {
                                TimeUnit::Microsecond => value.rem_euclid(1000) * 1000,
                                TimeUnit::Nanosecond => value.rem_euclid(1_000_000),
                                _ => 0,
                            };
                            let nanos = (millis.rem_euclid(1000) * 1_000_000 + sub_milli) as u32;
                            let seconds = if local {
                                let dt = DateTime::from_timestamp(seconds, nanos).ok_or_else(|| {
                                    invalid(format!("Timestamp {value} is out of range for local string conversion"))
                                })?;
                                seconds + i64::from(dt.with_timezone(&Local).offset().local_minus_utc())
                            } else { seconds };
                            Ok(format_epoch_timestamp(seconds, nanos, precision))
                        })
                        .transpose()
                })
                .collect::<crate::Result<Vec<_>>>()?
        }
        _ => return Ok(None),
    };
    // The temporal representation is ASCII. Java toCharacterString counts
    // characters, truncates both string kinds, and pads only CHAR.
    let strings = values.into_iter().map(|value| {
        value.map(|mut value| {
            value.truncate(length);
            if pad && value.len() < length {
                value.extend(std::iter::repeat_n(' ', length - value.len()));
            }
            value
        })
    });
    Ok(Some(Arc::new(StringArray::from_iter(strings))))
}

fn invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::nested_evolution::evolve_column;
    use crate::spec::{CharType, TimeType, TimestampType, VarCharType};
    use arrow_array::{Int64Array, TimestampNanosecondArray};

    fn strings(source: ArrayRef, source_type: DataType, target: DataType) -> StringArray {
        evolve_column(&source, &source_type, &target)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone()
    }

    #[test]
    fn time_string_matches_java_fraction_loop_and_negative_normalization() {
        let source: ArrayRef = Arc::new(Time32MillisecondArray::from(vec![
            None,
            Some(0),
            Some(100),
            Some(101),
            Some(-1),
            None,
            Some(0),
        ]));
        for (precision, expected) in [
            (0, ["00:00:00", "00:00:00", "00:00:00", "23:59:59"]),
            (1, ["00:00:00.0", "00:00:00.1", "00:00:00.1", "23:59:59.9"]),
            (
                2,
                ["00:00:00.0", "00:00:00.1", "00:00:00.10", "23:59:59.99"],
            ),
            (
                3,
                ["00:00:00.0", "00:00:00.1", "00:00:00.101", "23:59:59.999"],
            ),
            (
                9,
                ["00:00:00.0", "00:00:00.1", "00:00:00.101", "23:59:59.999"],
            ),
        ] {
            let actual = strings(
                source.slice(1, 5),
                DataType::Time(TimeType::new(precision).unwrap()),
                DataType::VarChar(VarCharType::string_type()),
            );
            assert_eq!(
                actual,
                StringArray::from_iter(expected.into_iter().map(Some).chain([None]))
            );
        }
    }

    #[test]
    fn timestamp_string_keeps_nanos_and_minimum_fraction_width() {
        let source: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
            None,
            Some(0),
            Some(123_456_789),
            Some(-1),
            None,
            Some(0),
        ]));
        for (precision, whole) in [
            (0, "1970-01-01 00:00:00"),
            (1, "1970-01-01 00:00:00.0"),
            (3, "1970-01-01 00:00:00.000"),
            (6, "1970-01-01 00:00:00.000000"),
            (9, "1970-01-01 00:00:00.000000000"),
        ] {
            let actual = strings(
                source.slice(1, 4),
                DataType::Timestamp(TimestampType::new(precision).unwrap()),
                DataType::VarChar(VarCharType::string_type()),
            );
            assert_eq!(
                actual,
                StringArray::from(vec![
                    Some(whole),
                    Some("1970-01-01 00:00:00.123456789"),
                    Some("1969-12-31 23:59:59.999999999"),
                    None
                ])
            );
        }
    }

    #[test]
    fn timestamp_string_handles_all_physical_units_and_character_lengths() {
        for (unit, ticks) in [
            (TimeUnit::Second, 1),
            (TimeUnit::Millisecond, 1_000),
            (TimeUnit::Microsecond, 1_000_000),
            (TimeUnit::Nanosecond, 1_000_000_000),
        ] {
            let raw: ArrayRef = Arc::new(Int64Array::from(vec![Some(ticks), Some(-ticks), None]));
            let source = arrow_cast::cast(&raw, &ArrowType::Timestamp(unit, None)).unwrap();
            for (target, first, second) in [
                (
                    DataType::VarChar(VarCharType::string_type()),
                    "1970-01-01 00:00:01.000",
                    "1969-12-31 23:59:59.000",
                ),
                (
                    DataType::VarChar(VarCharType::new(12).unwrap()),
                    "1970-01-01 0",
                    "1969-12-31 2",
                ),
                (
                    DataType::Char(CharType::new(25).unwrap()),
                    "1970-01-01 00:00:01.000  ",
                    "1969-12-31 23:59:59.000  ",
                ),
                (DataType::Char(CharType::new(4).unwrap()), "1970", "1969"),
            ] {
                let result = strings(
                    source.clone(),
                    DataType::Timestamp(TimestampType::new(3).unwrap()),
                    target,
                );
                assert_eq!(
                    result,
                    StringArray::from(vec![Some(first), Some(second), None])
                );
            }
        }
    }

    #[test]
    fn timestamp_string_uses_java_four_digits_outside_datetime_year_range() {
        use arrow_array::TimestampMillisecondArray;
        let source: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![
            Some(253_402_300_800_000),
            Some(-62_167_219_200_000),
            Some(-62_198_755_200_000),
            Some(i64::MAX),
            Some(i64::MIN),
            None,
        ]));
        let actual = strings(
            source,
            DataType::Timestamp(TimestampType::new(3).unwrap()),
            DataType::VarChar(VarCharType::string_type()),
        );
        assert_eq!(
            actual,
            StringArray::from(vec![
                Some("0000-01-01 00:00:00.000"),
                Some("0000-01-01 00:00:00.000"),
                Some("000/-01-01 00:00:00.000"),
                Some("/+-.-04-06 07:12:55.807"),
                Some("5471-09-26 16:47:04.192"),
                None
            ])
        );
    }

    #[test]
    fn timestamp_seconds_wrapping_keeps_wrapped_millisecond_fraction() {
        use arrow_array::TimestampSecondArray;
        let source: ArrayRef = Arc::new(TimestampSecondArray::from(vec![
            Some(9_223_372_036_854_776),
            None,
        ]));
        let actual = strings(
            source,
            DataType::Timestamp(TimestampType::new(3).unwrap()),
            DataType::VarChar(VarCharType::string_type()),
        );
        assert_eq!(
            actual,
            StringArray::from(vec![Some("5471-09-26 16:47:04.384"), None])
        );
    }
}
