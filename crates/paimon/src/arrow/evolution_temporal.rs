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

//! Java local-zone conversions for existing schema-evolution casts.

use crate::common::date_time::{
    epoch_day_to_java_date, java_date_to_epoch_day, local_offset, local_to_utc_seconds,
};
use crate::spec::DataType;
use arrow_array::{
    cast::AsArray,
    types::{Int32Type, Int64Type},
    ArrayRef, Date32Array, Int64Array, Time32MillisecondArray,
};
use arrow_schema::{DataType as ArrowType, TimeUnit};
use jiff::tz::TimeZone;
use std::sync::Arc;

pub(super) fn cast_temporal(
    source: &ArrayRef,
    from: &DataType,
    to: &DataType,
) -> crate::Result<Option<ArrayRef>> {
    if !matches!(
        (from, to),
        (
            DataType::Timestamp(_),
            DataType::LocalZonedTimestamp(_) | DataType::Date(_) | DataType::Time(_)
        ) | (
            DataType::LocalZonedTimestamp(_),
            DataType::Timestamp(_) | DataType::Date(_) | DataType::Time(_)
        ) | (
            DataType::Date(_) | DataType::BigInt(_),
            DataType::LocalZonedTimestamp(_)
        )
    ) {
        return Ok(None);
    }
    let zone = TimeZone::system();
    cast_in_zone(source, from, to, &zone)
}

pub(super) fn cast_in_zone(
    source: &ArrayRef,
    from: &DataType,
    to: &DataType,
    zone: &TimeZone,
) -> crate::Result<Option<ArrayRef>> {
    cast_in_zone_with_storage(source, from, to, zone, false)
}

pub(super) fn cast_in_zone_strict(
    source: &ArrayRef,
    from: &DataType,
    to: &DataType,
    zone: &TimeZone,
) -> crate::Result<Option<ArrayRef>> {
    cast_in_zone_with_storage(source, from, to, zone, true)
}

fn cast_in_zone_with_storage(
    source: &ArrayRef,
    from: &DataType,
    to: &DataType,
    zone: &TimeZone,
    strict: bool,
) -> crate::Result<Option<ArrayRef>> {
    let ltz = matches!(from, DataType::LocalZonedTimestamp(_));
    let timestamp = matches!(
        from,
        DataType::Timestamp(_) | DataType::LocalZonedTimestamp(_)
    );
    let cross = timestamp
        && matches!(
            to,
            DataType::Timestamp(_) | DataType::LocalZonedTimestamp(_)
        )
        && ltz != matches!(to, DataType::LocalZonedTimestamp(_));
    let date = matches!(
        (from, to),
        (DataType::Date(_), DataType::LocalZonedTimestamp(_))
    );
    let numeric = matches!(
        (from, to),
        (DataType::BigInt(_), DataType::LocalZonedTimestamp(_))
    );
    if !cross
        && !date
        && !numeric
        && !(timestamp && matches!(to, DataType::Date(_) | DataType::Time(_)))
    {
        return Ok(None);
    }
    let raw = if date {
        arrow_cast::cast(source, &ArrowType::Int32).map_err(invalid_cast)?
    } else {
        arrow_cast::cast(source, &ArrowType::Int64).map_err(invalid_cast)?
    };
    let values = if date {
        raw.as_primitive::<Int32Type>()
            .iter()
            .map(|v| v.map(i64::from))
            .collect::<Vec<_>>()
    } else {
        raw.as_primitive::<Int64Type>().iter().collect()
    };
    let unit = match source.data_type() {
        ArrowType::Timestamp(unit, _) => *unit,
        _ => TimeUnit::Second,
    };
    let target_arrow = super::paimon_type_to_arrow(to)?;
    let mut converted = Vec::with_capacity(values.len());
    for value in values {
        let Some(value) = value else {
            converted.push(None);
            continue;
        };
        let (mut millis, mut sub_milli) = if date {
            (java_date_to_epoch_day(value as i32)? * 86_400_000, 0)
        } else {
            timestamp_parts(value, &unit)
        };
        // LocalDateTime.ofInstant retains a wide epoch day for DATE/TIME;
        // Timestamp.fromLocalDateTime instead stores signed long milliseconds.
        let local_millis = i128::from(millis)
            + if ltz || numeric {
                i128::from(local_offset(millis.div_euclid(1000), zone)) * 1000
            } else {
                0
            };
        if cross && !ltz {
            // Timestamp.toLocalDateTime narrows the epoch day before resolving the zone.
            millis = i64::from(millis.div_euclid(86_400_000) as i32) * 86_400_000
                + millis.rem_euclid(86_400_000);
        }
        if date || cross && !ltz {
            let seconds = millis.div_euclid(1000);
            let utc = local_to_utc_seconds(seconds, zone)?;
            millis = utc * 1000 + millis.rem_euclid(1000);
        } else if ltz || numeric {
            millis = local_millis as i64;
        }
        let value = match to {
            DataType::Date(_) if ltz => Some(i64::from(epoch_day_to_java_date(
                local_millis.div_euclid(86_400_000) as i64,
            ))),
            DataType::Date(_) => Some(i64::from(millis.div_euclid(86_400_000) as i32)),
            DataType::Time(_) => Some(local_millis.rem_euclid(86_400_000) as i64),
            DataType::Timestamp(_) | DataType::LocalZonedTimestamp(_) => {
                let precision = match to {
                    DataType::Timestamp(t) => t.precision(),
                    DataType::LocalZonedTimestamp(t) => t.precision(),
                    _ => unreachable!(),
                };
                let from_precision = match from {
                    DataType::Timestamp(t) => t.precision(),
                    DataType::LocalZonedTimestamp(t) => t.precision(),
                    _ => 0,
                };
                if from_precision > precision {
                    if precision <= 3 {
                        let quantum = 10_i64.pow(3 - precision);
                        millis = millis.div_euclid(quantum).wrapping_mul(quantum);
                        sub_milli = 0;
                    } else {
                        let quantum = 10_i64.pow(9 - precision);
                        sub_milli = sub_milli / quantum * quantum;
                    }
                }
                let ArrowType::Timestamp(unit, _) = &target_arrow else {
                    unreachable!()
                };
                let ticks = match unit {
                    TimeUnit::Second => i128::from(millis.div_euclid(1000)),
                    TimeUnit::Millisecond => i128::from(millis),
                    TimeUnit::Microsecond => {
                        i128::from(millis) * 1000 + i128::from(sub_milli / 1000)
                    }
                    TimeUnit::Nanosecond => i128::from(millis) * 1_000_000 + i128::from(sub_milli),
                };
                if strict {
                    let divisor = match unit {
                        TimeUnit::Second => 1_000_000_000,
                        TimeUnit::Millisecond => 1_000_000,
                        TimeUnit::Microsecond => 1000,
                        TimeUnit::Nanosecond => 1,
                    };
                    if i128::from(millis) * 1_000_000 + i128::from(sub_milli) != ticks * divisor
                        || i64::try_from(ticks).is_err()
                    {
                        return Err(crate::Error::DataInvalid {
                            message:
                                "Java timestamp result cannot be represented by the Arrow target"
                                    .into(),
                            source: None,
                        });
                    }
                }
                i64::try_from(ticks).ok()
            }
            _ => unreachable!(),
        };
        converted.push(value);
    }
    Ok(Some(match to {
        DataType::Date(_) => Arc::new(Date32Array::from_iter(
            converted.into_iter().map(|v| v.map(|v| v as i32)),
        )) as ArrayRef,
        DataType::Time(_) => Arc::new(Time32MillisecondArray::from_iter(
            converted.into_iter().map(|v| v.map(|v| v as i32)),
        )),
        _ => arrow_cast::cast(
            &(Arc::new(Int64Array::from(converted)) as ArrayRef),
            &target_arrow,
        )
        .map_err(invalid_cast)?,
    }))
}

pub(super) fn timestamp_parts(value: i64, unit: &TimeUnit) -> (i64, i64) {
    match unit {
        TimeUnit::Second => (value.wrapping_mul(1000), 0),
        TimeUnit::Millisecond => (value, 0),
        TimeUnit::Microsecond => (value.div_euclid(1000), value.rem_euclid(1000) * 1000),
        TimeUnit::Nanosecond => (value.div_euclid(1_000_000), value.rem_euclid(1_000_000)),
    }
}

fn invalid_cast(error: arrow_schema::ArrowError) -> crate::Error {
    crate::Error::DataInvalid {
        message: format!("Cannot convert temporal schema-evolution value: {error}"),
        source: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{DateType, LocalZonedTimestampType, TimeType, TimestampType};
    use arrow_array::{Array, TimestampMillisecondArray, TimestampNanosecondArray};

    fn ts(ltz: bool, precision: u32) -> DataType {
        if ltz {
            DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(precision).unwrap())
        } else {
            DataType::Timestamp(TimestampType::new(precision).unwrap())
        }
    }

    fn raw(array: &ArrayRef) -> Vec<Option<i64>> {
        let integer = if matches!(array.data_type(), ArrowType::Date32 | ArrowType::Time32(_)) {
            let array = arrow_cast::cast(array, &ArrowType::Int32).unwrap();
            return array
                .as_primitive::<Int32Type>()
                .iter()
                .map(|v| v.map(i64::from))
                .collect();
        } else {
            arrow_cast::cast(array, &ArrowType::Int64).unwrap()
        };
        integer.as_primitive::<Int64Type>().iter().collect()
    }

    #[test]
    fn temporal_zone_casts_match_java_including_dst_and_long_boundaries() {
        let input = [
            0,
            -1,
            1710037800123,
            1730597400123,
            1710055800123,
            1730615400123,
            i64::MAX,
            i64::MIN,
        ];
        // Golden values from Java CastExecutors with source precision 9 and target 1.
        for (zone, expected) in [
            (
                "America/New_York",
                [
                    [
                        18000000,
                        17999900,
                        1710055800100,
                        1730611800100,
                        1710070200100,
                        1730633400100,
                        -53757322487462200,
                        53757322519624100,
                    ],
                    [
                        -18000000,
                        -18000100,
                        1710019800100,
                        1730583000100,
                        1710041400100,
                        1730597400100,
                        9223372036840375800,
                        9223372036837013800,
                    ],
                    [-1, -1, 19791, 20029, 19792, 20030, -622191233, 622191233],
                    [
                        68400000, 68399999, 77400123, 77400123, 12600123, 5400123, 11575807,
                        42662192,
                    ],
                ],
            ),
            (
                "Asia/Shanghai",
                [
                    [
                        -28800000,
                        -28800100,
                        1710009000100,
                        1730568600100,
                        1710027000100,
                        1730586600100,
                        -53757322534367200,
                        53757322476424100,
                    ],
                    [
                        28800000,
                        28799900,
                        1710066600100,
                        1730626200100,
                        1710084600100,
                        1730644200100,
                        -9223372036825975900,
                        -9223372036825632900,
                    ],
                    [0, 0, 19792, 20030, 19792, 20030, -622191233, 622191234],
                    [
                        28800000, 28799999, 37800123, 34200123, 55800123, 52200123, 54775807,
                        3167192,
                    ],
                ],
            ),
        ] {
            let zone = TimeZone::get(zone).unwrap();
            for (i, (from, to)) in [
                (ts(false, 9), ts(true, 1)),
                (ts(true, 9), ts(false, 1)),
                (ts(true, 9), DataType::Date(DateType::new())),
                (ts(true, 9), DataType::Time(TimeType::new(1).unwrap())),
            ]
            .into_iter()
            .enumerate()
            {
                let source: ArrayRef = Arc::new(TimestampMillisecondArray::from_iter(
                    [None]
                        .into_iter()
                        .chain(input.into_iter().map(Some))
                        .chain([None, None]),
                ));
                let actual = cast_in_zone(&source.slice(1, input.len() + 1), &from, &to, &zone)
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    raw(&actual),
                    expected[i]
                        .into_iter()
                        .map(Some)
                        .chain([None])
                        .collect::<Vec<_>>(),
                    "{zone:?} operation {i}"
                );
            }
        }
    }

    #[test]
    fn timezone_conversion_preserves_submillisecond_values_and_truncates_declared_precision() {
        let zone = TimeZone::get("America/New_York").unwrap();
        let source: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
            Some(1710037800123456789),
            Some(-1),
            None,
        ]));
        let output = cast_in_zone(&source, &ts(false, 9), &ts(true, 6), &zone)
            .unwrap()
            .unwrap();
        assert_eq!(
            raw(&output),
            vec![Some(1710055800123456), Some(17999999999), None]
        );
        let output = cast_in_zone(&source, &ts(false, 9), &ts(true, 1), &zone)
            .unwrap()
            .unwrap();
        assert_eq!(
            raw(&output),
            vec![Some(1710055800100), Some(17999900), None]
        );
    }

    #[test]
    fn date_to_ltz_resolves_midnight_and_skipped_days_like_java() {
        for (name, input, expected) in [
            (
                "Asia/Shanghai",
                [0, -1, 19875, 15338],
                [-28800000, -115200000, 1717171200000, 1325174400000],
            ),
            (
                "America/New_York",
                [0, -1, 19875, 15338],
                [18000000, -68400000, 1717214400000, 1325221200000],
            ),
            // Pacific/Apia skipped all of 2011-12-30; atZone advances to Dec 31.
            (
                "Pacific/Apia",
                [15338, 15339, 0, -1],
                [1325239200000, 1325239200000, 39600000, -46800000],
            ),
        ] {
            let source: ArrayRef = Arc::new(Date32Array::from_iter(
                input.into_iter().map(Some).chain([None]),
            ));
            let result = cast_in_zone(
                &source,
                &DataType::Date(DateType::new()),
                &ts(true, 3),
                &TimeZone::get(name).unwrap(),
            )
            .unwrap()
            .unwrap();
            assert_eq!(
                raw(&result),
                expected
                    .into_iter()
                    .map(Some)
                    .chain([None])
                    .collect::<Vec<_>>(),
                "{name}"
            );
        }
        for invalid in [i32::MIN, i32::MAX] {
            let source: ArrayRef = Arc::new(Date32Array::from(vec![invalid]));
            assert!(cast_in_zone(
                &source,
                &DataType::Date(DateType::new()),
                &ts(true, 3),
                &TimeZone::UTC
            )
            .unwrap_err()
            .to_string()
            .contains("Invalid Java DATE"));
        }
    }

    #[test]
    fn local_wall_time_can_cross_the_timezone_library_timestamp_bounds() {
        for (zone, input, expected) in [
            (
                "Asia/Shanghai",
                [-377705023201000, -377705023200000],
                [-377705052344000, -377705052343000],
            ),
            (
                "America/New_York",
                [253402207199000, 253402207200000],
                [253402225199000, 253402225200000],
            ),
        ] {
            let source: ArrayRef = Arc::new(TimestampMillisecondArray::from_iter(
                input.into_iter().map(Some).chain([None]),
            ));
            let output = cast_in_zone(
                &source,
                &ts(false, 3),
                &ts(true, 3),
                &TimeZone::get(zone).unwrap(),
            )
            .unwrap()
            .unwrap();
            assert_eq!(
                raw(&output),
                expected
                    .into_iter()
                    .map(Some)
                    .chain([None])
                    .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn numeric_ltz_keeps_java_epoch_seconds_and_long_wrapping() {
        let source: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(0),
            Some(-1),
            Some(i64::MAX),
            Some(i64::MIN),
            None,
        ]));
        let result = cast_in_zone(
            &source,
            &DataType::BigInt(crate::spec::BigIntType::new()),
            &ts(true, 3),
            &TimeZone::get("Asia/Shanghai").unwrap(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            raw(&result),
            vec![
                Some(28800000),
                Some(28799000),
                Some(28799000),
                Some(28800000),
                None
            ]
        );
    }

    #[test]
    fn ordinary_timestamp_date_and_time_keep_java_flooring() {
        let source: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
            Some(-1),
            Some(86400000000001),
            None,
        ]));
        let from = ts(false, 9);
        let date = cast_in_zone(
            &source,
            &from,
            &DataType::Date(DateType::new()),
            &TimeZone::UTC,
        )
        .unwrap()
        .unwrap();
        assert_eq!(raw(&date), vec![Some(-1), Some(1), None]);
        let time = cast_in_zone(
            &source,
            &from,
            &DataType::Time(TimeType::new(0).unwrap()),
            &TimeZone::UTC,
        )
        .unwrap()
        .unwrap();
        assert_eq!(raw(&time), vec![Some(86399999), Some(0), None]);
    }
}
