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
//   Unless required by applicable law or agreed to in writing,
//   software distributed under the License is distributed on an
//   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//   KIND, either express or implied.  See the License for the
//   specific language governing permissions and limitations
//   under the License.

//! Gregorian field extraction using Java Paimon's DATE/TIMESTAMP representation.

use crate::common::date_time::epoch_day_to_date;
use crate::spec::DatePart;
use arrow_array::{cast::AsArray, ArrayRef, Int32Array};
use arrow_schema::{DataType, TimeUnit};
use std::sync::Arc;

pub(crate) fn evaluate_date_extract(source: &ArrayRef, part: DatePart) -> crate::Result<ArrayRef> {
    let values: Vec<Option<i32>> = match source.data_type() {
        DataType::Date32 => source
            .as_primitive::<arrow_array::types::Date32Type>()
            .iter()
            .map(|day| day.map(|day| extract(day, 0, part)))
            .collect(),
        DataType::Timestamp(unit, None) => {
            let raw = arrow_cast::cast(source, &DataType::Int64).map_err(|e| {
                crate::Error::DataInvalid {
                    message: format!("cannot read calendar transform input: {e}"),
                    source: None,
                }
            })?;
            raw.as_primitive::<arrow_array::types::Int64Type>()
                .iter()
                .map(|ticks| {
                    ticks.map(|ticks| {
                        let millis = match unit {
                            TimeUnit::Second => ticks.wrapping_mul(1000),
                            TimeUnit::Millisecond => ticks,
                            TimeUnit::Microsecond => ticks.div_euclid(1000),
                            TimeUnit::Nanosecond => ticks.div_euclid(1_000_000),
                        };
                        // Timestamp.toLocalDateTime narrows the epoch day to an int.
                        // Casting after floor division is also exact when the day wraps.
                        extract(
                            millis.div_euclid(86_400_000) as i32,
                            millis.rem_euclid(86_400_000),
                            part,
                        )
                    })
                })
                .collect()
        }
        other => {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "calendar transforms require DATE or TIMESTAMP without time zone, got {other}"
                ),
                source: None,
            })
        }
    };
    Ok(Arc::new(Int32Array::from(values)))
}

fn extract(day: i32, millis: i64, part: DatePart) -> i32 {
    let day = i64::from(day);
    let (year, month, date) = epoch_day_to_date(day);
    let weekday = (day + 3).rem_euclid(7) as i32;
    match part {
        DatePart::Year => year,
        DatePart::Month => month as i32,
        DatePart::Day => date as i32,
        DatePart::Hour => (millis / 3_600_000) as i32,
        DatePart::Minute => (millis / 60_000 % 60) as i32,
        DatePart::Second => (millis / 1000 % 60) as i32,
        DatePart::Quarter => ((month - 1) / 3 + 1) as i32,
        DatePart::DayOfYear => (day - january_first(year) + 1) as i32,
        DatePart::Weekday => weekday,
        DatePart::IsoDayOfWeek => weekday + 1,
        DatePart::DayOfWeek => (weekday + 1) % 7 + 1,
        DatePart::Week | DatePart::YearOfWeek => {
            // The Thursday of an ISO week always belongs to its week-based year.
            let thursday = day + 3 - i64::from(weekday);
            let (week_year, _, _) = epoch_day_to_date(thursday);
            if part == DatePart::YearOfWeek {
                week_year
            } else {
                ((thursday - january_first(week_year)) / 7 + 1) as i32
            }
        }
    }
}

fn january_first(year: i32) -> i64 {
    // March-based Gregorian eras: floor division is essential before year zero.
    let year = i64::from(year) - 1;
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + 306 - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        Date32Array, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    };

    #[test]
    fn calendar_parts_match_actual_java_for_all_units_and_nulls() {
        let names = [
            "YEAR",
            "MONTH",
            "DAY",
            "HOUR",
            "MINUTE",
            "SECOND",
            "QUARTER",
            "DAY_OF_YEAR",
            "WEEKDAY",
            "ISO_DAY_OF_WEEK",
            "DAY_OF_WEEK",
            "WEEK",
            "YEAR_OF_WEEK",
        ];
        // Actual DateExtractTransform outputs, including Timestamp's int epoch-day
        // narrowing. Generated with Java DATE / timestamp(s,ms,us,ns) inputs.
        let cases: Vec<serde_json::Value> = serde_json::from_str(include_str!(
            "../../tests/fixtures/calendar_extract_java.json"
        ))
        .unwrap();
        for case in cases {
            let ticks = case["value"].as_i64().unwrap();
            let source: ArrayRef = match case["unit"].as_str().unwrap() {
                "date" => Arc::new(Date32Array::from(vec![Some(ticks as i32), None])),
                "s" => Arc::new(TimestampSecondArray::from(vec![Some(ticks), None])),
                "ms" => Arc::new(TimestampMillisecondArray::from(vec![Some(ticks), None])),
                "us" => Arc::new(TimestampMicrosecondArray::from(vec![Some(ticks), None])),
                "ns" => Arc::new(TimestampNanosecondArray::from(vec![Some(ticks), None])),
                _ => unreachable!(),
            };
            for (name, expected) in names.iter().zip(case["parts"].as_array().unwrap()) {
                let result =
                    evaluate_date_extract(&source, DatePart::from_name(name).unwrap()).unwrap();
                let values = result.as_primitive::<arrow_array::types::Int32Type>();
                assert_eq!(
                    values.iter().collect::<Vec<_>>(),
                    vec![Some(expected.as_i64().unwrap() as i32), None],
                    "{name}: {case}"
                );
            }
        }
    }

    #[test]
    fn date_extraction_rejects_noncalendar_and_local_zoned_arrays() {
        for source in [
            Arc::new(arrow_array::Int32Array::from(vec![1])) as ArrayRef,
            Arc::new(arrow_array::Time32MillisecondArray::from(vec![1])),
            Arc::new(TimestampMillisecondArray::from(vec![1]).with_timezone("UTC")),
        ] {
            assert!(evaluate_date_extract(&source, DatePart::Year).is_err());
        }
    }
}
