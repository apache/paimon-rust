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

use chrono::{Datelike, Months, NaiveDate, NaiveDateTime, TimeZone};
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Date32Array, Int64Array, Int64Builder,
    NullArray, StringArray, StringBuilder, TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use regex::{Regex, RegexBuilder};
use std::{collections::HashMap, sync::Arc};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Kind {
    Assert,
    Conv,
    DateDiff,
    ConvertTz,
    Like,
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Extra {
    kind: Kind,
    signature: Signature,
}
fn error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}
pub(crate) fn register(ctx: &SessionContext) {
    for kind in [
        Kind::Assert,
        Kind::Conv,
        Kind::DateDiff,
        Kind::ConvertTz,
        Kind::Like,
    ] {
        ctx.register_udf(ScalarUDF::from(Extra {
            kind,
            signature: Signature::user_defined(Volatility::Immutable),
        }));
    }
}
fn result(args: &ScalarFunctionArgs, array: ArrayRef) -> Result<ColumnarValue> {
    if args
        .args
        .iter()
        .all(|a| matches!(a, ColumnarValue::Scalar(_)))
    {
        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            array.as_ref(),
            0,
        )?))
    } else {
        Ok(ColumnarValue::Array(array))
    }
}
impl ScalarUDFImpl for Extra {
    fn name(&self) -> &str {
        match self.kind {
            Kind::Assert => "assert_true",
            Kind::Conv => "conv",
            Kind::DateDiff => "date_diff",
            Kind::ConvertTz => "convert_tz",
            Kind::Like => "__paimon_like",
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        Ok(match (self.kind, types.len()) {
            (Kind::Assert, 1 | 2) => {
                if !matches!(types[0], DataType::Boolean | DataType::Null) {
                    return Err(DataFusionError::Plan(
                        "assert_true requires a boolean condition".into(),
                    ));
                }
                let mut out = vec![DataType::Boolean];
                if types.len() == 2 {
                    out.push(DataType::Utf8);
                }
                out
            }
            (Kind::Conv, 3) => vec![DataType::Utf8, DataType::Int64, DataType::Int64],
            (Kind::DateDiff, 2) => vec![DataType::Date32, DataType::Date32],
            (Kind::DateDiff, 3) => vec![
                DataType::Utf8,
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ],
            (Kind::ConvertTz, 3) => {
                if matches!(types[0], DataType::Timestamp(_, Some(_))) {
                    return Err(DataFusionError::Plan(
                        "convert_tz expects a timezone-less datetime".into(),
                    ));
                }
                vec![DataType::Utf8; 3]
            }
            (Kind::Like, 4) => vec![
                DataType::Utf8,
                DataType::Utf8,
                DataType::Utf8,
                DataType::Boolean,
            ],
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "Invalid {} argument count",
                    self.name()
                )))
            }
        })
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(match self.kind {
            Kind::Assert => DataType::Null,
            Kind::Conv => DataType::Utf8,
            Kind::DateDiff => DataType::Int64,
            Kind::ConvertTz => DataType::Timestamp(TimeUnit::Microsecond, None),
            Kind::Like => DataType::Boolean,
        })
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let len = arrays[0].len();
        let out: ArrayRef = match self.kind {
            Kind::Assert => {
                let condition = arrays[0].as_any().downcast_ref::<BooleanArray>().unwrap();
                for row in 0..len {
                    if condition.is_null(row) || !condition.value(row) {
                        let message = arrays
                            .get(1)
                            .filter(|a| !a.is_null(row))
                            .map(|a| a.as_any().downcast_ref::<StringArray>().unwrap().value(row))
                            .unwrap_or("assertion failed");
                        return Err(error(message));
                    }
                }
                Arc::new(NullArray::new(len))
            }
            Kind::Conv => {
                let input = arrays[0].as_any().downcast_ref::<StringArray>().unwrap();
                let from = arrays[1].as_any().downcast_ref::<Int64Array>().unwrap();
                let to = arrays[2].as_any().downcast_ref::<Int64Array>().unwrap();
                let mut output = StringBuilder::new();
                for row in 0..len {
                    if arrays.iter().any(|a| a.is_null(row)) {
                        output.append_null();
                        continue;
                    }
                    match conv(input.value(row), from.value(row), to.value(row)) {
                        Some(v) => output.append_value(v),
                        None => output.append_null(),
                    }
                }
                Arc::new(output.finish())
            }
            Kind::DateDiff => {
                let mut output = Int64Builder::new();
                if arrays.len() == 2 {
                    let end = arrays[0].as_any().downcast_ref::<Date32Array>().unwrap();
                    let start = arrays[1].as_any().downcast_ref::<Date32Array>().unwrap();
                    for row in 0..len {
                        if end.is_null(row) || start.is_null(row) {
                            output.append_null();
                        } else {
                            output.append_value(
                                i64::from(end.value(row)) - i64::from(start.value(row)),
                            );
                        }
                    }
                } else {
                    let unit = arrays[0].as_any().downcast_ref::<StringArray>().unwrap();
                    let start = arrays[1]
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    let end = arrays[2]
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    for row in 0..len {
                        if arrays.iter().any(|a| a.is_null(row)) {
                            output.append_null();
                            continue;
                        }
                        output.append_value(date_diff(
                            unit.value(row),
                            start.value(row),
                            end.value(row),
                        )?);
                    }
                }
                Arc::new(output.finish())
            }
            Kind::ConvertTz => {
                let input = arrays[0].as_any().downcast_ref::<StringArray>().unwrap();
                let from = arrays[1].as_any().downcast_ref::<StringArray>().unwrap();
                let to = arrays[2].as_any().downcast_ref::<StringArray>().unwrap();
                let values = (0..len)
                    .map(|row| {
                        if arrays.iter().any(|a| a.is_null(row)) {
                            return None;
                        }
                        convert_tz(input.value(row), from.value(row), to.value(row))
                    })
                    .collect::<Vec<_>>();
                Arc::new(TimestampMicrosecondArray::from(values))
            }
            Kind::Like => {
                let input = arrays[0].as_any().downcast_ref::<StringArray>().unwrap();
                let pattern = arrays[1].as_any().downcast_ref::<StringArray>().unwrap();
                let escape = arrays[2].as_any().downcast_ref::<StringArray>().unwrap();
                let insensitive = arrays[3].as_any().downcast_ref::<BooleanArray>().unwrap();
                let mut cache: HashMap<(String, String, bool), Regex> = HashMap::new();
                let mut output = BooleanBuilder::new();
                for row in 0..len {
                    if arrays.iter().any(|a| a.is_null(row)) {
                        output.append_null();
                        continue;
                    }
                    let key = (
                        pattern.value(row).to_string(),
                        escape.value(row).to_string(),
                        insensitive.value(row),
                    );
                    if !cache.contains_key(&key) {
                        cache.insert(key.clone(), like_regex(&key.0, &key.1, key.2)?);
                    }
                    output.append_value(cache[&key].is_match(input.value(row)));
                }
                Arc::new(output.finish())
            }
        };
        result(&args, out)
    }
}
fn conv(input: &str, from: i64, to: i64) -> Option<String> {
    let from = from.checked_abs()?;
    let base = to.checked_abs()?;
    if !(2..=36).contains(&from) || !(2..=36).contains(&base) {
        return None;
    }
    let input = input.trim_start();
    let negative = input.starts_with('-');
    let digits = input.strip_prefix(['-', '+']).unwrap_or(input);
    let mut bits = 0u64;
    for c in digits.chars() {
        let Some(d) = c.to_digit(from as u32) else {
            break;
        };
        bits = bits.saturating_mul(from as u64).saturating_add(d as u64);
    }
    if negative {
        bits = bits.wrapping_neg();
    }
    let negative = to < 0 && (bits as i64) < 0;
    let mut number = if negative {
        (bits as i64).unsigned_abs()
    } else {
        bits
    };
    let mut result = Vec::new();
    loop {
        result.push(b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"[(number % base as u64) as usize]);
        number /= base as u64;
        if number == 0 {
            break;
        }
    }
    if negative {
        result.push(b'-');
    }
    result.reverse();
    String::from_utf8(result).ok()
}
fn date_diff(unit: &str, start: i64, end: i64) -> Result<i64> {
    let duration = i128::from(end) - i128::from(start);
    let divisor = match unit.to_ascii_lowercase().trim_end_matches('s') {
        "microsecond" => Some(1),
        "millisecond" => Some(1_000),
        "second" => Some(1_000_000),
        "minute" => Some(60_000_000),
        "hour" => Some(3_600_000_000),
        "day" => Some(86_400_000_000),
        "week" => Some(604_800_000_000),
        _ => None,
    };
    if let Some(divisor) = divisor {
        return i64::try_from(duration / divisor).map_err(|_| error("date_diff overflow"));
    }
    let start = chrono::DateTime::from_timestamp_micros(start)
        .ok_or_else(|| error("date_diff timestamp out of range"))?
        .naive_utc();
    let end = chrono::DateTime::from_timestamp_micros(end)
        .ok_or_else(|| error("date_diff timestamp out of range"))?
        .naive_utc();
    let (earlier, later, sign) = if start <= end {
        (start, end, 1)
    } else {
        (end, start, -1)
    };
    let mut months = (i64::from(later.year()) - i64::from(earlier.year())) * 12
        + i64::from(later.month())
        - i64::from(earlier.month());
    let candidate = earlier
        .checked_add_months(Months::new(
            u32::try_from(months).map_err(|_| error("date_diff calendar overflow"))?,
        ))
        .ok_or_else(|| error("date_diff calendar overflow"))?;
    if candidate > later {
        months -= 1;
    }
    let months = months * sign;
    match unit.to_ascii_lowercase().trim_end_matches('s') {
        "month" => Ok(months),
        "quarter" => Ok(months / 3),
        "year" => Ok(months / 12),
        _ => Err(error(format!("Unsupported date_diff unit: {unit}"))),
    }
}
fn convert_tz(input: &str, from: &str, to: &str) -> Option<i64> {
    let value = NaiveDateTime::parse_from_str(input, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(input, "%Y-%m-%dT%H:%M:%S%.f"))
        .ok()
        .or_else(|| {
            NaiveDate::parse_from_str(input, "%Y-%m-%d")
                .ok()?
                .and_hms_opt(0, 0, 0)
        })?;
    let from: Tz = from.parse().ok()?;
    let to: Tz = to.parse().ok()?;
    Some(
        from.from_local_datetime(&value)
            .single()?
            .with_timezone(&to)
            .naive_local()
            .and_utc()
            .timestamp_micros(),
    )
}
fn like_regex(pattern: &str, escape: &str, insensitive: bool) -> Result<Regex> {
    let mut esc = escape.chars();
    let escape = esc.next().ok_or_else(|| error("Empty LIKE escape"))?;
    if esc.next().is_some() {
        return Err(error("LIKE escape must be one character"));
    }
    let mut chars = pattern.chars();
    let mut regex = String::from("\\A");
    while let Some(c) = chars.next() {
        if c == escape {
            let next = chars
                .next()
                .ok_or_else(|| error("LIKE pattern ends with its escape character"))?;
            regex.push_str(&regex::escape(&next.to_string()));
        } else {
            match c {
                '%' => regex.push_str(".*"),
                '_' => regex.push('.'),
                _ => regex.push_str(&regex::escape(&c.to_string())),
            }
        }
    }
    regex.push_str("\\z");
    RegexBuilder::new(&regex)
        .case_insensitive(insensitive)
        .dot_matches_new_line(true)
        .build()
        .map_err(|e| error(e.to_string()))
}
