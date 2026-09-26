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

//! Arrow assignment semantics shared by native update callers.

use arrow_array::{ArrayRef, Float32Array, Float64Array, RecordBatch, StringArray, UInt32Array};
use arrow_schema::{DataType, SchemaRef, TimeUnit};
use arrow_select::take::take;
use std::sync::Arc;

/// A literal (one value) or an Arrow array, retaining its original chunks.
pub enum UpdateAssignment {
    Scalar(ArrayRef),
    Array(Vec<ArrayRef>),
}

fn invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

fn format_float<T: Copy + Into<f64> + std::fmt::Display + std::fmt::LowerExp>(value: T) -> String {
    let number = value.into();
    if number.is_nan() {
        return "nan".into();
    }
    if number.is_finite() && number != 0.0 {
        let text = format!("{value:e}");
        let (mantissa, exponent) = text.split_once('e').unwrap();
        // Decide from the shortest decimal representation: widening an f32
        // first can put its binary approximation just below the 1e-6 boundary.
        if !(-6..10).contains(&exponent.parse::<i32>().unwrap()) {
            return if exponent.starts_with('-') {
                text
            } else {
                format!("{mantissa}e+{exponent}")
            };
        }
    }
    value.to_string()
}

fn cast_to_string(array: &ArrayRef, target: &DataType) -> crate::Result<ArrayRef> {
    let mut options = arrow_cast::CastOptions {
        safe: false,
        ..Default::default()
    };
    let floats: Option<StringArray> = match array.data_type() {
        DataType::Float32 => Some(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .iter()
                .map(|value| value.map(format_float))
                .collect(),
        ),
        DataType::Float64 => Some(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .iter()
                .map(|value| value.map(format_float))
                .collect(),
        ),
        _ => None,
    };
    if let Some(floats) = floats {
        return arrow_cast::cast_with_options(&floats, target, &options)
            .map_err(|error| invalid(error.to_string()));
    }
    let unit = match array.data_type() {
        DataType::Timestamp(unit, _) | DataType::Time32(unit) | DataType::Time64(unit) => {
            Some(unit)
        }
        _ => None,
    };
    let fraction = match unit {
        Some(TimeUnit::Second) | None => "",
        Some(TimeUnit::Millisecond) => "%.3f",
        Some(TimeUnit::Microsecond) => "%.6f",
        Some(TimeUnit::Nanosecond) => "%.9f",
    };
    let timestamp_format = format!("%Y-%m-%d %H:%M:%S{fraction}");
    let time_format = format!("%H:%M:%S{fraction}");
    let timezone = match array.data_type() {
        DataType::Timestamp(_, Some(tz)) if tz.as_ref() == "UTC" => "Z",
        _ => "%z",
    };
    let zoned_format = format!("{timestamp_format}{timezone}");
    options.format_options = arrow_cast::display::FormatOptions::new()
        .with_timestamp_format(Some(&timestamp_format))
        .with_timestamp_tz_format(Some(&zoned_format))
        .with_time_format(Some(&time_format));
    let duration;
    let input = if matches!(array.data_type(), DataType::Duration(_)) {
        duration = arrow_cast::cast(array.as_ref(), &DataType::Int64)
            .map_err(|error| invalid(error.to_string()))?;
        duration.as_ref()
    } else {
        array.as_ref()
    };
    arrow_cast::cast_with_options(input, target, &options)
        .map_err(|error| invalid(error.to_string()))
}

fn parse_decimal_text(value: &str, precision: u8, scale: i8) -> crate::Result<i128> {
    let invalid_value = || invalid(format!("Invalid decimal assignment: {value}"));
    let (mantissa, exponent) = value
        .split_once(['e', 'E'])
        .map_or(Ok((value, 0)), |(m, e)| {
            e.parse::<i32>()
                .map(|e| (m, e))
                .map_err(|_| invalid_value())
        })?;
    let negative = mantissa.starts_with('-');
    let mantissa = mantissa.strip_prefix(['-', '+']).unwrap_or(mantissa);
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let mut digits = format!("{whole}{fraction}");
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid_value());
    }
    let shift = i64::from(scale) + i64::from(exponent) - fraction.len() as i64;
    if shift < 0 {
        let removed = usize::try_from(-shift)
            .unwrap_or(usize::MAX)
            .min(digits.len());
        if digits[digits.len() - removed..]
            .bytes()
            .any(|digit| digit != b'0')
        {
            return Err(invalid("Decimal assignment would lose scale precision"));
        }
        digits.truncate(digits.len() - removed);
    }
    let significant = digits.trim_start_matches('0');
    if significant.is_empty() {
        return Ok(0);
    }
    if significant.len() as i64 + shift.max(0) > i64::from(precision) {
        return Err(invalid("Decimal assignment exceeds target precision"));
    }
    let mut unscaled = significant.parse::<i128>().map_err(|_| invalid_value())?;
    if shift > 0 {
        unscaled *= 10_i128.pow(shift as u32);
    }
    Ok(if negative { -unscaled } else { unscaled })
}

fn cast_assignment(array: &ArrayRef, target: &DataType) -> crate::Result<ArrayRef> {
    let source = array.data_type();
    if source == target {
        return Ok(array.clone());
    }
    if target.is_string() {
        return cast_to_string(array, target);
    }
    let options = arrow_cast::CastOptions {
        safe: false,
        ..Default::default()
    };
    if source.is_string() {
        if let DataType::Decimal128(precision, scale) = target {
            let text = arrow_cast::cast_with_options(array.as_ref(), &DataType::Utf8, &options)
                .map_err(|error| invalid(error.to_string()))?;
            let values = text
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|value| {
                    value
                        .map(|value| parse_decimal_text(value, *precision, *scale))
                        .transpose()
                })
                .collect::<crate::Result<Vec<_>>>()?;
            return Ok(Arc::new(
                arrow_array::Decimal128Array::from(values)
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|error| invalid(error.to_string()))?,
            ));
        }
    }
    let converted = arrow_cast::cast_with_options(array.as_ref(), target, &options)
        .map_err(|error| invalid(format!("Invalid assignment cast: {error}")))?;
    // PyArrow's safe integer-to-float cast checks the consecutive-integer
    // range, even for exactly representable values beyond that range.
    if source.is_integer() && target.is_floating() {
        let limit = match target {
            DataType::Float16 => 2048.0,
            DataType::Float32 => 16_777_216.0,
            DataType::Float64 => 9_007_199_254_740_992.0,
            _ => unreachable!(),
        };
        let values = arrow_cast::cast_with_options(array.as_ref(), &DataType::Float64, &options)
            .map_err(|error| invalid(error.to_string()))?;
        if values
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .flatten()
            .any(|value| value.abs() > limit)
        {
            return Err(invalid(format!(
                "Integer assignment exceeds the safe range for {target:?}"
            )));
        }
    }
    // Integer and decimal rescaling must be exact; float/decimal conversions
    // may round. Timestamp-to-date/time casts intentionally discard the date
    // or time component, while unit rescaling must preserve precision.
    let exact_numeric = (source.is_numeric() && target.is_integer())
        || (source.is_integer() && target.is_numeric())
        || (source.is_decimal() && target.is_decimal());
    let exact_temporal = (matches!(source, DataType::Timestamp(..))
        && matches!(target, DataType::Timestamp(..)))
        || (matches!(source, DataType::Time32(_) | DataType::Time64(_))
            && matches!(target, DataType::Time32(_) | DataType::Time64(_)))
        || (matches!(source, DataType::Duration(_)) && matches!(target, DataType::Duration(_)))
        || (matches!(source, DataType::Date32 | DataType::Date64)
            && matches!(target, DataType::Date32 | DataType::Date64));
    let exact = exact_numeric || exact_temporal;
    if exact {
        let restored = arrow_cast::cast_with_options(converted.as_ref(), source, &options)
            .map_err(|error| invalid(format!("Lossy assignment cast: {error}")))?;
        let equal = if source.is_floating() {
            // Floating-point equality intentionally treats -0.0 and +0.0 alike.
            let original = arrow_cast::cast(array.as_ref(), &DataType::Float64)
                .map_err(|error| invalid(error.to_string()))?;
            let restored = arrow_cast::cast(restored.as_ref(), &DataType::Float64)
                .map_err(|error| invalid(error.to_string()))?;
            original
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .iter()
                .eq(restored
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .iter())
        } else {
            restored.to_data() == array.to_data()
        };
        if !equal {
            return Err(invalid(format!(
                "Assignment cast from {source:?} to {target:?} would lose precision"
            )));
        }
    }
    Ok(converted)
}

struct Values {
    scalar: bool,
    chunks: Vec<ArrayRef>,
    chunk: usize,
    offset: usize,
}

impl Values {
    fn remaining(&self) -> usize {
        if self.scalar {
            usize::MAX
        } else {
            self.chunks[self.chunk].len() - self.offset
        }
    }

    fn take(&mut self, count: usize) -> crate::Result<ArrayRef> {
        if self.scalar {
            return take(
                self.chunks[0].as_ref(),
                &UInt32Array::from(vec![0; count]),
                None,
            )
            .map_err(|error| invalid(error.to_string()));
        }
        let array = self.chunks[self.chunk].slice(self.offset, count);
        self.offset += count;
        if self.offset == self.chunks[self.chunk].len() {
            self.chunk += 1;
            self.offset = 0;
        }
        Ok(array)
    }
}

pub(super) fn assigned_batches(
    matched: &[RecordBatch],
    assignments: Vec<(String, UpdateAssignment)>,
    schema: SchemaRef,
) -> crate::Result<Vec<RecordBatch>> {
    let row_count: usize = matched.iter().map(RecordBatch::num_rows).sum();
    if row_count == 0 {
        return Ok(Vec::new());
    }
    let mut names = std::collections::HashSet::new();
    let mut fields = vec![Arc::new(arrow_schema::Field::new(
        "_ROW_ID",
        DataType::Int64,
        false,
    ))];
    let mut values = Vec::new();
    for (name, assignment) in assignments {
        if name == "_ROW_ID" || !names.insert(name.clone()) {
            return Err(invalid(format!(
                "Invalid or duplicate assignment column {name}"
            )));
        }
        let field = schema
            .field_with_name(&name)
            .map_err(|error| invalid(error.to_string()))?;
        let (scalar, chunks) = match assignment {
            UpdateAssignment::Scalar(value) => (true, vec![value]),
            UpdateAssignment::Array(chunks) => (false, chunks),
        };
        let length: usize = chunks.iter().map(|array| array.len()).sum();
        if length != if scalar { 1 } else { row_count } {
            return Err(invalid(format!(
                "Assignment array length must match matched row count: {length} != {row_count}"
            )));
        }
        let chunks = chunks
            .iter()
            .filter(|chunk| !chunk.is_empty())
            .map(|chunk| cast_assignment(chunk, field.data_type()))
            .collect::<crate::Result<Vec<_>>>()?;
        values.push(Values {
            scalar,
            chunks,
            chunk: 0,
            offset: 0,
        });
        fields.push(Arc::new(field.clone()));
    }
    if values.is_empty() {
        return Err(invalid("assignments must not be empty"));
    }
    let output_schema = Arc::new(arrow_schema::Schema::new(fields));
    let mut output = Vec::new();
    for batch in matched {
        let row_ids = batch
            .column_by_name("_ROW_ID")
            .ok_or_else(|| invalid("Input data must contain _ROW_ID column"))?;
        let mut offset = 0;
        while offset < batch.num_rows() {
            let count = values
                .iter()
                .map(Values::remaining)
                .fold(batch.num_rows() - offset, usize::min);
            let mut arrays = vec![row_ids.slice(offset, count)];
            for value in &mut values {
                arrays.push(value.take(count)?);
            }
            output.push(
                RecordBatch::try_new(output_schema.clone(), arrays)
                    .map_err(|error| invalid(error.to_string()))?,
            );
            offset += count;
        }
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use arrow_array::{
        Decimal128Array, Float32Array, Int32Array, Int64Array, TimestampMicrosecondArray,
    };
    use arrow_schema::TimeUnit;

    #[test]
    fn integer_float_cast_uses_pyarrow_safe_range() {
        let input: ArrayRef = Arc::new(Int64Array::from(vec![16_777_216]));
        let output = cast_assignment(&input, &DataType::Float32).unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(0),
            16_777_216.0
        );
        for (value, target) in [
            (16_777_218, DataType::Float32),
            (9_007_199_254_740_994, DataType::Float64),
        ] {
            let input: ArrayRef = Arc::new(Int64Array::from(vec![value]));
            assert!(cast_assignment(&input, &target).is_err());
        }
    }

    #[test]
    fn decimal_float_rounding_is_allowed_but_decimal_rescaling_is_exact() {
        let input: ArrayRef = Arc::new(Float64Array::from(vec![1.234]));
        let output = cast_assignment(&input, &DataType::Decimal128(10, 2)).unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            123
        );
        let output = cast_assignment(&output, &DataType::Float32).unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(0),
            1.23
        );
        let input: ArrayRef = Arc::new(
            Decimal128Array::from(vec![1234])
                .with_precision_and_scale(10, 3)
                .unwrap(),
        );
        assert!(cast_assignment(&input, &DataType::Decimal128(10, 2)).is_err());
    }

    #[test]
    fn decimal_text_rejects_nonzero_discarded_digits() {
        for value in ["1.234", "1e-3", "-1001e-5"] {
            let input: ArrayRef = Arc::new(StringArray::from(vec![value]));
            assert!(
                cast_assignment(&input, &DataType::Decimal128(10, 2)).is_err(),
                "{value}"
            );
        }
        let input: ArrayRef = Arc::new(StringArray::from(vec!["1.230", "-1000e-5", "0e-5"]));
        let output = cast_assignment(&input, &DataType::Decimal128(10, 2)).unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[123, -1, 0]
        );
    }

    #[test]
    fn timestamp_date_extraction_and_unit_rescaling_differ() {
        let input: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![1234]));
        assert!(
            cast_assignment(&input, &DataType::Timestamp(TimeUnit::Millisecond, None)).is_err()
        );
        let output = cast_assignment(&input, &DataType::Date32).unwrap();
        assert_eq!(output.to_data().buffers()[0].typed_data::<i32>(), &[0]);
        let input: ArrayRef = Arc::new(Float64Array::from(vec![2.5]));
        assert!(cast_assignment(&input, &DataType::Int32).is_err());
        let input: ArrayRef = Arc::new(Float64Array::from(vec![-0.0, 2.0]));
        assert_eq!(
            cast_assignment(&input, &DataType::Int32).unwrap().to_data(),
            Int32Array::from(vec![0, 2]).to_data()
        );
    }

    #[test]
    fn string_assignments_use_pyarrow_float_and_temporal_formats() {
        let input: ArrayRef = Arc::new(Float64Array::from(vec![
            1.0,
            -0.0,
            1e10,
            1e-6,
            1e-7,
            f64::NAN,
        ]));
        let output = cast_assignment(&input, &DataType::Utf8).unwrap();
        assert_eq!(
            output.to_data(),
            StringArray::from(vec!["1", "-0", "1e+10", "0.000001", "1e-7", "nan"]).to_data()
        );
        let input: ArrayRef = Arc::new(Float32Array::from(vec![1e-6, 1e-7, 1e10]));
        let output = cast_assignment(&input, &DataType::Utf8).unwrap();
        assert_eq!(
            output.to_data(),
            StringArray::from(vec!["0.000001", "1e-7", "1e+10"]).to_data()
        );
        let input: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![1]));
        let output = cast_assignment(&input, &DataType::Utf8).unwrap();
        assert_eq!(
            output.to_data(),
            StringArray::from(vec!["1970-01-01 00:00:00.000001"]).to_data()
        );
    }
}
