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

//! Input normalization and Arrow conversion shared by core update operations.

use std::sync::Arc;

use arrow_array::{
    new_null_array, Array, ArrayRef, FixedSizeListArray, Float32Array, Float64Array,
    GenericListArray, Int64Array, LargeListArray, ListArray, MapArray, OffsetSizeTrait,
    RecordBatch, StringArray, StructArray,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Schema, TimeUnit};

/// PyPaimon distinguishes evaluated assignments (safe casts) from row-ID
/// input (numeric coercion). Both use the same encodings, strings and nested
/// traversal; callers select the contract explicitly.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum CastMode {
    Assignment,
    RowUpdate,
    // Internal whole-column fallback; never retries safe casts on children.
    Constructor,
}

fn invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

/// Resolve a referenced input field without silently selecting a duplicate.
pub(super) fn unique_column_index(schema: &Schema, name: &str) -> crate::Result<usize> {
    let mut matches = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| f.name() == name);
    let index = matches
        .next()
        .map(|(index, _)| index)
        .ok_or_else(|| invalid(format!("Input data must contain {name} column")))?;
    if matches.next().is_some() {
        return Err(invalid(format!(
            "Input field {name} is ambiguous: duplicate column names"
        )));
    }
    Ok(index)
}

/// Normalize row IDs before matching, overlap detection or writing. No caller
/// may interpret a different integer representation as different row identity.
pub(super) fn normalize_row_ids(batch: RecordBatch) -> crate::Result<RecordBatch> {
    let schema = batch.schema();
    let index = unique_column_index(&schema, "_ROW_ID")?;
    // Empty logical inputs do not participate in matching or writing.
    if batch.num_rows() == 0 {
        return Ok(batch);
    }
    let input = decode_dictionary(batch.column(index))?;
    if !input.data_type().is_integer() {
        return Err(invalid("_ROW_ID column must have an integer type"));
    }
    if input.null_count() != 0 {
        return Err(invalid("_ROW_ID must not be null"));
    }
    if *batch.column(index).data_type() == DataType::Int64 {
        return Ok(batch);
    }
    let value = cast_update_value(&input, &DataType::Int64, CastMode::Assignment)?;
    let mut columns = batch.columns().to_vec();
    columns[index] = value;
    let mut fields = schema.fields().to_vec();
    fields[index] = Arc::new(
        fields[index]
            .as_ref()
            .clone()
            .with_data_type(DataType::Int64),
    );
    let schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
    RecordBatch::try_new(schema, columns).map_err(|error| invalid(error.to_string()))
}

fn decode_dictionary(array: &ArrayRef) -> crate::Result<ArrayRef> {
    let mut array = array.clone();
    while let DataType::Dictionary(_, values) = array.data_type() {
        array = arrow_cast::cast_with_options(
            array.as_ref(),
            values,
            &arrow_cast::CastOptions {
                safe: false,
                ..Default::default()
            },
        )
        .map_err(|error| invalid(error.to_string()))?;
    }
    Ok(array)
}

fn units_per_second(unit: &TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

/// Row-ID coercion retries the entire column through Python constructor
/// semantics. A failed child cannot independently opt into a lossy conversion.
pub(super) fn cast_update_value(
    array: &ArrayRef,
    target: &DataType,
    mode: CastMode,
) -> crate::Result<ArrayRef> {
    if mode == CastMode::RowUpdate {
        cast_values(array, target, CastMode::Assignment)
            .or_else(|_| cast_values(array, target, CastMode::Constructor))
    } else {
        cast_values(array, target, mode)
    }
}

/// Cast logical values recursively, so dictionary encoding and nesting cannot
/// bypass primitive validation. The final outer cast only converts the layout.
fn cast_values(array: &ArrayRef, target: &DataType, mode: CastMode) -> crate::Result<ArrayRef> {
    if mode == CastMode::Constructor && array.null_count() == array.len() {
        return Ok(new_null_array(target, array.len()));
    }
    if array.data_type() == target {
        return Ok(array.clone());
    }
    if matches!(array.data_type(), DataType::Dictionary(..)) {
        return cast_values(&decode_dictionary(array)?, target, mode);
    }
    if let DataType::ListView(field) | DataType::LargeListView(field) = array.data_type() {
        // Convert only the offsets/layout first. Casting children here would
        // bypass the recursive precision and formatting rules below.
        let layout = DataType::LargeList(field.clone());
        let materialized = arrow_cast::cast_with_options(
            array.as_ref(),
            &layout,
            &arrow_cast::CastOptions {
                safe: false,
                ..Default::default()
            },
        )
        .map_err(|error| invalid(error.to_string()))?;
        return cast_values(&materialized, target, mode);
    }
    if matches!(array.data_type(), DataType::RunEndEncoded(..)) {
        return Err(invalid(
            "Run-end encoded update inputs are not supported by PyArrow casts",
        ));
    }
    let nested: Option<ArrayRef> = match (array.data_type(), target) {
        (DataType::Struct(_), DataType::Struct(fields)) => {
            let input = array.as_any().downcast_ref::<StructArray>().unwrap();
            let columns = fields
                .iter()
                .map(|field| match input.column_by_name(field.name()) {
                    Some(column) => {
                        let column = constructor_child(column, input.nulls(), 1, mode)?;
                        cast_values(&column, field.data_type(), mode)
                    }
                    None => Ok(new_null_array(field.data_type(), input.len())),
                })
                .collect::<crate::Result<Vec<_>>>()?;
            Some(Arc::new(
                StructArray::try_new_with_length(
                    fields.clone(),
                    columns,
                    input.nulls().cloned(),
                    input.len(),
                )
                .map_err(|error| invalid(error.to_string()))?,
            ))
        }
        (
            _,
            DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _),
        ) => match array.data_type() {
            DataType::List(_) => Some(cast_list(
                array.as_any().downcast_ref::<ListArray>().unwrap(),
                field,
                mode,
            )?),
            DataType::LargeList(_) => Some(cast_list(
                array.as_any().downcast_ref::<LargeListArray>().unwrap(),
                field,
                mode,
            )?),
            DataType::FixedSizeList(_, size) => {
                let input = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
                let values =
                    constructor_child(input.values(), input.nulls(), *size as usize, mode)?;
                let values = cast_values(&values, field.data_type(), mode)?;
                Some(Arc::new(
                    FixedSizeListArray::try_new_with_length(
                        field.clone(),
                        *size,
                        values,
                        input.nulls().cloned(),
                        input.len(),
                    )
                    .map_err(|error| invalid(error.to_string()))?,
                ))
            }
            _ => None,
        },
        (DataType::Map(_, _), DataType::Map(field, sorted)) => {
            let input = array.as_any().downcast_ref::<MapArray>().unwrap();
            let offsets = input.value_offsets();
            let start = offsets[0];
            let length = offsets[offsets.len() - 1] - start;
            let (offsets, indices) = constructor_offsets(offsets, input.nulls(), mode);
            let length = indices.as_ref().map_or(length as usize, |i| i.len());
            let DataType::Struct(fields) = field.data_type() else {
                return Err(invalid("Map entries must have a struct type"));
            };
            // Map fields may use custom names. Key/value roles are positional,
            // unlike ROW fields, so never match these two children by name.
            let columns = [input.keys(), input.values()]
                .into_iter()
                .zip(fields)
                .map(|(values, field)| {
                    let values = match &indices {
                        Some(indices) => arrow_select::take::take(values.as_ref(), indices, None)
                            .map_err(|error| invalid(error.to_string()))?,
                        None => values.slice(start as usize, length),
                    };
                    cast_values(&values, field.data_type(), mode)
                })
                .collect::<crate::Result<Vec<_>>>()?;
            let entries = StructArray::try_new_with_length(fields.clone(), columns, None, length)
                .map_err(|error| invalid(error.to_string()))?;
            let offsets = OffsetBuffer::new(offsets.into());
            Some(Arc::new(
                MapArray::try_new(
                    field.clone(),
                    offsets,
                    entries,
                    input.nulls().cloned(),
                    *sorted,
                )
                .map_err(|error| invalid(error.to_string()))?,
            ))
        }
        _ => None,
    };
    if nested.is_none()
        && *array.data_type() != DataType::Null
        && (array.data_type().is_nested() || target.is_nested())
    {
        return Err(invalid(format!(
            "Unsupported update cast from {:?} to {target:?}",
            array.data_type()
        )));
    }
    if let Some(nested) = nested {
        // Child values already follow the selected contract; only change the
        // container layout here (for example List to LargeList).
        return arrow_cast::cast_with_options(
            nested.as_ref(),
            target,
            &arrow_cast::CastOptions {
                safe: false,
                ..Default::default()
            },
        )
        .map_err(|error| invalid(error.to_string()));
    }
    cast_primitive(array, target, mode)
}

fn cast_list<O: OffsetSizeTrait>(
    input: &GenericListArray<O>,
    field: &arrow_schema::FieldRef,
    mode: CastMode,
) -> crate::Result<ArrayRef> {
    let offsets = input.value_offsets();
    let start = offsets[0];
    let end = offsets[offsets.len() - 1];
    // A sliced list can retain unused values before and after its visible rows.
    // Only the visible child range participates in casting and validation.
    let (offsets, indices) = constructor_offsets(offsets, input.nulls(), mode);
    let values = match indices {
        Some(indices) => arrow_select::take::take(input.values().as_ref(), &indices, None)
            .map_err(|error| invalid(error.to_string()))?,
        None => input
            .values()
            .slice(start.as_usize(), (end - start).as_usize()),
    };
    let values = cast_values(&values, field.data_type(), mode)?;
    let offsets = OffsetBuffer::new(offsets.into());
    Ok(Arc::new(
        GenericListArray::<O>::try_new(field.clone(), offsets, values, input.nulls().cloned())
            .map_err(|error| invalid(error.to_string()))?,
    ))
}

// Rebuilding Python values discards children hidden by a null parent. Safe
// casts deliberately retain them, since Arrow C++ validates their values too.
fn constructor_child(
    array: &ArrayRef,
    nulls: Option<&arrow_buffer::NullBuffer>,
    width: usize,
    mode: CastMode,
) -> crate::Result<ArrayRef> {
    if mode != CastMode::Constructor || nulls.is_none_or(|n| n.null_count() == 0) {
        return Ok(array.clone());
    }
    let nulls = nulls.unwrap();
    let indices = arrow_array::UInt64Array::from_iter(
        (0..array.len()).map(|i| nulls.is_valid(i / width).then_some(i as u64)),
    );
    arrow_select::take::take(array.as_ref(), &indices, None)
        .map_err(|error| invalid(error.to_string()))
}

fn constructor_offsets<O: OffsetSizeTrait>(
    offsets: &[O],
    nulls: Option<&arrow_buffer::NullBuffer>,
    mode: CastMode,
) -> (Vec<O>, Option<arrow_array::UInt64Array>) {
    if mode != CastMode::Constructor || nulls.is_none_or(|n| n.null_count() == 0) {
        return (offsets.iter().map(|o| *o - offsets[0]).collect(), None);
    }
    let nulls = nulls.unwrap();
    let mut indices = Vec::new();
    let mut rebased = vec![O::usize_as(0)];
    for (i, range) in offsets.windows(2).enumerate() {
        if nulls.is_valid(i) {
            indices.extend((range[0].as_usize()..range[1].as_usize()).map(|i| i as u64));
        }
        rebased.push(O::usize_as(indices.len()));
    }
    (rebased, Some(arrow_array::UInt64Array::from(indices)))
}

/// PyPaimon rebuilds temporal row values after a lossy safe cast fails.
/// Its constructors floor duration values instead of rounding a
/// negative fraction toward zero. Exact conversions still use Arrow directly.
fn coerce_temporal(array: &ArrayRef, target: &DataType) -> crate::Result<Option<ArrayRef>> {
    let (from, to, time_of_day) = match (array.data_type(), target) {
        (DataType::Duration(from), DataType::Duration(to)) => {
            (units_per_second(from), units_per_second(to), false)
        }
        (
            DataType::Time32(from) | DataType::Time64(from),
            DataType::Time32(to) | DataType::Time64(to),
        ) => (units_per_second(from), units_per_second(to), true),
        (DataType::Date64, DataType::Date32) => (86_400_000, 1, false),
        _ => return Ok(None),
    };
    if from <= to {
        return Ok(None);
    }
    let options = arrow_cast::CastOptions {
        safe: false,
        ..Default::default()
    };
    let raw_type = if matches!(array.data_type(), DataType::Time32(_)) {
        DataType::Int32
    } else {
        DataType::Int64
    };
    let raw = arrow_cast::cast_with_options(array.as_ref(), &raw_type, &options)
        .and_then(|raw| arrow_cast::cast_with_options(raw.as_ref(), &DataType::Int64, &options))
        .map_err(|error| invalid(error.to_string()))?;
    let raw = raw.as_any().downcast_ref::<Int64Array>().unwrap();
    let divisor = from / to;
    if raw.iter().flatten().all(|value| value % divisor == 0) {
        return Ok(None);
    }
    let converted = Int64Array::from_iter(raw.iter().map(|value| {
        value.map(|value| {
            let value = value.div_euclid(divisor);
            if time_of_day {
                value.rem_euclid(86_400 * to)
            } else {
                value
            }
        })
    }));
    let physical_type = if matches!(target, DataType::Time32(_) | DataType::Date32) {
        DataType::Int32
    } else {
        DataType::Int64
    };
    let physical = arrow_cast::cast_with_options(&converted, &physical_type, &options)
        .map_err(|error| invalid(error.to_string()))?;
    // Values already have the target physical units; preserve target timezone
    // metadata without interpreting the epoch count as a local wall time.
    let data = physical
        .to_data()
        .into_builder()
        .data_type(target.clone())
        .build()
        .map_err(|error| invalid(error.to_string()))?;
    Ok(Some(arrow_array::make_array(data)))
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
        .with_time_format(Some(&time_format))
        .with_datetime_format(Some("%Y-%m-%d"));
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

// Match Arrow C++'s ParseTimestampISO8601 grammar and fractional precision.
// Parsing at the requested unit must reject extra digits, even trailing zeros.
fn parse_timestamp_text(value: &str, unit: &TimeUnit, zoned: bool) -> Option<i64> {
    fn digits(value: &str) -> Option<u32> {
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        value.parse().ok()
    }
    if !value.is_ascii() || value.len() < 10 || &value[4..5] != "-" || &value[7..8] != "-" {
        return None;
    }
    let date = chrono::NaiveDate::from_ymd_opt(
        digits(&value[..4])? as i32,
        digits(&value[5..7])?,
        digits(&value[8..10])?,
    )?;
    let midnight = date.and_hms_opt(0, 0, 0)?.and_utc().timestamp();
    let units = units_per_second(unit);
    if value.len() == 10 {
        return (!zoned).then(|| midnight.checked_mul(units)).flatten();
    }
    if !matches!(value.as_bytes()[10], b' ' | b'T') {
        return None;
    }
    let mut time = &value[11..];
    let mut offset = 0_i64;
    let has_offset = if let Some(local) = time.strip_suffix('Z') {
        time = local;
        true
    } else if let Some(index) = time.find(['+', '-']) {
        let zone = &time[index + 1..];
        let (hours, minutes) = match zone.len() {
            2 => (digits(zone)?, 0),
            4 => (digits(&zone[..2])?, digits(&zone[2..])?),
            5 if &zone[2..3] == ":" => (digits(&zone[..2])?, digits(&zone[3..])?),
            _ => return None,
        };
        if hours >= 24 || minutes >= 60 {
            return None;
        }
        offset = i64::from(hours * 3600 + minutes * 60);
        if time.as_bytes()[index] == b'-' {
            offset = -offset;
        }
        time = &time[..index];
        true
    } else {
        false
    };
    if has_offset != zoned {
        return None;
    }
    let (clock, fraction) = match time.split_once('.') {
        Some((clock, fraction)) if clock.len() == 8 => {
            let precision = units.ilog10() as usize;
            if fraction.is_empty() || fraction.len() > precision {
                return None;
            }
            let fraction =
                i64::from(digits(fraction)?) * 10_i64.pow((precision - fraction.len()) as u32);
            (clock, fraction)
        }
        Some(_) => return None,
        None => (time, 0),
    };
    let (hour, minute, second) = match clock.len() {
        2 => (digits(clock)?, 0, 0),
        5 if &clock[2..3] == ":" => (digits(&clock[..2])?, digits(&clock[3..])?, 0),
        8 if &clock[2..3] == ":" && &clock[5..6] == ":" => (
            digits(&clock[..2])?,
            digits(&clock[3..5])?,
            digits(&clock[6..])?,
        ),
        _ => return None,
    };
    if hour >= 24 || minute >= 60 || second >= 60 {
        return None;
    }
    let seconds = midnight + i64::from(hour * 3600 + minute * 60 + second) - offset;
    // Use a wider intermediate so valid negative boundary values do not fail
    // before their positive fractional component is added.
    i64::try_from(i128::from(seconds) * i128::from(units) + i128::from(fraction)).ok()
}

fn timestamp_array(values: Int64Array, target: &DataType) -> crate::Result<ArrayRef> {
    let data = values
        .to_data()
        .into_builder()
        .data_type(target.clone())
        .build()
        .map_err(|error| invalid(error.to_string()))?;
    Ok(arrow_array::make_array(data))
}

/// Timestamp timezones are metadata for Arrow casts. Change physical units
/// without interpreting a timezone-free epoch count as local wall time.
fn cast_timestamp(array: &ArrayRef, target: &DataType, mode: CastMode) -> crate::Result<ArrayRef> {
    let DataType::Timestamp(from, _) = array.data_type() else {
        unreachable!()
    };
    let DataType::Timestamp(to, _) = target else {
        unreachable!()
    };
    let raw = arrow_cast::cast(array.as_ref(), &DataType::Int64)
        .map_err(|error| invalid(error.to_string()))?;
    let raw = raw.as_any().downcast_ref::<Int64Array>().unwrap();
    let from = units_per_second(from);
    let to = units_per_second(to);
    let values = raw
        .iter()
        .map(|value| {
            value
                .map(|value| {
                    if to >= from {
                        value
                            .checked_mul(to / from)
                            .ok_or_else(|| invalid("Timestamp assignment overflow"))
                    } else {
                        let divisor = from / to;
                        if mode == CastMode::Assignment && value % divisor != 0 {
                            return Err(invalid("Timestamp assignment would lose precision"));
                        }
                        // PyPaimon's row-ID constructor fallback rounds negative epochs down.
                        Ok(value.div_euclid(divisor))
                    }
                })
                .transpose()
        })
        .collect::<crate::Result<Vec<_>>>()?;
    timestamp_array(Int64Array::from(values), target)
}

// Arrow C++ accepts decimal with an optional minus, or unsigned hexadecimal
// interpreted in the target width (including two's complement for signed types).
fn parse_integer_text(value: &str, target: &DataType) -> Option<i128> {
    let bits = match target {
        DataType::Int8 | DataType::UInt8 => 8,
        DataType::Int16 | DataType::UInt16 => 16,
        DataType::Int32 | DataType::UInt32 => 32,
        DataType::Int64 | DataType::UInt64 => 64,
        _ => unreachable!(),
    };
    let signed = target.is_signed_integer();
    if let Some(hex) = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
    {
        if hex.is_empty() || hex.len() > bits / 4 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }
        let value = i128::from_str_radix(hex, 16).ok()?;
        return Some(if signed && value >= 1_i128 << (bits - 1) {
            value - (1_i128 << bits)
        } else {
            value
        });
    }
    let digits = if signed {
        value.strip_prefix('-').unwrap_or(value)
    } else {
        value
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let value = value.parse::<i128>().ok()?;
    let (min, max) = if signed {
        (-(1_i128 << (bits - 1)), (1_i128 << (bits - 1)) - 1)
    } else {
        (0, (1_i128 << bits) - 1)
    };
    (min..=max).contains(&value).then_some(value)
}

fn cast_integer_text(array: &ArrayRef, target: &DataType) -> crate::Result<ArrayRef> {
    let text = arrow_cast::cast(array.as_ref(), &DataType::Utf8)
        .map_err(|error| invalid(error.to_string()))?;
    let values = text
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|value| {
            value
                .map(|value| {
                    parse_integer_text(value, target)
                        .ok_or_else(|| invalid(format!("Invalid integer assignment: {value}")))
                })
                .transpose()
        })
        .collect::<crate::Result<Vec<_>>>()?;
    // Range was checked in the requested width, before narrowing the storage.
    let integers: ArrayRef = if target.is_signed_integer() {
        Arc::new(Int64Array::from_iter(
            values.into_iter().map(|v| v.map(|v| v as i64)),
        ))
    } else {
        Arc::new(arrow_array::UInt64Array::from_iter(
            values.into_iter().map(|v| v.map(|v| v as u64)),
        ))
    };
    arrow_cast::cast(integers.as_ref(), target).map_err(|error| invalid(error.to_string()))
}

fn temporal_storage(data_type: &DataType) -> Option<DataType> {
    match data_type {
        DataType::Date32 | DataType::Time32(_) => Some(DataType::Int32),
        DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(..)
        | DataType::Duration(_) => Some(DataType::Int64),
        _ => None,
    }
}

// Arrow C++ exposes temporal/physical-integer casts only at the exact storage
// width. Arrow Rust additionally accepts other numeric pairs and string times.
fn temporal_cast_supported(source: &DataType, target: &DataType) -> bool {
    use DataType::*;
    if *source == Null || (!source.is_temporal() && !target.is_temporal()) {
        return true;
    }
    if target.is_string()
        || temporal_storage(source).as_ref() == Some(target)
        || temporal_storage(target).as_ref() == Some(source)
    {
        return true;
    }
    if source.is_string() {
        return matches!(target, Date32 | Date64 | Timestamp(..));
    }
    matches!(
        (source, target),
        (Date32 | Date64, Date32 | Date64 | Timestamp(..))
            | (
                Timestamp(..),
                Timestamp(..) | Date32 | Date64 | Time32(_) | Time64(_)
            )
            | (Time32(_) | Time64(_), Time32(_) | Time64(_))
            | (Duration(_), Duration(_))
    )
}

// These are Python value categories accepted by pa.array(..., type=target),
// after the safe cast of the whole column has failed. Never parse text or
// stringify numeric/temporal children during this fallback.
fn constructor_accepts(source: &DataType, target: &DataType) -> bool {
    use DataType::*;
    let bytes = |t: &DataType| {
        t.is_string() || matches!(t, Binary | LargeBinary | BinaryView | FixedSizeBinary(_))
    };
    if *source == Null || source == target {
        return true;
    }
    if bytes(target) {
        return bytes(source);
    }
    if target.is_integer() {
        return source.is_numeric();
    }
    if target.is_floating() {
        return source.is_integer() || source.is_floating() || *source == Boolean;
    }
    if target.is_decimal() {
        return source.is_integer() || source.is_decimal();
    }
    if temporal_storage(target).is_some() && source.is_numeric() {
        return true;
    }
    matches!(
        (source, target),
        (Date32 | Date64 | Timestamp(..), Date32 | Date64)
            | (Timestamp(..), Timestamp(..))
            | (Time32(_) | Time64(_), Time32(_) | Time64(_))
            | (Duration(_), Duration(_))
    )
}

fn cast_primitive(array: &ArrayRef, target: &DataType, mode: CastMode) -> crate::Result<ArrayRef> {
    let source = array.data_type();
    if source == target {
        return Ok(array.clone());
    }
    if mode == CastMode::Constructor && !constructor_accepts(source, target) {
        return Err(invalid(format!(
            "Cannot reconstruct {source:?} values as {target:?}"
        )));
    }
    if mode == CastMode::Assignment && !temporal_cast_supported(source, target) {
        return Err(invalid(format!(
            "Unsupported assignment cast from {source:?} to {target:?}"
        )));
    }
    if mode == CastMode::Constructor && source.is_numeric() && temporal_storage(target).is_some() {
        let storage = temporal_storage(target).unwrap();
        let raw = cast_primitive(array, &storage, mode)?;
        return Ok(arrow_array::make_array(
            raw.to_data()
                .into_builder()
                .data_type(target.clone())
                .build()
                .map_err(|e| invalid(e.to_string()))?,
        ));
    }
    if target.is_string() {
        return cast_to_string(array, target);
    }
    let options = arrow_cast::CastOptions {
        safe: false,
        ..Default::default()
    };
    if matches!(
        target,
        DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
    ) && !matches!(
        source,
        DataType::Null
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
    ) && !source.is_string()
    {
        return Err(invalid(format!(
            "Unsupported assignment cast from {source:?} to {target:?}"
        )));
    }
    if matches!(
        source,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
    ) && (target.is_numeric() || *target == DataType::Boolean)
    {
        // Binary numeric inputs contain text, never native-endian numeric bytes.
        let text = arrow_cast::cast_with_options(array.as_ref(), &DataType::Utf8, &options)
            .map_err(|error| invalid(error.to_string()))?;
        return cast_primitive(&text, target, mode);
    }
    if matches!(source, DataType::Timestamp(..)) && matches!(target, DataType::Timestamp(..)) {
        return cast_timestamp(array, target, mode);
    }
    if source.is_string() {
        if let DataType::Timestamp(unit, timezone) = target {
            let text = arrow_cast::cast_with_options(array.as_ref(), &DataType::Utf8, &options)
                .map_err(|error| invalid(error.to_string()))?;
            let values = text
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|value| {
                    value
                        .map(|value| {
                            parse_timestamp_text(value, unit, timezone.is_some()).ok_or_else(|| {
                                invalid(format!("Invalid timestamp assignment: {value}"))
                            })
                        })
                        .transpose()
                })
                .collect::<crate::Result<Vec<_>>>()?;
            return timestamp_array(Int64Array::from(values), target);
        }
    }
    if source.is_string() && matches!(target, DataType::Date32 | DataType::Date64) {
        let text = arrow_cast::cast(array.as_ref(), &DataType::Utf8)
            .map_err(|e| invalid(e.to_string()))?;
        for value in text
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .flatten()
        {
            if value.len() != 10 || parse_timestamp_text(value, &TimeUnit::Second, false).is_none()
            {
                return Err(invalid(format!("Invalid date assignment: {value}")));
            }
        }
    }
    if source.is_string() && target.is_integer() {
        return cast_integer_text(array, target);
    }
    if source.is_string() && *target == DataType::Boolean {
        let text = arrow_cast::cast_with_options(array.as_ref(), &DataType::Utf8, &options)
            .map_err(|error| invalid(error.to_string()))?;
        let values = text
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|value| {
                value
                    .map(|value| {
                        if value == "1" || value.eq_ignore_ascii_case("true") {
                            Ok(true)
                        } else if value == "0" || value.eq_ignore_ascii_case("false") {
                            Ok(false)
                        } else {
                            Err(invalid(format!("Invalid boolean assignment: {value}")))
                        }
                    })
                    .transpose()
            })
            .collect::<crate::Result<Vec<_>>>()?;
        return Ok(Arc::new(arrow_array::BooleanArray::from(values)));
    }
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
    if mode == CastMode::Constructor {
        if let Some(converted) = coerce_temporal(array, target)? {
            return Ok(converted);
        }
    }
    let converted = arrow_cast::cast_with_options(array.as_ref(), target, &options)
        .map_err(|error| invalid(format!("Invalid assignment cast: {error}")))?;
    if let (DataType::Timestamp(from, _), DataType::Time32(to) | DataType::Time64(to)) =
        (source, target)
    {
        let from_units = units_per_second(from);
        let to_units = units_per_second(to);
        if from_units > to_units {
            let divisor = from_units / to_units;
            let raw = arrow_cast::cast_with_options(array.as_ref(), &DataType::Int64, &options)
                .map_err(|error| invalid(error.to_string()))?;
            if raw
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .flatten()
                .any(|value| value % divisor != 0)
            {
                return Err(invalid("Timestamp-to-time assignment would lose precision"));
            }
        }
    }
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
    // may round. Date/time extraction permits dropping a component, but
    // timestamp-to-time precision was checked separately above.
    let exact_numeric = (source.is_numeric() && target.is_integer())
        || (source.is_integer() && target.is_numeric())
        || (source.is_decimal() && target.is_decimal());
    let exact_temporal = (matches!(source, DataType::Time32(_) | DataType::Time64(_))
        && matches!(target, DataType::Time32(_) | DataType::Time64(_)))
        || (matches!(source, DataType::Duration(_)) && matches!(target, DataType::Duration(_)))
        || (matches!(source, DataType::Date32 | DataType::Date64)
            && matches!(target, DataType::Date32 | DataType::Date64));
    // PyPaimon's row-ID constructor fallback permits float/decimal-to-int
    // truncation and temporal unit coercion, but still rejects integer-to-float
    // range loss and decimal rescaling loss. Keep that distinction explicit.
    let coerce_integer = mode == CastMode::Constructor
        && (source.is_floating() || source.is_decimal())
        && target.is_integer();
    let exact =
        (exact_numeric && !coerce_integer) || (exact_temporal && mode == CastMode::Assignment);
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

#[cfg(test)]
mod tests {
    use super::*;

    fn cast_assignment(array: &ArrayRef, target: &DataType) -> crate::Result<ArrayRef> {
        cast_update_value(array, target, CastMode::Assignment)
    }

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
    #[test]
    fn encoded_and_nested_assignments_validate_logical_values() {
        use arrow_array::{types::Int32Type, DictionaryArray, ListArray};
        use arrow_schema::Field;
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![0, 1]),
                Arc::new(Float64Array::from(vec![Some(1.5), None])),
            )
            .unwrap(),
        );
        assert!(cast_assignment(&dictionary, &DataType::Int32).is_err());
        let output = cast_update_value(&dictionary, &DataType::Int32, CastMode::RowUpdate).unwrap();
        assert_eq!(
            output.to_data(),
            Int32Array::from(vec![Some(1), None]).to_data()
        );
        let text: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![0]),
                Arc::new(StringArray::from(vec!["1.234"])),
            )
            .unwrap(),
        );
        assert!(cast_assignment(&text, &DataType::Decimal128(10, 2)).is_err());

        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let input: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Float64, true)),
            OffsetBuffer::from_lengths([1, 2, 1]),
            Arc::new(Float64Array::from(vec![
                Some(1.5),
                Some(2.0),
                None,
                Some(3.5),
            ])),
            None,
        ));
        let target = DataType::List(field.clone());
        assert!(cast_assignment(&input, &target).is_err());
        let sliced = input.slice(1, 1);
        let output = cast_assignment(&sliced, &target).unwrap();
        let expected = ListArray::new(
            field,
            OffsetBuffer::from_lengths([2]),
            Arc::new(Int32Array::from(vec![Some(2), None])),
            None,
        );
        assert_eq!(output.to_data(), expected.to_data());

        let input: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("a", DataType::Float64, true)),
            Arc::new(Float64Array::from(vec![1.5])) as ArrayRef,
        )]));
        let target = DataType::Struct(vec![Field::new("a", DataType::Int32, true)].into());
        assert!(cast_assignment(&input, &target).is_err());
    }

    #[test]
    fn string_boolean_cast_uses_pyarrow_literals_for_both_update_modes() {
        for mode in [CastMode::Assignment, CastMode::RowUpdate] {
            for value in ["yes", "on", "t", "f", " YES ", " true "] {
                let input: ArrayRef = Arc::new(StringArray::from(vec![value]));
                assert!(
                    cast_update_value(&input, &DataType::Boolean, mode).is_err(),
                    "{value}"
                );
            }
            let input: ArrayRef = Arc::new(StringArray::from(vec![
                Some("TRUE"),
                Some("False"),
                Some("1"),
                Some("0"),
                None,
            ]));
            let result = cast_update_value(&input, &DataType::Boolean, mode).unwrap();
            assert_eq!(
                result.to_data(),
                arrow_array::BooleanArray::from(vec![
                    Some(true),
                    Some(false),
                    Some(true),
                    Some(false),
                    None
                ])
                .to_data()
            );
        }
    }

    #[test]
    fn timestamp_time_cast_checks_fraction_without_requiring_same_date() {
        use arrow_array::{Time32MillisecondArray, TimestampNanosecondArray};
        let target = DataType::Time32(TimeUnit::Millisecond);
        for value in [1_234_567, -1, -1_234_567] {
            let input: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![value]));
            assert!(cast_assignment(&input, &target).is_err());
        }
        let input: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
            Some(86_400_001_000_000),
            Some(-1_000_000),
            None,
        ]));
        let output = cast_assignment(&input, &target).unwrap();
        assert_eq!(
            output.to_data(),
            Time32MillisecondArray::from(vec![Some(1), Some(86_399_999), None]).to_data()
        );
    }

    #[test]
    fn row_ids_normalize_integer_widths_and_reject_invalid_values() {
        for input in [
            Arc::new(Int32Array::from(vec![0, 2])) as ArrayRef,
            Arc::new(arrow_array::UInt64Array::from(vec![0, 2])) as ArrayRef,
        ] {
            let batch = RecordBatch::try_from_iter([("_ROW_ID", input)]).unwrap();
            let normalized = normalize_row_ids(batch).unwrap();
            assert_eq!(
                normalized.column(0).to_data(),
                Int64Array::from(vec![0, 2]).to_data()
            );
        }
        for input in [
            Arc::new(arrow_array::UInt64Array::from(vec![u64::MAX])) as ArrayRef,
            Arc::new(Int32Array::from(vec![None])) as ArrayRef,
            Arc::new(Float64Array::from(vec![0.5])) as ArrayRef,
        ] {
            let batch = RecordBatch::try_from_iter([("_ROW_ID", input)]).unwrap();
            assert!(normalize_row_ids(batch).is_err());
        }
    }
    #[test]
    fn row_update_coercion_preserves_range_and_decimal_checks() {
        for (array, target) in [
            (
                Arc::new(Int64Array::from(vec![i64::MAX])) as ArrayRef,
                DataType::Float64,
            ),
            (
                Arc::new(Int64Array::from(vec![16_777_218])) as ArrayRef,
                DataType::Float32,
            ),
            (
                Arc::new(
                    Decimal128Array::from(vec![1234])
                        .with_precision_and_scale(10, 3)
                        .unwrap(),
                ) as ArrayRef,
                DataType::Decimal128(10, 2),
            ),
        ] {
            assert!(cast_update_value(&array, &target, CastMode::RowUpdate).is_err());
        }
        let input: ArrayRef = Arc::new(Float64Array::from(vec![1.5, -1.5]));
        let output = cast_update_value(&input, &DataType::Int32, CastMode::RowUpdate).unwrap();
        assert_eq!(output.to_data(), Int32Array::from(vec![1, -1]).to_data());
        let input: ArrayRef = Arc::new(arrow_array::TimestampNanosecondArray::from(vec![
            1_234_567, -1_234_567, -1,
        ]));
        let target = DataType::Timestamp(TimeUnit::Millisecond, None);
        let output = cast_update_value(&input, &target, CastMode::RowUpdate).unwrap();
        assert_eq!(
            output.to_data(),
            arrow_array::TimestampMillisecondArray::from(vec![1, -2, -1]).to_data()
        );
    }
    #[test]
    fn nested_layouts_do_not_bypass_logical_value_casts() {
        use arrow_array::{types::Float64Type, FixedSizeListArray, ListViewArray, MapArray};
        use arrow_schema::Field;
        let float_field = Arc::new(Field::new("item", DataType::Float64, true));
        let int_field = Arc::new(Field::new("item", DataType::Int32, true));
        let view: ArrayRef = Arc::new(ListViewArray::new(
            float_field.clone(),
            vec![0].into(),
            vec![1].into(),
            Arc::new(Float64Array::from(vec![1.5])),
            None,
        ));
        assert!(cast_assignment(&view, &DataType::List(int_field.clone())).is_err());
        let fixed: ArrayRef = Arc::new(
            FixedSizeListArray::from_iter_primitive::<Float64Type, _, _>(
                [Some(vec![Some(1.5)])],
                1,
            ),
        );
        for mode in [CastMode::Assignment, CastMode::RowUpdate] {
            assert!(cast_update_value(&fixed, &DataType::Int32, mode).is_err());
            let scalar: ArrayRef = Arc::new(Float64Array::from(vec![1.5]));
            assert!(cast_update_value(&scalar, &DataType::List(int_field.clone()), mode).is_err());
            assert!(cast_update_value(
                &scalar,
                &DataType::FixedSizeList(int_field.clone(), 1),
                mode
            )
            .is_err());
        }
        assert!(cast_assignment(&fixed, &DataType::FixedSizeList(int_field, 1)).is_err());
        let entries = StructArray::from(vec![
            (
                Arc::new(Field::new("key", DataType::Int32, false)),
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("value", DataType::Float64, true)),
                Arc::new(Float64Array::from(vec![1.5])) as ArrayRef,
            ),
        ]);
        let entries_field = Arc::new(Field::new("entries", entries.data_type().clone(), false));
        let map: ArrayRef = Arc::new(MapArray::new(
            entries_field,
            OffsetBuffer::from_lengths([1]),
            entries,
            None,
            false,
        ));
        let target = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Int32, false),
                        Field::new("value", DataType::Int32, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        assert!(cast_assignment(&map, &target).is_err());
    }
    #[test]
    fn maps_cast_by_key_value_role_instead_of_field_name() {
        use arrow_schema::Field;
        let entries = StructArray::from(vec![
            (
                Arc::new(Field::new("k", DataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["x"])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("v", DataType::Float64, true)),
                Arc::new(Float64Array::from(vec![2.0])) as ArrayRef,
            ),
        ]);
        let map: ArrayRef = Arc::new(MapArray::new(
            Arc::new(Field::new("pairs", entries.data_type().clone(), false)),
            OffsetBuffer::from_lengths([1]),
            entries,
            None,
            false,
        ));
        let target = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Int32, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        let result = cast_assignment(&map, &target).unwrap();
        let result = result.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(
            result.keys().to_data(),
            StringArray::from(vec!["x"]).to_data()
        );
        assert_eq!(
            result.values().to_data(),
            Int32Array::from(vec![2]).to_data()
        );
    }

    #[test]
    fn dictionary_row_ids_validate_decoded_nulls_and_integer_identity() {
        use arrow_array::{types::Int32Type, DictionaryArray};
        for values in [vec![Some(0), Some(2)], vec![Some(0), None]] {
            let valid = values.iter().all(Option::is_some);
            let input: ArrayRef = Arc::new(
                DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(vec![0, 1]),
                    Arc::new(Int32Array::from(values)),
                )
                .unwrap(),
            );
            let result =
                normalize_row_ids(RecordBatch::try_from_iter([("_ROW_ID", input)]).unwrap());
            if valid {
                assert_eq!(
                    result.unwrap().column(0).to_data(),
                    Int64Array::from(vec![0, 2]).to_data()
                );
            } else {
                assert!(result.is_err());
            }
        }
    }

    #[test]
    fn timestamp_text_preserves_pyarrow_precision_and_offset_rules() {
        for mode in [CastMode::Assignment, CastMode::RowUpdate] {
            for (unit, fraction) in [
                (TimeUnit::Second, ""),
                (TimeUnit::Millisecond, ".123"),
                (TimeUnit::Microsecond, ".123456"),
                (TimeUnit::Nanosecond, ".123456789"),
            ] {
                let text = format!("1970-01-01 00:00:00{fraction}");
                let input: ArrayRef = Arc::new(StringArray::from(vec![Some(text.as_str()), None]));
                let target = DataType::Timestamp(unit, None);
                let result = cast_update_value(&input, &target, mode).unwrap();
                let raw = arrow_cast::cast(result.as_ref(), &DataType::Int64).unwrap();
                assert_eq!(
                    raw.as_any().downcast_ref::<Int64Array>().unwrap().value(0),
                    match unit {
                        TimeUnit::Second => 0,
                        TimeUnit::Millisecond => 123,
                        TimeUnit::Microsecond => 123456,
                        TimeUnit::Nanosecond => 123456789,
                    }
                );
                assert!(result.is_null(1));
                let extra = format!(
                    "1970-01-01 00:00:00{}0",
                    if fraction.is_empty() { "." } else { fraction }
                );
                let input: ArrayRef = Arc::new(StringArray::from(vec![extra]));
                assert!(cast_update_value(&input, &target, mode).is_err());
            }
        }
        for (text, expected) in [
            ("1970-01-01", 0),
            ("1970-01-01T01", 3_600_000),
            ("1970-01-01 00:01", 60_000),
            ("1969-12-31 23:59:59.999", -1),
        ] {
            assert_eq!(
                parse_timestamp_text(text, &TimeUnit::Millisecond, false),
                Some(expected)
            );
        }
        for text in [
            "1970-01-01 00:00:00.123456",
            "1970-01-01 00:00:00.0000",
            "1970-01-01 00:00:00.",
            "1970-01-01t00:00:00",
            "1970-02-30",
            "1970-01-01 24:00:00",
            "1970-01-01 23:59:60",
            "1970-01-01T00:00:00Z",
            "1970-01-01T00:00:00+08:00",
            "不是时间",
            "1970-01-01 00:00:00.1234567890",
        ] {
            assert!(
                parse_timestamp_text(text, &TimeUnit::Millisecond, false).is_none(),
                "{text}"
            );
        }
        for text in [
            "1970-01-01T08+08",
            "1970-01-01 08:00+0800",
            "1970-01-01 08:00:00+08:00",
            "1970-01-01T00:00:00Z",
        ] {
            assert_eq!(
                parse_timestamp_text(text, &TimeUnit::Millisecond, true),
                Some(0)
            );
        }
        assert!(
            parse_timestamp_text("1970-01-01 00:00:00", &TimeUnit::Millisecond, true).is_none()
        );
    }

    #[test]
    fn timestamp_cast_changes_timezone_metadata_without_changing_epoch() {
        for source_zone in [None, Some("Asia/Shanghai"), Some("America/New_York")] {
            let values = TimestampMicrosecondArray::from(vec![Some(-1000), None, Some(1000)])
                .with_timezone_opt(source_zone);
            let input: ArrayRef = Arc::new(values);
            for target_zone in [None, Some("UTC"), Some("Asia/Shanghai")] {
                for mode in [CastMode::Assignment, CastMode::RowUpdate] {
                    let target =
                        DataType::Timestamp(TimeUnit::Millisecond, target_zone.map(Into::into));
                    let output = cast_update_value(&input, &target, mode).unwrap();
                    let raw = arrow_cast::cast(output.as_ref(), &DataType::Int64).unwrap();
                    assert_eq!(
                        raw.to_data(),
                        Int64Array::from(vec![Some(-1), None, Some(1)]).to_data()
                    );
                    assert_eq!(output.data_type(), &target);
                }
            }
        }
        let input: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from(vec![-1001]).with_timezone("Asia/Shanghai"));
        let target = DataType::Timestamp(TimeUnit::Millisecond, None);
        assert!(cast_assignment(&input, &target).is_err());
        let output = cast_update_value(&input, &target, CastMode::RowUpdate).unwrap();
        assert_eq!(
            arrow_cast::cast(output.as_ref(), &DataType::Int64)
                .unwrap()
                .to_data(),
            Int64Array::from(vec![-2]).to_data()
        );
        let input: ArrayRef = Arc::new(arrow_array::TimestampSecondArray::from(vec![i64::MAX]));
        assert!(cast_assignment(&input, &target).is_err());
    }

    #[test]
    fn binary_assignments_parse_text_and_never_reinterpret_numeric_bytes() {
        let binary: ArrayRef = Arc::new(arrow_array::BinaryArray::from(vec![
            Some(b"1.25".as_slice()),
            None,
        ]));
        for mode in [CastMode::Assignment, CastMode::RowUpdate] {
            for input_type in [
                DataType::Binary,
                DataType::LargeBinary,
                DataType::BinaryView,
            ] {
                let input = arrow_cast::cast(binary.as_ref(), &input_type).unwrap();
                let output = cast_update_value(&input, &DataType::Float64, mode).unwrap();
                assert_eq!(
                    output.to_data(),
                    Float64Array::from(vec![Some(1.25), None]).to_data()
                );
            }
            let input: ArrayRef = Arc::new(Int64Array::from(vec![42]));
            for target in [
                DataType::Binary,
                DataType::LargeBinary,
                DataType::BinaryView,
                DataType::FixedSizeBinary(8),
            ] {
                assert!(cast_update_value(&input, &target, mode).is_err());
            }
            let input: ArrayRef =
                Arc::new(arrow_array::BinaryArray::from(vec![b"true".as_slice()]));
            assert_eq!(
                cast_update_value(&input, &DataType::Boolean, mode)
                    .unwrap()
                    .to_data(),
                arrow_array::BooleanArray::from(vec![true]).to_data()
            );
            for invalid_text in [b"yes".as_slice(), b"\xff"] {
                let input: ArrayRef = Arc::new(arrow_array::BinaryArray::from(vec![invalid_text]));
                assert!(cast_update_value(&input, &DataType::Boolean, mode).is_err());
            }
            let input: ArrayRef =
                Arc::new(arrow_array::BinaryArray::from(vec![b"1.234".as_slice()]));
            assert!(cast_update_value(&input, &DataType::Decimal128(10, 2), mode).is_err());
        }
    }

    #[test]
    fn date64_strings_use_dates_without_time_components() {
        let input: ArrayRef = Arc::new(arrow_array::Date64Array::from(vec![
            Some(0),
            None,
            Some(-86_400_000),
        ]));
        for mode in [CastMode::Assignment, CastMode::RowUpdate] {
            let output = cast_update_value(&input, &DataType::Utf8, mode).unwrap();
            assert_eq!(
                output.to_data(),
                StringArray::from(vec![Some("1970-01-01"), None, Some("1969-12-31")]).to_data()
            );
        }
    }

    #[test]
    fn integer_text_checks_syntax_width_and_signed_hex_values() {
        for (target, bits) in [
            (DataType::Int8, 8),
            (DataType::Int16, 16),
            (DataType::Int32, 32),
            (DataType::Int64, 64),
            (DataType::UInt8, 8),
            (DataType::UInt16, 16),
            (DataType::UInt32, 32),
            (DataType::UInt64, 64),
        ] {
            let hex = format!("0x{}", "f".repeat(bits / 4));
            let expected = if target.is_signed_integer() {
                -1
            } else {
                (1_i128 << bits) - 1
            };
            assert_eq!(parse_integer_text(&hex, &target), Some(expected));
            assert_eq!(parse_integer_text("0X2a", &target), Some(42));
            assert_eq!(parse_integer_text("00042", &target), Some(42));
            for invalid in ["+42", " 42", "42 ", "42.0", "", "-", "0x", "-0x2a"] {
                assert!(
                    parse_integer_text(invalid, &target).is_none(),
                    "{invalid} -> {target:?}"
                );
            }
            assert!(parse_integer_text(&format!("{hex}0"), &target).is_none());
            let upper = (1_i128 << (bits - usize::from(target.is_signed_integer()))) - 1;
            assert_eq!(parse_integer_text(&upper.to_string(), &target), Some(upper));
            assert!(parse_integer_text(&(upper + 1).to_string(), &target).is_none());
            if target.is_signed_integer() {
                assert_eq!(
                    parse_integer_text(&(-upper - 1).to_string(), &target),
                    Some(-upper - 1)
                );
                assert!(parse_integer_text(&(-upper - 2).to_string(), &target).is_none());
            } else {
                assert!(parse_integer_text("-0", &target).is_none());
            }
            let input: ArrayRef = Arc::new(arrow_array::BinaryArray::from(vec![
                Some(hex.as_bytes()),
                None,
            ]));
            let output = cast_assignment(&input, &target).unwrap();
            assert_eq!(output.data_type(), &target);
            assert!(output.is_null(1));
            let output = cast_to_string(&output, &DataType::Utf8).unwrap();
            assert_eq!(
                output
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                expected.to_string()
            );
        }
    }
    #[test]
    fn referenced_input_names_must_be_unambiguous() {
        use arrow_schema::Field;
        let schema = Schema::new(vec![
            Field::new("_ROW_ID", DataType::Int64, false),
            Field::new("value", DataType::Int32, true),
            Field::new("value", DataType::Int32, true),
        ]);
        assert_eq!(unique_column_index(&schema, "_ROW_ID").unwrap(), 0);
        assert!(unique_column_index(&schema, "value").is_err());
        assert!(unique_column_index(&schema, "missing").is_err());
        let batch = RecordBatch::try_from_iter([
            ("_ROW_ID", Arc::new(Int64Array::from(vec![0])) as ArrayRef),
            ("_ROW_ID", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        ])
        .unwrap();
        assert!(normalize_row_ids(batch.clone()).is_err());
        assert!(normalize_row_ids(batch.slice(0, 0)).is_err());
    }

    #[test]
    fn temporal_casts_enforce_cpp_pairs_and_constructor_numeric_units() {
        let inputs: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![1.5, -1.5])),
            Arc::new(
                Decimal128Array::from(vec![15, -15])
                    .with_precision_and_scale(8, 1)
                    .unwrap(),
            ),
        ];
        for input in inputs {
            for target in [
                DataType::Date32,
                DataType::Date64,
                DataType::Time32(TimeUnit::Millisecond),
                DataType::Time64(TimeUnit::Microsecond),
                DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Duration(TimeUnit::Millisecond),
            ] {
                assert!(cast_assignment(&input, &target).is_err(), "{target:?}");
                let output = cast_update_value(&input, &target, CastMode::RowUpdate).unwrap();
                let raw =
                    arrow_cast::cast(output.as_ref(), &temporal_storage(&target).unwrap()).unwrap();
                let raw = arrow_cast::cast(raw.as_ref(), &DataType::Int64).unwrap();
                assert_eq!(raw.to_data(), Int64Array::from(vec![1, -1]).to_data());
            }
        }
        let input: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![1_000_000]));
        for target in [DataType::Float64, DataType::Int32, DataType::Date32] {
            for mode in [CastMode::Assignment, CastMode::RowUpdate] {
                assert_eq!(
                    cast_update_value(&input, &target, mode).is_ok(),
                    target == DataType::Date32
                );
            }
        }
        for (text, target, succeeds) in [
            (
                "00:00:01.234567",
                DataType::Time32(TimeUnit::Millisecond),
                false,
            ),
            ("2024-01-02 12:34:56", DataType::Date32, false),
            ("2024-01-02", DataType::Date32, true),
        ] {
            let input: ArrayRef = Arc::new(StringArray::from(vec![text]));
            for mode in [CastMode::Assignment, CastMode::RowUpdate] {
                assert_eq!(cast_update_value(&input, &target, mode).is_ok(), succeeds);
            }
        }
        // The physical integer width is part of Arrow C++'s safe cast contract.
        let input: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        let target = DataType::Timestamp(TimeUnit::Millisecond, None);
        assert!(cast_assignment(&input, &target).is_err());
        assert!(cast_update_value(&input, &target, CastMode::RowUpdate).is_ok());
    }

    #[test]
    fn nested_row_fallback_reconstructs_every_child_in_the_column() {
        use arrow_schema::Field;
        let input: ArrayRef = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("a", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("b", DataType::Float64, true)),
                Arc::new(Float64Array::from(vec![1.5])) as ArrayRef,
            ),
        ]));
        let target = DataType::Struct(
            vec![
                Field::new("a", DataType::Utf8, true),
                Field::new("b", DataType::Int32, true),
            ]
            .into(),
        );
        assert!(cast_update_value(&input, &target, CastMode::RowUpdate).is_err());
        // Safe conversion remains available when no child needs reconstruction.
        let target = DataType::Struct(
            vec![
                Field::new("a", DataType::Utf8, true),
                Field::new("b", DataType::Float64, true),
            ]
            .into(),
        );
        assert!(cast_update_value(&input, &target, CastMode::RowUpdate).is_ok());
    }

    #[test]
    fn row_fallback_discards_values_under_null_parents_only_after_safe_failure() {
        use arrow_schema::Field;
        let values: ArrayRef = Arc::new(Int64Array::from(vec![i64::MAX, 7]));
        let nulls = Some(arrow_buffer::NullBuffer::from(vec![false, true]));
        let from_field = Arc::new(Field::new("a", DataType::Int64, true));
        let to_field = Arc::new(Field::new("a", DataType::Int32, true));
        let inputs: Vec<(ArrayRef, DataType)> = vec![
            (
                Arc::new(StructArray::new(
                    vec![from_field.clone()].into(),
                    vec![values.clone()],
                    nulls.clone(),
                )),
                DataType::Struct(vec![to_field.clone()].into()),
            ),
            (
                Arc::new(ListArray::new(
                    from_field.clone(),
                    OffsetBuffer::from_lengths([1, 1]),
                    values.clone(),
                    nulls.clone(),
                )),
                DataType::LargeList(to_field.clone()),
            ),
            (
                Arc::new(FixedSizeListArray::new(from_field, 1, values, nulls)),
                DataType::FixedSizeList(to_field, 1),
            ),
        ];
        for (input, target) in inputs {
            assert!(cast_assignment(&input, &target).is_err());
            let output = cast_update_value(&input, &target, CastMode::RowUpdate).unwrap();
            assert!(output.is_null(0));
            assert!(output.is_valid(1));
            let child: ArrayRef = match &target {
                DataType::Struct(_) => output
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap()
                    .column(0)
                    .slice(1, 1),
                DataType::LargeList(_) => output
                    .as_any()
                    .downcast_ref::<LargeListArray>()
                    .unwrap()
                    .value(1),
                DataType::FixedSizeList(..) => output
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .unwrap()
                    .value(1),
                _ => unreachable!(),
            };
            assert_eq!(child.to_data(), Int32Array::from(vec![7]).to_data());
        }
    }
}
