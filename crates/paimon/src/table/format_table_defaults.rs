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

//! Apply schema defaults to nullable Format Table input columns. Java's
//! `DefaultValueRow` wraps the row after nullability validation and before
//! partition extraction, so partition and file columns share this behavior.

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BinaryArray, Date32Array, Int16Array, Int32Array, Int64Array, Int8Array,
    ListArray, MapArray, RecordBatch, StringArray, StructArray, Time32MillisecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{
    ArrowError, DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};

use crate::spec::{DataField, DataType};
use crate::{Error, Result};

pub(super) struct FormatTableDefaults {
    values: Vec<Option<ArrayRef>>,
}

impl FormatTableDefaults {
    pub(super) fn new(fields: &[DataField], schema: &ArrowSchema) -> Result<Self> {
        let values = fields
            .iter()
            .enumerate()
            .map(|(index, field)| {
                field
                    .default_value()
                    .map(|text| {
                        // Java DefaultValueUtils removes one pair of outer
                        // single quotes before casting from VARCHAR.
                        let text = text
                            .strip_prefix('\'')
                            .and_then(|text| text.strip_suffix('\''))
                            .unwrap_or(text);
                        let value = cast_default(field.data_type(), text, schema.field(index))
                            .map_err(|source| Error::DataInvalid {
                                message: format!(
                                    "Unsupported default value '{}' for Format Table column '{}'",
                                    field.default_value().unwrap_or_default(),
                                    field.name()
                                ),
                                source: Some(Box::new(source)),
                            })?;
                        if value.is_null(0) {
                            return Err(Error::DataInvalid {
                                message: format!(
                                    "Unsupported default value '{}' for Format Table column '{}'",
                                    field.default_value().unwrap_or_default(),
                                    field.name()
                                ),
                                source: None,
                            });
                        }
                        Ok(value)
                    })
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { values })
    }

    pub(super) fn apply(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        if !self
            .values
            .iter()
            .enumerate()
            .any(|(index, value)| value.is_some() && batch.column(index).null_count() > 0)
        {
            return Ok(batch.clone());
        }
        let columns = batch
            .columns()
            .iter()
            .enumerate()
            .map(|(index, input)| {
                let Some(value) = &self.values[index] else {
                    return Ok(input.clone());
                };
                if input.null_count() == 0 {
                    return Ok(input.clone());
                }
                let positions = arrow_array::UInt32Array::from(vec![0_u32; batch.num_rows()]);
                let repeated = arrow_select::take::take(value.as_ref(), &positions, None).map_err(
                    |source| Error::DataInvalid {
                        message: format!("Cannot repeat Format Table default for column {index}"),
                        source: Some(Box::new(source)),
                    },
                )?;
                let present = arrow_array::BooleanArray::from_iter(
                    (0..input.len()).map(|row| Some(!input.is_null(row))),
                );
                arrow_select::zip::zip(&present, input, &repeated).map_err(|source| {
                    Error::DataInvalid {
                        message: format!("Cannot apply Format Table default for column {index}"),
                        source: Some(Box::new(source)),
                    }
                })
            })
            .collect::<Result<Vec<_>>>()?;
        RecordBatch::try_new(Arc::clone(&batch.schema()), columns).map_err(|source| {
            Error::DataInvalid {
                message: "Cannot assemble Format Table row with defaults".into(),
                source: Some(Box::new(source)),
            }
        })
    }
}

fn cast_default(
    data_type: &DataType,
    text: &str,
    arrow_field: &arrow_schema::Field,
) -> std::result::Result<ArrayRef, arrow_schema::ArrowError> {
    match data_type {
        // Java's StringToStringCastRule counts characters, then pads CHAR with
        // spaces or truncates VARCHAR to its declared length.
        DataType::Char(typ) => {
            let mut value = text.chars().take(typ.length()).collect::<String>();
            value.extend(std::iter::repeat_n(
                ' ',
                typ.length().saturating_sub(value.chars().count()),
            ));
            Ok(Arc::new(StringArray::from(vec![value])))
        }
        DataType::VarChar(typ) => Ok(Arc::new(StringArray::from(vec![text
            .chars()
            .take(typ.length() as usize)
            .collect::<String>()]))),
        // Java's StringToBinaryCastRule uses UTF-8 bytes and pads only BINARY.
        DataType::Binary(typ) => {
            let mut value = text.as_bytes().to_vec();
            value.resize(typ.length(), 0);
            value.truncate(typ.length());
            Ok(Arc::new(BinaryArray::from(vec![value.as_slice()])))
        }
        DataType::VarBinary(typ) => {
            let bytes = text.as_bytes();
            let bytes = &bytes[..bytes.len().min(typ.length() as usize)];
            Ok(Arc::new(BinaryArray::from(vec![bytes])))
        }
        // Java BinaryStringUtils.toByte/toShort/toInt/toLong accept a decimal
        // suffix, validate its digits and truncate it toward zero.
        DataType::TinyInt(_) => Ok(Arc::new(Int8Array::from(vec![parse_java_integer(
            text,
            i64::from(i8::MIN),
            i64::from(i8::MAX),
        )? as i8]))),
        DataType::SmallInt(_) => Ok(Arc::new(Int16Array::from(vec![parse_java_integer(
            text,
            i64::from(i16::MIN),
            i64::from(i16::MAX),
        )? as i16]))),
        DataType::Int(_) => Ok(Arc::new(Int32Array::from(vec![parse_java_integer(
            text,
            i64::from(i32::MIN),
            i64::from(i32::MAX),
        )? as i32]))),
        DataType::BigInt(_) => Ok(Arc::new(Int64Array::from(vec![parse_java_integer(
            text,
            i64::MIN,
            i64::MAX,
        )?]))),
        // Java's BinaryStringUtils treats an all-digit DATE/TIME value as the
        // internal day/millisecond count and a TIMESTAMP value as the count in
        // the requested precision, rather than parsing it as a calendar string.
        DataType::Date(_) if numeric_default(text) => Ok(Arc::new(Date32Array::from(vec![text
            .parse::<i32>()
            .map_err(|source| arrow_schema::ArrowError::CastError(source.to_string()))?]))),
        DataType::Time(_) if numeric_default(text) => {
            Ok(Arc::new(Time32MillisecondArray::from(vec![text
                .parse::<i32>()
                .map_err(|source| {
                    arrow_schema::ArrowError::CastError(source.to_string())
                })?])))
        }
        DataType::Timestamp(typ) if numeric_default(text) => {
            let value = text
                .parse::<i64>()
                .map_err(|source| arrow_schema::ArrowError::CastError(source.to_string()))?;
            match typ.precision() {
                0 => Ok(Arc::new(TimestampSecondArray::from(vec![value]))),
                3 => Ok(Arc::new(TimestampMillisecondArray::from(vec![value]))),
                6 => Ok(Arc::new(TimestampMicrosecondArray::from(vec![value]))),
                9 => Ok(Arc::new(TimestampNanosecondArray::from(vec![value]))),
                precision => Err(arrow_schema::ArrowError::CastError(format!(
                    "Java does not support a numeric TIMESTAMP default at precision {precision}"
                ))),
            }
        }
        DataType::Timestamp(typ) => {
            // Arrow stores timestamps at 0, 3, 6 or 9 digits, while Java's
            // DateTimeUtils.parseTimestampData truncates to the declared
            // precision before creating the internal timestamp.
            let value = arrow_cast::cast(&StringArray::from(vec![text]), arrow_field.data_type())?;
            if value.is_null(0) {
                return Ok(value);
            }
            let precision = typ.precision();
            let stored_precision = match precision {
                0 => 0,
                1..=3 => 3,
                4..=6 => 6,
                7..=9 => 9,
                _ => {
                    return Err(arrow_schema::ArrowError::CastError(format!(
                        "Unsupported timestamp precision {precision}"
                    )))
                }
            };
            let divisor = 10_i64.pow(stored_precision - precision);
            if divisor == 1 {
                return Ok(value);
            }
            let raw = match stored_precision {
                3 => value
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap()
                    .value(0),
                6 => value
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap()
                    .value(0),
                9 => value
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .value(0),
                _ => unreachable!(),
            };
            let truncated = raw.div_euclid(divisor) * divisor;
            match stored_precision {
                3 => Ok(Arc::new(TimestampMillisecondArray::from(vec![truncated]))),
                6 => Ok(Arc::new(TimestampMicrosecondArray::from(vec![truncated]))),
                9 => Ok(Arc::new(TimestampNanosecondArray::from(vec![truncated]))),
                _ => unreachable!(),
            }
        }
        DataType::LocalZonedTimestamp(typ) => {
            let datetime = arrow_cast::parse::string_to_datetime(&chrono::Local, text)?;
            let precision = typ.precision();
            let quantum = 10_u32.pow(9 - precision);
            let nanos = datetime.timestamp_subsec_nanos() / quantum * quantum;
            let (units_per_second, fraction) = match precision {
                0 => (1_i128, 0_i128),
                1..=3 => (1_000, i128::from(nanos / 1_000_000)),
                4..=6 => (1_000_000, i128::from(nanos / 1_000)),
                7..=9 => (1_000_000_000, i128::from(nanos)),
                _ => {
                    return Err(arrow_schema::ArrowError::CastError(format!(
                        "Unsupported local timestamp precision {precision}"
                    )))
                }
            };
            let value =
                i64::try_from(i128::from(datetime.timestamp()) * units_per_second + fraction)
                    .map_err(|source| arrow_schema::ArrowError::CastError(source.to_string()))?;
            match precision {
                0 => Ok(Arc::new(
                    TimestampSecondArray::from(vec![value]).with_timezone("UTC"),
                )),
                1..=3 => Ok(Arc::new(
                    TimestampMillisecondArray::from(vec![value]).with_timezone("UTC"),
                )),
                4..=6 => Ok(Arc::new(
                    TimestampMicrosecondArray::from(vec![value]).with_timezone("UTC"),
                )),
                _ => Ok(Arc::new(
                    TimestampNanosecondArray::from(vec![value]).with_timezone("UTC"),
                )),
            }
        }
        DataType::Array(typ) => {
            let ArrowDataType::List(element_field) = arrow_field.data_type() else {
                return Err(cast_error("ARRAY default has a non-list Arrow field"));
            };
            let content = literal_content(text, '[', ']', "ARRAY")?;
            let elements = split_tokens(content)
                .iter()
                .map(|token| cast_token(typ.element_type(), token, element_field))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let values = concat_default_values(&elements, element_field.data_type())?;
            Ok(Arc::new(ListArray::try_new(
                Arc::clone(element_field),
                single_offset(elements.len())?,
                values,
                None,
            )?))
        }
        DataType::Row(typ) => {
            let ArrowDataType::Struct(arrow_fields) = arrow_field.data_type() else {
                return Err(cast_error("ROW default has a non-struct Arrow field"));
            };
            let content = literal_content(text, '{', '}', "STRUCT")?;
            let tokens = split_tokens(content);
            if !content.is_empty() && tokens.len() != typ.fields().len() {
                return Err(cast_error(format!(
                    "ROW default has {} fields, expected {}",
                    tokens.len(),
                    typ.fields().len()
                )));
            }
            let values = typ
                .fields()
                .iter()
                .enumerate()
                .map(|(index, field)| match tokens.get(index) {
                    Some(token) => cast_token(field.data_type(), token, &arrow_fields[index]),
                    None => Ok(arrow_array::new_null_array(
                        arrow_fields[index].data_type(),
                        1,
                    )),
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(Arc::new(StructArray::try_new(
                arrow_fields.clone(),
                values,
                None,
            )?))
        }
        DataType::Map(typ) => {
            let ArrowDataType::Map(entries_field, sorted) = arrow_field.data_type() else {
                return Err(cast_error("MAP default has a non-map Arrow field"));
            };
            let ArrowDataType::Struct(entry_fields) = entries_field.data_type() else {
                return Err(cast_error("MAP default entries are not a struct"));
            };
            let trimmed = text.trim();
            let pairs: Vec<(DefaultToken, DefaultToken)> = if trimmed.starts_with('{') {
                let content = literal_content(trimmed, '{', '}', "MAP")?;
                split_raw(content, ",", 0)
                    .into_iter()
                    .filter(|entry| !entry.is_empty())
                    .map(|entry| {
                        let pair = split_raw(&entry, "->", 2);
                        if pair.len() != 2 {
                            return Err(cast_error(format!("Invalid MAP entry: {entry}")));
                        }
                        Ok((single_token(&pair[0])?, single_token(&pair[1])?))
                    })
                    .collect::<std::result::Result<Vec<_>, _>>()?
            } else {
                let content = literal_content(trimmed, '{', '}', "MAP")?;
                let tokens = split_tokens(content);
                if !tokens.len().is_multiple_of(2) {
                    return Err(cast_error("MAP default has an odd number of tokens"));
                }
                tokens
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|pair| (pair[0].clone(), pair[1].clone()))
                    .collect()
            };
            let mut keys: Vec<ArrayRef> = Vec::with_capacity(pairs.len());
            let mut values: Vec<ArrayRef> = Vec::with_capacity(pairs.len());
            for (key_token, value_token) in &pairs {
                let key = cast_token(typ.key_type(), key_token, &entry_fields[0])?;
                let value = cast_token(typ.value_type(), value_token, &entry_fields[1])?;
                // Java builds a HashMap, so a later entry replaces an earlier
                // value with the same cast key.
                if let Some(index) = keys.iter().position(|old| old.to_data() == key.to_data()) {
                    values[index] = value;
                } else {
                    keys.push(key);
                    values.push(value);
                }
            }
            let count = keys.len();
            let entries = StructArray::try_new(
                entry_fields.clone(),
                vec![
                    concat_default_values(&keys, entry_fields[0].data_type())?,
                    concat_default_values(&values, entry_fields[1].data_type())?,
                ],
                None,
            )?;
            Ok(Arc::new(MapArray::try_new(
                Arc::clone(entries_field),
                single_offset(count)?,
                entries,
                None,
                *sorted,
            )?))
        }
        DataType::Multiset(_) => Err(cast_error(
            "Java does not support casting a string default to MULTISET",
        )),
        DataType::Vector(_) => Err(cast_error(
            "Java does not support casting a string default to VECTOR",
        )),
        DataType::Variant(_) => Err(cast_error(
            "Java does not support casting a string default to VARIANT",
        )),
        DataType::Blob(_) => Err(arrow_schema::ArrowError::CastError(
            "Java does not support casting a string default to BLOB".into(),
        )),
        _ => arrow_cast::cast(&StringArray::from(vec![text]), arrow_field.data_type()),
    }
}

fn numeric_default(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit())
}

fn parse_java_integer(text: &str, min: i64, max: i64) -> std::result::Result<i64, ArrowError> {
    let bytes = text.as_bytes();
    if bytes.is_empty() {
        return Err(cast_error("Empty integer default"));
    }
    let negative = bytes[0] == b'-';
    let mut index = usize::from(negative || bytes[0] == b'+');
    if index == bytes.len() {
        return Err(cast_error(format!("Invalid integer default: {text}")));
    }

    // Accumulate a negative value so the signed minimum remains representable.
    // Like Java, the digits before '.' may be empty; the fractional part does
    // not affect the result but must contain only decimal digits.
    let limit = if negative { min } else { -max };
    let mut value = 0_i64;
    while index < bytes.len() && bytes[index] != b'.' {
        let digit = bytes[index];
        if !digit.is_ascii_digit() {
            return Err(cast_error(format!("Invalid integer default: {text}")));
        }
        value = value
            .checked_mul(10)
            .and_then(|value| value.checked_sub(i64::from(digit - b'0')))
            .filter(|value| *value >= limit)
            .ok_or_else(|| cast_error(format!("Integer default overflow: {text}")))?;
        index += 1;
    }
    if index < bytes.len() && !bytes[index + 1..].iter().all(u8::is_ascii_digit) {
        return Err(cast_error(format!("Invalid integer default: {text}")));
    }
    Ok(if negative { value } else { -value })
}

#[derive(Clone)]
struct DefaultToken {
    value: String,
    literal: bool,
}

fn cast_error(message: impl Into<String>) -> ArrowError {
    ArrowError::CastError(message.into())
}

fn cast_token(
    data_type: &DataType,
    token: &DefaultToken,
    arrow_field: &ArrowField,
) -> std::result::Result<ArrayRef, ArrowError> {
    if !token.literal && token.value == "null" {
        return Ok(arrow_array::new_null_array(arrow_field.data_type(), 1));
    }
    let value = cast_default(data_type, &token.value, arrow_field)?;
    if value.is_null(0) {
        return Err(cast_error(format!(
            "Cannot cast '{}' to {data_type}",
            token.value
        )));
    }
    Ok(value)
}

fn concat_default_values(
    values: &[ArrayRef],
    data_type: &ArrowDataType,
) -> std::result::Result<ArrayRef, ArrowError> {
    if values.is_empty() {
        return Ok(arrow_array::new_empty_array(data_type));
    }
    let arrays = values
        .iter()
        .map(|value| value.as_ref())
        .collect::<Vec<_>>();
    arrow_select::concat::concat(&arrays)
}

fn single_offset(count: usize) -> std::result::Result<OffsetBuffer<i32>, ArrowError> {
    let count = i32::try_from(count).map_err(|source| cast_error(source.to_string()))?;
    Ok(OffsetBuffer::new(ScalarBuffer::from(vec![0, count])))
}

// Java StringToArrayCastRule, StringToMapCastRule and StringToRowCastRule
// accept both bracket syntax and SQL function syntax.
fn literal_content<'a>(
    text: &'a str,
    open: char,
    close: char,
    function: &str,
) -> std::result::Result<&'a str, ArrowError> {
    let text = text.trim();
    if let Some(content) = text.strip_prefix(open).and_then(|v| v.strip_suffix(close)) {
        return Ok(content.trim());
    }
    let prefix = text.get(..function.len());
    if prefix.is_some_and(|prefix| prefix.eq_ignore_ascii_case(function)) {
        let tail = text[function.len()..].trim_start();
        if let Some(content) = tail.strip_prefix('(').and_then(|v| v.strip_suffix(')')) {
            return Ok(content.trim());
        }
    }
    Err(cast_error(format!("Invalid {function} default: {text}")))
}

fn single_token(text: &str) -> std::result::Result<DefaultToken, ArrowError> {
    let tokens = split_tokens(text);
    if tokens.len() != 1 {
        return Err(cast_error(format!("Invalid MAP entry token: {text}")));
    }
    Ok(tokens.into_iter().next().unwrap())
}

// Port of Java TokenSplitter.split. At the current level quotes and escapes
// make a token literal; nested punctuation is retained for recursive casting.
fn split_tokens(content: &str) -> Vec<DefaultToken> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut brackets = Vec::new();
    let mut in_quotes = false;
    let mut escaped = false;
    let mut literal = false;
    let mut end = 0;
    for ch in content.chars() {
        let nested = !brackets.is_empty();
        if escaped {
            escaped = false;
            current.push(ch);
            end = current.len();
            continue;
        }
        if ch == '\\' {
            escaped = true;
            if nested {
                current.push(ch);
                end = current.len();
            } else {
                literal = true;
            }
            continue;
        }
        if ch == '"' {
            in_quotes = !in_quotes;
            if nested {
                current.push(ch);
                end = current.len();
            } else {
                literal = true;
            }
            continue;
        }
        if !in_quotes {
            if is_open_bracket(ch) {
                brackets.push(ch);
            } else if is_close_bracket(ch) && !brackets.is_empty() {
                brackets.pop();
            } else if ch == ',' && brackets.is_empty() {
                push_token(&mut tokens, &current, end, literal);
                current.clear();
                end = 0;
                literal = false;
                continue;
            } else if ch.is_whitespace() && end == 0 {
                continue;
            }
        }
        current.push(ch);
        if in_quotes || !ch.is_whitespace() {
            end = current.len();
        }
    }
    push_token(&mut tokens, &current, end, literal);
    tokens
}

fn push_token(tokens: &mut Vec<DefaultToken>, current: &str, end: usize, literal: bool) {
    if end > 0 || literal {
        tokens.push(DefaultToken {
            value: current[..end].to_string(),
            literal,
        });
    }
}

// Java TokenSplitter.splitRaw leaves quotes and escapes in place for its
// second pass, where a MAP entry is split on `->`.
fn split_raw(content: &str, delimiter: &str, limit: usize) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut brackets = Vec::new();
    let mut in_quotes = false;
    let mut escaped = false;
    let mut splits = 0;
    let mut chars = content.chars().peekable();
    while let Some(ch) = chars.next() {
        if escaped {
            escaped = false;
            current.push(ch);
            continue;
        }
        if ch == '\\' {
            escaped = true;
            current.push(ch);
            continue;
        }
        if ch == '"' {
            in_quotes = !in_quotes;
            current.push(ch);
            continue;
        }
        if !in_quotes {
            if is_open_bracket(ch) {
                brackets.push(ch);
            } else if is_close_bracket(ch) && !brackets.is_empty() {
                brackets.pop();
            } else if brackets.is_empty()
                && (limit == 0 || splits < limit - 1)
                && ch == delimiter.chars().next().unwrap()
                && chars
                    .clone()
                    .take(delimiter.chars().count() - 1)
                    .collect::<String>()
                    == delimiter.chars().skip(1).collect::<String>()
            {
                tokens.push(current.trim().to_string());
                current.clear();
                splits += 1;
                for _ in 1..delimiter.chars().count() {
                    chars.next();
                }
                continue;
            }
        }
        current.push(ch);
    }
    tokens.push(current.trim().to_string());
    tokens
}

fn is_open_bracket(ch: char) -> bool {
    matches!(ch, '[' | '{' | '(')
}

fn is_close_bracket(ch: char) -> bool {
    matches!(ch, ']' | '}' | ')')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::build_target_arrow_schema;
    use crate::spec::{BigIntType, DataType, VarCharType};
    use arrow_array::{Int64Array, StringArray};

    fn fields() -> Vec<DataField> {
        vec![
            DataField::new(
                0,
                "dt".into(),
                DataType::VarChar(VarCharType::string_type()),
            )
            .with_default_value(Some("'new'".into())),
            DataField::new(1, "id".into(), DataType::BigInt(BigIntType::new()))
                .with_default_value(Some("42".into())),
            DataField::new(
                2,
                "note".into(),
                DataType::VarChar(VarCharType::string_type()),
            ),
        ]
    }

    #[test]
    fn replaces_only_nulls_in_partition_and_data_columns() {
        let fields = fields();
        let schema = build_target_arrow_schema(&fields).unwrap();
        let defaults = FormatTableDefaults::new(&fields, &schema).unwrap();
        let input = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![None, Some("old"), None])),
                Arc::new(Int64Array::from(vec![Some(7), None, None])),
                Arc::new(StringArray::from(vec![None, Some("x"), None])),
            ],
        )
        .unwrap();

        let actual = defaults.apply(&input).unwrap();
        let dt = actual
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let id = actual
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let note = actual
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            dt.iter().collect::<Vec<_>>(),
            vec![Some("new"), Some("old"), Some("new")]
        );
        assert_eq!(
            id.iter().collect::<Vec<_>>(),
            vec![Some(7), Some(42), Some(42)]
        );
        assert_eq!(note.iter().collect::<Vec<_>>(), vec![None, Some("x"), None]);
        assert_eq!(
            input.column(0).null_count(),
            2,
            "input must remain unchanged"
        );
    }

    #[test]
    fn rejects_invalid_default_at_writer_creation() {
        let mut fields = fields();
        fields[1] = fields[1]
            .clone()
            .with_default_value(Some("'not a number'".into()));
        let schema = build_target_arrow_schema(&fields).unwrap();
        assert!(FormatTableDefaults::new(&fields, &schema)
            .err()
            .unwrap()
            .to_string()
            .contains("Unsupported default value"));
    }

    #[test]
    fn integer_defaults_truncate_fraction_like_java() {
        use crate::spec::{IntType, SmallIntType, TinyIntType};
        use arrow_array::{Int16Array, Int32Array, Int8Array};

        let fields = vec![
            DataField::new(0, "tiny".into(), DataType::TinyInt(TinyIntType::new()))
                .with_default_value(Some("42.9".into())),
            DataField::new(1, "small".into(), DataType::SmallInt(SmallIntType::new()))
                .with_default_value(Some("-42.9".into())),
            DataField::new(2, "int".into(), DataType::Int(IntType::new()))
                .with_default_value(Some("2147483647.9".into())),
            DataField::new(3, "big".into(), DataType::BigInt(BigIntType::new()))
                .with_default_value(Some("-9223372036854775808.9".into())),
        ];
        let schema = build_target_arrow_schema(&fields).unwrap();
        let defaults = FormatTableDefaults::new(&fields, &schema).unwrap();
        let input = RecordBatch::try_new(
            schema.clone(),
            schema
                .fields()
                .iter()
                .map(|field| arrow_array::new_null_array(field.data_type(), 1))
                .collect(),
        )
        .unwrap();
        let actual = defaults.apply(&input).unwrap();
        assert_eq!(
            actual
                .column(0)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(0),
            42
        );
        assert_eq!(
            actual
                .column(1)
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(0),
            -42
        );
        assert_eq!(
            actual
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            i32::MAX
        );
        assert_eq!(
            actual
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            i64::MIN
        );
    }

    #[test]
    fn integer_defaults_reject_overflow_and_invalid_fraction() {
        use crate::spec::{IntType, SmallIntType, TinyIntType};

        for (data_type, text) in [
            (DataType::TinyInt(TinyIntType::new()), "128.1"),
            (DataType::SmallInt(SmallIntType::new()), "32768.1"),
            (DataType::Int(IntType::new()), "2147483648.1"),
            (DataType::BigInt(BigIntType::new()), "9223372036854775808.1"),
            (DataType::Int(IntType::new()), "42.x"),
        ] {
            let fields =
                [DataField::new(0, "number".into(), data_type)
                    .with_default_value(Some(text.into()))];
            let schema = build_target_arrow_schema(&fields).unwrap();
            assert!(
                FormatTableDefaults::new(&fields, &schema).is_err(),
                "{text}"
            );
        }
    }

    #[test]
    fn no_nulls_passes_input_through() {
        let fields = fields();
        let schema = build_target_arrow_schema(&fields).unwrap();
        let defaults = FormatTableDefaults::new(&fields, &schema).unwrap();
        let input = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["old"])),
                Arc::new(Int64Array::from(vec![7])),
                Arc::new(StringArray::from(vec!["x"])),
            ],
        )
        .unwrap();
        let actual = defaults.apply(&input).unwrap();
        assert_eq!(actual, input);
    }

    #[test]
    fn converts_java_quoted_dates_and_numeric_defaults() {
        use crate::spec::{BooleanType, DateType, DecimalType};
        use arrow_array::{BooleanArray, Date32Array, Decimal128Array};
        let fields = vec![
            DataField::new(0, "day".into(), DataType::Date(DateType::new()))
                .with_default_value(Some("'2025-01-02'".into())),
            DataField::new(1, "enabled".into(), DataType::Boolean(BooleanType::new()))
                .with_default_value(Some("true".into())),
            DataField::new(
                2,
                "amount".into(),
                DataType::Decimal(DecimalType::new(10, 2).unwrap()),
            )
            .with_default_value(Some("12.34".into())),
        ];
        let schema = build_target_arrow_schema(&fields).unwrap();
        let defaults = FormatTableDefaults::new(&fields, &schema).unwrap();
        let amount = Decimal128Array::from(vec![None])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let input = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Date32Array::from(vec![None])),
                Arc::new(BooleanArray::from(vec![None])),
                Arc::new(amount),
            ],
        )
        .unwrap();
        let actual = defaults.apply(&input).unwrap();
        let day = actual
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        let enabled = actual
            .column(1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let amount = actual
            .column(2)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(day.value(0), 20_090);
        assert!(enabled.value(0));
        assert_eq!(amount.value(0), 1234);
        assert_eq!(input.column(0).null_count(), 1);
    }

    #[test]
    fn character_and_binary_defaults_follow_java_length_rules() {
        use crate::spec::{BinaryType, CharType, VarBinaryType};
        let fields = vec![
            DataField::new(
                0,
                "fixed_text".into(),
                DataType::Char(CharType::new(3).unwrap()),
            )
            .with_default_value(Some("'猫'".into())),
            DataField::new(
                1,
                "short_text".into(),
                DataType::VarChar(VarCharType::new(2).unwrap()),
            )
            .with_default_value(Some("'猫狗鱼'".into())),
            DataField::new(
                2,
                "fixed_bytes".into(),
                DataType::Binary(BinaryType::new(4).unwrap()),
            )
            .with_default_value(Some("'é'".into())),
            DataField::new(
                3,
                "short_bytes".into(),
                DataType::VarBinary(VarBinaryType::new(2).unwrap()),
            )
            .with_default_value(Some("'猫'".into())),
        ];
        let schema = build_target_arrow_schema(&fields).unwrap();
        let defaults = FormatTableDefaults::new(&fields, &schema).unwrap();
        let input = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(BinaryArray::from(vec![None::<&[u8]>])),
                Arc::new(BinaryArray::from(vec![None::<&[u8]>])),
            ],
        )
        .unwrap();
        let actual = defaults.apply(&input).unwrap();
        assert_eq!(
            actual
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "猫  "
        );
        assert_eq!(
            actual
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "猫狗"
        );
        assert_eq!(
            actual
                .column(2)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            &[0xc3, 0xa9, 0, 0]
        );
        assert_eq!(
            actual
                .column(3)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            &[0xe7, 0x8c]
        );
    }

    #[test]
    fn numeric_temporal_defaults_use_java_internal_units() {
        use crate::spec::{DateType, TimeType, TimestampType};
        let fields = vec![
            DataField::new(0, "day".into(), DataType::Date(DateType::new()))
                .with_default_value(Some("42".into())),
            DataField::new(1, "time".into(), DataType::Time(TimeType::new(3).unwrap()))
                .with_default_value(Some("12345".into())),
            DataField::new(
                2,
                "seconds".into(),
                DataType::Timestamp(TimestampType::new(0).unwrap()),
            )
            .with_default_value(Some("123".into())),
            DataField::new(
                3,
                "micros".into(),
                DataType::Timestamp(TimestampType::new(6).unwrap()),
            )
            .with_default_value(Some("123456".into())),
        ];
        let schema = build_target_arrow_schema(&fields).unwrap();
        let defaults = FormatTableDefaults::new(&fields, &schema).unwrap();
        let input = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Date32Array::from(vec![None])),
                Arc::new(Time32MillisecondArray::from(vec![None])),
                Arc::new(TimestampSecondArray::from(vec![None])),
                Arc::new(TimestampMicrosecondArray::from(vec![None])),
            ],
        )
        .unwrap();
        let actual = defaults.apply(&input).unwrap();
        assert_eq!(
            actual
                .column(0)
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .value(0),
            42
        );
        assert_eq!(
            actual
                .column(1)
                .as_any()
                .downcast_ref::<Time32MillisecondArray>()
                .unwrap()
                .value(0),
            12345
        );
        assert_eq!(
            actual
                .column(2)
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap()
                .value(0),
            123
        );
        assert_eq!(
            actual
                .column(3)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .value(0),
            123456
        );
    }

    #[test]
    fn rejects_blob_default_like_java() {
        use crate::spec::BlobType;
        let fields = vec![
            DataField::new(0, "blob".into(), DataType::Blob(BlobType::new()))
                .with_default_value(Some("'raw bytes'".into())),
        ];
        let schema = build_target_arrow_schema(&fields).unwrap();
        assert!(FormatTableDefaults::new(&fields, &schema).is_err());
    }

    #[test]
    fn local_timestamp_default_uses_system_timezone_and_declared_precision() {
        use crate::spec::LocalZonedTimestampType;
        use chrono::TimeZone;
        let fields = vec![DataField::new(
            0,
            "local_time".into(),
            DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(3).unwrap()),
        )
        .with_default_value(Some("'2025-01-02 03:04:05.123456'".into()))];
        let schema = build_target_arrow_schema(&fields).unwrap();
        let defaults = FormatTableDefaults::new(&fields, &schema).unwrap();
        let input = RecordBatch::try_new(
            schema,
            vec![Arc::new(
                TimestampMillisecondArray::from(vec![None]).with_timezone("UTC"),
            )],
        )
        .unwrap();
        let actual = defaults.apply(&input).unwrap();
        let local = chrono::NaiveDate::from_ymd_opt(2025, 1, 2)
            .unwrap()
            .and_hms_milli_opt(3, 4, 5, 123)
            .unwrap();
        let expected = chrono::Local
            .from_local_datetime(&local)
            .single()
            .unwrap()
            .timestamp_millis();
        assert_eq!(
            actual
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .value(0),
            expected
        );
    }

    #[test]
    fn array_default_parses_each_element_like_java() {
        use crate::spec::{ArrayType, IntType};
        use arrow_array::{Int32Array, ListArray};

        let fields = vec![DataField::new(
            0,
            "numbers".into(),
            DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
        )
        .with_default_value(Some("'[1,2]'".into()))];
        let schema = build_target_arrow_schema(&fields).unwrap();
        let defaults = FormatTableDefaults::new(&fields, &schema).unwrap();
        let empty = arrow_array::new_null_array(schema.field(0).data_type(), 1);
        let input = RecordBatch::try_new(schema, vec![empty]).unwrap();
        let actual = defaults.apply(&input).unwrap();
        let values = actual
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value(0);
        let values = values.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(values.iter().collect::<Vec<_>>(), vec![Some(1), Some(2)]);
    }

    #[test]
    fn timestamp_default_truncates_to_declared_precision() {
        use crate::spec::TimestampType;

        let fields = vec![DataField::new(
            0,
            "time".into(),
            DataType::Timestamp(TimestampType::new(1).unwrap()),
        )
        .with_default_value(Some("'1970-01-01 00:00:00.123456'".into()))];
        let schema = build_target_arrow_schema(&fields).unwrap();
        let defaults = FormatTableDefaults::new(&fields, &schema).unwrap();
        let input = RecordBatch::try_new(
            schema,
            vec![Arc::new(TimestampMillisecondArray::from(vec![None]))],
        )
        .unwrap();
        let actual = defaults.apply(&input).unwrap();
        assert_eq!(
            actual
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .value(0),
            100
        );
    }

    #[test]
    fn intermediate_timestamp_precisions_follow_java() {
        use crate::spec::TimestampType;

        for (precision, expected) in [
            (1, 100_i64),
            (2, 120),
            (4, 123_400),
            (5, 123_450),
            (7, 123_456_700),
        ] {
            let field = DataField::new(
                0,
                "time".into(),
                DataType::Timestamp(TimestampType::new(precision).unwrap()),
            );
            let schema = build_target_arrow_schema(std::slice::from_ref(&field)).unwrap();
            let value = cast_default(
                field.data_type(),
                "1970-01-01 00:00:00.123456789",
                schema.field(0),
            )
            .unwrap();
            let actual = match precision {
                1 | 2 => value
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap()
                    .value(0),
                4 | 5 => value
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap()
                    .value(0),
                _ => value
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .value(0),
            };
            assert_eq!(actual, expected, "precision {precision}");
        }
    }

    #[test]
    fn nested_array_and_quoted_null_defaults_follow_java() {
        use crate::spec::{ArrayType, IntType};
        use arrow_array::{Int32Array, ListArray};

        let nested = DataField::new(
            0,
            "nested".into(),
            DataType::Array(ArrayType::new(DataType::Array(ArrayType::new(
                DataType::Int(IntType::new()),
            )))),
        )
        .with_default_value(Some("'[[1, 2], ARRAY(3, null)]'".into()));
        let labels = DataField::new(
            1,
            "labels".into(),
            DataType::Array(ArrayType::new(
                DataType::VarChar(VarCharType::string_type()),
            )),
        )
        .with_default_value(Some("'[null, \"null\", \"a,b\"]'".into()));
        let fields = [nested, labels];
        let schema = build_target_arrow_schema(&fields).unwrap();
        let defaults = FormatTableDefaults::new(&fields, &schema).unwrap();
        let nested = defaults.values[0]
            .as_ref()
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let values = nested.value(0);
        let values = values.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(
            values
                .value(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(1), Some(2)]
        );
        assert_eq!(
            values
                .value(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(3), None]
        );
        let labels = defaults.values[1]
            .as_ref()
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let values = labels.value(0);
        let values = values.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![None, Some("null"), Some("a,b")]
        );
    }

    #[test]
    fn map_and_row_defaults_use_java_token_parsing() {
        use crate::spec::{ArrayType, IntType, MapType, RowType};
        use arrow_array::{Int32Array, MapArray, StructArray};

        let fields = [
            DataField::new(
                0,
                "lookup".into(),
                DataType::Map(MapType::new(
                    DataType::VarChar(VarCharType::string_type()),
                    DataType::Int(IntType::new()),
                )),
            )
            .with_default_value(Some("'{\"a,b\" -> 1, c -> null}'".into())),
            DataField::new(
                1,
                "record".into(),
                DataType::Row(RowType::new(vec![
                    DataField::new(
                        10,
                        "name".into(),
                        DataType::VarChar(VarCharType::string_type()),
                    ),
                    DataField::new(
                        11,
                        "items".into(),
                        DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
                    ),
                ])),
            )
            .with_default_value(Some("'STRUCT(\"a,b\", [1,2])'".into())),
        ];
        let schema = build_target_arrow_schema(&fields).unwrap();
        let defaults = FormatTableDefaults::new(&fields, &schema).unwrap();
        let input = RecordBatch::try_new(
            schema.clone(),
            schema
                .fields()
                .iter()
                .map(|field| arrow_array::new_null_array(field.data_type(), 1))
                .collect(),
        )
        .unwrap();
        let actual = defaults.apply(&input).unwrap();
        let map = actual
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap();
        assert_eq!(
            map.keys()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some("a,b"), Some("c")]
        );
        assert_eq!(
            map.values()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(1), None]
        );
        let row = actual
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(
            row.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "a,b"
        );
        let items = row.column(1).as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(
            items
                .value(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(1), Some(2)]
        );

        let value =
            cast_default(fields[0].data_type(), "MAP(a, 1, a, 2)", schema.field(0)).unwrap();
        let map = value.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map.keys().len(), 1);
        assert_eq!(
            map.values()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            2
        );
    }

    #[test]
    fn malformed_complex_default_fails_before_writing() {
        use crate::spec::{ArrayType, IntType};
        let fields = [DataField::new(
            0,
            "numbers".into(),
            DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
        )
        .with_default_value(Some("'[1, bad]'".into()))];
        let schema = build_target_arrow_schema(&fields).unwrap();
        assert!(FormatTableDefaults::new(&fields, &schema).is_err());
    }
}
