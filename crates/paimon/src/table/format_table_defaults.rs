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
    Array, ArrayRef, Int64Array, ListArray, MapArray, RecordBatch, StringArray, StructArray,
    Time32MillisecondArray,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{
    ArrowError, DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};

use crate::arrow::CastExecutor;
use crate::spec::{DataField, DataType, VarCharType};
use crate::{Error, Result};

#[derive(Clone, Copy)]
enum DefaultParsing {
    Stored,
    NewDefinition,
}

impl DefaultParsing {
    fn trim(self, text: &str) -> &str {
        match self {
            Self::Stored => text.trim(),
            Self::NewDefinition => text.trim_matches(|ch| ch <= ' '),
        }
    }

    fn is_token_whitespace(self, ch: char) -> bool {
        match self {
            Self::Stored => ch.is_whitespace(),
            // Java TokenSplitter uses Character.isWhitespace, which preserves nonbreaking spaces.
            Self::NewDefinition => {
                matches!(ch, '\u{001c}'..='\u{001f}')
                    || ch.is_whitespace()
                        && !matches!(ch, '\u{0085}' | '\u{00a0}' | '\u{2007}' | '\u{202f}')
            }
        }
    }
}

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
                        parse_default(field, text, schema.field(index), DefaultParsing::Stored)
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

pub(crate) fn validate_default_value(field: &DataField, is_format_table: bool) -> Result<()> {
    let Some(text) = field.default_value() else {
        return Ok(());
    };
    let arrow_field = crate::arrow::paimon_type_to_arrow_field(field.name(), field.data_type())?;
    let java_value = parse_default(field, text, &arrow_field, DefaultParsing::NewDefinition)?;
    if is_format_table {
        let writer_value = parse_default(field, text, &arrow_field, DefaultParsing::Stored)?;
        if java_value.to_data() != writer_value.to_data() {
            return Err(Error::DataInvalid {
                message: format!(
                    "Unsupported default value '{text}' for column '{}' of type {}: Java and Rust Format Table writers interpret it differently",
                    field.name(),
                    field.data_type()
                ),
                source: None,
            });
        }
    }
    Ok(())
}

fn parse_default(
    field: &DataField,
    text: &str,
    arrow_field: &ArrowField,
    parsing: DefaultParsing,
) -> Result<ArrayRef> {
    let data_type = field.data_type();
    let invalid = |source| Error::DataInvalid {
        message: format!(
            "Unsupported default value '{text}' for column '{}' of type {data_type}",
            field.name()
        ),
        source,
    };
    if matches!(parsing, DefaultParsing::NewDefinition)
        && (text == "'" || !supports_string_default(data_type))
    {
        return Err(invalid(None));
    }
    // Java's DefaultValueUtils strips one pair of outer single quotes before casting.
    let text = text
        .strip_prefix('\'')
        .and_then(|text| text.strip_suffix('\''))
        .unwrap_or(text);
    let value = cast_default(data_type, text, arrow_field, parsing)
        .map_err(|error| invalid(Some(Box::new(error))))?;
    if value.is_null(0) {
        return Err(invalid(None));
    }
    Ok(value)
}

fn supports_string_default(data_type: &DataType) -> bool {
    match data_type {
        DataType::Array(array) => supports_string_default(array.element_type()),
        DataType::Map(map) => {
            supports_string_default(map.key_type()) && supports_string_default(map.value_type())
        }
        DataType::Row(row) => row
            .fields()
            .iter()
            .all(|field| supports_string_default(field.data_type())),
        DataType::Time(_) => true,
        _ => CastExecutor::supports(&DataType::VarChar(VarCharType::string_type()), data_type),
    }
}

fn cast_default(
    data_type: &DataType,
    text: &str,
    arrow_field: &ArrowField,
    parsing: DefaultParsing,
) -> std::result::Result<ArrayRef, ArrowError> {
    match data_type {
        DataType::Array(typ) => {
            let ArrowDataType::List(element_field) = arrow_field.data_type() else {
                return Err(cast_error("ARRAY default has a non-list Arrow field"));
            };
            let content = literal_content(text, '[', ']', "ARRAY", parsing)?;
            let elements = split_tokens(content, parsing)
                .iter()
                .map(|token| cast_token(typ.element_type(), token, element_field, parsing))
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
            let content = literal_content(text, '{', '}', "STRUCT", parsing)?;
            let tokens = split_tokens(content, parsing);
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
                    Some(token) => {
                        cast_token(field.data_type(), token, &arrow_fields[index], parsing)
                    }
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
            let content = literal_content(text, '{', '}', "MAP", parsing)?;
            let pairs: Vec<(DefaultToken, DefaultToken)> = if parsing.trim(text).starts_with('{') {
                split_raw(content, ",", 0, parsing)
                    .into_iter()
                    .filter(|entry| !entry.is_empty())
                    .map(|entry| {
                        let pair = split_raw(&entry, "->", 2, parsing);
                        if pair.len() != 2 {
                            return Err(cast_error(format!("Invalid MAP entry: {entry}")));
                        }
                        Ok((
                            single_token(&pair[0], parsing)?,
                            single_token(&pair[1], parsing)?,
                        ))
                    })
                    .collect::<std::result::Result<Vec<_>, _>>()?
            } else {
                let tokens = split_tokens(content, parsing);
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
                let key = cast_token(typ.key_type(), key_token, &entry_fields[0], parsing)?;
                let value = cast_token(typ.value_type(), value_token, &entry_fields[1], parsing)?;
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
        _ => cast_scalar_default(data_type, text, arrow_field.data_type(), parsing),
    }
}

fn cast_scalar_default(
    data_type: &DataType,
    text: &str,
    arrow_type: &ArrowDataType,
    parsing: DefaultParsing,
) -> std::result::Result<ArrayRef, ArrowError> {
    if matches!(parsing, DefaultParsing::Stored) {
        // Schema files carry no parser version. Preserve the original result
        // when available and use the Java parser when legacy parsing fails.
        let legacy = match data_type {
            DataType::Timestamp(typ) => cast_legacy_timestamp(text, typ.precision(), arrow_type),
            DataType::LocalZonedTimestamp(typ) => {
                cast_legacy_local_zoned_timestamp(text, typ.precision(), arrow_type)
            }
            DataType::Date(_) | DataType::Time(_) if is_unsigned_integer(text) => {
                return cast_java_scalar_default(data_type, text);
            }
            DataType::Boolean(_)
            | DataType::Decimal(_)
            | DataType::Float(_)
            | DataType::Double(_)
            | DataType::Date(_)
            | DataType::Time(_)
            | DataType::Geometry(_)
            | DataType::Geography(_) => {
                arrow_cast::cast(&StringArray::from(vec![text]), arrow_type)
            }
            _ => return cast_java_scalar_default(data_type, text),
        };
        if let Some(value) = legacy.ok().filter(|value| !value.is_null(0)) {
            return Ok(value);
        }
    }
    cast_java_scalar_default(data_type, text)
}

fn cast_legacy_timestamp(
    text: &str,
    precision: u32,
    arrow_type: &ArrowDataType,
) -> std::result::Result<ArrayRef, ArrowError> {
    if is_unsigned_integer(text) {
        if !precision.is_multiple_of(3) {
            return Err(cast_error(format!(
                "Invalid numeric TIMESTAMP({precision}) default"
            )));
        }
        let ticks = text
            .parse::<i64>()
            .map_err(|error| cast_error(error.to_string()))?;
        return arrow_cast::cast(&Int64Array::from(vec![ticks]), arrow_type);
    }
    let value = arrow_cast::cast(&StringArray::from(vec![text]), arrow_type)?;
    let divisor = 10_i64.pow(precision.div_ceil(3) * 3 - precision);
    if value.is_null(0) || divisor == 1 {
        return Ok(value);
    }
    let ticks = arrow_cast::cast(value.as_ref(), &ArrowDataType::Int64)?;
    let ticks = ticks
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    let truncated = ticks
        .div_euclid(divisor)
        .checked_mul(divisor)
        .ok_or_else(|| cast_error("TIMESTAMP default is outside the Arrow storage range"))?;
    arrow_cast::cast(&Int64Array::from(vec![truncated]), arrow_type)
}

fn cast_legacy_local_zoned_timestamp(
    text: &str,
    precision: u32,
    arrow_type: &ArrowDataType,
) -> std::result::Result<ArrayRef, ArrowError> {
    let datetime = arrow_cast::parse::string_to_datetime(&chrono::Local, text)?;
    let stored_precision = precision.div_ceil(3) * 3;
    let fraction = if precision == 0 {
        0
    } else {
        datetime.timestamp_subsec_nanos() / 10_u32.pow(9 - precision)
            * 10_u32.pow(stored_precision - precision)
    };
    let ticks =
        i128::from(datetime.timestamp()) * 10_i128.pow(stored_precision) + i128::from(fraction);
    let ticks = i64::try_from(ticks).map_err(|error| cast_error(error.to_string()))?;
    arrow_cast::cast(&Int64Array::from(vec![ticks]), arrow_type)
}

fn is_unsigned_integer(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit())
}

fn cast_java_scalar_default(
    data_type: &DataType,
    text: &str,
) -> std::result::Result<ArrayRef, ArrowError> {
    if matches!(data_type, DataType::Time(_)) {
        let millis = crate::arrow::parse_java_time(text)
            .map_err(|error| ArrowError::ExternalError(Box::new(error)))?;
        return Ok(Arc::new(Time32MillisecondArray::from(vec![millis])));
    }
    let string = DataType::VarChar(VarCharType::string_type());
    let input: ArrayRef = Arc::new(StringArray::from(vec![text]));
    CastExecutor::new(string.clone(), string, data_type.clone())
        .and_then(|cast| cast.cast(&input))
        .map_err(|error| ArrowError::ExternalError(Box::new(error)))
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
    parsing: DefaultParsing,
) -> std::result::Result<ArrayRef, ArrowError> {
    if !token.literal && token.value == "null" {
        return Ok(arrow_array::new_null_array(arrow_field.data_type(), 1));
    }
    let value = cast_default(data_type, &token.value, arrow_field, parsing)?;
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
    parsing: DefaultParsing,
) -> std::result::Result<&'a str, ArrowError> {
    let text = parsing.trim(text);
    let content = text
        .strip_prefix(open)
        .and_then(|body| body.strip_suffix(close))
        .or_else(|| {
            let (name, tail) = text.split_at_checked(function.len())?;
            if !name.eq_ignore_ascii_case(function) {
                return None;
            }
            let tail = match parsing {
                DefaultParsing::Stored => tail.trim_start(),
                DefaultParsing::NewDefinition => {
                    tail.trim_start_matches(|ch| matches!(ch, '\t'..='\r' | ' '))
                }
            };
            tail.strip_prefix('(')?.strip_suffix(')')
        })
        // Java's collection cast rules match the body with a non-DOTALL regex.
        .filter(|body| {
            matches!(parsing, DefaultParsing::Stored)
                || !body.contains(['\n', '\r', '\u{0085}', '\u{2028}', '\u{2029}'])
        })
        .ok_or_else(|| cast_error(format!("Invalid {function} default: {text}")))?;
    Ok(parsing.trim(content))
}

fn single_token(
    text: &str,
    parsing: DefaultParsing,
) -> std::result::Result<DefaultToken, ArrowError> {
    let tokens = split_tokens(text, parsing);
    if tokens.len() != 1 {
        return Err(cast_error(format!("Invalid MAP entry token: {text}")));
    }
    Ok(tokens.into_iter().next().unwrap())
}

// Port of Java TokenSplitter.split. At the current level quotes and escapes
// make a token literal; nested punctuation is retained for recursive casting.
fn split_tokens(content: &str, parsing: DefaultParsing) -> Vec<DefaultToken> {
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
        let whitespace = parsing.is_token_whitespace(ch);
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
            } else if whitespace && end == 0 {
                continue;
            }
        }
        current.push(ch);
        if in_quotes || !whitespace {
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
fn split_raw(content: &str, delimiter: &str, limit: usize, parsing: DefaultParsing) -> Vec<String> {
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
                tokens.push(parsing.trim(&current).to_string());
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
    tokens.push(parsing.trim(&current).to_string());
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
    use arrow_array::{
        BinaryArray, Date32Array, Int64Array, StringArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
    };

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
    fn converts_java_quoted_temporal_and_numeric_defaults() {
        use crate::spec::{BooleanType, DateType, DecimalType, TimeType};
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
            DataField::new(3, "clock".into(), DataType::Time(TimeType::new(3).unwrap()))
                .with_default_value(Some("'00:01:02.9995+01:30'".into())),
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
                Arc::new(Time32MillisecondArray::from(vec![None])),
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
        let clock = actual
            .column(3)
            .as_any()
            .downcast_ref::<Time32MillisecondArray>()
            .unwrap();
        assert_eq!(clock.value(0), 5_463_000);
        for (literal, stored_millis, java_millis) in [
            ("'1:20 PM'", 48_000_000, None),
            ("'00:00:00.0005'", 0, Some(1)),
            ("'00:00:00.001'", 1, Some(1)),
        ] {
            let field = fields[3].clone().with_default_value(Some(literal.into()));
            assert_eq!(
                validate_default_value(&field, false).is_ok(),
                java_millis.is_some(),
                "{literal}"
            );
            assert_eq!(
                validate_default_value(&field, true).is_ok(),
                java_millis == Some(stored_millis),
                "{literal}"
            );
            for (parsing, expected) in [
                (DefaultParsing::Stored, Some(stored_millis)),
                (DefaultParsing::NewDefinition, java_millis),
            ] {
                let millis = parse_default(&field, literal, input.schema_ref().field(3), parsing)
                    .ok()
                    .map(|value| {
                        value
                            .as_any()
                            .downcast_ref::<Time32MillisecondArray>()
                            .unwrap()
                            .value(0)
                    });
                assert_eq!(millis, expected, "{literal}");
            }
        }
        assert_eq!(input.column(0).null_count(), 1);
    }

    #[test]
    fn character_and_binary_defaults_preserve_widths_and_quotes() {
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

        let apostrophe = fields[1].clone().with_default_value(Some("'".into()));
        assert!(validate_default_value(&apostrophe, true).is_err());
        let stored = parse_default(
            &apostrophe,
            "'",
            actual.schema().field(1),
            DefaultParsing::Stored,
        )
        .unwrap();
        assert_eq!(
            stored
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "'"
        );
    }

    #[test]
    fn numeric_temporal_defaults_use_java_internal_units() {
        use crate::spec::{DateType, TimeType, TimestampType};
        let fields = vec![
            DataField::new(0, "day".into(), DataType::Date(DateType::new()))
                .with_default_value(Some("-42".into())),
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
            .with_default_value(Some("-123456".into())),
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
            -42
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
            -123456
        );
    }

    #[test]
    fn schema_validation_rejects_blob_and_geospatial_defaults() {
        use crate::spec::{ArrayType, BlobType, GeographyType, GeometryType, MapType, RowType};
        let geometry = DataType::Geometry(GeometryType::default());
        let string = DataType::VarChar(VarCharType::string_type());
        for (data_type, default) in [
            (DataType::Blob(BlobType::new()), "'raw bytes'"),
            (geometry.clone(), "'raw bytes'"),
            (DataType::Geography(GeographyType::default()), "'raw bytes'"),
            (DataType::Array(ArrayType::new(geometry.clone())), "'[]'"),
            (
                DataType::Map(MapType::new(geometry.clone(), string.clone())),
                "'{}'",
            ),
            (
                DataType::Map(MapType::new(string, geometry.clone())),
                "'{a -> null}'",
            ),
            (
                DataType::Row(RowType::new(vec![DataField::new(1, "g".into(), geometry)])),
                "'STRUCT(null)'",
            ),
        ] {
            let field = DataField::new(0, "value".into(), data_type)
                .with_default_value(Some(default.into()));
            assert!(
                validate_default_value(&field, true).is_err(),
                "{}",
                field.data_type()
            );
        }
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
                DefaultParsing::Stored,
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

        let value = cast_default(
            fields[0].data_type(),
            "MAP(a, 1, a, 2)",
            schema.field(0),
            DefaultParsing::NewDefinition,
        )
        .unwrap();
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
    fn collection_defaults_validate_java_syntax_and_preserve_stored_values() {
        use crate::spec::{ArrayType, IntType, MapType, RowType};

        let int = DataType::Int(IntType::new());
        let string = DataType::VarChar(VarCharType::string_type());
        let numbers = DataType::Array(ArrayType::new(int.clone()));
        let labels = DataType::Array(ArrayType::new(string.clone()));
        let lookup = DataType::Map(MapType::new(string, int.clone()));
        let row = DataType::Row(RowType::new(vec![DataField::new(1, "id".into(), int)]));
        for (data_type, literal, writable, valid_in_java, valid_for_format_table) in [
            (&numbers, "'[1, bad]'", false, false, false),
            (&numbers, "'[1,\n2]'", true, false, false),
            (&lookup, "'MAP(a,\n1)'", true, false, false),
            (&row, "'STRUCT(\n1)'", true, false, false),
            (&numbers, "'ARRAY\n(1, 2)'", true, true, true),
            (&labels, "'[a, \u{00a0}b]'", true, true, false),
            (&lookup, "'{a -> 1, \u{00a0}b -> 2}'", true, true, false),
        ] {
            let fields = [DataField::new(0, "value".into(), data_type.clone())
                .with_default_value(Some(literal.into()))];
            let schema = build_target_arrow_schema(&fields).unwrap();
            assert_eq!(
                FormatTableDefaults::new(&fields, &schema).is_ok(),
                writable,
                "{literal:?}"
            );
            assert_eq!(
                validate_default_value(&fields[0], false).is_ok(),
                valid_in_java,
                "{literal:?}"
            );
            assert_eq!(
                validate_default_value(&fields[0], true).is_ok(),
                valid_for_format_table,
                "{literal:?}"
            );
        }
    }
}
