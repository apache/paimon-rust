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

//! Arrow execution of Java's character and position transforms.

use std::collections::HashMap;
use std::sync::Arc;

use crate::spec::{PadDirection, Transform, TransformInput, TrimFlag};
use crate::Result;
use arrow_array::{cast::AsArray, Array, ArrayRef, Int32Array, Int64Array, StringArray};
use arrow_schema::DataType as ArrowType;

enum Input<'a> {
    Text(Option<&'a str>),
    Number(Option<i64>),
    IntegerText(&'a str),
    Strings(StringArray),
    Integers(Int64Array),
}

impl Input<'_> {
    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Text(value) => value.is_none(),
            Self::Number(value) => value.is_none(),
            Self::IntegerText(_) => false,
            Self::Strings(values) => values.is_null(row),
            Self::Integers(values) => values.is_null(row),
        }
    }
    fn text(&self, row: usize) -> Result<Option<&str>> {
        match self {
            Self::Text(value) => Ok(*value),
            Self::Strings(values) => Ok(values.is_valid(row).then(|| values.value(row))),
            _ => Err(invalid("string transform requires a character input")),
        }
    }

    fn integer(&self, row: usize) -> Result<Option<i32>> {
        let value = match self {
            Self::IntegerText(text) => return parse_integer(text).map(Some),
            Self::Number(value) => *value,
            Self::Integers(values) => values.is_valid(row).then(|| values.value(row)),
            _ => return Err(invalid("position transform requires an integer input")),
        };
        value
            .map(|v| {
                i32::try_from(v).map_err(|_| invalid("transform position exceeds Java INT range"))
            })
            .transpose()
    }
}

pub(crate) fn evaluate_string_transform(
    transform: &Transform,
    rows: usize,
    column: &dyn Fn(usize) -> Result<ArrayRef>,
) -> Result<ArrayRef> {
    let inputs = transform
        .inputs()
        .iter()
        .map(|input| match input {
            TransformInput::Literal(value) => Ok(Input::Text(value.as_deref())),
            TransformInput::Integer(value) => Ok(Input::Number(*value)),
            TransformInput::IntegerText(text) => Ok(Input::IntegerText(text)),
            TransformInput::Field(index) => {
                let array = column(*index)?;
                match array.data_type() {
                    ArrowType::Utf8 => Ok(Input::Strings(array.as_string::<i32>().clone())),
                    ArrowType::Int8 | ArrowType::Int16 | ArrowType::Int32 | ArrowType::Int64 => {
                        let array = arrow_cast::cast(&array, &ArrowType::Int64)
                            .map_err(|e| invalid(&e.to_string()))?;
                        Ok(Input::Integers(
                            array
                                .as_primitive::<arrow_array::types::Int64Type>()
                                .clone(),
                        ))
                    }
                    other => Err(invalid(&format!("invalid transform input type {other:?}"))),
                }
            }
        })
        .collect::<Result<Vec<_>>>()?;
    if matches!(transform, Transform::Length(_) | Transform::BitLength(_)) {
        let values = (0..rows)
            .map(|row| {
                Ok(inputs[0].text(row)?.map(|text| {
                    if matches!(transform, Transform::Length(_)) {
                        text.chars().count() as i32
                    } else {
                        (text.len() as i32).wrapping_mul(8)
                    }
                }))
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok(Arc::new(Int32Array::from(values)));
    }
    let values = (0..rows)
        .map(|row| evaluate_row(transform, &inputs, row))
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(StringArray::from(values)))
}

fn evaluate_row(transform: &Transform, inputs: &[Input<'_>], row: usize) -> Result<Option<String>> {
    let source = inputs[0].text(row)?;
    let value = match transform {
        Transform::Substring(_) => {
            // SubstringTransform checks source and null positions before
            // evaluating positions; a hidden NULL source cannot raise overflow.
            let Some(source) = source else {
                return Ok(None);
            };
            if inputs[1..].iter().any(|input| input.is_null(row)) {
                return Ok(None);
            }
            let Some(position) = inputs[1].integer(row)? else {
                return Ok(None);
            };
            let length = if inputs.len() == 3 {
                let Some(length) = inputs[2].integer(row)? else {
                    return Ok(None);
                };
                length
            } else {
                i32::MAX
            };
            substring_sql(source, position, length).to_owned()
        }
        Transform::Trim(_, flag) => {
            let Some(source) = source else {
                return Ok(None);
            };
            let chars = if inputs.len() == 2 {
                let Some(chars) = inputs[1].text(row)? else {
                    return Ok(None);
                };
                chars
            } else {
                " "
            };
            let contains = |c| chars.contains(c);
            match flag {
                TrimFlag::Leading => source.trim_start_matches(contains),
                TrimFlag::Trailing => source.trim_end_matches(contains),
                TrimFlag::Both => source.trim_matches(contains),
            }
            .to_owned()
        }
        Transform::Pad(_, direction) => {
            let length = inputs[1].integer(row)?;
            let padding = inputs[2].text(row)?;
            let (Some(source), Some(length), Some(padding)) = (source, length, padding) else {
                return Ok(None);
            };
            let count = source.chars().count() as i32;
            let needed = length.wrapping_sub(count);
            if needed <= 0 || padding.is_empty() {
                substring(source, 0, length).to_owned()
            } else {
                let pad_chars = padding.chars().count();
                let repeats = needed as usize / pad_chars;
                let tail = substring(padding, 0, needed % pad_chars as i32);
                let size = padding
                    .len()
                    .checked_mul(repeats)
                    .and_then(|n| n.checked_add(tail.len()))
                    .and_then(|n| n.checked_add(source.len()))
                    .filter(|n| *n <= i32::MAX as usize)
                    .ok_or_else(|| invalid("PAD result exceeds Arrow UTF-8 storage"))?;
                let mut result = String::new();
                result
                    .try_reserve(size)
                    .map_err(|e| invalid(&format!("cannot reserve PAD result: {e}")))?;
                if *direction == PadDirection::Right {
                    result.push_str(source);
                }
                for _ in 0..repeats {
                    result.push_str(padding);
                }
                result.push_str(tail);
                if *direction == PadDirection::Left {
                    result.push_str(source);
                }
                result
            }
        }
        Transform::Translate(_) => {
            let matching = inputs[1].text(row)?;
            let replacement = inputs[2].text(row)?;
            let (Some(source), Some(matching), Some(replacement)) = (source, matching, replacement)
            else {
                return Ok(None);
            };
            let replacements: Vec<char> = replacement.chars().collect();
            let mut mapping = HashMap::new();
            for (index, ch) in matching.chars().enumerate() {
                mapping
                    .entry(ch)
                    .or_insert(replacements.get(index).copied().filter(|c| *c != '\0'));
            }
            source
                .chars()
                .filter_map(|ch| mapping.get(&ch).copied().unwrap_or(Some(ch)))
                .collect()
        }
        Transform::Overlay(_) => {
            let replacement = inputs[1].text(row)?;
            let position = inputs[2].integer(row)?;
            let length = if inputs.len() == 4 {
                inputs[3].integer(row)?
            } else {
                None
            };
            let (Some(source), Some(replacement), Some(position)) = (source, replacement, position)
            else {
                return Ok(None);
            };
            if inputs.len() == 4 && length.is_none() {
                return Ok(None);
            }
            let replaced = length
                .filter(|length| *length >= 0)
                .unwrap_or_else(|| replacement.chars().count() as i32);
            format!(
                "{}{}{}",
                substring_sql(source, 1, position.wrapping_sub(1)),
                replacement,
                substring_sql(source, position.wrapping_add(replaced), i32::MAX)
            )
        }
        _ => return Err(invalid("unsupported character transform")),
    };
    Ok(Some(value))
}

fn substring_sql(source: &str, position: i32, length: i32) -> &str {
    let count = source.chars().count() as i32;
    let start = if position > 0 {
        position - 1
    } else if position < 0 {
        count + position
    } else {
        0
    };
    let end = (i64::from(start) + i64::from(length)).clamp(i64::from(i32::MIN), i64::from(i32::MAX))
        as i32;
    substring(source, start, end)
}

fn substring(source: &str, start: i32, end: i32) -> &str {
    if end <= start || end <= 0 {
        return "";
    }
    let start = source
        .char_indices()
        .nth(start.max(0) as usize)
        .map_or(source.len(), |(index, _)| index);
    let end = source
        .char_indices()
        .nth(end as usize)
        .map_or(source.len(), |(index, _)| index);
    &source[start..end.max(start)]
}

fn invalid(message: &str) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.to_owned(),
        source: None,
    }
}

/// Integer.parseInt uses Character.digit on UTF-16 chars: decimal digits in
/// BMP scripts are accepted, supplementary digits and whitespace are rejected.
fn parse_integer(text: &str) -> Result<i32> {
    let (negative, digits) = match text.strip_prefix('-') {
        Some(digits) => (true, digits),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    if digits.is_empty() {
        return Err(invalid("empty transform position"));
    }
    // Accumulate negatively, so i32::MIN remains representable.
    let mut value = 0i32;
    for ch in digits.chars() {
        let digit = crate::common::number::java_decimal_digit(ch)
            .map(i32::from)
            .ok_or_else(|| invalid("invalid Java integer position"))?;
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_sub(digit))
            .ok_or_else(|| invalid("transform position exceeds Java INT range"))?;
    }
    if negative {
        Ok(value)
    } else {
        value
            .checked_neg()
            .ok_or_else(|| invalid("transform position exceeds Java INT range"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{BigIntType, DataField, DataType, VarCharType};

    fn apply(rule: &str, source: Vec<Option<&str>>) -> Result<ArrayRef> {
        let fields = vec![DataField::new(
            0,
            "s".into(),
            DataType::VarChar(VarCharType::string_type()),
        )];
        let transform = Transform::from_rest_json(rule, &fields)?;
        let rows = source.len();
        let source = Arc::new(StringArray::from(source)) as ArrayRef;
        evaluate_string_transform(&transform, rows, &|_| Ok(source.clone()))
    }

    #[test]
    fn substring_positions_are_parsed_after_null_checks() {
        for position in [
            r#""bad""#,
            r#""999999999999999999999""#,
            "18446744073709551617",
        ] {
            let rule = format!(r#"{{"name":"SUBSTRING","inputs":[{{"name":"s"}}, {position}]}}"#);
            assert!(apply(&rule, vec![None]).unwrap().is_null(0));
            assert!(apply(&rule, vec![Some("abc")]).is_err());
            let rule =
                format!(r#"{{"name":"SUBSTRING","inputs":[{{"name":"s"}}, {position}, null]}}"#);
            assert!(apply(&rule, vec![Some("abc")]).unwrap().is_null(0));
        }
        let result = apply(
            r#"{"name":"SUBSTRING","inputs":[{"name":"s"},"+٢",1]}"#,
            vec![Some("abc")],
        )
        .unwrap();
        assert_eq!(result.as_string::<i32>().value(0), "b");
        for position in ["1.5", "1e2", "true"] {
            assert!(apply(
                &format!(r#"{{"name":"SUBSTRING","inputs":[null,{position}]}}"#),
                vec![None]
            )
            .is_err());
        }
    }

    #[test]
    fn integer_syntax_and_range_match_java() {
        for (text, value) in [
            ("-2147483648", i32::MIN),
            ("+2147483647", i32::MAX),
            ("٠٠٢", 2),
            ("２", 2),
        ] {
            assert_eq!(parse_integer(text).unwrap(), value);
        }
        for text in ["", "+", " 2", "2 ", "1_0", "2147483648", "-2147483649", "𝟚"] {
            assert!(parse_integer(text).is_err(), "{text}");
        }
    }

    #[test]
    fn pad_and_overlay_convert_integers_before_null_strings() {
        for rule in [
            r#"{"name":"PAD","direction":"LEFT","inputs":[null,2147483648,"x"]}"#,
            r#"{"name":"OVERLAY","inputs":[null,"x",2147483648,null]}"#,
            r#"{"name":"OVERLAY","inputs":[null,"x",null,2147483648]}"#,
        ] {
            assert!(apply(rule, vec![None]).is_err());
        }
    }

    #[test]
    fn mixed_fields_are_bound_by_name_and_checked_before_execution() {
        let fields = vec![
            DataField::new(0, "s".into(), DataType::VarChar(VarCharType::string_type())),
            DataField::new(1, "n".into(), DataType::BigInt(BigIntType::new())),
        ];
        for rule in [
            r#"{"name":"PAD","direction":"RIGHT","inputs":[{"name":"s","type":"INT"},3,"x"]}"#,
            r#"{"name":"PAD","direction":"RIGHT","inputs":["a",{"name":"n","type":"STRING"},"x"]}"#,
            r#"{"name":"OVERLAY","inputs":["a","x",{"name":"n","type":"STRING"}]}"#,
            r#"{"name":"TRIM","trimFlag":"BOTH","inputs":[{"name":"s","type":"INT"}]}"#,
            r#"{"name":"SUBSTRING","inputs":["a",{"name":"s","type":"INT"}]}"#,
        ] {
            assert!(Transform::from_rest_json(rule, &fields).is_err(), "{rule}");
        }
        let transform = Transform::from_rest_json(r#"{"name":"SUBSTRING","inputs":[{"index":99,"name":"s","type":"INT"},{"index":99,"name":"n","type":"STRING"},1]}"#, &fields).unwrap();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![Some("甲🙂乙"), None])),
            Arc::new(Int64Array::from(vec![2, i64::MAX])),
        ];
        let result =
            evaluate_string_transform(&transform, 2, &|index| Ok(columns[index].clone())).unwrap();
        assert_eq!(
            result.as_string::<i32>().iter().collect::<Vec<_>>(),
            vec![Some("🙂"), None]
        );
    }

    #[test]
    fn lengths_count_unicode_characters_and_utf8_bits_as_int() {
        for (name, expected) in [
            ("LENGTH", vec![Some(3), Some(2), Some(0), None]),
            ("BIT_LENGTH", vec![Some(80), Some(24), Some(0), None]),
        ] {
            let result = apply(
                &format!(r#"{{"name":"{name}","inputs":[{{"name":"s","type":"STRING"}}]}}"#),
                vec![Some("甲🙂乙"), Some("e\u{301}"), Some(""), None],
            )
            .unwrap();
            assert_eq!(
                result
                    .as_primitive::<arrow_array::types::Int32Type>()
                    .iter()
                    .collect::<Vec<_>>(),
                expected
            );
        }
    }
}
