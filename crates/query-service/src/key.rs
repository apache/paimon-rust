// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeSet;

use arrow_array::{
    Array, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray,
    LargeStringArray, StringArray, StringViewArray, Time32MillisecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
};
use base64::Engine;
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use paimon::spec::{DataField, DataType, Datum, Predicate, PredicateBuilder};
use serde_json::Value;

use crate::error::{LookupError, Result};
use crate::model::LookupKey;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum KeyComponent {
    Bool(bool),
    TinyInt(i8),
    SmallInt(i16),
    Int(i32),
    Long(i64),
    Float(u32),
    Double(u64),
    String(String),
    Bytes(Vec<u8>),
    Date(i32),
    Time(i32),
    Timestamp {
        millis: i64,
        nanos: i32,
    },
    LocalZonedTimestamp {
        millis: i64,
        nanos: i32,
    },
    Decimal {
        unscaled: i128,
        precision: u32,
        scale: u32,
    },
}

impl KeyComponent {
    fn datum(&self) -> Datum {
        match self {
            Self::Bool(value) => Datum::Bool(*value),
            Self::TinyInt(value) => Datum::TinyInt(*value),
            Self::SmallInt(value) => Datum::SmallInt(*value),
            Self::Int(value) => Datum::Int(*value),
            Self::Long(value) => Datum::Long(*value),
            Self::Float(bits) => Datum::Float(f32::from_bits(*bits)),
            Self::Double(bits) => Datum::Double(f64::from_bits(*bits)),
            Self::String(value) => Datum::String(value.clone()),
            Self::Bytes(value) => Datum::Bytes(value.clone()),
            Self::Date(value) => Datum::Date(*value),
            Self::Time(value) => Datum::Time(*value),
            Self::Timestamp { millis, nanos } => Datum::Timestamp {
                millis: *millis,
                nanos: *nanos,
            },
            Self::LocalZonedTimestamp { millis, nanos } => Datum::LocalZonedTimestamp {
                millis: *millis,
                nanos: *nanos,
            },
            Self::Decimal {
                unscaled,
                precision,
                scale,
            } => Datum::Decimal {
                unscaled: *unscaled,
                precision: *precision,
                scale: *scale,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct NormalizedKey(pub(crate) Vec<KeyComponent>);

#[derive(Debug, Clone)]
pub(crate) struct PreparedKey {
    pub(crate) original: LookupKey,
    pub(crate) normalized: NormalizedKey,
}

pub(crate) fn validate_key_type(field: &str, data_type: &DataType) -> Result<()> {
    if matches!(
        data_type,
        DataType::Boolean(_)
            | DataType::TinyInt(_)
            | DataType::SmallInt(_)
            | DataType::Int(_)
            | DataType::BigInt(_)
            | DataType::Float(_)
            | DataType::Double(_)
            | DataType::Char(_)
            | DataType::VarChar(_)
            | DataType::Binary(_)
            | DataType::VarBinary(_)
            | DataType::Date(_)
            | DataType::Time(_)
            | DataType::Timestamp(_)
            | DataType::LocalZonedTimestamp(_)
            | DataType::Decimal(_)
    ) {
        Ok(())
    } else {
        Err(LookupError::UnsupportedKeyType {
            field: field.to_string(),
            data_type: format!("{data_type:?}"),
        })
    }
}

pub(crate) fn supports_global_btree(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean(_)
            | DataType::TinyInt(_)
            | DataType::SmallInt(_)
            | DataType::Int(_)
            | DataType::BigInt(_)
            | DataType::Float(_)
            | DataType::Double(_)
            | DataType::Char(_)
            | DataType::VarChar(_)
            | DataType::Date(_)
            | DataType::Time(_)
            | DataType::Timestamp(_)
            | DataType::LocalZonedTimestamp(_)
            | DataType::Decimal(_)
    )
}

pub(crate) fn prepare_keys(
    keys: Vec<LookupKey>,
    key_fields: &[String],
    schema_fields: &[DataField],
) -> Result<Vec<PreparedKey>> {
    let expected = key_fields.iter().cloned().collect::<BTreeSet<_>>();
    let field_types = key_fields
        .iter()
        .map(|name| {
            schema_fields
                .iter()
                .find(|field| field.name() == name)
                .map(|field| field.data_type())
                .ok_or_else(|| {
                    LookupError::InvalidPolicy(format!(
                        "lookup key field '{name}' is missing from the table schema"
                    ))
                })
        })
        .collect::<Result<Vec<_>>>()?;

    keys.into_iter()
        .map(|original| {
            let actual = original.keys().cloned().collect::<BTreeSet<_>>();
            if actual != expected {
                return Err(LookupError::InvalidRequest(format!(
                    "lookup key fields must be exactly {:?}, got {:?}",
                    key_fields,
                    original.keys().collect::<Vec<_>>()
                )));
            }
            let components = key_fields
                .iter()
                .zip(&field_types)
                .map(|(name, data_type)| json_to_component(name, &original[name], data_type))
                .collect::<Result<Vec<_>>>()?;
            Ok(PreparedKey {
                original,
                normalized: NormalizedKey(components),
            })
        })
        .collect()
}

pub(crate) fn build_batch_predicate(
    prepared: &[PreparedKey],
    key_fields: &[String],
    schema_fields: &[DataField],
) -> Result<Predicate> {
    if prepared.is_empty() {
        return Ok(Predicate::AlwaysFalse);
    }

    let builder = PredicateBuilder::new(schema_fields);
    if key_fields.len() == 1 {
        let mut seen = std::collections::HashSet::new();
        let values = prepared
            .iter()
            .filter_map(|key| {
                let component = key.normalized.0[0].clone();
                seen.insert(component.clone()).then(|| component.datum())
            })
            .collect();
        return builder
            .is_in(&key_fields[0], values)
            .map_err(LookupError::from);
    }

    let mut seen = std::collections::HashSet::new();
    let disjunction = prepared
        .iter()
        .filter(|key| seen.insert(key.normalized.clone()))
        .map(|key| {
            key_fields
                .iter()
                .zip(&key.normalized.0)
                .map(|(field, value)| builder.equal(field, value.datum()))
                .collect::<paimon::Result<Vec<_>>>()
                .map(Predicate::and)
        })
        .collect::<paimon::Result<Vec<_>>>()?;
    Ok(Predicate::or(disjunction))
}

pub(crate) fn normalized_key_from_batch(
    batch: &arrow_array::RecordBatch,
    row: usize,
    key_fields: &[String],
    schema_fields: &[DataField],
) -> Result<NormalizedKey> {
    let mut components = Vec::with_capacity(key_fields.len());
    for (column_index, name) in key_fields.iter().enumerate() {
        let field = schema_fields
            .iter()
            .find(|field| field.name() == name)
            .ok_or_else(|| {
                LookupError::InvalidPolicy(format!(
                    "lookup key field '{name}' is missing from the table schema"
                ))
            })?;
        components.push(array_to_component(
            name,
            batch.column(column_index).as_ref(),
            row,
            field.data_type(),
        )?);
    }
    Ok(NormalizedKey(components))
}

fn json_to_component(field: &str, value: &Value, data_type: &DataType) -> Result<KeyComponent> {
    match data_type {
        DataType::Boolean(_) => value
            .as_bool()
            .map(KeyComponent::Bool)
            .ok_or_else(|| invalid_value(field, "expected a JSON boolean")),
        DataType::TinyInt(_) => parse_signed(field, value).and_then(|value| {
            i8::try_from(value)
                .map(KeyComponent::TinyInt)
                .map_err(|_| invalid_value(field, "value is outside TINYINT range"))
        }),
        DataType::SmallInt(_) => parse_signed(field, value).and_then(|value| {
            i16::try_from(value)
                .map(KeyComponent::SmallInt)
                .map_err(|_| invalid_value(field, "value is outside SMALLINT range"))
        }),
        DataType::Int(_) => parse_signed(field, value).and_then(|value| {
            i32::try_from(value)
                .map(KeyComponent::Int)
                .map_err(|_| invalid_value(field, "value is outside INT range"))
        }),
        DataType::BigInt(_) => parse_signed(field, value).map(KeyComponent::Long),
        DataType::Float(_) => parse_float(field, value),
        DataType::Double(_) => parse_double(field, value),
        DataType::Char(_) | DataType::VarChar(_) => value
            .as_str()
            .map(|value| KeyComponent::String(value.to_string()))
            .ok_or_else(|| invalid_value(field, "expected a JSON string")),
        DataType::Binary(_) | DataType::VarBinary(_) => {
            let encoded = value
                .as_object()
                .and_then(|object| object.get("base64"))
                .and_then(Value::as_str)
                .ok_or_else(|| invalid_value(field, "expected {\"base64\":\"...\"}"))?;
            base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map(KeyComponent::Bytes)
                .map_err(|error| invalid_value(field, format!("invalid base64: {error}")))
        }
        DataType::Date(_) => parse_date(field, value),
        DataType::Time(data_type) => parse_time(field, value, data_type.precision()),
        DataType::Timestamp(data_type) => {
            parse_timestamp(field, value, data_type.precision(), false)
        }
        DataType::LocalZonedTimestamp(data_type) => {
            parse_timestamp(field, value, data_type.precision(), true)
        }
        DataType::Decimal(data_type) => {
            parse_decimal(field, value, data_type.precision(), data_type.scale())
        }
        other => Err(LookupError::UnsupportedKeyType {
            field: field.to_string(),
            data_type: format!("{other:?}"),
        }),
    }
}

fn array_to_component(
    field: &str,
    array: &dyn Array,
    row: usize,
    data_type: &DataType,
) -> Result<KeyComponent> {
    if array.is_null(row) {
        return Err(LookupError::UnexpectedResult(format!(
            "lookup key field '{field}' was NULL"
        )));
    }

    macro_rules! primitive {
        ($array:ty, $variant:ident) => {
            array
                .as_any()
                .downcast_ref::<$array>()
                .map(|array| KeyComponent::$variant(array.value(row)))
                .ok_or_else(|| arrow_type_mismatch(field, array, data_type))
        };
    }

    match data_type {
        DataType::Boolean(_) => primitive!(BooleanArray, Bool),
        DataType::TinyInt(_) => primitive!(Int8Array, TinyInt),
        DataType::SmallInt(_) => primitive!(Int16Array, SmallInt),
        DataType::Int(_) => primitive!(Int32Array, Int),
        DataType::BigInt(_) => primitive!(Int64Array, Long),
        DataType::Float(_) => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|array| KeyComponent::Float(canonical_f32_bits(array.value(row))))
            .ok_or_else(|| arrow_type_mismatch(field, array, data_type)),
        DataType::Double(_) => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|array| KeyComponent::Double(canonical_f64_bits(array.value(row))))
            .ok_or_else(|| arrow_type_mismatch(field, array, data_type)),
        DataType::Char(_) | DataType::VarChar(_) => {
            string_component(array, row).ok_or_else(|| arrow_type_mismatch(field, array, data_type))
        }
        DataType::Binary(_) | DataType::VarBinary(_) => {
            binary_component(array, row).ok_or_else(|| arrow_type_mismatch(field, array, data_type))
        }
        DataType::Date(_) => primitive!(Date32Array, Date),
        DataType::Time(_) => primitive!(Time32MillisecondArray, Time),
        DataType::Timestamp(timestamp_type) => timestamp_component(
            field,
            array,
            row,
            timestamp_type.precision(),
            false,
            data_type,
        ),
        DataType::LocalZonedTimestamp(timestamp_type) => timestamp_component(
            field,
            array,
            row,
            timestamp_type.precision(),
            true,
            data_type,
        ),
        DataType::Decimal(decimal_type) => array
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .map(|array| KeyComponent::Decimal {
                unscaled: array.value(row),
                precision: decimal_type.precision(),
                scale: decimal_type.scale(),
            })
            .ok_or_else(|| arrow_type_mismatch(field, array, data_type)),
        other => Err(LookupError::UnsupportedKeyType {
            field: field.to_string(),
            data_type: format!("{other:?}"),
        }),
    }
}

fn parse_float(field: &str, value: &Value) -> Result<KeyComponent> {
    let value = match value {
        Value::Number(value) => value
            .as_f64()
            .map(|value| value as f32)
            .ok_or_else(|| invalid_value(field, "expected a finite JSON number"))?,
        Value::String(value) => value
            .parse::<f32>()
            .map_err(|error| invalid_value(field, format!("invalid FLOAT: {error}")))?,
        _ => return Err(invalid_value(field, "expected a number or numeric string")),
    };
    if !value.is_finite() {
        return Err(invalid_value(field, "FLOAT must be finite"));
    }
    Ok(KeyComponent::Float(canonical_f32_bits(value)))
}

fn parse_double(field: &str, value: &Value) -> Result<KeyComponent> {
    let value = match value {
        Value::Number(value) => value
            .as_f64()
            .ok_or_else(|| invalid_value(field, "expected a finite JSON number"))?,
        Value::String(value) => value
            .parse::<f64>()
            .map_err(|error| invalid_value(field, format!("invalid DOUBLE: {error}")))?,
        _ => return Err(invalid_value(field, "expected a number or numeric string")),
    };
    if !value.is_finite() {
        return Err(invalid_value(field, "DOUBLE must be finite"));
    }
    Ok(KeyComponent::Double(canonical_f64_bits(value)))
}

fn canonical_f32_bits(value: f32) -> u32 {
    if value == 0.0 {
        0.0_f32.to_bits()
    } else {
        value.to_bits()
    }
}

fn canonical_f64_bits(value: f64) -> u64 {
    if value == 0.0 {
        0.0_f64.to_bits()
    } else {
        value.to_bits()
    }
}

fn parse_decimal(field: &str, value: &Value, precision: u32, scale: u32) -> Result<KeyComponent> {
    let text = value.as_str().ok_or_else(|| {
        invalid_value(field, "expected a decimal string without exponent notation")
    })?;
    let (negative, unsigned) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    let mut parts = unsigned.split('.');
    let integer = parts.next().unwrap_or_default();
    let fraction = parts.next().unwrap_or_default();
    if integer.is_empty()
        || !integer.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        || parts.next().is_some()
        || (unsigned.contains('.') && fraction.is_empty())
    {
        return Err(invalid_value(
            field,
            "expected a decimal string such as \"123.45\"",
        ));
    }
    if fraction.len() > scale as usize {
        return Err(invalid_value(
            field,
            format!("fractional digits exceed DECIMAL scale {scale}"),
        ));
    }

    let mut digits = String::with_capacity(integer.len() + scale as usize);
    digits.push_str(integer);
    digits.push_str(fraction);
    digits.extend(std::iter::repeat_n('0', scale as usize - fraction.len()));
    let significant = digits.trim_start_matches('0');
    if significant.len() > precision as usize {
        return Err(invalid_value(
            field,
            format!("value exceeds DECIMAL({precision}, {scale}) precision"),
        ));
    }
    let magnitude = if significant.is_empty() {
        0
    } else {
        significant.parse::<i128>().map_err(|error| {
            invalid_value(field, format!("decimal cannot be represented: {error}"))
        })?
    };
    let unscaled = if negative { -magnitude } else { magnitude };
    Ok(KeyComponent::Decimal {
        unscaled,
        precision,
        scale,
    })
}

fn parse_date(field: &str, value: &Value) -> Result<KeyComponent> {
    let text = value
        .as_str()
        .ok_or_else(|| invalid_value(field, "expected an ISO date string YYYY-MM-DD"))?;
    let date = NaiveDate::parse_from_str(text, "%Y-%m-%d")
        .map_err(|error| invalid_value(field, format!("invalid ISO date: {error}")))?;
    validate_paimon_year(field, date.year())?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("Unix epoch must be valid");
    let days = i32::try_from(date.signed_duration_since(epoch).num_days())
        .map_err(|_| invalid_value(field, "date is outside DATE range"))?;
    Ok(KeyComponent::Date(days))
}

fn parse_time(field: &str, value: &Value, precision: u32) -> Result<KeyComponent> {
    let text = value
        .as_str()
        .ok_or_else(|| invalid_value(field, "expected an ISO time string HH:MM:SS[.fraction]"))?;
    validate_fraction_precision(field, text, precision)?;
    let time = NaiveTime::parse_from_str(text, "%H:%M:%S%.f")
        .map_err(|error| invalid_value(field, format!("invalid ISO time: {error}")))?;
    if time.nanosecond() >= 1_000_000_000 {
        return Err(invalid_value(field, "leap seconds are not supported"));
    }
    if time.nanosecond() % 1_000_000 != 0 {
        return Err(invalid_value(
            field,
            "TIME lookup values are limited to millisecond precision",
        ));
    }
    let millis = i32::try_from(
        u64::from(time.num_seconds_from_midnight()) * 1_000
            + u64::from(time.nanosecond() / 1_000_000),
    )
    .expect("millis of day always fit i32");
    Ok(KeyComponent::Time(millis))
}

fn parse_timestamp(
    field: &str,
    value: &Value,
    precision: u32,
    local_zoned: bool,
) -> Result<KeyComponent> {
    let text = value.as_str().ok_or_else(|| {
        invalid_value(
            field,
            if local_zoned {
                "expected an RFC 3339 timestamp with Z or an explicit offset"
            } else {
                "expected an ISO timestamp YYYY-MM-DDTHH:MM:SS[.fraction] without a time zone"
            },
        )
    })?;
    validate_fraction_precision(field, text, precision)?;

    let (seconds, subsecond_nanos) = if local_zoned {
        let timestamp = DateTime::parse_from_rfc3339(text).map_err(|error| {
            invalid_value(field, format!("invalid RFC 3339 timestamp: {error}"))
        })?;
        validate_paimon_year(field, timestamp.year())?;
        let timestamp = timestamp.with_timezone(&Utc);
        (timestamp.timestamp(), timestamp.timestamp_subsec_nanos())
    } else {
        let timestamp = NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f")
            .map_err(|error| invalid_value(field, format!("invalid ISO timestamp: {error}")))?;
        validate_paimon_year(field, timestamp.year())?;
        let timestamp = timestamp.and_utc();
        (timestamp.timestamp(), timestamp.timestamp_subsec_nanos())
    };
    if subsecond_nanos >= 1_000_000_000 {
        return Err(invalid_value(field, "leap seconds are not supported"));
    }
    let (millis, nanos) = checked_timestamp_parts(field, seconds, subsecond_nanos, precision)?;
    Ok(if local_zoned {
        KeyComponent::LocalZonedTimestamp { millis, nanos }
    } else {
        KeyComponent::Timestamp { millis, nanos }
    })
}

fn validate_paimon_year(field: &str, year: i32) -> Result<()> {
    if (0..=9_999).contains(&year) {
        Ok(())
    } else {
        Err(invalid_value(
            field,
            "year is outside the Paimon range 0000 through 9999",
        ))
    }
}

fn validate_fraction_precision(field: &str, value: &str, precision: u32) -> Result<()> {
    let fraction_digits = value
        .split_once('.')
        .map(|(_, suffix)| {
            suffix
                .bytes()
                .take_while(|byte| byte.is_ascii_digit())
                .count()
        })
        .unwrap_or(0);
    if fraction_digits > precision as usize {
        return Err(invalid_value(
            field,
            format!("fractional digits exceed declared precision {precision}"),
        ));
    }
    Ok(())
}

fn checked_timestamp_parts(
    field: &str,
    seconds: i64,
    subsecond_nanos: u32,
    precision: u32,
) -> Result<(i64, i32)> {
    let value_fits_arrow = match precision {
        0..=3 => seconds
            .checked_mul(1_000)
            .and_then(|value| value.checked_add(i64::from(subsecond_nanos / 1_000_000)))
            .is_some(),
        4..=6 => seconds
            .checked_mul(1_000_000)
            .and_then(|value| value.checked_add(i64::from(subsecond_nanos / 1_000)))
            .is_some(),
        7..=9 => seconds
            .checked_mul(1_000_000_000)
            .and_then(|value| value.checked_add(i64::from(subsecond_nanos)))
            .is_some(),
        _ => false,
    };
    if !value_fits_arrow {
        return Err(invalid_value(
            field,
            "timestamp is outside the Arrow range for the field precision",
        ));
    }
    let millis = seconds
        .checked_mul(1_000)
        .and_then(|value| value.checked_add(i64::from(subsecond_nanos / 1_000_000)))
        .ok_or_else(|| invalid_value(field, "timestamp milliseconds overflow i64"))?;
    Ok((millis, (subsecond_nanos % 1_000_000) as i32))
}

fn timestamp_component(
    field: &str,
    array: &dyn Array,
    row: usize,
    precision: u32,
    local_zoned: bool,
    data_type: &DataType,
) -> Result<KeyComponent> {
    let (millis, nanos) = match precision {
        0..=3 => array
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .map(|array| (array.value(row), 0)),
        4..=6 => array
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .map(|array| {
                let micros = array.value(row);
                (
                    micros.div_euclid(1_000),
                    (micros.rem_euclid(1_000) * 1_000) as i32,
                )
            }),
        7..=9 => array
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .map(|array| {
                let nanos = array.value(row);
                (
                    nanos.div_euclid(1_000_000),
                    nanos.rem_euclid(1_000_000) as i32,
                )
            }),
        _ => None,
    }
    .ok_or_else(|| arrow_type_mismatch(field, array, data_type))?;
    Ok(if local_zoned {
        KeyComponent::LocalZonedTimestamp { millis, nanos }
    } else {
        KeyComponent::Timestamp { millis, nanos }
    })
}

fn string_component(array: &dyn Array, row: usize) -> Option<KeyComponent> {
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        Some(KeyComponent::String(array.value(row).to_string()))
    } else if let Some(array) = array.as_any().downcast_ref::<StringViewArray>() {
        Some(KeyComponent::String(array.value(row).to_string()))
    } else {
        array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|array| KeyComponent::String(array.value(row).to_string()))
    }
}

fn binary_component(array: &dyn Array, row: usize) -> Option<KeyComponent> {
    if let Some(array) = array.as_any().downcast_ref::<BinaryArray>() {
        Some(KeyComponent::Bytes(array.value(row).to_vec()))
    } else if let Some(array) = array.as_any().downcast_ref::<BinaryViewArray>() {
        Some(KeyComponent::Bytes(array.value(row).to_vec()))
    } else {
        array
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .map(|array| KeyComponent::Bytes(array.value(row).to_vec()))
    }
}

fn parse_signed(field: &str, value: &Value) -> Result<i64> {
    if let Some(value) = value.as_i64() {
        return Ok(value);
    }
    value
        .as_str()
        .ok_or_else(|| invalid_value(field, "expected an integer or decimal integer string"))?
        .parse::<i64>()
        .map_err(|error| invalid_value(field, format!("invalid integer: {error}")))
}

fn invalid_value(field: &str, message: impl Into<String>) -> LookupError {
    LookupError::InvalidKeyValue {
        field: field.to_string(),
        message: message.into(),
    }
}

fn arrow_type_mismatch(field: &str, array: &dyn Array, data_type: &DataType) -> LookupError {
    LookupError::UnexpectedResult(format!(
        "lookup key field '{field}' expected {data_type:?}, got Arrow {:?}",
        array.data_type()
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arrow_array::{
        ArrayRef, BinaryViewArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
        RecordBatch, StringViewArray, Time32MillisecondArray, TimestampMicrosecondArray,
        TimestampNanosecondArray,
    };
    use arrow_schema::{Field, Schema};
    use paimon::spec::{
        BigIntType, DataField, DataType, DateType, DecimalType, DoubleType, FloatType, IntType,
        LocalZonedTimestampType, TimeType, TimestampType, VarBinaryType, VarCharType,
    };
    use serde_json::json;

    use super::*;

    fn fields() -> Vec<DataField> {
        vec![
            DataField::new(
                0,
                "tenant".into(),
                DataType::VarChar(VarCharType::string_type()),
            ),
            DataField::new(1, "id".into(), DataType::BigInt(BigIntType::new())),
            DataField::new(2, "shard".into(), DataType::Int(IntType::new())),
        ]
    }

    #[test]
    fn prepares_bigint_strings_in_policy_order() {
        let original = BTreeMap::from([
            ("id".to_string(), json!("9007199254740993")),
            ("tenant".to_string(), json!("t1")),
        ]);
        let prepared =
            prepare_keys(vec![original], &["tenant".into(), "id".into()], &fields()).unwrap();
        assert_eq!(
            prepared[0].normalized,
            NormalizedKey(vec![
                KeyComponent::String("t1".into()),
                KeyComponent::Long(9_007_199_254_740_993),
            ])
        );
    }

    #[test]
    fn rejects_missing_and_extra_fields() {
        let error = prepare_keys(
            vec![BTreeMap::from([
                ("tenant".to_string(), json!("t1")),
                ("extra".to_string(), json!(1)),
            ])],
            &["tenant".into(), "id".into()],
            &fields(),
        )
        .unwrap_err();
        assert!(matches!(error, LookupError::InvalidRequest(_)));
    }

    #[test]
    fn single_key_batch_uses_in_predicate() {
        let prepared = prepare_keys(
            vec![
                BTreeMap::from([("shard".to_string(), json!(1))]),
                BTreeMap::from([("shard".to_string(), json!(2))]),
            ],
            &["shard".into()],
            &fields(),
        )
        .unwrap();
        let predicate = build_batch_predicate(&prepared, &["shard".into()], &fields()).unwrap();
        assert_eq!(predicate.to_string(), "shard IN (1, 2)");
    }

    #[test]
    fn normalizes_extended_json_types_like_arrow_rows() {
        let fields = extended_fields();
        let key_fields = fields
            .iter()
            .map(|field| field.name().to_string())
            .collect::<Vec<_>>();
        let key = BTreeMap::from([
            ("float".to_string(), json!(-0.0)),
            ("double".to_string(), json!("1.25")),
            ("decimal".to_string(), json!("123.45")),
            ("date".to_string(), json!("2024-02-29")),
            ("time".to_string(), json!("12:34:56.789")),
            ("timestamp".to_string(), json!("2024-02-29T12:34:56.123456")),
            (
                "local_timestamp".to_string(),
                json!("2024-02-29T20:34:56.123456789+08:00"),
            ),
        ]);
        let mut prepared = prepare_keys(vec![key], &key_fields, &fields).unwrap();
        build_batch_predicate(&prepared, &key_fields, &fields).unwrap();
        let expected = prepared.remove(0).normalized;

        let instant = DateTime::parse_from_rfc3339("2024-02-29T12:34:56.123456789Z").unwrap();
        let timestamp_micros = instant.timestamp_micros();
        let timestamp_nanos = instant.timestamp_nanos_opt().unwrap();
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Float32Array::from(vec![0.0])),
            Arc::new(Float64Array::from(vec![1.25])),
            Arc::new(
                Decimal128Array::from(vec![12345_i128])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
            Arc::new(Date32Array::from(vec![19_782])),
            Arc::new(Time32MillisecondArray::from(vec![45_296_789])),
            Arc::new(TimestampMicrosecondArray::from(vec![timestamp_micros])),
            Arc::new(TimestampNanosecondArray::from(vec![timestamp_nanos]).with_timezone("UTC")),
        ];
        let arrow_fields = arrays
            .iter()
            .zip(&key_fields)
            .map(|(array, name)| Field::new(name, array.data_type().clone(), false))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(arrow_fields)), arrays).unwrap();

        assert_eq!(
            normalized_key_from_batch(&batch, 0, &key_fields, &fields).unwrap(),
            expected
        );
    }

    #[test]
    fn normalizes_negative_subsecond_timestamp() {
        let data_type = DataType::Timestamp(TimestampType::new(6).unwrap());
        let json = json_to_component(
            "timestamp",
            &json!("1969-12-31T23:59:59.999999"),
            &data_type,
        )
        .unwrap();
        let arrow = TimestampMicrosecondArray::from(vec![-1]);
        assert_eq!(
            array_to_component("timestamp", &arrow, 0, &data_type).unwrap(),
            json
        );
        assert_eq!(
            json,
            KeyComponent::Timestamp {
                millis: -1,
                nanos: 999_000,
            }
        );
    }

    #[test]
    fn rejects_lossy_or_ambiguous_extended_values() {
        let decimal = DataType::Decimal(DecimalType::new(5, 2).unwrap());
        assert_invalid_key(json_to_component("amount", &json!(12.34), &decimal));
        assert_invalid_key(json_to_component("amount", &json!("1e2"), &decimal));
        assert_invalid_key(json_to_component("amount", &json!("1.234"), &decimal));
        assert_invalid_key(json_to_component("amount", &json!("1234.00"), &decimal));

        let time = DataType::Time(TimeType::new(6).unwrap());
        assert_invalid_key(json_to_component("time", &json!("12:00:00.000001"), &time));
        assert_invalid_key(json_to_component("time", &json!("23:59:60"), &time));

        let timestamp = DataType::Timestamp(TimestampType::new(6).unwrap());
        assert_invalid_key(json_to_component(
            "timestamp",
            &json!("2024-01-01T00:00:00.1234567"),
            &timestamp,
        ));
        assert_invalid_key(json_to_component(
            "timestamp",
            &json!("2024-01-01T00:00:00Z"),
            &timestamp,
        ));

        let local_timestamp =
            DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(9).unwrap());
        assert_invalid_key(json_to_component(
            "local_timestamp",
            &json!("2024-01-01T00:00:00"),
            &local_timestamp,
        ));

        assert_invalid_key(json_to_component(
            "float",
            &json!("NaN"),
            &DataType::Float(FloatType::new()),
        ));
        assert_invalid_key(json_to_component(
            "double",
            &json!("inf"),
            &DataType::Double(DoubleType::new()),
        ));
    }

    #[test]
    fn accepts_arrow_view_arrays_for_text_and_binary_keys() {
        let text = StringViewArray::from(vec!["tenant-a"]);
        let binary = BinaryViewArray::from_iter_values([b"asset-1".as_slice()]);
        assert_eq!(
            array_to_component(
                "tenant",
                &text,
                0,
                &DataType::VarChar(VarCharType::string_type()),
            )
            .unwrap(),
            KeyComponent::String("tenant-a".into())
        );
        assert_eq!(
            array_to_component(
                "asset",
                &binary,
                0,
                &DataType::VarBinary(VarBinaryType::new(32).unwrap()),
            )
            .unwrap(),
            KeyComponent::Bytes(b"asset-1".to_vec())
        );
    }

    #[test]
    fn validates_supported_key_type_set() {
        for field in extended_fields() {
            validate_key_type(field.name(), field.data_type()).unwrap();
        }
        let error =
            validate_key_type("blob", &DataType::Blob(paimon::spec::BlobType::new())).unwrap_err();
        assert!(matches!(error, LookupError::UnsupportedKeyType { .. }));
    }

    #[test]
    fn global_btree_rejects_binary_keys_but_primary_key_normalization_supports_them() {
        let binary = DataType::VarBinary(VarBinaryType::new(32).unwrap());
        validate_key_type("asset", &binary).unwrap();
        assert!(!supports_global_btree(&binary));
        assert!(supports_global_btree(&DataType::Decimal(
            DecimalType::new(10, 2).unwrap()
        )));
    }

    fn extended_fields() -> Vec<DataField> {
        vec![
            DataField::new(0, "float".into(), DataType::Float(FloatType::new())),
            DataField::new(1, "double".into(), DataType::Double(DoubleType::new())),
            DataField::new(
                2,
                "decimal".into(),
                DataType::Decimal(DecimalType::new(10, 2).unwrap()),
            ),
            DataField::new(3, "date".into(), DataType::Date(DateType::new())),
            DataField::new(4, "time".into(), DataType::Time(TimeType::new(3).unwrap())),
            DataField::new(
                5,
                "timestamp".into(),
                DataType::Timestamp(TimestampType::new(6).unwrap()),
            ),
            DataField::new(
                6,
                "local_timestamp".into(),
                DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(9).unwrap()),
            ),
        ]
    }

    fn assert_invalid_key(result: Result<KeyComponent>) {
        assert!(matches!(result, Err(LookupError::InvalidKeyValue { .. })));
    }
}
