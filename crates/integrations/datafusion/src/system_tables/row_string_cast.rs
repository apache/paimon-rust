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

use std::num::NonZeroI32;

use chrono::NaiveDate;
use lexical_write_float::{format::STANDARD, Options, ToLexicalWithOptions};
use paimon::spec::{BinaryRow, DataField, DataType, Datum};
use paimon::{Error, Result};

const MILLIS_PER_DAY: i64 = 86_400_000;
const JAVA_FLOAT_OPTIONS: Options = Options::builder()
    .positive_exponent_break(NonZeroI32::new(6))
    .negative_exponent_break(NonZeroI32::new(-3))
    .exponent(b'E')
    .inf_string(Some(b"Infinity"))
    .build_strict();

pub(super) fn format_row_as_java_cast_string(
    row: &BinaryRow,
    fields: &[DataField],
) -> Result<String> {
    validate_row(row, fields)?;

    let mut out = String::from("{");
    for (pos, field) in fields.iter().enumerate() {
        if pos > 0 {
            out.push_str(", ");
        }
        out.push_str(&format_field(row, pos, field.data_type())?);
    }
    out.push('}');
    Ok(out)
}

pub(super) fn format_row_as_java_array_string(
    row: &BinaryRow,
    fields: &[DataField],
) -> Result<String> {
    validate_row(row, fields)?;

    let mut out = String::from("[");
    for (pos, field) in fields.iter().enumerate() {
        if pos > 0 {
            out.push_str(", ");
        }
        out.push_str(
            &format_object_field(row, pos, field.data_type())?.unwrap_or_else(|| "null".into()),
        );
    }
    out.push(']');
    Ok(out)
}

pub(super) fn format_row_field_as_java_object_string(
    row: &BinaryRow,
    pos: usize,
    data_type: &DataType,
) -> Result<Option<String>> {
    validate_row_has_field(row, pos)?;
    if row.is_null_at(pos) {
        return Ok(None);
    }
    format_object_field(row, pos, data_type)
}

fn format_object_field(
    row: &BinaryRow,
    pos: usize,
    data_type: &DataType,
) -> Result<Option<String>> {
    let value = match row.get_datum(pos, data_type)? {
        None => return Ok(None),
        Some(Datum::Date(value) | Datum::Time(value)) => value.to_string(),
        Some(Datum::Timestamp { millis, nanos } | Datum::LocalZonedTimestamp { millis, nanos }) => {
            paimon::common::format_iso_timestamp(
                millis.div_euclid(1000),
                (millis.rem_euclid(1000) * 1_000_000 + i64::from(nanos)) as u32,
            )
        }
        Some(_) => format_field(row, pos, data_type)?,
    };
    Ok(Some(value))
}

fn validate_row(row: &BinaryRow, fields: &[DataField]) -> Result<()> {
    if row.arity() < 0 {
        return Err(data_invalid(format!(
            "Row string cast row has negative arity {}",
            row.arity()
        )));
    }

    let arity = row.arity() as usize;
    if arity != fields.len() {
        return Err(data_invalid(format!(
            "Row string cast row arity {arity} does not match field count {}",
            fields.len()
        )));
    }

    let min_size = BinaryRow::cal_fix_part_size_in_bytes(row.arity()) as usize;
    if row.data().len() < min_size {
        return Err(data_invalid(format!(
            "Row string cast row data too short: need at least {min_size} bytes, got {}",
            row.data().len()
        )));
    }

    Ok(())
}

fn validate_row_has_field(row: &BinaryRow, pos: usize) -> Result<()> {
    if row.arity() < 0 {
        return Err(data_invalid(format!(
            "Row string cast row has negative arity {}",
            row.arity()
        )));
    }

    let arity = row.arity() as usize;
    if pos >= arity {
        return Err(data_invalid(format!(
            "Row string cast field index {pos} is outside row arity {arity}"
        )));
    }

    let min_size = BinaryRow::cal_fix_part_size_in_bytes(row.arity()) as usize;
    if row.data().len() < min_size {
        return Err(data_invalid(format!(
            "Row string cast row data too short: need at least {min_size} bytes, got {}",
            row.data().len()
        )));
    }

    Ok(())
}

fn format_field(row: &BinaryRow, pos: usize, data_type: &DataType) -> Result<String> {
    let Some(datum) = row.get_datum(pos, data_type)? else {
        return Ok("null".to_string());
    };

    match (datum, data_type) {
        (Datum::Bool(v), DataType::Boolean(_)) => Ok(v.to_string()),
        (Datum::TinyInt(v), DataType::TinyInt(_)) => Ok(v.to_string()),
        (Datum::SmallInt(v), DataType::SmallInt(_)) => Ok(v.to_string()),
        (Datum::Int(v), DataType::Int(_)) => Ok(v.to_string()),
        (Datum::Long(v), DataType::BigInt(_)) => Ok(v.to_string()),
        (Datum::Float(v), DataType::Float(_)) => Ok(format_float(v)),
        (Datum::Double(v), DataType::Double(_)) => Ok(format_double(v)),
        (Datum::String(v), DataType::Char(_) | DataType::VarChar(_)) => Ok(v),
        (Datum::Bytes(v), DataType::Binary(_) | DataType::VarBinary(_)) => {
            Ok(String::from_utf8_lossy(&v).into_owned())
        }
        (Datum::Date(v), DataType::Date(_)) => format_date(v),
        (Datum::Time(v), DataType::Time(t)) => format_time(v, t.precision()),
        (
            Datum::Decimal {
                unscaled, scale, ..
            },
            DataType::Decimal(_),
        ) => Ok(format_decimal_plain(unscaled, scale)),
        (Datum::Timestamp { .. } | Datum::LocalZonedTimestamp { .. }, _) => {
            let source = row.field_to_arrow(pos, data_type)?;
            let target = DataType::VarChar(paimon::spec::VarCharType::string_type());
            let strings =
                paimon::arrow::cast_array_for_schema_evolution(&source, data_type, &target)?;
            let strings = strings
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .ok_or_else(|| {
                    data_invalid("Timestamp string cast returned a non-string array".into())
                })?;
            Ok(strings.value(0).to_string())
        }
        (datum, _) => Err(data_invalid(format!(
            "Decoded row string cast datum {datum:?} does not match type {data_type:?}"
        ))),
    }
}

fn format_float(value: f32) -> String {
    const BUFFER_SIZE: usize = JAVA_FLOAT_OPTIONS.buffer_size_const::<f32, STANDARD>();
    let mut buffer = [0u8; BUFFER_SIZE];
    let bytes = value.to_lexical_with_options::<STANDARD>(&mut buffer, &JAVA_FLOAT_OPTIONS);
    std::str::from_utf8(bytes)
        .expect("lexical float output is valid UTF-8")
        .to_string()
}

fn format_double(value: f64) -> String {
    const BUFFER_SIZE: usize = JAVA_FLOAT_OPTIONS.buffer_size_const::<f64, STANDARD>();
    let mut buffer = [0u8; BUFFER_SIZE];
    let bytes = value.to_lexical_with_options::<STANDARD>(&mut buffer, &JAVA_FLOAT_OPTIONS);
    std::str::from_utf8(bytes)
        .expect("lexical double output is valid UTF-8")
        .to_string()
}

fn format_date(epoch_days: i32) -> Result<String> {
    let ce_days = epoch_days.checked_add(719_163).ok_or_else(|| {
        data_invalid(format!(
            "Date row string cast value {epoch_days} is outside supported range"
        ))
    })?;
    let date = NaiveDate::from_num_days_from_ce_opt(ce_days).ok_or_else(|| {
        data_invalid(format!(
            "Date row string cast value {epoch_days} is outside supported range"
        ))
    })?;
    Ok(date.format("%Y-%m-%d").to_string())
}

fn format_time(millis_of_day: i32, precision: u32) -> Result<String> {
    let mut millis = millis_of_day as i64;
    while millis < 0 {
        millis += MILLIS_PER_DAY;
    }

    let h = millis / 3_600_000;
    let m = (millis % 3_600_000) / 60_000;
    let s = (millis % 60_000) / 1_000;
    let mut ms = millis % 1_000;
    let mut out = format!("{h:02}:{m:02}:{s:02}");

    if precision > 0 {
        out.push('.');
        let mut remaining = precision;
        while remaining > 0 {
            out.push((b'0' + (ms / 100) as u8) as char);
            ms = (ms % 100) * 10;
            if ms == 0 {
                break;
            }
            remaining -= 1;
        }
    }

    Ok(out)
}

fn format_decimal_plain(unscaled: i128, scale: u32) -> String {
    if scale == 0 {
        return unscaled.to_string();
    }

    let negative = unscaled < 0;
    let abs = if unscaled == i128::MIN {
        (i128::MAX as u128) + 1
    } else {
        unscaled.unsigned_abs()
    };

    let digits = abs.to_string();
    let scale = scale as usize;
    let result = if digits.len() <= scale {
        let mut s = String::with_capacity(scale + 2);
        s.push_str("0.");
        for _ in 0..(scale - digits.len()) {
            s.push('0');
        }
        s.push_str(&digits);
        s
    } else {
        let int_len = digits.len() - scale;
        let mut s = String::with_capacity(digits.len() + 1);
        s.push_str(&digits[..int_len]);
        s.push('.');
        s.push_str(&digits[int_len..]);
        s
    };

    if negative {
        format!("-{result}")
    } else {
        result
    }
}

fn data_invalid(message: String) -> Error {
    Error::DataInvalid {
        message,
        source: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Local, TimeZone};
    use paimon::spec::{
        BigIntType, BinaryType, BlobType, BooleanType, CharType, DateType, DecimalType, DoubleType,
        FloatType, IntType, LocalZonedTimestampType, SmallIntType, TimeType, TimestampType,
        TinyIntType, VarBinaryType, VarCharType,
    };

    fn field(id: i32, data_type: DataType) -> DataField {
        DataField::new(id, format!("f{id}"), data_type)
    }

    fn row(values: &[(Option<Datum>, DataType)]) -> BinaryRow {
        let refs: Vec<_> = values
            .iter()
            .map(|(datum, data_type)| (datum.as_ref(), data_type))
            .collect();
        BinaryRow::from_datums(&refs)
    }

    fn format_value(values: &[(Option<Datum>, DataType)]) -> Result<String> {
        let fields: Vec<_> = values
            .iter()
            .enumerate()
            .map(|(i, (_, data_type))| field(i as i32, data_type.clone()))
            .collect();
        format_row_as_java_cast_string(&row(values), &fields)
    }

    #[test]
    fn test_format_supported_scalar_types() {
        let values = vec![
            (
                Some(Datum::Bool(true)),
                DataType::Boolean(BooleanType::new()),
            ),
            (
                Some(Datum::TinyInt(-1)),
                DataType::TinyInt(TinyIntType::new()),
            ),
            (
                Some(Datum::SmallInt(2)),
                DataType::SmallInt(SmallIntType::new()),
            ),
            (Some(Datum::Int(3)), DataType::Int(IntType::new())),
            (Some(Datum::Long(4)), DataType::BigInt(BigIntType::new())),
            (Some(Datum::Float(1.0)), DataType::Float(FloatType::new())),
            (
                Some(Datum::Double(10_000_000.0)),
                DataType::Double(DoubleType::new()),
            ),
            (
                Some(Datum::String("c".to_string())),
                DataType::Char(CharType::new(1).unwrap()),
            ),
            (
                Some(Datum::Bytes(b"xy".to_vec())),
                DataType::Binary(BinaryType::new(2).unwrap()),
            ),
            (
                Some(Datum::Bytes(b"abc".to_vec())),
                DataType::VarBinary(VarBinaryType::new(3).unwrap()),
            ),
            (Some(Datum::Date(19_723)), DataType::Date(DateType::new())),
            (
                Some(Datum::Time(45_296_000)),
                DataType::Time(TimeType::new(3).unwrap()),
            ),
            (
                Some(Datum::Decimal {
                    unscaled: -100,
                    precision: 10,
                    scale: 3,
                }),
                DataType::Decimal(DecimalType::new(10, 3).unwrap()),
            ),
            (
                Some(Datum::Timestamp {
                    millis: 1_704_110_400_123,
                    nanos: 456_000,
                }),
                DataType::Timestamp(TimestampType::new(6).unwrap()),
            ),
        ];

        assert_eq!(
            format_value(&values).unwrap(),
            "{true, -1, 2, 3, 4, 1.0, 1.0E7, c, xy, abc, 2024-01-01, 12:34:56.0, -0.100, 2024-01-01 12:00:00.123456}"
        );
    }

    #[test]
    fn test_format_float_double_uses_java_display_thresholds() {
        let values = vec![
            (
                Some(Datum::Double(9_999_999.0)),
                DataType::Double(DoubleType::new()),
            ),
            (
                Some(Datum::Double(10_000_000.0)),
                DataType::Double(DoubleType::new()),
            ),
            (
                Some(Datum::Double(0.001)),
                DataType::Double(DoubleType::new()),
            ),
            (
                Some(Datum::Double(0.000_999_999)),
                DataType::Double(DoubleType::new()),
            ),
            (Some(Datum::Float(-0.0)), DataType::Float(FloatType::new())),
            (
                Some(Datum::Double(f64::INFINITY)),
                DataType::Double(DoubleType::new()),
            ),
            (
                Some(Datum::Double(f64::NEG_INFINITY)),
                DataType::Double(DoubleType::new()),
            ),
            (
                Some(Datum::Double(f64::NAN)),
                DataType::Double(DoubleType::new()),
            ),
        ];

        assert_eq!(
            format_value(&values).unwrap(),
            "{9999999.0, 1.0E7, 0.001, 9.99999E-4, -0.0, Infinity, -Infinity, NaN}"
        );
    }

    #[test]
    fn test_format_binary_invalid_utf8_uses_lossy_string() {
        let bytes = vec![0xff];
        let data_type = DataType::VarBinary(VarBinaryType::new(1).unwrap());
        let expected = String::from_utf8_lossy(&bytes).into_owned();

        assert_eq!(
            format_value(&[(Some(Datum::Bytes(bytes)), data_type)]).unwrap(),
            format!("{{{expected}}}")
        );
    }

    #[test]
    fn test_format_null_values() {
        let values = vec![
            (None, DataType::Int(IntType::new())),
            (None, DataType::VarChar(VarCharType::string_type())),
        ];

        assert_eq!(format_value(&values).unwrap(), "{null, null}");
    }

    #[test]
    fn test_format_timestamp_precision_matches_java_cast() {
        let values = vec![
            (
                Some(Datum::Timestamp {
                    millis: 1_704_110_400_123,
                    nanos: 456_000,
                }),
                DataType::Timestamp(TimestampType::new(3).unwrap()),
            ),
            (
                Some(Datum::Timestamp {
                    millis: 1_704_110_400_123,
                    nanos: 456_000,
                }),
                DataType::Timestamp(TimestampType::new(6).unwrap()),
            ),
        ];

        assert_eq!(
            format_value(&values).unwrap(),
            "{2024-01-01 12:00:00.123, 2024-01-01 12:00:00.123456}"
        );
    }

    #[test]
    fn test_format_local_zoned_timestamp() {
        let data_type = DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(3).unwrap());
        let millis = 1_704_067_200_000;
        let expected = Local
            .timestamp_opt(millis / 1000, 0)
            .single()
            .map(|dt| format!("{}.000", dt.format("%Y-%m-%d %H:%M:%S")))
            .unwrap();

        assert_eq!(
            format_value(&[(
                Some(Datum::LocalZonedTimestamp { millis, nanos: 0 }),
                data_type,
            )])
            .unwrap(),
            format!("{{{expected}}}")
        );
    }

    #[test]
    fn metadata_objects_use_internal_values_and_iso_timestamps() {
        let timestamp_type = DataType::Timestamp(TimestampType::new(3).unwrap());
        let date_type = DataType::Date(DateType::new());
        let time_type = DataType::Time(TimeType::new(3).unwrap());
        let values = [
            (Some(Datum::Date(1)), date_type),
            (Some(Datum::Time(42)), time_type),
            (
                Some(Datum::Timestamp {
                    millis: 123,
                    nanos: 0,
                }),
                timestamp_type,
            ),
            (
                Some(Datum::LocalZonedTimestamp {
                    millis: 0,
                    nanos: 0,
                }),
                DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(3).unwrap()),
            ),
        ];
        let datums: Vec<_> = values
            .iter()
            .map(|(value, typ)| (value.as_ref(), typ))
            .collect();
        let row = BinaryRow::from_datums(&datums);
        let fields: Vec<_> = values
            .iter()
            .enumerate()
            .map(|(i, (_, typ))| DataField::new(i as i32, i.to_string(), typ.clone()))
            .collect();
        assert_eq!(
            format_row_as_java_array_string(&row, &fields).unwrap(),
            "[1, 42, 1970-01-01T00:00:00.123, 1970-01-01T00:00]"
        );
        assert_eq!(
            format_row_field_as_java_object_string(&row, 2, fields[2].data_type())
                .unwrap()
                .as_deref(),
            Some("1970-01-01T00:00:00.123")
        );
    }

    #[test]
    fn test_format_unsupported_type_returns_error() {
        let data_type = DataType::Blob(BlobType::new());
        let err = format_value(&[(Some(Datum::Bytes(b"x".to_vec())), data_type)])
            .expect_err("blob row string cast should be unsupported");
        assert!(matches!(err, Error::Unsupported { .. }));
    }

    #[test]
    fn test_format_arity_mismatch_returns_error() {
        let int_type = DataType::Int(IntType::new());
        let row = row(&[(Some(Datum::Int(1)), int_type.clone())]);
        let fields = vec![
            field(0, int_type.clone()),
            field(1, DataType::Int(IntType::new())),
        ];
        let err =
            format_row_as_java_cast_string(&row, &fields).expect_err("arity mismatch should fail");
        assert!(err.to_string().contains("arity"));
    }

    #[test]
    fn test_format_truncated_row_returns_error() {
        let row = BinaryRow::from_bytes(1, vec![0, 0, 0, 0]);
        let fields = vec![field(0, DataType::Int(IntType::new()))];
        let err =
            format_row_as_java_cast_string(&row, &fields).expect_err("truncated row should fail");
        assert!(err.to_string().contains("too short"));
    }
}
