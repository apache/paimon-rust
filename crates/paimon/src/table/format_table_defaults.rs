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
    Array, ArrayRef, BinaryArray, Date32Array, RecordBatch, StringArray, Time32MillisecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow_schema::Schema as ArrowSchema;

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
        DataType::Blob(_) => Err(arrow_schema::ArrowError::CastError(
            "Java does not support casting a string default to BLOB".into(),
        )),
        _ => arrow_cast::cast(&StringArray::from(vec![text]), arrow_field.data_type()),
    }
}

fn numeric_default(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit())
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
}
