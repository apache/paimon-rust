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

use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow_schema::Schema as ArrowSchema;

use crate::spec::DataField;
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
                        let source = StringArray::from(vec![text]);
                        let value = arrow_cast::cast(&source, schema.field(index).data_type())
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
}
