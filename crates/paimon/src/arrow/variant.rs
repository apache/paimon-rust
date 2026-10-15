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

use std::sync::Arc;

use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
use arrow_array::types::Int32Type;
use arrow_array::{Array, BinaryArray, DictionaryArray, FixedSizeListArray, StructArray};
use arrow_schema::{DataType, Field};

use crate::variant::VariantFloat32Projection;
use crate::{Error, Result};

/// Extracts literal top-level numeric fields from an Arrow Variant column.
///
/// The result is row-major and preserves `fields` order. Missing fields,
/// non-object roots, and Variant nulls become child nulls; SQL-null input rows
/// remain parent nulls. Numeric values are converted to `f32`, which may lose
/// precision.
pub fn variant_get_numeric_fields(
    column: &StructArray,
    fields: &[String],
) -> Result<FixedSizeListArray> {
    extract_numeric_fields(column, fields, NumericFieldMode::Numeric)
}

#[derive(Clone, Copy)]
pub(crate) enum NumericFieldMode<'a> {
    Numeric,
    Cast { fail_on_error: &'a [bool] },
}

pub(crate) fn extract_numeric_fields(
    column: &StructArray,
    fields: &[String],
    mode: NumericFieldMode<'_>,
) -> Result<FixedSizeListArray> {
    if fields.is_empty() {
        return data_invalid("Variant numeric field list must not be empty");
    }
    if let NumericFieldMode::Cast { fail_on_error } = mode {
        if fail_on_error.len() != fields.len() {
            return data_invalid("Variant float32 projection width mismatch");
        }
    }

    let width = i32::try_from(fields.len()).map_err(|_| Error::ResourceExhausted {
        message: "Variant numeric field count exceeds Arrow limits".to_string(),
    })?;
    let value_capacity =
        column
            .len()
            .checked_mul(fields.len())
            .ok_or_else(|| Error::ResourceExhausted {
                message: "Variant numeric output exceeds addressable memory".to_string(),
            })?;

    let value_column = variant_binary_child(column, 0, "value")?;
    let metadata_column = VariantMetadataColumn::new(column)?;
    let mut builder = FixedSizeListBuilder::with_capacity(
        Float32Builder::with_capacity(value_capacity),
        width,
        column.len(),
    )
    .with_field(Arc::new(Field::new("item", DataType::Float32, true)));
    let mut projection_metadata: Option<MetadataIdentity<'_>> = None;
    let mut projection = None;
    let mut offsets = Vec::new();
    let mut extracted = vec![None; fields.len()];

    for row in 0..column.len() {
        if column.is_null(row) {
            append_null_row(&mut builder, fields.len());
            continue;
        }
        if value_column.is_null(row) || metadata_column.is_null(row) {
            return data_invalid(format!(
                "Variant row {row} has a null value or metadata child"
            ));
        }

        let (metadata, identity) = metadata_column.value(row);
        if !projection_metadata.is_some_and(|previous| previous.same_as(&identity)) {
            projection = Some(VariantFloat32Projection::new(metadata, fields)?);
            projection_metadata = Some(identity);
        }
        match mode {
            NumericFieldMode::Cast { fail_on_error } => {
                projection.as_mut().unwrap().extract_float32_cast(
                    value_column.value(row),
                    metadata,
                    fail_on_error,
                    &mut offsets,
                    &mut extracted,
                )?
            }
            NumericFieldMode::Numeric => projection.as_mut().unwrap().extract_float32(
                value_column.value(row),
                metadata,
                &mut offsets,
                &mut extracted,
            )?,
        }
        for value in &extracted {
            builder.values().append_option(*value);
        }
        builder.append(true);
    }

    Ok(builder.finish())
}

/// Variant metadata read either as plain binary or, without per-row copies, dictionary-encoded.
enum VariantMetadataColumn<'a> {
    Plain(&'a BinaryArray),
    Dictionary {
        keys: &'a DictionaryArray<Int32Type>,
        values: &'a BinaryArray,
    },
}

/// Identifies the metadata a projection was built for; dictionary keys avoid comparing bytes.
#[derive(Clone, Copy)]
enum MetadataIdentity<'a> {
    Bytes(&'a [u8]),
    Key(i32),
}

impl MetadataIdentity<'_> {
    fn same_as(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Key(a), Self::Key(b)) => a == b,
            (Self::Bytes(a), Self::Bytes(b)) => {
                (a.as_ptr() == b.as_ptr() && a.len() == b.len()) || a == b
            }
            _ => false,
        }
    }
}

impl<'a> VariantMetadataColumn<'a> {
    fn new(column: &'a StructArray) -> Result<Self> {
        let Some(field) = column.fields().get(1) else {
            return data_invalid("Expected Variant struct fields value and metadata");
        };
        if column.num_columns() != 2 || field.name() != "metadata" {
            return data_invalid("Expected Variant struct fields value and metadata");
        }
        match field.data_type() {
            DataType::Binary => Ok(Self::Plain(variant_binary_child(column, 1, "metadata")?)),
            DataType::Dictionary(key, value)
                if key.as_ref() == &DataType::Int32 && value.as_ref() == &DataType::Binary =>
            {
                let keys = column
                    .column(1)
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .ok_or_else(|| Error::DataInvalid {
                        message: "Variant metadata dictionary must use Int32 keys".to_string(),
                        source: None,
                    })?;
                let values = keys
                    .values()
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| Error::DataInvalid {
                        message: "Variant metadata dictionary values must be Binary".to_string(),
                        source: None,
                    })?;
                Ok(Self::Dictionary { keys, values })
            }
            _ => data_invalid("Expected Variant struct fields value and metadata"),
        }
    }

    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Plain(array) => array.is_null(row),
            Self::Dictionary { keys, .. } => keys.is_null(row),
        }
    }

    fn value(&self, row: usize) -> (&'a [u8], MetadataIdentity<'a>) {
        match self {
            Self::Plain(array) => {
                let bytes = array.value(row);
                (bytes, MetadataIdentity::Bytes(bytes))
            }
            Self::Dictionary { keys, values } => {
                let key = keys.keys().value(row);
                (values.value(key as usize), MetadataIdentity::Key(key))
            }
        }
    }
}

fn variant_binary_child<'a>(
    column: &'a StructArray,
    index: usize,
    expected_name: &str,
) -> Result<&'a BinaryArray> {
    let Some(field) = column.fields().get(index) else {
        return data_invalid("Expected Variant struct fields value and metadata");
    };
    if column.num_columns() != 2
        || field.name() != expected_name
        || field.data_type() != &DataType::Binary
    {
        return data_invalid("Expected Variant struct fields value and metadata");
    }
    column
        .column(index)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| Error::DataInvalid {
            message: format!("Variant {expected_name} field must be Binary"),
            source: None,
        })
}

fn append_null_row(builder: &mut FixedSizeListBuilder<Float32Builder>, field_count: usize) {
    for _ in 0..field_count {
        builder.values().append_null();
    }
    builder.append(false);
}

fn data_invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(Error::DataInvalid {
        message: message.into(),
        source: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::variant::GenericVariant;
    use arrow_array::builder::BinaryBuilder;
    use arrow_array::cast::AsArray;
    use arrow_buffer::{BooleanBuffer, NullBuffer};

    fn variant_column(rows: &[Option<&str>]) -> StructArray {
        let mut values = BinaryBuilder::new();
        let mut metadata = BinaryBuilder::new();
        let mut validity = Vec::with_capacity(rows.len());
        for row in rows {
            match row {
                Some(json) => {
                    let variant = GenericVariant::parse_json(json).unwrap();
                    values.append_value(variant.value());
                    metadata.append_value(variant.metadata());
                    validity.push(true);
                }
                None => {
                    values.append_value([]);
                    metadata.append_value([]);
                    validity.push(false);
                }
            }
        }
        StructArray::new(
            match super::super::variant_arrow_type() {
                DataType::Struct(fields) => fields,
                _ => unreachable!(),
            },
            vec![Arc::new(values.finish()), Arc::new(metadata.finish())],
            Some(NullBuffer::new(BooleanBuffer::from(validity))),
        )
    }

    #[test]
    fn dictionary_metadata_matches_plain_metadata() {
        let plain = variant_column(&[
            Some(r#"{"x":1,"y":2.5}"#),
            None,
            Some(r#"{"y":-1,"x":4}"#),
            Some(r#"{"z":true}"#),
        ]);
        let dictionary_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Binary));
        let metadata = arrow_cast::cast(plain.column(1), &dictionary_type).unwrap();
        let mut fields = plain
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>();
        fields[1] = fields[1].clone().with_data_type(dictionary_type);
        let dictionary = StructArray::new(
            fields.into(),
            vec![plain.column(0).clone(), metadata],
            plain.nulls().cloned(),
        );
        let names = vec!["x".to_string(), "y".to_string()];
        let expected = variant_get_numeric_fields(&plain, &names).unwrap();
        let actual = variant_get_numeric_fields(&dictionary, &names).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn extracts_numeric_fields_in_requested_order() {
        let input = variant_column(&[
            Some(r#"{"i":7,"f":1.25e0,"d":12.50,"missing":null}"#),
            Some(r#"{"i":-2,"f":-0.0,"d":0}"#),
            None,
        ]);
        let fields = vec![
            "d".to_string(),
            "i".to_string(),
            "f".to_string(),
            "missing".to_string(),
            "absent".to_string(),
        ];

        let output = variant_get_numeric_fields(&input, &fields).unwrap();
        assert_eq!(output.len(), 3);
        assert!(output.is_valid(0));
        assert!(output.is_valid(1));
        assert!(output.is_null(2));
        let values = output
            .values()
            .as_primitive::<arrow_array::types::Float32Type>();
        assert_eq!(values.value(0), 12.5);
        assert_eq!(values.value(1), 7.0);
        assert_eq!(values.value(2), 1.25);
        assert!(values.is_null(3));
        assert!(values.is_null(4));
        assert_eq!(values.value(5), 0.0);
        assert_eq!(values.value(6), -2.0);
        assert_eq!(values.value(7), 0.0);
        assert!(values.is_null(8));
        assert!(values.is_null(9));
    }

    #[test]
    fn treats_field_names_as_literals() {
        let input = variant_column(&[Some(r#"{"a.b":3,"a":{"b":9}}"#)]);
        let output = variant_get_numeric_fields(&input, &["a.b".to_string()]).unwrap();
        let values = output
            .values()
            .as_primitive::<arrow_array::types::Float32Type>();
        assert_eq!(values.value(0), 3.0);
    }

    #[test]
    fn rejects_non_numeric_fields() {
        let input = variant_column(&[Some(r#"{"value":"3"}"#)]);
        let err = variant_get_numeric_fields(&input, &["value".to_string()]).unwrap_err();
        assert!(matches!(err, Error::Unsupported { .. }));
    }

    #[test]
    fn rejects_empty_field_list() {
        let input = variant_column(&[Some(r#"{"value":3}"#)]);
        let err = variant_get_numeric_fields(&input, &[]).unwrap_err();
        assert!(matches!(err, Error::DataInvalid { .. }));
    }

    #[test]
    fn returns_null_fields_for_non_object_roots() {
        let input = variant_column(&[
            Some(r#"{"value":3}"#),
            Some("null"),
            Some("3"),
            Some("[1,2]"),
            None,
        ]);
        let output = variant_get_numeric_fields(&input, &["value".to_string()]).unwrap();
        let values = output
            .values()
            .as_primitive::<arrow_array::types::Float32Type>();

        assert_eq!(values.value(0), 3.0);
        assert!(values.is_null(1));
        assert!(values.is_null(2));
        assert!(values.is_null(3));
        assert!(output.is_valid(0));
        assert!(output.is_valid(1));
        assert!(output.is_valid(2));
        assert!(output.is_valid(3));
        assert!(output.is_null(4));
    }

    #[test]
    fn rejects_malformed_payloads() {
        let mut values = BinaryBuilder::new();
        values.append_value([0x02]);
        let mut metadata = BinaryBuilder::new();
        metadata.append_value([0x01, 0x00, 0x00]);
        let input = StructArray::new(
            match super::super::variant_arrow_type() {
                DataType::Struct(fields) => fields,
                _ => unreachable!(),
            },
            vec![Arc::new(values.finish()), Arc::new(metadata.finish())],
            None,
        );

        let err = variant_get_numeric_fields(&input, &["value".to_string()]).unwrap_err();
        assert!(matches!(err, Error::DataInvalid { .. }));
    }

    #[test]
    fn respects_sliced_array_offsets() {
        let input = variant_column(&[Some(r#"{"v":1}"#), Some(r#"{"v":2}"#)]);
        let sliced = input.slice(1, 1);
        let output = variant_get_numeric_fields(&sliced, &["v".to_string()]).unwrap();
        let values = output
            .values()
            .as_primitive::<arrow_array::types::Float32Type>();
        assert_eq!(values.value(0), 2.0);
    }
}
