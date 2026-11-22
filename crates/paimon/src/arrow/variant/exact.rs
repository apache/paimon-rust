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

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeListArray,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray,
    LargeListArray, LargeStringArray, ListArray, MapArray, StringArray, StructArray,
    TimestampMicrosecondArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{ArrowError, DataType, TimeUnit};

use super::{data_invalid, VariantBinaryColumn, VariantMetadataColumn};
use crate::variant::{parse_path, VariantKind, VariantRef};
use crate::{Error, Result};

/// Extract a Variant path using the exact PyPaimon type contract, without casts.
///
/// Missing paths, Variant nulls and SQL nulls produce nulls. Supports booleans,
/// signed integers, floats, strings, binary, dates, microsecond timestamps,
/// Decimal128, and recursively typed structs, lists and string-keyed maps.
/// Float widths, timestamp timezone presence and decimal scales must match the
/// stored Variant type. Integer narrowing and decimal precision are checked.
/// Nested result fields are nullable because missing paths and Variant nulls
/// may introduce nulls, regardless of the requested field annotations. The
/// Python binding restores those annotations when exporting to PyArrow.
pub fn variant_get(column: &StructArray, path: &str, data_type: &DataType) -> Result<ArrayRef> {
    validate_type(data_type)?;
    let path = parse_path(path)?;
    let values = VariantBinaryColumn::new(column, 0, "value")?;
    let metadata = VariantMetadataColumn::new(column)?;
    let mut selected = Vec::with_capacity(column.len());
    for row in 0..column.len() {
        if column.is_null(row) {
            selected.push(None);
            continue;
        }
        if values.is_null(row) || metadata.is_null(row) {
            return data_invalid(format!(
                "Variant row {row} has a null value or metadata child"
            ));
        }
        let (metadata, _) = metadata.value(row);
        selected.push(VariantRef::new(values.value(row), metadata, 0)?.get_path_segments(&path)?);
    }
    extract_array(&selected, &nullable_type(data_type))
}

// Only field nullability changes; names, metadata and all physical layouts stay
// identical so the binding can export the originally requested Arrow schema.
fn nullable_type(data_type: &DataType) -> DataType {
    let nullable_field = |field: &arrow_schema::FieldRef| {
        Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(nullable_type(field.data_type()))
                .with_nullable(true),
        )
    };
    match data_type {
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(nullable_field).collect()),
        DataType::List(field) => DataType::List(nullable_field(field)),
        DataType::LargeList(field) => DataType::LargeList(nullable_field(field)),
        DataType::FixedSizeList(field, size) => {
            DataType::FixedSizeList(nullable_field(field), *size)
        }
        DataType::Map(entries, ordered) => {
            let DataType::Struct(fields) = entries.data_type() else {
                unreachable!()
            };
            DataType::Map(
                Arc::new(entries.as_ref().clone().with_data_type(DataType::Struct(
                    vec![fields[0].clone(), nullable_field(&fields[1])].into(),
                ))),
                *ordered,
            )
        }
        _ => data_type.clone(),
    }
}

fn validate_type(data_type: &DataType) -> Result<()> {
    match data_type {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::Date32
        | DataType::Timestamp(TimeUnit::Microsecond, _) => Ok(()),
        DataType::Decimal128(_, scale) if *scale < 0 => {
            data_invalid("VARIANT decimal scale must be non-negative")
        }
        DataType::Decimal128(precision, _) => {
            // PyArrow allows scale > precision for empty/all-null arrays. A
            // non-null coefficient must still fit both precision and scale.
            if !(1..=38).contains(precision) {
                return data_invalid("Invalid VARIANT Decimal128 precision or scale");
            }
            Ok(())
        }
        DataType::Struct(fields) => fields.iter().try_for_each(|f| validate_type(f.data_type())),
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            validate_type(field.data_type())
        }
        DataType::Map(entries, _) => {
            if let DataType::Struct(fields) = entries.data_type() {
                if fields.len() == 2
                    && matches!(fields[0].data_type(), DataType::Utf8 | DataType::LargeUtf8)
                {
                    return validate_type(fields[1].data_type());
                }
            }
            unsupported(data_type)
        }
        _ => unsupported(data_type),
    }
}

fn unsupported<T>(data_type: &DataType) -> Result<T> {
    Err(Error::Unsupported {
        message: format!("Unsupported exact VARIANT data type: {data_type}"),
    })
}

fn mismatch(data_type: &DataType) -> Error {
    Error::DataTypeInvalid {
        message: format!("VARIANT path type does not match {data_type}"),
    }
}

fn arrow_error(error: ArrowError) -> Error {
    Error::DataInvalid {
        message: error.to_string(),
        source: Some(Box::new(error)),
    }
}

fn extract_array(values: &[Option<VariantRef<'_>>], data_type: &DataType) -> Result<ArrayRef> {
    let expected = match data_type {
        DataType::Boolean => VariantKind::Boolean,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => VariantKind::Long,
        DataType::Float32 => VariantKind::Float,
        DataType::Float64 => VariantKind::Double,
        DataType::Utf8 | DataType::LargeUtf8 => VariantKind::String,
        DataType::Binary | DataType::LargeBinary => VariantKind::Binary,
        DataType::Date32 => VariantKind::Date,
        DataType::Timestamp(_, Some(_)) => VariantKind::Timestamp,
        DataType::Timestamp(_, None) => VariantKind::TimestampNtz,
        DataType::Decimal128(_, _) => VariantKind::Decimal,
        DataType::Struct(_) | DataType::Map(_, _) => VariantKind::Object,
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            VariantKind::Array
        }
        _ => return unsupported(data_type),
    };
    let values = values
        .iter()
        .map(|value| match value {
            Some(value) => match value.kind()? {
                VariantKind::Null => Ok(None),
                kind if kind == expected => Ok(Some(*value)),
                _ => Err(mismatch(data_type)),
            },
            None => Ok(None),
        })
        .collect::<Result<Vec<_>>>()?;

    macro_rules! primitive {
        ($array:ty, $get:expr) => {{
            let output = values
                .iter()
                .map(|value| value.map($get).transpose())
                .collect::<Result<Vec<_>>>()?;
            Arc::new(<$array>::from(output)) as ArrayRef
        }};
    }
    macro_rules! integer {
        ($array:ty, $native:ty) => {
            primitive!($array, |v: VariantRef<'_>| {
                <$native>::try_from(v.get_long()?).map_err(|_| Error::DataInvalid {
                    message: format!("VARIANT integer is out of range for {data_type}"),
                    source: None,
                })
            })
        };
    }
    Ok(match data_type {
        DataType::Boolean => primitive!(BooleanArray, |v| v.get_boolean()),
        DataType::Int8 => integer!(Int8Array, i8),
        DataType::Int16 => integer!(Int16Array, i16),
        DataType::Int32 => integer!(Int32Array, i32),
        DataType::Int64 => primitive!(Int64Array, |v| v.get_long()),
        DataType::Float32 => primitive!(Float32Array, |v| v.get_float()),
        DataType::Float64 => primitive!(Float64Array, |v| v.get_double()),
        DataType::Date32 => integer!(Date32Array, i32),
        DataType::Timestamp(_, timezone) => {
            let output = values
                .iter()
                .map(|value| value.map(|v| v.get_long()).transpose())
                .collect::<Result<Vec<_>>>()?;
            Arc::new(TimestampMicrosecondArray::from(output).with_timezone_opt(timezone.clone()))
        }
        DataType::Utf8 => primitive!(StringArray, |v| v.get_string()),
        DataType::LargeUtf8 => primitive!(LargeStringArray, |v| v.get_string()),
        DataType::Binary => primitive!(BinaryArray, |v| v.get_binary()),
        DataType::LargeBinary => primitive!(LargeBinaryArray, |v| v.get_binary()),
        DataType::Decimal128(precision, scale) => {
            let output = values
                .iter()
                .map(|value| {
                    value
                        .map(|value| {
                            let decimal = value.get_decimal()?;
                            if decimal.scale != *scale {
                                return Err(mismatch(data_type));
                            }
                            if decimal.precision.max(decimal.scale as u8) > *precision {
                                return data_invalid("VARIANT decimal exceeds target precision");
                            }
                            Ok(decimal.unscaled)
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?;
            Arc::new(Decimal128Array::from(output).with_data_type(data_type.clone()))
        }
        DataType::Struct(fields) => {
            let columns = fields
                .iter()
                .map(|field| {
                    let children = values
                        .iter()
                        .map(|value| {
                            value
                                .map(|v| v.get_field_by_key(field.name()))
                                .transpose()
                                .map(Option::flatten)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    extract_array(&children, field.data_type())
                })
                .collect::<Result<Vec<_>>>()?;
            Arc::new(
                StructArray::try_new_with_length(
                    fields.clone(),
                    columns,
                    Some(validity(&values)),
                    values.len(),
                )
                .map_err(arrow_error)?,
            )
        }
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            let mut children = Vec::new();
            let mut offsets = vec![0usize];
            for value in &values {
                let elements = value.map(|v| v.array_elements()).transpose()?;
                if let DataType::FixedSizeList(_, size) = data_type {
                    let size = usize::try_from(*size).map_err(|_| mismatch(data_type))?;
                    match elements {
                        Some(elements) if elements.len() == size => {
                            children.extend(elements.into_iter().map(Some));
                        }
                        Some(_) => return Err(mismatch(data_type)),
                        None => children.extend(std::iter::repeat_n(None, size)),
                    }
                } else if let Some(elements) = elements {
                    children.extend(elements.into_iter().map(Some));
                }
                offsets.push(children.len());
            }
            let children = extract_array(&children, field.data_type())?;
            let nulls = Some(validity(&values));
            match data_type {
                DataType::List(_) => Arc::new(
                    ListArray::try_new(field.clone(), checked_offsets(&offsets)?, children, nulls)
                        .map_err(arrow_error)?,
                ),
                DataType::LargeList(_) => Arc::new(
                    LargeListArray::try_new(
                        field.clone(),
                        checked_offsets(&offsets)?,
                        children,
                        nulls,
                    )
                    .map_err(arrow_error)?,
                ),
                DataType::FixedSizeList(_, size) => Arc::new(
                    FixedSizeListArray::try_new_with_length(
                        field.clone(),
                        *size,
                        children,
                        nulls,
                        values.len(),
                    )
                    .map_err(arrow_error)?,
                ),
                _ => unreachable!(),
            }
        }
        DataType::Map(entries, ordered) => {
            let DataType::Struct(fields) = entries.data_type() else {
                unreachable!()
            };
            let mut keys = Vec::new();
            let mut children = Vec::new();
            let mut offsets = vec![0usize];
            for value in &values {
                if let Some(value) = value {
                    for field in value.object_fields()? {
                        keys.push(field.key);
                        children.push(Some(field.value));
                    }
                }
                offsets.push(keys.len());
            }
            let keys: ArrayRef = match fields[0].data_type() {
                DataType::Utf8 => Arc::new(StringArray::from(keys)),
                DataType::LargeUtf8 => Arc::new(LargeStringArray::from(keys)),
                _ => unreachable!(),
            };
            let children = extract_array(&children, fields[1].data_type())?;
            let entry_array = StructArray::try_new(fields.clone(), vec![keys, children], None)
                .map_err(arrow_error)?;
            Arc::new(
                MapArray::try_new(
                    entries.clone(),
                    checked_offsets(&offsets)?,
                    entry_array,
                    Some(validity(&values)),
                    *ordered,
                )
                .map_err(arrow_error)?,
            )
        }
        _ => return unsupported(data_type),
    })
}

fn validity(values: &[Option<VariantRef<'_>>]) -> NullBuffer {
    values.iter().map(Option::is_some).collect()
}

fn checked_offsets<T: arrow_array::OffsetSizeTrait>(offsets: &[usize]) -> Result<OffsetBuffer<T>> {
    let offsets = offsets
        .iter()
        .map(|&offset| {
            T::from_usize(offset).ok_or_else(|| Error::ResourceExhausted {
                message: "Variant extraction exceeds Arrow offset limits".to_string(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(OffsetBuffer::new(offsets.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::variant::GenericVariant;
    use arrow_schema::Field;

    #[test]
    fn missing_fields_produce_valid_nullable_rust_arrays() {
        let variant = GenericVariant::parse_json("{}").unwrap();
        let input = StructArray::from(vec![
            (
                Arc::new(Field::new("value", DataType::Binary, false)),
                Arc::new(BinaryArray::from(vec![variant.value()])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("metadata", DataType::Binary, false)),
                Arc::new(BinaryArray::from(vec![variant.metadata()])) as ArrayRef,
            ),
        ]);
        let requested = DataType::Struct(vec![Field::new("x", DataType::Int64, false)].into());
        let output = variant_get(&input, "$", &requested).unwrap();
        output.to_data().validate_full().unwrap();
        let output = output.as_any().downcast_ref::<StructArray>().unwrap();
        assert!(output.fields()[0].is_nullable());
        assert!(output.column(0).is_null(0));
        assert!(output.is_valid(0));
    }
}
