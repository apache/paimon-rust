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

//! Schema evolution utilities for mapping between table schema and data file schema.
//!
//! Reference: [org.apache.paimon.schema.SchemaEvolutionUtil](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/schema/SchemaEvolutionUtil.java)

use crate::arrow::paimon_type_to_arrow;
use crate::spec::{DataField, DataType};
use arrow_array::builder::{BinaryBuilder, StringBuilder};
use arrow_array::types::{
    ArrowPrimitiveType, Date32Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
};
use arrow_array::{Array, ArrayRef, BinaryArray, PrimitiveArray, StringArray};
use arrow_cast::cast;
use std::collections::HashMap;
use std::sync::Arc;

/// Sentinel value indicating a field does not exist in the data schema.
pub const NULL_FIELD_INDEX: i32 = -1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SchemaEvolutionCast {
    Identity,
    NumericPrimitive,
    Decimal,
    DateToTimestamp,
    TimestampToDate,
    CharacterString,
    BinaryString,
}

/// Return whether a schema-evolution cast has a real executor.
///
/// This is deliberately narrower than SQL explicit casting. Schema admission
/// uses this function in addition to the logical Paimon cast rules, so every
/// accepted type change can be executed while reading old files.
pub(crate) fn schema_evolution_cast_implemented(source: &DataType, target: &DataType) -> bool {
    resolve_schema_evolution_cast(source, target).is_some()
}

fn resolve_schema_evolution_cast(
    source: &DataType,
    target: &DataType,
) -> Option<SchemaEvolutionCast> {
    if same_type_ignoring_nullability(source, target) {
        return is_top_level_scalar(source).then_some(SchemaEvolutionCast::Identity);
    }

    match (source, target) {
        (DataType::Char(_) | DataType::VarChar(_), DataType::Char(_) | DataType::VarChar(_)) => {
            Some(SchemaEvolutionCast::CharacterString)
        }
        (
            DataType::Binary(_) | DataType::VarBinary(_),
            DataType::Binary(_) | DataType::VarBinary(_),
        ) => Some(SchemaEvolutionCast::BinaryString),
        (source, target) if is_numeric_primitive(source) && is_numeric_primitive(target) => {
            Some(SchemaEvolutionCast::NumericPrimitive)
        }
        (source, DataType::Decimal(_))
            if is_integer_numeric(source) || matches!(source, DataType::Decimal(_)) =>
        {
            Some(SchemaEvolutionCast::Decimal)
        }
        (DataType::Date(_), DataType::Timestamp(timestamp)) if timestamp.precision() <= 3 => {
            Some(SchemaEvolutionCast::DateToTimestamp)
        }
        (DataType::Timestamp(_), DataType::Date(_)) => Some(SchemaEvolutionCast::TimestampToDate),
        _ => None,
    }
}

fn is_top_level_scalar(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean(_)
            | DataType::TinyInt(_)
            | DataType::SmallInt(_)
            | DataType::Int(_)
            | DataType::BigInt(_)
            | DataType::Decimal(_)
            | DataType::Double(_)
            | DataType::Float(_)
            | DataType::Binary(_)
            | DataType::VarBinary(_)
            | DataType::Char(_)
            | DataType::VarChar(_)
            | DataType::Date(_)
            | DataType::LocalZonedTimestamp(_)
            | DataType::Time(_)
            | DataType::Timestamp(_)
    )
}

fn is_integer_numeric(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::TinyInt(_) | DataType::SmallInt(_) | DataType::Int(_) | DataType::BigInt(_)
    )
}

fn is_numeric_primitive(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::TinyInt(_)
            | DataType::SmallInt(_)
            | DataType::Int(_)
            | DataType::BigInt(_)
            | DataType::Double(_)
            | DataType::Float(_)
    )
}

pub(crate) fn same_type_ignoring_nullability(source: &DataType, target: &DataType) -> bool {
    match (
        source.copy_with_nullable(true),
        target.copy_with_nullable(true),
    ) {
        (Ok(source), Ok(target)) => source == target,
        _ => false,
    }
}

/// Cast one physical Arrow column according to Paimon schema-evolution
/// semantics. Arrow is used only after the Paimon source/target pair has been
/// resolved to a supported executor.
pub(crate) fn cast_array_for_schema_evolution(
    array: &ArrayRef,
    source: &DataType,
    target: &DataType,
) -> crate::Result<ArrayRef> {
    // Unchanged complex fields can still appear in an old file selected by a
    // different table schema version. They need no ALTER TYPE executor, but
    // nested Arrow fields may still carry file metadata which must be removed
    // before constructing a batch with the current logical schema.
    if same_type_ignoring_nullability(source, target) {
        let target_arrow_type = paimon_type_to_arrow(target)?;
        if array.data_type() == &target_arrow_type {
            return Ok(array.clone());
        }
        return cast(array.as_ref(), &target_arrow_type).map_err(|error| {
            crate::Error::UnexpectedError {
                message: format!(
                    "Failed schema evolution Arrow normalization from {source:?} to {target:?}: {error}"
                ),
                source: Some(Box::new(error)),
            }
        });
    }
    let executor =
        resolve_schema_evolution_cast(source, target).ok_or_else(|| crate::Error::Unsupported {
            message: format!(
                "Schema evolution cast from {source:?} to {target:?} is not implemented"
            ),
        })?;

    match executor {
        SchemaEvolutionCast::Identity => Ok(array.clone()),
        SchemaEvolutionCast::NumericPrimitive => cast_numeric_primitive(array, source, target),
        SchemaEvolutionCast::Decimal => {
            let target_arrow_type = paimon_type_to_arrow(target)?;
            cast(array.as_ref(), &target_arrow_type).map_err(|error| {
                crate::Error::UnexpectedError {
                    message: format!(
                        "Failed schema evolution cast from {source:?} to {target:?}: {error}"
                    ),
                    source: Some(Box::new(error)),
                }
            })
        }
        SchemaEvolutionCast::DateToTimestamp => cast_date_to_timestamp(array, target),
        SchemaEvolutionCast::TimestampToDate => cast_timestamp_to_date(array, source),
        SchemaEvolutionCast::CharacterString => cast_character_string(array, target),
        SchemaEvolutionCast::BinaryString => cast_binary_string(array, target),
    }
}

fn downcast_primitive<'a, T: ArrowPrimitiveType>(
    array: &'a ArrayRef,
    semantic_type: &str,
) -> crate::Result<&'a PrimitiveArray<T>> {
    array
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| crate::Error::DataInvalid {
            message: format!(
                "Expected {semantic_type} array with physical type {:?}, found {:?}",
                T::DATA_TYPE,
                array.data_type()
            ),
            source: None,
        })
}

macro_rules! cast_numeric_primitive_array {
    ($array:expr, $source:ty, $target:expr) => {{
        let values = downcast_primitive::<$source>($array, "numeric")?;
        let result: ArrayRef = match $target {
            DataType::TinyInt(_) => {
                Arc::new(values.unary::<_, Int8Type>(|value| value as i32 as i8))
            }
            DataType::SmallInt(_) => {
                Arc::new(values.unary::<_, Int16Type>(|value| value as i32 as i16))
            }
            DataType::Int(_) => Arc::new(values.unary::<_, Int32Type>(|value| value as i32)),
            DataType::BigInt(_) => Arc::new(values.unary::<_, Int64Type>(|value| value as i64)),
            DataType::Float(_) => Arc::new(values.unary::<_, Float32Type>(|value| value as f32)),
            DataType::Double(_) => Arc::new(values.unary::<_, Float64Type>(|value| value as f64)),
            _ => unreachable!("numeric primitive executor requires a primitive numeric target"),
        };
        Ok(result)
    }};
}

fn cast_numeric_primitive(
    array: &ArrayRef,
    source: &DataType,
    target: &DataType,
) -> crate::Result<ArrayRef> {
    // Java Number converts floating-point values to byte/short through int,
    // combining saturating float-to-int conversion with wrapping integer
    // narrowing. The macro uses that two-stage conversion for those targets.
    match source {
        DataType::TinyInt(_) => cast_numeric_primitive_array!(array, Int8Type, target),
        DataType::SmallInt(_) => cast_numeric_primitive_array!(array, Int16Type, target),
        DataType::Int(_) => cast_numeric_primitive_array!(array, Int32Type, target),
        DataType::BigInt(_) => cast_numeric_primitive_array!(array, Int64Type, target),
        DataType::Float(_) => cast_numeric_primitive_array!(array, Float32Type, target),
        DataType::Double(_) => cast_numeric_primitive_array!(array, Float64Type, target),
        _ => unreachable!("numeric primitive executor requires a primitive numeric source"),
    }
}

fn cast_date_to_timestamp(array: &ArrayRef, target: &DataType) -> crate::Result<ArrayRef> {
    let values = downcast_primitive::<Date32Type>(array, "DATE")?;
    let DataType::Timestamp(timestamp) = target else {
        unreachable!("date executor requires a TIMESTAMP target")
    };
    let result: ArrayRef = match timestamp.precision() {
        0..=3 => Arc::new(
            values.unary::<_, TimestampMillisecondType>(|value| i64::from(value) * 86_400_000),
        ),
        _ => unreachable!("DATE to TIMESTAMP precision above 3 is not admitted"),
    };
    Ok(result)
}

fn cast_timestamp_to_date(array: &ArrayRef, source: &DataType) -> crate::Result<ArrayRef> {
    let DataType::Timestamp(timestamp) = source else {
        unreachable!("timestamp executor requires a TIMESTAMP source")
    };
    let result: ArrayRef = match timestamp.precision() {
        0..=3 => Arc::new(
            downcast_primitive::<TimestampMillisecondType>(array, "TIMESTAMP")?
                .unary::<_, Date32Type>(|value| (value / 86_400_000) as i32),
        ),
        4..=6 => Arc::new(
            downcast_primitive::<TimestampMicrosecondType>(array, "TIMESTAMP")?
                .unary::<_, Date32Type>(|value| (value / 86_400_000_000) as i32),
        ),
        7..=9 => Arc::new(
            downcast_primitive::<TimestampNanosecondType>(array, "TIMESTAMP")?
                .unary::<_, Date32Type>(|value| (value / 86_400_000_000_000) as i32),
        ),
        _ => unreachable!("TIMESTAMP precision is validated by its data type"),
    };
    Ok(result)
}

/// Normalize values before storage only for logical types whose constraints are
/// not represented by their Arrow type. Callers decide which evolved field IDs
/// require this normalization.
pub(crate) fn normalize_array_for_schema_evolution_storage(
    array: &ArrayRef,
    target: &DataType,
) -> crate::Result<ArrayRef> {
    match target {
        DataType::Char(_) | DataType::VarChar(_) => cast_character_string(array, target),
        DataType::Binary(_) | DataType::VarBinary(_) => cast_binary_string(array, target),
        _ => Ok(array.clone()),
    }
}

pub(crate) fn requires_schema_evolution_storage_normalization(target: &DataType) -> bool {
    match target {
        DataType::Char(_) | DataType::Binary(_) => true,
        DataType::VarChar(data_type) => data_type.length() < crate::spec::VarCharType::MAX_LENGTH,
        DataType::VarBinary(data_type) => {
            data_type.length() < crate::spec::VarBinaryType::MAX_LENGTH
        }
        _ => false,
    }
}

fn cast_character_string(array: &ArrayRef, target: &DataType) -> crate::Result<ArrayRef> {
    let values = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| crate::Error::DataInvalid {
            message: format!(
                "Expected Utf8 array for schema evolution cast, found {:?}",
                array.data_type()
            ),
            source: None,
        })?;
    let (length, fixed) = match target {
        DataType::Char(data_type) => (data_type.length(), true),
        DataType::VarChar(data_type) => (data_type.length() as usize, false),
        _ => unreachable!("character executor requires a character target"),
    };

    let mut builder = StringBuilder::new();
    for index in 0..values.len() {
        if values.is_null(index) {
            builder.append_null();
            continue;
        }
        let value = values.value(index);
        let char_count = value.chars().count();
        if char_count > length {
            let truncated = value.chars().take(length).collect::<String>();
            builder.append_value(truncated);
        } else if fixed && char_count < length {
            let mut padded = String::with_capacity(value.len() + length - char_count);
            padded.push_str(value);
            padded.extend(std::iter::repeat_n(' ', length - char_count));
            builder.append_value(padded);
        } else {
            builder.append_value(value);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn cast_binary_string(array: &ArrayRef, target: &DataType) -> crate::Result<ArrayRef> {
    let values = array
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| crate::Error::DataInvalid {
            message: format!(
                "Expected Binary array for schema evolution cast, found {:?}",
                array.data_type()
            ),
            source: None,
        })?;
    let (length, fixed) = match target {
        DataType::Binary(data_type) => (data_type.length(), true),
        DataType::VarBinary(data_type) => (data_type.length() as usize, false),
        _ => unreachable!("binary executor requires a binary target"),
    };

    let mut builder = BinaryBuilder::new();
    for index in 0..values.len() {
        if values.is_null(index) {
            builder.append_null();
            continue;
        }
        let value = values.value(index);
        if value.len() > length {
            builder.append_value(&value[..length]);
        } else if fixed && value.len() < length {
            let mut padded = Vec::with_capacity(length);
            padded.extend_from_slice(value);
            padded.resize(length, 0);
            builder.append_value(padded);
        } else {
            builder.append_value(value);
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Create index mapping from table fields to underlying data fields using field IDs.
///
/// For example, the table and data fields are as follows:
/// - table fields: `1->c, 6->b, 3->a`
/// - data fields: `1->a, 3->c`
///
/// We get the index mapping `[0, -1, 1]`, where:
/// - `0` is the index of table field `1->c` in data fields
/// - `-1` means field `6->b` does not exist in data fields
/// - `1` is the index of table field `3->a` in data fields
///
/// Returns `None` if the mapping is identity (no evolution needed).
///
/// Reference: [SchemaEvolutionUtil.createIndexMapping](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/schema/SchemaEvolutionUtil.java)
pub fn create_index_mapping(
    table_fields: &[DataField],
    data_fields: &[DataField],
) -> Option<Vec<i32>> {
    let mut field_id_to_index: HashMap<i32, i32> = HashMap::with_capacity(data_fields.len());
    for (i, field) in data_fields.iter().enumerate() {
        field_id_to_index.insert(field.id(), i as i32);
    }

    let mut index_mapping = Vec::with_capacity(table_fields.len());
    for field in table_fields {
        let data_index = field_id_to_index
            .get(&field.id())
            .copied()
            .unwrap_or(NULL_FIELD_INDEX);
        index_mapping.push(data_index);
    }

    // Check if mapping is identity (no evolution needed).
    let is_identity = index_mapping.len() == data_fields.len()
        && index_mapping
            .iter()
            .enumerate()
            .all(|(i, &idx)| idx == i as i32);

    if is_identity {
        None
    } else {
        Some(index_mapping)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{
        ArrayType, BigIntType, BinaryType, CharType, DataType, DateType, DecimalType, DoubleType,
        FloatType, IntType, SmallIntType, TimestampType, TinyIntType, VarBinaryType, VarCharType,
    };
    use arrow_array::{
        ArrayRef, Date32Array, Decimal128Array, Float32Array, Float64Array, Int16Array, Int32Array,
        Int64Array, Int8Array, ListArray, TimestampMillisecondArray,
    };
    use arrow_buffer::{OffsetBuffer, ScalarBuffer};
    use arrow_schema::{DataType as ArrowDataType, Field};

    fn field(id: i32, name: &str) -> DataField {
        DataField::new(id, name.to_string(), DataType::Int(IntType::new()))
    }

    #[test]
    fn test_identity_mapping() {
        let table_fields = vec![field(0, "a"), field(1, "b"), field(2, "c")];
        let data_fields = vec![field(0, "a"), field(1, "b"), field(2, "c")];
        assert_eq!(create_index_mapping(&table_fields, &data_fields), None);
    }

    #[test]
    fn test_added_column() {
        // Table has 3 fields, data file only has the first 2
        let table_fields = vec![field(0, "a"), field(1, "b"), field(2, "c")];
        let data_fields = vec![field(0, "a"), field(1, "b")];
        assert_eq!(
            create_index_mapping(&table_fields, &data_fields),
            Some(vec![0, 1, -1])
        );
    }

    #[test]
    fn test_reordered_fields() {
        let table_fields = vec![field(1, "c"), field(6, "b"), field(3, "a")];
        let data_fields = vec![field(1, "a"), field(3, "c")];
        assert_eq!(
            create_index_mapping(&table_fields, &data_fields),
            Some(vec![0, -1, 1])
        );
    }

    #[test]
    fn test_renamed_column() {
        // Field ID stays the same even if name changed
        let table_fields = vec![field(0, "id"), field(1, "new_name")];
        let data_fields = vec![field(0, "id"), field(1, "old_name")];
        // Identity mapping since field IDs match positionally
        assert_eq!(create_index_mapping(&table_fields, &data_fields), None);
    }

    #[test]
    fn test_empty_data_fields() {
        let table_fields = vec![field(0, "a"), field(1, "b")];
        let data_fields: Vec<DataField> = vec![];
        assert_eq!(
            create_index_mapping(&table_fields, &data_fields),
            Some(vec![-1, -1])
        );
    }

    #[test]
    fn test_type_promotion_same_mapping() {
        // Type promotion doesn't affect index mapping — only field IDs matter
        let table_fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "name".to_string(),
                DataType::VarChar(VarCharType::string_type()),
            ),
        ];
        let data_fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "name".to_string(),
                DataType::VarChar(VarCharType::string_type()),
            ),
        ];
        assert_eq!(create_index_mapping(&table_fields, &data_fields), None);
    }

    #[test]
    fn test_unchanged_nested_type_removes_file_field_metadata() {
        let element_field = Arc::new(
            Field::new("element", ArrowDataType::Int32, true).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "1".to_string(),
            )])),
        );
        let array: ArrayRef = Arc::new(ListArray::new(
            element_field,
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2])),
            Arc::new(Int32Array::from(vec![1, 2])),
            None,
        ));
        let data_type = DataType::Array(ArrayType::new(DataType::Int(IntType::new())));

        let normalized = cast_array_for_schema_evolution(&array, &data_type, &data_type).unwrap();

        assert_eq!(
            normalized.data_type(),
            &paimon_type_to_arrow(&data_type).unwrap()
        );
        assert_eq!(
            normalized
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .values()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[1, 2]
        );
    }

    #[test]
    fn test_schema_evolution_cast_matrix() {
        let cases = [
            (
                DataType::Int(IntType::new()),
                DataType::BigInt(BigIntType::new()),
                true,
            ),
            (
                DataType::Double(DoubleType::new()),
                DataType::Decimal(DecimalType::new(12, 2).unwrap()),
                false,
            ),
            (
                DataType::Int(IntType::new()),
                DataType::Decimal(DecimalType::new(12, 2).unwrap()),
                true,
            ),
            (
                DataType::Decimal(DecimalType::new(12, 4).unwrap()),
                DataType::Decimal(DecimalType::new(10, 2).unwrap()),
                true,
            ),
            (
                DataType::Char(CharType::new(8).unwrap()),
                DataType::VarChar(VarCharType::new(4).unwrap()),
                true,
            ),
            (
                DataType::Binary(BinaryType::new(8).unwrap()),
                DataType::VarBinary(VarBinaryType::new(4).unwrap()),
                true,
            ),
            (
                DataType::Timestamp(TimestampType::new(3).unwrap()),
                DataType::Date(DateType::new()),
                true,
            ),
            (
                DataType::Date(DateType::new()),
                DataType::Timestamp(TimestampType::new(3).unwrap()),
                true,
            ),
            (
                DataType::Date(DateType::new()),
                DataType::Timestamp(TimestampType::new(6).unwrap()),
                false,
            ),
            (
                DataType::Timestamp(TimestampType::new(3).unwrap()),
                DataType::Timestamp(TimestampType::new(6).unwrap()),
                false,
            ),
            (
                DataType::Int(IntType::new()),
                DataType::VarChar(VarCharType::new(10).unwrap()),
                false,
            ),
            (
                DataType::Array(ArrayType::new(DataType::Int(IntType::new()))),
                DataType::Array(ArrayType::new(DataType::BigInt(BigIntType::new()))),
                false,
            ),
        ];

        for (source, target, expected) in cases {
            assert_eq!(
                schema_evolution_cast_implemented(&source, &target),
                expected,
                "unexpected executor availability for {source:?} -> {target:?}"
            );
        }
    }

    #[test]
    fn test_character_schema_evolution_casts() {
        let source: ArrayRef = Arc::new(StringArray::from(vec![
            Some("abcdef"),
            Some("é猫"),
            Some("x"),
            None,
        ]));
        let cases = [
            (
                DataType::Char(CharType::new(3).unwrap()),
                vec![Some("abc"), Some("é猫 "), Some("x  "), None],
            ),
            (
                DataType::VarChar(VarCharType::new(3).unwrap()),
                vec![Some("abc"), Some("é猫"), Some("x"), None],
            ),
        ];

        for (target, expected) in cases {
            let casted = cast_array_for_schema_evolution(
                &source,
                &DataType::VarChar(VarCharType::new(20).unwrap()),
                &target,
            )
            .unwrap();
            let casted = casted.as_any().downcast_ref::<StringArray>().unwrap();
            let actual = (0..casted.len())
                .map(|index| (!casted.is_null(index)).then(|| casted.value(index)))
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn test_storage_normalization_is_required_only_for_bounded_targets() {
        let cases = [
            (DataType::Char(CharType::new(5).unwrap()), true),
            (DataType::VarChar(VarCharType::new(5).unwrap()), true),
            (DataType::VarChar(VarCharType::string_type()), false),
            (DataType::Binary(BinaryType::new(5).unwrap()), true),
            (DataType::VarBinary(VarBinaryType::new(5).unwrap()), true),
            (
                DataType::VarBinary(VarBinaryType::new(VarBinaryType::MAX_LENGTH).unwrap()),
                false,
            ),
            (DataType::Int(IntType::new()), false),
        ];

        for (target, expected) in cases {
            assert_eq!(
                requires_schema_evolution_storage_normalization(&target),
                expected,
                "unexpected storage normalization requirement for {target:?}"
            );
        }
    }

    #[test]
    fn test_binary_schema_evolution_casts() {
        let source: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(&b"abcd"[..]),
            Some(&b"x"[..]),
            None,
        ]));
        let cases = [
            (
                DataType::Binary(BinaryType::new(3).unwrap()),
                vec![Some(&b"abc"[..]), Some(&b"x\0\0"[..]), None],
            ),
            (
                DataType::VarBinary(VarBinaryType::new(3).unwrap()),
                vec![Some(&b"abc"[..]), Some(&b"x"[..]), None],
            ),
        ];

        for (target, expected) in cases {
            let casted = cast_array_for_schema_evolution(
                &source,
                &DataType::VarBinary(VarBinaryType::new(20).unwrap()),
                &target,
            )
            .unwrap();
            let casted = casted.as_any().downcast_ref::<BinaryArray>().unwrap();
            let actual = (0..casted.len())
                .map(|index| (!casted.is_null(index)).then(|| casted.value(index)))
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn test_numeric_primitive_cast_uses_paimon_semantics() {
        let numeric_source: ArrayRef = Arc::new(Int32Array::from(vec![Some(7), None, Some(-4)]));
        let numeric = cast_array_for_schema_evolution(
            &numeric_source,
            &DataType::Int(IntType::new()),
            &DataType::BigInt(BigIntType::new()),
        )
        .unwrap();
        assert_eq!(
            numeric.as_any().downcast_ref::<Int64Array>().unwrap(),
            &Int64Array::from(vec![Some(7), None, Some(-4)])
        );

        let narrowing_source: ArrayRef =
            Arc::new(Int64Array::from(vec![Some(i64::from(i32::MAX) + 1), None]));
        let narrowing = cast_array_for_schema_evolution(
            &narrowing_source,
            &DataType::BigInt(BigIntType::new()),
            &DataType::Int(IntType::new()),
        )
        .unwrap();
        assert_eq!(
            narrowing.as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(vec![Some(i32::MIN), None])
        );

        let floating_source: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(3.9),
            Some(-3.9),
        ]));
        let floating = cast_array_for_schema_evolution(
            &floating_source,
            &DataType::Double(DoubleType::new()),
            &DataType::Int(IntType::new()),
        )
        .unwrap();
        assert_eq!(
            floating.as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(vec![
                Some(0),
                Some(i32::MAX),
                Some(i32::MIN),
                Some(3),
                Some(-3)
            ])
        );

        let float_narrowing_cases = [
            (
                Arc::new(Float32Array::from(vec![
                    1000.0,
                    -1000.0,
                    f32::NAN,
                    f32::INFINITY,
                    f32::NEG_INFINITY,
                    f32::MAX,
                    -f32::MAX,
                    127.0,
                    128.0,
                    32_767.0,
                    32_768.0,
                ])) as ArrayRef,
                DataType::Float(FloatType::new()),
            ),
            (
                Arc::new(Float64Array::from(vec![
                    1000.0,
                    -1000.0,
                    f64::NAN,
                    f64::INFINITY,
                    f64::NEG_INFINITY,
                    f64::MAX,
                    -f64::MAX,
                    127.0,
                    128.0,
                    32_767.0,
                    32_768.0,
                ])) as ArrayRef,
                DataType::Double(DoubleType::new()),
            ),
        ];
        for (source, source_type) in float_narrowing_cases {
            let tinyint = cast_array_for_schema_evolution(
                &source,
                &source_type,
                &DataType::TinyInt(TinyIntType::new()),
            )
            .unwrap();
            assert_eq!(
                tinyint.as_any().downcast_ref::<Int8Array>().unwrap(),
                &Int8Array::from(vec![-24, 24, 0, -1, 0, -1, 0, 127, -128, -1, 0]),
                "unexpected {source_type:?} to TINYINT result"
            );

            let smallint = cast_array_for_schema_evolution(
                &source,
                &source_type,
                &DataType::SmallInt(SmallIntType::new()),
            )
            .unwrap();
            assert_eq!(
                smallint.as_any().downcast_ref::<Int16Array>().unwrap(),
                &Int16Array::from(vec![
                    1000, -1000, 0, -1, 0, -1, 0, 127, 128, 32_767, -32_768,
                ]),
                "unexpected {source_type:?} to SMALLINT result"
            );
        }
    }

    #[test]
    fn test_decimal_cast_uses_paimon_rounding_and_overflow_semantics() {
        let decimal_source: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(1_235), Some(-1_235), Some(99_999), None])
                .with_precision_and_scale(5, 3)
                .unwrap(),
        );
        let decimal = cast_array_for_schema_evolution(
            &decimal_source,
            &DataType::Decimal(DecimalType::new(5, 3).unwrap()),
            &DataType::Decimal(DecimalType::new(4, 2).unwrap()),
        )
        .unwrap();
        assert_eq!(
            decimal.as_any().downcast_ref::<Decimal128Array>().unwrap(),
            &Decimal128Array::from(vec![Some(124), Some(-124), None, None])
                .with_precision_and_scale(4, 2)
                .unwrap()
        );

        let integer_source: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(12), None]));
        let decimal = cast_array_for_schema_evolution(
            &integer_source,
            &DataType::Int(IntType::new()),
            &DataType::Decimal(DecimalType::new(3, 2).unwrap()),
        )
        .unwrap();
        assert_eq!(
            decimal.as_any().downcast_ref::<Decimal128Array>().unwrap(),
            &Decimal128Array::from(vec![Some(100), None, None])
                .with_precision_and_scale(3, 2)
                .unwrap()
        );
    }

    #[test]
    fn test_date_timestamp_cast_uses_paimon_epoch_semantics() {
        let date_source: ArrayRef = Arc::new(Date32Array::from(vec![Some(1), None]));
        let timestamp_source: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![
            Some(-1),
            Some(-86_400_000),
            Some(-86_400_001),
            None,
        ]));
        for precision in [0, 3] {
            let timestamp = cast_array_for_schema_evolution(
                &date_source,
                &DataType::Date(DateType::new()),
                &DataType::Timestamp(TimestampType::new(precision).unwrap()),
            )
            .unwrap();
            assert_eq!(
                timestamp
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap(),
                &TimestampMillisecondArray::from(vec![Some(86_400_000), None]),
                "unexpected DATE to TIMESTAMP({precision}) result"
            );

            let date = cast_array_for_schema_evolution(
                &timestamp_source,
                &DataType::Timestamp(TimestampType::new(precision).unwrap()),
                &DataType::Date(DateType::new()),
            )
            .unwrap();
            assert_eq!(
                date.as_any().downcast_ref::<Date32Array>().unwrap(),
                &Date32Array::from(vec![Some(0), Some(-1), Some(-1), None]),
                "unexpected TIMESTAMP({precision}) to DATE result"
            );
        }
    }
}
