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

//! Convert equivalent Arrow write layouts to the table's canonical schema.
//!
//! PyArrow calls list children `item`; Paimon's Arrow schema calls them
//! `element`. Parquet persists child names, so accepting the input schema
//! without rebuilding the arrays would produce a file that later projections
//! cannot reliably read by the table schema. The buffers and values are shared;
//! only nested array wrappers are rebuilt.

use crate::{Error, Result};
use arrow_array::{
    Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, ListArray, MapArray, StructArray,
};
use arrow_schema::DataType;
use std::sync::Arc;

pub(super) fn normalize_write_array(array: &ArrayRef, expected: &DataType) -> Result<ArrayRef> {
    if array.data_type() == expected {
        return Ok(array.clone());
    }

    match (array.data_type(), expected) {
        (DataType::FixedSizeBinary(_), DataType::Binary) => {
            let fixed = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            Ok(Arc::new(BinaryArray::from_iter((0..fixed.len()).map(
                |row| (!fixed.is_null(row)).then(|| fixed.value(row)),
            ))))
        }
        (DataType::List(_), DataType::List(expected_child)) => {
            let list = array.as_any().downcast_ref::<ListArray>().unwrap();
            let values = normalize_write_array(list.values(), expected_child.data_type())?;
            let normalized = ListArray::try_new(
                expected_child.clone(),
                list.offsets().clone(),
                values,
                list.nulls().cloned(),
            )
            .map_err(normalization_error)?;
            Ok(Arc::new(normalized))
        }
        (DataType::Struct(actual_fields), DataType::Struct(expected_fields))
            if actual_fields.len() == expected_fields.len() =>
        {
            let row = array.as_any().downcast_ref::<StructArray>().unwrap();
            let columns = actual_fields
                .iter()
                .zip(expected_fields)
                .enumerate()
                .map(|(index, (actual, expected))| {
                    if actual.name() != expected.name() {
                        return Err(Error::DataInvalid {
                            message: format!(
                                "Nested ROW field name mismatch at index {index}: expected '{}', actual '{}'",
                                expected.name(),
                                actual.name()
                            ),
                            source: None,
                        });
                    }
                    normalize_write_array(row.column(index), expected.data_type())
                })
                .collect::<Result<Vec<_>>>()?;
            let normalized =
                StructArray::try_new(expected_fields.clone(), columns, row.nulls().cloned())
                    .map_err(normalization_error)?;
            Ok(Arc::new(normalized))
        }
        (DataType::Map(_, _), DataType::Map(expected_entries, expected_ordered)) => {
            let map = array.as_any().downcast_ref::<MapArray>().unwrap();
            let entries: ArrayRef = Arc::new(map.entries().clone());
            let entries = normalize_write_array(&entries, expected_entries.data_type())?;
            let entries = entries
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .clone();
            let normalized = MapArray::try_new(
                expected_entries.clone(),
                map.offsets().clone(),
                entries,
                map.nulls().cloned(),
                *expected_ordered,
            )
            .map_err(normalization_error)?;
            Ok(Arc::new(normalized))
        }
        _ => Err(Error::DataInvalid {
            message: format!(
                "Arrow write type {:?} is incompatible with table type {expected:?}",
                array.data_type()
            ),
            source: None,
        }),
    }
}

fn normalization_error(error: arrow_schema::ArrowError) -> Error {
    Error::DataInvalid {
        message: format!("Invalid nested Arrow write array: {error}"),
        source: Some(Box::new(error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use arrow_buffer::{OffsetBuffer, ScalarBuffer};
    use arrow_schema::Field;

    #[test]
    fn fixed_binary_conversion_preserves_nulls_and_bytes() {
        let input: ArrayRef = Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                [Some(b"ab".as_slice()), None, Some(b"cd".as_slice())].into_iter(),
                2,
            )
            .unwrap(),
        );
        let output = normalize_write_array(&input, &DataType::Binary).unwrap();
        let output = output.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(output.value(0), b"ab");
        assert!(output.is_null(1));
        assert_eq!(output.value(2), b"cd");
    }

    #[test]
    fn list_child_alias_changes_only_schema_and_keeps_values() {
        let input: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 3])),
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            None,
        ));
        let expected = DataType::List(Arc::new(Field::new("element", DataType::Int32, true)));
        let output = normalize_write_array(&input, &expected).unwrap();
        assert_eq!(output.data_type(), &expected);
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value(0)
                .len(),
            2
        );

        let wrong = DataType::List(Arc::new(Field::new("element", DataType::Utf8, true)));
        assert!(normalize_write_array(&input, &wrong).is_err());
    }

    #[test]
    fn row_child_names_cannot_be_silently_reordered() {
        let input: ArrayRef = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("right", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("left", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec!["x"])) as ArrayRef,
            ),
        ]));
        let expected = DataType::Struct(
            vec![
                Field::new("left", DataType::Int32, true),
                Field::new("right", DataType::Utf8, true),
            ]
            .into(),
        );
        let error = normalize_write_array(&input, &expected).unwrap_err();
        assert!(error.to_string().contains("Nested ROW field name mismatch"));
    }

    #[test]
    fn map_value_list_alias_is_normalized_recursively() {
        let key_field = Arc::new(Field::new("key", DataType::Int32, false));
        let value_field = Arc::new(Field::new(
            "value",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        ));
        let values: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 3])),
            Arc::new(Int32Array::from(vec![10, 20, 21])),
            None,
        ));
        let entries = StructArray::try_new(
            vec![key_field.clone(), value_field].into(),
            vec![Arc::new(Int32Array::from(vec![1, 2])), values],
            None,
        )
        .unwrap();
        let input: ArrayRef = Arc::new(
            MapArray::try_new(
                Arc::new(Field::new("entries", entries.data_type().clone(), false)),
                OffsetBuffer::new(ScalarBuffer::from(vec![0, 2])),
                entries,
                None,
                false,
            )
            .unwrap(),
        );

        let expected_value = Arc::new(Field::new(
            "value",
            DataType::List(Arc::new(Field::new("element", DataType::Int32, true))),
            true,
        ));
        let expected_entries = Arc::new(Field::new(
            "entries",
            DataType::Struct(vec![key_field, expected_value].into()),
            false,
        ));
        let expected = DataType::Map(expected_entries, false);
        let output = normalize_write_array(&input, &expected).unwrap();
        assert_eq!(output.data_type(), &expected);
        let map = output.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map.value_length(0), 2);
        let lists = map
            .entries()
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(lists.value(1).len(), 2);
    }
}
