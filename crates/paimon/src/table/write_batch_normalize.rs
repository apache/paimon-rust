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
//! cannot reliably read by the table schema. Ordinary inputs share their
//! payload buffers; hidden children of NULL parents may require compaction.

use crate::{Error, Result};
use arrow_array::{
    Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, ListArray, MapArray, StructArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::DataType;
use std::sync::Arc;

pub(super) fn normalize_write_array(array: &ArrayRef, expected: &DataType) -> Result<ArrayRef> {
    normalize_visible_array(array, expected, None)
}

fn normalize_visible_array(
    array: &ArrayRef,
    expected: &DataType,
    parent_nulls: Option<&NullBuffer>,
) -> Result<ArrayRef> {
    // Foreign Arrow arrays can declare non-nullable nested fields without
    // satisfying them. Rebuild nested wrappers even when their schema matches
    // so the Arrow constructors validate the actual children before writing.
    if array.data_type() == expected
        && !matches!(
            expected,
            DataType::List(_) | DataType::Struct(_) | DataType::Map(_, _)
        )
    {
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
            let nulls = NullBuffer::union(list.nulls(), parent_nulls);
            let (values, offsets) =
                visible_children(list.values(), list.offsets(), nulls.as_ref())?;
            let values = normalize_write_array(&values, expected_child.data_type())?;
            let normalized = ListArray::try_new(expected_child.clone(), offsets, values, nulls)
                .map_err(normalization_error)?;
            Ok(Arc::new(normalized))
        }
        (DataType::Struct(actual_fields), DataType::Struct(expected_fields))
            if actual_fields.len() == expected_fields.len() =>
        {
            let row = array.as_any().downcast_ref::<StructArray>().unwrap();
            let nulls = NullBuffer::union(row.nulls(), parent_nulls);
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
                    normalize_visible_array(row.column(index), expected.data_type(), nulls.as_ref())
                })
                .collect::<Result<Vec<_>>>()?;
            let normalized = StructArray::try_new_with_length(
                expected_fields.clone(),
                columns,
                nulls,
                row.len(),
            )
            .map_err(normalization_error)?;
            Ok(Arc::new(normalized))
        }
        (DataType::Map(_, _), DataType::Map(expected_entries, expected_ordered)) => {
            let map = array.as_any().downcast_ref::<MapArray>().unwrap();
            let nulls = NullBuffer::union(map.nulls(), parent_nulls);
            let entries: ArrayRef = Arc::new(map.entries().clone());
            let (entries, offsets) = visible_children(&entries, map.offsets(), nulls.as_ref())?;
            let entries = entries.as_any().downcast_ref::<StructArray>().unwrap();
            // MAP key/value roles are positional. Their Arrow field names are
            // aliases, unlike user-defined ROW field names.
            let DataType::Struct(fields) = expected_entries.data_type() else {
                return Err(Error::DataInvalid {
                    message: "MAP entries must have a struct type".into(),
                    source: None,
                });
            };
            let columns = entries
                .columns()
                .iter()
                .zip(fields)
                .map(|(column, field)| normalize_write_array(column, field.data_type()))
                .collect::<Result<Vec<_>>>()?;
            let entries =
                StructArray::try_new(fields.clone(), columns, None).map_err(normalization_error)?;
            let normalized = MapArray::try_new(
                expected_entries.clone(),
                offsets,
                entries,
                nulls,
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

/// Keep only children referenced by visible parents. Ordinary inputs and
/// slices share their payload buffers; only disjoint ranges across NULL
/// parents need concatenation. Hidden children must not fail NOT NULL checks
/// or resolve descriptors which Java would never visit.
fn visible_children(
    values: &ArrayRef,
    offsets: &OffsetBuffer<i32>,
    nulls: Option<&NullBuffer>,
) -> Result<(ArrayRef, OffsetBuffer<i32>)> {
    if nulls.is_none_or(|nulls| nulls.null_count() == 0) {
        let start = offsets[0];
        let end = offsets[offsets.len() - 1];
        let normalized_offsets = if start == 0 {
            offsets.clone()
        } else {
            OffsetBuffer::new(ScalarBuffer::from(
                offsets
                    .iter()
                    .map(|offset| offset - start)
                    .collect::<Vec<_>>(),
            ))
        };
        return Ok((
            values.slice(start as usize, (end - start) as usize),
            normalized_offsets,
        ));
    }
    let mut ranges: Vec<std::ops::Range<usize>> = Vec::new();
    let mut normalized_offsets = Vec::with_capacity(offsets.len());
    normalized_offsets.push(0);
    let mut length = 0i32;
    for (row, pair) in offsets.windows(2).enumerate() {
        if !nulls.is_some_and(|nulls| nulls.is_null(row)) {
            let start = pair[0] as usize;
            let end = pair[1] as usize;
            if start != end {
                if let Some(previous) = ranges.last_mut().filter(|range| range.end == start) {
                    previous.end = end;
                } else {
                    ranges.push(start..end);
                }
                length += pair[1] - pair[0];
            }
        }
        normalized_offsets.push(length);
    }
    let children = match ranges.as_slice() {
        [] => values.slice(0, 0),
        [range] => values.slice(range.start, range.len()),
        ranges => {
            let slices: Vec<_> = ranges
                .iter()
                .map(|range| values.slice(range.start, range.len()))
                .collect();
            let arrays: Vec<_> = slices.iter().map(|array| array.as_ref()).collect();
            arrow_select::concat::concat(&arrays).map_err(normalization_error)?
        }
    };
    Ok((
        children,
        OffsetBuffer::new(ScalarBuffer::from(normalized_offsets)),
    ))
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
    use arrow_array::{Int32Array, LargeBinaryArray, StringArray};
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
        let key_field = Arc::new(Field::new("source_key", DataType::Int32, false));
        let value_field = Arc::new(Field::new(
            "source_value",
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
            DataType::Struct(
                vec![
                    Arc::new(Field::new("key", DataType::Int32, false)),
                    expected_value,
                ]
                .into(),
            ),
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

    #[test]
    fn matching_schemas_cannot_bypass_nested_map_value_nullability() {
        let fields = vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::LargeBinary, false),
        ]
        .into();
        // PyArrow can pass a MAP through FFI with a NOT NULL value field but
        // actual null values. Simulate that input without changing its schema.
        let entries = unsafe {
            StructArray::new_unchecked(
                fields,
                vec![
                    Arc::new(StringArray::from(vec!["key"])),
                    Arc::new(LargeBinaryArray::from(vec![None::<&[u8]>])),
                ],
                None,
            )
        };
        let map: ArrayRef = Arc::new(
            MapArray::try_new(
                Arc::new(Field::new("entries", entries.data_type().clone(), false)),
                OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
                entries,
                None,
                false,
            )
            .unwrap(),
        );
        let row: ArrayRef = Arc::new(StructArray::new(
            vec![Field::new("payload", map.data_type().clone(), true)].into(),
            vec![map.clone()],
            None,
        ));
        let list: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("element", map.data_type().clone(), true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
            map.clone(),
            None,
        ));
        for input in [map, row, list] {
            let error = normalize_write_array(&input, input.data_type()).unwrap_err();
            assert!(error.to_string().contains("non-nullable"));
        }
    }

    #[test]
    fn matching_empty_row_schema_keeps_its_length_and_nulls() {
        let input: ArrayRef = Arc::new(StructArray::new_empty_fields(
            3,
            Some(arrow_buffer::NullBuffer::from(vec![true, false, true])),
        ));
        let output = normalize_write_array(&input, input.data_type()).unwrap();
        assert_eq!(output.len(), 3);
        assert_eq!(output.nulls(), input.nulls());
    }

    #[test]
    fn hidden_null_children_do_not_violate_visible_collection_constraints() {
        let values: ArrayRef = Arc::new(LargeBinaryArray::from(vec![
            None,
            Some(b"one".as_slice()),
            None,
            Some(b"two".as_slice()),
        ]));
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 2, 3, 4]));
        let nulls = Some(NullBuffer::from(vec![false, true, false, true]));
        let list: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::LargeBinary, true)),
            offsets.clone(),
            values.clone(),
            nulls.clone(),
        ));
        let expected_list = DataType::List(Arc::new(Field::new(
            "element",
            DataType::LargeBinary,
            false,
        )));
        let entries = StructArray::new(
            vec![
                Field::new("source_key", DataType::Utf8, false),
                Field::new("source_value", DataType::LargeBinary, true),
            ]
            .into(),
            vec![
                Arc::new(StringArray::from(vec!["hidden", "first", "hidden", "last"])),
                values,
            ],
            None,
        );
        let map: ArrayRef = Arc::new(MapArray::new(
            Arc::new(Field::new(
                "source_entries",
                entries.data_type().clone(),
                false,
            )),
            offsets,
            entries,
            nulls,
            false,
        ));
        let expected_map = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::LargeBinary, false),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        for (input, expected) in [(list, expected_list), (map, expected_map)] {
            for input in [input.clone(), input.slice(1, 1)] {
                let output = normalize_write_array(&input, &expected).unwrap();
                assert_eq!(output.data_type(), &expected);
                assert_eq!(output.len(), input.len());
                assert_eq!(output.nulls(), input.nulls());
                let data = output.to_data();
                data.validate_full().unwrap();
            }
        }
    }

    #[test]
    fn ancestor_row_nulls_mask_nested_non_nullable_fields() {
        let inner: ArrayRef = Arc::new(StructArray::new(
            vec![Field::new("value", DataType::LargeBinary, true)].into(),
            vec![Arc::new(LargeBinaryArray::from(vec![
                None,
                Some(b"ok".as_slice()),
            ]))],
            None,
        ));
        let input: ArrayRef = Arc::new(StructArray::new(
            vec![Field::new("inner", inner.data_type().clone(), false)].into(),
            vec![inner],
            Some(NullBuffer::from(vec![false, true])),
        ));
        let expected = DataType::Struct(
            vec![Field::new(
                "inner",
                DataType::Struct(vec![Field::new("value", DataType::LargeBinary, false)].into()),
                false,
            )]
            .into(),
        );
        let output = normalize_write_array(&input, &expected).unwrap();
        output.to_data().validate_full().unwrap();
        assert_eq!(output.len(), 2);
        assert!(output.is_null(0));
        assert!(output.is_valid(1));
    }
}
