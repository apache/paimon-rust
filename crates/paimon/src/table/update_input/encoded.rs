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

//! Rebuild selected encoded values after the whole-column safe cast has failed.

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, UInt64Array, UnionArray};
use arrow_schema::DataType;
use indexmap::IndexMap;

use super::invalid;

/// Decode references before constructor conversion, then restore NULL keys.
/// Unlike taking a nullable index directly from a Union, taking from the typed
/// output can represent NULL without assigning it an unrelated active type.
pub(super) fn construct_dictionary(
    array: &ArrayRef,
    target: &DataType,
    convert: fn(&ArrayRef, &DataType) -> crate::Result<ArrayRef>,
) -> crate::Result<ArrayRef> {
    let dictionary = array.as_any_dictionary();
    let keys = arrow_cast::cast(dictionary.keys(), &DataType::UInt64)
        .map_err(|error| invalid(error.to_string()))?;
    let keys = keys.as_any().downcast_ref::<UInt64Array>().unwrap();
    construct_selected(dictionary.values(), keys, target, convert)
}

/// Restore missing values only after conversion to a type with a validity
/// bitmap. Union inputs cannot represent a missing selection independently of
/// their active child, including below NULL parents or absent ROW fields.
pub(super) fn construct_selected(
    array: &ArrayRef,
    indices: &UInt64Array,
    target: &DataType,
    convert: fn(&ArrayRef, &DataType) -> crate::Result<ArrayRef>,
) -> crate::Result<ArrayRef> {
    let mut selected = Vec::new();
    let restore = indices
        .iter()
        .map(|key| {
            key.map(|key| {
                let index = selected.len() as u64;
                selected.push(key);
                index
            })
        })
        .collect::<Vec<_>>();
    if selected.is_empty() {
        return Ok(arrow_array::new_null_array(target, indices.len()));
    }
    let values = if let Some(union) = array.as_any().downcast_ref::<UnionArray>() {
        // Taking a sparse Union would also take every inactive child. Select
        // its active values directly, including through dictionary wrappers.
        construct_union_rows(
            union,
            selected.into_iter().map(|row| row as usize),
            target,
            convert,
        )?
    } else {
        let values = arrow_select::take::take(array.as_ref(), &UInt64Array::from(selected), None)
            .map_err(|error| invalid(error.to_string()))?;
        convert(&values, target)?
    };
    arrow_select::take::take(values.as_ref(), &UInt64Array::from(restore), None)
        .map_err(|error| invalid(error.to_string()))
}

/// Only active children participate in the Python-value constructor. A sparse
/// Union can contain invalid values at inactive positions; a dense Union can
/// select any child offset repeatedly or in a different order.
pub(super) fn construct_union(
    array: &ArrayRef,
    target: &DataType,
    convert: fn(&ArrayRef, &DataType) -> crate::Result<ArrayRef>,
) -> crate::Result<ArrayRef> {
    let input = array.as_any().downcast_ref::<UnionArray>().unwrap();
    construct_union_rows(input, 0..input.len(), target, convert)
}

fn construct_union_rows(
    input: &UnionArray,
    rows: impl ExactSizeIterator<Item = usize>,
    target: &DataType,
    convert: fn(&ArrayRef, &DataType) -> crate::Result<ArrayRef>,
) -> crate::Result<ArrayRef> {
    if rows.len() == 0 {
        return Ok(arrow_array::new_empty_array(target));
    }
    let mut selected = IndexMap::<i8, Vec<u64>>::new();
    let mut positions = Vec::with_capacity(rows.len());
    for row in rows {
        let entry = selected.entry(input.type_id(row));
        let child = entry.index();
        let offsets = entry.or_default();
        positions.push((child, offsets.len()));
        offsets.push(input.value_offset(row) as u64);
    }
    let values = selected
        .into_iter()
        .map(|(id, offsets)| {
            let selected = arrow_select::take::take(
                input.child(id).as_ref(),
                &UInt64Array::from(offsets),
                None,
            )
            .map_err(|error| invalid(error.to_string()))?;
            convert(&selected, target)
        })
        .collect::<crate::Result<Vec<_>>>()?;
    let values = values
        .iter()
        .map(|array| array.as_ref())
        .collect::<Vec<_>>();
    arrow_select::interleave::interleave(&values, &positions)
        .map_err(|error| invalid(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::super::{cast_update_value, CastMode};
    use super::*;
    use arrow_array::{
        types::Int8Type, DictionaryArray, FixedSizeListArray, Float64Array, Int32Array, Int64Array,
        Int8Array, ListArray, MapArray, NullArray, StringArray, StructArray,
    };
    use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use arrow_schema::{Field, UnionFields};
    use std::sync::Arc;

    fn numeric_union(dense: bool) -> ArrayRef {
        let fields = UnionFields::try_new(
            [7, 42, 99],
            [
                Field::new("small", DataType::Int32, true),
                Field::new("large", DataType::Int64, true),
                Field::new("unused", DataType::Utf8, true),
            ],
        )
        .unwrap();
        let (left, right, offsets) = if dense {
            (
                vec![Some(-1), None, Some(2)],
                vec![Some(3), Some(1), Some(i64::MAX)],
                Some(ScalarBuffer::from(vec![0, 1, 2, 0, 1, 2])),
            )
        } else {
            (
                vec![Some(-1), Some(0), Some(2), Some(0), None, Some(0)],
                vec![
                    Some(i64::MAX),
                    Some(1),
                    Some(i64::MAX),
                    Some(3),
                    Some(i64::MAX),
                    Some(i64::MAX),
                ],
                None,
            )
        };
        Arc::new(
            UnionArray::try_new(
                fields,
                ScalarBuffer::from(vec![7, 42, 7, 42, 7, 42]),
                offsets,
                vec![
                    Arc::new(Int32Array::from(left)),
                    Arc::new(Int64Array::from(right)),
                    Arc::new(StringArray::from(vec![
                        "invalid";
                        if dense { 1 } else { 6 }
                    ])),
                ],
            )
            .unwrap(),
        )
    }

    #[test]
    fn active_values_keep_slices_nulls_and_nonstandard_type_ids() {
        for dense in [true, false] {
            let input = numeric_union(dense);
            let slice = input.slice(1, 4);
            assert_eq!(slice.null_count(), 0);
            assert!(cast_update_value(&slice, &DataType::Int32, CastMode::Assignment).is_err());
            let result = cast_update_value(&slice, &DataType::Int32, CastMode::RowUpdate).unwrap();
            assert_eq!(
                result.to_data(),
                Int32Array::from(vec![Some(1), Some(2), Some(3), None]).to_data()
            );
            // The same wide value must fail as soon as its type/offset is active.
            assert!(cast_update_value(&input, &DataType::Int32, CastMode::RowUpdate).is_err());
            assert_eq!(
                cast_update_value(&slice.slice(0, 0), &DataType::Int32, CastMode::RowUpdate)
                    .unwrap()
                    .len(),
                0
            );
        }
    }

    #[test]
    fn dense_offsets_can_repeat_and_reorder_child_values() {
        let input = numeric_union(true);
        let fields = match input.data_type() {
            DataType::Union(fields, _) => fields.clone(),
            _ => unreachable!(),
        };
        let input = input.as_any().downcast_ref::<UnionArray>().unwrap();
        let reordered: ArrayRef = Arc::new(
            UnionArray::try_new(
                fields,
                ScalarBuffer::from(vec![7, 7, 42, 7]),
                Some(ScalarBuffer::from(vec![2, 0, 1, 2])),
                vec![
                    input.child(7).clone(),
                    input.child(42).clone(),
                    input.child(99).clone(),
                ],
            )
            .unwrap(),
        );
        assert_eq!(
            cast_update_value(&reordered, &DataType::Int32, CastMode::RowUpdate)
                .unwrap()
                .to_data(),
            Int32Array::from(vec![2, -1, 1, 2]).to_data(),
        );
    }

    #[test]
    fn every_selected_child_uses_constructor_semantics() {
        let fields = UnionFields::try_new(
            [3, 8],
            [
                Field::new("number", DataType::Float64, true),
                Field::new("text", DataType::Utf8, true),
            ],
        )
        .unwrap();
        let input: ArrayRef = Arc::new(
            UnionArray::try_new(
                fields,
                ScalarBuffer::from(vec![3, 8]),
                Some(ScalarBuffer::from(vec![0, 0])),
                vec![
                    Arc::new(Float64Array::from(vec![1.5])),
                    Arc::new(StringArray::from(vec!["42"])),
                ],
            )
            .unwrap(),
        );
        assert_eq!(
            cast_update_value(&input.slice(0, 1), &DataType::Int32, CastMode::RowUpdate)
                .unwrap()
                .to_data(),
            Int32Array::from(vec![1]).to_data(),
        );
        // A numeric text child must not get a fresh safe-cast attempt after
        // the Union's whole-column safe cast has already failed.
        assert!(cast_update_value(&input, &DataType::Int32, CastMode::RowUpdate).is_err());
        assert!(
            cast_update_value(&input.slice(0, 1), &DataType::Utf8, CastMode::RowUpdate).is_err()
        );
    }

    #[test]
    fn constructors_recurse_through_rows_and_arrays() {
        for dense in [true, false] {
            let input = numeric_union(dense).slice(1, 4);
            let rows: ArrayRef = Arc::new(StructArray::from(vec![(
                Arc::new(Field::new("n", input.data_type().clone(), true)),
                input.clone(),
            )]));
            let target = DataType::Struct(vec![Field::new("n", DataType::Int32, true)].into());
            let output = cast_update_value(&rows, &target, CastMode::RowUpdate).unwrap();
            let expected = Int32Array::from(vec![Some(1), Some(2), Some(3), None]);
            assert_eq!(
                output
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap()
                    .column(0)
                    .to_data(),
                expected.to_data()
            );
            let lists: ArrayRef = Arc::new(ListArray::new(
                Arc::new(Field::new("item", input.data_type().clone(), true)),
                OffsetBuffer::from_lengths([2, 2]),
                input,
                None,
            ));
            let target = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
            let output = cast_update_value(&lists, &target, CastMode::RowUpdate).unwrap();
            assert_eq!(
                output
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap()
                    .values()
                    .to_data(),
                expected.to_data()
            );
        }
    }

    fn pair_union() -> ArrayRef {
        let pairs: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::from_lengths([2]),
            Arc::new(Int32Array::from(vec![1, 2])),
            None,
        ));
        let pairs: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", pairs.data_type().clone(), true)),
            OffsetBuffer::from_lengths([1]),
            pairs,
            None,
        ));
        Arc::new(
            UnionArray::try_new(
                UnionFields::try_new(
                    [7, 42],
                    [
                        Field::new("pairs", pairs.data_type().clone(), true),
                        Field::new("empty", DataType::Null, true),
                    ],
                )
                .unwrap(),
                ScalarBuffer::from(vec![7, 42]),
                Some(ScalarBuffer::from(vec![0, 0])),
                vec![pairs, Arc::new(NullArray::new(1))],
            )
            .unwrap(),
        )
    }

    fn map_type(key: DataType) -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", key, false),
                        Field::new("value", DataType::Int64, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        )
    }

    #[test]
    fn map_pair_conversion_is_root_only_through_encoding_wrappers() {
        let input = pair_union();
        let target = map_type(DataType::Int32);
        let output = cast_update_value(&input, &target, CastMode::RowUpdate).unwrap();
        let output = output.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(output.value_offsets(), &[0, 1, 1]);
        assert!(output.is_null(1));
        assert_eq!(
            output.values().to_data(),
            Int64Array::from(vec![2]).to_data()
        );
        let encoded: ArrayRef = Arc::new(
            DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0, 1]), input.clone())
                .unwrap(),
        );
        assert_eq!(
            cast_update_value(&encoded, &target, CastMode::RowUpdate)
                .unwrap()
                .to_data(),
            output.to_data()
        );
        let nested: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("m", input.data_type().clone(), true)),
            input,
        )]));
        let target = DataType::Struct(vec![Field::new("m", target, true)].into());
        assert!(cast_update_value(&nested, &target, CastMode::RowUpdate).is_err());
    }

    #[test]
    fn dictionary_union_preserves_null_indices_and_active_null_values() {
        for dense in [true, false] {
            let values = numeric_union(dense).slice(1, 4);
            let input: ArrayRef = Arc::new(
                DictionaryArray::<Int8Type>::try_new(
                    Int8Array::from(vec![Some(3), None, Some(0), Some(1)]),
                    values.clone(),
                )
                .unwrap(),
            );
            let output = cast_update_value(&input, &DataType::Int32, CastMode::RowUpdate).unwrap();
            assert_eq!(
                output.to_data(),
                Int32Array::from(vec![None, None, Some(1), Some(2)]).to_data()
            );
            let input: ArrayRef = Arc::new(
                DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![None, None]), values)
                    .unwrap(),
            );
            assert_eq!(
                cast_update_value(&input, &DataType::Int32, CastMode::RowUpdate)
                    .unwrap()
                    .null_count(),
                2
            );
        }
        let input: ArrayRef = Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![None, Some(0), Some(1), None]),
                pair_union(),
            )
            .unwrap(),
        );
        let output =
            cast_update_value(&input, &map_type(DataType::Int32), CastMode::RowUpdate).unwrap();
        let output = output.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(output.value_offsets(), &[0, 0, 1, 1, 1]);
        assert_eq!(output.null_count(), 3);
    }

    #[test]
    fn union_dict_values_allow_explicit_null_map_values() {
        let row: ArrayRef = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("a", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("b", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![None])) as ArrayRef,
            ),
        ]));
        let target = map_type(DataType::Utf8);
        assert!(cast_update_value(&row, &target, CastMode::RowUpdate).is_err());
        let input: ArrayRef = Arc::new(
            UnionArray::try_new(
                UnionFields::try_new([42], [Field::new("row", row.data_type().clone(), true)])
                    .unwrap(),
                ScalarBuffer::from(vec![42]),
                Some(ScalarBuffer::from(vec![0])),
                vec![row],
            )
            .unwrap(),
        );
        let output = cast_update_value(&input, &target, CastMode::RowUpdate).unwrap();
        let output = output.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(
            output.keys().to_data(),
            StringArray::from(vec!["a", "b"]).to_data()
        );
        assert_eq!(
            output.values().to_data(),
            Int64Array::from(vec![Some(1), None]).to_data()
        );
    }

    #[test]
    fn null_parents_hide_union_values_before_constructor_conversion() {
        for dense in [false, true] {
            let input: ArrayRef = Arc::new(
                UnionArray::try_new(
                    UnionFields::try_new(
                        [7, 42],
                        [
                            Field::new("number", DataType::Int32, true),
                            Field::new("text", DataType::Utf8, true),
                        ],
                    )
                    .unwrap(),
                    ScalarBuffer::from(vec![42, 7, 7, 42]),
                    dense.then(|| ScalarBuffer::from(vec![0, 0, 1, 1])),
                    if dense {
                        vec![
                            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
                            Arc::new(StringArray::from(vec!["hidden", "also hidden"])),
                        ]
                    } else {
                        vec![
                            Arc::new(Int32Array::from(vec![0, 1, 2, 0])) as ArrayRef,
                            Arc::new(StringArray::from(vec!["hidden"; 4])),
                        ]
                    },
                )
                .unwrap(),
            );
            let dictionary: ArrayRef = Arc::new(
                DictionaryArray::<Int8Type>::try_new(
                    Int8Array::from(vec![0, 1, 2, 3]),
                    input.clone(),
                )
                .unwrap(),
            );
            for input in [input, dictionary] {
                let parents = NullBuffer::from(vec![false, true, true, false]);
                let rows: ArrayRef = Arc::new(StructArray::new(
                    vec![Field::new("n", input.data_type().clone(), true)].into(),
                    vec![input.clone()],
                    Some(parents.clone()),
                ));
                let target = DataType::Struct(vec![Field::new("n", DataType::Int32, true)].into());
                assert!(cast_update_value(&rows, &target, CastMode::Assignment).is_err());
                let output = cast_update_value(&rows, &target, CastMode::RowUpdate).unwrap();
                let output = output.as_struct();
                assert_eq!(output.nulls(), Some(&parents));
                assert_eq!(
                    output.column(0).to_data(),
                    Int32Array::from(vec![None, Some(1), Some(2), None]).to_data()
                );

                let lists: ArrayRef = Arc::new(FixedSizeListArray::new(
                    Arc::new(Field::new("item", input.data_type().clone(), true)),
                    1,
                    input,
                    Some(parents.clone()),
                ));
                let target = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
                let output = cast_update_value(&lists, &target, CastMode::RowUpdate).unwrap();
                let output = output.as_list::<i32>();
                assert_eq!(output.nulls(), Some(&parents));
                assert_eq!(
                    output.value(1).to_data(),
                    Int32Array::from(vec![1]).to_data()
                );
                assert_eq!(
                    output.value(2).to_data(),
                    Int32Array::from(vec![2]).to_data()
                );
            }
        }
    }

    #[test]
    fn map_union_values_leave_absent_row_fields_null() {
        for dense in [false, true] {
            let values: ArrayRef = Arc::new(
                UnionArray::try_new(
                    UnionFields::try_new([7], [Field::new("number", DataType::Int32, true)])
                        .unwrap(),
                    ScalarBuffer::from(vec![7; 4]),
                    dense.then(|| ScalarBuffer::from(vec![0, 1, 2, 3])),
                    vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4]))],
                )
                .unwrap(),
            );
            let entries = StructArray::new(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", values.data_type().clone(), true),
                ]
                .into(),
                vec![Arc::new(StringArray::from(vec!["n"; 4])), values],
                None,
            );
            let input: ArrayRef = Arc::new(MapArray::new(
                Arc::new(Field::new("entries", entries.data_type().clone(), false)),
                OffsetBuffer::from_lengths([1; 4]),
                entries,
                None,
                false,
            ));
            let target = DataType::Struct(
                vec![
                    Field::new("n", DataType::Int32, true),
                    Field::new("v", DataType::Utf8, true),
                ]
                .into(),
            );
            let output = cast_update_value(&input, &target, CastMode::RowUpdate).unwrap();
            let output = output.as_struct();
            assert_eq!(
                output.column(0).to_data(),
                Int32Array::from(vec![1, 2, 3, 4]).to_data()
            );
            assert_eq!(
                output.column(1).to_data(),
                StringArray::from(vec![None::<&str>; 4]).to_data()
            );
        }
    }
}
