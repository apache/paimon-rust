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

use super::*;
use arrow_array::{types::Int8Type, DictionaryArray, Int32Array, Int8Array};
use arrow_buffer::NullBuffer;
use arrow_schema::Field;

fn map(keys: ArrayRef, values: ArrayRef, lengths: &[usize], valid: &[bool]) -> ArrayRef {
    let entries = StructArray::from(vec![
        (
            Arc::new(Field::new("encoded_key", keys.data_type().clone(), false)),
            keys,
        ),
        (
            Arc::new(Field::new(
                "encoded_value",
                values.data_type().clone(),
                true,
            )),
            values,
        ),
    ]);
    Arc::new(MapArray::new(
        Arc::new(Field::new("pairs", entries.data_type().clone(), false)),
        OffsetBuffer::from_lengths(lengths.iter().copied()),
        entries,
        Some(NullBuffer::from(valid.to_vec())),
        false,
    ))
}

fn pair_type(key: DataType, value: DataType) -> DataType {
    DataType::Struct(
        vec![
            Field::new("label", key, true),
            Field::new("amount", value, true),
        ]
        .into(),
    )
}

#[test]
fn map_to_array_rows_preserves_aliases_slices_empty_and_null_values() {
    let input = map(
        Arc::new(StringArray::from(vec!["unused", "a", "b", "z"])),
        Arc::new(Int32Array::from(vec![Some(-1), Some(1), None, Some(4)])),
        &[1, 2, 0, 0, 1],
        &[true, true, false, true, true],
    )
    .slice(1, 4);
    let fields = pair_type(DataType::Utf8, DataType::Int64);
    let item = Arc::new(Field::new("element", fields.clone(), false));
    for target in [
        DataType::List(item.clone()),
        DataType::LargeList(item.clone()),
    ] {
        let output = cast_update_value(&input, &target, CastMode::Assignment).unwrap();
        assert_eq!(output.data_type(), &target);
        assert!(output.is_null(1));
        // Normalize only the container to inspect both offset widths.
        let output = arrow_cast::cast(output.as_ref(), &DataType::List(item.clone())).unwrap();
        let output = output.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(output.value_offsets(), &[0, 2, 2, 2, 3]);
        let entries = output
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(entries.data_type(), &fields);
        assert_eq!(
            entries.column(0).to_data(),
            StringArray::from(vec!["a", "b", "z"]).to_data()
        );
        assert_eq!(
            entries.column(1).to_data(),
            Int64Array::from(vec![Some(1), None, Some(4)]).to_data()
        );
    }
}

#[test]
fn map_to_array_recurses_through_rows_lists_and_dictionaries() {
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![0, 1]),
            Arc::new(StringArray::from(vec!["42", "7"])),
        )
        .unwrap(),
    );
    let input = map(
        Arc::new(StringArray::from(vec!["a", "b"])),
        dictionary,
        &[1, 1],
        &[true, true],
    );
    let target = DataType::List(Arc::new(Field::new(
        "item",
        pair_type(DataType::Utf8, DataType::Int32),
        true,
    )));
    let expected = cast_update_value(&input, &target, CastMode::Assignment).unwrap();
    let outer_row: ArrayRef = Arc::new(StructArray::from(vec![(
        Arc::new(Field::new("nested", input.data_type().clone(), true)),
        input.clone(),
    )]));
    let result = cast_update_value(
        &outer_row,
        &DataType::Struct(vec![Field::new("nested", target.clone(), true)].into()),
        CastMode::Assignment,
    )
    .unwrap();
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .column(0)
            .to_data(),
        expected.to_data()
    );
    let outer_list: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", input.data_type().clone(), true)),
        OffsetBuffer::from_lengths([2]),
        input.clone(),
        None,
    ));
    let result = cast_update_value(
        &outer_list,
        &DataType::List(Arc::new(Field::new("item", target.clone(), true))),
        CastMode::Assignment,
    )
    .unwrap();
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .values()
            .to_data(),
        expected.to_data()
    );
    let encoded: ArrayRef =
        Arc::new(DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0, 1]), input).unwrap());
    assert_eq!(
        cast_update_value(&encoded, &target, CastMode::Assignment)
            .unwrap()
            .to_data(),
        expected.to_data()
    );
}

#[test]
fn map_to_array_keeps_whole_column_constructor_visibility() {
    let input = map(
        Arc::new(StringArray::from(vec!["a", "hidden"])),
        Arc::new(Int64Array::from(vec![7, i64::MAX])),
        &[1, 1, 0],
        &[true, false, true],
    );
    let target = DataType::List(Arc::new(Field::new(
        "item",
        pair_type(DataType::Utf8, DataType::Int32),
        true,
    )));
    assert!(cast_update_value(&input, &target, CastMode::Assignment).is_err());
    let output = cast_update_value(&input, &target, CastMode::RowUpdate).unwrap();
    let output = output.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(output.value_offsets(), &[0, 1, 1, 1]);
    assert!(output.is_null(1));
    let entries = output
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(
        entries.column(1).to_data(),
        Int32Array::from(vec![7]).to_data()
    );
    let visible = map(
        Arc::new(StringArray::from(vec!["bad"])),
        Arc::new(Int64Array::from(vec![i64::MAX])),
        &[1],
        &[true],
    );
    assert!(cast_update_value(&visible, &target, CastMode::RowUpdate).is_err());
}

#[test]
fn map_entries_require_two_fields_and_actual_child_nullability() {
    let input = map(
        Arc::new(StringArray::from(vec!["a"])),
        Arc::new(Int32Array::from(vec![7])),
        &[1],
        &[true],
    );
    for size in [0, 1, 3] {
        let fields = (0..size)
            .map(|i| Field::new(format!("f{i}"), DataType::Utf8, true))
            .collect::<Vec<_>>();
        let target = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(fields.into()),
            true,
        )));
        assert!(cast_update_value(&input, &target, CastMode::Assignment).is_err());
    }
    let fields = vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("v", DataType::Int32, false),
    ];
    let target = DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(fields.into()),
        false,
    )));
    // A nullable source value FIELD with non-null actual values can be cast.
    assert!(cast_update_value(&input, &target, CastMode::Assignment).is_ok());
    let null_value = map(
        Arc::new(StringArray::from(vec!["a"])),
        Arc::new(Int32Array::from(vec![None])),
        &[1],
        &[true],
    );
    assert!(cast_update_value(&null_value, &target, CastMode::Assignment).is_err());
}
