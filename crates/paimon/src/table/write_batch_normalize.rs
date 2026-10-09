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
    Array, ArrayRef, BinaryArray, Decimal128Array, FixedSizeBinaryArray, ListArray, MapArray,
    StringArray, StructArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field};
use std::sync::Arc;

pub(super) fn normalize_write_array(array: &ArrayRef, expected: &DataType) -> Result<ArrayRef> {
    normalize_visible_array(array, expected, None)
}

/// Java Blob maps allow a NULL key. Arrow Map does not, so dedicated Blob
/// writers use a list of key/value structs as their physical input. This
/// representation must never be passed to an ordinary Parquet MAP writer.
pub(super) fn blob_map_row_type(expected: &DataType) -> Option<DataType> {
    let DataType::Map(entries, _) = expected else {
        return None;
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return None;
    };
    if fields.len() != 2 {
        return None;
    }
    let fields = vec![
        Arc::new(fields[0].as_ref().clone().with_nullable(true)),
        fields[1].clone(),
    ];
    Some(DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(fields.into()),
        false,
    ))))
}

pub(super) fn normalize_blob_write_field(
    array: &ArrayRef,
    expected: &Field,
) -> Result<(Field, ArrayRef)> {
    let Some(target) = blob_map_row_type(expected.data_type()) else {
        return Ok((
            expected.clone(),
            normalize_write_array(array, expected.data_type())?,
        ));
    };
    let (entries, offsets, nulls) = match array.data_type() {
        DataType::Map(_, _) => {
            let map = array.as_any().downcast_ref::<MapArray>().unwrap();
            (
                Arc::new(map.entries().clone()) as ArrayRef,
                map.offsets(),
                map.nulls(),
            )
        }
        DataType::List(_) => {
            let list = array.as_any().downcast_ref::<ListArray>().unwrap();
            (list.values().clone(), list.offsets(), list.nulls())
        }
        _ => {
            return Err(Error::DataInvalid {
                message: "MAP<X, BLOB> input must be a map or a list of key/value structs".into(),
                source: None,
            })
        }
    };
    let (entries, offsets) = visible_children(&entries, offsets, nulls)?;
    let entries = entries
        .as_any()
        .downcast_ref::<StructArray>()
        .filter(|entries| entries.num_columns() == 2)
        .ok_or_else(|| Error::DataInvalid {
            message: "MAP<X, BLOB> entries must contain exactly a key and a value".into(),
            source: None,
        })?;
    if entries.null_count() != 0 {
        return Err(Error::DataInvalid {
            message: "MAP<X, BLOB> entries cannot be NULL".into(),
            source: None,
        });
    }
    let DataType::List(child) = &target else {
        unreachable!()
    };
    let DataType::Struct(fields) = child.data_type() else {
        unreachable!()
    };
    // Key/value roles are positional, like ordinary Arrow MAP aliases.
    let columns = entries
        .columns()
        .iter()
        .zip(fields)
        .enumerate()
        .map(|(index, (column, field))| {
            if index == 0 {
                normalize_blob_map_key(column, field.data_type())
            } else {
                normalize_write_array(column, field.data_type())
            }
        })
        .collect::<Result<Vec<_>>>()?;
    let entries =
        StructArray::try_new(fields.clone(), columns, None).map_err(normalization_error)?;
    let list = ListArray::try_new(child.clone(), offsets, Arc::new(entries), nulls.cloned())
        .map_err(normalization_error)?;
    Ok((expected.clone().with_data_type(target), Arc::new(list)))
}

fn normalize_blob_map_key(array: &ArrayRef, expected: &DataType) -> Result<ArrayRef> {
    // Row bridges transport arbitrary-scale decimals as plain text. Arrow's
    // declared-scale builder rejects these before Java HALF_UP can be applied.
    if let (DataType::Utf8, DataType::Decimal128(precision, scale)) = (array.data_type(), expected)
    {
        let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
        let values = strings
            .iter()
            .map(|value| {
                value
                    .map(|text| {
                        decimal_blob_key(text, *precision, *scale).ok_or_else(|| {
                            Error::DataInvalid {
                                message: "MAP DECIMAL key is invalid or exceeds declared precision"
                                    .into(),
                                source: None,
                            }
                        })
                    })
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok(Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(*precision, *scale)
                .map_err(normalization_error)?,
        ));
    }
    normalize_write_array(array, expected)
}

/// Decimal.fromBigDecimal: round magnitude HALF_UP, then check precision.
/// Discarded fractional digits are never accumulated into i128, so source
/// decimals can have more digits than the target's maximum precision of 38.
fn decimal_blob_key(text: &str, precision: u8, scale: i8) -> Option<i128> {
    let scale = usize::try_from(scale).ok()?;
    let (negative, text) = match text.strip_prefix('-') {
        Some(text) => (true, text),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    if whole.is_empty() && fraction.is_empty()
        || !whole
            .bytes()
            .chain(fraction.bytes())
            .all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let whole = whole.trim_start_matches('0');
    if whole.len() > usize::from(precision) {
        return None;
    }
    let mut value = 0i128;
    for digit in whole.bytes().chain(fraction.bytes().take(scale)) {
        value = value
            .checked_mul(10)?
            .checked_add(i128::from(digit - b'0'))?;
    }
    for _ in fraction.len()..scale {
        value = value.checked_mul(10)?;
    }
    if fraction
        .as_bytes()
        .get(scale)
        .is_some_and(|digit| *digit >= b'5')
    {
        value = value.checked_add(1)?;
    }
    if value.checked_ilog10().map_or(1, |log| log + 1) > u32::from(precision) {
        return None;
    }
    Some(if negative { -value } else { value })
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

    fn blob_map_field(nullable_value: bool) -> Field {
        Field::new(
            "payload",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::LargeBinary, nullable_value),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        )
    }

    fn blob_map_rows(
        keys: Vec<Option<&str>>,
        values: Vec<Option<&[u8]>>,
        offsets: Vec<i32>,
        nulls: Option<NullBuffer>,
        entry_nulls: Option<NullBuffer>,
    ) -> ArrayRef {
        let entries = StructArray::new(
            vec![
                Field::new("source_key", DataType::Utf8, true),
                Field::new("source_value", DataType::LargeBinary, true),
            ]
            .into(),
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(LargeBinaryArray::from(values)),
            ],
            entry_nulls,
        );
        Arc::new(ListArray::new(
            Arc::new(Field::new("input", entries.data_type().clone(), true)),
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Arc::new(entries),
            nulls,
        ))
    }

    #[test]
    fn blob_row_map_preserves_null_keys_empty_maps_and_parent_nulls() {
        let input = blob_map_rows(
            vec![None, Some("named")],
            vec![Some(b"value"), None],
            vec![0, 2, 2, 2],
            Some(NullBuffer::from(vec![true, true, false])),
            None,
        );
        let (field, output) = normalize_blob_write_field(&input, &blob_map_field(true)).unwrap();
        output.to_data().validate_full().unwrap();
        assert_eq!(field.data_type(), output.data_type());
        let lists = output.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(lists.value_length(0), 2);
        assert_eq!(lists.value_length(1), 0);
        assert!(lists.is_null(2));
        let entries = lists.value(0);
        let entries = entries.as_any().downcast_ref::<StructArray>().unwrap();
        let keys = entries
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = entries
            .column(1)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert!(keys.is_null(0));
        assert_eq!(keys.value(1), "named");
        assert_eq!(values.value(0), b"value");
        assert!(values.is_null(1));
    }

    #[test]
    fn blob_map_hidden_entries_do_not_fail_visible_not_null_values() {
        let input = blob_map_rows(
            vec![None, Some("visible")],
            vec![None, Some(b"value")],
            vec![0, 1, 2],
            Some(NullBuffer::from(vec![false, true])),
            Some(NullBuffer::from(vec![false, true])),
        );
        let (_, output) = normalize_blob_write_field(&input, &blob_map_field(false)).unwrap();
        output.to_data().validate_full().unwrap();
        let lists = output.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(lists.is_null(0));
        assert_eq!(lists.value_length(0), 0);
        assert_eq!(lists.value_length(1), 1);
        let entries = lists.value(1);
        let entries = entries.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(
            entries
                .column(1)
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .value(0),
            b"value"
        );
    }

    #[test]
    fn blob_map_visible_null_entries_and_not_null_values_are_rejected() {
        for entry_nulls in [None, Some(NullBuffer::from(vec![false]))] {
            let input = blob_map_rows(vec![Some("key")], vec![None], vec![0, 1], None, entry_nulls);
            assert!(normalize_blob_write_field(&input, &blob_map_field(false)).is_err());
        }
    }

    #[test]
    fn blob_map_entry_aliases_follow_positional_roles() {
        let input = blob_map_rows(
            vec![Some("key")],
            vec![Some(b"value")],
            vec![0, 1],
            None,
            None,
        );
        let expected = blob_map_field(true);
        let entries = input.as_any().downcast_ref::<ListArray>().unwrap().value(0);
        let entries = entries.as_any().downcast_ref::<StructArray>().unwrap();
        let fields = vec![
            Field::new("other_key", DataType::Utf8, false),
            Field::new("other_value", DataType::LargeBinary, true),
        ]
        .into();
        let entries = StructArray::new(fields, entries.columns().to_vec(), None);
        let map: ArrayRef = Arc::new(MapArray::new(
            Arc::new(Field::new(
                "aliased_entries",
                entries.data_type().clone(),
                false,
            )),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
            entries,
            None,
            false,
        ));
        let (_, row_input) = normalize_blob_write_field(&input, &expected).unwrap();
        let (_, arrow_input) = normalize_blob_write_field(&map, &expected).unwrap();
        assert_eq!(row_input.to_data(), arrow_input.to_data());
    }

    #[test]
    fn ordinary_map_writes_cannot_accept_blob_row_transport() {
        let input = blob_map_rows(vec![None], vec![Some(b"value")], vec![0, 1], None, None);
        assert!(normalize_write_array(&input, blob_map_field(true).data_type()).is_err());
    }

    #[test]
    fn decimal_blob_map_keys_round_half_up_and_check_precision() {
        for (text, expected) in [
            ("12.345", 1235),
            ("-12.345", -1235),
            ("12.344", 1234),
            ("+00012.3", 1230),
            (".005", 1),
            ("-.005", -1),
            ("0.004999999999999999999999999999999999999999999", 0),
            ("9.999999999999999999999999999999999999999999999", 1000),
        ] {
            assert_eq!(decimal_blob_key(text, 10, 2), Some(expected), "{text}");
        }
        for text in [
            "",
            ".",
            "12..3",
            " 1",
            "1e2",
            "99999999.995",
            "-99999999.995",
        ] {
            assert_eq!(decimal_blob_key(text, 10, 2), None, "{text}");
        }
        assert_eq!(
            decimal_blob_key("12345678901234567890.1234567890123456785", 38, 18),
            Some(12345678901234567890123456789012345679)
        );
    }

    #[test]
    fn decimal_blob_map_transport_preserves_null_and_prunes_hidden_keys() {
        let input = blob_map_rows(
            vec![Some("99999999.995"), Some("12.345"), None],
            vec![Some(b"hidden"), Some(b"rounded"), Some(b"null key")],
            vec![0, 1, 3],
            Some(NullBuffer::from(vec![false, true])),
            None,
        );
        let expected = Field::new(
            "payload",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Decimal128(10, 2), false),
                            Field::new("value", DataType::LargeBinary, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        );
        let (_, output) = normalize_blob_write_field(&input, &expected).unwrap();
        let lists = output.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(lists.is_null(0));
        let values = lists.value(1);
        let entries = values.as_any().downcast_ref::<StructArray>().unwrap();
        let keys = entries
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(keys.value(0), 1235);
        assert!(keys.is_null(1));
        assert_eq!(keys.data_type(), &DataType::Decimal128(10, 2));
        let visible_overflow = blob_map_rows(
            vec![Some("99999999.995")],
            vec![Some(b"value")],
            vec![0, 1],
            None,
            None,
        );
        assert!(normalize_blob_write_field(&visible_overflow, &expected).is_err());
    }

    #[test]
    fn sliced_blob_map_inputs_drop_unreferenced_entries() {
        let input = blob_map_rows(
            vec![Some("before"), None, Some("after")],
            vec![Some(b"before"), Some(b"value"), Some(b"after")],
            vec![0, 1, 2, 3],
            None,
            None,
        );
        let (_, output) =
            normalize_blob_write_field(&input.slice(1, 1), &blob_map_field(true)).unwrap();
        let lists = output.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(lists.offsets().as_ref(), &[0, 1]);
        let entries = lists.value(0);
        let entries = entries.as_any().downcast_ref::<StructArray>().unwrap();
        assert!(entries.column(0).is_null(0));
        assert_eq!(
            entries
                .column(1)
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .value(0),
            b"value"
        );
    }

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
