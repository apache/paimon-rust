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
    Array, ArrayRef, BinaryArray, Decimal128Array, FixedSizeBinaryArray, FixedSizeListArray,
    LargeBinaryArray, LargeStringArray, ListArray, MapArray, StringArray, StructArray,
};
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field};
use std::sync::Arc;

/// Normalize equivalent Arrow layouts for writing, validating NOT NULL children
/// only in values visible through non-NULL parents.
pub fn normalize_write_array(array: &ArrayRef, expected: &DataType) -> Result<ArrayRef> {
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
    normalize_blob_field(array, expected, true)
}

/// Normalize Blob map keys and their physical transport, retaining the value
/// input type for the row-ID updater's conversion at the existing read layout.
pub(super) fn normalize_blob_update_field(
    array: &ArrayRef,
    expected: &Field,
) -> Result<(Field, ArrayRef)> {
    let array = super::update_input::decode_dictionary(array)?;
    if matches!(array.data_type(), DataType::Map(..) | DataType::List(_)) {
        normalize_blob_field(&array, expected, false)
    } else {
        // NULLs and other row-ID constructor inputs retain their conversion
        // at the existing DE writer position, rather than the declared schema.
        Ok((
            expected.clone().with_data_type(array.data_type().clone()),
            array,
        ))
    }
}

fn normalize_blob_field(
    array: &ArrayRef,
    expected: &Field,
    normalize_values: bool,
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
                if normalize_values {
                    normalize_blob_map_key(column, field.data_type())
                } else {
                    normalize_blob_update_key(column, field.data_type())
                }
            } else if normalize_values {
                normalize_write_array(column, field.data_type())
            } else {
                Ok(column.clone())
            }
        })
        .collect::<Result<Vec<_>>>()?;
    let mut fields = fields.to_vec();
    fields[1] = Arc::new(
        fields[1]
            .as_ref()
            .clone()
            .with_data_type(columns[1].data_type().clone()),
    );
    let fields = fields.into();
    let entries = StructArray::try_new(fields, columns, None).map_err(normalization_error)?;
    let child = Arc::new(
        child
            .as_ref()
            .clone()
            .with_data_type(entries.data_type().clone()),
    );
    let list = ListArray::try_new(child, offsets, Arc::new(entries), nulls.cloned())
        .map_err(normalization_error)?;
    Ok((
        expected.clone().with_data_type(list.data_type().clone()),
        Arc::new(list),
    ))
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

fn normalize_blob_update_key(array: &ArrayRef, expected: &DataType) -> Result<ArrayRef> {
    let decoded = super::update_input::decode_dictionary(array)?;
    if matches!(expected, DataType::Decimal128(..)) && decoded.data_type() != &DataType::Null {
        // Blob map keys follow Java Decimal.fromBigDecimal (HALF_UP), rather
        // than the exact decimal conversion used to match table upsert keys.
        let text = if decoded.data_type().is_decimal() {
            super::update_input::cast_update_value(
                &decoded,
                &DataType::Utf8,
                super::update_input::CastMode::Assignment,
            )?
        } else {
            decoded
        };
        return normalize_blob_map_key(&text, expected);
    }
    if matches!(
        (decoded.data_type(), expected),
        (
            DataType::Time32(_) | DataType::Time64(_),
            DataType::Time32(_) | DataType::Time64(_)
        )
    ) {
        // Java/Python serialize TIME Blob keys in milliseconds. This is a
        // value normalization step, unlike lossless table-key matching.
        return super::update_input::cast_update_value(
            &decoded,
            expected,
            super::update_input::CastMode::RowUpdate,
        );
    }
    super::key_normalization::normalize_match_key(&decoded, expected)
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
            DataType::List(_)
                | DataType::FixedSizeList(_, _)
                | DataType::Struct(_)
                | DataType::Map(_, _)
        )
    {
        return Ok(array.clone());
    }

    match (array.data_type(), expected) {
        // Java has one STRING value representation. Arrow offset widths do
        // not change the logical values, including in keys and nested fields.
        (DataType::LargeUtf8, DataType::Utf8) => {
            normalize_large_string(array.as_any().downcast_ref().unwrap())
        }
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
        (
            DataType::FixedSizeList(_, actual_size),
            DataType::FixedSizeList(expected_child, expected_size),
        ) if actual_size == expected_size => {
            let list = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            // VECTOR elements are positional, just like ARRAY elements. Reuse
            // the values of sliced inputs and let Arrow validate NOT NULL
            // elements against the combined vector/containing ROW validity.
            let nulls = NullBuffer::union(list.nulls(), parent_nulls);
            let values = normalize_write_array(list.values(), expected_child.data_type())?;
            let normalized = FixedSizeListArray::try_new_with_length(
                expected_child.clone(),
                *expected_size,
                values,
                nulls,
                list.len(),
            )
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

/// Narrow only the visible byte range. A small slice may start beyond i32::MAX
/// in its backing buffer, so casting the original absolute offsets is wrong.
/// The value buffer remains shared and only the offset buffer is rebuilt.
fn normalize_large_string(array: &LargeStringArray) -> Result<ArrayRef> {
    let (offsets, values) = narrow_byte_range(array.value_offsets(), array.values())?;
    StringArray::try_new(offsets, values, array.nulls().cloned())
        .map(|array| Arc::new(array) as ArrayRef)
        .map_err(normalization_error)
}

/// Offset adaptation for matching keys; ordinary append type validation remains
/// separate so this does not broaden accepted write schemas.
pub(super) fn normalize_large_binary(array: &LargeBinaryArray) -> Result<ArrayRef> {
    let (offsets, values) =
        normalize_large_key_bytes(array.value_offsets(), array.values(), array.nulls())?;
    BinaryArray::try_new(offsets, values, array.nulls().cloned())
        .map(|array| Arc::new(array) as ArrayRef)
        .map_err(normalization_error)
}

/// Matching reads logical string values, including NULLs with hidden storage.
/// Ordinary write normalization keeps its existing offset-limit validation.
pub(super) fn normalize_large_string_key(array: &LargeStringArray) -> Result<ArrayRef> {
    let (offsets, values) =
        normalize_large_key_bytes(array.value_offsets(), array.values(), array.nulls())?;
    StringArray::try_new(offsets, values, array.nulls().cloned())
        .map(|array| Arc::new(array) as ArrayRef)
        .map_err(normalization_error)
}

fn normalize_large_key_bytes(
    source: &[i64],
    values: &Buffer,
    nulls: Option<&NullBuffer>,
) -> Result<(OffsetBuffer<i32>, Buffer)> {
    if let Some(nulls) = nulls.filter(|nulls| {
        nulls.null_count() != 0 && source[source.len() - 1] - source[0] > i64::from(i32::MAX)
    }) {
        // NULL slots can retain arbitrarily large, unobservable byte ranges.
        // Compact only when they prevent narrowing; ordinary arrays continue
        // to share the visible value buffer below.
        let visible_len = source
            .windows(2)
            .enumerate()
            .filter(|(row, _)| nulls.is_valid(*row))
            .try_fold(0_i64, |len, (_, range)| {
                len.checked_add(range[1] - range[0])
            });
        if visible_len.is_none_or(|len| len > i64::from(i32::MAX)) {
            return Err(Error::DataInvalid {
                message: "Arrow byte range exceeds 32-bit offsets".into(),
                source: None,
            });
        }
        let mut compacted = Vec::with_capacity(visible_len.unwrap() as usize);
        let mut offsets = Vec::with_capacity(source.len());
        offsets.push(0);
        for (row, range) in source.windows(2).enumerate() {
            if nulls.is_valid(row) {
                compacted.extend_from_slice(&values[range[0] as usize..range[1] as usize]);
            }
            offsets.push(compacted.len() as i32);
        }
        return Ok((OffsetBuffer::new(offsets.into()), Buffer::from(compacted)));
    }
    narrow_byte_range(source, values)
}

fn narrow_byte_range(source: &[i64], values: &Buffer) -> Result<(OffsetBuffer<i32>, Buffer)> {
    let start = source[0];
    let end = source[source.len() - 1];
    let offsets = source
        .iter()
        .map(|offset| {
            i32::try_from(offset - start).map_err(|_| Error::DataInvalid {
                message: "Arrow byte range exceeds 32-bit offsets".into(),
                source: None,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let values = values.slice_with_length(start as usize, (end - start) as usize);
    Ok((OffsetBuffer::new(offsets.into()), values))
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
    use arrow_array::{FixedSizeListArray, Int32Array, LargeBinaryArray, StringArray};
    use arrow_buffer::{OffsetBuffer, ScalarBuffer};
    use arrow_schema::Field;

    #[cfg(all(unix, target_pointer_width = "64"))]
    #[test]
    fn large_string_slices_after_two_gib_keep_only_the_visible_byte_range() {
        use arrow_array::LargeStringArray;
        use arrow_buffer::Buffer;
        use std::ptr::NonNull;

        struct Mapping {
            address: usize,
            len: usize,
        }
        impl Drop for Mapping {
            fn drop(&mut self) {
                // SAFETY: the mapping owns this mmap region until the last
                // Arrow buffer using it is dropped.
                unsafe { libc::munmap(self.address as *mut libc::c_void, self.len) };
            }
        }
        let start = i32::MAX as usize + 17;
        let tail = "abc中文tail".as_bytes();
        let len = start + tail.len();
        // Reserve virtual memory without allocating/touching a 2 GiB payload.
        // Only the small visible tail is initialized and read by this test.
        let address = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANON,
                -1,
                0,
            )
        };
        assert_ne!(address, libc::MAP_FAILED);
        let owner = Arc::new(Mapping {
            address: address as usize,
            len,
        });
        // SAFETY: mmap returned len initialized bytes; the tail fits within
        // the mapping and all writes finish before publishing the buffer.
        let values = unsafe {
            std::ptr::copy_nonoverlapping(
                tail.as_ptr(),
                address.cast::<u8>().add(start),
                tail.len(),
            );
            Buffer::from_custom_allocation(NonNull::new(address.cast()).unwrap(), len, owner)
        };
        let start = start as i64;
        let input = LargeStringArray::new(
            OffsetBuffer::new(vec![start, start + 3, start + 3, start + 9, start + 13].into()),
            values,
            Some(NullBuffer::from(vec![true, false, true, true])),
        );
        let input: ArrayRef = Arc::new(input.slice(0, 3));
        let result = normalize_write_array(&input, &DataType::Utf8).unwrap();
        assert_eq!(
            result.to_data(),
            StringArray::from(vec![Some("abc"), None, Some("中文")]).to_data()
        );
        let result = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result.value_offsets(), &[0, 3, 3, 9]);
        assert_eq!(result.values().as_ptr(), unsafe {
            address.cast::<u8>().add(start as usize)
        });
        let empty = normalize_write_array(&input.slice(3, 0), &DataType::Utf8).unwrap();
        assert!(empty.is_empty());
        let text = input.as_any().downcast_ref::<LargeStringArray>().unwrap();
        let binary: ArrayRef = Arc::new(LargeBinaryArray::new(
            text.offsets().clone(),
            text.values().clone(),
            text.nulls().cloned(),
        ));
        let normalized =
            crate::table::key_normalization::normalize_match_key(&binary, &DataType::Binary)
                .unwrap();
        assert_eq!(
            normalized.to_data(),
            BinaryArray::from(vec![Some("abc".as_bytes()), None, Some("中文".as_bytes())])
                .to_data()
        );
        assert!(normalize_write_array(&binary, &DataType::Binary).is_err());
        for mixed in [false, true] {
            let offsets = if mixed {
                vec![0, start, start + 3]
            } else {
                vec![0, start]
            };
            let input: ArrayRef = Arc::new(LargeBinaryArray::new(
                OffsetBuffer::new(offsets.into()),
                text.values().clone(),
                // Retain a nonzero validity-bit offset through compaction.
                Some(NullBuffer::from(vec![true, false, true]).slice(1, if mixed { 2 } else { 1 })),
            ));
            let output =
                crate::table::key_normalization::normalize_match_key(&input, &DataType::Binary)
                    .unwrap();
            let expected = if mixed {
                vec![None, Some(b"abc".as_slice())]
            } else {
                vec![None]
            };
            assert_eq!(output.to_data(), BinaryArray::from(expected).to_data());
            let bytes = input.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            // SAFETY: the shared mapping contains valid UTF-8. Avoid scanning
            // the hidden 2 GiB NULL range merely to construct the test input.
            let strings: ArrayRef = Arc::new(unsafe {
                LargeStringArray::new_unchecked(
                    bytes.offsets().clone(),
                    bytes.values().clone(),
                    bytes.nulls().cloned(),
                )
            });
            let output =
                crate::table::key_normalization::normalize_match_key(&strings, &DataType::Utf8)
                    .unwrap();
            assert_eq!(
                output.to_data(),
                StringArray::from(if mixed {
                    vec![None, Some("abc")]
                } else {
                    vec![None]
                })
                .to_data(),
            );
            assert!(normalize_write_array(&strings, &DataType::Utf8).is_err());
        }
        let visible_overflow: ArrayRef = Arc::new(LargeBinaryArray::new(
            OffsetBuffer::new(vec![0, start + 3, start + 3].into()),
            text.values().clone(),
            Some(NullBuffer::from(vec![true, false])),
        ));
        assert!(crate::table::key_normalization::normalize_match_key(
            &visible_overflow,
            &DataType::Binary,
        )
        .is_err());
        let bytes = visible_overflow
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        // SAFETY: as above, both the zero-filled prefix and tail are UTF-8.
        let visible_string_overflow: ArrayRef = Arc::new(unsafe {
            LargeStringArray::new_unchecked(
                bytes.offsets().clone(),
                bytes.values().clone(),
                bytes.nulls().cloned(),
            )
        });
        assert!(crate::table::key_normalization::normalize_match_key(
            &visible_string_overflow,
            &DataType::Utf8,
        )
        .is_err());
        // A visible range larger than i32::MAX still cannot be represented.
        // SAFETY: the mapped bytes are zeroes followed by a valid UTF-8 tail;
        // offsets and NULL bitmap are valid. Skip scanning the 2 GiB prefix.
        let too_wide: ArrayRef = Arc::new(unsafe {
            LargeStringArray::new_unchecked(
                OffsetBuffer::new(vec![0, start + 13].into()),
                input
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .unwrap()
                    .values()
                    .clone(),
                Some(NullBuffer::from(vec![false])),
            )
        });
        assert!(normalize_write_array(&too_wide, &DataType::Utf8)
            .unwrap_err()
            .to_string()
            .contains("32-bit offsets"));
    }

    #[test]
    fn binary_offset_widths_are_not_implicitly_converted() {
        let small: ArrayRef = Arc::new(BinaryArray::from(vec![Some(b"\xff\0".as_slice()), None]));
        let large: ArrayRef = Arc::new(LargeBinaryArray::from(vec![
            Some(b"\xff\0".as_slice()),
            None,
        ]));
        assert!(normalize_write_array(&small, &DataType::LargeBinary).is_err());
        assert!(normalize_write_array(&large, &DataType::Binary).is_err());
    }

    #[test]
    fn large_strings_preserve_slices_unicode_and_nulls() {
        let input: ArrayRef = Arc::new(
            arrow_array::LargeStringArray::from(vec![
                Some("outside"),
                Some("中文"),
                None,
                Some(""),
                Some("last"),
            ])
            .slice(1, 4),
        );
        let result = normalize_write_array(&input, &DataType::Utf8).unwrap();
        assert_eq!(
            result.to_data(),
            StringArray::from(vec![Some("中文"), None, Some(""), Some("last")]).to_data()
        );
        let empty = normalize_write_array(&input.slice(0, 0), &DataType::Utf8).unwrap();
        assert!(empty.is_empty());
        assert_eq!(empty.data_type(), &DataType::Utf8);
        // Layout compatibility must not become an implicit value cast.
        assert!(normalize_write_array(&input, &DataType::Int32).is_err());
        assert!(normalize_write_array(&input, &DataType::Binary).is_err());
    }

    #[test]
    fn large_strings_inside_rows_and_arrays_keep_visible_nullability_checks() {
        let text: ArrayRef = Arc::new(arrow_array::LargeStringArray::from(vec![
            None,
            Some("中文"),
            None,
        ]));
        let input = Arc::new(StructArray::new(
            vec![Field::new("text", DataType::LargeUtf8, true)].into(),
            vec![text],
            Some(NullBuffer::from(vec![false, true, true])),
        )) as ArrayRef;
        let target = DataType::Struct(vec![Field::new("text", DataType::Utf8, false)].into());
        assert!(normalize_write_array(&input, &target).is_err());
        // The first child's NULL is hidden by a NULL ROW, so it is allowed.
        let normalized = normalize_write_array(&input.slice(0, 2), &target).unwrap();
        let row = normalized.as_any().downcast_ref::<StructArray>().unwrap();
        assert!(row.is_null(0));
        assert_eq!(
            row.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(1),
            "中文"
        );
        let list = Arc::new(
            ListArray::new(
                Arc::new(Field::new("item", DataType::LargeUtf8, true)),
                OffsetBuffer::new(vec![0, 1, 3].into()),
                Arc::new(arrow_array::LargeStringArray::from(vec![
                    Some("outside"),
                    Some("中文"),
                    None,
                ])),
                None,
            )
            .slice(1, 1),
        ) as ArrayRef;
        let expected = DataType::List(Arc::new(Field::new("element", DataType::Utf8, true)));
        let list = normalize_write_array(&list, &expected).unwrap();
        assert_eq!(list.data_type(), &expected);
        assert_eq!(
            list.as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value(0)
                .to_data(),
            StringArray::from(vec![Some("中文"), None]).to_data()
        );
    }

    #[test]
    fn vector_child_aliases_preserve_sliced_values_and_nulls() {
        for name in ["item", "", "element"] {
            let input = FixedSizeListArray::new(
                Arc::new(Field::new(name, DataType::Int32, true)),
                2,
                Arc::new(Int32Array::from(vec![
                    Some(0),
                    Some(1),
                    Some(2),
                    Some(3),
                    None,
                    None,
                    Some(6),
                    Some(7),
                ])),
                Some(NullBuffer::from(vec![true, true, false, true])),
            )
            .slice(1, 3);
            let input: ArrayRef = Arc::new(input);
            let expected =
                DataType::FixedSizeList(Arc::new(Field::new("element", DataType::Int32, true)), 2);
            let output = normalize_write_array(&input, &expected).unwrap();
            output.to_data().validate_full().unwrap();
            assert_eq!(output.data_type(), &expected);
            assert_eq!(output.len(), 3);
            assert_eq!(output.nulls(), input.nulls());
            let source = input.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            let result = output
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .unwrap();
            assert!(Arc::ptr_eq(result.values(), source.values()));
            assert_eq!(
                result.value(0).to_data(),
                Int32Array::from(vec![2, 3]).to_data()
            );
            assert_eq!(
                result.value(2).to_data(),
                Int32Array::from(vec![6, 7]).to_data()
            );
        }
    }

    #[test]
    fn vector_normalization_rejects_dimensions_and_element_type_changes() {
        let input: ArrayRef = Arc::new(FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            2,
            Arc::new(Int32Array::from(vec![1, 2])),
            None,
        ));
        for expected in [
            DataType::FixedSizeList(Arc::new(Field::new("element", DataType::Int32, true)), 1),
            DataType::FixedSizeList(Arc::new(Field::new("element", DataType::Int64, true)), 2),
            DataType::List(Arc::new(Field::new("element", DataType::Int32, true))),
        ] {
            assert!(normalize_write_array(&input, &expected).is_err());
        }
    }

    #[test]
    fn vector_element_nullability_only_checks_visible_parents() {
        let expected =
            DataType::FixedSizeList(Arc::new(Field::new("element", DataType::Int32, false)), 2);
        let make_input = |nulls| {
            Arc::new(FixedSizeListArray::new(
                Arc::new(Field::new("item", DataType::Int32, true)),
                2,
                Arc::new(Int32Array::from(vec![None, None, Some(3), Some(4)])),
                nulls,
            )) as ArrayRef
        };
        let hidden = make_input(Some(NullBuffer::from(vec![false, true])));
        let output = normalize_write_array(&hidden, &expected).unwrap();
        assert!(output.is_null(0));
        assert!(!output.is_null(1));
        assert!(normalize_write_array(&make_input(None), &expected).is_err());

        let row: ArrayRef = Arc::new(StructArray::new(
            vec![Field::new("embedding", hidden.data_type().clone(), true)].into(),
            vec![make_input(None)],
            Some(NullBuffer::from(vec![false, true])),
        ));
        let row_type =
            DataType::Struct(vec![Field::new("embedding", expected.clone(), true)].into());
        let output = normalize_write_array(&row, &row_type).unwrap();
        let result = output.as_any().downcast_ref::<StructArray>().unwrap();
        assert!(result.column(0).is_null(0));
        assert!(!result.column(0).is_null(1));

        // FFI inputs can claim NOT NULL elements while carrying visible NULLs.
        let invalid = unsafe {
            make_input(None)
                .to_data()
                .into_builder()
                .data_type(expected.clone())
                .build_unchecked()
        };
        let invalid = arrow_array::make_array(invalid);
        assert!(normalize_write_array(&invalid, &expected).is_err());
    }

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
    fn blob_map_updates_normalize_keys_and_transport_before_value_casts() {
        let entries = StructArray::new(
            vec![
                Field::new("k", DataType::Utf8, false),
                Field::new("v", DataType::Utf8, true),
            ]
            .into(),
            vec![
                Arc::new(StringArray::from(vec!["1.235"])),
                Arc::new(StringArray::from(vec!["new"])),
            ],
            None,
        );
        let entry_field = Arc::new(Field::new("input", entries.data_type().clone(), false));
        let offsets = OffsetBuffer::new(vec![0_i32, 1].into());
        let map: ArrayRef = Arc::new(MapArray::new(
            entry_field.clone(),
            offsets.clone(),
            entries.clone(),
            None,
            false,
        ));
        let list: ArrayRef = Arc::new(ListArray::new(
            entry_field,
            offsets,
            Arc::new(entries),
            None,
        ));
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
        let normalized = [map, list]
            .iter()
            .map(|array| normalize_blob_update_field(array, &expected).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(normalized[0].0, normalized[1].0);
        assert_eq!(normalized[0].1.to_data(), normalized[1].1.to_data());
        let list = normalized[0]
            .1
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let entries = list
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(
            entries
                .column(0)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            124
        );
        // Preserve the original input value type until the row-ID updater
        // casts against the actual read layout, including its nullability.
        assert_eq!(entries.column(1).data_type(), &DataType::Utf8);
        assert!(normalize_blob_write_field(&normalized[0].1, &expected).is_err());
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
