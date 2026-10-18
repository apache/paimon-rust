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

//! Text input decoding respects logical NULLs, independently of their bytes.

use arrow_array::{builder::LargeStringBuilder, cast::AsArray, ArrayRef};
use arrow_schema::DataType;
use std::sync::Arc;

use super::invalid;

pub(super) fn cast_to_string(array: &ArrayRef, target: &DataType) -> crate::Result<ArrayRef> {
    let options = arrow_cast::CastOptions {
        safe: false,
        ..Default::default()
    };
    let result = arrow_cast::cast_with_options(array.as_ref(), target, &options);
    match result {
        Ok(array) => Ok(array),
        Err(_) => {
            // The zero-copy kernel validates the entire value buffer, including
            // NULL slots and bytes outside a slice. Arrow C++ skips those bytes.
            // Rebuild only after the ordinary cast fails,
            // still rejecting every invalid visible value. Large offsets avoid
            // overflow before the final checked conversion to the target width.
            let strings = match array.data_type() {
                DataType::Binary => visible_text(array.as_binary::<i32>().iter(), target)?,
                DataType::LargeBinary => visible_text(array.as_binary::<i64>().iter(), target)?,
                DataType::BinaryView => visible_text(array.as_binary_view().iter(), target)?,
                _ => unreachable!("binary text input was validated by the caller"),
            };
            arrow_cast::cast_with_options(strings.as_ref(), target, &options)
                .map_err(|error| invalid(error.to_string()))
        }
    }
}

fn visible_text<'a>(
    values: impl Iterator<Item = Option<&'a [u8]>> + Clone,
    target: &DataType,
) -> crate::Result<ArrayRef> {
    if target == &DataType::Utf8 {
        values
            .clone()
            .flatten()
            .try_fold(0_usize, |length, bytes| {
                length
                    .checked_add(bytes.len())
                    .filter(|length| *length <= i32::MAX as usize)
                    .ok_or_else(|| invalid("Binary text exceeds STRING offset range"))
            })?;
    }
    let mut builder = LargeStringBuilder::new();
    for value in values {
        match value {
            Some(bytes) => builder.append_value(
                std::str::from_utf8(bytes).map_err(|error| invalid(error.to_string()))?,
            ),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::super::{cast_update_value, CastMode};
    use super::*;
    use arrow_array::{Array, BinaryArray, Int32Array, LargeBinaryArray, StringArray};
    use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer};
    use std::sync::Arc;

    fn hidden_bytes(large: bool) -> ArrayRef {
        let offsets = vec![0, 1, 3, 5, 6, 8, 9];
        let values = Buffer::from(b"x\xff\xff42\xc3-7z".as_slice());
        let nulls = Some(NullBuffer::from(vec![true, false, true, false, true, true]));
        let array: ArrayRef = if large {
            Arc::new(LargeBinaryArray::new(
                OffsetBuffer::new(offsets.into()),
                values,
                nulls,
            ))
        } else {
            Arc::new(BinaryArray::new(
                OffsetBuffer::new(
                    offsets
                        .into_iter()
                        .map(|i| i as i32)
                        .collect::<Vec<_>>()
                        .into(),
                ),
                values,
                nulls,
            ))
        };
        array.slice(1, 4)
    }

    #[test]
    fn hidden_invalid_utf8_is_not_a_text_or_numeric_value() {
        for large in [false, true] {
            let source = hidden_bytes(large);
            for mode in [CastMode::Assignment, CastMode::RowUpdate] {
                let text = cast_update_value(&source, &DataType::Utf8, mode).unwrap();
                assert_eq!(
                    text.to_data(),
                    StringArray::from(vec![None, Some("42"), None, Some("-7")]).to_data()
                );
                let integers = cast_update_value(&source, &DataType::Int32, mode).unwrap();
                assert_eq!(
                    integers.to_data(),
                    Int32Array::from(vec![None, Some(42), None, Some(-7)]).to_data()
                );
                // A batch boundary may retain only a valid value while its
                // backing buffer still owns invalid bytes from other slots.
                let valid_slice = source.slice(1, 1);
                assert_eq!(valid_slice.null_count(), 0);
                let text = cast_update_value(&valid_slice, &DataType::Utf8, mode).unwrap();
                assert_eq!(text.to_data(), StringArray::from(vec!["42"]).to_data());
                let null_slice = source.slice(0, 1);
                let text = cast_update_value(&null_slice, &DataType::Utf8, mode).unwrap();
                assert_eq!(
                    text.to_data(),
                    StringArray::from(vec![None::<&str>]).to_data()
                );
                let empty = cast_update_value(&source.slice(1, 0), &DataType::Utf8, mode).unwrap();
                assert!(empty.is_empty());
            }
        }
    }

    #[test]
    fn oversized_visible_text_is_rejected_before_copying_values() {
        // Reuse one small allocation to represent a >2 GiB logical text input.
        let bytes = vec![b'x'; 1 << 20];
        let values = std::iter::repeat_n(Some(bytes.as_slice()), 2048);
        assert!(visible_text(values, &DataType::Utf8).is_err());
    }

    #[test]
    fn visible_invalid_utf8_still_rejects_the_whole_assignment() {
        let source: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(b"42".as_slice()),
            None,
            Some(b"\xff".as_slice()),
        ]));
        for mode in [CastMode::Assignment, CastMode::RowUpdate] {
            for target in [DataType::Utf8, DataType::Int32] {
                assert!(cast_update_value(&source, &target, mode).is_err());
            }
        }
    }

    #[test]
    fn ordinary_binary_text_keeps_its_shared_value_buffer() {
        let source = Arc::new(BinaryArray::from(vec![Some(b"42".as_slice()), None])) as ArrayRef;
        let result = cast_to_string(&source, &DataType::Utf8).unwrap();
        assert_eq!(
            source.to_data().buffers()[1].as_ptr(),
            result.to_data().buffers()[1].as_ptr()
        );
    }
}
