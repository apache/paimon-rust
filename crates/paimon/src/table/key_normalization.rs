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

//! Logical keys used by row-level matching, independent of Arrow encoding.

use arrow_array::{new_null_array, ArrayRef};
use arrow_schema::DataType;

use super::update_input::{cast_update_value, decode_dictionary, CastMode};
use super::write_batch_normalize::normalize_write_array;

/// Dictionary indices are local to each batch, never row identity. NULL keys
/// retain their validity bitmap; the operation decides whether NULL matches.
pub(super) fn normalize_match_key(
    array: &ArrayRef,
    expected: &DataType,
) -> crate::Result<ArrayRef> {
    let decoded = decode_dictionary(array)?;
    if decoded.data_type() == &DataType::Null {
        return Ok(new_null_array(expected, decoded.len()));
    }
    normalize_write_array(&decoded, expected)
}

/// Upsert matches logical values before deciding update versus append. Integer
/// widths, decimal scales and units within a temporal family can differ, but
/// conversion must be exact so distinct source keys never collapse. Payloads,
/// including the original key columns, keep their own action's write contract.
pub(super) fn normalize_upsert_key(
    array: &ArrayRef,
    expected: &DataType,
) -> crate::Result<ArrayRef> {
    let decoded = decode_dictionary(array)?;
    let source = decoded.data_type();
    let exact_family = source.is_integer() && expected.is_integer()
        || source.is_decimal() && expected.is_decimal()
        || matches!(
            (source, expected),
            (
                DataType::Date32 | DataType::Date64,
                DataType::Date32 | DataType::Date64
            ) | (
                DataType::Time32(_) | DataType::Time64(_),
                DataType::Time32(_) | DataType::Time64(_)
            )
        )
        || matches!((source, expected), (DataType::Timestamp(_, from), DataType::Timestamp(_, to))
            if from.is_some() == to.is_some());
    if exact_family {
        cast_update_value(&decoded, expected, CastMode::Assignment)
    } else {
        normalize_match_key(&decoded, expected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::types::Int8Type;
    use arrow_array::{
        Array, Decimal128Array, DictionaryArray, Int32Array, Int64Array, Int8Array, StringArray,
        TimestampMicrosecondArray, TimestampMillisecondArray,
    };
    use arrow_schema::TimeUnit;
    use std::sync::Arc;

    #[test]
    fn dictionary_keys_use_value_validity_as_well_as_index_validity() {
        let input: ArrayRef = Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![Some(0), None, Some(1), Some(0)]),
                Arc::new(StringArray::from(vec![None, Some("key")])),
            )
            .unwrap(),
        );
        let output = normalize_match_key(&input, &DataType::Utf8).unwrap();
        assert_eq!(
            output.as_any().downcast_ref::<StringArray>().unwrap(),
            &StringArray::from(vec![None, None, Some("key"), None])
        );
    }

    #[test]
    fn integer_keys_are_exact_and_keep_nulls() {
        let input: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(-1),
            None,
            Some(i32::MAX as i64),
        ]));
        let output = normalize_upsert_key(&input, &DataType::Int32).unwrap();
        assert_eq!(
            output.as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(vec![Some(-1), None, Some(i32::MAX)])
        );
        let overflow: ArrayRef = Arc::new(Int64Array::from(vec![i32::MAX as i64 + 1]));
        assert!(normalize_upsert_key(&overflow, &DataType::Int32).is_err());
        let text: ArrayRef = Arc::new(StringArray::from(vec!["42"]));
        assert!(normalize_upsert_key(&text, &DataType::Int32).is_err());
    }

    #[test]
    fn decimal_keys_cannot_round_into_existing_keys() {
        let array = |value| {
            Arc::new(
                Decimal128Array::from(vec![value])
                    .with_precision_and_scale(15, 4)
                    .unwrap(),
            ) as ArrayRef
        };
        let expected = DataType::Decimal128(10, 2);
        let output = normalize_upsert_key(&array(12500), &expected).unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            125
        );
        assert!(normalize_upsert_key(&array(12501), &expected).is_err());
    }

    #[test]
    fn temporal_keys_require_lossless_units_and_preserve_instant() {
        let input: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
            Some(-1000),
            None,
            Some(2000),
        ]));
        let output =
            normalize_upsert_key(&input, &DataType::Timestamp(TimeUnit::Millisecond, None))
                .unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap(),
            &TimestampMillisecondArray::from(vec![Some(-1), None, Some(2)])
        );
        let fractional: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![-1001]));
        assert!(normalize_upsert_key(
            &fractional,
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        )
        .is_err());
        let local: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from(vec![0]).with_timezone("America/New_York"));
        let output = normalize_upsert_key(
            &local,
            &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        )
        .unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .value(0),
            0
        );
        assert!(
            normalize_upsert_key(&local, &DataType::Timestamp(TimeUnit::Microsecond, None))
                .is_err()
        );
    }
}
