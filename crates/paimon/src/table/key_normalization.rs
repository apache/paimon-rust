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

//! Comparison keys follow Java's FLOAT/DOUBLE ordering: all NaNs compare
//! equal and after finite values, while signed zeros remain distinct.

use arrow_array::{ArrayRef, Float32Array, Float64Array};
use std::sync::Arc;

/// Arrow orders NaNs by their sign and payload. Canonicalize only comparison
/// arrays, preserving the original values written to or returned from files.
/// Non-float columns and already-canonical arrays keep their existing buffers.
pub(super) fn normalize_float_key(column: &ArrayRef) -> ArrayRef {
    if let Some(values) = column.as_any().downcast_ref::<Float32Array>() {
        if values
            .values()
            .iter()
            .any(|v| v.is_nan() && v.to_bits() != f32::NAN.to_bits())
        {
            return Arc::new(
                values
                    .iter()
                    .map(|v| v.map(|v| if v.is_nan() { f32::NAN } else { v }))
                    .collect::<Float32Array>(),
            );
        }
    } else if let Some(values) = column.as_any().downcast_ref::<Float64Array>() {
        if values
            .values()
            .iter()
            .any(|v| v.is_nan() && v.to_bits() != f64::NAN.to_bits())
        {
            return Arc::new(
                values
                    .iter()
                    .map(|v| v.map(|v| if v.is_nan() { f64::NAN } else { v }))
                    .collect::<Float64Array>(),
            );
        }
    }
    Arc::clone(column)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use arrow_row::{RowConverter, SortField};
    use arrow_schema::DataType;

    #[test]
    fn comparison_canonicalizes_nan_payloads_but_preserves_null_and_signed_zero() {
        for double in [false, true] {
            let values: ArrayRef = if double {
                Arc::new(Float64Array::from(vec![
                    None,
                    Some(f64::NEG_INFINITY),
                    Some(-0.0),
                    Some(0.0),
                    Some(f64::INFINITY),
                    Some(f64::NAN),
                    Some(-f64::NAN),
                    Some(f64::from_bits(0x7ff8_0000_0000_0042)),
                ]))
            } else {
                Arc::new(Float32Array::from(vec![
                    None,
                    Some(f32::NEG_INFINITY),
                    Some(-0.0),
                    Some(0.0),
                    Some(f32::INFINITY),
                    Some(f32::NAN),
                    Some(-f32::NAN),
                    Some(f32::from_bits(0x7fc0_0042)),
                ]))
            };
            let normalized = normalize_float_key(&values);
            assert!(normalized.is_null(0));
            let converter = RowConverter::new(vec![SortField::new(if double {
                DataType::Float64
            } else {
                DataType::Float32
            })])
            .unwrap();
            let rows = converter.convert_columns(&[normalized]).unwrap();
            for i in 0..5 {
                assert!(rows.row(i) < rows.row(i + 1));
            }
            assert_eq!(rows.row(5), rows.row(6));
            assert_eq!(rows.row(6), rows.row(7));
            // Comparison must not rewrite the payload supplied by the caller.
            if double {
                assert!(values
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(6)
                    .is_sign_negative());
            } else {
                assert!(values
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap()
                    .value(6)
                    .is_sign_negative());
            }
        }
    }
}
