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

use crate::{Error, Result};
pub(in crate::table) fn checked_vector_bytes(row_count: usize, dimension: usize) -> Result<usize> {
    row_count
        .checked_mul(dimension)
        .and_then(|values| values.checked_mul(std::mem::size_of::<f32>()))
        .ok_or_else(|| Error::DataInvalid {
            message: format!(
                "vindex vector byte length overflows: row_count={row_count}, dimension={dimension}"
            ),
            source: None,
        })
}

pub(in crate::table) fn checked_std_vector_bytes(
    row_count: usize,
    dimension: usize,
) -> std::io::Result<usize> {
    row_count
        .checked_mul(dimension)
        .and_then(|values| values.checked_mul(std::mem::size_of::<f32>()))
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "vindex vector byte length overflows usize",
            )
        })
}

pub(in crate::table) fn checked_training_vector_count(
    row_count: usize,
    ratio: f64,
) -> Result<usize> {
    if row_count == 0 || !(ratio > 0.0 && ratio <= 1.0) {
        return Err(Error::DataInvalid {
            message: format!(
                "Invalid vindex training sample: row_count={row_count}, ratio={ratio}; expected a positive row count and ratio in (0, 1]"
            ),
            source: None,
        });
    }
    Ok(((row_count as f64 * ratio).ceil() as usize).clamp(1, row_count))
}

pub(in crate::table) fn checked_training_sample_index(
    sample: usize,
    rows: usize,
    samples: usize,
) -> Result<usize> {
    sample
        .checked_mul(rows / samples)
        .and_then(|base| {
            sample
                .checked_mul(rows % samples)
                .and_then(|remainder| base.checked_add(remainder / samples))
        })
        .ok_or_else(|| Error::DataInvalid {
            message: "vindex training sample index overflows usize".to_string(),
            source: None,
        })
}

pub(in crate::table) fn checked_i32(value: u64, context: &str) -> Result<i32> {
    i32::try_from(value).map_err(|_| Error::DataInvalid {
        message: format!("{context}: {value}"),
        source: None,
    })
}

pub(in crate::table) fn checked_row_count(row_range_start: i64, row_range_end: i64) -> Result<i64> {
    if row_range_end < row_range_start {
        return Err(Error::DataInvalid {
            message: format!("Invalid vindex row range [{row_range_start}, {row_range_end}]"),
            source: None,
        });
    }
    row_range_end
        .checked_sub(row_range_start)
        .and_then(|count| count.checked_add(1))
        .ok_or_else(|| Error::DataInvalid {
            message: format!(
                "Row count overflows for row range [{row_range_start}, {row_range_end}]"
            ),
            source: None,
        })
}
