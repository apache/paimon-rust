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

use crate::spec::{CoreOptions, DataField};
use crate::table::global_index_build_common::vector::{
    find_index_field as find_vector_index_field,
    validate_vector_field as validate_common_vector_field,
};
use crate::table::Table;
use crate::{Error, Result};

pub(super) fn validate_table_options(table: &Table, core_options: &CoreOptions) -> Result<()> {
    if !core_options.row_tracking_enabled() {
        return Err(Error::DataInvalid {
            message: "vindex index build requires 'row-tracking.enabled' = 'true'".to_string(),
            source: None,
        });
    }
    if !core_options.data_evolution_enabled() {
        return Err(Error::DataInvalid {
            message: "vindex index build requires 'data-evolution.enabled' = 'true'".to_string(),
            source: None,
        });
    }
    if !core_options.global_index_enabled() {
        return Err(Error::DataInvalid {
            message: "vindex index build requires 'global-index.enabled' = 'true'".to_string(),
            source: None,
        });
    }
    if !table.schema().primary_keys().is_empty() {
        return Err(Error::Unsupported {
            message: "vindex index build does not support primary-key tables".to_string(),
        });
    }
    if core_options.deletion_vectors_enabled() {
        return Err(Error::Unsupported {
            message:
                "vindex index build does not support tables with deletion-vectors.enabled=true"
                    .to_string(),
        });
    }
    Ok(())
}

pub(super) fn find_index_field<'a>(table: &'a Table, column: &str) -> Result<&'a DataField> {
    find_vector_index_field(table, column)
}

pub(super) fn validate_vector_field(field: &DataField) -> Result<()> {
    validate_common_vector_field(field, "vindex")
}

pub(super) fn checked_vector_bytes(row_count: usize, dimension: usize) -> Result<usize> {
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

pub(super) fn checked_std_vector_bytes(
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

pub(super) fn checked_training_vector_count(row_count: usize, ratio: f64) -> Result<usize> {
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

pub(super) fn checked_training_sample_index(
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

pub(super) fn checked_i32(value: u64, context: &str) -> Result<i32> {
    i32::try_from(value).map_err(|_| Error::DataInvalid {
        message: format!("{context}: {value}"),
        source: None,
    })
}

pub(super) fn checked_i64(value: u64, context: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| Error::DataInvalid {
        message: format!("{context}: {value}"),
        source: None,
    })
}

pub(super) fn checked_row_count(row_range_start: i64, row_range_end: i64) -> Result<i64> {
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
