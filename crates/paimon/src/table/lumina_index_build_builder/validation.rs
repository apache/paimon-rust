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

//! Lumina table, field, option, and numeric validation.

use crate::lumina::{LuminaVectorIndexOptions, LUMINA_DIMENSION_OPTION};
use crate::spec::{CoreOptions, DataField, DataType};
use crate::table::global_index_build_common::vector::{
    find_index_field as find_vector_index_field,
    validate_vector_field as validate_common_vector_field,
};
use crate::table::Table;
use crate::{Error, Result};
use std::collections::HashMap;

pub(super) fn validate_table_options(table: &Table, core_options: &CoreOptions) -> Result<()> {
    if !table.schema().primary_keys().is_empty() {
        return Err(Error::Unsupported {
            message: "Lumina index build does not support primary-key tables".to_string(),
        });
    }
    if !core_options.row_tracking_enabled() {
        return Err(Error::DataInvalid {
            message: "Lumina index build requires 'row-tracking.enabled' = 'true'".to_string(),
            source: None,
        });
    }
    if !core_options.data_evolution_enabled() {
        return Err(Error::DataInvalid {
            message: "Lumina index build requires 'data-evolution.enabled' = 'true'".to_string(),
            source: None,
        });
    }
    if !core_options.global_index_enabled() {
        return Err(Error::DataInvalid {
            message: "Lumina index build requires 'global-index.enabled' = 'true'".to_string(),
            source: None,
        });
    }
    if core_options.deletion_vectors_enabled() {
        return Err(Error::Unsupported {
            message:
                "Lumina index build does not support tables with deletion-vectors.enabled=true"
                    .to_string(),
        });
    }
    Ok(())
}

pub(super) fn find_index_field<'a>(table: &'a Table, column: &str) -> Result<&'a DataField> {
    find_vector_index_field(table, column)
}

pub(super) fn validate_vector_field(field: &DataField) -> Result<()> {
    validate_common_vector_field(field, "Lumina")
}

pub(super) fn effective_lumina_options(
    field: &DataField,
    mut resolved: HashMap<String, String>,
) -> Result<HashMap<String, String>> {
    let DataType::Vector(vector) = field.data_type() else {
        return Ok(resolved);
    };
    let dimension = vector.length().to_string();
    match resolved.get(LUMINA_DIMENSION_OPTION) {
        None => {
            resolved.insert(LUMINA_DIMENSION_OPTION.to_string(), dimension);
        }
        Some(existing) if *existing == dimension => {}
        Some(existing) => {
            return Err(Error::ConfigInvalid {
                message: format!(
                    "Vector column '{}' has dimension {} from its type, but '{}' is set to '{}'. \
                     Remove the option or set it to {}.",
                    field.name(),
                    dimension,
                    LUMINA_DIMENSION_OPTION,
                    existing,
                    dimension
                ),
            });
        }
    }
    Ok(resolved)
}

pub(super) fn resolve_lumina_options(
    table_options: &HashMap<String, String>,
    user_options: &HashMap<String, String>,
) -> Result<HashMap<String, String>> {
    let mut options = table_options.clone();
    options.extend(user_options.clone());
    LuminaVectorIndexOptions::new(&options)?;
    Ok(options)
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
            message: format!("Invalid Lumina row range [{row_range_start}, {row_range_end}]"),
            source: None,
        });
    }
    row_range_end
        .checked_sub(row_range_start)
        .and_then(|span| span.checked_add(1))
        .ok_or_else(|| Error::DataInvalid {
            message: format!(
                "Row count overflows for row range [{row_range_start}, {row_range_end}]"
            ),
            source: None,
        })
}
