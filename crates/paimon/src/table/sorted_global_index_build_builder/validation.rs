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

//! Build-option, field-type, and row-range validation.

use crate::spec::{CoreOptions, DataField, DataType};
use crate::table::global_index_types::{FM_GLOBAL_INDEX_TYPE, MULTIVALUE_GLOBAL_INDEX_TYPE};
use crate::table::Table;
use crate::{Error, Result};

pub(super) fn validate_table_options(table: &Table, core_options: &CoreOptions) -> Result<()> {
    if !core_options.row_tracking_enabled() {
        return Err(Error::DataInvalid {
            message: "Sorted global index build requires 'row-tracking.enabled' = 'true'"
                .to_string(),
            source: None,
        });
    }
    if !core_options.data_evolution_enabled() {
        return Err(Error::DataInvalid {
            message: "Sorted global index build requires 'data-evolution.enabled' = 'true'"
                .to_string(),
            source: None,
        });
    }
    if !core_options.global_index_enabled() {
        return Err(Error::DataInvalid {
            message: "Sorted global index build requires 'global-index.enabled' = 'true'"
                .to_string(),
            source: None,
        });
    }
    if !table.schema().primary_keys().is_empty() {
        return Err(Error::Unsupported {
            message: "Sorted global index build does not support primary-key tables".to_string(),
        });
    }
    if core_options.deletion_vectors_enabled() {
        return Err(Error::Unsupported {
            message:
                "Sorted global index build does not support tables with deletion-vectors.enabled=true"
                    .to_string(),
        });
    }
    Ok(())
}

pub(super) fn find_index_field<'a>(table: &'a Table, column: &str) -> Result<&'a DataField> {
    table
        .schema()
        .fields()
        .iter()
        .find(|field| field.name() == column)
        .ok_or_else(|| Error::ColumnNotExist {
            full_name: table.identifier().full_name(),
            column: column.to_string(),
        })
}

pub(super) fn validate_btree_field(field: &DataField) -> Result<()> {
    if !is_btree_supported_data_type(field.data_type()) {
        return Err(Error::Unsupported {
            message: format!(
                "Sorted global index only supports scalar columns, got {:?} for column '{}'",
                field.data_type(),
                field.name()
            ),
        });
    }
    Ok(())
}

pub(super) fn index_key_type<'a>(index_type: &str, field: &'a DataField) -> Result<&'a DataType> {
    if index_type == MULTIVALUE_GLOBAL_INDEX_TYPE {
        let DataType::Array(array_type) = field.data_type() else {
            return Err(Error::Unsupported {
                message: format!(
                    "Multivalue global index requires an ARRAY column, got {:?} for column '{}'",
                    field.data_type(),
                    field.name()
                ),
            });
        };
        if !is_btree_supported_data_type(array_type.element_type()) {
            return Err(Error::Unsupported {
                message: format!(
                    "Multivalue global index does not support array element type {:?} for column '{}'",
                    array_type.element_type(),
                    field.name()
                ),
            });
        }
        Ok(array_type.element_type())
    } else if index_type == FM_GLOBAL_INDEX_TYPE {
        if !matches!(field.data_type(), DataType::Char(_) | DataType::VarChar(_)) {
            return Err(Error::Unsupported {
                message: format!(
                    "FM global index requires a character string column, got {:?} for column '{}'",
                    field.data_type(),
                    field.name()
                ),
            });
        }
        Ok(field.data_type())
    } else {
        validate_btree_field(field)?;
        Ok(field.data_type())
    }
}

pub(super) fn is_btree_supported_data_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean(_)
            | DataType::TinyInt(_)
            | DataType::SmallInt(_)
            | DataType::Int(_)
            | DataType::BigInt(_)
            | DataType::Decimal(_)
            | DataType::Double(_)
            | DataType::Float(_)
            | DataType::Char(_)
            | DataType::VarChar(_)
            | DataType::Date(_)
            | DataType::LocalZonedTimestamp(_)
            | DataType::Time(_)
            | DataType::Timestamp(_)
    )
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
            message: format!(
                "Invalid sorted global index row range [{row_range_start}, {row_range_end}]"
            ),
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

pub(super) fn ranges_overlap(
    left_start: i64,
    left_end: i64,
    right_start: i64,
    right_end: i64,
) -> bool {
    left_start <= right_end && right_start <= left_end
}
