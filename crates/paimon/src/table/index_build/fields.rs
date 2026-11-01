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

//! Scalar field validation used by PK and Data Evolution builders.
use crate::spec::{DataField, DataType};
use crate::table::global_index_types::{FM_GLOBAL_INDEX_TYPE, MULTIVALUE_GLOBAL_INDEX_TYPE};
use crate::{Error, Result};
pub(in crate::table) fn validate_btree_field(field: &DataField) -> Result<()> {
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

pub(in crate::table) fn index_key_type<'a>(
    index_type: &str,
    field: &'a DataField,
) -> Result<&'a DataType> {
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

pub(in crate::table) fn is_btree_supported_data_type(data_type: &DataType) -> bool {
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
