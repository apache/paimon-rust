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

//! Java's temporary selected-key MAP ROW read type.

use crate::spec::{DataField, DataType, RowType};
use crate::{Error, Result};
use std::collections::HashSet;

/// Java field-description prefix for a temporary selected-key MAP ROW.
pub const MAP_SELECTED_KEYS_METADATA_KEY: &str = "__PAIMON_MAP_SELECTED_KEYS:";

/// Whether the field is a synthetic ROW carrying selected MAP key metadata.
pub fn is_map_selected_keys_field(field: &DataField) -> bool {
    matches!(field.data_type(), DataType::Row(_))
        && field
            .description()
            .is_some_and(|value| value.starts_with(MAP_SELECTED_KEYS_METADATA_KEY))
}

/// Decode keys in child ordinal order, validating arity and uniqueness.
/// Ordinary fields return `None`.
pub fn map_selected_keys(field: &DataField) -> Result<Option<Vec<String>>> {
    if !is_map_selected_keys_field(field) {
        return Ok(None);
    }
    let DataType::Row(row) = field.data_type() else {
        unreachable!()
    };
    let encoded = field
        .description()
        .unwrap()
        .strip_prefix(MAP_SELECTED_KEYS_METADATA_KEY)
        .unwrap();
    let keys: Vec<String> = encoded.split(';').map(str::to_string).collect();
    if row.fields().is_empty()
        || keys
            .iter()
            .any(|key| key.starts_with(MAP_SELECTED_KEYS_METADATA_KEY))
        || keys.len() != row.fields().len()
        || keys.iter().collect::<HashSet<_>>().len() != keys.len()
    {
        return Err(Error::DataInvalid {
            message: format!(
                "Selected-key metadata for '{}' must match the ROW fields and contain unique keys",
                field.name()
            ),
            source: None,
        });
    }
    Ok(Some(keys))
}

/// Build Java's selected-key ROW for a STRING-keyed MAP, preserving the parent
/// field ID and the complete value type. Each selected value is nullable.
pub fn map_selected_keys_field(field: &DataField, keys: &[String]) -> Result<DataField> {
    let DataType::Map(map) = field.data_type() else {
        return Err(Error::DataInvalid {
            message: "Selected-key projection requires a MAP field".into(),
            source: None,
        });
    };
    if !matches!(map.key_type(), DataType::VarChar(_)) {
        return Err(Error::Unsupported {
            message: "Selected-key projection requires STRING map keys".into(),
        });
    }
    let mut seen = HashSet::new();
    if keys.is_empty()
        || keys.iter().any(|key| {
            key.contains(';')
                || key.starts_with(MAP_SELECTED_KEYS_METADATA_KEY)
                || !seen.insert(key)
        })
    {
        return Err(Error::DataInvalid {
            message: "Selected MAP keys must be nonempty, unique, and encodable with Java metadata"
                .into(),
            source: None,
        });
    }
    let children = keys
        .iter()
        .enumerate()
        .map(|(index, key)| {
            Ok(DataField::new(
                index as i32,
                key.clone(),
                map.value_type().copy_with_nullable(true)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(DataField::new(
        field.id(),
        field.name().into(),
        DataType::Row(RowType::with_nullable(
            field.data_type().is_nullable(),
            children,
        )),
    )
    .with_description(Some(format!(
        "{MAP_SELECTED_KEYS_METADATA_KEY}{}",
        keys.join(";")
    )))
    .with_default_value(field.default_value().map(str::to_string)))
}
