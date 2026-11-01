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

//! Java-compatible declarations for source-backed primary-key indexes.

use crate::spec::{CoreOptions, DataField, DataType, TableSchema};
use crate::table::index_build::full_text;
use crate::table::Table;
use crate::vindex::VindexVectorIndexOptions;
use crate::{Error, Result};
use std::collections::{BTreeMap, HashMap};

pub(super) struct Definition {
    pub field: DataField,
    pub index_type: String,
    pub options: HashMap<String, String>,
}

pub(super) fn field_options(
    options: &HashMap<String, String>,
    column: &str,
    family: &str,
    index_type: &str,
) -> Result<HashMap<String, String>> {
    crate::table::global_index_options::field_index_options(options, column, family, index_type)
}

pub(super) fn definitions(schema: &TableSchema) -> Result<Vec<Definition>> {
    let options = schema.options();
    let owners = crate::table::global_index_options::declarations(options)?
        .into_iter()
        .map(|definition| (definition.column.clone(), definition))
        .collect::<HashMap<_, _>>();
    for column in owners.keys() {
        if !schema.fields().iter().any(|field| field.name() == column) {
            return Err(Error::ConfigInvalid {
                message: format!("Primary-key index contains unknown column '{column}'"),
            });
        }
    }
    if CoreOptions::new(options).vector_index_enabled() {
        schema.core_options().single_vector_index_column()?;
    }
    let mut result = Vec::new();
    for field in schema.fields() {
        let Some(declaration) = owners.get(field.name()) else {
            continue;
        };
        let kind = declaration.index_type.as_str();
        let family = if crate::vindex::is_vindex_index_type(kind) {
            "vector"
        } else {
            kind
        };
        let configured = declaration.options.clone();
        let mut merged = options.clone();
        merged.extend(configured.clone());
        match family {
            "vector" => {
                match field.data_type() {
                    DataType::Vector(_) => {}
                    DataType::Array(array)
                        if matches!(array.element_type(), DataType::Float(_)) => {}
                    _ => {
                        return Err(Error::Unsupported {
                            message: format!(
                                "PK vector index requires VECTOR or ARRAY<FLOAT>: {}",
                                field.name()
                            ),
                        })
                    }
                }
                crate::vindex::VindexVectorIndexOptions::new(options, &configured, kind, field)?;
            }
            "full-text" => {
                if !matches!(field.data_type(), DataType::Char(_) | DataType::VarChar(_)) {
                    return Err(Error::Unsupported {
                        message: format!(
                            "PK full-text index requires a string column: {}",
                            field.name()
                        ),
                    });
                }
                let native =
                    crate::table::index_build::full_text::native_full_text_options(&merged);
                crate::table::index_build::full_text::full_text_config(&native)?;
            }
            _ => {
                crate::table::index_build::fields::index_key_type(kind, field)?;
                crate::table::index_build::scalar::ScalarIndexOptions::from_options(kind, &merged)?;
            }
        }
        result.push(Definition {
            field: field.clone(),
            index_type: kind.into(),
            options: configured,
        });
    }
    Ok(result)
}

/// Canonical persisted options used to validate PK payload compatibility.
pub(super) fn native_options(
    table: &Table,
    definition: &Definition,
) -> Result<BTreeMap<String, String>> {
    if crate::vindex::is_vindex_index_type(&definition.index_type) {
        Ok(VindexVectorIndexOptions::new(
            table.schema().options(),
            &definition.options,
            &definition.index_type,
            &definition.field,
        )?
        .native_options
        .into_iter()
        .collect())
    } else {
        let mut options = table.schema().options().clone();
        options.extend(definition.options.clone());
        Ok(full_text::native_full_text_options(&options)
            .into_iter()
            .collect())
    }
}
