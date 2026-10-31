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
use crate::table::global_index_options::parse_field_index_options;
use crate::table::global_index_types::normalize_global_index_type;
use crate::{Error, Result};
use std::collections::{HashMap, HashSet};

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
    let key = format!("fields.{column}.pk-{family}.index.options");
    let prefix = if matches!(family, "vector" | "full-text") {
        format!("{index_type}.")
    } else {
        format!("{family}-index.")
    };
    let mut resolved =
        parse_field_index_options(options, column, &key, &prefix, family == "vector")?;
    if family == "vector" {
        let metric = CoreOptions::new(options).primary_key_vector_distance_metric(column)?;
        crate::vindex::pkvector::metric::VectorSearchMetric::parse(&metric)?;
        resolved.insert(format!("{index_type}.metric"), metric);
    }
    Ok(resolved)
}

pub(super) fn definitions(schema: &TableSchema) -> Result<Vec<Definition>> {
    let options = schema.options();
    let mut owners = HashMap::new();
    for family in ["btree", "bitmap", "multivalue", "fm", "full-text", "vector"] {
        let key = format!("pk-{family}.index.columns");
        if let Some(columns) = options.get(&key) {
            let mut unique = HashSet::new();
            for column in columns.split(',').map(str::trim) {
                if column.is_empty() || !unique.insert(column) {
                    return Err(Error::ConfigInvalid {
                        message: format!("{key} contains an empty or duplicate column '{column}'"),
                    });
                }
                if !schema.fields().iter().any(|field| field.name() == column) {
                    return Err(Error::ConfigInvalid {
                        message: format!("{key} contains unknown column '{column}'"),
                    });
                }
                if owners.insert(column, family).is_some() {
                    return Err(Error::ConfigInvalid {
                        message: format!("Column '{column}' can own at most one primary-key index"),
                    });
                }
            }
        }
    }
    if options.contains_key("pk-vector.index.columns") {
        schema.core_options().primary_key_vector_index_column()?;
    }
    let mut result = Vec::new();
    for field in schema.fields() {
        let Some(&family) = owners.get(field.name()) else {
            continue;
        };
        let kind = if family == "vector" {
            let configured = schema
                .core_options()
                .primary_key_vector_index_type(field.name())?;
            normalize_global_index_type(configured.trim())
                .filter(|kind| crate::vindex::is_vindex_index_type(kind))
                .ok_or_else(|| Error::ConfigInvalid {
                    message: format!("Unsupported primary-key vector index type: {configured}"),
                })?
        } else {
            family
        };
        let configured = field_options(options, field.name(), family, kind)?;
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
