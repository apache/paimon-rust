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

//! Shared field declarations and native options for global-index maintenance and raw reads.

use crate::table::global_index_build_common::{resolve_index_fields, IndexColumns};
use crate::table::global_index_types::normalize_global_index_type;
use crate::table::{GlobalIndexBuildBuilder, Table};
use crate::Result;
use std::collections::{HashMap, HashSet};

pub(super) struct Definition {
    pub(super) column: String,
    pub(super) index_type: String,
    pub(super) options: HashMap<String, String>,
}

impl Definition {
    pub(super) fn builder<'a>(&self, table: &'a Table) -> GlobalIndexBuildBuilder<'a> {
        let mut builder = table.new_global_index_build_builder();
        builder
            .with_index_column(&self.column)
            .with_index_type(&self.index_type)
            .with_options(self.options.clone());
        builder
    }
}

pub(super) fn definitions(table: &Table) -> Result<Vec<Definition>> {
    let options = table.schema().options();
    let mut owners = HashMap::new();
    for family in ["btree", "bitmap", "multivalue", "fm", "full-text", "vector"] {
        let key = format!("global-index.{family}.index-columns");
        let Some(columns) = options.get(&key) else {
            continue;
        };
        let mut unique = HashSet::new();
        for column in columns.split(',').map(str::trim) {
            if column.is_empty() || !unique.insert(column) {
                return Err(crate::Error::ConfigInvalid {
                    message: format!("{key} contains an empty or duplicate column '{column}'"),
                });
            }
            if owners.insert(column, family).is_some() {
                return Err(crate::Error::ConfigInvalid {
                    message: format!("Column '{column}' can own at most one automatically maintained global index"),
                });
            }
            resolve_index_fields(table, &IndexColumns::Columns(vec![column.into()]), "btree")?;
        }
    }
    let mut result = Vec::with_capacity(owners.len());
    for field in table.schema().fields() {
        let column = field.name();
        let Some(&family) = owners.get(column) else {
            continue;
        };
        let index_type = if family == "vector" {
            vector_index_type(options, column)?.ok_or_else(|| crate::Error::ConfigInvalid {
                message: format!("fields.{column}.global-index.vector.index-type must specify a supported vector index type"),
            })?
        } else {
            family
        };
        let definition = Definition {
            column: column.into(),
            index_type: index_type.into(),
            options: field_index_options(options, column, family, index_type)?,
        };
        let mut merged = options.clone();
        merged.extend(definition.options.clone());
        crate::table::universal_compaction::UniversalCompaction::from_options(
            &merged,
            "global-index.",
        )?;
        result.push(definition);
    }
    Ok(result)
}

pub(super) fn field_index_options(
    table_options: &HashMap<String, String>,
    column: &str,
    family: &str,
    index_type: &str,
) -> Result<HashMap<String, String>> {
    let field_prefix = format!("fields.{column}.global-index.{family}.");
    let native_prefix = match family {
        "vector" | "full-text" => format!("{index_type}."),
        _ => format!("{family}-index."),
    };
    let options_key = format!("{field_prefix}index-options");
    let mut options = parse_field_index_options(
        table_options,
        column,
        &options_key,
        &native_prefix,
        family == "vector",
    )?;
    if family == "vector" {
        let metric_key = format!("{field_prefix}distance-metric");
        let metric = table_options
            .get(&metric_key)
            .map(String::as_str)
            .unwrap_or("inner_product");
        crate::vindex::pkvector::metric::VectorSearchMetric::parse(metric)?;
        options.insert(
            format!("{native_prefix}metric"),
            metric.to_ascii_lowercase().replace('-', "_"),
        );
    }
    Ok(options)
}

pub(super) fn parse_field_index_options(
    table_options: &HashMap<String, String>,
    column: &str,
    options_key: &str,
    native_prefix: &str,
    vector: bool,
) -> Result<HashMap<String, String>> {
    let mut options = HashMap::new();
    let mut insert = |key: &str, value: String| -> Result<()> {
        if key.trim().is_empty() {
            return Err(crate::Error::ConfigInvalid {
                message: format!("{options_key} contains an empty option key"),
            });
        }
        let qualified = if key.starts_with(native_prefix)
            || key.starts_with("fields.")
            || key.starts_with("global-index.")
            || key.starts_with("sorted-index.")
            || (vector && matches!(key, "index.type" | "vindex.build.granule.enabled"))
        {
            key.into()
        } else {
            format!("{native_prefix}{key}")
        };
        if options
            .get(&qualified)
            .or_else(|| match qualified.as_str() {
                // Maintenance policies are column overrides; native writer
                // options still reject conflicting table and field definitions.
                "global-index.num-sorted-run.compaction-trigger"
                | "global-index.compaction.size-ratio"
                | "global-index.compaction.max-size-amplification-percent" => None,
                _ => table_options.get(&qualified),
            })
            .is_some_and(|previous| previous != &value)
        {
            return Err(crate::Error::ConfigInvalid {
                message: format!("Conflicting field index options for {column}: {qualified}"),
            });
        }
        options.insert(qualified, value);
        Ok(())
    };
    if let Some(raw) = table_options
        .get(options_key)
        .filter(|raw| !raw.trim().is_empty())
    {
        let parsed: serde_json::Map<String, serde_json::Value> = serde_json::from_str(raw)
            .map_err(|_| crate::Error::ConfigInvalid {
                message: format!("{options_key} must be a JSON object of option key-value pairs"),
            })?;
        for (key, value) in parsed {
            let value = match value {
                serde_json::Value::String(value) => value,
                serde_json::Value::Number(_) | serde_json::Value::Bool(_) => value.to_string(),
                _ => {
                    return Err(crate::Error::ConfigInvalid {
                        message: format!(
                            "{options_key} contains an invalid option value for {key}"
                        ),
                    })
                }
            };
            insert(&key, value)?;
        }
    }
    Ok(options)
}

pub(super) fn vector_index_type(
    options: &HashMap<String, String>,
    column: &str,
) -> Result<Option<&'static str>> {
    let key = format!("fields.{column}.global-index.vector.index-type");
    match options.get(&key) {
        Some(value) => normalize_global_index_type(value.trim())
            .filter(|kind| crate::vindex::is_vindex_index_type(kind))
            .map(Some)
            .ok_or_else(|| crate::Error::ConfigInvalid {
                message: format!("{key} must specify a supported vector index type"),
            }),
        None => Ok(None),
    }
}

pub(super) fn declares_column(
    options: &HashMap<String, String>,
    column: &str,
    family: &str,
) -> bool {
    options
        .get(&format!("global-index.{family}.index-columns"))
        .is_some_and(|columns| columns.split(',').any(|name| name.trim() == column))
}

pub(super) fn declared_options(
    options: &HashMap<String, String>,
    column: &str,
    family: &str,
    index_type: &str,
) -> Result<HashMap<String, String>> {
    let prefix = format!("fields.{column}.global-index.{family}.");
    if declares_column(options, column, family)
        || options.keys().any(|key| key.starts_with(&prefix))
    {
        field_index_options(options, column, family, index_type)
    } else {
        Ok(HashMap::new())
    }
}
