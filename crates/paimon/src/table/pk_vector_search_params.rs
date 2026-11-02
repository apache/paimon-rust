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

//! Resolve PK search options once the captured plan selects a vector format.

use crate::spec::{
    CoreOptions, DataField, DataType, GlobalIndexSearchMode, Predicate, VectorIndexOptions,
};
use crate::table::bucket_filter::split_partition_and_data_predicates;
use crate::table::vector_search_common::{
    indexed_search_limit, resolve_vector_search_options, resolved_refine_factor,
};
use crate::table::Table;
use crate::vindex::pkvector::exact::validate_query;
use crate::vindex::pkvector::metric::VectorSearchMetric;
use crate::vindex::VindexVectorIndexOptions;
use std::collections::HashMap;

#[derive(Clone)]
pub(super) struct PkVectorSearchParams {
    pub(super) metric: VectorSearchMetric,
    pub(super) concurrency: usize,
    pub(super) vector_options: VectorIndexOptions,
    pub(super) vector_field: DataField,
    pub(super) skip_exact_fallback: bool,
    pub(super) refine_factor: usize,
    pub(super) indexed_limit: usize,
}

impl PkVectorSearchParams {
    /// Validate inputs independent of the snapshot's actual index format.
    pub(super) fn validate_inputs(
        table: &Table,
        filter: Option<&Predicate>,
        column: &str,
        queries: &[&[f32]],
        limit: usize,
    ) -> crate::Result<DataField> {
        let core = table.schema().core_options();
        core.ensure_read_authorized()?;
        let has_data_predicate = filter.is_some_and(|f| {
            let (_, data) = split_partition_and_data_predicates(
                f.clone(),
                table.schema().fields(),
                table.schema().partition_keys(),
            );
            !data.is_empty()
        });
        if has_data_predicate
            && !(core.deletion_vectors_enabled() && !core.deletion_vectors_merge_on_read())
        {
            return Err(crate::Error::DataInvalid {
                message:
                    "primary-key vector pre-filter requires deletion vectors without merge-on-read"
                        .into(),
                source: None,
            });
        }
        if limit == 0 {
            return Err(crate::Error::DataInvalid {
                message: "vector search limit must be positive".into(),
                source: None,
            });
        }
        let field = table
            .schema()
            .fields()
            .iter()
            .find(|f| f.name() == column)
            .cloned()
            .ok_or_else(|| crate::Error::DataInvalid {
                message: format!("PK-vector column '{column}' not found in schema"),
                source: None,
            })?;
        for query in queries {
            let dimension = match field.data_type() {
                DataType::Vector(vector) => vector.length() as usize,
                _ => query.len().max(1),
            };
            validate_query(query, dimension)?;
        }
        Ok(field)
    }

    pub(super) fn resolve(
        table: &Table,
        query_options: &HashMap<String, String>,
        filter: Option<&Predicate>,
        column: &str,
        queries: &[&[f32]],
        limit: usize,
        metadata_type: Option<&str>,
    ) -> crate::Result<Self> {
        let vector_field = Self::validate_inputs(table, filter, column, queries, limit)?;
        let core = CoreOptions::new(table.schema().options());
        let configuration = resolve_vector_search_options(
            table.schema().options(),
            query_options,
            column,
            metadata_type,
            true,
        )?;
        let mut vector_options = configuration.resolved;
        // Raw-only PK plans use the table metric. The reader replaces it with
        // the persisted payload metric when the captured plan has ANN segments.
        vector_options.metric = configuration.table_metric;
        let metric = VectorSearchMetric::parse(&vector_options.metric)?;
        let refine_factor = if let Some(kind) = vector_options.index_type.as_deref() {
            resolved_refine_factor(
                &configuration.query_options,
                &vector_options.options,
                column,
                kind,
            )?
        } else {
            0
        };
        let indexed_limit = indexed_search_limit(limit, refine_factor)?;
        if let Some(kind) = vector_options.index_type.as_deref() {
            if let Some(dimension) = resolved_query_dimension(
                &vector_options.options,
                &configuration.query_options,
                kind,
                &vector_field,
            )? {
                for query in queries {
                    validate_query(query, dimension)?;
                }
            }
        }
        Ok(Self {
            metric,
            concurrency: core.global_index_thread_num()?,
            vector_options,
            vector_field,
            skip_exact_fallback: core.vector_index_search_mode()? == GlobalIndexSearchMode::Fast,
            refine_factor,
            indexed_limit,
        })
    }
}

fn resolved_query_dimension(
    table_options: &HashMap<String, String>,
    query_options: &HashMap<String, String>,
    index_type: &str,
    vector_field: &DataField,
) -> crate::Result<Option<usize>> {
    match vector_field.data_type() {
        DataType::Vector(vector) if matches!(vector.element_type(), DataType::Float(_)) => {
            Ok(Some(vector.length() as usize))
        }
        DataType::Array(array) if matches!(array.element_type(), DataType::Float(_)) => {
            let mut dimension_options = HashMap::new();
            for key in [
                "dimension".to_string(),
                format!("{index_type}.dimension"),
                format!("{index_type}.index.dimension"),
                format!("fields.{}.dimension", vector_field.name()),
                format!("fields.{}.index.dimension", vector_field.name()),
            ] {
                if let Some(value) = query_options.get(&key) {
                    let key = if key == "dimension" {
                        format!("{index_type}.dimension")
                    } else {
                        key
                    };
                    dimension_options.insert(key, value.clone());
                }
            }
            let mut defaults = table_options.clone();
            if let Some(value) = table_options.get("dimension") {
                if !table_options.contains_key(&format!("{index_type}.dimension"))
                    && !table_options.contains_key(&format!("{index_type}.index.dimension"))
                {
                    defaults.insert(format!("{index_type}.dimension"), value.clone());
                }
            }
            Ok(Some(
                VindexVectorIndexOptions::new_with_defaults(
                    &defaults,
                    &HashMap::new(),
                    &dimension_options,
                    index_type,
                    vector_field,
                )?
                .dimension(),
            ))
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
fn pk_vector_query_dimension(
    table: &HashMap<String, String>,
    query: &HashMap<String, String>,
    kind: &str,
    field: &DataField,
) -> crate::Result<Option<usize>> {
    let table = CoreOptions::new(table).vector_index_options(Some(kind), field.name(), false)?;
    let query = CoreOptions::new(query).vector_index_options(Some(kind), field.name(), false)?;
    resolved_query_dimension(&table.options, &query.options, kind, field)
}

#[cfg(test)]
mod tests;
