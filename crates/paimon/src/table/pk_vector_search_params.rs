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

//! Resolve and validate PK vector-search parameters before scan planning.

use crate::lumina::{is_lumina_index_type, LuminaVectorIndexOptions};
use crate::spec::{CoreOptions, DataField, DataType, GlobalIndexSearchMode, Predicate};
use crate::table::bucket_filter::split_partition_and_data_predicates;
use crate::table::vector_search_common::{configured_refine_factor, indexed_search_limit};
use crate::table::Table;
use crate::vindex::pkvector::exact::validate_query;
use crate::vindex::pkvector::metric::VectorSearchMetric;
use crate::vindex::VindexVectorIndexOptions;
use std::collections::HashMap;

/// Query-level parameters for a primary-key vector search: everything resolvable
/// from the table schema, the options and the queries alone, independent of which
/// splits planning yields. Resolved before planning so a malformed query or option
/// fails loud even when the plan turns out empty.
pub(super) struct PkVectorSearchParams {
    pub(super) metric: VectorSearchMetric,
    /// Fan-out limit for bucket orchestration plus ANN and exact-file leaves (Java
    /// `GLOBAL_INDEX_THREAD_NUM`); `1` reproduces strictly sequential execution.
    pub(super) concurrency: usize,
    pub(super) index_type: String,
    pub(super) vector_field: DataField,
    pub(super) skip_exact_fallback: bool,
    pub(super) refine_factor: usize,
    pub(super) indexed_limit: usize,
}

impl PkVectorSearchParams {
    /// Resolve the query-level parameters and reject a query the search cannot answer
    /// correctly, before any planning or read happens.
    pub(super) fn resolve(
        table: &Table,
        query_options: &HashMap<String, String>,
        filter: Option<&Predicate>,
        pk_col: &str,
        queries: &[&[f32]],
        limit: usize,
    ) -> crate::Result<Self> {
        let core = CoreOptions::new(table.schema().options());
        core.ensure_read_authorized()?;
        // Residual pre-filter guard, mirroring Java `PrimaryKeyVectorScan`. A DATA
        // predicate set via `with_filter` is applied post-recall by re-reading each
        // candidate file's physical rows during search. That physical-position filtering
        // only agrees with the bucket search when the table exposes physical rows
        // directly: deletion vectors enabled and merge-on-read disabled. Under
        // merge-on-read (or without deletion vectors) a read merges multiple key
        // versions, so a scalar filter could retain a stale version whose live version
        // does not match — a silent wrong-read. Reject such queries rather than answer
        // them incorrectly.
        //
        // Guard on the DATA conjuncts, not the whole filter: partition-only conjuncts
        // are enforced entirely by scan planning (partition pruning) and produce no
        // per-row residual, so they need no physical-row read. This mirrors Java, where
        // `BatchVectorSearchBuilderImpl.withFilter` splits at the builder level and
        // leaves `this.filter == null` for a partition-only filter — the scan guard is
        // then skipped. No data predicate (partition-only or no filter) → nothing to
        // guard, so the search-only and read paths are unaffected.
        let physical_row_read =
            core.deletion_vectors_enabled() && !core.deletion_vectors_merge_on_read();
        let has_data_predicate = filter.is_some_and(|f| {
            let (_partition, data) = split_partition_and_data_predicates(
                f.clone(),
                table.schema().fields(),
                table.schema().partition_keys(),
            );
            !data.is_empty()
        });
        if has_data_predicate && !physical_row_read {
            return Err(crate::Error::DataInvalid {
                message:
                    "primary-key vector pre-filter requires deletion vectors without merge-on-read"
                        .to_string(),
                source: None,
            });
        }
        // `primary_key_vector_distance_metric` returns a validated name; re-parse into
        // the enum for the numeric semantics.
        let metric = VectorSearchMetric::parse(&core.primary_key_vector_distance_metric(pk_col)?)?;
        // Fan-out limit for bucket orchestration plus ANN and exact-file leaves (Java
        // `GLOBAL_INDEX_THREAD_NUM`); `1` reproduces strictly sequential execution.
        let concurrency = core.global_index_thread_num()?;
        let index_type = core.primary_key_vector_index_type(pk_col)?;
        let vector_field = table
            .schema()
            .fields()
            .iter()
            .find(|f| f.name() == pk_col)
            .cloned()
            .ok_or_else(|| crate::Error::DataInvalid {
                message: format!("PK-vector column '{pk_col}' not found in schema"),
                source: None,
            })?;

        let search_mode = core.vector_index_search_mode()?;
        let skip_exact_fallback = search_mode == GlobalIndexSearchMode::Fast;

        // A non-positive limit is invalid regardless of the plan; reject it before
        // planning so an empty plan cannot mask it with empty results.
        if limit == 0 {
            return Err(crate::Error::DataInvalid {
                message: "vector search limit must be positive".to_string(),
                source: None,
            });
        }

        // Resolve the refine factor from the query options first, then fall back to
        // the table options; a positive factor over-fetches indexed (approximate)
        // candidates so the reader's exact rerank has a wider pool to reorder. Factor 0
        // (unset) leaves `indexed_limit == limit`, byte-identical to the no-rerank
        // path. The two option maps are kept distinct (query options passed
        // separately from table options) so a broad query key cannot be overridden
        // by a more specific table key: query options take precedence as a whole.
        // Resolved before planning so an invalid factor (e.g. a non-numeric value)
        // fails loud regardless of whether the table currently has searchable data.
        let refine_factor =
            configured_refine_factor(query_options, table.schema().options(), pk_col, &index_type)?;
        let indexed_limit = indexed_search_limit(limit, refine_factor)?;

        // Validate every query against the vector column's dimension (and finiteness)
        // before planning or any read, so a malformed query fails loud even when the
        // plan turns out empty. VECTOR<FLOAT> carries the dimension in its type;
        // ARRAY<FLOAT> gets the index dimension from the same vindex option resolver
        // used by index reads. Both valid PK-vector column shapes must reject NaN/Inf
        // up front, not only after a non-empty plan opens readers.
        if let Some(dimension) = pk_vector_query_dimension(
            table.schema().options(),
            query_options,
            &index_type,
            &vector_field,
        )? {
            for query in queries {
                validate_query(query, dimension)?;
            }
        }

        Ok(Self {
            metric,
            concurrency,
            index_type,
            vector_field,
            skip_exact_fallback,
            refine_factor,
            indexed_limit,
        })
    }
}

fn pk_vector_query_dimension(
    table_options: &HashMap<String, String>,
    query_options: &HashMap<String, String>,
    index_type: &str,
    vector_field: &DataField,
) -> crate::Result<Option<usize>> {
    match vector_field.data_type() {
        DataType::Vector(vector_type)
            if matches!(vector_type.element_type(), DataType::Float(_)) =>
        {
            Ok(Some(vector_type.length() as usize))
        }
        DataType::Array(array_type) if matches!(array_type.element_type(), DataType::Float(_)) => {
            // Resolve the dimension per the configured backend. An `ARRAY<FLOAT>`
            // column carries no dimension in its type, so it comes from options —
            // but the option shape differs by backend. Lumina is not a vindex
            // index type, so routing it through `VindexVectorIndexOptions` would
            // reject it as unsupported before planning (even on an empty table).
            if is_lumina_index_type(index_type) {
                // Lumina reads `lumina.index.dimension` (default 128) from the
                // merged table+query options, matching `resolve_lumina_options`.
                let mut merged = table_options.clone();
                merged.extend(query_options.clone());
                let dimension = LuminaVectorIndexOptions::new(&merged)?.dimension;
                Ok(Some(dimension as usize))
            } else {
                let mut dimension_options = HashMap::new();
                for key in [
                    "dimension".to_string(),
                    format!("{index_type}.dimension"),
                    format!("fields.{}.dimension", vector_field.name()),
                ] {
                    if let Some(value) = query_options.get(&key) {
                        dimension_options.insert(key, value.clone());
                    }
                }
                Ok(Some(
                    VindexVectorIndexOptions::new(
                        table_options,
                        &dimension_options,
                        index_type,
                        vector_field,
                    )?
                    .dimension(),
                ))
            }
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests;
