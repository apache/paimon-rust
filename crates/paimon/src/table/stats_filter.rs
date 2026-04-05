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

//! Data-level stats predicate filtering for manifest entries and data evolution groups.

use super::Table;
use crate::arrow::schema_evolution::create_index_mapping;
use crate::spec::{
    extract_datum, BinaryRow, DataField, DataFileMeta, DataType, Datum, Predicate,
    PredicateOperator,
};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(super) struct FileStatsRows {
    pub(super) row_count: i64,
    min_values: Option<BinaryRow>,
    max_values: Option<BinaryRow>,
    null_counts: Vec<Option<i64>>,
    /// Maps schema field index → stats index. `None` means identity mapping
    /// (stats cover all schema fields in order). `Some` is used when
    /// `value_stats_cols` or `write_cols` is present (dense mode).
    stats_col_mapping: Option<Vec<Option<usize>>>,
}

impl FileStatsRows {
    /// Build a `FileStatsRows` for manifest-level partition stats (no column mapping).
    pub(super) fn for_manifest_partition(
        row_count: i64,
        min_values: Option<BinaryRow>,
        max_values: Option<BinaryRow>,
        null_counts: Vec<Option<i64>>,
    ) -> Self {
        Self {
            row_count,
            min_values,
            max_values,
            null_counts,
            stats_col_mapping: None,
        }
    }

    /// Build file stats from a data file, respecting `value_stats_cols`.
    ///
    /// When `value_stats_cols` is `None`, stats cover all fields in `schema_fields` order.
    /// When `value_stats_cols` is `Some`, stats are in dense mode — only covering those
    /// columns, and the mapping from schema field index to stats index is built by name.
    pub(super) fn from_data_file(file: &DataFileMeta, schema_fields: &[DataField]) -> Self {
        // Determine which columns the stats cover and build the mapping.
        // Priority: value_stats_cols > write_cols > all schema fields.
        let stats_col_mapping = if let Some(cols) = &file.value_stats_cols {
            let mapping: Vec<Option<usize>> = schema_fields
                .iter()
                .map(|field| cols.iter().position(|c| c == field.name()))
                .collect();
            Some(mapping)
        } else if let Some(cols) = &file.write_cols {
            let mapping: Vec<Option<usize>> = schema_fields
                .iter()
                .map(|field| cols.iter().position(|c| c == field.name()))
                .collect();
            Some(mapping)
        } else {
            None
        };

        Self {
            row_count: file.row_count,
            min_values: BinaryRow::from_serialized_bytes(file.value_stats.min_values()).ok(),
            max_values: BinaryRow::from_serialized_bytes(file.value_stats.max_values()).ok(),
            null_counts: file.value_stats.null_counts().clone(),
            stats_col_mapping,
        }
    }

    /// Resolve a schema field index to the corresponding stats index.
    fn stats_index(&self, schema_index: usize) -> Option<usize> {
        match &self.stats_col_mapping {
            None => Some(schema_index),
            Some(mapping) => mapping.get(schema_index).copied().flatten(),
        }
    }

    fn null_count(&self, stats_index: usize) -> Option<i64> {
        self.null_counts.get(stats_index).copied().flatten()
    }
}

#[derive(Debug)]
pub(super) struct ResolvedStatsSchema {
    file_fields: Vec<DataField>,
    field_mapping: Vec<Option<usize>>,
}

fn identity_field_mapping(num_fields: usize) -> Vec<Option<usize>> {
    (0..num_fields).map(Some).collect()
}

fn normalize_field_mapping(mapping: Option<Vec<i32>>, num_fields: usize) -> Vec<Option<usize>> {
    mapping
        .map(|field_mapping| {
            field_mapping
                .into_iter()
                .map(|index| usize::try_from(index).ok())
                .collect()
        })
        .unwrap_or_else(|| identity_field_mapping(num_fields))
}

/// Check whether a data file *may* contain rows matching all `predicates`.
///
/// Pruning is evaluated per file and fails open when stats cannot be
/// interpreted safely, including schema mismatches, incompatible stats arity,
/// and missing or corrupted stats.
pub(super) fn data_file_matches_predicates(
    file: &DataFileMeta,
    predicates: &[Predicate],
    current_schema_id: i64,
    schema_fields: &[DataField],
) -> bool {
    if predicates.is_empty() {
        return true;
    }

    if predicates
        .iter()
        .any(|p| matches!(p, Predicate::AlwaysFalse))
    {
        return false;
    }
    if predicates
        .iter()
        .all(|p| matches!(p, Predicate::AlwaysTrue))
    {
        return true;
    }

    if file.schema_id != current_schema_id {
        return true;
    }

    let stats = FileStatsRows::from_data_file(file, schema_fields);

    predicates
        .iter()
        .all(|predicate| data_predicate_may_match(predicate, &stats))
}

async fn resolve_stats_schema(
    table: &Table,
    file_schema_id: i64,
    schema_cache: &mut HashMap<i64, Option<Arc<ResolvedStatsSchema>>>,
) -> Option<Arc<ResolvedStatsSchema>> {
    if let Some(cached) = schema_cache.get(&file_schema_id) {
        return cached.clone();
    }

    let table_schema = table.schema();
    let current_fields = table_schema.fields();
    let resolved = if file_schema_id == table_schema.id() {
        Some(Arc::new(ResolvedStatsSchema {
            file_fields: current_fields.to_vec(),
            field_mapping: identity_field_mapping(current_fields.len()),
        }))
    } else {
        let file_schema = table.schema_manager().schema(file_schema_id).await.ok()?;
        let file_fields = file_schema.fields().to_vec();
        Some(Arc::new(ResolvedStatsSchema {
            field_mapping: normalize_field_mapping(
                create_index_mapping(current_fields, &file_fields),
                current_fields.len(),
            ),
            file_fields,
        }))
    };

    schema_cache.insert(file_schema_id, resolved.clone());
    resolved
}

pub(super) async fn data_file_matches_predicates_for_table(
    table: &Table,
    file: &DataFileMeta,
    predicates: &[Predicate],
    schema_cache: &mut HashMap<i64, Option<Arc<ResolvedStatsSchema>>>,
) -> bool {
    if predicates.is_empty() {
        return true;
    }

    if file.schema_id == table.schema().id() {
        return data_file_matches_predicates(
            file,
            predicates,
            table.schema().id(),
            table.schema().fields(),
        );
    }

    let Some(resolved) = resolve_stats_schema(table, file.schema_id, schema_cache).await else {
        return true;
    };

    let stats = FileStatsRows::from_data_file(file, &resolved.file_fields);

    predicates.iter().all(|predicate| {
        data_predicate_may_match_with_schema(
            predicate,
            &stats,
            &resolved.field_mapping,
            &resolved.file_fields,
        )
    })
}

fn data_predicate_may_match(predicate: &Predicate, stats: &FileStatsRows) -> bool {
    match predicate {
        Predicate::AlwaysTrue => true,
        Predicate::AlwaysFalse => false,
        Predicate::And(children) => children
            .iter()
            .all(|child| data_predicate_may_match(child, stats)),
        Predicate::Or(_) | Predicate::Not(_) => true,
        Predicate::Leaf {
            index,
            data_type,
            op,
            literals,
            ..
        } => {
            let Some(stats_idx) = stats.stats_index(*index) else {
                return true;
            };
            data_leaf_may_match(stats_idx, data_type, data_type, *op, literals, stats)
        }
    }
}

fn data_predicate_may_match_with_schema(
    predicate: &Predicate,
    stats: &FileStatsRows,
    field_mapping: &[Option<usize>],
    file_fields: &[DataField],
) -> bool {
    match predicate {
        Predicate::AlwaysTrue => true,
        Predicate::AlwaysFalse => false,
        Predicate::And(children) => children.iter().all(|child| {
            data_predicate_may_match_with_schema(child, stats, field_mapping, file_fields)
        }),
        Predicate::Or(_) | Predicate::Not(_) => true,
        Predicate::Leaf {
            index,
            data_type,
            op,
            literals,
            ..
        } => match field_mapping.get(*index).copied().flatten() {
            Some(file_index) => {
                let Some(file_field) = file_fields.get(file_index) else {
                    return true;
                };
                let Some(stats_idx) = stats.stats_index(file_index) else {
                    return true;
                };
                data_leaf_may_match(
                    stats_idx,
                    file_field.data_type(),
                    data_type,
                    *op,
                    literals,
                    stats,
                )
            }
            None => missing_field_may_match(*op, stats.row_count),
        },
    }
}

pub(super) fn data_leaf_may_match(
    index: usize,
    stats_data_type: &DataType,
    predicate_data_type: &DataType,
    op: PredicateOperator,
    literals: &[Datum],
    stats: &FileStatsRows,
) -> bool {
    let row_count = stats.row_count;
    if row_count <= 0 {
        return false;
    }

    let null_count = stats.null_count(index);
    let all_null = null_count.map(|count| count == row_count);

    match op {
        PredicateOperator::IsNull => {
            return null_count.is_none_or(|count| count > 0);
        }
        PredicateOperator::IsNotNull => {
            return all_null != Some(true);
        }
        PredicateOperator::In | PredicateOperator::NotIn => {
            return true;
        }
        PredicateOperator::Eq
        | PredicateOperator::NotEq
        | PredicateOperator::Lt
        | PredicateOperator::LtEq
        | PredicateOperator::Gt
        | PredicateOperator::GtEq => {}
    }

    if all_null == Some(true) {
        return false;
    }

    let literal = match literals.first() {
        Some(literal) => literal,
        None => return true,
    };

    let min_value = match stats
        .min_values
        .as_ref()
        .and_then(|row| extract_stats_datum(row, index, stats_data_type))
        .and_then(|datum| coerce_stats_datum_for_predicate(datum, predicate_data_type))
    {
        Some(value) => value,
        None => return true,
    };
    let max_value = match stats
        .max_values
        .as_ref()
        .and_then(|row| extract_stats_datum(row, index, stats_data_type))
        .and_then(|datum| coerce_stats_datum_for_predicate(datum, predicate_data_type))
    {
        Some(value) => value,
        None => return true,
    };

    match op {
        PredicateOperator::Eq => {
            !matches!(literal.partial_cmp(&min_value), Some(Ordering::Less))
                && !matches!(literal.partial_cmp(&max_value), Some(Ordering::Greater))
        }
        PredicateOperator::NotEq => !(min_value == *literal && max_value == *literal),
        PredicateOperator::Lt => !matches!(
            min_value.partial_cmp(literal),
            Some(Ordering::Greater | Ordering::Equal)
        ),
        PredicateOperator::LtEq => {
            !matches!(min_value.partial_cmp(literal), Some(Ordering::Greater))
        }
        PredicateOperator::Gt => !matches!(
            max_value.partial_cmp(literal),
            Some(Ordering::Less | Ordering::Equal)
        ),
        PredicateOperator::GtEq => !matches!(max_value.partial_cmp(literal), Some(Ordering::Less)),
        PredicateOperator::IsNull
        | PredicateOperator::IsNotNull
        | PredicateOperator::In
        | PredicateOperator::NotIn => true,
    }
}

fn missing_field_may_match(op: PredicateOperator, row_count: i64) -> bool {
    if row_count <= 0 {
        return false;
    }

    matches!(op, PredicateOperator::IsNull)
}

fn coerce_stats_datum_for_predicate(datum: Datum, predicate_data_type: &DataType) -> Option<Datum> {
    match (datum, predicate_data_type) {
        (datum @ Datum::Bool(_), DataType::Boolean(_))
        | (datum @ Datum::TinyInt(_), DataType::TinyInt(_))
        | (datum @ Datum::SmallInt(_), DataType::SmallInt(_))
        | (datum @ Datum::Int(_), DataType::Int(_))
        | (datum @ Datum::Long(_), DataType::BigInt(_))
        | (datum @ Datum::Float(_), DataType::Float(_))
        | (datum @ Datum::Double(_), DataType::Double(_))
        | (datum @ Datum::String(_), DataType::VarChar(_))
        | (datum @ Datum::String(_), DataType::Char(_))
        | (datum @ Datum::Bytes(_), DataType::Binary(_))
        | (datum @ Datum::Bytes(_), DataType::VarBinary(_))
        | (datum @ Datum::Date(_), DataType::Date(_))
        | (datum @ Datum::Time(_), DataType::Time(_))
        | (datum @ Datum::Timestamp { .. }, DataType::Timestamp(_))
        | (datum @ Datum::LocalZonedTimestamp { .. }, DataType::LocalZonedTimestamp(_))
        | (datum @ Datum::Decimal { .. }, DataType::Decimal(_)) => Some(datum),
        (Datum::TinyInt(value), DataType::SmallInt(_)) => Some(Datum::SmallInt(value as i16)),
        (Datum::TinyInt(value), DataType::Int(_)) => Some(Datum::Int(value as i32)),
        (Datum::TinyInt(value), DataType::BigInt(_)) => Some(Datum::Long(value as i64)),
        (Datum::SmallInt(value), DataType::Int(_)) => Some(Datum::Int(value as i32)),
        (Datum::SmallInt(value), DataType::BigInt(_)) => Some(Datum::Long(value as i64)),
        (Datum::Int(value), DataType::BigInt(_)) => Some(Datum::Long(value as i64)),
        (Datum::Float(value), DataType::Double(_)) => Some(Datum::Double(value as f64)),
        _ => None,
    }
}

fn extract_stats_datum(row: &BinaryRow, index: usize, data_type: &DataType) -> Option<Datum> {
    let min_row_len = BinaryRow::cal_fix_part_size_in_bytes(row.arity()) as usize;
    if index >= row.arity() as usize || row.data().len() < min_row_len {
        return None;
    }

    match extract_datum(row, index, data_type) {
        Ok(Some(datum)) => Some(datum),
        Ok(None) | Err(_) => None,
    }
}

/// Check whether a data-evolution file group *may* contain rows matching all `predicates`.
///
/// In data evolution mode, a logical row can be spread across multiple files with
/// different column sets. After `group_by_overlapping_row_id`, each group contains
/// files covering the same row ID range. Stats for each field come from the file
/// with the highest `max_sequence_number` that actually contains that field.
///
/// Reference: [DataEvolutionFileStoreScan.evolutionStats](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/operation/DataEvolutionFileStoreScan.java)
pub(super) fn data_evolution_group_matches_predicates(
    group: &[DataFileMeta],
    predicates: &[Predicate],
    table_fields: &[DataField],
) -> bool {
    if predicates.is_empty() || group.is_empty() {
        return true;
    }

    if predicates
        .iter()
        .any(|p| matches!(p, Predicate::AlwaysFalse))
    {
        return false;
    }
    if predicates
        .iter()
        .all(|p| matches!(p, Predicate::AlwaysTrue))
    {
        return true;
    }

    // Sort files by max_sequence_number descending so the highest-seq file wins per field.
    let mut sorted_files: Vec<&DataFileMeta> = group.iter().collect();
    sorted_files.sort_by(|a, b| b.max_sequence_number.cmp(&a.max_sequence_number));

    // For each table field, find which file (index in sorted_files) provides it,
    // and the field's offset within that file's stats.
    let field_sources: Vec<Option<(usize, usize)>> = table_fields
        .iter()
        .map(|field| {
            for (file_idx, file) in sorted_files.iter().enumerate() {
                let file_columns = file_stats_columns(file, table_fields);
                for (stats_idx, col_name) in file_columns.iter().enumerate() {
                    if *col_name == field.name() {
                        return Some((file_idx, stats_idx));
                    }
                }
            }
            None
        })
        .collect();

    // Build per-file stats without arity validation — data evolution files
    // may have fewer columns than the current table schema.
    let file_stats: Vec<FileStatsRows> = sorted_files
        .iter()
        .map(|file| FileStatsRows::from_data_file(file, table_fields))
        .collect();

    // row_count is the max across the group (overlapping row ranges).
    let row_count = group.iter().map(|f| f.row_count).max().unwrap_or(0);

    predicates.iter().all(|predicate| {
        data_evolution_predicate_may_match(
            predicate,
            table_fields,
            &field_sources,
            &file_stats,
            row_count,
        )
    })
}

/// Resolve which columns a file's value stats cover.
/// If `value_stats_cols` is set, those are the stats columns. Otherwise, the file's stats
/// cover all table fields (or `write_cols` if present).
fn file_stats_columns<'a>(file: &'a DataFileMeta, table_fields: &'a [DataField]) -> Vec<&'a str> {
    if let Some(cols) = &file.value_stats_cols {
        return cols.iter().map(|s| s.as_str()).collect();
    }
    match &file.write_cols {
        Some(cols) => cols.iter().map(|s| s.as_str()).collect(),
        None => table_fields.iter().map(|f| f.name()).collect(),
    }
}

fn data_evolution_predicate_may_match(
    predicate: &Predicate,
    table_fields: &[DataField],
    field_sources: &[Option<(usize, usize)>],
    file_stats: &[FileStatsRows],
    row_count: i64,
) -> bool {
    match predicate {
        Predicate::AlwaysTrue => true,
        Predicate::AlwaysFalse => false,
        Predicate::And(children) => children.iter().all(|child| {
            data_evolution_predicate_may_match(
                child,
                table_fields,
                field_sources,
                file_stats,
                row_count,
            )
        }),
        Predicate::Or(_) | Predicate::Not(_) => true,
        Predicate::Leaf {
            index,
            data_type,
            op,
            literals,
            ..
        } => {
            let Some(source) = field_sources.get(*index).copied().flatten() else {
                return missing_field_may_match(*op, row_count);
            };
            let (file_idx, stats_idx) = source;
            let stats = &file_stats[file_idx];
            let stats_data_type = table_fields
                .get(*index)
                .map(|f| f.data_type())
                .unwrap_or(data_type);
            data_leaf_may_match(stats_idx, stats_data_type, data_type, *op, literals, stats)
        }
    }
}

/// Groups data files by overlapping `row_id_range` for data evolution.
///
/// Files are sorted by `(first_row_id, -max_sequence_number)`. Files whose row ID ranges
/// overlap are merged into the same group (they contain different columns for the same rows).
/// Files without `first_row_id` become their own group.
///
/// Reference: [DataEvolutionSplitGenerator](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/table/source/splitread/DataEvolutionSplitGenerator.java)
pub(crate) fn group_by_overlapping_row_id(mut files: Vec<DataFileMeta>) -> Vec<Vec<DataFileMeta>> {
    files.sort_by(|a, b| {
        let a_row_id = a.first_row_id.unwrap_or(i64::MIN);
        let b_row_id = b.first_row_id.unwrap_or(i64::MIN);
        a_row_id
            .cmp(&b_row_id)
            .then_with(|| b.max_sequence_number.cmp(&a.max_sequence_number))
    });

    let mut result: Vec<Vec<DataFileMeta>> = Vec::new();
    let mut current_group: Vec<DataFileMeta> = Vec::new();
    let mut current_range_end: i64 = i64::MIN;

    for file in files {
        match file.row_id_range() {
            None => {
                if !current_group.is_empty() {
                    result.push(std::mem::take(&mut current_group));
                    current_range_end = i64::MIN;
                }
                result.push(vec![file]);
            }
            Some((start, end)) => {
                if current_group.is_empty() || start <= current_range_end {
                    if end > current_range_end {
                        current_range_end = end;
                    }
                    current_group.push(file);
                } else {
                    result.push(std::mem::take(&mut current_group));
                    current_range_end = end;
                    current_group.push(file);
                }
            }
        }
    }
    if !current_group.is_empty() {
        result.push(current_group);
    }
    result
}
