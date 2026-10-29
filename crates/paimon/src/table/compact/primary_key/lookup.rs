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

//! Per-key lookup changelog generation, matching Java's lookup merge wrapper.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{cast::AsArray, Array, ArrayRef, Int8Array, MapArray, RecordBatch, UInt64Array};
use arrow_row::{RowConverter, SortField};
use arrow_schema::{DataType as ArrowType, SchemaRef};

use crate::spec::{DataField, MergeEngine, RowKind, VALUE_KIND_FIELD_NAME};
use crate::table::kv_file_reader::KeyValueReadConfig;
use crate::table::sort_merge::{
    lookup_rows, BufferedBatch, MergeFunction, MergeResult, MergeRow, FILE_LEVEL_METADATA_KEY,
};
use crate::{Error, Result};

struct ChangelogOptions {
    deduplicate: bool,
    ignore_update_before: bool,
    ignore_delete: bool,
    ignored_fields: HashSet<String>,
}

impl ChangelogOptions {
    fn parse(options: &HashMap<String, String>) -> Result<Self> {
        let boolean = |key: &str| match options.get(key).map(String::as_str) {
            None => Ok(false),
            Some(value) if value.eq_ignore_ascii_case("true") => Ok(true),
            Some(value) if value.eq_ignore_ascii_case("false") => Ok(false),
            Some(value) => Err(Error::ConfigInvalid {
                message: format!("{key} must be true or false, got {value}"),
            }),
        };
        Ok(Self {
            deduplicate: boolean("changelog-producer.row-deduplicate")?,
            ignore_update_before: boolean("changelog-producer.ignore-update-before")?,
            ignore_delete: boolean("changelog-producer.ignore-delete")?,
            ignored_fields: options
                .get("changelog-producer.row-deduplicate-ignore-fields")
                .map(|fields| {
                    fields
                        .split(',')
                        .map(str::trim)
                        .filter(|field| !field.is_empty())
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default(),
        })
    }
}

pub(in crate::table::compact) fn validate_options(options: &HashMap<String, String>) -> Result<()> {
    ChangelogOptions::parse(options).map(|_| ())
}

struct LookupChangelogMergeFunction {
    inner: Box<dyn MergeFunction>,
    first_row: bool,
    options: ChangelogOptions,
    value_columns: Vec<usize>,
}

pub(super) fn new_merge_function(
    config: &KeyValueReadConfig,
    fields: &[DataField],
) -> Result<Box<dyn MergeFunction>> {
    let options = ChangelogOptions::parse(&config.table_options)?;
    Ok(Box::new(LookupChangelogMergeFunction {
        inner: super::new_merge_function(config, fields)?,
        first_row: config.merge_engine == MergeEngine::FirstRow,
        value_columns: fields
            .iter()
            .enumerate()
            .filter(|(_, field)| {
                config
                    .table_fields
                    .iter()
                    .any(|value| value.id() == field.id())
                    && !options.ignored_fields.contains(field.name())
            })
            .map(|(index, _)| index)
            .collect(),
        options,
    }))
}

fn source_row(
    batch_idx: usize,
    row_idx: usize,
    batches: &[BufferedBatch],
    columns: &[usize],
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let indices = UInt64Array::from(vec![row_idx as u64]);
    let values = (0..schema.fields().len())
        .map(|index| {
            arrow_select::take::take(
                batches[batch_idx].column_for_output(index, columns),
                &indices,
                None,
            )
            .map_err(|error| invalid(format!("Failed to materialize lookup value: {error}")))
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(schema.clone(), values)
        .map_err(|error| invalid(format!("Invalid lookup value: {error}")))
}

fn result_row(
    result: &MergeResult,
    batches: &[BufferedBatch],
    columns: &[usize],
    schema: &SchemaRef,
) -> Result<Option<RecordBatch>> {
    match result {
        MergeResult::SourceRow { batch_idx, row_idx } => {
            source_row(*batch_idx, *row_idx, batches, columns, schema).map(Some)
        }
        MergeResult::MaterializedRow(batch) | MergeResult::MaterializedDeleteRow(batch) => {
            Ok(Some(batch.clone()))
        }
        MergeResult::Omit => Ok(None),
    }
}

fn live_row(batch: Option<RecordBatch>) -> Result<Option<RecordBatch>> {
    match batch {
        Some(batch) => {
            let kinds = batch
                .column_by_name(VALUE_KIND_FIELD_NAME)
                .and_then(|column| column.as_any().downcast_ref::<Int8Array>())
                .ok_or_else(|| invalid("Lookup value must carry its row kind"))?;
            Ok(RowKind::from_value(kinds.value(0))?
                .is_add()
                .then_some(batch))
        }
        None => Ok(None),
    }
}

fn with_kind(batch: &RecordBatch, kind: RowKind) -> Result<RecordBatch> {
    let mut columns = batch.columns().to_vec();
    let index = batch
        .schema()
        .index_of(VALUE_KIND_FIELD_NAME)
        .map_err(|error| invalid(error.to_string()))?;
    columns[index] = Arc::new(Int8Array::from(vec![kind as i8]));
    RecordBatch::try_new(batch.schema(), columns).map_err(|error| invalid(error.to_string()))
}

fn contains_float(data_type: &ArrowType) -> bool {
    match data_type {
        ArrowType::Float32 | ArrowType::Float64 => true,
        ArrowType::List(field) | ArrowType::LargeList(field) | ArrowType::Map(field, _) => {
            contains_float(field.data_type())
        }
        ArrowType::FixedSizeList(field, _) => contains_float(field.data_type()),
        ArrowType::Struct(fields) => fields.iter().any(|field| contains_float(field.data_type())),
        _ => false,
    }
}

/// Java compares Maps by key and nested rows field by field. Binary arrays
/// retain their physical shortcut unless a floating-point descendant disables it.
fn values_equal(left: &ArrayRef, right: &ArrayRef) -> bool {
    if left.to_data() == right.to_data() {
        return true;
    }
    if left.len() != right.len() || left.data_type() != right.data_type() {
        return false;
    }
    for row in 0..left.len() {
        if left.is_null(row) || right.is_null(row) {
            if left.is_null(row) != right.is_null(row) {
                return false;
            }
            continue;
        }
        let equal = match left.data_type() {
            ArrowType::Map(_, _) => {
                let left = left.as_any().downcast_ref::<MapArray>().unwrap().value(row);
                let right = right
                    .as_any()
                    .downcast_ref::<MapArray>()
                    .unwrap()
                    .value(row);
                map_entries_equal(&left, &right)
            }
            ArrowType::Struct(_) => left
                .as_struct()
                .columns()
                .iter()
                .zip(right.as_struct().columns())
                .all(|(left, right)| values_equal(&left.slice(row, 1), &right.slice(row, 1))),
            kind if contains_float(kind) => match kind {
                ArrowType::List(_) => values_equal(
                    &left.as_list::<i32>().value(row),
                    &right.as_list::<i32>().value(row),
                ),
                ArrowType::LargeList(_) => values_equal(
                    &left.as_list::<i64>().value(row),
                    &right.as_list::<i64>().value(row),
                ),
                ArrowType::FixedSizeList(_, _) => values_equal(
                    &left.as_fixed_size_list().value(row),
                    &right.as_fixed_size_list().value(row),
                ),
                _ => left.slice(row, 1).to_data() == right.slice(row, 1).to_data(),
            },
            _ => left.slice(row, 1).to_data() == right.slice(row, 1).to_data(),
        };
        if !equal {
            return false;
        }
    }
    true
}

fn map_entries_equal(left: &arrow_array::StructArray, right: &arrow_array::StructArray) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let encoded = RowConverter::new(vec![SortField::new(left.column(0).data_type().clone())])
        .and_then(|converter| {
            Ok((
                converter.convert_columns(&[left.column(0).clone()])?,
                converter.convert_columns(&[right.column(0).clone()])?,
            ))
        })
        .ok();
    let mut by_key = HashMap::<Vec<u8>, Vec<usize>>::new();
    if let Some((_, keys)) = &encoded {
        for row in 0..right.len() {
            by_key
                .entry(keys.row(row).as_ref().to_vec())
                .or_default()
                .push(row);
        }
    }
    let all = (0..right.len()).collect::<Vec<_>>();
    let mut matched = vec![false; right.len()];
    for row in 0..left.len() {
        let candidates = match &encoded {
            Some((keys, _)) => by_key
                .get(keys.row(row).as_ref())
                .map(Vec::as_slice)
                .unwrap_or(&[]),
            None => &all,
        };
        let found = candidates.iter().copied().find(|&other| {
            !matched[other]
                && (encoded.is_some()
                    || values_equal(
                        &left.column(0).slice(row, 1),
                        &right.column(0).slice(other, 1),
                    ))
                && values_equal(
                    &left.column(1).slice(row, 1),
                    &right.column(1).slice(other, 1),
                )
        });
        let Some(other) = found else {
            return false;
        };
        matched[other] = true;
    }
    true
}

impl MergeFunction for LookupChangelogMergeFunction {
    fn merge(
        &self,
        rows: &[MergeRow],
        batches: &[BufferedBatch],
        columns: &[usize],
        schema: &SchemaRef,
    ) -> Result<MergeResult> {
        self.merge_with_changelog(rows, batches, columns, schema)
            .map(|(result, _)| result)
    }

    fn merge_with_changelog(
        &self,
        rows: &[MergeRow],
        batches: &[BufferedBatch],
        columns: &[usize],
        schema: &SchemaRef,
    ) -> Result<(MergeResult, Option<RecordBatch>)> {
        let (base, has_l0, selected) = lookup_rows(rows, batches)?;
        // Java deliberately excludes first-row from LookupMergeFunction: an
        // older, higher-level row can still be the first accepted record.
        let result = self.inner.merge(
            if self.first_row { rows } else { &selected },
            batches,
            columns,
            schema,
        )?;
        if !has_l0 {
            return Ok((result, None));
        }
        if self.first_row {
            let contains_history = rows.iter().any(|row| {
                matches!(row.value_kind, 0 | 2)
                    && matches!(&batches[row.batch_idx], BufferedBatch::Source(batch)
                        if batch.schema().metadata().get(FILE_LEVEL_METADATA_KEY)
                            .and_then(|level| level.parse::<i32>().ok())
                            .is_some_and(|level| level > 0))
            });
            // FirstRowMergeFunctionWrapper emits the first add unchanged,
            // including UPDATE_AFTER; an existing accepted history emits none.
            let changelog = if contains_history {
                None
            } else {
                live_row(result_row(&result, batches, columns, schema)?)?
            };
            return Ok((result, changelog));
        }
        let before = live_row(
            base.as_ref()
                .map(|row| source_row(row.batch_idx, row.row_idx, batches, columns, schema))
                .transpose()?,
        )?;
        let after = live_row(result_row(&result, batches, columns, schema)?)?;
        let mut changes = Vec::with_capacity(2);
        match (&before, &after) {
            (None, Some(after)) => changes.push(with_kind(after, RowKind::Insert)?),
            (Some(before), None) if !self.options.ignore_delete => {
                changes.push(with_kind(before, RowKind::Delete)?)
            }
            (Some(before), Some(after)) => {
                let equal = self.options.deduplicate
                    && self
                        .value_columns
                        .iter()
                        .all(|index| values_equal(before.column(*index), after.column(*index)));
                if !equal {
                    if !self.options.ignore_update_before {
                        changes.push(with_kind(before, RowKind::UpdateBefore)?);
                    }
                    changes.push(with_kind(after, RowKind::UpdateAfter)?);
                }
            }
            _ => {}
        }
        let changelog = if changes.is_empty() {
            None
        } else {
            Some(
                arrow_select::concat::concat_batches(schema, &changes)
                    .map_err(|error| invalid(error.to_string()))?,
            )
        };
        Ok((result, changelog))
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}
