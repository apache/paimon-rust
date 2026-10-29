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

use arrow_array::{Int8Array, RecordBatch, UInt64Array};
use arrow_schema::SchemaRef;

use crate::spec::{DataField, MergeEngine, RowKind, VALUE_KIND_FIELD_NAME};
use crate::table::kv_file_reader::KeyValueReadConfig;
use crate::table::sort_merge::{lookup_rows, BufferedBatch, MergeFunction, MergeResult, MergeRow};
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
        let result = self.inner.merge(&selected, batches, columns, schema)?;
        if !has_l0 {
            return Ok((result, None));
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
            (Some(before), None) if !self.first_row && !self.options.ignore_delete => {
                changes.push(with_kind(before, RowKind::Delete)?)
            }
            (Some(before), Some(after)) if !self.first_row => {
                let equal = self.options.deduplicate
                    && self.value_columns.iter().all(|index| {
                        before.column(*index).to_data() == after.column(*index).to_data()
                    });
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
