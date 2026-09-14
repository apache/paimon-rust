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

//! Merge policies for current-state audit reads.

use super::super::sort_merge::{
    compare_sequence_order, AggregateMergeFunction, BufferedBatch, MergeFunction, MergeResult,
    MergeRow, PartialUpdateMergeFunction,
};
use crate::arrow::build_target_arrow_schema;
use crate::spec::{
    CoreOptions, DataField, MergeEngine, RowKind, SEQUENCE_NUMBER_FIELD_ID, VALUE_KIND_FIELD_ID,
};
use crate::table::kv_file_reader::KeyValueReadConfig;
use crate::Error;
use arrow_array::{Int64Array, Int8Array, RecordBatch};
use arrow_schema::SchemaRef;
use std::sync::Arc;

pub(in crate::table) fn new_merge_function(
    config: &KeyValueReadConfig,
    fields: &[DataField],
) -> crate::Result<Box<dyn MergeFunction>> {
    let options = &config.table_options;
    let engine = config.merge_engine;
    if matches!(engine, MergeEngine::Deduplicate | MergeEngine::FirstRow) {
        return Ok(Box::new(AuditKeyMergeFunction {
            first_row: engine == MergeEngine::FirstRow,
            ignore_delete: CoreOptions::new(options).ignore_delete(),
        }));
    }
    let value_projection: Vec<_> = fields
        .iter()
        .enumerate()
        .filter(|(_, field)| !matches!(field.id(), SEQUENCE_NUMBER_FIELD_ID | VALUE_KIND_FIELD_ID))
        .map(|(index, _)| index)
        .collect();
    let value_fields: Vec<_> = value_projection
        .iter()
        .map(|&index| fields[index].clone())
        .collect();
    let inner: Box<dyn MergeFunction> = match engine {
        MergeEngine::PartialUpdate => Box::new(PartialUpdateMergeFunction::new_with_schema(
            options,
            &config.table_name,
            &config.table_fields,
            &value_fields,
            &config.primary_keys,
        )?),
        MergeEngine::Aggregation => Box::new(AggregateMergeFunction::new(
            options,
            &config.table_name,
            &value_fields,
            &config.primary_keys,
            &config.sequence_fields,
        )?),
        _ => unreachable!(),
    };
    if value_projection.len() == fields.len() {
        return Ok(inner);
    }
    Ok(Box::new(AuditValueMergeFunction {
        inner,
        value_projection,
        value_schema: build_target_arrow_schema(&value_fields)?,
        fields: fields.to_vec(),
    }))
}

// Reuse the ordinary value merge, then expose its logical INSERT and latest add sequence.
struct AuditValueMergeFunction {
    inner: Box<dyn MergeFunction>,
    value_projection: Vec<usize>,
    value_schema: SchemaRef,
    fields: Vec<DataField>,
}

impl MergeFunction for AuditValueMergeFunction {
    fn merge(
        &self,
        rows: &[MergeRow],
        batch_buffer: &[BufferedBatch],
        source_output_col_indices: &[usize],
        output_schema: &SchemaRef,
    ) -> crate::Result<MergeResult> {
        let indices: Vec<_> = self
            .value_projection
            .iter()
            .map(|&index| source_output_col_indices[index])
            .collect();
        match self
            .inner
            .merge(rows, batch_buffer, &indices, &self.value_schema)?
        {
            MergeResult::MaterializedRow(batch) => {
                let mut columns = batch.columns().to_vec();
                for (index, field) in self.fields.iter().enumerate() {
                    match field.id() {
                        SEQUENCE_NUMBER_FIELD_ID => {
                            let winner = rows
                                .iter()
                                .filter(|row| matches!(row.value_kind, 0 | 2))
                                .max_by(|left, right| compare_sequence_order(left, right))
                                .expect("materialized merge must contain an add row");
                            columns.insert(
                                index,
                                Arc::new(Int64Array::from(vec![winner.sequence_number])),
                            );
                        }
                        VALUE_KIND_FIELD_ID => {
                            columns.insert(index, Arc::new(Int8Array::from(vec![0])))
                        }
                        _ => {}
                    }
                }
                Ok(MergeResult::MaterializedRow(
                    RecordBatch::try_new(output_schema.clone(), columns).map_err(|error| {
                        Error::UnexpectedError {
                            message: format!("Failed to build audit merge row: {error}"),
                            source: Some(Box::new(error)),
                        }
                    })?,
                ))
            }
            result => Ok(result),
        }
    }
}

/// Keep the winning physical row, including retracts for deduplicate tables.
struct AuditKeyMergeFunction {
    first_row: bool,
    ignore_delete: bool,
}

impl MergeFunction for AuditKeyMergeFunction {
    fn merge(
        &self,
        rows: &[MergeRow],
        _batch_buffer: &[BufferedBatch],
        _source_output_col_indices: &[usize],
        _output_schema: &SchemaRef,
    ) -> crate::Result<MergeResult> {
        let mut winner = None;
        for row in rows {
            if (self.first_row || self.ignore_delete)
                && !RowKind::from_value(row.value_kind)?.is_add()
            {
                if self.ignore_delete {
                    continue;
                }
                return Err(Error::Unsupported {
                    message: "merge-engine=first-row does not support DELETE or UPDATE_BEFORE rows; set ignore-delete=true to ignore them".to_string(),
                });
            }
            if winner.is_none_or(|best| {
                let order = compare_sequence_order(row, best);
                if self.first_row {
                    order.is_lt()
                } else {
                    order.is_ge()
                }
            }) {
                winner = Some(row);
            }
        }
        Ok(match winner {
            Some(row) => MergeResult::SourceRow {
                batch_idx: row.batch_idx,
                row_idx: row.row_idx,
            },
            None => MergeResult::Omit,
        })
    }
}
