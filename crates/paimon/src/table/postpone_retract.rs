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

//! Validate the first retract using the unwrapped merge function, as Java's
//! PostponeBucketWriter does. Pending rows themselves are never merged.

use super::postpone_file_writer::PostponeWriteConfig;
use super::sort_merge::{
    AggregateMergeFunction, BufferedBatch, FirstRowMergeFunction, MergeFunction, MergeRow,
    PartialUpdateMergeFunction,
};
use crate::spec::{
    AggregationConfig, CoreOptions, MergeEngine, PartialUpdateConfig, RowKind,
    VALUE_KIND_FIELD_NAME,
};
use crate::Result;
use arrow_array::{Array, Int8Array, RecordBatch};

enum RetractMerge {
    Deduplicate,
    FirstRow(FirstRowMergeFunction),
    PartialUpdate(PartialUpdateMergeFunction),
    Aggregate(AggregateMergeFunction),
}

pub(super) struct PostponeRetractValidator {
    merge: Box<RetractMerge>,
    validated: bool,
    value_indices: Vec<usize>,
}

impl PostponeRetractValidator {
    pub(super) fn new(config: &PostponeWriteConfig) -> Result<Self> {
        let options = CoreOptions::new(&config.table_options);
        let merge = match options.merge_engine()? {
            MergeEngine::Deduplicate => RetractMerge::Deduplicate,
            MergeEngine::FirstRow => RetractMerge::FirstRow(FirstRowMergeFunction {
                ignore_delete: options.ignore_delete(),
            }),
            MergeEngine::PartialUpdate => {
                PartialUpdateConfig::new(&config.table_options)
                    .validate_write_mode(true, &config.table_name)?;
                RetractMerge::PartialUpdate(PartialUpdateMergeFunction::new_with_schema(
                    &config.table_options,
                    &config.table_name,
                    &config.value_fields,
                    &config.value_fields,
                    &config.primary_keys,
                )?)
            }
            MergeEngine::Aggregation => {
                AggregationConfig::new(&config.table_options)
                    .validate_runtime_mode(true, &config.table_name)?;
                let sequences = options
                    .sequence_fields()
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>();
                RetractMerge::Aggregate(AggregateMergeFunction::new(
                    &config.table_options,
                    &config.table_name,
                    &config.value_fields,
                    &config.primary_keys,
                    &sequences,
                )?)
            }
        };
        Ok(Self {
            merge: Box::new(merge),
            validated: false,
            value_indices: (0..config.value_fields.len()).collect(),
        })
    }

    pub(super) fn validate(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.validated {
            return Ok(());
        }
        let Some(kinds) = batch.column_by_name(VALUE_KIND_FIELD_NAME) else {
            return Ok(());
        };
        let kinds = kinds
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("validated row kinds");
        for index in 0..batch.num_rows() {
            if kinds.is_null(index) {
                continue;
            }
            let kind = RowKind::from_value(kinds.value(index))?;
            if !kind.is_retract() {
                continue;
            }
            let row = MergeRow {
                batch_idx: 0,
                row_idx: index,
                sequence_number: 0,
                value_kind: kind.to_value(),
                user_sequence: None,
            };
            let values =
                batch
                    .project(&self.value_indices)
                    .map_err(|error| crate::Error::DataInvalid {
                        message: format!("Cannot project postpone values: {error}"),
                        source: Some(Box::new(error)),
                    })?;
            let schema = values.schema();
            let buffer = [BufferedBatch::Source(values)];
            match self.merge.as_ref() {
                RetractMerge::Deduplicate => {}
                RetractMerge::FirstRow(merge) => {
                    merge.merge(&[row], &buffer, &self.value_indices, &schema)?;
                }
                RetractMerge::PartialUpdate(merge) => {
                    merge.merge_unreduced(&[row], &buffer, &self.value_indices, &schema)?;
                }
                RetractMerge::Aggregate(merge) => {
                    merge.merge_ordered(&[&row], &buffer, &self.value_indices, &schema)?;
                }
            }
            self.validated = true;
            break;
        }
        Ok(())
    }
}
