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

//! Current-state audit reads, including winning retract rows.

use super::{
    audit_sequence_number_enabled, rowkind_array_from_column, PaimonTableRead, TableRead,
    TableReadKind, MAX_MERGE_INPUT_STREAMS,
};
use crate::arrow::build_target_arrow_schema;
use crate::spec::{
    DataField, DataType, MergeEngine, TinyIntType, ROW_KIND_FIELD_ID, SEQUENCE_NUMBER_FIELD_ID,
    VALUE_KIND_FIELD_ID, VALUE_KIND_FIELD_NAME,
};
use crate::table::data_file_reader::DataFileReader;
use crate::table::kv_file_reader::{KeyValueFileReader, KeyValueReadConfig};
use crate::table::table_read::configured_mosaic_prefetch;
use crate::table::ArrowRecordBatchStream;
use crate::DataSplit;
use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions, StringArray};
use futures::{stream, StreamExt};
use std::sync::Arc;

/// Reads current-state audit rows using the supplied read's exact projection,
/// predicates and Parquet budget. Include `rowkind` in the projection to expose it.
#[derive(Debug, Clone)]
pub struct AuditLogRead<'a> {
    read: PaimonTableRead<'a>,
}

impl<'a> AuditLogRead<'a> {
    pub fn new(read: TableRead<'a>) -> crate::Result<Self> {
        read.ensure_query_auth_allowed()?;
        match read.0 {
            TableReadKind::Paimon(read) => Ok(Self { read }),
            TableReadKind::Format(_) => Err(crate::Error::Unsupported {
                message: "Format tables do not support audit log batch read".to_string(),
            }),
        }
    }

    /// Reads splits planned by an audit scan, retaining winning retract rows.
    pub fn to_arrow(&self, data_splits: &[DataSplit]) -> crate::Result<ArrowRecordBatchStream> {
        let output_read_type = self.read.read_type.clone();
        if output_read_type
            .iter()
            .any(|field| field.id() == SEQUENCE_NUMBER_FIELD_ID)
            && !audit_sequence_number_enabled(self.read.table)
        {
            return Err(crate::Error::DataInvalid {
                message: "Audit read requested _SEQUENCE_NUMBER but table-read.sequence-number.enabled is false".to_string(),
                source: None,
            });
        }
        let audit_schema = build_target_arrow_schema(&output_read_type)?;
        let has_primary_keys = !self.read.table.schema().primary_keys().is_empty();
        let mut read_type: Vec<_> = output_read_type
            .iter()
            .filter(|field| field.id() != ROW_KIND_FIELD_ID)
            .cloned()
            .collect();
        if has_primary_keys
            && output_read_type
                .iter()
                .any(|field| field.id() == ROW_KIND_FIELD_ID)
        {
            read_type.push(DataField::new(
                VALUE_KIND_FIELD_ID,
                VALUE_KIND_FIELD_NAME.to_string(),
                DataType::TinyInt(TinyIntType::new()),
            ));
        }

        let physical_stream = if has_primary_keys {
            let core_options = self.read.table.schema().core_options();
            let merge_engine = core_options.merge_engine()?;
            let (raw_splits, merge_splits): (Vec<_>, Vec<_>) = data_splits
                .iter()
                .cloned()
                .partition(|split| audit_raw_convertible(split, merge_engine));
            let parquet_read_budget = self.read.parquet_read_budget()?;
            let raw_stream = DataFileReader::new(
                self.read.table.file_io.clone(),
                self.read.table.schema_manager().clone(),
                self.read.table.schema().id(),
                self.read.table.schema.fields().to_vec(),
                read_type.clone(),
                self.read.data_predicates.clone(),
            )
            .with_file_index_read_enabled(core_options.file_index_read_enabled())
            .with_batch_size(Some(core_options.read_batch_size()?))
            .with_parquet_read_budget(Some(Arc::clone(&parquet_read_budget)))
            .with_table_options(self.read.table.schema().options().clone())
            .read(&raw_splits)?;
            let merge_stream = KeyValueFileReader::new(
                self.read.table.file_io.clone(),
                KeyValueReadConfig {
                    table_name: self.read.table.identifier().full_name(),
                    table_options: self.read.table.schema().options().clone(),
                    schema_manager: self.read.table.schema_manager().clone(),
                    table_schema_id: self.read.table.schema().id(),
                    table_fields: self.read.table.schema.fields().to_vec(),
                    read_type,
                    predicates: self.read.data_predicates.clone(),
                    primary_keys: self.read.table.schema.trimmed_primary_keys(),
                    table_primary_keys: self.read.table.schema.primary_keys().to_vec(),
                    merge_engine,
                    sequence_fields: core_options
                        .sequence_fields()
                        .iter()
                        .map(|field| field.to_string())
                        .collect(),
                    read_batch_size: core_options.read_batch_size()?,
                    merge_splits: false,
                    max_merge_input_streams: Some(MAX_MERGE_INPUT_STREAMS),
                    parquet_read_budget: Some(parquet_read_budget),
                    mosaic_prefetch: configured_mosaic_prefetch(self.read.table)?,
                },
            )
            .read_with_merge_function(
                &merge_splits,
                crate::table::audit_log_table::merge::new_merge_function,
            )?;
            Box::pin(stream::select_all([raw_stream, merge_stream]))
        } else {
            let mut read = self.read.clone();
            read.read_type = read_type;
            read.to_arrow(data_splits)?
        };

        Ok(Box::pin(async_stream::try_stream! {
            futures::pin_mut!(physical_stream);
            let mut projection = None;
            while let Some(batch) = physical_stream.next().await {
                let batch = batch?;
                if projection.is_none() {
                    projection = Some(output_read_type.iter().map(|field| {
                        let name = if field.id() == ROW_KIND_FIELD_ID {
                            if !has_primary_keys {
                                return Ok(None);
                            }
                            VALUE_KIND_FIELD_NAME
                        } else {
                            field.name()
                        };
                        batch.schema().index_of(name).map(Some).map_err(|error| crate::Error::DataInvalid {
                            message: format!("Audit read missing column '{name}': {error}"),
                            source: None,
                        })
                    }).collect::<crate::Result<Vec<_>>>()?);
                }
                let columns = output_read_type.iter().zip(projection.as_ref().unwrap())
                    .map(|(field, index)| {
                        let column: ArrayRef = match index {
                            None => Arc::new(StringArray::from(vec!["+I"; batch.num_rows()])),
                            Some(index) if field.id() == ROW_KIND_FIELD_ID =>
                                Arc::new(rowkind_array_from_column(batch.column(*index).as_ref())?),
                            Some(index) => batch.column(*index).clone(),
                        };
                        Ok(column)
                    }).collect::<crate::Result<Vec<_>>>()?;
                let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
                yield RecordBatch::try_new_with_options(audit_schema.clone(), columns, &options)
                    .map_err(|error| crate::Error::UnexpectedError {
                        message: format!("Failed to build audit log batch: {error}"),
                        source: Some(Box::new(error)),
                    })?;
            }
        }))
    }
}

// Legacy unknown delete counts and first-row level-0 files stay on the merge path.
fn audit_raw_convertible(split: &DataSplit, merge_engine: MergeEngine) -> bool {
    split.raw_convertible()
        && split.data_files().iter().all(|file| {
            file.delete_row_count == Some(0)
                && (merge_engine != MergeEngine::FirstRow || file.level != 0)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::table_read::tests::{file, split};

    #[test]
    fn test_audit_split_routing() {
        let raw = split(vec![file("a", 5, Some(0))], true);
        let merge = split(vec![file("a", 5, Some(0))], false);
        let legacy = split(vec![file("a", 5, None)], true);
        assert!(audit_raw_convertible(&raw, MergeEngine::Deduplicate));
        assert!(audit_raw_convertible(&raw, MergeEngine::FirstRow));
        assert!(!audit_raw_convertible(&merge, MergeEngine::Deduplicate));
        assert!(!audit_raw_convertible(&legacy, MergeEngine::Deduplicate));
        let level_zero = split(vec![file("a", 0, Some(0))], true);
        assert!(audit_raw_convertible(&level_zero, MergeEngine::Deduplicate));
        assert!(!audit_raw_convertible(&level_zero, MergeEngine::FirstRow));
    }
}
