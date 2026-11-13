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

//! DataSink implementation for writing to Paimon tables via DataFusion.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Schema, SchemaRef as ArrowSchemaRef,
};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::runtime::SpawnedTask;
use datafusion::datasource::sink::DataSink;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::DisplayAs;
use futures::channel::mpsc;
use futures::{SinkExt, StreamExt};
use paimon::spec::{CoreOptions, ROW_ID_FIELD_NAME};
use paimon::table::{CommitMessage, Table, WriteBuilder};

use crate::error::to_datafusion_error;

fn to_paimon_batch(batch: RecordBatch, strip_internal_row_id: bool) -> DFResult<RecordBatch> {
    let input_schema = batch.schema();
    let row_id_index = strip_internal_row_id.then(|| {
        input_schema
            .fields()
            .iter()
            .rposition(|field| field.name() == ROW_ID_FIELD_NAME)
    });
    let row_id_index = row_id_index.flatten();
    if row_id_index.is_none()
        && !input_schema
            .fields()
            .iter()
            .any(|field| field.data_type() == &ArrowDataType::Utf8View)
    {
        return Ok(batch);
    }

    let fields = input_schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(index, _)| Some(*index) != row_id_index)
        .map(|(_, field)| {
            if field.data_type() == &ArrowDataType::Utf8View {
                Arc::new(field.as_ref().clone().with_data_type(ArrowDataType::Utf8))
            } else {
                Arc::clone(field)
            }
        })
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        input_schema.metadata().clone(),
    ));
    let columns = batch
        .columns()
        .iter()
        .enumerate()
        .filter(|(index, _)| Some(*index) != row_id_index)
        .map(|(_, column)| column)
        .zip(schema.fields())
        .map(|(column, field)| {
            if column.data_type() == field.data_type() {
                Ok(Arc::clone(column))
            } else {
                cast(column.as_ref(), field.data_type()).map_err(Into::into)
            }
        })
        .collect::<DFResult<Vec<_>>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));

    RecordBatch::try_new_with_options(schema, columns, &options).map_err(Into::into)
}

/// Table option naming the number of concurrent writers, read from table
/// options as Java's FlinkSinkBuilder does. Only bucket-unaware append-only
/// `INSERT INTO` honors it.
const SINK_PARALLELISM: &str = "sink.parallelism";

/// DataSink that writes RecordBatches to a Paimon table.
///
/// Uses the Paimon write pipeline: `WriteBuilder` → `TableWrite` → `TableCommit`.
/// A `TableWrite` keeps one file writer per (partition, bucket) and writes the
/// groups of a batch in turn; only rolled files are closed in background tasks.
/// Bucket-unaware append-only `INSERT INTO` may therefore spread input batches
/// over `min(sink.parallelism, target_partitions)` `TableWrite`s; their
/// messages form one commit.
#[derive(Debug)]
pub struct PaimonDataSink {
    table: Table,
    schema: ArrowSchemaRef,
    strip_internal_row_id: bool,
    overwrite: bool,
}

impl PaimonDataSink {
    pub fn new(table: Table, schema: ArrowSchemaRef, overwrite: bool) -> Self {
        let strip_internal_row_id =
            CoreOptions::new(table.schema().options()).data_evolution_enabled();
        Self {
            table,
            schema,
            strip_internal_row_id,
            overwrite,
        }
    }

    /// Number of `TableWrite`s for this statement, capped by the session's
    /// `target_partitions`. The option is validated for every table. Any
    /// writer may write any partition only for bucket-unaware append-only
    /// tables, so other tables and overwrites keep the single writer.
    fn writer_count(&self, target_partitions: usize) -> DFResult<usize> {
        let options = self.table.schema().options();
        let parallelism = match options.get(SINK_PARALLELISM) {
            None => return Ok(1),
            // Same accepted forms as Java Integer.parseInt.
            Some(value) => match value.parse::<i32>() {
                Ok(count) if count > 0 => count as usize,
                _ => {
                    return Err(DataFusionError::Configuration(format!(
                        "'{SINK_PARALLELISM}' must be a positive integer, got '{value}'"
                    )))
                }
            },
        };
        let core_options = CoreOptions::new(options);
        if self.overwrite
            || core_options.is_format_table()
            || !self.table.schema().primary_keys().is_empty()
            || core_options.bucket() != -1
        {
            return Ok(1);
        }
        Ok(parallelism.min(target_partitions.max(1)))
    }

    /// Deal input batches round-robin to `writers` spawned `TableWrite`s and
    /// gather their commit messages.
    async fn write_parallel(
        &self,
        wb: &WriteBuilder<'_>,
        writers: usize,
        mut data: SendableRecordBatchStream,
    ) -> DFResult<(u64, Vec<CommitMessage>)> {
        let mut senders = Vec::with_capacity(writers);
        let mut tasks = Vec::with_capacity(writers);
        for _ in 0..writers {
            let mut tw = wb.new_write().map_err(to_datafusion_error)?;
            // One queued batch per writer bounds buffered input.
            let (sender, mut receiver) = mpsc::channel::<RecordBatch>(1);
            senders.push(sender);
            tasks.push(SpawnedTask::spawn(async move {
                while let Some(batch) = receiver.next().await {
                    tw.write_arrow_batch(&batch).await?;
                }
                tw.prepare_commit().await
            }));
        }

        let mut row_count = 0u64;
        let mut next = 0;
        while let Some(batch) = data.next().await {
            let batch = to_paimon_batch(batch?, self.strip_internal_row_id)?;
            row_count += batch.num_rows() as u64;
            if senders[next].send(batch).await.is_err() {
                // The writer stopped on an error, which its join reports below.
                break;
            }
            next = (next + 1) % writers;
        }
        drop(senders);

        let mut messages = Vec::new();
        for task in tasks {
            let result = task
                .join_unwind()
                .await
                .map_err(|error| DataFusionError::ExecutionJoin(Box::new(error)))?;
            messages.extend(result.map_err(to_datafusion_error)?);
        }
        Ok((row_count, messages))
    }
}

impl DisplayAs for PaimonDataSink {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        write!(f, "PaimonDataSink: table={}", self.table.identifier())
    }
}

#[async_trait]
impl DataSink for PaimonDataSink {
    fn schema(&self) -> &ArrowSchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> DFResult<u64> {
        let wb = if self.overwrite {
            self.table.new_write_builder().with_overwrite()
        } else {
            self.table.new_write_builder()
        };
        let resources = crate::memory::writer_resources(context).map_err(to_datafusion_error)?;
        let wb = wb.with_resources(resources);
        let writers = self.writer_count(context.session_config().target_partitions())?;
        let (row_count, messages) = if writers > 1 {
            self.write_parallel(&wb, writers, data).await?
        } else {
            let mut tw = wb.new_write().map_err(to_datafusion_error)?;
            let mut row_count = 0u64;
            while let Some(batch) = data.next().await {
                let batch = to_paimon_batch(batch?, self.strip_internal_row_id)?;
                row_count += batch.num_rows() as u64;
                tw.write_arrow_batch(&batch)
                    .await
                    .map_err(to_datafusion_error)?;
            }
            let messages = tw.prepare_commit().await.map_err(to_datafusion_error)?;
            (row_count, messages)
        };

        let commit = wb.try_new_commit().map_err(to_datafusion_error)?;

        if self.overwrite {
            commit
                .overwrite(messages, None)
                .await
                .map_err(to_datafusion_error)?;
        } else {
            commit.commit(messages).await.map_err(to_datafusion_error)?;
        }

        Ok(row_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, Int64Array, StringViewArray};
    use datafusion::arrow::datatypes::Field;

    #[test]
    fn test_to_paimon_batch_strips_internal_row_id_and_casts_string_views() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("name", ArrowDataType::Utf8View, true),
            Field::new(ROW_ID_FIELD_NAME, ArrowDataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringViewArray::from(vec![Some("a"), None])),
                Arc::new(Int64Array::from(vec![10, 11])),
            ],
        )
        .unwrap();

        let converted = to_paimon_batch(batch, true).unwrap();

        assert_eq!(converted.num_rows(), 2);
        assert_eq!(converted.num_columns(), 2);
        assert_eq!(converted.schema().field(0).name(), "id");
        assert_eq!(converted.schema().field(1).name(), "name");
        assert_eq!(
            converted.schema().field(1).data_type(),
            &ArrowDataType::Utf8
        );
    }
}
