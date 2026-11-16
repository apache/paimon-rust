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
use futures::future::try_join;
use futures::stream::FuturesUnordered;
use futures::{SinkExt, StreamExt, TryStreamExt};
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
                // Flink's PARALLELISM_DEFAULT: Java FlinkSinkBuilder treats -1
                // like an unset option, so it keeps one writer here as well.
                Ok(-1) => return Ok(1),
                _ => {
                    return Err(DataFusionError::Configuration(format!(
                        "'{SINK_PARALLELISM}' must be a positive integer or -1, got '{value}'"
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
    /// gather their commit messages. The input and every writer are polled
    /// together, so the first failure ends the statement and dropping the
    /// rest cancels the input and the other writers.
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

        let strip_internal_row_id = self.strip_internal_row_id;
        let dispatch = async move {
            let mut row_count = 0u64;
            let mut next = 0;
            while let Some(batch) = data.next().await {
                let batch = to_paimon_batch(batch?, strip_internal_row_id)?;
                row_count += batch.num_rows() as u64;
                if senders[next].send(batch).await.is_err() {
                    // The writer stopped on an error, which its task reports.
                    break;
                }
                next = (next + 1) % writers;
            }
            // Dropping the senders ends the writers' input.
            Ok::<_, DataFusionError>(row_count)
        };
        // FuturesUnordered yields each writer as it finishes, so a failed
        // writer is seen while earlier writers still run.
        let mut finished = tasks
            .into_iter()
            .enumerate()
            .map(|(index, task)| async move {
                let messages = task
                    .join_unwind()
                    .await
                    .map_err(|error| DataFusionError::ExecutionJoin(Box::new(error)))?
                    .map_err(to_datafusion_error)?;
                Ok::<_, DataFusionError>((index, messages))
            })
            .collect::<FuturesUnordered<_>>();
        let joins = async move {
            let mut per_writer = Vec::with_capacity(writers);
            while let Some(done) = finished.try_next().await? {
                per_writer.push(done);
            }
            // Keep the messages in writer order.
            per_writer.sort_by_key(|(index, _)| *index);
            Ok(per_writer.into_iter().flat_map(|(_, messages)| messages))
        };
        let (row_count, messages) = try_join(dispatch, joins).await?;
        Ok((row_count, messages.collect()))
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_parallel_writer_failure_ends_insert_while_input_pending() {
        use std::time::Duration;

        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use datafusion::prelude::SessionConfig;
        use paimon::catalog::Identifier;
        use paimon::io::FileIOBuilder;
        use paimon::spec::{DataType, IntType, Schema as PaimonSchema, TableSchema};

        // When a writer fails while the input waits for its next batch, the
        // INSERT must return the writer's error instead of waiting for input
        // that may never come.
        let dir = tempfile::tempdir().unwrap();
        // A regular file where the bucket directory belongs: opening the
        // first data file fails with a real filesystem error.
        std::fs::write(dir.path().join("bucket-0"), b"").unwrap();
        let schema = PaimonSchema::builder()
            .column("id", DataType::Int(IntType::new()))
            .option(SINK_PARALLELISM, "2")
            .build()
            .unwrap();
        let table = Table::new(
            FileIOBuilder::new("file").build().unwrap(),
            Identifier::new("default", "t"),
            format!("file:{}", dir.path().display()),
            TableSchema::new(0, &schema),
            None,
        );
        let arrow_schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int32,
            true,
        )]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap();
        // One batch for the first writer, then the input stays pending like a
        // slow source.
        let input = futures::stream::iter([Ok(batch)]).chain(futures::stream::pending());
        let data = Box::pin(RecordBatchStreamAdapter::new(arrow_schema.clone(), input));
        let context = Arc::new(
            TaskContext::default()
                .with_session_config(SessionConfig::new().with_target_partitions(2)),
        );
        let sink = PaimonDataSink::new(table, arrow_schema, false);

        let result =
            tokio::time::timeout(Duration::from_secs(10), sink.write_all(data, &context)).await;

        let error = result
            .expect("INSERT still waits for input after a writer failed")
            .unwrap_err();
        assert!(error.to_string().contains("bucket-0"), "{error}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_later_writer_failure_ends_insert_while_earlier_writer_waits() {
        use std::time::Duration;

        use datafusion::arrow::array::StringArray;
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use datafusion::prelude::SessionConfig;
        use paimon::catalog::Identifier;
        use paimon::io::FileIOBuilder;
        use paimon::spec::{DataType, IntType, Schema as PaimonSchema, TableSchema, VarCharType};

        // The second writer fails while the first writer is still alive and
        // waiting for more input. The INSERT must return the second writer's
        // error; waiting on the writers in creation order would block on the
        // first writer and never see it.
        let dir = tempfile::tempdir().unwrap();
        // A regular file where the bucket directory of partition p=bad
        // belongs: only writes to that partition fail, with a real filesystem
        // error.
        std::fs::create_dir_all(dir.path().join("p=bad")).unwrap();
        std::fs::write(dir.path().join("p=bad").join("bucket-0"), b"").unwrap();
        let schema = PaimonSchema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("p", DataType::VarChar(VarCharType::string_type()))
            .partition_keys(["p"])
            .option(SINK_PARALLELISM, "2")
            .build()
            .unwrap();
        let table = Table::new(
            FileIOBuilder::new("file").build().unwrap(),
            Identifier::new("default", "t"),
            format!("file:{}", dir.path().display()),
            TableSchema::new(0, &schema),
            None,
        );
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, true),
            Field::new("p", ArrowDataType::Utf8, true),
        ]));
        let batch = |id: i32, p: &str| {
            RecordBatch::try_new(
                arrow_schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![id])),
                    Arc::new(StringArray::from(vec![p])),
                ],
            )
            .unwrap()
        };
        // Batches are dealt round-robin: the p=ok batch goes to the first
        // writer, which buffers it and waits; the p=bad batch goes to the
        // second writer, which fails. Then the input stays pending like a
        // slow source.
        let input = futures::stream::iter([Ok(batch(1, "ok")), Ok(batch(2, "bad"))])
            .chain(futures::stream::pending());
        let data = Box::pin(RecordBatchStreamAdapter::new(arrow_schema.clone(), input));
        let context = Arc::new(
            TaskContext::default()
                .with_session_config(SessionConfig::new().with_target_partitions(2)),
        );
        let sink = PaimonDataSink::new(table, arrow_schema, false);

        let result =
            tokio::time::timeout(Duration::from_secs(10), sink.write_all(data, &context)).await;

        let error = result
            .expect("INSERT still waits after the second writer failed")
            .unwrap_err();
        // The error comes from opening a data file under p=bad/bucket-0, so
        // it names the failed partition.
        assert!(error.to_string().contains("p=bad"), "{error}");
    }
}
