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
use datafusion::datasource::sink::DataSink;
use datafusion::error::Result as DFResult;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::DisplayAs;
use futures::StreamExt;
use paimon::table::Table;

use crate::error::to_datafusion_error;

fn to_paimon_batch(batch: RecordBatch) -> DFResult<RecordBatch> {
    if !batch
        .schema()
        .fields()
        .iter()
        .any(|field| field.data_type() == &ArrowDataType::Utf8View)
    {
        return Ok(batch);
    }

    let fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| {
            if field.data_type() == &ArrowDataType::Utf8View {
                Arc::new(field.as_ref().clone().with_data_type(ArrowDataType::Utf8))
            } else {
                Arc::clone(field)
            }
        })
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        batch.schema().metadata().clone(),
    ));
    let columns = batch
        .columns()
        .iter()
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

/// DataSink that writes RecordBatches to a Paimon table.
///
/// Uses the Paimon write pipeline: `WriteBuilder` → `TableWrite` → `TableCommit`.
/// Internal parallelism is handled by `TableWrite` which routes each
/// (partition, bucket) to its own background tokio task.
#[derive(Debug)]
pub struct PaimonDataSink {
    table: Table,
    schema: ArrowSchemaRef,
    overwrite: bool,
}

impl PaimonDataSink {
    pub fn new(table: Table, schema: ArrowSchemaRef, overwrite: bool) -> Self {
        Self {
            table,
            schema,
            overwrite,
        }
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
        _context: &Arc<TaskContext>,
    ) -> DFResult<u64> {
        let wb = if self.overwrite {
            self.table.new_write_builder().with_overwrite()
        } else {
            self.table.new_write_builder()
        };
        let mut tw = wb.new_write().map_err(to_datafusion_error)?;
        let mut row_count = 0u64;

        while let Some(batch) = data.next().await {
            let batch = to_paimon_batch(batch?)?;
            row_count += batch.num_rows() as u64;
            tw.write_arrow_batch(&batch)
                .await
                .map_err(to_datafusion_error)?;
        }

        let messages = tw.prepare_commit().await.map_err(to_datafusion_error)?;
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
