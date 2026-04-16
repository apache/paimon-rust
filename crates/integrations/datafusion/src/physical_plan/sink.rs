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

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::datasource::sink::DataSink;
use datafusion::error::Result as DFResult;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::DisplayAs;
use futures::StreamExt;
use paimon::table::Table;

use crate::error::to_datafusion_error;

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
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &ArrowSchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> DFResult<u64> {
        let mut wb = self.table.new_write_builder();
        if self.overwrite {
            wb = wb.with_overwrite();
        }
        let mut tw = wb.new_write().map_err(to_datafusion_error)?;
        let mut row_count = 0u64;

        while let Some(batch) = data.next().await {
            let batch = batch?;
            row_count += batch.num_rows() as u64;
            tw.write_arrow_batch(&batch)
                .await
                .map_err(to_datafusion_error)?;
        }

        let messages = tw.prepare_commit().await.map_err(to_datafusion_error)?;
        let commit = wb.new_commit();

        if self.overwrite {
            commit
                .overwrite(messages)
                .await
                .map_err(to_datafusion_error)?;
        } else {
            commit.commit(messages).await.map_err(to_datafusion_error)?;
        }

        Ok(row_count)
    }
}
