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

//! Mirrors Java [PhysicalFilesSizeTable](https://github.com/apache/paimon/blob/release-1.4/paimon-core/src/main/java/org/apache/paimon/table/system).

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use paimon::table::referenced_files::{collect_physical_files_summary, PhysicalFilesSummary};
use paimon::table::Table;

use crate::error::to_datafusion_error;

pub(super) fn build(table: Table) -> DFResult<Arc<dyn TableProvider>> {
    Ok(Arc::new(PhysicalFilesSizeTable { table }))
}

fn output_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("manifest_file_count", DataType::Int64, false),
                Field::new("manifest_file_size", DataType::Int64, false),
                Field::new("data_file_count", DataType::Int64, false),
                Field::new("data_file_size", DataType::Int64, false),
                Field::new("index_file_count", DataType::Int64, false),
                Field::new("index_file_size", DataType::Int64, false),
            ]))
        })
        .clone()
}

#[derive(Debug)]
struct PhysicalFilesSizeTable {
    table: Table,
}

#[async_trait]
impl TableProvider for PhysicalFilesSizeTable {
    fn schema(&self) -> SchemaRef {
        output_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let table = self.table.clone();
        let summary = crate::runtime::await_with_runtime(async move {
            let partition_depth = table.schema().partition_keys().len();
            collect_physical_files_summary(table.file_io(), table.location(), partition_depth).await
        })
        .await
        .map_err(to_datafusion_error)?;

        let batch = summary_to_record_batch(&summary)?;
        let schema = output_schema();

        Ok(MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            schema,
            projection.cloned(),
        )?)
    }
}

fn summary_to_record_batch(s: &PhysicalFilesSummary) -> DFResult<RecordBatch> {
    Ok(RecordBatch::try_new(
        output_schema(),
        vec![
            Arc::new(Int64Array::from(vec![s.manifest_file_count])),
            Arc::new(Int64Array::from(vec![s.manifest_file_size])),
            Arc::new(Int64Array::from(vec![s.data_file_count])),
            Arc::new(Int64Array::from(vec![s.data_file_size])),
            Arc::new(Int64Array::from(vec![s.index_file_count])),
            Arc::new(Int64Array::from(vec![s.index_file_size])),
        ],
    )?)
}
