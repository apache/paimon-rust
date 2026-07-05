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

//! Mirrors Java [ReferencedFilesSizeTable](https://github.com/apache/paimon/blob/release-1.4/paimon-core/src/main/java/org/apache/paimon/table/system).

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use paimon::table::referenced_files::{collect_referenced_files_summary, ReferencedFilesSummary};
use paimon::table::Table;

use crate::error::to_datafusion_error;

pub(super) fn build(table: Table) -> DFResult<Arc<dyn TableProvider>> {
    Ok(Arc::new(ReferencedFilesSizeTable { table }))
}

fn output_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("source", DataType::Utf8, false),
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
struct ReferencedFilesSizeTable {
    table: Table,
}

#[async_trait]
impl TableProvider for ReferencedFilesSizeTable {
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
        let summaries = crate::runtime::await_with_runtime(async move {
            let schema = table.schema();
            let partition_keys = schema.partition_keys();
            let partition_fields = schema.partition_fields();
            collect_referenced_files_summary(
                table.file_io(),
                table.location(),
                partition_keys,
                &partition_fields,
            )
            .await
        })
        .await
        .map_err(to_datafusion_error)?;

        let batch = summaries_to_record_batch(&summaries)?;
        let schema = output_schema();

        Ok(MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            schema,
            projection.cloned(),
        )?)
    }
}

fn summaries_to_record_batch(summaries: &[ReferencedFilesSummary]) -> DFResult<RecordBatch> {
    let n = summaries.len();
    let mut sources = Vec::with_capacity(n);
    let mut mf_counts = Vec::with_capacity(n);
    let mut mf_sizes = Vec::with_capacity(n);
    let mut df_counts = Vec::with_capacity(n);
    let mut df_sizes = Vec::with_capacity(n);
    let mut if_counts = Vec::with_capacity(n);
    let mut if_sizes = Vec::with_capacity(n);

    for s in summaries {
        sources.push(s.source.as_str());
        mf_counts.push(s.manifest_file_count);
        mf_sizes.push(s.manifest_file_size);
        df_counts.push(s.data_file_count);
        df_sizes.push(s.data_file_size);
        if_counts.push(s.index_file_count);
        if_sizes.push(s.index_file_size);
    }

    Ok(RecordBatch::try_new(
        output_schema(),
        vec![
            Arc::new(StringArray::from(sources)),
            Arc::new(Int64Array::from(mf_counts)),
            Arc::new(Int64Array::from(mf_sizes)),
            Arc::new(Int64Array::from(df_counts)),
            Arc::new(Int64Array::from(df_sizes)),
            Arc::new(Int64Array::from(if_counts)),
            Arc::new(Int64Array::from(if_sizes)),
        ],
    )?)
}
