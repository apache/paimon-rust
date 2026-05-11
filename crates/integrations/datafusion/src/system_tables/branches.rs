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

//! Mirrors Java [BranchesTable](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/table/system/BranchesTable.java).

use std::any::Any;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use datafusion::arrow::array::{RecordBatch, StringArray, TimestampMillisecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use paimon::table::{BranchManager, Table};

use crate::error::to_datafusion_error;

pub(super) fn build(table: Table) -> DFResult<Arc<dyn TableProvider>> {
    Ok(Arc::new(BranchesTable { table }))
}

fn branches_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("branch_name", DataType::Utf8, false),
                Field::new(
                    "create_time",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
            ]))
        })
        .clone()
}

#[derive(Debug)]
struct BranchesTable {
    table: Table,
}

#[async_trait]
impl TableProvider for BranchesTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        branches_schema()
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
        let (names, create_times) =
            crate::runtime::await_with_runtime(async move { collect_branches(&table).await })
                .await
                .map_err(to_datafusion_error)?;

        let schema = branches_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(names)),
                Arc::new(TimestampMillisecondArray::from(create_times)),
            ],
        )?;

        Ok(MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            schema,
            projection.cloned(),
        )?)
    }
}

async fn collect_branches(table: &Table) -> paimon::Result<(Vec<String>, Vec<i64>)> {
    let file_io = table.file_io();
    let bm = BranchManager::new(file_io.clone(), table.location().to_string());
    let names = bm.list_all().await?;
    let mut create_times = Vec::with_capacity(names.len());
    for name in &names {
        let status = file_io.get_status(&bm.branch_path(name)).await?;
        create_times.push(
            status
                .last_modified
                .map(|dt| dt.timestamp_millis())
                .unwrap_or(0),
        );
    }
    Ok((names, create_times))
}
