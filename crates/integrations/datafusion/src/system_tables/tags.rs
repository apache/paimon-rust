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

//! Mirrors Java [TagsTable](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/table/system/TagsTable.java).

use std::any::Any;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use datafusion::arrow::array::{
    new_null_array, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use paimon::table::{Table, TagManager};

use crate::error::to_datafusion_error;

pub(super) fn build(table: Table) -> DFResult<Arc<dyn TableProvider>> {
    Ok(Arc::new(TagsTable { table }))
}

fn tags_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("tag_name", DataType::Utf8, false),
                Field::new("snapshot_id", DataType::Int64, false),
                Field::new("schema_id", DataType::Int64, false),
                Field::new(
                    "commit_time",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("record_count", DataType::Int64, true),
                Field::new(
                    "create_time",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ),
                Field::new("time_retained", DataType::Utf8, true),
            ]))
        })
        .clone()
}

#[derive(Debug)]
struct TagsTable {
    table: Table,
}

#[async_trait]
impl TableProvider for TagsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        tags_schema()
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
        let tm = TagManager::new(
            self.table.file_io().clone(),
            self.table.location().to_string(),
        );
        let tags = crate::runtime::await_with_runtime(async move { tm.list_all().await })
            .await
            .map_err(to_datafusion_error)?;

        let n = tags.len();
        let mut tag_names: Vec<String> = Vec::with_capacity(n);
        let mut snapshot_ids = Vec::with_capacity(n);
        let mut schema_ids = Vec::with_capacity(n);
        let mut commit_times = Vec::with_capacity(n);
        let mut record_counts: Vec<Option<i64>> = Vec::with_capacity(n);

        for (name, snap) in tags {
            tag_names.push(name);
            snapshot_ids.push(snap.id());
            schema_ids.push(snap.schema_id());
            commit_times.push(snap.time_millis() as i64);
            record_counts.push(snap.total_record_count());
        }

        let schema = tags_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(tag_names)),
                Arc::new(Int64Array::from(snapshot_ids)),
                Arc::new(Int64Array::from(schema_ids)),
                Arc::new(TimestampMillisecondArray::from(commit_times)),
                Arc::new(Int64Array::from(record_counts)),
                new_null_array(&DataType::Timestamp(TimeUnit::Millisecond, None), n),
                new_null_array(&DataType::Utf8, n),
            ],
        )?;

        Ok(MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            schema,
            projection.cloned(),
        )?)
    }
}
