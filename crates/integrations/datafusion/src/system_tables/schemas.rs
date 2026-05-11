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

//! Mirrors Java [SchemasTable](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/table/system/SchemasTable.java).

use std::any::Any;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray, TimestampMillisecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use paimon::table::Table;
use serde::Serialize;

use crate::error::to_datafusion_error;

pub(super) fn build(table: Table) -> DFResult<Arc<dyn TableProvider>> {
    Ok(Arc::new(SchemasTable { table }))
}

fn schemas_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("schema_id", DataType::Int64, false),
                Field::new("fields", DataType::Utf8, false),
                Field::new("partition_keys", DataType::Utf8, false),
                Field::new("primary_keys", DataType::Utf8, false),
                Field::new("options", DataType::Utf8, false),
                Field::new("comment", DataType::Utf8, true),
                Field::new(
                    "update_time",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
            ]))
        })
        .clone()
}

#[derive(Debug)]
struct SchemasTable {
    table: Table,
}

#[async_trait]
impl TableProvider for SchemasTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        schemas_schema()
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
        let schemas =
            crate::runtime::await_with_runtime(
                async move { table.schema_manager().list_all().await },
            )
            .await
            .map_err(to_datafusion_error)?;

        let n = schemas.len();
        let mut schema_ids: Vec<i64> = Vec::with_capacity(n);
        let mut fields_json: Vec<String> = Vec::with_capacity(n);
        let mut partition_keys_json: Vec<String> = Vec::with_capacity(n);
        let mut primary_keys_json: Vec<String> = Vec::with_capacity(n);
        let mut options_json: Vec<String> = Vec::with_capacity(n);
        let mut comments: Vec<Option<String>> = Vec::with_capacity(n);
        let mut update_times: Vec<i64> = Vec::with_capacity(n);

        for schema in &schemas {
            schema_ids.push(schema.id());
            fields_json.push(to_json(schema.fields())?);
            partition_keys_json.push(to_json(schema.partition_keys())?);
            primary_keys_json.push(to_json(schema.primary_keys())?);
            options_json.push(to_json(schema.options())?);
            comments.push(schema.comment().map(str::to_string));
            update_times.push(schema.time_millis());
        }

        let schema = schemas_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(schema_ids)),
                Arc::new(StringArray::from(fields_json)),
                Arc::new(StringArray::from(partition_keys_json)),
                Arc::new(StringArray::from(primary_keys_json)),
                Arc::new(StringArray::from(options_json)),
                Arc::new(StringArray::from(comments)),
                Arc::new(TimestampMillisecondArray::from(update_times)),
            ],
        )?;

        Ok(MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            schema,
            projection.cloned(),
        )?)
    }
}

fn to_json<T: Serialize + ?Sized>(value: &T) -> DFResult<String> {
    serde_json::to_string(value).map_err(|e| DataFusionError::External(Box::new(e)))
}
