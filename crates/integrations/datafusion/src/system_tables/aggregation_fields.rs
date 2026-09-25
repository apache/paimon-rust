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

//! Mirrors Java [AggregationFieldsTable](https://github.com/apache/paimon/blob/release-1.4/paimon-core/src/main/java/org/apache/paimon/table/system/AggregationFieldsTable.java).

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use datafusion::arrow::array::{RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use paimon::table::Table;

pub(super) fn build(table: Table) -> DFResult<Arc<dyn TableProvider>> {
    Ok(Arc::new(AggregationFieldsTable { table }))
}

fn aggregation_fields_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("field_name", ArrowDataType::Utf8, false),
                Field::new("field_type", ArrowDataType::Utf8, false),
                Field::new("function", ArrowDataType::Utf8, true),
                Field::new("function_options", ArrowDataType::Utf8, true),
                Field::new("comment", ArrowDataType::Utf8, true),
            ]))
        })
        .clone()
}

#[derive(Debug)]
pub(super) struct AggregationFieldsTable {
    table: Table,
}

#[async_trait]
impl TableProvider for AggregationFieldsTable {
    fn schema(&self) -> SchemaRef {
        aggregation_fields_schema()
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
        let table_schema = self.table.schema();
        let options = table_schema.options();

        let n = table_schema.fields().len();
        let mut field_names = Vec::with_capacity(n);
        let mut field_types = Vec::with_capacity(n);
        let mut functions = Vec::with_capacity(n);
        let mut function_options = Vec::with_capacity(n);
        let mut comments: Vec<Option<String>> = Vec::with_capacity(n);
        for field in table_schema.fields() {
            field_names.push(field.name().to_string());
            field_types.push(field.data_type().to_string());
            let (keys, values) = field_scoped_options(options, field.name());
            functions.push(format!("[{}]", values.join(", ")));
            function_options.push(format!("[{}]", keys.join(", ")));
            comments.push(field.description().map(str::to_string));
        }

        let schema = aggregation_fields_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(field_names)),
                Arc::new(StringArray::from(field_types)),
                Arc::new(StringArray::from(functions)),
                Arc::new(StringArray::from(function_options)),
                Arc::new(StringArray::from(comments)),
            ],
        )?;

        Ok(MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            schema,
            projection.cloned(),
        )?)
    }
}

/// Keys and values of the options scoped to `fields.<field_name>.*`, mirroring
/// Java `AggregationFieldsTable.extractFieldMultimap`. Java renders each as a
/// collection `toString` (`[a, b]`); we sort by key so the output is stable
/// (Java iterates its options map in unspecified order — single-option fields,
/// the common case, render identically either way).
fn field_scoped_options(
    options: &HashMap<String, String>,
    field_name: &str,
) -> (Vec<String>, Vec<String>) {
    let mut pairs: Vec<(&String, &String)> = options
        .iter()
        .filter(|(key, _)| {
            let parts: Vec<&str> = key.split('.').collect();
            parts.len() > 2 && parts[0] == "fields" && parts[1] == field_name
        })
        .collect();
    pairs.sort_by(|a, b| a.0.cmp(b.0));
    let keys = pairs.iter().map(|(k, _)| (*k).clone()).collect();
    let values = pairs.iter().map(|(_, v)| (*v).clone()).collect();
    (keys, values)
}
