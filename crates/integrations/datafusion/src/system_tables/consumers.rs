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

//! Mirrors Java [ConsumersTable](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/table/system/ConsumersTable.java).

use std::collections::HashSet;
use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::ScalarValue;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use paimon::table::Table;

use crate::error::to_datafusion_error;

pub(super) fn build(table: Table) -> DFResult<Arc<dyn TableProvider>> {
    Ok(Arc::new(ConsumersTable { table }))
}

fn consumers_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("consumer_id", DataType::Utf8, false),
                Field::new("next_snapshot_id", DataType::Int64, false),
            ]))
        })
        .clone()
}

#[derive(Debug)]
struct ConsumersTable {
    table: Table,
}

#[async_trait]
impl TableProvider for ConsumersTable {
    fn schema(&self) -> SchemaRef {
        consumers_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let manager = self.table.consumer_manager();
        let requested_ids = requested_consumer_ids(filters);
        let consumers = crate::runtime::await_with_runtime(async move {
            match requested_ids {
                Some(ids) => manager.list_by_ids(&ids).await,
                None => manager.list_all().await,
            }
        })
        .await
        .map_err(to_datafusion_error)?;

        let schema = consumers_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from_iter_values(
                    consumers.iter().map(|(id, _)| id.as_str()),
                )),
                Arc::new(Int64Array::from_iter_values(
                    consumers.iter().map(|(_, next_snapshot)| *next_snapshot),
                )),
            ],
        )?;

        Ok(MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            schema,
            projection.cloned(),
        )?)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if consumer_ids_from_filter(filter).is_some() {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
}

fn requested_consumer_ids(filters: &[Expr]) -> Option<Vec<String>> {
    let ids = filters
        .iter()
        .filter_map(consumer_ids_from_filter)
        .reduce(|mut left, right| {
            left.retain(|id| right.contains(id));
            left
        })?;
    let mut ids = ids.into_iter().collect::<Vec<_>>();
    ids.sort_unstable();
    Some(ids)
}

fn consumer_ids_from_filter(filter: &Expr) -> Option<HashSet<String>> {
    match filter {
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            consumer_id_literal(binary.left.as_ref(), binary.right.as_ref())
                .or_else(|| consumer_id_literal(binary.right.as_ref(), binary.left.as_ref()))
                .map(|id| HashSet::from([id]))
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            match (
                consumer_ids_from_filter(binary.left.as_ref()),
                consumer_ids_from_filter(binary.right.as_ref()),
            ) {
                (Some(mut left), Some(right)) => {
                    left.retain(|id| right.contains(id));
                    Some(left)
                }
                (Some(ids), None) | (None, Some(ids)) => Some(ids),
                (None, None) => None,
            }
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Or => {
            let mut left = consumer_ids_from_filter(binary.left.as_ref())?;
            left.extend(consumer_ids_from_filter(binary.right.as_ref())?);
            Some(left)
        }
        Expr::InList(in_list)
            if !in_list.negated && is_consumer_id_column(in_list.expr.as_ref()) =>
        {
            in_list.list.iter().map(string_literal).collect()
        }
        _ => None,
    }
}

fn consumer_id_literal(column: &Expr, literal: &Expr) -> Option<String> {
    is_consumer_id_column(column)
        .then_some(literal)
        .and_then(string_literal)
}

fn is_consumer_id_column(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(column) if column.name == "consumer_id")
}

fn string_literal(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(
            ScalarValue::Utf8(Some(value))
            | ScalarValue::LargeUtf8(Some(value))
            | ScalarValue::Utf8View(Some(value)),
            _,
        ) => Some(value.clone()),
        _ => None,
    }
}
