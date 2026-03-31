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

//! Paimon table provider for DataFusion (read-only).

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef as ArrowSchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use paimon::table::Table;

use crate::error::to_datafusion_error;
use crate::physical_plan::PaimonTableScan;
use crate::schema::paimon_schema_to_arrow;

/// Read-only table provider for a Paimon table.
///
/// Supports full table scan and column projection. Predicate pushdown and writes
/// are not yet supported.
#[derive(Debug, Clone)]
pub struct PaimonTableProvider {
    table: Table,
    schema: ArrowSchemaRef,
}

impl PaimonTableProvider {
    /// Create a table provider from a Paimon table.
    ///
    /// Loads the table schema and converts it to Arrow for DataFusion.
    pub fn try_new(table: Table) -> DFResult<Self> {
        let fields = table.schema().fields();
        let schema = paimon_schema_to_arrow(fields)?;
        Ok(Self { table, schema })
    }

    pub fn table(&self) -> &Table {
        &self.table
    }
}

/// Distribute `items` into `num_buckets` groups using round-robin assignment.
fn bucket_round_robin<T>(items: Vec<T>, num_buckets: usize) -> Vec<Vec<T>> {
    let mut buckets: Vec<Vec<T>> = (0..num_buckets).map(|_| Vec::new()).collect();
    for (i, item) in items.into_iter().enumerate() {
        buckets[i % num_buckets].push(item);
    }
    buckets
}

#[async_trait]
impl TableProvider for PaimonTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Convert projection indices to column names and compute projected schema
        let (projected_schema, projected_columns) = if let Some(indices) = projection {
            let fields: Vec<Field> = indices
                .iter()
                .map(|&i| self.schema.field(i).clone())
                .collect();
            let column_names: Vec<String> = fields.iter().map(|f| f.name().clone()).collect();
            (Arc::new(Schema::new(fields)), Some(column_names))
        } else {
            (self.schema.clone(), None)
        };

        // Plan splits eagerly so we know partition count upfront.
        let read_builder = self.table.new_read_builder();
        let scan = read_builder.new_scan();
        let plan = scan.plan().await.map_err(to_datafusion_error)?;

        // Distribute splits across DataFusion partitions, capped by the
        // session's target_partitions to avoid over-sharding with many small splits.
        // Each partition's splits are wrapped in Arc to avoid deep-cloning in execute().
        let splits = plan.splits().to_vec();
        let planned_partitions: Vec<Arc<[_]>> = if splits.is_empty() {
            // Empty plans get a single empty partition to avoid 0-partition edge cases.
            vec![Arc::from(Vec::new())]
        } else {
            let target = state.config_options().execution.target_partitions;
            let num_partitions = splits.len().min(target.max(1));
            bucket_round_robin(splits, num_partitions)
                .into_iter()
                .map(Arc::from)
                .collect()
        };

        Ok(Arc::new(PaimonTableScan::new(
            projected_schema,
            self.table.clone(),
            projected_columns,
            planned_partitions,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bucket_round_robin_distributes_evenly() {
        let result = bucket_round_robin(vec![0, 1, 2, 3, 4], 3);
        assert_eq!(result, vec![vec![0, 3], vec![1, 4], vec![2]]);
    }

    #[test]
    fn test_bucket_round_robin_fewer_items_than_buckets() {
        let result = bucket_round_robin(vec![10, 20], 2);
        assert_eq!(result, vec![vec![10], vec![20]]);
    }

    #[test]
    fn test_bucket_round_robin_single_bucket() {
        let result = bucket_round_robin(vec![1, 2, 3], 1);
        assert_eq!(result, vec![vec![1, 2, 3]]);
    }
}
