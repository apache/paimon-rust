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

//! Audit execution policy layered over the shared scan mechanics.

use std::sync::Arc;

use datafusion::common::{stats::Precision, Statistics};
use datafusion::config::ConfigOptions;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterPushdownPhase, FilterPushdownPropagation,
};
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use paimon::table::AuditLogRead;

use super::PaimonTableScan;

/// Retains retract rows and keeps logical audit columns out of physical pushdown.
#[derive(Debug, Clone)]
pub(crate) struct PaimonAuditLogScan {
    inner: PaimonTableScan,
}

impl PaimonAuditLogScan {
    pub(crate) fn new(inner: PaimonTableScan) -> Self {
        Self { inner }
    }
}

impl ExecutionPlan for PaimonAuditLogScan {
    fn name(&self) -> &str {
        "PaimonAuditLogScan"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan + 'static>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> DFResult<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        let result = self
            .inner
            .pushdown_filters(child_pushdown_result, |filter| {
                // Audit system-table names are case sensitive. Synthetic columns
                // have no counterpart in the underlying data files.
                collect_columns(filter).iter().all(|column| {
                    self.inner
                        .table()
                        .schema()
                        .fields()
                        .iter()
                        .any(|field| field.name() == column.name())
                })
            })?;
        Ok(FilterPushdownPropagation {
            filters: result.filters,
            updated_node: result
                .updated_node
                .map(|scan| Arc::new(Self::new(scan)) as Arc<dyn ExecutionPlan>),
        })
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        self.inner.execute_with(partition, |read, splits| {
            AuditLogRead::new(read)?.to_arrow(splits)
        })
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DFResult<Arc<Statistics>> {
        let mut statistics = self.inner.partition_statistics(partition)?;
        Arc::make_mut(&mut statistics).num_rows = Precision::Absent;
        Ok(statistics)
    }
}

impl DisplayAs for PaimonAuditLogScan {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        self.inner.fmt_scan(self.name(), f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::{datafusion_arrow_schema, PaimonScanBuilder};
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{lit, BinaryExpr, Column};
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_plan::filter_pushdown::{ChildFilterPushdownResult, PushedDown};
    use paimon::catalog::Identifier;
    use paimon::table::Table;
    use paimon::DataSplitBuilder;

    fn first_row_audit_scan() -> PaimonAuditLogScan {
        let file_io = paimon::io::FileIOBuilder::new("memory").build().unwrap();
        let schema = paimon::spec::Schema::builder()
            .column(
                "id",
                paimon::spec::DataType::Int(paimon::spec::IntType::new()),
            )
            .primary_key(["id"])
            .option("bucket", "1")
            .option("merge-engine", "first-row")
            .build()
            .unwrap();
        let table = Table::new(
            file_io,
            Identifier::new("default", "first_row_audit"),
            "memory:/first-row-audit".to_string(),
            paimon::spec::TableSchema::new(0, &schema),
            None,
        );
        let split = |snapshot| {
            DataSplitBuilder::new()
                .with_snapshot(snapshot)
                .with_partition(paimon::spec::BinaryRow::new(0))
                .with_bucket(0)
                .with_bucket_path("memory:/first-row-audit/bucket-0".to_string())
                .with_total_buckets(1)
                .with_data_files(vec![])
                .build()
                .unwrap()
        };
        let read_fields = paimon::table::AuditLogTable::new(table.clone())
            .fields()
            .unwrap();
        let arrow_schema = datafusion_arrow_schema(&read_fields, true).unwrap();
        let plan = PaimonScanBuilder {
            table: &table,
            schema: &arrow_schema,
            plan: paimon::table::Plan::new(vec![split(1), split(2)]),
            scan_trace: None,
            projection: None,
            pushed_predicate: None,
            limit: None,
            target_partitions: 8,
            filter_exact: false,
            case_sensitive: true,
        }
        .build_scan(read_fields)
        .unwrap();
        PaimonAuditLogScan::new(plan)
    }

    #[test]
    fn test_first_row_audit_distributes_independent_splits() {
        let scan = first_row_audit_scan();

        assert_eq!(scan.inner.planned_partitions().len(), 2);
        assert!(scan
            .inner
            .planned_partitions()
            .iter()
            .all(|splits| splits.len() == 1));
    }

    #[test]
    fn test_audit_policy_survives_filter_pushdown() {
        let scan = first_row_audit_scan();
        let filters: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("id", 1)),
                Operator::Gt,
                lit(1_i32),
            )),
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("rowkind", 0)),
                Operator::Eq,
                lit("-D"),
            )),
        ];
        let result = scan
            .handle_child_pushdown_result(
                FilterPushdownPhase::Post,
                ChildPushdownResult {
                    parent_filters: filters
                        .into_iter()
                        .map(|filter| ChildFilterPushdownResult {
                            filter,
                            child_results: Vec::new(),
                        })
                        .collect(),
                    self_filters: Vec::new(),
                },
                &ConfigOptions::default(),
            )
            .unwrap();

        assert!(matches!(
            result.filters.as_slice(),
            [PushedDown::Yes, PushedDown::No]
        ));
        let updated = result.updated_node.unwrap();
        assert!(updated.downcast_ref::<PaimonAuditLogScan>().is_some());
        assert_eq!(
            updated.partition_statistics(None).unwrap().num_rows,
            Precision::Absent
        );
    }
}
