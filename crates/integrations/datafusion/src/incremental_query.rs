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

//! The `paimon_incremental_query` table function reads a snapshot-id range
//! `(start_exclusive, end_inclusive]` as a batch. Snapshot planning stays in
//! paimon core; this only parses arguments and streams the core plan's output.
//!
//! ```sql
//! SELECT * FROM paimon_incremental_query('db.t', 0, 5);
//! SELECT * FROM paimon_incremental_query('db.t', 0, 5, 'delta');
//! SELECT * FROM paimon_incremental_query('db.t$audit_log', 0, 5, 'changelog');
//! ```
//!
//! A `$audit_log` table-name suffix prepends the `rowkind` audit column (and the
//! optional `_SEQUENCE_NUMBER`), mirroring the core audit-log read.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableFunctionImpl};
use datafusion::common::stats::Precision;
use datafusion::common::{internal_err, project_schema, Statistics};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::prelude::SessionContext;
use futures::{stream, TryStreamExt};
use paimon::catalog::Catalog;
use paimon::table::{AuditLogTable, IncrementalScanMode, Table};

use crate::error::to_datafusion_error;
use crate::runtime::{await_with_runtime, block_on_with_runtime};
use crate::table::datafusion_arrow_schema;
use crate::table_function_args::{
    extract_int_literal, extract_string_literal, parse_table_identifier,
};
use crate::table_loader::load_data_table_for_read;

const FUNCTION_NAME: &str = "paimon_incremental_query";
const AUDIT_LOG_SUFFIX: &str = "$audit_log";

/// Registers `paimon_incremental_query` against `catalog`.
pub(crate) fn register_incremental_query(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    default_database: &str,
) {
    ctx.register_udtf(
        FUNCTION_NAME,
        Arc::new(IncrementalQueryFunction {
            catalog,
            default_database: default_database.to_string(),
        }),
    );
}

pub(crate) struct IncrementalQueryFunction {
    catalog: Arc<dyn Catalog>,
    default_database: String,
}

impl fmt::Debug for IncrementalQueryFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IncrementalQueryFunction")
            .field("default_database", &self.default_database)
            .finish()
    }
}

/// Parses the optional mode argument. `Auto` resolves from `changelog-producer`
/// during planning; `Diff` is handled by core's diff planning.
fn parse_mode(raw: &str) -> DFResult<IncrementalScanMode> {
    match raw.to_ascii_lowercase().as_str() {
        "auto" => Ok(IncrementalScanMode::Auto),
        "delta" => Ok(IncrementalScanMode::Delta),
        "changelog" => Ok(IncrementalScanMode::Changelog),
        "diff" => Ok(IncrementalScanMode::Diff),
        other => Err(DataFusionError::Plan(format!(
            "{FUNCTION_NAME}: unknown scan mode '{other}', expected auto, delta, changelog, or diff"
        ))),
    }
}

impl TableFunctionImpl for IncrementalQueryFunction {
    fn call(&self, args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if args.len() < 3 || args.len() > 4 {
            return Err(DataFusionError::Plan(format!(
                "{FUNCTION_NAME} requires 3 or 4 arguments: (table_name, start_snapshot_exclusive, end_snapshot_inclusive [, mode])"
            )));
        }
        let table_name = extract_string_literal(FUNCTION_NAME, &args[0], "table_name")?;
        let start_exclusive =
            extract_int_literal(FUNCTION_NAME, &args[1], "start_snapshot_exclusive")?;
        let end_inclusive = extract_int_literal(FUNCTION_NAME, &args[2], "end_snapshot_inclusive")?;
        let mode = match args.get(3) {
            Some(expr) => parse_mode(&extract_string_literal(FUNCTION_NAME, expr, "mode")?)?,
            None => IncrementalScanMode::Auto,
        };
        if end_inclusive < start_exclusive {
            return Err(DataFusionError::Plan(format!(
                "{FUNCTION_NAME}: end_snapshot_inclusive ({end_inclusive}) must be >= start_snapshot_exclusive ({start_exclusive})"
            )));
        }

        // `db.t$audit_log` selects the audit-log projection of `db.t`.
        let (base_name, audit_log) = match table_name.strip_suffix(AUDIT_LOG_SUFFIX) {
            Some(base) => (base.to_string(), true),
            None => (table_name.clone(), false),
        };
        let identifier = parse_table_identifier(FUNCTION_NAME, &base_name, &self.default_database)?;

        let catalog = Arc::clone(&self.catalog);
        let table = block_on_with_runtime(
            async move { load_data_table_for_read(&catalog, &identifier, FUNCTION_NAME).await },
            "paimon_incremental_query: catalog access thread panicked",
        )?;

        Ok(Arc::new(IncrementalQueryTableProvider::try_new(
            table,
            audit_log,
            mode,
            start_exclusive,
            end_inclusive,
        )?))
    }
}
#[derive(Debug, Clone)]
struct IncrementalQueryTableProvider {
    table: Table,
    audit_log: bool,
    mode: IncrementalScanMode,
    start_exclusive: i64,
    end_inclusive: i64,
    schema: ArrowSchemaRef,
}

impl IncrementalQueryTableProvider {
    fn try_new(
        table: Table,
        audit_log: bool,
        mode: IncrementalScanMode,
        start_exclusive: i64,
        end_inclusive: i64,
    ) -> DFResult<Self> {
        // Audit-log output leads with `rowkind` (+ optional `_SEQUENCE_NUMBER`);
        // the plain projection is the table's own fields.
        let fields = if audit_log {
            AuditLogTable::new(table.clone())
                .fields()
                .map_err(to_datafusion_error)?
        } else {
            table.schema().fields().to_vec()
        };
        // Core streams raw Arrow (`Utf8`, not `Utf8View`), so declare the schema
        // the reader actually yields rather than the view-forced scan schema.
        let schema = datafusion_arrow_schema(&fields, false)?;
        Ok(Self {
            table,
            audit_log,
            mode,
            start_exclusive,
            end_inclusive,
            schema,
        })
    }
}

#[async_trait]
impl TableProvider for IncrementalQueryTableProvider {
    fn schema(&self) -> ArrowSchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let projected_schema = project_schema(&self.schema, projection)?;
        // An outer `LIMIT 0` needs no rows.
        if limit == Some(0) {
            return Ok(Arc::new(EmptyExec::new(projected_schema)));
        }
        Ok(Arc::new(IncrementalQueryExec::new(
            self.table.clone(),
            self.audit_log,
            self.mode,
            self.start_exclusive,
            self.end_inclusive,
            projection.cloned(),
            projected_schema,
        )))
    }
}
/// Execution-time plan: builds the incremental plan in paimon core and streams
/// its Arrow output when polled, so planning and `EXPLAIN` stay cheap.
#[derive(Debug, Clone)]
struct IncrementalQueryExec {
    table: Table,
    audit_log: bool,
    mode: IncrementalScanMode,
    start_exclusive: i64,
    end_inclusive: i64,
    projection: Option<Vec<usize>>,
    output_schema: ArrowSchemaRef,
    plan_properties: Arc<PlanProperties>,
}

impl IncrementalQueryExec {
    fn new(
        table: Table,
        audit_log: bool,
        mode: IncrementalScanMode,
        start_exclusive: i64,
        end_inclusive: i64,
        projection: Option<Vec<usize>>,
        output_schema: ArrowSchemaRef,
    ) -> Self {
        let plan_properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            table,
            audit_log,
            mode,
            start_exclusive,
            end_inclusive,
            projection,
            output_schema,
            plan_properties,
        }
    }

    async fn compute_batches(&self) -> DFResult<Vec<RecordBatch>> {
        // Core owns snapshot planning; its stream is `'static`, so it outlives
        // the transient read builder it is built from.
        let batches = await_with_runtime(async {
            let read_builder = self.table.new_read_builder();
            let scan = read_builder.new_incremental_scan(
                self.mode,
                self.start_exclusive,
                self.end_inclusive,
            );
            let plan = scan.plan().await.map_err(to_datafusion_error)?;
            let read = read_builder.new_read().map_err(to_datafusion_error)?;
            let mut stream = if self.audit_log {
                read.to_audit_log_arrow(&plan)
            } else {
                read.to_incremental_arrow(&plan)
            }
            .map_err(to_datafusion_error)?;
            let mut batches = Vec::new();
            while let Some(batch) = stream.try_next().await.map_err(to_datafusion_error)? {
                batches.push(batch);
            }
            Ok::<_, DataFusionError>(batches)
        })
        .await?;

        match &self.projection {
            Some(indices) => batches
                .iter()
                .map(|batch| batch.project(indices).map_err(DataFusionError::from))
                .collect(),
            None => Ok(batches),
        }
    }
}
impl DisplayAs for IncrementalQueryExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "IncrementalQueryExec: mode={:?}, range=({}, {}], audit_log={}",
            self.mode, self.start_exclusive, self.end_inclusive, self.audit_log
        )
    }
}

impl ExecutionPlan for IncrementalQueryExec {
    fn name(&self) -> &str {
        "IncrementalQueryExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("IncrementalQueryExec is a leaf and takes no children");
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!(
                "IncrementalQueryExec has a single partition, got partition {partition}"
            );
        }
        let exec = self.clone();
        let stream = stream::once(async move {
            let batches = exec.compute_batches().await?;
            Ok::<_, DataFusionError>(stream::iter(batches.into_iter().map(Ok)))
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            stream,
        )))
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> DFResult<Arc<Statistics>> {
        Ok(Arc::new(Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: Statistics::unknown_column(&self.output_schema),
        }))
    }
}
