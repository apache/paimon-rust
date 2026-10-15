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
//! paimon core; this parses arguments, picks the core planner that matches
//! Java's `incremental-between` for the table, and streams its output.
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
use futures::{stream, StreamExt, TryStreamExt};
use paimon::catalog::Catalog;
use paimon::resource::ResourceContext;
use paimon::spec::{CoreOptions, DataField};
use paimon::table::{
    ArrowRecordBatchStream, AuditLogTable, IncrementalPlan, IncrementalScanMode, IncrementalSplit,
    Table,
};

use crate::error::to_datafusion_error;
use crate::memory::reader_resources;
use crate::physical_plan::scan::to_datafusion_batch;
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
    fields: Vec<DataField>,
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
            fields,
            schema,
        })
    }

    /// Splits a projection into the table fields core reads and the column of
    /// core's batches each output column comes from. Core's audit-log batches
    /// lead with the same audit columns as `fields`, then the read fields.
    fn read_projection(&self, indices: &[usize]) -> ReadProjection {
        let audit_columns = self.fields.len() - self.table.schema().fields().len();
        let mut read_type = Vec::new();
        let columns = indices
            .iter()
            .map(|&index| {
                if index < audit_columns {
                    return index;
                }
                read_type.push(self.fields[index].clone());
                audit_columns + read_type.len() - 1
            })
            .collect();
        ReadProjection { read_type, columns }
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
            projection.map(|indices| self.read_projection(indices)),
            projected_schema,
        )))
    }
}

/// The table fields to read, and per output column the column of core's
/// batches it comes from.
#[derive(Debug, Clone)]
struct ReadProjection {
    read_type: Vec<DataField>,
    columns: Vec<usize>,
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
    projection: Option<ReadProjection>,
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
        projection: Option<ReadProjection>,
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

    /// Plans the range in paimon core and opens its batch stream. Reader memory
    /// comes from the executing task's pool, as in the ordinary scan. The
    /// projection is pushed into the read: Delta and Changelog decode the
    /// projected fields, while Diff decodes full rows to compare them and
    /// projects its output. The stream is `'static`, so it outlives the
    /// transient read builder it is built from.
    async fn open(&self, resources: ResourceContext) -> DFResult<ArrowRecordBatchStream> {
        let mut read_builder = self.table.new_read_builder();
        read_builder.with_resources(resources);
        if let Some(projection) = &self.projection {
            read_builder.with_read_type(projection.read_type.clone());
        }
        let scan =
            read_builder.new_incremental_scan(self.mode, self.start_exclusive, self.end_inclusive);
        let plan = if self.plans_range_as_one_batch() {
            let combined = scan
                .plan_combined_delta()
                .await
                .map_err(to_datafusion_error)?;
            IncrementalPlan::try_new(
                IncrementalScanMode::Delta,
                combined
                    .into_splits()
                    .into_iter()
                    .map(IncrementalSplit::Data)
                    .collect(),
            )
        } else {
            scan.plan().await
        }
        .map_err(to_datafusion_error)?;
        let read = read_builder.new_read().map_err(to_datafusion_error)?;
        if self.audit_log {
            read.to_audit_log_arrow(&plan)
        } else {
            read.to_incremental_arrow(&plan)
        }
        .map_err(to_datafusion_error)
    }

    /// Java's `incremental-between` scan plans a data-evolution Delta range as
    /// one batch, so a column update merges into the rows it updates when they
    /// were written in the same range; planned per commit, it would come back
    /// as rows of its own. `Auto` is Delta here, since such a table has no
    /// primary key and so no changelog producer. An empty range is left to
    /// `plan`; within the available snapshots it returns no splits, as Java's
    /// empty-range scanner does.
    fn plans_range_as_one_batch(&self) -> bool {
        CoreOptions::new(self.table.schema().options()).data_evolution_enabled()
            && matches!(
                self.mode,
                IncrementalScanMode::Auto | IncrementalScanMode::Delta
            )
            && self.start_exclusive < self.end_inclusive
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
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!(
                "IncrementalQueryExec has a single partition, got partition {partition}"
            );
        }
        let exec = self.clone();
        let stream = stream::once(async move {
            let resources = reader_resources(&context, partition).map_err(to_datafusion_error)?;
            let batches = await_with_runtime(exec.open(resources)).await?;
            // Batches are forwarded as core emits them, projected, and rebuilt
            // against the declared schema as the ordinary scan does.
            let columns = exec.projection.map(|projection| projection.columns);
            let schema = exec.output_schema;
            Ok::<_, DataFusionError>(batches.map(move |batch| {
                let batch = batch.map_err(to_datafusion_error)?;
                let batch = match &columns {
                    Some(columns) => batch.project(columns)?,
                    None => batch,
                };
                to_datafusion_batch(batch, &schema)
            }))
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
