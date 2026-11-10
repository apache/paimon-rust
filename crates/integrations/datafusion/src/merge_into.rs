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

//! MERGE INTO execution for Paimon tables.
//!
//! Supports two execution paths:
//! - **Data evolution tables**: partial-column writes via [`paimon::table::DataEvolutionWriter`].
//! - **Append-only tables** (no PK, no deletion vectors): copy-on-write file rewriting
//!   via [`paimon::table::CopyOnWriteMergeWriter`].

use std::collections::HashSet;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Int32Array, RecordBatch, UInt32Array, UInt64Array,
};
use datafusion::arrow::compute::{cast, filter_record_batch};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatchOptions;
use datafusion::catalog::Session;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, DFSchema, DFSchemaRef};
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::dml::{MergeIntoAction, MergeIntoClause, MergeIntoClauseKind};
use datafusion::logical_expr::execution_props::ExecutionProps;
use datafusion::logical_expr::{col, expr::Case, lit, Expr, JoinType, Operator};
use datafusion::physical_expr::{expressions::Column as PhysicalColumn, PhysicalExpr};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan};
use datafusion::prelude::{DataFrame, SessionContext};
use futures::TryStreamExt;

use paimon::spec::{datums_to_binary_row, extract_datum_from_arrow, CoreOptions, DataField};
use paimon::table::{CopyOnWriteMergeWriter, DataEvolutionDeleteWriter, DataSplitBuilder, Table};

use crate::error::to_datafusion_error;
use crate::sql_context::SQLContext;
use crate::table::PaimonTableProvider;

mod normalize;
pub(crate) use normalize::qualify_not_matched_exprs;

const PRESENT: &str = "__paimon_merge_present";
const FILE: &str = "__paimon_file_idx";
const OFFSET: &str = "__paimon_row_offset";

fn column_name(index: usize, target_len: usize) -> String {
    if index < target_len {
        format!("__paimon_merge_t_{index}")
    } else {
        format!("__paimon_merge_s_{}", index - target_len)
    }
}

/// Bind columns by their position in the native target+source schema. SQL aliases
/// and quoted names have already been resolved by DataFusion's SQL planner.
fn bind_expr(
    expr: Expr,
    schema: &DFSchema,
    target_len: usize,
    source_only: bool,
) -> DFResult<Expr> {
    expr.transform(|expr| match expr {
        Expr::Column(column) => {
            let index = schema.index_of_column(&column)?;
            if source_only && index < target_len {
                return Err(DataFusionError::Plan(
                    "MERGE NOT MATCHED expressions may only reference source columns".into(),
                ));
            }
            Ok(Transformed::yes(col(column_name(index, target_len))))
        }
        Expr::ScalarSubquery(_)
        | Expr::Exists(_)
        | Expr::InSubquery(_)
        | Expr::OuterReferenceColumn(_, _) => Err(DataFusionError::NotImplemented(
            "Paimon MERGE expressions containing subqueries are not supported".into(),
        )),
        other => Ok(Transformed::no(other)),
    })
    .map(|transformed| transformed.data)
}

pub(crate) fn plan_merge(
    state: &dyn Session,
    table: Table,
    mut source: Arc<dyn ExecutionPlan>,
    mut merge_schema: DFSchemaRef,
    on: Expr,
    mut clauses: Vec<MergeIntoClause>,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    if table.is_branch_reference() {
        return Err(DataFusionError::Plan(format!(
            "MERGE INTO on Paimon branch '{}' is not supported",
            table.branch()
        )));
    }
    if table.is_time_traveled() {
        return Err(DataFusionError::Plan(
            "Cannot execute MERGE INTO on a time-travel table".into(),
        ));
    }
    // The target rows are found by a read of `table`, which an incremental
    // window turns into the window's changes.
    crate::table_loader::ensure_table_has_no_incremental_window(
        "Cannot execute MERGE INTO",
        table.identifier(),
        &table,
    )?;
    let de = CoreOptions::new(table.schema().options()).data_evolution_enabled();
    if !table.schema().trimmed_primary_keys().is_empty() {
        return Err(DataFusionError::Plan(
            "MERGE INTO on primary-key tables without data-evolution is not supported".into(),
        ));
    }
    let target_len = merge_schema.fields().len() - source.schema().fields().len();
    // DataFusion 55 retains every DML input field. Add a physical projection
    // before executing the source so scan pushdown can avoid unused payloads.
    let mut required = on.column_refs();
    for clause in &clauses {
        if let Some(predicate) = &clause.predicate {
            required.extend(predicate.column_refs());
        }
        match &clause.action {
            MergeIntoAction::Update(assignments) => {
                for (_, value) in assignments {
                    required.extend(value.column_refs());
                }
            }
            MergeIntoAction::Insert { values, .. } => {
                for value in values {
                    required.extend(value.column_refs());
                }
            }
            MergeIntoAction::Delete => {}
        }
    }
    let mut source_indices = required
        .into_iter()
        .map(|column| merge_schema.index_of_column(column))
        .collect::<DFResult<Vec<_>>>()?;
    source_indices.retain(|index| *index >= target_len);
    source_indices.sort_unstable();
    source_indices.dedup();
    if source_indices.len() < source.schema().fields().len() {
        let physical_schema = source.schema();
        let expressions = source_indices
            .iter()
            .map(|index| {
                let source_index = index - target_len;
                let name = physical_schema.field(source_index).name().clone();
                (
                    Arc::new(PhysicalColumn::new(&name, source_index)) as Arc<dyn PhysicalExpr>,
                    name,
                )
            })
            .collect::<Vec<_>>();
        source = Arc::new(ProjectionExec::try_new(expressions, source)?);
        let fields = (0..target_len)
            .chain(source_indices)
            .map(|index| {
                let (qualifier, field) = merge_schema.qualified_field(index);
                (qualifier.cloned(), field.clone())
            })
            .collect();
        merge_schema = Arc::new(DFSchema::new_with_metadata(
            fields,
            merge_schema.metadata().clone(),
        )?);
    }
    let mut update_columns = Vec::new();
    let mut matched_count = 0;
    for clause in &mut clauses {
        match clause.kind {
            MergeIntoClauseKind::NotMatchedBySource => {
                return Err(DataFusionError::NotImplemented(format!(
                    "WHEN NOT MATCHED BY SOURCE is not yet supported for {} MERGE INTO",
                    if de { "data evolution" } else { "CoW" }
                )));
            }
            MergeIntoClauseKind::Matched => {
                if de && matched_count > 0 {
                    return Err(DataFusionError::NotImplemented(
                        "Multiple WHEN MATCHED clauses are not yet supported".into(),
                    ));
                }
                if de && clause.predicate.is_some() {
                    return Err(DataFusionError::NotImplemented(
                        "WHEN MATCHED AND <predicate> is not yet supported".into(),
                    ));
                }
                matched_count += 1;
            }
            _ => {}
        }
        if let Some(predicate) = clause.predicate.take() {
            clause.predicate = Some(bind_expr(
                predicate,
                &merge_schema,
                target_len,
                clause.kind != MergeIntoClauseKind::Matched,
            )?);
        }
        match &mut clause.action {
            MergeIntoAction::Update(assignments) => {
                let columns = assignments
                    .iter()
                    .map(|(c, _)| c.clone())
                    .collect::<Vec<_>>();
                validate_update_columns(&columns, table.schema().fields())?;
                if de {
                    // Validate storage constraints without scanning or writing.
                    let _ = table
                        .new_write_builder()
                        .new_data_evolution_writer(columns.clone())
                        .map_err(to_datafusion_error)?;
                }
                for (name, value) in assignments {
                    if !update_columns.contains(name) {
                        update_columns.push(name.clone());
                    }
                    *value = bind_expr(value.clone(), &merge_schema, target_len, false)?;
                }
            }
            MergeIntoAction::Insert { columns, values } => {
                if columns.is_empty() {
                    *columns = table
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.name().to_string())
                        .collect();
                }
                validate_update_columns(columns, table.schema().fields())?;
                for value in values {
                    *value = bind_expr(value.clone(), &merge_schema, target_len, true)?;
                }
            }
            MergeIntoAction::Delete if de => {
                let _ = table
                    .new_write_builder()
                    .new_delete()
                    .map_err(to_datafusion_error)?;
            }
            MergeIntoAction::Delete => {}
        }
    }
    let on = bind_expr(on, &merge_schema, target_len, false)?;
    let sink = PaimonMergeSink {
        table,
        schema: source.schema(),
        merge_schema,
        target_len,
        on,
        clauses,
        update_columns,
        de,
        execution_props: state.execution_props().clone(),
    };
    Ok(Arc::new(DataSinkExec::new(source, Arc::new(sink), None)))
}

#[derive(Debug)]
struct PaimonMergeSink {
    table: Table,
    schema: SchemaRef,
    merge_schema: DFSchemaRef,
    target_len: usize,
    on: Expr,
    clauses: Vec<MergeIntoClause>,
    update_columns: Vec<String>,
    de: bool,
    execution_props: ExecutionProps,
}

impl DisplayAs for PaimonMergeSink {
    fn fmt_as(
        &self,
        _: datafusion::physical_plan::DisplayFormatType,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        write!(f, "PaimonMerge: table={}", self.table.identifier())
    }
}

#[async_trait]
impl DataSink for PaimonMergeSink {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> DFResult<u64> {
        // Capture the source once, including self-merges and volatile expressions.
        // Each conflict retry rebuilds the target scan and writers from fresh metadata.
        let source: Vec<RecordBatch> = data.try_collect().await?;
        retry_on_conflict(
            if self.de {
                "MERGE INTO"
            } else {
                "CoW MERGE INTO"
            },
            if self.de {
                is_row_id_conflict
            } else {
                is_delete_conflict
            },
            || self.execute_once(&source, context),
        )
        .await
    }
}

impl PaimonMergeSink {
    async fn execute_once(
        &self,
        source: &[RecordBatch],
        context: &Arc<TaskContext>,
    ) -> DFResult<u64> {
        let ctx = SessionContext::new_with_state(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(context.session_config().clone())
                .with_runtime_env(context.runtime_env())
                .with_execution_props(self.execution_props.clone())
                .build(),
        );
        let target_schema = Arc::new(Schema::new(
            self.merge_schema.fields()[..self.target_len].to_vec(),
        ));
        let source_schema = Arc::new(Schema::new(
            self.merge_schema.fields()[self.target_len..].to_vec(),
        ));
        let source_df = memory_relation(
            &ctx,
            source,
            source_schema,
            self.target_len,
            self.target_len,
        )?;
        let mut cow_writer = None;
        let mut read_snapshot = None;
        let (target_df, read_table) = if self.de {
            let snapshot = self
                .table
                .resolve_read_snapshot()
                .await
                .map_err(to_datafusion_error)?;
            read_snapshot = snapshot.as_ref().map(|s| s.id());
            let read_table = self.table.copy_with_pinned_snapshot(snapshot.as_ref());
            // Keep the target as a scan with a projection, so updating a small
            // column does not read unrelated BLOB payloads or other wide fields.
            let target = ctx
                .read_table(Arc::new(PaimonTableProvider::try_new(read_table.clone())?))?
                .select(self.target_projection())?;
            (target, read_table)
        } else {
            let partitions = self.source_partitions(source)?;
            let writer =
                CopyOnWriteMergeWriter::new(&self.table, self.update_columns.clone(), partitions)
                    .await
                    .map_err(to_datafusion_error)?;
            let target = read_cow_target_batches(&self.table, &writer).await?;
            cow_writer = Some(writer);
            let mut fields = target_schema.fields().to_vec();
            fields.push(Arc::new(Field::new(FILE, ArrowDataType::Int32, false)));
            fields.push(Arc::new(Field::new(OFFSET, ArrowDataType::UInt32, false)));
            let target = memory_relation(
                &ctx,
                &target,
                Arc::new(Schema::new(fields)),
                0,
                self.target_len,
            )?;
            (target, self.table.clone())
        };
        let mut target_projection = target_df
            .schema()
            .fields()
            .iter()
            .map(|f| col(f.name()))
            .collect::<Vec<_>>();
        target_projection.push(lit(true).alias(PRESENT));
        let joined = source_df.join_on(
            target_df.select(target_projection)?,
            JoinType::Left,
            [self.on.clone()],
        )?;
        let schema = joined.schema().clone();
        let state = ctx.state();
        let action = Expr::Case(Case::new(
            None,
            self.clauses
                .iter()
                .enumerate()
                .map(|(index, clause)| {
                    let applicable = if clause.kind == MergeIntoClauseKind::Matched {
                        col(PRESENT).is_not_null()
                    } else {
                        col(PRESENT).is_null()
                    };
                    // AND does not guarantee short-circuit evaluation for mixed
                    // Arrow batches. CASE keeps predicates in their row scope.
                    let condition =
                        clause
                            .predicate
                            .clone()
                            .map_or(applicable.clone(), |predicate| {
                                Expr::Case(Case::new(
                                    None,
                                    vec![(Box::new(applicable), Box::new(predicate))],
                                    Some(Box::new(lit(false))),
                                ))
                            });
                    (Box::new(condition), Box::new(lit(index as i32)))
                })
                .collect(),
            Some(Box::new(lit(-1i32))),
        ));
        let action = state.create_physical_expr(action, &schema)?;
        let mut selected = vec![Vec::new(); self.clauses.len()];
        let mut seen_targets = HashSet::new();
        let matched = self
            .clauses
            .iter()
            .filter(|c| c.kind == MergeIntoClauseKind::Matched)
            .collect::<Vec<_>>();
        let deduplicate_delete = matched.len() == 1
            && matched[0].predicate.is_none()
            && matches!(matched[0].action, MergeIntoAction::Delete);
        for batch in joined.collect().await? {
            let actions = action.evaluate(&batch)?.into_array(batch.num_rows())?;
            let actions = actions
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| DataFusionError::Internal("Invalid MERGE action type".into()))?;
            for (index, clause) in self.clauses.iter().enumerate() {
                let mask = BooleanArray::from_iter(
                    (0..batch.num_rows()).map(|row| Some(actions.value(row) == index as i32)),
                );
                let mut batch = filter_record_batch(&batch, &mask)?;
                if batch.num_rows() == 0 {
                    continue;
                }
                if clause.kind == MergeIntoClauseKind::Matched {
                    let mut keep = Vec::with_capacity(batch.num_rows());
                    for row in 0..batch.num_rows() {
                        let key = if self.de {
                            let row_id = batch
                                .column_by_name(&column_name(self.target_len - 1, self.target_len))
                                .unwrap();
                            (
                                0,
                                row_id
                                    .as_any()
                                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                                    .unwrap()
                                    .value(row) as u64,
                            )
                        } else {
                            let (file, offset) = extract_tracking_columns(&batch)?;
                            (file.value(row), offset.value(row) as u64)
                        };
                        if !seen_targets.insert(key) {
                            if deduplicate_delete {
                                keep.push(false);
                                continue;
                            }
                            return Err(DataFusionError::Execution(
                                "MERGE INTO found duplicate UPDATE/DELETE matches for a target row"
                                    .into(),
                            ));
                        }
                        keep.push(true);
                    }
                    batch = filter_record_batch(&batch, &BooleanArray::from(keep))?;
                }
                selected[index].push(batch);
            }
        }

        // Evaluate and validate every action before staging files or publishing.
        let table_schema = paimon::arrow::build_target_arrow_schema(self.table.schema().fields())
            .map_err(to_datafusion_error)?;
        let mut updates = Vec::new();
        let mut deletes = Vec::new();
        let mut inserts = Vec::new();
        let mut count = 0;
        for (clause, batches) in self.clauses.iter().zip(selected) {
            for batch in batches {
                count += batch.num_rows() as u64;
                match &clause.action {
                    MergeIntoAction::Update(assignments) => {
                        let mut fields = Vec::new();
                        let mut columns = Vec::new();
                        if self.de {
                            fields.push(Field::new("_ROW_ID", ArrowDataType::Int64, false));
                            columns.push(
                                batch
                                    .column_by_name(&column_name(
                                        self.target_len - 1,
                                        self.target_len,
                                    ))
                                    .unwrap()
                                    .clone(),
                            );
                        }
                        for name in &self.update_columns {
                            let target_index = table_schema.index_of(name)?;
                            let value = assignments
                                .iter()
                                .find(|(c, _)| c == name)
                                .map(|(_, e)| e.clone())
                                .unwrap_or_else(|| col(column_name(target_index, self.target_len)));
                            fields.push(table_schema.field(target_index).clone());
                            columns.push(evaluate(
                                &state,
                                &schema,
                                &batch,
                                value,
                                table_schema.field(target_index).data_type(),
                            )?);
                        }
                        updates.push((
                            batch,
                            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?,
                        ));
                    }
                    MergeIntoAction::Delete => deletes.push(batch),
                    MergeIntoAction::Insert { columns, values } => {
                        let mut arrays = Vec::new();
                        for field in table_schema.fields() {
                            let expr = columns
                                .iter()
                                .position(|c| c == field.name())
                                .map(|i| values[i].clone())
                                .unwrap_or_else(|| lit(datafusion::common::ScalarValue::Null));
                            arrays.push(evaluate(
                                &state,
                                &schema,
                                &batch,
                                expr,
                                field.data_type(),
                            )?);
                        }
                        inserts.push(RecordBatch::try_new(table_schema.clone(), arrays)?);
                    }
                }
            }
        }
        let wb = self.table.new_write_builder();
        let mut messages = Vec::new();
        let commit = wb.try_new_commit().map_err(to_datafusion_error)?;
        let staged: DFResult<()> = async {
            if let Some(mut writer) = cow_writer {
                for (batch_index, (tracking, _)) in updates.iter().enumerate() {
                    let (files, offsets) = extract_tracking_columns(tracking)?;
                    for row in 0..tracking.num_rows() {
                        writer.add_matched_update(
                            files.value(row) as usize,
                            offsets.value(row) as usize,
                            batch_index,
                            row,
                        );
                    }
                }
                for batch in deletes {
                    let (files, offsets) = extract_tracking_columns(&batch)?;
                    for row in 0..batch.num_rows() {
                        writer.add_matched_delete(
                            files.value(row) as usize,
                            offsets.value(row) as usize,
                        );
                    }
                }
                writer.set_update_batches(updates.into_iter().map(|(_, values)| values).collect());
                messages.extend(writer.prepare_commit().await.map_err(to_datafusion_error)?);
            } else {
                if !updates.is_empty() {
                    let mut writer = wb
                        .new_data_evolution_writer(self.update_columns.clone())
                        .map_err(to_datafusion_error)?;
                    if let Some(snapshot) = read_snapshot {
                        writer.pin_read_snapshot(snapshot);
                    }
                    for (_, values) in updates {
                        writer
                            .add_matched_batch(values)
                            .map_err(to_datafusion_error)?;
                    }
                    messages.extend(writer.prepare_commit().await.map_err(to_datafusion_error)?);
                }
                if !deletes.is_empty() {
                    let mut writer =
                        DataEvolutionDeleteWriter::new(&read_table).map_err(to_datafusion_error)?;
                    for batch in deletes {
                        let row_id = batch
                            .column_by_name(&column_name(self.target_len - 1, self.target_len))
                            .unwrap();
                        let ids = row_id
                            .as_any()
                            .downcast_ref::<datafusion::arrow::array::Int64Array>()
                            .unwrap();
                        writer
                            .add_row_ids(ids.values().iter().copied())
                            .map_err(to_datafusion_error)?;
                    }
                    messages.extend(writer.prepare_commit().await.map_err(to_datafusion_error)?);
                }
            }
            if !inserts.is_empty() {
                let mut writer = wb.new_write().map_err(to_datafusion_error)?;
                let result = async {
                    for batch in inserts {
                        writer
                            .write_arrow_batch(&batch)
                            .await
                            .map_err(to_datafusion_error)?;
                    }
                    writer.prepare_commit().await.map_err(to_datafusion_error)
                }
                .await;
                writer.close().await;
                messages.extend(result?);
            }
            Ok(())
        }
        .await;
        if let Err(error) = staged {
            // These files have never been submitted. Once commit starts, its
            // outcome may be unknown and deleting output would be unsafe.
            if let Err(cleanup) = commit.abort(&messages).await {
                log::warn!("Failed to abort unsubmitted MERGE files: {cleanup}");
            }
            return Err(error);
        }
        if !messages.is_empty() {
            commit.commit(messages).await.map_err(to_datafusion_error)?;
        }
        Ok(count)
    }

    fn target_projection(&self) -> Vec<Expr> {
        let mut used = self.on.column_refs();
        for clause in &self.clauses {
            if let Some(predicate) = &clause.predicate {
                used.extend(predicate.column_refs());
            }
            match &clause.action {
                MergeIntoAction::Update(assignments) => {
                    for (_, expr) in assignments {
                        used.extend(expr.column_refs());
                    }
                }
                MergeIntoAction::Insert { values, .. } => {
                    for expr in values {
                        used.extend(expr.column_refs());
                    }
                }
                MergeIntoAction::Delete => {}
            }
        }
        (0..self.target_len)
            .filter(|index| {
                *index == self.target_len - 1
                    || used
                        .iter()
                        .any(|c| c.name == column_name(*index, self.target_len))
            })
            .map(|index| {
                let field = self.merge_schema.field(index);
                datafusion::logical_expr::expr_fn::cast(
                    Expr::Column(Column::new_unqualified(field.name())),
                    field.data_type().clone(),
                )
                .alias(column_name(index, self.target_len))
            })
            .collect()
    }

    fn source_partitions(&self, source: &[RecordBatch]) -> DFResult<Option<HashSet<Vec<u8>>>> {
        let keys = self.table.schema().partition_keys();
        if keys.is_empty() {
            return Ok(None);
        }
        let mut indices = Vec::new();
        for key in keys {
            let target_index = self
                .table
                .schema()
                .fields()
                .iter()
                .position(|f| f.name() == key)
                .unwrap();
            let target = column_name(target_index, self.target_len);
            let Some(index) = partition_source_index(
                &self.on,
                &target,
                self.target_len,
                self.merge_schema.fields().len(),
            ) else {
                return Ok(None);
            };
            indices.push(index);
        }
        let partitions = source
            .iter()
            .map(|b| b.project(&indices).map_err(Into::into))
            .collect::<DFResult<Vec<_>>>()?;
        build_partition_set_from_batches(&self.table, &partitions)
    }
}

fn partition_source_index(
    expr: &Expr,
    target: &str,
    target_len: usize,
    combined_len: usize,
) -> Option<usize> {
    if let Expr::BinaryExpr(binary) = expr {
        if binary.op == Operator::And {
            return partition_source_index(&binary.left, target, target_len, combined_len).or_else(
                || partition_source_index(&binary.right, target, target_len, combined_len),
            );
        }
        if binary.op == Operator::Eq {
            for (left, right) in [(&binary.left, &binary.right), (&binary.right, &binary.left)] {
                if matches!(left.as_ref(), Expr::Column(c) if c.name == target) {
                    if let Expr::Column(column) = right.as_ref() {
                        return (target_len..combined_len)
                            .find(|i| column.name == column_name(*i, target_len))
                            .map(|i| i - target_len);
                    }
                }
            }
        }
    }
    None
}

fn evaluate(
    state: &impl Session,
    schema: &DFSchema,
    batch: &RecordBatch,
    expr: Expr,
    target_type: &ArrowDataType,
) -> DFResult<ArrayRef> {
    let value = state
        .create_physical_expr(expr, schema)?
        .evaluate(batch)?
        .into_array(batch.num_rows())?;
    cast(value.as_ref(), target_type).map_err(Into::into)
}

fn memory_relation(
    ctx: &SessionContext,
    batches: &[RecordBatch],
    schema: SchemaRef,
    start: usize,
    target_len: usize,
) -> DFResult<DataFrame> {
    let renamed = Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let name = if start == 0 && i >= target_len {
                    f.name().clone()
                } else {
                    column_name(start + i, target_len)
                };
                f.as_ref().clone().with_name(name)
            })
            .collect::<Vec<_>>(),
    ));
    let batches = batches
        .iter()
        .map(|batch| {
            let columns = schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, field)| {
                    // File tracking columns are appended after user fields.
                    // Position preserves their identity even when a user column
                    // has the same name as one of those internal fields.
                    let array = batch.column(i);
                    cast(array.as_ref(), field.data_type()).map_err(Into::into)
                })
                .collect::<DFResult<Vec<_>>>()?;
            let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
            RecordBatch::try_new_with_options(renamed.clone(), columns, &options)
                .map_err(Into::into)
        })
        .collect::<DFResult<Vec<_>>>()?;
    ctx.read_table(Arc::new(MemTable::try_new(renamed, vec![batches])?))
}

/// Maximum number of retries when DML conflicts with concurrent compaction.
const DML_MAX_RETRIES: u32 = 5;

/// Quote a SQL identifier by wrapping in double-quotes and escaping embedded quotes.
pub(crate) fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

static COW_TABLE_COUNTER: AtomicU64 = AtomicU64::new(0);

fn next_cow_table_name(prefix: &str) -> String {
    let id = COW_TABLE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{id}")
}

/// Tracks registered temporary table names and auto-cleans them up on drop.
///
/// This RAII guard ensures temp tables are always deregistered, even if the
/// enclosing function panics or returns early with an error.
pub(crate) struct TempTableTracker<'a> {
    tables: Vec<String>,
    ctx: &'a SQLContext,
}

impl<'a> TempTableTracker<'a> {
    pub(crate) fn new(ctx: &'a SQLContext) -> Self {
        Self {
            tables: Vec::new(),
            ctx,
        }
    }

    pub(crate) fn register(&mut self, table_name: &str) {
        self.tables.push(table_name.to_string());
    }
}

impl Drop for TempTableTracker<'_> {
    fn drop(&mut self) {
        for table in &self.tables {
            let _ = self.ctx.deregister_temp_table(table);
        }
    }
}

/// Retry a DML operation on conflict, using `is_retryable` to detect retryable errors.
pub(crate) async fn retry_on_conflict<F, Fut, T>(
    op_name: &str,
    is_retryable: fn(&DataFusionError) -> bool,
    mut action: F,
) -> DFResult<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = DFResult<T>>,
{
    let mut last_err = None;
    for _ in 0..DML_MAX_RETRIES {
        match action().await {
            Ok(df) => return Ok(df),
            Err(e) if is_retryable(&e) => {
                last_err = Some(e);
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    Err(DataFusionError::External(Box::new(std::io::Error::other(
        format!(
            "{op_name} failed after {DML_MAX_RETRIES} retries due to concurrent compaction: {}",
            last_err.unwrap()
        ),
    ))))
}

/// Check if a DataFusion error is caused by a row ID conflict during commit.
pub(crate) fn is_row_id_conflict(err: &DataFusionError) -> bool {
    match err {
        DataFusionError::External(e) => {
            matches!(
                e.downcast_ref::<paimon::Error>(),
                Some(paimon::Error::RowIdExistenceConflict { .. })
            ) || e.to_string().contains("Row ID conflict")
        }
        _ => false,
    }
}

/// Check if a DataFusion error is caused by a delete conflict during commit.
pub(crate) fn is_delete_conflict(err: &DataFusionError) -> bool {
    match err {
        DataFusionError::External(e) => {
            matches!(
                e.downcast_ref::<paimon::Error>(),
                Some(paimon::Error::RowIdExistenceConflict { .. })
            ) || e.to_string().contains("Delete conflict")
        }
        _ => false,
    }
}

/// Extract _ROW_ID + __upd_X columns from matched batches, renaming __upd_X → X.
pub(crate) fn project_update_columns(
    batches: &[RecordBatch],
    update_columns: &[String],
) -> DFResult<Vec<RecordBatch>> {
    let mut result = Vec::new();
    for batch in batches {
        let row_id_idx = batch
            .schema()
            .index_of("_ROW_ID")
            .map_err(|e| DataFusionError::Internal(format!("_ROW_ID not found: {e}")))?;

        let mut columns = vec![batch.column(row_id_idx).clone()];
        let mut fields = vec![batch.schema().field(row_id_idx).clone()];

        for col in update_columns {
            let prefixed = format!("__upd_{col}");
            let idx = batch.schema().index_of(&prefixed).map_err(|e| {
                DataFusionError::Internal(format!("Column {prefixed} not found: {e}"))
            })?;
            columns.push(batch.column(idx).clone());
            fields.push(Field::new(
                col,
                batch.schema().field(idx).data_type().clone(),
                true,
            ));
        }

        let schema = Arc::new(Schema::new(fields));
        let projected = RecordBatch::try_new(schema, columns)?;
        result.push(projected);
    }
    Ok(result)
}

pub(crate) fn validate_update_columns(
    columns: &[String],
    table_fields: &[DataField],
) -> DFResult<()> {
    validate_target_columns(columns, table_fields, "UPDATE")
}

fn validate_target_columns(
    columns: &[String],
    table_fields: &[DataField],
    operation: &str,
) -> DFResult<()> {
    let target_columns: HashSet<&str> = table_fields.iter().map(|field| field.name()).collect();
    let mut seen = HashSet::new();

    for column in columns {
        if !seen.insert(column.as_str()) {
            return Err(DataFusionError::Plan(format!(
                "Duplicate column '{column}' in {operation}"
            )));
        }
        if !target_columns.contains(column.as_str()) {
            return Err(DataFusionError::Plan(format!(
                "Unknown column '{column}' in {operation}"
            )));
        }
    }

    Ok(())
}

pub(crate) fn extract_tracking_columns(
    batch: &RecordBatch,
) -> DFResult<(&Int32Array, &UInt32Array)> {
    let file_idx_col = batch
        .column_by_name("__paimon_file_idx")
        .ok_or_else(|| DataFusionError::Internal("__paimon_file_idx not found".to_string()))?
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| DataFusionError::Internal("__paimon_file_idx is not Int32".to_string()))?;

    let row_offset_col = batch
        .column_by_name("__paimon_row_offset")
        .ok_or_else(|| DataFusionError::Internal("__paimon_row_offset not found".to_string()))?
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| {
            DataFusionError::Internal("__paimon_row_offset is not UInt32".to_string())
        })?;

    Ok((file_idx_col, row_offset_col))
}

/// Read all files from a table via the CoW writer's file index, attach `__paimon_file_idx`
/// and `__paimon_row_offset` tracking columns, and register the result as a MemTable.
///
/// Returns `(has_data, cow_table_name)`. The caller is responsible for deregistering
/// via `TempTableTracker`.
///
/// Note: all matching partition files are loaded into memory at once. For partitions
/// with many large files this may cause significant memory pressure. A future
/// optimization could stream or batch-process files instead of materializing everything.
pub(crate) async fn register_cow_target_table(
    ctx: &SQLContext,
    table: &Table,
    writer: &CopyOnWriteMergeWriter,
    temp_tracker: &mut TempTableTracker<'_>,
) -> DFResult<(bool, String)> {
    let all_batches = read_cow_target_batches(table, writer).await?;
    let has_data = !all_batches.is_empty();
    let table_name = next_cow_table_name("__cow_target");
    if let Some(batch) = all_batches.first() {
        let mem_table =
            datafusion::datasource::MemTable::try_new(batch.schema(), vec![all_batches])?;
        ctx.register_temp_table(&table_name, Arc::new(mem_table))?;
        temp_tracker.register(&table_name);
    }
    Ok((has_data, table_name))
}

/// Read a captured CoW file index, retaining physical file/row identity.
async fn read_cow_target_batches(
    table: &Table,
    writer: &CopyOnWriteMergeWriter,
) -> DFResult<Vec<RecordBatch>> {
    let file_index = writer.file_index();
    // Read all files in parallel
    let read_futures: Vec<_> = file_index
        .iter()
        .enumerate()
        .map(|(file_idx, file_info)| async move {
            let single_split = DataSplitBuilder::new()
                .with_snapshot(file_info.snapshot_id)
                .with_partition(
                    paimon::spec::BinaryRow::from_serialized_bytes(&file_info.partition)
                        .map_err(to_datafusion_error)?,
                )
                .with_bucket(file_info.bucket)
                .with_bucket_path(file_info.bucket_path.clone())
                .with_total_buckets(file_info.total_buckets)
                .with_data_files(vec![file_info.file_meta.clone()])
                .build()
                .map_err(to_datafusion_error)?;

            let read = table
                .new_read_builder()
                .new_read()
                .map_err(to_datafusion_error)?;
            let batches: Vec<RecordBatch> = read
                .to_arrow(&[single_split])
                .map_err(to_datafusion_error)?
                .try_collect()
                .await
                .map_err(to_datafusion_error)?;

            Ok::<_, DataFusionError>((file_idx, batches))
        })
        .collect();

    let file_results = futures::future::try_join_all(read_futures).await?;

    let mut all_batches: Vec<RecordBatch> = Vec::new();

    for (file_idx, batches) in file_results {
        let mut row_offset = 0u32;
        for batch in batches {
            let num_rows = batch.num_rows();
            if num_rows == 0 {
                continue;
            }

            let file_idx_i32 = i32::try_from(file_idx).map_err(|_| {
                DataFusionError::Internal(format!("file_idx {file_idx} exceeds i32 range"))
            })?;
            let num_rows_u32 = u32::try_from(num_rows).map_err(|_| {
                DataFusionError::Internal(format!("batch num_rows {num_rows} exceeds u32 range"))
            })?;
            let file_idx_col = Arc::new(Int32Array::from(vec![file_idx_i32; num_rows]));
            let end_offset = row_offset.checked_add(num_rows_u32).ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "row_offset overflow: {row_offset} + {num_rows_u32}"
                ))
            })?;
            let row_offset_col = Arc::new(UInt32Array::from(
                (row_offset..end_offset).collect::<Vec<_>>(),
            ));

            let mut fields: Vec<Field> = batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.as_ref().clone())
                .collect();
            fields.push(Field::new("__paimon_file_idx", ArrowDataType::Int32, false));
            fields.push(Field::new(
                "__paimon_row_offset",
                ArrowDataType::UInt32,
                false,
            ));
            let augmented_schema = Arc::new(Schema::new(fields));

            let mut columns: Vec<Arc<dyn Array>> = batch.columns().to_vec();
            columns.push(file_idx_col);
            columns.push(row_offset_col);

            let augmented = RecordBatch::try_new(augmented_schema.clone(), columns)
                .map_err(|e| DataFusionError::Internal(format!("Failed to augment batch: {e}")))?;

            all_batches.push(augmented);
            row_offset = end_offset;
        }
    }

    Ok(all_batches)
}

/// Build a partition set from Arrow batches containing partition column values.
///
/// Converts each row's partition columns into serialized `BinaryRow` bytes.
/// Returns `None` for non-partitioned tables.
pub(crate) fn build_partition_set_from_batches(
    table: &Table,
    batches: &[RecordBatch],
) -> DFResult<Option<HashSet<Vec<u8>>>> {
    let partition_keys = table.schema().partition_keys();
    if partition_keys.is_empty() {
        return Ok(None);
    }

    let partition_fields = table.schema().partition_fields();
    let mut partition_set = HashSet::new();

    for batch in batches {
        for row in 0..batch.num_rows() {
            let datums: Vec<(Option<paimon::spec::Datum>, paimon::spec::DataType)> =
                partition_fields
                    .iter()
                    .enumerate()
                    .map(|(col_idx, field)| {
                        let datum =
                            extract_datum_from_arrow(batch, row, col_idx, field.data_type())
                                .map_err(to_datafusion_error)?;
                        Ok((datum, field.data_type().clone()))
                    })
                    .collect::<DFResult<_>>()?;
            let refs: Vec<(&Option<paimon::spec::Datum>, &paimon::spec::DataType)> =
                datums.iter().map(|(d, t)| (d, t)).collect();
            partition_set.insert(datums_to_binary_row(&refs));
        }
    }

    Ok(Some(partition_set))
}

/// Query a table for distinct partition values matching an optional WHERE clause.
///
/// Returns `None` for non-partitioned tables.
pub(crate) async fn build_partition_set_from_where(
    ctx: &SQLContext,
    table: &Table,
    table_ref: &str,
    where_clause: Option<&str>,
) -> DFResult<Option<HashSet<Vec<u8>>>> {
    let partition_keys = table.schema().partition_keys();
    if partition_keys.is_empty() {
        return Ok(None);
    }

    let cols = partition_keys
        .iter()
        .map(|k| quote_identifier(k))
        .collect::<Vec<_>>()
        .join(", ");
    let where_part = match where_clause {
        Some(w) => format!(" WHERE {w}"),
        None => String::new(),
    };
    let sql = format!("SELECT DISTINCT {cols} FROM {table_ref}{where_part}");
    let batches = ctx.ctx().sql(&sql).await?.collect().await?;

    build_partition_set_from_batches(table, &batches)
}

/// Return a DataFrame with a single "count" column.
pub(crate) fn ok_result(ctx: &SessionContext, count: u64) -> DFResult<DataFrame> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "count",
        ArrowDataType::UInt64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt64Array::from(vec![count]))],
    )?;
    ctx.read_batch(batch)
}

#[cfg(test)]
mod tests;
