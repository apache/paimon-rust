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

//! Storage execution for DataFusion's native MERGE logical plan.

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, Int32Array};
use datafusion::arrow::compute::{cast, filter_record_batch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::catalog::Session;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, DFSchema, DFSchemaRef};
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::dml::{MergeIntoAction, MergeIntoClause, MergeIntoClauseKind};
use datafusion::logical_expr::{col, expr::Case, lit, Expr, JoinType, Operator};
use datafusion::physical_plan::{DisplayAs, ExecutionPlan};
use datafusion::prelude::{DataFrame, SessionContext};
use futures::TryStreamExt;
use paimon::spec::CoreOptions;
use paimon::table::{CopyOnWriteMergeWriter, DataEvolutionDeleteWriter, Table};

use super::{
    build_partition_set_from_batches, extract_tracking_columns, is_delete_conflict,
    is_row_id_conflict, read_cow_target_batches, retry_on_conflict, validate_update_columns,
};
use crate::error::to_datafusion_error;
use crate::table::PaimonTableProvider;

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
fn bind_expr(expr: Expr, schema: &DFSchema, target_len: usize) -> Result<Expr> {
    expr.transform(|expr| match expr {
        Expr::Column(column) => Ok(Transformed::yes(col(column_name(
            schema.index_of_column(&column)?,
            target_len,
        )))),
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
    table: Table,
    source: Arc<dyn ExecutionPlan>,
    merge_schema: DFSchemaRef,
    on: Expr,
    mut clauses: Vec<MergeIntoClause>,
) -> Result<Arc<dyn ExecutionPlan>> {
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
    let de = CoreOptions::new(table.schema().options()).data_evolution_enabled();
    if !table.schema().trimmed_primary_keys().is_empty() {
        return Err(DataFusionError::Plan(
            "MERGE INTO on primary-key tables without data-evolution is not supported".into(),
        ));
    }
    let target_len = merge_schema.fields().len() - source.schema().fields().len();
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
            clause.predicate = Some(bind_expr(predicate, &merge_schema, target_len)?);
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
                    *value = bind_expr(value.clone(), &merge_schema, target_len)?;
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
                    *value = bind_expr(value.clone(), &merge_schema, target_len)?;
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
    let on = bind_expr(on, &merge_schema, target_len)?;
    let sink = PaimonMergeSink {
        table,
        schema: source.schema(),
        merge_schema,
        target_len,
        on,
        clauses,
        update_columns,
        de,
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
    ) -> Result<u64> {
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
    ) -> Result<u64> {
        let ctx = SessionContext::new_with_config_rt(
            context.session_config().clone(),
            context.runtime_env(),
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
            fields.push(Arc::new(Field::new(FILE, DataType::Int32, false)));
            fields.push(Arc::new(Field::new(OFFSET, DataType::UInt32, false)));
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
                    let condition = clause
                        .predicate
                        .clone()
                        .map_or(applicable.clone(), |p| applicable.and(p));
                    (Box::new(condition), Box::new(lit(index as i32)))
                })
                .collect(),
            Some(Box::new(lit(-1i32))),
        ));
        let action = state.create_physical_expr(action, &schema)?;
        let mut selected = vec![Vec::new(); self.clauses.len()];
        let mut seen_targets = HashSet::new();
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
                let batch = filter_record_batch(&batch, &mask)?;
                if batch.num_rows() == 0 {
                    continue;
                }
                if clause.kind == MergeIntoClauseKind::Matched {
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
                            return Err(DataFusionError::Execution(
                                "MERGE INTO found duplicate UPDATE/DELETE matches for a target row"
                                    .into(),
                            ));
                        }
                    }
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
                            fields.push(Field::new("_ROW_ID", DataType::Int64, false));
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
                    writer
                        .add_matched_delete(files.value(row) as usize, offsets.value(row) as usize);
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
            for batch in inserts {
                writer
                    .write_arrow_batch(&batch)
                    .await
                    .map_err(to_datafusion_error)?;
            }
            messages.extend(writer.prepare_commit().await.map_err(to_datafusion_error)?);
        }
        if !messages.is_empty() {
            wb.try_new_commit()
                .map_err(to_datafusion_error)?
                .commit(messages)
                .await
                .map_err(to_datafusion_error)?;
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

    fn source_partitions(&self, source: &[RecordBatch]) -> Result<Option<HashSet<Vec<u8>>>> {
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
            .collect::<Result<Vec<_>>>()?;
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
    target_type: &DataType,
) -> Result<ArrayRef> {
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
) -> Result<DataFrame> {
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
                .collect::<Result<Vec<_>>>()?;
            let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
            RecordBatch::try_new_with_options(renamed.clone(), columns, &options)
                .map_err(Into::into)
        })
        .collect::<Result<Vec<_>>>()?;
    ctx.read_table(Arc::new(MemTable::try_new(renamed, vec![batches])?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_partition_pruning_requires_a_conjunctive_target_source_equality() {
        let target = column_name(1, 2);
        let source = column_name(2, 2);
        for on in [
            col(&target).eq(col(&source)),
            col(&source).eq(col(&target)),
            col(&target)
                .eq(col(&source))
                .and(col(column_name(0, 2)).gt(lit(0))),
        ] {
            assert_eq!(partition_source_index(&on, &target, 2, 4), Some(0));
        }
        for on in [
            col(&target)
                .eq(col(&source))
                .or(col(column_name(0, 2)).eq(lit(0))),
            col(&target).eq(col(column_name(0, 2))),
            col(column_name(0, 2)).eq(col(&source)),
        ] {
            assert_eq!(partition_source_index(&on, &target, 2, 4), None);
        }
    }
}
