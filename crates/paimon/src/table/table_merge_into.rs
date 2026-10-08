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

//! Data-evolution MERGE orchestration, independent of an expression engine.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Int64Array, RecordBatch, UInt64Array};
use arrow_row::{RowConverter, SortField};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_select::{concat::concat_batches, take::take};
use futures::{future::BoxFuture, TryStreamExt};

use super::data_evolution_writer::RowIdFileIndex;
use super::{CommitMessage, DataEvolutionWriter, Table, TableUpdateByRowId, UpdateAssignment};

const ROW_ID: &str = "_ROW_ID";
type ConditionFunction =
    dyn Fn(RecordBatch) -> BoxFuture<'static, crate::Result<BooleanArray>> + Send + Sync;

/// An engine-compiled condition. NULL evaluates to false when choosing a clause.
/// Dependencies control target projection, including source fields for self MERGE.
/// Evaluation is asynchronous so expression engines can execute subquery plans.
#[derive(Clone)]
pub struct MergeCondition {
    pub target_columns: Vec<String>,
    pub source_columns: Vec<String>,
    pub evaluate: Arc<ConditionFunction>,
}

/// SET/INSERT values. Column references are evaluated by core, without callbacks.
#[derive(Clone)]
pub enum MergeAssignment {
    SourceColumn(String),
    TargetColumn(String),
    /// Evaluate once across all selected rows of this clause, in target scan order.
    Value(UpdateAssignment),
}

/// Ordered matched clauses, corresponding to WHEN MATCHED UPDATE/DELETE.
#[derive(Clone)]
pub struct WhenMatched {
    pub condition: Option<MergeCondition>,
    pub delete: bool,
    pub assignments: Vec<(String, MergeAssignment)>,
}

/// Ordered WHEN NOT MATCHED INSERT clauses.
#[derive(Clone)]
pub struct WhenNotMatched {
    pub condition: Option<MergeCondition>,
    pub assignments: Vec<(String, MergeAssignment)>,
}

/// Already materialized input, or the target itself joined on its row ID.
pub enum MergeSource {
    Batches(Vec<RecordBatch>),
    SelfTable,
}

fn invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

fn gather(batch: &RecordBatch, rows: &[usize]) -> crate::Result<RecordBatch> {
    let indices = UInt64Array::from(rows.iter().map(|&row| row as u64).collect::<Vec<_>>());
    let columns = batch
        .columns()
        .iter()
        .map(|array| {
            take(array.as_ref(), &indices, None).map_err(|error| invalid(error.to_string()))
        })
        .collect::<crate::Result<_>>()?;
    RecordBatch::try_new(batch.schema(), columns).map_err(|error| invalid(error.to_string()))
}

fn aliases(target: Option<&RecordBatch>, source: &RecordBatch) -> crate::Result<RecordBatch> {
    let mut fields = Vec::new();
    let mut columns = Vec::new();
    if let Some(target) = target {
        fields.push(Arc::new(Field::new(ROW_ID, DataType::Int64, false)));
        columns.push(target.column_by_name(ROW_ID).unwrap().clone());
        for (field, column) in target.schema().fields().iter().zip(target.columns()) {
            fields.push(Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_name(format!("t.{}", field.name())),
            ));
            columns.push(column.clone());
        }
    } else {
        fields.push(Arc::new(Field::new(ROW_ID, DataType::Int64, false)));
        columns.push(Arc::new(Int64Array::from(vec![0; source.num_rows()])));
    }
    for (field, column) in source.schema().fields().iter().zip(source.columns()) {
        fields.push(Arc::new(
            field
                .as_ref()
                .clone()
                .with_name(format!("s.{}", field.name())),
        ));
        columns.push(column.clone());
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|error| invalid(error.to_string()))
}

async fn select_clause(
    batch: RecordBatch,
    condition: &Option<MergeCondition>,
) -> crate::Result<(RecordBatch, RecordBatch)> {
    let mask = match condition {
        Some(condition) => Some((condition.evaluate)(batch.clone()).await?),
        None => None,
    };
    if mask
        .as_ref()
        .is_some_and(|mask| mask.len() != batch.num_rows())
    {
        return Err(invalid("MERGE condition length must match input row count"));
    }
    let mut selected = Vec::new();
    let mut remaining = Vec::new();
    for row in 0..batch.num_rows() {
        if mask
            .as_ref()
            .is_none_or(|mask| !mask.is_null(row) && mask.value(row))
        {
            selected.push(row);
        } else {
            remaining.push(row);
        }
    }
    Ok((gather(&batch, &selected)?, gather(&batch, &remaining)?))
}

fn column_assignment(
    batches: &[RecordBatch],
    prefix: &str,
    name: &str,
) -> crate::Result<UpdateAssignment> {
    Ok(UpdateAssignment::Array(
        batches
            .iter()
            .map(|batch| {
                batch
                    .column_by_name(&format!("{prefix}.{name}"))
                    .cloned()
                    .ok_or_else(|| invalid(format!("Missing MERGE {prefix} column {name}")))
            })
            .collect::<crate::Result<_>>()?,
    ))
}

fn assigned(
    batches: &[RecordBatch],
    assignments: &[(String, MergeAssignment)],
    columns: &[String],
    schema: SchemaRef,
    insert: bool,
) -> crate::Result<Vec<RecordBatch>> {
    let mut values = Vec::new();
    for name in columns {
        let value = assignments
            .iter()
            .find(|(column, _)| column == name)
            .map(|(_, value)| value);
        let assignment = match value {
            Some(MergeAssignment::Value(value)) => value.clone(),
            Some(MergeAssignment::SourceColumn(column)) => column_assignment(batches, "s", column)?,
            Some(MergeAssignment::TargetColumn(column)) => column_assignment(batches, "t", column)?,
            None if insert => UpdateAssignment::Scalar(arrow_array::new_null_array(
                schema.field_with_name(name).unwrap().data_type(),
                1,
            )),
            None => column_assignment(batches, "t", name)?,
        };
        values.push((name.clone(), assignment));
    }
    super::update_assignment::assigned_batches(batches, values, schema)
}

pub(super) async fn merge_into(
    table: &Table,
    commit_user: &str,
    source: MergeSource,
    on: Vec<(String, String)>,
    matched: Vec<WhenMatched>,
    not_matched: Vec<WhenNotMatched>,
) -> crate::Result<Vec<CommitMessage>> {
    if on.is_empty() || matched.is_empty() && not_matched.is_empty() {
        return Err(invalid("MERGE requires ON keys and at least one action"));
    }
    let schema = crate::arrow::build_target_arrow_schema(table.schema().fields())?;
    let mut update_columns = Vec::new();
    let mut projection: HashSet<String> = on.iter().map(|(target, _)| target.clone()).collect();
    projection.insert(ROW_ID.into());
    for clause in &matched {
        if clause.delete && !clause.assignments.is_empty() {
            return Err(invalid("MERGE DELETE cannot have assignments"));
        }
        for (name, value) in &clause.assignments {
            schema
                .field_with_name(name)
                .map_err(|error| invalid(error.to_string()))?;
            if table.schema().partition_keys().contains(name) {
                return Err(invalid("MERGE does not support updating partition columns"));
            }
            if !update_columns.contains(name) {
                update_columns.push(name.clone());
            }
            if let MergeAssignment::TargetColumn(name) = value {
                projection.insert(name.clone());
            }
            if matches!(value, MergeAssignment::Value(UpdateAssignment::Function(_))) {
                projection.extend(schema.fields().iter().map(|field| field.name().clone()));
            }
        }
        if let Some(condition) = &clause.condition {
            projection.extend(condition.target_columns.clone());
        }
    }
    projection.extend(update_columns.clone());
    for clause in &not_matched {
        if clause
            .condition
            .as_ref()
            .is_some_and(|condition| !condition.target_columns.is_empty())
            || clause
                .assignments
                .iter()
                .any(|(_, value)| matches!(value, MergeAssignment::TargetColumn(_)))
        {
            return Err(invalid("WHEN NOT MATCHED cannot reference target columns"));
        }
    }
    for conditions in [
        matched
            .iter()
            .map(|clause| &clause.condition)
            .collect::<Vec<_>>(),
        not_matched.iter().map(|clause| &clause.condition).collect(),
    ] {
        if conditions
            .iter()
            .take(conditions.len().saturating_sub(1))
            .any(|condition| condition.is_none())
        {
            return Err(invalid("Only the last MERGE clause may omit its condition"));
        }
    }
    let _ = DataEvolutionWriter::new(table, update_columns.clone())?;
    let snapshot = super::time_travel::resolve_snapshot(table).await?;
    let scan_table = match &snapshot {
        Some(snapshot) => table.copy_with_pinned_snapshot(snapshot),
        None => table.clone(),
    }
    .copy_with_options(HashMap::from([
        ("scalar-index.search-mode".into(), "FULL".into()),
        ("scan.mode".into(), "default".into()),
    ]));
    let self_merge = matches!(source, MergeSource::SelfTable);
    if self_merge {
        if on != [(ROW_ID.into(), ROW_ID.into())] || !not_matched.is_empty() {
            return Err(invalid(
                "Self MERGE requires ON _ROW_ID and no insert clauses",
            ));
        }
        for clause in &matched {
            if let Some(condition) = &clause.condition {
                projection.extend(condition.source_columns.clone());
            }
            for (_, assignment) in &clause.assignments {
                if let MergeAssignment::SourceColumn(name) = assignment {
                    projection.insert(name.clone());
                }
            }
        }
    }
    let projection = projection.into_iter().collect::<Vec<_>>();
    let mut builder = scan_table.new_read_builder();
    builder.with_projection(&projection.iter().map(String::as_str).collect::<Vec<_>>())?;
    let plan = if snapshot.is_some() {
        builder.new_scan().with_scan_all_files().plan().await?
    } else {
        super::source::Plan::new(Vec::new())
    };
    let index = RowIdFileIndex::from_splits(scan_table.clone(), plan.splits())?;
    let source = match source {
        MergeSource::Batches(batches) => {
            let first = batches
                .first()
                .ok_or_else(|| invalid("MERGE source needs an Arrow schema"))?;
            if batches.iter().any(|batch| batch.schema() != first.schema()) {
                return Err(invalid("MERGE source batches must share a schema"));
            }
            let names = first
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect::<HashSet<_>>();
            if names.len() != first.num_columns() {
                return Err(invalid("MERGE source has duplicate column names"));
            }
            Some(
                concat_batches(&first.schema(), &batches)
                    .map_err(|error| invalid(error.to_string()))?,
            )
        }
        MergeSource::SelfTable => None,
    };
    let source_schema = source
        .as_ref()
        .map_or_else(|| schema.clone(), RecordBatch::schema);
    for (assignments, condition, insert) in matched
        .iter()
        .map(|clause| (&clause.assignments, &clause.condition, false))
        .chain(
            not_matched
                .iter()
                .map(|clause| (&clause.assignments, &clause.condition, true)),
        )
    {
        let mut seen = HashSet::new();
        for (name, value) in assignments {
            if !seen.insert(name) {
                return Err(invalid(format!("Duplicate MERGE assignment {name}")));
            }
            schema
                .field_with_name(name)
                .map_err(|error| invalid(error.to_string()))?;
            match value {
                MergeAssignment::SourceColumn(name) => {
                    if !(self_merge && name == ROW_ID) {
                        source_schema
                            .field_with_name(name)
                            .map_err(|error| invalid(error.to_string()))?;
                    }
                }
                MergeAssignment::TargetColumn(name) => {
                    if insert {
                        return Err(invalid("INSERT cannot reference target columns"));
                    }
                    if name != ROW_ID {
                        schema
                            .field_with_name(name)
                            .map_err(|error| invalid(error.to_string()))?;
                    }
                }
                MergeAssignment::Value(_) => {}
            }
        }
        if let Some(condition) = condition {
            for name in &condition.target_columns {
                if name != ROW_ID {
                    schema
                        .field_with_name(name)
                        .map_err(|error| invalid(error.to_string()))?;
                }
            }
        }
    }
    if matched.iter().any(|clause| clause.delete)
        && !crate::spec::CoreOptions::new(table.schema().options()).deletion_vectors_enabled()
    {
        return Err(invalid(
            "MERGE DELETE requires deletion-vectors.enabled=true",
        ));
    }
    let mut converter = None;
    let mut source_keys = HashMap::<Vec<u8>, Vec<usize>>::new();
    let mut seen_source = vec![false; source.as_ref().map_or(0, RecordBatch::num_rows)];
    if let Some(source) = &source {
        let keys = on
            .iter()
            .map(|(target, source_name)| {
                let source_column = source
                    .column_by_name(source_name)
                    .ok_or_else(|| invalid(format!("Missing source ON key {source_name}")))?;
                let target_type = if target == ROW_ID {
                    &DataType::Int64
                } else {
                    schema
                        .field_with_name(target)
                        .map_err(|error| invalid(error.to_string()))?
                        .data_type()
                };
                if source_column.data_type() != target_type
                    || !super::upsert_key_matcher::supported_key_type(target_type)
                {
                    return Err(invalid(
                        "MERGE ON key types must match and support row encoding",
                    ));
                }
                Ok(source_column.clone())
            })
            .collect::<crate::Result<Vec<_>>>()?;
        let key_converter = RowConverter::new(
            keys.iter()
                .map(|column| SortField::new(column.data_type().clone()))
                .collect(),
        )
        .map_err(|error| invalid(error.to_string()))?;
        let rows = key_converter
            .convert_columns(&keys)
            .map_err(|error| invalid(error.to_string()))?;
        for row in 0..source.num_rows() {
            if keys.iter().all(|column| !column.is_null(row)) {
                source_keys
                    .entry(rows.row(row).as_ref().to_vec())
                    .or_default()
                    .push(row);
            }
        }
        converter = Some(key_converter);
    }
    // Spark validates cardinality before evaluating action predicates. A sole
    // unconditional DELETE may match multiple source rows without ambiguity.
    let check_cardinality = !matched.is_empty()
        && !(matched.len() == 1 && matched[0].delete && matched[0].condition.is_none());
    let mut joined = Vec::new();
    let mut stream = builder.new_read()?.to_arrow(plan.splits())?;
    while let Some(target) = stream.try_next().await? {
        if let Some(source) = &source {
            let keys = on
                .iter()
                .map(|(name, _)| target.column_by_name(name).unwrap().clone())
                .collect::<Vec<_>>();
            let rows = converter
                .as_ref()
                .unwrap()
                .convert_columns(&keys)
                .map_err(|error| invalid(error.to_string()))?;
            let mut target_rows = Vec::new();
            let mut source_rows = Vec::new();
            for row in 0..target.num_rows() {
                if keys.iter().any(|column| column.is_null(row)) {
                    continue;
                }
                if let Some(sources) = source_keys.get(rows.row(row).as_ref()) {
                    if check_cardinality && sources.len() > 1 {
                        return Err(invalid(
                            "MERGE matched multiple source rows to the same target _ROW_ID",
                        ));
                    }
                    for &source in sources {
                        seen_source[source] = true;
                    }
                    target_rows.push(row);
                    source_rows.push(sources[0]);
                }
            }
            if !target_rows.is_empty() {
                joined.push(aliases(
                    Some(&gather(&target, &target_rows)?),
                    &gather(source, &source_rows)?,
                )?);
            }
        } else if target.num_rows() > 0 {
            joined.push(aliases(Some(&target), &target)?);
        }
    }
    let mut updates = Vec::new();
    let mut deletes = Vec::new();
    let mut pending = joined;
    for clause in matched {
        let mut remaining = Vec::new();
        let mut clause_rows = Vec::new();
        for batch in pending {
            let (selected, rest) = select_clause(batch, &clause.condition).await?;
            if rest.num_rows() > 0 {
                remaining.push(rest);
            }
            if selected.num_rows() == 0 {
                continue;
            }
            if clause.delete {
                let ids = selected
                    .column_by_name(ROW_ID)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                deletes.extend(ids.values().iter().copied());
            } else if !update_columns.is_empty() {
                clause_rows.push(selected);
            }
        }
        // Array/function assignments cover the complete clause selection, not
        // each physical target batch. Shared assignment cursors consume chunks
        // across these batches while preserving row IDs and scan order.
        if !clause_rows.is_empty() {
            updates.extend(assigned(
                &clause_rows,
                &clause.assignments,
                &update_columns,
                schema.clone(),
                false,
            )?);
        }
        pending = remaining;
    }
    let mut inserts = Vec::new();
    if let Some(source) = source {
        let unmatched = seen_source
            .iter()
            .enumerate()
            .filter_map(|(row, seen)| (!seen).then_some(row))
            .collect::<Vec<_>>();
        let mut pending = aliases(None, &gather(&source, &unmatched)?)?;
        let columns = schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        for clause in not_matched {
            if pending.num_rows() == 0 {
                break;
            }
            let (selected, rest) = select_clause(pending, &clause.condition).await?;
            pending = rest;
            if selected.num_rows() > 0 {
                for batch in assigned(
                    std::slice::from_ref(&selected),
                    &clause.assignments,
                    &columns,
                    schema.clone(),
                    true,
                )? {
                    inserts.push(
                        batch
                            .project(&(1..batch.num_columns()).collect::<Vec<_>>())
                            .map_err(|error| invalid(error.to_string()))?,
                    );
                }
            }
        }
    }
    // Materialize/validate every action before staging. Once prepared, messages
    // may already be referenced by publication; a later error must retain their files.
    let mut messages = Vec::new();
    if !updates.is_empty() {
        let mut updater = TableUpdateByRowId::with_index(table, index)?;
        messages.extend(updater.update_columns(updates, update_columns).await?);
    }
    if !deletes.is_empty() {
        let mut writer = super::DataEvolutionDeleteWriter::new(&scan_table)?;
        writer.add_row_ids(deletes)?;
        messages.extend(writer.prepare_commit().await?);
    }
    if !inserts.is_empty() {
        let mut writer = table
            .new_write_builder()
            .with_commit_user(commit_user.to_string())?
            .new_write()?;
        let result = async {
            for batch in inserts {
                writer.write_arrow_batch(&batch).await?;
            }
            writer.prepare_commit().await
        }
        .await;
        writer.close().await;
        messages.extend(result?);
    }
    Ok(messages)
}
