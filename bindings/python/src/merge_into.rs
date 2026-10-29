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

//! Python MERGE descriptions and DataFusion expression compilation for core MERGE.

use std::sync::{Arc, Mutex};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use paimon::table::{MergeAssignment, MergeCondition, WhenMatched, WhenNotMatched};
use paimon_datafusion::runtime::runtime;
use pyo3::prelude::*;

use crate::error::to_py_err;
use crate::merge_types::{AssignmentValue, Assignments, PyWhenMatched, PyWhenNotMatched};

type CallbackError = Arc<Mutex<Option<PyErr>>>;

pub(crate) fn condition(
    py: Python<'_>,
    sql: Option<&str>,
    schema: SchemaRef,
) -> PyResult<Option<MergeCondition>> {
    sql.map(|sql| {
        py.detach(|| {
            runtime().block_on(paimon_datafusion::compile_merge_condition(
                sql.to_owned(),
                schema,
            ))
        })
        .map_err(to_py_err)
    })
    .transpose()
}

pub(crate) fn assignments(
    py: Python<'_>,
    values: &Assignments,
    schema: SchemaRef,
    errors: CallbackError,
) -> PyResult<Vec<(String, MergeAssignment)>> {
    values
        .iter()
        .map(|(name, value)| {
            let value = value.borrow(py);
            let assignment = match &value.value {
                AssignmentValue::SourceColumn(column) => {
                    MergeAssignment::SourceColumn(column.clone())
                }
                AssignmentValue::TargetColumn(column) => {
                    MergeAssignment::TargetColumn(column.clone())
                }
                AssignmentValue::Literal(value) => {
                    MergeAssignment::Value(crate::update_assignment::literal(
                        py,
                        name.clone(),
                        value.clone_ref(py),
                        schema.clone(),
                        errors.clone(),
                    )?)
                }
                AssignmentValue::Function(value) => MergeAssignment::Value(
                    crate::update_assignment::function(value.clone_ref(py), errors.clone()),
                ),
            };
            Ok((name.clone(), assignment))
        })
        .collect()
}

pub(crate) fn matched(
    py: Python<'_>,
    clauses: &Bound<'_, PyAny>,
    condition_schema: SchemaRef,
    target_schema: SchemaRef,
    errors: CallbackError,
    include_assignments: bool,
) -> PyResult<Vec<WhenMatched>> {
    clauses
        .try_iter()?
        .map(|item| {
            let item = item?;
            let clause = item.extract::<PyRef<PyWhenMatched>>()?;
            Ok(WhenMatched {
                condition: condition(py, clause.condition.as_deref(), condition_schema.clone())?,
                delete: clause.delete,
                assignments: if include_assignments {
                    assignments(
                        py,
                        &clause.assignments,
                        target_schema.clone(),
                        errors.clone(),
                    )?
                } else {
                    // DELETE output consumes preceding UPDATE conditions without
                    // inspecting values that this route will never materialize.
                    Vec::new()
                },
            })
        })
        .collect()
}

pub(crate) fn not_matched(
    py: Python<'_>,
    clauses: &Bound<'_, PyAny>,
    condition_schema: SchemaRef,
    target_schema: SchemaRef,
    errors: CallbackError,
) -> PyResult<Vec<WhenNotMatched>> {
    clauses
        .try_iter()?
        .map(|item| {
            let item = item?;
            let clause = item.extract::<PyRef<PyWhenNotMatched>>()?;
            Ok(WhenNotMatched {
                condition: condition(py, clause.condition.as_deref(), condition_schema.clone())?,
                assignments: assignments(
                    py,
                    &clause.assignments,
                    target_schema.clone(),
                    errors.clone(),
                )?,
            })
        })
        .collect()
}

pub(crate) fn from_python(
    py: Python<'_>,
    table: &paimon::table::Table,
    source_schema: SchemaRef,
    when_matched: &Bound<'_, PyAny>,
    when_not_matched: &Bound<'_, PyAny>,
    errors: CallbackError,
) -> PyResult<(Vec<WhenMatched>, Vec<WhenNotMatched>)> {
    let target_schema =
        paimon::arrow::build_target_arrow_schema(table.schema().fields()).map_err(to_py_err)?;
    let mut fields = vec![Field::new("t._ROW_ID", DataType::Int64, false)];
    fields.extend(target_schema.fields().iter().map(|field| {
        field
            .as_ref()
            .clone()
            .with_name(format!("t.{}", field.name()))
    }));
    fields.extend(source_schema.fields().iter().map(|field| {
        field
            .as_ref()
            .clone()
            .with_name(format!("s.{}", field.name()))
    }));
    let matched = matched(
        py,
        when_matched,
        Arc::new(Schema::new(fields)),
        target_schema.clone(),
        errors.clone(),
        true,
    )?;
    let schema = Arc::new(Schema::new(
        source_schema
            .fields()
            .iter()
            .map(|field| {
                field
                    .as_ref()
                    .clone()
                    .with_name(format!("s.{}", field.name()))
            })
            .collect::<Vec<_>>(),
    ));
    let not_matched = not_matched(py, when_not_matched, schema, target_schema, errors)?;
    Ok((matched, not_matched))
}
