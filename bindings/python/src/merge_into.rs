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

//! Language transport and DataFusion expression compilation for core MERGE.

use std::sync::{Arc, Mutex};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use paimon::table::{MergeAssignment, MergeCondition, WhenMatched, WhenNotMatched};
use paimon_datafusion::runtime::runtime;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::error::to_py_err;

type CallbackError = Arc<Mutex<Option<PyErr>>>;

fn condition(
    py: Python<'_>,
    clause: &Bound<'_, PyDict>,
    schema: SchemaRef,
) -> PyResult<Option<MergeCondition>> {
    let Some(value) = clause
        .get_item("condition")?
        .filter(|value| !value.is_none())
    else {
        return Ok(None);
    };
    let value = value.cast::<PyDict>()?;
    let sql: String = value
        .get_item("sql")?
        .ok_or_else(|| PyValueError::new_err("MERGE condition needs SQL"))?
        .extract()?;
    py.detach(|| runtime().block_on(paimon_datafusion::compile_merge_condition(sql, schema)))
        .map(Some)
        .map_err(to_py_err)
}

fn assignments(
    py: Python<'_>,
    clause: &Bound<'_, PyDict>,
    fields: &[paimon::spec::DataField],
    errors: CallbackError,
) -> PyResult<Vec<(String, MergeAssignment)>> {
    let values = clause
        .get_item("assignments")?
        .ok_or_else(|| PyValueError::new_err("MERGE clause needs assignments"))?;
    values
        .try_iter()?
        .map(|item| {
            let (name, kind, value): (String, String, Py<PyAny>) = item?.extract()?;
            let assignment = match kind.as_str() {
                "source" => MergeAssignment::SourceColumn(value.bind(py).extract()?),
                "target" => MergeAssignment::TargetColumn(value.bind(py).extract()?),
                "literal" => {
                    let values = PyDict::new(py);
                    values.set_item(&name, value)?;
                    let assignment =
                        crate::update_assignment::from_python(py, &values, fields, errors.clone())?
                            .remove(0)
                            .1;
                    MergeAssignment::Value(assignment)
                }
                _ => return Err(PyValueError::new_err("Unknown MERGE assignment kind")),
            };
            Ok((name, assignment))
        })
        .collect()
}

pub(crate) fn from_python(
    py: Python<'_>,
    table: &paimon::table::Table,
    source_schema: SchemaRef,
    matched: &Bound<'_, PyAny>,
    not_matched: &Bound<'_, PyAny>,
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
    let schema = Arc::new(Schema::new(fields));
    let matched = matched
        .try_iter()?
        .map(|item| {
            let item = item?;
            let clause = item.cast::<PyDict>()?;
            Ok(WhenMatched {
                condition: condition(py, clause, schema.clone())?,
                delete: clause
                    .get_item("delete")?
                    .ok_or_else(|| PyValueError::new_err("MERGE matched clause needs action"))?
                    .extract()?,
                assignments: assignments(py, clause, table.schema().fields(), errors.clone())?,
            })
        })
        .collect::<PyResult<Vec<_>>>()?;
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
    let not_matched = not_matched
        .try_iter()?
        .map(|item| {
            let item = item?;
            let clause = item.cast::<PyDict>()?;
            Ok(WhenNotMatched {
                condition: condition(py, clause, schema.clone())?,
                assignments: assignments(py, clause, table.schema().fields(), errors.clone())?,
            })
        })
        .collect::<PyResult<Vec<_>>>()?;
    Ok((matched, not_matched))
}
