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

//! Arrow and Python transport for the core prejoined MERGE evaluator.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow::record_batch::RecordBatch;
use paimon::table::{
    evaluate_merge_actions, MergeActionOutput, MergeAssignment, WhenMatched, WhenNotMatched,
};
use paimon_datafusion::runtime::runtime;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::error::to_py_err;

#[pyclass(name = "MergeBatchExecution")]
pub struct PyMergeBatchExecution {
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    target_schema: SchemaRef,
    matched: Vec<WhenMatched>,
    not_matched: Vec<WhenNotMatched>,
    output: MergeActionOutput,
    callback_error: Arc<Mutex<Option<PyErr>>>,
    execution: Mutex<()>,
}

fn matched_schema(schema: SchemaRef) -> PyResult<SchemaRef> {
    if schema.column_with_name("_ROW_ID").is_some() {
        if schema.column_with_name("t._ROW_ID").is_some() {
            return Err(PyValueError::new_err(
                "Matched MERGE input has ambiguous _ROW_ID and t._ROW_ID fields",
            ));
        }
        return Ok(schema);
    }
    let index = schema.index_of("t._ROW_ID").map_err(|error| {
        PyValueError::new_err(format!("Matched MERGE batch needs t._ROW_ID: {error}"))
    })?;
    let mut fields = schema.fields().to_vec();
    fields.push(Arc::new(
        fields[index].as_ref().clone().with_name("_ROW_ID"),
    ));
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

fn row_ids(batch: RecordBatch) -> PyResult<RecordBatch> {
    let schema = matched_schema(batch.schema())?;
    if schema.fields().len() == batch.num_columns() {
        return Ok(batch);
    }
    let index = batch.schema().index_of("t._ROW_ID").unwrap();
    let mut columns = batch.columns().to_vec();
    columns.push(batch.column(index).clone());
    RecordBatch::try_new(schema, columns).map_err(|error| PyValueError::new_err(error.to_string()))
}

#[pymethods]
impl PyMergeBatchExecution {
    #[new]
    fn new(
        py: Python<'_>,
        input_schema: &Bound<'_, PyAny>,
        output_schema: &Bound<'_, PyAny>,
        clauses: &Bound<'_, PyAny>,
        mode: &str,
    ) -> PyResult<Self> {
        let output = match mode {
            "update" => MergeActionOutput::Updates,
            "delete" => MergeActionOutput::Deletes,
            "insert" => MergeActionOutput::Inserts,
            _ => {
                return Err(PyValueError::new_err(
                    "MERGE execution mode must be update, delete or insert",
                ));
            }
        };
        let input_schema = Arc::new(Schema::from_pyarrow_bound(input_schema)?);
        let output_schema = Arc::new(Schema::from_pyarrow_bound(output_schema)?);
        if output != MergeActionOutput::Inserts {
            let row_id = output_schema.field_with_name("_ROW_ID").map_err(|_| {
                PyValueError::new_err("Matched MERGE output needs an Int64 _ROW_ID field")
            })?;
            if row_id.data_type() != &DataType::Int64 {
                return Err(PyValueError::new_err(
                    "Matched MERGE output needs an Int64 _ROW_ID field",
                ));
            }
            if output == MergeActionOutput::Deletes && output_schema.fields().len() != 1 {
                return Err(PyValueError::new_err(
                    "MERGE DELETE output must contain only _ROW_ID",
                ));
            }
        }
        let condition_schema = if output == MergeActionOutput::Inserts {
            input_schema.clone()
        } else {
            matched_schema(input_schema.clone())?
        };
        let target_schema = Arc::new(Schema::new(
            output_schema
                .fields()
                .iter()
                .filter(|field| output == MergeActionOutput::Inserts || field.name() != "_ROW_ID")
                .cloned()
                .collect::<Vec<_>>(),
        ));
        let callback_error = Arc::new(Mutex::new(None));
        let mut matched = Vec::new();
        let mut not_matched = Vec::new();
        for clause in clauses.try_iter()? {
            let clause = clause?;
            let clause = clause.cast::<PyDict>()?;
            let condition = crate::merge_into::condition(py, clause, condition_schema.clone())?;
            let assignments = if output == MergeActionOutput::Deletes {
                // A DELETE route still consumes preceding UPDATE conditions,
                // but must not inspect or evaluate those assignments.
                Vec::new()
            } else {
                crate::merge_into::assignments(
                    py,
                    clause,
                    target_schema.clone(),
                    callback_error.clone(),
                )?
            };
            if output == MergeActionOutput::Inserts {
                not_matched.push(WhenNotMatched {
                    condition,
                    assignments,
                });
            } else {
                matched.push(WhenMatched {
                    condition,
                    delete: clause
                        .get_item("delete")?
                        .ok_or_else(|| PyValueError::new_err("MERGE matched clause needs action"))?
                        .extract()?,
                    assignments,
                });
            }
        }
        if output == MergeActionOutput::Updates {
            // The explicit output schema may request fields untouched by every
            // clause. Include them in core's shared update-column projection.
            let assigned = matched
                .iter()
                .flat_map(|clause| clause.assignments.iter().map(|(name, _)| name.clone()))
                .collect::<HashSet<_>>();
            if let Some(clause) = matched.iter_mut().find(|clause| !clause.delete) {
                for field in target_schema.fields() {
                    if !assigned.contains(field.name()) {
                        clause.assignments.push((
                            field.name().clone(),
                            MergeAssignment::TargetColumn(field.name().clone()),
                        ));
                    }
                }
            }
        }
        // Reuse core's clause and assignment validation before a worker starts
        // consuming data. Empty input cannot evaluate lazy assignment values.
        py.detach(|| {
            runtime().block_on(evaluate_merge_actions(
                target_schema.clone(),
                Vec::new(),
                Vec::new(),
                &matched,
                &not_matched,
                output,
            ))
        })
        .map_err(to_py_err)?;
        Ok(Self {
            input_schema,
            output_schema,
            target_schema,
            matched,
            not_matched,
            output,
            callback_error,
            execution: Mutex::new(()),
        })
    }

    fn execute(&self, py: Python<'_>, batch: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let schema = Schema::from_pyarrow_bound(&batch.getattr("schema")?)?;
        if &schema != self.input_schema.as_ref() {
            return Err(PyValueError::new_err(
                "MERGE batch schema differs from the compiled input schema",
            ));
        }
        let pa = py.import("pyarrow")?;
        let mut batches = if batch.is_instance(&pa.getattr("RecordBatch")?)? {
            vec![crate::arrow_input::batch_from_pyarrow(batch)?]
        } else {
            batch
                .call_method0("to_batches")?
                .try_iter()?
                .map(|batch| crate::arrow_input::batch_from_pyarrow(&batch?))
                .collect::<PyResult<Vec<_>>>()?
        };
        if batches.is_empty() {
            batches.push(RecordBatch::new_empty(self.input_schema.clone()));
        }
        let (matched, unmatched) = if self.output == MergeActionOutput::Inserts {
            (Vec::new(), batches)
        } else {
            (
                batches.into_iter().map(row_ids).collect::<PyResult<_>>()?,
                Vec::new(),
            )
        };
        let (result, callback_error) = py.detach(|| {
            // Cached operators may be called by multiple Python threads. Take
            // this lock without the GIL so callbacks retain their error owner.
            let _execution = self.execution.lock().unwrap();
            let result = runtime().block_on(evaluate_merge_actions(
                self.target_schema.clone(),
                matched,
                unmatched,
                &self.matched,
                &self.not_matched,
                self.output,
            ));
            (result, self.callback_error.lock().unwrap().take())
        });
        if let Some(error) = callback_error {
            return Err(error);
        }
        let actions = result.map_err(to_py_err)?;
        let batches = match self.output {
            MergeActionOutput::Updates => actions.updates,
            MergeActionOutput::Inserts => actions.inserts,
            MergeActionOutput::Deletes => vec![RecordBatch::try_new(
                self.output_schema.clone(),
                vec![Arc::new(Int64Array::from(actions.deletes))],
            )
            .map_err(|error| PyValueError::new_err(error.to_string()))?],
            MergeActionOutput::All => unreachable!(),
        };
        let batches = batches
            .iter()
            .map(|batch| {
                let columns = self
                    .output_schema
                    .fields()
                    .iter()
                    .map(|field| {
                        batch.column_by_name(field.name()).cloned().ok_or_else(|| {
                            PyValueError::new_err(format!(
                                "Missing MERGE output column {}",
                                field.name()
                            ))
                        })
                    })
                    .collect::<PyResult<_>>()?;
                RecordBatch::try_new(self.output_schema.clone(), columns)
                    .map_err(|error| PyValueError::new_err(error.to_string()))?
                    .to_pyarrow(py)
            })
            .collect::<PyResult<Vec<_>>>()?;
        Ok(py
            .import("pyarrow")?
            .getattr("Table")?
            .call_method1(
                "from_batches",
                (
                    PyList::new(py, batches)?,
                    self.output_schema.to_pyarrow(py)?,
                ),
            )?
            .unbind())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::ffi::c_str;

    fn locals(py: Python<'_>) -> Bound<'_, PyDict> {
        let locals = PyDict::new(py);
        locals
            .set_item(
                "MergeBatchExecution",
                py.get_type::<PyMergeBatchExecution>(),
            )
            .unwrap();
        locals
    }

    #[test]
    fn ordered_matched_actions_keep_schema_and_callback_grouping() {
        Python::attach(|py| {
            let locals = locals(py);
            py.run(
                c_str!(
                    r#"
import pyarrow as pa
input_schema = pa.schema([
    pa.field('t._ROW_ID', pa.int64(), nullable=False),
    pa.field('t.value', pa.int32()),
    pa.field('s.value', pa.int32()),
    pa.field('t.keep', pa.string()),
    pa.field('t.unassigned', pa.int32()),
], metadata={'input': 'joined'})
first = pa.record_batch([
    pa.array([1, 2, 3], pa.int64()), pa.array([1, 2, 3], pa.int32()),
    pa.array([15, None, 99], pa.int32()), pa.array(['one', 'two', 'three']),
    pa.array([101, 102, 103], pa.int32()),
], schema=input_schema)
second = pa.record_batch([
    pa.array([4, 5], pa.int64()), pa.array([4, 5], pa.int32()),
    pa.array([20, 5], pa.int32()), pa.array(['four', 'five']),
    pa.array([104, 105], pa.int32()),
], schema=input_schema)
joined = pa.Table.from_batches([first, second])
calls = []
def update(rows):
    calls.append(rows['t._ROW_ID'].to_pylist())
    assert rows['s.value'].to_pylist() == [15, 20]
    return pa.chunked_array([pa.array([30]), pa.array([40])])
clauses = [
    dict(condition={'sql': '"s.value" >= 90'}, delete=True, assignments=[]),
    dict(condition={'sql': '"s.value" > 10'}, delete=False,
         assignments=[('value', 'callable', update), ('keep', 'target', 'keep')]),
    dict(condition=None, delete=False, assignments=[('value', 'literal', 50)]),
]
output_schema = pa.schema([
    pa.field('_ROW_ID', pa.int64(), nullable=False),
    pa.field('keep', pa.string(), metadata={'field': 'kept'}),
    pa.field('unassigned', pa.int32()),
    pa.field('value', pa.int32()),
], metadata={'output': 'updates'})
execution = MergeBatchExecution(input_schema, output_schema, clauses, 'update')
result = execution.execute(joined)
assert result.schema.equals(output_schema, check_metadata=True)
assert result.to_pydict() == {
    '_ROW_ID': [1, 4, 2, 5], 'keep': ['one', 'four', 'two', 'five'],
    'unassigned': [101, 104, 102, 105],
    'value': [30, 40, 50, 50],
}
assert calls == [[1, 4]]
delete_schema = pa.schema([pa.field('_ROW_ID', pa.int64(), nullable=False)])
deletes = MergeBatchExecution(input_schema, delete_schema, clauses, 'delete').execute(joined)
assert deletes.schema == delete_schema
assert deletes.to_pydict() == {'_ROW_ID': [3]}
assert calls == [[1, 4]]
assert execution.execute(input_schema.empty_table()).schema.equals(output_schema, check_metadata=True)
assert calls == [[1, 4]]
"#
                ),
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
        });
    }

    #[test]
    fn ordered_inserts_preserve_typed_nulls_and_evaluate_selected_rows_once() {
        Python::attach(|py| {
            let locals = locals(py);
            py.run(
                c_str!(
                    r#"
import pyarrow as pa
source = pa.table({'s.value': pa.array([None, 20, 5], pa.int32()),
                   's.id': pa.array([3, 1, 2], pa.int32())})
source = pa.Table.from_batches(source.to_batches(max_chunksize=1))
target = pa.schema([pa.field('label', pa.string()), pa.field('id', pa.int32()),
                    pa.field('value', pa.int32()), pa.field('missing', pa.bool_())])
calls = []
def insert(rows):
    calls.append(rows['s.id'].to_pylist())
    return pa.array([21], pa.int32())
clauses = [
    dict(condition={'sql': '"s.value" > 10'},
         assignments=[('id', 'source', 'id'), ('value', 'callable', insert),
                      ('label', 'literal', 'first')]),
    dict(condition=None,
         assignments=[('id', 'source', 'id'), ('value', 'literal', 7),
                      ('label', 'literal', 'last'), ('missing', 'literal', True)]),
]
execution = MergeBatchExecution(source.schema, target, clauses, 'insert')
result = execution.execute(source)
assert result.schema == target
assert result.to_pydict() == {'label': ['first', 'last', 'last'], 'id': [1, 3, 2],
                            'value': [21, 7, 7], 'missing': [None, True, True]}
assert calls == [[1]]
assert execution.execute(source.slice(0, 0)).schema == target
assert calls == [[1]]
"#
                ),
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
        });
    }

    #[test]
    fn callback_errors_keep_identity_and_do_not_poison_cached_execution() {
        Python::attach(|py| {
            let locals = locals(py);
            py.run(
                c_str!(
                    r#"
import pyarrow as pa
joined = pa.table({'t._ROW_ID': pa.array([7], pa.int64()),
                   's.value': pa.array([1], pa.int32())})
output = pa.schema([pa.field('_ROW_ID', pa.int64(), nullable=False),
                    pa.field('value', pa.int32())])
failure = LookupError('assignment failed')
attempts = []
def assignment(rows):
    attempts.append(rows.num_rows)
    if len(attempts) == 1:
        raise failure
    return pa.array([9], pa.int32())
clauses = [dict(condition=None, delete=False,
                assignments=[('value', 'callable', assignment)])]
execution = MergeBatchExecution(joined.schema, output, clauses, 'update')
try:
    execution.execute(joined)
except LookupError as error:
    assert error is failure
else:
    raise AssertionError('callback did not raise')
assert execution.execute(joined).to_pydict() == {'_ROW_ID': [7], 'value': [9]}
assert attempts == [1, 1]
assert execution.execute(joined.to_batches()[0]).to_pydict() == {'_ROW_ID': [7], 'value': [9]}
assert attempts == [1, 1, 1]
"#
                ),
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
        });
    }

    #[test]
    fn invalid_conditions_lengths_and_schema_changes_fail_before_output() {
        Python::attach(|py| {
            let locals = locals(py);
            py.run(
                c_str!(
                    r#"
import pyarrow as pa
joined = pa.table({'t._ROW_ID': pa.array([1, 2], pa.int64()),
                   's.value': pa.array([2, 3], pa.int32())})
output = pa.schema([pa.field('_ROW_ID', pa.int64(), nullable=False),
                    pa.field('value', pa.int32())])
def rejected(function, text):
    try:
        function()
    except ValueError as error:
        assert text in str(error), str(error)
    else:
        raise AssertionError('invalid MERGE operation succeeded')
clause = dict(condition={'sql': '"s.absent" > 0'}, delete=False,
              assignments=[('value', 'literal', 1)])
rejected(lambda: MergeBatchExecution(joined.schema, output, [clause], 'update'), 'absent')
clause = dict(condition=None, delete=False,
              assignments=[('value', 'callable', lambda rows: pa.array([1], pa.int32()))])
execution = MergeBatchExecution(joined.schema, output, [clause], 'update')
rejected(lambda: execution.execute(joined), 'length')
rejected(lambda: execution.execute(joined.rename_columns(['t._ROW_ID', 's.other'])), 'schema')
rejected(lambda: MergeBatchExecution(joined.schema, output, [clause], 'unknown'), 'mode')
rejected(lambda: MergeBatchExecution(joined.schema, pa.schema([('value', pa.int32())]),
                                    [clause], 'update'), '_ROW_ID')
rejected(lambda: MergeBatchExecution(joined.schema, pa.schema([('_ROW_ID', pa.int32())]),
                                    [clause], 'delete'), 'Int64')
rejected(lambda: MergeBatchExecution(joined.schema, output, [clause], 'delete'), 'only')
rejected(lambda: MergeBatchExecution(joined.schema, output, [clause, clause], 'update'), 'last')
dual_ids = joined.append_column('_ROW_ID', pa.array([99, 98], pa.int64()))
rejected(lambda: MergeBatchExecution(dual_ids.schema, output, [clause], 'update'), 'ambiguous')
bad_type = dict(condition=None, delete=False,
                assignments=[('value', 'callable', lambda rows: [1, 2])])
rejected(lambda: MergeBatchExecution(joined.schema, output, [bad_type], 'update').execute(joined),
         'pyarrow.Array')
# A no-match clause must not convert an unused literal or invoke a callback.
clause = dict(condition={'sql': '"s.value" < 0'}, delete=False,
              assignments=[('value', 'literal', object())])
assert MergeBatchExecution(joined.schema, output, [clause], 'update').execute(joined).num_rows == 0
"#
                ),
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
        });
    }
}
