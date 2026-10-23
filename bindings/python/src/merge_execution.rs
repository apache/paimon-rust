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
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow::record_batch::RecordBatch;
use paimon::table::{
    evaluate_merge_actions, MergeActionOutput, MergeAssignment, WhenMatched, WhenNotMatched,
};
use paimon_datafusion::runtime::runtime;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyList;

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

impl PyMergeBatchExecution {
    fn compile(
        py: Python<'_>,
        input_schema: SchemaRef,
        output_schema: SchemaRef,
        clauses: &Bound<'_, PyAny>,
        output: MergeActionOutput,
    ) -> PyResult<Self> {
        if output != MergeActionOutput::Inserts {
            let row_id = output_schema.field_with_name("_ROW_ID").map_err(|_| {
                PyValueError::new_err("Matched MERGE output needs an Int64 _ROW_ID field")
            })?;
            if row_id.data_type() != &DataType::Int64 {
                return Err(PyValueError::new_err(
                    "Matched MERGE output needs an Int64 _ROW_ID field",
                ));
            }
        }
        let condition_schema = if output == MergeActionOutput::Inserts {
            input_schema.clone()
        } else {
            matched_schema(input_schema.clone())?
        };
        let target_schema = Arc::new(Schema::new_with_metadata(
            output_schema
                .fields()
                .iter()
                .filter(|field| output == MergeActionOutput::Inserts || field.name() != "_ROW_ID")
                .cloned()
                .collect::<Vec<_>>(),
            output_schema.metadata().clone(),
        ));
        let callback_error = Arc::new(Mutex::new(None));
        let (mut matched, not_matched) = if output == MergeActionOutput::Inserts {
            (
                Vec::new(),
                crate::merge_into::not_matched(
                    py,
                    clauses,
                    condition_schema,
                    target_schema.clone(),
                    callback_error.clone(),
                )?,
            )
        } else {
            (
                crate::merge_into::matched(
                    py,
                    clauses,
                    condition_schema,
                    target_schema.clone(),
                    callback_error.clone(),
                    output != MergeActionOutput::Deletes,
                )?,
                Vec::new(),
            )
        };
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
}

#[pymethods]
impl PyMergeBatchExecution {
    #[staticmethod]
    fn for_updates(
        py: Python<'_>,
        input_schema: &Bound<'_, PyAny>,
        update_schema: &Bound<'_, PyAny>,
        when_matched: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        Self::compile(
            py,
            Arc::new(Schema::from_pyarrow_bound(input_schema)?),
            Arc::new(Schema::from_pyarrow_bound(update_schema)?),
            when_matched,
            MergeActionOutput::Updates,
        )
    }

    #[staticmethod]
    fn for_deletes(
        py: Python<'_>,
        input_schema: &Bound<'_, PyAny>,
        when_matched: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let input_schema = Arc::new(Schema::from_pyarrow_bound(input_schema)?);
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "_ROW_ID",
            DataType::Int64,
            false,
        )]));
        Self::compile(
            py,
            input_schema,
            output_schema,
            when_matched,
            MergeActionOutput::Deletes,
        )
    }

    #[staticmethod]
    fn for_inserts(
        py: Python<'_>,
        input_schema: &Bound<'_, PyAny>,
        target_schema: &Bound<'_, PyAny>,
        when_not_matched: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        Self::compile(
            py,
            Arc::new(Schema::from_pyarrow_bound(input_schema)?),
            Arc::new(Schema::from_pyarrow_bound(target_schema)?),
            when_not_matched,
            MergeActionOutput::Inserts,
        )
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
    use pyo3::types::PyDict;

    fn locals(py: Python<'_>) -> Bound<'_, PyDict> {
        let locals = PyDict::new(py);
        locals
            .set_item(
                "MergeBatchExecution",
                py.get_type::<PyMergeBatchExecution>(),
            )
            .unwrap();
        locals
            .set_item(
                "MergeAssignment",
                py.get_type::<crate::merge_types::PyMergeAssignment>(),
            )
            .unwrap();
        locals
            .set_item(
                "WhenMatched",
                py.get_type::<crate::merge_types::PyWhenMatched>(),
            )
            .unwrap();
        locals
            .set_item(
                "WhenNotMatched",
                py.get_type::<crate::merge_types::PyWhenNotMatched>(),
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
    WhenMatched.delete(condition='"s.value" >= 90'),
    WhenMatched.update({'value': MergeAssignment.function(update),
                        'keep': MergeAssignment.target_column('keep')},
                       condition='"s.value" > 10'),
    WhenMatched.update({'value': MergeAssignment.literal(50)}),
]
output_schema = pa.schema([
    pa.field('_ROW_ID', pa.int64(), nullable=False),
    pa.field('keep', pa.string(), metadata={'field': 'kept'}),
    pa.field('unassigned', pa.int32()),
    pa.field('value', pa.int32()),
], metadata={'output': 'updates'})
execution = MergeBatchExecution.for_updates(input_schema, output_schema, clauses)
result = execution.execute(joined)
assert result.schema.equals(output_schema, check_metadata=True)
assert result.to_pydict() == {
    '_ROW_ID': [1, 4, 2, 5], 'keep': ['one', 'four', 'two', 'five'],
    'unassigned': [101, 104, 102, 105],
    'value': [30, 40, 50, 50],
}
assert calls == [[1, 4]]
delete_schema = pa.schema([pa.field('_ROW_ID', pa.int64(), nullable=False)])
deletes = MergeBatchExecution.for_deletes(input_schema, clauses).execute(joined)
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
                    pa.field('value', pa.int32()), pa.field('missing', pa.bool_())],
                   metadata={'output': 'inserts'})
calls = []
def insert(rows):
    calls.append(rows['s.id'].to_pylist())
    return pa.array([21], pa.int32())
clauses = [
    WhenNotMatched({'id': MergeAssignment.source_column('id'),
                    'value': MergeAssignment.function(insert),
                    'label': MergeAssignment.literal('first')},
                   condition='"s.value" > 10'),
    WhenNotMatched({'id': MergeAssignment.source_column('id'),
                    'value': MergeAssignment.literal(7),
                    'label': MergeAssignment.literal('last'),
                    'missing': MergeAssignment.literal(True)}),
]
execution = MergeBatchExecution.for_inserts(source.schema, target, clauses)
result = execution.execute(source)
assert result.schema.equals(target, check_metadata=True)
assert result.to_pydict() == {'label': ['first', 'last', 'last'], 'id': [1, 3, 2],
                            'value': [21, 7, 7], 'missing': [None, True, True]}
assert calls == [[1]]
assert execution.execute(source.slice(0, 0)).schema.equals(target, check_metadata=True)
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
clauses = [WhenMatched.update({'value': MergeAssignment.function(assignment)})]
execution = MergeBatchExecution.for_updates(joined.schema, output, clauses)
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
clause = WhenMatched.update({'value': MergeAssignment.literal(1)},
                            condition='"s.absent" > 0')
rejected(lambda: MergeBatchExecution.for_updates(joined.schema, output, [clause]), 'absent')
clause = WhenMatched.update({'value': MergeAssignment.function(
    lambda rows: pa.array([1], pa.int32()))})
execution = MergeBatchExecution.for_updates(joined.schema, output, [clause])
rejected(lambda: execution.execute(joined), 'length')
rejected(lambda: execution.execute(joined.rename_columns(['t._ROW_ID', 's.other'])), 'schema')
rejected(lambda: MergeBatchExecution.for_updates(joined.schema,
    pa.schema([('value', pa.int32())]), [clause]), '_ROW_ID')
rejected(lambda: MergeBatchExecution.for_updates(joined.schema,
    pa.schema([('_ROW_ID', pa.int32()), ('value', pa.int32())]), [clause]), 'Int64')
rejected(lambda: MergeBatchExecution.for_updates(joined.schema, output, [clause, clause]), 'last')
dual_ids = joined.append_column('_ROW_ID', pa.array([99, 98], pa.int64()))
rejected(lambda: MergeBatchExecution.for_updates(dual_ids.schema, output, [clause]), 'ambiguous')
rejected(lambda: MergeBatchExecution.for_deletes(dual_ids.schema, [WhenMatched.delete()]), 'ambiguous')
rejected(lambda: MergeBatchExecution.for_deletes(pa.schema([('s.value', pa.int32())]),
    [WhenMatched.delete()]), '_ROW_ID')
wrong_ids = pa.table({'t._ROW_ID': pa.array([1], pa.int32())})
rejected(lambda: MergeBatchExecution.for_deletes(wrong_ids.schema,
    [WhenMatched.delete()]).execute(wrong_ids), 'Int64')
null_ids = pa.table({'_ROW_ID': pa.array([None], pa.int64())})
rejected(lambda: MergeBatchExecution.for_deletes(null_ids.schema,
    [WhenMatched.delete()]).execute(null_ids), 'NULL')
bad_type = WhenMatched.update({'value': MergeAssignment.function(lambda rows: [1, 2])})
rejected(lambda: MergeBatchExecution.for_updates(joined.schema, output, [bad_type]).execute(joined),
         'pyarrow.Array')
# A no-match clause must not convert an unused literal or invoke a callback.
clause = WhenMatched.update({'value': MergeAssignment.literal(object())},
                            condition='"s.value" < 0')
assert MergeBatchExecution.for_updates(joined.schema, output, [clause]).execute(joined).num_rows == 0
"#
                ),
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
        });
    }

    #[test]
    fn named_factories_reject_dictionary_protocol_and_wrong_clause_families() {
        Python::attach(|py| {
            let locals = locals(py);
            py.run(
                c_str!(
                    r#"
import pyarrow as pa
input_schema = pa.schema([('t._ROW_ID', pa.int64()), ('s.value', pa.int32())])
update_schema = pa.schema([('_ROW_ID', pa.int64()), ('value', pa.int32())])
target_schema = pa.schema([('value', pa.int32())])
matched = WhenMatched.update({'value': MergeAssignment.source_column('value')})
not_matched = WhenNotMatched({'value': MergeAssignment.source_column('value')})
legacy = dict(condition=None, delete=False,
              assignments=[('value', 'source', 'value')])
def rejected(function):
    try:
        function()
    except TypeError:
        pass
    else:
        raise AssertionError('untyped MERGE protocol or wrong clause family was accepted')
for clause in [legacy, not_matched]:
    rejected(lambda: MergeBatchExecution.for_updates(input_schema, update_schema, [clause]))
    rejected(lambda: MergeBatchExecution.for_deletes(input_schema, [clause]))
for clause in [legacy, matched]:
    rejected(lambda: MergeBatchExecution.for_inserts(input_schema, target_schema, [clause]))
rejected(lambda: MergeBatchExecution(input_schema, update_schema, [legacy], 'update'))
rejected(lambda: MergeBatchExecution())
"#
                ),
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
        });
    }

    #[test]
    fn deletes_keep_update_conditions_without_converting_update_assignments() {
        Python::attach(|py| {
            let locals = locals(py);
            py.run(
                c_str!(
                    r#"
import pyarrow as pa
schema = pa.schema([
    pa.field('_ROW_ID', pa.int64(), nullable=True, metadata={'input': 'row-id'}),
    pa.field('s.value', pa.int32()),
], metadata={'input': 'joined'})
joined = pa.record_batch([pa.array([1, 2, 3], pa.int64()),
                          pa.array([10, 20, 30], pa.int32())], schema=schema)
calls = []
def unused(rows):
    calls.append(rows.num_rows)
    raise AssertionError('DELETE evaluated an UPDATE callback')
clauses = [
    WhenMatched.update({'absent': MergeAssignment.literal(object()),
                        'also_absent': MergeAssignment.source_column('missing')},
                       condition='"_ROW_ID" = 1'),
    WhenMatched.update({'unavailable': MergeAssignment.function(unused)},
                       condition='"s.value" > 20'),
    WhenMatched.delete(),
]
execution = MergeBatchExecution.for_deletes(input_schema=schema, when_matched=clauses)
result = execution.execute(joined)
expected = pa.schema([pa.field('_ROW_ID', pa.int64(), nullable=False)])
assert result.schema.equals(expected, check_metadata=True)
assert result.to_pydict() == {'_ROW_ID': [2]}
assert execution.execute(schema.empty_table()).schema.equals(expected, check_metadata=True)
assert calls == []
"#
                ),
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
        });
    }

    #[test]
    fn cached_execution_keeps_concurrent_callback_errors_with_their_callers() {
        Python::attach(|py| {
            let locals = locals(py);
            py.run(
                c_str!(
                    r#"
import pyarrow as pa
import threading
from concurrent.futures import ThreadPoolExecutor
schema = pa.schema([('t._ROW_ID', pa.int64()), ('s.value', pa.int32())])
output = pa.schema([('_ROW_ID', pa.int64()), ('value', pa.int32())])
failures = {i: LookupError('assignment failed for ' + str(i)) for i in range(4)}
callback_entered = threading.Event()
release_callback = threading.Event()
def assignment(rows):
    row_id = rows['t._ROW_ID'][0].as_py()
    callback_entered.set()
    assert release_callback.wait(timeout=10)
    if row_id in failures:
        raise failures[row_id]
    return pa.array([9], pa.int32())
execution = MergeBatchExecution.for_updates(schema, output,
    [WhenMatched.update({'value': MergeAssignment.function(assignment)})])
barrier = threading.Barrier(4)
def execute(row_id):
    batch = pa.record_batch([pa.array([row_id], pa.int64()), pa.array([1], pa.int32())],
                             schema=schema)
    barrier.wait()
    try:
        execution.execute(batch)
    except LookupError as error:
        return error
    raise AssertionError('callback exception was lost')
with ThreadPoolExecutor(max_workers=4) as threads:
    futures = [threads.submit(execute, row_id) for row_id in range(4)]
    try:
        assert callback_entered.wait(timeout=10)
    finally:
        release_callback.set()
    results = [future.result(timeout=10) for future in futures]
assert all(error is failures[i] for i, error in enumerate(results))
failures.clear()
batch = pa.record_batch([pa.array([7], pa.int64()), pa.array([1], pa.int32())], schema=schema)
assert execution.execute(batch).to_pydict() == {'_ROW_ID': [7], 'value': [9]}
"#
                ),
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
        });
    }
}
