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

//! Immutable Python descriptions of core MERGE clauses and assignments.

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyMapping;
use pyo3::{PyTraverseError, PyVisit};

pub(crate) enum AssignmentValue {
    SourceColumn(String),
    TargetColumn(String),
    Literal(Py<PyAny>),
    Function(Py<PyAny>),
}

/// A typed column reference, literal, or function evaluated for selected rows.
#[pyclass(name = "MergeAssignment", module = "pypaimon_rust.datafusion", frozen)]
pub struct PyMergeAssignment {
    pub(crate) value: AssignmentValue,
}

#[pymethods]
impl PyMergeAssignment {
    #[staticmethod]
    fn source_column(name: String) -> Self {
        Self {
            value: AssignmentValue::SourceColumn(name),
        }
    }

    #[staticmethod]
    fn target_column(name: String) -> Self {
        Self {
            value: AssignmentValue::TargetColumn(name),
        }
    }

    #[staticmethod]
    fn literal(value: Py<PyAny>) -> Self {
        Self {
            value: AssignmentValue::Literal(value),
        }
    }

    #[staticmethod]
    fn function(function: Py<PyAny>, py: Python<'_>) -> PyResult<Self> {
        if !function.bind(py).is_callable() {
            return Err(PyTypeError::new_err(
                "MergeAssignment.function requires a callable",
            ));
        }
        Ok(Self {
            value: AssignmentValue::Function(function),
        })
    }

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        match &self.value {
            AssignmentValue::Literal(value) | AssignmentValue::Function(value) => visit.call(value),
            AssignmentValue::SourceColumn(_) | AssignmentValue::TargetColumn(_) => Ok(()),
        }
    }
}

pub(crate) type Assignments = Vec<(String, Py<PyMergeAssignment>)>;

fn assignments(values: &Bound<'_, PyMapping>) -> PyResult<Assignments> {
    values.items()?.extract()
}

/// An ordered WHEN MATCHED UPDATE or DELETE branch.
#[pyclass(name = "WhenMatched", module = "pypaimon_rust.datafusion", frozen)]
pub struct PyWhenMatched {
    pub(crate) condition: Option<String>,
    pub(crate) delete: bool,
    pub(crate) assignments: Assignments,
}

#[pymethods]
impl PyWhenMatched {
    #[staticmethod]
    #[pyo3(signature = (assignments, condition=None))]
    fn update(assignments: &Bound<'_, PyMapping>, condition: Option<String>) -> PyResult<Self> {
        Ok(Self {
            condition,
            delete: false,
            assignments: self::assignments(assignments)?,
        })
    }

    #[staticmethod]
    #[pyo3(signature = (condition=None))]
    fn delete(condition: Option<String>) -> Self {
        Self {
            condition,
            delete: true,
            assignments: Vec::new(),
        }
    }

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        for (_, value) in &self.assignments {
            visit.call(value)?;
        }
        Ok(())
    }
}

/// An ordered WHEN NOT MATCHED INSERT branch.
#[pyclass(name = "WhenNotMatched", module = "pypaimon_rust.datafusion", frozen)]
pub struct PyWhenNotMatched {
    pub(crate) condition: Option<String>,
    pub(crate) assignments: Assignments,
}

#[pymethods]
impl PyWhenNotMatched {
    #[new]
    #[pyo3(signature = (assignments, condition=None))]
    fn new(assignments: &Bound<'_, PyMapping>, condition: Option<String>) -> PyResult<Self> {
        Ok(Self {
            condition,
            assignments: self::assignments(assignments)?,
        })
    }

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        for (_, value) in &self.assignments {
            visit.call(value)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::ffi::c_str;

    #[test]
    fn typed_factories_validate_descriptions_and_snapshot_assignments() {
        Python::attach(|py| {
            let locals = pyo3::types::PyDict::new(py);
            locals
                .set_item("MergeAssignment", py.get_type::<PyMergeAssignment>())
                .unwrap();
            locals
                .set_item("WhenMatched", py.get_type::<PyWhenMatched>())
                .unwrap();
            locals
                .set_item("WhenNotMatched", py.get_type::<PyWhenNotMatched>())
                .unwrap();
            py.run(
                c_str!(
                    r#"
from types import MappingProxyType
value = MergeAssignment.source_column('value')
mapping = {'value': value}
matched = WhenMatched.update(MappingProxyType(mapping), condition='"s.value" > 0')
unmatched = WhenNotMatched(mapping)
mapping.clear()
for make in [
    lambda: MergeAssignment(),
    lambda: MergeAssignment.source_column(1),
    lambda: MergeAssignment.target_column(None),
    lambda: MergeAssignment.function(1),
    lambda: WhenMatched(),
    lambda: WhenMatched.update({'value': 1}),
    lambda: WhenMatched.update({1: value}),
    lambda: WhenMatched.update({'value': value}, condition={'sql': 'true'}),
    lambda: WhenMatched.delete(condition={'sql': 'true'}),
    lambda: WhenNotMatched({'value': 1}),
    lambda: WhenNotMatched({'value': value}, condition={'sql': 'true'}),
]:
    try:
        make()
    except TypeError:
        pass
    else:
        raise AssertionError('untyped MERGE description was accepted')
for description in [value, matched, unmatched]:
    try:
        description.condition = None
    except AttributeError:
        pass
    else:
        raise AssertionError('MERGE description was mutable')
"#
                ),
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
            let matched = locals
                .get_item("matched")
                .unwrap()
                .unwrap()
                .extract::<PyRef<PyWhenMatched>>()
                .unwrap();
            let unmatched = locals
                .get_item("unmatched")
                .unwrap()
                .unwrap()
                .extract::<PyRef<PyWhenNotMatched>>()
                .unwrap();
            assert_eq!(matched.assignments.len(), 1);
            assert_eq!(unmatched.assignments.len(), 1);
            assert_eq!(matched.assignments[0].0, "value");
            assert_eq!(unmatched.assignments[0].0, "value");
        });
    }
}
