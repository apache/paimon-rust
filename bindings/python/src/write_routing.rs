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

//! Arrow transport for pure Core input routing and bucket planning.

use crate::arrow_input::write_batch_from_pyarrow;
use crate::error::to_py_err;
use crate::schema::PyTableSchema;
use arrow::array::Array;
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow::record_batch::RecordBatch;
use paimon::spec::TableSchema;
use paimon::table::{BucketRouter, PostponeBucketPlan, PostponeBucketPlanner};
use pyo3::prelude::*;

#[pyclass(name = "BucketRouter", module = "pypaimon_rust.datafusion")]
pub struct PyBucketRouter {
    schema: TableSchema,
    inner: BucketRouter,
}

#[pymethods]
impl PyBucketRouter {
    #[new]
    fn new(schema: &PyTableSchema) -> PyResult<Self> {
        let inner = BucketRouter::new(&schema.inner).map_err(to_py_err)?;
        Ok(Self {
            schema: schema.inner.clone(),
            inner,
        })
    }

    fn with_bucket_plan<'py>(
        mut slf: PyRefMut<'py, Self>,
        plan: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let batch = RecordBatch::from_pyarrow_bound(plan)?;
        let plan = PostponeBucketPlan::from_arrow(&slf.schema, &batch).map_err(to_py_err)?;
        slf.inner = slf
            .inner
            .clone()
            .with_bucket_plan(plan)
            .map_err(to_py_err)?;
        Ok(slf)
    }

    fn route(&self, py: Python<'_>, batch: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let batch = write_batch_from_pyarrow(batch)?;
        let buckets = py.detach(|| self.inner.route(&batch)).map_err(to_py_err)?;
        buckets.to_data().to_pyarrow(py).map(Bound::unbind)
    }
}

#[pyclass(name = "PostponeBucketPlanner", module = "pypaimon_rust.datafusion")]
pub struct PyPostponeBucketPlanner {
    schema: TableSchema,
    inner: PostponeBucketPlanner,
}

#[pymethods]
impl PyPostponeBucketPlanner {
    #[new]
    #[pyo3(signature = (schema, known_plan=None))]
    fn new(schema: &PyTableSchema, known_plan: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        let known = match known_plan {
            Some(plan) => {
                let batch = RecordBatch::from_pyarrow_bound(plan)?;
                PostponeBucketPlan::from_arrow(&schema.inner, &batch).map_err(to_py_err)?
            }
            None => PostponeBucketPlan::default(),
        };
        let inner = PostponeBucketPlanner::new(&schema.inner, known).map_err(to_py_err)?;
        Ok(Self {
            schema: schema.inner.clone(),
            inner,
        })
    }

    fn current_plan(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.inner
            .current_plan()
            .to_arrow(&self.schema)
            .map_err(to_py_err)?
            .to_pyarrow(py)
            .map(Bound::unbind)
    }

    fn input_statistics(&self, py: Python<'_>, batch: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let batch = write_batch_from_pyarrow(batch)?;
        py.detach(|| self.inner.input_statistics(&batch))
            .map_err(to_py_err)?
            .to_pyarrow(py)
            .map(Bound::unbind)
    }

    fn plan(&mut self, py: Python<'_>, statistics: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let statistics = RecordBatch::from_pyarrow_bound(statistics)?;
        py.detach(|| self.inner.plan(&statistics))
            .map_err(to_py_err)?
            .to_arrow(&self.schema)
            .map_err(to_py_err)?
            .to_pyarrow(py)
            .map(Bound::unbind)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paimon::spec::{DataType, IntType, Schema, VarCharType};
    use pyo3::ffi::c_str;
    use pyo3::types::{PyDict, PyModule};

    #[test]
    fn routing_and_planning_use_typed_arrow_inputs_without_table_access() {
        Python::attach(|py| {
            let package = PyModule::new(py, "pypaimon_rust").unwrap();
            crate::pypaimon_rust(py, &package).unwrap();
            py.import("sys")
                .unwrap()
                .getattr("modules")
                .unwrap()
                .set_item("pypaimon_rust", &package)
                .unwrap();
            let locals = PyDict::new(py);
            locals
                .set_item("sdk", package.getattr("datafusion").unwrap())
                .unwrap();
            for (name, bucket, function) in [("fixed", "5", "mod"), ("postpone", "-2", "default")] {
                let schema = Schema::builder()
                    .column("pt", DataType::VarChar(VarCharType::string_type()))
                    .column("id", DataType::Int(IntType::new()))
                    .column("payload", DataType::VarChar(VarCharType::string_type()))
                    .partition_keys(["pt"])
                    .primary_key(["pt", "id"])
                    .option("bucket", bucket)
                    .option("bucket-key", "id")
                    .option("bucket-function.type", function)
                    .option("postpone.target-row-num-per-bucket", "2")
                    .option("postpone.batch-write-fixed-bucket.max-parallelism", "8")
                    .build()
                    .unwrap();
                locals
                    .set_item(
                        name,
                        Py::new(py, PyTableSchema::new(TableSchema::new(0, &schema))).unwrap(),
                    )
                    .unwrap();
            }
            py.run(
                c_str!(r#"
import pyarrow as pa
batch = pa.RecordBatch.from_arrays([
    pa.array([-17, -1, 0, 17], type=pa.int32()),
    pa.array(['a', 'a', 'a', 'b']),
], names=['id', 'pt'])
router = sdk.BucketRouter(fixed)
buckets = router.route(batch)
assert isinstance(buckets, pa.Int32Array)
assert buckets.to_pylist() == [3, 4, 0, 2]
assert buckets.null_count == 0

planner = sdk.PostponeBucketPlanner(postpone)
assert planner.current_plan().schema.names == ['pt', 'total_buckets']
assert planner.current_plan().num_rows == 0
statistics = planner.input_statistics(batch)
assert statistics.schema.names == ['pt', 'row_count', 'data_size']
rows = dict(zip(statistics.column(0).to_pylist(), statistics.column(1).to_pylist()))
assert rows == {'a': 3, 'b': 1}
assert statistics.column(2).to_pylist() == [0] * statistics.num_rows
plan = planner.plan(statistics)
assert dict(zip(plan.column(0).to_pylist(), plan.column(1).to_pylist())) == {'a': 2, 'b': 1}
postpone_router = sdk.BucketRouter(postpone)
assert postpone_router.with_bucket_plan(plan) is postpone_router
assert all(0 <= bucket < (2 if partition == 'a' else 1)
           for partition, bucket in zip(batch.column(1).to_pylist(), postpone_router.route(batch).to_pylist()))
known = sdk.PostponeBucketPlanner(postpone, plan)
assert known.input_statistics(batch).num_rows == 0
bad = pa.RecordBatch.from_arrays([pa.array(['c']), pa.array([-1], type=pa.int64()),
                                  pa.array([0], type=pa.int64())], names=['pt', 'row_count', 'data_size'])
try:
    planner.plan(bad)
except ValueError:
    pass
else:
    raise AssertionError('negative statistics must be rejected')
assert planner.current_plan().equals(plan)
"#),
                Some(&locals),
                Some(&locals),
            ).unwrap();
        });
    }
}
