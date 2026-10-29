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

use crate::arrow_input::{array_from_pyarrow, write_batch_from_pyarrow};
use crate::error::{to_catalog_py_err, to_py_err};
use crate::schema::PyTableSchema;
use crate::table::PyTable;
use arrow::array::{Array, Int32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow::record_batch::RecordBatch;
use paimon::spec::TableSchema;
use paimon::table::{
    BucketRouter, DynamicBucketAssigner, DynamicBucketAssignerRouter, PostponeBucketPlan,
    PostponeBucketPlanner,
};
use paimon_datafusion::runtime::runtime;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use std::sync::Arc;

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

#[pyclass(
    name = "DynamicBucketAssignerRouter",
    module = "pypaimon_rust.datafusion"
)]
pub struct PyDynamicBucketAssignerRouter {
    inner: DynamicBucketAssignerRouter,
}

#[pymethods]
impl PyDynamicBucketAssignerRouter {
    #[new]
    fn new(schema: &PyTableSchema, num_channels: i32, num_assigners: i32) -> PyResult<Self> {
        Ok(Self {
            inner: DynamicBucketAssignerRouter::new(&schema.inner, num_channels, num_assigners)
                .map_err(to_py_err)?,
        })
    }

    fn route(&self, py: Python<'_>, batch: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let batch = write_batch_from_pyarrow(batch)?;
        let routing = py.detach(|| self.inner.route(&batch)).map_err(to_py_err)?;
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("assigner", DataType::Int32, false),
                Field::new("key_hash", DataType::Int32, false),
            ])),
            vec![Arc::new(routing.assigners), Arc::new(routing.key_hashes)],
        )
        .map_err(|error| PyTypeError::new_err(error.to_string()))?
        .to_pyarrow(py)
        .map(Bound::unbind)
    }
}

#[pyclass(name = "DynamicBucketAssigner", module = "pypaimon_rust.datafusion")]
pub struct PyDynamicBucketAssigner {
    inner: DynamicBucketAssigner,
}

#[pymethods]
impl PyDynamicBucketAssigner {
    #[new]
    #[pyo3(signature = (table, num_channels, num_assigners, assign_id, *, restore_snapshot_id=None, overwrite=false))]
    fn new(
        py: Python<'_>,
        table: &PyTable,
        num_channels: i32,
        num_assigners: i32,
        assign_id: i32,
        restore_snapshot_id: Option<i64>,
        overwrite: bool,
    ) -> PyResult<Self> {
        let builder =
            DynamicBucketAssigner::builder(&table.inner, num_channels, num_assigners, assign_id);
        let builder = match restore_snapshot_id {
            Some(id) => builder
                .with_restore_snapshot(id)
                .map_err(to_catalog_py_err)?,
            None => builder,
        };
        let builder = if overwrite {
            builder.with_overwrite()
        } else {
            builder
        };
        let inner = py
            .detach(|| runtime().block_on(builder.build()))
            .map_err(to_catalog_py_err)?;
        Ok(Self { inner })
    }

    #[getter]
    fn restore_snapshot_id(&self) -> i64 {
        self.inner.restore_snapshot_id()
    }

    #[pyo3(signature = (batch, key_hashes=None))]
    fn assign(
        &mut self,
        py: Python<'_>,
        batch: &Bound<'_, PyAny>,
        key_hashes: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let batch = write_batch_from_pyarrow(batch)?;
        let hashes = key_hashes.map(array_from_pyarrow).transpose()?;
        let hashes = hashes
            .as_ref()
            .map(|hashes| {
                hashes
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| PyTypeError::new_err("key_hashes must be an Int32Array"))
            })
            .transpose()?;
        let buckets = py
            .detach(|| runtime().block_on(self.inner.assign(&batch, hashes)))
            .map_err(to_catalog_py_err)?;
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
    fn dynamic_routing_and_assignment_preserve_arrow_rows_and_write_baseline() {
        use paimon::catalog::Identifier;
        use paimon::io::FileIOBuilder;
        use paimon::table::Table;

        Python::attach(|py| {
            let schema = Schema::builder()
                .column("pt", DataType::VarChar(VarCharType::string_type()))
                .column("id", DataType::Int(IntType::new()))
                .column("payload", DataType::VarChar(VarCharType::string_type()))
                .partition_keys(["pt"])
                .primary_key(["pt", "id"])
                .option("bucket", "-1")
                .option("dynamic-bucket.target-row-num", "1")
                .build()
                .unwrap();
            let table = Arc::new(Table::new(
                FileIOBuilder::new("memory").build().unwrap(),
                Identifier::new("default", "dynamic_arrow_binding"),
                "memory:/dynamic_arrow_binding".into(),
                TableSchema::new(0, &schema),
                None,
            ));
            let locals = PyDict::new(py);
            locals
                .set_item(
                    "table",
                    Py::new(py, crate::table::PyTable::new(table)).unwrap(),
                )
                .unwrap();
            locals
                .set_item(
                    "DynamicBucketAssignerRouter",
                    py.get_type::<PyDynamicBucketAssignerRouter>(),
                )
                .unwrap();
            locals
                .set_item(
                    "DynamicBucketAssigner",
                    py.get_type::<PyDynamicBucketAssigner>(),
                )
                .unwrap();
            py.run(
                c_str!(
                    r#"
import pyarrow as pa
router = DynamicBucketAssignerRouter(table.schema(), 1, 1)
batch = pa.record_batch([
    pa.array(['unused', 'a', 'a', 'a', 'unused']),
    pa.array([0, 10, 20, 10, 0], type=pa.int32()),
    pa.array(['x', 'v1', 'v2', 'v3', 'x']),
], names=['pt', 'id', 'payload']).slice(1, 3)
routing = router.route(batch)
assert routing.schema.names == ['assigner', 'key_hash']
assert routing.column(0).to_pylist() == [0, 0, 0]
hashes = routing.column(1)
assert hashes.type == pa.int32() and hashes.null_count == 0
assert hashes[0] == hashes[2] and hashes[0] != hashes[1]

builder = table.new_batch_write_builder().with_restore_snapshot(0)
assert not hasattr(builder, 'new_dynamic_bucket_assigner')
assigner = DynamicBucketAssigner(table, 1, 1, 0, restore_snapshot_id=0)
assert assigner.restore_snapshot_id == 0
try:
    DynamicBucketAssigner(table, 1, 1, 0, restore_snapshot_id=-1)
except ValueError:
    pass
else:
    raise AssertionError('negative restore snapshots must be rejected')
for invalid in [pa.array([None, 1, 2], type=pa.int32()), hashes.slice(0, 2),
                pa.array([1, 2, 3], type=pa.int64())]:
    try:
        assigner.assign(batch, invalid)
    except (ValueError, TypeError):
        pass
    else:
        raise AssertionError('invalid hash transport must fail before assignment')
buckets = assigner.assign(batch, hashes)
assert buckets.to_pylist() == [0, 1, 0]
assert assigner.assign(batch.slice(1), hashes.slice(1)).to_pylist() == [1, 0]
assert DynamicBucketAssigner(table, 1, 1, 0, restore_snapshot_id=0).assign(batch).equals(buckets)

writer = builder.new_write()
writer.write_arrow(batch.take(pa.array([0, 2])), bucket=0)
writer.write_arrow(batch.slice(1, 1), bucket=1)
messages = writer.prepare_commit()
builder.new_commit().commit(messages)

restored = DynamicBucketAssigner(table, 1, 1, 0)
assert restored.restore_snapshot_id == 1
assert restored.assign(batch, hashes).equals(buckets)
# A captured empty base stays empty after publication of a newer snapshot.
assert assigner.restore_snapshot_id == 0
empty = DynamicBucketAssigner(table, 1, 1, 0, restore_snapshot_id=0)
assert empty.restore_snapshot_id == 0
assert empty.assign(batch.slice(1, 1)).to_pylist() == [0]
overwritten = DynamicBucketAssigner(table, 1, 1, 0, overwrite=True)
assert overwritten.restore_snapshot_id == 0
assert overwritten.assign(batch.slice(1, 1)).to_pylist() == [0]
"#
                ),
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
        });
    }

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
