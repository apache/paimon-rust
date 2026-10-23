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

//! Hybrid configuration forwarding; snapshot, routes and fusion belong to core.

use crate::error::to_py_err;
use crate::full_text_search::PyFullTextSearchBuilder;
use crate::global_index_result::PyGlobalIndexResult;
use crate::predicate::dict_to_table_predicate;
use crate::snapshot::PySnapshot;
use crate::vector_search::PyVectorSearchBuilder;
use paimon::spec::{Predicate, Snapshot};
use paimon::table::{
    HybridSearchRanker, HybridSearchRoute, HybridSearchRouteBuilder, HybridSearchRouteKind,
    HybridSearchRouteResult, HybridSearchWeightedResult, Table,
};
use paimon_datafusion::runtime::runtime;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use std::collections::HashMap;
use std::sync::Arc;

#[pyclass(
    name = "HybridSearchWeightedResult",
    module = "pypaimon_rust.datafusion",
    frozen
)]
pub struct PyHybridSearchWeightedResult {
    inner: HybridSearchWeightedResult,
}

#[pymethods]
impl PyHybridSearchWeightedResult {
    #[new]
    #[pyo3(signature = (result, weight=1.0))]
    fn new(result: &PyGlobalIndexResult, weight: f32) -> PyResult<Self> {
        HybridSearchWeightedResult::new(result.inner.clone(), weight)
            .map(|inner| Self { inner })
            .map_err(to_py_err)
    }

    fn result(&self) -> PyGlobalIndexResult {
        PyGlobalIndexResult::new(self.inner.result().clone())
    }

    fn weight(&self) -> f32 {
        self.inner.weight()
    }
}

#[pyclass(
    name = "HybridSearchRanker",
    module = "pypaimon_rust.datafusion",
    frozen
)]
pub struct PyHybridSearchRanker {
    inner: HybridSearchRanker,
}

#[pymethods]
impl PyHybridSearchRanker {
    #[classattr]
    const RRF: Self = Self {
        inner: HybridSearchRanker::Rrf,
    };

    #[classattr]
    const WEIGHTED_SCORE: Self = Self {
        inner: HybridSearchRanker::WeightedScore,
    };

    #[classattr]
    const MRR: Self = Self {
        inner: HybridSearchRanker::Mrr,
    };

    fn rank(
        &self,
        py: Python<'_>,
        results: Vec<PyRef<'_, PyHybridSearchWeightedResult>>,
        limit: usize,
    ) -> PyResult<PyGlobalIndexResult> {
        let results: Vec<_> = results.iter().map(|result| result.inner.clone()).collect();
        py.detach(|| self.inner.rank(&results, limit))
            .map(PyGlobalIndexResult::new)
            .map_err(to_py_err)
    }
}

/// Immutable route configuration; constructors and validation belong to core.
#[pyclass(
    name = "HybridSearchRoute",
    module = "pypaimon_rust.datafusion",
    frozen
)]
pub struct PyHybridSearchRoute {
    inner: HybridSearchRoute,
}

#[pymethods]
impl PyHybridSearchRoute {
    #[staticmethod]
    #[pyo3(signature = (field_name, vector, limit, weight=1.0, options=None))]
    fn vector(
        field_name: String,
        vector: Vec<f32>,
        limit: usize,
        weight: f32,
        options: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        HybridSearchRoute::vector(
            field_name,
            vector,
            limit,
            weight,
            options.unwrap_or_default(),
        )
        .map(|inner| Self { inner })
        .map_err(to_py_err)
    }

    #[staticmethod]
    #[pyo3(signature = (field_name, query, limit, weight=1.0, options=None))]
    fn full_text(
        field_name: String,
        query: String,
        limit: usize,
        weight: f32,
        options: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        HybridSearchRoute::full_text(
            field_name,
            query,
            limit,
            weight,
            options.unwrap_or_default(),
        )
        .map(|inner| Self { inner })
        .map_err(to_py_err)
    }

    fn kind(&self) -> &'static str {
        match self.inner.kind() {
            HybridSearchRouteKind::Vector => "vector",
            HybridSearchRouteKind::FullText => "full_text",
        }
    }

    fn field_name(&self) -> &str {
        self.inner.field_name()
    }

    fn vector_value(&self) -> Option<Vec<f32>> {
        self.inner.vector_value().map(<[f32]>::to_vec)
    }

    fn full_text_query(&self) -> Option<&str> {
        self.inner.full_text_query()
    }

    fn limit(&self) -> usize {
        self.inner.limit()
    }

    fn weight(&self) -> f32 {
        self.inner.weight()
    }

    fn options(&self) -> HashMap<String, String> {
        self.inner.options().clone()
    }
}

/// A core-owned route sharing the snapshot captured by route_builders().
#[pyclass(
    name = "HybridSearchRouteBuilder",
    module = "pypaimon_rust.datafusion",
    frozen
)]
pub struct PyHybridSearchRouteBuilder {
    inner: HybridSearchRouteBuilder,
}

#[pymethods]
impl PyHybridSearchRouteBuilder {
    fn route(&self) -> PyHybridSearchRoute {
        PyHybridSearchRoute {
            inner: self.inner.route().clone(),
        }
    }

    fn snapshot(&self) -> Option<PySnapshot> {
        self.inner
            .table()
            .travel_snapshot()
            .cloned()
            .map(PySnapshot::new)
    }

    fn search_builder(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self.inner.route().kind() {
            HybridSearchRouteKind::Vector => {
                Py::new(py, PyVectorSearchBuilder::from_hybrid_route(&self.inner)?)
                    .map(Py::into_any)
            }
            HybridSearchRouteKind::FullText => {
                Py::new(py, PyFullTextSearchBuilder::from_hybrid_route(&self.inner)?)
                    .map(Py::into_any)
            }
        }
    }

    fn execute(&self, py: Python<'_>) -> PyResult<PyGlobalIndexResult> {
        py.detach(|| runtime().block_on(self.inner.execute()))
            .map(PyGlobalIndexResult::new)
            .map_err(to_py_err)
    }
}

#[pyclass(
    name = "HybridSearchRouteResult",
    module = "pypaimon_rust.datafusion",
    frozen
)]
pub struct PyHybridSearchRouteResult {
    inner: HybridSearchRouteResult,
}

#[pymethods]
impl PyHybridSearchRouteResult {
    #[new]
    fn new(route: &PyHybridSearchRoute, result: &PyGlobalIndexResult) -> PyResult<Self> {
        HybridSearchRouteResult::new(route.inner.clone(), result.inner.clone())
            .map(|inner| Self { inner })
            .map_err(to_py_err)
    }

    fn route(&self) -> PyHybridSearchRoute {
        PyHybridSearchRoute {
            inner: self.inner.route().clone(),
        }
    }

    fn result(&self) -> PyGlobalIndexResult {
        PyGlobalIndexResult::new(self.inner.result().clone())
    }
}

#[pyclass(name = "HybridSearchBuilder", module = "pypaimon_rust.datafusion")]
pub struct PyHybridSearchBuilder {
    table: Arc<Table>,
    routes: Vec<HybridSearchRoute>,
    filters: Vec<Predicate>,
    limit: Option<usize>,
    ranker: HybridSearchRanker,
    pinned_snapshot: Option<Option<Snapshot>>,
}

impl PyHybridSearchBuilder {
    pub(crate) fn new(table: Arc<Table>) -> Self {
        Self {
            table,
            routes: Vec::new(),
            filters: Vec::new(),
            limit: None,
            ranker: HybridSearchRanker::Rrf,
            pinned_snapshot: None,
        }
    }

    fn builder(&self) -> paimon::table::HybridSearchBuilder<'_> {
        let mut builder = self.table.new_hybrid_search_builder();
        for route in &self.routes {
            builder.add_route(route.clone());
        }
        if let Some(limit) = self.limit {
            builder.with_limit(limit);
        }
        // The stored ranker has already been validated by the core parser.
        builder
            .with_ranker(self.ranker.as_str())
            .expect("validated ranker");
        for filter in &self.filters {
            builder.with_filter(filter.clone());
        }
        if let Some(snapshot) = &self.pinned_snapshot {
            builder.with_snapshot(snapshot.as_ref());
        }
        builder
    }
}

#[pymethods]
impl PyHybridSearchBuilder {
    #[pyo3(signature = (field_name, vector, limit, weight=1.0, options=None))]
    fn add_vector_route(
        mut slf: PyRefMut<'_, Self>,
        field_name: String,
        vector: Vec<f32>,
        limit: usize,
        weight: f32,
        options: Option<HashMap<String, String>>,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let route = HybridSearchRoute::vector(
            field_name,
            vector,
            limit,
            weight,
            options.unwrap_or_default(),
        )
        .map_err(to_py_err)?;
        slf.routes.push(route);
        Ok(slf)
    }

    #[pyo3(signature = (field_name, query, limit, weight=1.0, options=None))]
    fn add_full_text_route(
        mut slf: PyRefMut<'_, Self>,
        field_name: String,
        query: String,
        limit: usize,
        weight: f32,
        options: Option<HashMap<String, String>>,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let route = HybridSearchRoute::full_text(
            field_name,
            query,
            limit,
            weight,
            options.unwrap_or_default(),
        )
        .map_err(to_py_err)?;
        slf.routes.push(route);
        Ok(slf)
    }

    fn with_limit(mut slf: PyRefMut<'_, Self>, limit: usize) -> PyRefMut<'_, Self> {
        slf.limit = Some(limit);
        slf
    }

    #[pyo3(signature = (snapshot))]
    fn with_snapshot<'py>(
        mut slf: PyRefMut<'py, Self>,
        snapshot: Option<PyRef<'_, PySnapshot>>,
    ) -> PyRefMut<'py, Self> {
        slf.pinned_snapshot = Some(snapshot.map(|snapshot| snapshot.inner.clone()));
        slf
    }

    #[pyo3(signature = (ranker))]
    fn with_ranker<'py>(
        mut slf: PyRefMut<'py, Self>,
        ranker: Option<&str>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.ranker = HybridSearchRanker::parse(ranker.unwrap_or_default()).map_err(to_py_err)?;
        Ok(slf)
    }

    fn with_rrf_ranker(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.ranker = HybridSearchRanker::Rrf;
        slf
    }

    fn with_weighted_score_ranker(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.ranker = HybridSearchRanker::WeightedScore;
        slf
    }

    fn with_filter<'py>(
        mut slf: PyRefMut<'py, Self>,
        predicate: &Bound<'_, PyDict>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let filter = dict_to_table_predicate(predicate, slf.table.schema(), true)?;
        slf.filters.push(filter);
        Ok(slf)
    }

    fn with_partition_filter<'py>(
        mut slf: PyRefMut<'py, Self>,
        predicate: &Bound<'_, PyDict>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let filter = dict_to_table_predicate(predicate, slf.table.schema(), true)?;
        slf.builder()
            .with_partition_filter(filter.clone())
            .map_err(to_py_err)?;
        slf.filters.push(filter);
        Ok(slf)
    }

    fn execute(&self, py: Python<'_>) -> PyResult<PyGlobalIndexResult> {
        let builder = self.builder();
        py.detach(|| runtime().block_on(builder.execute_scored()))
            .map(PyGlobalIndexResult::new)
            .map_err(to_py_err)
    }

    fn route_builders(&self, py: Python<'_>) -> PyResult<Vec<PyHybridSearchRouteBuilder>> {
        let builder = self.builder();
        py.detach(|| runtime().block_on(builder.route_builders()))
            .map(|routes| {
                routes
                    .into_iter()
                    .map(|inner| PyHybridSearchRouteBuilder { inner })
                    .collect()
            })
            .map_err(to_py_err)
    }

    fn to_route_result(
        &self,
        route_builder: &PyHybridSearchRouteBuilder,
        result: &PyGlobalIndexResult,
    ) -> PyResult<PyHybridSearchRouteResult> {
        self.builder()
            .to_route_result(&route_builder.inner, result.inner.clone())
            .map(|inner| PyHybridSearchRouteResult { inner })
            .map_err(to_py_err)
    }

    fn rank(
        &self,
        py: Python<'_>,
        route_results: Vec<PyRef<'_, PyHybridSearchRouteResult>>,
    ) -> PyResult<PyGlobalIndexResult> {
        let route_results: Vec<_> = route_results
            .iter()
            .map(|result| result.inner.clone())
            .collect();
        py.detach(|| self.builder().rank(&route_results))
            .map(PyGlobalIndexResult::new)
            .map_err(to_py_err)
    }

    fn _execute_with_executor(
        &self,
        py: Python<'_>,
        executor: Py<PyAny>,
    ) -> PyResult<PyGlobalIndexResult> {
        let builder = self.builder();
        let executor = crate::search_task::PythonSearchTaskExecutor::new(executor);
        let result = py.detach(|| {
            runtime().block_on(paimon::table::internal::execute_hybrid_with_executor(
                &builder, &executor,
            ))
        });
        if let Some(error) = executor.take_error() {
            return Err(error);
        }
        result.map(PyGlobalIndexResult::new).map_err(to_py_err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paimon::catalog::Identifier;
    use paimon::io::FileIOBuilder;
    use paimon::spec::{
        ArrayType, CommitKind, DataType, FloatType, IntType, Schema, TableSchema, VarCharType,
    };
    use pyo3::types::PyModule;

    fn module(py: Python<'_>) -> Bound<'_, PyModule> {
        let package = PyModule::new(py, "pypaimon_rust").unwrap();
        crate::pypaimon_rust(py, &package).unwrap();
        py.import("sys")
            .unwrap()
            .getattr("modules")
            .unwrap()
            .set_item("pypaimon_rust", &package)
            .unwrap();
        package.getattr("datafusion").unwrap().cast_into().unwrap()
    }

    fn table() -> Arc<Table> {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("text", DataType::VarChar(VarCharType::string_type()))
            .column(
                "embedding",
                DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
            )
            .options(HashMap::from([
                ("bucket".into(), "-1".into()),
                ("row-tracking.enabled".into(), "true".into()),
                ("data-evolution.enabled".into(), "true".into()),
                ("global-index.enabled".into(), "true".into()),
            ]))
            .build()
            .unwrap();
        Arc::new(Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("default", "hybrid_phases"),
            "memory:/hybrid_phases".into(),
            TableSchema::new(0, &schema),
            None,
        ))
    }

    #[test]
    fn hybrid_rankers_fuse_typed_external_results_without_a_table() {
        Python::attach(|py| {
            let locals = PyDict::new(py);
            locals.set_item("sdk", module(py)).unwrap();
            py.run(
                cr#"
import math
first = sdk.GlobalIndexResult.from_scored_row_ids({10: 3.0, 20: 1.0})
second = sdk.GlobalIndexResult.from_scored_row_ids({20: 5.0, 30: 1.0})
results = (sdk.HybridSearchWeightedResult(first, 2.0),
           sdk.HybridSearchWeightedResult(second))
expected = (
    (sdk.HybridSearchRanker.RRF, {20: 2.0 / 62.0 + 1.0 / 61.0, 10: 2.0 / 61.0}),
    (sdk.HybridSearchRanker.WEIGHTED_SCORE, {10: 2.0, 20: 1.0}),
    (sdk.HybridSearchRanker.MRR, {10: 2.0, 20: 2.0}),
)
for ranker, scores in expected:
    actual = ranker.rank(results, 2)
    assert isinstance(actual, sdk.GlobalIndexResult)
    assert actual.snapshot_id() is None
    assert actual.row_ids().keys() == scores.keys()
    assert all(math.isclose(actual.row_ids()[key], score, rel_tol=1e-6)
               for key, score in scores.items())
    assert ranker.rank([], 2).row_ids() == {}
    empty = sdk.HybridSearchWeightedResult(sdk.GlobalIndexResult.from_row_ranges([]))
    assert empty.result().row_ids() == {}
    assert ranker.rank([empty], 2).row_ids() == {}
    try:
        ranker.rank(results, 0)
    except ValueError as error:
        assert 'Limit must be positive' in str(error)
    else:
        raise AssertionError('zero fusion limit must fail')
for weight in (0.0, -1.0, float('inf')):
    try:
        sdk.HybridSearchWeightedResult(first, weight)
    except ValueError as error:
        assert 'Weight must be finite and positive' in str(error)
    else:
        raise AssertionError('invalid weight must fail')
for result, message in (
    (sdk.GlobalIndexResult.from_row_ranges([(1, 2)]), 'scored index results'),
    (sdk.GlobalIndexResult.from_splits(None, []), 'primary-key physical results'),
):
    try:
        sdk.HybridSearchWeightedResult(result)
    except NotImplementedError as error:
        assert message in str(error)
    else:
        raise AssertionError('unsupported result family must fail')
try:
    sdk.HybridSearchRanker.RRF.rank([first], 2)
except TypeError:
    pass
else:
    raise AssertionError('rank must accept typed weighted results')
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
        });
    }

    #[test]
    fn hybrid_route_results_are_immutable_and_validate_result_families() {
        Python::attach(|py| {
            let locals = PyDict::new(py);
            locals.set_item("sdk", module(py)).unwrap();
            py.run(
                cr#"
route = sdk.HybridSearchRoute.vector('embedding', [1.0, 0.0], 3, 2.0, {'metric': 'l2'})
vector = route.vector_value()
vector[0] = 99.0
options = route.options()
options['metric'] = 'cosine'
assert route.vector_value() == [1.0, 0.0]
assert route.options() == {'metric': 'l2'}
assert route.kind() == 'vector' and route.full_text_query() is None
text = sdk.HybridSearchRoute.full_text('text', '{"match":{"query":"paimon"}}', 4)
assert text.kind() == 'full_text' and text.vector_value() is None
assert text.full_text_query() == '{"match":{"query":"paimon"}}'
for configuration in (route, text):
    scored = sdk.GlobalIndexResult.from_scored_row_ids({7: 0.5})
    result = sdk.HybridSearchRouteResult(configuration, scored)
    assert result.route().kind() == configuration.kind()
    assert result.result().row_ids() == {7: 0.5}
    empty = sdk.HybridSearchRouteResult(configuration, sdk.GlobalIndexResult.from_row_ranges([]))
    assert empty.result().row_ids() == {}
    try:
        sdk.HybridSearchRouteResult(configuration, sdk.GlobalIndexResult.from_row_ranges([(1, 2)]))
    except NotImplementedError as error:
        assert 'scored index results' in str(error)
    else:
        raise AssertionError('unscored nonempty route result must fail')
    try:
        configuration.extra = True
    except AttributeError:
        pass
    else:
        raise AssertionError('route configuration must be immutable')
assert sdk.HybridSearchRoute.vector('embedding', [], 3).vector_value() == []
assert sdk.HybridSearchRoute.full_text('text', '', 3).full_text_query() == ''
for create in (
    lambda: sdk.HybridSearchRoute.vector('embedding', [1.0], 0),
    lambda: sdk.HybridSearchRoute.full_text('text', '{}', 3, options={'ignored': 'no'}),
):
    try:
        create()
    except ValueError:
        pass
    else:
        raise AssertionError('invalid route must fail core validation')
try:
    sdk.HybridSearchRouteResult({}, sdk.GlobalIndexResult.from_scored_row_ids({}))
except TypeError:
    pass
else:
    raise AssertionError('route result must require typed route metadata')
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
        });
    }

    #[test]
    fn hybrid_phases_return_configured_search_builders_on_one_empty_view() {
        Python::attach(|py| {
            let locals = PyDict::new(py);
            locals.set_item("sdk", module(py)).unwrap();
            locals
                .set_item("builder", PyHybridSearchBuilder::new(table()))
                .unwrap();
            locals
                .set_item("invalid", PyHybridSearchBuilder::new(table()))
                .unwrap();
            locals
                .set_item("incomplete", PyHybridSearchBuilder::new(table()))
                .unwrap();
            let snapshot = Snapshot::builder()
                .version(3)
                .id(7)
                .schema_id(0)
                .base_manifest_list("base".into())
                .delta_manifest_list("delta".into())
                .commit_user("captured".into())
                .commit_identifier(1)
                .commit_kind(CommitKind::APPEND)
                .time_millis(0)
                .build();
            locals
                .set_item("snapshot", PySnapshot::new(snapshot))
                .unwrap();
            py.run(
                cr#"
builder.add_vector_route('embedding', [1.0, 0.0], 3, 2.0)
builder.add_full_text_route('text', '{"match":{"query":"paimon"}}', 4)
builder.with_limit(2).with_snapshot(None)
routes = builder.route_builders()
assert len(routes) == 2
assert all(isinstance(route, sdk.HybridSearchRouteBuilder) for route in routes)
assert all(route.snapshot() is None for route in routes)
vector, text = (route.search_builder() for route in routes)
assert isinstance(vector, sdk.VectorSearchBuilder)
assert isinstance(text, sdk.FullTextSearchBuilder)
for scan in (vector.new_vector_search_scan(), text.new_full_text_scan()):
    plan = scan.scan()
    assert plan.snapshot() is None and plan.splits() == []
assert vector.execute().row_ids() == {} and text.execute().row_ids() == {}
results = [builder.to_route_result(route, route.execute()) for route in routes]
assert [result.route().weight() for result in results] == [2.0, 1.0]
fused = builder.rank(tuple(results))
assert fused.row_ids() == {} and fused.snapshot_id() is None
# Reading an empty fused search retains its core table context.
assert fused.new_read_builder().with_projection(['id']).read() == []
weighted = [sdk.HybridSearchWeightedResult(result.result(), result.route().weight())
            for result in results]
assert sdk.HybridSearchRanker.RRF.rank(weighted, 2).new_read_builder().read() == []
builder.with_snapshot(snapshot)
captured = builder.route_builders()
assert all(isinstance(route.snapshot(), sdk.Snapshot) for route in captured)
assert all(route.snapshot().id() == 7 for route in captured)
# Reconfiguring the hybrid builder cannot change handles already returned.
builder.with_snapshot(None)
assert all(route.snapshot().id() == 7 for route in captured)
assert all(route.snapshot() is None for route in routes)
incomplete.add_vector_route('embedding', [], 3).add_full_text_route('text', '', 3)
incomplete.with_limit(2).with_snapshot(None)
unfinished = incomplete.route_builders()
vector, text = (route.search_builder() for route in unfinished)
vector.with_query_vector([1.0, 0.0])
text.with_query('text', '{"match":{"query":"paimon"}}')
assert vector.execute().row_ids() == {} and text.execute().row_ids() == {}
invalid.add_vector_route('missing', [1.0], 3).with_limit(2).with_snapshot(None)
try:
    invalid.route_builders()
except ValueError as error:
    assert "Vector column 'missing' does not exist" in str(error)
else:
    raise AssertionError('empty capture must still validate the route schema')
"#,
                Some(&locals),
                Some(&locals),
            )
            .unwrap();
        });
    }

    #[test]
    fn hybrid_snapshot_uses_typed_metadata_and_distinguishes_empty_capture() {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .build()
            .unwrap();
        let table = Arc::new(Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("default", "hybrid_typed_snapshot"),
            "memory:/hybrid_typed_snapshot".into(),
            TableSchema::new(0, &schema),
            None,
        ));
        let snapshot = Snapshot::builder()
            .version(3)
            .id(7)
            .schema_id(0)
            .base_manifest_list(String::new())
            .delta_manifest_list(String::new())
            .commit_user("captured".into())
            .commit_identifier(1)
            .commit_kind(CommitKind::APPEND)
            .time_millis(0)
            .build();
        Python::attach(|py| {
            let builder = Py::new(py, PyHybridSearchBuilder::new(table)).unwrap();
            assert!(builder.borrow(py).pinned_snapshot.is_none());
            let captured = Py::new(py, PySnapshot::new(snapshot.clone())).unwrap();
            let returned = builder
                .bind(py)
                .call_method1("with_snapshot", (captured,))
                .unwrap();
            assert!(returned.is(builder.bind(py)));
            assert_eq!(
                builder
                    .borrow(py)
                    .pinned_snapshot
                    .as_ref()
                    .unwrap()
                    .as_ref(),
                Some(&snapshot),
            );
            let error = builder
                .bind(py)
                .call_method1("with_snapshot", ("{}",))
                .unwrap_err();
            assert!(error.is_instance_of::<pyo3::exceptions::PyTypeError>(py));
            builder
                .bind(py)
                .call_method1("with_snapshot", (py.None(),))
                .unwrap();
            assert!(builder
                .borrow(py)
                .pinned_snapshot
                .as_ref()
                .unwrap()
                .is_none());
        });
    }
}
