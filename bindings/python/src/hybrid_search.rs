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
use crate::predicate::dict_to_table_predicate;
use paimon::spec::{Predicate, Snapshot};
use paimon::table::{HybridSearchRanker, HybridSearchRoute, Table};
use paimon::vector_search::ScoredRowIds;
use paimon_datafusion::runtime::runtime;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::HashMap;
use std::sync::Arc;

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

    #[pyo3(signature = (snapshot_json))]
    fn with_snapshot<'py>(
        mut slf: PyRefMut<'py, Self>,
        snapshot_json: Option<&str>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let snapshot = snapshot_json
            .map(serde_json::from_str)
            .transpose()
            .map_err(|error| {
                pyo3::exceptions::PyValueError::new_err(format!("Invalid snapshot JSON: {error}"))
            })?;
        slf.pinned_snapshot = Some(snapshot);
        Ok(slf)
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

    fn execute_local(&self, py: Python<'_>) -> PyResult<PyHybridSearchResult> {
        let builder = self.builder();
        py.detach(|| runtime().block_on(builder.execute_scored()))
            .map(|inner| PyHybridSearchResult { inner })
            .map_err(to_py_err)
    }
}

#[pyclass(name = "HybridSearchResult", module = "pypaimon_rust.datafusion")]
pub struct PyHybridSearchResult {
    inner: ScoredRowIds,
}

#[pymethods]
impl PyHybridSearchResult {
    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn row_ids(&self) -> HashMap<u64, f32> {
        self.inner
            .row_ids
            .iter()
            .copied()
            .zip(self.inner.scores.iter().copied())
            .collect()
    }
}
