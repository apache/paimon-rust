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

use std::collections::HashMap;
use std::sync::Arc;

use paimon::spec::Predicate;
use paimon::table::{CompactMetrics, Table};
use paimon_datafusion::runtime::runtime;
use pyo3::exceptions::PyRuntimeWarning;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::error::to_py_err;
use crate::predicate::dict_to_table_predicate;

/// One-shot table compaction, mirroring the `CALL sys.compact` procedure:
/// plan the tasks, execute all of them, then submit everything in a single
/// commit. No commit is made when planning yields no task.
///
/// The builder only captures configuration; the core `CompactBuilder` is
/// created per call, so the handle never borrows the table.
#[pyclass(name = "CompactBuilder", module = "pypaimon_rust.datafusion")]
pub struct PyCompactBuilder {
    table: Arc<Table>,
    partition_filters: Vec<Predicate>,
    commit_user: Option<String>,
}

impl PyCompactBuilder {
    pub(crate) fn new(table: Arc<Table>) -> Self {
        Self {
            table,
            partition_filters: Vec::new(),
            commit_user: None,
        }
    }
}

fn metrics_to_map(tasks: usize, metrics: &CompactMetrics) -> HashMap<&'static str, u64> {
    HashMap::from([
        ("tasks", tasks as u64),
        ("files_removed", metrics.files_removed as u64),
        ("files_added", metrics.files_added as u64),
        ("rows_written", metrics.rows_written),
        ("bytes_written", metrics.bytes_written),
        (
            "changelog_files_added",
            metrics.changelog_files_added as u64,
        ),
        ("changelog_rows_written", metrics.changelog_rows_written),
        ("changelog_bytes_written", metrics.changelog_bytes_written),
    ])
}

#[pymethods]
impl PyCompactBuilder {
    /// Restrict compaction to the partitions matching the predicate.
    /// Repeated calls combine the filters with AND.
    fn with_partition_filter<'py>(
        mut slf: PyRefMut<'py, Self>,
        predicate: &Bound<'_, PyDict>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let predicate = dict_to_table_predicate(predicate, slf.table.schema(), true)?;
        // Validation belongs to the core builder; each filter is checked alone.
        slf.table
            .new_compact_builder()
            .with_partition_filter(predicate.clone())
            .map_err(to_py_err)?;
        slf.partition_filters.push(predicate);
        Ok(slf)
    }

    /// Set the commit identity of the compaction commit. Defaults to a
    /// generated UUID. The snapshot records it as `<commit_user>-compact-<suffix>`.
    fn with_commit_user(
        mut slf: PyRefMut<'_, Self>,
        commit_user: String,
    ) -> PyResult<PyRefMut<'_, Self>> {
        slf.table
            .new_compact_builder()
            .with_commit_user(commit_user.clone())
            .map_err(to_py_err)?;
        slf.commit_user = Some(commit_user);
        Ok(slf)
    }

    /// Plan, execute and commit all compaction tasks.
    ///
    /// When a task fails, the files written by earlier tasks are deleted on a
    /// best-effort basis; a failed deletion is reported as a `RuntimeWarning`
    /// and the task's error is raised. Returns the task count and the write
    /// metrics of the compaction commit.
    fn execute(&self, py: Python<'_>) -> PyResult<HashMap<&'static str, u64>> {
        let mut abort_error = None;
        let result = py.detach(|| {
            runtime().block_on(compact(
                &self.table,
                &self.partition_filters,
                self.commit_user.as_deref(),
                &mut abort_error,
            ))
        });
        if let Some(error) = abort_error {
            warn_abort_failure(py, &self.table, &error);
        }
        result.map_err(to_py_err)
    }
}

async fn compact(
    table: &Table,
    partition_filters: &[Predicate],
    commit_user: Option<&str>,
    abort_error: &mut Option<paimon::Error>,
) -> paimon::Result<HashMap<&'static str, u64>> {
    let mut builder = table.new_compact_builder();
    for filter in partition_filters {
        builder = builder.with_partition_filter(filter.clone())?;
    }
    if let Some(user) = commit_user {
        builder = builder.with_commit_user(user)?;
    }

    let plan = builder.new_planner()?.plan().await?;
    let metrics = if plan.tasks().is_empty() {
        CompactMetrics::default()
    } else {
        let executor = builder.new_executor()?;
        let commit = builder.new_commit()?;
        let mut results = Vec::with_capacity(plan.tasks().len());
        for task in plan.tasks() {
            match executor.execute(task).await {
                Ok(result) => results.push(result),
                Err(error) => {
                    // Nothing has been submitted yet, so earlier outputs are
                    // unreferenced. Keep the task error as the one raised.
                    *abort_error = commit.abort_compact(&results).await.err();
                    return Err(error);
                }
            }
        }
        commit.commit_compact(results).await?
    };
    Ok(metrics_to_map(plan.tasks().len(), &metrics))
}

/// The task error is still raised, so a failing `warnings.warn` (for example
/// under `-W error`) is ignored rather than replacing it.
fn warn_abort_failure(py: Python<'_>, table: &Table, error: &paimon::Error) {
    if let Ok(warnings) = py.import("warnings") {
        let _ = warnings.call_method1(
            "warn",
            (
                format!(
                    "Failed to delete unsubmitted compaction files of table {}: {error}",
                    table.identifier().full_name()
                ),
                py.get_type::<PyRuntimeWarning>(),
            ),
        );
    }
}
