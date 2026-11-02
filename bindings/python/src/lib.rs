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

use pyo3::prelude::*;

mod arrow_input;
mod blob;
mod blob_uri_reader;
mod catalog_version;
mod commit_callback;
mod context;
mod error;
mod evolution;
mod full_text_search;
mod global_index_result;
mod hybrid_search;
mod index_build;
mod merge_execution;
mod merge_into;
mod merge_types;
mod oss_cpp;
mod predicate;
mod read;
mod schema;
mod search_plan;
mod search_task;
mod shard_update;
mod system_table;
mod table;
mod udf;
mod update_assignment;
mod variant;
mod vector_search;
mod vindex;
mod write;
mod write_routing;
// ---- #285: observability ----
mod partition;
mod snapshot;
mod tag;
mod temporal;

#[pymodule]
fn pypaimon_rust(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    error::register_module(py, m)?;
    context::register_module(py, m)?;
    m.add_function(wrap_pyfunction!(temporal::temporal_match, m)?)?;
    variant::register_module(py, m)?;
    evolution::register_module(m)?;
    Ok(())
}
