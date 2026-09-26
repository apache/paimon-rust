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

//! Internal Python bridge for the core upsert key matcher.

use arrow::pyarrow::FromPyArrow;
use arrow::record_batch::RecordBatch;
use paimon::table::UpsertKeyMatcher;
use pyo3::prelude::*;

use crate::error::to_py_err;

type MatchResult = (Vec<usize>, Vec<i64>, Vec<usize>);

#[pyfunction(name = "_match_upsert_keys")]
pub fn match_upsert_keys(
    source: &Bound<'_, PyAny>,
    keys: Vec<String>,
    existing_batches: &Bound<'_, PyAny>,
) -> PyResult<MatchResult> {
    let source = RecordBatch::from_pyarrow_bound(source)?;
    let mut matcher = UpsertKeyMatcher::new(&source, keys).map_err(to_py_err)?;
    for batch in existing_batches.try_iter()? {
        let batch = RecordBatch::from_pyarrow_bound(&batch?)?;
        matcher.add_existing_batch(&batch).map_err(to_py_err)?;
    }
    Ok(matcher.finish())
}
