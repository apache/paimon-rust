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

use std::fmt::Debug;

use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::{ArrowError, SchemaRef};

/// Per-file inputs for building engine-specific row filters.
pub struct RowFilterContext<'a> {
    /// Physical Arrow schema decoded from this file.
    pub file_schema: &'a SchemaRef,
}

/// A format-neutral row predicate evaluated against its projected Arrow batch.
///
/// File formats adapt this contract to their native filtering mechanism. For
/// example, Parquet wraps it as an Arrow decoder predicate.
pub trait RowFilter: Send + 'static {
    /// Schema of the columns required to evaluate this predicate.
    ///
    /// It must be a projection of [`RowFilterContext::file_schema`] that
    /// preserves the file schema's field order.
    fn projection(&self) -> &SchemaRef;

    /// Evaluates the predicate. Null values are treated as non-matches by
    /// format readers when applying the returned mask.
    fn evaluate(&mut self, batch: RecordBatch) -> Result<BooleanArray, ArrowError>;
}

/// Builds fresh format-neutral row filters for each file decoder.
///
/// Engine integrations can use this hook for expressions that Paimon's native
/// [`crate::spec::Predicate`] cannot represent. The caller must retain an exact
/// post-filter: this hook is an optimization and may return no filter for a
/// particular file.
pub trait RowFilterFactory: Debug + Send + Sync {
    fn create(&self, context: RowFilterContext<'_>) -> crate::Result<Vec<Box<dyn RowFilter>>>;
}
