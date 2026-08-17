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

use crate::file_index::file_index_result::FileIndexResult;
use crate::spec::{DataType, Datum, PredicateOperator};

/// Evaluates leaf predicates against one concrete file index.
pub(crate) trait FileIndexReader {
    /// Evaluates the fields carried by [`crate::spec::Predicate::Leaf`].
    ///
    /// Readers must return [`FileIndexResult::Remain`] for unsupported operators.
    fn evaluate(
        &self,
        _column: &str,
        _index: usize,
        _data_type: &DataType,
        _operator: PredicateOperator,
        _literals: &[Datum],
    ) -> FileIndexResult {
        FileIndexResult::Remain
    }
}
