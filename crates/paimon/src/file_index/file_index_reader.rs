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

/// Reader used by the outer format when a writer produced no rows.
pub(crate) struct EmptyFileIndexReader;

impl FileIndexReader for EmptyFileIndexReader {
    fn evaluate(
        &self,
        _column: &str,
        _index: usize,
        _data_type: &DataType,
        operator: PredicateOperator,
        _literals: &[Datum],
    ) -> FileIndexResult {
        match operator {
            PredicateOperator::Eq
            | PredicateOperator::Lt
            | PredicateOperator::LtEq
            | PredicateOperator::Gt
            | PredicateOperator::GtEq
            | PredicateOperator::In
            | PredicateOperator::IsNotNull
            | PredicateOperator::StartsWith
            | PredicateOperator::EndsWith
            | PredicateOperator::Contains => FileIndexResult::Skip,
            _ => FileIndexResult::Remain,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::IntType;

    #[test]
    fn test_empty_reader_matches_java_supported_predicates() {
        let reader = EmptyFileIndexReader;
        let data_type = DataType::Int(IntType::new());

        for operator in [
            PredicateOperator::Eq,
            PredicateOperator::Lt,
            PredicateOperator::LtEq,
            PredicateOperator::Gt,
            PredicateOperator::GtEq,
            PredicateOperator::In,
            PredicateOperator::IsNotNull,
            PredicateOperator::StartsWith,
            PredicateOperator::EndsWith,
            PredicateOperator::Contains,
        ] {
            assert_eq!(
                reader.evaluate("a", 0, &data_type, operator, &[Datum::Int(1)]),
                FileIndexResult::Skip
            );
        }

        for operator in [
            PredicateOperator::IsNull,
            PredicateOperator::NotEq,
            PredicateOperator::NotIn,
            PredicateOperator::ArrayContains,
            PredicateOperator::ArraysOverlap,
            PredicateOperator::ArrayContainsAll,
            PredicateOperator::Like,
            PredicateOperator::Between,
            PredicateOperator::NotBetween,
        ] {
            assert_eq!(
                reader.evaluate("a", 0, &data_type, operator, &[Datum::Int(1)]),
                FileIndexResult::Remain
            );
        }
    }
}
