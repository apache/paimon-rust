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

use crate::predicate::{FieldRef, FunctionVisitor};

use super::FileIndexResult;

/// Read file index from serialized bytes. Return true,
/// means we need to search this file, else means needn't.
pub trait FileIndexReader: FunctionVisitor<Target = FileIndexResult> {
    fn visit_is_not_null(&self, _field_ref: &FieldRef) -> FileIndexResult {
        FileIndexResult::Remain
    }

    fn visit_is_null(&self, _field_ref: &FieldRef) -> FileIndexResult {
        FileIndexResult::Remain
    }

    fn visit_starts_with(&self, _field_ref: &FieldRef, _literal: Self::Literal) -> FileIndexResult {
        FileIndexResult::Remain
    }

    fn visit_ends_with(&self, _field_ref: &FieldRef, _literal: Self::Literal) -> FileIndexResult {
        FileIndexResult::Remain
    }

    fn visit_less_than(&self, _field_ref: &FieldRef, _literal: Self::Literal) -> FileIndexResult {
        FileIndexResult::Remain
    }

    fn visit_greater_or_equal(
        &self,
        _field_ref: &FieldRef,
        _literal: Self::Literal,
    ) -> FileIndexResult {
        FileIndexResult::Remain
    }

    fn visit_not_equal(&self, _field_ref: &FieldRef, _literal: Self::Literal) -> FileIndexResult {
        FileIndexResult::Remain
    }

    fn visit_less_or_equal(
        &self,
        _field_ref: &FieldRef,
        _literal: Self::Literal,
    ) -> FileIndexResult {
        FileIndexResult::Remain
    }

    fn visit_equal(&self, _field_ref: &FieldRef, _literal: Self::Literal) -> FileIndexResult {
        FileIndexResult::Remain
    }

    fn visit_greater_than(
        &self,
        _field_ref: &FieldRef,
        _literal: Self::Literal,
    ) -> FileIndexResult {
        FileIndexResult::Remain
    }

    fn visit_in(&self, _field_ref: &FieldRef, _literals: Vec<Self::Literal>) -> FileIndexResult {
        let mut file_index_result = FileIndexResult::Remain;
        for key in _literals {
            file_index_result = match file_index_result {
                FileIndexResult::Remain => FileIndexReader::visit_equal(self, _field_ref, key),
                _ => file_index_result.or(FileIndexReader::visit_equal(self, _field_ref, key)),
            };
        }
        file_index_result
    }

    fn visit_not_in(
        &self,
        _field_ref: &FieldRef,
        _literals: Vec<Self::Literal>,
    ) -> FileIndexResult {
        let mut file_index_result = FileIndexResult::Remain;
        for key in _literals {
            file_index_result = match file_index_result {
                FileIndexResult::Remain => FileIndexReader::visit_not_equal(self, _field_ref, key),
                _ => file_index_result.or(FileIndexReader::visit_not_equal(self, _field_ref, key)),
            };
        }
        file_index_result
    }

    fn visit_and(&self, _children: Vec<FileIndexResult>) -> FileIndexResult {
        panic!("Should not invoke this");
    }

    fn visit_or(&self, _children: Vec<FileIndexResult>) -> FileIndexResult {
        panic!("Should not invoke this");
    }
}
