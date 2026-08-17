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

use crate::file_index::file_index_reader::FileIndexReader;
use crate::file_index::file_index_result::FileIndexResult;
use crate::spec::{DataType, Datum, Predicate, PredicateOperator};

/// Evaluates predicate trees against file index readers grouped by column.
pub(crate) struct FileIndexPredicate {
    column_readers: HashMap<String, Vec<Box<dyn FileIndexReader>>>,
}

impl FileIndexPredicate {
    /// Creates an evaluator from the index readers available for each column.
    pub(crate) fn new(column_readers: HashMap<String, Vec<Box<dyn FileIndexReader>>>) -> Self {
        Self { column_readers }
    }

    /// Evaluates a predicate without reading data outside the supplied indexes.
    pub(crate) fn evaluate(&self, predicate: &Predicate) -> FileIndexResult {
        match predicate {
            Predicate::AlwaysTrue => FileIndexResult::Remain,
            Predicate::AlwaysFalse => FileIndexResult::Skip,
            Predicate::And(children) => self.evaluate_and(children),
            Predicate::Or(children) => self.evaluate_or(children),
            Predicate::Not(inner) => self.evaluate_not(inner),
            Predicate::Leaf {
                column,
                index,
                data_type,
                op,
                literals,
            } => self.evaluate_leaf(column, *index, data_type, *op, literals),
        }
    }

    fn evaluate_and(&self, predicates: &[Predicate]) -> FileIndexResult {
        let mut result = FileIndexResult::Remain;
        for predicate in predicates {
            result = result.and(self.evaluate(predicate));
            if !result.remain() {
                break;
            }
        }
        result
    }

    fn evaluate_or(&self, predicates: &[Predicate]) -> FileIndexResult {
        let mut result = FileIndexResult::Skip;
        for predicate in predicates {
            result = result.or(self.evaluate(predicate));
            if matches!(&result, FileIndexResult::Remain) {
                break;
            }
        }
        result
    }

    fn evaluate_not(&self, predicate: &Predicate) -> FileIndexResult {
        match predicate {
            Predicate::AlwaysTrue => FileIndexResult::Skip,
            Predicate::AlwaysFalse => FileIndexResult::Remain,
            Predicate::Not(inner) => self.evaluate(inner),
            _ => FileIndexResult::Remain,
        }
    }

    fn evaluate_leaf(
        &self,
        column: &str,
        index: usize,
        data_type: &DataType,
        operator: PredicateOperator,
        literals: &[Datum],
    ) -> FileIndexResult {
        let Some(readers) = self.column_readers.get(column) else {
            return FileIndexResult::Remain;
        };

        let mut result = FileIndexResult::Remain;
        for reader in readers {
            result = result.and(reader.evaluate(column, index, data_type, operator, literals));
            if !result.remain() {
                break;
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use roaring::RoaringBitmap;

    use super::*;
    use crate::spec::{DataType, Datum, IntType, PredicateOperator};

    struct MockReader {
        supported_operator: PredicateOperator,
        result: FileIndexResult,
        calls: Arc<AtomicUsize>,
    }

    impl FileIndexReader for MockReader {
        fn evaluate(
            &self,
            _column: &str,
            _index: usize,
            _data_type: &DataType,
            operator: PredicateOperator,
            _literals: &[Datum],
        ) -> FileIndexResult {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if operator == self.supported_operator {
                self.result.clone()
            } else {
                FileIndexResult::Remain
            }
        }
    }

    struct DefaultReader;

    impl FileIndexReader for DefaultReader {}

    struct AssertingReader;

    impl FileIndexReader for AssertingReader {
        fn evaluate(
            &self,
            column: &str,
            index: usize,
            data_type: &DataType,
            operator: PredicateOperator,
            literals: &[Datum],
        ) -> FileIndexResult {
            assert_eq!(column, "a");
            assert_eq!(index, 7);
            assert_eq!(data_type, &int_type());
            assert_eq!(operator, PredicateOperator::Eq);
            assert_eq!(literals, &[Datum::Int(42)]);
            selection([1, 3])
        }
    }

    fn int_type() -> DataType {
        DataType::Int(IntType::new())
    }

    fn leaf(column: &str) -> Predicate {
        leaf_with_operator(column, PredicateOperator::Eq)
    }

    fn leaf_with_operator(column: &str, operator: PredicateOperator) -> Predicate {
        Predicate::Leaf {
            column: column.to_string(),
            index: 7,
            data_type: int_type(),
            op: operator,
            literals: vec![Datum::Int(42)],
        }
    }

    fn selection(rows: impl IntoIterator<Item = u32>) -> FileIndexResult {
        FileIndexResult::Selection(rows.into_iter().collect::<RoaringBitmap>())
    }

    fn mock_reader(result: FileIndexResult, calls: &Arc<AtomicUsize>) -> Box<dyn FileIndexReader> {
        Box::new(MockReader {
            supported_operator: PredicateOperator::Eq,
            result,
            calls: Arc::clone(calls),
        })
    }

    fn evaluator(
        readers: impl IntoIterator<Item = (String, Vec<Box<dyn FileIndexReader>>)>,
    ) -> FileIndexPredicate {
        FileIndexPredicate::new(readers.into_iter().collect())
    }

    #[test]
    fn test_evaluate_constants_and_empty_compounds() {
        let evaluator = FileIndexPredicate::new(HashMap::new());

        assert_eq!(
            evaluator.evaluate(&Predicate::AlwaysTrue),
            FileIndexResult::Remain
        );
        assert_eq!(
            evaluator.evaluate(&Predicate::AlwaysFalse),
            FileIndexResult::Skip
        );
        assert_eq!(
            evaluator.evaluate(&Predicate::And(vec![])),
            FileIndexResult::Remain
        );
        assert_eq!(
            evaluator.evaluate(&Predicate::Or(vec![])),
            FileIndexResult::Skip
        );
        assert_eq!(
            evaluator.evaluate(&Predicate::And(vec![
                Predicate::AlwaysTrue,
                Predicate::AlwaysFalse,
            ])),
            FileIndexResult::Skip
        );
        assert_eq!(
            evaluator.evaluate(&Predicate::Or(vec![
                Predicate::AlwaysFalse,
                Predicate::AlwaysTrue,
            ])),
            FileIndexResult::Remain
        );
    }

    #[test]
    fn test_leaf_passes_existing_predicate_fields_to_reader() {
        let evaluator = evaluator([(
            "a".to_string(),
            vec![Box::new(AssertingReader) as Box<dyn FileIndexReader>],
        )]);

        assert_eq!(evaluator.evaluate(&leaf("a")), selection([1, 3]));
    }

    #[test]
    fn test_missing_reader_and_unsupported_operator_remain() {
        let evaluator = evaluator([
            (
                "a".to_string(),
                vec![Box::new(DefaultReader) as Box<dyn FileIndexReader>],
            ),
            ("empty".to_string(), vec![]),
        ]);

        assert_eq!(
            evaluator.evaluate(&leaf("missing")),
            FileIndexResult::Remain
        );
        assert_eq!(evaluator.evaluate(&leaf("empty")), FileIndexResult::Remain);
        assert_eq!(
            evaluator.evaluate(&leaf_with_operator("a", PredicateOperator::Gt)),
            FileIndexResult::Remain
        );
    }

    #[test]
    fn test_leaf_intersects_reader_selections() {
        let first_calls = Arc::new(AtomicUsize::new(0));
        let second_calls = Arc::new(AtomicUsize::new(0));
        let evaluator = evaluator([(
            "a".to_string(),
            vec![
                mock_reader(selection([1, 2, 3]), &first_calls),
                mock_reader(selection([2, 3, 4]), &second_calls),
            ],
        )]);

        assert_eq!(evaluator.evaluate(&leaf("a")), selection([2, 3]));
        assert_eq!(first_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_leaf_combines_readers_and_short_circuits() {
        let first_calls = Arc::new(AtomicUsize::new(0));
        let second_calls = Arc::new(AtomicUsize::new(0));
        let evaluator = evaluator([(
            "a".to_string(),
            vec![
                mock_reader(FileIndexResult::Skip, &first_calls),
                mock_reader(FileIndexResult::Remain, &second_calls),
            ],
        )]);

        assert_eq!(evaluator.evaluate(&leaf("a")), FileIndexResult::Skip);
        assert_eq!(first_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_recursive_and_or_selection_combination() {
        let a_calls = Arc::new(AtomicUsize::new(0));
        let b_calls = Arc::new(AtomicUsize::new(0));
        let c_calls = Arc::new(AtomicUsize::new(0));
        let evaluator = evaluator([
            (
                "a".to_string(),
                vec![mock_reader(selection([1, 2, 3]), &a_calls)],
            ),
            (
                "b".to_string(),
                vec![mock_reader(selection([2, 3, 4]), &b_calls)],
            ),
            (
                "c".to_string(),
                vec![mock_reader(selection([3, 4]), &c_calls)],
            ),
        ]);
        let predicate = Predicate::And(vec![Predicate::Or(vec![leaf("a"), leaf("b")]), leaf("c")]);

        assert_eq!(evaluator.evaluate(&predicate), selection([3, 4]));
        assert_eq!(a_calls.load(Ordering::SeqCst), 1);
        assert_eq!(b_calls.load(Ordering::SeqCst), 1);
        assert_eq!(c_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_and_short_circuits_remaining_predicates() {
        let first_calls = Arc::new(AtomicUsize::new(0));
        let second_calls = Arc::new(AtomicUsize::new(0));
        let evaluator = evaluator([
            (
                "a".to_string(),
                vec![mock_reader(FileIndexResult::Skip, &first_calls)],
            ),
            (
                "b".to_string(),
                vec![mock_reader(FileIndexResult::Remain, &second_calls)],
            ),
        ]);

        assert_eq!(
            evaluator.evaluate(&Predicate::And(vec![leaf("a"), leaf("b")])),
            FileIndexResult::Skip
        );
        assert_eq!(first_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_or_short_circuits_after_remain() {
        let first_calls = Arc::new(AtomicUsize::new(0));
        let second_calls = Arc::new(AtomicUsize::new(0));
        let evaluator = evaluator([
            (
                "a".to_string(),
                vec![mock_reader(FileIndexResult::Remain, &first_calls)],
            ),
            (
                "b".to_string(),
                vec![mock_reader(FileIndexResult::Skip, &second_calls)],
            ),
        ]);

        assert_eq!(
            evaluator.evaluate(&Predicate::Or(vec![leaf("a"), leaf("b")])),
            FileIndexResult::Remain
        );
        assert_eq!(first_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_not_fails_open_except_for_safe_cases() {
        let calls = Arc::new(AtomicUsize::new(0));
        let evaluator = evaluator([(
            "a".to_string(),
            vec![mock_reader(FileIndexResult::Skip, &calls)],
        )]);

        assert_eq!(
            evaluator.evaluate(&Predicate::Not(Box::new(Predicate::AlwaysTrue))),
            FileIndexResult::Skip
        );
        assert_eq!(
            evaluator.evaluate(&Predicate::Not(Box::new(Predicate::AlwaysFalse))),
            FileIndexResult::Remain
        );
        assert_eq!(
            evaluator.evaluate(&Predicate::Not(Box::new(leaf("a")))),
            FileIndexResult::Remain
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        assert_eq!(
            evaluator.evaluate(&Predicate::Not(Box::new(Predicate::Not(Box::new(leaf(
                "a"
            )))))),
            FileIndexResult::Skip
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
