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

//! Generic "pick a row" aggregators: `last_value`, `first_value`,
//! `last_non_null_value`, `first_non_null_value`.
//!
//! These accept any Paimon type because the accumulator stores a 1-row Arrow
//! slice of the source array rather than a typed scalar.  The merge function
//! feeds rows in user-sequence ascending order, so "last" means the row with
//! the highest sequence and "first" the lowest.
//!
//! Reference: Java `FieldLastValueAgg`, `FieldFirstValueAgg`,
//! `FieldLastNonNullValueAgg`, `FieldFirstNonNullValueAgg` under
//! `org.apache.paimon.mergetree.compact.aggregate`.

use arrow_array::{new_null_array, Array, ArrayRef};
use arrow_schema::DataType as ArrowDataType;

use super::FieldAggregator;
use crate::arrow::paimon_type_to_arrow;
use crate::spec::DataType;

/// What constitutes "a winning row" for a given pick-style aggregator.
#[derive(Clone, Copy, Debug)]
enum PickPolicy {
    /// Replace the winner on every call, including NULL inputs.
    Last,
    /// Keep only the first call; subsequent calls (NULL or otherwise) are
    /// ignored.
    First,
    /// Replace on every non-NULL input.
    LastNonNull,
    /// Keep only the first non-NULL input; later inputs are ignored.
    FirstNonNull,
}

/// Internal accumulator shared by all four pick-style aggregators.  Only the
/// outer typed wrappers (e.g. [`LastValueAgg`]) implement [`FieldAggregator`];
/// this struct exposes inherent methods that the wrappers delegate to.
#[derive(Debug)]
struct PickValueAgg {
    policy: PickPolicy,
    arrow_type: ArrowDataType,
    initialized: bool,
    /// 1-row Arrow array holding the currently-winning value; `None` means
    /// no winning row has been observed yet for the current group.
    winner: Option<ArrayRef>,
}

impl PickValueAgg {
    fn new(policy: PickPolicy, data_type: &DataType) -> crate::Result<Self> {
        Ok(Self {
            policy,
            arrow_type: paimon_type_to_arrow(data_type)?,
            initialized: false,
            winner: None,
        })
    }

    fn reset(&mut self) {
        self.initialized = false;
        self.winner = None;
    }

    fn agg(&mut self, array: &dyn Array, row_idx: usize) {
        let is_null = array.is_null(row_idx);
        match self.policy {
            PickPolicy::Last => {
                self.winner = Some(array.slice(row_idx, 1));
            }
            PickPolicy::First if !self.initialized => {
                self.initialized = true;
                self.winner = Some(array.slice(row_idx, 1));
            }
            PickPolicy::LastNonNull if !is_null => {
                self.winner = Some(array.slice(row_idx, 1));
            }
            PickPolicy::FirstNonNull if !self.initialized && !is_null => {
                self.initialized = true;
                self.winner = Some(array.slice(row_idx, 1));
            }
            _ => {}
        }
    }

    fn agg_reversed(&mut self, array: &dyn Array, row_idx: usize) {
        let is_null = array.is_null(row_idx);
        match self.policy {
            // Java default: last_value.agg(input, accumulator) returns the
            // existing accumulator.
            PickPolicy::Last => {}
            // Java first_value returns the older input once the aggregator has
            // already been initialized by the current accumulator.
            PickPolicy::First => {
                if self.initialized {
                    self.winner = Some(array.slice(row_idx, 1));
                } else {
                    self.initialized = true;
                }
            }
            // Java last_non_null_value keeps a non-null accumulator, otherwise
            // it falls back to the older input.
            PickPolicy::LastNonNull => {
                if self.winner.is_none() && !is_null {
                    self.winner = Some(array.slice(row_idx, 1));
                }
            }
            // Preserve Java's stateful first_non_null_value behavior for
            // aggReversed(accumulator, input) == agg(input, accumulator).
            PickPolicy::FirstNonNull => {
                let current_is_non_null = self
                    .winner
                    .as_ref()
                    .is_some_and(|winner| !winner.is_null(0));
                if !self.initialized && current_is_non_null {
                    self.initialized = true;
                } else {
                    self.winner = Some(array.slice(row_idx, 1));
                }
            }
        }
    }

    fn result(&self) -> ArrayRef {
        match &self.winner {
            Some(arr) => arr.clone(),
            None => new_null_array(&self.arrow_type, 1),
        }
    }
}

macro_rules! pick_agg {
    ($struct_name:ident, $factory_name:literal, $policy:expr) => {
        #[derive(Debug)]
        pub(crate) struct $struct_name(PickValueAgg);

        impl $struct_name {
            pub(crate) fn new(_field_name: &str, data_type: &DataType) -> crate::Result<Self> {
                Ok(Self(PickValueAgg::new($policy, data_type)?))
            }
        }

        impl FieldAggregator for $struct_name {
            fn name(&self) -> &'static str {
                $factory_name
            }
            fn reset(&mut self) {
                self.0.reset();
            }
            fn agg(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
                self.0.agg(array, row_idx);
                Ok(())
            }
            fn replace_with_delete(
                &mut self,
                array: &dyn Array,
                row_idx: usize,
            ) -> crate::Result<()> {
                self.0.winner = Some(array.slice(row_idx, 1));
                Ok(())
            }
            fn agg_reversed(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
                self.0.agg_reversed(array, row_idx);
                Ok(())
            }
            fn retract(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
                match self.0.policy {
                    PickPolicy::Last => {
                        self.0.winner = None;
                        Ok(())
                    }
                    PickPolicy::LastNonNull => {
                        if !array.is_null(row_idx) {
                            self.0.winner = None;
                        }
                        Ok(())
                    }
                    PickPolicy::First | PickPolicy::FirstNonNull => {
                        Err(crate::Error::Unsupported {
                            message: format!(
                                "Aggregate function '{}' does not support retract",
                                self.name()
                            ),
                        })
                    }
                }
            }
            fn result(&self) -> crate::Result<ArrayRef> {
                Ok(self.0.result())
            }
        }
    };
}

pick_agg!(LastValueAgg, "last_value", PickPolicy::Last);
pick_agg!(FirstValueAgg, "first_value", PickPolicy::First);
pick_agg!(
    LastNonNullValueAgg,
    "last_non_null_value",
    PickPolicy::LastNonNull
);
pick_agg!(
    FirstNonNullValueAgg,
    "first_non_null_value",
    PickPolicy::FirstNonNull
);

#[cfg(test)]
mod delete_tests {
    use super::*;
    use arrow_array::StringArray;

    #[test]
    fn delete_replaces_cell_without_resetting_first_non_null_state() {
        let mut agg = FirstNonNullValueAgg::new(
            "value",
            &DataType::VarChar(crate::spec::VarCharType::new(10).unwrap()),
        )
        .unwrap();
        let input = StringArray::from(vec![Some("old"), None, Some("new")]);
        agg.agg(&input, 0).unwrap();
        agg.replace_with_delete(&input, 1).unwrap();
        agg.agg(&input, 2).unwrap();
        assert!(agg.result().unwrap().is_null(0));
    }
}

/// Java's internal `primary-key` function replaces its value for both add and
/// retract inputs. It can also appear explicitly in Java table options.
#[derive(Debug)]
pub(crate) struct PrimaryKeyAgg(PickValueAgg);

impl PrimaryKeyAgg {
    pub(crate) fn new(_field_name: &str, data_type: &DataType) -> crate::Result<Self> {
        Ok(Self(PickValueAgg::new(PickPolicy::Last, data_type)?))
    }
}

impl FieldAggregator for PrimaryKeyAgg {
    fn name(&self) -> &'static str {
        "primary-key"
    }
    fn reset(&mut self) {
        self.0.reset();
    }
    fn agg(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        self.0.agg(array, row_idx);
        Ok(())
    }
    fn agg_reversed(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        self.0.agg_reversed(array, row_idx);
        Ok(())
    }
    fn retract(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        self.0.agg(array, row_idx);
        Ok(())
    }
    fn result(&self) -> crate::Result<ArrayRef> {
        Ok(self.0.result())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{IntType, VarCharType};
    use arrow_array::{Int32Array, StringArray};

    fn collect_i32(arr: ArrayRef) -> Option<i32> {
        let a = arr.as_any().downcast_ref::<Int32Array>().unwrap();
        if a.is_null(0) {
            None
        } else {
            Some(a.value(0))
        }
    }

    fn collect_str(arr: ArrayRef) -> Option<String> {
        let a = arr.as_any().downcast_ref::<StringArray>().unwrap();
        if a.is_null(0) {
            None
        } else {
            Some(a.value(0).to_string())
        }
    }

    #[test]
    fn test_last_value_includes_trailing_null() {
        let mut agg = LastValueAgg::new("v", &DataType::Int(IntType::new())).unwrap();
        let arr = Int32Array::from(vec![Some(1), Some(2), None]);
        for i in 0..arr.len() {
            agg.agg(&arr, i).unwrap();
        }
        // Last row was NULL; last_value preserves it.
        assert_eq!(collect_i32(agg.result().unwrap()), None);
    }

    #[test]
    fn test_first_value_locks_first_row_including_null() {
        let mut agg = FirstValueAgg::new("v", &DataType::Int(IntType::new())).unwrap();
        let arr = Int32Array::from(vec![None, Some(2), Some(3)]);
        for i in 0..arr.len() {
            agg.agg(&arr, i).unwrap();
        }
        assert_eq!(collect_i32(agg.result().unwrap()), None);
    }

    #[test]
    fn test_last_non_null_value_skips_trailing_null() {
        let mut agg = LastNonNullValueAgg::new("v", &DataType::Int(IntType::new())).unwrap();
        let arr = Int32Array::from(vec![Some(1), Some(2), None]);
        for i in 0..arr.len() {
            agg.agg(&arr, i).unwrap();
        }
        assert_eq!(collect_i32(agg.result().unwrap()), Some(2));
    }

    #[test]
    fn test_first_non_null_value_locks_first_non_null() {
        let mut agg = FirstNonNullValueAgg::new("v", &DataType::Int(IntType::new())).unwrap();
        let arr = Int32Array::from(vec![None, Some(5), Some(7)]);
        for i in 0..arr.len() {
            agg.agg(&arr, i).unwrap();
        }
        assert_eq!(collect_i32(agg.result().unwrap()), Some(5));
    }

    #[test]
    fn test_pick_aggregators_handle_string() {
        let dt = DataType::VarChar(VarCharType::new(255).unwrap());
        let arr = StringArray::from(vec![Some("a"), None, Some("c")]);

        let mut last = LastNonNullValueAgg::new("v", &dt).unwrap();
        for i in 0..arr.len() {
            last.agg(&arr, i).unwrap();
        }
        assert_eq!(collect_str(last.result().unwrap()), Some("c".to_string()));

        let mut first = FirstNonNullValueAgg::new("v", &dt).unwrap();
        for i in 0..arr.len() {
            first.agg(&arr, i).unwrap();
        }
        assert_eq!(collect_str(first.result().unwrap()), Some("a".to_string()));
    }

    #[test]
    fn test_empty_group_returns_null_array_of_correct_type() {
        let agg = LastValueAgg::new("v", &DataType::Int(IntType::new())).unwrap();
        let out = agg.result().unwrap();
        assert!(out.is_null(0));
        assert_eq!(out.data_type(), &ArrowDataType::Int32);
    }

    #[test]
    fn test_reset_clears_state() {
        let mut agg = LastValueAgg::new("v", &DataType::Int(IntType::new())).unwrap();
        let arr = Int32Array::from(vec![Some(99)]);
        agg.agg(&arr, 0).unwrap();
        agg.reset();
        assert!(agg.result().unwrap().is_null(0));
    }

    #[test]
    fn test_last_value_reversed_keeps_current_value() {
        let mut agg = LastValueAgg::new("v", &DataType::Int(IntType::new())).unwrap();
        let arr = Int32Array::from(vec![Some(10), Some(5)]);
        agg.agg(&arr, 0).unwrap();
        agg.agg_reversed(&arr, 1).unwrap();
        assert_eq!(collect_i32(agg.result().unwrap()), Some(10));
    }

    #[test]
    fn test_first_value_reversed_uses_older_null() {
        let mut agg = FirstValueAgg::new("v", &DataType::Int(IntType::new())).unwrap();
        let arr = Int32Array::from(vec![Some(10), None]);
        agg.agg(&arr, 0).unwrap();
        agg.agg_reversed(&arr, 1).unwrap();
        assert_eq!(collect_i32(agg.result().unwrap()), None);
    }

    #[test]
    fn test_last_non_null_reversed_fills_only_empty_accumulator() {
        let mut agg = LastNonNullValueAgg::new("v", &DataType::Int(IntType::new())).unwrap();
        let arr = Int32Array::from(vec![None, Some(5), Some(3)]);
        agg.agg(&arr, 0).unwrap();
        agg.agg_reversed(&arr, 1).unwrap();
        agg.agg_reversed(&arr, 2).unwrap();
        assert_eq!(collect_i32(agg.result().unwrap()), Some(5));
    }

    #[test]
    fn test_first_non_null_reversed_matches_java_initialized_state() {
        let mut agg = FirstNonNullValueAgg::new("v", &DataType::Int(IntType::new())).unwrap();
        let arr = Int32Array::from(vec![None, Some(7), Some(5)]);
        agg.agg(&arr, 0).unwrap();
        agg.agg_reversed(&arr, 1).unwrap();
        agg.agg_reversed(&arr, 2).unwrap();
        assert_eq!(collect_i32(agg.result().unwrap()), Some(7));
    }

    #[test]
    fn test_first_non_null_reversed_after_forward_initialization_uses_older_input() {
        let mut agg = FirstNonNullValueAgg::new("v", &DataType::Int(IntType::new())).unwrap();
        let arr = Int32Array::from(vec![Some(10), None, Some(5)]);
        agg.agg(&arr, 0).unwrap();
        agg.agg_reversed(&arr, 1).unwrap();
        assert_eq!(collect_i32(agg.result().unwrap()), None);
        agg.agg_reversed(&arr, 2).unwrap();
        assert_eq!(collect_i32(agg.result().unwrap()), Some(5));
    }
}
