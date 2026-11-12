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

//! Java `FieldCollectAgg`: concatenate ARRAY elements, optionally distinct,
//! with optional `fields.<col>.count-limit`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{new_empty_array, new_null_array, Array, ArrayRef, ListArray};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType as ArrowDataType, FieldRef};
use arrow_select::concat::concat;

use super::{unsupported_type_error, FieldAggregator};
use crate::arrow::paimon_type_to_arrow;
use crate::spec::DataType;
use crate::Error;

#[derive(Debug)]
pub(crate) struct CollectAgg {
    field_name: String,
    element: FieldRef,
    distinct: bool,
    /// Java `CoreOptions.fieldCollectAggCountLimit`; default `i32::MAX`.
    count_limit: i32,
    java_uses_equaliser: bool,
    seen_input: bool,
    elements: Vec<ArrayRef>,
}

impl CollectAgg {
    pub(crate) fn new(
        field_name: &str,
        data_type: &DataType,
        table_options: &HashMap<String, String>,
    ) -> crate::Result<Self> {
        if !matches!(data_type, DataType::Array(_)) {
            return Err(unsupported_type_error("collect", field_name, data_type));
        }
        let ArrowDataType::List(element) = paimon_type_to_arrow(data_type)? else {
            unreachable!("ARRAY must map to Arrow List")
        };
        let distinct = table_options
            .get(&format!("fields.{field_name}.distinct"))
            .is_some_and(|value| value.eq_ignore_ascii_case("true"));
        let count_limit = match table_options.get(&format!("fields.{field_name}.count-limit")) {
            Some(value) => value.parse::<i32>().map_err(|_| Error::ConfigInvalid {
                message: format!("Invalid collect count-limit '{value}' for field '{field_name}'"),
            })?,
            None => i32::MAX,
        };
        Ok(Self {
            field_name: field_name.to_string(),
            java_uses_equaliser: matches!(
                element.data_type(),
                ArrowDataType::Struct(_)
                    | ArrowDataType::List(_)
                    | ArrowDataType::LargeList(_)
                    | ArrowDataType::Map(_, _)
                    | ArrowDataType::Binary
                    | ArrowDataType::LargeBinary
                    | ArrowDataType::FixedSizeBinary(_)
            ),
            element,
            distinct,
            count_limit,
            seen_input: false,
            elements: Vec::new(),
        })
    }

    fn abs_limit(&self) -> i64 {
        i64::from(self.count_limit).abs()
    }

    fn list_elements(
        &self,
        array: &dyn Array,
        row_idx: usize,
    ) -> crate::Result<Option<Vec<ArrayRef>>> {
        if array.is_null(row_idx) {
            return Ok(None);
        }
        let list =
            array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| Error::DataInvalid {
                    message: format!(
                        "collect column '{}' requires Arrow List, got {:?}",
                        self.field_name,
                        array.data_type()
                    ),
                    source: None,
                })?;
        let values = list.value(row_idx);
        Ok(Some(
            (0..values.len())
                .map(|index| values.slice(index, 1))
                .collect(),
        ))
    }

    fn preserve_raw(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        let Some(elements) = self.list_elements(array, row_idx)? else {
            return Ok(());
        };
        self.elements = elements;
        self.seen_input = true;
        Ok(())
    }

    /// Java `checkDuplicateUntil` for constructed/binary element types.
    fn check_duplicate_until(&self, has_duplicate: bool, collection_len: usize) -> usize {
        if self.distinct && self.java_uses_equaliser {
            if has_duplicate {
                usize::MAX
            } else {
                collection_len
            }
        } else {
            0
        }
    }

    fn contains(
        &self,
        collection: &[ArrayRef],
        element: &ArrayRef,
        check_duplicate_until: usize,
    ) -> bool {
        if self.distinct && !self.java_uses_equaliser {
            // LinkedHashSet path: always full content equality.
            return collection
                .iter()
                .any(|existing| existing.as_ref() == element.as_ref());
        }
        if check_duplicate_until == 0 {
            return false;
        }
        collection
            .iter()
            .take(check_duplicate_until)
            .any(|existing| existing.as_ref() == element.as_ref())
    }

    fn collect_into(
        &self,
        collection: &mut Vec<ArrayRef>,
        elements: &[ArrayRef],
        from_end: bool,
        check_duplicate_until: usize,
    ) {
        let limit = self.abs_limit();
        let mut i = 0usize;
        while i < elements.len() && (collection.len() as i64) < limit {
            let index = if from_end { elements.len() - 1 - i } else { i };
            let element = elements[index].clone();
            if !self.contains(collection, &element, check_duplicate_until) {
                collection.push(element);
            }
            i += 1;
        }
    }

    /// Java `FieldCollectAgg#agg(accumulator, accumulatorHasDuplicate, input, inputHasDuplicate)`.
    fn merge(
        &mut self,
        accumulator: Option<Vec<ArrayRef>>,
        accumulator_has_duplicate: bool,
        input: Option<Vec<ArrayRef>>,
        input_has_duplicate: bool,
    ) {
        if accumulator.is_none() && input.is_none() {
            return;
        }

        let abs_limit = self.abs_limit();
        if let (None, Some(input_elements)) = (&accumulator, &input) {
            if !self.distinct && (input_elements.len() as i64) <= abs_limit {
                self.elements = input_elements.clone();
                self.seen_input = true;
                return;
            }
        }
        if let (Some(acc_elements), None) = (&accumulator, &input) {
            if !self.distinct && (acc_elements.len() as i64) <= abs_limit {
                self.elements = acc_elements.clone();
                self.seen_input = true;
                return;
            }
        }

        let empty = Vec::new();
        let acc_ref = accumulator.as_ref().unwrap_or(&empty);
        let input_ref = input.as_ref().unwrap_or(&empty);
        let mut collection = Vec::new();
        if self.count_limit >= 0 {
            let until = self.check_duplicate_until(accumulator_has_duplicate, collection.len());
            self.collect_into(&mut collection, acc_ref, false, until);
            let until = self.check_duplicate_until(input_has_duplicate, collection.len());
            self.collect_into(&mut collection, input_ref, false, until);
        } else {
            let until = self.check_duplicate_until(input_has_duplicate, collection.len());
            self.collect_into(&mut collection, input_ref, true, until);
            let until = self.check_duplicate_until(accumulator_has_duplicate, collection.len());
            self.collect_into(&mut collection, acc_ref, true, until);
            collection.reverse();
        }
        self.elements = collection;
        self.seen_input = true;
    }

    fn merge_input(
        &mut self,
        array: &dyn Array,
        row_idx: usize,
        input_has_duplicate: bool,
    ) -> crate::Result<()> {
        let input = self.list_elements(array, row_idx)?;
        if !self.seen_input {
            // Java public `agg(null, input)` uses accumulatorHasDuplicate=false.
            self.merge(None, false, input, input_has_duplicate);
            return Ok(());
        }
        // Java public `agg(acc, input)` always passes accumulatorHasDuplicate=false,
        // even when the accumulator array still carries raw duplicates.
        let accumulator = Some(std::mem::take(&mut self.elements));
        self.merge(accumulator, false, input, input_has_duplicate);
        Ok(())
    }
}

impl FieldAggregator for CollectAgg {
    fn name(&self) -> &'static str {
        "collect"
    }

    fn reset(&mut self) {
        self.seen_input = false;
        self.elements.clear();
    }

    fn agg(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        // Java `agg(acc, input)` marks the input side as potentially carrying duplicates.
        self.merge_input(array, row_idx, true)
    }

    fn replace_with_delete(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        self.reset();
        self.preserve_raw(array, row_idx)
    }

    fn agg_reversed(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        // Java FieldCollectAgg#aggReversed(acc, older) =>
        // agg(older, true, acc, false).
        let older = self.list_elements(array, row_idx)?;
        if !self.seen_input {
            self.merge(older, true, None, false);
            return Ok(());
        }
        let current = Some(std::mem::take(&mut self.elements));
        self.merge(older, true, current, false);
        Ok(())
    }

    fn agg_reversed_via_agg(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        // Java FieldIgnoreRetractAgg delegates to CollectAgg#aggReversed.
        // Keep other aggregators on IgnoreRetractAgg::agg_reversed_via_agg
        // unchanged by implementing the CollectAgg override here only.
        self.agg_reversed(array, row_idx)
    }

    fn retract(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        if !self.seen_input || array.is_null(row_idx) {
            return Ok(());
        }
        let list =
            array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| Error::DataInvalid {
                    message: format!("collect column '{}' requires Arrow List", self.field_name),
                    source: None,
                })?;
        let values = list.value(row_idx);
        let mut retracted: Vec<ArrayRef> = (0..values.len())
            .map(|index| values.slice(index, 1))
            .collect();
        self.elements.retain(|candidate| {
            if let Some(position) = retracted
                .iter()
                .position(|value| value.as_ref() == candidate.as_ref())
            {
                retracted.remove(position);
                false
            } else {
                true
            }
        });
        Ok(())
    }

    fn result(&self) -> crate::Result<ArrayRef> {
        if !self.seen_input {
            return Ok(new_null_array(
                &ArrowDataType::List(self.element.clone()),
                1,
            ));
        }
        let values = if self.elements.is_empty() {
            new_empty_array(self.element.data_type())
        } else {
            let slices: Vec<&dyn Array> = self.elements.iter().map(|e| e.as_ref()).collect();
            concat(&slices).map_err(|e| Error::DataInvalid {
                message: format!(
                    "Failed to build collect result for '{}': {e}",
                    self.field_name
                ),
                source: Some(Box::new(e)),
            })?
        };
        let len = i32::try_from(self.elements.len()).map_err(|_| Error::DataInvalid {
            message: format!(
                "collect result for '{}' exceeds Arrow List offset",
                self.field_name
            ),
            source: None,
        })?;
        let result = ListArray::try_new(
            self.element.clone(),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, len])),
            values,
            None,
        )
        .map_err(|e| Error::DataInvalid {
            message: format!(
                "Failed to build collect result for '{}': {e}",
                self.field_name
            ),
            source: Some(Box::new(e)),
        })?;
        Ok(Arc::new(result))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::spec::{ArrayType, IntType, VarBinaryType};
    use arrow_array::{BinaryArray, Int32Array};

    fn input() -> ListArray {
        let field = Arc::new(arrow_schema::Field::new("item", ArrowDataType::Int32, true));
        ListArray::try_new(
            field,
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 4])),
            Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(2), Some(3)])),
            None,
        )
        .unwrap()
    }

    fn collect_options(distinct: bool) -> HashMap<String, String> {
        HashMap::from([("fields.items.distinct".into(), distinct.to_string())])
    }

    #[test]
    fn collect_preserves_duplicates_and_distinct_removes_them() {
        let data_type = DataType::Array(ArrayType::new(DataType::Int(IntType::new())));
        let input = input();
        for (distinct, expected) in [(false, vec![1, 2, 2, 3]), (true, vec![1, 2, 3])] {
            let mut agg = CollectAgg::new("items", &data_type, &collect_options(distinct)).unwrap();
            agg.agg(&input, 0).unwrap();
            agg.agg(&input, 1).unwrap();
            let result = agg.result().unwrap();
            let list = result.as_any().downcast_ref::<ListArray>().unwrap();
            let values = list.value(0);
            let values = values.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(values.values().to_vec(), expected);

            agg.reset();
            agg.agg_reversed(&input, 1).unwrap();
            let result = agg.result().unwrap();
            let list = result.as_any().downcast_ref::<ListArray>().unwrap();
            let values = list.value(0);
            let values = values.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(values.values().to_vec(), vec![2, 3]);
        }
    }

    #[test]
    fn delete_replacement_keeps_raw_elements_until_java_would_rebuild_them() {
        let data_type = DataType::Array(ArrayType::new(DataType::Int(IntType::new())));
        let field = Arc::new(arrow_schema::Field::new("item", ArrowDataType::Int32, true));
        let raw = ListArray::try_new(
            field,
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 3])),
            Arc::new(Int32Array::from(vec![1, 1, 2])),
            None,
        )
        .unwrap();
        let null = new_null_array(raw.data_type(), 1);
        let values = |agg: &CollectAgg| {
            let result = agg.result().unwrap();
            let list = result.as_any().downcast_ref::<ListArray>().unwrap();
            let elements = list.value(0);
            elements
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        };
        let mut agg = CollectAgg::new("items", &data_type, &collect_options(true)).unwrap();
        agg.replace_with_delete(&raw, 0).unwrap();
        assert_eq!(values(&agg), vec![1, 1, 2]);
        agg.agg(null.as_ref(), 0).unwrap();
        assert_eq!(values(&agg), vec![1, 2]);

        agg.replace_with_delete(&raw, 0).unwrap();
        let absent = ListArray::try_new(
            Arc::new(arrow_schema::Field::new("item", ArrowDataType::Int32, true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
            Arc::new(Int32Array::from(vec![99])),
            None,
        )
        .unwrap();
        agg.retract(&absent, 0).unwrap();
        assert_eq!(values(&agg), vec![1, 1, 2]);
    }

    #[test]
    fn collect_retract_removes_only_matching_occurrences() {
        let data_type = DataType::Array(ArrayType::new(DataType::Int(IntType::new())));
        let input = input();
        let mut agg = CollectAgg::new("items", &data_type, &collect_options(false)).unwrap();
        agg.agg(&input, 0).unwrap();
        agg.agg(&input, 1).unwrap();
        agg.retract(&input, 1).unwrap();
        let result = agg.result().unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list.value(0);
        let values = values.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(values.values().to_vec(), vec![1, 2]);
    }

    #[test]
    fn ignore_retract_reversed_delegates_to_collect_agg_reversed() {
        // Java FieldIgnoreRetractAgg calls CollectAgg#aggReversed.
        let data_type = DataType::Array(ArrayType::new(DataType::Int(IntType::new())));
        let input = input();
        for (distinct, expected) in [(false, vec![1, 2, 2, 3]), (true, vec![1, 2, 3])] {
            let options = HashMap::from([
                ("fields.items.ignore-retract".into(), "true".into()),
                ("fields.items.distinct".into(), distinct.to_string()),
            ]);
            let mut agg =
                super::super::new_aggregator("collect", "items", &data_type, &options).unwrap();
            agg.agg(&input, 1).unwrap();
            agg.agg_reversed(&input, 0).unwrap();
            let result = agg.result().unwrap();
            let list = result.as_any().downcast_ref::<ListArray>().unwrap();
            let values = list.value(0);
            let values = values.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(values.values().to_vec(), expected);
        }
    }

    #[test]
    fn ignore_retract_reversed_dedups_binary_older_via_collect_agg_reversed() {
        // CollectAgg#aggReversed marks older hasDuplicate=true, so equaliser
        // distinct collapses duplicates inside the older operand first.
        let data_type = DataType::Array(ArrayType::new(DataType::VarBinary(
            VarBinaryType::new(100).unwrap(),
        )));
        let input = ListArray::try_new(
            Arc::new(arrow_schema::Field::new(
                "item",
                ArrowDataType::Binary,
                true,
            )),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 3])),
            Arc::new(BinaryArray::from(vec![
                Some(&b"a"[..]),
                Some(&b"a"[..]),
                Some(&b"b"[..]),
            ])),
            None,
        )
        .unwrap();
        let options = HashMap::from([
            ("fields.items.ignore-retract".into(), "true".into()),
            ("fields.items.distinct".into(), "true".into()),
        ]);
        let mut agg =
            super::super::new_aggregator("collect", "items", &data_type, &options).unwrap();
        agg.agg(&input, 1).unwrap();
        agg.agg_reversed(&input, 0).unwrap();
        let result = agg.result().unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        let elements = list.value(0);
        let elements = elements.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(
            (0..elements.len())
                .map(|i| elements.value(i))
                .collect::<Vec<_>>(),
            vec![&b"a"[..], &b"b"[..]]
        );
    }

    #[test]
    fn replace_with_delete_then_agg_keeps_raw_binary_duplicates_like_java() {
        // Java public agg(acc, input) always uses accumulatorHasDuplicate=false,
        // so a raw delete payload with duplicates is kept until the input side
        // is merged with full dedup against that accumulator.
        let data_type = DataType::Array(ArrayType::new(DataType::VarBinary(
            VarBinaryType::new(100).unwrap(),
        )));
        let field = Arc::new(arrow_schema::Field::new(
            "item",
            ArrowDataType::Binary,
            true,
        ));
        let raw = ListArray::try_new(
            field.clone(),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2])),
            Arc::new(BinaryArray::from(vec![Some(&b"a"[..]), Some(&b"a"[..])])),
            None,
        )
        .unwrap();
        let newer = ListArray::try_new(
            field,
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
            Arc::new(BinaryArray::from(vec![Some(&b"b"[..])])),
            None,
        )
        .unwrap();
        let options = HashMap::from([("fields.items.distinct".into(), "true".into())]);
        let mut binary =
            super::super::new_aggregator("collect", "items", &data_type, &options).unwrap();
        binary.replace_with_delete(&raw, 0).unwrap();
        binary.agg(&newer, 0).unwrap();
        let result = binary.result().unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        let elements_array = list.value(0);
        let elements = elements_array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(
            (0..elements.len())
                .map(|i| elements.value(i))
                .collect::<Vec<_>>(),
            vec![&b"a"[..], &b"a"[..], &b"b"[..]]
        );

        let int_type = DataType::Array(ArrayType::new(DataType::Int(IntType::new())));
        let int_raw = list_of(&[1, 1]);
        let int_newer = list_of(&[2]);
        let mut ints =
            super::super::new_aggregator("collect", "items", &int_type, &options).unwrap();
        ints.replace_with_delete(&int_raw, 0).unwrap();
        ints.agg(&int_newer, 0).unwrap();
        let result = ints.result().unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list
            .value(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec();
        // INT uses LinkedHashSet semantics, so raw duplicates collapse on rebuild.
        assert_eq!(values, vec![1, 2]);
    }

    /// Mirrors Java `FieldAggregatorTest#testFieldCollectAggWithCountLimit`.
    fn list_of(values: &[i32]) -> ListArray {
        let field = Arc::new(arrow_schema::Field::new("item", ArrowDataType::Int32, true));
        ListArray::try_new(
            field,
            OffsetBuffer::new(ScalarBuffer::from(vec![0, values.len() as i32])),
            Arc::new(Int32Array::from(
                values.iter().copied().map(Some).collect::<Vec<_>>(),
            )),
            None,
        )
        .unwrap()
    }

    fn assert_collect_count_limit(distinct: bool, count_limit: i32, expected: &[i32]) {
        let data_type = DataType::Array(ArrayType::new(DataType::Int(IntType::new())));
        let options = HashMap::from([
            ("fields.items.distinct".into(), distinct.to_string()),
            ("fields.items.count-limit".into(), count_limit.to_string()),
        ]);
        let input = list_of(&[2, 1, 2, 3, 3, 1]);
        let older = list_of(&[2, 1, 2]);
        let newer = list_of(&[3, 3, 1]);

        let values_of = |agg: &mut Box<dyn super::super::FieldAggregator>| {
            let result = agg.result().unwrap();
            let list = result.as_any().downcast_ref::<ListArray>().unwrap();
            if list.is_null(0) {
                return Vec::new();
            }
            list.value(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        };

        let mut single =
            super::super::new_aggregator("collect", "items", &data_type, &options).unwrap();
        single.agg(&input, 0).unwrap();
        assert_eq!(
            values_of(&mut single),
            expected,
            "single-shot distinct={distinct} limit={count_limit}"
        );

        let mut forward =
            super::super::new_aggregator("collect", "items", &data_type, &options).unwrap();
        forward.agg(&older, 0).unwrap();
        forward.agg(&newer, 0).unwrap();
        assert_eq!(
            values_of(&mut forward),
            expected,
            "forward distinct={distinct} limit={count_limit}"
        );

        let mut reversed =
            super::super::new_aggregator("collect", "items", &data_type, &options).unwrap();
        reversed.agg_reversed(&newer, 0).unwrap();
        reversed.agg_reversed(&older, 0).unwrap();
        assert_eq!(
            values_of(&mut reversed),
            expected,
            "reversed distinct={distinct} limit={count_limit}"
        );
    }

    #[test]
    fn collect_count_limit_matches_java_matrix() {
        assert_collect_count_limit(false, 3, &[2, 1, 2]);
        assert_collect_count_limit(true, 3, &[2, 1, 3]);
        assert_collect_count_limit(false, -3, &[3, 3, 1]);
        assert_collect_count_limit(true, -3, &[2, 3, 1]);
        assert_collect_count_limit(false, 0, &[]);
        assert_collect_count_limit(true, 0, &[]);
    }

    #[test]
    fn collect_count_limit_rejects_invalid_option() {
        let data_type = DataType::Array(ArrayType::new(DataType::Int(IntType::new())));
        let options = HashMap::from([("fields.items.count-limit".into(), "x".into())]);
        let err =
            super::super::new_aggregator("collect", "items", &data_type, &options).unwrap_err();
        assert!(
            matches!(err, Error::ConfigInvalid { .. }),
            "expected ConfigInvalid, got {err:?}"
        );
    }

    #[test]
    fn collect_null_input_keeps_limited_accumulator() {
        let data_type = DataType::Array(ArrayType::new(DataType::Int(IntType::new())));
        let options = HashMap::from([("fields.items.count-limit".into(), "2".into())]);
        let mut agg =
            super::super::new_aggregator("collect", "items", &data_type, &options).unwrap();
        agg.agg(&list_of(&[1, 2, 3]), 0).unwrap();
        let null = new_null_array(
            &ArrowDataType::List(Arc::new(arrow_schema::Field::new(
                "item",
                ArrowDataType::Int32,
                true,
            ))),
            1,
        );
        agg.agg(null.as_ref(), 0).unwrap();
        let result = agg.result().unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list
            .value(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(values, vec![1, 2]);
    }

    fn row_list_of(values: &[Option<i32>]) -> ListArray {
        use arrow_array::StructArray;
        let row_fields = vec![Arc::new(arrow_schema::Field::new(
            "v",
            ArrowDataType::Int32,
            true,
        ))];
        let rows = StructArray::try_new(
            row_fields.clone().into(),
            vec![Arc::new(Int32Array::from(values.to_vec())) as ArrayRef],
            None,
        )
        .unwrap();
        ListArray::try_new(
            Arc::new(arrow_schema::Field::new(
                "item",
                ArrowDataType::Struct(row_fields.into()),
                true,
            )),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, values.len() as i32])),
            Arc::new(rows),
            None,
        )
        .unwrap()
    }

    fn row_values(agg: &mut Box<dyn super::super::FieldAggregator>) -> Vec<Option<i32>> {
        use arrow_array::StructArray;
        let result = agg.result().unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        if list.is_null(0) {
            return Vec::new();
        }
        let rows_array = list.value(0);
        let rows = rows_array.as_any().downcast_ref::<StructArray>().unwrap();
        let col = rows
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        (0..col.len())
            .map(|i| {
                if col.is_null(i) {
                    None
                } else {
                    Some(col.value(i))
                }
            })
            .collect()
    }

    #[test]
    fn collect_count_limit_row_equaliser_matches_java_matrix() {
        // Java FieldAggregatorTest runs the same count-limit matrix on ROW<INT>.
        let data_type = DataType::Array(ArrayType::new(DataType::Row(crate::spec::RowType::new(
            vec![crate::spec::DataField::new(
                1,
                "v".into(),
                DataType::Int(IntType::new()),
            )],
        ))));
        let cases = [
            (false, 3, vec![Some(2), Some(1), Some(2)]),
            (true, 3, vec![Some(2), Some(1), Some(3)]),
            (false, -3, vec![Some(3), Some(3), Some(1)]),
            (true, -3, vec![Some(2), Some(3), Some(1)]),
        ];
        for (distinct, limit, expected) in cases {
            let options = HashMap::from([
                ("fields.items.distinct".into(), distinct.to_string()),
                ("fields.items.count-limit".into(), limit.to_string()),
            ]);
            let input = row_list_of(&[Some(2), Some(1), Some(2), Some(3), Some(3), Some(1)]);
            let older = row_list_of(&[Some(2), Some(1), Some(2)]);
            let newer = row_list_of(&[Some(3), Some(3), Some(1)]);

            let mut single =
                super::super::new_aggregator("collect", "items", &data_type, &options).unwrap();
            single.agg(&input, 0).unwrap();
            assert_eq!(
                row_values(&mut single),
                expected,
                "single distinct={distinct}"
            );

            let mut forward =
                super::super::new_aggregator("collect", "items", &data_type, &options).unwrap();
            forward.agg(&older, 0).unwrap();
            forward.agg(&newer, 0).unwrap();
            assert_eq!(
                row_values(&mut forward),
                expected,
                "forward distinct={distinct}"
            );

            let mut reversed =
                super::super::new_aggregator("collect", "items", &data_type, &options).unwrap();
            reversed.agg_reversed(&newer, 0).unwrap();
            reversed.agg_reversed(&older, 0).unwrap();
            assert_eq!(
                row_values(&mut reversed),
                expected,
                "reversed distinct={distinct}"
            );
        }
    }

    #[test]
    fn collect_count_limit_row_multi_merge_and_cross_batch_duplicates() {
        let data_type = DataType::Array(ArrayType::new(DataType::Row(crate::spec::RowType::new(
            vec![crate::spec::DataField::new(
                1,
                "v".into(),
                DataType::Int(IntType::new()),
            )],
        ))));
        let options = HashMap::from([
            ("fields.items.distinct".into(), "true".into()),
            ("fields.items.count-limit".into(), "3".into()),
        ]);
        let mut forward =
            super::super::new_aggregator("collect", "items", &data_type, &options).unwrap();
        // First public agg collapses within-input duplicates (inputHasDuplicate=true).
        forward.agg(&row_list_of(&[Some(1), Some(1)]), 0).unwrap();
        // Later batches dedup against the accumulator; cross-batch 1/2 are skipped.
        forward.agg(&row_list_of(&[Some(2), Some(1)]), 0).unwrap();
        forward
            .agg(&row_list_of(&[Some(3), Some(2), Some(4)]), 0)
            .unwrap();
        assert_eq!(row_values(&mut forward), vec![Some(1), Some(2), Some(3)]);

        let neg = HashMap::from([
            ("fields.items.distinct".into(), "true".into()),
            ("fields.items.count-limit".into(), "-3".into()),
        ]);
        let mut reversed =
            super::super::new_aggregator("collect", "items", &data_type, &neg).unwrap();
        reversed
            .agg_reversed(&row_list_of(&[Some(4), Some(3)]), 0)
            .unwrap();
        reversed
            .agg_reversed(&row_list_of(&[Some(2), Some(2)]), 0)
            .unwrap();
        reversed.agg_reversed(&row_list_of(&[Some(1)]), 0).unwrap();
        // Negative limit keeps the last 3 in reverse-merge order: [2, 4, 3].
        assert_eq!(row_values(&mut reversed), vec![Some(2), Some(4), Some(3)]);
    }

    #[test]
    fn collect_distinguishes_null_array_from_null_elements() {
        let data_type = DataType::Array(ArrayType::new(DataType::Int(IntType::new())));
        let options = HashMap::from([
            ("fields.items.distinct".into(), "true".into()),
            ("fields.items.count-limit".into(), "3".into()),
        ]);
        let list_field = Arc::new(arrow_schema::Field::new("item", ArrowDataType::Int32, true));

        // NULL ARRAY input does not create an accumulator.
        let mut agg =
            super::super::new_aggregator("collect", "items", &data_type, &options).unwrap();
        let null_array = new_null_array(&ArrowDataType::List(list_field.clone()), 1);
        agg.agg(null_array.as_ref(), 0).unwrap();
        assert!(agg.result().unwrap().is_null(0));

        // Empty ARRAY is a present empty collection.
        let empty = ListArray::try_new(
            list_field.clone(),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 0])),
            Arc::new(Int32Array::from(Vec::<Option<i32>>::new())),
            None,
        )
        .unwrap();
        agg.reset();
        agg.agg(&empty, 0).unwrap();
        let result = agg.result().unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(!list.is_null(0));
        assert_eq!(list.value(0).len(), 0);

        // ARRAY with NULL elements keeps them; distinct collapses duplicate nulls.
        let with_nulls = ListArray::try_new(
            list_field,
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 4])),
            Arc::new(Int32Array::from(vec![None, Some(1), None, Some(1)])),
            None,
        )
        .unwrap();
        agg.reset();
        agg.agg(&with_nulls, 0).unwrap();
        let result = agg.result().unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values_array = list.value(0);
        let values = values_array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(values.len(), 2);
        assert!(values.is_null(0));
        assert_eq!(values.value(1), 1);
    }

    #[test]
    fn collect_count_limit_min_value_matches_full_concat_order() {
        // i32::MIN uses long abs in the collect loop (Java Math.abs((long))).
        // Final order matches an unlimited forward concat for small inputs.
        let data_type = DataType::Array(ArrayType::new(DataType::Int(IntType::new())));
        let options = HashMap::from([("fields.items.count-limit".into(), i32::MIN.to_string())]);
        let mut agg =
            super::super::new_aggregator("collect", "items", &data_type, &options).unwrap();
        agg.agg(&list_of(&[1, 2]), 0).unwrap();
        agg.agg(&list_of(&[3, 4]), 0).unwrap();
        let result = agg.result().unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list
            .value(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(values, vec![1, 2, 3, 4]);
    }
}
