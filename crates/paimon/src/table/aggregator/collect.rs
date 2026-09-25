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

//! Java `FieldCollectAgg`: concatenate ARRAY elements, optionally distinct.

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
    java_uses_equaliser: bool,
    seen_input: bool,
    raw_accumulator: bool,
    elements: Vec<ArrayRef>,
}

impl CollectAgg {
    pub(crate) fn new(
        field_name: &str,
        data_type: &DataType,
        distinct: bool,
    ) -> crate::Result<Self> {
        if !matches!(data_type, DataType::Array(_)) {
            return Err(unsupported_type_error("collect", field_name, data_type));
        }
        let ArrowDataType::List(element) = paimon_type_to_arrow(data_type)? else {
            unreachable!("ARRAY must map to Arrow List")
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
            seen_input: false,
            raw_accumulator: false,
            elements: Vec::new(),
        })
    }

    fn preserve_raw(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        if array.is_null(row_idx) {
            return Ok(());
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
        self.elements = (0..values.len())
            .map(|index| values.slice(index, 1))
            .collect();
        self.seen_input = true;
        self.raw_accumulator = true;
        Ok(())
    }

    fn normalize_raw_for_agg(&mut self) {
        if !self.raw_accumulator || !self.distinct || self.java_uses_equaliser {
            return;
        }
        let mut distinct: Vec<ArrayRef> = Vec::new();
        for element in std::mem::take(&mut self.elements) {
            if !distinct
                .iter()
                .any(|existing| existing.as_ref() == element.as_ref())
            {
                distinct.push(element);
            }
        }
        self.elements = distinct;
        self.raw_accumulator = false;
    }

    fn collect(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        if array.is_null(row_idx) {
            return Ok(());
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
        self.seen_input = true;
        let values = list.value(row_idx);
        for index in 0..values.len() {
            let element = values.slice(index, 1);
            if !self.distinct
                || !self
                    .elements
                    .iter()
                    .any(|existing| existing.as_ref() == element.as_ref())
            {
                self.elements.push(element);
            }
        }
        Ok(())
    }
}

impl FieldAggregator for CollectAgg {
    fn name(&self) -> &'static str {
        "collect"
    }

    fn reset(&mut self) {
        self.seen_input = false;
        self.raw_accumulator = false;
        self.elements.clear();
    }

    fn agg(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        self.normalize_raw_for_agg();
        self.collect(array, row_idx)
    }

    fn replace_with_delete(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        self.reset();
        self.preserve_raw(array, row_idx)
    }

    fn agg_reversed(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        // Java FieldCollectAgg overrides aggReversed to preserve accumulator
        // order (and the distinct state) instead of prepending the input.
        self.normalize_raw_for_agg();
        self.collect(array, row_idx)
    }

    fn agg_reversed_via_agg(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        // FieldIgnoreRetractAgg inherits the default reverse method, so it
        // calls collect.agg(older, current), not collect.aggReversed. Preserve
        // the current elements while prepending the older operand.
        let current = std::mem::take(&mut self.elements);
        let current_seen = self.seen_input;
        self.seen_input = false;
        self.raw_accumulator = false;
        if self.distinct && self.java_uses_equaliser {
            // Java copies the older accumulator unchanged for complex/binary
            // elements, then deduplicates only the newer input against it.
            self.preserve_raw(array, row_idx)?;
        } else {
            self.collect(array, row_idx)?;
        }
        for element in current {
            if !self.distinct
                || !self
                    .elements
                    .iter()
                    .any(|existing| existing.as_ref() == element.as_ref())
            {
                self.elements.push(element);
            }
        }
        self.seen_input |= current_seen;
        self.raw_accumulator = false;
        Ok(())
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

    #[test]
    fn collect_preserves_duplicates_and_distinct_removes_them() {
        let data_type = DataType::Array(ArrayType::new(DataType::Int(IntType::new())));
        let input = input();
        for (distinct, expected) in [(false, vec![1, 2, 2, 3]), (true, vec![1, 2, 3])] {
            let mut agg = CollectAgg::new("items", &data_type, distinct).unwrap();
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
        let mut agg = CollectAgg::new("items", &data_type, true).unwrap();
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
        let mut agg = CollectAgg::new("items", &data_type, false).unwrap();
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
    fn ignore_retract_reversed_uses_default_order_instead_of_collect_override() {
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
    fn ignore_retract_reversed_preserves_duplicate_binary_older_accumulator() {
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
            vec![&b"a"[..], &b"a"[..], &b"b"[..]]
        );
    }
}
