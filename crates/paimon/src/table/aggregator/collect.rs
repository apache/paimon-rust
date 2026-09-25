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
    seen_input: bool,
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
            element,
            distinct,
            seen_input: false,
            elements: Vec::new(),
        })
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
        self.elements.clear();
    }

    fn agg(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        self.collect(array, row_idx)
    }

    fn agg_reversed(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        // Java FieldCollectAgg overrides aggReversed to preserve accumulator
        // order (and the distinct state) instead of prepending the input.
        self.collect(array, row_idx)
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
    use super::*;
    use crate::spec::{ArrayType, IntType};
    use arrow_array::Int32Array;

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
}
