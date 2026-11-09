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

//! Java `FieldCollectAgg`: concatenate ARRAY elements, optionally distinct
//! and capped by `fields.<col>.count-limit`.

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{new_empty_array, new_null_array, Array, ArrayRef, ListArray};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_row::{RowConverter, Rows, SortField};
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
    /// A positive limit keeps the first N elements in merge order, a
    /// negative one keeps the last |N|.
    count_limit: i32,
    java_uses_equaliser: bool,
    /// Row encoder for distinct elements that Java collects into a
    /// `LinkedHashSet`. `None` for other columns, and for element types
    /// arrow-row cannot encode, which keep the linear comparison.
    key_encoder: Option<RowConverter>,
    seen_input: bool,
    /// The elements are a DELETE value kept as-is, so they may hold
    /// duplicates or exceed the limit until the next merge.
    raw_accumulator: bool,
    elements: Vec<ArrayRef>,
    /// Encoded keys of `elements` when `key_encoder` is set; `None` until
    /// rebuilt after a retract.
    element_keys: Option<HashSet<Box<[u8]>>>,
}

/// One side of Java's merge, with the encoded keys of its elements when
/// the column deduplicates through a set.
struct Operand {
    elements: Vec<ArrayRef>,
    keys: Option<Rows>,
    has_duplicate: bool,
}

impl CollectAgg {
    pub(crate) fn new(
        field_name: &str,
        data_type: &DataType,
        distinct: bool,
        count_limit: i32,
    ) -> crate::Result<Self> {
        let DataType::Array(array_type) = data_type else {
            return Err(unsupported_type_error("collect", field_name, data_type));
        };
        let ArrowDataType::List(element) = paimon_type_to_arrow(data_type)? else {
            unreachable!("ARRAY must map to Arrow List")
        };
        // Java `FieldCollectAgg#needsEqualiser`: constructed types and
        // BINARY/VARBINARY (Rust has no GEOMETRY/GEOGRAPHY).
        let java_uses_equaliser = matches!(
            array_type.element_type(),
            DataType::Array(_)
                | DataType::Vector(_)
                | DataType::Multiset(_)
                | DataType::Map(_)
                | DataType::Row(_)
                | DataType::Binary(_)
                | DataType::VarBinary(_)
        );
        let key_encoder = if distinct && !java_uses_equaliser {
            RowConverter::new(vec![SortField::new(element.data_type().clone())]).ok()
        } else {
            None
        };
        Ok(Self {
            field_name: field_name.to_string(),
            element,
            distinct,
            count_limit,
            java_uses_equaliser,
            key_encoder,
            seen_input: false,
            raw_accumulator: false,
            elements: Vec::new(),
            element_keys: None,
        })
    }

    fn limit(&self) -> usize {
        self.count_limit.unsigned_abs() as usize
    }

    fn list<'a>(&self, array: &'a dyn Array) -> crate::Result<&'a ListArray> {
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
            })
    }

    /// The input row's elements as one array, or `None` for a null cell.
    fn input_values(&self, array: &dyn Array, row_idx: usize) -> crate::Result<Option<ArrayRef>> {
        if array.is_null(row_idx) {
            return Ok(None);
        }
        Ok(Some(self.list(array)?.value(row_idx)))
    }

    fn concat_elements(&self, elements: &[ArrayRef]) -> crate::Result<ArrayRef> {
        if elements.is_empty() {
            return Ok(new_empty_array(self.element.data_type()));
        }
        let slices: Vec<&dyn Array> = elements.iter().map(|e| e.as_ref()).collect();
        concat(&slices).map_err(|e| Error::DataInvalid {
            message: format!(
                "Failed to build collect result for '{}': {e}",
                self.field_name
            ),
            source: Some(Box::new(e)),
        })
    }

    fn encode_keys(&self, values: &ArrayRef) -> crate::Result<Option<Rows>> {
        let Some(encoder) = &self.key_encoder else {
            return Ok(None);
        };
        encoder
            .convert_columns(std::slice::from_ref(values))
            .map(Some)
            .map_err(|e| Error::DataInvalid {
                message: format!(
                    "Failed to encode collect elements for '{}': {e}",
                    self.field_name
                ),
                source: Some(Box::new(e)),
            })
    }
    fn operand(&self, elements: Vec<ArrayRef>, has_duplicate: bool) -> crate::Result<Operand> {
        let keys = if self.key_encoder.is_some() {
            self.encode_keys(&self.concat_elements(&elements)?)?
        } else {
            None
        };
        Ok(Operand {
            elements,
            keys,
            has_duplicate,
        })
    }

    fn input_operand(&self, values: &ArrayRef) -> crate::Result<Operand> {
        Ok(Operand {
            elements: (0..values.len())
                .map(|index| values.slice(index, 1))
                .collect(),
            keys: self.encode_keys(values)?,
            has_duplicate: true,
        })
    }

    /// The accumulator as Java's merge would have collected it so far, so
    /// that the input can be appended without rebuilding the collection.
    fn take_collected(&mut self) -> crate::Result<Merged> {
        let elements = std::mem::take(&mut self.elements);
        let keys = match (self.element_keys.take(), self.key_encoder.is_some()) {
            (Some(keys), _) => Some(keys),
            (None, true) => self
                .encode_keys(&self.concat_elements(&elements)?)?
                .map(|rows| rows.iter().map(|row| row.as_ref().into()).collect()),
            (None, false) => None,
        };
        Ok(Merged { elements, keys })
    }

    /// Java's private `agg(first, firstHasDuplicate, second,
    /// secondHasDuplicate)`: a positive limit collects `first` then
    /// `second`; a negative one collects both backwards, `second` first,
    /// and reverses the result.
    fn merge(&self, first: &Operand, second: &Operand) -> Merged {
        let mut merged = Merged {
            elements: Vec::new(),
            keys: self.key_encoder.as_ref().map(|_| HashSet::new()),
        };
        if self.count_limit >= 0 {
            self.collect(&mut merged, first, false);
            self.collect(&mut merged, second, false);
        } else {
            self.collect(&mut merged, second, true);
            self.collect(&mut merged, first, true);
            merged.elements.reverse();
        }
        merged
    }

    /// Java `collect`: add the operand's elements until the collection
    /// holds |count-limit| of them, skipping duplicates when distinct.
    fn collect(&self, merged: &mut Merged, operand: &Operand, backwards: bool) {
        // Java `checkDuplicateUntil`: with the equaliser, an operand known
        // to be duplicate-free is checked only against what was collected
        // before it.
        let check_until = if operand.has_duplicate {
            usize::MAX
        } else {
            merged.elements.len()
        };
        let len = operand.elements.len();
        for index in (0..len).map(|i| if backwards { len - 1 - i } else { i }) {
            if merged.elements.len() >= self.limit() {
                break;
            }
            let element = &operand.elements[index];
            if self.distinct {
                let duplicate = match (&mut merged.keys, &operand.keys) {
                    (Some(seen), Some(rows)) => !seen.insert(rows.row(index).as_ref().into()),
                    _ => {
                        let checked = if self.java_uses_equaliser {
                            check_until.min(merged.elements.len())
                        } else {
                            merged.elements.len()
                        };
                        merged.elements[..checked]
                            .iter()
                            .any(|existing| existing.as_ref() == element.as_ref())
                    }
                };
                if duplicate {
                    continue;
                }
            }
            merged.elements.push(element.clone());
        }
    }

    fn store(&mut self, merged: Merged) {
        self.elements = merged.elements;
        self.element_keys = merged.keys;
        self.seen_input = true;
        self.raw_accumulator = false;
    }

    /// Java merges a stored DELETE value with a null input like any other
    /// accumulator; anything else is already in merged form.
    fn normalize_raw(&mut self) -> crate::Result<()> {
        if self.raw_accumulator {
            let elements = std::mem::take(&mut self.elements);
            let accumulator = self.operand(elements, false)?;
            let empty = self.operand(Vec::new(), true)?;
            let merged = self.merge(&accumulator, &empty);
            self.store(merged);
        }
        Ok(())
    }
}

struct Merged {
    elements: Vec<ArrayRef>,
    keys: Option<HashSet<Box<[u8]>>>,
}
impl FieldAggregator for CollectAgg {
    fn name(&self) -> &'static str {
        "collect"
    }

    fn reset(&mut self) {
        self.seen_input = false;
        self.raw_accumulator = false;
        self.elements.clear();
        self.element_keys = None;
    }

    /// Java `agg(acc, input)` is `agg(acc, false, input, true)`.
    fn agg(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        let Some(values) = self.input_values(array, row_idx)? else {
            return self.normalize_raw();
        };
        let input = self.input_operand(&values)?;
        let merged = if self.raw_accumulator || self.count_limit < 0 {
            let elements = std::mem::take(&mut self.elements);
            let accumulator = self.operand(elements, false)?;
            self.merge(&accumulator, &input)
        } else {
            // Java would copy the accumulator unchanged before the input,
            // so the input can be appended.
            let mut merged = self.take_collected()?;
            self.collect(&mut merged, &input, false);
            merged
        };
        self.store(merged);
        Ok(())
    }

    fn replace_with_delete(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        // Java keeps the DELETE value as it is until the next merge.
        self.reset();
        if let Some(values) = self.input_values(array, row_idx)? {
            self.elements = (0..values.len())
                .map(|index| values.slice(index, 1))
                .collect();
            self.seen_input = true;
            self.raw_accumulator = true;
        }
        Ok(())
    }

    /// Java `aggReversed(acc, input)` is `agg(input, true, acc, false)`:
    /// the older input comes first.
    fn agg_reversed(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        let Some(values) = self.input_values(array, row_idx)? else {
            return self.normalize_raw();
        };
        let input = self.input_operand(&values)?;
        let elements = std::mem::take(&mut self.elements);
        let accumulator = self.operand(elements, false)?;
        let merged = self.merge(&input, &accumulator);
        self.store(merged);
        Ok(())
    }

    fn retract(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        if !self.seen_input || array.is_null(row_idx) {
            return Ok(());
        }
        let values = self.list(array)?.value(row_idx);
        let mut retracted: Vec<ArrayRef> = (0..values.len())
            .map(|index| values.slice(index, 1))
            .collect();
        self.element_keys = None;
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
        let values = self.concat_elements(&self.elements)?;
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
    use crate::spec::{
        ArrayType, BlobType, DataField, DoubleType, IntType, RowType, VarBinaryType, VariantType,
        VectorType,
    };
    use arrow_array::{
        BinaryArray, FixedSizeListArray, Float64Array, Int32Array, LargeBinaryArray, StructArray,
    };

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
            let mut agg = CollectAgg::new("items", &data_type, distinct, i32::MAX).unwrap();
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
        let mut agg = CollectAgg::new("items", &data_type, true, i32::MAX).unwrap();
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
        let mut agg = CollectAgg::new("items", &data_type, false, i32::MAX).unwrap();
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
    fn ignore_retract_reversed_puts_older_input_first() {
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

    /// Java's ignore-retract wrapper delegates `aggReversed` to collect, so
    /// the older input is deduplicated like any input that may hold
    /// duplicates.
    #[test]
    fn ignore_retract_reversed_deduplicates_older_binary_input() {
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

    fn int_array() -> DataType {
        DataType::Array(ArrayType::new(DataType::Int(IntType::new())))
    }

    fn row_array() -> DataType {
        let field = DataField::new(0, "f0".into(), DataType::Int(IntType::new()));
        DataType::Array(ArrayType::new(DataType::Row(RowType::new(vec![field]))))
    }

    /// A one-row ARRAY<INT> or ARRAY<ROW<INT>> cell.
    fn cell(data_type: &DataType, values: &[Option<i32>]) -> ListArray {
        let ArrowDataType::List(element) = paimon_type_to_arrow(data_type).unwrap() else {
            unreachable!()
        };
        let ints: ArrayRef = Arc::new(Int32Array::from(values.to_vec()));
        let values: ArrayRef = match element.data_type() {
            ArrowDataType::Struct(fields) => {
                Arc::new(StructArray::try_new(fields.clone(), vec![ints], None).unwrap())
            }
            _ => ints,
        };
        ListArray::try_new(
            element,
            OffsetBuffer::from_lengths([values.len()]),
            values,
            None,
        )
        .unwrap()
    }

    fn ints(values: &[i32]) -> Vec<Option<i32>> {
        values.iter().copied().map(Some).collect()
    }

    fn collected(agg: &dyn FieldAggregator) -> Vec<Option<i32>> {
        let result = agg.result().unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list.value(0);
        let values = match values.as_any().downcast_ref::<StructArray>() {
            Some(rows) => rows.column(0).clone(),
            None => values,
        };
        values
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect()
    }

    /// Java `FieldAggregatorTest#testFieldCollectAggWithCountLimit`.
    #[test]
    fn count_limit_matches_java_for_scalar_and_row_elements() {
        let cases: [(bool, i32, &[i32]); 6] = [
            (false, 3, &[2, 1, 2]),
            (true, 3, &[2, 1, 3]),
            (false, -3, &[3, 3, 1]),
            (true, -3, &[2, 3, 1]),
            (false, 0, &[]),
            (true, 0, &[]),
        ];
        for data_type in [int_array(), row_array()] {
            let input = cell(&data_type, &ints(&[2, 1, 2, 3, 3, 1]));
            let older = cell(&data_type, &ints(&[2, 1, 2]));
            let newer = cell(&data_type, &ints(&[3, 3, 1]));
            for (distinct, limit, expected) in cases {
                let context = format!("{data_type:?} distinct={distinct} limit={limit}");
                let new = || CollectAgg::new("items", &data_type, distinct, limit).unwrap();

                let mut single = new();
                single.agg(&input, 0).unwrap();
                assert_eq!(collected(&single), ints(expected), "{context}");

                let mut forward = new();
                forward.agg(&older, 0).unwrap();
                forward.agg(&newer, 0).unwrap();
                assert_eq!(collected(&forward), ints(expected), "{context}");

                let mut reversed = new();
                reversed.agg_reversed(&newer, 0).unwrap();
                reversed.agg_reversed(&older, 0).unwrap();
                assert_eq!(collected(&reversed), ints(expected), "{context}");
            }
        }
    }

    /// Java `aggReversed(acc, input)` is `agg(input, true, acc, false)`.
    #[test]
    fn agg_reversed_puts_older_input_first() {
        for (distinct, expected) in [(false, [1, 2, 2, 3].as_slice()), (true, &[1, 2, 3])] {
            let mut agg = CollectAgg::new("items", &int_array(), distinct, i32::MAX).unwrap();
            agg.agg(&cell(&int_array(), &ints(&[2, 3])), 0).unwrap();
            agg.agg_reversed(&cell(&int_array(), &ints(&[1, 2])), 0)
                .unwrap();
            assert_eq!(collected(&agg), ints(expected), "distinct={distinct}");
        }
    }

    #[test]
    fn count_limit_option_is_read_and_validated() {
        let options = |limit: &str| {
            HashMap::from([("fields.items.count-limit".to_string(), limit.to_string())])
        };
        let mut agg =
            super::super::new_aggregator("collect", "items", &int_array(), &options("2")).unwrap();
        agg.agg(&cell(&int_array(), &ints(&[1, 2, 3])), 0).unwrap();
        assert_eq!(collected(agg.as_ref()), ints(&[1, 2]));
        for invalid in ["abc", "3000000000"] {
            let error =
                super::super::new_aggregator("collect", "items", &int_array(), &options(invalid))
                    .unwrap_err();
            assert!(matches!(error, Error::ConfigInvalid { .. }), "{invalid}");
        }
    }

    /// A DELETE value is kept whole; the next merge applies the limit.
    #[test]
    fn delete_value_is_capped_by_the_next_merge() {
        let raw = cell(&int_array(), &ints(&[1, 2, 3, 4, 5]));
        let null = new_null_array(raw.data_type(), 1);
        for (limit, expected) in [(2, [1, 2]), (-2, [4, 5])] {
            let mut agg = CollectAgg::new("items", &int_array(), false, limit).unwrap();
            agg.replace_with_delete(&raw, 0).unwrap();
            assert_eq!(collected(&agg), ints(&[1, 2, 3, 4, 5]));
            agg.agg(null.as_ref(), 0).unwrap();
            assert_eq!(collected(&agg), ints(&expected), "limit={limit}");
        }
    }

    /// With the equaliser, Java checks the older accumulator only against
    /// the elements collected before it, so its own duplicates survive.
    #[test]
    fn agg_reversed_keeps_duplicates_of_a_binary_delete_value() {
        let data_type = DataType::Array(ArrayType::new(DataType::VarBinary(
            VarBinaryType::new(100).unwrap(),
        )));
        let binary_cell = |values: &[&[u8]]| {
            ListArray::try_new(
                Arc::new(arrow_schema::Field::new(
                    "item",
                    ArrowDataType::Binary,
                    true,
                )),
                OffsetBuffer::from_lengths([values.len()]),
                Arc::new(BinaryArray::from(values.to_vec())),
                None,
            )
            .unwrap()
        };
        let mut agg = CollectAgg::new("items", &data_type, true, i32::MAX).unwrap();
        agg.replace_with_delete(&binary_cell(&[b"a", b"a", b"b"]), 0)
            .unwrap();
        agg.agg_reversed(&binary_cell(&[b"c"]), 0).unwrap();
        let result = agg.result().unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        let values = list.value(0);
        let values = values.as_any().downcast_ref::<BinaryArray>().unwrap();
        let values: Vec<&[u8]> = values.iter().flatten().collect();
        assert_eq!(values, vec![&b"c"[..], b"a", b"a", b"b"]);
    }

    /// The hashed membership check gives the same answers as Arrow `==`,
    /// including after a retract removes elements.
    #[test]
    fn distinct_keys_follow_arrow_equality() {
        let mut agg = CollectAgg::new("items", &int_array(), true, i32::MAX).unwrap();
        agg.agg(&cell(&int_array(), &[Some(1), None, Some(1), Some(2)]), 0)
            .unwrap();
        assert_eq!(collected(&agg), vec![Some(1), None, Some(2)]);
        agg.retract(&cell(&int_array(), &ints(&[1, 2])), 0).unwrap();
        assert_eq!(collected(&agg), vec![None]);
        agg.agg(&cell(&int_array(), &ints(&[1, 2])), 0).unwrap();
        assert_eq!(collected(&agg), vec![None, Some(1), Some(2)]);

        let data_type = DataType::Array(ArrayType::new(DataType::Double(DoubleType::new())));
        let doubles = |values: Vec<f64>| {
            ListArray::try_new(
                Arc::new(arrow_schema::Field::new(
                    "item",
                    ArrowDataType::Float64,
                    true,
                )),
                OffsetBuffer::from_lengths([values.len()]),
                Arc::new(Float64Array::from(values)),
                None,
            )
            .unwrap()
        };
        for (values, expected) in [(vec![f64::NAN, f64::NAN], 1), (vec![0.0, -0.0], 2)] {
            let mut agg = CollectAgg::new("items", &data_type, true, i32::MAX).unwrap();
            agg.agg(&doubles(values.clone()), 0).unwrap();
            let result = agg.result().unwrap();
            let list = result.as_any().downcast_ref::<ListArray>().unwrap();
            assert_eq!(list.value(0).len(), expected, "{values:?}");
        }
    }

    /// Java picks the equaliser from the Paimon element type: VECTOR keeps
    /// a DELETE value's duplicates through a null merge, VARIANT and BLOB
    /// go through a hash set and drop them.
    #[test]
    fn delete_value_duplicates_follow_the_java_element_family() {
        let vector = DataType::Vector(VectorType::new(2, DataType::Int(IntType::new())).unwrap());
        for (element, expected_len) in [
            (vector, 2),
            (DataType::Variant(VariantType::new()), 1),
            (DataType::Blob(BlobType::new()), 1),
        ] {
            let data_type = DataType::Array(ArrayType::new(element.clone()));
            let ArrowDataType::List(field) = paimon_type_to_arrow(&data_type).unwrap() else {
                unreachable!()
            };
            let one: ArrayRef = match field.data_type() {
                ArrowDataType::FixedSizeList(item, size) => Arc::new(
                    FixedSizeListArray::try_new(
                        item.clone(),
                        *size,
                        Arc::new(Int32Array::from(vec![1, 2])),
                        None,
                    )
                    .unwrap(),
                ),
                ArrowDataType::Struct(fields) => {
                    let bytes = || Arc::new(BinaryArray::from(vec![&b"\x01"[..]])) as ArrayRef;
                    Arc::new(
                        StructArray::try_new(fields.clone(), vec![bytes(), bytes()], None).unwrap(),
                    )
                }
                _ => Arc::new(LargeBinaryArray::from(vec![&b"x"[..]])),
            };
            let twice = concat(&[one.as_ref(), one.as_ref()]).unwrap();
            let raw =
                ListArray::try_new(field, OffsetBuffer::from_lengths([2]), twice, None).unwrap();
            let mut agg = CollectAgg::new("items", &data_type, true, i32::MAX).unwrap();
            agg.replace_with_delete(&raw, 0).unwrap();
            agg.agg(new_null_array(raw.data_type(), 1).as_ref(), 0)
                .unwrap();
            let result = agg.result().unwrap();
            let list = result.as_any().downcast_ref::<ListArray>().unwrap();
            assert_eq!(list.value(0).len(), expected_len, "{element:?}");
        }
    }
}
