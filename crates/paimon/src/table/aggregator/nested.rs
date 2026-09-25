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

//! `nested_update` and `nested_partial_update` for ARRAY<ROW> fields.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{new_empty_array, new_null_array, Array, ArrayRef, ListArray, StructArray};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_ord::ord::make_comparator;
use arrow_schema::{DataType as ArrowDataType, FieldRef, Fields, SortOptions};
use arrow_select::concat::concat;

use super::{unsupported_type_error, FieldAggregator};
use crate::arrow::paimon_type_to_arrow;
use crate::spec::DataType;
use crate::Error;

#[derive(Clone, Copy, Debug)]
enum NestedMode {
    Update,
    PartialUpdate,
}

#[derive(Clone, Copy, Debug)]
enum NullKeyStrategy {
    Merge,
    Ignore,
    Error,
}

#[derive(Debug)]
pub(crate) struct NestedAgg {
    field_name: String,
    mode: NestedMode,
    element_field: FieldRef,
    row_fields: Fields,
    key_indices: Vec<usize>,
    sequence_indices: Vec<usize>,
    null_key_strategy: NullKeyStrategy,
    count_limit: usize,
    seen_input: bool,
    rows: Vec<ArrayRef>,
}

impl NestedAgg {
    pub(crate) fn new(
        name: &str,
        field_name: &str,
        data_type: &DataType,
        options: &HashMap<String, String>,
    ) -> crate::Result<Self> {
        let mode = match name {
            "nested_update" => NestedMode::Update,
            "nested_partial_update" => NestedMode::PartialUpdate,
            _ => unreachable!("Only nested aggregators call this constructor"),
        };
        let DataType::Array(array_type) = data_type else {
            return Err(unsupported_type_error(name, field_name, data_type));
        };
        let DataType::Row(row_type) = array_type.element_type() else {
            return Err(unsupported_type_error(name, field_name, data_type));
        };
        let ArrowDataType::List(element_field) = paimon_type_to_arrow(data_type)? else {
            unreachable!("ARRAY must map to Arrow List")
        };
        let ArrowDataType::Struct(row_fields) = element_field.data_type() else {
            unreachable!("ARRAY<ROW> must map to List<Struct>")
        };
        let row_fields = row_fields.clone();
        let key_option = format!("fields.{field_name}.nested-key");
        let sequence_option = format!("fields.{field_name}.nested-sequence-field");
        let strategy_option = format!("fields.{field_name}.nested-key-null-strategy");
        let key_indices =
            parse_nested_fields(options.get(&key_option), row_type.fields(), &key_option)?;
        let sequence_indices = parse_nested_fields(
            options.get(&sequence_option),
            row_type.fields(),
            &sequence_option,
        )?;
        if matches!(mode, NestedMode::PartialUpdate) && key_indices.is_empty() {
            return Err(Error::ConfigInvalid {
                message: format!(
                    "nested_partial_update field '{field_name}' requires '{key_option}'"
                ),
            });
        }
        if key_indices.is_empty()
            && (options.contains_key(&strategy_option) || !sequence_indices.is_empty())
        {
            return Err(Error::ConfigInvalid {
                message: format!(
                    "Nested key strategy and sequence fields for '{field_name}' require '{key_option}'"
                ),
            });
        }
        let null_key_strategy = match options.get(&strategy_option).map(String::as_str) {
            None => NullKeyStrategy::Merge,
            Some(value) if value.eq_ignore_ascii_case("merge") => NullKeyStrategy::Merge,
            Some(value) if value.eq_ignore_ascii_case("ignore") => NullKeyStrategy::Ignore,
            Some(value) if value.eq_ignore_ascii_case("error") => NullKeyStrategy::Error,
            Some(value) => {
                return Err(Error::ConfigInvalid {
                    message: format!("Invalid nested-key-null-strategy '{value}'"),
                })
            }
        };
        let count_limit = options
            .get(&format!("fields.{field_name}.count-limit"))
            .map(|value| {
                value
                    .parse::<i32>()
                    .map(|n| n.max(0) as usize)
                    .map_err(|_| Error::ConfigInvalid {
                        message: format!("Invalid nested_update count-limit '{value}'"),
                    })
            })
            .transpose()?
            .unwrap_or(i32::MAX as usize);
        Ok(Self {
            field_name: field_name.to_string(),
            mode,
            element_field,
            row_fields,
            key_indices,
            sequence_indices,
            null_key_strategy,
            count_limit,
            seen_input: false,
            rows: Vec::new(),
        })
    }

    fn row<'a>(&self, value: &'a dyn Array) -> crate::Result<&'a StructArray> {
        value
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| Error::DataInvalid {
                message: format!(
                    "Nested aggregator for '{}' requires Arrow Struct elements",
                    self.field_name
                ),
                source: None,
            })
    }

    fn key_is_valid(&self, row: &StructArray) -> crate::Result<bool> {
        if self
            .key_indices
            .iter()
            .all(|&index| row.column(index).is_valid(0))
        {
            return Ok(true);
        }
        match self.null_key_strategy {
            NullKeyStrategy::Merge => Ok(true),
            NullKeyStrategy::Ignore => Ok(false),
            NullKeyStrategy::Error => Err(Error::DataInvalid {
                message: "Nested key contains null values. Primary key fields must not be null."
                    .into(),
                source: None,
            }),
        }
    }

    fn same_key(&self, left: &StructArray, right: &StructArray) -> bool {
        self.key_indices
            .iter()
            .all(|&index| left.column(index).as_ref() == right.column(index).as_ref())
    }

    fn compare_sequence(&self, left: &StructArray, right: &StructArray) -> crate::Result<Ordering> {
        for &index in &self.sequence_indices {
            let comparator = make_comparator(
                left.column(index).as_ref(),
                right.column(index).as_ref(),
                SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            )
            .map_err(|e| Error::DataInvalid {
                message: format!(
                    "Failed to compare nested sequence for '{}': {e}",
                    self.field_name
                ),
                source: Some(Box::new(e)),
            })?;
            let ordering = comparator(0, 0);
            if !ordering.is_eq() {
                return Ok(ordering);
            }
        }
        Ok(Ordering::Equal)
    }

    fn partial_update(&self, old: &StructArray, new: &StructArray) -> crate::Result<ArrayRef> {
        let columns = (0..self.row_fields.len())
            .map(|index| {
                let column = new.column(index);
                if column.is_valid(0) {
                    column.clone()
                } else {
                    old.column(index).clone()
                }
            })
            .collect();
        let result = StructArray::try_new(self.row_fields.clone(), columns, None).map_err(|e| {
            Error::DataInvalid {
                message: format!(
                    "Failed to build nested partial update for '{}': {e}",
                    self.field_name
                ),
                source: Some(Box::new(e)),
            }
        })?;
        Ok(Arc::new(result))
    }

    fn add_row(&mut self, incoming: ArrayRef, limit_new_keys: bool) -> crate::Result<()> {
        if incoming.is_null(0) {
            return Ok(());
        }
        let row = self.row(incoming.as_ref())?;
        if self.key_indices.is_empty() {
            if !limit_new_keys || self.rows.len() < self.count_limit {
                self.rows.push(incoming);
            }
            return Ok(());
        }
        if !self.key_is_valid(row)? {
            return Ok(());
        }
        let position = self.rows.iter().position(|existing| {
            self.same_key(self.row(existing.as_ref()).expect("stored Struct row"), row)
        });
        match position {
            Some(index) => {
                let existing = self.row(self.rows[index].as_ref())?;
                let replacement = match self.mode {
                    NestedMode::PartialUpdate => self.partial_update(existing, row)?,
                    NestedMode::Update
                        if self.sequence_indices.is_empty()
                            || self.compare_sequence(row, existing)?.is_ge() =>
                    {
                        incoming
                    }
                    NestedMode::Update => return Ok(()),
                };
                self.rows[index] = replacement;
            }
            None if !limit_new_keys
                || matches!(self.mode, NestedMode::PartialUpdate)
                || self.rows.len() < self.count_limit =>
            {
                self.rows.push(incoming)
            }
            None => {}
        }
        Ok(())
    }

    fn consume(
        &mut self,
        array: &dyn Array,
        row_idx: usize,
        limit_new_keys: bool,
    ) -> crate::Result<()> {
        if array.is_null(row_idx) {
            return Ok(());
        }
        let list =
            array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| Error::DataInvalid {
                    message: format!(
                        "Nested aggregator for '{}' requires Arrow List",
                        self.field_name
                    ),
                    source: None,
                })?;
        self.seen_input = true;
        let values = list.value(row_idx);
        for index in 0..values.len() {
            if values.is_valid(index) {
                self.add_row(values.slice(index, 1), limit_new_keys)?;
            }
        }
        Ok(())
    }

    fn preserve_raw_accumulator(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        if array.is_null(row_idx) {
            return Ok(());
        }
        let list =
            array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| Error::DataInvalid {
                    message: format!(
                        "Nested aggregator for '{}' requires Arrow List",
                        self.field_name
                    ),
                    source: None,
                })?;
        let values = list.value(row_idx);
        self.rows = (0..values.len())
            .map(|index| values.slice(index, 1))
            .collect();
        self.seen_input = true;
        Ok(())
    }
}

fn parse_nested_fields(
    option: Option<&String>,
    fields: &[crate::spec::DataField],
    option_name: &str,
) -> crate::Result<Vec<usize>> {
    option
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .map(|name| {
                    fields
                        .iter()
                        .position(|field| field.name() == name)
                        .ok_or_else(|| Error::ConfigInvalid {
                            message: format!(
                                "Nested field '{name}' referenced by '{option_name}' does not exist"
                            ),
                        })
                })
                .collect()
        })
        .unwrap_or_else(|| Ok(Vec::new()))
}

impl FieldAggregator for NestedAgg {
    fn name(&self) -> &'static str {
        match self.mode {
            NestedMode::Update => "nested_update",
            NestedMode::PartialUpdate => "nested_partial_update",
        }
    }

    fn reset(&mut self) {
        self.seen_input = false;
        self.rows.clear();
    }

    fn agg(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        self.consume(array, row_idx, true)
    }

    fn agg_reversed(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        if !self.seen_input {
            // Java agg(older, NULL) returns the older accumulator untouched,
            // including rows beyond count-limit and any null elements.
            return self.preserve_raw_accumulator(array, row_idx);
        }
        let current = std::mem::take(&mut self.rows);
        let current_seen = self.seen_input;
        self.seen_input = false;
        // Java agg(older, current) treats older as an accumulator: count-limit
        // only applies while adding current rows (or new nested keys).
        self.consume(array, row_idx, false)?;
        for row in current {
            self.add_row(row, true)?;
        }
        self.seen_input |= current_seen;
        Ok(())
    }

    fn retract(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        if matches!(self.mode, NestedMode::PartialUpdate) {
            return Err(Error::Unsupported {
                message: "nested_partial_update does not support retract".into(),
            });
        }
        if !self.seen_input || array.is_null(row_idx) {
            return Ok(());
        }
        let list =
            array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| Error::DataInvalid {
                    message: format!(
                        "Nested aggregator for '{}' requires Arrow List",
                        self.field_name
                    ),
                    source: None,
                })?;
        let values = list.value(row_idx);
        for index in 0..values.len() {
            if values.is_null(index) {
                continue;
            }
            let retract = values.slice(index, 1);
            let target = self.row(retract.as_ref())?;
            if !self.key_indices.is_empty() && !self.key_is_valid(target)? {
                continue;
            }
            let key_indices = self.key_indices.clone();
            self.rows.retain(|candidate| {
                let candidate = candidate
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("stored Struct row");
                if key_indices.is_empty() {
                    candidate != target
                } else {
                    !key_indices
                        .iter()
                        .all(|&i| candidate.column(i).as_ref() == target.column(i).as_ref())
                }
            });
        }
        Ok(())
    }

    fn result(&self) -> crate::Result<ArrayRef> {
        let data_type = ArrowDataType::List(self.element_field.clone());
        if !self.seen_input {
            return Ok(new_null_array(&data_type, 1));
        }
        let values = if self.rows.is_empty() {
            new_empty_array(self.element_field.data_type())
        } else {
            let arrays: Vec<&dyn Array> = self.rows.iter().map(|row| row.as_ref()).collect();
            concat(&arrays).map_err(|e| Error::DataInvalid {
                message: format!(
                    "Failed to build nested result for '{}': {e}",
                    self.field_name
                ),
                source: Some(Box::new(e)),
            })?
        };
        let len = i32::try_from(self.rows.len()).map_err(|_| Error::DataInvalid {
            message: format!(
                "Nested result for '{}' exceeds Arrow List offset",
                self.field_name
            ),
            source: None,
        })?;
        let result = ListArray::try_new(
            self.element_field.clone(),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, len])),
            values,
            None,
        )
        .map_err(|e| Error::DataInvalid {
            message: format!(
                "Failed to build nested result for '{}': {e}",
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
    use crate::spec::{ArrayType, DataField, IntType, RowType};
    use arrow_array::Int32Array;
    use arrow_schema::Field;

    fn input() -> (DataType, ListArray) {
        let data_type = DataType::Array(ArrayType::new(DataType::Row(RowType::new(vec![
            DataField::new(1, "id".into(), DataType::Int(IntType::new())),
            DataField::new(2, "value".into(), DataType::Int(IntType::new())),
            DataField::new(3, "seq".into(), DataType::Int(IntType::new())),
        ]))));
        let ArrowDataType::List(element) = paimon_type_to_arrow(&data_type).unwrap() else {
            unreachable!()
        };
        let ArrowDataType::Struct(fields) = element.data_type() else {
            unreachable!()
        };
        let rows = StructArray::try_new(
            fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(1), Some(3)])),
                Arc::new(Int32Array::from(vec![Some(10), Some(20), None, Some(30)])),
                Arc::new(Int32Array::from(vec![Some(2), Some(1), Some(1), Some(3)])),
            ],
            None,
        )
        .unwrap();
        let list = ListArray::try_new(
            Arc::new(Field::new(
                "item",
                ArrowDataType::Struct(fields.clone()),
                true,
            )),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 4])),
            Arc::new(rows),
            None,
        )
        .unwrap();
        (data_type, list)
    }

    type NestedTestRows = (Vec<Option<i32>>, Vec<Option<i32>>, Vec<Option<i32>>);

    fn result_rows(agg: &NestedAgg) -> NestedTestRows {
        let result = agg.result().unwrap();
        let list = result.as_any().downcast_ref::<ListArray>().unwrap();
        let rows = list.value(0);
        let rows = rows.as_any().downcast_ref::<StructArray>().unwrap();
        let values = |index| {
            let col = rows
                .column(index)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            (0..col.len())
                .map(|i| col.is_valid(i).then(|| col.value(i)))
                .collect::<Vec<_>>()
        };
        (values(0), values(1), values(2))
    }

    #[test]
    fn nested_update_respects_key_sequence_and_count_limit() {
        let (data_type, input) = input();
        let options = HashMap::from([
            ("fields.items.nested-key".into(), "id".into()),
            ("fields.items.nested-sequence-field".into(), "seq".into()),
            ("fields.items.count-limit".into(), "2".into()),
        ]);
        let mut agg = NestedAgg::new("nested_update", "items", &data_type, &options).unwrap();
        agg.agg(&input, 0).unwrap();
        agg.agg(&input, 1).unwrap();
        assert_eq!(
            result_rows(&agg),
            (
                vec![Some(1), Some(2)],
                vec![Some(10), Some(20)],
                vec![Some(2), Some(1)],
            )
        );
    }

    #[test]
    fn nested_reverse_keeps_raw_older_accumulator_beyond_count_limit() {
        let (data_type, input) = input();
        let options = HashMap::from([("fields.items.count-limit".into(), "1".into())]);
        let mut agg = NestedAgg::new("nested_update", "items", &data_type, &options).unwrap();
        let null = new_null_array(input.data_type(), 1);
        agg.agg(null.as_ref(), 0).unwrap();
        agg.agg_reversed(&input, 0).unwrap();
        assert_eq!(result_rows(&agg).0, vec![Some(1), Some(2)]);

        agg.reset();
        agg.agg(&input, 1).unwrap();
        agg.agg_reversed(&input, 0).unwrap();
        assert_eq!(result_rows(&agg).0, vec![Some(1), Some(2)]);
    }

    #[test]
    fn nested_partial_update_keeps_non_null_fields_for_matching_key() {
        let (data_type, input) = input();
        let options = HashMap::from([("fields.items.nested-key".into(), "id".into())]);
        let mut agg =
            NestedAgg::new("nested_partial_update", "items", &data_type, &options).unwrap();
        agg.agg(&input, 0).unwrap();
        agg.agg(&input, 1).unwrap();
        assert_eq!(
            result_rows(&agg),
            (
                vec![Some(1), Some(2), Some(3)],
                vec![Some(10), Some(20), Some(30)],
                vec![Some(1), Some(1), Some(3)],
            )
        );
    }

    #[test]
    fn nested_update_retract_removes_matching_keys() {
        let (data_type, input) = input();
        let options = HashMap::from([("fields.items.nested-key".into(), "id".into())]);
        let mut agg = NestedAgg::new("nested_update", "items", &data_type, &options).unwrap();
        agg.agg(&input, 0).unwrap();
        agg.agg(&input, 1).unwrap();
        agg.retract(&input, 0).unwrap();
        assert_eq!(
            result_rows(&agg),
            (vec![Some(3)], vec![Some(30)], vec![Some(3)],)
        );
    }
}
