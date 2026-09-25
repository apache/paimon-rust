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

//! Java `FieldMergeMapAgg`: merge maps by key, with later values winning.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{
    new_empty_array, new_null_array, Array, ArrayRef, MapArray, StringArray, StructArray,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType as ArrowDataType, FieldRef, Fields};
use arrow_select::concat::concat;

use super::{unsupported_type_error, FieldAggregator};
use crate::arrow::paimon_type_to_arrow;
use crate::spec::DataType;
use crate::Error;

#[derive(Debug)]
pub(crate) struct MergeMapAgg {
    field_name: String,
    entries_field: FieldRef,
    entry_fields: Fields,
    ordered: bool,
    timestamp_index: Option<usize>,
    seen_input: bool,
    normalized: bool,
    entries: Vec<(ArrayRef, ArrayRef)>,
}

impl MergeMapAgg {
    pub(crate) fn new(field_name: &str, data_type: &DataType) -> crate::Result<Self> {
        if !matches!(data_type, DataType::Map(_)) {
            return Err(unsupported_type_error("merge_map", field_name, data_type));
        }
        let ArrowDataType::Map(entries_field, ordered) = paimon_type_to_arrow(data_type)? else {
            unreachable!("MAP must map to Arrow Map")
        };
        let ArrowDataType::Struct(entry_fields) = entries_field.data_type() else {
            unreachable!("Arrow Map entries must be Struct")
        };
        Ok(Self {
            field_name: field_name.to_string(),
            entries_field: entries_field.clone(),
            entry_fields: entry_fields.clone(),
            ordered,
            timestamp_index: None,
            seen_input: false,
            normalized: false,
            entries: Vec::new(),
        })
    }

    pub(crate) fn new_with_keytime(
        field_name: &str,
        data_type: &DataType,
        options: &HashMap<String, String>,
    ) -> crate::Result<Self> {
        let DataType::Map(map_type) = data_type else {
            return Err(unsupported_type_error(
                "merge_map_with_keytime",
                field_name,
                data_type,
            ));
        };
        let DataType::Row(value_type) = map_type.value_type() else {
            return Err(unsupported_type_error(
                "merge_map_with_keytime",
                field_name,
                data_type,
            ));
        };
        if value_type.fields().len() < 2 {
            return Err(unsupported_type_error(
                "merge_map_with_keytime",
                field_name,
                data_type,
            ));
        }
        let timestamp_index = match options.get(&format!("fields.{field_name}.ts-field")) {
            Some(name) => value_type
                .fields()
                .iter()
                .position(|field| field.name() == name)
                .ok_or_else(|| Error::ConfigInvalid {
                    message: format!(
                        "Timestamp field '{name}' not found in ROW type for field '{field_name}'"
                    ),
                })?,
            None => value_type.fields().len() - 1,
        };
        let mut result = Self::new(field_name, data_type)?;
        result.timestamp_index = Some(timestamp_index);
        Ok(result)
    }

    fn matching_key(&self, key: &dyn Array) -> Option<usize> {
        self.entries
            .iter()
            .position(|(existing, _)| existing.as_ref() == key)
    }

    fn normalize(&mut self) {
        if self.normalized {
            return;
        }
        let old = std::mem::take(&mut self.entries);
        for (key, value) in old {
            if let Some(index) = self.matching_key(key.as_ref()) {
                self.entries[index].1 = value;
            } else {
                self.entries.push((key, value));
            }
        }
        self.normalized = true;
    }

    fn timestamp<'a>(&self, value: &'a dyn Array) -> crate::Result<Option<&'a str>> {
        if value.is_null(0) {
            return Ok(None);
        }
        let index = self.timestamp_index.expect("timestamp mode");
        let row = value
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| Error::DataInvalid {
                message: format!(
                    "merge_map_with_keytime field '{}' requires ROW values",
                    self.field_name
                ),
                source: None,
            })?;
        let column = row.column(index);
        if column.is_null(0) {
            return Ok(None);
        }
        let string = column
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::DataInvalid {
                message: format!(
                    "merge_map_with_keytime timestamp for '{}' requires STRING",
                    self.field_name
                ),
                source: None,
            })?;
        Ok(Some(string.value(0)))
    }

    fn apply_keytime_entry(&mut self, key: ArrayRef, value: ArrayRef) -> crate::Result<()> {
        let existing = self.matching_key(key.as_ref());
        if value.is_null(0) {
            // Java treats a NULL incoming ROW as a key tombstone.
            if let Some(index) = existing {
                self.entries.remove(index);
            }
            return Ok(());
        }
        let Some(new_timestamp) = self.timestamp(value.as_ref())? else {
            return Ok(());
        };
        match existing {
            None => self.entries.push((key, value)),
            Some(index) => {
                let old_timestamp = self.timestamp(self.entries[index].1.as_ref())?;
                if old_timestamp.is_none_or(|old| new_timestamp > old) {
                    self.entries[index].1 = value;
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, array: &dyn Array, row_idx: usize, newer: bool) -> crate::Result<()> {
        if array.is_null(row_idx) {
            return Ok(());
        }
        let map = array
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| Error::DataInvalid {
                message: format!(
                    "merge_map column '{}' requires Arrow Map, got {:?}",
                    self.field_name,
                    array.data_type()
                ),
                source: None,
            })?;
        let start =
            usize::try_from(map.value_offsets()[row_idx]).map_err(|_| Error::DataInvalid {
                message: "Negative Arrow Map offset".into(),
                source: None,
            })?;
        let end =
            usize::try_from(map.value_offsets()[row_idx + 1]).map_err(|_| Error::DataInvalid {
                message: "Negative Arrow Map offset".into(),
                source: None,
            })?;
        let keys = map.entries().column(0);
        let values = map.entries().column(1);
        if end > keys.len() || end > values.len() || start > end {
            return Err(Error::DataInvalid {
                message: "Arrow Map offsets exceed entries".into(),
                source: None,
            });
        }
        let incoming: Vec<_> = (start..end)
            .map(|index| (keys.slice(index, 1), values.slice(index, 1)))
            .collect();
        if !self.seen_input {
            // Java returns the first non-null map unchanged, including its
            // entry order and any duplicate keys.
            self.entries = incoming;
            self.seen_input = true;
            return Ok(());
        }
        self.normalize();
        if self.timestamp_index.is_some() {
            if newer {
                for (key, value) in incoming {
                    self.apply_keytime_entry(key, value)?;
                }
            } else {
                // Java's default aggReversed calls agg(older, accumulator).
                // Start with the older map, then apply the current values as
                // the incoming map so timestamp ties keep the older entry.
                let current = std::mem::replace(&mut self.entries, incoming);
                self.normalized = false;
                self.normalize();
                for (key, value) in current {
                    self.apply_keytime_entry(key, value)?;
                }
            }
        } else {
            for (key, value) in incoming {
                if let Some(existing) = self.matching_key(key.as_ref()) {
                    if newer {
                        self.entries[existing].1 = value;
                    }
                } else {
                    self.entries.push((key, value));
                }
            }
        }
        self.seen_input = true;
        Ok(())
    }

    fn concat_entries(&self, column: usize) -> crate::Result<ArrayRef> {
        if self.entries.is_empty() {
            return Ok(new_empty_array(self.entry_fields[column].data_type()));
        }
        let slices: Vec<&dyn Array> = self
            .entries
            .iter()
            .map(|entry| {
                if column == 0 {
                    entry.0.as_ref()
                } else {
                    entry.1.as_ref()
                }
            })
            .collect();
        concat(&slices).map_err(|e| Error::DataInvalid {
            message: format!(
                "Failed to build merge_map result for '{}': {e}",
                self.field_name
            ),
            source: Some(Box::new(e)),
        })
    }
}

impl FieldAggregator for MergeMapAgg {
    fn name(&self) -> &'static str {
        if self.timestamp_index.is_some() {
            "merge_map_with_keytime"
        } else {
            "merge_map"
        }
    }

    fn reset(&mut self) {
        self.seen_input = false;
        self.normalized = false;
        self.entries.clear();
    }

    fn agg(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        self.merge(array, row_idx, true)
    }

    fn agg_reversed(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        // Java's default aggReversed calls agg(input, accumulator): the
        // current accumulator wins duplicate keys over this older input.
        self.merge(array, row_idx, false)
    }

    fn retract(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        if self.timestamp_index.is_some() {
            return Err(Error::Unsupported {
                message: "merge_map_with_keytime does not support retract".into(),
            });
        }
        if !self.seen_input || array.is_null(row_idx) {
            return Ok(());
        }
        let map = array
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| Error::DataInvalid {
                message: format!("merge_map field '{}' requires Arrow Map", self.field_name),
                source: None,
            })?;
        let start =
            usize::try_from(map.value_offsets()[row_idx]).map_err(|_| Error::DataInvalid {
                message: "Negative Arrow Map offset".into(),
                source: None,
            })?;
        let end =
            usize::try_from(map.value_offsets()[row_idx + 1]).map_err(|_| Error::DataInvalid {
                message: "Negative Arrow Map offset".into(),
                source: None,
            })?;
        let keys = map.entries().column(0);
        if start > end || end > keys.len() {
            return Err(Error::DataInvalid {
                message: "Arrow Map offsets exceed entries".into(),
                source: None,
            });
        }
        self.normalize();
        for index in start..end {
            if let Some(position) = self.matching_key(keys.slice(index, 1).as_ref()) {
                self.entries.remove(position);
            }
        }
        Ok(())
    }

    fn result(&self) -> crate::Result<ArrayRef> {
        let data_type = ArrowDataType::Map(self.entries_field.clone(), self.ordered);
        if !self.seen_input {
            return Ok(new_null_array(&data_type, 1));
        }
        let entries = StructArray::try_new(
            self.entry_fields.clone(),
            vec![self.concat_entries(0)?, self.concat_entries(1)?],
            None,
        )
        .map_err(|e| Error::DataInvalid {
            message: format!(
                "Failed to build merge_map entries for '{}': {e}",
                self.field_name
            ),
            source: Some(Box::new(e)),
        })?;
        let len = i32::try_from(self.entries.len()).map_err(|_| Error::DataInvalid {
            message: format!(
                "merge_map result for '{}' exceeds Arrow Map offset",
                self.field_name
            ),
            source: None,
        })?;
        let result = MapArray::try_new(
            self.entries_field.clone(),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, len])),
            entries,
            None,
            self.ordered,
        )
        .map_err(|e| Error::DataInvalid {
            message: format!(
                "Failed to build merge_map result for '{}': {e}",
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
    use crate::spec::{DataField, IntType, MapType, RowType, VarCharType};
    use arrow_array::{Int32Array, StringArray};

    fn map_type() -> DataType {
        DataType::Map(MapType::new(
            DataType::VarChar(VarCharType::string_type()),
            DataType::Int(IntType::new()),
        ))
    }

    fn input() -> MapArray {
        let ArrowDataType::Map(entries_field, ordered) = paimon_type_to_arrow(&map_type()).unwrap()
        else {
            unreachable!()
        };
        let ArrowDataType::Struct(fields) = entries_field.data_type() else {
            unreachable!()
        };
        let entries = StructArray::try_new(
            fields.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "b", "c"])),
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            ],
            None,
        )
        .unwrap();
        MapArray::try_new(
            entries_field,
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 4])),
            entries,
            None,
            ordered,
        )
        .unwrap()
    }

    fn pairs(result: &dyn Array) -> Vec<(String, i32)> {
        let map = result.as_any().downcast_ref::<MapArray>().unwrap();
        let entries = map.value(0);
        let keys = entries
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = entries
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        (0..entries.len())
            .map(|i| (keys.value(i).to_string(), values.value(i)))
            .collect()
    }

    #[test]
    fn merge_map_newer_and_reversed_older_values() {
        let input = input();
        let mut agg = MergeMapAgg::new("m", &map_type()).unwrap();
        agg.agg(&input, 0).unwrap();
        agg.agg(&input, 1).unwrap();
        assert_eq!(
            pairs(agg.result().unwrap().as_ref()),
            vec![("a".into(), 1), ("b".into(), 3), ("c".into(), 4)]
        );

        agg.reset();
        agg.agg(&input, 1).unwrap();
        agg.agg_reversed(&input, 0).unwrap();
        assert_eq!(
            pairs(agg.result().unwrap().as_ref()),
            vec![("b".into(), 3), ("c".into(), 4), ("a".into(), 1)]
        );
    }

    #[test]
    fn merge_map_retract_removes_keys_regardless_of_values() {
        let input = input();
        let mut agg = MergeMapAgg::new("m", &map_type()).unwrap();
        agg.agg(&input, 0).unwrap();
        agg.agg(&input, 1).unwrap();
        agg.retract(&input, 0).unwrap();
        assert_eq!(pairs(agg.result().unwrap().as_ref()), vec![("c".into(), 4)]);
    }

    #[test]
    fn merge_map_with_keytime_keeps_larger_timestamp() {
        let data_type = DataType::Map(MapType::new(
            DataType::VarChar(VarCharType::string_type()),
            DataType::Row(RowType::new(vec![
                DataField::new(1, "payload".into(), DataType::Int(IntType::new())),
                DataField::new(
                    2,
                    "ts".into(),
                    DataType::VarChar(VarCharType::string_type()),
                ),
            ])),
        ));
        let ArrowDataType::Map(entries_field, ordered) = paimon_type_to_arrow(&data_type).unwrap()
        else {
            unreachable!()
        };
        let ArrowDataType::Struct(entry_fields) = entries_field.data_type() else {
            unreachable!()
        };
        let ArrowDataType::Struct(value_fields) = entry_fields[1].data_type() else {
            unreachable!()
        };
        let values = StructArray::try_new(
            value_fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec![
                    "2024", "2025", "2023", "2026", "2025",
                ])),
            ],
            None,
        )
        .unwrap();
        let entries = StructArray::try_new(
            entry_fields.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a", "b", "c"])),
                Arc::new(values),
            ],
            None,
        )
        .unwrap();
        let input = MapArray::try_new(
            entries_field,
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 5])),
            entries,
            None,
            ordered,
        )
        .unwrap();
        let mut agg = MergeMapAgg::new_with_keytime("m", &data_type, &HashMap::new()).unwrap();
        agg.agg(&input, 0).unwrap();
        agg.agg(&input, 1).unwrap();
        let result = agg.result().unwrap();
        let map = result.as_any().downcast_ref::<MapArray>().unwrap();
        let entries = map.value(0);
        let keys = entries
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = entries
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let payloads = values
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let actual: Vec<_> = (0..entries.len())
            .map(|index| (keys.value(index).to_string(), payloads.value(index)))
            .collect();
        assert_eq!(
            actual,
            vec![("a".into(), 1), ("b".into(), 4), ("c".into(), 5)]
        );
    }
}
