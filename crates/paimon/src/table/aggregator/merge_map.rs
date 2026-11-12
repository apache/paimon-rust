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

//! Java `FieldMergeMapAgg` / `FieldMergeMapWithKeyTimeAgg`: merge maps by key.
//!
//! `merge_map_with_keytime` keeps the ROW with the newer timestamp per key.
//! Character timestamps follow Java `String.compareTo` (UTF-16 code units).
//! FLOAT/DOUBLE follow Python `>` / `==` (NaN never updates; ±0.0 are equal).

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{
    new_empty_array, new_null_array, Array, ArrayRef, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, MapArray, StringArray,
    StructArray, Time32MillisecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType as ArrowDataType, FieldRef, Fields, TimeUnit};
use arrow_select::concat::concat;

use super::{unsupported_type_error, FieldAggregator};
use crate::arrow::paimon_type_to_arrow;
use crate::spec::DataType;
use crate::Error;

/// Timestamp-field physical kind resolved once at aggregator construction.
#[derive(Debug, Clone)]
enum KeytimeKind {
    Utf8,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Date32,
    Time32Ms,
    Decimal128,
    Timestamp { unit: TimeUnit },
}

#[derive(Debug)]
pub(crate) struct MergeMapAgg {
    field_name: String,
    entries_field: FieldRef,
    entry_fields: Fields,
    ordered: bool,
    timestamp_index: Option<usize>,
    keytime_kind: Option<KeytimeKind>,
    overwrite_on_equal_ts: bool,
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
            keytime_kind: None,
            overwrite_on_equal_ts: false,
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
        let (timestamp_index, keytime_kind) =
            resolve_keytime_config(field_name, data_type, options)?;
        let overwrite_on_equal_ts = options
            .get(&format!("fields.{field_name}.overwrite-on-equal-ts"))
            .is_some_and(|value| value.eq_ignore_ascii_case("true"));
        let mut result = Self::new(field_name, data_type)?;
        result.timestamp_index = Some(timestamp_index);
        result.keytime_kind = Some(keytime_kind);
        result.overwrite_on_equal_ts = overwrite_on_equal_ts;
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

    /// Returns the timestamp column for a single-row ROW value, or `None` when
    /// the ROW / timestamp field is null.
    fn timestamp_column<'a>(&self, value: &'a dyn Array) -> crate::Result<Option<&'a dyn Array>> {
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
        Ok(Some(column.as_ref()))
    }

    fn should_replace_by_timestamp(
        &self,
        new_ts: &dyn Array,
        old_ts: &dyn Array,
    ) -> crate::Result<bool> {
        let kind = self.keytime_kind.as_ref().expect("timestamp mode");
        let cmp = compare_keytime(kind, new_ts, old_ts, &self.field_name)?;
        Ok(match cmp {
            KeytimeCmp::Greater => true,
            KeytimeCmp::Equal => self.overwrite_on_equal_ts,
            KeytimeCmp::Less | KeytimeCmp::Unordered => false,
        })
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
        let Some(new_ts) = self.timestamp_column(value.as_ref())? else {
            return Ok(());
        };
        match existing {
            None => self.entries.push((key, value)),
            Some(index) => match self.timestamp_column(self.entries[index].1.as_ref())? {
                None => self.entries[index].1 = value,
                Some(old_ts) => {
                    if self.should_replace_by_timestamp(new_ts, old_ts)? {
                        self.entries[index].1 = value;
                    }
                }
            },
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
                // the incoming map so timestamp ties keep the older entry when
                // overwrite-on-equal-ts is false; when true, the accumulator
                // (applied second) wins equal ties, matching merge order.
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

/// Schema/runtime type check shared with [`MergeMapAgg::new_with_keytime`].
pub(crate) fn validate_merge_map_with_keytime(
    field_name: &str,
    data_type: &DataType,
    options: &HashMap<String, String>,
) -> crate::Result<()> {
    resolve_keytime_config(field_name, data_type, options).map(|_| ())
}

fn resolve_keytime_config(
    field_name: &str,
    data_type: &DataType,
    options: &HashMap<String, String>,
) -> crate::Result<(usize, KeytimeKind)> {
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
    let ts_field = &value_type.fields()[timestamp_index];
    let keytime_kind = keytime_kind_for_type(field_name, ts_field.name(), ts_field.data_type())?;
    Ok((timestamp_index, keytime_kind))
}

fn keytime_kind_for_type(
    field_name: &str,
    ts_field_name: &str,
    ts_type: &DataType,
) -> crate::Result<KeytimeKind> {
    match ts_type {
        DataType::Char(_) | DataType::VarChar(_) => Ok(KeytimeKind::Utf8),
        DataType::TinyInt(_) => Ok(KeytimeKind::Int8),
        DataType::SmallInt(_) => Ok(KeytimeKind::Int16),
        DataType::Int(_) => Ok(KeytimeKind::Int32),
        DataType::BigInt(_) => Ok(KeytimeKind::Int64),
        DataType::Float(_) => Ok(KeytimeKind::Float32),
        DataType::Double(_) => Ok(KeytimeKind::Float64),
        DataType::Date(_) => Ok(KeytimeKind::Date32),
        DataType::Time(_) => Ok(KeytimeKind::Time32Ms),
        DataType::Decimal(_) => Ok(KeytimeKind::Decimal128),
        DataType::Timestamp(_) | DataType::LocalZonedTimestamp(_) => {
            match paimon_type_to_arrow(ts_type)? {
                ArrowDataType::Timestamp(unit, _) => Ok(KeytimeKind::Timestamp { unit }),
                other => Err(Error::DataInvalid {
                    message: format!(
                        "Timestamp field '{ts_field_name}' for field '{field_name}' \
                         maps to unexpected Arrow type {other:?}"
                    ),
                    source: None,
                }),
            }
        }
        other => Err(Error::ConfigInvalid {
            message: format!(
                "Timestamp field '{ts_field_name}' for field '{field_name}' must be \
                 CHAR/VARCHAR, TINYINT/SMALLINT/INT/BIGINT, FLOAT/DOUBLE, DATE, TIME, \
                 DECIMAL, TIMESTAMP or TIMESTAMP_LTZ, but was {other:?}."
            ),
        }),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeytimeCmp {
    Less,
    Equal,
    Greater,
    /// Python float ordering: any comparison involving NaN is unordered, so
    /// the incoming value must not replace the existing one.
    Unordered,
}

fn compare_keytime(
    kind: &KeytimeKind,
    new_ts: &dyn Array,
    old_ts: &dyn Array,
    field_name: &str,
) -> crate::Result<KeytimeCmp> {
    match kind {
        KeytimeKind::Utf8 => {
            let new_v = downcast::<StringArray>(new_ts, field_name)?.value(0);
            let old_v = downcast::<StringArray>(old_ts, field_name)?.value(0);
            Ok(ordering_to_cmp(utf16_code_unit_cmp(new_v, old_v)))
        }
        KeytimeKind::Int8 => cmp_ord(
            downcast::<Int8Array>(new_ts, field_name)?.value(0),
            downcast::<Int8Array>(old_ts, field_name)?.value(0),
        ),
        KeytimeKind::Int16 => cmp_ord(
            downcast::<Int16Array>(new_ts, field_name)?.value(0),
            downcast::<Int16Array>(old_ts, field_name)?.value(0),
        ),
        KeytimeKind::Int32 => cmp_ord(
            downcast::<Int32Array>(new_ts, field_name)?.value(0),
            downcast::<Int32Array>(old_ts, field_name)?.value(0),
        ),
        KeytimeKind::Int64 => cmp_ord(
            downcast::<Int64Array>(new_ts, field_name)?.value(0),
            downcast::<Int64Array>(old_ts, field_name)?.value(0),
        ),
        KeytimeKind::Float32 => Ok(python_float_cmp(
            downcast::<Float32Array>(new_ts, field_name)?.value(0),
            downcast::<Float32Array>(old_ts, field_name)?.value(0),
        )),
        KeytimeKind::Float64 => Ok(python_float_cmp(
            downcast::<Float64Array>(new_ts, field_name)?.value(0),
            downcast::<Float64Array>(old_ts, field_name)?.value(0),
        )),
        KeytimeKind::Date32 => cmp_ord(
            downcast::<Date32Array>(new_ts, field_name)?.value(0),
            downcast::<Date32Array>(old_ts, field_name)?.value(0),
        ),
        KeytimeKind::Time32Ms => cmp_ord(
            downcast::<Time32MillisecondArray>(new_ts, field_name)?.value(0),
            downcast::<Time32MillisecondArray>(old_ts, field_name)?.value(0),
        ),
        KeytimeKind::Decimal128 => cmp_ord(
            downcast::<Decimal128Array>(new_ts, field_name)?.value(0),
            downcast::<Decimal128Array>(old_ts, field_name)?.value(0),
        ),
        KeytimeKind::Timestamp { unit } => {
            let new_v = timestamp_value(new_ts, *unit, field_name)?;
            let old_v = timestamp_value(old_ts, *unit, field_name)?;
            cmp_ord(new_v, old_v)
        }
    }
}

fn cmp_ord<T: Ord>(new_v: T, old_v: T) -> crate::Result<KeytimeCmp> {
    Ok(ordering_to_cmp(new_v.cmp(&old_v)))
}

fn ordering_to_cmp(ordering: Ordering) -> KeytimeCmp {
    match ordering {
        Ordering::Less => KeytimeCmp::Less,
        Ordering::Equal => KeytimeCmp::Equal,
        Ordering::Greater => KeytimeCmp::Greater,
    }
}

/// Python `>` / `==` for keytime floats: NaN never updates; ±0.0 compare equal.
fn python_float_cmp<T: Copy + PartialEq + PartialOrd>(new_v: T, old_v: T) -> KeytimeCmp {
    if new_v > old_v {
        KeytimeCmp::Greater
    } else if new_v == old_v {
        KeytimeCmp::Equal
    } else if new_v < old_v {
        KeytimeCmp::Less
    } else {
        KeytimeCmp::Unordered
    }
}

/// Java `String.compareTo` / Python `_utf16_code_unit_greater` order.
fn utf16_code_unit_cmp(left: &str, right: &str) -> Ordering {
    left.encode_utf16().cmp(right.encode_utf16())
}

fn timestamp_value(array: &dyn Array, unit: TimeUnit, field_name: &str) -> crate::Result<i64> {
    Ok(match unit {
        TimeUnit::Second => downcast::<TimestampSecondArray>(array, field_name)?.value(0),
        TimeUnit::Millisecond => downcast::<TimestampMillisecondArray>(array, field_name)?.value(0),
        TimeUnit::Microsecond => downcast::<TimestampMicrosecondArray>(array, field_name)?.value(0),
        TimeUnit::Nanosecond => downcast::<TimestampNanosecondArray>(array, field_name)?.value(0),
    })
}

fn downcast<'a, T: Array + 'static>(
    array: &'a dyn Array,
    field_name: &str,
) -> crate::Result<&'a T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| Error::DataInvalid {
            message: format!(
                "merge_map_with_keytime timestamp for '{field_name}' received Arrow array \
                 of unexpected type {:?}; expected {}",
                array.data_type(),
                std::any::type_name::<T>()
            ),
            source: None,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{
        BigIntType, DataField, DateType, DecimalType, DoubleType, FloatType, IntType,
        LocalZonedTimestampType, MapType, RowType, SmallIntType, TimeType, TimestampType,
        TinyIntType, VarCharType,
    };
    use arrow_array::Array;

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

    fn keytime_map_type(ts_type: DataType) -> DataType {
        DataType::Map(MapType::new(
            DataType::VarChar(VarCharType::string_type()),
            DataType::Row(RowType::new(vec![
                DataField::new(1, "payload".into(), DataType::Int(IntType::new())),
                DataField::new(2, "ts".into(), ts_type),
            ])),
        ))
    }

    fn keytime_map(ts_type: &DataType, payloads: Vec<i32>, timestamps: ArrayRef) -> MapArray {
        let data_type = keytime_map_type(ts_type.clone());
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
        let n = payloads.len();
        let keys: Vec<&str> = (0..n).map(|_| "k").collect();
        let values = StructArray::try_new(
            value_fields.clone(),
            vec![Arc::new(Int32Array::from(payloads)), timestamps],
            None,
        )
        .unwrap();
        let entries = StructArray::try_new(
            entry_fields.clone(),
            vec![Arc::new(StringArray::from(keys)), Arc::new(values)],
            None,
        )
        .unwrap();
        // One map per row: each row is a single-entry map so agg sees sequential updates.
        let mut offsets = vec![0i32];
        for i in 1..=n as i32 {
            offsets.push(i);
        }
        MapArray::try_new(
            entries_field,
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            entries,
            None,
            ordered,
        )
        .unwrap()
    }

    fn payload_after_merges(agg: &MergeMapAgg) -> Option<i32> {
        let result = agg.result().unwrap();
        if result.is_null(0) {
            return None;
        }
        let map = result.as_any().downcast_ref::<MapArray>().unwrap();
        if map.value_length(0) == 0 {
            return Some(-1); // empty map sentinel for tombstone tests
        }
        let entries = map.value(0);
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
        Some(payloads.value(0))
    }

    fn merge_two(
        ts_type: DataType,
        first_payload: i32,
        first_ts: ArrayRef,
        second_payload: i32,
        second_ts: ArrayRef,
        options: HashMap<String, String>,
    ) -> i32 {
        let data_type = keytime_map_type(ts_type.clone());
        // Build a 2-row map array with one entry each.
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
        let ts = concat(&[&*first_ts, &*second_ts]).unwrap();
        let values = StructArray::try_new(
            value_fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![first_payload, second_payload])),
                ts,
            ],
            None,
        )
        .unwrap();
        let entries = StructArray::try_new(
            entry_fields.clone(),
            vec![
                Arc::new(StringArray::from(vec!["k", "k"])),
                Arc::new(values),
            ],
            None,
        )
        .unwrap();
        let input = MapArray::try_new(
            entries_field,
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 2])),
            entries,
            None,
            ordered,
        )
        .unwrap();
        let mut agg = MergeMapAgg::new_with_keytime("m", &data_type, &options).unwrap();
        agg.agg(&input, 0).unwrap();
        agg.agg(&input, 1).unwrap();
        payload_after_merges(&agg).unwrap()
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
        let data_type = keytime_map_type(DataType::VarChar(VarCharType::string_type()));
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

    #[test]
    fn keytime_string_utf16_prefers_bmp_over_supplementary() {
        // U+10000 encodes as UTF-16 surrogates D800 DC00 (< U+E000).
        // UTF-8 byte order is the opposite; UTF-16 must keep the BMP value.
        let low = "\u{10000}";
        let high = "\u{E000}";
        let ts = DataType::VarChar(VarCharType::string_type());
        assert_eq!(
            merge_two(
                ts.clone(),
                1,
                Arc::new(StringArray::from(vec![high])),
                2,
                Arc::new(StringArray::from(vec![low])),
                HashMap::new(),
            ),
            1
        );
        assert_eq!(
            merge_two(
                ts,
                1,
                Arc::new(StringArray::from(vec![low])),
                2,
                Arc::new(StringArray::from(vec![high])),
                HashMap::new(),
            ),
            2
        );
    }

    #[test]
    fn keytime_integer_types_keep_newer() {
        let cases: Vec<(DataType, ArrayRef, ArrayRef)> = vec![
            (
                DataType::TinyInt(TinyIntType::new()),
                Arc::new(Int8Array::from(vec![9i8])),
                Arc::new(Int8Array::from(vec![10i8])),
            ),
            (
                DataType::SmallInt(SmallIntType::new()),
                Arc::new(Int16Array::from(vec![9i16])),
                Arc::new(Int16Array::from(vec![10i16])),
            ),
            (
                DataType::Int(IntType::new()),
                Arc::new(Int32Array::from(vec![9])),
                Arc::new(Int32Array::from(vec![10])),
            ),
            (
                DataType::BigInt(BigIntType::new()),
                Arc::new(Int64Array::from(vec![9i64])),
                Arc::new(Int64Array::from(vec![10i64])),
            ),
        ];
        for (ts_type, older, newer) in cases {
            assert_eq!(
                merge_two(ts_type, 1, older, 2, newer, HashMap::new()),
                2,
                "integer keytime should keep newer"
            );
        }
    }

    #[test]
    fn keytime_float_python_semantics() {
        let ts = DataType::Float(FloatType::new());
        assert_eq!(
            merge_two(
                ts.clone(),
                1,
                Arc::new(Float32Array::from(vec![9.5f32])),
                2,
                Arc::new(Float32Array::from(vec![10.25f32])),
                HashMap::new(),
            ),
            2
        );
        // Equal ±0.0 keeps first when overwrite is false.
        assert_eq!(
            merge_two(
                ts.clone(),
                1,
                Arc::new(Float32Array::from(vec![-0.0f32])),
                2,
                Arc::new(Float32Array::from(vec![0.0f32])),
                HashMap::new(),
            ),
            1
        );
        // Incoming NaN must not replace.
        assert_eq!(
            merge_two(
                ts.clone(),
                1,
                Arc::new(Float32Array::from(vec![1.0f32])),
                2,
                Arc::new(Float32Array::from(vec![f32::NAN])),
                HashMap::new(),
            ),
            1
        );
        // Existing NaN is not replaced by a finite value under Python `>`.
        assert_eq!(
            merge_two(
                ts,
                1,
                Arc::new(Float32Array::from(vec![f32::NAN])),
                2,
                Arc::new(Float32Array::from(vec![1.0f32])),
                HashMap::new(),
            ),
            1
        );

        let ts = DataType::Double(DoubleType::new());
        assert_eq!(
            merge_two(
                ts,
                1,
                Arc::new(Float64Array::from(vec![9.5f64])),
                2,
                Arc::new(Float64Array::from(vec![10.25f64])),
                HashMap::new(),
            ),
            2
        );
    }

    #[test]
    fn keytime_date_time_decimal_timestamp() {
        assert_eq!(
            merge_two(
                DataType::Date(DateType::new()),
                1,
                Arc::new(Date32Array::from(vec![1])),
                2,
                Arc::new(Date32Array::from(vec![2])),
                HashMap::new(),
            ),
            2
        );
        assert_eq!(
            merge_two(
                DataType::Time(TimeType::new(3).unwrap()),
                1,
                Arc::new(Time32MillisecondArray::from(vec![1])),
                2,
                Arc::new(Time32MillisecondArray::from(vec![2])),
                HashMap::new(),
            ),
            2
        );
        let decimal = DataType::Decimal(DecimalType::new(10, 2).unwrap());
        let older = Decimal128Array::from(vec![900i128])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let newer = Decimal128Array::from(vec![1000i128])
            .with_precision_and_scale(10, 2)
            .unwrap();
        assert_eq!(
            merge_two(
                decimal,
                1,
                Arc::new(older),
                2,
                Arc::new(newer),
                HashMap::new(),
            ),
            2
        );

        // TIMESTAMP(3) ms
        let ts3 = DataType::Timestamp(TimestampType::new(3).unwrap());
        assert_eq!(
            merge_two(
                ts3,
                1,
                Arc::new(TimestampMillisecondArray::from(vec![1_000i64])),
                2,
                Arc::new(TimestampMillisecondArray::from(vec![1_001i64])),
                HashMap::new(),
            ),
            2
        );
        // TIMESTAMP(6) us
        let ts6 = DataType::Timestamp(TimestampType::new(6).unwrap());
        assert_eq!(
            merge_two(
                ts6,
                1,
                Arc::new(TimestampMicrosecondArray::from(vec![1_000_001i64])),
                2,
                Arc::new(TimestampMicrosecondArray::from(vec![1_000_002i64])),
                HashMap::new(),
            ),
            2
        );
        // TIMESTAMP(9) ns — 1ns difference
        let ts9 = DataType::Timestamp(TimestampType::new(9).unwrap());
        assert_eq!(
            merge_two(
                ts9,
                1,
                Arc::new(TimestampNanosecondArray::from(vec![1_000_000_001i64])),
                2,
                Arc::new(TimestampNanosecondArray::from(vec![1_000_000_002i64])),
                HashMap::new(),
            ),
            2
        );
        // TIMESTAMP_LTZ(9)
        let ltz = DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(9).unwrap());
        assert_eq!(
            merge_two(
                ltz,
                1,
                Arc::new(
                    TimestampNanosecondArray::from(vec![1_000_000_001i64]).with_timezone("UTC")
                ),
                2,
                Arc::new(
                    TimestampNanosecondArray::from(vec![1_000_000_002i64]).with_timezone("UTC")
                ),
                HashMap::new(),
            ),
            2
        );
    }

    #[test]
    fn keytime_overwrite_on_equal_ts() {
        let ts = DataType::Int(IntType::new());
        let mut off = HashMap::new();
        assert_eq!(
            merge_two(
                ts.clone(),
                1,
                Arc::new(Int32Array::from(vec![10])),
                2,
                Arc::new(Int32Array::from(vec![10])),
                off.clone(),
            ),
            1
        );
        off.insert(
            "fields.m.overwrite-on-equal-ts".to_string(),
            "false".to_string(),
        );
        assert_eq!(
            merge_two(
                ts.clone(),
                1,
                Arc::new(Int32Array::from(vec![10])),
                2,
                Arc::new(Int32Array::from(vec![10])),
                off,
            ),
            1
        );
        let mut on = HashMap::new();
        on.insert(
            "fields.m.overwrite-on-equal-ts".to_string(),
            "true".to_string(),
        );
        assert_eq!(
            merge_two(
                ts,
                1,
                Arc::new(Int32Array::from(vec![10])),
                2,
                Arc::new(Int32Array::from(vec![10])),
                on,
            ),
            2
        );
    }

    fn keytime_single_entry_map(data_type: &DataType, payload: i32, ts: i32) -> MapArray {
        let ArrowDataType::Map(entries_field, ordered) = paimon_type_to_arrow(data_type).unwrap()
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
                Arc::new(Int32Array::from(vec![payload])),
                Arc::new(Int32Array::from(vec![ts])),
            ],
            None,
        )
        .unwrap();
        let entries = StructArray::try_new(
            entry_fields.clone(),
            vec![Arc::new(StringArray::from(vec!["k"])), Arc::new(values)],
            None,
        )
        .unwrap();
        MapArray::try_new(
            entries_field,
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
            entries,
            None,
            ordered,
        )
        .unwrap()
    }

    #[test]
    fn keytime_agg_reversed_equal_respects_overwrite() {
        let data_type = keytime_map_type(DataType::Int(IntType::new()));
        let older = keytime_single_entry_map(&data_type, 1, 10);
        let newer_acc = keytime_single_entry_map(&data_type, 2, 10);

        let mut agg = MergeMapAgg::new_with_keytime("m", &data_type, &HashMap::new()).unwrap();
        agg.agg(&newer_acc, 0).unwrap();
        agg.agg_reversed(&older, 0).unwrap();
        // Default: equal keeps older after reversed merge order.
        assert_eq!(payload_after_merges(&agg), Some(1));

        let mut on = HashMap::new();
        on.insert(
            "fields.m.overwrite-on-equal-ts".to_string(),
            "true".to_string(),
        );
        let mut agg = MergeMapAgg::new_with_keytime("m", &data_type, &on).unwrap();
        agg.agg(&newer_acc, 0).unwrap();
        agg.agg_reversed(&older, 0).unwrap();
        // overwrite=true: accumulator applied second wins the equal tie.
        assert_eq!(payload_after_merges(&agg), Some(2));
    }

    #[test]
    fn keytime_agg_reversed_three_stage_equal_greater_lesser() {
        // Three-stage reverse merges: seed with agg, then two agg_reversed calls.
        // Each stage exercises equal / greater / lesser timestamps under both
        // overwrite-on-equal-ts settings.
        let data_type = keytime_map_type(DataType::Int(IntType::new()));
        let map = |payload: i32, ts: i32| keytime_single_entry_map(&data_type, payload, ts);

        // overwrite=false
        // 1) seed (100, ts=10)
        // 2) reversed equal (200, ts=10) → keep 200 (older input wins equal)
        // 3) reversed greater (300, ts=20) → keep 300 (incoming older is newer by ts)
        // 4) reversed lesser (400, ts=5) → keep 300 (accumulator ts wins)
        let mut agg = MergeMapAgg::new_with_keytime("m", &data_type, &HashMap::new()).unwrap();
        agg.agg(&map(100, 10), 0).unwrap();
        agg.agg_reversed(&map(200, 10), 0).unwrap();
        assert_eq!(payload_after_merges(&agg), Some(200));
        agg.agg_reversed(&map(300, 20), 0).unwrap();
        assert_eq!(payload_after_merges(&agg), Some(300));
        agg.agg_reversed(&map(400, 5), 0).unwrap();
        assert_eq!(payload_after_merges(&agg), Some(300));

        // overwrite=true
        // 1) seed (100, ts=10)
        // 2) reversed equal (200, ts=10) → keep 100 (accumulator applied second wins)
        // 3) reversed greater (300, ts=20) → keep 300
        // 4) reversed lesser (400, ts=5) → keep 300
        // 5) reversed equal (500, ts=20) → keep 300 (accumulator wins equal)
        let mut on = HashMap::new();
        on.insert(
            "fields.m.overwrite-on-equal-ts".to_string(),
            "true".to_string(),
        );
        let mut agg = MergeMapAgg::new_with_keytime("m", &data_type, &on).unwrap();
        agg.agg(&map(100, 10), 0).unwrap();
        agg.agg_reversed(&map(200, 10), 0).unwrap();
        assert_eq!(payload_after_merges(&agg), Some(100));
        agg.agg_reversed(&map(300, 20), 0).unwrap();
        assert_eq!(payload_after_merges(&agg), Some(300));
        agg.agg_reversed(&map(400, 5), 0).unwrap();
        assert_eq!(payload_after_merges(&agg), Some(300));
        agg.agg_reversed(&map(500, 20), 0).unwrap();
        assert_eq!(payload_after_merges(&agg), Some(300));
    }

    #[test]
    fn keytime_timestamp_ltz_compares_ticks_not_timezone_metadata() {
        // Table schemas map TIMESTAMP_LTZ to Arrow Timestamp(..., "UTC"), so
        // MapArray construction cannot mix timezone labels in one concat. The
        // comparator itself must ignore timezone metadata and use ticks only.
        let kind = KeytimeKind::Timestamp {
            unit: TimeUnit::Nanosecond,
        };
        let utc = TimestampNanosecondArray::from(vec![1_000_000_000i64]).with_timezone("UTC");
        let la = TimestampNanosecondArray::from(vec![1_000_000_000i64])
            .with_timezone("America/Los_Angeles");
        assert_eq!(
            compare_keytime(&kind, &la, &utc, "m").unwrap(),
            KeytimeCmp::Equal
        );

        let lower_name_higher_ticks =
            TimestampNanosecondArray::from(vec![1_000_000_001i64]).with_timezone("UTC");
        let higher_name_lower_ticks =
            TimestampNanosecondArray::from(vec![1_000_000_000i64]).with_timezone("Zulu");
        assert_eq!(
            compare_keytime(
                &kind,
                &lower_name_higher_ticks,
                &higher_name_lower_ticks,
                "m"
            )
            .unwrap(),
            KeytimeCmp::Greater
        );

        // End-to-end MAP retention still uses schema-compatible UTC labels.
        let ltz = DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(9).unwrap());
        assert_eq!(
            merge_two(
                ltz,
                1,
                Arc::new(
                    TimestampNanosecondArray::from(vec![1_000_000_000i64]).with_timezone("UTC")
                ),
                2,
                Arc::new(
                    TimestampNanosecondArray::from(vec![1_000_000_001i64]).with_timezone("UTC")
                ),
                HashMap::new(),
            ),
            2
        );
    }

    #[test]
    fn keytime_null_timestamp_and_tombstone() {
        let ts_type = DataType::Int(IntType::new());
        let data_type = keytime_map_type(ts_type.clone());
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

        // Row0: payload=1, ts=10; Row1: payload=2, ts=NULL → keep 1
        let values = StructArray::try_new(
            value_fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![Some(10), None])),
            ],
            None,
        )
        .unwrap();
        let entries = StructArray::try_new(
            entry_fields.clone(),
            vec![
                Arc::new(StringArray::from(vec!["k", "k"])),
                Arc::new(values),
            ],
            None,
        )
        .unwrap();
        let input = MapArray::try_new(
            entries_field.clone(),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 2])),
            entries,
            None,
            ordered,
        )
        .unwrap();
        let mut agg = MergeMapAgg::new_with_keytime("m", &data_type, &HashMap::new()).unwrap();
        agg.agg(&input, 0).unwrap();
        agg.agg(&input, 1).unwrap();
        assert_eq!(payload_after_merges(&agg), Some(1));

        // Existing NULL ts replaced by valid ts.
        let values = StructArray::try_new(
            value_fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![None, Some(10)])),
            ],
            None,
        )
        .unwrap();
        let entries = StructArray::try_new(
            entry_fields.clone(),
            vec![
                Arc::new(StringArray::from(vec!["k", "k"])),
                Arc::new(values),
            ],
            None,
        )
        .unwrap();
        let input = MapArray::try_new(
            entries_field.clone(),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 2])),
            entries,
            None,
            ordered,
        )
        .unwrap();
        let mut agg = MergeMapAgg::new_with_keytime("m", &data_type, &HashMap::new()).unwrap();
        agg.agg(&input, 0).unwrap();
        agg.agg(&input, 1).unwrap();
        assert_eq!(payload_after_merges(&agg), Some(2));

        // NULL ROW tombstone removes the key.
        let values = StructArray::try_new(
            value_fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None])),
                Arc::new(Int32Array::from(vec![Some(10), None])),
            ],
            Some(vec![true, false].into()),
        )
        .unwrap();
        let entries = StructArray::try_new(
            entry_fields.clone(),
            vec![
                Arc::new(StringArray::from(vec!["k", "k"])),
                Arc::new(values),
            ],
            None,
        )
        .unwrap();
        let input = MapArray::try_new(
            entries_field,
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 2])),
            entries,
            None,
            ordered,
        )
        .unwrap();
        let mut agg = MergeMapAgg::new_with_keytime("m", &data_type, &HashMap::new()).unwrap();
        agg.agg(&input, 0).unwrap();
        agg.agg(&input, 1).unwrap();
        assert_eq!(payload_after_merges(&agg), Some(-1));
    }

    #[test]
    fn keytime_null_map_preserves_accumulator() {
        let ts_type = DataType::Int(IntType::new());
        let data_type = keytime_map_type(ts_type);
        let input = keytime_map(
            &DataType::Int(IntType::new()),
            vec![1],
            Arc::new(Int32Array::from(vec![10])),
        );
        let mut agg = MergeMapAgg::new_with_keytime("m", &data_type, &HashMap::new()).unwrap();
        agg.agg(&input, 0).unwrap();
        let null_map = new_null_array(input.data_type(), 1);
        agg.agg(null_map.as_ref(), 0).unwrap();
        assert_eq!(payload_after_merges(&agg), Some(1));
    }

    #[test]
    fn keytime_rejects_unsupported_timestamp_type() {
        let data_type = keytime_map_type(DataType::Boolean(crate::spec::BooleanType::new()));
        let err = MergeMapAgg::new_with_keytime("m", &data_type, &HashMap::new()).unwrap_err();
        match err {
            Error::ConfigInvalid { message } => {
                assert!(message.contains("TIMESTAMP or TIMESTAMP_LTZ"), "{message}");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn keytime_retract_remains_unsupported() {
        let data_type = keytime_map_type(DataType::Int(IntType::new()));
        let input = keytime_map(
            &DataType::Int(IntType::new()),
            vec![1],
            Arc::new(Int32Array::from(vec![10])),
        );
        let mut agg = MergeMapAgg::new_with_keytime("m", &data_type, &HashMap::new()).unwrap();
        agg.agg(&input, 0).unwrap();
        let err = agg.retract(&input, 0).unwrap_err();
        assert!(matches!(err, Error::Unsupported { .. }));
    }

    #[test]
    fn utf16_code_unit_cmp_matches_java_order() {
        assert_eq!(utf16_code_unit_cmp("a", "b"), Ordering::Less);
        assert_eq!(utf16_code_unit_cmp("\u{10000}", "\u{E000}"), Ordering::Less);
        assert_eq!(
            utf16_code_unit_cmp("\u{E000}", "\u{10000}"),
            Ordering::Greater
        );
    }
}
