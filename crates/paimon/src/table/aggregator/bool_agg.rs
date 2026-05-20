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

//! Boolean reducers: `bool_and` and `bool_or`.
//!
//! Both accumulate over BOOLEAN columns, skipping NULL inputs.  The output
//! cell is NULL when no non-NULL input was observed for the key.
//!
//! Reference: Java `FieldBoolAndAgg`, `FieldBoolOrAgg` under
//! `org.apache.paimon.mergetree.compact.aggregate`.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray};

use super::{unsupported_type_error, FieldAggregator};
use crate::spec::DataType;

fn ensure_boolean(field_name: &str, data_type: &DataType, op: &str) -> crate::Result<()> {
    match data_type {
        DataType::Boolean(_) => Ok(()),
        other => Err(unsupported_type_error(op, field_name, other)),
    }
}

#[derive(Debug)]
pub(crate) struct BoolAndAgg {
    field_name: String,
    acc: Option<bool>,
}

impl BoolAndAgg {
    pub(crate) fn new(field_name: &str, data_type: &DataType) -> crate::Result<Self> {
        ensure_boolean(field_name, data_type, "bool_and")?;
        Ok(Self {
            field_name: field_name.to_string(),
            acc: None,
        })
    }
}

impl FieldAggregator for BoolAndAgg {
    fn name(&self) -> &'static str {
        "bool_and"
    }

    fn reset(&mut self) {
        self.acc = None;
    }

    fn agg(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        if array.is_null(row_idx) {
            return Ok(());
        }
        let arr = array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| crate::Error::DataInvalid {
                message: format!(
                    "bool_and column '{}' received non-Boolean Arrow array {:?}",
                    self.field_name,
                    array.data_type()
                ),
                source: None,
            })?;
        let v = arr.value(row_idx);
        self.acc = Some(self.acc.map_or(v, |prev| prev && v));
        Ok(())
    }

    fn result(&self) -> crate::Result<ArrayRef> {
        Ok(Arc::new(BooleanArray::from(vec![self.acc])))
    }
}

#[derive(Debug)]
pub(crate) struct BoolOrAgg {
    field_name: String,
    acc: Option<bool>,
}

impl BoolOrAgg {
    pub(crate) fn new(field_name: &str, data_type: &DataType) -> crate::Result<Self> {
        ensure_boolean(field_name, data_type, "bool_or")?;
        Ok(Self {
            field_name: field_name.to_string(),
            acc: None,
        })
    }
}

impl FieldAggregator for BoolOrAgg {
    fn name(&self) -> &'static str {
        "bool_or"
    }

    fn reset(&mut self) {
        self.acc = None;
    }

    fn agg(&mut self, array: &dyn Array, row_idx: usize) -> crate::Result<()> {
        if array.is_null(row_idx) {
            return Ok(());
        }
        let arr = array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| crate::Error::DataInvalid {
                message: format!(
                    "bool_or column '{}' received non-Boolean Arrow array {:?}",
                    self.field_name,
                    array.data_type()
                ),
                source: None,
            })?;
        let v = arr.value(row_idx);
        self.acc = Some(self.acc.map_or(v, |prev| prev || v));
        Ok(())
    }

    fn result(&self) -> crate::Result<ArrayRef> {
        Ok(Arc::new(BooleanArray::from(vec![self.acc])))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{BooleanType, IntType};

    fn collect(arr: ArrayRef) -> Option<bool> {
        let a = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
        if a.is_null(0) {
            None
        } else {
            Some(a.value(0))
        }
    }

    #[test]
    fn test_bool_and_all_true_returns_true() {
        let mut agg = BoolAndAgg::new("b", &DataType::Boolean(BooleanType::new())).unwrap();
        let arr = BooleanArray::from(vec![Some(true), Some(true), None, Some(true)]);
        for i in 0..arr.len() {
            agg.agg(&arr, i).unwrap();
        }
        assert_eq!(collect(agg.result().unwrap()), Some(true));
    }

    #[test]
    fn test_bool_and_short_circuits_false() {
        let mut agg = BoolAndAgg::new("b", &DataType::Boolean(BooleanType::new())).unwrap();
        let arr = BooleanArray::from(vec![Some(true), Some(false), Some(true)]);
        for i in 0..arr.len() {
            agg.agg(&arr, i).unwrap();
        }
        assert_eq!(collect(agg.result().unwrap()), Some(false));
    }

    #[test]
    fn test_bool_or_any_true() {
        let mut agg = BoolOrAgg::new("b", &DataType::Boolean(BooleanType::new())).unwrap();
        let arr = BooleanArray::from(vec![Some(false), None, Some(true), Some(false)]);
        for i in 0..arr.len() {
            agg.agg(&arr, i).unwrap();
        }
        assert_eq!(collect(agg.result().unwrap()), Some(true));
    }

    #[test]
    fn test_bool_and_or_all_null_returns_null() {
        let mut and_agg = BoolAndAgg::new("b", &DataType::Boolean(BooleanType::new())).unwrap();
        let arr = BooleanArray::from(vec![None::<bool>, None]);
        for i in 0..arr.len() {
            and_agg.agg(&arr, i).unwrap();
        }
        assert_eq!(collect(and_agg.result().unwrap()), None);

        let mut or_agg = BoolOrAgg::new("b", &DataType::Boolean(BooleanType::new())).unwrap();
        for i in 0..arr.len() {
            or_agg.agg(&arr, i).unwrap();
        }
        assert_eq!(collect(or_agg.result().unwrap()), None);
    }

    #[test]
    fn test_bool_and_rejects_non_boolean_type() {
        let err = BoolAndAgg::new("b", &DataType::Int(IntType::new())).unwrap_err();
        assert!(
            matches!(err, crate::Error::ConfigInvalid { message } if message.contains("bool_and"))
        );
    }

    #[test]
    fn test_reset_clears_state() {
        let mut agg = BoolOrAgg::new("b", &DataType::Boolean(BooleanType::new())).unwrap();
        let arr = BooleanArray::from(vec![Some(true)]);
        agg.agg(&arr, 0).unwrap();
        agg.reset();
        assert_eq!(collect(agg.result().unwrap()), None);
    }
}
