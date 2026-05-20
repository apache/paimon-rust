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

//! `listagg`: concatenate non-NULL string values for a key, separated by a
//! per-field delimiter (`fields.<col>.list-agg-delimiter`, defaulting to
//! `","`).
//!
//! Reference: Java `FieldListaggAgg` under
//! `org.apache.paimon.mergetree.compact.aggregate`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, StringArray};

use super::{unsupported_type_error, FieldAggregator};
use crate::spec::DataType;

const FIELDS_PREFIX: &str = "fields.";
const LIST_AGG_DELIMITER_SUFFIX: &str = ".list-agg-delimiter";
const DEFAULT_DELIMITER: &str = ",";

fn list_agg_delimiter<'a>(field_name: &str, options: &'a HashMap<String, String>) -> &'a str {
    options
        .get(&format!(
            "{FIELDS_PREFIX}{field_name}{LIST_AGG_DELIMITER_SUFFIX}"
        ))
        .map(String::as_str)
        .unwrap_or(DEFAULT_DELIMITER)
}

#[derive(Debug)]
pub(crate) struct ListaggAgg {
    field_name: String,
    delimiter: String,
    acc: Option<String>,
}

impl ListaggAgg {
    pub(crate) fn new(
        field_name: &str,
        data_type: &DataType,
        table_options: &HashMap<String, String>,
    ) -> crate::Result<Self> {
        match data_type {
            DataType::Char(_) | DataType::VarChar(_) => {}
            other => return Err(unsupported_type_error("listagg", field_name, other)),
        }
        Ok(Self {
            field_name: field_name.to_string(),
            delimiter: list_agg_delimiter(field_name, table_options).to_string(),
            acc: None,
        })
    }
}

impl FieldAggregator for ListaggAgg {
    fn name(&self) -> &'static str {
        "listagg"
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
            .downcast_ref::<StringArray>()
            .ok_or_else(|| crate::Error::DataInvalid {
                message: format!(
                    "listagg column '{}' received non-Utf8 Arrow array {:?}",
                    self.field_name,
                    array.data_type()
                ),
                source: None,
            })?;
        let v = arr.value(row_idx);
        match &mut self.acc {
            None => self.acc = Some(v.to_string()),
            Some(prev) => {
                prev.push_str(&self.delimiter);
                prev.push_str(v);
            }
        }
        Ok(())
    }

    fn result(&self) -> crate::Result<ArrayRef> {
        Ok(Arc::new(StringArray::from(vec![self.acc.clone()])))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{IntType, VarCharType};

    fn collect(arr: ArrayRef) -> Option<String> {
        let a = arr.as_any().downcast_ref::<StringArray>().unwrap();
        if a.is_null(0) {
            None
        } else {
            Some(a.value(0).to_string())
        }
    }

    fn varchar_type() -> DataType {
        DataType::VarChar(VarCharType::new(255).unwrap())
    }

    #[test]
    fn test_listagg_default_delimiter_skips_null() {
        let mut agg = ListaggAgg::new("v", &varchar_type(), &HashMap::new()).unwrap();
        let arr = StringArray::from(vec![Some("a"), None, Some("b"), Some("c")]);
        for i in 0..arr.len() {
            agg.agg(&arr, i).unwrap();
        }
        assert_eq!(collect(agg.result().unwrap()), Some("a,b,c".to_string()));
    }

    #[test]
    fn test_listagg_custom_delimiter() {
        let opts = HashMap::from([("fields.v.list-agg-delimiter".to_string(), "|".to_string())]);
        let mut agg = ListaggAgg::new("v", &varchar_type(), &opts).unwrap();
        let arr = StringArray::from(vec![Some("x"), Some("y")]);
        for i in 0..arr.len() {
            agg.agg(&arr, i).unwrap();
        }
        assert_eq!(collect(agg.result().unwrap()), Some("x|y".to_string()));
    }

    #[test]
    fn test_listagg_all_null_returns_null() {
        let mut agg = ListaggAgg::new("v", &varchar_type(), &HashMap::new()).unwrap();
        let arr = StringArray::from(vec![None::<&str>, None]);
        for i in 0..arr.len() {
            agg.agg(&arr, i).unwrap();
        }
        assert_eq!(collect(agg.result().unwrap()), None);
    }

    #[test]
    fn test_listagg_single_value_does_not_prepend_delimiter() {
        let mut agg = ListaggAgg::new("v", &varchar_type(), &HashMap::new()).unwrap();
        let arr = StringArray::from(vec![Some("only")]);
        agg.agg(&arr, 0).unwrap();
        assert_eq!(collect(agg.result().unwrap()), Some("only".to_string()));
    }

    #[test]
    fn test_listagg_rejects_non_string_type() {
        let err =
            ListaggAgg::new("v", &DataType::Int(IntType::new()), &HashMap::new()).unwrap_err();
        assert!(
            matches!(err, crate::Error::ConfigInvalid { message } if message.contains("listagg"))
        );
    }

    #[test]
    fn test_reset_clears_state() {
        let mut agg = ListaggAgg::new("v", &varchar_type(), &HashMap::new()).unwrap();
        let arr = StringArray::from(vec![Some("keep_me")]);
        agg.agg(&arr, 0).unwrap();
        agg.reset();
        assert_eq!(collect(agg.result().unwrap()), None);
    }
}
