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

use crate::arrow::CastExecutor;
use crate::spec::{DataField, DataType, Transform};

/// The original Java executor next to an expression bound to the current row.
/// Include it in equality: different original executors are different grants.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AuthTransform {
    expression: Transform,
    pub(super) cast: Option<CastExecutor>,
}

impl AuthTransform {
    pub(super) fn from_rest_json(json: &str, fields: &[DataField]) -> crate::Result<Self> {
        let expression = Transform::from_rest_json(json, fields)?;
        let cast = if let Transform::Cast(index, to) = &expression {
            let value: serde_json::Value = serde_json::from_str(json)
                .map_err(|error| super::unsupported(&format!("invalid transform: {error}")))?;
            let stored: DataType = serde_json::from_value(value["fieldRef"]["type"].clone())
                .map_err(|error| super::unsupported(&format!("invalid CAST source: {error}")))?;
            Some(CastExecutor::new(
                stored,
                fields[*index].data_type().clone(),
                to.clone(),
            )?)
        } else {
            None
        };
        Ok(Self { expression, cast })
    }

    pub(crate) fn expression(&self) -> &Transform {
        &self.expression
    }

    pub(super) fn collect_field_indices(&self, out: &mut std::collections::HashSet<usize>) {
        self.expression.collect_field_indices(out);
    }

    #[cfg(test)]
    pub(super) fn from_expression(expression: Transform, fields: &[DataField]) -> Self {
        let cast = if let Transform::Cast(index, to) = &expression {
            let from = fields[*index].data_type().clone();
            Some(CastExecutor::new(from.clone(), from, to.clone()).unwrap())
        } else {
            None
        };
        Self { expression, cast }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{BigIntType, DataField};

    #[test]
    fn original_executor_is_part_of_grant_equality() {
        let fields = [DataField::new(
            0,
            "v".into(),
            DataType::BigInt(BigIntType::new()),
        )];
        let transform = |source| {
            let json = serde_json::json!({"name":"CAST", "fieldRef":{"name":"v", "type":source}, "type":"DECIMAL(38,0)"});
            AuthTransform::from_rest_json(&json.to_string(), &fields).unwrap()
        };
        let integer = transform("INT");
        let double = transform("DOUBLE");
        assert_eq!(integer.expression, double.expression);
        assert_ne!(integer, double);
        assert_eq!(integer, transform("INT"));
    }
}
