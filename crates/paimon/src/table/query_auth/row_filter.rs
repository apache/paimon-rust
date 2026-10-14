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

//! REST row predicates evaluated on transform outputs, as Java LeafPredicate does.

use super::rules::{
    canonicalize_nan, check_cast_source, check_transform_cast, column_of, evaluate_transform,
    rule_mask, transform_output_type,
};
use super::unsupported;
use crate::spec::{DataField, DataType, Predicate, Transform};
use crate::Result;
use arrow_array::{ArrayRef, BooleanArray, RecordBatch};
use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RowFilter {
    /// Existing field comparisons keep the ordinary predicate representation.
    Predicate(Predicate),
    /// The predicate compares the transform's result, not its source column.
    Transformed {
        transform: Transform,
        predicate: Predicate,
        data_type: DataType,
    },
    And(Vec<RowFilter>),
    Or(Vec<RowFilter>),
}

impl RowFilter {
    pub(super) fn parse(json: &str, fields: &[DataField]) -> Result<Self> {
        let value = serde_json::from_str(json)
            .map_err(|e| unsupported(&format!("invalid filter JSON: {e}")))?;
        Self::parse_value(&value, fields)
    }

    fn parse_value(value: &serde_json::Value, fields: &[DataField]) -> Result<Self> {
        let mut normalized = value.clone();
        if normalized["kind"] == "LEAF"
            && normalized["literals"].is_null()
            && matches!(
                normalized["function"].as_str(),
                Some("TRUE" | "FALSE" | "IS_NULL" | "IS_NOT_NULL")
            )
        {
            normalized["literals"] = serde_json::json!([]);
        }
        let value = &normalized;
        match value["kind"].as_str() {
            Some("COMPOUND") => {
                let children = value["children"]
                    .as_array()
                    .filter(|c| !c.is_empty())
                    .ok_or_else(|| unsupported("the server's row filter has empty children"))?;
                let children = children
                    .iter()
                    .map(|child| Self::parse_value(child, fields))
                    .collect::<Result<Vec<_>>>()?;
                match value["function"].as_str() {
                    Some("AND") => Ok(Self::And(children)),
                    Some("OR") => Ok(Self::Or(children)),
                    _ => Err(unsupported("unknown compound filter function")),
                }
            }
            Some("LEAF") => {
                let transform = Transform::from_rest_json(&value["transform"].to_string(), fields)?;
                let data_type = transform_output_type(&transform, fields)?;
                if matches!(data_type, DataType::Binary(_) | DataType::VarBinary(_))
                    && value["literals"]
                        .as_array()
                        .is_some_and(|literals| literals.iter().any(|literal| !literal.is_null()))
                {
                    return Err(unsupported(
                        "Java predicates do not support non-null binary literals",
                    ));
                }
                if matches!(transform, Transform::FieldRef(_))
                    && !matches!(value["function"].as_str(), Some("TRUE" | "FALSE"))
                {
                    return Predicate::from_rest_json(&value.to_string(), fields)
                        .map(Self::Predicate);
                }
                check_transform_cast(&transform, fields)?;
                check_cast_source(&transform, &value["transform"], fields)?;

                let output = vec![DataField::new(0, "value".into(), data_type.clone())];
                let mut leaf = value.clone();
                // Reuse the existing typed literal/function parser on the expression
                // output. This synthetic field is never a physical table column.
                leaf["transform"] = serde_json::json!({
                    "name": "FIELD_REF", "fieldRef": {"name": "value"}
                });
                let predicate =
                    if let Some(function @ ("TRUE" | "FALSE")) = value["function"].as_str() {
                        // Java deserializes every literal using transform.outputType(),
                        // even when the function's result is constant.
                        leaf["function"] = serde_json::json!("IN");
                        Predicate::from_rest_json(&leaf.to_string(), &output)?;
                        if function == "TRUE" {
                            Predicate::AlwaysTrue
                        } else {
                            Predicate::AlwaysFalse
                        }
                    } else {
                        Predicate::from_rest_json(&leaf.to_string(), &output)?
                    };
                Ok(Self::Transformed {
                    transform,
                    predicate,
                    data_type,
                })
            }
            _ => Err(unsupported("unknown row predicate kind")),
        }
    }

    pub(super) fn collect_field_indices(&self, out: &mut HashSet<usize>) {
        match self {
            Self::Predicate(predicate) => predicate.collect_leaf_field_indices(out),
            Self::Transformed { transform, .. } => transform.collect_field_indices(out),
            Self::And(children) | Self::Or(children) => children
                .iter()
                .for_each(|child| child.collect_field_indices(out)),
        }
    }

    pub(super) fn evaluate(
        &self,
        batch: &RecordBatch,
        fields: &[DataField],
        batch_fields: &[DataField],
    ) -> Result<BooleanArray> {
        match self {
            Self::Predicate(predicate) => rule_mask(batch, predicate, fields, batch_fields),
            Self::Transformed {
                transform,
                predicate,
                data_type,
            } => {
                let input = |index: usize| -> Result<ArrayRef> {
                    let field = fields.get(index).ok_or_else(|| {
                        unsupported("the server's transform reads an unknown column")
                    })?;
                    column_of(batch, field, batch_fields)
                };
                // Evaluate before the function, even for FALSE / a NULL literal:
                // Java LeafPredicate.test first calls transform.transform(row).
                let values = evaluate_transform(
                    transform,
                    batch.num_rows(),
                    &input,
                    &crate::arrow::paimon_type_to_arrow(data_type)?,
                    fields,
                )?;
                let field = DataField::new(0, "value".into(), data_type.clone());
                let values = canonicalize_nan(&values);
                let output = RecordBatch::try_from_iter(vec![("value", values)])
                    .map_err(|e| unsupported(&format!("cannot evaluate filter transform: {e}")))?;
                rule_mask(
                    &output,
                    predicate,
                    std::slice::from_ref(&field),
                    std::slice::from_ref(&field),
                )
            }
            Self::And(children) | Self::Or(children) => {
                let or = matches!(self, Self::Or(_));
                let mut result = vec![!or; batch.num_rows()];
                let mut pending: Vec<usize> = (0..batch.num_rows()).collect();
                for child in children {
                    if pending.is_empty() {
                        break;
                    }
                    // Preserve Java's row-by-row short circuit. A later substring
                    // or cast may fail on rows already decided by an earlier child.
                    let active = if pending.len() == batch.num_rows() {
                        batch.clone()
                    } else {
                        let selected =
                            BooleanArray::from(result.iter().map(|v| *v != or).collect::<Vec<_>>());
                        arrow_select::filter::filter_record_batch(batch, &selected)
                            .map_err(|e| unsupported(&format!("cannot select filter rows: {e}")))?
                    };
                    let mask = child.evaluate(&active, fields, batch_fields)?;
                    let mut undecided = Vec::new();
                    for (row, value) in pending.into_iter().zip(mask.iter()) {
                        let value = value.unwrap_or(false);
                        result[row] = value;
                        if value != or {
                            undecided.push(row);
                        }
                    }
                    pending = undecided;
                }
                Ok(BooleanArray::from(result))
            }
        }
    }
}

impl From<Predicate> for RowFilter {
    fn from(predicate: Predicate) -> Self {
        Self::Predicate(predicate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{BigIntType, DateType, IntType, VarCharType};
    use crate::table::query_auth::rules::filter_batch;
    use arrow_array::{Date32Array, Int32Array, Int64Array, StringArray};
    use serde_json::{json, Value};
    use std::sync::Arc;

    fn fields() -> Vec<DataField> {
        vec![
            DataField::new(0, "id".into(), DataType::Int(IntType::new())),
            DataField::new(1, "s".into(), DataType::VarChar(VarCharType::string_type())),
            DataField::new(2, "p".into(), DataType::BigInt(BigIntType::new())),
            DataField::new(3, "d".into(), DataType::Date(DateType::new())),
        ]
    }

    fn rows() -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int32Array::from(vec![0, 1, 2, 3])) as ArrayRef,
            ),
            (
                "s",
                Arc::new(StringArray::from(vec![
                    Some("甲🙂乙"),
                    Some("abcabc"),
                    Some(""),
                    None,
                ])),
            ),
            ("p", Arc::new(Int64Array::from(vec![2, -3, 0, i64::MAX]))),
            (
                "d",
                Arc::new(Date32Array::from(vec![
                    Some(19782),
                    Some(20087),
                    Some(-1),
                    None,
                ])),
            ),
        ])
        .unwrap()
    }

    fn input(name: &str, type_: &str) -> Value {
        json!({"name": name, "type": type_, "index": 99})
    }

    fn leaf(transform: Value, function: &str, literals: Value) -> Value {
        json!({"kind": "LEAF", "transform": transform, "function": function, "literals": literals})
    }

    fn kept(rule: Value) -> Vec<i32> {
        let fields = fields();
        let filter = RowFilter::parse(&rule.to_string(), &fields).unwrap();
        let result = filter_batch(&rows(), &[filter], &fields, &fields).unwrap();
        result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    #[test]
    fn all_existing_string_transforms_filter_their_outputs() {
        let source = input("s", "STRING");
        let cases = [
            (
                json!({"name":"UPPER","inputs":[source]}),
                "EQUAL",
                json!(["ABCABC"]),
                vec![1],
            ),
            (
                json!({"name":"LOWER","inputs":["ABC"]}),
                "EQUAL",
                json!(["abc"]),
                vec![0, 1, 2, 3],
            ),
            (
                json!({"name":"CONCAT","inputs":[source,"x"]}),
                "EQUAL",
                json!(["abcabcx"]),
                vec![1],
            ),
            (
                json!({"name":"CONCAT","inputs":[]}),
                "EQUAL",
                json!([""]),
                vec![0, 1, 2, 3],
            ),
            (
                json!({"name":"CONCAT_WS","inputs":["-",source,null]}),
                "EQUAL",
                json!([""]),
                vec![2, 3],
            ),
            (
                json!({"name":"SUBSTRING","inputs":[source,2,2]}),
                "EQUAL",
                json!(["🙂乙"]),
                vec![0],
            ),
            (
                json!({"name":"SUBSTRING","inputs":[source,input("p","BIGINT"),2]}),
                "EQUAL",
                json!(["ab"]),
                vec![1],
            ),
            (
                json!({"name":"TRIM","trimFlag":"BOTH","inputs":[source,"甲乙ac"]}),
                "EQUAL",
                json!(["🙂"]),
                vec![0],
            ),
            (
                json!({"name":"LENGTH","inputs":[source]}),
                "BETWEEN",
                json!([3, 6]),
                vec![0, 1],
            ),
            (
                json!({"name":"BIT_LENGTH","inputs":[source]}),
                "GREATER_THAN",
                json!([48]),
                vec![0],
            ),
            (
                json!({"name":"PAD","direction":"LEFT","inputs":[source,5,"🙂x"]}),
                "EQUAL",
                json!(["🙂x甲🙂乙"]),
                vec![0],
            ),
            (
                json!({"name":"TRANSLATE","inputs":[source,"甲🙂乙aba","XY"]}),
                "EQUAL",
                json!(["XY"]),
                vec![0],
            ),
            (
                json!({"name":"OVERLAY","inputs":[source,"x",2,1]}),
                "EQUAL",
                json!(["axcabc"]),
                vec![1],
            ),
            (
                json!({"name":"NULL"}),
                "IS_NULL",
                json!([]),
                vec![0, 1, 2, 3],
            ),
        ];
        for (transform, function, literals, expected) in cases {
            let rule = leaf(transform, function, literals);
            assert_eq!(kept(rule.clone()), expected, "{rule}");
        }
    }

    #[test]
    fn transformed_leaf_functions_keep_java_null_semantics() {
        let transform = json!({"name":"LENGTH","inputs":[input("s","STRING")]});
        for (function, literals, expected) in [
            ("EQUAL", json!([3]), vec![0]),
            ("NOT_EQUAL", json!([3]), vec![1, 2]),
            ("LESS_THAN", json!([3]), vec![2]),
            ("LESS_OR_EQUAL", json!([3]), vec![0, 2]),
            ("GREATER_THAN", json!([3]), vec![1]),
            ("GREATER_OR_EQUAL", json!([3]), vec![0, 1]),
            ("IN", json!([3, null]), vec![0]),
            ("NOT_IN", json!([3]), vec![1, 2]),
            ("NOT_IN", json!([3, null]), vec![]),
            ("NOT_BETWEEN", json!([1, 4]), vec![1, 2]),
            ("IS_NULL", json!([]), vec![3]),
            ("IS_NOT_NULL", json!([]), vec![0, 1, 2]),
            ("TRUE", json!([]), vec![0, 1, 2, 3]),
            ("FALSE", json!([]), vec![]),
            ("EQUAL", json!([null]), vec![]),
            ("NOT_EQUAL", json!([null]), vec![]),
        ] {
            assert_eq!(
                kept(leaf(transform.clone(), function, literals)),
                expected,
                "{function}"
            );
        }
        let upper = json!({"name":"UPPER","inputs":[input("s","STRING")]});
        for (function, literals) in [
            ("STARTS_WITH", json!(["AB"])),
            ("ENDS_WITH", json!(["BC"])),
            ("CONTAINS", json!(["BCA"])),
            ("LIKE", json!(["A_C%"])),
        ] {
            assert_eq!(
                kept(leaf(upper.clone(), function, literals)),
                vec![1],
                "{function}"
            );
        }
    }

    #[test]
    fn casts_compare_java_character_and_byte_widths() {
        for (type_, literal, expected) in [
            ("VARCHAR(1)", json!("甲"), vec![0]),
            ("CHAR(8)", json!("abcabc  "), vec![1]),
            ("VARCHAR", json!("a"), vec![1]),
            ("CHAR", json!(" "), vec![2]),
        ] {
            let transform = json!({"name":"CAST","fieldRef":input("s","STRING"),"type":type_});
            assert_eq!(
                kept(leaf(transform, "EQUAL", json!([literal]))),
                expected,
                "{type_}"
            );
        }
        for type_ in ["BINARY(8)", "VARBINARY(1)"] {
            let transform = json!({"name":"CAST","fieldRef":input("s","STRING"),"type":type_});
            assert_eq!(kept(leaf(transform.clone(), "IS_NULL", json!([]))), vec![3]);
            assert_eq!(
                kept(leaf(transform.clone(), "EQUAL", json!([null]))),
                Vec::<i32>::new()
            );
            // Java does not deserialize non-NULL binary predicate literals,
            // even when the function would ignore them.
            for function in ["EQUAL", "TRUE", "IS_NULL"] {
                let rule = leaf(transform.clone(), function, json!(["YQAA"]));
                assert!(RowFilter::parse(&rule.to_string(), &fields()).is_err());
            }
        }
    }

    #[test]
    fn numeric_cast_filters_use_java_narrowing() {
        assert_eq!(
            kept(leaf(
                json!({"name":"CAST","fieldRef":input("p","BIGINT"),"type":"TINYINT"}),
                "EQUAL",
                json!([2])
            )),
            vec![0]
        );
    }

    #[test]
    fn calendar_outputs_are_ints_and_keep_all_source_dependencies() {
        let rule = leaf(
            json!({"name":"YEAR_OF_WEEK","fieldRef":input("d","DATE")}),
            "EQUAL",
            json!([2025]),
        );
        assert_eq!(kept(rule.clone()), vec![1]);
        let filter = RowFilter::parse(&rule.to_string(), &fields()).unwrap();
        let mut deps = HashSet::new();
        filter.collect_field_indices(&mut deps);
        assert_eq!(deps, HashSet::from([3]));
        let compound = json!({"kind":"COMPOUND","function":"AND","children":[
            rule, leaf(json!({"name":"SUBSTRING","inputs":[input("s","STRING"),input("p","BIGINT"),1]}),"EQUAL",json!(["a"]))
        ]});
        let mut deps = HashSet::new();
        RowFilter::parse(&compound.to_string(), &fields())
            .unwrap()
            .collect_field_indices(&mut deps);
        assert_eq!(deps, HashSet::from([1, 2, 3]));
    }

    #[test]
    fn and_or_and_filter_list_short_circuit_before_throwing_transforms() {
        let fields = fields();
        let base = rows();
        let mut columns = base.columns().to_vec();
        columns[2] = Arc::new(Int64Array::from(vec![i64::MAX, 1, 0, i64::MAX]));
        let source = RecordBatch::try_new(base.schema(), columns).unwrap();
        let guard = |id| {
            leaf(
                json!({"name":"FIELD_REF","fieldRef":input("id","INT")}),
                "EQUAL",
                json!([id]),
            )
        };
        let substring = leaf(
            json!({"name":"SUBSTRING","inputs":[input("s","STRING"),input("p","BIGINT"),1]}),
            "EQUAL",
            json!(["a"]),
        );
        for (function, id, expected) in [("AND", 1, vec![1]), ("OR", 0, vec![0, 1])] {
            let rule =
                json!({"kind":"COMPOUND","function":function,"children":[guard(id),substring]});
            let filter = RowFilter::parse(&rule.to_string(), &fields).unwrap();
            let result = filter_batch(&source, &[filter], &fields, &fields).unwrap();
            assert_eq!(
                result
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec(),
                expected
            );
        }
        let nested = json!({"kind":"COMPOUND","function":"OR","children":[
            guard(0), {"kind":"COMPOUND","function":"AND","children":[guard(1),substring]}
        ]});
        let nested = RowFilter::parse(&nested.to_string(), &fields).unwrap();
        let result = filter_batch(&source, &[nested], &fields, &fields).unwrap();
        assert_eq!(
            result
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec(),
            vec![0, 1]
        );
        let filters = [guard(1), substring.clone()]
            .map(|rule| RowFilter::parse(&rule.to_string(), &fields).unwrap());
        assert_eq!(
            filter_batch(&source, &filters, &fields, &fields)
                .unwrap()
                .num_rows(),
            1
        );
        assert!(filter_batch(
            &source,
            &[RowFilter::parse(&substring.to_string(), &fields).unwrap()],
            &fields,
            &fields
        )
        .is_err());
        // Even a constant/NULL-literal function must execute its transform first.
        for (function, literals) in [
            ("TRUE", json!([])),
            ("FALSE", json!([])),
            ("EQUAL", json!([null])),
            ("NOT_EQUAL", json!([null])),
        ] {
            let rule = leaf(substring["transform"].clone(), function, literals);
            let filter = RowFilter::parse(&rule.to_string(), &fields).unwrap();
            assert!(
                filter_batch(&source, &[filter], &fields, &fields).is_err(),
                "{function}"
            );
        }
    }

    #[test]
    fn all_literals_and_unreachable_definitions_are_validated() {
        let fields = fields();
        for function in [
            "TRUE",
            "FALSE",
            "IS_NULL",
            "IS_NOT_NULL",
            "EQUAL",
            "NOT_EQUAL",
            "IN",
        ] {
            for literals in [json!(["bad"]), json!([null, "bad"])] {
                let rule = leaf(
                    json!({"name":"LENGTH","inputs":[input("s","STRING")]}),
                    function,
                    literals,
                );
                assert!(
                    RowFilter::parse(&rule.to_string(), &fields).is_err(),
                    "{rule}"
                );
                let unreachable = json!({"kind":"COMPOUND","function":"OR","children":[
                    leaf(json!({"name":"NULL"}),"TRUE",json!([])),rule
                ]});
                assert!(RowFilter::parse(&unreachable.to_string(), &fields).is_err());
            }
        }
        for function in ["TRUE", "FALSE", "IS_NULL", "IS_NOT_NULL"] {
            let mut rule = leaf(
                json!({"name":"LENGTH","inputs":[input("s","STRING")]}),
                function,
                Value::Null,
            );
            assert!(RowFilter::parse(&rule.to_string(), &fields).is_ok());
            rule.as_object_mut().unwrap().remove("literals");
            assert!(RowFilter::parse(&rule.to_string(), &fields).is_ok());
        }
        let rebound = leaf(
            json!({"name":"CAST","fieldRef":input("s","BOOLEAN"),"type":"STRING"}),
            "TRUE",
            json!([]),
        );
        assert!(RowFilter::parse(&rebound.to_string(), &fields)
            .unwrap_err()
            .to_string()
            .contains("rebound source"));
    }
}
