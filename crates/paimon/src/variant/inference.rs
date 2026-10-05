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

//! Bounded evidence for Java's rolling-writer-scoped adaptive Variant inference.

use super::{
    data_invalid, finalize_inferred_schema, inferred_count_field, inferred_field_count,
    inferred_variant_type, merge_inferred_schema, merge_inferred_types, schema_of_variant,
    ArrayType, BigIntType, DataField, DataType, DecimalType, GenericVariant, Result, RowType,
    VariantShreddingInferConfig,
};

#[derive(Clone)]
struct ColumnEvidence {
    root_value_count: f64,
    observed_schema: Option<DataType>,
}

impl ColumnEvidence {
    fn analyze(values: &[GenericVariant], max_depth: usize) -> Result<Self> {
        let mut observed_schema = None;
        for value in values {
            observed_schema = merge_inferred_schema(
                observed_schema,
                schema_of_variant(value.as_ref()?, max_depth)?,
            )?;
        }
        Ok(Self {
            root_value_count: values.len() as f64,
            observed_schema,
        })
    }

    fn bounded(mut self, sample_size: usize) -> Result<Self> {
        if self.root_value_count > sample_size as f64 {
            let scale = sample_size as f64 / self.root_value_count;
            self.observed_schema = self
                .observed_schema
                .as_ref()
                .map(|schema| scale_field_counts(schema, scale))
                .transpose()?;
            self.root_value_count = sample_size as f64;
        }
        Ok(self)
    }
}

struct InferenceResult {
    evidence: Vec<ColumnEvidence>,
    selected: Vec<DataType>,
}

/// Only a successful file close promotes pending evidence to the next file's prior.
pub(crate) struct VariantShreddingInferenceSession {
    config: VariantShreddingInferConfig,
    max_schema_width: usize,
    effective_sample_size: usize,
    retention_ratio: f64,
    committed: Option<InferenceResult>,
    pending: Option<InferenceResult>,
}

impl VariantShreddingInferenceSession {
    pub(crate) fn new(
        config: VariantShreddingInferConfig,
        max_schema_width: usize,
        effective_sample_size: usize,
        retention_ratio: f64,
    ) -> Result<Self> {
        if effective_sample_size == 0 {
            return data_invalid("Effective Variant inference sample size must be positive");
        }
        if !(0.0..=1.0).contains(&config.min_field_cardinality_ratio)
            || !(0.0..=config.min_field_cardinality_ratio).contains(&retention_ratio)
        {
            return data_invalid(
                "Variant inference requires 0 <= retention ratio <= admission ratio <= 1",
            );
        }
        Ok(Self {
            config,
            max_schema_width,
            effective_sample_size,
            retention_ratio,
            committed: None,
            pending: None,
        })
    }

    pub(crate) fn has_prior(&self) -> bool {
        self.committed.is_some()
    }

    pub(crate) fn infer_schema(
        &mut self,
        columns: &[Vec<GenericVariant>],
    ) -> Result<Vec<DataType>> {
        let mut remaining = self.max_schema_width;
        let mut evidence = Vec::with_capacity(columns.len());
        let mut selected = Vec::with_capacity(columns.len());
        for (index, values) in columns.iter().enumerate() {
            let current = ColumnEvidence::analyze(values, self.config.max_schema_depth)?;
            let (combined, schema) = if let Some(prior) = &self.committed {
                let previous = &prior.evidence[index];
                let combined = if current.root_value_count == 0.0 {
                    previous.clone()
                } else {
                    let bounded = previous.clone().bounded(self.effective_sample_size)?;
                    ColumnEvidence {
                        root_value_count: current.root_value_count + bounded.root_value_count,
                        observed_schema: merge_inferred_schema(
                            bounded.observed_schema,
                            current.observed_schema.clone(),
                        )?,
                    }
                    .bounded(self.effective_sample_size)?
                };
                let selector = AdaptiveSelection {
                    root_value_count: combined.root_value_count,
                    admission_ratio: self.config.min_field_cardinality_ratio,
                    retention_ratio: self.retention_ratio,
                };
                let schema = selector.finalize(
                    combined.observed_schema.as_ref(),
                    current.observed_schema.as_ref(),
                    Some(&prior.selected[index]),
                    &mut remaining,
                )?;
                (combined, schema)
            } else {
                let minimum =
                    (current.root_value_count * self.config.min_field_cardinality_ratio).ceil();
                let schema = finalize_inferred_schema(
                    current.observed_schema.clone(),
                    minimum,
                    &mut remaining,
                )?;
                (current.bounded(self.effective_sample_size)?, schema)
            };
            evidence.push(combined);
            selected.push(schema);
        }
        self.pending = Some(InferenceResult {
            evidence,
            selected: selected.clone(),
        });
        Ok(selected)
    }

    pub(crate) fn commit_pending_inference(&mut self) -> Result<()> {
        let Some(pending) = self.pending.take() else {
            return data_invalid("No pending Variant inference to commit");
        };
        self.committed = Some(pending);
        Ok(())
    }
}

fn scale_field_counts(schema: &DataType, scale: f64) -> Result<DataType> {
    Ok(match schema {
        DataType::Row(row) => DataType::Row(RowType::with_nullable(
            schema.is_nullable(),
            row.fields()
                .iter()
                .map(|field| {
                    Ok(inferred_count_field(
                        field.id(),
                        field.name(),
                        scale_field_counts(field.data_type(), scale)?,
                        inferred_field_count(field)? * scale,
                    ))
                })
                .collect::<Result<Vec<_>>>()?,
        )),
        DataType::Array(array) => DataType::Array(ArrayType::with_nullable(
            schema.is_nullable(),
            scale_field_counts(array.element_type(), scale)?,
        )),
        other => other.clone(),
    })
}

struct AdaptiveSelection {
    root_value_count: f64,
    admission_ratio: f64,
    retention_ratio: f64,
}

impl AdaptiveSelection {
    fn finalize<'a>(
        &self,
        mut combined: Option<&'a DataType>,
        current: Option<&'a DataType>,
        mut previous: Option<&'a DataType>,
        remaining: &mut usize,
    ) -> Result<DataType> {
        *remaining = remaining.saturating_sub(1);
        if *remaining == 0 {
            return Ok(inferred_variant_type());
        }
        if let (Some(prior), Some(current)) = (previous, current) {
            if !compatible_type_families(prior, current)? {
                combined = Some(current);
                previous = None;
            }
        }
        if combined.is_none_or(|value| matches!(value, DataType::Variant(_))) {
            if let Some(current) = current.filter(|value| !matches!(value, DataType::Variant(_))) {
                combined = Some(current);
            } else if let Some(previous) = previous {
                return Ok(retain_selected_schema(previous, remaining));
            } else {
                return Ok(inferred_variant_type());
            }
        }
        match combined.expect("combined evidence selected above") {
            DataType::Row(row) => {
                let mut candidates = Vec::new();
                for field in row.fields() {
                    let was_selected = find_field(previous, field.name()).is_some();
                    let threshold = if was_selected {
                        self.retention_ratio
                    } else {
                        self.admission_ratio
                    };
                    let ratio = if self.root_value_count == 0.0 {
                        0.0
                    } else {
                        inferred_field_count(field)? / self.root_value_count
                    };
                    if ratio >= threshold {
                        candidates.push((field, ratio, was_selected));
                    }
                }
                candidates.sort_by(|(a, ar, ap), (b, br, bp)| {
                    br.total_cmp(ar).then(bp.cmp(ap)).then_with(|| {
                        // Java's adaptive tie-breaker uses String.compareTo (UTF-16).
                        a.name().encode_utf16().cmp(b.name().encode_utf16())
                    })
                });
                let mut selected = Vec::new();
                for (field, _, _) in candidates {
                    if *remaining == 0 {
                        break;
                    }
                    let field_type = self.finalize(
                        Some(field.data_type()),
                        find_field(current, field.name()).map(DataField::data_type),
                        find_field(previous, field.name()).map(DataField::data_type),
                        remaining,
                    )?;
                    selected.push(DataField::new(0, field.name().to_string(), field_type));
                }
                selected.sort_by(|a, b| a.name().encode_utf16().cmp(b.name().encode_utf16()));
                Ok(selected_row(selected))
            }
            DataType::Array(array) => Ok(DataType::Array(ArrayType::new(self.finalize(
                Some(array.element_type()),
                array_element(current),
                array_element(previous),
                remaining,
            )?))),
            combined => {
                *remaining = remaining.saturating_sub(1);
                match (current, previous) {
                    (None, Some(previous)) => Ok(previous.clone()),
                    (None, None) => widen_scalar_type(combined),
                    (Some(current), None) => widen_scalar_type(current),
                    (Some(current), Some(previous)) => {
                        let merged = merge_inferred_types(previous.clone(), current.clone())?;
                        if matches!(merged, DataType::Variant(_)) {
                            widen_scalar_type(current)
                        } else {
                            Ok(merged)
                        }
                    }
                }
            }
        }
    }
}

fn compatible_type_families(previous: &DataType, current: &DataType) -> Result<bool> {
    Ok(match (previous, current) {
        (DataType::Row(_), DataType::Row(_)) | (DataType::Array(_), DataType::Array(_)) => true,
        (DataType::Row(_) | DataType::Array(_), _) | (_, DataType::Row(_) | DataType::Array(_)) => {
            false
        }
        _ => !matches!(
            merge_inferred_types(previous.clone(), current.clone())?,
            DataType::Variant(_)
        ),
    })
}

fn retain_selected_schema(selected: &DataType, remaining: &mut usize) -> DataType {
    match selected {
        DataType::Row(row) => {
            let mut fields = Vec::new();
            for field in row.fields() {
                if *remaining == 0 {
                    break;
                }
                *remaining -= 1;
                let retained = if *remaining == 0 {
                    inferred_variant_type()
                } else {
                    retain_selected_schema(field.data_type(), remaining)
                };
                fields.push(DataField::new(0, field.name().to_string(), retained));
            }
            selected_row(fields)
        }
        DataType::Array(array) => {
            *remaining = remaining.saturating_sub(1);
            let element = if *remaining == 0 {
                inferred_variant_type()
            } else {
                retain_selected_schema(array.element_type(), remaining)
            };
            DataType::Array(ArrayType::new(element))
        }
        other => {
            if !matches!(other, DataType::Variant(_)) {
                *remaining = remaining.saturating_sub(1);
            }
            other.clone()
        }
    }
}

fn selected_row(fields: Vec<DataField>) -> DataType {
    if fields.is_empty() {
        inferred_variant_type()
    } else {
        DataType::Row(RowType::new(
            fields
                .into_iter()
                .enumerate()
                .map(|(id, field)| {
                    DataField::new(id as i32, field.name().into(), field.data_type().clone())
                })
                .collect(),
        ))
    }
}

fn widen_scalar_type(data_type: &DataType) -> Result<DataType> {
    Ok(match data_type {
        DataType::Decimal(decimal) if decimal.precision() <= 18 && decimal.scale() == 0 => {
            DataType::BigInt(BigIntType::new())
        }
        DataType::Decimal(decimal) => DataType::Decimal(DecimalType::new(
            if decimal.precision() <= 18 {
                18
            } else {
                DecimalType::MAX_PRECISION
            },
            decimal.scale(),
        )?),
        other => other.clone(),
    })
}

fn find_field<'a>(schema: Option<&'a DataType>, name: &str) -> Option<&'a DataField> {
    match schema {
        Some(DataType::Row(row)) => row.fields().iter().find(|field| field.name() == name),
        _ => None,
    }
}

fn array_element(schema: Option<&DataType>) -> Option<&DataType> {
    match schema {
        Some(DataType::Array(array)) => Some(array.element_type()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{BooleanType, VarCharType};

    fn session(
        width: usize,
        sample_size: usize,
        admission: f64,
        retention: f64,
    ) -> VariantShreddingInferenceSession {
        VariantShreddingInferenceSession::new(
            VariantShreddingInferConfig {
                max_schema_depth: 50,
                min_field_cardinality_ratio: admission,
            },
            width,
            sample_size,
            retention,
        )
        .unwrap()
    }

    fn values(json: &[&str]) -> Vec<GenericVariant> {
        json.iter()
            .map(|value| GenericVariant::parse_json(value).unwrap())
            .collect()
    }

    fn row(fields: &[(&str, DataType)]) -> DataType {
        DataType::Row(RowType::new(
            fields
                .iter()
                .enumerate()
                .map(|(i, (name, value))| DataField::new(i as i32, (*name).into(), value.clone()))
                .collect(),
        ))
    }

    fn bigint() -> DataType {
        DataType::BigInt(BigIntType::new())
    }

    #[test]
    fn admission_and_retention_match_java() {
        let mut session = session(300, 10, 0.4, 0.2);
        let first = (0..10)
            .map(|i| {
                if i < 5 {
                    r#"{"legacy":"v","stable":1}"#
                } else {
                    r#"{"stable":1}"#
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(
            session.infer_schema(&[values(&first)]).unwrap(),
            vec![row(&[
                ("legacy", DataType::VarChar(VarCharType::string_type())),
                ("stable", bigint()),
            ])]
        );
        session.commit_pending_inference().unwrap();
        let second = (0..10)
            .map(|i| {
                if i < 9 {
                    r#"{"emerging":true,"stable":2}"#
                } else {
                    r#"{"stable":2}"#
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(
            session.infer_schema(&[values(&second)]).unwrap(),
            vec![row(&[
                ("emerging", DataType::Boolean(BooleanType::new())),
                ("legacy", DataType::VarChar(VarCharType::string_type())),
                ("stable", bigint()),
            ])]
        );
        session.commit_pending_inference().unwrap();
        assert_eq!(
            session
                .infer_schema(&[values(&[r#"{"stable":3}"#; 10])])
                .unwrap(),
            vec![row(&[
                ("emerging", DataType::Boolean(BooleanType::new())),
                ("stable", bigint()),
            ])]
        );
    }

    #[test]
    fn failed_file_does_not_supply_prior_evidence() {
        let mut session = session(300, 10, 0.1, 0.05);
        session
            .infer_schema(&[values(&[r#"{"failed":1}"#])])
            .unwrap();
        assert!(!session.has_prior());
        assert_eq!(
            session
                .infer_schema(&[values(&[r#"{"committed":2}"#])])
                .unwrap(),
            vec![row(&[("committed", bigint())])]
        );
        session.commit_pending_inference().unwrap();
        assert!(session.has_prior());
        assert!(session.commit_pending_inference().is_err());
        assert_eq!(
            session.infer_schema(&[vec![]]).unwrap(),
            vec![row(&[("committed", bigint())])]
        );
    }

    #[test]
    fn widens_scalar_selected_from_prior_evidence() {
        let mut session = session(6, 10, 0.1, 0.05);
        assert_eq!(
            session
                .infer_schema(&[
                    values(&[r#"{"a":1,"b":2}"#]),
                    values(&[r#"{"historical":12345}"#]),
                ])
                .unwrap(),
            vec![
                row(&[("a", bigint()), ("b", bigint())]),
                inferred_variant_type()
            ]
        );
        session.commit_pending_inference().unwrap();
        assert_eq!(
            session.infer_schema(&[values(&["1"]), vec![]]).unwrap(),
            vec![bigint(), row(&[("historical", bigint())])]
        );
    }

    #[test]
    fn keeps_selected_row_when_evidence_degraded_and_node_absent() {
        let mut session = session(300, 256, 0.1, 0.05);
        for value in [r#"{"k":1,"p":5}"#, r#"{"k":1,"p":{"x":1}}"#] {
            session.infer_schema(&[values(&[value])]).unwrap();
            session.commit_pending_inference().unwrap();
        }
        assert_eq!(
            session.infer_schema(&[values(&[r#"{"k":1}"#])]).unwrap(),
            vec![row(&[("k", bigint()), ("p", row(&[("x", bigint())])),])]
        );
    }

    #[test]
    fn retained_schema_consumes_shared_width_budget() {
        let mut session = session(8, 256, 0.1, 0.05);
        for first in [r#"{"p":5}"#, r#"{"p":{"x":1}}"#] {
            session
                .infer_schema(&[values(&[first]), values(&[r#"{"q":1}"#])])
                .unwrap();
            session.commit_pending_inference().unwrap();
        }
        let selected = session
            .infer_schema(&[values(&["{}"]), values(&[r#"{"q":1,"r":1}"#])])
            .unwrap();
        assert_eq!(selected[0], row(&[("p", row(&[("x", bigint())]))]));
        assert_eq!(
            selected[1],
            row(&[("q", bigint()), ("r", inferred_variant_type())])
        );
    }

    #[test]
    fn retained_schema_keeps_fields_at_last_budget_unit() {
        let mut session = session(7, 256, 0.1, 0.05);
        for second in ["5", r#"{"q":1}"#] {
            session
                .infer_schema(&[values(&["1"]), values(&[second])])
                .unwrap();
            session.commit_pending_inference().unwrap();
        }
        assert_eq!(
            session
                .infer_schema(&[values(&[r#"{"x":1,"y":1}"#]), vec![]])
                .unwrap()[1],
            row(&[("q", inferred_variant_type())])
        );
    }

    #[test]
    fn retained_variant_leaf_is_not_charged_twice() {
        let mut session = session(3, 256, 0.1, 0.05);
        session.infer_schema(&[vec![], values(&["5"])]).unwrap();
        session.commit_pending_inference().unwrap();
        assert_eq!(
            session.infer_schema(&[vec![], values(&["6"])]).unwrap(),
            vec![inferred_variant_type(), bigint()]
        );
    }

    #[test]
    fn type_drift_and_array_elements_follow_current_evidence() {
        let mut session = session(300, 10, 0.1, 0.05);
        for (json, expected) in [
            ("1", bigint()),
            ("1.25", DataType::Decimal(DecimalType::new(21, 2).unwrap())),
            (r#"[1,2]"#, DataType::Array(ArrayType::new(bigint()))),
            (
                r#"["s"]"#,
                DataType::Array(ArrayType::new(
                    DataType::VarChar(VarCharType::string_type()),
                )),
            ),
        ] {
            assert_eq!(
                session.infer_schema(&[values(&[json])]).unwrap(),
                vec![expected]
            );
            session.commit_pending_inference().unwrap();
        }
    }

    #[test]
    fn adaptive_session_rejects_invalid_settings() {
        for (sample, admission, retention) in [
            (0, 0.1, 0.05),
            (1, -0.1, 0.05),
            (1, 1.1, 0.05),
            (1, 0.1, -0.1),
            (1, 0.1, 0.2),
        ] {
            assert!(VariantShreddingInferenceSession::new(
                VariantShreddingInferConfig {
                    max_schema_depth: 50,
                    min_field_cardinality_ratio: admission
                },
                300,
                sample,
                retention,
            )
            .is_err());
        }
    }
}
