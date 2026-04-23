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

//! Partition filter for efficient manifest entry pruning.
//!
//! Reference: [Java MultiplePartitionPredicate](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/partition/PartitionPredicate.java)

use crate::predicate_stats::data_leaf_may_match;
use crate::spec::{
    eval_row, extract_datum, BinaryRow, BinaryRowBuilder, DataField, Datum, Predicate,
    PredicateBuilder, PredicateOperator,
};
use crate::table::stats_filter::FileStatsRows;
use std::collections::HashSet;

/// Per-field min/max bounds for stats-based manifest pruning.
#[derive(Debug, Clone)]
pub(crate) struct FieldBounds {
    min: Predicate,
    max: Predicate,
}

#[derive(Debug, Clone)]
pub(crate) enum PartitionFilter {
    /// Multiple known partitions: O(1) entry matching via HashSet,
    /// per-field min/max bounds for manifest-level stats pruning.
    PartitionSet {
        partitions: HashSet<Vec<u8>>,
        bounds: Vec<FieldBounds>,
    },
    /// General predicate for range/complex filters.
    Predicate(Predicate),
}

impl PartitionFilter {
    pub fn from_predicate(predicate: Predicate, partition_fields: &[DataField]) -> Self {
        if partition_fields.is_empty() {
            return PartitionFilter::Predicate(predicate);
        }

        let num_fields = partition_fields.len();
        let mut field_candidates: Vec<Option<Vec<Option<&Datum>>>> = vec![None; num_fields];
        collect_eq_candidates(&predicate, &mut field_candidates);

        if field_candidates.iter().any(|c| c.is_none()) {
            return PartitionFilter::Predicate(predicate);
        }

        let mut partitions = HashSet::new();
        let mut combo: Vec<usize> = vec![0; num_fields];
        loop {
            let mut builder = BinaryRowBuilder::new(num_fields as i32);
            for i in 0..num_fields {
                let vals = field_candidates[i].as_ref().unwrap();
                match vals[combo[i]] {
                    Some(datum) => {
                        builder.write_datum(i, datum, partition_fields[i].data_type());
                    }
                    None => builder.set_null_at(i),
                }
            }
            partitions.insert(builder.build_serialized());

            let mut carry = true;
            for i in (0..num_fields).rev() {
                if carry {
                    combo[i] += 1;
                    if combo[i] < field_candidates[i].as_ref().unwrap().len() {
                        carry = false;
                    } else {
                        combo[i] = 0;
                    }
                }
            }
            if carry {
                break;
            }
        }

        let bounds = match build_bounds_from_candidates(&field_candidates, partition_fields) {
            Some(b) => b,
            None => return PartitionFilter::Predicate(predicate),
        };

        PartitionFilter::PartitionSet { partitions, bounds }
    }

    pub fn from_partition_set(
        partitions: HashSet<Vec<u8>>,
        partition_fields: &[DataField],
    ) -> crate::Result<Self> {
        let bounds = build_bounds_from_partition_bytes(&partitions, partition_fields)?;
        Ok(PartitionFilter::PartitionSet { partitions, bounds })
    }

    pub fn matches_entry(&self, serialized_partition: &[u8]) -> crate::Result<bool> {
        match self {
            PartitionFilter::PartitionSet { partitions, .. } => {
                Ok(partitions.contains(serialized_partition))
            }
            PartitionFilter::Predicate(pred) => {
                match BinaryRow::from_serialized_bytes(serialized_partition) {
                    Ok(row) => eval_row(pred, &row),
                    Err(_) => Ok(true),
                }
            }
        }
    }

    pub(super) fn matches_manifest(
        &self,
        stats: &FileStatsRows,
        partition_fields: &[DataField],
    ) -> bool {
        match self {
            PartitionFilter::PartitionSet { bounds, .. } => {
                for b in bounds {
                    if !predicate_may_match(&b.min, stats, partition_fields)
                        || !predicate_may_match(&b.max, stats, partition_fields)
                    {
                        return false;
                    }
                }
                true
            }
            PartitionFilter::Predicate(pred) => predicate_may_match(pred, stats, partition_fields),
        }
    }
}

fn predicate_may_match(
    predicate: &Predicate,
    stats: &FileStatsRows,
    partition_fields: &[DataField],
) -> bool {
    match predicate {
        Predicate::AlwaysTrue => true,
        Predicate::AlwaysFalse => false,
        Predicate::And(children) => children
            .iter()
            .all(|child| predicate_may_match(child, stats, partition_fields)),
        Predicate::Or(children) => children
            .iter()
            .any(|child| predicate_may_match(child, stats, partition_fields)),
        Predicate::Not(_) => true,
        Predicate::Leaf {
            index,
            data_type,
            op,
            literals,
            ..
        } => {
            let stats_data_type = match partition_fields.get(*index) {
                Some(f) => f.data_type(),
                None => return true,
            };
            data_leaf_may_match(*index, stats_data_type, data_type, *op, literals, stats)
        }
    }
}

/// Build per-field min/max bounds from candidate values (from `collect_eq_candidates`).
fn build_bounds_from_candidates(
    field_candidates: &[Option<Vec<Option<&Datum>>>],
    partition_fields: &[DataField],
) -> Option<Vec<FieldBounds>> {
    let pb = PredicateBuilder::new(partition_fields);
    field_candidates
        .iter()
        .enumerate()
        .map(|(i, candidates)| {
            let vals = candidates.as_ref().unwrap();
            build_field_bounds(&pb, partition_fields[i].name(), vals)
        })
        .collect()
}

/// Build per-field min/max bounds from raw partition bytes.
fn build_bounds_from_partition_bytes(
    partitions: &HashSet<Vec<u8>>,
    partition_fields: &[DataField],
) -> crate::Result<Vec<FieldBounds>> {
    let num_fields = partition_fields.len();
    let partition_count = partitions.len();
    let mut field_values: Vec<Vec<Option<Datum>>> = (0..num_fields)
        .map(|_| Vec::with_capacity(partition_count))
        .collect();

    for bytes in partitions {
        let row = BinaryRow::from_serialized_bytes(bytes)?;
        for (i, field) in partition_fields.iter().enumerate() {
            let datum = extract_datum(&row, i, field.data_type())?;
            field_values[i].push(datum);
        }
    }

    let pb = PredicateBuilder::new(partition_fields);
    let mut bounds = Vec::with_capacity(num_fields);
    for (i, vals) in field_values.iter().enumerate() {
        let refs: Vec<Option<&Datum>> = vals.iter().map(|d| d.as_ref()).collect();
        match build_field_bounds(&pb, partition_fields[i].name(), &refs) {
            Some(b) => bounds.push(b),
            None => return Ok(Vec::new()),
        }
    }
    Ok(bounds)
}

/// Build min/max bounds for a single field from its candidate values.
///
/// Mirrors Java's `MultiplePartitionPredicate` constructor logic:
/// - All null → isNull for both min and max
/// - Some null → OR(isNull, greaterOrEqual(min)) / OR(isNull, lessOrEqual(max))
/// - No null → greaterOrEqual(min) / lessOrEqual(max)
fn build_field_bounds(
    pb: &PredicateBuilder,
    field_name: &str,
    values: &[Option<&Datum>],
) -> Option<FieldBounds> {
    let null_count = values.iter().filter(|v| v.is_none()).count();
    let total = values.len();

    if null_count == total {
        let is_null = pb.is_null(field_name).ok()?;
        return Some(FieldBounds {
            min: is_null.clone(),
            max: is_null,
        });
    }

    let non_null: Vec<&Datum> = values.iter().filter_map(|v| *v).collect();
    let min_val = non_null
        .iter()
        .copied()
        .min_by(|a, b| crate::spec::datum_cmp(a, b).unwrap_or(std::cmp::Ordering::Equal))
        .unwrap()
        .clone();
    let max_val = non_null
        .iter()
        .copied()
        .max_by(|a, b| crate::spec::datum_cmp(a, b).unwrap_or(std::cmp::Ordering::Equal))
        .unwrap()
        .clone();

    let mut min_pred = pb.greater_or_equal(field_name, min_val).ok()?;
    let mut max_pred = pb.less_or_equal(field_name, max_val).ok()?;

    if null_count > 0 {
        let is_null = pb.is_null(field_name).ok()?;
        min_pred = Predicate::or(vec![is_null.clone(), min_pred]);
        max_pred = Predicate::or(vec![is_null, max_pred]);
    }

    Some(FieldBounds {
        min: min_pred,
        max: max_pred,
    })
}

fn collect_eq_candidates<'a>(
    predicate: &'a Predicate,
    field_candidates: &mut Vec<Option<Vec<Option<&'a Datum>>>>,
) {
    match predicate {
        Predicate::And(children) => {
            for child in children {
                collect_eq_candidates(child, field_candidates);
            }
        }
        Predicate::Leaf {
            index,
            op,
            literals,
            ..
        } if *index < field_candidates.len() => match op {
            PredicateOperator::Eq => {
                if let Some(lit) = literals.first() {
                    field_candidates[*index] = Some(vec![Some(lit)]);
                }
            }
            PredicateOperator::In if !literals.is_empty() => {
                field_candidates[*index] = Some(literals.iter().map(Some).collect());
            }
            PredicateOperator::IsNull => {
                field_candidates[*index] = Some(vec![None]);
            }
            _ => {}
        },
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{DataType, IntType, VarCharType};

    fn partition_fields_dt_hr() -> Vec<DataField> {
        vec![
            DataField::new(
                0,
                "dt".to_string(),
                DataType::VarChar(VarCharType::default()),
            ),
            DataField::new(1, "hr".to_string(), DataType::Int(IntType::new())),
        ]
    }

    fn partition_fields_dt() -> Vec<DataField> {
        vec![DataField::new(
            0,
            "dt".to_string(),
            DataType::VarChar(VarCharType::default()),
        )]
    }

    #[test]
    fn test_eq_builds_partition_set() {
        let fields = partition_fields_dt();
        let pb = PredicateBuilder::new(&fields);
        let pred = pb.equal("dt", Datum::String("2024-01-01".into())).unwrap();
        let filter = PartitionFilter::from_predicate(pred, &fields);
        assert!(matches!(filter, PartitionFilter::PartitionSet { .. }));

        let mut builder = BinaryRowBuilder::new(1);
        builder.write_datum(
            0,
            &Datum::String("2024-01-01".into()),
            fields[0].data_type(),
        );
        let matching = builder.build_serialized();
        assert!(filter.matches_entry(&matching).unwrap());

        let mut builder = BinaryRowBuilder::new(1);
        builder.write_datum(
            0,
            &Datum::String("2024-01-02".into()),
            fields[0].data_type(),
        );
        let non_matching = builder.build_serialized();
        assert!(!filter.matches_entry(&non_matching).unwrap());
    }

    #[test]
    fn test_in_builds_partition_set() {
        let fields = partition_fields_dt();
        let pb = PredicateBuilder::new(&fields);
        let pred = pb
            .is_in(
                "dt",
                vec![
                    Datum::String("2024-01-01".into()),
                    Datum::String("2024-01-02".into()),
                ],
            )
            .unwrap();
        let filter = PartitionFilter::from_predicate(pred, &fields);
        assert!(matches!(filter, PartitionFilter::PartitionSet { .. }));

        for dt in ["2024-01-01", "2024-01-02"] {
            let mut builder = BinaryRowBuilder::new(1);
            builder.write_datum(0, &Datum::String(dt.into()), fields[0].data_type());
            assert!(filter.matches_entry(&builder.build_serialized()).unwrap());
        }

        let mut builder = BinaryRowBuilder::new(1);
        builder.write_datum(
            0,
            &Datum::String("2024-01-03".into()),
            fields[0].data_type(),
        );
        assert!(!filter.matches_entry(&builder.build_serialized()).unwrap());
    }

    #[test]
    fn test_composite_partition_cartesian_product() {
        let fields = partition_fields_dt_hr();
        let pb = PredicateBuilder::new(&fields);
        let pred = Predicate::and(vec![
            pb.is_in(
                "dt",
                vec![
                    Datum::String("2024-01-01".into()),
                    Datum::String("2024-01-02".into()),
                ],
            )
            .unwrap(),
            pb.is_in("hr", vec![Datum::Int(10), Datum::Int(20)])
                .unwrap(),
        ]);
        let filter = PartitionFilter::from_predicate(pred, &fields);
        match &filter {
            PartitionFilter::PartitionSet { partitions, .. } => {
                assert_eq!(partitions.len(), 4);
            }
            _ => panic!("expected PartitionSet"),
        }

        for (dt, hr) in [
            ("2024-01-01", 10),
            ("2024-01-01", 20),
            ("2024-01-02", 10),
            ("2024-01-02", 20),
        ] {
            let mut builder = BinaryRowBuilder::new(2);
            builder.write_datum(0, &Datum::String(dt.into()), fields[0].data_type());
            builder.write_datum(1, &Datum::Int(hr), fields[1].data_type());
            assert!(filter.matches_entry(&builder.build_serialized()).unwrap());
        }

        let mut builder = BinaryRowBuilder::new(2);
        builder.write_datum(
            0,
            &Datum::String("2024-01-03".into()),
            fields[0].data_type(),
        );
        builder.write_datum(1, &Datum::Int(10), fields[1].data_type());
        assert!(!filter.matches_entry(&builder.build_serialized()).unwrap());
    }

    #[test]
    fn test_range_predicate_falls_back() {
        let fields = partition_fields_dt();
        let pb = PredicateBuilder::new(&fields);
        let pred = pb
            .greater_than("dt", Datum::String("2024-01-01".into()))
            .unwrap();
        let filter = PartitionFilter::from_predicate(pred, &fields);
        assert!(matches!(filter, PartitionFilter::Predicate(_)));

        let mut builder = BinaryRowBuilder::new(1);
        builder.write_datum(
            0,
            &Datum::String("2024-01-02".into()),
            fields[0].data_type(),
        );
        assert!(filter.matches_entry(&builder.build_serialized()).unwrap());
    }

    #[test]
    fn test_partial_coverage_falls_back() {
        let fields = partition_fields_dt_hr();
        let pb = PredicateBuilder::new(&fields);
        let pred = pb.equal("dt", Datum::String("2024-01-01".into())).unwrap();
        let filter = PartitionFilter::from_predicate(pred, &fields);
        assert!(matches!(filter, PartitionFilter::Predicate(_)));
    }

    #[test]
    fn test_is_null_in_partition_set() {
        let fields = partition_fields_dt();
        let pb = PredicateBuilder::new(&fields);
        let pred = pb.is_null("dt").unwrap();
        let filter = PartitionFilter::from_predicate(pred, &fields);
        assert!(matches!(filter, PartitionFilter::PartitionSet { .. }));

        let mut builder = BinaryRowBuilder::new(1);
        builder.set_null_at(0);
        assert!(filter.matches_entry(&builder.build_serialized()).unwrap());

        let mut builder = BinaryRowBuilder::new(1);
        builder.write_datum(
            0,
            &Datum::String("2024-01-01".into()),
            fields[0].data_type(),
        );
        assert!(!filter.matches_entry(&builder.build_serialized()).unwrap());
    }

    #[test]
    fn test_decode_failure_in_predicate_mode_fails_open() {
        let fields = partition_fields_dt();
        let pb = PredicateBuilder::new(&fields);
        let pred = pb
            .greater_than("dt", Datum::String("2024-01-01".into()))
            .unwrap();
        let filter = PartitionFilter::from_predicate(pred, &fields);
        assert!(filter.matches_entry(&[0xFF, 0x00]).unwrap());
    }

    #[test]
    fn test_from_partition_set() {
        let fields = partition_fields_dt();
        let mut partitions = HashSet::new();
        for dt in ["2024-01-01", "2024-01-02"] {
            let mut builder = BinaryRowBuilder::new(1);
            builder.write_datum(0, &Datum::String(dt.into()), fields[0].data_type());
            partitions.insert(builder.build_serialized());
        }
        let filter = PartitionFilter::from_partition_set(partitions, &fields).unwrap();
        assert!(matches!(filter, PartitionFilter::PartitionSet { .. }));

        let mut builder = BinaryRowBuilder::new(1);
        builder.write_datum(
            0,
            &Datum::String("2024-01-01".into()),
            fields[0].data_type(),
        );
        assert!(filter.matches_entry(&builder.build_serialized()).unwrap());

        let mut builder = BinaryRowBuilder::new(1);
        builder.write_datum(
            0,
            &Datum::String("2024-01-03".into()),
            fields[0].data_type(),
        );
        assert!(!filter.matches_entry(&builder.build_serialized()).unwrap());
    }
}
