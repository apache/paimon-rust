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
    eval_row, extract_datum, BinaryRow, BinaryRowBuilder, DataField, Datum, ManifestFileMeta,
    Predicate, PredicateBuilder, PredicateOperator,
};
use crate::table::stats_filter::FileStatsRows;
use std::collections::HashSet;

/// Per-field min/max bounds for stats-based manifest pruning.
#[derive(Debug, Clone)]
pub(crate) struct FieldBounds {
    min: Predicate,
    max: Predicate,
}

struct FieldCandidates<'a> {
    predicate: &'a Predicate,
    values: Vec<Option<&'a Datum>>,
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
        let mut field_candidates = (0..num_fields).map(|_| None).collect::<Vec<_>>();
        let mut residuals = Vec::new();
        collect_eq_candidates(&predicate, &mut field_candidates, &mut residuals);

        if field_candidates.iter().any(|c| c.is_none()) {
            return PartitionFilter::Predicate(predicate);
        }

        let mut partitions = HashSet::new();
        let mut combo: Vec<usize> = vec![0; num_fields];
        loop {
            let mut builder = BinaryRowBuilder::new(num_fields as i32);
            for i in 0..num_fields {
                let vals = &field_candidates[i].as_ref().unwrap().values;
                match vals[combo[i]] {
                    Some(datum) => {
                        builder.write_datum(i, datum, partition_fields[i].data_type());
                    }
                    None => builder.set_null_at(i),
                }
            }
            let row = builder.build();
            // Prove the selected constraint with one comparison, not an IN rescan.
            // Fall back if encoding changes a literal (e.g. timestamp precision)
            // or it is not equal to itself (NaN), preserving full evaluation.
            for (i, candidate) in field_candidates.iter().enumerate() {
                let FieldCandidates {
                    predicate: Predicate::Leaf { data_type, .. },
                    values,
                } = candidate.as_ref().unwrap()
                else {
                    return PartitionFilter::Predicate(predicate);
                };
                match extract_datum(&row, i, data_type) {
                    Ok(value) if value.as_ref() == values[combo[i]] => {}
                    _ => return PartitionFilter::Predicate(predicate),
                }
            }
            let matches = residuals.iter().try_fold(true, |matched, residual| {
                if matched {
                    eval_row(residual, &row)
                } else {
                    Ok(false)
                }
            });
            match matches {
                Ok(true) => {
                    partitions.insert(row.to_serialized_bytes());
                }
                Ok(false) => {}
                // Keep construction infallible; entry matching reports evaluation errors.
                Err(_) => return PartitionFilter::Predicate(predicate),
            }

            let mut carry = true;
            for i in (0..num_fields).rev() {
                if carry {
                    combo[i] += 1;
                    if combo[i] < field_candidates[i].as_ref().unwrap().values.len() {
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

        // These bounds may be wider than the retained set, but remain safe for pruning.
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
        meta: &ManifestFileMeta,
        partition_fields: &[DataField],
    ) -> bool {
        if partition_fields.is_empty() {
            return true;
        }
        let stats = meta.partition_stats();
        let stats = FileStatsRows::for_manifest_partition(
            meta.num_added_files() + meta.num_deleted_files(),
            BinaryRow::from_serialized_bytes(stats.min_values()).ok(),
            BinaryRow::from_serialized_bytes(stats.max_values()).ok(),
            stats.null_counts().clone(),
        );
        match self {
            PartitionFilter::PartitionSet { bounds, .. } => {
                for b in bounds {
                    if !predicate_may_match(&b.min, &stats, partition_fields)
                        || !predicate_may_match(&b.max, &stats, partition_fields)
                    {
                        return false;
                    }
                }
                true
            }
            PartitionFilter::Predicate(pred) => predicate_may_match(pred, &stats, partition_fields),
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
    field_candidates: &[Option<FieldCandidates<'_>>],
    partition_fields: &[DataField],
) -> Option<Vec<FieldBounds>> {
    let pb = PredicateBuilder::new(partition_fields);
    field_candidates
        .iter()
        .enumerate()
        .map(|(i, candidates)| {
            let vals = &candidates.as_ref().unwrap().values;
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

/// Collect `Eq`/`In`/`IsNull` candidate values per partition field.
///
/// Preserve every other condition, including earlier constraints on the same
/// field, as a residual to evaluate before inserting a candidate partition.
/// A `PartitionSet` is the sole authority in `matches_entry`, and exact partition
/// filter pushdown lets DataFusion drop its residual filter, so the set must
/// enforce the complete predicate.
fn collect_eq_candidates<'a>(
    predicate: &'a Predicate,
    field_candidates: &mut [Option<FieldCandidates<'a>>],
    residuals: &mut Vec<&'a Predicate>,
) {
    match predicate {
        Predicate::And(children) => {
            for child in children {
                collect_eq_candidates(child, field_candidates, residuals);
            }
        }
        Predicate::Leaf {
            index,
            op,
            literals,
            ..
        } if *index < field_candidates.len() => {
            let values = match op {
                PredicateOperator::Eq if !literals.is_empty() => vec![Some(&literals[0])],
                PredicateOperator::In if !literals.is_empty() => {
                    literals.iter().map(Some).collect()
                }
                PredicateOperator::IsNull => vec![None],
                _ => {
                    residuals.push(predicate);
                    return;
                }
            };
            if let Some(previous) =
                field_candidates[*index].replace(FieldCandidates { predicate, values })
            {
                // A later constraint on the same field does not supersede the earlier one.
                residuals.push(previous.predicate);
            }
        }
        _ => residuals.push(predicate),
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
    fn test_manifest_pruning_uses_partition_statistics() {
        use crate::spec::stats::BinaryTableStats;

        let fields = partition_fields_dt();
        let pb = PredicateBuilder::new(&fields);
        let mut min = BinaryRowBuilder::new(1);
        min.write_string(0, "2024-01-01");
        let mut max = BinaryRowBuilder::new(1);
        max.write_string(0, "2024-01-03");
        let meta = ManifestFileMeta::new(
            "manifest".into(),
            1,
            10,
            5,
            BinaryTableStats::new(
                min.build_serialized(),
                max.build_serialized(),
                vec![Some(0)],
            ),
            0,
        );
        for (predicate, expected) in [
            (
                pb.equal("dt", Datum::String("2024-01-02".into())).unwrap(),
                true,
            ),
            (
                pb.equal("dt", Datum::String("2024-01-04".into())).unwrap(),
                false,
            ),
            (
                pb.greater_than("dt", Datum::String("2024-01-01".into()))
                    .unwrap(),
                true,
            ),
            (
                pb.greater_than("dt", Datum::String("2024-01-03".into()))
                    .unwrap(),
                false,
            ),
            (pb.is_null("dt").unwrap(), false),
        ] {
            let filter = PartitionFilter::from_predicate(predicate, &fields);
            assert_eq!(filter.matches_manifest(&meta, &fields), expected);
            assert!(filter.matches_manifest(&meta, &[]));
            let unknown = ManifestFileMeta::new(
                "unknown-stats".into(),
                1,
                10,
                5,
                BinaryTableStats::new(vec![0xFF], vec![0xFF], vec![]),
                0,
            );
            assert!(filter.matches_manifest(&unknown, &fields));
        }
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
    fn test_partition_candidates_preserve_all_conjuncts() {
        let fields = partition_fields_dt();
        let pb = PredicateBuilder::new(&fields);
        let a = pb.equal("dt", Datum::String("a".into())).unwrap();
        let b = pb.equal("dt", Datum::String("b".into())).unwrap();
        let candidates = pb
            .is_in(
                "dt",
                vec![Datum::String("a".into()), Datum::String("b".into())],
            )
            .unwrap();
        for (predicate, expected) in [
            (
                Predicate::and(vec![
                    candidates.clone(),
                    pb.greater_than("dt", Datum::String("a".into())).unwrap(),
                ]),
                vec![Some("b")],
            ),
            (
                Predicate::and(vec![
                    candidates.clone(),
                    pb.is_in(
                        "dt",
                        vec![Datum::String("b".into()), Datum::String("c".into())],
                    )
                    .unwrap(),
                ]),
                vec![Some("b")],
            ),
            (Predicate::and(vec![a.clone(), b.clone()]), vec![]),
            (
                Predicate::and(vec![
                    candidates.clone(),
                    Predicate::or(vec![b, pb.equal("dt", Datum::String("c".into())).unwrap()]),
                ]),
                vec![Some("b")],
            ),
            (
                Predicate::and(vec![candidates, Predicate::negate(a)]),
                vec![Some("b")],
            ),
            (
                Predicate::and(vec![
                    pb.is_null("dt").unwrap(),
                    pb.is_not_null("dt").unwrap(),
                ]),
                vec![],
            ),
        ] {
            let filter = PartitionFilter::from_predicate(predicate.clone(), &fields);
            assert!(matches!(filter, PartitionFilter::PartitionSet { .. }));
            for value in [None, Some("a"), Some("b"), Some("c")] {
                let mut row = BinaryRowBuilder::new(1);
                match value {
                    Some(value) => {
                        row.write_datum(0, &Datum::String(value.into()), fields[0].data_type())
                    }
                    None => row.set_null_at(0),
                }
                assert_eq!(
                    filter.matches_entry(&row.build_serialized()).unwrap(),
                    expected.contains(&value),
                    "{predicate:?}, {value:?}"
                );
            }
        }
    }

    #[test]
    fn test_large_in_candidates_with_residual_filters() {
        let fields = vec![DataField::new(
            0,
            "id".into(),
            DataType::Int(IntType::new()),
        )];
        let pb = PredicateBuilder::new(&fields);
        for size in [2_000, 4_000] {
            for with_range in [false, true] {
                let list = pb.is_in("id", (0..size).map(Datum::Int).collect()).unwrap();
                let predicate = if with_range {
                    Predicate::and(vec![
                        list,
                        pb.greater_or_equal("id", Datum::Int(size / 2)).unwrap(),
                    ])
                } else {
                    list
                };
                {
                    let mut candidates = vec![None];
                    let mut residuals = Vec::new();
                    collect_eq_candidates(&predicate, &mut candidates, &mut residuals);
                    assert_eq!(candidates[0].as_ref().unwrap().values.len(), size as usize);
                    assert_eq!(residuals.len(), usize::from(with_range));
                    assert!(residuals.iter().all(|p| matches!(
                        p,
                        Predicate::Leaf {
                            op: PredicateOperator::GtEq,
                            ..
                        }
                    )));
                }
                let started = std::time::Instant::now();
                let filter = PartitionFilter::from_predicate(predicate, &fields);
                println!(
                    "IN size={size}, range={with_range}: {:?}",
                    started.elapsed()
                );
                let PartitionFilter::PartitionSet { partitions, .. } = &filter else {
                    panic!("large IN must retain constant-time partition lookup");
                };
                assert_eq!(
                    partitions.len(),
                    if with_range { size / 2 } else { size } as usize
                );
                for value in [-1, 0, size / 2 - 1, size / 2, size - 1, size] {
                    let mut row = BinaryRowBuilder::new(1);
                    row.write_int(0, value);
                    assert_eq!(
                        filter.matches_entry(&row.build_serialized()).unwrap(),
                        (if with_range { size / 2 } else { 0 }..size).contains(&value)
                    );
                }
            }
        }
    }

    #[test]
    fn test_non_roundtripping_candidates_keep_full_predicate() {
        let timestamp = |millis, nanos| Datum::Timestamp { millis, nanos };
        for (data_type, literals, probes) in [
            (
                DataType::Double(crate::spec::DoubleType::new()),
                vec![Datum::Double(f64::NAN), Datum::Double(1.0)],
                vec![
                    (Datum::Double(f64::NAN), false),
                    (Datum::Double(1.0), true),
                    (Datum::Double(2.0), false),
                ],
            ),
            (
                DataType::Timestamp(crate::spec::TimestampType::new(3).unwrap()),
                vec![timestamp(5, 1), timestamp(5, 0)],
                vec![(timestamp(5, 0), true), (timestamp(6, 0), false)],
            ),
        ] {
            let fields = vec![DataField::new(0, "key".into(), data_type.clone())];
            let predicate = PredicateBuilder::new(&fields)
                .is_in("key", literals)
                .unwrap();
            let filter = PartitionFilter::from_predicate(predicate, &fields);
            assert!(matches!(filter, PartitionFilter::Predicate(_)));
            for (value, expected) in probes {
                let mut row = BinaryRowBuilder::new(1);
                row.write_datum(0, &value, &data_type);
                assert_eq!(
                    filter.matches_entry(&row.build_serialized()).unwrap(),
                    expected,
                    "{value:?}"
                );
            }
        }
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

    fn serialized_dt(fields: &[DataField], dt: &str) -> Vec<u8> {
        let mut builder = BinaryRowBuilder::new(1);
        builder.write_datum(0, &Datum::String(dt.into()), fields[0].data_type());
        builder.build_serialized()
    }

    /// A residual range must reject a candidate that contradicts it.
    #[test]
    fn test_unexpressible_conjunct_on_covered_field_is_preserved() {
        let fields = partition_fields_dt();
        let pb = PredicateBuilder::new(&fields);
        let pred = Predicate::and(vec![
            pb.equal("dt", Datum::String("2024-01-01".into())).unwrap(),
            pb.greater_or_equal("dt", Datum::String("2024-01-02".into()))
                .unwrap(),
        ]);
        let filter = PartitionFilter::from_predicate(pred, &fields);
        assert!(!filter
            .matches_entry(&serialized_dt(&fields, "2024-01-01"))
            .unwrap());
    }

    /// Two expressible conjuncts on one field: the second assignment used to
    /// overwrite the first, keeping whichever came last — here the wider `In`.
    #[test]
    fn test_second_conjunct_on_same_field_is_preserved() {
        let fields = partition_fields_dt();
        let pb = PredicateBuilder::new(&fields);
        let pred = Predicate::and(vec![
            pb.equal("dt", Datum::String("2024-01-02".into())).unwrap(),
            pb.is_in(
                "dt",
                vec![
                    Datum::String("2024-01-01".into()),
                    Datum::String("2024-01-02".into()),
                ],
            )
            .unwrap(),
        ]);
        let filter = PartitionFilter::from_predicate(pred, &fields);
        assert!(!filter
            .matches_entry(&serialized_dt(&fields, "2024-01-01"))
            .unwrap());
        assert!(filter
            .matches_entry(&serialized_dt(&fields, "2024-01-02"))
            .unwrap());
    }

    /// An `Or` over the partition field narrows the `In` beside it.
    #[test]
    fn test_or_conjunct_beside_covering_in_is_preserved() {
        let fields = partition_fields_dt();
        let pb = PredicateBuilder::new(&fields);
        let pred = Predicate::and(vec![
            Predicate::or(vec![
                pb.equal("dt", Datum::String("2024-01-01".into())).unwrap(),
                pb.equal("dt", Datum::String("2024-01-02".into())).unwrap(),
            ]),
            pb.is_in(
                "dt",
                vec![
                    Datum::String("2024-01-01".into()),
                    Datum::String("2024-01-02".into()),
                    Datum::String("2024-01-03".into()),
                ],
            )
            .unwrap(),
        ]);
        let filter = PartitionFilter::from_predicate(pred, &fields);
        assert!(!filter
            .matches_entry(&serialized_dt(&fields, "2024-01-03"))
            .unwrap());
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
