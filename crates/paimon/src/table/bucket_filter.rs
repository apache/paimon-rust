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

//! Bucket and partition predicate extraction and bucket hash pruning.

use crate::spec::{
    eval_row, field_idx_to_partition_idx, BinaryRow, BucketFunctionType, CoreOptions, DataField,
    DataType, Datum, Predicate, PredicateOperator,
};
use crate::table::bucket_function::bucket_for_datums;
use crate::table::Table;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

const MAX_VALUES: usize = 1000;

/// Java's BucketSelector keeps a separate selector for each concrete partition.
/// Clone the immutable configuration for concurrent manifest tasks; each task
/// caches partition simplification and bucket counts without a shared lock.
#[derive(Clone)]
pub(super) struct BucketSelector {
    config: Arc<BucketSelectorConfig>,
    partitions: HashMap<Vec<u8>, PartitionSelector>,
}

struct BucketSelectorConfig {
    predicate: Predicate,
    manifest_predicate: Option<Predicate>,
    partition_mapping: Vec<Option<usize>>,
    bucket_mapping: Vec<Option<usize>>,
    partition_arity: usize,
    partition_dependent: bool,
    bucket_fields: Vec<DataField>,
    function: BucketFunctionType,
}

#[derive(Clone)]
struct PartitionSelector {
    predicate: Option<Predicate>,
    buckets: HashMap<i32, Option<HashSet<i32>>>,
}

impl BucketSelector {
    pub(super) fn new(table: &Table, predicate: &Predicate) -> crate::Result<Option<Self>> {
        let options = CoreOptions::new(table.schema().options());
        // Dynamic and cross-partition bucket assignments are index-driven,
        // rather than a fixed hash modulo the file's bucket count.
        if options.bucket() < 1 && options.bucket() != -2 {
            return Ok(None);
        }
        let keys = options
            .bucket_key()
            .unwrap_or_else(|| table.schema().trimmed_primary_keys());
        let fields = table.schema().fields();
        let bucket_fields = keys
            .iter()
            .filter_map(|name| fields.iter().find(|field| field.name() == name).cloned())
            .collect::<Vec<_>>();
        if keys.is_empty() || bucket_fields.len() != keys.len() {
            return Ok(None);
        }
        let bucket_mapping = field_idx_to_partition_idx(fields, &keys);
        let partition_mapping = field_idx_to_partition_idx(fields, table.schema().partition_keys());
        let mut referenced = HashSet::new();
        predicate.collect_leaf_field_indices(&mut referenced);
        let partition_dependent = partition_mapping
            .iter()
            .enumerate()
            .any(|(index, mapped)| mapped.is_some() && referenced.contains(&index));
        let config = BucketSelectorConfig {
            predicate: predicate.clone(),
            manifest_predicate: project_bucket_predicate(predicate, &bucket_mapping),
            partition_mapping,
            bucket_mapping,
            partition_arity: table.schema().partition_keys().len(),
            partition_dependent,
            bucket_fields,
            function: options.bucket_function_type()?,
        };
        Ok(Some(Self {
            config: Arc::new(config),
            partitions: HashMap::new(),
        }))
    }

    pub(super) fn manifest_predicate(&self) -> Option<&Predicate> {
        self.config.manifest_predicate.as_ref()
    }

    pub(super) fn bucket_fields(&self) -> &[DataField] {
        &self.config.bucket_fields
    }

    pub(super) fn function(&self) -> BucketFunctionType {
        self.config.function
    }

    pub(super) fn test(&mut self, partition: &[u8], bucket: i32, total_buckets: i32) -> bool {
        // Pending buckets carry rows with no hash assignment yet. Java keeps
        // them independently of whether this scan excludes pending buckets.
        if bucket < 0 || total_buckets <= 0 {
            return true;
        }
        let config = &self.config;
        // A key-only predicate has the same selector in every partition.
        let partition = if config.partition_dependent {
            partition
        } else {
            &[]
        };
        if !self.partitions.contains_key(partition) {
            let predicate = if !config.partition_dependent {
                config.manifest_predicate.clone()
            } else {
                BinaryRow::from_serialized_bytes(partition)
                    .ok()
                    .filter(|row| row.arity() as usize == config.partition_arity)
                    .and_then(|row| {
                        replace_partition_predicates(
                            &config.predicate,
                            &config.partition_mapping,
                            &row,
                        )
                        .ok()
                    })
                    .map(|predicate| project_bucket_predicate(&predicate, &config.bucket_mapping))
                    .unwrap_or_else(|| config.manifest_predicate.clone())
            };
            self.partitions.insert(
                partition.to_vec(),
                PartitionSelector {
                    predicate,
                    buckets: HashMap::new(),
                },
            );
        }
        let selector = self.partitions.get_mut(partition).unwrap();
        selector
            .buckets
            .entry(total_buckets)
            .or_insert_with(|| config.targets(selector.predicate.as_ref(), total_buckets))
            .as_ref()
            .is_none_or(|targets| targets.contains(&bucket))
    }
}

impl BucketSelectorConfig {
    fn targets(&self, predicate: Option<&Predicate>, total_buckets: i32) -> Option<HashSet<i32>> {
        compute_target_buckets(
            predicate?,
            &self.bucket_fields,
            self.function,
            total_buckets,
        )
    }
}

fn replace_partition_predicates(
    predicate: &Predicate,
    mapping: &[Option<usize>],
    row: &BinaryRow,
) -> crate::Result<Predicate> {
    Ok(match predicate {
        Predicate::And(children) => Predicate::and(
            children
                .iter()
                .map(|child| replace_partition_predicates(child, mapping, row))
                .collect::<crate::Result<_>>()?,
        ),
        Predicate::Or(children) => Predicate::or(
            children
                .iter()
                .map(|child| replace_partition_predicates(child, mapping, row))
                .collect::<crate::Result<_>>()?,
        ),
        Predicate::Not(inner) => {
            Predicate::negate(replace_partition_predicates(inner, mapping, row)?)
        }
        Predicate::Leaf { .. } => match predicate.remap_field_index(mapping) {
            Some(partition) => {
                if eval_row(&partition, row)? {
                    Predicate::AlwaysTrue
                } else {
                    Predicate::AlwaysFalse
                }
            }
            None => predicate.clone(),
        },
        _ => predicate.clone(),
    })
}

// Java pickTransformFieldMapping keeps complete top-level conjuncts. A mixed
// OR must never contribute just one of its branches to bucket selection.
fn project_bucket_predicate(predicate: &Predicate, mapping: &[Option<usize>]) -> Option<Predicate> {
    let projected = predicate
        .clone()
        .split_and()
        .iter()
        .filter_map(|conjunct| conjunct.remap_field_index(mapping))
        .collect::<Vec<_>>();
    (!projected.is_empty()).then(|| Predicate::and(projected))
}

pub(super) fn split_partition_and_data_predicates(
    filter: Predicate,
    fields: &[DataField],
    partition_keys: &[String],
) -> (Option<Predicate>, Vec<Predicate>) {
    let mapping = field_idx_to_partition_idx(fields, partition_keys);
    let mut partition_predicates = Vec::new();
    let mut data_predicates = Vec::new();

    for conjunct in filter.split_and() {
        let strict_partition_only = conjunct.references_only_mapped_fields(&mapping);

        if let Some(projected) = conjunct.project_field_index_inclusive(&mapping) {
            partition_predicates.push(projected);
        }

        // Keep any conjunct that is not fully partition-only for data-level
        // stats pruning, even if part of it contributed to partition pruning.
        if !strict_partition_only {
            data_predicates.push(conjunct);
        }
    }

    let partition_predicate = if partition_predicates.is_empty() {
        None
    } else {
        Some(Predicate::and(partition_predicates))
    };

    (partition_predicate, data_predicates)
}

/// Extract a predicate projected onto the given key columns.
///
/// This is a generic utility that works for both partition keys and bucket keys.
/// Returns `None` if no conjuncts reference the given keys.
pub(super) fn extract_predicate_for_keys(
    filter: &Predicate,
    fields: &[DataField],
    keys: &[String],
) -> Option<Predicate> {
    if keys.is_empty() {
        return None;
    }
    let mapping = field_idx_to_partition_idx(fields, keys);
    let projected: Vec<Predicate> = filter
        .clone()
        .split_and()
        .into_iter()
        .filter_map(|conjunct| conjunct.project_field_index_inclusive(&mapping))
        .collect();
    if projected.is_empty() {
        None
    } else {
        Some(Predicate::and(projected))
    }
}

/// Compute the set of target buckets from a bucket predicate.
///
/// Extracts equal-value literals for each bucket key field from the predicate,
/// builds a BinaryRow, hashes it, and returns the target bucket(s).
///
/// Supports:
/// - `key = value` (single bucket)
/// - `key IN (v1, v2, ...)` (multiple buckets)
/// - AND of the above for composite bucket keys
///
/// Returns `None` if the predicate cannot determine target buckets (fail-open).
pub(super) fn compute_target_buckets(
    bucket_predicate: &Predicate,
    bucket_key_fields: &[DataField],
    bucket_function_type: BucketFunctionType,
    total_buckets: i32,
) -> Option<HashSet<i32>> {
    if total_buckets <= 0 || bucket_key_fields.is_empty() {
        return None;
    }

    // Collect equal-value candidates per bucket key field (by projected index).
    // Java accepts ORs of Equal/In leaves on one key, intersecting repeated
    // AND constraints. Other conjuncts do not help select buckets.
    let num_keys = bucket_key_fields.len();
    let mut field_candidates: Vec<Option<Vec<&Datum>>> = vec![None; num_keys];

    collect_and_candidates(bucket_predicate, &mut field_candidates)?;

    // All bucket key fields must have candidates.
    let candidates: Vec<&Vec<&Datum>> =
        field_candidates.iter().filter_map(|c| c.as_ref()).collect();
    if candidates.len() != num_keys {
        return None;
    }
    let mut combinations = 1usize;
    for values in &candidates {
        combinations = combinations.checked_mul(values.len())?;
        if combinations > MAX_VALUES {
            return None;
        }
    }
    if combinations == 0 {
        return Some(HashSet::new());
    }

    // Compute cartesian product of candidates and hash each combination.
    let mut buckets = HashSet::new();
    let mut combo: Vec<usize> = vec![0; num_keys];
    loop {
        let datums: Vec<(Option<&Datum>, &DataType)> = (0..num_keys)
            .map(|i| {
                let vals = field_candidates[i].as_ref().unwrap();
                (Some(vals[combo[i]]), bucket_key_fields[i].data_type())
            })
            .collect();

        let bucket = bucket_for_datums(&datums, bucket_function_type, total_buckets).ok()?;
        buckets.insert(bucket);

        // Advance the combination counter (rightmost first).
        let mut carry = true;
        for i in (0..num_keys).rev() {
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

    if buckets.is_empty() {
        None
    } else {
        Some(buckets)
    }
}

fn collect_and_candidates<'a>(
    predicate: &'a Predicate,
    candidates: &mut [Option<Vec<&'a Datum>>],
) -> Option<()> {
    if let Predicate::And(children) = predicate {
        for child in children {
            collect_and_candidates(child, candidates)?;
        }
    } else if let Some((index, values)) = collect_or_candidates(predicate, candidates.len()) {
        let mut values = values;
        if let Some(previous) = &mut candidates[index] {
            // Preserve the first conjunct's order and duplicates, as retainAll
            // does in Java, without quadratic comparisons for large IN lists.
            if let Some(first) = values.first().or(previous.first()) {
                // Variant equality is defined for identical payloads, but
                // distinct payloads have no ordering for sorting/searching.
                if matches!(first, Datum::Variant { .. }) {
                    return None;
                }
                let kind = std::mem::discriminant(*first);
                if values.iter().chain(previous.iter()).any(|value| {
                    std::mem::discriminant(*value) != kind || value.partial_cmp(value).is_none()
                }) {
                    return None;
                }
            }
            values.sort_unstable_by(|left, right| left.partial_cmp(right).unwrap());
            previous.retain(|value| {
                values
                    .binary_search_by(|candidate| candidate.partial_cmp(value).unwrap())
                    .is_ok()
            });
            // Java fails open rather than using a contradictory candidate set.
            if previous.is_empty() {
                return None;
            }
        } else {
            candidates[index] = Some(values);
        }
    }
    Some(())
}

/// One OR clause is usable only when every branch is Equal/In on the same key.
fn collect_or_candidates(predicate: &Predicate, num_keys: usize) -> Option<(usize, Vec<&Datum>)> {
    match predicate {
        Predicate::Or(children) => {
            let mut reference = None;
            let mut values = Vec::new();
            for child in children {
                let (index, child_values) = collect_or_candidates(child, num_keys)?;
                if reference.is_some_and(|previous| previous != index) {
                    return None;
                }
                reference = Some(index);
                values.extend(child_values);
            }
            Some((reference?, values))
        }
        Predicate::Leaf {
            index,
            op,
            literals,
            ..
        } if *index < num_keys => match op {
            PredicateOperator::Eq => Some((*index, vec![literals.first()?])),
            PredicateOperator::In => Some((*index, literals.iter().collect())),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Identifier;
    use crate::io::FileIOBuilder;
    use crate::spec::{
        BinaryRowBuilder, IntType, PredicateBuilder, Schema, TableSchema, VarCharType,
    };

    fn fields() -> Vec<DataField> {
        vec![
            DataField::new(0, "a".into(), DataType::Int(IntType::new())),
            DataField::new(1, "b".into(), DataType::Int(IntType::new())),
        ]
    }

    fn buckets(predicate: &Predicate, fields: &[DataField]) -> Option<HashSet<i32>> {
        compute_target_buckets(predicate, fields, BucketFunctionType::Default, 128)
    }

    #[test]
    fn same_key_or_and_repeated_constraints_follow_java() {
        let fields = fields();
        let pb = PredicateBuilder::new(&fields);
        let a = pb.equal("a", Datum::Int(2)).unwrap();
        let b = pb.equal("a", Datum::Int(3)).unwrap();
        let or = Predicate::or(vec![a.clone(), b.clone()]);
        let expected = buckets(&a, &fields[..1])
            .unwrap()
            .union(&buckets(&b, &fields[..1]).unwrap())
            .copied()
            .collect();
        assert_eq!(buckets(&or, &fields[..1]), Some(expected));
        let intersection = Predicate::and(vec![
            pb.is_in("a", vec![Datum::Int(1), Datum::Int(2)]).unwrap(),
            or.clone(),
        ]);
        assert_eq!(
            buckets(&intersection, &fields[..1]),
            buckets(&a, &fields[..1])
        );
        let reversed = Predicate::and(vec![
            or,
            pb.is_in("a", vec![Datum::Int(1), Datum::Int(2)]).unwrap(),
        ]);
        assert_eq!(buckets(&reversed, &fields[..1]), buckets(&a, &fields[..1]));
        assert!(buckets(&Predicate::and(vec![a, b]), &fields[..1]).is_none());
    }

    #[test]
    fn mixed_or_never_contributes_a_partial_key_constraint() {
        let fields = fields();
        let pb = PredicateBuilder::new(&fields);
        let key = pb.equal("a", Datum::Int(1)).unwrap();
        for unrelated in [
            pb.equal("b", Datum::Int(2)).unwrap(),
            pb.greater_than("a", Datum::Int(10)).unwrap(),
        ] {
            let mixed = Predicate::or(vec![key.clone(), unrelated]);
            assert!(buckets(&mixed, &fields[..1]).is_none());
            let safe = Predicate::and(vec![key.clone(), mixed]);
            assert_eq!(buckets(&safe, &fields[..1]), buckets(&key, &fields[..1]));
        }
        let null = pb.is_null("a").unwrap();
        assert!(buckets(&null, &fields[..1]).is_none());
    }

    #[test]
    fn candidate_product_is_bounded_before_hashing() {
        let fields = fields();
        let pb = PredicateBuilder::new(&fields);
        let first = pb.is_in("a", (0..40).map(Datum::Int).collect()).unwrap();
        let at_limit = Predicate::and(vec![
            first.clone(),
            pb.is_in("b", (0..25).map(Datum::Int).collect()).unwrap(),
        ]);
        assert!(buckets(&at_limit, &fields).is_some());
        let above_limit = Predicate::and(vec![
            first,
            pb.is_in("b", (0..26).map(Datum::Int).collect()).unwrap(),
        ]);
        assert!(buckets(&above_limit, &fields).is_none());
        // A large IN can still be narrowed by another conjunct before the cap.
        let narrowed = Predicate::and(vec![
            pb.is_in("a", (0..1001).map(Datum::Int).collect()).unwrap(),
            pb.equal("a", Datum::Int(3)).unwrap(),
        ]);
        assert_eq!(
            buckets(&narrowed, &fields[..1]),
            buckets(&pb.equal("a", Datum::Int(3)).unwrap(), &fields[..1])
        );
    }

    #[test]
    fn large_intersections_are_reduced_before_the_combination_limit() {
        let fields = fields();
        let pb = PredicateBuilder::new(&fields);
        let left = pb
            .is_in("a", (0..50_000).map(Datum::Int).collect())
            .unwrap();
        let right = pb
            .is_in("a", (49_990..100_000).map(Datum::Int).collect())
            .unwrap();
        let expected = buckets(
            &pb.is_in("a", (49_990..50_000).map(Datum::Int).collect())
                .unwrap(),
            &fields[..1],
        );
        assert!(expected.is_some());
        assert_eq!(
            buckets(
                &Predicate::and(vec![left.clone(), right.clone()]),
                &fields[..1]
            ),
            expected
        );
        assert_eq!(
            buckets(&Predicate::and(vec![right, left]), &fields[..1]),
            expected
        );
    }

    #[test]
    fn unordered_literals_in_repeated_constraints_fail_open() {
        let make_leaf = |json: &str| {
            let value = crate::variant::GenericVariant::parse_json(json).unwrap();
            Predicate::Leaf {
                column: "v".into(),
                index: 0,
                data_type: DataType::Variant(crate::spec::VariantType::new()),
                op: PredicateOperator::Eq,
                literals: vec![Datum::Variant {
                    value: value.value().to_vec(),
                    metadata: value.metadata().to_vec(),
                }],
            }
        };
        let predicate = Predicate::and(vec![make_leaf("1"), make_leaf("2")]);
        let mut candidates = vec![None];
        assert!(collect_and_candidates(&predicate, &mut candidates).is_none());
    }

    fn partitioned_table(bucket: &str, function: &str) -> Table {
        let schema = Schema::builder()
            .column("pt", DataType::VarChar(VarCharType::default()))
            .column("a", DataType::Int(IntType::new()))
            .column("b", DataType::Int(IntType::new()))
            .partition_keys(["pt"])
            .primary_key(["pt", "a", "b"])
            .option("bucket", bucket)
            .option("bucket-key", "a,b")
            .option("bucket-function.type", function)
            .build()
            .unwrap();
        Table::new(
            FileIOBuilder::new("memory").build().unwrap(),
            Identifier::new("db", "bucket"),
            "memory:/buckets".into(),
            TableSchema::new(0, &schema),
            None,
        )
    }

    fn partition(value: Option<&str>) -> Vec<u8> {
        let mut row = BinaryRowBuilder::new(1);
        match value {
            Some(value) => row.write_string(0, value),
            None => row.set_null_at(0),
        }
        row.build_serialized()
    }

    #[test]
    fn partition_specialization_and_bucket_count_caches_are_independent() {
        for function in ["default", "hive"] {
            let table = partitioned_table("8", function);
            let pb = PredicateBuilder::new(table.schema().fields());
            let branch = |pt: &str, a: i32| {
                Predicate::and(vec![
                    pb.equal("pt", Datum::String(pt.into())).unwrap(),
                    pb.equal("a", Datum::Int(a)).unwrap(),
                    pb.is_in("b", vec![Datum::Int(1), Datum::Int(2)]).unwrap(),
                ])
            };
            let predicate = Predicate::or(vec![branch("left", 10), branch("right", 20)]);
            let mut selector = BucketSelector::new(&table, &predicate).unwrap().unwrap();
            // Before reading a partition, correlated ANDs inside OR cannot be
            // interpreted as an OR on one bucket key, so early pruning stays open.
            assert!(selector
                .config
                .targets(selector.manifest_predicate(), 8)
                .is_none());
            for total in [8, 17, 8] {
                for (pt, a) in [("left", 10), ("right", 20)] {
                    let expected = (1..=2)
                        .map(|b| {
                            bucket_for_datums(
                                &[
                                    (
                                        Some(&Datum::Int(a)),
                                        selector.bucket_fields()[0].data_type(),
                                    ),
                                    (
                                        Some(&Datum::Int(b)),
                                        selector.bucket_fields()[1].data_type(),
                                    ),
                                ],
                                selector.function(),
                                total,
                            )
                            .unwrap()
                        })
                        .collect::<HashSet<_>>();
                    let actual = (0..total)
                        .filter(|&bucket| selector.test(&partition(Some(pt)), bucket, total))
                        .collect::<HashSet<_>>();
                    assert_eq!(actual, expected);
                }
            }
            // Java leaves an unmatchable partition to the partition filter.
            for bucket in 0..8 {
                assert!(selector.test(&partition(Some("missing")), bucket, 8));
                assert!(selector.test(&partition(None), bucket, 8));
                assert!(selector.test(b"invalid", bucket, 8));
            }
        }
    }

    #[test]
    fn unconstrained_partition_branch_pending_buckets_and_dynamic_modes_fail_open() {
        let table = partitioned_table("8", "default");
        let pb = PredicateBuilder::new(table.schema().fields());
        let predicate = Predicate::or(vec![
            Predicate::and(vec![
                pb.equal("pt", Datum::String("left".into())).unwrap(),
                pb.equal("a", Datum::Int(1)).unwrap(),
                pb.equal("b", Datum::Int(2)).unwrap(),
            ]),
            pb.equal("pt", Datum::String("right".into())).unwrap(),
        ]);
        let mut selector = BucketSelector::new(&table, &predicate).unwrap().unwrap();
        for bucket in 0..8 {
            assert!(selector.test(&partition(Some("right")), bucket, 8));
            assert!(selector.test(&partition(Some("left")), bucket, 0));
        }
        assert!(selector.test(&partition(Some("left")), -1, 8));
        assert!(selector.test(&partition(Some("left")), -2, 8));
        let dynamic = partitioned_table("-1", "default");
        assert!(BucketSelector::new(&dynamic, &predicate).unwrap().is_none());
        let postpone = partitioned_table("-2", "default");
        assert!(BucketSelector::new(&postpone, &predicate)
            .unwrap()
            .is_some());
    }
}
