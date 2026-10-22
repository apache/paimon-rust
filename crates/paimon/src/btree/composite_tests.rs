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

use super::test_util::{BytesFileRead, VecFileWrite};
use super::*;
use crate::spec::{
    BinaryRow, DataField, DataType, Datum, IntType, Predicate, PredicateBuilder, RowType,
    VarCharType,
};
use bytes::Bytes;
use std::cmp::Ordering;

fn fields() -> Vec<DataField> {
    vec![
        DataField::new(
            10,
            "category".into(),
            DataType::VarChar(VarCharType::string_type()),
        ),
        DataField::new(20, "item".into(), DataType::Int(IntType::new())),
        DataField::new(
            30,
            "tag".into(),
            DataType::VarChar(VarCharType::string_type()),
        ),
    ]
}
fn tuples() -> Vec<Vec<Option<Datum>>> {
    let mut rows = Vec::new();
    for category in [None, Some("a"), Some("b")] {
        for item in [None, Some(-2), Some(0), Some(1), Some(2)] {
            for tag in [None, Some(""), Some("z")] {
                rows.push(vec![
                    category.map(|v| Datum::String(v.into())),
                    item.map(Datum::Int),
                    tag.map(|v| Datum::String(v.into())),
                ]);
            }
        }
    }
    rows
}

#[test]
fn composite_compacted_encoding_and_comparison() {
    let fields = fields();
    let codec = CompositeKeyCodec::new(&fields);
    assert_eq!(codec.serialize(&[None, None, None]).unwrap(), vec![0, 7]);
    assert_eq!(
        codec
            .serialize(&[Some(Datum::String("a".into())), Some(Datum::Int(-2)), None])
            .unwrap(),
        vec![0, 4, 1, b'a', 254, 255, 255, 255]
    );
    let encoded = tuples()
        .iter()
        .map(|tuple| codec.serialize(tuple).unwrap())
        .collect::<Vec<_>>();
    for pair in encoded.windows(2) {
        assert_eq!(
            codec.compare_keys(&pair[0], &pair[1]).unwrap(),
            Ordering::Less
        );
    }
    for malformed in [
        vec![],
        vec![0],
        vec![0, 0, 128],
        vec![0, 0, 127],
        vec![0, 7, 0],
    ] {
        assert!(codec.validate_key(&malformed).is_err());
    }
}

#[test]
fn composite_query_declines_decimal_overflow_and_timestamp_precision_loss() {
    use crate::spec::{DecimalType, LocalZonedTimestampType, TimestampType};

    let fields = vec![
        DataField::new(
            0,
            "d".into(),
            DataType::Decimal(DecimalType::new(18, 2).unwrap()),
        ),
        DataField::new(1, "item".into(), DataType::Int(IntType::new())),
    ];
    let b = PredicateBuilder::new(&fields);
    for (unscaled, scale) in [
        (i128::MAX, 0),                // Exact rescaling would overflow i128.
        (i128::from(i64::MAX) + 1, 2), // Compact encoding would wrap the integer.
        (1, 3),                        // Dropping the final digit would change the bound.
    ] {
        let predicate = b
            .less_than(
                "d",
                Datum::Decimal {
                    unscaled,
                    precision: 38,
                    scale,
                },
            )
            .unwrap();
        assert!(CompositePlan::plan(&fields, &predicate).unwrap().is_none());
    }
    // A zero with an arbitrary literal scale is exactly representable.
    let predicate = b
        .equal(
            "d",
            Datum::Decimal {
                unscaled: 0,
                precision: 38,
                scale: 38,
            },
        )
        .unwrap();
    let plan = CompositePlan::plan(&fields, &predicate).unwrap().unwrap();
    let codec = CompositeKeyCodec::new(&fields);
    let zero = codec
        .serialize(&[
            Some(Datum::Decimal {
                unscaled: 0,
                precision: 18,
                scale: 2,
            }),
            Some(Datum::Int(1)),
        ])
        .unwrap();
    assert!(plan.may_match(Some(&zero), Some(&zero)).unwrap());
    for (ty, literal) in [
        (
            DataType::Timestamp(TimestampType::new(3).unwrap()),
            Datum::Timestamp {
                millis: 1000,
                nanos: 1,
            },
        ),
        (
            DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(3).unwrap()),
            Datum::LocalZonedTimestamp {
                millis: 1000,
                nanos: 1,
            },
        ),
    ] {
        let fields = vec![
            DataField::new(0, "ts".into(), ty),
            DataField::new(1, "item".into(), DataType::Int(IntType::new())),
        ];
        let predicate = PredicateBuilder::new(&fields)
            .less_than("ts", literal)
            .unwrap();
        assert!(CompositePlan::plan(&fields, &predicate).unwrap().is_none());
    }
}

#[test]
fn composite_slice_ordering_handles_offsets_and_typed_fields() {
    use crate::spec::VarBinaryType;

    let fields = vec![
        DataField::new(0, "number".into(), DataType::Int(IntType::new())),
        DataField::new(
            1,
            "text".into(),
            DataType::VarChar(VarCharType::string_type()),
        ),
        DataField::new(
            2,
            "bytes".into(),
            DataType::VarBinary(VarBinaryType::new(130).unwrap()),
        ),
    ];
    // Tuple order differs from raw byte order for little-endian integers,
    // length-prefixed strings, and the NULL header. Binary bytes are unsigned.
    let ordered = [
        (None, None, None),
        (Some(1), None, Some(vec![0xff])),
        (Some(1), Some(String::new()), None),
        (Some(1), Some("a".repeat(128)), Some(vec![0xff])),
        (Some(1), Some("b".into()), None),
        (Some(1), Some("b".into()), Some(vec![1, 2, 3])),
        (Some(1), Some("b".into()), Some(vec![1, 2, 4])),
        (Some(1), Some("b".into()), Some(vec![0xff])),
        (Some(256), None, None),
    ];
    let mut storage = vec![0xee; 17];
    let mut ranges = Vec::new();
    for (number, text, bytes) in ordered {
        let key = serialize_composite_key(
            &[
                number.map(Datum::Int),
                text.map(Datum::String),
                bytes.map(Datum::Bytes),
            ],
            &fields,
        )
        .unwrap();
        let start = storage.len();
        storage.extend(key);
        ranges.push(start..storage.len());
        storage.extend([0xff; 3]);
    }
    let compare = make_key_comparator(&DataType::Row(RowType::new(fields)));
    for (i, left) in ranges.iter().enumerate() {
        for (j, right) in ranges.iter().enumerate() {
            assert_eq!(
                compare(&storage[left.clone()], &storage[right.clone()]).unwrap(),
                i.cmp(&j),
                "tuple {i} versus {j}"
            );
        }
    }
}

#[test]
fn composite_slice_comparison_rejects_invalid_suffixes() {
    let fields = fields();
    let codec = CompositeKeyCodec::new(&fields);
    let key = |category: &str| {
        codec
            .serialize(&[
                Some(Datum::String(category.into())),
                Some(Datum::Int(1)),
                Some(Datum::String("z".into())),
            ])
            .unwrap()
    };
    let valid = key("a");
    let mut truncated = key("b");
    truncated.pop();
    let mut trailing = key("b");
    trailing.push(0);
    let predicate = PredicateBuilder::new(&fields)
        .equal("category", Datum::String("a".into()))
        .unwrap();
    let plan = CompositePlan::plan(&fields, &predicate).unwrap().unwrap();
    for malformed in [truncated, trailing] {
        // An earlier column decides the order, but the incompatible suffix must
        // still decline the index rather than allowing metadata to prune rows.
        assert!(codec.compare_keys(&valid, &malformed).is_err());
        assert!(codec.compare_keys(&malformed, &valid).is_err());
        assert!(plan.intervals[0]
            .lower
            .compare_key(&malformed, &plan.codec)
            .is_err());
        assert!(plan.intervals[0]
            .upper
            .compare_key(&malformed, &plan.codec)
            .is_err());
    }
}

#[test]
fn composite_nan_keys_are_canonical_and_null_header_spans_bytes() {
    use crate::spec::{DoubleType, FloatType};
    let fields = vec![
        DataField::new(0, "f".into(), DataType::Float(FloatType::new())),
        DataField::new(1, "d".into(), DataType::Double(DoubleType::new())),
    ];
    let codec = CompositeKeyCodec::new(&fields);
    let key = codec
        .serialize(&[Some(Datum::Float(f32::NAN)), Some(Datum::Double(f64::NAN))])
        .unwrap();
    let alternate = codec
        .serialize(&[
            Some(Datum::Float(f32::from_bits(0xffc00001))),
            Some(Datum::Double(f64::from_bits(0xfff8000000000001))),
        ])
        .unwrap();
    assert_eq!(key, alternate);
    assert_eq!(
        codec.compare_keys(&key, &alternate).unwrap(),
        Ordering::Equal
    );
    let inf = codec
        .serialize(&[
            Some(Datum::Float(f32::INFINITY)),
            Some(Datum::Double(f64::INFINITY)),
        ])
        .unwrap();
    assert_eq!(codec.compare_keys(&inf, &key).unwrap(), Ordering::Less);
    let fields = (0..9)
        .map(|i| DataField::new(i, format!("k{i}"), DataType::Int(IntType::new())))
        .collect::<Vec<_>>();
    assert_eq!(
        serialize_composite_key(&[const { None }; 9], &fields).unwrap(),
        vec![0, 255, 1]
    );
}

#[test]
fn composite_interval_expansion_and_residual_columns() {
    let fields = fields();
    let b = PredicateBuilder::new(&fields);
    let duplicate = b
        .is_in("category", vec![Datum::String("a".into()); 1000])
        .unwrap();
    assert_eq!(
        CompositePlan::plan(&fields, &duplicate)
            .unwrap()
            .unwrap()
            .intervals
            .len(),
        1
    );
    let many = b.is_in("item", (0..257).map(Datum::Int).collect()).unwrap();
    let predicate = Predicate::and(vec![
        b.equal("category", Datum::String("a".into())).unwrap(),
        many,
    ]);
    let plan = CompositePlan::plan(&fields, &predicate).unwrap().unwrap();
    assert_eq!(plan.intervals.len(), 257);
    assert_eq!(plan.equal_columns, 2);
    let predicate = Predicate::and(vec![
        b.greater_than("category", Datum::String("a".into()))
            .unwrap(),
        b.equal("item", Datum::Int(1)).unwrap(),
    ]);
    let plan = CompositePlan::plan(&fields, &predicate).unwrap().unwrap();
    assert_eq!(plan.bound_columns, 1);
    assert_eq!(plan.equal_columns, 0);
    assert!(
        CompositePlan::plan(&fields, &b.equal("item", Datum::Int(1)).unwrap())
            .unwrap()
            .is_none()
    );
    let contradiction = Predicate::and(vec![
        b.equal("category", Datum::String("a".into())).unwrap(),
        b.equal("category", Datum::String("b".into())).unwrap(),
    ]);
    assert!(CompositePlan::plan(&fields, &contradiction)
        .unwrap()
        .unwrap()
        .intervals
        .is_empty());
}

fn integer_fields() -> Vec<DataField> {
    (0..3)
        .map(|i| DataField::new(i, format!("k{i}"), DataType::Int(IntType::new())))
        .collect()
}

async fn reader_for_tuples(
    fields: &[DataField],
    tuples: &[Vec<Option<Datum>>],
) -> BTreeIndexReader<super::key_serde::KeyComparator> {
    let codec = CompositeKeyCodec::new(fields);
    let mut entries = tuples
        .iter()
        .enumerate()
        .map(|(id, tuple)| (codec.serialize(tuple).unwrap(), id as i64))
        .collect::<Vec<_>>();
    entries.sort_by(|(left, _), (right, _)| codec.compare_keys(left, right).unwrap());
    let key_type = DataType::Row(RowType::new(fields.to_vec()));
    let buf = VecFileWrite::new();
    let mut writer = BTreeIndexWriter::with_comparator_and_options(
        Box::new(buf.clone()),
        256,
        BlockCompressionType::None,
        1,
        false,
        make_key_comparator(&key_type),
    );
    for (key, id) in entries {
        writer.write(Some(&key), id).await.unwrap();
    }
    let result = writer.finish().await.unwrap();
    let bytes = Bytes::from(buf.to_vec());
    let size = bytes.len() as u64;
    BTreeIndexReader::open(
        Box::new(BytesFileRead(bytes)),
        size,
        &result.meta,
        make_key_comparator(&key_type),
    )
    .await
    .unwrap()
}

fn row_matches(fields: &[DataField], tuple: &[Option<Datum>], predicate: &Predicate) -> bool {
    let row = BinaryRow::from_datums(
        &tuple
            .iter()
            .zip(fields)
            .map(|(value, field)| (value.as_ref(), field.data_type()))
            .collect::<Vec<_>>(),
    );
    crate::spec::eval_row(predicate, &row).unwrap()
}

async fn assert_prefix_candidates_and_residual(
    fields: &[DataField],
    tuples: &[Vec<Option<Datum>>],
    predicate: &Predicate,
    prefix: &Predicate,
    expected_prefixes: usize,
) {
    let plan = CompositePlan::plan(fields, predicate).unwrap().unwrap();
    assert_eq!(plan.bound_columns, 1);
    assert_eq!(plan.equal_columns, 1);
    assert_eq!(plan.intervals.len(), expected_prefixes);
    assert!(!plan.is_point_lookup());
    assert!(plan
        .intervals
        .iter()
        .all(|interval| interval.point_key.is_none()));
    let reader = reader_for_tuples(fields, tuples).await;
    let candidates = reader.query_composite(&plan).await.unwrap();
    let expected = tuples
        .iter()
        .enumerate()
        .filter(|(_, tuple)| row_matches(fields, tuple, predicate))
        .map(|(id, _)| id as u64)
        .collect::<roaring::RoaringTreemap>();
    let expected_candidates = tuples
        .iter()
        .enumerate()
        .filter(|(_, tuple)| row_matches(fields, tuple, prefix))
        .map(|(id, _)| id as u64)
        .collect::<roaring::RoaringTreemap>();
    assert!(!expected.is_empty());
    assert!(expected.is_subset(&candidates));
    assert!(candidates.len() > expected.len());
    assert_eq!(candidates, expected_candidates);
    let actual = candidates
        .iter()
        .filter(|id| row_matches(fields, &tuples[*id as usize], predicate))
        .collect::<roaring::RoaringTreemap>();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn composite_large_in_keeps_every_point_and_null_suffix() {
    let fields = integer_fields();
    let b = PredicateBuilder::new(&fields);
    // An odd multiplier permutes the complete domain; duplicates must not
    // inflate expansion and the final values must not be silently truncated.
    let mut literals = (0..4096)
        .map(|i| Datum::Int((i * 1021) % 4096))
        .collect::<Vec<_>>();
    literals.extend((0..1000).map(Datum::Int));
    let predicate = Predicate::and(vec![
        b.is_in("k0", literals).unwrap(),
        b.is_null("k1").unwrap(),
        b.equal("k2", Datum::Int(7)).unwrap(),
    ]);
    let plan = CompositePlan::plan(&fields, &predicate).unwrap().unwrap();
    assert_eq!(plan.intervals.len(), 4096);
    assert_eq!(plan.bound_columns, 3);
    assert_eq!(plan.equal_columns, 3);
    assert!(plan.is_point_lookup());
    assert!(plan
        .intervals
        .iter()
        .all(|interval| interval.point_key.is_some()));
    let mut tuples = (0..4096)
        .map(|i| vec![Some(Datum::Int(i)), None, Some(Datum::Int(7))])
        .collect::<Vec<_>>();
    tuples.extend([
        vec![
            Some(Datum::Int(4095)),
            Some(Datum::Int(0)),
            Some(Datum::Int(7)),
        ],
        vec![Some(Datum::Int(4095)), None, Some(Datum::Int(8))],
        vec![Some(Datum::Int(5000)), None, Some(Datum::Int(7))],
    ]);
    let reader = reader_for_tuples(&fields, &tuples).await;
    let actual = reader.query_composite(&plan).await.unwrap();
    assert_eq!(actual, (0..4096).collect::<roaring::RoaringTreemap>());
    assert!(actual.contains(4095));
}

#[tokio::test]
async fn composite_cartesian_limit_keeps_left_prefix_and_residual() {
    let fields = integer_fields();
    let b = PredicateBuilder::new(&fields);
    let prefix = b
        .is_in("k0", (0..17).rev().map(Datum::Int).collect())
        .unwrap();
    let predicate = Predicate::and(vec![
        prefix.clone(),
        b.is_in("k1", (0..17).map(Datum::Int).collect()).unwrap(),
        b.equal("k2", Datum::Int(5)).unwrap(),
    ]);
    let mut tuples = Vec::new();
    for first in -1..19 {
        for second in -1..19 {
            for suffix in [4, 5] {
                tuples.push(vec![
                    Some(Datum::Int(first)),
                    Some(Datum::Int(second)),
                    Some(Datum::Int(suffix)),
                ]);
            }
        }
    }
    assert_prefix_candidates_and_residual(&fields, &tuples, &predicate, &prefix, 17).await;
}

#[tokio::test]
async fn composite_oversized_prefix_is_preserved_before_next_in() {
    let fields = integer_fields();
    let b = PredicateBuilder::new(&fields);
    let prefix = b
        .is_in("k0", (0..513).rev().map(Datum::Int).collect())
        .unwrap();
    let predicate = Predicate::and(vec![
        prefix.clone(),
        b.is_in("k1", vec![Datum::Int(0), Datum::Int(1)]).unwrap(),
        b.equal("k2", Datum::Int(5)).unwrap(),
    ]);
    let mut tuples = Vec::new();
    for first in 0..514 {
        for second in [0, 1, 2] {
            for suffix in [4, 5] {
                tuples.push(vec![
                    Some(Datum::Int(first)),
                    Some(Datum::Int(second)),
                    Some(Datum::Int(suffix)),
                ]);
            }
        }
    }
    assert_prefix_candidates_and_residual(&fields, &tuples, &predicate, &prefix, 513).await;
}

#[tokio::test]
async fn composite_large_in_intersects_other_constraints() {
    let fields = integer_fields();
    let b = PredicateBuilder::new(&fields);
    let first = b
        .is_in("k0", (0..4096).rev().map(Datum::Int).collect())
        .unwrap();
    let mut tuples = (0..8192)
        .map(|i| {
            vec![
                Some(Datum::Int(i)),
                Some(Datum::Int(1)),
                Some(Datum::Int(1)),
            ]
        })
        .collect::<Vec<_>>();
    tuples.push(vec![None, Some(Datum::Int(1)), Some(Datum::Int(1))]);
    let reader = reader_for_tuples(&fields, &tuples).await;
    for (constraint, expected) in [
        (
            Predicate::and(vec![
                b.greater_or_equal("k0", Datum::Int(4089)).unwrap(),
                b.less_than("k0", Datum::Int(4096)).unwrap(),
            ]),
            4089..4096,
        ),
        (
            b.is_in("k0", (2048..6144).map(Datum::Int).collect())
                .unwrap(),
            2048..4096,
        ),
    ] {
        let predicate = Predicate::and(vec![
            first.clone(),
            constraint,
            b.equal("k1", Datum::Int(1)).unwrap(),
            b.equal("k2", Datum::Int(1)).unwrap(),
        ]);
        let plan = CompositePlan::plan(&fields, &predicate).unwrap().unwrap();
        assert!(plan.is_point_lookup());
        assert_eq!(
            plan.intervals.len(),
            (expected.end - expected.start) as usize
        );
        let actual = reader.query_composite(&plan).await.unwrap();
        assert_eq!(actual, expected.collect::<roaring::RoaringTreemap>());
    }
}

#[tokio::test]
async fn composite_point_domain_identity_matches_encoded_keys() {
    use crate::spec::{DecimalType, DoubleType, FloatType};

    let fields = vec![
        DataField::new(0, "f".into(), DataType::Float(FloatType::new())),
        DataField::new(1, "d".into(), DataType::Double(DoubleType::new())),
        DataField::new(
            2,
            "n".into(),
            DataType::Decimal(DecimalType::new(18, 2).unwrap()),
        ),
    ];
    let b = PredicateBuilder::new(&fields);
    let decimal = |unscaled, scale| Datum::Decimal {
        unscaled,
        precision: 18,
        scale,
    };
    let predicate = Predicate::and(vec![
        b.is_in(
            "f",
            vec![
                Datum::Float(f32::NAN),
                Datum::Float(f32::from_bits(0xffc00001)),
                Datum::Float(-0.0),
                Datum::Float(0.0),
            ],
        )
        .unwrap(),
        b.is_in(
            "d",
            vec![
                Datum::Double(f64::NAN),
                Datum::Double(f64::from_bits(0xfff8000000000001)),
                Datum::Double(-0.0),
                Datum::Double(0.0),
            ],
        )
        .unwrap(),
        b.is_in("n", vec![decimal(1, 0), decimal(100, 2), decimal(1000, 3)])
            .unwrap(),
    ]);
    let plan = CompositePlan::plan(&fields, &predicate).unwrap().unwrap();
    assert!(plan.is_point_lookup());
    // Each floating domain has one canonical NaN and two distinct zeros;
    // all three decimal literals normalize to the same indexed value.
    assert_eq!(plan.intervals.len(), 9);
    let mut tuples = Vec::new();
    for float in [f32::NAN, -0.0, 0.0] {
        for double in [f64::NAN, -0.0, 0.0] {
            tuples.push(vec![
                Some(Datum::Float(float)),
                Some(Datum::Double(double)),
                Some(decimal(100, 2)),
            ]);
        }
    }
    let reader = reader_for_tuples(&fields, &tuples).await;
    assert_eq!(
        reader.query_composite(&plan).await.unwrap(),
        (0..9).collect::<roaring::RoaringTreemap>()
    );
    let narrowed = Predicate::and(vec![
        predicate,
        b.is_in(
            "f",
            vec![Datum::Float(0.0), Datum::Float(f32::from_bits(0xffc00001))],
        )
        .unwrap(),
        b.is_in("n", vec![decimal(1000, 3)]).unwrap(),
    ]);
    let plan = CompositePlan::plan(&fields, &narrowed).unwrap().unwrap();
    assert_eq!(
        reader.query_composite(&plan).await.unwrap(),
        [0, 1, 2, 6, 7, 8]
            .into_iter()
            .collect::<roaring::RoaringTreemap>()
    );
}

#[tokio::test]
async fn composite_point_lookup_batches_reads_without_block_cache() {
    use crate::io::FileRead;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;

    struct CountedRead {
        bytes: Bytes,
        reads: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl FileRead for CountedRead {
        async fn read(&self, range: std::ops::Range<u64>) -> crate::Result<Bytes> {
            self.reads.fetch_add(1, AtomicOrdering::Relaxed);
            Ok(self.bytes.slice(range.start as usize..range.end as usize))
        }
    }

    let fields = integer_fields();
    let b = PredicateBuilder::new(&fields);
    let predicate = Predicate::and(vec![
        b.is_in("k0", (0..128).rev().map(Datum::Int).collect())
            .unwrap(),
        b.is_null("k1").unwrap(),
        b.equal("k2", Datum::Int(7)).unwrap(),
    ]);
    let plan = CompositePlan::plan(&fields, &predicate).unwrap().unwrap();
    let codec = CompositeKeyCodec::new(&fields);
    let key_type = DataType::Row(RowType::new(fields.clone()));
    let buf = VecFileWrite::new();
    let mut writer = BTreeIndexWriter::with_comparator_and_options(
        Box::new(buf.clone()),
        1_000_000,
        BlockCompressionType::None,
        1,
        false,
        make_key_comparator(&key_type),
    );
    for id in 0..128 {
        let key = codec
            .serialize(&[Some(Datum::Int(id)), None, Some(Datum::Int(7))])
            .unwrap();
        writer.write(Some(&key), id as i64).await.unwrap();
    }
    let result = writer.finish().await.unwrap();
    let bytes = Bytes::from(buf.to_vec());
    let size = bytes.len() as u64;
    let reads = Arc::new(AtomicUsize::new(0));
    let reader = BTreeIndexReader::open_with_data_block_cache(
        Box::new(CountedRead {
            bytes,
            reads: Arc::clone(&reads),
        }),
        size,
        &result.meta,
        make_key_comparator(&key_type),
        Arc::new(BTreeDataBlockCache::new(0)),
        Arc::from("memory:/composite-batch.btree"),
    )
    .await
    .unwrap();
    reads.store(0, AtomicOrdering::Relaxed);
    assert_eq!(
        reader.query_composite(&plan).await.unwrap(),
        (0..128).collect::<roaring::RoaringTreemap>()
    );
    assert_eq!(reads.load(AtomicOrdering::Relaxed), 1);
}

fn predicates(fields: &[DataField]) -> Vec<Predicate> {
    let b = PredicateBuilder::new(fields);
    let mut predicates = vec![
        b.equal("category", Datum::String("a".into())).unwrap(),
        b.is_null("category").unwrap(),
        b.is_not_null("category").unwrap(),
    ];
    for prefix in [
        b.equal("category", Datum::String("a".into())).unwrap(),
        b.is_null("category").unwrap(),
        b.is_in(
            "category",
            vec![
                Datum::String("a".into()),
                Datum::String("b".into()),
                Datum::String("a".into()),
            ],
        )
        .unwrap(),
    ] {
        for range in [
            b.greater_than("item", Datum::Int(0)).unwrap(),
            b.greater_or_equal("item", Datum::Int(0)).unwrap(),
            b.less_than("item", Datum::Int(1)).unwrap(),
            b.less_or_equal("item", Datum::Int(1)).unwrap(),
            b.between("item", Datum::Int(-2), Datum::Int(1)).unwrap(),
            b.is_null("item").unwrap(),
            b.is_not_null("item").unwrap(),
            b.is_in("item", vec![Datum::Int(-2), Datum::Int(2)])
                .unwrap(),
            Predicate::and(vec![
                b.greater_than("item", Datum::Int(1)).unwrap(),
                b.less_than("item", Datum::Int(1)).unwrap(),
            ]),
        ] {
            predicates.push(Predicate::and(vec![prefix.clone(), range]));
        }
    }
    predicates.push(Predicate::and(vec![
        b.is_null("category").unwrap(),
        b.is_null("item").unwrap(),
        b.is_null("tag").unwrap(),
    ]));
    predicates.push(Predicate::and(vec![
        b.equal("tag", Datum::String("z".into())).unwrap(),
        b.equal("item", Datum::Int(1)).unwrap(),
        b.equal("category", Datum::String("a".into())).unwrap(),
    ]));
    predicates
}

/// Exercise range endpoints at every key position, including NULL prefixes,
/// nested disjunctions, duplicate IN values, and contradictory bounds. The
/// row predicate evaluator below is independent of the tuple interval planner.
fn predicate_matrix(fields: &[DataField]) -> Vec<Predicate> {
    let b = PredicateBuilder::new(fields);
    let prefixes = vec![
        b.is_null("category").unwrap(),
        b.equal("category", Datum::String("a".into())).unwrap(),
        b.equal("category", Datum::String("b".into())).unwrap(),
        b.is_in(
            "category",
            vec![
                Datum::String("b".into()),
                Datum::String("a".into()),
                Datum::String("b".into()),
            ],
        )
        .unwrap(),
        Predicate::Or(vec![
            b.equal("category", Datum::String("a".into())).unwrap(),
            Predicate::Or(vec![
                b.equal("category", Datum::String("b".into())).unwrap(),
                b.equal("category", Datum::String("a".into())).unwrap(),
            ]),
        ]),
    ];
    let mut items = vec![b.is_null("item").unwrap(), b.is_not_null("item").unwrap()];
    for value in [-3, -2, -1, 0, 1, 2, 3, 256] {
        items.extend([
            b.equal("item", Datum::Int(value)).unwrap(),
            b.greater_than("item", Datum::Int(value)).unwrap(),
            b.greater_or_equal("item", Datum::Int(value)).unwrap(),
            b.less_than("item", Datum::Int(value)).unwrap(),
            b.less_or_equal("item", Datum::Int(value)).unwrap(),
        ]);
    }
    items.push(
        b.is_in("item", vec![Datum::Int(2), Datum::Int(-2), Datum::Int(2)])
            .unwrap(),
    );
    for lower in [-3, -2, 0, 1, 2, 3] {
        for upper in [-3, -2, 0, 1, 2, 3] {
            // Use raw AND so contradictions reach the planner rather than a
            // builder short-circuit to AlwaysFalse.
            items.push(Predicate::And(vec![
                b.greater_or_equal("item", Datum::Int(lower)).unwrap(),
                b.less_than("item", Datum::Int(upper)).unwrap(),
            ]));
        }
    }
    let mut result = Vec::new();
    for prefix in prefixes {
        for item in &items {
            result.push(Predicate::And(vec![prefix.clone(), item.clone()]));
        }
        for item in [
            b.is_null("item").unwrap(),
            b.equal("item", Datum::Int(0)).unwrap(),
            b.is_in("item", vec![Datum::Int(2), Datum::Int(-2)])
                .unwrap(),
        ] {
            for value in ["", "a", "z", "zz", "联合"] {
                for tag in [
                    b.equal("tag", Datum::String(value.into())).unwrap(),
                    b.greater_than("tag", Datum::String(value.into())).unwrap(),
                    b.less_or_equal("tag", Datum::String(value.into())).unwrap(),
                ] {
                    result.push(Predicate::And(vec![prefix.clone(), item.clone(), tag]));
                }
            }
            for tag in [b.is_null("tag").unwrap(), b.is_not_null("tag").unwrap()] {
                result.push(Predicate::And(vec![prefix.clone(), item.clone(), tag]));
            }
        }
    }
    result
}

#[tokio::test]
async fn composite_java_files_and_rust_writer_match() {
    // Larger LZ4 blocks must actually compress, rather than exercising only
    // the uncompressed fallback of a writer configured for LZ4.
    assert!(
        include_bytes!("../../testdata/btree/btree_composite_v1_java_lz4.bin").len()
            < include_bytes!("../../testdata/btree/btree_composite_v1_java_none.bin").len()
    );
    assert!(
        include_bytes!("../../testdata/btree/btree_composite_v2_java_lz4.bin").len()
            < include_bytes!("../../testdata/btree/btree_composite_v2_java_none.bin").len()
    );
    let fields = fields();
    let codec = CompositeKeyCodec::new(&fields);
    let key_type = DataType::Row(RowType::new(fields.clone()));
    for (version, compression, fixture, metadata) in [
        (
            1,
            BlockCompressionType::None,
            include_bytes!("../../testdata/btree/btree_composite_v1_java_none.bin").as_slice(),
            include_bytes!("../../testdata/btree/btree_composite_v1_java_none.meta").as_slice(),
        ),
        (
            1,
            BlockCompressionType::Lz4,
            include_bytes!("../../testdata/btree/btree_composite_v1_java_lz4.bin").as_slice(),
            include_bytes!("../../testdata/btree/btree_composite_v1_java_lz4.meta").as_slice(),
        ),
        (
            2,
            BlockCompressionType::None,
            include_bytes!("../../testdata/btree/btree_composite_v2_java_none.bin").as_slice(),
            include_bytes!("../../testdata/btree/btree_composite_v2_java_none.meta").as_slice(),
        ),
        (
            2,
            BlockCompressionType::Lz4,
            include_bytes!("../../testdata/btree/btree_composite_v2_java_lz4.bin").as_slice(),
            include_bytes!("../../testdata/btree/btree_composite_v2_java_lz4.meta").as_slice(),
        ),
    ] {
        let meta = BTreeIndexMeta::deserialize(metadata).unwrap();
        let reader = BTreeIndexReader::open(
            Box::new(BytesFileRead(Bytes::copy_from_slice(fixture))),
            fixture.len() as u64,
            &meta,
            make_key_comparator(&key_type),
        )
        .await
        .unwrap();
        let tuples = tuples();
        for predicate in predicates(&fields)
            .into_iter()
            .chain(predicate_matrix(&fields))
        {
            let plan = CompositePlan::plan(&fields, &predicate).unwrap().unwrap();
            let mut expected = roaring::RoaringTreemap::new();
            for (id, tuple) in tuples.iter().enumerate() {
                let row = BinaryRow::from_datums(
                    &tuple
                        .iter()
                        .zip(&fields)
                        .map(|(v, field)| (v.as_ref(), field.data_type()))
                        .collect::<Vec<_>>(),
                );
                if crate::spec::eval_row(&predicate, &row).unwrap() {
                    expected.insert(id as u64);
                    expected.insert(id as u64 + 1000);
                }
            }
            assert_eq!(
                reader.query_composite(&plan).await.unwrap(),
                expected,
                "{predicate:?}, version={version}"
            );
        }
        assert!(reader.null_bitmap().await.unwrap().is_empty());
        let buf = VecFileWrite::new();
        let mut writer = BTreeIndexWriter::with_comparator_and_options(
            Box::new(buf.clone()),
            if compression == BlockCompressionType::None {
                64
            } else {
                512
            },
            compression,
            1,
            false,
            make_key_comparator(&key_type),
        )
        .with_file_version(version)
        .unwrap();
        for (id, tuple) in tuples.iter().enumerate() {
            let key = codec.serialize(tuple).unwrap();
            writer.write(Some(&key), id as i64).await.unwrap();
            writer.write(Some(&key), id as i64 + 1000).await.unwrap();
        }
        let result = writer.finish().await.unwrap();
        assert_eq!(result.meta.serialize(), metadata);
        if compression == BlockCompressionType::None {
            assert_eq!(buf.to_vec(), fixture);
        }
    }
}

#[test]
fn composite_java_scalar_types_match_compacted_encoding() {
    use crate::spec::{
        BigIntType, BooleanType, DecimalType, DoubleType, FloatType, SmallIntType, TimestampType,
        TinyIntType,
    };
    let types = vec![
        DataType::Boolean(BooleanType::new()),
        DataType::TinyInt(TinyIntType::new()),
        DataType::SmallInt(SmallIntType::new()),
        DataType::Int(IntType::new()),
        DataType::BigInt(BigIntType::new()),
        DataType::Float(FloatType::new()),
        DataType::Double(DoubleType::new()),
        DataType::Decimal(DecimalType::new(20, 2).unwrap()),
        DataType::Timestamp(TimestampType::new(6).unwrap()),
        DataType::VarChar(VarCharType::string_type()),
        DataType::Int(IntType::new()),
    ];
    let fields = types
        .into_iter()
        .enumerate()
        .map(|(i, ty)| DataField::new(i as i32, format!("k{i}"), ty))
        .collect::<Vec<_>>();
    let codec = CompositeKeyCodec::new(&fields);
    let values = vec![
        Some(Datum::Bool(true)),
        Some(Datum::TinyInt(-3)),
        Some(Datum::SmallInt(-257)),
        Some(Datum::Int(-12345)),
        Some(Datum::Long(1234567890123)),
        Some(Datum::Float(f32::from_bits(0xffc00001))),
        Some(Datum::Double(f64::from_bits(0xfff8000000000001))),
        Some(Datum::Decimal {
            unscaled: -129,
            precision: 20,
            scale: 2,
        }),
        Some(Datum::Timestamp {
            millis: -123000,
            nanos: 999999,
        }),
        Some(Datum::String("联合".into())),
        None,
    ];
    let key = codec.serialize(&values).unwrap();
    assert_eq!(
        key,
        include_bytes!("../../testdata/btree/btree_composite_java_scalar.key")
    );
    let mut larger = values.clone();
    larger[7] = Some(Datum::Decimal {
        unscaled: 128,
        precision: 20,
        scale: 2,
    });
    assert_eq!(
        codec
            .compare_keys(&key, &codec.serialize(&larger).unwrap())
            .unwrap(),
        Ordering::Less
    );
    larger = values.clone();
    larger[8] = Some(Datum::Timestamp {
        millis: -123000,
        nanos: 0,
    });
    assert_eq!(
        codec
            .compare_keys(&key, &codec.serialize(&larger).unwrap())
            .unwrap(),
        Ordering::Greater
    );
    let mut wrong_type = values;
    wrong_type[3] = Some(Datum::Float(0.0));
    assert!(
        codec.serialize(&wrong_type).is_err(),
        "same-width mismatched types must be rejected"
    );
}
