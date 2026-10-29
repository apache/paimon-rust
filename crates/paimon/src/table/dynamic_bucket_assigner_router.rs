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

//! Stateless input routing before distributed dynamic bucket assignment.

use super::bucket_router::project_routing_input;
use super::postpone_bucket_plan::data_invalid;
use crate::spec::{
    batch_build_binary_rows, batch_hash_codes, BinaryRow, CoreOptions, DataField, TableSchema,
};
use crate::Result;
use arrow_array::{Array, Int32Array, RecordBatch};
use arrow_schema::SchemaRef;
use std::collections::HashSet;

/// Assigner IDs and Java BinaryRow key hashes in the original input order.
/// The assigner subsequently uses its persisted HASH index to choose buckets.
#[derive(Debug, Clone)]
pub struct DynamicBucketAssignerRouting {
    pub assigners: Int32Array,
    pub key_hashes: Int32Array,
}

/// Hash only partition and primary-key columns without table access or I/O.
///
/// Java's `BucketAssigner.computeAssigner` combines the partition hash with
/// the trimmed primary-key hash. Neither hash is a bucket ID: one assigner
/// must own the persisted HASH-index state for each routed input.
#[derive(Debug, Clone)]
pub struct DynamicBucketAssignerRouter {
    fields: Vec<DataField>,
    arrow_schema: SchemaRef,
    partition_indices: Vec<usize>,
    key_indices: Vec<usize>,
    partition_fields: Vec<DataField>,
    partition_schema: SchemaRef,
    num_channels: i32,
    num_assigners: i32,
}

impl DynamicBucketAssignerRouter {
    pub fn new(schema: &TableSchema, num_channels: i32, num_assigners: i32) -> Result<Self> {
        if num_channels <= 0 || num_assigners <= 0 {
            return Err(data_invalid(
                "num_channels and num_assigners must be positive",
            ));
        }
        schema.validate_resolved_structure()?;
        let options = CoreOptions::new(schema.options());
        if options.is_format_table()
            || options.bucket() != -1
            || schema.primary_keys().is_empty()
            || schema
                .partition_keys()
                .iter()
                .any(|key| !schema.primary_keys().contains(key))
        {
            return Err(crate::Error::Unsupported {
                message: "Dynamic bucket routing requires a Paimon primary-key table with bucket=-1 and partition columns included in the primary key".into(),
            });
        }
        if schema.options().contains_key("bucket-key") {
            return Err(data_invalid(
                "Cannot define 'bucket-key' in dynamic bucket mode",
            ));
        }

        let keys = schema.trimmed_primary_keys();
        let required = schema
            .partition_keys()
            .iter()
            .chain(&keys)
            .map(String::as_str)
            .collect::<HashSet<_>>();
        let fields = schema
            .fields()
            .iter()
            .filter(|field| required.contains(field.name()))
            .cloned()
            .collect::<Vec<_>>();
        let indices = |names: &[String]| {
            names
                .iter()
                .map(|name| {
                    fields
                        .iter()
                        .position(|field| field.name() == name)
                        .ok_or_else(|| data_invalid(format!("Unknown routing column '{name}'")))
                })
                .collect::<Result<Vec<_>>>()
        };
        let partition_indices = indices(schema.partition_keys())?;
        let key_indices = indices(&keys)?;
        let partition_fields = schema.partition_fields();
        Ok(Self {
            arrow_schema: crate::arrow::build_target_arrow_schema(&fields)?,
            partition_schema: crate::arrow::build_target_arrow_schema(&partition_fields)?,
            fields,
            partition_fields,
            partition_indices,
            key_indices,
            num_channels,
            num_assigners,
        })
    }

    pub fn route(&self, batch: &RecordBatch) -> Result<DynamicBucketAssignerRouting> {
        Ok(self.route_input(batch, None)?.1)
    }

    /// Prehashed input needs only partition columns. Its payload and key
    /// arrays are deliberately not read or normalized a second time.
    pub(super) fn route_with_partitions(
        &self,
        batch: &RecordBatch,
        key_hashes: Option<&Int32Array>,
    ) -> Result<(Vec<Vec<u8>>, DynamicBucketAssignerRouting)> {
        let (partitions, routing) = self.route_input(batch, key_hashes)?;
        Ok((
            partitions
                .into_iter()
                .map(|row| row.to_serialized_bytes())
                .collect(),
            routing,
        ))
    }

    fn route_input(
        &self,
        batch: &RecordBatch,
        key_hashes: Option<&Int32Array>,
    ) -> Result<(Vec<BinaryRow>, DynamicBucketAssignerRouting)> {
        if let Some(hashes) = key_hashes {
            if hashes.len() != batch.num_rows() || hashes.null_count() != 0 {
                return Err(data_invalid(
                    "Dynamic key hashes must be non-null and match the input row count",
                ));
            }
        }
        let (input, fields, partition_indices, hashes) = if let Some(hashes) = key_hashes {
            (
                project_routing_input(batch, &self.partition_fields, &self.partition_schema)?,
                &self.partition_fields,
                (0..self.partition_fields.len()).collect::<Vec<_>>(),
                hashes.clone(),
            )
        } else {
            let input = project_routing_input(batch, &self.fields, &self.arrow_schema)?;
            let hashes =
                Int32Array::from(batch_hash_codes(&input, &self.key_indices, &self.fields)?);
            (input, &self.fields, self.partition_indices.clone(), hashes)
        };
        let partitions = batch_build_binary_rows(&input, &partition_indices, fields)?;
        let assigners = Int32Array::from_iter_values(partitions.iter().zip(hashes.values()).map(
            |(partition, hash)| {
                compute_assigner(
                    partition.hash_code(),
                    *hash,
                    self.num_channels,
                    self.num_assigners,
                )
            },
        ));
        Ok((
            partitions,
            DynamicBucketAssignerRouting {
                assigners,
                key_hashes: hashes,
            },
        ))
    }
}

/// Java signed remainder and `int` addition, including overflow before the
/// final remainder. The router constructor validates both divisors.
pub(super) fn compute_assigner(
    partition_hash: i32,
    key_hash: i32,
    num_channels: i32,
    num_assigners: i32,
) -> i32 {
    let start = (partition_hash % num_channels).wrapping_abs();
    let assigner = (key_hash % num_assigners).wrapping_abs();
    start.wrapping_add(assigner) % num_channels
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{
        DataType, DecimalType, IntType, LocalZonedTimestampType, Schema, TimestampType, VarCharType,
    };
    use arrow_array::{
        ArrayRef, Decimal128Array, LargeStringArray, StringArray, TimestampMicrosecondArray,
    };
    use arrow_schema::{DataType as ArrowType, Field, Schema as ArrowSchema};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn schema() -> TableSchema {
        TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("p", DataType::VarChar(VarCharType::string_type()))
                .column("label", DataType::VarChar(VarCharType::string_type()))
                .column("payload", DataType::VarChar(VarCharType::string_type()))
                .primary_key(["p", "id", "label"])
                .partition_keys(["p"])
                .option("bucket", "-1")
                .option("primary-key.nullable", "true")
                .build()
                .unwrap(),
        )
    }

    fn input() -> RecordBatch {
        RecordBatch::try_from_iter([
            (
                "label",
                Arc::new(LargeStringArray::from(vec![
                    Some("hello"),
                    Some(""),
                    Some("abcdefghijk"),
                    Some("你好"),
                    None,
                ])) as ArrayRef,
            ),
            (
                "ignored",
                Arc::new(Int32Array::from(vec![0; 5])) as ArrayRef,
            ),
            (
                "p",
                Arc::new(StringArray::from(vec![
                    Some("east"),
                    Some("west"),
                    None,
                    Some("abcdefghijk"),
                    Some("east"),
                ])) as ArrayRef,
            ),
            (
                "id",
                Arc::new(Int32Array::from(vec![
                    Some(5),
                    Some(-3),
                    None,
                    Some(i32::MIN),
                    Some(i32::MAX),
                ])) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    #[test]
    fn routes_reordered_key_only_rows_using_current_java_hashes() {
        // Current BinaryRowWriter + BucketAssigner.computeAssigner(7, 3).
        // The partition columns do not participate in the trimmed key hash.
        let router = DynamicBucketAssignerRouter::new(&schema(), 7, 3).unwrap();
        let batch = input();
        let routed = router.route(&batch).unwrap();
        assert_eq!(
            routed.key_hashes.values(),
            &[
                -1_451_184_867,
                556_362_117,
                1_425_286_886,
                308_673_824,
                -1_856_341_548
            ]
        );
        assert_eq!(routed.assigners.values(), &[6, 6, 5, 2, 6]);
        assert_eq!(routed.assigners.null_count(), 0);
        assert_eq!(routed.key_hashes.null_count(), 0);
        // SDK Tables arrive as batches: boundaries must not change ownership.
        let pieces = [
            router.route(&batch.slice(0, 2)).unwrap(),
            router.route(&batch.slice(2, 3)).unwrap(),
        ];
        assert_eq!(
            pieces
                .iter()
                .flat_map(|part| part.assigners.values().iter().copied())
                .collect::<Vec<_>>(),
            routed.assigners.values().to_vec()
        );
        let empty = router.route(&batch.slice(0, 0)).unwrap();
        assert_eq!(empty.assigners.len(), 0);
        assert_eq!(empty.key_hashes.len(), 0);
    }

    #[test]
    fn prehashed_routing_reads_only_partition_columns() {
        let router = DynamicBucketAssignerRouter::new(&schema(), 7, 3).unwrap();
        let batch = input();
        let expected = router.route(&batch).unwrap();
        let partition_only = batch.project(&[2]).unwrap();
        let (partitions, actual) = router
            .route_with_partitions(&partition_only, Some(&expected.key_hashes))
            .unwrap();
        assert_eq!(actual.assigners, expected.assigners);
        assert_eq!(actual.key_hashes, expected.key_hashes);
        assert_eq!(partitions.len(), batch.num_rows());
        assert!(router.route(&partition_only).is_err());
        for hashes in [Int32Array::from(vec![0]), Int32Array::from(vec![None; 5])] {
            assert!(router
                .route_with_partitions(&partition_only, Some(&hashes))
                .is_err());
        }
    }

    #[test]
    fn unpartitioned_prehashed_rows_retain_their_count() {
        let schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .primary_key(["id"])
                .option("bucket", "-1")
                .build()
                .unwrap(),
        );
        let router = DynamicBucketAssignerRouter::new(&schema, 7, 3).unwrap();
        let batch = RecordBatch::try_from_iter([(
            "payload",
            Arc::new(StringArray::from(vec!["ignored"; 4])) as ArrayRef,
        )])
        .unwrap();
        let hashes = Int32Array::from(vec![i32::MIN, -1, 0, i32::MAX]);
        let (partitions, routed) = router.route_with_partitions(&batch, Some(&hashes)).unwrap();
        // The empty Java BinaryRow hash is -1670924195, whose abs(% 7) is 3.
        assert_eq!(partitions.len(), 4);
        assert_eq!(routed.assigners.values(), &[5, 4, 3, 4]);
    }

    #[test]
    fn nullable_noncompact_keys_keep_java_variable_slots() {
        for (data_type, values, expected_hash) in [
            (
                DataType::Decimal(DecimalType::new(20, 2).unwrap()),
                Arc::new(
                    Decimal128Array::from(vec![None::<i128>])
                        .with_precision_and_scale(20, 2)
                        .unwrap(),
                ) as ArrayRef,
                -1_380_280_760,
            ),
            (
                DataType::Timestamp(TimestampType::new(6).unwrap()),
                Arc::new(TimestampMicrosecondArray::from(vec![None::<i64>])) as ArrayRef,
                -340_882_661,
            ),
            (
                DataType::LocalZonedTimestamp(LocalZonedTimestampType::new(6).unwrap()),
                Arc::new(TimestampMicrosecondArray::from(vec![None::<i64>]).with_timezone("UTC"))
                    as ArrayRef,
                -340_882_661,
            ),
        ] {
            let schema = TableSchema::new(
                0,
                &Schema::builder()
                    .column("id", data_type)
                    .primary_key(["id"])
                    .option("bucket", "-1")
                    .option("primary-key.nullable", "true")
                    .build()
                    .unwrap(),
            );
            let router = DynamicBucketAssignerRouter::new(&schema, 7, 3).unwrap();
            let batch = RecordBatch::try_from_iter([("id", values)]).unwrap();
            assert_eq!(
                router.route(&batch).unwrap().key_hashes.values(),
                &[expected_hash]
            );
        }
    }

    #[test]
    fn invalid_schemas_parallelism_and_required_inputs_fail_before_routing() {
        for (channels, assigners) in [(0, 1), (1, 0), (-1, 1), (1, -1)] {
            assert!(DynamicBucketAssignerRouter::new(&schema(), channels, assigners).is_err());
        }
        for options in [
            HashMap::from([("bucket".into(), "4".into())]),
            HashMap::from([("bucket-key".into(), "id".into())]),
            HashMap::from([("bucket-key".into(), "".into())]),
        ] {
            assert!(
                DynamicBucketAssignerRouter::new(&schema().copy_with_options(options), 7, 3)
                    .is_err()
            );
        }
        let no_primary_key = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .build()
                .unwrap(),
        );
        assert!(DynamicBucketAssignerRouter::new(&no_primary_key, 7, 3).is_err());
        let cross_partition = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Int(IntType::new()))
                .column("p", DataType::VarChar(VarCharType::string_type()))
                .primary_key(["id"])
                .partition_keys(["p"])
                .build()
                .unwrap(),
        );
        assert!(DynamicBucketAssignerRouter::new(&cross_partition, 7, 3).is_err());
        let router = DynamicBucketAssignerRouter::new(&schema(), 7, 3).unwrap();
        let batch = input();
        assert!(router.route(&batch.project(&[0, 2]).unwrap()).is_err());
        let duplicate = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                Field::new("id", ArrowType::Int32, true),
                Field::new("id", ArrowType::Int32, true),
            ])),
            vec![batch.column(3).clone(), batch.column(3).clone()],
        )
        .unwrap();
        assert!(router.route(&duplicate).is_err());
        let incompatible = RecordBatch::try_from_iter([
            ("p", batch.column(2).clone()),
            ("label", batch.column(0).clone()),
            ("id", Arc::new(StringArray::from(vec!["1"; 5])) as ArrayRef),
        ])
        .unwrap();
        assert!(router.route(&incompatible).is_err());
    }

    #[test]
    fn java_assigner_remainder_keeps_negative_hashes_and_int_min() {
        for (partition, expected) in [
            (i32::MIN, [4, 3, 2, 3]),
            (-1, [3, 2, 1, 2]),
            (0, [2, 1, 0, 1]),
            (i32::MAX, [3, 2, 1, 2]),
        ] {
            for (key, expected) in [i32::MIN, -1, 0, i32::MAX].into_iter().zip(expected) {
                assert_eq!(compute_assigner(partition, key, 7, 3), expected);
            }
        }
        assert_eq!(compute_assigner(-1, i32::MIN, 1, 1), 0);
    }
}
