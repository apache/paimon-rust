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

//! Bucket assignment for inputs distributed before constructing file writers.

use super::bucket_function::{batch_bucket_ids, validate_bucket_function};
use super::postpone_bucket_plan::data_invalid;
use super::postpone_fixed_bucket_router::validate_postpone_fixed_bucket_schema;
use super::table_write::take_rows;
use super::write_batch_normalize::normalize_write_array;
use crate::spec::{
    batch_to_serialized_bytes, BucketFunctionType, CoreOptions, DataField, TableSchema,
    POSTPONE_BUCKET,
};
use crate::table::PostponeBucketPlan;
use crate::Result;
use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::SchemaRef;
use std::collections::{HashMap, HashSet};

/// Compute the bucket of every input row without opening files or authorizing a table.
///
/// This is the same bucket computation used by fixed-bucket writers and by
/// Java's `FixedBucketRowKeyExtractor` and `PostponeBucketRouter`. Inputs can
/// omit payload fields and reorder columns, but must contain each partition
/// and bucket key exactly once. Output positions retain the input row order.
#[derive(Debug, Clone)]
pub struct BucketRouter {
    fields: Vec<DataField>,
    arrow_schema: SchemaRef,
    partition_indices: Vec<usize>,
    bucket_indices: Vec<usize>,
    bucket_function: BucketFunctionType,
    total_buckets: i32,
    plan: PostponeBucketPlan,
}

impl BucketRouter {
    /// Construct an owned router for a fixed-bucket or postpone-bucket schema.
    ///
    /// Postpone routing uses the configured default until an explicit plan is
    /// supplied. Without either, routing an unknown input partition fails.
    pub fn new(schema: &TableSchema) -> Result<Self> {
        let options = CoreOptions::new(schema.options());
        let total_buckets = options.bucket();
        if options.is_format_table() || (total_buckets <= 0 && total_buckets != POSTPONE_BUCKET) {
            return Err(crate::Error::Unsupported {
                message: "Bucket routing requires a Paimon fixed-bucket or postpone-bucket table"
                    .into(),
            });
        }
        if total_buckets == POSTPONE_BUCKET {
            validate_postpone_fixed_bucket_schema(schema)?;
        }

        let bucket_keys = schema.bucket_keys();
        let required = schema
            .partition_keys()
            .iter()
            .chain(&bucket_keys)
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
        let bucket_indices = indices(&bucket_keys)?;
        let bucket_function = options.bucket_function_type()?;
        let bucket_fields = bucket_indices
            .iter()
            .map(|&index| fields[index].clone())
            .collect::<Vec<_>>();
        validate_bucket_function(bucket_function, &bucket_fields)?;
        let plan = if total_buckets == POSTPONE_BUCKET {
            options
                .postpone_default_bucket_num()?
                .map(PostponeBucketPlan::with_default_bucket_num)
                .unwrap_or_default()
        } else {
            PostponeBucketPlan::default()
        };
        Ok(Self {
            arrow_schema: crate::arrow::build_target_arrow_schema(&fields)?,
            fields,
            partition_indices,
            bucket_indices,
            bucket_function,
            total_buckets,
            plan,
        })
    }

    /// Replace postpone routing metadata, including any configured default.
    pub fn with_bucket_plan(mut self, plan: PostponeBucketPlan) -> Result<Self> {
        if self.total_buckets != POSTPONE_BUCKET {
            return Err(data_invalid(
                "A bucket plan requires a postpone-bucket schema",
            ));
        }
        self.plan = plan;
        Ok(self)
    }

    /// Return non-null Int32 bucket IDs in input row order.
    pub fn route(&self, batch: &RecordBatch) -> Result<Int32Array> {
        Ok(self.route_with_partitions(batch)?.1)
    }

    pub(super) fn plan(&self) -> &PostponeBucketPlan {
        &self.plan
    }

    pub(super) fn plan_mut(&mut self) -> &mut PostponeBucketPlan {
        &mut self.plan
    }

    pub(super) fn route_with_partitions(
        &self,
        batch: &RecordBatch,
    ) -> Result<(Vec<Vec<u8>>, Int32Array)> {
        let input_schema = batch.schema();
        let mut names = HashSet::new();
        for field in input_schema.fields() {
            if !names.insert(field.name()) {
                return Err(data_invalid(format!(
                    "Duplicate input column '{}' in bucket routing",
                    field.name()
                )));
            }
        }
        let columns = self
            .fields
            .iter()
            .zip(self.arrow_schema.fields())
            .map(|(field, expected)| {
                let index = input_schema.index_of(field.name()).map_err(|_| {
                    data_invalid(format!("Missing routing column '{}'", field.name()))
                })?;
                normalize_write_array(batch.column(index), expected.data_type())
            })
            .collect::<Result<Vec<_>>>()?;
        // Project by name before applying indices resolved from the table
        // schema. Unused payloads do not participate in routing validation.
        let input = RecordBatch::try_new(self.arrow_schema.clone(), columns)
            .map_err(|error| data_invalid(error.to_string()))?;
        let partitions = batch_to_serialized_bytes(&input, &self.partition_indices, &self.fields)?;
        if self.total_buckets > 0 {
            let buckets = self.route_count(&input, self.total_buckets)?;
            return Ok((partitions, Int32Array::from(buckets)));
        }

        let mut groups: HashMap<i32, Vec<usize>> = HashMap::new();
        for (row, partition) in partitions.iter().enumerate() {
            let count = self.plan.total_buckets(partition).ok_or_else(|| {
                data_invalid("Postpone bucket plan does not contain an input partition")
            })?;
            groups.entry(count).or_default().push(row);
        }
        let mut buckets = vec![0; batch.num_rows()];
        for (count, rows) in groups {
            let group = if rows.len() == input.num_rows() {
                input.clone()
            } else {
                take_rows(&input, &rows)?
            };
            for (row, bucket) in rows.into_iter().zip(self.route_count(&group, count)?) {
                buckets[row] = bucket;
            }
        }
        Ok((partitions, Int32Array::from(buckets)))
    }

    fn route_count(&self, batch: &RecordBatch, count: i32) -> Result<Vec<i32>> {
        if count == 1 || (self.total_buckets == POSTPONE_BUCKET && self.bucket_indices.is_empty()) {
            return Ok(vec![0; batch.num_rows()]);
        }
        batch_bucket_ids(
            batch,
            &self.bucket_indices,
            &self.fields,
            self.bucket_function,
            count,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{
        BigIntType, DataType, DateType, DecimalType, IntType, Schema, TimestampType, VarCharType,
    };
    use arrow_array::{
        types::Int8Type, Array, ArrayRef, Date32Array, Decimal128Array, DictionaryArray,
        Int64Array, Int8Array, LargeStringArray, StringArray, StructArray,
        TimestampNanosecondArray,
    };
    use arrow_schema::{DataType as ArrowType, Field, Schema as ArrowSchema};
    use std::sync::Arc;

    fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
        let fields = columns
            .iter()
            .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
            .collect::<Vec<_>>();
        RecordBatch::try_new(
            Arc::new(ArrowSchema::new(fields)),
            columns.into_iter().map(|(_, array)| array).collect(),
        )
        .unwrap()
    }

    fn scalar_schema(data_type: DataType, function: &str, count: i32) -> TableSchema {
        TableSchema::new(
            0,
            &Schema::builder()
                .column("id", data_type)
                .option("bucket-key", "id")
                .option("bucket-function.type", function)
                .option("bucket", count.to_string())
                .build()
                .unwrap(),
        )
    }

    fn check_java_scalar(data_type: DataType, values: ArrayRef, expected: &[i32]) {
        let router = BucketRouter::new(&scalar_schema(data_type, "default", 100)).unwrap();
        assert_eq!(
            router.route(&batch(vec![("id", values)])).unwrap().values(),
            expected
        );
    }

    #[test]
    fn fixed_java_reference_keys_ignore_payload_and_input_order() {
        // FixedBucketRowKeyExtractorTest.testBucket: row (5, 6, 7), 100 buckets.
        // A nested payload is deliberately unrelated to any routing field.
        let nested = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("child", ArrowType::Int64, true)),
            Arc::new(Int64Array::from(vec![999])) as ArrayRef,
        )]));
        let input = batch(vec![
            ("c", Arc::new(Int32Array::from(vec![7]))),
            ("payload", nested),
            ("b", Arc::new(Int32Array::from(vec![6]))),
            ("a", Arc::new(Int32Array::from(vec![5]))),
        ]);
        for (keys, expected) in [("a", 96), ("a,b", 27), ("a,b,c", 40)] {
            let schema = TableSchema::new(
                0,
                &Schema::builder()
                    .column("a", DataType::Int(IntType::new()))
                    .column("b", DataType::Int(IntType::new()))
                    .column("c", DataType::Int(IntType::new()))
                    .option("bucket", "100")
                    .option("bucket-key", keys)
                    .build()
                    .unwrap(),
            );
            let router = BucketRouter::new(&schema).unwrap();
            assert_eq!(router.route(&input).unwrap().values(), &[expected]);
            if keys == "a" {
                assert_eq!(
                    router
                        .route(&input.project(&[3]).unwrap())
                        .unwrap()
                        .values(),
                    &[expected]
                );
            }
            assert!(router.route(&input.slice(0, 0)).unwrap().is_empty());
        }
    }

    #[test]
    fn default_scalar_values_match_current_java_binary_row_writers() {
        // Generated with current Java BinaryRow/BinaryRowWriter and
        // Math.abs(row.hashCode() % 100), including variable NULL slots.
        check_java_scalar(
            DataType::Int(IntType::new()),
            Arc::new(Int32Array::from(vec![i32::MIN, -3, 0, 5, i32::MAX])),
            &[19, 15, 99, 96, 21],
        );
        check_java_scalar(
            DataType::VarChar(VarCharType::string_type()),
            Arc::new(LargeStringArray::from(vec![
                None,
                Some(""),
                Some("hello"),
                Some("abcdefghijk"),
                Some("你好"),
            ])),
            &[44, 19, 46, 23, 58],
        );
        check_java_scalar(
            DataType::Decimal(DecimalType::new(20, 2).unwrap()),
            Arc::new(
                Decimal128Array::from(vec![None, Some(12340), Some(-9999999), Some(0)])
                    .with_precision_and_scale(20, 2)
                    .unwrap(),
            ),
            &[60, 60, 44, 85],
        );
        check_java_scalar(
            DataType::Date(DateType::new()),
            Arc::new(Date32Array::from(vec![-3, 0, 5])),
            &[15, 99, 96],
        );
        check_java_scalar(
            DataType::Timestamp(TimestampType::new(9).unwrap()),
            Arc::new(TimestampNanosecondArray::from(vec![
                -1_000_001,
                -1,
                0,
                1,
                1_000_001,
                i64::MAX,
            ])),
            &[38, 85, 54, 32, 55, 39],
        );
    }

    #[test]
    fn fixed_partition_only_rows_hash_the_empty_bucket_key() {
        let schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("pt", DataType::VarChar(VarCharType::string_type()))
                .partition_keys(["pt"])
                .option("bucket", "100")
                .build()
                .unwrap(),
        );
        let router = BucketRouter::new(&schema).unwrap();
        // Current Java BinaryRow(0).hashCode() is -1670924195. Fixed routing
        // still hashes that key when every table field is a partition field.
        assert_eq!(
            router
                .route(&batch(vec![(
                    "pt",
                    Arc::new(StringArray::from(vec![Some("a"), None])),
                )]))
                .unwrap()
                .values(),
            &[95, 95]
        );
    }

    #[test]
    fn nullable_decimal_primary_key_uses_java_bucket() {
        let schema = TableSchema::new(
            0,
            &Schema::builder()
                .column("id", DataType::Decimal(DecimalType::new(20, 2).unwrap()))
                .primary_key(["id"])
                .option("primary-key.nullable", "true")
                .option("bucket", "100")
                .build()
                .unwrap(),
        );
        let input = batch(vec![(
            "id",
            Arc::new(
                Decimal128Array::from(vec![None, Some(12340)])
                    .with_precision_and_scale(20, 2)
                    .unwrap(),
            ),
        )]);
        assert_eq!(
            BucketRouter::new(&schema)
                .unwrap()
                .route(&input)
                .unwrap()
                .values(),
            &[60, 60]
        );
    }

    #[test]
    fn mod_and_hive_preserve_nulls_and_integer_boundaries() {
        let input = batch(vec![(
            "id",
            Arc::new(Int64Array::from(vec![
                None,
                Some(i64::MIN),
                Some(-3),
                Some(0),
                Some(i64::MAX),
            ])),
        )]);
        let router = BucketRouter::new(&scalar_schema(
            DataType::BigInt(BigIntType::new()),
            "mod",
            5,
        ))
        .unwrap();
        assert_eq!(router.route(&input).unwrap().values(), &[0, 2, 2, 0, 2]);
        // HiveBucketFunction uses the positive low 31 bits of Long.hashCode.
        let router = BucketRouter::new(&scalar_schema(
            DataType::BigInt(BigIntType::new()),
            "hive",
            5,
        ))
        .unwrap();
        assert_eq!(router.route(&input).unwrap().values(), &[0, 0, 2, 0, 0]);
    }

    #[test]
    fn postpone_routes_interleaved_partitions_and_replaces_default() {
        let schema = TableSchema::new(
            0,
            &Schema::builder()
                .column(
                    "total_buckets",
                    DataType::VarChar(VarCharType::string_type()),
                )
                .column("id", DataType::Int(IntType::new()))
                .partition_keys(["total_buckets"])
                .primary_key(["total_buckets", "id"])
                .option("primary-key.nullable", "true")
                .option("bucket", "-2")
                .option("postpone.default-bucket-num", "1")
                .build()
                .unwrap(),
        );
        let input = batch(vec![
            (
                "id",
                Arc::new(Int32Array::from(vec![5, -3, i32::MIN, 0, i32::MAX])),
            ),
            (
                "total_buckets",
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("b"),
                    None,
                    Some("a"),
                    Some("b"),
                ])),
            ),
        ]);
        let router = BucketRouter::new(&schema).unwrap();
        assert_eq!(router.route(&input).unwrap().values(), &[0; 5]);
        // Both partition and final count field are called total_buckets;
        // the plan codec uses their positions to retain that valid schema.
        let plan = PostponeBucketPlan::from_arrow(
            &schema,
            &batch(vec![
                (
                    "total_buckets",
                    Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
                ),
                ("total_buckets", Arc::new(Int32Array::from(vec![3, 5, 7]))),
            ]),
        )
        .unwrap();
        let router = router.with_bucket_plan(plan).unwrap();
        // Java BinaryRow hashes: 260196596, -528982515, 916225219,
        // -300363099 and -1125657321, reduced by each partition's count.
        assert_eq!(router.route(&input).unwrap().values(), &[2, 0, 0, 0, 1]);
        let empty = router
            .clone()
            .with_bucket_plan(PostponeBucketPlan::default())
            .unwrap();
        assert!(empty
            .route(&input)
            .unwrap_err()
            .to_string()
            .contains("does not contain"));
    }

    #[test]
    fn routing_rejects_ambiguous_missing_and_incompatible_columns() {
        let schema = scalar_schema(DataType::Int(IntType::new()), "default", 2);
        let router = BucketRouter::new(&schema).unwrap();
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        assert!(router
            .route(&batch(vec![("id", ints.clone()), ("id", ints.clone())]))
            .unwrap_err()
            .to_string()
            .contains("Duplicate"));
        assert!(router
            .route(&batch(vec![("other", ints)]))
            .unwrap_err()
            .to_string()
            .contains("Missing routing column 'id'"));
        assert!(router
            .route(&batch(vec![("id", Arc::new(Int64Array::from(vec![1])))]))
            .is_err());
        let strings = Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![0]),
                Arc::new(StringArray::from(vec!["value"])),
            )
            .unwrap(),
        );
        let string_router = BucketRouter::new(&scalar_schema(
            DataType::VarChar(VarCharType::string_type()),
            "default",
            2,
        ))
        .unwrap();
        assert!(string_router.route(&batch(vec![("id", strings)])).is_err());
        assert!(router
            .with_bucket_plan(PostponeBucketPlan::default())
            .is_err());
        for count in [-2, -1, 0] {
            let mut options = schema.options().clone();
            options.insert("bucket".into(), count.to_string());
            assert!(BucketRouter::new(&schema.copy_with_replaced_options(options)).is_err());
        }
        assert!(BucketRouter::new(&scalar_schema(
            DataType::VarChar(VarCharType::string_type()),
            "mod",
            2
        ))
        .is_err());
    }
}
