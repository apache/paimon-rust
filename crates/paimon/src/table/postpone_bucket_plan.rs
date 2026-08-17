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

use crate::spec::batch_to_serialized_bytes;
use crate::table::Table;
use crate::Result;
use arrow_array::{Array, Int32Array, RecordBatch};
use std::collections::HashMap;
use std::sync::Arc;

pub const POSTPONE_BUCKET_PLAN_TOTAL_BUCKETS_FIELD: &str = "total_buckets";

pub(crate) fn data_invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

#[derive(Debug, Clone, Default)]
pub struct PostponeBucketPlan {
    bucket_counts: Arc<HashMap<Vec<u8>, i32>>,
}

impl PostponeBucketPlan {
    pub fn from_arrow(table: &Table, batch: &RecordBatch) -> Result<Self> {
        let partition_fields = table.schema().partition_fields();
        let partition_count = partition_fields.len();
        if batch.num_columns() != partition_count + 1 {
            return Err(data_invalid(format!(
                "Postpone bucket plan expected {} partition column(s) plus '{POSTPONE_BUCKET_PLAN_TOTAL_BUCKETS_FIELD}', got {} columns",
                partition_count,
                batch.num_columns()
            )));
        }

        let expected = crate::arrow::build_target_arrow_schema(&partition_fields)?;
        let actual = batch.schema();
        if !expected
            .fields()
            .iter()
            .zip(actual.fields())
            .all(|(expected, actual)| {
                actual.name() == expected.name() && actual.data_type() == expected.data_type()
            })
        {
            return Err(data_invalid(
                "Postpone bucket plan partition fields do not match the table schema",
            ));
        }
        for (index, field) in expected.fields().iter().enumerate() {
            if !field.is_nullable() && batch.column(index).null_count() != 0 {
                return Err(data_invalid(format!(
                    "Postpone bucket plan partition column '{}' is NOT NULL but contains null values",
                    field.name()
                )));
            }
        }

        let count_field = actual.field(partition_count);
        if count_field.name() != POSTPONE_BUCKET_PLAN_TOTAL_BUCKETS_FIELD
            || count_field.data_type() != &arrow_schema::DataType::Int32
        {
            return Err(data_invalid(format!(
                "Postpone bucket plan final field must be '{POSTPONE_BUCKET_PLAN_TOTAL_BUCKETS_FIELD}': Int32, got '{}': {:?}",
                count_field.name(),
                count_field.data_type()
            )));
        }
        let counts = batch
            .column(partition_count)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                data_invalid("Postpone bucket plan total_buckets column is not Int32")
            })?;
        let partition_indices = (0..partition_count).collect::<Vec<_>>();
        let partitions = batch_to_serialized_bytes(batch, &partition_indices, &partition_fields)?;
        let mut bucket_counts = HashMap::with_capacity(batch.num_rows());
        for (row, partition) in partitions.into_iter().enumerate() {
            if counts.is_null(row) {
                return Err(data_invalid(format!(
                    "Postpone bucket plan total_buckets is null at row {row}"
                )));
            }
            let total_buckets = counts.value(row);
            if total_buckets <= 0 {
                return Err(data_invalid(format!(
                    "Postpone bucket plan total_buckets must be positive at row {row}, got {total_buckets}"
                )));
            }
            if let Some(previous) = bucket_counts.insert(partition, total_buckets) {
                if previous != total_buckets {
                    return Err(data_invalid(format!(
                        "Postpone bucket plan contains conflicting total bucket counts {previous} and {total_buckets} for one partition"
                    )));
                }
            }
        }
        Ok(Self {
            bucket_counts: Arc::new(bucket_counts),
        })
    }

    pub(crate) fn total_buckets(&self, partition: &[u8]) -> Option<i32> {
        self.bucket_counts.get(partition).copied()
    }
}
