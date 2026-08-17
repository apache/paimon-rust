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

use super::postpone_bucket_plan::data_invalid;
use crate::spec::{
    batch_to_serialized_bytes, BucketFunctionType, CoreOptions, DataField, EMPTY_SERIALIZED_ROW,
    POSTPONE_BUCKET,
};
use crate::table::bucket_function::{batch_bucket_ids, validate_bucket_function};
use crate::table::table_write::take_rows;
use crate::table::{PostponeBucketPlan, Table};
use crate::Result;
use arrow_array::RecordBatch;
use std::collections::HashMap;

#[derive(Debug)]
pub(crate) struct PostponeBucketBatch {
    pub(crate) partition: Vec<u8>,
    pub(crate) bucket: i32,
    pub(crate) batch: RecordBatch,
}

pub(crate) struct PostponeFixedBucketRouter {
    fields: Vec<DataField>,
    partition_field_indices: Vec<usize>,
    bucket_key_indices: Vec<usize>,
    bucket_function_type: BucketFunctionType,
    plan: PostponeBucketPlan,
}

impl PostponeFixedBucketRouter {
    pub(crate) fn new(table: &Table, plan: PostponeBucketPlan) -> Result<Self> {
        validate_postpone_fixed_bucket_table(table)?;
        let schema = table.schema();
        let options = CoreOptions::new(schema.options());
        let bucket_key_indices = field_indices(schema.fields(), &schema.bucket_keys());
        let bucket_key_fields = bucket_key_indices
            .iter()
            .map(|&index| schema.fields()[index].clone())
            .collect::<Vec<_>>();
        let bucket_function_type = options.bucket_function_type()?;
        if !bucket_key_fields.is_empty() {
            validate_bucket_function(bucket_function_type, &bucket_key_fields)?;
        }
        Ok(Self {
            fields: schema.fields().to_vec(),
            partition_field_indices: field_indices(schema.fields(), schema.partition_keys()),
            bucket_key_indices,
            bucket_function_type,
            plan,
        })
    }

    pub(crate) fn route(&self, batch: &RecordBatch) -> Result<Vec<PostponeBucketBatch>> {
        let mut output = Vec::new();
        for (partition, batch) in
            partition_batches(batch, &self.partition_field_indices, &self.fields)?
        {
            let total_buckets = self.plan.total_buckets(&partition).ok_or_else(|| {
                data_invalid("Postpone bucket plan does not contain an input partition")
            })?;
            let buckets = if total_buckets <= 1 || self.bucket_key_indices.is_empty() {
                vec![0; batch.num_rows()]
            } else {
                batch_bucket_ids(
                    &batch,
                    &self.bucket_key_indices,
                    &self.fields,
                    self.bucket_function_type,
                    total_buckets,
                )?
            };
            let mut groups: HashMap<i32, Vec<usize>> = HashMap::new();
            for (row, bucket) in buckets.into_iter().enumerate() {
                groups.entry(bucket).or_default().push(row);
            }
            for (bucket, rows) in groups {
                output.push(PostponeBucketBatch {
                    partition: partition.clone(),
                    bucket,
                    batch: take_rows(&batch, &rows)?,
                });
            }
        }
        Ok(output)
    }

    pub(crate) fn total_buckets(&self, partition: &[u8]) -> Option<i32> {
        self.plan.total_buckets(partition)
    }
}

pub(crate) fn validate_postpone_fixed_bucket_table(table: &Table) -> Result<()> {
    let schema = table.schema();
    let bucket = CoreOptions::new(schema.options()).bucket();
    if table.is_format_table() || bucket != POSTPONE_BUCKET || schema.primary_keys().is_empty() {
        return Err(crate::Error::Unsupported {
            message: format!(
                "Postpone fixed-bucket writes require a Paimon primary-key table with bucket=-2, but table '{}' has bucket={bucket}",
                table.identifier().full_name()
            ),
        });
    }
    if schema
        .partition_keys()
        .iter()
        .any(|key| !schema.primary_keys().contains(key))
    {
        return Err(crate::Error::Unsupported {
            message: "Postpone fixed-bucket writes do not support cross-partition updates"
                .to_string(),
        });
    }
    Ok(())
}

fn field_indices(fields: &[DataField], names: &[String]) -> Vec<usize> {
    names
        .iter()
        .filter_map(|name| fields.iter().position(|field| field.name() == name))
        .collect()
}

fn partition_batches(
    batch: &RecordBatch,
    partition_field_indices: &[usize],
    fields: &[DataField],
) -> Result<Vec<(Vec<u8>, RecordBatch)>> {
    let partitions = if partition_field_indices.is_empty() {
        vec![EMPTY_SERIALIZED_ROW.clone(); batch.num_rows()]
    } else {
        batch_to_serialized_bytes(batch, partition_field_indices, fields)?
    };
    let mut groups: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
    for (row, partition) in partitions.into_iter().enumerate() {
        groups.entry(partition).or_default().push(row);
    }
    groups
        .into_iter()
        .map(|(partition, rows)| Ok((partition, take_rows(batch, &rows)?)))
        .collect()
}
