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

use crate::spec::{CoreOptions, TableSchema, POSTPONE_BUCKET};
use crate::table::table_write::take_rows;
use crate::table::{BucketRouter, PostponeBucketPlan, Table};
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
    router: BucketRouter,
}

impl PostponeFixedBucketRouter {
    pub(super) fn set_plan(&mut self, plan: PostponeBucketPlan) {
        *self.router.plan_mut() = plan;
    }

    pub(crate) fn new(table: &Table, plan: PostponeBucketPlan) -> Result<Self> {
        validate_postpone_fixed_bucket_table(table)?;
        Ok(Self {
            router: BucketRouter::new(table.schema())?.with_bucket_plan(plan)?,
        })
    }

    pub(crate) async fn load_existing_buckets(
        &mut self,
        table: &Table,
        snapshot_id: i64,
    ) -> Result<()> {
        self.router
            .plan_mut()
            .load_existing_buckets(table, snapshot_id)
            .await
    }

    pub(crate) fn route(&self, batch: &RecordBatch) -> Result<Vec<PostponeBucketBatch>> {
        let (partitions, buckets) = self.router.route_with_partitions(batch)?;
        let mut groups: HashMap<(Vec<u8>, i32), Vec<usize>> = HashMap::new();
        for (row, partition) in partitions.into_iter().enumerate() {
            groups
                .entry((partition, buckets.value(row)))
                .or_default()
                .push(row);
        }
        groups
            .into_iter()
            .map(|((partition, bucket), rows)| {
                Ok(PostponeBucketBatch {
                    partition,
                    bucket,
                    batch: take_rows(batch, &rows)?,
                })
            })
            .collect()
    }

    pub(crate) fn total_buckets(&self, partition: &[u8]) -> Option<i32> {
        self.router.plan().total_buckets(partition)
    }
}

pub(crate) fn validate_postpone_fixed_bucket_table(table: &Table) -> Result<()> {
    validate_postpone_fixed_bucket_schema(table.schema())
}

pub(super) fn validate_postpone_fixed_bucket_schema(schema: &TableSchema) -> Result<()> {
    let options = CoreOptions::new(schema.options());
    let bucket = options.bucket();
    if options.is_format_table() || bucket != POSTPONE_BUCKET || schema.primary_keys().is_empty() {
        return Err(crate::Error::Unsupported {
            message: format!(
                "Postpone fixed-bucket writes require a Paimon primary-key table with bucket=-2, but schema has bucket={bucket}"
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
