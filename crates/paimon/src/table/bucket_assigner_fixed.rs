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

//! Fixed bucket assigner for hash-based bucket assignment (bucket >= 2).

use crate::io::FileIO;
use crate::spec::{
    batch_hash_codes, batch_to_serialized_bytes, DataField, IndexFileMeta, EMPTY_SERIALIZED_ROW,
};
use crate::table::bucket_assigner::{BatchAssignOutput, BucketAssigner, PartitionBucketKey};
use crate::Result;
use arrow_array::RecordBatch;
use std::collections::HashMap;

/// Bucket assigner for fixed-bucket tables (bucket >= 2).
///
/// Routes rows to buckets via `hash(bucket_key) % total_buckets`.
/// Pure computation — no index loading or commit-time index files.
pub(crate) struct FixedBucketAssigner {
    partition_field_indices: Vec<usize>,
    bucket_key_indices: Vec<usize>,
    total_buckets: i32,
}

impl FixedBucketAssigner {
    pub fn new(
        partition_field_indices: Vec<usize>,
        bucket_key_indices: Vec<usize>,
        total_buckets: i32,
    ) -> Self {
        Self {
            partition_field_indices,
            bucket_key_indices,
            total_buckets,
        }
    }
}

impl BucketAssigner for FixedBucketAssigner {
    async fn assign_batch(
        &mut self,
        batch: &RecordBatch,
        fields: &[DataField],
    ) -> Result<BatchAssignOutput> {
        let num_rows = batch.num_rows();
        let partition_bytes = if self.partition_field_indices.is_empty() {
            vec![EMPTY_SERIALIZED_ROW.clone(); num_rows]
        } else {
            batch_to_serialized_bytes(batch, &self.partition_field_indices, fields)?
        };

        let hash_codes = batch_hash_codes(batch, &self.bucket_key_indices, fields)?;
        let buckets: Vec<i32> = hash_codes
            .iter()
            .map(|h| (h % self.total_buckets).wrapping_abs())
            .collect();

        Ok(BatchAssignOutput {
            partition_bytes,
            buckets,
            deletes: Vec::new(),
            skips: Vec::new(),
        })
    }

    async fn prepare_commit_index(
        &mut self,
        _file_io: &FileIO,
        _index_dir: &str,
    ) -> Result<HashMap<PartitionBucketKey, Vec<IndexFileMeta>>> {
        Ok(HashMap::new())
    }
}
