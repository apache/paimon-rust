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

//! Bucket assignment strategy for different bucket modes.
//!
//! Uses enum dispatch to avoid dynamic dispatch overhead while keeping
//! the different bucket modes in separate files.

use crate::io::FileIO;
use crate::spec::{DataField, IndexFileMeta};
use crate::table::bucket_assigner_constant::ConstantBucketAssigner;
use crate::table::bucket_assigner_cross::CrossPartitionAssigner;
use crate::table::bucket_assigner_dynamic::DynamicBucketAssigner;
use crate::table::bucket_assigner_fixed::FixedBucketAssigner;
use crate::Result;
use arrow_array::RecordBatch;
use std::collections::HashMap;

pub(crate) type PartitionBucketKey = (Vec<u8>, i32);

/// Output of batch-level bucket assignment.
pub(crate) struct BatchAssignOutput {
    /// Per-row partition bytes. Length == batch.num_rows().
    pub partition_bytes: Vec<Vec<u8>>,
    /// Per-row bucket assignment. Length == batch.num_rows().
    pub buckets: Vec<i32>,
    /// Sparse delete info for cross-partition migrations.
    /// Each entry: (row_idx, old_partition_bytes, old_bucket).
    pub deletes: Vec<(usize, Vec<u8>, i32)>,
    /// Row indices to skip entirely (FIRST_ROW: key already exists in another partition).
    pub skips: Vec<usize>,
}

/// Bucket assignment strategy. Different bucket modes implement this trait.
pub(crate) trait BucketAssigner: Send {
    /// Assign partitions and buckets for all rows in the batch.
    /// May perform async I/O (e.g., loading indexes) before assignment.
    fn assign_batch(
        &mut self,
        batch: &RecordBatch,
        fields: &[DataField],
    ) -> impl std::future::Future<Output = Result<BatchAssignOutput>> + Send;

    /// Collect index files at commit time. Returns (partition_bucket_key -> index_files).
    fn prepare_commit_index(
        &mut self,
        file_io: &FileIO,
        index_dir: &str,
    ) -> impl std::future::Future<Output = Result<HashMap<PartitionBucketKey, Vec<IndexFileMeta>>>> + Send;
}

/// Enum dispatch wrapper for the bucket assigner implementations.
pub(crate) enum BucketAssignerEnum {
    Constant(ConstantBucketAssigner),
    Fixed(FixedBucketAssigner),
    Dynamic(DynamicBucketAssigner),
    CrossPartition(Box<CrossPartitionAssigner>),
}

impl BucketAssignerEnum {
    pub async fn assign_batch(
        &mut self,
        batch: &RecordBatch,
        fields: &[DataField],
    ) -> Result<BatchAssignOutput> {
        match self {
            Self::Constant(a) => a.assign_batch(batch, fields).await,
            Self::Fixed(a) => a.assign_batch(batch, fields).await,
            Self::Dynamic(a) => a.assign_batch(batch, fields).await,
            Self::CrossPartition(a) => a.assign_batch(batch, fields).await,
        }
    }

    pub async fn prepare_commit_index(
        &mut self,
        file_io: &FileIO,
        index_dir: &str,
    ) -> Result<HashMap<PartitionBucketKey, Vec<IndexFileMeta>>> {
        match self {
            Self::Constant(a) => a.prepare_commit_index(file_io, index_dir).await,
            Self::Fixed(a) => a.prepare_commit_index(file_io, index_dir).await,
            Self::Dynamic(a) => a.prepare_commit_index(file_io, index_dir).await,
            Self::CrossPartition(a) => a.prepare_commit_index(file_io, index_dir).await,
        }
    }
}
