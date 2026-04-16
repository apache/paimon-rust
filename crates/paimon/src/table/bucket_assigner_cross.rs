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

//! Cross-partition bucket assigner for PK tables where PK does not include partition fields.
//!
//! Builds the global PK → (partition, bucket) index by scanning all data files.

use crate::io::FileIO;
use crate::spec::{batch_to_serialized_bytes, DataField, IndexFileMeta, MergeEngine};
use crate::table::bucket_assigner::{BatchAssignOutput, BucketAssigner, PartitionBucketKey};
use crate::table::Table;
use crate::Result;
use arrow_array::RecordBatch;
use futures::TryStreamExt;
use std::collections::{HashMap, HashSet};

/// Result of assigning a bucket for a key in cross-partition mode.
enum AssignResult {
    /// Key is new or stays in the same partition.
    SamePartition { bucket: i32 },
    /// Key moved to a different partition. Caller must write a DELETE to the old location.
    CrossPartition {
        old_partition: Vec<u8>,
        old_bucket: i32,
        new_bucket: i32,
    },
    /// FIRST_ROW: key already exists in a different partition, skip this row.
    Skip,
}

/// Global index that maps primary keys to (partition, bucket) across all partitions.
///
/// Uses serialized PK bytes as the lookup key to avoid hash collisions.
struct GlobalPartitionIndex {
    /// pk_bytes -> (partition_bytes, bucket)
    key_to_location: HashMap<Vec<u8>, (Vec<u8>, i32)>,
    /// partition -> { bucket -> row_count } (only non-full buckets)
    partition_non_full_buckets: HashMap<Vec<u8>, HashMap<i32, i64>>,
    /// partition -> all known bucket ids
    partition_all_buckets: HashMap<Vec<u8>, HashSet<i32>>,
    /// partition -> next bucket id to allocate (avoids linear scan)
    partition_next_bucket_id: HashMap<Vec<u8>, i32>,
    /// (partition, bucket) -> total row count
    bucket_row_counts: HashMap<(Vec<u8>, i32), i64>,
    target_bucket_row_number: i64,
    merge_engine: MergeEngine,
}

impl GlobalPartitionIndex {
    /// Build the global partition index by scanning all data files from the latest snapshot.
    ///
    /// Uses TableRead to get deduplicated PK rows per split, so DELETE records
    /// from previous cross-partition migrations are automatically filtered out.
    async fn load_from_data_scan(
        table: &Table,
        primary_key_indices: &[usize],
        target_bucket_row_number: i64,
        merge_engine: MergeEngine,
    ) -> Result<Self> {
        let mut key_to_location: HashMap<Vec<u8>, (Vec<u8>, i32)> = HashMap::new();
        let mut bucket_row_counts: HashMap<(Vec<u8>, i32), i64> = HashMap::new();

        let fields = table.schema().fields();
        let pk_field_names: Vec<&str> = primary_key_indices
            .iter()
            .map(|&idx| fields[idx].name())
            .collect();
        let pk_fields: Vec<DataField> = primary_key_indices
            .iter()
            .map(|&idx| fields[idx].clone())
            .collect();
        let projected_pk_indices: Vec<usize> = (0..pk_fields.len()).collect();

        let mut rb = table.new_read_builder();
        rb.with_projection(&pk_field_names);
        let scan = rb.new_scan().with_scan_all_files();
        let plan = scan.plan().await?;
        let read = rb.new_read()?;

        for split in plan.splits() {
            let partition_bytes = split.partition().to_serialized_bytes();
            let bucket = split.bucket();

            let batches: Vec<RecordBatch> = read
                .to_arrow(std::slice::from_ref(split))?
                .try_collect()
                .await?;

            let pb_key = (partition_bytes.clone(), bucket);
            for batch in &batches {
                let pk_bytes_vec =
                    batch_to_serialized_bytes(batch, &projected_pk_indices, &pk_fields)?;
                let count = pk_bytes_vec.len() as i64;
                for pk_bytes in pk_bytes_vec {
                    key_to_location.insert(pk_bytes, (partition_bytes.clone(), bucket));
                }
                *bucket_row_counts.entry(pb_key.clone()).or_insert(0) += count;
            }
        }

        let mut partition_all_buckets: HashMap<Vec<u8>, HashSet<i32>> = HashMap::new();
        let mut partition_non_full_buckets: HashMap<Vec<u8>, HashMap<i32, i64>> = HashMap::new();
        for ((partition, bucket), count) in &bucket_row_counts {
            partition_all_buckets
                .entry(partition.clone())
                .or_default()
                .insert(*bucket);
            if *count < target_bucket_row_number {
                partition_non_full_buckets
                    .entry(partition.clone())
                    .or_default()
                    .insert(*bucket, *count);
            }
        }

        let partition_next_bucket_id: HashMap<Vec<u8>, i32> = partition_all_buckets
            .iter()
            .map(|(p, buckets)| {
                let next = buckets.iter().copied().max().map_or(0, |m| m + 1);
                (p.clone(), next)
            })
            .collect();

        Ok(Self {
            key_to_location,
            partition_non_full_buckets,
            partition_all_buckets,
            partition_next_bucket_id,
            bucket_row_counts,
            target_bucket_row_number,
            merge_engine,
        })
    }

    /// Assign a bucket for the given primary key targeting `new_partition`.
    fn assign(&mut self, pk_bytes: &[u8], new_partition: &[u8]) -> AssignResult {
        if let Some((existing_partition, existing_bucket)) = self.key_to_location.get(pk_bytes) {
            if existing_partition == new_partition {
                return AssignResult::SamePartition {
                    bucket: *existing_bucket,
                };
            }

            // Key exists in a different partition
            match self.merge_engine {
                MergeEngine::FirstRow => {
                    // FIRST_ROW: keep old data, discard new row
                    return AssignResult::Skip;
                }
                MergeEngine::Deduplicate => {
                    let old_partition = existing_partition.clone();
                    let old_bucket = *existing_bucket;

                    // Decrement row count for old bucket
                    let old_key = (old_partition.clone(), old_bucket);
                    let old_count = self.bucket_row_counts.get(&old_key).copied().unwrap_or(0);
                    let new_count = (old_count - 1).max(0);
                    self.bucket_row_counts.insert(old_key, new_count);
                    if new_count < self.target_bucket_row_number {
                        self.partition_non_full_buckets
                            .entry(old_partition.clone())
                            .or_default()
                            .insert(old_bucket, new_count);
                    }

                    let new_bucket = self.assign_bucket_in_partition(new_partition);
                    self.key_to_location
                        .insert(pk_bytes.to_vec(), (new_partition.to_vec(), new_bucket));

                    return AssignResult::CrossPartition {
                        old_partition,
                        old_bucket,
                        new_bucket,
                    };
                }
            }
        }

        let bucket = self.assign_bucket_in_partition(new_partition);
        self.key_to_location
            .insert(pk_bytes.to_vec(), (new_partition.to_vec(), bucket));
        AssignResult::SamePartition { bucket }
    }

    fn assign_bucket_in_partition(&mut self, partition: &[u8]) -> i32 {
        let non_full = self
            .partition_non_full_buckets
            .entry(partition.to_vec())
            .or_default();

        let mut full_buckets = Vec::new();
        let mut assigned_bucket = None;
        for (&bucket, count) in non_full.iter_mut() {
            if *count < self.target_bucket_row_number {
                *count += 1;
                assigned_bucket = Some(bucket);
                break;
            } else {
                full_buckets.push(bucket);
            }
        }
        for b in full_buckets {
            non_full.remove(&b);
        }

        let bucket = if let Some(b) = assigned_bucket {
            b
        } else {
            let all = self
                .partition_all_buckets
                .entry(partition.to_vec())
                .or_default();
            let next_id = self
                .partition_next_bucket_id
                .entry(partition.to_vec())
                .or_insert(0);
            let new_bucket = *next_id;
            *next_id += 1;
            all.insert(new_bucket);
            self.partition_non_full_buckets
                .entry(partition.to_vec())
                .or_default()
                .insert(new_bucket, 1);
            new_bucket
        };

        *self
            .bucket_row_counts
            .entry((partition.to_vec(), bucket))
            .or_insert(0) += 1;

        bucket
    }
}

/// Bucket assigner for cross-partition update mode.
///
/// Used when PK does not include partition fields and bucket=-1 (dynamic).
/// A record's partition can change over time, requiring a global index
/// across all partitions and DELETE generation for the old location.
pub(crate) struct CrossPartitionAssigner {
    table: Table,
    partition_field_indices: Vec<usize>,
    primary_key_indices: Vec<usize>,
    global_partition_index: Option<GlobalPartitionIndex>,
    target_bucket_row_number: i64,
    merge_engine: MergeEngine,
}

impl CrossPartitionAssigner {
    pub fn new(
        table: Table,
        partition_field_indices: Vec<usize>,
        primary_key_indices: Vec<usize>,
        target_bucket_row_number: i64,
        merge_engine: MergeEngine,
    ) -> Self {
        Self {
            table,
            partition_field_indices,
            primary_key_indices,
            global_partition_index: None,
            target_bucket_row_number,
            merge_engine,
        }
    }
}

impl BucketAssigner for CrossPartitionAssigner {
    async fn assign_batch(
        &mut self,
        batch: &RecordBatch,
        fields: &[DataField],
    ) -> Result<BatchAssignOutput> {
        // Lazily load global partition index by scanning data files.
        if self.global_partition_index.is_none() {
            let index = GlobalPartitionIndex::load_from_data_scan(
                &self.table,
                &self.primary_key_indices,
                self.target_bucket_row_number,
                self.merge_engine,
            )
            .await?;
            self.global_partition_index = Some(index);
        }

        let partition_bytes_vec =
            batch_to_serialized_bytes(batch, &self.partition_field_indices, fields)?;
        let pk_bytes_vec = batch_to_serialized_bytes(batch, &self.primary_key_indices, fields)?;

        let global_index = self.global_partition_index.as_mut().unwrap();
        let num_rows = batch.num_rows();
        let mut buckets = Vec::with_capacity(num_rows);
        let mut deletes = Vec::new();
        let mut skips = Vec::new();

        for row_idx in 0..num_rows {
            match global_index.assign(&pk_bytes_vec[row_idx], &partition_bytes_vec[row_idx]) {
                AssignResult::SamePartition { bucket } => {
                    buckets.push(bucket);
                }
                AssignResult::CrossPartition {
                    old_partition,
                    old_bucket,
                    new_bucket,
                } => {
                    buckets.push(new_bucket);
                    deletes.push((row_idx, old_partition, old_bucket));
                }
                AssignResult::Skip => {
                    buckets.push(-1); // dummy, will be skipped
                    skips.push(row_idx);
                }
            }
        }

        Ok(BatchAssignOutput {
            partition_bytes: partition_bytes_vec,
            buckets,
            deletes,
            skips,
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
