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
use std::collections::{BTreeMap, HashMap};

/// Result of assigning a bucket for a key in cross-partition mode.
enum AssignResult {
    /// Key is new or stays in the same partition.
    SamePartition { bucket: i32 },
    /// Java UseOldExistingProcessor keeps the existing location for partial
    /// update and aggregation, and rewrites the incoming partition values.
    UseOldPartition { partition: Vec<u8>, bucket: i32 },
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
    /// Java BucketAssigner uses a TreeMap: reuse the first non-full bucket,
    /// then allocate the smallest unused non-negative ID.
    partition_bucket_counts: HashMap<Vec<u8>, BTreeMap<i32, i64>>,
    target_bucket_row_number: i64,
    merge_engine: MergeEngine,
}

impl GlobalPartitionIndex {
    fn empty(target_bucket_row_number: i64, merge_engine: MergeEngine) -> Self {
        Self {
            key_to_location: HashMap::new(),
            partition_bucket_counts: HashMap::new(),
            target_bucket_row_number,
            merge_engine,
        }
    }

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
        // The cross-partition index reads every primary key.
        crate::spec::CoreOptions::new(table.schema().options()).ensure_read_authorized()?;

        let mut key_to_location: HashMap<Vec<u8>, (Vec<u8>, i32)> = HashMap::new();
        let mut partition_bucket_counts: HashMap<Vec<u8>, BTreeMap<i32, i64>> = HashMap::new();

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
        rb.with_projection(&pk_field_names)?;
        let scan = rb.new_scan().with_scan_all_files();
        let plan = scan.plan().await?;
        let read = rb.new_read()?;

        for split in plan.splits() {
            let partition_bytes = split.partition().to_serialized_bytes();
            let bucket = split.bucket();

            let mut batches = read.to_arrow(std::slice::from_ref(split))?;
            while let Some(batch) = batches.try_next().await? {
                let pk_bytes_vec =
                    batch_to_serialized_bytes(&batch, &projected_pk_indices, &pk_fields)?;
                for pk_bytes in pk_bytes_vec {
                    if key_to_location
                        .insert(pk_bytes, (partition_bytes.clone(), bucket))
                        .is_some()
                    {
                        return Err(crate::Error::DataInvalid {
                            message: "Duplicate primary key found while bootstrapping the cross-partition index".into(),
                            source: None,
                        });
                    }
                    *partition_bucket_counts
                        .entry(partition_bytes.clone())
                        .or_default()
                        .entry(bucket)
                        .or_default() += 1;
                }
            }
        }

        let mut index = Self::empty(target_bucket_row_number, merge_engine);
        index.key_to_location = key_to_location;
        index.partition_bucket_counts = partition_bucket_counts;
        Ok(index)
    }

    /// Assign a bucket for the given primary key targeting `new_partition`.
    fn assign(&mut self, pk_bytes: &[u8], new_partition: &[u8]) -> Result<AssignResult> {
        if let Some((existing_partition, existing_bucket)) = self.key_to_location.get(pk_bytes) {
            if existing_partition == new_partition {
                return Ok(AssignResult::SamePartition {
                    bucket: *existing_bucket,
                });
            }

            // Key exists in a different partition
            match self.merge_engine {
                MergeEngine::FirstRow => {
                    // FIRST_ROW: keep old data, discard new row
                    return Ok(AssignResult::Skip);
                }
                MergeEngine::Deduplicate => {
                    let old_partition = existing_partition.clone();
                    let old_bucket = *existing_bucket;

                    let old_count = self
                        .partition_bucket_counts
                        .entry(old_partition.clone())
                        .or_default()
                        .entry(old_bucket)
                        .or_default();
                    *old_count -= 1;
                    let new_bucket = self.assign_bucket_in_partition(new_partition)?;
                    self.key_to_location
                        .insert(pk_bytes.to_vec(), (new_partition.to_vec(), new_bucket));

                    return Ok(AssignResult::CrossPartition {
                        old_partition,
                        old_bucket,
                        new_bucket,
                    });
                }
                MergeEngine::PartialUpdate | MergeEngine::Aggregation => {
                    return Ok(AssignResult::UseOldPartition {
                        partition: existing_partition.clone(),
                        bucket: *existing_bucket,
                    });
                }
            }
        }

        let bucket = self.assign_bucket_in_partition(new_partition)?;
        self.key_to_location
            .insert(pk_bytes.to_vec(), (new_partition.to_vec(), bucket));
        Ok(AssignResult::SamePartition { bucket })
    }

    fn assign_bucket_in_partition(&mut self, partition: &[u8]) -> Result<i32> {
        let buckets = self
            .partition_bucket_counts
            .entry(partition.to_vec())
            .or_default();
        for (&bucket, count) in buckets.iter_mut() {
            if *count < self.target_bucket_row_number {
                *count += 1;
                return Ok(bucket);
            }
        }
        let mut next = 0;
        while buckets.contains_key(&next) {
            next = next
                .checked_add(1)
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: "No bucket IDs available for cross-partition writes".into(),
                    source: None,
                })?;
        }
        buckets.insert(next, 1);
        Ok(next)
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
    ) -> Result<Self> {
        if !crate::spec::CoreOptions::new(table.schema().options())
            .sequence_fields()
            .is_empty()
        {
            return Err(crate::Error::DataInvalid {
                message: "Cannot define 'sequence.field' for cross-partition update tables".into(),
                source: None,
            });
        }
        if table.schema().options().contains_key("bucket-key") {
            return Err(crate::Error::DataInvalid {
                message: "Cannot define 'bucket-key' for dynamic bucket tables".into(),
                source: None,
            });
        }
        if table
            .schema()
            .options()
            .contains_key("cross-partition-upsert.index-ttl")
        {
            return Err(crate::Error::Unsupported {
                message:
                    "Cross-partition writes do not support 'cross-partition-upsert.index-ttl' yet"
                        .into(),
            });
        }
        Ok(Self {
            table,
            partition_field_indices,
            primary_key_indices,
            global_partition_index: None,
            target_bucket_row_number,
            merge_engine,
        })
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

        let mut partition_bytes_vec =
            batch_to_serialized_bytes(batch, &self.partition_field_indices, fields)?;
        let pk_bytes_vec = batch_to_serialized_bytes(batch, &self.primary_key_indices, fields)?;

        let global_index = self.global_partition_index.as_mut().unwrap();
        let num_rows = batch.num_rows();
        let mut buckets = Vec::with_capacity(num_rows);
        let mut deletes = Vec::new();
        let mut skips = Vec::new();

        for row_idx in 0..num_rows {
            match global_index.assign(&pk_bytes_vec[row_idx], &partition_bytes_vec[row_idx])? {
                AssignResult::SamePartition { bucket } => {
                    buckets.push(bucket);
                }
                AssignResult::UseOldPartition { partition, bucket } => {
                    partition_bytes_vec[row_idx] = partition;
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
    ) -> Result<HashMap<PartitionBucketKey, Vec<IndexFileMeta>>> {
        Ok(HashMap::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_assignment_reuses_lowest_nonfull_bucket_then_first_unused_id() {
        let mut index = GlobalPartitionIndex::empty(2, MergeEngine::Deduplicate);
        index
            .partition_bucket_counts
            .insert(vec![1], [(0, 2), (2, 1), (4, 1)].into());
        assert_eq!(index.assign_bucket_in_partition(&[1]).unwrap(), 2);
        assert_eq!(index.assign_bucket_in_partition(&[1]).unwrap(), 4);
        assert_eq!(index.assign_bucket_in_partition(&[1]).unwrap(), 1);
        assert_eq!(index.assign_bucket_in_partition(&[1]).unwrap(), 1);
        assert_eq!(index.assign_bucket_in_partition(&[1]).unwrap(), 3);
    }

    #[test]
    fn migration_releases_old_bucket_capacity_without_forgetting_other_keys() {
        let mut index = GlobalPartitionIndex::empty(1, MergeEngine::Deduplicate);
        assert!(matches!(
            index.assign(&[1], &[1]).unwrap(),
            AssignResult::SamePartition { bucket: 0 }
        ));
        assert!(matches!(
            index.assign(&[2], &[1]).unwrap(),
            AssignResult::SamePartition { bucket: 1 }
        ));
        assert!(matches!(
            index.assign(&[1], &[2]).unwrap(),
            AssignResult::CrossPartition {
                old_bucket: 0,
                new_bucket: 0,
                ..
            }
        ));
        assert!(matches!(
            index.assign(&[3], &[1]).unwrap(),
            AssignResult::SamePartition { bucket: 0 }
        ));
        assert!(matches!(
            index.assign(&[2], &[1]).unwrap(),
            AssignResult::SamePartition { bucket: 1 }
        ));
    }
}
