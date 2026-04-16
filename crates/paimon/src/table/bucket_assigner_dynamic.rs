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

//! Dynamic bucket assigner for PK tables with bucket=-1 where PK includes partition fields.
//!
//! Also contains the per-bucket index maintainer (`DynamicBucketIndexMaintainer`)
//! and per-partition index (`PartitionIndex`) used by both dynamic and cross-partition modes.

use crate::io::FileIO;
use crate::spec::{
    batch_hash_codes, batch_to_serialized_bytes, DataField, IndexFileMeta, IndexManifest,
    IndexManifestEntry, EMPTY_SERIALIZED_ROW,
};
use crate::table::bucket_assigner::{BatchAssignOutput, BucketAssigner, PartitionBucketKey};
use crate::table::SnapshotManager;
use crate::Result;
use arrow_array::RecordBatch;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Hash index file
// ---------------------------------------------------------------------------

/// Index type identifier for hash index files, matching Java's `HashIndexFile.HASH_INDEX`.
const HASH_INDEX: &str = "HASH";

/// Read/write hash index files.
///
/// A hash index file is a flat binary file containing `i32` values in little-endian byte order.
/// Each value is the hash code of a primary key that belongs to the associated bucket.
struct HashIndexFile;

impl HashIndexFile {
    /// Read all key hashes from a hash index file.
    async fn read(file_io: &FileIO, path: &str) -> Result<Vec<i32>> {
        let input = file_io.new_input(path)?;
        let content = input.read().await?;
        debug_assert!(
            content.len() % 4 == 0,
            "hash index file size {} is not aligned to 4 bytes",
            content.len()
        );
        let count = content.len() / 4;
        let mut hashes = Vec::with_capacity(count);
        for i in 0..count {
            let offset = i * 4;
            let bytes = [
                content[offset],
                content[offset + 1],
                content[offset + 2],
                content[offset + 3],
            ];
            hashes.push(i32::from_be_bytes(bytes));
        }
        Ok(hashes)
    }

    /// Write key hashes to a new hash index file, returning its metadata.
    async fn write(file_io: &FileIO, dir: &str, hashes: &[i32]) -> Result<IndexFileMeta> {
        let file_name = format!("index-{}-0", Uuid::new_v4());
        let path = format!("{dir}/{file_name}");

        let mut buf = Vec::with_capacity(hashes.len() * 4);
        for &h in hashes {
            buf.extend_from_slice(&h.to_be_bytes());
        }

        let file_size: i32 = buf
            .len()
            .try_into()
            .expect("hash index file size exceeds i32::MAX");
        let output = file_io.new_output(&path)?;
        output.write(bytes::Bytes::from(buf)).await?;

        Ok(IndexFileMeta {
            index_type: HASH_INDEX.to_string(),
            file_name,
            file_size,
            row_count: hashes
                .len()
                .try_into()
                .expect("hash index row count exceeds i32::MAX"),
            deletion_vectors_ranges: None,
            global_index_meta: None,
        })
    }
}

// ---------------------------------------------------------------------------
// DynamicBucketIndexMaintainer
// ---------------------------------------------------------------------------

/// Maintains the set of key hashes for a single (partition, bucket) pair.
///
/// On each write, `notify_new_record` records the key hash. At commit time,
/// `prepare_commit` writes the full hash set to a new hash index file.
///
/// Reference: [org.apache.paimon.index.DynamicBucketIndexMaintainer](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/index/DynamicBucketIndexMaintainer.java)
pub(crate) struct DynamicBucketIndexMaintainer {
    /// All key hashes in this bucket (restored + new).
    hashes: HashSet<i32>,
    /// Whether any new hashes were added since last commit.
    modified: bool,
}

impl DynamicBucketIndexMaintainer {
    /// Create a new maintainer, optionally restoring from existing hashes.
    pub fn new(restored_hashes: Vec<i32>) -> Self {
        let hashes: HashSet<i32> = restored_hashes.into_iter().collect();
        Self {
            hashes,
            modified: false,
        }
    }

    /// Record a key hash from a newly written record.
    pub fn notify_new_record(&mut self, key_hash: i32) {
        if self.hashes.insert(key_hash) {
            self.modified = true;
        }
    }

    /// Write the hash index file if modified, returning the new index file metadata.
    pub async fn prepare_commit(
        &mut self,
        file_io: &FileIO,
        index_dir: &str,
    ) -> Result<Vec<IndexFileMeta>> {
        if !self.modified {
            return Ok(Vec::new());
        }
        let hashes: Vec<i32> = self.hashes.iter().copied().collect();
        let meta = HashIndexFile::write(file_io, index_dir, &hashes).await?;
        self.modified = false;
        Ok(vec![meta])
    }
}

// ---------------------------------------------------------------------------
// PartitionIndex
// ---------------------------------------------------------------------------

/// Per-partition index that maps key hashes to bucket ids.
///
/// Also maintains per-bucket index files via embedded `DynamicBucketIndexMaintainer`s,
/// so callers only need a single `PartitionIndex` per partition.
///
/// Reference: [org.apache.paimon.index.PartitionIndex](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/index/PartitionIndex.java)
struct PartitionIndex {
    /// key hash → bucket id
    hash_to_bucket: HashMap<i32, i32>,
    /// bucket id → current row count (only non-full buckets)
    non_full_buckets: HashMap<i32, i64>,
    /// All known bucket ids
    all_buckets: HashSet<i32>,
    /// Next bucket id to allocate (avoids linear scan).
    next_bucket_id: i32,
    target_bucket_row_number: i64,
    /// Per-bucket index maintainers for writing hash index files at commit time.
    bucket_maintainers: HashMap<i32, DynamicBucketIndexMaintainer>,
}

impl PartitionIndex {
    /// Create an empty partition index.
    fn empty(target_bucket_row_number: i64) -> Self {
        Self {
            hash_to_bucket: HashMap::new(),
            non_full_buckets: HashMap::new(),
            all_buckets: HashSet::new(),
            next_bucket_id: 0,
            target_bucket_row_number,
            bucket_maintainers: HashMap::new(),
        }
    }

    /// Load partition index from existing hash index files.
    ///
    /// Reads all HASH-type index entries for this partition and reconstructs
    /// the hash→bucket mapping and bucket row counts.
    async fn load(
        file_io: &FileIO,
        index_dir: &str,
        entries: &[IndexManifestEntry],
        target_bucket_row_number: i64,
    ) -> Result<Self> {
        let mut hash_to_bucket = HashMap::new();
        let mut bucket_row_counts: HashMap<i32, i64> = HashMap::new();
        let mut bucket_hashes: HashMap<i32, Vec<i32>> = HashMap::new();

        for entry in entries {
            if entry.index_file.index_type != HASH_INDEX {
                continue;
            }
            let bucket = entry.bucket;
            let path = format!("{index_dir}/{}", entry.index_file.file_name);
            let hashes = HashIndexFile::read(file_io, &path).await?;
            let count = hashes.len() as i64;
            for &h in &hashes {
                hash_to_bucket.insert(h, bucket);
            }
            *bucket_row_counts.entry(bucket).or_insert(0) += count;
            bucket_hashes.entry(bucket).or_default().extend(hashes);
        }

        let all_buckets: HashSet<i32> = bucket_row_counts.keys().copied().collect();
        let non_full_buckets: HashMap<i32, i64> = bucket_row_counts
            .into_iter()
            .filter(|(_, count)| *count < target_bucket_row_number)
            .collect();

        let bucket_maintainers: HashMap<i32, DynamicBucketIndexMaintainer> = bucket_hashes
            .into_iter()
            .map(|(bucket, hashes)| (bucket, DynamicBucketIndexMaintainer::new(hashes)))
            .collect();

        let next_bucket_id = all_buckets.iter().copied().max().map_or(0, |m| m + 1);

        Ok(Self {
            hash_to_bucket,
            non_full_buckets,
            all_buckets,
            next_bucket_id,
            target_bucket_row_number,
            bucket_maintainers,
        })
    }

    /// Assign a bucket for the given key hash.
    ///
    /// 1. If the hash was seen before, return its existing bucket.
    /// 2. Otherwise, find a non-full bucket and assign the hash there.
    /// 3. If all buckets are full, create a new bucket.
    fn assign(&mut self, hash: i32) -> i32 {
        // 1. Already assigned
        if let Some(&bucket) = self.hash_to_bucket.get(&hash) {
            return bucket;
        }

        // 2. Find a non-full bucket
        let mut full_buckets = Vec::new();
        let mut assigned_bucket = None;
        for (&bucket, count) in &mut self.non_full_buckets {
            if *count < self.target_bucket_row_number {
                *count += 1;
                self.hash_to_bucket.insert(hash, bucket);
                assigned_bucket = Some(bucket);
                break;
            } else {
                full_buckets.push(bucket);
            }
        }
        for b in full_buckets {
            self.non_full_buckets.remove(&b);
        }
        if let Some(bucket) = assigned_bucket {
            self.bucket_maintainers
                .entry(bucket)
                .or_insert_with(|| DynamicBucketIndexMaintainer::new(vec![]))
                .notify_new_record(hash);
            return bucket;
        }

        // 3. Create a new bucket
        let new_bucket = self.next_bucket_id;
        self.next_bucket_id += 1;
        self.all_buckets.insert(new_bucket);
        self.non_full_buckets.insert(new_bucket, 1);
        self.hash_to_bucket.insert(hash, new_bucket);
        self.bucket_maintainers
            .entry(new_bucket)
            .or_insert_with(|| DynamicBucketIndexMaintainer::new(vec![]))
            .notify_new_record(hash);
        new_bucket
    }

    /// Write hash index files for all modified buckets, returning (bucket, index_files) pairs.
    async fn prepare_commit(
        &mut self,
        file_io: &FileIO,
        index_dir: &str,
    ) -> Result<Vec<(i32, Vec<IndexFileMeta>)>> {
        let mut result = Vec::new();
        let buckets: Vec<i32> = self.bucket_maintainers.keys().copied().collect();
        for bucket in buckets {
            if let Some(maintainer) = self.bucket_maintainers.get_mut(&bucket) {
                let files = maintainer.prepare_commit(file_io, index_dir).await?;
                if !files.is_empty() {
                    result.push((bucket, files));
                }
            }
        }
        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// DynamicBucketAssigner
// ---------------------------------------------------------------------------

/// Bucket assigner for dynamic bucket mode (bucket=-1) where PK includes partition fields.
///
/// Maintains a per-partition `PartitionIndex` that maps key hashes to bucket ids.
pub(crate) struct DynamicBucketAssigner {
    partition_field_indices: Vec<usize>,
    primary_key_indices: Vec<usize>,
    /// Schema fields for BinaryRow extraction.
    fields: Vec<DataField>,
    partition_indexes: HashMap<Vec<u8>, PartitionIndex>,
    target_bucket_row_number: i64,
    file_io: FileIO,
    table_location: String,
    /// Cached index manifest entries from the latest snapshot (loaded once).
    cached_index_entries: Option<Vec<IndexManifestEntry>>,
    /// Overwrite mode: skip loading existing index entries.
    is_overwrite: bool,
}

impl DynamicBucketAssigner {
    pub fn new(
        partition_field_indices: Vec<usize>,
        primary_key_indices: Vec<usize>,
        fields: Vec<DataField>,
        target_bucket_row_number: i64,
        file_io: FileIO,
        table_location: String,
        is_overwrite: bool,
    ) -> Self {
        Self {
            partition_field_indices,
            primary_key_indices,
            fields,
            partition_indexes: HashMap::new(),
            target_bucket_row_number,
            file_io,
            table_location,
            cached_index_entries: None,
            is_overwrite,
        }
    }

    /// Load all index manifest entries from the latest snapshot (cached).
    /// Overwrite mode skips loading — old index is irrelevant.
    async fn ensure_index_entries_loaded(&mut self) -> Result<()> {
        if self.cached_index_entries.is_some() {
            return Ok(());
        }
        if self.is_overwrite {
            self.cached_index_entries = Some(Vec::new());
            return Ok(());
        }
        let snapshot_manager =
            SnapshotManager::new(self.file_io.clone(), self.table_location.clone());
        let latest_snapshot = snapshot_manager.get_latest_snapshot().await?;

        let entries = if let Some(snapshot) = latest_snapshot {
            if let Some(index_manifest_name) = snapshot.index_manifest() {
                let manifest_dir = snapshot_manager.manifest_dir();
                let index_manifest_path = format!("{manifest_dir}/{index_manifest_name}");
                IndexManifest::read(&self.file_io, &index_manifest_path).await?
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };
        self.cached_index_entries = Some(entries);
        Ok(())
    }

    /// Load partition index from cached index manifest entries.
    async fn load_partition_index(&self, partition_bytes: &[u8]) -> Result<PartitionIndex> {
        let entries = self.cached_index_entries.as_deref().unwrap_or(&[]);
        let partition_entries: Vec<_> = entries
            .iter()
            .filter(|e| e.partition == partition_bytes && e.index_file.index_type == HASH_INDEX)
            .cloned()
            .collect();

        if !partition_entries.is_empty() {
            let index_dir = format!("{}/index", self.table_location);
            return PartitionIndex::load(
                &self.file_io,
                &index_dir,
                &partition_entries,
                self.target_bucket_row_number,
            )
            .await;
        }

        Ok(PartitionIndex::empty(self.target_bucket_row_number))
    }
}

impl BucketAssigner for DynamicBucketAssigner {
    async fn assign_batch(
        &mut self,
        batch: &RecordBatch,
        _fields: &[DataField],
    ) -> Result<BatchAssignOutput> {
        // Batch-compute partition bytes
        let partition_bytes_vec = if self.partition_field_indices.is_empty() {
            vec![EMPTY_SERIALIZED_ROW.clone(); batch.num_rows()]
        } else {
            batch_to_serialized_bytes(batch, &self.partition_field_indices, &self.fields)?
        };

        // Load indexes for unseen partitions
        let mut unseen = Vec::new();
        let mut seen_set = HashSet::new();
        for pb in &partition_bytes_vec {
            if !self.partition_indexes.contains_key(pb) && seen_set.insert(pb.clone()) {
                unseen.push(pb.clone());
            }
        }
        if !unseen.is_empty() {
            self.ensure_index_entries_loaded().await?;
        }
        for partition_bytes in unseen {
            let index = self.load_partition_index(&partition_bytes).await?;
            self.partition_indexes.insert(partition_bytes, index);
        }

        // Batch-compute hash codes and assign buckets
        let hash_codes = batch_hash_codes(batch, &self.primary_key_indices, &self.fields)?;
        let mut buckets = Vec::with_capacity(batch.num_rows());
        for (row_idx, pb) in partition_bytes_vec.iter().enumerate() {
            let partition_index = self.partition_indexes.get_mut(pb).unwrap();
            buckets.push(partition_index.assign(hash_codes[row_idx]));
        }

        Ok(BatchAssignOutput {
            partition_bytes: partition_bytes_vec,
            buckets,
            deletes: Vec::new(),
            skips: Vec::new(),
        })
    }

    async fn prepare_commit_index(
        &mut self,
        file_io: &FileIO,
        index_dir: &str,
    ) -> Result<HashMap<PartitionBucketKey, Vec<IndexFileMeta>>> {
        let mut result = HashMap::new();
        let partition_keys: Vec<Vec<u8>> = self.partition_indexes.keys().cloned().collect();
        for partition_bytes in partition_keys {
            if let Some(partition_index) = self.partition_indexes.get_mut(&partition_bytes) {
                let bucket_files = partition_index.prepare_commit(file_io, index_dir).await?;
                for (bucket, idx_files) in bucket_files {
                    result.insert((partition_bytes.clone(), bucket), idx_files);
                }
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- DynamicBucketIndexMaintainer tests --

    #[tokio::test]
    async fn test_maintainer_write_on_modify() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = format!("file://{}", tmp.path().display());
        let file_io = FileIO::from_url(&dir).unwrap().build().unwrap();

        let mut m = DynamicBucketIndexMaintainer::new(vec![]);
        // No modification → empty
        let files = m.prepare_commit(&file_io, &dir).await.unwrap();
        assert!(files.is_empty());

        // Add hashes
        m.notify_new_record(1);
        m.notify_new_record(2);
        m.notify_new_record(1); // duplicate, no effect
        let files = m.prepare_commit(&file_io, &dir).await.unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].index_type, HASH_INDEX);
        assert_eq!(files[0].row_count, 2);

        // No new modification → empty again
        let files = m.prepare_commit(&file_io, &dir).await.unwrap();
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn test_maintainer_with_restored() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = format!("file://{}", tmp.path().display());
        let file_io = FileIO::from_url(&dir).unwrap().build().unwrap();

        let mut m = DynamicBucketIndexMaintainer::new(vec![10, 20]);
        // Restored hashes don't count as modified
        let files = m.prepare_commit(&file_io, &dir).await.unwrap();
        assert!(files.is_empty());

        // Adding a new hash triggers write (includes restored + new)
        m.notify_new_record(30);
        let files = m.prepare_commit(&file_io, &dir).await.unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].row_count, 3);
    }

    // -- PartitionIndex tests --

    #[test]
    fn test_assign_new_keys() {
        let mut index = PartitionIndex::empty(3);
        assert_eq!(index.assign(100), 0);
        assert_eq!(index.assign(200), 0);
        assert_eq!(index.assign(300), 0);
        // Bucket 0 is full, next key goes to bucket 1
        assert_eq!(index.assign(400), 1);
    }

    #[test]
    fn test_assign_existing_key() {
        let mut index = PartitionIndex::empty(10);
        assert_eq!(index.assign(42), 0);
        // Same hash returns same bucket
        assert_eq!(index.assign(42), 0);
    }

    #[test]
    fn test_multiple_buckets() {
        let mut index = PartitionIndex::empty(2);
        assert_eq!(index.assign(1), 0);
        assert_eq!(index.assign(2), 0);
        // Bucket 0 full
        assert_eq!(index.assign(3), 1);
        assert_eq!(index.assign(4), 1);
        // Bucket 1 full
        assert_eq!(index.assign(5), 2);
    }

    // -- HashIndexFile tests --

    #[tokio::test]
    async fn test_hash_index_roundtrip() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = format!("file://{}", tmp.path().display());
        let file_io = FileIO::from_url(&dir).unwrap().build().unwrap();

        let hashes = vec![42, -1, 0, i32::MAX, i32::MIN];
        let meta = HashIndexFile::write(&file_io, &dir, &hashes).await.unwrap();

        assert_eq!(meta.index_type, HASH_INDEX);
        assert_eq!(meta.row_count, 5);
        assert_eq!(meta.file_size, 20);

        let path = format!("{dir}/{}", meta.file_name);
        let read_back = HashIndexFile::read(&file_io, &path).await.unwrap();
        assert_eq!(read_back, hashes);
    }

    #[tokio::test]
    async fn test_hash_index_empty() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = format!("file://{}", tmp.path().display());
        let file_io = FileIO::from_url(&dir).unwrap().build().unwrap();

        let meta = HashIndexFile::write(&file_io, &dir, &[]).await.unwrap();
        assert_eq!(meta.row_count, 0);
        assert_eq!(meta.file_size, 0);

        let path = format!("{dir}/{}", meta.file_name);
        let read_back = HashIndexFile::read(&file_io, &path).await.unwrap();
        assert!(read_back.is_empty());
    }
}
