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
use crate::spec::MAX_DYNAMIC_BUCKETS;
use crate::spec::{
    batch_hash_codes, batch_to_serialized_bytes, bucket_path_under, BinaryRow, CoreOptions,
    DataField, IndexFileMeta, IndexManifest, IndexManifestEntry, PartitionComputer,
    EMPTY_SERIALIZED_ROW,
};
use crate::table::bucket_assigner::{BatchAssignOutput, BucketAssigner, PartitionBucketKey};
use crate::table::data_file_path_factory::{DataFilePath, DataFilePathFactory};
use crate::table::index_file_path::IndexFileLocation;
use crate::table::partition_filter::PartitionFilter;
use crate::table::{Snapshot, Table, TableScan};
use crate::Result;
use arrow_array::RecordBatch;
use rand::seq::SliceRandom;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Hash index file
// ---------------------------------------------------------------------------

/// Index type identifier for hash index files, matching Java's `HashIndexFile.HASH_INDEX`.
const HASH_INDEX: &str = "HASH";

/// Read/write hash index files.
///
/// A hash index file is a flat binary file containing `i32` values in big-endian byte order.
/// Each value is the hash code of a primary key that belongs to the associated bucket.
struct HashIndexFile;

impl HashIndexFile {
    /// Read all key hashes from a hash index file.
    async fn read(file_io: &FileIO, path: &str) -> Result<Vec<i32>> {
        let input = file_io.new_input(path)?;
        let content = input.read().await?;
        if content.len() % 4 != 0 {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Corrupt HASH index {path}: expected a multiple of 4 bytes, got {}",
                    content.len()
                ),
                source: None,
            });
        }
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
    async fn write_at(
        file_io: &FileIO,
        file_name: String,
        location: DataFilePath,
        hashes: &[i32],
    ) -> Result<IndexFileMeta> {
        file_io.mkdirs(location.parent()).await?;
        let mut buf = Vec::with_capacity(hashes.len() * 4);
        for &h in hashes {
            buf.extend_from_slice(&h.to_be_bytes());
        }

        let file_size: i64 = buf
            .len()
            .try_into()
            .expect("hash index file size exceeds i64::MAX");
        let output = file_io.new_output(&location.path)?;
        if let Err(error) = output.write(bytes::Bytes::from(buf)).await {
            let _ = file_io.delete_file(&location.path).await;
            return Err(error);
        }

        Ok(IndexFileMeta {
            index_type: HASH_INDEX.to_string(),
            file_name,
            file_size,
            row_count: hashes
                .len()
                .try_into()
                .expect("hash index row count exceeds i32::MAX"),
            deletion_vectors_ranges: None,
            external_path: location.external_path,
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
struct DynamicBucketIndexMaintainer {
    /// All key hashes in this bucket (restored + new).
    hashes: HashSet<i32>,
    /// Whether any new hashes were added since last commit.
    modified: bool,
    /// Java caches a per-bucket index path factory across checkpoints.
    paths: Option<DataFilePathFactory>,
}

impl DynamicBucketIndexMaintainer {
    /// Create a new maintainer, optionally restoring from existing hashes.
    pub fn new(restored_hashes: Vec<i32>) -> Self {
        let hashes: HashSet<i32> = restored_hashes.into_iter().collect();
        Self {
            hashes,
            modified: false,
            paths: None,
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
        layout: &HashIndexLayout<'_>,
        bucket: i32,
        options: &HashMap<String, String>,
    ) -> Result<Vec<IndexFileMeta>> {
        if !self.modified {
            return Ok(Vec::new());
        }
        let hashes: Vec<i32> = self.hashes.iter().copied().collect();
        let file_name = format!("index-{}-0", Uuid::new_v4());
        let location = if layout.index_file_in_data_file_dir {
            if self.paths.is_none() {
                self.paths = Some(DataFilePathFactory::new(
                    layout.table_path,
                    layout.partition_path,
                    bucket,
                    options,
                )?);
            }
            self.paths.as_ref().unwrap().new_path(&file_name)?
        } else {
            let external_path =
                super::external_path::new_index_external_path(options, false, "", &file_name)?;
            DataFilePath {
                path: layout.resolve(bucket, &file_name, external_path.as_deref()),
                external_path,
            }
        };
        let meta = HashIndexFile::write_at(file_io, file_name, location, &hashes).await?;
        self.modified = false;
        Ok(vec![meta])
    }
}

// ---------------------------------------------------------------------------
// PartitionIndex
// ---------------------------------------------------------------------------

/// Where one partition's hash index files live.
///
/// A hash index is an index file, so it sits beside its bucket's data files when
/// the table keeps index files in the data-file directory, and under the table
/// `index/` directory otherwise. Reads and writes resolve through the same value
/// so a file written here is found again.
struct HashIndexLayout<'a> {
    table_path: &'a str,
    /// Partition directory, already terminated by `/`, or empty when unpartitioned.
    partition_path: &'a str,
    data_file_path_directory: Option<&'a str>,
    index_file_in_data_file_dir: bool,
}

impl HashIndexLayout<'_> {
    /// This layout as the shared resolver's bucket-local mode. The bucket
    /// directory is passed in so the resolver can borrow it.
    fn location<'b>(&'b self, bucket_path: &'b str) -> IndexFileLocation<'b> {
        IndexFileLocation::BucketLocal {
            table_path: self.table_path,
            bucket_path,
            index_file_in_data_file_dir: self.index_file_in_data_file_dir,
        }
    }

    fn bucket_path(&self, bucket: i32) -> String {
        let data_root = crate::spec::data_file_path(self.table_path, self.data_file_path_directory);
        bucket_path_under(&data_root, self.partition_path, bucket)
    }

    /// The default directory for a hash index without an external location.
    #[cfg(test)]
    fn directory(&self, bucket: i32) -> String {
        let bucket_path = self.bucket_path(bucket);
        self.location(&bucket_path).directory()
    }

    /// The path of an existing hash index file recorded for `bucket`.
    fn resolve(&self, bucket: i32, file_name: &str, external_path: Option<&str>) -> String {
        let bucket_path = self.bucket_path(bucket);
        self.location(&bucket_path)
            .resolve(file_name, external_path)
    }
}

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
    /// Next unused bucket id to try, including holes in a restored index.
    next_bucket_id: i32,
    bucket_ids: Vec<i32>,
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
            bucket_ids: Vec::new(),
            target_bucket_row_number,
            bucket_maintainers: HashMap::new(),
        }
    }

    /// Merge buckets restored or modified by direct writers. The base snapshot
    /// is shared, so an already-notified maintainer retains its newer state.
    fn merge(&mut self, other: Self) -> Result<()> {
        for (hash, bucket) in other.hash_to_bucket {
            self.record_mapping(hash, bucket)?;
        }
        self.bucket_maintainers.extend(other.bucket_maintainers);
        Ok(())
    }

    fn record_mapping(&mut self, hash: i32, bucket: i32) -> Result<()> {
        if let Some(&previous) = self.hash_to_bucket.get(&hash) {
            if previous != bucket {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Precomputed hash {hash} belongs to bucket {previous}, not {bucket}"
                    ),
                    source: None,
                });
            }
            return Ok(());
        }
        self.hash_to_bucket.insert(hash, bucket);
        if self.all_buckets.insert(bucket) {
            self.bucket_ids.push(bucket);
            self.non_full_buckets.insert(bucket, 0);
        }
        if let Some(count) = self.non_full_buckets.get_mut(&bucket) {
            *count += 1;
            if *count >= self.target_bucket_row_number {
                self.non_full_buckets.remove(&bucket);
            }
        }
        Ok(())
    }

    fn notify_precomputed(&mut self, bucket: i32, hashes: &[i32]) -> Result<()> {
        let notified: HashSet<i32> = hashes.iter().copied().collect();
        // Validate the entire group before changing the assignment or maintainer.
        for &hash in hashes {
            if let Some(&previous) = self.hash_to_bucket.get(&hash) {
                if previous != bucket {
                    return Err(crate::Error::DataInvalid {
                        message: format!(
                            "Precomputed hash {hash} belongs to bucket {previous}, not {bucket}"
                        ),
                        source: None,
                    });
                }
            }
        }
        for hash in notified {
            self.record_mapping(hash, bucket)?;
            self.bucket_maintainers
                .entry(bucket)
                .or_insert_with(|| DynamicBucketIndexMaintainer::new(vec![]))
                .notify_new_record(hash);
        }
        Ok(())
    }

    /// Load partition index from existing hash index files.
    ///
    /// Reads all HASH-type index entries for this partition and reconstructs
    /// the hash→bucket mapping and bucket row counts.
    async fn load(
        file_io: &FileIO,
        layout: &HashIndexLayout<'_>,
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
            if !(0..MAX_DYNAMIC_BUCKETS).contains(&bucket) {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Dynamic bucket id must be between 0 and {}, but was {bucket}",
                        MAX_DYNAMIC_BUCKETS - 1
                    ),
                    source: None,
                });
            }
            if bucket_hashes.contains_key(&bucket) {
                return Err(crate::Error::DataInvalid {
                    message: format!("Multiple HASH indexes for dynamic bucket {bucket}"),
                    source: None,
                });
            }
            let path = layout.resolve(
                bucket,
                &entry.index_file.file_name,
                entry.index_file.external_path.as_deref(),
            );
            let hashes = HashIndexFile::read(file_io, &path).await?;
            let count = hashes.len() as i64;
            if count != entry.index_file.row_count {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Corrupt HASH index {path}: expected {} hashes, got {count}",
                        entry.index_file.row_count
                    ),
                    source: None,
                });
            }
            for &h in &hashes {
                if let Some(previous) = hash_to_bucket.insert(h, bucket) {
                    if previous != bucket {
                        return Err(crate::Error::DataInvalid {
                            message: format!("HASH index assigns hash {h} to both buckets {previous} and {bucket}"), source: None,
                        });
                    }
                }
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

        let bucket_ids = all_buckets.iter().copied().collect();

        Ok(Self {
            hash_to_bucket,
            non_full_buckets,
            all_buckets,
            next_bucket_id: 0,
            bucket_ids,
            target_bucket_row_number,
            bucket_maintainers,
        })
    }

    /// Assign a bucket for the given key hash.
    ///
    /// 1. If the hash was seen before, return its existing bucket.
    /// 2. Otherwise, find a non-full bucket and assign the hash there.
    /// 3. If all buckets are full, create a new bucket.
    fn assign(&mut self, hash: i32, max_buckets: i32) -> Result<i32> {
        // 1. Already assigned
        if let Some(&bucket) = self.hash_to_bucket.get(&hash) {
            return Ok(bucket);
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
            return Ok(bucket);
        }

        // Java PartitionIndex first allocates an unused id in range. A configured
        // maximum is a soft row-count limit: once reached, reuse an existing bucket.
        let limit = if max_buckets == -1 {
            MAX_DYNAMIC_BUCKETS
        } else {
            max_buckets
        };
        while self.next_bucket_id < limit && self.all_buckets.contains(&self.next_bucket_id) {
            self.next_bucket_id += 1;
        }
        let bucket = if self.next_bucket_id < limit {
            let bucket = self.next_bucket_id;
            self.next_bucket_id += 1;
            self.all_buckets.insert(bucket);
            self.bucket_ids.push(bucket);
            self.non_full_buckets.insert(bucket, 1);
            bucket
        } else if max_buckets == -1 {
            return Err(crate::Error::DataInvalid {
                message: "No dynamic bucket id remains below Java Short.MAX_VALUE. Increase dynamic-bucket.target-row-num.".to_string(), source: None,
            });
        } else {
            *self
                .bucket_ids
                .choose(&mut rand::thread_rng())
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: "No dynamic bucket is available".to_string(),
                    source: None,
                })?
        };
        self.hash_to_bucket.insert(hash, bucket);
        self.bucket_maintainers
            .entry(bucket)
            .or_insert_with(|| DynamicBucketIndexMaintainer::new(vec![]))
            .notify_new_record(hash);
        Ok(bucket)
    }

    /// Write hash index files for all modified buckets, returning (bucket, index_files) pairs.
    async fn prepare_commit(
        &mut self,
        file_io: &FileIO,
        layout: &HashIndexLayout<'_>,
        options: &HashMap<String, String>,
        created_paths: &mut Vec<String>,
    ) -> Result<Vec<(i32, Vec<IndexFileMeta>)>> {
        let mut result = Vec::new();
        let buckets: Vec<i32> = self.bucket_maintainers.keys().copied().collect();
        for bucket in buckets {
            if let Some(maintainer) = self.bucket_maintainers.get_mut(&bucket) {
                let files = maintainer
                    .prepare_commit(file_io, layout, bucket, options)
                    .await?;
                for file in &files {
                    created_paths.push(layout.resolve(
                        bucket,
                        &file.file_name,
                        file.external_path.as_deref(),
                    ));
                }
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
    /// Direct writers restore only the supplied buckets. Assignment upgrades
    /// these partial indexes to a full partition index on first use.
    precomputed_indexes: HashMap<Vec<u8>, PartitionIndex>,
    target_bucket_row_number: i64,
    table: Table,
    max_buckets: i32,
    snapshot: Option<Snapshot>,
    snapshot_pinned: bool,
    /// Cached index manifest entries from the latest snapshot (loaded once).
    cached_index_entries: Option<Vec<IndexManifestEntry>>,
    /// Overwrite mode: skip loading existing index entries.
    is_overwrite: bool,
    /// Builds the partition directory of a bucket, so a hash index kept in the
    /// data-file directory is written and read in the same place. Yields an empty
    /// path for an unpartitioned table.
    partition_computer: PartitionComputer,
    /// Whether the table stores index files in the data-file (bucket) directory.
    index_file_in_data_file_dir: bool,
}

impl DynamicBucketAssigner {
    pub fn new(
        table: Table,
        partition_field_indices: Vec<usize>,
        primary_key_indices: Vec<usize>,
        is_overwrite: bool,
        partition_computer: PartitionComputer,
    ) -> Result<Self> {
        let options = CoreOptions::new(table.schema().options());
        if table.schema().options().contains_key("bucket-key") {
            return Err(crate::Error::ConfigInvalid {
                message: "Cannot define 'bucket-key' in dynamic bucket mode".to_string(),
            });
        }
        Ok(Self {
            partition_field_indices,
            primary_key_indices,
            fields: table.schema().fields().to_vec(),
            partition_indexes: HashMap::new(),
            precomputed_indexes: HashMap::new(),
            target_bucket_row_number: options.dynamic_bucket_target_row_num(),
            max_buckets: options.dynamic_bucket_max_buckets()?,
            snapshot: None,
            snapshot_pinned: false,
            cached_index_entries: None,
            is_overwrite,
            partition_computer,
            index_file_in_data_file_dir: options.index_file_in_data_file_dir(),
            table,
        })
    }

    pub fn set_overwrite(&mut self, is_overwrite: bool) {
        self.is_overwrite = is_overwrite;
    }

    /// Called before writes so all worker-side restoration uses the driver's base.
    pub fn configure_index(&mut self, ignore_existing: bool, snapshot: Option<Snapshot>) {
        self.is_overwrite = ignore_existing;
        self.snapshot = snapshot;
        self.snapshot_pinned = true;
        self.cached_index_entries = None;
        self.partition_indexes.clear();
        self.precomputed_indexes.clear();
    }

    /// Java's DynamicBucketIndexMaintainer sees keys after bucket assignment.
    /// Keep that same full-file replacement lifecycle for precomputed groups.
    pub async fn notify_precomputed_batch(
        &mut self,
        batch: &RecordBatch,
        partition: &[u8],
        bucket: i32,
        key_hashes: Option<&[i32]>,
    ) -> Result<()> {
        let computed;
        let hashes = match key_hashes {
            Some(hashes) => hashes,
            None => {
                computed = batch_hash_codes(batch, &self.primary_key_indices, &self.fields)?;
                &computed
            }
        };
        if let Some(index) = self.partition_indexes.get_mut(partition) {
            return index.notify_precomputed(bucket, hashes);
        }
        let loaded = self
            .precomputed_indexes
            .get(partition)
            .is_some_and(|index| index.bucket_maintainers.contains_key(&bucket));
        if !loaded {
            self.ensure_index_entries_loaded().await?;
            let entries: Vec<_> = self
                .cached_index_entries
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .filter(|entry| {
                    entry.partition == partition
                        && entry.bucket == bucket
                        && entry.index_file.index_type == HASH_INDEX
                })
                .cloned()
                .collect();
            let mut restored = self.load_hash_indexes(partition, &entries).await?;
            // Remember an empty bucket too, so subsequent batches do not restore
            // it repeatedly. It remains unmodified until a key is notified.
            restored
                .bucket_maintainers
                .entry(bucket)
                .or_insert_with(|| DynamicBucketIndexMaintainer::new(vec![]));
            self.precomputed_indexes
                .entry(partition.to_vec())
                .or_insert_with(|| PartitionIndex::empty(self.target_bucket_row_number))
                .merge(restored)?;
        }
        self.precomputed_indexes
            .get_mut(partition)
            .unwrap()
            .notify_precomputed(bucket, hashes)
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
        let snapshot_manager = self.table.snapshot_manager();
        let latest_snapshot = if self.snapshot_pinned {
            self.snapshot.clone()
        } else {
            snapshot_manager.get_latest_snapshot().await?
        };

        let entries = if let Some(snapshot) = &latest_snapshot {
            if let Some(index_manifest_name) = snapshot.index_manifest() {
                let manifest_dir = snapshot_manager.manifest_dir();
                let index_manifest_path = format!("{manifest_dir}/{index_manifest_name}");
                IndexManifest::read(self.table.file_io(), &index_manifest_path).await?
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };
        self.snapshot = latest_snapshot;
        self.cached_index_entries = Some(entries);
        Ok(())
    }

    /// The partition directory of a bucket, terminated by `/`, or empty when the
    /// table is unpartitioned.
    fn partition_path(&self, partition_bytes: &[u8]) -> Result<String> {
        let partition_row = BinaryRow::from_serialized_bytes(partition_bytes)?;
        self.partition_computer
            .generate_partition_path(&partition_row)
    }

    /// Load partition index from cached index manifest entries.
    async fn load_partition_index(&self, partition_bytes: &[u8]) -> Result<PartitionIndex> {
        let entries = self.cached_index_entries.as_deref().unwrap_or(&[]);
        let partition_entries: Vec<_> = entries
            .iter()
            .filter(|e| e.partition == partition_bytes && e.index_file.index_type == HASH_INDEX)
            .cloned()
            .collect();

        self.validate_data_buckets(partition_bytes, &partition_entries)
            .await?;
        self.load_hash_indexes(partition_bytes, &partition_entries)
            .await
    }

    async fn load_hash_indexes(
        &self,
        partition_bytes: &[u8],
        partition_entries: &[IndexManifestEntry],
    ) -> Result<PartitionIndex> {
        if !partition_entries.is_empty() {
            let partition_path = self.partition_path(partition_bytes)?;
            let options = CoreOptions::new(self.table.schema().options());
            let layout = HashIndexLayout {
                data_file_path_directory: options.data_file_path_directory(),
                table_path: self.table.location().trim_end_matches('/'),
                partition_path: &partition_path,
                index_file_in_data_file_dir: self.index_file_in_data_file_dir,
            };
            return PartitionIndex::load(
                self.table.file_io(),
                &layout,
                partition_entries,
                self.target_bucket_row_number,
            )
            .await;
        }

        Ok(PartitionIndex::empty(self.target_bucket_row_number))
    }

    /// Use the same snapshot as index restoration. Treating existing data without
    /// its HASH index as a new partition can assign an old key to a different bucket.
    async fn validate_data_buckets(
        &self,
        partition: &[u8],
        indexes: &[IndexManifestEntry],
    ) -> Result<()> {
        if self.is_overwrite {
            return Ok(());
        }
        let Some(snapshot) = &self.snapshot else {
            return Ok(());
        };
        let fields = self.table.schema().partition_fields();
        let filter = if fields.is_empty() {
            None
        } else {
            Some(PartitionFilter::from_partition_set(
                HashSet::from([partition.to_vec()]),
                &fields,
            )?)
        };
        let scan =
            TableScan::new(&self.table, filter, vec![], None, None, None).with_scan_all_files();
        let indexed: HashSet<i32> = indexes.iter().map(|entry| entry.bucket).collect();
        for entry in scan.plan_manifest_entries(snapshot).await? {
            if !indexed.contains(&entry.bucket()) {
                return Err(crate::Error::DataInvalid {
                    message: format!("Dynamic-bucket partition has data files but no complete HASH index for bucket {}. Rewrite the partition before incremental writes.", entry.bucket()),
                    source: None,
                });
            }
        }
        Ok(())
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
            let mut index = self.load_partition_index(&partition_bytes).await?;
            if let Some(notified) = self.precomputed_indexes.remove(&partition_bytes) {
                index.merge(notified)?;
            }
            self.partition_indexes.insert(partition_bytes, index);
        }

        // Batch-compute hash codes and assign buckets
        let hash_codes = batch_hash_codes(batch, &self.primary_key_indices, &self.fields)?;
        let mut buckets = Vec::with_capacity(batch.num_rows());
        for (row_idx, pb) in partition_bytes_vec.iter().enumerate() {
            let partition_index = self.partition_indexes.get_mut(pb).unwrap();
            buckets.push(partition_index.assign(hash_codes[row_idx], self.max_buckets)?);
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
    ) -> Result<HashMap<PartitionBucketKey, Vec<IndexFileMeta>>> {
        let mut created_paths = Vec::new();
        let result = async {
            let mut result = HashMap::new();
            let table_path = self.table.location().trim_end_matches('/').to_string();
            let index_file_in_data_file_dir = self.index_file_in_data_file_dir;
            let partition_keys: Vec<Vec<u8>> = self
                .partition_indexes
                .keys()
                .chain(self.precomputed_indexes.keys())
                .cloned()
                .collect();
            let mut partition_paths = Vec::with_capacity(partition_keys.len());
            for partition_bytes in &partition_keys {
                partition_paths.push(self.partition_path(partition_bytes)?);
            }
            for (partition_bytes, partition_path) in partition_keys.into_iter().zip(partition_paths)
            {
                let options = CoreOptions::new(self.table.schema().options());
                let layout = HashIndexLayout {
                    data_file_path_directory: options.data_file_path_directory(),
                    table_path: &table_path,
                    partition_path: &partition_path,
                    index_file_in_data_file_dir,
                };
                if let Some(partition_index) = self
                    .partition_indexes
                    .get_mut(&partition_bytes)
                    .or_else(|| self.precomputed_indexes.get_mut(&partition_bytes))
                {
                    let bucket_files = partition_index
                        .prepare_commit(
                            file_io,
                            &layout,
                            self.table.schema().options(),
                            &mut created_paths,
                        )
                        .await?;
                    for (bucket, idx_files) in bucket_files {
                        result.insert((partition_bytes.clone(), bucket), idx_files);
                    }
                }
            }
            Ok(result)
        }
        .await;
        if result.is_err() {
            for path in created_paths {
                let _ = file_io.delete_file(&path).await;
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table_layout(table_path: &str) -> HashIndexLayout<'_> {
        HashIndexLayout {
            table_path,
            partition_path: "",
            data_file_path_directory: None,
            index_file_in_data_file_dir: false,
        }
    }

    async fn write_hash_index(
        file_io: &FileIO,
        dir: &str,
        hashes: &[i32],
    ) -> Result<IndexFileMeta> {
        let file_name = format!("index-{}-0", Uuid::new_v4());
        let location = DataFilePath {
            path: format!("{dir}/{file_name}"),
            external_path: None,
        };
        HashIndexFile::write_at(file_io, file_name, location, hashes).await
    }

    // -- DynamicBucketIndexMaintainer tests --

    #[tokio::test]
    async fn test_maintainer_write_on_modify() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = format!("file://{}", tmp.path().display());
        let file_io = FileIO::from_url(&dir).unwrap().build().unwrap();

        let mut m = DynamicBucketIndexMaintainer::new(vec![]);
        // No modification → empty
        let files = m
            .prepare_commit(&file_io, &table_layout(&dir), 0, &HashMap::new())
            .await
            .unwrap();
        assert!(files.is_empty());

        // Add hashes
        m.notify_new_record(1);
        m.notify_new_record(2);
        m.notify_new_record(1); // duplicate, no effect
        let files = m
            .prepare_commit(&file_io, &table_layout(&dir), 0, &HashMap::new())
            .await
            .unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].index_type, HASH_INDEX);
        assert_eq!(files[0].row_count, 2);

        // No new modification → empty again
        let files = m
            .prepare_commit(&file_io, &table_layout(&dir), 0, &HashMap::new())
            .await
            .unwrap();
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn test_maintainer_with_restored() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = format!("file://{}", tmp.path().display());
        let file_io = FileIO::from_url(&dir).unwrap().build().unwrap();

        let mut m = DynamicBucketIndexMaintainer::new(vec![10, 20]);
        // Restored hashes don't count as modified
        let files = m
            .prepare_commit(&file_io, &table_layout(&dir), 0, &HashMap::new())
            .await
            .unwrap();
        assert!(files.is_empty());

        // Adding a new hash triggers write (includes restored + new)
        m.notify_new_record(30);
        let files = m
            .prepare_commit(&file_io, &table_layout(&dir), 0, &HashMap::new())
            .await
            .unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].row_count, 3);
    }

    #[tokio::test]
    async fn test_hash_external_round_robin_survives_checkpoints() {
        let tmp = tempfile::tempdir().unwrap();
        let root = format!("file://{}", tmp.path().display());
        let io = FileIO::from_url(&root).unwrap().build().unwrap();
        let options = HashMap::from([
            (
                "data-file.external-paths".to_string(),
                format!("{root}/one,{root}/two"),
            ),
            (
                "data-file.external-paths.strategy".to_string(),
                "round-robin".to_string(),
            ),
            ("data-file.path-directory".to_string(), "data".to_string()),
        ]);
        let layout = HashIndexLayout {
            table_path: &root,
            partition_path: "p=a/",
            data_file_path_directory: Some("data"),
            index_file_in_data_file_dir: true,
        };
        let mut maintainer = DynamicBucketIndexMaintainer::new(vec![]);
        let mut locations = Vec::new();
        for hash in [1, 2, 3] {
            maintainer.notify_new_record(hash);
            let files = maintainer
                .prepare_commit(&io, &layout, 0, &options)
                .await
                .unwrap();
            let path = files[0].external_path.as_ref().unwrap();
            assert!(path.contains("/data/p=a/bucket-0/index-"));
            assert_eq!(
                HashIndexFile::read(&io, path).await.unwrap().len(),
                hash as usize
            );
            locations.push(path.rsplit_once("/data/").unwrap().0.to_string());
        }
        assert_ne!(locations[0], locations[1]);
        assert_eq!(locations[0], locations[2]);
    }

    // -- PartitionIndex tests --

    #[test]
    fn test_assign_new_keys() {
        let mut index = PartitionIndex::empty(3);
        assert_eq!(index.assign(100, -1).unwrap(), 0);
        assert_eq!(index.assign(200, -1).unwrap(), 0);
        assert_eq!(index.assign(300, -1).unwrap(), 0);
        // Bucket 0 is full, next key goes to bucket 1
        assert_eq!(index.assign(400, -1).unwrap(), 1);
    }

    #[test]
    fn test_assign_existing_key() {
        let mut index = PartitionIndex::empty(10);
        assert_eq!(index.assign(42, -1).unwrap(), 0);
        // Same hash returns same bucket
        assert_eq!(index.assign(42, -1).unwrap(), 0);
    }

    #[test]
    fn test_multiple_buckets() {
        let mut index = PartitionIndex::empty(2);
        assert_eq!(index.assign(1, -1).unwrap(), 0);
        assert_eq!(index.assign(2, -1).unwrap(), 0);
        // Bucket 0 full
        assert_eq!(index.assign(3, -1).unwrap(), 1);
        assert_eq!(index.assign(4, -1).unwrap(), 1);
        // Bucket 1 full
        assert_eq!(index.assign(5, -1).unwrap(), 2);
    }

    #[test]
    fn test_dynamic_bucket_short_limit_and_explicit_reuse() {
        let mut index = PartitionIndex::empty(1);
        for bucket in 0..MAX_DYNAMIC_BUCKETS {
            assert_eq!(index.assign(bucket, -1).unwrap(), bucket);
        }
        assert!(index
            .assign(MAX_DYNAMIC_BUCKETS, -1)
            .unwrap_err()
            .to_string()
            .contains("Short.MAX_VALUE"));
        // Existing keys remain usable at the upper bound.
        assert_eq!(
            index.assign(MAX_DYNAMIC_BUCKETS - 1, -1).unwrap(),
            MAX_DYNAMIC_BUCKETS - 1
        );
        let bucket = index
            .assign(MAX_DYNAMIC_BUCKETS, MAX_DYNAMIC_BUCKETS)
            .unwrap();
        assert!((0..MAX_DYNAMIC_BUCKETS).contains(&bucket));
        assert_eq!(
            index
                .assign(MAX_DYNAMIC_BUCKETS, MAX_DYNAMIC_BUCKETS)
                .unwrap(),
            bucket
        );
    }

    #[tokio::test]
    async fn test_dynamic_bucket_restore_reuses_unused_ids() {
        let tmp = tempfile::tempdir().unwrap();
        let root = format!("file://{}", tmp.path().display());
        let io = FileIO::from_url(&root).unwrap().build().unwrap();
        let meta = write_hash_index(&io, &format!("{root}/index"), &[42])
            .await
            .unwrap();
        let entries = vec![IndexManifestEntry {
            version: 1,
            kind: crate::spec::FileKind::Add,
            partition: EMPTY_SERIALIZED_ROW.to_vec(),
            bucket: 2,
            index_file: meta,
        }];
        let layout = HashIndexLayout {
            table_path: &root,
            partition_path: "",
            data_file_path_directory: None,
            index_file_in_data_file_dir: false,
        };
        let mut index = PartitionIndex::load(&io, &layout, &entries, 1)
            .await
            .unwrap();
        assert_eq!(index.assign(42, 3).unwrap(), 2);
        assert_eq!(index.assign(43, 3).unwrap(), 0);
        assert_eq!(index.assign(44, 3).unwrap(), 1);
        assert!((0..3).contains(&index.assign(45, 3).unwrap()));
    }

    #[tokio::test]
    async fn test_dynamic_bucket_rejects_invalid_restored_indexes() {
        let tmp = tempfile::tempdir().unwrap();
        let root = format!("file://{}", tmp.path().display());
        let io = FileIO::from_url(&root).unwrap().build().unwrap();
        let meta = write_hash_index(&io, &format!("{root}/index"), &[42])
            .await
            .unwrap();
        let entry = IndexManifestEntry {
            version: 1,
            kind: crate::spec::FileKind::Add,
            partition: EMPTY_SERIALIZED_ROW.to_vec(),
            bucket: 0,
            index_file: meta,
        };
        let layout = HashIndexLayout {
            table_path: &root,
            partition_path: "",
            data_file_path_directory: None,
            index_file_in_data_file_dir: false,
        };
        for bucket in [-1, MAX_DYNAMIC_BUCKETS] {
            let mut invalid = entry.clone();
            invalid.bucket = bucket;
            let result = PartitionIndex::load(&io, &layout, &[invalid], 1).await;
            assert!(result
                .err()
                .unwrap()
                .to_string()
                .contains("Dynamic bucket id"));
        }
        let duplicate = vec![entry.clone(), entry.clone()];
        assert!(PartitionIndex::load(&io, &layout, &duplicate, 1)
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("Multiple HASH"));
        let mut conflicting = entry.clone();
        conflicting.bucket = 1;
        assert!(
            PartitionIndex::load(&io, &layout, &[entry.clone(), conflicting], 1)
                .await
                .err()
                .unwrap()
                .to_string()
                .contains("both buckets")
        );
        let mut truncated = entry.clone();
        truncated.index_file.row_count = 2;
        assert!(PartitionIndex::load(&io, &layout, &[truncated], 1)
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("expected 2 hashes"));
        let path = layout.resolve(0, &entry.index_file.file_name, None);
        io.new_output(&path)
            .unwrap()
            .write(bytes::Bytes::from_static(&[0, 0, 0]))
            .await
            .unwrap();
        assert!(PartitionIndex::load(&io, &layout, &[entry], 1)
            .await
            .err()
            .unwrap()
            .to_string()
            .contains("multiple of 4"));
    }

    #[tokio::test]
    async fn test_dynamic_bucket_requires_configured_index_location() {
        let tmp = tempfile::tempdir().unwrap();
        let root = format!("file://{}", tmp.path().display());
        let io = FileIO::from_url(&root).unwrap().build().unwrap();
        let meta = write_hash_index(&io, &format!("{root}/index"), &[42])
            .await
            .unwrap();
        let layout = HashIndexLayout {
            table_path: &root,
            partition_path: "p=a/",
            data_file_path_directory: Some("data"),
            index_file_in_data_file_dir: true,
        };
        let canonical = layout.resolve(0, &meta.file_name, None);
        let entry = IndexManifestEntry {
            version: 1,
            kind: crate::spec::FileKind::Add,
            partition: EMPTY_SERIALIZED_ROW.to_vec(),
            bucket: 0,
            index_file: meta,
        };
        let error = PartitionIndex::load(&io, &layout, std::slice::from_ref(&entry), 1)
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains(&entry.index_file.file_name));
        io.new_output(&canonical)
            .unwrap()
            .write(bytes::Bytes::from_static(&[0, 0, 0, 43]))
            .await
            .unwrap();
        let index = PartitionIndex::load(&io, &layout, &[entry], 1)
            .await
            .unwrap();
        assert_eq!(index.hash_to_bucket.get(&43), Some(&0));
        assert!(!index.hash_to_bucket.contains_key(&42));
    }

    // -- HashIndexFile tests --

    /// Reads and writes resolve through the same layout, so a hash index written
    /// under one configuration is found again; an explicit external path wins.
    #[tokio::test]
    async fn test_hash_index_layout_round_trips_read_and_write() {
        for index_file_in_data_file_dir in [false, true] {
            let tmp = tempfile::tempdir().unwrap();
            let table_path = format!("file://{}", tmp.path().display());
            let file_io = FileIO::from_url(&table_path).unwrap().build().unwrap();
            let layout = super::HashIndexLayout {
                data_file_path_directory: None,
                table_path: &table_path,
                partition_path: "pt=1/",
                index_file_in_data_file_dir,
            };

            // Write where this layout says, then read it back through the same layout.
            let dir = layout.directory(3);
            file_io.mkdirs(&dir).await.unwrap();
            let hashes = vec![7i32, 8, 9];
            let meta = write_hash_index(&file_io, &dir, &hashes).await.unwrap();
            let entries = vec![IndexManifestEntry {
                version: 1,
                kind: crate::spec::FileKind::Add,
                partition: EMPTY_SERIALIZED_ROW.to_vec(),
                bucket: 3,
                index_file: meta,
            }];
            let loaded = PartitionIndex::load(&file_io, &layout, &entries, 100)
                .await
                .unwrap();
            for hash in &hashes {
                assert_eq!(loaded.hash_to_bucket.get(hash), Some(&3));
            }

            let expected_dir = if index_file_in_data_file_dir {
                format!("{table_path}/pt=1/bucket-3")
            } else {
                format!("{table_path}/index")
            };
            assert_eq!(dir, expected_dir);
        }
    }

    /// An external path wins over both layouts.
    #[tokio::test]
    async fn test_hash_index_external_path_wins() {
        let tmp = tempfile::tempdir().unwrap();
        let table_path = format!("file://{}", tmp.path().display());
        let file_io = FileIO::from_url(&table_path).unwrap().build().unwrap();
        let external_dir = format!("{table_path}/elsewhere");
        file_io.mkdirs(&external_dir).await.unwrap();
        let mut index_file = write_hash_index(&file_io, &external_dir, &[42i32])
            .await
            .unwrap();
        index_file.external_path = Some(format!("{external_dir}/{}", index_file.file_name));

        for index_file_in_data_file_dir in [false, true] {
            let layout = super::HashIndexLayout {
                data_file_path_directory: None,
                table_path: &table_path,
                partition_path: "pt=1/",
                index_file_in_data_file_dir,
            };
            let entries = vec![IndexManifestEntry {
                version: 1,
                kind: crate::spec::FileKind::Add,
                partition: EMPTY_SERIALIZED_ROW.to_vec(),
                bucket: 5,
                index_file: index_file.clone(),
            }];
            let loaded = PartitionIndex::load(&file_io, &layout, &entries, 100)
                .await
                .unwrap();
            assert_eq!(loaded.hash_to_bucket.get(&42), Some(&5));
        }
    }

    #[tokio::test]
    async fn test_hash_index_roundtrip() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = format!("file://{}", tmp.path().display());
        let file_io = FileIO::from_url(&dir).unwrap().build().unwrap();

        let hashes = vec![42, -1, 0, i32::MAX, i32::MIN];
        let meta = write_hash_index(&file_io, &dir, &hashes).await.unwrap();

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

        let meta = write_hash_index(&file_io, &dir, &[]).await.unwrap();
        assert_eq!(meta.row_count, 0);
        assert_eq!(meta.file_size, 0);

        let path = format!("{dir}/{}", meta.file_name);
        let read_back = HashIndexFile::read(&file_io, &path).await.unwrap();
        assert!(read_back.is_empty());
    }
}
