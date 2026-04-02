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

//! Table source types: DataSplit, Plan, DeletionFile, and related structs.
//!
//! Reference: [org.apache.paimon.table.source](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/table/source/).

#![allow(dead_code)]

use crate::spec::{BinaryRow, DataFileMeta};
use serde::{Deserialize, Serialize};

// ======================= DeletionFile ===============================

/// Deletion file for a data file: describes a region in a file that stores deletion vector bitmap.
///
/// Format of the region (first 4 bytes length, then magic, then RoaringBitmap content):
/// - First 4 bytes: length (should equal [Self::length]).
/// - Next 4 bytes: magic number (1581511376).
/// - Remaining: serialized RoaringBitmap.
///
/// Reference: [org.apache.paimon.table.source.DeletionFile](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/table/source/DeletionFile.java)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeletionFile {
    /// Path of the file containing the deletion vector (e.g. index file path).
    path: String,
    /// Starting offset of the deletion vector data in the file.
    offset: i64,
    /// Length in bytes of the deletion vector data.
    length: i64,
    /// Number of deleted rows (cardinality of the bitmap), if known.
    cardinality: Option<i64>,
}

impl DeletionFile {
    pub fn new(path: String, offset: i64, length: i64, cardinality: Option<i64>) -> Self {
        Self {
            path,
            offset,
            length,
            cardinality,
        }
    }

    /// Path of the file.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Starting offset of data in the file.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Length of data in the file.
    pub fn length(&self) -> i64 {
        self.length
    }

    /// Number of deleted rows, if known.
    pub fn cardinality(&self) -> Option<i64> {
        self.cardinality
    }
}

// ======================= PartitionBucket ===============================

/// Key for grouping splits by partition and bucket: (partition bytes, bucket id).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionBucket {
    pub partition: Vec<u8>,
    pub bucket: i32,
}

impl PartitionBucket {
    pub fn new(partition: Vec<u8>, bucket: i32) -> Self {
        Self { partition, bucket }
    }
}

// ======================= DataSplit ===============================

/// Input split for reading: partition + bucket + list of data files and optional deletion files.
///
/// Reference: [org.apache.paimon.table.source.DataSplit](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/table/source/DataSplit.java)
#[derive(Debug, Clone)]
pub struct DataSplit {
    snapshot_id: i64,
    partition: BinaryRow,
    bucket: i32,
    bucket_path: String,
    total_buckets: i32,
    data_files: Vec<DataFileMeta>,
    /// Deletion file for each data file, same order as `data_files`.
    /// `None` at index `i` means no deletion file for `data_files[i]` (matches Java getDeletionFiles() / List<DeletionFile> with null elements).
    data_deletion_files: Option<Vec<Option<DeletionFile>>>,
    /// Whether this split can be read file-by-file without merging.
    /// `false` when files need column-wise merge (e.g. data evolution) or
    /// key-value merge (e.g. primary key tables without deletion vectors).
    raw_convertible: bool,
}

impl DataSplit {
    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }
    pub fn partition(&self) -> &BinaryRow {
        &self.partition
    }
    pub fn bucket(&self) -> i32 {
        self.bucket
    }
    pub fn bucket_path(&self) -> &str {
        &self.bucket_path
    }
    pub fn total_buckets(&self) -> i32 {
        self.total_buckets
    }

    pub fn data_files(&self) -> &[DataFileMeta] {
        &self.data_files
    }

    /// Deletion files for each data file (same order as `data_files`); `None` = no deletion file for that data file.
    pub fn data_deletion_files(&self) -> Option<&[Option<DeletionFile>]> {
        self.data_deletion_files.as_deref()
    }

    /// Whether this split can be read without column-wise merging.
    pub fn raw_convertible(&self) -> bool {
        self.raw_convertible
    }

    /// Returns the deletion file for the data file at the given index, if any. `None` at that index means no deletion file.
    pub fn deletion_file_for_data_file_index(&self, index: usize) -> Option<&DeletionFile> {
        self.data_deletion_files
            .as_deref()?
            .get(index)
            .and_then(Option::as_ref)
    }

    /// Returns the deletion file for the given data file (by file name), if any.
    pub fn deletion_file_for_data_file(&self, file: &DataFileMeta) -> Option<&DeletionFile> {
        let index = self
            .data_files
            .iter()
            .position(|f| f.file_name == file.file_name)?;
        self.deletion_file_for_data_file_index(index)
    }

    /// Full path for a single data file in this split (bucket_path + file_name).
    pub fn data_file_path(&self, file: &DataFileMeta) -> String {
        let base = self.bucket_path.trim_end_matches('/');
        format!("{}/{}", base, file.file_name)
    }

    /// Iterate over each data file in this split, yielding `(path, &DataFileMeta)`.
    /// Use this to read each data file one by one (e.g. in ArrowReader).
    pub fn data_file_entries(&self) -> impl Iterator<Item = (String, &DataFileMeta)> + '_ {
        let base = self.bucket_path.trim_end_matches('/');
        // todo: consider partition table
        // todo: consider external path
        self.data_files.iter().map(move |file| {
            let path = format!("{}/{}", base, file.file_name);
            (path, file)
        })
    }

    /// Total row count of all data files in this split.
    pub fn row_count(&self) -> i64 {
        self.data_files.iter().map(|f| f.row_count).sum()
    }

    pub fn builder() -> DataSplitBuilder {
        DataSplitBuilder::new()
    }
}

/// Builder for [DataSplit].
///
/// Reference: [DataSplit.Builder](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/table/source/DataSplit.java)
#[derive(Debug)]
pub struct DataSplitBuilder {
    snapshot_id: i64,
    partition: Option<BinaryRow>,
    bucket: i32,
    bucket_path: Option<String>,
    total_buckets: i32,
    data_files: Option<Vec<DataFileMeta>>,
    /// Same length as data_files; `None` at index i = no deletion file for data_files[i].
    data_deletion_files: Option<Vec<Option<DeletionFile>>>,
    raw_convertible: bool,
}

impl DataSplitBuilder {
    pub fn new() -> Self {
        Self {
            snapshot_id: -1,
            partition: None,
            bucket: -1,
            bucket_path: None,
            total_buckets: -1,
            data_files: None,
            data_deletion_files: None,
            raw_convertible: false,
        }
    }

    pub fn with_snapshot(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = snapshot_id;
        self
    }
    pub fn with_partition(mut self, partition: BinaryRow) -> Self {
        self.partition = Some(partition);
        self
    }
    pub fn with_bucket(mut self, bucket: i32) -> Self {
        self.bucket = bucket;
        self
    }
    pub fn with_bucket_path(mut self, bucket_path: String) -> Self {
        self.bucket_path = Some(bucket_path);
        self
    }
    pub fn with_total_buckets(mut self, total_buckets: i32) -> Self {
        self.total_buckets = total_buckets;
        self
    }
    pub fn with_data_files(mut self, data_files: Vec<DataFileMeta>) -> Self {
        self.data_files = Some(data_files);
        self
    }

    /// Sets deletion files; length must match data_files. Use `None` at index i when data_files[i] has no deletion file.
    pub fn with_data_deletion_files(
        mut self,
        data_deletion_files: Vec<Option<DeletionFile>>,
    ) -> Self {
        self.data_deletion_files = Some(data_deletion_files);
        self
    }

    pub fn with_raw_convertible(mut self, raw_convertible: bool) -> Self {
        self.raw_convertible = raw_convertible;
        self
    }

    pub fn build(self) -> crate::Result<DataSplit> {
        if self.snapshot_id == -1 {
            return Err(crate::Error::UnexpectedError {
                message: "DataSplit requires snapshot_id != -1".to_string(),
                source: None,
            });
        }
        let partition = self
            .partition
            .ok_or_else(|| crate::Error::UnexpectedError {
                message: "DataSplit requires partition".to_string(),
                source: None,
            })?;
        let bucket_path = self
            .bucket_path
            .ok_or_else(|| crate::Error::UnexpectedError {
                message: "DataSplit requires bucket_path".to_string(),
                source: None,
            })?;
        let data_files = self
            .data_files
            .ok_or_else(|| crate::Error::UnexpectedError {
                message: "DataSplit requires data_files".to_string(),
                source: None,
            })?;
        if self.bucket == -1 {
            return Err(crate::Error::UnexpectedError {
                message: "DataSplit requires bucket != -1".to_string(),
                source: None,
            });
        }
        if let Some(ref data_deletion_files) = self.data_deletion_files {
            if data_deletion_files.len() != data_files.len() {
                return Err(crate::Error::UnexpectedError {
                    message: format!(
                        "DataSplit deletion files length {} must match data_files length {}",
                        data_deletion_files.len(),
                        data_files.len()
                    ),
                    source: None,
                });
            }
        }
        Ok(DataSplit {
            snapshot_id: self.snapshot_id,
            partition,
            bucket: self.bucket,
            bucket_path,
            total_buckets: self.total_buckets,
            data_files,
            data_deletion_files: self.data_deletion_files,
            raw_convertible: self.raw_convertible,
        })
    }
}

impl Default for DataSplitBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ======================= Plan ===============================

/// Read plan: list of splits.
///
/// Reference: [org.apache.paimon.table.source.PlanImpl](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/table/source/PlanImpl.java)
#[derive(Debug)]
pub struct Plan {
    splits: Vec<DataSplit>,
}

impl Plan {
    pub fn new(splits: Vec<DataSplit>) -> Self {
        Self { splits }
    }
    pub fn splits(&self) -> &[DataSplit] {
        &self.splits
    }
}
