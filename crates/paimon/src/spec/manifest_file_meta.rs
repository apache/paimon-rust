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

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_bytes::{Bytes, Deserialize as SerdeBytesDeserialize, Serialize as SerdeBytesSerialize};
use std::fmt::{Display, Formatter};

/// Metadata of a manifest file.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/db8bcd7fdd9c2705435d2ab1d2341c52d1f67ee5/paimon-core/src/main/java/org/apache/paimon/manifest/ManifestFileMeta.java>
#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub struct ManifestFileMeta {
    /// manifest file name
    #[serde(rename = "_FILE_NAME")]
    file_name: String,

    /// manifest file size.
    #[serde(rename = "_FILE_SIZE")]
    file_size: i64,

    /// number added files in manifest.
    #[serde(rename = "_NUM_ADDED_FILES")]
    num_added_files: i64,

    /// number deleted files in manifest.
    #[serde(rename = "_NUM_DELETED_FILES")]
    num_deleted_files: i64,

    /// partition stats, the minimum and maximum values of partition fields in this manifest are beneficial for skipping certain manifest files during queries, it is a SimpleStats.
    #[serde(rename = "_PARTITION_STATS")]
    partition_stats: SimpleStats,

    /// schema id when writing this manifest file.
    #[serde(rename = "_SCHEMA_ID")]
    schema_id: i64,
}

impl ManifestFileMeta {
    /// Get the manifest file name
    #[inline]
    pub fn file_name(&self) -> &str {
        self.file_name.as_str()
    }

    /// Get the manifest file size.
    #[inline]
    pub fn file_size(&self) -> i64 {
        self.file_size
    }

    /// Get the number added files in manifest.
    #[inline]
    pub fn num_added_files(&self) -> i64 {
        self.num_added_files
    }

    /// Get the number deleted files in manifest.
    #[inline]
    pub fn num_deleted_files(&self) -> i64 {
        self.num_deleted_files
    }

    /// Get the partition stats
    pub fn partition_stats(&self) -> &SimpleStats {
        &self.partition_stats
    }

    /// Get the schema id when writing this manifest file.
    #[inline]
    pub fn schema_id(&self) -> i64 {
        self.schema_id
    }
}

impl Display for ManifestFileMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{{}, {}, {}, {}, {:?}, {}}}",
            self.file_name,
            self.file_size,
            self.num_added_files,
            self.num_deleted_files,
            self.partition_stats,
            self.schema_id
        )
    }
}

/// The statistics for columns, supports the following stats.
///
/// All statistics are stored in the form of a Binary, which can significantly reduce its memory consumption, but the cost is that the column type needs to be known when getting.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/db8bcd7fdd9c2705435d2ab1d2341c52d1f67ee5/paimon-core/src/main/java/org/apache/paimon/stats/SimpleStats.java>
///
/// Impl Reference: <https://github.com/apache/paimon/blob/db8bcd7fdd9c2705435d2ab1d2341c52d1f67ee5/paimon-core/src/main/java/org/apache/paimon/stats/SimpleStatsConverter.java#L111>
#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub struct SimpleStats {
    /// the minimum values of the columns
    #[serde(rename = "_MIN_VALUES", with = "serde_bytes")]
    min_values: Vec<u8>,

    /// the maximum values of the columns
    #[serde(rename = "_MAX_VALUES", with = "serde_bytes")]
    max_values: Vec<u8>,

    /// the number of nulls of the columns
    #[serde(
        rename = "_NULL_COUNTS",
        serialize_with = "serialize_null_counts",
        deserialize_with = "deserialize_null_counts"
    )]
    null_counts: Vec<i64>,
}

impl SimpleStats {
    /// Get the minimum values of the columns
    #[inline]
    pub fn min_values(&self) -> &[u8] {
        &self.min_values
    }

    /// Get the maximum values of the columns
    #[inline]
    pub fn max_values(&self) -> &[u8] {
        &self.max_values
    }

    /// Get the number of nulls of the columns
    #[inline]
    pub fn null_counts(&self) -> &Vec<i64> {
        &self.null_counts
    }
}

impl Display for SimpleStats {
    fn fmt(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

fn serialize_null_counts<S>(value: &Vec<i64>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut bytes = Vec::new();
    for &num in value {
        bytes.extend_from_slice(&num.to_le_bytes());
    }

    let bytes = Bytes::new(bytes.as_slice());
    serializer.serialize_bytes(bytes)
}

fn deserialize_null_counts<'de, D>(deserializer: D) -> Result<Vec<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    let bytes = Deserialize::deserialize(deserializer).map(Bytes::new)?;

    let size_of_i64 = std::mem::size_of::<i64>();
    let i64_count = bytes.len() / size_of_i64;
    let mut i64s = Vec::with_capacity(i64_count);
    for chunk in bytes.chunks_exact(size_of_i64) {
        i64s.push(i64::from_le_bytes(
            chunk.try_into().expect("Chunk must be 8 bytes long"),
        ));
    }
    Ok(i64s)
}
