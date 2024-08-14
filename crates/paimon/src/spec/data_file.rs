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

use crate::spec::{BinaryTableStats, RowType};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

pub const EMPTY_BINARY_ROW: BinaryRow = BinaryRow::new(0);

/// An implementation of InternalRow.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/data/BinaryRow.java>
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinaryRow {
    arity: i32,
    null_bits_size_in_bytes: i32,
}

impl BinaryRow {
    pub const HEADER_SIZE_IN_BYTES: i32 = 8;
    pub const fn cal_bit_set_width_in_bytes(arity: i32) -> i32 {
        ((arity + 63 + Self::HEADER_SIZE_IN_BYTES) / 64) * 8
    }
    pub const fn cal_fix_part_size_in_bytes(arity: i32) -> i32 {
        Self::cal_bit_set_width_in_bytes(arity) + 8 * arity
    }
    pub const fn new(arity: i32) -> Self {
        Self {
            arity,
            null_bits_size_in_bytes: (arity + 7) / 8,
        }
    }
}

/// Metadata of a data file.
///
/// Impl References: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/io/DataFileMeta.java>
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DataFileMeta {
    pub file_name: String,
    pub file_size: i64,
    // row_count tells the total number of rows (including add & delete) in this file.
    pub row_count: i64,
    pub min_key: BinaryRow,
    pub max_key: BinaryRow,
    pub key_stats: Option<BinaryTableStats>,
    pub value_stats: Option<BinaryTableStats>,
    pub min_sequence_number: i64,
    pub max_sequence_number: i64,
    pub schema_id: i64,
    pub level: i32,
    pub extra_files: Vec<String>,
    pub creation_time: DateTime<Utc>,
    // rowCount = add_row_count + delete_row_count.
    pub delete_row_count: Option<i64>,
    // file index filter bytes, if it is small, store in data file meta
    pub embedded_index: Option<Vec<u8>>,
}

impl Display for DataFileMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{fileName: {}, fileSize: {}, rowCount: {}, embeddedIndex: {:?}, \
            minKey: {:?}, maxKey: {:?}, keyStats: {:?}, valueStats: {:?}, \
            minSequenceNumber: {}, maxSequenceNumber: {}, \
            schemaId: {}, level: {}, extraFiles: {:?}, creationTime: {}, deleteRowCount: {:?}}}",
            self.file_name,
            self.file_size,
            self.row_count,
            self.embedded_index,
            self.min_key,
            self.max_key,
            self.key_stats,
            self.value_stats,
            self.min_sequence_number,
            self.max_sequence_number,
            self.schema_id,
            self.level,
            self.extra_files,
            self.creation_time,
            self.delete_row_count
        )
    }
}

impl DataFileMeta {
    pub const SCHEMA: RowType = RowType::new(vec![]);

    /// Get the file name.
    pub fn file_name(&self) -> &str {
        &self.file_name
    }

    /// Get the file size.
    pub fn file_size(&self) -> i64 {
        self.file_size
    }

    /// Get the row count.
    pub fn row_count(&self) -> i64 {
        self.row_count
    }

    /// Get the min key.
    pub fn min_key(&self) -> &BinaryRow {
        &self.min_key
    }

    /// Get the max key.
    pub fn max_key(&self) -> &BinaryRow {
        &self.max_key
    }

    /// Get the key stats.
    pub fn key_stats(&self) -> Option<&BinaryTableStats> {
        self.key_stats.as_ref()
    }

    /// Get the value stats.
    pub fn value_stats(&self) -> Option<&BinaryTableStats> {
        self.value_stats.as_ref()
    }

    /// Get the min sequence number.
    pub fn min_sequence_number(&self) -> i64 {
        self.min_sequence_number
    }

    /// Get the max sequence number.
    pub fn max_sequence_number(&self) -> i64 {
        self.max_sequence_number
    }

    /// Get the schema id.
    pub fn schema_id(&self) -> i64 {
        self.schema_id
    }

    /// Get the level.
    pub fn level(&self) -> i32 {
        self.level
    }

    /// Get the extra files.
    pub fn extra_files(&self) -> &[String] {
        &self.extra_files
    }

    /// Get the creation time.
    pub fn creation_time(&self) -> DateTime<Utc> {
        self.creation_time
    }

    /// Get the delete row count.
    pub fn delete_row_count(&self) -> Option<i64> {
        self.delete_row_count
    }

    /// Get the embedded index.
    pub fn embedded_index(&self) -> Option<&[u8]> {
        self.embedded_index.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_file_meta_serialize_deserialize() {
        let json_data = r#"
        {
           "fileName":"test.avro",
           "fileSize":1024,
           "rowCount":100,
           "minKey":{
              "arity":1,
              "nullBitsSizeInBytes":1
           },
           "maxKey":{
              "arity":10,
              "nullBitsSizeInBytes":2
           },
           "keyStats":null,
           "valueStats":null,
           "minSequenceNumber":0,
           "maxSequenceNumber":100,
           "schemaId":0,
           "level":0,
           "extraFiles":[],
           "creationTime":"2024-08-13T02:03:03.106490600Z",
           "deleteRowCount":5,
           "embeddedIndex":null
        }
        "#;

        let data_file_meta: DataFileMeta =
            serde_json::from_str(json_data).expect("Failed to deserialize DataFileMeta");

        assert_eq!(data_file_meta.file_name, "test.avro");
        assert_eq!(data_file_meta.file_size, 1024);
        assert_eq!(data_file_meta.row_count, 100);

        assert_eq!(data_file_meta.min_key.arity, 1);
        assert_eq!(data_file_meta.min_key.null_bits_size_in_bytes, 1);
        assert_eq!(data_file_meta.max_key.arity, 10);
        assert_eq!(data_file_meta.max_key.null_bits_size_in_bytes, 2);

        assert!(data_file_meta.key_stats.is_none());
        assert!(data_file_meta.value_stats.is_none());

        assert_eq!(data_file_meta.min_sequence_number, 0);
        assert_eq!(data_file_meta.max_sequence_number, 100);
        assert_eq!(data_file_meta.schema_id, 0);
        assert_eq!(data_file_meta.level, 0);
        assert_eq!(data_file_meta.extra_files.len(), 0);
        assert_eq!(
            data_file_meta.creation_time,
            DateTime::parse_from_rfc3339("2024-08-13T02:03:03.106490600Z")
                .unwrap()
                .with_timezone(&Utc)
        );
        assert_eq!(data_file_meta.delete_row_count, Some(5));
        assert!(data_file_meta.embedded_index.is_none());
    }
}
