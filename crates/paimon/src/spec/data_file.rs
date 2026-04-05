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

use crate::spec::stats::BinaryTableStats;
use chrono::serde::ts_milliseconds_option::deserialize as from_millis_opt;
use chrono::serde::ts_milliseconds_option::serialize as to_millis_opt;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Metadata of a data file.
///
/// Impl References: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/io/DataFileMeta.java>
#[derive(Debug, Eq, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DataFileMeta {
    #[serde(rename = "_FILE_NAME")]
    pub file_name: String,
    #[serde(rename = "_FILE_SIZE")]
    pub file_size: i64,
    // row_count tells the total number of rows (including add & delete) in this file.
    #[serde(rename = "_ROW_COUNT")]
    pub row_count: i64,
    #[serde(rename = "_MIN_KEY", with = "serde_bytes")]
    pub min_key: Vec<u8>,
    #[serde(rename = "_MAX_KEY", with = "serde_bytes")]
    pub max_key: Vec<u8>,
    #[serde(rename = "_KEY_STATS")]
    pub key_stats: BinaryTableStats,
    #[serde(rename = "_VALUE_STATS")]
    pub value_stats: BinaryTableStats,
    #[serde(rename = "_MIN_SEQUENCE_NUMBER")]
    pub min_sequence_number: i64,
    #[serde(rename = "_MAX_SEQUENCE_NUMBER")]
    pub max_sequence_number: i64,
    #[serde(rename = "_SCHEMA_ID")]
    pub schema_id: i64,
    #[serde(rename = "_LEVEL")]
    pub level: i32,
    #[serde(rename = "_EXTRA_FILES")]
    pub extra_files: Vec<String>,
    #[serde(
        rename = "_CREATION_TIME",
        serialize_with = "to_millis_opt",
        deserialize_with = "from_millis_opt",
        default
    )]
    pub creation_time: Option<DateTime<Utc>>,
    #[serde(rename = "_DELETE_ROW_COUNT")]
    // rowCount = add_row_count + delete_row_count.
    pub delete_row_count: Option<i64>,
    // file index filter bytes, if it is small, store in data file meta
    #[serde(rename = "_EMBEDDED_FILE_INDEX", with = "serde_bytes")]
    pub embedded_index: Option<Vec<u8>>,

    /// File source identifier (e.g. APPEND, COMPACT).
    #[serde(
        rename = "_FILE_SOURCE",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub file_source: Option<i32>,

    /// Column names covered by `_VALUE_STATS` when stats are written in dense mode.
    #[serde(
        rename = "_VALUE_STATS_COLS",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub value_stats_cols: Option<Vec<String>>,

    /// External path for the data file (e.g. when data is stored outside the table directory).
    #[serde(
        rename = "_EXTERNAL_PATH",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub external_path: Option<String>,

    /// The starting row ID for this file's data (used in data evolution mode).
    #[serde(
        rename = "_FIRST_ROW_ID",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub first_row_id: Option<i64>,

    /// Which table columns this file contains (used in data evolution mode).
    #[serde(
        rename = "_WRITE_COLS",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub write_cols: Option<Vec<String>>,
}

impl Display for DataFileMeta {
    fn fmt(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl DataFileMeta {
    /// Returns the row ID range `[first_row_id, first_row_id + row_count - 1]` if `first_row_id` is set.
    pub fn row_id_range(&self) -> Option<(i64, i64)> {
        self.first_row_id.map(|fid| (fid, fid + self.row_count - 1))
    }
}
