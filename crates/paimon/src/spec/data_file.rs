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

use super::schema::DataField;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Data type of a sequence of fields. A field consists of a field name, field type, and an optional
/// description. The most specific type of a row of a table is a row type. In this case, each column
/// of the row corresponds to the field of the row type that has the same ordinal position as the
/// column. Compared to the SQL standard, an optional field description simplifies the handling with
/// complex structures.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/db8bcd7fdd9c2705435d2ab1d2341c52d1f67ee5/paimon-common/src/main/java/org/apache/paimon/types/RowType.java>
///
/// TODO: make RowType extends DataType.
/// TODO: move me to a better place.
pub struct RowType {
    _fields: Vec<DataField>,
}

impl RowType {
    pub const fn new(list: Vec<DataField>) -> Self {
        Self { _fields: list }
    }
}

pub const EMPTY_BINARY_ROW: BinaryRow = BinaryRow::new(0);

/// An implementation of InternalRow.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/db8bcd7fdd9c2705435d2ab1d2341c52d1f67ee5/paimon-common/src/main/java/org/apache/paimon/data/BinaryRow.java>
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize, Copy, Clone)]
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

/// TODO: implement me.
/// The statistics for columns, supports the following stats.
///
/// Impl References: <https://github.com/apache/paimon/blob/db8bcd7fdd9c2705435d2ab1d2341c52d1f67ee5/paimon-core/src/main/java/org/apache/paimon/stats/SimpleStats.java>
type SimpleStats = ();

/// The Source of a file.
/// TODO: move me to the manifest module.
///
/// Impl References: <https://github.com/apache/paimon/blob/db8bcd7fdd9c2705435d2ab1d2341c52d1f67ee5/paimon-core/src/main/java/org/apache/paimon/manifest/FileSource.java>
#[repr(u8)]
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FileSource {
    Append = 0,
    Compact = 1,
}

/// Metadata of a data file.
///
/// Impl References: <https://github.com/apache/paimon/blob/db8bcd7fdd9c2705435d2ab1d2341c52d1f67ee5/paimon-core/src/main/java/org/apache/paimon/io/DataFileMeta.java>
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DataFileMeta {
    pub file_name: String,
    pub file_size: i64,
    // row_count tells the total number of rows (including add & delete) in this file.
    pub row_count: i64,
    pub min_key: BinaryRow,
    pub max_key: BinaryRow,
    pub key_stats: SimpleStats,
    pub value_stats: SimpleStats,
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
    pub file_source: Option<FileSource>,
}

impl Display for DataFileMeta {
    fn fmt(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl DataFileMeta {
    // TODO: implement me
    pub const SCHEMA: RowType = RowType::new(vec![]);
}
