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
use crate::spec::{
    extract_datum, serialize_binary_array_long, serialize_binary_array_str, BinaryRow,
    BinaryRowBuilder, DataField, Datum,
};
use chrono::serde::ts_milliseconds_option::deserialize as from_millis_opt;
use chrono::serde::ts_milliseconds_option::serialize as to_millis_opt;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Suffix for sidecar file-index files, matching Java `DataFilePathFactory.INDEX_PATH_SUFFIX`.
pub const DATA_FILE_INDEX_SUFFIX: &str = ".index";

/// Manifest value statistics for one column in one data file.
#[derive(Debug, Clone, PartialEq)]
pub struct DataFileColumnStats {
    pub min_value: Option<Datum>,
    pub max_value: Option<Datum>,
    pub null_count: Option<i64>,
}

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
    // A negative value means the producer does not know the row count; see
    // [`DataFileMeta::ROW_COUNT_UNKNOWN`]. Never treat it as a real count.
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
    /// Local wall-clock TIMESTAMP(3), matching Java Timestamp.now().
    /// The UTC label carries the serialized fields; it does not identify an instant.
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

    /// Per-column maximum sequence numbers, positionally aligned with
    /// [`Self::write_cols`] (used in data evolution mode). Absent on files
    /// written before this field existed.
    #[serde(
        rename = "_WRITE_COLS_SEQUENCES",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub column_max_sequence_numbers: Option<Vec<i64>>,
}

impl Display for DataFileMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DataFileMeta{{fileName={}, fileSize={}, rowCount={}, embeddedIndex={:?}, \
             minKey={:?}, maxKey={:?}, keyStats={:?}, valueStats={:?}, \
             minSequenceNumber={}, maxSequenceNumber={}, schemaId={}, level={}, \
             extraFiles={:?}, creationTime={:?}, deleteRowCount={:?}, fileSource={:?}, \
             valueStatsCols={:?}, externalPath={:?}, firstRowId={:?}, writeCols={:?}, \
             columnMaxSequenceNumbers={:?}}}",
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
            self.delete_row_count,
            self.file_source,
            self.value_stats_cols,
            self.external_path,
            self.first_row_id,
            self.write_cols,
            self.column_max_sequence_numbers,
        )
    }
}

/// Which `DataFileMeta.SCHEMA` revision a serialized `BinaryRow` follows.
///
/// The schema only ever grows by appending nullable fields, so the two layouts
/// share slots 0..=19 and differ solely in arity. Which one a `DataSplit` body
/// carries is decided by the split's own version, not by the row bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataFileMetaRowLayout {
    /// 20 fields, ending at `_WRITE_COLS`. Mirrors Java
    /// `DataFileMetaWriteColsLegacySerializer`.
    WriteCols,
    /// 21 fields, ending at `_WRITE_COLS_SEQUENCES`. Mirrors Java
    /// `DataFileMetaSerializer`, and is what this crate writes.
    WriteColsSequences,
}

impl DataFileMetaRowLayout {
    /// The layout this crate serializes to, matching current Java.
    pub const CURRENT: Self = Self::WriteColsSequences;

    fn arity(self) -> usize {
        match self {
            Self::WriteCols => 20,
            Self::WriteColsSequences => 21,
        }
    }
}

fn opt_long(b: &mut BinaryRowBuilder, pos: usize, v: Option<i64>) {
    match v {
        Some(x) => b.write_long(pos, x),
        None => b.set_null_at(pos),
    }
}

/// Write an `array<bigint not null>` field, or a null at `pos`.
fn opt_long_array(b: &mut BinaryRowBuilder, pos: usize, v: &Option<Vec<i64>>) {
    match v {
        Some(a) => {
            let elements: Vec<Option<i64>> = a.iter().copied().map(Some).collect();
            b.write_bytes(pos, &serialize_binary_array_long(&elements));
        }
        None => b.set_null_at(pos),
    }
}

/// Read an `array<bigint not null>` field. Java declares the elements non-null, so a
/// null element is malformed rather than something to silently map to a placeholder.
fn read_non_null_long_array(row: &BinaryRow, pos: usize) -> crate::Result<Vec<i64>> {
    crate::spec::deserialize_binary_array_long(row.get_binary(pos)?)?
        .into_iter()
        .map(|v| v.ok_or_else(|| data_file_err("array<bigint not null> has a null element")))
        .collect()
}

fn opt_str_array(b: &mut BinaryRowBuilder, pos: usize, v: &Option<Vec<String>>) {
    match v {
        Some(a) => b.write_bytes(pos, &serialize_binary_array_str(a)),
        None => b.set_null_at(pos),
    }
}

fn data_file_err(msg: &str) -> crate::Error {
    crate::Error::DataInvalid {
        message: msg.to_string(),
        source: None,
    }
}

/// Reverse of `BinaryRowBuilder::write_timestamp_compact`: read the compact
/// (precision-3) timestamp at `pos` back as epoch-millis and map it to `Utc`.
fn read_compact_millis_as_utc(
    row: &BinaryRow,
    pos: usize,
) -> crate::Result<chrono::DateTime<chrono::Utc>> {
    let (millis, _nanos) = row.get_timestamp_raw(pos, 3)?;
    chrono::DateTime::from_timestamp_millis(millis)
        .ok_or_else(|| data_file_err("creation_time out of range"))
}

impl DataFileMeta {
    /// Resolve the stored local creation timestamp in the system time zone,
    /// matching Java DataFileMeta.creationTimeEpochMillis().
    pub fn creation_time_epoch_millis(&self) -> crate::Result<Option<i64>> {
        self.creation_time_epoch_millis_in_zone(&jiff::tz::TimeZone::system())
    }

    pub(crate) fn creation_time_epoch_millis_in_zone(
        &self,
        zone: &jiff::tz::TimeZone,
    ) -> crate::Result<Option<i64>> {
        self.creation_time
            .map(|time| {
                let millis = time.timestamp_millis();
                crate::common::date_time::local_to_utc_seconds(millis.div_euclid(1000), zone)
                    .map(|seconds| seconds * 1000 + millis.rem_euclid(1000))
            })
            .transpose()
    }

    /// Placeholder for `row_count` when the producer cannot know how many rows
    /// a file holds.
    ///
    /// Metadata that comes from a manifest always carries a real count. Sources
    /// that plan over bare directory listings (for example `type=format-table`,
    /// which has no manifest) have nothing to fill in and must use this value:
    /// a plain `0` is indistinguishable from a file that really is empty, and
    /// every consumer that trusts it silently returns a wrong answer.
    pub const ROW_COUNT_UNKNOWN: i64 = -1;

    /// Whether `row_count` is a real count rather than [`Self::ROW_COUNT_UNKNOWN`].
    pub fn row_count_known(&self) -> bool {
        self.row_count >= 0
    }

    /// Decode this file's manifest value statistics for a field in the current schema.
    ///
    /// Returns `None` when statistics are missing, malformed, or belong to a different
    /// schema version. Callers must treat `None` as unknown rather than excluding data.
    pub fn value_stats_for_field(
        &self,
        current_schema_id: i64,
        current_fields: &[DataField],
        field: &DataField,
    ) -> Option<DataFileColumnStats> {
        if self.schema_id != current_schema_id {
            return None;
        }

        let (schema_index, schema_field) = current_fields
            .iter()
            .enumerate()
            .find(|(_, candidate)| candidate.id() == field.id())?;
        if schema_field.name() != field.name() || schema_field.data_type() != field.data_type() {
            return None;
        }
        let stats_index = if let Some(columns) = &self.value_stats_cols {
            columns.iter().position(|name| name == field.name())?
        } else if let Some(columns) = &self.write_cols {
            columns.iter().position(|name| name == field.name())?
        } else {
            schema_index
        };

        let min_values = BinaryRow::from_serialized_bytes(self.value_stats.min_values()).ok()?;
        let max_values = BinaryRow::from_serialized_bytes(self.value_stats.max_values()).ok()?;
        if stats_index >= usize::try_from(min_values.arity()).ok()?
            || stats_index >= usize::try_from(max_values.arity()).ok()?
        {
            return None;
        }
        let min_value = extract_datum(&min_values, stats_index, field.data_type()).ok()?;
        let max_value = extract_datum(&max_values, stats_index, field.data_type()).ok()?;
        let null_count = self
            .value_stats
            .null_counts()
            .get(stats_index)
            .copied()
            .flatten();

        if self.row_count < 0 || null_count.is_some_and(|count| count < 0 || count > self.row_count)
        {
            return None;
        }
        let all_null = null_count == Some(self.row_count);
        if all_null && (min_value.is_some() || max_value.is_some()) {
            return None;
        }
        if let (Some(min), Some(max)) = (&min_value, &max_value) {
            match min.partial_cmp(max) {
                Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal) => {}
                Some(std::cmp::Ordering::Greater) | None => return None,
            }
        }

        Some(DataFileColumnStats {
            min_value,
            max_value,
            null_count,
        })
    }

    /// Returns the row ID range `[first_row_id, first_row_id + row_count - 1]` if `first_row_id` is set.
    pub fn row_id_range(&self) -> Option<(i64, i64)> {
        let from = self.first_row_id?;
        if self.row_count <= 0 {
            return None;
        }
        let to = from.checked_add(self.row_count - 1)?;
        Some((from, to))
    }

    /// Serialize as a [`DataFileMetaRowLayout::CURRENT`] `DataFileMeta.SCHEMA` BinaryRow,
    /// raw data without the arity prefix -- the form `DataSplit#serialize` writes per file
    /// as `writeInt(len) + data`. Fields, order and nullability mirror Java
    /// `DataFileMetaSerializer#toRow`.
    ///
    /// Only the current layout is written. Reading an older one stays supported because a
    /// `DataSplit` on disk or on the wire may still carry it.
    pub fn to_serialized_row_data(&self) -> crate::Result<Vec<u8>> {
        let mut b = BinaryRowBuilder::new(DataFileMetaRowLayout::CURRENT.arity() as i32);
        b.write_bytes(0, self.file_name.as_bytes());
        b.write_long(1, self.file_size);
        b.write_long(2, self.row_count);
        b.write_bytes(3, &self.min_key);
        b.write_bytes(4, &self.max_key);
        b.write_bytes(5, &self.key_stats.to_simple_stats_row_data());
        b.write_bytes(6, &self.value_stats.to_simple_stats_row_data());
        b.write_long(7, self.min_sequence_number);
        b.write_long(8, self.max_sequence_number);
        b.write_long(9, self.schema_id);
        b.write_int(10, self.level);
        b.write_bytes(11, &serialize_binary_array_str(&self.extra_files));
        match self.creation_time {
            Some(t) => b.write_timestamp_compact(12, t.timestamp_millis()),
            None => b.set_null_at(12),
        }
        opt_long(&mut b, 13, self.delete_row_count);
        match &self.embedded_index {
            Some(v) => b.write_bytes(14, v),
            None => b.set_null_at(14),
        }
        match self.file_source {
            Some(v) => {
                let byte = i8::try_from(v).map_err(|_| crate::Error::DataInvalid {
                    message: format!("file_source {v} out of TINYINT range [-128, 127]"),
                    source: None,
                })?;
                b.write_byte(15, byte);
            }
            None => b.set_null_at(15),
        }
        opt_str_array(&mut b, 16, &self.value_stats_cols);
        match &self.external_path {
            Some(s) => b.write_bytes(17, s.as_bytes()),
            None => b.set_null_at(17),
        }
        opt_long(&mut b, 18, self.first_row_id);
        opt_str_array(&mut b, 19, &self.write_cols);
        opt_long_array(&mut b, 20, &self.column_max_sequence_numbers);
        Ok(b.build_row_data())
    }

    /// Reverse of [`DataFileMeta::to_serialized_row_data`]: decode a `DataFileMeta`
    /// BinaryRow written in `layout`.
    ///
    /// The row bytes carry no arity of their own, so the caller must supply the layout
    /// that the enclosing container declared.
    pub fn from_serialized_row_data(
        data: &[u8],
        layout: DataFileMetaRowLayout,
    ) -> crate::Result<DataFileMeta> {
        use crate::spec::deserialize_binary_array_str;
        let fixed_part = BinaryRow::cal_fix_part_size_in_bytes(layout.arity() as i32) as usize;
        if data.len() < fixed_part {
            return Err(data_file_err(&format!(
                "DataFileMeta row of {} bytes is shorter than the {fixed_part}-byte fixed part \
                 of the {layout:?} layout",
                data.len()
            )));
        }
        let row = BinaryRow::from_bytes(layout.arity() as i32, data.to_vec());

        let file_name = String::from_utf8(row.get_binary(0)?.to_vec())
            .map_err(|_| data_file_err("file_name is not valid UTF-8"))?;
        let file_size = row.get_long(1)?;
        let row_count = row.get_long(2)?;
        let min_key = row.get_binary(3)?.to_vec();
        let max_key = row.get_binary(4)?.to_vec();
        let key_stats = BinaryTableStats::from_simple_stats_row_data(row.get_binary(5)?)?;
        let value_stats = BinaryTableStats::from_simple_stats_row_data(row.get_binary(6)?)?;
        let min_sequence_number = row.get_long(7)?;
        let max_sequence_number = row.get_long(8)?;
        let schema_id = row.get_long(9)?;
        let level = row.get_int(10)?;
        let extra_files = deserialize_binary_array_str(row.get_binary(11)?)?;
        let creation_time = if row.is_null_at(12) {
            None
        } else {
            Some(read_compact_millis_as_utc(&row, 12)?)
        };
        let delete_row_count = if row.is_null_at(13) {
            None
        } else {
            Some(row.get_long(13)?)
        };
        let embedded_index = if row.is_null_at(14) {
            None
        } else {
            Some(row.get_binary(14)?.to_vec())
        };
        let file_source = if row.is_null_at(15) {
            None
        } else {
            Some(row.get_byte(15)? as i32)
        };
        let value_stats_cols = if row.is_null_at(16) {
            None
        } else {
            Some(deserialize_binary_array_str(row.get_binary(16)?)?)
        };
        let external_path = if row.is_null_at(17) {
            None
        } else {
            Some(
                String::from_utf8(row.get_binary(17)?.to_vec())
                    .map_err(|_| data_file_err("external_path is not valid UTF-8"))?,
            )
        };
        let first_row_id = if row.is_null_at(18) {
            None
        } else {
            Some(row.get_long(18)?)
        };
        let write_cols = if row.is_null_at(19) {
            None
        } else {
            Some(deserialize_binary_array_str(row.get_binary(19)?)?)
        };
        let column_max_sequence_numbers = match layout {
            DataFileMetaRowLayout::WriteCols => None,
            DataFileMetaRowLayout::WriteColsSequences if row.is_null_at(20) => None,
            DataFileMetaRowLayout::WriteColsSequences => Some(read_non_null_long_array(&row, 20)?),
        };

        Ok(DataFileMeta {
            file_name,
            file_size,
            row_count,
            min_key,
            max_key,
            key_stats,
            value_stats,
            min_sequence_number,
            max_sequence_number,
            schema_id,
            level,
            extra_files,
            creation_time,
            delete_row_count,
            embedded_index,
            file_source,
            value_stats_cols,
            external_path,
            first_row_id,
            write_cols,
            column_max_sequence_numbers,
        })
    }

    /// Full path for this data file.
    ///
    /// Mirrors Java `DataFilePathFactory#toPath(DataFileMeta)`: use
    /// `_EXTERNAL_PATH` when present, otherwise align the file name under the
    /// split/bucket path.
    pub fn data_file_path(&self, bucket_path: &str) -> String {
        self.external_path
            .clone()
            .unwrap_or_else(|| join_path(bucket_path, &self.file_name))
    }

    /// Parent directory of `_EXTERNAL_PATH`, if present.
    ///
    /// Mirrors Java `DataFileMeta#externalPathDir`.
    pub fn external_path_dir(&self) -> Option<&str> {
        self.external_path.as_deref().and_then(parent_path)
    }

    /// Full path for an extra file whose location should be aligned with this data file.
    ///
    /// Mirrors Java `DataFilePathFactory#toAlignedPath`: sidecars live beside
    /// the external data file when `_EXTERNAL_PATH` is present, otherwise under
    /// the data file's bucket path.
    pub fn aligned_file_path(&self, bucket_path: &str, file_name: &str) -> String {
        let parent = self.external_path_dir().unwrap_or(bucket_path);
        join_path(parent, file_name)
    }

    /// File name for the default independent file-index sidecar of this data file.
    ///
    /// Mirrors Java `DataFilePathFactory#dataFileToFileIndexPath`, which appends
    /// `.index` to the full data-file name.
    pub fn data_file_index_file_name(&self) -> String {
        data_file_to_file_index_file_name(&self.file_name)
    }

    /// Full path for the default independent file-index sidecar of this data file.
    pub fn data_file_index_path(&self, bucket_path: &str) -> String {
        self.aligned_file_path(bucket_path, &self.data_file_index_file_name())
    }

    /// Data file plus all extra files as physical paths.
    ///
    /// Mirrors Java `DataFileMeta#collectFiles`.
    pub fn collect_files(&self, bucket_path: &str) -> Vec<String> {
        let mut files = Vec::with_capacity(1 + self.extra_files.len());
        files.push(self.data_file_path(bucket_path));
        files.extend(
            self.extra_files
                .iter()
                .map(|extra| self.aligned_file_path(bucket_path, extra)),
        );
        files
    }
}

pub fn data_file_to_file_index_file_name(file_name: &str) -> String {
    format!("{file_name}{DATA_FILE_INDEX_SUFFIX}")
}

fn join_path(parent: &str, file_name: &str) -> String {
    let parent = parent.trim_end_matches('/');
    if parent.is_empty() {
        file_name.to_string()
    } else if parent == "/" {
        format!("/{file_name}")
    } else {
        format!("{parent}/{file_name}")
    }
}

fn parent_path(path: &str) -> Option<&str> {
    let trimmed = path.trim_end_matches('/');
    let idx = trimmed.rfind('/')?;
    if idx == 0 {
        Some("/")
    } else {
        Some(&trimmed[..idx])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{DataType, IntType};

    fn data_file(file_name: &str) -> DataFileMeta {
        let stats = BinaryTableStats::empty();
        DataFileMeta {
            file_name: file_name.to_string(),
            file_size: 42,
            row_count: 7,
            min_key: vec![1],
            max_key: vec![9],
            key_stats: stats.clone(),
            value_stats: stats,
            min_sequence_number: 3,
            max_sequence_number: 5,
            schema_id: 11,
            level: 2,
            extra_files: Vec::new(),
            creation_time: DateTime::from_timestamp_millis(1_234),
            delete_row_count: Some(1),
            embedded_index: Some(vec![4, 5]),
            file_source: Some(6),
            value_stats_cols: Some(vec!["v".to_string()]),
            external_path: None,
            first_row_id: Some(100),
            write_cols: Some(vec!["k".to_string(), "v".to_string()]),
            column_max_sequence_numbers: None,
        }
    }

    fn int_stats(min: Option<i32>, max: Option<i32>, null_count: Option<i64>) -> BinaryTableStats {
        let data_type = DataType::Int(IntType::new());
        let min = min.map(Datum::Int);
        let max = max.map(Datum::Int);
        BinaryTableStats::new(
            BinaryRow::from_datums(&[(min.as_ref(), &data_type)]).to_serialized_bytes(),
            BinaryRow::from_datums(&[(max.as_ref(), &data_type)]).to_serialized_bytes(),
            vec![null_count],
        )
    }

    fn int_field(id: i32, name: &str) -> DataField {
        DataField::new(id, name.to_string(), DataType::Int(IntType::new()))
    }

    fn sample_full_data_file_meta() -> DataFileMeta {
        let stats = BinaryTableStats::empty();
        DataFileMeta {
            file_name: "data-full.parquet".to_string(),
            file_size: 1024,
            row_count: 42,
            min_key: vec![1, 2, 3],
            max_key: vec![9, 8, 7],
            key_stats: stats.clone(),
            value_stats: stats,
            min_sequence_number: 10,
            max_sequence_number: 20,
            schema_id: 5,
            level: 3,
            extra_files: vec!["extra-1".to_string(), "extra-2".to_string()],
            creation_time: DateTime::from_timestamp_millis(1_700_000_000_123),
            delete_row_count: Some(4),
            embedded_index: Some(vec![7, 8, 9]),
            file_source: Some(0),
            value_stats_cols: Some(vec!["a".to_string(), "b".to_string()]),
            external_path: Some("s3://bucket/data-full.parquet".to_string()),
            first_row_id: Some(1_000),
            write_cols: Some(vec!["a".to_string(), "b".to_string(), "c".to_string()]),
            column_max_sequence_numbers: None,
        }
    }

    fn sample_minimal_data_file_meta() -> DataFileMeta {
        let stats = BinaryTableStats::empty();
        DataFileMeta {
            file_name: "data-min.parquet".to_string(),
            file_size: 0,
            row_count: 0,
            min_key: Vec::new(),
            max_key: Vec::new(),
            key_stats: stats.clone(),
            value_stats: stats,
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id: 0,
            level: 0,
            extra_files: Vec::new(),
            creation_time: None,
            delete_row_count: None,
            embedded_index: None,
            file_source: None,
            value_stats_cols: None,
            external_path: None,
            first_row_id: None,
            write_cols: None,
            column_max_sequence_numbers: None,
        }
    }

    #[test]
    fn creation_time_is_local_wall_time_in_every_wire_format() {
        use crate::common::date_time::local_timestamp;
        for (zone, instant, expected_wall) in [
            (
                "UTC",
                "1969-12-31T23:59:59.999Z",
                "1969-12-31T23:59:59.999Z",
            ),
            (
                "Asia/Shanghai",
                "2024-01-02T04:34:56.789Z",
                "2024-01-02T12:34:56.789Z",
            ),
            (
                "America/New_York",
                "2024-01-02T17:34:56.789Z",
                "2024-01-02T12:34:56.789Z",
            ),
            (
                "America/New_York",
                "2024-11-03T05:30:00.456Z",
                "2024-11-03T01:30:00.456Z",
            ),
        ] {
            let zone = jiff::tz::TimeZone::get(zone).unwrap();
            let instant = instant.parse::<DateTime<Utc>>().unwrap();
            let wall = expected_wall.parse::<DateTime<Utc>>().unwrap();
            let mut metadata = sample_full_data_file_meta();
            metadata.creation_time = Some(local_timestamp(instant, &zone));
            assert_eq!(metadata.creation_time, Some(wall));
            for decoded in [
                serde_json::from_slice::<DataFileMeta>(&serde_json::to_vec(&metadata).unwrap())
                    .unwrap(),
                DataFileMeta::from_serialized_row_data(
                    &metadata.to_serialized_row_data().unwrap(),
                    DataFileMetaRowLayout::CURRENT,
                )
                .unwrap(),
            ] {
                assert_eq!(decoded.creation_time, Some(wall));
                assert_eq!(
                    decoded.creation_time_epoch_millis_in_zone(&zone).unwrap(),
                    Some(instant.timestamp_millis())
                );
            }
        }
        assert_eq!(
            sample_minimal_data_file_meta()
                .creation_time_epoch_millis()
                .unwrap(),
            None
        );
    }

    #[test]
    fn data_file_meta_row_data_round_trips() {
        for meta in [
            sample_full_data_file_meta(),
            sample_minimal_data_file_meta(),
        ] {
            let bytes = meta.to_serialized_row_data().unwrap();
            let back =
                DataFileMeta::from_serialized_row_data(&bytes, DataFileMetaRowLayout::CURRENT)
                    .unwrap();
            assert_eq!(back, meta);
        }
    }

    #[test]
    fn column_max_sequence_numbers_round_trip() {
        let mut meta = sample_full_data_file_meta();
        meta.column_max_sequence_numbers = Some(vec![15, 100, 150, 200]);
        let bytes = meta.to_serialized_row_data().unwrap();
        let back =
            DataFileMeta::from_serialized_row_data(&bytes, DataFileMetaRowLayout::CURRENT).unwrap();
        assert_eq!(
            back.column_max_sequence_numbers,
            Some(vec![15, 100, 150, 200])
        );
        assert_eq!(back, meta);
    }

    #[test]
    fn empty_column_max_sequence_numbers_round_trips() {
        let mut meta = sample_full_data_file_meta();
        meta.column_max_sequence_numbers = Some(Vec::new());
        let bytes = meta.to_serialized_row_data().unwrap();
        let back =
            DataFileMeta::from_serialized_row_data(&bytes, DataFileMetaRowLayout::CURRENT).unwrap();
        // An empty array must stay distinguishable from an absent one.
        assert_eq!(back.column_max_sequence_numbers, Some(Vec::new()));
        assert_eq!(back, meta);
    }

    #[test]
    fn row_shorter_than_the_layout_fixed_part_is_rejected() {
        let meta = sample_full_data_file_meta();
        let bytes = meta.to_serialized_row_data().unwrap();
        let fixed_part =
            BinaryRow::cal_fix_part_size_in_bytes(DataFileMetaRowLayout::CURRENT.arity() as i32)
                as usize;

        for truncated in [0, fixed_part - 1] {
            assert!(
                DataFileMeta::from_serialized_row_data(
                    &bytes[..truncated],
                    DataFileMetaRowLayout::CURRENT
                )
                .is_err(),
                "a {truncated}-byte row must not decode as the current layout"
            );
        }
        assert!(
            DataFileMeta::from_serialized_row_data(&bytes, DataFileMetaRowLayout::CURRENT).is_ok()
        );
    }

    /// The legacy layout has no slot for the field, so decoding a row as `WriteCols`
    /// yields `None` rather than reading past the fields that layout declares. Java does
    /// the same: `DataFileMetaSerializerTest#testLegacySerializerDropsColumnSequences`
    /// round-trips through the legacy serializer and expects the field back as null.
    #[test]
    fn legacy_layout_decodes_without_column_max_sequence_numbers() {
        let mut meta = sample_full_data_file_meta();
        meta.column_max_sequence_numbers = Some(vec![7]);
        let bytes = meta.to_serialized_row_data().unwrap();

        let back = DataFileMeta::from_serialized_row_data(&bytes, DataFileMetaRowLayout::WriteCols)
            .unwrap();
        assert_eq!(back.column_max_sequence_numbers, None);
        meta.column_max_sequence_numbers = None;
        assert_eq!(back, meta);
    }

    #[test]
    fn row_id_range_rejects_non_positive_count_and_overflow() {
        let mut file = data_file("data.parquet");
        assert_eq!(file.row_id_range(), Some((100, 106)));

        file.row_count = 0;
        assert_eq!(file.row_id_range(), None);

        file.row_count = 2;
        file.first_row_id = Some(i64::MAX);
        assert_eq!(file.row_id_range(), None);
    }

    #[test]
    fn value_stats_for_field_resolves_dense_columns_and_schema() {
        let fields = vec![int_field(0, "id"), int_field(1, "v")];
        let mut file = data_file("data.parquet");
        file.value_stats = int_stats(Some(3), Some(9), Some(2));

        let stats = file.value_stats_for_field(11, &fields, &fields[1]).unwrap();
        assert_eq!(stats.min_value, Some(Datum::Int(3)));
        assert_eq!(stats.max_value, Some(Datum::Int(9)));
        assert_eq!(stats.null_count, Some(2));

        assert!(file
            .value_stats_for_field(11, &fields, &fields[0])
            .is_none());
        assert!(file
            .value_stats_for_field(12, &fields, &fields[1])
            .is_none());
        assert!(file
            .value_stats_for_field(11, &fields, &int_field(1, "other"))
            .is_none());
    }

    #[test]
    fn value_stats_for_field_rejects_inconsistent_or_malformed_stats() {
        let fields = vec![int_field(1, "v")];
        let mut file = data_file("data.parquet");
        file.row_count = 4;

        file.value_stats = int_stats(Some(1), Some(4), Some(5));
        assert!(file
            .value_stats_for_field(11, &fields, &fields[0])
            .is_none());

        file.value_stats = int_stats(Some(4), Some(1), Some(0));
        assert!(file
            .value_stats_for_field(11, &fields, &fields[0])
            .is_none());

        file.value_stats = int_stats(Some(1), Some(4), Some(4));
        assert!(file
            .value_stats_for_field(11, &fields, &fields[0])
            .is_none());

        file.value_stats = BinaryTableStats::new(vec![0], vec![0], vec![Some(0)]);
        assert!(file
            .value_stats_for_field(11, &fields, &fields[0])
            .is_none());

        file.value_stats = BinaryTableStats::new(
            BinaryRow::new(0).to_serialized_bytes(),
            BinaryRow::new(0).to_serialized_bytes(),
            vec![Some(4)],
        );
        assert!(file
            .value_stats_for_field(11, &fields, &fields[0])
            .is_none());
    }

    #[test]
    fn display_includes_all_data_file_meta_fields() {
        let file = DataFileMeta {
            extra_files: vec!["extra-1".to_string()],
            external_path: Some("s3://bucket/data-1.parquet".to_string()),
            ..data_file("data-1.parquet")
        };

        let display = file.to_string();
        for expected in [
            "DataFileMeta{fileName=data-1.parquet",
            "fileSize=42",
            "rowCount=7",
            "embeddedIndex=Some([4, 5])",
            "minKey=[1]",
            "maxKey=[9]",
            "keyStats=BinaryTableStats",
            "valueStats=BinaryTableStats",
            "minSequenceNumber=3",
            "maxSequenceNumber=5",
            "schemaId=11",
            "level=2",
            "extraFiles=[\"extra-1\"]",
            "creationTime=Some(1970-01-01T00:00:01.234Z)",
            "deleteRowCount=Some(1)",
            "fileSource=Some(6)",
            "valueStatsCols=Some([\"v\"])",
            "externalPath=Some(\"s3://bucket/data-1.parquet\")",
            "firstRowId=Some(100)",
            "writeCols=Some([\"k\", \"v\"])",
            "columnMaxSequenceNumbers=None",
        ] {
            assert!(
                display.contains(expected),
                "Display output missing `{expected}`: {display}"
            );
        }
    }

    #[test]
    fn data_file_paths_align_to_bucket_path() {
        let mut file = data_file("data-0.row");
        file.extra_files = vec!["data-0.row.index".to_string(), "data-0.row.dv".to_string()];

        assert_eq!(
            file.data_file_path("s3://warehouse/table/bucket-0"),
            "s3://warehouse/table/bucket-0/data-0.row"
        );
        assert_eq!(
            file.aligned_file_path("s3://warehouse/table/bucket-0/", "data-0.row.index"),
            "s3://warehouse/table/bucket-0/data-0.row.index"
        );
        assert_eq!(file.data_file_index_file_name(), "data-0.row.index");
        assert_eq!(
            file.data_file_index_path("s3://warehouse/table/bucket-0"),
            "s3://warehouse/table/bucket-0/data-0.row.index"
        );
        assert_eq!(
            file.collect_files("s3://warehouse/table/bucket-0"),
            vec![
                "s3://warehouse/table/bucket-0/data-0.row".to_string(),
                "s3://warehouse/table/bucket-0/data-0.row.index".to_string(),
                "s3://warehouse/table/bucket-0/data-0.row.dv".to_string(),
            ]
        );
    }

    #[test]
    fn data_file_paths_align_extra_files_to_external_path_parent() {
        let mut file = data_file("data-0.row");
        file.external_path = Some("s3://external/table/data-0.row".to_string());
        file.extra_files = vec!["data-0.row.index".to_string()];

        assert_eq!(
            file.data_file_path("s3://warehouse/table/bucket-0"),
            "s3://external/table/data-0.row"
        );
        assert_eq!(file.external_path_dir(), Some("s3://external/table"));
        assert_eq!(
            file.data_file_index_path("s3://warehouse/table/bucket-0"),
            "s3://external/table/data-0.row.index"
        );
        assert_eq!(
            file.collect_files("s3://warehouse/table/bucket-0"),
            vec![
                "s3://external/table/data-0.row".to_string(),
                "s3://external/table/data-0.row.index".to_string(),
            ]
        );
    }

    #[test]
    fn data_file_to_file_index_file_name_appends_java_suffix() {
        assert_eq!(
            data_file_to_file_index_file_name("part-0.row"),
            "part-0.row.index"
        );
    }
}
