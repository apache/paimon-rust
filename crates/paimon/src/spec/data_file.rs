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
    extract_datum, serialize_binary_array_str, BinaryRow, BinaryRowBuilder, DataField, Datum,
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
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DataFileMeta{{fileName={}, fileSize={}, rowCount={}, embeddedIndex={:?}, \
             minKey={:?}, maxKey={:?}, keyStats={:?}, valueStats={:?}, \
             minSequenceNumber={}, maxSequenceNumber={}, schemaId={}, level={}, \
             extraFiles={:?}, creationTime={:?}, deleteRowCount={:?}, fileSource={:?}, \
             valueStatsCols={:?}, externalPath={:?}, firstRowId={:?}, writeCols={:?}}}",
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
        )
    }
}

fn opt_long(b: &mut BinaryRowBuilder, pos: usize, v: Option<i64>) {
    match v {
        Some(x) => b.write_long(pos, x),
        None => b.set_null_at(pos),
    }
}

fn opt_str_array(b: &mut BinaryRowBuilder, pos: usize, v: &Option<Vec<String>>) {
    match v {
        Some(a) => b.write_bytes(pos, &serialize_binary_array_str(a)),
        None => b.set_null_at(pos),
    }
}

impl DataFileMeta {
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
        self.first_row_id.map(|fid| (fid, fid + self.row_count - 1))
    }

    /// Serialize as a `DataFileMeta.SCHEMA` (version 8) BinaryRow, raw data without the
    /// arity prefix -- the form `DataSplit#serialize` writes per file as `writeInt(len) + data`.
    /// Fields, order and nullability mirror Java `DataFileMetaSerializer#toRow`.
    pub fn to_serialized_row_data(&self) -> crate::Result<Vec<u8>> {
        let mut b = BinaryRowBuilder::new(20);
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
        Ok(b.build_row_data())
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
