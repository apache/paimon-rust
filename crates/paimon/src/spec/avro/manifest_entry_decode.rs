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

use super::cursor::AvroCursor;
use super::decode::{neg_count_to_usize, AvroRecordDecode};
use super::decode_helpers::{
    extract_record_schema, normalize_partition, read_bytes_field, read_int_field, read_long_field,
    read_string_field,
};
use super::manifest_file_meta_decode::decode_nullable_binary_table_stats;
use super::schema::{skip_nullable_field, FieldSchema, WriterSchema};
use crate::spec::manifest_common::FileKind;
use crate::spec::stats::BinaryTableStats;
use crate::spec::DataFileMeta;
use crate::spec::ManifestEntry;
use chrono::{DateTime, Utc};

impl AvroRecordDecode for ManifestEntry {
    fn decode(cursor: &mut AvroCursor, writer_schema: &WriterSchema) -> crate::Result<Self> {
        let mut kind: Option<FileKind> = None;
        let mut partition: Option<Vec<u8>> = None;
        let mut bucket: Option<i32> = None;
        let mut total_buckets: Option<i32> = None;
        let mut file: Option<DataFileMeta> = None;
        let mut version: Option<i32> = None;

        for field in &writer_schema.fields {
            match field.name.as_str() {
                "_KIND" => {
                    let v = read_int_field(cursor, field.nullable)?;
                    kind = Some(match v {
                        0 => FileKind::Add,
                        1 => FileKind::Delete,
                        _ => {
                            return Err(crate::Error::UnexpectedError {
                                message: format!("unknown FileKind: {v}"),
                                source: None,
                            })
                        }
                    });
                }
                "_PARTITION" => partition = Some(read_bytes_field(cursor, field.nullable)?),
                "_BUCKET" => bucket = Some(read_int_field(cursor, field.nullable)?),
                "_TOTAL_BUCKETS" => total_buckets = Some(read_int_field(cursor, field.nullable)?),
                "_FILE" => {
                    file = decode_nullable_data_file_meta(cursor, &field.schema, field.nullable)?;
                }
                "_VERSION" => version = Some(read_int_field(cursor, field.nullable)?),
                _ => skip_nullable_field(cursor, &field.schema, field.nullable)?,
            }
        }

        Ok(ManifestEntry::new(
            kind.unwrap_or(FileKind::Add),
            normalize_partition(partition),
            bucket.unwrap_or(0),
            total_buckets.unwrap_or(0),
            file.unwrap_or_else(default_data_file_meta),
            version.unwrap_or(0),
        ))
    }
}

/// Decode ManifestEntry records with a filter applied on lightweight fields.
///
/// Decodes only _KIND, _PARTITION, _BUCKET, _TOTAL_BUCKETS, _VERSION first.
/// If `filter` returns false, skips the expensive _FILE (DataFileMeta) decoding.
/// Returns only entries that pass the filter.
pub(crate) fn decode_manifest_entries_filtered<F>(
    cursor: &mut AvroCursor,
    writer_schema: &WriterSchema,
    is_union_wrapped: bool,
    filter: &mut F,
) -> crate::Result<Option<ManifestEntry>>
where
    F: FnMut(FileKind, &[u8], i32, i32) -> bool,
{
    if is_union_wrapped {
        let idx = cursor.read_union_index()?;
        if idx == 0 {
            return Err(crate::Error::UnexpectedError {
                message: "avro decode: unexpected null in top-level union".into(),
                source: None,
            });
        }
    }

    // Two-pass decode: first collect lightweight fields and record _FILE position,
    // then conditionally decode _FILE.
    let mut kind: Option<FileKind> = None;
    let mut partition: Option<Vec<u8>> = None;
    let mut bucket: Option<i32> = None;
    let mut total_buckets: Option<i32> = None;
    let mut version: Option<i32> = None;
    let mut file: Option<DataFileMeta> = None;
    let mut file_skipped = false;

    for field in &writer_schema.fields {
        match field.name.as_str() {
            "_KIND" => {
                let v = read_int_field(cursor, field.nullable)?;
                kind = Some(match v {
                    0 => FileKind::Add,
                    1 => FileKind::Delete,
                    _ => {
                        return Err(crate::Error::UnexpectedError {
                            message: format!("unknown FileKind: {v}"),
                            source: None,
                        })
                    }
                });
            }
            "_PARTITION" => partition = Some(read_bytes_field(cursor, field.nullable)?),
            "_BUCKET" => bucket = Some(read_int_field(cursor, field.nullable)?),
            "_TOTAL_BUCKETS" => total_buckets = Some(read_int_field(cursor, field.nullable)?),
            "_FILE" => {
                let can_filter = kind.is_some()
                    && partition.is_some()
                    && bucket.is_some()
                    && total_buckets.is_some();
                if can_filter {
                    let k = kind.unwrap_or(FileKind::Add);
                    let p = partition.as_deref().unwrap_or(&[]);
                    let b = bucket.unwrap_or(0);
                    let tb = total_buckets.unwrap_or(0);
                    if filter(k, p, b, tb) {
                        file =
                            decode_nullable_data_file_meta(cursor, &field.schema, field.nullable)?;
                    } else {
                        skip_nullable_field(cursor, &field.schema, field.nullable)?;
                        file_skipped = true;
                    }
                } else {
                    file = decode_nullable_data_file_meta(cursor, &field.schema, field.nullable)?;
                }
            }
            "_VERSION" => version = Some(read_int_field(cursor, field.nullable)?),
            _ => skip_nullable_field(cursor, &field.schema, field.nullable)?,
        }
    }

    if file_skipped {
        return Ok(None);
    }

    Ok(Some(ManifestEntry::new(
        kind.unwrap_or(FileKind::Add),
        normalize_partition(partition),
        bucket.unwrap_or(0),
        total_buckets.unwrap_or(0),
        file.unwrap_or_else(default_data_file_meta),
        version.unwrap_or(0),
    )))
}

fn decode_nullable_data_file_meta(
    cursor: &mut AvroCursor,
    field_schema: &FieldSchema,
    nullable: bool,
) -> crate::Result<Option<DataFileMeta>> {
    if nullable {
        let idx = cursor.read_union_index()?;
        if idx == 0 {
            return Ok(None);
        }
    }
    let record_schema =
        extract_record_schema(field_schema).ok_or_else(|| crate::Error::UnexpectedError {
            message: "avro decode: _FILE field is not a record".into(),
            source: None,
        })?;
    decode_data_file_meta(cursor, record_schema).map(Some)
}

/// Read string array, handling both `{"type":"array",...}` and `["null", {"type":"array",...}]`.
fn read_string_array_field(cursor: &mut AvroCursor, nullable: bool) -> crate::Result<Vec<String>> {
    if nullable {
        let idx = cursor.read_union_index()?;
        if idx == 0 {
            return Ok(vec![]);
        }
    }
    decode_string_array(cursor)
}

fn decode_data_file_meta(
    cursor: &mut AvroCursor,
    writer_schema: &WriterSchema,
) -> crate::Result<DataFileMeta> {
    let mut file_name: Option<String> = None;
    let mut file_size: Option<i64> = None;
    let mut row_count: Option<i64> = None;
    let mut min_key: Option<Vec<u8>> = None;
    let mut max_key: Option<Vec<u8>> = None;
    let mut key_stats: Option<BinaryTableStats> = None;
    let mut value_stats: Option<BinaryTableStats> = None;
    let mut min_sequence_number: Option<i64> = None;
    let mut max_sequence_number: Option<i64> = None;
    let mut schema_id: Option<i64> = None;
    let mut level: Option<i32> = None;
    let mut extra_files: Option<Vec<String>> = None;
    let mut creation_time: Option<DateTime<Utc>> = None;
    let mut delete_row_count: Option<i64> = None;
    let mut embedded_index: Option<Vec<u8>> = None;
    let mut file_source: Option<i32> = None;
    let mut value_stats_cols: Option<Vec<String>> = None;
    let mut external_path: Option<String> = None;
    let mut first_row_id: Option<i64> = None;
    let mut write_cols: Option<Vec<String>> = None;

    for field in &writer_schema.fields {
        match field.name.as_str() {
            "_FILE_NAME" => file_name = Some(read_string_field(cursor, field.nullable)?),
            "_FILE_SIZE" => file_size = Some(read_long_field(cursor, field.nullable)?),
            "_ROW_COUNT" => row_count = Some(read_long_field(cursor, field.nullable)?),
            "_MIN_KEY" => min_key = Some(read_bytes_field(cursor, field.nullable)?),
            "_MAX_KEY" => max_key = Some(read_bytes_field(cursor, field.nullable)?),
            "_KEY_STATS" => {
                key_stats =
                    decode_nullable_binary_table_stats(cursor, &field.schema, field.nullable)?
            }
            "_VALUE_STATS" => {
                value_stats =
                    decode_nullable_binary_table_stats(cursor, &field.schema, field.nullable)?
            }
            "_MIN_SEQUENCE_NUMBER" => {
                min_sequence_number = Some(read_long_field(cursor, field.nullable)?)
            }
            "_MAX_SEQUENCE_NUMBER" => {
                max_sequence_number = Some(read_long_field(cursor, field.nullable)?)
            }
            "_SCHEMA_ID" => schema_id = Some(read_long_field(cursor, field.nullable)?),
            "_LEVEL" => level = Some(read_int_field(cursor, field.nullable)?),
            "_EXTRA_FILES" => extra_files = Some(read_string_array_field(cursor, field.nullable)?),
            "_CREATION_TIME" => {
                creation_time = decode_nullable_timestamp_millis(cursor, field.nullable)?
            }
            "_DELETE_ROW_COUNT" => delete_row_count = decode_nullable_long(cursor, field.nullable)?,
            "_EMBEDDED_FILE_INDEX" => {
                embedded_index = decode_nullable_bytes(cursor, field.nullable)?
            }
            "_FILE_SOURCE" => file_source = decode_nullable_int(cursor, field.nullable)?,
            "_VALUE_STATS_COLS" => {
                value_stats_cols = decode_nullable_string_array(cursor, field.nullable)?
            }
            "_EXTERNAL_PATH" => external_path = decode_nullable_string(cursor, field.nullable)?,
            "_FIRST_ROW_ID" => first_row_id = decode_nullable_long(cursor, field.nullable)?,
            "_WRITE_COLS" => write_cols = decode_nullable_string_array(cursor, field.nullable)?,
            _ => skip_nullable_field(cursor, &field.schema, field.nullable)?,
        }
    }

    Ok(DataFileMeta {
        file_name: file_name.unwrap_or_default(),
        file_size: file_size.unwrap_or(0),
        row_count: row_count.unwrap_or(0),
        min_key: min_key.unwrap_or_default(),
        max_key: max_key.unwrap_or_default(),
        key_stats: key_stats.unwrap_or_else(|| BinaryTableStats::new(vec![], vec![], vec![])),
        value_stats: value_stats.unwrap_or_else(|| BinaryTableStats::new(vec![], vec![], vec![])),
        min_sequence_number: min_sequence_number.unwrap_or(0),
        max_sequence_number: max_sequence_number.unwrap_or(0),
        schema_id: schema_id.unwrap_or(0),
        level: level.unwrap_or(0),
        extra_files: extra_files.unwrap_or_default(),
        creation_time,
        delete_row_count,
        embedded_index,
        file_source,
        value_stats_cols,
        external_path,
        first_row_id,
        write_cols,
    })
}

fn decode_string_array(cursor: &mut AvroCursor) -> crate::Result<Vec<String>> {
    let mut result = Vec::new();
    loop {
        let count = cursor.read_long()?;
        if count == 0 {
            break;
        }
        let count = if count < 0 {
            cursor.skip_long()?;
            neg_count_to_usize(count)?
        } else {
            count as usize
        };
        result.reserve(count);
        for _ in 0..count {
            result.push(cursor.read_string()?.to_string());
        }
    }
    Ok(result)
}

fn decode_nullable_long(cursor: &mut AvroCursor, nullable: bool) -> crate::Result<Option<i64>> {
    if nullable {
        let idx = cursor.read_union_index()?;
        if idx == 0 {
            return Ok(None);
        }
    }
    Ok(Some(cursor.read_long()?))
}

fn decode_nullable_int(cursor: &mut AvroCursor, nullable: bool) -> crate::Result<Option<i32>> {
    if nullable {
        let idx = cursor.read_union_index()?;
        if idx == 0 {
            return Ok(None);
        }
    }
    Ok(Some(cursor.read_int()?))
}

fn decode_nullable_bytes(
    cursor: &mut AvroCursor,
    nullable: bool,
) -> crate::Result<Option<Vec<u8>>> {
    if nullable {
        let idx = cursor.read_union_index()?;
        if idx == 0 {
            return Ok(None);
        }
    }
    Ok(Some(cursor.read_bytes()?.to_vec()))
}

fn decode_nullable_string(
    cursor: &mut AvroCursor,
    nullable: bool,
) -> crate::Result<Option<String>> {
    if nullable {
        let idx = cursor.read_union_index()?;
        if idx == 0 {
            return Ok(None);
        }
    }
    Ok(Some(cursor.read_string()?.to_string()))
}

fn decode_nullable_string_array(
    cursor: &mut AvroCursor,
    nullable: bool,
) -> crate::Result<Option<Vec<String>>> {
    if nullable {
        let idx = cursor.read_union_index()?;
        if idx == 0 {
            return Ok(None);
        }
    }
    Ok(Some(decode_string_array(cursor)?))
}

fn decode_nullable_timestamp_millis(
    cursor: &mut AvroCursor,
    nullable: bool,
) -> crate::Result<Option<DateTime<Utc>>> {
    if nullable {
        let idx = cursor.read_union_index()?;
        if idx == 0 {
            return Ok(None);
        }
    }
    let millis = cursor.read_long()?;
    let secs = millis.div_euclid(1000);
    let nanos = (millis.rem_euclid(1000) * 1_000_000) as u32;
    Ok(DateTime::from_timestamp(secs, nanos))
}

fn default_data_file_meta() -> DataFileMeta {
    DataFileMeta {
        file_name: String::new(),
        file_size: 0,
        row_count: 0,
        min_key: vec![],
        max_key: vec![],
        key_stats: BinaryTableStats::new(vec![], vec![], vec![]),
        value_stats: BinaryTableStats::new(vec![], vec![], vec![]),
        min_sequence_number: 0,
        max_sequence_number: 0,
        schema_id: 0,
        level: 0,
        extra_files: vec![],
        creation_time: None,
        delete_row_count: None,
        embedded_index: None,
        file_source: None,
        value_stats_cols: None,
        external_path: None,
        first_row_id: None,
        write_cols: None,
    }
}
