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
    extract_record_schema, read_bytes_field, read_int_field, read_long_field, read_string_field,
};
use super::schema::{skip_nullable_field, FieldSchema, WriterSchema};
use crate::spec::stats::BinaryTableStats;
use crate::spec::ManifestFileMeta;

impl AvroRecordDecode for ManifestFileMeta {
    fn decode(cursor: &mut AvroCursor, writer_schema: &WriterSchema) -> crate::Result<Self> {
        let mut version: Option<i32> = None;
        let mut file_name: Option<String> = None;
        let mut file_size: Option<i64> = None;
        let mut num_added_files: Option<i64> = None;
        let mut num_deleted_files: Option<i64> = None;
        let mut partition_stats: Option<BinaryTableStats> = None;
        let mut schema_id: Option<i64> = None;

        for field in &writer_schema.fields {
            match field.name.as_str() {
                "_VERSION" => version = Some(read_int_field(cursor, field.nullable)?),
                "_FILE_NAME" => file_name = Some(read_string_field(cursor, field.nullable)?),
                "_FILE_SIZE" => file_size = Some(read_long_field(cursor, field.nullable)?),
                "_NUM_ADDED_FILES" => {
                    num_added_files = Some(read_long_field(cursor, field.nullable)?)
                }
                "_NUM_DELETED_FILES" => {
                    num_deleted_files = Some(read_long_field(cursor, field.nullable)?)
                }
                "_PARTITION_STATS" => {
                    partition_stats =
                        decode_nullable_binary_table_stats(cursor, &field.schema, field.nullable)?;
                }
                "_SCHEMA_ID" => schema_id = Some(read_long_field(cursor, field.nullable)?),
                _ => skip_nullable_field(cursor, &field.schema, field.nullable)?,
            }
        }

        Ok(ManifestFileMeta::new_with_version(
            version.unwrap_or(2),
            file_name.unwrap_or_default(),
            file_size.unwrap_or(0),
            num_added_files.unwrap_or(0),
            num_deleted_files.unwrap_or(0),
            partition_stats.unwrap_or_else(|| BinaryTableStats::new(vec![], vec![], vec![])),
            schema_id.unwrap_or(0),
        ))
    }
}

/// Decode a nullable BinaryTableStats: union ["null", record] or direct record.
pub(crate) fn decode_nullable_binary_table_stats(
    cursor: &mut AvroCursor,
    schema: &FieldSchema,
    nullable: bool,
) -> crate::Result<Option<BinaryTableStats>> {
    if nullable {
        let idx = cursor.read_union_index()?;
        if idx == 0 {
            return Ok(None);
        }
    }
    let record_schema =
        extract_record_schema(schema).ok_or_else(|| crate::Error::UnexpectedError {
            message: "avro decode: BinaryTableStats field is not a record".into(),
            source: None,
        })?;
    let mut min_values: Option<Vec<u8>> = None;
    let mut max_values: Option<Vec<u8>> = None;
    let mut null_counts: Option<Vec<Option<i64>>> = None;
    for field in &record_schema.fields {
        match field.name.as_str() {
            "_MIN_VALUES" => min_values = Some(read_bytes_field(cursor, field.nullable)?),
            "_MAX_VALUES" => max_values = Some(read_bytes_field(cursor, field.nullable)?),
            "_NULL_COUNTS" => {
                null_counts = Some(decode_nullable_long_array(cursor, field.nullable)?)
            }
            _ => super::schema::skip_nullable_field(cursor, &field.schema, field.nullable)?,
        }
    }
    Ok(Some(BinaryTableStats::new(
        min_values.unwrap_or_default(),
        max_values.unwrap_or_default(),
        null_counts.unwrap_or_default(),
    )))
}

fn decode_nullable_long_array(
    cursor: &mut AvroCursor,
    nullable: bool,
) -> crate::Result<Vec<Option<i64>>> {
    if nullable {
        let idx = cursor.read_union_index()?;
        if idx == 0 {
            return Ok(vec![]);
        }
    }
    let mut result = Vec::new();
    loop {
        let count = cursor.read_long()?;
        if count == 0 {
            break;
        }
        let count = if count < 0 {
            cursor.skip_long()?; // block byte size
            neg_count_to_usize(count)?
        } else {
            count as usize
        };
        for _ in 0..count {
            // Each item is union ["null", "long"]
            let item_idx = cursor.read_union_index()?;
            if item_idx == 0 {
                result.push(None);
            } else {
                result.push(Some(cursor.read_long()?));
            }
        }
    }
    Ok(result)
}
