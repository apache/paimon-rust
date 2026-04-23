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
    normalize_partition, read_bytes_field, read_int_field, read_long_field, read_string_field,
};
use super::schema::{skip_nullable_field, WriterSchema};
use crate::spec::index_manifest::IndexManifestEntry;
use crate::spec::manifest_common::FileKind;
use crate::spec::{DeletionVectorMeta, GlobalIndexMeta, IndexFileMeta};
use indexmap::IndexMap;

impl AvroRecordDecode for IndexManifestEntry {
    fn decode(cursor: &mut AvroCursor, writer_schema: &WriterSchema) -> crate::Result<Self> {
        let mut version: Option<i32> = None;
        let mut kind: Option<FileKind> = None;
        let mut partition: Option<Vec<u8>> = None;
        let mut bucket: Option<i32> = None;
        let mut index_type: Option<String> = None;
        let mut file_name: Option<String> = None;
        let mut file_size: Option<i32> = None;
        let mut row_count: Option<i32> = None;
        let mut deletion_vectors_ranges: Option<IndexMap<String, DeletionVectorMeta>> = None;
        let mut global_index_meta: Option<GlobalIndexMeta> = None;

        for field in &writer_schema.fields {
            match field.name.as_str() {
                "_VERSION" => version = Some(read_int_field(cursor, field.nullable)?),
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
                "_INDEX_TYPE" => index_type = Some(read_string_field(cursor, field.nullable)?),
                "_FILE_NAME" => file_name = Some(read_string_field(cursor, field.nullable)?),
                "_FILE_SIZE" => file_size = Some(read_long_field(cursor, field.nullable)? as i32),
                "_ROW_COUNT" => row_count = Some(read_long_field(cursor, field.nullable)? as i32),
                "_DELETIONS_VECTORS_RANGES" | "_DELETION_VECTORS_RANGES" => {
                    deletion_vectors_ranges = decode_nullable_dv_ranges(cursor, field.nullable)?;
                }
                "_GLOBAL_INDEX" => {
                    global_index_meta = decode_nullable_global_index(cursor, field.nullable)?;
                }
                _ => skip_nullable_field(cursor, &field.schema, field.nullable)?,
            }
        }

        Ok(IndexManifestEntry {
            version: version.unwrap_or(1),
            kind: kind.unwrap_or(FileKind::Add),
            partition: normalize_partition(partition),
            bucket: bucket.unwrap_or(0),
            index_file: IndexFileMeta {
                index_type: index_type.unwrap_or_default(),
                file_name: file_name.unwrap_or_default(),
                file_size: file_size.unwrap_or(0),
                row_count: row_count.unwrap_or(0),
                deletion_vectors_ranges,
                global_index_meta,
            },
        })
    }
}

fn decode_nullable_dv_ranges(
    cursor: &mut AvroCursor,
    nullable: bool,
) -> crate::Result<Option<IndexMap<String, DeletionVectorMeta>>> {
    if nullable {
        let idx = cursor.read_union_index()?;
        if idx == 0 {
            return Ok(None);
        }
    }
    // Array of nullable records
    let mut map = IndexMap::new();
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
        for _ in 0..count {
            // Each item is union ["null", record]
            let item_idx = cursor.read_union_index()?;
            if item_idx == 0 {
                continue;
            }
            // Record fields: f0 (string), f1 (int), f2 (int), _CARDINALITY (nullable long)
            let f0 = cursor.read_string()?.to_string();
            let f1 = cursor.read_int()?;
            let f2 = cursor.read_int()?;
            let cardinality = {
                let c_idx = cursor.read_union_index()?;
                if c_idx == 0 {
                    None
                } else {
                    Some(cursor.read_long()?)
                }
            };
            map.insert(
                f0,
                DeletionVectorMeta {
                    offset: f1,
                    length: f2,
                    cardinality,
                },
            );
        }
    }
    Ok(Some(map))
}

fn decode_nullable_global_index(
    cursor: &mut AvroCursor,
    nullable: bool,
) -> crate::Result<Option<GlobalIndexMeta>> {
    if nullable {
        let idx = cursor.read_union_index()?;
        if idx == 0 {
            return Ok(None);
        }
    }
    let row_range_start = cursor.read_long()?;
    let row_range_end = cursor.read_long()?;
    let index_field_id = cursor.read_int()?;

    // _EXTRA_FIELD_IDS: nullable array of int
    let extra_field_ids = {
        let u_idx = cursor.read_union_index()?;
        if u_idx == 0 {
            None
        } else {
            let mut ids = Vec::new();
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
                for _ in 0..count {
                    ids.push(cursor.read_int()?);
                }
            }
            Some(ids)
        }
    };

    // _INDEX_META: nullable bytes
    let index_meta = {
        let u_idx = cursor.read_union_index()?;
        if u_idx == 0 {
            None
        } else {
            Some(cursor.read_bytes()?.to_vec())
        }
    };

    Ok(Some(GlobalIndexMeta {
        row_range_start,
        row_range_end,
        index_field_id,
        extra_field_ids,
        index_meta,
    }))
}
