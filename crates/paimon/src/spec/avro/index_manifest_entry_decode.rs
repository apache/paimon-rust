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
    read_nullable_string_field, read_optional_long, read_string_field,
};
use super::schema::{skip_nullable_field, FieldSchema, WriterSchema};
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
        let mut file_size: Option<i64> = None;
        let mut row_count: Option<i64> = None;
        let mut deletion_vectors_ranges: Option<IndexMap<String, DeletionVectorMeta>> = None;
        let mut external_path: Option<String> = None;
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
                "_FILE_SIZE" => file_size = Some(read_long_field(cursor, field.nullable)?),
                "_ROW_COUNT" => row_count = Some(read_long_field(cursor, field.nullable)?),
                "_DELETIONS_VECTORS_RANGES" | "_DELETION_VECTORS_RANGES" => {
                    deletion_vectors_ranges = decode_nullable_dv_ranges(
                        cursor,
                        &field.name,
                        &field.schema,
                        field.nullable,
                    )?;
                }
                "_EXTERNAL_PATH" => {
                    external_path = read_nullable_string_field(cursor, field.nullable)?;
                }
                "_GLOBAL_INDEX" => {
                    global_index_meta =
                        decode_nullable_global_index(cursor, field.nullable, &field.schema)?;
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
                external_path,
                global_index_meta,
            },
        })
    }
}

/// Peel `array<["null", record]>` down to the item record's writer schema.
///
/// `WriterSchema::parse` unwraps a *field*'s nullable union, so the outer
/// `["null", array]` is already gone, but array items keep theirs — Java builds
/// the element with `RowType.of`, which is nullable, so the schema here is
/// `Array(Union([Null, Record]))` and `extract_record_schema` alone cannot reach
/// the record.
///
/// `f0`, `f1` and `f2` hold the data file name, offset and length, and Java has
/// always declared all three non-null. `f0` is the map key, so defaulting it
/// would silently collapse every item of an entry onto one key; reject the
/// schema instead, which is also what the serde reader does with such a file.
fn dv_item_record_schema<'a>(
    field_name: &str,
    schema: &'a FieldSchema,
) -> crate::Result<&'a WriterSchema> {
    let err = |detail: String| crate::Error::UnexpectedError {
        message: format!("avro decode: {field_name} {detail}"),
        source: None,
    };
    let record = match schema {
        FieldSchema::Array(item) => match item.as_ref() {
            FieldSchema::Union(branches) => branches.iter().find_map(extract_record_schema),
            _ => None,
        },
        _ => None,
    }
    .ok_or_else(|| err("is not an array of nullable records".to_owned()))?;
    for required in ["f0", "f1", "f2"] {
        if !record.fields.iter().any(|f| f.name == required) {
            return Err(err(format!("item record has no `{required}` field")));
        }
    }
    Ok(record)
}

fn decode_nullable_dv_ranges(
    cursor: &mut AvroCursor,
    field_name: &str,
    schema: &FieldSchema,
    nullable: bool,
) -> crate::Result<Option<IndexMap<String, DeletionVectorMeta>>> {
    if nullable {
        let idx = cursor.read_union_index()?;
        if idx == 0 {
            return Ok(None);
        }
    }
    // `_CARDINALITY` only exists in writer schemas from 1.0.0 on, where Java
    // pointed this field at `DeletionVectorMeta.SCHEMA` (#4699); 0.8.0 through
    // 0.9.x declared the item as `RowType.of(STRING, INT, INT)`, i.e. `f0`/`f1`/
    // `f2` and nothing else. Walk the writer's own field list so an older record
    // does not consume the next item's bytes, and so a future field appended to
    // `DeletionVectorMeta.SCHEMA` is skipped rather than misread.
    let item_schema = dv_item_record_schema(field_name, schema)?;
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
            // `f0`/`f1`/`f2` are known present — `dv_item_record_schema` rejects
            // an item record missing any of them — so these defaults never survive.
            let mut f0 = String::new();
            let mut f1 = 0;
            let mut f2 = 0;
            let mut cardinality = None;
            for field in &item_schema.fields {
                match field.name.as_str() {
                    "f0" => f0 = read_string_field(cursor, field.nullable)?,
                    "f1" => f1 = read_int_field(cursor, field.nullable)?,
                    "f2" => f2 = read_int_field(cursor, field.nullable)?,
                    "_CARDINALITY" => cardinality = read_optional_long(cursor, field.nullable)?,
                    _ => skip_nullable_field(cursor, &field.schema, field.nullable)?,
                }
            }
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
    schema: &super::schema::FieldSchema,
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

    // _SOURCE_META: nullable bytes — only present in >= #8549 writer schemas.
    // Guard on the writer's nested field list so a legacy 5-field _GLOBAL_INDEX
    // record does not misalign the cursor into the next record.
    let has_source_meta = extract_record_schema(schema)
        .map(|s| s.fields.iter().any(|f| f.name == "_SOURCE_META"))
        .unwrap_or(false);
    let source_meta = if has_source_meta {
        let u_idx = cursor.read_union_index()?;
        if u_idx == 0 {
            None
        } else {
            Some(cursor.read_bytes()?.to_vec())
        }
    } else {
        None
    };

    Ok(Some(GlobalIndexMeta {
        row_range_start,
        row_range_end,
        index_field_id,
        extra_field_ids,
        index_meta,
        source_meta,
    }))
}
