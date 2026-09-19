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
    read_nullable_string_field, read_string_field,
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
                    deletion_vectors_ranges =
                        decode_nullable_dv_ranges(cursor, field.nullable, &field.schema)?;
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

fn decode_nullable_dv_ranges(
    cursor: &mut AvroCursor,
    nullable: bool,
    schema: &FieldSchema,
) -> crate::Result<Option<IndexMap<String, DeletionVectorMeta>>> {
    if nullable {
        let idx = cursor.read_union_index()?;
        if idx == 0 {
            return Ok(None);
        }
    }
    let FieldSchema::Array(item_schema) = schema else {
        return Err(crate::Error::UnexpectedError {
            message: "deletion vector ranges must be an Avro array".into(),
            source: None,
        });
    };
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
            // Java writes nullable items; PyPaimon writes plain records.
            // Reading a union tag for a plain record consumes the file-name
            // length and can silently attach the deletion vector to a wrong key.
            let item_schema = match item_schema.as_ref() {
                FieldSchema::Union(branches) => {
                    let index = cursor.read_union_index()?;
                    branches
                        .get(index as usize)
                        .ok_or_else(|| crate::Error::UnexpectedError {
                            message: format!("invalid deletion vector item union index: {index}"),
                            source: None,
                        })?
                }
                schema => schema,
            };
            if matches!(item_schema, FieldSchema::Null) {
                continue;
            }
            let FieldSchema::Record(record) = item_schema else {
                return Err(crate::Error::UnexpectedError {
                    message: "deletion vector array item must be an Avro record".into(),
                    source: None,
                });
            };
            let mut file_name = String::new();
            let mut offset = 0;
            let mut length = 0;
            let mut cardinality = None;
            for field in &record.fields {
                match field.name.as_str() {
                    "f0" => file_name = read_string_field(cursor, field.nullable)?,
                    "f1" => offset = read_int_field(cursor, field.nullable)?,
                    "f2" => length = read_int_field(cursor, field.nullable)?,
                    "_CARDINALITY" => {
                        if !field.nullable || cursor.read_union_index()? != 0 {
                            cardinality = Some(cursor.read_long()?);
                        }
                    }
                    _ => skip_nullable_field(cursor, &field.schema, field.nullable)?,
                }
            }
            map.insert(
                file_name,
                DeletionVectorMeta {
                    offset,
                    length,
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
    // Walk the writer's own field list, as the deletion-vector record above does.
    // Nothing in a manifest says how many fields this record has: Java tried a
    // runtime `getFieldCount() <= 5` check when `_SOURCE_META` was added (#8549),
    // replaced it with an entry-serializer version (#8952), then reverted to
    // `GlobalIndexMeta.SCHEMA.getFieldCount()` (#9004) and deleted the versioned
    // serializer entirely (#9039). Shape compatibility is therefore delegated to the
    // file format's schema resolution, which is exactly what positional decoding
    // cannot do — the writer's schema is the only description of the record.
    let record = extract_record_schema(schema).ok_or_else(|| crate::Error::UnexpectedError {
        message: "global index metadata must be an Avro record".into(),
        source: None,
    })?;
    let mut row_range_start = 0;
    let mut row_range_end = 0;
    let mut index_field_id = 0;
    let mut extra_field_ids = None;
    let mut index_meta = None;
    let mut source_meta = None;
    for field in &record.fields {
        match field.name.as_str() {
            "_ROW_RANGE_START" => row_range_start = read_long_field(cursor, field.nullable)?,
            "_ROW_RANGE_END" => row_range_end = read_long_field(cursor, field.nullable)?,
            "_INDEX_FIELD_ID" => index_field_id = read_int_field(cursor, field.nullable)?,
            "_EXTRA_FIELD_IDS" => {
                extra_field_ids = decode_nullable_int_array(cursor, field.nullable)?
            }
            "_INDEX_META" => index_meta = read_optional_bytes(cursor, field.nullable)?,
            "_SOURCE_META" => source_meta = read_optional_bytes(cursor, field.nullable)?,
            _ => skip_nullable_field(cursor, &field.schema, field.nullable)?,
        }
    }

    Ok(Some(GlobalIndexMeta {
        row_range_start,
        row_range_end,
        index_field_id,
        extra_field_ids,
        index_meta,
        source_meta,
    }))
}

fn read_optional_bytes(cursor: &mut AvroCursor, nullable: bool) -> crate::Result<Option<Vec<u8>>> {
    if nullable && cursor.read_union_index()? == 0 {
        return Ok(None);
    }
    Ok(Some(cursor.read_bytes()?.to_vec()))
}

fn decode_nullable_int_array(
    cursor: &mut AvroCursor,
    nullable: bool,
) -> crate::Result<Option<Vec<i32>>> {
    if nullable && cursor.read_union_index()? == 0 {
        return Ok(None);
    }
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
    Ok(Some(ids))
}
