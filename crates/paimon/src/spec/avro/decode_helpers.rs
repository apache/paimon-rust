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
use super::schema::{FieldSchema, WriterSchema};

/// Extract the record WriterSchema from a field schema.
/// Nullable unions are already unwrapped at parse time, so this only matches direct records.
pub(crate) fn extract_record_schema(schema: &FieldSchema) -> Option<&WriterSchema> {
    match schema {
        FieldSchema::Record(ws) => Some(ws),
        _ => None,
    }
}

pub(crate) fn read_int_field(cursor: &mut AvroCursor, nullable: bool) -> crate::Result<i32> {
    if nullable {
        let idx = cursor.read_union_index()?;
        if idx == 0 {
            return Ok(0);
        }
    }
    cursor.read_int()
}

pub(crate) fn read_long_field(cursor: &mut AvroCursor, nullable: bool) -> crate::Result<i64> {
    if nullable {
        let idx = cursor.read_union_index()?;
        if idx == 0 {
            return Ok(0);
        }
    }
    cursor.read_long()
}

pub(crate) fn read_bytes_field(cursor: &mut AvroCursor, nullable: bool) -> crate::Result<Vec<u8>> {
    if nullable {
        let idx = cursor.read_union_index()?;
        if idx == 0 {
            return Ok(vec![]);
        }
    }
    Ok(cursor.read_bytes()?.to_vec())
}

pub(crate) fn read_string_field(cursor: &mut AvroCursor, nullable: bool) -> crate::Result<String> {
    if nullable {
        let idx = cursor.read_union_index()?;
        if idx == 0 {
            return Ok(String::new());
        }
    }
    Ok(cursor.read_string()?.to_string())
}

const EMPTY_PARTITION: [u8; 4] = [0, 0, 0, 0];

/// Null/missing/empty partition → valid empty BinaryRow (arity=0).
pub(crate) fn normalize_partition(partition: Option<Vec<u8>>) -> Vec<u8> {
    match partition {
        Some(p) if p.len() >= 4 => p,
        _ => EMPTY_PARTITION.to_vec(),
    }
}
