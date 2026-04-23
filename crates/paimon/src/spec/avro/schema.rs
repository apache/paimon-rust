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

use crate::Error;
use serde_json::Value;

/// Lightweight representation of an Avro writer schema, used to build
/// field-index mappings for schema-evolution-aware decoding.
#[derive(Debug)]
pub struct WriterSchema {
    /// The top-level record fields (after unwrapping a union wrapper if present).
    pub fields: Vec<WriterField>,
    /// Whether the original schema was a union (e.g. `["null", record]`).
    pub is_union_wrapped: bool,
}

#[derive(Debug)]
pub struct WriterField {
    pub name: String,
    pub schema: FieldSchema,
    pub nullable: bool,
}

/// Simplified Avro field schema — just enough to know how to skip unknown fields.
#[derive(Debug)]
pub enum FieldSchema {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Fixed(usize),
    Enum { symbols_count: usize },
    Union(Vec<FieldSchema>),
    Array(Box<FieldSchema>),
    Map(Box<FieldSchema>),
    Record(WriterSchema),
}

impl WriterSchema {
    pub fn parse(json: &str) -> crate::Result<Self> {
        let value: Value = serde_json::from_str(json).map_err(|e| Error::UnexpectedError {
            message: format!("avro schema: invalid JSON: {e}"),
            source: None,
        })?;
        parse_record_from_value(&value)
    }
}

fn parse_record_from_value(value: &Value) -> crate::Result<WriterSchema> {
    match value {
        Value::Object(obj) if obj.get("type").and_then(|t| t.as_str()) == Some("record") => {
            let mut ws = parse_record_object(obj)?;
            ws.is_union_wrapped = false;
            Ok(ws)
        }
        Value::Array(arr) => {
            for item in arr {
                if let Value::Object(obj) = item {
                    if obj.get("type").and_then(|t| t.as_str()) == Some("record") {
                        let mut ws = parse_record_object(obj)?;
                        ws.is_union_wrapped = true;
                        return Ok(ws);
                    }
                }
            }
            Err(Error::UnexpectedError {
                message: "avro schema: union does not contain a record type".into(),
                source: None,
            })
        }
        _ => Err(Error::UnexpectedError {
            message: "avro schema: expected record or union containing record".into(),
            source: None,
        }),
    }
}

fn parse_record_object(obj: &serde_json::Map<String, Value>) -> crate::Result<WriterSchema> {
    let fields_arr = obj
        .get("fields")
        .and_then(|f| f.as_array())
        .ok_or_else(|| Error::UnexpectedError {
            message: "avro schema: record missing 'fields' array".into(),
            source: None,
        })?;

    let mut fields = Vec::with_capacity(fields_arr.len());

    for field_val in fields_arr.iter() {
        let field_obj = field_val
            .as_object()
            .ok_or_else(|| Error::UnexpectedError {
                message: "avro schema: field is not an object".into(),
                source: None,
            })?;
        let name = field_obj
            .get("name")
            .and_then(|n| n.as_str())
            .ok_or_else(|| Error::UnexpectedError {
                message: "avro schema: field missing 'name'".into(),
                source: None,
            })?
            .to_string();
        let type_val = field_obj
            .get("type")
            .ok_or_else(|| Error::UnexpectedError {
                message: format!("avro schema: field '{name}' missing 'type'"),
                source: None,
            })?;
        let schema = parse_field_schema(type_val)?;
        let (schema, nullable) = unwrap_nullable(schema);
        fields.push(WriterField {
            name,
            schema,
            nullable,
        });
    }

    Ok(WriterSchema {
        fields,
        is_union_wrapped: false,
    })
}

/// For a nullable union `["null", T]`, extract the inner type `T` and return `(T, true)`.
/// For non-nullable schemas, return `(schema, false)`.
fn unwrap_nullable(schema: FieldSchema) -> (FieldSchema, bool) {
    if let FieldSchema::Union(branches) = &schema {
        let has_null = branches.iter().any(|b| matches!(b, FieldSchema::Null));
        if has_null && branches.len() == 2 {
            let FieldSchema::Union(mut branches) = schema else {
                unreachable!()
            };
            let inner = if matches!(branches[0], FieldSchema::Null) {
                branches.swap_remove(1)
            } else {
                branches.swap_remove(0)
            };
            return (inner, true);
        }
    }
    (schema, false)
}

fn parse_field_schema(value: &Value) -> crate::Result<FieldSchema> {
    match value {
        Value::String(s) => match s.as_str() {
            "null" => Ok(FieldSchema::Null),
            "boolean" => Ok(FieldSchema::Boolean),
            "int" => Ok(FieldSchema::Int),
            "long" => Ok(FieldSchema::Long),
            "float" => Ok(FieldSchema::Float),
            "double" => Ok(FieldSchema::Double),
            "bytes" => Ok(FieldSchema::Bytes),
            "string" => Ok(FieldSchema::String),
            _ => Err(Error::UnexpectedError {
                message: format!("avro schema: unsupported named type reference: {s}"),
                source: None,
            }),
        },
        Value::Object(obj) => {
            let type_str = obj.get("type").and_then(|t| t.as_str()).unwrap_or("");
            match type_str {
                "record" => {
                    let record = parse_record_object(obj)?;
                    Ok(FieldSchema::Record(record))
                }
                "array" => {
                    let items = obj.get("items").ok_or_else(|| Error::UnexpectedError {
                        message: "avro schema: array missing 'items'".into(),
                        source: None,
                    })?;
                    let item_schema = parse_field_schema(items)?;
                    Ok(FieldSchema::Array(Box::new(item_schema)))
                }
                "map" => {
                    let values = obj.get("values").ok_or_else(|| Error::UnexpectedError {
                        message: "avro schema: map missing 'values'".into(),
                        source: None,
                    })?;
                    let value_schema = parse_field_schema(values)?;
                    Ok(FieldSchema::Map(Box::new(value_schema)))
                }
                "fixed" => {
                    let size = obj.get("size").and_then(|s| s.as_u64()).ok_or_else(|| {
                        Error::UnexpectedError {
                            message: "avro schema: fixed missing 'size'".into(),
                            source: None,
                        }
                    })? as usize;
                    Ok(FieldSchema::Fixed(size))
                }
                "long" | "int" => {
                    if type_str == "long" {
                        Ok(FieldSchema::Long)
                    } else {
                        Ok(FieldSchema::Int)
                    }
                }
                "enum" => {
                    let symbols =
                        obj.get("symbols")
                            .and_then(|s| s.as_array())
                            .ok_or_else(|| Error::UnexpectedError {
                                message: "avro schema: enum missing 'symbols'".into(),
                                source: None,
                            })?;
                    Ok(FieldSchema::Enum {
                        symbols_count: symbols.len(),
                    })
                }
                _ => Err(Error::UnexpectedError {
                    message: format!("avro schema: unsupported type: {type_str}"),
                    source: None,
                }),
            }
        }
        Value::Array(arr) => {
            let branches: Vec<FieldSchema> = arr
                .iter()
                .map(parse_field_schema)
                .collect::<crate::Result<_>>()?;
            Ok(FieldSchema::Union(branches))
        }
        _ => Err(Error::UnexpectedError {
            message: format!("avro schema: unexpected value: {value}"),
            source: None,
        }),
    }
}

/// Skip a value in the cursor according to its schema.
pub fn skip_field(
    cursor: &mut super::cursor::AvroCursor,
    schema: &FieldSchema,
) -> crate::Result<()> {
    skip_field_inner(cursor, schema, false)
}

/// Skip a field that may have been unwrapped from a nullable union.
/// When `nullable` is true, reads the union index first.
pub fn skip_nullable_field(
    cursor: &mut super::cursor::AvroCursor,
    schema: &FieldSchema,
    nullable: bool,
) -> crate::Result<()> {
    skip_field_inner(cursor, schema, nullable)
}

fn skip_field_inner(
    cursor: &mut super::cursor::AvroCursor,
    schema: &FieldSchema,
    nullable: bool,
) -> crate::Result<()> {
    if nullable {
        let idx = cursor.read_union_index()?;
        if idx == 0 {
            return Ok(());
        }
    }
    match schema {
        FieldSchema::Null => Ok(()),
        FieldSchema::Boolean => cursor.skip_raw(1),
        FieldSchema::Int | FieldSchema::Long | FieldSchema::Enum { .. } => cursor.skip_long(),
        FieldSchema::Float => cursor.skip_raw(4),
        FieldSchema::Double => cursor.skip_raw(8),
        FieldSchema::Bytes | FieldSchema::String => cursor.skip_bytes(),
        FieldSchema::Fixed(size) => cursor.skip_raw(*size),
        FieldSchema::Union(branches) => {
            let idx = cursor.read_union_index()? as usize;
            if idx < branches.len() {
                skip_field(cursor, &branches[idx])
            } else {
                Err(Error::UnexpectedError {
                    message: format!("avro skip: union index {idx} out of range"),
                    source: None,
                })
            }
        }
        FieldSchema::Array(item_schema) => {
            loop {
                let count = cursor.read_long()?;
                if count == 0 {
                    break;
                }
                let count = if count < 0 {
                    cursor.skip_long()?;
                    (-count) as usize
                } else {
                    count as usize
                };
                for _ in 0..count {
                    skip_field(cursor, item_schema)?;
                }
            }
            Ok(())
        }
        FieldSchema::Map(value_schema) => {
            loop {
                let count = cursor.read_long()?;
                if count == 0 {
                    break;
                }
                let count = if count < 0 {
                    cursor.skip_long()?;
                    (-count) as usize
                } else {
                    count as usize
                };
                for _ in 0..count {
                    cursor.skip_bytes()?;
                    skip_field(cursor, value_schema)?;
                }
            }
            Ok(())
        }
        FieldSchema::Record(record_schema) => {
            for field in &record_schema.fields {
                skip_field_inner(cursor, &field.schema, field.nullable)?;
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_manifest_file_meta_schema() {
        let schema_json = r#"["null", {
            "type": "record",
            "name": "record",
            "namespace": "org.apache.paimon.avro.generated",
            "fields": [
                {"name": "_VERSION", "type": "int"},
                {"name": "_FILE_NAME", "type": "string"},
                {"name": "_FILE_SIZE", "type": "long"},
                {"name": "_NUM_ADDED_FILES", "type": "long"},
                {"name": "_NUM_DELETED_FILES", "type": "long"},
                {"name": "_PARTITION_STATS", "type": ["null", {
                    "type": "record",
                    "name": "record__PARTITION_STATS",
                    "fields": [
                        {"name": "_MIN_VALUES", "type": "bytes"},
                        {"name": "_MAX_VALUES", "type": "bytes"},
                        {"name": "_NULL_COUNTS", "type": ["null", {"type": "array", "items": ["null", "long"]}], "default": null}
                    ]
                }], "default": null},
                {"name": "_SCHEMA_ID", "type": "long"}
            ]
        }]"#;

        let ws = WriterSchema::parse(schema_json).unwrap();
        assert_eq!(ws.fields.len(), 7);
        assert!(ws.is_union_wrapped);
        assert_eq!(ws.fields[0].name, "_VERSION");
        assert_eq!(ws.fields[1].name, "_FILE_NAME");
        assert_eq!(ws.fields[6].name, "_SCHEMA_ID");
    }

    #[test]
    fn test_parse_nested_record() {
        let schema_json = r#"{"type": "record", "name": "test", "fields": [
            {"name": "nested", "type": ["null", {"type": "record", "name": "inner", "fields": [
                {"name": "x", "type": "int"}
            ]}]}
        ]}"#;

        let ws = WriterSchema::parse(schema_json).unwrap();
        assert!(!ws.is_union_wrapped);
        assert_eq!(ws.fields.len(), 1);
        assert!(ws.fields[0].nullable);
        match &ws.fields[0].schema {
            FieldSchema::Record(inner) => {
                assert_eq!(inner.fields.len(), 1);
                assert_eq!(inner.fields[0].name, "x");
            }
            _ => panic!("expected record"),
        }
    }
}
