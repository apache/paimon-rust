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

//! Table-level MAP layout validation, mirroring Java SchemaValidation.

use super::{CoreOptions, DataField, DataType};
use crate::{Error, Result};
use std::collections::HashMap;

// Bound both configured physical schemas and untrusted footer metadata.
pub(crate) const MAX_SHARED_SHREDDING_NUM_COLUMNS: usize = 16 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ColumnPlacement {
    Plain,
    Sequential,
    Lru,
}

pub(crate) fn column_placement(
    options: &HashMap<String, String>,
    name: &str,
) -> Result<ColumnPlacement> {
    let key = format!("fields.{name}.map.shared-shredding.column-placement-policy");
    match options
        .get(&key)
        .map(|s| s.to_ascii_lowercase())
        .as_deref()
        .unwrap_or("lru")
    {
        "plain" => Ok(ColumnPlacement::Plain),
        "sequential" => Ok(ColumnPlacement::Sequential),
        "lru" => Ok(ColumnPlacement::Lru),
        value => Err(invalid(format!(
            "Invalid value '{value}' for {key}: expected plain, sequential or lru"
        ))),
    }
}

pub(crate) fn max_columns(options: &HashMap<String, String>, name: &str) -> Result<usize> {
    let key = format!("fields.{name}.map.shared-shredding.max-columns");
    let value = options.get(&key).map(String::as_str).unwrap_or("256");
    let columns = value
        .parse::<i32>()
        .ok()
        .filter(|&n| n > 0)
        .map(|n| n as usize)
        .ok_or_else(|| invalid(format!("{key} must be a positive integer, got '{value}'")))?;
    if columns > MAX_SHARED_SHREDDING_NUM_COLUMNS {
        return Err(invalid(format!(
            "{key} exceeds the supported maximum of {MAX_SHARED_SHREDDING_NUM_COLUMNS}: {columns}"
        )));
    }
    Ok(columns)
}

pub(crate) fn validate(fields: &[DataField], options: &HashMap<String, String>) -> Result<()> {
    let mut active = false;
    for (key, value) in options {
        let Some(name) = key
            .strip_prefix("fields.")
            .and_then(|s| s.strip_suffix(".map.storage-layout"))
        else {
            continue;
        };
        let field = fields.iter().find(|f| f.name() == name)
            .ok_or_else(|| invalid(format!("Column '{name}' is configured with map.storage-layout but does not exist in table schema")))?;
        let DataType::Map(map) = field.data_type() else {
            return Err(invalid(format!(
                "Column '{name}' is configured with map.storage-layout but its type is not MAP"
            )));
        };
        match value.to_ascii_lowercase().as_str() {
            "default" => continue,
            "shared-shredding" => {}
            _ => {
                return Err(invalid(format!(
                    "Invalid value '{value}' for {key}: expected default or shared-shredding"
                )))
            }
        }
        active = true;
        if !matches!(map.key_type(), DataType::VarChar(_)) {
            return Err(invalid(format!(
                "Column '{name}' shared-shredding requires MAP<STRING NOT NULL, T>"
            )));
        }
        if map.key_type().is_nullable() {
            return Err(invalid(format!(
                "Column '{name}' shared-shredding map key type is nullable"
            )));
        }
        max_columns(options, name)?;
        column_placement(options, name)?;
        for (label, predicate) in [
            ("BLOB", is_blob as fn(&DataType) -> bool),
            ("VECTOR", is_vector as fn(&DataType) -> bool),
        ] {
            if contains(map.value_type(), predicate) {
                return Err(invalid(format!(
                    "MAP shared-shredding cannot contain {label} fields"
                )));
            }
        }
    }
    if !active {
        return Ok(());
    }
    for (label, predicate) in [
        ("Variant", is_variant as fn(&DataType) -> bool),
        ("MULTISET", is_multiset as fn(&DataType) -> bool),
    ] {
        if fields.iter().any(|f| contains(f.data_type(), predicate)) {
            return Err(invalid(format!(
                "MAP shared-shredding cannot be used with {label} fields"
            )));
        }
    }
    let core = CoreOptions::new(options);
    if core.bucket() == -2 {
        return Err(invalid(
            "MAP shared-shredding does not support postpone bucket mode",
        ));
    }
    validate_format("file.format", &core.file_format())?;
    validate_format("changelog.file.format", &core.changelog_file_format())?;
    validate_compression("file.compression", core.file_compression())?;
    validate_compression(
        "changelog.file.compression",
        core.changelog_file_compression(),
    )?;
    // Java also validates codecs and formats configured for higher levels.
    for (key, value) in options {
        if key == "file.format.per.level" {
            for entry in value.split(',') {
                if let Some((_, format)) = entry.split_once(':') {
                    validate_format(key, &format.trim().to_ascii_lowercase())?;
                }
            }
        }
        if key == "file.compression.per.level" {
            for entry in value.split(',') {
                if let Some((_, codec)) = entry.split_once(':') {
                    validate_compression(key, codec.trim())?;
                }
            }
        }
    }
    Ok(())
}

fn validate_format(key: &str, value: &str) -> Result<()> {
    if !matches!(value, "" | "parquet" | "orc") {
        return Err(invalid(format!(
            "MAP shared-shredding only supports parquet/orc file formats, but {key} is {value}"
        )));
    }
    Ok(())
}

fn validate_compression(key: &str, value: &str) -> Result<()> {
    if !matches!(
        value.to_ascii_lowercase().as_str(),
        "" | "none" | "lz4" | "zstd"
    ) {
        return Err(invalid(format!(
            "MAP shared-shredding only supports none/lz4/zstd compression, but {key} is {value}"
        )));
    }
    Ok(())
}

fn is_blob(t: &DataType) -> bool {
    matches!(t, DataType::Blob(_))
}
fn is_vector(t: &DataType) -> bool {
    matches!(t, DataType::Vector(_))
}
fn is_variant(t: &DataType) -> bool {
    matches!(t, DataType::Variant(_))
}
fn is_multiset(t: &DataType) -> bool {
    matches!(t, DataType::Multiset(_))
}

fn contains(t: &DataType, predicate: fn(&DataType) -> bool) -> bool {
    if predicate(t) {
        return true;
    }
    match t {
        DataType::Array(a) => contains(a.element_type(), predicate),
        DataType::Multiset(m) => contains(m.element_type(), predicate),
        DataType::Map(m) => {
            contains(m.key_type(), predicate) || contains(m.value_type(), predicate)
        }
        DataType::Row(r) => r
            .fields()
            .iter()
            .any(|f| contains(f.data_type(), predicate)),
        _ => false,
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::ConfigInvalid {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{IntType, MapType, Schema, VarCharType};

    fn fields() -> Vec<DataField> {
        vec![
            DataField::new(0, "id".into(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "tags".into(),
                DataType::Map(MapType::new(
                    DataType::VarChar(VarCharType::string_type())
                        .copy_with_nullable(false)
                        .unwrap(),
                    DataType::Int(IntType::new()),
                )),
            ),
        ]
    }
    fn options() -> HashMap<String, String> {
        HashMap::from([
            (
                "fields.tags.map.storage-layout".into(),
                "shared-shredding".into(),
            ),
            ("file.compression".into(), "zstd".into()),
        ])
    }
    #[test]
    fn rejects_invalid_map_table_options() {
        for (key, value, expected) in [
            (
                "fields.missing.map.storage-layout",
                "default",
                "does not exist",
            ),
            ("fields.id.map.storage-layout", "default", "not MAP"),
            ("fields.tags.map.storage-layout", "typo", "expected default"),
            (
                "fields.tags.map.shared-shredding.max-columns",
                "0",
                "positive integer",
            ),
            (
                "fields.tags.map.shared-shredding.max-columns",
                "-1",
                "positive integer",
            ),
            (
                "fields.tags.map.shared-shredding.max-columns",
                "2147483648",
                "positive integer",
            ),
            (
                "fields.tags.map.shared-shredding.column-placement-policy",
                "random",
                "plain, sequential or lru",
            ),
            (
                "fields.tags.map.shared-shredding.max-columns",
                "16385",
                "supported maximum of 16384",
            ),
            ("bucket", "-2", "postpone"),
            ("file.format", "avro", "parquet/orc"),
            ("changelog-file.format", "csv", "parquet/orc"),
            ("file.format.per.level", "0:parquet,1:avro", "parquet/orc"),
            ("file.compression", "snappy", "none/lz4/zstd"),
            ("changelog-file.compression", "gzip", "none/lz4/zstd"),
            (
                "file.compression.per.level",
                "0:lz4,1:snappy",
                "none/lz4/zstd",
            ),
        ] {
            let mut options = options();
            options.insert(key.into(), value.into());
            let error = validate(&fields(), &options).unwrap_err();
            assert!(
                error.to_string().contains(expected),
                "{key}={value}: {error}"
            );
        }
    }
    #[test]
    fn validates_map_key_and_nested_value_types() {
        use crate::spec::{ArrayType, BlobType, MultisetType, VariantType};
        let mut f = fields();
        f[1] = DataField::new(
            1,
            "tags".into(),
            DataType::Map(MapType::new(
                DataType::VarChar(VarCharType::string_type()),
                DataType::Int(IntType::new()),
            )),
        );
        assert!(validate(&f, &options())
            .unwrap_err()
            .to_string()
            .contains("nullable"));
        f[1] = DataField::new(
            1,
            "tags".into(),
            DataType::Map(MapType::new(
                DataType::Int(IntType::new()),
                DataType::Int(IntType::new()),
            )),
        );
        assert!(validate(&f, &options())
            .unwrap_err()
            .to_string()
            .contains("STRING NOT NULL"));
        for (data_type, message) in [
            (DataType::Blob(BlobType::new()), "BLOB"),
            (DataType::Variant(VariantType::new()), "Variant"),
            (
                DataType::Multiset(MultisetType::new(DataType::Int(IntType::new()))),
                "MULTISET",
            ),
        ] {
            f[1] = DataField::new(
                1,
                "tags".into(),
                DataType::Map(MapType::new(
                    DataType::VarChar(VarCharType::string_type())
                        .copy_with_nullable(false)
                        .unwrap(),
                    DataType::Array(ArrayType::new(data_type)),
                )),
            );
            assert!(validate(&f, &options())
                .unwrap_err()
                .to_string()
                .contains(message));
        }
    }
    #[test]
    fn accepts_valid_map_schema_and_policies() {
        let mut builder = Schema::builder();
        for field in fields() {
            builder = builder.column(field.name(), field.data_type().clone());
        }
        for (key, value) in options() {
            builder = builder.option(key, value);
        }
        assert!(builder.build().is_ok());
        assert_eq!(
            column_placement(&HashMap::new(), "tags").unwrap(),
            ColumnPlacement::Lru
        );
        for policy in ["plain", "sequential", "lru", "LRU"] {
            let mut options = options();
            options.insert(
                "fields.tags.map.shared-shredding.column-placement-policy".into(),
                policy.into(),
            );
            validate(&fields(), &options).unwrap();
        }
        let mut uppercase = options();
        uppercase.insert("file.format.per.level".into(), "0: PARQUET ,1: ORC".into());
        uppercase.insert(
            "file.compression.per.level".into(),
            "0: LZ4 ,1: ZSTD".into(),
        );
        uppercase.insert(
            "fields.tags.map.shared-shredding.max-columns".into(),
            "16384".into(),
        );
        validate(&fields(), &uppercase).unwrap();
        let mut inactive = options();
        inactive.insert("fields.tags.map.storage-layout".into(), "default".into());
        inactive.insert("file.compression".into(), "snappy".into());
        validate(&fields(), &inactive).unwrap();
    }
}
