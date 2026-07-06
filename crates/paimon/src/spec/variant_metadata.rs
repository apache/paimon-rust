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

//! Java-compatible markers for Variant extraction read types.

use crate::spec::{DataField, DataType, RowType};
use crate::{Error, Result};
use serde::{Deserialize, Serialize};

pub const VARIANT_METADATA_KEY: &str = "__VARIANT_METADATA";
const VARIANT_METADATA_DELIMITER: &str = ";";

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VariantFieldMetadata {
    path: String,
    fail_on_error: bool,
    time_zone_id: String,
}

impl VariantFieldMetadata {
    pub fn new(
        path: impl Into<String>,
        fail_on_error: bool,
        time_zone_id: impl Into<String>,
    ) -> Self {
        Self {
            path: path.into(),
            fail_on_error,
            time_zone_id: time_zone_id.into(),
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn fail_on_error(&self) -> bool {
        self.fail_on_error
    }

    pub fn time_zone_id(&self) -> &str {
        &self.time_zone_id
    }
}

pub fn build_variant_metadata(path: &str, fail_on_error: bool, time_zone_id: &str) -> String {
    let metadata = VariantFieldMetadata::new(path, fail_on_error, time_zone_id);
    let json = serde_json::to_string(&metadata)
        .expect("Variant metadata serialization should not fail for string and bool fields");
    format!("{VARIANT_METADATA_KEY}{json}")
}

pub fn parse_variant_metadata(description: &str) -> Result<VariantFieldMetadata> {
    let Some(raw) = description.strip_prefix(VARIANT_METADATA_KEY) else {
        return Err(Error::DataInvalid {
            message: "Variant metadata description is missing marker".to_string(),
            source: None,
        });
    };
    if raw.trim_start().starts_with('{') {
        let metadata =
            serde_json::from_str::<VariantFieldMetadata>(raw).map_err(|e| Error::DataInvalid {
                message: "Malformed Variant metadata JSON description".to_string(),
                source: Some(Box::new(e)),
            })?;
        validate_variant_metadata(&metadata)?;
        return Ok(metadata);
    }

    parse_legacy_variant_metadata(raw)
}

fn parse_legacy_variant_metadata(raw: &str) -> Result<VariantFieldMetadata> {
    let mut parts = raw.split(VARIANT_METADATA_DELIMITER);
    let path = parts.next().unwrap_or_default();
    let fail_on_error = parts.next().ok_or_else(|| Error::DataInvalid {
        message: "Variant metadata description is missing failOnError".to_string(),
        source: None,
    })?;
    let time_zone_id = parts.next().ok_or_else(|| Error::DataInvalid {
        message: "Variant metadata description is missing timeZoneId".to_string(),
        source: None,
    })?;
    if parts.next().is_some() || path.is_empty() {
        return Err(Error::DataInvalid {
            message: "Malformed Variant metadata description".to_string(),
            source: None,
        });
    }
    let fail_on_error = fail_on_error
        .parse::<bool>()
        .map_err(|e| Error::DataInvalid {
            message: format!("Invalid Variant metadata failOnError value '{fail_on_error}'"),
            source: Some(Box::new(e)),
        })?;
    Ok(VariantFieldMetadata::new(
        path.to_string(),
        fail_on_error,
        time_zone_id.to_string(),
    ))
}

fn validate_variant_metadata(metadata: &VariantFieldMetadata) -> Result<()> {
    if metadata.path().is_empty() {
        return Err(Error::DataInvalid {
            message: "Malformed Variant metadata description".to_string(),
            source: None,
        });
    }
    Ok(())
}

pub fn is_variant_metadata_description(description: Option<&str>) -> bool {
    description.is_some_and(|description| description.starts_with(VARIANT_METADATA_KEY))
}

pub fn is_variant_extraction_row_type(data_type: &DataType) -> bool {
    let DataType::Row(row_type) = data_type else {
        return false;
    };
    is_variant_extraction_row(row_type)
}

pub fn is_variant_extraction_row(row_type: &RowType) -> bool {
    !row_type.fields().is_empty()
        && row_type
            .fields()
            .iter()
            .all(|field| is_variant_metadata_description(field.description()))
}

pub fn variant_extraction_row(
    nullable: bool,
    extractions: impl IntoIterator<Item = (DataType, String, bool, String)>,
) -> RowType {
    let fields = extractions
        .into_iter()
        .enumerate()
        .map(|(idx, (data_type, path, fail_on_error, time_zone_id))| {
            DataField::new(idx as i32, idx.to_string(), data_type).with_description(Some(
                build_variant_metadata(&path, fail_on_error, &time_zone_id),
            ))
        })
        .collect();
    RowType::with_nullable(nullable, fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{DataType, IntType, VarCharType};

    #[test]
    fn parses_variant_metadata_description() {
        let description = build_variant_metadata("$.a", false, "UTC");
        let metadata = parse_variant_metadata(&description).unwrap();
        assert_eq!(metadata.path(), "$.a");
        assert!(!metadata.fail_on_error());
        assert_eq!(metadata.time_zone_id(), "UTC");
    }

    #[test]
    fn parses_variant_metadata_description_with_delimiters() {
        let description = build_variant_metadata("$.a;b", true, "UTC;8");
        let metadata = parse_variant_metadata(&description).unwrap();
        assert_eq!(metadata.path(), "$.a;b");
        assert!(metadata.fail_on_error());
        assert_eq!(metadata.time_zone_id(), "UTC;8");
    }

    #[test]
    fn parses_legacy_variant_metadata_description() {
        let description = format!("{VARIANT_METADATA_KEY}$.a;false;UTC");
        let metadata = parse_variant_metadata(&description).unwrap();
        assert_eq!(metadata.path(), "$.a");
        assert!(!metadata.fail_on_error());
        assert_eq!(metadata.time_zone_id(), "UTC");
    }

    #[test]
    fn identifies_variant_extraction_row_type() {
        let row = variant_extraction_row(
            true,
            vec![
                (
                    DataType::Int(IntType::new()),
                    "$.age".to_string(),
                    true,
                    "UTC".to_string(),
                ),
                (
                    DataType::VarChar(VarCharType::string_type()),
                    "$.name".to_string(),
                    false,
                    "UTC".to_string(),
                ),
            ],
        );
        assert!(is_variant_extraction_row(&row));
        assert!(is_variant_extraction_row_type(&DataType::Row(row)));
    }
}
