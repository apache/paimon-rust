// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;

use paimon::catalog::Identifier;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TableRef {
    pub database: String,
    pub table: String,
}

impl TableRef {
    pub fn new(database: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            database: database.into(),
            table: table.into(),
        }
    }

    pub(crate) fn identifier(&self) -> Identifier {
        Identifier::new(&self.database, &self.table)
    }

    pub(crate) fn full_name(&self) -> String {
        format!("{}.{}", self.database, self.table)
    }
}

pub type LookupKey = BTreeMap<String, Value>;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DescriptorFormat {
    #[default]
    Json,
    PaimonBase64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BatchGetRequest {
    pub table: TableRef,
    pub keys: Vec<LookupKey>,
    pub blob_fields: Vec<String>,
    #[serde(default)]
    pub snapshot_id: Option<i64>,
    #[serde(default)]
    pub descriptor_format: DescriptorFormat,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LookupStatus {
    Found,
    NotFound,
    NonUnique,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlobDescriptorDto {
    pub version: u8,
    pub uri: String,
    pub offset: i64,
    pub length: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoded: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LookupResult {
    pub key: LookupKey,
    pub status: LookupStatus,
    pub blobs: BTreeMap<String, Option<BlobDescriptorDto>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LookupScanStats {
    pub planned_files: usize,
    pub planned_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchGetResponse {
    pub table: TableRef,
    pub snapshot_id: Option<i64>,
    pub schema_id: i64,
    #[serde(default)]
    pub cache_hit: bool,
    #[serde(default)]
    pub scan: LookupScanStats,
    pub results: Vec<LookupResult>,
}
