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

use crate::error::Error;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

/// The table schema for paimon table.
///
/// Impl References: <https://github.com/apache/paimon/blob/db8bcd7fdd9c2705435d2ab1d2341c52d1f67ee5/paimon-core/src/main/java/org/apache/paimon/schema/TableSchema.java#L47>
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TableSchema {
    /// version of schema for paimon
    version: i32,
    id: i64,
    fields: Vec<DataField>,
    highest_field_id: i32,
    partition_keys: Vec<String>,
    primary_keys: Vec<String>,
    options: HashMap<String, String>,
    comment: Option<String>,
    time_millis: i64,
}

/// Data field for paimon table.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/db8bcd7fdd9c2705435d2ab1d2341c52d1f67ee5/paimon-common/src/main/java/org/apache/paimon/types/DataField.java#L40>
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct DataField {
    id: i32,
    name: String,
    #[serde(rename = "type")]
    #[serde_as(as = "DisplayFromStr")]
    typ: DataType,
    description: Option<String>,
}

/// Data type for paimon table.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/db8bcd7fdd9c2705435d2ab1d2341c52d1f67ee5/paimon-common/src/main/java/org/apache/paimon/types/DataType.java#L45>
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataType {
    is_nullable: bool,
    type_root: DataTypeRoot,
}

impl Display for DataType {
    fn fmt(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl FromStr for DataType {
    type Err = Error;

    fn from_str(_: &str) -> Result<Self, Self::Err> {
        todo!()
    }
}

/// The root of data type.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/db8bcd7fdd9c2705435d2ab1d2341c52d1f67ee5/paimon-common/src/main/java/org/apache/paimon/types/DataTypeRoot.java#L49>
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataTypeRoot {}
