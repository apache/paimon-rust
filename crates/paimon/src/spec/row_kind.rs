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

//! Row kind for primary-key table changelog semantics.
//!
//! Reference: [org.apache.paimon.types.RowKind](https://github.com/apache/paimon/blob/release-1.3/paimon-common/src/main/java/org/apache/paimon/types/RowKind.java)

/// The kind of a row in a changelog, matching Java Paimon's `RowKind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i8)]
pub enum RowKind {
    Insert = 0,
    UpdateBefore = 1,
    UpdateAfter = 2,
    Delete = 3,
}

impl RowKind {
    /// Create a `RowKind` from its byte value.
    pub fn from_value(value: i8) -> crate::Result<Self> {
        match value {
            0 => Ok(RowKind::Insert),
            1 => Ok(RowKind::UpdateBefore),
            2 => Ok(RowKind::UpdateAfter),
            3 => Ok(RowKind::Delete),
            _ => Err(crate::Error::DataInvalid {
                message: format!("Invalid RowKind value: {value}, expected 0-3"),
                source: None,
            }),
        }
    }

    /// Whether this row kind represents an addition (INSERT or UPDATE_AFTER).
    pub fn is_add(&self) -> bool {
        matches!(self, RowKind::Insert | RowKind::UpdateAfter)
    }
}
