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

    /// Byte value for serialization, matching Java `toByteValue()`.
    pub fn to_value(self) -> i8 {
        self as i8
    }

    /// Short string form: `"+I"`, `"-U"`, `"+U"`, `"-D"`.
    pub fn short_string(self) -> &'static str {
        match self {
            RowKind::Insert => "+I",
            RowKind::UpdateBefore => "-U",
            RowKind::UpdateAfter => "+U",
            RowKind::Delete => "-D",
        }
    }

    /// Parse from short string; case-insensitive (Java `RowKind.fromShortString`).
    pub fn from_short_string(value: &str) -> crate::Result<Self> {
        if value.len() != 2 {
            return Err(crate::Error::DataInvalid {
                message: format!("Unsupported short string '{value}' for row kind"),
                source: None,
            });
        }
        let bytes = value.as_bytes();
        let sign = bytes[0].to_ascii_uppercase();
        let letter = bytes[1].to_ascii_uppercase();
        match (sign, letter) {
            (b'+', b'I') => Ok(RowKind::Insert),
            (b'-', b'U') => Ok(RowKind::UpdateBefore),
            (b'+', b'U') => Ok(RowKind::UpdateAfter),
            (b'-', b'D') => Ok(RowKind::Delete),
            _ => Err(crate::Error::DataInvalid {
                message: format!("Unsupported short string '{value}' for row kind"),
                source: None,
            }),
        }
    }

    /// Whether this is `UPDATE_BEFORE` or `DELETE` (Java `isRetract()`).
    pub fn is_retract(self) -> bool {
        matches!(self, RowKind::UpdateBefore | RowKind::Delete)
    }
}

#[cfg(test)]
mod tests {
    use super::RowKind;

    #[test]
    fn from_short_string_accepts_case_insensitive_tokens() {
        assert_eq!(RowKind::from_short_string("+i").unwrap(), RowKind::Insert);
        assert_eq!(
            RowKind::from_short_string("-u").unwrap(),
            RowKind::UpdateBefore
        );
        assert_eq!(
            RowKind::from_short_string("+U").unwrap(),
            RowKind::UpdateAfter
        );
        assert_eq!(RowKind::from_short_string("-D").unwrap(), RowKind::Delete);
    }

    #[test]
    fn from_short_string_rejects_invalid() {
        assert!(RowKind::from_short_string("INSERT").is_err());
        assert!(RowKind::from_short_string("").is_err());
    }

    #[test]
    fn short_string_round_trip() {
        for kind in [
            RowKind::Insert,
            RowKind::UpdateBefore,
            RowKind::UpdateAfter,
            RowKind::Delete,
        ] {
            assert_eq!(
                RowKind::from_short_string(kind.short_string()).unwrap(),
                kind
            );
        }
    }
}
