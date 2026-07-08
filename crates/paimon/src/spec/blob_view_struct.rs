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

use crate::catalog::{Identifier, UNKNOWN_DATABASE};
use crate::Error;

const CURRENT_VERSION: u8 = 1;
const MAGIC: u64 = 0x424C4F4256494557; // "BLOBVIEW"

/// Serialized coordinates for a BLOB value stored in an upstream table.
///
/// Matches Java `org.apache.paimon.data.BlobViewStruct`: version byte,
/// `"BLOBVIEW"` magic, UTF-8 `database.table`, field id, and row id, all
/// little-endian where applicable.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlobViewStruct {
    identifier: Identifier,
    field_id: i32,
    row_id: i64,
}

impl BlobViewStruct {
    pub fn new(identifier: Identifier, field_id: i32, row_id: i64) -> Self {
        Self {
            identifier,
            field_id,
            row_id,
        }
    }

    pub fn identifier(&self) -> &Identifier {
        &self.identifier
    }

    pub fn field_id(&self) -> i32 {
        self.field_id
    }

    pub fn row_id(&self) -> i64 {
        self.row_id
    }

    pub fn serialize(&self) -> crate::Result<Vec<u8>> {
        if self.identifier.database() == UNKNOWN_DATABASE {
            return Err(Error::DataInvalid {
                message: format!(
                    "Blob view upstream table identifier must include database name: {}",
                    self.identifier.full_name()
                ),
                source: None,
            });
        }

        let identifier = self.identifier.full_name();
        let identifier_bytes = identifier.as_bytes();
        let identifier_length =
            i32::try_from(identifier_bytes.len()).map_err(|e| Error::DataInvalid {
                message: format!("BlobViewStruct identifier is too long: {identifier}"),
                source: Some(Box::new(e)),
            })?;
        let total_size = 1 + 8 + 4 + identifier_bytes.len() + 4 + 8;
        let mut buf = Vec::with_capacity(total_size);

        buf.push(CURRENT_VERSION);
        buf.extend_from_slice(&MAGIC.to_le_bytes());
        buf.extend_from_slice(&identifier_length.to_le_bytes());
        buf.extend_from_slice(identifier_bytes);
        buf.extend_from_slice(&self.field_id.to_le_bytes());
        buf.extend_from_slice(&self.row_id.to_le_bytes());
        Ok(buf)
    }

    pub fn deserialize(bytes: &[u8]) -> crate::Result<Self> {
        if bytes.is_empty() {
            return Err(invalid_payload("too short"));
        }

        let version = bytes[0];
        if version != CURRENT_VERSION {
            return Err(Error::Unsupported {
                message: format!(
                    "Expecting BlobViewStruct version to be {CURRENT_VERSION}, but found {version}."
                ),
            });
        }

        let mut pos = 1;
        if bytes.len() < pos + 8 {
            return Err(invalid_payload("too short"));
        }
        let magic = u64::from_le_bytes(bytes[pos..pos + 8].try_into().unwrap());
        if magic != MAGIC {
            return Err(Error::DataInvalid {
                message: format!(
                    "Invalid BlobViewStruct: missing magic header. Expected magic: {MAGIC}, but found: {magic}"
                ),
                source: None,
            });
        }
        pos += 8;

        if bytes.len() < pos + 4 {
            return Err(invalid_payload("too short"));
        }
        let identifier_length = i32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap());
        if identifier_length < 0 {
            return Err(invalid_payload(&format!(
                "negative identifier length: {identifier_length}"
            )));
        }
        let identifier_length = identifier_length as usize;
        pos += 4;

        if bytes.len() < pos + identifier_length + 4 + 8 {
            return Err(invalid_payload("identifier length exceeds data size"));
        }
        let identifier_text = String::from_utf8(bytes[pos..pos + identifier_length].to_vec())
            .map_err(|e| Error::DataInvalid {
                message: format!("Invalid UTF-8 in BlobViewStruct identifier: {e}"),
                source: Some(Box::new(e)),
            })?;
        pos += identifier_length;

        let field_id = i32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap());
        pos += 4;
        let row_id = i64::from_le_bytes(bytes[pos..pos + 8].try_into().unwrap());
        pos += 8;

        if pos != bytes.len() {
            return Err(invalid_payload("trailing bytes"));
        }

        Ok(Self {
            identifier: parse_identifier(&identifier_text)?,
            field_id,
            row_id,
        })
    }

    pub fn is_blob_view_struct(bytes: &[u8]) -> bool {
        if bytes.len() < 9 {
            return false;
        }
        let version = bytes[0];
        if version != CURRENT_VERSION {
            return false;
        }
        let magic = u64::from_le_bytes(bytes[1..9].try_into().unwrap());
        magic == MAGIC
    }
}

fn parse_identifier(full_name: &str) -> crate::Result<Identifier> {
    let Some((database, object)) = full_name.rsplit_once('.') else {
        return Err(Error::DataInvalid {
            message: format!(
                "BlobViewStruct identifier must be 'database.table', got: {full_name}"
            ),
            source: None,
        });
    };
    Ok(Identifier::new(database, object))
}

fn invalid_payload(message: &str) -> Error {
    Error::DataInvalid {
        message: format!("Invalid BlobViewStruct data: {message}"),
        source: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        let view = BlobViewStruct::new(Identifier::new("db", "source"), 3, 42);
        let bytes = view.serialize().unwrap();

        assert!(BlobViewStruct::is_blob_view_struct(&bytes));
        assert_eq!(BlobViewStruct::deserialize(&bytes).unwrap(), view);
    }

    #[test]
    fn test_rejects_unknown_database_on_serialize() {
        let view = BlobViewStruct::new(Identifier::new(UNKNOWN_DATABASE, "source"), 3, 42);
        assert!(view.serialize().is_err());
    }

    #[test]
    fn test_rejects_trailing_bytes() {
        let mut bytes = BlobViewStruct::new(Identifier::new("db", "source"), 3, 42)
            .serialize()
            .unwrap();
        bytes.push(1);

        let err = BlobViewStruct::deserialize(&bytes).unwrap_err();
        assert!(
            matches!(err, Error::DataInvalid { message, .. } if message.contains("trailing bytes"))
        );
    }
}
