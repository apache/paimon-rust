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

const CURRENT_VERSION: u8 = 2;
const MAGIC: u64 = 0x424C4F4244455343; // "BLOBDESC"

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobDescriptor {
    version: u8,
    uri: String,
    offset: i64,
    length: i64,
}

impl BlobDescriptor {
    pub fn new(uri: String, offset: i64, length: i64) -> Self {
        Self {
            version: CURRENT_VERSION,
            uri,
            offset,
            length,
        }
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    pub fn offset(&self) -> i64 {
        self.offset
    }

    pub fn length(&self) -> i64 {
        self.length
    }

    pub fn serialize(&self) -> Vec<u8> {
        let uri_bytes = self.uri.as_bytes();
        let uri_length = uri_bytes.len();
        let total_size = 1 + 8 + 4 + uri_length + 8 + 8;
        let mut buf = Vec::with_capacity(total_size);

        buf.push(self.version);
        buf.extend_from_slice(&MAGIC.to_le_bytes());
        buf.extend_from_slice(&(uri_length as i32).to_le_bytes());
        buf.extend_from_slice(uri_bytes);
        buf.extend_from_slice(&self.offset.to_le_bytes());
        buf.extend_from_slice(&self.length.to_le_bytes());

        buf
    }

    pub fn deserialize(bytes: &[u8]) -> crate::Result<Self> {
        if bytes.len() < 1 + 8 + 4 {
            return Err(Error::DataInvalid {
                message: "BlobDescriptor bytes too short".to_string(),
                source: None,
            });
        }

        let version = bytes[0];
        if version > CURRENT_VERSION {
            return Err(Error::Unsupported {
                message: format!(
                    "Expecting BlobDescriptor version <= {CURRENT_VERSION}, but found {version}"
                ),
            });
        }

        let mut pos = 1;

        if version > 1 {
            let magic = u64::from_le_bytes(bytes[pos..pos + 8].try_into().unwrap());
            if magic != MAGIC {
                return Err(Error::DataInvalid {
                    message: format!(
                        "Invalid BlobDescriptor: missing magic header. Expected {MAGIC:#X}, found {magic:#X}"
                    ),
                    source: None,
                });
            }
            pos += 8;
        }

        if bytes.len() < pos + 4 {
            return Err(Error::DataInvalid {
                message: "BlobDescriptor bytes too short for uri_length".to_string(),
                source: None,
            });
        }
        let uri_length_raw = i32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap());
        if uri_length_raw < 0 {
            return Err(Error::DataInvalid {
                message: format!("BlobDescriptor has negative uri_length: {uri_length_raw}"),
                source: None,
            });
        }
        let uri_length = uri_length_raw as usize;
        pos += 4;

        if bytes.len() < pos + uri_length + 16 {
            return Err(Error::DataInvalid {
                message: "BlobDescriptor bytes too short for uri + offset + length".to_string(),
                source: None,
            });
        }
        let uri = String::from_utf8(bytes[pos..pos + uri_length].to_vec()).map_err(|e| {
            Error::DataInvalid {
                message: format!("Invalid UTF-8 in BlobDescriptor uri: {e}"),
                source: Some(Box::new(e)),
            }
        })?;
        pos += uri_length;

        let offset = i64::from_le_bytes(bytes[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let length = i64::from_le_bytes(bytes[pos..pos + 8].try_into().unwrap());

        Ok(Self {
            version,
            uri,
            offset,
            length,
        })
    }

    pub fn is_blob_descriptor(bytes: &[u8]) -> bool {
        if bytes.len() < 9 {
            return false;
        }
        let version = bytes[0];
        if version > CURRENT_VERSION {
            return false;
        }
        let magic = u64::from_le_bytes(bytes[1..9].try_into().unwrap());
        magic == MAGIC
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        let desc = BlobDescriptor::new("s3://bucket/path/to/blob.blob".to_string(), 100, 2048);
        let bytes = desc.serialize();
        let deserialized = BlobDescriptor::deserialize(&bytes).unwrap();
        assert_eq!(desc, deserialized);
    }

    #[test]
    fn test_is_blob_descriptor() {
        let desc = BlobDescriptor::new("file:///tmp/test.blob".to_string(), 0, 1024);
        let bytes = desc.serialize();
        assert!(BlobDescriptor::is_blob_descriptor(&bytes));
        assert!(!BlobDescriptor::is_blob_descriptor(&[0u8; 5]));
        assert!(!BlobDescriptor::is_blob_descriptor(b"not a descriptor"));
    }

    #[test]
    fn test_deserialize_rejects_future_version() {
        let mut bytes = BlobDescriptor::new("x".to_string(), 0, 0).serialize();
        bytes[0] = 255;
        assert!(BlobDescriptor::deserialize(&bytes).is_err());
    }

    #[test]
    fn test_deserialize_rejects_negative_uri_length() {
        let mut bytes = BlobDescriptor::new("x".to_string(), 0, 0).serialize();
        // Overwrite uri_length (at offset 9, after version + magic) with -1
        bytes[9..13].copy_from_slice(&(-1_i32).to_le_bytes());
        let err = BlobDescriptor::deserialize(&bytes).unwrap_err();
        assert!(
            matches!(err, Error::DataInvalid { message, .. } if message.contains("negative uri_length"))
        );
    }

    #[test]
    fn test_deserialize_rejects_bad_magic() {
        let mut bytes = BlobDescriptor::new("x".to_string(), 0, 0).serialize();
        bytes[1] = 0xFF;
        assert!(BlobDescriptor::deserialize(&bytes).is_err());
    }
}
