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

//! Java-compatible manifest metadata for bitmap-backed multivalue indexes.
//!
//! The sorted-index metadata remains the prefix so readers which only need the
//! min/max keys can continue to deserialize it. The trailer records the exact
//! array element type, preventing a schema-evolved array from querying keys
//! with the wrong serializer.

use crate::btree::BTreeIndexMeta;
use crate::spec::DataType;
use crate::{Error, Result};

// "MVIM", matching Java `MultiValueIndexFileMeta`.
const MAGIC: i32 = 0x4d56_494d;
const TRAILER_SIZE: usize = std::mem::size_of::<i32>() * 2;

pub(crate) fn serialize_multivalue_index_meta(
    sorted_meta: &BTreeIndexMeta,
    element_type: &DataType,
) -> Result<Vec<u8>> {
    let mut bytes = sorted_meta.serialize();
    let type_bytes = serde_json::to_vec(element_type).map_err(|error| Error::DataInvalid {
        message: "Failed to serialize multivalue index element type".to_string(),
        source: Some(Box::new(error)),
    })?;
    let type_len = i32::try_from(type_bytes.len()).map_err(|error| Error::DataInvalid {
        message: "Multivalue index element type metadata is too large".to_string(),
        source: Some(Box::new(error)),
    })?;
    bytes.extend_from_slice(&type_bytes);
    // Java ByteBuffer uses big-endian byte order by default.
    bytes.extend_from_slice(&type_len.to_be_bytes());
    bytes.extend_from_slice(&MAGIC.to_be_bytes());
    Ok(bytes)
}

pub(crate) fn has_compatible_element_type(
    metadata: Option<&[u8]>,
    element_type: &DataType,
) -> bool {
    let Some(metadata) = metadata else {
        return false;
    };
    let Some(trailer) = metadata.get(metadata.len().saturating_sub(TRAILER_SIZE)..) else {
        return false;
    };
    if trailer.len() != TRAILER_SIZE {
        return false;
    }
    let type_len = i32::from_be_bytes(trailer[..4].try_into().unwrap());
    let magic = i32::from_be_bytes(trailer[4..].try_into().unwrap());
    let Ok(type_len) = usize::try_from(type_len) else {
        return false;
    };
    if magic != MAGIC || type_len > metadata.len().saturating_sub(TRAILER_SIZE) {
        return false;
    }
    let type_offset = metadata.len() - TRAILER_SIZE - type_len;
    serde_json::to_vec(element_type)
        .is_ok_and(|expected| metadata[type_offset..type_offset + type_len] == expected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{BigIntType, VarCharType};

    #[test]
    fn test_multivalue_meta_preserves_sorted_prefix_and_element_type() {
        let sorted = BTreeIndexMeta::new(Some(b"a".to_vec()), Some(b"z".to_vec()), false);
        let string_type = DataType::VarChar(VarCharType::string_type());
        let encoded = serialize_multivalue_index_meta(&sorted, &string_type).unwrap();

        let decoded = BTreeIndexMeta::deserialize(&encoded).unwrap();
        assert_eq!(decoded.first_key, Some(b"a".to_vec()));
        assert_eq!(decoded.last_key, Some(b"z".to_vec()));
        let type_len = i32::from_be_bytes(
            encoded[encoded.len() - 8..encoded.len() - 4]
                .try_into()
                .unwrap(),
        );
        let type_start = encoded.len() - TRAILER_SIZE - usize::try_from(type_len).unwrap();
        assert_eq!(
            &encoded[type_start..encoded.len() - TRAILER_SIZE],
            br#""STRING""#
        );
        assert_eq!(&encoded[encoded.len() - 4..], &MAGIC.to_be_bytes());
        assert!(has_compatible_element_type(Some(&encoded), &string_type));
        assert!(!has_compatible_element_type(
            Some(&encoded),
            &DataType::BigInt(BigIntType::new())
        ));
    }

    #[test]
    fn test_multivalue_meta_rejects_legacy_and_corrupt_trailers() {
        let sorted = BTreeIndexMeta::new(None, None, false);
        let element_type = DataType::BigInt(BigIntType::new());
        assert!(!has_compatible_element_type(
            Some(&sorted.serialize()),
            &element_type
        ));

        let mut encoded = serialize_multivalue_index_meta(&sorted, &element_type).unwrap();
        let last = encoded.len() - 1;
        encoded[last] ^= 1;
        assert!(!has_compatible_element_type(Some(&encoded), &element_type));
    }
}
