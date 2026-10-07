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

//! Java DeltaVarintCompressor signed-long indexes.

use crate::Error;

pub(crate) fn decode_delta_varints(bytes: &[u8]) -> crate::Result<Vec<i64>> {
    let mut values = Vec::new();
    let mut cursor = 0usize;
    let mut previous = 0_i64;

    while cursor < bytes.len() {
        let (delta, consumed) = decode_varint(&bytes[cursor..])?;
        cursor += consumed;

        let value = if values.is_empty() {
            delta
        } else {
            previous
                .checked_add(delta)
                .ok_or_else(|| Error::DataInvalid {
                    message: format!(
                        "Blob delta-varint index overflow after previous value {previous}"
                    ),
                    source: None,
                })?
        };
        values.push(value);
        previous = value;
    }

    Ok(values)
}

pub(super) fn decode_varint(bytes: &[u8]) -> crate::Result<(i64, usize)> {
    let mut value = 0_u64;
    let mut shift = 0_u32;

    for (idx, byte) in bytes.iter().copied().enumerate() {
        value |= u64::from(byte & 0x7f) << shift;
        if (byte & 0x80) == 0 {
            let decoded = ((value >> 1) as i64) ^ (-((value & 1) as i64));
            return Ok((decoded, idx + 1));
        }

        shift += 7;
        if shift > 63 {
            return Err(Error::DataInvalid {
                message: "Blob delta-varint index overflow".to_string(),
                source: None,
            });
        }
    }

    Err(Error::DataInvalid {
        message: "Unexpected end of blob delta-varint index".to_string(),
        source: None,
    })
}

pub(crate) fn encode_delta_varints(values: &[i64]) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }
    let mut encoded = Vec::new();
    let mut previous = 0_i64;
    for (idx, &value) in values.iter().enumerate() {
        let delta = if idx == 0 { value } else { value - previous };
        previous = value;
        encode_varint(delta, &mut encoded);
    }
    encoded
}

pub(super) fn encode_varint(value: i64, out: &mut Vec<u8>) {
    let mut remaining = ((value << 1) ^ (value >> 63)) as u64;
    while (remaining & !0x7f) != 0 {
        out.push(((remaining & 0x7f) as u8) | 0x80);
        remaining >>= 7;
    }
    out.push(remaining as u8);
}
