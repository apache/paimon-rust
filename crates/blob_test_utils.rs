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

use std::fs;
use std::path::Path;

const BLOB_MAGIC_NUMBER_BYTES: [u8; 4] = 1481511375_i32.to_le_bytes();
const BLOB_ENTRY_OVERHEAD: usize = 16;
const BLOB_FORMAT_VERSION: u8 = 1;

pub(crate) fn build_blob_file_bytes(rows: &[Option<&[u8]>]) -> Vec<u8> {
    let mut file_bytes = Vec::new();
    let mut lengths = Vec::with_capacity(rows.len());

    for row in rows {
        match row {
            Some(payload) => {
                let entry_length = payload
                    .len()
                    .checked_add(BLOB_ENTRY_OVERHEAD)
                    .and_then(|len| i64::try_from(len).ok())
                    .unwrap_or_else(|| {
                        panic!("Blob payload length {} exceeds test helper limits", payload.len())
                    });
                lengths.push(entry_length);

                file_bytes.extend_from_slice(&BLOB_MAGIC_NUMBER_BYTES);
                file_bytes.extend_from_slice(payload);
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(&BLOB_MAGIC_NUMBER_BYTES);
                hasher.update(payload);

                let entry_length_bytes = entry_length.to_le_bytes();
                file_bytes.extend_from_slice(&entry_length_bytes);
                hasher.update(&entry_length_bytes);
                file_bytes.extend_from_slice(&hasher.finalize().to_le_bytes());
            }
            None => lengths.push(-1),
        }
    }

    let index_bytes = encode_delta_varints(&lengths);
    let index_length = i32::try_from(index_bytes.len()).unwrap_or_else(|_| {
        panic!(
            "Blob index length {} exceeds test helper limits",
            index_bytes.len()
        )
    });
    file_bytes.extend_from_slice(&index_bytes);
    file_bytes.extend_from_slice(&index_length.to_le_bytes());
    file_bytes.push(BLOB_FORMAT_VERSION);
    file_bytes
}

pub(crate) fn write_blob_file(path: &Path, rows: &[Option<&[u8]>]) {
    let file_bytes = build_blob_file_bytes(rows);
    fs::write(path, file_bytes)
        .unwrap_or_else(|e| panic!("Failed to write blob test file {path:?}: {e}"));
}

pub(crate) fn encode_delta_varints(values: &[i64]) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }

    let mut encoded = Vec::new();
    let mut previous = 0_i64;
    for (idx, value) in values.iter().copied().enumerate() {
        let delta = if idx == 0 { value } else { value - previous };
        previous = value;
        encode_varint(delta, &mut encoded);
    }
    encoded
}

fn encode_varint(value: i64, out: &mut Vec<u8>) {
    let mut remaining = ((value << 1) ^ (value >> 63)) as u64;
    while (remaining & !0x7f) != 0 {
        out.push(((remaining & 0x7f) as u8) | 0x80);
        remaining >>= 7;
    }
    out.push(remaining as u8);
}
