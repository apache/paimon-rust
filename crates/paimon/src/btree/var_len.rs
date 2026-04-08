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

//! Variable-length integer encoding compatible with Java Paimon's VarLengthIntUtils.
//!
//! Uses unsigned LEB128 encoding: each byte stores 7 data bits + 1 continuation bit.

use std::io::{self, Read, Write};

/// Encode a non-negative i32 as var-length bytes.
pub fn encode_var_int(out: &mut impl Write, value: i32) -> io::Result<usize> {
    debug_assert!(value >= 0, "negative value: {value}");
    let mut v = value as u32;
    let mut count = 0;
    while (v & !0x7F) != 0 {
        out.write_all(&[((v & 0x7F) | 0x80) as u8])?;
        v >>= 7;
        count += 1;
    }
    out.write_all(&[v as u8])?;
    Ok(count + 1)
}

/// Decode a var-length encoded i32.
pub fn decode_var_int(input: &mut impl Read) -> io::Result<i32> {
    let mut result: u32 = 0;
    let mut buf = [0u8; 1];
    for offset in (0..32).step_by(7) {
        input.read_exact(&mut buf)?;
        let b = buf[0] as u32;
        result |= (b & 0x7F) << offset;
        if (b & 0x80) == 0 {
            return Ok(result as i32);
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "Malformed integer",
    ))
}

/// Encode a non-negative i64 as var-length bytes.
pub fn encode_var_long(out: &mut impl Write, value: i64) -> io::Result<usize> {
    debug_assert!(value >= 0, "negative value: {value}");
    let mut v = value as u64;
    let mut count = 0;
    while (v & !0x7F) != 0 {
        out.write_all(&[((v & 0x7F) | 0x80) as u8])?;
        v >>= 7;
        count += 1;
    }
    out.write_all(&[v as u8])?;
    Ok(count + 1)
}

/// Decode a var-length encoded i64.
pub fn decode_var_long(input: &mut impl Read) -> io::Result<i64> {
    let mut result: u64 = 0;
    let mut buf = [0u8; 1];
    for offset in (0..64).step_by(7) {
        input.read_exact(&mut buf)?;
        let b = buf[0] as u64;
        result |= (b & 0x7F) << offset;
        if (b & 0x80) == 0 {
            return Ok(result as i64);
        }
    }
    Err(io::Error::new(io::ErrorKind::InvalidData, "Malformed long"))
}

/// Encode var-int into a byte slice, returning bytes written.
pub fn encode_var_int_to_slice(bytes: &mut [u8], offset: usize, value: i32) -> usize {
    debug_assert!(value >= 0, "negative value: {value}");
    let mut v = value as u32;
    let mut i = 0;
    while (v & !0x7F) != 0 {
        bytes[offset + i] = ((v & 0x7F) | 0x80) as u8;
        v >>= 7;
        i += 1;
    }
    bytes[offset + i] = v as u8;
    i + 1
}

/// Decode var-int from a byte slice, returning (value, bytes_consumed).
pub fn decode_var_int_from_slice(bytes: &[u8], offset: usize) -> (i32, usize) {
    let mut result: u32 = 0;
    let mut i = 0;
    for shift in (0..32).step_by(7) {
        let b = bytes[offset + i] as u32;
        result |= (b & 0x7F) << shift;
        i += 1;
        if (b & 0x80) == 0 {
            return (result as i32, i);
        }
    }
    panic!("Malformed integer");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_var_int_roundtrip() {
        for &v in &[0, 1, 127, 128, 255, 16383, 16384, i32::MAX] {
            let mut buf = Vec::new();
            encode_var_int(&mut buf, v).unwrap();
            let decoded = decode_var_int(&mut Cursor::new(&buf)).unwrap();
            assert_eq!(v, decoded, "failed for {v}");
        }
    }

    #[test]
    fn test_var_long_roundtrip() {
        for &v in &[0i64, 1, 127, 128, 16384, i64::MAX] {
            let mut buf = Vec::new();
            encode_var_long(&mut buf, v).unwrap();
            let decoded = decode_var_long(&mut Cursor::new(&buf)).unwrap();
            assert_eq!(v, decoded, "failed for {v}");
        }
    }

    #[test]
    fn test_var_int_slice_roundtrip() {
        let mut buf = [0u8; 10];
        let written = encode_var_int_to_slice(&mut buf, 0, 300);
        let (decoded, consumed) = decode_var_int_from_slice(&buf, 0);
        assert_eq!(300, decoded);
        assert_eq!(written, consumed);
    }
}
