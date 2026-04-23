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

/// Zero-copy cursor over Avro binary-encoded data.
pub struct AvroCursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> AvroCursor<'a> {
    #[inline]
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    #[inline]
    #[allow(dead_code)]
    pub fn position(&self) -> usize {
        self.pos
    }

    #[inline]
    pub fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    #[inline]
    fn require(&self, n: usize) -> crate::Result<()> {
        if self.pos + n > self.data.len() {
            return Err(Error::UnexpectedError {
                message: format!(
                    "avro cursor: need {} bytes at offset {}, but only {} remain",
                    n,
                    self.pos,
                    self.remaining()
                ),
                source: None,
            });
        }
        Ok(())
    }

    /// Read a zigzag-encoded variable-length long (Avro int and long encoding).
    #[inline]
    pub fn read_long(&mut self) -> crate::Result<i64> {
        let raw = if self.data.len() - self.pos >= 10 {
            self.read_varint_fast()
        } else {
            self.read_varint_slow()
        }?;
        Ok(((raw >> 1) as i64) ^ -((raw & 1) as i64))
    }

    /// Fast path: no per-byte bounds check (caller guarantees >= 10 bytes remain).
    #[inline]
    fn read_varint_fast(&mut self) -> crate::Result<u64> {
        let buf = &self.data[self.pos..];
        let mut raw: u64 = 0;
        let mut shift: u32 = 0;
        let mut i = 0;
        loop {
            let b = buf[i] as u64;
            i += 1;
            raw |= (b & 0x7F) << shift;
            if b & 0x80 == 0 {
                self.pos += i;
                return Ok(raw);
            }
            shift += 7;
            if shift >= 64 {
                return Err(Error::UnexpectedError {
                    message: "avro cursor: varint overflow".into(),
                    source: None,
                });
            }
        }
    }

    /// Slow path: bounds check each byte (< 10 bytes remaining).
    #[inline]
    fn read_varint_slow(&mut self) -> crate::Result<u64> {
        let mut raw: u64 = 0;
        let mut shift: u32 = 0;
        loop {
            self.require(1)?;
            let b = self.data[self.pos] as u64;
            self.pos += 1;
            raw |= (b & 0x7F) << shift;
            if b & 0x80 == 0 {
                return Ok(raw);
            }
            shift += 7;
            if shift >= 64 {
                return Err(Error::UnexpectedError {
                    message: "avro cursor: varint overflow".into(),
                    source: None,
                });
            }
        }
    }

    #[inline]
    pub fn read_int(&mut self) -> crate::Result<i32> {
        let raw = if self.data.len() - self.pos >= 5 {
            self.read_varint_int_fast()
        } else {
            self.read_varint_slow()
        }?;
        let zigzag = ((raw >> 1) as i64) ^ -((raw & 1) as i64);
        Ok(zigzag as i32)
    }

    /// Fast path for int: no per-byte bounds check (caller guarantees >= 5 bytes remain).
    #[inline]
    fn read_varint_int_fast(&mut self) -> crate::Result<u64> {
        let buf = &self.data[self.pos..];
        let mut raw: u64 = 0;
        let mut shift: u32 = 0;
        let mut i = 0;
        loop {
            let b = buf[i] as u64;
            i += 1;
            raw |= (b & 0x7F) << shift;
            if b & 0x80 == 0 {
                self.pos += i;
                return Ok(raw);
            }
            shift += 7;
            if shift >= 35 {
                return Err(Error::UnexpectedError {
                    message: "avro cursor: int varint overflow".into(),
                    source: None,
                });
            }
        }
    }

    #[inline]
    #[allow(dead_code)]
    pub fn read_boolean(&mut self) -> crate::Result<bool> {
        self.require(1)?;
        let b = self.data[self.pos];
        self.pos += 1;
        Ok(b != 0)
    }

    #[inline]
    #[allow(dead_code)]
    pub fn read_float(&mut self) -> crate::Result<f32> {
        self.require(4)?;
        let bytes: [u8; 4] = self.data[self.pos..self.pos + 4].try_into().unwrap();
        self.pos += 4;
        Ok(f32::from_le_bytes(bytes))
    }

    #[inline]
    #[allow(dead_code)]
    pub fn read_double(&mut self) -> crate::Result<f64> {
        self.require(8)?;
        let bytes: [u8; 8] = self.data[self.pos..self.pos + 8].try_into().unwrap();
        self.pos += 8;
        Ok(f64::from_le_bytes(bytes))
    }

    /// Read Avro bytes: length-prefixed raw bytes, zero-copy.
    #[inline]
    pub fn read_bytes(&mut self) -> crate::Result<&'a [u8]> {
        let raw_len = self.read_long()?;
        if raw_len < 0 {
            return Err(Error::UnexpectedError {
                message: format!("avro cursor: negative bytes length: {raw_len}"),
                source: None,
            });
        }
        let len = raw_len as usize;
        self.require(len)?;
        let slice = &self.data[self.pos..self.pos + len];
        self.pos += len;
        Ok(slice)
    }

    /// Read Avro string: length-prefixed UTF-8, zero-copy.
    #[inline]
    pub fn read_string(&mut self) -> crate::Result<&'a str> {
        let bytes = self.read_bytes()?;
        std::str::from_utf8(bytes).map_err(|e| Error::UnexpectedError {
            message: format!("avro cursor: invalid UTF-8 string: {e}"),
            source: None,
        })
    }

    /// Read a union index (same encoding as long).
    #[inline]
    pub fn read_union_index(&mut self) -> crate::Result<i64> {
        self.read_long()
    }

    /// Skip `n` raw bytes.
    #[inline]
    pub fn skip_raw(&mut self, n: usize) -> crate::Result<()> {
        self.require(n)?;
        self.pos += n;
        Ok(())
    }

    /// Skip an Avro bytes or string value.
    #[inline]
    pub fn skip_bytes(&mut self) -> crate::Result<()> {
        let raw_len = self.read_long()?;
        if raw_len < 0 {
            return Err(Error::UnexpectedError {
                message: format!("avro cursor: negative bytes length: {raw_len}"),
                source: None,
            });
        }
        self.skip_raw(raw_len as usize)
    }

    /// Skip an Avro long/int value.
    #[inline]
    pub fn skip_long(&mut self) -> crate::Result<()> {
        self.read_long().map(|_| ())
    }

    /// Read Avro fixed bytes of known length, zero-copy.
    #[inline]
    pub fn read_fixed(&mut self, len: usize) -> crate::Result<&'a [u8]> {
        self.require(len)?;
        let slice = &self.data[self.pos..self.pos + len];
        self.pos += len;
        Ok(slice)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn zigzag_encode(n: i64) -> Vec<u8> {
        let mut encoded: u64 = ((n << 1) ^ (n >> 63)) as u64;
        let mut buf = Vec::new();
        loop {
            if encoded & !0x7F == 0 {
                buf.push(encoded as u8);
                break;
            }
            buf.push((encoded & 0x7F | 0x80) as u8);
            encoded >>= 7;
        }
        buf
    }

    #[test]
    fn test_read_long_values() {
        for val in [0i64, 1, -1, 42, -42, 127, -128, i64::MAX, i64::MIN] {
            let bytes = zigzag_encode(val);
            let mut cursor = AvroCursor::new(&bytes);
            assert_eq!(cursor.read_long().unwrap(), val, "failed for {val}");
            assert_eq!(cursor.remaining(), 0);
        }
    }

    #[test]
    fn test_read_int() {
        let bytes = zigzag_encode(100);
        let mut cursor = AvroCursor::new(&bytes);
        assert_eq!(cursor.read_int().unwrap(), 100);
    }

    #[test]
    fn test_read_boolean() {
        let mut cursor = AvroCursor::new(&[0, 1]);
        assert!(!cursor.read_boolean().unwrap());
        assert!(cursor.read_boolean().unwrap());
    }

    #[test]
    fn test_read_float_double() {
        let f_bytes = std::f32::consts::PI.to_le_bytes();
        let d_bytes = std::f64::consts::E.to_le_bytes();
        let mut data = Vec::new();
        data.extend_from_slice(&f_bytes);
        data.extend_from_slice(&d_bytes);

        let mut cursor = AvroCursor::new(&data);
        assert!((cursor.read_float().unwrap() - std::f32::consts::PI).abs() < 1e-6);
        assert!((cursor.read_double().unwrap() - std::f64::consts::E).abs() < 1e-10);
    }

    #[test]
    fn test_read_bytes_and_string() {
        let mut data = Vec::new();
        // bytes: length=5, content="hello"
        data.extend_from_slice(&zigzag_encode(5));
        data.extend_from_slice(b"hello");
        // string: length=5, content="world"
        data.extend_from_slice(&zigzag_encode(5));
        data.extend_from_slice(b"world");

        let mut cursor = AvroCursor::new(&data);
        assert_eq!(cursor.read_bytes().unwrap(), b"hello");
        assert_eq!(cursor.read_string().unwrap(), "world");
    }

    #[test]
    fn test_skip() {
        let mut data = Vec::new();
        data.extend_from_slice(&zigzag_encode(3));
        data.extend_from_slice(b"abc");
        data.extend_from_slice(&zigzag_encode(99));

        let mut cursor = AvroCursor::new(&data);
        cursor.skip_bytes().unwrap();
        assert_eq!(cursor.read_int().unwrap(), 99);
    }

    #[test]
    fn test_eof_error() {
        let mut cursor = AvroCursor::new(&[]);
        assert!(cursor.read_long().is_err());
    }
}
