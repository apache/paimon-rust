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

//! Primary-key vector (bucket-local ANN) source metadata.
//!
//! Parses the `_SOURCE_META` blob embedded in [`crate::spec::GlobalIndexMeta`],
//! written by Java Paimon's `PkVectorSourceMeta` (apache/paimon#8549). The blob
//! lists the ordered source data files backing an ANN segment; a segment's
//! ordinals concatenate those files in order, so an ordinal maps to a
//! `(data file, physical row position)` pair.
//!
//! Two distinct encodings are involved and must not be conflated: Avro extracts
//! the opaque `_SOURCE_META` bytes (handled in the Avro decoder), and the bytes
//! *inside* use Java `DataOutput` (big-endian ints/longs, modified-UTF-8 via
//! `writeUTF`) — parsed here, independent of Avro.

use crate::spec::GlobalIndexMeta;

fn data_invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

/// The `_SOURCE_META` frame version written by Java `PkVectorSourceMeta`.
const SOURCE_META_VERSION: i32 = 1;

/// One source data file captured when a PK-vector ANN segment was built.
///
/// Mirrors Java `org.apache.paimon.index.pkvector.PkVectorSourceFile`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PkVectorSourceFile {
    file_name: String,
    row_count: i64,
}

impl PkVectorSourceFile {
    pub fn new(file_name: String, row_count: i64) -> crate::Result<Self> {
        if row_count < 0 {
            return Err(data_invalid(format!(
                "source file row count must not be negative: {row_count}"
            )));
        }
        Ok(Self {
            file_name,
            row_count,
        })
    }

    pub fn file_name(&self) -> &str {
        &self.file_name
    }

    pub fn row_count(&self) -> i64 {
        self.row_count
    }
}

/// Ordered source data files for a primary-key vector index payload.
///
/// Mirrors Java `org.apache.paimon.index.pkvector.PkVectorSourceMeta`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PkVectorSourceMeta {
    data_level: i32,
    source_files: Vec<PkVectorSourceFile>,
}

impl PkVectorSourceMeta {
    pub fn new(data_level: i32, source_files: Vec<PkVectorSourceFile>) -> crate::Result<Self> {
        if data_level <= 0 {
            return Err(data_invalid(format!(
                "source meta data level must be positive: {data_level}"
            )));
        }
        if source_files.is_empty() {
            return Err(data_invalid("a vector index must reference source files"));
        }
        Ok(Self {
            data_level,
            source_files,
        })
    }

    pub fn data_level(&self) -> i32 {
        self.data_level
    }

    pub fn source_files(&self) -> &[PkVectorSourceFile] {
        &self.source_files
    }

    /// Build from the `_SOURCE_META` bytes carried on a [`GlobalIndexMeta`].
    /// Errors if the metadata carries no source blob.
    pub fn from_global_index_meta(meta: &GlobalIndexMeta) -> crate::Result<Self> {
        let bytes = meta
            .source_meta
            .as_deref()
            .ok_or_else(|| data_invalid("global index meta has no vector source metadata"))?;
        Self::deserialize(bytes)
    }

    /// Map an ANN segment ordinal to `(data file name, physical row position)`.
    ///
    /// Ordinals concatenate source files in stored order: file `i` owns
    /// `[sum(rows[..i]), sum(rows[..=i]))`. The returned position is local to
    /// its file.
    pub fn resolve(&self, ordinal: i64) -> crate::Result<(String, i64)> {
        if ordinal < 0 {
            return Err(data_invalid(format!(
                "vector ordinal must not be negative: {ordinal}"
            )));
        }
        let mut cumulative: i64 = 0;
        for file in &self.source_files {
            let next = cumulative
                .checked_add(file.row_count)
                .ok_or_else(|| data_invalid("vector source row counts overflow i64"))?;
            if ordinal < next {
                return Ok((file.file_name.clone(), ordinal - cumulative));
            }
            cumulative = next;
        }
        Err(data_invalid(format!(
            "vector ordinal {ordinal} is out of range (total rows {cumulative})"
        )))
    }

    /// Parse a Java `PkVectorSourceMeta`-serialized `_SOURCE_META` blob.
    pub fn deserialize(bytes: &[u8]) -> crate::Result<Self> {
        let mut cursor = DataInputCursor::new(bytes);
        let version = cursor.read_i32_be()?;
        if version != SOURCE_META_VERSION {
            return Err(data_invalid(format!(
                "unsupported vector source version: {version}"
            )));
        }
        let data_level = cursor.read_i32_be()?;
        if data_level <= 0 {
            return Err(data_invalid(format!(
                "source meta data level must be positive: {data_level}"
            )));
        }
        let count = cursor.read_i32_be()?;
        if count <= 0 {
            return Err(data_invalid("a vector index must reference source files"));
        }
        // NOT Vec::with_capacity(count): count is untrusted and may be huge.
        let mut source_files = Vec::new();
        for _ in 0..count {
            let file_name = read_java_utf(&mut cursor)?;
            let row_count = cursor.read_i64_be()?;
            source_files.push(PkVectorSourceFile::new(file_name, row_count)?);
        }
        if cursor.remaining() != 0 {
            return Err(data_invalid(
                "unexpected trailing bytes in vector source metadata",
            ));
        }
        Self::new(data_level, source_files)
    }
}

/// Minimal big-endian reader mirroring the Java `DataInput` primitives the
/// `_SOURCE_META` frame uses. Module-private by design: no shared `common/`
/// abstraction until a second consumer exists.
struct DataInputCursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> DataInputCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }

    fn read_exact(&mut self, len: usize) -> crate::Result<&'a [u8]> {
        if self.remaining() < len {
            return Err(data_invalid(format!(
                "unexpected end of vector source metadata: need {len} bytes, {} remain",
                self.remaining()
            )));
        }
        let slice = &self.bytes[self.position..self.position + len];
        self.position += len;
        Ok(slice)
    }

    fn read_u16_be(&mut self) -> crate::Result<u16> {
        let b = self.read_exact(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    fn read_i32_be(&mut self) -> crate::Result<i32> {
        let b = self.read_exact(4)?;
        Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn read_i64_be(&mut self) -> crate::Result<i64> {
        let b = self.read_exact(8)?;
        Ok(i64::from_be_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }
}

/// Decode one Java `DataOutput#writeUTF` value: a u16-BE byte length followed by
/// modified UTF-8. Reassembles UTF-16 code units, then `String::from_utf16`.
///
/// A Java-written NUL is the two-byte sequence `C0 80`; a raw `0x00` is accepted
/// (Java `DataInputStream#readUTF` treats `0x00..=0x7F` as single-byte chars).
fn read_java_utf(cursor: &mut DataInputCursor<'_>) -> crate::Result<String> {
    let len = cursor.read_u16_be()? as usize;
    let bytes = cursor.read_exact(len)?;
    let mut units: Vec<u16> = Vec::new();
    let mut i = 0;
    while i < len {
        let b0 = bytes[i];
        if b0 & 0x80 == 0 {
            // 1-byte: 0xxxxxxx (includes raw 0x00)
            units.push(b0 as u16);
            i += 1;
        } else if b0 & 0xE0 == 0xC0 {
            // 2-byte: 110xxxxx 10xxxxxx
            if i + 1 >= len || bytes[i + 1] & 0xC0 != 0x80 {
                return Err(data_invalid("malformed modified UTF-8 (2-byte)"));
            }
            let u = (((b0 & 0x1F) as u16) << 6) | ((bytes[i + 1] & 0x3F) as u16);
            units.push(u);
            i += 2;
        } else if b0 & 0xF0 == 0xE0 {
            // 3-byte: 1110xxxx 10xxxxxx 10xxxxxx
            if i + 2 >= len || bytes[i + 1] & 0xC0 != 0x80 || bytes[i + 2] & 0xC0 != 0x80 {
                return Err(data_invalid("malformed modified UTF-8 (3-byte)"));
            }
            let u = (((b0 & 0x0F) as u16) << 12)
                | (((bytes[i + 1] & 0x3F) as u16) << 6)
                | ((bytes[i + 2] & 0x3F) as u16);
            units.push(u);
            i += 3;
        } else {
            return Err(data_invalid("malformed modified UTF-8 (lead byte)"));
        }
    }
    String::from_utf16(&units)
        .map_err(|_| data_invalid("modified UTF-8 did not decode to valid UTF-16"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a Java `writeUTF` byte sequence (u16-BE length + modified UTF-8)
    /// for ASCII/BMP test strings. Encodes via UTF-16 code units to match Java.
    fn java_write_utf(s: &str) -> Vec<u8> {
        let mut body = Vec::new();
        for c in s.encode_utf16() {
            if (0x0001..=0x007F).contains(&c) {
                body.push(c as u8);
            } else if c > 0x07FF {
                body.push(0xE0 | (c >> 12) as u8);
                body.push(0x80 | ((c >> 6) & 0x3F) as u8);
                body.push(0x80 | (c & 0x3F) as u8);
            } else {
                body.push(0xC0 | (c >> 6) as u8);
                body.push(0x80 | (c & 0x3F) as u8);
            }
        }
        let mut out = (body.len() as u16).to_be_bytes().to_vec();
        out.extend_from_slice(&body);
        out
    }

    fn frame(data_level: i32, files: &[(&str, i64)]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&1i32.to_be_bytes()); // version
        out.extend_from_slice(&data_level.to_be_bytes());
        out.extend_from_slice(&(files.len() as i32).to_be_bytes());
        for (name, rows) in files {
            out.extend_from_slice(&java_write_utf(name));
            out.extend_from_slice(&rows.to_be_bytes());
        }
        out
    }

    #[test]
    fn read_java_utf_round_trips_ascii_and_multibyte() {
        for s in ["data-abc.parquet", "café", "ü", "日本語", "🚀"] {
            let bytes = java_write_utf(s);
            let mut cursor = DataInputCursor::new(&bytes);
            assert_eq!(read_java_utf(&mut cursor).unwrap(), s);
            assert_eq!(cursor.remaining(), 0);
        }
    }

    #[test]
    fn read_java_utf_decodes_c0_80_as_nul() {
        // Java writeUTF encodes U+0000 as 0xC0 0x80.
        let bytes = [0x00, 0x02, 0xC0, 0x80];
        let mut cursor = DataInputCursor::new(&bytes);
        assert_eq!(read_java_utf(&mut cursor).unwrap(), "\u{0000}");
    }

    #[test]
    fn read_java_utf_accepts_raw_nul() {
        // Raw 0x00 is a valid single-byte char for Java readUTF; must not error.
        let bytes = [0x00, 0x01, 0x00];
        let mut cursor = DataInputCursor::new(&bytes);
        assert_eq!(read_java_utf(&mut cursor).unwrap(), "\u{0000}");
    }

    #[test]
    fn read_java_utf_rejects_truncated_and_bad_continuation() {
        // 2-byte lead with no continuation.
        let mut c1 = DataInputCursor::new(&[0x00, 0x01, 0xC0]);
        assert!(read_java_utf(&mut c1).is_err());
        // 3-byte lead with bad 2nd byte.
        let mut c2 = DataInputCursor::new(&[0x00, 0x03, 0xE0, 0x00, 0x80]);
        assert!(read_java_utf(&mut c2).is_err());
    }

    #[test]
    fn deserialize_single_source_file() {
        let bytes = frame(1, &[("data-abc.parquet", 100)]);
        let meta = PkVectorSourceMeta::deserialize(&bytes).unwrap();
        assert_eq!(meta.data_level(), 1);
        assert_eq!(meta.source_files().len(), 1);
        assert_eq!(meta.source_files()[0].file_name(), "data-abc.parquet");
        assert_eq!(meta.source_files()[0].row_count(), 100);
    }

    #[test]
    fn deserialize_multi_source_files() {
        let bytes = frame(2, &[("f0", 3), ("f1", 5)]);
        let meta = PkVectorSourceMeta::deserialize(&bytes).unwrap();
        assert_eq!(meta.data_level(), 2);
        assert_eq!(meta.source_files().len(), 2);
        assert_eq!(meta.source_files()[1].row_count(), 5);
    }

    #[test]
    fn deserialize_rejects_bad_version() {
        let mut bytes = frame(1, &[("f0", 1)]);
        bytes[0..4].copy_from_slice(&2i32.to_be_bytes());
        assert!(PkVectorSourceMeta::deserialize(&bytes).is_err());
    }

    #[test]
    fn deserialize_rejects_zero_or_negative_data_level() {
        assert!(PkVectorSourceMeta::deserialize(&frame(0, &[("f0", 1)])).is_err());
        assert!(PkVectorSourceMeta::deserialize(&frame(-1, &[("f0", 1)])).is_err());
    }

    #[test]
    fn deserialize_rejects_zero_count() {
        let mut out = Vec::new();
        out.extend_from_slice(&1i32.to_be_bytes());
        out.extend_from_slice(&1i32.to_be_bytes());
        out.extend_from_slice(&0i32.to_be_bytes());
        assert!(PkVectorSourceMeta::deserialize(&out).is_err());
    }

    #[test]
    fn deserialize_rejects_trailing_bytes() {
        let mut bytes = frame(1, &[("f0", 1)]);
        bytes.push(0xFF);
        assert!(PkVectorSourceMeta::deserialize(&bytes).is_err());
    }

    #[test]
    fn deserialize_rejects_truncated_input() {
        let bytes = frame(1, &[("f0", 1)]);
        assert!(PkVectorSourceMeta::deserialize(&bytes[..bytes.len() - 2]).is_err());
    }

    #[test]
    fn deserialize_rejects_negative_row_count() {
        let bytes = frame(1, &[("f0", -1)]);
        assert!(PkVectorSourceMeta::deserialize(&bytes).is_err());
    }

    #[test]
    fn new_rejects_empty() {
        assert!(PkVectorSourceMeta::new(1, Vec::new()).is_err());
        assert!(PkVectorSourceMeta::new(
            0,
            vec![PkVectorSourceFile::new("f0".to_string(), 1).unwrap()]
        )
        .is_err());
    }

    #[test]
    fn resolve_single_file() {
        let meta = PkVectorSourceMeta::deserialize(&frame(1, &[("f0", 3)])).unwrap();
        assert_eq!(meta.resolve(0).unwrap(), ("f0".to_string(), 0));
        assert_eq!(meta.resolve(2).unwrap(), ("f0".to_string(), 2));
    }

    #[test]
    fn resolve_multi_file_prefix_sum_boundaries() {
        // f0 owns ordinals 0..=2, f1 owns 3..=7.
        let meta = PkVectorSourceMeta::deserialize(&frame(1, &[("f0", 3), ("f1", 5)])).unwrap();
        assert_eq!(meta.resolve(2).unwrap(), ("f0".to_string(), 2)); // last of f0
        assert_eq!(meta.resolve(3).unwrap(), ("f1".to_string(), 0)); // first of f1
        assert_eq!(meta.resolve(7).unwrap(), ("f1".to_string(), 4)); // last of f1
    }

    #[test]
    fn resolve_rejects_negative_ordinal() {
        let meta = PkVectorSourceMeta::deserialize(&frame(1, &[("f0", 3)])).unwrap();
        assert!(meta.resolve(-1).is_err());
    }

    #[test]
    fn resolve_rejects_ordinal_at_or_past_total() {
        let meta = PkVectorSourceMeta::deserialize(&frame(1, &[("f0", 3)])).unwrap();
        assert!(meta.resolve(3).is_err()); // total == 3, valid range 0..=2
    }

    #[test]
    fn from_global_index_meta_errors_without_source_meta() {
        let meta = GlobalIndexMeta {
            row_range_start: 0,
            row_range_end: 0,
            index_field_id: 0,
            extra_field_ids: None,
            index_meta: None,
            source_meta: None,
        };
        assert!(PkVectorSourceMeta::from_global_index_meta(&meta).is_err());
    }

    #[test]
    fn from_global_index_meta_parses_source_meta() {
        let meta = GlobalIndexMeta {
            row_range_start: 0,
            row_range_end: 0,
            index_field_id: 0,
            extra_field_ids: None,
            index_meta: None,
            source_meta: Some(frame(1, &[("f0", 3)])),
        };
        let parsed = PkVectorSourceMeta::from_global_index_meta(&meta).unwrap();
        assert_eq!(parsed.data_level(), 1);
        assert_eq!(parsed.source_files()[0].file_name(), "f0");
    }

    #[test]
    fn resolve_rejects_row_count_overflow() {
        // Two individually-valid row counts whose prefix sum overflows i64.
        // Resolving past the first file forces the checked_add on the second.
        let meta = PkVectorSourceMeta::new(
            1,
            vec![
                PkVectorSourceFile::new("f0".to_string(), i64::MAX).unwrap(),
                PkVectorSourceFile::new("f1".to_string(), 1).unwrap(),
            ],
        )
        .unwrap();
        assert!(meta.resolve(i64::MAX).is_err());
    }

    #[test]
    fn read_java_utf_rejects_lone_high_surrogate() {
        // U+D800 (a lone high surrogate) as 3-byte modified UTF-8: ED A0 80.
        // It decodes to a single UTF-16 code unit that is not valid on its own,
        // so String::from_utf16 must reject it (not panic).
        let bytes = [0x00, 0x03, 0xED, 0xA0, 0x80];
        let mut cursor = DataInputCursor::new(&bytes);
        assert!(read_java_utf(&mut cursor).is_err());
    }
}
