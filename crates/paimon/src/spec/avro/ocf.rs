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

use super::cursor::AvroCursor;
use super::decode::neg_count_to_usize;
use crate::Error;
use std::borrow::Cow;
use std::collections::HashMap;

const AVRO_MAGIC: &[u8; 4] = b"Obj\x01";
const SYNC_MARKER_LEN: usize = 16;

/// A decoded Avro OCF header.
pub struct OcfHeader {
    pub schema_json: String,
    pub codec: OcfCodec,
    pub sync_marker: [u8; SYNC_MARKER_LEN],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OcfCodec {
    Null,
    Snappy,
    Zstandard,
}

/// A single data block from the OCF.
pub struct OcfBlock<'a> {
    pub object_count: usize,
    pub data: Cow<'a, [u8]>,
}

/// Streaming iterator over OCF blocks with lazy decompression and reusable decoder state.
pub struct OcfBlockIter<'a> {
    cursor: AvroCursor<'a>,
    codec: OcfCodec,
    sync_marker: [u8; SYNC_MARKER_LEN],
    snappy_decoder: snap::raw::Decoder,
}

impl<'a> OcfBlockIter<'a> {
    fn new(cursor: AvroCursor<'a>, codec: OcfCodec, sync_marker: [u8; SYNC_MARKER_LEN]) -> Self {
        Self {
            cursor,
            codec,
            sync_marker,
            snappy_decoder: snap::raw::Decoder::new(),
        }
    }

    pub fn next_block(&mut self) -> crate::Result<Option<OcfBlock<'a>>> {
        if self.cursor.remaining() == 0 {
            return Ok(None);
        }

        let raw_object_count = self.cursor.read_long()?;
        if raw_object_count < 0 {
            return Err(Error::UnexpectedError {
                message: format!("avro ocf: negative object count: {raw_object_count}"),
                source: None,
            });
        }
        let object_count = raw_object_count as usize;
        let raw_compressed_size = self.cursor.read_long()?;
        if raw_compressed_size < 0 {
            return Err(Error::UnexpectedError {
                message: format!("avro ocf: negative compressed size: {raw_compressed_size}"),
                source: None,
            });
        }
        let compressed_size = raw_compressed_size as usize;
        let compressed_data = self.cursor.read_fixed(compressed_size)?;

        let data = self.decompress(compressed_data)?;

        let block_sync = self.cursor.read_fixed(SYNC_MARKER_LEN)?;
        if block_sync != self.sync_marker {
            return Err(Error::UnexpectedError {
                message: "avro ocf: sync marker mismatch".into(),
                source: None,
            });
        }

        Ok(Some(OcfBlock { object_count, data }))
    }

    fn decompress(&mut self, data: &'a [u8]) -> crate::Result<Cow<'a, [u8]>> {
        match self.codec {
            OcfCodec::Null => Ok(Cow::Borrowed(data)),
            OcfCodec::Snappy => {
                if data.len() < 4 {
                    return Err(Error::UnexpectedError {
                        message: "avro ocf: snappy block too short for CRC".into(),
                        source: None,
                    });
                }
                let compressed = &data[..data.len() - 4];
                let expected_crc = u32::from_be_bytes(data[data.len() - 4..].try_into().unwrap());
                let decompressed = self
                    .snappy_decoder
                    .decompress_vec(compressed)
                    .map_err(|e| Error::UnexpectedError {
                        message: format!("avro ocf: snappy decompression failed: {e}"),
                        source: None,
                    })?;
                let actual_crc = crc32fast::hash(&decompressed);
                if actual_crc != expected_crc {
                    return Err(Error::UnexpectedError {
                        message: format!(
                            "avro ocf: snappy CRC32C mismatch: expected {expected_crc:#010x}, got {actual_crc:#010x}"
                        ),
                        source: None,
                    });
                }
                Ok(Cow::Owned(decompressed))
            }
            OcfCodec::Zstandard => {
                let decompressed =
                    zstd::stream::decode_all(data).map_err(|e| Error::UnexpectedError {
                        message: format!("avro ocf: zstd decompression failed: {e}"),
                        source: None,
                    })?;
                Ok(Cow::Owned(decompressed))
            }
        }
    }
}

/// Parse an Avro OCF header and return a streaming block iterator.
pub fn parse_ocf_streaming(bytes: &[u8]) -> crate::Result<(OcfHeader, OcfBlockIter<'_>)> {
    let mut cursor = AvroCursor::new(bytes);

    let magic = cursor.read_fixed(4)?;
    if magic != AVRO_MAGIC {
        return Err(Error::UnexpectedError {
            message: "avro ocf: invalid magic bytes".into(),
            source: None,
        });
    }

    let meta = read_avro_map(&mut cursor)?;

    let schema_json = meta
        .get("avro.schema")
        .ok_or_else(|| Error::UnexpectedError {
            message: "avro ocf: missing avro.schema in header".into(),
            source: None,
        })?
        .clone();

    let codec = match meta.get("avro.codec").map(|s| s.as_str()) {
        None | Some("null") => OcfCodec::Null,
        Some("snappy") => OcfCodec::Snappy,
        Some("zstandard") => OcfCodec::Zstandard,
        Some(other) => {
            return Err(Error::UnexpectedError {
                message: format!("avro ocf: unsupported codec: {other}"),
                source: None,
            });
        }
    };

    let sync_marker: [u8; SYNC_MARKER_LEN] =
        cursor.read_fixed(SYNC_MARKER_LEN)?.try_into().unwrap();

    let header = OcfHeader {
        schema_json,
        codec,
        sync_marker,
    };

    let iter = OcfBlockIter::new(cursor, header.codec, header.sync_marker);
    Ok((header, iter))
}

/// Parse an Avro OCF file into header + blocks (eagerly decompresses all blocks).
#[cfg(test)]
pub fn parse_ocf(bytes: &[u8]) -> crate::Result<(OcfHeader, Vec<OcfBlock<'_>>)> {
    let (header, mut iter) = parse_ocf_streaming(bytes)?;
    let mut blocks = Vec::new();
    while let Some(block) = iter.next_block()? {
        blocks.push(block);
    }
    Ok((header, blocks))
}

/// Read an Avro-encoded map (used for OCF file metadata).
/// Map encoding: series of blocks, each block: count(long), entries...; terminated by 0-count.
fn read_avro_map(cursor: &mut AvroCursor) -> crate::Result<HashMap<String, String>> {
    let mut map = HashMap::new();
    loop {
        let count = cursor.read_long()?;
        if count == 0 {
            break;
        }
        let count = if count < 0 {
            // Negative count means the block size in bytes follows (we skip it).
            cursor.skip_long()?;
            neg_count_to_usize(count)?
        } else {
            count as usize
        };
        for _ in 0..count {
            let key = cursor.read_string()?.to_string();
            let value_bytes = cursor.read_bytes()?;
            let value = String::from_utf8_lossy(value_bytes).into_owned();
            map.insert(key, value);
        }
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ocf_roundtrip() {
        // Write a simple OCF with apache-avro, then parse with our reader
        use apache_avro::{Codec, Schema, Writer};

        let schema = Schema::parse_str(r#"{"type": "record", "name": "test", "fields": [{"name": "a", "type": "int"}, {"name": "b", "type": "string"}]}"#).unwrap();
        let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Null);
        let mut record = apache_avro::types::Record::new(&schema).unwrap();
        record.put("a", 42i32);
        record.put("b", "hello");
        writer.append(record).unwrap();
        let mut record2 = apache_avro::types::Record::new(&schema).unwrap();
        record2.put("a", 99i32);
        record2.put("b", "world");
        writer.append(record2).unwrap();
        let bytes = writer.into_inner().unwrap();

        let (header, blocks) = parse_ocf(&bytes).unwrap();
        assert_eq!(header.codec, OcfCodec::Null);
        assert!(header.schema_json.contains("test"));

        let total_objects: usize = blocks.iter().map(|b| b.object_count).sum();
        assert_eq!(total_objects, 2);
    }

    #[test]
    fn test_parse_ocf_zstd() {
        use apache_avro::{Codec, Schema, Writer};

        let schema = Schema::parse_str(
            r#"{"type": "record", "name": "test", "fields": [{"name": "x", "type": "long"}]}"#,
        )
        .unwrap();
        let mut writer = Writer::with_codec(
            &schema,
            Vec::new(),
            Codec::Zstandard(apache_avro::ZstandardSettings::default()),
        );
        let mut record = apache_avro::types::Record::new(&schema).unwrap();
        record.put("x", 67890i64);
        writer.append(record).unwrap();
        let bytes = writer.into_inner().unwrap();

        let (header, blocks) = parse_ocf(&bytes).unwrap();
        assert_eq!(header.codec, OcfCodec::Zstandard);
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].object_count, 1);
    }

    #[test]
    fn test_parse_ocf_snappy() {
        use apache_avro::{Codec, Schema, Writer};

        let schema = Schema::parse_str(
            r#"{"type": "record", "name": "test", "fields": [{"name": "x", "type": "long"}]}"#,
        )
        .unwrap();
        let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Snappy);
        let mut record = apache_avro::types::Record::new(&schema).unwrap();
        record.put("x", 12345i64);
        writer.append(record).unwrap();
        let bytes = writer.into_inner().unwrap();

        let (header, blocks) = parse_ocf(&bytes).unwrap();
        assert_eq!(header.codec, OcfCodec::Snappy);
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].object_count, 1);
    }
}
