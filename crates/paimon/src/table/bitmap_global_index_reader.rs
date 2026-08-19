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

//! Reader for Java Paimon's bitmap global index file format.
//!
//! Reference: `org.apache.paimon.globalindex.bitmap.BitmapGlobalIndexFormat`.

use super::bitmap_global_index_format::{
    block_info, is_bitmap_floating_residual_sensitive_op, make_bitmap_key_comparator,
    serialize_bitmap_datum, BlockInfo, BLOCK_TRAILER_LENGTH, FOOTER_LENGTH, MAGIC, VERSION,
};
#[cfg(test)]
use super::bitmap_global_index_writer::BitmapGlobalIndexWriter;
use crate::btree::var_len::{decode_var_int, decode_var_long};
use crate::btree::{compute_crc32, decompress_block, BlockCompressionType};
use crate::io::FileRead;
use crate::spec::{like_match, DataType, Datum, PredicateOperator};
#[cfg(test)]
use bytes::Bytes;
use roaring::RoaringTreemap;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::io::{self, Cursor, Read};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct DictionaryBlockMeta {
    first_key: Vec<u8>,
    block: BlockInfo,
}

#[derive(Clone)]
struct DictionaryEntry {
    key: Vec<u8>,
    bitmap_block: BlockInfo,
}

struct Footer {
    null_rows_block: BlockInfo,
    non_null_rows_block: BlockInfo,
    index_block: BlockInfo,
}

pub(crate) struct BitmapGlobalIndexReader {
    reader: Box<dyn FileRead>,
    footer: Footer,
    dictionary_blocks: Vec<DictionaryBlockMeta>,
    dictionary_block_cache: Mutex<HashMap<BlockInfo, Arc<Vec<DictionaryEntry>>>>,
}

impl BitmapGlobalIndexReader {
    pub(crate) async fn open(reader: Box<dyn FileRead>, file_size: u64) -> io::Result<Self> {
        let footer = read_footer(reader.as_ref(), file_size).await?;
        let index_block = read_compressible_block(reader.as_ref(), footer.index_block).await?;
        let dictionary_blocks = read_index_block(&index_block)?;
        Ok(Self {
            reader,
            footer,
            dictionary_blocks,
            dictionary_block_cache: Mutex::new(HashMap::new()),
        })
    }

    pub(crate) async fn query(
        &self,
        op: PredicateOperator,
        literals: &[Datum],
        data_type: &DataType,
    ) -> io::Result<RoaringTreemap> {
        if is_floating_point(data_type) && is_bitmap_floating_residual_sensitive_op(op) {
            return self.is_not_null().await;
        }
        match op {
            PredicateOperator::ArrayContains => {
                let key = serialize_bitmap_datum(&literals[0], data_type);
                self.equal(&key, data_type).await
            }
            PredicateOperator::ArraysOverlap => {
                let keys = literals
                    .iter()
                    .map(|literal| serialize_bitmap_datum(literal, data_type))
                    .collect::<Vec<_>>();
                self.in_keys(&keys, data_type).await
            }
            PredicateOperator::ArrayContainsAll => {
                let mut keys = literals
                    .iter()
                    .map(|literal| serialize_bitmap_datum(literal, data_type))
                    .collect::<Vec<_>>();
                keys.sort();
                keys.dedup();
                let mut result = None;
                for key in keys {
                    let current = self.equal(&key, data_type).await?;
                    result = Some(match result {
                        None => current,
                        Some(mut existing) => {
                            existing &= current;
                            existing
                        }
                    });
                }
                Ok(result.unwrap_or_default())
            }
            PredicateOperator::Eq => {
                let key = serialize_bitmap_datum(&literals[0], data_type);
                self.equal(&key, data_type).await
            }
            PredicateOperator::NotEq => {
                let mut result = self.is_not_null().await?;
                let key = serialize_bitmap_datum(&literals[0], data_type);
                result -= self.equal(&key, data_type).await?;
                Ok(result)
            }
            PredicateOperator::In => {
                let keys = literals
                    .iter()
                    .map(|literal| serialize_bitmap_datum(literal, data_type))
                    .collect::<Vec<_>>();
                self.in_keys(&keys, data_type).await
            }
            PredicateOperator::NotIn => {
                let mut result = self.is_not_null().await?;
                let keys = literals
                    .iter()
                    .map(|literal| serialize_bitmap_datum(literal, data_type))
                    .collect::<Vec<_>>();
                result -= self.in_keys(&keys, data_type).await?;
                Ok(result)
            }
            PredicateOperator::IsNull => self.is_null().await,
            PredicateOperator::IsNotNull => self.is_not_null().await,
            PredicateOperator::Lt => {
                let key = serialize_bitmap_datum(&literals[0], data_type);
                self.scan_dictionary(data_type, |candidate, cmp| cmp(candidate, &key).is_lt())
                    .await
            }
            PredicateOperator::LtEq => {
                let key = serialize_bitmap_datum(&literals[0], data_type);
                self.scan_dictionary(data_type, |candidate, cmp| !cmp(candidate, &key).is_gt())
                    .await
            }
            PredicateOperator::Gt => {
                let key = serialize_bitmap_datum(&literals[0], data_type);
                self.scan_dictionary(data_type, |candidate, cmp| cmp(candidate, &key).is_gt())
                    .await
            }
            PredicateOperator::GtEq => {
                let key = serialize_bitmap_datum(&literals[0], data_type);
                self.scan_dictionary(data_type, |candidate, cmp| !cmp(candidate, &key).is_lt())
                    .await
            }
            PredicateOperator::Between => {
                let from = serialize_bitmap_datum(&literals[0], data_type);
                let to = serialize_bitmap_datum(&literals[1], data_type);
                self.scan_dictionary(data_type, |candidate, cmp| {
                    !cmp(candidate, &from).is_lt() && !cmp(candidate, &to).is_gt()
                })
                .await
            }
            PredicateOperator::NotBetween => {
                let mut result = self.is_not_null().await?;
                let from = serialize_bitmap_datum(&literals[0], data_type);
                let to = serialize_bitmap_datum(&literals[1], data_type);
                let inside = self
                    .scan_dictionary(data_type, |candidate, cmp| {
                        !cmp(candidate, &from).is_lt() && !cmp(candidate, &to).is_gt()
                    })
                    .await?;
                result -= inside;
                Ok(result)
            }
            PredicateOperator::StartsWith => {
                if !is_character_string(data_type) {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "Bitmap global index starts_with only supports string columns",
                    ));
                }
                let prefix = serialize_bitmap_datum(&literals[0], data_type);
                if prefix.is_empty() {
                    return self.is_not_null().await;
                }
                self.scan_serialized_dictionary(|candidate| candidate.starts_with(&prefix))
                    .await
            }
            PredicateOperator::EndsWith => {
                if !is_character_string(data_type) {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "Bitmap global index ends_with only supports string columns",
                    ));
                }
                let suffix = serialize_bitmap_datum(&literals[0], data_type);
                if suffix.is_empty() {
                    return self.is_not_null().await;
                }
                self.scan_serialized_dictionary(|candidate| candidate.ends_with(&suffix))
                    .await
            }
            PredicateOperator::Contains => {
                if !is_character_string(data_type) {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "Bitmap global index contains only supports string columns",
                    ));
                }
                let needle = serialize_bitmap_datum(&literals[0], data_type);
                if needle.is_empty() {
                    return self.is_not_null().await;
                }
                self.scan_serialized_dictionary(|candidate| contains_bytes(candidate, &needle))
                    .await
            }
            PredicateOperator::Like => {
                if !is_character_string(data_type) {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "Bitmap global index like only supports string columns",
                    ));
                }
                let pattern = string_literal(literals, op)?.to_string();
                self.scan_serialized_dictionary(|candidate| {
                    std::str::from_utf8(candidate).is_ok_and(|value| like_match(value, &pattern))
                })
                .await
            }
        }
    }

    pub(crate) async fn range_query(
        &self,
        from: &[u8],
        to: &[u8],
        data_type: &DataType,
        from_inclusive: bool,
        to_inclusive: bool,
    ) -> io::Result<RoaringTreemap> {
        if is_floating_point(data_type) {
            return self.is_not_null().await;
        }
        self.scan_dictionary(data_type, |candidate, cmp| {
            let from_cmp = cmp(candidate, from);
            let to_cmp = cmp(candidate, to);
            (from_cmp.is_gt() || (from_inclusive && from_cmp.is_eq()))
                && (to_cmp.is_lt() || (to_inclusive && to_cmp.is_eq()))
        })
        .await
    }

    async fn is_null(&self) -> io::Result<RoaringTreemap> {
        self.read_bitmap(self.footer.null_rows_block).await
    }

    async fn is_not_null(&self) -> io::Result<RoaringTreemap> {
        self.read_bitmap(self.footer.non_null_rows_block).await
    }

    async fn equal(&self, key: &[u8], data_type: &DataType) -> io::Result<RoaringTreemap> {
        let logical_cmp = make_bitmap_key_comparator(data_type);
        self.equal_with_comparator(key, logical_cmp.as_ref()).await
    }

    async fn equal_with_comparator(
        &self,
        key: &[u8],
        logical_cmp: &(dyn Fn(&[u8], &[u8]) -> Ordering + Send + Sync),
    ) -> io::Result<RoaringTreemap> {
        match self.find_bitmap_block(key, logical_cmp).await? {
            Some(block) => self.read_bitmap(block).await,
            None => Ok(RoaringTreemap::new()),
        }
    }

    async fn in_keys(&self, keys: &[Vec<u8>], data_type: &DataType) -> io::Result<RoaringTreemap> {
        let mut sorted_keys = keys.to_vec();
        sorted_keys.sort();
        sorted_keys.dedup();

        let logical_cmp = make_bitmap_key_comparator(data_type);
        let mut result = RoaringTreemap::new();
        for key in sorted_keys {
            result |= self
                .equal_with_comparator(&key, logical_cmp.as_ref())
                .await?;
        }
        Ok(result)
    }

    async fn scan_dictionary(
        &self,
        data_type: &DataType,
        predicate: impl Fn(&[u8], &dyn Fn(&[u8], &[u8]) -> Ordering) -> bool,
    ) -> io::Result<RoaringTreemap> {
        let cmp = make_bitmap_key_comparator(data_type);
        self.scan_serialized_dictionary(|candidate| predicate(candidate, cmp.as_ref()))
            .await
    }

    async fn scan_serialized_dictionary(
        &self,
        predicate: impl Fn(&[u8]) -> bool,
    ) -> io::Result<RoaringTreemap> {
        let mut result = RoaringTreemap::new();
        for block_meta in &self.dictionary_blocks {
            for entry in self.read_dictionary_block(block_meta.block).await?.iter() {
                if predicate(&entry.key) {
                    result |= self.read_bitmap(entry.bitmap_block).await?;
                }
            }
        }
        Ok(result)
    }

    async fn find_bitmap_block(
        &self,
        key: &[u8],
        logical_cmp: &(dyn Fn(&[u8], &[u8]) -> Ordering + Send + Sync),
    ) -> io::Result<Option<BlockInfo>> {
        let Some(block_meta) = self.find_dictionary_block_meta(key, logical_cmp) else {
            return Ok(None);
        };
        for entry in self.read_dictionary_block(block_meta.block).await?.iter() {
            match logical_cmp(&entry.key, key) {
                Ordering::Equal => return Ok(Some(entry.bitmap_block)),
                Ordering::Greater => return Ok(None),
                Ordering::Less => {}
            }
        }
        Ok(None)
    }

    fn find_dictionary_block_meta(
        &self,
        key: &[u8],
        compare: impl Fn(&[u8], &[u8]) -> Ordering,
    ) -> Option<&DictionaryBlockMeta> {
        if self.dictionary_blocks.is_empty() {
            return None;
        }
        let mut low = 0usize;
        let mut high = self.dictionary_blocks.len();
        while low < high {
            let mid = (low + high) / 2;
            if compare(&self.dictionary_blocks[mid].first_key, key) != Ordering::Greater {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        low.checked_sub(1)
            .and_then(|index| self.dictionary_blocks.get(index))
    }

    async fn read_dictionary_block(
        &self,
        block: BlockInfo,
    ) -> io::Result<Arc<Vec<DictionaryEntry>>> {
        if let Some(entries) = self
            .dictionary_block_cache
            .lock()
            .map_err(|_| io::Error::other("Bitmap dictionary block cache lock was poisoned"))?
            .get(&block)
            .cloned()
        {
            return Ok(entries);
        }

        let bytes = read_compressible_block(self.reader.as_ref(), block).await?;
        let mut cursor = Cursor::new(bytes.as_slice());
        let entry_count = decode_var_int(&mut cursor)?;
        if entry_count < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid bitmap dictionary entry count: {entry_count}"),
            ));
        }
        let mut entries = Vec::with_capacity(entry_count as usize);
        for _ in 0..entry_count {
            let key = read_key(&mut cursor)?;
            let offset = decode_var_long(&mut cursor)?;
            let length = decode_var_int(&mut cursor)?;
            entries.push(DictionaryEntry {
                key,
                bitmap_block: block_info(offset, length)?,
            });
        }
        let entries = Arc::new(entries);
        let mut cache = self
            .dictionary_block_cache
            .lock()
            .map_err(|_| io::Error::other("Bitmap dictionary block cache lock was poisoned"))?;
        Ok(Arc::clone(cache.entry(block).or_insert(entries)))
    }

    async fn read_bitmap(&self, block: BlockInfo) -> io::Result<RoaringTreemap> {
        let bytes = self
            .reader
            .read(block.offset..block.offset + block.length as u64)
            .await
            .map_err(|e| io::Error::other(e.to_string()))?;
        RoaringTreemap::deserialize_from(bytes.as_ref())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

async fn read_footer(reader: &dyn FileRead, file_size: u64) -> io::Result<Footer> {
    if file_size < FOOTER_LENGTH as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid bitmap global index file size",
        ));
    }
    let bytes = reader
        .read(file_size - FOOTER_LENGTH as u64..file_size)
        .await
        .map_err(|e| io::Error::other(e.to_string()))?;

    let null_rows_block = block_info(read_i64_be(&bytes, 0)?, read_i32_be(&bytes, 8)?)?;
    let non_null_rows_block = block_info(read_i64_be(&bytes, 12)?, read_i32_be(&bytes, 20)?)?;
    let index_block = block_info(read_i64_be(&bytes, 24)?, read_i32_be(&bytes, 32)?)?;
    let value_count = read_i32_be(&bytes, 36)?;
    let version = read_i32_be(&bytes, 40)?;
    let magic = read_i32_be(&bytes, 44)?;

    if magic != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "File is not a bitmap global index file",
        ));
    }
    if version != VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Unsupported bitmap global index file version: {version}"),
        ));
    }
    if value_count < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid bitmap value count",
        ));
    }

    Ok(Footer {
        null_rows_block,
        non_null_rows_block,
        index_block,
    })
}

fn read_index_block(bytes: &[u8]) -> io::Result<Vec<DictionaryBlockMeta>> {
    let mut cursor = Cursor::new(bytes);
    let block_count = decode_var_int(&mut cursor)?;
    if block_count < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Invalid bitmap dictionary block count: {block_count}"),
        ));
    }
    let mut blocks = Vec::with_capacity(block_count as usize);
    for _ in 0..block_count {
        let first_key = read_key(&mut cursor)?;
        let offset = decode_var_long(&mut cursor)?;
        let length = decode_var_int(&mut cursor)?;
        blocks.push(DictionaryBlockMeta {
            first_key,
            block: block_info(offset, length)?,
        });
    }
    Ok(blocks)
}

async fn read_compressible_block(reader: &dyn FileRead, block: BlockInfo) -> io::Result<Vec<u8>> {
    if block.length > usize::MAX - BLOCK_TRAILER_LENGTH {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Bitmap block is too large",
        ));
    }
    let bytes = reader
        .read(block.offset..block.offset + (block.length + BLOCK_TRAILER_LENGTH) as u64)
        .await
        .map_err(|e| io::Error::other(e.to_string()))?;
    let block_bytes = &bytes[..block.length];
    let trailer = &bytes[block.length..block.length + BLOCK_TRAILER_LENGTH];
    let compression_type = BlockCompressionType::from_persistent_id(trailer[0])?;
    let expected_crc = u32::from_le_bytes([trailer[1], trailer[2], trailer[3], trailer[4]]);
    let actual_crc = compute_crc32(block_bytes, compression_type);
    if expected_crc != actual_crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Bitmap block CRC mismatch: expected 0x{expected_crc:08X}, got 0x{actual_crc:08X}"
            ),
        ));
    }

    decompress_block(block_bytes, compression_type)
}

fn read_key(input: &mut impl Read) -> io::Result<Vec<u8>> {
    let key_length = decode_var_int(input)?;
    if key_length < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Invalid bitmap key length: {key_length}"),
        ));
    }
    let mut key = vec![0; key_length as usize];
    input.read_exact(&mut key)?;
    Ok(key)
}

fn read_i64_be(bytes: &[u8], offset: usize) -> io::Result<i64> {
    let end = offset + 8;
    let value = bytes
        .get(offset..end)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Bitmap footer is truncated"))?;
    Ok(i64::from_be_bytes(value.try_into().unwrap()))
}

fn read_i32_be(bytes: &[u8], offset: usize) -> io::Result<i32> {
    let end = offset + 4;
    let value = bytes
        .get(offset..end)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Bitmap footer is truncated"))?;
    Ok(i32::from_be_bytes(value.try_into().unwrap()))
}

fn is_character_string(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Char(_) | DataType::VarChar(_))
}

fn is_floating_point(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Float(_) | DataType::Double(_))
}

fn string_literal(literals: &[Datum], op: PredicateOperator) -> io::Result<&str> {
    match literals.first() {
        Some(Datum::String(value)) => Ok(value),
        Some(other) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Bitmap global index {op} requires a string literal, got {other}"),
        )),
        None => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Bitmap global index {op} requires one literal"),
        )),
    }
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::compress_block;
    use crate::btree::test_util::{BytesFileRead, VecFileWrite};
    use crate::btree::var_len::encode_var_int;
    use crate::spec::{DoubleType, FloatType, IntType};
    use std::ops::Range;
    use std::sync::{Arc, Mutex};

    struct TrackingFileRead {
        bytes: Bytes,
        ranges: Arc<Mutex<Vec<Range<u64>>>>,
    }

    #[async_trait::async_trait]
    impl FileRead for TrackingFileRead {
        async fn read(&self, range: Range<u64>) -> crate::Result<Bytes> {
            self.ranges.lock().unwrap().push(range.clone());
            Ok(self.bytes.slice(range.start as usize..range.end as usize))
        }
    }

    async fn write_bitmap_bytes(
        data_type: &DataType,
        values: &[(Datum, i64)],
        dictionary_block_size: usize,
    ) -> Vec<u8> {
        let output = VecFileWrite::new();
        let captured = output.clone();
        let mut writer = BitmapGlobalIndexWriter::new(
            Box::new(output),
            dictionary_block_size,
            BlockCompressionType::None,
            make_bitmap_key_comparator(data_type),
        );
        for (value, row_id) in values {
            let key = serialize_bitmap_datum(value, data_type);
            writer.write(Some(&key), *row_id).unwrap();
        }
        writer.finish().await.unwrap();
        captured.to_vec()
    }

    async fn write_and_read_entries(
        data_type: &DataType,
        values: &[(Datum, i64)],
    ) -> (BitmapGlobalIndexReader, Vec<DictionaryEntry>) {
        let bytes = write_bitmap_bytes(data_type, values, 1 << 20).await;
        let reader = BitmapGlobalIndexReader::open(
            Box::new(BytesFileRead(Bytes::from(bytes.clone()))),
            bytes.len() as u64,
        )
        .await
        .unwrap();
        let mut entries = Vec::new();
        for block in &reader.dictionary_blocks {
            entries.extend(
                reader
                    .read_dictionary_block(block.block)
                    .await
                    .unwrap()
                    .iter()
                    .cloned(),
            );
        }
        (reader, entries)
    }

    fn java_codec_envelope(compressed: &[u8], original_len: usize) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(8 + compressed.len());
        encoded.extend_from_slice(&i32::try_from(compressed.len()).unwrap().to_le_bytes());
        encoded.extend_from_slice(&i32::try_from(original_len).unwrap().to_le_bytes());
        encoded.extend_from_slice(compressed);
        encoded
    }

    #[tokio::test]
    async fn test_decode_java_lz4_and_lzo_bitmap_blocks() {
        use base64::Engine;

        let original = b"java-compatible multivalue bitmap block".repeat(16);

        let lz4 = lz4_flex::block::compress(&original);
        let mut lz4_block = Vec::new();
        encode_var_int(&mut lz4_block, i32::try_from(original.len()).unwrap()).unwrap();
        lz4_block.extend_from_slice(&java_codec_envelope(&lz4, original.len()));
        assert_eq!(
            decompress_block(&lz4_block, BlockCompressionType::Lz4).unwrap(),
            original
        );

        // Produced by Airlift 2.0.3 `LzoCompressor`, the implementation used
        // by Java Paimon. Keep this as a cross-language golden rather than a
        // Rust self-roundtrip.
        let lzo = base64::engine::general_purpose::STANDARD
            .decode("OGphdmEtY29tcGF0aWJsZSBtdWx0aXZhbHVlIGJpdG1hcCBibG9jayAAACWYAAJibG9jaxEAAA==")
            .unwrap();
        let mut lzo_block = Vec::new();
        encode_var_int(&mut lzo_block, i32::try_from(original.len()).unwrap()).unwrap();
        lzo_block.extend_from_slice(&java_codec_envelope(&lzo, original.len()));
        assert_eq!(
            decompress_block(&lzo_block, BlockCompressionType::Lzo).unwrap(),
            original
        );

        let mut block = Vec::new();
        encode_var_int(&mut block, i32::try_from(original.len()).unwrap()).unwrap();
        block.extend_from_slice(&java_codec_envelope(&lzo, original.len()));
        let block_len = block.len();
        let crc = compute_crc32(&block, BlockCompressionType::Lzo);
        block.push(BlockCompressionType::Lzo as u8);
        block.extend_from_slice(&crc.to_le_bytes());
        assert_eq!(
            read_compressible_block(
                &BytesFileRead(Bytes::from(block)),
                BlockInfo {
                    offset: 0,
                    length: block_len,
                },
            )
            .await
            .unwrap(),
            original
        );
    }

    #[tokio::test]
    async fn test_encode_supported_java_bitmap_compressions() {
        let original = b"java-compatible multivalue bitmap block".repeat(64);
        for (compression_type, compression_level) in [
            (BlockCompressionType::Zstd, 7),
            (BlockCompressionType::Lz4, 1),
            (BlockCompressionType::Lzo, 1),
        ] {
            let (encoded, actual_type) =
                compress_block(&original, compression_type, compression_level).unwrap();
            assert_eq!(actual_type, compression_type);

            let block_length = encoded.len();
            let crc = compute_crc32(encoded.as_ref(), actual_type);
            let mut stored = encoded.into_owned();
            stored.push(actual_type as u8);
            stored.extend_from_slice(&crc.to_le_bytes());
            let decoded = read_compressible_block(
                &BytesFileRead(Bytes::from(stored)),
                BlockInfo {
                    offset: 0,
                    length: block_length,
                },
            )
            .await
            .unwrap();
            assert_eq!(decoded, original, "{compression_type:?}");
        }
    }

    #[tokio::test]
    async fn test_multivalue_queries_reuse_dictionary_block_reads() {
        let data_type = DataType::Int(IntType::new());
        let values = [(Datum::Int(1), 0), (Datum::Int(2), 1), (Datum::Int(3), 2)];
        let bytes = write_bitmap_bytes(&data_type, &values, 1 << 20).await;
        let ranges = Arc::new(Mutex::new(Vec::new()));
        let reader = BitmapGlobalIndexReader::open(
            Box::new(TrackingFileRead {
                bytes: Bytes::from(bytes.clone()),
                ranges: Arc::clone(&ranges),
            }),
            bytes.len() as u64,
        )
        .await
        .unwrap();
        assert_eq!(reader.dictionary_blocks.len(), 1);
        let dictionary = reader.dictionary_blocks[0].block;
        let dictionary_range = dictionary.offset
            ..dictionary.offset + (dictionary.length + BLOCK_TRAILER_LENGTH) as u64;

        ranges.lock().unwrap().clear();
        let overlap = reader
            .query(
                PredicateOperator::ArraysOverlap,
                &[Datum::Int(1), Datum::Int(2), Datum::Int(3)],
                &data_type,
            )
            .await
            .unwrap();
        assert_eq!(overlap.iter().collect::<Vec<_>>(), vec![0, 1, 2]);
        assert_eq!(
            ranges
                .lock()
                .unwrap()
                .iter()
                .filter(|range| **range == dictionary_range)
                .count(),
            1,
            "all overlap literals in the same block should share one dictionary read"
        );

        ranges.lock().unwrap().clear();
        let contains_all = reader
            .query(
                PredicateOperator::ArrayContainsAll,
                &[Datum::Int(1), Datum::Int(2), Datum::Int(3)],
                &data_type,
            )
            .await
            .unwrap();
        assert!(contains_all.is_empty());
        assert_eq!(
            ranges
                .lock()
                .unwrap()
                .iter()
                .filter(|range| **range == dictionary_range)
                .count(),
            0,
            "the reader should retain parsed dictionary blocks across predicates"
        );
    }

    #[tokio::test]
    async fn test_multivalue_writer_keeps_empty_source_coverage() {
        let data_type = DataType::Int(IntType::new());
        let output = VecFileWrite::new();
        let captured = output.clone();
        let writer = BitmapGlobalIndexWriter::new(
            Box::new(output),
            1 << 20,
            BlockCompressionType::None,
            make_bitmap_key_comparator(&data_type),
        );

        let result = writer.finish_with_source_row_count(4).await.unwrap();
        assert_eq!(result.row_count, 4);
        assert_eq!(result.meta.first_key, None);
        assert_eq!(result.meta.last_key, None);
        assert!(!result.meta.has_nulls);

        let bytes = captured.to_vec();
        let reader = BitmapGlobalIndexReader::open(
            Box::new(BytesFileRead(Bytes::from(bytes.clone()))),
            bytes.len() as u64,
        )
        .await
        .unwrap();
        let rows = reader
            .query(
                PredicateOperator::ArrayContains,
                &[Datum::Int(10)],
                &data_type,
            )
            .await
            .unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn test_bitmap_floating_residual_sensitive_operator_set() {
        for op in [
            PredicateOperator::NotEq,
            PredicateOperator::NotIn,
            PredicateOperator::Lt,
            PredicateOperator::LtEq,
            PredicateOperator::Gt,
            PredicateOperator::GtEq,
            PredicateOperator::Between,
            PredicateOperator::NotBetween,
        ] {
            assert!(is_bitmap_floating_residual_sensitive_op(op), "{op}");
        }
        for op in [
            PredicateOperator::Eq,
            PredicateOperator::In,
            PredicateOperator::IsNull,
            PredicateOperator::IsNotNull,
            PredicateOperator::StartsWith,
            PredicateOperator::EndsWith,
            PredicateOperator::Contains,
            PredicateOperator::Like,
        ] {
            assert!(!is_bitmap_floating_residual_sensitive_op(op), "{op}");
        }
    }

    #[tokio::test]
    async fn test_writer_orders_numeric_keys_logically() {
        let data_type = DataType::Int(IntType::new());
        let output = VecFileWrite::new();
        let captured = output.clone();
        let mut writer = BitmapGlobalIndexWriter::new(
            Box::new(output),
            1 << 20,
            BlockCompressionType::None,
            make_bitmap_key_comparator(&data_type),
        );

        writer.write(None, 5).unwrap();
        for (value, row_id) in [(-1i32, 0), (0, 1), (0, 2), (1, 3), (256, 4)] {
            writer.write(Some(&value.to_le_bytes()), row_id).unwrap();
        }
        writer.finish().await.unwrap();

        let bytes = captured.to_vec();
        let reader = BitmapGlobalIndexReader::open(
            Box::new(BytesFileRead(Bytes::from(bytes.clone()))),
            bytes.len() as u64,
        )
        .await
        .unwrap();
        assert_eq!(reader.dictionary_blocks.len(), 1);
        let keys = reader
            .read_dictionary_block(reader.dictionary_blocks[0].block)
            .await
            .unwrap()
            .iter()
            .map(|entry| i32::from_le_bytes(entry.key.as_slice().try_into().unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(keys, vec![-1, 0, 1, 256]);
    }

    #[tokio::test]
    async fn test_writer_orders_and_canonicalizes_float_keys_like_java() {
        let data_type = DataType::Float(FloatType::new());
        let values = [
            (Datum::Float(f32::from_bits(0xffc0_0001)), 0),
            (Datum::Float(-1.0), 1),
            (Datum::Float(-0.0), 2),
            (Datum::Float(0.0), 3),
            (Datum::Float(1.0), 4),
            (Datum::Float(f32::from_bits(0x7fc0_0010)), 5),
            (Datum::Float(f32::from_bits(0x7fff_1234)), 6),
        ];
        let (reader, entries) = write_and_read_entries(&data_type, &values).await;

        let key_bits = entries
            .iter()
            .map(|entry| u32::from_le_bytes(entry.key.as_slice().try_into().unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(
            key_bits,
            vec![
                (-1.0f32).to_bits(),
                (-0.0f32).to_bits(),
                0.0f32.to_bits(),
                1.0f32.to_bits(),
                0x7fc0_0000,
            ]
        );
        let nan_rows = reader
            .read_bitmap(entries.last().unwrap().bitmap_block)
            .await
            .unwrap()
            .iter()
            .collect::<Vec<_>>();
        assert_eq!(nan_rows, vec![0, 5, 6]);
    }

    #[tokio::test]
    async fn test_writer_orders_and_canonicalizes_double_keys_like_java() {
        let data_type = DataType::Double(DoubleType::new());
        let values = [
            (Datum::Double(f64::from_bits(0xfff8_0000_0000_0001)), 0),
            (Datum::Double(-1.0), 1),
            (Datum::Double(-0.0), 2),
            (Datum::Double(0.0), 3),
            (Datum::Double(1.0), 4),
            (Datum::Double(f64::from_bits(0x7ff8_0000_0000_0010)), 5),
            (Datum::Double(f64::from_bits(0x7fff_1234_5678_9abc)), 6),
        ];
        let (reader, entries) = write_and_read_entries(&data_type, &values).await;

        let key_bits = entries
            .iter()
            .map(|entry| u64::from_le_bytes(entry.key.as_slice().try_into().unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(
            key_bits,
            vec![
                (-1.0f64).to_bits(),
                (-0.0f64).to_bits(),
                0.0f64.to_bits(),
                1.0f64.to_bits(),
                0x7ff8_0000_0000_0000,
            ]
        );
        let nan_rows = reader
            .read_bitmap(entries.last().unwrap().bitmap_block)
            .await
            .unwrap()
            .iter()
            .collect::<Vec<_>>();
        assert_eq!(nan_rows, vec![0, 5, 6]);
    }

    async fn assert_eq_and_in_read_only_logical_candidate_block(
        data_type: DataType,
        negative_nan: Datum,
        positive_nan: Datum,
        canonical_nan: Datum,
        negative_one: Datum,
        zero: Datum,
        one: Datum,
    ) {
        let values = [
            (negative_nan.clone(), 0),
            (positive_nan.clone(), 1),
            (canonical_nan.clone(), 2),
            (negative_one.clone(), 3),
            (zero, 4),
            (one, 5),
        ];
        let bytes = write_bitmap_bytes(&data_type, &values, 1).await;
        let ranges = Arc::new(Mutex::new(Vec::new()));
        let reader = BitmapGlobalIndexReader::open(
            Box::new(TrackingFileRead {
                bytes: Bytes::from(bytes.clone()),
                ranges: Arc::clone(&ranges),
            }),
            bytes.len() as u64,
        )
        .await
        .unwrap();
        assert!(reader.dictionary_blocks.len() > 1);

        let cases = [
            (PredicateOperator::Eq, vec![negative_nan], vec![0, 1, 2]),
            (
                PredicateOperator::In,
                vec![positive_nan, canonical_nan],
                vec![0, 1, 2],
            ),
            (PredicateOperator::Eq, vec![negative_one], vec![3]),
        ];
        for (op, literals, expected) in cases {
            let key = serialize_bitmap_datum(&literals[0], &data_type);
            let cmp = make_bitmap_key_comparator(&data_type);
            let expected_block = reader
                .find_dictionary_block_meta(&key, cmp.as_ref())
                .unwrap()
                .block;
            let was_cached = reader
                .dictionary_block_cache
                .lock()
                .unwrap()
                .contains_key(&expected_block);
            ranges.lock().unwrap().clear();

            let actual = reader
                .query(op, &literals, &data_type)
                .await
                .unwrap()
                .iter()
                .collect::<Vec<_>>();
            assert_eq!(actual, expected, "{data_type:?}: {op}");

            let actual_ranges = ranges.lock().unwrap().clone();
            assert_eq!(
                actual_ranges.len(),
                if was_cached { 1 } else { 2 },
                "{data_type:?}: {op}"
            );
            if !was_cached {
                assert_eq!(
                    actual_ranges[0],
                    expected_block.offset
                        ..expected_block.offset
                            + (expected_block.length + BLOCK_TRAILER_LENGTH) as u64,
                    "{data_type:?}: {op}"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_eq_and_in_read_only_logical_candidate_block() {
        assert_eq_and_in_read_only_logical_candidate_block(
            DataType::Float(FloatType::new()),
            Datum::Float(f32::from_bits(0xffc0_0001)),
            Datum::Float(f32::from_bits(0x7fc0_0010)),
            Datum::Float(f32::NAN),
            Datum::Float(-1.0),
            Datum::Float(0.0),
            Datum::Float(1.0),
        )
        .await;
        assert_eq_and_in_read_only_logical_candidate_block(
            DataType::Double(DoubleType::new()),
            Datum::Double(f64::from_bits(0xfff8_0000_0000_0001)),
            Datum::Double(f64::from_bits(0x7ff8_0000_0000_0010)),
            Datum::Double(f64::NAN),
            Datum::Double(-1.0),
            Datum::Double(0.0),
            Datum::Double(1.0),
        )
        .await;
    }

    async fn assert_floating_residual_sensitive_ops_return_all_non_null_candidates(
        data_type: DataType,
        negative_nan: Datum,
        zero: Datum,
        one: Datum,
    ) {
        let output = VecFileWrite::new();
        let captured = output.clone();
        let mut writer = BitmapGlobalIndexWriter::new(
            Box::new(output),
            1,
            BlockCompressionType::None,
            make_bitmap_key_comparator(&data_type),
        );
        for (row_id, value) in [&negative_nan, &zero, &one].into_iter().enumerate() {
            let key = serialize_bitmap_datum(value, &data_type);
            writer.write(Some(&key), row_id as i64).unwrap();
        }
        writer.write(None, 3).unwrap();
        writer.finish().await.unwrap();

        let bytes = captured.to_vec();
        let reader = BitmapGlobalIndexReader::open(
            Box::new(BytesFileRead(Bytes::from(bytes.clone()))),
            bytes.len() as u64,
        )
        .await
        .unwrap();
        let expected = vec![0, 1, 2];
        let cases = [
            (PredicateOperator::NotEq, vec![negative_nan.clone()]),
            (
                PredicateOperator::NotIn,
                vec![negative_nan.clone(), zero.clone()],
            ),
            (PredicateOperator::Lt, vec![zero.clone()]),
            (PredicateOperator::LtEq, vec![zero.clone()]),
            (PredicateOperator::Gt, vec![negative_nan.clone()]),
            (PredicateOperator::GtEq, vec![negative_nan.clone()]),
            (
                PredicateOperator::Between,
                vec![negative_nan.clone(), zero.clone()],
            ),
            (
                PredicateOperator::NotBetween,
                vec![negative_nan.clone(), zero.clone()],
            ),
        ];
        for (op, literals) in cases {
            let actual = reader
                .query(op, &literals, &data_type)
                .await
                .unwrap()
                .iter()
                .collect::<Vec<_>>();
            assert_eq!(actual, expected, "{data_type:?}: {op}");
        }

        let direct_cases = [
            (PredicateOperator::Eq, vec![negative_nan.clone()], vec![0]),
            (
                PredicateOperator::In,
                vec![negative_nan.clone(), zero.clone()],
                vec![0, 1],
            ),
            (PredicateOperator::Eq, vec![zero.clone()], vec![1]),
            (
                PredicateOperator::In,
                vec![zero.clone(), one.clone()],
                vec![1, 2],
            ),
        ];
        for (op, literals, expected) in direct_cases {
            let actual = reader
                .query(op, &literals, &data_type)
                .await
                .unwrap()
                .iter()
                .collect::<Vec<_>>();
            assert_eq!(actual, expected, "{data_type:?}: direct {op}");
        }

        let from = serialize_bitmap_datum(&negative_nan, &data_type);
        let to = serialize_bitmap_datum(&zero, &data_type);
        let actual = reader
            .range_query(&from, &to, &data_type, true, true)
            .await
            .unwrap()
            .iter()
            .collect::<Vec<_>>();
        assert_eq!(actual, expected, "{data_type:?}: combined range");
    }

    #[tokio::test]
    async fn test_floating_residual_sensitive_ops_return_all_non_null_candidates() {
        assert_floating_residual_sensitive_ops_return_all_non_null_candidates(
            DataType::Float(FloatType::new()),
            Datum::Float(f32::from_bits(0xffc0_0001)),
            Datum::Float(0.0),
            Datum::Float(1.0),
        )
        .await;
        assert_floating_residual_sensitive_ops_return_all_non_null_candidates(
            DataType::Double(DoubleType::new()),
            Datum::Double(f64::from_bits(0xfff8_0000_0000_0001)),
            Datum::Double(0.0),
            Datum::Double(1.0),
        )
        .await;
    }
}
