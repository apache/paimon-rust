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

use std::collections::HashMap;

use bytes::{BufMut, Bytes, BytesMut};

use crate::{
    io::{FileIO, FileRead, FileStatus, InputFile, OutputFile},
    Error,
};

const FIXED_HEADER_LENGTH: usize = 16;
const MIN_HEADER_LENGTH: usize = 24;

/// Marks the beginning of a file index file.
pub const MAGIC: u64 = 1493475289347502;

/// Marks an empty index payload.
pub const EMPTY_INDEX_FLAG: i32 = -1;

#[derive(Debug)]
struct IndexInfo {
    start_pos: i32,
    length: i32,
}

#[repr(i32)]
#[derive(Debug, PartialEq, Eq)]
enum Version {
    V1 = 1,
}

fn format_invalid(message: impl Into<String>) -> Error {
    Error::FileIndexFormatInvalid {
        message: message.into(),
    }
}

fn modified_utf8_len(value: &str) -> crate::Result<usize> {
    let mut len = 0usize;
    for unit in value.encode_utf16() {
        let encoded_len = if (0x0001..=0x007f).contains(&unit) {
            1
        } else if unit > 0x07ff {
            3
        } else {
            2
        };
        len = len
            .checked_add(encoded_len)
            .ok_or_else(|| format_invalid("modified UTF-8 length overflow"))?;
        if len > u16::MAX as usize {
            return Err(format_invalid(format!(
                "modified UTF-8 string is {len} bytes, exceeding the 65535-byte limit"
            )));
        }
    }
    Ok(len)
}

fn write_java_utf(buffer: &mut BytesMut, value: &str) -> crate::Result<()> {
    let encoded_len = modified_utf8_len(value)?;
    buffer.put_u16(encoded_len as u16);
    for unit in value.encode_utf16() {
        if (0x0001..=0x007f).contains(&unit) {
            buffer.put_u8(unit as u8);
        } else if unit > 0x07ff {
            buffer.put_u8(0xe0 | (unit >> 12) as u8);
            buffer.put_u8(0x80 | ((unit >> 6) & 0x3f) as u8);
            buffer.put_u8(0x80 | (unit & 0x3f) as u8);
        } else {
            buffer.put_u8(0xc0 | (unit >> 6) as u8);
            buffer.put_u8(0x80 | (unit & 0x3f) as u8);
        }
    }
    Ok(())
}

fn take_bytes<'a>(buffer: &mut &'a [u8], len: usize, field: &str) -> crate::Result<&'a [u8]> {
    if buffer.len() < len {
        return Err(format_invalid(format!(
            "truncated {field}: need {len} bytes, but only {} remain",
            buffer.len()
        )));
    }
    let (value, remaining) = buffer.split_at(len);
    *buffer = remaining;
    Ok(value)
}

fn read_u16(buffer: &mut &[u8], field: &str) -> crate::Result<u16> {
    Ok(u16::from_be_bytes(
        take_bytes(buffer, 2, field)?.try_into().unwrap(),
    ))
}

fn read_i32(buffer: &mut &[u8], field: &str) -> crate::Result<i32> {
    Ok(i32::from_be_bytes(
        take_bytes(buffer, 4, field)?.try_into().unwrap(),
    ))
}

fn read_u64(buffer: &mut &[u8], field: &str) -> crate::Result<u64> {
    Ok(u64::from_be_bytes(
        take_bytes(buffer, 8, field)?.try_into().unwrap(),
    ))
}

fn read_count(buffer: &mut &[u8], field: &str) -> crate::Result<usize> {
    let count = read_i32(buffer, field)?;
    usize::try_from(count).map_err(|_| format_invalid(format!("negative {field}: {count}")))
}

fn read_java_utf(buffer: &mut &[u8], field: &str) -> crate::Result<String> {
    let len = read_u16(buffer, &format!("{field} length"))? as usize;
    let bytes = take_bytes(buffer, len, field)?;
    let mut units = Vec::with_capacity(bytes.len());
    let mut offset = 0;

    while offset < bytes.len() {
        let first = bytes[offset];
        let (unit, width) = if first & 0x80 == 0 {
            (first as u16, 1)
        } else if first & 0xe0 == 0xc0 {
            if offset + 1 >= bytes.len() || bytes[offset + 1] & 0xc0 != 0x80 {
                return Err(format_invalid(format!("invalid modified UTF-8 in {field}")));
            }
            (
                (((first & 0x1f) as u16) << 6) | (bytes[offset + 1] & 0x3f) as u16,
                2,
            )
        } else if first & 0xf0 == 0xe0 {
            if offset + 2 >= bytes.len()
                || bytes[offset + 1] & 0xc0 != 0x80
                || bytes[offset + 2] & 0xc0 != 0x80
            {
                return Err(format_invalid(format!("invalid modified UTF-8 in {field}")));
            }
            (
                (((first & 0x0f) as u16) << 12)
                    | (((bytes[offset + 1] & 0x3f) as u16) << 6)
                    | (bytes[offset + 2] & 0x3f) as u16,
                3,
            )
        } else {
            return Err(format_invalid(format!("invalid modified UTF-8 in {field}")));
        };
        units.push(unit);
        offset += width;
    }

    String::from_utf16(&units)
        .map_err(|_| format_invalid(format!("invalid UTF-16 sequence in {field}")))
}

fn usize_to_i32(value: usize, field: &str) -> crate::Result<i32> {
    i32::try_from(value).map_err(|_| format_invalid(format!("{field} exceeds i32::MAX: {value}")))
}

/// File index file format. All columns and offsets are stored in the header.
///
/// ```text
///   _____________________________________    _____________________
/// ｜     magic    ｜version｜head length ｜
/// ｜-------------------------------------｜
/// ｜            column number            ｜
/// ｜-------------------------------------｜
/// ｜   column 1        ｜ index number   ｜
/// ｜-------------------------------------｜
/// ｜  index name 1 ｜start pos ｜length  ｜
/// ｜-------------------------------------｜
/// ｜  index name 2 ｜start pos ｜length  ｜
/// ｜-------------------------------------｜
/// ｜  index name 3 ｜start pos ｜length  ｜
/// ｜-------------------------------------｜            HEADER
/// ｜   column 2        ｜ index number   ｜
/// ｜-------------------------------------｜
/// ｜  index name 1 ｜start pos ｜length  ｜
/// ｜-------------------------------------｜
/// ｜  index name 2 ｜start pos ｜length  ｜
/// ｜-------------------------------------｜
/// ｜  index name 3 ｜start pos ｜length  ｜
/// ｜-------------------------------------｜
/// ｜                 ...                 ｜
/// ｜-------------------------------------｜
/// ｜                 ...                 ｜
/// ｜-------------------------------------｜
/// ｜  redundant length ｜redundant bytes ｜
/// ｜-------------------------------------｜    ---------------------
/// ｜                BODY                 ｜
/// ｜                BODY                 ｜
/// ｜                BODY                 ｜             BODY
/// ｜                BODY                 ｜
/// ｜_____________________________________｜    _____________________
///
/// - `magic`: 8 bytes long
/// - `version`: 4-byte integer
/// - `head length`: 4-byte integer
/// - `column number`: 4-byte integer
/// - `column x`: Java modified UTF-8 string (2-byte length + bytes)
/// - `index number`: 4-byte integer (number of index items below)
/// - `index name x`: Java modified UTF-8 string
/// - `start pos`: 4-byte integer
/// - `length`: 4-byte integer
/// - `redundant length`: 4-byte integer (for compatibility with future versions; content is zero in this version)
/// - `redundant bytes`: variable-length bytes (for compatibility with future versions; empty in this version)
/// - `BODY`: sequence of index data (concatenated index data for each column)
/// ```
///
/// `None` represents an empty index and is encoded with [`EMPTY_INDEX_FLAG`]. `Some(Bytes::new())`
/// represents a present index with a zero-length payload.
///
/// Implementation reference: <https://github.com/apache/paimon/blob/release-2.0/paimon-common/src/main/java/org/apache/paimon/fileindex/FileIndexFormat.java>
pub async fn write_column_indexes(
    path: &str,
    indexes: HashMap<String, HashMap<String, Option<Bytes>>>,
) -> crate::Result<OutputFile> {
    let file_io = FileIO::from_path(path)?.build()?;
    let output = file_io.new_output(path)?;
    let mut body_info: HashMap<String, HashMap<String, IndexInfo>> = HashMap::new();
    let mut total_data_size = 0usize;

    for bytes_map in indexes.values() {
        for data in bytes_map.values().flatten() {
            if !data.is_empty() {
                total_data_size = total_data_size
                    .checked_add(data.len())
                    .ok_or_else(|| format_invalid("file index body length overflow"))?;
            }
        }
    }
    usize_to_i32(total_data_size, "file index body length")?;

    let mut body = BytesMut::with_capacity(total_data_size);

    for (column_name, bytes_map) in indexes {
        let inner_map = body_info.entry(column_name).or_default();
        for (index_name, data) in bytes_map {
            if let Some(data) = data {
                let start_pos = usize_to_i32(body.len(), "file index body offset")?;
                let length = usize_to_i32(data.len(), "file index entry length")?;
                body.extend_from_slice(&data);
                inner_map.insert(index_name, IndexInfo { start_pos, length });
            } else {
                inner_map.insert(
                    index_name,
                    IndexInfo {
                        start_pos: EMPTY_INDEX_FLAG,
                        length: 0,
                    },
                );
            }
        }
    }

    let head_length = calculate_head_length(&body_info)?;
    let head_length_i32 = usize_to_i32(head_length, "file index header length")?;
    let mut head_buffer = BytesMut::with_capacity(head_length);

    head_buffer.put_u64(MAGIC);
    head_buffer.put_i32(Version::V1 as i32);
    head_buffer.put_i32(head_length_i32);
    head_buffer.put_i32(usize_to_i32(body_info.len(), "column count")?);

    for (column_name, index_info) in body_info {
        write_java_utf(&mut head_buffer, &column_name)?;
        head_buffer.put_i32(usize_to_i32(index_info.len(), "index count")?);
        for (index_name, IndexInfo { start_pos, length }) in index_info {
            write_java_utf(&mut head_buffer, &index_name)?;
            let adjusted_start = if start_pos == EMPTY_INDEX_FLAG {
                EMPTY_INDEX_FLAG
            } else {
                start_pos.checked_add(head_length_i32).ok_or_else(|| {
                    format_invalid(format!(
                        "file index offset exceeds i32::MAX: {start_pos} + {head_length_i32}"
                    ))
                })?
            };
            head_buffer.put_i32(adjusted_start);
            head_buffer.put_i32(length);
        }
    }

    head_buffer.put_i32(0);
    debug_assert_eq!(head_buffer.len(), head_length);

    let mut writer = output.writer().await?;
    writer.write(head_buffer.freeze()).await?;
    writer.write(body.freeze()).await?;
    writer.close().await?;
    Ok(output)
}

fn calculate_head_length(
    body_info: &HashMap<String, HashMap<String, IndexInfo>>,
) -> crate::Result<usize> {
    let mut total_length = MIN_HEADER_LENGTH;

    for (column_name, index_info) in body_info {
        total_length = total_length
            .checked_add(2 + modified_utf8_len(column_name)?)
            .and_then(|length| length.checked_add(4))
            .ok_or_else(|| format_invalid("file index header length overflow"))?;

        for index_name in index_info.keys() {
            total_length = total_length
                .checked_add(2 + modified_utf8_len(index_name)?)
                .and_then(|length| length.checked_add(8))
                .ok_or_else(|| format_invalid("file index header length overflow"))?;
        }
    }

    usize_to_i32(total_length, "file index header length")?;
    Ok(total_length)
}

pub struct FileIndex {
    reader: Box<dyn FileRead>,
    header: HashMap<String, HashMap<String, IndexInfo>>,
}

impl FileIndex {
    pub async fn get_column_index(
        &self,
        column_name: &str,
    ) -> crate::Result<HashMap<String, Option<Bytes>>> {
        if let Some(index_info) = self.header.get(column_name) {
            let mut result = HashMap::new();
            for (index_name, info) in index_info {
                let bytes = self.get_bytes_with_start_and_length(info).await?;
                result.insert(index_name.clone(), bytes);
            }
            Ok(result)
        } else {
            Err(Error::FileIndexFormatInvalid {
                message: format!("Column '{column_name}' not found in header"),
            })
        }
    }

    pub async fn get_index(
        &self,
    ) -> crate::Result<HashMap<String, HashMap<String, Option<Bytes>>>> {
        let mut result = HashMap::new();
        for (column_name, index_info) in self.header.iter() {
            let mut column_index = HashMap::new();
            for (index_name, info) in index_info {
                let bytes = self.get_bytes_with_start_and_length(info).await?;
                column_index.insert(index_name.clone(), bytes);
            }
            result.insert(column_name.clone(), column_index);
        }
        Ok(result)
    }

    async fn get_bytes_with_start_and_length(
        &self,
        index_info: &IndexInfo,
    ) -> crate::Result<Option<Bytes>> {
        if index_info.start_pos == EMPTY_INDEX_FLAG {
            return Ok(None);
        }
        let start = index_info.start_pos as u64;
        let end = start
            .checked_add(index_info.length as u64)
            .ok_or_else(|| format_invalid("file index range overflow"))?;
        self.reader.read(start..end).await.map(Some)
    }

    /// Read bytes from the index file at the specified position and length
    pub async fn read_bytes(&self, start: i64, length: i64) -> crate::Result<Bytes> {
        let start = u64::try_from(start)
            .map_err(|_| format_invalid(format!("negative read offset: {start}")))?;
        let length = u64::try_from(length)
            .map_err(|_| format_invalid(format!("negative read length: {length}")))?;
        let end = start
            .checked_add(length)
            .ok_or_else(|| format_invalid("file index read range overflow"))?;
        self.reader.read(start..end).await
    }
}

pub struct FileIndexFormatReader {
    reader: Box<dyn FileRead>,
    stat: FileStatus,
}

impl FileIndexFormatReader {
    pub async fn get_file_index(input_file: InputFile) -> crate::Result<FileIndex> {
        let reader = input_file.reader().await?;
        let mut file_reader = Self {
            reader: Box::new(reader),
            stat: input_file.metadata().await?,
        };
        let header = file_reader.read_header().await?;
        Ok(FileIndex {
            header,
            reader: file_reader.reader,
        })
    }

    async fn read_header(&mut self) -> crate::Result<HashMap<String, HashMap<String, IndexInfo>>> {
        if self.stat.size < FIXED_HEADER_LENGTH as u64 {
            return Err(format_invalid(format!(
                "truncated fixed header: need {FIXED_HEADER_LENGTH} bytes, but file has {}",
                self.stat.size
            )));
        }

        let fixed_header = self
            .read_exact_range(0, FIXED_HEADER_LENGTH as u64, "fixed header")
            .await?;
        let mut fixed = fixed_header.as_ref();

        let magic = read_u64(&mut fixed, "magic")?;
        if magic != MAGIC {
            return Err(format_invalid(format!(
                "expected magic {MAGIC}, but found {magic}"
            )));
        }

        let version = read_i32(&mut fixed, "version")?;
        if version != Version::V1 as i32 {
            return Err(format_invalid(format!(
                "unsupported file index version: expected {}, but found {version}",
                Version::V1 as i32
            )));
        }

        let head_length = read_i32(&mut fixed, "header length")?;
        let head_length = usize::try_from(head_length)
            .map_err(|_| format_invalid(format!("negative header length: {head_length}")))?;
        if head_length < MIN_HEADER_LENGTH {
            return Err(format_invalid(format!(
                "header length {head_length} is smaller than the minimum {MIN_HEADER_LENGTH}"
            )));
        }
        if head_length as u64 > self.stat.size {
            return Err(format_invalid(format!(
                "header length {head_length} exceeds file size {}",
                self.stat.size
            )));
        }

        let encoded_header = self
            .read_exact_range(
                FIXED_HEADER_LENGTH as u64,
                head_length as u64,
                "variable header",
            )
            .await?;
        let mut buffer = encoded_header.as_ref();
        let column_number = read_count(&mut buffer, "column count")?;
        let mut header = HashMap::new();

        for _ in 0..column_number {
            let column_name = read_java_utf(&mut buffer, "column name")?;
            let index_number = read_count(&mut buffer, "index count")?;
            let mut index_info_map = HashMap::new();

            for _ in 0..index_number {
                let index_name = read_java_utf(&mut buffer, "index name")?;
                let start_pos = read_i32(&mut buffer, "index start position")?;
                let length = read_i32(&mut buffer, "index length")?;
                Self::validate_index_range(start_pos, length, head_length as u64, self.stat.size)?;
                index_info_map.insert(index_name, IndexInfo { start_pos, length });
            }

            header.insert(column_name, index_info_map);
        }

        let redundant_length = read_count(&mut buffer, "redundant length")?;
        take_bytes(&mut buffer, redundant_length, "redundant bytes")?;
        if !buffer.is_empty() {
            return Err(format_invalid(format!(
                "{} trailing bytes remain in the file index header",
                buffer.len()
            )));
        }

        Ok(header)
    }

    async fn read_exact_range(&self, start: u64, end: u64, field: &str) -> crate::Result<Bytes> {
        let length = end
            .checked_sub(start)
            .ok_or_else(|| format_invalid(format!("invalid {field} range: {start}..{end}")))?;
        let expected = usize::try_from(length)
            .map_err(|_| format_invalid(format!("{field} range is too large")))?;
        let bytes = self.reader.read(start..end).await?;
        if bytes.len() != expected {
            return Err(format_invalid(format!(
                "truncated {field}: need {expected} bytes, but read {}",
                bytes.len()
            )));
        }
        Ok(bytes)
    }

    fn validate_index_range(
        start_pos: i32,
        length: i32,
        head_length: u64,
        file_size: u64,
    ) -> crate::Result<()> {
        if start_pos == EMPTY_INDEX_FLAG {
            if length == 0 {
                return Ok(());
            }
            return Err(format_invalid(format!(
                "empty index has non-zero length {length}"
            )));
        }
        if start_pos < 0 {
            return Err(format_invalid(format!(
                "negative index start position: {start_pos}"
            )));
        }
        if length < 0 {
            return Err(format_invalid(format!("negative index length: {length}")));
        }

        let start = start_pos as u64;
        if start < head_length {
            return Err(format_invalid(format!(
                "index start position {start} overlaps header ending at {head_length}"
            )));
        }
        let end = start
            .checked_add(length as u64)
            .ok_or_else(|| format_invalid("file index range overflow"))?;
        if end > file_size {
            return Err(format_invalid(format!(
                "index range {start}..{end} exceeds file size {file_size}"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod file_index_format_tests {

    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap;

    const JAVA_V1_SIMPLE: &str = concat!(
        "00054e4ed01a35ae000000010000002a0000000100016100000001000162",
        "0000002a0000000300000000010203"
    );
    const JAVA_V1_SEMANTIC: &str = concat!(
        "00054e4ed01a35ae000000010000005200000001000963c080eda0bdedba80",
        "000000030005656d707479ffffffff0000000000047a65726f000000520000",
        "000000046461746100000052000000010000000041"
    );
    const JAVA_V1_UNICODE: &str = concat!(
        "00054e4ed01a35ae000000010000003500000001000963c080eda0bdedba80",
        "0000000100046461746100000035000000010000000041"
    );

    async fn write_fixture(path: &str, bytes: Vec<u8>) -> crate::Result<InputFile> {
        let file_io = FileIO::from_path(path)?.build()?;
        let output = file_io.new_output(path)?;
        output.write(Bytes::from(bytes)).await?;
        Ok(output.to_input_file())
    }

    #[tokio::test]
    async fn test_writer_matches_java_v1_bytes() -> crate::Result<()> {
        let indexes = HashMap::from([(
            "a".to_string(),
            HashMap::from([("b".to_string(), Some(Bytes::from_static(&[1, 2, 3])))]),
        )]);

        let output = write_column_indexes("memory:/tmp/java_v1_writer", indexes).await?;
        let actual = output.to_input_file().read().await?;

        assert_eq!(actual.as_ref(), hex::decode(JAVA_V1_SIMPLE).unwrap());
        Ok(())
    }

    #[tokio::test]
    async fn test_writer_matches_java_modified_utf8_bytes() -> crate::Result<()> {
        let indexes = HashMap::from([(
            "c\0🚀".to_string(),
            HashMap::from([("data".to_string(), Some(Bytes::from_static(&[65])))]),
        )]);

        let output = write_column_indexes("memory:/tmp/java_v1_unicode_writer", indexes).await?;
        let actual = output.to_input_file().read().await?;

        assert_eq!(actual.as_ref(), hex::decode(JAVA_V1_UNICODE).unwrap());
        Ok(())
    }

    #[tokio::test]
    async fn test_reader_reads_java_v1_bytes() -> crate::Result<()> {
        let input = write_fixture(
            "memory:/tmp/java_v1_reader",
            hex::decode(JAVA_V1_SIMPLE).unwrap(),
        )
        .await?;

        let reader = FileIndexFormatReader::get_file_index(input).await?;
        let indexes = reader.get_column_index("a").await?;

        assert_eq!(
            indexes.get("b"),
            Some(&Some(Bytes::from_static(&[1, 2, 3])))
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_reader_decodes_java_modified_utf8_and_empty_ranges() -> crate::Result<()> {
        let input = write_fixture(
            "memory:/tmp/java_v1_semantic",
            hex::decode(JAVA_V1_SEMANTIC).unwrap(),
        )
        .await?;

        let reader = FileIndexFormatReader::get_file_index(input).await?;
        let indexes = reader.get_column_index("c\0🚀").await?;

        assert_eq!(indexes.get("empty"), Some(&None));
        assert_eq!(indexes.get("zero"), Some(&Some(Bytes::new())));
        assert_eq!(indexes.get("data"), Some(&Some(Bytes::from_static(&[65]))));
        Ok(())
    }

    #[tokio::test]
    async fn test_reader_rejects_every_truncated_prefix_without_panicking() -> crate::Result<()> {
        let bytes = hex::decode(JAVA_V1_SEMANTIC).unwrap();
        for len in 0..bytes.len() {
            let input = write_fixture(
                &format!("memory:/tmp/truncated_file_index_{len}"),
                bytes[..len].to_vec(),
            )
            .await?;

            let error = match FileIndexFormatReader::get_file_index(input).await {
                Ok(_) => panic!("truncated prefix of {len} bytes must fail"),
                Err(error) => error,
            };
            assert!(matches!(error, Error::FileIndexFormatInvalid { .. }));
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_reader_rejects_malformed_java_v1_headers() -> crate::Result<()> {
        let valid = hex::decode(JAVA_V1_SIMPLE).unwrap();
        let mut cases = Vec::new();

        let mut bytes = valid.clone();
        bytes[12..16].copy_from_slice(&23i32.to_be_bytes());
        cases.push(("short_header_length", bytes));

        let mut bytes = valid.clone();
        bytes[12..16].copy_from_slice(&46i32.to_be_bytes());
        cases.push(("header_exceeds_file", bytes));

        let mut bytes = valid.clone();
        bytes[16..20].copy_from_slice(&(-1i32).to_be_bytes());
        cases.push(("negative_column_count", bytes));

        let mut bytes = valid.clone();
        bytes[23..27].copy_from_slice(&(-1i32).to_be_bytes());
        cases.push(("negative_index_count", bytes));

        let mut bytes = valid.clone();
        bytes[22] = 0x80;
        cases.push(("invalid_modified_utf8", bytes));

        let mut bytes = valid.clone();
        bytes[30..34].copy_from_slice(&(-2i32).to_be_bytes());
        cases.push(("negative_index_start", bytes));

        let mut bytes = valid.clone();
        bytes[34..38].copy_from_slice(&(-1i32).to_be_bytes());
        cases.push(("negative_index_length", bytes));

        let mut bytes = valid.clone();
        bytes[30..34].copy_from_slice(&EMPTY_INDEX_FLAG.to_be_bytes());
        bytes[34..38].copy_from_slice(&1i32.to_be_bytes());
        cases.push(("non_empty_sentinel", bytes));

        let mut bytes = valid.clone();
        bytes[30..34].copy_from_slice(&41i32.to_be_bytes());
        cases.push(("range_overlaps_header", bytes));

        let mut bytes = valid.clone();
        bytes[34..38].copy_from_slice(&4i32.to_be_bytes());
        cases.push(("range_exceeds_file", bytes));

        let mut bytes = valid;
        bytes[38..42].copy_from_slice(&1i32.to_be_bytes());
        cases.push(("missing_redundant_byte", bytes));

        for (name, bytes) in cases {
            let input = write_fixture(&format!("memory:/tmp/{name}"), bytes).await?;
            let error = match FileIndexFormatReader::get_file_index(input).await {
                Ok(_) => panic!("malformed case {name} must fail"),
                Err(error) => error,
            };
            assert!(matches!(error, Error::FileIndexFormatInvalid { .. }));
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_large_header_round_trip() -> crate::Result<()> {
        let mut column_indexes = HashMap::new();
        for index in 0..18 {
            let name = format!("{index:02}{}", "x".repeat(59_998));
            column_indexes.insert(name, Some(Bytes::from(vec![index as u8])));
        }
        let expected = column_indexes.clone();
        let indexes = HashMap::from([("column".to_string(), column_indexes)]);

        let output = write_column_indexes("memory:/tmp/large_header", indexes).await?;
        let raw = output.clone().to_input_file().read().await?;
        assert!(i32::from_be_bytes(raw[12..16].try_into().unwrap()) > 1024 * 1024);

        let reader = FileIndexFormatReader::get_file_index(output.to_input_file()).await?;
        assert_eq!(reader.get_column_index("column").await?, expected);
        Ok(())
    }

    #[tokio::test]
    async fn test_writer_rejects_overlong_modified_utf8_name() {
        let indexes =
            HashMap::from([("x".repeat(65_536), HashMap::<String, Option<Bytes>>::new())]);

        let error = match write_column_indexes("memory:/tmp/overlong_name", indexes).await {
            Ok(_) => panic!("overlong name must fail"),
            Err(error) => error,
        };

        assert!(matches!(error, Error::FileIndexFormatInvalid { .. }));
    }

    #[tokio::test]
    async fn test_single_column_single_index() -> crate::Result<()> {
        let path = "memory:/tmp/test_single_column_single_index";

        let mut indexes = HashMap::new();
        let mut index_map = HashMap::new();
        index_map.insert("index1".to_string(), Some(Bytes::from("sample_data")));
        indexes.insert("column111".to_string(), index_map);

        let output = write_column_indexes(path, indexes.clone()).await?;

        let input = output.to_input_file();

        let reader = FileIndexFormatReader::get_file_index(input).await?;
        let column_data = reader.get_column_index("column111").await?;
        assert_eq!(
            column_data.get("index1").unwrap(),
            &Some(Bytes::from("sample_data"))
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_columns_multiple_indexes() -> crate::Result<()> {
        let path = "memory:/tmp/test_multiple_columns_multiple_indexes";

        let mut indexes = HashMap::new();
        for col_num in 1..5 {
            let column_name = format!("column{col_num}");
            let mut index_map = HashMap::new();
            for idx_num in 1..5 {
                index_map.insert(
                    format!("index{idx_num}"),
                    Some(random_bytes(100 + col_num * idx_num)),
                );
            }
            indexes.insert(column_name, index_map);
        }

        let output = write_column_indexes(path, indexes.clone()).await?;

        let input = output.to_input_file();

        let reader = FileIndexFormatReader::get_file_index(input).await?;
        for (column, index_map) in indexes {
            let column_data = reader.get_column_index(&column).await?;
            for (index_name, expected_data) in index_map {
                assert_eq!(column_data.get(&index_name).unwrap(), &expected_data);
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_empty_file_index() -> crate::Result<()> {
        let path = "memory:/tmp/test_empty_file_index";

        let mut indexes = HashMap::new();
        let mut a_index = HashMap::new();
        a_index.insert("b".to_string(), None);
        a_index.insert("c".to_string(), Some(Bytes::new()));
        indexes.insert("a".to_string(), a_index);

        let output = write_column_indexes(path, indexes.clone()).await?;

        let input = output.to_input_file();

        let reader = FileIndexFormatReader::get_file_index(input).await?;

        let column_indexes = reader.get_column_index("a").await?;
        assert_eq!(column_indexes.len(), 2);
        assert_eq!(column_indexes.get("b"), Some(&None));
        assert_eq!(column_indexes.get("c"), Some(&Some(Bytes::new())));

        Ok(())
    }

    #[tokio::test]
    async fn test_large_data_set() -> crate::Result<()> {
        let path = "memory:/tmp/test_large_data_set";

        let mut indexes = HashMap::new();
        let mut large_data = HashMap::new();
        large_data.insert("large_index".to_string(), Some(random_bytes(100_000_000))); // 100MB data
        indexes.insert("large_column".to_string(), large_data);

        write_column_indexes(path, indexes.clone()).await?;

        let output = write_column_indexes(path, indexes.clone()).await?;

        let input = output.to_input_file();

        let reader = FileIndexFormatReader::get_file_index(input).await?;
        let column_data = reader.get_column_index("large_column").await?;
        assert_eq!(
            column_data.get("large_index"),
            indexes.get("large_column").unwrap().get("large_index")
        );

        Ok(())
    }

    fn random_bytes(len: usize) -> Bytes {
        use rand::RngCore;
        let mut rng = rand::thread_rng();
        let mut bytes = vec![0u8; len];
        rng.fill_bytes(&mut bytes);
        Bytes::from(bytes)
    }
}
