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

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{
    io::{FileIO, FileRead, FileStatus, InputFile, OutputFile},
    Error,
};

/// Default 1MB read block size
const READ_BLOCK_SIZE: u64 = 1024 * 1024;

/// Quoted from the Java implement of the structure,
/// `MAGIC`` is used to mark the beginning of a FileFormat structure.
pub const MAGIC: u64 = 1493475289347502;

/// Used to mark an empty INDEX.
pub const EMPTY_INDEX_FLAG: i64 = -1;

#[derive(Debug)]
struct IndexInfo {
    start_pos: i64,
    length: i64,
}

#[repr(i32)]
#[derive(Debug, PartialEq, Eq)]
enum Version {
    V1,
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
/// - `column x`: variable-length UTF-8 string (length + bytes)
/// - `index number`: 4-byte integer (number of index items below)
/// - `index name x`: variable-length UTF-8 string
/// - `start pos`: 4-byte integer
/// - `length`: 4-byte integer
/// - `redundant length`: 4-byte integer (for compatibility with future versions; content is zero in this version)
/// - `redundant bytes`: variable-length bytes (for compatibility with future versions; empty in this version)
/// - `BODY`: sequence of index data (concatenated index data for each column)
/// ```
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-common/src/main/java/org/apache/paimon/fileindex/FileIndexFormat.java>
pub async fn write_column_indexes(
    path: &str,
    indexes: HashMap<String, HashMap<String, Bytes>>,
) -> crate::Result<OutputFile> {
    let file_io = FileIO::from_url(path)?.build()?;
    let output = file_io.new_output(path)?;
    let mut writer = output.writer().await?;

    let mut body_info: HashMap<String, HashMap<String, IndexInfo>> = HashMap::new();
    let mut total_data_size = 0;

    // Calculate the total data size
    for bytes_map in indexes.values() {
        for data in bytes_map.values() {
            if !data.is_empty() {
                total_data_size += data.len();
            }
        }
    }

    let mut body = BytesMut::with_capacity(total_data_size);

    for (column_name, bytes_map) in indexes.into_iter() {
        let inner_map = body_info.entry(column_name.clone()).or_default();
        for (index_name, data) in bytes_map {
            let start_position = body.len() as i64;
            if data.is_empty() {
                inner_map.insert(
                    index_name,
                    IndexInfo {
                        start_pos: EMPTY_INDEX_FLAG,
                        length: 0,
                    },
                );
            } else {
                body.extend_from_slice(&data);
                inner_map.insert(
                    index_name,
                    IndexInfo {
                        start_pos: start_position,
                        length: body.len() as i64 - start_position,
                    },
                );
            }
        }
    }

    // write_head(writer, &body_info).await?;
    let head_length = calculate_head_length(&body_info)?;
    let mut head_buffer = BytesMut::with_capacity(head_length);

    // Magic
    head_buffer.put_u64_le(MAGIC);
    // Version
    head_buffer.put_i32_le(Version::V1 as i32);
    // HeadLength
    head_buffer.put_i32_le(head_length as i32);
    // ColumnSize
    head_buffer.put_i32_le(body_info.len() as i32);

    for (column_name, index_info) in body_info {
        // ColumnName
        head_buffer.put_u16_le(column_name.len() as u16);
        head_buffer.put_slice(column_name.as_bytes());
        // IndexTypeSize
        head_buffer.put_i32_le(index_info.len() as i32);
        // ColumnInfo，offset = headLength
        for (index_name, IndexInfo { start_pos, length }) in index_info {
            head_buffer.put_u16_le(index_name.len() as u16);
            head_buffer.put_slice(index_name.as_bytes());
            let adjusted_start = if start_pos == EMPTY_INDEX_FLAG {
                EMPTY_INDEX_FLAG
            } else {
                start_pos + head_length as i64
            };
            head_buffer.put_i64_le(adjusted_start);
            head_buffer.put_i64_le(length);
        }
    }

    // Redundant length for future compatibility
    head_buffer.put_i32_le(0);

    // Write into
    writer.write(head_buffer.freeze()).await?;
    writer.write(body.freeze()).await?;
    writer.close().await?;
    Ok(output)
}

fn calculate_head_length(
    body_info: &HashMap<String, HashMap<String, IndexInfo>>,
) -> crate::Result<usize> {
    // Magic + Version + HeadLength + ColumnNumber + RedundantLength
    let base_length = 8 + 4 + 4 + 4 + 4;
    let mut total_length = base_length;

    for (column_name, index_info) in body_info {
        // Column name length + actual column name length
        total_length += 2 + column_name.len();
        // IndexTypeSize (index number)
        total_length += 4;

        for index_name in index_info.keys() {
            // Index name length + actual index name length
            total_length += 2 + index_name.len();
            // start_pos (8 bytes) + length (8 bytes)
            total_length += 16;
        }
    }

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
    ) -> crate::Result<HashMap<String, Bytes>> {
        if let Some(index_info) = self.header.get(column_name) {
            let mut result = HashMap::new();
            for (index_name, info) in index_info {
                let bytes = self.get_bytes_with_start_and_length(info).await?;
                result.insert(index_name.clone(), bytes);
            }
            Ok(result)
        } else {
            Err(Error::FileIndexFormatInvalid {
                message: format!("Column '{}' not found in header", column_name),
            })
        }
    }

    pub async fn get_index(&self) -> crate::Result<HashMap<String, HashMap<String, Bytes>>> {
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
    ) -> crate::Result<Bytes> {
        let data_bytes = self
            .reader
            .read(index_info.start_pos as u64..(index_info.start_pos + index_info.length) as u64)
            .await?;

        Ok(data_bytes)
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
        let read_size = if self.stat.size < READ_BLOCK_SIZE {
            self.stat.size
        } else {
            READ_BLOCK_SIZE
        };
        let mut buffer = self.reader.read(0..read_size).await?;

        // Magic (8 bytes)
        let magic = buffer.get_u64_le();
        if magic != MAGIC {
            return Err(Error::FileIndexFormatInvalid {
                message: format!("Expected MAGIC: {}, but found: {}", MAGIC, magic),
            });
        }

        // Version (4 bytes)
        let version = buffer.get_i32_le();
        if version != Version::V1 as i32 {
            return Err(Error::FileIndexFormatInvalid {
                message: format!(
                    "Unsupported file index version: expected {}, but found: {}",
                    Version::V1 as i32,
                    version
                ),
            });
        }

        // Head Length (4 bytes)
        let head_length = buffer.get_i32_le() as usize;

        // Ensure the header is fully contained in the buffer
        if buffer.len() < head_length {
            let remaining = head_length - buffer.len();
            let mut remaining_head_buffer = BytesMut::with_capacity(remaining);
            let additional_data = self
                .reader
                .read(buffer.len() as u64..buffer.len() as u64 + remaining as u64)
                .await?;
            remaining_head_buffer.extend_from_slice(&additional_data);
            buffer = Bytes::from(
                [buffer.slice(0..), remaining_head_buffer.freeze().slice(0..)].concat(),
            );
        }

        // Column Number (4 bytes)
        let column_number = buffer.get_i32_le();

        let mut current_offset = 20;
        let mut header = HashMap::new();

        for _ in 0..column_number {
            // Column Name Length (2 bytes)
            let column_name_len = buffer.get_u16_le();
            current_offset += 2;

            // Column Name (variable-length UTF-8 string)
            let column_name = String::from_utf8(buffer.split_to(column_name_len as usize).to_vec())
                .map_err(|e| Error::FileIndexFormatInvalid {
                    message: format!("Invalid UTF-8 sequence in column name: {}", e),
                })?;
            current_offset += column_name_len as u64;

            // Index Number (4 bytes)
            let index_number = buffer.get_i32_le();
            current_offset += 4;

            let mut index_info_map = HashMap::new();
            for _ in 0..index_number {
                // Index Name Length (2 bytes)
                let index_name_len = buffer.get_u16_le();
                current_offset += 2;

                // Index Name (variable-length UTF-8 string)
                let index_name =
                    String::from_utf8(buffer.split_to(index_name_len as usize).to_vec()).unwrap();
                current_offset += index_name_len as u64;

                // Start Pos (8 bytes)
                let start_pos = buffer.get_i64_le();
                current_offset += 4;

                // Length (8 bytes)
                let length = buffer.get_i64_le();
                current_offset += 4;

                index_info_map.insert(index_name, IndexInfo { start_pos, length });
            }

            header.insert(column_name, index_info_map);
        }

        let redundant_length = buffer.get_i32_le() as u64;
        current_offset += 4;

        if redundant_length > 0 {
            let redundant_bytes = buffer.split_to(redundant_length as usize);

            if redundant_bytes.len() as u64 != redundant_length {
                return Err(Error::FileIndexFormatInvalid {
                    message: format!(
                        "Expected to read {} redundant bytes, but found only {}, on offset {}",
                        redundant_length,
                        redundant_bytes.len(),
                        current_offset
                    ),
                });
            }
        }

        Ok(header)
    }
}

#[cfg(test)]
mod file_index_format_tests {

    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_single_column_single_index() -> crate::Result<()> {
        let path = "memory:/tmp/test_single_column_single_index";

        let mut indexes = HashMap::new();
        let mut index_map = HashMap::new();
        index_map.insert("index1".to_string(), Bytes::from("sample_data"));
        indexes.insert("column111".to_string(), index_map);

        let output = write_column_indexes(path, indexes.clone()).await?;

        let input = output.to_input_file();

        let reader = FileIndexFormatReader::get_file_index(input).await?;
        let column_data = reader.get_column_index("column111").await?;
        assert_eq!(
            column_data.get("index1").unwrap(),
            &Bytes::from("sample_data")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_columns_multiple_indexes() -> crate::Result<()> {
        let path = "memory:/tmp/test_multiple_columns_multiple_indexes";

        let mut indexes = HashMap::new();
        for col_num in 1..5 {
            let column_name = format!("column{}", col_num);
            let mut index_map = HashMap::new();
            for idx_num in 1..5 {
                index_map.insert(
                    format!("index{}", idx_num),
                    random_bytes(100 + col_num * idx_num),
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
        a_index.insert("b".to_string(), Bytes::new());
        a_index.insert("c".to_string(), Bytes::new());
        indexes.insert("a".to_string(), a_index);

        let output = write_column_indexes(path, indexes.clone()).await?;

        let input = output.to_input_file();

        let reader = FileIndexFormatReader::get_file_index(input).await?;

        let column_indexes = reader.get_column_index("a").await?;
        assert_eq!(column_indexes.len(), 2);
        assert_eq!(column_indexes.get("b").unwrap(), &Bytes::new());
        assert_eq!(column_indexes.get("c").unwrap(), &Bytes::new());

        Ok(())
    }

    #[tokio::test]
    async fn test_large_data_set() -> crate::Result<()> {
        let path = "memory:/tmp/test_large_data_set";

        let mut indexes = HashMap::new();
        let mut large_data = HashMap::new();
        large_data.insert("large_index".to_string(), random_bytes(100_000_000)); // 100MB data
        indexes.insert("large_column".to_string(), large_data);

        write_column_indexes(path, indexes.clone()).await?;

        let output = write_column_indexes(path, indexes.clone()).await?;

        let input = output.to_input_file();

        let reader = FileIndexFormatReader::get_file_index(input).await?;
        let column_data = reader.get_column_index("large_column").await?;
        assert_eq!(
            column_data.get("large_index").unwrap(),
            &indexes
                .get("large_column")
                .unwrap()
                .get("large_index")
                .unwrap()
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
