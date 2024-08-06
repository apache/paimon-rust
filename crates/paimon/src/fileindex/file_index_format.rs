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

use std::{collections::HashMap, io::SeekFrom};

use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};

pub const MAGIC: u64 = 1493475289347502;
pub const EMPTY_INDEX_FLAG: i32 = -1;

/// File index file format. Put all columns and offsets in the header.
///
/// ```
///   _____________________________________    _____________________
/// ｜     magic    ｜version｜head length  ｜
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
/// ｜-------------------------------------｜            HEAD
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
/// ｜  redundant length ｜redundant bytes  ｜
/// ｜-------------------------------------｜    ---------------------
/// ｜                BODY                 ｜
/// ｜                BODY                 ｜
/// ｜                BODY                 ｜             BODY
/// ｜                BODY                 ｜
/// ｜_____________________________________｜    _____________________
/// ```
///
/// - `magic`: 8 bytes long
/// - `version`: 4 bytes int
/// - `head length`: 4 bytes int
/// - `column number`: 4 bytes int
/// - `column x`: var bytes utf (length + bytes)
/// - `index number`: 4 bytes int (how many column items below)
/// - `index name x`: var bytes utf
/// - `start pos`: 4 bytes int
/// - `length`: 4 bytes int
/// - `redundant length`: 4 bytes int (for compatibility with later versions, in this version, content is zero)
/// - `redundant bytes`: var bytes (for compatibility with later version, in this version, is empty)
/// - `BODY`: column index bytes + column index bytes + column index bytes + .......

#[derive(Debug)]
pub struct FileIndexFormat;

impl FileIndexFormat {
    pub fn create_writer<W: AsyncWrite + Unpin>(writer: W) -> Writer<W> {
        Writer::new(writer)
    }

    pub async fn create_reader<R: AsyncReadExt + AsyncSeekExt + Unpin + Clone>(
        reader: R,
    ) -> std::io::Result<Reader<R>> {
        Reader::new(reader).await
    }
}

#[allow(dead_code)]
pub struct Writer<W: AsyncWrite + Unpin> {
    writer: BufWriter<W>,
}

#[allow(dead_code)]
impl<W: AsyncWrite + Unpin> Writer<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer: BufWriter::new(writer),
        }
    }

    async fn write_column_indexes(
        &mut self,
        indexes: HashMap<String, HashMap<String, Vec<u8>>>,
    ) -> std::io::Result<()> {
        let mut body_info: HashMap<String, HashMap<String, IndexInfo>> = HashMap::new();
        let mut baos: Vec<u8> = Vec::new();

        for (column_name, bytes_map) in indexes.into_iter() {
            let inner_map = body_info.entry(column_name.clone()).or_default();
            for (index_name, data) in bytes_map {
                let start_position = baos.len() as i32;
                if data.is_empty() {
                    inner_map.insert(
                        index_name,
                        IndexInfo {
                            start_pos: EMPTY_INDEX_FLAG,
                            length: 0,
                        },
                    );
                } else {
                    baos.extend(data);
                    inner_map.insert(
                        index_name,
                        IndexInfo {
                            start_pos: start_position,
                            length: baos.len() as i32 - start_position,
                        },
                    );
                }
            }
        }

        let body = baos;
        self.write_head(&body_info).await?;
        self.writer.write_all(&body).await?;
        Ok(())
    }

    async fn write_head(
        &mut self,
        body_info: &HashMap<String, HashMap<String, IndexInfo>>,
    ) -> std::io::Result<()> {
        let head_length = self.calculate_head_length(body_info).await?;

        // write Magic
        self.writer.write_u64(MAGIC).await?;
        // write Version
        self.writer.write_i32(Version::V1.into()).await?;
        // write HeadLength
        self.writer.write_i32(head_length as i32).await?;
        // write ColumnSize
        self.writer.write_i32(body_info.len() as i32).await?;
        for (column_name, index_info) in body_info {
            // write ColumnName
            self.writer.write_u16(column_name.len() as u16).await?;
            self.writer.write_all(column_name.as_bytes()).await?;
            // write IndexTypeSize
            self.writer.write_i32(index_info.len() as i32).await?;
            // write ColumnInfo, offset = headLength
            for (index_name, IndexInfo { start_pos, length }) in index_info {
                self.writer.write_u16(index_name.len() as u16).await?;
                self.writer.write_all(index_name.as_bytes()).await?;
                let adjusted_start = if *start_pos == EMPTY_INDEX_FLAG {
                    EMPTY_INDEX_FLAG
                } else {
                    *start_pos + head_length as i32
                };
                self.writer.write_i32(adjusted_start).await?;
                self.writer.write_i32(*length).await?;
            }
        }
        // write RedundantLength
        self.writer.write_i32(0).await?;
        Ok(())
    }

    async fn calculate_head_length(
        &self,
        body_info: &HashMap<String, HashMap<String, IndexInfo>>,
    ) -> std::io::Result<usize> {
        let base_length = 8
            + 4
            + 4
            + 4
            + body_info.values().map(|v| v.len()).sum::<usize>() * 8
            + body_info.len() * 4
            + 4;

        let mut baos = Vec::new();
        for (column_name, index_info) in body_info {
            baos.extend_from_slice(&(column_name.len() as u16).to_be_bytes());
            baos.extend_from_slice(column_name.as_bytes());
            for index_name in index_info.keys() {
                baos.extend_from_slice(&(index_name.len() as u16).to_be_bytes());
                baos.extend_from_slice(index_name.as_bytes());
            }
        }

        Ok(base_length + baos.len())
    }
}

#[allow(dead_code)]
pub struct Reader<R: AsyncReadExt + AsyncSeekExt + Unpin + Clone> {
    reader: BufReader<R>,
    header: HashMap<String, HashMap<String, IndexInfo>>,
}

#[allow(dead_code)]
impl<R: AsyncReadExt + AsyncSeekExt + Unpin + Clone> Reader<R> {
    pub async fn new(reader: R) -> std::io::Result<Self> {
        let mut buf_reader = BufReader::new(reader);
        let magic = buf_reader.read_u64().await?;
        if magic != MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid magic number",
            ));
        }

        let version: Version = buf_reader.read_i32().await?.into();
        if version != Version::V1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Unsupported file index version",
            ));
        }

        let head_length = buf_reader.read_i32().await?;
        let mut header = HashMap::new();

        let column_size = buf_reader.read_i32().await?;
        for _ in 0..column_size {
            let column_name = Self::read_string(&mut buf_reader).await?;
            let index_size = buf_reader.read_i32().await?;
            let mut index_info = HashMap::new();
            for _ in 0..index_size {
                let index_name = Self::read_string(&mut buf_reader).await?;
                let start_pos = buf_reader.read_i32().await?;
                let length = buf_reader.read_i32().await?;
                index_info.insert(index_name, IndexInfo { start_pos, length });
            }
            header.insert(column_name, index_info);
        }

        buf_reader.seek(SeekFrom::Start(head_length as u64)).await?;

        Ok(Reader {
            reader: buf_reader,
            header,
        })
    }

    async fn read_column_index(
        &self,
        column_name: &str,
    ) -> std::io::Result<HashMap<String, Vec<u8>>> {
        if let Some(index_info) = self.header.get(column_name) {
            let mut result = HashMap::new();
            for (index_name, info) in index_info {
                let bytes = self.get_bytes_with_start_and_length(info).await?;
                result.insert(index_name.clone(), bytes);
            }
            Ok(result)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Column not found",
            ))
        }
    }

    async fn get_bytes_with_start_and_length(
        &self,
        index_info: &IndexInfo,
    ) -> std::io::Result<Vec<u8>> {
        let mut reader = self.reader.get_ref().clone();
        let mut buffer = vec![0; index_info.length as usize];
        reader
            .seek(SeekFrom::Start(index_info.start_pos as u64))
            .await?;
        reader.read_exact(&mut buffer).await?;
        Ok(buffer)
    }

    async fn read_string(reader: &mut BufReader<R>) -> std::io::Result<String> {
        let len = reader.read_u16().await? as usize;
        let mut buffer = vec![0; len];
        reader.read_exact(&mut buffer).await?;
        Ok(String::from_utf8(buffer).expect("Invalid UTF-8"))
    }

    #[cfg(test)]
    pub async fn get_bytes_with_name_and_type(
        &self,
        column_name: &str,
        index_type: &str,
    ) -> Option<Vec<u8>> {
        if let Some(indexes) = self.header.get(column_name) {
            if let Some(index_info) = indexes.get(index_type) {
                return self.get_bytes_with_start_and_length(index_info).await.ok();
            }
        }
        None
    }
}

#[derive(Debug)]
struct IndexInfo {
    start_pos: i32,
    length: i32,
}

#[derive(Debug, PartialEq, Eq)]
enum Version {
    V1,
}

impl From<i32> for Version {
    fn from(version: i32) -> Self {
        match version {
            1 => Version::V1,
            _ => panic!("Unsupported file index version: {}", version),
        }
    }
}

impl From<Version> for i32 {
    fn from(version: Version) -> Self {
        match version {
            Version::V1 => 1,
        }
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, io::Cursor};

    use rand::{distributions::Uniform, Rng};
    use tokio::io::AsyncWriteExt;

    use crate::fileindex::FileIndexFormat;

    #[tokio::test]
    async fn test_write_read() -> std::io::Result<()> {
        let mut writer_buffer = Cursor::new(Vec::new());
        let mut writer = FileIndexFormat::create_writer(&mut writer_buffer);

        let mut indexes: HashMap<String, HashMap<String, Vec<u8>>> = HashMap::new();
        indexes
            .entry("column1".to_string())
            .or_default()
            .insert("index1".to_string(), vec![1, 2, 3, 4]);
        indexes
            .entry("column1".to_string())
            .or_default()
            .insert("index2".to_string(), vec![5, 6, 7, 8]);

        writer.write_column_indexes(indexes.clone()).await?;
        writer.writer.flush().await?;
        let index_bytes = writer_buffer.into_inner();

        let reader = FileIndexFormat::create_reader(Cursor::new(index_bytes)).await?;
        for (column, types) in indexes {
            for (type_name, expected_data) in types {
                let data = reader
                    .get_bytes_with_name_and_type(&column, &type_name)
                    .await
                    .ok_or_else(|| {
                        std::io::Error::new(std::io::ErrorKind::NotFound, "Data not found")
                    })?;
                assert_eq!(data, expected_data);
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_random_data() -> std::io::Result<()> {
        let mut writer_buffer = Cursor::new(Vec::new());
        let mut writer = FileIndexFormat::create_writer(&mut writer_buffer);

        let mut indexes: HashMap<String, HashMap<String, Vec<u8>>> = HashMap::new();
        let mut rng = rand::thread_rng();
        let char_range = Uniform::new(b'a', b'z' + 1);
        let num_columns = rng.gen_range(1..=100);
        for _ in 0..num_columns {
            let column_name: String = (0..rng.gen_range(1..=50))
                .map(|_| char::from(rng.sample(char_range)))
                .collect();
            let mut column_indexes = HashMap::new();
            let num_indexes = rng.gen_range(1..=100);
            for _ in 0..num_indexes {
                let index_name: String = (0..rng.gen_range(1..=20))
                    .map(|_| char::from(rng.sample(char_range)))
                    .collect();
                let data_len = rng.gen_range(1..=1000);
                let data: Vec<u8> = (0..data_len).map(|_| rng.gen()).collect();
                column_indexes.insert(index_name, data);
            }
            indexes.insert(column_name, column_indexes);
        }

        writer.write_column_indexes(indexes.clone()).await?;
        writer.writer.flush().await?;
        let index_bytes = writer_buffer.into_inner();

        let reader = FileIndexFormat::create_reader(Cursor::new(index_bytes)).await?;
        for (column, types) in indexes {
            for (type_name, expected_data) in types {
                let data = reader
                    .get_bytes_with_name_and_type(&column, &type_name)
                    .await
                    .ok_or_else(|| {
                        std::io::Error::new(std::io::ErrorKind::NotFound, "Data not found")
                    })?;
                assert_eq!(data, expected_data);
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_empty_and_missing_data() -> std::io::Result<()> {
        let mut writer_buffer = Cursor::new(Vec::new());
        let mut writer = FileIndexFormat::create_writer(&mut writer_buffer);

        let mut indexes: HashMap<String, HashMap<String, Vec<u8>>> = HashMap::new();
        indexes
            .entry("a".to_string())
            .or_default()
            .insert("b".to_string(), Vec::new());
        indexes
            .entry("a".to_string())
            .or_default()
            .insert("c".to_string(), vec![1, 2, 3]);

        writer.write_column_indexes(indexes.clone()).await?;
        writer.writer.flush().await?;
        let index_bytes = writer_buffer.into_inner();

        let reader = FileIndexFormat::create_reader(Cursor::new(index_bytes)).await?;
        let file_index_format_list = reader.read_column_index("a").await?;
        assert_eq!(file_index_format_list.len(), 2);

        let empty_data = reader
            .get_bytes_with_name_and_type("a", "b")
            .await
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "Data not found"))?;
        assert!(empty_data.is_empty());

        let normal_data = reader
            .get_bytes_with_name_and_type("a", "c")
            .await
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "Data not found"))?;
        assert_eq!(normal_data, vec![1, 2, 3]);

        Ok(())
    }

    #[should_panic]
    #[tokio::test]
    async fn test_corrupted_data() {
        let mut writer_buffer = Cursor::new(Vec::new());
        let mut writer = FileIndexFormat::create_writer(&mut writer_buffer);

        let mut indexes: HashMap<String, HashMap<String, Vec<u8>>> = HashMap::new();
        indexes
            .entry("column1".to_string())
            .or_default()
            .insert("index1".to_string(), vec![1, 2, 3, 4]);

        assert!(writer.write_column_indexes(indexes).await.is_ok());
        assert!(writer.writer.flush().await.is_ok());
        let mut index_bytes = writer_buffer.into_inner();

        index_bytes[10] = 0xFF;

        let _ = FileIndexFormat::create_reader(Cursor::new(index_bytes)).await;
    }
}
