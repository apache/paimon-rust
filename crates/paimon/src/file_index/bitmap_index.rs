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

use std::hash::Hash;
use std::sync::Arc;

use crate::error::{BitmapSerializationSnafu, DeserializationSnafu, SerializationSnafu};
use crate::io::{FileRead, InputFile};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use indexmap::IndexMap;
use roaring::RoaringTreemap;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

const BITMAP_VERSION_1: u8 = 1;

#[derive(Serialize, Deserialize)]
struct BitmapFileIndexMeta<K>
where
    K: Serialize + DeserializeOwned + Clone + Eq + Hash,
{
    row_count: u64,
    non_null_bitmap_number: u64,
    has_null_value: bool,
    null_value_offset: Option<i64>,
    #[serde(with = "indexmap::map::serde_seq")]
    bitmap_offsets: IndexMap<K, i64>,
}

pub struct BitmapFileIndexWriter<K>
where
    K: Serialize + DeserializeOwned + Clone + Eq + Hash,
{
    id2bitmap: IndexMap<K, RoaringTreemap>,
    null_bitmap: RoaringTreemap,
    row_number: u64,
}

impl<K> Default for BitmapFileIndexWriter<K>
where
    K: Serialize + DeserializeOwned + Clone + Eq + Hash,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K> BitmapFileIndexWriter<K>
where
    K: Serialize + DeserializeOwned + Clone + Eq + Hash,
{
    pub fn new() -> Self {
        Self {
            id2bitmap: IndexMap::new(),
            null_bitmap: RoaringTreemap::new(),
            row_number: 0,
        }
    }

    pub fn write(&mut self, key: Option<K>) {
        if let Some(key) = key {
            self.id2bitmap
                .entry(key)
                .or_default()
                .insert(self.row_number);
        } else {
            self.null_bitmap.insert(self.row_number);
        }
        self.row_number += 1;
    }

    pub fn serialized_bytes(&self) -> crate::Result<Bytes> {
        // 1. Serialize the null_bitmap
        let mut null_bitmap_bytes = Vec::new();
        self.null_bitmap
            .serialize_into(&mut null_bitmap_bytes)
            .context(BitmapSerializationSnafu)?;
        let null_bitmap_size = null_bitmap_bytes.len();

        // 2. Serialize each bitmap and calculate total size
        let mut bitmap_offsets = IndexMap::new();
        let mut serialized_bitmaps = Vec::new();
        let mut total_bitmap_size = 0;

        for (key, bitmap) in &self.id2bitmap {
            if bitmap.len() == 1 {
                // Single value bitmap, offset is negative
                let value = -1_i64 - (bitmap.iter().next().unwrap() as i64);
                bitmap_offsets.insert(key.clone(), value);
            } else {
                let mut bitmap_bytes = Vec::new();
                bitmap
                    .serialize_into(&mut bitmap_bytes)
                    .context(BitmapSerializationSnafu)?;
                let bitmap_size = bitmap_bytes.len();
                serialized_bitmaps.push((bitmap_bytes, bitmap_size));
                bitmap_offsets.insert(key.clone(), total_bitmap_size as i64);
                total_bitmap_size += bitmap_size;
            }
        }

        // 3. Handle null bitmap offset
        let null_value_offset = if !self.null_bitmap.is_empty() {
            Some(if self.null_bitmap.len() == 1 {
                -1_i64 - (self.null_bitmap.iter().next().unwrap() as i64)
            } else {
                0_i64
            })
        } else {
            None
        };

        // 4. Create metadata and serialize it
        let meta = BitmapFileIndexMeta {
            row_count: self.row_number,
            non_null_bitmap_number: self.id2bitmap.len() as u64,
            has_null_value: !self.null_bitmap.is_empty(),
            null_value_offset,
            bitmap_offsets,
        };

        let meta_bytes = serde_json::to_vec(&meta).context(SerializationSnafu)?;
        let meta_size = meta_bytes.len();

        // 5. Calculate total size
        let version_size = 1; // BITMAP_VERSION_1 is a single byte
        let meta_size_size = 8; // u64
        let total_size = version_size
            + meta_size_size
            + meta_size
            + if self.null_bitmap.len() > 1 {
                null_bitmap_size
            } else {
                0
            }
            + total_bitmap_size;

        // 6. Allocate buffer with total_size
        let mut output = BytesMut::with_capacity(total_size);

        // 7. Write data into buffer
        // Write version
        output.put_u8(BITMAP_VERSION_1);

        // Write meta_size as u64 (little-endian)
        output.put_u64_le(meta_size as u64);

        // Write metadata
        output.put_slice(&meta_bytes);

        // Write null_bitmap if necessary
        if self.null_bitmap.len() > 1 {
            output.put_slice(&null_bitmap_bytes);
        }

        // Write all bitmaps
        for (bitmap_bytes, _size) in serialized_bitmaps {
            output.put_slice(&bitmap_bytes);
        }

        Ok(output.freeze())
    }
}

pub struct BitmapFileIndexReader<K>
where
    K: Serialize + DeserializeOwned + Clone + Eq + Hash,
{
    input_file: Arc<InputFile>,
    meta: BitmapFileIndexMeta<K>,
    bitmaps: IndexMap<K, RoaringTreemap>,
    null_bitmap: Option<RoaringTreemap>,
    body_offset: u64,
}

impl<K> BitmapFileIndexReader<K>
where
    K: Serialize + DeserializeOwned + Clone + Eq + Hash,
{
    pub async fn new(input_file: Arc<InputFile>) -> crate::Result<Self> {
        let input = input_file.read().await?;
        let mut buf = input.clone();

        if buf.remaining() < 1 {
            return Err(crate::Error::FileIndexFormatInvalid {
                message: "File too small to contain version byte".to_string(),
            });
        }
        let version = buf.get_u8();
        if version != BITMAP_VERSION_1 {
            return Err(crate::Error::FileIndexFormatInvalid {
                message: format!("Unsupported version: {}", version),
            });
        }

        if buf.remaining() < 8 {
            return Err(crate::Error::FileIndexFormatInvalid {
                message: "File too small to contain meta_size".to_string(),
            });
        }
        let meta_size = buf.get_u64_le() as usize;

        if buf.remaining() < meta_size {
            return Err(crate::Error::FileIndexFormatInvalid {
                message: "File too small to contain metadata".to_string(),
            });
        }
        let meta_bytes = buf.copy_to_bytes(meta_size);

        let meta: BitmapFileIndexMeta<K> =
            serde_json::from_slice(&meta_bytes).context(DeserializationSnafu)?;

        let body_offset = input.len() - buf.remaining();

        Ok(Self {
            input_file,
            meta,
            bitmaps: IndexMap::new(),
            null_bitmap: None,
            body_offset: body_offset as u64,
        })
    }

    pub async fn get_bitmap(&mut self, key: Option<&K>) -> crate::Result<RoaringTreemap> {
        if let Some(key) = key {
            if let Some(bitmap) = self.bitmaps.get(key) {
                return Ok(bitmap.clone());
            }
            if let Some(&offset) = self.meta.bitmap_offsets.get(key) {
                let bitmap = self.load_bitmap(offset).await?;
                self.bitmaps.insert(key.clone(), bitmap.clone());
                Ok(bitmap)
            } else {
                Ok(RoaringTreemap::new())
            }
        } else {
            if let Some(bitmap) = &self.null_bitmap {
                return Ok(bitmap.clone());
            }
            if let Some(offset) = self.meta.null_value_offset {
                let bitmap = self.load_bitmap(offset).await?;
                self.null_bitmap = Some(bitmap.clone());
                Ok(bitmap)
            } else {
                Ok(RoaringTreemap::new())
            }
        }
    }

    async fn load_bitmap(&self, offset: i64) -> crate::Result<RoaringTreemap> {
        if offset < 0 {
            let index = (-1 - offset) as u64;
            let mut bitmap = RoaringTreemap::new();
            bitmap.insert(index);
            Ok(bitmap)
        } else {
            let bitmap_pos = self.body_offset as i64 + offset;
            let file_meta = self.input_file.metadata().await?;
            if bitmap_pos < 0 {
                return Err(crate::Error::FileIndexFormatInvalid {
                    message: format!("Invalid bitmap offset: {}", bitmap_pos),
                });
            }

            if bitmap_pos as u64 > file_meta.size {
                return Err(crate::Error::FileIndexFormatInvalid {
                    message: format!(
                        "Bitmap offset {} exceeds file size {}",
                        bitmap_pos, file_meta.size
                    ),
                });
            }

            let reader = self.input_file.reader().await?;
            let range = bitmap_pos as u64..file_meta.size;
            let buf = reader.read(range).await?;
            let bitmap = RoaringTreemap::deserialize_from(&mut &buf[..])
                .map_err(|e| crate::Error::BitmapDeserializationError { source: e })?;

            Ok(bitmap)
        }
    }
}

#[cfg(test)]
mod basic_bitmap_index_test {
    use super::*;
    use crate::io::FileIO;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_basic_bitmap_index_read_write() -> crate::Result<()> {
        let path = "memory:/tmp/test_basic_bitmap_index";
        let file_io = FileIO::from_url(path)?.build()?;

        let mut writer = BitmapFileIndexWriter::<String>::new();

        writer.write(Some("key1".to_string()));
        writer.write(None);
        writer.write(Some("key2".to_string()));
        writer.write(Some("key1".to_string()));

        let bytes = writer.serialized_bytes()?;

        let output = file_io.new_output(path)?;
        let mut file_writer = output.writer().await?;
        file_writer.write(bytes).await?;
        file_writer.close().await?;

        let input_file = output.to_input_file();

        let mut reader = BitmapFileIndexReader::<String>::new(Arc::new(input_file)).await?;

        let bitmap_key1 = reader.get_bitmap(Some(&"key1".to_string())).await?;
        assert_eq!(bitmap_key1.len(), 2);
        assert!(bitmap_key1.contains(0));
        assert!(bitmap_key1.contains(3));

        let bitmap_key2 = reader.get_bitmap(Some(&"key2".to_string())).await?;
        assert_eq!(bitmap_key2.len(), 1);
        assert!(bitmap_key2.contains(2));

        let bitmap_none = reader.get_bitmap(None).await?;
        assert_eq!(bitmap_none.len(), 1);
        assert!(bitmap_none.contains(1));

        file_io.delete_file(path).await?;

        Ok(())
    }
}
