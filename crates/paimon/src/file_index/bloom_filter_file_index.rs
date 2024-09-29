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

use crate::error::{DeserializationSnafu, SerializationSnafu};
use crate::io::InputFile;

use bloomfilter::Bloom;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

const BLOOM_FILTER_VERSION_1: u8 = 1;

#[derive(Serialize, Deserialize)]
struct BloomFilterFileIndexMeta {
    bitmap_bits: u64,
    k_num: u32,
    sip_keys: [(u64, u64); 2],
}

pub struct BloomFilterFileIndexWriter<K>
where
    K: Serialize + DeserializeOwned + Clone + Eq + Hash,
{
    bloom_filter: Bloom<K>,
}

impl<K> BloomFilterFileIndexWriter<K>
where
    K: Serialize + DeserializeOwned + Clone + Eq + Hash,
{
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        let bloom_filter = Bloom::new_for_fp_rate(expected_items, false_positive_rate);
        Self { bloom_filter }
    }

    pub fn write(&mut self, key: Option<K>) {
        if let Some(key) = key {
            self.bloom_filter.set(&key);
        }
    }

    pub fn serialized_bytes(&self) -> crate::Result<Bytes> {
        // 1. Get the bitmap and metadata of the Bloom Filter
        let bit_vec_bytes = self.bloom_filter.bitmap();
        let bitmap_bits = self.bloom_filter.number_of_bits();
        let k_num = self.bloom_filter.number_of_hash_functions();
        let sip_keys = self.bloom_filter.sip_keys();

        // 2. Create metadata and serialize
        let meta = BloomFilterFileIndexMeta {
            bitmap_bits,
            k_num,
            sip_keys,
        };
        let meta_bytes = serde_json::to_vec(&meta).context(SerializationSnafu)?;
        let meta_size = meta_bytes.len();

        // 3. Calculate total size
        let version_size = 1; // BLOOM_FILTER_VERSION_1 is one byte
        let meta_size_size = 8; // u64
        let bit_vec_size_size = 8; // u64
        let bit_vec_size = bit_vec_bytes.len();
        let total_size =
            version_size + meta_size_size + meta_size + bit_vec_size_size + bit_vec_size;

        // 4. Allocate buffer
        let mut output = BytesMut::with_capacity(total_size);

        // 5. Write data
        // Write version
        output.put_u8(BLOOM_FILTER_VERSION_1);

        // Write metadata size
        output.put_u64_le(meta_size as u64);

        // Write metadata
        output.put_slice(&meta_bytes);

        // Write bitmap size
        output.put_u64_le(bit_vec_size as u64);

        // Write bitmap
        output.put_slice(&bit_vec_bytes);

        Ok(output.freeze())
    }
}

#[allow(dead_code)]
pub struct BloomFilterFileIndexReader<K>
where
    K: Serialize + DeserializeOwned + Clone + Eq + Hash,
{
    input_file: Arc<InputFile>,
    bloom_filter: Option<Bloom<K>>,
}

impl<K> BloomFilterFileIndexReader<K>
where
    K: Serialize + DeserializeOwned + Clone + Eq + Hash,
{
    pub async fn new(input_file: Arc<InputFile>) -> crate::Result<Self> {
        let input = input_file.read().await?;
        let mut buf = input.clone();

        if buf.remaining() < 1 {
            return Err(crate::Error::FileIndexFormatInvalid {
                message: "The file is too small to contain the version byte".to_string(),
            });
        }
        let version = buf.get_u8();
        if version != BLOOM_FILTER_VERSION_1 {
            return Err(crate::Error::FileIndexFormatInvalid {
                message: format!("Unsupported version: {}", version),
            });
        }

        if buf.remaining() < 8 {
            return Err(crate::Error::FileIndexFormatInvalid {
                message: "The file is too small to contain metadata size".to_string(),
            });
        }
        let meta_size = buf.get_u64_le() as usize;

        if buf.remaining() < meta_size {
            return Err(crate::Error::FileIndexFormatInvalid {
                message: "The file is too small to contain metadata".to_string(),
            });
        }
        let meta_bytes = buf.copy_to_bytes(meta_size);

        let meta: BloomFilterFileIndexMeta =
            serde_json::from_slice(&meta_bytes).context(DeserializationSnafu)?;

        if buf.remaining() < 8 {
            return Err(crate::Error::FileIndexFormatInvalid {
                message: "The file is too small to contain bitmap size".to_string(),
            });
        }
        let bit_vec_size = buf.get_u64_le() as usize;

        if buf.remaining() < bit_vec_size {
            return Err(crate::Error::FileIndexFormatInvalid {
                message: "The file is too small to contain bitmap".to_string(),
            });
        }
        let bit_vec_bytes = buf.copy_to_bytes(bit_vec_size);

        let bloom_filter =
            Bloom::from_existing(&bit_vec_bytes, meta.bitmap_bits, meta.k_num, meta.sip_keys);

        Ok(Self {
            input_file,
            bloom_filter: Some(bloom_filter),
        })
    }

    pub fn contains(&self, key: &K) -> bool {
        if let Some(bloom_filter) = &self.bloom_filter {
            bloom_filter.check(key)
        } else {
            false
        }
    }
}

#[cfg(test)]
mod bloom_filter_index_test {
    use super::*;
    use crate::io::FileIO;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_bloom_filter_index_read_write() -> crate::Result<()> {
        let path = "memory:/tmp/test_bloom_filter_index";
        let file_io = FileIO::from_url(path)?.build()?;

        let mut writer = BloomFilterFileIndexWriter::<String>::new(1000, 0.01);

        writer.write(Some("key1".to_string()));
        writer.write(Some("key2".to_string()));
        writer.write(Some("key3".to_string()));

        let bytes = writer.serialized_bytes()?;

        let output = file_io.new_output(path)?;
        let mut file_writer = output.writer().await?;
        file_writer.write(bytes).await?;
        file_writer.close().await?;

        let input_file = output.to_input_file();

        let reader = BloomFilterFileIndexReader::<String>::new(Arc::new(input_file)).await?;

        assert!(reader.contains(&"key1".to_string()));
        assert!(reader.contains(&"key2".to_string()));
        assert!(reader.contains(&"key3".to_string()));
        assert!(!reader.contains(&"key4".to_string()));

        file_io.delete_file(path).await?;

        Ok(())
    }
}
