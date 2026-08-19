// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Java-compatible write options shared by sorted global indexes.

use super::global_index_types::{
    BITMAP_GLOBAL_INDEX_TYPE, BTREE_GLOBAL_INDEX_TYPE, MULTIVALUE_GLOBAL_INDEX_TYPE,
};
use crate::btree::BlockCompressionType;
use crate::{Error, Result};
use std::collections::HashMap;

pub(crate) const BTREE_BLOCK_SIZE_OPTION: &str = "btree-index.block-size";
pub(crate) const BTREE_COMPRESSION_OPTION: &str = "btree-index.compression";
pub(crate) const BTREE_COMPRESSION_LEVEL_OPTION: &str = "btree-index.compression-level";
pub(crate) const BITMAP_DICTIONARY_BLOCK_SIZE_OPTION: &str = "bitmap-index.dictionary-block-size";
pub(crate) const BITMAP_COMPRESSION_OPTION: &str = "bitmap-index.compression";
pub(crate) const BITMAP_COMPRESSION_LEVEL_OPTION: &str = "bitmap-index.compression-level";
pub(crate) const MULTIVALUE_DICTIONARY_BLOCK_SIZE_OPTION: &str =
    "multivalue-index.dictionary-block-size";
pub(crate) const MULTIVALUE_COMPRESSION_OPTION: &str = "multivalue-index.compression";
pub(crate) const MULTIVALUE_COMPRESSION_LEVEL_OPTION: &str = "multivalue-index.compression-level";

const DEFAULT_BTREE_BLOCK_SIZE: usize = 64 * 1024;
const DEFAULT_BITMAP_DICTIONARY_BLOCK_SIZE: usize = 16 * 1024;
const DEFAULT_COMPRESSION_LEVEL: i32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SortedIndexWriteOptions {
    pub(crate) block_size: usize,
    pub(crate) compression_type: BlockCompressionType,
    pub(crate) compression_level: i32,
}

impl SortedIndexWriteOptions {
    pub(crate) fn from_options(
        index_type: &str,
        options: &HashMap<String, String>,
    ) -> Result<Self> {
        let (block_size_option, compression_option, compression_level_option, default_block_size) =
            match index_type {
                BTREE_GLOBAL_INDEX_TYPE => (
                    BTREE_BLOCK_SIZE_OPTION,
                    BTREE_COMPRESSION_OPTION,
                    BTREE_COMPRESSION_LEVEL_OPTION,
                    DEFAULT_BTREE_BLOCK_SIZE,
                ),
                BITMAP_GLOBAL_INDEX_TYPE => (
                    BITMAP_DICTIONARY_BLOCK_SIZE_OPTION,
                    BITMAP_COMPRESSION_OPTION,
                    BITMAP_COMPRESSION_LEVEL_OPTION,
                    DEFAULT_BITMAP_DICTIONARY_BLOCK_SIZE,
                ),
                MULTIVALUE_GLOBAL_INDEX_TYPE => (
                    MULTIVALUE_DICTIONARY_BLOCK_SIZE_OPTION,
                    MULTIVALUE_COMPRESSION_OPTION,
                    MULTIVALUE_COMPRESSION_LEVEL_OPTION,
                    DEFAULT_BITMAP_DICTIONARY_BLOCK_SIZE,
                ),
                _ => unreachable!("normalized sorted global index type"),
            };

        Ok(Self {
            block_size: parse_block_size(options, block_size_option, default_block_size)?,
            compression_type: parse_compression(options, compression_option)?,
            compression_level: parse_compression_level(
                options,
                compression_level_option,
                DEFAULT_COMPRESSION_LEVEL,
            )?,
        })
    }
}

fn parse_block_size(
    options: &HashMap<String, String>,
    option: &str,
    default: usize,
) -> Result<usize> {
    let Some(raw) = options.get(option) else {
        return Ok(default);
    };
    let bytes = crate::common::options::parse_memory_size(raw).map_err(|_| Error::DataInvalid {
        message: format!("Option '{option}' must be a valid memory size, got: {raw}"),
        source: None,
    })?;
    if bytes <= 0 {
        return Err(Error::DataInvalid {
            message: format!("Option '{option}' must be greater than 0, got: {raw}"),
            source: None,
        });
    }
    usize::try_from(bytes).map_err(|_| Error::DataInvalid {
        message: format!("Option '{option}' is too large: {raw}"),
        source: None,
    })
}

fn parse_compression(
    options: &HashMap<String, String>,
    option: &str,
) -> Result<BlockCompressionType> {
    let compression = options
        .get(option)
        .map(String::as_str)
        .unwrap_or("none")
        .trim()
        .to_ascii_lowercase();
    match compression.as_str() {
        "none" => Ok(BlockCompressionType::None),
        "zstd" => Ok(BlockCompressionType::Zstd),
        "lz4" => Ok(BlockCompressionType::Lz4),
        "lzo" => Ok(BlockCompressionType::Lzo),
        _ => Err(Error::DataInvalid {
            message: format!(
                "Option '{option}' must be one of none, zstd, lz4, or lzo, got: {compression}"
            ),
            source: None,
        }),
    }
}

fn parse_compression_level(
    options: &HashMap<String, String>,
    option: &str,
    default: i32,
) -> Result<i32> {
    options
        .get(option)
        .map(|raw| {
            raw.parse::<i32>().map_err(|_| Error::DataInvalid {
                message: format!("Option '{option}' must be an integer, got: {raw}"),
                source: None,
            })
        })
        .transpose()
        .map(|level| level.unwrap_or(default))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults_match_java_writer_options() {
        let options = HashMap::new();
        assert_eq!(
            SortedIndexWriteOptions::from_options(BTREE_GLOBAL_INDEX_TYPE, &options).unwrap(),
            SortedIndexWriteOptions {
                block_size: 64 * 1024,
                compression_type: BlockCompressionType::None,
                compression_level: 1,
            }
        );
        for index_type in [BITMAP_GLOBAL_INDEX_TYPE, MULTIVALUE_GLOBAL_INDEX_TYPE] {
            assert_eq!(
                SortedIndexWriteOptions::from_options(index_type, &options).unwrap(),
                SortedIndexWriteOptions {
                    block_size: 16 * 1024,
                    compression_type: BlockCompressionType::None,
                    compression_level: 1,
                }
            );
        }
    }

    #[test]
    fn test_index_specific_options() {
        for (index_type, block_option, compression_option, level_option) in [
            (
                BTREE_GLOBAL_INDEX_TYPE,
                BTREE_BLOCK_SIZE_OPTION,
                BTREE_COMPRESSION_OPTION,
                BTREE_COMPRESSION_LEVEL_OPTION,
            ),
            (
                BITMAP_GLOBAL_INDEX_TYPE,
                BITMAP_DICTIONARY_BLOCK_SIZE_OPTION,
                BITMAP_COMPRESSION_OPTION,
                BITMAP_COMPRESSION_LEVEL_OPTION,
            ),
            (
                MULTIVALUE_GLOBAL_INDEX_TYPE,
                MULTIVALUE_DICTIONARY_BLOCK_SIZE_OPTION,
                MULTIVALUE_COMPRESSION_OPTION,
                MULTIVALUE_COMPRESSION_LEVEL_OPTION,
            ),
        ] {
            let options = HashMap::from([
                (block_option.to_string(), "32kb".to_string()),
                (compression_option.to_string(), "LZ4".to_string()),
                (level_option.to_string(), "7".to_string()),
            ]);
            assert_eq!(
                SortedIndexWriteOptions::from_options(index_type, &options).unwrap(),
                SortedIndexWriteOptions {
                    block_size: 32 * 1024,
                    compression_type: BlockCompressionType::Lz4,
                    compression_level: 7,
                }
            );
        }
    }

    #[test]
    fn test_invalid_options() {
        for (index_type, option, value) in [
            (BTREE_GLOBAL_INDEX_TYPE, BTREE_BLOCK_SIZE_OPTION, "0"),
            (
                BITMAP_GLOBAL_INDEX_TYPE,
                BITMAP_DICTIONARY_BLOCK_SIZE_OPTION,
                "invalid",
            ),
            (
                MULTIVALUE_GLOBAL_INDEX_TYPE,
                MULTIVALUE_COMPRESSION_OPTION,
                "snappy",
            ),
            (
                BTREE_GLOBAL_INDEX_TYPE,
                BTREE_COMPRESSION_LEVEL_OPTION,
                "fast",
            ),
        ] {
            let options = HashMap::from([(option.to_string(), value.to_string())]);
            assert!(SortedIndexWriteOptions::from_options(index_type, &options).is_err());
        }
    }
}
