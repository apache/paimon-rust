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

use super::{FMReadOptions, FMWriteOptions};
use crate::btree::BlockCompressionType;
use crate::{Error, Result};
use std::collections::HashMap;

pub(crate) const PARTITION_SIZE_OPTION: &str = "fm-index.partition-size";
pub(crate) const PARTITION_ROW_COUNT_OPTION: &str = "fm-index.partition-row-count";
pub(crate) const SA_SAMPLE_RATE_OPTION: &str = "fm-index.sa-sample-rate";
pub(crate) const COMPRESSION_OPTION: &str = "fm-index.compression";
pub(crate) const COMPRESSION_LEVEL_OPTION: &str = "fm-index.compression-level";
pub(crate) const READ_CACHE_SIZE_OPTION: &str = "fm-index.read-cache-size";
pub(crate) const DEMAND_PAGE_SIZE_OPTION: &str = "fm-index.demand-page-size";
pub(crate) const LOCATE_COST_RATIO_OPTION: &str = "fm-index.locate-cost-ratio";

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct FMOptions {
    pub(crate) write: FMWriteOptions,
    pub(crate) read: FMReadOptions,
}

impl FMOptions {
    pub(crate) fn from_options(options: &HashMap<String, String>) -> Result<Self> {
        let write = FMWriteOptions {
            partition_size: parse_memory(
                options,
                PARTITION_SIZE_OPTION,
                FMWriteOptions::default().partition_size,
            )?,
            partition_row_count: parse_usize(
                options,
                PARTITION_ROW_COUNT_OPTION,
                FMWriteOptions::default().partition_row_count,
            )?,
            sample_rate: parse_usize(
                options,
                SA_SAMPLE_RATE_OPTION,
                FMWriteOptions::default().sample_rate,
            )?,
            compression: parse_compression(options)?,
            compression_level: parse_i32(
                options,
                COMPRESSION_LEVEL_OPTION,
                FMWriteOptions::default().compression_level,
            )?,
        };
        write
            .validate()
            .map_err(|error| invalid_option(error.to_string()))?;

        let read = FMReadOptions {
            cache_size: parse_memory(
                options,
                READ_CACHE_SIZE_OPTION,
                FMReadOptions::default().cache_size,
            )?,
            demand_page_size: parse_memory(
                options,
                DEMAND_PAGE_SIZE_OPTION,
                FMReadOptions::default().demand_page_size,
            )?,
            locate_cost_ratio: parse_f64(
                options,
                LOCATE_COST_RATIO_OPTION,
                FMReadOptions::default().locate_cost_ratio,
            )?,
        };
        read.validate()
            .map_err(|error| invalid_option(error.to_string()))?;
        Ok(Self { write, read })
    }
}

fn parse_memory(options: &HashMap<String, String>, key: &str, default: usize) -> Result<usize> {
    let Some(raw) = options.get(key) else {
        return Ok(default);
    };
    let value = crate::common::options::parse_memory_size(raw).map_err(|_| {
        invalid_option(format!(
            "Option '{key}' must be a non-negative memory size, got: {raw}"
        ))
    })?;
    usize::try_from(value).map_err(|_| {
        invalid_option(format!(
            "Option '{key}' must be a non-negative memory size supported by this platform, got: {raw}"
        ))
    })
}

fn parse_usize(options: &HashMap<String, String>, key: &str, default: usize) -> Result<usize> {
    options
        .get(key)
        .map(|raw| {
            raw.parse::<usize>().map_err(|_| {
                invalid_option(format!(
                    "Option '{key}' must be a non-negative integer, got: {raw}"
                ))
            })
        })
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn parse_i32(options: &HashMap<String, String>, key: &str, default: i32) -> Result<i32> {
    options
        .get(key)
        .map(|raw| {
            raw.parse::<i32>().map_err(|_| {
                invalid_option(format!("Option '{key}' must be an integer, got: {raw}"))
            })
        })
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn parse_f64(options: &HashMap<String, String>, key: &str, default: f64) -> Result<f64> {
    options
        .get(key)
        .map(|raw| {
            raw.parse::<f64>()
                .map_err(|_| invalid_option(format!("Option '{key}' must be a number, got: {raw}")))
        })
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn parse_compression(options: &HashMap<String, String>) -> Result<BlockCompressionType> {
    match options
        .get(COMPRESSION_OPTION)
        .map(String::as_str)
        .unwrap_or("lz4")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "none" => Ok(BlockCompressionType::None),
        "zstd" => Ok(BlockCompressionType::Zstd),
        "lz4" => Ok(BlockCompressionType::Lz4),
        "lzo" => Ok(BlockCompressionType::Lzo),
        value => Err(invalid_option(format!(
            "Option '{COMPRESSION_OPTION}' must be one of none, zstd, lz4, or lzo, got: {value}"
        ))),
    }
}

fn invalid_option(message: impl Into<String>) -> Error {
    Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_java_option_names() {
        assert_eq!(
            FMOptions::from_options(&HashMap::new()).unwrap(),
            FMOptions {
                write: FMWriteOptions::default(),
                read: FMReadOptions::default(),
            }
        );
        let options = HashMap::from([
            (PARTITION_SIZE_OPTION.to_string(), "2mb".to_string()),
            (PARTITION_ROW_COUNT_OPTION.to_string(), "7".to_string()),
            (SA_SAMPLE_RATE_OPTION.to_string(), "8".to_string()),
            (COMPRESSION_OPTION.to_string(), "NONE".to_string()),
            (COMPRESSION_LEVEL_OPTION.to_string(), "3".to_string()),
            (READ_CACHE_SIZE_OPTION.to_string(), "1mb".to_string()),
            (DEMAND_PAGE_SIZE_OPTION.to_string(), "64kb".to_string()),
            (LOCATE_COST_RATIO_OPTION.to_string(), "0.25".to_string()),
        ]);
        assert_eq!(
            FMOptions::from_options(&options).unwrap(),
            FMOptions {
                write: FMWriteOptions {
                    partition_size: 2 * 1024 * 1024,
                    partition_row_count: 7,
                    sample_rate: 8,
                    compression: BlockCompressionType::None,
                    compression_level: 3,
                },
                read: FMReadOptions {
                    cache_size: 1024 * 1024,
                    demand_page_size: 64 * 1024,
                    locate_cost_ratio: 0.25,
                },
            }
        );
    }

    #[test]
    fn rejects_invalid_ranges() {
        for (key, value) in [
            (PARTITION_SIZE_OPTION, "1"),
            (PARTITION_ROW_COUNT_OPTION, "0"),
            (SA_SAMPLE_RATE_OPTION, "3"),
            (READ_CACHE_SIZE_OPTION, "-1"),
            (DEMAND_PAGE_SIZE_OPTION, "1kb"),
            (LOCATE_COST_RATIO_OPTION, "NaN"),
            (LOCATE_COST_RATIO_OPTION, "1.1"),
        ] {
            assert!(
                FMOptions::from_options(&HashMap::from([(key.to_string(), value.to_string(),)]))
                    .is_err(),
                "{key}={value}"
            );
        }
    }
}
