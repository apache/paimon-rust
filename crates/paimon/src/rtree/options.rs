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

use super::{format::page_bytes, RTreeIndexOptions, RTreePageCache};
use crate::{Error, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Semaphore;

/// Page bytes and active file traversals are bounded across queries using the same index.
pub(crate) struct RTreeReadContext {
    pub cache: Arc<RTreePageCache>,
    pub batch_bytes: usize,
    pub permits: Arc<Semaphore>,
}

impl Default for RTreeReadContext {
    fn default() -> Self {
        Self {
            cache: Arc::new(RTreePageCache::new(8 << 20)),
            batch_bytes: 1 << 20,
            permits: Arc::new(Semaphore::new(4)),
        }
    }
}

impl RTreeReadContext {
    pub fn from_options(map: &HashMap<String, String>) -> Result<Self> {
        let invalid = |key: &str| Error::ConfigInvalid {
            message: format!("Invalid R-tree option {key}: {:?}", map.get(key)),
        };
        let memory = |key: &str, default| -> Result<usize> {
            map.get(key).map_or(Ok(default), |value| {
                crate::common::options::parse_memory_size(value)
                    .ok()
                    .and_then(|v| usize::try_from(v).ok())
                    .ok_or_else(|| invalid(key))
            })
        };
        let mut cache = 8 << 20;
        if let Some(pages) = map.get("rtree-index.read-cache-pages") {
            let pages = pages
                .parse::<usize>()
                .map_err(|_| invalid("rtree-index.read-cache-pages"))?;
            if !map.contains_key("rtree-index.read-cache-size") {
                cache = pages
                    .checked_mul(page_bytes(RTreeIndexOptions::from_options(map)?.page_size))
                    .ok_or_else(|| invalid("rtree-index.read-cache-pages"))?;
            }
        }
        let cache = memory("rtree-index.read-cache-size", cache)?;
        let batch_bytes = memory("rtree-index.read-batch-size", 1 << 20)?;
        if !(4096..=16 << 20).contains(&batch_bytes) {
            return Err(invalid("rtree-index.read-batch-size"));
        }
        let concurrency = map
            .get("rtree-index.read-file-concurrency")
            .map_or(Ok(4), |v| {
                v.parse::<usize>()
                    .map_err(|_| invalid("rtree-index.read-file-concurrency"))
            })?;
        if !(1..=Semaphore::MAX_PERMITS).contains(&concurrency) {
            return Err(invalid("rtree-index.read-file-concurrency"));
        }
        Ok(Self {
            cache: Arc::new(RTreePageCache::new(cache)),
            batch_bytes,
            permits: Arc::new(Semaphore::new(concurrency)),
        })
    }
}
