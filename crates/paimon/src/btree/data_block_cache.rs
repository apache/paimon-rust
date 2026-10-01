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

use crate::btree::block::BlockReader;
use lru::LruCache;
use std::sync::{Arc, Mutex};

#[derive(Clone, PartialEq, Eq, Hash)]
pub(super) struct DataBlockCacheKey {
    pub(super) file: Arc<str>,
    pub(super) offset: u64,
    pub(super) size: u32,
}

struct Entry {
    block: Arc<BlockReader>,
    retained_bytes: usize,
}

struct State {
    entries: LruCache<DataBlockCacheKey, Entry>,
    retained_bytes: usize,
}

pub(crate) struct BTreeDataBlockCache {
    max_bytes: usize,
    state: Mutex<State>,
}

impl BTreeDataBlockCache {
    pub(crate) fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            state: Mutex::new(State {
                entries: LruCache::unbounded(),
                retained_bytes: 0,
            }),
        }
    }

    pub(super) fn get(&self, key: &DataBlockCacheKey) -> Option<Arc<BlockReader>> {
        if self.max_bytes == 0 {
            return None;
        }
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entries
            .get(key)
            .map(|entry| Arc::clone(&entry.block))
    }

    pub(super) fn put(&self, key: DataBlockCacheKey, block: Arc<BlockReader>) -> Arc<BlockReader> {
        if self.max_bytes == 0 {
            return block;
        }
        let retained_bytes = block.retained_bytes();
        if retained_bytes > self.max_bytes {
            return block;
        }

        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(entry) = state.entries.get(&key) {
            return Arc::clone(&entry.block);
        }
        state.retained_bytes = state.retained_bytes.saturating_add(retained_bytes);
        state.entries.put(
            key,
            Entry {
                block: Arc::clone(&block),
                retained_bytes,
            },
        );
        while state.retained_bytes > self.max_bytes {
            let Some((_, entry)) = state.entries.pop_lru() else {
                break;
            };
            state.retained_bytes = state.retained_bytes.saturating_sub(entry.retained_bytes);
        }
        block
    }

    #[cfg(test)]
    pub(crate) fn retained_bytes(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .retained_bytes
    }
}
