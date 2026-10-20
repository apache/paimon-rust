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

use bytes::Bytes;
use std::sync::Mutex;

use super::priority_lru::{protected_limit, PriorityLru, DEFAULT_HIGH_PRIORITY_POOL_RATIO};
use super::state::BlockKey;

#[derive(Debug)]
pub(super) struct MemoryCache {
    state: Mutex<MemoryState>,
}

#[derive(Debug)]
struct MemoryState {
    entries: PriorityLru<Bytes>,
    current_size: u64,
    max_size: Option<u64>,
}

impl MemoryCache {
    #[cfg(test)]
    pub(super) fn new(max_size: Option<u64>) -> Self {
        Self::with_ratio(max_size, DEFAULT_HIGH_PRIORITY_POOL_RATIO)
    }

    pub(super) fn with_ratio(max_size: Option<u64>, ratio: f64) -> Self {
        Self {
            state: Mutex::new(MemoryState {
                entries: PriorityLru::new(protected_limit(max_size, ratio)),
                current_size: 0,
                max_size,
            }),
        }
    }

    pub(super) fn get_block(&self, key: &BlockKey) -> Option<Bytes> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.entries.touch(key).cloned()
    }

    pub(super) fn put_block(&self, key: &BlockKey, payload: Bytes) {
        let payload_size = payload.len() as u64;
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state
            .max_size
            .is_some_and(|max_size| payload_size > max_size)
        {
            return;
        }
        if let Some((_, previous_size)) = state.entries.insert(key.clone(), payload, payload_size) {
            state.current_size = state.current_size.saturating_sub(previous_size);
        }
        state.current_size = state.current_size.saturating_add(payload_size);
        while state
            .max_size
            .is_some_and(|max_size| state.current_size > max_size)
        {
            let Some((_, _, size)) = state.entries.pop_lru() else {
                break;
            };
            state.current_size = state.current_size.saturating_sub(size);
        }
    }

    pub(super) fn remove_block(&self, key: &BlockKey) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let Some((_, size)) = state.entries.remove(key) {
            state.current_size = state.current_size.saturating_sub(size);
        }
    }

    pub(super) fn invalidate_path(&self, namespace: &str, path: &str) {
        self.invalidate_matching(|key| key.matches_path(namespace, path));
    }

    pub(super) fn invalidate_prefix(&self, namespace: &str, prefix: &str) {
        let prefix = prefix.trim_end_matches('/');
        self.invalidate_matching(|key| key.matches_prefix(namespace, prefix));
    }

    fn invalidate_matching(&self, matches: impl Fn(&BlockKey) -> bool) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let keys = state
            .entries
            .keys()
            .filter(|key| matches(key))
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            if let Some((_, size)) = state.entries.remove(&key) {
                state.current_size = state.current_size.saturating_sub(size);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_cache_refreshes_lru_and_evicts_by_payload_bytes() {
        let cache = MemoryCache::new(Some(8));
        let first = BlockKey::new("snapshot-1", 4, 0);
        let second = BlockKey::new("snapshot-1", 4, 1);
        let third = BlockKey::new("snapshot-1", 4, 2);

        cache.put_block(&first, Bytes::from_static(b"aaaa"));
        cache.put_block(&second, Bytes::from_static(b"bbbb"));
        assert_eq!(cache.get_block(&first), Some(Bytes::from_static(b"aaaa")));
        cache.put_block(&third, Bytes::from_static(b"cccc"));

        assert_eq!(cache.get_block(&second), None);
        assert_eq!(cache.get_block(&first), Some(Bytes::from_static(b"aaaa")));
        assert_eq!(cache.get_block(&third), Some(Bytes::from_static(b"cccc")));
    }

    #[test]
    fn test_memory_cache_skips_block_larger_than_capacity() {
        let cache = MemoryCache::new(Some(3));
        let key = BlockKey::new("snapshot-1", 4, 0);

        cache.put_block(&key, Bytes::from_static(b"data"));

        assert_eq!(cache.get_block(&key), None);
    }

    #[test]
    fn test_memory_cache_protects_metadata_from_data_scan() {
        let cache = MemoryCache::new(Some(16));
        let metadata = BlockKey::new("snapshot-1", 4, 0);
        cache.put_block(&metadata, Bytes::from_static(b"meta"));
        for index in 1..=4 {
            cache.put_block(
                &BlockKey::new(format!("data-{index}"), 4, 0),
                Bytes::from_static(b"data"),
            );
        }

        assert_eq!(
            cache.get_block(&metadata),
            Some(Bytes::from_static(b"meta"))
        );
        assert_eq!(cache.get_block(&BlockKey::new("data-1", 4, 0)), None);
    }

    #[test]
    fn test_memory_cache_high_priority_uses_idle_capacity() {
        let cache = MemoryCache::new(Some(16));
        for index in 1..=5 {
            cache.put_block(
                &BlockKey::new(format!("snapshot-{index}"), 4, 0),
                Bytes::from_static(b"meta"),
            );
        }

        assert_eq!(cache.get_block(&BlockKey::new("snapshot-1", 4, 0)), None);
        for index in 2..=5 {
            assert_eq!(
                cache.get_block(&BlockKey::new(format!("snapshot-{index}"), 4, 0)),
                Some(Bytes::from_static(b"meta"))
            );
        }
    }

    #[test]
    fn test_memory_cache_zero_ratio_restores_single_lru() {
        let cache = MemoryCache::with_ratio(Some(16), 0.0);
        let metadata = BlockKey::new("snapshot-1", 4, 0);
        cache.put_block(&metadata, Bytes::from_static(b"meta"));
        for index in 1..=4 {
            cache.put_block(
                &BlockKey::new(format!("data-{index}"), 4, 0),
                Bytes::from_static(b"data"),
            );
        }
        assert_eq!(cache.get_block(&metadata), None);
    }

    #[test]
    fn test_memory_cache_high_priority_overflow_competes_with_data() {
        let cache = MemoryCache::new(Some(16));
        for index in 1..=2 {
            cache.put_block(
                &BlockKey::new(format!("snapshot-{index}"), 4, 0),
                Bytes::from_static(b"meta"),
            );
        }
        for index in 1..=3 {
            cache.put_block(
                &BlockKey::new(format!("data-{index}"), 4, 0),
                Bytes::from_static(b"data"),
            );
        }
        assert_eq!(cache.get_block(&BlockKey::new("snapshot-1", 4, 0)), None);
        assert_eq!(
            cache.get_block(&BlockKey::new("snapshot-2", 4, 0)),
            Some(Bytes::from_static(b"meta"))
        );
    }
}
