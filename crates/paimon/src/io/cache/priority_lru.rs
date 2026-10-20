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

use lru::LruCache;

use super::file_type::FileType;
use super::state::BlockKey;

pub(super) const DEFAULT_HIGH_PRIORITY_POOL_RATIO: f64 = 0.25;

pub(super) fn protected_limit(max_size: Option<u64>, ratio: f64) -> u64 {
    if ratio == 0.0 {
        0
    } else {
        max_size
            .map(|size| (size as f64 * ratio) as u64)
            .unwrap_or(u64::MAX)
    }
}

#[derive(Debug)]
struct Entry<V> {
    value: V,
    size: u64,
}

/// Newest high-priority blocks occupy a protected LRU; excess blocks compete
/// with ordinary blocks in the regular LRU. Either class may use idle capacity.
#[derive(Debug)]
pub(super) struct PriorityLru<V> {
    protected: LruCache<BlockKey, Entry<V>>,
    regular: LruCache<BlockKey, Entry<V>>,
    protected_size: u64,
    protected_limit: u64,
}

impl<V> PriorityLru<V> {
    pub(super) fn new(protected_limit: u64) -> Self {
        Self {
            protected: LruCache::unbounded(),
            regular: LruCache::unbounded(),
            protected_size: 0,
            protected_limit,
        }
    }

    pub(super) fn set_protected_limit(&mut self, limit: u64) {
        self.protected_limit = limit;
        self.demote_excess();
    }

    pub(super) fn contains_key(&self, key: &BlockKey) -> bool {
        self.protected.contains(key) || self.regular.contains(key)
    }

    pub(super) fn keys(&self) -> impl Iterator<Item = &BlockKey> {
        self.protected
            .iter()
            .map(|(key, _)| key)
            .chain(self.regular.iter().map(|(key, _)| key))
    }

    pub(super) fn touch(&mut self, key: &BlockKey) -> Option<&V> {
        if self.protected.contains(key) {
            return self.protected.get(key).map(|entry| &entry.value);
        }
        if !self.regular.contains(key) {
            return None;
        }
        if self.protected_limit > 0 && Self::high_priority(key) {
            let entry = self.regular.pop(key)?;
            self.protected_size = self.protected_size.saturating_add(entry.size);
            self.protected.put(key.clone(), entry);
            self.demote_excess();
            if self.protected.contains(key) {
                self.protected.get(key).map(|entry| &entry.value)
            } else {
                self.regular.get(key).map(|entry| &entry.value)
            }
        } else {
            self.regular.get(key).map(|entry| &entry.value)
        }
    }

    pub(super) fn insert(&mut self, key: BlockKey, value: V, size: u64) -> Option<(V, u64)> {
        let previous = self.remove(&key);
        let entry = Entry { value, size };
        if self.protected_limit > 0 && Self::high_priority(&key) {
            self.protected_size = self.protected_size.saturating_add(size);
            self.protected.put(key, entry);
            self.demote_excess();
        } else {
            self.regular.put(key, entry);
        }
        previous
    }

    pub(super) fn remove(&mut self, key: &BlockKey) -> Option<(V, u64)> {
        if let Some(entry) = self.protected.pop(key) {
            self.protected_size = self.protected_size.saturating_sub(entry.size);
            return Some((entry.value, entry.size));
        }
        self.regular.pop(key).map(|entry| (entry.value, entry.size))
    }

    pub(super) fn pop_lru(&mut self) -> Option<(BlockKey, V, u64)> {
        if let Some((key, entry)) = self.regular.pop_lru() {
            return Some((key, entry.value, entry.size));
        }
        let (key, entry) = self.protected.pop_lru()?;
        self.protected_size = self.protected_size.saturating_sub(entry.size);
        Some((key, entry.value, entry.size))
    }

    fn demote_excess(&mut self) {
        while self.protected_size > self.protected_limit {
            let Some((key, entry)) = self.protected.pop_lru() else {
                break;
            };
            self.protected_size = self.protected_size.saturating_sub(entry.size);
            self.regular.put(key, entry);
        }
    }

    fn high_priority(key: &BlockKey) -> bool {
        matches!(
            FileType::classify(&key.path),
            FileType::Meta | FileType::GlobalIndex
        )
    }
}
