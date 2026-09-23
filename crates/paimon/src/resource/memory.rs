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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::{Error, Result};

/// Optional adapter to an embedding engine's memory budget.
///
/// Calls may run concurrently on any runtime thread. Failed reservations must
/// leave the pool unchanged. Methods must not panic; `release` is infallible and
/// may run during unwinding. The pool must not wait for, or call back into, a
/// reader to free memory. Reclamation belongs to the consumer, outside the pool.
pub trait MemoryPool: std::fmt::Debug + Send + Sync + 'static {
    fn try_reserve(&self, bytes: usize) -> Result<()>;
    fn release(&self, bytes: usize);
}

/// Exact reservation counters, not measurements of the process's allocations.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResourceMetrics {
    pub reserved_memory_bytes: usize,
    pub peak_reserved_memory_bytes: usize,
}

#[derive(Debug)]
pub(super) struct MemoryAccount {
    limit: usize,
    external: Option<Arc<dyn MemoryPool>>,
    // Includes requests awaiting approval from the external pool. This makes
    // concurrent admission obey the local cap even before external approval.
    admitted: AtomicUsize,
    reserved: AtomicUsize,
    peak: AtomicUsize,
}

impl MemoryAccount {
    pub(super) fn new(limit: Option<usize>, external: Option<Arc<dyn MemoryPool>>) -> Self {
        Self {
            limit: limit.unwrap_or(usize::MAX),
            external,
            admitted: AtomicUsize::new(0),
            reserved: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }

    fn try_reserve(&self, bytes: usize) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        self.admitted
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(bytes).filter(|&next| next <= self.limit)
            })
            .map_err(|used| Error::ResourceExhausted {
                message: format!(
                    "Cannot reserve {bytes} bytes: {used} bytes already reserved or pending, limit {}",
                    self.limit
                ),
            })?;
        if let Some(pool) = &self.external {
            if let Err(error) = pool.try_reserve(bytes) {
                self.admitted.fetch_sub(bytes, Ordering::Relaxed);
                return Err(error);
            }
        }
        let used = self.reserved.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.peak.fetch_max(used, Ordering::Relaxed);
        Ok(())
    }

    fn release(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        self.reserved.fetch_sub(bytes, Ordering::Relaxed);
        if let Some(pool) = &self.external {
            pool.release(bytes);
        }
        self.admitted.fetch_sub(bytes, Ordering::Relaxed);
    }

    pub(super) fn metrics(&self) -> ResourceMetrics {
        ResourceMetrics {
            reserved_memory_bytes: self.reserved.load(Ordering::Relaxed),
            peak_reserved_memory_bytes: self.peak.load(Ordering::Relaxed),
        }
    }
}

/// Owned reservation. Moving it transfers accounting; dropping it releases it.
///
/// This type deliberately does not implement `Clone`. Shared allocations should
/// retain one reservation in their shared owner rather than charge every alias.
#[derive(Debug)]
pub struct MemoryReservation {
    account: Arc<MemoryAccount>,
    size: usize,
}

impl MemoryReservation {
    pub(super) fn new(account: Arc<MemoryAccount>) -> Self {
        Self { account, size: 0 }
    }

    pub fn size(&self) -> usize {
        self.size
    }

    /// Grow atomically with respect to admission. Failure preserves this guard.
    pub fn try_grow(&mut self, bytes: usize) -> Result<()> {
        self.account.try_reserve(bytes)?;
        // Account-wide checked admission also guarantees this sum fits.
        self.size += bytes;
        Ok(())
    }

    /// Reserve additional bytes, or release surplus bytes, to reach `size`.
    pub fn try_resize(&mut self, size: usize) -> Result<()> {
        if size > self.size {
            self.try_grow(size - self.size)?;
        } else {
            self.account.release(self.size - size);
            self.size = size;
        }
        Ok(())
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        self.account.release(self.size);
    }
}
