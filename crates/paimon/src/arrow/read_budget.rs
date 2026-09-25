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

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::resource::{MemoryReservation, ResourceContext};

use crate::spec::PARQUET_ROW_GROUP_MAX_INFLIGHT_BYTES_OPTION;

const BYTE_PERMIT_UNIT: u64 = 1024 * 1024;
const DEFAULT_BYTE_OPTION: &str = "the row-group read budget";
const DEFAULT_PARALLELISM: usize = 8;
const DEFAULT_MAX_INFLIGHT_BYTES: u64 = 256 * 1024 * 1024;

/// Scheduling window for concurrent row-group reads, with memory reservations
/// backed by a [`ResourceContext`]. Slot and prefetch-byte limits control how
/// much work may start; the context admits the full estimate without clamping.
/// Without an explicit context, memory reservations have no additional limit
/// and the existing oversized-row-group scheduling behavior is preserved.
#[derive(Debug, Clone)]
pub struct ReadBudget {
    parallelism: usize,
    row_groups: Arc<Semaphore>,
    // A look-ahead window, independent of shared memory accounting.
    prefetch: Arc<Semaphore>,
    resources: Option<ResourceContext>,
    byte_permits: u32,
    byte_permit_unit: u64,
    max_inflight_bytes: u64,
    byte_option: &'static str,
    oversized_warning_logged: Arc<AtomicBool>,
    diagnostics: Arc<ReadBudgetDiagnostics>,
}

#[derive(Debug)]
struct ReadBudgetDiagnostics {
    enabled: AtomicBool,
    row_group_count: AtomicU64,
    projected_bytes_min: AtomicU64,
    projected_bytes_max: AtomicU64,
    projected_bytes_total: AtomicU64,
    current_inflight: AtomicUsize,
    peak_inflight: AtomicUsize,
}

impl Default for ReadBudgetDiagnostics {
    fn default() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            row_group_count: AtomicU64::new(0),
            projected_bytes_min: AtomicU64::new(u64::MAX),
            projected_bytes_max: AtomicU64::new(0),
            projected_bytes_total: AtomicU64::new(0),
            current_inflight: AtomicUsize::new(0),
            peak_inflight: AtomicUsize::new(0),
        }
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct ReadBudgetDiagnosticsSnapshot {
    pub(crate) row_group_count: u64,
    pub(crate) projected_bytes_min: u64,
    pub(crate) projected_bytes_max: u64,
    pub(crate) projected_bytes_total: u64,
    pub(crate) current_inflight: usize,
    pub(crate) peak_inflight: usize,
}

impl ReadBudget {
    /// Scan-wide scheduling window with MiB-granular prefetch permits.
    /// Its oversized-row-group warning names the Parquet option.
    pub fn new(parallelism: usize, max_inflight_bytes: u64) -> crate::Result<Self> {
        Ok(
            Self::with_byte_granularity(parallelism, max_inflight_bytes, BYTE_PERMIT_UNIT)?
                .with_byte_option(PARQUET_ROW_GROUP_MAX_INFLIGHT_BYTES_OPTION),
        )
    }

    /// Scheduling window whose permits count `byte_permit_unit` bytes each.
    /// Mosaic passes `1`; at most `u32::MAX` permits are tracked, which caps
    /// that window at 4 GiB. Memory reservations always use the full estimate.
    pub(crate) fn with_byte_granularity(
        parallelism: usize,
        max_inflight_bytes: u64,
        byte_permit_unit: u64,
    ) -> crate::Result<Self> {
        if parallelism == 0 || parallelism > Semaphore::MAX_PERMITS {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Row-group read parallelism must be between 1 and {}, got {parallelism}",
                    Semaphore::MAX_PERMITS
                ),
                source: None,
            });
        }
        if max_inflight_bytes == 0 {
            return Err(crate::Error::DataInvalid {
                message: "Row-group read max in-flight bytes must be greater than 0".to_string(),
                source: None,
            });
        }
        let byte_permit_unit = byte_permit_unit.max(1);
        let max_byte_permits = Semaphore::MAX_PERMITS.min(u32::MAX as usize) as u32;
        let byte_permits = max_inflight_bytes
            .div_ceil(byte_permit_unit)
            .min(u64::from(max_byte_permits)) as u32;

        Ok(Self {
            parallelism,
            row_groups: Arc::new(Semaphore::new(parallelism)),
            prefetch: Arc::new(Semaphore::new(byte_permits as usize)),
            resources: None,
            byte_permits,
            byte_permit_unit,
            max_inflight_bytes,
            byte_option: DEFAULT_BYTE_OPTION,
            oversized_warning_logged: Arc::new(AtomicBool::new(false)),
            diagnostics: Arc::new(ReadBudgetDiagnostics::default()),
        })
    }

    /// Name the table option that set the byte budget, for the oversized warning.
    pub(crate) fn with_byte_option(mut self, byte_option: &'static str) -> Self {
        self.byte_option = byte_option;
        self
    }

    pub fn parallelism(&self) -> usize {
        self.parallelism
    }

    pub(crate) fn enable_diagnostics(&self) {
        self.diagnostics.enabled.store(true, Ordering::Relaxed);
    }

    pub(crate) fn diagnostics_enabled(&self) -> bool {
        self.diagnostics.enabled.load(Ordering::Relaxed)
    }

    pub(crate) fn record_projected_row_groups(&self, projected_bytes: &[u64]) {
        if !self.diagnostics_enabled() || projected_bytes.is_empty() {
            return;
        }
        self.diagnostics
            .row_group_count
            .fetch_add(projected_bytes.len() as u64, Ordering::Relaxed);
        self.diagnostics.projected_bytes_min.fetch_min(
            *projected_bytes.iter().min().expect("checked non-empty"),
            Ordering::Relaxed,
        );
        self.diagnostics.projected_bytes_max.fetch_max(
            *projected_bytes.iter().max().expect("checked non-empty"),
            Ordering::Relaxed,
        );
        self.diagnostics.projected_bytes_total.fetch_add(
            projected_bytes
                .iter()
                .copied()
                .fold(0u64, u64::saturating_add),
            Ordering::Relaxed,
        );
    }

    pub(crate) fn diagnostics(&self) -> ReadBudgetDiagnosticsSnapshot {
        let row_group_count = self.diagnostics.row_group_count.load(Ordering::Relaxed);
        ReadBudgetDiagnosticsSnapshot {
            row_group_count,
            projected_bytes_min: if row_group_count == 0 {
                0
            } else {
                self.diagnostics.projected_bytes_min.load(Ordering::Relaxed)
            },
            projected_bytes_max: self.diagnostics.projected_bytes_max.load(Ordering::Relaxed),
            projected_bytes_total: self
                .diagnostics
                .projected_bytes_total
                .load(Ordering::Relaxed),
            current_inflight: self.diagnostics.current_inflight.load(Ordering::Relaxed),
            peak_inflight: self.diagnostics.peak_inflight.load(Ordering::Relaxed),
        }
    }

    /// Bind a shared memory budget while preserving this scheduling window.
    /// Working estimates draw from this context. An
    /// oversized row group must still fit the context's memory limit.
    pub fn with_resources(&self, resources: ResourceContext) -> Self {
        Self {
            resources: Some(resources),
            ..self.clone()
        }
    }

    pub(crate) fn has_resources(&self) -> bool {
        self.resources.is_some()
    }

    /// Merge inputs need to advance in lockstep. Keep memory admission, while
    /// disabling background row-group prefetch that could hold slots they need.
    pub(crate) fn without_prefetch(&self) -> Self {
        Self {
            parallelism: 1,
            ..self.clone()
        }
    }

    /// Wait for scheduling capacity, then try memory admission. Memory rejection
    /// is immediate: waiting for a caller's retained buffers could deadlock.
    pub(crate) async fn acquire(&self, estimated_bytes: u64) -> crate::Result<ReadPermit> {
        let row_group = Arc::clone(&self.row_groups)
            .acquire_owned()
            .await
            .map_err(|_| Self::closed("row-group"))?;
        let prefetch = Arc::clone(&self.prefetch)
            .acquire_many_owned(self.byte_permits_for(estimated_bytes))
            .await
            .map_err(|_| Self::closed("prefetch"))?;
        let mut permit = self.reserve_memory(estimated_bytes)?;
        permit.prefetch = Some((row_group, prefetch));
        Ok(permit)
    }

    /// Take scheduling capacity and memory if both are available now.
    pub(crate) fn try_acquire(&self, estimated_bytes: u64) -> Option<ReadPermit> {
        let row_group = Arc::clone(&self.row_groups).try_acquire_owned().ok()?;
        let prefetch = Arc::clone(&self.prefetch)
            .try_acquire_many_owned(self.byte_permits_for(estimated_bytes))
            .ok()?;
        let mut permit = self.reserve_memory(estimated_bytes).ok()?;
        permit.prefetch = Some((row_group, prefetch));
        Some(permit)
    }

    /// Foreground reads, including merge inputs, must not wait while another
    /// input holds a scheduling slot. They still reserve from the shared pool.
    pub(crate) fn reserve_memory(&self, estimated_bytes: u64) -> crate::Result<ReadPermit> {
        let bytes =
            usize::try_from(estimated_bytes).map_err(|_| crate::Error::ResourceExhausted {
                message: format!("Row-group estimate {estimated_bytes} exceeds addressable memory"),
            })?;
        let memory = self
            .resources
            .as_ref()
            .map(|resources| {
                let mut memory = resources.reservation();
                memory.try_grow(bytes)?;
                Ok::<_, crate::Error>(memory)
            })
            .transpose()?;
        let diagnostics = self.diagnostics_enabled().then(|| {
            let current = self
                .diagnostics
                .current_inflight
                .fetch_add(1, Ordering::Relaxed)
                + 1;
            self.diagnostics
                .peak_inflight
                .fetch_max(current, Ordering::Relaxed);
            Arc::clone(&self.diagnostics)
        });
        Ok(ReadPermit {
            _memory: memory,
            prefetch: None,
            diagnostics,
        })
    }

    /// Block until a row-group slot and the estimated bytes are free. The
    /// Mosaic reader decodes on its own threads, so it waits here.
    pub(crate) fn acquire_blocking(&self, estimated_bytes: u64) -> crate::Result<ReadPermit> {
        futures::executor::block_on(self.acquire(estimated_bytes))
    }

    fn closed(resource: &str) -> crate::Error {
        crate::Error::UnexpectedError {
            message: format!("The {resource} read budget was closed"),
            source: None,
        }
    }

    fn byte_permits_for(&self, estimated_bytes: u64) -> u32 {
        if estimated_bytes > self.max_inflight_bytes
            && !self.oversized_warning_logged.swap(true, Ordering::Relaxed)
        {
            log::warn!(
                "A row group's estimated size ({estimated_bytes} bytes) exceeds {} ({} bytes); it \
                 will occupy the entire prefetch window and may reduce row-group read parallelism; \
                 increase the option if memory allows",
                self.byte_option,
                self.max_inflight_bytes
            );
        }
        estimated_bytes
            .max(1)
            .div_ceil(self.byte_permit_unit)
            .min(u64::from(self.byte_permits)) as u32
    }
}

impl Default for ReadBudget {
    fn default() -> Self {
        Self::new(DEFAULT_PARALLELISM, DEFAULT_MAX_INFLIGHT_BYTES)
            .expect("default Parquet read budget is valid")
    }
}

#[derive(Debug)]
pub(crate) struct ReadPermit {
    // Return memory before waking scheduling waiters.
    _memory: Option<MemoryReservation>,
    prefetch: Option<(OwnedSemaphorePermit, OwnedSemaphorePermit)>,
    diagnostics: Option<Arc<ReadBudgetDiagnostics>>,
}

impl Drop for ReadPermit {
    fn drop(&mut self) {
        if let Some(diagnostics) = &self.diagnostics {
            diagnostics.current_inflight.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn shared_budget_blocks_until_permits_are_released() {
        let budget = Arc::new(ReadBudget::new(2, BYTE_PERMIT_UNIT).unwrap());
        let first = budget.acquire(2 * BYTE_PERMIT_UNIT).await.unwrap();

        assert!(
            tokio::time::timeout(Duration::from_millis(20), budget.acquire(1))
                .await
                .is_err(),
            "the projected-byte budget must be shared across readers"
        );

        drop(first);
        tokio::time::timeout(Duration::from_secs(1), budget.acquire(1))
            .await
            .expect("dropping a read must release its permits")
            .unwrap();
    }

    #[tokio::test]
    async fn diagnostics_aggregate_shared_row_group_reads() {
        let budget = Arc::new(ReadBudget::new(2, 2 * BYTE_PERMIT_UNIT).unwrap());
        budget.enable_diagnostics();
        budget.record_projected_row_groups(&[300, 100, 200]);

        let first = budget.acquire(1).await.unwrap();
        let second = budget.acquire(1).await.unwrap();
        assert_eq!(
            budget.diagnostics(),
            ReadBudgetDiagnosticsSnapshot {
                row_group_count: 3,
                projected_bytes_min: 100,
                projected_bytes_max: 300,
                projected_bytes_total: 600,
                current_inflight: 2,
                peak_inflight: 2,
            }
        );

        drop(first);
        drop(second);
        assert_eq!(budget.diagnostics().current_inflight, 0);
        assert_eq!(budget.diagnostics().peak_inflight, 2);
    }

    #[test]
    fn rejects_invalid_limits() {
        assert!(ReadBudget::new(0, BYTE_PERMIT_UNIT).is_err());
        assert!(ReadBudget::new(1, 0).is_err());
        assert!(
            ReadBudget::new(Semaphore::MAX_PERMITS.saturating_add(1), BYTE_PERMIT_UNIT).is_err()
        );
    }

    #[tokio::test]
    async fn oversized_row_group_consumes_the_budget() {
        let max_inflight_bytes = 8 * BYTE_PERMIT_UNIT + 1;
        let budget = Arc::new(ReadBudget::new(8, max_inflight_bytes).unwrap());
        let first = budget.acquire(max_inflight_bytes).await.unwrap();
        assert!(!budget.oversized_warning_logged.load(Ordering::Relaxed));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), budget.acquire(1))
                .await
                .is_err(),
            "an oversized row group must consume the whole byte budget"
        );
        drop(first);
        let oversized = budget.acquire(max_inflight_bytes + 1).await.unwrap();
        assert!(budget.oversized_warning_logged.load(Ordering::Relaxed));
        drop(oversized);
        budget.acquire(1).await.unwrap();
    }

    #[tokio::test]
    async fn small_row_groups_keep_exact_accounting() {
        let budget = Arc::new(ReadBudget::new(4, 4 * BYTE_PERMIT_UNIT).unwrap());
        let mut permits = Vec::new();
        for _ in 0..4 {
            permits.push(budget.acquire(BYTE_PERMIT_UNIT).await.unwrap());
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(20), budget.acquire(BYTE_PERMIT_UNIT))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn tiny_budget_still_admits_one_at_a_time() {
        let budget = Arc::new(ReadBudget::new(8, BYTE_PERMIT_UNIT).unwrap());
        let first = budget.acquire(100 * BYTE_PERMIT_UNIT).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), budget.acquire(1))
                .await
                .is_err(),
            "a single-permit budget admits exactly one read"
        );
        drop(first);
        budget.acquire(1).await.unwrap();
    }

    #[test]
    fn try_acquire_is_cumulative_and_an_oversized_head_takes_the_whole_budget() {
        // Byte-exact granularity, the way the Mosaic reader charges row groups.
        let budget = ReadBudget::with_byte_granularity(8, 20, 1).unwrap();
        let first = budget.try_acquire(10).unwrap();
        let second = budget.try_acquire(10).unwrap();
        assert!(budget.try_acquire(10).is_none());
        drop(first);
        let third = budget.try_acquire(10).unwrap();
        drop((second, third));

        let oversized = budget.try_acquire(21).unwrap();
        assert!(budget.try_acquire(1).is_none());
        drop(oversized);
        assert!(budget.try_acquire(1).is_some());

        let slot_limited = ReadBudget::with_byte_granularity(2, u64::MAX, 1).unwrap();
        let first = slot_limited.try_acquire(1).unwrap();
        let second = slot_limited.try_acquire(1).unwrap();
        assert!(slot_limited.try_acquire(1).is_none());
        drop((first, second));
    }

    #[test]
    fn acquire_blocking_waits_off_the_runtime_until_a_permit_is_released() {
        let budget = Arc::new(ReadBudget::with_byte_granularity(1, 10, 1).unwrap());
        let held = budget.try_acquire(10).unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let waiter = std::thread::spawn({
            let budget = Arc::clone(&budget);
            move || {
                let permit = budget.acquire_blocking(10);
                tx.send(()).ok();
                permit
            }
        });

        assert!(
            rx.recv_timeout(Duration::from_millis(20)).is_err(),
            "the budget must hold the waiter until a permit is released"
        );
        drop(held);
        waiter.join().unwrap().unwrap();
    }

    #[tokio::test]
    async fn row_groups_and_other_consumers_share_one_memory_limit() {
        let resources = ResourceContext::builder().memory_limit(32).build().unwrap();
        let first = ReadBudget::new(2, 1024 * 1024)
            .unwrap()
            .with_resources(resources.clone());
        let second = ReadBudget::new(2, 1024 * 1024)
            .unwrap()
            .with_resources(resources.clone());
        let read = first.acquire(20).await.unwrap();
        let mut consumer = resources.reservation();
        consumer.try_grow(8).unwrap();
        assert!(matches!(
            second.acquire(5).await,
            Err(crate::Error::ResourceExhausted { .. })
        ));
        assert_eq!(resources.metrics().reserved_memory_bytes, 28);
        // Rejected memory admission must release both scheduling resources.
        let other = second.acquire(4).await.unwrap();
        assert_eq!(resources.metrics().reserved_memory_bytes, 32);
        drop((read, other, consumer));
        assert_eq!(resources.metrics().reserved_memory_bytes, 0);
        assert_eq!(resources.metrics().peak_reserved_memory_bytes, 32);
    }

    #[tokio::test]
    async fn oversized_prefetch_admission_cannot_bypass_the_memory_limit() {
        let resources = ResourceContext::builder().memory_limit(3).build().unwrap();
        let budget = ReadBudget::with_byte_granularity(2, 1, 1)
            .unwrap()
            .with_resources(resources.clone());
        assert!(matches!(
            budget.acquire(4).await,
            Err(crate::Error::ResourceExhausted { .. })
        ));
        assert_eq!(resources.metrics().reserved_memory_bytes, 0);
        let permit = budget.acquire(3).await.unwrap();
        // The prefetch window is clamped to one; the memory charge is all three.
        assert_eq!(resources.metrics().reserved_memory_bytes, 3);
        drop(permit);
        assert_eq!(resources.metrics().reserved_memory_bytes, 0);
    }

    #[tokio::test]
    async fn cancelling_a_prefetch_waiter_does_not_hold_slots_or_memory() {
        let resources = ResourceContext::builder().memory_limit(32).build().unwrap();
        let budget = ReadBudget::new(2, 1024 * 1024)
            .unwrap()
            .with_resources(resources.clone());
        let first = budget.acquire(16).await.unwrap();
        let mut waiting = Box::pin(budget.acquire(16));
        assert!(futures::poll!(&mut waiting).is_pending());
        assert_eq!(resources.metrics().reserved_memory_bytes, 16);
        drop(waiting);
        drop(first);
        assert_eq!(resources.metrics().reserved_memory_bytes, 0);
        let first = budget.acquire(32).await.unwrap();
        drop(first);
        assert_eq!(budget.row_groups.available_permits(), 2);
        assert_eq!(resources.metrics().reserved_memory_bytes, 0);
    }
}
