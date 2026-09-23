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
use std::sync::{Arc, Barrier};

use super::{MemoryPool, ResourceContext};
use crate::{Error, Result};

fn used(context: &ResourceContext) -> usize {
    context.metrics().reserved_memory_bytes
}

#[test]
fn reservations_share_the_limit_and_failed_growth_is_atomic() {
    let resources = ResourceContext::builder().memory_limit(8).build().unwrap();
    let mut first = resources.reservation();
    let mut second = resources.clone().reservation();
    first.try_grow(8).unwrap();
    assert!(matches!(
        second.try_grow(1),
        Err(Error::ResourceExhausted { .. })
    ));
    assert_eq!(second.size(), 0);
    first.try_resize(3).unwrap();
    second.try_grow(5).unwrap();
    assert!(first.try_resize(9).is_err());
    assert_eq!(first.size(), 3);
    assert_eq!(used(&resources), 8);
    drop(first);
    assert_eq!(used(&resources), 5);
    drop(second);
    assert_eq!(used(&resources), 0);
    assert_eq!(resources.metrics().peak_reserved_memory_bytes, 8);
}

#[test]
fn zero_limit_and_address_space_overflow_are_rejected() {
    let zero = ResourceContext::builder().memory_limit(0).build().unwrap();
    let mut reservation = zero.reservation();
    reservation.try_grow(0).unwrap();
    assert!(reservation.try_grow(1).is_err());
    assert_eq!(used(&zero), 0);

    let unlimited = ResourceContext::builder().build().unwrap();
    let mut reservation = unlimited.reservation();
    // Reservations are accounting; this does not allocate memory.
    reservation.try_grow(usize::MAX).unwrap();
    assert!(reservation.try_grow(1).is_err());
    assert!(unlimited.reservation().try_grow(1).is_err());
    assert_eq!(used(&unlimited), usize::MAX);
    drop(reservation);
    assert_eq!(used(&unlimited), 0);
}

#[derive(Debug)]
struct EnginePool {
    limit: usize,
    reserved: AtomicUsize,
    released: AtomicUsize,
}

impl MemoryPool for EnginePool {
    fn try_reserve(&self, bytes: usize) -> Result<()> {
        self.reserved
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(bytes).filter(|next| *next <= self.limit)
            })
            .map_err(|_| Error::ResourceExhausted {
                message: "engine budget".to_string(),
            })?;
        Ok(())
    }

    fn release(&self, bytes: usize) {
        assert!(self.reserved.fetch_sub(bytes, Ordering::Relaxed) >= bytes);
        self.released.fetch_add(bytes, Ordering::Relaxed);
    }
}

#[test]
fn external_pool_failure_rolls_back_local_admission() {
    let pool = Arc::new(EnginePool {
        limit: 12,
        reserved: AtomicUsize::new(0),
        released: AtomicUsize::new(0),
    });
    let left = ResourceContext::builder()
        .memory_limit(10)
        .memory_pool(pool.clone())
        .build()
        .unwrap();
    let right = ResourceContext::builder()
        .memory_limit(10)
        .memory_pool(pool.clone())
        .build()
        .unwrap();
    let mut first = left.reservation();
    let mut second = right.reservation();
    first.try_grow(8).unwrap();
    let error = second.try_grow(5).unwrap_err();
    assert!(matches!(error, Error::ResourceExhausted { message } if message == "engine budget"));
    assert_eq!(used(&right), 0);
    assert_eq!(right.metrics().peak_reserved_memory_bytes, 0);
    second.try_grow(4).unwrap();
    assert!(first.try_grow(3).is_err());
    drop(first);
    // This reaches the local limit, proving the rejected request was rolled back.
    second.try_grow(6).unwrap();
    assert_eq!(used(&right), 10);
    assert_eq!(pool.reserved.load(Ordering::Relaxed), 10);
    drop(second);
    assert_eq!(pool.reserved.load(Ordering::Relaxed), 0);
    assert_eq!(pool.released.load(Ordering::Relaxed), 18);
}

#[test]
fn concurrent_reservations_cannot_overcommit() {
    const THREADS: usize = 16;
    let resources = ResourceContext::builder().memory_limit(32).build().unwrap();
    let start = Arc::new(Barrier::new(THREADS + 1));
    let ready = Arc::new(Barrier::new(THREADS + 1));
    let release = Arc::new(Barrier::new(THREADS + 1));
    let threads: Vec<_> = (0..THREADS)
        .map(|_| {
            let resources = resources.clone();
            let (start, ready, release) = (start.clone(), ready.clone(), release.clone());
            std::thread::spawn(move || {
                let mut reservation = resources.reservation();
                start.wait();
                let admitted = reservation.try_grow(8).is_ok();
                ready.wait();
                release.wait();
                admitted
            })
        })
        .collect();
    start.wait();
    ready.wait();
    assert_eq!(used(&resources), 32);
    release.wait();
    let admitted = threads
        .into_iter()
        .map(|thread| usize::from(thread.join().unwrap()))
        .sum::<usize>();
    assert_eq!(admitted, 4);
    assert_eq!(used(&resources), 0);
    assert_eq!(resources.metrics().peak_reserved_memory_bytes, 32);
}
