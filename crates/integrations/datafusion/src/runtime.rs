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

use std::cell::Cell;
use std::future::Future;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::OnceLock;

use tokio::runtime::{Builder, Handle, Runtime};

struct ProcessRuntime {
    pid: u32,
    runtime: OnceLock<Runtime>,
}

static RUNTIME: AtomicPtr<ProcessRuntime> = AtomicPtr::new(std::ptr::null_mut());

thread_local! {
    // Set on every thread the process runtime starts, where `block_in_place` is allowed.
    static ON_PROCESS_RUNTIME_THREAD: Cell<bool> = const { Cell::new(false) };
}

fn build_process_runtime() -> std::io::Result<Runtime> {
    Builder::new_multi_thread()
        .enable_all()
        .on_thread_start(|| ON_PROCESS_RUNTIME_THREAD.with(|flag| flag.set(true)))
        .build()
}

fn global_runtime() -> &'static Runtime {
    let pid = std::process::id();
    let mut current = RUNTIME.load(Ordering::Acquire);
    loop {
        if !current.is_null() {
            // SAFETY: Published states are never freed. Acquire pairs with the
            // successful publication below, so both fields are initialized.
            let state = unsafe { &*current };
            if state.pid == pid {
                return state.runtime.get_or_init(|| {
                    build_process_runtime().expect(
                        "failed to build global tokio runtime for paimon datafusion integration",
                    )
                });
            }
        }

        // A fork inherits the runtime but none of its worker threads. Publish
        // a fresh, uninitialized state before constructing a runtime so a fork
        // during initialization also bypasses the parent's locked OnceLock.
        let next = Box::into_raw(Box::new(ProcessRuntime {
            pid,
            runtime: OnceLock::new(),
        }));
        match RUNTIME.compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => {
                // Do not drop the inherited state: runtime shutdown can wait
                // for threads which no longer exist in the child process.
                current = next;
            }
            Err(actual) => {
                // SAFETY: This state was never published or initialized; no
                // other thread can hold a reference to it.
                unsafe { drop(Box::from_raw(next)) };
                current = actual;
            }
        }
    }
}

/// Returns a [`Handle`] to the global Tokio runtime.
///
/// If a Tokio runtime is already entered on the current thread, its handle is
/// returned directly. Otherwise a lazily-initialised runtime for the current
/// process is used, replacing any state inherited from a parent after `fork`.
pub fn runtime() -> Handle {
    match Handle::try_current() {
        Ok(h) => h,
        _ => global_runtime().handle().clone(),
    }
}

// These helpers work around DataFusion FFI callbacks that may run without an
// entered Tokio runtime. See https://github.com/apache/datafusion/issues/16312.
// A runtime shared within each process avoids creating one on every call;
// if DataFusion fixes runtime propagation end-to-end, we should be able to
// remove these manual fallback runtimes.
pub(crate) async fn await_with_runtime<F>(future: F) -> F::Output
where
    F: Future,
{
    if Handle::try_current().is_ok() {
        future.await
    } else {
        global_runtime().block_on(future)
    }
}

// The blocking variant is for synchronous DataFusion FFI callbacks such as
// CatalogProvider::schema(), where we cannot `.await` directly.
pub(crate) fn block_on_with_runtime<F>(future: F, panic_error: &'static str) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    if Handle::try_current().is_err() {
        return global_runtime().block_on(future);
    }
    if ON_PROCESS_RUNTIME_THREAD.with(Cell::get) {
        // A worker blocked mid-poll strands its LIFO slot and the I/O driver it last parked on.
        // `block_in_place` hands both to another thread first.
        return tokio::task::block_in_place(|| global_runtime().block_on(future));
    }
    // Threads of other runtimes: `block_in_place` panics in a `LocalSet` or a current-thread runtime.
    let handle = global_runtime().handle().clone();
    std::thread::spawn(move || handle.block_on(future))
        .join()
        .expect(panic_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_callers_share_the_process_runtime() {
        let barrier = std::sync::Barrier::new(8);
        std::thread::scope(|scope| {
            let threads: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        barrier.wait();
                        let runtime = global_runtime();
                        assert_eq!(
                            runtime.block_on(async { tokio::spawn(async { 42 }).await.unwrap() }),
                            42
                        );
                        runtime as *const Runtime as usize
                    })
                })
                .collect();
            let runtimes: Vec<_> = threads
                .into_iter()
                .map(|thread| thread.join().unwrap())
                .collect();
            assert!(runtimes.iter().all(|runtime| *runtime == runtimes[0]));
        });
    }

    #[test]
    fn blocking_on_a_process_runtime_worker_keeps_its_queued_tasks_running() {
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        global_runtime().spawn(async move {
            let (value_tx, value_rx) = tokio::sync::oneshot::channel();
            // Spawned from a worker, this lands in that worker's LIFO slot, which cannot be stolen.
            tokio::spawn(async move {
                let _ = value_tx.send(7);
            });
            let value = block_on_with_runtime(
                async move { value_rx.await.unwrap() },
                "blocking runtime test panicked",
            );
            done_tx.send(value).unwrap();
        });
        assert_eq!(
            done_rx.recv_timeout(std::time::Duration::from_secs(30)),
            Ok(7),
            "the blocked worker stranded the task it had just queued"
        );
    }

    #[test]
    fn callers_outside_the_process_runtime_keep_the_thread_based_path() {
        let block = || block_on_with_runtime(async { 7 }, "blocking runtime test panicked");

        // `block_in_place` would panic in both of these.
        let multi_thread = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        let in_local_set = local.block_on(&multi_thread, async move {
            let spawned = tokio::task::spawn_local(async move { block() });
            (block(), spawned.await.unwrap())
        });
        assert_eq!(in_local_set, (7, 7));

        let current_thread = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        assert_eq!(current_thread.block_on(async move { block() }), 7);
    }

    #[test]
    fn entered_runtime_is_preserved() {
        let local = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _entered = local.enter();
        assert_eq!(
            runtime().runtime_flavor(),
            tokio::runtime::RuntimeFlavor::CurrentThread
        );
    }
}
